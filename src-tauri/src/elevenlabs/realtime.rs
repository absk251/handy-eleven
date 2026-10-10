//! Native Scribe v2 Realtime transport. No reconnect or batch fallback: either
//! could upload/bill the same recording twice. The key only travels in a TLS header.
use super::client::Transcript;
use anyhow::{anyhow, bail, Context, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{mpsc as async_mpsc, Notify};
use tokio::time::{timeout, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
};

const ENDPOINT: &str = "wss://api.elevenlabs.io/v1/speech-to-text/realtime";
const SAMPLE_RATE: usize = 16_000;
const CHUNK_SAMPLES: usize = 1_600; // 100 ms, recommended minimum chunk size.
const QUEUE_CAPACITY: usize = 512;
const SEGMENT_SAMPLES: usize = SAMPLE_RATE * 20;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const FINAL_TIMEOUT: Duration = Duration::from_secs(20);
const FINISH_TIMEOUT: Duration = Duration::from_secs(35);
const NETWORK_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TEXT_BYTES: usize = 2 * 1024 * 1024;

type Progress = Arc<dyn Fn(String, String) + Send + Sync>;
type KeyLoader = Box<dyn FnOnce() -> Result<String> + Send>;

#[derive(Default)]
struct Control {
    cancelled: AtomicBool,
    overflow: AtomicBool,
    finishing: AtomicBool,
    wake: Notify,
}

impl Control {
    async fn interrupted(&self) -> anyhow::Error {
        loop {
            // Register before checking the flags, so cancellation cannot be lost.
            let notified = self.wake.notified();
            if self.overflow.load(Ordering::Acquire) {
                return anyhow!("ElevenLabs streaming could not keep up with the microphone. Recording was not resent; check your connection and retry.");
            }
            if self.cancelled.load(Ordering::Acquire) {
                return anyhow!("ElevenLabs streaming cancelled.");
            }
            notified.await;
        }
    }
}

/// One push-to-talk session. Feed is bounded and nonblocking. Finish must run on
/// a blocking worker after the recorder has stopped delivering frames.
pub struct RealtimeSession {
    audio: async_mpsc::Sender<Vec<f32>>,
    control: Arc<Control>,
    result: Mutex<Option<mpsc::Receiver<Result<Transcript>>>>,
}

impl RealtimeSession {
    /// Start connecting immediately; audio may queue while TLS/keychain opens.
    pub fn start(
        key_loader: impl FnOnce() -> Result<String> + Send + 'static,
        language: String,
        on_partial: Progress,
    ) -> Result<Self> {
        Self::start_at(ENDPOINT.into(), Box::new(key_loader), language, on_partial)
    }

    fn start_at(
        endpoint: String,
        key_loader: KeyLoader,
        language: String,
        on_partial: Progress,
    ) -> Result<Self> {
        let (audio, rx) = async_mpsc::channel(QUEUE_CAPACITY);
        let (result_tx, result_rx) = mpsc::channel();
        let control = Arc::new(Control::default());
        let worker_control = control.clone();
        std::thread::Builder::new()
            .name("elevenlabs-realtime".into())
            .spawn(move || {
                let result = (|| {
                    // System credential stores can block or prompt; never run
                    // this on the microphone or Tauri asynchronous executor.
                    let key = key_loader()?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .context("Cannot start ElevenLabs streaming worker")?;
                    runtime.block_on(async {
                        tokio::select! {
                            biased;
                            error = worker_control.interrupted() => Err(error),
                            result = run_session(&endpoint, &key, &language, rx, &worker_control, &on_partial, FINAL_TIMEOUT) => result,
                        }
                    })
                })();
                let _ = result_tx.send(result);
            })
            .context("Cannot start ElevenLabs streaming thread")?;
        Ok(Self {
            audio,
            control,
            result: Mutex::new(Some(result_rx)),
        })
    }

    /// Accept mono 16 kHz floating-point PCM without waiting for the network.
    /// Overflow fails the whole session rather than dropping words silently.
    pub fn feed(&self, samples: &[f32]) {
        if self.control.finishing.load(Ordering::Acquire)
            || self.control.cancelled.load(Ordering::Acquire)
            || self.control.overflow.load(Ordering::Acquire)
        {
            return;
        }
        for chunk in samples.chunks(CHUNK_SAMPLES) {
            match self.audio.try_send(chunk.to_vec()) {
                Ok(()) => {}
                Err(async_mpsc::error::TrySendError::Full(_)) => {
                    self.control.overflow.store(true, Ordering::Release);
                    self.control.wake.notify_one();
                    return;
                }
                // Preserve the worker's useful error (e.g. invalid key).
                Err(async_mpsc::error::TrySendError::Closed(_)) => return,
            }
        }
    }

    /// Drain queued audio, explicitly commit, and wait for its final transcript.
    /// The same Arc may still be cancelled while this method waits.
    pub fn finish(&self) -> Result<Transcript> {
        let receiver = self
            .result
            .lock()
            .map_err(|_| anyhow!("ElevenLabs streaming result lock failed"))?
            .take()
            .context("ElevenLabs streaming was already finalized")?;
        self.control.finishing.store(true, Ordering::Release);
        let deadline = std::time::Instant::now() + FINISH_TIMEOUT;
        loop {
            if self.control.cancelled.load(Ordering::Acquire) {
                bail!("ElevenLabs streaming cancelled.");
            }
            if self.control.overflow.load(Ordering::Acquire) {
                bail!("ElevenLabs streaming could not keep up with the microphone. Recording was not resent; check your connection and retry.");
            }
            match receiver.recv_timeout(Duration::from_millis(50)) {
                Ok(result) => {
                    if self.control.cancelled.load(Ordering::Acquire) {
                        bail!("ElevenLabs streaming cancelled.");
                    }
                    return result;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    bail!("ElevenLabs streaming worker stopped unexpectedly.")
                }
                Err(mpsc::RecvTimeoutError::Timeout) if std::time::Instant::now() < deadline => {}
                Err(_) => {
                    self.cancel();
                    bail!("ElevenLabs streaming timed out. Audio already sent may have been billed; retry manually.")
                }
            }
        }
    }

    /// Cancel pending connection, upload, or finalization without emitting text.
    pub fn cancel(&self) {
        self.control.cancelled.store(true, Ordering::Release);
        self.control.wake.notify_one();
    }
}

impl Drop for RealtimeSession {
    fn drop(&mut self) {
        self.cancel();
    }
}

fn normalize_language(language: &str) -> Option<String> {
    match language.trim() {
        "" | "auto" => None,
        "zh-Hans" | "zh-Hant" => Some("zh".into()),
        "jw" => Some("jv".into()),
        code => Some(code.into()),
    }
}

fn audio_message(samples: &[f32], commit: bool) -> Message {
    let mut pcm = Vec::with_capacity(samples.len() * 2);
    for &sample in samples {
        let finite = if sample.is_finite() {
            sample.clamp(-1.0, 1.0)
        } else {
            0.0
        };
        pcm.extend_from_slice(&((finite * i16::MAX as f32) as i16).to_le_bytes());
    }
    Message::Text(
        json!({
            "message_type": "input_audio_chunk", "audio_base_64": STANDARD.encode(pcm),
            "sample_rate": SAMPLE_RATE, "commit": commit,
        })
        .to_string()
        .into(),
    )
}

fn provider_error(kind: &str) -> &'static str {
    // Never return the provider's `error`/message/body: it can echo private input.
    match kind {
        "auth_error" => "ElevenLabs rejected the API key. Check Speech-to-Text permission.",
        "quota_exceeded" | "rate_limited" => {
            "ElevenLabs streaming quota or rate limit reached. Check your account and retry later."
        }
        "unaccepted_terms" => {
            "Accept the Scribe terms in your ElevenLabs dashboard before using streaming."
        }
        "input_error" | "invalid_request" => {
            "ElevenLabs rejected the streaming configuration. Check the language setting."
        }
        "session_time_limit_exceeded" => {
            "ElevenLabs streaming session limit reached. Start a new recording."
        }
        _ => {
            "ElevenLabs streaming failed. Audio already sent may have been billed; retry manually."
        }
    }
}

async fn run_session(
    endpoint: &str,
    key: &str,
    language: &str,
    mut audio: async_mpsc::Receiver<Vec<f32>>,
    control: &Control,
    progress: &Progress,
    final_timeout: Duration,
) -> Result<Transcript> {
    let language = normalize_language(language);
    let mut url = reqwest::Url::parse(endpoint).context("Invalid ElevenLabs streaming endpoint")?;
    url.query_pairs_mut()
        .append_pair("model_id", "scribe_v2_realtime")
        .append_pair("audio_format", "pcm_16000")
        .append_pair("commit_strategy", "manual")
        .append_pair("keepalive_interval_ms", "3000");
    if let Some(code) = &language {
        url.query_pairs_mut().append_pair("language_code", code);
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| anyhow!("Cannot configure ElevenLabs streaming"))?;
    let mut header = key
        .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
        .map_err(|_| anyhow!("Invalid ElevenLabs API key format"))?;
    header.set_sensitive(true);
    request.headers_mut().insert("xi-api-key", header);
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_TEXT_BYTES))
        .max_frame_size(Some(MAX_TEXT_BYTES));
    let (mut socket, _) = timeout(
        CONNECT_TIMEOUT,
        connect_async_with_config(request, Some(config), true),
    )
    .await
    .map_err(|_| anyhow!("ElevenLabs streaming connection timed out."))?
    .map_err(|_| {
        anyhow!("Cannot connect to ElevenLabs streaming. Check your connection and API key.")
    })?;
    let mut started = false;
    let start_deadline = Instant::now() + CONNECT_TIMEOUT;
    let mut final_deadline = None;
    let mut last_message = Instant::now();
    let mut pending = Vec::with_capacity(CHUNK_SAMPLES);
    let mut segment_samples = 0usize;
    let mut total_samples = 0usize;
    let mut commits = VecDeque::new(); // true marks the final manual commit.
    let mut committed = String::new();
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    let session_deadline = Instant::now() + Duration::from_secs(60 * 60);
    loop {
        tokio::select! {
            // Read first when both ready: commit acknowledgements remain ordered.
            biased;
            message = socket.next() => {
                let message = message.context("ElevenLabs streaming disconnected before final transcription")?
                    .map_err(|_| anyhow!("ElevenLabs streaming connection failed. Retry manually."))?;
                last_message = Instant::now();
                match message {
                    Message::Text(text) => {
                        let event: Value = serde_json::from_str(&text)
                            .map_err(|_| anyhow!("ElevenLabs returned an invalid streaming event"))?;
                        let kind = event.get("message_type").and_then(Value::as_str).unwrap_or("");
                        match kind {
                            "session_started" => { started = true; },
                            "partial_transcript" => {
                                let text = event.get("text").and_then(Value::as_str).unwrap_or("");
                                if !control.cancelled.load(Ordering::Acquire) {
                                    progress(committed.clone(), text.to_owned());
                                }
                            },
                            "committed_transcript" => {
                                let text = event.get("text").and_then(Value::as_str)
                                    .context("ElevenLabs returned a final event without text")?.trim();
                                if committed.len() + text.len() > MAX_TEXT_BYTES { bail!("ElevenLabs transcript exceeded the session text limit."); }
                                if !text.is_empty() {
                                    if !committed.is_empty() { committed.push(' '); }
                                    committed.push_str(text);
                                }
                                let is_final = commits.pop_front().unwrap_or(false);
                                if !control.cancelled.load(Ordering::Acquire) { progress(committed.clone(), String::new()); }
                                if is_final {
                                    // Do not delay the result on the server's close handshake.
                                    return Ok(Transcript { text: committed, language_code: language });
                                }
                            },
                            // Timestamp events duplicate committed text. We do not request
                            // them or paid processing add-ons, but tolerate future metadata.
                            "committed_transcript_with_timestamps" | "warning" => {},
                            _ => bail!("{}", provider_error(kind)),
                        }
                    },
                    Message::Ping(bytes) => {
                        timeout(NETWORK_TIMEOUT, socket.send(Message::Pong(bytes))).await
                            .map_err(|_| anyhow!("ElevenLabs streaming connection stalled"))?
                            .map_err(|_| anyhow!("ElevenLabs streaming connection failed"))?;
                    },
                    Message::Close(_) => bail!("ElevenLabs streaming closed before final transcription. Retry manually."),
                    _ => {},
                }
            },
            samples = audio.recv(), if started && final_deadline.is_none() => {
                let Some(samples) = samples else { bail!("ElevenLabs streaming cancelled."); };
                total_samples += samples.len();
                pending.extend_from_slice(&samples);
                while pending.len() >= CHUNK_SAMPLES {
                    let chunk: Vec<_> = pending.drain(..CHUNK_SAMPLES).collect();
                    segment_samples += chunk.len();
                    let commit = segment_samples >= SEGMENT_SAMPLES;
                    timeout(NETWORK_TIMEOUT, socket.send(audio_message(&chunk, commit))).await
                        .map_err(|_| anyhow!("ElevenLabs audio upload stalled. Retry manually."))?
                        .map_err(|_| anyhow!("ElevenLabs audio upload failed. Retry manually."))?;
                    if commit { commits.push_back(false); segment_samples = 0; }
                }
            },
            _ = tick.tick() => {
                let now = Instant::now();
                if !started && now >= start_deadline { bail!("ElevenLabs did not start the streaming session."); }
                if now >= session_deadline { bail!("ElevenLabs streaming reached the one-hour recording limit."); }
                if final_deadline.is_some_and(|deadline| now >= deadline) {
                    bail!("ElevenLabs final transcript timed out. Audio already sent may have been billed; retry manually.");
                }
                if started && final_deadline.is_none() && now.duration_since(last_message) > Duration::from_secs(30) {
                    bail!("ElevenLabs streaming stopped responding. Retry manually.");
                }
                if started && final_deadline.is_none() && control.finishing.load(Ordering::Acquire) && audio.is_empty() {
                    if total_samples == 0 {
                        return Ok(Transcript { text: String::new(), language_code: language });
                    }
                    // At an exact periodic boundary there is no uncommitted
                    // audio. Reuse its acknowledgement instead of sending a
                    // second, empty commit that could be throttled.
                    if segment_samples == 0 && pending.is_empty() {
                        if let Some(last) = commits.back_mut() {
                            *last = true;
                            final_deadline = Some(Instant::now() + final_timeout);
                            continue;
                        }
                        return Ok(Transcript { text: committed, language_code: language });
                    }
                    // Match the official SDK's commit(): an empty audio chunk
                    // is allowed and flushes already-sent audio. Do not add
                    // synthetic seconds of silence (which may be billable).
                    timeout(NETWORK_TIMEOUT, socket.send(audio_message(&pending, true))).await
                        .map_err(|_| anyhow!("ElevenLabs final audio upload stalled. Retry manually."))?
                        .map_err(|_| anyhow!("ElevenLabs final audio upload failed. Retry manually."))?;
                    commits.push_back(true);
                    final_deadline = Some(Instant::now() + final_timeout);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    // Tungstenite requires its unboxed HTTP error response in this callback.
    #[allow(clippy::result_large_err)]
    fn server<F, Fut>(handler: F) -> (String, std::thread::JoinHandle<()>)
    where
        F: FnOnce(tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Fut
            + Send
            + 'static,
        Fut: std::future::Future<Output = ()>,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let thread = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                    let (socket, _) = listener.accept().await.unwrap();
                    let socket = tokio_tungstenite::accept_hdr_async(
                        socket,
                        |request: &Request, response: Response| {
                            assert_eq!(request.headers()["xi-api-key"], "test-secret");
                            assert!(request
                                .uri()
                                .query()
                                .unwrap()
                                .contains("model_id=scribe_v2_realtime"));
                            assert!(request
                                .uri()
                                .query()
                                .unwrap()
                                .contains("audio_format=pcm_16000"));
                            assert!(!request.uri().to_string().contains("test-secret"));
                            Ok(response)
                        },
                    )
                    .await
                    .unwrap();
                    timeout(Duration::from_secs(5), handler(socket))
                        .await
                        .unwrap();
                });
        });
        (format!("ws://{address}/v1/speech-to-text/realtime"), thread)
    }

    async fn event(
        socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        kind: &str,
        text: &str,
    ) {
        socket
            .send(Message::Text(
                json!({"message_type":kind,"text":text}).to_string().into(),
            ))
            .await
            .unwrap();
    }

    fn session(endpoint: String, callback: Progress) -> RealtimeSession {
        RealtimeSession::start_at(
            endpoint,
            Box::new(|| Ok("test-secret".into())),
            "auto".into(),
            callback,
        )
        .unwrap()
    }

    #[test]
    fn streams_before_stop_and_waits_for_committed_text() {
        let (first_audio_tx, first_audio_rx) = mpsc::channel();
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            let first = socket.next().await.unwrap().unwrap();
            let chunk: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
            assert_eq!(chunk["commit"], false);
            let pcm = STANDARD
                .decode(chunk["audio_base_64"].as_str().unwrap())
                .unwrap();
            assert_eq!(pcm.len(), CHUNK_SAMPLES * 2);
            assert_eq!(i16::from_le_bytes([pcm[0], pcm[1]]), 16383);
            first_audio_tx.send(()).unwrap();
            event(&mut socket, "partial_transcript", "hello wor").await;
            let final_chunk = socket.next().await.unwrap().unwrap();
            let final_chunk: Value = serde_json::from_str(final_chunk.to_text().unwrap()).unwrap();
            assert_eq!(final_chunk["commit"], true);
            event(&mut socket, "committed_transcript", "hello world").await;
        });
        let (partial_tx, partial_rx) = mpsc::channel();
        let session = session(
            endpoint,
            Arc::new(move |_, partial| {
                let _ = partial_tx.send(partial);
            }),
        );
        session.feed(&vec![0.5; CHUNK_SAMPLES]);
        first_audio_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            partial_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
            "hello wor"
        );
        assert_eq!(session.finish().unwrap().text, "hello world");
        worker.join().unwrap();
    }

    #[test]
    fn aggregates_segments_and_does_not_mistake_prior_commit_for_final() {
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            let mut commits = 0;
            while let Some(Ok(message)) = socket.next().await {
                let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if data["commit"] == true {
                    commits += 1;
                    if commits == 2 {
                        // Deliberately defer the earlier acknowledgement until
                        // after the final request, exercising FIFO commit tracking.
                        event(&mut socket, "committed_transcript", "first segment").await;
                        event(
                            &mut socket,
                            "committed_transcript_with_timestamps",
                            "first segment",
                        )
                        .await;
                        event(&mut socket, "committed_transcript", "last segment").await;
                        return;
                    }
                }
            }
            panic!("client failed to finalize");
        });
        let session = session(endpoint, Arc::new(|_, _| {}));
        session.feed(&vec![0.1; SEGMENT_SAMPLES + CHUNK_SAMPLES]);
        assert_eq!(session.finish().unwrap().text, "first segment last segment");
        worker.join().unwrap();
    }

    #[test]
    fn provider_errors_do_not_expose_private_payload() {
        let (endpoint, worker) = server(move |mut socket| async move {
            socket
                .send(Message::Text(
                    json!({"message_type":"auth_error","error":"test-secret private audio"})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        });
        let session = session(endpoint, Arc::new(|_, _| {}));
        session.feed(&[0.1; CHUNK_SAMPLES]);
        let error = session.finish().err().unwrap().to_string();
        assert!(error.contains("API key"));
        assert!(!error.contains("test-secret"));
        assert!(!error.contains("private audio"));
        worker.join().unwrap();
    }

    #[test]
    fn cancel_interrupts_wait_for_final_transcript() {
        let (commit_tx, commit_rx) = mpsc::channel();
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            while let Some(Ok(message)) = socket.next().await {
                let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if data["commit"] == true {
                    commit_tx.send(()).unwrap();
                    break;
                }
            }
            // Cancellation must close the socket without awaiting an answer.
            let _ = socket.next().await;
        });
        let session = Arc::new(session(endpoint, Arc::new(|_, _| {})));
        session.feed(&[0.1; CHUNK_SAMPLES]);
        let finishing = session.clone();
        let finisher = std::thread::spawn(move || finishing.finish());
        commit_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let now = std::time::Instant::now();
        session.cancel();
        assert!(finisher
            .join()
            .unwrap()
            .err()
            .unwrap()
            .to_string()
            .contains("cancelled"));
        assert!(now.elapsed() < Duration::from_secs(1));
        worker.join().unwrap();
    }

    #[test]
    fn overflowing_audio_queue_fails_without_silent_loss_or_connection() {
        let (release_tx, release_rx) = mpsc::channel();
        let session = RealtimeSession::start_at(
            "ws://127.0.0.1:1".into(),
            Box::new(move || {
                release_rx.recv().unwrap();
                Ok("test-secret".into())
            }),
            "auto".into(),
            Arc::new(|_, _| {}),
        )
        .unwrap();
        session.feed(&vec![0.0; CHUNK_SAMPLES * (QUEUE_CAPACITY + 1)]);
        release_tx.send(()).unwrap();
        assert!(session
            .finish()
            .err()
            .unwrap()
            .to_string()
            .contains("could not keep up"));
    }

    #[test]
    fn cancel_does_not_wait_for_blocked_credential_store() {
        let (release_tx, release_rx) = mpsc::channel();
        let session = RealtimeSession::start_at(
            "ws://127.0.0.1:1".into(),
            Box::new(move || {
                let _ = release_rx.recv();
                Ok("test-secret".into())
            }),
            "auto".into(),
            Arc::new(|_, _| {}),
        )
        .unwrap();
        session.cancel();
        let now = std::time::Instant::now();
        assert!(session
            .finish()
            .err()
            .unwrap()
            .to_string()
            .contains("cancelled"));
        assert!(now.elapsed() < Duration::from_secs(1));
        release_tx.send(()).unwrap();
    }

    #[test]
    fn short_recording_commits_exact_audio_without_synthetic_silence() {
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            let message = socket.next().await.unwrap().unwrap();
            let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            assert_eq!(data["commit"], true);
            let pcm = STANDARD
                .decode(data["audio_base_64"].as_str().unwrap())
                .unwrap();
            assert_eq!(pcm.len(), 160); // Five milliseconds, no invented padding.
            event(&mut socket, "committed_transcript", "short").await;
        });
        let session = session(endpoint, Arc::new(|_, _| {}));
        session.feed(&[0.1; 80]);
        assert_eq!(session.finish().unwrap().text, "short");
        worker.join().unwrap();
    }

    #[test]
    fn exact_segment_boundary_reuses_the_existing_commit() {
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            while let Some(Ok(message)) = socket.next().await {
                let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if data["commit"] == true {
                    event(&mut socket, "committed_transcript", "boundary").await;
                    let next = socket.next().await;
                    assert!(!matches!(next, Some(Ok(Message::Text(_)))));
                    return;
                }
            }
        });
        let session = session(endpoint, Arc::new(|_, _| {}));
        session.feed(&vec![0.1; SEGMENT_SAMPLES]);
        assert_eq!(session.finish().unwrap().text, "boundary");
        worker.join().unwrap();
    }

    #[test]
    fn silence_can_finalize_to_empty_text() {
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            while let Some(Ok(message)) = socket.next().await {
                let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if data["commit"] == true {
                    event(&mut socket, "committed_transcript", "").await;
                    return;
                }
            }
        });
        let session = session(endpoint, Arc::new(|_, _| {}));
        session.feed(&[0.0; CHUNK_SAMPLES]);
        assert_eq!(session.finish().unwrap().text, "");
        worker.join().unwrap();
    }

    #[test]
    fn missing_final_ack_times_out_without_returning_partial_text() {
        let (endpoint, worker) = server(move |mut socket| async move {
            event(&mut socket, "session_started", "").await;
            while let Some(Ok(message)) = socket.next().await {
                let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                if data["commit"] == true {
                    event(
                        &mut socket,
                        "partial_transcript",
                        "unfinished private draft",
                    )
                    .await;
                    let _ = socket.next().await;
                    return;
                }
            }
        });
        let (audio_tx, audio_rx) = async_mpsc::channel(4);
        audio_tx.try_send(vec![0.1; 80]).unwrap();
        let control = Control::default();
        control.finishing.store(true, Ordering::Release);
        let callback: Progress = Arc::new(|_, _| {});
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_session(
                &endpoint,
                "test-secret",
                "auto",
                audio_rx,
                &control,
                &callback,
                Duration::from_millis(75),
            ));
        let error = result.err().unwrap().to_string();
        assert!(error.contains("final transcript timed out"));
        assert!(!error.contains("private"));
        worker.join().unwrap();
    }

    #[test]
    fn pcm_is_little_endian_and_finite() {
        let message = audio_message(&[-2.0, 2.0, f32::NAN], false);
        let data: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        let pcm = STANDARD
            .decode(data["audio_base_64"].as_str().unwrap())
            .unwrap();
        assert_eq!(pcm, [1, 128, 255, 127, 0, 0]);
        assert_eq!(normalize_language("zh-Hans"), Some("zh".into()));
        assert_eq!(normalize_language("auto"), None);
    }
}
