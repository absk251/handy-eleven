use anyhow::{bail, Context, Result};
use reqwest::blocking::{multipart, Client};
use serde::Deserialize;
use std::io::Cursor;

pub(super) fn validate_model(model: &str) -> Result<()> {
    if !matches!(model, "scribe_v1" | "scribe_v2") {
        bail!("Choose Scribe v1 or Scribe v2. Realtime models require a separate streaming integration.");
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct Transcript {
    pub text: String,
    pub language_code: Option<String>,
}

pub(super) fn send_transcription(
    client: &Client,
    endpoint: &str,
    key: &str,
    audio: &[f32],
    model: &str,
    language: &str,
) -> Result<Transcript> {
    validate_model(model)?;
    let file = multipart::Part::bytes(encode_wav(audio)?)
        .file_name("recording.wav")
        .mime_str("audio/wav")?;
    let mut form = multipart::Form::new()
        .text("model_id", model.to_owned())
        .text("tag_audio_events", "false")
        .text("diarize", "false")
        .text("timestamps_granularity", "none")
        .part("file", file);
    if let Some(language) = normalize_language(language) {
        form = form.text("language_code", language);
    }
    // Custom words are corrected locally. No paid keyterm or editing add-ons.
    // Do not retry automatically: a timed-out request may already be billable.
    let response = client
        .post(endpoint)
        .header("xi-api-key", key)
        .multipart(form)
        .send()
        .map_err(|e| {
            anyhow::anyhow!(if e.is_timeout() {
                "ElevenLabs timed out. The request may have been billed; retry manually."
            } else {
                "Cannot reach ElevenLabs. Check your internet connection."
            })
        })?;
    let code = response.status().as_u16();
    if !response.status().is_success() {
        // Do not log/display provider bodies: they may echo private input.
        bail!("{}", error_message(code));
    }
    let mut transcript: Transcript = response
        .json()
        .context("ElevenLabs returned an invalid transcript")?;
    transcript.text = transcript.text.trim().to_owned();
    // Scribe may report ISO-639-3, whereas Handy's output pipeline uses 639-1.
    transcript.language_code = transcript.language_code.map(|code| {
        isolang::Language::from_639_3(&code)
            .and_then(|lang| lang.to_639_1())
            .map(str::to_owned)
            .unwrap_or(code)
    });
    Ok(transcript)
}

fn normalize_language(language: &str) -> Option<String> {
    match language.trim() {
        "" | "auto" => None,
        "zh-Hans" | "zh-Hant" => Some("zh".into()),
        "jw" => Some("jv".into()),
        language => Some(language.into()),
    }
}

fn error_message(status: u16) -> String {
    match status {
        401 | 403 => "ElevenLabs rejected the API key or its Speech-to-Text permission. Check the key and account quota.".into(),
        402 => "ElevenLabs has insufficient credits. Check your account billing.".into(),
        429 => "ElevenLabs rate limit or quota reached. Check your account and retry later.".into(),
        500..=599 => "ElevenLabs is temporarily unavailable. Retry later.".into(),
        _ => format!("ElevenLabs request failed (HTTP {status}). Check the model and language settings."),
    }
}

fn encode_wav(audio: &[f32]) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(Vec::new());
    {
        let spec = hound::WavSpec {
            channels: 1,
            sample_rate: 16_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
        for sample in audio {
            let sample = if sample.is_finite() {
                sample.clamp(-1.0, 1.0)
            } else {
                0.0
            };
            writer.write_sample((sample * i16::MAX as f32) as i16)?;
        }
        // API requires at least 100 ms; pad very short recordings with silence.
        for _ in audio.len()..1600 {
            writer.write_sample(0i16)?;
        }
        writer.finalize()?;
    }
    Ok(cursor.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn server(status: u16, body: &str) -> (String, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/transcribe", listener.local_addr().unwrap());
        let body = body.to_string();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0u8; 4096];
            loop {
                let count = stream.read(&mut buf).unwrap();
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..count]);
                if let Some(header_end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let header = String::from_utf8_lossy(&bytes[..header_end]);
                    let length: usize = header
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= header_end + 4 + length {
                        break;
                    }
                }
            }
            let response = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).unwrap();
            bytes
        });
        (url, handle)
    }

    #[test]
    fn sends_selected_model_language_and_mono_wav() {
        let (url, server) = server(200, r#"{"text":"  Привет  ","language_code":"rus"}"#);
        let client = Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let result = send_transcription(
            &client,
            &url,
            "test-key-not-real",
            &[0.5; 1600],
            "scribe_v1",
            "ru",
        )
        .unwrap();
        assert_eq!(result.text, "Привет");
        assert_eq!(result.language_code.as_deref(), Some("ru"));
        let bytes = server.join().unwrap();
        let request = String::from_utf8_lossy(&bytes);
        assert!(request.contains("xi-api-key: test-key-not-real"));
        assert!(request.contains("name=\"model_id\"\r\n\r\nscribe_v1"));
        assert!(request.contains("name=\"language_code\"\r\n\r\nru"));
        assert!(request.contains("name=\"tag_audio_events\"\r\n\r\nfalse"));
        assert!(!request.contains("keyterms"));
        assert!(request.contains("RIFF"));
    }

    #[test]
    fn auto_detection_omits_language_and_sends_v2() {
        let (url, server) = server(200, r#"{"text":"hello","language_code":"en"}"#);
        let client = Client::builder().no_proxy().build().unwrap();
        send_transcription(&client, &url, "test", &[0.0; 1600], "scribe_v2", "auto").unwrap();
        let bytes = server.join().unwrap();
        let request = String::from_utf8_lossy(&bytes);
        assert!(!request.contains("name=\"language_code\""));
        assert!(request.contains("name=\"model_id\"\r\n\r\nscribe_v2"));
    }

    #[test]
    fn errors_never_echo_provider_body_or_key() {
        for code in [401, 403, 402, 429, 500, 422] {
            let (url, server) = server(code, r#"{"detail":"private-audio-secret-key"}"#);
            let client = Client::builder().no_proxy().build().unwrap();
            let err = send_transcription(
                &client,
                &url,
                "private-key",
                &[0.0; 1600],
                "scribe_v2",
                "auto",
            )
            .err()
            .unwrap()
            .to_string();
            assert!(!err.contains("private"));
            assert!(err.contains("ElevenLabs"));
            server.join().unwrap();
        }
    }

    #[test]
    fn wav_has_correct_format_and_clamps_samples() {
        let bytes = encode_wav(&[-2.0, 2.0, f32::NAN]).unwrap();
        let mut reader = hound::WavReader::new(Cursor::new(bytes)).unwrap();
        assert_eq!(reader.spec().sample_rate, 16000);
        assert_eq!(reader.spec().channels, 1);
        let samples: Vec<i16> = reader.samples().map(Result::unwrap).collect();
        assert_eq!(samples.len(), 1600);
        assert_eq!(&samples[..3], &[-32767, 32767, 0]);
    }

    #[test]
    fn rejects_realtime_and_normalizes_language() {
        assert!(validate_model("scribe_v2_realtime").is_err());
        assert!(validate_model("scribe_v2").is_ok());
        assert_eq!(normalize_language("auto"), None);
        assert_eq!(normalize_language("zh-Hant").as_deref(), Some("zh"));
        assert_eq!(normalize_language("kk").as_deref(), Some("kk"));
    }
}
