//! Opt-in ElevenLabs transcription. Credentials never enter settings JSON.
//! WAV/multipart approach informed by christophostertag/Handy PR #1241 (MIT).
use crate::managers::{audio::AudioRecordingManager, transcription::TranscriptionManager};
use crate::settings::{get_settings, write_settings, AppSettings};
use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use serde::Serialize;
use specta::Type;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tauri::{AppHandle, Emitter, Manager};

const ENDPOINT: &str = "https://api.elevenlabs.io/v1/speech-to-text";
mod client;
pub mod realtime;
use client::{send_transcription, validate_model, Transcript};

pub fn realtime_selected(settings: &AppSettings) -> bool {
    settings.elevenlabs_enabled && settings.elevenlabs_model == "scribe_v2_realtime"
}

pub fn start_realtime(
    settings: &AppSettings,
    on_partial: Arc<dyn Fn(String, String) + Send + Sync>,
) -> Result<realtime::RealtimeSession> {
    realtime::RealtimeSession::start(
        || read_key()?.context("Add your ElevenLabs API key in Models > ElevenLabs"),
        settings.selected_language.clone(),
        on_partial,
    )
}

static CREDENTIAL_LOCK: Mutex<()> = Mutex::new(());

#[derive(Serialize, Type)]
pub struct ElevenLabsStatus {
    enabled: bool,
    model: String,
    has_api_key: bool,
}

fn credential() -> Result<keyring::Entry> {
    // Do not silently use keyring's in-memory mock backend on other platforms.
    if !cfg!(any(target_os = "macos", target_os = "windows")) {
        bail!("ElevenLabs secure key storage is supported on macOS and Windows.");
    }
    keyring::Entry::new("com.absk251.handy-eleven", "elevenlabs-api-key")
        .context("Cannot open the system credential store")
}

fn read_key() -> Result<Option<String>> {
    let _guard = CREDENTIAL_LOCK
        .lock()
        .map_err(|_| anyhow::anyhow!("Credential store lock failed"))?;
    match credential()?.get_password() {
        Ok(key) => Ok(Some(key)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(_) => bail!(
            "Cannot read the ElevenLabs key. Unlock your system credential store and try again."
        ),
    }
}

fn ensure_idle(app: &AppHandle) -> Result<()> {
    if app.state::<Arc<AudioRecordingManager>>().is_recording() || crate::tray::is_busy(app) {
        bail!("Finish or cancel the current recording/transcription before changing settings.");
    }
    Ok(())
}

fn status(app: &AppHandle) -> Result<ElevenLabsStatus> {
    let settings = get_settings(app);
    Ok(ElevenLabsStatus {
        enabled: settings.elevenlabs_enabled,
        model: settings.elevenlabs_model,
        has_api_key: read_key()?.is_some_and(|key| !key.is_empty()),
    })
}

#[tauri::command]
#[specta::specta]
pub async fn get_elevenlabs_status(app: AppHandle) -> Result<ElevenLabsStatus, String> {
    tauri::async_runtime::spawn_blocking(move || status(&app).map_err(|e| e.to_string()))
        .await
        .map_err(|_| "Unable to read ElevenLabs settings".to_string())?
}

#[tauri::command]
#[specta::specta]
pub async fn configure_elevenlabs(
    app: AppHandle,
    enabled: bool,
    model: String,
    api_key: Option<String>,
) -> Result<ElevenLabsStatus, String> {
    tauri::async_runtime::spawn_blocking(move || -> Result<ElevenLabsStatus> {
        ensure_idle(&app)?;
        if model != "scribe_v2_realtime" {
            validate_model(&model)?;
        }
        if let Some(key) = api_key {
            let key = key.trim();
            if key.is_empty() || key.contains(['\r', '\n']) {
                bail!("Enter a non-empty ElevenLabs API key without line breaks.");
            }
            let _guard = CREDENTIAL_LOCK
                .lock()
                .map_err(|_| anyhow::anyhow!("Credential store lock failed"))?;
            credential()?.set_password(key).map_err(|_| {
                anyhow::anyhow!("Cannot save the key in the system credential store")
            })?;
        }
        if enabled && read_key()?.is_none() {
            bail!("Add your ElevenLabs API key first.");
        }
        let mut settings = get_settings(&app);
        settings.elevenlabs_enabled = enabled;
        settings.elevenlabs_model = model;
        if enabled {
            settings.onboarding_completed = true;
        }
        write_settings(&app, settings);
        if enabled {
            app.state::<Arc<TranscriptionManager>>().request_unload();
        }
        let _ = app.emit("settings-changed", serde_json::json!({}));
        crate::tray::update_tray_menu(&app);
        status(&app)
    })
    .await
    .map_err(|_| "Unable to save ElevenLabs settings".to_string())?
    .map_err(|e| e.to_string())
}

#[tauri::command]
#[specta::specta]
pub async fn remove_elevenlabs_key(app: AppHandle) -> Result<(), String> {
    tauri::async_runtime::spawn_blocking(move || -> Result<()> {
        ensure_idle(&app)?;
        let _guard = CREDENTIAL_LOCK
            .lock()
            .map_err(|_| anyhow::anyhow!("Credential store lock failed"))?;
        match credential()?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => {}
            Err(_) => bail!("Cannot remove the key from the system credential store"),
        }
        let mut settings = get_settings(&app);
        settings.elevenlabs_enabled = false;
        write_settings(&app, settings);
        let _ = app.emit("settings-changed", serde_json::json!({}));
        crate::tray::update_tray_menu(&app);
        Ok(())
    })
    .await
    .map_err(|_| "Unable to remove ElevenLabs key".to_string())?
    .map_err(|e| e.to_string())
}

pub fn transcribe(audio: &[f32], settings: &AppSettings) -> Result<Transcript> {
    validate_model(&settings.elevenlabs_model)?;
    let key = read_key()?.context("Add your ElevenLabs API key in Models > ElevenLabs")?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    send_transcription(
        &client,
        ENDPOINT,
        &key,
        audio,
        &settings.elevenlabs_model,
        &settings.selected_language,
    )
}
