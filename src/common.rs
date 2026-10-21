//! Canonical settings are `phone.toml`, `screening.md` and `secrets/` in one
//! private state root. Resolve them at each admission/delivery.
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

pub type SafeResult<T> = Result<T, &'static str>;

pub fn check(value: bool, error: &'static str) -> SafeResult<()> {
    if value { Ok(()) } else { Err(error) }
}

pub fn private_dir(path: &Path) -> SafeResult<()> {
    if !path.exists() {
        fs::create_dir(path).map_err(|_| "directory_create_failed")?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|_| "directory_permissions_failed")?;
    }
    existing_private_dir(path)
}

/// Validate an existing private directory without creating or repairing it.
pub fn existing_private_dir(path: &Path) -> SafeResult<()> {
    let m = fs::symlink_metadata(path).map_err(|_| "directory_metadata_failed")?;
    // geteuid only reads this process's effective identity.
    check(
        m.is_dir()
            && !m.file_type().is_symlink()
            && m.uid() == unsafe { libc::geteuid() }
            && m.mode() & 0o077 == 0,
        "unsafe_private_directory",
    )
}

const MAX_PRIVATE_READ: u64 = 2 * 1024 * 1024;

pub fn private_read(path: &Path) -> SafeResult<String> {
    let mut f = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "private_read_open_failed")?;
    let m = f.metadata().map_err(|_| "private_read_metadata_failed")?;
    check(
        m.is_file()
            && m.uid() == unsafe { libc::geteuid() }
            && m.mode() & 0o077 == 0
            && m.len() <= MAX_PRIVATE_READ,
        "unsafe_private_file",
    )?;
    let mut value = String::new();
    (&mut f)
        .take(MAX_PRIVATE_READ + 1)
        .read_to_string(&mut value)
        .map_err(|_| "private_read_failed")?;
    check(
        value.len() as u64 <= MAX_PRIVATE_READ,
        "private_read_too_large",
    )?;
    Ok(value)
}

pub fn atomic_private_write(path: &Path, bytes: &[u8]) -> SafeResult<()> {
    let parent = path.parent().ok_or("missing_parent")?;
    private_dir(parent)?;
    if path.exists() {
        let m = fs::symlink_metadata(path).map_err(|_| "target_metadata_failed")?;
        check(
            m.is_file() && !m.file_type().is_symlink() && m.uid() == unsafe { libc::geteuid() },
            "unsafe_write_target",
        )?;
    }
    let mut f = tempfile::NamedTempFile::new_in(parent).map_err(|_| "temporary_file_failed")?;
    f.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "file_permissions_failed")?;
    f.write_all(bytes).map_err(|_| "private_write_failed")?;
    f.as_file().sync_all().map_err(|_| "private_sync_failed")?;
    f.persist(path).map_err(|_| "private_publish_failed")?;
    fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|_| "directory_sync_failed")
}

/// `phone.toml` in the private state root. Secrets never live here: they are
/// read from `secrets/<name>` (mode 0600) or the matching environment variable.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhoneConfig {
    pub enabled: bool,
    pub port: u16,
    pub public_base: String,
    pub account_sid: String,
    /// The Twilio number that answers inbound calls and places outbound calls.
    pub from_number: String,
    /// Optional owner phone that forwards unanswered calls here. Direct calls to
    /// `from_number` are always admitted; this only labels forwarded ones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forwarded_from: Option<String>,
    /// Outbound caller ID: `from_number` or a number verified with Twilio as an
    /// outgoing caller ID (for example the owner's own cell).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_id: Option<String>,
    /// When set, outbound calls may only go to these E.164 numbers. Agents
    /// cannot change it; only the owner editing `phone.toml` can.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_destinations: Option<Vec<String>>,
    #[serde(default = "default_max_duration")]
    pub max_duration_secs: u64,
    /// Canonical admission policy for new inbound calls.
    #[serde(default)]
    pub recording_consent: RecordingConsentMode,
    /// Shown to callers and called parties as the person being represented.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_name: Option<String>,
    /// Hook run once per finished summary and per downloaded recording. Absent
    /// means everything stays local (still readable through MCP and the CLI).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notify: Option<NotifyConfig>,
    /// Post-call summarizer. Absent means notifications carry the transcript only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryConfig>,
    /// Which engine bridges call audio. Absent means the integrated Realtime session.
    #[serde(default, skip_serializing_if = "VoiceConfig::is_default")]
    pub voice: VoiceConfig,
}

fn default_max_duration() -> u64 {
    300
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifyConfig {
    /// argv; the event JSON is written to the command's stdin. No shell is used.
    pub command: Vec<String>,
    #[serde(default = "default_notify_timeout")]
    pub timeout_secs: u64,
}

fn default_notify_timeout() -> u64 {
    60
}

impl NotifyConfig {
    fn validate(&self) -> SafeResult<()> {
        check(
            !self.command.is_empty()
                && self.command.len() <= 32
                && Path::new(&self.command[0]).is_absolute()
                && self
                    .command
                    .iter()
                    .all(|arg| arg.len() <= 4096 && !arg.contains('\0'))
                && (1..=600).contains(&self.timeout_secs),
            "notify_config_invalid",
        )
    }
}

/// Any OpenAI-compatible chat completions endpoint (Ollama, llama.cpp, OpenAI,
/// OpenRouter, ...). `api_key_secret` names a file under `secrets/`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SummaryConfig {
    pub url: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_secret: Option<String>,
    /// Extra JSON fields merged into each request, e.g. `reasoning_effort = "none"`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl SummaryConfig {
    fn validate(&self) -> SafeResult<()> {
        let url = url::Url::parse(&self.url).map_err(|_| "summary_endpoint_invalid")?;
        check(
            matches!(url.scheme(), "https" | "http")
                && url.username().is_empty()
                && url.password().is_none()
                && valid_model_name(&self.model)
                && self.api_key_secret.as_deref().is_none_or(valid_secret_name),
            "summary_config_invalid",
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VoiceEngine {
    /// One integrated OpenAI Realtime session (speech in, model, speech out).
    #[default]
    Realtime,
    /// Separate speech-to-text, chat model, and text-to-speech stages.
    Cascade,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceConfig {
    #[serde(default)]
    pub engine: VoiceEngine,
    /// Kept when the engine is switched back so the fallback is one line to undo.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cascade: Option<CascadeConfig>,
}

impl VoiceConfig {
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsEngine {
    /// An OpenAI-compatible `/v1/audio/speech` endpoint at `tts_url`.
    #[default]
    Http,
    /// macOS `say` (fast, local, and robotic with voices such as Fred or Zarvox).
    Say,
}

/// Endpoints for the cascade. Speech recognition and synthesis speak the common
/// OpenAI-compatible HTTP shapes (`/v1/audio/transcriptions`, `/v1/audio/speech`,
/// `/v1/chat/completions`), so a local Kokoro or Whisper server, a local model
/// server, or OpenAI itself can fill any stage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CascadeConfig {
    pub stt_url: String,
    #[serde(default = "default_stt_model")]
    pub stt_model: String,
    /// Optional ISO-639-1 hint passed to the recognizer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stt_language: Option<String>,
    pub llm_url: String,
    pub llm_model: String,
    /// Extra JSON fields merged into each chat request, e.g. `reasoning_effort = "none"`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub llm_extra: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub tts: TtsEngine,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tts_url: Option<String>,
    #[serde(default = "default_tts_model")]
    pub tts_model: String,
    #[serde(default = "default_tts_voice")]
    pub tts_voice: String,
    /// Words per minute for `say`; ignored by HTTP synthesis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tts_rate: Option<u32>,
}

fn default_stt_model() -> String {
    "gpt-transcribe".into()
}

fn default_tts_model() -> String {
    "kokoro".into()
}

fn default_tts_voice() -> String {
    "af_heart".into()
}

/// Endpoints must be `https`, or plain `http` to this machine. The OpenAI
/// credential is only ever attached to `https://api.openai.com` (see cascade).
pub fn validate_endpoint(value: &str) -> SafeResult<url::Url> {
    let url = url::Url::parse(value).map_err(|_| "cascade_endpoint_invalid")?;
    let loopback = match url.host() {
        Some(url::Host::Domain(name)) => name == "localhost",
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    };
    check(
        (url.scheme() == "https" || (url.scheme() == "http" && loopback))
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && value.len() <= 512,
        "cascade_endpoint_unsafe",
    )?;
    Ok(url)
}

fn valid_model_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

impl CascadeConfig {
    pub fn validate(&self) -> SafeResult<()> {
        validate_endpoint(&self.stt_url)?;
        validate_endpoint(&self.llm_url)?;
        match self.tts {
            TtsEngine::Http => {
                validate_endpoint(self.tts_url.as_deref().ok_or("cascade_tts_url_missing")?)?;
            }
            TtsEngine::Say => check(
                self.tts_voice
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, ' ' | '-' | '_' | '(' | ')'))
                    && self.tts_rate.is_none_or(|rate| (80..=500).contains(&rate)),
                "cascade_say_voice_invalid",
            )?,
        }
        check(
            valid_model_name(&self.stt_model)
                && valid_model_name(&self.llm_model)
                && valid_model_name(&self.tts_model)
                && valid_model_name(&self.tts_voice)
                && self.stt_language.as_deref().is_none_or(|l| {
                    (2..=8).contains(&l.len())
                        && l.chars().all(|c| c.is_ascii_alphabetic() || c == '-')
                }),
            "cascade_model_invalid",
        )
    }
}

impl VoiceConfig {
    fn validate(&self) -> SafeResult<()> {
        if let Some(cascade) = &self.cascade {
            cascade.validate()?;
        }
        check(
            self.engine != VoiceEngine::Cascade || self.cascade.is_some(),
            "cascade_config_missing",
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingConsentMode {
    #[default]
    Explicit,
    Notice,
}

pub struct Settings {
    pub enabled: bool,
    pub port: u16,
    pub public_base: String,
    pub account_sid: String,
    pub auth_token: String,
    pub from_number: String,
    pub forwarded_from: Option<String>,
    pub caller_id: String,
    pub allowed_destinations: Option<Vec<String>>,
    pub max_duration_secs: u64,
    pub recording_consent: RecordingConsentMode,
    pub owner_name: Option<String>,
    pub notify: Option<NotifyConfig>,
    pub summary: Option<SummaryConfig>,
    pub recording_dir: PathBuf,
    /// OpenAI key for the Realtime engine and api.openai.com stages; may be empty.
    pub api_key: String,
    pub instructions: String,
    pub voice: VoiceConfig,
}

fn valid_secret_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// `secrets/<name>` under the root (private, 0600), else the uppercased
/// environment variable. `Ok(None)` when neither exists.
pub fn read_secret(root: &Path, name: &str) -> SafeResult<Option<String>> {
    check(valid_secret_name(name), "secret_name_invalid")?;
    let path = root.join("secrets").join(name);
    let value = if path.exists() {
        existing_private_dir(&root.join("secrets"))?;
        private_read(&path)?
    } else {
        match std::env::var(name.to_ascii_uppercase()) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        }
    };
    let value = value.trim().to_owned();
    check(
        !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_whitespace),
        "secret_invalid",
    )?;
    Ok(Some(value))
}

pub fn write_secret(root: &Path, name: &str, value: &str) -> SafeResult<()> {
    check(valid_secret_name(name), "secret_name_invalid")?;
    let value = value.trim();
    check(
        !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_whitespace),
        "secret_invalid",
    )?;
    private_dir(root)?;
    private_dir(&root.join("secrets"))?;
    atomic_private_write(&root.join("secrets").join(name), value.as_bytes())
}

pub fn read_config(root: &Path) -> SafeResult<PhoneConfig> {
    toml::from_str(&private_read(&root.join("phone.toml"))?).map_err(|_| "phone_config_invalid")
}

pub fn load(root: &Path) -> SafeResult<Settings> {
    private_dir(root)?;
    let p = read_config(root)?;
    p.voice.validate()?;
    if let Some(notify) = &p.notify {
        notify.validate()?;
    }
    if let Some(summary) = &p.summary {
        summary.validate()?;
    }
    let base = url::Url::parse(&p.public_base).map_err(|_| "public_url_invalid")?;
    check(
        base.scheme() == "https"
            && base.username().is_empty()
            && base.password().is_none()
            && base.host_str().is_some()
            && base.query().is_none()
            && base.fragment().is_none()
            && base.path() == "/"
            && base.port().is_none(),
        "unsafe_public_url",
    )?;
    check(
        p.port > 1024 && (1..=1800).contains(&p.max_duration_secs),
        "invalid_call_limits",
    )?;
    check(
        crate::protocol::valid_sid(&p.account_sid, "AC")
            && e164(&p.from_number)
            && p.forwarded_from.as_deref().is_none_or(e164)
            && p.caller_id.as_deref().is_none_or(e164)
            && p.allowed_destinations
                .as_ref()
                .is_none_or(|list| list.len() <= 1000 && list.iter().all(|n| e164(n))),
        "invalid_phone_identity",
    )?;
    check(
        p.owner_name.as_deref().is_none_or(|name| {
            !name.trim().is_empty() && name.len() <= 80 && !name.chars().any(char::is_control)
        }),
        "owner_name_invalid",
    )?;
    let auth_token =
        read_secret(root, "twilio_auth_token")?.ok_or("phone_credential_unavailable")?;
    check(
        (20..=1024).contains(&auth_token.len()),
        "phone_credential_invalid",
    )?;
    let api_key = read_secret(root, "openai_api_key")?.unwrap_or_default();
    check(
        p.voice.engine != VoiceEngine::Realtime || !api_key.is_empty(),
        "voice_key_missing",
    )?;
    let instructions = private_read(&root.join("screening.md"))?;
    check(
        !instructions.is_empty() && instructions.len() < 64_000,
        "screening_policy_invalid",
    )?;
    let from_number = p.from_number.clone();
    Ok(Settings {
        enabled: p.enabled,
        port: p.port,
        public_base: p.public_base.trim_end_matches('/').to_owned(),
        account_sid: p.account_sid,
        auth_token,
        from_number: from_number.clone(),
        forwarded_from: p.forwarded_from,
        caller_id: p.caller_id.unwrap_or_else(|| from_number.clone()),
        allowed_destinations: p.allowed_destinations,
        max_duration_secs: p.max_duration_secs,
        recording_consent: p.recording_consent,
        owner_name: p.owner_name,
        notify: p.notify,
        summary: p.summary,
        recording_dir: root.join("recordings"),
        api_key,
        instructions,
        voice: p.voice,
    })
}

pub fn e164(s: &str) -> bool {
    let b = s.as_bytes();
    (3..=16).contains(&b.len())
        && b[0] == b'+'
        && (b'1'..=b'9').contains(&b[1])
        && b[2..].iter().all(u8::is_ascii_digit)
}

pub fn open_db(root: &Path) -> SafeResult<Connection> {
    open_db_with_timeout(root, Duration::from_secs(5))
}

pub fn open_db_with_timeout(root: &Path, busy_timeout: Duration) -> SafeResult<Connection> {
    private_dir(root)?;
    let path = root.join("phone.sqlite");
    if path.exists() {
        let m = fs::symlink_metadata(&path).map_err(|_| "database_metadata_failed")?;
        check(
            m.is_file()
                && !m.file_type().is_symlink()
                && m.uid() == unsafe { libc::geteuid() }
                && m.mode() & 0o077 == 0,
            "unsafe_database",
        )?;
    } else {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| "database_create_failed")?;
    }
    let c = Connection::open(path).map_err(|_| "database_open_failed")?;
    c.busy_timeout(busy_timeout)
        .map_err(|_| "database_timeout_failed")?;
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
        CREATE TABLE IF NOT EXISTS calls (
            call_sid TEXT PRIMARY KEY, account_sid TEXT NOT NULL, from_candidate TEXT NOT NULL,
            consent INTEGER, consent_token TEXT NOT NULL UNIQUE, media_token TEXT UNIQUE,
            created_ms INTEGER NOT NULL, phase TEXT NOT NULL, transcript TEXT, outcome TEXT,
            summary_status TEXT NOT NULL DEFAULT 'pending', summary_text TEXT, summary_message_id INTEGER
        );").map_err(|_| "database_initialize_failed")?;
    Ok(c)
}

/// Synthetic private roots for worker tests. Nothing here touches a real
/// configuration, credential, database or network service.
#[cfg(test)]
pub(crate) mod fixture {
    use super::*;

    pub(crate) const ACCOUNT: &str = "AC00000000000000000000000000000000";

    pub(crate) fn config() -> PhoneConfig {
        PhoneConfig {
            enabled: true,
            port: 43335,
            public_base: "https://phone.test.invalid".into(),
            account_sid: ACCOUNT.into(),
            from_number: "+15550001001".into(),
            forwarded_from: Some("+15550001002".into()),
            caller_id: None,
            allowed_destinations: None,
            max_duration_secs: 180,
            recording_consent: RecordingConsentMode::Explicit,
            owner_name: None,
            notify: None,
            summary: None,
            voice: VoiceConfig::default(),
        }
    }

    pub(crate) fn root() -> (tempfile::TempDir, PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root = directory.path().join("phone");
        private_dir(&root).unwrap();
        write_secret(&root, "twilio_auth_token", "fixture-twilio-auth-token").unwrap();
        write_secret(&root, "openai_api_key", "fixture-model-key-no-network").unwrap();
        write_config(&root, &config());
        atomic_private_write(&root.join("screening.md"), b"Synthetic screening").unwrap();
        (directory, root)
    }

    pub(crate) fn write_config(root: &Path, config: &PhoneConfig) {
        atomic_private_write(
            &root.join("phone.toml"),
            toml::to_string(config).unwrap().as_bytes(),
        )
        .unwrap();
    }

    pub(crate) fn edit_config(root: &Path, edit: impl FnOnce(&mut PhoneConfig)) {
        let mut config = read_config(root).unwrap();
        edit(&mut config);
        write_config(root, &config);
    }
}

#[cfg(test)]
mod bounded_private_io_tests {
    use super::*;
    use std::{ffi::CString, time::Instant};

    #[test]
    fn fifo_is_rejected_before_read_and_oversize_private_files_are_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("policy-fifo");
        let encoded = CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
        let started = Instant::now();
        assert!(private_read(&fifo).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        let large = dir.path().join("large-policy");
        let f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&large)
            .unwrap();
        f.set_len(MAX_PRIVATE_READ + 1).unwrap();
        assert!(private_read(&large).is_err());
    }

    #[test]
    fn short_database_budget_applies_to_initial_pragmas() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("private");
        private_dir(&root).unwrap();
        let db = open_db(&root).unwrap();
        db.execute_batch("PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE;")
            .unwrap();
        let started = Instant::now();
        assert!(open_db_with_timeout(&root, Duration::from_millis(40)).is_err());
        assert!(started.elapsed() < Duration::from_millis(600));
        db.execute_batch("ROLLBACK;").unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CASCADE: &str = r#"
engine = "cascade"
[cascade]
stt_url = "http://127.0.0.1:8000/v1/audio/transcriptions"
llm_url = "https://api.openai.com/v1/chat/completions"
llm_model = "chat-model"
tts_url = "http://127.0.0.1:8880/v1/audio/speech"
"#;

    #[test]
    fn absent_voice_settings_keep_the_realtime_engine_and_stay_absent_on_write() {
        let voice = VoiceConfig::default();
        assert_eq!(voice.engine, VoiceEngine::Realtime);
        assert!(voice.is_default());
        assert!(voice.validate().is_ok());
        let parsed: VoiceConfig = toml::from_str("").unwrap();
        assert!(parsed.is_default());
    }

    #[test]
    fn cascade_settings_parse_with_documented_defaults() {
        let voice: VoiceConfig = toml::from_str(CASCADE).unwrap();
        assert_eq!(voice.engine, VoiceEngine::Cascade);
        let cascade = voice.cascade.as_ref().unwrap();
        assert_eq!(cascade.tts, TtsEngine::Http);
        assert_eq!(cascade.tts_model, "kokoro");
        assert_eq!(cascade.tts_voice, "af_heart");
        assert_eq!(cascade.stt_model, "gpt-transcribe");
        assert!(voice.validate().is_ok());
        assert!(!voice.is_default());
        let round_trip: VoiceConfig = toml::from_str(&toml::to_string(&voice).unwrap()).unwrap();
        assert_eq!(round_trip, voice);
    }

    #[test]
    fn say_synthesis_needs_no_url_and_rejects_unsafe_voices() {
        let base = CASCADE.replace(
            "tts_url = \"http://127.0.0.1:8880/v1/audio/speech\"",
            "tts = \"say\"\ntts_voice = \"Fred\"\nllm_extra = { reasoning_effort = \"none\" }",
        );
        let voice: VoiceConfig = toml::from_str(&base).unwrap();
        assert!(voice.validate().is_ok());
        let cascade = voice.cascade.as_ref().unwrap();
        assert_eq!(cascade.llm_extra["reasoning_effort"], "none");
        let bad: VoiceConfig = toml::from_str(&base.replace("Fred", "Fred; rm")).unwrap();
        assert_eq!(bad.validate(), Err("cascade_say_voice_invalid"));
        let missing: VoiceConfig = toml::from_str(
            &CASCADE.replace("tts_url = \"http://127.0.0.1:8880/v1/audio/speech\"", ""),
        )
        .unwrap();
        assert_eq!(missing.validate(), Err("cascade_tts_url_missing"));
    }

    #[test]
    fn switching_back_to_realtime_keeps_the_cascade_section_valid_and_dormant() {
        let mut voice: VoiceConfig = toml::from_str(CASCADE).unwrap();
        voice.engine = VoiceEngine::Realtime;
        assert!(voice.validate().is_ok());
        assert!(!voice.is_default());
    }

    #[test]
    fn settings_load_secrets_from_private_files() {
        let (_directory, root) = fixture::root();
        let settings = load(&root).unwrap();
        assert_eq!(settings.account_sid, fixture::ACCOUNT);
        assert_eq!(settings.auth_token, "fixture-twilio-auth-token");
        assert!(settings.notify.is_none());
        fs::set_permissions(
            root.join("secrets/twilio_auth_token"),
            fs::Permissions::from_mode(0o644),
        )
        .unwrap();
        assert!(load(&root).is_err());
    }

    #[test]
    fn realtime_requires_a_key_but_cascade_does_not() {
        let (_directory, root) = fixture::root();
        fs::remove_file(root.join("secrets/openai_api_key")).unwrap();
        assert!(matches!(load(&root), Err("voice_key_missing")));
        fixture::edit_config(&root, |config| {
            config.voice = toml::from_str(CASCADE).unwrap();
        });
        assert!(load(&root).unwrap().api_key.is_empty());
    }

    #[test]
    fn notify_hooks_must_be_absolute_argv() {
        let (_directory, root) = fixture::root();
        fixture::edit_config(&root, |config| {
            config.notify = Some(NotifyConfig {
                command: vec!["notify.sh".into()],
                timeout_secs: 60,
            });
        });
        assert!(matches!(load(&root), Err("notify_config_invalid")));
        fixture::edit_config(&root, |config| {
            config.notify.as_mut().unwrap().command = vec!["/usr/bin/true".into()];
        });
        assert!(load(&root).is_ok());
    }

    #[test]
    fn invalid_cascade_settings_are_rejected() {
        let missing: VoiceConfig = toml::from_str("engine = \"cascade\"").unwrap();
        assert_eq!(missing.validate(), Err("cascade_config_missing"));
        assert!(toml::from_str::<VoiceConfig>("engine = \"other\"").is_err());
        assert!(toml::from_str::<VoiceConfig>("surprise = 1").is_err());
        assert!(toml::from_str::<VoiceConfig>("[cascade]\nstt_url = \"x\"").is_err());

        for (from, to) in [
            (
                "http://127.0.0.1:8880/v1/audio/speech",
                "http://tts.example.com/v1/audio/speech",
            ),
            ("llm_model = \"chat-model\"", "llm_model = \"\""),
            ("llm_model = \"chat-model\"", "llm_model = \"bad\\nmodel\""),
        ] {
            let text = CASCADE.replace(from, to);
            let voice: VoiceConfig = toml::from_str(&text).unwrap();
            assert!(voice.validate().is_err(), "{to}");
        }
        let language = format!("{CASCADE}stt_language = \"english is fine!\"\n");
        let voice: VoiceConfig = toml::from_str(&language).unwrap();
        assert_eq!(voice.validate(), Err("cascade_model_invalid"));
    }
}
