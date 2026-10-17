//! Recording delivery has a durable outbox separate from the call lifecycle.
//! Only the authenticated, consent-checking ingress may enqueue identifiers.
//! Each recording is downloaded into the private archive, deleted from Twilio,
//! and then announced once through the notify hook (when one is configured).

use crate::common::{self, SafeResult, Settings};
use crate::notify::{self, Outcome};
use chrono::Utc;
use reqwest::{Client, Response, StatusCode, redirect::Policy};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::json;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_AUDIO_BYTES: usize = 49_000_000;
const MAX_DOWNLOAD_ATTEMPTS: i64 = 6;

/// Create this module's canonical delivery store without recovering live work.
pub fn initialize(conn: &Connection) -> SafeResult<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS recording_outbox (
            recording_sid TEXT PRIMARY KEY,
            account_sid TEXT NOT NULL,
            call_sid TEXT NOT NULL,
            provider_status TEXT NOT NULL CHECK(provider_status IN ('in-progress','completed','absent')),
            state TEXT NOT NULL CHECK(state IN ('waiting','queued','downloading','ready','sending','sent','stored','uncertain','failed','absent')),
            created_ms INTEGER NOT NULL,
            updated_ms INTEGER NOT NULL,
            next_attempt_ms INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            notify_attempts INTEGER NOT NULL DEFAULT 0,
            local_name TEXT,
            last_error TEXT
        );
        CREATE INDEX IF NOT EXISTS recording_outbox_due
            ON recording_outbox(state,next_attempt_ms,created_ms);",
    )
    .map_err(|_| "recording_schema_failed")
}

/// Caller must authenticate the callback and verify this call's recording consent.
/// A duplicate callback never changes the identity a job was first admitted with.
pub fn enqueue(
    conn: &Connection,
    account_sid: &str,
    call_sid: &str,
    recording_sid: &str,
    status: &str,
) -> SafeResult<()> {
    if !valid_sid(account_sid, "AC")
        || !valid_sid(call_sid, "CA")
        || !valid_sid(recording_sid, "RE")
        || !matches!(status, "in-progress" | "completed" | "absent")
    {
        return Err("invalid_recording_callback");
    }
    let now = Utc::now().timestamp_millis();
    let state = match status {
        "completed" => "queued",
        "absent" => "absent",
        _ => "waiting",
    };
    let tx = conn
        .unchecked_transaction()
        .map_err(|_| "recording_transaction_failed")?;
    tx.execute(
        "INSERT OR IGNORE INTO recording_outbox
            (recording_sid,account_sid,call_sid,provider_status,state,created_ms,updated_ms,next_attempt_ms)
         VALUES (?1,?2,?3,?4,?5,?6,?6,?6)",
        params![recording_sid, account_sid, call_sid, status, state, now],
    )
    .map_err(|_| "recording_enqueue_failed")?;
    let existing: (String, String, String) = tx
        .query_row(
            "SELECT account_sid,call_sid,provider_status FROM recording_outbox WHERE recording_sid=?1",
            [recording_sid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|_| "recording_lookup_failed")?;
    if existing.0 != account_sid || existing.1 != call_sid {
        return Err("recording_identifier_conflict");
    }
    if existing.2 != status && status != "in-progress" {
        if existing.2 != "in-progress" {
            return Err("recording_status_conflict");
        }
        tx.execute(
            "UPDATE recording_outbox SET provider_status=?2,state=?3,updated_ms=?4,next_attempt_ms=?4
             WHERE recording_sid=?1 AND state='waiting'",
            params![recording_sid, status, state, now],
        )
        .map_err(|_| "recording_enqueue_failed")?;
    }
    tx.commit().map_err(|_| "recording_commit_failed")
}

/// Keep the lock across polling and network awaits; startup recovery must never
/// reinterpret another live worker's in-flight send as a crashed send.
pub async fn run(root: PathBuf) -> SafeResult<()> {
    let _worker_lock = worker_lock(&root)?;
    prepare(&root)?;
    let client = http_client()?;
    loop {
        tick_locked(&root, &client).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// A single explicit iteration, mutually exclusive with the persistent worker.
pub async fn tick(root: &Path) -> SafeResult<()> {
    let _worker_lock = worker_lock(root)?;
    prepare(root)?;
    tick_locked(root, &http_client()?).await
}

fn prepare(root: &Path) -> SafeResult<()> {
    let conn = common::open_db(root)?;
    initialize(&conn)?;
    recover(&conn)
}

fn recover(conn: &Connection) -> SafeResult<()> {
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         UPDATE recording_outbox SET state='uncertain',last_error='interrupted_send'
             WHERE state='sending';
         UPDATE recording_outbox SET state='queued' WHERE state='downloading';
         COMMIT;",
    )
    .map_err(|_| "recording_recovery_failed")
}

fn worker_lock(root: &Path) -> SafeResult<File> {
    common::private_dir(root)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root.join("recording-worker.lock"))
        .map_err(|_| "recording_lock_failed")?;
    verify_private_file(&file)?;
    // The File owns this descriptor and stays open throughout the worker lifetime.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        return Err("recording_worker_busy");
    }
    Ok(file)
}

fn http_client() -> SafeResult<Client> {
    Client::builder()
        .https_only(true)
        .no_proxy()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(90))
        .build()
        .map_err(|_| "recording_http_client_failed")
}

struct Job {
    recording_sid: String,
    account_sid: String,
    call_sid: String,
    state: String,
    created_ms: i64,
    attempts: i64,
    notify_attempts: i64,
    local_name: Option<String>,
}

async fn tick_locked(root: &Path, client: &Client) -> SafeResult<()> {
    let settings = common::load(root)?;
    if !settings.enabled {
        return Ok(());
    }
    let Some(job) = claim_job(root, &settings)? else {
        return Ok(());
    };
    match job.state.as_str() {
        "queued" => download_job(root, client, &settings, &job).await,
        _ => send_job(root, &settings, &job).await,
    }
}

fn claim_job(root: &Path, settings: &Settings) -> SafeResult<Option<Job>> {
    let mut conn = common::open_db(root)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| "recording_transaction_failed")?;
    suppress_declined(&tx)?;
    let job = tx
        .query_row(
            "SELECT r.recording_sid,r.account_sid,r.call_sid,r.state,r.created_ms,r.attempts,
                    r.notify_attempts,r.local_name
             FROM recording_outbox r JOIN calls c
               ON c.call_sid=r.call_sid AND c.account_sid=r.account_sid
             WHERE r.state IN ('queued','ready') AND r.next_attempt_ms<=?1
               AND c.consent=1 AND c.phase='ended' AND c.outcome IS NOT NULL
               AND c.outcome!='service_interrupted'
             ORDER BY r.next_attempt_ms,r.created_ms LIMIT 1",
            params![Utc::now().timestamp_millis()],
            |row| {
                Ok(Job {
                    recording_sid: row.get(0)?,
                    account_sid: row.get(1)?,
                    call_sid: row.get(2)?,
                    state: row.get(3)?,
                    created_ms: row.get(4)?,
                    attempts: row.get(5)?,
                    notify_attempts: row.get(6)?,
                    local_name: row.get(7)?,
                })
            },
        )
        .optional()
        .map_err(|_| "recording_lookup_failed")?;
    let Some(mut job) = job else {
        tx.commit().map_err(|_| "recording_commit_failed")?;
        return Ok(None);
    };
    if !valid_sid(&job.account_sid, "AC")
        || !valid_sid(&job.call_sid, "CA")
        || !valid_sid(&job.recording_sid, "RE")
        || settings.account_sid != job.account_sid
    {
        return Err("recording_configuration_mismatch");
    }
    if job.state == "queued" {
        if job.attempts >= MAX_DOWNLOAD_ATTEMPTS {
            tx.execute(
                "UPDATE recording_outbox SET state='failed',last_error='download_retries_exhausted' WHERE recording_sid=?1",
                [&job.recording_sid],
            )
            .map_err(|_| "recording_update_failed")?;
            tx.commit().map_err(|_| "recording_commit_failed")?;
            return Ok(None);
        }
        tx.execute(
            "UPDATE recording_outbox SET state='downloading',attempts=attempts+1,updated_ms=?2
             WHERE recording_sid=?1 AND state='queued'",
            params![job.recording_sid, Utc::now().timestamp_millis()],
        )
        .map_err(|_| "recording_claim_failed")?;
        job.attempts += 1;
    }
    tx.commit().map_err(|_| "recording_commit_failed")?;
    Ok(Some(job))
}

// A carrier completion callback can arrive before the bridge's final outcome.
// The calls row remains authoritative: never download or send until that outcome
// has been persisted, including any withdrawal of recording consent.
fn suppress_declined(conn: &Connection) -> SafeResult<()> {
    conn.execute(
        "UPDATE recording_outbox SET state='failed',last_error='recording_consent_withdrawn'
         WHERE state IN ('waiting','queued','ready','downloading') AND EXISTS (
           SELECT 1 FROM calls c WHERE c.call_sid=recording_outbox.call_sid
             AND c.account_sid=recording_outbox.account_sid
             AND (c.consent=0 OR c.outcome='service_interrupted'))",
        [],
    )
    .map_err(|_| "recording_consent_update_failed")?;
    Ok(())
}

fn claim_send(conn: &Connection, recording_sid: &str) -> SafeResult<bool> {
    let claimed = conn
        .execute(
            "UPDATE recording_outbox SET state='sending',updated_ms=?2,last_error=NULL
         WHERE recording_sid=?1 AND state='ready' AND EXISTS (
           SELECT 1 FROM calls c WHERE c.call_sid=recording_outbox.call_sid
             AND c.account_sid=recording_outbox.account_sid AND c.consent=1
             AND c.phase='ended' AND c.outcome IS NOT NULL
             AND c.outcome!='service_interrupted')",
            params![recording_sid, Utc::now().timestamp_millis()],
        )
        .map_err(|_| "recording_send_claim_failed")?;
    Ok(claimed == 1)
}

enum FetchFailure {
    Retry(&'static str),
    Stop(&'static str),
}

async fn download_job(
    root: &Path,
    client: &Client,
    settings: &Settings,
    job: &Job,
) -> SafeResult<()> {
    let result = fetch_audio(client, settings, &job.recording_sid).await;
    match result {
        Ok(bytes) => {
            keep_download(root, settings, job, &bytes)?;
            // The private archive is now the copy of record; Twilio's is removed.
            delete_remote(client, settings, &job.recording_sid).await;
            Ok(())
        }
        Err(FetchFailure::Retry(error)) if job.attempts < MAX_DOWNLOAD_ATTEMPTS => {
            defer(root, job, "queued", error, job.attempts, false)
        }
        Err(FetchFailure::Retry(error) | FetchFailure::Stop(error)) => {
            finish(root, job, "failed", error)
        }
    }
}

fn keep_download(root: &Path, settings: &Settings, job: &Job, bytes: &[u8]) -> SafeResult<()> {
    let name = format!("{}.mp3", job.recording_sid);
    if let Err(error) = archive(&settings.recording_dir, &name, bytes) {
        return finish(root, job, "failed", error);
    }
    // Without a hook the private archive is the whole delivery.
    let state = if settings.notify.is_some() { "ready" } else { "stored" };
    let conn = common::open_db(root)?;
    conn.execute(
        "UPDATE recording_outbox SET state=?4,local_name=?2,last_error=NULL,
            updated_ms=?3,next_attempt_ms=?3 WHERE recording_sid=?1 AND state='downloading'",
        params![job.recording_sid, name, Utc::now().timestamp_millis(), state],
    )
    .map_err(|_| "recording_archive_receipt_failed")?;
    Ok(())
}

/// Best effort: a failed delete leaves Twilio's copy, which the owner can remove.
async fn delete_remote(client: &Client, settings: &Settings, recording_sid: &str) {
    let Ok(url) = media_url(&settings.account_sid, recording_sid) else {
        return;
    };
    let url = url.trim_end_matches(".mp3").to_owned() + ".json";
    if !client
        .delete(url)
        .basic_auth(&settings.account_sid, Some(&settings.auth_token))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())

    {
        eprintln!("recording_remote_delete_failed");
    }
}

fn media_url(account_sid: &str, recording_sid: &str) -> SafeResult<String> {
    if !valid_sid(account_sid, "AC") || !valid_sid(recording_sid, "RE") {
        return Err("invalid_recording_identifier");
    }
    Ok(format!(
        "https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Recordings/{recording_sid}.mp3"
    ))
}

async fn fetch_audio(
    client: &Client,
    settings: &Settings,
    recording_sid: &str,
) -> Result<Vec<u8>, FetchFailure> {
    let url = media_url(&settings.account_sid, recording_sid).map_err(FetchFailure::Stop)?;
    let response = client
        .get(url)
        .basic_auth(&settings.account_sid, Some(&settings.auth_token))
        .send()
        .await
        .map_err(|_| FetchFailure::Retry("recording_download_transport_failed"))?;
    let status = response.status();
    if status.is_redirection() {
        return Err(FetchFailure::Stop("recording_media_redirect_blocked"));
    }
    if status == StatusCode::NOT_FOUND
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        return Err(FetchFailure::Retry(
            "recording_media_temporarily_unavailable",
        ));
    }
    if !status.is_success() {
        return Err(FetchFailure::Stop("recording_media_rejected"));
    }
    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if !mime.is_some_and(|value| value.eq_ignore_ascii_case("audio/mpeg")) {
        return Err(FetchFailure::Stop("recording_media_type_rejected"));
    }
    let bytes = read_capped(response, MAX_AUDIO_BYTES)
        .await
        .map_err(|error| match error {
            "recording_response_interrupted" => FetchFailure::Retry(error),
            _ => FetchFailure::Stop(error),
        })?;
    if !mp3_header(&bytes) {
        return Err(FetchFailure::Stop("recording_media_invalid_mp3"));
    }
    Ok(bytes)
}

async fn read_capped(mut response: Response, cap: usize) -> SafeResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > cap as u64)
    {
        return Err("recording_response_too_large");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "recording_response_interrupted")?
    {
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > cap)
        {
            return Err("recording_response_too_large");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn archive(directory: &Path, name: &str, bytes: &[u8]) -> SafeResult<()> {
    common::private_dir(directory)?;
    let destination = directory.join(name);
    let mut temp = tempfile::Builder::new()
        .prefix(".recording-")
        .tempfile_in(directory)
        .map_err(|_| "recording_archive_create_failed")?;
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| "recording_archive_permissions_failed")?;
    temp.write_all(bytes)
        .map_err(|_| "recording_archive_write_failed")?;
    temp.as_file()
        .sync_all()
        .map_err(|_| "recording_archive_sync_failed")?;
    match temp.persist_noclobber(&destination) {
        Ok(_) => {}
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            // A crash between file creation and the SQLite receipt can leave this
            // exact file. Prove byte equality; never overwrite or send another file.
            if read_archive(&destination)? != bytes {
                return Err("recording_archive_conflict");
            }
        }
        Err(_) => return Err("recording_archive_commit_failed"),
    }
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|_| "recording_archive_directory_sync_failed")
}

fn verify_private_file(file: &File) -> SafeResult<()> {
    let metadata = file
        .metadata()
        .map_err(|_| "recording_file_metadata_failed")?;
    // geteuid has no inputs and cannot fail; only this service user's files qualify.
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err("recording_file_not_private");
    }
    Ok(())
}

fn read_archive(path: &Path) -> SafeResult<Vec<u8>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| "recording_archive_open_failed")?;
    verify_private_file(&file)?;
    let mut bytes = Vec::new();
    file.take((MAX_AUDIO_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "recording_archive_read_failed")?;
    if bytes.len() > MAX_AUDIO_BYTES || !mp3_header(&bytes) {
        return Err("recording_archive_invalid");
    }
    Ok(bytes)
}

async fn send_job(root: &Path, settings: &Settings, job: &Job) -> SafeResult<()> {
    let Some(hook) = settings.notify.as_ref() else {
        return finish(root, job, "stored", "");
    };
    let expected_name = format!("{}.mp3", job.recording_sid);
    if job.local_name.as_deref() != Some(expected_name.as_str()) {
        return finish(root, job, "failed", "recording_archive_name_mismatch");
    }
    let path = settings.recording_dir.join(&expected_name);
    let bytes = match read_archive(&path) {
        Ok(bytes) => bytes,
        Err(error) => return finish(root, job, "failed", error),
    };
    let conn = common::open_db(root)?;
    let (direction, from): (String, String) = conn
        .query_row(
            "SELECT CASE WHEN EXISTS(SELECT 1 FROM outbound_requests o WHERE o.call_sid=c.call_sid)
                    THEN 'outbound' ELSE 'inbound' END, c.from_candidate
             FROM calls c WHERE c.call_sid=?1",
            [&job.call_sid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|_| "recording_lookup_failed")?;
    if !claim_send(&conn, &job.recording_sid)? {
        return Err("recording_send_claim_lost");
    }
    drop(conn);
    let started = chrono::DateTime::<Utc>::from_timestamp_millis(job.created_ms)
        .map(|time| time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .ok_or("recording_timestamp_invalid")?;
    let event = json!({
        "event": "recording",
        "call_sid": job.call_sid,
        "direction": direction,
        "caller_id_unverified": common::e164(&from).then_some(from),
        "started": started,
        "recording_path": path.display().to_string(),
        "bytes": bytes.len(),
        "mime": "audio/mpeg",
    });
    match notify::send(hook, &event).await {
        Outcome::Sent => finish(root, job, "sent", ""),
        Outcome::Uncertain => finish(root, job, "uncertain", "notify_hook_timed_out"),
        Outcome::Failed if job.notify_attempts + 1 >= notify::MAX_ATTEMPTS => {
            finish(root, job, "failed", "notify_hook_failed")
        }
        Outcome::Failed => defer(
            root,
            job,
            "ready",
            "notify_hook_failed",
            job.notify_attempts + 1,
            true,
        ),
    }
}

fn defer(
    root: &Path,
    job: &Job,
    state: &str,
    error: &'static str,
    attempt: i64,
    notify_attempt: bool,
) -> SafeResult<()> {
    let delay_ms = 10_000_i64.saturating_mul(1_i64 << attempt.clamp(0, 6));
    let now = Utc::now().timestamp_millis();
    let conn = common::open_db(root)?;
    conn.execute(
        "UPDATE recording_outbox SET state=?2,last_error=?3,next_attempt_ms=?4,
             updated_ms=?5,notify_attempts=notify_attempts+?6 WHERE recording_sid=?1",
        params![
            job.recording_sid,
            state,
            error,
            now.saturating_add(delay_ms),
            now,
            i64::from(notify_attempt)
        ],
    )
    .map_err(|_| "recording_retry_update_failed")?;
    Ok(())
}

fn finish(root: &Path, job: &Job, state: &str, error: &'static str) -> SafeResult<()> {
    let conn = common::open_db(root)?;
    conn.execute(
        "UPDATE recording_outbox SET state=?2,last_error=?3,updated_ms=?4 WHERE recording_sid=?1",
        params![
            job.recording_sid,
            state,
            if error.is_empty() { None } else { Some(error) },
            Utc::now().timestamp_millis()
        ],
    )
    .map_err(|_| "recording_final_receipt_failed")?;
    Ok(())
}

/// Archived recording path for a call, if one was kept.
pub fn local_recording(root: &Path, call_sid: &str) -> SafeResult<Option<PathBuf>> {
    let conn = common::open_db(root)?;
    initialize(&conn)?;
    let name: Option<String> = conn
        .query_row(
            "SELECT local_name FROM recording_outbox WHERE call_sid=?1 AND local_name IS NOT NULL
             ORDER BY created_ms DESC LIMIT 1",
            [call_sid],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| "recording_lookup_failed")?
        .flatten();
    Ok(name.map(|name| root.join("recordings").join(name)))
}

fn valid_sid(value: &str, prefix: &str) -> bool {
    value.len() == 34
        && value.starts_with(prefix)
        && value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
}

fn mp3_header(bytes: &[u8]) -> bool {
    let offset = if bytes.starts_with(b"ID3") {
        if bytes.len() < 10
            || !matches!(bytes[3], 2..=4)
            || bytes[6..10].iter().any(|byte| byte & 0x80 != 0)
        {
            return false;
        }
        let size = bytes[6..10]
            .iter()
            .fold(0_usize, |size, byte| (size << 7) | usize::from(*byte));
        10 + size
            + if bytes[3] == 4 && bytes[5] & 0x10 != 0 {
                10
            } else {
                0
            }
    } else {
        0
    };
    let Some(audio) = bytes.get(offset..) else {
        return false;
    };
    audio.iter().take(4096).enumerate().any(|(index, _)| {
        let Some(frame) = audio.get(index..index + 4) else {
            return false;
        };
        frame[0] == 0xff
            && frame[1] & 0xe0 == 0xe0
            && (frame[1] >> 3) & 3 != 1
            && (frame[1] >> 1) & 3 == 1
            && !matches!(frame[2] >> 4, 0 | 15)
            && (frame[2] >> 2) & 3 != 3
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::fixture;

    const CALL: &str = "CA11111111111111111111111111111111";
    const REC: &str = "RE22222222222222222222222222222222";

    fn admitted(root: &Path, consent: bool) -> Connection {
        let db = common::open_db(root).unwrap();
        initialize(&db).unwrap();
        crate::outbound::initialize(&db).unwrap();
        db.execute(
            "INSERT INTO calls(call_sid,account_sid,from_candidate,consent,consent_token,created_ms,phase,outcome)
             VALUES(?1,?2,'+15550000001',?3,'nonce',0,'ended','remote_hangup')",
            params![CALL, fixture::ACCOUNT, consent],
        )
        .unwrap();
        db
    }

    fn state(db: &Connection) -> String {
        db.query_row("SELECT state FROM recording_outbox", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn duplicate_callbacks_keep_their_first_identity() {
        let (_dir, root) = fixture::root();
        let db = admitted(&root, true);
        enqueue(&db, fixture::ACCOUNT, CALL, REC, "in-progress").unwrap();
        assert_eq!(state(&db), "waiting");
        enqueue(&db, fixture::ACCOUNT, CALL, REC, "completed").unwrap();
        assert_eq!(state(&db), "queued");
        let other = "CA33333333333333333333333333333333";
        assert_eq!(
            enqueue(&db, fixture::ACCOUNT, other, REC, "completed"),
            Err("recording_identifier_conflict")
        );
        assert!(enqueue(&db, fixture::ACCOUNT, CALL, "nope", "completed").is_err());
    }

    #[test]
    fn withdrawn_consent_suppresses_queued_recordings() {
        let (_dir, root) = fixture::root();
        let db = admitted(&root, false);
        enqueue(&db, fixture::ACCOUNT, CALL, REC, "completed").unwrap();
        let settings = common::load(&root).unwrap();
        assert!(claim_job(&root, &settings).unwrap().is_none());
        assert_eq!(state(&db), "failed");
    }

    #[tokio::test]
    async fn archived_recordings_are_announced_once_through_the_hook() {
        let (_dir, root) = fixture::root();
        let out = root.join("hook-events");
        fixture::edit_config(&root, |config| {
            config.notify = Some(common::NotifyConfig {
                command: vec![
                    "/bin/sh".into(),
                    "-c".into(),
                    format!("cat >> '{}'; echo >> '{}'", out.display(), out.display()),
                ],
                timeout_secs: 10,
            });
        });
        let db = admitted(&root, true);
        enqueue(&db, fixture::ACCOUNT, CALL, REC, "completed").unwrap();
        let settings = common::load(&root).unwrap();
        let job = claim_job(&root, &settings).unwrap().unwrap();
        let mp3 = [b"ID3\x03\0\0\0\0\0\0".as_slice(), &[0xff, 0xfb, 0x90, 0x64], &[0; 64]].concat();
        keep_download(&root, &settings, &job, &mp3).unwrap();
        assert_eq!(state(&db), "ready");
        let job = claim_job(&root, &settings).unwrap().unwrap();
        send_job(&root, &settings, &job).await.unwrap();
        assert_eq!(state(&db), "sent");
        let events = fs::read_to_string(&out).unwrap();
        let event: serde_json::Value = serde_json::from_str(events.trim()).unwrap();
        assert_eq!(event["event"], "recording");
        assert_eq!(event["direction"], "inbound");
        assert_eq!(event["caller_id_unverified"], "+15550000001");
        assert!(claim_job(&root, &settings).unwrap().is_none());
        assert_eq!(
            local_recording(&root, CALL).unwrap().unwrap(),
            root.join("recordings").join(format!("{REC}.mp3"))
        );
    }

    #[test]
    fn mp3_validation_accepts_frames_and_rejects_other_media() {
        assert!(mp3_header(&[0xff, 0xfb, 0x90, 0x64]));
        assert!(!mp3_header(b"<html>"));
        assert!(!mp3_header(b"ID3"));
    }
}
