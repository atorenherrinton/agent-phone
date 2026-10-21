//! Owner "coach" leg for outbound calls: a second Twilio call to the owner's own
//! configured number that listens to the AI call live.
//!
//! The owner hears both sides (callee audio mixed with the assistant's speech).
//! Their speech is transcribed and delivered to the AI call as private, trusted
//! coaching that the other party never hears. Pressing 1 takes over: the owner's
//! audio goes straight to the other party and the assistant stays silent until
//! 2 hands the call back (or the owner hangs up).
//!
//! The two legs are separate websockets in the same process, joined through a
//! `Registry` keyed by outbound request id. The AI bridge never blocks on the
//! owner leg: every message to it is a `try_send` that drops when full.

use crate::audio::{self, EndpointEvent, Endpointer};
use crate::cascade::{SpeechToText, StageError};
use crate::realtime::{
    BridgeResult, EndReason, MAX_SECONDS, next_twilio, send_twilio, string, valid_sid,
    validate_start,
};
use axum::extract::ws::{Message as TwilioMessage, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::SinkExt;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

const FRAME: usize = 160; // 20 ms of 8 kHz mu-law.
const MAX_MEDIA_PAYLOAD: usize = 8 * 1024;
/// Callee audio older than this is dropped so the owner hears the call live.
const MAX_CALLEE_BUFFER: usize = 8_000;
const MAX_ASSISTANT_BUFFER: usize = crate::realtime::MAX_QUEUED_OUTPUT;
const MAX_PENDING_WHISPERS: usize = 4;
const CHANNEL: usize = 256;
const MULAW_SILENCE: u8 = 0xFF;

/// AI call -> owner leg.
pub enum ToOwner {
    /// Real-time callee audio, as received from the carrier.
    Callee(Vec<u8>),
    /// Synthesized assistant speech, delivered in bursts faster than real time.
    Assistant(Vec<u8>),
    /// The assistant was interrupted: drop its unplayed audio.
    ClearAssistant,
}

/// Owner leg -> AI call.
pub enum FromOwner {
    Joined,
    /// Transcribed owner speech: private coaching, or direct speech in takeover.
    Speech(String),
    /// Raw owner audio, forwarded to the other party only during takeover.
    Audio(Vec<u8>),
    TakeOver,
    HandBack,
}

/// The AI call's end of the link. Dropping it ends the owner leg.
pub struct MainLink {
    pub to_owner: mpsc::Sender<ToOwner>,
    pub from_owner: mpsc::Receiver<FromOwner>,
}

pub struct OwnerLink {
    to_owner: mpsc::Receiver<ToOwner>,
    from_owner: mpsc::Sender<FromOwner>,
}

struct Pending {
    main: Option<MainLink>,
    owner: Option<OwnerLink>,
}

/// Pairs the two websockets of one coached call, whichever connects first.
#[derive(Default)]
pub struct Registry {
    links: Mutex<HashMap<String, Pending>>,
}

impl Registry {
    fn take<T>(&self, request_id: &str, pick: impl FnOnce(&mut Pending) -> Option<T>) -> Option<T> {
        let mut links = self.links.lock().ok()?;
        let entry = links.entry(request_id.to_owned()).or_insert_with(|| {
            let (to_tx, to_rx) = mpsc::channel(CHANNEL);
            let (from_tx, from_rx) = mpsc::channel(CHANNEL);
            Pending {
                main: Some(MainLink {
                    to_owner: to_tx,
                    from_owner: from_rx,
                }),
                owner: Some(OwnerLink {
                    to_owner: to_rx,
                    from_owner: from_tx,
                }),
            }
        });
        let taken = pick(entry);
        if entry.main.is_none() && entry.owner.is_none() {
            links.remove(request_id);
        }
        taken
    }

    pub fn main(&self, request_id: &str) -> Option<MainLink> {
        self.take(request_id, |p| p.main.take())
    }

    pub fn owner(&self, request_id: &str) -> Option<OwnerLink> {
        self.take(request_id, |p| p.owner.take())
    }

    /// Forget a link whose other side will never connect.
    pub fn forget(&self, request_id: &str) {
        if let Ok(mut links) = self.links.lock() {
            links.remove(request_id);
        }
    }
}

// Intentionally no Debug.
pub struct OwnerOptions {
    pub expected_account_sid: String,
    pub expected_call_sid: String,
    pub max_duration_secs: u64,
}

struct Task<T>(JoinHandle<T>);

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn join_stt(
    task: &mut Option<Task<Result<String, StageError>>>,
) -> Result<String, StageError> {
    match task {
        Some(handle) => {
            let result = (&mut handle.0).await.unwrap_or(Err(StageError));
            *task = None;
            result
        }
        None => std::future::pending().await,
    }
}

async fn next_to_owner(link: &mut Option<OwnerLink>) -> Option<ToOwner> {
    match link {
        Some(link) => link.to_owner.recv().await,
        None => std::future::pending().await,
    }
}

fn decode_media(encoded: &str) -> BridgeResult<Vec<u8>> {
    if encoded.is_empty() || encoded.len() > MAX_MEDIA_PAYLOAD.div_ceil(3) * 4 {
        return Err(EndReason::ResourceLimit);
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| EndReason::ProtocolError)?;
    if bytes.is_empty() || bytes.len() > MAX_MEDIA_PAYLOAD {
        return Err(EndReason::ResourceLimit);
    }
    Ok(bytes)
}

/// One 20 ms frame of both sides, or `None` when there is nothing to play.
pub(crate) fn mix_frame(
    callee: &mut VecDeque<u8>,
    assistant: &mut VecDeque<u8>,
) -> Option<Vec<u8>> {
    if callee.is_empty() && assistant.is_empty() {
        return None;
    }
    let take = |buffer: &mut VecDeque<u8>| {
        let n = buffer.len().min(FRAME);
        let mut frame: Vec<u8> = buffer.drain(..n).collect();
        frame.resize(FRAME, MULAW_SILENCE);
        frame
    };
    let a = take(callee);
    let b = take(assistant);
    Some(
        a.iter()
            .zip(&b)
            .map(|(x, y)| {
                let sum = i32::from(audio::mulaw_decode(*x)) + i32::from(audio::mulaw_decode(*y));
                audio::mulaw_encode(sum.clamp(i16::MIN.into(), i16::MAX.into()) as i16)
            })
            .collect(),
    )
}

/// Owner-side state that needs no I/O.
#[derive(Default)]
pub(crate) struct OwnerCore {
    pub(crate) callee: VecDeque<u8>,
    pub(crate) assistant: VecDeque<u8>,
    pub(crate) takeover: bool,
}

impl OwnerCore {
    pub(crate) fn receive(&mut self, message: ToOwner) {
        match message {
            ToOwner::Callee(bytes) => {
                self.callee.extend(bytes);
                let excess = self.callee.len().saturating_sub(MAX_CALLEE_BUFFER);
                self.callee.drain(..excess);
            }
            ToOwner::Assistant(bytes) => {
                let room = MAX_ASSISTANT_BUFFER.saturating_sub(self.assistant.len());
                self.assistant.extend(bytes.into_iter().take(room));
            }
            ToOwner::ClearAssistant => self.assistant.clear(),
        }
    }

    /// Keypad control: 1 takes over, 2 hands back. Other keys do nothing.
    pub(crate) fn digit(&mut self, digit: &str) -> Option<FromOwner> {
        match (digit, self.takeover) {
            ("1", false) => {
                self.takeover = true;
                Some(FromOwner::TakeOver)
            }
            ("2", true) => {
                self.takeover = false;
                Some(FromOwner::HandBack)
            }
            _ => None,
        }
    }
}

fn notify(link: &Option<OwnerLink>, message: FromOwner) {
    if let Some(link) = link {
        let _ = link.from_owner.try_send(message);
    }
}

async fn run_owner(
    socket: &mut WebSocket,
    options: &OwnerOptions,
    link: &mut Option<OwnerLink>,
    stt: &Arc<dyn SpeechToText>,
    deadline: Instant,
) -> BridgeResult<()> {
    let setup_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
    let first = next_twilio(socket, setup_deadline).await?;
    let start = if first["event"] == "connected" {
        next_twilio(socket, setup_deadline).await?
    } else {
        first
    };
    let stream_sid = validate_start(
        &start,
        &options.expected_account_sid,
        &options.expected_call_sid,
    )?;
    notify(link, FromOwner::Joined);
    let mut core = OwnerCore::default();
    let mut vad = Endpointer::default();
    let mut stt_task: Option<Task<Result<String, StageError>>> = None;
    let mut stt_queue: VecDeque<Vec<u8>> = VecDeque::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(20));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => return Err(EndReason::DurationLimit),
            value = next_twilio(socket, deadline) => {
                let value: Value = value?;
                if value["streamSid"].as_str() != Some(stream_sid.as_str()) {
                    return Err(EndReason::ProtocolError);
                }
                match string(&value, "event")? {
                    "media" => {
                        let bytes = decode_media(string(&value["media"], "payload")?)?;
                        if core.takeover {
                            notify(link, FromOwner::Audio(bytes.clone()));
                        }
                        for event in vad.push(&bytes) {
                            if let EndpointEvent::SpeechStopped(Some(utterance)) = event {
                                if stt_task.is_none() {
                                    let stt = stt.clone();
                                    stt_task = Some(Task(tokio::spawn(async move { stt.transcribe(utterance).await })));
                                } else if stt_queue.len() < MAX_PENDING_WHISPERS {
                                    stt_queue.push_back(utterance);
                                }
                            }
                        }
                    }
                    "dtmf" => {
                        if let Some(message) = core.digit(string(&value["dtmf"], "digit")?) {
                            notify(link, message);
                        }
                    }
                    "mark" => {}
                    "stop" => return Err(EndReason::CallEnded),
                    _ => return Err(EndReason::ProtocolError),
                }
            }
            message = next_to_owner(link) => match message {
                Some(message) => core.receive(message),
                // The AI call ended.
                None => return Err(EndReason::CallEnded),
            },
            result = join_stt(&mut stt_task) => {
                if let Ok(text) = result {
                    let text = text.trim();
                    if !text.is_empty() {
                        notify(link, FromOwner::Speech(text.to_owned()));
                    }
                }
                if let Some(utterance) = stt_queue.pop_front() {
                    let stt = stt.clone();
                    stt_task = Some(Task(tokio::spawn(async move { stt.transcribe(utterance).await })));
                }
            }
            _ = ticker.tick() => {
                if let Some(frame) = mix_frame(&mut core.callee, &mut core.assistant) {
                    let message = json!({"event":"media","streamSid":stream_sid,
                        "media":{"payload":STANDARD.encode(frame)}});
                    send_twilio(socket, TwilioMessage::Text(message.to_string().into()), deadline).await?;
                }
            }
        }
    }
}

/// Run one already-authorized owner leg until either call ends. Closing the
/// socket lets Twilio reach the `<Hangup/>` that follows `<Connect>`.
pub async fn owner_bridge(
    mut socket: WebSocket,
    options: OwnerOptions,
    link: Option<OwnerLink>,
    stt: Arc<dyn SpeechToText>,
) {
    let deadline =
        Instant::now() + Duration::from_secs(options.max_duration_secs.min(MAX_SECONDS + 30));
    let mut link = link;
    if options.max_duration_secs > 0
        && link.is_some()
        && valid_sid(&options.expected_account_sid, "AC")
        && valid_sid(&options.expected_call_sid, "CA")
    {
        let _ = run_owner(&mut socket, &options, &mut link, &stt, deadline).await;
    }
    drop(link);
    let _ = timeout_at(Instant::now() + Duration::from_millis(250), socket.close()).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_pairs_either_order_and_cleans_up() {
        let registry = Registry::default();
        let mut main = registry.main("r1").unwrap();
        assert!(registry.main("r1").is_none());
        let owner = registry.owner("r1").unwrap();
        assert!(registry.links.lock().unwrap().is_empty());
        owner.from_owner.try_send(FromOwner::Joined).unwrap();
        assert!(matches!(main.from_owner.try_recv(), Ok(FromOwner::Joined)));
        let _owner_first = registry.owner("r2").unwrap();
        assert!(registry.main("r2").is_some());
        registry.owner("r3").unwrap();
        registry.forget("r3");
        assert!(registry.links.lock().unwrap().is_empty());
    }

    #[test]
    fn mixer_pads_and_sums_both_sides() {
        let mut callee: VecDeque<u8> = VecDeque::new();
        let mut assistant: VecDeque<u8> = VecDeque::new();
        assert!(mix_frame(&mut callee, &mut assistant).is_none());
        callee.extend(vec![audio::mulaw_encode(1000); 100]);
        assistant.extend(vec![audio::mulaw_encode(2000); 300]);
        let frame = mix_frame(&mut callee, &mut assistant).unwrap();
        assert_eq!(frame.len(), FRAME);
        let first = audio::mulaw_decode(frame[0]);
        assert!((2800..=3200).contains(&first), "{first}");
        let padded = audio::mulaw_decode(frame[150]);
        assert!((1900..=2100).contains(&padded), "{padded}");
        assert!(callee.is_empty());
        assert_eq!(assistant.len(), 140);
    }

    #[test]
    fn owner_core_bounds_buffers_and_toggles_takeover() {
        let mut core = OwnerCore::default();
        core.receive(ToOwner::Callee(vec![0; MAX_CALLEE_BUFFER + 500]));
        assert_eq!(core.callee.len(), MAX_CALLEE_BUFFER);
        core.receive(ToOwner::Assistant(vec![0; 1000]));
        core.receive(ToOwner::ClearAssistant);
        assert!(core.assistant.is_empty());
        assert!(core.digit("2").is_none());
        assert!(matches!(core.digit("1"), Some(FromOwner::TakeOver)));
        assert!(core.digit("1").is_none());
        assert!(matches!(core.digit("2"), Some(FromOwner::HandBack)));
        assert!(core.digit("#").is_none());
    }
}
