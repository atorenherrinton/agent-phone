//! Isolated, bounded Twilio <Connect><Stream> / OpenAI Realtime audio bridge.
//!
//! The ingress must verify Twilio's upgrade signature and consume a single-use
//! call nonce BEFORE calling `bridge`. This module additionally binds the start
//! event to the expected account/call. It reads no configuration, memory, files,
//! environment variables, or general tools; an optional call-bound local scheduler
//! handles one narrow proposal function. It never logs credentials, audio, or text.
//!
//! Protocol references (GA schema, checked 2026-09-03):
//! https://developers.openai.com/api/reference/resources/realtime/client-events
//! https://developers.openai.com/api/docs/guides/realtime-conversations
//! https://www.twilio.com/docs/voice/media-streams/websocket-messages

use crate::appointments::{self, AppointmentScheduler, SchedulingFuture};
use axum::extract::ws::{Message as TwilioMessage, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};
use tokio_tungstenite::tungstenite::{
    Message as ModelMessage, client::IntoClientRequest, http::HeaderValue,
    protocol::WebSocketConfig,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub const MODEL: &str = "gpt-realtime-2.1";
pub const VOICE: &str = "marin";
const TRANSCRIPTION_MODEL: &str = "gpt-transcribe";
const ENDPOINT: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1";
pub(crate) const MAX_SECONDS: u64 = 180;
const MAX_FRAME: usize = 256 * 1024;
const MAX_AUDIO_DELTA: usize = 64 * 1024;
const MAX_PENDING_INPUT: usize = 64 * 1024; // 8 seconds at 8 kHz, 8-bit mu-law.
const MAX_PENDING_CHUNKS: usize = 512;
pub(crate) const MAX_QUEUED_OUTPUT: usize = 240 * 1024; // Fail closed above ~30 seconds.
pub(crate) const MAX_TOTAL_INPUT: usize = 8_000 * 181;
pub(crate) const MAX_TOTAL_OUTPUT: usize = 8_000 * 360; // Includes interrupted generations.
pub(crate) const PLAYBACK_CHUNK: usize = 800; // Acknowledged playback granularity: <=100 ms.
pub(crate) const MAX_MARKS: usize = 2048;
pub(crate) const MAX_ITEMS: usize = 256;
pub(crate) const MAX_TRANSCRIPT: usize = 128 * 1024;
pub(crate) const MAX_INSTRUCTIONS: usize = 32 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
pub(crate) const END_CONFIRM_SILENCE: Duration = Duration::from_secs(8);
const END_CONFIRM_PURPOSE: &str = "agent_phone_end_call_confirmation";
pub(crate) const END_CONFIRM_INSTRUCTIONS: &str = "The call must remain open for one final confirmation turn. Briefly recap the current outcome in one sentence, ask whether the other party needs anything else before you go, then add that if not, you thank them and say goodbye. Do not call any tool in this response and never speak a tool name aloud. Do not claim the call has already ended. Wait for their reply. After they reply, invoke the end_call tool without saying its name aloud unless they continue the authorized task.";
pub(crate) const END_CALL_DESCRIPTION: &str = "Request to end this phone call only when the bounded task is complete, refused, a wrong number, or cannot continue. Invoke this as a tool call; never say the tool name aloud. Interactive calls may return a fixed confirmation requirement. Follow it, wait for the other party's reply, then invoke this tool again unless they continue the authorized task.";
pub(crate) const DECLINE_RECORDING_DESCRIPTION: &str = "Immediately stop this call and discard its recording and transcript when the caller objects to being recorded or withdraws recording consent. Invoke this fixed no-argument tool immediately; do not say goodbye, ask for confirmation, or continue the conversation.";
type Upstream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub(crate) type BridgeResult<T> = Result<T, EndReason>;

// Intentionally no Debug: these fields include a secret and private instructions.
pub struct RealtimeOptions {
    pub api_key: String,
    pub instructions: String,
    pub expected_account_sid: String,
    pub expected_call_sid: String,
    pub max_duration_secs: u64,
    /// Allows only the fixed no-argument `end_call` function.
    pub allow_end_call: bool,
    /// Requires a server-controlled audible close check and a remote reply (or
    /// bounded silence) before an interactive call may end. Voicemail disables it.
    pub confirm_end_call: bool,
    /// Signed ingress enables this only for inbound recorded calls. Caller
    /// refusal or withdrawal discards the transcript and terminates immediately.
    pub stop_on_recording_decline: bool,
    /// Present only for an authenticated inbound call with local scheduling enabled.
    pub appointments: Option<Arc<dyn AppointmentScheduler>>,
}

/// Text is generated/transcribed, NOT a word-accurate record of what was heard.
/// For an interrupted assistant entry, never assume all `text` reached the caller;
/// `heard_audio_ms` is the conservative, mark-acknowledged amount actually played.
#[derive(Deserialize, Serialize)]
pub struct TranscriptEntry {
    pub speaker: String,
    pub text: String,
    pub interrupted: bool,
    pub heard_audio_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EndReason {
    CallEnded,
    PeerClosed,
    DurationLimit,
    InvalidOptions,
    SetupFailed,
    UpstreamClosed,
    UpstreamError,
    ProtocolError,
    ResourceLimit,
    IoTimeout,
    AssistantEnded,
    RecordingDeclined,
}

// No Debug: transcript content must not accidentally enter service logs.
#[derive(Serialize)]
pub struct BridgeOutcome {
    pub transcript: Vec<TranscriptEntry>,
    pub reason: EndReason,
    pub duration_ms: u64,
    pub model_session_ready: bool,
}

enum Action {
    Twilio(Value),
    Model(Value),
}

struct Utterance {
    id: String,
    speaker: &'static str,
    response_id: Option<String>,
    text: String,
    sent_bytes: usize,
    played_bytes: usize,
    interrupted: bool,
    audio_done: bool,
}

struct PlaybackMark {
    item_index: usize,
    end_bytes: usize,
    chunk_bytes: usize,
}

struct EndCall {
    tool: CallTool,
    item_id: String,
    response_id: String,
    argument_bytes: usize,
    arguments_done: bool,
    arguments: Option<Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallTool {
    EndCall,
    DeclineRecording,
    Appointment,
}

impl CallTool {
    fn from_name(name: &str) -> BridgeResult<Self> {
        match name {
            "end_call" => Ok(Self::EndCall),
            "decline_recording" => Ok(Self::DeclineRecording),
            appointments::TOOL_NAME => Ok(Self::Appointment),
            _ => Err(EndReason::ProtocolError),
        }
    }
}

#[derive(Default, PartialEq, Eq)]
enum ClosePhase {
    #[default]
    Open,
    PromptPending,
    Prompting(String),
    PromptPlayback(String),
    AwaitingReply,
    Ready,
}

#[derive(Default)]
struct State {
    stream_sid: Option<String>,
    ready: bool,
    draining: bool,
    speaking: bool,
    active_response: Option<String>,
    cancelled_responses: BTreeSet<String>,
    cancellation_events: BTreeSet<String>,
    items: Vec<Utterance>,
    marks: BTreeMap<String, PlaybackMark>,
    next_mark: u64,
    next_cancel: u64,
    pending_input: VecDeque<String>,
    pending_input_bytes: usize,
    queued_output_bytes: usize,
    total_input_bytes: usize,
    total_output_bytes: usize,
    uncommitted_input_bytes: usize,
    transcript_bytes: usize,
    allow_end_call: bool,
    confirm_end_call: bool,
    stop_on_recording_decline: bool,
    session_instructions: String,
    allow_appointments: bool,
    pending_appointment: Option<(String, appointments::Request)>,
    appointment_busy: bool,
    appointment_response_pending: bool,
    end_calls: BTreeMap<String, EndCall>,
    close_phase: ClosePhase,
    close_deadline: Option<Instant>,
    ending: bool,
}

pub(crate) fn valid_sid(value: &str, prefix: &str) -> bool {
    value.len() == 34
        && value.starts_with(prefix)
        && value.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

pub(crate) fn string<'a>(value: &'a Value, key: &str) -> BridgeResult<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or(EndReason::ProtocolError)
}

/// Bind a Twilio `start` event to the expected account/call and the exact
/// 8 kHz mono mu-law media format. Returns the stream SID.
pub(crate) fn validate_start(
    value: &Value,
    expected_account_sid: &str,
    expected_call_sid: &str,
) -> BridgeResult<String> {
    if value["event"] != "start" {
        return Err(EndReason::ProtocolError);
    }
    let start = &value["start"];
    let stream_sid = string(start, "streamSid")?;
    if !valid_sid(stream_sid, "MZ")
        || value["streamSid"] != stream_sid
        || start["accountSid"] != expected_account_sid
        || start["callSid"] != expected_call_sid
        || start["tracks"] != json!(["inbound"])
        || start["mediaFormat"]["encoding"] != "audio/x-mulaw"
        || start["mediaFormat"]["sampleRate"] != 8000
        || start["mediaFormat"]["channels"] != 1
    {
        return Err(EndReason::ProtocolError);
    }
    Ok(stream_sid.to_owned())
}

fn item_id(value: &Value, key: &str) -> BridgeResult<String> {
    let id = string(value, key)?;
    if !valid_id(id) {
        return Err(EndReason::ProtocolError);
    }
    Ok(id.to_owned())
}

fn decode_audio(encoded: &str) -> BridgeResult<Vec<u8>> {
    if encoded.is_empty() || encoded.len() > MAX_AUDIO_DELTA.div_ceil(3) * 4 {
        return Err(EndReason::ResourceLimit);
    }
    let bytes = STANDARD
        .decode(encoded)
        .map_err(|_| EndReason::ProtocolError)?;
    if bytes.is_empty() || bytes.len() > MAX_AUDIO_DELTA {
        return Err(EndReason::ResourceLimit);
    }
    Ok(bytes)
}

fn end_call_tool() -> Value {
    json!({"type":"function","name":"end_call","description":END_CALL_DESCRIPTION,"parameters":{"type":"object","additionalProperties":false,"properties":{}}})
}

fn session_tools(
    allow_end_call: bool,
    stop_on_recording_decline: bool,
    allow_appointments: bool,
) -> Value {
    let mut tools = if allow_end_call {
        vec![end_call_tool()]
    } else {
        Vec::new()
    };
    if stop_on_recording_decline {
        tools.push(json!({"type":"function","name":"decline_recording","description":DECLINE_RECORDING_DESCRIPTION,"parameters":{"type":"object","additionalProperties":false,"properties":{}}}));
    }
    if allow_appointments {
        tools.push(appointments::tool_definition());
    }
    json!(tools)
}

fn close_prompt_actions(call_id: &str, session_instructions: &str) -> Vec<Action> {
    let instructions =
        format!("{session_instructions}\n\nRuntime close protocol: {END_CONFIRM_INSTRUCTIONS}");
    vec![
        Action::Model(json!({
            "type":"conversation.item.create",
            "item":{
                "type":"function_call_output",
                "call_id":call_id,
                "output":"{\"ended\":false,\"reason\":\"confirmation_required\"}"
            }
        })),
        Action::Model(json!({
            "type":"response.create",
            "response":{
                "instructions":instructions,
                "tools":[],
                "tool_choice":"none",
                "metadata":{"purpose":END_CONFIRM_PURPOSE}
            }
        })),
    ]
}

fn session_update(
    instructions: &str,
    allow_end_call: bool,
    stop_on_recording_decline: bool,
    allow_appointments: bool,
) -> Value {
    let tools = session_tools(
        allow_end_call,
        stop_on_recording_decline,
        allow_appointments,
    );
    json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "instructions": instructions,
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "transcription": {"model": TRANSCRIPTION_MODEL},
                    "turn_detection": {
                        "type": "server_vad", "threshold": 0.5,
                        "prefix_padding_ms": 300, "silence_duration_ms": 500,
                        "create_response": true, "interrupt_response": true
                    }
                },
                "output": {"format": {"type": "audio/pcmu"}, "voice": VOICE}
            },
            "reasoning": {"effort": "low"},
            "max_output_tokens": 1024,
            "tools": tools,
            "tool_choice": if allow_end_call || stop_on_recording_decline || allow_appointments { "auto" } else { "none" },
            "tracing": null
        }
    })
}

fn verify_session(
    event: &Value,
    allow_end_call: bool,
    stop_on_recording_decline: bool,
    allow_appointments: bool,
) -> bool {
    let s = &event["session"];
    s["type"] == "realtime"
        && s["model"] == MODEL
        && s["audio"]["input"]["format"]["type"] == "audio/pcmu"
        && s["audio"]["output"]["format"]["type"] == "audio/pcmu"
        && s["audio"]["output"]["voice"] == VOICE
        && s["audio"]["input"]["transcription"]["model"] == TRANSCRIPTION_MODEL
        && s["audio"]["input"]["turn_detection"]["type"] == "server_vad"
        && s["audio"]["input"]["turn_detection"]["create_response"] == true
        && s["audio"]["input"]["turn_detection"]["interrupt_response"] == true
        && s["audio"]["input"]["turn_detection"]["threshold"] == 0.5
        && s["audio"]["input"]["turn_detection"]["prefix_padding_ms"] == 300
        && s["audio"]["input"]["turn_detection"]["silence_duration_ms"] == 500
        && s["reasoning"]["effort"] == "low"
        && s["output_modalities"] == json!(["audio"])
        && if allow_end_call || stop_on_recording_decline || allow_appointments {
            s["tools"]
                == session_tools(
                    allow_end_call,
                    stop_on_recording_decline,
                    allow_appointments,
                )
                && s["tool_choice"] == "auto"
        } else {
            s["tools"].as_array().is_some_and(Vec::is_empty) && s["tool_choice"] == "none"
        }
}

impl State {
    fn tool_allowed(&self, tool: CallTool) -> bool {
        match tool {
            CallTool::EndCall => self.allow_end_call,
            CallTool::DeclineRecording => self.stop_on_recording_decline,
            CallTool::Appointment => self.allow_appointments,
        }
    }

    fn start(&mut self, value: &Value, options: &RealtimeOptions) -> BridgeResult<()> {
        if self.stream_sid.is_some() {
            return Err(EndReason::ProtocolError);
        }
        self.stream_sid = Some(validate_start(
            value,
            &options.expected_account_sid,
            &options.expected_call_sid,
        )?);
        Ok(())
    }

    fn ensure_item(&mut self, id: String, speaker: &'static str) -> BridgeResult<usize> {
        if let Some(index) = self.items.iter().position(|item| item.id == id) {
            if self.items[index].speaker != speaker {
                return Err(EndReason::ProtocolError);
            }
            return Ok(index);
        }
        if self.items.len() >= MAX_ITEMS {
            return Err(EndReason::ResourceLimit);
        }
        self.items.push(Utterance {
            id,
            speaker,
            response_id: None,
            text: String::new(),
            sent_bytes: 0,
            played_bytes: 0,
            interrupted: false,
            audio_done: false,
        });
        Ok(self.items.len() - 1)
    }

    fn transcript(&mut self, index: usize, text: &str) -> BridgeResult<()> {
        let next_bytes = self.transcript_bytes - self.items[index].text.len() + text.len();
        if next_bytes > MAX_TRANSCRIPT {
            return Err(EndReason::ResourceLimit);
        }
        self.transcript_bytes = next_bytes;
        self.items[index].text = text.to_owned();
        Ok(())
    }

    fn cancel(&mut self, response_id: String) -> BridgeResult<Action> {
        if self.cancelled_responses.len() >= MAX_ITEMS
            || self.cancellation_events.len() >= MAX_ITEMS
        {
            return Err(EndReason::ResourceLimit);
        }
        self.cancelled_responses.insert(response_id.clone());
        self.next_cancel += 1;
        let event_id = format!("zc-cancel-{}", self.next_cancel);
        self.cancellation_events.insert(event_id.clone());
        Ok(Action::Model(json!({
            "type": "response.cancel", "response_id": response_id, "event_id": event_id
        })))
    }

    fn interrupt(&mut self) -> BridgeResult<Vec<Action>> {
        let mut actions = Vec::new();
        let active = self.active_response.take();
        // Purge BEFORE sending clear: Twilio acknowledges discarded marks too.
        // Monotonic names are never reused, so late clear acknowledgements cannot
        // advance the playback position of a later response.
        let had_audio = !self.marks.is_empty();
        self.marks.clear();
        self.queued_output_bytes = 0;
        if had_audio {
            actions.push(Action::Twilio(json!({
                "event": "clear", "streamSid": self.stream_sid
            })));
        }
        if let Some(id) = active.as_ref() {
            // server_vad already cancels automatically. This is an idempotent
            // explicit backstop; only errors tied to OUR cancel IDs are ignored.
            actions.push(self.cancel(id.clone())?);
        }
        for item in &mut self.items {
            if item.speaker == "assistant"
                && !item.interrupted
                && (item.sent_bytes > item.played_bytes
                    || (active.is_some() && item.response_id == active))
            {
                item.interrupted = true;
                if item.sent_bytes > 0 {
                    actions.push(Action::Model(json!({
                        "type": "conversation.item.truncate", "item_id": item.id,
                        "content_index": 0, "audio_end_ms": item.played_bytes / 8
                    })));
                }
            }
        }
        Ok(actions)
    }

    fn response_has_pending_playback(&self, response_id: &str) -> bool {
        self.marks
            .values()
            .any(|mark| self.items[mark.item_index].response_id.as_deref() == Some(response_id))
    }

    fn response_was_audible(&self, response_id: &str) -> bool {
        self.items.iter().any(|item| {
            item.response_id.as_deref() == Some(response_id)
                && item.speaker == "assistant"
                && item.audio_done
                && item.sent_bytes > 0
                && !item.interrupted
        })
    }

    fn start_close_wait_if_played(&mut self) {
        let response_id = match &self.close_phase {
            ClosePhase::PromptPlayback(response_id) => response_id.clone(),
            _ => return,
        };
        if !self.response_has_pending_playback(&response_id) {
            self.close_phase = ClosePhase::AwaitingReply;
            self.close_deadline = Some(Instant::now() + END_CONFIRM_SILENCE);
        }
    }

    fn request_close_confirmation(&mut self, call_id: &str) -> Vec<Action> {
        self.close_phase = ClosePhase::PromptPending;
        self.close_deadline = None;
        close_prompt_actions(call_id, &self.session_instructions)
    }

    fn close_timeout(&self) -> Option<Instant> {
        if self.close_phase == ClosePhase::AwaitingReply {
            self.close_deadline
        } else {
            None
        }
    }

    fn twilio(&mut self, value: Value) -> BridgeResult<Vec<Action>> {
        if value["streamSid"].as_str() != self.stream_sid.as_deref() {
            return Err(EndReason::ProtocolError);
        }
        match string(&value, "event")? {
            "media" => {
                if value["media"]["track"] != "inbound" {
                    return Err(EndReason::ProtocolError);
                }
                let payload = string(&value["media"], "payload")?;
                let bytes = decode_audio(payload)?.len();
                self.total_input_bytes += bytes;
                self.uncommitted_input_bytes += bytes;
                if self.total_input_bytes > MAX_TOTAL_INPUT {
                    return Err(EndReason::ResourceLimit);
                }
                if self.ready {
                    Ok(vec![Action::Model(json!({
                        "type": "input_audio_buffer.append", "audio": payload
                    }))])
                } else {
                    if self.pending_input_bytes + bytes > MAX_PENDING_INPUT
                        || self.pending_input.len() >= MAX_PENDING_CHUNKS
                    {
                        return Err(EndReason::ResourceLimit);
                    }
                    self.pending_input_bytes += bytes;
                    self.pending_input.push_back(payload.to_owned());
                    Ok(Vec::new())
                }
            }
            "mark" => {
                let name = string(&value["mark"], "name")?;
                if name.len() > 128 {
                    return Err(EndReason::ProtocolError);
                }
                if let Some(mark) = self.marks.remove(name) {
                    let item = &mut self.items[mark.item_index];
                    item.played_bytes = item.played_bytes.max(mark.end_bytes);
                    self.queued_output_bytes -= mark.chunk_bytes;
                }
                self.start_close_wait_if_played();
                if self.ending && self.active_response.is_none() && self.marks.is_empty() {
                    Err(EndReason::AssistantEnded)
                } else {
                    Ok(Vec::new())
                }
            }
            "stop" => Err(EndReason::CallEnded),
            "dtmf" => Ok(Vec::new()), // Consent belongs to the signed ingress.
            _ => Err(EndReason::ProtocolError),
        }
    }

    fn model(&mut self, value: Value) -> BridgeResult<Vec<Action>> {
        let event_type = string(&value, "type")?;
        if event_type == "error" {
            let event_id = value["error"]["event_id"].as_str().unwrap_or("");
            if self.cancellation_events.remove(event_id)
                || (self.draining && event_id == "zc-final-commit")
            {
                return Ok(Vec::new());
            }
            return Err(EndReason::UpstreamError);
        }
        if event_type == "session.updated" {
            if !verify_session(
                &value,
                self.allow_end_call,
                self.stop_on_recording_decline,
                self.allow_appointments,
            ) {
                return Err(EndReason::SetupFailed);
            }
            if self.ready || self.draining {
                return Ok(Vec::new());
            }
            self.ready = true;
            // The sole initial response trigger; the probe never calls this path.
            // The sole later per-response override repeats these exact isolation
            // instructions before adding its fixed, tool-free close protocol.
            let mut actions = vec![Action::Model(json!({"type": "response.create"}))];
            while let Some(audio) = self.pending_input.pop_front() {
                actions.push(Action::Model(json!({
                    "type": "input_audio_buffer.append", "audio": audio
                })));
            }
            self.pending_input_bytes = 0;
            return Ok(actions);
        }
        if !self.ready {
            return match event_type {
                "session.created" | "rate_limits.updated" => Ok(Vec::new()),
                _ => Err(EndReason::SetupFailed),
            };
        }
        match event_type {
            "input_audio_buffer.speech_started" => {
                self.speaking = true;
                match self.close_phase {
                    ClosePhase::PromptPending
                    | ClosePhase::Prompting(_)
                    | ClosePhase::PromptPlayback(_)
                    | ClosePhase::AwaitingReply => {
                        // Any speech after the first close request is a remote
                        // turn. It may be an explicit hangup request or a task
                        // continuation; in either case, let the model respond and
                        // keep final-close authorization until it invokes the tool.
                        self.close_phase = ClosePhase::Ready;
                        self.close_deadline = None;
                    }
                    ClosePhase::Open | ClosePhase::Ready => {}
                }
                self.interrupt()
            }
            "input_audio_buffer.speech_stopped" => {
                self.speaking = false;
                Ok(Vec::new())
            }
            "input_audio_buffer.committed" => {
                self.uncommitted_input_bytes = 0;
                self.ensure_item(item_id(&value, "item_id")?, "caller")?;
                Ok(Vec::new())
            }
            "conversation.item.input_audio_transcription.completed" => {
                let text = string(&value, "transcript")?;
                if self.stop_on_recording_decline && crate::protocol::recording_declined(text) {
                    return Err(EndReason::RecordingDeclined);
                }
                let index = self.ensure_item(item_id(&value, "item_id")?, "caller")?;
                self.transcript(index, text)?;
                Ok(Vec::new())
            }
            "conversation.item.input_audio_transcription.failed" => {
                let index = self.ensure_item(item_id(&value, "item_id")?, "caller")?;
                self.transcript(index, "[Caller speech transcription unavailable]")?;
                Ok(Vec::new())
            }
            "response.created" => {
                let id = item_id(&value["response"], "id")?;
                if self.draining {
                    return Ok(vec![self.cancel(id)?]);
                }
                if self.active_response.is_some() {
                    return Err(EndReason::ProtocolError);
                }
                let confirmation = value["response"]["metadata"]["purpose"] == END_CONFIRM_PURPOSE;
                self.active_response = Some(id.clone());
                if confirmation {
                    if self.close_phase != ClosePhase::PromptPending || self.speaking {
                        // If the remote party spoke before this requested
                        // response was created, preserve that final-close
                        // authorization while cancelling the now-stale prompt.
                        if self.close_phase != ClosePhase::Ready {
                            self.close_phase = ClosePhase::Open;
                        }
                        return Ok(vec![self.cancel(id)?]);
                    }
                    self.close_phase = ClosePhase::Prompting(id);
                }
                Ok(Vec::new())
            }
            "response.output_item.added" => {
                let item = &value["item"];
                if item["type"] == "function_call" {
                    let tool = CallTool::from_name(string(item, "name")?)?;
                    if !self.tool_allowed(tool) {
                        return Err(EndReason::ProtocolError);
                    }
                    let response_id = item_id(&value, "response_id")?;
                    let output_item_id = item_id(item, "id")?;
                    let call_id = item_id(item, "call_id")?;
                    if self.active_response.as_deref() != Some(&response_id)
                        || self.end_calls.len() >= MAX_ITEMS
                        || self.end_calls.contains_key(&call_id)
                    {
                        return Err(EndReason::ProtocolError);
                    }
                    self.end_calls.insert(
                        call_id,
                        EndCall {
                            tool,
                            item_id: output_item_id,
                            response_id,
                            argument_bytes: 0,
                            arguments_done: false,
                            arguments: None,
                        },
                    );
                    return Ok(Vec::new());
                }
                if item["type"] != "message" || item["role"] != "assistant" {
                    return Err(EndReason::ProtocolError);
                }
                let index = self.ensure_item(item_id(item, "id")?, "assistant")?;
                let response_id = item_id(&value, "response_id")?;
                self.items[index].interrupted = self.cancelled_responses.contains(&response_id);
                self.items[index].response_id = Some(response_id);
                Ok(Vec::new())
            }
            "response.output_audio.delta" => {
                if value["content_index"] != 0 {
                    return Err(EndReason::ProtocolError);
                }
                let response_id = item_id(&value, "response_id")?;
                if self.draining || self.cancelled_responses.contains(&response_id) {
                    return Ok(Vec::new());
                }
                if self.active_response.as_deref() != Some(&response_id) {
                    return Err(EndReason::ProtocolError);
                }
                let index = self.ensure_item(item_id(&value, "item_id")?, "assistant")?;
                if self.items[index].interrupted {
                    return Ok(Vec::new());
                }
                self.items[index].response_id = Some(response_id);
                let bytes = decode_audio(string(&value, "delta")?)?;
                if self.queued_output_bytes + bytes.len() > MAX_QUEUED_OUTPUT
                    || self.total_output_bytes + bytes.len() > MAX_TOTAL_OUTPUT
                    || self.marks.len() + bytes.len().div_ceil(PLAYBACK_CHUNK) > MAX_MARKS
                {
                    return Err(EndReason::ResourceLimit);
                }
                self.queued_output_bytes += bytes.len();
                self.total_output_bytes += bytes.len();
                let mut actions = Vec::new();
                for chunk in bytes.chunks(PLAYBACK_CHUNK) {
                    self.items[index].sent_bytes += chunk.len();
                    self.next_mark += 1;
                    let name = format!("zc-play-{}", self.next_mark);
                    self.marks.insert(
                        name.clone(),
                        PlaybackMark {
                            item_index: index,
                            end_bytes: self.items[index].sent_bytes,
                            chunk_bytes: chunk.len(),
                        },
                    );
                    actions.push(Action::Twilio(json!({
                        "event": "media", "streamSid": self.stream_sid,
                        "media": {"payload": STANDARD.encode(chunk)}
                    })));
                    actions.push(Action::Twilio(json!({
                        "event": "mark", "streamSid": self.stream_sid, "mark": {"name": name}
                    })));
                }
                Ok(actions)
            }
            "response.output_audio.done" => {
                let index = self.ensure_item(item_id(&value, "item_id")?, "assistant")?;
                self.items[index].audio_done = true;
                Ok(Vec::new())
            }
            "response.output_audio_transcript.done" => {
                let index = self.ensure_item(item_id(&value, "item_id")?, "assistant")?;
                let response_id = item_id(&value, "response_id")?;
                self.items[index].interrupted |= self.cancelled_responses.contains(&response_id);
                self.items[index].response_id = Some(response_id);
                self.transcript(index, string(&value, "transcript")?)?;
                Ok(Vec::new())
            }
            "response.done" => {
                let response = &value["response"];
                let response_id = item_id(response, "id")?;
                let confirmation_response = matches!(
                    &self.close_phase,
                    ClosePhase::Prompting(id) if id == &response_id
                );
                if self.active_response.as_deref() == Some(&response_id) {
                    self.active_response = None;
                }
                let status = string(response, "status")?;
                if status == "failed" {
                    return Err(EndReason::UpstreamError);
                }
                if !matches!(status, "completed" | "cancelled" | "incomplete") {
                    return Err(EndReason::ProtocolError);
                }
                let mut completed_end_call = None;
                let mut completed_appointment = None;
                if let Some(output) = response["output"].as_array() {
                    for item in output {
                        if item["type"] == "function_call" {
                            let output_item_id = item_id(item, "id")?;
                            let call_id = item_id(item, "call_id")?;
                            let arguments: Value = serde_json::from_str(string(item, "arguments")?)
                                .map_err(|_| EndReason::ProtocolError)?;
                            let binding = self
                                .end_calls
                                .get(&call_id)
                                .ok_or(EndReason::ProtocolError)?;
                            let tool = CallTool::from_name(string(item, "name")?)?;
                            if !self.tool_allowed(tool)
                                || binding.tool != tool
                                || binding.item_id != output_item_id
                                || binding.response_id != response_id
                                || !binding.arguments_done
                                || binding.arguments.as_ref() != Some(&arguments)
                            {
                                return Err(EndReason::ProtocolError);
                            }
                            if status == "completed" && tool == CallTool::DeclineRecording {
                                return Err(EndReason::RecordingDeclined);
                            }
                            if status == "completed"
                                && !self.cancelled_responses.contains(&response_id)
                            {
                                if tool == CallTool::Appointment {
                                    let request = appointments::Request::parse(arguments)
                                        .map_err(|_| EndReason::ProtocolError)?;
                                    if self.draining
                                        || self.appointment_busy
                                        || completed_appointment
                                            .replace((call_id, request))
                                            .is_some()
                                    {
                                        return Err(EndReason::ProtocolError);
                                    }
                                } else if completed_end_call.replace(call_id).is_some() {
                                    return Err(EndReason::ProtocolError);
                                }
                            }
                            continue;
                        }
                        if item["type"] != "message" || item["role"] != "assistant" {
                            return Err(EndReason::ProtocolError);
                        }
                        let index = self.ensure_item(item_id(item, "id")?, "assistant")?;
                        self.items[index].response_id = Some(response_id.clone());
                        self.items[index].interrupted |=
                            self.cancelled_responses.contains(&response_id)
                                || response["status"] == "cancelled";
                        if let Some(parts) = item["content"].as_array() {
                            for part in parts {
                                if let Some(text) = part["transcript"].as_str() {
                                    self.transcript(index, text)?;
                                }
                            }
                        }
                    }
                }
                self.end_calls
                    .retain(|_, call| call.response_id != response_id);
                if confirmation_response {
                    self.close_deadline = None;
                    if status == "completed" && self.response_was_audible(&response_id) {
                        self.close_phase = ClosePhase::PromptPlayback(response_id);
                        self.start_close_wait_if_played();
                    } else {
                        self.close_phase = ClosePhase::Open;
                    }
                    return Ok(Vec::new());
                }
                if let Some(request) = completed_appointment {
                    if completed_end_call.is_some() {
                        return Err(EndReason::ProtocolError);
                    }
                    self.pending_appointment = Some(request);
                    self.appointment_busy = true;
                }
                if let Some(call_id) = completed_end_call {
                    if self.confirm_end_call && self.close_phase != ClosePhase::Ready {
                        return Ok(self.request_close_confirmation(&call_id));
                    }
                    self.ending = true;
                }
                if self.ending && self.marks.is_empty() {
                    Err(EndReason::AssistantEnded)
                } else {
                    Ok(Vec::new())
                }
            }
            "response.function_call_arguments.delta" => {
                let response_id = item_id(&value, "response_id")?;
                let output_item_id = item_id(&value, "item_id")?;
                let call_id = item_id(&value, "call_id")?;
                let delta = string(&value, "delta")?;
                let binding = self
                    .end_calls
                    .get_mut(&call_id)
                    .ok_or(EndReason::ProtocolError)?;
                if binding.response_id != response_id
                    || binding.item_id != output_item_id
                    || binding.arguments_done
                {
                    return Err(EndReason::ProtocolError);
                }
                binding.argument_bytes = binding
                    .argument_bytes
                    .checked_add(delta.len())
                    .ok_or(EndReason::ResourceLimit)?;
                if binding.argument_bytes
                    > if binding.tool == CallTool::Appointment {
                        appointments::MAX_ARGUMENT_BYTES
                    } else {
                        64
                    }
                {
                    return Err(EndReason::ResourceLimit);
                }
                Ok(Vec::new())
            }
            "response.function_call_arguments.done" => {
                let response_id = item_id(&value, "response_id")?;
                let output_item_id = item_id(&value, "item_id")?;
                let call_id = item_id(&value, "call_id")?;
                let raw_arguments = string(&value, "arguments")?;
                if raw_arguments.len() > appointments::MAX_ARGUMENT_BYTES {
                    return Err(EndReason::ResourceLimit);
                }
                let arguments: Value =
                    serde_json::from_str(raw_arguments).map_err(|_| EndReason::ProtocolError)?;
                let tool = CallTool::from_name(string(&value, "name")?)?;
                if tool != CallTool::Appointment && raw_arguments.len() > 64 {
                    return Err(EndReason::ResourceLimit);
                }
                if !self.tool_allowed(tool) {
                    return Err(EndReason::ProtocolError);
                }
                let binding = self
                    .end_calls
                    .get_mut(&call_id)
                    .ok_or(EndReason::ProtocolError)?;
                if binding.tool != tool
                    || (tool != CallTool::Appointment && arguments != json!({}))
                    || (tool == CallTool::Appointment
                        && appointments::Request::parse(arguments.clone()).is_err())
                    || binding.response_id != response_id
                    || binding.item_id != output_item_id
                {
                    return Err(EndReason::ProtocolError);
                }
                if binding.arguments_done {
                    return Err(EndReason::ProtocolError);
                }
                binding.arguments_done = true;
                binding.arguments = Some(arguments);
                if tool == CallTool::DeclineRecording {
                    // Unlike ordinary end_call, a refusal must not wait for
                    // response completion, playback marks, or a close check.
                    // Caller barge-in may cancel this response after the tool
                    // was bound. That cannot invalidate its verified refusal.
                    return Err(EndReason::RecordingDeclined);
                }
                Ok(Vec::new())
            }
            name if name.contains("function_call") || name.contains("mcp") => {
                Err(EndReason::ProtocolError)
            }
            // Audio-transcript deltas are not accumulated: the final transcript
            // and response.done are authoritative and bound retained memory.
            _ => Ok(Vec::new()),
        }
    }

    fn outcome(self, reason: EndReason, duration_ms: u64) -> BridgeOutcome {
        if reason == EndReason::RecordingDeclined {
            return BridgeOutcome {
                transcript: Vec::new(),
                reason,
                duration_ms,
                model_session_ready: self.ready,
            };
        }
        let transcript = self
            .items
            .into_iter()
            .filter_map(|item| {
                if item.speaker == "assistant" && item.text.is_empty() && item.sent_bytes == 0 {
                    return None;
                }
                let assistant = item.speaker == "assistant";
                Some(TranscriptEntry {
                    speaker: item.speaker.to_owned(),
                    text: if item.text.is_empty() {
                        if assistant {
                            "[Assistant audio transcript unavailable]"
                        } else {
                            "[Caller speech transcription unavailable]"
                        }
                        .to_owned()
                    } else {
                        item.text
                    },
                    interrupted: item.interrupted
                        || (assistant && (item.sent_bytes > item.played_bytes || !item.audio_done)),
                    heard_audio_ms: assistant.then_some((item.played_bytes / 8) as u64),
                })
            })
            .collect();
        BridgeOutcome {
            transcript,
            reason,
            duration_ms,
            model_session_ready: self.ready,
        }
    }
}

fn parse_json(text: &str) -> BridgeResult<Value> {
    if text.len() > MAX_FRAME {
        return Err(EndReason::ResourceLimit);
    }
    serde_json::from_str(text).map_err(|_| EndReason::ProtocolError)
}

fn operation_deadline(deadline: Instant) -> Instant {
    deadline.min(Instant::now() + IO_TIMEOUT)
}

pub(crate) async fn send_twilio(
    socket: &mut WebSocket,
    message: TwilioMessage,
    deadline: Instant,
) -> BridgeResult<()> {
    timeout_at(operation_deadline(deadline), socket.send(message))
        .await
        .map_err(|_| EndReason::IoTimeout)?
        .map_err(|_| EndReason::PeerClosed)
}

async fn send_model(
    socket: &mut Upstream,
    message: ModelMessage,
    deadline: Instant,
) -> BridgeResult<()> {
    timeout_at(operation_deadline(deadline), socket.send(message))
        .await
        .map_err(|_| EndReason::IoTimeout)?
        .map_err(|_| EndReason::UpstreamClosed)
}

pub(crate) async fn next_twilio(socket: &mut WebSocket, deadline: Instant) -> BridgeResult<Value> {
    loop {
        let frame = timeout_at(deadline, socket.recv())
            .await
            .map_err(|_| EndReason::IoTimeout)?
            .ok_or(EndReason::PeerClosed)?
            .map_err(|_| EndReason::ProtocolError)?;
        match frame {
            TwilioMessage::Text(text) => return parse_json(text.as_str()),
            TwilioMessage::Ping(bytes) => {
                send_twilio(socket, TwilioMessage::Pong(bytes), deadline).await?
            }
            TwilioMessage::Pong(_) => {}
            TwilioMessage::Close(_) => return Err(EndReason::PeerClosed),
            TwilioMessage::Binary(_) => return Err(EndReason::ProtocolError),
        }
    }
}

async fn next_model(socket: &mut Upstream, deadline: Instant) -> BridgeResult<Value> {
    loop {
        let frame = timeout_at(deadline, socket.next())
            .await
            .map_err(|_| EndReason::IoTimeout)?
            .ok_or(EndReason::UpstreamClosed)?
            .map_err(|_| EndReason::ProtocolError)?;
        match frame {
            ModelMessage::Text(text) => return parse_json(text.as_str()),
            ModelMessage::Ping(bytes) => {
                send_model(socket, ModelMessage::Pong(bytes), deadline).await?
            }
            ModelMessage::Pong(_) => {}
            ModelMessage::Close(_) => return Err(EndReason::UpstreamClosed),
            ModelMessage::Binary(_) | ModelMessage::Frame(_) => {
                return Err(EndReason::ProtocolError);
            }
        }
    }
}

async fn connect_model(api_key: &str) -> Result<Upstream, &'static str> {
    if api_key.is_empty() || api_key.len() > 8192 || api_key.contains(['\r', '\n']) {
        return Err("invalid realtime credential");
    }
    let mut request = ENDPOINT
        .into_client_request()
        .map_err(|_| "invalid realtime endpoint")?;
    let mut authorization = HeaderValue::from_str(&format!("Bearer {api_key}"))
        .map_err(|_| "invalid realtime credential")?;
    authorization.set_sensitive(true);
    request.headers_mut().insert("Authorization", authorization);
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let result = tokio_tungstenite::connect_async_with_config(request, Some(config), true).await;
    result
        .map(|(socket, _)| socket)
        .map_err(|_| "realtime connection rejected or unavailable")
}

async fn actions(
    twilio: &mut WebSocket,
    model: &mut Upstream,
    actions: Vec<Action>,
    deadline: Instant,
) -> BridgeResult<()> {
    for action in actions {
        match action {
            Action::Twilio(value) => {
                send_twilio(
                    twilio,
                    TwilioMessage::Text(value.to_string().into()),
                    deadline,
                )
                .await?;
            }
            Action::Model(value) => {
                send_model(
                    model,
                    ModelMessage::Text(value.to_string().into()),
                    deadline,
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn run_bridge<F>(
    twilio: &mut WebSocket,
    options: &RealtimeOptions,
    state: &mut State,
    upstream: &mut Option<Upstream>,
    deadline: Instant,
    connection: F,
) -> BridgeResult<()>
where
    F: std::future::Future<Output = Result<Upstream, &'static str>>,
{
    let setup_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
    // The initial connected envelope is optional; no media is accepted until the
    // independently verified start event has established the expected call.
    let first = next_twilio(twilio, setup_deadline).await?;
    let start = if first["event"] == "connected" {
        if first["protocol"] != "Call" || first["version"] != "1.0.0" {
            return Err(EndReason::ProtocolError);
        }
        next_twilio(twilio, setup_deadline).await?
    } else {
        first
    };
    state.start(&start, options)?;
    // Consume incoming audio during TLS setup into a byte-bounded queue. No
    // unbounded channels/tasks, and a caller can hang up during connection setup.
    tokio::pin!(connection);
    let model = loop {
        tokio::select! {
            _ = tokio::time::sleep_until(setup_deadline) => return Err(EndReason::SetupFailed),
            result = &mut connection => break result.map_err(|_| EndReason::SetupFailed)?,
            value = next_twilio(twilio, setup_deadline) => { state.twilio(value?)?; }
        }
    };
    *upstream = Some(model);
    let model = upstream.as_mut().ok_or(EndReason::SetupFailed)?;
    send_model(
        model,
        ModelMessage::Text(
            session_update(
                &options.instructions,
                options.allow_end_call,
                options.stop_on_recording_decline,
                options.appointments.is_some(),
            )
            .to_string()
            .into(),
        ),
        setup_deadline,
    )
    .await?;
    let mut appointment_job: Option<(String, SchedulingFuture)> = None;
    loop {
        if let Some((id, request)) = state.pending_appointment.take() {
            let scheduler = options
                .appointments
                .as_ref()
                .ok_or(EndReason::ProtocolError)?;
            if appointment_job.is_some() {
                return Err(EndReason::ProtocolError);
            }
            appointment_job = Some((id, scheduler.schedule(request)));
        }
        if state.appointment_response_pending && state.active_response.is_none() && !state.speaking
        {
            state.appointment_response_pending = false;
            actions(
                twilio,
                model,
                vec![Action::Model(json!({"type":"response.create"}))],
                deadline,
            )
            .await?;
        }
        let bridge_deadline = if state.ready {
            deadline
        } else {
            setup_deadline
        };
        let close_timeout = state.close_timeout();
        let current_deadline = close_timeout
            .map(|value| value.min(bridge_deadline))
            .unwrap_or(bridge_deadline);
        let generated_actions = tokio::select! {
            // Caller disconnect/refusal and the hard call deadline remain polled
            // while local lookup runs. No scheduling task escapes this owner.
            biased;
            _ = tokio::time::sleep_until(current_deadline) => {
                if close_timeout.is_some_and(|value| value <= bridge_deadline && Instant::now() >= value) {
                    return Err(EndReason::AssistantEnded);
                }
                return Err(if state.ready { EndReason::DurationLimit } else { EndReason::SetupFailed });
            }
            value = next_twilio(twilio, current_deadline) => state.twilio(value?)?,
            value = next_model(model, current_deadline) => state.model(value?)?,
            result = async {
                match appointment_job.as_mut() {
                    Some((_, future)) => future.await,
                    None => std::future::pending().await,
                }
            } => {
                let (id, _) = appointment_job.take().ok_or(EndReason::ProtocolError)?;
                state.appointment_busy = false;
                state.appointment_response_pending = true;
                vec![Action::Model(json!({"type":"conversation.item.create","item":{
                    "type":"function_call_output","call_id":id,"output":result.to_string()
                }}))]
            },
        };
        actions(twilio, model, generated_actions, current_deadline).await?;
    }
}

/// Small, bounded post-hangup grace period for asynchronous caller transcripts.
/// No audio is forwarded and no new model response is allowed during this phase.
async fn drain_transcripts(
    model: &mut Upstream,
    state: &mut State,
    deadline: Instant,
) -> Option<EndReason> {
    if !state.ready || Instant::now() >= deadline {
        return None;
    }
    state.draining = true;
    let grace = deadline.min(Instant::now() + Duration::from_millis(1500));
    if let Some(response) = state.active_response.take()
        && let Ok(Action::Model(event)) = state.cancel(response)
    {
        let _ = send_model(model, ModelMessage::Text(event.to_string().into()), grace).await;
    }
    if state.speaking && state.uncommitted_input_bytes >= 800 {
        let _ = send_model(
            model,
            ModelMessage::Text(
                json!({
                    "type": "input_audio_buffer.commit", "event_id": "zc-final-commit"
                })
                .to_string()
                .into(),
            ),
            grace,
        )
        .await;
    }
    while Instant::now() < grace {
        let Ok(value) = next_model(model, grace).await else {
            break;
        };
        let events = match state.model(value) {
            Ok(events) => events,
            Err(EndReason::RecordingDeclined) => return Some(EndReason::RecordingDeclined),
            Err(_) => break,
        };
        for event in events {
            // Only cancellation may leave the bridge after the caller disconnects.
            if let Action::Model(value) = event
                && value["type"] == "response.cancel"
            {
                let _ =
                    send_model(model, ModelMessage::Text(value.to_string().into()), grace).await;
            }
        }
    }
    None
}

async fn finish_bridge(
    socket: &mut WebSocket,
    upstream: &mut Option<Upstream>,
    state: &mut State,
    mut reason: EndReason,
    deadline: Instant,
) -> EndReason {
    if let Some(model) = upstream.as_mut()
        && matches!(
            reason,
            EndReason::CallEnded | EndReason::PeerClosed | EndReason::AssistantEnded
        )
        && let Some(drain_reason) = drain_transcripts(model, state, deadline).await
    {
        reason = drain_reason;
    }
    if reason == EndReason::RecordingDeclined {
        // Stop the carrier before waiting on the model. Do not drain transcripts
        // after a refusal, and discard the whole call when constructing outcome.
        let _ = timeout_at(
            Instant::now() + Duration::from_millis(250),
            socket.send(TwilioMessage::Close(None)),
        )
        .await;
    }
    if let Some(model) = upstream.as_mut() {
        let _ = timeout_at(
            Instant::now() + Duration::from_millis(250),
            model.close(None),
        )
        .await;
    }
    if reason != EndReason::RecordingDeclined {
        let _ = timeout_at(Instant::now() + Duration::from_millis(250), socket.close()).await;
    }
    reason
}

/// Bridge one already-authorized call. The hard cap can be shortened, not raised.
/// Outcomes contain private transcripts and should only be persisted privately.
pub async fn bridge(mut socket: WebSocket, options: RealtimeOptions) -> BridgeOutcome {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(options.max_duration_secs.min(MAX_SECONDS));
    let mut state = State {
        allow_end_call: options.allow_end_call,
        confirm_end_call: options.confirm_end_call,
        stop_on_recording_decline: options.stop_on_recording_decline,
        session_instructions: options.instructions.clone(),
        allow_appointments: options.appointments.is_some(),
        ..State::default()
    };
    let mut upstream = None;
    let valid = options.max_duration_secs > 0
        && !options.api_key.is_empty()
        && !options.instructions.trim().is_empty()
        && options.instructions.len() <= MAX_INSTRUCTIONS
        && (!options.confirm_end_call || options.allow_end_call)
        && valid_sid(&options.expected_account_sid, "AC")
        && valid_sid(&options.expected_call_sid, "CA");
    let reason = if valid {
        run_bridge(
            &mut socket,
            &options,
            &mut state,
            &mut upstream,
            deadline,
            connect_model(&options.api_key),
        )
        .await
        .err()
        .unwrap_or(EndReason::CallEnded)
    } else {
        EndReason::InvalidOptions
    };
    let reason = if reason == EndReason::IoTimeout && Instant::now() >= deadline {
        EndReason::DurationLimit
    } else {
        reason
    };
    let reason = finish_bridge(&mut socket, &mut upstream, &mut state, reason, deadline).await;
    state.outcome(
        reason,
        started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    )
}

/// Validate the exact account/model/voice/session configuration without supplying
/// audio, creating responses, executing tools, or changing any account setting.
/// This makes an authenticated external request ONLY when explicitly called.
pub async fn probe(api_key: &str) -> Result<(), &'static str> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut model = timeout_at(deadline, connect_model(api_key))
        .await
        .map_err(|_| "realtime connection timed out")??;
    let result = async {
        send_model(
            &mut model,
            ModelMessage::Text(
                session_update(
                    "Configuration validation only. Do not speak or invoke tools.",
                    false,
                    false,
                    false,
                )
                .to_string()
                .into(),
            ),
            deadline,
        )
        .await
        .map_err(|_| "realtime configuration send failed")?;
        for _ in 0..32 {
            let value = next_model(&mut model, deadline)
                .await
                .map_err(|_| "realtime configuration response unavailable")?;
            match value["type"].as_str() {
                Some("session.updated") if verify_session(&value, false, false, false) => {
                    return Ok(());
                }
                Some("session.created" | "rate_limits.updated") => {}
                _ => return Err("realtime session configuration rejected"),
            }
        }
        Err("realtime configuration response limit exceeded")
    }
    .await;
    let _ = timeout_at(
        Instant::now() + Duration::from_millis(250),
        model.close(None),
    )
    .await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> RealtimeOptions {
        RealtimeOptions {
            api_key: "synthetic-test-key".into(),
            instructions: "Isolated screening".into(),
            expected_account_sid: format!("AC{}", "1".repeat(32)),
            expected_call_sid: format!("CA{}", "2".repeat(32)),
            max_duration_secs: 180,
            allow_end_call: false,
            confirm_end_call: false,
            stop_on_recording_decline: false,
            appointments: None,
        }
    }

    fn start_event() -> Value {
        let o = options();
        let stream = format!("MZ{}", "3".repeat(32));
        json!({"event":"start","streamSid":stream,"start":{
            "streamSid":stream,"accountSid":o.expected_account_sid,
            "callSid":o.expected_call_sid,"tracks":["inbound"],
            "mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1}
        }})
    }

    fn ready_event() -> Value {
        let mut event = session_update("Synthetic isolated instructions", false, false, false);
        event["type"] = json!("session.updated");
        event["session"]["model"] = json!(MODEL);
        event
    }

    fn state() -> State {
        let mut state = State::default();
        assert!(state.start(&start_event(), &options()).is_ok());
        assert!(state.model(ready_event()).is_ok());
        state
    }

    fn start_response(state: &mut State, response: &str) {
        assert!(
            state
                .model(json!({"type":"response.created","response":{"id":response}}))
                .is_ok()
        );
    }

    fn delta(response: &str, item: &str, bytes: usize) -> Value {
        json!({"type":"response.output_audio.delta","response_id":response,
            "item_id":item,"content_index":0,"delta":STANDARD.encode(vec![0xff;bytes])})
    }

    fn add_end_call(state: &mut State, response: &str, item: &str, call: &str) {
        state
            .model(
                json!({"type":"response.output_item.added","response_id":response,
                "item":{"id":item,"type":"function_call","name":"end_call","call_id":call}}),
            )
            .unwrap();
        state
            .model(
                json!({"type":"response.function_call_arguments.delta","response_id":response,
                "item_id":item,"call_id":call,"delta":"{}"}),
            )
            .unwrap();
        state
            .model(
                json!({"type":"response.function_call_arguments.done","response_id":response,
                "item_id":item,"name":"end_call","call_id":call,"arguments":"{}"}),
            )
            .unwrap();
    }

    fn end_call_done(response: &str, item: &str, call: &str) -> Value {
        json!({"type":"response.done","response":{"id":response,"status":"completed",
            "output":[{"id":item,"type":"function_call","name":"end_call",
            "call_id":call,"arguments":"{}"}]}})
    }

    #[test]
    fn validates_call_binding_and_codec_before_model_connection() {
        let mut event = start_event();
        event["start"]["callSid"] = json!(format!("CA{}", "f".repeat(32)));
        assert_eq!(
            State::default().start(&event, &options()),
            Err(EndReason::ProtocolError)
        );
        event = start_event();
        event["start"]["mediaFormat"]["sampleRate"] = json!(24000);
        assert_eq!(
            State::default().start(&event, &options()),
            Err(EndReason::ProtocolError)
        );
        event = start_event();
        event["start"]["tracks"] = json!(["outbound"]);
        assert_eq!(
            State::default().start(&event, &options()),
            Err(EndReason::ProtocolError)
        );
        assert!(valid_sid(&options().expected_call_sid, "CA"));
        assert!(!valid_sid("CA<script>", "CA"));
    }

    #[test]
    fn session_is_exact_and_tool_free_greeting_only_after_ack() {
        let update = session_update("Screen only", false, false, false);
        assert_eq!(update["session"]["tools"], json!([]));
        assert_eq!(update["session"]["tool_choice"], "none");
        assert!(update["session"]["audio"]["input"]["format"]["rate"].is_null());
        assert!(verify_session(&ready_event(), false, false, false));
        let mut bad = ready_event();
        bad["session"]["tools"] = json!([{"type":"function","name":"private_memory"}]);
        assert!(!verify_session(&bad, false, false, false));
        let mut state = State::default();
        assert!(
            state
                .model(json!({"type":"session.created"}))
                .unwrap()
                .is_empty()
        );
        let events = state.model(ready_event()).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], Action::Model(v) if v["type"] == "response.create"));
        assert!(state.model(ready_event()).unwrap().is_empty());
    }

    #[test]
    fn outbound_session_exposes_only_end_call_and_waits_for_response_completion() {
        let update = session_update("One bounded outbound task", true, false, false);
        let mut ready = update.clone();
        ready["type"] = json!("session.updated");
        ready["session"]["model"] = json!(MODEL);
        assert!(verify_session(&ready, true, false, false));
        assert_eq!(update["session"]["tools"], json!([end_call_tool()]));

        let mut outbound = State {
            allow_end_call: true,
            ..State::default()
        };
        outbound.start(&start_event(), &options()).unwrap();
        outbound.model(ready.clone()).unwrap();
        outbound
            .model(json!({"type":"response.created","response":{"id":"resp_end"}}))
            .unwrap();
        outbound
            .model(json!({"type":"response.output_item.added","response_id":"resp_end","item":{"id":"item_end","type":"function_call","name":"end_call","call_id":"call_end"}}))
            .unwrap();
        outbound
            .model(json!({"type":"response.function_call_arguments.delta","response_id":"resp_end","item_id":"item_end","call_id":"call_end","delta":"{}"}))
            .unwrap();
        outbound
            .model(json!({"type":"response.function_call_arguments.done","response_id":"resp_end","item_id":"item_end","name":"end_call","call_id":"call_end","arguments":"{ }"}))
            .unwrap();
        assert!(!outbound.ending);
        assert!(matches!(
            outbound.model(json!({"type":"response.done","response":{"id":"resp_end","status":"completed","output":[{"id":"item_end","type":"function_call","name":"end_call","call_id":"call_end","arguments":"{}"}]}})),
            Err(EndReason::AssistantEnded)
        ));

        let mut cancelled = State {
            allow_end_call: true,
            ..State::default()
        };
        cancelled.start(&start_event(), &options()).unwrap();
        cancelled.model(ready).unwrap();
        cancelled
            .model(json!({"type":"response.created","response":{"id":"resp_cancel"}}))
            .unwrap();
        cancelled
            .model(json!({"type":"response.output_item.added","response_id":"resp_cancel","item":{"id":"item_cancel","type":"function_call","name":"end_call","call_id":"call_cancel"}}))
            .unwrap();
        cancelled
            .model(json!({"type":"response.function_call_arguments.done","response_id":"resp_cancel","item_id":"item_cancel","name":"end_call","call_id":"call_cancel","arguments":"{}"}))
            .unwrap();
        cancelled
            .model(json!({"type":"response.done","response":{"id":"resp_cancel","status":"cancelled","output":[{"id":"item_cancel","type":"function_call","name":"end_call","call_id":"call_cancel","arguments":"{}"}]}}))
            .unwrap();
        assert!(!cancelled.ending);

        let mut blocked = state();
        assert!(matches!(
            blocked.model(json!({"type":"response.output_item.added","response_id":"resp_1","item":{"type":"function_call","name":"end_call","call_id":"call_end"}})),
            Err(EndReason::ProtocolError)
        ));
    }

    fn inbound_recording_state() -> State {
        let mut inbound = State {
            allow_end_call: true,
            stop_on_recording_decline: true,
            ..State::default()
        };
        inbound.start(&start_event(), &options()).unwrap();
        let mut ready = session_update("Synthetic inbound recording", true, true, false);
        ready["type"] = json!("session.updated");
        ready["session"]["model"] = json!(MODEL);
        inbound.model(ready).unwrap();
        inbound
    }

    fn decline_tool_added() -> Value {
        json!({"type":"response.output_item.added","response_id":"resp_decline",
            "item":{"id":"item_decline","type":"function_call",
                "name":"decline_recording","call_id":"call_decline"}})
    }

    fn decline_tool_arguments() -> Value {
        json!({"type":"response.function_call_arguments.done","response_id":"resp_decline",
            "item_id":"item_decline","name":"decline_recording",
            "call_id":"call_decline","arguments":"{}"})
    }

    #[test]
    fn inbound_recording_tool_schema_is_exact_and_outbound_cannot_use_it() {
        let mut ready = session_update("Synthetic inbound recording", true, true, false);
        ready["type"] = json!("session.updated");
        ready["session"]["model"] = json!(MODEL);
        assert!(verify_session(&ready, true, true, false));
        assert!(!verify_session(&ready, true, false, false));
        assert_eq!(ready["session"]["tools"].as_array().unwrap().len(), 2);
        assert_eq!(ready["session"]["tools"][1]["name"], "decline_recording");
        ready["session"]["tools"][1]["parameters"]["properties"] = json!({"url":{"type":"string"}});
        assert!(!verify_session(&ready, true, true, false));

        let mut outbound = state();
        outbound.allow_end_call = true;
        start_response(&mut outbound, "resp_decline");
        assert!(matches!(
            outbound.model(decline_tool_added()),
            Err(EndReason::ProtocolError)
        ));
    }

    #[test]
    fn caller_recording_refusal_discards_whole_transcript_only_when_enabled() {
        let previous = json!({"type":"conversation.item.input_audio_transcription.completed",
            "item_id":"caller_previous","transcript":"A synthetic earlier message"});
        let refusal = json!({"type":"conversation.item.input_audio_transcription.completed",
            "item_id":"caller_refusal","transcript":"Please stop recording me."});
        let mut inbound = inbound_recording_state();
        inbound.model(previous.clone()).unwrap();
        assert!(matches!(
            inbound.model(refusal.clone()),
            Err(EndReason::RecordingDeclined)
        ));
        assert_eq!(inbound.items.len(), 1);
        assert!(
            inbound
                .outcome(EndReason::RecordingDeclined, 0)
                .transcript
                .is_empty()
        );

        let mut outbound = state();
        outbound.allow_end_call = true;
        outbound.model(previous).unwrap();
        outbound.model(refusal).unwrap();
        assert_eq!(
            outbound.outcome(EndReason::CallEnded, 0).transcript.len(),
            2
        );
    }

    #[test]
    fn bound_recording_decline_tool_bypasses_close_confirmation_and_playback() {
        let mut inbound = inbound_recording_state();
        inbound.confirm_end_call = true;
        start_response(&mut inbound, "resp_decline");
        inbound
            .model(delta("resp_decline", "assistant_1", 800))
            .unwrap();
        inbound.model(decline_tool_added()).unwrap();
        assert!(matches!(
            inbound.model(decline_tool_arguments()),
            Err(EndReason::RecordingDeclined)
        ));
        assert!(!inbound.marks.is_empty());
        assert!(inbound.close_phase == ClosePhase::Open);
        assert!(
            inbound
                .outcome(EndReason::RecordingDeclined, 0)
                .transcript
                .is_empty()
        );
    }

    #[test]
    fn malformed_and_unbound_recording_decline_tools_fail_closed() {
        for (field, value) in [
            ("name", json!("end_call")),
            ("name", json!("read_memory")),
            ("response_id", json!("another_response")),
            ("item_id", json!("another_item")),
            ("call_id", json!("another_call")),
            ("arguments", json!("{\"destination\":\"private\"}")),
            ("arguments", json!("not json")),
        ] {
            let mut inbound = inbound_recording_state();
            start_response(&mut inbound, "resp_decline");
            inbound.model(decline_tool_added()).unwrap();
            let mut event = decline_tool_arguments();
            event[field] = value;
            assert!(matches!(
                inbound.model(event),
                Err(EndReason::ProtocolError)
            ));
        }
        let mut unbound = inbound_recording_state();
        start_response(&mut unbound, "resp_decline");
        assert!(matches!(
            unbound.model(decline_tool_arguments()),
            Err(EndReason::ProtocolError)
        ));
    }

    #[test]
    fn caller_barge_in_cannot_invalidate_a_bound_recording_decline() {
        let mut cancelled = inbound_recording_state();
        cancelled
            .model(
                json!({"type":"conversation.item.input_audio_transcription.completed",
                "item_id":"caller_previous","transcript":"A synthetic earlier message"}),
            )
            .unwrap();
        start_response(&mut cancelled, "resp_decline");
        cancelled.model(decline_tool_added()).unwrap();
        cancelled
            .model(json!({"type":"input_audio_buffer.speech_started"}))
            .unwrap();
        assert!(cancelled.active_response.is_none());
        assert!(cancelled.cancelled_responses.contains("resp_decline"));
        assert!(matches!(
            cancelled.model(decline_tool_arguments()),
            Err(EndReason::RecordingDeclined)
        ));
        assert!(
            cancelled
                .outcome(EndReason::RecordingDeclined, 0)
                .transcript
                .is_empty()
        );
    }

    #[test]
    fn interactive_end_call_authorization_survives_separate_goodbye_and_tool_responses() {
        let mut ready = session_update("One bounded outbound task", true, false, false);
        ready["type"] = json!("session.updated");
        ready["session"]["model"] = json!(MODEL);
        let mut outbound = State {
            allow_end_call: true,
            confirm_end_call: true,
            session_instructions: "One bounded outbound task".into(),
            ..State::default()
        };
        outbound.start(&start_event(), &options()).unwrap();
        outbound.model(ready).unwrap();

        start_response(&mut outbound, "resp_first_end");
        add_end_call(
            &mut outbound,
            "resp_first_end",
            "item_first_end",
            "call_first_end",
        );
        let actions = outbound
            .model(end_call_done(
                "resp_first_end",
                "item_first_end",
                "call_first_end",
            ))
            .unwrap();
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], Action::Model(v)
            if v["type"] == "conversation.item.create"
                && v["item"]["type"] == "function_call_output"
                && v["item"]["call_id"] == "call_first_end"));
        assert!(matches!(&actions[1], Action::Model(v)
            if v["type"] == "response.create"
                && v["response"]["tools"] == json!([])
                && v["response"]["tool_choice"] == "none"
                && v["response"]["instructions"].as_str().is_some_and(|text|
                    text.starts_with("One bounded outbound task\n\nRuntime close protocol:"))
                && v["response"]["metadata"]["purpose"] == END_CONFIRM_PURPOSE));
        assert!(outbound.close_phase == ClosePhase::PromptPending);
        assert!(!outbound.ending);

        outbound
            .model(
                json!({"type":"response.created","response":{"id":"resp_close_check",
                "metadata":{"purpose":END_CONFIRM_PURPOSE}}}),
            )
            .unwrap();
        outbound
            .model(
                json!({"type":"response.output_item.added","response_id":"resp_close_check",
                "item":{"id":"msg_close_check","type":"message","role":"assistant"}}),
            )
            .unwrap();
        assert_eq!(
            outbound
                .model(delta("resp_close_check", "msg_close_check", 800))
                .unwrap()
                .len(),
            2
        );
        outbound
            .model(
                json!({"type":"response.output_audio.done","response_id":"resp_close_check",
                "item_id":"msg_close_check"}),
            )
            .unwrap();
        outbound
            .model(json!({"type":"response.output_audio_transcript.done","response_id":"resp_close_check",
                "item_id":"msg_close_check","transcript":"Anything else before I go? If not, thank you and goodbye."}))
            .unwrap();
        outbound
            .model(json!({"type":"response.done","response":{"id":"resp_close_check",
                "status":"completed","output":[{"id":"msg_close_check","type":"message",
                "role":"assistant","content":[{"type":"output_audio","transcript":"Anything else before I go? If not, thank you and goodbye."}]}]}}))
            .unwrap();
        assert!(matches!(
            outbound.close_phase,
            ClosePhase::PromptPlayback(_)
        ));
        assert!(outbound.close_deadline.is_none());
        outbound
            .twilio(json!({"event":"mark","streamSid":outbound.stream_sid,
                "mark":{"name":"zc-play-1"}}))
            .unwrap();
        assert!(outbound.close_phase == ClosePhase::AwaitingReply);
        let close_deadline = outbound.close_timeout().unwrap();
        assert!(close_deadline > Instant::now());
        assert!(close_deadline <= Instant::now() + END_CONFIRM_SILENCE);

        outbound
            .model(json!({"type":"input_audio_buffer.speech_started"}))
            .unwrap();
        outbound
            .model(json!({"type":"input_audio_buffer.speech_stopped"}))
            .unwrap();
        assert!(outbound.close_phase == ClosePhase::Ready);
        assert!(outbound.close_timeout().is_none());

        // The model may say a natural goodbye in one response and emit the tool
        // in the next. Authorization must remain sticky across that boundary.
        start_response(&mut outbound, "resp_goodbye");
        outbound
            .model(
                json!({"type":"response.output_item.added","response_id":"resp_goodbye",
                "item":{"id":"msg_goodbye","type":"message","role":"assistant"}}),
            )
            .unwrap();
        assert_eq!(
            outbound
                .model(delta("resp_goodbye", "msg_goodbye", 800))
                .unwrap()
                .len(),
            2
        );
        outbound
            .model(
                json!({"type":"response.output_audio.done","response_id":"resp_goodbye",
                "item_id":"msg_goodbye"}),
            )
            .unwrap();
        outbound
            .model(
                json!({"type":"response.output_audio_transcript.done","response_id":"resp_goodbye",
                "item_id":"msg_goodbye","transcript":"Thank you. Goodbye."}),
            )
            .unwrap();
        outbound
            .model(json!({"type":"response.done","response":{"id":"resp_goodbye",
                "status":"completed","output":[{"id":"msg_goodbye","type":"message",
                "role":"assistant","content":[{"type":"output_audio","transcript":"Thank you. Goodbye."}]}]}}))
            .unwrap();
        assert!(outbound.close_phase == ClosePhase::Ready);
        outbound
            .twilio(json!({"event":"mark","streamSid":outbound.stream_sid,
                "mark":{"name":"zc-play-2"}}))
            .unwrap();

        start_response(&mut outbound, "resp_final_tool");
        add_end_call(
            &mut outbound,
            "resp_final_tool",
            "item_final_tool",
            "call_final_tool",
        );
        assert!(matches!(
            outbound.model(end_call_done(
                "resp_final_tool",
                "item_final_tool",
                "call_final_tool"
            )),
            Err(EndReason::AssistantEnded)
        ));
    }

    #[test]
    fn remote_barge_in_before_close_prompt_creation_preserves_authorization() {
        let mut state = state();
        state.allow_end_call = true;
        state.confirm_end_call = true;
        state.close_phase = ClosePhase::PromptPending;
        state
            .model(json!({"type":"input_audio_buffer.speech_started"}))
            .unwrap();
        assert!(state.close_phase == ClosePhase::Ready);
        let actions = state
            .model(
                json!({"type":"response.created","response":{"id":"resp_stale_close",
                "metadata":{"purpose":END_CONFIRM_PURPOSE}}}),
            )
            .unwrap();
        assert!(state.close_phase == ClosePhase::Ready);
        assert!(matches!(&actions[..], [Action::Model(v)]
            if v["type"] == "response.cancel" && v["response_id"] == "resp_stale_close"));
    }

    #[test]
    fn caller_pcmu_is_forwarded_without_transcoding() {
        let mut state = state();
        let payload = STANDARD.encode([0xff, 0xfe, 0x00, 0x7f]);
        let actions = state
            .twilio(json!({"event":"media","streamSid":state.stream_sid,
            "media":{"track":"inbound","payload":payload}}))
            .unwrap();
        assert!(matches!(&actions[0], Action::Model(v)
            if v["type"] == "input_audio_buffer.append" && v["audio"] == payload));
    }

    #[test]
    fn interrupted_playback_truncates_at_ack_and_ignores_cleared_marks() {
        let mut state = state();
        start_response(&mut state, "resp_1");
        let output = state.model(delta("resp_1", "item_1", 1600)).unwrap();
        assert_eq!(output.len(), 4);
        state
            .twilio(json!({"event":"mark","streamSid":state.stream_sid,
            "mark":{"name":"zc-play-1"}}))
            .unwrap();
        let actions = state
            .model(json!({"type":"input_audio_buffer.speech_started"}))
            .unwrap();
        assert!(matches!(&actions[0], Action::Twilio(v) if v["event"] == "clear"));
        assert!(actions.iter().any(|a| matches!(a, Action::Model(v)
            if v["type"] == "response.cancel" && v["response_id"] == "resp_1")));
        assert!(actions.iter().any(|a| matches!(a, Action::Model(v)
            if v["type"] == "conversation.item.truncate" && v["audio_end_ms"] == 100)));
        state
            .twilio(json!({"event":"mark","streamSid":state.stream_sid,
            "mark":{"name":"zc-play-2"}}))
            .unwrap();
        assert_eq!(state.items[0].played_bytes, 800);
        assert_eq!(state.queued_output_bytes, 0);
        assert!(
            state
                .model(delta("resp_1", "item_1", 800))
                .unwrap()
                .is_empty()
        );
        start_response(&mut state, "resp_2");
        assert_eq!(
            state.model(delta("resp_2", "item_2", 800)).unwrap().len(),
            2
        );
        assert!(state.marks.contains_key("zc-play-3"));
    }

    #[test]
    fn interruption_after_response_done_still_clears_unheard_audio() {
        let mut state = state();
        start_response(&mut state, "resp_1");
        state.model(delta("resp_1", "item_1", 800)).unwrap();
        state
            .model(json!({"type":"response.done","response":{"id":"resp_1",
            "status":"completed","output":[]}}))
            .unwrap();
        let actions = state.interrupt().unwrap();
        assert!(actions.iter().any(|a| matches!(a, Action::Model(v)
            if v["type"] == "conversation.item.truncate" && v["audio_end_ms"] == 0)));
    }

    #[test]
    fn cancellation_error_is_ignored_only_for_our_event() {
        let mut state = state();
        start_response(&mut state, "resp_1");
        state.interrupt().unwrap();
        assert!(
            state
                .model(json!({"type":"error","error":{"event_id":"zc-cancel-1"}}))
                .is_ok()
        );
        assert!(matches!(
            state.model(json!({"type":"error","error":{"event_id":"other"}})),
            Err(EndReason::UpstreamError)
        ));
    }

    #[test]
    fn transcript_order_dedup_and_partial_hearing_are_preserved() {
        let mut state = state();
        state
            .model(json!({"type":"input_audio_buffer.committed","item_id":"caller_1"}))
            .unwrap();
        start_response(&mut state, "resp_1");
        state.model(delta("resp_1", "assistant_1", 1600)).unwrap();
        state
            .model(
                json!({"type":"response.output_audio_transcript.done","response_id":"resp_1",
            "item_id":"assistant_1","transcript":"A generated answer"}),
            )
            .unwrap();
        state
            .model(
                json!({"type":"conversation.item.input_audio_transcription.completed",
            "item_id":"caller_1","transcript":"A caller question"}),
            )
            .unwrap();
        state
            .model(
                json!({"type":"conversation.item.input_audio_transcription.completed",
            "item_id":"caller_1","transcript":"A caller question"}),
            )
            .unwrap();
        state
            .twilio(json!({"event":"mark","streamSid":state.stream_sid,
            "mark":{"name":"zc-play-1"}}))
            .unwrap();
        let outcome = state.outcome(EndReason::CallEnded, 1000);
        assert_eq!(outcome.transcript.len(), 2);
        assert_eq!(outcome.transcript[0].speaker, "caller");
        assert_eq!(outcome.transcript[1].heard_audio_ms, Some(100));
        assert!(outcome.transcript[1].interrupted);
    }

    #[test]
    fn malicious_tool_event_and_memory_limits_fail_closed() {
        let mut state = state();
        assert!(matches!(
            state.model(json!({"type":"response.output_item.added",
            "item":{"type":"function_call","name":"read_memory"}})),
            Err(EndReason::ProtocolError)
        ));
        let index = state.ensure_item("caller_1".into(), "caller").unwrap();
        assert_eq!(
            state.transcript(index, &"x".repeat(MAX_TRANSCRIPT + 1)),
            Err(EndReason::ResourceLimit)
        );
        start_response(&mut state, "resp_1");
        for _ in 0..3 {
            state
                .model(delta("resp_1", "item_1", MAX_AUDIO_DELTA))
                .unwrap();
        }
        assert!(matches!(
            state.model(delta("resp_1", "item_1", MAX_AUDIO_DELTA)),
            Err(EndReason::ResourceLimit)
        ));
        assert!(decode_audio("not-base64!").is_err());
    }

    #[test]
    fn pre_session_input_is_bounded_and_drained_after_ack() {
        let mut state = State::default();
        state.start(&start_event(), &options()).unwrap();
        let media = |sid: &Option<String>, bytes| {
            json!({"event":"media","streamSid":sid,
            "media":{"track":"inbound","payload":STANDARD.encode(vec![0xff;bytes])}})
        };
        assert!(
            state
                .twilio(media(&state.stream_sid, 800))
                .unwrap()
                .is_empty()
        );
        let actions = state.model(ready_event()).unwrap();
        assert_eq!(actions.len(), 2);
        assert!(state.pending_input.is_empty());
        assert_eq!(state.pending_input_bytes, 0);
        state.ready = false;
        state
            .twilio(media(&state.stream_sid, MAX_PENDING_INPUT))
            .unwrap();
        assert!(matches!(
            state.twilio(media(&state.stream_sid, 800)),
            Err(EndReason::ResourceLimit)
        ));
    }

    #[tokio::test]
    async fn mock_recording_decline_closes_carrier_without_playback_or_transcript_drain() {
        use axum::{Router, extract::ws::WebSocketUpgrade, routing::get};
        use tokio::net::TcpListener;
        use tokio::sync::mpsc;

        let test = async {
            for semantic_tool in [false, true] {
                let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let model_address = model_listener.local_addr().unwrap();
                let model_task = tokio::spawn(async move {
                    let (tcp, _) = model_listener.accept().await.unwrap();
                    let mut model = tokio_tungstenite::accept_async(tcp).await.unwrap();
                    let update = model.next().await.unwrap().unwrap().into_text().unwrap();
                    let mut ready: Value = serde_json::from_str(&update).unwrap();
                    assert_eq!(ready["session"]["tools"], session_tools(true, true, false));
                    ready["type"] = json!("session.updated");
                    ready["session"]["model"] = json!(MODEL);
                    model
                        .send(ModelMessage::Text(ready.to_string().into()))
                        .await
                        .unwrap();
                    let greeting = model.next().await.unwrap().unwrap().into_text().unwrap();
                    assert_eq!(
                        serde_json::from_str::<Value>(&greeting).unwrap()["type"],
                        "response.create"
                    );
                    let mut events = vec![
                        json!({"type":"conversation.item.input_audio_transcription.completed",
                            "item_id":"caller_previous","transcript":"A synthetic earlier message"}),
                        json!({"type":"response.created","response":{"id":"resp_decline"}}),
                        delta("resp_decline", "assistant_1", 800),
                    ];
                    if semantic_tool {
                        events.extend([decline_tool_added(), decline_tool_arguments()]);
                    } else {
                        events.push(
                            json!({"type":"conversation.item.input_audio_transcription.completed",
                            "item_id":"caller_decline","transcript":"Please stop recording me."}),
                        );
                    }
                    // A late event must never be drained into the returned call.
                    events.push(
                        json!({"type":"conversation.item.input_audio_transcription.completed",
                        "item_id":"caller_late","transcript":"Synthetic text after withdrawal"}),
                    );
                    for event in events {
                        if model
                            .send(ModelMessage::Text(event.to_string().into()))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    assert!(matches!(
                        model.next().await.unwrap().unwrap(),
                        ModelMessage::Close(_)
                    ));
                });

                let twilio_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let twilio_address = twilio_listener.local_addr().unwrap();
                let (result_sender, mut result_receiver) = mpsc::channel(1);
                let router = Router::new().route(
                    "/ws",
                    get(move |upgrade: WebSocketUpgrade| {
                        let sender = result_sender.clone();
                        async move {
                            upgrade.on_upgrade(move |mut socket| async move {
                                let mut options = options();
                                options.allow_end_call = true;
                                options.confirm_end_call = true;
                                options.stop_on_recording_decline = true;
                                let mut state = State {
                                    allow_end_call: true,
                                    confirm_end_call: true,
                                    stop_on_recording_decline: true,
                                    ..State::default()
                                };
                                let mut model = None;
                                let connection = async move {
                                    tokio_tungstenite::connect_async(format!(
                                        "ws://{model_address}"
                                    ))
                                    .await
                                    .map(|(ws, _)| ws)
                                    .map_err(|_| "mock connect failed")
                                };
                                let deadline = Instant::now() + Duration::from_secs(4);
                                let reason = run_bridge(
                                    &mut socket,
                                    &options,
                                    &mut state,
                                    &mut model,
                                    deadline,
                                    connection,
                                )
                                .await
                                .err()
                                .unwrap();
                                let reason = finish_bridge(
                                    &mut socket,
                                    &mut model,
                                    &mut state,
                                    reason,
                                    deadline,
                                )
                                .await;
                                sender.send(state.outcome(reason, 0)).await.unwrap();
                            })
                        }
                    }),
                );
                let twilio_task = tokio::spawn(async move {
                    axum::serve(twilio_listener, router).await.unwrap();
                });
                let (mut twilio, _) =
                    tokio_tungstenite::connect_async(format!("ws://{twilio_address}/ws"))
                        .await
                        .unwrap();
                twilio
                    .send(ModelMessage::Text(start_event().to_string().into()))
                    .await
                    .unwrap();
                // Leave playback unacknowledged: refusal must close it promptly.
                for expected in ["media", "mark"] {
                    let received = twilio.next().await.unwrap().unwrap().into_text().unwrap();
                    assert_eq!(
                        serde_json::from_str::<Value>(&received).unwrap()["event"],
                        expected
                    );
                }
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), twilio.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    ModelMessage::Close(_)
                ));
                let outcome = result_receiver.recv().await.unwrap();
                assert_eq!(outcome.reason, EndReason::RecordingDeclined);
                assert!(outcome.transcript.is_empty());
                assert!(outcome.model_session_ready);
                model_task.await.unwrap();
                twilio_task.abort();
            }
        };
        tokio::time::timeout(Duration::from_secs(6), test)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_late_recording_refusal_during_hangup_drain_discards_transcript() {
        let test = async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                socket.send(ModelMessage::Text(json!({
                    "type":"conversation.item.input_audio_transcription.completed",
                    "item_id":"caller_late_refusal","transcript":"I do not consent to recording."
                }).to_string().into())).await.unwrap();
                assert!(matches!(
                    socket.next().await.unwrap().unwrap(),
                    ModelMessage::Close(_)
                ));
            });
            let (mut model, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
                .await
                .unwrap();
            let mut state = inbound_recording_state();
            state
                .model(
                    json!({"type":"conversation.item.input_audio_transcription.completed",
                "item_id":"caller_previous","transcript":"A synthetic earlier message"}),
                )
                .unwrap();
            let reason = drain_transcripts(
                &mut model,
                &mut state,
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();
            assert_eq!(reason, EndReason::RecordingDeclined);
            assert!(state.outcome(reason, 0).transcript.is_empty());
            model.close(None).await.unwrap();
            server.await.unwrap();
        };
        tokio::time::timeout(Duration::from_secs(4), test)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn mock_websockets_greet_forward_interrupt_and_capture_without_external_calls() {
        use axum::{Router, extract::ws::WebSocketUpgrade, routing::get};
        use tokio::net::TcpListener;
        use tokio::sync::mpsc;

        // Both endpoints are ephemeral loopback listeners. The production public
        // API has no endpoint override; only this private runner receives a mock
        // connection future, which is not polled until start validation succeeds.
        let test = async {
            let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let model_address = model_listener.local_addr().unwrap();
            let model_task = tokio::spawn(async move {
                let (tcp, _) = model_listener.accept().await.unwrap();
                let mut model = tokio_tungstenite::accept_async(tcp).await.unwrap();
                let update = model.next().await.unwrap().unwrap().into_text().unwrap();
                let update: Value = serde_json::from_str(&update).unwrap();
                assert_eq!(update["type"], "session.update");
                assert_eq!(update["session"]["tools"], json!([]));
                model
                    .send(ModelMessage::Text(ready_event().to_string().into()))
                    .await
                    .unwrap();
                let greeting = model.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&greeting).unwrap()["type"],
                    "response.create"
                );
                for event in [
                    json!({"type":"response.created","response":{"id":"resp_1"}}),
                    delta("resp_1", "assistant_1", 1600),
                    json!({"type":"response.output_audio_transcript.done","response_id":"resp_1",
                        "item_id":"assistant_1","transcript":"A synthetic greeting"}),
                    json!({"type":"response.output_audio.done","response_id":"resp_1",
                        "item_id":"assistant_1","content_index":0}),
                ] {
                    model
                        .send(ModelMessage::Text(event.to_string().into()))
                        .await
                        .unwrap();
                }
                let input = model.next().await.unwrap().unwrap().into_text().unwrap();
                let input: Value = serde_json::from_str(&input).unwrap();
                assert_eq!(input["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(input["audio"].as_str().unwrap()).unwrap(),
                    vec![0xff; 800]
                );
                for event in [
                    json!({"type":"input_audio_buffer.committed","item_id":"caller_1"}),
                    json!({"type":"conversation.item.input_audio_transcription.completed",
                        "item_id":"caller_1","transcript":"Synthetic caller interruption"}),
                    json!({"type":"input_audio_buffer.speech_started"}),
                ] {
                    model
                        .send(ModelMessage::Text(event.to_string().into()))
                        .await
                        .unwrap();
                }
                let cancel = model.next().await.unwrap().unwrap().into_text().unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&cancel).unwrap()["type"],
                    "response.cancel"
                );
                let truncate = model.next().await.unwrap().unwrap().into_text().unwrap();
                let truncate: Value = serde_json::from_str(&truncate).unwrap();
                assert_eq!(truncate["type"], "conversation.item.truncate");
                assert_eq!(truncate["audio_end_ms"], 100);
                let _ = model.close(None).await;
            });

            let twilio_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let twilio_address = twilio_listener.local_addr().unwrap();
            let (result_sender, mut result_receiver) = mpsc::channel(1);
            let router = Router::new().route(
                "/ws",
                get(move |upgrade: WebSocketUpgrade| {
                    let sender = result_sender.clone();
                    async move {
                        upgrade
                            .max_frame_size(MAX_FRAME)
                            .max_message_size(MAX_FRAME)
                            .on_upgrade(move |mut socket| async move {
                                let options = options();
                                let mut state = State::default();
                                let mut model = None;
                                let connection = async move {
                                    tokio_tungstenite::connect_async(format!(
                                        "ws://{model_address}"
                                    ))
                                    .await
                                    .map(|(ws, _)| ws)
                                    .map_err(|_| "mock connect failed")
                                };
                                let reason = run_bridge(
                                    &mut socket,
                                    &options,
                                    &mut state,
                                    &mut model,
                                    Instant::now() + Duration::from_secs(4),
                                    connection,
                                )
                                .await
                                .err()
                                .unwrap();
                                let _ = sender.send(state.outcome(reason, 0)).await;
                            })
                    }
                }),
            );
            let twilio_task = tokio::spawn(async move {
                axum::serve(twilio_listener, router).await.unwrap();
            });
            let (mut twilio, _) =
                tokio_tungstenite::connect_async(format!("ws://{twilio_address}/ws"))
                    .await
                    .unwrap();
            twilio
                .send(ModelMessage::Text(start_event().to_string().into()))
                .await
                .unwrap();
            for index in 0..4 {
                let received = twilio.next().await.unwrap().unwrap().into_text().unwrap();
                let event: Value = serde_json::from_str(&received).unwrap();
                assert_eq!(
                    event["event"],
                    if index % 2 == 0 { "media" } else { "mark" }
                );
                if index == 1 {
                    twilio
                        .send(ModelMessage::Text(event.to_string().into()))
                        .await
                        .unwrap();
                }
            }
            let sid = start_event()["streamSid"].clone();
            twilio
                .send(ModelMessage::Text(
                    json!({"event":"media","streamSid":sid,
                "media":{"track":"inbound","payload":STANDARD.encode(vec![0xff;800])}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let clear = twilio.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&clear).unwrap()["event"],
                "clear"
            );
            twilio
                .send(ModelMessage::Text(
                    json!({"event":"stop","streamSid":sid}).to_string().into(),
                ))
                .await
                .unwrap();
            let outcome = result_receiver.recv().await.unwrap();
            // Either endpoint may close first after both protocols are verified.
            assert!(matches!(
                outcome.reason,
                EndReason::CallEnded | EndReason::UpstreamClosed
            ));
            assert!(outcome.model_session_ready);
            assert_eq!(outcome.transcript.len(), 2);
            assert!(outcome.transcript[0].interrupted);
            assert_eq!(outcome.transcript[0].heard_audio_ms, Some(100));
            assert_eq!(outcome.transcript[1].text, "Synthetic caller interruption");
            model_task.await.unwrap();
            twilio_task.abort();
        };
        tokio::time::timeout(Duration::from_secs(6), test)
            .await
            .unwrap();
    }
}

#[cfg(test)]
#[path = "realtime_appointment_tests.rs"]
mod appointment_tests;
