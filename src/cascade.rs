//! Isolated, bounded Twilio <Connect><Stream> bridge built from separate
//! speech-to-text, chat-model, and text-to-speech stages (for example a local
//! Whisper server, any chat-completions server, and a local Kokoro server).
//!
//! It replaces the single integrated Realtime session but keeps the same contract
//! for callers: the ingress must verify Twilio's upgrade signature and consume a
//! single-use nonce BEFORE calling `bridge`, the same options gate the fixed
//! `end_call`/`decline_recording` tools, and the result is the same
//! `BridgeOutcome`. This module reads no configuration, memory, files, or
//! environment variables, and never logs credentials, audio, or text.
//!
//! `Core` is a synchronous state machine that turns Twilio frames and stage
//! results into `Effect`s. The async driver only executes effects, so every
//! safety property below is covered by tests that need no network.
//!
//! Preserved from the Realtime bridge: caller barge-in with mark-acknowledged
//! playback accounting (`heard_audio_ms`), the two-phase `end_call` close with a
//! tool-free audible confirmation turn, the eight-second silence fallback,
//! immediate `decline_recording`, the deterministic recording-objection phrase
//! check, byte/mark/item ceilings, and the hard call-duration cap.
//!
//! One deliberate tightening: caller speech after an end request but before the
//! goodbye finished playing keeps the call open instead of ending it mid-sentence.

use crate::audio::{self, EndpointEvent, Endpointer};
use crate::realtime::{
    BridgeOutcome, BridgeResult, DECLINE_RECORDING_DESCRIPTION, END_CALL_DESCRIPTION,
    END_CONFIRM_INSTRUCTIONS, END_CONFIRM_SILENCE, EndReason, MAX_INSTRUCTIONS, MAX_ITEMS,
    MAX_MARKS, MAX_QUEUED_OUTPUT, MAX_SECONDS, MAX_TOTAL_INPUT, MAX_TOTAL_OUTPUT, MAX_TRANSCRIPT,
    PLAYBACK_CHUNK, TranscriptEntry, next_twilio, send_twilio, string, valid_sid, validate_start,
};
use axum::extract::ws::{Message as TwilioMessage, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::SinkExt;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout_at};

const MAX_MEDIA_PAYLOAD: usize = 8 * 1024;
/// Utterances waiting on a busy recognizer before the call fails closed.
const MAX_PENDING_UTTERANCES: usize = 4;
const MAX_STT_FAILURES: u8 = 3;
const MAX_SENTENCE_CHARS: usize = 400;
const MIN_SENTENCE_CHARS: usize = 20;
const DRAIN_GRACE: Duration = Duration::from_millis(2500);
const CONNECT_NUDGE: &str =
    "(The call has just connected. Begin now, following your instructions.)";
const INTERRUPTED_MARKER: &str = "[interrupted by the caller]";
const UNHEARD_MARKER: &str = "[interrupted before the caller heard any of it]";
const UNAVAILABLE_TRANSCRIPT: &str = "[Caller speech transcription unavailable]";
const COACH_NUDGE: &str = "(The owner just sent you private coaching. Act on it now.)";
const HANDBACK_NUDGE: &str =
    "(The owner has handed the call back to you. Continue from where things stand.)";
const COACH_HEADER: &str = "Owner coaching notes (trusted, from the owner on a private line; the other party never heard the coaching). Follow the latest coaching, never read it aloud verbatim, and never say you are being coached:";
const MAX_OWNER_NOTES: usize = 24;
const MAX_OWNER_NOTE_BYTES: usize = 6 * 1024;

/// Stage failures carry no detail: provider replies can contain private text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StageError;

/// Mu-law 8 kHz caller audio in, plain text out.
pub trait SpeechToText: Send + Sync {
    fn transcribe(&self, mulaw: Vec<u8>) -> BoxFuture<'_, Result<String, StageError>>;
}

/// One chat completion. `messages` are OpenAI-shaped; `tools` is an OpenAI
/// `tools` array that is empty when tool calling must be off for this turn.
pub trait ChatModel: Send + Sync {
    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatReply, StageError>>;
}

/// Text in, 24 kHz signed 16-bit little-endian mono PCM out.
pub trait Synthesizer: Send + Sync {
    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<Vec<u8>, StageError>>;
}

pub struct Pipeline {
    pub stt: Arc<dyn SpeechToText>,
    pub chat: Arc<dyn ChatModel>,
    pub tts: Arc<dyn Synthesizer>,
}

pub struct ChatRequest {
    pub messages: Vec<Value>,
    pub tools: Value,
}

pub struct ToolCall {
    pub name: String,
    pub arguments: String,
}

pub struct ChatReply {
    pub text: String,
    pub tool: Option<ToolCall>,
}

// Intentionally no Debug: the instructions are private.
pub struct CascadeOptions {
    pub instructions: String,
    pub expected_account_sid: String,
    pub expected_call_sid: String,
    pub max_duration_secs: u64,
    /// Allows only the fixed no-argument `end_call` function.
    pub allow_end_call: bool,
    /// Requires an audible close check and a reply (or bounded silence) first.
    pub confirm_end_call: bool,
    /// Caller refusal or withdrawal discards the transcript and ends immediately.
    pub stop_on_recording_decline: bool,
}

pub(crate) fn chat_tools(allow_end_call: bool, stop_on_recording_decline: bool) -> Value {
    let mut tools = Vec::new();
    let empty = json!({"type":"object","additionalProperties":false,"properties":{}});
    if allow_end_call {
        tools.push(json!({"type":"function","function":{"name":"end_call","description":END_CALL_DESCRIPTION,"parameters":empty}}));
    }
    if stop_on_recording_decline {
        tools.push(json!({"type":"function","function":{"name":"decline_recording","description":DECLINE_RECORDING_DESCRIPTION,"parameters":empty}}));
    }
    json!(tools)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallTool {
    EndCall,
    DeclineRecording,
}

impl CallTool {
    fn from_name(name: &str) -> BridgeResult<Self> {
        match name {
            "end_call" => Ok(Self::EndCall),
            "decline_recording" => Ok(Self::DeclineRecording),
            _ => Err(EndReason::ProtocolError),
        }
    }
}

pub(crate) struct TurnRequest {
    messages: Vec<Value>,
    tools: Value,
}

pub(crate) enum TurnEvent {
    Speech {
        text: String,
        mulaw: Vec<u8>,
    },
    Done {
        text: String,
        tool: Option<ToolCall>,
    },
    Failed,
}

pub(crate) enum Effect {
    Twilio(Value),
    Transcribe(Vec<u8>),
    StartTurn(TurnRequest),
    AbortTurn,
    /// Audio for the owner's coach leg; dropped when it cannot keep up.
    Owner(crate::coach::ToOwner),
}

struct Utterance {
    speaker: &'static str,
    text: String,
    /// Spoken sentences with the cumulative bytes sent when each one ends. Only
    /// sentences whose end was acknowledged by a playback mark count as heard.
    segments: Vec<(String, usize)>,
    sent_bytes: usize,
    played_bytes: usize,
    interrupted: bool,
    done: bool,
    history_index: Option<usize>,
}

impl Utterance {
    fn new(speaker: &'static str) -> Self {
        Self {
            speaker,
            text: String::new(),
            segments: Vec::new(),
            sent_bytes: 0,
            played_bytes: 0,
            interrupted: false,
            done: false,
            history_index: None,
        }
    }
}

struct PlaybackMark {
    item_index: usize,
    end_bytes: usize,
    chunk_bytes: usize,
}

struct ActiveTurn {
    item_index: usize,
    confirmation: bool,
}

#[derive(Default, PartialEq, Eq)]
enum ClosePhase {
    #[default]
    Open,
    Prompting,
    PromptPlayback,
    AwaitingReply,
    Ready,
}

pub(crate) struct Core {
    stream_sid: Option<String>,
    vad: Endpointer,
    speaking: bool,
    stt_inflight: bool,
    stt_queue: VecDeque<Vec<u8>>,
    stt_failures: u8,
    turn: Option<ActiveTurn>,
    items: Vec<Utterance>,
    marks: BTreeMap<String, PlaybackMark>,
    next_mark: u64,
    queued_output_bytes: usize,
    total_input_bytes: usize,
    total_output_bytes: usize,
    transcript_bytes: usize,
    history: Vec<Value>,
    instructions: String,
    allow_end_call: bool,
    confirm_end_call: bool,
    stop_on_recording_decline: bool,
    close_phase: ClosePhase,
    close_deadline: Option<Instant>,
    ending: bool,
    draining: bool,
    started: bool,
    /// The owner's coach leg is connected and hearing the call.
    owner_present: bool,
    /// The owner is speaking to the other party directly; the assistant is silent.
    takeover: bool,
    coach_pending: bool,
    owner_notes: VecDeque<String>,
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

impl Core {
    pub(crate) fn new(options: &CascadeOptions) -> Self {
        Self {
            stream_sid: None,
            vad: Endpointer::default(),
            speaking: false,
            stt_inflight: false,
            stt_queue: VecDeque::new(),
            stt_failures: 0,
            turn: None,
            items: Vec::new(),
            marks: BTreeMap::new(),
            next_mark: 0,
            queued_output_bytes: 0,
            total_input_bytes: 0,
            total_output_bytes: 0,
            transcript_bytes: 0,
            history: vec![json!({"role":"system","content":options.instructions})],
            instructions: options.instructions.clone(),
            allow_end_call: options.allow_end_call,
            confirm_end_call: options.confirm_end_call,
            stop_on_recording_decline: options.stop_on_recording_decline,
            close_phase: ClosePhase::Open,
            close_deadline: None,
            ending: false,
            draining: false,
            started: false,
            owner_present: false,
            takeover: false,
            coach_pending: false,
            owner_notes: VecDeque::new(),
        }
    }

    fn system_prompt(&self) -> String {
        if self.owner_notes.is_empty() {
            return self.instructions.clone();
        }
        let notes = json!(self.owner_notes);
        format!("{}\n\n{COACH_HEADER}\n{notes}", self.instructions)
    }

    pub(crate) fn start(&mut self, value: &Value, options: &CascadeOptions) -> BridgeResult<()> {
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

    /// The call is connected: the assistant speaks first, as with Realtime.
    pub(crate) fn begin(&mut self) -> BridgeResult<Vec<Effect>> {
        self.started = true;
        self.history
            .push(json!({"role":"user","content":CONNECT_NUDGE}));
        self.start_turn(false)
    }

    fn unanswered(&self) -> bool {
        self.history
            .last()
            .is_some_and(|message| message["role"] == "user")
    }

    fn start_turn(&mut self, confirmation: bool) -> BridgeResult<Vec<Effect>> {
        if self.items.len() >= MAX_ITEMS {
            return Err(EndReason::ResourceLimit);
        }
        let (messages, tools) = if confirmation {
            let mut messages = self.history.clone();
            messages[0] = json!({"role":"system","content":format!(
                "{}\n\nRuntime close protocol: {END_CONFIRM_INSTRUCTIONS}", self.system_prompt()
            )});
            (messages, json!([]))
        } else {
            (
                self.history.clone(),
                chat_tools(self.allow_end_call, self.stop_on_recording_decline),
            )
        };
        self.items.push(Utterance::new("assistant"));
        self.turn = Some(ActiveTurn {
            item_index: self.items.len() - 1,
            confirmation,
        });
        Ok(vec![Effect::StartTurn(TurnRequest { messages, tools })])
    }

    fn maybe_start_turn(&mut self) -> BridgeResult<Vec<Effect>> {
        if !self.started
            || self.draining
            || self.speaking
            || self.stt_inflight
            || !self.stt_queue.is_empty()
            || self.turn.is_some()
            || self.takeover
            || !self.unanswered()
        {
            return Ok(Vec::new());
        }
        self.start_turn(false)
    }

    fn set_text(&mut self, index: usize, text: &str) -> BridgeResult<()> {
        let next = self.transcript_bytes - self.items[index].text.len() + text.len();
        if next > MAX_TRANSCRIPT {
            return Err(EndReason::ResourceLimit);
        }
        self.transcript_bytes = next;
        self.items[index].text = text.to_owned();
        Ok(())
    }

    fn hear(&mut self, text: &str) -> BridgeResult<()> {
        if self.stop_on_recording_decline && crate::protocol::recording_declined(text) {
            return Err(EndReason::RecordingDeclined);
        }
        if self.items.len() >= MAX_ITEMS {
            return Err(EndReason::ResourceLimit);
        }
        self.items.push(Utterance::new("caller"));
        let index = self.items.len() - 1;
        self.items[index].done = true;
        self.set_text(index, text)?;
        match self.history.last_mut() {
            Some(last)
                if last["role"] == "user"
                    && [CONNECT_NUDGE, COACH_NUDGE, HANDBACK_NUDGE]
                        .contains(&last["content"].as_str().unwrap_or_default()) =>
            {
                last["content"] = json!(text);
            }
            Some(last) if last["role"] == "user" => {
                // Several utterances before one answer read as a single turn, and
                // strict chat templates reject consecutive user messages.
                let merged = format!("{} {text}", last["content"].as_str().unwrap_or_default());
                last["content"] = json!(merged);
            }
            _ => self.history.push(json!({"role":"user","content":text})),
        }
        Ok(())
    }

    fn queue_utterance(&mut self, audio: Vec<u8>) -> BridgeResult<Vec<Effect>> {
        if self.stt_inflight {
            if self.stt_queue.len() >= MAX_PENDING_UTTERANCES {
                return Err(EndReason::ResourceLimit);
            }
            self.stt_queue.push_back(audio);
            return Ok(Vec::new());
        }
        self.stt_inflight = true;
        Ok(vec![Effect::Transcribe(audio)])
    }

    /// Hangup mid-sentence: submit what was heard.
    pub(crate) fn flush_speech(&mut self) -> BridgeResult<Vec<Effect>> {
        self.speaking = false;
        match self.vad.flush() {
            Some(audio) => self.queue_utterance(audio),
            None => Ok(Vec::new()),
        }
    }

    pub(crate) fn transcribed(
        &mut self,
        result: Result<String, StageError>,
    ) -> BridgeResult<Vec<Effect>> {
        self.stt_inflight = false;
        match result {
            Ok(text) => {
                self.stt_failures = 0;
                let text = text.trim();
                if !text.is_empty() {
                    self.hear(text)?;
                }
            }
            Err(_) => {
                self.stt_failures += 1;
                if self.stt_failures >= MAX_STT_FAILURES {
                    return Err(EndReason::UpstreamError);
                }
                self.hear(UNAVAILABLE_TRANSCRIPT)?;
            }
        }
        let mut effects = Vec::new();
        if let Some(audio) = self.stt_queue.pop_front() {
            self.stt_inflight = true;
            effects.push(Effect::Transcribe(audio));
        }
        effects.extend(self.maybe_start_turn()?);
        Ok(effects)
    }

    fn heard_text(&self, index: usize) -> String {
        let item = &self.items[index];
        item.segments
            .iter()
            .take_while(|(_, end)| *end <= item.played_bytes)
            .map(|(text, _)| text.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// The model must not believe the caller heard words they talked over.
    fn settle_interrupted(&mut self, index: usize) {
        let heard = self.heard_text(index);
        let content = if heard.is_empty() {
            UNHEARD_MARKER.to_owned()
        } else {
            format!("{heard} {INTERRUPTED_MARKER}")
        };
        match self.items[index].history_index {
            Some(position) => self.history[position]["content"] = json!(content),
            None if !heard.is_empty() => {
                self.history
                    .push(json!({"role":"assistant","content":content}));
                self.items[index].history_index = Some(self.history.len() - 1);
            }
            None => {}
        }
    }

    fn interrupt(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        let aborted = self.turn.take();
        if aborted.is_some() {
            effects.push(Effect::AbortTurn);
        }
        // Purge BEFORE sending clear: Twilio acknowledges discarded marks too.
        // Monotonic names are never reused, so late acknowledgements cannot
        // advance the playback position of later audio.
        let had_audio = !self.marks.is_empty();
        self.marks.clear();
        self.queued_output_bytes = 0;
        if had_audio {
            effects.push(Effect::Twilio(
                json!({"event":"clear","streamSid":self.stream_sid}),
            ));
            if self.owner_present {
                effects.push(Effect::Owner(crate::coach::ToOwner::ClearAssistant));
            }
        }
        let aborted_index = aborted.map(|turn| turn.item_index);
        for index in 0..self.items.len() {
            let item = &self.items[index];
            if item.speaker == "assistant"
                && !item.interrupted
                && (item.sent_bytes > item.played_bytes || aborted_index == Some(index))
            {
                self.items[index].interrupted = true;
                self.settle_interrupted(index);
            }
        }
        effects
    }

    fn speech_started(&mut self) -> Vec<Effect> {
        self.speaking = true;
        // Speech after the first close request is a caller turn: it may be an
        // explicit goodbye or a continuation. Either way the model answers, and
        // final-close authorization stays until it invokes the tool again.
        if matches!(
            self.close_phase,
            ClosePhase::Prompting | ClosePhase::PromptPlayback | ClosePhase::AwaitingReply
        ) {
            self.close_phase = ClosePhase::Ready;
            self.close_deadline = None;
        }
        self.ending = false;
        self.interrupt()
    }

    fn speech_stopped(&mut self, audio: Option<Vec<u8>>) -> BridgeResult<Vec<Effect>> {
        self.speaking = false;
        match audio {
            Some(audio) => self.queue_utterance(audio),
            // A click interrupted playback or an in-flight reply: answer again.
            None => self.maybe_start_turn(),
        }
    }

    fn response_was_audible(&self, index: usize) -> bool {
        let item = &self.items[index];
        item.done && item.sent_bytes > 0 && !item.interrupted
    }

    fn start_close_wait_if_played(&mut self) {
        if self.close_phase == ClosePhase::PromptPlayback && self.marks.is_empty() {
            self.close_phase = ClosePhase::AwaitingReply;
            self.close_deadline = Some(Instant::now() + END_CONFIRM_SILENCE);
        }
    }

    pub(crate) fn close_timeout(&self) -> Option<Instant> {
        if self.close_phase == ClosePhase::AwaitingReply {
            self.close_deadline
        } else {
            None
        }
    }

    fn ended_if_finished(&self) -> BridgeResult<()> {
        if self.ending && self.turn.is_none() && self.marks.is_empty() {
            Err(EndReason::AssistantEnded)
        } else {
            Ok(())
        }
    }

    pub(crate) fn twilio(&mut self, value: Value) -> BridgeResult<Vec<Effect>> {
        if value["streamSid"].as_str() != self.stream_sid.as_deref() {
            return Err(EndReason::ProtocolError);
        }
        match string(&value, "event")? {
            "media" => {
                if value["media"]["track"] != "inbound" {
                    return Err(EndReason::ProtocolError);
                }
                let bytes = decode_media(string(&value["media"], "payload")?)?;
                self.total_input_bytes += bytes.len();
                if self.total_input_bytes > MAX_TOTAL_INPUT {
                    return Err(EndReason::ResourceLimit);
                }
                let mut effects = Vec::new();
                if self.owner_present {
                    effects.push(Effect::Owner(crate::coach::ToOwner::Callee(bytes.clone())));
                }
                for event in self.vad.push(&bytes) {
                    match event {
                        EndpointEvent::SpeechStarted => effects.extend(self.speech_started()),
                        EndpointEvent::SpeechStopped(audio) => {
                            effects.extend(self.speech_stopped(audio)?)
                        }
                    }
                }
                Ok(effects)
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
                self.ended_if_finished()?;
                Ok(Vec::new())
            }
            "stop" => Err(EndReason::CallEnded),
            "dtmf" => Ok(Vec::new()), // Consent belongs to the signed ingress.
            _ => Err(EndReason::ProtocolError),
        }
    }

    pub(crate) fn turn_event(&mut self, event: TurnEvent) -> BridgeResult<Vec<Effect>> {
        if self.turn.is_none() {
            return Err(EndReason::ProtocolError);
        }
        match event {
            TurnEvent::Speech { text, mulaw } => self.speech(text, mulaw),
            TurnEvent::Done { text, tool } => self.done(text, tool),
            TurnEvent::Failed => Err(EndReason::UpstreamError),
        }
    }

    fn speech(&mut self, text: String, mulaw: Vec<u8>) -> BridgeResult<Vec<Effect>> {
        let index = self
            .turn
            .as_ref()
            .ok_or(EndReason::ProtocolError)?
            .item_index;
        if self.queued_output_bytes + mulaw.len() > MAX_QUEUED_OUTPUT
            || self.total_output_bytes + mulaw.len() > MAX_TOTAL_OUTPUT
            || self.marks.len() + mulaw.len().div_ceil(PLAYBACK_CHUNK) > MAX_MARKS
        {
            return Err(EndReason::ResourceLimit);
        }
        self.queued_output_bytes += mulaw.len();
        self.total_output_bytes += mulaw.len();
        let mut effects = Vec::new();
        if self.owner_present {
            effects.push(Effect::Owner(crate::coach::ToOwner::Assistant(mulaw.clone())));
        }
        for chunk in mulaw.chunks(PLAYBACK_CHUNK) {
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
            effects.push(Effect::Twilio(json!({
                "event":"media","streamSid":self.stream_sid,
                "media":{"payload":STANDARD.encode(chunk)}
            })));
            effects.push(Effect::Twilio(json!({
                "event":"mark","streamSid":self.stream_sid,"mark":{"name":name}
            })));
        }
        let sent = self.items[index].sent_bytes;
        let mut spoken = self.items[index].text.clone();
        if !spoken.is_empty() {
            spoken.push(' ');
        }
        spoken.push_str(&text);
        self.set_text(index, &spoken)?;
        self.items[index].segments.push((text, sent));
        Ok(effects)
    }

    fn done(&mut self, text: String, tool: Option<ToolCall>) -> BridgeResult<Vec<Effect>> {
        let active = self.turn.take().ok_or(EndReason::ProtocolError)?;
        let index = active.item_index;
        self.items[index].done = true;
        if !text.is_empty() {
            self.set_text(index, &text)?;
            self.history
                .push(json!({"role":"assistant","content":text}));
            self.items[index].history_index = Some(self.history.len() - 1);
        }
        if active.confirmation {
            // The confirmation turn is tool-free; a tool call from a model that
            // emits one anyway is ignored rather than trusted.
            if self.response_was_audible(index) {
                self.close_phase = ClosePhase::PromptPlayback;
                self.start_close_wait_if_played();
            } else {
                self.close_phase = ClosePhase::Open;
            }
            return Ok(Vec::new());
        }
        let mut effects = Vec::new();
        if let Some(call) = tool {
            let tool = CallTool::from_name(&call.name)?;
            let allowed = match tool {
                CallTool::EndCall => self.allow_end_call,
                CallTool::DeclineRecording => self.stop_on_recording_decline,
            };
            let arguments: Value = if call.arguments.trim().is_empty() {
                json!({})
            } else {
                serde_json::from_str(&call.arguments).map_err(|_| EndReason::ProtocolError)?
            };
            if !allowed || arguments != json!({}) {
                return Err(EndReason::ProtocolError);
            }
            match tool {
                // A refusal must not wait for playback or a close check.
                CallTool::DeclineRecording => return Err(EndReason::RecordingDeclined),
                CallTool::EndCall
                    if self.confirm_end_call && self.close_phase != ClosePhase::Ready =>
                {
                    self.close_phase = ClosePhase::Prompting;
                    self.close_deadline = None;
                    effects.extend(self.start_turn(true)?);
                    return Ok(effects);
                }
                CallTool::EndCall => self.ending = true,
            }
        }
        self.ended_if_finished()?;
        effects.extend(self.coach_followup()?);
        Ok(effects)
    }

    /// Answer pending owner coaching once the assistant is free to speak.
    fn coach_followup(&mut self) -> BridgeResult<Vec<Effect>> {
        if !self.coach_pending
            || self.takeover
            || self.ending
            || self.turn.is_some()
            || self.close_phase != ClosePhase::Open
        {
            return Ok(Vec::new());
        }
        self.coach_pending = false;
        if !self.unanswered() {
            self.history.push(json!({"role":"user","content":COACH_NUDGE}));
        }
        self.maybe_start_turn()
    }

    fn add_owner_note(&mut self, note: String) {
        self.owner_notes.push_back(note);
        let mut bytes: usize = self.owner_notes.iter().map(String::len).sum();
        while self.owner_notes.len() > MAX_OWNER_NOTES
            || (bytes > MAX_OWNER_NOTE_BYTES && self.owner_notes.len() > 1)
        {
            bytes -= self.owner_notes.pop_front().map_or(0, |n| n.len());
        }
        self.history[0] = json!({"role":"system","content":self.system_prompt()});
    }

    /// One event from the owner's coach leg.
    pub(crate) fn owner(&mut self, event: crate::coach::FromOwner) -> BridgeResult<Vec<Effect>> {
        use crate::coach::FromOwner;
        match event {
            FromOwner::Joined => {
                self.owner_present = true;
                Ok(Vec::new())
            }
            FromOwner::Speech(text) => {
                let text: String = text.chars().filter(|c| !c.is_control()).take(1000).collect();
                let text = text.trim();
                if text.is_empty() || !self.started || self.items.len() >= MAX_ITEMS {
                    return Ok(Vec::new());
                }
                self.items.push(Utterance::new("owner"));
                let index = self.items.len() - 1;
                self.items[index].done = true;
                self.set_text(index, text)?;
                if self.takeover {
                    self.add_owner_note(format!("The owner said directly to the other party: {text}"));
                    Ok(Vec::new())
                } else {
                    self.add_owner_note(format!("Coaching: {text}"));
                    self.coach_pending = true;
                    self.coach_followup()
                }
            }
            FromOwner::Audio(bytes) => {
                if !self.takeover || bytes.is_empty() || bytes.len() > MAX_MEDIA_PAYLOAD {
                    return Ok(Vec::new());
                }
                Ok(vec![Effect::Twilio(json!({
                    "event":"media","streamSid":self.stream_sid,
                    "media":{"payload":STANDARD.encode(bytes)}
                }))])
            }
            FromOwner::TakeOver => {
                if self.takeover {
                    return Ok(Vec::new());
                }
                self.takeover = true;
                self.coach_pending = false;
                self.ending = false;
                self.close_phase = ClosePhase::Open;
                self.close_deadline = None;
                Ok(self.interrupt())
            }
            FromOwner::HandBack => self.hand_back(),
        }
    }

    fn hand_back(&mut self) -> BridgeResult<Vec<Effect>> {
        if !self.takeover {
            return Ok(Vec::new());
        }
        self.takeover = false;
        if !self.unanswered() {
            self.history.push(json!({"role":"user","content":HANDBACK_NUDGE}));
        }
        self.maybe_start_turn()
    }

    /// The coach leg hung up: stop forwarding and give the call back to the AI.
    pub(crate) fn owner_left(&mut self) -> BridgeResult<Vec<Effect>> {
        self.owner_present = false;
        self.hand_back()
    }

    pub(crate) fn outcome(self, reason: EndReason, duration_ms: u64) -> BridgeOutcome {
        if reason == EndReason::RecordingDeclined {
            return BridgeOutcome {
                transcript: Vec::new(),
                reason,
                duration_ms,
                model_session_ready: self.started,
            };
        }
        let transcript = self
            .items
            .into_iter()
            .filter_map(|item| {
                let assistant = item.speaker == "assistant";
                if assistant && item.text.is_empty() && item.sent_bytes == 0 {
                    return None;
                }
                Some(TranscriptEntry {
                    speaker: item.speaker.to_owned(),
                    text: if item.text.is_empty() {
                        "[Assistant audio transcript unavailable]".to_owned()
                    } else {
                        item.text
                    },
                    interrupted: item.interrupted
                        || (assistant && (item.sent_bytes > item.played_bytes || !item.done)),
                    heard_audio_ms: assistant.then_some((item.played_bytes / 8) as u64),
                })
            })
            .collect();
        BridgeOutcome {
            transcript,
            reason,
            duration_ms,
            model_session_ready: self.started,
        }
    }
}

/// Strip markup a model may emit that a speech engine would read aloud.
pub(crate) fn clean_speech(text: &str) -> String {
    let stripped: String = text
        .chars()
        .filter(|c| !matches!(c, '*' | '_' | '#' | '`' | '~'))
        .collect();
    stripped.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Sentence-sized chunks so the first one can be spoken while later ones are
/// still being synthesized. Fragments shorter than a phrase join a neighbour
/// ("Dr." + "Smith will call back.").
pub(crate) fn split_sentences(text: &str) -> Vec<String> {
    let mut sentences: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        current.push(c);
        let boundary = matches!(c, '.' | '!' | '?' | '\u{2026}')
            && chars.peek().is_none_or(|next| next.is_whitespace());
        let too_long = current.len() >= MAX_SENTENCE_CHARS && c.is_whitespace();
        if boundary || too_long {
            let done = std::mem::take(&mut current);
            let done = done.trim();
            if !done.is_empty() {
                sentences.push(done.to_owned());
            }
        }
    }
    let tail = current.trim();
    if !tail.is_empty() {
        sentences.push(tail.to_owned());
    }
    let mut merged: Vec<String> = Vec::new();
    let mut carry = String::new();
    for sentence in sentences {
        if !carry.is_empty() {
            carry.push(' ');
        }
        carry.push_str(&sentence);
        if carry.len() >= MIN_SENTENCE_CHARS {
            merged.push(std::mem::take(&mut carry));
        }
    }
    if !carry.is_empty() {
        // A trailing scrap ("Thanks!") is better prosody attached to its sentence.
        match merged.last_mut() {
            Some(last) if carry.len() < MIN_SENTENCE_CHARS => {
                last.push(' ');
                last.push_str(&carry);
            }
            _ => merged.push(carry),
        }
    }
    merged
}

/// One assistant turn: chat completion, then per-sentence synthesis streamed to
/// the bridge as it becomes available. Aborting the task drops in-flight HTTP.
pub(crate) async fn run_turn(
    pipeline: Arc<Pipeline>,
    request: TurnRequest,
    events: mpsc::Sender<TurnEvent>,
) {
    let outcome: Result<(String, Option<ToolCall>), StageError> = async {
        let reply = pipeline
            .chat
            .complete(ChatRequest {
                messages: request.messages,
                tools: request.tools,
            })
            .await?;
        let text = clean_speech(&reply.text);
        for sentence in split_sentences(&text) {
            let pcm = pipeline.tts.synthesize(sentence.clone()).await?;
            let mulaw = audio::pcm24k_to_mulaw(&pcm);
            if mulaw.is_empty() {
                continue;
            }
            events
                .send(TurnEvent::Speech {
                    text: sentence,
                    mulaw,
                })
                .await
                .map_err(|_| StageError)?;
        }
        Ok((text, reply.tool))
    }
    .await;
    let last = match outcome {
        Ok((text, tool)) => TurnEvent::Done { text, tool },
        Err(_) => TurnEvent::Failed,
    };
    let _ = events.send(last).await;
}

/// Aborts its task when dropped, so no stage outlives the call.
struct Task<T>(JoinHandle<T>);

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct TurnTask {
    events: mpsc::Receiver<TurnEvent>,
    _task: Task<()>,
}

#[derive(Default)]
struct Stages {
    stt: Option<Task<Result<String, StageError>>>,
    turn: Option<TurnTask>,
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

async fn next_turn_event(turn: &mut Option<TurnTask>) -> Option<TurnEvent> {
    match turn {
        Some(task) => task.events.recv().await,
        None => std::future::pending().await,
    }
}

async fn next_owner_event(
    coach: &mut Option<crate::coach::MainLink>,
) -> Option<crate::coach::FromOwner> {
    match coach {
        Some(link) => link.from_owner.recv().await,
        None => std::future::pending().await,
    }
}

fn spawn_transcription(stages: &mut Stages, pipeline: &Arc<Pipeline>, audio: Vec<u8>) {
    let pipeline = pipeline.clone();
    stages.stt = Some(Task(tokio::spawn(async move {
        pipeline.stt.transcribe(audio).await
    })));
}

async fn execute(
    twilio: &mut WebSocket,
    stages: &mut Stages,
    pipeline: &Arc<Pipeline>,
    coach: &Option<crate::coach::MainLink>,
    effects: Vec<Effect>,
    deadline: Instant,
) -> BridgeResult<()> {
    for effect in effects {
        match effect {
            Effect::Twilio(value) => {
                send_twilio(
                    twilio,
                    TwilioMessage::Text(value.to_string().into()),
                    deadline,
                )
                .await?;
            }
            Effect::Transcribe(audio) => spawn_transcription(stages, pipeline, audio),
            Effect::StartTurn(request) => {
                let (sender, events) = mpsc::channel(8);
                let pipeline = pipeline.clone();
                let task = Task(tokio::spawn(run_turn(pipeline, request, sender)));
                stages.turn = Some(TurnTask {
                    events,
                    _task: task,
                });
            }
            Effect::AbortTurn => stages.turn = None,
            Effect::Owner(message) => {
                if let Some(link) = coach {
                    let _ = link.to_owner.try_send(message);
                }
            }
        }
    }
    Ok(())
}

async fn run_bridge(
    twilio: &mut WebSocket,
    options: &CascadeOptions,
    core: &mut Core,
    stages: &mut Stages,
    pipeline: &Arc<Pipeline>,
    coach: &mut Option<crate::coach::MainLink>,
    deadline: Instant,
) -> BridgeResult<()> {
    let setup_deadline = deadline.min(Instant::now() + Duration::from_secs(10));
    // The initial connected envelope is optional; nothing is accepted until the
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
    core.start(&start, options)?;
    let effects = core.begin()?;
    execute(twilio, stages, pipeline, coach, effects, setup_deadline).await?;
    loop {
        let close_timeout = core.close_timeout();
        let current_deadline = close_timeout
            .map(|value| value.min(deadline))
            .unwrap_or(deadline);
        let effects = tokio::select! {
            _ = tokio::time::sleep_until(current_deadline) => {
                if close_timeout.is_some_and(|value| value <= deadline && Instant::now() >= value) {
                    return Err(EndReason::AssistantEnded);
                }
                return Err(EndReason::DurationLimit);
            }
            value = next_twilio(twilio, current_deadline) => core.twilio(value?)?,
            result = join_stt(&mut stages.stt) => core.transcribed(result)?,
            event = next_turn_event(&mut stages.turn) => match event {
                Some(event) => {
                    if matches!(event, TurnEvent::Done { .. } | TurnEvent::Failed) {
                        stages.turn = None;
                    }
                    core.turn_event(event)?
                }
                // The turn task ended without a final event.
                None => return Err(EndReason::UpstreamError),
            },
            event = next_owner_event(coach) => match event {
                Some(event) => core.owner(event)?,
                None => {
                    *coach = None;
                    core.owner_left()?
                }
            },
        };
        execute(twilio, stages, pipeline, coach, effects, current_deadline).await?;
    }
}

/// Small, bounded post-hangup grace period for the caller's last words. No new
/// reply is generated and nothing is sent to the carrier during this phase.
async fn drain_transcripts(
    core: &mut Core,
    stages: &mut Stages,
    pipeline: &Arc<Pipeline>,
    deadline: Instant,
) -> Option<EndReason> {
    if !core.started || Instant::now() >= deadline {
        return None;
    }
    core.draining = true;
    stages.turn = None;
    let grace = deadline.min(Instant::now() + DRAIN_GRACE);
    let mut pending = core.flush_speech().unwrap_or_default();
    loop {
        for effect in pending.drain(..) {
            if let Effect::Transcribe(audio) = effect {
                spawn_transcription(stages, pipeline, audio);
            }
        }
        stages.stt.as_ref()?;
        let Ok(result) = timeout_at(grace, join_stt(&mut stages.stt)).await else {
            return None;
        };
        match core.transcribed(result) {
            Ok(effects) => pending = effects,
            Err(EndReason::RecordingDeclined) => return Some(EndReason::RecordingDeclined),
            Err(_) => return None,
        }
    }
}

/// Bridge one already-authorized call. The hard cap can be shortened, not raised.
/// Outcomes contain private transcripts and should only be persisted privately.
pub async fn bridge(
    socket: WebSocket,
    options: CascadeOptions,
    pipeline: Arc<Pipeline>,
) -> BridgeOutcome {
    coached_bridge(socket, options, pipeline, None).await
}

/// `bridge` with an optional owner coach leg. Dropping the link when the call
/// ends closes the owner leg too.
pub async fn coached_bridge(
    mut socket: WebSocket,
    options: CascadeOptions,
    pipeline: Arc<Pipeline>,
    mut coach: Option<crate::coach::MainLink>,
) -> BridgeOutcome {
    let started = Instant::now();
    let deadline = started + Duration::from_secs(options.max_duration_secs.min(MAX_SECONDS));
    let mut core = Core::new(&options);
    let mut stages = Stages::default();
    let valid = options.max_duration_secs > 0
        && !options.instructions.trim().is_empty()
        && options.instructions.len() <= MAX_INSTRUCTIONS
        && (!options.confirm_end_call || options.allow_end_call)
        && valid_sid(&options.expected_account_sid, "AC")
        && valid_sid(&options.expected_call_sid, "CA");
    let mut reason = if valid {
        run_bridge(
            &mut socket,
            &options,
            &mut core,
            &mut stages,
            &pipeline,
            &mut coach,
            deadline,
        )
        .await
        .err()
        .unwrap_or(EndReason::CallEnded)
    } else {
        EndReason::InvalidOptions
    };
    if reason == EndReason::IoTimeout && Instant::now() >= deadline {
        reason = EndReason::DurationLimit;
    }
    if matches!(
        reason,
        EndReason::CallEnded | EndReason::PeerClosed | EndReason::AssistantEnded
    ) && let Some(drain_reason) =
        drain_transcripts(&mut core, &mut stages, &pipeline, deadline).await
    {
        reason = drain_reason;
    }
    drop(stages);
    drop(coach);
    if reason == EndReason::RecordingDeclined {
        // Stop the carrier first and discard the whole call when building the outcome.
        let _ = timeout_at(
            Instant::now() + Duration::from_millis(250),
            socket.send(TwilioMessage::Close(None)),
        )
        .await;
    } else {
        let _ = timeout_at(Instant::now() + Duration::from_millis(250), socket.close()).await;
    }
    core.outcome(
        reason,
        started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    )
}

#[cfg(test)]
mod tests;
