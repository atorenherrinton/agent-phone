use super::*;
use crate::audio::{test_silence, test_tone};
use std::sync::Mutex;

const INSTRUCTIONS: &str =
    "Isolated call policy. Disclose that you are an AI assistant before anything else.";

fn options() -> CascadeOptions {
    CascadeOptions {
        instructions: INSTRUCTIONS.into(),
        expected_account_sid: format!("AC{}", "1".repeat(32)),
        expected_call_sid: format!("CA{}", "2".repeat(32)),
        max_duration_secs: 180,
        allow_end_call: true,
        confirm_end_call: false,
        stop_on_recording_decline: false,
    }
}

fn stream_sid() -> String {
    format!("MZ{}", "3".repeat(32))
}

fn start_event(o: &CascadeOptions) -> Value {
    let stream = stream_sid();
    json!({"event":"start","streamSid":stream,"start":{
        "streamSid":stream,"accountSid":o.expected_account_sid,
        "callSid":o.expected_call_sid,"tracks":["inbound"],
        "mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1}
    }})
}

fn started(o: &CascadeOptions) -> (Core, Vec<Effect>) {
    let mut core = Core::new(o);
    core.start(&start_event(o), o).unwrap();
    let effects = core.begin().unwrap();
    (core, effects)
}

fn media(core: &mut Core, mulaw: &[u8]) -> BridgeResult<Vec<Effect>> {
    core.twilio(json!({
        "event":"media","streamSid":stream_sid(),
        "media":{"track":"inbound","payload":STANDARD.encode(mulaw)}
    }))
}

fn mark(core: &mut Core, name: &str) -> BridgeResult<Vec<Effect>> {
    core.twilio(json!({"event":"mark","streamSid":stream_sid(),"mark":{"name":name}}))
}

/// The caller says something: speech, then a pause that ends the utterance.
fn talk(core: &mut Core) -> Vec<Effect> {
    let mut effects = media(core, &test_tone(12, 6_000.0)).unwrap();
    effects.extend(media(core, &test_silence(26)).unwrap());
    effects
}

/// A caller utterance that recognition returns as `text`.
fn say(core: &mut Core, text: &str) -> Vec<Effect> {
    let mut effects = talk(core);
    assert!(effects.iter().any(|e| matches!(e, Effect::Transcribe(_))));
    effects.extend(core.transcribed(Ok(text.into())).unwrap());
    effects
}

fn turn_request(effects: &[Effect]) -> Option<&TurnRequest> {
    effects.iter().find_map(|effect| match effect {
        Effect::StartTurn(request) => Some(request),
        _ => None,
    })
}

fn twilio_events<'a>(effects: &'a [Effect], kind: &str) -> Vec<&'a Value> {
    effects
        .iter()
        .filter_map(|effect| match effect {
            Effect::Twilio(value) if value["event"] == kind => Some(value),
            _ => None,
        })
        .collect()
}

fn has_abort(effects: &[Effect]) -> bool {
    effects.iter().any(|e| matches!(e, Effect::AbortTurn))
}

fn speech(text: &str, bytes: usize) -> TurnEvent {
    TurnEvent::Speech {
        text: text.into(),
        mulaw: vec![0xff; bytes],
    }
}

fn done(text: &str, tool: Option<&str>) -> TurnEvent {
    TurnEvent::Done {
        text: text.into(),
        tool: tool.map(|name| ToolCall {
            name: name.into(),
            arguments: "{}".into(),
        }),
    }
}

fn mark_name(effects: &[Effect], index: usize) -> String {
    twilio_events(effects, "mark")[index]["mark"]["name"]
        .as_str()
        .unwrap()
        .to_owned()
}

fn last_history(core: &Core) -> &Value {
    core.history.last().unwrap()
}

#[test]
fn start_event_is_bound_to_the_expected_call_and_media_format() {
    let o = options();
    let mut core = Core::new(&o);
    for mutate in [
        |v: &mut Value| v["start"]["accountSid"] = json!(format!("AC{}", "9".repeat(32))),
        |v: &mut Value| v["start"]["callSid"] = json!(format!("CA{}", "9".repeat(32))),
        |v: &mut Value| v["start"]["mediaFormat"]["sampleRate"] = json!(16000),
        |v: &mut Value| v["start"]["tracks"] = json!(["inbound", "outbound"]),
        |v: &mut Value| v["event"] = json!("connected"),
    ] {
        let mut event = start_event(&o);
        mutate(&mut event);
        assert_eq!(core.start(&event, &o), Err(EndReason::ProtocolError));
    }
    assert!(core.start(&start_event(&o), &o).is_ok());
    assert_eq!(
        core.start(&start_event(&o), &o),
        Err(EndReason::ProtocolError),
        "a second start cannot rebind the stream"
    );
}

#[test]
fn frames_for_another_stream_or_track_are_rejected() {
    let o = options();
    let (mut core, _) = started(&o);
    assert_eq!(
        core.twilio(json!({"event":"stop","streamSid":format!("MZ{}", "4".repeat(32))}))
            .err(),
        Some(EndReason::ProtocolError)
    );
    assert_eq!(
        core.twilio(json!({"event":"media","streamSid":stream_sid(),
            "media":{"track":"outbound","payload":STANDARD.encode([0xffu8; 160])}}))
            .err(),
        Some(EndReason::ProtocolError)
    );
    assert_eq!(
        core.twilio(json!({"event":"bogus","streamSid":stream_sid()}))
            .err(),
        Some(EndReason::ProtocolError)
    );
    assert!(
        core.twilio(json!({"event":"dtmf","streamSid":stream_sid()}))
            .is_ok()
    );
    assert_eq!(
        core.twilio(json!({"event":"stop","streamSid":stream_sid()}))
            .err(),
        Some(EndReason::CallEnded)
    );
}

#[test]
fn assistant_speaks_first_and_the_policy_reaches_the_model_verbatim() {
    let o = options();
    let (core, effects) = started(&o);
    let request = turn_request(&effects).expect("initial turn");
    assert_eq!(request.messages[0]["role"], "system");
    assert_eq!(request.messages[0]["content"], INSTRUCTIONS);
    assert_eq!(request.messages.last().unwrap()["role"], "user");
    let names: Vec<_> = request
        .tools
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["function"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["end_call"]);
    assert!(core.started);
}

#[test]
fn tool_definitions_follow_the_options() {
    assert!(chat_tools(false, false).as_array().unwrap().is_empty());
    let both = chat_tools(true, true);
    let names: Vec<_> = both
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["function"]["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["end_call", "decline_recording"]);
    for tool in both.as_array().unwrap() {
        assert_eq!(
            tool["function"]["parameters"]["additionalProperties"],
            false
        );
        assert!(
            tool["function"]["parameters"]["properties"]
                .as_object()
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn assistant_audio_is_chunked_with_marks_and_heard_time_follows_acknowledgements() {
    let o = options();
    let (mut core, _) = started(&o);
    let effects = core.turn_event(speech("Hello.", 2_000)).unwrap();
    let media_frames = twilio_events(&effects, "media");
    assert_eq!(media_frames.len(), 3, "800 + 800 + 400 bytes");
    assert_eq!(twilio_events(&effects, "mark").len(), 3);
    assert_eq!(
        STANDARD
            .decode(media_frames[2]["media"]["payload"].as_str().unwrap())
            .unwrap()
            .len(),
        400
    );
    core.turn_event(done("Hello.", None)).unwrap();
    assert!(mark(&mut core, "zc-play-999").unwrap().is_empty());
    mark(&mut core, &mark_name(&effects, 0)).unwrap();
    let outcome = core.outcome(EndReason::CallEnded, 1);
    let entry = &outcome.transcript[0];
    assert_eq!(entry.speaker, "assistant");
    assert_eq!(entry.heard_audio_ms, Some(100));
    assert!(
        entry.interrupted,
        "unacknowledged audio is never assumed heard"
    );
}

#[test]
fn caller_barge_in_clears_playback_aborts_the_turn_and_corrects_history() {
    let o = options();
    let (mut core, _) = started(&o);
    let first = core
        .turn_event(speech("Hello, I am an AI assistant.", 800))
        .unwrap();
    core.turn_event(speech("Calling for the owner.", 800))
        .unwrap();
    core.turn_event(done(
        "Hello, I am an AI assistant. Calling for the owner.",
        None,
    ))
    .unwrap();
    mark(&mut core, &mark_name(&first, 0)).unwrap();

    let effects = media(&mut core, &test_tone(12, 6_000.0)).unwrap();
    assert!(twilio_events(&effects, "clear").len() == 1);
    assert!(!has_abort(&effects), "the turn already finished");
    assert_eq!(
        last_history(&core)["content"],
        "Hello, I am an AI assistant. [interrupted by the caller]"
    );
    // Marks were purged, so a late acknowledgement cannot advance playback.
    assert!(mark(&mut core, "zc-play-2").unwrap().is_empty());
    let outcome = core.outcome(EndReason::CallEnded, 1);
    assert_eq!(outcome.transcript[0].heard_audio_ms, Some(100));
    assert!(outcome.transcript[0].interrupted);
}

#[test]
fn barge_in_during_generation_aborts_the_turn() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(speech("Thinking out loud, sorry.", 800))
        .unwrap();
    let effects = media(&mut core, &test_tone(12, 6_000.0)).unwrap();
    assert!(has_abort(&effects));
    assert_eq!(twilio_events(&effects, "clear").len(), 1);
    // Nothing was acknowledged, so nothing is remembered as heard.
    assert_ne!(last_history(&core)["role"], "assistant");
    assert_eq!(
        core.turn_event(done("late", None)).err(),
        Some(EndReason::ProtocolError),
        "events from an aborted turn are refused"
    );
}

#[test]
fn caller_speech_is_answered_once_it_stops_and_merges_while_unanswered() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(done("Hello there.", None)).unwrap();
    let effects = say(&mut core, "hi, who is this");
    let request = turn_request(&effects).expect("reply turn");
    let roles: Vec<_> = request
        .messages
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["system", "user", "assistant", "user"]);
    assert_eq!(request.messages[3]["content"], "hi, who is this");

    // The caller keeps talking while the reply is still being generated.
    let effects = talk(&mut core);
    assert!(has_abort(&effects));
    let effects = core
        .transcribed(Ok("and why are you calling".into()))
        .unwrap();
    let request = turn_request(&effects).expect("single merged answer");
    assert_eq!(
        request.messages.last().unwrap()["content"],
        "hi, who is this and why are you calling"
    );
    assert_eq!(request.messages.len(), 4, "no consecutive user messages");
}

#[test]
fn a_caller_who_speaks_first_follows_the_unheard_opener() {
    let o = options();
    let (mut core, _) = started(&o);
    let effects = say(&mut core, "hello?");
    let request = turn_request(&effects).unwrap();
    assert_eq!(request.messages.len(), 4);
    assert_eq!(request.messages[1]["content"], CONNECT_NUDGE);
    assert_eq!(request.messages[2]["content"], UNHEARD_MARKER);
    assert_eq!(request.messages[3]["content"], "hello?");
}

#[test]
fn empty_recognition_does_not_start_a_reply() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(done("Hello.", None)).unwrap();
    talk(&mut core);
    assert!(core.transcribed(Ok("   ".into())).unwrap().is_empty());
}

#[test]
fn a_click_that_interrupts_a_reply_does_not_strand_the_caller() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(done("Hello.", None)).unwrap();
    say(&mut core, "are you there");
    // A short click interrupts generation, then stops without being speech.
    let mut effects = media(&mut core, &test_tone(4, 6_000.0)).unwrap();
    assert!(has_abort(&effects));
    effects = media(&mut core, &test_silence(40)).unwrap();
    let request = turn_request(&effects).expect("the unanswered question is answered again");
    assert_eq!(request.messages.last().unwrap()["content"], "are you there");
}

#[test]
fn recognition_is_serialized_and_kept_in_order() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(done("Hello.", None)).unwrap();
    let first = talk(&mut core);
    let audio_len = |effects: &[Effect]| {
        effects.iter().find_map(|e| match e {
            Effect::Transcribe(audio) => Some(audio.len()),
            _ => None,
        })
    };
    let first_len = audio_len(&first).expect("first goes out immediately");
    let mut second = media(&mut core, &test_tone(20, 6_000.0)).unwrap();
    second.extend(media(&mut core, &test_silence(26)).unwrap());
    assert!(audio_len(&second).is_none(), "recognizer is busy: queued");
    let next = core.transcribed(Ok("one".into())).unwrap();
    assert!(audio_len(&next).is_some_and(|len| len != first_len));
    assert!(
        turn_request(&next).is_none(),
        "wait for the queued utterance"
    );
    let last = core.transcribed(Ok("two".into())).unwrap();
    assert_eq!(
        turn_request(&last).unwrap().messages.last().unwrap()["content"],
        "one two"
    );
}

#[test]
fn recognition_backlog_and_repeated_failures_fail_closed() {
    let o = options();
    let (mut core, _) = started(&o);
    let mut error = None;
    for _ in 0..8 {
        let mut result = media(&mut core, &test_tone(12, 6_000.0));
        if result.is_ok() {
            result = media(&mut core, &test_silence(26));
        }
        if let Err(e) = result {
            error = Some(e);
            break;
        }
    }
    assert_eq!(error, Some(EndReason::ResourceLimit));

    let (mut core, _) = started(&o);
    assert!(core.transcribed(Err(StageError)).is_ok());
    assert!(core.transcribed(Err(StageError)).is_ok());
    assert_eq!(
        core.transcribed(Err(StageError)).err(),
        Some(EndReason::UpstreamError)
    );
    let outcome = core.outcome(EndReason::UpstreamError, 1);
    assert_eq!(
        outcome
            .transcript
            .iter()
            .filter(|e| e.text == "[Caller speech transcription unavailable]")
            .count(),
        2
    );
}

#[test]
fn one_step_end_call_waits_for_every_playback_mark() {
    let o = options();
    let (mut core, _) = started(&o);
    let audio = core
        .turn_event(speech("Thank you, goodbye.", 1_000))
        .unwrap();
    core.turn_event(done("Thank you, goodbye.", Some("end_call")))
        .unwrap();
    mark(&mut core, &mark_name(&audio, 0)).unwrap();
    assert_eq!(
        mark(&mut core, &mark_name(&audio, 1)).err(),
        Some(EndReason::AssistantEnded)
    );

    let (mut core, _) = started(&o);
    assert_eq!(
        core.turn_event(done("", Some("end_call"))).err(),
        Some(EndReason::AssistantEnded),
        "nothing is playing"
    );
}

#[test]
fn interactive_end_call_runs_a_tool_free_confirmation_before_closing() {
    let mut o = options();
    o.confirm_end_call = true;
    let (mut core, _) = started(&o);
    let effects = core.turn_event(done("All set.", Some("end_call"))).unwrap();
    let request = turn_request(&effects).expect("confirmation turn");
    assert!(request.tools.as_array().unwrap().is_empty());
    let system = request.messages[0]["content"].as_str().unwrap();
    assert!(system.starts_with(INSTRUCTIONS));
    assert!(system.contains(END_CONFIRM_INSTRUCTIONS));
    assert!(system.contains("never speak a tool name aloud"));
    assert!(core.close_timeout().is_none());

    let audio = core
        .turn_event(speech("Anything else before I go?", 800))
        .unwrap();
    core.turn_event(done("Anything else before I go?", None))
        .unwrap();
    assert!(core.close_timeout().is_none(), "audio is still playing");
    mark(&mut core, &mark_name(&audio, 0)).unwrap();
    let deadline = core
        .close_timeout()
        .expect("silence window starts after playback");
    assert!(deadline <= Instant::now() + END_CONFIRM_SILENCE);

    // Any caller speech is a turn of its own and cancels the silence close.
    media(&mut core, &test_tone(12, 6_000.0)).unwrap();
    assert!(core.close_timeout().is_none());
    media(&mut core, &test_silence(26)).unwrap();
    let effects = core
        .transcribed(Ok("no, that's everything".into()))
        .unwrap();
    let request = turn_request(&effects).unwrap();
    assert_eq!(request.tools.as_array().unwrap().len(), 1, "tools are back");
    assert_eq!(
        core.turn_event(done("", Some("end_call"))).err(),
        Some(EndReason::AssistantEnded),
        "the second request closes"
    );
}

#[test]
fn an_inaudible_confirmation_reopens_the_call_and_the_next_request_asks_again() {
    let mut o = options();
    o.confirm_end_call = true;
    let (mut core, _) = started(&o);
    core.turn_event(done("", Some("end_call"))).unwrap();
    // The prompt was never audible, so it cannot count as the confirmation.
    core.turn_event(done("", None)).unwrap();
    assert!(core.close_timeout().is_none());
    let effects = say(&mut core, "hello?");
    assert!(turn_request(&effects).is_some());
    core.turn_event(done("", Some("end_call"))).unwrap();
    assert!(
        core.turn.as_ref().is_some_and(|turn| turn.confirmation),
        "a new confirmation turn starts instead of closing"
    );
}

#[test]
fn the_confirmation_turn_cannot_close_the_call_itself() {
    let mut o = options();
    o.confirm_end_call = true;
    let (mut core, _) = started(&o);
    core.turn_event(done("", Some("end_call"))).unwrap();
    core.turn_event(speech("Anything else?", 800)).unwrap();
    assert!(
        core.turn_event(done("Anything else?", Some("end_call")))
            .is_ok()
    );
    assert!(core.close_timeout().is_none(), "still waiting for playback");
}

#[test]
fn caller_speech_over_a_goodbye_keeps_the_call_open() {
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(speech("Goodbye.", 800)).unwrap();
    core.turn_event(done("Goodbye.", Some("end_call"))).unwrap();
    let effects = media(&mut core, &test_tone(12, 6_000.0)).unwrap();
    assert_eq!(twilio_events(&effects, "clear").len(), 1);
    assert!(mark(&mut core, "zc-play-1").is_ok(), "no longer closing");
    media(&mut core, &test_silence(26)).unwrap();
    let effects = core.transcribed(Ok("wait, one more thing".into())).unwrap();
    assert!(turn_request(&effects).is_some());
}

#[test]
fn decline_recording_ends_at_once_and_discards_the_transcript() {
    let mut o = options();
    o.stop_on_recording_decline = true;
    let (mut core, _) = started(&o);
    core.turn_event(speech("Recording is on.", 800)).unwrap();
    assert_eq!(
        core.turn_event(done("", Some("decline_recording"))).err(),
        Some(EndReason::RecordingDeclined),
        "no playback or close check is awaited"
    );
    assert!(
        core.outcome(EndReason::RecordingDeclined, 1)
            .transcript
            .is_empty()
    );

    let (mut core, _) = started(&options());
    assert_eq!(
        core.turn_event(done("", Some("decline_recording"))).err(),
        Some(EndReason::ProtocolError),
        "the tool is only allowed when the ingress enabled it"
    );
}

#[test]
fn recording_objections_are_detected_deterministically_only_when_enabled() {
    let mut o = options();
    o.stop_on_recording_decline = true;
    let (mut core, _) = started(&o);
    talk(&mut core);
    assert_eq!(
        core.transcribed(Ok("I do not consent to being recorded".into()))
            .err(),
        Some(EndReason::RecordingDeclined)
    );
    let (mut core, _) = started(&options());
    talk(&mut core);
    assert!(
        core.transcribed(Ok("I do not consent to being recorded".into()))
            .is_ok()
    );
}

#[test]
fn unexpected_tools_and_arguments_fail_closed() {
    let o = options();
    let (mut core, _) = started(&o);
    assert_eq!(
        core.turn_event(done("", Some("shell"))).err(),
        Some(EndReason::ProtocolError)
    );
    let (mut core, _) = started(&o);
    let bad_arguments = TurnEvent::Done {
        text: String::new(),
        tool: Some(ToolCall {
            name: "end_call".into(),
            arguments: "{\"force\":true}".into(),
        }),
    };
    assert_eq!(
        core.turn_event(bad_arguments).err(),
        Some(EndReason::ProtocolError)
    );
    let mut off = options();
    off.allow_end_call = false;
    let (mut core, _) = started(&off);
    assert_eq!(
        core.turn_event(done("", Some("end_call"))).err(),
        Some(EndReason::ProtocolError)
    );
}

#[test]
fn ceilings_and_stage_failures_fail_closed() {
    let o = options();
    let (mut core, _) = started(&o);
    assert_eq!(
        core.turn_event(speech("Too much.", MAX_QUEUED_OUTPUT + 1))
            .err(),
        Some(EndReason::ResourceLimit)
    );
    let (mut core, _) = started(&o);
    assert_eq!(
        core.turn_event(TurnEvent::Failed).err(),
        Some(EndReason::UpstreamError)
    );
    let (mut core, _) = started(&o);
    core.turn_event(done("Hi.", None)).unwrap();
    assert_eq!(
        core.turn_event(done("", None)).err(),
        Some(EndReason::ProtocolError),
        "no turn is active any more"
    );
    let (mut core, _) = started(&o);
    let frame = vec![0xffu8; 8 * 1024];
    let mut error = None;
    for _ in 0..200 {
        if let Err(e) = media(&mut core, &frame) {
            error = Some(e);
            break;
        }
    }
    assert_eq!(error, Some(EndReason::ResourceLimit));
    let (mut core, _) = started(&o);
    assert_eq!(
        media(&mut core, &vec![0xffu8; 8 * 1024 + 1]).err(),
        Some(EndReason::ResourceLimit)
    );
}

#[test]
fn flushed_speech_at_hangup_is_submitted_for_recognition() {
    let o = options();
    let (mut core, _) = started(&o);
    media(&mut core, &test_tone(12, 6_000.0)).unwrap();
    let effects = core.flush_speech().unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Transcribe(_)]));
    core.draining = true;
    assert!(
        core.transcribed(Ok("please call me back".into()))
            .unwrap()
            .is_empty()
    );
    let outcome = core.outcome(EndReason::PeerClosed, 1);
    assert!(
        outcome
            .transcript
            .iter()
            .any(|e| e.speaker == "caller" && e.text == "please call me back")
    );
}

#[test]
fn sentences_split_at_boundaries_and_short_fragments_join_the_next() {
    assert_eq!(
        split_sentences(
            "Hello, this is an AI assistant calling for Alex. Is now a good time to talk?"
        ),
        [
            "Hello, this is an AI assistant calling for Alex.",
            "Is now a good time to talk?"
        ]
    );
    assert_eq!(
        split_sentences("Dr. Smith will call back. Thanks!"),
        ["Dr. Smith will call back. Thanks!"]
    );
    assert_eq!(split_sentences("Okay."), ["Okay."]);
    assert_eq!(split_sentences("  "), Vec::<String>::new());
    let long = "word ".repeat(200);
    let chunks = split_sentences(&long);
    assert!(chunks.len() > 1);
    assert!(chunks.iter().all(|c| c.len() <= MAX_SENTENCE_CHARS + 5));
    assert_eq!(
        clean_speech("**Sure** - `ok` #1\n\n fine"),
        "Sure - ok 1 fine"
    );
}

struct FakeStt;
impl SpeechToText for FakeStt {
    fn transcribe(&self, _: Vec<u8>) -> BoxFuture<'_, Result<String, StageError>> {
        Box::pin(async { Ok("unused".into()) })
    }
}

struct FakeChat(
    Mutex<Option<Result<ChatReply, StageError>>>,
    Mutex<Vec<Value>>,
);
impl ChatModel for FakeChat {
    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatReply, StageError>> {
        self.1.lock().unwrap().extend(request.messages);
        let reply = self.0.lock().unwrap().take().unwrap();
        Box::pin(async move { reply })
    }
}

struct FakeTts {
    fail_on: Option<&'static str>,
}
impl Synthesizer for FakeTts {
    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<Vec<u8>, StageError>> {
        let fail = self.fail_on.is_some_and(|needle| text.contains(needle));
        Box::pin(async move {
            if fail {
                return Err(StageError);
            }
            // 100 ms of 24 kHz audio.
            Ok((0..2_400i16).flat_map(|n| (n * 5).to_le_bytes()).collect())
        })
    }
}

fn pipeline(reply: Result<ChatReply, StageError>, fail_on: Option<&'static str>) -> Arc<Pipeline> {
    Arc::new(Pipeline {
        stt: Arc::new(FakeStt),
        chat: Arc::new(FakeChat(Mutex::new(Some(reply)), Mutex::new(Vec::new()))),
        tts: Arc::new(FakeTts { fail_on }),
    })
}

async fn drive(pipeline: Arc<Pipeline>) -> Vec<TurnEvent> {
    let (sender, mut receiver) = mpsc::channel(8);
    let request = TurnRequest {
        messages: vec![json!({"role":"system","content":"x"})],
        tools: json!([]),
    };
    tokio::spawn(run_turn(pipeline, request, sender));
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await {
        events.push(event);
    }
    events
}

#[tokio::test]
async fn a_turn_streams_sentence_audio_then_reports_the_tool_call() {
    let reply = ChatReply {
        text: "Hello, this is an AI assistant calling for Alex. Is **now** a good time to talk?"
            .into(),
        tool: Some(ToolCall {
            name: "end_call".into(),
            arguments: "{}".into(),
        }),
    };
    let events = drive(pipeline(Ok(reply), None)).await;
    assert_eq!(events.len(), 3);
    let TurnEvent::Speech { text, mulaw } = &events[0] else {
        panic!("first event is speech");
    };
    assert_eq!(text, "Hello, this is an AI assistant calling for Alex.");
    assert!(
        (mulaw.len() as i32 - 800).abs() <= 11,
        "100 ms of 8 kHz mu-law"
    );
    let TurnEvent::Done { text, tool } = &events[2] else {
        panic!("last event is done");
    };
    assert_eq!(
        text,
        "Hello, this is an AI assistant calling for Alex. Is now a good time to talk?"
    );
    assert_eq!(tool.as_ref().unwrap().name, "end_call");
}

#[tokio::test]
async fn model_or_synthesis_failures_end_the_turn_with_a_failure_event() {
    let events = drive(pipeline(Err(StageError), None)).await;
    assert!(matches!(events.as_slice(), [TurnEvent::Failed]));

    let reply = ChatReply {
        text: "This part works perfectly fine. This sentence breaks synthesis.".into(),
        tool: None,
    };
    let events = drive(pipeline(Ok(reply), Some("breaks"))).await;
    assert!(matches!(
        events.as_slice(),
        [TurnEvent::Speech { .. }, TurnEvent::Failed]
    ));
}

#[tokio::test]
async fn a_tool_only_reply_produces_no_audio() {
    let reply = ChatReply {
        text: String::new(),
        tool: Some(ToolCall {
            name: "decline_recording".into(),
            arguments: "{}".into(),
        }),
    };
    let events = drive(pipeline(Ok(reply), None)).await;
    assert!(matches!(events.as_slice(), [TurnEvent::Done { .. }]));
}

// --- Driver tests: a real WebSocket plays Twilio against `bridge` -------------

use futures_util::StreamExt;
use std::collections::VecDeque;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message as ClientMessage;

struct ScriptedStt(&'static str);
impl SpeechToText for ScriptedStt {
    fn transcribe(&self, _: Vec<u8>) -> BoxFuture<'_, Result<String, StageError>> {
        let text = self.0.to_owned();
        Box::pin(async move { Ok(text) })
    }
}

struct ScriptedChat(Mutex<VecDeque<ChatReply>>);
impl ChatModel for ScriptedChat {
    fn complete(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatReply, StageError>> {
        let reply = self.0.lock().unwrap().pop_front().ok_or(StageError);
        Box::pin(async move { reply })
    }
}

fn reply(text: &str, tool: Option<&str>) -> ChatReply {
    ChatReply {
        text: text.into(),
        tool: tool.map(|name| ToolCall {
            name: name.into(),
            arguments: "{}".into(),
        }),
    }
}

struct Harness {
    options: Mutex<Option<CascadeOptions>>,
    pipeline: Arc<Pipeline>,
    outcome: Mutex<Option<oneshot::Sender<BridgeOutcome>>>,
}

async fn upgrade(
    axum::extract::State(harness): axum::extract::State<Arc<Harness>>,
    ws: axum::extract::WebSocketUpgrade,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| async move {
        let options = harness.options.lock().unwrap().take().unwrap();
        let outcome = bridge(socket, options, harness.pipeline.clone()).await;
        if let Some(sender) = harness.outcome.lock().unwrap().take() {
            let _ = sender.send(outcome);
        }
    })
}

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect as Twilio would: `connected`, then the verified `start`.
async fn connect(
    options: CascadeOptions,
    stt: &'static str,
    replies: Vec<ChatReply>,
) -> (Client, oneshot::Receiver<BridgeOutcome>) {
    let (sender, receiver) = oneshot::channel();
    let start = start_event(&options);
    let harness = Arc::new(Harness {
        options: Mutex::new(Some(options)),
        pipeline: Arc::new(Pipeline {
            stt: Arc::new(ScriptedStt(stt)),
            chat: Arc::new(ScriptedChat(Mutex::new(replies.into()))),
            tts: Arc::new(FakeTts { fail_on: None }),
        }),
        outcome: Mutex::new(Some(sender)),
    });
    let router = axum::Router::new()
        .route("/ws", axum::routing::get(upgrade))
        .with_state(harness);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    // A loopback test server has no TLS; the URI is built from parts.
    let uri = tokio_tungstenite::tungstenite::http::Uri::builder()
        .scheme("ws")
        .authority(address.to_string())
        .path_and_query("/ws")
        .build()
        .unwrap();
    let (mut client, _) = tokio_tungstenite::connect_async(uri).await.unwrap();
    for frame in [
        json!({"event":"connected","protocol":"Call","version":"1.0.0"}),
        start,
    ] {
        // A bridge that rejects its options may already have closed the stream.
        let _ = client
            .send(ClientMessage::Text(frame.to_string().into()))
            .await;
    }
    (client, receiver)
}

async fn send(client: &mut Client, frame: Value) {
    client
        .send(ClientMessage::Text(frame.to_string().into()))
        .await
        .unwrap();
}

async fn send_audio(client: &mut Client, mulaw: &[u8]) {
    send(
        client,
        json!({"event":"media","streamSid":stream_sid(),
            "media":{"track":"inbound","payload":STANDARD.encode(mulaw)}}),
    )
    .await;
}

/// Read what the bridge sends until it goes quiet, acknowledging playback marks
/// the way Twilio does. Returns the number of audio frames heard, or `None` if
/// the bridge closed the stream.
async fn listen(client: &mut Client, quiet: Duration) -> Option<usize> {
    let mut audio = 0;
    loop {
        match tokio::time::timeout(quiet, client.next()).await {
            Err(_) => return Some(audio),
            Ok(None) | Ok(Some(Ok(ClientMessage::Close(_)))) | Ok(Some(Err(_))) => return None,
            Ok(Some(Ok(ClientMessage::Text(text)))) => {
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                match frame["event"].as_str() {
                    Some("media") => audio += 1,
                    Some("mark") => {
                        send(
                            client,
                            json!({"event":"mark","streamSid":stream_sid(),
                                "mark":{"name":frame["mark"]["name"]}}),
                        )
                        .await
                    }
                    _ => {}
                }
            }
            Ok(Some(Ok(_))) => {}
        }
    }
}

async fn finished(receiver: oneshot::Receiver<BridgeOutcome>) -> BridgeOutcome {
    tokio::time::timeout(Duration::from_secs(10), receiver)
        .await
        .expect("bridge finished")
        .expect("outcome sent")
}

#[tokio::test]
async fn a_full_call_greets_listens_answers_and_hangs_up_after_the_goodbye_plays() {
    let (mut client, outcome) = connect(
        options(),
        "yes, this is Alex",
        vec![
            reply(
                "Hello, this is an AI assistant calling for the owner.",
                None,
            ),
            reply("Thank you. Goodbye and take care.", Some("end_call")),
        ],
    )
    .await;
    assert!(
        listen(&mut client, Duration::from_millis(400))
            .await
            .unwrap()
            > 0
    );

    send_audio(&mut client, &test_tone(12, 6_000.0)).await;
    send_audio(&mut client, &test_silence(26)).await;
    assert_eq!(
        listen(&mut client, Duration::from_secs(5)).await,
        None,
        "the bridge closes once the goodbye has been acknowledged as played"
    );

    let outcome = finished(outcome).await;
    assert_eq!(outcome.reason, EndReason::AssistantEnded);
    assert!(outcome.model_session_ready);
    let spoken: Vec<_> = outcome
        .transcript
        .iter()
        .map(|entry| (entry.speaker.as_str(), entry.text.as_str()))
        .collect();
    assert_eq!(
        spoken,
        [
            (
                "assistant",
                "Hello, this is an AI assistant calling for the owner."
            ),
            ("caller", "yes, this is Alex"),
            ("assistant", "Thank you. Goodbye and take care."),
        ]
    );
    for entry in outcome
        .transcript
        .iter()
        .filter(|e| e.speaker == "assistant")
    {
        assert!(!entry.interrupted);
        assert!(entry.heard_audio_ms.unwrap() > 0);
    }
}

#[tokio::test]
async fn a_recording_objection_stops_the_call_and_discards_everything() {
    let mut o = options();
    o.stop_on_recording_decline = true;
    let (mut client, outcome) = connect(
        o,
        "I do not consent to this recording",
        vec![reply("Recording has started.", None)],
    )
    .await;
    listen(&mut client, Duration::from_millis(400)).await;
    send_audio(&mut client, &test_tone(12, 6_000.0)).await;
    send_audio(&mut client, &test_silence(26)).await;
    assert_eq!(listen(&mut client, Duration::from_secs(5)).await, None);
    let outcome = finished(outcome).await;
    assert_eq!(outcome.reason, EndReason::RecordingDeclined);
    assert!(outcome.transcript.is_empty());
}

#[tokio::test]
async fn words_spoken_just_before_hangup_are_still_transcribed() {
    let (mut client, outcome) = connect(
        options(),
        "please call me back tomorrow",
        vec![reply(
            "Hello, this is an AI assistant taking a message.",
            None,
        )],
    )
    .await;
    listen(&mut client, Duration::from_millis(400)).await;
    send_audio(&mut client, &test_tone(14, 6_000.0)).await;
    send(
        &mut client,
        json!({"event":"stop","streamSid":stream_sid()}),
    )
    .await;
    let outcome = finished(outcome).await;
    assert_eq!(outcome.reason, EndReason::CallEnded);
    assert!(
        outcome
            .transcript
            .iter()
            .any(|e| e.speaker == "caller" && e.text == "please call me back tomorrow")
    );
}

#[tokio::test]
async fn invalid_options_never_touch_the_stages() {
    let mut o = options();
    o.confirm_end_call = true;
    o.allow_end_call = false;
    let (mut client, outcome) = connect(o, "unused", Vec::new()).await;
    let _ = listen(&mut client, Duration::from_millis(400)).await;
    let outcome = finished(outcome).await;
    assert_eq!(outcome.reason, EndReason::InvalidOptions);
    assert!(outcome.transcript.is_empty());
    assert!(!outcome.model_session_ready);
}

fn owner_frames(effects: &[Effect]) -> usize {
    effects
        .iter()
        .filter(|e| matches!(e, Effect::Owner(_)))
        .count()
}

#[test]
fn coach_leg_hears_both_sides_only_after_joining() {
    use crate::coach::FromOwner;
    let o = options();
    let (mut core, _) = started(&o);
    let before = core.turn_event(speech("Hello.", 800)).unwrap();
    assert_eq!(owner_frames(&before), 0);
    core.owner(FromOwner::Joined).unwrap();
    let assistant = core.turn_event(speech("Still there?", 800)).unwrap();
    assert_eq!(owner_frames(&assistant), 1);
    let callee = media(&mut core, &test_silence(1)).unwrap();
    assert_eq!(owner_frames(&callee), 1);
    let barge = talk(&mut core);
    assert!(
        barge
            .iter()
            .any(|e| matches!(e, Effect::Owner(crate::coach::ToOwner::ClearAssistant)))
    );
}

#[test]
fn owner_coaching_reaches_the_system_prompt_and_prompts_a_reply() {
    use crate::coach::FromOwner;
    let o = options();
    let (mut core, _) = started(&o);
    core.owner(FromOwner::Joined).unwrap();
    // A reply is in flight: coaching waits for it instead of interrupting.
    let waiting = core
        .owner(FromOwner::Speech("Ask about Saturday.".into()))
        .unwrap();
    assert!(turn_request(&waiting).is_none());
    let effects = core.turn_event(done("Hello.", None)).unwrap();
    let request = turn_request(&effects).expect("coaching is acted on");
    let system = request.messages[0]["content"].as_str().unwrap();
    assert!(system.starts_with(INSTRUCTIONS));
    assert!(system.contains("Coaching: Ask about Saturday."));
    assert_eq!(request.messages.last().unwrap()["content"], COACH_NUDGE);
    // The callee speaking first replaces the nudge instead of merging with it.
    core.turn_event(done("Is Saturday possible?", None))
        .unwrap();
    say(&mut core, "Saturday works.");
    let outcome = core.outcome(EndReason::CallEnded, 1);
    assert!(
        outcome
            .transcript
            .iter()
            .any(|e| e.speaker == "owner" && e.text == "Ask about Saturday.")
    );
}

#[test]
fn takeover_silences_the_assistant_and_passes_owner_audio_through() {
    use crate::coach::FromOwner;
    let o = options();
    let (mut core, _) = started(&o);
    core.owner(FromOwner::Joined).unwrap();
    let playing = core.turn_event(speech("Hello.", 800)).unwrap();
    assert!(!twilio_events(&playing, "mark").is_empty());
    let taken = core.owner(FromOwner::TakeOver).unwrap();
    assert!(has_abort(&taken));
    assert_eq!(twilio_events(&taken, "clear").len(), 1);
    let audio = core.owner(FromOwner::Audio(vec![0x7f; 160])).unwrap();
    assert_eq!(twilio_events(&audio, "media").len(), 1);
    // The callee is still transcribed, but the assistant does not answer.
    let reply = say(&mut core, "Who is this?");
    assert!(turn_request(&reply).is_none());
    let direct = core
        .owner(FromOwner::Speech("Hi, it's me.".into()))
        .unwrap();
    assert!(turn_request(&direct).is_none());
    assert!(
        core.history[0]["content"]
            .as_str()
            .unwrap()
            .contains("said directly to the other party: Hi, it's me.")
    );
    let back = core.owner(FromOwner::HandBack).unwrap();
    assert!(
        turn_request(&back).is_some(),
        "the unanswered callee gets a reply"
    );
    assert!(
        core.owner(FromOwner::Audio(vec![0x7f; 160]))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn owner_hanging_up_during_takeover_returns_the_call_to_the_assistant() {
    use crate::coach::FromOwner;
    let o = options();
    let (mut core, _) = started(&o);
    core.turn_event(done("Hello.", None)).unwrap();
    core.owner(FromOwner::Joined).unwrap();
    core.owner(FromOwner::TakeOver).unwrap();
    let left = core.owner_left().unwrap();
    let request = turn_request(&left).expect("assistant resumes");
    assert_eq!(request.messages.last().unwrap()["content"], HANDBACK_NUDGE);
    assert_eq!(
        owner_frames(&media(&mut core, &test_silence(1)).unwrap()),
        0
    );
}

#[test]
fn opening_request_matches_the_first_live_turn() {
    for (end, decline) in [(true, false), (true, true), (false, false)] {
        let o = CascadeOptions {
            allow_end_call: end,
            stop_on_recording_decline: decline,
            ..options()
        };
        let (_, effects) = started(&o);
        let live = turn_request(&effects).unwrap();
        let expected = opening_request(&o.instructions, end, decline);
        assert_eq!(live.messages, expected.messages);
        assert_eq!(live.tools, expected.tools);
    }
}

/// Sends its reply in fragments and finishes only when released.
struct GatedChat {
    fragments: Vec<&'static str>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}
impl ChatModel for GatedChat {
    fn complete(&self, _: ChatRequest) -> BoxFuture<'_, Result<ChatReply, StageError>> {
        Box::pin(async { Err(StageError) })
    }
    fn complete_streaming(
        &self,
        _: ChatRequest,
        text: mpsc::UnboundedSender<String>,
    ) -> BoxFuture<'_, Result<ChatReply, StageError>> {
        let release = self.release.lock().unwrap().take().unwrap();
        Box::pin(async move {
            for fragment in &self.fragments {
                text.send((*fragment).to_owned()).unwrap();
            }
            release.await.unwrap();
            Ok(ChatReply {
                text: self.fragments.concat(),
                tool: None,
            })
        })
    }
}

#[tokio::test]
async fn a_finished_sentence_is_spoken_before_the_reply_completes() {
    let (release, gate) = tokio::sync::oneshot::channel();
    let pipeline = Arc::new(Pipeline {
        stt: Arc::new(FakeStt),
        chat: Arc::new(GatedChat {
            fragments: vec![
                "Hi Ari, this is your",
                " AI assistant. Is now",
                " a good time?",
            ],
            release: Mutex::new(Some(gate)),
        }),
        tts: Arc::new(FakeTts { fail_on: None }),
    });
    let (sender, mut receiver) = mpsc::channel(8);
    let request = TurnRequest {
        messages: vec![json!({"role":"system","content":"x"})],
        tools: json!([]),
    };
    tokio::spawn(run_turn(pipeline, request, sender));
    let Some(TurnEvent::Speech { text, .. }) = receiver.recv().await else {
        panic!("first sentence is spoken while the model is still generating");
    };
    assert_eq!(text, "Hi Ari, this is your AI assistant.");
    release.send(()).unwrap();
    let Some(TurnEvent::Speech { text, .. }) = receiver.recv().await else {
        panic!("the rest is spoken once the reply completes");
    };
    assert_eq!(text, "Is now a good time?");
    let Some(TurnEvent::Done { text, tool }) = receiver.recv().await else {
        panic!("turn completes");
    };
    assert_eq!(
        text,
        "Hi Ari, this is your AI assistant. Is now a good time?"
    );
    assert!(tool.is_none());
}

#[test]
fn an_opener_interrupted_before_any_audio_keeps_the_prompt_prefix() {
    let o = options();
    let (mut core, effects) = started(&o);
    let opening = turn_request(&effects).unwrap().messages.clone();
    let effects = say(&mut core, "Hello?");
    let next = turn_request(&effects).expect("the caller's words start a turn");
    assert_eq!(next.messages[..opening.len()], opening[..]);
    assert_eq!(next.messages[opening.len()]["role"], "assistant");
    assert_eq!(next.messages[opening.len()]["content"], UNHEARD_MARKER);
    assert_eq!(next.messages[opening.len() + 1]["content"], "Hello?");
}
