use super::*;
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode as HttpStatus},
    response::IntoResponse,
    routing::post,
};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Seen {
    requests: Mutex<Vec<(HeaderMap, Vec<u8>)>>,
    hits: AtomicUsize,
}

type Shared = Arc<Seen>;

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{address}")
}

async fn record(State(seen): State<Shared>, headers: HeaderMap, body: Bytes) {
    seen.hits.fetch_add(1, Ordering::SeqCst);
    seen.requests.lock().unwrap().push((headers, body.to_vec()));
}

fn config(base: &str) -> CascadeConfig {
    CascadeConfig {
        stt_url: format!("{base}/v1/audio/transcriptions"),
        stt_model: "whisper-test".into(),
        stt_language: Some("en".into()),
        llm_url: format!("{base}/v1/chat/completions"),
        llm_model: "chat-test".into(),
        llm_extra: Default::default(),
        tts: crate::common::TtsEngine::Http,
        tts_url: Some(format!("{base}/v1/audio/speech")),
        tts_model: "kokoro".into(),
        tts_voice: "af_heart".into(),
        tts_rate: None,
    }
}

#[test]
fn the_openai_credential_is_attached_to_openai_only() {
    let key = "sk-fixture";
    let openai = Url::parse("https://api.openai.com/v1/chat/completions").unwrap();
    let header = authorization(&openai, key).expect("sent to OpenAI");
    assert!(header.is_sensitive());
    for other in [
        "https://openrouter.ai/api/v1/chat/completions",
        "https://api.openai.com.evil.example/v1/chat/completions",
        "http://api.openai.com/v1/chat/completions",
        "http://127.0.0.1:8880/v1/audio/speech",
        "http://localhost:8000/v1/audio/transcriptions",
    ] {
        assert!(
            authorization(&Url::parse(other).unwrap(), key).is_none(),
            "{other}"
        );
    }
    assert!(authorization(&openai, "bad\nkey").is_none());
    assert!(authorization(&openai, "").is_none());
}

#[test]
fn endpoints_must_be_https_or_local() {
    for bad in [
        "http://example.com/v1/audio/speech",
        "ftp://127.0.0.1/x",
        "https://user:pass@example.com/x",
        "https://example.com/x?key=1",
        "https://example.com/x#frag",
        "not a url",
    ] {
        assert!(validate_endpoint(bad).is_err(), "{bad}");
    }
    for good in [
        "https://api.openai.com/v1/chat/completions",
        "http://127.0.0.1:8880/v1/audio/speech",
        "http://localhost:8000/v1/audio/transcriptions",
        "http://[::1]:8000/x",
    ] {
        assert!(validate_endpoint(good).is_ok(), "{good}");
    }
    let mut unsafe_config = config("http://127.0.0.1:1");
    unsafe_config.tts_url = Some("http://tts.example.com/v1/audio/speech".into());
    assert!(build_pipeline(&unsafe_config, "key").is_err());
}

#[test]
fn chat_bodies_match_the_target_server_dialect() {
    let request = ChatRequest {
        messages: vec![json!({"role":"user","content":"hi"})],
        tools: crate::cascade::chat_tools(true, false),
    };
    let local = chat_body("local-model", false, &Default::default(), &request);
    assert_eq!(local["max_tokens"], MAX_REPLY_TOKENS);
    assert!(local.get("max_completion_tokens").is_none());
    assert!(local.get("parallel_tool_calls").is_none());
    assert_eq!(local["tool_choice"], "auto");
    assert_eq!(local["stream"], false);

    let openai = chat_body("gpt", true, &Default::default(), &request);
    assert_eq!(openai["max_completion_tokens"], MAX_REPLY_TOKENS);
    assert_eq!(openai["parallel_tool_calls"], false);
    assert!(openai.get("max_tokens").is_none());

    let tool_free = chat_body(
        "m",
        true,
        &Default::default(),
        &ChatRequest {
            messages: Vec::new(),
            tools: json!([]),
        },
    );
    assert!(tool_free.get("tools").is_none());
    assert!(tool_free.get("tool_choice").is_none());
}

#[test]
fn chat_replies_are_parsed_strictly() {
    let text = parse_chat_reply(
        br#"{"choices":[{"message":{"role":"assistant","content":"Hello there."}}]}"#,
    )
    .unwrap();
    assert_eq!(text.text, "Hello there.");
    assert!(text.tool.is_none());

    let tool = parse_chat_reply(
        br#"{"choices":[{"message":{"content":null,"tool_calls":[
            {"id":"c1","type":"function","function":{"name":"end_call","arguments":"{}"}}]}}]}"#,
    )
    .unwrap();
    assert_eq!(tool.tool.unwrap().name, "end_call");

    let spoken_and_tool = parse_chat_reply(
        br#"{"choices":[{"message":{"content":"Goodbye.","tool_calls":[
            {"id":"c1","type":"function","function":{"name":"end_call","arguments":""}}]}}]}"#,
    )
    .unwrap();
    assert_eq!(spoken_and_tool.text, "Goodbye.");

    for bad in [
        &br#"{"choices":[]}"#[..],
        br#"{"choices":[{"message":{"content":"  "}}]}"#,
        br#"{"choices":[{"message":{"content":"x","tool_calls":[
            {"type":"function","function":{"name":"end_call","arguments":"{}"}},
            {"type":"function","function":{"name":"end_call","arguments":"{}"}}]}}]}"#,
        br#"{"choices":[{"message":{"content":"x","tool_calls":[{"type":"other"}]}}]}"#,
        b"not json",
    ] {
        assert!(parse_chat_reply(bad).is_err());
    }
    assert_eq!(parse_transcript(br#"{"text":"  hi  "}"#).unwrap(), "hi");
    assert!(parse_transcript(br#"{"nope":1}"#).is_err());
}

#[tokio::test]
async fn tts_requests_raw_pcm_and_sends_no_credential_to_a_local_server() {
    let seen = Shared::default();
    let router = Router::new()
        .route(
            "/v1/audio/speech",
            post(
                |State(seen): State<Shared>, headers: HeaderMap, body: Bytes| async move {
                    record(State(seen), headers, body).await;
                    vec![0u8; 4_800]
                },
            ),
        )
        .with_state(seen.clone());
    let base = serve(router).await;
    let pipeline = build_pipeline(&config(&base), "sk-fixture-secret").unwrap();
    let pcm = pipeline
        .tts
        .synthesize("Hello there.".into())
        .await
        .unwrap();
    assert_eq!(pcm.len(), 4_800);
    let requests = seen.requests.lock().unwrap();
    let (headers, body) = &requests[0];
    assert!(headers.get("authorization").is_none());
    let body: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(body["input"], "Hello there.");
    assert_eq!(body["voice"], "af_heart");
    assert_eq!(body["model"], "kokoro");
    assert_eq!(body["response_format"], "pcm");
}

#[tokio::test]
async fn stt_uploads_a_wav_with_the_model_and_language() {
    let seen = Shared::default();
    let router = Router::new()
        .route(
            "/v1/audio/transcriptions",
            post(
                |State(seen): State<Shared>, headers: HeaderMap, body: Bytes| async move {
                    record(State(seen), headers, body).await;
                    axum::Json(json!({"text": " Hi, can you hear me? "}))
                },
            ),
        )
        .with_state(seen.clone());
    let base = serve(router).await;
    let pipeline = build_pipeline(&config(&base), "sk-fixture-secret").unwrap();
    let text = pipeline.stt.transcribe(vec![0xff; 3_200]).await.unwrap();
    assert_eq!(text, "Hi, can you hear me?");
    let requests = seen.requests.lock().unwrap();
    let (headers, body) = &requests[0];
    assert!(
        headers["content-type"]
            .to_str()
            .unwrap()
            .starts_with("multipart/form-data")
    );
    let haystack = String::from_utf8_lossy(body);
    for expected in [
        "audio.wav",
        "RIFF",
        "whisper-test",
        "name=\"language\"",
        "\r\nen\r\n",
    ] {
        assert!(haystack.contains(expected), "missing {expected}");
    }
}

#[tokio::test]
async fn chat_sends_tools_and_returns_the_single_tool_call() {
    let seen = Shared::default();
    let router = Router::new()
        .route(
            "/v1/chat/completions",
            post(|State(seen): State<Shared>, headers: HeaderMap, body: Bytes| async move {
                record(State(seen), headers, body).await;
                axum::Json(json!({"choices":[{"message":{"content":"Goodbye.","tool_calls":[
                    {"id":"c1","type":"function","function":{"name":"end_call","arguments":"{}"}}]}}]}))
            }),
        )
        .with_state(seen.clone());
    let base = serve(router).await;
    let pipeline = build_pipeline(&config(&base), "sk-fixture-secret").unwrap();
    let reply = pipeline
        .chat
        .complete(ChatRequest {
            messages: vec![
                json!({"role":"system","content":"policy"}),
                json!({"role":"user","content":"bye"}),
            ],
            tools: crate::cascade::chat_tools(true, true),
        })
        .await
        .unwrap();
    assert_eq!(reply.text, "Goodbye.");
    assert_eq!(reply.tool.unwrap().name, "end_call");
    let requests = seen.requests.lock().unwrap();
    let body: Value = serde_json::from_slice(&requests[0].1).unwrap();
    assert_eq!(body["model"], "chat-test");
    assert_eq!(body["messages"][0]["content"], "policy");
    assert_eq!(body["tools"].as_array().unwrap().len(), 2);
    assert!(requests[0].0.get("authorization").is_none());
}

#[tokio::test]
async fn transient_server_errors_are_retried_once_and_client_errors_are_not() {
    let hits = Arc::new(AtomicUsize::new(0));
    let flaky = hits.clone();
    let always_down = Arc::new(AtomicUsize::new(0));
    let down = always_down.clone();
    let rejected = Arc::new(AtomicUsize::new(0));
    let bad = rejected.clone();
    let router = Router::new()
        .route(
            "/flaky",
            post(move || {
                let hits = flaky.clone();
                async move {
                    if hits.fetch_add(1, Ordering::SeqCst) == 0 {
                        HttpStatus::INTERNAL_SERVER_ERROR.into_response()
                    } else {
                        "ok".into_response()
                    }
                }
            }),
        )
        .route(
            "/down",
            post(move || {
                let hits = down.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    HttpStatus::SERVICE_UNAVAILABLE
                }
            }),
        )
        .route(
            "/bad",
            post(move || {
                let hits = bad.clone();
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    HttpStatus::BAD_REQUEST
                }
            }),
        );
    let base = serve(router).await;
    let endpoint = |path: &str| {
        Endpoint::new(&format!("{base}{path}"), "key", Duration::from_secs(5)).unwrap()
    };
    let flaky = endpoint("/flaky");
    assert!(flaky.send(|| flaky.post()).await.is_ok());
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let down = endpoint("/down");
    assert!(down.send(|| down.post()).await.is_err());
    assert_eq!(always_down.load(Ordering::SeqCst), 2);
    let bad = endpoint("/bad");
    assert!(bad.send(|| bad.post()).await.is_err());
    assert_eq!(rejected.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn redirects_are_not_followed_and_oversized_audio_is_refused() {
    let elsewhere = Arc::new(AtomicUsize::new(0));
    let landed = elsewhere.clone();
    let router = Router::new()
        .route(
            "/redirect",
            post(|| async { (HttpStatus::TEMPORARY_REDIRECT, [("location", "/elsewhere")]) }),
        )
        .route(
            "/elsewhere",
            post(move || {
                let landed = landed.clone();
                async move {
                    landed.fetch_add(1, Ordering::SeqCst);
                    "reached"
                }
            }),
        )
        .route(
            "/huge",
            post(|| async { vec![0u8; MAX_AUDIO_RESPONSE + 1] }),
        );
    let base = serve(router).await;
    let redirect =
        Endpoint::new(&format!("{base}/redirect"), "key", Duration::from_secs(5)).unwrap();
    assert!(redirect.send(|| redirect.post()).await.is_err());
    assert_eq!(elsewhere.load(Ordering::SeqCst), 0);

    let huge = Endpoint::new(&format!("{base}/huge"), "key", Duration::from_secs(5)).unwrap();
    let response = huge.send(|| huge.post()).await.unwrap();
    assert!(read_limited(response, MAX_AUDIO_RESPONSE).await.is_err());
}

#[tokio::test]
async fn probe_exercises_all_three_stages() {
    let seen = Shared::default();
    let pcm: Vec<u8> = (0..2_400i16).flat_map(|n| (n * 5).to_le_bytes()).collect();
    let router = Router::new()
        .route(
            "/v1/audio/speech",
            post(move || {
                let pcm = pcm.clone();
                async move { pcm }
            }),
        )
        .route(
            "/v1/audio/transcriptions",
            post(|| async { axum::Json(json!({"text": ""})) }),
        )
        .route(
            "/v1/chat/completions",
            post(
                |State(seen): State<Shared>, headers: HeaderMap, body: Bytes| async move {
                    record(State(seen), headers, body).await;
                    axum::Json(json!({"choices":[{"message":{"content":"Ready."}}]}))
                },
            ),
        )
        .with_state(seen.clone());
    let base = serve(router).await;
    let pipeline = build_pipeline(&config(&base), "key").unwrap();
    assert!(probe(&pipeline).await.is_ok());
    assert_eq!(seen.hits.load(Ordering::SeqCst), 1);

    let offline = build_pipeline(&config("http://127.0.0.1:9"), "key").unwrap();
    assert_eq!(probe(&offline).await, Err("cascade_tts_unavailable"));
}

#[test]
fn extra_chat_fields_merge_without_replacing_the_request() {
    let request = ChatRequest {
        messages: vec![json!({"role":"user","content":"hi"})],
        tools: json!([]),
    };
    let extra = [
        ("reasoning_effort".to_owned(), json!("none")),
        ("model".to_owned(), json!("other")),
    ]
    .into_iter()
    .collect();
    let body = chat_body("local", false, &extra, &request);
    assert_eq!(body["reasoning_effort"], "none");
    assert_eq!(body["model"], "local");
}

#[test]
fn wav_data_chunk_is_extracted() {
    let mut wav = b"RIFF\0\0\0\0WAVEfmt \x10\0\0\0".to_vec();
    wav.extend_from_slice(&[0; 16]);
    wav.extend_from_slice(b"data\x04\0\0\0\x01\x02\x03\x04");
    assert_eq!(super::wav_pcm(&wav).unwrap(), vec![1, 2, 3, 4]);
    assert!(super::wav_pcm(b"RIFF").is_none());
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn say_synthesizes_pcm() {
    let mut config = config("http://127.0.0.1:1");
    config.tts = crate::common::TtsEngine::Say;
    config.tts_voice = "Fred".into();
    let pipeline = build_pipeline(&config, "").unwrap();
    let pcm = pipeline
        .tts
        .synthesize("Hello there.".into())
        .await
        .unwrap();
    assert!(pcm.len() > 4_800);
}
