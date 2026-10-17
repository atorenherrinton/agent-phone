//! HTTP clients for the cascade stages.
//!
//! Each stage speaks the common OpenAI-compatible shape, so Kokoro-FastAPI, a
//! Whisper server, Ollama/llama.cpp, or OpenAI itself can fill it:
//!
//! - `POST {stt_url}` multipart (`file`, `model`) -> `{"text": "..."}`
//! - `POST {llm_url}` chat completions -> `choices[0].message`
//! - `POST {tts_url}` `{model, input, voice, response_format: "pcm"}` -> raw
//!   24 kHz signed 16-bit little-endian mono PCM
//!
//! The account's OpenAI credential is attached ONLY to `https://api.openai.com`.
//! Redirects and environment proxies are disabled so a credential can never be
//! forwarded to another host, and errors carry no provider text.

use crate::audio;
use crate::cascade::{
    ChatModel, ChatReply, ChatRequest, Pipeline, SpeechToText, StageError, Synthesizer, ToolCall,
};
use crate::common::{CascadeConfig, SafeResult, TtsEngine, check, validate_endpoint};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use reqwest::header::{AUTHORIZATION, HeaderValue};
use reqwest::multipart::{Form, Part};
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const STT_TIMEOUT: Duration = Duration::from_secs(20);
const CHAT_TIMEOUT: Duration = Duration::from_secs(40);
const TTS_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_TEXT_RESPONSE: usize = 256 * 1024;
const MAX_AUDIO_RESPONSE: usize = 6 * 1024 * 1024; // ~2 minutes of 24 kHz PCM16
const MAX_REPLY_TOKENS: u32 = 1024;

fn openai_host(url: &Url) -> bool {
    url.scheme() == "https" && url.host_str() == Some("api.openai.com")
}

/// The credential is sent to OpenAI and nowhere else.
pub(crate) fn authorization(url: &Url, api_key: &str) -> Option<HeaderValue> {
    if !openai_host(url)
        || api_key.is_empty()
        || api_key.len() > 8192
        || api_key.contains(['\r', '\n'])
    {
        return None;
    }
    let mut value = HeaderValue::from_str(&format!("Bearer {api_key}")).ok()?;
    value.set_sensitive(true);
    Some(value)
}

struct Endpoint {
    client: Client,
    url: Url,
    authorization: Option<HeaderValue>,
}

impl Endpoint {
    fn new(value: &str, api_key: &str, timeout: Duration) -> SafeResult<Self> {
        let url = validate_endpoint(value)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(3))
            .timeout(timeout)
            .build()
            .map_err(|_| "cascade_client_failed")?;
        Ok(Self {
            client,
            authorization: authorization(&url, api_key),
            url,
        })
    }

    fn post(&self) -> RequestBuilder {
        let request = self.client.post(self.url.clone());
        match &self.authorization {
            Some(value) => request.header(AUTHORIZATION, value.clone()),
            None => request,
        }
    }

    /// One retry for transient failures. Every stage request is idempotent
    /// (recognize, complete, synthesize), unlike the phone-call actions.
    async fn send(
        &self,
        build: impl Fn() -> RequestBuilder,
    ) -> Result<reqwest::Response, StageError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match build().send().await {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response)
                    if attempts < 2
                        && (response.status().is_server_error()
                            || response.status() == StatusCode::TOO_MANY_REQUESTS) => {}
                Ok(_) => return Err(StageError),
                Err(_) if attempts < 2 => {}
                Err(_) => return Err(StageError),
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
}

async fn read_limited(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, StageError> {
    if response
        .content_length()
        .is_some_and(|len| len as usize > limit)
    {
        return Err(StageError);
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| StageError)?;
        if body.len() + chunk.len() > limit {
            return Err(StageError);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

struct HttpStt {
    endpoint: Endpoint,
    model: String,
    language: Option<String>,
}

impl SpeechToText for HttpStt {
    fn transcribe(&self, mulaw: Vec<u8>) -> BoxFuture<'_, Result<String, StageError>> {
        Box::pin(async move {
            let wav = audio::wav_from_mulaw(&mulaw);
            let response = self
                .endpoint
                .send(|| {
                    let file = Part::bytes(wav.clone())
                        .file_name("audio.wav")
                        .mime_str("audio/wav")
                        .unwrap_or_else(|_| Part::bytes(wav.clone()));
                    let mut form = Form::new()
                        .part("file", file)
                        .text("model", self.model.clone())
                        .text("response_format", "json")
                        .text("temperature", "0");
                    if let Some(language) = &self.language {
                        form = form.text("language", language.clone());
                    }
                    self.endpoint.post().multipart(form)
                })
                .await?;
            parse_transcript(&read_limited(response, MAX_TEXT_RESPONSE).await?)
        })
    }
}

pub(crate) fn parse_transcript(body: &[u8]) -> Result<String, StageError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| StageError)?;
    value["text"]
        .as_str()
        .map(|text| text.trim().to_owned())
        .ok_or(StageError)
}

struct HttpChat {
    endpoint: Endpoint,
    model: String,
    openai: bool,
    extra: BTreeMap<String, Value>,
}

pub(crate) fn chat_body(
    model: &str,
    openai: bool,
    extra: &BTreeMap<String, Value>,
    request: &ChatRequest,
) -> Value {
    let mut body = json!({"model": model, "messages": request.messages, "stream": false});
    for (key, value) in extra {
        if !matches!(key.as_str(), "model" | "messages" | "stream" | "tools") {
            body[key] = value.clone();
        }
    }
    // OpenAI's current models reject `max_tokens`; local servers may not know
    // `max_completion_tokens`.
    body[if openai {
        "max_completion_tokens"
    } else {
        "max_tokens"
    }] = json!(MAX_REPLY_TOKENS);
    if request
        .tools
        .as_array()
        .is_some_and(|tools| !tools.is_empty())
    {
        body["tools"] = request.tools.clone();
        body["tool_choice"] = json!("auto");
        if openai {
            body["parallel_tool_calls"] = json!(false);
        }
    }
    body
}

pub(crate) fn parse_chat_reply(body: &[u8]) -> Result<ChatReply, StageError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| StageError)?;
    let message = &value["choices"][0]["message"];
    let text = message["content"].as_str().unwrap_or_default().to_owned();
    let calls = message["tool_calls"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    // One fixed tool per turn, as with Realtime: anything else is a protocol fault.
    if calls.len() > 1 {
        return Err(StageError);
    }
    let tool = match calls.first() {
        Some(call) => {
            if call["type"] != "function" {
                return Err(StageError);
            }
            Some(ToolCall {
                name: call["function"]["name"]
                    .as_str()
                    .ok_or(StageError)?
                    .to_owned(),
                arguments: call["function"]["arguments"]
                    .as_str()
                    .unwrap_or("{}")
                    .to_owned(),
            })
        }
        None => None,
    };
    // Dead air is a failure, not a reply (for example a reasoning model that
    // spent its whole token budget before speaking).
    if text.trim().is_empty() && tool.is_none() {
        return Err(StageError);
    }
    Ok(ChatReply { text, tool })
}

impl ChatModel for HttpChat {
    fn complete(&self, request: ChatRequest) -> BoxFuture<'_, Result<ChatReply, StageError>> {
        Box::pin(async move {
            let body = chat_body(&self.model, self.openai, &self.extra, &request);
            let response = self
                .endpoint
                .send(|| self.endpoint.post().json(&body))
                .await?;
            parse_chat_reply(&read_limited(response, MAX_TEXT_RESPONSE).await?)
        })
    }
}

struct HttpTts {
    endpoint: Endpoint,
    model: String,
    voice: String,
}

impl Synthesizer for HttpTts {
    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<Vec<u8>, StageError>> {
        Box::pin(async move {
            let body = json!({
                "model": self.model, "input": text, "voice": self.voice,
                "response_format": "pcm", "speed": 1.0
            });
            let response = self
                .endpoint
                .send(|| self.endpoint.post().json(&body))
                .await?;
            let pcm = read_limited(response, MAX_AUDIO_RESPONSE).await?;
            if pcm.len() < 2 {
                return Err(StageError);
            }
            Ok(pcm)
        })
    }
}

/// macOS `say`, run without a shell. It writes 24 kHz 16-bit little-endian
/// WAVE, which is the same PCM the HTTP synthesizer returns once the RIFF
/// header is removed.
struct SayTts {
    voice: String,
    rate: Option<u32>,
}

impl Synthesizer for SayTts {
    fn synthesize(&self, text: String) -> BoxFuture<'_, Result<Vec<u8>, StageError>> {
        Box::pin(async move {
            let text = text.replace(['\0', '\r', '\n'], " ");
            if text.trim().is_empty() || text.len() > 8192 {
                return Err(StageError);
            }
            let file = tempfile::Builder::new()
                .suffix(".wav")
                .tempfile()
                .map_err(|_| StageError)?;
            let mut command = tokio::process::Command::new("/usr/bin/say");
            command
                .arg("-v")
                .arg(&self.voice)
                .arg("--file-format=WAVE")
                .arg("--data-format=LEI16@24000")
                .arg("-o")
                .arg(file.path());
            if let Some(rate) = self.rate {
                command.arg("-r").arg(rate.to_string());
            }
            // `--` keeps text that starts with a dash from being read as an option.
            command
                .arg("--")
                .arg(&text)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true);
            let status = tokio::time::timeout(TTS_TIMEOUT, command.status())
                .await
                .map_err(|_| StageError)?
                .map_err(|_| StageError)?;
            if !status.success() {
                return Err(StageError);
            }
            let wav = tokio::fs::read(file.path()).await.map_err(|_| StageError)?;
            wav_pcm(&wav).ok_or(StageError)
        })
    }
}

/// The `data` chunk of a 16-bit PCM WAVE file.
pub(crate) fn wav_pcm(wav: &[u8]) -> Option<Vec<u8>> {
    if wav.len() < 12 || &wav[..4] != b"RIFF" || &wav[8..12] != b"WAVE" {
        return None;
    }
    let mut at = 12;
    while at + 8 <= wav.len() {
        let id = &wav[at..at + 4];
        let len = u32::from_le_bytes(wav[at + 4..at + 8].try_into().ok()?) as usize;
        let body = at + 8;
        if id == b"data" {
            let end = body.checked_add(len)?.min(wav.len());
            let pcm = wav[body..end].to_vec();
            return (pcm.len() >= 2 && pcm.len() <= MAX_AUDIO_RESPONSE).then_some(pcm);
        }
        at = body.checked_add(len)?.checked_add(len % 2)?;
    }
    None
}

/// Build the three stage clients from validated configuration. `api_key` is the
/// account's OpenAI credential; it is used only for `https://api.openai.com`.
pub fn build_pipeline(config: &CascadeConfig, api_key: &str) -> SafeResult<Pipeline> {
    config.validate()?;
    let stt = Endpoint::new(&config.stt_url, api_key, STT_TIMEOUT)?;
    let chat = Endpoint::new(&config.llm_url, api_key, CHAT_TIMEOUT)?;
    let openai = openai_host(&chat.url);
    let tts: Arc<dyn Synthesizer> = match config.tts {
        TtsEngine::Http => Arc::new(HttpTts {
            endpoint: Endpoint::new(
                config.tts_url.as_deref().ok_or("cascade_tts_url_missing")?,
                api_key,
                TTS_TIMEOUT,
            )?,
            model: config.tts_model.clone(),
            voice: config.tts_voice.clone(),
        }),
        TtsEngine::Say => Arc::new(SayTts {
            voice: config.tts_voice.clone(),
            rate: config.tts_rate,
        }),
    };
    Ok(Pipeline {
        stt: Arc::new(HttpStt {
            endpoint: stt,
            model: config.stt_model.clone(),
            language: config.stt_language.clone(),
        }),
        chat: Arc::new(HttpChat {
            endpoint: chat,
            model: config.llm_model.clone(),
            openai,
            extra: config.llm_extra.clone(),
        }),
        tts,
    })
}

/// Best-effort request that makes a local model server load the chat model
/// before the first turn (Ollama otherwise pays a multi-second cold start).
pub async fn warm(config: &CascadeConfig) {
    let Ok(endpoint) = Endpoint::new(&config.llm_url, "", CHAT_TIMEOUT) else {
        return;
    };
    if openai_host(&endpoint.url) {
        return;
    }
    let mut body = chat_body(
        &config.llm_model,
        false,
        &config.llm_extra,
        &ChatRequest {
            messages: vec![json!({"role":"user","content":"ok"})],
            tools: json!([]),
        },
    );
    body["max_tokens"] = json!(1);
    let _ = endpoint.post().json(&body).send().await;
}

/// Exercise all three stages once without a call: synthesize a short phrase,
/// recognize a quarter second of silence, and request a minimal completion.
/// Like the Realtime probe, this makes external requests only when invoked.
pub async fn probe(pipeline: &Pipeline) -> SafeResult<()> {
    let pcm = pipeline
        .tts
        .synthesize("Configuration check.".into())
        .await
        .map_err(|_| "cascade_tts_unavailable")?;
    check(
        !audio::pcm24k_to_mulaw(&pcm).is_empty(),
        "cascade_tts_silent",
    )?;
    pipeline
        .stt
        .transcribe(vec![0xff; 2_000])
        .await
        .map_err(|_| "cascade_stt_unavailable")?;
    pipeline
        .chat
        .complete(ChatRequest {
            messages: vec![
                json!({"role":"system","content":"Configuration validation only. Reply with one word."}),
                json!({"role":"user","content":"Ready?"}),
            ],
            tools: json!([]),
        })
        .await
        .map_err(|_| "cascade_llm_unavailable")?;
    Ok(())
}

#[cfg(test)]
mod tests;
