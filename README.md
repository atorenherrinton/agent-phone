# agent-phone

Phone calls for AI agents. A small Rust service that puts a Twilio number in
front of an AI voice assistant and exposes it to **any agent** over MCP (or a
JSON CLI):

- **Voicemail screening.** Inbound calls (direct, or forwarded from your cell)
  are answered by an AI that discloses itself, gets recording consent, and takes
  a message. You get a summary and the audio through a notify hook.
- **Outbound AI calls.** An agent asks for a call with a bounded purpose; the
  phone AI places it, discloses that it is an AI calling on your behalf, handles
  the conversation, and reports the outcome and transcript.
- **Coach mode (3-way).** With `coach`, the service also rings your own number
  (`forwarded_from`). You hear both sides live, anything you say is a private
  note only the AI hears, **1** takes over the call (your voice goes straight to
  the other party, the AI goes quiet) and **2** hands it back. Hanging up
  returns the call to the AI. Requires the cascade voice engine.

Forked from `zeroclaw-phone` and made agent-agnostic: no ZeroClaw config,
secret store, Discord, or calendar integration. It runs fully locally if you
want (Whisper + any OpenAI-compatible local model + macOS `say`), or with the
OpenAI Realtime API.

## Security model

The voice AI on a call is an isolated session. It has no memory, files,
contacts or tools beyond ending its own call; it only knows the screening
policy, the owner's name, the caller ID and (outbound) the agent-supplied
`purpose`. So a caller cannot extract data it does not have, but **anything an
agent writes into `purpose` may be spoken**.

- Twilio webhooks are signature-verified and bound to one-time nonces.
- One call at a time; exact duplicate call requests are coalesced for ten
  minutes and outcome-unknown requests are never replayed.
- Optional `allowed_destinations` allowlist that agents cannot edit.
- Callers' words reach agents only as fields marked `untrusted_*`.
- Recordings are downloaded to a private 0600 archive and deleted from Twilio.
- Secrets live in `secrets/` (0600 files) or environment variables, never in
  `phone.toml`.

## Install

```sh
cargo build --release
rm -f ~/.local/bin/agent-phone && cp target/release/agent-phone ~/.local/bin/
codesign -f -s - ~/.local/bin/agent-phone   # macOS: a reinstalled linker-signed binary can fail launchd's signature check
# Upgrading a running LaunchAgent: `launchctl bootout` it before replacing the binary and
# `launchctl bootstrap` it after; swapping it under a running job can fail to relaunch.
mkdir -p ~/.agent-phone && chmod 700 ~/.agent-phone
cp examples/phone.toml examples/screening.md ~/.agent-phone/ && chmod 600 ~/.agent-phone/*
printf %s "$TWILIO_AUTH_TOKEN" | agent-phone set-secret twilio_auth_token
agent-phone check          # {"configValid":true}
agent-phone probe          # exercises STT, LLM and TTS once
agent-phone serve          # webhook server on 127.0.0.1:<port> and <root>/serve.sock
```

Expose the server over HTTPS (Tailscale Funnel, Cloudflare Tunnel, ngrok...) and
point the Twilio number's voice webhook at `https://<public>/voice/webhook`
(POST). With a userspace `tailscaled`, proxy to the unix socket rather than the
port: tailscaled drops its TCP connections to localhost on every network link
change, which Twilio sees as a 502 and the caller as a busy signal.

```sh
sudo tailscale funnel --bg unix:$HOME/.agent-phone/serve.sock   # unix backends require root
twilio phone-numbers:update +12065550100 --voice-url https://<public>/voice/webhook --voice-method POST \
  --voice-fallback-url https://<public>/voice/webhook --voice-fallback-method POST \
  --status-callback "https://<public>/voice/webhook?type=status" --status-callback-method POST
agent-phone route-check    # public health + unsigned requests rejected
agent-phone preflight      # Twilio number points at this service
```

## Connect an agent

MCP (stdio), e.g. Claude Code, Claude Desktop, nullclaw, or any MCP client:

```json
{"mcpServers": {"phone": {"command": "agent-phone", "args": ["--root", "/Users/you/.agent-phone", "mcp"]}}}
```

Tools: `place_call`, `call_status`, `list_calls`, `get_call`.

CLI equivalents (JSON output): `agent-phone call --to +1... --purpose "..." [--coach]`,
`status ID`, `calls`, `show CALL_SID`. Owner-only: `allow +1...` /
`disallow +1...` edit the allowlist; don't expose them to agents.

## Notify hook

`[notify] command` runs once per event with JSON on stdin, no shell:

```json
{"event":"summary","call_sid":"CA...","direction":"inbound","caller_id_unverified":"+1...","outcome":"...","text":"..."}
{"event":"recording","call_sid":"CA...","direction":"inbound","recording_path":"/.../RE....mp3","mime":"audio/mpeg"}
```

Exit 0 = delivered; non-zero is retried a few times; a hook still running at
`timeout_secs` is recorded as uncertain and never resent.
`examples/notify-telegram.py` sends both to a Telegram chat.

## Voice engines

| Engine | Pipeline |
| --- | --- |
| `cascade` | STT endpoint → chat model → TTS (`say` or an OpenAI-compatible `/v1/audio/speech`) |
| `realtime` | One OpenAI Realtime session (needs `secrets/openai_api_key`) |

Cascade stages speak the common OpenAI-compatible shapes, so whisper.cpp,
Ollama, llama.cpp, Kokoro-FastAPI or OpenAI can fill any of them. `llm_extra`
is merged into every chat request (e.g. `reasoning_effort = "none"` to turn off
thinking on local reasoning models). On a cascade call the service pre-warms
the model when the call arrives so a cold local model loads during the
greeting.

## Inbound consent

`recording_consent = "notice"` plays an AI and recording notice and asks the
caller to say "I agree"; silence or an objection ends the call without
recording. `"explicit"` asks for a keypad opt-in and transcribes without
recording if declined. Recognized objections later in the call end it and
discard the recording and transcript. Whether notice alone is sufficient
depends on your jurisdiction.

## Development

```sh
cargo test --release
cargo clippy --release --all-targets -- -D warnings
```

Tests are offline: fake Twilio, fake model stages, temporary private roots.
A real phone call is still the only end-to-end check of timing and voice.

## License

MIT
