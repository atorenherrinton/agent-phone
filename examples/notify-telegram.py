#!/usr/bin/python3 -I
"""agent-phone notify hook: send call summaries and recordings to Telegram.

agent-phone runs this once per event with one JSON object on stdin:
  {"event": "summary",   "call_sid": ..., "direction": "inbound"|"outbound", "text": ...}
  {"event": "recording", "call_sid": ..., "recording_path": "/.../RE....mp3", ...}

Configuration (environment):
  TELEGRAM_CHAT_ID     chat to send to (required)
  TELEGRAM_BOT_TOKEN   bot token, or
  NULLCLAW_CONFIG      path to a nullclaw config.json whose
                       channels.telegram.accounts.main.bot_token is used

Exit 0 only when Telegram accepted the message, so agent-phone can retry.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
import uuid


def bot_token():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if token:
        return token
    path = os.environ.get("NULLCLAW_CONFIG", os.path.expanduser("~/.nullclaw/config.json"))
    with open(path) as f:
        return json.load(f)["channels"]["telegram"]["accounts"]["main"]["bot_token"]


def call(token, method, body, content_type):
    request = urllib.request.Request(
        "https://api.telegram.org/bot%s/%s" % (token, method),
        data=body,
        headers={"Content-Type": content_type},
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        if json.load(response).get("ok") is not True:
            raise RuntimeError("Telegram refused the message")


def send_text(token, chat, text):
    for start in range(0, len(text), 3900):
        body = urllib.parse.urlencode({"chat_id": chat, "text": text[start:start + 3900]}).encode()
        call(token, "sendMessage", body, "application/x-www-form-urlencoded")


def send_audio(token, chat, path, caption):
    boundary = uuid.uuid4().hex
    with open(path, "rb") as f:
        audio = f.read()
    parts = []
    for name, value in (("chat_id", chat), ("caption", caption), ("title", "Call recording")):
        parts.append(
            ("--%s\r\nContent-Disposition: form-data; name=\"%s\"\r\n\r\n%s\r\n" % (boundary, name, value)).encode()
        )
    parts.append(
        ("--%s\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"call-recording.mp3\"\r\n"
         "Content-Type: audio/mpeg\r\n\r\n" % boundary).encode() + audio + b"\r\n"
    )
    parts.append(("--%s--\r\n" % boundary).encode())
    call(token, "sendAudio", b"".join(parts), "multipart/form-data; boundary=%s" % boundary)


def main():
    event = json.load(sys.stdin)
    chat = os.environ["TELEGRAM_CHAT_ID"]
    token = bot_token()
    kind = event.get("event")
    if kind == "summary":
        icon = "📞" if event.get("direction") == "outbound" else "📨"
        send_text(token, chat, "%s %s" % (icon, event.get("text", "")))
    elif kind == "recording":
        who = event.get("caller_id_unverified") or "unknown caller"
        caption = "🎙 Recording (%s call, %s)" % (event.get("direction", "inbound"), who)
        send_audio(token, chat, event["recording_path"], caption)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:  # report the failure class only; never echo call content
        print("notify-telegram failed: %s" % type(error).__name__, file=sys.stderr)
        sys.exit(1)
