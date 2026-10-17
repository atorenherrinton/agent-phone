use crate::common::{SafeResult, check};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::collections::BTreeMap;

pub type Form = BTreeMap<String, String>;

fn decode(s: &str) -> SafeResult<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => {
                check(i + 2 < b.len(), "invalid_form_escape")?;
                let h = (b[i + 1] as char)
                    .to_digit(16)
                    .ok_or("invalid_form_escape")?;
                let l = (b[i + 2] as char)
                    .to_digit(16)
                    .ok_or("invalid_form_escape")?;
                out.push((h * 16 + l) as u8);
                i += 2;
            }
            v => out.push(v),
        }
        i += 1;
    }
    let value = String::from_utf8(out).map_err(|_| "invalid_form_utf8")?;
    check(!value.contains('\0'), "invalid_form_nul")?;
    Ok(value)
}

pub fn parse_form(bytes: &[u8]) -> SafeResult<Form> {
    check(bytes.len() <= 32 * 1024, "form_too_large")?;
    let raw = std::str::from_utf8(bytes).map_err(|_| "invalid_form_utf8")?;
    let mut out = Form::new();
    if raw.is_empty() {
        return Ok(out);
    }
    for pair in raw.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        let k = decode(k)?;
        let v = decode(v)?;
        check(
            !k.is_empty() && !out.contains_key(&k),
            "ambiguous_form_field",
        )?;
        out.insert(k, v);
    }
    Ok(out)
}

pub fn one<'a>(form: &'a Form, key: &str) -> SafeResult<&'a str> {
    form.get(key)
        .map(String::as_str)
        .ok_or("missing_form_field")
}

pub fn valid_sid(s: &str, prefix: &str) -> bool {
    s.len() == 34 && s.starts_with(prefix) && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn signature_ok(token: &str, public_url: &str, form: &Form, signature: &str) -> bool {
    if token.is_empty() || signature.len() != 28 {
        return false;
    }
    let Ok(signature) = STANDARD.decode(signature) else {
        return false;
    };
    let mut message = public_url.as_bytes().to_vec();
    for (key, value) in form {
        message.extend(key.as_bytes());
        message.extend(value.as_bytes());
    }
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, token.as_bytes());
    ring::hmac::verify(&key, &message, &signature).is_ok()
}

pub fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Twilio documents an optional trailing slash in voice WebSocket signatures.
/// Only these four exact spellings of our configured, nonce-bound URL are valid.
pub fn websocket_signature_ok(token: &str, https_url: &str, signature: &str) -> bool {
    if !https_url.starts_with("https://") || https_url.contains('?') || https_url.contains('#') {
        return false;
    }
    let wss_url = https_url.replacen("https://", "wss://", 1);
    let empty = Form::new();
    [
        https_url.to_owned(),
        format!("{https_url}/"),
        wss_url.clone(),
        format!("{wss_url}/"),
    ]
    .iter()
    .any(|url| signature_ok(token, url, &empty, signature))
}

pub const EMPTY: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response/>";
pub const REJECT: &str =
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Reject reason=\"rejected\"/></Response>";
pub const HANGUP: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Hangup/></Response>";

pub fn consent_xml(base: &str, nonce: &str) -> String {
    let action = xml(&format!("{base}/voice/consent/{nonce}"));
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Gather input=\"dtmf\" numDigits=\"1\" timeout=\"10\" actionOnEmptyResult=\"true\" action=\"{action}\" method=\"POST\"><Say>You have reached an AI call-screening assistant. This call will be transcribed to take a message. With your permission, the conversation will also be recorded and privately sent to the person you called. Press 1 to agree to recording, or press 2 to continue without recording. If you do not want transcription, please hang up.</Say></Gather><Hangup/></Response>"
    )
}

pub fn notice_xml(base: &str, nonce: &str) -> String {
    let action = xml(&format!("{base}/voice/notice/{nonce}"));
    // Keep disclosure outside Gather: caller speech must not interrupt it.
    // Only the following speech gate is transcribed; no call audio is recorded here.
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Say>Hi, I am an AI assistant taking a message for the person you called. This conversation will be recorded and transcribed, and privately sent to them. Before leaving your message, please say I agree, then wait for me to ask for your message. If you do not agree, please hang up now.</Say><Gather input=\"speech\" timeout=\"10\" speechTimeout=\"auto\" actionOnEmptyResult=\"true\" action=\"{action}\" method=\"POST\"><Say>Do you agree?</Say></Gather><Hangup/></Response>"
    )
}

/// Deterministic stop signal for recognizable recording/transcription objections.
/// This is deliberately independent of model behavior, not a general intent classifier.
pub fn recording_declined(text: &str) -> bool {
    let words = normalized_words(text);
    let contains = |phrase: &str| words.contains(&format!(" {phrase} "));
    if [
        "i do not consent",
        "i dont consent",
        "i refuse consent",
        "i withdraw consent",
        "i revoke consent",
    ]
    .iter()
    .any(|phrase| contains(phrase))
        || ["i do not agree", "i dont agree", "i decline"].contains(&words.trim())
    {
        return true;
    }
    recording_subject(&words)
        && [
            "do not record",
            "dont record",
            "do not transcribe",
            "dont transcribe",
            "do not want",
            "dont want",
            "not to",
            "no recording",
            "no transcription",
            "stop",
            "turn off",
            "without",
            "not comfortable",
            "not okay",
            "not ok",
            "not agree",
            "dont agree",
            "never agreed",
            "not consent",
            "not consenting",
            "object",
            "refuse",
            "decline",
            "rather not",
            "avoid",
            "wasnt",
            "was not",
            "isnt",
            "is not",
            "off the record",
        ]
        .iter()
        .any(|phrase| contains(phrase))
}

/// A privacy question at the notice gate is not affirmative continuation.
/// The bridge uses recording_declined only; normal in-call questions are not withdrawals.
pub fn recording_question(text: &str) -> bool {
    let words = normalized_words(text);
    recording_subject(&words)
        && (text.contains('?')
            || [
                "are you",
                "are we",
                "is this",
                "is it",
                "will you",
                "will this",
                "can you",
                "could you",
                "why",
                "who",
                "what",
                "where",
                "how",
            ]
            .iter()
            .any(|phrase| words.contains(&format!(" {phrase} "))))
}

fn recording_subject(words: &str) -> bool {
    [
        "record",
        "recorded",
        "recording",
        "recorder",
        "transcribe",
        "transcribed",
        "transcribing",
        "transcription",
    ]
    .iter()
    .any(|word| words.contains(&format!(" {word} ")))
}

fn normalized_words(text: &str) -> String {
    let normalized: String = text
        .to_lowercase()
        .chars()
        .filter_map(|c| match c {
            '\'' | '’' => None,
            c if c.is_alphanumeric() => Some(c),
            _ => Some(' '),
        })
        .collect();
    let normalized = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    format!(" {normalized} ")
}

pub fn connect_xml(base: &str, nonce: &str, record: bool) -> String {
    let stream = xml(&format!(
        "{}/voice/media/{nonce}",
        base.replacen("https://", "wss://", 1)
    ));
    let recording = if record {
        format!(
            "<Start><Recording track=\"both\" channels=\"dual\" trim=\"do-not-trim\" recordingStatusCallback=\"{}\" recordingStatusCallbackMethod=\"POST\" recordingStatusCallbackEvent=\"completed absent\"/></Start>",
            xml(&format!("{base}/voice/recording"))
        )
    } else {
        String::new()
    };
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response>{recording}<Connect><Stream url=\"{stream}\"/></Connect><Hangup/></Response>"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn websocket_signatures_only_accept_exact_canonical_variants() {
        use base64::Engine;
        let target = "https://phone.example/voice/media/fixture-nonce";
        for spelling in [
            target.to_owned(),
            format!("{target}/"),
            target.replacen("https://", "wss://", 1),
            format!("{}/", target.replacen("https://", "wss://", 1)),
        ] {
            let key =
                ring::hmac::Key::new(ring::hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, b"fixture-token");
            let signature = base64::engine::general_purpose::STANDARD
                .encode(ring::hmac::sign(&key, spelling.as_bytes()).as_ref());
            assert!(websocket_signature_ok("fixture-token", target, &signature));
            assert!(!websocket_signature_ok(
                "fixture-token",
                "https://other.example/voice/media/fixture-nonce",
                &signature
            ));
            assert!(!websocket_signature_ok(
                "fixture-token",
                "https://phone.example/voice/media/other-nonce",
                &signature
            ));
        }
        assert!(!websocket_signature_ok(
            "fixture-token",
            &format!("{target}?x=1"),
            "invalid"
        ));
    }
    #[test]
    fn parser_rejects_ambiguity_and_invalid_bytes() {
        for value in [
            "CallSid=a&CallSid=b",
            "a=%ZZ",
            "a=%FF",
            "a=%",
            "a=%00",
            "=x",
        ] {
            assert!(parse_form(value.as_bytes()).is_err());
        }
        assert_eq!(parse_form(b"x=a%2Bb+c").unwrap()["x"], "a+b c");
    }
    #[test]
    fn official_signature_fixture() {
        let f = parse_form(b"CallSid=CA1234567890ABCDE&Caller=%2B14158675310&Digits=1234&From=%2B14158675310&To=%2B18005551212").unwrap();
        assert!(signature_ok(
            "12345",
            "https://example.com/myapp.php?foo=1&bar=2",
            &f,
            "L/OH5YylLD5NRKLltdqwSvS0BnU="
        ));
        assert!(!signature_ok(
            "wrong",
            "https://example.com/myapp.php?foo=1&bar=2",
            &f,
            "L/OH5YylLD5NRKLltdqwSvS0BnU="
        ));
    }
    #[test]
    fn consent_precedes_and_controls_recording() {
        assert!(!consent_xml("https://test.invalid", "nonce").contains("<Recording"));
        assert!(!connect_xml("https://test.invalid", "nonce", false).contains("<Recording"));
        let xml = connect_xml("https://test.invalid", "nonce", true);
        assert!(xml.find("<Recording").unwrap() < xml.find("<Connect").unwrap());
        assert!(xml.contains("channels=\"dual\""));
    }

    #[test]
    fn notice_finishes_before_speech_gate_and_never_records() {
        let notice = notice_xml("https://test.invalid", "nonce");
        assert!(notice.find("</Say>").unwrap() < notice.find("<Gather").unwrap());
        assert!(notice.contains("input=\"speech\""));
        assert!(notice.contains("please say I agree, then wait"));
        assert!(notice.contains("<Say>Do you agree?</Say>"));
        assert!(!notice.contains("Please go ahead"));
        assert!(notice.contains("actionOnEmptyResult=\"true\""));
        assert!(notice.contains("/voice/notice/nonce"));
        assert!(!notice.contains("dtmf") && !notice.contains("Press"));
        assert!(!notice.contains("<Recording") && !notice.contains("<Connect"));
        assert!(notice.ends_with("</Gather><Hangup/></Response>"));
    }

    #[test]
    fn natural_recording_and_transcription_refusals_stop() {
        for speech in [
            "Please don't record this call.",
            "I don’t want to be recorded",
            "Do not transcribe me",
            "I do not consent.",
            "No recording please",
            "Can you stop recording?",
            "I'd rather not have this transcribed",
            "I'm not comfortable with recording",
            "I object to being recorded",
            "Can we continue without transcription?",
            "Please turn off the recorder",
            "I withdraw consent to recording",
            "I am not consenting to being recorded",
            "I never agreed to recording",
            "Please avoid recording me",
            "I would prefer this wasn't recorded",
            "I do not agree.",
            "I don't agree to recording",
            "I decline",
            "I decline to be recorded",
            "Please keep this off the record",
            "Off the record, please",
        ] {
            assert!(recording_declined(speech), "missed refusal: {speech}");
        }
        for speech in [
            "Hi, this is Alex calling about the appointment",
            "Recording is fine",
            "I don't mind being recorded",
            "Please don't forget to record my callback number",
            "No, Tuesday works better",
            "Nope",
            "Nah",
            "No, please",
            "I do not agree with the appointment time",
            "I don't agree with that estimate",
            "I decline the invitation",
        ] {
            assert!(!recording_declined(speech), "false refusal: {speech}");
        }
    }

    #[test]
    fn privacy_questions_are_not_notice_continuation_or_runtime_withdrawal() {
        for speech in [
            "Are you recording me?",
            "Is this being recorded",
            "Who gets this recording",
            "Will you transcribe my call?",
        ] {
            assert!(recording_question(speech));
            assert!(!recording_declined(speech));
        }
        assert!(!recording_question("Recording is fine. My name is Alex."));
        assert!(!recording_question("Can you please tell them Alex called?"));
    }
}
