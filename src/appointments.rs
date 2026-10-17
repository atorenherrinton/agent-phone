//! Narrow inbound scheduling boundary. The remote voice model supplies a
//! proposal; the local scheduler owns verification, calendar access and receipts.

use crate::common::{SafeResult, check};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};

pub const RESCHEDULING_MESSAGE_INSTRUCTIONS: &str = "\nFor appointment rescheduling, override earlier requests to collect caller details: do not ask for the caller's name, business name, branch address, callback number, or caller-ID confirmation. Use the incoming caller ID as the default callback. A volunteered callback number may be recorded but never changes verification. Ask only for the missing proposed date/time and clarify timezone only when unclear, then read the proposed time back once for confirmation. Do not ask for the original appointment time; use it only if already volunteered. If verification or calendar matching is unavailable or ambiguous, take the short rescheduling message without further identity questions or any booking claim. Recording objection still immediately takes priority.\n";

pub const INBOUND_INSTRUCTIONS: &str = "\nRuntime appointment capability: for this inbound call only, the earlier message-only and no-callback restrictions have one narrow exception. A business asking to reschedule an existing appointment may propose a tentative replacement time through tentatively_reschedule_appointment. After confirming the proposed time, invoke the tool with proposed_start and caller_confirmed. Include original_start only if the caller already volunteered it; otherwise omit it. The local service automatically looks up this call's incoming caller ID in public Apple Maps listings and identifies the existing appointment from the matching business listing and calendar. Never ask the caller for verification details, search contacts, or expose owner calendar entries. Do not claim those checks passed before a successful result. Only a tentative_hold_created result permits saying the time is penciled in tentatively, pending owner review, and that the owner will call back to reschedule if that date does not work. Keep the original appointment pending owner review; do not claim it was cancelled or definitively moved. If the tool cannot verify the arrangement or its result is uncertain, take the proposed-time message and use incoming caller ID for callback without more questions. Never infer availability, probe alternative times, retry scheduling in this call, or place an outbound call. Recording objection still immediately takes priority.\n";

pub const TOOL_NAME: &str = "tentatively_reschedule_appointment";
pub const MAX_ARGUMENT_BYTES: usize = 2048;

// Deliberately no Debug: appointment dates and caller statements are private.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_start: Option<String>,
    pub proposed_start: String,
    pub caller_confirmed: bool,
}

impl Request {
    pub fn parse(value: Value) -> SafeResult<Self> {
        let request: Self =
            serde_json::from_value(value).map_err(|_| "appointment_arguments_invalid")?;
        for text in request
            .original_start
            .iter()
            .chain([&request.proposed_start])
        {
            check(
                text.len() <= 40 && chrono::DateTime::parse_from_rfc3339(text).is_ok(),
                "appointment_time_invalid",
            )?;
        }
        check(request.caller_confirmed, "appointment_confirmation_missing")?;
        Ok(request)
    }

    /// Project historical durable receipts without accepting identity hints
    /// from the live voice model. Never rewrite existing proposal evidence.
    pub fn parse_stored(mut value: Value) -> SafeResult<Self> {
        let object = value
            .as_object_mut()
            .ok_or("appointment_arguments_invalid")?;
        if object.contains_key("business_name") || object.contains_key("business_address") {
            for key in ["business_name", "business_address"] {
                let text = object
                    .get(key)
                    .and_then(Value::as_str)
                    .ok_or("appointment_business_invalid")?;
                check(
                    (3..=200).contains(&text.len())
                        && !text.chars().any(char::is_control)
                        && !text.contains("://"),
                    "appointment_business_invalid",
                )?;
            }
            check(
                object.get("original_start").is_some_and(Value::is_string),
                "appointment_time_invalid",
            )?;
            object.remove("business_name");
            object.remove("business_address");
        }
        Self::parse(value)
    }
}

pub type SchedulingFuture = Pin<Box<dyn Future<Output = Value> + Send>>;

pub trait AppointmentScheduler: Send + Sync {
    /// Implementations receive their call identity from authenticated ingress,
    /// never from these model arguments. Dropping the future stops new work.
    fn schedule(&self, request: Request) -> SchedulingFuture;
}

pub fn tool_definition() -> Value {
    json!({
        "type":"function", "name":TOOL_NAME,
        "description":"For an inbound business asking to reschedule an existing appointment only. Ask only for missing proposed date/time, clarify timezone if unclear, read it back and obtain confirmation. Do not ask for caller/business name, branch/address, callback number, caller-ID confirmation, or the original appointment time. The local service automatically verifies this call's incoming caller ID against a public Apple Maps business listing, identifies the existing calendar appointment, checks conflicts, and may create one tentative hold with no invitations. Include original_start only if already volunteered. It preserves the original appointment for owner review. Do not claim verification, availability or a calendar change until the tool explicitly succeeds. On any unsuccessful or uncertain result, take the proposed-time message using incoming caller ID for callback, without further verification questions or a booking promise. Never reveal other calendar events or propose times by probing this tool.",
        "parameters":{
            "type":"object","additionalProperties":false,
            "properties":{
                "original_start":{"type":"string","description":"Optional existing appointment start in RFC3339 with explicit UTC offset, only if already volunteered. Otherwise omit; do not ask for it or invent it."},
                "proposed_start":{"type":"string","description":"Proposed appointment start, read back to the caller, in RFC3339 with an explicit UTC offset."},
                "caller_confirmed":{"type":"boolean","enum":[true]}
            },
            "required":["proposed_start","caller_confirmed"]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn request() -> Value {
        json!({"proposed_start":"2026-10-02T10:00:00-07:00","caller_confirmed":true})
    }

    #[test]
    fn proposal_cannot_supply_identity_credentials_or_ambiguous_dates() {
        assert!(Request::parse(request()).is_ok());
        for (key, value) in [
            ("caller_number", json!("+12065550100")),
            ("business_name", json!("Example Clinic")),
            ("calendar_id", json!("primary")),
            ("owner_authorized", json!(true)),
            ("original_start", json!("tomorrow at nine")),
            ("caller_confirmed", json!(false)),
            ("business_address", json!("https://caller.invalid")),
        ] {
            let mut data = request();
            data[key] = value;
            assert!(Request::parse(data).is_err(), "{key}");
        }
    }

    #[test]
    fn historical_identity_fields_are_readable_only_from_durable_receipts() {
        let mut historical = request();
        historical["business_name"] = json!("Example Clinic");
        historical["business_address"] = json!("123 Example St, Example City");
        historical["original_start"] = json!("2026-10-01T09:00:00-07:00");
        assert!(Request::parse(historical.clone()).is_err());
        let decoded = Request::parse_stored(historical.clone()).unwrap();
        assert_eq!(
            decoded.original_start.as_deref(),
            Some("2026-10-01T09:00:00-07:00")
        );
        historical["caller_number"] = json!("+12065550100");
        assert!(Request::parse_stored(historical).is_err());
        assert!(
            Request::parse_stored(request())
                .unwrap()
                .original_start
                .is_none()
        );
    }
}
