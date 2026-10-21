//! Owner notifications through one configured hook command.
//!
//! The hook receives a single JSON event on stdin and no shell is involved, so
//! any agent harness (Telegram bot, Slack, email, a chat agent's inbox) can be
//! plugged in with a small script. Exit status 0 means delivered. A non-zero
//! exit is retried a few times; a hook that is still running at its timeout is
//! killed and recorded as `uncertain`, never resent, because it may already
//! have delivered.

use crate::common::NotifyConfig;
use serde_json::Value;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

/// Bounded number of hook runs per event before it is marked failed.
pub const MAX_ATTEMPTS: i64 = 4;

#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Sent,
    Failed,
    Uncertain,
}

pub async fn send(config: &NotifyConfig, event: &Value) -> Outcome {
    let Ok(body) = serde_json::to_vec(event) else {
        return Outcome::Failed;
    };
    let Some((program, args)) = config.command.split_first() else {
        return Outcome::Failed;
    };
    let child = tokio::process::Command::new(program)
        .args(args)
        .env("AGENT_PHONE_EVENT", event["event"].as_str().unwrap_or(""))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn();
    let Ok(mut child) = child else {
        return Outcome::Failed;
    };
    let run = async {
        if let Some(mut stdin) = child.stdin.take() {
            // A hook that ignores stdin may close it early; that is not a failure.
            let _ = stdin.write_all(&body).await;
            let _ = stdin.shutdown().await;
        }
        child.wait().await
    };
    match tokio::time::timeout(Duration::from_secs(config.timeout_secs), run).await {
        Ok(Ok(status)) if status.success() => Outcome::Sent,
        Ok(_) => Outcome::Failed,
        Err(_) => Outcome::Uncertain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hook(script: &str, timeout_secs: u64) -> NotifyConfig {
        NotifyConfig {
            command: vec!["/bin/sh".into(), "-c".into(), script.into()],
            timeout_secs,
        }
    }

    #[tokio::test]
    async fn hook_receives_the_event_on_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("event.json");
        let config = hook(&format!("cat > '{}'", out.display()), 10);
        let event = json!({"event":"summary","text":"hello"});
        assert_eq!(send(&config, &event).await, Outcome::Sent);
        let seen: Value = serde_json::from_slice(&std::fs::read(out).unwrap()).unwrap();
        assert_eq!(seen, event);
    }

    #[tokio::test]
    async fn failures_and_timeouts_are_distinguished() {
        assert_eq!(send(&hook("exit 3", 10), &json!({})).await, Outcome::Failed);
        assert_eq!(
            send(&hook("sleep 5", 1), &json!({})).await,
            Outcome::Uncertain
        );
        let missing = NotifyConfig {
            command: vec!["/nonexistent/hook".into()],
            timeout_secs: 5,
        };
        assert_eq!(send(&missing, &json!({})).await, Outcome::Failed);
    }
}
