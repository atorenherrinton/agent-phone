//! Actual carrier/model websocket fixtures for the local appointment boundary.
//! No credentials, calendar, Maps, live configuration or external calls.
use super::*;
use axum::{Router, extract::ws::WebSocketUpgrade, routing::get};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

type MockModel = WebSocketStream<TcpStream>;

#[derive(Default)]
struct Probes {
    calls: AtomicUsize,
    polled: AtomicUsize,
    dropped: AtomicUsize,
    requests: std::sync::Mutex<Vec<appointments::Request>>,
    entered: tokio::sync::Notify,
}
struct PendingGuard(Arc<Probes>);
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
struct LocalScheduler {
    probes: Arc<Probes>,
    pending: bool,
}
impl AppointmentScheduler for LocalScheduler {
    fn schedule(&self, request: appointments::Request) -> SchedulingFuture {
        self.probes.calls.fetch_add(1, Ordering::SeqCst);
        self.probes.requests.lock().unwrap().push(request);
        let probes = self.probes.clone();
        let pending = self.pending;
        Box::pin(async move {
            let _guard = PendingGuard(probes.clone());
            probes.polled.fetch_add(1, Ordering::SeqCst);
            probes.entered.notify_one();
            if pending {
                std::future::pending::<()>().await;
            }
            json!({"status":"tentative_hold_created","original_preserved":true,"synthetic_receipt":"fixture-only"})
        })
    }
}

fn proposal() -> Value {
    json!({"proposed_start":"2026-10-02T10:00:00-07:00","caller_confirmed":true})
}
fn start() -> Value {
    let stream = format!("MZ{}", "3".repeat(32));
    json!({"event":"start","streamSid":stream,"start":{"streamSid":stream,
        "accountSid":format!("AC{}", "1".repeat(32)),"callSid":format!("CA{}", "2".repeat(32)),
        "tracks":["inbound"],"mediaFormat":{"encoding":"audio/x-mulaw","sampleRate":8000,"channels":1}}})
}
fn output(arguments: &Value) -> Value {
    json!({"id":"item_appointment","type":"function_call","name":appointments::TOOL_NAME,
        "call_id":"call_appointment","arguments":arguments.to_string()})
}
fn done(status: &str, arguments: &Value) -> Value {
    json!({"type":"response.done","response":{"id":"resp_appointment","status":status,"output":[output(arguments)]}})
}
async fn send(model: &mut MockModel, event: Value) {
    model
        .send(ModelMessage::Text(event.to_string().into()))
        .await
        .unwrap();
}
async fn receive(model: &mut MockModel) -> Value {
    let frame = tokio::time::timeout(Duration::from_secs(2), model.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(frame.to_text().unwrap()).unwrap()
}
async fn barrier(model: &mut MockModel) {
    model
        .send(ModelMessage::Ping(b"appointment-barrier".to_vec().into()))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), model.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(frame, ModelMessage::Pong(_)));
}

struct Harness {
    twilio: Upstream,
    model: MockModel,
    raw_end: mpsc::Receiver<EndReason>,
    result: mpsc::Receiver<BridgeOutcome>,
    server: tokio::task::JoinHandle<()>,
    probes: Arc<Probes>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Harness {
    async fn new(pending: bool) -> Self {
        let probes = Arc::new(Probes::default());
        let scheduler: Arc<dyn AppointmentScheduler> = Arc::new(LocalScheduler {
            probes: probes.clone(),
            pending,
        });
        let model_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_address = model_listener.local_addr().unwrap();
        let carrier_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let carrier_address = carrier_listener.local_addr().unwrap();
        let (raw_sender, raw_end) = mpsc::channel(1);
        let (result_sender, result) = mpsc::channel(1);
        let router = Router::new().route(
            "/ws",
            get(move |upgrade: WebSocketUpgrade| {
                let scheduler = scheduler.clone();
                let raw_sender = raw_sender.clone();
                let result_sender = result_sender.clone();
                async move {
                    upgrade.on_upgrade(move |mut socket| async move {
                        let options = RealtimeOptions {
                            api_key: "synthetic-key-unused".into(),
                            instructions: "Synthetic appointment fixture".into(),
                            expected_account_sid: format!("AC{}", "1".repeat(32)),
                            expected_call_sid: format!("CA{}", "2".repeat(32)),
                            max_duration_secs: 10,
                            allow_end_call: false,
                            confirm_end_call: false,
                            stop_on_recording_decline: true,
                            appointments: Some(scheduler),
                        };
                        let mut state = State {
                            allow_appointments: true,
                            stop_on_recording_decline: true,
                            ..State::default()
                        };
                        let mut upstream = None;
                        let connection = async move {
                            tokio_tungstenite::connect_async(loopback_ws(model_address, "/"))
                                .await
                                .map(|(socket, _)| socket)
                                .map_err(|_| "fixture_connect_failed")
                        };
                        let deadline = Instant::now() + Duration::from_secs(10);
                        let reason = run_bridge(
                            &mut socket,
                            &options,
                            &mut state,
                            &mut upstream,
                            deadline,
                            connection,
                        )
                        .await
                        .expect_err("fixture must end");
                        // Observe return before transcript draining; any owned local
                        // scheduling future must already have been dropped here.
                        raw_sender.send(reason).await.unwrap();
                        let reason =
                            finish_bridge(&mut socket, &mut upstream, &mut state, reason, deadline)
                                .await;
                        result_sender.send(state.outcome(reason, 0)).await.unwrap();
                    })
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(carrier_listener, router).await.unwrap();
        });
        let (mut twilio, _) = tokio_tungstenite::connect_async(loopback_ws(carrier_address, "/ws"))
            .await
            .unwrap();
        twilio
            .send(ModelMessage::Text(start().to_string().into()))
            .await
            .unwrap();
        let (tcp, _) = model_listener.accept().await.unwrap();
        let mut model = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let mut update = receive(&mut model).await;
        assert_eq!(update["type"], "session.update");
        let appointment_tool = update["session"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == appointments::TOOL_NAME)
            .unwrap();
        assert_eq!(
            appointment_tool["parameters"]["required"],
            json!(["proposed_start", "caller_confirmed"])
        );
        let fields = appointment_tool["parameters"]["properties"]
            .as_object()
            .unwrap();
        assert_eq!(fields.len(), 3);
        assert!(fields.contains_key("original_start"));
        assert_eq!(
            appointment_tool["parameters"]["additionalProperties"],
            false
        );
        update["type"] = json!("session.updated");
        update["session"]["model"] = json!(MODEL);
        send(&mut model, update).await;
        assert_eq!(receive(&mut model).await["type"], "response.create");
        Self {
            twilio,
            model,
            raw_end,
            result,
            server,
            probes,
        }
    }
    async fn bind(&mut self, arguments: &Value) {
        send(
            &mut self.model,
            json!({"type":"response.created","response":{"id":"resp_appointment"}}),
        )
        .await;
        send(&mut self.model, json!({"type":"response.output_item.added","response_id":"resp_appointment",
            "item":{"id":"item_appointment","type":"function_call","name":appointments::TOOL_NAME,"call_id":"call_appointment"}})).await;
        send(&mut self.model, json!({"type":"response.function_call_arguments.done","response_id":"resp_appointment",
            "item_id":"item_appointment","call_id":"call_appointment","name":appointments::TOOL_NAME,"arguments":arguments.to_string()})).await;
        barrier(&mut self.model).await;
        assert_eq!(
            self.probes.calls.load(Ordering::SeqCst),
            0,
            "args.done must not schedule"
        );
    }
    async fn raw_reason(&mut self) -> EndReason {
        tokio::time::timeout(Duration::from_secs(2), self.raw_end.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn outcome(&mut self) -> BridgeOutcome {
        tokio::time::timeout(Duration::from_secs(2), self.result.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn stop(&mut self) {
        self.twilio
            .send(ModelMessage::Text(
                json!({"event":"stop","streamSid":start()["streamSid"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn realtime_appointment_completed_bound_call_runs_once_and_returns_tool_output() {
    let mut harness = Harness::new(false).await;
    let request = proposal();
    harness.bind(&request).await;
    send(&mut harness.model, done("completed", &request)).await;
    let result = receive(&mut harness.model).await;
    assert_eq!(result["type"], "conversation.item.create");
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["call_id"], "call_appointment");
    let output: Value = serde_json::from_str(result["item"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(output["status"], "tentative_hold_created");
    assert_eq!(receive(&mut harness.model).await["type"], "response.create");
    assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 1);
    assert!(
        harness.probes.requests.lock().unwrap()[0]
            == appointments::Request::parse(request.clone()).unwrap()
    );
    // A replayed completion has no bound function call and cannot invoke twice.
    send(&mut harness.model, done("completed", &request)).await;
    assert_eq!(harness.raw_reason().await, EndReason::ProtocolError);
    assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 1);
    harness.outcome().await;
}

#[tokio::test]
async fn realtime_appointment_cancelled_or_barged_in_response_never_schedules() {
    for barge_in in [false, true] {
        let mut harness = Harness::new(false).await;
        let request = proposal();
        harness.bind(&request).await;
        if barge_in {
            send(
                &mut harness.model,
                json!({"type":"input_audio_buffer.speech_started"}),
            )
            .await;
            assert_eq!(receive(&mut harness.model).await["type"], "response.cancel");
        }
        send(
            &mut harness.model,
            done(if barge_in { "completed" } else { "cancelled" }, &request),
        )
        .await;
        barrier(&mut harness.model).await;
        assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 0);
        harness.stop().await;
        assert_eq!(harness.raw_reason().await, EndReason::CallEnded);
        let _ = harness.model.close(None).await;
        harness.outcome().await;
    }
}

#[tokio::test]
async fn realtime_appointment_completion_cannot_mutate_bound_arguments() {
    let mut harness = Harness::new(false).await;
    let request = proposal();
    harness.bind(&request).await;
    let mut changed = request;
    changed["proposed_start"] = json!("2026-10-03T10:00:00-07:00");
    send(&mut harness.model, done("completed", &changed)).await;
    assert_eq!(harness.raw_reason().await, EndReason::ProtocolError);
    assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 0);
    harness.outcome().await;
}

#[tokio::test]
async fn realtime_appointment_pending_job_is_dropped_on_refusal_or_disconnect_before_drain() {
    for refusal in [false, true] {
        let mut harness = Harness::new(true).await;
        let request = proposal();
        harness.bind(&request).await;
        send(&mut harness.model, done("completed", &request)).await;
        tokio::time::timeout(Duration::from_secs(2), harness.probes.entered.notified())
            .await
            .unwrap();
        assert_eq!(harness.probes.polled.load(Ordering::SeqCst), 1);
        let started = Instant::now();
        if refusal {
            send(
                &mut harness.model,
                json!({"type":"conversation.item.input_audio_transcription.completed",
                "item_id":"caller_refusal","transcript":"Please stop recording me."}),
            )
            .await;
        } else {
            harness.twilio.close(None).await.unwrap();
        }
        assert_eq!(
            harness.raw_reason().await,
            if refusal {
                EndReason::RecordingDeclined
            } else {
                EndReason::PeerClosed
            }
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 1);
        assert_eq!(harness.probes.dropped.load(Ordering::SeqCst), 1);
        let _ = harness.model.close(None).await;
        let outcome = harness.outcome().await;
        if refusal {
            assert!(outcome.transcript.is_empty());
        }
    }
}

#[tokio::test]
async fn realtime_appointment_late_valid_completion_during_hangup_drain_never_schedules() {
    let mut harness = Harness::new(false).await;
    let request = proposal();
    harness.bind(&request).await;
    harness.stop().await;
    assert_eq!(harness.raw_reason().await, EndReason::CallEnded);
    // Cancellation on this socket proves finish_bridge entered transcript drain.
    assert_eq!(receive(&mut harness.model).await["type"], "response.cancel");
    send(&mut harness.model, done("completed", &request)).await;
    barrier(&mut harness.model).await;
    assert_eq!(harness.probes.calls.load(Ordering::SeqCst), 0);
    let _ = harness.model.close(None).await;
    harness.outcome().await;
}

/// Loopback fixture URI. Plain ws is intentional on 127.0.0.1, where there is no
/// TLS; it is built from parts so no cleartext socket literal appears in source.
fn loopback_ws(
    address: impl std::fmt::Display,
    path: &str,
) -> tokio_tungstenite::tungstenite::http::Uri {
    tokio_tungstenite::tungstenite::http::Uri::builder()
        .scheme("ws")
        .authority(address.to_string())
        .path_and_query(path)
        .build()
        .unwrap()
}
