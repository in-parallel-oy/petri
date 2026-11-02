//! Native sessions against a scripted model and real Petri execution scopes.

use std::collections::BTreeMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use attractor_steps::parallel::fenced_item;
use attractor_steps::pebble::PebbleClient;
use attractor_steps::pebble::environment::PebbleEnvironment;
use attractor_steps::skills::FabroHome;
use attractor_steps::{AgentPermissionCeiling, AgentPermissions, register};
use frontend::Diagnostics;
use frontend_attractor::RunSettings;
use frontend_attractor::mcps::{DEFAULT_TOOL_TIMEOUT_MS, McpServer, McpTransport};
use ir::{CancelScopeId, Graph, RunStatus, ScopeId};
use lithos_llm::types::{ContentPart, ReasoningEffort};
use pebble_coding_agent::environment::{Environment, ExecRequest};
use pebble_coding_agent::test_support::{
    EnvironmentContract, ScriptedCall, ScriptedProvider, client_from, message_text,
    scripted_client, text_response, tool_call_response,
};
use runtime::driver::{
    DeliverDisposition, EventObserver, ExecutionReport, ObserveError, RunHandle,
};
use runtime::engine::{EngineState, Event, EventRecord, ReplayMismatch};
use runtime::executor::sandbox::HostExecutor;
use runtime::executor::{AcquireContext, Executor, MapSecrets, Retention, ScopeOutcome, ScopeSpec};
use runtime::frontend::{CompileInputs, NoFiles};
use runtime::steps::{Answer, Question};
use runtime::{RunOptions, Runtime};
use serde_json::{Value, json};
use smol_str::SmolStr;
use testkit::{RunDir, output_of};
use tokio::fs;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

fn graph(extra: &str) -> Graph {
    let source = format!(
        r#"digraph T {{
        graph [default_model="test/model"]
        start [shape=Mdiamond]
        a [prompt="Make the change and verify it" {extra}]
        exit [shape=Msquare]
        start -> a -> exit
    }}"#
    );
    let lowered = frontend_attractor::load("test.fabro", &source, &NoFiles, &CompileInputs::new());
    lowered.graph.expect("valid workflow")
}

fn runtime(dir: &RunDir, client: lithos_llm::Client) -> Runtime {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(100);
    options.retention = Retention::Never;
    options.echo = false;
    register(Runtime::standard())
        .capability(PebbleClient(client))
        .options(options)
}

fn metrics(report: &ExecutionReport) -> &BTreeMap<SmolStr, Value> {
    &report
        .state
        .history()
        .iter()
        .find(|row| row.name == "a")
        .expect("agent outcome")
        .outcome
        .metrics
        .custom
}

#[tokio::test]
async fn environment_meets_the_pebble_contract() {
    let dir = RunDir::new("pebble-contract");
    let executor = HostExecutor::new(dir.path());
    let scope = ScopeSpec::new(ScopeId::new(1), "pebble").with_grace(Duration::from_millis(50));
    let handle = executor
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect("scope");
    let environment = PebbleEnvironment::prepare(
        handle.exec(),
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .expect("prepare");
    let contract = EnvironmentContract::new(&environment, "contract");
    contract.verify_files().await.expect("files");
    contract.verify_search().await.expect("search");
    contract.verify_commands().await.expect("commands");
    let path = "odd 'name; $(touch injected)\nfile";
    environment
        .write_file(path, "safe")
        .await
        .expect("write odd path");
    environment
        .rename_file(path, "moved 'name")
        .await
        .expect("move odd path");
    assert_eq!(
        environment
            .read_file_text("moved 'name")
            .await
            .expect("read"),
        "safe"
    );
    assert!(!environment.file_exists("injected").await.expect("exists"));
    assert!(
        executor
            .release(handle, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
}

#[tokio::test]
async fn native_tools_edit_and_verify_in_the_scope() {
    let dir = RunDir::new("pebble-edit-test");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "write_file",
            "write",
            json!({"path":"answer.txt","content":"42\n"}),
        )),
        ScriptedCall::response(tool_call_response(
            "shell",
            "verify",
            json!({"command":"test \"$(cat answer.txt)\" = 42 && printf verified"}),
        )),
        ScriptedCall::response(text_response("Done and verified.")),
    ]);
    let report = runtime(&dir, client).run(graph("")).await.expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], "Done and verified.");
    assert_eq!(provider.requests().len(), 3);
    let requests = serde_json::to_string(&provider.requests()).expect("requests");
    assert!(requests.contains("verified"), "{requests}");
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 30);
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["output"], 15);
    // One route, so one entry: all of the session's usage, on its route.
    assert_eq!(
        metrics(&report)["pebble.usage_by_model"],
        json!([{
            "provider": "test",
            "model": "model",
            "usage": metrics(&report)["pebble.usage"],
        }])
    );
}

/// The node's step config, as the lowering wrote it.
fn node_config<'a>(graph: &'a mut Graph, name: &str) -> &'a mut Value {
    &mut graph
        .body
        .nodes
        .iter_mut()
        .find(|n| n.name == name)
        .unwrap_or_else(|| panic!("node `{name}`"))
        .step
        .config
}

#[tokio::test]
async fn tool_shells_see_workflow_secrets_beneath_their_own_env() {
    let dir = RunDir::new("pebble-workflow-secrets");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "shell",
            "check",
            json!({"command":"test \"$AGENT_KEY\" = workflow-secret-value && echo $((6 * 7))-seen"}),
        )),
        ScriptedCall::response(text_response("Checked.")),
    ]);
    let mut graph = graph("");
    node_config(&mut graph, "a")["env"] = json!({"AGENT_KEY": {"$secret": "WORKFLOW_KEY"}});
    let secrets = MapSecrets::from_pairs(&[("WORKFLOW_KEY", "workflow-secret-value")]);
    let report = runtime(&dir, client)
        .secrets(secrets)
        .run(graph)
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let requests = serde_json::to_string(&provider.requests()).expect("requests");
    assert!(requests.contains("42-seen"), "{requests}");
}

#[tokio::test]
async fn a_workflow_secret_the_run_cannot_supply_fails_the_native_node() {
    let dir = RunDir::new("pebble-missing-secret");
    let (client, _provider) =
        scripted_client(vec![ScriptedCall::response(text_response("Never asked."))]);
    let mut graph = graph("");
    node_config(&mut graph, "a")["env"] = json!({"AGENT_KEY": {"$secret": "NOT_THERE"}});
    let report = runtime(&dir, client).run(graph).await.expect("replay");
    // The node fails before the session opens; this graph routes the failure
    // on, so the node's outcome is what says so.
    let output = output_of(&report, "a");
    assert_eq!(output["outcome"], json!("failed"));
    assert_eq!(output["failure_class"], json!("secret_unavailable"));
    assert!(
        output["failure_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("NOT_THERE")),
        "{output}"
    );
}

#[tokio::test]
async fn repairs_share_history_and_sum_accounting() {
    let dir = RunDir::new("pebble-repair");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(text_response("not JSON")),
        ScriptedCall::response(text_response(r#"{"value":42}"#)),
    ]);
    let report = runtime(&dir, client).run(graph(r#", output_schema="{\"type\":\"object\",\"required\":[\"value\"],\"properties\":{\"value\":{\"type\":\"integer\"}}}""#)).await.expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["structured"]["value"], 42);
    assert_eq!(metrics(&report)["pebble.prompts"], 2);
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 20);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        serde_json::to_string(&requests[1])
            .expect("request")
            .contains("not JSON")
    );
}

#[tokio::test]
async fn invalid_output_keeps_accounting() {
    let dir = RunDir::new("pebble-bad-output");
    let (client, _) = scripted_client(vec![ScriptedCall::response(text_response("not JSON"))]);
    let report = runtime(&dir, client)
        .run(graph(r#", output_schema="routing", output_retries=1"#))
        .await
        .expect("replay");
    assert_eq!(testkit::status_of(&report, "a").as_deref(), Some("failure"));
    assert_eq!(output_of(&report, "a")["failure_class"], "bad_output");
    assert_eq!(metrics(&report)["pebble.prompts"], 2);
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 20);
}

#[tokio::test]
async fn cancellation_settles_the_prompt_and_preserves_usage() {
    let dir = RunDir::new("pebble-cancel");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "shell",
            "work",
            json!({"command":"printf ready"}),
        )),
        ScriptedCall::PendingOpen,
    ]);
    let rt = runtime(&dir, client);
    let driver = rt.driver(graph(""));
    let handle = driver.handle();
    let run = tokio::spawn(driver.run());
    timeout(Duration::from_secs(15), async {
        provider.wait_for_call().await;
        provider.wait_for_call().await;
    })
    .await
    .expect("second call");
    handle.cancel(CancelScopeId::ROOT).await;
    let report = timeout(Duration::from_secs(10), run)
        .await
        .expect("cancel settles")
        .expect("run task");
    assert_ne!(report.status, RunStatus::Success);
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 10);
    assert_eq!(metrics(&report)["pebble.prompts"], 1);
}

#[tokio::test]
async fn oversized_process_output_keeps_bounded_head_and_tail() {
    let dir = RunDir::new("pebble-output-capture");
    let executor = HostExecutor::new(dir.path());
    let scope = ScopeSpec::new(ScopeId::new(1), "pebble").with_grace(Duration::from_millis(50));
    let handle = executor
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect("scope");
    let environment = PebbleEnvironment::prepare(
        handle.exec(),
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .expect("prepare");
    let outcome = environment.exec(ExecRequest { output_bytes_cap: Some(12), ..ExecRequest::new("printf 'HEAD\\r\\n'; printf '%0200000d' 0; printf '\\000\\377TAIL'; printf 'err\\r\\n\\000\\377' >&2") }).await.expect("capture");
    assert_eq!(outcome.stdout_capture.observed_bytes, 200_012);
    assert_eq!(outcome.stdout_capture.retained_bytes, 12);
    assert_eq!(outcome.stdout_capture.omitted_bytes, 200_000);
    assert_eq!(outcome.result.stdout, "HEAD\r\n\0\u{fffd}TAIL");
    assert_eq!(outcome.result.stderr, "err\r\n\0\u{fffd}");
    assert_eq!(outcome.stderr_capture.observed_bytes, 7);
    assert_eq!(outcome.stderr_capture.retained_bytes, 7);
    assert_eq!(outcome.stderr_capture.omitted_bytes, 0);
    assert!(outcome.result.is_success(), "truncation is not a failure");
    assert!(
        executor
            .release(handle, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
}

#[tokio::test]
async fn model_failure_keeps_prior_usage_and_known_cost() {
    use lithos_llm::types::{Cost, CostSource, ErrorKind};
    use pebble_coding_agent::test_support::ScriptedFailure;
    let dir = RunDir::new("pebble-model-error");
    let mut first = tool_call_response("shell", "work", json!({"command":"true"}));
    first.cost = Some(Cost {
        usd_micros: 123,
        source:     CostSource::Provider,
    });
    let (client, _) = scripted_client(vec![
        ScriptedCall::response(first),
        ScriptedCall::Failure(ScriptedFailure::terminal(
            ErrorKind::Authentication,
            "test failure",
        )),
    ]);
    let report = runtime(&dir, client).run(graph("")).await.expect("replay");
    assert_eq!(
        output_of(&report, "a")["failure_class"],
        "llm:authentication"
    );
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 10);
    assert_eq!(
        metrics(&report)["pebble.usage"]["cost"],
        json!({"usd_micros": 123, "source": "provider"})
    );
}

#[test]
fn backend_selection_inherits_and_accepts_stylesheets() {
    for source in [
        r#"digraph T { start [shape=Mdiamond]; a [backend="api", prompt="hello"]; exit [shape=Msquare]; start -> a -> exit; }"#,
        r#"digraph T { graph [backend="api"]; start [shape=Mdiamond]; a [prompt="hello"]; exit [shape=Msquare]; start -> a -> exit; }"#,
        r#"digraph T { graph [model_stylesheet="* { backend: api; }"]; start [shape=Mdiamond]; a [prompt="hello"]; exit [shape=Msquare]; start -> a -> exit; }"#,
    ] {
        let result =
            frontend_attractor::load("test.fabro", source, &NoFiles, &CompileInputs::new());
        assert!(
            result.diagnostics.iter().next().is_none(),
            "backend selection must not warn: {:?}",
            result.diagnostics
        );
        let graph = result
            .graph
            .unwrap_or_else(|| panic!("lowering failed: {:?}", result.diagnostics));
        let node = graph
            .body
            .nodes
            .iter()
            .find(|node| node.name == "a")
            .expect("agent");
        assert_eq!(node.step.config["backend"], "api");
    }
    let invalid = frontend_attractor::load(
        "test.fabro",
        r#"digraph T { start [shape=Mdiamond]; a [backend="invalid"]; exit [shape=Msquare]; start -> a -> exit; }"#,
        &NoFiles,
        &CompileInputs::new(),
    );
    assert!(invalid.graph.is_none());
    assert!(
        invalid
            .diagnostics
            .iter()
            .any(|d| d.code == "attractor.bad_backend")
    );
}

struct NativeStarted(mpsc::Sender<ir::FiringId>);
impl EventObserver for NativeStarted {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, _: &EngineState) {
        if let Event::StepProgressRecorded {
            firing,
            ev: ir::StepEvent::Custom(value),
        } = &record.event
            && value["kind"] == "pebble"
        {
            let _ = self.0.try_send(*firing);
        }
    }
}

#[tokio::test]
async fn steering_and_attributed_events_reach_the_native_session() {
    let dir = RunDir::new("pebble-steer");
    let gate = dir.path().join("release");
    let command = format!("while [[ ! -f '{}' ]]; do sleep 0.01; done", gate.display());
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "shell",
            "gate",
            json!({"command":command}),
        )),
        ScriptedCall::response(text_response("original answer")),
        ScriptedCall::response(text_response("steered answer")),
    ]);
    let (send, mut receive) = mpsc::channel(1);
    let rt = runtime(&dir, client).observe(Arc::new(NativeStarted(send)));
    let driver = rt.driver(graph(""));
    let handle = driver.handle();
    let run = tokio::spawn(driver.run());
    let firing = timeout(Duration::from_secs(10), receive.recv())
        .await
        .expect("native event")
        .expect("firing");
    timeout(Duration::from_secs(10), provider.wait_for_call())
        .await
        .expect("first model call");
    assert_eq!(
        handle
            .deliver(
                firing,
                ir::Control::Deliver(json!({"text":"Check the edge cases too"}))
            )
            .await,
        DeliverDisposition::Delivered
    );
    fs::write(gate, "go").await.expect("release tool");
    let report = timeout(Duration::from_secs(15), run)
        .await
        .expect("run settles")
        .expect("task");
    assert_eq!(output_of(&report, "a")["text"], "steered answer");
    let requests = provider.requests();
    assert!(
        serde_json::to_string(&requests.last())
            .expect("request")
            .contains("Check the edge cases too")
    );
    let events: Vec<_> = report
        .state
        .log
        .events()
        .filter_map(|event| match event {
            Event::StepProgressRecorded {
                ev: ir::StepEvent::Custom(value),
                ..
            } if value["kind"] == "pebble" => Some(value),
            _ => None,
        })
        .collect();
    assert!(!events.is_empty());
    let stream = &events[0]["event"]["stream_id"];
    for (index, envelope) in events.iter().enumerate() {
        assert_eq!(envelope["node"], "a");
        assert_eq!(envelope["firing"], json!(firing));
        assert!(!envelope["attempt"].is_null());
        assert_eq!(envelope["event"]["seq"], json!(index + 1));
        assert_eq!(&envelope["event"]["stream_id"], stream);
        assert_eq!(&envelope["event"]["session_id"], stream);
    }
    assert!(
        events
            .iter()
            .any(|event| event["event"]["event"].get("ToolCallCompleted").is_some())
    );
}

/// Reports the firing of node `a` once its step has started: the native
/// session's build begins at once and, here, waits on an MCP server that
/// never answers.
struct BuildStarted(mpsc::Sender<ir::FiringId>);
impl EventObserver for BuildStarted {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, state: &EngineState) {
        if let Event::StepStarted { firing, .. } = &record.event
            && state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .is_some_and(|node| node.name == "a")
        {
            let _ = self.0.try_send(*firing);
        }
    }
}

/// A delivery that arrives while the session is still being built waits on
/// the node run's steering bus and reaches the session when it attaches, in
/// the mode it was sent: a follow-up, which runs as its own turn once the
/// first answer is reached. Here Pebble holds the build on an MCP server
/// that never completes its handshake; the node proceeds without it.
#[tokio::test]
async fn a_delivery_before_the_session_is_built_runs_as_a_follow_up() {
    let dir = RunDir::new("pebble-deliver-before-build");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(text_response("original answer")),
        ScriptedCall::response(text_response("follow-up answer")),
    ]);
    // One stdio server whose handshake never completes within its startup
    // timeout, as the Fabro frontend resolves `[run.agent.mcps.slow]` with
    // `command = ["sleep", "5"]` and `startup_timeout = "1s"`.
    let settings = RunSettings {
        mcps: vec![McpServer {
            name:               "slow".to_owned(),
            transport:          McpTransport::Stdio {
                command: vec!["sleep".to_owned(), "5".to_owned()],
                env:     BTreeMap::new(),
            },
            startup_timeout_ms: 1_000,
            tool_timeout_ms:    DEFAULT_TOOL_TIMEOUT_MS,
            source:             "workflow.toml".to_owned(),
        }],
        ..RunSettings::default()
    };
    let lowered = frontend_attractor::lower(
        "wf/w.fabro",
        r#"digraph T {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        a [prompt="Make the change and verify it"]
        exit [shape=Msquare]
        start -> a -> exit
    }"#,
        &NoFiles,
        &CompileInputs::new(),
        settings,
        Diagnostics::new(),
    );
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("valid workflow");
    let (send, mut receive) = mpsc::channel(1);
    let rt = runtime(&dir, client).observe(Arc::new(BuildStarted(send)));
    let driver = rt.driver(graph);
    let handle = driver.handle();
    let run = tokio::spawn(driver.run());
    let firing = timeout(Duration::from_secs(10), receive.recv())
        .await
        .expect("the step starts")
        .expect("firing");
    // The step is started before its control channel is live; the build
    // holds for the server's startup timeout, so a short retry lands the
    // delivery well inside it.
    let mut disposition = DeliverDisposition::NotLive;
    for _ in 0..50 {
        disposition = handle
            .deliver(
                firing,
                ir::Control::Deliver(json!({"text":"Also check the docs"})),
            )
            .await;
        if disposition == DeliverDisposition::Delivered {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(disposition, DeliverDisposition::Delivered);
    let report = timeout(Duration::from_secs(30), run)
        .await
        .expect("run settles")
        .expect("task");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], "follow-up answer");
    let requests: Vec<String> = provider
        .requests()
        .iter()
        .map(|request| serde_json::to_string(request).expect("request"))
        .collect();
    assert_eq!(requests.len(), 2, "the prompt, then the follow-up turn");
    assert!(
        !requests[0].contains("Also check the docs"),
        "the first turn is the prompt alone: {}",
        requests[0]
    );
    assert!(
        requests[1].contains("Also check the docs"),
        "the follow-up ran after the first answer: {}",
        requests[1]
    );
}

/// A durable store that refuses every write.
struct RefusingStore;

#[async_trait::async_trait]
impl EventObserver for RefusingStore {
    fn on_record(&self, _: &EventRecord, _recorded_at: u64, _: &EngineState) {}

    async fn durable(&self, _seq: u64) -> Result<(), ObserveError> {
        Err(ObserveError::new("refusing-store", "the write failed"))
    }
}

/// Pebble's events are recorded acknowledged: a store that cannot write them
/// fails the acknowledgement, and the session stops with that failure — here
/// at its first event, before any model request — instead of running on with
/// nothing recording it. The stage's failure names the store and the cause.
#[tokio::test]
async fn a_durable_store_failure_stops_the_native_session() {
    let dir = RunDir::new("pebble-durable-failure");
    let (client, provider) = scripted_client(vec![ScriptedCall::response(text_response(
        "never recorded",
    ))]);
    let report = runtime(&dir, client)
        .observe(Arc::new(RefusingStore))
        .run(graph(""))
        .await
        .expect("replay");
    let failure = report
        .state
        .history()
        .iter()
        .find(|row| row.name == "a")
        .and_then(|row| row.outcome.status.failure_info().cloned())
        .expect("the agent node failed");
    assert_eq!(failure.class.as_str(), "pebble_config");
    assert!(
        failure.message.contains("did not reach durable storage")
            && failure.message.contains("refusing-store: the write failed"),
        "the store's failure is the node's: {}",
        failure.message
    );
    assert!(
        provider.requests().is_empty(),
        "the session stopped before its first model request"
    );
}

#[tokio::test]
async fn kill_stops_a_tool_that_ignores_term() {
    let dir = RunDir::new("pebble-kill");
    let marker = dir.path().join("started");
    let command = format!(
        "trap '' TERM; echo $$ > '{}'; while :; do :; done",
        marker.display()
    );
    let (client, _) = scripted_client(vec![ScriptedCall::response(tool_call_response(
        "shell",
        "busy",
        json!({"command":command}),
    ))]);
    let (send, mut receive) = mpsc::channel(1);
    let rt = runtime(&dir, client).observe(Arc::new(NativeStarted(send)));
    let driver = rt.driver(graph(""));
    let handle = driver.handle();
    let run = tokio::spawn(driver.run());
    let firing = timeout(Duration::from_secs(10), receive.recv())
        .await
        .expect("native event")
        .expect("firing");
    assert!(testkit::wait_for_file(&marker, Duration::from_secs(10)).await);
    let pid = fs::read_to_string(&marker).await.expect("pid");
    let _ = firing;
    handle.cancel(CancelScopeId::ROOT).await;
    handle.cancel(CancelScopeId::ROOT).await;
    let report = timeout(Duration::from_secs(5), run)
        .await
        .expect("kill settles")
        .expect("task");
    assert_eq!(
        testkit::status_of(&report, "a").as_deref(),
        Some("cancelled")
    );
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 10);
    let status = Command::new("kill")
        .args(["-0", pid.trim()])
        .stderr(Stdio::null())
        .status()
        .await
        .expect("pid probe");
    assert!(!status.success(), "the tool process was reaped");
}

#[tokio::test]
async fn node_settings_select_the_actual_model_and_reasoning() {
    let dir = RunDir::new("pebble-model-settings");
    let (client, provider) = scripted_client(vec![ScriptedCall::response(text_response("done"))]);
    let report = runtime(&dir, client)
        .run(graph(
            r#", model="thinking", provider="test", reasoning_effort="high""#,
        ))
        .await
        .expect("replay");
    assert_eq!(testkit::status_of(&report, "a").as_deref(), Some("success"));
    let requests = provider.requests();
    assert_eq!(requests[0].model(), "test/thinking");
    assert_eq!(requests[0].reasoning_effort(), Some(ReasoningEffort::High));
}

#[tokio::test(flavor = "multi_thread")]
async fn environment_contract_runs_inside_a_container_without_host_files() {
    use runtime::executor::sandbox::RoutingExecutor;
    if !testkit::is_docker_ready().await {
        return;
    }
    let dir = RunDir::new("pebble-docker-contract");
    let executor = RoutingExecutor::local(dir.path(), Retention::Never);
    let scope = ScopeSpec::new(ScopeId::new(1), "pebble-docker")
        .with_grace(Duration::from_millis(100))
        .with_runtime(ir::RuntimeSpec::container("buildpack-deps:noble"));
    let handle = executor
        .acquire(&scope, &AcquireContext::bare())
        .await
        .expect("container");
    assert!(!handle.exec().shares_host_filesystem());
    let environment = PebbleEnvironment::prepare(
        handle.exec(),
        CancellationToken::new(),
        CancellationToken::new(),
    )
    .await
    .expect("prepare container");
    assert_eq!(environment.platform(), "linux");
    let contract = EnvironmentContract::new(&environment, "contract")
        .with_operation_timeout(Duration::from_secs(30));
    contract.verify_files().await.expect("container files");
    contract
        .verify_search()
        .await
        .expect("container search (grep fallback)");
    contract
        .verify_commands()
        .await
        .expect("container commands");
    assert!(
        executor
            .release(handle, ScopeOutcome::Succeeded)
            .await
            .is_clean()
    );
}

#[test]
fn mixed_backends_inherit_only_their_own_configuration() {
    let source = r#"digraph T {
        graph [acp.command="agent-command", default_model="test/model"]
        start [shape=Mdiamond]
        native [backend="api", prompt="native"]
        implicit [prompt="native by default"]
        external [backend="acp", prompt="ACP"]
        exit [shape=Msquare]
        start -> native -> implicit -> external -> exit
    }"#;
    let result = frontend_attractor::load("mixed.fabro", source, &NoFiles, &CompileInputs::new());
    let graph = result.graph.expect("mixed workflow");
    let native = graph
        .body
        .nodes
        .iter()
        .find(|node| node.name == "native")
        .expect("native");
    let external = graph
        .body
        .nodes
        .iter()
        .find(|node| node.name == "external")
        .expect("external");
    assert_eq!(native.step.config["backend"], "api");
    assert!(native.step.config.get("acp").is_none());
    let implicit = graph
        .body
        .nodes
        .iter()
        .find(|node| node.name == "implicit")
        .expect("implicit");
    assert!(implicit.step.config.get("acp").is_none());
    assert_eq!(external.step.config["backend"], "acp");
    assert_eq!(external.step.config["acp"]["command"], "agent-command");
}

/// Answers the first core `Question` it sees with `answer`, through the run
/// handle: the smallest stand-in for the host's interview dispatcher.
struct GateAnswerer {
    handle: Mutex<Option<RunHandle>>,
    answer: Answer,
    asked:  Mutex<Vec<Question>>,
}

impl EventObserver for GateAnswerer {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, _state: &EngineState) {
        let Event::StepProgressRecorded { firing, ev } = &record.event else {
            return;
        };
        let Some(question) = Question::from_event(ev) else {
            return;
        };
        self.asked
            .lock()
            .expect("not poisoned")
            .push(question.clone());
        let handle = self
            .handle
            .lock()
            .expect("not poisoned")
            .clone()
            .expect("wired");
        let answer = self.answer.clone().for_question(&question.id);
        let firing = *firing;
        tokio::spawn(async move {
            handle.deliver(firing, answer.to_control()).await;
        });
    }
}

#[tokio::test]
async fn an_agent_question_rides_the_core_question_protocol() {
    let dir = RunDir::new("pebble-question");
    // The test catalog's model runs the Anthropic harness, whose question
    // tool is `AskUserQuestion`; the options become `option_1`, `option_2`.
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "AskUserQuestion",
            "ask",
            json!({"questions":[{"question":"Which file?","header":"File","options":[{"label":"README"},{"label":"CHANGELOG"}],"multiSelect":false}]}),
        )),
        ScriptedCall::response(text_response("Editing CHANGELOG.")),
    ]);
    let rt = runtime(&dir, client);
    let answerer = Arc::new(GateAnswerer {
        handle: Mutex::new(None),
        answer: Answer::choice("option_2"),
        asked:  Mutex::new(Vec::new()),
    });
    let graph = graph("");
    let driver = rt.driver(graph.clone()).observe(answerer.clone());
    *answerer.handle.lock().expect("not poisoned") = Some(driver.handle());
    let report = rt
        .run_verified(graph, |_| Ok::<_, ReplayMismatch>(driver))
        .await
        .expect("replay is byte-identical");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], "Editing CHANGELOG.");
    let asked = answerer.asked.lock().expect("not poisoned").clone();
    assert_eq!(asked.len(), 1);
    assert!(asked[0].id.starts_with("a#"), "{}", asked[0].id);
    assert!(asked[0].id.contains("/agent/"), "{}", asked[0].id);
    assert!(asked[0].id.ends_with("/ask/0"), "{}", asked[0].id);
    assert_eq!(asked[0].kind.as_deref(), Some("multiple_choice"));
    assert_eq!(asked[0].options.len(), 2);
    assert_eq!(asked[0].options[1].key, "option_2");
    assert!(asked[0].freeform);
    let requests = serde_json::to_string(&provider.requests()).expect("requests");
    assert!(
        requests.contains("option_2"),
        "the answer reaches the next model request: {requests}"
    );
}

/// A native agent attempt is `ExecutorEnforced`: the driver owns the
/// deadline and cancels Pebble through the prompt's cancellation token.
/// Pebble's own wall-clock timer is left unset, so the attempt reports
/// `timed_out` from the driver, with the settled prompt's usage kept.
#[tokio::test]
async fn the_driver_deadline_cancels_a_native_attempt_through_its_token() {
    let dir = RunDir::new("pebble-deadline");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "shell",
            "work",
            json!({"command":"printf ready"}),
        )),
        ScriptedCall::PendingOpen,
    ]);
    let rt = runtime(&dir, client);
    let graph = graph(r#", timeout="1s""#);
    assert_eq!(
        graph
            .nodes
            .iter()
            .find(|n| n.name == "a")
            .expect("a")
            .budget
            .timeout_policy,
        ir::TimeoutPolicy::ExecutorEnforced
    );
    let driver = rt.driver(graph);
    let run = tokio::spawn(driver.run());
    timeout(Duration::from_secs(15), async {
        provider.wait_for_call().await;
        provider.wait_for_call().await;
    })
    .await
    .expect("second call");
    let report = timeout(Duration::from_secs(15), run)
        .await
        .expect("the deadline settles the attempt")
        .expect("run task");
    assert_eq!(
        testkit::status_of(&report, "a").as_deref(),
        Some("timed_out")
    );
    assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 10);
    assert_eq!(metrics(&report)["pebble.prompts"], 1);
}

/// A template without a placeholder would discard the entire assembled
/// workflow input if Petri failed to keep the authored segment separate.
async fn commit_skill(dir: &RunDir, graph: &mut Graph, node: &str) {
    let skills = dir.path().join("skill-fixtures");
    fs::create_dir_all(skills.join("commit"))
        .await
        .expect("skill directory");
    fs::write(
        skills.join("commit/SKILL.md"),
        "---\nname: commit\ndescription: Test commit instructions\n---\nCOMMIT TEMPLATE SENTINEL\n",
    )
    .await
    .expect("skill template");
    node_config(graph, node)["skill_dirs"] = json!([skills]);
}

/// A runtime whose Fabro home is isolated, so the developer's own skills
/// stay out of the session.
fn skill_runtime(dir: &RunDir, client: lithos_llm::Client) -> Runtime {
    runtime(dir, client).capability(FabroHome(dir.path().join("fabro-home")))
}

fn activated_skills(report: &ExecutionReport, node: &str) -> Vec<Value> {
    report
        .state
        .log
        .events()
        .filter_map(|event| match event {
            Event::StepProgressRecorded {
                ev: ir::StepEvent::Custom(value),
                ..
            } if value["kind"] == "pebble" && value["node"] == node => {
                value["event"]["event"].get("SkillActivated").cloned()
            }
            _ => None,
        })
        .collect()
}

/// The input a node sends after a prior stage, with the item, and the skills
/// it activated. The workflow context must survive whatever the authored
/// prompt expands to.
async fn native_skill_turn(prior: &str, authored: &str) -> (String, String, Vec<Value>) {
    let dir = RunDir::new("pebble-skill-context");
    let source = r#"digraph T {
        graph [backend="api", default_model="test/model", goal="Keep workflow context"]
        start [shape=Mdiamond]
        prior [prompt="Report the previous result"]
        a [prompt="placeholder", fidelity="summary:high"]
        exit [shape=Msquare]
        start -> prior -> a -> exit
    }"#;
    let mut graph = frontend_attractor::load("test.fabro", source, &NoFiles, &CompileInputs::new())
        .graph
        .expect("valid workflow");
    commit_skill(&dir, &mut graph, "a").await;
    let config = node_config(&mut graph, "a");
    config["prompt"] = json!(authored);
    // This is the fenced value the for_each branch hands to its target.
    let item = fenced_item(&json!("/commit item /tmp"));
    config["item_data"] = json!(item);
    config["output_schema"] = json!({"type":"object", "description":"Contract /commit /tmp"});
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(text_response(prior)),
        ScriptedCall::response(text_response("{}")),
    ]);
    let report = skill_runtime(&dir, client)
        .run(graph)
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let sent = message_text(requests[1].messages().last().expect("user message"));
    assert!(sent.contains(prior), "prior context remains: {sent}");
    assert!(
        sent.contains("Keep workflow context"),
        "goal remains: {sent}"
    );
    assert!(sent.contains(&item), "item remains: {sent}");
    assert!(
        sent.contains("Fabro final-output contract"),
        "contract remains: {sent}"
    );
    assert!(
        sent.contains("Contract /commit /tmp"),
        "schema remains: {sent}"
    );
    (sent, item, activated_skills(&report, "a"))
}

// Regression scenarios reported by denkhaus in fabro-sh/fabro#784.
async fn check_no_expansion(prior: &str, authored: &str) {
    let (sent, _, activations) = native_skill_turn(prior, authored).await;
    assert!(sent.contains(authored), "authored task remains: {sent}");
    assert!(
        !sent.contains("COMMIT TEMPLATE SENTINEL"),
        "no expansion: {sent}"
    );
    assert!(activations.is_empty(), "{activations:?}");
}

#[tokio::test]
async fn native_skill_expansion_ignores_paths_in_prior_context() {
    check_no_expansion(
        "smoke-tested via a /tmp build",
        "Make the change and verify it",
    )
    .await;
}

#[tokio::test]
async fn native_skill_expansion_ignores_echoed_failures() {
    check_no_expansion(
        "previous stage failed: unknown skill: /tmp",
        "Recover the failed stage",
    )
    .await;
}

#[tokio::test]
async fn native_skill_expansion_ignores_known_commands_in_feedback() {
    check_no_expansion(
        "Reviewer feedback mentions /commit",
        "Address the reviewer feedback",
    )
    .await;
}

#[tokio::test]
async fn native_skill_expansion_preserves_context_around_authored_commands() {
    let prior = "Prior stage result /tmp";
    let authored = "  /commit authored arguments /tmp";
    let (sent, item, activations) = native_skill_turn(prior, authored).await;
    assert!(
        sent.contains("COMMIT TEMPLATE SENTINEL"),
        "template expanded: {sent}"
    );
    assert!(
        !sent.contains(authored),
        "template replaces only authored input: {sent}"
    );
    assert_eq!(activations, vec![
        json!({"skill_name":"commit", "source":"slash"})
    ]);
    assert!(sent.find(prior) < sent.find("COMMIT TEMPLATE SENTINEL"));
    assert!(sent.find("COMMIT TEMPLATE SENTINEL") < sent.find(&item));
}

#[tokio::test]
async fn generated_repairs_preserve_slash_tokens_without_activating_skills() {
    let dir = RunDir::new("pebble-skill-repair");
    let mut graph = graph("");
    commit_skill(&dir, &mut graph, "a").await;
    node_config(&mut graph, "a")["output_schema"] = json!({
        "type":"object", "properties":{"answer":{"type":"integer"}}, "required":["answer"]
    });
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(text_response(r#"{"answer":"/commit unknown skill: /tmp"}"#)),
        ScriptedCall::response(text_response(r#"{"answer":42}"#)),
    ]);
    let report = skill_runtime(&dir, client)
        .run(graph)
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let repair = message_text(requests[1].messages().last().expect("repair"));
    assert!(
        repair.contains("Your previous response did not satisfy the output contract"),
        "{repair}"
    );
    assert!(repair.contains("/commit unknown skill: /tmp"), "{repair}");
    assert!(activated_skills(&report, "a").is_empty());
}

#[tokio::test]
async fn delivered_follow_ups_expand_only_intentional_leading_commands() {
    for (follow_up, activates) in [
        ("Check /commit and /tmp as literal examples", false),
        ("  /commit follow-up arguments /tmp", true),
    ] {
        let dir = RunDir::new("pebble-skill-follow-up");
        let gate = dir.path().join("release");
        let command = format!("while [[ ! -f '{}' ]]; do sleep 0.01; done", gate.display());
        let (client, provider) = scripted_client(vec![
            ScriptedCall::response(tool_call_response(
                "shell",
                "gate",
                json!({"command":command}),
            )),
            ScriptedCall::response(text_response("original answer")),
            ScriptedCall::response(text_response("follow-up answer")),
        ]);
        let mut graph = graph("");
        commit_skill(&dir, &mut graph, "a").await;
        let (send, mut receive) = mpsc::channel(1);
        let driver = skill_runtime(&dir, client)
            .observe(Arc::new(NativeStarted(send)))
            .driver(graph);
        let handle = driver.handle();
        let run = tokio::spawn(driver.run());
        let firing = timeout(Duration::from_secs(10), receive.recv())
            .await
            .expect("native event")
            .expect("firing");
        timeout(Duration::from_secs(10), provider.wait_for_call())
            .await
            .expect("first model call");
        assert_eq!(
            handle
                .deliver(firing, ir::Control::Deliver(json!({"text":follow_up})))
                .await,
            DeliverDisposition::Delivered
        );
        fs::write(gate, "go").await.expect("release tool");
        let report = timeout(Duration::from_secs(15), run)
            .await
            .expect("run settles")
            .expect("task");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.errors()
        );
        assert_eq!(output_of(&report, "a")["text"], "follow-up answer");
        let requests = provider.requests();
        assert_eq!(requests.len(), 3);
        let sent = message_text(requests[2].messages().last().expect("follow-up input"));
        if activates {
            assert!(sent.contains("COMMIT TEMPLATE SENTINEL"), "{sent}");
            assert_eq!(activated_skills(&report, "a"), vec![
                json!({"skill_name":"commit", "source":"slash"})
            ]);
        } else {
            assert_eq!(sent, follow_up);
            assert!(activated_skills(&report, "a").is_empty());
        }
    }
}

/// Empty context is absence, not an empty provider text block. In
/// particular, Anthropic rejects such blocks after a cross-provider fallback.
#[tokio::test]
async fn native_prompts_omit_absent_context_parts() {
    for (fidelity, authored) in [
        ("compact", "Make the change and verify it"),
        ("full", "Make the change and verify it"),
        ("compact", ""),
    ] {
        let dir = RunDir::new("pebble-absent-context");
        let (client, provider) =
            scripted_client(vec![ScriptedCall::response(text_response("done"))]);
        let mut graph = graph(&format!(r#", fidelity="{fidelity}""#));
        node_config(&mut graph, "a")["prompt"] = json!(authored);
        let report = runtime(&dir, client).run(graph).await.expect("replay");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.errors()
        );
        let requests = provider.requests();
        assert_eq!(requests.len(), 1);
        let sent = requests[0].messages().last().expect("user input");
        assert!(
            sent.content()
                .iter()
                .all(|part| !matches!(part, ContentPart::Text { text } if text.is_empty())),
            "{fidelity} must not emit empty context blocks: {sent:?}"
        );
        let text = message_text(sent);
        assert!(text.contains(authored));
        if fidelity == "full" {
            assert_eq!(text, authored);
        } else {
            assert!(
                text.contains("Goal:"),
                "the nonempty preamble remains: {text}"
            );
        }
    }
}

/// A backend omitted from the source dispatches to native API and must
/// enforce the same attempt budget as explicitly selecting that backend.
#[tokio::test]
async fn default_and_explicit_native_agents_reject_a_response_after_the_deadline() {
    for backend in ["", r#", backend="api""#] {
        let dir = RunDir::new("pebble-delayed-deadline");
        let (client, provider) = client_from(
            ScriptedProvider::new(vec![ScriptedCall::response(text_response("too late"))])
                .delayed(Duration::from_secs(3)),
        );
        let report = timeout(
            Duration::from_secs(15),
            runtime(&dir, client).run(graph(&format!(r#", timeout="1s"{backend}"#))),
        )
        .await
        .expect("the attempt settles")
        .expect("replay");
        assert_eq!(
            testkit::status_of(&report, "a").as_deref(),
            Some("timed_out"),
            "backend={backend}"
        );
        assert!(
            provider.requests().is_empty(),
            "the delayed call did not finish"
        );
        assert_eq!(metrics(&report)["pebble.usage"]["tokens"]["input"], 0);
    }
}

fn permission_runtime(dir: &RunDir, client: lithos_llm::Client) -> Runtime {
    let runtime = runtime(dir, client);
    let mut options = runtime.run_options().clone();
    options.retention = Retention::Always;
    runtime.options(options)
}

#[tokio::test]
async fn permissions_enforce_writes_and_shell_with_a_host_ceiling() {
    for (requested, ceiling, writes, shell) in [
        ("read-only", AgentPermissions::Full, false, false),
        ("read-write", AgentPermissions::Full, true, false),
        ("full", AgentPermissions::Full, true, true),
        ("full", AgentPermissions::ReadOnly, false, false),
        ("full", AgentPermissions::ReadWrite, true, false),
    ] {
        let dir = RunDir::new("permissions");
        let written = dir.path().join("scopes/scope-0/work/agent-write.txt");
        let shelled = dir.path().join("scopes/scope-0/work/agent-shell.txt");
        let (client, provider) = scripted_client(vec![
            ScriptedCall::response(tool_call_response(
                "write_file",
                "write",
                json!({"file_path": "agent-write.txt", "content": "written"}),
            )),
            ScriptedCall::response(tool_call_response(
                "shell",
                "shell",
                json!({"command": format!("touch '{}'", shelled.display())}),
            )),
            ScriptedCall::response(text_response("Done.")),
        ]);
        let report = permission_runtime(&dir, client)
            .capability(AgentPermissionCeiling(ceiling))
            .run(graph(&format!("permissions=\"{requested}\"")))
            .await
            .expect("run");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.history()
        );
        assert_eq!(written.exists(), writes, "{requested}, {ceiling:?}");
        assert_eq!(shelled.exists(), shell, "{requested}, {ceiling:?}");
        assert_eq!(provider.requests().len(), 3);
    }
}

#[tokio::test]
async fn retained_conversation_rebinds_permissions_and_scripts_still_write() {
    let dir = RunDir::new("permissions-resume");
    let written = dir.path().join("scopes/scope-0/work/first-stage.txt");
    let forbidden = dir.path().join("scopes/scope-0/work/second-stage.txt");
    let report_file = dir.path().join("scopes/scope-0/work/report.txt");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "write_file",
            "write-first",
            json!({"file_path": "first-stage.txt", "content": "first"}),
        )),
        ScriptedCall::response(text_response("First stage remembered.")),
        ScriptedCall::response(tool_call_response(
            "write_file",
            "write-second",
            json!({"file_path": "second-stage.txt", "content": "forbidden"}),
        )),
        ScriptedCall::response(text_response("Reviewed.")),
    ]);
    let source = format!(
        r#"digraph T {{
        graph [default_model="test/model"]
        start [shape=Mdiamond]
        a [prompt="write", fidelity="full", thread_id="shared", permissions="full"]
        b [prompt="review", fidelity="full", thread_id="shared", permissions="read-only"]
        report [shape=parallelogram, script="touch '{}'" ]
        exit [shape=Msquare]
        start -> a -> b -> report -> exit
    }}"#,
        report_file.display()
    );
    let graph = frontend_attractor::load("test.fabro", &source, &NoFiles, &CompileInputs::new())
        .graph
        .expect("graph");
    let report = permission_runtime(&dir, client)
        .run(graph)
        .await
        .expect("run");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.history()
    );
    assert!(written.exists());
    assert!(!forbidden.exists());
    assert!(
        report_file.exists(),
        "workflow scripts retain their own access"
    );
    let requests = provider.requests();
    assert!(
        serde_json::to_string(&requests[2])
            .expect("request")
            .contains("First stage remembered.")
    );
}

#[tokio::test]
async fn host_ceiling_refuses_acp_before_starting_a_process() {
    let dir = RunDir::new("permissions-acp");
    let (client, _) = scripted_client(Vec::new());
    let mut graph = graph("");
    let config = node_config(&mut graph, "a");
    config["backend"] = json!("acp");
    config.as_object_mut().expect("object").remove("model");
    let report = permission_runtime(&dir, client)
        .capability(AgentPermissionCeiling(AgentPermissions::ReadOnly))
        .run(graph)
        .await
        .expect("run");
    assert_eq!(testkit::status_of(&report, "a").as_deref(), Some("failure"));
    assert!(format!("{:?}", report.state.history()).contains("unsupported_permissions"));
}

#[tokio::test]
async fn agent_hooks_obey_stage_permissions_and_host_ceiling() {
    use frontend_attractor::hooks::{HookDefinition, HookEvent, HookKind};
    let cases = [
        ("read-only", AgentPermissions::Full),
        ("full", AgentPermissions::ReadOnly),
    ]
    .into_iter()
    .flat_map(|(stage, ceiling)| {
        [
            HookEvent::StageStart,
            HookEvent::PreToolUse,
            HookEvent::RunStart,
        ]
        .map(|event| (stage, ceiling, event))
    });
    for (stage, ceiling, event) in cases {
        let dir = RunDir::new("hook-permissions");
        let forbidden = dir.path().join("scopes/scope-0/work/hook-write.txt");
        let mut calls = vec![
            ScriptedCall::response(tool_call_response(
                "write_file",
                "write",
                json!({"file_path": "hook-write.txt", "content": "forbidden"}),
            )),
            ScriptedCall::response(text_response(r#"{"ok":true}"#)),
            ScriptedCall::response(text_response("Reviewed.")),
        ];
        let is_tool = event == HookEvent::PreToolUse;
        if is_tool {
            calls.insert(
                0,
                ScriptedCall::response(tool_call_response(
                    "read_file",
                    "read",
                    json!({"file_path": "source.txt"}),
                )),
            );
        }
        let expected = calls.len();
        let (client, provider) = scripted_client(calls);
        let hook = HookDefinition {
            name: "review gate".into(),
            id: None,
            event,
            kind: HookKind::Agent {
                prompt:          "Review".into(),
                model:           Some("test/model".into()),
                max_tool_rounds: Some(3),
            },
            matcher: (event != HookEvent::RunStart).then(|| "^a$".into()),
            blocking: None,
            timeout_ms: None,
            sandbox: None,
            source: None,
        };
        let source = format!(
            r#"digraph T {{ graph [default_model="test/model", default_permissions="{stage}"]
            start [shape=Mdiamond] a [prompt="work"]
            setup [shape=parallelogram, script="printf 'source for review' > source.txt"]
            exit [shape=Msquare] start -> setup -> a -> exit }}"#
        );
        let settings = RunSettings {
            hooks: vec![hook],
            ..RunSettings::default()
        };
        let graph = frontend_attractor::lower(
            "test.fabro",
            &source,
            &NoFiles,
            &CompileInputs::new(),
            settings,
            Diagnostics::new(),
        )
        .graph
        .expect("graph");
        let report = permission_runtime(&dir, client)
            .capability(AgentPermissionCeiling(ceiling))
            .run(graph)
            .await
            .expect("run");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.history()
        );
        assert!(!forbidden.exists());
        let requests = provider.requests();
        assert_eq!(requests.len(), expected, "hook actually ran");
        if is_tool {
            assert!(
                serde_json::to_string(requests.last().expect("request"))
                    .expect("request")
                    .contains("source for review"),
                "native reads remain allowed"
            );
        }
    }
}

#[tokio::test]
async fn stage_outputs_leave_out_agent_permissions() {
    let dir = RunDir::new("permissions-output");
    let (client, _) = scripted_client(vec![ScriptedCall::response(text_response("Done."))]);
    let report = permission_runtime(&dir, client)
        .run(graph(r#"permissions="read-only""#))
        .await
        .expect("run");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.history()
    );
    for stage in ["start", "exit"] {
        let output = output_of(&report, stage);
        assert!(
            output.get("permissions").is_none() && output.get("default_permissions").is_none(),
            "{stage}: {output}"
        );
    }
}

#[tokio::test]
async fn parallel_hooks_use_the_parallel_nodes_permissions() {
    use frontend_attractor::hooks::{HookDefinition, HookEvent, HookKind};
    let cases = [
        ("read-only", AgentPermissions::Full),
        ("full", AgentPermissions::ReadOnly),
    ]
    .into_iter()
    .flat_map(|(fork, ceiling)| {
        [HookEvent::ParallelStart, HookEvent::ParallelComplete].map(|event| (fork, ceiling, event))
    });
    for (fork, ceiling, event) in cases {
        let dir = RunDir::new("parallel-hook-permissions");
        let forbidden = dir.path().join("scopes/scope-0/work/hook-write.txt");
        let calls = vec![
            ScriptedCall::response(tool_call_response(
                "write_file",
                "write",
                json!({"file_path": "hook-write.txt", "content": "forbidden"}),
            )),
            ScriptedCall::response(text_response(r#"{"ok":true}"#)),
        ];
        let expected = calls.len();
        let (client, provider) = scripted_client(calls);
        let hook = HookDefinition {
            name: "fork gate".into(),
            id: None,
            event,
            kind: HookKind::Agent {
                prompt:          "Review".into(),
                model:           Some("test/model".into()),
                max_tool_rounds: Some(3),
            },
            matcher: Some("^fork$".into()),
            blocking: None,
            timeout_ms: None,
            sandbox: None,
            source: None,
        };
        // The graph default is `full`: only the parallel node restricts.
        let source = format!(
            r#"digraph T {{ graph [default_model="test/model"]
            start [shape=Mdiamond] exit [shape=Msquare]
            fork [shape=component, permissions="{fork}"]
            ok [shape=parallelogram, script="true"]
            also [shape=parallelogram, script="true"]
            merge [shape=tripleoctagon]
            start -> fork fork -> ok fork -> also ok -> merge also -> merge merge -> exit }}"#
        );
        let settings = RunSettings {
            hooks: vec![hook],
            ..RunSettings::default()
        };
        let graph = frontend_attractor::lower(
            "test.fabro",
            &source,
            &NoFiles,
            &CompileInputs::new(),
            settings,
            Diagnostics::new(),
        )
        .graph
        .expect("graph");
        let report = permission_runtime(&dir, client)
            .capability(AgentPermissionCeiling(ceiling))
            .run(graph)
            .await
            .expect("run");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{event:?}: {:?}",
            report.state.history()
        );
        assert_eq!(
            provider.requests().len(),
            expected,
            "{event:?}: the hook actually ran"
        );
        assert!(!forbidden.exists(), "{fork}, {ceiling:?}, {event:?}");
    }
}
