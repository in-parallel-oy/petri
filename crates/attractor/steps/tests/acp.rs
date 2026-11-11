//! The ACP client against Petri's scripted agent
//! (`tests/testdata/scripted_acp_agent.py`), which speaks what the real
//! products speak beyond a text turn: every `session/update` variant is
//! recorded as the `acp` envelope, a permission request is allowed always
//! when no `pre_tool_use` hook is configured, the session usage extension
//! folds into the stage's metrics, an agent that requires authentication is
//! authenticated with its API-key method, and the secrets a launch names
//! reach the agent's environment, and no others, masked in every log. The hook
//! mapping itself is covered with `[[run.hooks]]` in
//! `crates/fabro/frontend/tests/hooks.rs`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use attractor_steps::acp::ENVELOPE_KIND;
use attractor_steps::register;
use frontend_attractor::load;
use runtime::driver::ExecutionReport;
use runtime::engine::Event;
use runtime::executor::{MapSecrets, Retention};
use runtime::frontend::{CompileInputs, NoFiles};
use runtime::ir::{ExprOrValue, Graph, RunStatus, StepEvent, Value};
use runtime::{RunOptions, Runtime};
use serde_json::json;
use smol_str::SmolStr;
use testkit::{RunDir, backend_event, log_lines, output_of, status_of};

/// The scripted agent, copied from the test data to where a run can execute
/// it.
fn scripted_agent(dir: &RunDir) -> PathBuf {
    scripted_agent_named(dir, "scripted_acp_agent.py")
}

/// The scripted agent under `name`: named `codex-acp…`, the client takes it
/// for codex-acp.
fn scripted_agent_named(dir: &RunDir, name: &str) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/testdata/scripted_acp_agent.py");
    let script =
        fs::read_to_string(&source).unwrap_or_else(|e| panic!("{}: {e}", source.display()));
    let path = dir.path().join(name);
    fs::write(&path, script).expect("write the scripted agent");
    path
}

#[expect(
    clippy::print_stderr,
    reason = "a graph that fails to lower explains itself in the test output"
)]
fn lower(text: &str) -> Graph {
    let lowered = load("test.fabro", text, &NoFiles, &CompileInputs::new());
    for d in lowered.diagnostics.iter() {
        eprintln!("{d}");
    }
    lowered.graph.expect("lowers")
}

/// A one-agent graph on the ACP backend running the scripted agent.
fn agent_graph(agent: &Path) -> Graph {
    lower(&format!(
        r#"digraph T {{
        graph [goal="Greet", backend="acp", acp.command="python3 {}"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [prompt="Write the file"]
        start -> a -> exit
    }}"#,
        agent.display()
    ))
}

/// The scripted agent's behaviour is chosen through `ACP_MODE` and friends
/// in the environment: set them on the scope so the agent process sees them.
fn with_env(mut graph: Graph, pairs: &[(&str, &str)]) -> Graph {
    for scope in &mut graph.body.scopes {
        for (key, value) in pairs {
            scope
                .env
                .insert((*key).into(), ExprOrValue::Value(json!(value)));
        }
    }
    graph
}

/// The node's step config, to name the agent by `acp.config` with an
/// environment the DOT attribute would have to escape.
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

fn runtime(dir: &RunDir, secrets: MapSecrets) -> Runtime {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_secs(2);
    options.retention = Retention::Always;
    options.echo = false;
    register(Runtime::standard().secrets(secrets)).options(options)
}

async fn run(dir: &RunDir, graph: Graph, secrets: MapSecrets) -> ExecutionReport {
    runtime(dir, secrets)
        .run(graph)
        .await
        .expect("replay is byte-identical")
}

/// The `step.progress.recorded` custom payloads of the run, in order.
fn customs(report: &ExecutionReport) -> Vec<Value> {
    report
        .state
        .log
        .events()
        .filter_map(|event| match event {
            Event::StepProgressRecorded {
                ev: StepEvent::Custom(value),
                ..
            } => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn envelopes(report: &ExecutionReport) -> Vec<Value> {
    customs(report)
        .into_iter()
        .filter(|value| value["kind"] == ENVELOPE_KIND)
        .collect()
}

/// What an envelope is about: the update's variant, or the method for
/// anything else.
fn subject(envelope: &Value) -> String {
    envelope["event"]["update"]["sessionUpdate"]
        .as_str()
        .or_else(|| envelope["event"]["method"].as_str())
        .unwrap_or("?")
        .to_owned()
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

/// Every `session/update` the agent sends, and the permission exchange, is
/// on the stream as the backend envelope: `kind = "acp"`, the stage's
/// identity, and an `event` with the session, a sequence and the tool call
/// the update names. Without a `pre_tool_use` hook the permission request is
/// allowed always, and the usage the agent reports is the stage's
/// `acp.usage`.
#[tokio::test]
async fn every_session_update_is_recorded_as_the_acp_envelope() {
    let dir = RunDir::new("acp-envelope");
    let agent = scripted_agent(&dir);
    let permission = dir.path().join("permission.json");
    let graph = with_env(agent_graph(&agent), &[
        ("ACP_MODE", "tools"),
        ("ACP_PERMISSION", permission.to_str().expect("utf-8")),
    ]);
    let report = run(&dir, graph, MapSecrets::empty()).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], json!("done"));
    assert_eq!(
        fs::read_to_string(dir.workspace().join("hello.txt")).expect("the agent wrote the file"),
        "hello from acp\n"
    );

    let envelopes = envelopes(&report);
    let subjects: Vec<String> = envelopes.iter().map(subject).collect();
    assert_eq!(
        subjects,
        [
            "agent_thought_chunk",
            "plan",
            "tool_call",
            "session/request_permission",
            "tool_call_update",
            "tool_call",
            "tool_call_update",
            "usage_update",
            "agent_message_chunk",
        ],
        "every update in the agent's order, the permission exchange among them"
    );
    for (index, envelope) in envelopes.iter().enumerate() {
        assert_eq!(envelope["node"], "a");
        assert_eq!(envelope["attempt"], 1);
        assert!(envelope["firing"].is_number(), "{envelope}");
        assert!(envelope["scope"].is_number(), "{envelope}");
        let backend = backend_event(envelope).expect("the envelope shape a backend event has");
        assert_eq!(backend.backend, "acp");
        assert_eq!(backend.session.as_deref(), Some("sess-1"));
        assert_eq!(
            backend.stream_seq,
            Some(u64::try_from(index + 1).expect("small")),
            "one sequence per envelope"
        );
    }
    let tool_calls: Vec<Option<String>> = envelopes
        .iter()
        .filter(|e| subject(e).starts_with("tool_call") || subject(e).starts_with("session/"))
        .map(|e| backend_event(e).and_then(|b| b.tool_call))
        .collect();
    assert_eq!(
        tool_calls,
        [
            Some("call-1".to_owned()),
            Some("call-1".to_owned()),
            Some("call-1".to_owned()),
            Some("call-2".to_owned()),
            Some("call-2".to_owned()),
        ],
        "the tool call id rides on every update and exchange about a call"
    );
    let asked = envelopes
        .iter()
        .find(|e| subject(e) == "session/request_permission")
        .expect("the permission exchange");
    assert_eq!(asked["event"]["outcome"]["outcome"], "selected");
    assert_eq!(
        asked["event"]["outcome"]["optionId"], "always",
        "no pre_tool_use hook: allowed always, as Fabro's client answered"
    );
    assert!(asked["event"]["blocked"].is_null(), "{asked}");
    assert_eq!(asked["event"]["params"]["toolCall"]["toolCallId"], "call-1");
    let answered = fs::read_to_string(&permission).expect("the agent recorded the answer");
    assert!(answered.contains("\"always\""), "{answered}");
    let plan = envelopes
        .iter()
        .find(|e| subject(e) == "plan")
        .expect("plan");
    assert_eq!(
        plan["event"]["update"]["entries"][0]["content"],
        "Write hello.txt"
    );

    let metrics = metrics(&report);
    assert_eq!(metrics["acp.turns"], json!(1));
    assert_eq!(metrics["acp.usage"]["tokens"]["input"], json!(100));
    assert_eq!(metrics["acp.usage"]["tokens"]["output"], json!(50));
    assert_eq!(metrics["acp.usage"]["tokens"]["reasoning"], json!(5));
    assert_eq!(metrics["acp.usage"]["tokens"]["cache_read"], json!(20));
    assert_eq!(metrics["acp.usage"]["tokens"]["cache_write"], json!(0));
    assert_eq!(
        metrics["acp.usage"]["cost"]["usd_micros"],
        json!(12_500),
        "the session's cumulative cost, from the usage update"
    );
    assert_eq!(
        metrics["acp.context"],
        json!({ "used": 1200, "size": 200_000 }),
        "the last context window report"
    );
}

/// codex-acp, and no other agent, is offered native sub-agent sessions at
/// `initialize`. A child's updates and its permission exchange are recorded
/// under the child's own session; the stage's answer and its context window
/// come from the root session alone.
#[tokio::test]
async fn codex_subagent_sessions_are_recorded_as_their_own() {
    let dir = RunDir::new("acp-subagents");
    let agent = scripted_agent_named(&dir, "codex-acp.py");
    let initialize = dir.path().join("initialize.json");
    let graph = with_env(agent_graph(&agent), &[
        ("ACP_MODE", "subagents"),
        ("ACP_INITIALIZE_PARAMS", initialize.to_str().expect("utf-8")),
    ]);
    let report = run(&dir, graph, MapSecrets::empty()).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let offered: Value = serde_json::from_str(
        &fs::read_to_string(&initialize).expect("the agent recorded initialize"),
    )
    .expect("json");
    assert_eq!(offered["clientCapabilities"]["subagents"], json!({}));
    assert_eq!(
        offered["clientCapabilities"]["_meta"]["jetbrains"]["air"]["capabilities"],
        json!(["nativeSubagentSessions"])
    );

    let recorded: Vec<(String, Option<String>)> = envelopes(&report)
        .iter()
        .map(|e| {
            let session = backend_event(e).and_then(|b| b.session);
            (subject(e), session)
        })
        .collect();
    let root = || Some("sess-1".to_owned());
    let child = || Some("sess-child".to_owned());
    assert_eq!(recorded, [
        ("subagent_spawned".to_owned(), root()),
        ("tool_call".to_owned(), child()),
        ("session/request_permission".to_owned(), child()),
        ("tool_call_update".to_owned(), child()),
        ("usage_update".to_owned(), child()),
        ("agent_message_chunk".to_owned(), child()),
        ("subagent_state_update".to_owned(), root()),
        ("usage_update".to_owned(), root()),
        ("agent_message_chunk".to_owned(), root()),
        ("usage_update".to_owned(), child()),
    ]);
    assert_eq!(
        output_of(&report, "a")["text"],
        json!("Reviewed."),
        "the root's text alone"
    );
    assert_eq!(
        metrics(&report)["acp.context"],
        json!({ "used": 5000, "size": 272_000 }),
        "the root's window, not the child's later report"
    );
}

/// An agent other than codex-acp is not offered sub-agent sessions.
#[tokio::test]
async fn other_agents_are_not_offered_subagent_sessions() {
    let dir = RunDir::new("acp-no-subagents");
    let agent = scripted_agent(&dir);
    let initialize = dir.path().join("initialize.json");
    let graph = with_env(agent_graph(&agent), &[
        ("ACP_MODE", "plain"),
        ("ACP_INITIALIZE_PARAMS", initialize.to_str().expect("utf-8")),
    ]);
    let report = run(&dir, graph, MapSecrets::empty()).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let offered: Value = serde_json::from_str(
        &fs::read_to_string(&initialize).expect("the agent recorded initialize"),
    )
    .expect("json");
    assert_eq!(
        offered["clientCapabilities"],
        json!({ "fs": { "readTextFile": false, "writeTextFile": false } })
    );
}

/// An agent that refuses `session/new` with `auth_required` is
/// authenticated with the API-key method it advertised (its key already in
/// its environment from the run's secrets), and the session opens on the
/// second try.
#[tokio::test]
async fn an_agent_that_requires_authentication_gets_its_api_key_method() {
    let dir = RunDir::new("acp-auth");
    let agent = scripted_agent(&dir);
    let auth_record = dir.path().join("auth.txt");
    let mut graph = with_env(agent_graph(&agent), &[
        ("ACP_MODE", "auth"),
        ("ACP_AUTH_RECORD", auth_record.to_str().expect("utf-8")),
    ]);
    node_config(&mut graph, "a")["acp"] = json!({
        "config": {
            "command": "python3",
            "args": [agent.to_str().expect("utf-8")],
            "env": { "AGENT_KEY": { "$secret": "AGENT_KEY" } },
        }
    });
    let secrets = MapSecrets::from_pairs(&[("AGENT_KEY", "agent-secret-value")]);
    let report = run(&dir, graph, secrets).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], json!("hello from acp"));
    assert_eq!(
        fs::read_to_string(&auth_record)
            .expect("the agent recorded the method")
            .trim(),
        "scripted-api-key",
        "the method marked `_meta.api-key`, not the login"
    );
    assert!(
        log_lines(&report)
            .iter()
            .any(|line| line.contains("authenticating with the agent's `scripted-api-key` method")),
        "{:?}",
        log_lines(&report)
    );
}

/// The agent starts with the `$secret` references its `acp.config` names
/// and nothing else of the secrets, not even a product's API key the
/// provider knows; what the agent prints of them is masked.
#[tokio::test]
async fn only_named_secrets_reach_the_agent_masked() {
    let dir = RunDir::new("acp-credentials");
    let agent = scripted_agent(&dir);
    let env_record = dir.path().join("env.json");
    let mut graph = with_env(agent_graph(&agent), &[
        ("ACP_MODE", "tools"),
        ("ACP_ENV_RECORD", env_record.to_str().expect("utf-8")),
    ]);
    node_config(&mut graph, "a")["acp"] = json!({
        "config": {
            "command": "python3",
            "args": [agent.to_str().expect("utf-8")],
            "env": { "AGENT_KEY": { "$secret": "AGENT_KEY" } },
        }
    });
    let secrets = MapSecrets::from_pairs(&[
        ("AGENT_KEY", "agent-secret-value"),
        ("GEMINI_API_KEY", "gemini-secret-value"),
        ("UNRELATED", "unrelated-secret-value"),
    ]);
    let report = run(&dir, graph, secrets).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let seen: Value = serde_json::from_str(
        &fs::read_to_string(&env_record).expect("the agent recorded its environment"),
    )
    .expect("json");
    assert_eq!(
        seen,
        json!({ "AGENT_KEY": "agent-secret-value" }),
        "the named reference, no other secret"
    );
    let lines = log_lines(&report);
    assert!(
        lines.iter().any(|line| line == "AGENT_KEY=***"),
        "the referenced secret the agent printed is masked: {lines:?}"
    );
    let text = format!("{lines:?}");
    assert!(
        !text.contains("secret-value"),
        "no secret value in any log line: {lines:?}"
    );
}

/// A `$secret` reference the run cannot supply fails the node before the
/// agent starts, with the class every secret-less command fails with.
#[tokio::test]
async fn a_secret_reference_the_run_cannot_supply_fails_the_node() {
    let dir = RunDir::new("acp-missing-secret");
    let agent = scripted_agent(&dir);
    let mut graph = lower(&format!(
        r#"digraph T {{
        graph [goal="G", backend="acp", acp.command="python3 {}"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [prompt="Write the file", on_failure="exit"]
        start -> a -> exit
    }}"#,
        agent.display()
    ));
    node_config(&mut graph, "a")["acp"] = json!({
        "config": {
            "command": "python3",
            "args": [agent.to_str().expect("utf-8")],
            "env": { "AGENT_KEY": { "$secret": "NOT_THERE" } },
        }
    });
    let report = run(&dir, graph, MapSecrets::empty()).await;
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(status_of(&report, "a").as_deref(), Some("failure"));
    let output = output_of(&report, "a");
    assert_eq!(output["failure_class"], json!("secret_unavailable"));
    assert!(
        output["failure_reason"]
            .as_str()
            .is_some_and(|reason| reason.contains("NOT_THERE")),
        "{output}"
    );
}

#[tokio::test]
async fn workflow_secrets_reach_command_and_config_acp_agents_with_explicit_overrides() {
    for config_launch in [false, true] {
        let dir = RunDir::new("acp-workflow-secrets");
        let agent = scripted_agent(&dir);
        let record = dir.path().join("env.json");
        let mut graph = with_env(agent_graph(&agent), &[
            ("ACP_MODE", "tools"),
            ("ACP_ENV_RECORD", record.to_str().expect("utf-8")),
            ("ACP_ENV_RECORD_KEYS", "AGENT_KEY"),
        ]);
        let config = node_config(&mut graph, "a");
        config["env"] = json!({ "AGENT_KEY": { "$secret": "WORKFLOW_KEY" } });
        if config_launch {
            config["acp"] = json!({
                "config": {"command":"python3", "args":[agent], "env":{"AGENT_KEY":{"$secret":"OVERRIDE_KEY"}}},
            });
        }
        let secrets = MapSecrets::from_pairs(&[
            ("WORKFLOW_KEY", "workflow-secret-value"),
            ("OVERRIDE_KEY", "override-secret-value"),
        ]);
        let report = run(&dir, graph, secrets).await;
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.errors()
        );
        let seen: Value =
            serde_json::from_str(&fs::read_to_string(record).expect("record")).expect("json");
        let expected = if config_launch {
            "override-secret-value"
        } else {
            "workflow-secret-value"
        };
        assert_eq!(seen, json!({"AGENT_KEY": expected}));
        let logs = log_lines(&report);
        assert!(logs.iter().any(|line| line == "AGENT_KEY=***"), "{logs:?}");
        assert!(!format!("{logs:?}").contains("secret-value"));
    }
}
