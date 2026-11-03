//! The local hook system, readiness item 5: `[[run.hooks]]` loaded from the
//! workflow's configuration and executed at every reference phase, with the
//! four executor types, Fabro's decision rules, and the tool boundary of the
//! native backend. Plus fidelity, threads and project memory on real runs.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, fs, process};

use attractor_steps::acp::ENVELOPE_KIND;
use attractor_steps::agent::THREAD_EVENT;
use attractor_steps::hooks::{REPORT_EVENT, WARNING_EVENT};
use attractor_steps::pebble::PebbleClient;
use attractor_steps::{
    AGENT_KIND, BranchStep, CommandStep, FanInStep, ForkStep, HumanStep, PROMPT_KIND, StageStep,
    StubStep, WAIT_KIND, WORKFLOW_KIND, register,
};
use execution::events::{Parsed, RunEvent, replay_run_dir};
use execution::hooks::{
    HookActivity, HookAdapter, HookDecision, HookPoint, HookReport, HookRequest, HookRun,
    HookService, HookServiceHandle,
};
use execution::host::{self, HostRun};
use frontend::{CompileInputs, Lowered, MapFiles};
use ir::{Graph, RunStatus, StepEvent, Value};
use lithos_llm::types::{ErrorKind, Message, Role, Speed};
use pebble_coding_agent::test_support::{
    ScriptedCall, ScriptedCompletion, ScriptedFailure, ScriptedProvider, client_from, message_text,
    text_response, tool_call_response,
};
use runtime::driver::lifecycle::Note;
use runtime::driver::{EventObserver, ExecutionReport};
use runtime::engine::{EngineState, Event, EventRecord};
use runtime::executor::Retention;
use runtime::{RunOptions, Runtime};
use serde_json::json;
use testkit::{RunDir, backend_event, output_of, status_of};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// Every `StepEvent::Custom` the run emitted, with the node it came from.
#[derive(Default)]
struct Customs(Mutex<Vec<(String, Value)>>);

impl EventObserver for Customs {
    fn on_record(&self, record: &EventRecord, _recorded_at: u64, state: &EngineState) {
        if let Event::StepProgressRecorded {
            firing,
            ev: StepEvent::Custom(value),
        } = &record.event
        {
            let node = state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .map(|n| n.name.to_string())
                .unwrap_or_default();
            self.0
                .lock()
                .expect("not poisoned")
                .push((node, value.clone()));
        }
    }
}

impl Customs {
    fn all(&self) -> Vec<(String, Value)> {
        self.0.lock().expect("not poisoned").clone()
    }

    /// The hook notes the adapter recorded, `(node, report)`.
    fn hook_notes(&self) -> Vec<(String, Value)> {
        self.all()
            .into_iter()
            .filter_map(|(node, value)| {
                let note = Note::from_step_event(&StepEvent::Custom(value))?;
                (note.kind == "hook").then_some((node, note.payload))
            })
            .collect()
    }

    /// The `hook.activity` notes: `(node, payload)`, in order.
    fn hook_activity(&self) -> Vec<(String, Value)> {
        self.all()
            .into_iter()
            .filter_map(|(node, value)| {
                let note = Note::from_step_event(&StepEvent::Custom(value))?;
                (note.kind == "hook.activity").then_some((node, note.payload))
            })
            .collect()
    }

    /// The reports steps drove themselves (`attractor.hook`).
    fn reports(&self) -> Vec<(String, Value)> {
        self.all()
            .into_iter()
            .filter(|(_, v)| v["kind"] == REPORT_EVENT)
            .collect()
    }

    fn warnings(&self) -> Vec<Value> {
        self.all()
            .into_iter()
            .filter(|(_, v)| v["kind"] == WARNING_EVENT)
            .map(|(_, v)| v)
            .collect()
    }

    fn threads(&self) -> Vec<(String, Value)> {
        self.all()
            .into_iter()
            .filter(|(_, v)| v["kind"] == THREAD_EVENT)
            .collect()
    }
}

/// Every hook name that ran, in order, from both record kinds.
fn hook_names(customs: &Customs) -> Vec<String> {
    let mut names = Vec::new();
    for (_, report) in customs.hook_notes() {
        for hook in report["hooks"].as_array().into_iter().flatten() {
            names.push(hook["name"].as_str().unwrap_or("?").to_owned());
        }
    }
    for (_, event) in customs.reports() {
        for hook in event["report"]["hooks"].as_array().into_iter().flatten() {
            names.push(hook["name"].as_str().unwrap_or("?").to_owned());
        }
    }
    names
}

fn lower(dot: &str, toml: &str) -> Graph {
    lower_all(dot, toml).graph.expect("lowers")
}

/// The whole lowering, child graphs included (a parallel node's branches).
fn lower_all(dot: &str, toml: &str) -> Lowered {
    let files = MapFiles(BTreeMap::from([(
        "wf/workflow.toml".to_string(),
        toml.to_string(),
    )]));
    let lowered = frontend_fabro::load("wf/w.fabro", dot, &files, &CompileInputs::new());
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    lowered
}

/// Real commands, stages, branches and fan-ins under the coordinator (a
/// branch is a child invocation), simulated agents.
async fn run_coordinated(dir: &RunDir, lowered: Lowered) -> (ExecutionReport, Arc<Customs>) {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(200);
    options.retention = Retention::Always;
    options.echo = false;
    let customs = Arc::new(Customs::default());
    let mut registry = Runtime::standard().registry().clone();
    for kind in [&AGENT_KIND, &PROMPT_KIND, &WAIT_KIND, &WORKFLOW_KIND] {
        registry.register_runner(Arc::new(StubStep::new((*kind).clone())));
    }
    registry.register(CommandStep);
    registry.register(HumanStep);
    registry.register(StageStep);
    registry.register(ForkStep);
    registry.register(BranchStep);
    registry.register(FanInStep);
    let rt = attractor_steps::services(
        Runtime::standard()
            .steps(registry)
            .observe(customs.clone())
            .options(options),
    );
    let graph = lowered.graph.expect("lowers");
    let report = host::run_configured(
        &rt,
        HostRun::new(graph).with_children(lowered.children),
        |_, _| {},
    )
    .await
    .expect("the run completes");
    (report, customs)
}

fn runtime(dir: &RunDir, client: Option<lithos_llm::Client>) -> (Runtime, Arc<Customs>) {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(200);
    options.retention = Retention::Always;
    options.echo = false;
    let customs = Arc::new(Customs::default());
    let rt = Runtime::standard()
        .observe(customs.clone())
        .options(options);
    let rt = match client {
        Some(client) => rt.capability(PebbleClient(client)),
        None => rt,
    };
    (register(rt), customs)
}

async fn run(
    dir: &RunDir,
    graph: Graph,
    client: Option<lithos_llm::Client>,
) -> (ExecutionReport, Arc<Customs>) {
    let (rt, customs) = runtime(dir, client);
    let report = rt.run(graph).await.expect("replay is byte-identical");
    (report, customs)
}

/// Real commands and stages, simulated agents: what a retry phase needs, as
/// a real command never requests a retry (Fabro's does not either).
async fn run_with_stub_agents(dir: &RunDir, graph: Graph) -> (ExecutionReport, Arc<Customs>) {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(200);
    options.retention = Retention::Always;
    options.echo = false;
    let customs = Arc::new(Customs::default());
    let mut registry = Runtime::standard().registry().clone();
    for kind in [&AGENT_KIND, &PROMPT_KIND, &WAIT_KIND, &WORKFLOW_KIND] {
        registry.register_runner(Arc::new(StubStep::new((*kind).clone())));
    }
    registry.register(CommandStep);
    registry.register(HumanStep);
    registry.register(StageStep);
    let rt = attractor_steps::services(
        Runtime::standard()
            .steps(registry)
            .observe(customs.clone())
            .options(options),
    );
    let report = rt.run(graph).await.expect("replay is byte-identical");
    (report, customs)
}

/// Script a stub node's calls, as the embedding test does.
fn simulate(graph: &mut Graph, node: &str, calls: &Value) {
    let node = graph
        .body
        .nodes
        .iter_mut()
        .find(|n| n.name == node)
        .expect("node");
    let Value::Object(config) = &mut node.step.config else {
        panic!("an object config");
    };
    config.insert("simulate".into(), json!({ "calls": calls }));
}

/// Run under a host that replaced the hook service before `register`: the
/// service behind the adapter and behind the `HookServiceHandle` the steps
/// ask. The local service is then not installed at all.
async fn run_hosted(
    dir: &RunDir,
    graph: Graph,
    client: Option<lithos_llm::Client>,
    service: Arc<dyn HookService>,
) -> (ExecutionReport, Arc<Customs>) {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(200);
    options.retention = Retention::Always;
    options.echo = false;
    let customs = Arc::new(Customs::default());
    let rt = Runtime::standard()
        .observe(customs.clone())
        .options(options)
        .hooks(Arc::new(HookAdapter::new(service.clone())))
        .capability(HookServiceHandle(service));
    let rt = match client {
        Some(client) => rt.capability(PebbleClient(client)),
        None => rt,
    };
    let report = register(rt)
        .run(graph)
        .await
        .expect("replay is byte-identical");
    (report, customs)
}

/// A host's replacement service: it counts every request by point and node,
/// blocks tool calls when told, and names the tool hooks it claims to have
/// configured, so the ACP backend can warn about the boundaries it lacks.
struct HostService {
    calls:       Mutex<Vec<(HookPoint, String)>>,
    block_tools: bool,
    configured:  BTreeMap<HookPoint, Vec<String>>,
}

impl HostService {
    fn new(block_tools: bool, configured: BTreeMap<HookPoint, Vec<String>>) -> Arc<Self> {
        Arc::new(Self {
            calls: Mutex::new(Vec::new()),
            block_tools,
            configured,
        })
    }

    fn count(&self, point: HookPoint, node: &str) -> usize {
        self.calls
            .lock()
            .expect("not poisoned")
            .iter()
            .filter(|(p, n)| *p == point && n == node)
            .count()
    }
}

#[async_trait::async_trait]
impl HookService for HostService {
    async fn run(&self, request: HookRequest) -> HookReport {
        let node = request
            .view
            .as_deref()
            .map(|view| view.node_name().to_owned())
            .unwrap_or_default();
        self.calls
            .lock()
            .expect("not poisoned")
            .push((request.point, node));
        let mut report = HookReport::proceed(request.point);
        if request.point == HookPoint::BeforeToolUse && self.block_tools {
            report.decision = HookDecision::Block {
                reason: "the host says no".into(),
            };
            report.hooks.push(HookRun {
                name:        "host-guard".into(),
                state:       "executed".into(),
                duration_ms: Some(1),
                message:     None,
                usage:       None,
            });
        }
        report
    }

    fn configured_hooks(&self, point: HookPoint) -> Vec<String> {
        self.configured.get(&point).cloned().unwrap_or_default()
    }
}

fn workspace(dir: &RunDir) -> PathBuf {
    dir.path().join("scopes/scope-0/work")
}

/// The workspace of a run under the coordinator (`host::run_configured`).
fn coordinated_workspace(dir: &RunDir) -> PathBuf {
    dir.path().join("scopes/invocation-0-scope-0/work")
}

/// Run one graph under the coordinator, as the standalone host does, so the
/// run dir replays (`replay_run` reads the coordinator log).
async fn run_replayable(
    dir: &RunDir,
    graph: Graph,
    client: Option<lithos_llm::Client>,
) -> (ExecutionReport, Arc<Customs>) {
    let (rt, customs) = runtime(dir, client);
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    (report, customs)
}

/// The variant name of a Pebble event envelope's `event`: the one key of a
/// variant with data, the string itself for a unit variant.
fn variant(envelope: &Value) -> Option<String> {
    match &envelope["event"] {
        Value::String(name) => Some(name.clone()),
        Value::Object(map) => map.keys().next().cloned(),
        _ => None,
    }
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

// ── Configuration, dispatch, command hooks ──────────────────────────────────

/// Every reference phase fires a command hook with Fabro's payload, in the
/// sandbox by default and on the host when asked; the log the hooks write
/// shows the order and the env each one saw.
#[tokio::test]
async fn command_hooks_fire_at_every_reference_phase_with_fabros_payload() {
    let dir = RunDir::new("hooks-phases");
    let ws = workspace(&dir);
    fs::create_dir_all(&ws).expect("workspace");
    let host_log = dir.path().join("host.log");
    let toml = format!(
        r#"
[[run.hooks]]
event = "run_start"
script = "echo run_start:$FABRO_EVENT:$FABRO_WORKFLOW >> hooks.log; test -n \"$FABRO_RUN_ID\""

[[run.hooks]]
event = "sandbox_ready"
script = "echo sandbox_ready:$(pwd) >> hooks.log"

[[run.hooks]]
name = "stage-start"
event = "stage_start"
script = "echo stage_start:$FABRO_NODE_ID:$(grep -o '\"attempt\":[0-9]*' \"$FABRO_HOOK_CONTEXT\" | cut -d: -f2) >> hooks.log"

[[run.hooks]]
event = "stage_complete"
script = "echo stage_complete:$FABRO_NODE_ID >> hooks.log"

[[run.hooks]]
event = "stage_failed"
script = "echo stage_failed:$FABRO_NODE_ID >> hooks.log"

[[run.hooks]]
event = "stage_retrying"
script = "echo stage_retrying:$FABRO_NODE_ID >> hooks.log"

[[run.hooks]]
event = "edge_selected"
script = "cat > ctx.json; echo edge_selected:$(grep -o '\"edge_from\":\"[^\"]*\"' ctx.json | cut -d'\"' -f4)-$(grep -o '\"edge_to\":\"[^\"]*\"' ctx.json | cut -d'\"' -f4) >> hooks.log"
sandbox = false

[[run.hooks]]
event = "run_complete"
script = "echo run_complete:$FABRO_EVENT >> {host}"
sandbox = false

[[run.hooks]]
event = "checkpoint_saved"
script = "echo never >> hooks.log"
"#,
        host = host_log.display()
    );
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        prepare [shape=parallelogram, script="echo prepared"]
        flaky [prompt="try", max_retries=1]
        start -> prepare -> flaky -> exit
    }"#,
        &toml,
    );
    let mut graph = graph;
    simulate(
        &mut graph,
        "flaky",
        &json!([
            { "outcome": "failed", "failure_class": "retry_requested" },
            { "outcome": "succeeded" }
        ]),
    );
    let (report, customs) = run_with_stub_agents(&dir, graph).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let log = read(&ws.join("hooks.log"));
    let lines: Vec<&str> = log.lines().collect();
    assert!(
        !lines.is_empty(),
        "no hook output\nnotes: {:#?}\nreports: {:#?}\nwarnings: {:#?}",
        customs.hook_notes(),
        customs.reports(),
        customs.warnings()
    );
    // Fabro's order: the sandbox is ready before the run starts.
    assert!(
        lines[0].starts_with("sandbox_ready:") && lines[0].contains("work"),
        "{log}"
    );
    assert_eq!(lines[1], "run_start:run_start:W", "{log}");
    // The start stage's own `stage_start` follows, with the sandbox in place.
    assert_eq!(lines[2], "stage_start:start:1", "{log}");
    assert!(lines.contains(&"stage_start:prepare:1"), "{log}");
    assert!(lines.contains(&"stage_complete:prepare"), "{log}");
    // The retrying command: attempt 1 fails, the retry hook runs, attempt 2
    // starts and completes.
    assert!(lines.contains(&"stage_start:flaky:1"), "{log}");
    assert!(lines.contains(&"stage_retrying:flaky"), "{log}");
    assert!(lines.contains(&"stage_start:flaky:2"), "{log}");
    assert!(lines.contains(&"stage_complete:flaky"), "{log}");
    assert!(
        !lines.contains(&"stage_failed:flaky"),
        "a retried attempt is not a failed stage: {log}"
    );
    // Edge hooks ran on the host (the context on stdin) and saw both ends.
    assert!(lines.contains(&"edge_selected:prepare-flaky"), "{log}");
    assert!(lines.contains(&"edge_selected:flaky-exit"), "{log}");
    assert!(!log.contains("never"), "checkpoint_saved never runs: {log}");
    assert_eq!(read(&host_log).trim(), "run_complete:run_complete");
    // The reports: one per point with a hook; checkpoint_saved recorded as
    // unsupported, never executed.
    let names = hook_names(&customs);
    assert!(
        names.iter().filter(|n| n.as_str() == "stage-start").count() >= 3,
        "{names:?}"
    );
    let unsupported: Vec<_> = customs
        .hook_notes()
        .into_iter()
        .chain(
            customs
                .reports()
                .into_iter()
                .map(|(n, e)| (n, e["report"].clone())),
        )
        .flat_map(|(_, r)| r["hooks"].as_array().cloned().unwrap_or_default())
        .filter(|h| h["state"] == "unsupported")
        .collect();
    assert!(
        unsupported.is_empty(),
        "checkpoint_saved matched nothing at run time: {unsupported:?}"
    );
    // The stage payload the hooks saw: node, label, handler, attempts.
    let ctx: Value = serde_json::from_str(&read(&ws.join("ctx.json"))).expect("edge context");
    assert_eq!(ctx["event"], "edge_selected");
    assert_eq!(ctx["workflow_name"], "W");
    assert_eq!(ctx["handler_type"], "agent");
}

/// Fabro's exit-code rule and the decision points: a `stage_start` skip
/// skips the node, a block fails it and the run; a nonblocking post hook's
/// decision is ignored; a `blocking = false` override on a decision point is
/// ignored too.
#[tokio::test]
async fn command_decisions_skip_block_and_ignore_nonblocking_hooks() {
    let dir = RunDir::new("hooks-decisions");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        skipped [shape=parallelogram, script="echo ran > skipped.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> skipped -> b -> exit
    }"#,
        r#"
[[run.hooks]]
name = "skip-it"
event = "stage_start"
matcher = "^skipped$"
script = "echo '{\"decision\":\"skip\",\"reason\":\"not today\"}'"

[[run.hooks]]
name = "loud-but-ignored"
event = "stage_complete"
matcher = "^a$"
script = "exit 3"

[[run.hooks]]
name = "nonblocking-block"
event = "stage_start"
matcher = "^b$"
blocking = false
script = "exit 2"
"#,
    );
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = workspace(&dir);
    assert!(ws.join("a.txt").exists());
    assert!(
        !ws.join("skipped.txt").exists(),
        "the skipped node never ran"
    );
    assert!(
        ws.join("b.txt").exists(),
        "a nonblocking block does not block"
    );
    assert_eq!(status_of(&report, "skipped").as_deref(), Some("skipped"));
    let notes = customs.hook_notes();
    let skip = notes
        .iter()
        .find(|(n, _)| n == "skipped")
        .map(|(_, r)| r.clone())
        .expect("the skip report");
    assert_eq!(skip["decision"]["decision"], "skip");
    assert_eq!(skip["hooks"][0]["name"], "skip-it");
    assert_eq!(skip["hooks"][0]["message"], "skip: not today");
    let ignored = notes
        .iter()
        .find(|(n, r)| n == "a" && r["point"] == "after_visit")
        .map(|(_, r)| r.clone())
        .expect("the post report");
    assert_eq!(ignored["decision"]["decision"], "proceed");
    assert!(
        ignored["warnings"][0]
            .as_str()
            .is_some_and(|w| w.contains("not blocking")),
        "{ignored}"
    );

    // A block at stage_start fails the node with Fabro's reason and ends the run.
    let dir = RunDir::new("hooks-block");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        guarded [shape=parallelogram, script="echo ran > guarded.txt"]
        start -> guarded -> exit
    }"#,
        r#"
[[run.hooks]]
event = "stage_start"
matcher = "^guarded$"
script = "echo 'no' >&2; exit 2"
"#,
    );
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(report.status, RunStatus::Failed);
    assert!(!workspace(&dir).join("guarded.txt").exists());
    let block = customs
        .hook_notes()
        .into_iter()
        .find(|(n, _)| n == "guarded")
        .map(|(_, r)| r)
        .expect("block report");
    assert_eq!(block["decision"]["decision"], "block");
    assert_eq!(block["decision"]["reason"], "hook exited with code 2");
}

/// `edge_selected`: an override redirects to a named target that is one of
/// the node's edges; a block stops advancement; a bad target is a warning.
#[tokio::test]
async fn edge_hooks_override_and_block_routes() {
    let dir = RunDir::new("hooks-edges");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        pick [shape=parallelogram, script="echo picked"]
        left [shape=parallelogram, script="echo left > left.txt"]
        right [shape=parallelogram, script="echo right > right.txt"]
        start -> pick
        pick -> left
        pick -> right [weight=-1]
        left -> exit
        right -> exit
    }"#,
        r#"
[[run.hooks]]
name = "reroute"
event = "edge_selected"
matcher = "^pick$"
script = "echo '{\"decision\":\"override\",\"edge_to\":\"right\"}'"
"#,
    );
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = workspace(&dir);
    assert!(
        ws.join("right.txt").exists(),
        "the override took the right edge"
    );
    assert!(!ws.join("left.txt").exists());
    let note = customs
        .hook_notes()
        .into_iter()
        .find(|(n, r)| n == "pick" && r["point"] == "route_selected")
        .map(|(_, r)| r)
        .expect("route report");
    assert_eq!(note["decision"]["decision"], "override");

    let dir = RunDir::new("hooks-edge-block");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        r#"
[[run.hooks]]
event = "edge_selected"
matcher = "^a$"
script = "exit 1"
"#,
    );
    let (report, _) = run(&dir, graph, None).await;
    assert_eq!(report.status, RunStatus::Failed);
    assert!(
        !workspace(&dir).join("b.txt").exists(),
        "a blocked edge never advances"
    );
}

/// A blocking `run_start` hook stops the run before any node; a blocked
/// `sandbox_ready` too. The stage reports it and the exit is never reached.
#[tokio::test]
async fn a_blocking_run_start_hook_stops_the_run_before_work() {
    let dir = RunDir::new("hooks-run-start");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [shape=parallelogram, script="echo worked > worked.txt"]
        start -> work -> exit
    }"#,
        r#"
[[run.hooks]]
name = "env-check"
event = "run_start"
script = "echo '{\"decision\":\"block\",\"reason\":\"missing credential\"}'"
sandbox = false
"#,
    );
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(report.status, RunStatus::Failed);
    assert!(!workspace(&dir).join("worked.txt").exists());
    assert_eq!(status_of(&report, "start").as_deref(), Some("failure"));
    assert_eq!(output_of(&report, "start")["failure_class"], "hook_blocked");
    assert!(
        output_of(&report, "start")["failure_reason"]
            .as_str()
            .is_some_and(|r| r.contains("missing credential"))
    );
    let start = customs
        .reports()
        .into_iter()
        .find(|(_, e)| e["event"] == "run_start")
        .expect("run_start report");
    assert_eq!(start.1["report"]["hooks"][0]["name"], "env-check");
}

/// A command hook that outlives its timeout blocks (exit -1), as Fabro's
/// sandbox timeout does; cancellation of the run stops a running hook.
#[tokio::test]
async fn a_command_hook_timeout_blocks() {
    let dir = RunDir::new("hooks-timeout");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        start -> a -> exit
    }"#,
        r#"
[[run.hooks]]
event = "stage_start"
matcher = "^a$"
script = "sleep 5"
timeout = "300ms"
"#,
    );
    let started = Instant::now();
    let (report, customs) = run(&dir, graph, None).await;
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "the timeout ended the hook"
    );
    assert_eq!(report.status, RunStatus::Failed);
    let block = customs
        .hook_notes()
        .into_iter()
        .find(|(n, _)| n == "a")
        .map(|(_, r)| r)
        .expect("report");
    assert_eq!(block["decision"]["reason"], "hook exited with code -1");
}

// ── HTTP hooks ──────────────────────────────────────────────────────────────

/// A loopback listener that records one request body and answers with a
/// fixed status and body.
async fn http_endpoint(
    status: &'static str,
    body: &'static str,
) -> (SocketAddr, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let record = record.clone();
            tokio::spawn(async move {
                let mut buf = vec![0_u8; 65536];
                let mut total = Vec::new();
                loop {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    total.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&total).into_owned();
                    if let Some(split) = text.find("\r\n\r\n") {
                        let head = &text[..split];
                        let len: usize = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if text.len() >= split + 4 + len {
                            record.lock().expect("not poisoned").push(text);
                            break;
                        }
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (addr, seen)
}

/// An HTTP hook posts Fabro's context with the configured headers and takes
/// the decision the endpoint returns; `tls = "off"` allows plain http; a
/// verify-mode `http://` URL blocks; a failing endpoint fails open.
#[tokio::test]
async fn http_hooks_post_the_context_and_fail_open() {
    let (blocker, seen) = http_endpoint(
        "200 OK",
        r#"{"decision":"block","reason":"webhook said no"}"#,
    )
    .await;
    let (down, _) = http_endpoint("500 Internal Server Error", "boom").await;
    let dir = RunDir::new("hooks-http");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        &format!(
            r#"
[[run.hooks]]
name = "notify"
event = "stage_complete"
matcher = "^a$"
url = "http://{down}/done"
tls = "off"
blocking = true

[[run.hooks]]
name = "insecure"
event = "stage_start"
matcher = "^a$"
url = "http://{blocker}/gate"
blocking = false

[[run.hooks]]
name = "gate"
event = "stage_start"
matcher = "^b$"
url = "http://{blocker}/gate"
tls = "off"
[run.hooks.headers]
X-Env = "test"
"#
        ),
    );
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(report.status, RunStatus::Failed);
    let ws = workspace(&dir);
    assert!(ws.join("a.txt").exists());
    assert!(!ws.join("b.txt").exists(), "the webhook blocked b");
    let requests = seen.lock().expect("not poisoned").clone();
    assert_eq!(
        requests.len(),
        1,
        "the verify-mode http URL never reached the endpoint: {requests:?}"
    );
    let request = &requests[0];
    assert!(request.starts_with("POST /gate HTTP/1.1"), "{request}");
    assert!(
        request.to_ascii_lowercase().contains("x-env: test"),
        "{request}"
    );
    let body: Value =
        serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap_or("")).expect("json body");
    assert_eq!(body["event"], "stage_start");
    assert_eq!(body["node_id"], "b");
    assert_eq!(body["attempt"], 1);
    let notes = customs.hook_notes();
    let a_start = notes
        .iter()
        .find(|(n, r)| n == "a" && r["point"] == "before_attempt")
        .expect("a start")
        .1
        .clone();
    assert_eq!(a_start["hooks"][0]["name"], "insecure");
    assert!(
        a_start["hooks"][0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("https://")),
        "{a_start}"
    );
    assert_eq!(
        a_start["decision"]["decision"], "proceed",
        "a nonblocking hook's block is ignored"
    );
    let a_done = notes
        .iter()
        .find(|(n, r)| n == "a" && r["point"] == "after_visit")
        .expect("a done")
        .1
        .clone();
    assert_eq!(a_done["hooks"][0]["state"], "failed_open");
    assert!(
        a_done["warnings"][0]
            .as_str()
            .is_some_and(|w| w.contains("500")),
        "{a_done}"
    );
    let b_start = notes
        .iter()
        .find(|(n, r)| n == "b" && r["point"] == "before_attempt")
        .expect("b start")
        .1
        .clone();
    assert_eq!(b_start["decision"]["reason"], "webhook said no");
}

// ── Prompt and agent hooks ─────────────────────────────────────────────────

fn scripted(
    stream: Vec<ScriptedCall>,
    completions: Vec<ScriptedCompletion>,
) -> (lithos_llm::Client, Arc<ScriptedProvider>) {
    client_from(ScriptedProvider::new(stream).completing(completions))
}

/// A prompt hook makes one non-streaming call with Fabro's evaluator prompt
/// and the context; `ok: false` blocks, an unparseable answer fails open, a
/// model error fails open.
#[tokio::test]
async fn prompt_hooks_evaluate_with_one_model_call_and_fail_open() {
    let dir = RunDir::new("hooks-prompt");
    let (client, provider) = scripted(vec![], vec![
        ScriptedCompletion::response(text_response(r#"{"ok": true}"#)),
        ScriptedCompletion::response(text_response("I am not sure")),
        ScriptedCompletion::response(text_response(r#"{"ok": false, "reason": "unsafe stage"}"#)),
    ]);
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        c [shape=parallelogram, script="echo c > c.txt"]
        start -> a -> b -> c -> exit
    }"#,
        r#"
[[run.hooks]]
name = "guard"
event = "stage_start"
matcher = "^(a|b|c)$"
prompt = "Should this stage proceed?"
model = "test/model"
"#,
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(report.status, RunStatus::Failed);
    let ws = workspace(&dir);
    assert!(ws.join("a.txt").exists());
    assert!(
        ws.join("b.txt").exists(),
        "an unparseable verdict fails open"
    );
    assert!(!ws.join("c.txt").exists(), "ok:false blocks");
    let requests = provider.completion_requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].model(), "test/model");
    let sent = message_text(&requests[0].messages()[1]);
    assert!(
        sent.starts_with("Hook prompt: Should this stage proceed?"),
        "{sent}"
    );
    assert!(sent.contains("\"node_id\": \"a\""), "{sent}");
    assert!(requests[0].tools().is_empty());
    let notes = customs.hook_notes();
    let b = notes
        .iter()
        .find(|(n, r)| n == "b" && r["point"] == "before_attempt")
        .expect("b")
        .1
        .clone();
    assert_eq!(b["hooks"][0]["state"], "failed_open");
    let c = notes
        .iter()
        .find(|(n, r)| n == "c" && r["point"] == "before_attempt")
        .expect("c")
        .1
        .clone();
    assert_eq!(c["decision"]["reason"], "unsafe stage");
    assert_eq!(c["hooks"][0]["state"], "executed");
    // Each verdict cost one request, whose usage is on the record: the
    // scripted model bills ten input and five output tokens per answer.
    let a = notes
        .iter()
        .find(|(n, r)| n == "a" && r["point"] == "before_attempt")
        .expect("a")
        .1
        .clone();
    for (node, report) in [("a", &a), ("b", &b), ("c", &c)] {
        let usage = &report["hooks"][0]["usage"];
        assert_eq!(usage["requests"], 1, "{node}: {report}");
        assert_eq!(usage["usage"]["tokens"]["input"], 10, "{node}: {report}");
        assert_eq!(usage["usage"]["tokens"]["output"], 5, "{node}: {report}");
        assert_eq!(
            usage["tool_calls"], 0,
            "a prompt hook runs no tool: {report}"
        );
    }
    assert!(
        customs.hook_activity().is_empty(),
        "a prompt hook has no agent"
    );
}

/// A prompt hook whose request fails or never answers still records the
/// request it made, with no tokens, beside its fail-open state.
#[tokio::test]
async fn prompt_hook_usage_records_a_failed_and_a_timed_out_request() {
    let dir = RunDir::new("hooks-prompt-usage");
    let (client, provider) = scripted(vec![], vec![
        ScriptedCompletion::Failure(ScriptedFailure::terminal(ErrorKind::QuotaExceeded, "spent")),
        ScriptedCompletion::Pending,
    ]);
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        r#"
[[run.hooks]]
name = "guard"
event = "stage_start"
matcher = "^(a|b)$"
prompt = "Should this stage proceed?"
model = "test/model"
timeout = "300ms"
"#,
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = workspace(&dir);
    assert!(
        ws.join("a.txt").exists() && ws.join("b.txt").exists(),
        "both fail open"
    );
    assert_eq!(provider.completion_requests().len(), 2);
    let notes = customs.hook_notes();
    let of = |node: &str| {
        notes
            .iter()
            .find(|(n, r)| n == node && r["point"] == "before_attempt")
            .map_or_else(|| panic!("{node}: {notes:?}"), |(_, r)| r.clone())
    };
    let a = of("a");
    assert_eq!(a["hooks"][0]["state"], "failed_open", "{a}");
    assert!(
        a["hooks"][0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("model call failed")),
        "{a}"
    );
    assert_eq!(a["hooks"][0]["usage"]["requests"], 1, "{a}");
    assert!(
        a["hooks"][0]["usage"].get("usage").is_none(),
        "no answer, no usage: {a}"
    );
    let b = of("b");
    assert_eq!(b["hooks"][0]["state"], "failed_open", "{b}");
    // The request's own deadline (the hook's timeout) or the hook's wrapper:
    // whichever fires first, the hook fails open with the request on record.
    assert!(
        b["hooks"][0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("timed out") || m.contains("deadline expired")),
        "{b}"
    );
    assert_eq!(b["hooks"][0]["usage"]["requests"], 1, "{b}");
    assert!(b["hooks"][0]["usage"].get("usage").is_none(), "{b}");
}

/// An agent hook runs a Pebble agent with the coding tools in the sandbox:
/// it reads the workspace through a real tool before deciding. Its tool calls
/// do not fire tool hooks again.
#[tokio::test]
async fn agent_hooks_investigate_the_workspace_then_decide() {
    let dir = RunDir::new("hooks-agent");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(tool_call_response(
                "shell",
                "look",
                json!({"command": "cat marker.txt"}),
            )),
            ScriptedCall::response(text_response(
                r#"{"ok": false, "reason": "marker says stop"}"#,
            )),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo stop > marker.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        r#"
[[run.hooks]]
name = "verify"
event = "stage_start"
matcher = "^b$"
agent = "enabled"
prompt = "Read marker.txt and decide."
model = "test/model"
max_tool_rounds = 3

[[run.hooks]]
name = "tool-guard"
event = "pre_tool_use"
script = "echo tool-guard-ran >> tool-hooks.log"
"#,
    );
    let (report, customs) = run_replayable(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Failed,
        "{:?}",
        report.state.errors()
    );
    let ws = coordinated_workspace(&dir);
    assert!(!ws.join("b.txt").exists());
    assert_eq!(provider.requests().len(), 2, "one tool round, one verdict");
    let sent = serde_json::to_string(&provider.requests()[1]).expect("request");
    assert!(
        sent.contains("stop"),
        "the tool result reached the hook agent: {sent}"
    );
    assert!(
        !ws.join("tool-hooks.log").exists(),
        "hook work fires no hooks"
    );
    let b = customs
        .hook_notes()
        .into_iter()
        .find(|(n, r)| n == "b" && r["point"] == "before_attempt")
        .map(|(_, r)| r)
        .expect("b");
    assert_eq!(b["decision"]["reason"], "marker says stop");
    assert_eq!(b["hooks"][0]["name"], "verify");
    // What the hook's agent spent: two model turns and one tool call, the
    // scripted model's ten-and-five per answer summed.
    let usage = &b["hooks"][0]["usage"];
    assert_eq!(usage["requests"], 2, "{b}");
    assert_eq!(usage["tool_calls"], 1, "{b}");
    assert_eq!(usage["usage"]["tokens"]["input"], 20, "{b}");
    assert_eq!(usage["usage"]["tokens"]["output"], 10, "{b}");
    assert!(
        usage["inference_ms"].is_u64() && usage["tool_ms"].is_u64(),
        "{b}"
    );
    // What it did, on the record under the hook's identity, attributed to
    // the stage it guarded: Pebble's own events, the tool call among them,
    // through to the session's end.
    let activity = customs.hook_activity();
    assert!(activity.iter().all(|(node, _)| node == "b"), "{activity:?}");
    assert!(
        activity.iter().all(|(_, a)| {
            a["hook"] == json!({ "point": "before_attempt", "hook": "verify" })
                && a["backend"] == "pebble"
        }),
        "{activity:?}"
    );
    let variants: Vec<String> = activity
        .iter()
        .filter_map(|(_, a)| variant(&a["envelope"]))
        .collect();
    assert!(
        variants.contains(&"ToolCallStarted".to_owned()),
        "{variants:?}"
    );
    assert!(
        variants.contains(&"SessionEnded".to_owned()),
        "{variants:?}"
    );
    let tool = activity
        .iter()
        .find(|(_, a)| a["envelope"]["event"].get("ToolCallStarted").is_some())
        .expect("the tool call")
        .1
        .clone();
    assert_eq!(
        tool["envelope"]["event"]["ToolCallStarted"]["tool_name"],
        "shell"
    );
    // The same facts through the public stream, replayed from the run dir:
    // typed hook activity apart from any stage's own agent activity.
    let events = replay_run_dir(dir.path()).await.expect("replays");
    let replayed: Vec<_> = events
        .iter()
        .filter(|e| hook_activity_of(e).is_some())
        .collect();
    assert_eq!(replayed.len(), activity.len(), "{replayed:#?}");
    assert!(replayed.iter().all(|e| {
        e.subject.as_ref().is_some_and(|s| s.node.name == "b")
            && hook_activity_of(e).is_some_and(|activity| {
                activity.hook.hook == "verify"
                    && activity.backend == "pebble"
                    && activity.envelope["session_id"].is_string()
            })
    }));
    assert!(
        !events
            .iter()
            .any(|e| e.custom().and_then(backend_event).is_some()),
        "no stage ran an agent of its own"
    );
    let hook_note = events
        .iter()
        .find(|e| {
            e.note().is_some_and(|note| {
                note.kind == "hook" && note.payload["point"] == "before_attempt"
            }) && e.subject.as_ref().is_some_and(|s| s.node.name == "b")
        })
        .expect("the hook note on b");
    let payload = &hook_note.note().expect("a note").payload;
    assert_eq!(payload["hooks"][0]["usage"]["requests"], 2);
}

/// A stage that runs its own native agent, guarded by an agent hook: the
/// hook's model turns and tool call go on the record under the hook's
/// identity, the stage's usage counts the stage's one turn alone, and the
/// public stream keeps the two apart, live and after replay.
#[tokio::test]
async fn an_agent_hooks_activity_is_kept_apart_from_the_stages_own() {
    let dir = RunDir::new("hooks-agent-apart");
    let (client, provider) = scripted(
        vec![
            // The hook: one look, one verdict.
            ScriptedCall::response(tool_call_response(
                "shell",
                "look",
                json!({"command": "cat marker.txt"}),
            )),
            ScriptedCall::response(text_response(r#"{"ok": true}"#)),
            // The stage's own agent: one answer.
            ScriptedCall::response(text_response("Done.")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo go > marker.txt"]
        b [prompt="Do the work."]
        start -> a -> b -> exit
    }"#,
        r#"
[[run.hooks]]
name = "verify"
event = "stage_start"
matcher = "^b$"
agent = "enabled"
prompt = "Read marker.txt and decide."
model = "test/model"
"#,
    );
    let (report, customs) = run_replayable(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(provider.requests().len(), 3);
    let events = replay_run_dir(dir.path()).await.expect("replays");
    let on_b = |e: &&RunEvent| e.subject.as_ref().is_some_and(|s| s.node.name == "b");
    // The stage's own accounting: one prompt, one answer's tokens.
    let finished = events
        .iter()
        .filter(on_b)
        .find_map(|e| match e.engine() {
            Some(Event::StepFinished { outcome, .. }) => Some(outcome.clone()),
            _ => None,
        })
        .expect("b's attempt");
    assert_eq!(finished.metrics.custom["pebble.prompts"], 1);
    assert_eq!(
        finished.metrics.custom["pebble.usage"]["tokens"]["input"],
        10
    );
    assert_eq!(
        finished.metrics.custom["pebble.usage"]["tokens"]["output"],
        5
    );
    // The hook's accounting, on the hook's record.
    let hook_note = events
        .iter()
        .filter(on_b)
        .find_map(|e| match e.note() {
            Some(note) if note.kind == "hook" && note.payload["point"] == "before_attempt" => {
                Some(note.payload.clone())
            }
            _ => None,
        })
        .expect("the hook note on b");
    assert_eq!(hook_note["hooks"][0]["name"], "verify");
    assert_eq!(hook_note["hooks"][0]["usage"]["requests"], 2);
    assert_eq!(hook_note["hooks"][0]["usage"]["tool_calls"], 1);
    assert_eq!(
        hook_note["hooks"][0]["usage"]["usage"]["tokens"]["input"],
        20
    );
    // Two agents, two sessions, two event families: the stage's under
    // `agent_activity`, the hook's under `hook_activity`, never mixed.
    let stage_sessions: BTreeSet<String> = events
        .iter()
        .filter(on_b)
        .filter_map(|e| e.custom().and_then(backend_event)?.session)
        .collect();
    let hook_sessions: BTreeSet<String> = events
        .iter()
        .filter(on_b)
        .filter_map(|e| match hook_activity_of(e) {
            Some(activity) if activity.hook.hook == "verify" => {
                activity.envelope["session_id"].as_str().map(str::to_owned)
            }
            _ => None,
        })
        .collect();
    assert_eq!(stage_sessions.len(), 1, "{stage_sessions:?}");
    assert_eq!(hook_sessions.len(), 1, "{hook_sessions:?}");
    assert!(stage_sessions.is_disjoint(&hook_sessions));
    let stage_tool_calls = events
        .iter()
        .filter(on_b)
        .filter(|e| {
            e.custom()
                .and_then(backend_event)
                .is_some_and(|a| a.envelope["event"].get("ToolCallStarted").is_some())
        })
        .count();
    assert_eq!(
        stage_tool_calls, 0,
        "the hook's tool call is not the stage's"
    );
    let hook_tool_calls = events
        .iter()
        .filter(|e| {
            hook_activity_of(e)
                .is_some_and(|activity| activity.envelope["event"].get("ToolCallStarted").is_some())
        })
        .count();
    assert_eq!(hook_tool_calls, 1);
    // Replay carries exactly what was recorded live.
    let live = customs.hook_activity().len();
    let replayed = events
        .iter()
        .filter(|e| hook_activity_of(e).is_some())
        .count();
    assert_eq!(replayed, live);
}

/// The hook activity a `step.progress.recorded` event's note reads as.
fn hook_activity_of(event: &RunEvent) -> Option<&HookActivity> {
    match event.parsed() {
        Some(Parsed::Note { hook_activity, .. }) => hook_activity.as_ref(),
        _ => None,
    }
}

/// A `pgrep -f` pattern for the tool process [`ticking_tool`] starts: the
/// `bash -c` the environment spawns, anchored at its start so the sandbox
/// plugin's sentinel wrapper, which carries the same command in its own
/// argument list and lives until the scope is released, does not match. The
/// first character of the id goes into a bracket class so the command line
/// carrying the pattern itself does not match either. The id starts with the
/// process id: nextest runs each test in its own process, where the counter
/// starts again, and another test's tool must not match.
fn tool_process_pattern(marker: &str) -> String {
    let (prefix, rest) = marker.split_at("hooks-leak-".len());
    let mut chars = rest.chars();
    let first = chars.next().expect("a marker has an id");
    format!("^bash -c echo {prefix}[{first}]{}", chars.as_str())
}

/// Whether the tool process for `marker` is running.
fn process_running(marker: &str) -> bool {
    Command::new("pgrep")
        .args(["-f", &tool_process_pattern(marker)])
        .output()
        .expect("pgrep runs")
        .status
        .success()
}

fn line_count(path: &Path) -> usize {
    read(path).lines().count()
}

/// The shell tool an agent hook is scripted to call: a loop that writes a
/// tick every 100 ms for thirty seconds, with `marker` on its command line so
/// the process can be found.
fn ticking_tool(marker: &str) -> Value {
    json!({
        "command": format!(
            "echo {marker} >/dev/null; for i in $(seq 1 300); do echo tick >> ticks.log; sleep 0.1; done"
        )
    })
}

/// An agent hook that runs out of time while its tool is still running stops
/// that tool and joins the agent before it fails open: the stage the hook
/// guarded starts with the tool's process gone and its file no longer
/// growing. The stage itself takes that reading, right after the hook
/// settled and before anything else could clean up.
#[tokio::test]
async fn an_agent_hook_timeout_stops_its_tool_before_failing_open() {
    let dir = RunDir::new("hooks-agent-timeout");
    let marker = format!("hooks-leak-{}-{}", process::id(), testkit::unique_id());
    let (client, _provider) = scripted(
        vec![
            ScriptedCall::response(tool_call_response("shell", "slow", ticking_tool(&marker))),
            ScriptedCall::response(text_response(r#"{"ok": false, "reason": "never reached"}"#)),
        ],
        vec![],
    );
    let probe = format!(
        "pgrep -f '{}' | xargs ps -o pid=,ppid=,stat=,command= -p > leak.txt 2>/dev/null || true; wc -l < ticks.log | tr -d ' ' > count1.txt; sleep 0.6; \
         wc -l < ticks.log | tr -d ' ' > count2.txt; echo b > b.txt",
        tool_process_pattern(&marker)
    );
    let graph = lower(
        &format!(
            r#"digraph W {{
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        b [shape=parallelogram, script="{probe}"]
        start -> a -> b -> exit
    }}"#
        ),
        r#"
[[run.hooks]]
name = "slow"
event = "stage_start"
matcher = "^b$"
agent = "enabled"
prompt = "Look around, then decide."
model = "test/model"
timeout = "1500ms"
"#,
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = workspace(&dir);
    assert!(ws.join("b.txt").exists(), "the hook failed open");
    let b = customs
        .hook_notes()
        .into_iter()
        .find(|(n, r)| n == "b" && r["point"] == "before_attempt")
        .map(|(_, r)| r)
        .expect("b's admission report");
    assert_eq!(b["hooks"][0]["state"], "failed_open", "{b}");
    assert!(
        b["hooks"][0]["message"]
            .as_str()
            .is_some_and(|m| m.contains("timed out")),
        "{b}"
    );
    // The interrupted agent's record: the one model turn that asked for the
    // tool, the tool call, and its events up to the session's end.
    assert_eq!(b["hooks"][0]["usage"]["requests"], 1, "{b}");
    assert_eq!(b["hooks"][0]["usage"]["tool_calls"], 1, "{b}");
    let activity = customs.hook_activity();
    assert!(
        activity.iter().any(|(node, a)| node == "b"
            && a["hook"]["hook"] == "slow"
            && a["envelope"]["event"].get("ToolCallStarted").is_some()),
        "{activity:?}"
    );
    assert!(
        activity
            .iter()
            .any(|(_, a)| variant(&a["envelope"]).as_deref() == Some("SessionEnded")),
        "the agent was shut down: {activity:?}"
    );
    assert!(
        line_count(&ws.join("ticks.log")) > 0,
        "the tool ran before the hook timed out"
    );
    assert_eq!(
        read(&ws.join("leak.txt")).trim(),
        "",
        "the tool's process was still running when the guarded stage started"
    );
    let (first, second) = (read(&ws.join("count1.txt")), read(&ws.join("count2.txt")));
    assert!(
        !first.trim().is_empty() && first == second,
        "the tool kept writing after the hook settled: {first} then {second}"
    );
}

/// A run cancelled while an agent hook's tool is running leaves no process
/// behind and no further writes: the hook's owner stops the tool and joins
/// the agent even though the driver dropped the callback that was awaiting
/// it.
#[tokio::test]
async fn a_cancelled_run_stops_an_agent_hooks_running_tool() {
    let dir = RunDir::new("hooks-agent-cancel");
    let marker = format!("hooks-leak-{}-{}", process::id(), testkit::unique_id());
    let (client, _provider) = scripted(
        vec![
            ScriptedCall::response(tool_call_response("shell", "slow", ticking_tool(&marker))),
            ScriptedCall::response(text_response(r#"{"ok": true}"#)),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > a.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        r#"
[[run.hooks]]
name = "slow"
event = "stage_start"
matcher = "^b$"
agent = "enabled"
prompt = "Look around, then decide."
model = "test/model"
"#,
    );
    let (rt, _customs) = runtime(&dir, Some(client));
    let ws = dir.path().join("scopes/invocation-0-scope-0/work");
    let ticks = ws.join("ticks.log");
    let report = host::run_configured(&rt, HostRun::new(graph), |handle, _| {
        // Cancel once the tool has started writing.
        tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(30);
            while !ticks.exists() && Instant::now() < deadline {
                sleep(Duration::from_millis(20)).await;
            }
            handle.cancel_root();
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(!ws.join("b.txt").exists(), "the guarded stage never ran");
    // The hook's owner finishes on its own after the callback was dropped:
    // the tool is stopped within the sandbox's grace, then the agent joined.
    let deadline = Instant::now() + Duration::from_secs(10);
    while process_running(&marker) && Instant::now() < deadline {
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !process_running(&marker),
        "the tool's process outlived the cancelled run"
    );
    let before = line_count(&ws.join("ticks.log"));
    assert!(before > 0, "the tool ran before the cancellation");
    sleep(Duration::from_millis(600)).await;
    assert_eq!(
        line_count(&ws.join("ticks.log")),
        before,
        "the tool kept writing after the run was cancelled"
    );
}

/// One agent-hook budget case: a hook on `b` with `max_tool_rounds` set to
/// `rounds` (or left at its default) and a model scripted with `calls`.
/// Returns the run directory (alive, so the workspace can be read), the
/// report, its records, and the provider.
async fn run_bounded_agent_hook(
    name: &str,
    rounds: Option<u32>,
    calls: Vec<ScriptedCall>,
) -> (RunDir, ExecutionReport, Arc<Customs>, Arc<ScriptedProvider>) {
    let dir = RunDir::new(name);
    let (client, provider) = scripted(calls, vec![]);
    let bound = rounds.map_or(String::new(), |n| format!("max_tool_rounds = {n}\n"));
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo go > marker.txt"]
        b [shape=parallelogram, script="echo b > b.txt"]
        start -> a -> b -> exit
    }"#,
        &format!(
            r#"
[[run.hooks]]
name = "verify"
event = "stage_start"
matcher = "^b$"
agent = "enabled"
prompt = "Investigate, then decide."
model = "test/model"
{bound}"#
        ),
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    (dir, report, customs, provider)
}

/// A scripted tool turn that leaves a mark for each round the agent ran.
fn round_call() -> ScriptedCall {
    ScriptedCall::response(tool_call_response(
        "shell",
        "round",
        json!({"command": "echo round >> rounds.log"}),
    ))
}

/// `max_tool_rounds` is the hard bound Fabro's loop has, not advice to the
/// model: an agent that keeps asking for tools runs that many model turns,
/// its tools run for all but the last, and the hook fails open with the
/// reference's warning, with the exhausted prompt's usage and events on the
/// record. Zero rounds asks the model nothing. A verdict inside the budget
/// decides as usual, and an unset bound leaves the default's room.
#[tokio::test]
async fn agent_hook_tool_rounds_are_a_hard_bound_that_fails_open() {
    let hook_note = |customs: &Customs| {
        customs
            .hook_notes()
            .into_iter()
            .find(|(n, r)| n == "b" && r["point"] == "before_attempt")
            .map(|(_, r)| r)
            .expect("b's admission report")
    };
    let exhausted = |report: &Value| {
        report["hooks"][0]["state"] == "failed_open"
            && report["hooks"][0]["message"]
                .as_str()
                .is_some_and(|m| m.contains("exhausted max tool rounds"))
    };

    // Zero rounds: no model call, no tool, proceed.
    let (dir, report, customs, provider) =
        run_bounded_agent_hook("hooks-rounds-zero", Some(0), vec![round_call(); 4]).await;
    let ws = workspace(&dir);
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(ws.join("b.txt").exists(), "the hook proceeded");
    assert_eq!(provider.requests().len(), 0, "no model call");
    assert!(!ws.join("rounds.log").exists(), "no tool ran");
    let b = hook_note(&customs);
    assert!(exhausted(&b), "{b}");

    // One round: one model turn, which asks for tools; none runs.
    let (dir, report, customs, provider) =
        run_bounded_agent_hook("hooks-rounds-one", Some(1), vec![round_call(); 4]).await;
    let ws = workspace(&dir);
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(ws.join("b.txt").exists());
    assert_eq!(provider.requests().len(), 1, "one model turn");
    assert!(
        !ws.join("rounds.log").exists(),
        "the refused turn's tool never ran"
    );
    let b = hook_note(&customs);
    assert!(exhausted(&b), "{b}");
    assert_eq!(b["hooks"][0]["usage"]["requests"], 1, "{b}");
    assert!(
        customs
            .hook_activity()
            .iter()
            .any(|(_, a)| variant(&a["envelope"]).as_deref() == Some("ToolRoundsExhausted")),
        "the exhaustion is on the record"
    );

    // Several rounds: as many model turns as rounds, tools for all but the
    // last, then proceed.
    let (dir, report, customs, provider) =
        run_bounded_agent_hook("hooks-rounds-three", Some(3), vec![round_call(); 6]).await;
    let ws = workspace(&dir);
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(ws.join("b.txt").exists());
    assert_eq!(provider.requests().len(), 3, "three model turns");
    assert_eq!(line_count(&ws.join("rounds.log")), 2, "two tool rounds ran");
    let b = hook_note(&customs);
    assert!(exhausted(&b), "{b}");
    assert_eq!(b["hooks"][0]["usage"]["requests"], 3, "{b}");
    assert_eq!(b["hooks"][0]["usage"]["tool_calls"], 2, "{b}");

    // A verdict inside the budget decides.
    let (dir, report, customs, provider) =
        run_bounded_agent_hook("hooks-rounds-verdict", Some(3), vec![
            round_call(),
            ScriptedCall::response(text_response(r#"{"ok": false, "reason": "no"}"#)),
        ])
        .await;
    let ws = workspace(&dir);
    assert_eq!(
        report.status,
        RunStatus::Failed,
        "the block fails the stage"
    );
    assert!(!ws.join("b.txt").exists());
    assert_eq!(provider.requests().len(), 2);
    assert_eq!(line_count(&ws.join("rounds.log")), 1);
    let b = hook_note(&customs);
    assert_eq!(b["hooks"][0]["state"], "executed", "{b}");
    assert_eq!(b["decision"]["reason"], "no", "{b}");

    // Unset: the default's fifty rounds leave room for a longer look.
    let (dir, report, customs, provider) =
        run_bounded_agent_hook("hooks-rounds-default", None, vec![
            round_call(),
            round_call(),
            round_call(),
            ScriptedCall::response(text_response(r#"{"ok": true}"#)),
        ])
        .await;
    let ws = workspace(&dir);
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(ws.join("b.txt").exists());
    assert_eq!(provider.requests().len(), 4);
    assert_eq!(line_count(&ws.join("rounds.log")), 3);
    let b = hook_note(&customs);
    assert_eq!(b["hooks"][0]["state"], "executed", "{b}");
}

// ── Tool hooks at the native boundary ──────────────────────────────────────

/// A `pre_tool_use` command hook blocks a real tool call: the file the shell
/// tool would write never exists, the model sees the denial, and the post
/// hooks fire for the calls that ran, with the tool output and the failure
/// message.
#[tokio::test]
async fn native_tool_hooks_block_pre_and_observe_post() {
    let dir = RunDir::new("hooks-tools");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(tool_call_response(
                "shell",
                "danger",
                json!({"command": "echo pwned > pwned.txt"}),
            )),
            ScriptedCall::response(tool_call_response(
                "shell",
                "fine",
                json!({"command": "echo fine > fine.txt && echo FINE_DONE"}),
            )),
            ScriptedCall::response(tool_call_response(
                "shell",
                "broken",
                json!({"command": "exit 7"}),
            )),
            ScriptedCall::response(text_response("Done.")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        agent [prompt="Do the work."]
        start -> agent -> exit
    }"#,
        r#"
[[run.hooks]]
name = "no-pwn"
event = "pre_tool_use"
matcher = "^shell$"
script = "if grep -q pwned \"$FABRO_HOOK_CONTEXT\"; then echo '{\"decision\":\"block\",\"reason\":\"no pwn\"}'; exit 2; fi"

[[run.hooks]]
name = "after"
event = "post_tool_use"
script = "echo post:$FABRO_NODE_ID >> tool-hooks.log; grep -o 'fine' \"$FABRO_HOOK_CONTEXT\" | head -1 >> tool-hooks.log"

[[run.hooks]]
name = "after-failure"
event = "post_tool_use_failure"
script = "echo failure >> tool-hooks.log"
"#,
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = workspace(&dir);
    assert!(!ws.join("pwned.txt").exists(), "the blocked tool never ran");
    assert!(ws.join("fine.txt").exists());
    let log = read(&ws.join("tool-hooks.log"));
    assert!(log.contains("post:agent"), "{log}");
    assert!(
        log.contains("fine"),
        "the tool output reached the post hook: {log}"
    );
    let requests = provider.requests();
    let denial = serde_json::to_string(&requests[1]).expect("request");
    assert!(
        denial.contains("no pwn"),
        "the model saw the block: {denial}"
    );
    let reports = customs.reports();
    let pre = reports
        .iter()
        .find(|(_, e)| e["event"] == "pre_tool_use")
        .expect("pre report")
        .1
        .clone();
    assert_eq!(pre["node"], "agent");
    assert_eq!(pre["report"]["decision"]["decision"], "block");
    assert_eq!(pre["report"]["hooks"][0]["name"], "no-pwn");
    let events: Vec<&str> = reports
        .iter()
        .filter_map(|(_, e)| e["event"].as_str())
        .collect();
    assert!(events.contains(&"post_tool_use"), "{events:?}");
    // The third call exits 7: Pebble reports a shell exit as a tool result,
    // not a tool failure, so the failure hook fires only when the tool itself
    // fails. Either way the log shows what ran.
    assert!(
        events.iter().filter(|e| **e == "pre_tool_use").count() >= 3,
        "{events:?}"
    );
}

/// A host that replaced the hook service, with nothing of the local one
/// installed, receives every phase a step asks itself: the start stage's
/// `sandbox_ready`, `run_start` and admission with the sandbox in place, the
/// native tool boundary before each call, the run's end and the scope's
/// release, once each. Its block at the tool boundary is enforced: the tool
/// never runs, the model sees the reason, and the report reaches the public
/// stream.
#[tokio::test]
async fn a_replacement_service_receives_every_step_driven_phase_once() {
    let dir = RunDir::new("hooks-host-native");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(tool_call_response(
                "shell",
                "danger",
                json!({"command": "echo pwned > pwned.txt"}),
            )),
            ScriptedCall::response(text_response("Done.")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        agent [prompt="Do the work."]
        start -> agent -> exit
    }"#,
        "",
    );
    let service = HostService::new(true, BTreeMap::new());
    let (report, customs) = run_hosted(&dir, graph, Some(client), service.clone()).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(
        !workspace(&dir).join("pwned.txt").exists(),
        "the host's block kept the tool from running"
    );
    for (point, node) in [
        (HookPoint::ScopeReady, "start"),
        (HookPoint::RunStarted, "start"),
        (HookPoint::BeforeVisit, "start"),
        (HookPoint::BeforeAttempt, "start"),
        (HookPoint::BeforeToolUse, "agent"),
        (HookPoint::RunFinished, ""),
        (HookPoint::ScopeReleased, ""),
    ] {
        assert_eq!(
            service.count(point, node),
            1,
            "{point:?} on `{node}`: {:?}",
            service.calls.lock().expect("not poisoned")
        );
    }
    assert_eq!(
        service.count(HookPoint::AfterToolUse, "agent"),
        0,
        "a blocked call has no result"
    );
    let calls = service.calls.lock().expect("not poisoned").clone();
    let start: Vec<HookPoint> = calls
        .iter()
        .filter(|(_, node)| node == "start")
        .map(|(point, _)| *point)
        .take(4)
        .collect();
    assert_eq!(
        start,
        [
            HookPoint::ScopeReady,
            HookPoint::RunStarted,
            HookPoint::BeforeVisit,
            HookPoint::BeforeAttempt
        ],
        "Fabro's order at the start stage"
    );
    let denial = serde_json::to_string(&provider.requests()[1]).expect("request");
    assert!(
        denial.contains("the host says no"),
        "the model saw the block: {denial}"
    );
    let pre = customs
        .reports()
        .into_iter()
        .find(|(_, e)| e["event"] == "pre_tool_use")
        .expect("the tool-boundary report")
        .1;
    assert_eq!(pre["node"], "agent");
    assert_eq!(pre["report"]["decision"]["decision"], "block");
    assert_eq!(pre["report"]["hooks"][0]["name"], "host-guard");
}

/// The ACP backend asks the same replaced service at its boundaries (the
/// permission request here; a reported tool call finishing, which the fake
/// agent never sends), and says what each tool hook the service reports as
/// configured can see, with nothing of the local service installed. A block
/// still answers with the rejecting option.
#[tokio::test]
async fn a_replacement_service_serves_acp_permission_requests_best_effort() {
    let dir = RunDir::new("hooks-host-acp");
    let agent = fake_agent(&dir);
    let permission = dir.path().join("permission.json");
    let graph = lower(
        &format!(
            r#"digraph W {{
        graph [goal="G", backend="acp", acp.command="python3 {}"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [prompt="Say hello"]
        start -> a -> exit
    }}"#,
            agent.display()
        ),
        "",
    );
    let graph = with_env(graph, &[
        ("ACP_MODE", "permission"),
        ("ACP_PERMISSION", permission.to_str().expect("utf-8")),
    ]);
    let service = HostService::new(
        true,
        BTreeMap::from([
            (HookPoint::BeforeToolUse, vec!["deny-all".to_owned()]),
            (HookPoint::AfterToolUse, vec!["after".to_owned()]),
        ]),
    );
    let (report, customs) = run_hosted(&dir, graph, None, service.clone()).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let answered: Value = serde_json::from_str(&read(&permission)).expect("permission answer");
    assert_eq!(answered["outcome"]["optionId"], "reject", "{answered}");
    assert_eq!(service.count(HookPoint::BeforeToolUse, "a"), 1);
    assert_eq!(
        service.count(HookPoint::AfterToolUse, "a"),
        0,
        "the fake agent reports no tool call finishing, so no post-tool point"
    );
    let pre = customs
        .reports()
        .into_iter()
        .find(|(_, e)| e["event"] == "pre_tool_use")
        .expect("the permission report")
        .1;
    assert_eq!(pre["report"]["decision"]["decision"], "block");
    assert_eq!(pre["report"]["hooks"][0]["name"], "host-guard");
    let warnings = customs.warnings();
    assert!(
        warnings.iter().any(|w| w["backend"] == "acp"
            && w["hook"] == "deny-all"
            && w["event"] == "pre_tool_use"
            && w["boundary"] == "session/request_permission"),
        "{warnings:?}"
    );
    assert!(
        warnings.iter().any(|w| w["backend"] == "acp"
            && w["hook"] == "after"
            && w["event"] == "post_tool_use"
            && w["boundary"] == "session/update"),
        "{warnings:?}"
    );
}

// ── Fidelity, threads, memory ───────────────────────────────────────────────

/// Two `full` nodes on one thread share a conversation: the second prompt
/// carries the first's history and no preamble. A `compact` node between
/// them gets the preamble and a fresh session. Events are attributed to the
/// node that used the session.
#[tokio::test]
async fn full_fidelity_nodes_continue_their_thread_and_others_start_fresh() {
    let dir = RunDir::new("threads-continue");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(text_response("planned")),
            ScriptedCall::response(text_response("implemented")),
            ScriptedCall::response(text_response("reviewed")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model", goal="Ship it"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        plan [prompt="Plan.", fidelity="full", thread_id="impl"]
        implement [prompt="Implement.", fidelity="full", thread_id="impl"]
        review [prompt="Review.", fidelity="summary:low"]
        start -> plan -> implement -> review -> exit
    }"#,
        "",
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    let first_user = message_text(
        requests[0]
            .messages()
            .iter()
            .find(|m| m.role() == Role::User)
            .expect("user"),
    );
    assert_eq!(first_user, "Plan.", "full fidelity: no preamble");
    // The second request carries the first exchange.
    let second: Vec<String> = requests[1].messages().iter().map(message_text).collect();
    assert!(second.iter().any(|m| m == "Plan."), "{second:?}");
    assert!(second.iter().any(|m| m.contains("planned")), "{second:?}");
    assert!(second.iter().any(|m| m == "Implement."), "{second:?}");
    // The review starts fresh with a low summary of both stages.
    let third: Vec<String> = requests[2].messages().iter().map(message_text).collect();
    assert!(!third.iter().any(|m| m == "Plan."), "{third:?}");
    let review_prompt = third.last().expect("review prompt");
    assert!(
        review_prompt.starts_with("Goal: Ship it\nRun ID:"),
        "{review_prompt}"
    );
    assert!(
        review_prompt.contains("Recent stages:\n- plan: succeeded"),
        "{review_prompt}"
    );
    assert!(
        review_prompt.contains("- implement: succeeded"),
        "{review_prompt}"
    );
    assert!(review_prompt.ends_with("\n\nReview."), "{review_prompt}");
    let threads = customs.threads();
    let of = |node: &str| {
        threads
            .iter()
            .find(|(n, _)| n == node)
            .map(|(_, v)| v.clone())
            .expect(node)
    };
    assert_eq!(of("plan")["thread"], "impl");
    assert_eq!(of("plan")["reused"], false);
    assert_eq!(of("implement")["reused"], true);
    assert_eq!(of("implement")["fidelity"], "full");
    assert_eq!(of("review")["fidelity"], "summary:low");
    assert_eq!(
        of("review")["thread"],
        "implement",
        "the previous node id is the fallback thread"
    );
    assert_eq!(of("review")["thread_source"], "previous");
    // Pebble events of the reused session are attributed to `implement`.
    let pebble: Vec<(String, Value)> = customs
        .all()
        .into_iter()
        .filter(|(_, v)| v["kind"] == "pebble")
        .collect();
    assert!(
        pebble
            .iter()
            .any(|(n, v)| n == "implement" && v["node"] == "implement")
    );
    assert!(
        !pebble
            .iter()
            .any(|(n, v)| n == "implement" && v["node"] == "plan")
    );
    // Per-stage metrics start at zero for the second node.
    let metrics = |name: &str| {
        report
            .state
            .history()
            .iter()
            .find(|r| r.name == name)
            .expect(name)
            .outcome
            .metrics
            .custom
            .clone()
    };
    assert_eq!(metrics("plan")["pebble.prompts"], 1);
    assert_eq!(metrics("implement")["pebble.prompts"], 1);
}

/// Edge fidelity beats node fidelity; `truncate` is the goal and run id; a
/// thread whose conversation was lost (the previous node failed) degrades a
/// later `full` node to `summary:high`.
#[tokio::test]
async fn edge_fidelity_wins_and_a_lost_thread_degrades_to_summary_high() {
    let dir = RunDir::new("threads-edge");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(text_response("not json at all")),
            ScriptedCall::response(text_response("second")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model", goal="G", default_fidelity="full", default_thread="t"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        first [prompt="First.", output_schema="routing", output_retries=0]
        second [prompt="Second."]
        start -> first
        first -> second [fidelity="truncate"]
        first -> second
        second -> exit
    }"#,
        "",
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(status_of(&report, "first").as_deref(), Some("failure"));
    let requests = provider.requests();
    let prompt = message_text(requests[1].messages().last().expect("prompt"));
    assert!(prompt.starts_with("Goal: G\nRun ID: "), "{prompt}");
    assert!(
        prompt.ends_with("\n\n\nSecond.") || prompt.ends_with("\n\nSecond."),
        "{prompt}"
    );
    assert!(
        !prompt.contains("Completed"),
        "truncate carries no stages: {prompt}"
    );
    let threads = customs.threads();
    let second = threads
        .iter()
        .find(|(n, _)| n == "second")
        .map(|(_, v)| v.clone())
        .expect("second");
    assert_eq!(second["fidelity"], "truncate");
    assert_eq!(second["fidelity_source"], "edge");
    assert_eq!(second["thread"], "t");

    // A lost thread: the first node failed, so a later full node on the
    // same thread degrades.
    let dir = RunDir::new("threads-lost");
    let (client, provider) = scripted(
        vec![
            ScriptedCall::response(text_response("not json at all")),
            ScriptedCall::response(text_response("recovered")),
        ],
        vec![],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model", goal="G", default_fidelity="full", default_thread="t"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        first [prompt="First.", output_schema="routing", output_retries=0]
        second [prompt="Second."]
        start -> first -> second -> exit
    }"#,
        "",
    );
    let (report, customs) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let second = customs
        .threads()
        .iter()
        .find(|(n, _)| n == "second")
        .map(|(_, v)| v.clone())
        .expect("second");
    assert_eq!(second["fidelity"], "summary:high");
    assert_eq!(second["fidelity_source"], "resume");
    assert_eq!(second["reused"], false);
    let prompt = message_text(provider.requests()[1].messages().last().expect("prompt"));
    assert!(
        prompt.contains("## Stage: first\n- Status: failed"),
        "{prompt}"
    );
}

/// A native session loads the profile's project documents from the Git root
/// down to the working directory; a prompt node reads the working directory
/// only, and `project_memory=false` reads nothing.
#[tokio::test]
async fn project_memory_follows_the_profile_and_the_node_kind() {
    let dir = RunDir::new("memory-paths");
    let ws = workspace(&dir);
    fs::create_dir_all(ws.join("sub")).expect("workspace");
    // The workspace is a Git repository whose root is `ws`; the run's working
    // directory is the workspace itself, so the walk is one level. Put a
    // parent-level file above to prove the walk starts at the Git root, not
    // above it.
    let git = Command::new("git")
        .args(["init", "-q"])
        .current_dir(&ws)
        .status();
    if !git.is_ok_and(|s| s.success()) {
        return;
    }
    fs::write(ws.join("AGENTS.md"), "Root rules.").expect("write");
    fs::write(ws.join("CLAUDE.md"), "Claude rules.").expect("write");
    fs::write(ws.join("GEMINI.md"), "Gemini rules.").expect("write");
    fs::write(dir.path().join("AGENTS.md"), "ABOVE THE ROOT").expect("write");
    let (client, provider) = scripted(
        vec![ScriptedCall::response(text_response("agent done"))],
        vec![
            ScriptedCompletion::response(text_response("prompt done")),
            ScriptedCompletion::response(text_response("quiet done")),
        ],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        agent [prompt="Agent."]
        summary [shape=tab, prompt="Summarize."]
        quiet [shape=tab, prompt="Quiet.", project_memory=false]
        start -> agent -> summary -> quiet -> exit
    }"#,
        "",
    );
    let (report, provider_calls) = {
        let (report, _) = run(&dir, graph, Some(client)).await;
        (report, provider)
    };
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    // The agent session's system prompt carries AGENTS.md and CLAUDE.md
    // (the test catalog's model is the anthropic profile), not GEMINI.md and
    // nothing from above the Git root.
    let agent_request = &provider_calls.requests()[0];
    let system = agent_request
        .messages()
        .iter()
        .filter(|m| m.role() == Role::System)
        .map(message_text)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(system.contains("Root rules."), "{system}");
    assert!(system.contains("Claude rules."), "{system}");
    assert!(!system.contains("Gemini rules."), "{system}");
    assert!(!system.contains("ABOVE THE ROOT"), "{system}");
    let completions = provider_calls.completion_requests();
    assert_eq!(completions.len(), 2);
    let summary_system = completions[0]
        .messages()
        .iter()
        .filter(|m| m.role() == Role::System)
        .map(message_text)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        summary_system.contains("Root rules.") && summary_system.contains("Claude rules."),
        "{summary_system}"
    );
    assert!(
        completions[1]
            .messages()
            .iter()
            .all(|m| m.role() != Role::System),
        "project_memory=false"
    );
}

/// A prompt node and a native session load project memory through the one
/// loader, so both see the same text within the budget: a file that crosses
/// the 32 KiB line is cut to what remains and ends with Pebble's marker, no
/// newline before it, and the files after it are skipped.
#[tokio::test]
async fn prompt_nodes_and_sessions_load_the_same_truncated_memory() {
    let dir = RunDir::new("memory-budget");
    let ws = workspace(&dir);
    fs::create_dir_all(&ws).expect("workspace");
    let big = "every rule in this file holds\n".repeat(1500);
    assert!(big.len() > 32 * 1024);
    fs::write(ws.join("AGENTS.md"), &big).expect("write");
    fs::write(ws.join("CLAUDE.md"), "Claude rules.").expect("write");
    let marker = "[Project instructions truncated at 32KB]";
    let expected = format!("{}{marker}", &big[..32 * 1024 - marker.len()]);
    let (client, provider) = scripted(
        vec![ScriptedCall::response(text_response("agent done"))],
        vec![ScriptedCompletion::response(text_response("prompt done"))],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        agent [prompt="Agent."]
        summary [shape=tab, prompt="Summarize."]
        start -> agent -> summary -> exit
    }"#,
        "",
    );
    let (report, _) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let system_of = |messages: &[Message]| {
        messages
            .iter()
            .filter(|m| m.role() == Role::System)
            .map(message_text)
            .collect::<Vec<_>>()
            .join("\n")
    };
    let prompt_system = system_of(provider.completion_requests()[0].messages());
    assert_eq!(
        prompt_system, expected,
        "the prompt node's system prompt is the loaded text"
    );
    let agent_system = system_of(provider.requests()[0].messages());
    assert!(
        agent_system.ends_with(&expected),
        "the session appends the same text: {}",
        &agent_system[agent_system.len().saturating_sub(120)..]
    );
    assert!(
        !agent_system.contains("Claude rules."),
        "the budget was spent"
    );
    assert_eq!(expected.len(), 32 * 1024);
}

/// Model request controls: `speed` and `max_tokens` reach the native
/// session's requests and a prompt node's request.
#[tokio::test]
async fn speed_and_max_tokens_reach_the_model_requests() {
    let dir = RunDir::new("controls-speed");
    let (client, provider) = scripted(
        vec![ScriptedCall::response(text_response("agent done"))],
        vec![ScriptedCompletion::response(text_response("prompt done"))],
    );
    let graph = lower(
        r#"digraph W {
        graph [backend="api", default_model="test/model"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        agent [prompt="Agent.", speed="fast", max_tokens=777]
        summary [shape=tab, prompt="Summarize.", speed="standard", max_tokens=555, project_memory=false]
        start -> agent -> summary -> exit
    }"#,
        "",
    );
    let (report, _) = run(&dir, graph, Some(client)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let agent = &provider.requests()[0];
    assert_eq!(agent.speed(), Some(Speed::Fast));
    assert_eq!(agent.max_output_tokens(), Some(777));
    let prompt = &provider.completion_requests()[0];
    assert_eq!(prompt.speed(), Some(Speed::Balanced));
    assert_eq!(prompt.max_output_tokens(), Some(555));
}

// ── ACP: best effort ────────────────────────────────────────────────────────

fn fake_agent(dir: &RunDir) -> PathBuf {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../acceptance/testdata/fake_acp_agent.py");
    let script =
        fs::read_to_string(&source).unwrap_or_else(|e| panic!("{}: {e}", source.display()));
    let path = dir.path().join("fake_acp_agent.py");
    fs::write(&path, script).expect("write the fake agent");
    path
}

fn with_env(mut graph: Graph, pairs: &[(&str, &str)]) -> Graph {
    for scope in &mut graph.body.scopes {
        for (key, value) in pairs {
            scope
                .env
                .insert((*key).into(), ir::ExprOrValue::Value(json!(value)));
        }
    }
    graph
}

/// Petri's scripted agent (`crates/attractor/steps/tests/testdata/
/// scripted_acp_agent.py`) speaks the tool boundaries the products speak:
/// a permission request for its write, and `tool_call_update` reports for
/// every call.
fn scripted_agent(dir: &RunDir) -> PathBuf {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../attractor/steps/tests/testdata/scripted_acp_agent.py");
    let script =
        fs::read_to_string(&source).unwrap_or_else(|e| panic!("{}: {e}", source.display()));
    let path = dir.path().join("scripted_acp_agent.py");
    fs::write(&path, script).expect("write the scripted agent");
    path
}

/// The permission exchanges the ACP client recorded: the `acp` envelopes
/// whose method is `session/request_permission`.
fn permission_exchanges(customs: &Customs) -> Vec<Value> {
    customs
        .all()
        .into_iter()
        .filter(|(_, v)| {
            v["kind"] == ENVELOPE_KIND && v["event"]["method"] == "session/request_permission"
        })
        .map(|(_, v)| v["event"].clone())
        .collect()
}

/// The two tool boundaries ACP offers, mapped: a `pre_tool_use` hook runs
/// at the permission request and a block answers with the rejecting
/// option, so the write never happens and the agent reports the call
/// failed, which runs `post_tool_use_failure`; a call the agent reports
/// finished runs `post_tool_use` with its output; a call the agent ran
/// without asking is warned once per hook and tool. The node says before
/// the agent starts what each hook can see.
#[tokio::test]
async fn acp_tool_hooks_are_best_effort_with_explicit_warnings() {
    let dir = RunDir::new("hooks-acp");
    let agent = scripted_agent(&dir);
    let permission = dir.path().join("permission.json");
    let graph = lower(
        &format!(
            r#"digraph W {{
        graph [goal="G", backend="acp", acp.command="python3 {}"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [prompt="Write the file"]
        start -> a -> exit
    }}"#,
            agent.display()
        ),
        r#"
[[run.hooks]]
name = "deny-writes"
event = "pre_tool_use"
matcher = "Write"
script = "exit 2"

[[run.hooks]]
name = "after"
event = "post_tool_use"
script = '''grep -o '"tool_output":"[^"]*"' "$FABRO_HOOK_CONTEXT" >> post.log'''

[[run.hooks]]
name = "after-failure"
event = "post_tool_use_failure"
script = '''grep -o '"error_message":"[^"]*"' "$FABRO_HOOK_CONTEXT" >> failed.log'''
"#,
    );
    let graph = with_env(graph, &[
        ("ACP_MODE", "tools"),
        ("ACP_PERMISSION", permission.to_str().expect("utf-8")),
    ]);
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    // The permission request was answered with the rejection: the write
    // never happened.
    let answered: Value = serde_json::from_str(&read(&permission)).expect("permission answer");
    assert_eq!(answered["outcome"]["outcome"], "selected");
    assert_eq!(answered["outcome"]["optionId"], "reject", "{answered}");
    assert!(
        !workspace(&dir).join("hello.txt").exists(),
        "the blocked write did not happen"
    );
    let exchanges = permission_exchanges(&customs);
    assert_eq!(exchanges.len(), 1, "{exchanges:?}");
    assert_eq!(exchanges[0]["outcome"]["optionId"], "reject");
    assert_eq!(exchanges[0]["tool_call_id"], "call-1");
    assert!(
        exchanges[0]["blocked"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "the block's reason rides on the exchange: {}",
        exchanges[0]
    );
    // The pre report is recorded as executed and blocking.
    let pre = customs
        .reports()
        .into_iter()
        .find(|(_, e)| e["event"] == "pre_tool_use")
        .expect("pre")
        .1;
    assert_eq!(pre["report"]["decision"]["decision"], "block");
    assert_eq!(pre["report"]["hooks"][0]["name"], "deny-writes");
    // The rejected write's failure ran the failure hook with the agent's
    // error; the read the agent reported finished ran the post hook with
    // its output.
    assert_eq!(
        read(&workspace(&dir).join("failed.log")).trim(),
        r#""error_message":"permission denied""#
    );
    assert_eq!(
        read(&workspace(&dir).join("post.log")).trim(),
        r##""tool_output":"# readme""##
    );
    let post_events: Vec<String> = customs
        .reports()
        .iter()
        .filter_map(|(_, e)| e["event"].as_str().map(str::to_owned))
        .filter(|event| event.starts_with("post_tool_use"))
        .collect();
    assert_eq!(post_events, ["post_tool_use_failure", "post_tool_use"]);
    // What each hook can see, said before the agent started; and the read
    // that ran past the pre hook, said once.
    let warnings = customs.warnings();
    assert!(
        warnings.iter().any(|w| w["backend"] == "acp"
            && w["hook"] == "deny-writes"
            && w["event"] == "pre_tool_use"
            && w["boundary"] == "session/request_permission"),
        "{warnings:?}"
    );
    for hook in ["after", "after-failure"] {
        assert!(
            warnings.iter().any(|w| w["backend"] == "acp"
                && w["hook"] == hook
                && w["event"] == "post_tool_use"
                && w["boundary"] == "session/update"),
            "{warnings:?}"
        );
    }
    let unasked: Vec<&Value> = warnings
        .iter()
        .filter(|w| {
            w["hook"] == "deny-writes"
                && w["boundary"] == "session/update"
                && w["message"]
                    .as_str()
                    .is_some_and(|m| m.contains("`Read README`"))
        })
        .collect();
    assert_eq!(unasked.len(), 1, "{warnings:?}");
}

/// A `pre_tool_use` hook that proceeds allows the call once, never always:
/// the next call of that kind asks again, so the hook keeps seeing every
/// write. Without a `pre_tool_use` hook the client allows always
/// (`petri-attractor-steps::acp`).
#[tokio::test]
async fn a_proceeding_pre_tool_use_hook_allows_once_so_every_call_asks() {
    let dir = RunDir::new("hooks-acp-allow-once");
    let agent = scripted_agent(&dir);
    let graph = lower(
        &format!(
            r#"digraph W {{
        graph [goal="G", backend="acp", acp.command="python3 {}"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [prompt="Write the file"]
        start -> a -> exit
    }}"#,
            agent.display()
        ),
        r#"
[[run.hooks]]
name = "watch-writes"
event = "pre_tool_use"
script = "exit 0"
"#,
    );
    let graph = with_env(graph, &[("ACP_MODE", "tools")]);
    let (report, customs) = run(&dir, graph, None).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(read(&workspace(&dir).join("hello.txt")), "hello from acp\n");
    let exchanges = permission_exchanges(&customs);
    assert_eq!(exchanges.len(), 1, "{exchanges:?}");
    assert_eq!(
        exchanges[0]["outcome"]["optionId"], "once",
        "allowed once, so the next write asks the hook again: {}",
        exchanges[0]
    );
    assert!(exchanges[0]["blocked"].is_null());
    let pre = customs
        .reports()
        .into_iter()
        .find(|(_, e)| e["event"] == "pre_tool_use")
        .expect("pre")
        .1;
    assert_eq!(pre["report"]["decision"]["decision"], "proceed");
    assert_eq!(pre["report"]["hooks"][0]["name"], "watch-writes");
}

/// The run-end hooks follow Fabro's `on_run_end`: a failed run fires
/// `run_failed` with the failure reason and never `run_complete`; then
/// `sandbox_cleanup` runs in the sandbox before it is released. Both run
/// while the workspace is still there.
#[tokio::test]
async fn run_failed_then_sandbox_cleanup_run_at_the_run_end_in_fabros_order() {
    let dir = RunDir::new("hooks-run-end");
    let host_log = dir.path().join("host.log");
    let graph = lower(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        prepare [shape=parallelogram, script="echo prepared"]
        broken [shape=parallelogram, script="echo boom >&2; exit 3", on_failure="exit"]
        start -> prepare -> broken -> exit
    }"#,
        &format!(
            r#"
[[run.hooks]]
event = "run_failed"
script = "echo run_failed:$(grep -o '\"failure_reason\":\"[^\"]*\"' \"$FABRO_HOOK_CONTEXT\" | cut -d'\"' -f4) >> hooks.log"

[[run.hooks]]
event = "sandbox_cleanup"
script = "echo sandbox_cleanup:$(basename $(pwd)) >> hooks.log"

[[run.hooks]]
event = "run_complete"
script = "echo run_complete >> {host}"
sandbox = false
"#,
            host = host_log.display()
        ),
    );
    let (report, _customs) = run(&dir, graph, None).await;
    assert_eq!(report.status, RunStatus::Failed);
    let log = read(&workspace(&dir).join("hooks.log"));
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.len(), 2, "{log}");
    assert!(
        lines[0].starts_with("run_failed:") && lines[0].len() > "run_failed:".len(),
        "the failure reason reaches the hook: {log}"
    );
    assert_eq!(lines[1], "sandbox_cleanup:work", "{log}");
    assert!(
        !host_log.exists(),
        "a failed run never fires run_complete: {}",
        read(&host_log)
    );
}

/// `parallel_start` fires once before a fork's branches and
/// `parallel_complete` once after the last branch joined, both naming the
/// parallel node; the branches (child invocations) run their own stage
/// hooks in between.
#[tokio::test]
async fn parallel_start_and_parallel_complete_surround_the_branches() {
    let dir = RunDir::new("hooks-parallel");
    let lowered = lower_all(
        r#"digraph W {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        fork [shape=component]
        a [shape=parallelogram, script="echo a >> order.log"]
        b [shape=parallelogram, script="echo b >> order.log"]
        join [shape=tripleoctagon]
        after [shape=parallelogram, script="cat order.log"]
        start -> fork
        fork -> a
        fork -> b
        a -> join
        b -> join
        join -> after -> exit
    }"#,
        r#"
[[run.hooks]]
event = "parallel_start"
script = "echo parallel_start:$FABRO_NODE_ID >> hooks.log"

[[run.hooks]]
event = "parallel_complete"
script = "echo parallel_complete:$FABRO_NODE_ID >> hooks.log"

[[run.hooks]]
event = "stage_complete"
matcher = "^(a|b|after)$"
script = "echo stage_complete:$FABRO_NODE_ID >> hooks.log"
"#,
    );
    let (report, customs) = run_coordinated(&dir, lowered).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let ws = dir.path().join("scopes/invocation-0-scope-0/work");
    let log = read(&ws.join("hooks.log"));
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.first().copied(), Some("parallel_start:fork"), "{log}");
    let complete = lines
        .iter()
        .position(|l| *l == "parallel_complete:fork")
        .unwrap_or_else(|| panic!("no parallel_complete: {log}"));
    for branch in ["stage_complete:a", "stage_complete:b"] {
        let at = lines
            .iter()
            .position(|l| *l == branch)
            .unwrap_or_else(|| panic!("no {branch}: {log}"));
        assert!(at < complete, "{branch} before parallel_complete: {log}");
    }
    assert_eq!(lines.last().copied(), Some("stage_complete:after"), "{log}");
    assert_eq!(
        lines.iter().filter(|l| l.starts_with("parallel_")).count(),
        2,
        "once each: {log}"
    );
    let events: Vec<_> = customs
        .reports()
        .into_iter()
        .filter(|(_, e)| e["event"] == "parallel_complete")
        .collect();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].1["report"]["hooks"][0]["state"], "executed");
}
