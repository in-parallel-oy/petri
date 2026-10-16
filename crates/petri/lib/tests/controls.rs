//! Readiness item 6 on the standalone host: the failure circuit breaker
//! (`loop_restart_signature_limit`), node visit totals across `loop_restart`,
//! the stall watchdog (`stall_timeout`), and the control service (pause at
//! admission, steering into a live stage). Agents are Fabro's simulated
//! stubs; commands and human gates are real.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use petri::attractor::{
    AGENT_KIND, CommandStep, HumanStep, StageStep, StubStep, WAIT_KIND, WORKFLOW_KIND,
};
use petri::engine::{EngineState, Event, EventRecord, RunError};
use petri::execution::controls::{ControlError, ControlService};
use petri::execution::events::{
    CollectingSink, EventProjector, EventSource, ViewEvent, replay_run_dir,
};
use petri::execution::host::{self, HostRun};
use petri::execution::inspect::inspect_run_dir;
use petri::execution::watchdog::{StallWatchdog, WatchdogTask};
use petri::execution::{
    CancelReason, CoordinatorEvent, CoordinatorRecord, ExecutionId, ExecutionObserver,
    InterviewDispatcher, InterviewReply, InterviewRequest, Interviewer,
};
use petri::executor::Retention;
use petri::frontend::fabro::Fabro;
use petri::frontend::{CompileInputs, Lowered};
use petri::ir::{FiringId, Graph, RunStatus, Status, Value};
use petri::steps::Answer;
use petri::{RunOptions, Runtime, driver};
use serde_json::json;
use testkit::RunDir;
use tokio::sync::Notify;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

fn runtime(dir: &RunDir) -> Runtime {
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_secs(2);
    options.retention = Retention::Never;
    options.echo = false;
    let mut registry = Runtime::standard().registry().clone();
    for kind in [&AGENT_KIND, &WAIT_KIND, &WORKFLOW_KIND] {
        registry.register_runner(Arc::new(StubStep::new((*kind).clone())));
    }
    registry.register(CommandStep);
    registry.register(HumanStep);
    registry.register(StageStep);
    Runtime::standard()
        .frontend(Fabro::new())
        .steps(registry)
        .options(options)
}

fn lower(rt: &Runtime, dir: &RunDir, text: &str) -> Lowered {
    let path = dir.path().join("wf.fabro");
    fs::write(&path, text).expect("write the workflow");
    let lowered = rt
        .check(&path, None, None, &CompileInputs::new())
        .expect("loads");
    assert!(
        lowered.graph.is_some(),
        "the workflow lowers: {:?}",
        lowered.diagnostics
    );
    lowered
}

/// Script a stub node's calls.
fn script(graph: &mut Graph, node: &str, calls: &Value) {
    let node = graph
        .body
        .nodes
        .iter_mut()
        .find(|n| n.name == node)
        .expect("the node");
    let Value::Object(config) = &mut node.step.config else {
        panic!("an object config");
    };
    config.insert("simulate".into(), json!({ "calls": calls.clone() }));
}

fn firings_of(report: &driver::ExecutionReport, node: &str) -> usize {
    report
        .state
        .history()
        .iter()
        .filter(|r| r.name == node)
        .count()
}

// ── Circuit breaker ─────────────────────────────────────────────────────────

const RESTART_LOOP: &str = r#"digraph G {
    graph [loop_restart_signature_limit=3]
    start [shape=Mdiamond]
    exit [shape=Msquare]
    work [prompt="x", max_visits=10]
    check [shape=diamond]
    start -> work -> check
    check -> exit [condition="context.done=true"]
    check -> work [loop_restart=true]
}"#;

/// Three repeats of one deterministic failure signature fail the run, and
/// the count survives the restart chain: each repeat is in its own
/// execution.
#[tokio::test]
async fn a_repeated_deterministic_failure_trips_the_breaker_across_restarts() {
    let dir = RunDir::new("breaker-trip");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    assert_eq!(
        graph
            .policy
            .loop_restart_signature_limit
            .map(NonZeroU32::get),
        Some(3)
    );
    script(
        &mut graph,
        "work",
        &json!([{
            "outcome": "failed",
            "failure_reason": "the tests failed on line 12",
            "context_updates": { "done": "false" }
        }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    assert_eq!(
        firings_of(&report, "work"),
        1,
        "the last execution ran work once"
    );
    let blocked = report.state.errors().iter().find_map(|error| match error {
        RunError::RoutingBlocked { reason, .. } => Some(reason.to_string()),
        _ => None,
    });
    let reason = blocked.expect("the route was blocked");
    assert!(
        reason.contains("deterministic failure cycle detected"),
        "{reason}"
    );
    assert!(reason.contains("repeated 3 times (limit 3)"), "{reason}");
    // Three executions ran (two restarts), one `work` each.
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 3, "{document:?}");
}

/// Success does not clear a signature's count.
#[tokio::test]
async fn a_success_between_failures_does_not_clear_the_count() {
    let dir = RunDir::new("breaker-no-clear");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    let fail = json!({
        "outcome": "failed",
        "failure_reason": "the tests failed",
        "context_updates": { "done": "false" }
    });
    script(
        &mut graph,
        "work",
        &json!([fail, fail, { "context_updates": { "done": "false" } }, fail]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 4, "fail, fail, success, fail");
}

/// A `loop_restart` edge taken by the failing node itself: a deterministic
/// failure may not restart; a transient one may.
#[tokio::test]
async fn a_restart_edge_admits_only_transient_failures() {
    const SELF_RESTART: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [prompt="x", max_visits=10]
        start -> work
        work -> work [loop_restart=true, condition="outcome=failed"]
        work -> exit
    }"#;
    let dir = RunDir::new("breaker-restart-deterministic");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, SELF_RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "outcome": "failed", "failure_reason": "syntax error" }, {}]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    let reason = report
        .state
        .errors()
        .iter()
        .find_map(|error| match error {
            RunError::RoutingBlocked { reason, .. } => Some(reason.to_string()),
            _ => None,
        })
        .expect("blocked");
    assert!(
        reason.contains("loop_restart blocked: failure_class=deterministic"),
        "{reason}"
    );

    let dir = RunDir::new("breaker-restart-transient");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, SELF_RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "outcome": "failed", "failure_reason": "connection refused by the registry" }, {}]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 2, "one restart, then success");
}

/// Real command diagnostics stay deterministic both on normal loop edges
/// (where repeats must trip the breaker) and on transient-only restarts.
#[tokio::test]
async fn command_diagnostics_cannot_bypass_the_breaker() {
    check_command_diagnostics(false).await;
}

#[tokio::test]
async fn command_diagnostics_cannot_take_a_restart_edge() {
    check_command_diagnostics(true).await;
}

async fn check_command_diagnostics(restart: bool) {
    for diagnostic in [
        "src/domain/order.py:42:5: F401 unused import",
        "src/domain/order.py:502:5: F401 unused import",
        "error: passed by value at crates/domain/src/lib.rs:500:13",
        "FAILED tests/test_retry.py::test_timeout",
        "error: undefined name budget",
    ] {
        let dir = RunDir::new("breaker-command-diagnostics");
        let rt = runtime(&dir);
        let workflow = format!(
            r#"digraph G {{
                    graph [loop_restart_signature_limit=3]
                    start [shape=Mdiamond]
                    exit [shape=Msquare]
                    work [shape=parallelogram, max_visits=4,
                          script="echo '{diagnostic}' >&2; exit 1"]
                    start -> work
                    work -> work [condition="outcome=failed", loop_restart={restart}]
                    work -> exit
                }}"#
        );
        let graph = lower(&rt, &dir, &workflow).graph.expect("lowers");
        let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
            .await
            .expect("the run completes");
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(
            firings_of(&report, "work"),
            if restart { 1 } else { 3 },
            "{diagnostic}, restart={restart}: {:?}",
            report.state.errors()
        );
        let reason = report
            .state
            .errors()
            .iter()
            .find_map(|error| match error {
                RunError::RoutingBlocked { reason, .. } => Some(reason.as_str()),
                _ => None,
            })
            .expect("the breaker blocks the route");
        if restart {
            assert!(
                reason.contains("loop_restart blocked: failure_class=deterministic"),
                "{reason}"
            );
        } else {
            assert!(
                reason.contains("deterministic failure cycle detected"),
                "{reason}"
            );
            assert!(reason.contains("repeated 3 times (limit 3)"), "{reason}");
        }
        let record = report
            .state
            .history()
            .iter()
            .find(|r| r.name == "work")
            .expect("command record");
        let info = record
            .outcome
            .status
            .failure_info()
            .expect("command failure");
        assert_eq!(info.class.as_str(), "exit_status:1");
        assert!(info.message.contains(diagnostic), "{}", info.message);
        assert_eq!(
            record.outcome.output["stdout"],
            json!(format!("{diagnostic}\n"))
        );
        assert_eq!(
            report.state.run_context().get("command.output"),
            Some(&json!(format!("{diagnostic}\n")))
        );
        let document = inspect_run_dir(dir.path()).await.expect("inspects");
        assert_eq!(document.executions.len(), 1, "no restart: {diagnostic}");
    }
}

/// Node visit totals survive a `loop_restart` while the context is
/// replaced: `max_visits=2` is reached on the third execution's visit, not
/// reset by the restart.
#[tokio::test]
async fn node_visit_totals_survive_a_restart_while_context_resets() {
    const RESTART: &str = r#"digraph G {
        graph [loop_restart_signature_limit=10]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        work [prompt="x", max_visits=2]
        check [shape=diamond]
        start -> work -> check
        check -> exit [condition="context.done=true"]
        check -> work [loop_restart=true]
    }"#;
    let dir = RunDir::new("breaker-visits");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{ "context_updates": { "done": "false", "seen": "yes" } }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);
    assert!(
        report
            .state
            .errors()
            .iter()
            .any(|error| matches!(error, RunError::BudgetExceeded { max_firings: 2, .. })),
        "{:?}",
        report.state.errors()
    );
    let document = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(document.executions.len(), 3);
    // The final execution started with an empty context: the restart
    // replaced it, and `seen` was written again by nothing.
    assert!(
        report.state.run_context().get("seen").is_none(),
        "{:?}",
        report.state.run_context().kv
    );
}

/// Both signature maps are restored on resume: a run crashed after the third
/// failure was recorded but before its route was resolved still blocks the
/// route when resumed.
#[tokio::test]
async fn the_breaker_state_is_restored_on_resume() {
    let dir = RunDir::new("breaker-resume");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, RESTART_LOOP);
    let mut graph = lowered.graph.expect("lowers");
    script(
        &mut graph,
        "work",
        &json!([{
            "outcome": "failed",
            "failure_reason": "the tests failed",
            "context_updates": { "done": "false" }
        }]),
    );
    let report = host::run_configured(&rt, HostRun::new(graph), |_, _| {})
        .await
        .expect("the run completes");
    assert_eq!(report.status, RunStatus::Failed);

    // Crash the last execution right after `work`'s final StepFinished:
    // the block decision is not in the log any more.
    let executions = dir.path().join("executions");
    let mut dirs: Vec<_> = fs::read_dir(&executions)
        .expect("executions")
        .map(|e| e.expect("entry").path())
        .collect();
    dirs.sort();
    let events = dirs.last().expect("an execution").join("events.jsonl");
    let text = fs::read_to_string(&events).expect("events");
    let lines: Vec<&str> = text.lines().collect();
    let cut = lines
        .iter()
        .position(|line| line.contains("\"step.finished\"") && line.contains("failure"))
        .expect("the failure record")
        + 1;
    fs::write(&events, format!("{}\n", lines[..cut].join("\n"))).expect("truncate");
    // The coordinator log must also forget the exit and the run's finish.
    let coordinator = dir.path().join("coordinator.jsonl");
    let text = fs::read_to_string(&coordinator).expect("coordinator log");
    let kept: Vec<&str> = text
        .lines()
        .filter(|line| {
            !(line.contains("\"run.finished\"")
                || line.contains("\"invocation.finished\"")
                || line.contains("\"scope.released\"")
                || (line.contains("\"execution.finished\"") && line.contains("terminal")))
        })
        .collect();
    fs::write(&coordinator, format!("{}\n", kept.join("\n"))).expect("rewrite");

    let resumed = host::resume(&rt).await.expect("resumes");
    assert_eq!(resumed.status, RunStatus::Failed);
    let reason = resumed
        .state
        .errors()
        .iter()
        .find_map(|error| match error {
            RunError::RoutingBlocked { reason, .. } => Some(reason.to_string()),
            _ => None,
        })
        .expect("the restored count blocks the route");
    assert!(reason.contains("repeated 3 times"), "{reason}");
}

// ── Stall watchdog ──────────────────────────────────────────────────────────

/// A run with no execution activity for the stall budget is cancelled.
#[tokio::test]
async fn an_idle_run_is_cancelled_by_the_watchdog() {
    const IDLE: &str = r#"digraph G {
        graph [stall_timeout="400ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        long [shape=parallelogram, script="sleep 20"]
        start -> long -> exit
    }"#;
    let dir = RunDir::new("watchdog-idle");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, IDLE);
    let graph = lowered.graph.expect("lowers");
    assert_eq!(graph.policy.stall_timeout, Some(Duration::from_millis(400)));
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let host_run = HostRun::new(graph)
        .observe(Arc::new(watchdog.clone()))
        .observe(projector.clone() as Arc<dyn ExecutionObserver>);
    let mut task = None;
    let started = Instant::now();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        task = Some(watchdog.start(handle));
    })
    .await
    .expect("the run completes");
    task.expect("started").stop().await;
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert_eq!(report.status, RunStatus::Cancelled);
    let stall = watchdog.tripped().expect("the watchdog fired");
    assert_eq!(stall.stall_timeout_ms, 400);
    assert!(stall.idle_ms >= 400);
    // The stall is a public event, derived from the durable cancel record
    // that carries it as the reason.
    let events = sink.events();
    let stalls: Vec<_> = events
        .iter()
        .filter(|e| matches!(e.view(), Some(ViewEvent::RunStalled { .. })))
        .collect();
    assert_eq!(stalls.len(), 1, "{events:#?}");
    assert!(matches!(
        stalls[0].view(),
        Some(ViewEvent::RunStalled {
            stall_timeout_ms: 400,
            idle_ms
        }) if *idle_ms >= 400
    ));
    assert!(
        events.iter().any(|e| matches!(
            e.coordinator(),
            Some(CoordinatorEvent::InvocationCancelRequested {
                reason: Some(CancelReason::StallTimeout {
                    stall_timeout_ms: 400,
                    ..
                }),
                ..
            })
        )),
        "the cancel names its reason: {events:#?}"
    );
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    assert!(
        replayed
            .iter()
            .any(|e| matches!(e.view(), Some(ViewEvent::RunStalled { .. }))),
        "the stall survives replay"
    );
}

/// A pause parks the watchdog: a run paused far longer than its stall
/// budget, with nothing running, is not a stall, and it finishes once
/// unpaused. The watchdog is stopped before the unpause: what the step
/// then takes to launch its sandbox is wall-clock time under test load,
/// not idleness, and the budget is not for measuring it. An unpause
/// restarting the full budget is the watchdog's own unit test.
#[tokio::test]
async fn a_pause_parks_the_watchdog() {
    const IDLE_WHEN_PAUSED: &str = r#"digraph G {
        graph [stall_timeout="400ms"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("watchdog-paused");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let graph = lower(&rt, &dir, IDLE_WHEN_PAUSED).graph.expect("lowers");
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let host_run = HostRun::new(graph)
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(watchdog.clone()));
    controls.pause();
    let paused = controls.clone();
    let monitor = watchdog.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle.clone());
        let task = monitor.start(handle);
        let paused = paused.clone();
        tokio::spawn(async move {
            // Three budgets paused, with nothing running.
            sleep(Duration::from_millis(1200)).await;
            task.stop().await;
            paused.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert!(watchdog.tripped().is_none(), "a paused run is not stalled");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
}

/// A pending question parks the watchdog: a gate that waits far longer than
/// the stall budget is not a stall, and the run continues once answered.
/// The answer stops the watchdog first, for the same reason the pause test
/// does: the step's sandbox launch is not idleness.
#[tokio::test]
async fn a_pending_question_parks_the_watchdog() {
    const GATED: &str = r#"digraph G {
        graph [stall_timeout="1s"]
        start [shape=Mdiamond]
        exit [shape=Msquare]
        gate [shape=hexagon, label="Go?"]
        go [shape=parallelogram, script="echo go"]
        start -> gate
        gate -> go [label="[Y] Yes"]
        go -> exit
    }"#;
    /// Answers after three budgets, stopping the watchdog it is handed first.
    struct Slow(Mutex<Option<WatchdogTask>>);
    #[async_trait::async_trait]
    impl Interviewer for Slow {
        async fn reply(&self, _: InterviewRequest, cancel: CancellationToken) -> InterviewReply {
            tokio::select! {
                () = sleep(Duration::from_secs(3)) => {
                    let task = self.0.lock().expect("the watchdog task").take();
                    if let Some(task) = task {
                        task.stop().await;
                    }
                    InterviewReply::Answered(Answer::choice("Y"))
                }
                () = cancel.cancelled() => InterviewReply::Cancelled,
            }
        }
    }
    let dir = RunDir::new("watchdog-parked");
    let rt = runtime(&dir);
    let lowered = lower(&rt, &dir, GATED);
    let graph = lowered.graph.expect("lowers");
    let watchdog = StallWatchdog::new(graph.policy.stall_timeout.expect("a budget"));
    let slow = Arc::new(Slow(Mutex::new(None)));
    let dispatcher = InterviewDispatcher::new(slow.clone());
    let host_run = HostRun::new(graph)
        .observe(Arc::new(watchdog.clone()))
        .observe(Arc::new(dispatcher.clone()));
    let report = host::run_configured(&rt, host_run, |handle, secrets| {
        dispatcher.wire(handle.clone(), secrets);
        *slow.0.lock().expect("the watchdog task") = Some(watchdog.start(handle));
    })
    .await
    .expect("the run completes");
    let receipt = dispatcher.shutdown().await;
    assert!(
        watchdog.tripped().is_none(),
        "a blocked run never stalls: {:?}",
        watchdog.tripped()
    );
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert!(receipt.is_clean(), "{:?}", receipt.errors);
}

// ── Controls ────────────────────────────────────────────────────────────────

/// Which nodes started, in order.
#[derive(Default)]
struct Starts(Mutex<Vec<String>>);

impl ExecutionObserver for Starts {
    fn on_engine_record(
        &self,
        _: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        state: &EngineState,
    ) {
        if let Event::StepStarted { firing, .. } = &record.event
            && let Some(name) = state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .map(|node| node.name.to_string())
        {
            self.0.lock().expect("not poisoned").push(name);
        }
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}

/// A paused run admits no new attempt; unpausing releases them, and the
/// held firing kept its identity (one visit, one attempt).
#[tokio::test]
async fn pause_holds_admission_and_unpause_releases_it() {
    const TWO: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a"]
        b [shape=parallelogram, script="echo b"]
        start -> a -> b -> exit
    }"#;
    let dir = RunDir::new("controls-pause");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, TWO);
    let starts = Arc::new(Starts::default());
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(projector.clone() as Arc<dyn ExecutionObserver>)
        .observe(starts.clone());
    controls.pause();
    let paused = controls.clone();
    let seen = starts.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle);
        tokio::spawn(async move {
            sleep(Duration::from_millis(700)).await;
            assert!(
                seen.0.lock().expect("not poisoned").is_empty(),
                "nothing started while paused"
            );
            assert!(paused.is_paused());
            paused.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["start", "a", "b", "exit"]);
    for record in report.state.history() {
        assert_eq!(record.attempt.raw(), 1, "{}", record.name);
    }
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    // The pause and the unpause are durable coordinator records, in order,
    // before the first attempt started.
    let events = sink.events();
    let notices: Vec<_> = events
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .collect();
    assert_eq!(notices.len(), 2, "{events:#?}");
    assert!(matches!(
        notices[0].coordinator(),
        Some(CoordinatorEvent::RunPaused)
    ));
    assert!(matches!(
        notices[1].coordinator(),
        Some(CoordinatorEvent::RunUnpaused)
    ));
    assert!(
        notices
            .iter()
            .all(|e| e.id.source == EventSource::Coordinator && e.subject.is_none()),
        "{notices:#?}"
    );
    assert!(notices[0].id.seq < notices[1].id.seq);
    let first_attempt = events
        .iter()
        .position(|e| matches!(e.engine(), Some(Event::StepStarted { .. })))
        .expect("an attempt started");
    let unpaused = events
        .iter()
        .position(|e| matches!(e.coordinator(), Some(CoordinatorEvent::RunUnpaused)))
        .expect("unpaused");
    assert!(unpaused < first_attempt, "{events:#?}");
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    let durable: Vec<_> = replayed
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .map(|e| e.coordinator().cloned())
        .collect();
    assert_eq!(
        durable,
        [
            Some(CoordinatorEvent::RunPaused),
            Some(CoordinatorEvent::RunUnpaused)
        ],
        "replay carries both controls"
    );
}

/// Cancellation stays responsive during a pause: the held firing settles as
/// cancelled and no attempt ever starts.
#[tokio::test]
async fn a_paused_run_can_still_be_cancelled() {
    const ONE: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="echo a > ran.txt"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("controls-pause-cancel");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, ONE);
    let host_run = HostRun::new(lowered.graph.expect("lowers")).observe(Arc::new(controls.clone()));
    controls.pause();
    let paused = controls.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        paused.wire(handle);
        tokio::spawn(async move {
            sleep(Duration::from_millis(300)).await;
            paused.cancel().expect("the run is live");
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(
        !report
            .state
            .history()
            .iter()
            .any(|r| r.name == "a" && r.attempt.raw() > 0 && r.outcome.status == Status::Success),
        "`a` never ran"
    );
    assert!(!dir.workspace().join("ran.txt").exists());
}

/// A steer reaches the named stage's live firing and never answers its
/// question; the answer still comes from the interviewer.
#[tokio::test]
async fn a_steer_reaches_the_stage_and_does_not_answer_its_question() {
    const GATE: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        gate [shape=hexagon, label="Go?"]
        go [shape=parallelogram, script="echo go"]
        start -> gate
        gate -> go [label="[Y] Yes"]
        go -> exit
    }"#;
    struct AfterSteer {
        controls: ControlService,
        steered:  Mutex<Option<Result<(), ControlError>>>,
    }
    #[async_trait::async_trait]
    impl Interviewer for AfterSteer {
        async fn reply(&self, request: InterviewRequest, _: CancellationToken) -> InterviewReply {
            // Steer the stage while its question is open, then answer.
            let result = self.controls.steer(&request.node, "hurry up").await;
            *self.steered.lock().expect("not poisoned") = Some(result);
            sleep(Duration::from_millis(200)).await;
            InterviewReply::Answered(Answer::choice("Y"))
        }
    }
    let dir = RunDir::new("controls-steer");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, &dir, GATE);
    let interviewer = Arc::new(AfterSteer {
        controls: controls.clone(),
        steered:  Mutex::new(None),
    });
    let dispatcher = InterviewDispatcher::new(interviewer.clone());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(dispatcher.clone()));
    let report = host::run_configured(&rt, host_run, |handle, secrets| {
        dispatcher.wire(handle.clone(), secrets);
        controls.wire(handle);
    })
    .await
    .expect("the run completes");
    let receipt = dispatcher.shutdown().await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        *interviewer.steered.lock().expect("not poisoned"),
        Some(Ok(()))
    );
    assert!(receipt.is_clean(), "{:?}", receipt.errors);
    assert_eq!(receipt.questions.len(), 1);
    let deliveries = report
        .state
        .log
        .events()
        .filter(|e| matches!(e, Event::ControlRequested { .. }))
        .count();
    assert_eq!(deliveries, 2, "the steer and the answer");
    assert_eq!(
        controls.steer("gate", "too late").await,
        Err(ControlError::NoSuchStage("gate".into()))
    );
    let _: BTreeMap<_, _> = controls.stages();
}

// ── Durable pause across resume ─────────────────────────────────────────────

const PAUSE_TWO: &str = r#"digraph G {
    start [shape=Mdiamond]
    exit [shape=Msquare]
    a [shape=parallelogram, script="echo a > a.txt"]
    b [shape=parallelogram, script="echo b > b.txt"]
    start -> a -> b -> exit
}"#;

/// Pause the run the moment `node` starts, and say when it has finished.
struct PauseOnStart {
    node:     &'static str,
    controls: ControlService,
    finished: Arc<Notify>,
}

impl ExecutionObserver for PauseOnStart {
    fn on_engine_record(
        &self,
        _: ExecutionId,
        record: &EventRecord,
        _recorded_at: u64,
        state: &EngineState,
    ) {
        let named = |firing: &FiringId| {
            state
                .firing_node(*firing)
                .and_then(|id| state.graph().node(id))
                .is_some_and(|node| node.name == self.node)
        };
        match &record.event {
            Event::StepStarted { firing, .. } if named(firing) => self.controls.pause(),
            Event::StepFinished { firing, .. } if named(firing) => self.finished.notify_one(),
            _ => {}
        }
    }

    fn on_lifecycle(&self, _: &CoordinatorRecord) {}
}

/// Run [`PAUSE_TWO`] until `a` has finished under a pause taken as `a`
/// started, then drop the run mid-flight: the crash. Returns the nodes that
/// were admitted before it.
async fn crash_while_paused(dir: &RunDir) -> Vec<String> {
    let controls = ControlService::new();
    let rt = runtime(dir).hooks(controls.hooks(None));
    let lowered = lower(&rt, dir, PAUSE_TWO);
    let finished = Arc::new(Notify::new());
    let starts = Arc::new(Starts::default());
    let host_run = HostRun::new(lowered.graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(Arc::new(PauseOnStart {
            node:     "a",
            controls: controls.clone(),
            finished: finished.clone(),
        }))
        .observe(starts.clone());
    let wired = controls.clone();
    let run = Box::pin(host::run_configured(&rt, host_run, |handle, _| {
        wired.wire(handle);
    }));
    tokio::select! {
        report = run => panic!(
            "the run finished while paused: {:?}",
            report.map(|report| report.status)
        ),
        () = async {
            finished.notified().await;
            // `b` reaches admission and is held; the pause record is durable.
            sleep(Duration::from_millis(400)).await;
        } => {}
    }
    assert!(controls.is_paused());
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["start", "a"], "`b` was held at admission");
    started
}

/// What a resumed run admitted during its first `wait`, and whether its
/// control service came back paused.
#[derive(Clone, Default)]
struct ResumeObservation {
    admitted: Vec<String>,
    paused:   bool,
}

/// Every pause and unpause the coordinator records, in order.
#[derive(Default)]
struct PauseRecords(Mutex<Vec<bool>>);

impl ExecutionObserver for PauseRecords {
    fn on_engine_record(&self, _: ExecutionId, _: &EventRecord, _: u64, _: &EngineState) {}

    fn on_lifecycle(&self, record: &CoordinatorRecord) {
        let paused = match record.body {
            CoordinatorEvent::RunPaused => true,
            CoordinatorEvent::RunUnpaused => false,
            _ => return,
        };
        self.0.lock().expect("not poisoned").push(paused);
    }
}

/// A pause and an unpause asked for back to back are recorded in that
/// order, so the run's record ends unpaused, as its gate does.
#[tokio::test]
async fn a_pause_and_an_immediate_unpause_are_recorded_in_order() {
    const SLOW: &str = r#"digraph G {
        start [shape=Mdiamond]
        exit [shape=Msquare]
        a [shape=parallelogram, script="sleep 0.5"]
        start -> a -> exit
    }"#;
    let dir = RunDir::new("controls-pause-order");
    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let records = Arc::new(PauseRecords::default());
    let host_run = HostRun::new(lower(&rt, &dir, SLOW).graph.expect("lowers"))
        .observe(Arc::new(controls.clone()))
        .observe(records.clone());
    let service = controls.clone();
    let report = host::run_configured(&rt, host_run, |handle, _| {
        service.wire(handle);
        let service = service.clone();
        tokio::spawn(async move {
            service.pause();
            service.unpause().await;
        });
    })
    .await
    .expect("the run completes");
    assert_eq!(report.status, RunStatus::Success);
    assert!(!controls.is_paused());
    assert_eq!(
        *records.0.lock().expect("not poisoned"),
        [true, false],
        "the pause, then the unpause"
    );
}

/// A pause survives the crash: the resumed run admits nothing until an
/// unpause arrives through the new control service, then finishes.
#[tokio::test]
async fn a_pause_survives_resume_and_holds_admission_until_unpaused() {
    let dir = RunDir::new("controls-pause-resume");
    crash_while_paused(&dir).await;

    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let starts = Arc::new(Starts::default());
    let observed = Arc::new(Mutex::new(ResumeObservation::default()));
    let releaser = controls.clone();
    let seen = starts.clone();
    let record = observed.clone();
    let report = host::resume_configured(
        &rt,
        Vec::new(),
        vec![Arc::new(controls.clone()), starts.clone()],
        |handle, _| {
            releaser.wire(handle);
            tokio::spawn(async move {
                sleep(Duration::from_millis(700)).await;
                *record.lock().expect("not poisoned") = ResumeObservation {
                    admitted: seen.0.lock().expect("not poisoned").clone(),
                    paused:   releaser.is_paused(),
                };
                releaser.unpause().await;
            });
        },
    )
    .await
    .expect("resumes");
    let observed = observed.lock().expect("not poisoned").clone();
    assert!(
        observed.paused,
        "the resumed control service starts paused: the last recorded control was a pause"
    );
    assert_eq!(
        observed.admitted,
        Vec::<String>::new(),
        "nothing was admitted while the resumed run waited for the unpause"
    );
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let started = starts.0.lock().expect("not poisoned").clone();
    assert_eq!(started, ["b", "exit"], "the resume continued at `b`");
    for record in report.state.history() {
        assert_eq!(record.attempt.raw(), 1, "{}", record.name);
    }
    // Both controls are durable coordinator records: the pause from the
    // first process, the unpause from the second.
    let replayed = replay_run_dir(dir.path()).await.expect("replays");
    let controls_seen: Vec<_> = replayed
        .iter()
        .filter(|e| {
            matches!(
                e.coordinator(),
                Some(CoordinatorEvent::RunPaused | CoordinatorEvent::RunUnpaused)
            )
        })
        .collect();
    assert_eq!(controls_seen.len(), 2, "{replayed:#?}");
    assert!(matches!(
        controls_seen[0].coordinator(),
        Some(CoordinatorEvent::RunPaused)
    ));
    assert!(matches!(
        controls_seen[1].coordinator(),
        Some(CoordinatorEvent::RunUnpaused)
    ));
    assert!(
        controls_seen
            .iter()
            .all(|e| e.id.source == EventSource::Coordinator),
        "{controls_seen:#?}"
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert!(!inspection.paused);
}

/// A resumed run that came back paused can still be cancelled: the held
/// firing settles as cancelled and no attempt starts.
#[tokio::test]
async fn a_paused_resumed_run_can_be_cancelled() {
    let dir = RunDir::new("controls-pause-resume-cancel");
    crash_while_paused(&dir).await;
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert!(inspection.paused, "the run directory records the pause");

    let controls = ControlService::new();
    let rt = runtime(&dir).hooks(controls.hooks(None));
    let starts = Arc::new(Starts::default());
    let canceller = controls.clone();
    let report = host::resume_configured(
        &rt,
        Vec::new(),
        vec![Arc::new(controls.clone()), starts.clone()],
        |handle, _| {
            canceller.wire(handle);
            tokio::spawn(async move {
                sleep(Duration::from_millis(300)).await;
                assert!(canceller.is_paused());
                canceller.cancel().expect("the run is live");
            });
        },
    )
    .await
    .expect("resumes");
    assert_eq!(report.status, RunStatus::Cancelled);
    assert_eq!(
        starts.0.lock().expect("not poisoned").clone(),
        Vec::<String>::new(),
        "no attempt started"
    );
    assert!(!dir.workspace().join("b.txt").exists());
}
