//! Fabro parallel nodes end to end on the host executor: every branch a
//! child invocation with its own context in the shared workspace, the
//! fan-in's envelopes in branch order, `parallel.results` published, and the
//! branch lifecycle across failure, duplicates, empty and labelled
//! `for_each` lists, repeated forks, nested forks, cancellation and resume.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, thread};

use attractor_steps::blobs::{BlobStore as _, holds_ref, hydrate};
use attractor_steps::parallel::fenced_item;
use attractor_steps::{
    AGENT_KIND, BLOBS_DIR, BranchStep, CommandStep, FanInStep, ForkStep, HUMAN_KIND,
    LocalBlobStore, STAGE_KIND, StubStep, WAIT_KIND, WORKFLOW_KIND,
};
use execution::events::{
    CollectingSink, EventProjector, ForkDisposition, ForkOccurrence, RunEvent, ViewEvent,
    replay_run_dir,
};
use execution::host::{self, HostRun};
use execution::inspect::{InvocationInspection, RunInspection, inspect_run_dir};
use execution::{CoordinatorEvent, ExecutionObserver};
use frontend::{CompileInputs, Lowered, NoFiles};
use frontend_attractor::kinds::BRANCH_ITEM_KEY;
use ir::StepEvent;
use runtime::driver::{BranchRole, ExecutionReport};
use runtime::engine::Event;
use runtime::executor::Retention;
use runtime::ir::{Graph, RunStatus};
use runtime::{RunOptions, Runtime};
use serde_json::{Value, json};
use testkit::{RunDir, output_of, status_of};

fn dot(body: &str) -> String {
    format!("digraph T {{\n  start [shape=Mdiamond]\n  exit [shape=Msquare]\n{body}\n}}")
}

#[expect(
    clippy::print_stderr,
    reason = "a graph that fails to lower explains itself in the test output"
)]
fn lower(text: &str) -> Lowered {
    let lowered = frontend_attractor::load("test.fabro", text, &NoFiles, &CompileInputs::new());
    for d in lowered.diagnostics.iter() {
        eprintln!("{d}");
    }
    assert!(lowered.graph.is_some(), "lowers");
    lowered
}

/// Real commands and the structural steps; stubs for the stages a model or a
/// person would run.
fn runtime(dir: &Path) -> Runtime {
    runtime_retaining(dir, Retention::Never)
}

fn runtime_retaining(dir: &Path, retention: Retention) -> Runtime {
    let mut options = RunOptions::new(dir);
    options.grace = Duration::from_secs(2);
    options.retention = retention;
    options.echo = false;
    let runtime = Runtime::standard().step(CommandStep);
    let mut registry = runtime.registry().clone();
    for kind in [AGENT_KIND, HUMAN_KIND, WAIT_KIND, WORKFLOW_KIND, STAGE_KIND] {
        registry.register_runner(Arc::new(StubStep::new(kind)));
    }
    registry.register(ForkStep);
    registry.register(BranchStep);
    registry.register(FanInStep);
    // The run services: the output store the fork and the fan-in offload to.
    attractor_steps::services(runtime.steps(registry).options(options))
}

async fn run(rt: &Runtime, lowered: Lowered) -> ExecutionReport {
    run_with(rt, lowered, |_| {}).await
}

async fn run_with(
    rt: &Runtime,
    lowered: Lowered,
    with_handle: impl FnOnce(execution::CoordinatorHandle),
) -> ExecutionReport {
    let graph: Graph = lowered.graph.expect("lowers");
    host::run_configured(
        rt,
        HostRun::new(graph).with_children(lowered.children),
        |handle, _| with_handle(handle),
    )
    .await
    .expect("the run completes")
}

/// `run_with`, with the public event stream projected live beside the run.
async fn run_projected(
    rt: &Runtime,
    lowered: Lowered,
    with_handle: impl FnOnce(execution::CoordinatorHandle),
) -> (ExecutionReport, Vec<RunEvent>) {
    let sink = Arc::new(CollectingSink::default());
    let projector = EventProjector::new(sink.clone());
    let graph: Graph = lowered.graph.expect("lowers");
    let report = host::run_configured(
        rt,
        HostRun::new(graph)
            .with_children(lowered.children)
            .observe(projector.clone() as Arc<dyn ExecutionObserver>),
        |handle, _| with_handle(handle),
    )
    .await
    .expect("the run completes");
    let receipt = projector.shutdown().await;
    assert!(receipt.is_clean(), "{receipt:?}");
    (report, sink.events())
}

/// The `step_custom` payloads of one `fabro.parallel.*` kind, in record
/// order.
fn customs(events: &[RunEvent], kind: &str) -> Vec<Value> {
    events
        .iter()
        .filter_map(|event| match event.engine() {
            Some(Event::StepProgressRecorded {
                ev: StepEvent::Custom(value),
                ..
            }) if value["kind"] == json!(kind) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

/// Every `branch_completed` as `(index, status tag)`, in record order.
fn branch_closes(events: &[RunEvent]) -> Vec<(u32, String)> {
    events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::BranchCompleted { result, .. }) => {
                Some((result.branch.index, result.status.tag().to_owned()))
            }
            _ => None,
        })
        .collect()
}

/// Every `fork_completed` as its disposition and `(index, status tag)` per
/// result, in record order.
fn fork_closes(events: &[RunEvent]) -> Vec<(ForkDisposition, Vec<(u32, String)>)> {
    events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::ForkCompleted {
                results,
                disposition,
                ..
            }) => Some((
                *disposition,
                results
                    .iter()
                    .map(|result| (result.branch.index, result.status.tag().to_owned()))
                    .collect(),
            )),
            _ => None,
        })
        .collect()
}

/// Every `fork_started` as its occurrence and branch indexes, in record
/// order.
fn fork_starts(events: &[RunEvent]) -> Vec<(ForkOccurrence, Vec<u32>)> {
    events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::ForkStarted {
                occurrence,
                branches,
            }) => Some((
                occurrence.clone(),
                branches.iter().map(|branch| branch.index).collect(),
            )),
            _ => None,
        })
        .collect()
}

/// Every `branch_completed` as its occurrence and branch index, in record
/// order.
fn branch_occurrences(events: &[RunEvent]) -> Vec<(ForkOccurrence, u32)> {
    events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::BranchCompleted { occurrence, result }) => {
                Some((occurrence.clone(), result.branch.index))
            }
            _ => None,
        })
        .collect()
}

/// Every `fork_completed` as its occurrence and result indexes, in record
/// order.
fn close_occurrences(events: &[RunEvent]) -> Vec<(ForkOccurrence, Vec<u32>)> {
    events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::ForkCompleted {
                occurrence,
                results,
                ..
            }) => Some((
                occurrence.clone(),
                results.iter().map(|result| result.branch.index).collect(),
            )),
            _ => None,
        })
        .collect()
}

/// The `occurrence.firing` a `fabro.parallel.*` payload names.
fn custom_occurrence(event: &Value) -> u64 {
    event["occurrence"]["firing"]
        .as_u64()
        .unwrap_or_else(|| panic!("an occurrence on {event}"))
}

/// The call slots of every child invocation, in declaration order.
fn child_slots(events: &[RunEvent]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event.coordinator() {
            Some(CoordinatorEvent::InvocationDeclared {
                call: Some(call), ..
            }) => Some(call.slot.to_string()),
            _ => None,
        })
        .collect()
}

/// How many `kill_requested` events the execution of `invocation` carries.
fn kills_in(events: &[RunEvent], invocation: u64) -> usize {
    events
        .iter()
        .filter(|event| {
            event
                .context
                .invocation
                .is_some_and(|id| id.raw() == invocation)
                && matches!(event.engine(), Some(Event::KillRequested { .. }))
        })
        .count()
}

/// The child invocation a branch delegate declared, by the target in its
/// call slot.
fn child_invocation(events: &[RunEvent], target: &str) -> u64 {
    events
        .iter()
        .find_map(|event| match event.coordinator() {
            Some(CoordinatorEvent::InvocationDeclared {
                invocation,
                call: Some(call),
                ..
            }) if call.slot.ends_with(&format!(":{target}")) => Some(invocation.raw()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a child for `{target}`"))
}

/// The replayed stream equals the live one, identity for identity, with
/// the live-only `observed_at` set aside.
async fn assert_replay_matches(dir: &Path, live: &[RunEvent]) {
    let mut live: Vec<RunEvent> = live
        .iter()
        .cloned()
        .map(|mut event| {
            event.observed_at = None;
            event
        })
        .collect();
    live.sort_by_key(|event| event.id);
    let mut replayed = replay_run_dir(dir).await.expect("replays");
    replayed.sort_by_key(|event| event.id);
    assert_eq!(
        replayed.len(),
        live.len(),
        "the replay has every live event"
    );
    for (from_replay, from_live) in replayed.iter().zip(&live) {
        assert_eq!(from_replay, from_live);
    }
}

/// Poll until `condition` holds, or fail after `limit`.
fn wait_until(limit: Duration, what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !condition() {
        assert!(Instant::now() < deadline, "{what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// How many coordinator records of `kind` the run dir holds so far.
fn coordinator_records(dir: &Path, kind: &str) -> usize {
    fs::read_to_string(dir.join("coordinator.jsonl"))
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains(kind))
        .count()
}

fn published_results(report: &ExecutionReport) -> Vec<Value> {
    report
        .state
        .run_context()
        .get("parallel.results")
        .and_then(Value::as_array)
        .cloned()
        .expect("parallel.results published")
}

async fn invocation_count(dir: &Path) -> usize {
    inspect_run_dir(dir)
        .await
        .expect("inspects")
        .invocations
        .len()
}

const FINDERS: &str = r#"
    fork [shape=component]
    a [shape=parallelogram, output_schema="routing", script="printf '%s' '{\"context_updates\":{\"output.finder\":{\"found\":\"a\"}}}'"]
    b [shape=parallelogram, output_schema="routing", script="printf '%s' '{\"context_updates\":{\"output.finder\":{\"found\":\"b\"}}}'"]
    merge [shape=tripleoctagon]
    report [shape=parallelogram, script="cat", stdin_source="context.parallel.results"]
    start -> fork
    fork -> a
    fork -> b
    a -> merge
    b -> merge
    merge -> report -> exit
"#;

#[tokio::test]
async fn static_branches_return_envelopes_that_never_merge_into_the_parent() {
    let dir = RunDir::new("parallel-static");
    let rt = runtime(dir.path());
    let report = run(&rt, lower(&dot(FINDERS))).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results.len(), 2);
    for (index, (envelope, found)) in results.iter().zip(["a", "b"]).enumerate() {
        assert_eq!(envelope["id"], json!(found));
        assert_eq!(envelope["index"], json!(index));
        assert_eq!(envelope["status"], json!("succeeded"));
        assert!(
            envelope.get("item_label").is_none(),
            "static branches have no label"
        );
        assert_eq!(
            envelope["context_updates"]["output.finder"]["found"],
            json!(found)
        );
        assert!(envelope["context_updates"]["command.output"].is_string());
    }
    let kv = report.state.run_context();
    assert!(
        kv.get("output.finder").is_none(),
        "branch keys stay out of the parent"
    );
    assert_eq!(kv.get("parallel.branch_count"), Some(&json!(2)));
    assert_eq!(output_of(&report, "merge"), Value::Array(results.clone()));
    let stdin: Value = serde_json::from_str(
        output_of(&report, "report")["stdout"]
            .as_str()
            .expect("the report read stdin"),
    )
    .expect("stdin is the results JSON");
    assert_eq!(stdin, Value::Array(results));
    assert_eq!(
        invocation_count(dir.path()).await,
        3,
        "the root and one child per branch"
    );
}

#[tokio::test]
async fn mixed_failures_join_partially_and_all_failed_fails_the_fan_in() {
    let dir = RunDir::new("parallel-mixed");
    let rt = runtime(dir.path());
    let report = run(
        &rt,
        lower(&dot(r#"
        fork [shape=component]
        ok [shape=parallelogram, script="echo fine"]
        bad [shape=parallelogram, script="echo boom; exit 3"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> ok
        fork -> bad
        ok -> merge
        bad -> merge
        merge -> exit
    "#)),
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        status_of(&report, "merge").as_deref(),
        Some("partial_success")
    );
    assert_eq!(status_of(&report, "bad").as_deref(), Some("failure"));
    let results = published_results(&report);
    assert_eq!(results[0]["status"], json!("succeeded"));
    assert_eq!(results[1]["id"], json!("bad"));
    assert_eq!(results[1]["status"], json!("failed"));
    assert_eq!(results[1]["index"], json!(1));
    // A failed branch keeps its identity and reports what it wrote (the
    // command's output), never stale success data, and never Petri's
    // `failure_class` bookkeeping: Fabro's branch path applies no stage
    // bookkeeping, so its branch updates carry no such key.
    assert_eq!(
        results[1]["context_updates"],
        json!({ "command.output": "boom\n" })
    );

    let dir = RunDir::new("parallel-all-failed");
    let rt = runtime(dir.path());
    let report = run(
        &rt,
        lower(&dot(r#"
        fork [shape=component]
        x [shape=parallelogram, script="exit 1"]
        y [shape=parallelogram, script="exit 2"]
        merge [shape=tripleoctagon]
        recover [shape=parallelogram, script="echo recovered"]
        start -> fork
        fork -> x
        fork -> y
        x -> merge
        y -> merge
        merge -> recover [condition="outcome=failed"]
        merge -> exit
        recover -> exit
    "#)),
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(status_of(&report, "merge").as_deref(), Some("failure"));
    let merge = report
        .state
        .history()
        .iter()
        .find(|r| r.name == "merge")
        .expect("merge ran");
    assert_eq!(
        merge
            .outcome
            .status
            .failure_info()
            .map(|f| f.message.as_str()),
        Some("All parallel branches failed")
    );
    assert_eq!(status_of(&report, "recover").as_deref(), Some("success"));
    assert_eq!(published_results(&report).len(), 2);
}

/// The reference contract's `branch_context_updates`: a key the branch
/// wrote back with the value the fork snapshot already had is reported,
/// because it is an outcome update, while a branch that wrote nothing
/// reports only the diff its command output made.
#[tokio::test]
async fn an_unchanged_write_back_is_reported_in_the_branch_updates() {
    let dir = RunDir::new("parallel-unchanged");
    let rt = runtime(dir.path());
    let report = run(
        &rt,
        lower(&dot(r#"
        seed [shape=parallelogram, output_schema="routing", script="printf '%s' '{\"context_updates\":{\"x\":\"same\"}}'"]
        fork [shape=component]
        a [shape=parallelogram, output_schema="routing", script="printf '%s' '{\"context_updates\":{\"x\":\"same\"}}'"]
        b [shape=parallelogram, script="true"]
        merge [shape=tripleoctagon]
        start -> seed -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#)),
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results.len(), 2, "{results:#?}");
    assert_eq!(
        results[0]["context_updates"],
        json!({
            "x": "same",
            "command.output": "{\"context_updates\":{\"x\":\"same\"}}",
        }),
        "{results:#?}"
    );
    assert_eq!(
        results[1]["context_updates"],
        json!({ "command.output": "" }),
        "{results:#?}"
    );
    assert_eq!(report.state.run_context().get("x"), Some(&json!("same")));
}

#[tokio::test]
async fn a_succeed_policy_promotes_a_failed_branch_before_collection() {
    let dir = RunDir::new("parallel-promote");
    let rt = runtime(dir.path());
    let report = run(
        &rt,
        lower(&dot(r#"
        fork [shape=component]
        ok [shape=parallelogram, script="echo fine"]
        soft [shape=parallelogram, script="echo boom; exit 3", on_failure="succeed"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> ok
        fork -> soft
        ok -> merge
        soft -> merge
        merge -> exit
    "#)),
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results[1]["id"], json!("soft"));
    assert_eq!(
        results[1]["status"],
        json!("succeeded"),
        "the branch's own policy promoted it before collection"
    );
    assert_eq!(status_of(&report, "merge").as_deref(), Some("success"));
    assert_eq!(status_of(&report, "soft").as_deref(), Some("success"));
}

/// Two branches with the same target are two branches of one fork
/// occurrence, told apart by index everywhere: the envelopes, the typed
/// branch events and the children's call slots.
#[tokio::test]
async fn duplicate_targets_are_separate_branches_with_their_own_index() {
    let dir = RunDir::new("parallel-duplicate");
    let rt = runtime(dir.path());
    let (report, events) = run_projected(
        &rt,
        lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="echo $RANDOM"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> a
        a -> merge
        merge -> exit
    "#)),
        |_| {},
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], json!("a"));
    assert_eq!(results[1]["id"], json!("a"));
    assert_eq!(results[0]["index"], json!(0));
    assert_eq!(results[1]["index"], json!(1));
    assert_eq!(status_of(&report, "a.branch1").as_deref(), Some("success"));
    assert_eq!(invocation_count(dir.path()).await, 3);

    let starts = fork_starts(&events);
    assert_eq!(starts.len(), 1, "{starts:#?}");
    let (occurrence, branches) = &starts[0];
    assert_eq!(*branches, vec![0, 1]);
    assert_eq!(occurrence.visit, 1);
    assert_eq!(branch_occurrences(&events), vec![
        (occurrence.clone(), 0),
        (occurrence.clone(), 1),
    ]);
    assert_eq!(close_occurrences(&events), vec![(
        occurrence.clone(),
        vec![0, 1]
    )]);
    let firing = occurrence.firing.raw();
    assert_eq!(child_slots(&events), vec![
        format!("branch:fork@{firing}:0:a"),
        format!("branch:fork@{firing}:1:a"),
    ]);
    assert_replay_matches(dir.path(), &events).await;
}

const FOR_EACH: &str = r#"
    plan [shape=parallelogram, output_schema="routing", script="printf '%s' '{\"context_updates\":{\"jobs\":JOBS}}'"]
    fan [shape=component, for_each="context.jobs", max_parallel=2]
    job [prompt="Do the job"]
    join [shape=tripleoctagon]
    report [shape=parallelogram, script="cat", stdin_source="context.parallel.results"]
    start -> plan -> fan -> job -> join -> report -> exit
"#;

/// An empty list: the fan-in joins zero results, no child runs, and the
/// public stream shows a fork with zero branches, live and on replay. The
/// placeholder clone the lowering fires to reach the fan-in is a synthetic
/// node with no branch role, never a branch.
#[tokio::test]
async fn an_empty_for_each_list_joins_with_no_branches_and_no_child() {
    let dir = RunDir::new("parallel-empty");
    let rt = runtime(dir.path());
    let (report, events) =
        run_projected(&rt, lower(&dot(&FOR_EACH.replace("JOBS", "[]"))), |_| {}).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(published_results(&report), Vec::<Value>::new());
    assert_eq!(
        report.state.run_context().get("parallel.branch_count"),
        Some(&json!(0))
    );
    assert_eq!(status_of(&report, "join").as_deref(), Some("success"));
    assert_eq!(status_of(&report, "report").as_deref(), Some("success"));
    // `cat` of the results JSON, byte for byte: no newline the script did
    // not write.
    assert_eq!(output_of(&report, "report")["stdout"], json!("[]"));
    assert_eq!(invocation_count(dir.path()).await, 1, "no child ran");

    let forks: Vec<(String, Vec<u32>)> = events
        .iter()
        .filter_map(|event| match event.view() {
            Some(ViewEvent::ForkStarted { branches, .. }) => Some((
                event
                    .subject
                    .as_ref()
                    .map(|subject| subject.node.name.to_string())
                    .unwrap_or_default(),
                branches.iter().map(|branch| branch.index).collect(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(forks, vec![("fan".to_owned(), Vec::new())]);
    assert_eq!(branch_closes(&events), Vec::new(), "no branch completed");
    assert_eq!(fork_closes(&events), vec![(
        ForkDisposition::Joined,
        Vec::new()
    )]);
    let completed = customs(&events, "attractor.parallel.completed");
    assert_eq!(completed.len(), 1, "{completed:#?}");
    assert_eq!(completed[0]["branch_count"], json!(0));
    assert!(
        customs(&events, "attractor.parallel.branch.started").is_empty()
            && customs(&events, "attractor.parallel.branch.completed").is_empty(),
        "the placeholder clone reports no branch lifecycle"
    );
    // The roles as the expansion left them: the fork's first events precede
    // the expansion, so the last subject of each node is the one to read.
    let role_of = |name: &str| {
        events
            .iter()
            .filter_map(|event| event.subject.as_ref())
            .rev()
            .find(|subject| subject.node.name == name)
            .map_or_else(
                || panic!("events of `{name}`"),
                |subject| subject.branch.clone(),
            )
    };
    assert_eq!(role_of("fan"), BranchRole::Fork { branches: 0 });
    assert_eq!(role_of("join"), BranchRole::Join {
        fork: events
            .iter()
            .filter_map(|event| event.subject.as_ref())
            .find(|subject| subject.node.name == "fan")
            .map(|subject| subject.node.id)
            .expect("the fork node"),
    });
    assert_eq!(
        role_of("job#0"),
        BranchRole::None,
        "the placeholder clone is no member of any branch"
    );
    assert_replay_matches(dir.path(), &events).await;
}

#[tokio::test]
async fn for_each_items_are_labelled_and_ordered_by_index() {
    let dir = RunDir::new("parallel-items");
    let rt = runtime(dir.path());
    let jobs = r#"[{\"name\":\"alpha\"},{\"label\":\"second\"},\"third\"]"#;
    let report = run(&rt, lower(&dot(&FOR_EACH.replace("JOBS", jobs)))).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results.len(), 3);
    for (index, (envelope, label)) in results.iter().zip(["alpha", "second", "2"]).enumerate() {
        assert_eq!(envelope["id"], json!("job"));
        assert_eq!(envelope["index"], json!(index));
        assert_eq!(envelope["item_label"], json!(label));
        assert_eq!(envelope["status"], json!("succeeded"));
    }
    assert_eq!(invocation_count(dir.path()).await, 4);
}

/// Routing output crosses the context offload boundary before the fork.
/// Stored and inline lists must declare the same ordered branch item data.
#[tokio::test]
async fn for_each_reads_artifact_backed_routing_items() {
    for width in [10, 50_000] {
        let dir = RunDir::new("parallel-stored-items");
        let rt = runtime(dir.path());
        let workflow = fan_out_over(3).replace("* 100", &format!("* {width}"));
        let (report, events) = run_projected(&rt, lower(&workflow), |_| {}).await;
        let jobs = &report.state.run_context().kv["jobs"];
        assert_eq!(holds_ref(jobs), width == 50_000);
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.errors()
        );
        let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
        let children = children_of(&inspection, "fan");
        assert_eq!(children.len(), 3);
        for (index, child) in children.iter().enumerate() {
            let item = &child.context[BRANCH_ITEM_KEY];
            let expected = json!({"name": format!("job-{index}"), "brief": "x".repeat(width)});
            assert_eq!(item, &json!(fenced_item(&expected)));
            assert!(holds_ref(&child.context["jobs"]));
        }
        assert_eq!(published_results(&report).len(), 3);
        let snapshots: Vec<_> = events
            .iter()
            .filter_map(|event| match event.engine() {
                Some(Event::TokenEmitted { token }) if token.payload.get("snapshot").is_some() => {
                    Some(&token.payload)
                }
                _ => None,
            })
            .collect();
        assert!(!snapshots.is_empty());
        assert!(
            snapshots
                .iter()
                .all(|payload| payload.get("items").is_none()),
            "branch tokens must not copy the whole list"
        );
        assert_replay_matches(dir.path(), &events).await;
    }
}

/// An explicitly stored empty list retains the zero-branch join behavior.
#[tokio::test]
async fn an_artifact_backed_empty_for_each_list_joins_without_children() {
    let dir = RunDir::new("parallel-stored-empty");
    let store = LocalBlobStore::new(dir.path().join(BLOBS_DIR));
    let digest = store.put(b"[]").await.expect("stores empty list");
    let source = json!(format!("blob://sha256/{digest}#json"));
    let jobs = source.to_string().replace('"', "\\\"");
    let report = run(
        &runtime(dir.path()),
        lower(&dot(&FOR_EACH.replace("JOBS", &jobs))),
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(report.state.run_context().get("jobs"), Some(&source));
    assert_eq!(published_results(&report), Vec::<Value>::new());
    assert_eq!(invocation_count(dir.path()).await, 1);
}

/// A missing blob, malformed JSON or a stored non-array must fail before
/// declaring children, rather than treating the reference text as items.
#[tokio::test]
async fn invalid_artifact_backed_for_each_sources_fail_without_children() {
    for bytes in [None, Some(b"not json".as_slice()), Some(b"{}".as_slice())] {
        let dir = RunDir::new("parallel-invalid-stored-items");
        let store = LocalBlobStore::new(dir.path().join(BLOBS_DIR));
        let digest = match bytes {
            Some(bytes) => store.put(bytes).await.expect("stores invalid source"),
            None => "0".repeat(64),
        };
        let source = json!(format!("blob://sha256/{digest}#json"));
        let jobs = source.to_string().replace('"', "\\\"");
        let report = run(
            &runtime(dir.path()),
            lower(&dot(&FOR_EACH.replace("JOBS", &jobs))),
        )
        .await;
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(invocation_count(dir.path()).await, 1);
        assert_eq!(status_of(&report, "fan").as_deref(), Some("failure"));
        assert!(
            report
                .state
                .history()
                .iter()
                .all(|r| !r.name.starts_with("job"))
        );
    }
}

/// The cap applies to the array length after hydration, never to the
/// reference string's length.
#[tokio::test]
async fn oversized_for_each_lists_fail_before_creating_branches() {
    for stored in [false, true] {
        let dir = RunDir::new("parallel-too-many-items");
        let mut source = json!(vec![0; 1_001]);
        if stored {
            let store = LocalBlobStore::new(dir.path().join(BLOBS_DIR));
            let digest = store
                .put(&serde_json::to_vec(&source).expect("JSON"))
                .await
                .expect("stores list");
            source = json!(format!("blob://sha256/{digest}#json"));
        }
        let jobs = source.to_string().replace('"', "\\\"");
        let report = run(
            &runtime(dir.path()),
            lower(&dot(&FOR_EACH.replace("JOBS", &jobs))),
        )
        .await;
        assert_eq!(report.status, RunStatus::Failed);
        assert_eq!(invocation_count(dir.path()).await, 1);
        assert_eq!(status_of(&report, "fan").as_deref(), Some("failure"));
        assert!(
            report
                .state
                .history()
                .iter()
                .all(|r| !r.name.starts_with("job"))
        );
    }
}

/// A fork visited twice is two fork occurrences: each visit's `fork_started`,
/// `branch_completed` and `fork_completed` name its own occurrence (the
/// fork's firing and visit), the `fabro.parallel.*` events and the
/// children's call slots name the same firing, and nothing about the second
/// visit matches the first by position.
#[tokio::test]
async fn a_repeated_fork_publishes_results_per_visit_with_its_own_children() {
    let dir = RunDir::new("parallel-repeat");
    let rt = runtime(dir.path());
    let (report, events) = run_projected(
        &rt,
        lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="cat round 2>/dev/null || echo first"]
        b [shape=parallelogram, script="echo b"]
        merge [shape=tripleoctagon]
        check [shape=parallelogram, output_schema="routing", script="if [ -f round ]; then printf '%s' '{\"context_updates\":{\"done\":\"yes\"}}'; else echo second > round; printf '%s' '{\"context_updates\":{\"done\":\"no\"}}'; fi"]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> check
        check -> exit [condition="context.done=yes"]
        check -> fork
    "#)),
        |_| {},
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let merges = report
        .state
        .history()
        .iter()
        .filter(|r| r.name == "merge")
        .count();
    assert_eq!(merges, 2, "the fork ran twice");
    // The second visit's results stand in the context; the branch `a`
    // saw the workspace the first visit wrote.
    let results = published_results(&report);
    assert_eq!(
        results[0]["context_updates"]["command.output"],
        json!("second\n")
    );
    assert_eq!(
        invocation_count(dir.path()).await,
        5,
        "two children per visit"
    );

    let starts = fork_starts(&events);
    assert_eq!(starts.len(), 2, "{starts:#?}");
    let (first, second) = (&starts[0].0, &starts[1].0);
    assert_eq!(first.fork, second.fork, "one fork node");
    assert_eq!(first.execution, second.execution);
    assert_ne!(first.firing, second.firing, "two occurrences");
    assert_eq!((first.visit, second.visit), (1, 2));
    assert_ne!(first.generation, second.generation);
    assert_eq!(starts[0].1, vec![0, 1]);
    assert_eq!(starts[1].1, vec![0, 1]);
    assert_eq!(branch_occurrences(&events), vec![
        (first.clone(), 0),
        (first.clone(), 1),
        (second.clone(), 0),
        (second.clone(), 1),
    ]);
    assert_eq!(close_occurrences(&events), vec![
        (first.clone(), vec![0, 1]),
        (second.clone(), vec![0, 1]),
    ]);
    // The Fabro events of each visit name the same occurrence, by the
    // fork's firing in the execution.
    for kind in [
        "attractor.parallel.branch.started",
        "attractor.parallel.branch.completed",
    ] {
        let mut firings: Vec<u64> = customs(&events, kind)
            .iter()
            .map(custom_occurrence)
            .collect();
        firings.sort_unstable();
        assert_eq!(
            firings,
            vec![
                first.firing.raw(),
                first.firing.raw(),
                second.firing.raw(),
                second.firing.raw()
            ],
            "{kind}"
        );
    }
    let completed = customs(&events, "attractor.parallel.completed");
    assert_eq!(
        completed.iter().map(custom_occurrence).collect::<Vec<_>>(),
        vec![first.firing.raw(), second.firing.raw()]
    );
    assert_eq!(completed[0]["fork"], json!("fork"));
    // Each child's call slot names its visit's occurrence.
    let (first_firing, second_firing) = (first.firing.raw(), second.firing.raw());
    assert_eq!(child_slots(&events), vec![
        format!("branch:fork@{first_firing}:0:a"),
        format!("branch:fork@{first_firing}:1:b"),
        format!("branch:fork@{second_firing}:0:a"),
        format!("branch:fork@{second_firing}:1:b"),
    ]);
    assert_replay_matches(dir.path(), &events).await;
}

/// An inner fork inside a branch is its own occurrence in the branch's child
/// execution: the outer and inner events never share an occurrence, and the
/// Fabro completion events name their own fork.
#[tokio::test]
async fn a_nested_fork_runs_inside_its_branch_and_reports_its_own_results() {
    let dir = RunDir::new("parallel-nested");
    let rt = runtime(dir.path());
    let (report, events) = run_projected(
        &rt,
        lower(&dot(r#"
        outer [shape=component]
        x [shape=parallelogram, script="echo x"]
        inner [shape=component]
        p [shape=parallelogram, script="echo p"]
        q [shape=parallelogram, script="echo q"]
        inner_join [shape=tripleoctagon]
        outer_join [shape=tripleoctagon]
        start -> outer
        outer -> x
        outer -> inner
        inner -> p
        inner -> q
        p -> inner_join
        q -> inner_join
        x -> outer_join
        inner_join -> outer_join
        outer_join -> exit
    "#)),
        |_| {},
    )
    .await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let results = published_results(&report);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["id"], json!("x"));
    assert_eq!(results[1]["id"], json!("inner"));
    assert_eq!(results[1]["status"], json!("succeeded"));
    let inner = &results[1]["context_updates"];
    assert_eq!(inner["parallel.branch_count"], json!(2));
    let inner_results = inner["parallel.results"].as_array().expect("inner results");
    assert_eq!(inner_results[0]["id"], json!("p"));
    assert_eq!(inner_results[1]["id"], json!("q"));
    assert_eq!(
        inner_results[1]["context_updates"]["command.output"],
        json!("q\n")
    );
    // Root, x, inner, p, q.
    assert_eq!(invocation_count(dir.path()).await, 5);

    // The outer fork starts first; the inner one, inside branch 1's child,
    // starts and closes before the outer join fires.
    let starts = fork_starts(&events);
    assert_eq!(starts.len(), 2, "{starts:#?}");
    let (outer, inner) = (&starts[0].0, &starts[1].0);
    assert_eq!(outer.execution.raw(), 0, "the outer fork is in the root");
    assert_ne!(
        inner.execution, outer.execution,
        "the inner fork runs in its branch's child"
    );
    assert_ne!(inner.fork, outer.fork);
    assert_eq!(branch_occurrences(&events), vec![
        (inner.clone(), 0),
        (inner.clone(), 1),
        (outer.clone(), 0),
        (outer.clone(), 1),
    ]);
    assert_eq!(close_occurrences(&events), vec![
        (inner.clone(), vec![0, 1]),
        (outer.clone(), vec![0, 1]),
    ]);
    let completed = customs(&events, "attractor.parallel.completed");
    assert_eq!(completed.len(), 2, "{completed:#?}");
    let of = |fork: &str| {
        completed
            .iter()
            .find(|event| event["fork"] == json!(fork))
            .cloned()
            .unwrap_or_else(|| panic!("`{fork}` completed: {completed:#?}"))
    };
    assert_eq!(custom_occurrence(&of("outer")), outer.firing.raw());
    assert_eq!(custom_occurrence(&of("inner")), inner.firing.raw());
    assert_replay_matches(dir.path(), &events).await;
}

#[tokio::test]
async fn cancelling_the_run_settles_every_branch_child() {
    let dir = RunDir::new("parallel-cancel");
    let rt = runtime(dir.path());
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let lowered = lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="touch started_a; sleep 30; touch finished_a"]
        b [shape=parallelogram, script="touch started_b; sleep 30; touch finished_b"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#));
    let report = run_with(&rt, lowered, |handle| {
        let workspace = workspace.clone();
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !(workspace.join("started_a").exists() && workspace.join("started_b").exists()) {
                assert!(Instant::now() < deadline, "both branches started");
                thread::sleep(Duration::from_millis(20));
            }
            handle.cancel_root();
        });
    })
    .await;
    assert_eq!(report.status, RunStatus::Cancelled);
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(inspection.invocations.len(), 3);
    for invocation in &inspection.invocations {
        assert_eq!(
            invocation.status, "finished",
            "every branch settled before the run ended: {invocation:?}"
        );
    }
    assert!(!workspace.join("finished_a").exists());
    assert!(!workspace.join("finished_b").exists());
    assert_eq!(status_of(&report, "a").as_deref(), Some("cancelled"));
}

/// Two running branches, one polite cancel: both children settle cancelled
/// without a kill, every branch reports a `cancelled` disposition after its
/// `started`, no group completion is claimed (Fabro emits none either), and
/// the typed stream still closes the fork as cancelled with both branches,
/// live and on replay.
#[tokio::test]
async fn a_clean_cancel_during_work_closes_the_branches_and_the_fork() {
    let dir = RunDir::new("parallel-cancel-clean");
    let rt = runtime(dir.path());
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let lowered = lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="touch started_a; sleep 30"]
        b [shape=parallelogram, script="touch started_b; sleep 30"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#));
    let (report, events) = run_projected(&rt, lowered, |handle| {
        let workspace = workspace.clone();
        thread::spawn(move || {
            wait_until(Duration::from_secs(20), "both branches started", || {
                workspace.join("started_a").exists() && workspace.join("started_b").exists()
            });
            handle.cancel_root();
        });
    })
    .await;
    assert_eq!(report.status, RunStatus::Cancelled);

    let started = customs(&events, "attractor.parallel.branch.started");
    assert_eq!(started.len(), 2, "{started:#?}");
    let completed = customs(&events, "attractor.parallel.branch.completed");
    assert_eq!(completed.len(), 2, "{completed:#?}");
    for event in &completed {
        assert!(event["index"].is_u64(), "{event}");
        assert_eq!(event["started"], json!(true), "{event}");
        assert_eq!(event["disposition"], json!("cancelled"), "{event}");
        assert_eq!(event["status"], json!("failed"), "{event}");
        assert!(event["invocation"].is_u64(), "{event}");
    }
    assert!(
        customs(&events, "attractor.parallel.completed").is_empty(),
        "the fan-in never ran, so no group completion is claimed"
    );
    assert_eq!(branch_closes(&events), vec![
        (0, "cancelled".to_owned()),
        (1, "cancelled".to_owned()),
    ]);
    assert_eq!(fork_closes(&events), vec![(
        ForkDisposition::Cancelled,
        vec![(0, "cancelled".to_owned()), (1, "cancelled".to_owned()),]
    )]);
    // The closure names the occurrence the start announced, as do the
    // branches and the Fabro events.
    let starts = fork_starts(&events);
    assert_eq!(starts.len(), 1);
    let occurrence = &starts[0].0;
    assert_eq!(close_occurrences(&events), vec![(
        occurrence.clone(),
        vec![0, 1]
    )]);
    assert!(
        branch_occurrences(&events)
            .iter()
            .all(|(branch, _)| branch == occurrence)
    );
    assert!(
        completed
            .iter()
            .all(|event| custom_occurrence(event) == occurrence.firing.raw())
    );
    for invocation in 0..3 {
        assert_eq!(
            kills_in(&events, invocation),
            0,
            "a clean cancel never escalates invocation {invocation}"
        );
    }
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(inspection.invocations.len(), 3);
    assert!(
        inspection
            .invocations
            .iter()
            .all(|invocation| invocation.status == "finished")
    );
    assert_replay_matches(dir.path(), &events).await;
}

/// One slot, two branches: the cancel arrives while the second child is
/// still queued. Its branch reports `started: false` and never reports a
/// start; the child is still finished as cancelled, without ever running an
/// attempt; the fork closes with both branches.
#[tokio::test]
async fn a_cancel_before_admission_records_a_branch_that_never_started() {
    let dir = RunDir::new("parallel-cancel-queued");
    let rt = runtime(dir.path());
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let lowered = lower(&dot(r#"
        fork [shape=component, max_parallel=1]
        a [shape=parallelogram, script="touch started_a; sleep 30"]
        b [shape=parallelogram, script="touch started_b; sleep 30"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#));
    let (report, events) = run_projected(&rt, lowered, |handle| {
        let workspace = workspace.clone();
        thread::spawn(move || {
            wait_until(Duration::from_secs(20), "branch a started", || {
                workspace.join("started_a").exists()
            });
            handle.cancel_root();
        });
    })
    .await;
    assert_eq!(report.status, RunStatus::Cancelled);
    assert!(!workspace.join("started_b").exists(), "b never ran");

    let started = customs(&events, "attractor.parallel.branch.started");
    assert_eq!(started.len(), 1, "{started:#?}");
    assert_eq!(started[0]["branch"], json!("a"));
    let completed = customs(&events, "attractor.parallel.branch.completed");
    assert_eq!(completed.len(), 2, "{completed:#?}");
    let of = |branch: &str| {
        completed
            .iter()
            .find(|event| event["branch"] == json!(branch))
            .cloned()
            .unwrap_or_else(|| panic!("`{branch}` completed: {completed:#?}"))
    };
    assert_eq!(of("a")["started"], json!(true));
    assert_eq!(of("a")["disposition"], json!("cancelled"));
    assert_eq!(of("b")["started"], json!(false));
    assert_eq!(of("b")["disposition"], json!("cancelled"));
    assert_eq!(of("b")["status"], json!("failed"));
    assert_eq!(fork_closes(&events), vec![(
        ForkDisposition::Cancelled,
        vec![(0, "cancelled".to_owned()), (1, "cancelled".to_owned()),]
    )]);
    // The queued child was finished as cancelled without an attempt.
    let child_b = child_invocation(&events, "b");
    assert!(events.iter().any(|event| matches!(
        event.coordinator(),
        Some(CoordinatorEvent::InvocationFinished { invocation, result })
            if invocation.raw() == child_b && result.status == RunStatus::Cancelled
    )));
    assert!(
        !events.iter().any(|event| {
            event
                .context
                .invocation
                .is_some_and(|id| id.raw() == child_b)
                && matches!(event.engine(), Some(Event::StepStarted { .. }))
        }),
        "no attempt of b's child started"
    );
    assert_replay_matches(dir.path(), &events).await;
}

/// The cancel arrives after one branch finished and before the fan-in: the
/// finished branch keeps its success, the other is cancelled, the fork closes
/// as cancelled with both results, and `parallel.results` is never published.
#[tokio::test]
async fn a_cancel_before_the_fan_in_keeps_the_finished_branch_result() {
    let dir = RunDir::new("parallel-cancel-before-join");
    let rt = runtime(dir.path());
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let lowered = lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="echo a"]
        b [shape=parallelogram, script="touch started_b; sleep 30"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#));
    let run_dir = dir.path().to_path_buf();
    let (report, events) = run_projected(&rt, lowered, |handle| {
        let workspace = workspace.clone();
        thread::spawn(move || {
            wait_until(Duration::from_secs(20), "a finished and b started", || {
                workspace.join("started_b").exists()
                    && coordinator_records(&run_dir, "invocation.finished") >= 1
            });
            handle.cancel_root();
        });
    })
    .await;
    assert_eq!(report.status, RunStatus::Cancelled);

    let completed = customs(&events, "attractor.parallel.branch.completed");
    assert_eq!(completed.len(), 2, "{completed:#?}");
    let of = |branch: &str| {
        completed
            .iter()
            .find(|event| event["branch"] == json!(branch))
            .cloned()
            .unwrap_or_else(|| panic!("`{branch}` completed: {completed:#?}"))
    };
    assert_eq!(of("a")["disposition"], json!("completed"), "{completed:#?}");
    assert_eq!(of("a")["status"], json!("succeeded"));
    assert_eq!(of("b")["disposition"], json!("cancelled"));
    assert!(customs(&events, "attractor.parallel.completed").is_empty());
    assert_eq!(fork_closes(&events), vec![(
        ForkDisposition::Cancelled,
        vec![(0, "success".to_owned()), (1, "cancelled".to_owned()),]
    )]);
    assert!(
        report.state.run_context().get("parallel.results").is_none(),
        "the fan-in never published"
    );
    assert_replay_matches(dir.path(), &events).await;
}

/// A second cancel escalates to a kill while the children ignore the polite
/// signal: every branch reports `killed`, the fork closes as killed from the
/// branches' own records (the join never fires), and the kill is in every
/// execution's log.
#[tokio::test]
async fn a_kill_after_the_cancel_closes_the_fork_as_killed() {
    let dir = RunDir::new("parallel-kill");
    let rt = runtime(dir.path());
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let lowered = lower(&dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="trap '' TERM; touch started_a; while :; do sleep 1; done"]
        b [shape=parallelogram, script="trap '' TERM; touch started_b; while :; do sleep 1; done"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#));
    let run_dir = dir.path().to_path_buf();
    let (report, events) = run_projected(&rt, lowered, |handle| {
        let workspace = workspace.clone();
        thread::spawn(move || {
            wait_until(Duration::from_secs(20), "both branches started", || {
                workspace.join("started_a").exists() && workspace.join("started_b").exists()
            });
            handle.cancel_root();
            wait_until(
                Duration::from_secs(20),
                "the cancel reached every child",
                || coordinator_records(&run_dir, "invocation.cancel.requested") >= 3,
            );
            handle.cancel_root();
        });
    })
    .await;
    assert_eq!(report.status, RunStatus::Cancelled);

    let completed = customs(&events, "attractor.parallel.branch.completed");
    assert_eq!(completed.len(), 2, "{completed:#?}");
    for event in &completed {
        assert_eq!(event["started"], json!(true), "{event}");
        assert_eq!(event["disposition"], json!("killed"), "{event}");
    }
    assert_eq!(fork_closes(&events), vec![(ForkDisposition::Killed, vec![
        (0, "cancelled".to_owned()),
        (1, "cancelled".to_owned()),
    ])]);
    assert!(kills_in(&events, 0) >= 1, "the root was killed");
    for target in ["a", "b"] {
        assert!(
            kills_in(&events, child_invocation(&events, target)) >= 1,
            "the kill reached `{target}`'s child"
        );
    }
    assert_replay_matches(dir.path(), &events).await;
}

/// Keep `lines` of a JSONL file up to and including the first line `keep`
/// accepts.
fn truncate_after(path: &Path, keep: impl Fn(&str) -> bool) {
    let text = fs::read_to_string(path).expect("reads");
    let mut out = Vec::new();
    for line in text.lines() {
        out.push(line);
        if keep(line) {
            break;
        }
    }
    fs::write(path, format!("{}\n", out.join("\n"))).expect("writes");
}

/// Keep the lines of a JSONL file before the first line `cut` accepts.
fn truncate_before(path: &Path, cut: impl Fn(&str) -> bool) {
    let text = fs::read_to_string(path).expect("reads");
    let mut out = Vec::new();
    for line in text.lines() {
        if cut(line) {
            break;
        }
        out.push(line);
    }
    fs::write(path, format!("{}\n", out.join("\n"))).expect("writes");
}

/// A crash after branch `a`'s child finished but before `b`'s did: the
/// resume reattaches `a`'s declared invocation without running it again and
/// finishes `b`.
#[tokio::test]
async fn resume_keeps_a_finished_branch_and_finishes_the_unfinished_one() {
    let dir = RunDir::new("parallel-resume");
    // The workspace outlives the first run: a resume needs the sandbox the
    // crash left behind.
    let rt = runtime_retaining(dir.path(), Retention::Always);
    let text = dot(r#"
        fork [shape=component]
        a [shape=parallelogram, script="echo a >> a_runs; echo a"]
        b [shape=parallelogram, script="sleep 1; echo b >> b_runs; echo b"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#);
    let report = run(&rt, lower(&text)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    let child_of = |slot: &str| {
        inspection
            .invocations
            .iter()
            .find(|i| i.parent.as_ref().is_some_and(|p| p.slot.contains(slot)))
            .map(|i| i.invocation.raw())
            .expect("the branch child")
    };
    let (child_a, child_b) = (child_of(":a"), child_of(":b"));
    // A branch child's first execution carries the child's own id.
    let log_of = |invocation: u64| {
        dir.path()
            .join(format!("executions/{invocation:016x}/events.jsonl"))
    };
    let a_log_before = fs::read_to_string(log_of(child_a)).expect("a's log");

    // The crash: the coordinator log ends with a's invocation finished; the
    // root log ends with a's branch step finished; b's log has no finish.
    let coordinator = dir.path().join("coordinator.jsonl");
    truncate_after(&coordinator, |line| {
        line.contains("invocation.finished") && line.contains(&format!("\"invocation\":{child_a}"))
    });
    truncate_after(&log_of(0), |line| {
        line.contains("step.finished") && line.contains("\"id\":\"a\"")
    });
    truncate_before(&log_of(child_b), |line| line.contains("step.finished"));

    let rt = runtime_retaining(dir.path(), Retention::Always);
    let resumed = host::resume(&rt).await.expect("resumes");
    assert_eq!(
        resumed.status,
        RunStatus::Success,
        "{:?}",
        resumed.state.errors()
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(
        inspection.invocations.len(),
        3,
        "no branch was declared again"
    );
    assert_eq!(
        fs::read_to_string(log_of(child_a)).expect("a's log"),
        a_log_before,
        "the finished branch was not run again"
    );
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    assert_eq!(
        fs::read_to_string(workspace.join("a_runs")).expect("a ran"),
        "a\n"
    );
    assert_eq!(
        fs::read_to_string(workspace.join("b_runs")).expect("b ran"),
        "b\nb\n"
    );
    let results = published_results(&resumed);
    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0]["context_updates"]["command.output"],
        json!("a\n")
    );
    assert_eq!(
        results[1]["context_updates"]["command.output"],
        json!("b\n")
    );
}

/// A `for_each` over `n` items a command produces (each item a name and a
/// 100-character brief, so 50 items are about 6.5 KB), a stub template, a
/// fan-in, and a command that reads the joined results on stdin.
fn fan_out_over(n: usize) -> String {
    dot(&format!(
        r#"
        plan [shape=parallelogram, output_schema="routing", script="python3 -c 'import json; print(json.dumps({{\"context_updates\": {{\"jobs\": [{{\"name\": \"job-\" + str(i), \"brief\": \"x\" * 100}} for i in range({n})]}}}}))'"]
        fan [shape=component, for_each="context.jobs", max_parallel=8]
        job [prompt="Do the job"]
        join [shape=tripleoctagon]
        report [shape=parallelogram, script="cat", stdin_source="context.parallel.results"]
        start -> plan -> fan -> job -> join -> report -> exit
    "#
    ))
}

/// The branch children of `fork`, in declaration order.
fn children_of<'a>(inspection: &'a RunInspection, fork: &str) -> Vec<&'a InvocationInspection> {
    let prefix = format!("branch:{fork}@");
    inspection
        .invocations
        .iter()
        .filter(|i| {
            i.parent
                .as_ref()
                .is_some_and(|p| p.slot.starts_with(&prefix))
        })
        .collect()
}

fn coordinator_len(dir: &Path) -> u64 {
    fs::metadata(dir.join("coordinator.jsonl"))
        .expect("coordinator.jsonl")
        .len()
}

/// Fifty items: every child is declared from references (the list, the plan
/// command's output and the stage records each one blob, shared by all), the
/// joined results are published as a reference that the downstream command
/// reads back whole, and the inspect document shows the same references the
/// run context holds, resolvable under `<run_dir>/blobs`.
#[tokio::test]
async fn a_fifty_item_fork_declares_its_children_from_references() {
    let dir = RunDir::new("parallel-fifty");
    let rt = runtime(dir.path());
    let report = run(&rt, lower(&fan_out_over(50))).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    let children = children_of(&inspection, "fan");
    assert_eq!(children.len(), 50);
    let mut list_refs: BTreeSet<String> = BTreeSet::new();
    for child in &children {
        let declared = serde_json::to_string(&child.context).expect("json");
        assert!(
            declared.len() < 2 * 1024,
            "a child's declared context is small: {declared}"
        );
        assert_eq!(
            declared.matches("job-").count(),
            1,
            "the only item name in a child's context is its own: {declared}"
        );
        let jobs = child
            .context
            .get("jobs")
            .and_then(Value::as_str)
            .expect("the source list is a reference string");
        assert!(jobs.starts_with("blob://sha256/"), "{jobs}");
        list_refs.insert(jobs.to_owned());
        let output = child
            .context
            .get("command.output")
            .expect("the plan's output");
        assert!(holds_ref(output), "{output}");
    }
    assert_eq!(
        list_refs.len(),
        1,
        "one blob for the list, shared by every child: {list_refs:?}"
    );
    let coordinator = coordinator_len(dir.path());
    assert!(
        coordinator < 300 * 1024,
        "coordinator.jsonl is {coordinator} bytes for 50 children"
    );
    let blobs = fs::read_dir(dir.path().join(BLOBS_DIR))
        .expect("blobs")
        .count();
    assert!(
        blobs <= 6,
        "one blob per offloaded value, not per child: {blobs}"
    );
    // The join published a reference; its logical value is the envelopes.
    let stored = report
        .state
        .run_context()
        .get("parallel.results")
        .cloned()
        .expect("published");
    assert!(holds_ref(&stored), "{stored}");
    let root = inspection
        .invocations
        .iter()
        .find(|i| i.parent.is_none())
        .expect("the root");
    let shown = root
        .result
        .as_ref()
        .expect("finished")
        .context
        .get("parallel.results")
        .cloned()
        .expect("shown");
    assert_eq!(
        shown, stored,
        "inspect shows the reference the run context holds"
    );
    let store = LocalBlobStore::new(dir.path().join(BLOBS_DIR));
    let results = hydrate(stored, &store).await;
    let results = results.as_array().expect("hydrates to the list");
    assert_eq!(results.len(), 50);
    assert_eq!(results[49]["item_label"], json!("job-49"));
    assert_eq!(results[49]["index"], json!(49));
    // The downstream command read the whole list on stdin.
    let stdout = output_of(&report, "report")["stdout"]
        .as_str()
        .expect("stdout")
        .to_owned();
    let read: Value = serde_json::from_str(&stdout).expect("the report read JSON");
    assert_eq!(read, Value::Array(results.clone()));
}

/// Drop the last `n` lines of a log.
fn drop_last_lines(path: &Path, n: usize) {
    let text = fs::read_to_string(path).expect("the log");
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let keep = lines.len().saturating_sub(n);
    fs::write(path, lines[..keep].concat()).expect("write the cut log");
}

/// A crash after the fork joined: the resumed run's downstream command reads
/// the offloaded results back through a store reopened over the run
/// directory, and no branch is declared again.
#[tokio::test]
async fn a_resumed_run_reads_the_offloaded_results_after_the_fork() {
    let dir = RunDir::new("parallel-resume-offloaded");
    let rt = runtime_retaining(dir.path(), Retention::Always);
    let report = run(&rt, lower(&fan_out_over(60))).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let stored = report
        .state
        .run_context()
        .get("parallel.results")
        .cloned()
        .expect("published");
    assert!(
        holds_ref(&stored),
        "60 envelopes are above the fan-out threshold: {stored}"
    );
    // The crash: the coordinator log ends with the last branch child's
    // result (the root's exit, result and the run's finish are gone); the
    // root log ends before the report finished.
    drop_last_lines(&dir.path().join("coordinator.jsonl"), 3);
    let root_log = dir.path().join("executions/0000000000000000/events.jsonl");
    truncate_before(&root_log, |line| {
        line.contains("step.finished") && line.contains("\"stdout\":\"[{")
    });
    let rt = runtime_retaining(dir.path(), Retention::Always);
    let resumed = host::resume(&rt).await.expect("resumes");
    assert_eq!(
        resumed.status,
        RunStatus::Success,
        "{:?}",
        resumed.state.errors()
    );
    assert_eq!(
        invocation_count(dir.path()).await,
        61,
        "no branch was declared again"
    );
    let stdout = output_of(&resumed, "report")["stdout"]
        .as_str()
        .expect("stdout")
        .to_owned();
    let read: Value = serde_json::from_str(&stdout).expect("the resumed report read JSON");
    let read = read.as_array().expect("the whole list");
    assert_eq!(read.len(), 60);
    assert_eq!(read[59]["item_label"], json!("job-59"));
}

/// Two forks in sequence: the second fork's children carry the first fork's
/// results as a reference, so the bytes per child do not grow with the
/// earlier fork.
#[tokio::test]
async fn two_forks_in_sequence_keep_every_child_small() {
    let dir = RunDir::new("parallel-two-forks");
    let rt = runtime(dir.path());
    let text = dot(&format!(
        r#"
        plan [shape=parallelogram, output_schema="routing", script="python3 -c 'import json; print(json.dumps({{\"context_updates\": {{\"jobs\": [{{\"name\": \"job-\" + str(i), \"brief\": \"x\" * 100}} for i in range({n})]}}}}))'"]
        first [shape=component, for_each="context.jobs", max_parallel=8]
        job [prompt="Do the job"]
        first_join [shape=tripleoctagon]
        second [shape=component, for_each="context.jobs", max_parallel=8]
        again [prompt="Do the job again"]
        second_join [shape=tripleoctagon]
        start -> plan -> first -> job -> first_join -> second -> again -> second_join -> exit
    "#,
        n = 60
    ));
    let report = run(&rt, lower(&text)).await;
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let inspection = inspect_run_dir(dir.path()).await.expect("inspects");
    assert_eq!(inspection.invocations.len(), 121);
    let second = children_of(&inspection, "second");
    assert_eq!(second.len(), 60);
    for child in &second {
        let declared = serde_json::to_string(&child.context).expect("json");
        assert!(
            declared.len() < 2 * 1024,
            "a second-fork child's declared context is small: {declared}"
        );
        let results = child
            .context
            .get("parallel.results")
            .expect("the first fork's results are in the snapshot");
        assert!(
            holds_ref(results),
            "the first fork's envelopes are one reference, not 60 copies: {results}"
        );
    }
    let coordinator = coordinator_len(dir.path());
    assert!(
        coordinator < 121 * 4 * 1024,
        "coordinator.jsonl is {coordinator} bytes for 120 children"
    );
    let stored = report
        .state
        .run_context()
        .get("parallel.results")
        .cloned()
        .expect("published");
    assert!(holds_ref(&stored), "{stored}");
    let store = LocalBlobStore::new(dir.path().join(BLOBS_DIR));
    let results = hydrate(stored, &store).await;
    let results = results.as_array().expect("hydrates to the list");
    assert_eq!(results.len(), 60);
    assert_eq!(results[59]["id"], json!("again"));
    assert_eq!(results[59]["item_label"], json!("job-59"));
}
