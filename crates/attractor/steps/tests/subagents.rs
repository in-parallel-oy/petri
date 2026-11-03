//! Sub-agents on the native backend (readiness item 9d): a parent delegates
//! to Pebble-built children in the same workspace, the run's tool hooks and
//! the question rule hold inside them, their events and usage are attributed
//! to the parent stage, the workflow's invocation ceiling never counts them,
//! and cancellation, thread reuse and resume behave as the reference does.
//!
//! Every session of a tree answers from its own script: Pebble's
//! `test_support::routed_client` keys a child's script on the task its parent
//! gave it, so the order in which the parent and its children reach the
//! provider cannot hand an answer to the wrong session. A parent spawns in one
//! turn and waits in the next.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use std::{env, fs, process, thread};

use attractor_steps::pebble::PebbleClient;
use attractor_steps::skills::FabroHome;
use attractor_steps::subagents::METRIC;
use attractor_steps::{AgentPermissionCeiling, AgentPermissions, register};
use execution::host::{self, HostRun};
use execution::inspect::inspect_run_dir;
use frontend::{CompileInputs, Diagnostics, Lowered, NoFiles};
use frontend_attractor::RunSettings;
use frontend_attractor::hooks::{HookDefinition, HookEvent, HookKind};
use ir::{CancelScopeId, Graph, RunStatus, StepEvent, Value};
use lithos_llm::types::{ErrorKind, Request, Response, TokenCounts};
use pebble_coding_agent::test_support::{
    ScriptedCall, ScriptedCompletion, ScriptedFailure, ScriptedProvider, multi_tool_call_response,
    routed_client, scripted_client, text_response, tool_call_response, with_usage,
};
use runtime::driver::ExecutionReport;
use runtime::engine::Event;
use runtime::executor::Retention;
use runtime::{RunOptions, Runtime};
use serde_json::json;
use smol_str::SmolStr;
use testkit::{RunDir, output_of};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

fn dot(body: &str) -> String {
    format!(
        "digraph T {{\n  graph [default_model=\"test/model\"]\n  start \
         [shape=Mdiamond]\n  exit [shape=Msquare]\n{body}\n}}"
    )
}

/// One agent node `a` with `extra` attributes.
fn one_agent(extra: &str) -> String {
    dot(&format!(
        "  a [prompt=\"Delegate the work\" {extra}]\n  start -> a -> exit"
    ))
}

#[expect(
    clippy::print_stderr,
    reason = "a graph that fails to lower explains itself in the test output"
)]
fn lower(text: &str, hooks: Vec<HookDefinition>) -> Lowered {
    let settings = RunSettings {
        hooks,
        ..RunSettings::default()
    };
    let lowered = frontend_attractor::lower(
        "test.fabro",
        text,
        &NoFiles,
        &CompileInputs::new(),
        settings,
        Diagnostics::new(),
    );
    for d in lowered.diagnostics.iter() {
        eprintln!("{d}");
    }
    assert!(lowered.graph.is_some(), "lowers");
    lowered
}

fn graph(text: &str, hooks: Vec<HookDefinition>) -> Graph {
    lower(text, hooks).graph.expect("lowers")
}

/// A command hook as the Fabro frontend resolves one `[[run.hooks]]` entry.
fn command_hook(name: &str, event: HookEvent, script: &str) -> HookDefinition {
    HookDefinition {
        name: name.to_owned(),
        id: None,
        event,
        kind: HookKind::Command {
            command: script.to_owned(),
        },
        matcher: None,
        blocking: None,
        timeout_ms: None,
        sandbox: None,
        source: Some("workflow.toml".to_owned()),
    }
}

fn runtime(dir: &Path, client: lithos_llm::Client, retention: Retention) -> Runtime {
    let mut options = RunOptions::new(dir);
    options.grace = Duration::from_millis(200);
    options.retention = retention;
    options.echo = false;
    register(Runtime::standard())
        .capability(PebbleClient(client))
        .options(options)
}

fn metrics<'a>(report: &'a ExecutionReport, node: &str) -> &'a BTreeMap<SmolStr, Value> {
    &report
        .state
        .history()
        .iter()
        .find(|row| row.name == node)
        .unwrap_or_else(|| panic!("{node} finished"))
        .outcome
        .metrics
        .custom
}

/// Every `pebble` envelope the run recorded, in order.
fn pebble_events(report: &ExecutionReport) -> Vec<Value> {
    report
        .state
        .log
        .events()
        .filter_map(|event| match event {
            Event::StepProgressRecorded {
                ev: StepEvent::Custom(value),
                ..
            } if value["kind"] == "pebble" => Some(value.clone()),
            _ => None,
        })
        .collect()
}

/// The envelopes whose Pebble event is `variant`.
fn events_of<'a>(events: &'a [Value], variant: &str) -> Vec<&'a Value> {
    events
        .iter()
        .filter(|e| e["event"]["event"].get(variant).is_some())
        .collect()
}

/// The workspace of a single-invocation run.
fn workspace(dir: &RunDir) -> PathBuf {
    dir.path().join("scopes/scope-0/work")
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// A parent turn that spawns `tasks`, one child each.
fn spawn(tasks: &[&str]) -> Response {
    let ids: Vec<String> = (0..tasks.len()).map(|i| format!("spawn-{i}")).collect();
    multi_tool_call_response(
        tasks
            .iter()
            .zip(&ids)
            .map(|(task, id)| ("spawn_agent", id.as_str(), json!({ "task": task })))
            .collect(),
    )
}

/// A parent turn that waits for every child it spawned.
fn wait_for_children() -> Response {
    tool_call_response("wait", "wait", json!({}))
}

/// A session's script.
fn script(calls: Vec<ScriptedCall>) -> ScriptedProvider {
    ScriptedProvider::new(calls)
}

/// A session's script that ends in one text answer.
fn answers(text: &str) -> ScriptedProvider {
    script(vec![ScriptedCall::response(text_response(text))])
}

/// The tool names a request advertised.
fn tool_names(request: &Request) -> Vec<String> {
    request.tools().iter().map(|t| t.name.clone()).collect()
}

/// The question tool the profile registers on the root: the one whose name
/// says it asks a person.
fn question_tool(names: &[String]) -> Option<&String> {
    names.iter().find(|n| {
        let lower = n.to_ascii_lowercase();
        lower.contains("question") || lower.contains("user_input")
    })
}

// ── Delegation, attribution, accounting ─────────────────────────────────────

/// A parent hands a file change to a child; the child does it in the
/// parent's workspace; the parent reports the child's answer. The events of
/// the child carry its own session and the parent's; the stage's metrics
/// account for the child from those events.
#[tokio::test]
async fn a_child_changes_the_parents_workspace_and_the_stage_accounts_for_it() {
    let dir = RunDir::new("subagents-delegate");
    let task = "child: write child.txt";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("The child wrote the file.")),
        ]),
        vec![(
            task,
            script(vec![
                ScriptedCall::response(tool_call_response(
                    "shell",
                    "write",
                    json!({"command": "printf 'from child\\n' > child.txt && echo WROTE"}),
                )),
                ScriptedCall::response(text_response("Wrote child.txt.")),
            ]),
        )],
    );
    let report = runtime(dir.path(), client, Retention::Always)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        read(&workspace(&dir).join("child.txt")),
        "from child\n",
        "the child acted in the parent's workspace"
    );
    assert_eq!(output_of(&report, "a")["text"], "The child wrote the file.");
    assert_eq!(provider.lane(task).requests().len(), 2);
    let requests = provider.root().requests();
    assert_eq!(requests.len(), 3);
    let wait_result = serde_json::to_string(&requests[2]).expect("request");
    assert!(
        wait_result.contains("Agent completed (success: true")
            && wait_result.contains("Wrote child.txt."),
        "the parent saw the child's result: {wait_result}"
    );

    // Events: the lifecycle under the parent's session, the child's own
    // events under its session naming the parent, one stream and sequence.
    let events = pebble_events(&report);
    let parent_session = events[0]["event"]["session_id"]
        .as_str()
        .expect("session")
        .to_owned();
    let spawned = events_of(&events, "SubAgentSpawned");
    assert_eq!(spawned.len(), 1);
    assert_eq!(spawned[0]["event"]["session_id"], parent_session);
    assert_eq!(spawned[0]["event"]["event"]["SubAgentSpawned"]["depth"], 1);
    assert_eq!(
        spawned[0]["event"]["event"]["SubAgentSpawned"]["task"],
        "child: write child.txt"
    );
    let completed = events_of(&events, "SubAgentCompleted");
    assert_eq!(completed.len(), 1);
    assert_eq!(
        completed[0]["event"]["event"]["SubAgentCompleted"]["success"],
        true
    );
    assert_eq!(
        events_of(&events, "SubAgentClosed").len(),
        1,
        "shutdown closed the child"
    );
    let child_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["event"]["parent_session_id"] == parent_session)
        .collect();
    assert!(!child_events.is_empty());
    let child_session = child_events[0]["event"]["session_id"]
        .as_str()
        .expect("child session");
    assert_ne!(child_session, parent_session);
    assert!(
        child_events
            .iter()
            .any(|e| e["event"]["event"].get("ToolCallCompleted").is_some()),
        "the child's tool call is on the stream"
    );
    for (index, envelope) in events.iter().enumerate() {
        assert_eq!(envelope["node"], "a", "every event is the parent stage's");
        assert_eq!(envelope["event"]["stream_id"], parent_session);
        assert_eq!(
            envelope["event"]["seq"],
            json!(index + 1),
            "one sequence for the tree"
        );
    }

    // Accounting: the parent's own usage and the children's, reconstructed
    // from the events.
    let custom = metrics(&report, "a");
    assert_eq!(
        custom["pebble.usage"]["tokens"]["input"], 30,
        "three parent messages"
    );
    // The parent's usage by route is the parent's alone: the child's runs
    // on the same model and still stays out of it.
    assert_eq!(
        custom["pebble.usage_by_model"],
        json!([{
            "provider": "test",
            "model": "model",
            "usage": custom["pebble.usage"],
        }])
    );
    let subagents = &custom[METRIC];
    assert_eq!(subagents["spawned"], 1);
    assert_eq!(subagents["completed"], 1);
    assert_eq!(subagents["failed"], 0);
    assert_eq!(subagents["closed"], 1);
    assert_eq!(
        subagents["usage"]["tokens"]["input"], 20,
        "two child messages"
    );
    assert_eq!(
        subagents["sessions"][child_session]["parent"],
        parent_session
    );
    // The child's route, as its own `SessionStarted` reported it: the
    // parent's model, which it inherits.
    assert_eq!(subagents["sessions"][child_session]["provider"], "test");
    assert_eq!(subagents["sessions"][child_session]["model"], "model");
    assert_eq!(subagents["sessions"][child_session]["messages"], 2);
    let mut from_events: BTreeMap<String, u64> = BTreeMap::new();
    for e in &events {
        if let (Some(message), Some(session)) = (
            e["event"]["event"].get("AssistantMessage"),
            e["event"]["session_id"].as_str(),
        ) && e["event"]["parent_session_id"].is_string()
        {
            *from_events.entry(session.to_owned()).or_default() +=
                message["usage"]["tokens"]["input"].as_u64().unwrap_or(0);
        }
    }
    assert_eq!(
        from_events,
        BTreeMap::from([(child_session.to_owned(), 20)])
    );
}

/// A `pre_tool_use` hook configured for the run blocks a child's tool call
/// before its effect, and the child sees the reason.
#[tokio::test]
async fn the_runs_tool_hooks_apply_inside_a_child() {
    let dir = RunDir::new("subagents-hooks");
    let ws = workspace(&dir);
    fs::create_dir_all(&ws).expect("workspace");
    fs::write(ws.join("important.txt"), "precious\n").expect("seed");
    let task = "child: clean up";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("The child cleaned up.")),
        ]),
        vec![(
            task,
            script(vec![
                ScriptedCall::response(tool_call_response(
                    "shell",
                    "destroy",
                    json!({"command": "rm -f important.txt && echo REMOVED"}),
                )),
                ScriptedCall::response(tool_call_response(
                    "shell",
                    "safe",
                    json!({"command": "printf 'kept\\n' > safe.txt && echo SAFE"}),
                )),
                ScriptedCall::response(text_response("Cleaned up without deleting anything.")),
            ]),
        )],
    );
    let hooks = vec![
        command_hook(
            "no-destruction",
            HookEvent::PreToolUse,
            r#"if grep -q 'rm ' "$FABRO_HOOK_CONTEXT"; then echo '{"decision":"block","reason":"destructive commands are not allowed"}'; exit 2; fi"#,
        ),
        command_hook(
            "log-tools",
            HookEvent::PostToolUse,
            "echo ran:$FABRO_NODE_ID >> tool-hooks.log",
        ),
    ];
    let report = runtime(dir.path(), client, Retention::Always)
        .run(graph(&one_agent(""), hooks))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        read(&ws.join("important.txt")),
        "precious\n",
        "the blocked rm never ran"
    );
    assert_eq!(read(&ws.join("safe.txt")), "kept\n");
    let log = read(&ws.join("tool-hooks.log"));
    assert!(
        log.contains("ran:a"),
        "the post hook saw the child's call under the parent node: {log}"
    );
    let child = provider.lane(task).requests();
    assert_eq!(child.len(), 3);
    let denial = serde_json::to_string(&child[1]).expect("request");
    assert!(
        denial.contains("destructive commands are not allowed"),
        "the child saw the block reason: {denial}"
    );
    assert_eq!(provider.root().requests().len(), 3);
    assert_eq!(output_of(&report, "a")["text"], "The child cleaned up.");
}

/// The root advertises the question tool; a child does not. A child does
/// carry the sub-agent tools and the workspace tools.
#[tokio::test]
async fn a_child_has_no_question_tool() {
    let dir = RunDir::new("subagents-questions");
    let task = "child: look around";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Done.")),
        ]),
        vec![(task, answers("Looked."))],
    );
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(report.status, RunStatus::Success);
    let parent = tool_names(&provider.root().requests()[0]);
    let child = tool_names(&provider.lane(task).requests()[0]);
    let question = question_tool(&parent).expect("the root has a question tool");
    assert!(
        !child.contains(question),
        "the child has no {question}: {child:?}"
    );
    for tool in ["spawn_agent", "send_input", "wait", "close_agent", "shell"] {
        assert!(
            parent.iter().any(|n| n == tool),
            "root has {tool}: {parent:?}"
        );
        assert!(
            child.iter().any(|n| n == tool),
            "child has {tool}: {child:?}"
        );
    }
}

/// A child a Fabro agent spawns re-reads the project documents, and so does a
/// Pebble child, because Petri asks for it
/// (`SubagentOptions::with_inherited_memory`).
#[tokio::test]
async fn a_child_reads_the_project_documents_its_parent_read() {
    let dir = RunDir::new("subagents-memory");
    let ws = workspace(&dir);
    fs::create_dir_all(&ws).expect("workspace");
    fs::write(ws.join("AGENTS.md"), "Always sign notes with -- petri\n").expect("memory");
    let task = "child: write a note";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Done.")),
        ]),
        vec![(task, answers("Noted."))],
    );
    let report = runtime(dir.path(), client, Retention::Always)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(report.status, RunStatus::Success);
    let parent = serde_json::to_string(&provider.root().requests()[0]).expect("request");
    assert!(
        parent.contains("sign notes with -- petri"),
        "the root read AGENTS.md"
    );
    let child = serde_json::to_string(&provider.lane(task).requests()[0]).expect("request");
    assert!(
        child.contains("sign notes with -- petri"),
        "the reference child reads the project documents too: {child}"
    );
}

// ── Limits ──────────────────────────────────────────────────────────────────

/// Three children at once are Pebble sessions inside one invocation: a run
/// whose ceiling admits the root alone still completes with all of them.
#[tokio::test]
async fn agent_children_do_not_consume_the_invocation_ceiling() {
    let dir = RunDir::new("subagents-ceiling");
    let tasks = [
        "child alpha: report",
        "child beta: report",
        "child gamma: report",
    ];
    let (client, _) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&tasks)),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("All three reported.")),
        ]),
        tasks
            .iter()
            .map(|task| (*task, answers("Reporting.")))
            .collect(),
    );
    let lowered = lower(&one_agent(""), Vec::new());
    let mut graph = lowered.graph.expect("lowers");
    // Fabro's ceiling is 10,000; one is the smallest the coordinator takes,
    // and it leaves no room for any child invocation.
    graph.policy.max_invocations = NonZeroU32::new(1);
    let rt = runtime(dir.path(), client, Retention::Never);
    let report = host::run_configured(
        &rt,
        HostRun::new(graph).with_children(lowered.children),
        |_, _| {},
    )
    .await
    .expect("the run completes");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], "All three reported.");
    let custom = metrics(&report, "a");
    assert_eq!(custom[METRIC]["spawned"], 3);
    assert_eq!(custom[METRIC]["completed"], 3);
    assert_eq!(
        custom[METRIC]["sessions"]
            .as_object()
            .map(serde_json::Map::len),
        Some(3)
    );
    assert_eq!(
        inspect_run_dir(dir.path())
            .await
            .expect("inspects")
            .invocations
            .len(),
        1,
        "no child is a workflow invocation"
    );
}

/// A node's open-session bound refuses a spawn the tree has no room for as
/// the tool's answer; the parent reads it and carries on.
#[tokio::test]
async fn a_spawn_over_the_open_session_bound_is_refused_and_the_stage_carries_on() {
    let dir = RunDir::new("subagents-bound");
    let (client, provider) = scripted_client(vec![
        ScriptedCall::response(tool_call_response(
            "spawn_agent",
            "spawn",
            json!({"task": "child: anything"}),
        )),
        ScriptedCall::response(text_response("No room; did it myself.")),
    ]);
    let mut graph = graph(&one_agent(""), Vec::new());
    let node = graph
        .body
        .nodes
        .iter_mut()
        .find(|n| n.name == "a")
        .expect("the agent node");
    assert_eq!(
        node.step.config["subagents"]["max_open_sessions"], 4,
        "the reference default"
    );
    node.step.config["subagents"]["max_open_sessions"] = json!(1);
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph)
        .await
        .expect("replay");
    assert_eq!(report.status, RunStatus::Success);
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let refusal = serde_json::to_string(&requests[1]).expect("request");
    assert!(
        refusal.contains("Cannot spawn another agent")
            && refusal.contains("(1, counting the root)"),
        "{refusal}"
    );
    assert_eq!(metrics(&report, "a")[METRIC]["spawned"], 0);
    assert_eq!(output_of(&report, "a")["text"], "No room; did it myself.");
}

// ── Failure, nesting, cancellation ──────────────────────────────────────────

/// A child whose model fails reports the failure to the waiting parent as
/// the tool's answer; the parent's stage succeeds, as in the reference.
#[tokio::test]
async fn a_childs_failure_reaches_the_parent_without_failing_the_stage() {
    let dir = RunDir::new("subagents-failure");
    let task = "child: fail please";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("The child failed.")),
        ]),
        vec![(
            task,
            script(vec![ScriptedCall::Failure(ScriptedFailure::terminal(
                ErrorKind::Authentication,
                "the child's model fell over",
            ))]),
        )],
    );
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(output_of(&report, "a")["text"], "The child failed.");
    let requests = provider.root().requests();
    let seen = serde_json::to_string(&requests[2]).expect("request");
    assert!(
        seen.contains("fell over"),
        "the parent read the failure: {seen}"
    );
    let events = pebble_events(&report);
    let failed = events_of(&events, "SubAgentFailed");
    assert_eq!(failed.len(), 1);
    assert!(
        failed[0]["event"]["event"]["SubAgentFailed"]["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("fell over"))
    );
    let custom = metrics(&report, "a");
    assert_eq!(custom[METRIC]["failed"], 1);
    assert_eq!(custom[METRIC]["completed"], 0);
    assert_eq!(
        custom[METRIC]["usage"]["tokens"]["input"], 0,
        "the child committed no message"
    );
}

/// A child delegates further: the grandchild's events name the child as its
/// parent and the root as the stream, and every layer is attributed to the
/// one stage.
#[tokio::test]
async fn nested_delegation_reaches_a_grandchild_within_the_open_session_bound() {
    let dir = RunDir::new("subagents-nested");
    let child_task = "child: delegate the write";
    let grandchild_task = "grandchild: write deep.txt";
    let (client, _) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[child_task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("The grandchild wrote the file.")),
        ]),
        vec![
            (
                child_task,
                script(vec![
                    ScriptedCall::response(spawn(&[grandchild_task])),
                    ScriptedCall::response(wait_for_children()),
                    ScriptedCall::response(text_response("My child wrote deep.txt.")),
                ]),
            ),
            (
                grandchild_task,
                script(vec![
                    ScriptedCall::response(tool_call_response(
                        "shell",
                        "write",
                        json!({"command": "printf 'deep\\n' > deep.txt && echo WROTE"}),
                    )),
                    ScriptedCall::response(text_response("Wrote deep.txt.")),
                ]),
            ),
        ],
    );
    let report = runtime(dir.path(), client, Retention::Always)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(read(&workspace(&dir).join("deep.txt")), "deep\n");
    assert_eq!(
        output_of(&report, "a")["text"],
        "The grandchild wrote the file."
    );
    let events = pebble_events(&report);
    let root = events[0]["event"]["session_id"]
        .as_str()
        .expect("root")
        .to_owned();
    let spawned = events_of(&events, "SubAgentSpawned");
    assert_eq!(spawned.len(), 2);
    assert_eq!(spawned[0]["event"]["event"]["SubAgentSpawned"]["depth"], 1);
    assert_eq!(spawned[0]["event"]["session_id"], root);
    assert_eq!(spawned[1]["event"]["event"]["SubAgentSpawned"]["depth"], 2);
    let child = spawned[1]["event"]["session_id"]
        .as_str()
        .expect("child")
        .to_owned();
    assert_ne!(child, root, "the child published the grandchild's spawn");
    assert_eq!(spawned[1]["event"]["parent_session_id"], root);
    let grandchild_tool = events
        .iter()
        .find(|e| {
            e["event"]["event"].get("ToolCallCompleted").is_some()
                && e["event"]["parent_session_id"] == child
        })
        .expect("the grandchild's tool call, naming the child as its parent");
    assert_eq!(grandchild_tool["event"]["stream_id"], root);
    assert_eq!(grandchild_tool["node"], "a");
    let custom = metrics(&report, "a");
    assert_eq!(custom[METRIC]["spawned"], 2);
    assert_eq!(custom[METRIC]["completed"], 2);
    assert_eq!(custom[METRIC]["closed"], 2);
    assert_eq!(
        custom[METRIC]["sessions"]
            .as_object()
            .map(serde_json::Map::len),
        Some(2)
    );
}

/// Cancelling the run while the parent waits closes the child, ends its
/// tool process, and the stage settles as cancelled.
#[tokio::test]
async fn cancelling_the_run_stops_every_descendant() {
    assert_stops_every_descendant(false).await;
}

/// The default native backend's deadline must close the child session and
/// stop both its tool shell and the process that shell launched.
#[tokio::test]
async fn a_default_native_deadline_stops_every_descendant() {
    assert_stops_every_descendant(true).await;
}

async fn assert_stops_every_descendant(at_deadline: bool) {
    let dir = RunDir::new("subagents-cancel");
    let marker = format!(
        "petri-subagent-cancel-{}-{}",
        process::id(),
        testkit::unique_id()
    );
    let child_marker = format!(
        "petri-tool-child-{}-{}",
        process::id(),
        testkit::unique_id()
    );
    let waiting = dir.path().join("waiting");
    let command = format!(
        "sh -c 'touch \"$1\"; while true; do sleep 0.1; done' {child_marker} '{}' & wait # {marker}",
        waiting.display()
    );
    let task = "child: wait forever";
    let (client, _) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::PendingOpen,
        ]),
        vec![(
            task,
            script(vec![
                ScriptedCall::response(tool_call_response(
                    "shell",
                    "block",
                    json!({"command": command}),
                )),
                ScriptedCall::PendingOpen,
            ]),
        )],
    );
    let rt = runtime(dir.path(), client, Retention::Never);
    let extra = if at_deadline { r#", timeout="3s""# } else { "" };
    let driver = rt.driver(graph(&one_agent(extra), Vec::new()));
    let handle = driver.handle();
    let mut runs = JoinSet::new();
    runs.spawn(driver.run());
    timeout(Duration::from_secs(15), async {
        while !waiting.exists() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the child's tool started");
    if !at_deadline {
        handle.cancel(CancelScopeId::ROOT).await;
    }
    let report = timeout(Duration::from_secs(20), runs.join_next())
        .await
        .expect("cancel settles")
        .expect("run task exists")
        .expect("run task");
    if !at_deadline {
        assert_ne!(report.status, RunStatus::Success);
    }
    assert_eq!(
        testkit::status_of(&report, "a").as_deref(),
        Some(if at_deadline {
            "timed_out"
        } else {
            "cancelled"
        })
    );
    let events = pebble_events(&report);
    assert_eq!(events_of(&events, "SubAgentSpawned").len(), 1);
    assert_eq!(
        events_of(&events, "SubAgentClosed").len(),
        1,
        "the child was closed on the way out"
    );
    // The child's shell loop is gone: nothing carries the marker any more.
    // The plugin's kill is asynchronous to the report; give it a moment.
    for _ in 0..50 {
        if !process_running(&marker) && !process_running(&child_marker) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !process_running(&marker) && !process_running(&child_marker),
        "the child's tool shell and its descendant were stopped"
    );
}

fn process_running(marker: &str) -> bool {
    Command::new("pgrep")
        .args(["-f", marker])
        .output()
        .is_ok_and(|o| o.status.success())
}

// ── Threads and resume ──────────────────────────────────────────────────────

/// A later `full` node on the thread continues the conversation with the
/// child's result in it, and can delegate again on the retained session.
#[tokio::test]
async fn a_retained_thread_keeps_a_childs_result_for_the_next_node() {
    let dir = RunDir::new("subagents-thread");
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&["child: find the answer"])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Found it.")),
            ScriptedCall::response(spawn(&["child: confirm the answer"])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Confirmed.")),
        ]),
        vec![
            ("child: find the answer", answers("The answer is 42.")),
            ("child: confirm the answer", answers("Confirmed: 42.")),
        ],
    );
    let text = dot(
        r#"  a [prompt="Find the answer", fidelity="full", thread_id="t"]
  b [prompt="Confirm the answer", fidelity="full", thread_id="t"]
  start -> a -> b -> exit"#,
    );
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph(&text, Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    let requests = provider.root().requests();
    assert_eq!(requests.len(), 6, "three turns per node, one session");
    let second_node = serde_json::to_string(&requests[3]).expect("request");
    assert!(
        second_node.contains("The answer is 42.") && second_node.contains("Confirm the answer"),
        "the retained conversation carries the child's result: {second_node}"
    );
    assert_eq!(output_of(&report, "b")["text"], "Confirmed.");
    assert_eq!(metrics(&report, "a")[METRIC]["spawned"], 1);
    assert_eq!(
        metrics(&report, "b")[METRIC]["spawned"],
        1,
        "each stage accounts for its own children"
    );
    let events = pebble_events(&report);
    let spawned = events_of(&events, "SubAgentSpawned");
    assert_eq!(spawned.len(), 2);
    assert_eq!(spawned[0]["node"], "a");
    assert_eq!(spawned[1]["node"], "b");
    assert_eq!(
        spawned[0]["event"]["session_id"], spawned[1]["event"]["session_id"],
        "one session across the thread"
    );
}

/// A run that dies while a child works resumes by running the stage again:
/// nothing of the child is restored, and the files it had written stay in
/// the workspace.
#[tokio::test]
async fn a_resumed_run_restarts_the_stage_and_keeps_an_unfinished_childs_files() {
    let dir = RunDir::new("subagents-resume");
    let gate = dir.path().join("gate");
    let command = format!(
        "printf 'partial\\n' > partial.txt; while [ ! -f '{}' ]; do sleep 0.05; done",
        gate.display()
    );
    // The root's script serves both runs: the first parent spawns and blocks
    // in its wait; the resumed run's new session takes the rest.
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&["child: write partial.txt"])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(spawn(&["child: write final.txt"])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Finished after the restart.")),
        ]),
        vec![
            (
                "child: write partial.txt",
                script(vec![
                    ScriptedCall::response(tool_call_response(
                        "shell",
                        "partial",
                        json!({"command": command}),
                    )),
                    // The run dies with this child mid-tool; if anything of
                    // it survived to ask again, it would hang here.
                    ScriptedCall::PendingOpen,
                ]),
            ),
            (
                "child: write final.txt",
                script(vec![
                    ScriptedCall::response(tool_call_response(
                        "shell",
                        "final",
                        json!({"command": "printf 'final\\n' > final.txt && echo WROTE"}),
                    )),
                    ScriptedCall::response(text_response("Wrote final.txt.")),
                ]),
            ),
        ],
    );
    let lowered = lower(&one_agent(""), Vec::new());
    let graph = lowered.graph.expect("lowers");
    let workspace = dir.path().join("scopes/invocation-0-scope-0/work");
    let rt = runtime(dir.path(), client.clone(), Retention::Always);
    let first = tokio::spawn({
        let rt = runtime(dir.path(), client, Retention::Always);
        let graph = graph.clone();
        async move { host::run_configured(&rt, HostRun::new(graph), |_, _| {}).await }
    });
    timeout(Duration::from_secs(15), async {
        // The parent has asked twice (spawn, then wait) and the child is in
        // its tool before the run dies.
        while !(workspace.join("partial.txt").exists() && provider.root().requests().len() == 2) {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the parent waits and the child wrote its file");
    assert_eq!(
        provider.lane("child: write partial.txt").requests().len(),
        1
    );
    // The crash: the run's task is dropped with the child mid-tool.
    first.abort();
    let _ = first.await;
    fs::write(&gate, "go").expect("release the blocked tool");

    let resumed = host::resume(&rt).await.expect("resumes");
    assert_eq!(
        resumed.status,
        RunStatus::Success,
        "{:?}",
        resumed.state.errors()
    );
    assert_eq!(
        read(&workspace.join("partial.txt")),
        "partial\n",
        "the unfinished child's file stays"
    );
    assert_eq!(read(&workspace.join("final.txt")), "final\n");
    assert_eq!(
        output_of(&resumed, "a")["text"],
        "Finished after the restart."
    );
    assert_eq!(
        inspect_run_dir(dir.path())
            .await
            .expect("inspects")
            .invocations
            .len(),
        1
    );
    let events = pebble_events(&resumed);
    let spawned = events_of(&events, "SubAgentSpawned");
    assert_eq!(
        spawned.len(),
        2,
        "the first run's spawn is in the log; the restart spawned again"
    );
    assert_ne!(
        spawned[0]["event"]["session_id"], spawned[1]["event"]["session_id"],
        "the restarted stage is a new session; no child was restored"
    );
    assert_eq!(metrics(&resumed, "a")[METRIC]["spawned"], 1);
}

// ── Skills ──────────────────────────────────────────────────────────────────

/// The `repo` skills fixture as a Git repository in the run's workspace, or
/// `None` when Git is unavailable.
fn skills_repository(dir: &RunDir) -> Option<PathBuf> {
    fn copy_tree(from: &Path, to: &Path) {
        fs::create_dir_all(to).expect("target dir");
        for entry in fs::read_dir(from).expect("fixture dir") {
            let entry = entry.expect("entry");
            let target = to.join(entry.file_name());
            if entry.file_type().expect("type").is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                fs::copy(entry.path(), &target).expect("copy");
            }
        }
    }
    let fixtures =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fabro/acceptance/testdata/skills");
    let ws = workspace(dir);
    copy_tree(&fixtures.join("repo"), &ws);
    let git = Command::new("git")
        .args(["init", "-q"])
        .current_dir(&ws)
        .status();
    git.is_ok_and(|s| s.success()).then_some(ws)
}

/// The parent discovers the workspace's skills (task 14's directories); a
/// child a Fabro agent spawns re-discovers them from the shared sandbox, and
/// so does a Pebble child, because Petri asks for it
/// (`SubagentOptions::with_inherited_skills`).
#[tokio::test]
async fn a_child_sees_the_skills_its_parent_discovered() {
    let dir = RunDir::new("subagents-skills");
    if skills_repository(&dir).is_none() {
        return;
    }
    let task = "child: greet Ada";
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("Done.")),
        ]),
        vec![(task, answers("Greeted."))],
    );
    let mut options = RunOptions::new(dir.path());
    options.grace = Duration::from_millis(200);
    options.retention = Retention::Always;
    options.echo = false;
    let rt = register(
        Runtime::standard()
            .options(options)
            .capability(PebbleClient(client))
            .capability(FabroHome(dir.path().join("no-home"))),
    );
    let report = rt
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(report.status, RunStatus::Success);
    let root = provider.root().requests();
    let parent = serde_json::to_string(&root[0]).expect("request");
    assert!(
        parent.contains("# Available Skills")
            && tool_names(&root[0]).iter().any(|n| n == "use_skill"),
        "the root discovered the repository's skills"
    );
    let first = &provider.lane(task).requests()[0];
    let child = serde_json::to_string(first).expect("request");
    assert!(
        child.contains("# Available Skills") && tool_names(first).iter().any(|n| n == "use_skill"),
        "the reference child re-discovers the skills: tools {:?}",
        tool_names(first)
    );
}

// ── Compaction (item 9e's cross-feature check) ──────────────────────────────

/// One token above Fabro's trigger for the test catalog's 200,000-token
/// window, as task 16's compaction suite computes it.
const ABOVE_THRESHOLD: u64 = 200_000 * 80 / 100 + 1;

/// A tool round for the history a compaction discards.
fn shell(id: &str, command: &str) -> ScriptedCall {
    ScriptedCall::response(tool_call_response(
        "shell",
        id,
        json!({ "command": command }),
    ))
}

/// A response whose reported usage crosses the trigger once committed.
fn crossing(response: Response) -> ScriptedCall {
    ScriptedCall::response(with_usage(response, TokenCounts {
        input: ABOVE_THRESHOLD - 5,
        output: 5,
        ..TokenCounts::default()
    }))
}

/// A child inherits the parent's compaction settings: a child whose history
/// crosses the trigger compacts, its `CompactionStarted`/`CompactionCompleted`
/// events carry the child's session and name the parent, the summary call is
/// the child's, and the stage's ledger attributes the compaction to the child
/// session while the parent's own compaction count stays zero.
#[tokio::test]
async fn a_child_compacts_under_the_inherited_settings_and_its_events_name_the_child() {
    let dir = RunDir::new("subagents-child-compaction");
    let task = "child: do four things";
    let child = script(vec![
        shell("first", "echo FIRST_OUTPUT_MARKER"),
        shell("second", "echo second"),
        shell("third", "echo third"),
        shell("fourth", "echo fourth"),
        crossing(text_response("Four things done.")),
    ])
    .completing(vec![ScriptedCompletion::response(with_usage(
        text_response("SUMMARY OF THE CHILD'S WORK"),
        TokenCounts {
            input: 70,
            output: 7,
            ..TokenCounts::default()
        },
    ))]);
    let (client, provider) = routed_client(
        script(vec![
            ScriptedCall::response(spawn(&[task])),
            ScriptedCall::response(wait_for_children()),
            ScriptedCall::response(text_response("The child did four things.")),
        ]),
        vec![(task, child)],
    );
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        output_of(&report, "a")["text"],
        "The child did four things."
    );
    assert_eq!(
        provider.lane(task).completion_count(),
        1,
        "one summary call, the child's"
    );
    assert_eq!(provider.root().completion_count(), 0);
    let events = pebble_events(&report);
    let parent_session = events[0]["event"]["session_id"]
        .as_str()
        .expect("root")
        .to_owned();
    let started = events_of(&events, "CompactionStarted");
    let completed = events_of(&events, "CompactionCompleted");
    assert_eq!(started.len(), 1, "{events:?}");
    assert_eq!(completed.len(), 1);
    let child_session = completed[0]["event"]["session_id"]
        .as_str()
        .expect("child")
        .to_owned();
    assert_ne!(
        child_session, parent_session,
        "the compaction is the child's"
    );
    assert_eq!(completed[0]["event"]["parent_session_id"], parent_session);
    assert_eq!(completed[0]["node"], "a", "attributed to the parent stage");
    let custom = metrics(&report, "a");
    assert_eq!(
        custom["pebble.compactions"], 0,
        "the parent did not compact"
    );
    assert_eq!(custom[METRIC]["sessions"][&child_session]["compactions"], 1);
    assert_eq!(
        custom[METRIC]["sessions"][&child_session]["parent"],
        parent_session
    );
    assert_eq!(custom[METRIC]["spawned"], 1);
    // The child's summary usage is not in any Petri metric at the pin: Pebble's
    // `CompactionCompleted` carries none and the child's history is Pebble's.
    assert_eq!(custom["pebble.compaction_usage"]["tokens"]["input"], 0);
}

/// A parent whose own history compacts keeps its supervisor: the turn after
/// the compaction spawns a child and waits for it as before.
#[tokio::test]
async fn a_parent_still_delegates_after_its_own_compaction() {
    let dir = RunDir::new("subagents-parent-compaction");
    let task = "child: finish the work";
    let root = script(vec![
        shell("first", "echo FIRST_OUTPUT_MARKER"),
        shell("second", "echo second"),
        shell("third", "echo third"),
        shell("fourth", "echo fourth"),
        crossing(spawn(&[task])),
        ScriptedCall::response(wait_for_children()),
        ScriptedCall::response(text_response("Delegated after compacting.")),
    ])
    .completing(vec![ScriptedCompletion::response(with_usage(
        text_response("SUMMARY OF THE FIRST EXCHANGE"),
        TokenCounts {
            input: 70,
            output: 7,
            ..TokenCounts::default()
        },
    ))]);
    let (client, provider) = routed_client(root, vec![(task, answers("Finished."))]);
    let report = runtime(dir.path(), client, Retention::Never)
        .run(graph(&one_agent(""), Vec::new()))
        .await
        .expect("replay");
    assert_eq!(
        report.status,
        RunStatus::Success,
        "{:?}",
        report.state.errors()
    );
    assert_eq!(
        output_of(&report, "a")["text"],
        "Delegated after compacting."
    );
    assert_eq!(
        provider.root().completion_count(),
        1,
        "the parent's summary call"
    );
    assert_eq!(provider.lane(task).completion_count(), 0);
    let requests = provider.root().requests();
    let last = serde_json::to_string(requests.last().expect("request")).expect("request");
    assert!(
        last.contains("SUMMARY OF THE FIRST EXCHANGE") && !last.contains("FIRST_OUTPUT_MARKER"),
        "the parent's later request carries the summary, not the discarded output: {last}"
    );
    assert!(last.contains("Agent completed (success: true"), "{last}");
    let custom = metrics(&report, "a");
    assert_eq!(custom["pebble.compactions"], 1);
    assert_eq!(custom[METRIC]["spawned"], 1);
    assert_eq!(custom[METRIC]["completed"], 1);
    let events = pebble_events(&report);
    let parent_session = events[0]["event"]["session_id"]
        .as_str()
        .expect("root")
        .to_owned();
    let completed = events_of(&events, "CompactionCompleted");
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["event"]["session_id"], parent_session);
    let spawned = events_of(&events, "SubAgentSpawned");
    assert_eq!(spawned.len(), 1);
    assert!(
        completed[0]["event"]["seq"].as_u64() < spawned[0]["event"]["seq"].as_u64(),
        "the compaction came first"
    );
}

#[tokio::test]
async fn subagent_permissions_never_exceed_the_parent() {
    for (attributes, ceiling) in [
        (r#"permissions="read-only""#, AgentPermissions::Full),
        (r#"permissions="full""#, AgentPermissions::ReadOnly),
    ] {
        let dir = RunDir::new("subagent-permissions");
        let task = "child: attempt a write";
        let forbidden = workspace(&dir).join("child-write.txt");
        let (client, provider) = routed_client(
            script(vec![
                ScriptedCall::response(spawn(&[task])),
                ScriptedCall::response(wait_for_children()),
                ScriptedCall::response(text_response("Reviewed.")),
            ]),
            vec![(
                task,
                script(vec![
                    ScriptedCall::response(tool_call_response(
                        "write_file",
                        "write",
                        json!({"file_path": "child-write.txt", "content": "forbidden"}),
                    )),
                    ScriptedCall::response(text_response("Cannot write.")),
                ]),
            )],
        );
        let report = runtime(dir.path(), client, Retention::Always)
            .capability(AgentPermissionCeiling(ceiling))
            .run(graph(&one_agent(attributes), Vec::new()))
            .await
            .expect("run");
        assert_eq!(
            report.status,
            RunStatus::Success,
            "{:?}",
            report.state.errors()
        );
        assert!(!forbidden.exists());
        assert_eq!(
            provider.lane(task).requests().len(),
            2,
            "child actually ran"
        );
    }
}
