//! `petri resume --run-dir <dir>` through the shipped binary: a run killed
//! with SIGKILL continues from its run directory without repeating finished
//! work, a paused run stays paused across the resume, a gate that was waiting
//! at the crash asks again, and the command refuses a finished run, a run
//! another process holds, and a run directory that is missing or corrupt.
//!
//! Every case runs real shell commands on the host backend. The kill targets
//! only the `petri` child the case spawned, by pid.

mod support;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use support::fabro::interview;
use support::fabro::launch::{Case, Launch};
use support::fabro::twins::{Provider, Twin, model, scenario, text};
use tokio::time::{Instant, sleep};

/// Three command nodes. `first` records each execution outside the
/// workspace, `second` blocks on `marker` and records whether `unpaused`
/// existed when it ran, `third` records that it ran.
fn three_nodes(root: &Path) -> String {
    let root = root.display();
    format!(
        r#"digraph G {{
    start [shape=Mdiamond]
    exit [shape=Msquare]
    first [shape=parallelogram, script="touch {root}/first-started; while [ ! -f {root}/go1 ]; do sleep 0.1; done; echo once >> {root}/first.log"]
    second [shape=parallelogram, script="touch {root}/second-started; if [ -f {root}/unpaused ]; then echo after >> {root}/order; else echo before >> {root}/order; fi; while [ ! -f {root}/marker ]; do sleep 0.1; done; echo second >> {root}/order"]
    third [shape=parallelogram, script="echo third >> {root}/order"]
    start -> first -> second -> third -> exit
}}"#
    )
}

fn lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .map(|text| text.lines().map(str::to_owned).collect())
        .unwrap_or_default()
}

fn utf8(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

/// The node-instance records of the root's final execution, by name.
fn root_nodes(document: &Value) -> &Value {
    let execution = document["root"]["final_execution"]
        .as_u64()
        .expect("the root invocation finished");
    let record = document["executions"]
        .as_array()
        .expect("executions is a list")
        .iter()
        .find(|e| e["execution"] == execution)
        .expect("the final execution is listed");
    &record["engine"]["context"]["nodes"]
}

/// Wait until `path` exists, within the harness deadline.
async fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// A crash during a command must not make an already-used agent thread
/// look new. Capture the actual provider requests in both processes.
async fn shared_thread_across_command(restart: bool, second_thread: &str) {
    let mut case = Case::new(if restart {
        "thread-recovered"
    } else {
        "thread-control"
    });
    let provider = Provider::OpenAi;
    let model = model(provider);
    let scripts = [
        ("first", "Remember the first answer", "QA_FIRST_CONTEXT"),
        (
            "second",
            "Continue with the second answer",
            "QA_SECOND_CONTEXT",
        ),
        (
            "third",
            "Continue with the third answer",
            "QA_THIRD_CONTEXT",
        ),
    ]
    .into_iter()
    .map(|(id, prompt, answer)| {
        scenario(provider, &case.credential, id, model, prompt, text(answer))
    })
    .collect();
    let twin = Twin::start(provider, &case.root.join("twins"), scripts).await;
    case.redirect(&twin);
    let root = case.root.display();
    let workflow = case.workflow(&format!(r#"digraph G {{
    graph [backend="api"]
    start [shape=Mdiamond]
    exit [shape=Msquare]
    first [prompt="Remember the first answer.", model="{model}", provider="openai", fidelity="full", thread_id="same"]
    hold [shape=parallelogram, script="touch {root}/holding; while [ ! -f {root}/release ]; do sleep 0.1; done"]
    second [prompt="Continue with the second answer.", model="{model}", provider="openai", fidelity="full", thread_id="{second_thread}"]
    third [prompt="Continue with the third answer.", model="{model}", provider="openai", fidelity="full", thread_id="{second_thread}"]
    start -> first -> hold -> second -> third -> exit
}}"#), None);
    let finished = if restart {
        let killed = case
            .run_with(&workflow, &["--quiet"], Launch {
                kill_when: Some((case.root.join("holding"), Duration::from_millis(300))),
                ..Launch::default()
            })
            .await;
        assert!(!killed.timed_out, "{}", killed.stderr);
        assert_eq!(killed.code, None, "{}", killed.stderr);
        assert_eq!(twin.requests_for(&case.credential).len(), 1);
        fs::write(case.root.join("release"), "go").expect("release command");
        case.resume_with(&["--quiet"], Launch::default()).await
    } else {
        fs::write(case.root.join("release"), "go").expect("release command");
        case.run(&workflow, &["--quiet"]).await
    };
    finished.assert_code(0);
    let requests = twin.requests_for(&case.credential);
    fs::write(
        case.root.join("captured-requests.json"),
        serde_json::to_vec_pretty(&requests).expect("requests"),
    )
    .expect("save requests");
    assert_eq!(requests.len(), 3, "{requests:?}");
    let second = requests[1]["input"].to_string();
    assert_eq!(
        second.contains("QA_FIRST_CONTEXT"),
        second_thread == "same",
        "second request; evidence: {}\n{second}",
        case.root.display()
    );
    assert_eq!(
        second.contains("## Stage: first"),
        restart && second_thread == "same",
        "only a recovered thread needs the summary: {second}"
    );
    let assistant_answers = |request: &Value| {
        request["input"]
            .as_array()
            .expect("input messages")
            .iter()
            .filter(|message| message["role"] == "assistant")
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    };
    assert_eq!(
        assistant_answers(&requests[1]).contains("QA_FIRST_CONTEXT"),
        !restart && second_thread == "same",
        "only the uninterrupted run has the original transcript"
    );
    assert!(
        assistant_answers(&requests[2]).contains("QA_SECOND_CONTEXT"),
        "the next node reuses the rebuilt conversation, rather than rebuilding another summary"
    );
    assert_eq!(twin.unmatched(), 0);
    finished.assert_no_leaked_processes().await;
    twin.stop();
}

#[tokio::test]
async fn an_uninterrupted_thread_keeps_its_conversation_across_a_command() {
    shared_thread_across_command(false, "same").await;
}

#[tokio::test]
async fn a_thread_lost_during_a_command_resumes_with_the_prior_answer() {
    shared_thread_across_command(true, "same").await;
}

#[tokio::test]
async fn a_new_thread_after_resume_does_not_inherit_another_threads_summary() {
    shared_thread_across_command(true, "other").await;
}

/// (a) A run killed once its first node's completion is durable resumes from
/// the run directory: the first node is not run again, the second and third
/// run, `petri inspect` shows one continued execution, and the exit code is
/// the run's status.
#[tokio::test]
async fn a_killed_run_resumes_without_repeating_finished_work() {
    let case = Case::new("resume-kill");
    let workflow = case.workflow(&three_nodes(&case.root), None);
    // `first` runs to completion at once; `second` starts and blocks.
    fs::write(case.root.join("go1"), "go\n").expect("release first");
    let killed = case
        .run_with(&workflow, &["--quiet"], Launch {
            // `second` has started, so `first`'s finish is on disk.
            kill_when: Some((case.root.join("second-started"), Duration::from_millis(300))),
            ..Launch::default()
        })
        .await;
    assert!(!killed.timed_out, "the run outlived its deadline");
    assert_eq!(
        killed.code, None,
        "the run was killed by a signal, not exited\n{}",
        killed.stderr
    );
    assert_eq!(lines(&case.root.join("first.log")), ["once"]);
    assert!(case.run_dir.join("run.json").exists());
    assert!(!case.root.join("marker").exists());

    let resumed = case
        .resume_with(&["--quiet"], Launch {
            // Release `second` once the resumed process is under way.
            append_when: vec![(
                case.run_dir.join("run.json"),
                case.root.join("marker"),
                "go\n".into(),
                Duration::from_millis(1500),
            )],
            ..Launch::default()
        })
        .await;
    resumed.assert_code(0);
    assert_eq!(resumed.status_line(), Some("success"), "{}", resumed.stderr);
    assert_eq!(
        lines(&case.root.join("first.log")),
        ["once"],
        "`first` ran exactly once across the crash"
    );
    // `second` was in flight at the crash, so the resume dispatches it again
    // and it records "before" twice; the finished `first` is not repeated.
    assert_eq!(lines(&case.root.join("order")), [
        "before", "before", "second", "third"
    ]);

    let document = resumed.inspect();
    assert_eq!(document["complete"], json!(true), "{document}");
    assert_eq!(document["status"], json!("success"));
    assert_eq!(document["paused"], json!(false), "{document}");
    let executions = document["executions"].as_array().expect("executions");
    assert_eq!(
        executions.len(),
        1,
        "a resume continues the execution; only a restart adds one\n{document}"
    );
    assert_eq!(executions[0]["log"]["replay"], json!("verified"));
    let nodes = root_nodes(&document);
    for node in ["first", "second", "third"] {
        assert_eq!(nodes[node]["status"], json!("success"), "{node}: {nodes}");
    }
    assert_eq!(nodes["first"]["attempts"], json!(1));
    resumed.assert_no_leaked_processes().await;
}

/// (b) A paused run stays paused across a resume: the second node is not
/// admitted before the crash, nor after the resume, until an unpause arrives
/// through the same control file.
#[tokio::test]
async fn a_paused_run_stays_paused_across_resume_until_unpaused() {
    let case = Case::new("resume-paused");
    let workflow = case.workflow(&three_nodes(&case.root), None);
    let control = case.root.join("controls.txt");
    fs::write(&control, "").expect("control file");
    let first_started = case.root.join("first-started");
    let killed = case
        .run_with(
            &workflow,
            &["--quiet", "--control", utf8(&control)],
            Launch {
                append_when: vec![
                    // Pause while `first` runs, then let `first` finish: `second`
                    // is held at admission.
                    (
                        first_started.clone(),
                        control.clone(),
                        "pause\n".into(),
                        Duration::from_millis(0),
                    ),
                    (
                        first_started,
                        case.root.join("go1"),
                        "go\n".into(),
                        Duration::from_millis(800),
                    ),
                ],
                // `first` is done and recorded; `second` has been held a while.
                kill_when: Some((case.root.join("first.log"), Duration::from_millis(1500))),
                ..Launch::default()
            },
        )
        .await;
    assert!(!killed.timed_out, "the run outlived its deadline");
    assert_eq!(killed.code, None, "killed by a signal\n{}", killed.stderr);
    assert!(
        killed.stderr.contains("control: paused"),
        "{}",
        killed.stderr
    );
    assert_eq!(lines(&case.root.join("first.log")), ["once"]);
    assert!(
        !case.root.join("second-started").exists(),
        "`second` was admitted while paused, before the crash"
    );

    fs::write(case.root.join("marker"), "go\n").expect("second never blocks now");
    let resumed = case
        .resume_with(&["--quiet", "--control", utf8(&control)], Launch {
            append_when: vec![
                // Three seconds of paused resume, then the evidence marker
                // and the unpause. `second` records which came first.
                (
                    case.run_dir.join("run.json"),
                    case.root.join("unpaused"),
                    "yes\n".into(),
                    Duration::from_secs(3),
                ),
                (
                    case.run_dir.join("run.json"),
                    control.clone(),
                    "unpause\n".into(),
                    Duration::from_millis(0),
                ),
            ],
            ..Launch::default()
        })
        .await;
    assert_eq!(
        lines(&case.root.join("order")),
        ["after", "second", "third"],
        "`second` ran once, and only after the unpause; the pause survived the resume\n{}",
        resumed.stderr
    );
    resumed.assert_code(0);
    assert_eq!(resumed.status_line(), Some("success"), "{}", resumed.stderr);
    assert!(
        resumed.stderr.contains("control: unpaused"),
        "{}",
        resumed.stderr
    );
    assert_eq!(lines(&case.root.join("first.log")), ["once"]);
    let document = resumed.inspect();
    assert_eq!(document["complete"], json!(true), "{document}");
    assert_eq!(document["paused"], json!(false), "{document}");
    resumed.assert_no_leaked_processes().await;
}

/// The paused state is reported by `petri inspect` while the run is down.
#[tokio::test]
async fn inspect_reports_a_paused_run_as_paused() {
    let case = Case::new("resume-inspect-paused");
    let workflow = case.workflow(&three_nodes(&case.root), None);
    let control = case.root.join("controls.txt");
    fs::write(&control, "").expect("control file");
    let first_started = case.root.join("first-started");
    let killed = case
        .run_with(
            &workflow,
            &["--quiet", "--control", utf8(&control)],
            Launch {
                append_when: vec![
                    (
                        first_started.clone(),
                        control.clone(),
                        "pause\n".into(),
                        Duration::from_millis(0),
                    ),
                    (
                        first_started,
                        case.root.join("go1"),
                        "go\n".into(),
                        Duration::from_millis(800),
                    ),
                ],
                kill_when: Some((case.root.join("first.log"), Duration::from_secs(1))),
                ..Launch::default()
            },
        )
        .await;
    assert_eq!(killed.code, None, "killed by a signal\n{}", killed.stderr);
    let document = support::fabro::inspect::inspect_incomplete(&case.run_dir);
    assert_eq!(document["complete"], json!(false), "{document}");
    assert_eq!(document["status"], Value::Null);
    assert_eq!(document["paused"], json!(true), "{document}");
}

/// (d) A human gate that was waiting for its answer when the process died
/// asks again on resume, and the resumed interviewer answers it.
#[tokio::test]
async fn a_gate_waiting_at_the_crash_asks_again_on_resume() {
    let case = Case::new("resume-gate");
    let root = case.root.display();
    let workflow = case.workflow(
        &format!(
            r#"digraph G {{
    start [shape=Mdiamond]
    exit [shape=Msquare]
    prepare [shape=parallelogram, script="echo prepared >> {root}/prepare.log; touch {root}/prepared"]
    gate [shape=hexagon, label="Ship it?", question_type="yes_no"]
    ship [shape=parallelogram, script="echo shipped >> {root}/order"]
    hold [shape=parallelogram, script="echo held >> {root}/order"]
    start -> prepare -> gate
    gate -> ship [label="[Y] Yes"]
    gate -> hold [label="[N] No"]
    ship -> exit
    hold -> exit
}}"#
        ),
        None,
    );
    // The first process never answers: the gate is waiting when it dies.
    let withhold = interview::write(&case.root, "withhold", &[json!({
        "id": "never",
        "match": { "node": "gate" },
        "required": false,
        "action": { "kind": "withhold" }
    })]);
    let killed = case
        .run_with(
            &workflow,
            &["--quiet", "--interview-script", utf8(&withhold)],
            Launch {
                kill_when: Some((case.root.join("prepared"), Duration::from_millis(1500))),
                ..Launch::default()
            },
        )
        .await;
    assert_eq!(killed.code, None, "killed by a signal\n{}", killed.stderr);
    let interrupted = support::fabro::inspect::inspect_incomplete(&case.run_dir);
    assert_eq!(
        interrupted["executions"][0]["engine"]["live"][0]["node"],
        json!("gate"),
        "the gate was waiting when the process died\n{interrupted}"
    );
    assert!(!case.root.join("order").exists());

    let answer = interview::write(&case.root, "answer", &[interview::entry(
        "ship",
        "gate",
        interview::choice("Y"),
    )]);
    let resumed = case
        .resume_with(
            &["--quiet", "--interview-script", utf8(&answer)],
            Launch::default(),
        )
        .await;
    resumed.assert_code(0);
    assert_eq!(resumed.status_line(), Some("success"), "{}", resumed.stderr);
    assert_eq!(lines(&case.root.join("prepare.log")), ["prepared"]);
    assert_eq!(lines(&case.root.join("order")), ["shipped"]);
    let receipt = resumed.receipt();
    assert_eq!(receipt["errors"], json!([]), "{receipt}");
    assert_eq!(receipt["questions"][0]["node"], json!("gate"), "{receipt}");
    assert_eq!(receipt["questions"][0]["reply"]["choice"], json!("Y"));
    assert_eq!(resumed.final_context()["human.gate.selected"], json!("Y"));
    resumed.assert_no_leaked_processes().await;
}

/// (c) A finished run is not resumed: the command says so and does no work.
#[tokio::test]
async fn resume_of_a_finished_run_says_so_and_does_no_work() {
    let case = Case::new("resume-finished");
    let root = case.root.display();
    let workflow = case.workflow(
        &format!(
            r#"digraph G {{
    start [shape=Mdiamond]
    exit [shape=Msquare]
    only [shape=parallelogram, script="echo ran >> {root}/only.log"]
    start -> only -> exit
}}"#
        ),
        None,
    );
    let finished = case.run(&workflow, &["--quiet"]).await;
    finished.assert_code(0);
    let before = fs::read(case.run_dir.join("coordinator.jsonl")).expect("coordinator log");

    let resumed = case.resume_with(&["--quiet"], Launch::default()).await;
    resumed.assert_code(2);
    assert!(
        resumed.stderr.contains("already finished") && resumed.stderr.contains("success"),
        "{}",
        resumed.stderr
    );
    assert_eq!(lines(&case.root.join("only.log")), ["ran"], "no work ran");
    assert_eq!(
        fs::read(case.run_dir.join("coordinator.jsonl")).expect("coordinator log"),
        before,
        "the log is untouched"
    );
}

/// (c) A run another process holds is refused through the lease.
#[tokio::test]
async fn resume_refuses_a_run_another_process_holds() {
    let case = Case::new("resume-leased");
    let workflow = case.workflow(&three_nodes(&case.root), None);
    fs::write(case.root.join("go1"), "go\n").expect("release first");
    let second_started = case.root.join("second-started");
    let marker = case.root.join("marker");
    let live = case.run(&workflow, &["--quiet"]);
    let contender = async {
        wait_for(&second_started).await;
        let refused = case.resume_with(&["--quiet"], Launch::default()).await;
        fs::write(&marker, "go\n").expect("release second");
        refused
    };
    let (live, refused) = tokio::join!(live, contender);
    live.assert_code(0);
    refused.assert_code(2);
    assert!(
        refused.stderr.contains("already in use"),
        "{}",
        refused.stderr
    );
    assert_eq!(lines(&case.root.join("first.log")), ["once"]);
    assert_eq!(lines(&case.root.join("order")), [
        "before", "second", "third"
    ]);
}

/// (c) A missing or corrupt run directory is exit 2 with the cause.
#[tokio::test]
async fn resume_of_a_missing_or_corrupt_run_dir_exits_2_with_the_cause() {
    let case = Case::new("resume-missing");
    let missing = case.resume_with(&["--quiet"], Launch::default()).await;
    missing.assert_code(2);
    assert!(
        missing.stderr.contains("error:") && missing.stderr.contains("run.json"),
        "{}",
        missing.stderr
    );

    let corrupt = Case::new("resume-corrupt");
    let root = corrupt.root.display();
    let workflow = corrupt.workflow(
        &format!(
            r#"digraph G {{
    start [shape=Mdiamond]
    exit [shape=Msquare]
    only [shape=parallelogram, script="echo ran >> {root}/only.log"]
    start -> only -> exit
}}"#
        ),
        None,
    );
    corrupt.run(&workflow, &["--quiet"]).await.assert_code(0);
    let log: PathBuf = corrupt.run_dir.join("coordinator.jsonl");
    let mut text = fs::read_to_string(&log).expect("coordinator log");
    // A complete, newline-terminated line that is not a record: corruption,
    // not a torn tail.
    text.push_str("not a record\n");
    fs::write(&log, text).expect("corrupt the log");
    let refused = corrupt.resume_with(&["--quiet"], Launch::default()).await;
    refused.assert_code(2);
    assert!(
        refused.stderr.contains("error:") && refused.stderr.contains("is not JSON"),
        "{}",
        refused.stderr
    );
    assert_eq!(
        lines(&corrupt.root.join("only.log")),
        ["ran"],
        "no work ran"
    );
}
