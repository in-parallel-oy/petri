//! What an agent or prompt node hears about the run so far: Fabro's
//! preambles for every fidelity mode, built from the run context with no
//! model call, plus the run-time resolution of the mode and the thread.
//!
//! The text follows `fabro-workflow/src/handler/llm/preamble.rs` at the
//! pinned revision: `truncate` is the goal and the run id; `compact` is the
//! nested-bullet summary; `summary:low` and `summary:medium` show the last
//! two and five stages; `summary:high` is the per-stage report with a
//! context table. `full` has no preamble at all: the node continues its
//! thread's conversation. Fabro orders stages by execution; the run context
//! records them by name, so this module orders them by the workflow's
//! declaration order (`stages`) and appends anything else by name.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use frontend_attractor::fidelity::resolve_thread;
pub use frontend_attractor::fidelity::{Fidelity, Source};
use frontend_attractor::kinds::GOAL_CHECK_NODE;
use ir::Value;
use serde::Deserialize;

use crate::outcome::fabro_outcome;

/// The compact and medium preambles keep this many trailing lines of a
/// command's output.
const COMPACT_OUTPUT_MAX_LINES: usize = 25;
/// The high summary keeps this many.
const SUMMARY_HIGH_OUTPUT_MAX_LINES: usize = 50;
/// `summary:low` shows this many recent stages (about 600 tokens).
const SUMMARY_LOW_STAGES: usize = 2;
/// `summary:medium` shows this many recent stages (about 1,500 tokens).
const SUMMARY_MEDIUM_STAGES: usize = 5;
/// One value contributes at most this much serialized JSON inline. Fabro
/// materializes a larger value as a file; Petri truncates it with a marker
/// and a preview, since a preamble value is not a workspace artifact.
pub const INLINE_VALUE_MAX: usize = 8 * 1024;
/// How much of a truncated value the marker shows.
const PREVIEW_CHARS: usize = 300;

/// The token an edge into an LLM node carries: which node it left, and its
/// own `fidelity` and `thread_id`, when set.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Incoming {
    pub from:      Option<String>,
    pub fidelity:  Option<String>,
    pub thread_id: Option<String>,
}

impl Incoming {
    /// Read the token payload. Anything that is not an edge payload (a seed
    /// token, a branch payload) reads as no incoming edge.
    pub fn from_value(value: &Value) -> Self {
        if !value.is_object() {
            return Self::default();
        }
        serde_json::from_value(value.clone()).unwrap_or_default()
    }
}

/// Everything the node config carries about threads and fidelity.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ThreadConfig {
    pub fidelity:         Option<String>,
    pub default_fidelity: Option<String>,
    pub thread_id:        Option<String>,
    pub default_thread:   Option<String>,
    pub classes:          Vec<String>,
}

/// What resolved for one node: the mode, the thread, and where each came
/// from, for the event a host reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub fidelity:        Fidelity,
    pub fidelity_source: Source,
    pub thread:          Option<String>,
    pub thread_source:   Option<Source>,
}

/// Fabro's resolution for a node entered along `incoming`. `branch` says the
/// node is a parallel branch's first node: thread ids are inert and an
/// explicit `full` degrades to `summary:high`. `degrade` is the resume
/// fallback: the node's conversation is gone, so `full` degrades once.
pub fn resolve(
    config: &ThreadConfig,
    incoming: &Incoming,
    branch: bool,
    degrade: bool,
) -> Resolved {
    let parse = |text: &Option<String>| text.as_deref().and_then(|t| t.parse::<Fidelity>().ok());
    let (mut fidelity, mut fidelity_source) = Fidelity::resolve(
        parse(&incoming.fidelity),
        parse(&config.fidelity),
        parse(&config.default_fidelity),
    );
    if branch || degrade {
        let degraded = fidelity.degraded();
        if degraded != fidelity {
            fidelity = degraded;
            fidelity_source = if branch {
                Source::Branch
            } else {
                Source::Resume
            };
        }
    }
    let thread = if branch {
        None
    } else {
        resolve_thread(
            incoming.thread_id.as_deref(),
            config.thread_id.as_deref(),
            config.default_thread.as_deref(),
            config.classes.first().map(String::as_str),
            incoming.from.as_deref(),
        )
    };
    let (thread, thread_source) = thread
        .map(|(thread, source)| (thread.to_owned(), source))
        .unzip();
    Resolved {
        fidelity,
        fidelity_source,
        thread,
        thread_source,
    }
}

/// One stage the lowering described: its id, handler kind, script and
/// model, in declaration order.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StageInfo {
    pub id:     String,
    pub kind:   Option<String>,
    pub script: Option<String>,
    pub model:  Option<String>,
}

/// The inputs a preamble is built from.
pub struct Preamble<'a> {
    pub goal:   &'a str,
    pub run_id: &'a str,
    /// The lowering's stage list, declaration order.
    pub stages: &'a [StageInfo],
    /// The run context's node records (`nodes.<name>`).
    pub nodes:  &'a Value,
    /// The run context's `kv`.
    pub kv:     &'a Value,
}

/// One completed stage, as the preamble renders it.
struct Completed<'a> {
    id:       &'a str,
    kind:     Option<&'a str>,
    script:   Option<&'a str>,
    status:   &'a str,
    notes:    Option<String>,
    reason:   Option<String>,
    output:   Option<String>,
    model:    Option<String>,
    text:     Option<String>,
    files:    Option<String>,
    /// The context keys this stage's output already rendered.
    rendered: Vec<String>,
}

/// How much of a completed stage a preamble shows under its heading.
#[derive(Clone, Copy, Debug)]
enum Detail {
    /// Fabro's compact details, by the stage's kind: a command's script and
    /// output, an LLM stage's model. `compact` and `summary:medium`.
    Compact,
    /// The handler, the script and the model. `summary:low`.
    Low,
    /// Everything the record carries: the handler, script, output, model,
    /// response, notes and failure reason. `summary:high`.
    High,
}

impl Preamble<'_> {
    /// The preamble for `fidelity`. Empty for `full`.
    pub fn render(&self, fidelity: Fidelity) -> String {
        match fidelity {
            Fidelity::Full => String::new(),
            Fidelity::Truncate => format!("Goal: {}\nRun ID: {}\n", self.goal, self.run_id),
            Fidelity::Compact => self.compact(),
            Fidelity::SummaryLow => self.summary_low(),
            Fidelity::SummaryMedium => self.summary_medium(),
            Fidelity::SummaryHigh => self.summary_high(),
        }
    }

    /// The prompt an LLM node sends: the preamble, a blank line, the node's
    /// prompt. A `full` node sends the prompt alone.
    pub fn prompt(&self, fidelity: Fidelity, prompt: &str) -> String {
        let mut out = self.prefix(fidelity);
        out.push_str(prompt);
        out
    }

    /// What precedes the node's prompt: the preamble and a blank line, or
    /// nothing for `full`.
    pub fn prefix(&self, fidelity: Fidelity) -> String {
        let mut preamble = self.render(fidelity);
        if !preamble.is_empty() {
            preamble.push_str("\n\n");
        }
        preamble
    }

    /// The completed stages, in declaration order then by name.
    fn completed(&self) -> Vec<Completed<'_>> {
        let Value::Object(nodes) = self.nodes else {
            return Vec::new();
        };
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for stage in self.stages {
            if let Some(record) = nodes.get(&stage.id) {
                seen.insert(stage.id.as_str());
                if let Some(completed) = completed_stage(&stage.id, Some(stage), record) {
                    out.push(completed);
                }
            }
        }
        for (name, record) in nodes {
            if seen.contains(name.as_str()) {
                continue;
            }
            if let Some(completed) = completed_stage(name, None, record) {
                out.push(completed);
            }
        }
        out
    }

    fn compact(&self) -> String {
        let completed = self.completed();
        let mut parts = vec![format!("Goal: {}", self.goal)];
        if !completed.is_empty() {
            parts.push("\n## Completed stages".to_owned());
            for stage in &completed {
                parts.push(format!("- **{}**: {}", stage.id, stage.status));
                parts.extend(stage.details(Detail::Compact, "  "));
            }
        }
        parts.extend(self.context_list(&completed));
        parts.push(String::new());
        parts.join("\n")
    }

    fn summary_high(&self) -> String {
        let completed = self.completed();
        let total = self
            .stages
            .iter()
            .filter(|s| !matches!(s.kind.as_deref(), Some("start" | "exit")))
            .count()
            .max(completed.len());
        let mut parts = vec![
            format!("Goal: {}", self.goal),
            format!("Run ID: {}", self.run_id),
            format!(
                "Pipeline progress: {} of {total} stages completed",
                completed.len()
            ),
        ];
        for stage in &completed {
            parts.push(format!("\n## Stage: {}", stage.id));
            parts.push(format!("- Status: {}", stage.status));
            parts.extend(stage.details(Detail::High, ""));
        }
        let rows = self.context_rows(&completed);
        if !rows.is_empty() {
            parts.push("\n## Current context".to_owned());
            parts.push("| Key | Value |".to_owned());
            parts.push("|-----|-------|".to_owned());
            for (key, value) in rows {
                let cell = value
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
                    .replace('|', "\\|");
                parts.push(format!("| {key} | {cell} |"));
            }
        }
        parts.push(String::new());
        parts.join("\n")
    }

    fn summary_medium(&self) -> String {
        let completed = self.completed();
        let mut parts = self.summary_header(&completed, SUMMARY_MEDIUM_STAGES);
        let start = completed.len().saturating_sub(SUMMARY_MEDIUM_STAGES);
        for stage in &completed[start..] {
            parts.push(stage.line());
            parts.extend(stage.details(Detail::Compact, "  "));
        }
        parts.extend(self.context_list(&completed));
        parts.push(String::new());
        parts.join("\n")
    }

    fn summary_low(&self) -> String {
        let completed = self.completed();
        let mut parts = self.summary_header(&completed, SUMMARY_LOW_STAGES);
        let start = completed.len().saturating_sub(SUMMARY_LOW_STAGES);
        for stage in &completed[start..] {
            parts.push(stage.line());
            parts.extend(stage.details(Detail::Low, "  "));
        }
        parts.push(String::new());
        parts.join("\n")
    }

    fn summary_header(&self, completed: &[Completed<'_>], window: usize) -> Vec<String> {
        let mut parts = vec![
            format!("Goal: {}", self.goal),
            format!("Run ID: {}", self.run_id),
            format!("Completed {} stage(s) so far.", completed.len()),
        ];
        if completed.len() > window {
            parts.push(format!(
                "\n({} earlier stage(s) omitted)",
                completed.len() - window
            ));
        }
        parts.push("\nRecent stages:".to_owned());
        parts
    }

    /// The `## Context` list: public keys no stage rendered.
    fn context_list(&self, completed: &[Completed<'_>]) -> Vec<String> {
        let rows = self.context_rows(completed);
        if rows.is_empty() {
            return Vec::new();
        }
        let mut parts = vec!["\n## Context".to_owned()];
        for (key, value) in rows {
            parts.push(format!("- {key}: {value}"));
        }
        parts
    }

    fn context_rows(&self, completed: &[Completed<'_>]) -> Vec<(String, String)> {
        let Value::Object(kv) = self.kv else {
            return Vec::new();
        };
        let rendered: BTreeSet<&str> = completed
            .iter()
            .flat_map(|stage| stage.rendered.iter().map(String::as_str))
            .collect();
        kv.iter()
            .filter(|(key, value)| {
                !is_hidden_key(key) && !rendered.contains(key.as_str()) && !is_blank(value)
            })
            .map(|(key, value)| (key.clone(), render_value(value)))
            .collect()
    }
}

/// One completed stage's record as the preambles show it; `None` for the
/// structural stages (`start`, `exit`, the goal check) and unfinished ones.
fn completed_stage<'a>(
    id: &'a str,
    info: Option<&'a StageInfo>,
    record: &Value,
) -> Option<Completed<'a>> {
    let kind = info.and_then(|s| s.kind.as_deref());
    if id == "start" || id == GOAL_CHECK_NODE || matches!(kind, Some("start" | "exit")) {
        return None;
    }
    let output = record.get("output").cloned().unwrap_or(Value::Null);
    let status = output
        .get("outcome")
        .and_then(Value::as_str)
        .map_or_else(|| status_word(record), str::to_owned);
    let reason = output
        .get("failure_reason")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let notes = output
        .get("notes")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let is_llm = matches!(kind, Some("agent" | "prompt"))
        || output.get("text").is_some() && output.get("stdout").is_none();
    let mut rendered = Vec::new();
    let stdout = output
        .get("stdout")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if stdout.is_some() {
        rendered.push("command.output".to_owned());
    }
    let text = output
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if is_llm {
        rendered.push(format!("response.{id}"));
        rendered.push("last_stage".to_owned());
        rendered.push("last_response".to_owned());
    }
    let model = info.and_then(|s| s.model.clone()).or_else(|| {
        output
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    Some(Completed {
        id,
        kind: kind.or(if is_llm {
            Some("agent")
        } else if stdout.is_some() {
            Some("command")
        } else {
            None
        }),
        script: info.and_then(|s| s.script.as_deref()),
        status: leak_status(&status),
        notes,
        reason,
        output: stdout,
        model: if is_llm { model } else { None },
        text: if is_llm { text } else { None },
        files: output
            .get("files_touched")
            .and_then(Value::as_array)
            .and_then(|paths| {
                let paths: Vec<&str> = paths.iter().filter_map(Value::as_str).collect();
                // JSON keeps line breaks, quotes and delimiters in paths unambiguous.
                (!paths.is_empty()).then(|| render_value(&serde_json::json!(paths)))
            }),
        rendered,
    })
}

impl Completed<'_> {
    /// The stage's summary line: its id and status, with the notes and the
    /// failure reason when it has them.
    fn line(&self) -> String {
        let mut line = format!("- {}: {}", self.id, self.status);
        if let Some(notes) = &self.notes {
            let _ = write!(line, " ({notes})");
        }
        if let Some(reason) = &self.reason {
            let _ = write!(line, " [reason: {reason}]");
        }
        line
    }

    /// The bullet lines under the stage's heading, each prefixed with
    /// `indent`.
    fn details(&self, detail: Detail, indent: &str) -> Vec<String> {
        let handler = self.kind.map(|kind| format!("{indent}- Handler: {kind}"));
        let script = self
            .script
            .map(|script| format!("{indent}- Script: `{script}`"));
        let model = self
            .model
            .as_deref()
            .map(|model| format!("{indent}- Model: {model}"));
        let files = self
            .files
            .as_deref()
            .map(|files| format!("{indent}- Files: {files}"));
        let mut parts = Vec::new();
        match detail {
            Detail::Compact => match self.kind {
                Some("command") => {
                    parts.extend(script);
                    parts.extend(self.output_block(COMPACT_OUTPUT_MAX_LINES, indent));
                }
                Some("agent" | "prompt") => {
                    parts.extend(model);
                    parts.extend(files);
                }
                _ => {}
            },
            Detail::Low => {
                parts.extend(handler);
                parts.extend(script);
                parts.extend(model);
            }
            Detail::High => {
                parts.extend(handler);
                parts.extend(script);
                parts.extend(self.output_block(SUMMARY_HIGH_OUTPUT_MAX_LINES, indent));
                parts.extend(model);
                parts.extend(files);
                if let Some(text) = &self.text {
                    parts.push(format!("{indent}- Response:"));
                    parts.extend(
                        bounded(text)
                            .lines()
                            .map(|line| format!("{indent}  > {line}")),
                    );
                }
                parts.extend(
                    self.notes
                        .as_deref()
                        .map(|notes| format!("{indent}- Notes: {notes}")),
                );
                parts.extend(
                    self.reason
                        .as_deref()
                        .map(|reason| format!("{indent}- Failure reason: {reason}")),
                );
            }
        }
        parts
    }

    /// The command's output as a fenced block of its last `max_lines`
    /// lines, or `(empty)`; nothing for a stage that produced none.
    fn output_block(&self, max_lines: usize, indent: &str) -> Vec<String> {
        let Some(output) = self.output.as_deref() else {
            return Vec::new();
        };
        let output = output.trim();
        if output.is_empty() {
            return vec![format!("{indent}- Output: (empty)")];
        }
        vec![
            format!("{indent}- Output:"),
            format!("{indent}  ```"),
            tail_lines(output, max_lines, &format!("{indent}  ")),
            format!("{indent}  ```"),
        ]
    }
}

/// The last `max` lines of `text`, each prefixed with `indent`, with an
/// omission note when lines were dropped.
fn tail_lines(text: &str, max: usize, indent: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut out = Vec::new();
    if lines.len() > max {
        out.push(format!("{indent}({} lines omitted)", lines.len() - max));
    }
    let start = lines.len().saturating_sub(max);
    for line in &lines[start..] {
        out.push(format!("{indent}{line}"));
    }
    out.join("\n")
}

/// A context value as the preamble shows it: a string as written, anything
/// else as JSON, bounded to [`INLINE_VALUE_MAX`].
fn render_value(value: &Value) -> String {
    let text = match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    bounded(&text)
}

fn bounded(text: &str) -> String {
    if text.len() <= INLINE_VALUE_MAX {
        return text.to_owned();
    }
    let preview: String = text.chars().take(PREVIEW_CHARS).collect();
    format!(
        "({} bytes; too large to inline) Preview: {preview}…",
        text.len()
    )
}

/// Keys Fabro keeps out of preambles: engine state, graph mirrors, thread
/// bookkeeping, and the keys a stage's own output already shows.
fn is_hidden_key(key: &str) -> bool {
    key.starts_with("internal.")
        || key.starts_with("graph.")
        || key.starts_with("thread.")
        || key.starts_with("current")
        || key.starts_with("response.")
        || matches!(
            key,
            "outcome" | "last_stage" | "last_response" | "preferred_label" | "failure_class"
        )
}

fn is_blank(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.trim().is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(map) => map.is_empty(),
        _ => false,
    }
}

/// The Fabro outcome word for a node record without a reported `outcome`.
fn status_word(record: &Value) -> String {
    let tag = record.get("status").and_then(Value::as_str).unwrap_or("?");
    let status = match tag {
        "success" => ir::Status::Success,
        "partial" => ir::Status::partial_clean(),
        "skipped" => ir::Status::Skipped,
        "cancelled" => ir::Status::Cancelled,
        "timed_out" => ir::Status::TimedOut,
        _ => ir::Status::failure("failed"),
    };
    fabro_outcome(&status).as_str().to_owned()
}

/// The four Fabro outcome words are static; anything else is shown as
/// `failed`.
fn leak_status(status: &str) -> &'static str {
    match status {
        "succeeded" => "succeeded",
        "partially_succeeded" => "partially_succeeded",
        "skipped" => "skipped",
        _ => "failed",
    }
}

/// The `stages` list from a step config.
pub fn stages(value: &Value) -> Vec<StageInfo> {
    serde_json::from_value(value.clone()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::{Map, json};

    use super::*;

    fn preamble<'a>(nodes: &'a Value, kv: &'a Value, stages: &'a [StageInfo]) -> Preamble<'a> {
        Preamble {
            goal: "Add a /health endpoint",
            run_id: "run-1",
            stages,
            nodes,
            kv,
        }
    }

    fn stage(id: &str, kind: &str, script: Option<&str>) -> StageInfo {
        StageInfo {
            id:     id.into(),
            kind:   Some(kind.into()),
            script: script.map(Into::into),
            model:  None,
        }
    }

    #[test]
    fn every_mode_renders_deterministically_and_full_is_empty() {
        let stages = vec![
            stage("start", "start", None),
            stage("plan", "agent", None),
            stage("test", "command", Some("cargo test")),
            stage("exit", "exit", None),
        ];
        let nodes = json!({
            "start": {"status": "success", "output": null},
            "plan": {"status": "success", "output": {"text": "Plan it.", "outcome": "succeeded", "model": "claude"}},
            "test": {"status": "failure", "output": {"stdout": "line 1\nline 2\n", "outcome": "failed", "failure_reason": "exit 1"}},
        });
        let kv = json!({"tests_passed": false, "response.plan": "Plan it.", "internal.run_id": "x", "last_stage": "test", "command.output": "line 1\nline 2\n"});
        let p = preamble(&nodes, &kv, &stages);
        assert_eq!(p.render(Fidelity::Full), "");
        assert_eq!(
            p.render(Fidelity::Truncate),
            "Goal: Add a /health endpoint\nRun ID: run-1\n"
        );
        let compact = p.render(Fidelity::Compact);
        assert_eq!(
            compact,
            "Goal: Add a /health endpoint\n\n## Completed stages\n- **plan**: succeeded\n  - Model: claude\n- **test**: failed\n  - Script: `cargo test`\n  - Output:\n    ```\n    line 1\n    line 2\n    ```\n\n## Context\n- tests_passed: false\n"
        );
        assert_eq!(
            p.render(Fidelity::SummaryHigh),
            "Goal: Add a /health endpoint\nRun ID: run-1\nPipeline progress: 2 of 2 stages completed\n\n## Stage: plan\n- Status: succeeded\n- Handler: agent\n- Model: claude\n- Response:\n  > Plan it.\n\n## Stage: test\n- Status: failed\n- Handler: command\n- Script: `cargo test`\n- Output:\n  ```\n  line 1\n  line 2\n  ```\n- Failure reason: exit 1\n\n## Current context\n| Key | Value |\n|-----|-------|\n| tests_passed | false |\n"
        );
        assert_eq!(
            p.render(Fidelity::SummaryLow),
            "Goal: Add a /health endpoint\nRun ID: run-1\nCompleted 2 stage(s) so far.\n\nRecent stages:\n- plan: succeeded\n  - Handler: agent\n  - Model: claude\n- test: failed [reason: exit 1]\n  - Handler: command\n  - Script: `cargo test`\n"
        );
        assert_eq!(
            p.render(Fidelity::SummaryMedium),
            "Goal: Add a /health endpoint\nRun ID: run-1\nCompleted 2 stage(s) so far.\n\nRecent stages:\n- plan: succeeded\n  - Model: claude\n- test: failed [reason: exit 1]\n  - Script: `cargo test`\n  - Output:\n    ```\n    line 1\n    line 2\n    ```\n\n## Context\n- tests_passed: false\n"
        );
        assert_eq!(p.prompt(Fidelity::Full, "Do it."), "Do it.");
        assert!(
            p.prompt(Fidelity::Compact, "Do it.")
                .ends_with("\n\n\nDo it.")
        );
    }

    #[test]
    fn tracked_files_follow_fidelity_and_value_bounds() {
        let stages = vec![stage("edit", "agent", None)];
        let paths = json!(["normal.txt", "odd\n\"name.txt"]);
        let mut nodes = json!({"edit": {"status":"success", "output":{"files_touched":paths}}});
        let kv = json!({});
        let p = preamble(&nodes, &kv, &stages);
        for mode in [
            Fidelity::Compact,
            Fidelity::SummaryMedium,
            Fidelity::SummaryHigh,
        ] {
            let text = p.render(mode);
            assert!(text.contains(&format!("Files: {paths}")), "{text}");
            assert!(
                !text.contains("odd\n"),
                "a filename must not inject a new line"
            );
        }
        for mode in [Fidelity::Full, Fidelity::Truncate, Fidelity::SummaryLow] {
            assert!(!p.render(mode).contains("Files:"));
        }
        for absent in [Value::Null, json!([]), json!(42), json!([null, 7])] {
            nodes["edit"]["output"]["files_touched"] = absent;
            assert!(
                !preamble(&nodes, &kv, &stages)
                    .render(Fidelity::Compact)
                    .contains("Files:")
            );
        }
        let large = json!(["x".repeat(INLINE_VALUE_MAX + 1)]);
        nodes["edit"]["output"]["files_touched"] = large.clone();
        let text = preamble(&nodes, &kv, &stages).render(Fidelity::Compact);
        assert!(text.contains("too large to inline"), "{text}");
        assert!(text.len() < INLINE_VALUE_MAX);
        assert_eq!(
            nodes["edit"]["output"]["files_touched"], large,
            "rendering preserves durable paths"
        );
    }

    #[test]
    fn summaries_window_recent_stages_and_bound_values() {
        let stages: Vec<StageInfo> = (1..=7)
            .map(|i| stage(&format!("s{i}"), "command", None))
            .collect();
        let nodes: Value = (1..=7)
            .map(|i| (format!("s{i}"), json!({"status": "success", "output": {"stdout": "ok", "outcome": "succeeded"}})))
            .collect::<Map<_, _>>()
            .into();
        let big = "x".repeat(INLINE_VALUE_MAX + 10);
        let kv = json!({"big": big, "empty": ""});
        let p = preamble(&nodes, &kv, &stages);
        let low = p.render(Fidelity::SummaryLow);
        assert!(low.contains("(5 earlier stage(s) omitted)"), "{low}");
        assert!(
            low.contains("- s6: succeeded")
                && low.contains("- s7: succeeded")
                && !low.contains("- s5:"),
            "{low}"
        );
        let medium = p.render(Fidelity::SummaryMedium);
        assert!(medium.contains("(2 earlier stage(s) omitted)"), "{medium}");
        assert!(medium.contains("too large to inline"), "{medium}");
        assert!(
            !medium.contains("- empty:"),
            "blank values are skipped: {medium}"
        );
        let long = (1..=30)
            .map(|i| format!("l{i}"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert!(tail_lines(long.trim(), 25, "  ").starts_with("  (5 lines omitted)\n  l6"));
    }

    #[test]
    fn resolution_follows_fabro_including_branches_and_resume() {
        let config = ThreadConfig {
            fidelity:         Some("full".into()),
            default_fidelity: None,
            thread_id:        None,
            default_thread:   None,
            classes:          vec!["impl".into()],
        };
        let incoming = Incoming {
            from:      Some("plan".into()),
            fidelity:  None,
            thread_id: None,
        };
        let r = resolve(&config, &incoming, false, false);
        assert_eq!(
            (r.fidelity, r.fidelity_source),
            (Fidelity::Full, Source::Node)
        );
        assert_eq!(
            (r.thread.as_deref(), r.thread_source),
            (Some("impl"), Some(Source::Class))
        );
        let r = resolve(&config, &incoming, true, false);
        assert_eq!(
            (r.fidelity, r.fidelity_source),
            (Fidelity::SummaryHigh, Source::Branch)
        );
        assert_eq!((r.thread, r.thread_source), (None, None));
        let r = resolve(&config, &incoming, false, true);
        assert_eq!(
            (r.fidelity, r.fidelity_source),
            (Fidelity::SummaryHigh, Source::Resume)
        );
        let edge = Incoming {
            from:      Some("plan".into()),
            fidelity:  Some("truncate".into()),
            thread_id: Some("side".into()),
        };
        let r = resolve(&config, &edge, false, false);
        assert_eq!(
            (r.fidelity, r.fidelity_source),
            (Fidelity::Truncate, Source::Edge)
        );
        assert_eq!(
            (r.thread.as_deref(), r.thread_source),
            (Some("side"), Some(Source::Edge))
        );
        let bare = resolve(
            &ThreadConfig::default(),
            &Incoming::from_value(&json!({"from": "a"})),
            false,
            false,
        );
        assert_eq!(
            (bare.fidelity, bare.fidelity_source),
            (Fidelity::Compact, Source::Default)
        );
        assert_eq!(
            (bare.thread.as_deref(), bare.thread_source),
            (Some("a"), Some(Source::Previous))
        );
        assert_eq!(Incoming::from_value(&json!(null)), Incoming::default());
    }
}
