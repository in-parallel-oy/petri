//! The step kinds a lowered Fabro graph names.
//!
//! The frontend depends on the names, not on the implementations: the real
//! steps and the stub registry both register under these ids, and which
//! registry a run uses is the distribution's choice.

use ir::StepKindId;
use serde::{Deserialize, Serialize};

/// An agent node (`box`): one turn of an agent over ACP or native Pebble.
pub const AGENT_KIND: StepKindId = StepKindId::new_static("attractor/agent");
/// A prompt node (`tab`, or a `tripleoctagon` with a `prompt`): one model
/// call through the application's model client, with no agent tools.
pub const PROMPT_KIND: StepKindId = StepKindId::new_static("attractor/prompt");
/// A command node (`parallelogram`, or any node with a `script`).
pub const COMMAND_KIND: StepKindId = StepKindId::new_static("attractor/command");
/// A human gate (`hexagon`).
pub const HUMAN_KIND: StepKindId = StepKindId::new_static("attractor/human");
/// A wait node (`insulator`).
pub const WAIT_KIND: StepKindId = StepKindId::new_static("attractor/wait");
/// A manager loop (`house`): a nested workflow.
pub const WORKFLOW_KIND: StepKindId = StepKindId::new_static("attractor/workflow");
/// A parallel node (`component`) itself: the fork. It runs once per visit,
/// takes the fork-time snapshot of the parent's context and stage records,
/// offloads what a fan-out would otherwise copy into every branch child, and
/// hands the snapshot to the branches as its output.
pub const FORK_KIND: StepKindId = StepKindId::new_static("attractor/fork");
/// One branch of a parallel node (`component`): the branch target runs in
/// a child invocation with its own context and the parent's sandbox, and
/// the step returns the branch's result envelope.
pub const BRANCH_KIND: StepKindId = StepKindId::new_static("attractor/branch");
/// A plain fan-in (`tripleoctagon`): the barrier that collects the branch
/// envelopes in branch order and publishes `parallel.results`.
pub const FAN_IN_KIND: StepKindId = StepKindId::new_static("attractor/fan_in");
/// The field of a fork's output that carries the fork snapshot of `kv`; the
/// branch delegates read it as their `kv`.
pub const FORK_SNAPSHOT_FIELD: &str = "snapshot";
/// The field of a fork's output that carries the fork-time stage records,
/// when a branch target is an agent or prompt node.
pub const FORK_NODES_FIELD: &str = "nodes";
/// The field of a fork's output that names the fork occurrence: `{ fork,
/// firing }`, the parallel node's name and the fork step's firing in its
/// execution. The branch delegates carry it in their call slot and their
/// events and the fan-in in its completion event, so every Fabro parallel
/// event of one fork visit names the same occurrence the typed
/// `fork_started` does.
pub const FORK_OCCURRENCE_FIELD: &str = "occurrence";
/// The hydrated source array on a dynamic fork's output. Expansion reads it
/// from the fork's record; it is excluded from branch input tokens.
pub const FORK_ITEMS_FIELD: &str = "items";
/// The context key a branch child reads the parent's stage records from: the
/// fork snapshot its preamble is rendered from, since the child's own records
/// are empty when its target starts.
pub const BRANCH_NODES_KEY: &str = "internal.parallel_nodes";
/// The context key a `for_each` branch child reads its fenced item data from.
pub const BRANCH_ITEM_KEY: &str = "internal.parallel_item";
/// A structural stage (`start`, `exit`): runs nothing, records where its scope
/// runs so hooks placed in the sandbox find it, and fires the run-level hooks.
pub const STAGE_KIND: StepKindId = StepKindId::new_static("attractor/stage");
/// Reserved name of the synthetic node that enforces goal gates.
pub const GOAL_CHECK_NODE: &str = "goal_check";
/// The most repair turns one agent step accepts from configuration.
pub const MAX_OUTPUT_RETRIES: u64 = 100;

/// The structural kinds a parallel node lowers to. A dry run registers the
/// real steps for these: a fork still takes its snapshot, a branch still
/// invokes its child, and a fan-in still collects, while the stages inside
/// them are simulated.
pub const STRUCTURAL: &[&StepKindId] = &[&FORK_KIND, &BRANCH_KIND, &FAN_IN_KIND];

/// Every stage kind the frontend emits, for a registry that stubs them all.
pub const ALL: &[&StepKindId] = &[
    &AGENT_KIND,
    &PROMPT_KIND,
    &COMMAND_KIND,
    &HUMAN_KIND,
    &WAIT_KIND,
    &WORKFLOW_KIND,
    &STAGE_KIND,
];

/// The closed set of stage outcomes a Fabro step may report, and the only
/// values `outcome=X` may name in a condition.
pub const OUTCOMES: &[&str] = &["succeeded", "partially_succeeded", "failed", "skipped"];

/// The closed set of outcomes a Fabro stage reports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageOutcome {
    #[default]
    Succeeded,
    PartiallySucceeded,
    Failed,
    Skipped,
}

impl StageOutcome {
    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "succeeded" => Self::Succeeded,
            "partially_succeeded" => Self::PartiallySucceeded,
            "failed" => Self::Failed,
            "skipped" => Self::Skipped,
            _ => return None,
        })
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::PartiallySucceeded => "partially_succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

/// The failure class a step reports when it wants another attempt: the one
/// class the lowered retry policy retries on.
pub const RETRY_REQUESTED_CLASS: &str = "retry_requested";

/// The day the `outcome=success` condition alias expires. The alias names
/// this date in its warning and carries a `REMOVE AFTER 2026-10-04` comment.
/// `on_failure="succeed"` and `auto_status` are no longer shims: Fabro's
/// reference revision supports both, so Petri keeps them for as long as the
/// reference does.
///
/// REMOVE AFTER 2026-10-04: delete the alias and this constant together.
pub const COMPAT_SUNSET: &str = "2026-10-04";
