//! The Attractor frontend: Graphviz DOT workflows → HIR.
//!
//! Text in, `Graph` and diagnostics out, exactly as the GitHub Actions
//! frontend. The reference implementation (Fabro) renders templates,
//! resolves `@file` references and applies its model stylesheet once at run
//! creation and persists the literal graph; this frontend does the same at
//! load, so what the engine sees is literal.
//!
//! This crate lowers the language alone. It reads no settings file: a host
//! frontend that has one (Fabro's `workflow.toml` and its settings layers,
//! `frontend_fabro`) resolves it into a [`RunSettings`] and calls [`lower`].
//! [`Attractor`] is the bare frontend, with default settings.
//!
//! What lowers where:
//!
//! | Attractor                     | Petri                                            |
//! |-------------------------------|--------------------------------------------------|
//! | `Mdiamond` start              | `noop`, the graph entry                          |
//! | `Msquare` exit                | `noop`; `Completion::TerminalNode`               |
//! | `diamond` conditional         | `noop`                                           |
//! | `box` agent, `tab` prompt     | `attractor/agent`                                    |
//! | `parallelogram` command       | `attractor/command`                                  |
//! | `hexagon` human               | `attractor/human`                                    |
//! | `component` parallel          | fan-out groups, or `Expansion::ForEach`          |
//! | `tripleoctagon` fan-in        | `noop`, `join: all`, output = branch results     |
//! | `insulator` wait              | `attractor/wait`                                     |
//! | `house` manager loop          | `attractor/workflow`                                 |
//! | edge selection                | one `Tiered` group per node, four tiers          |
//! | `goal_gate`                   | a `goal_check` noop before exit with back arms   |
//! | `max_visits`, unlimited       | `Budget.max_firings`, capped at 500              |
//! | `loop_restart`                | `EdgeTransition::Restart`                        |
//!
//! Everything else is refused with a specific `unsupported.*` code. See
//! `crates/attractor/FORMAT.md` for the language as lowered.

pub mod condition;
pub mod dot;
pub mod fidelity;
pub mod hooks;
pub mod kinds;
pub mod labels;
mod lower;
pub use lower::fallbacks;
pub mod mcps;
pub mod model;
mod permissions;
pub use permissions::{AgentPermissions, UnknownPermissions};
pub mod stylesheet;
pub mod template;

use std::path::Path;

use frontend::{
    CompileInputs, Diagnostics, FileSource, Frontend, Lowered, NoFiles, WorkspaceRetention,
};
pub use kinds::MAX_FOR_EACH_ITEMS;
pub use lower::policy::{DEFAULT_SIGNATURE_LIMIT, DEFAULT_STALL_TIMEOUT};
pub use lower::{
    BRANCH_META_KIND, CloneSettings, CompactionSettings, DEFAULT_MAX_PARALLEL,
    DEFAULT_PRESERVE_TURNS, DEFAULT_THRESHOLD_PERCENT, EnvValue, Environment, FailurePolicy,
    IMPORT_ERROR, Kind, MAX_CALL_DEPTH, MAX_FIRINGS, MAX_INVOCATIONS, ModelDefaults, ModelOverride,
    Policy, ROUTES_KEY, RunSettings, SkillSettings, shape_of, skills, subagents,
};

/// Parse and lower one workflow under `settings`. `file` is the
/// repository-relative path the spans carry and `@file` references resolve
/// beside; `files` reads them; `inputs` are the run's inputs and variables,
/// resolved (a host frontend has applied its file defaults); `diags` is what
/// the caller diagnosed while resolving them, so an error there still
/// rejects the graph.
pub fn lower(
    file: &str,
    text: &str,
    files: &dyn FileSource,
    inputs: &CompileInputs,
    settings: RunSettings,
    mut diags: Diagnostics,
) -> Lowered {
    let dot = match dot::parse(file, text) {
        Ok(dot) => dot,
        Err(diagnostic) => {
            diags.push(diagnostic);
            return Lowered::rejected(diags);
        }
    };
    let workflow = model::build(&dot);
    lower::lower(workflow, file, files, inputs, settings, diags)
}

/// [`lower`] under the default settings: the bare language.
pub fn load(file: &str, text: &str, files: &dyn FileSource, inputs: &CompileInputs) -> Lowered {
    lower(
        file,
        text,
        files,
        inputs,
        RunSettings::default(),
        Diagnostics::new(),
    )
}

/// [`load`] with no repository: every `@file` reference is missing.
pub fn load_text(file: &str, text: &str) -> Lowered {
    load(file, text, &NoFiles, &CompileInputs::new())
}

/// The bare language, as a [`Frontend`]: it claims `*.fabro` and `*.dot`
/// and lowers under [`RunSettings::default`]. The shipped binary registers
/// the Fabro frontend instead, which wraps this one; a host that wants the
/// language without Fabro's settings files registers this.
#[derive(Debug, Default)]
pub struct Attractor;

impl Attractor {
    pub fn new() -> Self {
        Self
    }
}

impl Frontend for Attractor {
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the `Frontend` trait fixes this signature; an impl cannot widen the lifetime"
    )]
    fn name(&self) -> &str {
        "attractor"
    }

    fn claims(&self, path: &Path) -> bool {
        matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("fabro" | "dot")
        )
    }

    fn load(
        &self,
        file: &str,
        text: &str,
        files: &dyn FileSource,
        inputs: &CompileInputs,
    ) -> Lowered {
        load(file, text, files, inputs)
    }

    /// A run's result is the files its stages produced or changed, so a
    /// standalone run keeps its workspace after success, failure and
    /// cancellation; only an explicit `--retain never` deletes it.
    fn default_retention(&self) -> WorkspaceRetention {
        WorkspaceRetention::Always
    }
}
