//! The Fabro frontend: Fabro's workflow bundle on top of the Attractor
//! language.
//!
//! Fabro runs Attractor graphs (`*.fabro`, `*.dot`) with a settings layer
//! around them: `workflow.toml` beside the workflow, `.fabro/project.toml` at
//! the bundle root, and the operator's `~/.fabro/settings.toml`. This crate
//! reads those files the way Fabro reads them, resolves them into the
//! [`RunSettings`] the Attractor lowering applies, and lowers through
//! [`frontend_attractor::lower`]. The graph it returns carries Fabro's launch
//! record (`fabro.launch`, `fabro.environment`) beside the language's own
//! parameters.
//!
//! What is Fabro's here: the files and their layering, `[run.inputs]`
//! defaults under the host's `--input`, `[run] goal`, `[run.model]` with its
//! fallback chains, `[run.execution]`, `[run.environment]`,
//! `[run.clone]`, `[[run.hooks]]`, `[run.agent.mcps]`, the launch precedence
//! (`petri run --model`, `--provider`, `--environment`, `--goal`, `--dry-run`,
//! `--auto-approve`, `--backend`), and the `.fabro` bundle root. What the graph
//! runs, the step kinds, and every construct in the DOT file are the language's
//! (`crates/attractor/FORMAT.md`). `crates/fabro/FORMAT.md` says how each
//! file section lowers.

mod agent_permissions;
pub mod fallbacks;
pub mod hooks;
pub mod mcps;
mod model_layers;
mod secrets;
mod skills;
pub mod workflow_toml;

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use frontend::{
    CompileInputs, Diagnostics, FileSource, Frontend, LaunchSettings, Lowered, NoFiles,
    WorkspaceRetention,
};
pub use frontend_attractor::RunSettings;
use frontend_attractor::template::Context;
pub use hooks::{PROJECT_FILE, SETTINGS_HOOKS_VAR};
pub use mcps::MCP_CATALOG_VAR;
pub use model_layers::LaunchModel;
use serde_json::Value;
use smol_str::SmolStr;
pub use workflow_toml::{ENVIRONMENT_PARAM, LAUNCH_PARAM, Settings};

/// The warning that marks a settings key Fabro's reference schema refuses as
/// a Petri extension.
pub(crate) const PETRI_EXTENSION: &str = "fabro.petri_extension";

/// Read the settings layers beside `file` and lower the workflow under them.
/// `file` is the repository-relative path the spans carry and `@file`
/// references resolve beside; `files` reads them and the settings files;
/// `inputs` is what the host supplied, which `[run.inputs]` defaults fill in
/// under.
pub fn load(file: &str, text: &str, files: &dyn FileSource, inputs: &CompileInputs) -> Lowered {
    let mut diags = Diagnostics::new();
    let mut template = Context::new(inputs);
    let mut settings = workflow_toml::read(file, files, inputs, &mut template, &mut diags);
    if diags.has_errors() {
        return Lowered::rejected(diags);
    }
    model_layers::apply(files, inputs, &mut settings.run.model, &mut diags);
    settings.run.permissions =
        agent_permissions::resolve(files, inputs, settings.agent_permissions, &mut diags);
    // The launch itself: what `petri run --model`/`--provider` asked for
    // overrides the files and the graph's defaults, and the host's default
    // fills what nothing else set.
    settings.launch = LaunchModel::from_inputs(inputs);
    settings.launch.fill_defaults(&mut settings.run.model);
    settings.run.model_override = settings.launch.model_override();
    settings.run.hooks = hooks::load(files, inputs, settings.hooks_text.as_ref(), &mut diags);
    settings.run.mcps = mcps::load(
        files,
        inputs,
        settings.hooks_text.as_ref(),
        &template,
        &mut diags,
    );
    // The resolved inputs: the host's over the file defaults, as the template
    // context now holds them.
    let to_map = |map: &BTreeMap<String, Value>| {
        map.iter()
            .map(|(k, v)| (SmolStr::new(k), v.clone()))
            .collect()
    };
    let resolved = CompileInputs {
        inputs:             to_map(template.inputs()),
        vars:               to_map(template.vars()),
        unbound_is_warning: inputs.unbound_is_warning,
    };
    let repository = inputs
        .vars
        .get(frontend::REPOSITORY_VAR)
        .and_then(Value::as_str)
        .map(str::to_owned);
    let launch = settings.launch_param(repository.as_deref());
    let environment = settings.environment_param();
    let mut lowered = frontend_attractor::lower(file, text, files, &resolved, settings.run, diags);
    if let Some(graph) = lowered.graph.as_mut() {
        graph.params.insert(SmolStr::new(LAUNCH_PARAM), launch);
        if let Some(environment) = environment {
            graph
                .params
                .insert(SmolStr::new(ENVIRONMENT_PARAM), environment);
        }
    }
    lowered
}

/// [`load`] with no repository: no settings files, and every `@file`
/// reference is missing.
pub fn load_text(file: &str, text: &str) -> Lowered {
    load(file, text, &NoFiles, &CompileInputs::new())
}

/// Fabro, as a [`Frontend`]: it claims `*.fabro` and `*.dot`.
#[derive(Debug, Default)]
pub struct Fabro {
    /// The host's user settings layer (`$FABRO_HOME/settings.toml`), bound
    /// as the `fabro.settings_toml` variable of every load that does not
    /// bind its own.
    settings_toml:    Option<String>,
    /// The host's MCP catalog, bound as the `fabro.mcp_catalog_toml`
    /// variable the same way.
    mcp_catalog_toml: Option<String>,
}

impl Fabro {
    pub fn new() -> Self {
        Self::default()
    }

    /// Carry the user settings layer's text into every load. Fabro reads
    /// `~/.fabro/settings.toml` (`$FABRO_HOME` when set) for hooks, MCP
    /// servers, `[run.model]` defaults and `[environments.*]`; the host
    /// reads the file and hands the text here, so lowering stays free of
    /// environment reads. A Fabro server hands its environment catalog
    /// here too, as `[environments.<id>]` tables.
    #[must_use]
    pub fn with_settings_toml(mut self, text: Option<String>) -> Self {
        self.settings_toml = text;
        self
    }

    /// Carry the host's MCP catalog into every load: a TOML table keyed by
    /// catalog id, each entry in the inline `[run.agent.mcps.<name>]`
    /// shape, which `[run.agent.mcps.<name>] id = "..."` references resolve
    /// against ([`MCP_CATALOG_VAR`]).
    #[must_use]
    pub fn with_mcp_catalog_toml(mut self, text: Option<String>) -> Self {
        self.mcp_catalog_toml = text;
        self
    }

    /// The host's layers, bound as compile variables of a load that does
    /// not bind them itself.
    fn bind(&self, inputs: &CompileInputs) -> Option<CompileInputs> {
        let unbound = [
            (SETTINGS_HOOKS_VAR, &self.settings_toml),
            (MCP_CATALOG_VAR, &self.mcp_catalog_toml),
        ]
        .into_iter()
        .filter_map(|(var, text)| Some((var, text.as_deref()?)))
        .filter(|(var, _)| !inputs.vars.contains_key(*var))
        .collect::<Vec<_>>();
        if unbound.is_empty() {
            return None;
        }
        let mut bound = inputs.clone();
        for (var, text) in unbound {
            bound
                .vars
                .insert(SmolStr::new(var), Value::String(text.to_owned()));
        }
        Some(bound)
    }
}

impl Frontend for Fabro {
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the `Frontend` trait fixes this signature; an impl cannot widen the lifetime"
    )]
    fn name(&self) -> &str {
        "fabro"
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
        match self.bind(inputs) {
            Some(bound) => load(file, text, files, &bound),
            None => load(file, text, files, inputs),
        }
    }

    /// The launch settings `workflow.toml` declared, read back from the
    /// persisted graph's `fabro.launch` parameter.
    /// Every root graph this frontend lowers carries the launch parameter.
    fn claims_graph(&self, graph: &ir::Graph) -> bool {
        graph.params.contains_key(LAUNCH_PARAM)
    }

    fn launch_settings(&self, graph: &ir::Graph) -> LaunchSettings {
        let Some(launch) = graph.params.get(LAUNCH_PARAM) else {
            return LaunchSettings::default();
        };
        LaunchSettings {
            sandbox_backend: launch["sandbox_backend"].as_str().map(str::to_owned),
            dry_run:         launch["dry_run"].as_bool().unwrap_or(false),
            auto_approve:    launch["auto_approve"].as_bool().unwrap_or(false),
        }
    }

    /// A Fabro run's result is the files its stages produced or changed, so a
    /// standalone run keeps its workspace after success, failure and
    /// cancellation; only an explicit `--retain never` deletes it.
    fn default_retention(&self) -> WorkspaceRetention {
        WorkspaceRetention::Always
    }

    /// The nearest ancestor holding a `.fabro` directory — the bundle root
    /// Fabro resolves `fabro/...` paths against — else the file's own
    /// directory. A file inside the bundle itself belongs to the bundle's
    /// parent, never to `.fabro`.
    fn repo_root(&self, file: &Path) -> PathBuf {
        let dir = file
            .parent()
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let mut current = Some(dir.as_path());
        while let Some(candidate) = current {
            if candidate.join(".fabro").is_dir() {
                return candidate.to_path_buf();
            }
            if candidate
                .components()
                .next_back()
                .is_some_and(|c| c == Component::Normal(".fabro".as_ref()))
            {
                return candidate
                    .parent()
                    .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
            }
            current = candidate.parent();
        }
        dir
    }
}
