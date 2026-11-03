//! `[run.agent] permissions` across the settings layers, a Petri extension:
//! the bundle's `workflow.toml` wins, then `.fabro/project.toml`, then the
//! host's `settings.toml`, as `[run.model]` layers. Every layer that sets the
//! key is validated, even one a higher layer overrides.

use frontend::{CompileInputs, Diagnostics, FileSource, Span};
use frontend_attractor::AgentPermissions;
use serde_json::Value;

use crate::PETRI_EXTENSION;
use crate::hooks::{PROJECT_FILE, SETTINGS_HOOKS_VAR};

/// The `unsupported.*` feature of a value that is not a permission level.
pub(crate) const UNSUPPORTED: &str = "workflow_toml.run.agent.permissions";

/// Read the `permissions` value of `[run.agent]` in `path`. A value that is
/// not a level is refused; a level warns that Fabro would refuse the key.
pub(crate) fn read(
    value: &toml::Value,
    path: &str,
    span: &Span,
    diags: &mut Diagnostics,
) -> Option<AgentPermissions> {
    let Some(level) = value.as_str().and_then(|text| text.parse().ok()) else {
        diags.unsupported(
            UNSUPPORTED,
            span.clone(),
            format!("invalid `run.agent.permissions` in `{path}`"),
            &format!("use {}", AgentPermissions::expected()),
        );
        return None;
    };
    diags.warning(
        PETRI_EXTENSION,
        span.clone(),
        format!(
            "`run.agent.permissions` in `{path}` is a Petri extension; the reference Fabro \
             settings schema does not accept it"
        ),
    );
    Some(level)
}

/// The run default: `workflow` (what the bundle's `workflow.toml` set),
/// else the project layer's, else the host settings layer's, else `full`.
pub(crate) fn resolve(
    files: &dyn FileSource,
    inputs: &CompileInputs,
    workflow: Option<AgentPermissions>,
    diags: &mut Diagnostics,
) -> AgentPermissions {
    let project = files
        .read(PROJECT_FILE)
        .and_then(|text| layer(&text, PROJECT_FILE, diags));
    let settings = match inputs.vars.get(SETTINGS_HOOKS_VAR) {
        Some(Value::String(text)) => layer(text, "settings.toml", diags),
        _ => None,
    };
    workflow.or(project).or(settings).unwrap_or_default()
}

/// One lower layer's `[run.agent] permissions`. Malformed TOML is the other
/// layer readers' to report.
fn layer(text: &str, path: &str, diags: &mut Diagnostics) -> Option<AgentPermissions> {
    let table: toml::Table = text.parse().ok()?;
    let value = table
        .get("run")
        .and_then(toml::Value::as_table)
        .and_then(|run| run.get("agent"))
        .and_then(toml::Value::as_table)
        .and_then(|agent| agent.get("permissions"))?;
    read(value, path, &Span::file(path), diags)
}
