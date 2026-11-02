//! `[run.agent] skills`: reading the skill directories a workflow names, a
//! Petri extension ([`frontend_attractor::lower::skills`] for what the list
//! does on an agent node).

use frontend::{Diagnostics, Span};
pub(crate) use frontend_attractor::SkillSettings;

/// The `unsupported.*` feature of a malformed `skills` value.
pub(crate) const UNSUPPORTED: &str = "workflow_toml.run.agent.skills";

/// Read the `skills` value of `[run.agent]`. A value that is not a list of
/// non-empty strings is refused; a non-empty list warns that Fabro would
/// refuse the key.
pub(crate) fn read(
    value: &toml::Value,
    path: &str,
    span: &Span,
    diags: &mut Diagnostics,
) -> SkillSettings {
    let Some(entries) = value.as_array() else {
        diags.unsupported(
            UNSUPPORTED,
            span.clone(),
            format!("`run.agent.skills` in `{path}` must be a list of directory paths"),
            "write `skills = [\"path/to/skills\"]`",
        );
        return SkillSettings::default();
    };
    let mut dirs = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some(dir) = entry.as_str().map(str::trim).filter(|dir| !dir.is_empty()) else {
            diags.unsupported(
                UNSUPPORTED,
                span.clone(),
                format!(
                    "every `run.agent.skills` entry in `{path}` must be a non-empty directory path"
                ),
                "write `skills = [\"path/to/skills\"]`",
            );
            return SkillSettings::default();
        };
        dirs.push(dir.to_owned());
    }
    if !dirs.is_empty() {
        diags.warning(
            crate::PETRI_EXTENSION,
            span.clone(),
            format!(
                "`run.agent.skills` in `{path}` is a Petri extension: Fabro's `[run.agent]` \
                 refuses the key; the listed directories are searched after Fabro's own"
            ),
        );
    }
    SkillSettings { dirs }
}
