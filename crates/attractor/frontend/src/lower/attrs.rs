//! Which attributes mean something, which are pure Graphviz layout, and
//! which are refused. Anything not listed here is diagnosed: a value that
//! silently does nothing is the failure this table exists to prevent.

use std::mem;

/// Graphviz attributes that only affect a rendering. Dropped without a word:
/// they carry no workflow meaning and every real `.fabro` file has some.
pub(super) const LAYOUT: &[&str] = &[
    "arrowhead",
    "arrowsize",
    "arrowtail",
    "bgcolor",
    "center",
    "clusterrank",
    "color",
    "comment",
    "compound",
    "concentrate",
    "constraint",
    "decorate",
    "dir",
    "distortion",
    "dpi",
    "fillcolor",
    "fixedsize",
    "fontcolor",
    "fontname",
    "fontsize",
    "group",
    "headport",
    "height",
    "href",
    "id",
    "image",
    "imagescale",
    "labelangle",
    "labeldistance",
    "labelfloat",
    "labeljust",
    "labelloc",
    "layout",
    "lhead",
    "ltail",
    "margin",
    "mclimit",
    "minlen",
    "newrank",
    "nodesep",
    "nojustify",
    "nslimit",
    "ordering",
    "orientation",
    "outputorder",
    "overlap",
    "pad",
    "page",
    "pencolor",
    "penwidth",
    "peripheries",
    "pos",
    "rank",
    "rankdir",
    "ranksep",
    "ratio",
    "regular",
    "remincross",
    "rotate",
    "samehead",
    "sametail",
    "searchsize",
    "shapefile",
    "sides",
    "size",
    "skew",
    "splines",
    "style",
    "tailport",
    "target",
    "tooltip",
    "URL",
    "width",
    "xlabel",
];

/// Graph-level attributes with workflow meaning.
pub(super) const GRAPH: &[&str] = &[
    "goal",
    "default_thread",
    "label",
    "backend",
    "model_stylesheet",
    "default_max_retries",
    "retry_target",
    "fallback_retry_target",
    "on_failure",
    "on_retries_exhausted",
    "default_fidelity",
    "default_model",
    "default_permissions",
    "default_provider",
    "max_node_visits",
    "selection",
    "acp.command",
    "acp.config",
    "stall_timeout",
    "loop_restart_signature_limit",
];

/// Graph-level attributes Petri accepts but does not act on, each named in
/// an `ignored.*` warning. Empty: every graph attribute Fabro defines is
/// acted on.
pub(super) const GRAPH_IGNORED: &[(&str, &str)] = &[];

/// Node attributes with workflow meaning.
pub(super) const NODE: &[&str] = &[
    "label",
    "shape",
    "type",
    "backend",
    "prompt",
    "script",
    "language",
    "for_each",
    "max_parallel",
    "stdin_source",
    "output_schema",
    "output_retries",
    "max_retries",
    "retry_policy",
    "max_visits",
    "goal_gate",
    "retry_target",
    "fallback_retry_target",
    "on_failure",
    "on_retries_exhausted",
    "allow_partial",
    "fidelity",
    "thread_id",
    "project_memory",
    "permissions",
    "speed",
    "max_tokens",
    "timeout",
    "model",
    "provider",
    "reasoning_effort",
    "acp.command",
    "acp.config",
    "selection",
    "class",
    "question_type",
    "sensitive",
    "review_target",
    "human.default_choice",
    "duration",
    "stack.child_workflow",
    "stack.child_dot_source",
    "manager.max_cycles",
    "manager.poll_interval",
    "manager.stop_condition",
];

/// Node attributes Petri accepts but does not act on, each named in an
/// `ignored.*` warning. Empty: every node attribute Fabro defines is acted
/// on; `tool_hooks.*` is not one and is diagnosed as unknown.
pub(super) const NODE_IGNORED: &[(&str, &str)] = &[];

/// Edge attributes with workflow meaning.
pub(super) const EDGE: &[&str] = &[
    "label",
    "condition",
    "weight",
    "fidelity",
    "thread_id",
    "loop_restart",
    "freeform",
    "human.description",
    "human.preview",
];

/// The legacy dialect's attributes, refused as `unsupported.legacy_dialect`.
/// The draft specification follows the reference implementation, which no
/// longer reads them.
pub(super) const LEGACY_DIALECT: &[&str] = &["llm_prompt", "is_codergen", "node_type"];

/// The extension namespace. An attribute under it belongs to tooling or a
/// host, carries no workflow meaning to Petri, and is dropped without a
/// word, as a layout attribute is. Everything else Fabro does not define is
/// refused: a misspelt attribute that silently did nothing is the failure
/// this table exists to prevent.
pub(super) const EXTENSION_PREFIX: &str = "x.";

/// The attribute in `known` closest to `key`, when it is close enough to be
/// the one the author meant: within two single-character edits, the
/// distance of a typo (`max_retrys`) or a swapped separator
/// (`stack_child_workflow`).
pub(super) fn closest<'a>(key: &str, known: &[&'a str]) -> Option<&'a str> {
    const MAX_DISTANCE: usize = 2;
    known
        .iter()
        .copied()
        .filter(|candidate| *candidate != key)
        .map(|candidate| (edit_distance(key, candidate), candidate))
        .filter(|(distance, _)| *distance <= MAX_DISTANCE)
        .min_by_key(|(distance, candidate)| (*distance, candidate.len()))
        .map(|(_, candidate)| candidate)
}

/// The Levenshtein distance between two attribute names.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let substitution = previous[j] + usize::from(ca != cb);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
}

/// The Fabro fidelity modes.
pub(super) const FIDELITIES: &[&str] = &[
    "full",
    "truncate",
    "compact",
    "summary:low",
    "summary:medium",
    "summary:high",
];

/// The Fabro question types a human gate may declare.
pub(super) const QUESTION_TYPES: &[&str] = &[
    "yes_no",
    "confirmation",
    "multiple_choice",
    "multi_select",
    "freeform",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closest_names_a_typo_and_stays_quiet_on_an_unrelated_name() {
        assert_eq!(closest("max_retrys", NODE), Some("max_retries"));
        assert_eq!(
            closest("stack_child_workflow", NODE),
            Some("stack.child_workflow")
        );
        assert_eq!(
            closest("stack.child_workflo", NODE),
            Some("stack.child_workflow")
        );
        assert_eq!(closest("frobnicate", NODE), None);
        assert_eq!(
            closest("label", NODE),
            None,
            "an exact match is not a suggestion"
        );
    }

    #[test]
    fn edit_distance_counts_single_character_edits() {
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
        assert_eq!(edit_distance("model", "model"), 0);
    }
}
