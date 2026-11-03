//! Workflow imports, expanded at load as Fabro's `ImportTransform` expands
//! them.
//!
//! A node with `import="<path>"` is a placeholder for another workflow file.
//! The file's nodes are spliced in under the placeholder's id as a prefix
//! (`<placeholder>.<node>`), its start and exit sentinels are dropped, the
//! placeholder's incoming edges reach the imported entry node and its
//! outgoing edges leave the imported exit predecessor. The placeholder's own
//! attributes (a short allowed list) become defaults the imported nodes
//! inherit; its classes and a class made from its id propagate; `retry_target`
//! and `fallback_retry_target` inside the import are rewritten to the
//! prefixed ids. Imports nest; a cycle is refused. Every rule and message
//! below follows Fabro's transform, so a workflow Fabro rejects is rejected
//! here with the same reason.
//!
//! The expanded workflow is what lowers, so the persisted graph carries the
//! imported nodes and a replay never reads the imported file again.

use std::collections::HashSet;
use std::iter;

use frontend::{Diagnostics, FileSource, Span};

use crate::model::{AttrValue, Attrs, EdgeDecl, NodeDecl, Workflow};
use crate::{dot, model};

/// The diagnostic code every import problem carries, Fabro's `import_error`.
pub const IMPORT_ERROR: &str = "attractor.import";

/// The placeholder attributes an import may carry beside `import` and
/// `class`; each becomes a default the imported nodes inherit.
const PLACEHOLDER_ATTRS: &[&str] = &[
    "model",
    "provider",
    "reasoning_effort",
    "speed",
    "backend",
    "permissions",
    "acp.command",
    "acp.config",
    "fidelity",
    "max_retries",
    "thread_id",
];

/// Edge attributes that carry workflow meaning: a boundary edge of an import
/// may not carry one.
const SEMANTIC_EDGE_ATTRS: &[&str] = &[
    "condition",
    "label",
    "weight",
    "fidelity",
    "thread_id",
    "loop_restart",
    "freeform",
];

/// Expand every `import` placeholder in `workflow`, in place. `file` is the
/// workflow's own path (for the import stack) and `base_dir` its directory,
/// which relative import paths resolve against. Errors are diagnosed on the
/// placeholder; the placeholder then stays in the workflow with its `import`
/// attribute removed, so the rest of lowering can still run.
pub(super) fn expand(
    workflow: &mut Workflow,
    file: &str,
    base_dir: &str,
    root_base_dir: &str,
    files: &dyn FileSource,
    diags: &mut Diagnostics,
) {
    let mut stack = vec![normalize(file)];
    expand_with_stack(workflow, base_dir, root_base_dir, files, diags, &mut stack);
}

fn expand_with_stack(
    workflow: &mut Workflow,
    base_dir: &str,
    root_base_dir: &str,
    files: &dyn FileSource,
    diags: &mut Diagnostics,
    stack: &mut Vec<String>,
) {
    let placeholders: Vec<String> = workflow
        .nodes
        .iter()
        .filter(|node| node.attrs.contains("import"))
        .map(|node| node.id.clone())
        .collect();
    for id in placeholders {
        let Some(node) = workflow.node(&id) else {
            continue;
        };
        let Some(attr) = node.attrs.get("import") else {
            continue;
        };
        let span = attr.span.clone();
        let path = attr.value.as_text();
        if let Err(message) = expand_one(
            workflow,
            &id,
            &path,
            base_dir,
            root_base_dir,
            files,
            diags,
            stack,
        ) {
            poison(workflow, &id);
            diags.error(IMPORT_ERROR, span, format!("import `{id}`: {message}"));
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "one import expansion carries the file source, both base directories, the \
              diagnostics and the cycle stack; a struct would only rename the arguments"
)]
fn expand_one(
    workflow: &mut Workflow,
    placeholder_id: &str,
    path: &str,
    base_dir: &str,
    root_base_dir: &str,
    files: &dyn FileSource,
    diags: &mut Diagnostics,
    stack: &mut Vec<String>,
) -> Result<(), String> {
    if path.contains("{{") {
        return Err(format!(
            "import path `{path}` must be a static reference; templates are not allowed"
        ));
    }
    let placeholder = placeholder_options(workflow, placeholder_id)?;
    let resolved = normalize(&join(base_dir, path));
    if stack.iter().any(|entry| entry == &resolved) {
        let mut cycle = stack.clone();
        cycle.push(resolved);
        return Err(format!("circular import detected: {}", cycle.join(" -> ")));
    }
    let Some(text) = files.read(&resolved) else {
        return Err(format!("file not found: {path} (read as `{resolved}`)"));
    };
    let parsed = dot::parse(&resolved, &text)
        .map_err(|diagnostic| format!("failed to parse {resolved}: {}", diagnostic.message))?;
    let mut imported = model::build(&parsed);
    if imported.attrs.contains("model_stylesheet") {
        diags.warning(
            "attractor.imported_model_stylesheet_ignored",
            Span::file(&resolved),
            format!(
                "`{resolved}` sets a `model_stylesheet`, which an imported workflow does not \
                 apply; the importing workflow's stylesheet governs"
            ),
        );
    }
    let imported_dir = resolved.rfind('/').map_or("", |i| &resolved[..i]);
    // Nested imports resolve against the imported file's own directory.
    stack.push(resolved.clone());
    expand_with_stack(
        &mut imported,
        imported_dir,
        root_base_dir,
        files,
        diags,
        stack,
    );
    stack.pop();
    let prepared = validate_imported(&imported)?;
    splice(
        workflow,
        placeholder_id,
        &placeholder,
        imported,
        &prepared,
        imported_dir,
        root_base_dir,
    )
}

struct PlaceholderOptions {
    defaults:         Attrs,
    classes:          Vec<String>,
    normalized_class: String,
}

fn placeholder_options(workflow: &Workflow, id: &str) -> Result<PlaceholderOptions, String> {
    let node = workflow
        .node(id)
        .ok_or_else(|| format!("missing import placeholder '{id}'"))?;
    let mut defaults = Attrs::default();
    for (key, attr) in node.attrs.iter() {
        if key == "import" || key == "class" {
            continue;
        }
        if PLACEHOLDER_ATTRS.contains(&key) {
            defaults.insert(key, attr.value.clone(), attr.span.clone());
            continue;
        }
        return Err(format!(
            "import placeholder '{id}' has unsupported attribute '{key}'"
        ));
    }
    Ok(PlaceholderOptions {
        defaults,
        classes: node.classes.clone(),
        normalized_class: normalize_class_name(id),
    })
}

struct Prepared {
    start:            String,
    exit:             String,
    entry:            String,
    exit_predecessor: String,
    is_empty:         bool,
}

fn is_start_sentinel(node: &NodeDecl) -> bool {
    node.attrs.text("shape").as_deref() == Some("Mdiamond")
        || matches!(node.id.as_str(), "start" | "Start")
}

fn is_exit_sentinel(node: &NodeDecl) -> bool {
    node.attrs.text("shape").as_deref() == Some("Msquare")
        || matches!(node.id.as_str(), "exit" | "Exit" | "end" | "End")
}

fn has_semantic_attrs(edge: &EdgeDecl) -> bool {
    SEMANTIC_EDGE_ATTRS
        .iter()
        .any(|key| edge.attrs.contains(key))
}

/// Fabro's boundary rules for an imported workflow.
fn validate_imported(imported: &Workflow) -> Result<Prepared, String> {
    let has_non_sentinel = imported
        .nodes
        .iter()
        .any(|node| !is_start_sentinel(node) && !is_exit_sentinel(node));
    let starts: Vec<&NodeDecl> = imported
        .nodes
        .iter()
        .filter(|n| is_start_sentinel(n))
        .collect();
    if starts.len() != 1 {
        return Err(format!(
            "imported workflow must have exactly one start node, found {}",
            starts.len()
        ));
    }
    let exits: Vec<&NodeDecl> = imported
        .nodes
        .iter()
        .filter(|n| is_exit_sentinel(n))
        .collect();
    if exits.len() != 1 {
        return Err(format!(
            "imported workflow must have exactly one exit node, found {}",
            exits.len()
        ));
    }
    let start = starts[0].id.clone();
    let exit = exits[0].id.clone();
    if !imported.incoming(&start).is_empty() {
        return Err(format!(
            "imported start node '{start}' must not have incoming edges"
        ));
    }
    if !imported.outgoing(&exit).is_empty() {
        return Err(format!(
            "imported exit node '{exit}' must not have outgoing edges"
        ));
    }
    let start_edges = imported.outgoing(&start);
    if start_edges.len() != 1 {
        return Err(format!(
            "imported start node '{start}' must have exactly one successor"
        ));
    }
    if has_semantic_attrs(start_edges[0]) {
        return Err(format!(
            "imported edge '{} -> {}' must not carry semantic attributes",
            start_edges[0].from, start_edges[0].to
        ));
    }
    let entry = start_edges[0].to.clone();
    if has_non_sentinel && entry == exit {
        return Err(
            "imported start node cannot route directly to exit when non-sentinel nodes exist"
                .to_string(),
        );
    }
    let exit_edges = imported.incoming(&exit);
    if exit_edges.len() != 1 {
        return Err(format!(
            "imported exit node '{exit}' must have exactly one predecessor"
        ));
    }
    if has_semantic_attrs(exit_edges[0]) {
        return Err(format!(
            "imported edge '{} -> {}' must not carry semantic attributes",
            exit_edges[0].from, exit_edges[0].to
        ));
    }
    let exit_predecessor = exit_edges[0].from.clone();
    if has_non_sentinel && exit_predecessor == start {
        return Err(
            "imported exit node cannot be reached directly from start when non-sentinel nodes \
             exist"
                .to_string(),
        );
    }
    Ok(Prepared {
        start,
        exit,
        entry,
        exit_predecessor,
        is_empty: !has_non_sentinel,
    })
}

/// Replace the placeholder by the imported nodes and edges.
fn splice(
    workflow: &mut Workflow,
    placeholder_id: &str,
    placeholder: &PlaceholderOptions,
    imported: Workflow,
    prepared: &Prepared,
    imported_dir: &str,
    root_base_dir: &str,
) -> Result<(), String> {
    let existing: HashSet<String> = workflow.nodes.iter().map(|n| n.id.clone()).collect();
    for node in &imported.nodes {
        if node.id == prepared.start || node.id == prepared.exit {
            continue;
        }
        let prefixed = format!("{placeholder_id}.{}", node.id);
        if existing.contains(&prefixed) {
            return Err(format!(
                "import placeholder '{placeholder_id}' would overwrite existing node '{prefixed}'"
            ));
        }
    }
    let incoming: Vec<EdgeDecl> = workflow
        .incoming(placeholder_id)
        .into_iter()
        .cloned()
        .collect();
    let outgoing: Vec<EdgeDecl> = workflow
        .outgoing(placeholder_id)
        .into_iter()
        .cloned()
        .collect();
    if incoming.iter().any(|e| e.from == placeholder_id) {
        return Err(format!(
            "import placeholder '{placeholder_id}' cannot have a self-loop"
        ));
    }

    let mut nodes: Vec<NodeDecl> = workflow
        .nodes
        .iter()
        .filter(|n| n.id != placeholder_id)
        .cloned()
        .collect();
    let mut edges: Vec<EdgeDecl> = workflow
        .edges
        .iter()
        .filter(|e| e.from != placeholder_id && e.to != placeholder_id)
        .cloned()
        .collect();

    if prepared.is_empty {
        if incoming.iter().any(has_semantic_attrs) || outgoing.iter().any(has_semantic_attrs) {
            return Err(format!(
                "empty import '{placeholder_id}' cannot bypass semantic edges"
            ));
        }
        for from in &incoming {
            for to in &outgoing {
                edges.push(EdgeDecl {
                    from:    from.from.clone(),
                    to:      to.to.clone(),
                    attrs:   Attrs::default(),
                    span:    from.span.clone(),
                    to_span: to.to_span.clone(),
                });
            }
        }
    } else {
        // The imported workflow's other graph attributes stay behind, as
        // Fabro's do; its agent permissions are a safety setting, so its
        // `default_permissions` reaches its nodes, below the placeholder's.
        let mut defaults = Attrs::default();
        if let Some(attr) = imported.attrs.get("default_permissions") {
            defaults.insert("permissions", attr.value.clone(), attr.span.clone());
        }
        defaults.overlay(&placeholder.defaults);
        for node in imported.nodes {
            if node.id == prepared.start || node.id == prepared.exit {
                continue;
            }
            let mut attrs = defaults.clone();
            attrs.overlay(&node.attrs);
            remap_retry_targets(&mut attrs, placeholder_id);
            relocate_file_references(&mut attrs, imported_dir, root_base_dir);
            let mut classes = placeholder.classes.clone();
            for class in &node.classes {
                if !classes.contains(class) {
                    classes.push(class.clone());
                }
            }
            if !placeholder.normalized_class.is_empty()
                && !classes.contains(&placeholder.normalized_class)
            {
                classes.push(placeholder.normalized_class.clone());
            }
            nodes.push(NodeDecl {
                id: format!("{placeholder_id}.{}", node.id),
                attrs,
                classes,
                span: node.span,
                declared: true,
            });
        }
        for edge in imported.edges {
            if edge.from == prepared.start
                || edge.to == prepared.start
                || edge.from == prepared.exit
                || edge.to == prepared.exit
            {
                continue;
            }
            edges.push(EdgeDecl {
                from:    format!("{placeholder_id}.{}", edge.from),
                to:      format!("{placeholder_id}.{}", edge.to),
                attrs:   edge.attrs,
                span:    edge.span,
                to_span: edge.to_span,
            });
        }
        for edge in incoming {
            edges.push(EdgeDecl {
                to: format!("{placeholder_id}.{}", prepared.entry),
                ..edge
            });
        }
        for edge in outgoing {
            edges.push(EdgeDecl {
                from: format!("{placeholder_id}.{}", prepared.exit_predecessor),
                ..edge
            });
        }
    }
    *workflow = Workflow::from_parts(
        workflow.name.clone(),
        workflow.attrs.clone(),
        nodes,
        edges,
        workflow.span.clone(),
    );
    Ok(())
}

fn remap_retry_targets(attrs: &mut Attrs, placeholder_id: &str) {
    for key in ["retry_target", "fallback_retry_target"] {
        let Some(attr) = attrs.get(key).cloned() else {
            continue;
        };
        attrs.insert(
            key,
            AttrValue::Str(format!("{placeholder_id}.{}", attr.value.as_text())),
            attr.span,
        );
    }
}

/// An `@file` reference inside an imported workflow resolves beside the
/// imported file, as Fabro inlines it there. Lowering resolves references
/// beside the root file, so the reference is rewritten to a path relative to
/// the root.
fn relocate_file_references(attrs: &mut Attrs, imported_dir: &str, root_base_dir: &str) {
    for key in ["prompt", "output_schema"] {
        let Some(attr) = attrs.get(key).cloned() else {
            continue;
        };
        let Some(reference) = attr.value.as_str().and_then(|s| s.strip_prefix('@')) else {
            continue;
        };
        let absolute = normalize(&join(imported_dir, reference));
        let relative = relative_to(root_base_dir, &absolute);
        attrs.insert(key, AttrValue::Str(format!("@{relative}")), attr.span);
    }
}

/// Drop the `import` attribute after a failure, so lowering does not report
/// it twice; the node keeps everything else and lowers as what it is.
fn poison(workflow: &mut Workflow, id: &str) {
    if let Some(node) = workflow.node_mut(id) {
        node.attrs.remove("import");
    }
}

/// Fabro's class name for a placeholder id: lowercase, spaces become `-`,
/// anything but `[a-z0-9-]` is dropped.
fn normalize_class_name(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .map(|c| if c == ' ' { '-' } else { c })
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect()
}

fn join(dir: &str, path: &str) -> String {
    if dir.is_empty() || path.starts_with('/') {
        path.to_string()
    } else {
        format!("{dir}/{path}")
    }
}

/// Collapse `.` and `..` segments so two spellings of one file compare equal.
fn normalize(path: &str) -> String {
    let absolute = path.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|last| *last != "..") {
                    out.pop();
                } else if !absolute {
                    out.push("..");
                }
            }
            other => out.push(other),
        }
    }
    let joined = out.join("/");
    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// `target` expressed relative to `base`, both normalized and relative to
/// the repository root.
fn relative_to(base: &str, target: &str) -> String {
    let base: Vec<&str> = base.split('/').filter(|s| !s.is_empty()).collect();
    let target: Vec<&str> = target.split('/').filter(|s| !s.is_empty()).collect();
    let common = base.iter().zip(&target).take_while(|(a, b)| a == b).count();
    let mut out: Vec<&str> = iter::repeat_n("..", base.len() - common).collect();
    out.extend(&target[common..]);
    out.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_normalize_and_relate() {
        assert_eq!(normalize("a/./b/../c"), "a/c");
        assert_eq!(normalize("../x"), "../x");
        assert_eq!(relative_to("flows", "flows/sub/p.md"), "sub/p.md");
        assert_eq!(relative_to("flows/a", "flows/b/p.md"), "../b/p.md");
        assert_eq!(relative_to("", "p.md"), "p.md");
    }

    #[test]
    fn class_names_follow_fabro() {
        assert_eq!(normalize_class_name("Review Step_1"), "review-step1");
    }
}
