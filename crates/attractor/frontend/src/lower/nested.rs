//! Nested workflows: the child a manager loop runs, lowered with the root
//! so `petri check` validates it and the run registers it before the root
//! starts.

use std::collections::BTreeMap;

use frontend::{CompileInputs, Diagnostics, Span};
use serde_json::{Map, Value};
use smol_str::SmolStr;

use super::{
    Ctx, FailurePolicy, MAX_CALL_DEPTH, RunSettings, duration_ms, lower_nested, placeholder,
};
use crate::model::{self, NodeDecl, Workflow};
use crate::{condition, dot};

impl Ctx<'_> {
    pub(super) fn workflow_config(
        &mut self,
        node: &NodeDecl,
        workflow: &Workflow,
        policy: FailurePolicy,
        permissions: crate::AgentPermissions,
    ) -> Value {
        let mut config = Map::new();
        config.insert(
            "label".into(),
            Value::String(node.attrs.text("label").unwrap_or_else(|| node.id.clone())),
        );
        config.insert("node".into(), Value::String(node.id.clone()));
        Self::policy_config(&mut config, node, workflow, policy);
        let kv = self.b.exprs().var("kv");
        config.insert("kv".into(), placeholder(kv));
        // Fabro's normalization: missing, non-integer or negative is 1000;
        // zero is 1. A value Fabro would silently discard is named here.
        let cycles = match node.attrs.get("manager.max_cycles") {
            None => 1000,
            Some(attr) => match &attr.value {
                model::AttrValue::Int(n) if *n >= 0 => (*n).max(1),
                model::AttrValue::Str(s) if s.trim().parse::<i64>().is_ok_and(|n| n >= 0) => {
                    s.trim().parse::<i64>().unwrap_or(1000).max(1)
                }
                other => {
                    self.diags.warning(
                        "attractor.manager.max_cycles",
                        attr.span.clone(),
                        format!(
                            "`manager.max_cycles={}` is not a non-negative integer; Fabro reads \
                             it as 1000",
                            other.as_text()
                        ),
                    );
                    1000
                }
            },
        };
        config.insert("max_cycles".into(), Value::from(cycles));
        if let Some(interval) = node
            .attrs
            .duration("manager.poll_interval", &mut self.diags)
        {
            config.insert("poll_interval_ms".into(), duration_ms(interval));
        }
        if let Some(stop) = node.attrs.text("manager.stop_condition") {
            let span = node.attrs.span_of("manager.stop_condition", &node.span);
            let mut table = ir::ExprTable::new();
            if condition::lower(&stop, &mut table, &span, &mut self.diags, false).is_some() {
                config.insert("stop_condition".into(), Value::String(stop));
            }
        }
        let source = node.attrs.text("stack.child_workflow");
        let inline = node.attrs.text("stack.child_dot_source");
        let child = match (source, inline) {
            (Some(path), _) => {
                config.insert("child_workflow".into(), Value::String(path.clone()));
                let span = node.attrs.span_of("stack.child_workflow", &node.span);
                self.child_from_file(&path, &span, &node.id, permissions)
            }
            (None, Some(source)) => {
                config.insert("child_dot_source".into(), Value::String(source.clone()));
                let span = node.attrs.span_of("stack.child_dot_source", &node.span);
                let name = format!(
                    "{}#{}",
                    self.stack.last().map_or("", String::as_str),
                    node.id
                );
                self.child_from_text(&name, &source, &span, permissions)
            }
            (None, None) => {
                self.diags.error(
                    "attractor.manager_loop_without_child",
                    node.span.clone(),
                    format!(
                        "manager loop `{}` needs `stack.child_workflow` or `stack.child_dot_source`",
                        node.id
                    ),
                );
                None
            }
        };
        if let Some(digest) = child {
            config.insert("child_digest".into(), Value::String(digest));
        }
        Value::Object(config)
    }

    /// Where a `stack.child_workflow` path reads from: as written against the
    /// repository root, with Fabro's bundle prefix `fabro/` standing for
    /// `.fabro/`, or beside the workflow file.
    fn child_from_file(
        &mut self,
        path: &str,
        span: &Span,
        node: &str,
        permissions: crate::AgentPermissions,
    ) -> Option<String> {
        let mut candidates = vec![path.to_string()];
        if let Some(rest) = path.strip_prefix("fabro/") {
            candidates.push(format!(".fabro/{rest}"));
        }
        if !self.base_dir.is_empty() {
            candidates.push(format!("{}/{path}", self.base_dir));
        }
        let found = candidates.iter().find_map(|candidate| {
            self.files
                .read(candidate)
                .map(|text| (candidate.clone(), text))
        });
        let Some((resolved, text)) = found else {
            self.diags.error(
                "attractor.child_workflow_not_found",
                span.clone(),
                format!(
                    "manager loop `{node}` names `{path}`, which cannot be read ({} tried)",
                    candidates.join(", ")
                ),
            );
            return None;
        };
        self.child_from_text(&resolved, &text, span, permissions)
    }

    /// Lower a child workflow now, so `petri check` validates it and the run
    /// registers it before the root starts. Its digest names it. The calling
    /// node's agent permissions are the child's run default, as run settings
    /// are a root workflow's: the child's own graph and nodes may override
    /// them.
    fn child_from_text(
        &mut self,
        name: &str,
        text: &str,
        span: &Span,
        permissions: crate::AgentPermissions,
    ) -> Option<String> {
        if self.stack.iter().any(|f| f == name) {
            self.diags.error(
                "attractor.workflow_cycle",
                span.clone(),
                format!(
                    "workflow call cycle: {} -> `{name}`",
                    self.stack.join(" -> ")
                ),
            );
            return None;
        }
        if self.stack.len() > MAX_CALL_DEPTH {
            self.diags.error(
                "attractor.workflow_depth",
                span.clone(),
                format!("nested workflows nest more than {MAX_CALL_DEPTH} deep at `{name}`"),
            );
            return None;
        }
        let dot = match dot::parse(name, text) {
            Ok(dot) => dot,
            Err(diagnostic) => {
                self.diags.push(diagnostic);
                return None;
            }
        };
        let workflow = model::build(&dot);
        let to_map = |map: &BTreeMap<String, Value>| {
            map.iter()
                .map(|(k, v)| (SmolStr::new(k), v.clone()))
                .collect()
        };
        let inputs = CompileInputs {
            inputs:             to_map(self.template.inputs()),
            vars:               to_map(self.template.vars()),
            unbound_is_warning: self.lenient_unbound,
        };
        let lowered = lower_nested(
            workflow,
            name,
            self.files,
            &inputs,
            Diagnostics::new(),
            self.stack.clone(),
            RunSettings {
                permissions,
                model: self.settings.model.clone(),
                model_override: self.settings.model_override.clone(),
                mcps: self.settings.mcps.clone(),
                ..RunSettings::default()
            },
        );
        for diagnostic in lowered.diagnostics.iter() {
            self.diags.push(diagnostic.clone());
        }
        let graph = lowered.graph?;
        let digest = frontend::graph_digest(&graph);
        self.children.extend(lowered.children);
        self.children.push(graph);
        Some(digest)
    }
}
