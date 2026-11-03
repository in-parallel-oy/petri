//! Pass 2: every node's step and config, by kind.

use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use frontend::{Diagnostic, Span};
use ir::placeholder::{ADMISSION_HOOKS_BY_STEP, ADMISSION_HOOKS_META};
use ir::{Budget, ExprId, StepRef};
use serde_json::{Map, Value, json};

use super::{
    AGENT_TIMEOUT, COMMAND_TIMEOUT, Ctx, EnvValue, FailurePolicy, HUMAN_TIMEOUT, Kind, NodeRef,
    ROUTES_KEY, STRUCTURAL_TIMEOUT, attrs, duration_ms, fallbacks, lints, placeholder, policy,
    promotion, routing, settings, shape_of, skills, subagents, threads,
};
use crate::kinds::{
    AGENT_KIND, COMMAND_KIND, HUMAN_KIND, MAX_OUTPUT_RETRIES, PROMPT_KIND, STAGE_KIND, WAIT_KIND,
    WORKFLOW_KIND,
};
use crate::model::{Attrs, NodeDecl, Workflow};
use crate::{labels, template};

impl Ctx<'_> {
    pub(super) fn node(&mut self, node: &NodeDecl, workflow: &Workflow) {
        self.unknown_attrs(
            &node.attrs,
            attrs::NODE,
            attrs::NODE_IGNORED,
            &format!("node `{}`", node.id),
        );
        let NodeRef { id, kind, policy } = self.nodes[&node.id];
        let shape = shape_of(node);
        let label = node.attrs.text("label").unwrap_or_else(|| node.id.clone());
        for key in ["on_failure", "on_retries_exhausted"] {
            self.check_policy(key, &node.attrs, &node.span);
        }
        lints::reserved_keyword_node_id(node, &mut self.diags);
        lints::inert_attributes(node, kind, &mut self.diags);
        lints::for_each_requires_parallel(node, kind, &mut self.diags);
        lints::retry_targets(
            &node.attrs,
            &node.span,
            workflow,
            &format!("node `{}`", node.id),
            &mut self.diags,
        );
        let explicit = self.explicit_timeout(node);
        let permissions = self
            .permissions_attr(&node.attrs, "permissions", &node.span)
            .unwrap_or(self.permissions);

        let mut meta = json!({
            "label": label,
            "permissions": permissions,
            "shape": shape,
            "kind": kind.name(),
            "classes": node.classes,
            "span": { "line": node.span.line, "column": node.span.column },
        });
        for key in ["model", "provider", "reasoning_effort"] {
            if let Some(value) = node.attrs.text(key) {
                meta[key] = Value::String(value);
            }
        }
        if kind == Kind::Start {
            // The stage step drives `start`'s admission hooks itself, after
            // `sandbox_ready` and `run_start`, with the sandbox in place: the
            // driver admits `start` before the scope's environment exists.
            meta[ADMISSION_HOOKS_META] = Value::String(ADMISSION_HOOKS_BY_STEP.into());
        }
        self.b.set_meta(id, meta);

        let (step, timeout) = match kind {
            Kind::Conditional => (None, STRUCTURAL_TIMEOUT),
            // `start` and `exit` run the stage step: it records the scope's
            // environment for sandbox-placed hooks and fires the run-level
            // hooks (`sandbox_ready`, `run_start`, `run_complete`).
            Kind::Start | Kind::Exit => {
                let kv = self.b.exprs().var("kv");
                let mut config = json!({
                    "node": node.id,
                    "kind": kind.name(),
                    "label": label,
                    "permissions": permissions,
                    "default_permissions": self.permissions,
                    "workflow": self.workflow_name,
                    "kv": placeholder(kv),
                });
                if self.stack.len() == 1 {
                    config["hooks"] = settings::hooks_param(&self.settings.hooks);
                    // The checkout and the repository the host bound: the
                    // root `start` stage checks the repository out into its
                    // workspace before anything runs there.
                    if kind == Kind::Start {
                        config["checkout"] = json!({
                            "enabled": self.settings.clone.enabled,
                            "depth": self.settings.clone.depth,
                            "repository": self.repository,
                        });
                    }
                }
                // `[run.model.fallbacks]` as written: the `start` stage
                // checks the table against the catalog before anything
                // runs, as Fabro's server refuses a bad table at run start.
                if kind == Kind::Start
                    && let Some(config) = config.as_object_mut()
                {
                    fallbacks::write(&self.settings, config);
                }
                (Some(StepRef::new(STAGE_KIND, config)), STRUCTURAL_TIMEOUT)
            }
            Kind::FanIn => {
                if node
                    .attrs
                    .text("prompt")
                    .is_some_and(|prompt| !prompt.trim().is_empty())
                {
                    // A prompted fan-in: the ordered barrier, then one model
                    // call over the branch results.
                    let mut config =
                        self.agent_config(node, kind, workflow, policy, explicit, permissions);
                    Self::fan_in_sources(node, workflow, &mut config);
                    (
                        Some(StepRef::new(PROMPT_KIND, config)),
                        explicit.unwrap_or(AGENT_TIMEOUT),
                    )
                } else {
                    (None, STRUCTURAL_TIMEOUT)
                }
            }
            Kind::Parallel => (None, explicit.unwrap_or(STRUCTURAL_TIMEOUT)),
            Kind::Agent => {
                let mut config =
                    self.agent_config(node, kind, workflow, policy, explicit, permissions);
                if let Some(object) = config.as_object_mut() {
                    object.insert("compaction".into(), self.settings.compaction.to_json());
                }
                (
                    Some(StepRef::new(AGENT_KIND, config)),
                    explicit.unwrap_or(AGENT_TIMEOUT),
                )
            }
            Kind::Prompt => {
                let config = self.agent_config(node, kind, workflow, policy, explicit, permissions);
                (
                    Some(StepRef::new(PROMPT_KIND, config)),
                    explicit.unwrap_or(AGENT_TIMEOUT),
                )
            }
            Kind::Command => {
                let config = self.command_config(node, workflow, policy, explicit);
                (
                    Some(StepRef::new(COMMAND_KIND, config)),
                    explicit.unwrap_or(COMMAND_TIMEOUT),
                )
            }
            Kind::Human => {
                let config = self.human_config(node, workflow, policy, explicit);
                (
                    Some(StepRef::new(HUMAN_KIND, config)),
                    explicit.unwrap_or(HUMAN_TIMEOUT),
                )
            }
            Kind::Wait => {
                let duration = node.attrs.duration("duration", &mut self.diags);
                if duration.is_none() && !node.attrs.contains("duration") {
                    self.diags.error(
                        "attractor.wait_requires_duration",
                        node.span.clone(),
                        format!("wait node `{}` needs a `duration`", node.id),
                    );
                }
                let duration = duration.unwrap_or_default();
                let config = json!({ "label": label, "duration_ms": duration_ms(duration) });
                let timeout = explicit.unwrap_or(duration + STRUCTURAL_TIMEOUT);
                (Some(StepRef::new(WAIT_KIND, config)), timeout)
            }
            Kind::ManagerLoop => {
                let config = self.workflow_config(node, workflow, policy, permissions);
                (
                    Some(StepRef::new(WORKFLOW_KIND, config)),
                    explicit.unwrap_or(AGENT_TIMEOUT),
                )
            }
        };
        if let Some(step) = step {
            self.b.node_mut(id).step = step;
        }
        self.b.node_mut(id).budget = Budget::new(1, timeout)
            .with_timeout_policy(policy::timeout_policy(kind, node, workflow));
        self.b.node_mut(id).retry = routing::retry_policy(node, workflow, &mut self.diags);
    }

    /// The node's `timeout`. A bare number is Attractor's spelling (seconds);
    /// Fabro needs a unit, and reads a unitless value as no timeout at all.
    fn explicit_timeout(&mut self, node: &NodeDecl) -> Option<Duration> {
        if let Some(attr) = node.attrs.get("timeout")
            && attr.value.as_text().trim().parse::<f64>().is_ok()
        {
            self.diags.unsupported(
                "legacy_dialect",
                attr.span.clone(),
                format!(
                    "`timeout={}` is a bare number, the legacy-dialect spelling",
                    attr.value.as_text()
                ),
                "write the unit: `timeout=\"1200s\"`",
            );
            return None;
        }
        node.attrs.duration("timeout", &mut self.diags)
    }

    /// The shared part of every step config: label, goal, the run context.
    fn base_config(
        &mut self,
        node: &NodeDecl,
        workflow: &Workflow,
        policy: FailurePolicy,
        timeout: Option<Duration>,
    ) -> Map<String, Value> {
        let mut config = Map::new();
        config.insert(
            "label".into(),
            Value::String(node.attrs.text("label").unwrap_or_else(|| node.id.clone())),
        );
        config.insert("node".into(), Value::String(node.id.clone()));
        config.insert("goal".into(), Value::String(self.goal().to_owned()));
        let kv = self.b.exprs().var("kv");
        config.insert("kv".into(), placeholder(kv));
        Self::policy_config(&mut config, node, workflow, policy);
        if let Some(ms) = timeout.map(duration_ms) {
            config.insert("timeout_ms".into(), ms);
        }
        config
    }

    /// The node's failure policies, and the explicit routes when either can
    /// promote a failure: Fabro promotes only when no explicit route matches,
    /// and the step decides that with the node's routes in hand.
    pub(super) fn policy_config(
        config: &mut Map<String, Value>,
        node: &NodeDecl,
        workflow: &Workflow,
        policy: FailurePolicy,
    ) {
        config.insert(
            "on_failure".into(),
            Value::String(policy.on_failure.name().into()),
        );
        config.insert(
            "on_retries_exhausted".into(),
            Value::String(policy.on_retries_exhausted.name().into()),
        );
        if policy.promotes() {
            config.insert(
                ROUTES_KEY.into(),
                promotion::explicit_routes(node, workflow),
            );
        }
    }

    fn agent_config(
        &mut self,
        node: &NodeDecl,
        kind: Kind,
        workflow: &Workflow,
        policy: FailurePolicy,
        timeout: Option<Duration>,
        permissions: crate::AgentPermissions,
    ) -> Value {
        let mut config = self.base_config(node, workflow, policy, timeout);
        config.insert("kind".into(), Value::String(kind.name().into()));
        let prompt_span = node.attrs.span_of("prompt", &node.span);
        let prompt = match node.attrs.text("prompt") {
            Some(prompt) if !prompt.trim().is_empty() => prompt,
            _ => {
                self.diags.warning(
                    "attractor.prompt_missing",
                    node.span.clone(),
                    format!(
                        "agent node `{}` has no `prompt`; its label is the prompt",
                        node.id
                    ),
                );
                node.attrs.text("label").unwrap_or_else(|| node.id.clone())
            }
        };
        if let Some(prompt) = self.rendered(
            &prompt,
            &prompt_span,
            &format!("node `{}` `prompt`", node.id),
        ) {
            config.insert("prompt".into(), Value::String(prompt));
        }
        let is_prompt = kind != Kind::Agent;
        let backend = node
            .attrs
            .text("backend")
            .or_else(|| workflow.attrs.text("backend"));
        // Fabro's `backend_valid`: an ACP agent reads no API-only attribute.
        let acp_backend = !is_prompt && backend.as_deref() == Some("acp");
        if !is_prompt {
            config.insert("permissions".into(), json!(permissions));
            if acp_backend && permissions != crate::AgentPermissions::Full {
                self.diags.error(
                    "attractor.permissions_backend",
                    node.span.clone(),
                    "backend=acp cannot enforce restricted agent permissions; use backend=api",
                );
            }
        }
        if acp_backend {
            lints::acp_api_only_attributes(node, &self.styled, &mut self.diags);
        }
        if let Some(backend) = backend {
            if !matches!(backend.as_str(), "acp" | "api") {
                self.diags.error(
                    "attractor.bad_backend",
                    node.attrs.span_of("backend", &node.span),
                    "agent backend must be acp or api",
                );
            }
            // Fabro's `backend_valid` rule: a prompt node is API-only. The
            // graph's `backend="acp"` applies to agent nodes only.
            if is_prompt && backend == "acp" {
                if node.attrs.contains("backend") {
                    self.diags.error(
                        "attractor.prompt_backend",
                        node.attrs.span_of("backend", &node.span),
                        "backend=\"acp\" is only valid on agent nodes; prompt nodes are API-only",
                    );
                }
            } else {
                config.insert("backend".into(), Value::String(backend));
            }
        }
        for key in ["model", "provider", "reasoning_effort"] {
            if let Some(value) = node.attrs.text(key) {
                config.insert(key.into(), Value::String(value));
            }
        }
        // The launch's model choice, below the node (its attributes and
        // stylesheet rules) and above the graph and run defaults. A node that
        // names its own model leaves its provider to the node and the
        // defaults, so the launch's provider never pairs with a model the
        // node chose.
        if !config.contains_key("model") {
            let launch = &self.settings.model_override;
            if let Some(model) = &launch.name {
                config.insert("model".into(), Value::String(model.clone()));
            }
            if !config.contains_key("provider")
                && let Some(provider) = &launch.provider
            {
                config.insert("provider".into(), Value::String(provider.clone()));
            }
        }
        // The graph's defaults, then the run settings' model defaults.
        if !config.contains_key("model")
            && let Some(model) = workflow
                .attrs
                .text("default_model")
                .or_else(|| self.settings.model.name.clone())
        {
            config.insert("model".into(), Value::String(model));
        }
        if !config.contains_key("provider")
            && let Some(provider) = workflow
                .attrs
                .text("default_provider")
                .or_else(|| self.settings.model.provider.clone())
        {
            config.insert("provider".into(), Value::String(provider));
        }
        if !config.contains_key("reasoning_effort")
            && let Some(effort) = self.settings.model.reasoning_effort.clone()
        {
            config.insert("reasoning_effort".into(), Value::String(effort));
        }
        fallbacks::write(&self.settings, &mut config);
        let branch_first = threads::is_branch_first(node, workflow, &self.nodes);
        let default_speed = self.settings.model.speed.clone();
        let threads = threads::ThreadAttrs::read(
            node,
            workflow,
            branch_first,
            default_speed.as_deref(),
            &mut self.diags,
        );
        threads.write(self.b.exprs(), &mut config);
        if !is_prompt {
            skills::write(&self.settings.skills, &mut config);
            subagents::write(&mut config);
        }
        config.insert("stages".into(), threads::stages(workflow, &self.nodes));
        if !is_prompt {
            config.insert("mcps".into(), settings::mcps_param(&self.settings.mcps));
        }
        self.output_schema(node, &mut config);
        if let Some(retries) = node.attrs.int("output_retries", &mut self.diags) {
            let retries = retries.max(0);
            if retries > i64::try_from(MAX_OUTPUT_RETRIES).unwrap_or(i64::MAX) {
                self.diags.error(
                    "attractor.output_retries_too_large",
                    node.attrs.span_of("output_retries", &node.span),
                    format!("`output_retries={retries}` exceeds the hard maximum of {MAX_OUTPUT_RETRIES}"),
                );
            }
            config.insert(
                "output_retries".into(),
                Value::from(retries.min(i64::try_from(MAX_OUTPUT_RETRIES).unwrap_or(i64::MAX))),
            );
        }
        let sets_acp = node.attrs.contains("acp.command") || node.attrs.contains("acp.config");
        if acp_backend {
            self.acp(node, workflow, &mut config);
        } else if sets_acp && is_prompt {
            self.diags.error(
                "attractor.backend_options",
                node.span.clone(),
                "a prompt node cannot set acp.command or acp.config; prompt nodes are API-only",
            );
        } else if sets_acp
            && matches!(
                config.get("backend").and_then(Value::as_str),
                None | Some("api")
            )
        {
            // With no backend anywhere, the node runs on the API, which reads
            // no ACP agent.
            self.diags.push(
                Diagnostic::error(
                    "attractor.backend_options",
                    node.span.clone(),
                    "an API node cannot set acp.command or acp.config",
                )
                .with_hint("set `backend=\"acp\"` to run the node on an ACP agent"),
            );
        }
        // An agent's processes, on either backend, see the workflow's secrets:
        // the ACP agent itself, and a native session's tool shells.
        if !is_prompt {
            let env = self.workflow_secret_env();
            if !env.is_empty() {
                config.insert("env".into(), Value::Object(env));
            }
        }
        let nodes = self.b.exprs().var("nodes");
        config.insert("nodes".into(), placeholder(nodes));
        Value::Object(config)
    }

    fn output_schema(&mut self, node: &NodeDecl, config: &mut Map<String, Value>) {
        let Some(schema) = node.attrs.text("output_schema") else {
            return;
        };
        let span = node.attrs.span_of("output_schema", &node.span);
        if schema == "routing" {
            config.insert("output_schema".into(), Value::String("routing".into()));
            return;
        }
        let Some(text) = self.rendered(
            &schema,
            &span,
            &format!("node `{}` `output_schema`", node.id),
        ) else {
            return;
        };
        match serde_json::from_str::<Value>(&text) {
            Ok(value @ Value::Object(_)) => {
                config.insert("output_schema".into(), value);
            }
            _ => self.diags.error(
                "attractor.bad_output_schema",
                span,
                format!(
                    "`output_schema` on `{}` must be `routing` or a JSON Schema object",
                    node.id
                ),
            ),
        }
    }

    /// The ACP agent of a node on `backend="acp"`, from the node or the
    /// graph. Command and config are one choice: a node setting either
    /// inherits neither from the graph. Naming no agent is an error.
    fn acp(&mut self, node: &NodeDecl, workflow: &Workflow, config: &mut Map<String, Value>) {
        let agent = if node.attrs.contains("acp.command") || node.attrs.contains("acp.config") {
            self.acp_agent(&node.attrs, &format!("node `{}`", node.id), &node.span)
        } else if workflow.attrs.contains("acp.command") || workflow.attrs.contains("acp.config") {
            // The graph's pair was diagnosed once, at the graph.
            self.graph_acp.clone()
        } else {
            lints::acp_requires_command(node, &mut self.diags);
            None
        };
        if let Some(agent) = agent {
            config.insert("acp".into(), agent);
        }
    }

    /// The ACP agent `attrs` name, as a step's `acp` value: `acp.command`, a
    /// command line, or `acp.config`, a JSON stdio server with a `command`;
    /// never both. `owner` names whose attributes they are in diagnostics,
    /// which point at `span` when the attribute has no position. The rest of
    /// the stdio shape is the step's to check when it starts the agent.
    pub(super) fn acp_agent(&mut self, attrs: &Attrs, owner: &str, span: &Span) -> Option<Value> {
        match (attrs.text("acp.command"), attrs.text("acp.config")) {
            (Some(_), Some(_)) => {
                self.diags.error(
                    "attractor.acp_both",
                    attrs.span_of("acp.command", span),
                    format!("{owner} sets both `acp.command` and `acp.config`"),
                );
                None
            }
            (Some(command), None) => {
                let problem = match shlex::split(command.trim()) {
                    None => "has unbalanced quotes",
                    Some(words) if words.is_empty() => "is empty",
                    Some(_) => return Some(json!({ "command": command })),
                };
                self.diags.error(
                    "attractor.bad_acp_command",
                    attrs.span_of("acp.command", span),
                    format!("`acp.command` on {owner} {problem}"),
                );
                None
            }
            (None, Some(text)) => {
                let problem = match serde_json::from_str::<Value>(&text) {
                    Ok(value)
                        if value
                            .get("command")
                            .and_then(Value::as_str)
                            .is_some_and(|command| !command.trim().is_empty()) =>
                    {
                        return Some(json!({ "config": value }));
                    }
                    Ok(_) => "must be a JSON object with a non-empty `command`".to_string(),
                    Err(error) => format!("is not JSON: {error}"),
                };
                self.diags.error(
                    "attractor.bad_acp_config",
                    attrs.span_of("acp.config", span),
                    format!("`acp.config` on {owner} {problem}"),
                );
                None
            }
            (None, None) => None,
        }
    }

    /// The workflow environment's secret references, for a step that starts
    /// processes. Literal values reach every process through the scope's
    /// env; secret values stay references until the step spawns.
    fn workflow_secret_env(&self) -> Map<String, Value> {
        let mut env = Map::new();
        if let Some(environment) = &self.settings.environment {
            for (key, value) in &environment.env {
                if let EnvValue::Secret(_) = value {
                    env.insert(key.clone(), value.to_json());
                }
            }
        }
        env
    }

    fn command_config(
        &mut self,
        node: &NodeDecl,
        workflow: &Workflow,
        policy: FailurePolicy,
        timeout: Option<Duration>,
    ) -> Value {
        let mut config = self.base_config(node, workflow, policy, timeout);
        let language = node
            .attrs
            .text("language")
            .unwrap_or_else(|| "shell".into());
        if language != "shell" && language != "python" {
            self.diags.error(
                "attractor.bad_language",
                node.attrs.span_of("language", &node.span),
                format!("`language` must be `shell` or `python`, not `{language}`"),
            );
        }
        config.insert("language".into(), Value::String(language.clone()));
        match node.attrs.text("script") {
            Some(script) if !script.trim().is_empty() => {
                lints::script_absolute_cd(node, &script, &mut self.diags);
                let script = match template::render_script(&script, &language, &self.template) {
                    Ok(script) => Some(script),
                    Err(error) => {
                        let span = node.attrs.span_of("script", &node.span);
                        let unrendered = self.lenient_unbound && error.is_unbound();
                        self.template_error(&error, &span, &format!("node `{}` `script`", node.id));
                        unrendered.then_some(script)
                    }
                };
                if let Some(script) = script {
                    // The text the step runs, on the config for the step and
                    // on `meta` for a host that shows the stage.
                    config.insert("script".into(), Value::String(script.clone()));
                    let id = self.nodes[&node.id].id;
                    if let Value::Object(meta) = &mut self.b.node_mut(id).meta {
                        meta.insert("script".into(), Value::String(script));
                    }
                }
            }
            _ => self.diags.error(
                "attractor.command_requires_script",
                node.span.clone(),
                format!("command node `{}` needs a `script`", node.id),
            ),
        }
        let env = self.workflow_secret_env();
        if !env.is_empty() {
            config.insert("env".into(), Value::Object(env));
        }
        if let Some(source) = node.attrs.text("stdin_source") {
            let span = node.attrs.span_of("stdin_source", &node.span);
            match self.context_source(&source, node, workflow, &span) {
                Some(expr) => {
                    config.insert("stdin".into(), placeholder(expr));
                }
                None => self.diags.error(
                    "attractor.bad_stdin_source",
                    span,
                    format!("`stdin_source` must name a context key such as `context.output`, not `{source}`"),
                ),
            }
        }
        self.output_schema(node, &mut config);
        Value::Object(config)
    }

    /// `context.K` as an expression. `context.parallel.results` reads the
    /// nearest upstream fan-in's output, where the branch results live under
    /// the engine (they ride tokens, never `kv`).
    fn context_source(
        &mut self,
        source: &str,
        node: &NodeDecl,
        workflow: &Workflow,
        span: &Span,
    ) -> Option<ExprId> {
        let key = source.strip_prefix("context.").unwrap_or(source).trim();
        if key.is_empty() {
            return None;
        }
        if key == "parallel.results" && !self.upstream_fan_in(&node.id, workflow) {
            self.diags.warning(
                "attractor.parallel_results_without_fan_in",
                span.clone(),
                format!(
                    "`{}` reads `context.parallel.results` but no fan-in node precedes it",
                    node.id
                ),
            );
        }
        if key.starts_with("internal.") {
            self.diags.warning(
                "attractor.internal_context",
                span.clone(),
                format!("`{key}` is Fabro-internal run state, which Petri does not populate; it reads as null"),
            );
        }
        let kv = self.b.exprs().var("kv");
        let name = self.b.exprs().lit(key);
        Some(self.b.exprs().call("get", vec![kv, name]))
    }

    /// Whether a fan-in, or a parallel node whose fan-in publishes the
    /// results, precedes `id`.
    fn upstream_fan_in(&self, id: &str, workflow: &Workflow) -> bool {
        let mut seen: HashSet<&str> = HashSet::from([id]);
        let mut queue: VecDeque<&str> = VecDeque::from([id]);
        while let Some(current) = queue.pop_front() {
            for edge in workflow.incoming(current) {
                if matches!(
                    self.nodes.get(&edge.from).map(|n| n.kind),
                    Some(Kind::FanIn | Kind::Parallel)
                ) {
                    return true;
                }
                if seen.insert(&edge.from) {
                    queue.push_back(&edge.from);
                }
            }
        }
        false
    }

    fn human_config(
        &mut self,
        node: &NodeDecl,
        workflow: &Workflow,
        policy: FailurePolicy,
        timeout: Option<Duration>,
    ) -> Value {
        let mut config = self.base_config(node, workflow, policy, timeout);
        let mut choices = Vec::new();
        let mut freeform_target = None;
        for edge in workflow.outgoing(&node.id) {
            if edge
                .attrs
                .bool("freeform", &mut self.diags)
                .unwrap_or(false)
            {
                if freeform_target.is_some() {
                    self.diags.error(
                        "attractor.freeform_edge_count",
                        edge.span.clone(),
                        format!(
                            "human gate `{}` has more than one `freeform=true` edge",
                            node.id
                        ),
                    );
                }
                freeform_target = Some(edge.to.clone());
                continue;
            }
            let label = edge
                .attrs
                .text("label")
                .filter(|l| !l.is_empty())
                .unwrap_or_else(|| edge.to.clone());
            let mut choice = json!({
                "key": labels::accelerator_key(&label),
                "label": label,
                "to": edge.to,
            });
            // What a host shows beside the choice, when the edge says.
            for (attr, field) in [
                ("human.description", "description"),
                ("human.preview", "preview"),
            ] {
                if let Some(text) = edge.attrs.text(attr).filter(|t| !t.trim().is_empty()) {
                    choice[field] = Value::String(text);
                }
            }
            choices.push(choice);
        }
        if choices.is_empty() && freeform_target.is_none() {
            self.diags.error(
                "attractor.human_without_edges",
                node.span.clone(),
                format!(
                    "human gate `{}` has no outgoing edges to offer as choices",
                    node.id
                ),
            );
        }
        if let Some(kind) = node.attrs.text("question_type") {
            if !attrs::QUESTION_TYPES.contains(&kind.as_str()) {
                self.diags.error(
                    "attractor.bad_question_type",
                    node.attrs.span_of("question_type", &node.span),
                    format!(
                        "`question_type` must be one of {}",
                        attrs::QUESTION_TYPES.join(", ")
                    ),
                );
            }
            config.insert("question_type".into(), Value::String(kind));
        }
        if let Some(sensitive) = node.attrs.bool("sensitive", &mut self.diags) {
            config.insert("sensitive".into(), Value::Bool(sensitive));
        }
        if let Some(review) = node.attrs.bool("review_target", &mut self.diags) {
            config.insert("review_target".into(), Value::Bool(review));
        }
        if let Some(default) = node.attrs.text("human.default_choice") {
            if !choices.iter().any(|choice| {
                choice["key"] == Value::String(default.clone())
                    || choice["to"] == Value::String(default.clone())
            }) {
                self.diags.error(
                    "attractor.bad_default_choice",
                    node.attrs.span_of("human.default_choice", &node.span),
                    format!(
                        "`human.default_choice=\"{default}\"` names none of the gate's \
                         choices or targets"
                    ),
                );
            }
            config.insert("default_choice".into(), Value::String(default));
        }
        config.insert("choices".into(), Value::Array(choices));
        if let Some(target) = freeform_target {
            config.insert("freeform_target".into(), Value::String(target));
        }
        Value::Object(config)
    }

    /// The branch source nodes a prompted fan-in joins, for its prompt and
    /// its record.
    fn fan_in_sources(node: &NodeDecl, workflow: &Workflow, config: &mut Value) {
        let sources: Vec<Value> = workflow
            .incoming(&node.id)
            .into_iter()
            .map(|edge| Value::String(edge.from.clone()))
            .collect();
        if let Value::Object(config) = config {
            config.insert("sources".into(), Value::Array(sources));
        }
    }
}
