//! The local Fabro hook system: `[[run.hooks]]` executed in standalone Petri.
//!
//! [`LocalHooks`] is the `execution::hooks::HookService` the Fabro component
//! installs. Every caller reaches it as a `HookService`: the `HookAdapter`
//! asks it at the driver's points; the `attractor/stage` step asks it at
//! `start` for `sandbox_ready`, `run_start` and the start stage's own
//! admission; the fork and fan-in steps ask it for `parallel_start` and
//! `parallel_complete`; the native agent's tool middleware ([`tools`]) and the
//! ACP client ask it at the tool boundary. Each of them goes through the
//! `HookServiceHandle` capability, the handle a host's replacement service
//! would be behind, so a configured hook runs once whoever serves the point,
//! and a replacement receives every point.
//!
//! The service configures itself from the first firing it sees whose step
//! config carries the run's `hooks` list: the lowering puts the merged
//! `[[run.hooks]]` entries on the `start` and `exit` stages. Until then, and
//! for a run with no hooks, every point proceeds.
//!
//! Fabro's rules, as `fabro-hooks` implements them at the pinned revision:
//! hooks matching an event run in configuration order; a hook's `matcher`
//! regex is tested against the node id, handler type, edge ends and tool name
//! the event carries; when any matched hook is blocking, decisions merge as
//! block > skip or override > proceed and a block ends the sequence; a
//! non-blocking hook still runs, awaited, and its decision is ignored.
//! Command hooks decide by exit code (0 proceeds unless stdout is a JSON
//! decision, 2 blocks unless stdout is one, anything else blocks); HTTP,
//! prompt and agent hooks fail open on errors and timeouts.

pub mod executors;
pub mod tools;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use execution::hooks::{
    ForkCompletedPayload, HOOK_ACTIVITY_NOTE_KIND, HookActivity, HookDecision, HookOperation,
    HookPoint, HookReport, HookRequest, HookRun, HookService, RunFinishedPayload,
    ScopeReleasedPayload,
};
use executor::ExecEnv;
use frontend_attractor::hooks::{HookDefinition, HookEvent};
use frontend_attractor::kinds::GOAL_CHECK_NODE;
use ir::{EdgeId, Outcome, RunStatus, Status, Value};
use runtime::driver::lifecycle::Note;
use runtime::driver::{BranchRole, FiringView};
use runtime::engine::RouteDecision;
use serde::{Deserialize, Serialize};
use serde_json::{Map, json};
use smol_str::SmolStr;

use crate::outcome::reported_outcome;
use crate::pebble::PebbleClient;
use crate::stage::{RunInfo, ScopeEnvironments};

/// The `kind` of the `StepEvent::Custom` payload a report a step asked for
/// itself rides on (the start stage's points, a fork's, the tool boundary's):
/// `{ kind, node, firing, attempt, event, report }`.
pub const REPORT_EVENT: &str = "attractor.hook";

/// The `kind` of the `StepEvent::Custom` payload an enforcement warning rides
/// on: `{ kind, node, firing, attempt, backend, hook, event, boundary,
/// message }`.
pub const WARNING_EVENT: &str = "attractor.hook.warning";

/// The failure class of a stage a blocking run-level hook stopped.
pub const BLOCKED_CLASS: &str = "hook_blocked";

/// What Fabro's hook decision JSON looks like on a command's stdout or an
/// HTTP body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum Decision {
    #[default]
    Proceed,
    Skip {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Block {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Override {
        edge_to: String,
    },
}

impl Decision {
    /// Fabro's merge: block beats skip and override, which beat proceed; on
    /// two of the same rank the first wins.
    #[must_use]
    pub fn merge(self, other: Self) -> Self {
        match (&self, &other) {
            (Self::Block { .. }, _) => self,
            (_, Self::Block { .. }) => other,
            (Self::Skip { .. } | Self::Override { .. }, _) => self,
            (_, Self::Skip { .. } | Self::Override { .. }) => other,
            _ => Self::Proceed,
        }
    }

    pub fn is_block(&self) -> bool {
        matches!(self, Self::Block { .. })
    }
}

/// Fabro's hook context: the JSON a command reads, an HTTP hook receives, a
/// prompt or agent hook evaluates. Absent fields are left out.
#[derive(Clone, Debug, Serialize)]
pub struct Context {
    /// Effective access for an agent hook; not part of the hook wire payload.
    #[serde(skip)]
    pub permissions:    crate::AgentPermissions,
    pub event:          HookEvent,
    pub run_id:         String,
    pub workflow_name:  String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd:            Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id:        Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_label:     Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handler_type:   Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status:         Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_from:      Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_to:        Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge_label:     Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt:        Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_attempts:   Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name:      Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_input:     Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id:   Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_output:    Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message:  Option<String>,
}

impl Context {
    pub fn new(event: HookEvent, run_id: String, workflow_name: String) -> Self {
        Self {
            permissions: crate::AgentPermissions::Full,
            event,
            run_id,
            workflow_name,
            cwd: None,
            node_id: None,
            node_label: None,
            handler_type: None,
            status: None,
            edge_from: None,
            edge_to: None,
            edge_label: None,
            failure_reason: None,
            attempt: None,
            max_attempts: None,
            tool_name: None,
            tool_input: None,
            tool_call_id: None,
            tool_output: None,
            error_message: None,
        }
    }
}

/// The payload a caller at the tool boundary supplies in
/// `HookRequest::payload`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ToolPayload {
    pub tool_name:     String,
    pub tool_call_id:  Option<String>,
    pub tool_input:    Option<Value>,
    pub tool_output:   Option<String>,
    pub error_message: Option<String>,
}

/// A step's binding to the hook service at the tool boundary: the service,
/// the firing view it reads, and the node, firing and attempt every report
/// and warning is attributed to. Both agent backends ask through one: the
/// native agent's tool middleware ([`tools::ToolHooks`]) and the ACP client.
pub(crate) struct ToolHookBinding {
    service: Arc<dyn HookService>,
    view:    Arc<FiringView>,
    node:    SmolStr,
    firing:  ir::FiringId,
    attempt: ir::Attempt,
}

impl ToolHookBinding {
    pub(crate) fn new(
        service: Arc<dyn HookService>,
        view: Arc<FiringView>,
        node: SmolStr,
        firing: ir::FiringId,
        attempt: ir::Attempt,
    ) -> Self {
        Self {
            service,
            view,
            node,
            firing,
            attempt,
        }
    }

    /// Ask the service at one tool point and record what it reported; the
    /// decision is the caller's to apply.
    pub(crate) async fn ask(
        &self,
        point: HookPoint,
        payload: ToolPayload,
        logs: &steps::ProgressSender,
    ) -> HookDecision {
        let report = self
            .service
            .run(HookRequest {
                point,
                view: Some(self.view.clone()),
                outcome: None,
                routes: Vec::new(),
                payload: serde_json::to_value(&payload).unwrap_or(Value::Null),
            })
            .await;
        record_report(
            logs,
            &self.node,
            self.firing,
            self.attempt,
            tool_event(point),
            &report,
        )
        .await;
        report.decision
    }

    /// The [`WARNING_EVENT`] that says this node's backend could not enforce
    /// `hook` at `boundary`.
    pub(crate) fn warning(
        &self,
        backend: &str,
        hook: &str,
        event: HookEvent,
        boundary: &str,
        message: &str,
    ) -> ir::StepEvent {
        warning_event(
            &self.node,
            self.firing,
            self.attempt,
            backend,
            hook,
            event,
            boundary,
            message,
        )
    }
}

/// The Fabro event a report from the tool boundary is recorded under.
fn tool_event(point: HookPoint) -> HookEvent {
    match point {
        HookPoint::BeforeToolUse => HookEvent::PreToolUse,
        HookPoint::AfterToolUse => HookEvent::PostToolUse,
        // `AfterToolFailure`: nothing else is a tool boundary, so a binding
        // never asks another point.
        _ => HookEvent::PostToolUseFailure,
    }
}

/// One configured hook with its compiled matcher.
struct Configured {
    definition: HookDefinition,
    matcher:    Option<regex::Regex>,
}

struct Config {
    hooks:               Vec<Configured>,
    workflow:            String,
    /// The access an agent hook outside any stage gets.
    default_permissions: crate::AgentPermissions,
}

/// The local hook service.
pub struct LocalHooks {
    ceiling: Mutex<crate::AgentPermissions>,
    config:  Mutex<Option<Arc<Config>>>,
    run:     Mutex<Option<RunInfo>>,
    client:  Mutex<Option<PebbleClient>>,
    envs:    ScopeEnvironments,
    http:    executors::HttpClients,
}

tokio::task_local! {
    /// Set while a hook's own work runs: a hook that runs work inside another
    /// hook's work (an agent hook's tool calls) must not fire hooks again, as
    /// the reference never does. Task-local, so hooks of parallel branches
    /// running at the same time do not see each other as nesting.
    static IN_HOOK: bool;
}

fn in_hook() -> bool {
    IN_HOOK.try_with(|nested| *nested).unwrap_or(false)
}

impl Default for LocalHooks {
    fn default() -> Self {
        Self::new(ScopeEnvironments::new())
    }
}

impl LocalHooks {
    pub fn new(envs: ScopeEnvironments) -> Self {
        Self {
            ceiling: Mutex::new(crate::AgentPermissions::Full),
            config: Mutex::new(None),
            run: Mutex::new(None),
            client: Mutex::new(None),
            envs,
            http: executors::HttpClients::default(),
        }
    }

    /// The scope environments the service runs sandbox-placed hooks in: the
    /// capability steps record into (`stage::record`). A clone shares the
    /// map.
    pub fn environments(&self) -> ScopeEnvironments {
        self.envs.clone()
    }

    /// The run this service serves, set by the per-run provisioner.
    pub fn set_run(&self, run: RunInfo) {
        *self.run.lock().unwrap_or_else(PoisonError::into_inner) = Some(run);
    }

    /// Set the embedding host's ceiling for all local agent hooks.
    pub fn set_permission_ceiling(&self, ceiling: crate::AgentPermissions) {
        *self.ceiling.lock().unwrap_or_else(PoisonError::into_inner) = ceiling;
    }

    /// The model client prompt and agent hooks use.
    pub fn set_client(&self, client: Option<PebbleClient>) {
        *self.client.lock().unwrap_or_else(PoisonError::into_inner) = client;
    }

    /// Install the run's hook list, once. Later calls with the same list are
    /// no-ops; a different list replaces it (a second run on one runtime).
    pub fn configure(
        &self,
        hooks: &[HookDefinition],
        workflow: &str,
        default_permissions: crate::AgentPermissions,
    ) {
        let configured = hooks
            .iter()
            .map(|definition| Configured {
                matcher:    definition
                    .matcher
                    .as_deref()
                    .and_then(|pattern| regex::Regex::new(pattern).ok()),
                definition: definition.clone(),
            })
            .collect();
        *self.config.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(Config {
            hooks: configured,
            workflow: workflow.to_owned(),
            default_permissions,
        }));
    }

    /// Configure from a stage config that carries `hooks`, `workflow` and
    /// `default_permissions`, when it does.
    pub fn configure_from(&self, config: &Value) -> bool {
        let Some(list) = config.get("hooks") else {
            return false;
        };
        let hooks: Vec<HookDefinition> = serde_json::from_value(list.clone()).unwrap_or_default();
        let workflow = config
            .get("workflow")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let default_permissions =
            permissions_in(config.get("default_permissions")).unwrap_or_default();
        self.configure(&hooks, workflow, default_permissions);
        true
    }

    pub fn is_configured(&self) -> bool {
        self.config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    fn config(&self) -> Option<Arc<Config>> {
        self.config
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The configured hooks for `event`, whatever their matcher.
    fn hooks_for(&self, event: HookEvent) -> Vec<HookDefinition> {
        self.config().map_or_else(Vec::new, |config| {
            config
                .hooks
                .iter()
                .filter(|hook| hook.definition.event == event)
                .map(|hook| hook.definition.clone())
                .collect()
        })
    }

    fn run_info(&self) -> (String, String) {
        let run = self.run.lock().unwrap_or_else(PoisonError::into_inner);
        let workflow = self
            .config()
            .map(|config| config.workflow.clone())
            .unwrap_or_default();
        (
            run.as_ref()
                .map(|run| run.run_id.clone())
                .unwrap_or_default(),
            workflow,
        )
    }

    /// A context with the run's identity and its agent hooks' access, capped
    /// by the host ceiling, filled in.
    pub fn context(&self, event: HookEvent) -> Context {
        let (run_id, workflow_name) = self.run_info();
        let mut context = Context::new(event, run_id, workflow_name);
        let requested = self
            .config()
            .map_or(crate::AgentPermissions::Full, |config| {
                config.default_permissions
            });
        context.permissions = self.capped(requested);
        context
    }

    /// `requested`, capped by the embedding host's ceiling.
    fn capped(&self, requested: crate::AgentPermissions) -> crate::AgentPermissions {
        requested.min(*self.ceiling.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Run every configured hook matching `context.event`, in order, with
    /// Fabro's blocking rules. The report names the point; the decision is
    /// Fabro's raw decision, mapped by the caller to what its point consumes.
    async fn dispatch(
        &self,
        point: HookPoint,
        context: &Context,
        env: Option<Arc<dyn ExecEnv>>,
    ) -> (Decision, HookReport) {
        let mut report = HookReport::proceed(point);
        let Some(config) = self.config() else {
            return (Decision::Proceed, report);
        };
        if in_hook() {
            return (Decision::Proceed, report);
        }
        let matched: Vec<&Configured> = config
            .hooks
            .iter()
            .filter(|hook| hook.definition.event == context.event && matches(hook, context))
            .collect();
        if matched.is_empty() {
            return (Decision::Proceed, report);
        }
        if context.event == HookEvent::CheckpointSaved {
            for hook in matched {
                report.hooks.push(HookRun {
                    name:        hook.definition.name.clone(),
                    state:       "unsupported".into(),
                    duration_ms: None,
                    message:     Some(
                        "checkpoint_saved is a Fabro platform event; Petri writes no checkpoints, \
                         so the hook does not run"
                            .into(),
                    ),
                    usage:       None,
                });
            }
            return (Decision::Proceed, report);
        }
        let blocking = matched.iter().any(|hook| hook.definition.is_blocking());
        let client = self
            .client
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let mut merged = Decision::Proceed;
        for hook in matched {
            let started = Instant::now();
            let executors::Execution {
                outcome: result,
                usage,
                activity,
            } = IN_HOOK
                .scope(
                    true,
                    executors::execute(
                        &hook.definition,
                        context,
                        env.as_ref(),
                        client.as_ref(),
                        &self.http,
                    ),
                )
                .await;
            let duration = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            // The hook's own agent activity goes on the record under the
            // hook's identity, apart from the stage's own agent.
            report
                .activity
                .extend(activity.into_iter().map(|envelope| HookActivity {
                    hook: HookOperation {
                        point,
                        hook: hook.definition.name.clone(),
                    },
                    backend: SmolStr::new(executors::AGENT_BACKEND),
                    envelope,
                }));
            let mut run = HookRun {
                name: hook.definition.name.clone(),
                state: "executed".into(),
                duration_ms: Some(duration),
                message: None,
                usage,
            };
            let decision = match result {
                executors::Executed::Decided(decision) => decision,
                executors::Executed::FailedOpen(message) => {
                    run.state = "failed_open".into();
                    run.message = Some(message.clone());
                    report.warnings.push(format!(
                        "hook `{}` ({}) failed open: {message}",
                        hook.definition.name,
                        hook.definition.kind.name()
                    ));
                    Decision::Proceed
                }
                executors::Executed::Unsupported(message) => {
                    run.state = "unsupported".into();
                    run.message = Some(message.clone());
                    report.warnings.push(format!(
                        "hook `{}` could not run: {message}",
                        hook.definition.name
                    ));
                    Decision::Proceed
                }
            };
            if decision != Decision::Proceed {
                run.message = Some(match &decision {
                    Decision::Skip { reason } => {
                        format!(
                            "skip{}",
                            reason
                                .as_deref()
                                .map_or(String::new(), |r| format!(": {r}"))
                        )
                    }
                    Decision::Block { reason } => {
                        format!(
                            "block{}",
                            reason
                                .as_deref()
                                .map_or(String::new(), |r| format!(": {r}"))
                        )
                    }
                    Decision::Override { edge_to } => format!("override: {edge_to}"),
                    Decision::Proceed => String::new(),
                });
            }
            let consumed = blocking && hook.definition.is_blocking();
            if !consumed && decision != Decision::Proceed {
                report.warnings.push(format!(
                    "hook `{}` is not blocking; its decision is ignored",
                    hook.definition.name
                ));
            }
            report.hooks.push(run);
            if consumed {
                merged = merged.merge(decision);
                if merged.is_block() {
                    break;
                }
            }
        }
        (merged, report)
    }
}

/// Fabro's matcher: absent matches every occurrence; a regex is tested
/// against whichever of the node id, handler type, edge ends and tool name
/// the event carries, unanchored.
fn matches(hook: &Configured, context: &Context) -> bool {
    if hook.definition.matcher.is_none() {
        return true;
    }
    let Some(regex) = &hook.matcher else {
        return false;
    };
    [
        context.node_id.as_deref(),
        context.handler_type.as_deref(),
        context.edge_to.as_deref(),
        context.edge_from.as_deref(),
        context.tool_name.as_deref(),
    ]
    .into_iter()
    .flatten()
    .any(|field| regex.is_match(field))
}

/// The Fabro handler type of a node, from the frontend's `meta.kind`.
fn handler_type(view: &FiringView) -> Option<String> {
    view.meta()
        .get("kind")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn is_synthetic(view: &FiringView) -> bool {
    view.meta().get("synthetic").and_then(Value::as_bool) == Some(true)
        || view.node_name() == GOAL_CHECK_NODE
}

/// The edge table the frontend attached to a node: `meta.edges`, one entry
/// per arm id with its target node id and label.
fn edge_meta(view: &FiringView, edge: EdgeId) -> Option<(String, Option<String>)> {
    let entry = view.meta().get("edges")?.get(edge.raw().to_string())?;
    let to = entry.get("to")?.as_str()?.to_owned();
    let label = entry
        .get("label")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Some((to, label))
}

/// The arm whose target node id is `to`, for an override.
fn edge_for_target(view: &FiringView, to: &str) -> Option<(u32, EdgeId)> {
    let edges = view.meta().get("edges")?.as_object()?;
    let id = edges
        .iter()
        .find(|(_, entry)| entry.get("to").and_then(Value::as_str) == Some(to))
        .and_then(|(id, _)| id.parse::<u32>().ok())?;
    let edge = EdgeId::new(id);
    view.node
        .routing
        .groups
        .iter()
        .enumerate()
        .find(|(_, group)| group.arms.iter().any(|arm| arm.id == edge))
        .map(|(index, _)| (u32::try_from(index).unwrap_or(0), edge))
}

impl LocalHooks {
    fn stage_context(&self, event: HookEvent, view: &FiringView) -> Context {
        let mut context = self.context(event);
        // A step-built view carries the stage's level on its config; a
        // node's own view, on its meta.
        let stage = view
            .config
            .as_ref()
            .and_then(|config| config.get("permissions"))
            .or_else(|| view.meta().get("permissions"));
        if let Some(requested) = permissions_in(stage) {
            context.permissions = self.capped(requested);
        }
        context.node_id = Some(view.node_name().to_owned());
        context.node_label = view
            .meta()
            .get("label")
            .and_then(Value::as_str)
            .map(str::to_owned);
        context.handler_type = handler_type(view);
        context
    }

    fn env_for(&self, view: &FiringView) -> Option<Arc<dyn ExecEnv>> {
        self.envs.get(view.scope)
    }

    /// Fabro's `stage_start`: before every attempt. The `start` stage's
    /// arrives from the stage step with the sandbox in place, after
    /// `sandbox_ready` and `run_start`; every other node's from the adapter.
    async fn before_attempt(&self, view: &FiringView) -> HookReport {
        let mut context = self.stage_context(HookEvent::StageStart, view);
        context.cwd = self
            .env_for(view)
            .map(|env| env.workspace_path().to_owned());
        context.attempt = Some(view.attempt.raw());
        context.max_attempts = Some(view.node.retry.max_attempts.get());
        let (decision, mut report) = self
            .dispatch(HookPoint::BeforeAttempt, &context, self.env_for(view))
            .await;
        report.decision = match decision {
            Decision::Skip { .. } => HookDecision::Skip {
                status: Status::Skipped,
            },
            Decision::Block { reason } => HookDecision::Block {
                reason: reason.unwrap_or_else(|| "blocked by StageStart hook".into()),
            },
            Decision::Proceed | Decision::Override { .. } => HookDecision::Proceed,
        };
        report
    }

    /// Fabro's `parallel_start`: once per fork visit, before its branches,
    /// asked by the fork step. The view is the parallel node's.
    async fn fork_started(&self, view: &FiringView) -> HookReport {
        let mut context = self.stage_context(HookEvent::ParallelStart, view);
        context.cwd = self
            .env_for(view)
            .map(|env| env.workspace_path().to_owned());
        let (_, report) = self
            .dispatch(HookPoint::ForkStarted, &context, self.env_for(view))
            .await;
        report
    }

    /// Fabro's `parallel_complete`, asked by the fan-in step once every
    /// branch of the fork is in. The context names the parallel node, as
    /// Fabro's does; the payload says which.
    async fn fork_completed(&self, view: &FiringView, payload: &Value) -> HookReport {
        let point = HookPoint::ForkCompleted;
        let Ok(completed) = serde_json::from_value::<ForkCompletedPayload>(payload.clone()) else {
            return HookReport::proceed(point);
        };
        let mut context = self.context(HookEvent::ParallelComplete);
        context.node_id = Some(completed.fork.to_string());
        context.handler_type = Some("parallel".into());
        context.cwd = self
            .env_for(view)
            .map(|env| env.workspace_path().to_owned());
        let (_, report) = self.dispatch(point, &context, self.env_for(view)).await;
        report
    }

    /// Fabro's `sandbox_ready` and `run_start`, asked by the `start` stage
    /// with the sandbox in place. Both block: a block stops the run before
    /// any work.
    async fn run_level_start(&self, point: HookPoint, view: &FiringView) -> HookReport {
        let (event, title) = match point {
            HookPoint::ScopeReady => (HookEvent::SandboxReady, "SandboxReady"),
            _ => (HookEvent::RunStart, "RunStart"),
        };
        let context = self.context(event);
        let (decision, mut report) = self.dispatch(point, &context, self.env_for(view)).await;
        if let Decision::Block { reason } = decision {
            report.decision = HookDecision::Block {
                reason: reason.unwrap_or_else(|| format!("blocked by {title} hook")),
            };
        }
        report
    }

    async fn retrying(&self, view: &FiringView) -> HookReport {
        let mut context = self.stage_context(HookEvent::StageRetrying, view);
        context.attempt = Some(view.attempt.raw());
        context.max_attempts = Some(view.node.retry.max_attempts.get());
        let (_, report) = self
            .dispatch(HookPoint::Retrying, &context, self.env_for(view))
            .await;
        report
    }

    async fn after_visit(&self, view: &FiringView, outcome: Option<&Outcome>) -> HookReport {
        let Some(outcome) = outcome else {
            return HookReport::proceed(HookPoint::AfterVisit);
        };
        // A skipped stage had no start, so it has no completion either.
        if matches!(outcome.status, Status::Skipped) {
            return HookReport::proceed(HookPoint::AfterVisit);
        }
        let reported = reported_outcome(outcome);
        let event = if outcome.status.is_failure() || matches!(outcome.status, Status::Cancelled) {
            HookEvent::StageFailed
        } else {
            HookEvent::StageComplete
        };
        let mut context = self.stage_context(event, view);
        context.status = Some(reported.as_str().to_owned());
        context.failure_reason = outcome
            .status
            .failure_info()
            .map(|info| info.message.clone())
            .or_else(|| {
                outcome
                    .output
                    .get("failure_reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        let (_, report) = self
            .dispatch(HookPoint::AfterVisit, &context, self.env_for(view))
            .await;
        report
    }

    async fn route_selected(
        &self,
        view: &FiringView,
        routes: &[(u32, RouteDecision)],
    ) -> HookReport {
        let mut report = HookReport::proceed(HookPoint::RouteSelected);
        for (group, decision) in routes {
            let RouteDecision::Emit(edge) = decision else {
                continue;
            };
            let Some((to, label)) = edge_meta(view, *edge) else {
                continue;
            };
            let mut context = self.stage_context(HookEvent::EdgeSelected, view);
            context.edge_from = Some(view.node_name().to_owned());
            context.edge_to = Some(to);
            context.edge_label = label;
            let (decision, extra) = self
                .dispatch(HookPoint::RouteSelected, &context, self.env_for(view))
                .await;
            merge_report(&mut report, extra);
            match decision {
                Decision::Block { reason } => {
                    report.decision = HookDecision::Block {
                        reason: reason.unwrap_or_else(|| "blocked by EdgeSelected hook".into()),
                    };
                    return report;
                }
                Decision::Override { edge_to } => match edge_for_target(view, &edge_to) {
                    Some((_, edge)) => {
                        report.decision = HookDecision::Override {
                            group: *group,
                            edge,
                        };
                    }
                    None => report.warnings.push(format!(
                        "edge_selected hook named `{edge_to}`, which is not a target of `{}`; the \
                         override is ignored",
                        view.node_name()
                    )),
                },
                Decision::Proceed | Decision::Skip { .. } => {}
            }
        }
        report
    }

    /// The run ended: Fabro's `on_run_end`. `run_complete` for a run that
    /// succeeded, `run_failed` with the failure reason for one that failed,
    /// nothing for a cancelled run. The environments are still usable, so a
    /// sandbox-placed hook runs in the run's sandbox.
    async fn run_finished(&self, payload: &Value) -> HookReport {
        let point = HookPoint::RunFinished;
        let Ok(finished) = serde_json::from_value::<RunFinishedPayload>(payload.clone()) else {
            return HookReport::proceed(point);
        };
        let event = match finished.status {
            RunStatus::Success => HookEvent::RunComplete,
            RunStatus::Failed => HookEvent::RunFailed,
            RunStatus::Cancelled => return HookReport::proceed(point),
        };
        let mut context = self.context(event);
        if event == HookEvent::RunFailed {
            context.failure_reason = Some(finished.failure.unwrap_or_else(|| "run failed".into()));
        }
        let (_, report) = self.dispatch(point, &context, self.envs.any()).await;
        report
    }

    /// A scope's environment is about to go: Fabro's `sandbox_cleanup`, run
    /// in that environment before it is stopped.
    async fn scope_released(&self, payload: &Value) -> HookReport {
        let point = HookPoint::ScopeReleased;
        let Ok(released) = serde_json::from_value::<ScopeReleasedPayload>(payload.clone()) else {
            return HookReport::proceed(point);
        };
        let context = self.context(HookEvent::SandboxCleanup);
        let (_, report) = self
            .dispatch(point, &context, self.envs.get(released.scope))
            .await;
        self.envs.remove(released.scope);
        report
    }

    /// A tool-boundary point, asked by the agent backend.
    async fn tool_point(&self, point: HookPoint, view: &FiringView, payload: &Value) -> HookReport {
        let event = match point {
            HookPoint::BeforeToolUse => HookEvent::PreToolUse,
            HookPoint::AfterToolUse => HookEvent::PostToolUse,
            _ => HookEvent::PostToolUseFailure,
        };
        let tool: ToolPayload = serde_json::from_value(payload.clone()).unwrap_or_default();
        let mut context = self.stage_context(event, view);
        context.tool_name = Some(tool.tool_name);
        match event {
            HookEvent::PreToolUse => context.tool_input = tool.tool_input,
            HookEvent::PostToolUse => {
                context.tool_call_id = tool.tool_call_id;
                context.tool_output = tool.tool_output;
            }
            _ => {
                context.tool_call_id = tool.tool_call_id;
                context.error_message = tool.error_message;
            }
        }
        let (decision, mut report) = self.dispatch(point, &context, self.env_for(view)).await;
        if point == HookPoint::BeforeToolUse
            && let Decision::Block { reason } = decision
        {
            report.decision = HookDecision::Block {
                reason: reason.unwrap_or_else(|| "Blocked by hook".into()),
            };
        }
        report
    }
}

fn merge_report(report: &mut HookReport, extra: HookReport) {
    report.hooks.extend(extra.hooks);
    report.warnings.extend(extra.warnings);
    report.activity.extend(extra.activity);
}

/// Record a report a step asked for itself on the step's progress channel:
/// each entry of the hooks' agent activity as its own `hook.activity` note,
/// as the adapter records them, then the report as a [`REPORT_EVENT`] when it
/// says something.
pub async fn record_report(
    logs: &steps::ProgressSender,
    node: &str,
    firing: ir::FiringId,
    attempt: ir::Attempt,
    event: HookEvent,
    report: &HookReport,
) {
    for activity in &report.activity {
        let note = Note::new(
            HOOK_ACTIVITY_NOTE_KIND,
            serde_json::to_value(activity).unwrap_or(Value::Null),
        );
        let _ = logs.send(note.to_step_event()).await;
    }
    if !report.is_silent() {
        let _ = logs
            .send(report_event(node, firing, attempt, event, report))
            .await;
    }
}

#[async_trait::async_trait]
impl HookService for LocalHooks {
    async fn run(&self, request: HookRequest) -> HookReport {
        let Some(view) = request.view.as_deref() else {
            if !self.is_configured() {
                return HookReport::proceed(request.point);
            }
            return match request.point {
                HookPoint::RunFinished => self.run_finished(&request.payload).await,
                HookPoint::ScopeReleased => self.scope_released(&request.payload).await,
                _ => HookReport::proceed(request.point),
            };
        };
        if let Some(config) = &view.config
            && config.get("hooks").is_some()
        {
            self.configure_from(config);
        }
        if !self.is_configured() || is_synthetic(view) {
            return HookReport::proceed(request.point);
        }
        match request.point {
            HookPoint::BeforeVisit
            | HookPoint::AfterAttempt
            | HookPoint::RunFinished
            | HookPoint::ScopeReleased => HookReport::proceed(request.point),
            HookPoint::BeforeAttempt => self.before_attempt(view).await,
            HookPoint::Retrying => self.retrying(view).await,
            HookPoint::AfterVisit => self.after_visit(view, request.outcome.as_ref()).await,
            HookPoint::RouteSelected => self.route_selected(view, &request.routes).await,
            HookPoint::ForkStarted => self.fork_started(view).await,
            HookPoint::ForkCompleted => self.fork_completed(view, &request.payload).await,
            HookPoint::ScopeReady | HookPoint::RunStarted => {
                self.run_level_start(request.point, view).await
            }
            HookPoint::BeforeToolUse | HookPoint::AfterToolUse | HookPoint::AfterToolFailure => {
                self.tool_point(request.point, view, &request.payload).await
            }
        }
    }

    fn configured_hooks(&self, point: HookPoint) -> Vec<String> {
        events_of(point)
            .iter()
            .flat_map(|event| self.hooks_for(*event))
            .map(|hook| hook.name)
            .collect()
    }
}

/// The Fabro events a point serves, for [`HookService::configured_hooks`].
/// `BeforeVisit` and `AfterAttempt` serve none: Fabro's `stage_start` is the
/// per-attempt point, and Fabro has no result-adjusting hook.
fn events_of(point: HookPoint) -> &'static [HookEvent] {
    match point {
        HookPoint::BeforeVisit | HookPoint::AfterAttempt => &[],
        HookPoint::BeforeAttempt => &[HookEvent::StageStart],
        HookPoint::Retrying => &[HookEvent::StageRetrying],
        HookPoint::AfterVisit => &[HookEvent::StageComplete, HookEvent::StageFailed],
        HookPoint::RouteSelected => &[HookEvent::EdgeSelected],
        HookPoint::ForkStarted => &[HookEvent::ParallelStart],
        HookPoint::ForkCompleted => &[HookEvent::ParallelComplete],
        HookPoint::ScopeReady => &[HookEvent::SandboxReady],
        HookPoint::RunStarted => &[HookEvent::RunStart],
        HookPoint::RunFinished => &[HookEvent::RunComplete, HookEvent::RunFailed],
        HookPoint::ScopeReleased => &[HookEvent::SandboxCleanup],
        HookPoint::BeforeToolUse => &[HookEvent::PreToolUse],
        HookPoint::AfterToolUse => &[HookEvent::PostToolUse],
        HookPoint::AfterToolFailure => &[HookEvent::PostToolUseFailure],
    }
}

/// The `StepEvent::Custom` payload for a report a step drives itself.
pub fn report_event(
    node: &str,
    firing: ir::FiringId,
    attempt: ir::Attempt,
    event: HookEvent,
    report: &HookReport,
) -> ir::StepEvent {
    ir::StepEvent::Custom(json!({
        "kind": REPORT_EVENT,
        "node": node,
        "firing": firing,
        "attempt": attempt,
        "event": event.as_str(),
        "report": report,
    }))
}

/// The `StepEvent::Custom` payload for an enforcement warning.
pub fn warning_event(
    node: &str,
    firing: ir::FiringId,
    attempt: ir::Attempt,
    backend: &str,
    hook: &str,
    event: HookEvent,
    boundary: &str,
    message: &str,
) -> ir::StepEvent {
    ir::StepEvent::Custom(json!({
        "kind": WARNING_EVENT,
        "node": node,
        "firing": firing,
        "attempt": attempt,
        "backend": backend,
        "hook": hook,
        "event": event.as_str(),
        "boundary": boundary,
        "message": message,
    }))
}

/// The permission level a config or meta value names. A value that is not a
/// level fails closed to `read-only`.
fn permissions_in(value: Option<&Value>) -> Option<crate::AgentPermissions> {
    value.map(|value| {
        value
            .as_str()
            .and_then(|text| text.parse().ok())
            .unwrap_or(crate::AgentPermissions::ReadOnly)
    })
}

/// A `FiringView` for a step that asks a point itself (the tool boundary,
/// the start stage's points, a fork's): what the service reads from a view,
/// built from the step's context. `kind` is the Fabro handler type.
pub fn step_view(ctx: &steps::StepCtx, kind: &str, label: &str, kv: &Value) -> Arc<FiringView> {
    view_of(ctx, kind, label, kv, None)
}

/// [`step_view`] with the step's resolved config on the view, for a step
/// whose config the service reads (the `start` stage carries the run's hook
/// list).
pub fn view_of(
    ctx: &steps::StepCtx,
    kind: &str,
    label: &str,
    kv: &Value,
    config: Option<Value>,
) -> Arc<FiringView> {
    let node = ir::Node::new(
        ir::NodeId::new(0),
        &ctx.node,
        ctx.scope,
        ir::StepRef::new(kind, Value::Null),
    )
    .with_meta(json!({ "kind": kind, "label": label }));
    let mut context = ir::RunContext::new();
    if let Value::Object(map) = kv {
        context.merge(
            &map.iter()
                .map(|(k, v)| (smol_str::SmolStr::new(k), v.clone()))
                .collect(),
        );
    }
    Arc::new(FiringView {
        firing: ctx.firing,
        attempt: ctx.attempt,
        generation: ir::Generation::ZERO,
        node: Arc::new(node),
        visit: 0,
        scope: ctx.scope,
        inputs: Vec::new(),
        config,
        context,
        branch: BranchRole::None,
    })
}

/// The empty JSON object, for callers with no context.
pub fn empty_map() -> Map<String, Value> {
    Map::new()
}

impl fmt::Debug for LocalHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalHooks")
            .field("configured", &self.is_configured())
            .finish_non_exhaustive()
    }
}

/// The hook definitions a stage config carries, for a caller that needs the
/// list itself.
pub fn definitions_in(config: &Value) -> Vec<HookDefinition> {
    config
        .get("hooks")
        .cloned()
        .map(|list| serde_json::from_value(list).unwrap_or_default())
        .unwrap_or_default()
}

/// A lookup by name, for tests and warnings.
pub fn by_name(hooks: &[HookDefinition]) -> HashMap<&str, &HookDefinition> {
    hooks
        .iter()
        .map(|hook| (hook.name.as_str(), hook))
        .collect()
}

#[cfg(test)]
mod tests {
    use frontend_attractor::hooks::HookKind;

    use super::*;

    #[test]
    fn decisions_merge_as_fabro_merges_them() {
        let block = Decision::Block { reason: None };
        let skip = Decision::Skip { reason: None };
        let over = Decision::Override {
            edge_to: "b".into(),
        };
        assert_eq!(Decision::Proceed.merge(skip.clone()), skip);
        assert_eq!(skip.clone().merge(block.clone()), block);
        assert_eq!(block.clone().merge(skip.clone()), block);
        assert_eq!(
            skip.clone().merge(over.clone()),
            skip,
            "the first non-proceed wins"
        );
        assert_eq!(over.clone().merge(skip), over);
        assert_eq!(
            Decision::Proceed.merge(Decision::Proceed),
            Decision::Proceed
        );
    }

    #[test]
    fn matchers_test_every_present_field_unanchored() {
        let hook = |pattern: Option<&str>| Configured {
            definition: HookDefinition {
                name:       "h".into(),
                id:         None,
                event:      HookEvent::StageStart,
                kind:       HookKind::Command {
                    command: "true".into(),
                },
                matcher:    pattern.map(Into::into),
                blocking:   None,
                timeout_ms: None,
                sandbox:    None,
                source:     None,
            },
            matcher:    pattern.and_then(|p| regex::Regex::new(p).ok()),
        };
        let mut context = Context::new(HookEvent::StageStart, String::new(), String::new());
        context.node_id = Some("implement_step".into());
        context.handler_type = Some("agent".into());
        assert!(matches(&hook(None), &context));
        assert!(matches(&hook(Some("implement")), &context));
        assert!(matches(&hook(Some("^agent$")), &context));
        assert!(!matches(&hook(Some("^implement$")), &context));
        let bare = Context::new(HookEvent::StageStart, String::new(), String::new());
        assert!(!matches(&hook(Some("agent")), &bare), "no field, no match");
        assert!(matches(&hook(None), &bare));
    }

    #[test]
    fn decision_json_is_fabros_wire_shape() {
        assert_eq!(
            serde_json::from_str::<Decision>(r#"{"decision":"skip","reason":"ci"}"#).ok(),
            Some(Decision::Skip {
                reason: Some("ci".into()),
            })
        );
        assert_eq!(
            serde_json::from_str::<Decision>(r#"{"decision":"override","edge_to":"deploy"}"#).ok(),
            Some(Decision::Override {
                edge_to: "deploy".into(),
            })
        );
        assert!(serde_json::from_str::<Decision>(r#"{"ok":true}"#).is_err());
    }
}
