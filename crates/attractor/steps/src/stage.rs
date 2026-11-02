//! `attractor/stage`: Fabro's structural stages (`start`, `exit`, a
//! conditional) as a step that records where its scope runs.
//!
//! The engine acquires a scope's environment when the first firing in it is
//! admitted, after the admission hooks ran. A hook that must run in the
//! sandbox (`sandbox = true`, the default for command hooks) therefore
//! needs the environment from a step that has already run in that scope.
//! Every Fabro step registers its environment in [`ScopeEnvironments`]
//! through [`record`]; `start` is the first, so `sandbox_ready` and every
//! later stage hook find it. The step itself does what `noop` did: it
//! returns its resolved config as its output.
//!
//! `start` also carries the run's `[run.model.fallbacks]` table and checks
//! it against the model catalog before anything else: a table the run
//! cannot use fails the run at run start, as Fabro's server refuses it,
//! not at the first LLM stage that would have read it. A graph admitted
//! with a catalog carries no table here: admission checked it and pinned
//! every route (`crate::admission`), so `start` has nothing to check.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use execution::hooks::{
    HookDecision, HookPoint, HookRequest, HookService, HookServiceHandle, ScopeReadyPayload,
};
use executor::ExecEnv;
use frontend_attractor::hooks::HookEvent;
use frontend_attractor::kinds::{STAGE_KIND, StageOutcome};
use ir::{Outcome, ScopeId, StepKindId, Value};
use serde::Deserialize;
use steps::{Step, StepCtx};

use crate::hooks::{BLOCKED_CLASS, record_report, view_of};
use crate::outcome::Stage;
use crate::pebble::PebbleClient;
use crate::{checkout, fallback};

pub const KIND: StepKindId = STAGE_KIND;

/// The run's identity as hooks see it: `FABRO_RUN_ID` and the `run_id` field
/// of every hook context. Set by the per-run provisioner from the run
/// directory's name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunInfo {
    pub run_id: String,
}

/// The environments of the scopes that have run a Fabro step, by scope id.
/// Registered as a capability by [`crate::register`], where every Fabro step
/// records its environment through [`record`]; the local hook service holds
/// a clone and runs sandbox-placed hooks through it. Clones share one map.
#[derive(Clone, Default)]
pub struct ScopeEnvironments {
    envs: Arc<Mutex<HashMap<ScopeId, Arc<dyn ExecEnv>>>>,
}

impl ScopeEnvironments {
    pub fn new() -> Self {
        Self::default()
    }

    /// The first environment recorded for a scope stays: a branch child runs
    /// in the parent's sandbox under its own execution-local scope id, and its
    /// handle is released when the child ends, so it must not replace the
    /// run's own. [`remove`](Self::remove) clears a scope when it is released.
    pub fn record(&self, scope: ScopeId, env: Arc<dyn ExecEnv>) {
        self.envs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(scope)
            .or_insert(env);
    }

    /// Forget a scope's environment: it is being released.
    pub fn remove(&self, scope: ScopeId) {
        self.envs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&scope);
    }

    /// The scope's environment, or, when that scope has not run a step yet,
    /// any recorded environment (every scope of a standalone Fabro run
    /// shares one sandbox).
    pub fn get(&self, scope: ScopeId) -> Option<Arc<dyn ExecEnv>> {
        let envs = self.envs.lock().unwrap_or_else(PoisonError::into_inner);
        envs.get(&scope).or_else(|| envs.values().next()).cloned()
    }

    /// Any recorded environment: where a run-level hook runs.
    pub fn any(&self) -> Option<Arc<dyn ExecEnv>> {
        self.envs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .next()
            .cloned()
    }
}

/// Record the step's environment for hooks that run in its scope.
pub fn record(ctx: &StepCtx) {
    if let Some(envs) = ctx.capability::<ScopeEnvironments>() {
        envs.record(ctx.scope, ctx.env.clone());
    }
}

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StageConfig {
    pub node:                String,
    pub kind:                String,
    pub label:               String,
    /// The run's merged `[[run.hooks]]`, on `start` and `exit`.
    pub hooks:               Value,
    /// The workflow's name, for `FABRO_WORKFLOW`.
    pub workflow:            String,
    /// The run context at spawn.
    pub kv:                  Value,
    /// `[run.clone]` and the repository the host bound, on the root
    /// `start` stage: the workspace is checked out from it first.
    pub checkout:            Value,
    /// `[run.model.fallbacks]` as written, on `start`: checked against the
    /// catalog before anything runs, so a table the run cannot use fails
    /// the run where Fabro's server refuses it, at run start, and not at
    /// the first LLM stage that would have read it. Absent once admission
    /// checked the table and pinned the run's routes.
    pub fallbacks:           Value,
    /// The stage's own agent permissions, for hooks attached to it.
    pub permissions:         crate::AgentPermissions,
    /// The run-level agent hooks' permissions, on `start` and `exit`.
    pub default_permissions: crate::AgentPermissions,
    #[serde(flatten)]
    pub rest:                serde_json::Map<String, Value>,
}

impl Default for StageConfig {
    fn default() -> Self {
        Self {
            node:                String::new(),
            kind:                String::new(),
            label:               String::new(),
            hooks:               Value::Null,
            workflow:            String::new(),
            kv:                  Value::Null,
            checkout:            Value::Null,
            fallbacks:           Value::Null,
            permissions:         crate::AgentPermissions::Full,
            default_permissions: crate::AgentPermissions::Full,
            rest:                serde_json::Map::new(),
        }
    }
}

pub struct StageStep;

/// The hooks a stage asks for, in Fabro's order. `start` asks `sandbox_ready`
/// (the sandbox exists once the first step runs in it) then `run_start`, then
/// its own `stage_start`; all block. `run_complete`, `run_failed` and
/// `sandbox_cleanup` are not a stage's: the hook service runs them from the
/// driver's run-end and scope-release points, by the run's final status,
/// with the sandbox still in place.
#[async_trait::async_trait]
impl Step for StageStep {
    const NAME: &'static str = "attractor/stage";
    type Config = StageConfig;

    async fn run(&self, config: StageConfig, ctx: StepCtx) -> Outcome {
        record(&ctx);
        // Configuration first: a `[run.model.fallbacks]` table the catalog
        // cannot resolve ends the run before a checkout, a hook or a node
        // spends anything on it.
        if config.kind == "start"
            && !config.fallbacks.is_null()
            && let Some(client) = ctx.capability::<PebbleClient>()
            && let Err(message) = fallback::check_table(&client.0, &config.fallbacks)
        {
            return Stage::failed(
                message,
                "bad_config",
                Some(frontend_attractor::Policy::Exit),
            )
            .into_outcome(&config.node);
        }
        // The checkout comes first: the sandbox is "ready" once the
        // repository is in it, as Fabro's clone precedes `sandbox_ready`.
        if config.kind == "start"
            && let Err(error) = checkout::seed(&ctx, &config.checkout).await
        {
            return Stage::failed(
                format!("checkout: {error}"),
                checkout::CLASS,
                Some(frontend_attractor::Policy::Exit),
            )
            .into_outcome(&config.node);
        }
        if config.kind == "start"
            && let Some(handle) = ctx.capability::<HookServiceHandle>()
            && let Some(stopped) = start_hooks(&handle.0, &config, &ctx).await
        {
            return stopped.into_outcome(&config.node);
        }
        let mut stage = Stage::new(StageOutcome::Succeeded, None);
        for (key, value) in config.rest {
            stage.output.insert(key, value);
        }
        // Fabro sets `internal.run_id` at run creation; the bundles' prepare
        // nodes read it through `stdin_source`. The root `start` stage is the
        // first thing that runs, so it publishes the run's identity.
        if config.kind == "start"
            && !config.checkout.is_null()
            && let Some(run) = ctx.capability::<RunInfo>()
            && !run.run_id.is_empty()
        {
            stage
                .context_updates
                .insert("internal.run_id".into(), Value::String(run.run_id.clone()));
        }
        stage.into_outcome(&config.node)
    }
}

/// The points `start` asks the hook service itself, with the sandbox in
/// place, in Fabro's order: `sandbox_ready` (`ScopeReady`), `run_start`
/// (`RunStarted`), then its own admission (`BeforeVisit` and
/// `BeforeAttempt`, Fabro's `stage_start`). The driver admits `start` before
/// its scope's environment exists, so the frontend marks the node
/// `admission_hooks = "step"` and the adapter stays away from its admission
/// points: each is asked once, here. The two run-level points are the root
/// workflow's alone (its `start` carries the run's hook list); a nested
/// workflow's `start` asks only its own admission. Every report that says
/// something is recorded as an `attractor.hook` event on this firing. Returns
/// the stage a stopping decision ends the run with.
async fn start_hooks(
    service: &Arc<dyn HookService>,
    config: &StageConfig,
    ctx: &StepCtx,
) -> Option<Stage> {
    let view = view_of(
        ctx,
        "start",
        &config.label,
        &config.kv,
        Some(ctx.config.clone()),
    );
    let mut points: Vec<(HookPoint, HookEvent, Value)> = Vec::new();
    if !config.hooks.is_null() {
        let ready = ScopeReadyPayload {
            scope:     ctx.scope,
            workspace: ctx.env.workspace_path().to_owned(),
        };
        points.push((
            HookPoint::ScopeReady,
            HookEvent::SandboxReady,
            serde_json::to_value(ready).unwrap_or(Value::Null),
        ));
        points.push((HookPoint::RunStarted, HookEvent::RunStart, Value::Null));
    }
    points.push((HookPoint::BeforeVisit, HookEvent::StageStart, Value::Null));
    points.push((HookPoint::BeforeAttempt, HookEvent::StageStart, Value::Null));
    for (point, event, payload) in points {
        let report = service
            .run(HookRequest {
                point,
                view: Some(view.clone()),
                outcome: None,
                routes: Vec::new(),
                payload,
            })
            .await;
        record_report(
            &ctx.logs,
            &ctx.node,
            ctx.firing,
            ctx.attempt,
            event,
            &report,
        )
        .await;
        let admission = matches!(point, HookPoint::BeforeVisit | HookPoint::BeforeAttempt);
        match report.decision {
            HookDecision::Skip { .. } if admission => {
                return Some(Stage::new(StageOutcome::Skipped, None));
            }
            HookDecision::Block { reason } => {
                return Some(Stage::failed(
                    format!("blocked: {reason}"),
                    BLOCKED_CLASS,
                    Some(frontend_attractor::Policy::Exit),
                ));
            }
            // A decision the point does not consume is recorded and ignored,
            // as the adapter ignores it.
            _ => {}
        }
    }
    None
}
