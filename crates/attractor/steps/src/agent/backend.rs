//! The two agent transports share the step's output contract and repair loop.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use execution::hooks::{HookPoint, HookServiceHandle};
use frontend_attractor::kinds::RETRY_REQUESTED_CLASS;
use ir::{Control, Value};
use pebble_coding_agent::{CodingAgentExport, CodingInput, InputContent, ShutdownReason};
use serde::Deserialize;
use smol_str::SmolStr;
use steps::{SECRET_UNAVAILABLE_CLASS, StepCtx};
use tokio::sync::mpsc;
use tokio::time::timeout;

use super::AgentConfig;
use crate::acp::{AcpError, AcpHooks, Client, Stage};
use crate::fallback::{ModelFailure, Plan};
use crate::hooks::{ToolHookBinding, step_view};
use crate::pebble::{NativeSession, Resume};
use crate::permissions;

/// How an agent node runs. The native API agent is the default, as Fabro's
/// `select_run_backend` picks `Api` for a node that names no backend; the
/// pinned bundles name none.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentBackend {
    Acp,
    #[default]
    Api,
}

/// The authored command is separate from workflow data. ACP still receives
/// their concatenation; only the native session expands skills.
pub(crate) enum Prompt {
    Authored {
        before: String,
        text:   String,
        after:  String,
    },
    Generated(String),
}

impl Prompt {
    fn assembled(self) -> String {
        match self {
            Self::Authored {
                mut before,
                text,
                after,
            } => {
                before.push_str(&text);
                before.push_str(&after);
                before
            }
            Self::Generated(text) => text,
        }
    }

    fn into_coding_input(self) -> CodingInput {
        match self {
            Self::Authored {
                before,
                text,
                after,
            } => CodingInput::from(prompt_content(text))
                .with_context(prompt_content(before), prompt_content(after)),
            Self::Generated(text) => CodingInput::text(text).without_skill_expansion(),
        }
    }
}

/// An absent prompt segment must not become an empty text block:
/// providers such as Anthropic reject those blocks, including on fallback.
fn prompt_content(text: String) -> InputContent {
    if text.is_empty() {
        InputContent::default()
    } else {
        InputContent::text(text)
    }
}

pub(crate) enum Session {
    Acp(Box<Client>),
    Pebble(Box<NativeSession>),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AgentError {
    #[error("agent cancelled")]
    Cancelled,
    #[error("{message}")]
    Failed { class: String, message: String },
    /// A typed model error, kept whole so the stage reports its class and
    /// the prompt step's own plan can read it.
    #[error("{0}")]
    Model(ModelFailure),
}
impl AgentError {
    pub(crate) fn failed(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Failed {
            class:   class.into(),
            message: message.into(),
        }
    }
}
impl From<AcpError> for AgentError {
    /// A turn that fails after the agent started asks for a retry. Fabro's
    /// ACP backend turns every such failure (the process exits before the
    /// protocol completes, a protocol error, a rejected request, a stop
    /// reason other than `end_turn` or `refusal`) into a handler error
    /// (`handler/llm/acp.rs::acp_error_to_workflow`), and its engine retries
    /// a handler error while attempts remain (`handler/mod.rs::should_retry`
    /// is `is_retryable`, true for the handler stage). The same failures
    /// carry the `retry_requested` class here, so `max_retries` and
    /// `retry_policy` apply to them; the message keeps what went wrong.
    fn from(error: AcpError) -> Self {
        match error {
            AcpError::Cancelled => Self::Cancelled,
            AcpError::StopReason(reason) => Self::failed(
                RETRY_REQUESTED_CLASS,
                format!("the agent stopped with `{reason}`"),
            ),
            other => Self::failed(RETRY_REQUESTED_CLASS, other.to_string()),
        }
    }
}

impl Session {
    /// Open the node's session. `resume` says where a native conversation
    /// comes from (a plan's route, or a retained export with the plan it
    /// carries); an ACP node ignores it (ACP never reuses threads and runs
    /// no fallback, and the caller has said so).
    pub(crate) async fn open(
        config: &AgentConfig,
        ctx: &mut StepCtx,
        resume: Resume,
    ) -> Result<Self, AgentError> {
        match config.backend {
            AgentBackend::Api => NativeSession::open(config, ctx, resume)
                .await
                .map(|session| Self::Pebble(Box::new(session))),
            AgentBackend::Acp => {
                if permissions::effective(config.permissions, ctx) != crate::AgentPermissions::Full
                {
                    return Err(AgentError::failed(
                        "unsupported_permissions",
                        "backend=acp cannot enforce restricted agent permissions; use backend=api",
                    ));
                }
                let command = config
                    .command()
                    .map_err(|e| AgentError::failed("acp_unconfigured", e))?;
                if config.model.is_some()
                    || config.provider.is_some()
                    || config.reasoning_effort.is_some()
                {
                    tracing::warn!(node = %config.node, "the ACP command owns model selection; model, provider and reasoning_effort are observer metadata");
                }
                // The agent's environment: the workflow's secrets and the
                // command's own `env`, references resolved. Nothing else of
                // the run's secrets, not even a product's API key.
                let spec = command
                    .spec(ctx.secrets.as_ref())
                    .map_err(|e| AgentError::failed(SECRET_UNAVAILABLE_CLASS.as_str(), e))?;
                let stage = Stage {
                    node:    ctx.node.clone(),
                    firing:  ctx.firing,
                    attempt: ctx.attempt,
                    scope:   ctx.scope,
                    masker:  ctx.secrets.masker(),
                };
                let mut client = Client::spawn(ctx.env.as_ref(), spec, ctx.logs.clone(), stage)
                    .await
                    .map_err(|e| AgentError::failed("spawn_failed", e.to_string()))?;
                // The hook service is asked at the two boundaries ACP has (a
                // permission request, a reported tool call finishing),
                // whoever serves it. The service also says which tool hooks
                // are configured, so the node can say what each one sees.
                if let Some(handle) = ctx.capability::<HookServiceHandle>() {
                    let service = &handle.0;
                    let mut post = service.configured_hooks(HookPoint::AfterToolUse);
                    post.extend(service.configured_hooks(HookPoint::AfterToolFailure));
                    let binding = ToolHookBinding::new(
                        service.clone(),
                        step_view(
                            ctx,
                            "agent",
                            &config.label,
                            &config.kv,
                            permissions::effective(config.permissions, ctx),
                        ),
                        ctx.node.clone(),
                        ctx.firing,
                        ctx.attempt,
                    );
                    let hooks = AcpHooks::new(
                        binding,
                        service.configured_hooks(HookPoint::BeforeToolUse),
                        post,
                    );
                    client.with_hooks(Arc::new(hooks)).await;
                }
                let subagents = command.is_codex_acp();
                if let Err(error) = client
                    .open_session(ctx.env.workspace_path(), subagents)
                    .await
                {
                    client.terminate(ctx.env.grace()).await;
                    return Err(error.into());
                }
                Ok(Self::Acp(Box::new(client)))
            }
        }
    }
    /// One prompt turn. `deadline` is the node's `timeout`, which an ACP
    /// agent consumes itself (`TimeoutPolicy::HandlerManaged`, as Fabro hands
    /// its `timeout_ms` to the ACP turn): a turn that outlives it is
    /// terminated and asks for a retry (class `retry_requested`), as Fabro's
    /// timed-out turn is a retryable handler error. A native Pebble session
    /// ignores it; the driver's interview-aware timer owns that deadline.
    pub(crate) async fn prompt(
        &mut self,
        prompt: Prompt,
        control: &mut mpsc::Receiver<Control>,
        grace: Duration,
        deadline: Option<Duration>,
    ) -> Result<String, AgentError> {
        match self {
            Self::Acp(client) => {
                let text = prompt.assembled();
                let turn = client.prompt(&text, control, grace);
                let result = match deadline {
                    Some(deadline) => timeout(deadline, turn).await.ok(),
                    None => Some(turn.await),
                };
                let Some(result) = result else {
                    client.terminate(grace).await;
                    return Err(AgentError::failed(
                        RETRY_REQUESTED_CLASS,
                        format!(
                            "the agent turn timed out after {}ms",
                            deadline
                                .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
                        ),
                    ));
                };
                result.map(|turn| turn.text).map_err(Into::into)
            }
            Self::Pebble(session) => session.prompt(prompt.into_coding_input(), control).await,
        }
    }
    /// The native session's fallback plan at the route it reached, after
    /// any failover Pebble ran. An ACP session has no plan.
    pub(crate) fn plan(&self) -> Option<Plan> {
        match self {
            Self::Acp(_) => None,
            Self::Pebble(session) => Some(session.plan()),
        }
    }
    pub(crate) async fn shutdown(
        &mut self,
        reason: ShutdownReason,
        grace: Duration,
    ) -> Result<(), AgentError> {
        match self {
            Self::Acp(client) => {
                client.terminate(grace).await;
                Ok(())
            }
            Self::Pebble(session) => session.shutdown(reason).await,
        }
    }
    /// The native conversation, warm, for the next node on its thread. An
    /// ACP session has none.
    pub(crate) fn export(&self) -> Option<CodingAgentExport> {
        match self {
            Self::Acp(_) => None,
            Self::Pebble(session) => Some(session.export()),
        }
    }
    /// ACP does not report tracked native writes. An empty list is absence,
    /// not a claim that a stage made no filesystem changes.
    pub(crate) fn take_files_touched(&mut self) -> Vec<String> {
        match self {
            Self::Acp(_) => Vec::new(),
            Self::Pebble(session) => session.take_files_touched(),
        }
    }

    pub(crate) fn metrics(&self, turns: u64) -> BTreeMap<SmolStr, Value> {
        match self {
            Self::Acp(client) => client.metrics(turns),
            Self::Pebble(session) => session.metrics(),
        }
    }
}
