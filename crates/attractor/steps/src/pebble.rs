//! Native Pebble sessions. The embedding application owns the model client.
//!
//! A session binds Petri's services for one node: the event sink attributed
//! to the node's firing and attempt, the question bridge, the redactor, the
//! tool-hook middleware, and the model controls. A node that continues a
//! retained thread resumes Pebble's warm export with a fresh set of those
//! bindings, so events, questions and hooks of the new stage are attributed
//! to the new stage; the predecessor was shut down after it exported. Per
//! stage metrics start at zero for each node.
//!
//! A session opens on a plan's route ([`Resume::Fresh`]) or continues a
//! retained export on the export's route with the plan the thread carries
//! ([`Resume::Export`]). Either way the plan's remaining routes are named to
//! Pebble as its fallback routes; Pebble moves the conversation when a model
//! error qualifies and reports the move on its own event stream, and the
//! session reads the route reached back for the thread a later node
//! continues. A prompt's model error stays typed ([`AgentError::Model`]) so
//! the stage reports its class.
//!
//! Pebble's `CodingAgentEvent` stream, recorded here as the `pebble`
//! envelope (the public `agent_activity`), is the contract for everything
//! Pebble knows: routes, MCP servers and their calls, tools, usage. The sink
//! restates none of it as a Petri event; it emits a `StepEvent::Custom` only
//! for a fact Pebble cannot know (a server never named to it,
//! [`mcp::UNAVAILABLE_EVENT`]; the skill directories' conventions) or puts
//! on no event of its own (the tools a session has, [`tools::EVENT`], once
//! per session), and puts two of Pebble's facts on the node's stderr for
//! the terminal: a fallback move and a server that did not start.
//!
//! Text a host delivers to the node (`Control::Deliver`) that is not an
//! answer to the agent's question is a follow-up: it runs as its own user
//! turn once the current answer is reached. Deliveries ride Pebble's
//! steering bus, one per node run: text that arrives before the session is
//! built waits on the bus and reaches the session when it attaches, in the
//! same mode.
//!
//! A host's interrupt (`Interrupt` under `$interrupt`) stops the current
//! model turn through the same bus: the request in flight and the tool calls
//! it is running are cancelled, Pebble publishes `RoundInterrupted`, and the
//! session stays open. With `steer` text the interrupt and the text land in
//! one step and the text opens the next turn. Without it the prompt parks at
//! its next turn boundary, and the next text the host delivers is sent as
//! steering, not as a follow-up, so it is what wakes the prompt. The node
//! reports the stop as [`INTERRUPTED_EVENT`] beside Pebble's own event.

pub mod environment;
pub mod mcp;
pub mod questions;
pub mod tools;

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use environment::{InheritedEnv, PebbleEnvironment, ScopePortRoutes, elapsed_ms};
use execution::hooks::HookServiceHandle;
use executor::Masker;
use ir::{Attempt, Control, FiringId, LogStream, ScopeId, StepEvent, Value};
use lithos_llm::Client;
use lithos_llm::catalog::Metadata;
use lithos_llm::types::{ErrorKind, ReasoningEffort, Request, Speed, Usage};
use pebble_coding_agent::events::{
    AgentProfileKind, CodingAgentEvent, CodingEvent, EventSink, EventSinkError,
};
use pebble_coding_agent::extensions::Redactor;
use pebble_coding_agent::steering::{DroppedSteer, SteeringBus};
use pebble_coding_agent::{
    CodingAgent, CodingAgentBuilder, CodingAgentExport, CodingAgentOptions, CodingInput,
    MemoryDiscovery, PromptReport, ShutdownReason, SteeringMessage,
};
use questions::AgentQuestions;
use serde_json::json;
use smol_str::SmolStr;
use steps::{Interrupt, ProgressError, ProgressSender, SECRET_UNAVAILABLE_CLASS, Steer, StepCtx};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::acp::resolve_env;
use crate::agent::backend::AgentError;
use crate::agent::{AgentConfig, INTERRUPTED_EVENT};
use crate::compaction::{self, CompactionPolicyHandle};
use crate::fallback::{self, Disposition, Plan};
use crate::hooks::tools::ToolHooks;
use crate::hooks::{self};
use crate::subagents::{self, Ledger};
use crate::{host_tools, permissions, route_usage, skills};

/// Host capability supplied by applications embedding the native backend.
/// Construct the client with the application's catalog, credentials, and
/// retry policy. The capability owns the client on Petri's host.
#[derive(Clone)]
pub struct PebbleClient(pub Client);

/// Where a native session's conversation comes from. Either carries the
/// node's fallback plan at the route reached: Pebble runs the routes after
/// it.
pub(crate) enum Resume {
    /// A new conversation on the plan's current route.
    Fresh(Plan),
    /// A retained thread's warm export, on the export's own route, with the
    /// plan the thread carries.
    Export {
        export: Box<CodingAgentExport>,
        plan:   Plan,
    },
}

pub(crate) struct NativeSession {
    agent:           CodingAgent,
    /// The node's sink, kept for the node's name the bus keys on.
    events:          Arc<PetriEvents>,
    /// The node's fallback plan as the session was opened; the route
    /// reached is read off the agent's remaining routes.
    plan:            Plan,
    questions:       Arc<AgentQuestions>,
    /// The node run's steering bus, with this session attached under the
    /// node's name: every delivered follow-up goes through it.
    steering:        SteeringBus<SmolStr>,
    /// What the session's own compactions cost, folded by the sink.
    compaction:      Arc<compaction::Accounting>,
    /// What the session spent on each route, folded by the sink.
    route_usage:     Arc<route_usage::Accounting>,
    cancel:          CancellationToken,
    kill:            CancellationToken,
    _cancel_on_drop: DropGuard,
    /// What the session's children spent and did, for the metrics.
    subagents:       Arc<Ledger>,
    /// What the session's settled prompts used: their tokens, and their
    /// cost when every answer that used tokens was priced.
    usage:           Usage,
    inference:       Duration,
    tool:            Duration,
    prompts:         u64,
}

/// The Pebble profile the model selector resolves to, from the catalog's
/// shared `metadata.agent.profile` on the model then the provider: the rule
/// Pebble applies when it builds a session. A prompt node, which builds no
/// session, asks here for the profile whose instruction files it reads.
pub fn profile_of(client: &Client, selector: &str) -> Option<AgentProfileKind> {
    #[derive(serde::Deserialize, Default)]
    struct AgentMetadata {
        profile: Option<String>,
    }
    let probe = Request::builder()
        .model(selector)
        .user("probe")
        .build()
        .ok()?;
    let route = client.resolve_route(&probe).ok()?;
    let read = |metadata: &Metadata| {
        metadata
            .namespace::<AgentMetadata>("agent")
            .ok()
            .flatten()
            .and_then(|m| m.profile)
    };
    let named = read(route.model().metadata()).or_else(|| read(route.provider().metadata()))?;
    AgentProfileKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == named)
}

/// Fabro's `speed` to lithos-llm's: `fast` asks for the fast tier,
/// `standard` for the balanced one.
pub fn speed_of(text: &str) -> Option<Speed> {
    match text {
        "fast" => Some(Speed::Fast),
        "standard" => Some(Speed::Balanced),
        _ => None,
    }
}

impl NativeSession {
    pub(crate) async fn open(
        config: &AgentConfig,
        ctx: &mut StepCtx,
        resume: Resume,
    ) -> Result<Self, AgentError> {
        let client = ctx.capability::<PebbleClient>().ok_or_else(|| {
            AgentError::failed(
                "pebble_unconfigured",
                "Native Pebble requires a PebbleClient capability",
            )
        })?;
        if config.acp.is_some() {
            return Err(AgentError::failed(
                "bad_config",
                "backend=api cannot use acp configuration",
            ));
        }
        let model = config
            .model
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                AgentError::failed("bad_config", "backend=api requires model or default_model")
            })?;
        let selector = config.provider.as_ref().map_or_else(
            || model.to_owned(),
            |provider| {
                if model.starts_with(&format!("{provider}/")) {
                    model.to_owned()
                } else {
                    format!("{provider}/{model}")
                }
            },
        );
        let mut reasoning = config
            .reasoning_effort
            .as_ref()
            .map(|value| serde_json::from_value::<ReasoningEffort>(json!(value)))
            .transpose()
            .map_err(|e| AgentError::failed("bad_config", e.to_string()))?;
        let mut speed = match config.speed.as_deref() {
            None => None,
            Some(text) => Some(speed_of(text).ok_or_else(|| {
                AgentError::failed(
                    "bad_config",
                    format!(
                        "Invalid speed \"{text}\" for node \"{}\"; expected one of: standard, \
                         fast",
                        config.node
                    ),
                )
            })?),
        };
        // A routed open runs on the route's model and controls; an export
        // keeps its own route and takes the node's controls.
        let (selector, plan) = match &resume {
            Resume::Fresh(plan) => {
                let route = plan.current();
                reasoning = route.reasoning_effort;
                speed = route.speed;
                (route.selector(), plan.clone())
            }
            Resume::Export { plan, .. } => (selector, plan.clone()),
        };
        let hook_service = ctx.capability::<HookServiceHandle>();
        let compaction_policy = ctx.capability::<CompactionPolicyHandle>();
        let level = permissions::effective(config.permissions, ctx);
        // Pebble's permission policy refuses every MCP and host tool below
        // `full`, so a restricted stage starts no MCP server and builds no
        // host tool it could never call.
        let full_access = level == crate::AgentPermissions::Full;
        // The host's own tools for this stage, built with the stage's
        // identity; Pebble registers them beside its tools, under the same
        // hooks and on the same event stream.
        let host_tools = if full_access {
            host_tools::for_node(ctx)
                .map_err(|failure| AgentError::failed(failure.class.as_str(), failure.message))?
        } else {
            Vec::new()
        };
        // Which host tools a child may inherit, read before the builder
        // takes them: the tools event names them for each child session.
        let inheritable_hosts = tools::inheritable_host_tools(&host_tools);
        let cancel = CancellationToken::new();
        let kill = CancellationToken::new();
        let guard = cancel.clone().drop_guard();
        // Fabro's skill directories, in its order then the workflow's own;
        // Pebble discovers and reports, the sink attributes.
        let (skill_discovery, skill_labels) = skills::for_node(config, ctx);
        let compaction_accounting = Arc::new(compaction::Accounting::default());
        let route_usage = Arc::new(route_usage::Accounting::default());
        let events = Arc::new(PetriEvents {
            sender:      ctx.logs.clone(),
            masker:      ctx.secrets.masker(),
            firing:      ctx.firing,
            attempt:     ctx.attempt,
            scope:       ctx.scope,
            node:        ctx.node.clone(),
            skills:      skill_labels,
            compaction:  compaction_accounting.clone(),
            route_usage: route_usage.clone(),
            tools:       Mutex::new(Vec::new()),
        });
        // The plan's remaining routes, for Pebble to fail over to in order,
        // each with its own controls; an export starts with none of its own.
        let fallback_routes = plan.pebble_routes(config.max_tokens);
        let ledger = Arc::new(Ledger::default());
        let sink = subagents::observe(events.clone(), ledger.clone());
        let redactor = Arc::new(PetriRedactor(ctx.secrets.masker()));
        // Agent questions ride the same progress and control channels a human
        // gate uses, so the host's one interviewer answers both.
        let questions = Arc::new(AgentQuestions::new(
            ctx.logs.clone(),
            ctx.node.clone(),
            ctx.firing,
        ));
        // The attempt's deadline is the driver's (`TimeoutPolicy::ExecutorEnforced`):
        // it counts active work only and cancels this session through `cancel`
        // when it expires. Pebble's own wall-clock timer stays unset, so it
        // cannot expire during an excluded interview wait.
        let env = ctx.env.clone();
        // A tool shell sees the workflow's secrets beneath its own env, as
        // Fabro's did; MCP servers and port routes use the scope as it is.
        let inherited = resolve_env(&config.env, ctx.secrets.as_ref())
            .map_err(|e| AgentError::failed(SECRET_UNAVAILABLE_CLASS.as_str(), e))?;
        let shell_env = executor::layer_exec(env.clone(), Arc::new(InheritedEnv::new(inherited)));
        let provider = questions.clone();
        let tool_hooks = hook_service.map(|handle| {
            let view = hooks::step_view(ctx, "agent", &config.label, &config.kv, level);
            let binding = hooks::ToolHookBinding::new(
                handle.0.clone(),
                view,
                ctx.node.clone(),
                ctx.firing,
                ctx.attempt,
            );
            Arc::new(ToolHooks::new(binding, ctx.logs.clone()))
        });
        let mcps = if full_access {
            config.mcps.clone()
        } else {
            Vec::new()
        };
        let secrets = ctx.secrets.clone();
        let session_events = events.clone();
        let permissions = permissions::pebble(level);
        let build = async {
            let environment = PebbleEnvironment::prepare(shell_env, cancel.clone(), kill.clone())
                .await
                .map_err(|e| AgentError::failed("pebble_environment", e.to_string()))?;
            let environment = Arc::new(environment);
            // The node's MCP servers: Fabro's entries as Pebble's, their
            // secrets resolved here and nowhere else. Pebble starts them while
            // it builds the agent and reports each one on its stream; a
            // server whose secret the run cannot supply is never named to
            // Pebble, so it is reported here. A send that fails here means
            // the driver stopped taking this attempt's progress; the build's
            // own outcome reports that.
            let servers = mcp::pebble_servers(&mcps, env.as_ref(), secrets.as_ref());
            for (server, error) in &servers.unavailable {
                let _ = session_events.unavailable(server, error).await;
            }
            // Fabro's project documents for the model's profile, from the
            // Git root down to the working directory: Pebble names the
            // files, does the walk, and loads them.
            let options = CodingAgentOptions::default()
                .with_reasoning_effort(reasoning)
                .with_speed(speed)
                .with_max_tokens(config.max_tokens)
                .with_memory_discovery(MemoryDiscovery::from_git_root())
                .with_skill_discovery(skill_discovery);
            let options = compaction::options(options, &config.compaction);
            // A resumed export keeps its route and its conversation; the
            // builder binds this node's services either way.
            let mut builder: CodingAgentBuilder = match resume {
                Resume::Export { export, .. } => {
                    CodingAgent::resume_from_export(client.0.clone(), environment, *export)
                }
                Resume::Fresh(_) => {
                    CodingAgent::builder(client.0.clone(), environment).model(&selector)
                }
            };
            builder = builder
                .options(options)
                .fallback_routes(fallback_routes)
                .permission_level(permissions)
                .event_sink(sink)
                .redactor(redactor)
                .human_input(provider)
                .mcp_servers(servers.servers)
                .port_routes(Arc::new(ScopePortRoutes::new(env)))
                .tools(host_tools);
            builder = compaction::install(builder, compaction_policy);
            if let Some(middleware) = tool_hooks {
                builder = builder.tool_middleware(middleware);
            }
            builder = subagents::configure(builder, &config.subagents);
            // The chain, not the head alone: a refused event sink or a bad
            // model selector is the cause under Pebble's summary.
            builder
                .build()
                .await
                .map_err(|e| AgentError::failed("pebble_config", chain(&e)))
        };
        tokio::pin!(build);
        // Text delivered before the session exists waits on the bus, as a
        // follow-up, and reaches the session when it attaches below. An
        // interrupt finds no turn here: only the text it carries is kept.
        let steering = SteeringBus::new();
        let mut closed = false;
        let agent = loop {
            tokio::select! {
                result = &mut build => break match result { Err(_) if cancel.is_cancelled() => return Err(AgentError::Cancelled), other => other? },
                control = ctx.control.recv(), if !closed => {
                    match control {
                        Some(Control::Deliver(value)) if !cancel.is_cancelled() => {
                            if !questions.answer(&value) && let Some(text) = delivered_text(&value) { follow_up(&steering, text); }
                        },
                        Some(Control::Kill) => { kill.cancel(); cancel.cancel(); },
                        Some(Control::Cancel) => cancel.cancel(),
                        None => { closed = true; cancel.cancel(); },
                        _ => {},
                    }
                }
            }
        };
        questions.set_session(agent.snapshot().session_id());
        // The session's tools, once, now that Pebble has registered them
        // all; the sink keeps the list for the children Pebble builds from
        // it. A send that fails here means the driver stopped taking this
        // attempt's progress; the first prompt reports that.
        let session_tools = tools::of_session(agent.snapshot().tools(), &inheritable_hosts);
        let _ = events
            .tools(agent.snapshot().session_id(), &session_tools)
            .await;
        events.set_tools(session_tools);
        let mut session = Self {
            compaction: compaction_accounting,
            route_usage,
            agent,
            events,
            plan,
            questions,
            steering,
            cancel: cancel.clone(),
            kill: kill.clone(),
            _cancel_on_drop: guard,
            subagents: ledger,
            usage: Usage::default(),
            inference: Duration::ZERO,
            tool: Duration::ZERO,
            prompts: 0,
        };
        if cancel.is_cancelled() {
            session.shutdown(ShutdownReason::Cancelled).await?;
            return Err(AgentError::Cancelled);
        }
        // The bus keys the attachment by the node; the control handle
        // follows the agent through any failover Pebble runs, so one
        // attachment serves the node run. Attaching drains what waited.
        let key = ctx.node.clone();
        if let Err(error) = session.steering.attach(
            key.clone(),
            session.session_id(),
            Arc::new(session.agent.control_handle()),
        ) {
            session.shutdown(ShutdownReason::Error).await?;
            return Err(AgentError::failed("pebble_config", error.to_string()));
        }
        let drained = session.steering.drain_pending_into(&key);
        if !drained.dropped.is_empty() {
            tracing::warn!(
                dropped = drained.dropped.len(),
                "follow-ups delivered before the session was built were dropped"
            );
        }
        Ok(session)
    }

    /// One prompt turn. A model error comes back typed as
    /// [`AgentError::Model`]. Pebble runs the plan's remaining routes inside
    /// the turn and reports each move on its stream.
    pub(crate) async fn prompt(
        &mut self,
        input: CodingInput,
        control: &mut mpsc::Receiver<Control>,
    ) -> Result<String, AgentError> {
        let questions = self.questions.clone();
        let cancel = self.cancel.clone();
        let kill = self.kill.clone();
        let report = {
            let prompt = self.agent.prompt_with_cancellation(input, &cancel);
            tokio::pin!(prompt);
            let mut closed = false;
            // A plain interrupt parked the prompt: the next delivered text
            // is its next input and goes as steering.
            let mut awaiting_input = false;
            loop {
                tokio::select! {
                    biased;
                    message = control.recv(), if !closed => match message {
                        Some(Control::Deliver(value)) if !cancel.is_cancelled() => {
                            if !questions.answer(&value) { deliver(&self.steering, &mut awaiting_input, &value); }
                        },
                        Some(Control::Kill) => { kill.cancel(); cancel.cancel(); },
                        Some(Control::Cancel) => cancel.cancel(),
                        None => { closed = true; cancel.cancel(); },
                        _ => {},
                    },
                    report = &mut prompt => break report,
                }
            }
        };
        self.account(&report);
        if cancel.is_cancelled() {
            return Err(AgentError::Cancelled);
        }
        match report.result {
            Ok(output) => Ok(output.text.unwrap_or_default()),
            Err(error) => Err(match fallback::classify(&error) {
                Disposition::Cancelled => AgentError::Cancelled,
                Disposition::Model(failure) => AgentError::Model(failure),
                // A skill reference that does not expand keeps the skills
                // module's class and reason; any other agent error is what
                // the classifier said.
                Disposition::Other { class, message } => match &error {
                    pebble_coding_agent::Error::SkillExpansion(_) => {
                        AgentError::failed(skills::failure_class(&error), skills::describe(&error))
                    }
                    _ => AgentError::failed(class, message),
                },
            }),
        }
    }

    fn account(&mut self, report: &PromptReport) {
        self.prompts += 1;
        self.usage = self.usage.saturating_add(report.usage);
        self.inference = self.inference.saturating_add(report.timing.inference);
        self.tool = self.tool.saturating_add(report.timing.tool);
    }

    /// The fallback plan at the route reached: the routes Pebble has taken
    /// are the ones no longer among its remaining fallback routes.
    pub(crate) fn plan(&self) -> Plan {
        let mut plan = self.plan.clone();
        let taken = plan
            .remaining_routes()
            .len()
            .saturating_sub(self.agent.remaining_fallback_routes().len());
        for _ in 0..taken {
            plan.advance();
        }
        plan
    }

    pub(crate) fn session_id(&self) -> String {
        self.agent.id().to_owned()
    }

    pub(crate) fn metrics(&self) -> BTreeMap<SmolStr, Value> {
        let mut metrics = BTreeMap::from([
            ("pebble.prompts".into(), json!(self.prompts)),
            ("pebble.usage".into(), json!(self.usage)),
            (route_usage::METRIC.into(), self.route_usage.metrics()),
            (
                "pebble.inference_ms".into(),
                json!(elapsed_ms(self.inference)),
            ),
            ("pebble.tool_ms".into(), json!(elapsed_ms(self.tool))),
            (subagents::METRIC.into(), self.subagents.metrics()),
        ]);
        metrics.extend(self.compaction.metrics());
        metrics
    }

    /// The conversation, warm, for a later node on the same thread.
    pub(crate) fn export(&self) -> CodingAgentExport {
        self.agent.export()
    }

    pub(crate) async fn shutdown(&mut self, reason: ShutdownReason) -> Result<(), AgentError> {
        // The session leaves the bus first: a delivery that lands during
        // the shutdown waits there and is dropped with the bus, as the
        // agent could not run it.
        self.steering.detach(&self.events.node, &self.session_id());
        // Pebble closes the MCP servers it started with the agent, after its
        // last tool call and before this returns, so before the node returns
        // and the scope's environment is released.
        self.agent
            .shutdown(reason)
            .await
            .map(|_| ())
            .map_err(|e| AgentError::failed("pebble_shutdown", e.to_string()))
    }
}

/// An error and its source chain on one line, for the places that must
/// flatten a typed error into a recorded message.
fn chain(error: &dyn Error) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// The guidance a delivered value carries: a core [`Steer`], or the older
/// bare string and `{ "text": ... }` spellings.
fn steering_text(value: &Value) -> Option<String> {
    if let Some(steer) = Steer::from_value(value) {
        return Some(steer.text);
    }
    value
        .as_str()
        .or_else(|| value.get("text").and_then(Value::as_str))
        .map(str::to_owned)
}

/// The text a delivered value carries for the session, whatever the
/// control: a steer's guidance, or the next input an interrupt names.
fn delivered_text(value: &Value) -> Option<String> {
    match Interrupt::from_value(value) {
        Some(interrupt) => interrupt.steer,
        None => steering_text(value),
    }
}

/// Apply a delivered value that is not an answer while a prompt runs. An
/// interrupt stops the round through the bus: with text, the text replaces
/// the round in one step; without, the prompt parks and `awaiting_input` is
/// set so the next text wakes it as steering. Any other text is a follow-up.
fn deliver(bus: &SteeringBus<SmolStr>, awaiting_input: &mut bool, value: &Value) {
    if let Some(interrupt) = Interrupt::from_value(value) {
        if let Some(text) = interrupt.steer {
            let message: SteeringMessage = text.into();
            let done = bus.interrupt_then_steer(&message);
            *awaiting_input = false;
            tracing::info!(
                sessions = done.interrupted.len(),
                "model turn interrupted; the delivered text opens the next"
            );
            report_dropped(&done.dropped);
        } else {
            let done = bus.interrupt();
            *awaiting_input = true;
            tracing::info!(
                sessions = done.interrupted.len(),
                "model turn interrupted; the prompt waits for its next input"
            );
        }
    } else if let Some(text) = steering_text(value) {
        if *awaiting_input {
            let delivery = bus.steer(text.into());
            *awaiting_input = false;
            report_dropped(&delivery.dropped);
        } else {
            follow_up(bus, text);
        }
    }
}

/// Queue delivered text on the node run's bus as a follow-up: on the
/// attached session, to run as its own turn once the current answer is
/// reached, or on the bus itself while no session is attached yet. A full
/// queue evicts its oldest message; the bus reports what it dropped.
fn follow_up(bus: &SteeringBus<SmolStr>, text: String) {
    let delivery = bus.follow_up(text.into());
    report_dropped(&delivery.dropped);
}

fn report_dropped(dropped: &[DroppedSteer<SmolStr>]) {
    if !dropped.is_empty() {
        tracing::warn!(
            dropped = dropped.len(),
            "a steering queue was full; its oldest message was dropped"
        );
    }
}

struct PetriRedactor(Masker);
impl Redactor for PetriRedactor {
    fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        Cow::Owned(self.0.mask(text))
    }
}

/// Pebble's event sink over the attempt's progress channel. Each event is
/// sent acknowledged: `record` returns only after the driver appended the
/// `StepProgress` record and every durable store confirmed the write, so an
/// acknowledgement to Pebble means the event is in Petri's log, and a write
/// failure stops the prompt as Pebble's contract asks. Queueing alone would not
/// do: the driver's completion fence drains the channel before it records the
/// outcome, which orders the events ahead of the outcome, but a crash after an
/// unacknowledged send can lose the event. What a crash still repeats is the
/// attempt: an attempt whose finish never landed is re-dispatched on resume
/// and emits its events again, so a record may appear twice; Pebble's
/// `(stream_id, seq)` in the nested envelope is the idempotency key.
///
/// Beside the envelope, the sink emits Petri's own events only for what
/// Pebble cannot know or puts on no event: the conventions behind the skill
/// directories Pebble reports having searched, a server never named to
/// Pebble because its secret is unavailable ([`PetriEvents::unavailable`]),
/// the node and attempt a completed compaction of the session belongs to
/// ([`compaction::EVENT`], folded from Pebble's `CompactionCompleted` as it
/// is recorded), and the tools a session has ([`tools::EVENT`]: the node's
/// own session once it is built, each child session as its `SessionStarted`
/// is recorded). Two of Pebble's facts also go to the node's stderr, for
/// the terminal: a fallback move and a server that did not start
/// ([`stderr_line`]).
struct PetriEvents {
    sender:      ProgressSender,
    masker:      Masker,
    firing:      FiringId,
    attempt:     Attempt,
    scope:       ScopeId,
    node:        SmolStr,
    /// What names the skill directories Pebble reports having searched.
    skills:      skills::Labels,
    /// The session's own compactions, folded as Pebble reports them.
    compaction:  Arc<compaction::Accounting>,
    /// The session's own usage by route, folded as Pebble reports it.
    route_usage: Arc<route_usage::Accounting>,
    /// The node's session's tools, set once the agent is built; what each
    /// child session's list is derived from.
    tools:       Mutex<Vec<tools::Tool>>,
}

impl PetriEvents {
    /// The session's tool list, kept for the children built from it.
    fn set_tools(&self, list: Vec<tools::Tool>) {
        *self.tools.lock().unwrap_or_else(PoisonError::into_inner) = list;
    }

    /// The [`tools::EVENT`] payload for `session`, acknowledged.
    async fn tools(&self, session: &str, list: &[tools::Tool]) -> Result<(), ProgressError> {
        self.sender
            .send_acked(StepEvent::Custom(self.masker.mask_value(&json!({
                "kind": tools::EVENT,
                "node": self.node,
                "firing": self.firing,
                "attempt": self.attempt,
                "session": session,
                "tools": list,
            }))))
            .await
    }

    /// A child session Pebble just started: its tools are the ones it
    /// inherits from this node's session.
    async fn child_started(&self, event: &CodingAgentEvent) -> Result<(), EventSinkError> {
        if event.parent_session_id.is_none()
            || !matches!(event.event, CodingEvent::SessionStarted { .. })
        {
            return Ok(());
        }
        let inherited =
            tools::inherited(&self.tools.lock().unwrap_or_else(PoisonError::into_inner));
        self.tools(&event.session_id, &inherited)
            .await
            .map_err(|error| EventSinkError::new(error.to_string()).with_source(error))
    }

    /// A server Petri never named to Pebble because a secret its entry
    /// needs is unavailable: the line on the node's stderr, then
    /// [`mcp::UNAVAILABLE_EVENT`], both acknowledged.
    async fn unavailable(&self, server: &str, error: &str) -> Result<(), ProgressError> {
        self.sender
            .send_acked(StepEvent::Log {
                stream: LogStream::Stderr,
                line:   self
                    .masker
                    .mask(&format!("mcp server `{server}` failed to start: {error}")),
            })
            .await?;
        self.sender
            .send_acked(StepEvent::Custom(self.masker.mask_value(&json!({
                "kind": mcp::UNAVAILABLE_EVENT,
                "node": self.node,
                "firing": self.firing,
                "attempt": self.attempt,
                "server": server,
                "error": error,
            }))))
            .await
    }
}

/// What a person watching the node's stderr should read of `event`: a
/// fallback move, or a server that did not start. Both facts stay Pebble's
/// on the event stream; the line is for the terminal.
fn stderr_line(event: &CodingEvent) -> Option<String> {
    match event {
        CodingEvent::RouteFailover {
            from,
            to,
            attempt,
            error,
            ..
        } => Some(format!(
            "model fallback: {from} failed ({}); continuing on {to} (attempt {attempt} of the plan)",
            error.llm_kind.as_ref().map_or("error", ErrorKind::as_str)
        )),
        CodingEvent::McpServerFailed { server, error, .. } => {
            Some(format!("mcp server `{server}` failed to start: {error}"))
        }
        _ => None,
    }
}

#[async_trait::async_trait]
impl EventSink for PetriEvents {
    async fn record(&self, event: &CodingAgentEvent) -> Result<(), EventSinkError> {
        // Counted before anything is sent, so a send that fails cannot
        // leave the usage the session's report holds out of its breakdown.
        self.route_usage.observe(event);
        let value = serde_json::to_value(event)
            .map_err(|e| EventSinkError::new("Could not encode Pebble event").with_source(e))?;
        self.sender
            .send_acked(StepEvent::Custom(
                json!({"kind": "pebble", "firing": self.firing, "attempt": self.attempt, "scope": self.scope, "node": self.node, "event": self.masker.mask_value(&value)}),
            ))
            .await
            .map_err(|error| EventSinkError::new(error.to_string()).with_source(error))?;
        if let Some(line) = stderr_line(&event.event) {
            self.sender
                .send_acked(StepEvent::Log {
                    stream: LogStream::Stderr,
                    line:   self.masker.mask(&line),
                })
                .await
                .map_err(|error| EventSinkError::new(error.to_string()).with_source(error))?;
        }
        // The node's own turn was stopped by a host's interrupt: Pebble's
        // fact is the envelope above, this is the node's report of it. A
        // child session's interrupt is the child's.
        if matches!(event.event, CodingEvent::RoundInterrupted { .. })
            && event.parent_session_id.is_none()
        {
            self.sender
                .send_acked(StepEvent::Custom(json!({
                    "kind": INTERRUPTED_EVENT,
                    "node": self.node,
                    "firing": self.firing,
                    "attempt": self.attempt,
                    "backend": "api",
                    "session": event.session_id,
                })))
                .await
                .map_err(|error| EventSinkError::new(error.to_string()).with_source(error))?;
        }
        // A child's tools, right after the child's own start: the envelope
        // above is Pebble's record of the session, this is what it can call.
        self.child_started(event).await?;
        // The directories Pebble searched are Petri's record, with the
        // convention behind each; a skipped file or directory is Pebble's
        // report and the diagnostic is Petri's.
        let at = skills::Attribution {
            node:    self.node.clone(),
            firing:  self.firing,
            attempt: self.attempt,
        };
        if let Some(dirs) = skills::labeled(event, &self.skills) {
            skills::record_resolved(&self.sender, &at, self.scope, &dirs).await;
        }
        let skipped = skills::skipped(event);
        if !skipped.is_empty() {
            skills::report(&self.sender, &at, &skipped).await;
        }
        // The session's own compaction, as it completes: the envelope above
        // is Pebble's record of it, this is Petri's attribution.
        let at = compaction::Attribution {
            node:    self.node.clone(),
            firing:  self.firing,
            attempt: self.attempt,
        };
        self.compaction.observe(event, &self.sender, &at).await;
        Ok(())
    }
}
