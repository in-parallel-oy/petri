//! `attractor/prompt`: Fabro's prompt node (`tab`), and a fan-in with a
//! `prompt`. One model call through the application's `lithos-llm` client,
//! with no agent tools and no coding-agent loop. Petri owns the prompt
//! assembly, the output contract, the repair turns and the result mapping;
//! `lithos-llm` owns the provider transport.
//!
//! Fabro's prompt handler sends the fidelity preamble and the node's prompt
//! as one user message, then validates the response against the node's
//! `output_schema`. A response that misses the contract gets a repair turn:
//! the failed response and the repair message are appended and the model is
//! asked again, up to `output_retries` times. The result writes
//! `last_stage`, `last_response` (the first 200 characters) and
//! `response.<node>`, then the routing fields or `output.<node>`.
//!
//! A prompted fan-in is the same stage with the ordered branch results in
//! its prompt: Fabro renders `parallel.results` into the preamble's context
//! section; here the results ride the fan-in's inputs and are rendered the
//! same way, with each branch's id, status and updates.
//!
//! The one-shot request runs on a fallback plan ([`crate::fallback`]): the
//! one admission froze on the node's config when the runtime had a catalog
//! ([`crate::admission`]), else the one the node builds. A provider-local
//! model error re-sends the same messages on the next configured route,
//! and the repair turns stay on that plan.
//!
//! The step emits two `StepEvent::Custom` payloads a host can map onto
//! Fabro's `stage.prompt` and `prompt.completed` events, each with a stable
//! `kind`: [`PROMPT_EVENT`] before the first model call carries the rendered
//! prompt, the model selector and the fan-in sources; [`COMPLETED_EVENT`]
//! after the last call carries the response text, the outcome, the usage
//! (tokens and, when every answer was priced, cost), the number of calls and
//! the duration.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use frontend_attractor::Policy;
use frontend_attractor::kinds::{PROMPT_KIND, StageOutcome};
use ir::{Control, LogStream, Metrics, Outcome, StepEvent, StepKindId, Value};
use lithos_llm::types::{Message, Request, Response, ResponseFormat, Role, Usage};
use lithos_llm::{Client, Error};
use pebble_coding_agent::{MemoryDiscovery, ProjectMemory};
use serde::Deserialize;
use serde_json::json;
use smol_str::SmolStr;
use steps::{Step, StepCtx};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::agent::AgentBackend;
use crate::agent::backend::AgentError;
use crate::blobs::{self, OutputStore};
use crate::contract::{Contract, Parsed, repair_message, validate};
use crate::fallback::{self, FrozenPlan, ModelFailure, StageRequest};
use crate::fidelity::{self, Fidelity, Incoming, Preamble, StageInfo, ThreadConfig};
use crate::outcome::{ExplicitRoutes, Stage};
use crate::parallel::{BRANCH_COUNT_KEY, RESULTS_KEY, parallel_complete, strip_placeholders};
use crate::pebble::environment::PebbleEnvironment;
use crate::pebble::{PebbleClient, profile_of};
use crate::stage::{self, RunInfo};

pub const KIND: StepKindId = PROMPT_KIND;

/// How much of the response `last_response` keeps, Fabro's truncation.
const LAST_RESPONSE_CHARS: usize = 200;

/// The `kind` of the `StepEvent::Custom` payload emitted before the first
/// model call: `{ kind, node, firing, attempt, model, prompt, sources }`.
pub const PROMPT_EVENT: &str = "attractor.prompt";

/// The `kind` of the `StepEvent::Custom` payload emitted after the last
/// model call: `{ kind, node, firing, attempt, model, outcome, response,
/// calls, repairs, usage, duration_ms }`; `usage` is lithos-llm's `Usage`.
pub const COMPLETED_EVENT: &str = "attractor.prompt.completed";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptConfig {
    /// A prompt node is API-only; the lowering leaves `backend` out unless a
    /// node names one, so the default here is the only one it can run on.
    #[serde(default = "api_backend")]
    pub backend:              AgentBackend,
    pub label:                String,
    pub node:                 String,
    #[serde(default)]
    pub kind:                 Option<String>,
    #[serde(default)]
    pub goal:                 String,
    #[serde(default)]
    pub prompt:               String,
    #[serde(default)]
    pub model:                Option<String>,
    #[serde(default)]
    pub provider:             Option<String>,
    #[serde(default)]
    pub reasoning_effort:     Option<String>,
    #[serde(default)]
    pub fidelity:             Option<String>,
    #[serde(default)]
    pub default_fidelity:     Option<String>,
    #[serde(default)]
    pub thread_id:            Option<String>,
    #[serde(default)]
    pub default_thread:       Option<String>,
    #[serde(default)]
    pub classes:              Vec<String>,
    #[serde(default)]
    pub incoming:             Value,
    #[serde(default)]
    pub branch:               bool,
    /// Fabro's `project_memory`: the working directory's project documents,
    /// loaded by Pebble's loader within its budget, become the system prompt.
    /// Default true.
    #[serde(default = "default_true")]
    pub project_memory:       bool,
    #[serde(default)]
    pub speed:                Option<String>,
    #[serde(default)]
    pub max_tokens:           Option<i64>,
    /// `[run.model.fallbacks]` as lowered. Absent once admission resolved
    /// them.
    #[serde(default)]
    pub fallbacks:            BTreeMap<String, Vec<String>>,
    /// The plan admission froze for this node (`crate::admission`). Absent
    /// when the graph was admitted without a catalog; the node then builds
    /// its own.
    #[serde(default)]
    pub plan:                 Option<FrozenPlan>,
    #[serde(default)]
    pub stages:               Value,
    #[serde(default)]
    pub output_schema:        Option<Value>,
    #[serde(default = "default_output_retries")]
    pub output_retries:       u64,
    #[serde(default)]
    pub on_failure:           Option<Policy>,
    #[serde(default)]
    pub on_retries_exhausted: Option<Policy>,
    #[serde(default, rename = "routes")]
    pub explicit_routes:      Option<ExplicitRoutes>,
    #[serde(default)]
    pub timeout_ms:           Option<u64>,
    #[serde(default)]
    pub kv:                   Value,
    /// A `for_each` branch's item, rendered as fenced untrusted data by the
    /// branch step. Appended after the prompt, as Fabro appends it.
    #[serde(default)]
    pub item_data:            Option<String>,
    #[serde(default)]
    pub nodes:                Value,
    /// A prompted fan-in: the branch source node ids, in edge order.
    #[serde(default)]
    pub sources:              Vec<String>,
    /// A prompted fan-in: the ordered branch results the barrier collected.
    #[serde(default)]
    pub branch_results:       Value,
    /// A prompted fan-in: the parallel node it joins, for `parallel_complete`.
    #[serde(default)]
    pub fork:                 Option<String>,
    /// The joined parallel node's agent permissions, for its
    /// `parallel_complete` agent hooks.
    #[serde(default)]
    pub fork_permissions:     crate::AgentPermissions,
}

fn default_output_retries() -> u64 {
    2
}

fn api_backend() -> AgentBackend {
    AgentBackend::Api
}

fn default_true() -> bool {
    true
}

pub struct PromptStep;

impl PromptConfig {
    /// The model selector `lithos-llm` resolves: `provider/model` when a
    /// provider qualifies the model.
    fn selector(&self) -> Result<String, String> {
        let model = self
            .model
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "prompt node `{}` names no model: set `model`, the graph's `default_model`, \
                     `[run.model] name` in workflow.toml, or `--model`/`--provider` at launch",
                    self.node
                )
            })?;
        Ok(self.provider.as_ref().map_or_else(
            || model.to_owned(),
            |provider| {
                if model.starts_with(&format!("{provider}/")) {
                    model.to_owned()
                } else {
                    format!("{provider}/{model}")
                }
            },
        ))
    }

    /// Fabro's resolution of this node's fidelity. A prompt node never
    /// reuses a thread, so `full` means no preamble and nothing more.
    fn fidelity(&self) -> Fidelity {
        let config = ThreadConfig {
            fidelity:         self.fidelity.clone(),
            default_fidelity: self.default_fidelity.clone(),
            thread_id:        self.thread_id.clone(),
            default_thread:   self.default_thread.clone(),
            classes:          self.classes.clone(),
        };
        fidelity::resolve(
            &config,
            &Incoming::from_value(&self.incoming),
            self.branch,
            false,
        )
        .fidelity
    }

    /// The one user message: the fidelity preamble, the branch results for
    /// a fan-in, the node's prompt, and the contract.
    fn assemble(&self, contract: &Contract, results: &Value, run_id: &str) -> String {
        let stages: Vec<StageInfo> = fidelity::stages(&self.stages);
        let preamble = Preamble {
            goal: &self.goal,
            run_id,
            stages: &stages,
            nodes: &self.nodes,
            kv: &self.kv,
        };
        let mut body = String::new();
        if !self.sources.is_empty() {
            body.push_str(&branch_results(&self.sources, results));
        }
        body.push_str(&self.prompt);
        if let Some(item) = self.item_data.as_deref().filter(|item| !item.is_empty()) {
            body.push_str("\n\n");
            body.push_str(item);
        }
        let mut out = preamble.prompt(self.fidelity(), &body);
        out.push_str(&contract.prompt_suffix());
        out
    }

    fn response_format(contract: &Contract) -> Option<ResponseFormat> {
        match contract {
            Contract::None => None,
            Contract::Routing => Some(ResponseFormat::JsonObject),
            Contract::Schema(_, schema) => Some(ResponseFormat::JsonSchema {
                name:   "fabro_output".to_string(),
                schema: schema.clone(),
            }),
        }
    }
}

#[async_trait::async_trait]
impl Step for PromptStep {
    const NAME: &'static str = "attractor/prompt";
    type Config = PromptConfig;

    async fn run(&self, mut config: PromptConfig, mut ctx: StepCtx) -> Outcome {
        if let Some(store) = ctx.capability::<OutputStore>() {
            blobs::restore_fabro_view(&mut config.kv, &mut config.nodes, store.0.as_ref()).await;
        }
        let on_failure = config.on_failure;
        let on_retries_exhausted = config.on_retries_exhausted;
        let final_attempt = ctx.is_final_attempt();
        let routes = config.explicit_routes.clone();
        let kv = config.kv.clone();
        let fail = |reason: String, class: &str| {
            Stage::failed(reason, class, on_failure)
                .with_routing(routes.clone(), kv.clone())
                .with_retries(on_retries_exhausted, final_attempt)
                .into_outcome(&config.node)
        };
        if config.backend == AgentBackend::Acp {
            return fail(
                "backend=\"acp\" is only valid on agent nodes; prompt nodes are API-only".into(),
                "bad_config",
            );
        }
        let Some(client) = ctx.capability::<PebbleClient>() else {
            return fail(
                "a prompt node requires the application's model client (PebbleClient capability)"
                    .into(),
                "pebble_unconfigured",
            );
        };
        let contract = match Contract::from_config(config.output_schema.as_ref()) {
            Ok(contract) => contract,
            Err(message) => return fail(message, "bad_config"),
        };
        // A provider with no model (`--provider` alone, or a bare
        // `default_provider`) runs the provider's default model.
        if let Err(message) = fallback::fill_provider_default(
            &client.0,
            &mut config.model,
            config.provider.as_deref(),
        ) {
            return fail(
                format!("prompt node `{}`: {message}", config.node),
                "bad_config",
            );
        }
        let selector = match config.selector() {
            Ok(selector) => selector,
            Err(message) => return fail(message, "bad_config"),
        };
        let store = ctx.capability::<OutputStore>();
        let mut results = match &store {
            Some(store) => blobs::hydrate(config.branch_results.clone(), store.0.as_ref()).await,
            None => config.branch_results.clone(),
        };
        strip_placeholders(&mut results);
        let started = Instant::now();
        stage::record(&ctx);
        if let Some(fork) = &config.fork {
            parallel_complete(&ctx, fork, config.fork_permissions, &config.label).await;
        }
        let run_id = ctx
            .capability::<RunInfo>()
            .map(|run| run.run_id.clone())
            .unwrap_or_default();
        let prompt = config.assemble(&contract, &results, &run_id);
        let request = StageRequest {
            node:             &config.node,
            provider:         config.provider.as_deref(),
            model:            config.model.as_deref(),
            reasoning_effort: config.reasoning_effort.as_deref(),
            speed:            config.speed.as_deref(),
            fallbacks:        &config.fallbacks,
            plan:             config.plan.as_ref(),
        };
        let frozen = match fallback::plan_for_stage(&mut ctx, &client.0, &request).await {
            Ok(frozen) => frozen,
            Err(AgentError::Failed { class, message }) => return fail(message, &class),
            Err(AgentError::Cancelled) => return Outcome::cancelled(),
            Err(AgentError::Model(failure)) => return fail(failure.to_string(), &failure.class()),
        };
        let mut plan = frozen.plan();
        fallback::Stage::of(&ctx).plan(&plan, &frozen.notices).await;
        // Fabro's prompt handler: with `project_memory` on, the working
        // directory's instruction files for the model's profile become the
        // system prompt. Petri selects the paths; Pebble's loader, the one a
        // native session runs, reads them within its budget.
        let system_prompt = if config.project_memory {
            let reader = PebbleEnvironment::for_files(ctx.env.clone());
            let never = CancellationToken::new();
            // The working directory alone, with the profile's filenames:
            // Pebble names the files and loads them; the discovery and the
            // loader err only when their token is cancelled, and this one
            // never is. A model with no profile reads the shared file.
            let paths = match profile_of(&client.0, &selector) {
                Some(profile) => MemoryDiscovery::working_directory()
                    .resolve(&reader, profile, &never)
                    .await
                    .unwrap_or_default(),
                None => vec!["AGENTS.md".to_owned()],
            };
            ProjectMemory::load(&reader, &paths, &never)
                .await
                .ok()
                .filter(|memory| !memory.is_empty())
                .map(|memory| memory.text())
        } else {
            None
        };
        let _ = ctx
            .logs
            .send(StepEvent::Custom(json!({
                "kind": PROMPT_EVENT,
                "node": config.node,
                "firing": ctx.firing,
                "attempt": ctx.attempt,
                "model": selector,
                "prompt": prompt,
                "sources": config.sources,
            })))
            .await;
        let completed =
            |outcome: &str, response: Option<&str>, calls: u64, repairs: u64, usage: &Usage| {
                StepEvent::Custom(json!({
                    "kind": COMPLETED_EVENT,
                    "node": config.node,
                    "firing": ctx.firing,
                    "attempt": ctx.attempt,
                    "model": selector,
                    "outcome": outcome,
                    "response": response,
                    "calls": calls,
                    "repairs": repairs,
                    "usage": usage,
                    "duration_ms": u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                }))
            };
        let mut messages = Vec::new();
        if let Some(system) = &system_prompt {
            messages.push(Message::text(Role::System, system.clone()));
        }
        messages.push(Message::text(Role::User, prompt));
        let mut usage = Usage::default();
        let mut turns = 0_u64;
        let mut repairs = 0_u64;
        let (parsed, text) = loop {
            let route = plan.current().clone();
            let selector = route.selector();
            let mut request = Request::builder().model(&selector);
            for message in &messages {
                request = request.message(message.clone());
            }
            if let Some(effort) = route.reasoning_effort {
                request = request.reasoning_effort(effort);
            }
            if let Some(speed) = route.speed {
                request = request.speed(speed);
            }
            if let Some(tokens) = config.max_tokens.and_then(|t| u32::try_from(t).ok()) {
                request = request.max_output_tokens(tokens);
            }
            // Fabro asks the provider for a JSON response shape when the node
            // has a contract. A model whose catalog row does not offer that
            // shape still answers; the contract is then enforced by the
            // validation below alone, as it is for an agent node.
            if let Some(format) = PromptConfig::response_format(&contract)
                && supports_format(&client.0, &selector, &format)
            {
                request = request.response_format(format);
            }
            if let Some(ms) = config.timeout_ms {
                request = request.timeout(Duration::from_millis(ms));
            }
            let request = match request.build() {
                Ok(request) => request,
                Err(error) => return fail(error.to_string(), "bad_config"),
            };
            let response = match complete(&client.0, request, &mut ctx.control).await {
                Ok(Some(response)) => response,
                Ok(None) => return Outcome::cancelled(),
                Err(error) => {
                    let failure = ModelFailure::from_error(&error);
                    if failure.eligible && plan.has_next() {
                        // The same messages, on the next route; the repair
                        // history so far travels with them. A prompt node
                        // runs no session, so the move is reported here, on
                        // the node's stderr.
                        plan.advance();
                        ctx.log(
                            LogStream::Stderr,
                            format!(
                                "model fallback: {} failed ({}); continuing on {} (attempt {} of \
                                 the plan)",
                                plan.previous().selector(),
                                failure.kind,
                                plan.current().selector(),
                                plan.position()
                            ),
                        )
                        .await;
                        continue;
                    }
                    let _ = ctx
                        .logs
                        .send(completed("failed", None, turns, repairs, &usage))
                        .await;
                    let mut outcome = fail(failure.to_string(), &failure.class());
                    outcome.metrics = metrics(started, turns, &usage);
                    return outcome;
                }
            };
            turns += 1;
            usage = usage.saturating_add(response.usage_with_cost());
            let text = response.text();
            ctx.log(LogStream::Stdout, text.clone()).await;
            match validate(&contract, &text) {
                Ok(parsed) => break (parsed, text),
                Err(problem) if repairs < config.output_retries => {
                    repairs += 1;
                    ctx.log(
                        LogStream::Stderr,
                        format!(
                            "the response does not meet the output contract ({problem}); repair \
                             turn {repairs}"
                        ),
                    )
                    .await;
                    messages.push(Message::text(Role::Assistant, text));
                    messages.push(Message::text(Role::User, repair_message(&problem)));
                }
                Err(problem) => {
                    let _ = ctx
                        .logs
                        .send(completed("failed", Some(&text), turns, repairs, &usage))
                        .await;
                    let mut outcome = fail(
                        format!(
                            "output schema validation failed after {repairs} repair attempt(s): \
                             {problem}"
                        ),
                        "bad_output",
                    );
                    outcome.metrics = metrics(started, turns, &usage);
                    return outcome;
                }
            }
        };

        let mut stage = Stage::new(StageOutcome::Succeeded, on_failure)
            .with_routing(routes.clone(), kv.clone())
            .with_retries(on_retries_exhausted, final_attempt);
        stage.output.insert("text".into(), json!(text));
        stage.output.insert("turns".into(), json!(turns));
        stage.output.insert("model".into(), json!(selector));
        if !config.sources.is_empty() {
            stage.output.insert("sources".into(), json!(config.sources));
            stage.output.insert(
                "branch_count".into(),
                json!(results.as_array().map_or(0, Vec::len)),
            );
            // The prompted fan-in is the barrier too: it publishes the
            // results the plain fan-in would have.
            stage
                .context_updates
                .insert(SmolStr::new(RESULTS_KEY), results.clone());
            stage.context_updates.insert(
                SmolStr::new(BRANCH_COUNT_KEY),
                json!(results.as_array().map_or(0, Vec::len)),
            );
        }
        stage.context_updates.insert(
            SmolStr::new(format!("response.{}", config.node)),
            json!(text),
        );
        let excerpt: String = text.chars().take(LAST_RESPONSE_CHARS).collect();
        stage
            .context_updates
            .insert(SmolStr::new("last_response"), json!(excerpt));
        stage
            .context_updates
            .insert(SmolStr::new("last_stage"), json!(config.node));
        match parsed {
            Parsed::Directive(directive) => directive.apply_to(&mut stage),
            Parsed::Structured(value) => {
                stage.output.insert("structured".into(), value.clone());
                stage
                    .context_updates
                    .insert(SmolStr::new(format!("output.{}", config.node)), value);
            }
            Parsed::Plain => {}
        }
        if let Some(store) = &store {
            blobs::offload_updates(&mut stage.context_updates, store.0.as_ref()).await;
            // The joined list follows the plain fan-in's rule: above the
            // fan-out threshold it is published as a reference.
            if let Some(results) = stage.context_updates.get_mut(RESULTS_KEY) {
                blobs::offload_above(results, store.0.as_ref(), blobs::FAN_OUT_OFFLOAD_THRESHOLD)
                    .await;
            }
        }
        let _ = ctx
            .logs
            .send(completed(
                stage.outcome.as_str(),
                Some(&text),
                turns,
                repairs,
                &usage,
            ))
            .await;
        let mut outcome = stage.into_outcome(&config.node);
        outcome.metrics = metrics(started, turns, &usage);
        outcome
    }
}

/// Whether the catalog row the selector resolves to offers `format`. A
/// selector that does not resolve is left to the call, which reports why.
fn supports_format(client: &Client, selector: &str, format: &ResponseFormat) -> bool {
    let Ok(probe) = Request::builder().model(selector).user("probe").build() else {
        return true;
    };
    match client.resolve_route(&probe) {
        Ok(route) => !route
            .model()
            .capabilities()
            .response_format(format)
            .is_unsupported(),
        Err(_) => true,
    }
}

/// One model call, cancel-aware: a `Cancel` or `Kill` on the control channel
/// abandons the request (`None`); a `Deliver` is ignored, a prompt has no
/// session to steer.
async fn complete(
    client: &Client,
    request: Request,
    control: &mut mpsc::Receiver<Control>,
) -> Result<Option<Response>, Error> {
    let call = client.complete(request);
    tokio::pin!(call);
    let mut closed = false;
    loop {
        tokio::select! {
            result = &mut call => return result.map(Some),
            message = control.recv(), if !closed => match message {
                Some(Control::Cancel | Control::Kill) => return Ok(None),
                None => closed = true,
                Some(_) => {}
            },
        }
    }
}

fn metrics(started: Instant, turns: u64, usage: &Usage) -> Metrics {
    Metrics {
        duration_ms: Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
        custom: [
            (SmolStr::new("prompt.calls"), json!(turns)),
            (SmolStr::new("prompt.usage"), json!(usage)),
        ]
        .into_iter()
        .collect(),
        ..Metrics::default()
    }
}

/// The branch results a prompted fan-in joins, one section per branch in
/// branch order: the source node, its status, and its result. A result that
/// still holds an output reference is named as such; the step hydrates
/// references before rendering when the store is available.
fn branch_results(sources: &[String], results: &Value) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Parallel branch results ({} branches: {}):",
        sources.len(),
        sources.join(", ")
    );
    match results.as_array() {
        Some(items) if !items.is_empty() => {
            for (position, item) in items.iter().enumerate() {
                let id = item.get("id").and_then(Value::as_str).unwrap_or("?");
                let status = item.get("status").and_then(Value::as_str).unwrap_or("?");
                let index = item
                    .get("index")
                    .and_then(Value::as_u64)
                    .unwrap_or(position as u64);
                let _ = writeln!(out, "\n### Branch {index}: {id} ({status})");
                let body = item
                    .get("context_updates")
                    .or_else(|| item.get("output"))
                    .cloned()
                    .unwrap_or(Value::Null);
                if blobs::holds_ref(&body) {
                    out.push_str("(result stored as an output reference)\n");
                }
                out.push_str(&serde_json::to_string_pretty(&body).unwrap_or_default());
                out.push('\n');
            }
        }
        _ => out.push_str("(no branch results)\n"),
    }
    out.push('\n');
    out
}
