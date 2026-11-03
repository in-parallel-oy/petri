//! The four hook executors: a command in the sandbox or on the host, an
//! HTTP POST, one model turn, an agent with the coding tools.
//!
//! Each returns [`Executed`]: a decision, a fail-open note (HTTP, prompt and
//! agent errors and timeouts), or a reason the hook could not run at all (a
//! sandbox hook with no environment, a model hook with no client). Command
//! hooks never fail open: Fabro turns their exit code into a decision, and a
//! timeout is exit code -1, a block.
//!
//! An agent hook owns work that outlives a dropped future: a tool process in
//! the sandbox and the agent's own tasks. That work runs on a task of its
//! own ([`AgentWork`]), so a timeout or a cancellation stops the tool and
//! joins the agent before the hook's result is returned, and a hook future
//! the driver dropped (a root kill aborts the awaiting callback) still leaves
//! an owner to finish that cleanup.
//!
//! What a hook's own model and tool work did is returned with its outcome
//! ([`Execution`]): the usage of a prompt hook's one request, and for an
//! agent hook every event its agent produced (recorded by a Petri sink) with
//! the prompt's usage, so the service can put them on the record under the
//! hook's own identity.

use std::collections::BTreeMap;
use std::mem;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use execution::hooks::HookUsage;
use executor::{ExecEnv, OutputMode, ProcessSpec, Sig, StdinMode};
use frontend_attractor::hooks::{
    DEFAULT_MAX_TOOL_ROUNDS, DEFAULT_MODEL, HookDefinition, HookKind, TlsMode,
};
use lithos_llm::types::{Message, Request, ResponseFormat, Role};
use pebble_coding_agent::events::{CodingAgentEvent, CodingEvent, EventSink, EventSinkError};
use pebble_coding_agent::{
    CodingAgent, CodingAgentOptions, Error as AgentError, PromptReport, ShutdownReason,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use super::{Context, Decision, IN_HOOK};
use crate::pebble::PebbleClient;
use crate::pebble::environment::PebbleEnvironment;
use crate::permissions;

/// Fabro's prompt for its hook evaluator.
const EVALUATOR_SYSTEM: &str = "You are a hook evaluator for a workflow engine. Given context \
                                about a workflow event, evaluate the condition.";

/// The reference's warning when an agent hook runs out of tool rounds: the
/// hook proceeds.
const ROUNDS_EXHAUSTED: &str = "agent hook exhausted max tool rounds, proceeding";

/// How long an agent hook's cleanup may take beyond the sandbox's grace once
/// the hook is out of time or cancelled. The environment gives a stopped tool
/// a TERM, the grace, then a KILL it waits five seconds for; the agent's loop
/// then commits the tool's result and its tasks join. The same bound covers
/// the agent's shutdown after a completed prompt.
const CLEANUP_MARGIN: Duration = Duration::from_secs(6);

/// The response a prompt or agent hook returns: `{"ok": bool, "reason"}`.
#[derive(Debug, Deserialize)]
struct Verdict {
    ok:     bool,
    #[serde(default)]
    reason: Option<String>,
}

/// How one hook ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Executed {
    Decided(Decision),
    FailedOpen(String),
    Unsupported(String),
}

/// The agent backend an agent hook runs on, as the record names it.
pub const AGENT_BACKEND: &str = "pebble";

/// How one hook ended, with what its own model and tool work cost and did.
#[derive(Clone, Debug)]
pub struct Execution {
    pub outcome:  Executed,
    /// Present when the hook made a model request, answered or not.
    pub usage:    Option<HookUsage>,
    /// Every event the hook's agent produced, in order: Pebble's
    /// `CodingAgentEvent` envelopes.
    pub activity: Vec<Value>,
}

impl Execution {
    /// An outcome with no model or tool work behind it.
    fn of(outcome: Executed) -> Self {
        Self {
            outcome,
            usage: None,
            activity: Vec::new(),
        }
    }
}

/// One `reqwest` client per TLS mode, built on first use.
#[derive(Default)]
pub struct HttpClients {
    clients: Mutex<BTreeMap<u8, reqwest::Client>>,
}

impl HttpClients {
    fn client(&self, tls: TlsMode) -> Result<reqwest::Client, reqwest::Error> {
        let key = match tls {
            TlsMode::Verify => 0,
            TlsMode::NoVerify => 1,
            TlsMode::Off => 2,
        };
        let mut clients = self.clients.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(client) = clients.get(&key) {
            return Ok(client.clone());
        }
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(tls != TlsMode::Verify)
            .build()?;
        clients.insert(key, client.clone());
        Ok(client)
    }
}

/// Run one hook.
pub async fn execute(
    hook: &HookDefinition,
    context: &Context,
    env: Option<&Arc<dyn ExecEnv>>,
    client: Option<&PebbleClient>,
    http: &HttpClients,
) -> Execution {
    match &hook.kind {
        HookKind::Command { command } => {
            Execution::of(command_hook(hook, command, context, env).await)
        }
        HookKind::Http { url, headers, tls } => {
            Execution::of(http_hook(hook, url, headers, *tls, context, http).await)
        }
        HookKind::Prompt { prompt, model } => {
            prompt_hook(hook, prompt, model.as_deref(), context, client).await
        }
        HookKind::Agent {
            prompt,
            model,
            max_tool_rounds,
        } => {
            agent_hook(
                hook,
                prompt,
                model.as_deref(),
                max_tool_rounds.unwrap_or(DEFAULT_MAX_TOOL_ROUNDS),
                context,
                env,
                client,
            )
            .await
        }
    }
}

/// Fabro's exit-code rule.
fn parse_decision(code: i32, stdout: &str) -> Decision {
    let json = serde_json::from_str::<Decision>(stdout.trim()).ok();
    match (code, json) {
        (0 | 2, Some(decision)) => decision,
        (0, None) => Decision::Proceed,
        (code, _) => Decision::Block {
            reason: Some(format!("hook exited with code {code}")),
        },
    }
}

fn env_vars(context: &Context) -> BTreeMap<String, String> {
    let mut vars = BTreeMap::new();
    vars.insert("FABRO_EVENT".into(), context.event.as_str().to_owned());
    vars.insert("FABRO_RUN_ID".into(), context.run_id.clone());
    vars.insert("FABRO_WORKFLOW".into(), context.workflow_name.clone());
    if let Some(node) = &context.node_id {
        vars.insert("FABRO_NODE_ID".into(), node.clone());
    }
    vars
}

async fn command_hook(
    hook: &HookDefinition,
    command: &str,
    context: &Context,
    env: Option<&Arc<dyn ExecEnv>>,
) -> Executed {
    let payload = serde_json::to_vec(context).unwrap_or_default();
    let vars = env_vars(context);
    if hook.runs_in_sandbox() {
        let Some(env) = env else {
            return Executed::Unsupported(
                "the hook runs in the sandbox, and no sandbox environment is available at this \
                 point (set `sandbox = false` to run it on the host)"
                    .into(),
            );
        };
        return sandbox_command(hook, command, &payload, vars, env.as_ref()).await;
    }
    host_command(hook, command, &payload, vars, env.map(Arc::as_ref)).await
}

/// In the sandbox: `bash -c`, the context on stdin and in a file the command
/// removes when it ends, so `FABRO_HOOK_CONTEXT` works as Fabro's scripts
/// expect and the workspace is left as it was.
async fn sandbox_command(
    hook: &HookDefinition,
    command: &str,
    payload: &[u8],
    mut vars: BTreeMap<String, String>,
    env: &dyn ExecEnv,
) -> Executed {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let relative = format!(".fabro-hook-context-{nanos}.json");
    let context_path = if env.write_file(Path::new(&relative), payload).await.is_ok() {
        let absolute = format!("{}/{relative}", env.workspace_path().trim_end_matches('/'));
        vars.insert("FABRO_HOOK_CONTEXT".into(), absolute.clone());
        Some(absolute)
    } else {
        None
    };
    let script = match &context_path {
        Some(path) => format!(
            "{command}\n__fabro_status=$?\nrm -f -- '{}'\nexit $__fabro_status",
            path.replace('\'', "'\\''")
        ),
        None => command.to_owned(),
    };
    let spec = ProcessSpec::new("bash", &["-c", &script])
        .with_output(OutputMode::Bytes)
        .with_stdin(StdinMode::Piped)
        .with_env(
            vars.iter()
                .map(|(k, v)| (k.as_str().into(), v.as_str().into()))
                .collect(),
        );
    let mut handle = match env.spawn(spec).await {
        Ok(handle) => handle,
        Err(error) => {
            return Executed::Decided(Decision::Block {
                reason: Some(format!("sandbox exec failed: {error}")),
            });
        }
    };
    if let Some(mut stdin) = handle.stdin() {
        let bytes = payload.to_vec();
        tokio::spawn(async move {
            let _ = stdin.write_all(&bytes).await;
            let _ = stdin.shutdown().await;
        });
    }
    let Some(mut bytes) = handle.bytes() else {
        let _ = handle.signal(Sig::Kill).await;
        let _ = handle.wait().await;
        return Executed::Decided(Decision::Block {
            reason: Some("sandbox exec failed: no output stream".into()),
        });
    };
    let drain = tokio::spawn(async move {
        let mut out = Vec::new();
        while let Some(chunk) = bytes.recv().await {
            if chunk.stream == ir::LogStream::Stdout {
                out.extend(chunk.bytes);
            }
        }
        out
    });
    let status = match timeout(hook.timeout(), handle.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(error)) => {
            return Executed::Decided(Decision::Block {
                reason: Some(format!("sandbox exec failed: {error}")),
            });
        }
        Err(_) => {
            let _ = handle.signal(Sig::Kill).await;
            let _ = handle.wait().await;
            drain.abort();
            return Executed::Decided(parse_decision(-1, ""));
        }
    };
    let stdout = drain.await.unwrap_or_default();
    let code = status.code.unwrap_or(-1);
    Executed::Decided(parse_decision(code, &String::from_utf8_lossy(&stdout)))
}

/// On the host: `sh -c`, the context on stdin, in the workspace directory
/// when the sandbox shares the host filesystem.
async fn host_command(
    hook: &HookDefinition,
    command: &str,
    payload: &[u8],
    vars: BTreeMap<String, String>,
    env: Option<&dyn ExecEnv>,
) -> Executed {
    let mut process = Command::new("sh");
    process
        .arg("-c")
        .arg(command)
        .envs(vars)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(env) = env
        && env.shares_host_filesystem()
    {
        process.current_dir(env.workspace_path());
    }
    let mut child = match process.spawn() {
        Ok(child) => child,
        Err(error) => {
            return Executed::Decided(Decision::Block {
                reason: Some(format!("command spawn failed: {error}")),
            });
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        let bytes = payload.to_vec();
        tokio::spawn(async move {
            let _ = stdin.write_all(&bytes).await;
            let _ = stdin.shutdown().await;
        });
    }
    match timeout(hook.timeout(), child.wait_with_output()).await {
        Ok(Ok(output)) => Executed::Decided(parse_decision(
            output.status.code().unwrap_or(1),
            &String::from_utf8_lossy(&output.stdout),
        )),
        Ok(Err(error)) => Executed::Decided(Decision::Block {
            reason: Some(format!("command wait failed: {error}")),
        }),
        Err(_) => Executed::Decided(parse_decision(-1, "")),
    }
}

async fn http_hook(
    hook: &HookDefinition,
    url: &str,
    headers: &BTreeMap<String, String>,
    tls: TlsMode,
    context: &Context,
    http: &HttpClients,
) -> Executed {
    if tls != TlsMode::Off && !url.starts_with("https://") {
        return Executed::Decided(Decision::Block {
            reason: Some(format!(
                "HTTP hook URL must use https:// (tls mode is {tls:?})"
            )),
        });
    }
    let client = match http.client(tls) {
        Ok(client) => client,
        Err(error) => return Executed::FailedOpen(format!("HTTP client: {error}")),
    };
    let mut request = client.post(url).timeout(hook.timeout()).json(context);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(error) => return Executed::FailedOpen(format!("HTTP request failed: {error}")),
    };
    if !response.status().is_success() {
        return Executed::FailedOpen(format!("HTTP hook returned {}", response.status()));
    }
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => return Executed::FailedOpen(format!("HTTP body: {error}")),
    };
    if body.trim().is_empty() {
        return Executed::Decided(Decision::Proceed);
    }
    match serde_json::from_str::<Decision>(body.trim()) {
        Ok(decision) => Executed::Decided(decision),
        Err(_) => Executed::FailedOpen("HTTP response is not a hook decision".into()),
    }
}

fn evaluator_message(prompt: &str, context: &Context) -> String {
    format!(
        "Hook prompt: {prompt}\n\nEvent context:\n{}",
        serde_json::to_string_pretty(context).unwrap_or_default()
    )
}

/// Fabro's verdict to a decision: `ok` proceeds, anything else blocks.
fn verdict(text: &str) -> Option<Decision> {
    let trimmed = text.trim();
    let inner = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|rest| rest.strip_suffix("```"))
        .unwrap_or(trimmed)
        .trim();
    let verdict: Verdict = serde_json::from_str(inner).ok()?;
    Some(if verdict.ok {
        Decision::Proceed
    } else {
        Decision::Block {
            reason: verdict.reason,
        }
    })
}

async fn prompt_hook(
    hook: &HookDefinition,
    prompt: &str,
    model: Option<&str>,
    context: &Context,
    client: Option<&PebbleClient>,
) -> Execution {
    let Some(client) = client else {
        return Execution::of(Executed::Unsupported(
            "a prompt hook needs the application's model client".into(),
        ));
    };
    let model = model.unwrap_or(DEFAULT_MODEL);
    let schema = json!({
        "type": "object",
        "properties": { "ok": { "type": "boolean" }, "reason": { "type": "string" } },
        "required": ["ok"],
        "additionalProperties": false,
    });
    let mut builder = Request::builder()
        .model(model)
        .system(EVALUATOR_SYSTEM)
        .message(Message::text(
            Role::User,
            evaluator_message(prompt, context),
        ))
        .max_output_tokens(1024)
        .timeout(hook.timeout());
    let format = ResponseFormat::JsonSchema {
        name: "hook_verdict".into(),
        schema,
    };
    if let Ok(probe) = Request::builder().model(model).user("probe").build()
        && let Ok(route) = client.0.resolve_route(&probe)
        && !route
            .model()
            .capabilities()
            .response_format(&format)
            .is_unsupported()
    {
        builder = builder.response_format(format);
    }
    let request = match builder.build() {
        Ok(request) => request,
        Err(error) => {
            return Execution::of(Executed::FailedOpen(format!(
                "prompt hook request: {error}"
            )));
        }
    };
    // One request, answered or not; the answer brings its usage.
    let mut usage = HookUsage {
        requests: 1,
        ..HookUsage::default()
    };
    let outcome = match timeout(hook.timeout(), client.0.complete(request)).await {
        Ok(Ok(response)) => {
            usage.usage = Some(json!(response.usage_with_cost()));
            match verdict(&response.text()) {
                Some(decision) => Executed::Decided(decision),
                None => Executed::FailedOpen("the model did not return a hook verdict".into()),
            }
        }
        Ok(Err(error)) => Executed::FailedOpen(format!("prompt hook model call failed: {error}")),
        Err(_) => Executed::FailedOpen(format!(
            "prompt hook timed out after {} ms",
            hook.timeout().as_millis()
        )),
    };
    Execution {
        outcome,
        usage: Some(usage),
        activity: Vec::new(),
    }
}

async fn agent_hook(
    hook: &HookDefinition,
    prompt: &str,
    model: Option<&str>,
    max_tool_rounds: u32,
    context: &Context,
    env: Option<&Arc<dyn ExecEnv>>,
    client: Option<&PebbleClient>,
) -> Execution {
    let Some(client) = client else {
        return Execution::of(Executed::Unsupported(
            "an agent hook needs the application's model client".into(),
        ));
    };
    let Some(env) = env else {
        return Execution::of(Executed::Unsupported(
            "an agent hook runs its tools in the sandbox, and no sandbox environment is available \
             at this point"
                .into(),
        ));
    };
    // Fabro runs at most `max_tool_rounds` model turns, executing the tools
    // each asks for, and proceeds when the last one still asks for tools.
    // Pebble's budget of `rounds` lets `rounds` tool turns run and refuses
    // the next one without running its tools, so `max_tool_rounds - 1`
    // reaches the same decision at the same turn and spares the last,
    // useless tool execution. Zero rounds is Fabro's loop that never asks
    // the model: proceed without an agent.
    let Some(rounds) = max_tool_rounds.checked_sub(1) else {
        return Execution::of(Executed::FailedOpen(ROUNDS_EXHAUSTED.into()));
    };
    let work = AgentWork {
        permissions:  context.permissions,
        client:       client.clone(),
        env:          env.clone(),
        model:        model.unwrap_or(DEFAULT_MODEL).to_owned(),
        instructions: format!(
            "{EVALUATOR_SYSTEM}\n\n{}\n\nWhen you have decided, reply with only a JSON object: \
             {{\"ok\": true}} or {{\"ok\": false, \"reason\": \"...\"}}. You may use at most \
             {max_tool_rounds} tool rounds.",
            evaluator_message(prompt, context)
        ),
        rounds:       usize::try_from(rounds).unwrap_or(usize::MAX),
        budget:       hook.timeout(),
    };
    // The agent and its tool belong to a task of their own, not to this
    // future: the driver drops the awaiting callback on a root kill, and a
    // dropped future can await nothing. The guard turns that drop into
    // cancellation, and the task then stops the tool, joins the agent, and
    // ends on its own. While this future lives it awaits the same task, so a
    // timeout returns fail-open only once that cleanup finished. The
    // recursion guard rides along: hooks fire no hooks.
    let cancel = CancellationToken::new();
    let guard = cancel.clone().drop_guard();
    let task = tokio::spawn(IN_HOOK.scope(true, work.run(cancel)));
    let outcome = task.await;
    drop(guard);
    match outcome {
        Ok(execution) => execution,
        Err(error) => Execution::of(Executed::FailedOpen(format!(
            "agent hook task failed: {error}"
        ))),
    }
}

/// The hook agent's Petri event sink: every event the agent publishes, in
/// order, kept for the hook's record, and the counts its usage wants. The
/// driver's completion fence does not apply here; the agent's shutdown
/// flushes the last events before the record is taken.
#[derive(Default)]
struct HookEvents {
    events:     Mutex<Vec<Value>>,
    requests:   AtomicU64,
    tool_calls: AtomicU64,
}

#[async_trait::async_trait]
impl EventSink for HookEvents {
    async fn record(&self, event: &CodingAgentEvent) -> Result<(), EventSinkError> {
        match &event.event {
            CodingEvent::LlmRequestStarted { .. } => {
                self.requests.fetch_add(1, Ordering::Relaxed);
            }
            CodingEvent::ToolCallStarted { .. } => {
                self.tool_calls.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        let value = serde_json::to_value(event)
            .map_err(|e| EventSinkError::new("Could not encode Pebble event").with_source(e))?;
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(value);
        Ok(())
    }
}

impl HookEvents {
    /// The record so far: the events, and the usage with the counts filled
    /// in.
    fn finish(&self, outcome: Executed, mut usage: HookUsage) -> Execution {
        usage.requests = self.requests.load(Ordering::Relaxed);
        usage.tool_calls = self.tool_calls.load(Ordering::Relaxed);
        let activity = mem::take(&mut *self.events.lock().unwrap_or_else(PoisonError::into_inner));
        Execution {
            outcome,
            usage: Some(usage),
            activity,
        }
    }
}

/// Add a settled prompt's accounting to the hook's usage.
fn account(usage: &mut HookUsage, report: &PromptReport) {
    let ms = |duration: Duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    usage.usage = Some(json!(report.usage));
    usage.inference_ms = Some(ms(report.timing.inference));
    usage.tool_ms = Some(ms(report.timing.tool));
}

/// One agent hook's work, owned by the task that runs it.
struct AgentWork {
    permissions:  crate::AgentPermissions,
    client:       PebbleClient,
    env:          Arc<dyn ExecEnv>,
    model:        String,
    instructions: String,
    /// Pebble's tool-round budget: how many tool turns may run before a turn
    /// that asks for tools ends the prompt.
    rounds:       usize,
    /// The hook's timeout: the whole of environment, agent start and prompt.
    budget:       Duration,
}

/// Why a prompt did not run to its end.
enum Interrupted {
    Cancelled,
    TimedOut,
}

impl AgentWork {
    /// Run the agent to a verdict within the budget, unless `cancel` fires
    /// first. Whichever way it ends, the tool the agent may be running is
    /// stopped and the agent's tasks are joined before this returns, and the
    /// record carries every event the agent produced up to then.
    async fn run(self, cancel: CancellationToken) -> Execution {
        let kill = CancellationToken::new();
        let events = Arc::new(HookEvents::default());
        let grace = self.env.grace();
        let cleanup = grace + CLEANUP_MARGIN;
        let timed_out = || {
            Executed::FailedOpen(format!(
                "agent hook timed out after {} ms",
                self.budget.as_millis()
            ))
        };
        let mut usage = HookUsage::default();
        let deadline = sleep(self.budget);
        tokio::pin!(deadline);
        let mut agent = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return events.finish(
                    Executed::FailedOpen("agent hook cancelled before it started".into()),
                    usage,
                );
            }
            () = &mut deadline => return events.finish(timed_out(), usage),
            started = self.start(&cancel, &kill, events.clone()) => match started {
                Ok(agent) => agent,
                Err(message) => return events.finish(Executed::FailedOpen(message), usage),
            },
        };
        let (text, reason) = {
            let prompt = agent.prompt_with_cancellation(&self.instructions, &cancel);
            tokio::pin!(prompt);
            let ended = tokio::select! {
                biased;
                report = &mut prompt => Ok(report),
                () = cancel.cancelled() => Err(Interrupted::Cancelled),
                () = &mut deadline => Err(Interrupted::TimedOut),
            };
            match ended {
                Ok(report) => {
                    account(&mut usage, &report);
                    let reason = if report.result.is_ok() {
                        ShutdownReason::Completed
                    } else {
                        ShutdownReason::Error
                    };
                    let text = match report.result {
                        Ok(output) => Ok(output.text.unwrap_or_default()),
                        // The bound Fabro's loop has: the last turn still
                        // asked for tools, so the hook proceeds.
                        Err(AgentError::ToolRoundsExhausted { .. }) => {
                            Err(ROUNDS_EXHAUSTED.to_owned())
                        }
                        Err(error) => Err(format!("agent hook failed: {error}")),
                    };
                    (text, reason)
                }
                Err(why) => {
                    // Cancel the prompt and let its loop unwind: the running
                    // tool gets a TERM, the grace, then a KILL, and its result
                    // is committed. The kill token is the last resort should
                    // even that outlive the bound; the prompt is then dropped
                    // and `shutdown` finishes what it can. An unwound prompt
                    // still reports what it spent.
                    cancel.cancel();
                    match timeout(cleanup, &mut prompt).await {
                        Ok(report) => account(&mut usage, &report),
                        Err(_) => kill.cancel(),
                    }
                    let message = match why {
                        Interrupted::Cancelled => Err("agent hook cancelled".to_owned()),
                        Interrupted::TimedOut => Err(format!(
                            "agent hook timed out after {} ms",
                            self.budget.as_millis()
                        )),
                    };
                    (message, ShutdownReason::Cancelled)
                }
            }
        };
        // Join the agent's tasks, flushing its last events to the sink,
        // before the record is taken and the verdict returned.
        let _ = timeout(cleanup, agent.shutdown(reason)).await;
        let outcome = match text {
            Ok(text) => match verdict(&text) {
                Some(decision) => Executed::Decided(decision),
                None => Executed::FailedOpen("the agent did not return a hook verdict".into()),
            },
            Err(message) => Executed::FailedOpen(message),
        };
        events.finish(outcome, usage)
    }

    /// Probe the environment and start the agent with the coding tools and
    /// the hook's own event sink. The agent runs no tool-hook middleware:
    /// hooks fire no hooks.
    async fn start(
        &self,
        cancel: &CancellationToken,
        kill: &CancellationToken,
        events: Arc<HookEvents>,
    ) -> Result<CodingAgent, String> {
        let environment =
            PebbleEnvironment::prepare(self.env.clone(), cancel.clone(), kill.clone())
                .await
                .map_err(|e| format!("agent hook environment: {e}"))?;
        CodingAgent::builder(self.client.0.clone(), Arc::new(environment))
            .model(&self.model)
            .options(CodingAgentOptions::default().with_max_tool_rounds(self.rounds))
            .permission_level(permissions::pebble(self.permissions))
            .event_sink(events)
            .build()
            .await
            .map_err(|e| format!("agent hook could not start: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_decide_as_fabro_decides() {
        assert_eq!(parse_decision(0, ""), Decision::Proceed);
        assert_eq!(
            parse_decision(0, r#"{"decision":"skip","reason":"ci"}"#),
            Decision::Skip {
                reason: Some("ci".into()),
            }
        );
        assert_eq!(parse_decision(2, ""), Decision::Block {
            reason: Some("hook exited with code 2".into()),
        });
        assert_eq!(
            parse_decision(2, r#"{"decision":"proceed"}"#),
            Decision::Proceed
        );
        assert_eq!(
            parse_decision(3, r#"{"decision":"proceed"}"#),
            Decision::Block {
                reason: Some("hook exited with code 3".into()),
            }
        );
        assert_eq!(parse_decision(-1, ""), Decision::Block {
            reason: Some("hook exited with code -1".into()),
        });
    }

    #[test]
    fn verdicts_parse_with_or_without_fences() {
        assert_eq!(verdict(r#"{"ok": true}"#), Some(Decision::Proceed));
        assert_eq!(
            verdict("```json\n{\"ok\": false, \"reason\": \"tests fail\"}\n```"),
            Some(Decision::Block {
                reason: Some("tests fail".into()),
            })
        );
        assert_eq!(verdict("maybe"), None);
    }
}
