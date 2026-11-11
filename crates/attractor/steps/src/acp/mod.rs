//! A minimal Agent Client Protocol client over a step's process handle.
//!
//! The agent is a subprocess in the scope environment speaking ACP 1
//! JSON-RPC over stdio, as Claude Code (through the `claude-code-acp`
//! adapter) and Gemini CLI (`gemini --acp`) do: `initialize`, `session/new`
//! (after `authenticate` when the agent asks for it), `session/prompt`, with
//! `session/update` notifications streaming the agent's text, thoughts, tool
//! calls, plan and context usage back, and `session/request_permission`
//! requests answered by the run's tool hooks. Written against the wire
//! protocol rather than the `agent-client-protocol` crate: the subset a turn
//! needs is small, and the transport is Petri's [`ProcessHandle`] rather
//! than a socket the crate owns.
//!
//! Every notification the agent sends is recorded on the step's progress
//! channel as the backend envelope, `kind = "acp"` ([`ENVELOPE_KIND`]), the
//! way the native backend records Pebble's events: `{ kind, node, firing,
//! attempt, scope, event }` with `event` carrying `session_id`, `seq`, the
//! `tool_call_id` when the update names one, and the update itself. A
//! permission request and the answer Petri gave it are recorded the same
//! way.
//!
//! For codex-acp the client advertises native sub-agent sessions
//! (`clientCapabilities.subagents`, the draft ACP sub-agent RFD): the agent
//! announces each child on its parent with `subagent_spawned`
//! (`subagentSessionId`, `name`, `task`), streams the child's messages, tool
//! calls and permission requests under the child's own session id, and
//! reports its end on the parent with `subagent_state_update`; the prompt's
//! response waits for the children. Each envelope carries the session the
//! message names, so a child's work is recorded under its own session. The
//! stage's answer and its context window come from the root session alone;
//! a child's tool calls still pass through the tool hooks. claude-agent-acp
//! has the same sessions (since 0.71.0) but is not offered them: its
//! Task/Agent children would move off the root session, a shape no Claude
//! run has been checked against. Any other agent is not offered them either.
//!
//! Tool hooks map onto the two tool boundaries ACP offers. A
//! `pre_tool_use` hook runs at `session/request_permission`: a block answers
//! with the rejecting option, so the effect does not happen for that call;
//! otherwise the request is allowed, "always" when no `pre_tool_use` hook is
//! configured (nothing needs to see the next call of that kind, as Fabro's
//! client answered) and "once" when one is, so every later call still asks
//! and the hook still runs. `post_tool_use` and `post_tool_use_failure`
//! hooks run when the agent reports a tool call finished (`tool_call_update`
//! with status `completed` or `failed`), with the output or error the update
//! carries; their decisions are ignored, as Fabro ignores them. Both are
//! best effort: the agent decides which calls ask for permission and which
//! it reports. A call seen running or finished without a permission request
//! is warned once per hook and tool, and the client says before the first
//! prompt what each configured hook can and cannot see.
//!
//! Usage from the session usage extension (`unstable_session_usage`: the
//! `usage` a `session/prompt` response carries, and `usage_update`
//! notifications with the context window and the cumulative cost) folds into
//! the stage's `acp.usage` metric as lithos-llm's `Usage`, beside
//! `acp.turns` and the last `acp.context`.
//!
//! The agent process starts in the scope (a host directory or a container)
//! with the scope's environment, the workflow's secrets and the command's
//! own `env` (a value may be a `{"$secret": NAME}` reference, resolved
//! through the run's secrets); no secret it does not name. The session's
//! `cwd` is the scope's workspace.
//!
//! A host's interrupt (`Interrupt` under `$interrupt`) is `session/cancel`
//! without ending the process: the agent answers the prompt in flight with
//! stop reason `cancelled`, the client reports [`INTERRUPTED_EVENT`], and the
//! session's next prompt is the interrupt's `steer` text, else the next text
//! the host delivers. An interrupted turn's partial text is not the stage's
//! answer. Cancellation (a `Cancel` or `Kill` control, a closed channel) is
//! the same notification followed by stopping the process.

mod command;
mod hooks;

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use executor::{
    EnvError, ExitStatus, LineStream, Masker, ProcessHandle, ProcessSpec, Sig, StdinWriter,
};
use ir::{Attempt, Control, FiringId, LogStream, ScopeId, StepEvent, Value};
use lithos_llm::types::{Cost, CostSource, TokenCounts, Usage};
use serde_json::json;
use smol_str::SmolStr;
use steps::{Answer, Interrupt, ProgressSender, Steer};
use tokio::io::AsyncWriteExt as _;
use tokio::sync::mpsc;
use tokio::time;

pub use self::command::{AgentCommand, EnvValue, resolve_env};
pub use self::hooks::AcpHooks;
use self::hooks::{Finished, ToolCall, tool_output};
use crate::agent::INTERRUPTED_EVENT;
use crate::hooks::WARNING_EVENT;

/// The backend name in hook warnings and the thread event.
pub const BACKEND: &str = "acp";

/// The `kind` of the `StepEvent::Custom` payload every ACP notification and
/// permission exchange is recorded as: `{ kind, node, firing, attempt,
/// scope, event }`, where `event` is `{ session_id, seq, tool_call_id?,
/// method, update }` for a `session/update`, `{ session_id, seq, method,
/// params }` for any other notification, and `{ session_id, seq,
/// tool_call_id?, method, params, outcome }` for a
/// `session/request_permission` with the answer Petri gave. `session_id` is
/// the session the message names, a sub-agent's own for its work, else the
/// root session.
pub const ENVELOPE_KIND: &str = "acp";

/// The JSON-RPC error code ACP reserves for `auth_required`.
const AUTH_REQUIRED: i64 = -32000;

/// Why a turn did not complete.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AcpError {
    #[error("the agent process exited before the protocol completed{0}")]
    ProcessExited(String),
    #[error("the agent sent something that is not JSON-RPC: {0}")]
    Protocol(String),
    #[error("the agent answered `{method}` with an error: {message}")]
    Rejected {
        method:  String,
        message: String,
        code:    Option<i64>,
    },
    #[error("the turn ended with stop reason `{0}`")]
    StopReason(String),
    #[error("cancelled")]
    Cancelled,
}

/// What a completed turn produced.
#[derive(Clone, Debug, Default)]
pub struct Turn {
    pub text: String,
}

/// The stage a client serves, for the events it reports in Petri's own
/// vocabulary, and the mask set its envelopes pass through.
#[derive(Clone, Debug)]
pub struct Stage {
    pub node:    SmolStr,
    pub firing:  FiringId,
    pub attempt: Attempt,
    pub scope:   ScopeId,
    pub masker:  Masker,
}

/// One live connection to an agent process.
pub struct Client {
    handle:       Box<dyn ProcessHandle>,
    stdin:        StdinWriter,
    lines:        LineStream,
    logs:         ProgressSender,
    stage:        Stage,
    next_id:      u64,
    session_id:   Option<String>,
    exited:       bool,
    exit_grace:   Duration,
    stderr_tail:  String,
    hooks:        Option<Arc<AcpHooks>>,
    /// The envelope sequence, per connection.
    seq:          u64,
    /// The `authMethods` the agent advertised at `initialize`.
    auth_methods: Vec<Value>,
    /// The tool calls the agent named, by id.
    tools:        BTreeMap<String, ToolCall>,
    /// The tokens the session's turns used, from the session usage
    /// extension.
    usage:        Usage,
    /// The session's cumulative cost, from its last `usage_update`.
    cost:         Option<Cost>,
    /// The last context window report: tokens used, window size.
    context:      Option<(u64, u64)>,
}

/// What arrived from the agent, sorted by JSON-RPC shape.
enum Incoming {
    Response {
        id:     Value,
        result: Option<Value>,
        error:  Option<Value>,
    },
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id:     Value,
        method: String,
        params: Value,
    },
}

impl Client {
    /// Spawn the agent and take both ends of its stdio.
    pub async fn spawn(
        env: &dyn executor::ExecEnv,
        spec: ProcessSpec,
        logs: ProgressSender,
        stage: Stage,
    ) -> Result<Self, AcpError> {
        let program = spec.program.clone();
        let mut handle = env
            .spawn(spec)
            .await
            .map_err(|e| AcpError::ProcessExited(format!(": could not start `{program}`: {e}")))?;
        let stdin = handle
            .stdin()
            .ok_or_else(|| AcpError::Protocol("the executor gave the agent no stdin".into()))?;
        let lines = handle
            .lines()
            .ok_or_else(|| AcpError::Protocol("the executor gave the agent no stdout".into()))?;
        Ok(Self {
            handle,
            stdin,
            lines,
            logs,
            stage,
            next_id: 1,
            session_id: None,
            exited: false,
            exit_grace: env.grace(),
            stderr_tail: String::new(),
            hooks: None,
            seq: 0,
            auth_methods: Vec::new(),
            tools: BTreeMap::new(),
            usage: Usage::default(),
            cost: None,
            context: None,
        })
    }

    /// Bind the tool hooks this node configured. Warns about what each
    /// boundary can and cannot see before the first prompt.
    pub async fn with_hooks(&mut self, hooks: Arc<AcpHooks>) {
        for warning in hooks.known_gaps() {
            if let StepEvent::Custom(value) = &warning
                && value["kind"] == WARNING_EVENT
            {
                let _ = self
                    .logs
                    .send(StepEvent::Log {
                        stream: LogStream::Stderr,
                        line:   format!(
                            "hook warning ({}): hook `{}` on `{}`: {}",
                            BACKEND,
                            value["hook"].as_str().unwrap_or("?"),
                            value["event"].as_str().unwrap_or("?"),
                            value["message"].as_str().unwrap_or("")
                        ),
                    })
                    .await;
            }
            let _ = self.logs.send(warning).await;
        }
        self.hooks = Some(hooks);
    }

    /// The stage's ACP metrics: the turns, the usage the session usage
    /// extension reported, and the last context window report.
    pub fn metrics(&self, turns: u64) -> BTreeMap<SmolStr, Value> {
        let usage = Usage {
            tokens: self.usage.tokens,
            cost:   self.cost,
        };
        let mut metrics = BTreeMap::from([
            ("acp.turns".into(), Value::from(turns)),
            ("acp.usage".into(), json!(usage)),
        ]);
        if let Some((used, size)) = self.context {
            metrics.insert("acp.context".into(), json!({ "used": used, "size": size }));
        }
        metrics
    }

    async fn send(&mut self, message: Value) -> Result<(), AcpError> {
        let mut bytes =
            serde_json::to_vec(&message).map_err(|e| AcpError::Protocol(e.to_string()))?;
        bytes.push(b'\n');
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(|e| AcpError::ProcessExited(format!(": stdin closed: {e}")))?;
        self.stdin
            .flush()
            .await
            .map_err(|e| AcpError::ProcessExited(format!(": stdin closed: {e}")))
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<u64, AcpError> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;
        Ok(id)
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), AcpError> {
        self.send(json!({ "jsonrpc": "2.0", "method": method, "params": params }))
            .await
    }

    /// The next JSON-RPC message from stdout. Stderr lines are forwarded as
    /// step logs; stdout lines that are not JSON are forwarded too, since an
    /// agent that prints prose over its protocol stream is still worth
    /// hearing from.
    async fn receive(&mut self) -> Result<Incoming, AcpError> {
        loop {
            let Some(line) = self.lines.recv().await else {
                let detail = match time::timeout(self.exit_grace, self.handle.wait()).await {
                    Ok(outcome) => {
                        self.exited = true;
                        exit_detail(outcome)
                    }
                    Err(_) => ": stdout closed before the process exited".to_owned(),
                };
                let detail = if self.stderr_tail.is_empty() {
                    detail
                } else {
                    format!("{detail}; stderr: {}", self.stderr_tail.trim_end())
                };
                return Err(AcpError::ProcessExited(self.stage.masker.mask(&detail)));
            };
            if line.stream == LogStream::Stderr {
                self.log_stderr(line.line).await;
                continue;
            }
            let text = line.line;
            let Ok(value) = serde_json::from_str::<Value>(text.trim()) else {
                let _ = self
                    .logs
                    .send(StepEvent::Log {
                        stream: LogStream::Stdout,
                        line:   text,
                    })
                    .await;
                continue;
            };
            let method = value
                .get("method")
                .and_then(Value::as_str)
                .map(str::to_string);
            let id = value.get("id").cloned();
            return Ok(match (method, id) {
                (Some(method), Some(id)) => Incoming::Request {
                    id,
                    method,
                    params: value.get("params").cloned().unwrap_or(Value::Null),
                },
                (Some(method), None) => Incoming::Notification {
                    method,
                    params: value.get("params").cloned().unwrap_or(Value::Null),
                },
                (None, Some(id)) => Incoming::Response {
                    id,
                    result: value.get("result").cloned(),
                    error: value.get("error").cloned(),
                },
                (None, None) => {
                    return Err(AcpError::Protocol(format!(
                        "neither a request nor a response: {text}"
                    )));
                }
            });
        }
    }

    async fn log_stderr(&mut self, line: String) {
        self.stderr_tail.push_str(&self.stage.masker.mask(&line));
        self.stderr_tail.push('\n');
        // Keep a bounded UTF-8 tail for unexpected protocol EOF.
        let mut excess = self.stderr_tail.len().saturating_sub(8 * 1024);
        while !self.stderr_tail.is_char_boundary(excess) {
            excess += 1;
        }
        self.stderr_tail.drain(..excess);
        let _ = self
            .logs
            .send(StepEvent::Log {
                stream: LogStream::Stderr,
                line,
            })
            .await;
    }

    /// Record one ACP exchange on the progress channel as the backend
    /// envelope. `event` is the exchange's own fields; the session (the one
    /// the message named, else the root) and the sequence are added here.
    async fn record(
        &mut self,
        session: Option<&str>,
        tool_call_id: Option<&str>,
        mut event: serde_json::Map<String, Value>,
    ) {
        self.seq += 1;
        let session = session.or(self.session_id.as_deref());
        event.insert("session_id".into(), json!(session));
        event.insert("seq".into(), json!(self.seq));
        if let Some(id) = tool_call_id {
            event.insert("tool_call_id".into(), json!(id));
        }
        let _ = self
            .logs
            .send(StepEvent::Custom(json!({
                "kind": ENVELOPE_KIND,
                "node": self.stage.node,
                "firing": self.stage.firing,
                "attempt": self.stage.attempt,
                "scope": self.stage.scope,
                "event": self.stage.masker.mask_value(&Value::Object(event)),
            })))
            .await;
    }

    /// Answer a permission request. A blocking `pre_tool_use` hook answers
    /// with the rejecting option, so the call does not happen; otherwise the
    /// call is allowed, always when no `pre_tool_use` hook is configured and
    /// once when one is, so the next call of that kind still asks.
    async fn answer_permission(&mut self, id: Value, params: &Value) -> Result<(), AcpError> {
        let options = params
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let pick = |kind: &str| {
            options
                .iter()
                .find(|o| o.get("kind").and_then(Value::as_str) == Some(kind))
        };
        let named = params.get("toolCall").cloned().unwrap_or(Value::Null);
        // The call is remembered under its id; a request naming no id is
        // answered from what it says and not remembered.
        let mut unnamed = ToolCall::default();
        let call = match named.get("toolCallId").and_then(Value::as_str) {
            Some(id) => self.tools.entry(id.to_owned()).or_default(),
            None => &mut unnamed,
        };
        call.merge(&named);
        call.asked = true;
        let call = call.clone();
        let hooks = self.hooks.clone();
        let blocked = match &hooks {
            Some(hooks) => hooks.pre_tool(&call, &self.logs).await,
            None => None,
        };
        let chosen = if let Some(reason) = &blocked {
            let _ = self
                .logs
                .send(StepEvent::Log {
                    stream: LogStream::Stderr,
                    line:   format!("pre_tool_use hook blocked a permission request: {reason}"),
                })
                .await;
            pick("reject_once").or_else(|| pick("reject_always"))
        } else {
            let allow_always = hooks.as_ref().is_none_or(|hooks| !hooks.has_pre());
            let (first, second) = if allow_always {
                ("allow_always", "allow_once")
            } else {
                ("allow_once", "allow_always")
            };
            pick(first).or_else(|| pick(second)).or_else(|| {
                options.iter().find(|o| {
                    !matches!(
                        o.get("kind").and_then(Value::as_str),
                        Some("reject_once" | "reject_always")
                    )
                })
            })
        };
        let outcome = match chosen.and_then(|o| o.get("optionId")) {
            Some(option) => json!({ "outcome": "selected", "optionId": option }),
            None => json!({ "outcome": "cancelled" }),
        };
        let mut event = serde_json::Map::new();
        event.insert("method".into(), json!("session/request_permission"));
        event.insert("params".into(), params.clone());
        event.insert("outcome".into(), outcome.clone());
        event.insert("blocked".into(), json!(blocked));
        self.record(session_of(params), call.id.as_deref(), event)
            .await;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "result": { "outcome": outcome } }))
            .await
    }

    /// Wait for the response to request `id`, serving what arrives meanwhile.
    async fn response(
        &mut self,
        id: u64,
        method: &str,
        turn: &mut Turn,
    ) -> Result<Value, AcpError> {
        loop {
            match self.receive().await? {
                Incoming::Response {
                    id: got,
                    result,
                    error,
                } if got == json!(id) => {
                    if let Some(error) = error {
                        return Err(rejected(method, &error));
                    }
                    return Ok(result.unwrap_or(Value::Null));
                }
                Incoming::Response { .. } => {}
                Incoming::Notification { method, params } => {
                    self.on_notification(&method, params, turn).await;
                }
                Incoming::Request { id, method, params } => {
                    self.on_request(id, &method, &params).await?;
                }
            }
        }
    }

    async fn on_notification(&mut self, method: &str, params: Value, turn: &mut Turn) {
        let session = session_of(&params).map(str::to_owned);
        if method != "session/update" {
            let mut event = serde_json::Map::new();
            event.insert("method".into(), json!(method));
            event.insert("params".into(), params);
            self.record(session.as_deref(), None, event).await;
            return;
        }
        let update = params.get("update").cloned().unwrap_or(Value::Null);
        match feeds(self.session_id.as_deref(), session.as_deref(), &update) {
            Feeds::Answer(text) => turn.text.push_str(text),
            Feeds::ToolCall => self.on_tool_call(&update).await,
            Feeds::Context => self.on_usage_update(&update),
            Feeds::Nothing => {}
        }
        let tool_call_id = update
            .get("toolCallId")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut event = serde_json::Map::new();
        event.insert("method".into(), json!(method));
        event.insert("update".into(), update);
        self.record(session.as_deref(), tool_call_id.as_deref(), event)
            .await;
    }

    /// A `tool_call` or `tool_call_update`: remember the call, warn when it
    /// runs past every `pre_tool_use` hook, and run the post-tool hooks when
    /// it finishes.
    async fn on_tool_call(&mut self, update: &Value) {
        let Some(id) = update.get("toolCallId").and_then(Value::as_str) else {
            return;
        };
        let status = update.get("status").and_then(Value::as_str);
        let call = self.tools.entry(id.to_owned()).or_default();
        call.merge(update);
        let Some(hooks) = self.hooks.clone() else {
            return;
        };
        // A call seen running or finished that never asked ran past every
        // `pre_tool_use` hook. Said once per call: the next update of this
        // call is not a second unasked run.
        let unasked = !call.asked && matches!(status, Some("in_progress" | "completed" | "failed"));
        if unasked {
            call.asked = true;
        }
        let call = call.clone();
        if unasked {
            hooks.unintercepted(&call.name, &self.logs).await;
        }
        let finished = match status {
            Some("completed") => Finished::Completed {
                output: tool_output(update),
            },
            Some("failed") => Finished::Failed {
                error: tool_output(update),
            },
            _ => return,
        };
        hooks.post_tool(&call, &finished, &self.logs).await;
    }

    /// A `usage_update`: the context window, and the session's cumulative
    /// cost when it is in US dollars.
    fn on_usage_update(&mut self, update: &Value) {
        if let (Some(used), Some(size)) = (
            update.get("used").and_then(Value::as_u64),
            update.get("size").and_then(Value::as_u64),
        ) {
            self.context = Some((used, size));
        }
        if let Some(cost) = update.get("cost")
            && cost.get("currency").and_then(Value::as_str) == Some("USD")
            && let Some(amount) = cost.get("amount").and_then(Value::as_f64)
            && amount.is_finite()
            && amount >= 0.0
        {
            // A whole number of micros once rounded, and non-negative: the
            // conversion saturates on an absurd amount rather than wrapping.
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "rounded and non-negative; saturates on out-of-range values"
            )]
            let usd_micros = (amount * 1_000_000.0).round() as u64;
            self.cost = Some(Cost {
                usd_micros,
                source: CostSource::Provider,
            });
        }
    }

    /// The `usage` a `session/prompt` response carries, folded into the
    /// session's usage.
    fn fold_turn_usage(&mut self, result: &Value) {
        let Some(usage) = result.get("usage").filter(|u| u.is_object()) else {
            return;
        };
        let count = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
        let turn = Usage {
            tokens: TokenCounts {
                input:       count("inputTokens"),
                output:      count("outputTokens"),
                reasoning:   count("thoughtTokens"),
                cache_read:  count("cachedReadTokens"),
                cache_write: count("cachedWriteTokens"),
            },
            cost:   None,
        };
        self.usage = self.usage.saturating_add(turn);
    }

    async fn on_request(
        &mut self,
        id: Value,
        method: &str,
        params: &Value,
    ) -> Result<(), AcpError> {
        match method {
            "session/request_permission" => self.answer_permission(id, params).await,
            other => {
                self.send(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32601, "message": format!("method not found: {other}") },
                }))
                .await
            }
        }
    }

    /// `initialize` then `session/new` in `cwd`. An agent that answers
    /// `session/new` with `auth_required` is authenticated with the API-key
    /// method it advertised (the key is in its environment), then asked
    /// again.
    /// `subagents` offers the agent native sub-agent sessions.
    pub async fn open_session(&mut self, cwd: &str, subagents: bool) -> Result<(), AcpError> {
        let mut scratch = Turn::default();
        let id = self
            .request("initialize", initialize_params(subagents))
            .await?;
        let initialized = self.response(id, "initialize", &mut scratch).await?;
        self.auth_methods = initialized
            .get("authMethods")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let new_session = json!({ "cwd": cwd, "mcpServers": [] });
        let id = self.request("session/new", new_session.clone()).await?;
        let result = match self.response(id, "session/new", &mut scratch).await {
            Err(AcpError::Rejected {
                code: Some(AUTH_REQUIRED),
                message,
                ..
            }) => {
                let Some(method) = api_key_method(&self.auth_methods) else {
                    return Err(AcpError::Rejected {
                        method:  "session/new".into(),
                        message: format!(
                            "{message} (the agent requires authentication and advertises no \
                             API-key method)"
                        ),
                        code:    Some(AUTH_REQUIRED),
                    });
                };
                let _ = self
                    .logs
                    .send(StepEvent::Log {
                        stream: LogStream::Stderr,
                        line:   format!("authenticating with the agent's `{method}` method"),
                    })
                    .await;
                let id = self
                    .request("authenticate", json!({ "methodId": method }))
                    .await?;
                self.response(id, "authenticate", &mut scratch).await?;
                let id = self.request("session/new", new_session).await?;
                self.response(id, "session/new", &mut scratch).await?
            }
            other => other?,
        };
        let session = result
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| AcpError::Protocol("`session/new` returned no sessionId".into()))?;
        self.session_id = Some(session.to_string());
        Ok(())
    }

    /// One prompt turn. `control` delivers steering (`Deliver` text is queued
    /// and sent as a follow-up prompt when the turn ends), an interrupt
    /// (`session/cancel`; the turn ends with `cancelled` and the session
    /// continues with its next input) and cancellation (`session/cancel`,
    /// then the process is stopped).
    pub async fn prompt(
        &mut self,
        text: &str,
        control: &mut mpsc::Receiver<Control>,
        grace: Duration,
    ) -> Result<Turn, AcpError> {
        let session = self
            .session_id
            .clone()
            .ok_or_else(|| AcpError::Protocol("no session".into()))?;
        let mut turn = Turn::default();
        let mut pending = VecDeque::from([text.to_string()]);
        while let Some(prompt) = pending.pop_front() {
            let id = self
                .request(
                    "session/prompt",
                    json!({
                        "sessionId": session,
                        "prompt": [{ "type": "text", "text": prompt }],
                    }),
                )
                .await?;
            // Whether the host interrupted this prompt: its `cancelled` stop
            // reason is then the interrupt landing, not a cancellation.
            let mut interrupted = false;
            let stop_reason = loop {
                tokio::select! {
                    message = self.receive() => match message? {
                        Incoming::Response { id: got, result, error } if got == json!(id) => {
                            if let Some(error) = error {
                                return Err(rejected("session/prompt", &error));
                            }
                            let result = result.unwrap_or(Value::Null);
                            self.fold_turn_usage(&result);
                            break result
                                .get("stopReason")
                                .and_then(Value::as_str)
                                .unwrap_or("end_turn")
                                .to_string();
                        }
                        Incoming::Response { .. } => {}
                        Incoming::Notification { method, params } => self.on_notification(&method, params, &mut turn).await,
                        Incoming::Request { id, method, params } => self.on_request(id, &method, &params).await?,
                    },
                    ctl = control.recv() => {
                        if let Some(Control::Deliver(value)) = ctl {
                            if let Some(interrupt) = Interrupt::from_value(&value) {
                                if let Some(text) = interrupt.steer {
                                    pending.push_back(text);
                                }
                                if !interrupted {
                                    interrupted = true;
                                    self.notify("session/cancel", json!({ "sessionId": session })).await?;
                                }
                            } else if let Some(text) = steer_text(&value) {
                                pending.push_back(text);
                            }
                        } else {
                            self.cancel(&session, grace).await;
                            return Err(AcpError::Cancelled);
                        }
                    }
                }
            };
            match stop_reason.as_str() {
                "end_turn" | "refusal" => {}
                "cancelled" if interrupted => {
                    // The interrupted turn produced no answer; what it said
                    // so far is not the stage's text.
                    turn.text.clear();
                    interrupted_event(&self.logs, &self.stage, &session).await;
                    if pending.is_empty() {
                        pending.push_back(self.next_input(control, &session, grace).await?);
                    }
                }
                "cancelled" => return Err(AcpError::Cancelled),
                other => return Err(AcpError::StopReason(other.to_string())),
            }
        }
        Ok(turn)
    }

    /// After a plain interrupt: wait for the next text the host delivers,
    /// which is the session's next prompt. A further interrupt carrying text
    /// supplies it too; one without has nothing to stop. Anything else on
    /// the channel cancels.
    async fn next_input(
        &mut self,
        control: &mut mpsc::Receiver<Control>,
        session: &str,
        grace: Duration,
    ) -> Result<String, AcpError> {
        loop {
            let Some(Control::Deliver(value)) = control.recv().await else {
                self.cancel(session, grace).await;
                return Err(AcpError::Cancelled);
            };
            let text = match Interrupt::from_value(&value) {
                Some(interrupt) => interrupt.steer,
                None => steer_text(&value),
            };
            if let Some(text) = text {
                return Ok(text);
            }
        }
    }

    /// `session/cancel`, a grace period for the agent to wind down, then the
    /// process is stopped: TERM, grace, KILL.
    async fn cancel(&mut self, session: &str, grace: Duration) {
        let _ = self
            .notify("session/cancel", json!({ "sessionId": session }))
            .await;
        let deadline = time::sleep(Duration::from_millis(500));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                message = self.receive() => if message.is_err() { break },
                () = &mut deadline => break,
            }
        }
        self.terminate(grace).await;
    }

    /// Stop the process: TERM, grace, KILL. Idempotent once it has exited.
    pub async fn terminate(&mut self, grace: Duration) {
        if self.exited {
            return;
        }
        let _ = self.handle.signal(Sig::Term).await;
        if time::timeout(grace, self.handle.wait()).await.is_err() {
            let _ = self.handle.signal(Sig::Kill).await;
            let _ = self.handle.wait().await;
        }
        self.exited = true;
    }
}

/// How the agent process ended, as a suffix for [`AcpError::ProcessExited`].
fn exit_detail(outcome: Result<ExitStatus, EnvError>) -> String {
    match outcome {
        Err(error) => format!(": {error}"),
        Ok(status) if status.timed_out => ": process timed out".to_owned(),
        Ok(status) => match (status.code, status.signal) {
            (Some(code), _) => format!(": exit code {code}"),
            (_, Some(signal)) => format!(": signal {signal}"),
            _ => ": exit status unavailable".to_owned(),
        },
    }
}

/// The `initialize` request: no client file system, and native sub-agent
/// sessions when `subagents`, so the agent streams each child's work under
/// the child's own session.
fn initialize_params(subagents: bool) -> Value {
    let mut capabilities = json!({ "fs": { "readTextFile": false, "writeTextFile": false } });
    if subagents {
        // The draft ACP field, and the `_meta` form codex-acp 1.13 honours:
        // its SDK strips a capability the schema does not know yet.
        capabilities["subagents"] = json!({});
        capabilities["_meta"] = json!({ "jetbrains": { "air": {
            "version": 1,
            "capabilities": ["nativeSubagentSessions"],
        }}});
    }
    json!({ "protocolVersion": 1, "clientCapabilities": capabilities })
}

/// The session a notification or request names.
fn session_of(params: &Value) -> Option<&str> {
    params.get("sessionId").and_then(Value::as_str)
}

/// Whether a message names a session other than the root: a sub-agent's.
fn is_subagent(root: Option<&str>, session: Option<&str>) -> bool {
    matches!((root, session), (Some(root), Some(session)) if root != session)
}

/// What a `session/update` feeds besides its envelope.
#[derive(Debug, PartialEq, Eq)]
enum Feeds<'a> {
    /// Text of the stage's answer.
    Answer(&'a str),
    /// The tool hooks, from any session.
    ToolCall,
    /// The stage's context window and cost.
    Context,
    Nothing,
}

/// What `update` from `session` feeds: the answer and the context window
/// come from the root session alone.
fn feeds<'a>(root: Option<&str>, session: Option<&str>, update: &'a Value) -> Feeds<'a> {
    let subagent = is_subagent(root, session);
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("agent_message_chunk") if !subagent => update
            .pointer("/content/text")
            .and_then(Value::as_str)
            .filter(|_| update.pointer("/content/type").and_then(Value::as_str) == Some("text"))
            .map_or(Feeds::Nothing, Feeds::Answer),
        Some("tool_call" | "tool_call_update") => Feeds::ToolCall,
        Some("usage_update") if !subagent => Feeds::Context,
        _ => Feeds::Nothing,
    }
}

/// A JSON-RPC error answer as the typed rejection.
fn rejected(method: &str, error: &Value) -> AcpError {
    AcpError::Rejected {
        method:  method.to_string(),
        message: error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("unknown error")
            .to_string(),
        code:    error.get("code").and_then(Value::as_i64),
    }
}

/// The API-key authentication method among those the agent advertised: the
/// first marked `_meta.api-key` (Gemini CLI marks `gemini-api-key` so), else
/// the first whose id says so.
fn api_key_method(methods: &[Value]) -> Option<String> {
    let id_of = |method: &Value| method.get("id").and_then(Value::as_str).map(str::to_owned);
    methods
        .iter()
        .find(|method| method.pointer("/_meta/api-key").is_some())
        .and_then(id_of)
        .or_else(|| {
            methods.iter().find_map(|method| {
                let id = id_of(method)?;
                let lower = id.to_ascii_lowercase();
                (lower.contains("api-key") || lower.contains("api_key") || lower.contains("apikey"))
                    .then_some(id)
            })
        })
}

/// The stage's report that a host's interrupt stopped its turn.
async fn interrupted_event(logs: &ProgressSender, stage: &Stage, session: &str) {
    let _ = logs
        .send(StepEvent::Custom(json!({
            "kind": INTERRUPTED_EVENT,
            "node": stage.node,
            "firing": stage.firing,
            "attempt": stage.attempt,
            "backend": BACKEND,
            "session": session,
        })))
        .await;
}

/// The guidance a delivered value carries: a core [`Steer`], or the older
/// answer-shaped spelling (a bare string, or `text`/`choice` fields).
fn steer_text(value: &Value) -> Option<String> {
    if let Some(steer) = Steer::from_value(value) {
        return Some(steer.text);
    }
    let answer = Answer::from_value(value)?;
    answer
        .text
        .and_then(|text| text.as_str().map(str::to_string))
        .or(answer.choice)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_api_key_method_is_the_marked_one_else_the_named_one() {
        let gemini = json!([
            { "id": "oauth-personal", "name": "Log in with Google" },
            { "id": "gemini-api-key", "name": "Gemini API key", "_meta": { "api-key": { "provider": "google" } } },
            { "id": "vertex-ai", "name": "Vertex AI" },
        ]);
        assert_eq!(
            api_key_method(gemini.as_array().expect("array")).as_deref(),
            Some("gemini-api-key")
        );
        let named = json!([{ "id": "login" }, { "id": "my_api_key" }]);
        assert_eq!(
            api_key_method(named.as_array().expect("array")).as_deref(),
            Some("my_api_key")
        );
        let claude = json!([{ "id": "claude-login", "name": "Log in with Claude Code" }]);
        assert_eq!(api_key_method(claude.as_array().expect("array")), None);
    }

    #[test]
    fn native_subagent_sessions_are_offered_only_when_asked() {
        let fs = json!({ "readTextFile": false, "writeTextFile": false });
        assert_eq!(
            initialize_params(true)["clientCapabilities"],
            json!({ "fs": fs, "subagents": {}, "_meta": { "jetbrains": { "air": {
                "version": 1,
                "capabilities": ["nativeSubagentSessions"],
            }}}})
        );
        assert_eq!(
            initialize_params(false)["clientCapabilities"],
            json!({ "fs": fs })
        );
    }

    #[test]
    fn only_the_root_session_feeds_the_answer_and_the_context_window() {
        let chunk = |text: &str| {
            json!({ "sessionUpdate": "agent_message_chunk",
                "content": { "type": "text", "text": text } })
        };
        let usage = json!({ "sessionUpdate": "usage_update", "used": 90_000, "size": 272_000 });
        let call = json!({ "sessionUpdate": "tool_call", "toolCallId": "call_c1" });
        let root = Some("thr_root");
        let (root_chunk, child_chunk) = (chunk("Reviewed."), chunk("One TODO."));
        let fed: Vec<Feeds<'_>> = [
            (Some("thr_root"), &root_chunk),
            (Some("thr_child"), &child_chunk),
            (Some("thr_child"), &usage),
            (Some("thr_child"), &call),
            (Some("thr_root"), &usage),
            (None, &root_chunk),
        ]
        .into_iter()
        .map(|(session, update)| feeds(root, session, update))
        .collect();
        assert_eq!(fed, [
            Feeds::Answer("Reviewed."),
            Feeds::Nothing,
            Feeds::Nothing,
            Feeds::ToolCall,
            Feeds::Context,
            Feeds::Answer("Reviewed."),
        ]);
        let image = json!({ "sessionUpdate": "agent_message_chunk",
            "content": { "type": "image", "text": "x" } });
        assert_eq!(feeds(root, root, &image), Feeds::Nothing);
    }

    #[test]
    fn only_a_session_other_than_the_root_is_a_subagent() {
        let child = json!({ "sessionId": "thr_child", "update": {} });
        assert_eq!(session_of(&child), Some("thr_child"));
        assert!(is_subagent(Some("thr_root"), session_of(&child)));
        assert!(!is_subagent(Some("thr_child"), session_of(&child)));
        assert!(!is_subagent(
            Some("thr_root"),
            session_of(&json!({ "update": {} }))
        ));
        assert!(!is_subagent(None, Some("thr_child")), "no root yet");
    }
}
