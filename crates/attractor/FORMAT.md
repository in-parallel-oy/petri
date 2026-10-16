# The Attractor language, as lowered

Petri runs Attractor workflows: Graphviz DOT files (`*.fabro`, `*.dot`) in the
dialect the reference implementation, Fabro, accepts, which the next revision
of the Attractor specification is being drafted to take in. This page says
what each construct in the DOT file becomes in the engine's IR, and what is
refused. `.ai/plans/done/fabro-frontend-phase-one.md` records the lowering
decisions and `.ai/plans/attractor-split.md` the split of this page from the
Fabro layer. The reference revision, the required workflow bundles, the
feature matrix, and the accepted differences are frozen in
[`crates/fabro/acceptance/CONTRACT.md`](../fabro/acceptance/CONTRACT.md).

The rule throughout: **every construct lowers onto what the core has**. No
engine semantics were added for the language. A construct that cannot lower
is a specific `unsupported.*` rejection, never a silent approximation.

What is in the DOT file is this page's. What comes from Fabro's settings
files beside it (`workflow.toml`, `.fabro/project.toml`, the user settings
layer) and from the `fabro run` launch is the Fabro layer's,
[`crates/fabro/FORMAT.md`](../fabro/FORMAT.md): that page says how each
file section resolves into the run settings the lowering here applies. Three
Fabro conventions the language carries are listed at the end of this page.
What a host that embeds Petri relies on is
[`crates/fabro/HANDOFF.md`](../fabro/HANDOFF.md). The public events every
construct below produces, and the projection tests that prove them, are the
event-coverage matrix in `crates/core/execution/EVENTS.md`.

## Run creation happens at load

The reference implementation renders templates, resolves `@file` references
and applies its model stylesheet once, when a run is created, and persists
the literal graph. The frontend does the same at load, so
`petri check --print-graph` shows the graph a run will execute.

| In the DOT file | At load |
|---|---|
| `{{ inputs.* }}`, `{{ vars.* }}`, `{{ goal }}` in the goal and prompts | rendered with MiniJinja, strict: an unbound name is `unsupported.template.unbound_input` with the `--input KEY=VALUE` hint. `petri check` given no inputs at all downgrades it to the warning `attractor.unbound_input` and leaves the text unrendered, so a file validates before its inputs exist; a run is always strict |
| the same tokens in a `script` | Fabro's token interpolation: each token is one shell-quoted word |
| `prompt="@prompts/x.md"`, `output_schema="@schemas/x.json"` | read beside the workflow file; `{% include %}` resolves beside the included file |
| `model_stylesheet` | rendered, parsed (`*`, shape, `.class`, `#id`; specificity 0–3), written onto nodes; an explicit node attribute wins |
| `import="<path>"` | expanded at load as Fabro's import transform expands it (below); the persisted graph carries the imported nodes |

Inputs, vars and the rendered goal land in `Graph.params` (`inputs`, `vars`,
`goal`), so the persisted graph is self-describing for replay. The graph's
`goal` attribute is the run's goal by default; a run goal the host frontend's
settings resolve (Fabro's `[run] goal`, or the launch's goal above it,
`crates/fabro/FORMAT.md`) replaces it, and is the goal every stage and
`{{ goal }}` see. The merged
hooks land in `Graph.params["attractor.hooks"]` and the workflow's name in
`Graph.params["attractor.workflow"]`. The frontend that resolved the run's
settings adds its own record beside them (Fabro: `fabro.launch`,
`fabro.environment`).

## Imports

A node with `import="<path>"` is a placeholder for another workflow file,
resolved relative to the importing file. The imported file's nodes are
spliced in under the placeholder's id as a prefix (`<placeholder>.<node>`);
its start and exit sentinels (`Mdiamond`/`Msquare`, or the ids `start`,
`exit`, `end`) are dropped; the placeholder's incoming edges reach the
imported entry node and its outgoing edges leave the imported exit
predecessor, both with their attributes. The placeholder may carry only
`import`, `class`, and the inheritable defaults `model`, `provider`,
`reasoning_effort`, `speed`, `backend`, `acp.command`, `acp.config`,
`fidelity`, `max_retries`, `thread_id`; each default lands on every imported
node that does not set it. The placeholder's classes and a class made from its
id (lowercase, spaces to `-`, `[a-z0-9-]` only) propagate to every imported
node. `retry_target` and `fallback_retry_target` inside the import are rewritten
to the prefixed ids; `@file` references inside it resolve beside the imported
file. Imports nest, each relative to its own file; a cycle is refused.

Fabro's boundary rules apply and every failure is `attractor.import` on the
placeholder with Fabro's message: exactly one start and one exit; no edge into
start or out of exit; exactly one successor of start and one predecessor of
exit, neither edge carrying `condition`, `label`, `weight`, `fidelity`,
`thread_id`, `loop_restart` or `freeform`; an empty import (start to exit
only) is removed and its neighbours wired directly, but not across a semantic
edge; a placeholder with any other attribute, a self-loop, or a prefixed id
that already exists is refused. An imported `model_stylesheet` is ignored with
a warning; the importing workflow's stylesheet governs.

## Nodes

| Shape / type | Petri node | Step config |
|---|---|---|
| `Mdiamond` start | `attractor/stage`, the entry | `kind = "start"`, the workflow name, the merged hooks |
| `Msquare` exit | `attractor/stage`; `Completion::TerminalNode(exit)` | `kind = "exit"`, the workflow name, the merged hooks |
| `diamond` conditional | `noop` | |
| `box` agent | `attractor/agent` | prompt, goal, `fidelity` and `default_fidelity`, `thread_id`, `default_thread` and the node's classes, `project_memory`, `backend`, model settings (`model`, `provider`, `reasoning_effort`, `speed`, `max_tokens`), `output_schema`, `output_retries`, `acp`, `mcps` (the run's MCP servers), the workflow's stage list for the preamble |
| `tab` prompt | `attractor/prompt` | prompt, goal, `fidelity` and the thread attributes (accepted; a prompt node never continues a conversation), `project_memory`, model settings (`model`, `provider`, `reasoning_effort`, `speed`, `max_tokens`), `output_schema`, `output_retries`; API-only: `backend="acp"` on the node is `attractor.prompt_backend`, and the graph's ACP settings never reach it |
| `parallelogram` command, or any node with `script` | `attractor/command` | script, language, `stdin` (an expression over `kv`), `output_schema`, `env` (`[run.prepare]` step env and the environment's `$secret` values) |
| `hexagon` human | `attractor/human` | the choices (from the edges, each with the edge's `human.description` and `human.preview` when set), `question_type`, `freeform_target`, `sensitive`, `review_target`, `default_choice` (from `human.default_choice`), `timeout_ms` |
| `component` parallel | `attractor/fork`: takes the fork snapshot of `kv` (and of the stage records for agent or prompt targets) once per visit, offloads the `for_each` source list and every other value above 4 KiB to the output store, and outputs `{ snapshot, nodes }`; each branch target becomes a synthetic `attractor/branch` delegate (`kind = "parallel.branch"`) that runs a copy of the target in a child invocation from that snapshot; `for_each` marks the delegate `Expansion::ForEach` (below) | fork: `label`, `node`, `kv`, `nodes`, `source`, `inline`; branch: `label`, `node`, `fork`, `index`, `item`, `for_each`, `max_parallel`, `child_digest`, `target_kind`, `kv`, `nodes`, `generation` |
| `tripleoctagon` fan-in | `attractor/fan_in`, `join: all`; publishes `parallel.results` and `parallel.branch_count`; its output is the ordered branch results | |
| `tripleoctagon` fan-in with a `prompt` | `attractor/prompt`, `join: all`: the ordered barrier, the same `parallel.results` publication, then one model call over the branch results (`sources`, `branch_results`) | |
| a `component` whose branches share a plain successor | a synthetic `<fork>.fan_in` (`attractor/fan_in`, `synthetic: true`) before that successor | |
| `insulator` wait | `attractor/wait` | `duration_ms` |
| `house` manager loop | `attractor/workflow` | the child graph's digest, `manager.*` |
| `circle`, `doublecircle`, other shapes | `attractor/agent`, with an `attractor.unknown_shape` warning | |

Every node's `meta` carries `label`, `shape`, `kind`, `classes`, `span`, and
`model` / `provider` / `reasoning_effort` when set; a command node's `meta`
also carries `script`, the text the step runs (the same value as its
config's), and every node with outgoing edges carries `edges`, one entry per
routing arm keyed by the arm's edge id (the id a `route.applied` record
names): `{ to, label, condition }`, the target node name, the edge's `label`
(null without one) and its `condition` as written, trimmed (absent on an
unconditional edge). A host that shows a stage's script or the condition a
routing decision matched reads both off `subject.node.meta`
(`crates/core/execution/EVENTS.md`, "Source metadata"). Every step config carries
`kv` (the run context at spawn) and `on_failure`; a node whose `on_failure` is
`succeed` or `partially_succeed` also carries `routes`, its explicit routes
(condition texts, label keys, unconditional targets) for the promotion check
below.

Timeouts: `timeout` is the per-attempt `Budget.timeout`. Without one, a
command gets 600 s (Fabro's default), an agent 24 h, a human gate 30 days, a
wait its duration plus an hour. A bare number (`timeout=1200`) is the
Attractor spelling and is refused; write the unit.

Who enforces the timeout follows Fabro's handler policies
(`Budget.timeout_policy`). A command, a human gate and an ACP agent are
`HandlerManaged`: the command sends its deadline to the sandbox (`timeout_ms`
in the step config, 600 s by default) and fails with `Script timed out after
Nms` and class `timeout`; the human gate's timeout is its answer deadline (see
"Steps at run time"); the ACP agent hands the deadline to its turn. The
driver arms no timer of its own around those steps, so a human gate's 30 day
default is not an answer deadline. Every other node, including a native
`backend="api"` agent (also the default when no backend is set) and a `tab`
prompt on it, is `ExecutorEnforced`: the driver's timer counts active work
only, stops while the step has a question pending with the host, and resumes
with the remaining time when the last pending question is answered. A
sibling's question never extends another stage's budget. On expiry the driver cancels the step (for a native agent,
through Pebble's prompt cancellation token; Pebble's own wall-clock timer stays
unset). The attempt is `timed_out` and a retry gets a fresh budget.

Run policies: `stall_timeout` (default 30 m; `0s` disables) is the stall
watchdog's budget, and `loop_restart_signature_limit` (default 3, at least 1)
is the failure circuit breaker's limit. Both lower to the graph's
`RunPolicy`; the host enforces them (see "Watchdog and circuit breaker").

Retries: `max_retries` (default `default_max_retries`, default 0) or a
`retry_policy` preset (`none`, `standard`, `aggressive`, `linear`, `patient`)
becomes `RetryPolicy`. Only a failure the step classed `retry_requested` is
retried: Fabro's retry intent is a flag on the outcome, never a status. The
engine's exhaustion stays `Fail` for every Fabro node: what the last retryable
failure becomes is the step's decision under `on_retries_exhausted` (see
"Failure policy"), made on the final attempt with the node's explicit routes
in hand. `allow_partial=true` is Fabro's spelling of
`on_retries_exhausted="partially_succeed"`.

## Routing

Each node's outgoing edges become one routing group with `SelectionPolicy::Tiered`
and Fabro's four tiers, `Fallthrough::NoEmit` (no match is a normal end):

| Tier | Candidates | `when` | pick |
|---|---|---|---|
| 1 | edges with a `condition` | the lowered condition | `selection`: `HighestWeightThenLexical` (default) or `WeightedRandom` |
| 2 | unconditional edges with a `label` | `normalize_label(output.preferred_label) == "<label key>"` | `First` |
| 3 | unconditional edges | `index_of(output.suggested_next_ids, "<target>") != null`, ranked by that index | `LowestRankThenArmOrder` |
| 4 | unconditional edges | the failure policy (below) | `selection` |

Weights are Fabro's: highest wins, ties break on the lexical target id, and a
negative `weight` deprioritizes an edge. The engine's weights are unsigned, so a
node with a negative weight has every weight shifted up until the lowest is
zero (order and ties unchanged). Under `selection="random"` a weight at or below
zero counts as one, as Fabro counts it.

Labels: the accelerator prefix (`[Y] Yes`, `Y) Yes`, `Y - Yes`) is stripped on
both sides before the engine's `normalize_label`. `selection="random"` with a
conditional edge is `attractor.random_with_conditions`, as in Fabro. `loop_restart=true`
is `EdgeTransition::Restart`: the execution ends and a successor starts at the
target with empty context.

### Conditions

| Condition | Petri expression |
|---|---|
| `outcome=succeeded` / `partially_succeeded` / `skipped` | `status == 'success'` / `'partial_success'` / `'skipped'` |
| `outcome=failed` | `failure() || cancelled() || timed_out()` |
| `outcome=success` | read as `outcome=succeeded` with the warning `deprecated.outcome_alias`, until 2026-10-04 (Fabro itself never matches it) |
| `outcome=<anything else>` | `unsupported.outcome_value`: domain signals ride `context_updates` |
| `preferred_label=X` | `to_string(default(output.preferred_label, '')) == 'X'` |
| `context.K=X`, bare `K=X` | `to_string(default(get(kv, 'K'), '')) == 'X'` — Fabro's text comparison |
| `K` (bare) | non-empty, not `"false"`, not `"0"` |
| `K > 5` and friends | both sides numeric, else false (`loose_*` builtins) |
| `K contains X` | array element equality, else substring |
| `K matches re` | `matches(...)`; the pattern is validated at load |

### Failure policy

Two attributes, one value set — Fabro's `route`, `exit`, `succeed`, plus
Petri's `partially_succeed` — and the specific one wins:

- `on_failure` decides a non-retryable failure; `on_retries_exhausted` decides a
  retryable one that ran out of attempts (`allow_partial=true` spells the latter's
  `partially_succeed`).
- `route`: the unconditional edge is taken. `exit`: it is guarded `!failed`, so
  the run quiesces and fails under `TerminalNode`.
- `succeed` promotes a failure the way Fabro's executor does. The step first
  checks the node's explicit routes against the failed outcome and the
  prospective context (the run context with the stage's own updates applied):
  a conditional edge whose condition holds, a preferred label naming a
  labelled edge, or a suggested target naming an edge. A failure an explicit
  route matches stays a failure and routing takes that route. Any other
  non-retryable failure is promoted: the record is a `PartialSuccess` with the
  failure kept in `underlying`, `output.promoted` says so, the reported
  `output.outcome` is `succeeded`, and the node's `outcome=succeeded`
  conditions match it while `outcome=partially_succeeded` does not, as Fabro
  shows its conditions. The event log never records a clean success for a
  failed step. `auto_status=true` is the deprecated spelling
  (`deprecated.auto_status`, Fabro's `auto_status_deprecated` rule) and is
  ignored when `on_failure` is set.
- `partially_succeed` is a Petri extension (`fabro.petri_extension` names it):
  the same promotion check and the same `PartialSuccess` record, but the
  reported outcome is `partially_succeeded`, so `outcome=partially_succeeded`
  conditions match the promoted stage. Fabro's validator refuses the spelling
  (`on_failure_valid` accepts `route`, `exit`, `succeed`), so a workflow that
  uses it runs on Petri only; the oracle case
  `partially_succeed_policy_classifies_before_routing` records that
  rejection beside Petri's result, and `crates/fabro/acceptance/CONTRACT.md`
  lists the spelling under accepted differences.
- A retryable failure with an attempt left is returned failed for the engine
  to retry. On the final attempt (`StepCtx::is_final_attempt`) it is the
  stage's outcome and `on_retries_exhausted` decides it, in Fabro's order
  (`finalize_retries_exhausted`, then `apply_succeed_policy`):
  `partially_succeed` (`allow_partial`) accepts it as a `PartialSuccess` with
  no route check, reporting `partially_succeeded`; `succeed` checks the
  explicit routes against the failed outcome first and promotes only an
  unmatched failure, reporting `succeeded`, so an `outcome=failed` edge still
  recovers from an exhausted gate and an `outcome=succeeded` edge matches a
  promoted one; `route` and `exit` leave it failed. The step's config carries
  both policies and the routes; the failure's `retry_requested` class stays on
  the record, which is how the fallback tier knows which policy governs it
  (acceptance `exhaustion.rs`, black box
  `an_expired_gate_under_succeed_takes_its_explicit_failure_edge`).
- A human gate never falls through on failure, whatever the policy.

### Goal gates and loops

`goal_gate=true` nodes lower to a `goal_check` noop in front of `exit`: for each
gate (in id order) an arm guarded by `!default(nodes.<gate>.success_like, false)`
routes back to the first existing retry target of the node's `retry_target`, its
`fallback_retry_target`, the graph's, the graph's fallback; a gate with no target
ends the run failed; the last arm reaches `exit` when every gate passed.

A depth-first search from start marks every cycle-closing edge `back`; every
node forward-reachable from a back edge's target gets a finite
`Budget.max_firings`: `max_visits`, else `max_node_visits`, else 500 (Fabro's
unlimited, with one `info.budget.default` note). A value above 500 is refused.
Every node except a fan-in joins with `Any`.

## Parallel

A `component` node is a fork. Petri runs each branch the way Fabro does: as a
child invocation of a copy of the branch target, started from a snapshot of
the parent context at the fork and never merged back. Parallel workflows
therefore need the coordinator path (`host::run_configured`, the CLI, an
embedding host); `Runtime::run` alone has no child invocations.

The parallel node itself is the `attractor/fork` step. It runs once per visit,
before any branch, and takes the fork snapshot: the parent's `kv` and, when
a branch target is an agent or prompt node, the parent's stage records. It
offloads the `for_each` source list at any size and every other snapshot
value above 4 KiB (`attractor_steps::blobs::FAN_OUT_OFFLOAD_THRESHOLD`) to the
run's output store, so the snapshot holds `blob://sha256/<hex>` references
in their place, one blob per value for the whole fork; the parent's own
`kv` is not changed. Its output is `{ snapshot, nodes }`, which the branch
delegates read as their `kv` and `nodes`, and which every clone of a
`for_each` template receives as its input token. A branch child's request,
its `InvocationDeclared`, `ExecutionDeclared` and `InvocationFinished`
records, its own `ExecutionStarted` context and the parent's per-clone
tokens are therefore bounded by the threshold, not by the item count. A
snapshot key a branch's own graph reads by expression (the source list of a
`for_each` nested inside a static branch) is named in the fork's `inline`
list and stays inline. With no output store the fork keeps every value
inline.

Lowering replaces every branch target with a synthetic `attractor/branch`
delegate in the parent graph (`meta.kind = "parallel.branch"`,
`meta.branch = { fork, target, index }`, `synthetic: true`, one attempt, no
retry). The delegate's config names the child graph digest, the fork, the
branch index, the target's kind and the fork snapshot of `kv` (from the
fork's output). The child
graph is the branch target alone with its routes removed, entered through
`meta.branch_role`, with `result = NodeOutput(<target>)`; its digest is
pushed with the parent's graph. An agent or prompt target reads the branch's
`nodes` and item data from the snapshot (`internal.parallel_nodes`,
`internal.parallel_item`) and runs with `branch: true` (the branch fidelity
rule). A branch target that is the start, the exit, or a fan-in is
`attractor.parallel.bad_branch_target`. The same target named by two edges runs
twice, as `<target>` and `<target>.branch<index>`. A nested `component`
inside a branch is lowered first, innermost out, so an outer branch's child
graph carries the inner fork whole.

Branch edges never route. The delegates route to the fork's collector: the
common direct successor of every branch (an inner fork counts as its own
join). A `tripleoctagon` there is the `attractor/fan_in` step. A plain successor
gets a synthetic `<fork>.fan_in` in front of it. No common successor is
`attractor.parallel.no_join`; an edge that leaves a branch elsewhere is
`attractor.parallel.branch_edge_ignored`. Every delegate's edge into the collector
carries `{ index, value }`, so the `All` join sees the results in branch order.
The collector publishes a result list above 4 KiB as a reference, before it
reaches the parent's `kv`, so a later fork snapshots the reference and not
the list; `stdin_source`, a prompted fan-in and the agent and prompt
preambles read the list back through the store.

The collector publishes `parallel.results`, the array of branch envelopes
`{ id, index, item_label, status, context_updates }` in branch order, and
`parallel.branch_count`. `status` is the branch's final stage outcome (a
cancelled child is `failed`). `context_updates` is the branch's own change
set: every public key whose final value differs from the fork snapshot, with
`internal.*`, `graph.*`, `thread.*` and `current*` excluded. Branch context is
output only; nothing merges into the parent `kv`. The fan-in's own outcome
follows Fabro's aggregation: all succeeded (or no branches) is `succeeded`,
all failed is `failed`, anything else is `partially_succeeded`. An empty
array of results is the failure `No parallel results to join`
(`no_parallel_results`); a fan-in that receives no branch at all is
`attractor.parallel.no_branches` at load. `stdin_source="context.parallel.results"`
on a later command reads `get(kv, 'parallel.results')`; a command placed
before any fan-in warns `upstream_fan_in`. The same publication and stripping
happen in a prompted fan-in before its model call.

`for_each="context.K"` names one template branch. The template's expansion
evaluates `get(kv, 'K')` from the context itself, never from the fork's
output (Fabro's 1000-item cap is a precondition on the fork, `fail_fast:
false`), and each clone carries one item as `item`.
The item reaches the model after the prompt as fenced untrusted data: a
notice, then the item's JSON inside `<untrusted-<16 hex>>` tags whose tag is
derived from the item and never appears in it. `item_label` is the item's
`name`, else `label`, else its index, sanitized to 80 characters. An empty
list fires the template once with the IR's placeholder item
(`{"$placeholder": true}`, `ir::placeholder::PLACEHOLDER_ITEM_KEY`); the
fan-in strips placeholders and joins zero results, so no model call happens,
and the placeholder clone is no branch in the public event stream (the fork
starts and closes with zero branches). `for_each` inside a `for_each` branch
is `attractor.for_each.nested`.

`max_parallel` bounds the fork's live children per fork occurrence: a
missing, non-integer or negative value is 4 (`attractor.max_parallel.normalized`),
zero is 1. Every branch child is declared at once, so its call site, its
durable `InvocationDeclared` record and its place in the invocation count are
fixed at the fork. The child's engine starts only when the fork has a free
slot, in declaration order, and the child keeps the slot until its engine
ends. A branch waiting out a retry backoff holds none, so a queued sibling
runs during the backoff; the live count exceeds `max_parallel` only by the
branches between attempts. A branch cancelled while it waits still starts,
under the bound, and finishes as cancelled. The slots are one gate per parent
execution and fork visit (`AttemptAdmission` on the child invocation). On
resume the gate is rebuilt from the coordinator log, and every declared but
unfinished branch queues again under the same bound. The fork step's output
names the fork occurrence (`occurrence = { fork, firing }`, this visit of the
parallel node); every branch child's call slot is
`branch:<fork>@<firing>:<index>:<target>`. Branch steps report
`attractor.parallel.branch.started` once the child's engine has started and
`attractor.parallel.branch.completed` on every path a branch ends (with the
envelope's `status`, a `disposition` of `completed`, `cancelled`, `killed` or
`failed_to_start`, and whether the child ever `started`); the fan-in reports
`attractor.parallel.completed` when it runs. All three carry the occurrence and
are `StepEvent::Custom`; `crates/core/execution/EVENTS.md` ("Fork closure")
maps them to Fabro's `parallel.*` events beside the typed `fork.completed`
that closes a cancelled or killed fork.

Every Fabro run has a hard ceiling of 10,000 invocations, root and all
children counted, finished ones included (`RunPolicy.max_invocations`, set by
lowering; `execution::MAX_INVOCATIONS`). It cannot be raised or disabled; a
lower value is allowed. The coordinator refuses the 10,001st declaration with
`InvokeError::InvocationLimit { total, limit, parent, firing, slot }`, refuses
to create or resume a run whose policy asks for more, and the count survives
a resume. Other dialects keep the coordinator's 1,024 default.

## Nested workflows

`stack.child_workflow` (a path; `fabro/…` stands for `.fabro/…`) or
`stack.child_dot_source` (inline DOT) is lowered with the parent, to at most
three levels, with cycles refused. The child graph is registered before the
run starts. The `attractor/workflow` step follows Fabro's manager loop: it starts
the child once per manager attempt, at one durable call site (the node id), so
a re-dispatch of the same attempt after a crash reattaches to the child it
declared instead of starting another; a later attempt starts a fresh child.
It then polls: every `manager.poll_interval` (45 seconds when unset) it
evaluates `manager.stop_condition` against the parent's public context with a
reference success outcome (`outcome=succeeded`, no preferred label). A
satisfied condition cancels the child and the node succeeds with no context
updates; `manager.max_cycles` polls without child completion cancel the child
and fail the node (`max_cycles`). A child that completes first returns its
status, its failure (message and class) when it failed, and every public key
it changed (`internal.*`, `graph.*`, `thread.*` and `current*` keys excluded).
`manager.max_cycles` normalizes as Fabro does: missing, non-integer or negative
is 1000 (a warning names the bad value), zero is 1. A 1,000-poll manager
consumes one child invocation. The child inherits the parent's sandbox and
secrets; the parent's cancel cancels it. A host that runs Fabro steps outside
the coordinator registers `attractor_steps::workflow::ChildInvoker`.

## Steps at run time

- **`attractor/command`** runs the script in bash (`language="python"`: `python3 -c`)
  with stderr merged, with the config's `env` (secret references resolved at
  spawn), feeds `stdin_source` through the process's stdin (an output
  reference is read back through the store first), records the output in
  `output.stdout` and `command.output`, and with `output_schema="routing"`
  reads the last JSON object of the output as the routing directive
  (`outcome`, `preferred_next_label`, `suggested_next_ids`, `context_updates`,
  `failure_reason`). Output above 100 KiB leaves the record for the output
  store (below); the in-memory cap is 8 MiB. No output byte is lost
  silently: a line up to 1 MiB (the executor's line cap,
  `executor::lines::LINE_CAP`) reaches the step whole, so a long line takes
  the output to the store whole; a longer line is cut at the cap and ends
  with ` …[line truncated: N bytes dropped]`; the in-memory cap discards
  the front of the output behind `… [output truncated]`. Every byte either
  cap discarded is counted on the attempt's metrics as
  `output.dropped_bytes`, with `output.truncated_lines` for the lines the
  line cap cut, both present only when not zero; a capture that ended on
  silence after the script was gone records `output.incomplete: true`
  beside its marker, since nobody counted that loss.
- **`attractor/prompt`** is one model call through the application's `lithos-llm`
  client (the `PebbleClient` capability), with no tools and no coding-agent
  loop: the goal, the preamble of earlier stages at the node's resolved
  fidelity ("Fidelity and threads" below; `full` has no preamble and a prompt
  node never continues a conversation, so it reads as `summary:high`), the
  branch results for a prompted fan-in, the node's prompt and the output
  contract, as one user message. `project_memory` (default `true`) prepends
  the project instruction files of the working directory alone, selected by
  the model's agent profile as for an agent node, as a system message;
  `project_memory=false` reads none. `model` (the node's own, else
  `--model`/`--provider` at launch, else `default_model`, else
  `[run.model] name`, else a host default) is required;
  `provider` qualifies it, and a provider with no model runs the provider's
  catalog default; `reasoning_effort`,
  `speed` (`standard`, `fast`) and `max_tokens` ride the request; a JSON
  response format is requested when the catalog row offers it. A
  response that misses the contract gets a repair turn (the failed reply and
  the repair message appended), up to `output_retries` times (default 2), then
  fails `bad_output`. The result writes `response.<node>`, `last_response`
  (the first 200 characters), `last_stage`, then the routing fields or
  `output.<node>`. Two `StepEvent::Custom` payloads carry what a host maps
  onto Fabro's `stage.prompt` and `prompt.completed`: `kind = "attractor.prompt"`
  (`node`, `firing`, `attempt`, `model`, `prompt`, `sources`) before the first
  call, and `kind = "attractor.prompt.completed"` (`node`, `firing`, `attempt`,
  `model`, `outcome`, `response`, `calls`, `repairs`, `usage`,
  `duration_ms`) after the last. Metrics: `prompt.calls`, `prompt.usage`.
  `usage` is the calls' sum as a lithos-llm `Usage` ("Usage" below).
- **`attractor/agent`** assembles the prompt from the goal, the preamble of
  earlier stages at the node's resolved fidelity, and the node's prompt
  ("Fidelity and threads" below). Both backends share routing, `output_schema`
  validation, `output_retries` repair turns, steering deliveries, and the
  interrupt control: a host stops the node's current model turn (the request
  in flight and the tool calls it is running), the session stays open, and
  the node continues with its next input, the interrupt's text if it carries
  one, else the next steer delivered to it. The node reports the stopped
  turn as `kind = "attractor.turn.interrupted"` (`node`, `firing`, `attempt`,
  `backend`, `session`). A node with no turn in flight refuses the control.
  Each attempt starts a fresh agent session unless the node continues a
  retained thread at `full` fidelity (native backend only). Repair turns keep that
  session's history. `speed` and `max_tokens` configure the native model
  request; on ACP they are observer metadata like the other model settings.
  `backend="api"` is the default, as Fabro's `select_run_backend` picks the
  native agent for a node that names no backend (the pinned bundles name
  none). It runs the Pebble Rust library in Petri. `model` (the node's own, else
  `--model`/`--provider` at launch, else graph `default_model`, else
  `[run.model] name`, else a host default; a provider with no model runs its
  catalog default) is required.
  `backend="acp"` starts
  the Agent Client Protocol command from `acp.command` / `acp.config` (node,
  graph, then `PETRI_ACP_COMMAND`). The ACP command owns model selection;
  model settings are observer metadata. What a real product needs, and what
  the client gives it, is under "ACP products" below. `provider` (or `default_provider`) qualifies the
  model selector, and `reasoning_effort` configures the actual model request.
  The node's backend overrides the graph's backend; model stylesheets can also
  select it. Graph ACP configuration applies only to ACP nodes. Setting ACP
  options directly on an API node is an error. An ACP turn that fails after
  the agent started (the process exits before the protocol completes, a
  protocol error, a rejected request, a stop reason other than `end_turn` or
  `refusal`, or a turn that outlives the node's `timeout`) is classed
  `retry_requested`, as Fabro's retryable handler error
  is, so `max_retries` and `retry_policy` apply to it; a node with no attempts
  left fails and routes on `outcome=failed` as before.
- **`attractor/human`** asks through the core `Question` event and routes on the
  delivered answer. The host's interviewer answers: `petri run --interactive`
  from the terminal, `--auto-approve` with the first choice,
  `--interview-script <file>` from a script (see the README's terminal path
  section). A `question_type="multi_select"` answer names several choices
  (`Answer::choices`, the `option_keys` of Fabro's `multi_selected` answer);
  the first routes, and `human.gate.selected` / `human.gate.label` record every
  selected key and label joined by `,` and `, `, as Fabro does. Every answered
  gate also records `human.gate.<node>.question`, `.answer` and `.label`. A
  `sensitive=true` gate's free text crosses as a `$secret` reference, which is
  a Petri extension. An answer marked `cancelled` (the interviewer failed, or
  the wait was cancelled) fails the gate closed with class `interrupted`. A
  delivered steer (`{"$steer": ...}`) is not an answer: the gate ignores it
  and keeps its question open.
  - What a host shows beside the question. Each choice carries its edge's
    `human.description` (what choosing it means) and `human.preview` (a
    sample of what it would do) as the option's `description` and `preview`;
    both are Petri extensions to the edge attributes, optional, and a blank
    value is the same as none. The question's `context` is the previous
    stage's response (`response.<last_stage>`, trimmed, when `last_stage`
    names a stage whose response has text), as Fabro's gate shows it; an
    offloaded response shows as its reference text. A native agent's
    question carries Pebble's own option descriptions and previews the same
    way and no context.
  - `timeout` is the answer deadline. An unanswered question expires in the
    step, which reports the expiry on its progress channel first
    (`parsed.expired` on the `step.progress.recorded` event, `timed_out` with the default
    taken in the interview receipt, as Fabro emits `InterviewTimeout`): with
    `human.default_choice="<target or key>"` the gate takes that choice and
    records `timeout` as the answer; without one it fails with Fabro's retry
    outcome (class `retry_requested`), so `max_retries` asks again and
    `on_retries_exhausted` decides after that. The question carries
    `timeout_ms` so a host can show the deadline.
  - `review_target=true` reads `review_target` from the run context
    (`{"label", "url", "kind"}`, as an earlier stage's `context_updates`
    wrote it), validates it as Fabro does (a non-empty label of at most 200
    characters, an absolute `http`/`https` URL of at most 2048 characters with
    a host and no credentials or `<>|` characters), asks Fabro's sentence
    `Review the <label> <kind>, then choose the next action.` with the
    reference attached to the question, and logs `review: <label> <url>`. A
    missing or invalid target fails the gate before anyone is asked, with
    Fabro's message and class `review_target`; the refused URL is never
    repeated.
- **`attractor/wait`** sleeps, cancel-aware.
- **`attractor/workflow`** is the nested invocation above.
- **`attractor/stage`** is `start`, `exit` and a conditional: it returns its
  config as its output, as `noop` did, and records the scope's environment so
  sandbox-placed hooks can run. The root `start` also checks the repository
  out (`[run.clone]` above) and writes `internal.run_id`, the run's identity
  Fabro sets at run creation (the run directory's name in the standalone
  runner), which a `stdin_source="context.internal.run_id"` node reads. `start` drives the run-level hooks
  `sandbox_ready`, `run_start` and its own `stage_start`, in Fabro's order.
  `run_complete`, `run_failed` and `sandbox_cleanup` are not a stage's: the
  driver reports the run's end (by its final status, as Fabro's `on_run_end`
  does: `run_complete` for success, `run_failed` with the failure reason for a
  failure, neither for a cancelled run) and each scope's release, with the
  sandbox still in place, and the local hook service runs them there, in that
  order. A blocking hook that blocks at `start` fails the stage with class
  `hook_blocked` and ends the run; a skip at `start` records a skipped stage
  and the run goes on.
- **Output references.** A stage value whose serialized form is above 100 KiB
  (Fabro's offload threshold; a scalar never, a string by its JSON size) does
  not stay inline in the context or the event log. The step writes it to the
  run's output store and records `blob://sha256/<hex>` in its place (a
  structured value carries a `#json` suffix so it parses back). `stdin_source`,
  a prompted fan-in's branch results, and the agent and prompt steps' own
  updates read the logical value back through the store; a route or a
  condition that reads such a key sees the reference text, as Fabro's edge
  selection does for every key but `command.output`. The store is the
  `attractor_steps::OutputStore` capability: `attractor_steps::register` installs a
  `LocalBlobStore` under `<run_dir>/blobs` unless the host registered its own
  before the run, so a Fabro host replaces it with platform storage without
  changing node semantics. The store is content addressed, so a resumed run
  reads the same references. Two Petri-only rules use the same store below
  Fabro's threshold, for the values a fan-out multiplies: a fork offloads
  what it would otherwise copy into every branch child (the `for_each`
  source list at any size, every other snapshot value above 4 KiB), and a
  fan-in publishes `parallel.results` above 4 KiB as a reference. Readers
  that show Fabro's view of the context (the agent and prompt preambles, a
  nested workflow's starting context) put back every reference whose blob is
  at most 100 KiB, which only these rules create, so the model sees the
  value Fabro's model sees; a route or a condition still sees the reference
  text. `petri inspect` shows the references as recorded; they resolve under
  `<run_dir>/blobs/<hex>`.
- **`petri run --dry-run`** is the stub registry: every stage succeeds, a human
  gate takes its first choice, as Fabro's `--dry-run` does. A dry run touches
  no provider: every scope is acquired on the simulated provider, whatever
  backend the workflow's environment selects, so no sandbox plugin is needed
  and no workspace exists. Its `scope.acquired` and `scope.released` records
  say `provider: simulated`.

### Fidelity and threads

Fabro's `fidelity` decides how much of the run so far an LLM node hears:
`full`, `truncate`, `compact`, `summary:low`, `summary:medium`,
`summary:high`. Any other value is `attractor.bad_fidelity` at load. The node's
mode is resolved when it fires: the incoming edge's `fidelity`, else the
node's, else the graph's `default_fidelity`, else `compact`. The preambles
are deterministic text built from the run context with no model call, as
`fabro-workflow`'s `preamble.rs` writes them: `truncate` is the goal and the
run id; `compact` the nested bullets of every completed stage (script,
prompt, output tail, context keys); `summary:low` and `summary:medium` the
last two and five stages; `summary:high` the per-stage report with a context
table. A stage value above 8 KiB is shown as a preview. `full` sends no
preamble: the node continues its thread's conversation.

A thread is resolved the same way: the edge's `thread_id`, else the node's,
else the graph's `default_thread`, else the node's first class, else the
previous node's id. `thread_id` on a node or edge without effective `full`
fidelity is `attractor.thread_id_requires_fidelity_full` at load. The first
node of a parallel branch has no thread and an explicit `full` reads as
`summary:high` (Fabro's branch rule); a node whose thread's conversation was
discarded (its predecessor on the thread failed, or the run resumed) also
reads `full` as `summary:high`, once, and starts the thread again. This is
Fabro's own rule for a restart: its `AgentApiBackend` keeps full-fidelity
sessions in an in-memory map per worker, so a resumed node runs from the
`summary:high` preamble there too. Petri does not persist retained threads
across `petri resume` for the same reason.

Recovery identifies previously used threads from their recorded thread
events, even when the interrupted stage was a command. The first successful
agent on a lost thread retains its rebuilt conversation, so later agents on
that thread return to `full` fidelity. A thread that was never used before
the restart still starts fresh.

The native backend retains a successful node's conversation per thread for
the run (`attractor_steps::sessions::SessionService`, one per invocation). A
later node at effective `full` on the same thread resumes it from Pebble's
export with its own event sink, question handler, tool hooks and metrics
bound; a node that names another model than the retained conversation's warns
and continues it on the retained route. A failed node discards its session,
as Fabro does. ACP never reuses a session. Each resolution is a
`StepEvent::Custom` with `kind = "attractor.thread"` (`node`, `firing`,
`attempt`, `fidelity`, `fidelity_source`, `thread`, `thread_source`,
`reused`, `backend`).

### Hooks

`[[run.hooks]]` entries run in the standalone runner through
`attractor_steps::hooks::LocalHooks`, the `execution::hooks::HookService` the
Fabro component installs (`crates/core/execution/HOOKS.md`). One service
serves every point, and every caller reaches it as the `HookServiceHandle`
capability, so a hook runs once whoever drives it and a replacement service
receives every point: the engine's `HookAdapter` at the per-firing points,
the `attractor/stage` step at the root `start` for `sandbox_ready`, `run_start`
and the start stage's own `stage_start` (the driver admits `start` before
its sandbox exists, so the step asks once the sandbox is there), the fork
and fan-in steps for `parallel_start` and `parallel_complete`, the native
agent's Pebble `ToolMiddleware` at the tool boundary, and the ACP client's
permission requests. A host that installs its own `Runtime::hooks` and
`HookServiceHandle` before `attractor_steps::register` runs keeps them; the local
service is then not installed.

Fields: `id` (merge identity), `name`, `event`, `matcher`, `blocking`,
`timeout` (`60s` default; `30s` for prompt hooks), `sandbox` (default
`true`), and one transport: `script` or `command` (a command hook), `url`
with `headers` and `tls = "no_verify"` (an HTTP hook), `prompt` with `model`
(a prompt hook, default model `haiku`), or `agent = "enabled"` with `prompt`,
`model` and `max_tool_rounds` (an agent hook, default 50 rounds). The rounds
are a hard bound, as in Fabro's loop, not advice to the model: the hook's
agent may ask for tools in at most that many model turns (Pebble's
`with_max_tool_rounds`, set one below, so the turn Fabro would run and then
discard is refused without running its tools), and a turn past the bound that
asks for tools again ends the hook, which fails open with the reference's
warning `agent hook exhausted max tool rounds, proceeding` and its usage and
events on the record; `max_tool_rounds = 0` proceeds without a model call, as
Fabro's empty loop does. Events, as
Fabro names them: `run_start`, `run_complete`, `run_failed`, `stage_start`,
`stage_complete`, `stage_failed`, `stage_retrying`, `edge_selected`,
`parallel_start`, `parallel_complete`, `sandbox_ready`, `sandbox_cleanup`,
`checkpoint_saved`, `pre_tool_use`, `post_tool_use`,
`post_tool_use_failure`. Every event is validated, merged and dispatched at
its reference phase: `parallel_start` fires once per fork visit before the
branches, from the fork step (the parallel node itself, so a `for_each` fork
counts), `parallel_complete` once every branch is in, from the fan-in itself,
both naming the parallel node; `run_complete`/`run_failed` at the run's end by its
status and `sandbox_cleanup` at the scope's release, both with the sandbox
still there. `checkpoint_saved` warns `fabro.hooks.checkpoint_saved` at
load and never runs (the standalone runner makes no checkpoints). Fabro's rules apply: `matcher` is
an unanchored regex tested against the node id, handler type, edge ends and
tool name the event carries; `run_start`, `sandbox_ready`, `stage_start`,
`edge_selected` and `pre_tool_use` are blocking by default and the rest are
not; a non-blocking hook still runs and its decision is ignored. Decisions merge as block over skip or override over
proceed, the first of a rank winning, and a block ends the sequence. The
reference never dispatches `stage_retrying`; Petri does, at the engine's
`Retrying` point, and consumes no decision from it.

Placement: a `sandbox = true` hook runs `sh -c` in the firing's scope with
the scope's environment; `sandbox = false` runs `sh -c` on the host, with the
scope's workspace as cwd when the scope is a host directory. Both see `FABRO_HOOK_CONTEXT` (the path of the
JSON context: `event`, `run_id`, `workflow_name`, `cwd`, `node_id`,
`node_label`, `handler_type`, `status`, `edge_from`, `edge_to`,
`edge_label`, `failure_reason`, `attempt`, `max_attempts`, `tool_name`,
`tool_input`, `tool_call_id`, `tool_output`, `error_message`), plus
`FABRO_EVENT`, `FABRO_RUN_ID`, `FABRO_NODE_ID`, `FABRO_WORKFLOW`. A command
decides by exit code: `0` proceeds unless stdout is a decision JSON
(`{"decision": "proceed" | "skip" | "block" | "override", "reason",
"edge_to"}`), `2` blocks unless stdout is one, any other code blocks. An
HTTP hook posts the context and reads the same JSON from a `2xx` body. A
prompt hook asks the model for `{"ok", "reason"}` and blocks on `false`; an
agent hook does the same with tools in the sandbox. HTTP, prompt and agent
hooks fail open on errors and timeouts; a command that times out is killed
and, when blocking, blocks. An agent hook that runs out of time while a tool
is running stops that tool (TERM, the scope's grace, then KILL) and joins its
agent before it fails open, so the stage it guarded starts with nothing of
the hook still running. A hook whose placement is unavailable (a sandbox
hook before any scope exists, a model hook with no client) is recorded as
`unsupported` and proceeds. Cancelling the firing cancels the hook; an agent
hook's tool and agent are stopped and joined the same way, even though the
cancelled firing no longer waits for them. Hook output reaches the run log
through the same event pipeline as every other step output, so Petri's
secret masking applies to it. What a prompt or agent hook spent is on its
record (`usage` on the hook's entry of the `hook` note or `attractor.hook`
event: requests, tool calls, tokens, cost, timings), and every event an
agent hook's agent produced is recorded under the hook's identity as a
`hook.activity` note (`parsed.hook_activity` in the public stream), apart
from the stage's own agent activity and usage
(`crates/core/execution/HOOKS.md`, "Recording").

What a decision does: `stage_start` `skip` skips the node, `block` fails it
with class `hook_blocked`; `edge_selected` `override` routes to `edge_to`
when it names an edge out of the node, `block` fails the transition;
`pre_tool_use` `block` denies the tool call (the tool never runs and the
model sees the reason); every other event's decision is recorded and
ignored. Per-firing reports are `hook` notes on the firing
(point, decision, each hook's name, state, duration, message, and fail-open
warnings). Tool and run-level reports are `StepEvent::Custom` with
`kind = "attractor.hook"` (`node`, `firing`, `attempt`, `event`, `report`), and
an enforcement gap is `kind = "attractor.hook.warning"` (`backend`, `hook`,
`event`, `boundary`, `message`).

Tool hooks on the ACP backend map onto the two tool boundaries the protocol
offers, and both are best effort because the agent decides what it asks
and what it reports:

- `pre_tool_use` runs at `session/request_permission`, with the request's
  tool title as `tool_name`, its `toolCallId` and its `rawInput`. A block
  answers with the rejecting option (`reject_once`, else `reject_always`),
  so the effect does not happen for that call and the agent sees the
  denial. Otherwise the request is allowed: with the `allow_always` option
  when no `pre_tool_use` hook is configured (nothing needs to see the next
  call of that kind, as Fabro's client answered), and with `allow_once` when
  one is, so every later call of that kind asks again and the hook keeps
  running.
- `post_tool_use` and `post_tool_use_failure` run when the agent reports a
  tool call finished: a `tool_call_update` (or `tool_call`) with status
  `completed` carries the call's text content, else its `rawOutput`, as
  `tool_output`; status `failed` carries the same as `error_message`. The
  decisions are ignored, as Fabro ignores them.

A tool call the agent runs without asking (a permission mode that never
asks, a tool the agent treats as safe) is seen when it is reported running
or finished, and warned once per hook and tool as `attractor.hook.warning`,
naming the backend (`acp`), the hook, the event and the boundary
(`session/update`). Before the first prompt the node says, once per
configured tool hook, what that hook can see: the permission boundary for
`pre_tool_use`, the update boundary for the post-tool events. Fabro ignores
ACP tool hooks silently; the warnings are an accepted difference.

An interrupt on the ACP backend is `session/cancel` without ending the
process: the agent answers the prompt in flight with stop reason
`cancelled`, the client records `attractor.turn.interrupted`, and the
session's next `session/prompt` is the interrupt's text, else the next text
the host delivers. The interrupted turn's partial text is not the node's
answer. An agent that ignores `session/cancel` keeps the turn running until
it ends on its own.

### ACP products

The ACP client (`attractor_steps::acp`) speaks ACP 1 over the agent's
stdio and is complete against what Claude Code and Gemini CLI speak. Claude
Code has no ACP mode of its own; it speaks the protocol through the
`claude-code-acp` adapter (the `@zed-industries/claude-code-acp` package),
so the command is `acp.command="claude-code-acp"`. Gemini CLI speaks it as
`acp.command="gemini --acp"`. The command is started in the node's scope, a
host directory or a container, so the product must be installed where the
scope runs (a container image with the product on `PATH`).

The agent's environment is the scope's, plus:

- the workflow environment's secret references (the step config's `env`,
  which a frontend lowers from secret-valued workflow environment entries,
  such as Fabro's `[run.environment.env]`). These reach the agent however it
  is named: `acp.command`, `acp.config` or `PETRI_ACP_COMMAND`.
- the command's own `env` from `acp.config`, on top. A value is a string or
  a `{"$secret": "NAME"}` reference.

Each reference resolves through the run's secret provider (the standalone
runner reads `PETRI_SECRET_<NAME>`; Fabro its vault) and is masked in every
log. No other secret reaches the agent. That includes a product's API key
(`ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `OPENAI_API_KEY`) the provider knows
but the workflow does not name: a product signed in to a subscription can
bill an API key it finds in its environment instead, so the workflow names
the key when it wants the agent to use one, as it does in Fabro.

Every reference, the workflow's and the command's, resolves when the agent
starts. One the run cannot supply fails the node with class
`secret_unavailable` before the agent starts, whether or not the agent reads
it.

A native (`backend="api"`) agent's tool shells get the same workflow secret
references, resolved when its session opens, beneath the variables a tool call
sets itself; a reference the run cannot supply fails the node the same way.
Its MCP servers get only their own configured env.

The session opens in the scope's workspace (`session/new` with `cwd`). An
agent that answers `session/new` with `auth_required` (Gemini CLI, until
it has authenticated) is authenticated with the API-key method it
advertised at `initialize` (the first marked `_meta.api-key`, else the
first whose id says `api-key`), then asked again; an agent that advertises
no such method fails the node with the agent's own error. `session/prompt`
sends the node's prompt as one text block; the turn's text is the
`agent_message_chunk` text.

Every notification the agent sends is recorded on the public stream as the
backend envelope, `StepEvent::Custom` with `kind = "acp"`: `{ kind, node,
firing, attempt, scope, event }`, where `event` is `{ session_id, seq,
tool_call_id?, method, update }` for a `session/update` of any variant
(`agent_message_chunk`, `agent_thought_chunk`, `user_message_chunk`,
`tool_call`, `tool_call_update`, `plan`, `available_commands_update`,
`current_mode_update`, `usage_update`, and whatever a product adds),
`{ session_id, seq, method, params }` for any other notification, and
`{ session_id, seq, tool_call_id?, method, params, outcome, blocked }` for a
`session/request_permission` with the answer Petri gave and the blocking
hook's reason when one blocked. `seq` counts the envelopes of one agent
process; `tool_call_id` is the update's `toolCallId` when it names one.

Usage comes from the session usage extension (`unstable_session_usage`),
which Gemini CLI reports and the Claude Code adapter (0.16.2) does not: the
`usage` a `session/prompt` response carries (`inputTokens`, `outputTokens`,
`thoughtTokens`, `cachedReadTokens`, `cachedWriteTokens`) is summed over the
node's turns into `acp.usage.tokens`, and a `usage_update` notification sets
`acp.context` (`used`, `size`) and, when its `cost` is in USD, the session's
cumulative cost as `acp.usage.cost` (`usd_micros`, source `provider`).

The live tier `crates/petri/lib/tests/acp_products.rs` runs both products
on the host and in a container (`#[ignore]`; each cell skips itself without
the product's binary and credential).

## Native Pebble

```dot
digraph change {
  graph [backend="api", default_model="anthropic/claude-sonnet-4.6"]
  start [shape=Mdiamond]
  implement [prompt="Fix the failing tests, then run the test suite."]
  exit [shape=Msquare]
  start -> implement -> exit
}
```

The `petri` distribution supplies a lithos-llm client with the built-in model
catalog and environment credentials, such as `ANTHROPIC_API_KEY` or
`OPENAI_API_KEY`. It enables Anthropic, OpenAI, Gemini, and OpenAI-compatible
adapters. Applications that use `attractor_steps::register` directly must provide
`attractor_steps::pebble::PebbleClient(client)` through `Runtime::capability`.
The application owns the client's catalog, credentials, and retry middleware.

An application may also give every native session tools of its own: register
an `attractor_steps::host_tools::HostTools` capability holding builders of
Pebble `RegisteredTool`s. Each session calls the builders once with a
`HostToolContext` (the run key, invocation, execution, node, firing and
attempt of the stage) and passes the tools to Pebble beside its own. A host
tool then runs under the run's tool hooks, is recorded in the `pebble`
envelope under the stage, and reaches a sub-agent when the tool is marked
`allow_in_subagents`. The standalone runner registers none.

Tools use the firing's `ExecEnv`. Commands run as `bash -c` inside the scope;
files use the scope's filesystem. Bash, find, grep, and the usual file utilities
must be available there. Content search uses ripgrep when available and grep
otherwise. Searches fail explicitly when their captured output exceeds 4 MiB.
The backend reads the project instruction files Fabro's `discover_memory`
selects for the model's agent profile (the catalog's shared `metadata.agent`
namespace): `AGENTS.md` and `CLAUDE.md` for Anthropic models, `AGENTS.md` and
`.codex/instructions.md` for OpenAI, `AGENTS.md` and `GEMINI.md` for Gemini,
`AGENTS.md` alone otherwise; from the Git root down to the working directory,
root first, when the working directory is inside a repository. Pebble owns
the filenames, the walk and the loader (`MemoryDiscovery::from_git_root`,
the 32 KiB budget, deduplication and truncation); a prompt node reads the
working directory's files through the same discovery and loader
(`MemoryDiscovery::working_directory`, `ProjectMemory::load`), so its system
prompt is the text a session would load, the crossing file cut with Pebble's
marker. The backend
searches Fabro's skill directories (below, "Skills"). Every
session carries the run's tool hooks as Pebble middleware (`pre_tool_use`
denies before the tool runs; `post_tool_use` observes the outcome), and the
node's `speed` and `max_tokens`. Tools have full access within the scope's
policy. Petri's sandbox owns process isolation. This integration does not
install interactive approvals. Tool output is bounded by Pebble's capture and
preview limits. Omitted bytes are discarded and cannot be retrieved.

Every native session has Pebble's sub-agent tools (`spawn_agent`,
`send_input`, `wait`, `close_agent`), as every API-backend agent does in
Fabro, which has no setting to turn them off (the `[run.agent] subagents`
key is refused, as Fabro refuses it). The lowering puts the reference
configuration on every agent node (`frontend_attractor::subagents::SubagentConfig`:
`enabled = true`, `max_open_sessions = 4`, Pebble's bound on the sessions one
tree holds open at once, the node's own session included) and
`attractor_steps::subagents::configure` hands it to `CodingAgentBuilder::subagents`.
Pebble builds and owns the children: each child runs in the parent's scope on
the parent's model, under the same tool middleware, so the run's
`pre_tool_use` hooks block inside a child, and the workflow's MCP tools
(`[run.agent.mcps]`) reach a child through the parent's connection. A child
re-reads the parent's project memory files and re-discovers its skill
directories, as Fabro's child does (`SubagentOptions::with_inherited_memory`
and `with_inherited_skills`); it never gets the question tool; it may
delegate again within the open-session bound. `wait` blocks until the child finishes; a
child's failure is the parent's tool result and never fails the stage; a
cancelled wait closes the child; the session's shutdown closes every child
before the node releases its scope. Children are Pebble sessions, not
workflow invocations: they never count against the run's invocation ceiling.
Nothing of a child survives a retained thread's export or a resume; a later
`full` node continues the conversation with the child's result in it and may
delegate again. Sub-agents reach ACP agents through the agent's own tools,
not through Petri. Accepted differences from the reference are in
`crates/fabro/acceptance/CONTRACT.md`.

Sub-agent facts are the `pebble` events (below). Pebble publishes the
lifecycle (`SubAgentSpawned`, `SubAgentTurnStarted`, `SubAgentCompleted`,
`SubAgentFailed`, `SubAgentClosed`, each with `agent_id`, `depth` and
`generation`) under the parent's `session_id`; a child's own events carry
the child's `session_id`, its immediate parent in `parent_session_id`, and
the tree's one `stream_id` and `seq`. Every event of the tree is attributed
to the node, firing and attempt that owns the root session. Pebble's prompt
report excludes descendants, so the node's `pebble.usage` is the parent's
own and the `pebble.subagents` metric is the tree's: `{ spawned,
turns_started, completed, failed, closed, usage, sessions }`, where `usage`
(a `Usage`, "Usage" below) sums every descendant session's committed
assistant messages and `sessions` maps each child session to `{ parent,
provider, model, usage, messages, compactions }`. `provider`
and `model` are the route the child runs on, as its `SessionStarted` reported
it (or the model of its first answer), so a host prices the child's tokens at
the child's own model; both are null when the stream named neither. A child
compacts under the parent's settings; its
`CompactionStarted`/`CompactionCompleted` events carry the child's session,
and `compactions` counts them. A public consumer reconstructs the same
totals from the backend envelopes in `step.progress.recorded`
(`AssistantMessage` payloads of sessions with a parent).

`Control::Deliver` accepts a string or `{ "text": "..." }` and queues a
follow-up: a new user turn once the current answer is reached. Deliveries
ride Pebble's steering bus, one per node run, in its follow-up mode
(`SteeringBus::follow_up`); text delivered while the session is still being
built waits on the bus and reaches the session when it attaches, in the same
mode. A delivered core `Answer` naming one of the session's open
questions answers it instead (below). A delivered `Interrupt`
(`{ "$interrupt": { "steer"? } }`) stops the round in progress through the
bus: with text, `SteeringBus::interrupt_then_steer`, and the text opens the
next round; without, `SteeringBus::interrupt`, the prompt parks at its next
turn boundary, and the next text the host delivers is sent as steering
(`SteeringBus::steer`), which is what wakes it. Pebble reports the stop as
`RoundInterrupted` on its stream and the node adds
`attractor.turn.interrupted`; the session and its history survive, with the
cancelled tool calls answered as cancelled. Cancellation settles the active
prompt and shuts down its session.
Kill stops active tool processes immediately. A driver hard abort can discard
an unsettled prompt report; scope release remains responsible for cleanup.

Pebble events appear as `StepEvent::Custom` with `kind="pebble"`, firing,
attempt, scope, node, and the original event envelope. The envelope preserves
stream sequence, session, parent session, and tool-call identifiers. Petri's
secret masker applies before forwarding. These events use Petri's existing
log pipeline. A successful node's session is retained in memory for the run
by thread ("Fidelity and threads"); nothing is checkpointed, so a resumed run
starts every thread again.

Pebble puts no event on its stream that lists a session's tools, so the
backend records the list itself, once per session, as `StepEvent::Custom`
with `kind = "attractor.tools"`: `{ kind, node, firing, attempt, session,
tools }`, where `tools` is one entry per tool the model was offered, in
Pebble's order (by name): `{ name, description, source, category }`. `name`
and `description` are what the model sees; `source` is Pebble's
`ToolSource` as it reports it (`{"kind": "native"}`, `{"kind":
"application"}`, `{"kind": "mcp", "server_name", "original_name"}`,
`{"kind": "skill"}`); `category` is Petri's grouping: `builtin` (Pebble's
own tools and a skill's), `mcp`, `subagent` (`spawn_agent`, `send_input`,
`wait`, `close_agent`), `host` (a `HostTools` tool), `question` (the
question tool). The node's own session is listed once the agent is built,
from Pebble's snapshot, before the first prompt; each child session is
listed right after its `SessionStarted` envelope, with the tools Pebble's
inheritance gives a child, as Petri reads that rule: every built-in, skill
and MCP tool, a host tool the host marked `allow_in_subagents`, never the
question tool. A node that continues a retained thread opens a session of
its own and lists it again under the new node.

The session's question tool (`request_user_input` for GPT-5.6 and GPT-6,
`AskUserQuestion` for Claude) reaches the same interviewer a human gate does.
Petri implements Pebble's `HumanInputProvider`: each question in a batch
becomes a core `Question` on the step's progress channel, with id
`<node>#<firing>/agent/<session>/<tool call>/<index>`, `kind`
`multiple_choice` or `multi_select`, the harness's `option_N` keys, and
`freeform` set as Pebble allows. The delivered answer's choices (or free
text) go back to Pebble as that question's answers; a cancelled answer or a
cancelled prompt goes back as `cancelled`. The interview receipt records
these questions beside the workflow's own gates.

Attempt metrics include `pebble.prompts`, `pebble.usage`, `pebble.inference_ms`,
and `pebble.tool_ms`. They sum all settled prompt reports, including repair
turns, failed prompts, and cancellation. These metrics exclude the model
calls a tool makes. They include the compaction summary call, which Pebble
bills to the prompt that compacted; `pebble.compactions` and
`pebble.compaction_usage` break that share out (below, "Compaction").
`pebble.usage_by_model` breaks `pebble.usage` out by the route that spent
it: an array of `{ provider, model, usage }`, one entry per route the
node's own session spent usage on, in the order the routes were first used.
`provider` and `model` are named as a `pebble.subagents` session account
names them, the provider ID and the model ID apart (a `provider/model`
selector splits at its first `/`; `provider` is null when the stream never
named it). Each committed answer counts on the route in effect when it
landed, as `SessionStarted` (a fresh session or a resumed export) and
`RouteFailover` set it, and each compaction's summary call on the route
that ran it, so every entry is priced at its own route's model. The entries
sum to `pebble.usage`; a route that spent nothing has no entry, and a
session that used nothing reports `[]`. Descendant usage stays in
`pebble.subagents`. An ACP
node reports `acp.turns`, `acp.usage` (the session usage extension as
lithos-llm's `Usage`, "ACP products" above) and, once the agent reported
its context window, `acp.context` (`used`, `size`).

### Usage

Every `usage` Petri records, on its own payloads and metrics and on Pebble's
events alike, is lithos-llm's `Usage`: token counts with an optional cost.

```json
{ "tokens": { "input": 28640, "output": 8750, "reasoning": 1200,
              "cache_read": 4800, "cache_write": 1500 },
  "cost": { "usd_micros": 720000, "source": "catalog" } }
```

`tokens` holds five disjoint buckets; a total is their sum, which no field
carries. `cost` is absent when the value is not priced: an answer the
provider and catalog gave no price for, or a sum with any unpriced part that
used tokens. A present `cost` is therefore the whole value's cost, never a
subtotal. `source` is `catalog` (priced from the client's catalog),
`provider` (reported by the provider) or `application` (supplied by the
caller, or a sum whose parts differ in source). Sums are
`Usage::saturating_add`. The places that carry one: `pebble.usage`, each
`pebble.usage_by_model[*].usage`, `pebble.compaction_usage`, `pebble.subagents.usage` and each of its
`sessions[*].usage`, `prompt.usage`, `fabro.prompt.completed.usage`,
`fabro.compaction.usage`, a hook report's `hooks[].usage.usage`, and
Pebble's own `AssistantMessage`, `CompactionCompleted`, `CompactionFailed`
and `RouteFailover`. Before 2026-09-14 the five buckets were the top level
of `usage` and the cost a separate `cost_usd_micros` number (null when
unknown) beside it, with `pebble.cost_usd_micros`,
`pebble.compaction_cost_usd_micros` and `prompt.cost_usd_micros` as their
own metrics; those fields are gone.

### Model resolution at admission

Fabro resolves every model selector to a concrete provider and model against
the eligible providers when a run is created, and persists the result. Petri
does the same at `Runtime::check` when the runtime has a catalog: with the
`PebbleClient` capability installed, the admission pass
`attractor_steps::admission::ModelAdmission` (registered by
`attractor_steps::register`, an `AdmissionPass` of `petri-runtime`) resolves
every `attractor/agent` node on the API backend and every `attractor/prompt`
node (a `tab`, or a `tripleoctagon` with a `prompt`) exactly as the stage
would at its first firing: the provider's default model when the node names
a provider alone, the canonical route the `model` and `provider` resolve
to, the `[run.model.fallbacks]` chain keyed by that model, and the
reasoning effort mapped per target. The result is written on the node's
config and the persisted graph is what dispatch and resume read, so the
choices are frozen with the run whatever the catalog says later (an alias
that moves, a default provider that changes, a provider that leaves the
eligible set). `petri run` admits this way when the client builds; `petri
check` and `petri run --dry-run` lower on the simulated runtime, which has
no client, so their graphs keep the selectors.

What the pass writes: `model` and `provider` become the original route's
catalog model id and provider id; the `fallbacks` table is removed; and
`plan` is the frozen plan,
`{ original: Route, remaining: [Route], notices: [{code, level, message}] }`
with each `Route` a concrete `{provider, model, reasoning_effort?, speed?}`
and the notices the resolution produced. The node's `meta` keeps `model`,
`provider` and `reasoning_effort` as written: it is the frontend's display
record. The `start` stage's copy of the table is checked here and removed
too, so `start` does not check a table already admitted against a catalog
that may have changed since. Pre-lowered children (parallel branches,
nested workflows) are resolved with the root; a changed child's digest
moves and `Runtime::check` rewrites the reference to it. A nested workflow
the workflow step lowers during the run is not admitted here and resolves
at its stages.

What refuses the graph, each an error diagnostic on the node's span (the
file's, for the table):

| Problem | Code |
|---|---|
| a `model` or `provider` no available provider offers, a provider not in the catalog, a provider named alone with no default model, or a chain key or reference that names a model or provider the catalog does not know | `attractor.model.unknown` |
| a `[run.model.fallbacks]` table that is malformed, a reference that does not parse, a key that names a provider or a provider-qualified model, or two keys that resolve to one model | `attractor.model.fallbacks` |

A node with no model and no provider is left for the stage to refuse with
`bad_config`, as it does today.

At the stage, a frozen plan is used as it stands. The stage first checks
that the plan's original route is still one the client can address
(`Client::resolve_route` on its `provider/model` selector); when it is not,
the stage fails with class `llm:pinned_route_unavailable` and a message
naming the route, and no request leaves. The remaining routes stay as
frozen: a fallback target that is gone fails at request time, where Pebble's
failover handles it. The plan event and the once-per-run stderr notices are
emitted from the frozen data exactly as they are from a plan built at the
stage. Without a catalog at admission the graph keeps its selectors and
each stage resolves them at its first firing, as described next.

### Model fallback

`[run.model.fallbacks]` is applied by the native agent and prompt steps
(`attractor_steps::fallback`). A stage runs on a *plan*: the canonical route its
`model` and `provider` resolve to, then the targets the chain keyed by that
canonical model id lists, in order. Petri builds the plan, at admission when
the runtime has a catalog (above) and at the stage otherwise; on a native
agent node Pebble runs it (the plan's remaining routes are the builder's
`fallback_routes`) and reports every move on its own event stream, which is
the record of the routes (below). The chain is resolved once per run as
Fabro's server resolves it at run start, and so does Petri: admission
refuses a table the catalog cannot resolve (`attractor.model.unknown`,
`attractor.model.fallbacks`), and for a graph admitted without a catalog the
`start` stage checks the table before anything runs, so a key that names a
provider, a provider-qualified key, two keys that resolve to one model, or
an unknown key fail the run at `start` with class `bad_config`, before a
checkout, a hook or a node (a stage that reads the table later meets the
same error); a candidate on a provider that is not available
(`PETRI_LLM_PROVIDERS`, credentials) is skipped with Fabro's
`model_fallback_skipped` notice, as is a bare provider with no offering of
the requested model, a bare model no available provider offers, a duplicate
target, and (at plan time) a target with no reasoning level near the
requested effort (`NoNearbyReasoningLevel`). A configured chain left with
nothing usable warns `model_fallback_chain_empty` and the stage runs on its
primary alone. Each notice is printed once per run on stderr (`warn: ...`)
and always carried on the stage's plan event.

The requested reasoning effort maps per target through the catalog: the
nearest advertised level, a tie going up, as Fabro's `closest_supported`;
a target whose catalog row advertises no levels keeps the request. `speed`
and `max_tokens` travel unchanged. The original route is filtered out of its
own chain.

A model error moves the plan when it is eligible and a target remains.
Eligibility is `lithos-llm`'s `failover_eligible`, the rule Pebble applies:
a failure the client classifies as retryable (whatever its kind), the
provider-local kinds `rate_limit`, `server`, `network`, `stream_decode`,
`timeout`, `authentication`, `access_denied`, `not_found` and
`quota_exceeded`, and a `content_filter` whose provider code is `refusal`.
`invalid_request`, `context_length`, any other `content_filter`,
`configuration`, `model_selection`, `resource_limit`, `middleware`, a
`provider` or `response_decode` failure the client would not retry, and an
unknown kind end the stage with class `llm:<kind>`. Cancellation, the
driver's attempt budget, and any non-LLM agent error (a tool failure, a
missing skill) never start fallback. The reference's own mapping, written in
`.ai/reviews/fabro-unified/task12-fallback.md`, differs on two classes: it
moved a `provider` or `response_decode` failure whatever the retry class,
and never moved a retryable failure of another kind.

The plan is fixed when the stage opens. An output-repair turn runs on the
route the plan reached; advancing to a target never activates the target's
own chain; a retained `full` thread carries its plan to the next node, which
continues on the route reached (position and all) and builds no plan of its
own; a workflow retry (a new firing) builds a new plan at position 0. ACP
agents have no plan: the command owns their model.

A native session keeps its conversation across the change: Pebble resumes
the failed session's durable record on the next route with
`ResumeMode::UseModel`, the same session id continuing, and carries the
prompt on from the history as it stands. Two continuations exist, and Pebble
names the one it took on its `RouteFailover` event. When nothing this prompt
committed is in the conversation (the failed request carried the prompt
itself), the prompt is asked again on the new route (`replay_prompt`). When
work happened first (the conversation holds tool results or an assistant
turn this prompt committed), the next model continues that unfinished turn
with no new input (`continue_turn`): it answers the committed tool results as
they stand, and the tool that ran is not run again. The reference Fabro
(`05ebd0f`) runs its failover in Pebble the same way, so both engines run
the tool once; the reference before it rebuilt the session from the
original prompt and repeated the tool. A prompt node re-sends its
messages, repair history included, on the next route (`replay_prompt`).

Three retry mechanisms exist and each has one owner: the client's own
same-route retries (`PETRI_LLM_RETRY_ATTEMPTS`, default 3), Pebble's turn
replay after a broken response stream (Pebble's default policy), and this
chain, which Pebble starts only once both are spent. `PETRI_LLM_TIMEOUT_MS`
bounds one client call, retries included; its expiry is a `timeout` and
eligible. Both same-route mechanisms report on the agent's own event stream
as Pebble's `LlmRetry` event, with `phase` `open` for a client retry and
`consume` for a turn replay: Petri installs Pebble's `RetryEventObserver` on
the client it builds.

Recovery: nothing of a plan's progress is durable. A run resumed after a
crash starts the interrupted node's attempt again at position 0 of its plan,
on the primary (the frozen plan from the stored graph when the run was
admitted with a catalog, else a new plan); the thread it may have continued
is gone (the node degrades to `summary:high` as documented under "Fidelity
and threads"). Any model request that was in flight when the process died
may therefore be sent again, on the primary, and a tool effect that ran
before the crash may run again: the existing at-least-once limit for
external effects applies to fallback as to every other stage.

Events. Petri emits one `StepEvent::Custom` kind, for the fact Pebble cannot
know: `attractor.fallback.plan`, once per stage that builds a plan (`node`,
`firing`, `attempt`, `requested`, `routes[]` with `position`, `provider`,
`model`, `reasoning_effort`, `speed`, and `notices[]` with `code`, `level`,
`message`); a node that reuses a retained thread emits none. Every route fact
is Pebble's own event, recorded as a `step.progress.recorded` payload under
the node:
`SessionStarted` (`provider`, `model`) for the route each session starts on,
the primary and then each route a failover moved to; `RouteFailover` (`from`
and `to` as `provider/model`, `attempt`, the failed route's `usage`,
`inference_ms` and `tool_ms`, `error` with `llm_kind`,
`message`, `provider`, `status`, `provider_code` and `retry`, and
`continuation` `replay_prompt` or `continue_turn`); `RouteFailoverStopped`
(`route`, `attempt`, `reason` `ineligible` or `exhausted`, `error`) when a
model error ends the prompt although the plan named a fallback route (a plan
with no usable target names none, so Pebble publishes no stop and the stage
fails with the primary's error; a cancelled prompt publishes none either);
and `AssistantMessage` (`model`, `usage`) for each answer.
The prompt report Pebble hands the session names the route the prompt ended
on and its totals; the stage metrics carry Pebble's totals under
`pebble.*` and one breakdown per route, `pebble.usage_by_model`, folded
from those same events (above, "Attempt metrics"). A prompt node (`tab`) emits the plan alone
and reports its own move on stderr. `crates/petri/lib/tests/fallback_events.rs`
rebuilds a stage's outcome and per-route accounting from the public stream;
decision `pebble-events-are-the-agent-contract` records the kinds this
replaced.

On a native agent node the session names the plan's remaining routes to
Pebble and puts one line on the node's stderr per move (`model fallback:
<from> failed (<kind>); continuing on <to> (attempt <n> of the plan)`, from
Pebble's `RouteFailover`). Nothing else about the routes is Petri's to say.

### MCP servers

A native agent node connects to the run's `[run.agent.mcps]` servers through
Pebble's `mcp` feature (`attractor_steps::pebble::mcp`). Petri maps each entry
onto a Pebble `McpServer` (a `stdio` entry is `McpPlacement::Stdio`, `http`
is `McpPlacement::Http`, `sandbox` is `McpPlacement::Environment`), resolves
its secrets, and names the servers to the agent builder in name order. Pebble
starts them while it builds the agent, registers each discovered tool as a
`RegisteredTool` whose source is `ToolSource::Mcp { server, original name }`,
and closes them when the agent shuts down, so before the node returns and
before the scope's environment is released. The MCP protocol lives in the
`rmcp` client library Pebble pins (1.7.0, the version the pinned Fabro
locks); Pebble's agent loop sees only tools, so the run's tool hooks (a
`pre_tool_use` hook can block an MCP tool before it reaches the server),
Pebble's history, output bounds, cancellation and agent events apply to an
MCP tool as to any other. The tool the model sees is named
`mcp__<server>__<tool>`, with every character outside alphanumerics and `_`
replaced by `_`, as Fabro and Pebble both name it; a hook `matcher` matches
that name.

Placement follows Fabro. A `stdio` server is a child process of Petri's host
(Fabro's run worker), never of the sandbox; its working directory is the
scope's workspace when the scope shares the host filesystem (the host
backend), else Petri's own, and it gets the entry's `env` on top of Petri's
environment. An `http` server is reached from the host with the entry's
`headers`, over streamable HTTP or, with `protocol = "sse"`, the older SSE
transport (Pebble's own client; the stream at `url`, posts to the endpoint
it names, refused off the stream's origin). A `sandbox` server is launched in
the scope's execution environment through Pebble's `Environment::exec`
(`bash -c` for a `script`) and reached over the same two protocols through
the environment's route to its port: a streamable HTTP server at the route
itself, an SSE server's stream at `/sse` under it, where Fabro has always
reached one. Pebble takes that route as its own `PortRoutes` contract
(`pebble_coding_agent::mcp::PortRoutes`, so Petri shares no sandbox crate
with Pebble to implement it); Petri hands it
`attractor_steps::pebble::environment::ScopePortRoutes`, which answers from
`ExecEnv::preview_url` (the sandbox-driver `access/preview_url`
operation): the host's own loopback on a host scope; a forward the Docker
plugin opens on Petri's loopback and bridges into the container, so nothing
is published on the daemon and a remote daemon works the same; Daytona's
preview link with its token header, which rides on the request. Pebble polls
that route with an HTTP request until the server answers, within
`startup_timeout`, because a forward accepts a connection before the port
inside does, and releases the route when the server stops
(`access/preview_release`). An `http` server is probed the same way before
the handshake, so one that nothing answers at fails after `startup_timeout`
rather than at once. An environment that offers no preview URL fails
the server with Pebble's reason (`no route to port <port> in the
environment: the environment does not route to its ports`).

Failure behavior follows Fabro. A server that does not start (a launch error,
no handshake within `startup_timeout`, a protocol error) is reported by
Pebble (`McpServerFailed`) with the reason, including the tail of its own
error output; Petri writes the reason as a line on the node's stderr (`mcp
server \`<name>\` failed to start: ...`), and the session proceeds with the
tools of the servers that started. A server whose `env` or `headers` secret
the run cannot supply is never named to Pebble: Petri writes the same stderr
line and emits `attractor.mcp.unavailable` (below). A result the server marks
`isError` reaches the model as the tool's error text. A call with no answer
within `tool_timeout`, a call the agent cancelled, and a call to a server
whose connection closed each reach the model as a failed call with a reason;
the protocol's `notifications/cancelled` is sent for the first two. A server
whose connection closes mid-session is reported once by Pebble
(`McpServerDisconnected`, from the call that first found it closed, before
that call's own failure), and every later call to it fails at once; nothing
reconnects within a session. When Pebble's build fails after the servers
started, Pebble shuts them down before it reports the failure, so a server
launched in the environment does not outlive the node; the node reports the
build's error as its own. A retained thread's next node names the same servers again, so
Pebble starts its own and registers the same names, and the conversation's
earlier tool calls stay valid; a resumed run starts every thread again
anyway. Secrets in `env` and `headers` are resolved when the servers are
named, through the run's `SecretProvider`, and never written down; the
masker applies to every event. Sub-agents: Pebble registers MCP tools as
inheritable by child sessions (readiness item 9d verifies that against the
reference). ACP agents receive no MCP servers (Fabro passes none either).

Events. Pebble's own events, recorded as `step.progress.recorded` payloads
under the node,
are the record of the servers and their calls: `McpServerReady` (`server`,
`tools` as `[{ name, original_name }]`, `startup_ms`: launch to tools
listed) or `McpServerFailed` (`server`, `error`, `startup_ms`) once per
server Pebble was given, on the stream once the agent is up;
`McpServerDisconnected` (`server`, `error`) once, when a call first finds the
connection closed, before that call's own completion; and `ToolCallStarted`
and `ToolCallCompleted` under `mcp__<server>__<tool>` for every call, with
Pebble's `tool_call_id`, `output`, `is_error` and `error_kind` (`timeout` for
no answer within `tool_timeout`, `unavailable` for a call the closed or
absent server could not take, `cancelled`, `denied` for a call a hook
blocked before it reached the server, `invalid_arguments`). Pebble closes the
servers with the agent and publishes nothing for the shutdown; the server's
own log or process is the evidence. Petri emits one `StepEvent::Custom`
kind, for the one fact Pebble cannot know:

| `kind` | Fields | When |
|---|---|---|
| `attractor.mcp.unavailable` | `node`, `firing`, `attempt`, `server`, `error` | once per configured server Petri never named to Pebble because a secret its `env` or `headers` needs is unavailable, before the agent is built; the same reason is a `mcp server \`<name>\` failed to start: ...` line on the node's stderr |

Petri emitted `fabro.mcp.server` (`starting`, `ready`, `failed`,
`disconnected`, `stopped`) and `fabro.mcp.tool` until 2026-09-12; decision
`pebble-events-are-the-agent-contract` records their removal.
### Skills

A skill is a `<dir>/<name>/SKILL.md` file: a frontmatter block with `name:`
and an optional `description:`, then the prompt it expands into. Petri
states the directories the way Fabro's agent orders them, as a Pebble
`SkillDiscovery` (`attractor_steps::skills`), lowest precedence first, and Pebble
resolves and searches them:

1. the configured skills directory, `$FABRO_HOME/skills` when `FABRO_HOME`
   is set, else `$HOME/.fabro/skills`; an embedding host names the home
   with the `attractor_steps::skills::FabroHome` capability instead;
2. `<root>/.fabro/skills`, where the root is the Git root above the scope's
   working directory, or the working directory outside a repository;
3. `<root>/skills`;
4. the directories `[run.agent] skills` lists in `workflow.toml`, in order,
   a relative path resolved against the working directory. A Petri
   extension: the pinned Fabro refuses the key, so the load warns
   `fabro.petri_extension`.

Pebble discovers `*/SKILL.md` in each directory, a later directory
overriding an earlier name, lists the result under `# Available Skills` in
the system prompt (Fabro's text), registers the skill tool (`use_skill`
with `skill_name` for Fabro's own and the Codex vocabulary; `Skill` with
`skill` and `args` for Claude 5 and Kimi Code), and serves a tool call
with the template. On the native backend, a `/name` command expands only
at the beginning of the authored node prompt, after leading whitespace.
Later slash tokens remain arguments or ordinary text. Petri keeps the
fidelity preamble, `for_each` item data, and output contract outside that
expansion. They remain present even when a template has no `{{user_input}}`
placeholder and replaces the authored prompt. Generated output-repair
messages bypass expansion; user-delivered follow-ups retain leading-command
support. ACP receives the same assembled text as before, and its agent
owns any command interpretation. One-shot prompt nodes do not expand skills.

A session that discovers no skills offers no tool and no section. With
skills discovered, a leading command naming an unknown skill fails the
node with class `skill_missing` and the reason
`expanding a skill reference: Unknown skill: /name`. The reference cases
live in `crates/fabro/acceptance/testdata/skills`.

Skill loading is separate from the fidelity preamble and from project
document selection: the three share only the Git root probe. A skill's
template is ordinary conversation content, so a tool it drives passes the
node's tool hooks like any other call, and a skill loaded in one session
(a parallel branch, a `compact` node) reaches no other session; a `full`
thread carries it on as part of the conversation.

Events, all `StepEvent::Custom`:

- `kind = "attractor.skills"`: `{ kind, node, firing, attempt, scope, dirs }`
  once per native session, `dirs` the ordered list of
  `{ path, source }` with `source` one of `configured`, `project_fabro`,
  `project`, `workflow`.
- `kind = "attractor.skills.warning"`: `{ kind, node, firing, attempt, reason,
  path, message }` for each file or directory Pebble will skip:
  `malformed` (Pebble's parser rejects the file; the message says why in
  the parser's words), `unreadable` (the file was found but could not be
  read), `unsearchable` (a directory Pebble could not search) and
  `missing_directory` (a workflow-named directory that does not exist; a
  missing conventional directory is ordinary and silent). The same text
  reaches the terminal as a stderr line `skills: <path> <message>` of the
  node. Fabro records Pebble's report verbatim (`agent.skills.discovered`)
  and adds no warning of its own. The first three come from Pebble's
  own `SkillsDiscovered.skipped` report, read once per stage from the root
  session; the fourth is Petri's own probe, because Pebble says nothing
  about a directory that is not there.
- `kind = "pebble"` envelopes carry Pebble's own `SkillsDiscovered`
  (`profile`, `source_dirs`, `skills[{name, description}]`, `skipped`) and
  `SkillActivated` (`skill_name`, `source` = `slash` or `tool`), attributed
  to the node, firing, attempt and scope like every Pebble event.

### Compaction

A native agent's conversation is summarized as it approaches the model's
context window, so a long task stays inside the window. Fabro has no setting
for this: its agent sessions run with compaction on, a trigger at 80 percent
of the context window, and the six most recent turns kept verbatim. Petri
lowers those values onto every agent node (`frontend_attractor::CompactionSettings`)
and translates them into Pebble's options (`attractor_steps::compaction`). A
`[run.agent] compaction` key is refused, as Fabro refuses it (below,
"Refused"). Compaction is separate from workflow fidelity: the `compact`
fidelity mode is a deterministic preamble with no model call ("Fidelity and
threads"); this is the agent loop trimming its own history with a model call.

Pebble owns the whole operation. Before and after each model turn it estimates
the active context: the tokens the model last reported plus a local estimate
of the turns since, or a local estimate of the system prompt and the whole
history when no usage has been reported. When the estimate is above
`window * 80 / 100` (strictly above; exactly at the threshold does not
compact) it summarizes the older turns and replaces them with the summary,
keeping the recent turns and never separating a tool call from its result.
The summary is one non-streaming call on the node's own model; a host may
supply it instead through Pebble's `CompactionPolicy` (Petri installs a
`attractor_steps::compaction::CompactionPolicyHandle` capability on every native
session, resumed ones included). A summary that fails or is cancelled leaves
the history unchanged and does not fail the node: the agent continues on the
full history, and one failure is not retried for the rest of that prompt.
Petri never rewrites Pebble's committed history.

The compacted conversation travels with the session's warm export, so a later
node at effective `full` fidelity on the same thread continues it. A node
whose thread lost its conversation (its predecessor failed, or the run
resumed) starts again at `summary:high`, as after any lost session.

Pebble emits `CompactionStarted`, `CompactionCompleted`, `CompactionFailed`
and `CompactionCancelled` through the `pebble` envelope, and a
`context_window` warning at the threshold. `CompactionCompleted` carries the
summary call's `usage` (tokens and cost, "Usage" above), so the node's sink
folds the session's own compactions as they arrive and emits, right after
each `CompactionCompleted` it records, a `StepEvent::Custom` with
`kind = "attractor.compaction"`: `{ kind, node, firing, attempt, session, reason,
original_turn_count, preserved_turn_count, estimated_tokens_before,
summary_token_estimate, tracked_file_count, usage }`.
`estimated_tokens_before` is the estimate the compaction's
`CompactionStarted` reported (null when none preceded the completion); the
rest is the completion's. The payload no longer carries `summary_truncated`:
Pebble puts that on the history's `Compaction` turn and on no event. A failed
or cancelled compaction is not reported, and a child's is reported under the
child's session by Pebble alone. The attempt metrics `pebble.compactions` and
`pebble.compaction_usage` sum the completions. Pebble also bills the summary
call to the prompt that compacted, so these two are a breakdown of
`pebble.usage`, not an addition to it.

## Refused

| Construct | Code |
|---|---|
| `outcome=X` for X outside the four outcomes (and, after 2026-10-04, `success`) | `unsupported.outcome_value` |
| `llm_prompt`, `is_codergen`, `node_type`, bare-number timeouts (the legacy dialect the reference implementation no longer reads) | `unsupported.legacy_dialect` |
| an `import` Fabro's transform would refuse (missing file, bad boundary, cycle, extra placeholder attribute) | `attractor.import` |
| `backend="acp"` on a `tab` prompt node | `attractor.prompt_backend` |
| `acp_command` (legacy) | `unsupported.acp_command` |
| an unbound `{{ inputs.* }}` (a warning, `attractor.unbound_input`, under `petri check` with no inputs) | `unsupported.template.unbound_input` |
| ports, HTML strings, undirected graphs, `strict`, anonymous subgraphs | `unsupported.dot.*` |
| any graph, node or edge attribute Fabro does not define (`tool_hooks.*` and the removed `join_policy` included), outside the `x.` namespace | `attractor.unknown_attribute` |
| a node whose every outgoing edge has a `condition`, so no edge is the fallback | `attractor.all_conditional_edges` |
| a node that sets both `script` and `prompt` | `attractor.script_prompt_conflict` |
| `for_each` on a node that is not a `component` | `attractor.for_each.not_parallel` |
| an agent on `backend="acp"` with no `acp.command` or `acp.config` on the node or the graph | `attractor.acp_requires_command` |
| an agent on `backend="acp"` that sets `model`, `provider`, `reasoning_effort`, `max_tokens` or `speed` itself (a stylesheet's value does not count) | `attractor.acp_api_only_attributes` |
| with a model client at `Runtime::check`: a model, provider, or chain entry the catalog cannot resolve (see "Model resolution at admission") | `attractor.model.unknown` |
| with a model client at `Runtime::check`: a `[run.model.fallbacks]` table that is malformed or keyed by a provider | `attractor.model.fallbacks` |

**Fabro's validator rules.** Fabro checks a workflow with the rules in
`fabro-validate` before it creates a run. Every rule about the language is
raised by this lowering, so a host that embeds Petri needs no validator of its
own; [`LINTS.md`](LINTS.md) maps each of the 38 rules to its Petri code. The
five errors above are the ports; the ported warnings are
`attractor.inert_attribute` (an attribute only another kind of node reads,
Fabro's table), `attractor.retry_target_not_found` (a `retry_target` or
`fallback_retry_target`, on a node or the graph, that names no node),
`attractor.script_absolute_cd` (a command script with `cd /...`),
`attractor.bad_rankdir` (a `rankdir` outside `TB`, `LR`, `BT`, `RL`) and
`attractor.reserved_keyword_node_id` (a node id that is a DOT keyword). Each
carries the span of the attribute or node and a hint.

**Accepted until 2026-10-04.** One spelling is a dated shim, with a warning
that names the date and a `REMOVE AFTER 2026-10-04` comment at every site
(`grep -r "REMOVE AFTER"`): `outcome=success` in a condition (see
"Conditions"). Fabro accepts that spelling but it never matches, so Petri's
later rejection is deliberately stricter. `on_failure="succeed"` and
`auto_status` are supported for as long as the reference Fabro supports them
(see "Failure policy"); their earlier sunset was withdrawn by the readiness
plan. `.ai/plans/done/fabro-local-workflows.md` lists the workflows that
depend on the alias and what to do at the sunset.

**Unknown attributes are refused.** An attribute Fabro does not define on a
graph, a node or an edge is an error (`attractor.unknown_attribute`), because a
misspelt attribute that silently did nothing is the failure the attribute
tables exist to prevent. The hint names the Fabro attribute within two
character edits when there is one (`max_retrys` suggests `max_retries`);
for `tool_hooks.pre` and `tool_hooks.post` it points at `[[run.hooks]]`
(`pre_tool_use`, `post_tool_use`), which is where tool hooks are
configured, never on a node. Fabro's validator does not check attribute
names, so this is Petri-stricter. Two families are never diagnosed: the
Graphviz layout attributes (`color`, `rankdir`, `style`, ...) and the
extension namespace `x.` (`x.owner="platform"`, `"x.ticket"="PLAT-12"`),
which tooling and hosts use for attributes Petri carries without reading.

## Watchdog and circuit breaker

Two host policies ride the graph's `RunPolicy` and never change routing on
their own.

**Stall watchdog** (`stall_timeout`, default 30 m, `0s` disables). The
standalone host installs `execution::watchdog::StallWatchdog` as an observer.
Any engine or lifecycle record of any execution is activity. A question
pending with the host parks the clock: a run waiting on a person is blocked,
not stalled. When the last pending question is answered the run gets a full
stall budget again. A pause parks it too, until the unpause, which restarts
the full budget; a run resumed paused starts parked. A run idle for the whole budget is cancelled through the
coordinator, and the terminal prints `stall watchdog: no execution activity
for N s`. This is separate from each attempt's active-work timer.

**Circuit breaker** (`loop_restart_signature_limit`, default 3, at least 1).
The standalone host installs `execution::breaker::CircuitBreaker` as routing
middleware (`host::policy_middleware`), on run and on resume. After every
node's final outcome a failure is classified into Fabro's categories
(`transient_infra`, `deterministic`, `budget_exhausted`, `compilation_loop`,
`canceled`, `structural`, from the failure class and the reference's message
hints) and a signature `<node>|<category>|<normalized reason>`. A
`deterministic` or `structural` signature is counted; reaching the limit
blocks the failed firing's route with `deterministic failure cycle detected`,
which fails the run. A `loop_restart` edge is blocked for any classified
failure other than `transient_infra`, and a tracked failure's restart
signature is counted in its own map with the same limit. Success never clears
a count. Both maps live in the middleware state, so they survive a restart
successor and are restored on resume. Node visit totals also survive a
`loop_restart` (the successor starts with the predecessor's firing counts)
while the context is replaced; the run-wide invocation total never resets.

## Pause and resume

A run control's pause (`petri run --control <FILE>`, the line `pause`, or an
embedding host's `ControlService::pause`) holds every attempt not yet
admitted; running work continues. Each pause and unpause is a coordinator
record (`RunPaused`, `RunUnpaused`), so the pause is durable the way the
pinned Fabro persists `Paused` as a run status: `petri inspect` reports
`paused`, `replay_run` carries `run_paused` and `run_unpaused`, and a resume
of a run whose last recorded control was a pause starts with admission held
until an `unpause` arrives. A pause holds at once and records after; an
unpause records first and releases after, so a crash between the two never
resumes paused.

`petri resume --run-dir <dir>` continues an interrupted run from its run
directory alone, with the same session options as `petri run`. Finished
nodes are not repeated; the node in flight at the crash starts again; a human
gate that was waiting asks again and the resumed interviewer answers it. A
retained agent thread is not restored (above). The command refuses a
finished run, a run another process holds, a paused run given no `--control`
file, and a run directory that does not decode, at exit 2 before any work.

## Syntax both runners reject, and Petri's stricter diagnostics

Rejected by both: legacy-dialect attributes (`unsupported.legacy_dialect`), a bare-number
`timeout`, an `outcome=` value outside the four outcomes
(`unsupported.outcome_value`), the legacy `acp_command`, an import Fabro's
transform refuses (`attractor.import`), `backend="acp"` on a prompt node, a human
gate with no edges, a `for_each` template that is not an LLM node, structural
mistakes (no start, no exit, unreachable nodes), a node with only conditional
edges (`attractor.all_conditional_edges`), a node with both `script` and
`prompt` (`attractor.script_prompt_conflict`), `for_each` off a parallel node
(`attractor.for_each.not_parallel`), an ACP agent with no agent named or with
API-only attributes (`attractor.acp_requires_command`,
`attractor.acp_api_only_attributes`), and the `workflow.toml` keys
Fabro's parser refuses (`unsupported.workflow_toml.key`). [`LINTS.md`](LINTS.md)
has the full map of Fabro's validator rules.

Petri-stricter, tested as differences and listed in
`crates/fabro/acceptance/CONTRACT.md`: the 500-firing cap and its
`attractor.max_visits_too_large` / `info.budget.default` diagnostics (Fabro is
unlimited), `outcome=success` after its sunset, the 10,000-invocation maximum,
the platform-only `workflow.toml` warnings,
unknown graph, node and edge attributes outside the `x.` namespace
(`attractor.unknown_attribute`; Fabro has no rule for attribute names), and
`on_failure="partially_succeed"` in the other direction: Petri accepts a
spelling Fabro refuses.

## Fabro conventions the language carries

Three pieces of the reference implementation's convention live in the
Attractor crates, because a workflow's files or a user's scripts depend on
them and the draft specification has not yet said whether they are the
language's. Each keeps its name and behavior.

- **The skills search path.** `$FABRO_HOME/skills`, else
  `$HOME/.fabro/skills`, then `.fabro/skills` and `skills` under the Git root
  (`attractor_steps::skills`). A host names the home through the `FabroHome`
  capability; the environment read is the fallback.
- **The child workflow path prefix.** `stack.child_workflow="fabro/..."`
  resolves against `.fabro/`. It is written in the DOT, so it is language
  surface as the reference implementation defined it.
- **The hook script environment.** `FABRO_EVENT`, `FABRO_RUN_ID`,
  `FABRO_WORKFLOW`, `FABRO_NODE_ID`, `FABRO_HOOK_CONTEXT` and the
  `.fabro-hook-context-*.json` file (`attractor_steps::hooks`). User scripts
  read these names.
