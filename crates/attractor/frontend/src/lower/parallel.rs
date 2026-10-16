//! Parallel nodes (`component`) and their fan-ins.
//!
//! Every branch of a parallel node runs as an internal child invocation, the
//! way Fabro runs it as a branch task: the branch target is lowered into a
//! graph of its own, and the parent graph's node for that target becomes a
//! `attractor/branch` step that starts the child with a snapshot of the
//! parent's context, waits for it, and returns the branch envelope. The child
//! inherits the parent's sandbox and workspace and keeps its own context; its
//! changes come back as output only and never merge into the parent's `kv`. A
//! branch never follows its target's outgoing edges: every branch routes to the
//! join, the one direct successor all branch targets share.
//!
//! Static branches are the parallel node's outgoing edges, in order, each
//! its own branch node (a duplicate target gets a synthetic node so each
//! branch has its own index). A `for_each` fan-out expands the one template
//! node over the runtime items with the same branch step; an empty list
//! expands to the IR's one placeholder item (`ir::placeholder`) so the
//! fan-in still fires, while branch maps and the event stream count no
//! branch for the clone.
//!
//! The parallel node itself is the `attractor/fork` step. It runs once per
//! visit, before any branch, and takes the fork snapshot of `kv` and, for
//! agent or prompt targets, of the stage records. It offloads the `for_each`
//! source list and every other large value to the run's output store, so
//! the snapshot every branch child is declared from, and every clone's input
//! token, hold references instead of O(items) copies. Its output is
//! `{ snapshot, nodes, occurrence }`, plus the hydrated `items` for a dynamic
//! fork. Expansion reads `items` from the fork's record; the edge excludes
//! it from the token the branch delegates read their `kv` and `nodes` from. A
//! snapshot key a branch's own graph reads by expression (the source list of a
//! `for_each` nested inside a static branch) is named in the fork's `inline`
//! list and stays inline.
//!
//! The fan-in the branches join at collects the envelopes in branch order,
//! publishes `parallel.results` and `parallel.branch_count`, and takes the
//! aggregate status. A join that is not a `tripleoctagon` gets a synthetic
//! fan-in in front of it, so `parallel.results` is published either way and
//! the join runs once.
//!
//! `max_parallel` is not an expansion limit: it bounds the *attempts* of a
//! fork's branches through the coordinator's attempt admission, so a branch
//! waiting out a retry backoff holds no slot. Fabro's normalization applies:
//! missing, non-integer or negative is 4, zero is 1.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem;
use std::time::Duration;

use frontend::{Diagnostic, Span};
use ir::placeholder::{BRANCH_ROLE_META, placeholder_item};
use ir::{
    BinOp, Budget, Edge, ExpandTarget, ExprId, ExprTable, GraphBuilder, JoinPolicy, NodeId,
    ResultProjection, RetryPolicy, Routing, StepRef,
};
use serde_json::{Map, Value, json};

use super::{Ctx, Kind, NodeRef, attrs, placeholder, threads};
use crate::kinds::{
    AGENT_KIND, BRANCH_ITEM_KEY, BRANCH_KIND, BRANCH_NODES_KEY, FAN_IN_KIND, FORK_ITEMS_FIELD,
    FORK_KIND, FORK_NODES_FIELD, FORK_OCCURRENCE_FIELD, FORK_SNAPSHOT_FIELD, PROMPT_KIND,
};
use crate::model::{AttrValue, EdgeDecl, NodeDecl, Workflow};

/// Fabro's `max_parallel` when the attribute is missing, not an integer, or
/// negative.
pub const DEFAULT_MAX_PARALLEL: u32 = 4;

/// The `meta.kind` of a parent-side branch node: the delegate that starts
/// one branch's child invocation and returns its envelope.
pub const BRANCH_META_KIND: &str = "parallel.branch";

/// How a branch's position is known: fixed at lowering for a static branch,
/// bound by the expansion for a `for_each` item.
#[derive(Clone, Copy)]
enum BranchIndex {
    Static(u32),
    Item,
}

/// One parallel node as its branches are lowered: the declaration, its
/// engine node, its branch edges in order, and its normalized
/// `max_parallel`.
struct Fork<'w> {
    decl:         &'w NodeDecl,
    id:           NodeId,
    edges:        Vec<&'w EdgeDecl>,
    max_parallel: u32,
}

/// What an outer fork needs of an inner one it takes as a branch: the
/// workflow node the inner branches join at, so the outer fork continues
/// from where they converged, and the snapshot keys the inner branch graphs
/// read by expression, so the outer fork keeps them inline in its snapshot.
struct LoweredFork {
    join:   String,
    inline: BTreeSet<String>,
}

impl Ctx<'_> {
    /// Lower every parallel node and configure every fan-in. Nested parallel
    /// nodes (a parallel node that is another's branch target) lower first,
    /// so the outer branch's child graph copies the finished inner region.
    pub(super) fn parallel(&mut self, workflow: &Workflow) {
        for node in &workflow.nodes {
            let res = self.nodes[&node.id];
            if res.kind == Kind::FanIn {
                self.fan_in_step(node, res.id);
            }
        }
        let mut pending: Vec<&NodeDecl> = workflow
            .nodes
            .iter()
            .filter(|node| self.nodes.get(&node.id).map(|n| n.kind) == Some(Kind::Parallel))
            .collect();
        let mut done: HashSet<String> = HashSet::new();
        let mut forks: HashMap<String, LoweredFork> = HashMap::new();
        while !pending.is_empty() {
            let ready = pending.iter().position(|node| {
                workflow.outgoing(&node.id).iter().all(|edge| {
                    self.nodes.get(&edge.to).map(|n| n.kind) != Some(Kind::Parallel)
                        || done.contains(&edge.to)
                })
            });
            let Some(position) = ready else {
                for node in &pending {
                    self.diags.error(
                        "attractor.parallel.cycle",
                        node.span.clone(),
                        format!(
                            "parallel node `{}` is a branch of a parallel node that is a branch of \
                             it; parallel nodes cannot nest in a cycle",
                            node.id
                        ),
                    );
                }
                return;
            };
            let node = pending.remove(position);
            if let Some(lowered) = self.lower_parallel(node, workflow, &forks) {
                forks.insert(node.id.clone(), lowered);
            }
            done.insert(node.id.clone());
        }
    }

    /// The fan-in step: the ordered branch envelopes its inputs carry. A
    /// prompted fan-in (already an `attractor/prompt` step) reads the same
    /// list.
    fn fan_in_step(&mut self, node: &NodeDecl, id: NodeId) {
        let ordered = self.ordered_results();
        let occurrences = self.ordered_field(FORK_OCCURRENCE_FIELD);
        let step = &mut self.b.node_mut(id).step;
        if step.kind == PROMPT_KIND {
            if let Value::Object(config) = &mut step.config {
                config.insert("branch_results".into(), placeholder(ordered));
            }
            return;
        }
        *step = StepRef::new(
            FAN_IN_KIND,
            json!({
                "label": node.attrs.text("label").unwrap_or_else(|| node.id.clone()),
                "node": node.id,
                "results": placeholder(ordered),
                "occurrences": placeholder(occurrences),
            }),
        );
    }

    /// Lower one parallel node. Returns what an outer fork needs of it, when
    /// its branches have a join.
    fn lower_parallel(
        &mut self,
        node: &NodeDecl,
        workflow: &Workflow,
        forks: &HashMap<String, LoweredFork>,
    ) -> Option<LoweredFork> {
        let edges = workflow.outgoing(&node.id);
        for edge in &edges {
            // The routing pass skips a parallel node's edges, so they are
            // checked here.
            self.unknown_attrs(
                &edge.attrs,
                attrs::EDGE,
                &[],
                &format!("edge `{} -> {}`", edge.from, edge.to),
            );
            threads::check_fork_edge(edge, &mut self.diags);
            if edge
                .attrs
                .text("condition")
                .is_some_and(|c| !c.trim().is_empty())
            {
                self.diags.error(
                    "attractor.parallel.conditional_branch",
                    edge.span.clone(),
                    "a parallel node's branches are unconditional",
                );
            }
        }
        if edges.is_empty() {
            self.diags.error(
                "attractor.parallel.no_branches",
                node.span.clone(),
                format!("parallel node `{}` has no branches", node.id),
            );
            return None;
        }
        let fork = Fork {
            decl: node,
            id: self.nodes[&node.id].id,
            edges,
            max_parallel: self.max_parallel(node),
        };
        if let Some(source) = node.attrs.text("for_each") {
            self.dynamic_branches(&fork, &source, workflow, forks)
        } else {
            self.static_branches(&fork, workflow, forks)
        }
    }

    /// The `attractor/fork` step on the parallel node: the fork-time snapshot
    /// of `kv` (and of the stage records when a branch target renders a
    /// preamble), the `for_each` source key it offloads at any size, and the
    /// keys it must keep inline.
    fn fork_step(
        &mut self,
        fork: &Fork,
        source: Option<&str>,
        inline: &BTreeSet<String>,
        with_nodes: bool,
    ) {
        let mut config = Map::new();
        config.insert(
            "label".into(),
            Value::String(
                fork.decl
                    .attrs
                    .text("label")
                    .unwrap_or_else(|| fork.decl.id.clone()),
            ),
        );
        config.insert("node".into(), Value::String(fork.decl.id.clone()));
        let kv = self.b.exprs().var("kv");
        config.insert("kv".into(), placeholder(kv));
        if with_nodes {
            let nodes = self.b.exprs().var("nodes");
            config.insert("nodes".into(), placeholder(nodes));
        }
        if let Some(source) = source {
            config.insert("source".into(), Value::String(source.to_owned()));
        }
        if !inline.is_empty() {
            config.insert("inline".into(), json!(inline));
        }
        self.b.node_mut(fork.id).step = StepRef::new(FORK_KIND, Value::Object(config));
    }

    /// Fabro's `max_parallel`: missing, non-integer or negative is 4; zero
    /// is 1. Petri says so where Fabro is silent.
    fn max_parallel(&mut self, node: &NodeDecl) -> u32 {
        let Some(attr) = node.attrs.get("max_parallel") else {
            return DEFAULT_MAX_PARALLEL;
        };
        let parsed = match &attr.value {
            AttrValue::Int(n) => Some(*n),
            AttrValue::Str(s) => s.trim().parse::<i64>().ok(),
            AttrValue::Float(_) | AttrValue::Bool(_) => None,
        };
        let span = attr.span.clone();
        match parsed {
            Some(n) if n > 0 => u32::try_from(n).unwrap_or(u32::MAX),
            Some(0) => {
                self.diags.warning(
                    "attractor.max_parallel.normalized",
                    span,
                    format!(
                        "`max_parallel=0` on `{}` runs one branch at a time, as Fabro does",
                        node.id
                    ),
                );
                1
            }
            Some(n) => {
                self.diags.warning(
                    "attractor.max_parallel.normalized",
                    span,
                    format!(
                        "`max_parallel={n}` on `{}` is negative; Fabro reads it as \
                         {DEFAULT_MAX_PARALLEL}",
                        node.id
                    ),
                );
                DEFAULT_MAX_PARALLEL
            }
            None => {
                self.diags.warning(
                    "attractor.max_parallel.normalized",
                    span,
                    format!(
                        "`max_parallel={}` on `{}` is not an integer; Fabro reads it as \
                         {DEFAULT_MAX_PARALLEL}",
                        attr.value.as_text(),
                        node.id
                    ),
                );
                DEFAULT_MAX_PARALLEL
            }
        }
    }

    /// The node every branch joins at: the one direct successor all branch
    /// targets share (the lowest id when there are several, as Fabro picks
    /// it). A `tripleoctagon` join is the collector itself; any other join
    /// gets a synthetic fan-in in front of it, so the results are published
    /// and the join runs once. Edges a branch target has to other nodes are
    /// never taken and are reported.
    fn branch_collector(
        &mut self,
        fork: &Fork,
        workflow: &Workflow,
        forks: &HashMap<String, LoweredFork>,
    ) -> Option<(NodeId, String)> {
        // A branch that is itself a parallel node continues from its own join.
        let exit_of = |target: &str| {
            forks
                .get(target)
                .map_or_else(|| target.to_owned(), |inner| inner.join.clone())
        };
        let mut common: Option<HashSet<String>> = None;
        for branch in &fork.edges {
            let targets: HashSet<String> = workflow
                .outgoing(&exit_of(&branch.to))
                .into_iter()
                .map(|edge| edge.to.clone())
                .collect();
            common = Some(match common {
                None => targets,
                Some(shared) => shared.intersection(&targets).cloned().collect(),
            });
        }
        let mut shared: Vec<String> = common.unwrap_or_default().into_iter().collect();
        shared.sort();
        let Some(join) = shared.into_iter().next() else {
            self.diags.error(
                "attractor.parallel.no_join",
                fork.decl.span.clone(),
                format!(
                    "the branches of parallel node `{}` share no direct successor to join at; \
                     every branch target must have an edge to the same node",
                    fork.decl.id
                ),
            );
            return None;
        };
        for branch in &fork.edges {
            for edge in workflow.outgoing(&exit_of(&branch.to)) {
                if edge.to != join {
                    self.diags.warning(
                        "attractor.parallel.branch_edge_ignored",
                        edge.span.clone(),
                        format!(
                            "`{} -> {}` is never taken: `{}` runs as a branch of `{}` and returns \
                             to the join `{join}`",
                            edge.from, edge.to, branch.to, fork.decl.id
                        ),
                    );
                }
            }
        }
        let join_id = self.nodes.get(&join).map(|n| n.id)?;
        if self.nodes.get(&join).map(|n| n.kind) == Some(Kind::FanIn) {
            // The join reports `parallel_complete` for this fork.
            if let Value::Object(config) = &mut self.b.node_mut(join_id).step.config {
                config.insert("fork".into(), Value::String(fork.decl.id.clone()));
            }
            return Some((join_id, join));
        }
        let exit = self.exit();
        let target = self
            .goal_check
            .filter(|_| join_id == exit)
            .unwrap_or(join_id);
        let name = format!("{}.fan_in", fork.decl.id);
        let results = placeholder(self.ordered_results());
        let occurrences = placeholder(self.ordered_field(FORK_OCCURRENCE_FIELD));
        let collector = self.b.add_node(
            &name,
            self.scope,
            StepRef::new(
                FAN_IN_KIND,
                json!({
                    "label": format!("Fan-in of {}", fork.decl.id),
                    "node": name,
                    "fork": fork.decl.id,
                    "results": results,
                    "occurrences": occurrences,
                }),
            ),
        );
        self.spans.insert(collector, fork.decl.span.clone());
        self.b.set_meta(
            collector,
            json!({
                "label": format!("Fan-in of {}", fork.decl.id),
                "shape": "tripleoctagon",
                "kind": Kind::FanIn.name(),
                "classes": [],
                "synthetic": true,
                "span": { "line": fork.decl.span.line, "column": fork.decl.span.column },
            }),
        );
        self.b.set_join(collector, JoinPolicy::All);
        self.b.link(collector, target);
        Some((collector, join))
    }

    /// The branch envelopes the fan-in's inputs carry, in branch order.
    fn ordered_results(&mut self) -> ExprId {
        self.ordered_field("value")
    }

    /// One field of every input token of a fan-in, in branch order: the
    /// tokens are `{ index, value, occurrence }` from the branch delegates.
    fn ordered_field(&mut self, field: &str) -> ExprId {
        let exprs = self.b.exprs();
        let inputs = exprs.var("inputs");
        let index = exprs.lit("index");
        let sorted = exprs.call("sort_by_key", vec![inputs, index]);
        let field = exprs.lit(field);
        exprs.call("pluck", vec![sorted, field])
    }

    fn static_branches(
        &mut self,
        fork: &Fork,
        workflow: &Workflow,
        forks: &HashMap<String, LoweredFork>,
    ) -> Option<LoweredFork> {
        let mut ok = true;
        for edge in &fork.edges {
            let kind = self.nodes.get(&edge.to).map(|n| n.kind);
            if matches!(kind, None | Some(Kind::Start | Kind::Exit | Kind::FanIn)) {
                self.diags.error(
                    "attractor.parallel.bad_branch_target",
                    edge.to_span.clone(),
                    format!(
                        "`{}` cannot be a branch of parallel node `{}`; a branch target is a \
                         stage that does work and then returns to the join",
                        edge.to, fork.decl.id
                    ),
                );
                ok = false;
            }
        }
        if !ok {
            return None;
        }
        let (collector, join) = self.branch_collector(fork, workflow, forks)?;
        // A nested fork's branch graphs read their `for_each` lists from the
        // context by expression, which cannot see through a reference: those
        // keys stay inline in this fork's snapshot.
        let mut inline: BTreeSet<String> = BTreeSet::new();
        for edge in &fork.edges {
            if self.nodes.get(&edge.to).map(|n| n.kind) != Some(Kind::Parallel) {
                continue;
            }
            inline.extend(workflow.node(&edge.to).and_then(for_each_key));
            if let Some(inner) = forks.get(&edge.to) {
                inline.extend(inner.inline.iter().cloned());
            }
        }
        let with_nodes = fork
            .edges
            .iter()
            .any(|edge| self.nodes[&edge.to].kind.is_llm());
        // Every child graph first: a branch node is rewritten into its branch
        // step only after every branch copied its target as lowered, so a
        // duplicate target's child copies the stage and not a branch step.
        let mut prepared = Vec::with_capacity(fork.edges.len());
        for (index, edge) in fork.edges.iter().enumerate() {
            let index = u32::try_from(index).unwrap_or(u32::MAX);
            let NodeRef {
                id: target, kind, ..
            } = self.nodes[&edge.to];
            let region = self.branch_region(&edge.to, target, kind);
            let Some(child) = self.branch_child(&edge.to, &region, Some((fork.id, index))) else {
                continue;
            };
            prepared.push((index, *edge, target, region, child));
        }
        let mut seen: HashSet<&str> = HashSet::new();
        let mut branch_nodes = Vec::with_capacity(prepared.len());
        for (index, edge, target, region, child) in prepared {
            let branch_node = if seen.insert(edge.to.as_str()) {
                target
            } else {
                // A duplicate target: its own branch node, so the index and
                // the result stay its own.
                let duplicate = self.b.add_node(
                    &format!("{}.branch{index}", edge.to),
                    self.scope,
                    StepRef::new("noop", Value::Null),
                );
                let meta = self.b.graph().node(target).map(|n| n.meta.clone());
                if let Some(meta) = meta {
                    self.b.set_meta(duplicate, meta);
                }
                self.spans.insert(duplicate, edge.to_span.clone());
                duplicate
            };
            // A nested fork's region now runs inside the child: its nodes
            // stay in the parent graph without edges, so the inner join no
            // longer feeds the outer one.
            for dead in region.iter().skip(1) {
                self.b.node_mut(*dead).routing = Routing::default();
            }
            self.branch_step(
                branch_node,
                fork,
                &edge.to,
                BranchIndex::Static(index),
                &child,
                collector,
            );
            branch_nodes.push(branch_node);
        }
        self.b.fan_out(fork.id, &branch_nodes);
        self.fork_step(fork, None, &inline, with_nodes);
        Some(LoweredFork { join, inline })
    }

    fn dynamic_branches(
        &mut self,
        fork: &Fork,
        source: &str,
        workflow: &Workflow,
        forks: &HashMap<String, LoweredFork>,
    ) -> Option<LoweredFork> {
        let span = fork.decl.attrs.span_of("for_each", &fork.decl.span);
        let key = source_key(source);
        if key.is_empty() {
            self.diags.error(
                "attractor.for_each.source",
                span,
                format!("`for_each` on `{}` must name a context key", fork.decl.id),
            );
            return None;
        }
        if fork.edges.len() != 1 {
            self.diags.error(
                "attractor.for_each.template_edges",
                fork.decl.span.clone(),
                format!(
                    "`for_each` node `{}` needs exactly one template edge",
                    fork.decl.id
                ),
            );
            return None;
        }
        let template = fork.edges[0];
        let NodeRef {
            id: target, kind, ..
        } = *self.nodes.get(&template.to)?;
        if !kind.is_llm() {
            self.diags.error(
                "attractor.for_each.target",
                template.to_span.clone(),
                format!(
                    "the `for_each` template `{}` must be an agent or prompt node",
                    template.to
                ),
            );
            return None;
        }
        if workflow
            .node(&template.to)
            .is_some_and(|n| n.attrs.contains("for_each"))
        {
            self.diags.error(
                "attractor.for_each.nested",
                template.to_span.clone(),
                "nested `for_each` is not supported",
            );
            return None;
        }
        let (collector, join) = self.branch_collector(fork, workflow, forks)?;
        let child = self.branch_child(&template.to, &[target], None)?;

        // The fork hydrates and validates the source once. Expansion reads
        // its output record, while the edge carries only the compact
        // snapshot so no clone's token contains the whole list.
        let items = {
            let exprs = self.b.exprs();
            let items = exprs.path("nodes", &[&fork.decl.id, "output", FORK_ITEMS_FIELD]);
            let len = exprs.call("len", vec![items]);
            let zero = exprs.lit(0);
            let empty = exprs.binary(BinOp::Eq, len, zero);
            let marker = exprs.lit(placeholder_item());
            let placeholder_list = exprs.array(vec![marker]);
            exprs.cond(empty, placeholder_list, items)
        };
        self.fork_step(fork, Some(&key), &BTreeSet::new(), true);
        let payload = {
            let exprs = self.b.exprs();
            let output = exprs.var("output");
            let snapshot = exprs.field(output, FORK_SNAPSHOT_FIELD);
            let nodes = exprs.field(output, FORK_NODES_FIELD);
            let occurrence = exprs.field(output, FORK_OCCURRENCE_FIELD);
            exprs.object(vec![
                (FORK_SNAPSHOT_FIELD, snapshot),
                (FORK_NODES_FIELD, nodes),
                (FORK_OCCURRENCE_FIELD, occurrence),
            ])
        };
        let success = self.b.exprs().call("success", vec![]);
        let mut edge = Edge::when(self.b.next_edge_id(), target, success);
        edge.map = Some(payload);
        self.b.node_mut(fork.id).routing = Routing::next(edge);
        self.branch_step(
            target,
            fork,
            &template.to,
            BranchIndex::Item,
            &child,
            collector,
        );
        ir::parallel_for_each(&mut self.b, target, items, ExpandTarget::Node, None, false);
        Some(LoweredFork {
            join,
            inline: BTreeSet::new(),
        })
    }

    /// The parent-graph nodes a branch child copies: the target alone, or a
    /// nested parallel node with its branch nodes and its collector.
    fn branch_region(&self, target_name: &str, target: NodeId, kind: Kind) -> Vec<NodeId> {
        if kind != Kind::Parallel {
            return vec![target];
        }
        let graph = self.b.graph();
        let mut region = vec![target];
        let mut collector = None;
        if let Some(node) = graph.node(target) {
            for arm in node.routing.edges() {
                region.push(arm.to);
                if let Some(branch) = graph.node(arm.to) {
                    collector = branch.routing.edges().next().map(|edge| edge.to);
                }
            }
        }
        // A nested parallel node that failed its own lowering has no
        // collector; its diagnostic already stands, and the region is what
        // exists.
        let _ = target_name;
        region.extend(collector);
        region
    }

    /// Turn the parent node `branch_node` into the branch step for `target`,
    /// whose child graph is registered under the digest `child`.
    fn branch_step(
        &mut self,
        branch_node: NodeId,
        fork: &Fork,
        target: &str,
        index: BranchIndex,
        child: &str,
        collector: NodeId,
    ) {
        let kind = self.nodes[target].kind;
        let label = self
            .b
            .graph()
            .node(branch_node)
            .and_then(|n| n.meta.get("label").and_then(Value::as_str))
            .map_or_else(|| target.to_string(), str::to_string);
        let mut config = Map::new();
        config.insert("label".into(), Value::String(label));
        config.insert("node".into(), Value::String(target.to_string()));
        config.insert("fork".into(), Value::String(fork.decl.id.clone()));
        let index_expr = match index {
            BranchIndex::Static(i) => {
                config.insert("index".into(), Value::from(i));
                self.b.exprs().lit(u64::from(i))
            }
            BranchIndex::Item => {
                let item = self.b.exprs().var("item");
                config.insert("item".into(), placeholder(item));
                config.insert("for_each".into(), Value::Bool(true));
                let index = self.b.exprs().var("index");
                config.insert("index".into(), placeholder(index));
                index
            }
        };
        config.insert("max_parallel".into(), Value::from(fork.max_parallel));
        config.insert("child_digest".into(), Value::String(child.to_string()));
        config.insert("target_kind".into(), Value::String(kind.name().into()));
        // The fork snapshot, from the fork step's output on the incoming
        // token: the same object for every branch, with its large values
        // offloaded once.
        let kv = {
            let exprs = self.b.exprs();
            let input = exprs.var("input");
            exprs.field(input, FORK_SNAPSHOT_FIELD)
        };
        config.insert("kv".into(), placeholder(kv));
        let generation = self.b.exprs().var("generation");
        config.insert("generation".into(), placeholder(generation));
        // The fork occurrence, from the same token: the fork step's firing.
        let fork_firing = {
            let exprs = self.b.exprs();
            let input = exprs.var("input");
            let occurrence = exprs.field(input, FORK_OCCURRENCE_FIELD);
            exprs.field(occurrence, "firing")
        };
        config.insert("fork_firing".into(), placeholder(fork_firing));
        if kind.is_llm() {
            let nodes = {
                let exprs = self.b.exprs();
                let input = exprs.var("input");
                exprs.field(input, FORK_NODES_FIELD)
            };
            config.insert("nodes".into(), placeholder(nodes));
        }
        let payload = {
            let exprs = self.b.exprs();
            let output = exprs.var("output");
            let input = exprs.var("input");
            let occurrence = exprs.field(input, FORK_OCCURRENCE_FIELD);
            exprs.object(vec![
                ("index", index_expr),
                ("value", output),
                (FORK_OCCURRENCE_FIELD, occurrence),
            ])
        };
        let edge_id = self.b.next_edge_id();
        let mut edge = Edge::always(edge_id, collector);
        edge.map = Some(payload);
        let node = self.b.node_mut(branch_node);
        node.step = StepRef::new(BRANCH_KIND, Value::Object(config));
        node.retry = RetryPolicy::none();
        node.budget = Budget::new(1, Duration::ZERO);
        node.precondition = None;
        node.routing = Routing::next(edge);
        if let Value::Object(meta) = &mut node.meta {
            // The parent-side node stands for the branch, not the stage: the
            // stage runs in the child with the target's own metadata. Hosts
            // tell the two apart by kind, and skip this one as a lowering
            // artifact where they skip `goal_check`.
            let mut branch = json!({ "fork": fork.decl.id, "target": target });
            if let BranchIndex::Static(i) = index {
                branch["index"] = Value::from(i);
            }
            meta.insert("branch".into(), branch);
            meta.insert("kind".into(), Value::String(BRANCH_META_KIND.into()));
            meta.insert("synthetic".into(), Value::Bool(true));
        }
    }

    /// The child graph a branch runs: a copy of `region` from the parent
    /// graph with its own expression table, the same scopes, the same run
    /// parameters, the entry at the region's first node and the result at
    /// its last. The copy drops every edge that leaves the region (a branch
    /// never follows its target's edges) and the target's explicit routes (a
    /// branch has none, so its failure policy applies unconditionally).
    /// Returns the registered digest.
    fn branch_child(
        &mut self,
        target_name: &str,
        region: &[NodeId],
        role: Option<(NodeId, u32)>,
    ) -> Option<String> {
        let (mut builder, copies) = GraphBuilder::copy_region(self.b.graph(), region);
        let entry = copies.get(region.first()?).copied()?;
        let result = copies.get(region.last()?).copied()?;
        let target = builder.node_mut(entry);
        let is_llm = target.step.kind == AGENT_KIND || target.step.kind == PROMPT_KIND;
        let mut config = mem::take(&mut target.step.config);
        if let Value::Object(map) = &mut config {
            map.remove(super::ROUTES_KEY);
            if is_llm {
                // The child's own records are empty when its target starts:
                // the preamble renders from the parent's records at fork
                // time, which the branch step puts in the snapshot.
                let nodes = context_read(builder.exprs(), BRANCH_NODES_KEY);
                map.insert("nodes".into(), placeholder(nodes));
                let item = context_read(builder.exprs(), BRANCH_ITEM_KEY);
                map.insert("item_data".into(), placeholder(item));
                // Fabro's branch rules: threads are inert and an explicit
                // `full` degrades to `summary:high`.
                map.insert("branch".into(), Value::Bool(true));
            }
        }
        let target = builder.node_mut(entry);
        target.step.config = config;
        if let Some((fork, index)) = role
            && let Value::Object(meta) = &mut target.meta
        {
            meta.insert(
                BRANCH_ROLE_META.into(),
                json!({ "fork": fork.raw(), "index": index }),
            );
        }
        builder.mark_entry(entry);
        let mut graph = builder.build();
        graph.result = ResultProjection::NodeOutput(result);
        graph.params = self.params();
        let report = ir::check(&graph);
        let span = self
            .nodes
            .get(target_name)
            .and_then(|declared| self.spans.get(&declared.id).cloned())
            .unwrap_or_else(|| Span::file(target_name));
        for error in &report.errors {
            let mut diagnostic = Diagnostic::validation_error(error, span.clone());
            diagnostic.message = format!("branch `{target_name}`: {}", diagnostic.message);
            self.diags.push(diagnostic);
        }
        if !report.errors.is_empty() {
            return None;
        }
        let digest = frontend::graph_digest(&graph);
        self.children.push(graph);
        Some(digest)
    }
}

/// The context key a `for_each` attribute names: `context.K` or `K`.
fn source_key(source: &str) -> String {
    source
        .strip_prefix("context.")
        .unwrap_or(source)
        .trim()
        .to_string()
}

/// The `for_each` source key of a parallel node, when it has a non-empty one.
fn for_each_key(node: &NodeDecl) -> Option<String> {
    node.attrs
        .text("for_each")
        .map(|source| source_key(&source))
        .filter(|key| !key.is_empty())
}

/// `get(kv, key)` in `table`.
fn context_read(table: &mut ExprTable, key: &str) -> ExprId {
    let kv = table.var("kv");
    let name = table.lit(key);
    table.call("get", vec![kv, name])
}
