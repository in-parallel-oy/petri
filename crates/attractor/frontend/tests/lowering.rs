//! Acceptance §7 item 3, the pure half: what lowering produces — routing
//! tiers, failure policies, goal gates, parallel, budgets, and the specific
//! rejections. The run half is `crates/fabro/acceptance/tests/routing.rs`. What
//! the settings files put into a graph is the Fabro frontend's
//! (`crates/fabro/frontend/tests/lowering.rs`).

mod support;

use std::num::NonZeroU32;
use std::time::Duration;

use frontend::print::print_expr;
use frontend::{CompileInputs, NoFiles};
use frontend_attractor::kinds::{
    AGENT_KIND, BRANCH_KIND, COMMAND_KIND, FAN_IN_KIND, FORK_KIND, HUMAN_KIND, PROMPT_KIND,
    STAGE_KIND, WAIT_KIND, WORKFLOW_KIND,
};
use frontend_attractor::{MAX_FIRINGS, load};
use ir::placeholder::{BRANCH_ROLE_META, contains_placeholder};
use ir::{Completion, EdgeTransition, Exhaustion, Guard, JoinPolicy, PickPolicy, TimeoutPolicy};
use serde_json::json;
use support::*;

#[test]
fn shapes_lower_to_their_step_kinds() {
    let graph = lower_ok(&dot(r#"
        a [label="Agent", prompt="do"]
        p [label="Prompt", shape=tab, prompt="say"]
        c [label="Cmd", shape=parallelogram, script="true"]
        inferred [label="Inferred", script="true"]
        h [shape=hexagon, label="Ok?"]
        w [shape=insulator, duration="1s"]
        d [shape=diamond]
        m [shape=house, stack.child_dot_source="digraph C { start [shape=Mdiamond] exit [shape=Msquare] start -> exit }", manager.max_cycles=3]
        start -> a -> p -> c -> inferred -> h
        h -> w [label="[Y] Yes"]
        w -> d -> m -> exit
    "#));
    assert_eq!(node(&graph, "start").step.kind, STAGE_KIND);
    assert_eq!(node(&graph, "start").step.config["kind"], json!("start"));
    assert_eq!(node(&graph, "exit").step.kind, STAGE_KIND);
    assert_eq!(node(&graph, "d").step.kind.as_str(), "noop");
    assert_eq!(node(&graph, "a").step.kind, AGENT_KIND);
    assert_eq!(node(&graph, "p").step.kind, PROMPT_KIND);
    assert_eq!(node(&graph, "p").step.config["kind"], json!("prompt"));
    assert_eq!(node(&graph, "c").step.kind, COMMAND_KIND);
    assert_eq!(node(&graph, "inferred").step.kind, COMMAND_KIND);
    assert_eq!(node(&graph, "h").step.kind, HUMAN_KIND);
    assert_eq!(node(&graph, "w").step.kind, WAIT_KIND);
    assert_eq!(node(&graph, "w").step.config["duration_ms"], json!(1000));
    assert_eq!(node(&graph, "m").step.kind, WORKFLOW_KIND);
    assert!(node(&graph, "m").step.config["child_digest"].is_string());
    assert_eq!(
        graph.completion,
        Completion::TerminalNode(node_id(&graph, "exit"))
    );
    assert_eq!(graph.entry, vec![node_id(&graph, "start")]);
    assert_eq!(node(&graph, "a").meta["label"], json!("Agent"));
    assert_eq!(node(&graph, "a").meta["shape"], json!("box"));
    ir::validate(&graph).expect("validates");
}

#[test]
fn the_four_tiers_lower_in_order_with_their_picks() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x"]
        b [prompt="x"]
        c [prompt="x"]
        d [prompt="x"]
        start -> a
        a -> b [condition="outcome=succeeded", weight=2]
        a -> c [label="[F] Fix"]
        a -> d
        b -> exit
        c -> exit
        d -> exit
    "#));
    let tiers = tiers(&graph, "a");
    assert_eq!(tiers.len(), 4);
    assert_eq!(tiers[0].0, PickPolicy::HighestWeightThenLexical);
    assert_eq!(
        tiers[0]
            .1
            .iter()
            .map(|(t, _)| t.as_str())
            .collect::<Vec<_>>(),
        ["b"]
    );
    assert_eq!(tiers[1].0, PickPolicy::First);
    assert_eq!(
        tiers[1]
            .1
            .iter()
            .map(|(t, _)| t.as_str())
            .collect::<Vec<_>>(),
        ["c"]
    );
    assert_eq!(tiers[2].0, PickPolicy::LowestRankThenArmOrder);
    assert_eq!(
        tiers[2]
            .1
            .iter()
            .map(|(t, _)| t.as_str())
            .collect::<Vec<_>>(),
        ["c", "d"]
    );
    assert_eq!(tiers[3].0, PickPolicy::HighestWeightThenLexical);
    assert_eq!(
        tiers[3]
            .1
            .iter()
            .map(|(t, _)| t.as_str())
            .collect::<Vec<_>>(),
        ["c", "d"]
    );
    assert!(
        matches!(tiers[3].1[0].1, Guard::Always),
        "route: the fallback is unconditional"
    );

    // Tier 1 fires on the outcome, tier 2 on the normalized label, tier 3 on
    // the suggested id.
    let ok = statics("success", &json!({}));
    assert!(eval_guard(&graph, tiers[0].1[0].1, &ok, &[]));
    let failed = statics("failure", &json!({}));
    assert!(!eval_guard(&graph, tiers[0].1[0].1, &failed, &[]));
    let labelled = statics("success", &json!({ "preferred_label": "fix" }));
    assert!(eval_guard(&graph, tiers[1].1[0].1, &labelled, &[]));
    let other = statics("success", &json!({ "preferred_label": "Approve" }));
    assert!(!eval_guard(&graph, tiers[1].1[0].1, &other, &[]));
    let suggested = statics("success", &json!({ "suggested_next_ids": ["d", "c"] }));
    assert!(eval_guard(&graph, tiers[2].1[0].1, &suggested, &[]));
    assert!(eval_guard(&graph, tiers[2].1[1].1, &suggested, &[]));
    let weights: Vec<u32> = node(&graph, "a").routing.groups[0]
        .arms
        .iter()
        .map(|a| a.weight)
        .collect();
    assert_eq!(weights, [2, 0, 0]);
    assert_eq!(
        node(&graph, "a").routing.groups[0].arms[1].label.as_deref(),
        Some("[F] Fix")
    );
}

#[test]
fn random_selection_picks_weighted_and_forbids_conditions() {
    let graph = lower_ok(&dot(r#"
        graph [selection="random"]
        a [prompt="x"]
        b [prompt="x"]
        start -> a
        a -> b [weight=3]
        a -> exit
        b -> exit
    "#));
    let tiers = tiers(&graph, "a");
    assert_eq!(
        tiers.last().expect("fallback").0,
        PickPolicy::WeightedRandom
    );
    let weights: Vec<u32> = node(&graph, "a").routing.groups[0]
        .arms
        .iter()
        .map(|a| a.weight)
        .collect();
    assert_eq!(
        weights,
        [3, 1],
        "a zero weight counts as one under random selection"
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", selection="random"]
        start -> a
        a -> exit [condition="outcome=succeeded"]
        a -> exit
    "#))
        .contains(&"attractor.random_with_conditions".to_string())
    );
}

#[test]
fn failure_policies_guard_the_fallback_tier() {
    // route: always; exit: only a non-failed outcome; a human gate never falls
    // through on failure whatever its policy says.
    let graph = lower_ok(&dot(r#"
        graph [on_failure="exit"]
        r [prompt="x", on_failure="route"]
        e [prompt="x"]
        h [shape=hexagon, on_failure="route"]
        x [prompt="x", on_failure="route", on_retries_exhausted="exit", max_retries=1]
        start -> r -> e -> h
        h -> x [label="Go"]
        x -> exit
    "#));
    let fallback = |name: &str| tiers(&graph, name).last().expect("fallback").1[0].1;
    assert!(matches!(fallback("r"), Guard::Always));
    let failed = statics("failure", &json!({ "failure_class": "" }));
    let ok = statics("success", &json!({}));
    let skipped = statics("skipped", &json!({}));
    let timed_out = statics("timed_out", &json!({}));
    assert!(!eval_guard(&graph, fallback("e"), &failed, &[]));
    assert!(!eval_guard(&graph, fallback("e"), &timed_out, &[]));
    assert!(eval_guard(&graph, fallback("e"), &ok, &[]));
    assert!(eval_guard(&graph, fallback("e"), &skipped, &[]));
    assert!(!eval_guard(&graph, fallback("h"), &failed, &[]));
    assert!(eval_guard(&graph, fallback("h"), &ok, &[]));
    // Mixed: a non-retryable failure routes, an exhausted retryable one exits.
    let exhausted = statics("failure", &json!({ "failure_class": "retry_requested" }));
    assert!(eval_guard(&graph, fallback("x"), &failed, &[]));
    assert!(!eval_guard(&graph, fallback("x"), &exhausted, &[]));
    assert_eq!(node(&graph, "x").retry.max_attempts.get(), 2);
    assert_eq!(node(&graph, "x").retry.on_exhaustion, Exhaustion::Fail);
}

/// The exhaustion policy rides in the step config beside `on_failure`, and
/// the engine's own exhaustion stays `Fail`: the step decides what the last
/// retryable failure becomes, with the explicit routes in hand, as Fabro's
/// executor applies its failure policy after its retries.
#[test]
fn exhaustion_policies_ride_in_the_step_config_and_the_engine_never_accepts_partial() {
    let graph = lower_ok(&dot(r#"
        graph [on_failure="succeed"]
        a [prompt="x", allow_partial=true, max_retries=2]
        b [prompt="x", on_retries_exhausted="partially_succeed", on_failure="route"]
        c [prompt="x", on_failure="partially_succeed"]
        d [prompt="x", on_failure="route", on_retries_exhausted="succeed"]
        e [prompt="x", on_failure="route", max_retries=1]
        start -> a -> b -> c -> d -> e -> exit
        e -> a [condition="outcome=failed"]
    "#));
    for name in ["a", "b", "c", "d", "e"] {
        assert_eq!(
            node(&graph, name).retry.on_exhaustion,
            Exhaustion::Fail,
            "{name}"
        );
    }
    let config = |name: &str| node(&graph, name).step.config.clone();
    assert_eq!(node(&graph, "a").retry.max_attempts.get(), 3);
    assert_eq!(config("a")["on_failure"], json!("succeed"));
    assert_eq!(
        config("a")["on_retries_exhausted"],
        json!("partially_succeed")
    );
    assert_eq!(config("b")["on_failure"], json!("route"));
    assert_eq!(
        config("b")["on_retries_exhausted"],
        json!("partially_succeed")
    );
    assert_eq!(config("c")["on_failure"], json!("partially_succeed"));
    assert_eq!(
        config("c")["on_retries_exhausted"],
        json!("partially_succeed"),
        "the exhaustion policy defaults to `on_failure`"
    );
    assert_eq!(config("d")["on_failure"], json!("route"));
    assert_eq!(config("d")["on_retries_exhausted"], json!("succeed"));
    assert_eq!(
        config("d")["routes"]["targets"],
        json!(["e"]),
        "a promoting exhaustion policy hands the step the explicit routes"
    );
    assert_eq!(config("e")["on_failure"], json!("route"));
    assert_eq!(config("e")["on_retries_exhausted"], json!("route"));
    assert!(
        config("e").get("routes").is_none(),
        "a routing node needs no promotion check"
    );
    let retry_on = &node(&graph, "a").retry.retry_on;
    assert!(retry_on.statuses.is_empty());
    assert_eq!(retry_on.failure_classes, vec![ir::FailureClass::new(
        "retry_requested"
    )]);
}

/// A promoting policy hands the step the node's explicit routes, so the step
/// can keep a failure an explicit route matches, as Fabro's executor does.
#[test]
fn promoting_policies_carry_the_explicit_routes() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x", on_failure="succeed"]
        b [prompt="x"]
        c [prompt="x"]
        d [prompt="x", on_failure="route"]
        start -> a
        a -> b [condition="outcome=failed"]
        a -> c [label="[C] Continue"]
        a -> exit
        b -> exit
        c -> d -> exit
    "#));
    let routes = &node(&graph, "a").step.config["routes"];
    assert_eq!(routes["conditions"], json!(["outcome=failed"]));
    assert_eq!(routes["labels"], json!(["continue"]));
    assert_eq!(routes["targets"], json!(["c", "exit"]));
    assert!(
        node(&graph, "d").step.config.get("routes").is_none(),
        "a routing policy needs no promotion check"
    );
    let found = codes(&dot(r#"
        a [prompt="x", on_failure="partially_succeed"]
        start -> a -> exit
    "#));
    assert!(
        found.contains(&"attractor.petri_extension".to_string()),
        "the Petri-only spelling is named: {found:?}"
    );
}

#[test]
fn succeed_is_supported_and_auto_status_is_a_warned_alias() {
    let text = dot(r#"
        a [prompt="x", on_failure="succeed"]
        b [prompt="x", auto_status=true]
        start -> a -> b -> exit
    "#);
    let found = codes(&text);
    assert!(
        !found.iter().any(|code| code.contains("succeed")),
        "`on_failure=\"succeed\"` is supported without a warning: {found:?}"
    );
    assert!(
        found.contains(&"deprecated.auto_status".to_string()),
        "{found:?}"
    );
    let graph = lower_ok(&text);
    assert_eq!(
        node(&graph, "a").step.config["on_failure"],
        json!("succeed")
    );
    assert_eq!(
        node(&graph, "b").step.config["on_failure"],
        json!("succeed"),
        "`auto_status=true` is the node's `on_failure=\"succeed\"`"
    );
    assert!(
        codes(&dot(r#"
        graph [on_failure="stop"]
        a [prompt="x"]
        start -> a -> exit
    "#))
        .contains(&"attractor.bad_on_failure".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", allow_partial=true, on_retries_exhausted="exit"]
        start -> a -> exit
    "#))
        .contains(&"attractor.allow_partial_conflict".to_string())
    );
}

#[test]
fn retry_presets_and_defaults_lower_to_retry_policies() {
    let graph = lower_ok(&dot(r#"
        graph [default_max_retries=3]
        d [prompt="x"]
        n [prompt="x", max_retries=0]
        s [prompt="x", retry_policy="standard"]
        l [prompt="x", retry_policy="linear"]
        start -> d -> n -> s -> l -> exit
    "#));
    assert_eq!(node(&graph, "d").retry.max_attempts.get(), 4);
    assert_eq!(node(&graph, "n").retry.max_attempts.get(), 1);
    assert_eq!(node(&graph, "s").retry.max_attempts.get(), 5);
    assert_eq!(node(&graph, "l").retry.max_attempts.get(), 3);
    assert!((node(&graph, "l").retry.backoff.factor - 1.0).abs() < f64::EPSILON);
    assert_eq!(
        node(&graph, "l").retry.backoff.initial,
        Duration::from_millis(500)
    );
    assert_eq!(
        node(&graph, "d").retry.backoff.initial,
        Duration::from_secs(5)
    );
}

#[test]
fn goal_gates_insert_a_check_with_back_arms_in_resolution_order() {
    let graph = lower_ok(&dot(r#"
        graph [retry_target="plan"]
        plan [prompt="x"]
        work [prompt="x", goal_gate=true, retry_target="missing", fallback_retry_target="work"]
        verify [prompt="x", goal_gate=true]
        start -> plan -> work -> verify -> exit
    "#));
    let check = node(&graph, "goal_check");
    assert_eq!(
        target_name(&graph, node(&graph, "verify").routing.groups[0].arms[0].to),
        "goal_check"
    );
    let arms = &check.routing.groups[0].arms;
    assert_eq!(arms.len(), 3);
    // Gates in id order: `verify` falls to the graph target, `work` to its own
    // fallback (its `retry_target` names a node that does not exist).
    assert_eq!(target_name(&graph, arms[0].to), "plan");
    assert!(arms[0].back);
    assert_eq!(target_name(&graph, arms[1].to), "work");
    assert!(arms[1].back);
    assert_eq!(target_name(&graph, arms[2].to), "exit");
    assert!(!arms[2].back);
    // An unvisited gate fails the check; success-like records pass it.
    let none = statics("success", &json!({}));
    assert!(
        eval_guard(&graph, arms[0].guard, &none, &[]),
        "unvisited: back to the retry target"
    );
    assert!(
        !eval_guard(&graph, arms[2].guard, &none, &[]),
        "unvisited: no exit"
    );
    assert!(check.budget.is_finite());
    assert_eq!(node(&graph, "plan").budget.max_firings, MAX_FIRINGS);
    assert_eq!(node(&graph, "plan").join, JoinPolicy::Any);
    ir::validate(&graph).expect("validates");
}

#[test]
fn loops_get_back_edges_any_joins_and_capped_budgets() {
    let graph = lower_ok(&dot(r#"
        graph [max_node_visits=30]
        impl [prompt="x", max_visits=12]
        check [shape=diamond]
        fix [prompt="x", max_visits=5]
        start -> impl -> check
        check -> exit [condition="outcome=succeeded"]
        check -> impl [label="Retry"]
        impl -> fix
        fix -> check [loop_restart=true]
    "#));
    let retry = &node(&graph, "check").routing.groups[0].arms[1];
    assert!(retry.back, "the cycle-closing edge is a back edge");
    assert_eq!(node(&graph, "impl").join, JoinPolicy::Any);
    assert_eq!(node(&graph, "impl").budget.max_firings, 12);
    assert_eq!(node(&graph, "check").budget.max_firings, 30);
    assert_eq!(node(&graph, "fix").budget.max_firings, 5);
    assert_eq!(
        node(&graph, "fix").routing.groups[0].arms[0].transition,
        EdgeTransition::Restart
    );
    ir::validate(&graph).expect("validates");
}

#[test]
fn visit_limits_above_the_cap_are_rejected_and_unlimited_is_capped_with_a_note() {
    assert!(
        codes(&dot(r#"
        a [prompt="x", max_visits=501]
        start -> a -> exit
    "#))
        .contains(&"attractor.max_visits_too_large".to_string())
    );
    let lowered = frontend_attractor::load_text(
        "w.fabro",
        &dot(r#"
        a [prompt="x"]
        start -> a
        a -> a [condition="outcome=failed"]
        a -> exit
    "#),
    );
    let graph = lowered.graph.expect("lowers");
    assert_eq!(node(&graph, "a").budget.max_firings, MAX_FIRINGS);
    assert!(
        lowered
            .diagnostics
            .iter()
            .any(|d| d.code == "info.budget.default")
    );
}

#[test]
fn static_fan_out_and_fan_in_lower_to_groups_and_an_all_join() {
    let graph = lower_ok(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        b [prompt="x"]
        merge [shape=tripleoctagon]
        report [prompt="x"]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> report -> exit
    "#));
    assert_eq!(
        node(&graph, "fork").routing.groups.len(),
        2,
        "one group per branch"
    );
    assert_eq!(node(&graph, "merge").join, JoinPolicy::All);
    assert_eq!(node(&graph, "a").join, JoinPolicy::Any);
    // Each branch is an `attractor/branch` step over a child graph of its own,
    // routing only to the fan-in with its envelope.
    let a = node(&graph, "a");
    assert_eq!(a.step.kind, BRANCH_KIND);
    assert_eq!(a.step.config["node"], json!("a"));
    assert_eq!(a.step.config["fork"], json!("fork"));
    assert_eq!(a.step.config["index"], json!(0));
    assert_eq!(a.step.config["max_parallel"], json!(4));
    assert_eq!(node(&graph, "b").step.config["index"], json!(1));
    assert_eq!(a.routing.groups.len(), 1);
    let arm = &a.routing.groups[0].arms[0];
    assert_eq!(target_name(&graph, arm.to), "merge");
    assert!(arm.map.is_some(), "a branch hands the fan-in its result");
    assert_eq!(a.meta["kind"], json!("parallel.branch"));
    assert_eq!(a.meta["synthetic"], json!(true));
    assert_eq!(node(&graph, "merge").step.kind, FAN_IN_KIND);
    assert!(contains_placeholder(&node(&graph, "merge").step.config));
    ir::validate(&graph).expect("validates");
}

#[test]
fn branches_lower_to_child_graphs_that_keep_the_target_and_its_role() {
    let lowered = load(
        "w.fabro",
        &dot(r#"
        fork [shape=component, max_parallel=2]
        a [shape=parallelogram, script="echo a", max_retries=2]
        b [prompt="x", on_failure="succeed"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        b -> merge
        merge -> exit
    "#),
        &NoFiles,
        &CompileInputs::new(),
    );
    let graph = lowered.graph.expect("lowers");
    assert_eq!(lowered.children.len(), 2, "one child graph per branch");
    let child_of = |name: &str| {
        let digest = graph
            .nodes
            .iter()
            .find(|n| n.name == name)
            .and_then(|n| n.step.config["child_digest"].as_str())
            .unwrap_or_else(|| panic!("{name}'s child digest"))
            .to_owned();
        lowered
            .children
            .iter()
            .find(|child| frontend::graph_digest(child) == digest)
            .unwrap_or_else(|| panic!("the child {name} names is registered"))
    };
    let child = child_of("a");
    assert_eq!(child.nodes.len(), 1, "the target alone");
    let target = &child.nodes[0];
    assert_eq!(target.name, "a");
    assert_eq!(target.step.kind, COMMAND_KIND);
    assert_eq!(
        target.retry.max_attempts.get(),
        3,
        "retries stay inside the child"
    );
    assert_eq!(target.meta["kind"], json!("command"));
    assert_eq!(
        target.meta[BRANCH_ROLE_META],
        json!({ "fork": node_id(&graph, "fork").raw(), "index": 0 })
    );
    assert!(target.routing.groups.is_empty(), "a branch follows no edge");
    assert_eq!(child.entry, vec![target.id]);
    assert_eq!(child.result, ir::ResultProjection::NodeOutput(target.id));
    // A prompt branch reads the parent's stage records from the fork
    // snapshot, carries its item data, and has no explicit routes: its
    // succeed policy applies unconditionally.
    let config = &child_of("b").nodes[0].step.config;
    assert!(config.get("routes").is_none());
    assert_eq!(
        print_expr(
            &child_of("b").exprs,
            ir::ExprId::new(
                u32::try_from(config["nodes"]["$expr"].as_u64().expect("placeholder"))
                    .expect("u32")
            ),
        ),
        "get(kv, 'internal.parallel_nodes')"
    );
    assert!(contains_placeholder(&config["item_data"]));
    assert_eq!(config["branch"], json!(true));
    for child in &lowered.children {
        ir::validate(child).expect("child validates");
    }
}

#[test]
fn max_parallel_follows_fabro_normalization() {
    let branch = |attrs: &str| {
        lower_ok(&dot(&format!(
            r#"
        fork [shape=component{attrs}]
        a [prompt="x"]
        merge [shape=tripleoctagon]
        start -> fork -> a -> merge -> exit
    "#
        )))
    };
    let of = |graph: &ir::Graph| node(graph, "a").step.config["max_parallel"].clone();
    assert_eq!(of(&branch("")), json!(4));
    assert_eq!(of(&branch(", max_parallel=7")), json!(7));
    assert_eq!(of(&branch(", max_parallel=0")), json!(1));
    assert_eq!(of(&branch(", max_parallel=-3")), json!(4));
    assert_eq!(of(&branch(", max_parallel=\"many\"")), json!(4));
    assert!(
        codes(&dot(r#"
        fork [shape=component, max_parallel="many"]
        a [prompt="x"]
        merge [shape=tripleoctagon]
        start -> fork -> a -> merge -> exit
    "#))
        .contains(&"attractor.max_parallel.normalized".to_string())
    );
}

#[test]
fn static_branch_payloads_carry_the_branch_index() {
    let graph = lower_ok(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        b [prompt="x"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a -> merge
        fork -> b -> merge
        merge -> exit
    "#));
    let index = |name: &str| {
        let map = node(&graph, name).routing.groups[0].arms[0]
            .map
            .expect("branch payload");
        eval_expr(&graph, map, &statics("success", &json!(name)), &[])["index"]
            .as_u64()
            .expect("numeric branch index")
    };
    assert_eq!(index("a"), 0);
    assert_eq!(index("b"), 1);
}

#[test]
fn branches_must_share_a_join_and_a_branch_follows_no_other_edge() {
    // A tail after a branch target is never taken: the branches share no
    // direct successor, so the fork has no join.
    assert!(
        codes(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        a_tail [prompt="x"]
        b [prompt="x"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a -> a_tail -> merge
        fork -> b -> merge
        merge -> exit
    "#))
        .contains(&"attractor.parallel.no_join".to_string())
    );
    // An edge from a branch target to anything but the join is reported.
    assert!(
        codes(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        b [prompt="x"]
        merge [shape=tripleoctagon]
        extra [prompt="x"]
        start -> fork
        fork -> a
        fork -> b
        a -> merge
        a -> extra
        b -> merge
        merge -> exit
        extra -> exit
    "#))
        .contains(&"attractor.parallel.branch_edge_ignored".to_string())
    );
    // The exit is not a branch target.
    assert!(
        codes(&dot(r#"
        fork [shape=component]
        gate [prompt="x"]
        start -> fork
        fork -> gate
        fork -> exit
        gate -> exit
    "#))
        .contains(&"attractor.parallel.bad_branch_target".to_string())
    );
}

#[test]
fn a_join_that_is_not_a_fan_in_gets_a_synthetic_fan_in_that_publishes_the_results() {
    let graph = lower_ok(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        b [prompt="x"]
        debate [prompt="x"]
        start -> fork
        fork -> a
        fork -> b
        a -> debate
        b -> debate
        debate -> exit
    "#));
    let collector = node(&graph, "fork.fan_in");
    assert_eq!(collector.step.kind, FAN_IN_KIND);
    assert_eq!(collector.join, JoinPolicy::All);
    assert_eq!(collector.meta["synthetic"], json!(true));
    assert_eq!(collector.meta["kind"], json!("parallel.fan_in"));
    assert_eq!(
        target_name(&graph, collector.routing.groups[0].arms[0].to),
        "debate"
    );
    for branch in ["a", "b"] {
        assert_eq!(
            target_name(&graph, node(&graph, branch).routing.groups[0].arms[0].to),
            "fork.fan_in"
        );
    }
    ir::validate(&graph).expect("validates");
}

#[test]
fn a_duplicate_branch_target_gets_its_own_branch_node_and_index() {
    let graph = lower_ok(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        merge [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> a
        a -> merge
        merge -> exit
    "#));
    assert_eq!(node(&graph, "a").step.config["index"], json!(0));
    let duplicate = node(&graph, "a.branch1");
    assert_eq!(duplicate.step.kind, BRANCH_KIND);
    assert_eq!(duplicate.step.config["node"], json!("a"));
    assert_eq!(duplicate.step.config["index"], json!(1));
    assert_eq!(node(&graph, "fork").routing.groups.len(), 2);
    ir::validate(&graph).expect("validates");
}

/// A `for_each` nested in a static branch reads its list from the context by
/// expression inside the child, which cannot see through a reference: the
/// outer fork keeps that key inline in its snapshot, and the inner fork
/// offloads it from its own children.
#[test]
fn a_static_fork_keeps_a_nested_for_each_list_inline_in_its_snapshot() {
    let lowered = load(
        "w.fabro",
        &dot(r#"
        plan [shape=parallelogram, script="echo"]
        outer [shape=component]
        inner [shape=component, for_each="context.jobs"]
        job [prompt="x"]
        inner_join [shape=tripleoctagon]
        other [shape=parallelogram, script="true"]
        join [shape=tripleoctagon]
        start -> plan -> outer
        outer -> inner -> job -> inner_join -> join
        outer -> other -> join
        join -> exit
    "#),
        &NoFiles,
        &CompileInputs::new(),
    );
    let graph = lowered
        .graph
        .unwrap_or_else(|| panic!("{:?}", lowered.diagnostics));
    let outer = node(&graph, "outer");
    assert_eq!(outer.step.kind, FORK_KIND);
    assert_eq!(outer.step.config["inline"], json!(["jobs"]));
    assert!(outer.step.config.get("source").is_none());
    assert!(
        outer.step.config.get("nodes").is_none(),
        "no direct branch target renders a preamble"
    );
    // The parent holds the branch delegate for `inner`; the inner fork itself
    // runs in that branch's child graph, where it offloads the list from its
    // own children.
    assert_eq!(node(&graph, "inner").step.kind, BRANCH_KIND);
    let inner = lowered
        .children
        .iter()
        .flat_map(|child| child.nodes.iter())
        .find(|n| n.name == "inner" && n.step.kind == FORK_KIND)
        .expect("the inner fork step in a child graph");
    assert_eq!(inner.step.config["source"], json!("jobs"));
    assert!(inner.step.config.get("inline").is_none());
    assert!(contains_placeholder(&inner.step.config["nodes"]));
}

#[test]
fn for_each_lowers_to_an_expansion_on_the_template_node() {
    let graph = lower_ok(&dot(r#"
        plan [shape=parallelogram, script="echo"]
        fan [shape=component, for_each="context.jobs", max_parallel=4]
        job [prompt="x"]
        join [shape=tripleoctagon]
        start -> plan -> fan -> job -> join -> exit
    "#));
    let job = node(&graph, "job");
    let Some(ir::Expansion::ForEach {
        max_parallel,
        fail_fast,
        target,
        ..
    }) = &job.expand
    else {
        panic!("job expands");
    };
    // The expansion itself is unbounded: `max_parallel` bounds the branch
    // attempts through the child invocations' admission.
    assert_eq!(*max_parallel, None);
    assert!(!fail_fast);
    assert_eq!(*target, ir::ExpandTarget::Node);
    assert_eq!(job.step.kind, BRANCH_KIND);
    assert_eq!(job.step.config["max_parallel"], json!(4));
    assert_eq!(job.step.config["for_each"], json!(true));
    assert!(contains_placeholder(&job.step.config["item"]));
    assert!(contains_placeholder(&job.step.config["index"]));
    // The branch reads its snapshot from the fork's output, not the live
    // context.
    assert!(contains_placeholder(&job.step.config["kv"]));
    assert!(contains_placeholder(&job.step.config["nodes"]));
    let fan = node(&graph, "fan");
    assert!(
        fan.precondition.is_none(),
        "the fork validates the hydrated list"
    );
    // The parallel node is the fork step: it snapshots `kv` and the stage
    // records once, and offloads the source list at any size.
    assert_eq!(fan.step.kind, FORK_KIND);
    assert_eq!(fan.step.config["source"], json!("jobs"));
    assert_eq!(fan.step.config["node"], json!("fan"));
    assert!(contains_placeholder(&fan.step.config["kv"]));
    assert!(contains_placeholder(&fan.step.config["nodes"]));
    assert!(fan.step.config.get("inline").is_none());
    assert!(
        codes(&dot(r#"
        fan [shape=component, for_each="context.jobs"]
        c [shape=parallelogram, script="x"]
        start -> fan -> c -> exit
    "#))
        .contains(&"attractor.for_each.target".to_string())
    );
}

#[test]
fn human_gates_offer_their_edges_as_choices() {
    let graph = lower_ok(&dot(r#"
        gate [shape=hexagon, label="Approve?", question_type="multiple_choice"]
        yes [prompt="x"]
        no [prompt="x"]
        free [prompt="x"]
        start -> gate
        gate -> yes [label="[A] Approve"]
        gate -> no [label="Reject"]
        gate -> free [freeform=true]
        yes -> exit
        no -> exit
        free -> exit
    "#));
    let config = &node(&graph, "gate").step.config;
    assert_eq!(
        config["choices"],
        json!([
            { "key": "A", "label": "[A] Approve", "to": "yes" },
            { "key": "R", "label": "Reject", "to": "no" },
        ])
    );
    assert_eq!(config["freeform_target"], json!("free"));
    assert_eq!(config["question_type"], json!("multiple_choice"));
    // The labelled tier matches an accelerator-free answer.
    let t = tiers(&graph, "gate");
    let answer = statics("success", &json!({ "preferred_label": "Approve" }));
    assert!(eval_guard(&graph, t[0].1[0].1, &answer, &[]));
    assert!(!eval_guard(&graph, t[0].1[1].1, &answer, &[]));
}

/// A choice edge's `human.description` and `human.preview` ride the choice,
/// for the host to show beside it; an edge without them has neither key,
/// and a blank value is the same as none.
#[test]
fn human_gate_choices_carry_the_edges_description_and_preview() {
    let graph = lower_ok(&dot(r#"
        gate [shape=hexagon, label="Deploy?"]
        yes [prompt="x"]
        no [prompt="x"]
        later [prompt="x"]
        start -> gate
        gate -> yes [label="[Y] Yes", "human.description"="Merge and deploy to production", "human.preview"="deploy --prod"]
        gate -> no [label="[N] No", "human.description"="  "]
        gate -> later [label="[L] Later"]
        yes -> exit
        no -> exit
        later -> exit
    "#));
    let config = &node(&graph, "gate").step.config;
    assert_eq!(
        config["choices"],
        json!([
            {
                "key": "Y",
                "label": "[Y] Yes",
                "to": "yes",
                "description": "Merge and deploy to production",
                "preview": "deploy --prod",
            },
            { "key": "N", "label": "[N] No", "to": "no" },
            { "key": "L", "label": "[L] Later", "to": "later" },
        ])
    );
}

/// What a host that renders a stage reads off `meta` alone: a command
/// node's `script` as the step runs it, and for every routing arm the
/// target, the label and the `condition` as written, keyed by the arm's
/// edge id, which `route.applied` names.
#[test]
fn meta_carries_the_script_and_each_edges_condition_text() {
    let graph = lower_ok(&dot(r#"
        build [shape=parallelogram, script="make build\nmake test"]
        ok [prompt="x"]
        bad [prompt="x"]
        start -> build
        build -> ok [condition="  outcome=succeeded "]
        build -> bad [label="[F] Failed"]
        ok -> exit
        bad -> exit
    "#));
    let build = node(&graph, "build");
    assert_eq!(build.meta["script"], json!("make build\nmake test"));
    assert_eq!(build.meta["script"], build.step.config["script"]);
    assert!(node(&graph, "ok").meta.get("script").is_none());
    let edges = build.meta["edges"].as_object().expect("the edge table");
    assert_eq!(edges.len(), 2);
    let arms = &build.routing.groups[0].arms;
    let entry = |i: usize| &edges[&arms[i].id.raw().to_string()];
    assert_eq!(
        entry(0),
        &json!({ "to": "ok", "label": null, "condition": "outcome=succeeded" })
    );
    assert_eq!(entry(1), &json!({ "to": "bad", "label": "[F] Failed" }));
}

#[test]
fn stylesheets_write_model_properties_that_explicit_attributes_beat() {
    let graph = lower_ok(&dot(r#"
        graph [model_stylesheet="* { model: a; } .code { model: b; reasoning_effort: high } #c { model: c }"]
        x [prompt="x"]
        y [prompt="x", class="code"]
        c [prompt="x", class="code", model="mine"]
        start -> x -> y -> c -> exit
    "#));
    assert_eq!(node(&graph, "x").step.config["model"], json!("a"));
    assert_eq!(node(&graph, "y").step.config["model"], json!("b"));
    assert_eq!(
        node(&graph, "y").step.config["reasoning_effort"],
        json!("high")
    );
    assert_eq!(node(&graph, "c").step.config["model"], json!("mine"));
    assert_eq!(node(&graph, "y").meta["model"], json!("b"));
}

#[test]
fn templates_render_inputs_and_unbound_inputs_are_specific_rejections() {
    let inputs = CompileInputs::new().with_input("name", "Ada");
    let graph = lower_ok_with(
        &dot(r#"
        graph [goal="Greet {{ inputs.name }}"]
        a [prompt="Say hi to {{ inputs.name }} for {{ goal }}"]
        c [shape=parallelogram, script="echo {{ inputs.name }} {{ goal }}"]
        start -> a -> c -> exit
    "#),
        &frontend::NoFiles,
        &inputs,
    );
    assert_eq!(
        node(&graph, "a").step.config["prompt"],
        json!("Say hi to Ada for Greet Ada")
    );
    assert_eq!(
        node(&graph, "c").step.config["script"],
        json!("echo Ada 'Greet Ada'")
    );
    assert_eq!(graph.params["goal"], json!("Greet Ada"));
    assert_eq!(graph.params["inputs"], json!({ "name": "Ada" }));
    assert!(
        codes(&dot(r#"
        a [prompt="{{ inputs.missing }}"]
        start -> a -> exit
    "#))
        .contains(&"unsupported.template.unbound_input".to_string())
    );
}

/// Imports expand at load as Fabro's transform expands them: prefixed ids,
/// dropped sentinels, spliced boundary edges, inherited defaults, propagated
/// classes, rewritten retry targets, nested imports relative to their own
/// file, and Fabro's refusals with Fabro's messages.
#[test]
fn imports_expand_at_load_with_fabro_rules() {
    let files = files(&[
        (
            "flows/checks.fabro",
            r#"digraph Checks {
                start [shape=Mdiamond]
                exit [shape=Msquare]
                lint [shape=parallelogram, script="lint", retry_target="lint"]
                test [prompt="@prompts/test.md", class="verification"]
                start -> lint -> test -> exit
            }"#,
        ),
        (
            "flows/prompts/test.md",
            "Run the tests for {{ inputs.target }}.",
        ),
        (
            "flows/outer.fabro",
            r#"digraph Outer {
                start [shape=Mdiamond]
                exit [shape=Msquare]
                inner [import="checks.fabro", model="m1"]
                start -> inner -> exit
            }"#,
        ),
        (
            "flows/loop.fabro",
            r#"digraph Loop {
                start [shape=Mdiamond]
                exit [shape=Msquare]
                again [import="loop.fabro"]
                start -> again -> exit
            }"#,
        ),
        (
            "flows/bad.fabro",
            r#"digraph Bad {
                start [shape=Mdiamond]
                exit [shape=Msquare]
                a [prompt="x"]
                b [prompt="x"]
                start -> a
                start -> b
                a -> exit
                b -> exit
            }"#,
        ),
        (
            "flows/empty.fabro",
            "digraph Empty {\n                start [shape=Mdiamond]\n                exit \
             [shape=Msquare]\n                start -> exit\n            }",
        ),
    ]);
    let inputs = CompileInputs::new().with_input("target", "main");
    let load = |text: &str| frontend_attractor::load("flows/main.fabro", text, &files, &inputs);
    let lowered = load(&dot(r#"
        build [shape=parallelogram, script="build"]
        review [import="checks.fabro", model="m2", reasoning_effort="high", class="Review Step"]
        ship [shape=parallelogram, script="ship"]
        start -> build
        build -> review [label="[G] Go"]
        review -> ship [condition="outcome=succeeded"]
        review -> exit
        ship -> exit
    "#));
    assert!(
        !lowered.diagnostics.has_errors(),
        "{:?}",
        lowered.diagnostics
    );
    let graph = lowered.graph.expect("lowers");
    let names: Vec<&str> = graph.nodes.iter().map(|n| n.name.as_str()).collect();
    assert!(names.contains(&"review.lint") && names.contains(&"review.test"));
    assert!(
        !names.contains(&"review"),
        "the placeholder is gone: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n.contains("start") && n != &"start"),
        "the import's sentinels are dropped: {names:?}"
    );
    let test = node(&graph, "review.test");
    assert_eq!(test.step.config["model"], json!("m2"), "inherited default");
    assert_eq!(test.step.config["reasoning_effort"], json!("high"));
    assert_eq!(
        test.step.config["prompt"],
        json!("Run the tests for main."),
        "an @file inside the import resolves beside the imported file"
    );
    assert_eq!(
        test.meta["classes"],
        json!(["Review", "Step", "verification", "review"]),
        "the placeholder's classes as parsed, the import's own, then Fabro's class from the \
         placeholder id"
    );
    let lint = node(&graph, "review.lint");
    assert_eq!(lint.step.kind, COMMAND_KIND);
    assert!(
        lint.step.config.get("model").is_none(),
        "a command node takes no model default"
    );
    // The boundary edges keep their attributes and reach the right ends.
    let build_tiers = tiers(&graph, "build");
    assert_eq!(
        build_tiers[0].1[0].0, "review.lint",
        "incoming edge to the entry"
    );
    let review_tiers = tiers(&graph, "review.test");
    assert_eq!(
        review_tiers[0].1[0].0, "ship",
        "outgoing edge from the exit predecessor"
    );
    // Retry targets inside the import are rewritten to the prefix.
    let goal = load(&dot(r#"
        review [import="checks.fabro"]
        start -> review -> exit
    "#));
    assert!(!goal.diagnostics.has_errors(), "{:?}", goal.diagnostics);

    // Nested imports and their cycle.
    let nested = load(&dot(r#"
        outer [import="outer.fabro"]
        start -> outer -> exit
    "#));
    assert!(!nested.diagnostics.has_errors(), "{:?}", nested.diagnostics);
    let graph = nested.graph.expect("lowers");
    assert_eq!(
        node(&graph, "outer.inner.test").step.config["model"],
        json!("m1"),
        "the inner placeholder's default reaches the doubly imported node"
    );
    let cycle = load(&dot(r#"
        again [import="loop.fabro"]
        start -> again -> exit
    "#));
    let messages: Vec<String> = cycle
        .diagnostics
        .iter()
        .filter(|d| d.code == "attractor.import")
        .map(|d| d.message.clone())
        .collect();
    assert!(
        messages
            .iter()
            .any(|m| m.contains("circular import detected")),
        "{messages:?}"
    );

    // Fabro's refusals, with Fabro's reasons.
    let refused = |body: &str| -> Vec<String> {
        load(&dot(body))
            .diagnostics
            .iter()
            .filter(|d| d.code == "attractor.import")
            .map(|d| d.message.clone())
            .collect()
    };
    assert!(
        refused("bad [import=\"bad.fabro\"]\nstart -> bad -> exit")
            .iter()
            .any(|m| m.contains("must have exactly one successor")),
    );
    assert!(
        refused("x [import=\"checks.fabro\", prompt=\"no\"]\nstart -> x -> exit")
            .iter()
            .any(|m| m.contains("has unsupported attribute 'prompt'")),
    );
    assert!(
        refused("x [import=\"missing.fabro\"]\nstart -> x -> exit")
            .iter()
            .any(|m| m.contains("file not found")),
    );
    assert!(
        refused(
            "x [import=\"empty.fabro\"]\ny [prompt=\"p\"]\nstart -> x [label=\"[A] A\"]\nx -> y\ny -> exit"
        )
        .iter()
        .any(|m| m.contains("cannot bypass semantic edges")),
    );
    // An empty import with plain edges is removed and its neighbours wired.
    let bypass = load(&dot(r#"
        x [import="empty.fabro"]
        y [prompt="p"]
        start -> x -> y -> exit
    "#));
    assert!(!bypass.diagnostics.has_errors(), "{:?}", bypass.diagnostics);
    let graph = bypass.graph.expect("lowers");
    assert_eq!(tiers(&graph, "start")[0].1[0].0, "y");
}

#[test]
fn structural_mistakes_are_specific_errors() {
    let id_only = lower_ok("digraph G { start; exit; start -> exit }");
    assert_eq!(id_only.entry, [node_id(&id_only, "start")]);
    assert_eq!(
        id_only.completion,
        Completion::TerminalNode(node_id(&id_only, "exit"))
    );
    let typed = lower_ok("digraph G { begin [type=start]; done [type=exit]; begin -> done }");
    assert_eq!(typed.entry, [node_id(&typed, "begin")]);
    assert_eq!(
        typed.completion,
        Completion::TerminalNode(node_id(&typed, "done"))
    );
    assert!(
        codes("digraph G { start; other [shape=Mdiamond]; exit; start -> other -> exit }")
            .contains(&"attractor.multiple_starts".to_string())
    );
    assert!(
        codes("digraph G { start; exit; done [shape=Msquare]; start -> exit; start -> done }")
            .contains(&"attractor.multiple_exits".to_string())
    );
    assert!(
        codes(&dot(
            "goal_check [prompt=\"x\"] start -> goal_check -> exit"
        ))
        .contains(&"attractor.reserved_node_id".to_string())
    );
    assert!(
        codes("digraph G { a [prompt=\"x\"] a -> exit  exit [shape=Msquare] }")
            .contains(&"attractor.no_start".to_string())
    );
    assert!(codes(&dot("start -> ghost")).contains(&"attractor.undeclared_node".to_string()));
    assert!(
        codes(&dot(r#"
        a [prompt="x"]
        b [prompt="x"]
        start -> a -> exit
    "#))
        .contains(&"attractor.unreachable_node".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x"]
        start -> a -> exit
        a -> start
    "#))
        .contains(&"attractor.start_has_incoming".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x"]
        start -> a -> exit -> a
    "#))
        .contains(&"attractor.exit_has_outgoing".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [llm_prompt="x"]
        start -> a -> exit
    "#))
        .contains(&"unsupported.legacy_dialect".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", timeout=1200]
        start -> a -> exit
    "#))
        .contains(&"unsupported.legacy_dialect".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", import="other.fabro"]
        start -> a -> exit
    "#))
        .contains(&"attractor.import".to_string()),
        "a missing import file is an import error"
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", frobnicate=1]
        start -> a -> exit
    "#))
        .contains(&"attractor.unknown_attribute".to_string())
    );
    assert!(
        codes(&dot(r#"
        a [prompt="x", color=red, timeout="10s"]
        start -> a -> exit
    "#))
        .is_empty(),
        "layout attributes are dropped silently"
    );
}

#[test]
fn unknown_attributes_are_refused_with_the_closest_name_as_the_hint() {
    // An attribute Fabro does not define is an error, not a warning: the
    // workflow does not load.
    let lowered = load(
        "w.fabro",
        &dot(r#"
            a [prompt="x", frobnicate="yes"]
            start -> a -> exit
        "#),
        &NoFiles,
        &CompileInputs::new(),
    );
    assert!(
        lowered.graph.is_none(),
        "an unknown attribute rejects the workflow"
    );
    let unknown: Vec<&frontend::Diagnostic> = lowered
        .diagnostics
        .iter()
        .filter(|d| d.code == "attractor.unknown_attribute")
        .collect();
    assert_eq!(unknown.len(), 1, "{:?}", lowered.diagnostics);
    assert!(unknown[0].is_error());
    assert!(
        unknown[0]
            .hint
            .as_deref()
            .is_some_and(|hint| hint.contains("`x.`")),
        "an unrelated name points at the extension namespace: {:?}",
        unknown[0].hint
    );
    // A typo names the attribute the author meant, on nodes, edges and the
    // graph alike.
    let typo = diagnostics(&dot(r#"
        graph [default_modle="m"]
        a [prompt="x", max_retrys=3]
        start -> a
        a -> exit [conditon="outcome=succeeded"]
    "#));
    let hints: Vec<String> = typo
        .iter()
        .filter(|d| d.code == "attractor.unknown_attribute")
        .map(|d| d.hint.clone().unwrap_or_default())
        .collect();
    assert_eq!(hints.len(), 3, "{typo:?}");
    assert!(
        hints.iter().any(|h| h.contains("`default_model`")),
        "{hints:?}"
    );
    assert!(
        hints.iter().any(|h| h.contains("`max_retries`")),
        "{hints:?}"
    );
    assert!(hints.iter().any(|h| h.contains("`condition`")), "{hints:?}");
    // The extension namespace is never diagnosed, wherever it appears.
    assert!(
        codes(&dot(r#"
            graph ["x.owner"="platform"]
            a [prompt="x", "x.ticket"="PLAT-12"]
            start -> a -> exit ["x.note"="generated"]
        "#))
        .is_empty(),
        "`x.` attributes are carried without a word"
    );
}

#[test]
fn unbounded_agent_repairs_are_rejected() {
    assert!(
        codes(&dot(r#"
            a [prompt="x", output_retries=101]
            start -> a -> exit
        "#))
        .contains(&"attractor.output_retries_too_large".to_string())
    );
}

#[test]
fn timeouts_lower_to_per_attempt_budgets_with_fabro_defaults() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x", timeout="20m"]
        c [shape=parallelogram, script="true"]
        start -> a -> c -> exit
    "#));
    assert_eq!(node(&graph, "a").budget.timeout, Duration::from_secs(1200));
    assert_eq!(
        node(&graph, "a").step.config["timeout_ms"],
        json!(1_200_000)
    );
    assert_eq!(node(&graph, "c").budget.timeout, Duration::from_secs(600));
}

/// Who enforces each node's timeout follows Fabro's handler policies: the
/// command sends its deadline to the sandbox, the human gate owns its answer
/// deadline, an ACP agent hands the deadline to its turn, and a native API
/// agent, a prompt on the native backend, a wait and a nested workflow keep
/// the driver's interview-aware timer.
#[test]
fn timeout_policy_follows_the_handler() {
    let graph = lower_ok(&dot(r#"
        acp [prompt="x", backend="acp", acp.command="test-agent"]
        api [prompt="x", backend="api", model="m"]
        p [shape=tab, prompt="say", backend="api", model="m"]
        c [shape=parallelogram, script="true"]
        h [shape=hexagon, label="Ok?"]
        w [shape=insulator, duration="1s"]
        start -> acp -> api -> p -> c -> h
        h -> w [label="[Y] Yes"]
        w -> exit
    "#));
    let policy = |name: &str| node(&graph, name).budget.timeout_policy;
    assert_eq!(policy("acp"), TimeoutPolicy::HandlerManaged);
    assert_eq!(policy("api"), TimeoutPolicy::ExecutorEnforced);
    assert_eq!(policy("p"), TimeoutPolicy::ExecutorEnforced);
    assert_eq!(policy("c"), TimeoutPolicy::HandlerManaged);
    assert_eq!(policy("h"), TimeoutPolicy::HandlerManaged);
    assert_eq!(policy("w"), TimeoutPolicy::ExecutorEnforced);
    assert_eq!(policy("start"), TimeoutPolicy::ExecutorEnforced);
}

/// Timeout ownership must resolve the same backend as dispatch, including
/// the implicit native API backend and graph/stylesheet overrides.
#[test]
fn native_agent_timeout_policy_follows_backend_resolution() {
    for (graph_attrs, node_attrs, backend, policy) in [
        ("", "", "api", TimeoutPolicy::ExecutorEnforced),
        (
            "",
            r#"backend="api""#,
            "api",
            TimeoutPolicy::ExecutorEnforced,
        ),
        (
            r#"backend="api""#,
            "",
            "api",
            TimeoutPolicy::ExecutorEnforced,
        ),
        ("", r#"backend="acp""#, "acp", TimeoutPolicy::HandlerManaged),
        (r#"backend="acp""#, "", "acp", TimeoutPolicy::HandlerManaged),
        (
            r#"backend="acp""#,
            r#"backend="api""#,
            "api",
            TimeoutPolicy::ExecutorEnforced,
        ),
        (
            r#"backend="api""#,
            r#"backend="acp""#,
            "acp",
            TimeoutPolicy::HandlerManaged,
        ),
        (
            r##"backend="acp", model_stylesheet="#a { backend: api; }""##,
            "",
            "api",
            TimeoutPolicy::ExecutorEnforced,
        ),
        (
            r##"model_stylesheet="#a { backend: acp; }""##,
            "",
            "acp",
            TimeoutPolicy::HandlerManaged,
        ),
        (
            r##"model_stylesheet="#a { backend: acp; }""##,
            r#"backend="api""#,
            "api",
            TimeoutPolicy::ExecutorEnforced,
        ),
    ] {
        let graph = lower_ok(&format!(
            r#"digraph T {{
            graph [acp.command="test-agent", {graph_attrs}]
            start [shape=Mdiamond]
            a [prompt="work", timeout="1s", {node_attrs}]
            exit [shape=Msquare]
            start -> a -> exit
        }}"#
        ));
        let agent = node(&graph, "a");
        assert_eq!(
            agent.step.config["backend"].as_str().unwrap_or("api"),
            backend
        );
        assert_eq!(
            agent.budget.timeout_policy, policy,
            "graph={graph_attrs}, node={node_attrs}"
        );
        assert_eq!(agent.budget.timeout, Duration::from_secs(1));
    }
}

/// `stall_timeout` and `loop_restart_signature_limit` lower to the graph's
/// run policy with Fabro's defaults; zero disables the watchdog and a limit
/// below one is refused.
#[test]
fn run_policies_lower_with_fabro_defaults() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert_eq!(
        graph.policy.stall_timeout,
        Some(Duration::from_secs(30 * 60))
    );
    assert_eq!(
        graph
            .policy
            .loop_restart_signature_limit
            .map(NonZeroU32::get),
        Some(3)
    );
    let graph = lower_ok(&dot(r#"
        graph [stall_timeout="0s", loop_restart_signature_limit=5]
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert_eq!(graph.policy.stall_timeout, None);
    assert_eq!(
        graph
            .policy
            .loop_restart_signature_limit
            .map(NonZeroU32::get),
        Some(5)
    );
    let graph = lower_ok(&dot(r#"
        graph [stall_timeout="90s"]
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert_eq!(graph.policy.stall_timeout, Some(Duration::from_secs(90)));
    let codes = codes(&dot(r#"
        graph [loop_restart_signature_limit=0]
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert!(
        codes.contains(&"attractor.bad_signature_limit".to_string()),
        "{codes:?}"
    );
    let diags = diagnostics(&dot(r#"
        graph [stall_timeout="5m", loop_restart_signature_limit=2]
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert!(
        !diags
            .iter()
            .any(|d| d.code.starts_with("ignored.stall_timeout")
                || d.code.starts_with("ignored.loop_restart")),
        "{diags:?}"
    );
}

/// A human gate's review target and default choice lower into its config;
/// a default that names none of the gate's choices is refused.
#[test]
fn human_gate_review_target_and_default_choice_lower() {
    let graph = lower_ok(&dot(r#"
        h [shape=hexagon, label="Ok?", review_target=true, human.default_choice="deploy", timeout="90s"]
        deploy [shape=parallelogram, script="true"]
        hold [shape=parallelogram, script="true"]
        start -> h
        h -> deploy [label="[D] Deploy"]
        h -> hold [label="[H] Hold"]
        deploy -> exit
        hold -> exit
    "#));
    let config = &node(&graph, "h").step.config;
    assert_eq!(config["review_target"], json!(true));
    assert_eq!(config["default_choice"], json!("deploy"));
    assert_eq!(config["timeout_ms"], json!(90_000));
    let codes = codes(&dot(r#"
        h [shape=hexagon, label="Ok?", human.default_choice="nowhere"]
        a [shape=parallelogram, script="true"]
        start -> h
        h -> a [label="[A] A"]
        a -> exit
    "#));
    assert!(
        codes.contains(&"attractor.bad_default_choice".to_string()),
        "{codes:?}"
    );
}

#[test]
fn stdin_source_reads_the_context_or_the_fan_in() {
    let graph = lower_ok(&dot(r#"
        fork [shape=component]
        a [prompt="x"]
        merge [shape=tripleoctagon]
        m [shape=parallelogram, script="cat", stdin_source="context.parallel.results"]
        k [shape=parallelogram, script="cat", stdin_source="context.output.a"]
        start -> fork -> a -> merge -> m -> k -> exit
    "#));
    let stdin = |name: &str| {
        let id = node(&graph, name).step.config["stdin"]["$expr"]
            .as_u64()
            .expect("placeholder");
        print_expr(
            &graph.exprs,
            ir::ExprId::new(u32::try_from(id).expect("u32")),
        )
    };
    // The fan-in publishes `parallel.results` into the context, where a
    // later command reads it like any other key.
    assert_eq!(stdin("m"), "get(kv, 'parallel.results')");
    assert_eq!(stdin("k"), "get(kv, 'output.a')");
}

#[test]
fn negative_weights_shift_so_the_lowest_is_zero_and_order_holds() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x"]
        b [prompt="x"]
        c [prompt="x"]
        d [prompt="x"]
        start -> a
        a -> b [weight=-1]
        a -> c [weight=-5]
        a -> d
        a -> exit [weight=2]
        b -> exit
        c -> exit
        d -> exit
    "#));
    let weights: Vec<u32> = node(&graph, "a").routing.groups[0]
        .arms
        .iter()
        .map(|a| a.weight)
        .collect();
    assert_eq!(weights, [4, 0, 5, 7], "shifted by the lowest, -5");
    let graph = lower_ok(&dot(r#"
        graph [selection="random"]
        a [prompt="x"]
        b [prompt="x"]
        start -> a
        a -> b [weight=-3]
        a -> exit [weight=2]
        b -> exit
    "#));
    let weights: Vec<u32> = node(&graph, "a").routing.groups[0]
        .arms
        .iter()
        .map(|a| a.weight)
        .collect();
    assert_eq!(
        weights,
        [1, 2],
        "random: a weight at or below zero counts as one"
    );
}

/// REMOVE AFTER 2026-10-04 with the shim.
#[test]
fn a_succeed_node_reads_its_converted_failure_as_succeeded() {
    let graph = lower_ok(&dot(r#"
        a [prompt="x", on_failure="succeed"]
        b [prompt="x"]
        c [prompt="x"]
        d [prompt="x"]
        start -> a
        a -> b [condition="outcome=succeeded"]
        a -> c [condition="outcome=partially_succeeded"]
        a -> d [condition="outcome=failed"]
        a -> exit
        b -> exit
        c -> exit
        d -> exit
    "#));
    let tiers = tiers(&graph, "a");
    let (succeeded, partial, failed) = (tiers[0].1[0].1, tiers[0].1[1].1, tiers[0].1[2].1);
    let converted = statics("partial_success", &json!({ "outcome": "succeeded" }));
    assert!(eval_guard(&graph, succeeded, &converted, &[]));
    assert!(!eval_guard(&graph, partial, &converted, &[]));
    assert!(!eval_guard(&graph, failed, &converted, &[]));
    let exhausted = statics("partial_success", &json!({ "outcome": "failed" }));
    assert!(
        eval_guard(&graph, succeeded, &exhausted, &[]),
        "retries that ran out under `succeed` read as succeeded too"
    );
    let genuine = statics(
        "partial_success",
        &json!({ "outcome": "partially_succeeded" }),
    );
    assert!(!eval_guard(&graph, succeeded, &genuine, &[]));
    assert!(eval_guard(&graph, partial, &genuine, &[]));
    let clean = statics("success", &json!({ "outcome": "succeeded" }));
    assert!(eval_guard(&graph, succeeded, &clean, &[]));
    assert!(matches!(
        tiers.last().expect("fallback").1[0].1,
        Guard::Always
    ));
}

#[test]
fn a_check_with_no_inputs_warns_on_unbound_inputs_and_keeps_the_text() {
    let text = dot(r#"
        graph [goal="Fix {{ inputs.pr }}"]
        a [prompt="Look at {{ inputs.pr }} for {{ vars.owner }}"]
        c [shape=parallelogram, script="gh pr view {{ inputs.pr }}"]
        start -> a -> c -> exit
    "#);
    let lenient = CompileInputs::new().with_unbound_as_warning();
    let lowered = frontend_attractor::load("w.fabro", &text, &frontend::NoFiles, &lenient);
    let graph = lowered.graph.expect("lowers with warnings");
    let found: Vec<&str> = lowered
        .diagnostics
        .iter()
        .map(|d| d.code.as_str())
        .collect();
    assert!(
        found.iter().all(|c| *c == "attractor.unbound_input"),
        "{found:?}"
    );
    assert_eq!(lowered.diagnostics.errors().count(), 0);
    assert_eq!(
        node(&graph, "a").step.config["prompt"],
        json!("Look at {{ inputs.pr }} for {{ vars.owner }}"),
        "left unrendered"
    );
    assert_eq!(
        node(&graph, "c").step.config["script"],
        json!("gh pr view {{ inputs.pr }}")
    );
    assert!(
        codes(&text).contains(&"unsupported.template.unbound_input".to_string()),
        "strict without the flag"
    );
}

#[test]
fn a_template_local_set_inside_an_if_renders_and_a_missing_input_is_named() {
    let text = dot(r#"
        graph [model_stylesheet="
            {% if 'kimi' in inputs.model %}{% set effort = 'high' %}{% else %}{% set effort = 'low' %}{% endif %}
            * { model: {{ inputs.model }}; reasoning_effort: {{ effort }}; }
        "]
        a [prompt="x"]
        start -> a -> exit
    "#);
    let graph = lower_ok_with(
        &text,
        &frontend::NoFiles,
        &CompileInputs::new().with_input("model", "kimi-k3"),
    );
    assert_eq!(node(&graph, "a").step.config["model"], json!("kimi-k3"));
    assert_eq!(
        node(&graph, "a").step.config["reasoning_effort"],
        json!("high")
    );
    let strict = diagnostics(&text);
    assert!(
        strict
            .iter()
            .any(|d| d.code == "unsupported.template.unbound_input"
                && d.message.contains("inputs.model")),
        "the missing input is named, not the template-local `effort`: {strict:?}"
    );
    // A lenient check skips the stylesheet it could not render instead of
    // parsing the template text as a stylesheet.
    let lenient = frontend_attractor::load(
        "w.fabro",
        &text,
        &frontend::NoFiles,
        &CompileInputs::new().with_unbound_as_warning(),
    );
    let found: Vec<&str> = lenient
        .diagnostics
        .iter()
        .map(|d| d.code.as_str())
        .collect();
    assert_eq!(found, ["attractor.unbound_input"], "{found:?}");
    assert!(lenient.graph.is_some());
}

#[test]
fn threads_fidelity_memory_and_controls_lower_onto_agent_nodes_and_edges() {
    let graph = lower_ok(&dot(r#"
        graph [default_fidelity="full", default_thread="shared"]
        plan [prompt="plan", thread_id="impl", class="build"]
        work [prompt="work", fidelity="summary:low", project_memory=false, speed="fast", max_tokens=2000]
        start -> plan
        plan -> work [fidelity="truncate", thread_id="side"]
        work -> exit
    "#));
    let plan = &node(&graph, "plan").step.config;
    assert!(
        plan.get("fidelity").is_none(),
        "the graph default is not the node's own"
    );
    assert_eq!(plan["default_fidelity"], json!("full"));
    assert_eq!(plan["thread_id"], json!("impl"));
    assert_eq!(plan["default_thread"], json!("shared"));
    assert_eq!(plan["classes"], json!(["build"]));
    assert!(plan.get("project_memory").is_none());
    assert!(contains_placeholder(&plan["incoming"]));
    assert!(
        plan["stages"]
            .as_array()
            .is_some_and(|s| s.iter().any(|st| st["id"] == "work"))
    );
    let work = &node(&graph, "work").step.config;
    assert_eq!(work["fidelity"], json!("summary:low"));
    assert_eq!(work["project_memory"], json!(false));
    assert_eq!(work["speed"], json!("fast"));
    assert_eq!(work["max_tokens"], json!(2000));
    // The edge into `work` carries its own fidelity and thread.
    let plan_node = node(&graph, "plan");
    let arm = &plan_node.routing.groups[0].arms[0];
    let map = arm.map.expect("the edge maps its payload");
    let payload = eval_expr(&graph, map, &statics("success", &json!({})), &[]);
    assert_eq!(
        payload,
        json!({"from": "plan", "fidelity": "truncate", "thread_id": "side"})
    );
    assert_eq!(graph.params["attractor.hooks"], json!([]));
}

#[test]
fn thread_ids_without_full_fidelity_warn_and_bad_modes_are_errors() {
    let codes = codes(&dot(r#"
        graph [default_thread="shared"]
        a [prompt="x", thread_id="t"]
        b [prompt="y"]
        start -> a
        a -> b [thread_id="u"]
        b -> exit
    "#));
    assert!(
        codes.contains(&"attractor.thread_id_requires_fidelity_full".to_string()),
        "{codes:?}"
    );
    assert!(
        !codes.iter().any(|c| c.starts_with("ignored.")),
        "{codes:?}"
    );
    let bad = codes_of(&dot(r#"
        a [prompt="x", fidelity="loud", speed="warp"]
        start -> a
        a -> exit [fidelity="quiet"]
    "#));
    assert!(
        bad.contains(&"attractor.bad_fidelity".to_string()),
        "{bad:?}"
    );
    assert!(bad.contains(&"attractor.bad_speed".to_string()), "{bad:?}");
    // A thread on a parallel branch is inert, and says so.
    let branch = codes_of(&dot(r#"
        fork [shape=component]
        a [prompt="x", thread_id="t", fidelity="full"]
        join [shape=tripleoctagon]
        start -> fork -> a -> join -> exit
    "#));
    assert!(
        branch.contains(&"attractor.parallel_branch_inert_attribute".to_string()),
        "{branch:?}"
    );
    // `tool_hooks.*` are not Fabro attributes.
    let unknown = diagnostics(&dot(r#"
        a [prompt="x", tool_hooks.pre="echo", tool_hooks.post="echo"]
        start -> a -> exit
    "#));
    let unknown: Vec<&frontend::Diagnostic> = unknown
        .iter()
        .filter(|d| d.code == "attractor.unknown_attribute")
        .collect();
    assert_eq!(unknown.len(), 2, "{unknown:?}");
    assert!(
        unknown
            .iter()
            .all(|d| d.is_error() && d.message.contains("tool_hooks.")),
        "{unknown:?}"
    );
    assert!(
        unknown.iter().all(|d| d
            .hint
            .as_deref()
            .is_some_and(|h| h.contains("[[run.hooks]]"))),
        "the hint names where tool hooks are configured: {unknown:?}"
    );
}

fn codes_of(text: &str) -> Vec<String> {
    codes(text)
}

/// Every agent node carries Fabro's compaction values (always on, 80
/// percent of the context window, six preserved turns); a prompt node, which
/// runs no agent loop, carries none.
#[test]
fn agent_nodes_carry_fabros_compaction_settings() {
    let graph = lower_ok(&dot(r#"
        graph [backend="api", default_model="openai/gpt-5.6-sol"]
        a [prompt="Work."]
        p [shape=tab, prompt="Answer."]
        start -> a -> p -> exit
    "#));
    assert_eq!(
        node(&graph, "a").step.config["compaction"],
        json!({"enabled": true, "threshold_percent": 80, "preserve_turns": 6})
    );
    assert!(node(&graph, "p").step.config.get("compaction").is_none());
}

/// The language alone: a `workflow.toml` beside the file, a
/// `.fabro/project.toml` at the root and a settings-layer variable are not
/// this frontend's to read. The same DOT lowers to the same graph with and
/// without them, and none of their sections reaches a node. The Fabro
/// frontend is what applies them (`crates/fabro/frontend/tests/lowering.rs`).
#[test]
fn attractor_lowers_the_same_graph_beside_fabro_settings_files() {
    let text = dot(r#"
        graph [default_model="graph/model"]
        a [prompt="Do the work"]
        c [shape=parallelogram, script="true"]
        start -> a -> c -> exit
    "#);
    // The settings-layer text rides a compile variable, and every compile
    // variable lands in the graph's `vars` parameter whoever reads it, so
    // both lowerings get the same inputs and only the files differ.
    let inputs = CompileInputs::new().with_var(
        "fabro.settings_toml",
        "[run.model]\nprovider = \"settings-provider\"\n",
    );
    let bare = lower_ok_with(&text, &NoFiles, &inputs);
    let with_files = files(&[
        (
            "workflow.toml",
            "_version = 1\n[run.model]\nname = \"toml/model\"\n[run.prepare]\nsteps = [{ script = \
             \"echo prepared\" }]\n[[run.hooks]]\nevent = \"stage_start\"\nscript = \"true\"\n\
             [run.agent.mcps.files]\ntype = \"stdio\"\ncommand = [\"true\"]\n",
        ),
        (
            ".fabro/project.toml",
            "[run.model]\nprovider = \"project-provider\"\n",
        ),
    ]);
    let beside = lower_ok_with(&text, &with_files, &inputs);
    assert_eq!(
        frontend::graph_digest(&bare),
        frontend::graph_digest(&beside),
        "the settings files change nothing in the language's lowering"
    );
    let agent = node(&beside, "a");
    assert_eq!(agent.step.config["model"], json!("graph/model"));
    assert!(agent.step.config.get("provider").is_none());
    assert_eq!(
        agent.step.config["mcps"],
        json!([]),
        "no server reaches the node"
    );
    assert!(
        !beside
            .nodes
            .iter()
            .any(|n| n.name.starts_with(frontend_attractor::PREPARE_NODE_PREFIX)),
        "no prepare node"
    );
    assert_eq!(beside.params["attractor.hooks"], json!([]));
    assert!(!beside.params.contains_key("fabro.launch"));
}

// ── Fabro's lint rules (`crates/attractor/LINTS.md`) ───────────────────────

/// The first diagnostic with `code`, when the workflow raises one.
fn diagnostic_with(text: &str, code: &str) -> Option<frontend::Diagnostic> {
    diagnostics(text).into_iter().find(|d| d.code == code)
}

fn has_code(text: &str, code: &str) -> bool {
    codes(text).contains(&code.to_string())
}

/// Fabro's `all_conditional_edges` (and its subset `orphan_custom_outcome`):
/// a node whose every edge has a condition has no fallback.
#[test]
fn all_conditional_edges_need_an_unconditional_fallback() {
    let d = diagnostic_with(
        &dot(r#"
        a [prompt="x"]
        b [prompt="y"]
        start -> a
        a -> b [condition="outcome=succeeded"]
        a -> exit [condition="outcome=failed"]
        b -> exit
    "#),
        "attractor.all_conditional_edges",
    )
    .expect("refused");
    assert!(d.is_error());
    assert!(d.message.contains("`a`"), "{d:?}");
    assert!(d.hint.is_some(), "{d:?}");
    assert!(
        codes(&dot(r#"
        a [prompt="x"]
        b [prompt="y"]
        start -> a
        a -> b [condition="outcome=succeeded"]
        a -> exit
        b -> exit
    "#))
        .is_empty(),
        "an unconditional edge is the fallback"
    );
}

/// Fabro's `inert_attribute` and `script_prompt_conflict`.
#[test]
fn inert_attributes_warn_and_a_script_beside_a_prompt_is_refused() {
    let inert: Vec<frontend::Diagnostic> = diagnostics(&dot(r#"
        h [shape=hexagon, output_schema="routing"]
        w [shape=insulator, duration="1s", script="ls"]
        start -> h
        h -> w [label="Go"]
        w -> exit
    "#))
    .into_iter()
    .filter(|d| d.code == "attractor.inert_attribute")
    .collect();
    assert_eq!(inert.len(), 2, "{inert:?}");
    assert!(inert.iter().all(|d| !d.is_error() && d.hint.is_some()));
    assert!(
        inert
            .iter()
            .any(|d| d.message.contains("`output_schema` on node `h`")),
        "{inert:?}"
    );
    assert!(
        inert
            .iter()
            .any(|d| d.message.contains("`script` on node `w`")),
        "{inert:?}"
    );
    // A prompted fan-in is a prompt node here, so it reads `output_schema`.
    assert!(
        !has_code(
            &dot(r#"
        fork [shape=component]
        a [prompt="x"]
        j [shape=tripleoctagon, prompt="summarize", output_schema="routing"]
        start -> fork -> a -> j -> exit
    "#),
            "attractor.inert_attribute"
        ),
        "a prompted fan-in reads output_schema"
    );
    let conflict = diagnostic_with(
        &dot(r#"
        c [prompt="x", script="ls"]
        start -> c -> exit
    "#),
        "attractor.script_prompt_conflict",
    )
    .expect("refused");
    assert!(conflict.is_error());
    assert!(conflict.hint.is_some());
}

/// Fabro's `for_each_contract`, first clause: `for_each` fans out only on a
/// parallel node.
#[test]
fn for_each_on_a_node_that_is_not_parallel_is_refused() {
    let d = diagnostic_with(
        &dot(r#"
        a [prompt="x", for_each="context.items"]
        start -> a -> exit
    "#),
        "attractor.for_each.not_parallel",
    )
    .expect("refused");
    assert!(d.is_error());
    assert!(d.hint.is_some());
}

/// Fabro's `retry_target_exists`, on the node and on the graph.
#[test]
fn retry_targets_that_name_no_node_warn() {
    let missing: Vec<frontend::Diagnostic> = diagnostics(&dot(r#"
        graph [fallback_retry_target="nowhere"]
        a [prompt="x", goal_gate=true, retry_target="ghost"]
        start -> a -> exit
    "#))
    .into_iter()
    .filter(|d| d.code == "attractor.retry_target_not_found")
    .collect();
    assert_eq!(missing.len(), 2, "{missing:?}");
    assert!(missing.iter().all(|d| !d.is_error() && d.hint.is_some()));
    assert!(missing.iter().any(|d| d.message.contains("\"ghost\"")));
    assert!(missing.iter().any(|d| d.message.contains("the graph")));
    assert!(!has_code(
        &dot(r#"
        a [prompt="x", goal_gate=true, retry_target="a"]
        start -> a -> exit
    "#),
        "attractor.retry_target_not_found"
    ));
}

/// Fabro's `script_absolute_cd`.
#[test]
fn a_script_that_changes_to_an_absolute_directory_warns() {
    let d = diagnostic_with(
        &dot(r#"
        c [shape=parallelogram, script="cd /tmp && make"]
        start -> c -> exit
    "#),
        "attractor.script_absolute_cd",
    )
    .expect("warned");
    assert!(!d.is_error());
    assert!(d.hint.is_some());
    assert!(!has_code(
        &dot(r#"
        c [shape=parallelogram, script="cd src && make"]
        start -> c -> exit
    "#),
        "attractor.script_absolute_cd"
    ));
}

/// Fabro's `direction_valid`.
#[test]
fn an_unknown_rankdir_warns() {
    let d = diagnostic_with(
        &dot(r#"
        rankdir=XY
        a [prompt="x"]
        start -> a -> exit
    "#),
        "attractor.bad_rankdir",
    )
    .expect("warned");
    assert!(!d.is_error());
    assert!(d.hint.as_deref().is_some_and(|h| h.contains("LR")));
    assert!(
        codes(&dot(r#"
        graph [rankdir=LR]
        a [prompt="x"]
        start -> a -> exit
    "#))
        .is_empty()
    );
}

/// Fabro's `reserved_keyword_node_id`, case-insensitively.
#[test]
fn a_node_id_that_is_a_dot_keyword_warns() {
    let reserved: Vec<frontend::Diagnostic> = diagnostics(&dot(r#"
        Strict [prompt="x"]
        if [prompt="y"]
        start -> Strict -> if -> exit
    "#))
    .into_iter()
    .filter(|d| d.code == "attractor.reserved_keyword_node_id")
    .collect();
    assert_eq!(reserved.len(), 2, "{reserved:?}");
    assert!(reserved.iter().all(|d| !d.is_error() && d.hint.is_some()));
}

/// Fabro's `backend_valid`, the ACP clauses.
#[test]
fn acp_agents_name_an_agent_and_take_no_api_only_attributes() {
    let d = diagnostic_with(
        &dot(r#"
        a [prompt="x", backend="acp"]
        start -> a -> exit
    "#),
        "attractor.acp_requires_command",
    )
    .expect("refused");
    assert!(d.is_error());
    assert!(d.hint.as_deref().is_some_and(|h| h.contains("acp.command")));
    let d = diagnostic_with(
        &dot(r#"
        a [prompt="x", backend="acp", acp.command="agent", model="m", speed="fast"]
        start -> a -> exit
    "#),
        "attractor.acp_api_only_attributes",
    )
    .expect("refused");
    assert!(d.is_error());
    assert!(d.message.contains("`model`, `speed`"), "{d:?}");
    assert!(d.hint.is_some());
    // The graph may name the agent, and a stylesheet's model is not the
    // node's own attribute.
    let quiet = codes(&dot(r#"
        graph [backend="acp", acp.command="agent", model_stylesheet="* { model: m }"]
        a [prompt="x"]
        start -> a -> exit
    "#));
    assert!(
        !quiet.iter().any(|c| c.starts_with("attractor.acp_")),
        "{quiet:?}"
    );
}

/// Fabro's `join_policy_removed`: the attribute is not in the dialect, and
/// the refusal says what replaced it.
#[test]
fn join_policy_is_refused_with_a_removal_hint() {
    let d = diagnostic_with(
        &dot(r#"
        a [prompt="x", join_policy="all"]
        start -> a -> exit
    "#),
        "attractor.unknown_attribute",
    )
    .expect("refused");
    assert!(d.is_error());
    assert!(
        d.hint.as_deref().is_some_and(|h| h.contains("join_policy")),
        "{d:?}"
    );
}

/// The Fabro rules the lowering already covered, each by its Petri code.
#[test]
fn covered_fabro_rules_raise_their_petri_codes() {
    // command_requires_script
    assert!(has_code(
        &dot("c [shape=parallelogram] start -> c -> exit"),
        "attractor.command_requires_script"
    ));
    // freeform_edge_count
    assert!(has_code(
        &dot(r#"
        h [shape=hexagon]
        a [prompt="x"]
        b [prompt="y"]
        start -> h
        h -> a [freeform=true]
        h -> b [freeform=true]
        a -> exit
        b -> exit
    "#),
        "attractor.freeform_edge_count"
    ));
    // goal_gate_has_retry
    assert!(has_code(
        &dot(r#"a [prompt="x", goal_gate=true] start -> a -> exit"#),
        "attractor.goal_gate_without_target"
    ));
    // prompt_on_llm_nodes (a warning; Petri warns even when a label stands in)
    let d = diagnostic_with(
        &dot(r#"a [label="Do it"] start -> a -> exit"#),
        "attractor.prompt_missing",
    )
    .expect("warned");
    assert!(!d.is_error());
    // selection_valid
    assert!(has_code(
        &dot(r#"a [prompt="x", selection="sometimes"] start -> a -> exit"#),
        "attractor.bad_selection"
    ));
    // stdin_source_valid
    assert!(has_code(
        &dot(r#"c [shape=parallelogram, script="cat", stdin_source=""] start -> c -> exit"#),
        "attractor.bad_stdin_source"
    ));
    // stylesheet_syntax
    assert!(has_code(
        &dot(r#"graph [model_stylesheet="* model: a }"] a [prompt="x"] start -> a -> exit"#),
        "attractor.stylesheet.syntax"
    ));
    // terminal_node
    assert!(has_code(
        r#"digraph G { start [shape=Mdiamond]; a [prompt="x"]; start -> a }"#,
        "attractor.no_exit"
    ));
    // type_known
    assert!(has_code(
        &dot(r#"a [type="widget", prompt="x"] start -> a -> exit"#),
        "attractor.unknown_type"
    ));
    // for_each_contract: the source, the template edge count
    assert!(has_code(
        &dot(r#"
        fork [shape=component, for_each=""]
        a [prompt="x"]
        j [shape=tripleoctagon]
        start -> fork -> a -> j -> exit
    "#),
        "attractor.for_each.source"
    ));
    assert!(has_code(
        &dot(r#"
        fork [shape=component, for_each="context.items"]
        a [prompt="x"]
        b [prompt="y"]
        j [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b
        a -> j
        b -> j
        j -> exit
    "#),
        "attractor.for_each.template_edges"
    ));
    // backend_valid: an unknown backend, ACP on a prompt node, both ACP
    // spellings at once
    assert!(has_code(
        &dot(r#"a [prompt="x", backend="cli"] start -> a -> exit"#),
        "attractor.bad_backend"
    ));
    assert!(has_code(
        &dot(r#"p [shape=tab, prompt="x", backend="acp"] start -> p -> exit"#),
        "attractor.prompt_backend"
    ));
    assert!(has_code(
        &dot(
            r#"a [prompt="x", backend="acp", acp.command="agent", acp.config="{}"] start -> a -> exit"#
        ),
        "attractor.acp_both"
    ));
    // on_failure_valid: an edge does not take the attribute (Petri refuses
    // it where Fabro warns)
    assert!(has_code(
        &dot(r#"a [prompt="x"] start -> a  a -> exit [on_failure="route"]"#),
        "attractor.unknown_attribute"
    ));
}

/// The `fidelity="full"` clause of Fabro's `parallel_branch_inert_attribute`:
/// a branch runs at most at `summary:high`, on the fork edge and on the
/// branch's first node.
#[test]
fn full_fidelity_on_a_parallel_branch_warns_that_it_is_degraded() {
    let degraded: Vec<frontend::Diagnostic> = diagnostics(&dot(r#"
        fork [shape=component]
        a [prompt="x", fidelity="full"]
        b [prompt="y"]
        join [shape=tripleoctagon]
        start -> fork
        fork -> a
        fork -> b [fidelity="full"]
        a -> join
        b -> join
        join -> exit
    "#))
    .into_iter()
    .filter(|d| {
        d.code == "attractor.parallel_branch_inert_attribute" && d.message.contains("degraded")
    })
    .collect();
    assert_eq!(degraded.len(), 2, "{degraded:?}");
    assert!(degraded.iter().all(|d| !d.is_error() && d.hint.is_some()));
    assert!(degraded.iter().any(|d| d.message.contains("`a`")));
    assert!(degraded.iter().any(|d| d.message.contains("fork -> b")));
}
