//! The settings a Fabro host's sandbox provider plugins need admitted: an
//! environment on any provider kind (such as `gcloud`), and the run
//! sections such a host acts on itself, `[run.pull_request_review]` and
//! `[[run.services]]`.

mod support;

use frontend::{CompileInputs, Diagnostic, Frontend};
use frontend_fabro::{Fabro, load};
use serde_json::json;
use support::*;

const WORKFLOW: &str = "wf/workflow.fabro";

fn lower(workflow_toml: &str) -> (Option<ir::Graph>, Vec<Diagnostic>) {
    let lowered = load(
        WORKFLOW,
        &dot("a [shape=parallelogram, script=\"true\"]\nstart -> a -> exit"),
        &files(&[("wf/workflow.toml", workflow_toml)]),
        &CompileInputs::new(),
    );
    let diagnostics = lowered.diagnostics.iter().cloned().collect();
    (lowered.graph, diagnostics)
}

fn codes(diagnostics: &[Diagnostic]) -> Vec<String> {
    diagnostics.iter().map(|d| d.code.to_string()).collect()
}

#[test]
fn an_environment_on_a_named_provider_selects_that_backend() {
    let (graph, diagnostics) = lower(
        "[run.environment]\nid = \"vm\"\n\
         [environments.vm]\nprovider = \"gcloud\"\n\
         [environments.vm.env]\nLANG = \"C.UTF-8\"\n",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:#?}");
    let graph = graph.expect("lowers");
    let launch = Fabro::new().launch_settings(&graph);
    assert_eq!(launch.sandbox_backend.as_deref(), Some("gcloud"));
    assert_eq!(
        graph.params["fabro.environment"]["provider"],
        json!("gcloud")
    );

    // The built-in providers keep their backends.
    for (provider, backend) in [
        ("local", "host"),
        ("docker", "docker"),
        ("daytona", "daytona"),
    ] {
        let (graph, _) = lower(&format!(
            "[run.environment]\nid = \"e\"\n[environments.e]\nprovider = \"{provider}\"\n"
        ));
        let launch = Fabro::new().launch_settings(&graph.expect("lowers"));
        assert_eq!(
            launch.sandbox_backend.as_deref(),
            Some(backend),
            "{provider}"
        );
    }
}

#[test]
fn a_named_provider_runs_natively_so_an_image_is_ignored() {
    let (graph, diagnostics) = lower(
        "[run.environment]\nid = \"vm\"\n\
         [environments.vm]\nprovider = \"gcloud\"\n\
         [environments.vm.image]\ndocker = \"ghcr.io/acme/review:1\"\n",
    );
    assert_eq!(codes(&diagnostics), [
        "ignored.workflow_toml.environments.vm.image"
    ]);
    assert!(
        diagnostics[0]
            .message
            .contains("`gcloud` provider runs the scope natively")
    );
    let graph = graph.expect("lowers");
    assert_eq!(graph.params["fabro.environment"]["image"], json!(null));
}

#[test]
fn a_named_providers_configuration_is_the_platforms_and_passes_silently() {
    let (graph, diagnostics) = lower(
        "[run.environment]\nid = \"vm\"\n\
         [run.environment.provider_config.gcloud]\nmachine_type = \"e2-standard-8\"\n\
         [environments.vm]\nprovider = \"gcloud\"\n\
         [environments.vm.provider_config.gcloud]\nzone = \"europe-north1-a\"\n",
    );
    assert!(diagnostics.is_empty(), "{diagnostics:#?}");
    let launch = Fabro::new().launch_settings(&graph.expect("lowers"));
    assert_eq!(launch.sandbox_backend.as_deref(), Some("gcloud"));
}

#[test]
fn resources_on_a_named_provider_are_its_configurations() {
    let (_, diagnostics) = lower(
        "[run.environment]\nid = \"vm\"\n\
         [environments.vm]\nprovider = \"gcloud\"\n\
         [environments.vm.resources]\ncpu = 8\n",
    );
    assert_eq!(codes(&diagnostics), [
        "ignored.workflow_toml.environments.vm.resources"
    ]);
    assert!(
        diagnostics[0]
            .message
            .contains("`gcloud` provider sizes its sandboxes from its own configuration"),
        "{}",
        diagnostics[0].message
    );
}

#[test]
fn a_provider_that_is_not_a_provider_kind_is_refused() {
    for provider in ["K8s", "host", "simulated", "-vm", "vm-", "g cloud", ""] {
        let (_, diagnostics) = lower(&format!(
            "[run.environment]\nid = \"e\"\n[environments.e]\nprovider = \"{provider}\"\n"
        ));
        assert_eq!(
            codes(&diagnostics),
            ["unsupported.workflow_toml.environments.provider"],
            "{provider:?}"
        );
    }
}

#[test]
fn review_and_services_sections_are_admitted_for_the_host() {
    let (graph, diagnostics) = lower(
        "[run.pull_request_review]\nenabled = true\n\
         [[run.services]]\nname = \"app\"\ncommand = \"bin/dev\"\n\
         ready = { http = \"http://127.0.0.1:4000/health\", timeout = \"300s\" }\n\
         [[run.services]]\nname = \"queue\"\ncommand = \"bin/queue\"\n",
    );
    assert!(graph.is_some(), "{diagnostics:#?}");
    assert!(
        diagnostics.iter().all(|d| !d.is_error()),
        "{diagnostics:#?}"
    );
    assert_eq!(codes(&diagnostics), [
        "ignored.workflow_toml.run.pull_request_review",
        "ignored.workflow_toml.run.services",
    ]);
    assert!(
        diagnostics
            .iter()
            .all(|d| d.message.contains("is ignored: "))
    );
}
