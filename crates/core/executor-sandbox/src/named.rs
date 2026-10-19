//! Sandboxes of a [`SandboxBackend::Named`](crate::SandboxBackend::Named)
//! provider: any kind other than Host, Docker or Daytona, reached through
//! the linked factory an embedder registers under its name
//! ([`RoutingExecutor::with_in_process`](crate::RoutingExecutor::with_in_process)).
//! A standalone router registers no plugin for such a kind, so it fails at
//! acquire as not configured.
//!
//! Petri assumes nothing of such a provider's allocation model. The
//! provider owns its image, machine size, workspace, user and network
//! defaults; Petri contributes the run's labels, the sandbox's name and the
//! run's non-secret `provider_config`, and runs the scope's processes
//! natively in whatever the provider creates. What a native sandbox cannot
//! realize, a container target or scope services, is refused before
//! anything is allocated.

use executor::{EnvError, ScopeSpec};
use ir::RuntimeTarget;
use sandbox_driver::{ProviderKind, SandboxKind, SandboxSource, SandboxSpec};

/// The create request for `scope` on the named provider `kind`. Resources
/// are never set: the provider's configuration decides them.
pub(crate) fn build_plugin_spec(
    scope: &ScopeSpec,
    labels: &[(String, String)],
    name: &str,
    kind: &ProviderKind,
    provider_config: &serde_json::Value,
) -> Result<SandboxSpec, EnvError> {
    if !matches!(scope.runtime.target, RuntimeTarget::HostProcess) {
        return Err(EnvError::backend(
            kind.as_str(),
            "acquire",
            format!(
                "the {kind} provider runs scopes as native processes; a `container:` target needs \
                 the docker or daytona backend"
            ),
        ));
    }
    if !scope.services.is_empty() {
        return Err(EnvError::backend(
            kind.as_str(),
            "acquire",
            format!(
                "the {kind} provider cannot run scope services; services need the docker or \
                 daytona backend"
            ),
        ));
    }
    let mut spec = SandboxSpec::new(SandboxSource::HostDirectory)
        .name(name)
        .sandbox_kind(SandboxKind::VirtualMachine)
        .provider_config(provider_config.clone());
    spec.public = Some(false);
    spec.labels.extend(labels.iter().cloned());
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use executor::{AcquireContext, Executor, Retention, ServiceSpec};
    use ir::{RuntimeSpec, ScopeId};
    use sandbox_driver::{
        Capabilities, Capability, Error as DriverError, EventContext, HealthStatus, Isolation,
        ProviderHealth, Resources, Sandbox, SandboxFilter, SandboxId, SandboxProvider,
        SandboxStatus,
    };
    use serde_json::json;
    use testkit::RunDir;

    use super::*;
    use crate::in_process::{
        InProcessProviders, ProviderContext, ProviderFactory, ProviderNetwork,
    };
    use crate::{RUN_LABEL, RoutingExecutor, SandboxBackend, SandboxOptions};

    /// A `gcloud` provider that records each create request and creates
    /// nothing.
    struct Recording {
        kind:         ProviderKind,
        capabilities: Capabilities,
        created:      Arc<Mutex<Vec<SandboxSpec>>>,
    }

    #[async_trait]
    impl SandboxProvider for Recording {
        fn kind(&self) -> &ProviderKind {
            &self.kind
        }

        fn capabilities(&self) -> &Capabilities {
            &self.capabilities
        }

        async fn create(
            &self,
            spec: &SandboxSpec,
            _: Option<EventContext>,
        ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
            self.created.lock().unwrap().push(spec.clone());
            Err(DriverError::unsupported(Capability::LifecycleUndelete))
        }

        async fn attach(
            &self,
            _: &SandboxId,
            _: Option<EventContext>,
        ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
            Err(DriverError::unsupported(Capability::LifecycleUndelete))
        }

        async fn list(&self, _: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
            Ok(Vec::new())
        }

        async fn health(&self) -> sandbox_driver::Result<ProviderHealth> {
            Ok(ProviderHealth::new(HealthStatus::Ok))
        }
    }

    #[derive(Default)]
    struct RecordingFactory {
        created: Arc<Mutex<Vec<SandboxSpec>>>,
    }

    #[async_trait]
    impl ProviderFactory for RecordingFactory {
        fn kind(&self) -> &'static str {
            "gcloud"
        }

        fn fingerprint_seed(&self, _: &ProviderContext) -> String {
            "gcloud:test".to_owned()
        }

        fn network(&self) -> ProviderNetwork {
            ProviderNetwork::none()
        }

        async fn connect(
            &self,
            _: &ProviderContext,
        ) -> sandbox_driver::Result<Arc<dyn SandboxProvider>> {
            Ok(Arc::new(Recording {
                kind:         gcloud(),
                capabilities: Capabilities::minimal(Isolation::Vm),
                created:      self.created.clone(),
            }))
        }
    }

    fn gcloud() -> ProviderKind {
        ProviderKind::try_new("gcloud").unwrap()
    }

    fn scope(runtime: RuntimeSpec) -> ScopeSpec {
        ScopeSpec::new(ScopeId::new(1), "env-1").with_runtime(runtime)
    }

    fn labels() -> Vec<(String, String)> {
        vec![("petri.run".to_owned(), "run-1".to_owned())]
    }

    #[test]
    fn any_provider_kind_parses_as_a_named_backend() {
        let backend: SandboxBackend = "gcloud".parse().unwrap();
        assert_eq!(backend, SandboxBackend::Named(gcloud()));
        assert_eq!(backend.to_string(), "gcloud");
        for builtin in ["host", "docker", "daytona"] {
            let backend: SandboxBackend = builtin.parse().unwrap();
            assert!(!matches!(backend, SandboxBackend::Named(_)), "{builtin}");
            assert_eq!(backend.to_string(), builtin);
        }
        let local = "local".parse::<SandboxBackend>().unwrap_err();
        assert!(local.contains("host"), "{local}");
        assert!("simulated".parse::<SandboxBackend>().is_err());
        assert!("GCloud".parse::<SandboxBackend>().is_err());
        assert!("".parse::<SandboxBackend>().is_err());
    }

    #[test]
    fn a_named_backend_routes_every_scope_to_its_kind() {
        let router = RoutingExecutor::with_options(
            env::temp_dir().join("petri-named-routing"),
            Retention::Never,
            SandboxOptions {
                backend: SandboxBackend::Named(gcloud()),
                ..Default::default()
            },
        );
        assert_eq!(
            router.provider_kind_for(&RuntimeSpec::host_process()),
            "gcloud"
        );
        assert_eq!(
            router.provider_kind_for(&RuntimeSpec::container("ubuntu:24.04")),
            "gcloud"
        );
    }

    #[test]
    fn the_spec_carries_provider_config_and_labels_and_no_resources() {
        let config = json!({ "machine_type": "e2-standard-8", "zone": "europe-north1-a" });
        let spec = build_plugin_spec(
            &scope(RuntimeSpec::host_process()),
            &labels(),
            "petri-run-1-1",
            &gcloud(),
            &config,
        )
        .unwrap();
        assert!(matches!(spec.source, SandboxSource::HostDirectory));
        assert_eq!(spec.name.as_deref(), Some("petri-run-1-1"));
        assert_eq!(spec.sandbox_kind, Some(SandboxKind::VirtualMachine));
        assert_eq!(spec.provider_config, config);
        assert_eq!(
            spec.labels.get("petri.run").map(String::as_str),
            Some("run-1")
        );
        assert_eq!(spec.resources, Resources::default());
        assert_eq!(spec.public, Some(false));
    }

    #[test]
    fn container_targets_and_services_are_refused() {
        let container = scope(RuntimeSpec::container("ubuntu:24.04"));
        let error = build_plugin_spec(&container, &labels(), "n", &gcloud(), &json!({}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("container"), "{error}");

        let with_services = scope(RuntimeSpec::host_process())
            .with_services(vec![ServiceSpec::new("db", "postgres:16")]);
        let error = build_plugin_spec(&with_services, &labels(), "n", &gcloud(), &json!({}))
            .unwrap_err()
            .to_string();
        assert!(error.contains("services"), "{error}");
    }

    #[tokio::test]
    async fn a_router_acquires_a_named_scope_through_its_linked_factory() {
        let dir = RunDir::new("named-router");
        let factory = Arc::new(RecordingFactory::default());
        let config = json!({ "zone": "europe-north1-a" });
        let router = RoutingExecutor::with_options(dir.path(), Retention::Never, SandboxOptions {
            backend: SandboxBackend::Named(gcloud()),
            provider_config: config.clone(),
            ..Default::default()
        })
        .with_run_id("run-1")
        .with_in_process(InProcessProviders::new().with(factory.clone()));

        let container = scope(RuntimeSpec::container("ubuntu:24.04"));
        let error = router
            .acquire(&container, &AcquireContext::bare())
            .await
            .expect_err("a container target is refused");
        assert!(error.to_string().contains("container"), "{error}");
        assert!(
            factory.created.lock().unwrap().is_empty(),
            "refused before the provider creates anything"
        );

        router
            .acquire(&scope(RuntimeSpec::host_process()), &AcquireContext::bare())
            .await
            .expect_err("the recording provider creates nothing");
        let created = factory.created.lock().unwrap().clone();
        let [spec] = created.as_slice() else {
            panic!("one create request: {created:#?}");
        };
        assert!(matches!(spec.source, SandboxSource::HostDirectory));
        assert_eq!(spec.sandbox_kind, Some(SandboxKind::VirtualMachine));
        assert_eq!(spec.provider_config, config);
        assert_eq!(
            spec.labels.get(RUN_LABEL).map(String::as_str),
            Some("run-1")
        );
        assert_eq!(spec.resources, Resources::default());
        assert!(spec.env.is_empty(), "{:?}", spec.env);
        router.shutdown().await;
    }
}
