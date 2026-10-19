//! Sandboxes by lease: one live handle per durable lease, shared by every
//! holder, fenced only during crash recovery.
//!
//! A container sandbox lives as long as the lease that names its workspace.
//! The coordinator allocates the lease and hands it to each execution that
//! runs in the sandbox; this manager keeps the one live handle and a holder
//! count. A second acquire on a live lease — a restarted execution, an
//! inherited nested invocation, concurrent scopes over one workspace —
//! reuses the handle and never stops the sandbox. Per-execution release only
//! drops a holder. The sandbox is stopped when its invocation releases the
//! lease and kept or deleted by retention; `petri sandbox prune` deletes
//! kept ones later.
//!
//! # Recovery
//!
//! When no live holder exists — a fresh process over a run dir, or a plugin
//! generation change that invalidated every old handle — acquire
//! reconciles: it lists the provider by the workspace label, attaches the
//! one recorded match, stops it once (ending whatever a dead execution or a
//! dead plugin left running), and starts it once before any holder resumes.
//! More than one match is an error, never an arbitrary choice; a confirmed
//! record whose resource is missing is an error, never a silent replacement,
//! because its workspace was lost. A create happens only when reconciliation
//! finds nothing.
//!
//! # The ledger
//!
//! Every transition is written to a [`LeaseLedger`] before the provider call
//! that makes it true (`allocating` before create, `pending: stop` before
//! stop, `pending: delete` before delete) and confirmed only after the
//! provider succeeds. Recovery repeats a pending idempotent operation. The
//! coordinator's resource store is the durable ledger; a standalone driver
//! uses an in-memory one and releases its sandbox at scope release.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use executor::{EnvError, ReleaseReport, Retention, SandboxLeaseId, ScopeOutcome};
use sandbox_driver::{
    Error as DriverError, Sandbox, SandboxFilter, SandboxId, SandboxProvider, SandboxSpec,
};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio::task::JoinSet;

use crate::plugin::ProviderSource;
use crate::run::{LEASE_LABEL, RUN_LABEL, RunIdentity, WORKSPACE_LABEL};
use crate::services::RunServices;
use crate::{BACKEND, LostSandbox, acquire_failed};

/// Where a lease's sandbox stands, durably.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    /// Reserved; the provider may or may not have created the resource.
    #[default]
    Allocating,
    /// The resource exists and was last known running.
    Live,
    /// The resource exists, stopped; its workspace is kept.
    Stopped,
    /// The resource was deleted. The record stays as a tombstone.
    Deleted,
}

/// An operation written down before it is asked of the provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingIntent {
    Stop,
    Delete,
}

/// The durable facts about one lease the manager reads and writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub state:       LeaseState,
    pub pending:     Option<PendingIntent>,
    /// The real provider kind, once known.
    pub provider:    Option<SmolStr>,
    /// The provider's resource id, once known.
    pub resource_id: Option<SmolStr>,
    /// The non-secret provider fingerprint recorded at allocation.
    pub fingerprint: Option<SmolStr>,
}

/// Why a ledger operation failed.
#[derive(Debug, thiserror::Error)]
#[error("sandbox lease ledger: {0}")]
pub struct LedgerError(pub String);

/// The durable record of every lease, as the manager needs it. Every write
/// resolves only once its record is durable: the manager awaits `allocating`
/// before `provider.create`, and `pending` before a stop or a delete, so no
/// provider mutation starts before its intent is stored. A ledger that
/// queues the write and returns breaks crash recovery.
#[async_trait::async_trait]
pub trait LeaseLedger: Send + Sync {
    async fn lookup(&self, lease: SandboxLeaseId) -> Result<Option<LeaseRecord>, LedgerError>;

    /// Records that the provider is about to be asked to create the
    /// resource, and which provider on which backend.
    async fn allocating(
        &self,
        lease: SandboxLeaseId,
        provider: &str,
        fingerprint: &str,
    ) -> Result<(), LedgerError>;

    /// Records the resource as created (or found) and running.
    async fn live(&self, lease: SandboxLeaseId, resource_id: &str) -> Result<(), LedgerError>;

    /// Records an intent before the provider call. The intent stays until
    /// the matching confirmation.
    async fn pending(
        &self,
        lease: SandboxLeaseId,
        intent: PendingIntent,
    ) -> Result<(), LedgerError>;

    async fn stopped(&self, lease: SandboxLeaseId) -> Result<(), LedgerError>;

    async fn deleted(&self, lease: SandboxLeaseId) -> Result<(), LedgerError>;
}

/// A ledger that forgets everything when the process ends: for a driver
/// with no coordinator, whose sandboxes end with their scopes.
#[derive(Default)]
pub struct MemoryLedger {
    records: Mutex<HashMap<SandboxLeaseId, LeaseRecord>>,
}

impl MemoryLedger {
    fn update(&self, lease: SandboxLeaseId, update: impl FnOnce(&mut LeaseRecord)) {
        let mut records = self.records.lock().unwrap_or_else(PoisonError::into_inner);
        let record = records.entry(lease).or_insert_with(|| LeaseRecord {
            state:       LeaseState::Allocating,
            pending:     None,
            provider:    None,
            resource_id: None,
            fingerprint: None,
        });
        update(record);
    }
}

#[async_trait::async_trait]
impl LeaseLedger for MemoryLedger {
    async fn lookup(&self, lease: SandboxLeaseId) -> Result<Option<LeaseRecord>, LedgerError> {
        Ok(self
            .records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&lease)
            .cloned())
    }

    async fn allocating(
        &self,
        lease: SandboxLeaseId,
        provider: &str,
        fingerprint: &str,
    ) -> Result<(), LedgerError> {
        self.update(lease, |record| {
            record.state = LeaseState::Allocating;
            record.provider = Some(SmolStr::new(provider));
            record.fingerprint = Some(SmolStr::new(fingerprint));
        });
        Ok(())
    }

    async fn live(&self, lease: SandboxLeaseId, resource_id: &str) -> Result<(), LedgerError> {
        self.update(lease, |record| {
            record.state = LeaseState::Live;
            record.pending = None;
            record.resource_id = Some(SmolStr::new(resource_id));
        });
        Ok(())
    }

    async fn pending(
        &self,
        lease: SandboxLeaseId,
        intent: PendingIntent,
    ) -> Result<(), LedgerError> {
        self.update(lease, |record| record.pending = Some(intent));
        Ok(())
    }

    async fn stopped(&self, lease: SandboxLeaseId) -> Result<(), LedgerError> {
        self.update(lease, |record| {
            record.state = LeaseState::Stopped;
            record.pending = None;
        });
        Ok(())
    }

    async fn deleted(&self, lease: SandboxLeaseId) -> Result<(), LedgerError> {
        self.update(lease, |record| {
            record.state = LeaseState::Deleted;
            record.pending = None;
        });
        Ok(())
    }
}

/// One lease as the run's records name it, for reconciliation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedLease {
    pub lease:        SandboxLeaseId,
    /// The workspace the lease names: the reconcile label.
    pub workspace_id: String,
}

/// What a reconciliation did to the run's resources on one provider.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Leases whose resource a lost create had produced: attached, fenced
    /// and recorded live.
    pub adopted:  Vec<(SandboxLeaseId, SandboxId)>,
    /// Resources under the run's labels that no record names: removed.
    pub removed:  Vec<SandboxId>,
    /// What could not be reconciled, with why. A problem leaves the record
    /// as it was; the next acquire or prune tries again.
    pub problems: Vec<String>,
}

impl ReconcileReport {
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }

    pub fn merge(&mut self, other: Self) {
        self.adopted.extend(other.adopted);
        self.removed.extend(other.removed);
        self.problems.extend(other.problems);
    }
}

/// The live handle a lease currently has.
pub(crate) struct LiveSandbox {
    pub(crate) sandbox:    Arc<dyn Sandbox>,
    pub(crate) generation: u64,
}

/// Per-lease state, serialized by one lock so allocation and recovery for
/// one lease never race.
#[derive(Default)]
struct LeaseSlot {
    live:    Option<LiveSandbox>,
    holders: usize,
}

/// What an acquire needs to know beyond the lease.
pub(crate) struct LeaseRequest<'a> {
    pub lease:        SandboxLeaseId,
    /// The workspace the lease names, for the reconcile label.
    pub workspace_id: &'a str,
    /// A standalone acquisition has no coordinator to release its lease.
    pub standalone:   bool,
}

/// One live handle and a holder count per lease, over one provider source.
pub struct SandboxLeaseManager {
    source:       Arc<dyn ProviderSource>,
    ledger:       Arc<dyn LeaseLedger>,
    identity:     Arc<RunIdentity>,
    lost_sandbox: LostSandbox,
    services:     Option<Arc<RunServices>>,
    leases:       Mutex<HashMap<SandboxLeaseId, Arc<AsyncMutex<LeaseSlot>>>>,
    cleanup:      Mutex<JoinSet<()>>,
}

impl SandboxLeaseManager {
    pub fn new(
        source: Arc<dyn ProviderSource>,
        ledger: Arc<dyn LeaseLedger>,
        identity: Arc<RunIdentity>,
    ) -> Self {
        Self {
            source,
            ledger,
            identity,
            lost_sandbox: LostSandbox::Refuse,
            services: None,
            leases: Mutex::new(HashMap::new()),
            cleanup: Mutex::new(JoinSet::new()),
        }
    }

    /// What acquire does when a recorded sandbox is gone from the provider;
    /// [`LostSandbox::Refuse`] until set.
    #[must_use]
    pub fn with_lost_sandbox(mut self, policy: LostSandbox) -> Self {
        self.lost_sandbox = policy;
        self
    }

    /// The run's services, held in every sandbox this manager's leases
    /// hold and stopped before the lease's sandbox is; `None` runs none.
    #[must_use]
    pub fn with_services(mut self, services: Option<Arc<RunServices>>) -> Self {
        self.services = services;
        self
    }

    pub fn source(&self) -> &Arc<dyn ProviderSource> {
        &self.source
    }

    pub(crate) fn services(&self) -> Option<&Arc<RunServices>> {
        self.services.as_ref()
    }

    fn slot(&self, lease: SandboxLeaseId) -> Arc<AsyncMutex<LeaseSlot>> {
        Arc::clone(
            self.leases
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(lease)
                .or_default(),
        )
    }

    /// A lease record the ledger could not write: the run's store failed,
    /// not the provider.
    fn ledger_failed(error: &LedgerError) -> EnvError {
        EnvError::Store {
            message: error.to_string(),
        }
    }

    /// The sandbox for `request.lease`, allocating, attaching, or reusing
    /// as the lease's state requires, and counting the caller as a holder.
    /// `build_spec` produces the create spec, given the labels every Petri
    /// sandbox carries.
    pub(crate) async fn acquire<Fut>(
        self: &Arc<Self>,
        request: LeaseRequest<'_>,
        build_spec: impl FnOnce(Vec<(String, String)>, Arc<dyn sandbox_driver::SandboxProvider>) -> Fut
        + Send,
    ) -> Result<AcquiredSandbox, EnvError>
    where
        Fut: Future<Output = Result<SandboxSpec, EnvError>> + Send,
    {
        // An owned guard: a create hands it to its task, so an acquire that
        // is dropped mid-create still keeps the lease locked until the
        // create has settled one way or the other.
        let mut slot = self.slot(request.lease).lock_owned().await;
        let (provider, generation) = self.source.current().await?;
        if let Some(live) = &slot.live {
            if live.generation == generation {
                let sandbox = Arc::clone(&live.sandbox);
                slot.holders += 1;
                return Ok(self.acquired(&request, sandbox));
            }
            // A new plugin generation: every old handle is dead, and the
            // sandbox is fenced below before anyone resumes on it.
            tracing::warn!(
                lease = request.lease.raw(),
                old_generation = live.generation,
                generation,
                "sandbox plugin generation changed; fencing the lease's sandbox"
            );
            slot.live = None;
        }

        let record = self
            .ledger
            .lookup(request.lease)
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        let run_id = self.identity.run_id();
        let workspace_label = self.identity.workspace_label(request.workspace_id);
        let labels = self.identity.labels(request.lease, request.workspace_id);

        let sandbox = match record {
            Some(LeaseRecord {
                state: LeaseState::Deleted,
                ..
            }) => {
                return Err(EnvError::backend(
                    BACKEND,
                    "acquire",
                    format!(
                        "sandbox lease {} was deleted; its workspace is gone",
                        request.lease
                    ),
                ));
            }
            Some(record) => {
                self.check_fingerprint(request.lease, &record)?;
                let matches = list_by_label(&*provider, &workspace_label).await?;
                match matches.len() {
                    0 if record.pending == Some(PendingIntent::Delete) => {
                        // Delete completed before its confirmation was durable.
                        // The missing resource is the requested result, so
                        // finish the tombstone rather than report a lost lease.
                        self.ledger
                            .deleted(request.lease)
                            .await
                            .map_err(|error| Self::ledger_failed(&error))?;
                        return Err(EnvError::backend(
                            BACKEND,
                            "acquire",
                            format!(
                                "sandbox lease {} was deleted; its workspace is gone",
                                request.lease
                            ),
                        ));
                    }
                    0 if record.state == LeaseState::Allocating => {
                        // The record was reserved but no resource exists:
                        // the create never happened or never completed.
                        return self
                            .create(&provider, &request, &labels, build_spec, slot, generation)
                            .await;
                    }
                    0 if self.lost_sandbox == LostSandbox::Replace => {
                        // The workspace went with the sandbox; the host
                        // asked for a fresh one and restores it itself.
                        tracing::warn!(
                            lease = request.lease.raw(),
                            state = ?record.state,
                            "the lease's recorded sandbox is gone from the provider; creating a \
                             fresh one with an empty workspace"
                        );
                        if self.services.is_some() {
                            // Not a decision made here: nothing reruns the
                            // run's prepare steps in the new sandbox.
                            tracing::warn!(
                                lease = request.lease.raw(),
                                "the run's services will start in the fresh sandbox without the \
                                 run's prepare steps having run there; one that needs them can \
                                 crash until an agent prepares the workspace and runs \
                                 `fabro-service start <name>`"
                            );
                        }
                        return self
                            .create(&provider, &request, &labels, build_spec, slot, generation)
                            .await;
                    }
                    0 => {
                        return Err(EnvError::backend(
                            BACKEND,
                            "acquire",
                            format!(
                                "sandbox lease {} is recorded {:?} but no sandbox carries \
                                 {WORKSPACE_LABEL}={workspace_label} on the provider; its \
                                 workspace was lost and Petri will not replace it silently",
                                request.lease, record.state
                            ),
                        ));
                    }
                    1 => {
                        let found = &matches[0];
                        if let Some(recorded) = &record.resource_id
                            && recorded.as_str() != found.as_str()
                        {
                            return Err(EnvError::backend(
                                BACKEND,
                                "acquire",
                                format!(
                                    "sandbox lease {} records resource {recorded} but the \
                                     provider holds {found} for its workspace",
                                    request.lease
                                ),
                            ));
                        }
                        self.recover(&*provider, request.lease, found, record.pending)
                            .await?
                    }
                    count => {
                        return Err(EnvError::backend(
                            BACKEND,
                            "acquire",
                            format!(
                                "{count} sandboxes carry {WORKSPACE_LABEL}={workspace_label}; \
                                 Petri does not choose one arbitrarily"
                            ),
                        ));
                    }
                }
            }
            None => {
                // Nothing recorded: reconcile by label anyway, so a create
                // that a crash left unrecorded is found, not duplicated.
                let matches = list_by_label(&*provider, &workspace_label).await?;
                match matches.len() {
                    0 => {
                        return self
                            .create(&provider, &request, &labels, build_spec, slot, generation)
                            .await;
                    }
                    1 => {
                        self.ledger
                            .allocating(
                                request.lease,
                                self.source.kind(),
                                self.source.fingerprint(),
                            )
                            .await
                            .map_err(|error| Self::ledger_failed(&error))?;
                        self.recover(&*provider, request.lease, &matches[0], None)
                            .await?
                    }
                    count => {
                        return Err(EnvError::backend(
                            BACKEND,
                            "acquire",
                            format!(
                                "{count} sandboxes carry {WORKSPACE_LABEL}={workspace_label} \
                                 (run {run_id}); Petri does not choose one arbitrarily"
                            ),
                        ));
                    }
                }
            }
        };
        slot.live = Some(LiveSandbox {
            sandbox: Arc::clone(&sandbox),
            generation,
        });
        // Older generations can still have holders finishing or abandoning
        // initialization. Their later release must not consume this holder.
        slot.holders += 1;
        Ok(self.acquired(&request, sandbox))
    }

    fn acquired(
        self: &Arc<Self>,
        request: &LeaseRequest<'_>,
        sandbox: Arc<dyn Sandbox>,
    ) -> AcquiredSandbox {
        AcquiredSandbox {
            manager:    self.clone(),
            lease:      request.lease,
            standalone: request.standalone,
            sandbox:    Some(sandbox),
        }
    }

    fn check_fingerprint(
        &self,
        lease: SandboxLeaseId,
        record: &LeaseRecord,
    ) -> Result<(), EnvError> {
        if let Some(recorded) = &record.fingerprint {
            let current = self.source.fingerprint();
            if recorded.as_str() != current {
                return Err(EnvError::backend(
                    BACKEND,
                    "acquire",
                    format!(
                        "sandbox lease {lease} was allocated on `{recorded}` but this run is \
                         configured for `{current}`; a changed DOCKER_HOST, Daytona \
                         organization or target must be restored before the run continues"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Creates the sandbox, recording `allocating` first and `live` after,
    /// so a crash on either side of the create is reconciled by label.
    ///
    /// The create runs in its own task, which owns the lease's lock: an
    /// acquire nobody waited out (a cancelled scope, a sweep aborting the
    /// run) drops this future, and the provider's create — already sent —
    /// still completes, with the lease locked until it has. The task
    /// returns an acquisition guard. Dropping either the task's unread
    /// result or later environment initialization releases that holder.
    async fn create<Fut>(
        self: &Arc<Self>,
        provider: &Arc<dyn sandbox_driver::SandboxProvider>,
        request: &LeaseRequest<'_>,
        labels: &[(String, String)],
        build_spec: impl FnOnce(Vec<(String, String)>, Arc<dyn sandbox_driver::SandboxProvider>) -> Fut
        + Send,
        mut slot: OwnedMutexGuard<LeaseSlot>,
        generation: u64,
    ) -> Result<AcquiredSandbox, EnvError>
    where
        Fut: Future<Output = Result<SandboxSpec, EnvError>> + Send,
    {
        let lease = request.lease;
        let standalone = request.standalone;
        self.ledger
            .allocating(lease, self.source.kind(), self.source.fingerprint())
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        let spec = build_spec(labels.to_vec(), provider.clone()).await?;
        let provider = provider.clone();
        let manager = self.clone();
        let created = tokio::spawn(async move {
            let sandbox = match provider.create(&spec, None).await {
                Ok(sandbox) => sandbox,
                Err(error) => {
                    if standalone {
                        let report = manager
                            .release_locked(lease, Retention::Never, ScopeOutcome::Failed, slot)
                            .await;
                        for problem in report.problems {
                            tracing::warn!(
                                lease = lease.raw(),
                                problem,
                                "failed sandbox create cleanup"
                            );
                        }
                    }
                    return Err(acquire_failed(&error));
                }
            };
            let acquired = AcquiredSandbox {
                manager: manager.clone(),
                lease,
                standalone,
                sandbox: Some(sandbox.clone()),
            };
            slot.live = Some(LiveSandbox {
                sandbox: sandbox.clone(),
                generation,
            });
            slot.holders += 1;
            manager
                .ledger
                .live(lease, sandbox.id().as_str())
                .await
                .map_err(|error| Self::ledger_failed(&error))?;
            drop(slot);
            Ok::<_, EnvError>(acquired)
        });
        created.await.map_err(|error| {
            EnvError::backend(
                BACKEND,
                "acquire",
                format!("the sandbox create task failed: {error}"),
            )
        })?
    }

    /// Attaches a recorded sandbox and fences it: one stop, then one start.
    async fn recover(
        &self,
        provider: &dyn sandbox_driver::SandboxProvider,
        lease: SandboxLeaseId,
        id: &SandboxId,
        pending: Option<PendingIntent>,
    ) -> Result<Arc<dyn Sandbox>, EnvError> {
        let sandbox = provider
            .attach(id, None)
            .await
            .map_err(|error| acquire_failed(&error))?;
        if pending == Some(PendingIntent::Delete) {
            // The invocation meant to delete it and died before the
            // provider confirmed; finish that, and the lease is gone.
            delete_sandbox(provider, id)
                .await
                .map_err(|error| acquire_failed(&error))?;
            self.ledger
                .deleted(lease)
                .await
                .map_err(|error| Self::ledger_failed(&error))?;
            return Err(EnvError::backend(
                BACKEND,
                "acquire",
                format!("sandbox lease {lease} was being deleted; its workspace is gone"),
            ));
        }
        tracing::info!(lease = lease.raw(), sandbox = %id, "fencing a recorded sandbox");
        self.ledger
            .pending(lease, PendingIntent::Stop)
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        sandbox
            .stop()
            .await
            .map_err(|error| acquire_failed(&error))?;
        sandbox
            .start()
            .await
            .map_err(|error| acquire_failed(&error))?;
        self.ledger
            .live(lease, id.as_str())
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        Ok(sandbox)
    }

    /// Drops one holder. Never stops the sandbox: that is the lease
    /// release's job.
    pub async fn release_holder(&self, lease: SandboxLeaseId) -> usize {
        let slot = self.slot(lease);
        let mut slot = slot.lock().await;
        slot.holders = slot.holders.saturating_sub(1);
        slot.holders
    }

    async fn abandon(&self, lease: SandboxLeaseId, standalone: bool) {
        let mut slot = self.slot(lease).lock_owned().await;
        slot.holders = slot.holders.saturating_sub(1);
        if standalone && slot.holders == 0 {
            let report = self
                .release_locked(lease, Retention::Never, ScopeOutcome::Failed, slot)
                .await;
            for problem in report.problems {
                tracing::warn!(
                    lease = lease.raw(),
                    problem,
                    "abandoned sandbox cleanup failed"
                );
            }
        }
    }

    fn abandon_in_background(self: &Arc<Self>, lease: SandboxLeaseId, standalone: bool) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        while let Some(result) = cleanup.try_join_next() {
            if let Err(error) = result {
                tracing::warn!(%error, "sandbox cleanup task failed");
            }
        }
        let manager = self.clone();
        cleanup.spawn(async move { manager.abandon(lease, standalone).await });
    }

    /// The live handle, if the lease has one in this process.
    pub async fn live(&self, lease: SandboxLeaseId) -> Option<Arc<dyn Sandbox>> {
        let slot = self.slot(lease);
        let slot = slot.lock().await;
        slot.live.as_ref().map(|live| Arc::clone(&live.sandbox))
    }

    /// Ends the lease: stops the sandbox, then keeps it stopped or deletes
    /// it as `retention` decides for `outcome`. Each step is written to the
    /// ledger as an intent first and confirmed after.
    pub async fn release_lease(
        &self,
        lease: SandboxLeaseId,
        retention: Retention,
        outcome: ScopeOutcome,
    ) -> ReleaseReport {
        let slot = self.slot(lease).lock_owned().await;
        self.release_locked(lease, retention, outcome, slot).await
    }

    async fn release_locked(
        &self,
        lease: SandboxLeaseId,
        retention: Retention,
        outcome: ScopeOutcome,
        mut slot: OwnedMutexGuard<LeaseSlot>,
    ) -> ReleaseReport {
        let report = ReleaseReport::default();
        let live = slot.live.take();
        slot.holders = 0;
        // The services go first: their held execs must not outlive the
        // sandbox they run in, nor meet it stopped.
        if let Some(services) = &self.services {
            services.detach(lease).await;
        }
        let record = match self.ledger.lookup(lease).await {
            Ok(record) => record,
            Err(error) => return report.problem(error.to_string()),
        };
        let Some(record) = record else {
            return report;
        };
        if record.state == LeaseState::Deleted {
            return report;
        }
        let resource_id = record.resource_id.clone().or_else(|| {
            live.as_ref()
                .map(|live| SmolStr::new(live.sandbox.id().as_str()))
        });
        if resource_id.is_none() && record.fingerprint.is_none() {
            // No allocation was attempted, so no provider cleanup is needed.
            return report;
        }
        let (provider, generation) = match self.source.current().await {
            Ok(current) => current,
            Err(error) => return report.problem(format!("sandbox provider unavailable: {error}")),
        };
        if let Err(error) = self.check_fingerprint(lease, &record) {
            return report.problem(error.to_string());
        }
        let id = match resource_id {
            Some(resource_id) => match SandboxId::try_new(resource_id.as_str()) {
                Ok(id) => id,
                Err(error) => return report.problem(error.to_string()),
            },
            None => match self.find_allocated(&*provider, lease).await {
                // The create reached the provider but never its record: name
                // the sandbox first, as reconciliation adopts one, so a kept
                // lease names what it keeps.
                Ok(Some(id)) => match self.ledger.live(lease, id.as_str()).await {
                    Ok(()) => id,
                    Err(error) => return report.problem(error.to_string()),
                },
                Ok(None) => {
                    return match self.ledger.deleted(lease).await {
                        Ok(()) => report,
                        Err(error) => report.problem(error.to_string()),
                    };
                }
                Err(error) => return report.problem(error.to_string()),
            },
        };
        let keep = record.pending != Some(PendingIntent::Delete) && retention.keeps(outcome);
        let intent = if keep {
            PendingIntent::Stop
        } else {
            PendingIntent::Delete
        };
        if let Err(error) = self.ledger.pending(lease, intent).await {
            return report.problem(error.to_string());
        }
        if keep {
            let outcome = match live {
                Some(live) if live.generation == generation => live.sandbox.stop().await,
                _ => match provider.attach(&id, None).await {
                    Ok(sandbox) => sandbox.stop().await,
                    Err(error) => Err(error),
                },
            };
            match outcome {
                Ok(()) => {
                    if let Err(error) = self.ledger.stopped(lease).await {
                        return report.problem(error.to_string());
                    }
                    report.kept(format!("sandbox {id} (stopped, lease {lease})"))
                }
                // Someone outside the run deleted the sandbox: there is
                // nothing left to keep. The lease ends as a delete that
                // finds nothing ends, and the release says what was lost.
                Err(DriverError::NotFound { .. }) => match self.ledger.deleted(lease).await {
                    Ok(()) => report.problem(format!(
                        "sandbox {id} was gone before it could be kept; lease {lease} is deleted"
                    )),
                    Err(error) => report.problem(error.to_string()),
                },
                Err(error) => report.problem(format!("sandbox {id} stop failed: {error}")),
            }
        } else {
            match delete_sandbox(&*provider, &id).await {
                Ok(()) => {
                    if let Err(error) = self.ledger.deleted(lease).await {
                        return report.problem(error.to_string());
                    }
                    report.released(format!("sandbox {id} (lease {lease})"))
                }
                Err(error) => report.problem(format!("sandbox {id} delete failed: {error}")),
            }
        }
    }

    /// A create may have reached the provider without returning an id.
    async fn find_allocated(
        &self,
        provider: &dyn SandboxProvider,
        lease: SandboxLeaseId,
    ) -> Result<Option<SandboxId>, EnvError> {
        let mut filter = SandboxFilter::default();
        filter
            .labels
            .insert(RUN_LABEL.to_owned(), self.identity.run_id().to_owned());
        filter
            .labels
            .insert(LEASE_LABEL.to_owned(), lease.raw().to_string());
        let mut matches = provider
            .list(&filter)
            .await
            .map_err(|error| acquire_failed(&error))?;
        match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop().map(|status| status.id)),
            count => Err(EnvError::backend(
                BACKEND,
                "release",
                format!(
                    "{count} sandboxes belong to lease {lease}; Petri does not choose one arbitrarily"
                ),
            )),
        }
    }

    /// Deletes a recorded sandbox without a live handle: the prune path.
    /// The fingerprint must match, the intent is written first, and the
    /// record becomes a tombstone only after the provider confirms. A
    /// record that never learned its resource id — a crash between create
    /// and the `live` write — is reconciled by the workspace label of
    /// `workspace_id`, the same key recovery uses. The ids deleted come
    /// back; an empty list means nothing was on the provider.
    pub async fn delete_recorded(
        &self,
        lease: SandboxLeaseId,
        workspace_id: &str,
    ) -> Result<Vec<SandboxId>, EnvError> {
        let slot = self.slot(lease);
        let mut slot = slot.lock().await;
        slot.live = None;
        slot.holders = 0;
        if let Some(services) = &self.services {
            services.detach(lease).await;
        }
        let Some(record) = self
            .ledger
            .lookup(lease)
            .await
            .map_err(|error| Self::ledger_failed(&error))?
        else {
            return Ok(Vec::new());
        };
        if record.state == LeaseState::Deleted {
            return Ok(Vec::new());
        }
        let (provider, _) = self.source.current().await?;
        self.check_fingerprint(lease, &record)?;
        let ids = if let Some(resource_id) = record.resource_id {
            vec![
                SandboxId::try_new(resource_id.as_str())
                    .map_err(|error| EnvError::backend(BACKEND, "prune", error.to_string()))?,
            ]
        } else {
            let label = self.identity.workspace_label(workspace_id);
            list_by_label(&*provider, &label).await?
        };
        self.ledger
            .pending(lease, PendingIntent::Delete)
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        for id in &ids {
            delete_sandbox(&*provider, id)
                .await
                .map_err(|error| EnvError::backend(BACKEND, "prune", error.to_string()))?;
        }
        self.ledger
            .deleted(lease)
            .await
            .map_err(|error| Self::ledger_failed(&error))?;
        Ok(ids)
    }

    /// Every recorded lease's fingerprint against the provider's, before
    /// reconciliation changes anything. A lease with no fingerprint never
    /// sent a create, so it has nothing on any provider and cannot
    /// disagree. The problems are every mismatch found, and any record that
    /// could not be read: an unread record cannot vouch for the namespace
    /// either.
    async fn verify_namespace(&self, leases: &[RecordedLease]) -> Result<(), Vec<String>> {
        let mut problems = Vec::new();
        for recorded in leases {
            match self.ledger.lookup(recorded.lease).await {
                Ok(Some(record)) => {
                    if let Err(error) = self.check_fingerprint(recorded.lease, &record) {
                        problems.push(error.to_string());
                    }
                }
                Ok(None) => {}
                Err(error) => problems.push(error.to_string()),
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(problems)
        }
    }

    /// Reconcile the run's leases with the provider before any create: the
    /// takeover step of a `Write` open. First every recorded lease's
    /// fingerprint is checked against the provider's: the run's records
    /// name the namespace its resources live in, and a provider that
    /// reports another one is the wrong place to adopt or remove anything,
    /// so a mismatch is reported and nothing below runs. Then, for each
    /// lease recorded `allocating` with a create attempted (a fingerprint),
    /// a resource under its workspace label is the lost create's: it is
    /// adopted (attached, fenced with one stop and one start, recorded
    /// live). Then every resource under the run's label that no record
    /// names by resource id is garbage — a stale owner's late create, whose
    /// `live` write was refused — and is removed. Invariant afterwards: a
    /// resource under the run's labels is named by a live or stopped record.
    pub async fn reconcile(&self, leases: &[RecordedLease]) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let (provider, generation) = match self.source.current().await {
            Ok(current) => current,
            Err(error) => {
                return report.with_problem(format!("sandbox provider unavailable: {error}"));
            }
        };
        if let Err(problems) = self.verify_namespace(leases).await {
            report.problems = problems;
            return report;
        }
        for recorded in leases {
            let slot = self.slot(recorded.lease);
            let mut slot = slot.lock().await;
            let record = match self.ledger.lookup(recorded.lease).await {
                Ok(Some(record)) => record,
                Ok(None) => continue,
                Err(error) => {
                    report.problems.push(error.to_string());
                    continue;
                }
            };
            if record.state != LeaseState::Allocating
                || record.fingerprint.is_none()
                || record.pending.is_some()
                || slot.live.is_some()
            {
                continue;
            }
            let label = self.identity.workspace_label(&recorded.workspace_id);
            let matches = match list_by_label(&*provider, &label).await {
                Ok(matches) => matches,
                Err(error) => {
                    report.problems.push(error.to_string());
                    continue;
                }
            };
            match matches.as_slice() {
                [] => {}
                [found] => match self.recover(&*provider, recorded.lease, found, None).await {
                    Ok(sandbox) => {
                        slot.live = Some(LiveSandbox {
                            sandbox,
                            generation,
                        });
                        report.adopted.push((recorded.lease, found.clone()));
                    }
                    Err(error) => report.problems.push(error.to_string()),
                },
                found => report.problems.push(format!(
                    "{} sandboxes carry {WORKSPACE_LABEL}={label}; Petri does not choose one \
                     arbitrarily",
                    found.len()
                )),
            }
        }
        report.merge(self.sweep_unrecorded(&*provider).await);
        report
    }

    /// Remove every resource under the run's label that no record names by
    /// resource id. Action hosts carry no lease label and are left to their
    /// own sweep.
    async fn sweep_unrecorded(&self, provider: &dyn SandboxProvider) -> ReconcileReport {
        let mut report = ReconcileReport::default();
        let mut filter = SandboxFilter::default();
        filter
            .labels
            .insert(RUN_LABEL.to_owned(), self.identity.run_id().to_owned());
        let statuses = match provider.list(&filter).await {
            Ok(statuses) => statuses,
            Err(error) => return report.with_problem(acquire_failed(&error).to_string()),
        };
        for status in statuses {
            let Some(lease) = status
                .labels
                .get(LEASE_LABEL)
                .and_then(|raw| raw.parse::<u64>().ok())
                .map(SandboxLeaseId::new)
            else {
                continue;
            };
            let named = match self.ledger.lookup(lease).await {
                Ok(record) => record.is_some_and(|record| {
                    record.state != LeaseState::Deleted
                        && record.resource_id.as_deref() == Some(status.id.as_str())
                }),
                Err(error) => {
                    report.problems.push(error.to_string());
                    continue;
                }
            };
            if named {
                continue;
            }
            tracing::warn!(
                lease = lease.raw(),
                sandbox = %status.id,
                "removing a sandbox no lease record names"
            );
            match delete_sandbox(provider, &status.id).await {
                Ok(()) => report.removed.push(status.id),
                Err(error) => report
                    .problems
                    .push(format!("sandbox {} delete failed: {error}", status.id)),
            }
        }
        report
    }
}

impl ReconcileReport {
    fn with_problem(mut self, problem: String) -> Self {
        self.problems.push(problem);
        self
    }
}

/// Deletion is complete when the resource is already absent, including after
/// an interrupted delete whose confirmation never reached the ledger.
pub(crate) async fn delete_sandbox(
    provider: &dyn SandboxProvider,
    id: &SandboxId,
) -> sandbox_driver::Result<()> {
    match provider.delete(id, None).await {
        Ok(()) | Err(DriverError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

/// Owns a holder until environment initialization hands it to an EnvHandle.
/// An unread create result and an interrupted environment read both drop
/// this guard. The manager owns any asynchronous cleanup that drop starts.
pub(crate) struct AcquiredSandbox {
    manager:    Arc<SandboxLeaseManager>,
    lease:      SandboxLeaseId,
    standalone: bool,
    sandbox:    Option<Arc<dyn Sandbox>>,
}

impl AcquiredSandbox {
    pub(crate) fn sandbox(&self) -> &Arc<dyn Sandbox> {
        self.sandbox
            .as_ref()
            .expect("an acquisition owns its sandbox")
    }

    pub(crate) fn into_sandbox(mut self) -> Arc<dyn Sandbox> {
        self.sandbox
            .take()
            .expect("an acquisition owns its sandbox")
    }

    pub(crate) async fn release(mut self) {
        self.manager.abandon(self.lease, self.standalone).await;
        self.sandbox = None;
    }
}

impl Drop for AcquiredSandbox {
    fn drop(&mut self) {
        if self.sandbox.is_some() {
            self.manager
                .abandon_in_background(self.lease, self.standalone);
        }
    }
}

async fn list_by_label(
    provider: &dyn sandbox_driver::SandboxProvider,
    workspace_label: &str,
) -> Result<Vec<SandboxId>, EnvError> {
    let mut filter = SandboxFilter::default();
    filter
        .labels
        .insert(WORKSPACE_LABEL.to_owned(), workspace_label.to_owned());
    let matches = provider
        .list(&filter)
        .await
        .map_err(|error| acquire_failed(&error))?;
    Ok(matches.into_iter().map(|status| status.id).collect())
}

#[cfg(test)]
mod identity_tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use sandbox_driver::{Capabilities, EventContext, Isolation, ProviderKind, SandboxStatus};
    use testkit::RunDir;

    use super::*;

    struct UntouchedProvider {
        kind:         ProviderKind,
        capabilities: Capabilities,
    }

    #[async_trait::async_trait]
    impl SandboxProvider for UntouchedProvider {
        fn kind(&self) -> &ProviderKind {
            &self.kind
        }
        fn capabilities(&self) -> &Capabilities {
            &self.capabilities
        }
        async fn create(
            &self,
            _: &SandboxSpec,
            _: Option<EventContext>,
        ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
            panic!("a changed account must not create resources during recovery")
        }
        async fn attach(
            &self,
            _: &SandboxId,
            _: Option<EventContext>,
        ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
            panic!("a changed account must not attach or mutate recorded resources")
        }
        async fn list(&self, _: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
            panic!("a changed account must not search for recorded resources")
        }
    }

    struct VerifiedSource {
        provider: Arc<UntouchedProvider>,
        verified: AtomicBool,
    }

    #[async_trait::async_trait]
    impl ProviderSource for VerifiedSource {
        async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError> {
            self.verified.store(true, Ordering::SeqCst);
            Ok((self.provider.clone(), 1))
        }
        fn fingerprint(&self) -> &str {
            if self.verified.load(Ordering::SeqCst) {
                "daytona:::organization:other"
            } else {
                "daytona:::"
            }
        }
        #[expect(
            clippy::unnecessary_literal_bound,
            reason = "the trait fixes the signature"
        )]
        fn kind(&self) -> &str {
            "daytona"
        }
    }

    async fn manager(dir: &RunDir) -> (SandboxLeaseManager, Arc<MemoryLedger>) {
        let ledger = Arc::new(MemoryLedger::default());
        ledger
            .allocating(SandboxLeaseId::new(0), "daytona", "daytona:::")
            .await
            .expect("allocation");
        ledger
            .live(SandboxLeaseId::new(0), "existing-resource")
            .await
            .expect("live record");
        let source = Arc::new(VerifiedSource {
            provider: Arc::new(UntouchedProvider {
                kind:         ProviderKind::try_new("daytona").expect("kind"),
                capabilities: Capabilities::minimal(Isolation::Vm),
            }),
            verified: AtomicBool::new(false),
        });
        (
            SandboxLeaseManager::new(
                source,
                ledger.clone(),
                Arc::new(RunIdentity::new(dir.path().to_path_buf(), "identity-test")),
            ),
            ledger,
        )
    }

    #[tokio::test]
    async fn release_verifies_account_identity_before_stop_or_delete() {
        for retention in [Retention::Always, Retention::Never] {
            let dir = RunDir::new("release-provider-identity");
            let (manager, ledger) = manager(&dir).await;
            let report = manager
                .release_lease(SandboxLeaseId::new(0), retention, ScopeOutcome::Succeeded)
                .await;
            assert!(!report.is_clean());
            assert!(
                report
                    .problems
                    .iter()
                    .any(|problem| problem.contains("organization:other"))
            );
            assert_eq!(
                ledger
                    .lookup(SandboxLeaseId::new(0))
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                LeaseState::Live
            );
        }
    }

    #[tokio::test]
    async fn prune_verifies_account_identity_before_deleting() {
        let dir = RunDir::new("prune-provider-identity");
        let (manager, ledger) = manager(&dir).await;
        let error = manager
            .delete_recorded(SandboxLeaseId::new(0), "workspace")
            .await
            .expect_err("different account");
        assert!(error.to_string().contains("organization:other"));
        assert_eq!(
            ledger
                .lookup(SandboxLeaseId::new(0))
                .await
                .unwrap()
                .unwrap()
                .state,
            LeaseState::Live
        );
    }
}
