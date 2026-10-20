//! Run-level services: long-running commands (a dev server, say) that every
//! stage of a run reaches in its sandbox.
//!
//! A workload command cannot leave a process behind: a provider may run
//! each command in a PID namespace of its own and kill whatever is left in
//! it when the command exits. So each service is one workload exec held
//! open for as long as its sandbox is, with the environment and confinement
//! of any other command and no timeout. The exec runs a POSIX sh supervisor
//! (`services/supervisor.sh`) that runs the declared command, restarts it
//! with backoff, leaves it `crashed` after repeated quick crashes, and keeps
//! its status, a bounded log and a control file under
//! `/tmp/fabro-services/<name>/` (under the run directory on the host
//! provider). The same exec installs `fabro-service`
//! (`services/fabro-service.sh`) beside them, at the end of every workload
//! command's `PATH`, for agents to list, read, stop, start and restart
//! services: it
//! only writes the control file and reads the rest, because a command in
//! another PID namespace cannot signal the supervisor.
//!
//! The lease owns the lifecycle. Each acquisition that brings a new sandbox
//! handle (a create, a recovery fence, a fresh sandbox for a lost one)
//! attaches it here, and its services start only once the host has said
//! both that the run's prepare steps are done ([`RunServices::prepared`])
//! and that the workspace is in place, restored if it had to be
//! ([`RunServices::scope_ready`]). A run resumed by a new process in a
//! stage past the prepare steps learns the first from the sandbox it
//! reattached: one where a supervisor already left its state was past them
//! ([`RunServices::scope_resumed`]). A held exec that ends without being
//! asked to is spawned again; a new handle for the lease replaces the old
//! one's execs; the lease stops them before its sandbox is stopped or
//! deleted. Nothing here fails the run: the branch a run works on may well
//! break its services, which is for an agent to see and fix, so a service
//! that crashes or never gets ready is a warning in the run log. Only the
//! supervisor's state lines reach that log: the service's own output may
//! hold secrets, so it stays in the service's log in the sandbox.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use std::{fmt, ptr};

use executor::{OutputMode, ProcessHandle, SandboxLeaseId, Sig};
use sandbox_driver::{ExecSpec, Sandbox};
use smol_str::SmolStr;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;

use crate::env::{Job, spawn_streamed};
use crate::gate::RunGate;

/// Where services keep their state in a sandbox, when the workload commands
/// have a `/tmp` of their own.
pub const SERVICES_DIR: &str = "/tmp/fabro-services";
/// The variable that tells the supervisor and `fabro-service` where that is.
pub const SERVICES_DIR_VAR: &str = "FABRO_SERVICES_DIR";

pub(crate) const SUPERVISOR: &str = include_str!("services/supervisor.sh");
pub(crate) const CLI: &str = include_str!("services/fabro-service.sh");

/// How long a stopping supervisor gets to stop its service and exit before
/// its exec is killed.
const STOP_GRACE: Duration = Duration::from_secs(10);
/// The first and the longest wait before a held exec that ended is spawned
/// again.
const RESPAWN_MIN: Duration = Duration::from_secs(1);
const RESPAWN_MAX: Duration = Duration::from_secs(30);
/// How long a resumed run's check for services a reattached sandbox held
/// before gets.
const HELD_BEFORE_TIMEOUT: Duration = Duration::from_secs(30);
/// Exits zero when any service named in its arguments has a status file
/// under `$FABRO_SERVICES_DIR`: its supervisor ran there.
const HELD_BEFORE: &str =
    r#"for name; do [ -f "${FABRO_SERVICES_DIR:?}/$name/status" ] && exit 0; done; exit 1"#;

/// One declared service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunService {
    /// `[a-z0-9-]`: its directory and its name in `fabro-service`.
    pub name:    String,
    /// A foreground command, run with `sh -c` in the workspace.
    pub command: String,
    pub ready:   Option<ServiceReady>,
}

/// What says a started service is up, and how long the first stage after
/// the prepare steps waits for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceReady {
    pub probe:   ReadyProbe,
    pub timeout: Duration,
}

/// A readiness probe, run by the supervisor inside the sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadyProbe {
    /// A successful response from the URL.
    Http(String),
    /// An accepted connection to `host:port`.
    Tcp(String),
    /// A shell command that exits zero.
    Command(String),
}

impl ReadyProbe {
    fn args(&self) -> [&str; 2] {
        match self {
            Self::Http(url) => ["http", url],
            Self::Tcp(address) => ["tcp", address],
            Self::Command(command) => ["command", command],
        }
    }
}

/// A service's state as its supervisor last reported it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceState {
    Starting,
    Ready,
    Running,
    Crashed,
    Stopped,
}

impl ServiceState {
    /// The state a supervisor's `state=<state> ...` line reports.
    fn from_line(line: &str) -> Option<Self> {
        let state = line.strip_prefix("state=")?.split(' ').next()?;
        Some(match state {
            "starting" => Self::Starting,
            "ready" => Self::Ready,
            "running" => Self::Running,
            "crashed" => Self::Crashed,
            "stopped" => Self::Stopped,
            _ => return None,
        })
    }
}

/// The run's services across its sandboxes: one set of held execs per
/// lease, shared by the executors of the run and the host that says when
/// they may start.
pub struct RunServices {
    services: Vec<RunService>,
    /// The run's prepare steps are done: set once, never cleared.
    prepared: AtomicBool,
    leases:   Mutex<HashMap<SandboxLeaseId, Held>>,
}

impl fmt::Debug for RunServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunServices")
            .field("services", &self.services)
            .field("prepared", &self.prepared.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

/// One lease's sandbox handle and its services.
struct Held {
    workspace:   String,
    sandbox:     Arc<dyn Sandbox>,
    working_dir: String,
    /// A workload command's environment over the sandbox's own.
    env:         BTreeMap<SmolStr, SmolStr>,
    /// The host said this handle's workspace is in place.
    cleared:     bool,
    /// The run's admission to the sandbox: every held exec passes it.
    gate:        RunGate,
    /// The held execs, once started; dropping them aborts them.
    running:     Option<Running>,
}

struct Running {
    stop:  CancellationToken,
    tasks: JoinSet<()>,
    waits: Vec<Wait>,
}

/// A started service's readiness wait: every stage that arrives before its
/// deadline waits for it, not only the one that started it.
#[derive(Clone)]
struct Wait {
    name:     String,
    /// `None` when the service declares no readiness check.
    deadline: Option<Instant>,
    state:    watch::Receiver<Option<ServiceState>>,
}

impl RunServices {
    pub fn new(services: Vec<RunService>) -> Self {
        Self {
            services,
            prepared: AtomicBool::new(false),
            leases: Mutex::new(HashMap::new()),
        }
    }

    pub fn services(&self) -> &[RunService] {
        &self.services
    }

    /// The run's prepare steps are done. Starts the services in every
    /// sandbox whose workspace is in place and waits, at most each one's
    /// readiness timeout, for those it started. Cheap once they run: a host
    /// may say it before every stage.
    pub async fn prepared(&self) {
        self.prepared.store(true, Ordering::SeqCst);
        self.start(|_| true).await;
    }

    /// The workspace `workspace` names is in place in its sandbox: restored,
    /// when a resumed run had to. Starts its services if the prepare steps
    /// are done, and waits for them as [`Self::prepared`] does.
    pub async fn scope_ready(&self, workspace: &str) {
        for held in self
            .lock()
            .values_mut()
            .filter(|held| held.workspace == workspace)
        {
            held.cleared = true;
        }
        self.start(|held| held.workspace == workspace).await;
    }

    /// [`Self::scope_ready`] in a run resumed by a new process whose
    /// attempt past the prepare steps, the one the old process was running,
    /// is dispatched again without the admission that would say
    /// [`Self::prepared`]. Services start only after the prepare steps, so a
    /// sandbox where one of their supervisors left its state was past them:
    /// the run is taken as prepared, and the services start again before
    /// that attempt goes on. A sandbox that holds none (a fresh one that
    /// replaced a lost one, say) leaves its services to the next stage.
    pub async fn scope_resumed(&self, workspace: &str) {
        if !self.prepared.load(Ordering::SeqCst) && self.held_before(workspace).await {
            tracing::info!(
                workspace,
                "the reattached sandbox held the run's services before the run resumed; \
                 starting them again"
            );
            self.prepared.store(true, Ordering::SeqCst);
        }
        self.scope_ready(workspace).await;
    }

    /// Whether a supervisor of any of the run's services left its state in
    /// the sandbox `workspace` names. A check that cannot run says no.
    async fn held_before(&self, workspace: &str) -> bool {
        let checks: Vec<(Arc<dyn Sandbox>, ExecSpec)> = self
            .lock()
            .values()
            .filter(|held| held.workspace == workspace)
            .filter_map(|held| {
                let root = held.env.get(SERVICES_DIR_VAR)?;
                Some((
                    Arc::clone(&held.sandbox),
                    held_before_spec(root, &self.services),
                ))
            })
            .collect();
        for (sandbox, spec) in checks {
            match sandbox.exec().run(&spec).await {
                Ok(result) if result.success() => return true,
                Ok(_) => {}
                Err(error) => tracing::warn!(
                    sandbox = %sandbox.id(),
                    error = %error,
                    "the reattached sandbox could not be checked for the run's services; they \
                     start with the next stage"
                ),
            }
        }
        false
    }

    async fn start(&self, which: impl Fn(&Held) -> bool) {
        if !self.prepared.load(Ordering::SeqCst) {
            return;
        }
        let now = Instant::now();
        let mut waits = Vec::new();
        for (lease, held) in self.lock().iter_mut() {
            if !held.cleared || !which(held) {
                continue;
            }
            let started = held.running.is_none();
            if started {
                held.start(*lease, &self.services);
            }
            let pending = held.running.iter().flat_map(|running| &running.waits);
            waits.extend(
                pending
                    .filter(|wait| wait.deadline.is_some_and(|deadline| deadline > now))
                    .map(|wait| (wait.clone(), started)),
            );
        }
        wait_ready(waits).await;
    }

    /// The sandbox `lease` holds now, as an acquisition found it. A handle
    /// already attached is left alone; a new one replaces the old, whose
    /// execs ended with its plugin generation or its fence, and waits for
    /// [`Self::scope_ready`].
    pub(crate) fn attach(
        &self,
        lease: SandboxLeaseId,
        workspace: &str,
        sandbox: &Arc<dyn Sandbox>,
        working_dir: &str,
        env: &BTreeMap<SmolStr, SmolStr>,
        gate: &RunGate,
    ) {
        let mut leases = self.lock();
        if leases
            .get(&lease)
            .is_some_and(|held| same_handle(&held.sandbox, sandbox))
        {
            return;
        }
        leases.insert(lease, Held {
            workspace:   workspace.to_owned(),
            sandbox:     Arc::clone(sandbox),
            working_dir: working_dir.to_owned(),
            env:         env.clone(),
            cleared:     false,
            gate:        gate.clone(),
            running:     None,
        });
    }

    /// Stops the lease's services, before its sandbox is stopped or
    /// deleted: each supervisor gets a TERM, stops its service and exits,
    /// or its exec is killed.
    pub(crate) async fn detach(&self, lease: SandboxLeaseId) {
        let held = self.lock().remove(&lease);
        let Some(mut running) = held.and_then(|held| held.running) else {
            return;
        };
        running.stop.cancel();
        let stopped = time::timeout(STOP_GRACE * 2, async {
            while running.tasks.join_next().await.is_some() {}
        })
        .await;
        if stopped.is_err() {
            tracing::warn!(
                lease = lease.raw(),
                "the run's services did not stop in time; their execs are abandoned"
            );
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<SandboxLeaseId, Held>> {
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Held {
    fn start(&mut self, lease: SandboxLeaseId, services: &[RunService]) {
        let now = Instant::now();
        let stop = CancellationToken::new();
        let mut tasks = JoinSet::new();
        let mut waits = Vec::with_capacity(services.len());
        for service in services {
            tracing::info!(
                lease = lease.raw(),
                sandbox = %self.sandbox.id(),
                service = %service.name,
                "starting the run's service"
            );
            let (state, receiver) = watch::channel(None);
            tasks.spawn(hold(
                Arc::clone(&self.sandbox),
                self.gate.clone(),
                service.name.clone(),
                self.exec_spec(service),
                stop.clone(),
                state,
            ));
            waits.push(Wait {
                name:     service.name.clone(),
                deadline: service.ready.as_ref().map(|ready| now + ready.timeout),
                state:    receiver,
            });
        }
        self.running = Some(Running { stop, tasks, waits });
    }

    /// The held exec: the supervisor, as a workload command with no
    /// deadline.
    fn exec_spec(&self, service: &RunService) -> ExecSpec {
        let mut spec = ExecSpec::new("sh")
            .args([
                "-c",
                SUPERVISOR,
                "fabro-service-supervisor",
                &service.name,
                &service.command,
            ])
            .no_timeout()
            .working_dir(self.working_dir.as_str());
        if let Some(ready) = &service.ready {
            spec = spec.args(ready.probe.args());
        }
        for (key, value) in &self.env {
            spec = spec.env_var(key.as_str(), value.as_str());
        }
        spec.env_var("FABRO_SERVICE_CLI", CLI)
    }
}

/// Keeps one service's supervisor running until `stop` fires, mirroring
/// its lines into the run log and its states into `state`.
async fn hold(
    sandbox: Arc<dyn Sandbox>,
    gate: RunGate,
    name: String,
    spec: ExecSpec,
    stop: CancellationToken,
    state: watch::Sender<Option<ServiceState>>,
) {
    let mut respawn = RESPAWN_MIN;
    loop {
        let started = Instant::now();
        // A run that has finished admits nothing more into its sandbox; a
        // held exec does not hold the run's close up (it never settles).
        let Ok(admission) = gate.admit_unawaited("service") else {
            return;
        };
        let mut process = spawn_streamed(
            Arc::clone(&sandbox),
            Job::Exec(spec.clone()),
            false,
            OutputMode::Lines,
            admission,
        );
        let mut lines = process.lines();
        let stopping = loop {
            let Some(receiver) = lines.as_mut() else {
                break false;
            };
            tokio::select! {
                () = stop.cancelled() => break true,
                line = receiver.recv() => match line {
                    Some(line) => report(&name, &line.line, &state),
                    None => break false,
                },
            }
        };
        if stopping {
            let _ = process.signal(Sig::Term).await;
            let stopped = time::timeout(STOP_GRACE, async {
                if let Some(receiver) = lines.as_mut() {
                    while let Some(line) = receiver.recv().await {
                        report(&name, &line.line, &state);
                    }
                }
                process.wait().await
            })
            .await;
            if stopped.is_err() {
                let _ = process.signal(Sig::Kill).await;
                let _ = time::timeout(STOP_GRACE, process.wait()).await;
            }
            return;
        }
        let ended = tokio::select! {
            () = stop.cancelled() => return,
            ended = process.wait() => ended,
        };
        match ended {
            Ok(status) => tracing::warn!(
                service = %name,
                code = ?status.code,
                signal = ?status.signal,
                "the service's supervisor exited; starting it again"
            ),
            Err(error) => tracing::warn!(
                service = %name,
                error = %error,
                "the service's supervisor exec was lost; starting it again"
            ),
        }
        if started.elapsed() > RESPAWN_MAX {
            respawn = RESPAWN_MIN;
        }
        tokio::select! {
            () = stop.cancelled() => return,
            () = time::sleep(respawn) => {}
        }
        respawn = (respawn * 2).min(RESPAWN_MAX);
    }
}

/// One supervisor line into the run log, and its state, if it reports one.
fn report(name: &str, line: &str, state: &watch::Sender<Option<ServiceState>>) {
    match ServiceState::from_line(line) {
        Some(ServiceState::Crashed) => {
            state.send_replace(Some(ServiceState::Crashed));
            tracing::warn!(service = name, "{line}");
        }
        Some(next) => {
            state.send_replace(Some(next));
            tracing::info!(service = name, "{line}");
        }
        None => tracing::info!(service = name, "{line}"),
    }
}

/// Waits for each service until it is ready or settled, at most until its
/// deadline. The caller that started it reports how that went: not being
/// ready is a warning, never an error.
async fn wait_ready(waits: Vec<(Wait, bool)>) {
    for (
        Wait {
            name,
            deadline,
            mut state,
        },
        report,
    ) in waits
    {
        let Some(deadline) = deadline else {
            continue;
        };
        let settled = state.wait_for(|state| {
            matches!(
                state,
                Some(ServiceState::Ready | ServiceState::Crashed | ServiceState::Stopped)
            )
        });
        let settled = time::timeout_at(deadline, settled).await;
        if !report {
            continue;
        }
        match settled {
            Ok(Ok(settled)) if *settled == Some(ServiceState::Ready) => {
                tracing::info!(service = %name, "the run's service is ready");
            }
            Ok(Ok(settled)) => tracing::warn!(
                service = %name,
                state = ?*settled,
                "the run's service did not get ready; the run goes on without it"
            ),
            Ok(Err(_)) => tracing::warn!(
                service = %name,
                "the run's service stopped before it got ready; the run goes on without it"
            ),
            Err(_) => tracing::warn!(
                service = %name,
                "the run's service is not ready in time; the run goes on without waiting"
            ),
        }
    }
}

/// The check [`RunServices::held_before`] runs in a sandbox whose services
/// keep their state under `root`.
pub(crate) fn held_before_spec(root: &str, services: &[RunService]) -> ExecSpec {
    ExecSpec::new("sh")
        .args(["-c", HELD_BEFORE, "fabro-service-held-before"])
        .args(services.iter().map(|service| service.name.as_str()))
        .env_var(SERVICES_DIR_VAR, root)
        .timeout(HELD_BEFORE_TIMEOUT)
}

/// Whether two handles are the same object: a fenced sandbox keeps its id
/// but comes back as a new handle.
fn same_handle(left: &Arc<dyn Sandbox>, right: &Arc<dyn Sandbox>) -> bool {
    ptr::addr_eq(Arc::as_ptr(left), Arc::as_ptr(right))
}

/// Exposes the services to a workload command: where their state lives,
/// and `fabro-service` on its `PATH`. Last, so a file a command drops there
/// never shadows a tool Fabro itself runs in the sandbox (`git`, say).
pub(crate) fn expose_services(
    env: &mut BTreeMap<SmolStr, SmolStr>,
    ambient: &BTreeMap<String, String>,
    root: &str,
) {
    let path = env
        .get("PATH")
        .map(ToString::to_string)
        .or_else(|| ambient.get("PATH").cloned());
    env.insert(SmolStr::new(SERVICES_DIR_VAR), SmolStr::new(root));
    if let Some(path) = path {
        env.insert(
            SmolStr::new("PATH"),
            SmolStr::new(format!("{path}:{root}/.bin")),
        );
    }
}
