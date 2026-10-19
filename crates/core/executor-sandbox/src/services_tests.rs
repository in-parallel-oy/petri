//! Run services: the supervisor script driven directly with a stub command,
//! and their lifecycle under a lease: held from the end of the prepare
//! steps, spawned again when the held exec ends, respawned on a fenced
//! sandbox, and stopped before the sandbox at release.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use async_trait::async_trait;
use executor::{
    AcquireContext, EnvError, Executor, Retention, SandboxLeaseId, ScopeOutcome, ScopeSpec,
};
use sandbox_driver::{
    Capabilities, EventContext, Exec, ExecControls, ExecResult, ExecSpec, ExecStreamingResult,
    Filesystem, Isolation, OutputStream, PlatformInfo, ProviderKind, Sandbox, SandboxFilter,
    SandboxId, SandboxProvider, SandboxSpec, SandboxState, SandboxStatus, Termination,
};
use testkit::RunDir;
use tokio::fs;
use tokio::process::{Child, Command};
use tokio::sync::Notify;
use tokio::time::{Instant, sleep, timeout};

use crate::services::{CLI, SUPERVISOR, held_before_spec};
use crate::{
    MemoryLedger, ProviderSource, ReadyProbe, RunIdentity, RunService, RunServices, SERVICES_DIR,
    SERVICES_DIR_VAR, SandboxExecutor, SandboxOptions, ServiceReady,
};

const LEASE: SandboxLeaseId = SandboxLeaseId::new(3);
const WORKSPACE: &str = "scope-0";

/// Polls `check` until it holds, for at most `seconds`.
async fn eventually(seconds: u64, what: &str, mut check: impl FnMut() -> bool) {
    until(seconds, what, || {
        let held = check();
        async move { held }
    })
    .await;
}

/// [`eventually`] for a check that reads files or runs commands.
async fn until<F: Future<Output = bool>>(seconds: u64, what: &str, mut check: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(50)).await;
    }
}

// ── The supervisor script ────────────────────────────────────────────────

/// One supervisor running on this machine under `root`, as a held exec
/// would run it in a sandbox.
struct Supervised {
    root:  PathBuf,
    child: Child,
}

impl Supervised {
    fn start(root: &Path, command: &str, probe: Option<[&str; 2]>) -> Self {
        Self::start_with(root, command, probe, &[])
    }

    fn start_with(
        root: &Path,
        command: &str,
        probe: Option<[&str; 2]>,
        env: &[(&str, &str)],
    ) -> Self {
        let mut supervisor = Command::new("sh");
        supervisor
            .args(["-c", SUPERVISOR, "fabro-service-supervisor", "app", command])
            .args(probe.into_iter().flatten())
            .current_dir(root)
            .env(SERVICES_DIR_VAR, root)
            .env("FABRO_SERVICE_CLI", CLI)
            .env("FABRO_SERVICE_BACKOFF", "0")
            .env("FABRO_SERVICE_MAX_CRASHES", "3")
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        Self {
            root:  root.to_path_buf(),
            child: supervisor.spawn().expect("sh runs the supervisor"),
        }
    }

    async fn field(&self, key: &str) -> Option<String> {
        let status = fs::read_to_string(self.root.join("app/status"))
            .await
            .ok()?;
        status
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{key}=")).map(str::to_owned))
    }

    async fn state_becomes(&self, state: &str, seconds: u64) {
        until(seconds, &format!("state {state}"), || async {
            self.field("state").await.as_deref() == Some(state)
        })
        .await;
    }

    /// `fabro-service` as an agent runs it, from its installed path.
    async fn cli(&self, args: &[&str]) -> (bool, String) {
        let output = Command::new(self.root.join(".bin/fabro-service"))
            .args(args)
            .env(SERVICES_DIR_VAR, &self.root)
            .output()
            .await
            .expect("fabro-service runs");
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        (output.status.success(), text)
    }

    /// TERMs the supervisor, as a stopped held exec does, and returns what
    /// it printed.
    async fn terminate(self) -> String {
        let pid = self.child.id().expect("the supervisor is running");
        let signalled = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .await
            .expect("kill runs");
        assert!(signalled.success());
        self.exits().await
    }

    /// Waits for the supervisor to exit by itself, and returns what it
    /// printed.
    async fn exits(self) -> String {
        let output = timeout(Duration::from_secs(20), self.child.wait_with_output())
            .await
            .expect("the supervisor exits on TERM")
            .expect("its output");
        assert!(output.status.success(), "{:?}", output.status);
        String::from_utf8_lossy(&output.stdout).into_owned()
    }
}

async fn alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .stderr(Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

#[tokio::test]
async fn a_crashing_service_restarts_with_backoff_then_stays_crashed_until_started() {
    let dir = RunDir::new("services-crash");
    let supervised = Supervised::start(dir.path(), "echo boom; exit 3", None);
    supervised.state_becomes("crashed", 20).await;
    assert_eq!(supervised.field("restarts").await.as_deref(), Some("2"));
    assert_eq!(supervised.field("last_exit").await.as_deref(), Some("3"));

    // Left down: nothing restarts it by itself.
    sleep(Duration::from_secs(2)).await;
    assert_eq!(supervised.field("restarts").await.as_deref(), Some("2"));
    let (ok, list) = supervised.cli(&["list"]).await;
    assert!(ok, "{list}");
    assert!(
        list.lines().any(|line| {
            line.split_whitespace().collect::<Vec<_>>() == ["app", "crashed", "2", "3"]
        }),
        "{list}"
    );

    // An agent starts it again; it gets a fresh run of quick crashes.
    let (ok, started) = supervised.cli(&["start", "app"]).await;
    assert!(ok, "{started}");
    until(10, "a restart", || async {
        supervised.field("state").await.as_deref() != Some("crashed")
    })
    .await;
    supervised.state_becomes("crashed", 20).await;
    assert_eq!(supervised.field("restarts").await.as_deref(), Some("5"));

    let (ok, logs) = supervised.cli(&["logs", "-n", "1", "app"]).await;
    assert!(ok, "{logs}");
    assert_eq!(logs.trim(), "boom");
    let printed = supervised.terminate().await;
    assert!(
        printed.contains("state=crashed restarts=2 last_exit=3"),
        "{printed}"
    );
    // The service's output may hold secrets: it stays in its log, out of
    // the lines the host mirrors into the run log.
    assert!(!printed.contains("boom"), "{printed}");
}

#[tokio::test]
async fn the_cli_stops_starts_and_restarts_a_service_and_its_children() {
    let dir = RunDir::new("services-control");
    let command = "echo up; sleep 600 & echo $! > child.pid; wait";
    let supervised = Supervised::start(dir.path(), command, Some(["command", "test -f child.pid"]));
    supervised.state_becomes("ready", 10).await;
    let child = fs::read_to_string(dir.path().join("child.pid"))
        .await
        .expect("the child's pid");
    let child = child.trim().to_owned();
    assert!(alive(&child).await);

    let (ok, stopped) = supervised.cli(&["stop", "app"]).await;
    assert!(ok, "{stopped}");
    assert_eq!(stopped.trim(), "app: stopped");
    until(5, "the service's child to end with it", || async {
        !alive(&child).await
    })
    .await;

    fs::remove_file(dir.path().join("child.pid"))
        .await
        .expect("the old pid file");
    let (ok, started) = supervised.cli(&["start", "app"]).await;
    assert!(ok, "{started}");
    supervised.state_becomes("ready", 10).await;
    assert_eq!(supervised.field("restarts").await.as_deref(), Some("1"));

    let (ok, restarted) = supervised.cli(&["restart", "app"]).await;
    assert!(ok, "{restarted}");
    assert_eq!(supervised.field("restarts").await.as_deref(), Some("2"));
    supervised.state_becomes("ready", 10).await;

    let (ok, unknown) = supervised.cli(&["stop", "nope"]).await;
    assert!(!ok);
    assert!(unknown.contains("no service named 'nope'"), "{unknown}");

    let child = fs::read_to_string(dir.path().join("child.pid"))
        .await
        .expect("the child's pid");
    let child = child.trim().to_owned();
    let printed = supervised.terminate().await;
    assert!(
        printed
            .trim_end()
            .ends_with("state=stopped restarts=2 last_exit=-"),
        "{printed}"
    );
    until(
        5,
        "the service's child to end with the supervisor",
        || async { !alive(&child).await },
    )
    .await;
    assert!(
        !fs::try_exists(dir.path().join("app/supervisor"))
            .await
            .expect("stat")
    );
}

/// The pid a service command wrote to `child.pid` under `root`.
async fn child_pid(root: &Path) -> String {
    fs::read_to_string(root.join("child.pid"))
        .await
        .expect("the child's pid")
        .trim()
        .to_owned()
}

#[tokio::test]
async fn a_new_supervisor_takes_the_service_over_from_an_old_one() {
    let dir = RunDir::new("services-takeover");
    let command = "sleep 600 & echo $! > child.pid; wait";
    let old = Supervised::start(dir.path(), command, None);
    old.state_becomes("running", 10).await;
    let old_child = child_pid(dir.path()).await;
    fs::remove_file(dir.path().join("child.pid"))
        .await
        .expect("the old pid file");

    // Its exec's connection dropped, say: the host starts another.
    let new = Supervised::start(dir.path(), command, None);
    let printed = old.exits().await;
    assert!(
        !alive(&old_child).await,
        "the old service outlived its supervisor"
    );
    // The old one leaves the state to the new one.
    assert!(!printed.contains("state=stopped"), "{printed}");
    until(10, "the new service", || async {
        fs::try_exists(dir.path().join("child.pid"))
            .await
            .unwrap_or(false)
    })
    .await;
    new.state_becomes("running", 10).await;
    let new_child = child_pid(dir.path()).await;
    assert!(alive(&new_child).await);
    new.terminate().await;
    assert!(!alive(&new_child).await);
}

#[tokio::test]
async fn a_supervisor_whose_claim_is_taken_stops_its_service_and_leaves() {
    // What a supervisor in another PID namespace sees of a newer one: its
    // claim, which it cannot check for life.
    let dir = RunDir::new("services-claim");
    let command = "sleep 600 & echo $! > child.pid; wait";
    let supervised = Supervised::start(dir.path(), command, None);
    supervised.state_becomes("running", 10).await;
    let child = child_pid(dir.path()).await;
    fs::write(dir.path().join("app/supervisor"), "1 pid:[1]\n")
        .await
        .expect("the claim");
    let printed = supervised.exits().await;
    assert!(!printed.contains("state=stopped"), "{printed}");
    assert!(!alive(&child).await);
    assert_eq!(
        fs::read_to_string(dir.path().join("app/supervisor"))
            .await
            .expect("the claim stays"),
        "1 pid:[1]\n"
    );
}

#[tokio::test]
async fn the_http_probe_reaches_the_service_past_a_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a port");
    let port = listener.local_addr().expect("its address").port();
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut request = [0_u8; 1024];
            let _ = stream.read(&mut request);
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        }
    });
    let dir = RunDir::new("services-http");
    let url = format!("http://127.0.0.1:{port}/health");
    // A proxy nothing listens on, and no NO_PROXY: a probe through it
    // would never read ready.
    let supervised = Supervised::start_with(dir.path(), "sleep 600", Some(["http", &url]), &[
        ("HTTP_PROXY", "http://127.0.0.1:9"),
        ("http_proxy", "http://127.0.0.1:9"),
    ]);
    supervised.state_becomes("ready", 15).await;
    supervised.terminate().await;
}

#[tokio::test]
async fn the_tcp_probe_waits_for_a_listener() {
    let dir = RunDir::new("services-tcp");
    let port = {
        let probe = TcpListener::bind("127.0.0.1:0").expect("a port");
        probe.local_addr().expect("its address").port()
    };
    let address = format!("127.0.0.1:{port}");
    let supervised = Supervised::start(dir.path(), "sleep 600", Some(["tcp", &address]));
    supervised.state_becomes("starting", 10).await;
    sleep(Duration::from_secs(2)).await;
    assert_eq!(supervised.field("state").await.as_deref(), Some("starting"));
    let _listener = TcpListener::bind(&address).expect("the port again");
    supervised.state_becomes("ready", 15).await;
    supervised.terminate().await;
}

/// Runs a resumed run's check for earlier services on this machine, as a
/// sandbox would.
async fn held_before(root: &Path, names: &[&str]) -> bool {
    let services: Vec<RunService> = names
        .iter()
        .map(|name| RunService {
            name:    (*name).to_owned(),
            command: "true".to_owned(),
            ready:   None,
        })
        .collect();
    let spec = held_before_spec(&root.to_string_lossy(), &services);
    Command::new(&spec.program)
        .args(&spec.args)
        .envs(&spec.env)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .expect("sh runs the check")
        .success()
}

#[tokio::test]
async fn the_check_for_earlier_services_finds_a_supervisors_state() {
    let dir = RunDir::new("services-held-before");
    assert!(!held_before(dir.path(), &["app"]).await);

    // A supervisor that ran and ended, as one does when its sandbox is
    // fenced, leaves its state behind.
    let supervised = Supervised::start(dir.path(), "sleep 600", None);
    supervised.state_becomes("running", 10).await;
    supervised.terminate().await;
    assert!(held_before(dir.path(), &["app"]).await);
    assert!(held_before(dir.path(), &["web", "app"]).await);
    // Only a declared service counts.
    assert!(!held_before(dir.path(), &["web"]).await);
}

// ── The lease lifecycle ──────────────────────────────────────────────────

/// Everything the fake sandbox saw, in order, across its handles.
#[derive(Default)]
struct World {
    journal: Mutex<Vec<String>>,
    specs:   Mutex<Vec<ExecSpec>>,
    /// Ends every running held exec, as a supervisor that exits would.
    end:     Notify,
    alive:   AtomicUsize,
    /// Whether a held exec reports ready once its probe is asked for.
    ready:   AtomicUsize,
    /// Whether the sandbox holds a supervisor's state from before.
    held:    AtomicUsize,
}

impl World {
    fn push(&self, event: impl Into<String>) {
        self.journal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.into());
    }

    fn events(&self) -> Vec<String> {
        self.journal
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn count(&self, event: &str) -> usize {
        self.events().iter().filter(|seen| *seen == event).count()
    }
}

/// One handle on the sandbox: an attach after a fence is a new one.
struct FakeSandbox {
    id:           SandboxId,
    capabilities: Capabilities,
    world:        Arc<World>,
}

#[async_trait]
impl Sandbox for FakeSandbox {
    fn id(&self) -> &SandboxId {
        &self.id
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn describe(&self) -> sandbox_driver::Result<SandboxStatus> {
        Ok(SandboxStatus::new(self.id.clone(), SandboxState::Running))
    }

    fn working_directory(&self) -> &str {
        crate::CONTAINER_WORKSPACE
    }

    async fn environment(&self) -> sandbox_driver::Result<BTreeMap<String, String>> {
        Ok(BTreeMap::from([(
            "PATH".to_owned(),
            "/usr/bin:/bin".to_owned(),
        )]))
    }

    async fn platform_info(&self) -> sandbox_driver::Result<PlatformInfo> {
        panic!("services do not probe the platform")
    }

    async fn start(&self) -> sandbox_driver::Result<()> {
        self.world.push("start");
        Ok(())
    }

    async fn stop(&self) -> sandbox_driver::Result<()> {
        self.world.push("stop");
        Ok(())
    }

    async fn delete(&self) -> sandbox_driver::Result<()> {
        self.world.push("delete");
        self.world.alive.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn exec(&self) -> &dyn Exec {
        self
    }

    fn fs(&self) -> &dyn Filesystem {
        panic!("services do not touch the workspace's files")
    }
}

#[async_trait]
impl Exec for FakeSandbox {
    /// The only buffered exec: a resumed run's check for services the
    /// sandbox held before.
    async fn run(&self, spec: &ExecSpec) -> sandbox_driver::Result<ExecResult> {
        assert_eq!(
            spec.args.get(2).map(String::as_str),
            Some("fabro-service-held-before"),
            "{spec:?}"
        );
        self.world.push("held-before");
        let code = i32::from(self.world.held.load(Ordering::SeqCst) == 0);
        Ok(ExecResult::new(
            Termination::Exited,
            Some(code),
            Duration::ZERO,
        ))
    }

    /// A supervisor: reports its state, then runs until it is stopped or
    /// told to end.
    async fn run_streaming(
        &self,
        spec: &ExecSpec,
        controls: ExecControls,
    ) -> sandbox_driver::Result<ExecStreamingResult> {
        let name = spec.args[3].clone();
        self.world.push(format!("exec {name}"));
        self.world
            .specs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(spec.clone());
        let sink = controls.sink.expect("a held exec streams its output");
        let (term, kill) = (
            controls.term.expect("a held exec can be stopped"),
            controls.kill.expect("a held exec can be killed"),
        );
        let probed = spec.args.len() > 5;
        let first = if probed {
            "state=starting"
        } else {
            "state=running"
        };
        sink(
            OutputStream::Stdout,
            format!("{first} restarts=0 last_exit=-\n").into_bytes(),
        )
        .await?;
        if probed && self.world.ready.load(Ordering::SeqCst) > 0 {
            sink(
                OutputStream::Stdout,
                b"state=ready restarts=0 last_exit=-\n".to_vec(),
            )
            .await?;
        }
        let termination = tokio::select! {
            () = term.cancelled() => {
                self.world.push(format!("term {name}"));
                sink(OutputStream::Stdout, b"state=stopped restarts=0 last_exit=-\n".to_vec()).await?;
                Termination::Exited
            }
            () = kill.cancelled() => Termination::Killed,
            () = self.world.end.notified() => Termination::Exited,
        };
        Ok(ExecStreamingResult::new(ExecResult::new(
            termination,
            Some(0),
            Duration::ZERO,
        )))
    }
}

struct FakeProvider {
    kind:         ProviderKind,
    capabilities: Capabilities,
    world:        Arc<World>,
}

impl FakeProvider {
    fn handle(&self) -> Arc<dyn Sandbox> {
        Arc::new(FakeSandbox {
            id:           SandboxId::try_new("sandbox-1").expect("the test sandbox id is valid"),
            capabilities: self.capabilities.clone(),
            world:        self.world.clone(),
        })
    }
}

#[async_trait]
impl SandboxProvider for FakeProvider {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }

    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn create(
        &self,
        _spec: &SandboxSpec,
        _events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        self.world.push("create");
        self.world.alive.store(1, Ordering::SeqCst);
        Ok(self.handle())
    }

    async fn attach(
        &self,
        _id: &SandboxId,
        _events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        self.world.push("attach");
        Ok(self.handle())
    }

    async fn list(&self, _filter: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
        if self.world.alive.load(Ordering::SeqCst) == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![SandboxStatus::new(
            SandboxId::try_new("sandbox-1").expect("the test sandbox id is valid"),
            SandboxState::Running,
        )])
    }
}

/// A provider whose plugin generation the test moves on, as a restarted
/// plugin process does.
struct Generations {
    provider:   Arc<FakeProvider>,
    generation: AtomicU64,
}

#[async_trait]
impl ProviderSource for Generations {
    async fn current(&self) -> Result<(Arc<dyn SandboxProvider>, u64), EnvError> {
        Ok((
            self.provider.clone() as Arc<dyn SandboxProvider>,
            self.generation.load(Ordering::SeqCst),
        ))
    }

    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the trait fixes the signature"
    )]
    fn fingerprint(&self) -> &str {
        "fake:fixed"
    }

    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "the trait fixes the signature"
    )]
    fn kind(&self) -> &str {
        "fake"
    }
}

struct Fixture {
    dir:      RunDir,
    world:    Arc<World>,
    source:   Arc<Generations>,
    ledger:   Arc<MemoryLedger>,
    services: Arc<RunServices>,
}

impl Fixture {
    fn new(ready: Option<Duration>) -> Self {
        let world = Arc::new(World::default());
        let provider = Arc::new(FakeProvider {
            kind:         ProviderKind::try_new("fake").expect("the test provider kind is valid"),
            capabilities: Capabilities::minimal(Isolation::Container),
            world:        world.clone(),
        });
        Self {
            dir: RunDir::new("services-lease"),
            world,
            source: Arc::new(Generations {
                provider,
                generation: AtomicU64::new(1),
            }),
            ledger: Arc::new(MemoryLedger::default()),
            services: Arc::new(RunServices::new(vec![RunService {
                name:    "app".to_owned(),
                command: "bin/dev".to_owned(),
                ready:   ready.map(|wait| ServiceReady {
                    probe:   ReadyProbe::Http("http://127.0.0.1:4000/health".to_owned()),
                    timeout: wait,
                }),
            }])),
        }
    }

    /// An executor as a worker process builds it: its own lease manager
    /// over the run's durable ledger, and the run's services.
    fn executor(&self, services: &Arc<RunServices>) -> SandboxExecutor {
        SandboxExecutor::new(
            self.source.clone(),
            self.ledger.clone(),
            Arc::new(RunIdentity::new(self.dir.path().to_path_buf(), "run-1")),
            Retention::Always,
            Some(crate::DOCKER_HOST_ALIAS.to_owned()),
            SandboxOptions {
                services: Some(services.clone()),
                ..SandboxOptions::default()
            },
        )
    }

    fn execs(&self) -> usize {
        self.world.count("exec app")
    }
}

fn scope() -> ScopeSpec {
    ScopeSpec::new(ir::ScopeId::new(0), WORKSPACE).with_runtime(ir::RuntimeSpec::container("img"))
}

fn managed() -> AcquireContext {
    AcquireContext::bare().with_lease(LEASE)
}

#[tokio::test]
async fn services_start_once_prepare_is_done_and_the_workspace_is_in_place() {
    let fixture = Fixture::new(None);
    let executor = fixture.executor(&fixture.services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("the sandbox is created");
    // Neither a raw acquire nor the workspace alone starts anything.
    fixture.services.scope_ready(WORKSPACE).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.execs(), 0);

    fixture.services.prepared().await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;
    // Said again before the next stage, it starts nothing more.
    fixture.services.prepared().await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.execs(), 1);

    let spec = fixture
        .world
        .specs
        .lock()
        .unwrap_or_else(PoisonError::into_inner)[0]
        .clone();
    assert_eq!(spec.program, "sh");
    assert_eq!(&spec.args[2..], [
        "fabro-service-supervisor",
        "app",
        "bin/dev"
    ]);
    assert_eq!(spec.timeout, None);
    assert_eq!(
        spec.working_dir.as_deref(),
        Some(crate::CONTAINER_WORKSPACE)
    );
    assert_eq!(
        spec.env.get(SERVICES_DIR_VAR).map(String::as_str),
        Some(SERVICES_DIR)
    );
    assert_eq!(
        spec.env.get("PATH").map(String::as_str),
        Some("/usr/bin:/bin:/tmp/fabro-services/.bin")
    );
    assert!(spec.env.contains_key("FABRO_SERVICE_CLI"));
    // Every workload command finds `fabro-service` on its PATH too.
    let workload = handle.exec();
    assert_eq!(
        workload.ambient_env("PATH").as_deref(),
        Some("/usr/bin:/bin:/tmp/fabro-services/.bin")
    );
    drop(handle);
}

#[tokio::test]
async fn a_supervisor_that_ends_is_spawned_again() {
    let fixture = Fixture::new(None);
    let executor = fixture.executor(&fixture.services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    fixture.services.prepared().await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;

    sleep(Duration::from_millis(100)).await;
    fixture.world.end.notify_waiters();
    eventually(5, "the respawned exec", || fixture.execs() == 2).await;
    drop(handle);
}

#[tokio::test]
async fn release_stops_the_services_before_the_sandbox() {
    for retention in [Retention::Always, Retention::Never] {
        let fixture = Fixture::new(None);
        let executor = fixture.executor(&fixture.services);
        let handle = executor
            .acquire(&scope(), &managed())
            .await
            .expect("acquired");
        fixture.services.scope_ready(WORKSPACE).await;
        fixture.services.prepared().await;
        eventually(5, "the held exec", || fixture.execs() == 1).await;

        executor.release(handle, ScopeOutcome::Succeeded).await;
        let report = executor
            .manager()
            .release_lease(LEASE, retention, ScopeOutcome::Succeeded)
            .await;
        assert!(report.is_clean(), "{report:?}");
        let events = fixture.world.events();
        let term = events.iter().position(|event| event == "term app");
        let end = events
            .iter()
            .position(|event| event == "stop" || event == "delete");
        assert!(term.is_some() && term < end, "{events:?}");
        // Nothing respawns a stopped service.
        sleep(Duration::from_millis(1500)).await;
        assert_eq!(fixture.execs(), 1);
    }
}

#[tokio::test]
async fn a_fenced_sandbox_gets_its_services_back_once_its_workspace_is_in_place() {
    let fixture = Fixture::new(None);
    let executor = fixture.executor(&fixture.services);
    let first = executor
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    fixture.services.prepared().await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;

    // A new plugin generation: the next acquisition fences the sandbox and
    // brings a new handle, whose services wait for the host's word.
    fixture.source.generation.store(2, Ordering::SeqCst);
    let second = executor
        .acquire(&scope(), &managed())
        .await
        .expect("fenced");
    let events = fixture.world.events();
    assert!(
        events.ends_with(&["attach".to_owned(), "stop".to_owned(), "start".to_owned()]),
        "{events:?}"
    );
    sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.execs(), 1);

    fixture.services.scope_ready(WORKSPACE).await;
    eventually(5, "the respawned exec", || fixture.execs() == 2).await;
    drop((first, second));
}

#[tokio::test]
async fn a_resumed_run_starts_services_in_the_recovered_sandbox() {
    let fixture = Fixture::new(None);
    let handle = fixture
        .executor(&fixture.services)
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    drop(handle);

    // A new worker process that admits a stage past the prepare steps
    // before acquiring its scope: its own services, told first that the
    // run is past them, then that the fenced workspace is restored.
    let services = Arc::new(RunServices::new(fixture.services.services().to_vec()));
    let executor = fixture.executor(&services);
    services.prepared().await;
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("recovered");
    assert_eq!(fixture.execs(), 0);
    services.scope_ready(WORKSPACE).await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;
    drop(handle);
}

#[tokio::test]
async fn a_resumed_run_restores_services_its_sandbox_held_before_the_restart() {
    let fixture = Fixture::new(None);
    let handle = fixture
        .executor(&fixture.services)
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    fixture.services.prepared().await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;
    fixture.world.held.store(1, Ordering::SeqCst);
    drop(handle);

    // A new worker process re-dispatches the stage the old one was running,
    // with no admission to say the prepare steps are done: the fenced
    // sandbox's services say it.
    let services = Arc::new(RunServices::new(fixture.services.services().to_vec()));
    let executor = fixture.executor(&services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("recovered");
    services.scope_resumed(WORKSPACE).await;
    eventually(5, "the restored held exec", || fixture.execs() == 2).await;
    assert_eq!(fixture.world.count("held-before"), 1);

    // Said again before the next stage, it starts nothing more.
    services.prepared().await;
    services.scope_resumed(WORKSPACE).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.execs(), 2);
    assert_eq!(fixture.world.count("held-before"), 1);
    drop(handle);
}

#[tokio::test]
async fn services_a_replacement_sandbox_never_held_wait_for_the_next_stage() {
    let fixture = Fixture::new(None);
    let handle = fixture
        .executor(&fixture.services)
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    drop(handle);

    // A sandbox that never held the services, as one that replaced a lost
    // one: resuming starts none until the next stage says the prepare steps
    // are done.
    let services = Arc::new(RunServices::new(fixture.services.services().to_vec()));
    let executor = fixture.executor(&services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("recovered");
    services.scope_resumed(WORKSPACE).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.execs(), 0);
    assert_eq!(fixture.world.count("held-before"), 1);

    services.prepared().await;
    eventually(5, "the held exec", || fixture.execs() == 1).await;
    drop(handle);
}

#[tokio::test]
async fn readiness_is_waited_for_at_most_its_timeout() {
    let fixture = Fixture::new(Some(Duration::from_millis(300)));
    let executor = fixture.executor(&fixture.services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    let started = Instant::now();
    fixture.services.prepared().await;
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(300), "{waited:?}");
    assert!(waited < Duration::from_secs(5), "{waited:?}");
    let spec = fixture
        .world
        .specs
        .lock()
        .unwrap_or_else(PoisonError::into_inner)[0]
        .clone();
    assert_eq!(&spec.args[5..], ["http", "http://127.0.0.1:4000/health"]);
    drop(handle);

    let fixture = Fixture::new(Some(Duration::from_secs(60)));
    fixture.world.ready.store(1, Ordering::SeqCst);
    let executor = fixture.executor(&fixture.services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    let started = Instant::now();
    fixture.services.prepared().await;
    assert!(started.elapsed() < Duration::from_secs(5));
    drop(handle);
}

#[tokio::test]
async fn every_stage_before_the_deadline_waits_for_readiness() {
    let fixture = Fixture::new(Some(Duration::from_millis(600)));
    let executor = fixture.executor(&fixture.services);
    let handle = executor
        .acquire(&scope(), &managed())
        .await
        .expect("acquired");
    fixture.services.scope_ready(WORKSPACE).await;
    // Two stages admitted together after the prepare steps: the second
    // finds the services started and still waits for them.
    let started = Instant::now();
    let ((), second) = tokio::join!(fixture.services.prepared(), async {
        sleep(Duration::from_millis(100)).await;
        fixture.services.prepared().await;
        started.elapsed()
    });
    assert!(second >= Duration::from_millis(550), "{second:?}");
    assert_eq!(fixture.execs(), 1);
    // Past the deadline, a stage no longer waits.
    let started = Instant::now();
    fixture.services.prepared().await;
    assert!(started.elapsed() < Duration::from_millis(100));
    drop(handle);
}
