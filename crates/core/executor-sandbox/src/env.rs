//! [`ExecEnv`] and [`ContainerRunner`] over one [`Sandbox`].
//!
//! A step spawn becomes one `run_streaming` call in an owned task; a
//! Docker action becomes one `OneShot::run` call the same way. Output
//! chunks are fed through the interface crate's own line pump (a duplex
//! pipe per stream), so the 64 KiB line cap and truncation marker match
//! every other executor exactly. The step's cancellation ladder is the only
//! one: `SIGTERM` fires the sandbox-driver `term` token, `SIGKILL` fires
//! `kill`, and the provider sends exactly that signal and nothing more.
//!
//! Workspace files go through the sandbox's filesystem facet. The
//! workspace lives inside the sandbox — a volume it owns — and nothing on
//! Petri's machine mirrors it, so this is the only path to it, on a local
//! daemon and a remote one alike.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use executor::lines::{LINE_CHANNEL_CAPACITY, pump};
use executor::{
    ByteStream, ContainerImage, ContainerRunner, DirectoryEntry, EnvError, ExecEnv, ExitStatus,
    LineStream, OneShotContainer, OutputChunk, OutputMode, PreviewUrl, ProcessHandle, ProcessSpec,
    Sig, StdinMode, StdinWriter,
};
use sandbox_driver::{
    Error as DriverError, ExecControls, ExecSpec, ExecStreamingResult, OneShotImage, OneShotSpec,
    OutputLoss, OutputStream, Sandbox, StdinSource, Termination,
};
use smol_str::SmolStr;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::BACKEND;
use crate::gate::{Admission, RunGate};

/// Bytes of pipe buffer between the output sink and each line pump.
const OUTPUT_PIPE_CAPACITY: usize = 64 * 1024;
/// How long a mkdir for a step's cwd may take.
const MKDIR_TIMEOUT: Duration = Duration::from_secs(30);

/// One live sandbox, handed to step kinds as their spawn capability.
pub(crate) struct SandboxEnv {
    pub(crate) host:         bool,
    pub(crate) sandbox:      Arc<dyn Sandbox>,
    /// The workspace as the sandbox sees it: its working directory.
    pub(crate) workspace:    String,
    /// The effective environment, read once at acquire.
    pub(crate) ambient:      BTreeMap<String, String>,
    /// This holder's scope environment and container options, over the
    /// sandbox's original environment and under the process's own.
    pub(crate) env:          BTreeMap<SmolStr, SmolStr>,
    pub(crate) grace:        Duration,
    /// How a process in the sandbox reaches services on Petri's machine.
    pub(crate) host_address: Option<String>,
    /// The run's admission: every operation passes it, and none starts
    /// after the run closes it.
    pub(crate) gate:         RunGate,
}

/// Maps a finished `run_streaming` to the executor's exit status. The
/// observed signal wins over a code, so a foreign signal and the step's own
/// ladder read the same on every backend. The fallbacks below are for a
/// provider that stopped the command but could not observe how (Daytona ends
/// a session without seeing the child's status).
fn exit_status(termination: Termination, code: Option<i32>, signal: Option<i32>) -> ExitStatus {
    // The provider's own deadline ended the command: the step reads it as a
    // timeout, whatever signal the provider observed on the way.
    if termination == Termination::TimedOut {
        return ExitStatus::timed_out(signal.unwrap_or_else(|| Sig::Kill.number()));
    }
    if let Some(signal) = signal {
        return ExitStatus::signalled(signal);
    }
    match termination {
        Termination::Killed => ExitStatus::signalled(Sig::Kill.number()),
        // The step's ladder decides cancel vs timeout; the handle only needs a
        // plausible signalled status for a stop it did not exit from.
        Termination::Cancelled => ExitStatus::signalled(Sig::Term.number()),
        // Exited, Unknown, and any future variant: report the code as-is.
        _ => ExitStatus {
            code,
            signal: None,
            timed_out: false,
        },
    }
}

/// Retained bytes are deliberately omitted, but delivery to the line pumps
/// must be complete before Petri can report the command's exit status. A
/// loss the provider counted is the exception: it marks both captures
/// truncated because it cannot say which stream lost the bytes, the
/// command still ran to its status, and the loss reaches the step as one
/// stderr line ([`output_loss_line`]) instead of failing it.
fn streaming_exit_status(streaming: &ExecStreamingResult) -> Result<ExitStatus, String> {
    let status = exit_status(
        streaming.result.termination,
        streaming.result.exit_code,
        streaming.result.signal,
    );
    // A command the provider killed at its deadline has its output cut off
    // by design; the timeout is the status, not an incomplete delivery.
    if status.timed_out || streaming.output_loss.is_lossy() {
        return Ok(status);
    }
    if streaming.stdout_capture.truncated || streaming.stderr_capture.truncated {
        return Err("sandbox command output delivery was incomplete".to_owned());
    }
    Ok(status)
}

/// The line appended to a command's stderr when the provider dropped
/// output on its own transport (Daytona's encoded exec resyncs past a torn
/// record and counts it), so the agent reading the output and the run log
/// both learn that some is missing. `None` when nothing was lost.
fn output_loss_line(loss: OutputLoss) -> Option<String> {
    loss.is_lossy().then(|| {
        format!(
            "[sandbox] {} output frame(s), {} bytes dropped by the provider",
            loss.dropped_frames, loss.dropped_bytes
        )
    })
}

/// Logs a counted loss and appends its line to the stderr pipe, after the
/// command's own output, which the sink has fully written by the time the
/// run resolves.
async fn report_output_loss(loss: OutputLoss, stderr: &Mutex<Option<DuplexStream>>) {
    let Some(line) = output_loss_line(loss) else {
        return;
    };
    tracing::warn!(
        dropped_frames = loss.dropped_frames,
        dropped_bytes = loss.dropped_bytes,
        "sandbox provider dropped command output"
    );
    let mut guard = stderr.lock().await;
    if let Some(writer) = guard.as_mut() {
        let _ = writer.write_all(line.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

/// What one streamed job runs: a step's exec, an action's container, or a
/// run service's held exec ([`crate::services`]).
pub(crate) enum Job {
    Exec(ExecSpec),
    OneShot(OneShotSpec),
}

/// Runs `job` in an owned task with the interface's line pumps on its
/// output and the step's stop tokens on its controls. The task holds
/// `admission` until the job settles, so the run's gate waits for it.
pub(crate) fn spawn_streamed(
    sandbox: Arc<dyn Sandbox>,
    job: Job,
    stdin: bool,
    output: OutputMode,
    admission: Admission,
) -> SandboxProcess {
    // Stdin: a piped step gets a writer whose read half streams into the
    // command for its whole life.
    let stdin_writer = stdin.then(|| {
        let (writer, reader) = duplex(OUTPUT_PIPE_CAPACITY);
        let source = StdinSource::new(reader);
        (Box::new(writer) as StdinWriter, source)
    });

    // Output: one duplex per stream feeds the interface line pump, so the
    // line cap and truncation marker are the shared ones.
    let (line_tx, line_rx) = mpsc::channel(LINE_CHANNEL_CAPACITY);
    let (stdout_writer, stdout_reader) = duplex(OUTPUT_PIPE_CAPACITY);
    let (stderr_writer, stderr_reader) = duplex(OUTPUT_PIPE_CAPACITY);
    let mut workers = JoinSet::new();
    let (byte_tx, byte_rx) = mpsc::channel(16);
    let (lines, bytes) = match output {
        OutputMode::Lines => {
            workers.spawn(pump(stdout_reader, ir::LogStream::Stdout, line_tx.clone()));
            workers.spawn(pump(stderr_reader, ir::LogStream::Stderr, line_tx));
            (Some(line_rx), None)
        }
        OutputMode::Bytes => {
            workers.spawn(pump_bytes(
                stdout_reader,
                ir::LogStream::Stdout,
                byte_tx.clone(),
            ));
            workers.spawn(pump_bytes(stderr_reader, ir::LogStream::Stderr, byte_tx));
            (None, Some(byte_rx))
        }
    };

    let term = CancellationToken::new();
    let kill = CancellationToken::new();
    let (status_tx, status_rx) = watch::channel(None);

    // The sink writes each chunk into the matching stream pipe; the pumps
    // read those pipes and split into lines. The writers are shared into
    // the closure so the task can shut them for EOF when the run ends.
    let stdout_slot = Arc::new(Mutex::new(Some(stdout_writer)));
    let stderr_slot = Arc::new(Mutex::new(Some(stderr_writer)));
    let sink_stdout = stdout_slot.clone();
    let sink_stderr = stderr_slot.clone();
    let stdin_source = stdin_writer.as_ref().map(|(_, source)| source.clone());
    let controls = ExecControls {
        term:                  Some(term.clone()),
        kill:                  Some(kill.clone()),
        stdin:                 stdin_source,
        sink:                  Some(Arc::new(move |stream, chunk| {
            let slot = match stream {
                OutputStream::Stdout => sink_stdout.clone(),
                OutputStream::Stderr => sink_stderr.clone(),
            };
            Box::pin(async move {
                let mut guard = slot.lock().await;
                if let Some(writer) = guard.as_mut() {
                    let _ = writer.write_all(&chunk).await;
                }
                Ok(())
            })
        })),
        retained_output_limit: Some(0),
    };

    workers.spawn(async move {
        let outcome: Result<ExecStreamingResult, DriverError> = match &job {
            Job::Exec(spec) => sandbox.exec().run_streaming(spec, controls).await,
            Job::OneShot(spec) => match sandbox.one_shot() {
                Some(one_shot) => one_shot.run(spec, controls).await,
                None => Err(DriverError::unsupported(
                    sandbox_driver::Capability::OneShot,
                )),
            },
        };
        if let Ok(streaming) = &outcome {
            report_output_loss(streaming.output_loss, &stderr_slot).await;
        }
        // Close the pipes so the pumps flush and end.
        if let Some(mut writer) = stdout_slot.lock().await.take() {
            let _ = writer.shutdown().await;
        }
        if let Some(mut writer) = stderr_slot.lock().await.take() {
            let _ = writer.shutdown().await;
        }
        let status: Result<ExitStatus, String> = match outcome {
            Ok(streaming) => streaming_exit_status(&streaming),
            Err(error) => Err(error.to_string()),
        };
        let _ = status_tx.send(Some(status));
        drop(admission);
    });

    SandboxProcess {
        lines,
        bytes,
        stdin: stdin_writer.map(|(writer, _)| writer),
        term,
        kill,
        status: status_rx,
        cached: None,
        workers,
    }
}

/// The duplex reader cannot fail independently of its writer. A closed
/// receiver ends the pump; dropping the process owns remote cancellation.
async fn pump_bytes(
    mut reader: impl AsyncRead + Unpin,
    stream: ir::LogStream,
    sender: mpsc::Sender<OutputChunk>,
) {
    let mut buffer = vec![0; 16 * 1024];
    loop {
        let Ok(length) = reader.read(&mut buffer).await else {
            return;
        };
        if length == 0 {
            return;
        }
        if sender
            .send(OutputChunk {
                stream,
                bytes: buffer[..length].to_vec(),
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

/// Maps a facet failure to the executor's error.
fn facet_error(operation: &str, error: &DriverError) -> EnvError {
    EnvError::backend(BACKEND, operation, error.to_string())
}

#[async_trait]
impl ExecEnv for SandboxEnv {
    async fn spawn(&self, spec: ProcessSpec) -> Result<Box<dyn ProcessHandle>, EnvError> {
        let admission = self.gate.admit("exec")?;
        // `docker exec -w` refuses a directory that does not exist yet
        // (`repo/` before the first checkout), so create the step's cwd
        // first, with the one command every image contract provides.
        if let Some(cwd) = &spec.cwd {
            let cwd = cwd.to_string_lossy().into_owned();
            let mkdir = ExecSpec::new("mkdir")
                .args(["-p", cwd.as_str()])
                .timeout(MKDIR_TIMEOUT);
            let result = self
                .sandbox
                .exec()
                .run(&mkdir)
                .await
                .map_err(|error| facet_error("mkdir", &error))?;
            if !result.success() {
                return Err(EnvError::backend(
                    BACKEND,
                    "mkdir",
                    format!("could not create `{cwd}`: {}", result.stderr_lossy().trim()),
                ));
            }
        }
        let working_dir = spec
            .cwd
            .as_ref()
            .map(|cwd| cwd.to_string_lossy().into_owned());

        // The step's program and arguments go to the sandbox as given: the
        // exec contract is an argument vector, so nothing is quoted or
        // interpreted on the way.
        // The step's deadline rides to the provider, which ends the command
        // itself; a step that sets none runs until it exits or is stopped.
        let mut exec_spec =
            ExecSpec::new(spec.program.as_str()).args(spec.args.iter().map(SmolStr::as_str));
        exec_spec = match spec.timeout {
            Some(timeout) => exec_spec.timeout(timeout),
            None => exec_spec.no_timeout(),
        };
        if let Some(dir) = working_dir {
            exec_spec = exec_spec.working_dir(dir);
        }
        for (key, value) in self
            .env
            .iter()
            .filter(|(key, _)| !spec.env.contains_key(*key))
            .chain(spec.env.iter())
        {
            exec_spec = exec_spec.env_var(key.as_str(), value.as_str());
        }
        let stdin = matches!(spec.stdin, StdinMode::Piped);
        Ok(Box::new(spawn_streamed(
            self.sandbox.clone(),
            Job::Exec(exec_spec),
            stdin,
            spec.output,
            admission,
        )))
    }

    fn workspace_path(&self) -> &str {
        &self.workspace
    }

    fn host_address(&self) -> Result<&str, EnvError> {
        self.host_address
            .as_deref()
            .ok_or(EnvError::HostUnreachable)
    }

    fn ambient_env(&self, name: &str) -> Option<String> {
        self.env
            .get(name)
            .map(ToString::to_string)
            .or_else(|| self.ambient.get(name).cloned())
    }

    fn shares_host_filesystem(&self) -> bool {
        self.host
    }

    /// The provider's preview URL for the port: the Host provider answers
    /// with its own loopback, the Docker provider opens a forward on the
    /// plugin's loopback bridged into the container (nothing is published on
    /// the daemon), Daytona returns its preview link and token header. A
    /// provider without the facet offers no route.
    async fn preview_url(&self, port: u16) -> Result<Option<PreviewUrl>, EnvError> {
        let _admission = self.gate.admit("preview_url")?;
        let Some(previews) = self.sandbox.preview_urls() else {
            return Ok(None);
        };
        let preview = previews
            .preview_url(port)
            .await
            .map_err(|error| facet_error("preview_url", &error))?;
        Ok(Some(PreviewUrl {
            url:     preview.url,
            headers: preview.headers,
        }))
    }

    async fn release_preview_url(&self, port: u16) -> Result<(), EnvError> {
        let _admission = self.gate.admit("release_preview_url")?;
        match self.sandbox.preview_urls() {
            Some(previews) => previews
                .release_preview_url(port)
                .await
                .map_err(|error| facet_error("release_preview_url", &error)),
            None => Ok(()),
        }
    }

    async fn read_file(&self, relative: &Path) -> Result<Option<Vec<u8>>, EnvError> {
        let _admission = self.gate.admit("read")?;
        match self.sandbox.fs().read(&relative.to_string_lossy()).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(DriverError::NotFound { .. }) => Ok(None),
            Err(error) => Err(facet_error("read", &error)),
        }
    }

    async fn read_file_limited(
        &self,
        relative: &Path,
        limit: usize,
    ) -> Result<Option<Vec<u8>>, EnvError> {
        let _admission = self.gate.admit("read")?;
        // One byte past the limit is enough to know the file is too big,
        // without ever pulling the whole of it across.
        let bounded = limit.saturating_add(1) as u64;
        match self
            .sandbox
            .fs()
            .read_range(&relative.to_string_lossy(), 0, Some(bounded))
            .await
        {
            Ok(bytes) if bytes.len() > limit => Err(EnvError::workspace(
                "read",
                relative.display(),
                executor::oversized_read(limit),
            )),
            Ok(bytes) => Ok(Some(bytes)),
            Err(DriverError::NotFound { .. }) => Ok(None),
            Err(error) => Err(facet_error("read", &error)),
        }
    }

    async fn write_file(&self, relative: &Path, contents: &[u8]) -> Result<(), EnvError> {
        let _admission = self.gate.admit("write")?;
        self.sandbox
            .fs()
            .write(&relative.to_string_lossy(), contents)
            .await
            .map_err(|error| facet_error("write", &error))
    }

    async fn list_directory(
        &self,
        path: &Path,
        depth: usize,
    ) -> Result<Vec<DirectoryEntry>, EnvError> {
        let _admission = self.gate.admit("list_directory")?;
        self.sandbox
            .fs()
            .list_dir(&path.to_string_lossy(), depth)
            .await
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| DirectoryEntry {
                        path:   entry.path,
                        is_dir: entry.kind == sandbox_driver::FileKind::Directory,
                        size:   if entry.kind == sandbox_driver::FileKind::File {
                            entry.size
                        } else {
                            None
                        },
                    })
                    .collect()
            })
            .map_err(|error| facet_error("list_directory", &error))
    }

    fn grace(&self) -> Duration {
        self.grace
    }
}

/// The process handle a step drives: its output, its stdin, its wait, and the
/// two raw stop signals the step's ladder fires.
pub(crate) struct SandboxProcess {
    lines:   Option<LineStream>,
    bytes:   Option<ByteStream>,
    stdin:   Option<StdinWriter>,
    term:    CancellationToken,
    kill:    CancellationToken,
    status:  watch::Receiver<Option<Result<ExitStatus, String>>>,
    cached:  Option<ExitStatus>,
    workers: JoinSet<()>,
}

impl Drop for SandboxProcess {
    fn drop(&mut self) {
        // Aborting the RPC drops its stream cancellation guard. Scope release
        // remains the fence for remote work; local pumps cannot outlive their
        // process handle while waiting on output or a slow provider.
        self.workers.abort_all();
    }
}

#[async_trait]
impl ProcessHandle for SandboxProcess {
    fn lines(&mut self) -> Option<LineStream> {
        self.lines.take()
    }

    fn bytes(&mut self) -> Option<ByteStream> {
        self.bytes.take()
    }

    fn stdin(&mut self) -> Option<StdinWriter> {
        self.stdin.take()
    }

    async fn wait(&mut self) -> Result<ExitStatus, EnvError> {
        if let Some(status) = self.cached {
            return Ok(status);
        }
        let mut status = self.status.clone();
        loop {
            let current = status.borrow_and_update().clone();
            if let Some(result) = current {
                let value =
                    result.map_err(|message| EnvError::backend(BACKEND, "exec", message))?;
                self.cached = Some(value);
                return Ok(value);
            }
            if status.changed().await.is_err() {
                return Err(EnvError::Gone);
            }
        }
    }

    async fn signal(&mut self, sig: Sig) -> Result<(), EnvError> {
        match sig {
            Sig::Term => self.term.cancel(),
            Sig::Kill => self.kill.cancel(),
        }
        Ok(())
    }
}

/// One-shot containers in a sandbox's world: the provider's own operation,
/// which shares the sandbox's workspace and network namespace, streams
/// through the same pumps, and is ended by the sandbox's stop and delete.
pub(crate) struct OneShotRunner {
    pub(crate) sandbox:      Arc<dyn Sandbox>,
    pub(crate) workspace:    String,
    pub(crate) host_address: Option<String>,
    /// The scope's env, under the spec's own.
    pub(crate) env:          BTreeMap<SmolStr, SmolStr>,
    pub(crate) gate:         RunGate,
}

impl OneShotRunner {
    /// Runs `spec` under an admission its caller already holds.
    pub(crate) fn run_admitted(
        &self,
        spec: OneShotContainer,
        admission: Admission,
    ) -> Result<Box<dyn ProcessHandle>, EnvError> {
        if self.sandbox.one_shot().is_none() {
            return Err(EnvError::backend(
                BACKEND,
                "one-shot",
                "this sandbox provider runs no one-shot containers",
            ));
        }
        let image = match spec.image {
            ContainerImage::Registry { image } => OneShotImage::Registry {
                reference: image.to_string(),
            },
            ContainerImage::Build {
                context,
                dockerfile,
                tag,
                reuse,
            } => OneShotImage::Build {
                context: context.to_string_lossy().into_owned(),
                dockerfile: dockerfile.map(|file| file.to_string()),
                tag: tag.to_string(),
                reuse,
            },
        };
        let mut one_shot = OneShotSpec::new(image);
        if let Some(entrypoint) = spec.entrypoint {
            one_shot = one_shot.entrypoint(entrypoint.as_str());
        }
        one_shot = one_shot.args(spec.args.iter().map(SmolStr::as_str));
        // The scope's env first, the spec's own on top.
        for (key, value) in self
            .env
            .iter()
            .filter(|(key, _)| !spec.env.contains_key(*key))
            .chain(spec.env.iter())
        {
            one_shot = one_shot.env_var(key.as_str(), value.as_str());
        }
        if let Some(workdir) = spec.workdir {
            one_shot = one_shot.working_dir(workdir.as_str());
        }
        Ok(Box::new(spawn_streamed(
            self.sandbox.clone(),
            Job::OneShot(one_shot),
            false,
            OutputMode::Lines,
            admission,
        )))
    }
}

#[async_trait]
impl ContainerRunner for OneShotRunner {
    fn workspace_path(&self) -> &str {
        &self.workspace
    }

    fn host_address(&self) -> Result<&str, EnvError> {
        self.host_address
            .as_deref()
            .ok_or(EnvError::HostUnreachable)
    }

    async fn run(&self, spec: OneShotContainer) -> Result<Box<dyn ProcessHandle>, EnvError> {
        let admission = self.gate.admit("one-shot")?;
        self.run_admitted(spec, admission)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use sandbox_driver::{
        Capabilities, CaptureStats, Exec, ExecResult, Filesystem, Isolation, PlatformInfo,
        SandboxId, SandboxStatus,
    };

    use super::*;

    fn completed_command() -> ExecStreamingResult {
        ExecStreamingResult::new(ExecResult::new(
            Termination::Exited,
            Some(0),
            Duration::ZERO,
        ))
    }

    #[test]
    fn incomplete_stdout_or_stderr_fails_even_when_the_command_exits_zero() {
        for stream in [OutputStream::Stdout, OutputStream::Stderr] {
            let mut result = completed_command();
            match stream {
                OutputStream::Stdout => result.stdout_capture.truncated = true,
                OutputStream::Stderr => result.stderr_capture.truncated = true,
            }
            let error = streaming_exit_status(&result).expect_err("incomplete delivery");
            assert_eq!(error, "sandbox command output delivery was incomplete");
        }
    }

    #[test]
    fn output_delivered_without_a_retained_copy_keeps_the_exit_status() {
        let mut result = completed_command();
        let mut stats = CaptureStats::default();
        stats.observed_bytes = 4096;
        stats.omitted_bytes = 4096;
        result.stdout_capture = stats;
        result.stderr_capture = stats;
        result.result.exit_code = Some(3);
        let status = streaming_exit_status(&result).expect("complete delivery");
        assert_eq!(status.code, Some(3));
        assert_eq!(status.signal, None);
    }

    #[test]
    fn complete_delivery_preserves_the_observed_signal() {
        let mut result = completed_command();
        result.result.signal = Some(Sig::Term.number());
        let status = streaming_exit_status(&result).expect("complete delivery");
        assert_eq!(status.signal, Some(Sig::Term.number()));
    }

    fn counted_loss() -> OutputLoss {
        let mut loss = OutputLoss::default();
        loss.dropped_frames = 2;
        loss.dropped_bytes = 300;
        loss
    }

    #[test]
    fn a_counted_loss_keeps_the_exit_status_although_both_captures_are_marked() {
        let mut result = completed_command();
        result.result.exit_code = Some(3);
        result.output_loss = counted_loss();
        result.stdout_capture.truncated = true;
        result.stderr_capture.truncated = true;
        let status = streaming_exit_status(&result).expect("a counted loss is reported, not fatal");
        assert_eq!(status.code, Some(3));
        assert_eq!(
            output_loss_line(result.output_loss).as_deref(),
            Some("[sandbox] 2 output frame(s), 300 bytes dropped by the provider")
        );
        assert_eq!(output_loss_line(OutputLoss::default()), None);
    }

    /// A command whose output is scripted: one stdout chunk, one stderr
    /// chunk, exit 2, and the given loss report.
    struct ScriptedExec {
        loss: OutputLoss,
    }

    #[async_trait]
    impl Exec for ScriptedExec {
        async fn run(&self, _spec: &ExecSpec) -> sandbox_driver::Result<ExecResult> {
            panic!("a step runs its command streaming")
        }

        async fn run_streaming(
            &self,
            _spec: &ExecSpec,
            controls: ExecControls,
        ) -> sandbox_driver::Result<ExecStreamingResult> {
            let sink = controls.sink.expect("the step supplies a sink");
            sink(OutputStream::Stdout, b"first\n".to_vec()).await?;
            sink(OutputStream::Stderr, b"warned\n".to_vec()).await?;
            let mut result = completed_command();
            result.result.exit_code = Some(2);
            result.output_loss = self.loss;
            // The provider cannot tell which stream lost bytes, so a loss
            // marks both captures, as the protocol requires.
            if self.loss.is_lossy() {
                result.stdout_capture.truncated = true;
                result.stderr_capture.truncated = true;
            }
            Ok(result)
        }
    }

    struct ScriptedSandbox {
        id:   SandboxId,
        exec: ScriptedExec,
    }

    #[async_trait]
    impl Sandbox for ScriptedSandbox {
        fn id(&self) -> &SandboxId {
            &self.id
        }

        fn capabilities(&self) -> &Capabilities {
            static CAPABILITIES: OnceLock<Capabilities> = OnceLock::new();
            CAPABILITIES.get_or_init(|| Capabilities::minimal(Isolation::Container))
        }

        async fn describe(&self) -> sandbox_driver::Result<SandboxStatus> {
            panic!("a step does not describe its sandbox")
        }

        fn working_directory(&self) -> &str {
            crate::CONTAINER_WORKSPACE
        }

        async fn platform_info(&self) -> sandbox_driver::Result<PlatformInfo> {
            panic!("a step does not probe the platform")
        }

        async fn start(&self) -> sandbox_driver::Result<()> {
            panic!("a step does not start its sandbox")
        }

        async fn stop(&self) -> sandbox_driver::Result<()> {
            panic!("a step does not stop its sandbox")
        }

        async fn delete(&self) -> sandbox_driver::Result<()> {
            panic!("a step does not delete its sandbox")
        }

        fn exec(&self) -> &dyn Exec {
            &self.exec
        }

        fn fs(&self) -> &dyn Filesystem {
            panic!("a step's spawn does not touch the filesystem")
        }
    }

    /// Runs the scripted command through the step's spawn path; returns
    /// its stdout lines, its stderr lines in order, and its exit status.
    async fn run_scripted(loss: OutputLoss) -> (Vec<String>, Vec<String>, ExitStatus) {
        let sandbox: Arc<dyn Sandbox> = Arc::new(ScriptedSandbox {
            id:   SandboxId::try_new("scripted").expect("the test sandbox id is valid"),
            exec: ScriptedExec { loss },
        });
        let admission = RunGate::default()
            .admit("exec")
            .expect("an open gate admits");
        let mut process = spawn_streamed(
            sandbox,
            Job::Exec(ExecSpec::new("true")),
            false,
            OutputMode::Lines,
            admission,
        );
        let mut lines = process.lines().expect("a lines stream");
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        while let Some(line) = lines.recv().await {
            match line.stream {
                ir::LogStream::Stdout => stdout.push(line.line),
                ir::LogStream::Stderr => stderr.push(line.line),
            }
        }
        let status = process.wait().await.expect("the command's status");
        (stdout, stderr, status)
    }

    #[tokio::test]
    async fn a_counted_loss_is_one_stderr_line_after_the_output() {
        let (stdout, stderr, status) = run_scripted(counted_loss()).await;
        assert_eq!(status.code, Some(2));
        assert_eq!(stdout, vec!["first".to_owned()]);
        assert_eq!(stderr, vec![
            "warned".to_owned(),
            "[sandbox] 2 output frame(s), 300 bytes dropped by the provider".to_owned(),
        ]);
    }

    #[tokio::test]
    async fn a_lossless_result_passes_the_output_through_unchanged() {
        let (stdout, stderr, status) = run_scripted(OutputLoss::default()).await;
        assert_eq!(status.code, Some(2));
        assert_eq!(stdout, vec!["first".to_owned()]);
        assert_eq!(stderr, vec!["warned".to_owned()]);
    }
}
