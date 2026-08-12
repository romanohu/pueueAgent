use std::{
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    time::{timeout, Instant},
};

use crate::{
    codex_command::{CodexArgvBuilder, CodexCapabilities},
    codex_session,
    config::AgentConfig,
    db::AgentRunRepository,
    execution_policy::ResolvedProjectExecutionPolicy,
    interventions::InterventionReservation,
    models::{launch_gate_marker_path, AgentContextMode, AgentRunStatus, NewAgentRun, Project},
    retry::{EventResolution, RetryPolicy},
    upgrade::AgentStartUpgradeGuard,
    AppError,
};

#[derive(Debug, Clone)]
pub struct AgentRunnerConfig {
    pub log_dir_override: Option<PathBuf>,
    pub codex_home_override: Option<PathBuf>,
    codex_policy: Option<ResolvedProjectExecutionPolicy>,
    codex_capabilities: CodexCapabilities,
}

impl AgentRunnerConfig {
    pub fn production() -> Self {
        Self {
            log_dir_override: None,
            codex_home_override: None,
            codex_policy: None,
            codex_capabilities: CodexCapabilities::none(),
        }
    }

    pub fn for_tests(log_path: PathBuf) -> Self {
        Self {
            log_dir_override: log_path.parent().map(PathBuf::from),
            codex_home_override: None,
            codex_policy: None,
            codex_capabilities: CodexCapabilities::none(),
        }
    }

    pub fn with_codex_home(mut self, codex_home: PathBuf) -> Self {
        self.codex_home_override = Some(codex_home);
        self
    }

    /// Bind this runner to the immutable project policy resolved at startup.
    /// Codex command construction remains unavailable until both the policy
    /// and capability probe have succeeded.
    pub fn with_codex_policy(
        mut self,
        policy: ResolvedProjectExecutionPolicy,
        capabilities: CodexCapabilities,
    ) -> Self {
        self.codex_policy = Some(policy);
        self.codex_capabilities = capabilities;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSpawnStage {
    PreBinding,
    RunBoundPreMarker { run_id: i64, resolved: bool },
    PostMarker { run_id: i64, resolved: bool },
}

#[derive(Debug)]
pub struct AgentSpawnError {
    pub stage: AgentSpawnStage,
    pub source: AppError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LaunchMarkerState {
    Confirmed,
    Missing,
    Indeterminate,
}

impl std::fmt::Display for AgentSpawnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for AgentSpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(unix)]
const LAUNCH_GATE_SCRIPT: &str = r#"
log_path=$1
marker_path=$2
shift 2
IFS= read -r release || exit 0
[ "$release" = x ] || exit 0
[ "$#" -gt 0 ] || exit 127
case "$1" in
    */*) [ -x "$1" ] || exit 127 ;;
    *) command -v "$1" >/dev/null 2>&1 || exit 127 ;;
esac

"$@" >>"$log_path" 2>&1 &
child_pid=$!
marker_tmp="${marker_path}.$$"
if ! (umask 077 && : >"$marker_tmp" && mv -f "$marker_tmp" "$marker_path"); then
    rm -f "$marker_tmp"
    kill "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
    exit 1
fi
printf 'released\n'
wait "$child_pid"
exit $?
"#;

#[cfg(unix)]
fn configure_launch_gate(
    process: &mut Command,
    log_path: &std::path::Path,
    marker_path: &std::path::Path,
    command: &AgentCommand,
) {
    process
        .arg("-c")
        .arg(LAUNCH_GATE_SCRIPT)
        .arg("pueue-agent-launch-gate")
        .arg(log_path)
        .arg(marker_path)
        .arg(&command.program)
        .args(&command.args);
}

pub struct AgentHandle {
    pub project_id: String,
    pub run_id: i64,
    pub child: tokio::process::Child,
    pub pid: i64,
    pub timeout_deadline: Instant,
    pub log_path: PathBuf,
    pub retry_policy: RetryPolicy,
    terminal_outcome: Option<TerminalOutcome>,
}

#[derive(Debug, Clone)]
struct TerminalOutcome {
    status: AgentRunStatus,
    exit_code: Option<i64>,
    last_error: Option<String>,
}

pub struct AgentRunner {
    config: AgentRunnerConfig,
}

impl AgentRunner {
    pub fn new(config: AgentRunnerConfig) -> Self {
        Self { config }
    }

    pub fn command_for(
        &self,
        project: &Project,
        config: &AgentConfig,
        prompt: &str,
    ) -> Result<AgentCommand, AppError> {
        if config.program == "codex" {
            if let Some(policy) = self.config.codex_policy.as_ref() {
                let private_tmp = policy
                    .root_anchor
                    .canonical_path
                    .join(&policy.private_temp_relative_root)
                    .join("run");
                let argv = CodexArgvBuilder::new(policy.clone(), self.config.codex_capabilities)
                    .build(config, prompt, &private_tmp)
                    .map_err(AppError::from)?;
                return Ok(AgentCommand {
                    program: policy
                        .agent_anchor
                        .canonical_path
                        .to_str()
                        .ok_or(AppError::Configuration {
                            field: "agent.program",
                        })?
                        .to_owned(),
                    args: argv
                        .into_iter()
                        .map(|arg| {
                            arg.to_str()
                                .map(str::to_owned)
                                .ok_or(AppError::Configuration { field: "agent.args" })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                });
            }
        }
        match &config.context {
            AgentContextMode::Fresh => Ok(AgentCommand {
                program: config.program.clone(),
                args: config
                    .args
                    .iter()
                    .map(|arg| arg.replace("{prompt}", prompt))
                    .collect(),
            }),
            AgentContextMode::Resume { session_id } => {
                if config.program != "codex" || session_id.trim().is_empty() {
                    return Err(AppError::Configuration {
                        field: "agent.context",
                    });
                }
                let codex_home = match &self.config.codex_home_override {
                    Some(path) => path.clone(),
                    None => codex_session::home_from_environment()?,
                };
                let session_id = codex_session::verify_project_ownership(
                    &codex_home,
                    &project.root_path,
                    session_id,
                )?;
                Ok(AgentCommand {
                    program: "codex".to_owned(),
                    args: vec![
                        "exec".to_owned(),
                        "-C".to_owned(),
                        path_string(&project.root_path, "project.root_path")?,
                        "resume".to_owned(),
                        session_id,
                        prompt.to_owned(),
                    ],
                })
            }
            AgentContextMode::ResumeLatest => {
                if config.program != "codex" {
                    return Err(AppError::Configuration {
                        field: "agent.context",
                    });
                }
                Ok(AgentCommand {
                    program: "codex".to_owned(),
                    args: vec![
                        "exec".to_owned(),
                        "-C".to_owned(),
                        path_string(&project.root_path, "project.root_path")?,
                        "resume".to_owned(),
                        "--last".to_owned(),
                        prompt.to_owned(),
                    ],
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn(
        &self,
        db: &crate::db::Db,
        project: &Project,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        reservation: Option<&InterventionReservation>,
        prompt: &str,
        now: i64,
    ) -> Result<AgentHandle, AgentSpawnError> {
        let agent_start_guard = AgentStartUpgradeGuard::acquire(db).map_err(pre_binding_error)?;
        let command = self
            .command_for(project, config, prompt)
            .map_err(pre_binding_error)?;
        let log_path = self
            .log_path(project, primary_event_id, now)
            .map_err(pre_binding_error)?;
        let gate_marker_path = launch_gate_marker_path(&log_path);
        let repository = AgentRunRepository::new(db);
        let run = repository
            .insert_with_events_and_reservation(
                &NewAgentRun::with_context(
                    &project.project_id,
                    primary_event_id,
                    None,
                    AgentRunStatus::Starting,
                    now,
                    &log_path,
                    config.context.clone(),
                    config.context.session_id().map(str::to_owned),
                    event_ids.iter().map(i64::to_string).collect(),
                ),
                event_ids,
                reservation.map(|reservation| reservation.token.as_str()),
            )
            .map_err(pre_binding_error)?;
        drop(agent_start_guard);
        let mut spawned_child = None;
        #[cfg(unix)]
        let mut release_stdin: Option<tokio::process::ChildStdin> = None;
        #[cfg(unix)]
        let mut gate_stdout: Option<tokio::process::ChildStdout> = None;
        let startup = (|| -> Result<i64, AppError> {
            ensure_launch_gate_platform_supported()?;
            ensure_agent_program_available(&command.program)?;
            match fs::remove_file(&gate_marker_path) {
                Ok(()) => {}
                Err(source) if source.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(AppError::Io {
                        operation: "remove stale agent launch gate marker",
                        source,
                    });
                }
            }
            let log_file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)
                .map_err(|source| AppError::Io {
                    operation: "open agent log",
                    source,
                })?;
            let stderr = log_file.try_clone().map_err(|source| AppError::Io {
                operation: "clone agent log handle",
                source,
            })?;

            let mut process = {
                #[cfg(unix)]
                {
                    let mut process = Command::new("/bin/sh");
                    configure_launch_gate(&mut process, &log_path, &gate_marker_path, &command);
                    process.stdin(Stdio::piped());
                    process.stdout(Stdio::piped());
                    process
                }
                #[cfg(not(unix))]
                {
                    let mut process = Command::new(&command.program);
                    process.args(&command.args);
                    process.stdin(Stdio::null());
                    process
                }
            };
            process
                .current_dir(&project.root_path)
                .env("PUEUE_AGENT_RUN_ID", run.run_id.to_string())
                .env("PUEUE_AGENT_PROJECT_ID", &project.project_id)
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
            #[cfg(unix)]
            drop(log_file);
            #[cfg(not(unix))]
            process.stdout(Stdio::from(log_file));
            process_tree::configure_agent_command(&mut process);

            spawned_child = Some(process.spawn().map_err(|source| AppError::Io {
                operation: "spawn agent process",
                source,
            })?);
            let pid = spawned_child
                .as_ref()
                .and_then(tokio::process::Child::id)
                .map(i64::from)
                .ok_or(AppError::Runtime {
                    operation: "read spawned agent PID",
                })?;
            #[cfg(unix)]
            {
                release_stdin = spawned_child.as_mut().and_then(|child| child.stdin.take());
                gate_stdout = spawned_child.as_mut().and_then(|child| child.stdout.take());
            }
            repository.mark_running_and_apply_interventions(
                &project.project_id,
                run.run_id,
                pid,
                now,
            )?;
            repository.mark_gate_release_requested(&project.project_id, run.run_id)?;
            Ok(pid)
        })();
        let pid = match startup {
            Ok(pid) => pid,
            Err(error) => {
                #[cfg(unix)]
                drop(release_stdin.take());
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = error.to_string();
                return Err(resolve_pre_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    &reason,
                    retry_policy,
                    error,
                ));
            }
        };
        #[cfg(unix)]
        {
            let Some(mut release) = release_stdin.take() else {
                let error = AppError::Runtime {
                    operation: "open agent launch gate stdin",
                };
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = error.to_string();
                return Err(resolve_pre_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    &reason,
                    retry_policy,
                    error,
                ));
            };
            if let Err(source) = release.write_all(b"x\n").await {
                drop(release);
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = format!("release agent launch gate: {source}");
                return Err(resolve_pre_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    &reason,
                    retry_policy,
                    AppError::Io {
                        operation: "release agent launch gate",
                        source,
                    },
                ));
            }

            let Some(gate_stdout) = gate_stdout.take() else {
                let error = AppError::Runtime {
                    operation: "open agent launch gate acknowledgement",
                };
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = error.to_string();
                return Err(resolve_pre_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    &reason,
                    retry_policy,
                    error,
                ));
            };
            let mut acknowledgement = String::new();
            let read_result = timeout(
                Duration::from_secs(5),
                BufReader::new(gate_stdout).read_line(&mut acknowledgement),
            )
            .await;
            let acknowledged = matches!(
                &read_result,
                Ok(Ok(count)) if *count > 0 && acknowledgement == "released\n"
            );
            if !acknowledged {
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = match read_result {
                    Ok(Ok(0)) => "agent launch gate closed before acknowledgement".to_owned(),
                    Ok(Ok(_)) => "agent launch gate returned an invalid acknowledgement".to_owned(),
                    Ok(Err(source)) => format!("read agent launch gate acknowledgement: {source}"),
                    Err(_) => "timed out waiting for agent launch gate acknowledgement".to_owned(),
                };
                return Err(resolve_launch_gate_ack_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    inspect_launch_marker(&gate_marker_path),
                    &reason,
                    retry_policy,
                    AppError::Runtime {
                        operation: "confirm agent launch gate release",
                    },
                ));
            }
            if let Err(error) = repository.acknowledge_dispatch(&project.project_id, run.run_id) {
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                return Err(resolve_post_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    "post_marker_dispatch_ack",
                    error,
                ));
            }
        }
        let child = match spawned_child.take() {
            Some(child) => child,
            None => {
                let error = AppError::Runtime {
                    operation: "take spawned agent process",
                };
                let reason = error.to_string();
                return Err(resolve_post_marker_failure(
                    &repository,
                    &project.project_id,
                    run.run_id,
                    now,
                    &reason,
                    error,
                ));
            }
        };

        Ok(AgentHandle {
            project_id: project.project_id.clone(),
            run_id: run.run_id,
            child,
            pid,
            timeout_deadline: Instant::now()
                + Duration::from_secs(u64::from(config.timeout_minutes) * 60),
            log_path,
            retry_policy,
            terminal_outcome: None,
        })
    }

    fn log_path(
        &self,
        project: &Project,
        primary_event_id: i64,
        now: i64,
    ) -> Result<PathBuf, AppError> {
        let log_dir = self
            .config
            .log_dir_override
            .clone()
            .unwrap_or_else(|| project.root_path.join(".pueue-agent/logs"));
        fs::create_dir_all(&log_dir).map_err(|source| AppError::Io {
            operation: "create agent log directory",
            source,
        })?;
        Ok(log_dir.join(format!("agent-{now}-{primary_event_id}.log")))
    }
}

fn pre_binding_error(source: AppError) -> AgentSpawnError {
    AgentSpawnError {
        stage: AgentSpawnStage::PreBinding,
        source,
    }
}

fn inspect_launch_marker(path: &std::path::Path) -> LaunchMarkerState {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => LaunchMarkerState::Confirmed,
        Ok(_) => LaunchMarkerState::Indeterminate,
        Err(source) if source.kind() == io::ErrorKind::NotFound => LaunchMarkerState::Missing,
        Err(_) => LaunchMarkerState::Indeterminate,
    }
}

fn resolve_launch_gate_ack_failure(
    repository: &AgentRunRepository<'_>,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    marker_state: LaunchMarkerState,
    reason: &str,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    match marker_state {
        LaunchMarkerState::Confirmed => resolve_post_marker_failure(
            repository,
            project_id,
            run_id,
            finished_at,
            "post_marker_launch_gate_ack",
            source,
        ),
        LaunchMarkerState::Indeterminate => resolve_post_marker_failure(
            repository,
            project_id,
            run_id,
            finished_at,
            "post_marker_launch_gate_ack_indeterminate",
            source,
        ),
        LaunchMarkerState::Missing => resolve_pre_marker_failure(
            repository,
            project_id,
            run_id,
            finished_at,
            reason,
            policy,
            source,
        ),
    }
}

fn resolve_pre_marker_failure(
    repository: &AgentRunRepository<'_>,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    reason: &str,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    match repository.fail_before_gate_release_with_policy(
        project_id,
        run_id,
        finished_at,
        reason,
        policy,
    ) {
        Ok(_) => AgentSpawnError {
            stage: AgentSpawnStage::RunBoundPreMarker {
                run_id,
                resolved: true,
            },
            source,
        },
        Err(finalizer_error) => AgentSpawnError {
            stage: AgentSpawnStage::RunBoundPreMarker {
                run_id,
                resolved: false,
            },
            source: finalizer_error,
        },
    }
}

fn resolve_post_marker_failure(
    repository: &AgentRunRepository<'_>,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    reason: &str,
    source: AppError,
) -> AgentSpawnError {
    match repository.finish_after_marker_failure(project_id, run_id, finished_at, reason) {
        Ok(_) => AgentSpawnError {
            stage: AgentSpawnStage::PostMarker {
                run_id,
                resolved: true,
            },
            source,
        },
        Err(finalizer_error) => AgentSpawnError {
            stage: AgentSpawnStage::PostMarker {
                run_id,
                resolved: false,
            },
            source: finalizer_error,
        },
    }
}

impl AgentHandle {
    fn finalize_terminal_outcome(
        &self,
        db: &crate::db::Db,
        now: i64,
        outcome: &TerminalOutcome,
    ) -> Result<AgentRunStatus, AppError> {
        AgentRunRepository::new(db).finish_and_resolve_events(
            &self.project_id,
            self.run_id,
            outcome.status,
            now,
            outcome.exit_code,
            outcome.last_error.as_deref(),
            EventResolution::RetryPolicy(self.retry_policy),
        )?;
        Ok(outcome.status)
    }

    fn finalize_stored_outcome(
        &self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        let Some(outcome) = self.terminal_outcome.as_ref() else {
            return Err(AppError::Runtime {
                operation: "finalize missing agent process outcome",
            });
        };
        self.finalize_terminal_outcome(db, now, outcome)
    }

    fn store_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
        outcome: TerminalOutcome,
    ) -> Result<AgentRunStatus, AppError> {
        if self.terminal_outcome.is_none() {
            self.terminal_outcome = Some(outcome);
        }
        self.finalize_stored_outcome(db, now)
    }

    pub async fn poll(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<Option<AgentRunStatus>, AppError> {
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome(db, now).map(Some);
        }

        if Instant::now() >= self.timeout_deadline {
            process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
            return self
                .store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status: AgentRunStatus::TimedOut,
                        exit_code: None,
                        last_error: Some("agent timed out".to_owned()),
                    },
                )
                .map(Some);
        }

        let exit = match self.child.try_wait() {
            Ok(Some(exit)) => exit,
            Ok(None) => return Ok(None),
            Err(source) => {
                return self
                    .store_outcome(
                        db,
                        now,
                        TerminalOutcome {
                            status: AgentRunStatus::Failed,
                            exit_code: None,
                            last_error: Some(format!("poll agent process: {source}")),
                        },
                    )
                    .map(Some);
            }
        };

        let code = exit.code().map(i64::from);
        let status = if exit.success() {
            AgentRunStatus::Completed
        } else {
            AgentRunStatus::Failed
        };
        let last_error = (!exit.success()).then(|| {
            code.map_or_else(
                || "agent exited unsuccessfully".to_owned(),
                |code| format!("agent exited with code {code}"),
            )
        });
        self.store_outcome(
            db,
            now,
            TerminalOutcome {
                status,
                exit_code: code,
                last_error,
            },
        )
        .map(Some)
    }

    pub async fn wait(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome(db, now);
        }

        let remaining = self
            .timeout_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_secs(0));
        match tokio::time::timeout(remaining, self.child.wait()).await {
            Ok(Ok(exit)) => {
                let code = exit.code().map(i64::from);
                let status = if exit.success() {
                    AgentRunStatus::Completed
                } else {
                    AgentRunStatus::Failed
                };
                let last_error = (!exit.success()).then(|| {
                    code.map_or_else(
                        || "agent exited unsuccessfully".to_owned(),
                        |code| format!("agent exited with code {code}"),
                    )
                });
                self.store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status,
                        exit_code: code,
                        last_error,
                    },
                )
            }
            Ok(Err(source)) => {
                let result = self.store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status: AgentRunStatus::Failed,
                        exit_code: None,
                        last_error: Some(format!("wait for agent process: {source}")),
                    },
                );
                match result {
                    Ok(status) => Ok(status),
                    Err(error) => Err(error),
                }
            }
            Err(_) => {
                process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
                self.store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status: AgentRunStatus::TimedOut,
                        exit_code: None,
                        last_error: Some("agent timed out".to_owned()),
                    },
                )
            }
        }
    }

    pub async fn timeout_now(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome(db, now);
        }

        process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
        self.store_outcome(
            db,
            now,
            TerminalOutcome {
                status: AgentRunStatus::TimedOut,
                exit_code: None,
                last_error: Some("agent timed out".to_owned()),
            },
        )
    }
}

#[cfg(unix)]
mod process_tree {
    use std::{io, os::raw::c_int, time::Duration};

    use tokio::process::{Child, Command};

    const SIGTERM: c_int = 15;
    const SIGKILL: c_int = 9;
    const ESRCH: i32 = 3;

    unsafe extern "C" {
        fn setsid() -> c_int;
        fn kill(pid: c_int, sig: c_int) -> c_int;
    }

    pub(super) fn configure_agent_command(command: &mut Command) {
        unsafe {
            command.pre_exec(|| {
                let _ = setsid();
                Ok(())
            });
        }
    }

    pub(super) async fn terminate_agent_process_tree(child: &mut Child, pid: Option<i64>) {
        if let Some(Ok(pid)) = pid.map(c_int::try_from) {
            let _ = signal_process_group(pid, SIGTERM);
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(500), child.wait()).await
            {
                return;
            }

            let _ = signal_process_group(pid, SIGKILL);
            if let Ok(Ok(_)) = tokio::time::timeout(Duration::from_millis(500), child.wait()).await
            {
                return;
            }
        }

        let _ = child.kill().await;
    }

    fn signal_process_group(pid: c_int, signal: c_int) -> io::Result<()> {
        let process_group = pid.checked_neg().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "agent pid cannot form process group",
            )
        })?;
        let result = unsafe { kill(process_group, signal) };
        if result == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ESRCH) {
                Ok(())
            } else {
                Err(error)
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(not(unix))]
mod process_tree {
    use tokio::process::{Child, Command};

    pub(super) fn configure_agent_command(_command: &mut Command) {}

    pub(super) async fn terminate_agent_process_tree(child: &mut Child, _pid: Option<i64>) {
        let _ = child.kill().await;
    }
}

fn path_string(path: &std::path::Path, field: &'static str) -> Result<String, AppError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(AppError::Configuration { field })
}

fn ensure_agent_program_available(program: &str) -> Result<(), AppError> {
    let candidate = if program.contains(std::path::MAIN_SEPARATOR) {
        PathBuf::from(program)
    } else {
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .map(|directory| directory.join(program))
            .find(|path| path.is_file())
            .unwrap_or_else(|| PathBuf::from(program))
    };
    if candidate.is_file() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if candidate
                .metadata()
                .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
            {
                return Ok(());
            }
        }
        #[cfg(not(unix))]
        return Ok(());
    }
    Err(AppError::Io {
        operation: "spawn agent process",
        source: io::Error::new(
            io::ErrorKind::NotFound,
            format!("configured agent program not found: {program}"),
        ),
    })
}

#[cfg(unix)]
fn ensure_launch_gate_platform_supported() -> Result<(), AppError> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_launch_gate_platform_supported() -> Result<(), AppError> {
    Err(AppError::Runtime {
        operation: "agent launch gate requires a Unix process launcher",
    })
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, process::Stdio};

    use crate::{
        db::{AgentRunRepository, Db, EventRepository, InterventionRepository, ProjectRepository},
        interventions::InterventionStatus,
        models::{AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent, NewProject},
        retry::RetryPolicy,
        AppError,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt},
        process::Command,
    };
    use uuid::Uuid;

    use super::{
        configure_launch_gate, inspect_launch_marker, resolve_launch_gate_ack_failure,
        AgentCommand, LaunchMarkerState,
    };

    #[test]
    fn launch_marker_inspection_is_conservative_after_ack_failure() {
        let directory =
            std::env::temp_dir().join(format!("pueue-agent-marker-{}/marker", Uuid::new_v4()));
        let parent = directory.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        assert_eq!(inspect_launch_marker(&directory), LaunchMarkerState::Missing);
        fs::write(&directory, "marker").unwrap();
        assert_eq!(inspect_launch_marker(&directory), LaunchMarkerState::Confirmed);
        fs::remove_file(&directory).unwrap();
        fs::create_dir(&directory).unwrap();
        assert_eq!(
            inspect_launch_marker(&directory),
            LaunchMarkerState::Indeterminate
        );
        fs::remove_dir(&directory).unwrap();
        fs::remove_dir(parent).unwrap();
    }

    #[test]
    fn confirmed_ack_failure_dead_letters_event_and_keeps_applied_intervention() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project-a");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let marker_path = temp.path().join("confirmed-ack-failure.gate-started");
        fs::write(&marker_path, "started").unwrap();
        let marker_state = inspect_launch_marker(&marker_path);
        assert_eq!(marker_state, LaunchMarkerState::Confirmed);
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                root.join("config.toml"),
                100,
            ))
            .unwrap();
        let event_id = EventRepository::new(&db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFailed,
                "confirmed-ack-failure",
                json!({"source": "test"}),
                100,
                100,
            ))
            .unwrap()
            .event_id;
        EventRepository::new(&db)
            .claim_batch(100, 200, 1)
            .unwrap();
        let intervention = InterventionRepository::new(&db)
            .insert_pending("project-a", "retain this audit", 100)
            .unwrap();
        let reservation = InterventionRepository::new(&db)
            .reserve_pending("project-a", "confirmed-ack-token", 100, 200, 1, 128)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event_id,
                    None,
                    AgentRunStatus::Starting,
                    100,
                    "/tmp/confirmed-ack-failure.log",
                ),
                &[event_id],
                Some(&reservation.token),
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 110)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE events SET status = 'in_flight' WHERE event_id = ?1",
                [event_id],
            )
            .unwrap();

        let error = resolve_launch_gate_ack_failure(
            &AgentRunRepository::new(&db),
            "project-a",
            run.run_id,
            120,
            marker_state,
            "invalid acknowledgement",
            RetryPolicy { max_retries: 2 },
            AppError::Runtime {
                operation: "confirm agent launch gate release",
            },
        );
        assert!(matches!(
            error.stage,
            super::AgentSpawnStage::PostMarker {
                run_id,
                resolved: true
            } if run_id == run.run_id
        ));
        let state: (AgentRunStatus, EventStatus, InterventionStatus, Option<String>) = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT agent_runs.status, events.status, interventions.status,
                        events.last_error
                 FROM agent_runs
                 JOIN agent_run_events
                   ON agent_run_events.run_id = agent_runs.run_id
                  AND agent_run_events.project_id = agent_runs.project_id
                 JOIN events
                   ON events.event_id = agent_run_events.event_id
                  AND events.project_id = agent_run_events.project_id
                 JOIN interventions
                   ON interventions.agent_run_id = agent_runs.run_id
                  AND interventions.project_id = agent_runs.project_id
                 WHERE agent_runs.run_id = ?1 AND interventions.intervention_id = ?2",
                rusqlite::params![run.run_id, intervention.intervention_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(state.0, AgentRunStatus::Failed);
        assert_eq!(state.1, EventStatus::DeadLetter);
        assert_eq!(state.2, InterventionStatus::Applied);
        assert_eq!(state.3.as_deref(), Some("post_marker_launch_gate_ack"));
    }

    #[tokio::test]
    async fn launch_gate_exits_on_eof_without_executing_configured_agent() {
        let directory =
            std::env::temp_dir().join(format!("pueue-agent-gate-{}/marker", Uuid::new_v4()));
        let parent = directory.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        let log_path = parent.join("agent.log");
        let gate_marker = parent.join("agent.log.gate-started");
        let child_marker = parent.join("configured-agent-started");
        let command = AgentCommand {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "printf executed > \"$1\"".to_owned(),
                "configured-agent".to_owned(),
                child_marker.display().to_string(),
            ],
        };
        let mut process = Command::new("/bin/sh");
        configure_launch_gate(&mut process, &log_path, &gate_marker, &command);
        process.stdin(Stdio::piped());
        let mut child = process.spawn().unwrap();
        drop(child.stdin.take());

        let status = child.wait().await.unwrap();

        assert!(status.success());
        assert!(!gate_marker.exists());
        assert!(!child_marker.exists());
        fs::remove_dir_all(parent).unwrap();
    }

    #[tokio::test]
    async fn launch_gate_acknowledges_only_after_child_spawn_and_marker_commit() {
        let marker =
            std::env::temp_dir().join(format!("pueue-agent-gate-{}/marker", Uuid::new_v4()));
        let parent = marker.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        let log_path = parent.join("agent.log");
        let gate_marker = parent.join("agent.log.gate-started");
        let command = AgentCommand {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "printf '%s' \"$1\" > \"$2\"".to_owned(),
                "configured-agent".to_owned(),
                "$(not-shell-expanded)".to_owned(),
                marker.display().to_string(),
            ],
        };
        let mut process = Command::new("/bin/sh");
        configure_launch_gate(&mut process, &log_path, &gate_marker, &command);
        process.stdin(Stdio::piped());
        process.stdout(Stdio::piped());
        let mut child = process.spawn().unwrap();
        let mut release = child.stdin.take().unwrap();
        let mut acknowledgement = tokio::io::BufReader::new(child.stdout.take().unwrap());
        assert!(!marker.exists());
        release.write_all(b"x\n").await.unwrap();
        drop(release);
        let mut line = String::new();
        acknowledgement.read_line(&mut line).await.unwrap();
        assert_eq!(line, "released\n");
        assert!(gate_marker.exists());

        let status = child.wait().await.unwrap();

        assert!(status.success());
        assert_eq!(
            fs::read_to_string(&marker).unwrap(),
            "$(not-shell-expanded)"
        );
        fs::remove_file(&marker).unwrap();
        fs::remove_file(&gate_marker).unwrap();
        fs::remove_file(&log_path).unwrap();
        fs::remove_dir(parent).unwrap();
    }

    #[tokio::test]
    async fn launch_gate_exits_without_ack_for_unavailable_configured_program() {
        let marker =
            std::env::temp_dir().join(format!("pueue-agent-gate-{}/marker", Uuid::new_v4()));
        let parent = marker.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
        let log_path = parent.join("agent.log");
        let gate_marker = parent.join("agent.log.gate-started");
        let command = AgentCommand {
            program: parent.join("does-not-exist").display().to_string(),
            args: Vec::new(),
        };
        let mut process = Command::new("/bin/sh");
        configure_launch_gate(&mut process, &log_path, &gate_marker, &command);
        process.stdin(Stdio::piped());
        process.stdout(Stdio::piped());
        let mut child = process.spawn().unwrap();
        let mut release = child.stdin.take().unwrap();
        release.write_all(b"x\n").await.unwrap();
        drop(release);

        let output = child.wait_with_output().await.unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!gate_marker.exists());
        fs::remove_dir_all(parent).unwrap();
    }
}
