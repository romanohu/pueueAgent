use std::{
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use tokio::{io::AsyncWriteExt, process::Command, time::Instant};

use crate::{
    codex_session,
    config::AgentConfig,
    db::AgentRunRepository,
    interventions::InterventionReservation,
    models::{AgentContextMode, AgentRunStatus, NewAgentRun, Project},
    AppError,
};

#[derive(Debug, Clone)]
pub struct AgentRunnerConfig {
    pub log_dir_override: Option<PathBuf>,
    pub codex_home_override: Option<PathBuf>,
}

impl AgentRunnerConfig {
    pub fn production() -> Self {
        Self {
            log_dir_override: None,
            codex_home_override: None,
        }
    }

    pub fn for_tests(log_path: PathBuf) -> Self {
        Self {
            log_dir_override: log_path.parent().map(PathBuf::from),
            codex_home_override: None,
        }
    }

    pub fn with_codex_home(mut self, codex_home: PathBuf) -> Self {
        self.codex_home_override = Some(codex_home);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[cfg(unix)]
const LAUNCH_GATE_SCRIPT: &str = r#"
IFS= read -r release || exit 0
[ "$release" = x ] || exit 0
exec "$@"
"#;

#[cfg(unix)]
fn configure_launch_gate(process: &mut Command, command: &AgentCommand) {
    process
        .arg("-c")
        .arg(LAUNCH_GATE_SCRIPT)
        .arg("pueue-agent-launch-gate")
        .arg(&command.program)
        .args(&command.args);
}

pub struct AgentHandle {
    pub run_id: i64,
    pub child: tokio::process::Child,
    pub pid: i64,
    pub timeout_deadline: Instant,
    pub log_path: PathBuf,
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
        primary_event_id: i64,
        event_ids: &[i64],
        reservation: Option<&InterventionReservation>,
        prompt: &str,
        now: i64,
    ) -> Result<AgentHandle, AppError> {
        let command = self.command_for(project, config, prompt)?;
        let log_path = self.log_path(project, primary_event_id, now)?;
        let repository = AgentRunRepository::new(db);
        let run = repository.insert_with_events_and_reservation(
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
        )?;
        let mut spawned_child = None;
        #[cfg(unix)]
        let mut release_stdin: Option<tokio::process::ChildStdin> = None;
        let startup = (|| -> Result<i64, AppError> {
            ensure_agent_program_available(&command.program)?;
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
                    configure_launch_gate(&mut process, &command);
                    process.stdin(Stdio::piped());
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
                .stdout(Stdio::from(log_file))
                .stderr(Stdio::from(stderr))
                .kill_on_drop(true);
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
            }
            repository.mark_running_and_apply_interventions(
                &project.project_id,
                run.run_id,
                pid,
                now,
            )?;
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
                repository.finish_and_release_interventions(
                    &project.project_id,
                    run.run_id,
                    AgentRunStatus::Failed,
                    now,
                    None,
                    Some(&reason),
                )?;
                return Err(error);
            }
        };
        #[cfg(unix)]
        if let Some(mut release) = release_stdin.take() {
            if let Err(source) = release.write_all(b"x\n").await {
                drop(release);
                if let Some(child) = spawned_child.as_mut() {
                    process_tree::terminate_agent_process_tree(child, child.id().map(i64::from))
                        .await;
                }
                let reason = format!("release agent launch gate: {source}");
                repository.finish_and_release_interventions(
                    &project.project_id,
                    run.run_id,
                    AgentRunStatus::Failed,
                    now,
                    None,
                    Some(&reason),
                )?;
                return Err(AppError::Io {
                    operation: "release agent launch gate",
                    source,
                });
            }
        }
        let child = match spawned_child.take() {
            Some(child) => child,
            None => {
                let error = AppError::Runtime {
                    operation: "take spawned agent process",
                };
                let reason = error.to_string();
                repository.finish_and_release_interventions(
                    &project.project_id,
                    run.run_id,
                    AgentRunStatus::Failed,
                    now,
                    None,
                    Some(&reason),
                )?;
                return Err(error);
            }
        };

        Ok(AgentHandle {
            run_id: run.run_id,
            child,
            pid,
            timeout_deadline: Instant::now()
                + Duration::from_secs(u64::from(config.timeout_minutes) * 60),
            log_path,
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

impl AgentHandle {
    pub async fn poll(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<Option<AgentRunStatus>, AppError> {
        if Instant::now() >= self.timeout_deadline {
            process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
            AgentRunRepository::new(db).finish(
                self.run_id,
                AgentRunStatus::TimedOut,
                now,
                None,
                Some("agent timed out"),
            )?;
            return Ok(Some(AgentRunStatus::TimedOut));
        }

        let Some(exit) = self.child.try_wait().map_err(|source| AppError::Io {
            operation: "poll agent process",
            source,
        })?
        else {
            return Ok(None);
        };

        let code = exit.code().map(i64::from);
        let status = if exit.success() {
            AgentRunStatus::Completed
        } else {
            AgentRunStatus::Failed
        };
        AgentRunRepository::new(db).finish(self.run_id, status, now, code, None)?;
        Ok(Some(status))
    }

    pub async fn wait(mut self, db: &crate::db::Db, now: i64) -> Result<AgentRunStatus, AppError> {
        let remaining = self
            .timeout_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_secs(0));
        let status = match tokio::time::timeout(remaining, self.child.wait()).await {
            Ok(Ok(exit)) => {
                let code = exit.code().map(i64::from);
                let status = if exit.success() {
                    AgentRunStatus::Completed
                } else {
                    AgentRunStatus::Failed
                };
                AgentRunRepository::new(db).finish(self.run_id, status, now, code, None)?;
                status
            }
            Ok(Err(source)) => {
                AgentRunRepository::new(db).finish(
                    self.run_id,
                    AgentRunStatus::Failed,
                    now,
                    None,
                    Some("wait failed"),
                )?;
                return Err(AppError::Io {
                    operation: "wait for agent process",
                    source,
                });
            }
            Err(_) => {
                process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
                AgentRunRepository::new(db).finish(
                    self.run_id,
                    AgentRunStatus::TimedOut,
                    now,
                    None,
                    Some("agent timed out"),
                )?;
                AgentRunStatus::TimedOut
            }
        };
        Ok(status)
    }

    pub async fn timeout_now(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        process_tree::terminate_agent_process_tree(&mut self.child, Some(self.pid)).await;
        AgentRunRepository::new(db).finish(
            self.run_id,
            AgentRunStatus::TimedOut,
            now,
            None,
            Some("agent timed out"),
        )?;
        Ok(AgentRunStatus::TimedOut)
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

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, process::Stdio};

    use tokio::{io::AsyncWriteExt, process::Command};
    use uuid::Uuid;

    use super::{configure_launch_gate, AgentCommand};

    #[tokio::test]
    async fn launch_gate_exits_on_eof_without_executing_configured_agent() {
        let marker =
            std::env::temp_dir().join(format!("pueue-agent-gate-{}/marker", Uuid::new_v4()));
        let command = AgentCommand {
            program: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "printf executed > \"$1\"".to_owned(),
                "configured-agent".to_owned(),
                marker.display().to_string(),
            ],
        };
        let mut process = Command::new("/bin/sh");
        configure_launch_gate(&mut process, &command);
        process.stdin(Stdio::piped());
        let mut child = process.spawn().unwrap();
        drop(child.stdin.take());

        let status = child.wait().await.unwrap();

        assert!(status.success());
        assert!(!marker.exists());
    }

    #[tokio::test]
    async fn launch_gate_releases_fixed_argv_only_after_release_line() {
        let marker =
            std::env::temp_dir().join(format!("pueue-agent-gate-{}/marker", Uuid::new_v4()));
        let parent = marker.parent().unwrap();
        fs::create_dir_all(parent).unwrap();
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
        configure_launch_gate(&mut process, &command);
        process.stdin(Stdio::piped());
        let mut child = process.spawn().unwrap();
        let mut release = child.stdin.take().unwrap();
        assert!(!marker.exists());
        release.write_all(b"x\n").await.unwrap();
        drop(release);

        let status = child.wait().await.unwrap();

        assert!(status.success());
        assert_eq!(
            fs::read_to_string(&marker).unwrap(),
            "$(not-shell-expanded)"
        );
        fs::remove_file(&marker).unwrap();
        fs::remove_dir(parent).unwrap();
    }
}
