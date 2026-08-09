use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use tokio::{process::Command, time::Instant};

use crate::{
    config::AgentConfig,
    db::AgentRunRepository,
    models::{AgentContextMode, AgentRunStatus, NewAgentRun, Project},
    AppError,
};

#[derive(Debug, Clone)]
pub struct AgentRunnerConfig {
    pub log_dir_override: Option<PathBuf>,
}

impl AgentRunnerConfig {
    pub fn production() -> Self {
        Self {
            log_dir_override: None,
        }
    }

    pub fn for_tests(log_path: PathBuf) -> Self {
        Self {
            log_dir_override: log_path.parent().map(PathBuf::from),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommand {
    pub program: String,
    pub args: Vec<String>,
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
                Ok(AgentCommand {
                    program: "codex".to_owned(),
                    args: vec![
                        "exec".to_owned(),
                        "-C".to_owned(),
                        path_string(&project.root_path, "project.root_path")?,
                        "resume".to_owned(),
                        session_id.clone(),
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
    pub fn spawn(
        &self,
        db: &crate::db::Db,
        project: &Project,
        config: &AgentConfig,
        primary_event_id: i64,
        event_ids: &[i64],
        prompt: &str,
        now: i64,
    ) -> Result<AgentHandle, AppError> {
        let command = Self::command_for(project, config, prompt)?;
        let log_path = self.log_path(project, primary_event_id, now)?;
        let run = AgentRunRepository::new(db).insert(&NewAgentRun::with_context(
            &project.project_id,
            primary_event_id,
            None,
            AgentRunStatus::Starting,
            now,
            &log_path,
            config.context.clone(),
            config.context.session_id().map(str::to_owned),
            event_ids.iter().map(i64::to_string).collect(),
        ))?;
        for event_id in event_ids {
            AgentRunRepository::new(db).attach_event(run.run_id, *event_id)?;
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

        let mut process = Command::new(&command.program);
        process
            .args(&command.args)
            .current_dir(&project.root_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(stderr));
        process_tree::configure_agent_command(&mut process);

        let child = match process.spawn() {
            Ok(child) => child,
            Err(source) => {
                AgentRunRepository::new(db).finish(
                    run.run_id,
                    AgentRunStatus::Failed,
                    now,
                    None,
                    Some("spawn failed"),
                )?;
                return Err(AppError::Io {
                    operation: "spawn agent process",
                    source,
                });
            }
        };
        let pid = child.id().map(i64::from).ok_or(AppError::Runtime {
            operation: "read spawned agent PID",
        })?;
        AgentRunRepository::new(db).mark_running(run.run_id, pid)?;

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
            process_tree::terminate_agent_process_tree(&mut self.child, self.pid).await;
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
                process_tree::terminate_agent_process_tree(&mut self.child, self.pid).await;
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

    pub(super) async fn terminate_agent_process_tree(child: &mut Child, pid: i64) {
        if let Ok(pid) = c_int::try_from(pid) {
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

    pub(super) async fn terminate_agent_process_tree(child: &mut Child, _pid: i64) {
        let _ = child.kill().await;
    }
}

fn path_string(path: &std::path::Path, field: &'static str) -> Result<String, AppError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or(AppError::Configuration { field })
}
