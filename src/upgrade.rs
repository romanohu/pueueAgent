use std::{
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    cli::UpgradeArgs,
    db::{AgentRunRepository, Db, ProjectRepository},
    output::bounded_redacted_text,
    service::{ServiceControl, ServiceStatus},
    AppError,
};

pub const SOURCE_ROOT_ENV: &str = "PUEUE_AGENT_SOURCE_ROOT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeOptions {
    pub source: Option<PathBuf>,
    pub json: bool,
    pub branch: String,
    pub remote: String,
    pub release_binary: PathBuf,
    pub pueue_binary: PathBuf,
    pub pueue_config: Option<PathBuf>,
}

impl UpgradeOptions {
    pub fn from_args(args: UpgradeArgs) -> Self {
        Self {
            source: args.source,
            json: args.json,
            branch: "main".to_owned(),
            remote: "origin".to_owned(),
            release_binary: std::env::current_exe()
                .unwrap_or_else(|_| PathBuf::from("pueue-agent")),
            pueue_binary: std::env::var_os("PUEUE_BINARY")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("pueue")),
            pueue_config: std::env::var_os("PUEUE_CONFIG").map(PathBuf::from),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeReport {
    pub source: PathBuf,
    pub checkout: CheckoutState,
    pub old_revision: String,
    pub new_revision: String,
    pub tests: UpgradeStep,
    pub build: UpgradeStep,
    pub install: UpgradeStep,
    pub restart: UpgradeStep,
    pub health: UpgradeStep,
    pub rollback: UpgradeRollback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UpgradeStep {
    pub attempted: bool,
    pub succeeded: bool,
}

impl UpgradeStep {
    const fn succeeded() -> Self {
        Self {
            attempted: true,
            succeeded: true,
        }
    }

    const fn attempted() -> Self {
        Self {
            attempted: true,
            succeeded: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UpgradeRollback {
    #[default]
    NotRequired,
    Succeeded,
    Failed,
}

#[derive(Debug)]
pub struct UpgradeFailure {
    error: AppError,
    report: Option<UpgradeReport>,
}

impl UpgradeFailure {
    fn with_report(report: UpgradeReport, error: AppError) -> Self {
        Self {
            error,
            report: Some(report),
        }
    }

    pub fn report(&self) -> Option<&UpgradeReport> {
        self.report.as_ref()
    }
}

impl From<AppError> for UpgradeFailure {
    fn from(error: AppError) -> Self {
        Self {
            error,
            report: None,
        }
    }
}

impl std::fmt::Display for UpgradeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&bounded_redacted_text(&self.error.to_string()))
    }
}

impl std::error::Error for UpgradeFailure {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutState {
    pub branch: String,
    pub upstream: String,
    pub head: String,
    pub upstream_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub trait GitCommandRunner {
    fn run(&self, source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeCommandOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl UpgradeCommandOutput {
    pub fn success() -> Self {
        Self {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    pub fn success_with_stdout(stdout: impl AsRef<str>) -> Self {
        Self {
            success: true,
            stdout: sanitize_upgrade_stdout(stdout.as_ref()),
            stderr: String::new(),
        }
    }

    pub fn failure(stderr: impl AsRef<str>) -> Self {
        Self {
            success: false,
            stdout: String::new(),
            stderr: bounded_redacted_text(stderr.as_ref()),
        }
    }
}

pub trait UpgradeCommandRunner: GitCommandRunner {
    fn run_command(
        &self,
        working_directory: &Path,
        program: &OsStr,
        args: &[OsString],
    ) -> Result<UpgradeCommandOutput, AppError>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessGitCommandRunner;

impl GitCommandRunner for ProcessGitCommandRunner {
    fn run(&self, source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError> {
        let output = Command::new("git")
            .current_dir(source)
            .args(args)
            .output()
            .map_err(|source| AppError::Io {
                operation: "run git for upgrade",
                source,
            })?;

        let stdout = if args == ["status", "--porcelain"] {
            if output.stdout.is_empty() {
                String::new()
            } else {
                "changes present".to_owned()
            }
        } else {
            bounded_text(&String::from_utf8_lossy(&output.stdout))
        };
        Ok(GitCommandOutput {
            success: output.status.success(),
            stdout,
            stderr: bounded_redacted_text(&String::from_utf8_lossy(&output.stderr)),
        })
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessUpgradeCommandRunner;

impl GitCommandRunner for ProcessUpgradeCommandRunner {
    fn run(&self, source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError> {
        ProcessGitCommandRunner.run(source, args)
    }
}

impl UpgradeCommandRunner for ProcessUpgradeCommandRunner {
    fn run_command(
        &self,
        working_directory: &Path,
        program: &OsStr,
        args: &[OsString],
    ) -> Result<UpgradeCommandOutput, AppError> {
        let output = Command::new(program)
            .current_dir(working_directory)
            .args(args)
            .output()
            .map_err(|source| AppError::Io {
                operation: "run upgrade command",
                source,
            })?;
        let stdout = sanitize_upgrade_stdout(&String::from_utf8_lossy(&output.stdout));
        let stderr = bounded_redacted_text(&String::from_utf8_lossy(&output.stderr));
        Ok(UpgradeCommandOutput {
            success: output.status.success(),
            stdout,
            stderr,
        })
    }
}

pub fn resolve_source_root(
    explicit: Option<&Path>,
    current_exe: &Path,
    env_source: Option<&Path>,
) -> Result<PathBuf, AppError> {
    if let Some(source) = explicit {
        return validate_source_root(source);
    }

    if let Some(source) = source_root_from_release_binary(current_exe) {
        return validate_source_root(&source);
    }

    if let Some(source) = env_source {
        return validate_source_root(source);
    }

    Err(AppError::Message {
        message: format!(
            "upgrade source was not found; pass --source, run from target/release/pueue-agent, or set {SOURCE_ROOT_ENV}"
        ),
    })
}

pub fn validate_checkout(
    source: &Path,
    branch: &str,
    remote: &str,
) -> Result<CheckoutState, AppError> {
    validate_checkout_with(source, branch, remote, &ProcessGitCommandRunner)
}

pub fn validate_checkout_with<R: GitCommandRunner>(
    source: &Path,
    branch: &str,
    remote: &str,
    runner: &R,
) -> Result<CheckoutState, AppError> {
    let source = validate_source_root(source)?;
    let expected_upstream = format!("{remote}/{branch}");

    let status = run_git(runner, &source, &["status", "--porcelain"])?;
    if !status.stdout.trim().is_empty() {
        return Err(AppError::Message {
            message: "upgrade source checkout must be clean".to_owned(),
        });
    }

    let current_branch = git_stdout(runner, &source, &["branch", "--show-current"])?;
    if current_branch != branch {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout must be on branch `{branch}`, found `{}`",
                bounded_redacted_text(&current_branch)
            ),
        });
    }

    let upstream_output = runner.run(
        &source,
        &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
    )
    .map_err(|error| git_runner_error(&["rev-parse", "--abbrev-ref"], error))?;
    if !upstream_output.success {
        return Err(AppError::Message {
            message: format!("upgrade source checkout must track `{expected_upstream}`"),
        });
    }
    let upstream = bounded_text(upstream_output.stdout.trim());
    if upstream != expected_upstream {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout must track `{expected_upstream}`, found `{}`",
                bounded_redacted_text(&upstream)
            ),
        });
    }

    let head = git_stdout(runner, &source, &["rev-parse", "HEAD"])?;
    let upstream_head = git_stdout(runner, &source, &["rev-parse", &expected_upstream])?;
    let ancestry = runner.run(
        &source,
        &["merge-base", "--is-ancestor", "HEAD", &expected_upstream],
    )
    .map_err(|error| git_runner_error(&["merge-base", "--is-ancestor"], error))?;
    if !ancestry.success {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout has diverged from `{expected_upstream}` and cannot fast-forward"
            ),
        });
    }

    Ok(CheckoutState {
        branch: current_branch,
        upstream,
        head,
        upstream_head,
    })
}

pub struct UpgradeRunner<'a, S, R> {
    options: UpgradeOptions,
    db: &'a Db,
    service: &'a S,
    commands: &'a R,
}

impl<'a, S, R> UpgradeRunner<'a, S, R>
where
    S: ServiceControl,
    R: UpgradeCommandRunner,
{
    pub fn new(options: UpgradeOptions, db: &'a Db, service: &'a S, commands: &'a R) -> Self {
        Self {
            options,
            db,
            service,
            commands,
        }
    }

    pub async fn run(&self) -> Result<UpgradeReport, UpgradeFailure> {
        let state_dir = self.state_dir()?;
        let _lock = UpgradeLock::acquire(&state_dir)?;
        self.reject_active_agent_runs()?;
        let source = self
            .options
            .source
            .as_deref()
            .ok_or_else(|| AppError::Message {
                message: "upgrade source was not configured".to_owned(),
            })?;
        let source = validate_source_root(source)?;

        let before_fetch = validate_checkout_with(
            &source,
            &self.options.branch,
            &self.options.remote,
            self.commands,
        )?;
        // The lock serializes upgrades. These two checks close the observable windows before
        // source mutation and before binary replacement without stopping Pueue experiment tasks.
        self.reject_active_agent_runs()?;
        let upstream = format!("{}/{}", self.options.remote, self.options.branch);
        run_git(
            self.commands,
            &source,
            &["fetch", &self.options.remote, &self.options.branch],
        )?;
        let checkout = validate_checkout_with(
            &source,
            &self.options.branch,
            &self.options.remote,
            self.commands,
        )?;

        let mut report = UpgradeReport {
            source: source.clone(),
            checkout: checkout.clone(),
            old_revision: before_fetch.head,
            new_revision: checkout.upstream_head.clone(),
            tests: UpgradeStep::default(),
            build: UpgradeStep::default(),
            install: UpgradeStep::default(),
            restart: UpgradeStep::default(),
            health: UpgradeStep::default(),
            rollback: UpgradeRollback::NotRequired,
        };
        let retry_marker_path = state_dir.join("upgrade.pending");
        let retry_marker = read_retry_marker(&retry_marker_path)
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        let needs_fast_forward = checkout.head != checkout.upstream_head;
        let retry_pending_revision = retry_marker
            .as_deref()
            .is_some_and(|revision| revision == checkout.head);

        if !needs_fast_forward && !retry_pending_revision {
            report.new_revision = checkout.head;
            return Ok(report);
        }

        if needs_fast_forward {
            write_retry_marker(&retry_marker_path, &checkout.upstream_head)
                .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
            run_git(self.commands, &source, &["merge", "--ff-only", &upstream])
                .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
            report.checkout.head = checkout.upstream_head.clone();
        }

        let target_dir = TemporaryDirectory::create(&state_dir, "upgrade-target")
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        report.tests = UpgradeStep::attempted();
        let test_args = vec![
            OsString::from("test"),
            OsString::from("--locked"),
            OsString::from("--all-targets"),
            OsString::from("--target-dir"),
            target_dir.path().as_os_str().to_os_string(),
        ];
        self.run_checked_command(&source, OsStr::new("cargo"), &test_args, "cargo test")
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        report.tests = UpgradeStep::succeeded();

        report.build = UpgradeStep::attempted();
        let build_args = vec![
            OsString::from("build"),
            OsString::from("--locked"),
            OsString::from("--release"),
            OsString::from("--target-dir"),
            target_dir.path().as_os_str().to_os_string(),
        ];
        self.run_checked_command(&source, OsStr::new("cargo"), &build_args, "cargo build")
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        report.build = UpgradeStep::succeeded();

        let install_target = resolve_install_target(&self.options.release_binary)
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        let binary_name = install_target
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| AppError::Message {
                message: "upgrade release binary path has no file name".to_owned(),
            })
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        let built_binary = target_dir.path().join("release").join(binary_name);
        let install_parent = install_target
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let install_candidate = temporary_path(&install_parent, "upgrade-candidate")
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        let backup = temporary_path(&state_dir, "upgrade-backup")
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;

        report.install = UpgradeStep::attempted();
        if let Err(error) = copy_file(
            &built_binary,
            &install_candidate,
            "copy built upgrade candidate to install directory",
        ) {
            return Err(UpgradeFailure::with_report(report, error));
        }
        if let Err(error) = copy_file(
            &install_target,
            &backup,
            "copy installed binary to upgrade backup",
        ) {
            remove_file_if_present(&install_candidate);
            return Err(UpgradeFailure::with_report(report, error));
        }
        if let Err(error) = self.reject_active_agent_runs() {
            remove_file_if_present(&install_candidate);
            remove_file_if_present(&backup);
            return Err(UpgradeFailure::with_report(report, error));
        }
        if let Err(error) = fs::rename(&install_candidate, &install_target) {
            remove_file_if_present(&install_candidate);
            remove_file_if_present(&backup);
            return Err(UpgradeFailure::with_report(
                report,
                AppError::Io {
                    operation: "atomically install upgrade candidate",
                    source: error,
                },
            ));
        }
        report.install = UpgradeStep::succeeded();
        report.restart = UpgradeStep::attempted();

        if let Err(error) = self.service.restart() {
            return Err(self.rollback_after_post_install_failure(
                &backup,
                &install_target,
                report,
                error,
            ));
        }
        report.restart = UpgradeStep::succeeded();

        report.health = UpgradeStep::attempted();
        if let Err(error) = self.check_health(&source) {
            return Err(self.rollback_after_post_install_failure(
                &backup,
                &install_target,
                report,
                error,
            ));
        }
        report.health = UpgradeStep::succeeded();
        remove_file_if_present(&backup);
        remove_retry_marker(&retry_marker_path)
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
        Ok(report)
    }

    fn reject_active_agent_runs(&self) -> Result<(), AppError> {
        let projects = ProjectRepository::new(self.db).list_enabled()?;
        let agent_runs = AgentRunRepository::new(self.db);
        for project in projects {
            if let Some(run) = agent_runs.find_active_by_project(&project.project_id)? {
                return Err(AppError::Message {
                    message: format!(
                        "upgrade refused while active agent run {} exists for enabled project `{}`",
                        run.run_id, project.project_id
                    ),
                });
            }
        }
        Ok(())
    }

    fn state_dir(&self) -> Result<PathBuf, AppError> {
        self.db
            .path()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .ok_or_else(|| AppError::Message {
                message: "upgrade database path has no state directory".to_owned(),
            })
    }

    fn run_checked_command(
        &self,
        working_directory: &Path,
        program: &OsStr,
        args: &[OsString],
        operation: &'static str,
    ) -> Result<UpgradeCommandOutput, AppError> {
        let output = self
            .commands
            .run_command(working_directory, program, args)
            .map_err(|error| AppError::Message {
                message: format!(
                    "upgrade {operation} failed: {}",
                    bounded_redacted_text(&error.to_string())
                ),
            })?;
        if output.success {
            Ok(output)
        } else {
            Err(AppError::Message {
                message: format!(
                    "upgrade {operation} failed: {}",
                    bounded_redacted_text(output.stderr.trim())
                ),
            })
        }
    }

    fn check_health(&self, source: &Path) -> Result<(), AppError> {
        if self.service.status()? != ServiceStatus::Running {
            return Err(AppError::Message {
                message: "upgrade service health check failed: service is not running".to_owned(),
            });
        }
        self.db.connect()?;

        let mut args = Vec::new();
        if let Some(config) = &self.options.pueue_config {
            args.push(OsString::from("--config"));
            args.push(config.as_os_str().to_os_string());
        }
        args.push(OsString::from("status"));
        args.push(OsString::from("--json"));
        let pueue_status = self.run_checked_command(
            source,
            self.options.pueue_binary.as_os_str(),
            &args,
            "Pueue health check",
        )?;
        let status = serde_json::from_str::<serde_json::Value>(&pueue_status.stdout).map_err(
            |_| AppError::Message {
                message: "upgrade Pueue health check returned invalid status JSON".to_owned(),
            },
        )?;
        if status
            .as_object()
            .and_then(|object| object.get("tasks"))
            .is_some_and(serde_json::Value::is_object)
        {
            Ok(())
        } else {
            Err(AppError::Message {
                message: "upgrade Pueue health check returned an invalid status schema".to_owned(),
            })
        }
    }

    fn rollback_after_post_install_failure(
        &self,
        backup: &Path,
        install_target: &Path,
        mut report: UpgradeReport,
        failure: AppError,
    ) -> UpgradeFailure {
        let restore = self.restore_backup(backup, install_target);
        let restart = self.service.restart();
        let rollback_health = if restart.is_ok() {
            self.check_service_health()
        } else {
            Ok(())
        };
        match (restore, restart, rollback_health) {
            (Ok(()), Ok(()), Ok(())) => {
                remove_file_if_present(backup);
                report.rollback = UpgradeRollback::Succeeded;
                UpgradeFailure::with_report(
                    report,
                    AppError::Message {
                        message: format!(
                            "upgrade failed after installation: {}; rollback succeeded",
                            bounded_redacted_text(&failure.to_string())
                        ),
                    },
                )
            }
            (restore, restart, rollback_health) => {
                report.rollback = UpgradeRollback::Failed;
                let details = [
                    restore.err().map(|error| format!("restore: {error}")),
                    restart.err().map(|error| format!("service restart: {error}")),
                    rollback_health
                        .err()
                        .map(|error| format!("service health: {error}")),
                ]
                .into_iter()
                .flatten()
                .map(|detail| bounded_redacted_text(&detail))
                .collect::<Vec<_>>()
                .join("; ");
                UpgradeFailure::with_report(
                    report,
                    AppError::Message {
                        message: format!(
                            "upgrade failed after installation: {}; rollback failed: {details}",
                            bounded_redacted_text(&failure.to_string())
                        ),
                    },
                )
            }
        }
    }

    fn restore_backup(&self, backup: &Path, install_target: &Path) -> Result<(), AppError> {
        let install_parent = install_target
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let restore_candidate = temporary_path(&install_parent, "upgrade-rollback")?;
        if let Err(error) = copy_file(
            backup,
            &restore_candidate,
            "copy upgrade backup for rollback",
        ) {
            return Err(error);
        }
        if let Err(error) = fs::rename(&restore_candidate, install_target) {
            remove_file_if_present(&restore_candidate);
            return Err(AppError::Io {
                operation: "atomically restore upgrade backup",
                source: error,
            });
        }
        Ok(())
    }

    fn check_service_health(&self) -> Result<(), AppError> {
        if self.service.status()? == ServiceStatus::Running {
            Ok(())
        } else {
            Err(AppError::Message {
                message: "upgrade rollback service health check failed: service is not running"
                    .to_owned(),
            })
        }
    }
}

struct TemporaryDirectory {
    path: PathBuf,
}

impl TemporaryDirectory {
    fn create(parent: &Path, prefix: &str) -> Result<Self, AppError> {
        fs::create_dir_all(parent).map_err(|source| AppError::Io {
            operation: "create upgrade temporary directory parent",
            source,
        })?;
        for _ in 0..32 {
            let path = temporary_path(parent, prefix)?;
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(source) => {
                    return Err(AppError::Io {
                        operation: "create upgrade temporary target directory",
                        source,
                    })
                }
            }
        }
        Err(AppError::Message {
            message: "could not allocate an upgrade temporary target directory".to_owned(),
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn resolve_install_target(release_binary: &Path) -> Result<PathBuf, AppError> {
    let metadata = fs::symlink_metadata(release_binary).map_err(|source| AppError::Io {
        operation: "inspect installed upgrade binary",
        source,
    })?;
    if metadata.file_type().is_symlink() {
        release_binary.canonicalize().map_err(|source| AppError::Io {
            operation: "resolve installed upgrade binary symlink",
            source,
        })
    } else {
        Ok(release_binary.to_path_buf())
    }
}

fn read_retry_marker(path: &Path) -> Result<Option<String>, AppError> {
    match fs::read_to_string(path) {
        Ok(revision) => {
            let revision = revision.trim();
            if revision.is_empty() {
                Err(AppError::Message {
                    message: "upgrade retry marker is invalid".to_owned(),
                })
            } else {
                Ok(Some(revision.to_owned()))
            }
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(AppError::Io {
            operation: "read upgrade retry marker",
            source,
        }),
    }
}

fn write_retry_marker(path: &Path, revision: &str) -> Result<(), AppError> {
    let parent = path.parent().ok_or_else(|| AppError::Message {
        message: "upgrade retry marker has no parent directory".to_owned(),
    })?;
    let candidate = temporary_path(parent, "upgrade-pending")?;
    if let Err(source) = fs::write(&candidate, revision) {
        remove_file_if_present(&candidate);
        return Err(AppError::Io {
            operation: "write upgrade retry marker",
            source,
        });
    }
    if let Err(source) = fs::rename(&candidate, path) {
        remove_file_if_present(&candidate);
        return Err(AppError::Io {
            operation: "atomically persist upgrade retry marker",
            source,
        });
    }
    Ok(())
}

fn remove_retry_marker(path: &Path) -> Result<(), AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(AppError::Io {
            operation: "clear upgrade retry marker",
            source,
        }),
    }
}

struct UpgradeLock {
    path: PathBuf,
    owner_path: PathBuf,
}

impl UpgradeLock {
    fn acquire(state_dir: &Path) -> Result<Self, AppError> {
        fs::create_dir_all(state_dir).map_err(|source| AppError::Io {
            operation: "create upgrade state directory",
            source,
        })?;
        let _acquisition_guard = UpgradeLockAcquisitionGuard::acquire(state_dir)?;
        let path = state_dir.join("upgrade.lock");
        let owner_path = path.join("owner");
        for _ in 0..32 {
            match fs::create_dir(&path) {
                Ok(()) => {
                    if let Err(source) = fs::write(&owner_path, std::process::id().to_string()) {
                        remove_file_if_present(&owner_path);
                        let _ = fs::remove_dir(&path);
                        return Err(AppError::Io {
                            operation: "write upgrade lock PID",
                            source,
                        });
                    }
                    return Ok(Self { path, owner_path });
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(&path).map_err(|source| AppError::Io {
                        operation: "inspect upgrade lock",
                        source,
                    })?;
                    if !metadata.is_dir() {
                        return Err(AppError::Message {
                            message: "upgrade lock path is not a directory".to_owned(),
                        });
                    }
                    if let Some(pid) = read_live_lock_owner(&owner_path)? {
                        return Err(AppError::Message {
                            message: format!("upgrade lock is held by live PID {pid}"),
                        });
                    }
                    reclaim_stale_lock(state_dir, &path)?;
                }
                Err(source) => {
                    return Err(AppError::Io {
                        operation: "create upgrade lock",
                        source,
                    })
                }
            }
        }
        Err(AppError::Message {
            message: "could not acquire upgrade lock".to_owned(),
        })
    }
}

impl Drop for UpgradeLock {
    fn drop(&mut self) {
        remove_file_if_present(&self.owner_path);
        let _ = fs::remove_dir(&self.path);
    }
}

fn read_live_lock_owner(owner_path: &Path) -> Result<Option<u32>, AppError> {
    let contents = match fs::read_to_string(owner_path) {
        Ok(contents) => contents,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(AppError::Io {
                operation: "read upgrade lock PID",
                source,
            })
        }
    };
    let Ok(pid) = contents.trim().parse::<u32>() else {
        return Ok(None);
    };
    Ok(process_is_alive(pid).then_some(pid))
}

fn reclaim_stale_lock(state_dir: &Path, lock_path: &Path) -> Result<(), AppError> {
    let reclaimed = temporary_path(state_dir, "upgrade-lock-reclaimed")?;
    match fs::rename(lock_path, &reclaimed) {
        Ok(()) => {
            remove_file_if_present(&reclaimed.join("owner"));
            if let Err(source) = fs::remove_dir(&reclaimed) {
                if source.kind() != std::io::ErrorKind::DirectoryNotEmpty {
                    return Err(AppError::Io {
                        operation: "remove reclaimed upgrade lock directory",
                        source,
                    });
                }
            }
            Ok(())
        }
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(AppError::Io {
            operation: "atomically reclaim stale upgrade lock",
            source,
        }),
    }
}

struct UpgradeLockAcquisitionGuard {
    #[cfg(unix)]
    file: File,
}

impl UpgradeLockAcquisitionGuard {
    fn acquire(state_dir: &Path) -> Result<Self, AppError> {
        #[cfg(unix)]
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(state_dir.join("upgrade.lock.guard"))
                .map_err(|source| AppError::Io {
                    operation: "open upgrade lock acquisition guard",
                    source,
                })?;
            lock_file(&file)?;
            Ok(Self { file })
        }
        #[cfg(not(unix))]
        {
            let _ = state_dir;
            Ok(Self {})
        }
    }
}

#[cfg(unix)]
impl Drop for UpgradeLockAcquisitionGuard {
    fn drop(&mut self) {
        unlock_file(&self.file);
    }
}

#[cfg(not(unix))]
impl Drop for UpgradeLockAcquisitionGuard {
    fn drop(&mut self) {}
}

#[cfg(unix)]
fn lock_file(file: &File) -> Result<(), AppError> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    const LOCK_EX: std::os::raw::c_int = 2;
    if unsafe { flock(file.as_raw_fd(), LOCK_EX) } == 0 {
        Ok(())
    } else {
        Err(AppError::Io {
            operation: "acquire upgrade lock acquisition guard",
            source: std::io::Error::last_os_error(),
        })
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    const LOCK_UN: std::os::raw::c_int = 8;
    let _ = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
}

static TEMPORARY_PATH_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_UPGRADE_COMMAND_OUTPUT_BYTES: usize = 240;

fn bounded_text(value: &str) -> String {
    if value.len() <= MAX_UPGRADE_COMMAND_OUTPUT_BYTES {
        return value.to_owned();
    }

    let mut prefix = String::new();
    for character in value.chars() {
        if prefix.len() + character.len_utf8() > MAX_UPGRADE_COMMAND_OUTPUT_BYTES - 3 {
            break;
        }
        prefix.push(character);
    }
    format!("{prefix}...")
}

fn sanitize_upgrade_stdout(value: &str) -> String {
    let Ok(status) = serde_json::from_str::<serde_json::Value>(value) else {
        return bounded_redacted_text(value);
    };
    if status
        .as_object()
        .and_then(|object| object.get("tasks"))
        .is_some_and(serde_json::Value::is_object)
    {
        return r#"{"tasks":{}}"#.to_owned();
    }
    bounded_redacted_text(value)
}

fn temporary_path(parent: &Path, prefix: &str) -> Result<PathBuf, AppError> {
    for _ in 0..32 {
        let sequence = TEMPORARY_PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(".{prefix}-{}-{sequence}", std::process::id()));
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(AppError::Message {
        message: "could not allocate an upgrade temporary path".to_owned(),
    })
}

fn copy_file(from: &Path, to: &Path, operation: &'static str) -> Result<(), AppError> {
    fs::copy(from, to)
        .map(|_| ())
        .map_err(|source| AppError::Io { operation, source })
}

fn remove_file_if_present(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {}
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    use std::os::raw::c_int;

    if pid == 0 || pid > c_int::MAX as u32 {
        return false;
    }
    unsafe extern "C" {
        fn kill(pid: c_int, signal: c_int) -> c_int;
    }
    if unsafe { kill(pid as c_int, 0) } == 0 {
        true
    } else {
        std::io::Error::last_os_error().kind() != std::io::ErrorKind::NotFound
    }
}

#[cfg(not(unix))]
fn process_is_alive(pid: u32) -> bool {
    pid == std::process::id()
}

fn source_root_from_release_binary(current_exe: &Path) -> Option<PathBuf> {
    let executable = current_exe.canonicalize().ok()?;
    if executable.file_name()? != OsStr::new("pueue-agent") {
        return None;
    }

    let release = executable.parent()?;
    if release.file_name()? != OsStr::new("release") {
        return None;
    }

    let target = release.parent()?;
    if target.file_name()? != OsStr::new("target") {
        return None;
    }

    target.parent().map(Path::to_path_buf)
}

fn validate_source_root(source: &Path) -> Result<PathBuf, AppError> {
    let source = source.canonicalize().map_err(|source_error| AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: {source_error}",
            source.display()
        ),
    })?;

    if !source.join(".git").exists() {
        return Err(invalid_source_root(&source));
    }

    let manifest_path = source.join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path).map_err(|source_error| {
        AppError::Message {
            message: format!(
                "invalid upgrade source `{}`: {source_error}",
                source.display()
            ),
        }
    })?;
    let manifest = manifest.parse::<toml::Value>().map_err(|source_error| AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: {source_error}",
            source.display()
        ),
    })?;

    if manifest
        .get("package")
        .and_then(toml::Value::as_table)
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        != Some("pueue-agent")
    {
        return Err(invalid_source_root(&source));
    }

    Ok(source)
}

fn invalid_source_root(source: &Path) -> AppError {
    AppError::Message {
        message: format!(
            "invalid upgrade source `{}`: expected a pueue-agent Cargo checkout",
            source.display()
        ),
    }
}

fn run_git<R: GitCommandRunner>(
    runner: &R,
    source: &Path,
    args: &[&str],
) -> Result<GitCommandOutput, AppError> {
    let output = runner.run(source, args).map_err(|error| git_runner_error(args, error))?;
    if output.success {
        Ok(output)
    } else {
        Err(AppError::Message {
            message: format!(
                "upgrade git command failed (`git {}`): {}",
                args.join(" "),
                output.stderr.trim()
            ),
        })
    }
}

fn git_runner_error(args: &[&str], error: AppError) -> AppError {
    AppError::Message {
        message: format!(
            "upgrade git command failed (`git {}`): {}",
            args.join(" "),
            bounded_redacted_text(&error.to_string())
        ),
    }
}

fn git_stdout<R: GitCommandRunner>(
    runner: &R,
    source: &Path,
    args: &[&str],
) -> Result<String, AppError> {
    Ok(bounded_text(run_git(runner, source, args)?.stdout.trim()))
}
