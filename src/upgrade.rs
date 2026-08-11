use std::{
    ffi::{OsStr, OsString},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{
    cli::UpgradeArgs,
    db::{AgentRunRepository, Db, ProjectRepository},
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
    stderr: String,
}

impl UpgradeCommandOutput {
    pub fn success() -> Self {
        Self {
            success: true,
            stderr: String::new(),
        }
    }

    pub fn failure(stderr: impl Into<String>) -> Self {
        Self {
            success: false,
            stderr: stderr.into(),
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

        Ok(GitCommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
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
        if output.status.success() {
            Ok(UpgradeCommandOutput::success())
        } else {
            Ok(UpgradeCommandOutput::failure(format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )))
        }
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
                "upgrade source checkout must be on branch `{branch}`, found `{current_branch}`"
            ),
        });
    }

    let upstream_output = runner.run(
        &source,
        &["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
    )?;
    if !upstream_output.success {
        return Err(AppError::Message {
            message: format!("upgrade source checkout must track `{expected_upstream}`"),
        });
    }
    let upstream = upstream_output.stdout.trim().to_owned();
    if upstream != expected_upstream {
        return Err(AppError::Message {
            message: format!(
                "upgrade source checkout must track `{expected_upstream}`, found `{upstream}`"
            ),
        });
    }

    let head = git_stdout(runner, &source, &["rev-parse", "HEAD"])?;
    let upstream_head = git_stdout(runner, &source, &["rev-parse", &expected_upstream])?;
    let ancestry = runner.run(
        &source,
        &["merge-base", "--is-ancestor", "HEAD", &expected_upstream],
    )?;
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

    pub async fn run(&self) -> Result<UpgradeReport, AppError> {
        self.reject_active_agent_runs()?;
        let state_dir = self.state_dir()?;
        let _lock = UpgradeLock::acquire(&state_dir)?;
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

        if checkout.head == checkout.upstream_head {
            return Ok(UpgradeReport {
                source,
                checkout: checkout.clone(),
                old_revision: before_fetch.head,
                new_revision: checkout.head,
                tests: UpgradeStep::default(),
                build: UpgradeStep::default(),
                install: UpgradeStep::default(),
                restart: UpgradeStep::default(),
                health: UpgradeStep::default(),
                rollback: UpgradeRollback::NotRequired,
            });
        }

        run_git(self.commands, &source, &["merge", "--ff-only", &upstream])?;

        self.run_checked_command(
            &source,
            OsStr::new("cargo"),
            &[OsString::from("test"), OsString::from("--all-targets")],
            "cargo test",
        )?;

        let target_dir = TemporaryDirectory::create(&state_dir, "upgrade-target")?;
        let build_args = vec![
            OsString::from("build"),
            OsString::from("--locked"),
            OsString::from("--release"),
            OsString::from("--target-dir"),
            target_dir.path().as_os_str().to_os_string(),
        ];
        self.run_checked_command(&source, OsStr::new("cargo"), &build_args, "cargo build")?;

        let binary_name = self
            .options
            .release_binary
            .file_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| AppError::Message {
                message: "upgrade release binary path has no file name".to_owned(),
            })?;
        let built_binary = target_dir.path().join("release").join(binary_name);
        let install_parent = self
            .options
            .release_binary
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let install_candidate = temporary_path(&install_parent, "upgrade-candidate")?;
        let backup = temporary_path(&state_dir, "upgrade-backup")?;

        if let Err(error) = copy_file(
            &built_binary,
            &install_candidate,
            "copy built upgrade candidate to install directory",
        ) {
            return Err(error);
        }
        if let Err(error) = copy_file(
            &self.options.release_binary,
            &backup,
            "copy installed binary to upgrade backup",
        ) {
            remove_file_if_present(&install_candidate);
            return Err(error);
        }
        if let Err(error) = fs::rename(&install_candidate, &self.options.release_binary) {
            remove_file_if_present(&install_candidate);
            remove_file_if_present(&backup);
            return Err(AppError::Io {
                operation: "atomically install upgrade candidate",
                source: error,
            });
        }

        let report = UpgradeReport {
            source: source.clone(),
            checkout: checkout.clone(),
            old_revision: before_fetch.head,
            new_revision: checkout.upstream_head,
            tests: UpgradeStep::succeeded(),
            build: UpgradeStep::succeeded(),
            install: UpgradeStep::succeeded(),
            restart: UpgradeStep::attempted(),
            health: UpgradeStep::attempted(),
            rollback: UpgradeRollback::NotRequired,
        };

        if let Err(error) = self.service.restart() {
            return Err(self.rollback_after_post_install_failure(&backup, error));
        }
        let mut report = report;
        report.restart = UpgradeStep::succeeded();

        if let Err(error) = self.check_health(&source) {
            return Err(self.rollback_after_post_install_failure(&backup, error));
        }
        report.health = UpgradeStep::succeeded();
        remove_file_if_present(&backup);
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
    ) -> Result<(), AppError> {
        let output = self
            .commands
            .run_command(working_directory, program, args)?;
        if output.success {
            Ok(())
        } else {
            Err(AppError::Message {
                message: format!(
                    "upgrade {operation} failed: {}",
                    output.stderr.trim()
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
        self.run_checked_command(
            source,
            self.options.pueue_binary.as_os_str(),
            &args,
            "Pueue health check",
        )
    }

    fn rollback_after_post_install_failure(&self, backup: &Path, failure: AppError) -> AppError {
        let restore = self.restore_backup(backup);
        let restart = self.service.restart();
        match (restore, restart) {
            (Ok(()), Ok(())) => {
                remove_file_if_present(backup);
                AppError::Message {
                    message: format!("upgrade failed after installation: {failure}; rollback succeeded"),
                }
            }
            (restore, restart) => AppError::Message {
                message: format!(
                    "upgrade failed after installation: {failure}; rollback failed: {}{}",
                    restore
                        .err()
                        .map(|error| error.to_string())
                        .unwrap_or_default(),
                    restart
                        .err()
                        .map(|error| format!(" service restart: {error}"))
                        .unwrap_or_default(),
                ),
            },
        }
    }

    fn restore_backup(&self, backup: &Path) -> Result<(), AppError> {
        let install_parent = self
            .options
            .release_binary
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
        if let Err(error) = fs::rename(&restore_candidate, &self.options.release_binary) {
            remove_file_if_present(&restore_candidate);
            return Err(AppError::Io {
                operation: "atomically restore upgrade backup",
                source: error,
            });
        }
        Ok(())
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

struct UpgradeLock {
    path: PathBuf,
}

impl UpgradeLock {
    fn acquire(state_dir: &Path) -> Result<Self, AppError> {
        fs::create_dir_all(state_dir).map_err(|source| AppError::Io {
            operation: "create upgrade state directory",
            source,
        })?;
        let path = state_dir.join("upgrade.lock");
        for _ in 0..3 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut lock) => {
                    if let Err(source) = writeln!(lock, "{}", std::process::id()) {
                        remove_file_if_present(&path);
                        return Err(AppError::Io {
                            operation: "write upgrade lock PID",
                            source,
                        });
                    }
                    return Ok(Self { path });
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    let contents = fs::read_to_string(&path).map_err(|source| AppError::Io {
                        operation: "read upgrade lock PID",
                        source,
                    })?;
                    let pid = contents.trim().parse::<u32>().map_err(|_| AppError::Message {
                        message: "upgrade lock contains an invalid PID".to_owned(),
                    })?;
                    if process_is_alive(pid) {
                        return Err(AppError::Message {
                            message: format!("upgrade lock is held by live PID {pid}"),
                        });
                    }
                    fs::remove_file(&path).map_err(|source| AppError::Io {
                        operation: "reclaim stale upgrade lock",
                        source,
                    })?;
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
        remove_file_if_present(&self.path);
    }
}

static TEMPORARY_PATH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn temporary_path(parent: &Path, prefix: &str) -> Result<PathBuf, AppError> {
    let sequence = TEMPORARY_PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let path = parent.join(format!(".{prefix}-{}-{sequence}", std::process::id()));
    if path.exists() {
        return Err(AppError::Message {
            message: "upgrade temporary path already exists".to_owned(),
        });
    }
    Ok(path)
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
    let output = runner.run(source, args)?;
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

fn git_stdout<R: GitCommandRunner>(
    runner: &R,
    source: &Path,
    args: &[&str],
) -> Result<String, AppError> {
    Ok(run_git(runner, source, args)?.stdout.trim().to_owned())
}
