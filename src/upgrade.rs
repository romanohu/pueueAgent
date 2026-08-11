use std::{
    env,
    ffi::{OsStr, OsString},
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use crate::{
    cli::UpgradeArgs,
    db::{AgentRunRepository, Db, ProjectRepository},
    output::{bounded_redacted_text, human_summary},
    service::{ServiceControl, ServiceStatus},
    AppError,
};

pub const SOURCE_ROOT_ENV: &str = "PUEUE_AGENT_SOURCE_ROOT";
const PACKAGE_BINARY_NAME: &str = "pueue-agent";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeOptions {
    pub source: Option<PathBuf>,
    pub json: bool,
    pub branch: String,
    pub remote: String,
    pub release_binary: PathBuf,
    pub pueue_binary: PathBuf,
    pub pueue_config: Option<PathBuf>,
    pub installed_revision: String,
}

impl UpgradeOptions {
    pub fn from_args(args: UpgradeArgs) -> Self {
        let env_pueue_config = env::var_os("PUEUE_CONFIG").map(PathBuf::from);
        let home = env::var_os("HOME").map(PathBuf::from);
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
            pueue_config: resolve_pueue_config(
                args.pueue_config.as_deref(),
                env_pueue_config.as_deref(),
                home.as_deref(),
            ),
            installed_revision: option_env!("PUEUE_AGENT_GIT_REVISION")
                .unwrap_or("unknown")
                .to_owned(),
        }
    }
}

pub fn resolve_pueue_config(
    explicit: Option<&Path>,
    environment: Option<&Path>,
    home: Option<&Path>,
) -> Option<PathBuf> {
    explicit
        .or(environment)
        .map(Path::to_path_buf)
        .or_else(|| home.map(|home| home.join(".config/pueue/pueue.yml")))
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

pub fn render_report(report: &UpgradeReport, json: bool) -> Result<String, AppError> {
    render_report_with_status(report, report_status(report), json)
}

pub fn render_failure_report(report: &UpgradeReport, json: bool) -> Result<String, AppError> {
    render_report_with_status(report, "failed", json)
}

fn render_report_with_status(
    report: &UpgradeReport,
    status: &str,
    json: bool,
) -> Result<String, AppError> {
    let source = bounded_redacted_text(&report.source.display().to_string());
    let old_revision = bounded_redacted_text(&report.old_revision);
    let new_revision = bounded_redacted_text(&report.new_revision);
    let noop = status == "noop";

    if json {
        return serde_json::to_string(&serde_json::json!({
            "schema_version": 1,
            "status": status,
            "noop": noop,
            "source": source,
            "revisions": {
                "old": old_revision,
                "new": new_revision,
                "head": bounded_redacted_text(&report.checkout.head),
                "upstream": bounded_redacted_text(&report.checkout.upstream_head),
            },
            "steps": {
                "tests": step_json(report.tests),
                "build": step_json(report.build),
                "install": step_json(report.install),
                "restart": step_json(report.restart),
                "health": step_json(report.health),
            },
            "rollback": rollback_label(report.rollback),
        }))
        .map_err(|source| AppError::Serialization {
            operation: "serialize upgrade report",
            source,
        });
    }

    let summary = match status {
        "noop" => "upgrade already current",
        "failed" => match report.rollback {
            UpgradeRollback::Succeeded => "upgrade failed; rollback succeeded",
            UpgradeRollback::Failed => "upgrade failed; rollback failed",
            UpgradeRollback::NotRequired => "upgrade failed",
        },
        _ => "upgrade completed",
    };
    Ok(format!(
        "pueue-agent upgrade\nstatus={status}\nnoop={noop}\nsource={source}\nold_revision={old_revision}\nnew_revision={new_revision}\ntests={}\nbuild={}\ninstall={}\nrestart={}\nhealth={}\nrollback={}\n{}",
        step_label(report.tests),
        step_label(report.build),
        step_label(report.install),
        step_label(report.restart),
        step_label(report.health),
        rollback_label(report.rollback),
        human_summary(summary),
    ))
}

fn report_status(report: &UpgradeReport) -> &'static str {
    if report.old_revision == report.new_revision
        && !report.tests.attempted
        && !report.build.attempted
        && !report.install.attempted
        && !report.restart.attempted
        && !report.health.attempted
    {
        "noop"
    } else {
        "updated"
    }
}

fn revisions_match(installed_revision: &str, checkout_head: &str) -> bool {
    if installed_revision.is_empty()
        || installed_revision == "unknown"
        || checkout_head.is_empty()
    {
        return false;
    }

    checkout_head.starts_with(installed_revision) || installed_revision.starts_with(checkout_head)
}

fn step_label(step: UpgradeStep) -> &'static str {
    if !step.attempted {
        "not_attempted"
    } else if step.succeeded {
        "ok"
    } else {
        "failed"
    }
}

fn step_json(step: UpgradeStep) -> serde_json::Value {
    serde_json::json!({
        "attempted": step.attempted,
        "succeeded": step.succeeded,
    })
}

fn rollback_label(rollback: UpgradeRollback) -> &'static str {
    match rollback {
        UpgradeRollback::NotRequired => "not_required",
        UpgradeRollback::Succeeded => "succeeded",
        UpgradeRollback::Failed => "failed",
    }
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
    stdout_limited: bool,
}

impl UpgradeCommandOutput {
    pub fn success() -> Self {
        Self {
            success: true,
            stdout: String::new(),
            stderr: String::new(),
            stdout_limited: false,
        }
    }

    pub fn success_with_stdout(stdout: impl AsRef<str>) -> Self {
        let stdout = sanitize_upgrade_stdout(stdout.as_ref());
        Self {
            success: true,
            stdout: stdout.text,
            stderr: String::new(),
            stdout_limited: stdout.limited,
        }
    }

    pub fn failure(stderr: impl AsRef<str>) -> Self {
        Self {
            success: false,
            stdout: String::new(),
            stderr: bounded_redacted_text(stderr.as_ref()),
            stdout_limited: false,
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
        let mut command = Command::new("git");
        command.current_dir(source).args(args);
        let output = run_process_bounded(&mut command, "run git for upgrade")?;

        let stdout = if args == ["status", "--porcelain"] {
            if output.stdout.bytes.is_empty() {
                String::new()
            } else {
                "changes present".to_owned()
            }
        } else {
            bounded_text(&String::from_utf8_lossy(&output.stdout.bytes))
        };
        Ok(GitCommandOutput {
            success: output.success,
            stdout,
            stderr: bounded_redacted_text(&String::from_utf8_lossy(&output.stderr.bytes)),
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
        let mut command = Command::new(program);
        command.current_dir(working_directory).args(args);
        let output = run_process_bounded(&mut command, "run upgrade command")?;
        let stdout = if output.stdout.truncated {
            SanitizedUpgradeStdout::limited()
        } else {
            sanitize_upgrade_stdout(&String::from_utf8_lossy(&output.stdout.bytes))
        };
        let stderr = bounded_redacted_text(&String::from_utf8_lossy(&output.stderr.bytes));
        Ok(UpgradeCommandOutput {
            success: output.success,
            stdout: stdout.text,
            stderr,
            stdout_limited: stdout.limited,
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

        let installed_revision_is_current =
            revisions_match(&self.options.installed_revision, &checkout.head);
        if !needs_fast_forward && !retry_pending_revision && installed_revision_is_current {
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
        let built_binary = target_dir
            .path()
            .join("release")
            .join(PACKAGE_BINARY_NAME);
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
            return Err(UpgradeFailure::with_report(
                report,
                with_cleanup_failure(error, &[&install_candidate, &backup]),
            ));
        }
        if let Err(error) = copy_file(
            &install_target,
            &backup,
            "copy installed binary to upgrade backup",
        ) {
            return Err(UpgradeFailure::with_report(
                report,
                with_cleanup_failure(error, &[&install_candidate, &backup]),
            ));
        }
        if let Err(error) = self.reject_active_agent_runs() {
            return Err(UpgradeFailure::with_report(
                report,
                with_cleanup_failure(error, &[&install_candidate, &backup]),
            ));
        }
        if let Err(error) = fs::rename(&install_candidate, &install_target) {
            return Err(UpgradeFailure::with_report(
                report,
                with_cleanup_failure(
                    AppError::Io {
                        operation: "atomically install upgrade candidate",
                        source: error,
                    },
                    &[&install_candidate, &backup],
                ),
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
        remove_file_if_present(&backup)
            .map_err(|error| UpgradeFailure::with_report(report.clone(), error))?;
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
        if pueue_status.stdout_limited {
            return Err(AppError::Message {
                message: "upgrade Pueue health check output exceeded the safe limit".to_owned(),
            });
        }
        let status = serde_json::from_str::<serde_json::Value>(&pueue_status.stdout).map_err(
            |_| AppError::Message {
                message: "upgrade Pueue health check returned invalid status JSON".to_owned(),
            },
        )?;
        validate_pueue_status_shape(&status)
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
                report.rollback = UpgradeRollback::Succeeded;
                UpgradeFailure::with_report(
                    report,
                    AppError::Message {
                        message: format!(
                            "upgrade failed after installation: {}; rollback succeeded{}",
                            bounded_redacted_text(&failure.to_string()),
                            cleanup_failure_suffix(&[backup])
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
            return Err(with_cleanup_failure(error, &[&restore_candidate]));
        }
        if let Err(error) = fs::rename(&restore_candidate, install_target) {
            return Err(with_cleanup_failure(
                AppError::Io {
                    operation: "atomically restore upgrade backup",
                    source: error,
                },
                &[&restore_candidate],
            ));
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
        return Err(with_cleanup_failure(
            AppError::Io {
                operation: "write upgrade retry marker",
                source,
            },
            &[&candidate],
        ));
    }
    if let Err(source) = fs::rename(&candidate, path) {
        return Err(with_cleanup_failure(
            AppError::Io {
                operation: "atomically persist upgrade retry marker",
                source,
            },
            &[&candidate],
        ));
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
    owner_token: String,
    _coordination_guard: UpgradeCoordinationGuard,
}

impl UpgradeLock {
    fn acquire(state_dir: &Path) -> Result<Self, AppError> {
        fs::create_dir_all(state_dir).map_err(|source| AppError::Io {
            operation: "create upgrade state directory",
            source,
        })?;
        let coordination_guard = UpgradeCoordinationGuard::try_acquire(state_dir)?;
        let path = state_dir.join("upgrade.lock");
        let owner_path = path.join("owner");
        for _ in 0..32 {
            match fs::create_dir(&path) {
                Ok(()) => {
                    let owner_token = lock_owner_token();
                    if let Err(source) = fs::write(&owner_path, &owner_token) {
                        let _ = remove_file_if_present(&owner_path);
                        let _ = fs::remove_dir(&path);
                        return Err(AppError::Io {
                            operation: "write upgrade lock PID",
                            source,
                        });
                    }
                    return Ok(Self {
                        path,
                        owner_path,
                        owner_token,
                        _coordination_guard: coordination_guard,
                    });
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
        remove_owned_lock(&self.path, &self.owner_path, &self.owner_token);
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
    let pid_text = contents.trim().split_once(':').map_or(contents.trim(), |(pid, _)| pid);
    let Ok(pid) = pid_text.parse::<u32>() else {
        return Ok(None);
    };
    Ok(process_is_alive(pid).then_some(pid))
}

fn reclaim_stale_lock(state_dir: &Path, lock_path: &Path) -> Result<(), AppError> {
    let reclaimed = temporary_path(state_dir, "upgrade-lock-reclaimed")?;
    match fs::rename(lock_path, &reclaimed) {
        Ok(()) => {
            remove_file_if_present(&reclaimed.join("owner"))?;
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

fn remove_owned_lock(path: &Path, owner_path: &Path, owner_token: &str) {
    let matches_owner = fs::read_to_string(owner_path)
        .ok()
        .is_some_and(|owner| owner.trim() == owner_token);
    if matches_owner {
        let _ = remove_file_if_present(owner_path);
        let _ = fs::remove_dir(path);
    }
}

pub struct AgentStartUpgradeGuard {
    _coordination_guard: UpgradeCoordinationGuard,
}

impl AgentStartUpgradeGuard {
    pub fn acquire(db: &Db) -> Result<Self, AppError> {
        let state_dir = db
            .path()
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .ok_or_else(|| AppError::Message {
                message: "agent start database path has no state directory".to_owned(),
            })?;
        fs::create_dir_all(&state_dir).map_err(|source| AppError::Io {
            operation: "create agent start state directory",
            source,
        })?;
        Ok(Self {
            _coordination_guard: UpgradeCoordinationGuard::try_acquire(&state_dir)?,
        })
    }
}

struct UpgradeCoordinationGuard {
    #[cfg(unix)]
    file: File,
}

impl UpgradeCoordinationGuard {
    fn try_acquire(state_dir: &Path) -> Result<Self, AppError> {
        Self::acquire(state_dir, true)
    }

    fn acquire(state_dir: &Path, nonblocking: bool) -> Result<Self, AppError> {
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
            if let Err(source) = lock_file(&file, nonblocking) {
                if nonblocking && source.kind() == std::io::ErrorKind::WouldBlock {
                    return Err(AppError::UpgradeInProgress);
                }
                return Err(AppError::Io {
                    operation: "acquire upgrade coordination guard",
                    source,
                });
            }
            Ok(Self { file })
        }
        #[cfg(not(unix))]
        {
            let _ = (state_dir, nonblocking);
            Ok(Self {})
        }
    }
}

#[cfg(unix)]
impl Drop for UpgradeCoordinationGuard {
    fn drop(&mut self) {
        unlock_file(&self.file);
    }
}

#[cfg(not(unix))]
impl Drop for UpgradeCoordinationGuard {
    fn drop(&mut self) {}
}

#[cfg(unix)]
fn lock_file(file: &File, nonblocking: bool) -> Result<(), std::io::Error> {
    use std::os::fd::AsRawFd;

    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    const LOCK_EX: std::os::raw::c_int = 2;
    const LOCK_NB: std::os::raw::c_int = 4;
    let operation = if nonblocking { LOCK_EX | LOCK_NB } else { LOCK_EX };
    if unsafe { flock(file.as_raw_fd(), operation) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
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
static LOCK_OWNER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const MAX_UPGRADE_COMMAND_OUTPUT_BYTES: usize = 240;
const MAX_RAW_PROCESS_OUTPUT_BYTES: usize = 1_048_576;
const MAX_SANITIZED_PUEUE_STATUS_BYTES: usize = 262_144;
const MAX_PUEUE_STATUS_TASKS: usize = 1_024;
const MAX_PUEUE_STATUS_STATES: usize = 4;

fn lock_owner_token() -> String {
    let sequence = LOCK_OWNER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{}:{sequence}", std::process::id())
}

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

struct BoundedProcessOutput {
    success: bool,
    stdout: CappedBytes,
    stderr: CappedBytes,
}

struct CappedBytes {
    bytes: Vec<u8>,
    truncated: bool,
}

fn run_process_bounded(
    command: &mut Command,
    operation: &'static str,
) -> Result<BoundedProcessOutput, AppError> {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source| AppError::Io { operation, source })?;
    let stdout = child.stdout.take().ok_or(AppError::Runtime {
        operation: "capture bounded command stdout",
    })?;
    let stderr = child.stderr.take().ok_or(AppError::Runtime {
        operation: "capture bounded command stderr",
    })?;
    let stdout_reader = thread::spawn(move || read_capped(stdout));
    let stderr_reader = thread::spawn(move || read_capped(stderr));
    let status = child.wait().map_err(|source| AppError::Io { operation, source })?;
    let stdout = join_capped_output(stdout_reader, "read bounded command stdout")?;
    let stderr = join_capped_output(stderr_reader, "read bounded command stderr")?;
    Ok(BoundedProcessOutput {
        success: status.success(),
        stdout,
        stderr,
    })
}

fn read_capped<R: Read>(mut reader: R) -> Result<CappedBytes, std::io::Error> {
    let mut bytes = Vec::with_capacity(MAX_RAW_PROCESS_OUTPUT_BYTES.min(8_192));
    let mut buffer = [0_u8; 8_192];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = MAX_RAW_PROCESS_OUTPUT_BYTES.saturating_sub(bytes.len());
        let retained = count.min(remaining);
        bytes.extend_from_slice(&buffer[..retained]);
        truncated |= retained < count;
    }
    Ok(CappedBytes { bytes, truncated })
}

fn join_capped_output(
    reader: thread::JoinHandle<Result<CappedBytes, std::io::Error>>,
    operation: &'static str,
) -> Result<CappedBytes, AppError> {
    reader
        .join()
        .map_err(|_| AppError::Runtime { operation })?
        .map_err(|source| AppError::Io { operation, source })
}

struct SanitizedUpgradeStdout {
    text: String,
    limited: bool,
}

impl SanitizedUpgradeStdout {
    fn limited() -> Self {
        Self {
            text: String::new(),
            limited: true,
        }
    }
}

fn sanitize_upgrade_stdout(value: &str) -> SanitizedUpgradeStdout {
    if value.len() > MAX_RAW_PROCESS_OUTPUT_BYTES {
        return SanitizedUpgradeStdout::limited();
    }
    let Ok(status) = serde_json::from_str::<serde_json::Value>(value) else {
        return SanitizedUpgradeStdout {
            text: bounded_redacted_text(value),
            limited: false,
        };
    };
    let Some(status) = sanitize_pueue_status_shape(&status) else {
        return SanitizedUpgradeStdout {
            text: bounded_redacted_text(value),
            limited: false,
        };
    };
    let Ok(text) = serde_json::to_string(&status) else {
        return SanitizedUpgradeStdout {
            text: bounded_redacted_text(value),
            limited: false,
        };
    };
    if text.len() > MAX_SANITIZED_PUEUE_STATUS_BYTES {
        SanitizedUpgradeStdout::limited()
    } else {
        SanitizedUpgradeStdout {
            text,
            limited: false,
        }
    }
}

fn sanitize_pueue_status_shape(status: &serde_json::Value) -> Option<serde_json::Value> {
    let tasks = status.as_object()?.get("tasks")?.as_object()?;
    if tasks.len() > MAX_PUEUE_STATUS_TASKS {
        return None;
    }
    let mut safe_tasks = serde_json::Map::new();
    for (index, task) in tasks.values().enumerate() {
        safe_tasks.insert(index.to_string(), sanitize_pueue_task_shape(task));
    }
    let mut safe_status = serde_json::Map::new();
    safe_status.insert("tasks".to_owned(), serde_json::Value::Object(safe_tasks));
    Some(serde_json::Value::Object(safe_status))
}

fn sanitize_pueue_task_shape(task: &serde_json::Value) -> serde_json::Value {
    let Some(task) = task.as_object() else {
        return serde_json::Value::Null;
    };
    let mut safe_task = serde_json::Map::new();
    if let Some(id) = task.get("id") {
        safe_task.insert("id".to_owned(), sanitize_pueue_task_id(id));
    }
    for field in ["group", "command"] {
        if let Some(value) = task.get(field) {
            safe_task.insert(field.to_owned(), sanitize_string_shape(value));
        }
    }
    if let Some(status) = task.get("status") {
        safe_task.insert("status".to_owned(), sanitize_pueue_task_status(status));
    }
    serde_json::Value::Object(safe_task)
}

fn sanitize_pueue_task_id(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Number(value) if value.as_i64().is_some_and(|id| id >= 0) => {
            serde_json::json!(0)
        }
        serde_json::Value::String(value)
            if value.parse::<i64>().is_ok_and(|id| id >= 0) =>
        {
            serde_json::json!("0")
        }
        _ => serde_json::Value::Null,
    }
}

fn sanitize_string_shape(value: &serde_json::Value) -> serde_json::Value {
    if value.is_string() {
        serde_json::json!("")
    } else {
        serde_json::Value::Null
    }
}

fn sanitize_pueue_task_status(value: &serde_json::Value) -> serde_json::Value {
    let Some(statuses) = value.as_object() else {
        return serde_json::Value::Null;
    };
    if statuses.len() > MAX_PUEUE_STATUS_STATES {
        return serde_json::Value::Null;
    }
    let mut safe_statuses = serde_json::Map::new();
    for (index, details) in statuses.values().enumerate() {
        safe_statuses.insert(
            format!("state-{index}"),
            sanitize_pueue_state_details(details),
        );
    }
    serde_json::Value::Object(safe_statuses)
}

fn sanitize_pueue_state_details(value: &serde_json::Value) -> serde_json::Value {
    let Some(details) = value.as_object() else {
        return serde_json::Value::Null;
    };
    let mut safe_details = serde_json::Map::new();
    for field in ["enqueued_at", "start", "end"] {
        if let Some(value) = details.get(field) {
            safe_details.insert(
                field.to_owned(),
                sanitize_timestamp_shape(value),
            );
        }
    }
    serde_json::Value::Object(safe_details)
}

fn sanitize_timestamp_shape(value: &serde_json::Value) -> serde_json::Value {
    if value.is_null() {
        serde_json::Value::Null
    } else if value.is_string() {
        serde_json::json!("")
    } else {
        serde_json::Value::Bool(false)
    }
}

fn validate_pueue_status_shape(status: &serde_json::Value) -> Result<(), AppError> {
    let tasks = status
        .as_object()
        .and_then(|status| status.get("tasks"))
        .and_then(serde_json::Value::as_object)
        .ok_or_else(invalid_pueue_status_shape)?;
    for task in tasks.values() {
        let task = task.as_object().ok_or_else(invalid_pueue_status_shape)?;
        let valid_id = match task.get("id") {
            Some(serde_json::Value::Number(value)) => value.as_i64().is_some_and(|id| id >= 0),
            Some(serde_json::Value::String(value)) => {
                value.parse::<i64>().is_ok_and(|id| id >= 0)
            }
            _ => false,
        };
        let valid_strings = ["group", "command"]
            .into_iter()
            .all(|field| task.get(field).is_some_and(serde_json::Value::is_string));
        let status = task
            .get("status")
            .and_then(serde_json::Value::as_object)
            .filter(|status| status.len() == 1)
            .and_then(|status| status.values().next())
            .and_then(serde_json::Value::as_object);
        let valid_timestamps = status.is_some_and(|details| {
            ["enqueued_at", "start", "end"].into_iter().all(|field| {
                details
                    .get(field)
                    .is_none_or(|value| value.is_null() || value.is_string())
            })
        });
        if !valid_id || !valid_strings || !valid_timestamps {
            return Err(invalid_pueue_status_shape());
        }
    }
    Ok(())
}

fn invalid_pueue_status_shape() -> AppError {
    AppError::Message {
        message: "upgrade Pueue health check returned an invalid status schema".to_owned(),
    }
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

fn remove_file_if_present(path: &Path) -> Result<(), AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(AppError::Io {
            operation: "remove upgrade temporary file",
            source,
        }),
    }
}

fn with_cleanup_failure(error: AppError, paths: &[&Path]) -> AppError {
    let suffix = cleanup_failure_suffix(paths);
    if suffix.is_empty() {
        error
    } else {
        AppError::Message {
            message: format!("{}{}", bounded_redacted_text(&error.to_string()), suffix),
        }
    }
}

fn cleanup_failure_suffix(paths: &[&Path]) -> String {
    let details = paths
        .iter()
        .filter_map(|path| remove_file_if_present(path).err())
        .map(|error| bounded_redacted_text(&error.to_string()))
        .collect::<Vec<_>>();
    if details.is_empty() {
        String::new()
    } else {
        format!("; upgrade cleanup failed: {}", details.join("; "))
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
    let mut output = runner.run(source, args).map_err(|error| git_runner_error(args, error))?;
    output.stderr = bounded_redacted_text(&output.stderr);
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Cursor,
        path::Path,
        sync::mpsc,
        time::Duration,
    };

    use tempfile::TempDir;

    use super::{
        cleanup_failure_suffix, lock_file, read_capped, run_git, AgentStartUpgradeGuard,
        GitCommandOutput, GitCommandRunner, UpgradeLock, MAX_RAW_PROCESS_OUTPUT_BYTES,
    };
    use crate::{
        db::Db,
        AppError,
    };

    struct FailingGitRunner;

    impl GitCommandRunner for FailingGitRunner {
        fn run(&self, _source: &Path, _args: &[&str]) -> Result<GitCommandOutput, AppError> {
            Ok(GitCommandOutput {
                success: false,
                stdout: String::new(),
                stderr: format!("token=adapter-secret {}", "x".repeat(10_000)),
            })
        }
    }

    #[test]
    fn git_adapter_redacts_and_bounds_runner_stderr() {
        let error = run_git(&FailingGitRunner, Path::new("."), &["fetch", "origin", "main"])
            .unwrap_err();

        assert!(!error.to_string().contains("adapter-secret"));
        assert!(error.to_string().len() < 600);
    }

    #[test]
    fn old_lock_cleanup_keeps_a_replaced_lock_generation() {
        let temporary = TempDir::new().unwrap();
        let lock = UpgradeLock::acquire(temporary.path()).unwrap();
        let path = lock.path.clone();
        fs::write(path.join("owner"), "new-lock-generation").unwrap();

        drop(lock);

        assert!(path.is_dir());
        assert_eq!(fs::read_to_string(path.join("owner")).unwrap(), "new-lock-generation");
    }

    #[cfg(unix)]
    #[test]
    fn agent_start_guard_cannot_enter_while_upgrade_owns_the_coordination_guard() {
        let temporary = TempDir::new().unwrap();
        let db = Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let _upgrade_lock = UpgradeLock::acquire(temporary.path()).unwrap();

        let error = AgentStartUpgradeGuard::acquire(&db)
            .err()
            .expect("upgrade coordination guard must exclude agent start");

        assert!(matches!(error, AppError::UpgradeInProgress));
    }

    #[cfg(unix)]
    #[test]
    fn competing_upgrade_rejects_flock_contention_without_waiting() {
        let temporary = TempDir::new().unwrap();
        let guard_path = temporary.path().join("upgrade.lock.guard");
        let guard_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(guard_path)
            .unwrap();
        lock_file(&guard_file, false).unwrap();

        let state_dir = temporary.path().to_path_buf();
        let (sender, receiver) = mpsc::channel();
        let handle = std::thread::spawn(move || {
            sender.send(UpgradeLock::acquire(&state_dir)).unwrap();
        });

        let immediate_result = receiver.recv_timeout(Duration::from_millis(100));
        drop(guard_file);
        let eventual_result = handle.join().unwrap();

        assert!(
            matches!(immediate_result, Ok(Err(AppError::UpgradeInProgress))),
            "competing upgrade must reject immediately; eventual result: {eventual_result:?}"
        );
    }

    #[test]
    fn process_capture_discards_bytes_after_the_safe_limit() {
        let output = read_capped(Cursor::new(vec![b'x'; MAX_RAW_PROCESS_OUTPUT_BYTES + 1]))
            .unwrap();

        assert_eq!(output.bytes.len(), MAX_RAW_PROCESS_OUTPUT_BYTES);
        assert!(output.truncated);
    }

    #[test]
    fn cleanup_failure_is_retained_in_the_failure_diagnostics() {
        let temporary = TempDir::new().unwrap();
        let directory = temporary.path().join("not-a-file");
        fs::create_dir(&directory).unwrap();

        let suffix = cleanup_failure_suffix(&[&directory]);

        assert!(suffix.contains("upgrade cleanup failed"));
        assert!(suffix.len() < 600);
    }
}
