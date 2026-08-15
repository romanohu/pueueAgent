use std::{
    cell::{Cell, RefCell},
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU32, Ordering},
};

use async_trait::async_trait;
#[cfg(unix)]
use std::os::unix::fs::symlink;

use pueue_agent::{
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    models::{AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewProject},
    pueue::PueueError,
    service::{ServiceControl, ServiceDefinition, ServiceStatus},
    upgrade::{
        render_report, resolve_pueue_config, resolve_pueue_config_with_service,
        resolve_source_root, validate_checkout,
        validate_checkout_with, CheckoutState, GitCommandOutput, GitCommandRunner,
        UpgradeCommandOutput, UpgradeCommandRunner, UpgradeFailure, UpgradeReport, UpgradeRollback,
        UpgradePueueHealth, UpgradeRunner, UpgradeStep,
    },
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[test]
fn upgrade_report_rendering_is_bounded_and_redacts_sensitive_values() {
    let report = UpgradeReport {
        source: PathBuf::from("/tmp/SECRET_TOKEN=hidden"),
        checkout: CheckoutState {
            branch: "main".to_owned(),
            upstream: "origin/main".to_owned(),
            head: "old-revision".to_owned(),
            upstream_head: "new-revision".to_owned(),
        },
        old_revision: "old-revision".to_owned(),
        new_revision: "new-revision".to_owned(),
        tests: UpgradeStep {
            attempted: true,
            succeeded: true,
        },
        build: UpgradeStep {
            attempted: true,
            succeeded: true,
        },
        install: UpgradeStep {
            attempted: true,
            succeeded: true,
        },
        restart: UpgradeStep {
            attempted: true,
            succeeded: true,
        },
        health: UpgradeStep {
            attempted: true,
            succeeded: true,
        },
        rollback: UpgradeRollback::NotRequired,
    };

    let human = render_report(&report, false).unwrap();
    assert!(human.starts_with("pueue-agent upgrade\n"));
    assert!(human.contains("status=updated"));
    assert!(human.contains("tests=ok"));
    assert!(human.contains("rollback=not_required"));
    assert!(human.lines().all(|line| line.len() <= 260));
    assert!(!human.contains("hidden"));

    let json = render_report(&report, true).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["status"], "updated");
    assert_eq!(value["noop"], false);
    assert_eq!(value["steps"]["health"]["succeeded"], true);
    assert_eq!(value["rollback"], "not_required");
    assert!(!json.contains("hidden"));
}

#[test]
fn pueue_config_resolution_prefers_explicit_env_and_default() {
    let explicit = PathBuf::from("/tmp/explicit-pueue.yml");
    let from_env = PathBuf::from("/tmp/env-pueue.yml");
    let home = PathBuf::from("/tmp/home");

    assert_eq!(
        resolve_pueue_config(Some(&explicit), Some(&from_env), Some(&home)),
        Some(explicit.clone())
    );
    assert_eq!(
        resolve_pueue_config(None, Some(&from_env), Some(&home)),
        Some(from_env)
    );
    assert_eq!(
        resolve_pueue_config(None, None, Some(&home)),
        Some(home.join(".config/pueue/pueue.yml"))
    );
}

#[test]
fn pueue_config_resolution_does_not_require_home_for_an_explicit_profile() {
    let explicit = PathBuf::from("/tmp/explicit-pueue.yml");
    let from_env = PathBuf::from("/tmp/env-pueue.yml");

    assert_eq!(
        resolve_pueue_config(Some(&explicit), Some(&from_env), None),
        Some(explicit)
    );
}

#[test]
fn pueue_config_resolution_uses_the_installed_service_before_the_default() {
    let service = PathBuf::from("/tmp/custom-pueue.yml");
    let home = PathBuf::from("/tmp/home");

    assert_eq!(
        resolve_pueue_config_with_service(None, None, Some(&service), Some(&home)),
        Some(service)
    );
}

#[test]
fn upgrade_has_no_ambient_pueue_binary_execution_path() {
    let source = include_str!("../../src/upgrade.rs");
    assert!(!source.contains("PUEUE_BINARY"));
    assert!(!source.contains("Command::new(\"pueue\")"));
}

#[test]
fn upgrade_command_does_not_open_the_mutable_database_before_the_runner() {
    let source = include_str!("../../src/main.rs");
    let upgrade_command = source
        .split("pub async fn upgrade")
        .nth(1)
        .unwrap()
        .split("fn upgrade_diagnostic_error")
        .next()
        .unwrap();

    assert!(!upgrade_command.contains("Db::open("));
    assert!(upgrade_command.contains("UpgradeRunner::new"));
    assert!(upgrade_command.contains("state_db"));
}

fn state_artifacts(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    if !path.exists() {
        return Vec::new();
    }
    let mut artifacts = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            let contents = if path.is_file() {
                fs::read(&path).unwrap()
            } else {
                Vec::new()
            };
            (path, contents)
        })
        .collect::<Vec<_>>();
    artifacts.sort_by(|left, right| left.0.cmp(&right.0));
    artifacts
}

struct UpgradeFixture {
    temp: TempDir,
    source: PathBuf,
    installed_binary: PathBuf,
    db: Db,
    commands: FakeCommandRunner,
    service: FakeServiceRecorder,
    pueue: FakePueue,
}

impl UpgradeFixture {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let remote = temp.path().join("origin.git");
        run_git(temp.path(), ["init", "--bare", remote.to_str().unwrap()]);

        let source = temp.path().join("source");
        run_git(temp.path(), ["init", "-b", "main", source.to_str().unwrap()]);
        fs::write(
            source.join("Cargo.toml"),
            "[package]\nname = \"pueue-agent\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        fs::write(source.join("README.md"), "fixture\n").unwrap();
        run_git(&source, ["add", "."]);
        run_git(
            &source,
            [
                "-c",
                "user.name=Upgrade Fixture",
                "-c",
                "user.email=upgrade-fixture@example.test",
                "commit",
                "-m",
                "initial fixture",
            ],
        );
        run_git(&source, ["remote", "add", "origin", remote.to_str().unwrap()]);
        run_git(&source, ["push", "-u", "origin", "main"]);

        let installed_binary = temp.path().join("bin/pueue-agent");
        fs::create_dir_all(installed_binary.parent().unwrap()).unwrap();
        fs::write(&installed_binary, Self::old_binary_bytes()).unwrap();
        let db = Db::open(&temp.path().join("state/state.sqlite3")).unwrap();

        Self {
            temp,
            source: source.canonicalize().unwrap(),
            installed_binary,
            db: db.clone(),
            commands: FakeCommandRunner::default(),
            service: FakeServiceRecorder::with_db(db.clone()),
            pueue: FakePueue::default(),
        }
    }

    fn dirty_main_checkout() -> Self {
        let fixture = Self::new();
        fs::write(fixture.source.join("README.md"), "dirty fixture\n").unwrap();
        fixture
    }

    fn build_failure() -> Self {
        let fixture = Self::new();
        fixture.commands.fail_build.set(true);
        fixture
    }

    fn test_failure() -> Self {
        let fixture = Self::new();
        fixture.commands.fail_tests.set(true);
        fixture
    }

    fn service_failure() -> Self {
        let fixture = Self::new();
        fixture
            .service
            .set_statuses([ServiceStatus::Stopped, ServiceStatus::Running]);
        fixture
    }

    fn pueue_failure() -> Self {
        let fixture = Self::new();
        fixture
            .pueue
            .fail_on_call(1, FakePueueFailure::Unavailable);
        fixture
    }

    fn post_restart_pueue_failure() -> Self {
        let fixture = Self::new();
        fixture.pueue.fail_on_call(2, FakePueueFailure::Timeout);
        fixture
    }

    fn pueue_invalid_status() -> Self {
        let fixture = Self::new();
        fixture
            .pueue
            .fail_on_call(1, FakePueueFailure::InvalidJson);
        fixture
    }

    fn pueue_malformed_task_status() -> Self {
        let fixture = Self::new();
        fixture
            .pueue
            .fail_on_call(1, FakePueueFailure::InvalidTask);
        fixture
    }

    fn pueue_valid_task_status() -> Self {
        Self::new()
    }

    fn pueue_malformed_timestamp_status() -> Self {
        let fixture = Self::new();
        fixture
            .pueue
            .fail_on_call(1, FakePueueFailure::InvalidTimestamp);
        fixture
    }

    fn pueue_oversized_status() -> Self {
        let fixture = Self::new();
        fixture
            .pueue
            .fail_on_call(1, FakePueueFailure::OutputLimit);
        fixture
    }

    fn source_root(&self) -> &Path {
        &self.source
    }

    fn release_binary(&self) -> PathBuf {
        let binary = self.source.join("target/release/pueue-agent");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, "fixture binary\n").unwrap();
        binary
    }

    fn old_binary_bytes() -> &'static [u8] {
        b"old fixture binary\n"
    }

    fn new_binary_bytes() -> &'static [u8] {
        b"new fixture binary\n"
    }

    fn installed_binary(&self) -> Vec<u8> {
        fs::read(&self.installed_binary).unwrap()
    }

    #[cfg(unix)]
    fn install_as_symlink(&self) -> PathBuf {
        let target = self.temp.path().join("bin/pueue-agent-previous");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::rename(&self.installed_binary, &target).unwrap();
        symlink(&target, &self.installed_binary).unwrap();
        target
    }

    fn service_calls(&self) -> Vec<String> {
        self.service.actions.borrow().clone()
    }

    async fn run_upgrade(
        &self,
    ) -> Result<pueue_agent::upgrade::UpgradeReport, UpgradeFailure> {
        UpgradeRunner::new(
            self.options(),
            self.db.path().to_path_buf(),
            &self.service,
            &self.commands,
            &self.pueue,
        )
        .run()
        .await
    }

    fn options(&self) -> pueue_agent::upgrade::UpgradeOptions {
        pueue_agent::upgrade::UpgradeOptions {
            source: Some(self.source.clone()),
            json: false,
            branch: "main".to_owned(),
            remote: "origin".to_owned(),
            release_binary: self.installed_binary.clone(),
            installed_revision: "old-revision".to_owned(),
        }
    }

    async fn run_upgrade_with_installed_revision(
        &self,
        installed_revision: &str,
    ) -> Result<pueue_agent::upgrade::UpgradeReport, UpgradeFailure> {
        let mut options = self.options();
        options.installed_revision = installed_revision.to_owned();
        UpgradeRunner::new(
            options,
            self.db.path().to_path_buf(),
            &self.service,
            &self.commands,
            &self.pueue,
        )
        .run()
        .await
    }

    fn make_noop(&self) {
        self.commands.no_update.set(true);
    }

    fn make_noop_at_revision(&self, revision: &str) {
        self.make_noop();
        *self.commands.revision_override.borrow_mut() = Some(revision.to_owned());
    }

    fn retry_marker(&self) -> PathBuf {
        self.state_dir().join("upgrade.pending")
    }

    fn fail_fetch_with(&self, stderr: impl Into<String>) {
        self.commands.fail_fetch.set(true);
        *self.commands.fetch_stderr.borrow_mut() = stderr.into();
    }

    fn fail_build_with(&self, stderr: impl Into<String>) {
        self.commands.fail_build.set(true);
        *self.commands.build_stderr.borrow_mut() = stderr.into();
    }

    fn register_active_agent_run(&self) {
        self.active_run_plan("project-a").activate();
    }

    fn active_run_plan(&self, project_id: &str) -> ActiveRunPlan {
        ActiveRunPlan {
            db: self.db.clone(),
            project_id: project_id.to_owned(),
            project_root: self.temp.path().join(project_id),
            log_path: self.temp.path().join(format!("{project_id}.log")),
        }
    }

    fn activate_agent_during_checkout_validation(&self) {
        *self.commands.activate_during_checkout.borrow_mut() =
            Some(self.active_run_plan("project-during-checkout"));
    }

    fn activate_agent_before_install(&self) {
        *self.commands.activate_before_install.borrow_mut() =
            Some(self.active_run_plan("project-before-install"));
    }

    fn write_lock(&self, pid: impl std::fmt::Display) {
        let lock = self.state_dir().join("upgrade.lock");
        fs::create_dir_all(&lock).unwrap();
        fs::write(lock.join("owner"), pid.to_string()).unwrap();
    }

    fn state_dir(&self) -> &Path {
        self.db.path().parent().unwrap()
    }

    fn set_database_marker(&self, value: &str) {
        let connection = self.db.connect().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS upgrade_fixture_marker (value TEXT NOT NULL);
                 DELETE FROM upgrade_fixture_marker;",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO upgrade_fixture_marker (value) VALUES (?1)",
                [value],
            )
            .unwrap();
    }

    fn database_marker(&self) -> String {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT value FROM upgrade_fixture_marker ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn database_backups(&self) -> Vec<PathBuf> {
        fs::read_dir(self.state_dir())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| {
                        name.to_string_lossy()
                            .starts_with(".upgrade-database-backup-")
                    })
            })
            .collect()
    }
}

#[tokio::test]
async fn pueue_preflight_failure_does_not_create_a_missing_database_parent() {
    let temporary = TempDir::new().unwrap();
    let database_path = temporary.path().join("missing-state/state.sqlite3");
    let pueue = FakePueue::default();
    pueue.fail_on_call(1, FakePueueFailure::Unavailable);
    let service = FakeServiceRecorder::running();
    let commands = FakeCommandRunner::default();
    let options = pueue_agent::upgrade::UpgradeOptions {
        source: Some(temporary.path().join("unused-source")),
        json: false,
        branch: "main".to_owned(),
        remote: "origin".to_owned(),
        release_binary: temporary.path().join("unused-binary"),
        installed_revision: "old-revision".to_owned(),
    };

    let failure = UpgradeRunner::new(options, database_path.clone(), &service, &commands, &pueue)
        .run()
        .await
        .unwrap_err();

    assert!(failure.to_string().contains("Pueue preflight"));
    assert_eq!(pueue.status_calls.load(Ordering::SeqCst), 1);
    assert!(!database_path.exists());
    assert!(!database_path.parent().unwrap().exists());
    assert!(service.actions.borrow().is_empty());
    assert!(commands.invocations.borrow().is_empty());
    assert!(commands.command_invocations.borrow().is_empty());
}

struct ActiveRunPlan {
    db: Db,
    project_id: String,
    project_root: PathBuf,
    log_path: PathBuf,
}

impl ActiveRunPlan {
    fn activate(self) {
        fs::create_dir_all(&self.project_root).unwrap();
        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                &self.project_id,
                &self.project_root,
                format!("pa-{}", self.project_id),
                self.project_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let event = EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                &self.project_id,
                EventKind::TaskFinished,
                format!("upgrade-active-run-{}", self.project_id),
                json!({"task_id": 42}),
                100,
                100,
            ))
            .unwrap();
        AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::new(
                &self.project_id,
                event.event_id,
                None,
                AgentRunStatus::Running,
                100,
                &self.log_path,
            ))
            .unwrap();
    }
}

#[derive(Default)]
struct FakePueue {
    status_calls: AtomicU32,
    failure_call: AtomicU32,
    failure_kind: AtomicU32,
}

#[derive(Clone, Copy)]
enum FakePueueFailure {
    Unavailable = 1,
    Timeout = 2,
    InvalidJson = 3,
    InvalidTask = 4,
    InvalidTimestamp = 5,
    OutputLimit = 6,
}

impl FakePueue {
    fn fail_on_call(&self, call: u32, failure: FakePueueFailure) {
        self.failure_kind.store(failure as u32, Ordering::SeqCst);
        self.failure_call.store(call, Ordering::SeqCst);
    }

    fn clear_failure(&self) {
        self.failure_call.store(0, Ordering::SeqCst);
    }

    fn failure(&self) -> AppError {
        match self.failure_kind.load(Ordering::SeqCst) {
            value if value == FakePueueFailure::Timeout as u32 => {
                AppError::Pueue(PueueError::Timeout {
                    operation: "status",
                })
            }
            value if value == FakePueueFailure::InvalidJson as u32 => {
                AppError::Pueue(PueueError::InvalidStatusJson {
                    source: serde_json::from_str::<serde_json::Value>("not-json").unwrap_err(),
                })
            }
            value if value == FakePueueFailure::InvalidTask as u32 => {
                AppError::Pueue(PueueError::InvalidStatusTask {
                    reason: "task is not an object",
                })
            }
            value if value == FakePueueFailure::InvalidTimestamp as u32 => {
                AppError::Pueue(PueueError::InvalidStatusTask {
                    reason: "task status timestamp is invalid",
                })
            }
            value if value == FakePueueFailure::OutputLimit as u32 => {
                AppError::Pueue(PueueError::OutputLimit {
                    operation: "status",
                    stream: "stdout",
                })
            }
            _ => AppError::Message {
                message: "fixture Pueue unavailable".to_owned(),
            },
        }
    }
}

#[async_trait]
impl UpgradePueueHealth for FakePueue {
    async fn check_status(&self) -> Result<(), AppError> {
        let call = self.status_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.failure_call.load(Ordering::SeqCst) {
            return Err(self.failure());
        }
        Ok(())
    }
}

fn accepts_upgrade_health<P: UpgradePueueHealth>(_health: &P) {}

#[test]
fn upgrade_fake_preserves_the_health_provider_contract() {
    accepts_upgrade_health(&FakePueue::default());
}

#[derive(Default)]
struct FakeCommandRunner {
    invocations: RefCell<Vec<Vec<String>>>,
    command_invocations: RefCell<Vec<(String, Vec<String>)>>,
    fail_tests: Cell<bool>,
    fail_build: Cell<bool>,
    fail_fetch: Cell<bool>,
    fetch_stderr: RefCell<String>,
    build_stderr: RefCell<String>,
    activate_during_checkout: RefCell<Option<ActiveRunPlan>>,
    activate_before_install: RefCell<Option<ActiveRunPlan>>,
    no_update: Cell<bool>,
    revision_override: RefCell<Option<String>>,
    merged: Cell<bool>,
}

impl GitCommandRunner for FakeCommandRunner {
    fn run(&self, _source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError> {
        self.invocations
            .borrow_mut()
            .push(args.iter().map(|arg| (*arg).to_owned()).collect());

        let revision_override = self.revision_override.borrow().clone();
        let (success, stdout, stderr) = match args {
            ["status", "--porcelain"] => {
                if let Some(plan) = self.activate_during_checkout.borrow_mut().take() {
                    plan.activate();
                }
                (true, "", String::new())
            }
            ["branch", "--show-current"] => (true, "main", String::new()),
            ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"] => {
                (true, "origin/main", String::new())
            }
            ["fetch", "origin", "main"] if self.fail_fetch.get() => {
                (false, "", self.fetch_stderr.borrow().clone())
            }
            ["fetch", "origin", "main"] => (true, "", String::new()),
            ["rev-parse", "HEAD"] => (
                true,
                revision_override
                    .as_deref()
                    .unwrap_or(if self.merged.get() {
                        "new-revision"
                    } else {
                        "old-revision"
                    }),
                String::new(),
            ),
            ["rev-parse", "origin/main"] if self.no_update.get() => {
                (
                    true,
                    revision_override
                        .as_deref()
                        .unwrap_or("old-revision"),
                    String::new(),
                )
            }
            ["rev-parse", "origin/main"] => (true, "new-revision", String::new()),
            ["merge-base", "--is-ancestor", "HEAD", "origin/main"] => (true, "", String::new()),
            ["merge", "--ff-only", "origin/main"] => {
                self.merged.set(true);
                (true, "", String::new())
            }
            _ => panic!("unexpected git arguments: {args:?}"),
        };

        Ok(GitCommandOutput {
            success,
            stdout: stdout.to_owned(),
            stderr,
        })
    }
}

impl UpgradeCommandRunner for FakeCommandRunner {
    fn run_command(
        &self,
        _working_directory: &Path,
        program: &OsStr,
        args: &[OsString],
    ) -> Result<UpgradeCommandOutput, AppError> {
        let program = program.to_string_lossy().into_owned();
        let arguments = args
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        self.command_invocations
            .borrow_mut()
            .push((program.clone(), arguments.clone()));

        match (program.as_str(), arguments.first().map(String::as_str)) {
            ("cargo", Some("test")) if self.fail_tests.get() => {
                Ok(UpgradeCommandOutput::failure("test failure"))
            }
            ("cargo", Some("build")) if self.fail_build.get() => {
                let stderr = self.build_stderr.borrow();
                Ok(UpgradeCommandOutput::failure(if stderr.is_empty() {
                    "build failure"
                } else {
                    &stderr
                }))
            }
            ("cargo", Some("build")) => {
                let target_dir = arguments
                    .windows(2)
                    .find_map(|arguments| {
                        (arguments[0] == "--target-dir").then_some(PathBuf::from(&arguments[1]))
                    })
                    .expect("build must use a temporary target directory");
                let candidate = target_dir.join("release/pueue-agent");
                fs::create_dir_all(candidate.parent().unwrap()).unwrap();
                fs::write(candidate, UpgradeFixture::new_binary_bytes()).unwrap();
                if let Some(plan) = self.activate_before_install.borrow_mut().take() {
                    plan.activate();
                }
                Ok(UpgradeCommandOutput::success())
            }
            ("cargo", Some("test")) => Ok(UpgradeCommandOutput::success()),
            _ => panic!("unexpected upgrade command: {program} {arguments:?}"),
        }
    }
}

struct FakeServiceRecorder {
    actions: RefCell<Vec<String>>,
    database: RefCell<Option<Db>>,
    status: Cell<ServiceStatus>,
    status_sequence: RefCell<Vec<ServiceStatus>>,
    failed_restarts_remaining: Cell<u8>,
    stop_calls: Cell<u8>,
    fail_stop_on_call: Cell<Option<u8>>,
    mutate_database_on_first_stop: Cell<bool>,
    mutate_database_on_first_restart: Cell<bool>,
    #[cfg(unix)]
    blocked_directory: RefCell<Option<(PathBuf, u32)>>,
}

impl Default for FakeServiceRecorder {
    fn default() -> Self {
        Self::running()
    }
}

impl FakeServiceRecorder {
    fn running() -> Self {
        Self {
            actions: RefCell::new(Vec::new()),
            database: RefCell::new(None),
            status: Cell::new(ServiceStatus::Running),
            status_sequence: RefCell::new(Vec::new()),
            failed_restarts_remaining: Cell::new(0),
            stop_calls: Cell::new(0),
            fail_stop_on_call: Cell::new(None),
            mutate_database_on_first_stop: Cell::new(false),
            mutate_database_on_first_restart: Cell::new(false),
            #[cfg(unix)]
            blocked_directory: RefCell::new(None),
        }
    }

    fn with_db(db: Db) -> Self {
        let service = Self::running();
        *service.database.borrow_mut() = Some(db);
        service
    }

    fn mutate_database_on_first_restart(&self) {
        self.mutate_database_on_first_restart.set(true);
    }

    fn mutate_database_on_first_stop(&self) {
        self.mutate_database_on_first_stop.set(true);
    }

    fn set_statuses(&self, statuses: impl IntoIterator<Item = ServiceStatus>) {
        *self.status_sequence.borrow_mut() = statuses.into_iter().collect();
    }

    fn fail_restarts(&self, count: u8) {
        self.failed_restarts_remaining.set(count);
    }

    fn fail_stop_on_call(&self, call: u8) {
        self.fail_stop_on_call.set(Some(call));
    }

    #[cfg(unix)]
    fn block_writes_on_first_stop(&self, directory: &Path) {
        *self.blocked_directory.borrow_mut() = Some((directory.to_path_buf(), 0));
    }

    #[cfg(unix)]
    fn restore_blocked_directory(&self) {
        use std::os::unix::fs::PermissionsExt;

        if let Some((directory, mode)) = self.blocked_directory.borrow_mut().take() {
            if mode != 0 {
                fs::set_permissions(directory, fs::Permissions::from_mode(mode)).unwrap();
            }
        }
    }
}

impl ServiceControl for FakeServiceRecorder {
    fn install(&self, _definition: &ServiceDefinition) -> Result<(), AppError> {
        panic!("upgrade must not install or replace the service definition")
    }

    fn start(&self) -> Result<(), AppError> {
        panic!("upgrade must restart the existing service instead of starting it")
    }

    fn stop(&self) -> Result<(), AppError> {
        self.actions.borrow_mut().push("stop".to_owned());
        let call = self.stop_calls.get() + 1;
        self.stop_calls.set(call);
        if self.fail_stop_on_call.get() == Some(call) {
            return Err(AppError::Message {
                message: "fake stop failure".to_owned(),
            });
        }
        #[cfg(unix)]
        if let Some((directory, mode)) = self.blocked_directory.borrow_mut().as_mut() {
            use std::os::unix::fs::PermissionsExt;

            *mode = fs::metadata(&*directory).unwrap().permissions().mode();
            fs::set_permissions(&*directory, fs::Permissions::from_mode(*mode & !0o222)).unwrap();
        }
        if self.mutate_database_on_first_stop.replace(false) {
            let connection = self.database.borrow().as_ref().unwrap().connect().unwrap();
            connection
                .execute(
                    "UPDATE upgrade_fixture_marker SET value = 'quiesced'",
                    [],
                )
                .unwrap();
        }
        Ok(())
    }

    fn restart(&self) -> Result<(), AppError> {
        self.actions.borrow_mut().push("restart".to_owned());
        #[cfg(unix)]
        self.restore_blocked_directory();
        let remaining = self.failed_restarts_remaining.get();
        if remaining > 0 {
            self.failed_restarts_remaining.set(remaining - 1);
            return Err(AppError::Message {
                message: "fake restart failure".to_owned(),
            });
        }
        if self.mutate_database_on_first_restart.replace(false) {
            let connection = self.database.borrow().as_ref().unwrap().connect().unwrap();
            connection
                .execute(
                    "UPDATE upgrade_fixture_marker SET value = 'candidate'",
                    [],
                )
                .unwrap();
        }
        Ok(())
    }

    fn status(&self) -> Result<ServiceStatus, AppError> {
        if self.status_sequence.borrow().is_empty() {
            Ok(self.status.get())
        } else {
            Ok(self.status_sequence.borrow_mut().remove(0))
        }
    }
}

fn run_git<const N: usize>(working_directory: &Path, args: [&str; N]) {
    let output = Command::new("git")
        .current_dir(working_directory)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn source_resolution_prefers_explicit_source_and_requires_cargo_manifest() {
    let fixture = UpgradeFixture::new();

    let source = resolve_source_root(
        Some(fixture.source_root()),
        Path::new("/unrelated/target/release/pueue-agent"),
        None,
    )
    .unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_uses_release_binary_ancestor_before_environment_fallback() {
    let fixture = UpgradeFixture::new();
    let executable = fixture.release_binary();

    let source = resolve_source_root(None, &executable, None).unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_falls_back_to_environment_source() {
    let fixture = UpgradeFixture::new();

    let source = resolve_source_root(
        None,
        Path::new("/unrelated/pueue-agent"),
        Some(fixture.source_root()),
    )
    .unwrap();

    assert_eq!(source, fixture.source_root());
}

#[test]
fn source_resolution_rejects_a_manifest_for_another_package() {
    let fixture = UpgradeFixture::new();
    let wrong_package = fixture.temp.path().join("wrong-package");
    fs::create_dir_all(wrong_package.join(".git")).unwrap();
    fs::write(
        wrong_package.join("Cargo.toml"),
        "[package]\nname = \"another-package\"\nversion = \"0.1.0\"\n",
    )
    .unwrap();

    let error = resolve_source_root(
        Some(&wrong_package),
        Path::new("/unrelated/pueue-agent"),
        None,
    )
    .unwrap_err();

    assert!(error.to_string().contains("pueue-agent"));
}

#[test]
fn clean_main_checkout_tracking_origin_main_is_accepted() {
    let fixture = UpgradeFixture::new();

    let state = validate_checkout(fixture.source_root(), "main", "origin").unwrap();

    assert_eq!(state.branch, "main");
    assert_eq!(state.upstream, "origin/main");
}

#[test]
fn checkout_validation_does_not_issue_fetch_commands() {
    let fixture = UpgradeFixture::new();

    validate_checkout_with(
        fixture.source_root(),
        "main",
        "origin",
        &fixture.commands,
    )
    .unwrap();

    assert!(fixture
        .commands
        .invocations
        .borrow()
        .iter()
        .all(|args| args.first().is_none_or(|command| command != "fetch")));
}

#[test]
fn dirty_or_diverged_checkout_is_rejected_without_mutation() {
    let fixture = UpgradeFixture::dirty_main_checkout();
    let head_before = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("clean") || error.to_string().contains("dirty"));
    assert_eq!(git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]), head_before);
}

#[test]
fn checkout_on_another_branch_is_rejected() {
    let fixture = UpgradeFixture::new();
    run_git(fixture.source_root(), ["checkout", "-b", "feature"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("main"));
}

#[test]
fn checkout_without_origin_main_upstream_is_rejected() {
    let fixture = UpgradeFixture::new();
    run_git(fixture.source_root(), ["branch", "--unset-upstream"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("origin/main"));
}

#[test]
fn checkout_ahead_of_origin_main_is_rejected_before_merge() {
    let fixture = UpgradeFixture::new();
    fs::write(fixture.source_root().join("ahead.txt"), "ahead\n").unwrap();
    run_git(fixture.source_root(), ["add", "ahead.txt"]);
    run_git(
        fixture.source_root(),
        [
            "-c",
            "user.name=Upgrade Fixture",
            "-c",
            "user.email=upgrade-fixture@example.test",
            "commit",
            "-m",
            "ahead fixture",
        ],
    );
    let head_before = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);

    let error = validate_checkout(fixture.source_root(), "main", "origin").unwrap_err();

    assert!(error.to_string().contains("fast-forward") || error.to_string().contains("diverged"));
    assert_eq!(git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]), head_before);
}

fn git_stdout<const N: usize>(working_directory: &Path, args: [&str; N]) -> String {
    let output = Command::new("git")
        .current_dir(working_directory)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

#[tokio::test]
async fn successful_upgrade_fast_forwards_builds_installs_and_checks_health() {
    let fixture = UpgradeFixture::new();

    let report = fixture.run_upgrade().await.unwrap();

    assert_eq!(report.old_revision, "old-revision");
    assert_eq!(report.new_revision, "new-revision");
    assert!(report.tests.succeeded);
    assert!(report.build.succeeded);
    assert!(report.install.succeeded);
    assert!(report.restart.succeeded);
    assert!(report.health.succeeded);
    assert_eq!(report.rollback, UpgradeRollback::NotRequired);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert_eq!(fixture.service_calls(), ["stop", "restart"]);
    assert_eq!(fixture.pueue.status_calls.load(Ordering::SeqCst), 2);
    assert!(fixture.database_backups().is_empty());
    assert!(fixture.commands.invocations.borrow().iter().any(|arguments| {
        arguments == &vec!["merge".to_owned(), "--ff-only".to_owned(), "origin/main".to_owned()]
    }));
    assert!(fixture.commands.command_invocations.borrow().iter().any(
        |(program, arguments)| {
            program == "cargo"
                && arguments.starts_with(&[
                    "test".to_owned(),
                    "--locked".to_owned(),
                    "--all-targets".to_owned(),
                    "--target-dir".to_owned(),
                ])
        }
    ));
    assert!(fixture.commands.command_invocations.borrow().iter().any(
        |(program, arguments)| {
            program == "cargo"
                && arguments.starts_with(&[
                    "build".to_owned(),
                    "--locked".to_owned(),
                    "--release".to_owned(),
                    "--target-dir".to_owned(),
                ])
        }
    ));
    let pueue_commands = fixture
        .commands
        .command_invocations
        .borrow()
        .iter()
        .filter(|(program, _)| program == "pueue")
        .cloned()
        .collect::<Vec<_>>();
    assert!(pueue_commands.is_empty());
}

#[tokio::test]
async fn pueue_preflight_failure_happens_before_upgrade_mutation_or_install() {
    let fixture = UpgradeFixture::pueue_failure();
    let state_before = state_artifacts(fixture.state_dir());

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert_eq!(fixture.pueue.status_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert!(fixture.service_calls().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
    assert!(fixture.commands.invocations.borrow().is_empty());
    assert!(!fixture.retry_marker().exists());
    assert_eq!(state_artifacts(fixture.state_dir()), state_before);
}

#[tokio::test]
async fn no_op_upgrade_skips_test_build_install_and_restart() {
    let fixture = UpgradeFixture::new();
    fixture.make_noop();

    let report = fixture.run_upgrade().await.unwrap();

    assert_eq!(report.old_revision, "old-revision");
    assert_eq!(report.new_revision, "old-revision");
    assert!(!report.tests.attempted);
    assert!(!report.build.attempted);
    assert!(!report.install.attempted);
    assert!(!report.restart.attempted);
    assert!(!report.health.attempted);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}

#[tokio::test]
async fn twelve_character_installed_revision_matches_full_checkout_head() {
    let fixture = UpgradeFixture::new();
    let full_head = git_stdout(fixture.source_root(), ["rev-parse", "HEAD"]);
    let installed_revision = full_head[..12].to_owned();
    fixture.make_noop_at_revision(&full_head);

    let report = fixture
        .run_upgrade_with_installed_revision(&installed_revision)
        .await
        .unwrap();

    assert_eq!(report.old_revision, full_head);
    assert_eq!(report.new_revision, full_head);
    assert!(!report.tests.attempted);
    assert!(!report.build.attempted);
    assert!(!report.install.attempted);
    assert!(!report.restart.attempted);
    assert!(!report.health.attempted);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert!(fixture.service_calls().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}

#[tokio::test]
async fn stale_or_unknown_installed_revision_does_not_take_noop_path() {
    for installed_revision in ["stale-revision", "unknown", ""] {
        let fixture = UpgradeFixture::new();
        fixture.make_noop();

        let report = fixture
            .run_upgrade_with_installed_revision(installed_revision)
            .await
            .unwrap();

        assert!(report.tests.attempted);
        assert!(report.build.attempted);
        assert!(report.install.attempted);
        assert!(report.restart.attempted);
        assert!(report.health.attempted);
        assert_eq!(fixture.service_calls(), ["stop", "restart"]);
    }
}

#[tokio::test]
async fn test_failure_leaves_installed_binary_and_service_unchanged() {
    let fixture = UpgradeFixture::test_failure();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("test"));
    assert!(failure.report().unwrap().tests.attempted);
    assert!(fixture.retry_marker().exists());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn build_failure_leaves_installed_binary_and_service_unchanged() {
    let fixture = UpgradeFixture::build_failure();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("build"));
    assert!(failure.report().unwrap().build.attempted);
    assert!(fixture.retry_marker().exists());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn service_health_failure_restores_previous_binary() {
    let fixture = UpgradeFixture::service_failure();
    fixture.set_database_marker("old");
    fixture.service.mutate_database_on_first_restart();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("rollback"));
    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "old");
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop", "restart"]);
    assert!(fixture.database_backups().is_empty());
}

#[tokio::test]
async fn post_restart_pueue_failure_restores_binary_database_and_service() {
    let fixture = UpgradeFixture::post_restart_pueue_failure();
    fixture.set_database_marker("old");
    fixture.service.mutate_database_on_first_restart();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert_eq!(fixture.pueue.status_calls.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "old");
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop", "restart"]);
    assert!(fixture.retry_marker().exists());
    assert!(fixture.database_backups().is_empty());
}

#[tokio::test]
async fn snapshot_is_created_after_the_service_is_quiesced_and_rollback_restores_both_states() {
    let fixture = UpgradeFixture::service_failure();
    fixture.set_database_marker("old");
    fixture.service.mutate_database_on_first_stop();
    fixture.service.mutate_database_on_first_restart();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "quiesced");
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop", "restart"]);
}

#[tokio::test]
async fn initial_service_stop_failure_leaves_installed_binary_and_database_unchanged() {
    let fixture = UpgradeFixture::new();
    fixture.set_database_marker("old");
    fixture.service.fail_stop_on_call(1);

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::NotRequired);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "old");
    assert_eq!(fixture.service_calls(), ["stop"]);
}

#[cfg(unix)]
#[tokio::test]
async fn snapshot_failure_restarts_the_unchanged_service() {
    let fixture = UpgradeFixture::new();
    fixture.set_database_marker("old");
    fixture
        .service
        .block_writes_on_first_stop(fixture.state_dir());

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("recovery succeeded"));
    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "old");
    assert_eq!(fixture.service_calls(), ["stop", "restart"]);
    assert!(fixture.database_backups().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn candidate_rename_failure_restarts_the_unchanged_service() {
    let fixture = UpgradeFixture::new();
    fixture.set_database_marker("old");
    fixture
        .service
        .block_writes_on_first_stop(fixture.installed_binary.parent().unwrap());

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("recovery succeeded"));
    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.database_marker(), "old");
    assert_eq!(fixture.service_calls(), ["stop", "restart"]);
    assert!(fixture.database_backups().is_empty());
}

#[tokio::test]
async fn pueue_health_failure_prevents_install_without_touching_experiment_tasks() {
    let fixture = UpgradeFixture::pueue_failure();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert!(failure.report().is_none());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert!(fixture.service_calls().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}

#[tokio::test]
async fn active_agent_run_rejects_upgrade_before_fetch_or_mutation() {
    let fixture = UpgradeFixture::new();
    fixture.register_active_agent_run();

    let error = fixture.run_upgrade().await.unwrap_err();

    assert!(error.to_string().contains("active agent run"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
    assert!(fixture.commands.invocations.borrow().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}

#[tokio::test]
async fn live_upgrade_lock_rejects_upgrade_before_fetch_or_mutation() {
    let fixture = UpgradeFixture::new();
    fixture.write_lock(std::process::id());

    let error = fixture.run_upgrade().await.unwrap_err();

    assert!(error.to_string().contains("lock"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
    assert!(fixture.commands.invocations.borrow().is_empty());
    assert!(fixture.commands.command_invocations.borrow().is_empty());
}

#[tokio::test]
async fn stale_upgrade_lock_is_reclaimed_before_a_successful_upgrade() {
    let fixture = UpgradeFixture::new();
    fixture.write_lock(u32::MAX);

    fixture.run_upgrade().await.unwrap();

    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert!(!fixture.state_dir().join("upgrade.lock").exists());
}

#[tokio::test]
async fn invalid_upgrade_lock_owner_is_reclaimed_before_a_successful_upgrade() {
    let fixture = UpgradeFixture::new();
    fixture.write_lock("not-a-pid");

    fixture.run_upgrade().await.unwrap();

    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert!(!fixture.state_dir().join("upgrade.lock").exists());
}

#[tokio::test]
async fn reclaimed_lock_directory_never_recursively_deletes_unknown_contents() {
    let fixture = UpgradeFixture::new();
    fixture.write_lock(u32::MAX);
    fs::write(
        fixture.state_dir().join("upgrade.lock/do-not-delete"),
        "preserve me",
    )
    .unwrap();

    fixture.run_upgrade().await.unwrap();

    let reclaimed = fs::read_dir(fixture.state_dir())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(".upgrade-lock-reclaimed"))
        })
        .expect("stale lock is preserved when it contains unknown files");
    assert_eq!(fs::read_to_string(reclaimed.join("do-not-delete")).unwrap(), "preserve me");
}

#[tokio::test]
async fn failed_post_fast_forward_upgrade_is_retried_for_the_pending_revision() {
    let fixture = UpgradeFixture::test_failure();

    fixture.run_upgrade().await.unwrap_err();
    assert!(fixture.retry_marker().exists());
    fixture.commands.fail_tests.set(false);

    let report = fixture.run_upgrade().await.unwrap();

    assert_eq!(report.new_revision, "new-revision");
    assert!(report.tests.succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert!(!fixture.retry_marker().exists());
    assert_eq!(
        fixture
            .commands
            .invocations
            .borrow()
            .iter()
            .filter(|arguments| arguments.first().is_some_and(|command| command == "merge"))
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_pueue_preflight_can_be_retried_without_a_pending_revision() {
    let fixture = UpgradeFixture::pueue_failure();

    fixture.run_upgrade().await.unwrap_err();
    assert!(!fixture.retry_marker().exists());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    fixture.pueue.clear_failure();

    let report = fixture.run_upgrade().await.unwrap();

    assert!(report.health.succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert!(!fixture.retry_marker().exists());
}

#[tokio::test]
async fn fetch_failure_is_bounded_redacted_and_preserves_the_installed_binary() {
    let fixture = UpgradeFixture::new();
    fixture.fail_fetch_with(format!("credential=top-secret {}", "x".repeat(10_000)));

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("fetch"));
    assert!(!failure.to_string().contains("top-secret"));
    assert!(failure.to_string().len() < 600);
    assert!(!fixture.retry_marker().exists());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn build_failure_stderr_is_bounded_and_redacted() {
    let fixture = UpgradeFixture::new();
    fixture.fail_build_with(format!("api-key=build-secret {}", "x".repeat(10_000)));

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("build"));
    assert!(!failure.to_string().contains("build-secret"));
    assert!(failure.to_string().len() < 600);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn invalid_pueue_status_json_is_rejected_before_install() {
    let fixture = UpgradeFixture::pueue_invalid_status();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert!(failure.report().is_none());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert!(fixture.service_calls().is_empty());
}

#[tokio::test]
async fn malformed_pueue_task_entry_is_rejected_before_install() {
    let fixture = UpgradeFixture::pueue_malformed_task_status();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert!(failure.report().is_none());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
}

#[tokio::test]
async fn pueue_task_status_shape_used_by_the_adapter_is_healthy() {
    let fixture = UpgradeFixture::pueue_valid_task_status();

    let report = fixture.run_upgrade().await.unwrap();

    assert!(report.health.succeeded);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
}

#[tokio::test]
async fn malformed_pueue_timestamp_is_rejected_before_install() {
    let fixture = UpgradeFixture::pueue_malformed_timestamp_status();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert!(failure.report().is_none());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
}

#[tokio::test]
async fn oversized_pueue_status_is_rejected_before_install_without_retaining_output() {
    let fixture = UpgradeFixture::pueue_oversized_status();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("Pueue"));
    assert!(failure.to_string().len() < 600);
    assert!(failure.report().is_none());
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
}

#[tokio::test]
async fn rollback_failure_is_exposed_in_the_failure_report() {
    let fixture = UpgradeFixture::new();
    fixture.service.fail_restarts(2);

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("rollback failed"));
    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Failed);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop", "restart"]);
}

#[tokio::test]
async fn rollback_stop_failure_includes_database_restore_detail() {
    let fixture = UpgradeFixture::service_failure();
    fixture.service.fail_stop_on_call(2);

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Failed);
    assert!(failure.to_string().contains("service stop"));
    assert!(failure.to_string().contains("database restore"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::new_binary_bytes());
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop"]);
}

#[tokio::test]
async fn rollback_requires_the_restarted_service_to_be_running() {
    let fixture = UpgradeFixture::service_failure();
    fixture
        .service
        .set_statuses([ServiceStatus::Stopped, ServiceStatus::Stopped]);

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Failed);
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), ["stop", "restart", "stop", "restart"]);
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_install_path_remains_a_symlink_to_the_replaced_target() {
    let fixture = UpgradeFixture::new();
    let target = fixture.install_as_symlink();

    fixture.run_upgrade().await.unwrap();

    assert!(fs::symlink_metadata(&fixture.installed_binary)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(target).unwrap(), UpgradeFixture::new_binary_bytes());
}

#[cfg(unix)]
#[tokio::test]
async fn symlinked_install_path_remains_intact_when_rollback_restores_its_target() {
    let fixture = UpgradeFixture::service_failure();
    let target = fixture.install_as_symlink();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert_eq!(failure.report().unwrap().rollback, UpgradeRollback::Succeeded);
    assert!(fs::symlink_metadata(&fixture.installed_binary)
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(fs::read(target).unwrap(), UpgradeFixture::old_binary_bytes());
}

#[tokio::test]
async fn active_agent_started_during_checkout_validation_blocks_fetch() {
    let fixture = UpgradeFixture::new();
    fixture.activate_agent_during_checkout_validation();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("active agent run"));
    assert!(!fixture
        .commands
        .invocations
        .borrow()
        .iter()
        .any(|arguments| arguments.first().is_some_and(|command| command == "fetch")));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
}

#[tokio::test]
async fn active_agent_started_before_install_blocks_binary_replacement() {
    let fixture = UpgradeFixture::new();
    fixture.activate_agent_before_install();

    let failure = fixture.run_upgrade().await.unwrap_err();

    assert!(failure.to_string().contains("active agent run"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}
