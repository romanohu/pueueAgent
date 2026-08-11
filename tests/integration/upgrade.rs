use std::{
    cell::{Cell, RefCell},
    ffi::{OsStr, OsString},
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use pueue_agent::{
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    models::{AgentRunStatus, EventKind, NewAgentRun, NewEvent, NewProject},
    service::{ServiceControl, ServiceDefinition, ServiceStatus},
    upgrade::{
        resolve_source_root, validate_checkout, validate_checkout_with, GitCommandOutput,
        GitCommandRunner, UpgradeCommandOutput, UpgradeCommandRunner, UpgradeRollback,
        UpgradeRunner,
    },
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

struct UpgradeFixture {
    temp: TempDir,
    source: PathBuf,
    installed_binary: PathBuf,
    db: Db,
    commands: FakeCommandRunner,
    service: FakeServiceRecorder,
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
            db,
            commands: FakeCommandRunner::default(),
            service: FakeServiceRecorder::default(),
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
        fixture.service.set_status(ServiceStatus::Stopped);
        fixture
    }

    fn pueue_failure() -> Self {
        let fixture = Self::new();
        fixture.commands.fail_pueue_status.set(true);
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

    fn service_calls(&self) -> Vec<String> {
        self.service.actions.borrow().clone()
    }

    async fn run_upgrade(&self) -> Result<pueue_agent::upgrade::UpgradeReport, AppError> {
        UpgradeRunner::new(self.options(), &self.db, &self.service, &self.commands).run()
            .await
    }

    fn options(&self) -> pueue_agent::upgrade::UpgradeOptions {
        pueue_agent::upgrade::UpgradeOptions {
            source: Some(self.source.clone()),
            json: false,
            branch: "main".to_owned(),
            remote: "origin".to_owned(),
            release_binary: self.installed_binary.clone(),
            pueue_binary: PathBuf::from("pueue"),
            pueue_config: None,
        }
    }

    fn make_noop(&self) {
        self.commands.no_update.set(true);
    }

    fn register_active_agent_run(&self) {
        let project_root = self.temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                "project-a",
                &project_root,
                "pa-project-a",
                project_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        let event = EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::TaskFinished,
                "upgrade-active-run",
                json!({"task_id": 42}),
                100,
                100,
            ))
            .unwrap();
        AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Running,
                100,
                self.temp.path().join("active-run.log"),
            ))
            .unwrap();
    }

    fn write_lock(&self, pid: u32) {
        fs::write(self.state_dir().join("upgrade.lock"), pid.to_string()).unwrap();
    }

    fn state_dir(&self) -> &Path {
        self.db.path().parent().unwrap()
    }
}

#[derive(Default)]
struct FakeCommandRunner {
    invocations: RefCell<Vec<Vec<String>>>,
    command_invocations: RefCell<Vec<(String, Vec<String>)>>,
    fail_tests: Cell<bool>,
    fail_build: Cell<bool>,
    fail_pueue_status: Cell<bool>,
    no_update: Cell<bool>,
    merged: Cell<bool>,
}

impl GitCommandRunner for FakeCommandRunner {
    fn run(&self, _source: &Path, args: &[&str]) -> Result<GitCommandOutput, AppError> {
        self.invocations
            .borrow_mut()
            .push(args.iter().map(|arg| (*arg).to_owned()).collect());

        let (success, stdout) = match args {
            ["status", "--porcelain"] => (true, ""),
            ["branch", "--show-current"] => (true, "main"),
            ["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"] => {
                (true, "origin/main")
            }
            ["fetch", "origin", "main"] => (true, ""),
            ["rev-parse", "HEAD"] if self.merged.get() => (true, "new-revision"),
            ["rev-parse", "HEAD"] => (true, "old-revision"),
            ["rev-parse", "origin/main"] if self.no_update.get() => (true, "old-revision"),
            ["rev-parse", "origin/main"] => (true, "new-revision"),
            ["merge-base", "--is-ancestor", "HEAD", "origin/main"] => (true, ""),
            ["merge", "--ff-only", "origin/main"] => {
                self.merged.set(true);
                (true, "")
            }
            _ => panic!("unexpected git arguments: {args:?}"),
        };

        Ok(GitCommandOutput {
            success,
            stdout: stdout.to_owned(),
            stderr: String::new(),
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
                Ok(UpgradeCommandOutput::failure("build failure"))
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
                Ok(UpgradeCommandOutput::success())
            }
            ("cargo", Some("test")) => Ok(UpgradeCommandOutput::success()),
            ("pueue", Some("status")) if self.fail_pueue_status.get() => {
                Ok(UpgradeCommandOutput::failure("Pueue unavailable"))
            }
            ("pueue", Some("status")) => Ok(UpgradeCommandOutput::success()),
            _ => panic!("unexpected upgrade command: {program} {arguments:?}"),
        }
    }
}

struct FakeServiceRecorder {
    actions: RefCell<Vec<String>>,
    status: Cell<ServiceStatus>,
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
            status: Cell::new(ServiceStatus::Running),
        }
    }

    fn set_status(&self, status: ServiceStatus) {
        self.status.set(status);
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
        panic!("upgrade must not stop the service separately")
    }

    fn restart(&self) -> Result<(), AppError> {
        self.actions.borrow_mut().push("restart".to_owned());
        Ok(())
    }

    fn status(&self) -> Result<ServiceStatus, AppError> {
        Ok(self.status.get())
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
    assert_eq!(fixture.service_calls(), ["restart"]);
    assert!(fixture.commands.invocations.borrow().iter().any(|arguments| {
        arguments == &vec!["merge".to_owned(), "--ff-only".to_owned(), "origin/main".to_owned()]
    }));
    assert!(fixture.commands.command_invocations.borrow().iter().any(
        |(program, arguments)| {
            program == "cargo"
                && arguments
                    == &vec!["test".to_owned(), "--all-targets".to_owned()]
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
    assert_eq!(
        pueue_commands,
        vec![(
            "pueue".to_owned(),
            vec!["status".to_owned(), "--json".to_owned()]
        )]
    );
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
async fn test_failure_leaves_installed_binary_and_service_unchanged() {
    let fixture = UpgradeFixture::test_failure();

    let error = fixture.run_upgrade().await.unwrap_err();

    assert!(error.to_string().contains("test"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn build_failure_leaves_installed_binary_and_service_unchanged() {
    let fixture = UpgradeFixture::build_failure();

    let report = fixture.run_upgrade().await.unwrap_err();

    assert!(report.to_string().contains("build"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), Vec::<String>::new());
}

#[tokio::test]
async fn service_health_failure_restores_previous_binary() {
    let fixture = UpgradeFixture::service_failure();

    let error = fixture.run_upgrade().await.unwrap_err();

    assert!(error.to_string().contains("rollback"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), ["restart", "restart"]);
}

#[tokio::test]
async fn pueue_health_failure_restores_previous_binary_without_touching_experiment_tasks() {
    let fixture = UpgradeFixture::pueue_failure();

    let error = fixture.run_upgrade().await.unwrap_err();

    assert!(error.to_string().contains("rollback"));
    assert_eq!(fixture.installed_binary(), UpgradeFixture::old_binary_bytes());
    assert_eq!(fixture.service_calls(), ["restart", "restart"]);
    assert!(fixture
        .commands
        .command_invocations
        .borrow()
        .iter()
        .filter(|(program, _)| program == "pueue")
        .all(|(_, arguments)| {
            arguments == &vec!["status".to_owned(), "--json".to_owned()]
        }));
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
