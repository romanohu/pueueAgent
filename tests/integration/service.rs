use std::{
    cell::{Cell, RefCell},
    ffi::OsString,
    fs,
    path::PathBuf,
    sync::Mutex,
};

use async_trait::async_trait;
use pueue_agent::{
    db::{Db, ProjectRepository},
    pueue::{PueueApi, PueueTask},
    service::{
        callback_command, enable_with, install_callback_once, CallbackRegistry, EnableOptions,
        PueueConfigCallbackRegistry, ServiceControl, ServiceDefinition, ServicePaths,
        ServiceStatus,
    },
    AppError,
};
use tempfile::TempDir;

#[derive(Default)]
struct FakeCallbackRegistry {
    current: RefCell<Option<String>>,
    writes: Cell<usize>,
}

impl FakeCallbackRegistry {
    fn with_current(current: impl Into<String>) -> Self {
        Self {
            current: RefCell::new(Some(current.into())),
            writes: Cell::new(0),
        }
    }

    fn current_value(&self) -> Option<String> {
        self.current.borrow().clone()
    }
}

impl CallbackRegistry for FakeCallbackRegistry {
    fn current_callback(&self) -> Result<Option<String>, AppError> {
        Ok(self.current.borrow().clone())
    }

    fn set_callback(&self, command: &str) -> Result<(), AppError> {
        self.writes.set(self.writes.get() + 1);
        *self.current.borrow_mut() = Some(command.to_owned());
        Ok(())
    }
}

#[derive(Default)]
struct FakeService {
    install_error: Cell<bool>,
    installed: Cell<usize>,
    status_checks: Cell<usize>,
    status: RefCell<ServiceStatus>,
}

#[derive(Default)]
struct FakePueueControl {
    groups: Mutex<Vec<String>>,
}

#[async_trait]
impl PueueApi for FakePueueControl {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        panic!("enable must not query Pueue task status")
    }

    async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
        panic!("enable must not submit Pueue tasks")
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("enable must not kill Pueue tasks")
    }

    async fn ensure_group(&self, group: &str) -> Result<(), AppError> {
        self.groups.lock().unwrap().push(group.to_owned());
        Ok(())
    }
}

impl FakeService {
    fn running() -> Self {
        Self {
            status: RefCell::new(ServiceStatus::Running),
            ..Self::default()
        }
    }

    fn failing_install() -> Self {
        Self {
            install_error: Cell::new(true),
            status: RefCell::new(ServiceStatus::NotInstalled),
            ..Self::default()
        }
    }

    fn stopped_after_install() -> Self {
        Self {
            status: RefCell::new(ServiceStatus::Stopped),
            ..Self::default()
        }
    }
}

impl ServiceControl for FakeService {
    fn install(&self, _definition: &ServiceDefinition) -> Result<(), AppError> {
        self.installed.set(self.installed.get() + 1);
        if self.install_error.get() {
            return Err(AppError::Runtime {
                operation: "fake service install",
            });
        }
        Ok(())
    }

    fn status(&self) -> Result<ServiceStatus, AppError> {
        self.status_checks.set(self.status_checks.get() + 1);
        Ok(*self.status.borrow())
    }
}

#[test]
fn callback_command_uses_release_binary_and_pueue_placeholders() {
    let paths = service_paths();

    let command = callback_command(&paths);

    assert!(command.starts_with("'/opt/pueue-agent/target/release/pueue-agent' event callback"));
    assert!(command.contains("--group '{{ group }}'"));
    assert!(command.contains("--task-id '{{ id }}'"));
    assert!(!command.contains("bin/pueue-agent' callback"));
}

#[test]
fn service_definitions_include_explicit_paths_environment_and_restart_policy() {
    let paths = service_paths();

    let systemd = ServiceDefinition::systemd(&paths).render();
    assert!(systemd.contains("ExecStart=\"/opt/pueue-agent/target/release/pueue-agent\" daemon --foreground --pueue-config \"/Users/alice/.config/pueue/pueue.yml\""));
    assert!(systemd.contains("Environment=\"PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin\""));
    assert!(systemd
        .contains("Environment=\"PUEUE_AGENT_STATE_DIR=/Users/alice/.local/state/pueue-agent\""));
    assert!(systemd.contains("WorkingDirectory=\"/Users/alice/project\""));
    assert!(systemd.contains("Restart=on-failure"));

    let launchd = ServiceDefinition::launchd(&paths).render();
    assert!(launchd.contains("<string>/opt/pueue-agent/target/release/pueue-agent</string>"));
    assert!(launchd.contains("<string>--pueue-config</string>"));
    assert!(launchd.contains("<string>/Users/alice/.config/pueue/pueue.yml</string>"));
    assert!(launchd.contains("<key>KeepAlive</key>"));
    assert!(launchd.contains("<key>WorkingDirectory</key>"));
}

#[test]
fn systemd_definition_quotes_paths_with_spaces_without_changing_launchd_argument_boundaries() {
    let paths = service_paths_with_spaces();

    let systemd = ServiceDefinition::systemd(&paths).render();
    assert!(systemd.contains(
        "ExecStart=\"/opt/Pueue Agent/target/release/pueue-agent\" daemon --foreground --pueue-config \"/Users/alice/Library/Application Support/pueue/pueue.yml\""
    ));
    assert!(systemd.contains("Environment=\"PATH=/tmp/tool dir/bin:/usr/bin:/bin\""));
    assert!(systemd.contains(
        "Environment=\"PUEUE_AGENT_STATE_DIR=/Users/alice/Library/Application Support/pueue-agent\""
    ));
    assert!(systemd.contains("WorkingDirectory=\"/Users/alice/project with spaces\""));

    let launchd = ServiceDefinition::launchd(&paths).render();
    assert!(launchd.contains("<string>/opt/Pueue Agent/target/release/pueue-agent</string>"));
    assert!(launchd
        .contains("<string>/Users/alice/Library/Application Support/pueue/pueue.yml</string>"));
    assert!(launchd.contains("<string>/Users/alice/project with spaces</string>"));
}

#[test]
fn systemd_definition_escapes_quotes_backslashes_and_percent_specifiers() {
    let paths = ServicePaths {
        release_binary: PathBuf::from("/opt/Pueue Agent 100%/bin/pueue\"agent"),
        pueue_config: PathBuf::from("/Users/alice/pueue\\profiles/pueue.yml"),
        state_dir: PathBuf::from("/Users/alice/state 100%/pueue-agent"),
        working_dir: PathBuf::from("/Users/alice/project \"quoted\""),
        path_env: "/tmp/tool 100%/bin:/usr/bin:/bin".to_owned(),
    };

    let systemd = ServiceDefinition::systemd(&paths).render();

    assert!(systemd.contains("ExecStart=\"/opt/Pueue Agent 100%%/bin/pueue\\\"agent\" daemon --foreground --pueue-config \"/Users/alice/pueue\\\\profiles/pueue.yml\""));
    assert!(systemd.contains("Environment=\"PATH=/tmp/tool 100%%/bin:/usr/bin:/bin\""));
    assert!(systemd
        .contains("Environment=\"PUEUE_AGENT_STATE_DIR=/Users/alice/state 100%%/pueue-agent\""));
    assert!(systemd.contains("WorkingDirectory=\"/Users/alice/project \\\"quoted\\\"\""));
}

#[test]
fn conflicting_existing_callback_is_rejected() {
    let registry = FakeCallbackRegistry::with_current("notify-send hi");
    let error = install_callback_once(&registry, "pueue-agent event callback")
        .expect_err("conflicting callbacks must not be overwritten");

    assert!(error.to_string().contains("conflicting Pueue callback"));
    assert_eq!(registry.current_value().as_deref(), Some("notify-send hi"));
    assert_eq!(registry.writes.get(), 0);
}

#[tokio::test]
async fn partial_enable_failure_leaves_project_and_callback_recoverable() {
    let harness = EnableHarness::new();
    let service = FakeService::failing_install();
    let registry = FakeCallbackRegistry::default();
    let pueue = FakePueueControl::default();

    let error = enable_with(&harness.db, &harness.options, &service, &registry, &pueue)
        .await
        .expect_err("service install failure should make enable fail visibly");

    assert!(error.to_string().contains("fake service install"));
    assert!(ProjectRepository::new(&harness.db)
        .find_by_root(&harness.project_root)
        .unwrap()
        .is_some());
    assert_eq!(
        registry.current_value().as_deref(),
        Some(callback_command(&harness.options.service_paths).as_str())
    );
    assert_eq!(service.installed.get(), 1);
    assert_eq!(service.status_checks.get(), 0);
}

#[tokio::test]
async fn enable_provisions_configured_pueue_group_before_callback_and_service_install() {
    let harness = EnableHarness::new();
    let service = FakeService::running();
    let registry = FakeCallbackRegistry::default();
    let pueue = FakePueueControl::default();

    enable_with(&harness.db, &harness.options, &service, &registry, &pueue)
        .await
        .unwrap();

    assert_eq!(pueue.groups.lock().unwrap().as_slice(), ["pa-project"]);
    assert_eq!(
        registry.current_value().as_deref(),
        Some(callback_command(&harness.options.service_paths).as_str())
    );
    assert_eq!(service.installed.get(), 1);
}

#[tokio::test]
async fn enable_verifies_daemon_health_before_success() {
    let harness = EnableHarness::new();
    let service = FakeService::stopped_after_install();
    let registry = FakeCallbackRegistry::default();
    let pueue = FakePueueControl::default();

    let error = enable_with(&harness.db, &harness.options, &service, &registry, &pueue)
        .await
        .expect_err("enable should fail when service is not healthy");

    assert!(error.to_string().contains("daemon health"));
    assert_eq!(service.installed.get(), 1);
    assert_eq!(service.status_checks.get(), 1);
    assert!(registry.current_value().is_some());
}

#[test]
fn pueue_config_callback_registry_updates_daemon_scoped_callback_only() {
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("pueue.yml");
    fs::write(
        &config,
        r#"shared:
  callback: "notify-send shared"
daemon:
  pause_group_on_failure: false
  callback: "old daemon callback"
client:
  callback: "notify-send client"
"#,
    )
    .unwrap();
    let registry = PueueConfigCallbackRegistry::new(&config);

    assert_eq!(
        registry.current_callback().unwrap().as_deref(),
        Some("old daemon callback")
    );
    registry
        .set_callback("pueue-agent event callback --group '{{ group }}' --task-id '{{ id }}'")
        .unwrap();

    let contents = fs::read_to_string(&config).unwrap();
    assert!(contents.contains(
        r#"shared:
  callback: "notify-send shared""#
    ));
    assert!(contents.contains(
        r#"daemon:
  pause_group_on_failure: false
  callback: "pueue-agent event callback --group '{{ group }}' --task-id '{{ id }}'""#
    ));
    assert!(contents.contains(
        r#"client:
  callback: "notify-send client""#
    ));
}

#[test]
fn pueue_config_callback_registry_rejects_non_daemon_callback_conflicts() {
    let temp = TempDir::new().unwrap();
    let config = temp.path().join("pueue.yml");
    fs::write(
        &config,
        r#"client:
  callback: "notify-send client"
"#,
    )
    .unwrap();
    let registry = PueueConfigCallbackRegistry::new(&config);

    let error = registry
        .set_callback("pueue-agent event callback")
        .expect_err("non-daemon callback should require manual resolution");

    assert!(error.to_string().contains("callback exists outside daemon"));
    assert_eq!(
        fs::read_to_string(&config).unwrap(),
        r#"client:
  callback: "notify-send client"
"#
    );
}

struct EnableHarness {
    _temp: TempDir,
    db: Db,
    project_root: PathBuf,
    options: EnableOptions,
}

impl EnableHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(project_root.join(".pueue-agent")).unwrap();
        fs::write(
            project_root.join(".pueue-agent/config.toml"),
            r#"
project_id = "project-a"
pueue_group = "pa-project"

[agent]
program = "/bin/echo"
args = ["{prompt}"]
timeout_minutes = 1
max_retries = 1

[check]
interval_minutes = 10
deep_check_every = 6
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
        )
        .unwrap();

        let options = EnableOptions {
            project_root: project_root.clone(),
            service_paths: service_paths(),
            now: 200,
        };

        Self {
            _temp: temp,
            db,
            project_root,
            options,
        }
    }
}

fn service_paths() -> ServicePaths {
    ServicePaths {
        release_binary: PathBuf::from("/opt/pueue-agent/target/release/pueue-agent"),
        pueue_config: PathBuf::from("/Users/alice/.config/pueue/pueue.yml"),
        state_dir: PathBuf::from("/Users/alice/.local/state/pueue-agent"),
        working_dir: PathBuf::from("/Users/alice/project"),
        path_env: "/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin".to_owned(),
    }
}

fn service_paths_with_spaces() -> ServicePaths {
    ServicePaths {
        release_binary: PathBuf::from("/opt/Pueue Agent/target/release/pueue-agent"),
        pueue_config: PathBuf::from("/Users/alice/Library/Application Support/pueue/pueue.yml"),
        state_dir: PathBuf::from("/Users/alice/Library/Application Support/pueue-agent"),
        working_dir: PathBuf::from("/Users/alice/project with spaces"),
        path_env: "/tmp/tool dir/bin:/usr/bin:/bin".to_owned(),
    }
}
