use std::{
    cell::{Cell, RefCell},
    fs,
    path::PathBuf,
};

use pueue_agent::{
    db::{Db, ProjectRepository},
    service::{
        callback_command, enable_with, install_callback_once, CallbackRegistry, EnableOptions,
        ServiceControl, ServiceDefinition, ServicePaths, ServiceStatus,
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

impl FakeService {
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
    assert!(systemd.contains("ExecStart=/opt/pueue-agent/target/release/pueue-agent daemon --foreground --pueue-config /Users/alice/.config/pueue/pueue.yml"));
    assert!(systemd.contains("Environment=PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin"));
    assert!(
        systemd.contains("Environment=PUEUE_AGENT_STATE_DIR=/Users/alice/.local/state/pueue-agent")
    );
    assert!(systemd.contains("WorkingDirectory=/Users/alice/project"));
    assert!(systemd.contains("Restart=on-failure"));

    let launchd = ServiceDefinition::launchd(&paths).render();
    assert!(launchd.contains("<string>/opt/pueue-agent/target/release/pueue-agent</string>"));
    assert!(launchd.contains("<string>--pueue-config</string>"));
    assert!(launchd.contains("<string>/Users/alice/.config/pueue/pueue.yml</string>"));
    assert!(launchd.contains("<key>KeepAlive</key>"));
    assert!(launchd.contains("<key>WorkingDirectory</key>"));
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

#[test]
fn partial_enable_failure_leaves_project_and_callback_recoverable() {
    let harness = EnableHarness::new();
    let service = FakeService::failing_install();
    let registry = FakeCallbackRegistry::default();

    let error = enable_with(&harness.db, &harness.options, &service, &registry)
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

#[test]
fn enable_verifies_daemon_health_before_success() {
    let harness = EnableHarness::new();
    let service = FakeService::stopped_after_install();
    let registry = FakeCallbackRegistry::default();

    let error = enable_with(&harness.db, &harness.options, &service, &registry)
        .expect_err("enable should fail when service is not healthy");

    assert!(error.to_string().contains("daemon health"));
    assert_eq!(service.installed.get(), 1);
    assert_eq!(service.status_checks.get(), 1);
    assert!(registry.current_value().is_some());
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
