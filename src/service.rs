use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{
    config,
    db::{Db, ProjectRepository},
    models::NewProject,
    output::bounded_redacted_text,
    paths,
    pueue::PueueApi,
    AppError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePaths {
    pub release_binary: PathBuf,
    pub pueue_config: PathBuf,
    pub state_dir: PathBuf,
    pub working_dir: PathBuf,
    pub path_env: String,
}

impl ServicePaths {
    pub fn from_environment(
        project_root: &Path,
        pueue_config: Option<PathBuf>,
    ) -> Result<Self, AppError> {
        let state_db = paths::state_db_path()?;
        let state_dir =
            state_db
                .parent()
                .map(Path::to_path_buf)
                .ok_or(AppError::Configuration {
                    field: "state_db_path",
                })?;
        let pueue_config = match pueue_config {
            Some(path) => path,
            None => {
                let home = env::var_os("HOME")
                    .map(PathBuf::from)
                    .ok_or(AppError::Configuration { field: "HOME" })?;
                home.join(".config/pueue/pueue.yml")
            }
        };
        let release_binary = release_binary_path()?;
        let path_env = env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_owned());

        Ok(Self {
            release_binary,
            pueue_config,
            state_dir,
            working_dir: project_root.to_path_buf(),
            path_env,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnableOptions {
    pub project_root: PathBuf,
    pub service_paths: ServicePaths,
    pub now: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServiceStatus {
    Running,
    Stopped,
    #[default]
    NotInstalled,
}

pub trait ServiceControl {
    fn install(&self, definition: &ServiceDefinition) -> Result<(), AppError>;

    fn start(&self) -> Result<(), AppError>;

    fn stop(&self) -> Result<(), AppError>;

    fn restart(&self) -> Result<(), AppError>;

    fn status(&self) -> Result<ServiceStatus, AppError>;
}

pub trait CallbackRegistry {
    fn current_callback(&self) -> Result<Option<String>, AppError>;

    fn set_callback(&self, command: &str) -> Result<(), AppError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDefinition {
    platform: ServicePlatform,
    paths: ServicePaths,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServicePlatform {
    Systemd,
    Launchd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchdAgent {
    domain: String,
    plist: PathBuf,
}

impl LaunchdAgent {
    pub fn new(domain: impl Into<String>, plist: impl Into<PathBuf>) -> Self {
        Self {
            domain: domain.into(),
            plist: plist.into(),
        }
    }

    fn service_target(&self) -> String {
        format!("{}/com.pueue-agent", self.domain)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceCommandOutput {
    success: bool,
    status: i32,
    details: String,
}

impl ServiceCommandOutput {
    pub fn success() -> Self {
        Self {
            success: true,
            status: 0,
            details: String::new(),
        }
    }

    pub fn failure(status: i32, details: impl Into<String>) -> Self {
        Self {
            success: false,
            status,
            details: details.into(),
        }
    }
}

pub trait ServiceCommandRunner {
    fn run(&self, program: &str, args: &[&str]) -> Result<ServiceCommandOutput, AppError>;
}

struct ProcessServiceCommandRunner;

impl ServiceCommandRunner for ProcessServiceCommandRunner {
    fn run(&self, program: &str, args: &[&str]) -> Result<ServiceCommandOutput, AppError> {
        let output = Command::new(program)
            .args(args)
            .output()
            .map_err(|source| AppError::Io {
                operation: "run platform service manager",
                source,
            })?;
        if output.status.success() {
            Ok(ServiceCommandOutput::success())
        } else {
            Ok(ServiceCommandOutput::failure(
                output.status.code().unwrap_or(-1),
                format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            ))
        }
    }
}

impl ServiceDefinition {
    pub fn current_platform(paths: &ServicePaths) -> Self {
        if cfg!(target_os = "macos") {
            Self::launchd(paths)
        } else {
            Self::systemd(paths)
        }
    }

    pub fn systemd(paths: &ServicePaths) -> Self {
        Self {
            platform: ServicePlatform::Systemd,
            paths: paths.clone(),
        }
    }

    pub fn launchd(paths: &ServicePaths) -> Self {
        Self {
            platform: ServicePlatform::Launchd,
            paths: paths.clone(),
        }
    }

    pub fn render(&self) -> String {
        match self.platform {
            ServicePlatform::Systemd => render_systemd(&self.paths),
            ServicePlatform::Launchd => render_launchd(&self.paths),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ServiceManager;

impl ServiceControl for ServiceManager {
    fn install(&self, definition: &ServiceDefinition) -> Result<(), AppError> {
        let rendered = definition.render();
        match definition.platform {
            ServicePlatform::Systemd => install_systemd(&rendered),
            ServicePlatform::Launchd => install_launchd(&rendered),
        }
    }

    fn status(&self) -> Result<ServiceStatus, AppError> {
        if cfg!(target_os = "macos") {
            launchd_status()
        } else {
            systemd_status()
        }
    }

    fn start(&self) -> Result<(), AppError> {
        let runner = ProcessServiceCommandRunner;
        if cfg!(target_os = "macos") {
            let agent = launchd_agent()?;
            self.start_with(ServicePlatform::Launchd, &runner, Some(&agent))
        } else {
            self.start_with(ServicePlatform::Systemd, &runner, None)
        }
    }

    fn stop(&self) -> Result<(), AppError> {
        let runner = ProcessServiceCommandRunner;
        if cfg!(target_os = "macos") {
            let agent = launchd_agent()?;
            self.stop_with(ServicePlatform::Launchd, &runner, Some(&agent))
        } else {
            self.stop_with(ServicePlatform::Systemd, &runner, None)
        }
    }

    fn restart(&self) -> Result<(), AppError> {
        let runner = ProcessServiceCommandRunner;
        if cfg!(target_os = "macos") {
            let agent = launchd_agent()?;
            self.restart_with(ServicePlatform::Launchd, &runner, Some(&agent))
        } else {
            self.restart_with(ServicePlatform::Systemd, &runner, None)
        }
    }
}

impl ServiceManager {
    pub fn start_with(
        &self,
        platform: ServicePlatform,
        runner: &impl ServiceCommandRunner,
        launchd: Option<&LaunchdAgent>,
    ) -> Result<(), AppError> {
        match platform {
            ServicePlatform::Systemd => run_lifecycle_command(
                runner,
                "systemctl",
                &["--user", "start", "pueue-agent.service"],
            ),
            ServicePlatform::Launchd => {
                let agent = required_launchd_agent(launchd)?;
                let service = agent.service_target();
                let output = runner.run("launchctl", &["kickstart", service.as_str()])?;
                if output.success {
                    Ok(())
                } else if launchd_service_is_not_loaded(&output.details) {
                    run_lifecycle_command(
                        runner,
                        "launchctl",
                        &[
                            "bootstrap",
                            agent.domain.as_str(),
                            agent.plist.to_str().ok_or(AppError::Configuration {
                                field: "launchd.plist",
                            })?,
                        ],
                    )
                } else {
                    lifecycle_command_error("launchctl", output.status)
                }
            }
        }
    }

    pub fn stop_with(
        &self,
        platform: ServicePlatform,
        runner: &impl ServiceCommandRunner,
        launchd: Option<&LaunchdAgent>,
    ) -> Result<(), AppError> {
        match platform {
            ServicePlatform::Systemd => run_lifecycle_command(
                runner,
                "systemctl",
                &["--user", "stop", "pueue-agent.service"],
            ),
            ServicePlatform::Launchd => {
                let agent = required_launchd_agent(launchd)?;
                let service = agent.service_target();
                run_lifecycle_command(runner, "launchctl", &["bootout", service.as_str()])
            }
        }
    }

    pub fn restart_with(
        &self,
        platform: ServicePlatform,
        runner: &impl ServiceCommandRunner,
        launchd: Option<&LaunchdAgent>,
    ) -> Result<(), AppError> {
        match platform {
            ServicePlatform::Systemd => run_lifecycle_command(
                runner,
                "systemctl",
                &["--user", "restart", "pueue-agent.service"],
            ),
            ServicePlatform::Launchd => {
                let agent = required_launchd_agent(launchd)?;
                let service = agent.service_target();
                let output = runner.run(
                    "launchctl",
                    &["kickstart", "-k", service.as_str()],
                )?;
                if output.success {
                    Ok(())
                } else if launchd_service_is_not_loaded(&output.details) {
                    run_lifecycle_command(
                        runner,
                        "launchctl",
                        &[
                            "bootstrap",
                            agent.domain.as_str(),
                            agent.plist.to_str().ok_or(AppError::Configuration {
                                field: "launchd.plist",
                            })?,
                        ],
                    )
                } else {
                    lifecycle_command_error("launchctl", output.status)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PueueConfigCallbackRegistry {
    config_path: PathBuf,
}

impl PueueConfigCallbackRegistry {
    pub fn new(config_path: impl Into<PathBuf>) -> Self {
        Self {
            config_path: config_path.into(),
        }
    }
}

impl CallbackRegistry for PueueConfigCallbackRegistry {
    fn current_callback(&self) -> Result<Option<String>, AppError> {
        let contents = fs::read_to_string(&self.config_path).map_err(|source| AppError::Io {
            operation: "read Pueue configuration",
            source,
        })?;
        let scan = scan_callbacks(&contents);
        if scan.daemon_callback_line.is_none() && scan.outside_callback_line.is_some() {
            return Err(AppError::Message {
                message: "Pueue callback exists outside daemon; refusing to edit configuration"
                    .to_owned(),
            });
        }
        Ok(scan
            .daemon_callback_line
            .and_then(|line| contents.lines().nth(line))
            .and_then(parse_callback_value))
    }

    fn set_callback(&self, command: &str) -> Result<(), AppError> {
        let contents = fs::read_to_string(&self.config_path).map_err(|source| AppError::Io {
            operation: "read Pueue configuration",
            source,
        })?;
        let replacement = replace_or_insert_daemon_callback(&contents, command)?;
        fs::write(&self.config_path, replacement).map_err(|source| AppError::Io {
            operation: "write Pueue configuration",
            source,
        })
    }
}

pub fn callback_command(paths: &ServicePaths) -> String {
    format!(
        "{} event callback --group '{}' --task-id '{}'",
        shell_quote(&paths.release_binary),
        "{{ group }}",
        "{{ id }}"
    )
}

pub fn install_callback_once(
    registry: &impl CallbackRegistry,
    expected_command: &str,
) -> Result<(), AppError> {
    match registry.current_callback()? {
        None => registry.set_callback(expected_command),
        Some(existing) if existing == expected_command => Ok(()),
        Some(_) => Err(AppError::Message {
            message: "conflicting Pueue callback already exists; refusing to overwrite it"
                .to_owned(),
        }),
    }
}

pub async fn enable_with(
    db: &Db,
    options: &EnableOptions,
    service: &impl ServiceControl,
    callbacks: &impl CallbackRegistry,
    pueue: &impl PueueApi,
) -> Result<(), AppError> {
    let config_path = options.project_root.join(".pueue-agent/config.toml");
    let project_config = config::load(&config_path)?;
    register_project_if_needed(db, options, &config_path, &project_config)?;

    pueue.ensure_group(&project_config.pueue_group).await?;

    let command = callback_command(&options.service_paths);
    install_callback_once(callbacks, &command)?;

    let definition = ServiceDefinition::current_platform(&options.service_paths);
    service.install(&definition)?;

    match service.status()? {
        ServiceStatus::Running => Ok(()),
        ServiceStatus::Stopped | ServiceStatus::NotInstalled => Err(AppError::Message {
            message: "daemon health check failed after service installation".to_owned(),
        }),
    }
}

fn register_project_if_needed(
    db: &Db,
    options: &EnableOptions,
    config_path: &Path,
    project_config: &config::ProjectConfig,
) -> Result<(), AppError> {
    let repository = ProjectRepository::new(db);
    if let Some(existing) = repository.find_by_root(&options.project_root)? {
        if existing.project_id == project_config.project_id
            && existing.pueue_group == project_config.pueue_group
        {
            return Ok(());
        }
        return Err(AppError::DatabaseConflict { field: "root_path" });
    }

    repository.register(&NewProject::new(
        &project_config.project_id,
        &options.project_root,
        &project_config.pueue_group,
        config_path,
        options.now,
    ))?;
    Ok(())
}

fn render_systemd(paths: &ServicePaths) -> String {
    format!(
        r#"[Unit]
Description=pueue-agent supervisor
After=default.target

[Service]
Type=simple
ExecStart={} daemon --foreground --pueue-config {}
Environment={}
Environment={}
WorkingDirectory={}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"#,
        systemd_quote(&paths.release_binary.display().to_string()),
        systemd_quote(&paths.pueue_config.display().to_string()),
        systemd_quote(&format!("PATH={}", paths.path_env)),
        systemd_quote(&format!(
            "PUEUE_AGENT_STATE_DIR={}",
            paths.state_dir.display()
        )),
        systemd_quote(&paths.working_dir.display().to_string()),
    )
}

fn render_launchd(paths: &ServicePaths) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.pueue-agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>{}</string>
    <string>daemon</string>
    <string>--foreground</string>
    <string>--pueue-config</string>
    <string>{}</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>{}</string>
    <key>PUEUE_AGENT_STATE_DIR</key>
    <string>{}</string>
  </dict>
  <key>WorkingDirectory</key>
  <string>{}</string>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
"#,
        xml_escape(&paths.release_binary.display().to_string()),
        xml_escape(&paths.pueue_config.display().to_string()),
        xml_escape(&paths.path_env),
        xml_escape(&paths.state_dir.display().to_string()),
        xml_escape(&paths.working_dir.display().to_string()),
    )
}

fn install_systemd(rendered: &str) -> Result<(), AppError> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(AppError::Configuration { field: "HOME" })?;
    let service_dir = home.join(".config/systemd/user");
    fs::create_dir_all(&service_dir).map_err(|source| AppError::Io {
        operation: "create systemd user service directory",
        source,
    })?;
    fs::write(service_dir.join("pueue-agent.service"), rendered).map_err(|source| {
        AppError::Io {
            operation: "write systemd user service",
            source,
        }
    })?;
    run_service_command("systemctl", &["--user", "daemon-reload"])?;
    run_service_command(
        "systemctl",
        &["--user", "enable", "--now", "pueue-agent.service"],
    )
}

fn install_launchd(rendered: &str) -> Result<(), AppError> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(AppError::Configuration { field: "HOME" })?;
    let agent_dir = home.join("Library/LaunchAgents");
    fs::create_dir_all(&agent_dir).map_err(|source| AppError::Io {
        operation: "create launchd user agent directory",
        source,
    })?;
    let plist = agent_dir.join("com.pueue-agent.plist");
    fs::write(&plist, rendered).map_err(|source| AppError::Io {
        operation: "write launchd user agent",
        source,
    })?;
    let domain = launchd_gui_domain()?;
    run_service_command(
        "launchctl",
        &[
            "bootstrap",
            domain.as_str(),
            plist.to_str().ok_or(AppError::Configuration {
                field: "launchd.plist",
            })?,
        ],
    )
}

fn systemd_status() -> Result<ServiceStatus, AppError> {
    let load_state_output = Command::new("systemctl")
        .args([
            "--user",
            "show",
            "pueue-agent.service",
            "--property=LoadState",
            "--value",
        ])
        .output()
        .map_err(|source| AppError::Io {
            operation: "query systemd user service",
            source,
        })?;
    let load_state_details = bounded_redacted_text(&String::from_utf8_lossy(
        &load_state_output.stderr,
    ));

    if load_state_output.status.success()
        && systemd_state_from_output(&load_state_output.stdout) == "not-found"
    {
        return Ok(ServiceStatus::NotInstalled);
    }

    let active_state_output = Command::new("systemctl")
        .args(["--user", "is-active", "pueue-agent.service"])
        .output()
        .map_err(|source| AppError::Io {
            operation: "query systemd user service",
            source,
        })?;
    let active_state_details = bounded_redacted_text(&String::from_utf8_lossy(
        &active_state_output.stderr,
    ));

    if load_state_output.status.success() {
        Ok(systemd_status_from_load_state_output(
            true,
            &load_state_output.stdout,
            active_state_output.status.success(),
            &active_state_output.stdout,
        ))
    } else {
        let details = format!("{load_state_details}{active_state_details}");
        Ok(systemd_status_from_output(
            active_state_output.status.success(),
            &active_state_output.stdout,
            &details,
        ))
    }
}

pub fn systemd_status_from_load_state_output(
    load_state_command_succeeded: bool,
    load_state_stdout: &[u8],
    active_state_command_succeeded: bool,
    active_state_stdout: &[u8],
) -> ServiceStatus {
    if !load_state_command_succeeded {
        return ServiceStatus::Stopped;
    }

    match systemd_state_from_output(load_state_stdout).as_str() {
        "not-found" => ServiceStatus::NotInstalled,
        "loaded"
            if active_state_command_succeeded
                && systemd_state_from_output(active_state_stdout) == "active" =>
        {
            ServiceStatus::Running
        }
        _ => ServiceStatus::Stopped,
    }
}

fn systemd_state_from_output(stdout: &[u8]) -> String {
    String::from_utf8_lossy(stdout)
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

pub fn systemd_status_from_output(
    command_succeeded: bool,
    stdout: &[u8],
    details: &str,
) -> ServiceStatus {
    let state = systemd_state_from_output(stdout);

    match state.as_str() {
        "active" if command_succeeded => ServiceStatus::Running,
        "unknown" => ServiceStatus::NotInstalled,
        "inactive" | "failed" | "activating" | "deactivating" | "maintenance" => {
            ServiceStatus::Stopped
        }
        _ if systemd_unit_is_not_installed(details) => ServiceStatus::NotInstalled,
        _ => ServiceStatus::Stopped,
    }
}

fn launchd_status() -> Result<ServiceStatus, AppError> {
    let agent = launchd_agent()?;
    let service = agent.service_target();
    let output = Command::new("launchctl")
        .args(["print", service.as_str()])
        .output()
        .map_err(|source| AppError::Io {
            operation: "query launchd user service",
            source,
        })?;
    let plist_exists = agent.plist.exists();
    Ok(launchd_status_from_output(
        output.status.success(),
        &output.stdout,
        plist_exists,
    ))
}

pub fn launchd_status_from_output(
    command_succeeded: bool,
    stdout: &[u8],
    plist_exists: bool,
) -> ServiceStatus {
    if !command_succeeded {
        return if plist_exists {
            ServiceStatus::Stopped
        } else {
            ServiceStatus::NotInstalled
        };
    }

    if String::from_utf8_lossy(stdout).lines().any(|line| {
        let Some((key, value)) = line.split_once('=') else {
            return false;
        };
        key.trim().eq_ignore_ascii_case("state")
            && value.trim().eq_ignore_ascii_case("running")
    }) {
        ServiceStatus::Running
    } else {
        ServiceStatus::Stopped
    }
}

fn launchd_agent() -> Result<LaunchdAgent, AppError> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(AppError::Configuration { field: "HOME" })?;
    let plist = home.join("Library/LaunchAgents/com.pueue-agent.plist");
    Ok(LaunchdAgent::new(launchd_gui_domain()?, plist))
}

fn required_launchd_agent(agent: Option<&LaunchdAgent>) -> Result<&LaunchdAgent, AppError> {
    agent.ok_or(AppError::Configuration {
        field: "launchd agent",
    })
}

fn launchd_service_is_not_loaded(details: &str) -> bool {
    details.to_lowercase().contains("could not find service")
}

fn systemd_unit_is_not_installed(details: &str) -> bool {
    let details = details.to_ascii_lowercase();
    details.contains("could not be found")
        || details.contains("not-found")
        || details.contains("not loaded")
}

fn run_lifecycle_command(
    runner: &impl ServiceCommandRunner,
    program: &str,
    args: &[&str],
) -> Result<(), AppError> {
    let output = runner.run(program, args)?;
    if output.success {
        Ok(())
    } else {
        lifecycle_command_error(program, output.status)
    }
}

fn lifecycle_command_error(program: &str, status: i32) -> Result<(), AppError> {
    Err(AppError::Message {
        message: format!("{program} service command failed with status {status}"),
    })
}

fn launchd_gui_domain() -> Result<String, AppError> {
    let output = Command::new("id")
        .arg("-u")
        .output()
        .map_err(|source| AppError::Io {
            operation: "resolve current user ID",
            source,
        })?;
    if !output.status.success() {
        return Err(AppError::Message {
            message: format!("id -u failed with status {}", output.status),
        });
    }
    let uid = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if uid.is_empty() {
        return Err(AppError::Runtime {
            operation: "resolve current user ID",
        });
    }
    Ok(format!("gui/{uid}"))
}

fn run_service_command(program: &str, args: &[&str]) -> Result<(), AppError> {
    let output = Command::new(program)
        .args(args)
        .output()
        .map_err(|source| AppError::Io {
            operation: "run platform service manager",
            source,
        })?;
    if output.status.success() {
        Ok(())
    } else {
        service_command_error(program, output.status)
    }
}

fn service_command_error(program: &str, status: std::process::ExitStatus) -> Result<(), AppError> {
    Err(AppError::Message {
        message: format!("{program} service command failed with status {status}"),
    })
}

fn parse_callback_value(line: &str) -> Option<String> {
    let value = line.split_once(':')?.1.trim();
    if value == "null" || value == "~" || value.is_empty() {
        return None;
    }
    if let Some(stripped) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        return Some(stripped.replace("\\\"", "\"").replace("\\\\", "\\"));
    }
    Some(value.to_owned())
}

#[derive(Debug, Default)]
struct CallbackScan {
    daemon_line: Option<usize>,
    daemon_indent: usize,
    daemon_end_line: usize,
    daemon_callback_line: Option<usize>,
    outside_callback_line: Option<usize>,
}

fn scan_callbacks(contents: &str) -> CallbackScan {
    let lines = contents.lines().collect::<Vec<_>>();
    let mut scan = CallbackScan {
        daemon_end_line: lines.len(),
        ..CallbackScan::default()
    };

    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if key_name(trimmed) == Some("daemon") {
            scan.daemon_line = Some(index);
            scan.daemon_indent = line.len() - trimmed.len();
            break;
        }
    }

    if let Some(daemon_line) = scan.daemon_line {
        for (index, line) in lines.iter().enumerate().skip(daemon_line + 1) {
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let indent = line.len() - trimmed.len();
            if indent <= scan.daemon_indent {
                scan.daemon_end_line = index;
                break;
            }
            if key_name(trimmed) == Some("callback") {
                scan.daemon_callback_line = Some(index);
            }
        }
    }

    for (index, line) in lines.iter().enumerate() {
        if Some(index) == scan.daemon_callback_line {
            continue;
        }
        if scan
            .daemon_line
            .is_some_and(|daemon_line| index > daemon_line && index < scan.daemon_end_line)
        {
            continue;
        }
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if key_name(trimmed) == Some("callback") {
            scan.outside_callback_line = Some(index);
            break;
        }
    }

    scan
}

fn key_name(trimmed_line: &str) -> Option<&str> {
    let (key, _) = trimmed_line.split_once(':')?;
    let key = key.trim();
    if key.is_empty() || key.starts_with('#') {
        None
    } else {
        Some(key)
    }
}

fn replace_or_insert_daemon_callback(contents: &str, command: &str) -> Result<String, AppError> {
    let scan = scan_callbacks(contents);
    if scan.daemon_callback_line.is_none() && scan.outside_callback_line.is_some() {
        return Err(AppError::Message {
            message: "Pueue callback exists outside daemon; refusing to edit configuration"
                .to_owned(),
        });
    }

    let escaped = command.replace('\\', "\\\\").replace('"', "\\\"");
    let callback_line = if scan.daemon_line.is_some() {
        format!(
            "{}callback: \"{}\"",
            " ".repeat(scan.daemon_indent + 2),
            escaped
        )
    } else {
        format!("  callback: \"{}\"", escaped)
    };
    let mut lines = contents.lines().map(str::to_owned).collect::<Vec<_>>();

    if let Some(line) = scan.daemon_callback_line {
        lines[line] = callback_line;
    } else if scan.daemon_line.is_some() {
        lines.insert(scan.daemon_end_line, callback_line);
    } else {
        if !lines.is_empty() {
            lines.push("daemon:".to_owned());
        } else {
            lines = vec!["daemon:".to_owned()];
        }
        lines.push(callback_line);
    }

    let mut output = lines.join("\n");
    if contents.ends_with('\n') || !output.is_empty() {
        output.push('\n');
    }
    Ok(output)
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\"'\"'"))
}

fn systemd_quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '%' => escaped.push_str("%%"),
            _ => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn release_binary_path() -> Result<PathBuf, AppError> {
    let current = env::current_exe().map_err(|source| AppError::Io {
        operation: "resolve current executable",
        source,
    })?;
    let mut components = current.components().collect::<Vec<_>>();
    if let Some(index) = components
        .iter()
        .position(|component| component.as_os_str() == "debug")
    {
        let mut path = PathBuf::new();
        for component in components.drain(..index) {
            path.push(component.as_os_str());
        }
        path.push("release");
        path.push("pueue-agent");
        return Ok(path);
    }
    Ok(current)
}
