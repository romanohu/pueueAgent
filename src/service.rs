use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{
    config,
    db::{Db, ProjectRepository},
    models::NewProject,
    paths, AppError,
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
enum ServicePlatform {
    Systemd,
    Launchd,
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
        Ok(find_callback_line(&contents).and_then(parse_callback_value))
    }

    fn set_callback(&self, command: &str) -> Result<(), AppError> {
        let contents = fs::read_to_string(&self.config_path).map_err(|source| AppError::Io {
            operation: "read Pueue configuration",
            source,
        })?;
        let replacement = replace_or_append_callback(&contents, command);
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

pub fn enable_with(
    db: &Db,
    options: &EnableOptions,
    service: &impl ServiceControl,
    callbacks: &impl CallbackRegistry,
) -> Result<(), AppError> {
    let config_path = options.project_root.join(".pueue-agent/config.toml");
    let project_config = config::load(&config_path)?;
    register_project_if_needed(db, options, &config_path, &project_config)?;

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
Environment=PATH={}
Environment=PUEUE_AGENT_STATE_DIR={}
WorkingDirectory={}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"#,
        paths.release_binary.display(),
        paths.pueue_config.display(),
        paths.path_env,
        paths.state_dir.display(),
        paths.working_dir.display(),
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
    let output = Command::new("systemctl")
        .args(["--user", "is-active", "pueue-agent.service"])
        .output()
        .map_err(|source| AppError::Io {
            operation: "query systemd user service",
            source,
        })?;
    if output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "active" {
        Ok(ServiceStatus::Running)
    } else {
        Ok(ServiceStatus::Stopped)
    }
}

fn launchd_status() -> Result<ServiceStatus, AppError> {
    let service = format!("{}/com.pueue-agent", launchd_gui_domain()?);
    let output = Command::new("launchctl")
        .args(["print", service.as_str()])
        .output()
        .map_err(|source| AppError::Io {
            operation: "query launchd user service",
            source,
        })?;
    if output.status.success() {
        Ok(ServiceStatus::Running)
    } else {
        Ok(ServiceStatus::Stopped)
    }
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
        Err(AppError::Message {
            message: format!(
                "{program} service command failed with status {}",
                output.status
            ),
        })
    }
}

fn find_callback_line(contents: &str) -> Option<&str> {
    contents
        .lines()
        .find(|line| line.trim_start().starts_with("callback:"))
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

fn replace_or_append_callback(contents: &str, command: &str) -> String {
    let escaped = command.replace('\\', "\\\\").replace('"', "\\\"");
    let mut replaced = false;
    let mut output = contents
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("callback:") {
                replaced = true;
                let indent_len = line.len() - line.trim_start().len();
                format!("{}callback: \"{}\"", &line[..indent_len], escaped)
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !replaced {
        if !output.ends_with('\n') {
            output.push('\n');
        }
        output.push_str(&format!("callback: \"{}\"\n", escaped));
    } else if contents.ends_with('\n') {
        output.push('\n');
    }
    output
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\"'\"'"))
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
