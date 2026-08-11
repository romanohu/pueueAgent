use std::{env, path::PathBuf};

use serde::Serialize;

use crate::{
    output::bounded_redacted_text,
    service::{ServiceControl, ServiceManager, ServiceStatus},
    AppError,
};

const JSON_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildInfo {
    pub package_version: String,
    pub revision: String,
    pub source_root: PathBuf,
    pub service: Option<String>,
}

impl BuildInfo {
    pub fn current() -> Result<Self, AppError> {
        let _executable = env::current_exe().map_err(|source| AppError::Io {
            operation: "resolve current executable",
            source,
        })?;

        Ok(Self {
            package_version: env!("CARGO_PKG_VERSION").to_owned(),
            revision: option_env!("PUEUE_AGENT_GIT_REVISION")
                .unwrap_or("unknown")
                .to_owned(),
            source_root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            service: Some(service_label(ServiceManager.status())),
        })
    }
}

fn service_label(status: Result<ServiceStatus, AppError>) -> String {
    match status {
        Ok(ServiceStatus::Running) => "running".to_owned(),
        Ok(ServiceStatus::Stopped) => "stopped".to_owned(),
        Ok(ServiceStatus::NotInstalled) => "not_installed".to_owned(),
        Err(_) => "unknown".to_owned(),
    }
}

#[derive(Serialize)]
struct VersionReport<'a> {
    schema_version: u32,
    package_version: &'a str,
    revision: &'a str,
    source: &'a str,
    service: Option<&'a str>,
}

pub fn render(info: BuildInfo, json: bool) -> Result<String, AppError> {
    let package_version = bounded_redacted_text(&info.package_version);
    let revision = bounded_redacted_text(&info.revision);
    let source = bounded_redacted_text(&info.source_root.display().to_string());
    let service = info.service.as_deref().map(bounded_redacted_text);

    if json {
        return serde_json::to_string(&VersionReport {
            schema_version: JSON_SCHEMA_VERSION,
            package_version: &package_version,
            revision: &revision,
            source: &source,
            service: service.as_deref(),
        })
        .map_err(|source| AppError::Serialization {
            operation: "serialize version report",
            source,
        });
    }

    Ok(format!(
        "pueue-agent {package_version}\nrevision: {revision}\nsource: {source}\nservice: {}",
        service.as_deref().unwrap_or("none")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_reports_bounded_human_service_state() {
        let info = BuildInfo {
            package_version: "v".repeat(500),
            revision: "r".repeat(500),
            source_root: PathBuf::from("/tmp/SECRET_TOKEN=hidden"),
            service: Some("running".to_owned()),
        };

        let rendered = render(info, false).unwrap();

        assert!(rendered.contains("service: running"));
        assert!(rendered.lines().all(|line| line.len() <= 260));
        assert!(!rendered.contains("hidden"));
    }

    #[test]
    fn render_reports_bounded_json_service_state() {
        let info = BuildInfo {
            package_version: "v".repeat(500),
            revision: "r".repeat(500),
            source_root: PathBuf::from("/tmp/SECRET_TOKEN=hidden"),
            service: Some("stopped".to_owned()),
        };

        let rendered = render(info, true).unwrap();
        let value: serde_json::Value = serde_json::from_str(&rendered).unwrap();

        assert_eq!(value["service"], "stopped");
        assert!(value["package_version"].as_str().unwrap().len() <= 240);
        assert!(value["revision"].as_str().unwrap().len() <= 240);
        assert!(value["source"].as_str().unwrap().len() <= 240);
        assert!(value["service"].as_str().unwrap().len() <= 240);
        assert!(!rendered.contains("hidden"));
    }

    #[test]
    fn service_label_uses_status_or_safe_unknown_fallback() {
        assert_eq!(service_label(Ok(ServiceStatus::Running)), "running");
        assert_eq!(service_label(Ok(ServiceStatus::Stopped)), "stopped");
        assert_eq!(
            service_label(Ok(ServiceStatus::NotInstalled)),
            "not_installed"
        );
        assert_eq!(
            service_label(Err(AppError::Runtime {
                operation: "query service",
            })),
            "unknown"
        );
    }
}
