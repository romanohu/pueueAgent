use std::{env, path::PathBuf};

use serde::Serialize;

use crate::{output::bounded_redacted_text, AppError};

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
        let executable = env::current_exe().map_err(|source| AppError::Io {
            operation: "resolve current executable",
            source,
        })?;
        let service = executable
            .file_stem()
            .and_then(|name| name.to_str())
            .map(ToOwned::to_owned);

        Ok(Self {
            package_version: env!("CARGO_PKG_VERSION").to_owned(),
            revision: option_env!("PUEUE_AGENT_GIT_REVISION")
                .unwrap_or("unknown")
                .to_owned(),
            source_root: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            service,
        })
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
