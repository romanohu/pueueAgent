use std::{
    fs,
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::AppError;

const CONFIG_PATH: &str = ".pueue-agent/config.toml";
const GROUP_SUFFIX_LENGTH: usize = 6;

pub fn find_root(start: &Path) -> Result<PathBuf, AppError> {
    let mut current = start.canonicalize().map_err(|source| AppError::Io {
        operation: "resolve project path",
        source,
    })?;

    if fs::metadata(&current)
        .map_err(|source| AppError::Io {
            operation: "inspect project path",
            source,
        })?
        .is_file()
    {
        current = current
            .parent()
            .map(Path::to_path_buf)
            .ok_or(AppError::Configuration {
                field: "project_root",
            })?;
    }

    loop {
        if current.join(CONFIG_PATH).is_file() {
            return Ok(current);
        }

        let Some(parent) = current.parent() else {
            break;
        };
        if parent == current.as_path() {
            break;
        }
        current = parent.to_path_buf();
    }

    Err(AppError::Configuration {
        field: "project_root",
    })
}

pub fn new_project_id() -> String {
    Uuid::new_v4().to_string()
}

pub fn default_pueue_group(project_root: &Path, project_id: &str) -> Result<String, AppError> {
    let name = project_root
        .file_name()
        .and_then(|name| name.to_str())
        .map(sanitize_group_component)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "project".to_owned());
    let suffix = project_id_suffix(project_id)?;

    Ok(format!("pa-{name}-{suffix}"))
}

fn project_id_suffix(project_id: &str) -> Result<String, AppError> {
    let suffix: String = project_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .rev()
        .take(GROUP_SUFFIX_LENGTH)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    if suffix.len() != GROUP_SUFFIX_LENGTH {
        return Err(AppError::Configuration {
            field: "project_id",
        });
    }

    Ok(suffix.to_ascii_lowercase())
}

fn sanitize_group_component(value: &str) -> String {
    let mut output = String::new();
    let mut needs_separator = false;

    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            if needs_separator && !output.is_empty() {
                output.push('-');
            }
            output.push(character.to_ascii_lowercase());
            needs_separator = false;
        } else {
            needs_separator = true;
        }
    }

    output
}
