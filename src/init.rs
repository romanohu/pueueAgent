use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{project, AppError};

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const STATE_TEMPLATE: &str = include_str!("../templates/STATE.md");
const INSTRUCTIONS_TEMPLATE: &str = include_str!("../templates/instructions.md");

pub fn run(project_root: &Path) -> Result<PathBuf, AppError> {
    let project_root = project_root.canonicalize().map_err(|source| AppError::Io {
        operation: "resolve project path for initialization",
        source,
    })?;
    if !project_root.is_dir() {
        return Err(AppError::Configuration {
            field: "project_root",
        });
    }

    let state_dir = project_root.join(".pueue-agent");
    let config_path = state_dir.join("config.toml");
    if config_path.exists() {
        return Err(AppError::Message {
            message: format!("project is already initialized: {}", config_path.display()),
        });
    }

    fs::create_dir_all(state_dir.join("logs")).map_err(|source| AppError::Io {
        operation: "create project state directory",
        source,
    })?;

    write_if_missing(&state_dir.join("STATE.md"), STATE_TEMPLATE)?;
    write_if_missing(&state_dir.join("instructions.md"), INSTRUCTIONS_TEMPLATE)?;

    let project_id = project::new_project_id();
    let pueue_group = project::default_pueue_group(&project_root, &project_id)?;
    let config = CONFIG_TEMPLATE
        .replace("{{PROJECT_ID}}", &project_id)
        .replace("{{PUEUE_GROUP}}", &pueue_group);
    write_new(&config_path, &config)?;

    Ok(project_root)
}

fn write_if_missing(path: &Path, contents: &str) -> Result<(), AppError> {
    if path.exists() {
        return Ok(());
    }
    write_new(path, contents)
}

fn write_new(path: &Path, contents: &str) -> Result<(), AppError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| AppError::Io {
            operation: "create project state file",
            source,
        })?;
    file.write_all(contents.as_bytes())
        .map_err(|source| AppError::Io {
            operation: "write project state file",
            source,
        })
}
