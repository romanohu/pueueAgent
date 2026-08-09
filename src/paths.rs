use std::{env, path::PathBuf};

use crate::AppError;

pub fn state_db_path() -> Result<PathBuf, AppError> {
    let state_home = env::var_os("XDG_STATE_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or_else(platform_state_home)
        .ok_or(AppError::Configuration {
            field: "XDG_STATE_HOME or HOME",
        })?;

    Ok(state_home.join("pueue-agent").join("state.sqlite3"))
}

#[cfg(target_os = "macos")]
fn platform_state_home() -> Option<PathBuf> {
    env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join("Library/Application Support"))
}

#[cfg(not(target_os = "macos"))]
fn platform_state_home() -> Option<PathBuf> {
    env::var_os("HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .map(|home| home.join(".local/state"))
}
