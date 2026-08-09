use std::{
    env,
    path::{Path, PathBuf},
};

use crate::AppError;

pub fn state_db_path() -> Result<PathBuf, AppError> {
    let explicit_state_dir = env::var_os("PUEUE_AGENT_STATE_DIR").map(PathBuf::from);
    let xdg_state_home = env::var_os("XDG_STATE_HOME").map(PathBuf::from);
    let home = env::var_os("HOME").map(PathBuf::from);

    state_db_path_with_override(
        explicit_state_dir.as_deref(),
        xdg_state_home.as_deref(),
        home.as_deref(),
    )
}

pub fn state_db_path_with(
    xdg_state_home: Option<&Path>,
    home: Option<&Path>,
) -> Result<PathBuf, AppError> {
    state_db_path_with_override(None, xdg_state_home, home)
}

pub fn state_db_path_with_override(
    explicit_state_dir: Option<&Path>,
    xdg_state_home: Option<&Path>,
    home: Option<&Path>,
) -> Result<PathBuf, AppError> {
    if let Some(state_dir) =
        explicit_state_dir.filter(|path| !path.as_os_str().is_empty() && path.is_absolute())
    {
        return Ok(state_dir.join("state.sqlite3"));
    }

    let state_home = xdg_state_home
        .filter(|path| !path.as_os_str().is_empty() && path.is_absolute())
        .map(Path::to_path_buf)
        .or_else(|| platform_state_home(home))
        .ok_or(AppError::Configuration {
            field: "PUEUE_AGENT_STATE_DIR, XDG_STATE_HOME, or HOME",
        })?;

    Ok(state_home.join("pueue-agent").join("state.sqlite3"))
}

#[cfg(target_os = "macos")]
fn platform_state_home(home: Option<&Path>) -> Option<PathBuf> {
    home.filter(|path| !path.as_os_str().is_empty())
        .map(|home| home.join("Library/Application Support"))
}

#[cfg(not(target_os = "macos"))]
fn platform_state_home(home: Option<&Path>) -> Option<PathBuf> {
    home.filter(|path| !path.as_os_str().is_empty())
        .map(|home| home.join(".local/state"))
}
