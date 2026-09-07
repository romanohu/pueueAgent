use std::{
    fs::{self, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::{
    fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
};

use crate::{project, AppError};

const CONFIG_TEMPLATE: &str = include_str!("../templates/config.toml");
const STATE_TEMPLATE: &str = include_str!("../templates/STATE.md");
const CANONICAL_STATE_TEMPLATE: &str = include_str!("../templates/state.json");
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

    install_git_exclude(&project_root)?;

    fs::create_dir_all(state_dir.join("logs")).map_err(|source| AppError::Io {
        operation: "create project state directory",
        source,
    })?;

    write_if_missing(&state_dir.join("STATE.md"), STATE_TEMPLATE)?;
    write_if_missing(&state_dir.join("state.json"), CANONICAL_STATE_TEMPLATE)?;
    write_if_missing(&state_dir.join("instructions.md"), INSTRUCTIONS_TEMPLATE)?;

    let project_id = project::new_project_id();
    let pueue_group = project::default_pueue_group(&project_root, &project_id)?;
    let config = CONFIG_TEMPLATE
        .replace("{{PROJECT_ID}}", &project_id)
        .replace("{{PUEUE_GROUP}}", &pueue_group);
    write_new(&config_path, &config)?;

    Ok(project_root)
}

#[cfg(not(unix))]
fn install_git_exclude(_project_root: &Path) -> Result<(), AppError> {
    Ok(())
}

#[cfg(unix)]
fn install_git_exclude(project_root: &Path) -> Result<(), AppError> {
    let dot_git = project_root.join(".git");
    let metadata = match fs::symlink_metadata(&dot_git) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect Git metadata for initialization",
            })
        }
    };
    if metadata.file_type().is_symlink() {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "local Git metadata must not be a symlink",
        });
    }
    let admin = if metadata.is_dir() {
        dot_git
    } else if metadata.is_file() {
        resolve_git_pointer(&dot_git, project_root, "gitdir:")?
    } else {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "local Git metadata must be a directory or worktree file",
        });
    };
    let common = common_git_directory(&admin)?;
    ensure_secure_git_directory(&common)?;
    ensure_git_metadata_shape(&admin, &common)?;
    let info = common.join("info");
    ensure_git_info_directory(&info)?;
    append_git_exclude(&info.join("exclude"))
}

#[cfg(unix)]
fn resolve_git_pointer(path: &Path, base: &Path, prefix: &str) -> Result<PathBuf, AppError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AppError::Runtime {
        operation: "read Git metadata pointer",
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata pointer must be a regular file",
        });
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| AppError::Runtime {
            operation: "open Git metadata pointer",
        })?;
    ensure_secure_git_file(&file)?;
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AppError::Runtime {
            operation: "read Git metadata pointer",
        })?;
    if bytes.len() > 16 * 1024 {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata pointer exceeds the bounded size",
        });
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| AppError::Validation {
        field: "git.metadata",
        message: "Git metadata pointer must be valid UTF-8",
    })?;
    let text = text.strip_suffix('\n').unwrap_or(text);
    if text.contains(['\r', '\n', '\0']) {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata pointer must contain one line",
        });
    }
    let value = text
        .strip_prefix(prefix)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata pointer must identify a Git directory",
        })?;
    let target = Path::new(value);
    let target = if target.is_absolute() {
        target.to_owned()
    } else {
        base.join(target)
    };
    reject_symlink_components(&target)?;
    let target = fs::canonicalize(&target).map_err(|_| AppError::Runtime {
        operation: "resolve Git metadata pointer",
    })?;
    ensure_secure_git_directory(&target)?;
    Ok(target)
}

#[cfg(unix)]
fn common_git_directory(admin: &Path) -> Result<PathBuf, AppError> {
    let commondir = admin.join("commondir");
    let metadata = match fs::symlink_metadata(&commondir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(admin.to_owned()),
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect Git common directory",
            })
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git common-directory file must be a regular file",
        });
    }
    let target = resolve_git_pointer(&commondir, admin, "")?;
    Ok(target)
}

#[cfg(unix)]
fn ensure_secure_git_directory(path: &Path) -> Result<(), AppError> {
    reject_symlink_components(path)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| AppError::Runtime {
        operation: "inspect Git directory",
    })?;
    if !metadata.is_dir() || metadata.uid() != unsafe { libc::geteuid() as u32 } || metadata.mode() & 0o022 != 0 {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git directory must be an owned non-writable directory",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_secure_git_file(file: &std::fs::File) -> Result<(), AppError> {
    let metadata = file.metadata().map_err(|_| AppError::Runtime {
        operation: "inspect Git metadata file",
    })?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.nlink() != 1
        || metadata.mode() & 0o022 != 0
    {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata file must be owned and non-writable",
        });
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_git_metadata_shape(admin: &Path, common: &Path) -> Result<(), AppError> {
    for name in ["HEAD", "config"] {
        ensure_git_metadata_file(&common.join(name))?;
    }
    for name in ["objects", "refs"] {
        ensure_secure_git_directory(&common.join(name))?;
    }
    if admin != common {
        ensure_git_metadata_file(&admin.join("HEAD"))?;
        ensure_git_metadata_file(&admin.join("gitdir"))?;
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_git_metadata_file(path: &Path) -> Result<(), AppError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AppError::Runtime {
        operation: "inspect Git metadata file",
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::Validation {
            field: "git.metadata",
            message: "Git metadata file must be a regular file",
        });
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .map_err(|_| AppError::Runtime {
            operation: "open Git metadata file",
        })?;
    ensure_secure_git_file(&file)
}

#[cfg(unix)]
fn ensure_git_info_directory(path: &Path) -> Result<(), AppError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(AppError::Validation {
                field: "git.metadata",
                message: "Git info directory must not be a symlink",
            })
        }
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(AppError::Validation {
                field: "git.metadata",
                message: "Git info path must be a directory",
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|_| AppError::Runtime {
                operation: "create Git info directory",
            })?;
            let mut permissions = fs::metadata(path)
                .map_err(|_| AppError::Runtime {
                    operation: "inspect Git info directory",
                })?
                .permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(path, permissions).map_err(|_| AppError::Runtime {
                operation: "secure Git info directory",
            })?;
        }
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect Git info directory",
            })
        }
    }
    ensure_secure_git_directory(path)
}

#[cfg(unix)]
fn append_git_exclude(path: &Path) -> Result<(), AppError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let mut file = options.open(path).map_err(|_| AppError::Runtime {
        operation: "open Git exclude file",
    })?;
    ensure_secure_git_file(&file)?;
    file.seek(SeekFrom::Start(0)).map_err(|_| AppError::Runtime {
        operation: "read Git exclude file",
    })?;
    let mut bytes = Vec::new();
    (&mut file).take(16 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AppError::Runtime {
        operation: "read Git exclude file",
    })?;
    if bytes.len() > 16 * 1024 {
        return Err(AppError::Validation {
            field: "git.exclude",
            message: "Git exclude file exceeds the bounded size",
        });
    }
    if bytes
        .split(|byte| *byte == b'\n')
        .any(|line| line.strip_suffix(b"\r").unwrap_or(line) == b"/.pueue-agent/")
    {
        return Ok(());
    }
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        file.write_all(b"\n").map_err(|_| AppError::Runtime {
            operation: "update Git exclude file",
        })?;
    }
    file.write_all(b"/.pueue-agent/\n")
        .map_err(|_| AppError::Runtime {
            operation: "update Git exclude file",
        })
}

#[cfg(unix)]
fn reject_symlink_components(path: &Path) -> Result<(), AppError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(AppError::Validation {
                    field: "git.metadata",
                    message: "Git metadata path must not contain a symlink",
                })
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(_) => {
                return Err(AppError::Runtime {
                    operation: "inspect Git metadata path",
                })
            }
        }
    }
    Ok(())
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
