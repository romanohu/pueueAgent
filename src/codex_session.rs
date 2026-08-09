use std::{
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use uuid::Uuid;

use crate::AppError;

const SESSION_STORES: [&str; 2] = ["sessions", "archived_sessions"];
const MAX_SESSION_METADATA_BYTES: usize = 1024 * 1024;

#[derive(Debug, Deserialize)]
struct SessionMetadata {
    #[serde(rename = "type")]
    kind: String,
    payload: SessionMetadataPayload,
}

#[derive(Debug, Deserialize)]
struct SessionMetadataPayload {
    id: String,
    cwd: PathBuf,
}

pub fn normalize_session_id(value: &str) -> Result<String, AppError> {
    let value = value.trim();
    let parsed = Uuid::parse_str(value).map_err(|_| AppError::Configuration {
        field: "agent.context.session_id",
    })?;
    let normalized = parsed.hyphenated().to_string();
    if value != normalized {
        return Err(AppError::Configuration {
            field: "agent.context.session_id",
        });
    }
    Ok(normalized)
}

pub fn home_from_environment() -> Result<PathBuf, AppError> {
    if let Some(codex_home) = env::var_os("CODEX_HOME") {
        if codex_home.is_empty() {
            return Err(AppError::Configuration {
                field: "CODEX_HOME",
            });
        }
        return Ok(PathBuf::from(codex_home));
    }

    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .ok_or(AppError::Configuration { field: "HOME" })?;
    Ok(PathBuf::from(home).join(".codex"))
}

pub fn verify_project_ownership(
    codex_home: &Path,
    project_root: &Path,
    session_id: &str,
) -> Result<String, AppError> {
    let session_id = normalize_session_id(session_id)?;
    let metadata_path = locate_metadata(codex_home, &session_id)?;
    let metadata = read_metadata(&metadata_path, &session_id)?;

    if metadata.kind != "session_meta" {
        return Err(metadata_error(
            &session_id,
            "metadata is malformed (first record is not session_meta)",
        ));
    }
    let metadata_id = normalize_metadata_id(&metadata.payload.id, &session_id)?;
    if metadata_id != session_id {
        return Err(metadata_error(
            &session_id,
            "metadata session ID does not match the requested session",
        ));
    }
    if !metadata.payload.cwd.is_absolute() {
        return Err(metadata_error(
            &session_id,
            "metadata is malformed (cwd is not absolute)",
        ));
    }

    let canonical_project_root = fs::canonicalize(project_root).map_err(|source| AppError::Io {
        operation: "canonicalize project root for Codex resume",
        source,
    })?;
    let canonical_session_cwd = fs::canonicalize(&metadata.payload.cwd)
        .map_err(|_| metadata_error(&session_id, "metadata cwd cannot be canonicalized"))?;
    if !canonical_session_cwd.starts_with(&canonical_project_root) {
        return Err(metadata_error(
            &session_id,
            "metadata cwd is outside project root",
        ));
    }

    Ok(session_id)
}

fn locate_metadata(codex_home: &Path, session_id: &str) -> Result<PathBuf, AppError> {
    let suffix = format!("-{session_id}.jsonl");
    let mut matches = Vec::new();

    for store_name in SESSION_STORES {
        let store = codex_home.join(store_name);
        match fs::metadata(&store) {
            Ok(metadata) if metadata.is_dir() => {
                collect_matching_metadata(&store, &suffix, session_id, &mut matches)?;
            }
            Ok(_) => {
                return Err(metadata_error(
                    session_id,
                    "session store is not a directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(metadata_error(session_id, "session store is unreadable"));
            }
        }
    }

    match matches.len() {
        0 => Err(metadata_error(
            session_id,
            "metadata was not found in CODEX_HOME",
        )),
        1 => Ok(matches.remove(0)),
        _ => Err(metadata_error(
            session_id,
            "metadata is ambiguous across local session stores",
        )),
    }
}

fn collect_matching_metadata(
    directory: &Path,
    suffix: &str,
    session_id: &str,
    matches: &mut Vec<PathBuf>,
) -> Result<(), AppError> {
    let mut pending = vec![directory.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(directory)
            .map_err(|_| metadata_error(session_id, "session store is unreadable"))?;
        for entry in entries {
            let entry =
                entry.map_err(|_| metadata_error(session_id, "session store is unreadable"))?;
            let file_type = entry
                .file_type()
                .map_err(|_| metadata_error(session_id, "session metadata type is unreadable"))?;
            let path = entry.path();

            if file_type.is_dir() {
                pending.push(path);
            } else if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(suffix))
            {
                if !file_type.is_file() {
                    return Err(metadata_error(
                        session_id,
                        "metadata path is not a regular file",
                    ));
                }
                matches.push(path);
            }
        }
    }
    Ok(())
}

fn read_metadata(path: &Path, session_id: &str) -> Result<SessionMetadata, AppError> {
    let file =
        fs::File::open(path).map_err(|_| metadata_error(session_id, "metadata is unreadable"))?;
    let reader = BufReader::new(file);
    let mut limited = reader.take((MAX_SESSION_METADATA_BYTES + 1) as u64);
    let mut first_record = Vec::new();
    limited
        .read_until(b'\n', &mut first_record)
        .map_err(|_| metadata_error(session_id, "metadata is unreadable"))?;

    if first_record.is_empty() || first_record.len() > MAX_SESSION_METADATA_BYTES {
        return Err(metadata_error(
            session_id,
            "metadata is empty or exceeds the size limit",
        ));
    }

    serde_json::from_slice(&first_record)
        .map_err(|_| metadata_error(session_id, "metadata is malformed"))
}

fn normalize_metadata_id(value: &str, session_id: &str) -> Result<String, AppError> {
    let parsed = Uuid::parse_str(value)
        .map_err(|_| metadata_error(session_id, "metadata is malformed (invalid session ID)"))?;
    Ok(parsed.hyphenated().to_string())
}

fn metadata_error(session_id: &str, reason: &'static str) -> AppError {
    AppError::CodexSessionMetadata {
        session_id: session_id.to_owned(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    const SESSION_ID: &str = "019f9f30-5f31-7a40-8e28-bd95e1f6c537";
    const OTHER_SESSION_ID: &str = "019f9f30-a553-7e21-b108-16a5c341f728";

    #[test]
    fn finds_session_metadata_at_any_nested_depth() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let mut metadata_dir = temp.path().join("codex-home/sessions");
        for depth in 0..10 {
            metadata_dir = metadata_dir.join(format!("level-{depth}"));
        }
        write_metadata(&metadata_dir, SESSION_ID, SESSION_ID, &project_root);

        let verified =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap();

        assert_eq!(verified, SESSION_ID);
    }

    #[test]
    fn rejects_metadata_with_a_different_payload_id() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        write_metadata(
            &temp.path().join("codex-home/sessions"),
            SESSION_ID,
            OTHER_SESSION_ID,
            &project_root,
        );

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata session ID does not match the requested session",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn rejects_a_first_metadata_record_over_the_size_limit() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let metadata_dir = temp.path().join("codex-home/sessions");
        fs::create_dir_all(&metadata_dir).unwrap();
        fs::write(
            metadata_dir.join(format!("rollout-test-{SESSION_ID}.jsonl")),
            vec![b' '; MAX_SESSION_METADATA_BYTES + 1],
        )
        .unwrap();

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata is empty or exceeds the size limit",
            } if session_id == SESSION_ID
        ));
    }

    fn write_metadata(directory: &Path, filename_id: &str, payload_id: &str, cwd: &Path) {
        fs::create_dir_all(directory).unwrap();
        fs::write(
            directory.join(format!("rollout-test-{filename_id}.jsonl")),
            format!(
                "{}\n",
                json!({
                    "timestamp": "2026-08-09T00:00:00Z",
                    "type": "session_meta",
                    "payload": {
                        "id": payload_id,
                        "cwd": cwd,
                    }
                })
            ),
        )
        .unwrap();
    }
}
