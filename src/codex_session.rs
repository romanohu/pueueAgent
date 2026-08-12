use std::{
    collections::BTreeMap,
    env, fs,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

use serde::Deserialize;
use uuid::Uuid;

use crate::{
    execution_policy::{PolicyViolation, PolicyViolationCode, PolicyViolationStage},
    AppError,
};

const SESSION_STORES: [&str; 2] = ["sessions", "archived_sessions"];
const MAX_SESSION_STORE_DEPTH: usize = 32;
const MAX_SESSION_STORE_ENTRIES: usize = 4096;
const MAX_SESSION_METADATA_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
struct LatestSessionCandidate {
    id: String,
    path: PathBuf,
    modified_nanos: u128,
}

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

/// Resolve the most recently modified valid session owned by `project_root`.
///
/// This is intentionally an explicit, bounded supervisor operation.  It does
/// not use Codex's ambient `--last` state and never falls back to a fresh
/// session.  Candidate metadata is treated as untrusted: malformed, foreign,
/// and symlinked entries are skipped; duplicate valid ownership is rejected.
pub fn resolve_latest_owned_session(
    codex_home: &Path,
    project_root: &Path,
) -> Result<String, PolicyViolation> {
    let canonical_project_root = fs::canonicalize(project_root).map_err(|_| {
        PolicyViolation::new(
            PolicyViolationCode::RootChanged,
            PolicyViolationStage::PreBinding,
        )
    })?;
    let mut remaining_entries = MAX_SESSION_STORE_ENTRIES;
    let mut candidates = BTreeMap::<String, LatestSessionCandidate>::new();

    for store_name in SESSION_STORES {
        let store = codex_home.join(store_name);
        match fs::metadata(&store) {
            Ok(metadata) if metadata.is_dir() => collect_latest_candidates(
                &store,
                0,
                &canonical_project_root,
                &mut remaining_entries,
                &mut candidates,
            )?,
            Ok(_) => return Err(session_not_owned()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(session_not_owned()),
        }
    }

    candidates
        .into_values()
        .max_by(|left, right| {
            left.modified_nanos
                .cmp(&right.modified_nanos)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.path.cmp(&right.path))
        })
        .map(|candidate| candidate.id)
        .ok_or_else(session_missing)
}

fn collect_latest_candidates(
    directory: &Path,
    depth: usize,
    canonical_project_root: &Path,
    remaining_entries: &mut usize,
    candidates: &mut BTreeMap<String, LatestSessionCandidate>,
) -> Result<(), PolicyViolation> {
    let entries = fs::read_dir(directory).map_err(|_| session_not_owned())?;
    for entry in entries {
        if *remaining_entries == 0 {
            return Err(session_not_owned());
        }
        *remaining_entries -= 1;

        let entry = entry.map_err(|_| session_not_owned())?;
        let file_type = entry.file_type().map_err(|_| session_not_owned())?;
        let path = entry.path();
        if file_type.is_dir() {
            if depth == MAX_SESSION_STORE_DEPTH {
                return Err(session_not_owned());
            }
            collect_latest_candidates(
                &path,
                depth + 1,
                canonical_project_root,
                remaining_entries,
                candidates,
            )?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Some(filename_id) = filename_session_id(&path) else {
            continue;
        };
        let Ok(metadata) = read_metadata(&path, &filename_id) else {
            continue;
        };
        if metadata.kind != "session_meta"
            || normalize_metadata_id(&metadata.payload.id, &filename_id)
                .ok()
                .as_deref()
                != Some(filename_id.as_str())
            || !metadata.payload.cwd.is_absolute()
        {
            continue;
        }
        let Ok(canonical_cwd) = fs::canonicalize(&metadata.payload.cwd) else {
            continue;
        };
        if !canonical_cwd.starts_with(canonical_project_root) {
            continue;
        }
        let Ok(file_metadata) = fs::metadata(&path) else {
            continue;
        };
        let Ok(modified) = file_metadata.modified() else {
            continue;
        };
        let Ok(modified_nanos) = modified.duration_since(UNIX_EPOCH) else {
            continue;
        };
        let candidate = LatestSessionCandidate {
            id: filename_id.clone(),
            path,
            modified_nanos: modified_nanos.as_nanos(),
        };
        if candidates.insert(filename_id, candidate).is_some() {
            return Err(session_not_owned());
        }
    }
    Ok(())
}

fn filename_session_id(path: &Path) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    let stem = filename.strip_suffix(".jsonl")?;
    stem.match_indices('-')
        .filter_map(|(index, _)| stem.get(index + 1..))
        .find_map(|candidate| normalize_session_id(candidate).ok())
}

fn session_missing() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::SessionMissing,
        PolicyViolationStage::PreBinding,
    )
}

fn session_not_owned() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::SessionNotOwned,
        PolicyViolationStage::PreBinding,
    )
}

fn locate_metadata(codex_home: &Path, session_id: &str) -> Result<PathBuf, AppError> {
    locate_metadata_in_stores(
        codex_home,
        session_id,
        &SESSION_STORES,
        MAX_SESSION_STORE_ENTRIES,
    )
}

fn locate_metadata_in_stores(
    codex_home: &Path,
    session_id: &str,
    store_names: &[&str],
    max_entries: usize,
) -> Result<PathBuf, AppError> {
    let suffix = format!("-{session_id}.jsonl");
    let mut matched_path = None;
    let mut remaining_entries = max_entries;

    for store_name in store_names {
        let store = codex_home.join(store_name);
        match fs::metadata(&store) {
            Ok(metadata) if metadata.is_dir() => {
                collect_matching_metadata(
                    &store,
                    &suffix,
                    session_id,
                    0,
                    &mut remaining_entries,
                    &mut matched_path,
                )?;
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

    matched_path.ok_or_else(|| metadata_error(session_id, "metadata was not found in CODEX_HOME"))
}

fn collect_matching_metadata(
    directory: &Path,
    suffix: &str,
    session_id: &str,
    depth: usize,
    remaining_entries: &mut usize,
    matched_path: &mut Option<PathBuf>,
) -> Result<(), AppError> {
    let entries = fs::read_dir(directory)
        .map_err(|_| metadata_error(session_id, "session store is unreadable"))?;
    for entry in entries {
        if *remaining_entries == 0 {
            return Err(metadata_error(
                session_id,
                "metadata discovery exceeded the traversal limit",
            ));
        }
        *remaining_entries -= 1;

        let entry = entry.map_err(|_| metadata_error(session_id, "session store is unreadable"))?;
        let file_type = entry
            .file_type()
            .map_err(|_| metadata_error(session_id, "session metadata type is unreadable"))?;
        let path = entry.path();

        if file_type.is_dir() {
            if depth == MAX_SESSION_STORE_DEPTH {
                return Err(metadata_error(
                    session_id,
                    "metadata discovery exceeded the traversal limit",
                ));
            }
            collect_matching_metadata(
                &path,
                suffix,
                session_id,
                depth + 1,
                remaining_entries,
                matched_path,
            )?;
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
            if matched_path.replace(path).is_some() {
                return Err(metadata_error(
                    session_id,
                    "metadata is ambiguous across local session stores",
                ));
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
    fn finds_session_metadata_at_the_maximum_discovery_depth() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let mut metadata_dir = temp.path().join("codex-home/sessions");
        for depth in 0..MAX_SESSION_STORE_DEPTH {
            metadata_dir = metadata_dir.join(format!("level-{depth}"));
        }
        write_metadata(&metadata_dir, SESSION_ID, SESSION_ID, &project_root);

        let verified =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap();

        assert_eq!(verified, SESSION_ID);
    }

    #[test]
    fn rejects_metadata_discovery_beyond_the_depth_limit() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        let mut metadata_dir = temp.path().join("codex-home/sessions");
        for depth in 0..=MAX_SESSION_STORE_DEPTH {
            metadata_dir = metadata_dir.join(format!("level-{depth}"));
        }
        write_metadata(&metadata_dir, SESSION_ID, SESSION_ID, &project_root);

        let error =
            verify_project_ownership(&temp.path().join("codex-home"), &project_root, SESSION_ID)
                .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata discovery exceeded the traversal limit",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn shares_metadata_discovery_entry_limit_across_session_stores() {
        let temp = TempDir::new().unwrap();
        for (store_name, entry_names) in [
            ("sessions", &["one"][..]),
            ("archived_sessions", &["two", "three"][..]),
        ] {
            let store = temp.path().join("codex-home").join(store_name);
            fs::create_dir_all(&store).unwrap();
            for name in entry_names {
                fs::write(store.join(name), "not metadata").unwrap();
            }
        }

        let error = locate_metadata_in_stores(
            &temp.path().join("codex-home"),
            SESSION_ID,
            &SESSION_STORES,
            2,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata discovery exceeded the traversal limit",
            } if session_id == SESSION_ID
        ));
    }

    #[test]
    fn stops_discovery_when_the_second_metadata_candidate_is_found() {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        fs::create_dir_all(&project_root).unwrap();
        write_metadata(
            &temp.path().join("codex-home/sessions"),
            SESSION_ID,
            SESSION_ID,
            &project_root,
        );
        write_metadata(
            &temp.path().join("codex-home/archived_sessions"),
            SESSION_ID,
            SESSION_ID,
            &project_root,
        );
        fs::write(
            temp.path().join("codex-home/must-not-be-read"),
            "not a store",
        )
        .unwrap();

        let error = locate_metadata_in_stores(
            &temp.path().join("codex-home"),
            SESSION_ID,
            &["sessions", "archived_sessions", "must-not-be-read"],
            MAX_SESSION_STORE_ENTRIES,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AppError::CodexSessionMetadata {
                session_id,
                reason: "metadata is ambiguous across local session stores",
            } if session_id == SESSION_ID
        ));
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
