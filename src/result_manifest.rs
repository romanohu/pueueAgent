//! Result-manifest ingestion for terminal experiment projections.
//!
//! A finished task's environment cannot be read after exit, so discovery
//! reconstructs the declared `PUEUE_AGENT_RESULT_PATH` first and then falls
//! back to the fixed project-relative default path. Manifest contents are
//! validated as numbers and identifiers only; artifact paths are never
//! followed or digested here.

use std::{
    collections::BTreeMap,
    io::Read,
    path::{Path, PathBuf},
};

use rusqlite::OptionalExtension;
use serde_json::Value;

use crate::{
    db::{database_error, Db, MetricsRepository},
    execution_policy::ProjectRootAnchor,
    models::{ExperimentMetricsRow, ObjectiveMetric},
    project_logs::{ProjectRootLogReader, ResultManifestOpen},
    AppError,
};

const MAX_RESULT_MANIFEST_BYTES: usize = 16 * 1024;
const RESULT_DIRECTORY: &str = "results";

/// Declared result locations for one campaign experiment task, in discovery
/// order. The first entry mirrors the injected `PUEUE_AGENT_RESULT_PATH`
/// value; the second is the fixed project-relative default path.
pub fn result_path_candidates(
    project_root: &Path,
    experiment_id: &str,
    pueue_task_id: i64,
) -> Vec<PathBuf> {
    let results = project_root
        .join(crate::environment::private_service_root())
        .join(RESULT_DIRECTORY);
    vec![
        results.join(format!("{experiment_id}.json")),
        results.join(format!("{pueue_task_id}.json")),
    ]
}

enum ManifestOutcome {
    Invalid,
    Valid {
        metrics: BTreeMap<String, f64>,
        metrics_json: String,
    },
}

enum ManifestRead {
    Invalid,
    Bytes(Vec<u8>),
}

/// A bounded terminal-result classification that has not yet changed the
/// database. The optional read error preserves the ordinary retryable I/O
/// behavior when this value is persisted.
pub(crate) struct StagedTerminalResult {
    row: ExperimentMetricsRow,
    read_error: Option<AppError>,
}

impl StagedTerminalResult {
    /// Persist the staged terminal classification. A read error is returned
    /// after its retryable marker is durable, matching ordinary ingestion.
    pub(crate) fn persist(self, db: &Db) -> Result<(), AppError> {
        MetricsRepository::insert_terminal_classification(db, &self.row)?;
        if let Some(error) = self.read_error {
            return Err(error);
        }
        Ok(())
    }
}

fn relative_result_path_candidates(experiment_id: &str, pueue_task_id: i64) -> Vec<PathBuf> {
    let results = Path::new(".pueue-agent").join(RESULT_DIRECTORY);
    vec![
        results.join(format!("{experiment_id}.json")),
        results.join(format!("{pueue_task_id}.json")),
    ]
}

fn read_manifest(
    root: &ProjectRootLogReader,
    relative: &Path,
) -> Result<Option<ManifestRead>, AppError> {
    let file = match root.open_result_manifest(relative)? {
        ResultManifestOpen::Missing => return Ok(None),
        ResultManifestOpen::Invalid => return Ok(Some(ManifestRead::Invalid)),
        ResultManifestOpen::File(file) => file,
    };
    let mut bytes = Vec::with_capacity(MAX_RESULT_MANIFEST_BYTES + 1);
    file.take((MAX_RESULT_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| AppError::Io {
            operation: "read result manifest",
            source,
        })?;
    Ok(Some(ManifestRead::Bytes(bytes)))
}

fn classify_manifest(bytes: &[u8], experiment_id: &str) -> Result<ManifestOutcome, AppError> {
    if bytes.len() > MAX_RESULT_MANIFEST_BYTES {
        return Ok(ManifestOutcome::Invalid);
    }
    let value = match serde_json::from_slice::<Value>(bytes) {
        Ok(value) => value,
        Err(_) => return Ok(ManifestOutcome::Invalid),
    };
    if !value.is_object()
        || value.get("schema_version").and_then(Value::as_i64) != Some(1)
        || value.get("experiment_id").and_then(Value::as_str) != Some(experiment_id)
    {
        return Ok(ManifestOutcome::Invalid);
    }
    let Some(metrics_value) = value.get("metrics").and_then(Value::as_object) else {
        return Ok(ManifestOutcome::Invalid);
    };
    let mut metrics = BTreeMap::new();
    for (name, metric) in metrics_value {
        match metric.as_f64() {
            Some(number) if number.is_finite() => {
                metrics.insert(name.clone(), number);
            }
            _ => return Ok(ManifestOutcome::Invalid),
        }
    }
    let metrics_json =
        serde_json::to_string(&Value::Object(metrics_value.clone())).map_err(|source| {
            AppError::Serialization {
                operation: "serialize result manifest metrics",
                source,
            }
        })?;
    Ok(ManifestOutcome::Valid {
        metrics,
        metrics_json,
    })
}

fn defect_row(experiment_id: &str, defect: &'static str, now: i64) -> ExperimentMetricsRow {
    ExperimentMetricsRow {
        experiment_id: experiment_id.to_owned(),
        source: "manifest".to_owned(),
        primary_metric_name: None,
        primary_metric_value: None,
        metrics_json: "{}".to_owned(),
        artifact_defect: Some(defect.to_owned()),
        created_at: now,
        updated_at: now,
        evaluated_at: None,
    }
}

fn row_from_manifest_outcome(
    outcome: ManifestOutcome,
    experiment_id: &str,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> ExperimentMetricsRow {
    match outcome {
        ManifestOutcome::Invalid => defect_row(experiment_id, "result_invalid", now),
        ManifestOutcome::Valid {
            metrics,
            metrics_json,
        } => ExperimentMetricsRow {
            experiment_id: experiment_id.to_owned(),
            source: "manifest".to_owned(),
            primary_metric_name: objective.map(|objective| objective.name.clone()),
            primary_metric_value: objective
                .and_then(|objective| metrics.get(&objective.name).copied()),
            metrics_json,
            artifact_defect: None,
            created_at: now,
            updated_at: now,
            evaluated_at: None,
        },
    }
}

/// Ingest the finished experiment's result manifest, storing either the
/// validated metrics or a bounded artifact defect row. Idempotent per
/// experiment via the primary key on `experiment_metrics`.
///
/// On I/O failure, persist a retryable result_io_error marker and return the
/// original error so the experiment remains accepted and retryable. Other
/// errors are returned without creating a retry marker.
#[allow(clippy::too_many_arguments)]
pub fn ingest(
    db: &Db,
    project_root: &Path,
    project_id: &str,
    experiment_id: &str,
    pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<(), AppError> {
    let canonical_root = std::fs::canonicalize(project_root).map_err(|source| AppError::Io {
        operation: "canonicalize project root for result manifest",
        source,
    })?;
    let root_anchor = ProjectRootAnchor::resolve(&canonical_root).map_err(AppError::from)?;
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    let root = ProjectRootLogReader::from_verified(verified_root);
    let staged = stage_from_verified_root(
        &root,
        project_id,
        experiment_id,
        pueue_task_id,
        objective,
        now,
    )?;
    staged.persist(db)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn stage_from_verified_root(
    root: &ProjectRootLogReader,
    _project_id: &str,
    experiment_id: &str,
    pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<StagedTerminalResult, AppError> {
    validate_experiment_id(experiment_id)?;
    match stage_inner(
        root,
        _project_id,
        experiment_id,
        pueue_task_id,
        objective,
        now,
    ) {
        Ok(row) => Ok(StagedTerminalResult {
            row,
            read_error: None,
        }),
        Err(error @ AppError::Io { .. }) => Ok(StagedTerminalResult {
            row: defect_row(experiment_id, "result_io_error", now),
            read_error: Some(error),
        }),
        Err(error) => Err(error),
    }
}

pub(crate) fn stage_defect(
    experiment_id: &str,
    defect: &'static str,
    now: i64,
) -> Result<StagedTerminalResult, AppError> {
    validate_experiment_id(experiment_id)?;
    Ok(StagedTerminalResult {
        row: defect_row(experiment_id, defect, now),
        read_error: None,
    })
}

/// Stage a manifest that was read from a descriptor-bound runtime output.
/// Callers must reverify that binding before persisting the returned row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn stage_from_bound_manifest(
    bytes: &[u8],
    _project_id: &str,
    experiment_id: &str,
    _pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<StagedTerminalResult, AppError> {
    validate_experiment_id(experiment_id)?;
    let row = row_from_manifest_outcome(classify_manifest(bytes, experiment_id)?, experiment_id, objective, now);
    Ok(StagedTerminalResult {
        row,
        read_error: None,
    })
}

fn validate_experiment_id(experiment_id: &str) -> Result<(), AppError> {
    if experiment_id.is_empty()
        || experiment_id.len() > 128
        || experiment_id == "."
        || experiment_id == ".."
        || experiment_id.bytes().any(|byte| {
            !matches!(
                byte,
                b'a'..=b'z'
                    | b'A'..=b'Z'
                    | b'0'..=b'9'
                    | b'_'
                    | b'-'
                    | b'.'
                    | b':'
            )
        })
    {
        return Err(AppError::Validation {
            field: "experiment_id",
            message: "must be a safe internal identifier",
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn ingest_from_verified_root(
    db: &Db,
    root: &ProjectRootLogReader,
    project_id: &str,
    experiment_id: &str,
    pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<(), AppError> {
    let staged = stage_from_verified_root(
        root,
        project_id,
        experiment_id,
        pueue_task_id,
        objective,
        now,
    )?;
    staged.persist(db)
}

#[allow(clippy::too_many_arguments)]
fn stage_inner(
    root: &ProjectRootLogReader,
    _project_id: &str,
    experiment_id: &str,
    pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<ExperimentMetricsRow, AppError> {
    let mut outcome = None;
    for path in relative_result_path_candidates(experiment_id, pueue_task_id) {
        match read_manifest(root, &path)? {
            None => {}
            Some(ManifestRead::Invalid) => {
                outcome = Some(ManifestOutcome::Invalid);
                break;
            }
            Some(ManifestRead::Bytes(bytes)) => {
                outcome = Some(classify_manifest(&bytes, experiment_id)?);
                break;
            }
        }
    }
    let row = outcome.map_or_else(
        || defect_row(experiment_id, "result_missing", now),
        |outcome| row_from_manifest_outcome(outcome, experiment_id, objective, now),
    );
    Ok(row)
}

/// The declared objective metric of a campaign, if any.
pub(crate) fn campaign_objective(
    db: &Db,
    campaign_id: &str,
) -> Result<Option<ObjectiveMetric>, AppError> {
    let connection = db.connect()?;
    let stored: Option<Option<String>> = connection
        .query_row(
            "SELECT objective_metric_json FROM campaigns WHERE campaign_id = ?1",
            [campaign_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read campaign objective metric"))?;
    match stored.flatten() {
        None => Ok(None),
        Some(text) => {
            let objective = serde_json::from_str::<ObjectiveMetric>(&text).map_err(|source| {
                AppError::Serialization {
                    operation: "parse campaign objective metric",
                    source,
                }
            })?;
            objective.validate()?;
            Ok(Some(objective))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn stage_from_verified_root_rejects_unsafe_experiment_ids() {
        let temporary = tempdir().unwrap();
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = temporary.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root = ProjectRootLogReader::open_for_tests(&root_path).unwrap();

        for experiment_id in ["", "foo/bar", "foo\\bar", ".", "..", "\0"] {
            assert!(
                stage_from_verified_root(&root, "project-a", experiment_id, 41, None, 100).is_err(),
                "unsafe experiment id was accepted: {experiment_id:?}"
            );
        }
        assert!(
            stage_from_verified_root(&root, "project-a", &"a".repeat(129), 41, None, 100,).is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn staged_terminal_result_does_not_persist_until_requested() {
        let temporary = tempdir().unwrap();
        fs::set_permissions(temporary.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = temporary.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let db = Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path, enabled, paused,
                    halted_reason, created_at, updated_at
                 ) VALUES ('project-a', ?1, 'group-a', ?2, 1, 0, NULL, 100, 100)",
                rusqlite::params![
                    root_path.to_str().unwrap(),
                    root_path.join(".pueue-agent/config.toml").to_str().unwrap(),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, state_reason, baseline_experiment_id,
                    next_eligible_at, objective_metric_json, current_best_experiment_id,
                    plateau_count, base_revision_sha, created_at, updated_at
                 ) VALUES ('campaign-a', 'project-a', 'objective', 'digest',
                    '[]', 'active', NULL, NULL, NULL, NULL, NULL, 0, NULL, 100, 100)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO proposals (
                    proposal_id, campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, canonical_digest, reject_reason,
                    created_at, updated_at
                 ) VALUES ('proposal-a', 'campaign-a', 'experiment', 'accepted',
                    'hypothesis', NULL, '[\"python\",\"train.py\"]', '.', '[]',
                    'proposal-digest', NULL, 100, 100)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status, kind, metadata_json,
                    origin_agent_run_id
                 ) VALUES ('submission-a', 'project-a', '[\"python\",\"train.py\"]',
                    100, 41, 'task-signature', 'accepted', 'experiment', '{}', NULL)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO experiments (
                    experiment_id, campaign_id, proposal_id, submission_id,
                    parent_experiment_id, attempt, status, pueue_task_id,
                    task_signature, failure_code, failure_fingerprint, created_at,
                    updated_at, finished_at
                 ) VALUES ('experiment-a', 'campaign-a', 'proposal-a', 'submission-a',
                    NULL, 0, 'accepted', 41, 'task-signature', NULL, NULL, 100, 100, NULL)",
                [],
            )
            .unwrap();
        let service = root_path.join(".pueue-agent");
        let results = service.join("results");
        fs::create_dir_all(&results).unwrap();
        fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&results, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(
            results.join("experiment-a.json"),
            br#"{"schema_version":1,"experiment_id":"experiment-a","metrics":{"loss":0.1}}"#,
        )
        .unwrap();
        fs::set_permissions(
            results.join("experiment-a.json"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();

        let root = ProjectRootLogReader::open_for_tests(&root_path).unwrap();
        let staged =
            stage_from_verified_root(&root, "project-a", "experiment-a", 41, None, 200).unwrap();
        assert!(MetricsRepository::get(&db, "experiment-a")
            .unwrap()
            .is_none());

        staged.persist(&db).unwrap();
        assert!(MetricsRepository::get(&db, "experiment-a")
            .unwrap()
            .is_some());
    }
}
