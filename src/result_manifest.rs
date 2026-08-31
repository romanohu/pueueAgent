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

#[cfg(not(unix))]
use std::fs::File;

use rusqlite::OptionalExtension;
use serde_json::Value;

use crate::{
    db::{database_error, Db, MetricsRepository},
    models::{ExperimentMetricsRow, ObjectiveMetric},
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

fn read_manifest(path: &Path) -> Result<Option<ManifestRead>, AppError> {
    #[cfg(unix)]
    let file_result = {
        use std::os::unix::fs::OpenOptionsExt;

        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)
    };

    #[cfg(not(unix))]
    let file_result = File::open(path);

    let file = match file_result {
        Ok(file) => file,
        #[cfg(not(unix))]
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        #[cfg(unix)]
        Err(source) => {
            // The safe open above remains the security decision. This
            // no-follow probe only classifies an already-rejected candidate
            // and never authorizes a later read.
            match std::fs::symlink_metadata(path) {
                Ok(metadata) => {
                    let file_type = metadata.file_type();
                    if file_type.is_symlink() || !file_type.is_file() {
                        return Ok(Some(ManifestRead::Invalid));
                    }
                }
                Err(metadata_source) if metadata_source.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(_) => {
                    return Err(AppError::Io {
                        operation: "open result manifest",
                        source,
                    });
                }
            }
            return Err(AppError::Io {
                operation: "open result manifest",
                source,
            });
        }
        #[cfg(not(unix))]
        Err(source) => {
            return Err(AppError::Io {
                operation: "open result manifest",
                source,
            })
        }
    };
    if !file
        .metadata()
        .map_err(|source| AppError::Io {
            operation: "read result manifest metadata",
            source,
        })?
        .file_type()
        .is_file()
    {
        return Ok(Some(ManifestRead::Invalid));
    }
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
    match ingest_inner(
        db,
        project_root,
        project_id,
        experiment_id,
        pueue_task_id,
        objective,
        now,
    ) {
        Ok(()) => Ok(()),
        Err(error @ AppError::Io { .. }) => {
            // A read/open failure is retryable while the experiment remains
            // nonterminal. The terminal-classification insert cannot overwrite
            // an already-frozen result row.
            MetricsRepository::insert_terminal_classification(
                db,
                &defect_row(experiment_id, "result_io_error", now),
            )?;
            Err(error)
        }
        Err(error) => Err(error),
    }
}

#[allow(clippy::too_many_arguments)]
fn ingest_inner(
    db: &Db,
    project_root: &Path,
    _project_id: &str,
    experiment_id: &str,
    pueue_task_id: i64,
    objective: Option<&ObjectiveMetric>,
    now: i64,
) -> Result<(), AppError> {
    let mut outcome = None;
    for path in result_path_candidates(project_root, experiment_id, pueue_task_id) {
        match read_manifest(&path)? {
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
    let row = match outcome {
        None => defect_row(experiment_id, "result_missing", now),
        Some(ManifestOutcome::Invalid) => defect_row(experiment_id, "result_invalid", now),
        Some(ManifestOutcome::Valid {
            metrics,
            metrics_json,
        }) => ExperimentMetricsRow {
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
    };
    // Freeze the first terminal classification and allow recovery only from a
    // prior retryable result_io_error marker.
    MetricsRepository::insert_terminal_classification(db, &row)?;
    Ok(())
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
        Some(text) => serde_json::from_str(&text)
            .map(Some)
            .map_err(|source| AppError::Serialization {
                operation: "parse campaign objective metric",
                source,
            }),
    }
}
