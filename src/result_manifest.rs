//! Result-manifest ingestion for terminal experiment projections.
//!
//! A finished task's environment cannot be read after exit, so discovery
//! reconstructs the declared `PUEUE_AGENT_RESULT_PATH` first and then falls
//! back to the fixed project-relative default path. Manifest contents are
//! validated as numbers and identifiers only; artifact paths are never
//! followed or digested here.

use std::{
    collections::BTreeMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

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

fn read_manifest(path: &Path) -> Result<Option<Vec<u8>>, AppError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(AppError::Io {
                operation: "open result manifest",
                source,
            })
        }
    };
    let mut bytes = Vec::with_capacity(MAX_RESULT_MANIFEST_BYTES + 1);
    file.take((MAX_RESULT_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| AppError::Io {
            operation: "read result manifest",
            source,
        })?;
    Ok(Some(bytes))
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
    }
}

/// Ingest the finished experiment's result manifest, storing either the
/// validated metrics or a bounded artifact defect row. Idempotent per
/// experiment via the primary key on `experiment_metrics`.
///
/// On failure a best-effort bounded defect row is still persisted before the
/// error propagates, so a transient IO or database problem can never leave the
/// experiment without any durable metrics row; the upsert keeps later retries
/// authoritative.
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
        Err(error) => {
            let _ = MetricsRepository::upsert(db, &defect_row(experiment_id, "result_invalid", now));
            Err(error)
        }
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
        if let Some(bytes) = read_manifest(&path)? {
            outcome = Some(classify_manifest(&bytes, experiment_id)?);
            break;
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
        },
    };
    MetricsRepository::upsert(db, &row)
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
