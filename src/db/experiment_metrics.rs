use rusqlite::{params, OptionalExtension};

use crate::{models::ExperimentMetricsRow, AppError};

use super::{database_error, Db};

const EXPERIMENT_METRICS_SELECT: &str = "SELECT
    experiment_id, source, primary_metric_name, primary_metric_value,
    metrics_json, artifact_defect, created_at, updated_at, evaluated_at
    FROM experiment_metrics";

pub struct MetricsRepository;

impl MetricsRepository {
    /// Simple upsert for general metrics writes. The terminal-ingestion path
    /// uses `insert_terminal_classification` when evidence must be frozen.
    pub fn upsert(db: &Db, row: &ExperimentMetricsRow) -> Result<(), AppError> {
        let connection = db.connect()?;
        connection
            .execute(
                "INSERT INTO experiment_metrics (
                    experiment_id, source, primary_metric_name, primary_metric_value,
                    metrics_json, artifact_defect, created_at, updated_at, evaluated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(experiment_id) DO UPDATE SET
                    source = excluded.source,
                    primary_metric_name = excluded.primary_metric_name,
                    primary_metric_value = excluded.primary_metric_value,
                    metrics_json = excluded.metrics_json,
                    artifact_defect = excluded.artifact_defect,
                    updated_at = excluded.updated_at,
                    evaluated_at = COALESCE(experiment_metrics.evaluated_at, excluded.evaluated_at)",
                params![
                    row.experiment_id,
                    row.source,
                    row.primary_metric_name,
                    row.primary_metric_value,
                    row.metrics_json,
                    row.artifact_defect,
                    row.created_at,
                    row.updated_at,
                    row.evaluated_at,
                ],
            )
            .map_err(database_error("upsert experiment metrics row"))?;
        Ok(())
    }

    /// Insert a terminal classification row, replacing only a prior retryable
    /// result I/O marker. Once a valid, missing, or invalid classification is
    /// stored, later ingestion attempts cannot overwrite its evidence.
    pub fn insert_terminal_classification(
        db: &Db,
        row: &ExperimentMetricsRow,
    ) -> Result<(), AppError> {
        let connection = db.connect()?;
        connection
            .execute(
                "INSERT INTO experiment_metrics (
                    experiment_id, source, primary_metric_name, primary_metric_value,
                    metrics_json, artifact_defect, created_at, updated_at, evaluated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(experiment_id) DO UPDATE SET
                    source = excluded.source,
                    primary_metric_name = excluded.primary_metric_name,
                    primary_metric_value = excluded.primary_metric_value,
                    metrics_json = excluded.metrics_json,
                    artifact_defect = excluded.artifact_defect,
                    updated_at = excluded.updated_at,
                    evaluated_at = COALESCE(experiment_metrics.evaluated_at, excluded.evaluated_at)
                 WHERE experiment_metrics.evaluated_at IS NULL
                   AND experiment_metrics.artifact_defect = 'result_io_error'",
                params![
                    row.experiment_id,
                    row.source,
                    row.primary_metric_name,
                    row.primary_metric_value,
                    row.metrics_json,
                    row.artifact_defect,
                    row.created_at,
                    row.updated_at,
                    row.evaluated_at,
                ],
            )
            .map_err(database_error("insert terminal classification"))?;
        Ok(())
    }

    pub fn get(
        db: &Db,
        experiment_id: &str,
    ) -> Result<Option<ExperimentMetricsRow>, AppError> {
        let connection = db.connect()?;
        connection
            .query_row(
                &format!("{EXPERIMENT_METRICS_SELECT} WHERE experiment_id = ?1"),
                [experiment_id],
                read_experiment_metrics_row,
            )
            .optional()
            .map_err(database_error("read experiment metrics row"))
    }
}

fn read_experiment_metrics_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ExperimentMetricsRow> {
    Ok(ExperimentMetricsRow {
        experiment_id: row.get(0)?,
        source: row.get(1)?,
        primary_metric_name: row.get(2)?,
        primary_metric_value: row.get(3)?,
        metrics_json: row.get(4)?,
        artifact_defect: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        evaluated_at: row.get(8)?,
    })
}
