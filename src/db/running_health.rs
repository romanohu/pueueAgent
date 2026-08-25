use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};

use crate::{
    models::{HealthState, RunningHealthRow, SignalSummaryEntry},
    AppError,
};

use super::{database_error, Db};

const MAX_SIGNAL_SUMMARY_ENTRIES: usize = 32;

const RUNNING_HEALTH_SELECT: &str = "SELECT
    experiment_id, campaign_id, project_id, pueue_task_id, state,
    observation_count, last_observed_at, signal_summary_json, diagnosis_json,
    created_at, updated_at
    FROM running_health";

pub struct HealthRepository;

impl HealthRepository {
    pub fn ensure_running(
        db: &Db,
        project_id: &str,
        campaign_id: &str,
        experiment_id: &str,
        pueue_task_id: i64,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin running health registration"))?;
        validate_experiment_lineage(&transaction, project_id, campaign_id, experiment_id)?;
        transaction
            .execute(
                "INSERT INTO running_health (
                    experiment_id, campaign_id, project_id, pueue_task_id, state,
                    observation_count, last_observed_at, signal_summary_json,
                    created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 'healthy', 0, ?5, '[]', ?5, ?5)
                 ON CONFLICT(experiment_id) DO NOTHING",
                params![experiment_id, campaign_id, project_id, pueue_task_id, now],
            )
            .map_err(database_error("register running health row"))?;
        transaction
            .commit()
            .map_err(database_error("commit running health registration"))
    }

    pub fn get(
        db: &Db,
        experiment_id: &str,
    ) -> Result<Option<RunningHealthRow>, AppError> {
        let connection = db.connect()?;
        connection
            .query_row(
                &format!("{RUNNING_HEALTH_SELECT} WHERE experiment_id = ?1"),
                [experiment_id],
                read_running_health_row,
            )
            .optional()
            .map_err(database_error("read running health row"))
    }

    /// Rows for an operator listing surface, most recently updated first.
    pub fn list_for_project(
        db: &Db,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<RunningHealthRow>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let fetch_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let connection = db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{RUNNING_HEALTH_SELECT}
                 WHERE project_id = ?1
                 ORDER BY updated_at DESC, experiment_id DESC
                 LIMIT ?2"
            ))
            .map_err(database_error("prepare running health list query"))?;
        let rows = statement
            .query_map(params![project_id, fetch_limit], read_running_health_row)
            .map_err(database_error("query running health list"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read running health list"))
    }

    pub fn due_observations(
        db: &Db,
        now: i64,
        interval_minutes: u32,
        limit: usize,
    ) -> Result<Vec<RunningHealthRow>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let due_before = now - i64::from(interval_minutes) * 60;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let connection = db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{RUNNING_HEALTH_SELECT}
                 WHERE last_observed_at <= ?1
                 ORDER BY last_observed_at, experiment_id
                 LIMIT ?2"
            ))
            .map_err(database_error("prepare due running health query"))?;
        let rows = statement
            .query_map(params![due_before, limit], read_running_health_row)
            .map_err(database_error("query due running health rows"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read due running health rows"))
    }

    pub fn record_observation(
        db: &Db,
        experiment_id: &str,
        observed_at: i64,
        entry: SignalSummaryEntry,
    ) -> Result<(), AppError> {
        let mut connection = db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin running health observation"))?;
        let current_summary: String = transaction
            .query_row(
                "SELECT signal_summary_json FROM running_health WHERE experiment_id = ?1",
                [experiment_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("find running health row for observation"))?
            .ok_or_else(|| {
                validation_error(
                    "experiment_id",
                    "has no running health row to observe",
                )
            })?;
        let mut summary: Vec<serde_json::Value> = serde_json::from_str(&current_summary)
            .map_err(|source| AppError::Serialization {
                operation: "parse stored running health signal summary",
                source,
            })?;
        let entry_value =
            serde_json::to_value(&entry).map_err(|source| AppError::Serialization {
                operation: "encode running health signal summary entry",
                source,
            })?;
        summary.push(entry_value);
        if summary.len() > MAX_SIGNAL_SUMMARY_ENTRIES {
            let excess = summary.len() - MAX_SIGNAL_SUMMARY_ENTRIES;
            summary.drain(0..excess);
        }
        let summary_json = serde_json::to_string(&summary).map_err(|source| {
            AppError::Serialization {
                operation: "store running health signal summary",
                source,
            }
        })?;
        let updated = transaction
            .execute(
                "UPDATE running_health
                 SET signal_summary_json = ?1, observation_count = observation_count + 1,
                     last_observed_at = ?2, updated_at = ?2
                 WHERE experiment_id = ?3",
                params![summary_json, observed_at, experiment_id],
            )
            .map_err(database_error("record running health observation"))?;
        if updated != 1 {
            return Err(validation_error(
                "experiment_id",
                "changed while recording a running health observation",
            ));
        }
        transaction
            .commit()
            .map_err(database_error("commit running health observation"))
    }

    pub fn mark_observed(db: &Db, experiment_id: &str, observed_at: i64) -> Result<(), AppError> {
        let connection = db.connect()?;
        let updated = connection
            .execute(
                "UPDATE running_health
                 SET observation_count = observation_count + 1, last_observed_at = ?1,
                     updated_at = ?1
                 WHERE experiment_id = ?2",
                params![observed_at, experiment_id],
            )
            .map_err(database_error("mark running health observed"))?;
        require_running_health_row(updated)
    }

    pub fn set_state(
        db: &Db,
        experiment_id: &str,
        state: HealthState,
        now: i64,
    ) -> Result<(), AppError> {
        let connection = db.connect()?;
        let updated = connection
            .execute(
                "UPDATE running_health SET state = ?1, updated_at = ?2
                 WHERE experiment_id = ?3",
                params![state, now, experiment_id],
            )
            .map_err(database_error("set running health state"))?;
        require_running_health_row(updated)
    }

    pub fn store_diagnosis(
        db: &Db,
        experiment_id: &str,
        diagnosis: &serde_json::Value,
        now: i64,
    ) -> Result<(), AppError> {
        let diagnosis_json =
            serde_json::to_string(diagnosis).map_err(|source| AppError::Serialization {
                operation: "store running health diagnosis",
                source,
            })?;
        let connection = db.connect()?;
        let updated = connection
            .execute(
                "UPDATE running_health SET diagnosis_json = ?1, updated_at = ?2
                 WHERE experiment_id = ?3",
                params![diagnosis_json, now, experiment_id],
            )
            .map_err(database_error("store running health diagnosis"))?;
        require_running_health_row(updated)
    }

    pub fn reset_to_healthy(db: &Db, experiment_id: &str, now: i64) -> Result<(), AppError> {
        let connection = db.connect()?;
        let updated = connection
            .execute(
                "UPDATE running_health SET state = 'healthy', updated_at = ?1
                 WHERE experiment_id = ?2",
                params![now, experiment_id],
            )
            .map_err(database_error("reset running health state"))?;
        require_running_health_row(updated)
    }

    pub fn delete_for_experiment(db: &Db, experiment_id: &str) -> Result<(), AppError> {
        let connection = db.connect()?;
        connection
            .execute(
                "DELETE FROM running_health WHERE experiment_id = ?1",
                [experiment_id],
            )
            .map_err(database_error("delete running health row"))?;
        Ok(())
    }

    /// Suspicious rows with diagnosis attempts left, oldest update first.
    pub fn due_diagnoses(db: &Db, limit: usize) -> Result<Vec<RunningHealthRow>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let fetch_limit = i64::try_from(limit.saturating_mul(4).max(limit)).unwrap_or(i64::MAX);
        let connection = db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{RUNNING_HEALTH_SELECT}
                 WHERE state = 'suspicious'
                 ORDER BY updated_at, experiment_id
                 LIMIT ?1"
            ))
            .map_err(database_error("prepare due running health diagnoses query"))?;
        let rows = statement
            .query_map([fetch_limit], read_running_health_row)
            .map_err(database_error("query due running health diagnoses"))?;
        let rows = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read due running health diagnoses"))?;
        Ok(rows
            .into_iter()
            .filter(|row| row.diagnosis_attempt_count() < crate::models::MAX_DIAGNOSIS_ATTEMPTS)
            .take(limit)
            .collect())
    }

    /// ActionPending rows carrying a stored diagnosis, oldest update first.
    pub fn pending_actions(db: &Db, limit: usize) -> Result<Vec<RunningHealthRow>, AppError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let fetch_limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let connection = db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{RUNNING_HEALTH_SELECT}
                 WHERE state = 'action_pending' AND diagnosis_json IS NOT NULL
                 ORDER BY updated_at, experiment_id
                 LIMIT ?1"
            ))
            .map_err(database_error("prepare pending running health actions query"))?;
        let rows = statement
            .query_map([fetch_limit], read_running_health_row)
            .map_err(database_error("query pending running health actions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read pending running health actions"))
    }

    /// Revert diagnosing rows whose diagnosis run is no longer live so the
    /// bounded retry budget can resume after a daemon restart.
    pub fn requeue_interrupted_diagnoses(db: &Db, now: i64) -> Result<usize, AppError> {
        let connection = db.connect()?;
        let updated = connection
            .execute(
                "UPDATE running_health SET state = 'suspicious', updated_at = ?1
                 WHERE state = 'diagnosing' AND NOT EXISTS (
                     SELECT 1 FROM agent_run_events are
                     JOIN events e ON e.project_id = are.project_id AND e.event_id = are.event_id
                     JOIN agent_runs ar ON ar.project_id = are.project_id AND ar.run_id = are.run_id
                     WHERE e.kind = 'health_diagnosis'
                       AND e.experiment_id = running_health.experiment_id
                       AND ar.status IN ('starting', 'running'))",
                params![now],
            )
            .map_err(database_error("requeue interrupted running health diagnoses"))?;
        Ok(updated)
    }
}

fn read_running_health_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunningHealthRow> {
    Ok(RunningHealthRow {
        experiment_id: row.get(0)?,
        campaign_id: row.get(1)?,
        project_id: row.get(2)?,
        pueue_task_id: row.get(3)?,
        state: row.get(4)?,
        observation_count: row.get(5)?,
        last_observed_at: row.get(6)?,
        signal_summary_json: row.get(7)?,
        diagnosis_json: row.get(8)?,
        created_at: row.get(9)?,
        updated_at: row.get(10)?,
    })
}

fn require_running_health_row(updated: usize) -> Result<(), AppError> {
    if updated == 1 {
        Ok(())
    } else {
        Err(validation_error(
            "experiment_id",
            "has no running health row",
        ))
    }
}

fn validate_experiment_lineage(
    transaction: &Transaction<'_>,
    project_id: &str,
    campaign_id: &str,
    experiment_id: &str,
) -> Result<(), AppError> {
    let actual: Option<(String, String)> = transaction
        .query_row(
            "SELECT c.project_id, e.campaign_id
             FROM experiments e
             JOIN campaigns c ON c.campaign_id = e.campaign_id
             WHERE e.experiment_id = ?1",
            [experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(database_error("read running health experiment lineage"))?;
    match actual {
        Some((actual_project_id, actual_campaign_id))
            if actual_project_id == project_id && actual_campaign_id == campaign_id =>
        {
            Ok(())
        }
        _ => Err(validation_error(
            "experiment_id",
            "must identify an existing experiment in the campaign lineage",
        )),
    }
}

fn validation_error(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}
