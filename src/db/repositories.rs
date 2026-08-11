use std::{collections::BTreeMap, fs, path::PathBuf};

use rusqlite::{
    params, types::Type, Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};
use serde_json::json;
use uuid::Uuid;

use crate::{
    batches::{
        derive_request_status, validate_accepted_result, validate_error, validate_new_batch,
        BatchJobResult, MAX_BATCH_ARGV_JSON_BYTES, MAX_BATCH_METADATA_JSON_BYTES,
    },
    diagnostics::{EventFilter, MAX_EVENT_LIST_LIMIT},
    interventions::{
        validate_message, Intervention, InterventionCounts, InterventionReservation,
        MAX_INTERVENTIONS_PER_RUN, MAX_INTERVENTION_BYTES_PER_RUN,
    },
    models::{
        launch_gate_marker_path, path_text, AgentContextMode, AgentRun, AgentRunEvent,
        AgentRunStatus, BatchJob, BatchJobStatus, BatchRequest, BatchStatus, Event, EventKind,
        EventStatus, Incident, IncidentTransition, IncidentUpdate, IntegrationEvent,
        InterventionStatus, NewAgentRun, NewBatchRequest, NewEvent, NewIncident,
        NewIntegrationEvent, NewProject, NewSubmission, NewTaskObservation, NewTerminationRequest,
        Project, Submission, SubmissionKind, SubmissionStatus, TaskObservation, TerminationRequest,
        TerminationRequestStatus,
    },
    output::bounded_redacted_text,
    AppError,
};

use super::{database_error, Db};

pub struct ProjectRepository<'db> {
    db: &'db Db,
}

impl<'db> ProjectRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn register(&self, project: &NewProject) -> Result<Project, AppError> {
        let canonical_root =
            fs::canonicalize(&project.root_path).map_err(|source| AppError::Io {
                operation: "canonicalize project root",
                source,
            })?;
        let root_path = path_text(&canonical_root, "root_path")?;
        let config_path = path_text(&project.config_path, "config_path")?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project registration"))?;

        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE root_path = ?1",
            root_path,
        )? {
            return Err(AppError::DatabaseConflict { field: "root_path" });
        }
        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE pueue_group = ?1",
            &project.pueue_group,
        )? {
            return Err(AppError::DatabaseConflict {
                field: "pueue_group",
            });
        }
        if exists(
            &transaction,
            "SELECT 1 FROM projects WHERE project_id = ?1",
            &project.project_id,
        )? {
            return Err(AppError::DatabaseConflict {
                field: "project_id",
            });
        }

        transaction
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path, enabled, paused,
                    halted_reason, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, ?7, ?7)",
                params![
                    project.project_id,
                    root_path,
                    project.pueue_group,
                    config_path,
                    project.enabled,
                    project.paused,
                    project.created_at,
                ],
            )
            .map_err(project_constraint_error)?;
        let registered = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [&project.project_id],
                project_from_row,
            )
            .map_err(database_error("read registered project"))?;
        transaction
            .commit()
            .map_err(database_error("commit project registration"))?;
        Ok(registered)
    }

    pub fn find_by_group(&self, pueue_group: &str) -> Result<Option<Project>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE pueue_group = ?1",
                [pueue_group],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by Pueue group"))
    }

    pub fn find_by_id(&self, project_id: &str) -> Result<Option<Project>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by ID"))
    }

    pub fn list_enabled(&self) -> Result<Vec<Project>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE enabled = 1 ORDER BY project_id",
            )
            .map_err(database_error("prepare enabled project query"))?;
        let projects = statement
            .query_map([], project_from_row)
            .map_err(database_error("list enabled projects"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read enabled projects"))?;
        Ok(projects)
    }

    pub fn list_active(&self) -> Result<Vec<Project>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects
                 WHERE enabled = 1 AND paused = 0 AND halted_reason IS NULL
                 ORDER BY project_id",
            )
            .map_err(database_error("prepare active project query"))?;
        let projects = statement
            .query_map([], project_from_row)
            .map_err(database_error("list active projects"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read active projects"))?;
        Ok(projects)
    }

    pub fn pause(&self, project_id: &str, now: i64) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project pause"))?;
        transaction
            .execute(
                "UPDATE projects SET paused = 1, updated_at = ?1 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("pause project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read paused project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "pause",
            &json!({
                "paused": true,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project pause"))?;
        Ok(project)
    }

    pub fn resume(&self, project_id: &str, now: i64) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project resume"))?;
        let was_halted: Option<String> = transaction
            .query_row(
                "SELECT halted_reason FROM projects WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("read project halt state before resume"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET paused = 0, halted_reason = NULL, updated_at = ?1
                 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("resume project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read resumed project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "resume",
            &json!({
                "paused": false,
                "cleared_halt": was_halted.is_some(),
                "previous_halted_reason": was_halted,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project resume"))?;
        Ok(project)
    }

    pub fn halt(&self, project_id: &str, reason: &str, now: i64) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project halt"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET paused = 1, halted_reason = ?1, updated_at = ?2
                 WHERE project_id = ?3",
                params![reason, now, project_id],
            )
            .map_err(database_error("halt project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read halted project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "halt",
            &json!({
                "paused": true,
                "halted_reason": reason,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project halt"))?;
        Ok(project)
    }

    pub fn disable(
        &self,
        project_id: &str,
        now: i64,
        unresolved_task_ids: &[i64],
    ) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project disable"))?;
        transaction
            .execute(
                "UPDATE projects
                 SET enabled = 0, paused = 1, updated_at = ?1
                 WHERE project_id = ?2",
                params![now, project_id],
            )
            .map_err(database_error("disable project"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read disabled project"))?;
        insert_operator_log(
            &transaction,
            &project,
            "disable",
            &json!({
                "group_released": false,
                "mode": "keep_reservation",
                "unresolved_task_count": unresolved_task_ids.len(),
                "unresolved_task_ids": unresolved_task_ids,
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit project disable"))?;
        Ok(project)
    }

    pub fn remove(
        &self,
        project_id: &str,
        now: i64,
        unresolved_task_ids: &[i64],
    ) -> Result<Project, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin project removal"))?;
        let project = transaction
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE project_id = ?1",
                [project_id],
                project_from_row,
            )
            .map_err(database_error("read project before removal"))?;
        insert_operator_log(
            &transaction,
            &project,
            "remove",
            &json!({
                "group_released": true,
                "mode": "remove",
                "unresolved_task_count": unresolved_task_ids.len(),
                "unresolved_task_ids": unresolved_task_ids,
            }),
            now,
        )?;
        transaction
            .execute("DELETE FROM projects WHERE project_id = ?1", [project_id])
            .map_err(database_error("remove project"))?;
        transaction
            .commit()
            .map_err(database_error("commit project removal"))?;
        Ok(project)
    }

    pub fn record_task_cancellation(
        &self,
        project: &Project,
        task_id: i64,
        task_signature: &str,
        requested_state: &str,
        final_state: &str,
        reason: &str,
        now: i64,
    ) -> Result<(), AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin task cancellation operator log"))?;
        insert_operator_log(
            &transaction,
            project,
            "cancel",
            &json!({
                "task_id": task_id,
                "task_signature": bounded_redacted_text(task_signature),
                "requested_state": bounded_redacted_text(requested_state),
                "final_state": bounded_redacted_text(final_state),
                "reason": bounded_redacted_text(reason),
            }),
            now,
        )?;
        transaction
            .commit()
            .map_err(database_error("commit task cancellation operator log"))
    }

    pub fn find_by_root(&self, root: &std::path::Path) -> Result<Option<Project>, AppError> {
        let canonical_root = match fs::canonicalize(root) {
            Ok(path) => path,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(AppError::Io {
                    operation: "canonicalize project root for lookup",
                    source,
                })
            }
        };
        let root_path = path_text(&canonical_root, "root_path")?;
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT project_id, root_path, pueue_group, config_path, enabled, paused,
                        halted_reason, created_at, updated_at
                 FROM projects WHERE root_path = ?1",
                [root_path],
                project_from_row,
            )
            .optional()
            .map_err(database_error("find project by canonical root"))
    }
}

fn insert_operator_log(
    transaction: &Transaction<'_>,
    project: &Project,
    action: &'static str,
    details: &serde_json::Value,
    now: i64,
) -> Result<(), AppError> {
    let details_json =
        serde_json::to_string(details).map_err(|source| AppError::Serialization {
            operation: "serialize operator log details",
            source,
        })?;
    transaction
        .execute(
            "INSERT INTO operator_logs (
                project_id, pueue_group, action, details_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                project.project_id,
                project.pueue_group,
                action,
                details_json,
                now,
            ],
        )
        .map_err(database_error("insert operator log"))?;
    Ok(())
}

pub struct EventRepository<'db> {
    db: &'db Db,
}

impl<'db> EventRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(&self, event: &NewEvent) -> Result<Event, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize event payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent event insert"))?;
        transaction
            .execute(
                "INSERT INTO events (
                    project_id, kind, dedup_key, payload_json, status, attempts,
                    not_before, lease_until, created_at, completed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, 'pending', 0, ?5, NULL, ?6, NULL, NULL)
                 ON CONFLICT(project_id, dedup_key) DO NOTHING",
                params![
                    event.project_id,
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                ],
            )
            .map_err(database_error("insert event"))?;
        let stored = transaction
            .query_row(
                &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
                params![event.project_id, event.dedup_key],
                event_from_row,
            )
            .map_err(database_error("read idempotent event"))?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent event insert"))?;
        Ok(stored)
    }

    pub fn find_by_id(&self, event_id: i64) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                [event_id],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find event by ID"))
    }

    pub fn find_by_dedup_key(
        &self,
        project_id: &str,
        dedup_key: &str,
    ) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE project_id = ?1 AND dedup_key = ?2", EVENT_SELECT),
                params![project_id, dedup_key],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find event by deduplication key"))
    }

    pub fn find_terminal_by_pueue_task(
        &self,
        project_id: &str,
        pueue_group: &str,
        pueue_task_id: i64,
    ) -> Result<Option<Event>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1
                        AND kind IN ('task_finished', 'task_failed')
                        AND json_extract(payload_json, '$.group') = ?2
                        AND json_extract(payload_json, '$.task_id') = ?3
                     ORDER BY event_id DESC
                     LIMIT 1",
                    EVENT_SELECT
                ),
                params![project_id, pueue_group, pueue_task_id],
                event_from_row,
            )
            .optional()
            .map_err(database_error("find terminal event by Pueue task"))
    }

    pub fn replace_pending(
        &self,
        event_id: i64,
        event: &NewEvent,
    ) -> Result<Option<Event>, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize replacement event payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pending event replacement"))?;
        let changed = transaction
            .execute(
                "UPDATE events
                 SET kind = ?1, dedup_key = ?2, payload_json = ?3,
                     not_before = ?4, created_at = ?5
                 WHERE event_id = ?6 AND status = 'pending'",
                params![
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                    event_id,
                ],
            )
            .map_err(database_error("replace pending event"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(
                transaction
                    .query_row(
                        &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                        [event_id],
                        event_from_row,
                    )
                    .map_err(database_error("read replaced event"))?,
            )
        };
        transaction
            .commit()
            .map_err(database_error("commit pending event replacement"))?;
        Ok(stored)
    }

    pub fn replace_callback_with_terminal(
        &self,
        event_id: i64,
        event: &NewEvent,
    ) -> Result<Event, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize terminal callback replacement payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin callback terminal replacement"))?;
        let changed = transaction
            .execute(
                "UPDATE events
                 SET kind = ?1, dedup_key = ?2, payload_json = ?3,
                     not_before = ?4, created_at = ?5
                 WHERE event_id = ?6",
                params![
                    event.kind,
                    event.dedup_key,
                    payload_json,
                    event.not_before,
                    event.created_at,
                    event_id,
                ],
            )
            .map_err(database_error("replace callback with terminal event"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "replace callback with terminal event",
            });
        }
        let stored = transaction
            .query_row(
                &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                [event_id],
                event_from_row,
            )
            .map_err(database_error("read terminal callback replacement"))?;
        transaction
            .commit()
            .map_err(database_error("commit callback terminal replacement"))?;
        Ok(stored)
    }

    pub fn discard_pending(&self, event_id: i64) -> Result<bool, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pending callback cleanup"))?;
        let removed = transaction
            .execute(
                "DELETE FROM events WHERE event_id = ?1 AND status = 'pending'",
                [event_id],
            )
            .map_err(database_error("discard duplicate pending callback"))?;
        transaction
            .commit()
            .map_err(database_error("commit pending callback cleanup"))?;
        Ok(removed == 1)
    }

    pub fn claim_batch(
        &self,
        now: i64,
        lease_until: i64,
        limit: usize,
    ) -> Result<Vec<Event>, AppError> {
        if lease_until <= now {
            return Err(AppError::Configuration {
                field: "event_lease",
            });
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit).map_err(|_| AppError::Configuration {
            field: "event_claim_limit",
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin immediate event claim"))?;
        let event_ids = {
            let mut statement = transaction
                .prepare(
                    "SELECT event_id FROM events
                     WHERE status IN ('pending', 'retry_wait') AND not_before <= ?1
                       AND EXISTS (
                           SELECT 1 FROM projects
                           WHERE projects.project_id = events.project_id
                             AND enabled = 1
                             AND paused = 0
                             AND halted_reason IS NULL
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM agent_runs
                           WHERE agent_runs.project_id = events.project_id
                             AND status IN ('starting', 'running')
                       )
                     ORDER BY created_at, event_id
                     LIMIT ?2",
                )
                .map_err(database_error("prepare event claim"))?;
            let rows = statement
                .query_map(params![now, limit], |row| row.get::<_, i64>(0))
                .map_err(database_error("query claimable events"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read claimable events"))?;
            rows
        };

        for event_id in &event_ids {
            transaction
                .execute(
                    "UPDATE events
                     SET status = 'claimed', lease_until = ?1, attempts = attempts + 1
                     WHERE event_id = ?2
                       AND status IN ('pending', 'retry_wait')
                       AND not_before <= ?3",
                    params![lease_until, event_id, now],
                )
                .map_err(database_error("claim event"))?;
        }

        let mut events = Vec::with_capacity(event_ids.len());
        for event_id in event_ids {
            events.push(
                transaction
                    .query_row(
                        &format!("{} WHERE event_id = ?1", EVENT_SELECT),
                        [event_id],
                        event_from_row,
                    )
                    .map_err(database_error("read claimed event"))?,
            );
        }
        transaction
            .commit()
            .map_err(database_error("commit event claim"))?;
        Ok(events)
    }

    pub fn recover_expired_claims(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin expired claim recovery"))?;
        let recovered = transaction
            .execute(
                "UPDATE events
                 SET status = 'pending', lease_until = NULL
                 WHERE status = 'claimed' AND lease_until <= ?1",
                [now],
            )
            .map_err(database_error("recover expired event claims"))?;
        transaction
            .commit()
            .map_err(database_error("commit expired claim recovery"))?;
        Ok(recovered)
    }

    pub fn transition_many(
        &self,
        event_ids: &[i64],
        status: EventStatus,
        now: i64,
        not_before: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<usize, AppError> {
        if event_ids.is_empty() {
            return Ok(0);
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin event status transition"))?;
        let mut changed = 0;
        for event_id in event_ids {
            changed += transaction
                .execute(
                    "UPDATE events
                     SET status = ?1,
                         lease_until = NULL,
                         not_before = COALESCE(?2, not_before),
                         completed_at = CASE WHEN ?1 IN ('completed', 'failed') THEN ?3 ELSE completed_at END,
                         last_error = ?4
                     WHERE event_id = ?5",
                    params![status, not_before, now, last_error, event_id],
                )
                .map_err(database_error("transition event status"))?;
        }
        transaction
            .commit()
            .map_err(database_error("commit event status transition"))?;
        Ok(changed)
    }

    pub fn recent_events(&self, project_id: &str, limit: usize) -> Result<Vec<Event>, AppError> {
        let limit = i64::try_from(limit).map_err(|_| AppError::Configuration {
            field: "event_query_limit",
        })?;
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?2",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare recent event query"))?;
        let rows = statement
            .query_map(params![project_id, limit], event_from_row)
            .map_err(database_error("query recent events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read recent events"))
    }

    pub fn list_filtered(
        &self,
        project_id: &str,
        filter: &EventFilter,
    ) -> Result<Vec<Event>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 AND (?2 IS NULL OR kind = ?2)
                 AND (?3 IS NULL OR status = ?3)
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?4",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare filtered event query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    filter.kind,
                    filter.status,
                    bounded_diagnostic_limit(filter.limit),
                ],
                event_from_row,
            )
            .map_err(database_error("query filtered events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read filtered events"))
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<Event>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 AND json_extract(payload_json, '$.task_signature') = ?2
                 ORDER BY created_at DESC, event_id DESC
                 LIMIT ?3",
                EVENT_SELECT
            ))
            .map_err(database_error("prepare task event query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                event_from_row,
            )
            .map_err(database_error("query task events"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task events"))
    }

    pub fn count_consecutive_failures(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<u32, AppError> {
        let mut count = 0;
        for event in self.recent_events(project_id, limit)? {
            match event.kind {
                EventKind::Crash
                | EventKind::TaskFailed
                | EventKind::Stalled
                | EventKind::AutoKilled
                | EventKind::TerminationFailed => count += 1,
                EventKind::TaskFinished | EventKind::DeepCheck | EventKind::OperatorWake => break,
            }
        }
        Ok(count)
    }
}

pub struct IntegrationEventRepository<'db> {
    db: &'db Db,
}

impl<'db> IntegrationEventRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(
        &self,
        event: &NewIntegrationEvent,
    ) -> Result<IntegrationEvent, AppError> {
        let payload_json =
            serde_json::to_string(&event.payload).map_err(|source| AppError::Serialization {
                operation: "serialize integration event payload",
                source,
            })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent integration event insert"))?;
        transaction
            .execute(
                "INSERT INTO integration_events (
                    kind, dedup_key, payload_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(dedup_key) DO NOTHING",
                params![event.kind, event.dedup_key, payload_json, event.created_at],
            )
            .map_err(database_error("insert integration event"))?;
        let stored = transaction
            .query_row(
                &format!("{} WHERE dedup_key = ?1", INTEGRATION_EVENT_SELECT),
                [&event.dedup_key],
                integration_event_from_row,
            )
            .map_err(database_error("read idempotent integration event"))?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent integration event insert"))?;
        Ok(stored)
    }
}

pub struct IncidentRepository<'db> {
    db: &'db Db,
}

impl<'db> IncidentRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn upsert_active(&self, incident: &NewIncident) -> Result<IncidentUpdate, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin active incident upsert"))?;
        let latest_resolved = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
                     AND status = 'resolved' AND resolved_at IS NOT NULL
                     ORDER BY resolved_at DESC, incident_id DESC LIMIT 1",
                    INCIDENT_SELECT
                ),
                params![incident.project_id, incident.kind, incident.fingerprint],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("read latest resolved incident"))?;

        let update = if let Some(resolved) = latest_resolved.filter(|resolved| {
            resolved
                .resolved_at
                .is_some_and(|at| incident.seen_at <= at)
        }) {
            IncidentUpdate {
                incident: resolved,
                transition: IncidentTransition::Unchanged,
            }
        } else {
            let active = transaction
                .query_row(
                    &format!(
                        "{} WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
                         AND status IN ('open', 'acknowledged')",
                        INCIDENT_SELECT
                    ),
                    params![incident.project_id, incident.kind, incident.fingerprint],
                    incident_from_row,
                )
                .optional()
                .map_err(database_error("read active incident"))?;
            if let Some(active) = active {
                let task_changed = incident
                    .task_key
                    .as_ref()
                    .is_some_and(|task_key| Some(task_key) != active.task_key.as_ref());
                transaction
                    .execute(
                        "UPDATE incidents
                         SET task_key = COALESCE(?1, task_key),
                             last_seen_at = MAX(last_seen_at, ?2)
                         WHERE incident_id = ?3",
                        params![incident.task_key, incident.seen_at, active.incident_id],
                    )
                    .map_err(database_error("update active incident"))?;
                let stored = read_incident(&transaction, active.incident_id)?;
                IncidentUpdate {
                    incident: stored,
                    transition: if task_changed {
                        IncidentTransition::Updated
                    } else {
                        IncidentTransition::Unchanged
                    },
                }
            } else {
                transaction
                    .execute(
                        "INSERT INTO incidents (
                            project_id, kind, task_key, fingerprint, status,
                            first_seen_at, last_seen_at, acknowledged_at, resolved_at
                         ) VALUES (?1, ?2, ?3, ?4, 'open', ?5, ?5, NULL, NULL)",
                        params![
                            incident.project_id,
                            incident.kind,
                            incident.task_key,
                            incident.fingerprint,
                            incident.seen_at,
                        ],
                    )
                    .map_err(database_error("insert active incident"))?;
                let stored = read_incident(&transaction, transaction.last_insert_rowid())?;
                IncidentUpdate {
                    incident: stored,
                    transition: IncidentTransition::Opened,
                }
            }
        };

        transaction
            .commit()
            .map_err(database_error("commit active incident upsert"))?;
        Ok(update)
    }

    pub fn resolve(
        &self,
        incident_id: i64,
        resolved_at: i64,
    ) -> Result<IncidentTransition, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin incident resolution"))?;
        let changed = transaction
            .execute(
                "UPDATE incidents
                 SET status = 'resolved', resolved_at = ?1,
                     last_seen_at = MAX(last_seen_at, ?1)
                 WHERE incident_id = ?2 AND status IN ('open', 'acknowledged')",
                params![resolved_at, incident_id],
            )
            .map_err(database_error("resolve incident"))?;
        if changed == 0 {
            return Err(AppError::Runtime {
                operation: "resolve an active incident",
            });
        }
        transaction
            .commit()
            .map_err(database_error("commit incident resolution"))?;
        Ok(IncidentTransition::Resolved)
    }

    pub fn find_by_id(&self, incident_id: i64) -> Result<Option<Incident>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE incident_id = ?1", INCIDENT_SELECT),
                [incident_id],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("find incident by ID"))
    }

    pub fn find_by_project_and_id(
        &self,
        project_id: &str,
        incident_id: i64,
    ) -> Result<Option<Incident>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND incident_id = ?2",
                    INCIDENT_SELECT
                ),
                params![project_id, incident_id],
                incident_from_row,
            )
            .optional()
            .map_err(database_error("find project incident by ID"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<Incident>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT ?2",
                INCIDENT_SELECT
            ))
            .map_err(database_error("prepare project incident query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                incident_from_row,
            )
            .map_err(database_error("query project incidents"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project incidents"))
    }

    pub fn find_by_task_key(
        &self,
        project_id: &str,
        task_key: &str,
        limit: usize,
    ) -> Result<Vec<Incident>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_key = ?2
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT ?3",
                INCIDENT_SELECT
            ))
            .map_err(database_error("prepare task incident query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_key, bounded_diagnostic_limit(limit)],
                incident_from_row,
            )
            .map_err(database_error("query task incidents"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task incidents"))
    }
}

pub struct SubmissionRepository<'db> {
    db: &'db Db,
}

pub struct BatchRepository<'db> {
    db: &'db Db,
}

impl<'db> BatchRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn create_or_get(&self, request: &NewBatchRequest) -> Result<BatchRequest, AppError> {
        validate_new_batch(request)?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch create or get"))?;
        let existing = transaction
            .query_row(
                "SELECT project_id, manifest_hash
                 FROM batch_requests WHERE request_id = ?1",
                [&request.request_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(database_error("find existing batch request"))?;

        if let Some((project_id, manifest_hash)) = existing {
            if project_id != request.project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "request_id belongs to another project",
                });
            }
            if manifest_hash != request.manifest_hash {
                return Err(AppError::Validation {
                    field: "manifest_hash",
                    message: "request_id already has a different manifest",
                });
            }
            let stored = read_batch(&transaction, &request.project_id, &request.request_id)?
                .ok_or(AppError::Runtime {
                    operation: "read existing batch request",
                })?;
            transaction
                .commit()
                .map_err(database_error("commit existing batch request"))?;
            return Ok(stored);
        }

        transaction
            .execute(
                "INSERT INTO batch_requests (
                    request_id, project_id, manifest_hash, status,
                    lease_until, lease_token, created_at, updated_at, last_error
                 ) VALUES (?1, ?2, ?3, 'pending', NULL, NULL, ?4, ?4, NULL)",
                params![
                    request.request_id,
                    request.project_id,
                    request.manifest_hash,
                    request.created_at,
                ],
            )
            .map_err(database_error("insert batch request intent"))?;

        for job in &request.jobs {
            let argv_json =
                serde_json::to_string(&job.argv).map_err(|source| AppError::Serialization {
                    operation: "serialize batch job arguments",
                    source,
                })?;
            let metadata_json =
                serde_json::to_string(&job.metadata).map_err(|source| AppError::Serialization {
                    operation: "serialize batch job metadata",
                    source,
                })?;
            debug_assert!(argv_json.len() <= MAX_BATCH_ARGV_JSON_BYTES);
            debug_assert!(metadata_json.len() <= MAX_BATCH_METADATA_JSON_BYTES);
            transaction
                .execute(
                    "INSERT INTO batch_jobs (
                        request_id, job_id, ordinal, kind, argv_json, metadata_json,
                        status, pueue_task_id, submission_id, last_error
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, NULL, NULL)",
                    params![
                        request.request_id,
                        job.job_id,
                        job.ordinal,
                        job.kind,
                        argv_json,
                        metadata_json,
                    ],
                )
                .map_err(database_error("insert batch job intent"))?;
        }

        let stored = read_batch(&transaction, &request.project_id, &request.request_id)?.ok_or(
            AppError::Runtime {
                operation: "read created batch request",
            },
        )?;
        transaction
            .commit()
            .map_err(database_error("commit batch request intent"))?;
        Ok(stored)
    }

    pub fn find(
        &self,
        project_id: &str,
        request_id: &str,
    ) -> Result<Option<BatchRequest>, AppError> {
        let connection = self.db.connect()?;
        read_batch(&connection, project_id, request_id)
    }

    pub fn claim(
        &self,
        project_id: &str,
        request_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<BatchRequest>, AppError> {
        if lease_until <= now {
            return Err(AppError::Validation {
                field: "lease_until",
                message: "must be later than now",
            });
        }
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch claim"))?;
        let lease_token = Uuid::new_v4().to_string();
        let changed = transaction
            .execute(
                "UPDATE batch_requests
                 SET status = 'dispatching', lease_until = ?1,
                     lease_token = ?2, updated_at = ?3
                 WHERE request_id = ?4 AND project_id = ?5
                   AND lease_until IS NULL
                   AND status IN ('pending', 'accepted', 'partial')
                   AND EXISTS(
                       SELECT 1 FROM batch_jobs
                       WHERE batch_jobs.request_id = batch_requests.request_id
                         AND batch_jobs.status = 'pending'
                   )",
                params![lease_until, lease_token, now, request_id, project_id],
            )
            .map_err(database_error("claim batch request"))?;
        if changed == 0 {
            transaction
                .commit()
                .map_err(database_error("commit empty batch claim"))?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE batch_jobs SET status = 'dispatching'
                 WHERE request_id = ?1 AND status = 'pending'",
                [request_id],
            )
            .map_err(database_error("claim batch jobs"))?;
        let claimed =
            read_batch(&transaction, project_id, request_id)?.ok_or(AppError::Runtime {
                operation: "read claimed batch request",
            })?;
        transaction
            .commit()
            .map_err(database_error("commit batch claim"))?;
        Ok(Some(claimed))
    }

    pub fn record_job_result(
        &self,
        project_id: &str,
        request_id: &str,
        job_id: &str,
        lease_token: &str,
        result: BatchJobResult,
        now: i64,
    ) -> Result<BatchRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch job result"))?;
        let parent = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND request_id = ?2",
                    BATCH_REQUEST_SELECT
                ),
                params![project_id, request_id],
                batch_request_from_row,
            )
            .optional()
            .map_err(database_error("find batch for job result"))?
            .ok_or(AppError::Validation {
                field: "project_id",
                message: "batch request is not owned by this project",
            })?;
        let current = transaction
            .query_row(
                "SELECT status, pueue_task_id, submission_id, last_error
                 FROM batch_jobs WHERE request_id = ?1 AND job_id = ?2",
                params![request_id, job_id],
                |row| {
                    Ok((
                        row.get::<_, BatchJobStatus>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(database_error("find batch job for result"))?
            .ok_or(AppError::Validation {
                field: "job_id",
                message: "job does not belong to this batch",
            })?;
        let active_lease = matches!(
            parent.status,
            BatchStatus::Dispatching | BatchStatus::Accepted
        ) && parent.lease_until.is_some_and(|lease| lease > now)
            && parent.lease_token.as_deref() == Some(lease_token);

        match result {
            BatchJobResult::Accepted {
                pueue_task_id,
                submission_id,
            } => {
                validate_accepted_result(pueue_task_id, &submission_id)?;
                if current.0 == BatchJobStatus::Accepted {
                    if current.1 == Some(pueue_task_id)
                        && current.2.as_deref() == Some(submission_id.as_str())
                    {
                        let stored = read_batch(&transaction, project_id, request_id)?.ok_or(
                            AppError::Runtime {
                                operation: "read idempotent batch job result",
                            },
                        )?;
                        transaction
                            .commit()
                            .map_err(database_error("commit idempotent batch job result"))?;
                        return Ok(stored);
                    }
                    return Err(AppError::Validation {
                        field: "pueue_task_id",
                        message: "accepted job already has a different external result",
                    });
                }
                if current.0 != BatchJobStatus::Dispatching {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "job is not dispatching",
                    });
                }
                if !active_lease {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'accepted', pueue_task_id = ?1,
                             submission_id = ?2, last_error = NULL
                         WHERE request_id = ?3 AND job_id = ?4
                           AND EXISTS(
                               SELECT 1 FROM batch_requests
                               WHERE batch_requests.request_id = batch_jobs.request_id
                                 AND batch_requests.project_id = ?5
                                 AND batch_requests.lease_token = ?6
                                 AND batch_requests.lease_until > ?7
                                 AND batch_requests.status IN ('dispatching', 'accepted')
                           )",
                        params![
                            pueue_task_id,
                            submission_id,
                            request_id,
                            job_id,
                            project_id,
                            lease_token,
                            now,
                        ],
                    )
                    .map_err(database_error("record accepted batch job"))?;
                if transaction.changes() != 1 {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                let jobs = read_batch(&transaction, project_id, request_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read batch jobs after acceptance",
                    })?
                    .jobs;
                let mut status = derive_request_status(&jobs);
                if status == BatchStatus::Partial
                    && jobs.iter().any(|job| {
                        matches!(
                            job.status,
                            BatchJobStatus::Pending | BatchJobStatus::Dispatching
                        )
                    })
                {
                    status = BatchStatus::Accepted;
                }
                let lease = (matches!(status, BatchStatus::Accepted | BatchStatus::Dispatching))
                    .then_some(parent.lease_until.unwrap());
                let token = lease.is_some().then_some(lease_token);
                transaction
                    .execute(
                        "UPDATE batch_requests SET status = ?1, lease_until = ?2,
                         lease_token = ?3, updated_at = ?4, last_error = NULL
                         WHERE request_id = ?5 AND project_id = ?6
                           AND lease_token = ?7 AND lease_until > ?8",
                        params![
                            status,
                            lease,
                            token,
                            now,
                            request_id,
                            project_id,
                            lease_token,
                            now
                        ],
                    )
                    .map_err(database_error("update batch status after acceptance"))?;
            }
            BatchJobResult::Failed { error } => {
                validate_error(&error)?;
                if current.0 == BatchJobStatus::Accepted {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "accepted job cannot be marked failed",
                    });
                }
                if current.0 == BatchJobStatus::Failed {
                    if current.3.as_deref() == Some(error.as_str()) {
                        let stored = read_batch(&transaction, project_id, request_id)?.ok_or(
                            AppError::Runtime {
                                operation: "read idempotent failed batch job result",
                            },
                        )?;
                        transaction
                            .commit()
                            .map_err(database_error("commit idempotent failed batch result"))?;
                        return Ok(stored);
                    }
                    return Err(AppError::Validation {
                        field: "last_error",
                        message: "failed job already has a different error",
                    });
                }
                if current.0 != BatchJobStatus::Dispatching {
                    return Err(AppError::Validation {
                        field: "job_id",
                        message: "job is not dispatching",
                    });
                }
                if !active_lease {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'failed', last_error = ?1
                         WHERE request_id = ?2 AND job_id = ?3
                           AND EXISTS(
                               SELECT 1 FROM batch_requests
                               WHERE batch_requests.request_id = batch_jobs.request_id
                                 AND batch_requests.project_id = ?4
                                 AND batch_requests.lease_token = ?5
                                 AND batch_requests.lease_until > ?6
                                 AND batch_requests.status IN ('dispatching', 'accepted')
                           )",
                        params![error, request_id, job_id, project_id, lease_token, now],
                    )
                    .map_err(database_error("record failed batch job"))?;
                if transaction.changes() != 1 {
                    return Err(AppError::Validation {
                        field: "lease_token",
                        message: "batch lease is stale or not owned by this worker",
                    });
                }
                transaction
                    .execute(
                        "UPDATE batch_jobs
                         SET status = 'pending', pueue_task_id = NULL,
                             submission_id = NULL, last_error = NULL
                         WHERE request_id = ?1 AND status = 'dispatching'",
                        [request_id],
                    )
                    .map_err(database_error("reset unsubmitted batch jobs"))?;
                let jobs = read_batch(&transaction, project_id, request_id)?
                    .ok_or(AppError::Runtime {
                        operation: "read batch jobs after failure",
                    })?
                    .jobs;
                let status = derive_request_status(&jobs);
                transaction
                    .execute(
                        "UPDATE batch_requests SET status = ?1, lease_until = NULL,
                         lease_token = NULL, updated_at = ?2, last_error = ?3
                         WHERE request_id = ?4 AND project_id = ?5",
                        params![status, now, error, request_id, project_id],
                    )
                    .map_err(database_error("update batch status after failure"))?;
            }
        }

        let stored =
            read_batch(&transaction, project_id, request_id)?.ok_or(AppError::Runtime {
                operation: "read updated batch request",
            })?;
        transaction
            .commit()
            .map_err(database_error("commit batch job result"))?;
        Ok(stored)
    }

    pub fn recover_expired(
        &self,
        project_id: &str,
        now: i64,
    ) -> Result<Vec<BatchRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin batch lease recovery"))?;
        let mut statement = transaction
            .prepare(
                "SELECT request_id FROM batch_requests
                 WHERE project_id = ?1 AND lease_until IS NOT NULL AND lease_until <= ?2
                   AND status IN ('dispatching', 'accepted')
                 ORDER BY request_id",
            )
            .map_err(database_error("prepare expired batch query"))?;
        let request_ids = statement
            .query_map(params![project_id, now], |row| row.get::<_, String>(0))
            .map_err(database_error("query expired batch requests"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read expired batch requests"))?;
        drop(statement);

        let mut recovered = Vec::with_capacity(request_ids.len());
        for request_id in request_ids {
            transaction
                .execute(
                    "UPDATE batch_jobs SET status = 'pending'
                     WHERE request_id = ?1 AND status = 'dispatching'",
                    [&request_id],
                )
                .map_err(database_error("recover unaccepted batch jobs"))?;
            let jobs = read_batch(&transaction, project_id, &request_id)?
                .ok_or(AppError::Runtime {
                    operation: "read recovered batch jobs",
                })?
                .jobs;
            let status = derive_request_status(&jobs);
            transaction
                .execute(
                    "UPDATE batch_requests SET status = ?1, lease_until = NULL,
                     lease_token = NULL, updated_at = ?2
                     WHERE request_id = ?3 AND project_id = ?4",
                    params![status, now, request_id, project_id],
                )
                .map_err(database_error("clear expired batch lease"))?;
            recovered.push(read_batch(&transaction, project_id, &request_id)?.ok_or(
                AppError::Runtime {
                    operation: "read recovered batch request",
                },
            )?);
        }
        transaction
            .commit()
            .map_err(database_error("commit batch lease recovery"))?;
        Ok(recovered)
    }
}

impl<'db> SubmissionRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(&self, submission: &NewSubmission) -> Result<Submission, AppError> {
        let argv_json =
            serde_json::to_string(&submission.argv).map_err(|source| AppError::Serialization {
                operation: "serialize submission arguments",
                source,
            })?;
        if !submission.metadata.is_object() {
            return Err(AppError::Message {
                message: "submission metadata must be a JSON object".to_owned(),
            });
        }
        let metadata_json = serde_json::to_string(&submission.metadata).map_err(|source| {
            AppError::Serialization {
                operation: "serialize submission metadata",
                source,
            }
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent submission insert"))?;
        validate_submission_origin_agent_run(&transaction, submission)?;
        transaction
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
                 ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, ?8)
                 ON CONFLICT(submission_id) DO NOTHING",
                params![
                    submission.submission_id,
                    submission.project_id,
                    argv_json,
                    submission.created_at,
                    submission.status,
                    submission.kind,
                    metadata_json,
                    submission.origin_agent_run_id,
                ],
            )
            .map_err(database_error("insert submission"))?;
        let stored = read_submission(&transaction, &submission.submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit idempotent submission insert"))?;
        Ok(stored)
    }

    pub fn find_by_id(&self, submission_id: &str) -> Result<Option<Submission>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE submission_id = ?1", SUBMISSION_SELECT),
                [submission_id],
                submission_from_row,
            )
            .optional()
            .map_err(database_error("find submission by ID"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY created_at DESC, submission_id DESC
                 LIMIT ?2",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare project submission query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                submission_from_row,
            )
            .map_err(database_error("query project submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project submissions"))
    }

    pub fn list_originless_by_project_page_after(
        &self,
        project_id: &str,
        limit: usize,
        after: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        if let Some(after) = after {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                     AND (created_at < ?2
                          OR (created_at = ?2 AND submission_id < ?3))
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?4",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare paged originless submission query"))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        after.created_at,
                        after.submission_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query paged originless submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read paged originless submissions"))
        } else {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?2",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare originless submission query"))?;
            let rows = statement
                .query_map(
                    params![project_id, bounded_diagnostic_limit(limit)],
                    submission_from_row,
                )
                .map_err(database_error("query originless submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read originless submissions"))
        }
    }

    pub fn list_originless_by_project_page_since(
        &self,
        project_id: &str,
        limit: usize,
        since: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                 AND (created_at > ?2
                      OR (created_at = ?2 AND submission_id > ?3))
                 ORDER BY created_at ASC, submission_id ASC
                 LIMIT ?4",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare new originless submission query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    since.created_at,
                    since.submission_id,
                    bounded_diagnostic_limit(limit),
                ],
                submission_from_row,
            )
            .map_err(database_error("query new originless submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read new originless submissions"))
    }

    pub fn list_originless_by_project_page_at(
        &self,
        project_id: &str,
        at: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id IS NULL
                 AND created_at = ?2 AND submission_id = ?3
                 LIMIT 1",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare boundary originless submission query",
            ))?;
        let rows = statement
            .query_map(
                params![project_id, at.created_at, at.submission_id],
                submission_from_row,
            )
            .map_err(database_error("query boundary originless submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read boundary originless submissions"))
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_signature = ?2
                 ORDER BY created_at DESC, submission_id DESC
                 LIMIT ?3",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare task submission query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                submission_from_row,
            )
            .map_err(database_error("query task submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task submissions"))
    }

    pub fn list_by_origin_agent_run(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
    ) -> Result<Vec<Submission>, AppError> {
        self.list_by_origin_agent_run_page_after(project_id, origin_agent_run_id, limit, None)
    }

    pub fn list_by_origin_agent_run_page_after(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
        after: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        if let Some(after) = after {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                     AND (created_at < ?3
                          OR (created_at = ?3 AND submission_id < ?4))
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?5",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error(
                    "prepare paged origin agent run submission query",
                ))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        origin_agent_run_id,
                        after.created_at,
                        after.submission_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query paged origin agent run submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read paged origin agent run submissions"))
        } else {
            let mut statement = connection
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                     ORDER BY created_at DESC, submission_id DESC
                     LIMIT ?3",
                    SUBMISSION_SELECT
                ))
                .map_err(database_error("prepare origin agent run submission query"))?;
            let rows = statement
                .query_map(
                    params![
                        project_id,
                        origin_agent_run_id,
                        bounded_diagnostic_limit(limit),
                    ],
                    submission_from_row,
                )
                .map_err(database_error("query origin agent run submissions"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read origin agent run submissions"))
        }
    }

    pub fn list_by_origin_agent_run_page_since(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        limit: usize,
        since: Option<&SubmissionPageCursor>,
    ) -> Result<Vec<Submission>, AppError> {
        let Some(since) = since else {
            return Ok(Vec::new());
        };
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                 AND (created_at > ?3
                      OR (created_at = ?3 AND submission_id > ?4))
                 ORDER BY created_at ASC, submission_id ASC
                 LIMIT ?5",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare new origin agent run submission query",
            ))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    origin_agent_run_id,
                    since.created_at,
                    since.submission_id,
                    bounded_diagnostic_limit(limit),
                ],
                submission_from_row,
            )
            .map_err(database_error("query new origin agent run submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read new origin agent run submissions"))
    }

    pub fn list_by_origin_agent_run_page_at(
        &self,
        project_id: &str,
        origin_agent_run_id: i64,
        at: &SubmissionPageCursor,
    ) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND origin_agent_run_id = ?2
                 AND created_at = ?3 AND submission_id = ?4
                 LIMIT 1",
                SUBMISSION_SELECT
            ))
            .map_err(database_error(
                "prepare boundary origin agent run submission query",
            ))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    origin_agent_run_id,
                    at.created_at,
                    at.submission_id,
                ],
                submission_from_row,
            )
            .map_err(database_error(
                "query boundary origin agent run submissions",
            ))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read boundary origin agent run submissions"))
    }

    pub fn mark_accepted(
        &self,
        submission_id: &str,
        pueue_task_id: i64,
        task_signature: &str,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission acceptance"))?;
        transaction
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = ?1, task_signature = ?2, status = ?3
                 WHERE submission_id = ?4",
                params![
                    pueue_task_id,
                    task_signature,
                    SubmissionStatus::Accepted,
                    submission_id,
                ],
            )
            .map_err(database_error("mark submission accepted"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission acceptance"))?;
        Ok(stored)
    }

    pub fn adopt(
        &self,
        submission_id: &str,
        pueue_task_id: i64,
        task_signature: &str,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission adoption"))?;
        transaction
            .execute(
                "UPDATE submissions
                 SET pueue_task_id = ?1, task_signature = ?2, status = ?3
                 WHERE submission_id = ?4",
                params![
                    pueue_task_id,
                    task_signature,
                    SubmissionStatus::Adopted,
                    submission_id,
                ],
            )
            .map_err(database_error("adopt Pueue task for submission"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission adoption"))?;
        Ok(stored)
    }

    pub fn transition_status(
        &self,
        submission_id: &str,
        status: SubmissionStatus,
    ) -> Result<Submission, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin submission status transition"))?;
        transaction
            .execute(
                "UPDATE submissions SET status = ?1 WHERE submission_id = ?2",
                params![status, submission_id],
            )
            .map_err(database_error("transition submission status"))?;
        let stored = read_submission(&transaction, submission_id)?;
        transaction
            .commit()
            .map_err(database_error("commit submission status transition"))?;
        Ok(stored)
    }

    pub fn find_unreconciled(&self, project_id: &str) -> Result<Vec<Submission>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                    AND (
                        status IN (?2, ?3)
                        OR (
                            pueue_task_id IS NULL
                            AND status IN (?4, ?5)
                        )
                    )
                 ORDER BY created_at, submission_id",
                SUBMISSION_SELECT
            ))
            .map_err(database_error("prepare unreconciled submission query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    SubmissionStatus::Pending,
                    SubmissionStatus::Unreconciled,
                    SubmissionStatus::Accepted,
                    SubmissionStatus::Adopted,
                ],
                submission_from_row,
            )
            .map_err(database_error("find unreconciled submissions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read unreconciled submissions"))
    }

    pub fn count_started_or_accepted(&self, project_id: &str) -> Result<u32, AppError> {
        let connection = self.db.connect()?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM submissions
                 WHERE project_id = ?1
                   AND kind = 'experiment'
                   AND (
                       pueue_task_id IS NOT NULL
                       OR status IN ('accepted', 'adopted', 'unreconciled')
                   )",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count accepted submissions"))?;
        u32::try_from(count).map_err(|_| AppError::Runtime {
            operation: "count accepted submissions",
        })
    }
}

fn validate_submission_origin_agent_run(
    transaction: &Transaction<'_>,
    submission: &NewSubmission,
) -> Result<(), AppError> {
    let Some(origin_agent_run_id) = submission.origin_agent_run_id else {
        return Ok(());
    };
    let belongs_to_project: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM agent_runs WHERE run_id = ?1 AND project_id = ?2
             )",
            params![origin_agent_run_id, submission.project_id],
            |row| row.get(0),
        )
        .map_err(database_error("validate submission origin agent run"))?;
    if belongs_to_project {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "origin_agent_run_id",
            message: "must identify an agent run in the submission project",
        })
    }
}

pub struct AgentRunRepository<'db> {
    db: &'db Db,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionLineage {
    pub submission_id: String,
    pub created_at: i64,
    pub kind: SubmissionKind,
    pub status: SubmissionStatus,
    pub pueue_task_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SubmissionPageCursor {
    pub created_at: i64,
    pub submission_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLineage {
    pub event_id: Option<i64>,
    pub event_kind: Option<EventKind>,
    pub event_status: Option<EventStatus>,
    pub run_id: Option<i64>,
    pub mode: Option<String>,
    pub run_status: Option<AgentRunStatus>,
    pub started_at: i64,
    pub submissions: Vec<SubmissionLineage>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunLineageCursor {
    pub started_at: i64,
    pub run_id: i64,
    pub event_id: Option<i64>,
    pub submission_created_at: Option<i64>,
    pub submission_id: Option<String>,
    pub task_id: Option<i64>,
}

impl RunLineageCursor {
    pub fn new(
        started_at: i64,
        run_id: i64,
        submission_id: Option<String>,
        task_id: Option<i64>,
    ) -> Self {
        Self {
            started_at,
            run_id,
            event_id: None,
            submission_id,
            submission_created_at: None,
            task_id,
        }
    }

    pub fn submission(
        started_at: i64,
        run_id: i64,
        submission_id: String,
        submission_created_at: i64,
        task_id: Option<i64>,
    ) -> Self {
        Self {
            started_at,
            run_id,
            event_id: None,
            submission_id: Some(submission_id),
            submission_created_at: Some(submission_created_at),
            task_id,
        }
    }

    pub fn event_only(started_at: i64, event_id: i64) -> Self {
        Self {
            started_at,
            run_id: 0,
            event_id: Some(event_id),
            submission_id: None,
            submission_created_at: None,
            task_id: None,
        }
    }
}

impl RunLineage {
    pub fn root_cursor(&self) -> Option<RunLineageCursor> {
        match (self.run_id, self.event_id) {
            (Some(run_id), _) => Some(RunLineageCursor::new(self.started_at, run_id, None, None)),
            (None, Some(event_id)) => Some(RunLineageCursor::event_only(self.started_at, event_id)),
            (None, None) => None,
        }
    }

    pub fn cursors(&self) -> Vec<RunLineageCursor> {
        let run_id = self.run_id.unwrap_or_default();
        let mut cursors = Vec::new();
        if self.submissions.is_empty() {
            if let Some(cursor) = self.root_cursor() {
                cursors.push(cursor);
            }
        }
        cursors.extend(self.submissions.iter().map(|submission| {
            RunLineageCursor::submission(
                self.started_at,
                run_id,
                submission.submission_id.clone(),
                submission.created_at,
                submission.pueue_task_id,
            )
        }));
        cursors
    }
}

pub struct RunLineageRepository<'db> {
    db: &'db Db,
}

const FOLLOW_SUBMISSION_PAGE_LIMIT: usize = 1;
pub const MAX_FOLLOW_LINEAGE_SUBMISSIONS: usize = FOLLOW_SUBMISSION_PAGE_LIMIT * 4;

impl<'db> RunLineageRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        self.list_by_project_with_submission_page(
            project_id,
            limit,
            limit,
            &BTreeMap::new(),
            &BTreeMap::new(),
            false,
        )
    }

    pub fn list_by_project_follow(
        &self,
        project_id: &str,
        limit: usize,
        after: &BTreeMap<i64, SubmissionPageCursor>,
        head: &BTreeMap<i64, SubmissionPageCursor>,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        self.list_by_project_with_submission_page(
            project_id,
            limit,
            FOLLOW_SUBMISSION_PAGE_LIMIT,
            after,
            head,
            true,
        )
    }

    fn list_by_project_with_submission_page(
        &self,
        project_id: &str,
        limit: usize,
        submission_page_limit: usize,
        after: &BTreeMap<i64, SubmissionPageCursor>,
        head: &BTreeMap<i64, SubmissionPageCursor>,
        follow: bool,
    ) -> Result<Vec<RunLineage>, AppError> {
        let limit = bounded_diagnostic_limit(limit) as usize;
        let runs = AgentRunRepository::new(self.db).list_by_project(project_id, limit)?;
        let event_repository = EventRepository::new(self.db);
        let submission_repository = SubmissionRepository::new(self.db);
        let mut lineages = Vec::new();
        for run in runs {
            let event = event_repository
                .find_by_id(run.primary_event_id)?
                .filter(|event| event.project_id == project_id);
            let mut run_submissions = if let Some(after) = after.get(&run.run_id) {
                let mut page = submission_repository.list_by_origin_agent_run_page_after(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    Some(after),
                )?;
                page.extend(
                    submission_repository
                        .list_by_origin_agent_run_page_at(project_id, run.run_id, after)?,
                );
                page
            } else if head.contains_key(&run.run_id) {
                Vec::new()
            } else {
                submission_repository.list_by_origin_agent_run_page_after(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    None,
                )?
            };
            if let Some(head) = head.get(&run.run_id) {
                run_submissions.extend(submission_repository.list_by_origin_agent_run_page_since(
                    project_id,
                    run.run_id,
                    submission_page_limit,
                    Some(head),
                )?);
                run_submissions.extend(
                    submission_repository
                        .list_by_origin_agent_run_page_at(project_id, run.run_id, head)?,
                );
            }
            run_submissions.sort_by(|left, right| {
                right
                    .created_at
                    .cmp(&left.created_at)
                    .then_with(|| right.submission_id.cmp(&left.submission_id))
            });
            run_submissions.dedup_by(|left, right| left.submission_id == right.submission_id);
            let run_submissions = run_submissions
                .iter()
                .map(SubmissionLineage::from)
                .collect();
            lineages.push(RunLineage {
                event_id: event
                    .as_ref()
                    .map(|event| event.event_id)
                    .or(Some(run.primary_event_id)),
                event_kind: event.as_ref().map(|event| event.kind),
                event_status: event.as_ref().map(|event| event.status),
                run_id: Some(run.run_id),
                mode: Some(run.context_mode.as_str().to_owned()),
                run_status: Some(run.status),
                started_at: run.started_at,
                submissions: run_submissions,
            });
        }
        let selected_primary_event_ids = lineages
            .iter()
            .filter_map(|lineage| lineage.event_id)
            .collect::<std::collections::BTreeSet<_>>();
        let remaining = limit.saturating_sub(lineages.len());
        let events = event_repository
            .recent_events(project_id, limit)?
            .into_iter()
            .filter(|event| !selected_primary_event_ids.contains(&event.event_id));
        let mut submissions = if follow {
            let mut page = if let Some(after) = after.get(&0) {
                let mut page = submission_repository.list_originless_by_project_page_after(
                    project_id,
                    submission_page_limit,
                    Some(after),
                )?;
                page.extend(
                    submission_repository.list_originless_by_project_page_at(project_id, after)?,
                );
                page
            } else if head.contains_key(&0) {
                Vec::new()
            } else {
                submission_repository.list_originless_by_project_page_after(
                    project_id,
                    submission_page_limit,
                    None,
                )?
            };
            if let Some(head) = head.get(&0) {
                page.extend(submission_repository.list_originless_by_project_page_since(
                    project_id,
                    submission_page_limit,
                    head,
                )?);
                page.extend(
                    submission_repository.list_originless_by_project_page_at(project_id, head)?,
                );
            }
            page.sort_by(|left, right| {
                right
                    .created_at
                    .cmp(&left.created_at)
                    .then_with(|| right.submission_id.cmp(&left.submission_id))
            });
            page.dedup_by(|left, right| left.submission_id == right.submission_id);
            page
        } else {
            submission_repository
                .list_by_project(project_id, limit)?
                .into_iter()
                .filter(|submission| submission.origin_agent_run_id.is_none())
                .collect()
        };
        let mut incomplete = Vec::new();
        for event in events {
            incomplete.push(RunLineage {
                event_id: Some(event.event_id),
                event_kind: Some(event.kind),
                event_status: Some(event.status),
                run_id: None,
                mode: None,
                run_status: None,
                started_at: event.created_at,
                submissions: Vec::new(),
            });
        }
        if follow {
            if !submissions.is_empty() {
                let started_at = submissions[0].created_at;
                incomplete.push(RunLineage {
                    event_id: None,
                    event_kind: None,
                    event_status: None,
                    run_id: None,
                    mode: None,
                    run_status: None,
                    started_at,
                    submissions: submissions.iter().map(SubmissionLineage::from).collect(),
                });
            }
        } else {
            for submission in submissions.drain(..) {
                incomplete.push(RunLineage {
                    event_id: None,
                    event_kind: None,
                    event_status: None,
                    run_id: submission.origin_agent_run_id,
                    mode: None,
                    run_status: None,
                    started_at: submission.created_at,
                    submissions: vec![SubmissionLineage::from(&submission)],
                });
            }
        }
        incomplete.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.run_id.cmp(&left.run_id))
        });
        if !follow {
            incomplete.truncate(remaining);
        }
        lineages.extend(incomplete);
        Ok(lineages)
    }
}

impl From<&Submission> for SubmissionLineage {
    fn from(submission: &Submission) -> Self {
        Self {
            submission_id: submission.submission_id.clone(),
            created_at: submission.created_at,
            kind: submission.kind,
            status: submission.status,
            pueue_task_id: submission.pueue_task_id,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgentRunRecovery {
    pub failed_runs: usize,
    pub requeued_events: usize,
}

impl<'db> AgentRunRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert(&self, run: &NewAgentRun) -> Result<AgentRun, AppError> {
        let log_path = path_text(&run.log_path, "log_path")?;
        let context_lineage_json =
            serde_json::to_string(&run.context_lineage).map_err(|source| {
                AppError::Serialization {
                    operation: "serialize agent context lineage",
                    source,
                }
            })?;
        let launch_gate_state = if run.status == AgentRunStatus::Starting {
            "pending"
        } else {
            "released"
        };
        let connection = self.db.connect()?;
        connection
            .execute(
                "INSERT INTO agent_runs (
                    project_id, primary_event_id, pid, status, started_at,
                    finished_at, exit_code, log_path, last_error, launch_gate_state,
                    context_mode,
                    context_session_id, context_lineage_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, NULL, ?7, ?8, ?9, ?10)",
                params![
                    run.project_id,
                    run.primary_event_id,
                    run.pid,
                    run.status,
                    run.started_at,
                    log_path,
                    launch_gate_state,
                    run.context_mode.as_str(),
                    run.context_session_id.as_deref(),
                    context_lineage_json,
                ],
            )
            .map_err(database_error("insert agent run"))?;
        read_agent_run(&connection, connection.last_insert_rowid())
    }

    pub fn insert_with_events(
        &self,
        run: &NewAgentRun,
        event_ids: &[i64],
    ) -> Result<AgentRun, AppError> {
        self.insert_with_events_and_reservation(run, event_ids, None)
    }

    pub fn insert_with_events_and_reservation(
        &self,
        run: &NewAgentRun,
        event_ids: &[i64],
        reservation_token: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run, event, and intervention insertion",
            ))?;
        let run_id = insert_agent_run(&transaction, run)?;
        for event_id in event_ids {
            attach_event_to_agent_run(&transaction, &run.project_id, run_id, *event_id)?;
        }
        if let Some(reservation_token) = reservation_token {
            let changed = transaction
                .execute(
                    "UPDATE interventions
                     SET agent_run_id = ?1
                     WHERE project_id = ?2 AND reservation_token = ?3 AND status = ?4",
                    params![
                        run_id,
                        run.project_id,
                        reservation_token,
                        InterventionStatus::Reserved,
                    ],
                )
                .map_err(database_error(
                    "attach intervention reservation to agent run",
                ))?;
            if changed == 0 {
                return Err(AppError::Runtime {
                    operation: "attach intervention reservation to agent run",
                });
            }
        }
        transaction.commit().map_err(database_error(
            "commit agent run, event, and intervention insertion",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn find_active_by_project(&self, project_id: &str) -> Result<Option<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND status IN ('starting', 'running')
                     ORDER BY started_at DESC, run_id DESC LIMIT 1",
                    AGENT_RUN_SELECT
                ),
                [project_id],
                agent_run_from_row,
            )
            .optional()
            .map_err(database_error("find active agent run"))
    }

    pub fn recover_interrupted(
        &self,
        finished_at: i64,
        reason: &str,
    ) -> Result<AgentRunRecovery, AppError> {
        let marker_confirmed_run_ids = {
            let connection = self.db.connect()?;
            let mut statement = connection
                .prepare(
                    "SELECT run_id, log_path
                     FROM agent_runs
                     WHERE launch_gate_state = 'release_requested'",
                )
                .map_err(database_error("prepare launch gate recovery marker query"))?;
            let run_logs = statement
                .query_map([], |row| {
                    let run_id = row.get::<_, i64>(0)?;
                    let log_path = PathBuf::from(row.get::<_, String>(1)?);
                    Ok((run_id, log_path))
                })
                .map_err(database_error("inspect launch gate recovery markers"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read launch gate recovery markers"))?;
            let mut confirmed = Vec::new();
            for (run_id, log_path) in run_logs {
                match fs::metadata(launch_gate_marker_path(&log_path)) {
                    Ok(metadata) if metadata.is_file() => confirmed.push(run_id),
                    Ok(_) => {}
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(AppError::Io {
                            operation: "inspect agent launch gate marker",
                            source,
                        });
                    }
                }
            }
            confirmed
        };
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin interrupted agent run recovery"))?;
        for run_id in marker_confirmed_run_ids {
            transaction
                .execute(
                    "UPDATE agent_runs
                     SET launch_gate_state = 'released'
                     WHERE run_id = ?1 AND launch_gate_state = 'release_requested'",
                    [run_id],
                )
                .map_err(database_error("promote marker-confirmed agent launch gate"))?;
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, applied_at = ?2, lease_expires_at = NULL,
                     reservation_token = NULL
                 WHERE status = ?3 AND agent_run_id IS NOT NULL
                   AND EXISTS (
                       SELECT 1 FROM agent_runs
                       WHERE agent_runs.project_id = interventions.project_id
                         AND agent_runs.run_id = interventions.agent_run_id
                         AND agent_runs.launch_gate_state = 'released'
                         AND (agent_runs.pid IS NOT NULL OR agent_runs.status = 'running')
                   )",
                params![
                    InterventionStatus::Applied,
                    finished_at,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error(
                "apply delivered interventions during agent run recovery",
            ))?;
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL,
                     agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                 WHERE status IN (?2, ?3) AND agent_run_id IS NOT NULL
                   AND EXISTS (
                       SELECT 1 FROM agent_runs
                       WHERE agent_runs.project_id = interventions.project_id
                         AND agent_runs.run_id = interventions.agent_run_id
                         AND (
                             agent_runs.launch_gate_state IN ('pending', 'release_requested', 'failed')
                             OR (agent_runs.pid IS NULL AND agent_runs.status <> 'running')
                         )
                   )",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    InterventionStatus::Applied,
                ],
            )
            .map_err(database_error(
                "release undelivered interventions during agent run recovery",
            ))?;
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL,
                     agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                 WHERE status = ?2 AND agent_run_id IS NULL AND lease_expires_at <= ?3",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    finished_at,
                ],
            )
            .map_err(database_error(
                "recover expired interventions during agent run recovery",
            ))?;
        let requeued_events = transaction
            .execute(
                "UPDATE events
                 SET status = 'pending', lease_until = NULL
                 WHERE status = 'claimed'
                   AND event_id IN (
                       SELECT agent_run_events.event_id
                       FROM agent_run_events
                       JOIN agent_runs
                         ON agent_runs.run_id = agent_run_events.run_id
                        AND agent_runs.project_id = agent_run_events.project_id
                       WHERE agent_runs.status IN ('starting', 'running')
                   )",
                [],
            )
            .map_err(database_error(
                "requeue events attached to interrupted agent runs",
            ))?;
        let failed_runs = transaction
            .execute(
                "UPDATE agent_runs
                 SET status = 'failed', finished_at = ?1, last_error = ?2,
                     launch_gate_state = CASE
                         WHEN launch_gate_state IN ('pending', 'release_requested') THEN 'failed'
                         ELSE launch_gate_state
                     END
                 WHERE status IN ('starting', 'running')",
                params![finished_at, reason],
            )
            .map_err(database_error("fail interrupted agent runs"))?;
        transaction
            .commit()
            .map_err(database_error("commit interrupted agent run recovery"))?;
        Ok(AgentRunRecovery {
            failed_runs,
            requeued_events,
        })
    }

    pub fn attach_event(&self, run_id: i64, event_id: i64) -> Result<AgentRunEvent, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin agent run event attachment"))?;
        let project_id = transaction
            .query_row(
                "SELECT project_id FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(database_error("read agent run project"))?;
        attach_event_to_agent_run(&transaction, &project_id, run_id, event_id)?;
        transaction
            .commit()
            .map_err(database_error("commit agent run event attachment"))?;
        Ok(AgentRunEvent {
            project_id,
            run_id,
            event_id,
        })
    }

    pub fn mark_running(&self, run_id: i64, pid: i64) -> Result<AgentRun, AppError> {
        let connection = self.db.connect()?;
        connection
            .execute(
                "UPDATE agent_runs
                 SET pid = ?1, status = 'running', launch_gate_state = 'released'
                 WHERE run_id = ?2",
                params![pid, run_id],
            )
            .map_err(database_error("mark agent run running"))?;
        read_agent_run(&connection, run_id)
    }

    pub fn mark_running_and_apply_interventions(
        &self,
        project_id: &str,
        run_id: i64,
        pid: i64,
        applied_at: i64,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run start and intervention application",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE agent_runs SET pid = ?1, status = 'running', launch_gate_state = 'pending'
                 WHERE project_id = ?2 AND run_id = ?3",
                params![pid, project_id, run_id],
            )
            .map_err(database_error("mark agent run running"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "mark project agent run running",
            });
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, applied_at = ?2, lease_expires_at = NULL,
                     reservation_token = NULL
                 WHERE project_id = ?3 AND agent_run_id = ?4 AND status = ?5",
                params![
                    InterventionStatus::Applied,
                    applied_at,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error(
                "mark interventions applied for running agent",
            ))?;
        transaction.commit().map_err(database_error(
            "commit agent run start and intervention application",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn mark_gate_release_requested(
        &self,
        project_id: &str,
        run_id: i64,
    ) -> Result<(), AppError> {
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE agent_runs
                 SET launch_gate_state = 'release_requested'
                 WHERE project_id = ?1 AND run_id = ?2 AND launch_gate_state = 'pending'",
                params![project_id, run_id],
            )
            .map_err(database_error("record agent launch gate release request"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "record agent launch gate release request",
            });
        }
        Ok(())
    }

    pub fn mark_gate_released(&self, project_id: &str, run_id: i64) -> Result<(), AppError> {
        let connection = self.db.connect()?;
        let changed = connection
            .execute(
                "UPDATE agent_runs
                 SET launch_gate_state = 'released'
                 WHERE project_id = ?1 AND run_id = ?2
                   AND launch_gate_state = 'release_requested'",
                params![project_id, run_id],
            )
            .map_err(database_error("record agent launch gate release"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "record agent launch gate release",
            });
        }
        Ok(())
    }

    pub fn fail_before_gate_release(
        &self,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
        reason: &str,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin pre-release agent run failure"))?;
        let changed = transaction
            .execute(
                "UPDATE agent_runs
                 SET status = 'failed', finished_at = ?1, last_error = ?2,
                     launch_gate_state = 'failed'
                 WHERE project_id = ?3 AND run_id = ?4
                   AND launch_gate_state IN ('pending', 'release_requested')",
                params![finished_at, reason, project_id, run_id],
            )
            .map_err(database_error("fail agent run before launch gate release"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "fail agent run before launch gate release",
            });
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = 'pending', reserved_at = NULL, applied_at = NULL,
                     agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?1 AND agent_run_id = ?2
                   AND status IN ('reserved', 'applied')",
                params![project_id, run_id],
            )
            .map_err(database_error(
                "requeue interventions after launch gate failure",
            ))?;
        transaction
            .commit()
            .map_err(database_error("commit pre-release agent run failure"))?;
        read_agent_run(&connection, run_id)
    }

    pub fn finish(
        &self,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let connection = self.db.connect()?;
        connection
            .execute(
                "UPDATE agent_runs
                 SET status = ?1, finished_at = ?2, exit_code = ?3, last_error = ?4
                 WHERE run_id = ?5",
                params![status, finished_at, exit_code, last_error, run_id],
            )
            .map_err(database_error("finish agent run"))?;
        read_agent_run(&connection, run_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn finish_and_release_interventions(
        &self,
        project_id: &str,
        run_id: i64,
        status: AgentRunStatus,
        finished_at: i64,
        exit_code: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<AgentRun, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin agent run finish and intervention release",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE agent_runs
                 SET status = ?1, finished_at = ?2, exit_code = ?3, last_error = ?4,
                     launch_gate_state = CASE
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 = 'failed' THEN 'failed'
                         WHEN launch_gate_state IN ('pending', 'release_requested')
                              AND ?1 IN ('completed', 'timed_out', 'cancelled') THEN 'released'
                         ELSE launch_gate_state
                     END
                 WHERE project_id = ?5 AND run_id = ?6",
                params![
                    status,
                    finished_at,
                    exit_code,
                    last_error,
                    project_id,
                    run_id,
                ],
            )
            .map_err(database_error("finish project agent run"))?;
        if changed != 1 {
            return Err(AppError::Runtime {
                operation: "finish project agent run",
            });
        }
        transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL,
                     agent_run_id = NULL, lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND agent_run_id = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error(
                "release interventions for finished agent run",
            ))?;
        transaction.commit().map_err(database_error(
            "commit agent run finish and intervention release",
        ))?;
        read_agent_run(&connection, run_id)
    }

    pub fn count_by_project(&self, project_id: &str) -> Result<u32, AppError> {
        let connection = self.db.connect()?;
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("count agent runs"))?;
        u32::try_from(count).map_err(|_| AppError::Runtime {
            operation: "count agent runs",
        })
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY started_at DESC, run_id DESC
                 LIMIT ?2",
                AGENT_RUN_SELECT
            ))
            .map_err(database_error("prepare project agent run query"))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                agent_run_from_row,
            )
            .map_err(database_error("query project agent runs"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project agent runs"))
    }

    pub fn find_by_event(
        &self,
        project_id: &str,
        event_id: i64,
        limit: usize,
    ) -> Result<Vec<AgentRun>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT agent_runs.run_id, agent_runs.project_id, agent_runs.primary_event_id,
                        agent_runs.pid, agent_runs.status, agent_runs.started_at,
                        agent_runs.finished_at, agent_runs.exit_code, agent_runs.log_path,
                        agent_runs.last_error, agent_runs.launch_gate_state,
                        agent_runs.context_mode, agent_runs.context_session_id,
                        agent_runs.context_lineage_json
                 FROM agent_runs
                 JOIN agent_run_events
                   ON agent_run_events.project_id = agent_runs.project_id
                  AND agent_run_events.run_id = agent_runs.run_id
                 WHERE agent_runs.project_id = ?1 AND agent_run_events.event_id = ?2
                 ORDER BY agent_runs.started_at DESC, agent_runs.run_id DESC
                 LIMIT ?3",
            )
            .map_err(database_error("prepare event agent run query"))?;
        let rows = statement
            .query_map(
                params![project_id, event_id, bounded_diagnostic_limit(limit)],
                agent_run_from_row,
            )
            .map_err(database_error("query event agent runs"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read event agent runs"))
    }
}

pub struct TerminationRequestRepository<'db> {
    db: &'db Db,
}

impl<'db> TerminationRequestRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_idempotent(
        &self,
        request: &NewTerminationRequest,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin idempotent termination request insert",
            ))?;
        transaction
            .execute(
                "INSERT INTO termination_requests (
                    incident_id, project_id, task_signature, reason, status,
                    requested_at, grace_until, confirmed_at, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL)
                 ON CONFLICT(project_id, incident_id, task_signature) DO NOTHING",
                params![
                    request.incident_id,
                    request.project_id,
                    request.task_signature,
                    request.reason,
                    request.status,
                    request.requested_at,
                    request.grace_until,
                ],
            )
            .map_err(database_error("insert termination request"))?;
        let stored = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND incident_id = ?2 AND task_signature = ?3",
                    TERMINATION_REQUEST_SELECT
                ),
                params![
                    request.project_id,
                    request.incident_id,
                    request.task_signature,
                ],
                termination_request_from_row,
            )
            .map_err(database_error("read idempotent termination request"))?;
        transaction.commit().map_err(database_error(
            "commit idempotent termination request insert",
        ))?;
        Ok(stored)
    }

    pub fn find_by_id(&self, request_id: i64) -> Result<Option<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!("{} WHERE request_id = ?1", TERMINATION_REQUEST_SELECT),
                [request_id],
                termination_request_from_row,
            )
            .optional()
            .map_err(database_error("find termination request by ID"))
    }

    pub fn transition_status(
        &self,
        request_id: i64,
        status: TerminationRequestStatus,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin termination request status transition",
            ))?;
        transaction
            .execute(
                "UPDATE termination_requests SET status = ?1 WHERE request_id = ?2",
                params![status, request_id],
            )
            .map_err(database_error("transition termination request status"))?;
        let stored = read_termination_request(&transaction, request_id)?;
        transaction.commit().map_err(database_error(
            "commit termination request status transition",
        ))?;
        Ok(stored)
    }

    pub fn transition_status_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        next: TerminationRequestStatus,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request status transition",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL
                 WHERE request_id = ?2 AND status = ?3",
                params![next, request_id, current],
            )
            .map_err(database_error(
                "guarded transition termination request status",
            ))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request status transition",
        ))?;
        Ok(stored)
    }

    pub fn claim_for_dispatch(
        &self,
        request_id: i64,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination request dispatch claim"))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = ?2
                 WHERE request_id = ?3
                   AND (
                       status = ?4
                       OR (
                           status = ?5
                           AND (
                               dispatch_lease_until IS NULL
                               OR dispatch_lease_until <= ?6
                           )
                       )
                   )",
                params![
                    TerminationRequestStatus::Dispatching,
                    lease_until,
                    request_id,
                    TerminationRequestStatus::Requested,
                    TerminationRequestStatus::Dispatching,
                    now,
                ],
            )
            .map_err(database_error("claim termination request dispatch"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction
            .commit()
            .map_err(database_error("commit termination request dispatch claim"))?;
        Ok(stored)
    }

    pub fn update_result(
        &self,
        request_id: i64,
        status: TerminationRequestStatus,
        confirmed_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<TerminationRequest, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination request result update"))?;
        transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = ?2, last_error = ?3
                 WHERE request_id = ?4",
                params![status, confirmed_at, last_error, request_id],
            )
            .map_err(database_error("update termination request result"))?;
        let stored = read_termination_request(&transaction, request_id)?;
        transaction
            .commit()
            .map_err(database_error("commit termination request result update"))?;
        Ok(stored)
    }

    pub fn update_result_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        next: TerminationRequestStatus,
        confirmed_at: Option<i64>,
        last_error: Option<&str>,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request result update",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = ?2, last_error = ?3
                 WHERE request_id = ?4 AND status = ?5",
                params![next, confirmed_at, last_error, request_id, current],
            )
            .map_err(database_error("guarded update termination request result"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request result update",
        ))?;
        Ok(stored)
    }

    pub fn set_grace_until_if_current(
        &self,
        request_id: i64,
        current: TerminationRequestStatus,
        grace_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin guarded termination request grace update",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET grace_until = ?1
                 WHERE request_id = ?2 AND status = ?3 AND grace_until IS NULL",
                params![grace_until, request_id, current],
            )
            .map_err(database_error("guarded update termination request grace"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit guarded termination request grace update",
        ))?;
        Ok(stored)
    }

    pub fn mark_dispatched_if_current(
        &self,
        request_id: i64,
        dispatch_lease_until: i64,
        grace_until: i64,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin termination request dispatch confirmation",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL, grace_until = ?2
                 WHERE request_id = ?3 AND status = ?4 AND dispatch_lease_until = ?5",
                params![
                    TerminationRequestStatus::Sent,
                    grace_until,
                    request_id,
                    TerminationRequestStatus::Dispatching,
                    dispatch_lease_until,
                ],
            )
            .map_err(database_error("mark termination request as dispatched"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction.commit().map_err(database_error(
            "commit termination request dispatch confirmation",
        ))?;
        Ok(stored)
    }

    pub fn finish_dispatch_if_current(
        &self,
        request_id: i64,
        dispatch_lease_until: i64,
        status: TerminationRequestStatus,
        last_error: Option<&str>,
    ) -> Result<Option<TerminationRequest>, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin termination dispatch result update"))?;
        let changed = transaction
            .execute(
                "UPDATE termination_requests
                 SET status = ?1, dispatch_lease_until = NULL,
                     confirmed_at = NULL, last_error = ?2
                 WHERE request_id = ?3 AND status = ?4 AND dispatch_lease_until = ?5",
                params![
                    status,
                    last_error,
                    request_id,
                    TerminationRequestStatus::Dispatching,
                    dispatch_lease_until,
                ],
            )
            .map_err(database_error("update termination dispatch result"))?;
        let stored = if changed == 0 {
            None
        } else {
            Some(read_termination_request(&transaction, request_id)?)
        };
        transaction
            .commit()
            .map_err(database_error("commit termination dispatch result update"))?;
        Ok(stored)
    }

    pub fn find_pending(&self, project_id: &str) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND status IN (?2, ?3, ?4)
                 ORDER BY requested_at, request_id",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare pending termination request query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    TerminationRequestStatus::Requested,
                    TerminationRequestStatus::Dispatching,
                    TerminationRequestStatus::Sent,
                ],
                termination_request_from_row,
            )
            .map_err(database_error("find pending termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read pending termination requests"))
    }

    pub fn find_by_project(&self, project_id: &str) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 ORDER BY requested_at, request_id",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare project termination request query"))?;
        let rows = statement
            .query_map([project_id], termination_request_from_row)
            .map_err(database_error("find project termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read project termination requests"))
    }

    pub fn list_by_project(
        &self,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1
                 ORDER BY requested_at DESC, request_id DESC
                 LIMIT ?2",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error(
                "prepare bounded project termination request query",
            ))?;
        let rows = statement
            .query_map(
                params![project_id, bounded_diagnostic_limit(limit)],
                termination_request_from_row,
            )
            .map_err(database_error("query bounded project termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read bounded project termination requests"))
    }

    pub fn find_by_task_signature(
        &self,
        project_id: &str,
        task_signature: &str,
        limit: usize,
    ) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND task_signature = ?2
                 ORDER BY requested_at DESC, request_id DESC
                 LIMIT ?3",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare task termination request query"))?;
        let rows = statement
            .query_map(
                params![project_id, task_signature, bounded_diagnostic_limit(limit)],
                termination_request_from_row,
            )
            .map_err(database_error("query task termination requests"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read task termination requests"))
    }
}

pub struct InterventionRepository<'db> {
    db: &'db Db,
}

impl<'db> InterventionRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert_pending(
        &self,
        project_id: &str,
        message: &str,
        created_at: i64,
    ) -> Result<Intervention, AppError> {
        validate_message(message)?;
        let intervention_id = Uuid::new_v4().to_string();
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention insert"))?;
        let insertion_sequence: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(insertion_sequence), 0) + 1
                 FROM interventions WHERE project_id = ?1",
                [project_id],
                |row| row.get(0),
            )
            .map_err(database_error("allocate intervention insertion sequence"))?;
        transaction
            .execute(
                "INSERT INTO interventions (
                    intervention_id, project_id, insertion_sequence, message, status, created_at, reserved_at,
                    applied_at, agent_run_id, attempts, lease_expires_at, reservation_token
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, 0, NULL, NULL)",
                params![
                    intervention_id,
                    project_id,
                    insertion_sequence,
                    message,
                    InterventionStatus::Pending,
                    created_at,
                ],
            )
            .map_err(database_error("insert intervention"))?;
        let intervention = transaction
            .query_row(
                &format!(
                    "{} WHERE intervention_id = ?1 AND project_id = ?2",
                    INTERVENTION_SELECT
                ),
                params![intervention_id, project_id],
                intervention_from_row,
            )
            .map_err(database_error("read inserted intervention"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention insert"))?;
        Ok(intervention)
    }

    pub fn list(
        &self,
        project_id: &str,
        status: InterventionStatus,
        limit: usize,
    ) -> Result<Vec<Intervention>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND status = ?2
                 ORDER BY insertion_sequence ASC, created_at ASC, intervention_id ASC
                 LIMIT ?3",
                INTERVENTION_SELECT
            ))
            .map_err(database_error("prepare intervention list"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    status,
                    limit.min(MAX_INTERVENTIONS_PER_RUN) as i64,
                ],
                intervention_from_row,
            )
            .map_err(database_error("query interventions"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read interventions"))
    }

    pub fn count_by_project(&self, project_id: &str) -> Result<InterventionCounts, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status = 'reserved' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN status = 'applied' THEN 1 ELSE 0 END), 0)
                 FROM interventions
                 WHERE project_id = ?1",
                [project_id],
                |row| {
                    Ok(InterventionCounts {
                        pending: row.get(0)?,
                        reserved: row.get(1)?,
                        applied: row.get(2)?,
                    })
                },
            )
            .map_err(database_error("count project interventions"))
    }

    pub fn reserve_pending(
        &self,
        project_id: &str,
        token: &str,
        now: i64,
        lease_until: i64,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<InterventionReservation, AppError> {
        if lease_until <= now {
            return Err(AppError::Configuration {
                field: "intervention_lease",
            });
        }
        let max_count = max_count.min(MAX_INTERVENTIONS_PER_RUN);
        let max_bytes = max_bytes.min(MAX_INTERVENTION_BYTES_PER_RUN);
        if max_count == 0 || max_bytes == 0 {
            return Ok(InterventionReservation {
                token: token.to_owned(),
                items: Vec::new(),
            });
        }

        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin immediate intervention reservation"))?;
        let pending = {
            let mut statement = transaction
                .prepare(&format!(
                    "{} WHERE project_id = ?1 AND status = ?2
                     ORDER BY insertion_sequence ASC, created_at ASC, intervention_id ASC
                     LIMIT ?3",
                    INTERVENTION_SELECT
                ))
                .map_err(database_error("prepare pending intervention reservation"))?;
            let pending = statement
                .query_map(
                    params![project_id, InterventionStatus::Pending, max_count as i64],
                    intervention_from_row,
                )
                .map_err(database_error("query pending interventions"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(database_error("read pending interventions"))?;
            pending
        };

        let mut total_bytes = 0;
        let mut items = Vec::with_capacity(pending.len());
        for intervention in pending {
            let next_total = total_bytes + intervention.message.len();
            if next_total > max_bytes {
                break;
            }
            let changed = transaction
                .execute(
                    "UPDATE interventions
                     SET status = ?1, reserved_at = ?2, attempts = attempts + 1,
                         lease_expires_at = ?3, reservation_token = ?4
                     WHERE intervention_id = ?5 AND project_id = ?6 AND status = ?7",
                    params![
                        InterventionStatus::Reserved,
                        now,
                        lease_until,
                        token,
                        intervention.intervention_id,
                        project_id,
                        InterventionStatus::Pending,
                    ],
                )
                .map_err(database_error("reserve intervention"))?;
            if changed == 1 {
                total_bytes = next_total;
                items.push(
                    transaction
                        .query_row(
                            &format!(
                                "{} WHERE intervention_id = ?1 AND project_id = ?2",
                                INTERVENTION_SELECT
                            ),
                            params![intervention.intervention_id, project_id],
                            intervention_from_row,
                        )
                        .map_err(database_error("read reserved intervention"))?,
                );
            }
        }
        transaction
            .commit()
            .map_err(database_error("commit intervention reservation"))?;
        Ok(InterventionReservation {
            token: token.to_owned(),
            items,
        })
    }

    pub fn mark_applied_for_run(
        &self,
        project_id: &str,
        run_id: i64,
        applied_at: i64,
    ) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention application"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, applied_at = ?2, lease_expires_at = NULL,
                     reservation_token = NULL
                 WHERE project_id = ?3 AND agent_run_id = ?4 AND status = ?5",
                params![
                    InterventionStatus::Applied,
                    applied_at,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("mark interventions applied for run"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention application"))?;
        Ok(changed)
    }

    pub fn release_for_run(&self, project_id: &str, run_id: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention release"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND agent_run_id = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    run_id,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("release interventions for run"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention release"))?;
        Ok(changed)
    }

    pub fn release_reservation(
        &self,
        project_id: &str,
        reservation_token: &str,
    ) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin intervention reservation release"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE project_id = ?2 AND reservation_token = ?3 AND status = ?4",
                params![
                    InterventionStatus::Pending,
                    project_id,
                    reservation_token,
                    InterventionStatus::Reserved,
                ],
            )
            .map_err(database_error("release intervention reservation"))?;
        transaction
            .commit()
            .map_err(database_error("commit intervention reservation release"))?;
        Ok(changed)
    }

    pub fn recover_expired(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin expired intervention recovery"))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE status = ?2 AND lease_expires_at <= ?3",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    now,
                ],
            )
            .map_err(database_error("recover expired interventions"))?;
        transaction
            .commit()
            .map_err(database_error("commit expired intervention recovery"))?;
        Ok(changed)
    }

    pub fn recover_expired_unattached(&self, now: i64) -> Result<usize, AppError> {
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error(
                "begin expired unattached intervention recovery",
            ))?;
        let changed = transaction
            .execute(
                "UPDATE interventions
                 SET status = ?1, reserved_at = NULL, applied_at = NULL, agent_run_id = NULL,
                     lease_expires_at = NULL, reservation_token = NULL
                 WHERE status = ?2 AND agent_run_id IS NULL AND lease_expires_at <= ?3",
                params![
                    InterventionStatus::Pending,
                    InterventionStatus::Reserved,
                    now,
                ],
            )
            .map_err(database_error("recover expired unattached interventions"))?;
        transaction.commit().map_err(database_error(
            "commit expired unattached intervention recovery",
        ))?;
        Ok(changed)
    }
}

pub struct TaskObservationRepository<'db> {
    db: &'db Db,
}

impl<'db> TaskObservationRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn upsert(&self, observation: &NewTaskObservation) -> Result<TaskObservation, AppError> {
        let command_json = serde_json::to_string(&observation.command).map_err(|source| {
            AppError::Serialization {
                operation: "serialize task observation command",
                source,
            }
        })?;
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin task observation upsert"))?;
        transaction
            .execute(
                "INSERT INTO task_observations (
                    project_id, task_signature, pueue_task_id, pueue_group, command_json,
                    state, enqueued_at, started_at, ended_at, result, observed_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(project_id, task_signature) DO UPDATE SET
                    pueue_task_id = excluded.pueue_task_id,
                    pueue_group = excluded.pueue_group,
                    command_json = excluded.command_json,
                    state = excluded.state,
                    enqueued_at = excluded.enqueued_at,
                    started_at = excluded.started_at,
                    ended_at = excluded.ended_at,
                    result = excluded.result,
                    observed_at = excluded.observed_at",
                params![
                    observation.project_id,
                    observation.task_signature,
                    observation.pueue_task_id,
                    observation.pueue_group,
                    command_json,
                    observation.state,
                    observation.enqueued_at,
                    observation.started_at,
                    observation.ended_at,
                    observation.result,
                    observation.observed_at,
                ],
            )
            .map_err(database_error("upsert task observation"))?;
        let stored = transaction
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND task_signature = ?2",
                    TASK_OBSERVATION_SELECT
                ),
                params![observation.project_id, observation.task_signature],
                task_observation_from_row,
            )
            .map_err(database_error("read upserted task observation"))?;
        transaction
            .commit()
            .map_err(database_error("commit task observation upsert"))?;
        Ok(stored)
    }

    pub fn find(
        &self,
        project_id: &str,
        task_signature: &str,
    ) -> Result<Option<TaskObservation>, AppError> {
        let connection = self.db.connect()?;
        connection
            .query_row(
                &format!(
                    "{} WHERE project_id = ?1 AND task_signature = ?2",
                    TASK_OBSERVATION_SELECT
                ),
                params![project_id, task_signature],
                task_observation_from_row,
            )
            .optional()
            .map_err(database_error("find task observation"))
    }

    pub fn find_by_pueue_task(
        &self,
        project_id: &str,
        pueue_task_id: i64,
        limit: usize,
    ) -> Result<Vec<TaskObservation>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND pueue_task_id = ?2
                 ORDER BY observed_at DESC, task_signature DESC
                 LIMIT ?3",
                TASK_OBSERVATION_SELECT
            ))
            .map_err(database_error("prepare Pueue task observation query"))?;
        let rows = statement
            .query_map(
                params![project_id, pueue_task_id, bounded_diagnostic_limit(limit)],
                task_observation_from_row,
            )
            .map_err(database_error("query Pueue task observations"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read Pueue task observations"))
    }
}

const EVENT_SELECT: &str =
    "SELECT event_id, project_id, kind, dedup_key, payload_json, status, attempts,
            not_before, lease_until, created_at, completed_at, last_error
     FROM events";

const INTEGRATION_EVENT_SELECT: &str =
    "SELECT integration_event_id, kind, dedup_key, payload_json, created_at
     FROM integration_events";

const INCIDENT_SELECT: &str = "SELECT incident_id, project_id, kind, task_key, fingerprint, status,
            first_seen_at, last_seen_at, acknowledged_at, resolved_at
     FROM incidents";

const SUBMISSION_SELECT: &str = "SELECT submission_id, project_id, argv_json, created_at,
            pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
     FROM submissions";

const AGENT_RUN_SELECT: &str = "SELECT run_id, project_id, primary_event_id, pid, status,
            started_at, finished_at, exit_code, log_path, last_error,
            launch_gate_state, context_mode, context_session_id, context_lineage_json
     FROM agent_runs";

const TERMINATION_REQUEST_SELECT: &str = "SELECT request_id, incident_id, project_id,
            task_signature, reason, status, requested_at, dispatch_lease_until,
            grace_until, confirmed_at, last_error
     FROM termination_requests";

const TASK_OBSERVATION_SELECT: &str = "SELECT project_id, task_signature, pueue_task_id,
            pueue_group, command_json, state, enqueued_at, started_at, ended_at, result, observed_at
     FROM task_observations";

const INTERVENTION_SELECT: &str = "SELECT intervention_id, project_id, insertion_sequence,
            message, status, created_at, reserved_at, applied_at, agent_run_id, attempts,
            lease_expires_at, reservation_token
     FROM interventions";

const BATCH_REQUEST_SELECT: &str = "SELECT request_id, project_id, manifest_hash, status,
            lease_until, lease_token, created_at, updated_at, last_error
     FROM batch_requests";

const BATCH_JOB_SELECT: &str = "SELECT request_id, job_id, ordinal, kind, argv_json,
            metadata_json, status, pueue_task_id, submission_id, last_error
     FROM batch_jobs";

fn bounded_diagnostic_limit(limit: usize) -> i64 {
    limit.min(MAX_EVENT_LIST_LIMIT) as i64
}

fn exists(
    transaction: &Transaction<'_>,
    query: &str,
    value: impl rusqlite::ToSql,
) -> Result<bool, AppError> {
    transaction
        .query_row(query, [value], |_| Ok(()))
        .optional()
        .map(|row| row.is_some())
        .map_err(database_error("check database uniqueness"))
}

fn project_from_row(row: &Row<'_>) -> rusqlite::Result<Project> {
    Ok(Project {
        project_id: row.get(0)?,
        root_path: PathBuf::from(row.get::<_, String>(1)?),
        pueue_group: row.get(2)?,
        config_path: PathBuf::from(row.get::<_, String>(3)?),
        enabled: row.get(4)?,
        paused: row.get(5)?,
        halted_reason: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn event_from_row(row: &Row<'_>) -> rusqlite::Result<Event> {
    let payload_json: String = row.get(4)?;
    let payload = serde_json::from_str(&payload_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(source))
    })?;
    Ok(Event {
        event_id: row.get(0)?,
        project_id: row.get(1)?,
        kind: row.get(2)?,
        dedup_key: row.get(3)?,
        payload,
        status: row.get(5)?,
        attempts: row.get(6)?,
        not_before: row.get(7)?,
        lease_until: row.get(8)?,
        created_at: row.get(9)?,
        completed_at: row.get(10)?,
        last_error: row.get(11)?,
    })
}

fn intervention_from_row(row: &Row<'_>) -> rusqlite::Result<Intervention> {
    Ok(Intervention {
        intervention_id: row.get(0)?,
        project_id: row.get(1)?,
        insertion_sequence: row.get(2)?,
        message: row.get(3)?,
        status: row.get(4)?,
        created_at: row.get(5)?,
        reserved_at: row.get(6)?,
        applied_at: row.get(7)?,
        agent_run_id: row.get(8)?,
        attempts: row.get(9)?,
        lease_expires_at: row.get(10)?,
        reservation_token: row.get(11)?,
    })
}

fn integration_event_from_row(row: &Row<'_>) -> rusqlite::Result<IntegrationEvent> {
    let payload_json: String = row.get(3)?;
    let payload = serde_json::from_str(&payload_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(source))
    })?;
    Ok(IntegrationEvent {
        integration_event_id: row.get(0)?,
        kind: row.get(1)?,
        dedup_key: row.get(2)?,
        payload,
        created_at: row.get(4)?,
    })
}

fn incident_from_row(row: &Row<'_>) -> rusqlite::Result<Incident> {
    Ok(Incident {
        incident_id: row.get(0)?,
        project_id: row.get(1)?,
        kind: row.get(2)?,
        task_key: row.get(3)?,
        fingerprint: row.get(4)?,
        status: row.get(5)?,
        first_seen_at: row.get(6)?,
        last_seen_at: row.get(7)?,
        acknowledged_at: row.get(8)?,
        resolved_at: row.get(9)?,
    })
}

fn submission_from_row(row: &Row<'_>) -> rusqlite::Result<Submission> {
    let argv_json: String = row.get(2)?;
    let argv = serde_json::from_str(&argv_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(2, Type::Text, Box::new(source))
    })?;
    let metadata_json: String = row.get(8)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(8, Type::Text, Box::new(source))
    })?;
    if !metadata.is_object() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            8,
            Type::Text,
            "submission metadata must be a JSON object".into(),
        ));
    }
    Ok(Submission {
        submission_id: row.get(0)?,
        project_id: row.get(1)?,
        argv,
        created_at: row.get(3)?,
        pueue_task_id: row.get(4)?,
        task_signature: row.get(5)?,
        status: row.get(6)?,
        kind: row.get(7)?,
        metadata,
        origin_agent_run_id: row.get(9)?,
    })
}

fn batch_request_from_row(row: &Row<'_>) -> rusqlite::Result<BatchRequest> {
    Ok(BatchRequest {
        request_id: row.get(0)?,
        project_id: row.get(1)?,
        manifest_hash: row.get(2)?,
        status: row.get(3)?,
        lease_until: row.get(4)?,
        lease_token: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        last_error: row.get(8)?,
        jobs: Vec::new(),
    })
}

fn batch_job_from_row(row: &Row<'_>) -> rusqlite::Result<BatchJob> {
    let argv_json: String = row.get(4)?;
    let argv = serde_json::from_str(&argv_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(source))
    })?;
    let metadata_json: String = row.get(5)?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(source))
    })?;
    if !metadata.is_object() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            5,
            Type::Text,
            "batch metadata must be a JSON object".into(),
        ));
    }
    Ok(BatchJob {
        request_id: row.get(0)?,
        job_id: row.get(1)?,
        ordinal: row.get(2)?,
        kind: row.get(3)?,
        argv,
        metadata,
        status: row.get(6)?,
        pueue_task_id: row.get(7)?,
        submission_id: row.get(8)?,
        last_error: row.get(9)?,
    })
}

fn read_batch(
    connection: &Connection,
    project_id: &str,
    request_id: &str,
) -> Result<Option<BatchRequest>, AppError> {
    let Some(mut batch) = connection
        .query_row(
            &format!(
                "{} WHERE project_id = ?1 AND request_id = ?2",
                BATCH_REQUEST_SELECT
            ),
            params![project_id, request_id],
            batch_request_from_row,
        )
        .optional()
        .map_err(database_error("read batch request"))?
    else {
        return Ok(None);
    };
    let mut statement = connection
        .prepare(&format!(
            "{} WHERE request_id = ?1 ORDER BY ordinal, job_id",
            BATCH_JOB_SELECT
        ))
        .map_err(database_error("prepare batch job query"))?;
    batch.jobs = statement
        .query_map([request_id], batch_job_from_row)
        .map_err(database_error("query batch jobs"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(database_error("read batch jobs"))?;
    Ok(Some(batch))
}

fn read_submission(connection: &Connection, submission_id: &str) -> Result<Submission, AppError> {
    connection
        .query_row(
            &format!("{} WHERE submission_id = ?1", SUBMISSION_SELECT),
            [submission_id],
            submission_from_row,
        )
        .map_err(database_error("read submission"))
}

fn agent_run_from_row(row: &Row<'_>) -> rusqlite::Result<AgentRun> {
    let launch_gate_state: String = row.get(10)?;
    let context_mode_value: String = row.get(11)?;
    let context_session_id: Option<String> = row.get(12)?;
    let context_mode =
        AgentContextMode::from_db_parts(&context_mode_value, context_session_id.clone()).map_err(
            |source| rusqlite::Error::FromSqlConversionFailure(11, Type::Text, Box::new(source)),
        )?;
    let context_lineage_json: String = row.get(13)?;
    let context_lineage = serde_json::from_str(&context_lineage_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(source))
    })?;
    Ok(AgentRun {
        run_id: row.get(0)?,
        project_id: row.get(1)?,
        primary_event_id: row.get(2)?,
        pid: row.get(3)?,
        status: row.get(4)?,
        started_at: row.get(5)?,
        finished_at: row.get(6)?,
        exit_code: row.get(7)?,
        log_path: PathBuf::from(row.get::<_, String>(8)?),
        last_error: row.get(9)?,
        launch_gate_state,
        context_mode,
        context_session_id,
        context_lineage,
    })
}

fn insert_agent_run(transaction: &Transaction<'_>, run: &NewAgentRun) -> Result<i64, AppError> {
    let log_path = path_text(&run.log_path, "log_path")?;
    let context_lineage_json =
        serde_json::to_string(&run.context_lineage).map_err(|source| AppError::Serialization {
            operation: "serialize agent context lineage",
            source,
        })?;
    let launch_gate_state = if run.status == AgentRunStatus::Starting {
        "pending"
    } else {
        "released"
    };
    transaction
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at,
                finished_at, exit_code, log_path, last_error, launch_gate_state,
                context_mode,
                context_session_id, context_lineage_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, NULL, ?7, ?8, ?9, ?10)",
            params![
                run.project_id,
                run.primary_event_id,
                run.pid,
                run.status,
                run.started_at,
                log_path,
                launch_gate_state,
                run.context_mode.as_str(),
                run.context_session_id.as_deref(),
                context_lineage_json,
            ],
        )
        .map_err(database_error("insert agent run"))?;
    Ok(transaction.last_insert_rowid())
}

fn attach_event_to_agent_run(
    transaction: &Transaction<'_>,
    project_id: &str,
    run_id: i64,
    event_id: i64,
) -> Result<(), AppError> {
    transaction
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(run_id, event_id) DO NOTHING",
            params![project_id, run_id, event_id],
        )
        .map_err(database_error("attach event to agent run"))?;
    Ok(())
}

fn read_agent_run(connection: &Connection, run_id: i64) -> Result<AgentRun, AppError> {
    connection
        .query_row(
            &format!("{} WHERE run_id = ?1", AGENT_RUN_SELECT),
            [run_id],
            agent_run_from_row,
        )
        .map_err(database_error("read agent run"))
}

fn read_termination_request(
    connection: &Connection,
    request_id: i64,
) -> Result<TerminationRequest, AppError> {
    connection
        .query_row(
            &format!("{} WHERE request_id = ?1", TERMINATION_REQUEST_SELECT),
            [request_id],
            termination_request_from_row,
        )
        .map_err(database_error("read termination request"))
}

fn termination_request_from_row(row: &Row<'_>) -> rusqlite::Result<TerminationRequest> {
    Ok(TerminationRequest {
        request_id: row.get(0)?,
        incident_id: row.get(1)?,
        project_id: row.get(2)?,
        task_signature: row.get(3)?,
        reason: row.get(4)?,
        status: row.get(5)?,
        requested_at: row.get(6)?,
        dispatch_lease_until: row.get(7)?,
        grace_until: row.get(8)?,
        confirmed_at: row.get(9)?,
        last_error: row.get(10)?,
    })
}

fn task_observation_from_row(row: &Row<'_>) -> rusqlite::Result<TaskObservation> {
    let command_json: String = row.get(4)?;
    let command = serde_json::from_str(&command_json).map_err(|source| {
        rusqlite::Error::FromSqlConversionFailure(4, Type::Text, Box::new(source))
    })?;
    Ok(TaskObservation {
        project_id: row.get(0)?,
        task_signature: row.get(1)?,
        pueue_task_id: row.get(2)?,
        pueue_group: row.get(3)?,
        command,
        state: row.get(5)?,
        enqueued_at: row.get(6)?,
        started_at: row.get(7)?,
        ended_at: row.get(8)?,
        result: row.get(9)?,
        observed_at: row.get(10)?,
    })
}

fn read_incident(transaction: &Transaction<'_>, incident_id: i64) -> Result<Incident, AppError> {
    transaction
        .query_row(
            &format!("{} WHERE incident_id = ?1", INCIDENT_SELECT),
            [incident_id],
            incident_from_row,
        )
        .map_err(database_error("read incident"))
}

fn project_constraint_error(source: rusqlite::Error) -> AppError {
    let message = source.to_string();
    for (column, field) in [
        ("projects.root_path", "root_path"),
        ("projects.pueue_group", "pueue_group"),
        ("projects.project_id", "project_id"),
    ] {
        if message.contains(column) {
            return AppError::DatabaseConflict { field };
        }
    }
    AppError::Database {
        operation: "register project",
        source,
    }
}
