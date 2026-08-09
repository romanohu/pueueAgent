use std::{fs, path::PathBuf};

use rusqlite::{
    params, types::Type, Connection, OptionalExtension, Row, Transaction, TransactionBehavior,
};

use crate::{
    models::{
        path_text, AgentRun, AgentRunEvent, Event, Incident, IncidentTransition, IncidentUpdate,
        IntegrationEvent, NewAgentRun, NewEvent, NewIncident, NewIntegrationEvent, NewProject,
        NewSubmission, NewTaskObservation, NewTerminationRequest, Project, Submission,
        SubmissionStatus, TaskObservation, TerminationRequest, TerminationRequestStatus,
    },
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
}

pub struct SubmissionRepository<'db> {
    db: &'db Db,
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
        let mut connection = self.db.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error("begin idempotent submission insert"))?;
        transaction
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status
                 ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5)
                 ON CONFLICT(submission_id) DO NOTHING",
                params![
                    submission.submission_id,
                    submission.project_id,
                    argv_json,
                    submission.created_at,
                    submission.status,
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
}

pub struct AgentRunRepository<'db> {
    db: &'db Db,
}

impl<'db> AgentRunRepository<'db> {
    pub fn new(db: &'db Db) -> Self {
        Self { db }
    }

    pub fn insert(&self, run: &NewAgentRun) -> Result<AgentRun, AppError> {
        let log_path = path_text(&run.log_path, "log_path")?;
        let connection = self.db.connect()?;
        connection
            .execute(
                "INSERT INTO agent_runs (
                    project_id, primary_event_id, pid, status, started_at,
                    finished_at, exit_code, log_path, last_error
                 ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, NULL)",
                params![
                    run.project_id,
                    run.primary_event_id,
                    run.pid,
                    run.status,
                    run.started_at,
                    log_path,
                ],
            )
            .map_err(database_error("insert agent run"))?;
        read_agent_run(&connection, connection.last_insert_rowid())
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
        transaction
            .execute(
                "INSERT INTO agent_run_events (project_id, run_id, event_id)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(run_id, event_id) DO NOTHING",
                params![project_id, run_id, event_id],
            )
            .map_err(database_error("attach event to agent run"))?;
        transaction
            .commit()
            .map_err(database_error("commit agent run event attachment"))?;
        Ok(AgentRunEvent {
            project_id,
            run_id,
            event_id,
        })
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
                 SET status = ?1, confirmed_at = ?2, last_error = ?3
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

    pub fn find_pending(&self, project_id: &str) -> Result<Vec<TerminationRequest>, AppError> {
        let connection = self.db.connect()?;
        let mut statement = connection
            .prepare(&format!(
                "{} WHERE project_id = ?1 AND status IN (?2, ?3)
                 ORDER BY requested_at, request_id",
                TERMINATION_REQUEST_SELECT
            ))
            .map_err(database_error("prepare pending termination request query"))?;
        let rows = statement
            .query_map(
                params![
                    project_id,
                    TerminationRequestStatus::Requested,
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
            pueue_task_id, task_signature, status
     FROM submissions";

const AGENT_RUN_SELECT: &str = "SELECT run_id, project_id, primary_event_id, pid, status,
            started_at, finished_at, exit_code, log_path, last_error
     FROM agent_runs";

const TERMINATION_REQUEST_SELECT: &str = "SELECT request_id, incident_id, project_id,
            task_signature, reason, status, requested_at, grace_until, confirmed_at, last_error
     FROM termination_requests";

const TASK_OBSERVATION_SELECT: &str = "SELECT project_id, task_signature, pueue_task_id,
            pueue_group, command_json, state, enqueued_at, started_at, ended_at, result, observed_at
     FROM task_observations";

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
    Ok(Submission {
        submission_id: row.get(0)?,
        project_id: row.get(1)?,
        argv,
        created_at: row.get(3)?,
        pueue_task_id: row.get(4)?,
        task_signature: row.get(5)?,
        status: row.get(6)?,
    })
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
    })
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
        grace_until: row.get(7)?,
        confirmed_at: row.get(8)?,
        last_error: row.get(9)?,
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
