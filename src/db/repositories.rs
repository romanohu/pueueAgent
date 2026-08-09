use std::{fs, path::PathBuf};

use rusqlite::{params, types::Type, OptionalExtension, Row, Transaction, TransactionBehavior};

use crate::{
    models::{
        path_text, Event, Incident, IncidentTransition, IncidentUpdate, NewEvent, NewIncident,
        NewProject, Project,
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

        let update = if let Some(active) = active {
            let changed = incident.seen_at > active.last_seen_at
                || incident
                    .task_key
                    .as_ref()
                    .is_some_and(|task_key| Some(task_key) != active.task_key.as_ref());
            if changed {
                transaction
                    .execute(
                        "UPDATE incidents
                         SET task_key = COALESCE(?1, task_key),
                             last_seen_at = MAX(last_seen_at, ?2)
                         WHERE incident_id = ?3",
                        params![incident.task_key, incident.seen_at, active.incident_id],
                    )
                    .map_err(database_error("update active incident"))?;
            }
            let stored = read_incident(&transaction, active.incident_id)?;
            IncidentUpdate {
                incident: stored,
                transition: if changed {
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
}

const EVENT_SELECT: &str =
    "SELECT event_id, project_id, kind, dedup_key, payload_json, status, attempts,
            not_before, lease_until, created_at, completed_at, last_error
     FROM events";

const INCIDENT_SELECT: &str = "SELECT incident_id, project_id, kind, task_key, fingerprint, status,
            first_seen_at, last_seen_at, acknowledged_at, resolved_at
     FROM incidents";

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
