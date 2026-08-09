use rusqlite::{Connection, TransactionBehavior};

use crate::AppError;

use super::database_error;

const LATEST_SCHEMA_VERSION: i64 = 1;

pub(super) fn migrate(connection: &mut Connection) -> Result<(), AppError> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(database_error("begin SQLite migration"))?;
    let version: i64 = transaction
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(database_error("read SQLite schema version"))?;
    if version > LATEST_SCHEMA_VERSION {
        return Err(AppError::Runtime {
            operation: "open a database created by a newer pueue-agent",
        });
    }
    if version == LATEST_SCHEMA_VERSION {
        transaction
            .commit()
            .map_err(database_error("commit SQLite migration check"))?;
        return Ok(());
    }

    transaction
        .execute_batch(
            r#"
            CREATE TABLE projects (
                project_id TEXT PRIMARY KEY,
                root_path TEXT NOT NULL UNIQUE,
                pueue_group TEXT NOT NULL UNIQUE,
                config_path TEXT NOT NULL,
                enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
                paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
                halted_reason TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE events (
                event_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                kind TEXT NOT NULL CHECK (kind IN (
                    'task_finished', 'task_failed', 'crash', 'stalled',
                    'deep_check', 'auto_killed', 'termination_failed'
                )),
                dedup_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN (
                    'pending', 'claimed', 'completed', 'retry_wait', 'failed'
                )),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                not_before INTEGER NOT NULL,
                lease_until INTEGER,
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                last_error TEXT,
                UNIQUE(project_id, dedup_key),
                UNIQUE(project_id, event_id),
                CHECK (
                    (status = 'claimed' AND lease_until IS NOT NULL)
                    OR (status <> 'claimed' AND lease_until IS NULL)
                )
            );

            CREATE TABLE incidents (
                incident_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                task_key TEXT,
                fingerprint TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('open', 'acknowledged', 'resolved')),
                first_seen_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                acknowledged_at INTEGER,
                resolved_at INTEGER,
                UNIQUE(project_id, incident_id)
            );

            CREATE TABLE agent_runs (
                run_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                primary_event_id INTEGER NOT NULL,
                pid INTEGER,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                exit_code INTEGER,
                log_path TEXT NOT NULL,
                last_error TEXT,
                UNIQUE(project_id, run_id),
                FOREIGN KEY(project_id, primary_event_id)
                    REFERENCES events(project_id, event_id) ON DELETE RESTRICT
            );

            CREATE TABLE agent_run_events (
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                run_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                PRIMARY KEY(run_id, event_id),
                FOREIGN KEY(project_id, run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE CASCADE,
                FOREIGN KEY(project_id, event_id)
                    REFERENCES events(project_id, event_id) ON DELETE RESTRICT
            );

            CREATE TABLE submissions (
                submission_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                argv_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                pueue_task_id INTEGER,
                task_signature TEXT,
                status TEXT NOT NULL
            );

            CREATE TABLE termination_requests (
                request_id INTEGER PRIMARY KEY,
                incident_id INTEGER NOT NULL,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                task_signature TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN (
                    'requested', 'sent', 'confirmed', 'timed_out', 'failed'
                )),
                requested_at INTEGER NOT NULL,
                grace_until INTEGER,
                confirmed_at INTEGER,
                last_error TEXT,
                UNIQUE(project_id, incident_id, task_signature),
                FOREIGN KEY(project_id, incident_id)
                    REFERENCES incidents(project_id, incident_id) ON DELETE CASCADE
            );

            CREATE TABLE task_observations (
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                task_signature TEXT NOT NULL,
                pueue_task_id INTEGER NOT NULL,
                pueue_group TEXT NOT NULL,
                command_json TEXT NOT NULL,
                state TEXT NOT NULL,
                enqueued_at INTEGER,
                started_at INTEGER,
                ended_at INTEGER,
                result TEXT,
                observed_at INTEGER NOT NULL,
                PRIMARY KEY(project_id, task_signature)
            );

            CREATE INDEX events_claimable_idx
                ON events(status, not_before, created_at, event_id)
                WHERE status IN ('pending', 'retry_wait');
            CREATE INDEX events_project_status_idx
                ON events(project_id, status, created_at);
            CREATE UNIQUE INDEX incidents_active_fingerprint_idx
                ON incidents(project_id, kind, fingerprint)
                WHERE status IN ('open', 'acknowledged');
            CREATE INDEX incidents_project_status_idx
                ON incidents(project_id, status, last_seen_at);
            CREATE INDEX agent_runs_project_status_idx
                ON agent_runs(project_id, status, started_at);
            CREATE UNIQUE INDEX agent_runs_one_active_per_project_idx
                ON agent_runs(project_id)
                WHERE status IN ('starting', 'running');
            CREATE INDEX agent_run_events_event_idx
                ON agent_run_events(event_id);
            CREATE INDEX submissions_project_status_idx
                ON submissions(project_id, status, created_at);
            CREATE INDEX termination_requests_project_status_idx
                ON termination_requests(project_id, status, requested_at);
            CREATE INDEX task_observations_group_state_idx
                ON task_observations(project_id, pueue_group, state, observed_at);

            PRAGMA user_version = 1;
            "#,
        )
        .map_err(database_error("apply SQLite migrations"))?;
    transaction
        .commit()
        .map_err(database_error("commit SQLite migration"))?;

    Ok(())
}
