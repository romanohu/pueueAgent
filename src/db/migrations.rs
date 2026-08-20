use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::{environment::MAX_PRIVATE_TEMP_RUN_ID, AppError};

use super::database_error;

pub const LATEST_SCHEMA_VERSION: i64 = 17;
const ACTIVE_AGENT_INDEX_SQL: &str = r#"
    CREATE UNIQUE INDEX IF NOT EXISTS agent_runs_one_active_per_project_idx
        ON agent_runs(project_id)
        WHERE status IN ('starting', 'running');
"#;
const INTERVENTION_SEQUENCE_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX interventions_project_sequence_idx
    ON interventions(project_id, insertion_sequence);";
const INTERVENTION_STATUS_INDEX_SQL: &str = "CREATE INDEX interventions_project_status_created_idx
    ON interventions(project_id, status, insertion_sequence, intervention_id);";
const EVENTS_PROJECT_STATUS_NOT_BEFORE_INDEX_SQL: &str = "CREATE INDEX events_project_status_not_before_idx
    ON events(project_id, status, not_before, event_id);";
const EVENTS_V13_STATUS_LIST: &str =
    "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter'";
const AGENT_RUN_V14_EXECUTION_COLUMNS: [(&str, &str); 5] = [
    (
        "execution_kind",
        "ALTER TABLE agent_runs ADD COLUMN execution_kind TEXT",
    ),
    (
        "executable_path",
        "ALTER TABLE agent_runs ADD COLUMN executable_path TEXT",
    ),
    (
        "executable_identity",
        "ALTER TABLE agent_runs ADD COLUMN executable_identity TEXT",
    ),
    (
        "policy_code",
        "ALTER TABLE agent_runs ADD COLUMN policy_code TEXT",
    ),
    (
        "failure_stage",
        "ALTER TABLE agent_runs ADD COLUMN failure_stage TEXT",
    ),
];
const AGENT_RUN_ID_SEQUENCE_TABLE_SQL: &str = r#"
    CREATE TABLE agent_run_id_sequence (
        sequence_id INTEGER PRIMARY KEY CHECK (sequence_id = 1),
        last_run_id INTEGER NOT NULL CHECK (
            last_run_id >= 0 AND last_run_id <= 9223372036854775806
        )
    );
"#;
const OPERATOR_LOGS_SQL: &str = r#"
    CREATE TABLE IF NOT EXISTS operator_logs (
        log_id INTEGER PRIMARY KEY,
        project_id TEXT NOT NULL,
        pueue_group TEXT NOT NULL,
        action TEXT NOT NULL CHECK (action IN (
            'pause', 'resume', 'halt', 'disable', 'remove', 'cancel'
        )),
        details_json TEXT NOT NULL,
        created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS operator_logs_project_created_idx
        ON operator_logs(project_id, created_at, log_id);
"#;
const CAMPAIGNS_V16_TABLE_SQL: &str = r#"
CREATE TABLE campaigns (
    campaign_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    objective_text TEXT NOT NULL,
    objective_digest TEXT NOT NULL,
    initial_argv_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'active','budget_waiting','goal_reached_pending_review','paused',
        'degraded','halted','retired'
    )),
    state_reason TEXT,
    baseline_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    next_eligible_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
"#;
const PROPOSALS_V16_TABLE_SQL: &str = r#"
CREATE TABLE proposals (
    proposal_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN (
        'experiment','repair','broader_search','recipe','code_change','data_evaluation'
    )),
    status TEXT NOT NULL CHECK (status IN ('pending','accepted','rejected')),
    hypothesis TEXT NOT NULL,
    source_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    argv_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    expected_evidence_json TEXT NOT NULL,
    canonical_digest TEXT NOT NULL,
    reject_reason TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(campaign_id, canonical_digest)
);
"#;
const EXPERIMENTS_V16_TABLE_SQL: &str = r#"
CREATE TABLE experiments (
    experiment_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    proposal_id TEXT NOT NULL REFERENCES proposals(proposal_id) ON DELETE RESTRICT,
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions(submission_id) ON DELETE RESTRICT,
    parent_experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    attempt INTEGER NOT NULL CHECK (attempt >= 0),
    status TEXT NOT NULL CHECK (status IN (
        'reserved','submitting','accepted','unreconciled',
        'succeeded','failed','cancelled'
    )),
    pueue_task_id INTEGER,
    task_signature TEXT,
    failure_code TEXT,
    failure_fingerprint TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    finished_at INTEGER,
    UNIQUE(proposal_id, attempt),
    CHECK ((pueue_task_id IS NULL) = (task_signature IS NULL))
);
"#;
const BUDGET_RESERVATIONS_V16_TABLE_SQL: &str = r#"
CREATE TABLE budget_reservations (
    reservation_id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id) ON DELETE CASCADE,
    experiment_id TEXT REFERENCES experiments(experiment_id) ON DELETE RESTRICT,
    dimension TEXT NOT NULL CHECK (dimension IN ('experiment','agent_run','code_change')),
    subject_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('reserved','consumed','released')),
    window_started_at INTEGER NOT NULL,
    window_ends_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(campaign_id, dimension, subject_key),
    CHECK (window_ends_at > window_started_at),
    CHECK (
        (dimension = 'experiment' AND experiment_id IS NOT NULL)
        OR (dimension <> 'experiment' AND experiment_id IS NULL)
    )
);
"#;
const CAMPAIGNS_ONE_LIVE_PROJECT_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX campaigns_one_live_project_idx
    ON campaigns(project_id) WHERE state <> 'retired';";
const CAMPAIGNS_STATE_NEXT_ELIGIBLE_INDEX_SQL: &str =
    "CREATE INDEX campaigns_state_next_eligible_idx
    ON campaigns(state, next_eligible_at, campaign_id);";
const PROPOSALS_CAMPAIGN_STATUS_CREATED_INDEX_SQL: &str =
    "CREATE INDEX proposals_campaign_status_created_idx
    ON proposals(campaign_id, status, created_at, proposal_id);";
const EXPERIMENTS_CAMPAIGN_STATUS_CREATED_INDEX_SQL: &str =
    "CREATE INDEX experiments_campaign_status_created_idx
    ON experiments(campaign_id, status, created_at, experiment_id);";
const EXPERIMENTS_PUEUE_TASK_LOOKUP_INDEX_SQL: &str =
    "CREATE INDEX experiments_pueue_task_lookup_idx
    ON experiments(pueue_task_id, task_signature);";
const BUDGET_RESERVATIONS_CAMPAIGN_DIMENSION_WINDOW_INDEX_SQL: &str =
    "CREATE INDEX budget_reservations_campaign_dimension_window_idx
    ON budget_reservations(campaign_id, dimension, window_started_at, window_ends_at, reservation_id);";
const EVENTS_CAMPAIGN_STATUS_NOT_BEFORE_INDEX_SQL: &str =
    "CREATE INDEX events_campaign_status_not_before_idx
    ON events(campaign_id, status, not_before, event_id);";

pub(super) fn migrate(connection: &mut Connection) -> Result<(), AppError> {
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(database_error("read SQLite schema version"))?;
    if version > LATEST_SCHEMA_VERSION {
        return Err(AppError::Runtime {
            operation: "open a database created by a newer pueue-agent",
        });
    }
    let current_schema_has_composite_origin_foreign_key =
        version == LATEST_SCHEMA_VERSION
            && submissions_have_composite_origin_foreign_key(connection)?;
    let current_schema_has_execution_projection = version == LATEST_SCHEMA_VERSION
        && missing_execution_projection_columns(connection)?.is_empty();
    if version == LATEST_SCHEMA_VERSION {
        validate_agent_run_id_sequence(connection)?;
        // Current-schema databases used to bypass all validation. Keep the
        // no-write fast path only after checking the canonical status CHECK,
        // integrity, and the new required event index.
        verify_events_v13(connection, EVENTS_V13_STATUS_LIST)?;
        let event_status_not_before_index_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master
                 WHERE type = 'index' AND name = 'events_project_status_not_before_idx'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("check current SQLite event indexes"))?;
        let has_canonical_event_status_not_before_index = event_status_not_before_index_sql
            .as_deref()
            .is_some_and(|sql| {
                compact_sql(sql) == compact_sql(EVENTS_PROJECT_STATUS_NOT_BEFORE_INDEX_SQL)
            });
        if current_schema_has_composite_origin_foreign_key
            && current_schema_has_execution_projection
            && has_canonical_event_status_not_before_index
        {
            verify_campaign_schema_v16(connection)?;
            verify_campaign_event_lineage_v17(connection)?;
            return Ok(());
        }
    }
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
    if version == 0 {
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
                    'deep_check', 'auto_killed', 'termination_failed', 'operator_wake'
                )),
                dedup_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN (
                    'pending', 'claimed', 'in_flight', 'dispatched',
                    'completed', 'retry_wait', 'failed', 'dead_letter'
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

            CREATE TABLE integration_events (
                integration_event_id INTEGER PRIMARY KEY,
                kind TEXT NOT NULL CHECK (kind IN ('unknown_callback_group')),
                dedup_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
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
                launch_gate_state TEXT NOT NULL DEFAULT 'pending' CHECK (
                    launch_gate_state IN ('pending', 'release_requested', 'released', 'failed')
                ),
                context_mode TEXT NOT NULL DEFAULT 'fresh' CHECK (context_mode IN (
                    'fresh', 'resume', 'resume_latest'
                )),
                context_session_id TEXT,
                context_lineage_json TEXT NOT NULL DEFAULT '[]',
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
                status TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'experiment',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                origin_agent_run_id INTEGER,
                FOREIGN KEY (project_id, origin_agent_run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE RESTRICT
            );

            CREATE TABLE termination_requests (
                request_id INTEGER PRIMARY KEY,
                incident_id INTEGER NOT NULL,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                task_signature TEXT NOT NULL,
                reason TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN (
                    'requested', 'dispatching', 'sent', 'confirmed', 'timed_out', 'failed'
                )),
                requested_at INTEGER NOT NULL,
                dispatch_lease_until INTEGER,
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
                first_observed_at INTEGER NOT NULL,
                observed_at INTEGER NOT NULL,
                PRIMARY KEY(project_id, task_signature)
            );

            CREATE TABLE operator_logs (
                log_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL,
                pueue_group TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN (
                    'pause', 'resume', 'halt', 'disable', 'remove', 'cancel'
                )),
                details_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );

            CREATE TABLE interventions (
                intervention_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                insertion_sequence INTEGER NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'reserved', 'applied')),
                created_at INTEGER NOT NULL,
                reserved_at INTEGER,
                applied_at INTEGER,
                agent_run_id INTEGER,
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                lease_expires_at INTEGER,
                reservation_token TEXT,
                FOREIGN KEY (project_id, agent_run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE SET NULL,
                CHECK (
                    (status = 'pending' AND reserved_at IS NULL AND applied_at IS NULL AND agent_run_id IS NULL AND lease_expires_at IS NULL AND reservation_token IS NULL)
                    OR (status = 'reserved' AND reserved_at IS NOT NULL AND applied_at IS NULL AND lease_expires_at IS NOT NULL AND reservation_token IS NOT NULL)
                    OR (status = 'applied' AND reserved_at IS NOT NULL AND applied_at IS NOT NULL AND agent_run_id IS NOT NULL)
                )
            );

            CREATE INDEX events_claimable_idx
                ON events(status, not_before, created_at, event_id)
                WHERE status IN ('pending', 'retry_wait');
            CREATE INDEX events_project_status_idx
                ON events(project_id, status, created_at);
            CREATE INDEX events_project_status_not_before_idx
                ON events(project_id, status, not_before, event_id);
            CREATE INDEX integration_events_kind_created_idx
                ON integration_events(kind, created_at);
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
            CREATE INDEX submissions_project_kind_status_idx
                ON submissions(project_id, kind, status, created_at);
            CREATE INDEX submissions_project_origin_agent_run_idx
                ON submissions(project_id, origin_agent_run_id, created_at, submission_id);
            CREATE INDEX termination_requests_project_status_idx
                ON termination_requests(project_id, status, requested_at);
            CREATE INDEX task_observations_group_state_idx
                ON task_observations(project_id, pueue_group, state, observed_at);
            CREATE INDEX operator_logs_project_created_idx
                ON operator_logs(project_id, created_at, log_id);
            CREATE UNIQUE INDEX interventions_project_sequence_idx
                ON interventions(project_id, insertion_sequence);
            CREATE INDEX interventions_project_status_created_idx
                ON interventions(project_id, status, insertion_sequence, intervention_id);
            CREATE INDEX interventions_reservation_lease_idx
                ON interventions(status, lease_expires_at, reservation_token);

            PRAGMA user_version = 8;
            "#,
            )
            .map_err(database_error("apply SQLite migrations"))?;
    } else if version == 1 {
        transaction
            .execute_batch(
                r#"
            CREATE TABLE integration_events (
                integration_event_id INTEGER PRIMARY KEY,
                kind TEXT NOT NULL CHECK (kind IN ('unknown_callback_group')),
                dedup_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX integration_events_kind_created_idx
                ON integration_events(kind, created_at);

            ALTER TABLE agent_runs
                ADD COLUMN context_mode TEXT NOT NULL DEFAULT 'fresh';
            ALTER TABLE agent_runs
                ADD COLUMN context_session_id TEXT;
            ALTER TABLE agent_runs
                ADD COLUMN context_lineage_json TEXT NOT NULL DEFAULT '[]';

            CREATE TABLE operator_logs (
                log_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL,
                pueue_group TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN (
                    'pause', 'resume', 'halt', 'disable', 'remove'
                )),
                details_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX operator_logs_project_created_idx
                ON operator_logs(project_id, created_at, log_id);

            PRAGMA user_version = 4;
            "#,
            )
            .map_err(database_error("apply SQLite v2 migration"))?;
    } else if version == 2 {
        transaction
            .execute_batch(
                r#"
            ALTER TABLE agent_runs
                ADD COLUMN context_mode TEXT NOT NULL DEFAULT 'fresh';
            ALTER TABLE agent_runs
                ADD COLUMN context_session_id TEXT;
            ALTER TABLE agent_runs
                ADD COLUMN context_lineage_json TEXT NOT NULL DEFAULT '[]';

            CREATE TABLE operator_logs (
                log_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL,
                pueue_group TEXT NOT NULL,
                action TEXT NOT NULL CHECK (action IN (
                    'pause', 'resume', 'halt', 'disable', 'remove'
                )),
                details_json TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX operator_logs_project_created_idx
                ON operator_logs(project_id, created_at, log_id);

            PRAGMA user_version = 4;
            "#,
            )
            .map_err(database_error("apply SQLite v3 migration"))?;
    } else if version == 3 {
        transaction
            .execute_batch(&format!(
                r#"
            {OPERATOR_LOGS_SQL}

            PRAGMA user_version = 4;
            "#
            ))
            .map_err(database_error("apply SQLite v4 migration"))?;
    } else if version == 4 {
        migrate_termination_requests_to_v5(&transaction)?;
    } else if version == 5 {
        migrate_interventions_to_v6(&transaction)?;
    } else if version == 6 {
        migrate_submissions_to_v7(&transaction)?;
    } else if version == 7 {
        migrate_submissions_to_v7(&transaction)?;
        migrate_events_to_v8(&transaction)?;
    }
    if (1..=3).contains(&version) {
        ensure_agent_run_event_project_id(&transaction)?;
        migrate_termination_requests_to_v5(&transaction)?;
    }
    if (1..=4).contains(&version) {
        migrate_interventions_to_v6(&transaction)?;
    }
    if (1..=5).contains(&version) {
        migrate_submissions_to_v7(&transaction)?;
    }
    if (1..=6).contains(&version) {
        migrate_events_to_v8(&transaction)?;
    }
    if version <= 8 {
        migrate_batches_to_v9(&transaction)?;
    }
    if version <= 9 {
        migrate_batches_to_v10(&transaction)?;
    }
    if version <= 10 {
        migrate_operator_logs_to_v11(&transaction)?;
    }
    if version <= 11 {
        migrate_task_observations_to_v12(&transaction)?;
    }
    if version <= 12 {
        migrate_events_to_v13(&transaction)?;
    }
    if version >= 13 {
        verify_events_v13(&transaction, EVENTS_V13_STATUS_LIST)?;
    }
    let missing_execution_projection_columns =
        missing_execution_projection_columns(&transaction)?;
    if version <= 13 || !missing_execution_projection_columns.is_empty() {
        migrate_agent_run_execution_projection_to_v14(
            &transaction,
            &missing_execution_projection_columns,
        )?;
    }
    ensure_agent_run_id_sequence(&transaction, version)?;
    ensure_agent_run_launch_gate(&transaction)?;
    ensure_intervention_insertion_sequence(&transaction)?;
    ensure_invariant_indexes(&transaction)?;
    ensure_index_definition(
        &transaction,
        "events_project_status_not_before_idx",
        EVENTS_PROJECT_STATUS_NOT_BEFORE_INDEX_SQL,
    )?;
    if version != LATEST_SCHEMA_VERSION || !current_schema_has_composite_origin_foreign_key {
        ensure_submission_indexes(&transaction)?;
    }
    if version <= 15 {
        migrate_campaign_schema_to_v16(&transaction)?;
    } else {
        verify_campaign_schema_v16(&transaction)?;
    }
    if version <= 16 {
        migrate_campaign_event_lineage_to_v17(&transaction)?;
    } else {
        verify_campaign_event_lineage_v17(&transaction)?;
    }
    transaction
        .commit()
        .map_err(database_error("commit SQLite migration"))?;

    Ok(())
}

fn migrate_agent_run_execution_projection_to_v14(
    transaction: &rusqlite::Transaction<'_>,
    missing_columns: &[&str],
) -> Result<(), AppError> {
    for (name, statement) in AGENT_RUN_V14_EXECUTION_COLUMNS {
        if missing_columns.contains(&name) {
            transaction
                .execute_batch(statement)
                .map_err(database_error("add agent run v14 projection column"))?;
        }
    }
    if !missing_execution_projection_columns(transaction)?.is_empty() {
        return Err(AppError::Runtime {
            operation: "verify SQLite v14 agent run execution projection schema",
        });
    }
    transaction
        .execute_batch("PRAGMA user_version = 14;")
        .map_err(database_error("set SQLite v14 schema version"))
}

fn missing_execution_projection_columns(
    connection: &Connection,
) -> Result<Vec<&'static str>, AppError> {
    let columns = {
        let mut statement = connection
            .prepare("PRAGMA table_info(agent_runs)")
            .map_err(database_error("inspect agent run v14 projection columns"))?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(database_error("query agent run v14 projection columns"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(database_error("read agent run v14 projection columns"))?
    };
    let mut missing = Vec::new();
    for (expected_name, _) in AGENT_RUN_V14_EXECUTION_COLUMNS {
        let Some((name, declared_type, not_null)) = columns
            .iter()
            .find(|(name, _, _)| name.eq_ignore_ascii_case(expected_name))
        else {
            missing.push(expected_name);
            continue;
        };
        let has_canonical_name = name == expected_name;
        let has_text_type = declared_type.trim().eq_ignore_ascii_case("TEXT");
        if !has_canonical_name || !has_text_type || *not_null != 0 {
            return Err(AppError::Runtime {
                operation: "verify SQLite v14 agent run execution projection schema",
            });
        }
    }
    Ok(missing)
}

fn validate_agent_run_id_sequence(connection: &Connection) -> Result<(), AppError> {
    let table_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'agent_run_id_sequence'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| invalid_agent_run_id_sequence())?;
    if !table_sql.as_deref().is_some_and(|sql| {
        compact_sql(sql) == compact_sql(AGENT_RUN_ID_SEQUENCE_TABLE_SQL)
    }) {
        return Err(AppError::Runtime {
            operation: "validate SQLite agent run ID sequence schema",
        });
    }
    let row_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_run_id_sequence",
            [],
            |row| row.get(0),
        )
        .map_err(|_| invalid_agent_run_id_sequence())?;
    if row_count != 1 {
        return Err(invalid_agent_run_id_sequence());
    }
    let (sequence_id, last_run_id): (i64, i64) = connection
        .query_row(
            "SELECT sequence_id, last_run_id FROM agent_run_id_sequence",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|_| invalid_agent_run_id_sequence())?;
    let max_run_id: i64 = connection
        .query_row(
            "SELECT COALESCE(MAX(run_id), 0) FROM agent_runs",
            [],
            |row| row.get(0),
        )
        .map_err(|_| invalid_agent_run_id_sequence())?;
    if sequence_id != 1
        || last_run_id < 0
        || last_run_id > MAX_PRIVATE_TEMP_RUN_ID
        || max_run_id > MAX_PRIVATE_TEMP_RUN_ID
        || last_run_id < max_run_id
    {
        return Err(invalid_agent_run_id_sequence());
    }
    Ok(())
}

fn invalid_agent_run_id_sequence() -> AppError {
    AppError::Runtime {
        operation: "validate SQLite agent run ID sequence",
    }
}

fn ensure_agent_run_id_sequence(
    transaction: &rusqlite::Transaction<'_>,
    source_version: i64,
) -> Result<(), AppError> {
    let table_sql: Option<String> = transaction
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'agent_run_id_sequence'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("check agent run ID sequence table"))?;
    if let Some(sql) = table_sql {
        if compact_sql(&sql) != compact_sql(AGENT_RUN_ID_SEQUENCE_TABLE_SQL) {
            return Err(AppError::Runtime {
                operation: "validate SQLite agent run ID sequence schema",
            });
        }
    } else {
        transaction
            .execute_batch(AGENT_RUN_ID_SEQUENCE_TABLE_SQL)
            .map_err(database_error("create agent run ID sequence table"))?;
        let max_run_id: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(run_id), 0) FROM agent_runs",
                [],
                |row| row.get(0),
            )
            .map_err(database_error("backfill maximum agent run ID"))?;
        transaction
            .execute(
                "INSERT INTO agent_run_id_sequence (sequence_id, last_run_id)
                 VALUES (1, ?1)",
                [max_run_id],
            )
            .map_err(database_error("initialize agent run ID sequence"))?;
    }
    validate_agent_run_id_sequence(transaction)
        .map_err(|_| AppError::Runtime {
            operation: "validate SQLite agent run ID sequence after migration",
        })?;
    if source_version < 15 {
        transaction
            .execute_batch("PRAGMA user_version = 15;")
            .map_err(database_error("set SQLite v15 schema version"))?;
    }
    Ok(())
}

fn migrate_events_to_v8(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    transaction.execute_batch(r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(sql, '''termination_failed''', '''termination_failed'', ''operator_wake''')
         WHERE type = 'table' AND name = 'events';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 8;
    "#).map_err(database_error("apply SQLite v8 event migration"))
}

fn migrate_events_to_v13(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    const OLD_STATUS_LIST: &str =
        "'pending', 'claimed', 'completed', 'retry_wait', 'failed'";
    let current_event_sql: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite events schema for v13 migration"))?;
    if current_event_sql.contains(EVENTS_V13_STATUS_LIST) {
        verify_events_v13(transaction, EVENTS_V13_STATUS_LIST)?;
        return transaction
            .execute_batch("PRAGMA user_version = 13;")
            .map_err(database_error("set SQLite v13 schema version"));
    }

    transaction
        .execute_batch("PRAGMA writable_schema = ON;")
        .map_err(database_error("enable SQLite writable schema for v13 event migration"))?;
    let replaced = transaction
        .execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'
                AND sql LIKE '%' || ?1 || '%'",
            params![OLD_STATUS_LIST, EVENTS_V13_STATUS_LIST],
        )
        .map_err(database_error("replace SQLite v13 event status list"));
    let writable_schema_disabled = transaction
        .execute_batch("PRAGMA writable_schema = OFF;")
        .map_err(database_error("disable SQLite writable schema after v13 event migration"));
    let replaced = replaced?;
    writable_schema_disabled?;
    if replaced != 1 {
        return Err(AppError::Runtime {
            operation: "migrate exactly one SQLite events status list to v13",
        });
    }

    verify_events_v13(transaction, EVENTS_V13_STATUS_LIST)?;
    transaction
        .execute_batch("PRAGMA user_version = 13;")
        .map_err(database_error("set SQLite v13 schema version"))
}

fn verify_events_v13(
    connection: &Connection,
    canonical_status_list: &str,
) -> Result<(), AppError> {
    let event_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite v13 events schema"))?;
    let canonical_status_list = canonical_status_list
        .split_whitespace()
        .collect::<String>();
    let actual_status_list = event_sql
        .split_once("status TEXT NOT NULL CHECK (status IN (")
        .and_then(|(_, remainder)| remainder.split_once(")").map(|(list, _)| list))
        .map(|list| list.split_whitespace().collect::<String>());
    if actual_status_list.as_deref() != Some(canonical_status_list.as_str()) {
        return Err(AppError::Runtime {
            operation: "verify SQLite v13 events status list",
        });
    }
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(database_error("check SQLite integrity after v13 event migration"))?;
    if integrity != "ok" {
        return Err(AppError::Runtime {
            operation: "verify SQLite integrity after v13 event migration",
        });
    }
    Ok(())
}

fn migrate_batches_to_v9(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    transaction
        .execute_batch(
            r#"
        CREATE TABLE IF NOT EXISTS batch_requests (
            request_id TEXT PRIMARY KEY
                CHECK (length(request_id) BETWEEN 1 AND 128),
            project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
            manifest_hash TEXT NOT NULL
                CHECK (length(manifest_hash) BETWEEN 1 AND 128),
            status TEXT NOT NULL CHECK (status IN (
                'pending', 'dispatching', 'accepted', 'partial', 'failed', 'completed'
            )),
            lease_until INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2048),
            CHECK (
                (status = 'dispatching' AND lease_until IS NOT NULL)
                OR status = 'accepted'
                OR (status <> 'dispatching' AND lease_until IS NULL)
            )
        );

        CREATE TABLE IF NOT EXISTS batch_jobs (
            request_id TEXT NOT NULL REFERENCES batch_requests(request_id) ON DELETE CASCADE,
            job_id TEXT NOT NULL CHECK (length(job_id) BETWEEN 1 AND 128),
            ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
            kind TEXT NOT NULL CHECK (kind IN ('experiment', 'control')),
            argv_json TEXT NOT NULL CHECK (length(argv_json) <= 65536),
            metadata_json TEXT NOT NULL CHECK (length(metadata_json) <= 16384),
            status TEXT NOT NULL CHECK (status IN ('pending', 'dispatching', 'accepted', 'failed')),
            pueue_task_id INTEGER CHECK (pueue_task_id IS NULL OR pueue_task_id >= 0),
            submission_id TEXT CHECK (submission_id IS NULL OR length(submission_id) BETWEEN 1 AND 128),
            last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2048),
            PRIMARY KEY (request_id, job_id),
            UNIQUE (request_id, ordinal),
            CHECK (
                (status = 'accepted' AND pueue_task_id IS NOT NULL AND submission_id IS NOT NULL)
                OR (status <> 'accepted' AND pueue_task_id IS NULL AND submission_id IS NULL)
            )
        );

        CREATE INDEX IF NOT EXISTS batch_requests_project_status_idx
            ON batch_requests(project_id, status, updated_at, request_id);
        CREATE INDEX IF NOT EXISTS batch_requests_lease_idx
            ON batch_requests(status, lease_until, project_id, request_id);
        CREATE INDEX IF NOT EXISTS batch_jobs_request_status_idx
            ON batch_jobs(request_id, status, ordinal, job_id);

        PRAGMA user_version = 9;
        "#,
        )
        .map_err(database_error("apply SQLite v9 batch migration"))
}

fn migrate_batches_to_v10(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    let has_lease_token: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('batch_requests')
                 WHERE name = 'lease_token'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check batch lease token for migration"))?;
    if !has_lease_token {
        transaction
            .execute_batch(
                "ALTER TABLE batch_requests
                     ADD COLUMN lease_token TEXT
                         CHECK (lease_token IS NULL OR length(lease_token) <= 128);",
            )
            .map_err(database_error("apply SQLite v10 batch lease migration"))?;
    }
    transaction
        .execute_batch("PRAGMA user_version = 10;")
        .map_err(database_error("set SQLite v10 schema version"))
}

fn migrate_operator_logs_to_v11(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    transaction
        .execute_batch(
            r#"
        DROP INDEX IF EXISTS operator_logs_project_created_idx;
        ALTER TABLE operator_logs RENAME TO operator_logs_v10_legacy;
        CREATE TABLE operator_logs (
            log_id INTEGER PRIMARY KEY,
            project_id TEXT NOT NULL,
            pueue_group TEXT NOT NULL,
            action TEXT NOT NULL CHECK (action IN (
                'pause', 'resume', 'halt', 'disable', 'remove', 'cancel'
            )),
            details_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        INSERT INTO operator_logs (
            log_id, project_id, pueue_group, action, details_json, created_at
        )
        SELECT log_id, project_id, pueue_group, action, details_json, created_at
        FROM operator_logs_v10_legacy;
        DROP TABLE operator_logs_v10_legacy;
        CREATE INDEX operator_logs_project_created_idx
            ON operator_logs(project_id, created_at, log_id);
        PRAGMA user_version = 11;
        "#,
        )
        .map_err(database_error("apply SQLite v11 operator log migration"))
}

fn migrate_task_observations_to_v12(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_task_observations: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'task_observations'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check task observations for migration"))?;
    if has_task_observations {
        let has_first_observed_at: bool = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_table_info('task_observations')
                     WHERE name = 'first_observed_at'
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(database_error("check task observation anchor for migration"))?;
        if !has_first_observed_at {
            transaction
                .execute_batch(
                    "ALTER TABLE task_observations
                         ADD COLUMN first_observed_at INTEGER NOT NULL DEFAULT 0;
                     UPDATE task_observations
                        SET first_observed_at = observed_at;",
                )
                .map_err(database_error("add task observation anchor"))?;
        }
    }
    transaction
        .execute_batch("PRAGMA user_version = 12;")
        .map_err(database_error("set SQLite v12 task observation migration"))
}

fn migrate_interventions_to_v6(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    transaction
        .execute_batch(
            r#"
        CREATE TABLE interventions (
            intervention_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
            insertion_sequence INTEGER NOT NULL,
            message TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'reserved', 'applied')),
            created_at INTEGER NOT NULL,
            reserved_at INTEGER,
            applied_at INTEGER,
            agent_run_id INTEGER,
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            lease_expires_at INTEGER,
            reservation_token TEXT,
            FOREIGN KEY (project_id, agent_run_id)
                REFERENCES agent_runs(project_id, run_id) ON DELETE SET NULL,
            CHECK (
                (status = 'pending' AND reserved_at IS NULL AND applied_at IS NULL AND agent_run_id IS NULL AND lease_expires_at IS NULL AND reservation_token IS NULL)
                OR (status = 'reserved' AND reserved_at IS NOT NULL AND applied_at IS NULL AND lease_expires_at IS NOT NULL AND reservation_token IS NOT NULL)
                OR (status = 'applied' AND reserved_at IS NOT NULL AND applied_at IS NOT NULL AND agent_run_id IS NOT NULL)
            )
        );
        CREATE UNIQUE INDEX interventions_project_sequence_idx
            ON interventions(project_id, insertion_sequence);
        CREATE INDEX interventions_project_status_created_idx
            ON interventions(project_id, status, insertion_sequence, intervention_id);
        CREATE INDEX interventions_reservation_lease_idx
            ON interventions(status, lease_expires_at, reservation_token);
        PRAGMA user_version = 6;
        "#,
        )
        .map_err(database_error("apply SQLite v6 migration"))
}

fn migrate_submissions_to_v7(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    let has_submissions: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'submissions'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check submission table for migration"))?;
    if !has_submissions {
        transaction
            .execute_batch(
                r#"
            CREATE TABLE submissions (
                submission_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                argv_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                pueue_task_id INTEGER,
                task_signature TEXT,
                status TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'experiment',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                origin_agent_run_id INTEGER,
                FOREIGN KEY (project_id, origin_agent_run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE RESTRICT
            );
            "#,
            )
            .map_err(database_error(
                "create submission table for SQLite v7 migration",
            ))?;
    } else {
        let kind = if submission_column_exists(transaction, "kind")? {
            "kind"
        } else {
            "'experiment'"
        };
        let metadata = if submission_column_exists(transaction, "metadata_json")? {
            "metadata_json"
        } else {
            "'{}'"
        };
        let origin = if submission_column_exists(transaction, "origin_agent_run_id")? {
            "CASE WHEN EXISTS (
                 SELECT 1 FROM agent_runs
                 WHERE agent_runs.project_id = submissions_v7_legacy.project_id
                   AND agent_runs.run_id = submissions_v7_legacy.origin_agent_run_id
             ) THEN origin_agent_run_id ELSE NULL END"
        } else {
            "NULL"
        };
        transaction
            .execute_batch(
                r#"
            DROP INDEX IF EXISTS submissions_project_status_idx;
            DROP INDEX IF EXISTS submissions_project_kind_status_idx;
            DROP INDEX IF EXISTS submissions_project_origin_agent_run_idx;
            ALTER TABLE submissions RENAME TO submissions_v7_legacy;
            CREATE TABLE submissions (
                submission_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                argv_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                pueue_task_id INTEGER,
                task_signature TEXT,
                status TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'experiment',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                origin_agent_run_id INTEGER,
                FOREIGN KEY (project_id, origin_agent_run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE RESTRICT
            );
            "#,
            )
            .map_err(database_error(
                "rebuild submission table for SQLite v7 migration",
            ))?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO submissions (
                        submission_id, project_id, argv_json, created_at,
                        pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
                     ) SELECT submission_id, project_id, argv_json, created_at,
                        pueue_task_id, task_signature, status, {kind}, {metadata}, {origin}
                     FROM submissions_v7_legacy"
                ),
                [],
            )
            .map_err(database_error("copy submissions into SQLite v7 schema"))?;
        transaction
            .execute_batch("DROP TABLE submissions_v7_legacy;")
            .map_err(database_error(
                "remove legacy submission table after SQLite v7 migration",
            ))?;
    }
    transaction
        .execute_batch("PRAGMA user_version = 7;")
        .map_err(database_error("finish SQLite v7 migration"))?;
    ensure_submission_indexes(transaction)
}

fn submissions_have_composite_origin_foreign_key(
    connection: &Connection,
) -> Result<bool, AppError> {
    connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1
                 FROM pragma_foreign_key_list('submissions') AS project_fk
                 JOIN pragma_foreign_key_list('submissions') AS origin_fk
                   ON project_fk.id = origin_fk.id
                 WHERE project_fk.seq = 0
                   AND project_fk.\"table\" = 'agent_runs'
                   AND project_fk.\"from\" = 'project_id'
                   AND project_fk.\"to\" = 'project_id'
                   AND project_fk.on_delete = 'RESTRICT'
                   AND origin_fk.seq = 1
                   AND origin_fk.\"table\" = 'agent_runs'
                   AND origin_fk.\"from\" = 'origin_agent_run_id'
                   AND origin_fk.\"to\" = 'run_id'
                   AND origin_fk.on_delete = 'RESTRICT'
                   AND 2 = (
                       SELECT COUNT(*)
                       FROM pragma_foreign_key_list('submissions') AS fk_part
                       WHERE fk_part.id = project_fk.id
                   )
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error(
            "check composite submission origin foreign key",
        ))
}

fn submission_column_exists(
    transaction: &rusqlite::Transaction<'_>,
    column: &str,
) -> Result<bool, AppError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('submissions')
                 WHERE name = ?1
             )",
            [column],
            |row| row.get(0),
        )
        .map_err(database_error("check submission column for migration"))
}

fn ensure_submission_indexes(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    transaction
        .execute_batch(
            r#"
        CREATE INDEX IF NOT EXISTS submissions_project_status_idx
            ON submissions(project_id, status, created_at);
        CREATE INDEX IF NOT EXISTS submissions_project_kind_status_idx
            ON submissions(project_id, kind, status, created_at);
        CREATE INDEX IF NOT EXISTS submissions_project_origin_agent_run_idx
            ON submissions(project_id, origin_agent_run_id, created_at, submission_id);
        "#,
        )
        .map_err(database_error("ensure submission indexes"))
}

fn ensure_intervention_insertion_sequence(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_interventions: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'interventions'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check intervention table for migration"))?;
    if !has_interventions {
        return Ok(());
    }

    let has_sequence: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('interventions')
                 WHERE name = 'insertion_sequence'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check intervention insertion sequence"))?;
    if !has_sequence {
        transaction
            .execute(
                "ALTER TABLE interventions
                 ADD COLUMN insertion_sequence INTEGER NOT NULL DEFAULT 0",
                [],
            )
            .map_err(database_error("add intervention insertion sequence"))?;
        transaction
            .execute(
                "UPDATE interventions AS current
                 SET insertion_sequence = (
                     SELECT COUNT(*)
                     FROM interventions AS prior
                     WHERE prior.project_id = current.project_id
                       AND (
                           prior.created_at < current.created_at
                           OR (prior.created_at = current.created_at AND prior.rowid <= current.rowid)
                       )
                 )",
                [],
            )
            .map_err(database_error("backfill intervention insertion sequence"))?;
    }

    ensure_index_definition(
        transaction,
        "interventions_project_sequence_idx",
        INTERVENTION_SEQUENCE_INDEX_SQL,
    )?;
    ensure_index_definition(
        transaction,
        "interventions_project_status_created_idx",
        INTERVENTION_STATUS_INDEX_SQL,
    )
}

fn ensure_agent_run_launch_gate(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    let has_agent_runs: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'agent_runs'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check agent run table for launch gate"))?;
    if !has_agent_runs {
        return Ok(());
    }

    let has_gate_state: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('agent_runs')
                 WHERE name = 'launch_gate_state'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check agent run launch gate state"))?;
    if !has_gate_state {
        transaction
            .execute(
                "ALTER TABLE agent_runs
                 ADD COLUMN launch_gate_state TEXT NOT NULL DEFAULT 'released'
                 CHECK (launch_gate_state IN ('pending', 'release_requested', 'released', 'failed'))",
                [],
            )
            .map_err(database_error("add agent run launch gate state"))?;
    }
    transaction
        .execute(
            "UPDATE agent_runs
             SET launch_gate_state = 'pending'
             WHERE status = 'starting' AND launch_gate_state = 'released'",
            [],
        )
        .map_err(database_error("initialize starting agent launch gates"))?;
    Ok(())
}

fn ensure_index_definition(
    transaction: &rusqlite::Transaction<'_>,
    name: &str,
    expected_sql: &str,
) -> Result<(), AppError> {
    let existing_sql = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
            [name],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(database_error("read intervention index definition"))?
        .flatten();
    if existing_sql
        .as_deref()
        .is_some_and(|sql| compact_sql(sql) == compact_sql(expected_sql))
    {
        return Ok(());
    }

    transaction
        .execute(&format!("DROP INDEX IF EXISTS {name}"), [])
        .map_err(database_error("replace stale intervention index"))?;
    transaction
        .execute_batch(expected_sql)
        .map_err(database_error("create intervention FIFO index"))
}

fn compact_sql(sql: &str) -> String {
    compact_sql_exact(sql).to_ascii_lowercase()
}

fn compact_sql_exact(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .to_owned()
}

fn migrate_campaign_schema_to_v16(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    if !campaign_schema_v16_is_canonical(transaction).unwrap_or(false) {
        for sql in [
            CAMPAIGNS_V16_TABLE_SQL,
            CAMPAIGNS_ONE_LIVE_PROJECT_INDEX_SQL,
            PROPOSALS_V16_TABLE_SQL,
            EXPERIMENTS_V16_TABLE_SQL,
            BUDGET_RESERVATIONS_V16_TABLE_SQL,
            CAMPAIGNS_STATE_NEXT_ELIGIBLE_INDEX_SQL,
            PROPOSALS_CAMPAIGN_STATUS_CREATED_INDEX_SQL,
            EXPERIMENTS_CAMPAIGN_STATUS_CREATED_INDEX_SQL,
            EXPERIMENTS_PUEUE_TASK_LOOKUP_INDEX_SQL,
            BUDGET_RESERVATIONS_CAMPAIGN_DIMENSION_WINDOW_INDEX_SQL,
        ] {
            transaction
                .execute_batch(sql)
                .map_err(database_error("apply SQLite v16 campaign migration"))?;
        }
    }
    verify_campaign_schema_v16(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 16;")
        .map_err(database_error("set SQLite v16 schema version"))
}

fn migrate_campaign_event_lineage_to_v17(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_campaign_id: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('events') WHERE name = 'campaign_id'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check event campaign lineage column"))?;
    if !has_campaign_id {
        transaction
            .execute_batch(
                "ALTER TABLE events ADD COLUMN campaign_id TEXT
                     REFERENCES campaigns(campaign_id) ON DELETE CASCADE;",
            )
            .map_err(database_error("add event campaign lineage column"))?;
    }
    let has_experiment_id: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('events') WHERE name = 'experiment_id'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check event experiment lineage column"))?;
    if !has_experiment_id {
        transaction
            .execute_batch(
                "ALTER TABLE events ADD COLUMN experiment_id TEXT
                     REFERENCES experiments(experiment_id) ON DELETE SET NULL;",
            )
            .map_err(database_error("add event experiment lineage column"))?;
    }
    ensure_index_definition(
        transaction,
        "events_campaign_status_not_before_idx",
        EVENTS_CAMPAIGN_STATUS_NOT_BEFORE_INDEX_SQL,
    )?;

    transaction
        .execute(
            "UPDATE submissions
             SET status = 'unreconciled'
             WHERE status = 'accepted'
               AND task_signature LIKE 'provisional-submit:v1:%'
               AND EXISTS (
                   SELECT 1 FROM experiments
                   WHERE experiments.submission_id = submissions.submission_id
               )",
            [],
        )
        .map_err(database_error("quarantine provisional managed submissions"))?;
    transaction
        .execute(
            "UPDATE experiments
             SET status = 'unreconciled',
                 failure_code = 'legacy_provisional_task_identity',
                 failure_fingerprint = NULL
             WHERE task_signature LIKE 'provisional-submit:v1:%'
               AND status != 'unreconciled'",
            [],
        )
        .map_err(database_error("quarantine provisional managed experiments"))?;

    verify_campaign_event_lineage_v17(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 17;")
        .map_err(database_error("set SQLite v17 schema version"))
}

fn verify_campaign_event_lineage_v17(connection: &Connection) -> Result<(), AppError> {
    let columns = connection
        .prepare("SELECT name, type FROM pragma_table_info('events')")
        .and_then(|mut statement| {
            statement
                .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(database_error("verify SQLite v17 event lineage columns"))?;
    if !columns.iter().any(|column| column == &("campaign_id".to_owned(), "TEXT".to_owned()))
        || !columns
            .iter()
            .any(|column| column == &("experiment_id".to_owned(), "TEXT".to_owned()))
    {
        return Err(AppError::Runtime {
            operation: "verify SQLite v17 event lineage schema",
        });
    }
    let index_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'events_campaign_status_not_before_idx'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("verify SQLite v17 event lineage index"))?;
    if !index_sql.as_deref().is_some_and(|sql| {
        compact_sql(sql) == compact_sql(EVENTS_CAMPAIGN_STATUS_NOT_BEFORE_INDEX_SQL)
    }) {
        return Err(AppError::Runtime {
            operation: "verify SQLite v17 event lineage schema",
        });
    }
    if !campaign_foreign_keys_match(
        connection,
        "events",
        &[
            ("projects", "project_id", "project_id", "CASCADE"),
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            ("experiments", "experiment_id", "experiment_id", "SET NULL"),
        ],
    )
    .unwrap_or(false)
    {
        return Err(AppError::Runtime {
            operation: "verify SQLite v17 event lineage schema",
        });
    }
    let provisional_accepted: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM submissions
                 WHERE status = 'accepted'
                   AND task_signature LIKE 'provisional-submit:v1:%'
                   AND EXISTS (
                       SELECT 1 FROM experiments
                       WHERE experiments.submission_id = submissions.submission_id
                   )
                 UNION ALL
                 SELECT 1 FROM experiments
                 WHERE status != 'unreconciled'
                   AND task_signature LIKE 'provisional-submit:v1:%'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("verify provisional managed identity quarantine"))?;
    if provisional_accepted {
        return Err(AppError::Runtime {
            operation: "verify SQLite v17 managed task identity quarantine",
        });
    }
    Ok(())
}

fn verify_campaign_schema_v16(connection: &Connection) -> Result<(), AppError> {
    if campaign_schema_v16_is_canonical(connection).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v16 campaign schema",
        })
    }
}

fn campaign_schema_v16_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    for (table, expected_sql) in [
        ("campaigns", CAMPAIGNS_V16_TABLE_SQL),
        ("proposals", PROPOSALS_V16_TABLE_SQL),
        ("experiments", EXPERIMENTS_V16_TABLE_SQL),
        ("budget_reservations", BUDGET_RESERVATIONS_V16_TABLE_SQL),
    ] {
        let actual_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()?;
        if !actual_sql
            .as_deref()
            .is_some_and(|sql| compact_sql_exact(sql) == compact_sql_exact(expected_sql))
        {
            return Ok(false);
        }
    }

    if !campaign_table_info_matches(
        connection,
        "campaigns",
        &[
            ("campaign_id", "TEXT", 0, 1),
            ("project_id", "TEXT", 1, 0),
            ("objective_text", "TEXT", 1, 0),
            ("objective_digest", "TEXT", 1, 0),
            ("initial_argv_json", "TEXT", 1, 0),
            ("state", "TEXT", 1, 0),
            ("state_reason", "TEXT", 0, 0),
            ("baseline_experiment_id", "TEXT", 0, 0),
            ("next_eligible_at", "INTEGER", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
        ],
    )? || !campaign_table_info_matches(
        connection,
        "proposals",
        &[
            ("proposal_id", "TEXT", 0, 1),
            ("campaign_id", "TEXT", 1, 0),
            ("kind", "TEXT", 1, 0),
            ("status", "TEXT", 1, 0),
            ("hypothesis", "TEXT", 1, 0),
            ("source_experiment_id", "TEXT", 0, 0),
            ("argv_json", "TEXT", 1, 0),
            ("working_directory", "TEXT", 1, 0),
            ("expected_evidence_json", "TEXT", 1, 0),
            ("canonical_digest", "TEXT", 1, 0),
            ("reject_reason", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
        ],
    )? || !campaign_table_info_matches(
        connection,
        "experiments",
        &[
            ("experiment_id", "TEXT", 0, 1),
            ("campaign_id", "TEXT", 1, 0),
            ("proposal_id", "TEXT", 1, 0),
            ("submission_id", "TEXT", 1, 0),
            ("parent_experiment_id", "TEXT", 0, 0),
            ("attempt", "INTEGER", 1, 0),
            ("status", "TEXT", 1, 0),
            ("pueue_task_id", "INTEGER", 0, 0),
            ("task_signature", "TEXT", 0, 0),
            ("failure_code", "TEXT", 0, 0),
            ("failure_fingerprint", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
            ("finished_at", "INTEGER", 0, 0),
        ],
    )? || !campaign_table_info_matches(
        connection,
        "budget_reservations",
        &[
            ("reservation_id", "TEXT", 0, 1),
            ("campaign_id", "TEXT", 1, 0),
            ("experiment_id", "TEXT", 0, 0),
            ("dimension", "TEXT", 1, 0),
            ("subject_key", "TEXT", 1, 0),
            ("status", "TEXT", 1, 0),
            ("window_started_at", "INTEGER", 1, 0),
            ("window_ends_at", "INTEGER", 1, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
        ],
    )? {
        return Ok(false);
    }

    if !campaign_foreign_keys_match(
        connection,
        "campaigns",
        &[
            ("projects", "project_id", "project_id", "CASCADE"),
            (
                "experiments",
                "baseline_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ],
    )? || !campaign_foreign_keys_match(
        connection,
        "proposals",
        &[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            (
                "experiments",
                "source_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ],
    )? || !campaign_foreign_keys_match(
        connection,
        "experiments",
        &[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            ("proposals", "proposal_id", "proposal_id", "RESTRICT"),
            (
                "submissions",
                "submission_id",
                "submission_id",
                "RESTRICT",
            ),
            (
                "experiments",
                "parent_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ],
    )? || !campaign_foreign_keys_match(
        connection,
        "budget_reservations",
        &[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            (
                "experiments",
                "experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ],
    )? {
        return Ok(false);
    }

    for (name, expected_sql) in [
        (
            "campaigns_one_live_project_idx",
            CAMPAIGNS_ONE_LIVE_PROJECT_INDEX_SQL,
        ),
        (
            "campaigns_state_next_eligible_idx",
            CAMPAIGNS_STATE_NEXT_ELIGIBLE_INDEX_SQL,
        ),
        (
            "proposals_campaign_status_created_idx",
            PROPOSALS_CAMPAIGN_STATUS_CREATED_INDEX_SQL,
        ),
        (
            "experiments_campaign_status_created_idx",
            EXPERIMENTS_CAMPAIGN_STATUS_CREATED_INDEX_SQL,
        ),
        (
            "experiments_pueue_task_lookup_idx",
            EXPERIMENTS_PUEUE_TASK_LOOKUP_INDEX_SQL,
        ),
        (
            "budget_reservations_campaign_dimension_window_idx",
            BUDGET_RESERVATIONS_CAMPAIGN_DIMENSION_WINDOW_INDEX_SQL,
        ),
    ] {
        let actual_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()?;
        if !actual_sql
            .as_deref()
            .is_some_and(|sql| compact_sql_exact(sql) == compact_sql_exact(expected_sql))
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn campaign_table_info_matches(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, i64, i64)],
) -> rusqlite::Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(actual
        == expected
            .iter()
            .map(|(name, declared_type, not_null, primary_key)| {
                (
                    (*name).to_owned(),
                    (*declared_type).to_owned(),
                    *not_null,
                    *primary_key,
                )
            })
            .collect::<Vec<_>>())
}

fn campaign_foreign_keys_match(
    connection: &Connection,
    table: &str,
    expected: &[(&str, &str, &str, &str)],
) -> rusqlite::Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA foreign_key_list({table})"))?;
    let mut actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|(target_table, from, to, on_delete)| {
            (
                (*target_table).to_owned(),
                (*from).to_owned(),
                (*to).to_owned(),
                "NO ACTION".to_owned(),
                (*on_delete).to_owned(),
                "NONE".to_owned(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    Ok(actual == expected)
}

fn migrate_termination_requests_to_v5(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_termination_requests: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'termination_requests'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error(
            "check termination request table for migration",
        ))?;
    if !has_termination_requests {
        transaction
            .execute_batch("PRAGMA user_version = 5;")
            .map_err(database_error("finish legacy SQLite migration"))?;
        return Ok(());
    }

    transaction
        .execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS incidents_project_incident_migration_idx
             ON incidents(project_id, incident_id);",
        )
        .map_err(database_error("prepare incident key for migration"))?;

    transaction
        .execute_batch(
            r#"
        CREATE TABLE termination_requests_v5 (
            request_id INTEGER PRIMARY KEY,
            incident_id INTEGER NOT NULL,
            project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
            task_signature TEXT NOT NULL,
            reason TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN (
                'requested', 'dispatching', 'sent', 'confirmed', 'timed_out', 'failed'
            )),
            requested_at INTEGER NOT NULL,
            dispatch_lease_until INTEGER,
            grace_until INTEGER,
            confirmed_at INTEGER,
            last_error TEXT,
            UNIQUE(project_id, incident_id, task_signature),
            FOREIGN KEY(project_id, incident_id)
                REFERENCES incidents(project_id, incident_id) ON DELETE CASCADE
        );

        INSERT INTO termination_requests_v5 (
            request_id, incident_id, project_id, task_signature, reason, status,
            requested_at, dispatch_lease_until, grace_until, confirmed_at, last_error
        )
        SELECT request_id, incident_id, project_id, task_signature, reason, status,
               requested_at, NULL, grace_until, confirmed_at, last_error
        FROM termination_requests;

        DROP TABLE termination_requests;
        ALTER TABLE termination_requests_v5 RENAME TO termination_requests;
        CREATE INDEX termination_requests_project_status_idx
            ON termination_requests(project_id, status, requested_at);

        PRAGMA user_version = 5;
        "#,
        )
        .map_err(database_error("apply SQLite v5 migration"))?;
    Ok(())
}

fn ensure_agent_run_event_project_id(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_table: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'table' AND name = 'agent_run_events'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check agent run event table for migration"))?;
    if !has_table {
        return Ok(());
    }
    let has_project_id: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('agent_run_events')
                 WHERE name = 'project_id'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check agent run event project column"))?;
    if has_project_id {
        return Ok(());
    }

    transaction
        .execute_batch(
            "ALTER TABLE agent_run_events ADD COLUMN project_id TEXT;
             UPDATE agent_run_events
             SET project_id = (
                 SELECT project_id FROM agent_runs
                 WHERE agent_runs.run_id = agent_run_events.run_id
             );",
        )
        .map_err(database_error("add agent run event project column"))?;
    Ok(())
}

fn ensure_invariant_indexes(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    transaction
        .execute_batch(ACTIVE_AGENT_INDEX_SQL)
        .map_err(database_error("ensure SQLite invariant indexes"))
        .and_then(|_| {
            transaction
                .execute_batch(OPERATOR_LOGS_SQL)
                .map_err(database_error("ensure SQLite operator log table"))
        })
}
