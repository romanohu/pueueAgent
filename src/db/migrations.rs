use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use crate::{environment::MAX_PRIVATE_TEMP_RUN_ID, AppError};

use super::database_error;

pub const LATEST_SCHEMA_VERSION: i64 = 27;
const EVENTS_V23_KIND_LIST: &str =
    "'task_finished', 'task_failed', 'crash', 'stalled', 'deep_check', 'auto_killed', 'termination_failed', 'operator_wake', 'campaign_decision', 'health_diagnosis'";
const EVENTS_V26_KIND_LIST: &str =
    "'task_finished', 'task_failed', 'crash', 'stalled', 'deep_check', 'auto_killed', 'termination_failed', 'operator_wake', 'campaign_decision', 'health_diagnosis', 'code_change'";
const EVENTS_V18_KIND_LIST: &str =
    "'task_finished', 'task_failed', 'crash', 'stalled', 'deep_check', 'auto_killed', 'termination_failed', 'operator_wake', 'campaign_decision'";
const EVENTS_V17_KIND_LIST: &str =
    "'task_finished', 'task_failed', 'crash', 'stalled', 'deep_check', 'auto_killed', 'termination_failed', 'operator_wake'";
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
const DECISION_CYCLES_V18_TABLE_SQL: &str = r#"
CREATE TABLE decision_cycles (
  cycle_id TEXT PRIMARY KEY,
  campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
  source_experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
  state TEXT NOT NULL CHECK (state IN ('pending','analyzing','waiting','completed','degraded')),
  next_wake_at INTEGER,
  consecutive_failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failed_attempts >= 0),
  last_decision_kind TEXT,
  last_failure_code TEXT,
  last_failure_summary TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(campaign_id, source_experiment_id)
);
"#;
const DECISION_CYCLES_V19_TABLE_SQL: &str = r#"
CREATE TABLE decision_cycles (
  cycle_id TEXT PRIMARY KEY,
  campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
  source_experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
  state TEXT NOT NULL CHECK (state IN ('pending','analyzing','waiting','completed','degraded')),
  next_wake_at INTEGER,
  consecutive_failed_attempts INTEGER NOT NULL DEFAULT 0 CHECK (consecutive_failed_attempts >= 0),
  last_decision_kind TEXT,
  last_failure_code TEXT,
  last_failure_summary TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  source_terminal_at INTEGER NOT NULL DEFAULT 1 CHECK(source_terminal_at > 0),
  UNIQUE(campaign_id, source_experiment_id)
);
"#;
const DECISION_ATTEMPTS_V18_TABLE_SQL: &str = r#"
CREATE TABLE decision_attempts (
  cycle_id TEXT NOT NULL REFERENCES decision_cycles(cycle_id),
  attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
  state TEXT NOT NULL CHECK (state IN ('reserved','evidence_ready','running','decided','failed')),
  context_schema_version INTEGER,
  context_json TEXT,
  context_digest TEXT,
  agent_run_id INTEGER REFERENCES agent_runs(run_id),
  decision_json TEXT,
  decision_digest TEXT,
  decision_kind TEXT,
  failure_code TEXT,
  failure_summary TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  finished_at INTEGER,
  PRIMARY KEY(cycle_id, attempt_number),
  UNIQUE(agent_run_id)
);
"#;
const DECISION_CYCLES_DUE_INDEX_SQL: &str = "CREATE INDEX decision_cycles_state_wake_updated_idx
    ON decision_cycles(state, next_wake_at, updated_at);";
const DECISION_CYCLES_CAMPAIGN_INDEX_SQL: &str = "CREATE INDEX decision_cycles_campaign_state_updated_idx
    ON decision_cycles(campaign_id, state, updated_at);";
const DECISION_CYCLES_STATE_SOURCE_INDEX_SQL: &str =
    "CREATE INDEX decision_cycles_state_source_order_idx
    ON decision_cycles(state, source_terminal_at, source_experiment_id, cycle_id);";
const DECISION_CYCLES_STATE_WAKE_SOURCE_INDEX_SQL: &str =
    "CREATE INDEX decision_cycles_state_wake_source_order_idx
    ON decision_cycles(state, next_wake_at, source_terminal_at, source_experiment_id, cycle_id);";
const DECISION_CYCLES_CAMPAIGN_STATE_SOURCE_INDEX_SQL: &str =
    "CREATE INDEX decision_cycles_campaign_state_source_order_idx
    ON decision_cycles(campaign_id, state, source_terminal_at, source_experiment_id, cycle_id);";
const DECISION_CYCLES_CAMPAIGN_WAKE_SOURCE_INDEX_SQL: &str =
    "CREATE INDEX decision_cycles_campaign_state_wake_source_order_idx
    ON decision_cycles(campaign_id, state, next_wake_at, source_terminal_at, source_experiment_id, cycle_id);";
const DECISION_ATTEMPTS_STATE_INDEX_SQL: &str = "CREATE INDEX decision_attempts_state_created_idx
    ON decision_attempts(state, created_at);";
const DECISION_ATTEMPTS_UNBOUND_STATE_INDEX_SQL: &str =
    "CREATE INDEX decision_attempts_unbound_state_created_idx
    ON decision_attempts(state, created_at, cycle_id, attempt_number)
    WHERE agent_run_id IS NULL AND state IN ('reserved','evidence_ready');";
const AGENT_RUN_EVENTS_PROJECT_EVENT_RUN_INDEX_SQL: &str =
    "CREATE INDEX agent_run_events_project_event_run_idx
    ON agent_run_events(project_id, event_id, run_id DESC);";
const RUNNING_HEALTH_V22_TABLE_SQL: &str = r#"
    CREATE TABLE running_health (
        experiment_id     TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                          ON DELETE CASCADE,
        campaign_id       TEXT NOT NULL,
        project_id        TEXT NOT NULL,
        pueue_task_id     INTEGER NOT NULL,
        state             TEXT NOT NULL CHECK (state IN (
                              'healthy','suspicious','diagnosing','action_pending')),
        observation_count INTEGER NOT NULL DEFAULT 0,
        last_observed_at  INTEGER NOT NULL,
        signal_summary_json TEXT NOT NULL DEFAULT '[]',
        diagnosis_json    TEXT,
        created_at        INTEGER NOT NULL,
        updated_at        INTEGER NOT NULL
    );
    CREATE INDEX running_health_due_idx ON running_health (last_observed_at);
    CREATE INDEX running_health_campaign_state_idx
        ON running_health (campaign_id, state);
"#;
const EXPERIMENTS_V22_RESUME_COLUMN_SQL: &str =
    "ALTER TABLE experiments ADD COLUMN resume_of_experiment_id TEXT REFERENCES experiments(experiment_id);";
const EXPERIMENTS_V22_CHECKPOINT_NOTE_COLUMN_SQL: &str =
    "ALTER TABLE experiments ADD COLUMN checkpoint_note TEXT;";
const CAMPAIGNS_V24_OBJECTIVE_METRIC_COLUMN_SQL: &str =
    "ALTER TABLE campaigns ADD COLUMN objective_metric_json TEXT;";
const CAMPAIGNS_V24_CURRENT_BEST_COLUMN_SQL: &str =
    "ALTER TABLE campaigns ADD COLUMN current_best_experiment_id TEXT;";
const CAMPAIGNS_V24_PLATEAU_COUNT_COLUMN_SQL: &str =
    "ALTER TABLE campaigns ADD COLUMN plateau_count INTEGER NOT NULL DEFAULT 0;";
const EXPERIMENT_METRICS_V24_TABLE_SQL: &str = r#"
    CREATE TABLE experiment_metrics (
        experiment_id        TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                             ON DELETE CASCADE,
        source               TEXT NOT NULL CHECK (source IN ('manifest')),
        primary_metric_name  TEXT,
        primary_metric_value REAL,
        metrics_json         TEXT NOT NULL DEFAULT '{}',
        artifact_defect      TEXT,
        created_at           INTEGER NOT NULL,
        updated_at           INTEGER NOT NULL
    );
"#;
const EXPERIMENT_METRICS_V25_EVALUATED_AT_COLUMN_SQL: &str =
    "ALTER TABLE experiment_metrics ADD COLUMN evaluated_at TEXT NULL;";
const EXPERIMENT_METRICS_V25_TABLE_SQL: &str = r#"
    CREATE TABLE experiment_metrics (
        experiment_id        TEXT PRIMARY KEY REFERENCES experiments(experiment_id)
                             ON DELETE CASCADE,
        source               TEXT NOT NULL CHECK (source IN ('manifest')),
        primary_metric_name  TEXT,
        primary_metric_value REAL,
        metrics_json         TEXT NOT NULL DEFAULT '{}',
        artifact_defect      TEXT,
        created_at           INTEGER NOT NULL,
        updated_at           INTEGER NOT NULL,
        evaluated_at         TEXT NULL
    );
"#;
const CODE_CHANGE_RUNS_V26_TABLE_SQL: &str = r#"
    CREATE TABLE code_change_runs (
        code_change_run_id TEXT PRIMARY KEY,
        proposal_id TEXT NOT NULL UNIQUE REFERENCES proposals(proposal_id),
        campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
        state TEXT NOT NULL CHECK (state IN (
            'reserved', 'preparing_worktree', 'editing', 'checking', 'committing',
            'candidate_ready', 'experiment_submitted', 'evaluated',
            'cleanup_pending', 'completed', 'rejected', 'recovery_required'
        )),
        base_sha TEXT NOT NULL CHECK (
            length(base_sha) IN (40, 64) AND base_sha NOT GLOB '*[^0-9a-f]*'
        ),
        candidate_sha TEXT CHECK (
            candidate_sha IS NULL OR (
                length(candidate_sha) IN (40, 64) AND candidate_sha NOT GLOB '*[^0-9a-f]*'
            )
        ),
        candidate_ref TEXT NOT NULL,
        best_ref TEXT NOT NULL,
        worktree_id TEXT NOT NULL UNIQUE,
        worktree_relative_path TEXT NOT NULL UNIQUE,
        editor_session_id TEXT,
        editor_attempts INTEGER NOT NULL DEFAULT 0 CHECK (editor_attempts BETWEEN 0 AND 2),
        diff_digest TEXT,
        changed_file_count INTEGER CHECK (changed_file_count IS NULL OR changed_file_count >= 0),
        diff_bytes INTEGER CHECK (diff_bytes IS NULL OR diff_bytes >= 0),
        experiment_id TEXT UNIQUE REFERENCES experiments(experiment_id),
        rejection_code TEXT,
        rejection_summary TEXT,
        promotion_outcome TEXT,
        promotion_expected_best_experiment_id TEXT,
        promotion_expected_old_sha TEXT CHECK (
            promotion_expected_old_sha IS NULL OR (
                length(promotion_expected_old_sha) IN (40, 64)
                AND promotion_expected_old_sha NOT GLOB '*[^0-9a-f]*'
            )
        ),
        promotion_target_sha TEXT CHECK (
            promotion_target_sha IS NULL OR (
                length(promotion_target_sha) IN (40, 64)
                AND promotion_target_sha NOT GLOB '*[^0-9a-f]*'
            )
        ),
        cleanup_completed_at INTEGER,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL,
        CHECK (
            state IN ('reserved', 'preparing_worktree', 'editing', 'checking', 'committing')
            AND candidate_sha IS NULL
            OR state IN ('committing', 'candidate_ready', 'experiment_submitted', 'evaluated', 'cleanup_pending', 'completed')
            AND candidate_sha IS NOT NULL
            OR state IN ('rejected', 'recovery_required')
        ),
        CHECK (
            state NOT IN ('experiment_submitted', 'evaluated', 'cleanup_pending', 'completed')
            OR (experiment_id IS NOT NULL OR state = 'recovery_required')
        )
    );
"#;
const CODE_CHANGE_EDITOR_ATTEMPTS_V26_TABLE_SQL: &str = r#"
    CREATE TABLE code_change_editor_attempts (
        code_change_run_id TEXT NOT NULL REFERENCES code_change_runs(code_change_run_id),
        attempt INTEGER NOT NULL CHECK (attempt IN (1, 2)),
        agent_run_id INTEGER NOT NULL UNIQUE REFERENCES agent_runs(run_id),
        editor_session_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('reserved', 'running', 'ready', 'failed')),
        result_digest TEXT,
        failure_code TEXT,
        failure_summary TEXT,
        started_at INTEGER,
        finished_at INTEGER,
        PRIMARY KEY(code_change_run_id, attempt)
    );
"#;
const CODE_CHANGE_CHECKS_V26_TABLE_SQL: &str = r#"
    CREATE TABLE code_change_checks (
        code_change_run_id TEXT NOT NULL REFERENCES code_change_runs(code_change_run_id),
        attempt INTEGER NOT NULL,
        ordinal INTEGER NOT NULL,
        source TEXT NOT NULL CHECK (source IN ('supervisor', 'discovered', 'editor')),
        argv_json TEXT NOT NULL,
        working_directory TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('reserved', 'passed', 'failed', 'timed_out')),
        output_digest TEXT,
        summary TEXT,
        started_at INTEGER,
        finished_at INTEGER,
        PRIMARY KEY(code_change_run_id, attempt, ordinal)
    );
"#;
const CODE_CHANGE_ONE_LIVE_PER_CAMPAIGN_INDEX_SQL: &str =
    "CREATE UNIQUE INDEX code_change_one_live_per_campaign
     ON code_change_runs(campaign_id)
     WHERE state NOT IN ('completed', 'rejected');";
const CAMPAIGNS_V26_BASE_REVISION_COLUMN_SQL: &str =
    "ALTER TABLE campaigns ADD COLUMN base_revision_sha TEXT;";
const EXPERIMENTS_V26_RUN_COLUMN_SQL: &str =
    "ALTER TABLE experiments ADD COLUMN code_change_run_id TEXT REFERENCES code_change_runs(code_change_run_id);";
const EXPERIMENTS_V26_REVISION_COLUMN_SQL: &str =
    "ALTER TABLE experiments ADD COLUMN code_revision_sha TEXT;";
const CODE_CHANGE_V27_PROOF_COLUMNS: &[&str] = &[
    "state_root_identity",
    "worktrees_identity",
    "campaign_identity",
    "candidate_root_identity",
    "candidate_admin_identity",
    "candidate_common_identity",
    "candidate_admin_path",
    "candidate_common_path",
];

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
        verify_decision_schema_v21(connection)?;
        verify_running_health_schema_v22(connection)?;
        verify_evaluation_schema_v24(connection)?;
        verify_evaluation_schema_v25(connection)?;
        verify_event_kinds_v26(connection)?;
        verify_code_change_schema_v26(connection)?;
        verify_code_change_schema_v27(connection)?;
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
    if version <= 17 {
        migrate_decision_schema_to_v18(&transaction)?;
    } else if version == 18 {
        verify_decision_schema_v18(&transaction)?;
    }
    if version <= 18 {
        migrate_decision_schema_to_v19(&transaction)?;
    } else {
        verify_decision_schema_v19(&transaction)?;
    }
    if version <= 19 {
        migrate_decision_schema_to_v20(&transaction)?;
    } else {
        verify_decision_schema_v20(&transaction)?;
    }
    if version <= 20 {
        migrate_decision_schema_to_v21(&transaction)?;
    } else {
        verify_decision_schema_v21(&transaction)?;
    }
    if version <= 21 {
        migrate_running_health_schema_to_v22(&transaction)?;
    } else {
        verify_running_health_schema_v22(&transaction)?;
    }
    if version <= 22 {
        migrate_event_kinds_to_v23(&transaction)?;
    } else {
        verify_event_kinds_v23(&transaction)?;
    }
    if version <= 23 {
        migrate_evaluation_schema_to_v24(&transaction)?;
    } else {
        verify_evaluation_schema_v24(&transaction)?;
    }
    if version <= 24 {
        migrate_evaluation_marker_to_v25(&transaction)?;
    } else {
        verify_evaluation_schema_v25(&transaction)?;
    }
    if version <= 25 {
        migrate_code_change_schema_to_v26(&transaction)?;
    } else {
        verify_event_kinds_v26(&transaction)?;
        verify_code_change_schema_v26(&transaction)?;
    }
    if version <= 26 {
        migrate_code_change_schema_to_v27(&transaction)?;
    } else {
        verify_code_change_schema_v27(&transaction)?;
    }
    transaction
        .commit()
        .map_err(database_error("commit SQLite migration"))?;

    Ok(())
}

fn migrate_code_change_schema_to_v27(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    for column in CODE_CHANGE_V27_PROOF_COLUMNS {
        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('code_change_runs') WHERE name = ?1)",
                [*column],
                |row| row.get(0),
            )
            .map_err(database_error("inspect SQLite v27 code-change proof column"))?;
        if !exists {
            transaction
                .execute(
                    &format!("ALTER TABLE code_change_runs ADD COLUMN {column} TEXT"),
                    [],
                )
                .map_err(database_error("add SQLite v27 code-change proof column"))?;
        }
    }
    verify_code_change_schema_v27(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 27;")
        .map_err(database_error("set SQLite v27 schema version"))
}

fn verify_code_change_schema_v27(connection: &Connection) -> Result<(), AppError> {
    for column in CODE_CHANGE_V27_PROOF_COLUMNS {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('code_change_runs') WHERE name = ?1)",
                [*column],
                |row| row.get(0),
            )
            .map_err(database_error("inspect SQLite v27 code-change proof column"))?;
        if !exists {
            return Err(AppError::Runtime {
                operation: "verify SQLite v27 code-change schema",
            });
        }
    }
    Ok(())
}

fn migrate_event_kinds_to_v23(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    let event_sql: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite events schema for v23 migration"))?;
    if !event_kind_list_matches(&event_sql, EVENTS_V23_KIND_LIST) {
        if !event_kind_list_matches(&event_sql, EVENTS_V18_KIND_LIST)
            && !event_kind_list_matches(&event_sql, EVENTS_V26_KIND_LIST)
        {
            return Err(AppError::Runtime {
                operation: "verify SQLite event kinds before v23 migration",
            });
        }
        let old_kind_list = event_kind_list(&event_sql).ok_or(AppError::Runtime {
            operation: "read SQLite events kind list for v23 migration",
        })?;
        transaction
            .execute_batch("PRAGMA writable_schema = ON;")
            .map_err(database_error("enable SQLite writable schema for v23 event migration"))?;
        let replaced = transaction.execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'
                AND sql LIKE '%' || ?1 || '%'",
            params![
                old_kind_list,
                EVENTS_V23_KIND_LIST
            ],
        );
        let writable_schema_disabled = transaction
            .execute_batch("PRAGMA writable_schema = OFF;")
            .map_err(database_error(
                "disable SQLite writable schema after v23 event migration",
            ));
        let replaced = replaced.map_err(database_error("add health diagnosis event kind"))?;
        writable_schema_disabled?;
        if replaced != 1 {
            return Err(AppError::Runtime {
                operation: "migrate exactly one SQLite events kind list to v23",
            });
        }
    }
    verify_event_kinds_v23(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 23;")
        .map_err(database_error("set SQLite v23 schema version"))
}

fn verify_event_kinds_v23(connection: &Connection) -> Result<(), AppError> {
    let event_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read SQLite events schema for v23 verification"))?;
    if event_sql
        .as_deref()
        .is_some_and(|sql| {
            event_kind_list_matches(sql, EVENTS_V23_KIND_LIST)
                || event_kind_list_matches(sql, EVENTS_V26_KIND_LIST)
        })
    {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v23 event kinds",
        })
    }
}

fn migrate_event_kinds_to_v26(transaction: &rusqlite::Transaction<'_>) -> Result<(), AppError> {
    let event_sql: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite events schema for v26 migration"))?;
    if !event_kind_list_matches(&event_sql, EVENTS_V26_KIND_LIST) {
        if !event_kind_list_matches(&event_sql, EVENTS_V23_KIND_LIST) {
            return Err(AppError::Runtime {
                operation: "verify SQLite event kinds before v26 migration",
            });
        }
        let old_kind_list = event_kind_list(&event_sql).ok_or(AppError::Runtime {
            operation: "read SQLite event kind list for v26 migration",
        })?;
        transaction
            .execute_batch("PRAGMA writable_schema = ON;")
            .map_err(database_error("enable SQLite writable schema for v26 event migration"))?;
        let replaced = transaction.execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'
                AND sql LIKE '%' || ?1 || '%'",
            params![old_kind_list, EVENTS_V26_KIND_LIST],
        );
        let writable_schema_disabled = transaction
            .execute_batch("PRAGMA writable_schema = OFF;")
            .map_err(database_error(
                "disable SQLite writable schema after v26 event migration",
            ));
        let replaced = replaced.map_err(database_error("add code-change event kind"))?;
        writable_schema_disabled?;
        if replaced != 1 {
            return Err(AppError::Runtime {
                operation: "migrate exactly one SQLite events kind list to v26",
            });
        }
    }
    verify_event_kinds_v26(transaction)
}

fn verify_event_kinds_v26(connection: &Connection) -> Result<(), AppError> {
    let event_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read SQLite events schema for v26 verification"))?;
    if event_sql
        .as_deref()
        .is_some_and(|sql| event_kind_list_matches(sql, EVENTS_V26_KIND_LIST))
    {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v26 event kinds",
        })
    }
}

fn migrate_code_change_schema_to_v26(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    migrate_event_kinds_to_v26(transaction)?;

    let already = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'code_change_runs'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v26 code-change runs"))?;
    if already == 0 {
        transaction
            .execute_batch(CODE_CHANGE_RUNS_V26_TABLE_SQL)
            .map_err(database_error("create SQLite v26 code-change runs"))?;
    }
    let attempts = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'code_change_editor_attempts'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v26 editor attempts"))?;
    if attempts == 0 {
        transaction
            .execute_batch(CODE_CHANGE_EDITOR_ATTEMPTS_V26_TABLE_SQL)
            .map_err(database_error("create SQLite v26 editor attempts"))?;
    }
    let checks = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'table' AND name = 'code_change_checks'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v26 code-change checks"))?;
    if checks == 0 {
        transaction
            .execute_batch(CODE_CHANGE_CHECKS_V26_TABLE_SQL)
            .map_err(database_error("create SQLite v26 code-change checks"))?;
    }
    for (name, sql) in [
        ("base_revision_sha", CAMPAIGNS_V26_BASE_REVISION_COLUMN_SQL),
    ] {
        let present = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_table_info('campaigns') WHERE name = ?1
                 )",
                [name],
                |row| row.get::<_, bool>(0),
            )
            .map_err(database_error("check SQLite v26 campaign column"))?;
        if !present {
            transaction
                .execute_batch(sql)
                .map_err(database_error("add SQLite v26 campaign column"))?;
        }
    }
    for (name, sql) in [
        ("code_change_run_id", EXPERIMENTS_V26_RUN_COLUMN_SQL),
        ("code_revision_sha", EXPERIMENTS_V26_REVISION_COLUMN_SQL),
    ] {
        let present = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_table_info('experiments') WHERE name = ?1
                 )",
                [name],
                |row| row.get::<_, bool>(0),
            )
            .map_err(database_error("check SQLite v26 experiment column"))?;
        if !present {
            transaction
                .execute_batch(sql)
                .map_err(database_error("add SQLite v26 experiment column"))?;
        }
    }
    for (name, sql) in [
        (
            "code_change_one_live_per_campaign",
            CODE_CHANGE_ONE_LIVE_PER_CAMPAIGN_INDEX_SQL,
        ),
    ] {
        ensure_index_definition(transaction, name, sql)?;
    }
    verify_code_change_schema_v26(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 26;")
        .map_err(database_error("set SQLite v26 schema version"))
}

fn verify_code_change_schema_v26(connection: &Connection) -> Result<(), AppError> {
    let required_columns = [
        ("campaigns", "base_revision_sha"),
        ("experiments", "code_change_run_id"),
        ("experiments", "code_revision_sha"),
    ];
    if required_columns.iter().any(|(table, column)| {
        connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM pragma_table_info('{table}') WHERE name = ?1)"
                ),
                [*column],
                |row| row.get::<_, bool>(0),
            )
            .unwrap_or(false)
            == false
    }) {
        return Err(AppError::Runtime {
            operation: "verify SQLite v26 code-change schema",
        });
    }
    for (table, expected_sql) in [
        ("code_change_runs", CODE_CHANGE_RUNS_V26_TABLE_SQL),
        (
            "code_change_editor_attempts",
            CODE_CHANGE_EDITOR_ATTEMPTS_V26_TABLE_SQL,
        ),
        ("code_change_checks", CODE_CHANGE_CHECKS_V26_TABLE_SQL),
    ] {
        let actual_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read SQLite v26 code-change schema"))?;
        if !actual_sql.as_deref().is_some_and(|sql| {
            let sql = if table == "code_change_runs" {
                strip_v27_code_change_additions(sql)
            } else {
                sql.to_owned()
            };
            compact_sql_exact(&sql) == compact_sql_exact(expected_sql)
        })
        {
            return Err(AppError::Runtime {
                operation: "verify SQLite v26 code-change schema",
            });
        }
    }
    let campaign_columns_match = campaign_table_info_matches(
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
            ("objective_metric_json", "TEXT", 0, 0),
            ("current_best_experiment_id", "TEXT", 0, 0),
            ("plateau_count", "INTEGER", 1, 0),
            ("base_revision_sha", "TEXT", 0, 0),
        ],
    )
    .map_err(database_error("verify SQLite v26 campaign columns"))?;
    let experiment_columns_match = campaign_table_info_matches(
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
            ("resume_of_experiment_id", "TEXT", 0, 0),
            ("checkpoint_note", "TEXT", 0, 0),
            ("code_change_run_id", "TEXT", 0, 0),
            ("code_revision_sha", "TEXT", 0, 0),
        ],
    )
    .map_err(database_error("verify SQLite v26 experiment columns"))?;
    if !campaign_columns_match || !experiment_columns_match {
        return Err(AppError::Runtime {
            operation: "verify SQLite v26 code-change schema",
        });
    }
    for (name, expected_sql) in [
        (
            "code_change_one_live_per_campaign",
            CODE_CHANGE_ONE_LIVE_PER_CAMPAIGN_INDEX_SQL,
        ),
    ] {
        let actual_sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read SQLite v26 code-change indexes"))?;
        if !actual_sql
            .as_deref()
            .is_some_and(|sql| compact_sql_exact(sql) == compact_sql_exact(expected_sql))
        {
            return Err(AppError::Runtime {
                operation: "verify SQLite v26 code-change schema",
            });
        }
    }
    let run_foreign_keys_match = campaign_foreign_keys_match(
        connection,
        "code_change_runs",
        &[
            ("proposals", "proposal_id", "proposal_id", "NO ACTION"),
            ("campaigns", "campaign_id", "campaign_id", "NO ACTION"),
            ("experiments", "experiment_id", "experiment_id", "NO ACTION"),
        ],
    )
    .map_err(database_error("verify SQLite v26 code-change run foreign keys"))?;
    let attempt_foreign_keys_match = campaign_foreign_keys_match(
        connection,
        "code_change_editor_attempts",
        &[
            (
                "code_change_runs",
                "code_change_run_id",
                "code_change_run_id",
                "NO ACTION",
            ),
            ("agent_runs", "agent_run_id", "run_id", "NO ACTION"),
        ],
    )
    .map_err(database_error("verify SQLite v26 editor attempt foreign keys"))?;
    let check_foreign_keys_match = campaign_foreign_keys_match(
        connection,
        "code_change_checks",
        &[(
            "code_change_runs",
            "code_change_run_id",
            "code_change_run_id",
            "NO ACTION",
        )],
    )
    .map_err(database_error("verify SQLite v26 check foreign keys"))?;
    let experiment_foreign_keys_match = campaign_foreign_keys_match(
        connection,
        "experiments",
        &[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            ("proposals", "proposal_id", "proposal_id", "RESTRICT"),
            ("submissions", "submission_id", "submission_id", "RESTRICT"),
            (
                "experiments",
                "parent_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
            (
                "experiments",
                "resume_of_experiment_id",
                "experiment_id",
                "NO ACTION",
            ),
            (
                "code_change_runs",
                "code_change_run_id",
                "code_change_run_id",
                "NO ACTION",
            ),
        ],
    )
    .map_err(database_error("verify SQLite v26 experiment foreign keys"))?;
    if !run_foreign_keys_match
        || !attempt_foreign_keys_match
        || !check_foreign_keys_match
        || !experiment_foreign_keys_match
    {
        return Err(AppError::Runtime {
            operation: "verify SQLite v26 code-change schema",
        });
    }
    Ok(())
}

fn strip_v27_code_change_additions(sql: &str) -> String {
    CODE_CHANGE_V27_PROOF_COLUMNS.iter().fold(compact_sql_exact(sql), |sql, column| {
        sql.replace(&format!(", {column} TEXT"), "")
            .replace(&format!(",{column}TEXT"), "")
    })
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
    let event_sql: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite events schema for v8 migration"))?;
    if event_kind_list(&event_sql).is_some_and(|list| list.contains("'operator_wake'")) {
        return transaction
            .execute_batch("PRAGMA user_version = 8;")
            .map_err(database_error("set existing SQLite v8 event version"));
    }
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

fn compact_table_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .replace("\"experiment_metrics\"", "experiment_metrics")
        .replace(',', " , ")
        .replace('(', " ( ")
        .replace(')', " ) ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
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

fn migrate_decision_schema_to_v18(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    if decision_schema_v19_is_canonical(transaction).unwrap_or(false) {
        return transaction
            .execute_batch("PRAGMA user_version = 18;")
            .map_err(database_error("retain existing SQLite v19 decision schema"));
    }
    if decision_schema_v18_is_canonical(transaction).unwrap_or(false) {
        return transaction
            .execute_batch("PRAGMA user_version = 18;")
            .map_err(database_error("set existing SQLite v18 schema version"));
    }
    let any_decision_schema: bool = transaction
        .query_row(
            "SELECT EXISTS(
                SELECT 1 FROM sqlite_master
                WHERE name IN (
                    'decision_cycles', 'decision_attempts',
                    'decision_cycles_state_wake_updated_idx',
                    'decision_cycles_campaign_state_updated_idx',
                    'decision_attempts_state_created_idx'
                )
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check partial SQLite v18 decision schema"))?;
    if any_decision_schema && !decision_objects_v18_are_canonical(transaction).unwrap_or(false) {
        return Err(AppError::Runtime {
            operation: "verify partial SQLite v18 decision schema before migration",
        });
    }
    let event_sql: String = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("read SQLite events schema for v18 migration"))?;
    if !event_kind_list_matches(&event_sql, EVENTS_V18_KIND_LIST) {
        if !event_kind_list_matches(&event_sql, EVENTS_V17_KIND_LIST) {
            return Err(AppError::Runtime {
                operation: "verify SQLite event kinds before v18 migration",
            });
        }
        let old_kind_list = event_kind_list(&event_sql).ok_or(AppError::Runtime {
            operation: "read SQLite events kind list for v18 migration",
        })?;
        transaction
            .execute_batch("PRAGMA writable_schema = ON;")
            .map_err(database_error("enable SQLite writable schema for v18 event migration"))?;
        let replaced = transaction.execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'
                AND sql LIKE '%' || ?1 || '%'",
            params![
                old_kind_list,
                EVENTS_V18_KIND_LIST
            ],
        );
        let writable_schema_disabled = transaction
            .execute_batch("PRAGMA writable_schema = OFF;")
            .map_err(database_error(
                "disable SQLite writable schema after v18 event migration",
            ));
        let replaced = replaced.map_err(database_error("add campaign decision event kind"))?;
        writable_schema_disabled?;
        if replaced != 1 {
            return Err(AppError::Runtime {
                operation: "migrate exactly one SQLite events kind list to v18",
            });
        }
    }

    if !any_decision_schema {
        for sql in [
            DECISION_CYCLES_V18_TABLE_SQL,
            DECISION_ATTEMPTS_V18_TABLE_SQL,
            DECISION_CYCLES_DUE_INDEX_SQL,
            DECISION_CYCLES_CAMPAIGN_INDEX_SQL,
            DECISION_ATTEMPTS_STATE_INDEX_SQL,
        ] {
            transaction
                .execute_batch(sql)
                .map_err(database_error("apply SQLite v18 decision migration"))?;
        }
    }
    verify_decision_schema_v18(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 18;")
        .map_err(database_error("set SQLite v18 schema version"))
}

fn verify_decision_schema_v18(connection: &Connection) -> Result<(), AppError> {
    if decision_schema_v18_is_canonical(connection).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v18 decision schema",
        })
    }
}

fn decision_schema_v18_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    let event_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if !event_sql
        .as_deref()
        .is_some_and(|sql| events_kind_list_is_current(sql))
    {
        return Ok(false);
    }

    decision_objects_v18_are_canonical(connection)
}

fn decision_objects_v18_are_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    for (table, expected_sql) in [
        ("decision_cycles", DECISION_CYCLES_V18_TABLE_SQL),
        ("decision_attempts", DECISION_ATTEMPTS_V18_TABLE_SQL),
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
        "decision_cycles",
        &[
            ("cycle_id", "TEXT", 0, 1),
            ("campaign_id", "TEXT", 1, 0),
            ("source_experiment_id", "TEXT", 1, 0),
            ("state", "TEXT", 1, 0),
            ("next_wake_at", "INTEGER", 0, 0),
            ("consecutive_failed_attempts", "INTEGER", 1, 0),
            ("last_decision_kind", "TEXT", 0, 0),
            ("last_failure_code", "TEXT", 0, 0),
            ("last_failure_summary", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
        ],
    )? || !campaign_table_info_matches(
        connection,
        "decision_attempts",
        &[
            ("cycle_id", "TEXT", 1, 1),
            ("attempt_number", "INTEGER", 1, 2),
            ("state", "TEXT", 1, 0),
            ("context_schema_version", "INTEGER", 0, 0),
            ("context_json", "TEXT", 0, 0),
            ("context_digest", "TEXT", 0, 0),
            ("agent_run_id", "INTEGER", 0, 0),
            ("decision_json", "TEXT", 0, 0),
            ("decision_digest", "TEXT", 0, 0),
            ("decision_kind", "TEXT", 0, 0),
            ("failure_code", "TEXT", 0, 0),
            ("failure_summary", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("started_at", "INTEGER", 0, 0),
            ("finished_at", "INTEGER", 0, 0),
        ],
    )? {
        return Ok(false);
    }

    if !campaign_foreign_keys_match(
        connection,
        "decision_cycles",
        &[
            ("campaigns", "campaign_id", "campaign_id", "NO ACTION"),
            (
                "experiments",
                "source_experiment_id",
                "experiment_id",
                "NO ACTION",
            ),
        ],
    )? || !campaign_foreign_keys_match(
        connection,
        "decision_attempts",
        &[
            ("decision_cycles", "cycle_id", "cycle_id", "NO ACTION"),
            ("agent_runs", "agent_run_id", "run_id", "NO ACTION"),
        ],
    )? {
        return Ok(false);
    }

    for (name, expected_sql) in [
        (
            "decision_cycles_state_wake_updated_idx",
            DECISION_CYCLES_DUE_INDEX_SQL,
        ),
        (
            "decision_cycles_campaign_state_updated_idx",
            DECISION_CYCLES_CAMPAIGN_INDEX_SQL,
        ),
        (
            "decision_attempts_state_created_idx",
            DECISION_ATTEMPTS_STATE_INDEX_SQL,
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

fn migrate_decision_schema_to_v19(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    if decision_schema_v19_is_canonical(transaction).unwrap_or(false) {
        return transaction
            .execute_batch("PRAGMA user_version = 19;")
            .map_err(database_error("set existing SQLite v19 schema version"));
    }
    verify_decision_schema_v18(transaction)?;
    transaction
        .execute_batch(
            "ALTER TABLE decision_cycles
                 ADD COLUMN source_terminal_at INTEGER NOT NULL DEFAULT 1
                     CHECK(source_terminal_at > 0);
             UPDATE decision_cycles
                SET source_terminal_at = (
                    SELECT CASE
                        WHEN typeof(COALESCE(e.finished_at, e.updated_at)) = 'integer'
                         AND e.status IN ('succeeded','failed','cancelled')
                        THEN COALESCE(e.finished_at, e.updated_at)
                    END
                    FROM experiments e
                    WHERE e.experiment_id = decision_cycles.source_experiment_id
                      AND e.campaign_id = decision_cycles.campaign_id
                );
             DROP INDEX decision_cycles_state_wake_updated_idx;
             DROP INDEX decision_cycles_campaign_state_updated_idx;
             DROP INDEX IF EXISTS decision_cycles_campaign_state_wake_updated_idx;
             DROP INDEX IF EXISTS experiments_campaign_terminal_order_idx;",
        )
        .map_err(database_error("backfill SQLite v19 decision source order"))?;
    for sql in [
        DECISION_CYCLES_STATE_SOURCE_INDEX_SQL,
        DECISION_CYCLES_CAMPAIGN_STATE_SOURCE_INDEX_SQL,
        DECISION_CYCLES_CAMPAIGN_WAKE_SOURCE_INDEX_SQL,
    ] {
        transaction
            .execute_batch(sql)
            .map_err(database_error("create SQLite v19 decision order index"))?;
    }
    verify_decision_schema_v19(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 19;")
        .map_err(database_error("set SQLite v19 schema version"))
}

fn verify_decision_schema_v19(connection: &Connection) -> Result<(), AppError> {
    if decision_schema_v19_is_canonical(connection).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v19 decision schema",
        })
    }
}

fn migrate_decision_schema_to_v20(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    verify_decision_schema_v19(transaction)?;
    let existing_index_sql: Option<String> = transaction
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
            ["decision_cycles_state_wake_source_order_idx"],
            |row| row.get(0),
        )
        .optional()
        .map_err(database_error("read SQLite v20 decision wake index"))?;
    match existing_index_sql.as_deref() {
        None => transaction
            .execute_batch(DECISION_CYCLES_STATE_WAKE_SOURCE_INDEX_SQL)
            .map_err(database_error("create SQLite v20 decision wake index"))?,
        Some(sql)
            if compact_sql_exact(sql)
                == compact_sql_exact(DECISION_CYCLES_STATE_WAKE_SOURCE_INDEX_SQL) => {}
        Some(_) => {
            return Err(AppError::Runtime {
                operation: "verify SQLite v20 decision schema",
            });
        }
    }
    verify_decision_schema_v20(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 20;")
        .map_err(database_error("set SQLite v20 schema version"))
}

fn verify_decision_schema_v20(connection: &Connection) -> Result<(), AppError> {
    if decision_schema_v20_is_canonical(connection).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v20 decision schema",
        })
    }
}

fn decision_schema_v20_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    if !decision_schema_v19_is_canonical(connection)? {
        return Ok(false);
    }
    let actual_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
            ["decision_cycles_state_wake_source_order_idx"],
            |row| row.get(0),
        )
        .optional()?;
    Ok(actual_sql.as_deref().is_some_and(|sql| {
        compact_sql_exact(sql) == compact_sql_exact(DECISION_CYCLES_STATE_WAKE_SOURCE_INDEX_SQL)
    }))
}

fn migrate_decision_schema_to_v21(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    verify_decision_schema_v20(transaction)?;
    for (name, sql) in [
        (
            "decision_attempts_unbound_state_created_idx",
            DECISION_ATTEMPTS_UNBOUND_STATE_INDEX_SQL,
        ),
        (
            "agent_run_events_project_event_run_idx",
            AGENT_RUN_EVENTS_PROJECT_EVENT_RUN_INDEX_SQL,
        ),
    ] {
        let existing_sql: Option<String> = transaction
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("read SQLite v21 decision repair index"))?;
        match existing_sql.as_deref() {
            None => transaction
                .execute_batch(sql)
                .map_err(database_error("create SQLite v21 decision repair index"))?,
            Some(actual) if compact_sql_exact(actual) == compact_sql_exact(sql) => {}
            Some(_) => {
                return Err(AppError::Runtime {
                    operation: "verify SQLite v21 decision schema",
                });
            }
        }
    }
    verify_decision_schema_v21(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 21;")
        .map_err(database_error("set SQLite v21 schema version"))
}

fn verify_decision_schema_v21(connection: &Connection) -> Result<(), AppError> {
    if decision_schema_v21_is_canonical(connection).unwrap_or(false) {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v21 decision schema",
        })
    }
}

fn decision_schema_v21_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    if !decision_schema_v20_is_canonical(connection)? {
        return Ok(false);
    }
    for (name, expected_sql) in [
        (
            "decision_attempts_unbound_state_created_idx",
            DECISION_ATTEMPTS_UNBOUND_STATE_INDEX_SQL,
        ),
        (
            "agent_run_events_project_event_run_idx",
            AGENT_RUN_EVENTS_PROJECT_EVENT_RUN_INDEX_SQL,
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

fn migrate_running_health_schema_to_v22(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let already = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='running_health'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v22 running_health"))?;
    if already == 0 {
        transaction
            .execute_batch(RUNNING_HEALTH_V22_TABLE_SQL)
            .map_err(database_error("create SQLite v22 running_health"))?;
    }
    for (name, sql) in [
        (
            "resume_of_experiment_id",
            EXPERIMENTS_V22_RESUME_COLUMN_SQL,
        ),
        ("checkpoint_note", EXPERIMENTS_V22_CHECKPOINT_NOTE_COLUMN_SQL),
    ] {
        let present = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_table_info('experiments') WHERE name = ?1
                 )",
                [name],
                |row| row.get::<_, bool>(0),
            )
            .map_err(database_error("check SQLite v22 experiment column"))?;
        if !present {
            transaction
                .execute_batch(sql)
                .map_err(database_error("apply SQLite v22 experiment column"))?;
        }
    }
    verify_running_health_schema_v22(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 22;")
        .map_err(database_error("set SQLite v22 schema version"))
}

fn verify_running_health_schema_v22(connection: &Connection) -> Result<(), AppError> {
    let found: i64 = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM sqlite_master
                      WHERE type = 'table' AND name = 'running_health')
                  + (SELECT COUNT(*) FROM pragma_table_info('experiments')
                     WHERE name IN ('resume_of_experiment_id', 'checkpoint_note'))",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("verify SQLite v22 running health schema"))?;
    if found == 3 {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v22 running health schema",
        })
    }
}

fn migrate_evaluation_schema_to_v24(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let already = transaction
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='experiment_metrics'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(database_error("probe SQLite v24 experiment_metrics"))?;
    if already == 0 {
        transaction
            .execute_batch(EXPERIMENT_METRICS_V24_TABLE_SQL)
            .map_err(database_error("create SQLite v24 experiment_metrics"))?;
    }
    for (name, sql) in [
        (
            "objective_metric_json",
            CAMPAIGNS_V24_OBJECTIVE_METRIC_COLUMN_SQL,
        ),
        (
            "current_best_experiment_id",
            CAMPAIGNS_V24_CURRENT_BEST_COLUMN_SQL,
        ),
        ("plateau_count", CAMPAIGNS_V24_PLATEAU_COUNT_COLUMN_SQL),
    ] {
        let present = transaction
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pragma_table_info('campaigns') WHERE name = ?1
                 )",
                [name],
                |row| row.get::<_, bool>(0),
            )
            .map_err(database_error("check SQLite v24 campaign column"))?;
        if !present {
            transaction
                .execute_batch(sql)
                .map_err(database_error("apply SQLite v24 campaign column"))?;
        }
    }
    verify_evaluation_schema_v24(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 24;")
        .map_err(database_error("set SQLite v24 schema version"))
}

fn migrate_evaluation_marker_to_v25(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), AppError> {
    let has_evaluated_at: bool = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('experiment_metrics') WHERE name = 'evaluated_at'
             )",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("check SQLite v25 evaluated_at"))?;
    if !has_evaluated_at {
        transaction
            .execute_batch(EXPERIMENT_METRICS_V25_EVALUATED_AT_COLUMN_SQL)
            .map_err(database_error("add SQLite v25 evaluated_at"))?;
    } else {
        // A v24 database with an existing marker is the legacy crash-window
        // shape. Preserve its values, including NULL terminal markers, while
        // canonicalizing the declaration to nullable TEXT.
        let marker_schema: (String, i64) = transaction
            .query_row(
                "SELECT type, \"notnull\" FROM pragma_table_info('experiment_metrics')
                 WHERE name = 'evaluated_at'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(database_error("read evaluated_at type"))?;
        if marker_schema.0.to_uppercase() != "TEXT" || marker_schema.1 != 0 {
            transaction
                .execute_batch(
                    r#"
                CREATE TABLE experiment_metrics_v25_new (
                    experiment_id TEXT PRIMARY KEY REFERENCES experiments(experiment_id) ON DELETE CASCADE,
                    source TEXT NOT NULL CHECK (source IN ('manifest')),
                    primary_metric_name TEXT,
                    primary_metric_value REAL,
                    metrics_json TEXT NOT NULL DEFAULT '{}',
                    artifact_defect TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    evaluated_at TEXT NULL
                );
                INSERT INTO experiment_metrics_v25_new (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at, evaluated_at)
                    SELECT experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at, CAST(evaluated_at AS TEXT) FROM experiment_metrics;
                DROP TABLE experiment_metrics;
                ALTER TABLE experiment_metrics_v25_new RENAME TO experiment_metrics;
                "#,
                )
                .map_err(database_error("migrate evaluated_at type to TEXT"))?;
        }
    }
    if !has_evaluated_at {
        transaction
            .execute(
                "UPDATE experiment_metrics
                 SET evaluated_at = CAST(updated_at AS TEXT)
                 WHERE evaluated_at IS NULL
                   AND experiment_id IN (
                       SELECT experiment_id FROM experiments
                       WHERE status IN ('succeeded', 'failed', 'cancelled')
                   )",
                [],
            )
            .map_err(database_error("backfill evaluated_at for terminal experiments"))?;
    }
    verify_evaluation_schema_v25(transaction)?;
    transaction
        .execute_batch("PRAGMA user_version = 25;")
        .map_err(database_error("set SQLite v25 schema version"))
}

fn verify_evaluation_schema_v25(connection: &Connection) -> Result<(), AppError> {
    let found: i64 = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM sqlite_master
                      WHERE type = 'table' AND name = 'experiment_metrics')
                  + (SELECT COUNT(*) FROM pragma_table_info('experiment_metrics')
                     WHERE name = 'evaluated_at' AND type = 'TEXT' AND \"notnull\" = 0)
                  + (SELECT COUNT(*) FROM pragma_table_info('campaigns')
                     WHERE name IN ('objective_metric_json', 'current_best_experiment_id',
                                    'plateau_count'))",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("verify SQLite v25 evaluation schema"))?;
    if found == 5 {
        verify_evaluation_schema_v24(connection)?;
        if !experiment_metrics_schema_v25_is_canonical(connection).unwrap_or(false) {
            return Err(AppError::Runtime {
                operation: "verify SQLite v25 evaluation schema",
            });
        }
        let bad: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM experiment_metrics em
                 JOIN experiments e ON e.experiment_id = em.experiment_id
                 WHERE e.status IN ('reserved', 'submitting', 'accepted', 'unreconciled')
                   AND em.evaluated_at IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .map_err(database_error("verify v25 backfill nonterminal"))?;
        if bad != 0 {
            return Err(AppError::Runtime {
                operation: "verify SQLite v25 backfill nonterminal evaluated_at must be NULL",
            });
        }
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v25 evaluation schema",
        })
    }
}

fn experiment_metrics_schema_v25_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    let actual_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'table' AND name = 'experiment_metrics'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if !actual_sql.as_deref().is_some_and(|sql| {
        compact_table_sql(sql) == compact_table_sql(EXPERIMENT_METRICS_V25_TABLE_SQL)
    }) {
        return Ok(false);
    }
    if !campaign_table_info_matches(
        connection,
        "experiment_metrics",
        &[
            ("experiment_id", "TEXT", 0, 1),
            ("source", "TEXT", 1, 0),
            ("primary_metric_name", "TEXT", 0, 0),
            ("primary_metric_value", "REAL", 0, 0),
            ("metrics_json", "TEXT", 1, 0),
            ("artifact_defect", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
            ("evaluated_at", "TEXT", 0, 0),
        ],
    )? {
        return Ok(false);
    }
    campaign_foreign_keys_match(
        connection,
        "experiment_metrics",
        &[(
            "experiments",
            "experiment_id",
            "experiment_id",
            "CASCADE",
        )],
    )
}

fn verify_evaluation_schema_v24(connection: &Connection) -> Result<(), AppError> {
    let found: i64 = connection
        .query_row(
            "SELECT (SELECT COUNT(*) FROM sqlite_master
                      WHERE type = 'table' AND name = 'experiment_metrics')
                  + (SELECT COUNT(*) FROM pragma_table_info('campaigns')
                     WHERE name IN ('objective_metric_json', 'current_best_experiment_id',
                                    'plateau_count'))",
            [],
            |row| row.get(0),
        )
        .map_err(database_error("verify SQLite v24 evaluation schema"))?;
    if found == 4 {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "verify SQLite v24 evaluation schema",
        })
    }
}


fn decision_schema_v19_is_canonical(connection: &Connection) -> rusqlite::Result<bool> {
    let event_sql: Option<String> = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if !event_sql
        .as_deref()
        .is_some_and(|sql| events_kind_list_is_current(sql))
    {
        return Ok(false);
    }

    for (table, expected_sql) in [
        ("decision_cycles", DECISION_CYCLES_V19_TABLE_SQL),
        ("decision_attempts", DECISION_ATTEMPTS_V18_TABLE_SQL),
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
        "decision_cycles",
        &[
            ("cycle_id", "TEXT", 0, 1),
            ("campaign_id", "TEXT", 1, 0),
            ("source_experiment_id", "TEXT", 1, 0),
            ("state", "TEXT", 1, 0),
            ("next_wake_at", "INTEGER", 0, 0),
            ("consecutive_failed_attempts", "INTEGER", 1, 0),
            ("last_decision_kind", "TEXT", 0, 0),
            ("last_failure_code", "TEXT", 0, 0),
            ("last_failure_summary", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("updated_at", "INTEGER", 1, 0),
            ("source_terminal_at", "INTEGER", 1, 0),
        ],
    )? || !campaign_table_info_matches(
        connection,
        "decision_attempts",
        &[
            ("cycle_id", "TEXT", 1, 1),
            ("attempt_number", "INTEGER", 1, 2),
            ("state", "TEXT", 1, 0),
            ("context_schema_version", "INTEGER", 0, 0),
            ("context_json", "TEXT", 0, 0),
            ("context_digest", "TEXT", 0, 0),
            ("agent_run_id", "INTEGER", 0, 0),
            ("decision_json", "TEXT", 0, 0),
            ("decision_digest", "TEXT", 0, 0),
            ("decision_kind", "TEXT", 0, 0),
            ("failure_code", "TEXT", 0, 0),
            ("failure_summary", "TEXT", 0, 0),
            ("created_at", "INTEGER", 1, 0),
            ("started_at", "INTEGER", 0, 0),
            ("finished_at", "INTEGER", 0, 0),
        ],
    )? {
        return Ok(false);
    }

    if !campaign_foreign_keys_match(
        connection,
        "decision_cycles",
        &[
            ("campaigns", "campaign_id", "campaign_id", "NO ACTION"),
            (
                "experiments",
                "source_experiment_id",
                "experiment_id",
                "NO ACTION",
            ),
        ],
    )? || !campaign_foreign_keys_match(
        connection,
        "decision_attempts",
        &[
            ("decision_cycles", "cycle_id", "cycle_id", "NO ACTION"),
            ("agent_runs", "agent_run_id", "run_id", "NO ACTION"),
        ],
    )? {
        return Ok(false);
    }

    for (name, expected_sql) in [
        (
            "decision_cycles_state_source_order_idx",
            DECISION_CYCLES_STATE_SOURCE_INDEX_SQL,
        ),
        (
            "decision_cycles_campaign_state_source_order_idx",
            DECISION_CYCLES_CAMPAIGN_STATE_SOURCE_INDEX_SQL,
        ),
        (
            "decision_cycles_campaign_state_wake_source_order_idx",
            DECISION_CYCLES_CAMPAIGN_WAKE_SOURCE_INDEX_SQL,
        ),
        (
            "decision_attempts_state_created_idx",
            DECISION_ATTEMPTS_STATE_INDEX_SQL,
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

    let invalid_projection: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM decision_cycles dc
             LEFT JOIN experiments e ON e.experiment_id = dc.source_experiment_id
             WHERE typeof(dc.source_terminal_at) != 'integer'
                OR dc.source_terminal_at <= 0
                OR e.experiment_id IS NULL
                OR e.campaign_id != dc.campaign_id
                OR e.status NOT IN ('succeeded','failed','cancelled')
                OR typeof(COALESCE(e.finished_at, e.updated_at)) != 'integer'
                OR dc.source_terminal_at != COALESCE(e.finished_at, e.updated_at)
         )",
        [],
        |row| row.get(0),
    )?;
    Ok(!invalid_projection)
}

fn event_kind_list_matches(event_sql: &str, canonical_kind_list: &str) -> bool {
    let canonical_kind_list = canonical_kind_list
        .split_whitespace()
        .collect::<String>();
    event_kind_list(event_sql)
        .map(|list| list.split_whitespace().collect::<String>())
        .as_deref()
        == Some(canonical_kind_list.as_str())
}

/// Databases mid-migration carry the v18 event kind list; current databases
/// carry the v23 superset with `health_diagnosis`.
fn events_kind_list_is_current(event_sql: &str) -> bool {
    event_kind_list_matches(event_sql, EVENTS_V26_KIND_LIST)
        || event_kind_list_matches(event_sql, EVENTS_V23_KIND_LIST)
        || event_kind_list_matches(event_sql, EVENTS_V18_KIND_LIST)
}

fn event_kind_list(event_sql: &str) -> Option<&str> {
    event_sql
        .split_once("kind TEXT NOT NULL CHECK (kind IN (")
        .and_then(|(_, remainder)| remainder.split_once(')').map(|(list, _)| list))
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
        let matches = actual_sql.as_deref().is_some_and(|sql| {
            match table {
                "experiments" => compact_sql_exact(&strip_v26_experiment_additions(sql))
                    == compact_sql_exact(expected_sql),
                "campaigns" => compact_sql_exact(&strip_v26_campaign_additions(sql))
                    == compact_sql_exact(expected_sql),
                _ => compact_sql_exact(sql) == compact_sql_exact(expected_sql),
            }
        });
        if !matches {
            return Ok(false);
        }
    }

    let mut campaign_columns: Vec<(&str, &str, i64, i64)> = vec![
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
    ];
    if campaign_has_v24_additions(connection)? {
        campaign_columns.push(("objective_metric_json", "TEXT", 0, 0));
        campaign_columns.push(("current_best_experiment_id", "TEXT", 0, 0));
        campaign_columns.push(("plateau_count", "INTEGER", 1, 0));
    }
    if campaign_has_v26_additions(connection)? {
        campaign_columns.push(("base_revision_sha", "TEXT", 0, 0));
    }
    if !campaign_table_info_matches(
        connection,
        "campaigns",
        &campaign_columns,
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
    )? || !experiments_campaign_columns_match(
        connection,
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
    )? || !experiments_foreign_keys_match(
        connection,
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

fn experiments_campaign_columns_match(connection: &Connection) -> rusqlite::Result<bool> {
    const BASE_COLUMNS: [(&str, &str, i64, i64); 14] = [
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
    ];
    let mut expected = BASE_COLUMNS.to_vec();
    if experiment_has_v22_additions(connection)? {
        expected.push(("resume_of_experiment_id", "TEXT", 0, 0));
        expected.push(("checkpoint_note", "TEXT", 0, 0));
    }
    if experiment_has_v26_additions(connection)? {
        expected.push(("code_change_run_id", "TEXT", 0, 0));
        expected.push(("code_revision_sha", "TEXT", 0, 0));
    }
    campaign_table_info_matches(connection, "experiments", &expected)
}

fn experiments_foreign_keys_match(connection: &Connection) -> rusqlite::Result<bool> {
    let mut expected: Vec<(&str, &str, &str, &str)> = vec![
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
    ];
    if experiment_has_v22_additions(connection)? {
        expected.push((
            "experiments",
            "resume_of_experiment_id",
            "experiment_id",
            "NO ACTION",
        ));
    }
    if experiment_has_v26_additions(connection)? {
        expected.push((
            "code_change_runs",
            "code_change_run_id",
            "code_change_run_id",
            "NO ACTION",
        ));
    }
    campaign_foreign_keys_match(connection, "experiments", &expected)
}

fn experiment_has_v22_additions(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM pragma_table_info('experiments')
             WHERE name = 'checkpoint_note'
         )",
        [],
        |row| row.get(0),
    )
}

fn strip_v22_experiment_additions(sql: &str) -> String {
    sql.replace(
        ", resume_of_experiment_id TEXT REFERENCES experiments(experiment_id), checkpoint_note TEXT",
        "",
    )
}

fn strip_v26_experiment_additions(sql: &str) -> String {
    strip_v22_experiment_additions(sql).replace(
        ", code_change_run_id TEXT REFERENCES code_change_runs(code_change_run_id), code_revision_sha TEXT",
        "",
    )
}

fn campaign_has_v24_additions(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM pragma_table_info('campaigns')
             WHERE name = 'plateau_count'
         )",
        [],
        |row| row.get(0),
    )
}

fn strip_v24_campaign_additions(sql: &str) -> String {
    sql.replace(
        ", objective_metric_json TEXT, current_best_experiment_id TEXT, plateau_count INTEGER NOT NULL DEFAULT 0",
        "",
    )
}

fn strip_v26_campaign_additions(sql: &str) -> String {
    strip_v24_campaign_additions(sql).replace(", base_revision_sha TEXT", "")
}

fn campaign_has_v26_additions(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM pragma_table_info('campaigns')
             WHERE name = 'base_revision_sha'
         )",
        [],
        |row| row.get(0),
    )
}

fn experiment_has_v26_additions(connection: &Connection) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM pragma_table_info('experiments')
             WHERE name = 'code_change_run_id'
         )",
        [],
        |row| row.get(0),
    )
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

#[cfg(test)]
mod v23_event_kind_tests {
    use super::*;

    fn events_kind_list(connection: &Connection) -> String {
        let sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        event_kind_list(&sql).unwrap().to_owned()
    }

    #[test]
    fn fresh_databases_accept_health_diagnosis_events_at_v23() {
        let temporary = tempfile::tempdir().unwrap();
        let db_path = temporary.path().join("state.sqlite3");
        let db = crate::db::Db::open(&db_path).unwrap();
        {
            let connection = db.connect().unwrap();
            assert_eq!(
                connection
                    .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                LATEST_SCHEMA_VERSION
            );
            assert!(events_kind_list(&connection).contains("health_diagnosis"));
        }
    }

    #[test]
    fn v22_databases_gain_the_health_diagnosis_event_kind_on_reopen() {
        let temporary = tempfile::tempdir().unwrap();
        let db_path = temporary.path().join("state.sqlite3");
        {
            let _db = crate::db::Db::open(&db_path).unwrap();
        }
        {
            let mut connection = Connection::open(&db_path).unwrap();
            let transaction = connection.transaction().unwrap();
            transaction
                .execute_batch("PRAGMA writable_schema = ON;")
                .unwrap();
            transaction
                .execute(
                    "UPDATE sqlite_master
                        SET sql = replace(sql, ?1, ?2)
                      WHERE type = 'table' AND name = 'events'",
                    params![
                        ", 'health_diagnosis'",
                        ""
                    ],
                )
                .unwrap();
            transaction
                .execute_batch("PRAGMA writable_schema = OFF;")
                .unwrap();
            transaction
                .execute_batch("PRAGMA user_version = 22;")
                .unwrap();
            transaction.commit().unwrap();
        }
        let connection = Connection::open(&db_path).unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            22
        );
        drop(connection);

        let db = crate::db::Db::open(&db_path).unwrap();
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            LATEST_SCHEMA_VERSION
        );
        assert!(events_kind_list(&connection).contains("health_diagnosis"));
    }
}
