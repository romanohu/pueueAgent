use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
};

use pueue_agent::{
    batches::BatchJobResult,
    decision_evidence::{
        DecisionEvidenceBuilder, DecisionEvidenceRequest, DecisionPueueTaskProjection,
        MAX_DECISION_CONTEXT_BYTES,
    },
    db::{
        inferred_pre_binding_policy_code, AgentDecisionReservation, AgentRunRepository,
        BatchRepository, CampaignRepository, Db, EventRepository, ExperimentRepository,
        DecisionRepository, IncidentRepository, InterventionRepository, ProjectRepository,
        RunLineageRepository, ProposalAcceptance, StartCampaignRequest, SubmissionRepository,
        TaskObservationRepository, TerminationRequestRepository, LATEST_SCHEMA_VERSION,
    },
    diagnostics::{EventFilter, MAX_EVENT_LIST_LIMIT},
    execution_policy::{
        CampaignLimits, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
        ProjectRootAnchor,
    },
    interventions::{
        InterventionStatus, MAX_INTERVENTIONS_PER_RUN, MAX_INTERVENTION_BYTES,
        MAX_INTERVENTION_BYTES_PER_RUN,
    },
    models::{
        AgentRunStatus, BatchJobStatus, BatchStatus, BudgetDimension, BudgetReservation,
        BudgetReservationStatus, Campaign, CampaignState, EventKind, EventStatus,
        DecisionCycleState, ExecutionProjection, Experiment, ExperimentStatus,
        ExperimentTerminalOutcome,
        IncidentStatus, IncidentTransition, NewAgentRun, NewBatchJob, NewBatchRequest, NewEvent,
        NewIncident, NewProject, NewSubmission, NewTaskObservation, NewTerminationRequest, Proposal,
        ProposalKind, ProposalStatus, SubmissionKind, SubmissionStatus, TerminationRequestStatus,
        MAX_EXECUTABLE_IDENTITY_BYTES, MAX_EXECUTABLE_PATH_BYTES,
    },
    proposals::{self, ProposalInput, ValidatedProposal},
    runs::{collect_fresh, FollowCursor},
    retry::{EventResolution, RetryPolicy},
    state::ObjectiveSnapshot,
    AppError,
};
use rusqlite::{params, Connection};
use serde_json::json;
use tempfile::TempDir;

struct TestDatabase {
    _temp: TempDir,
    path: PathBuf,
    db: Db,
}

impl TestDatabase {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("state.sqlite3");
        let db = Db::open(&path).unwrap();
        Self {
            _temp: temp,
            path,
            db,
        }
    }

    fn project_root(&self, name: &str) -> PathBuf {
        let root = self._temp.path().join(name);
        fs::create_dir_all(&root).unwrap();
        root
    }
}

struct CampaignDbHarness {
    test: TestDatabase,
    db: Db,
    project_id: String,
    campaign_id: String,
    experiment_id: String,
}

impl CampaignDbHarness {
    const PROJECT_ID: &'static str = "campaign-project";
    const CAMPAIGN_ID: &'static str = "campaign-1";
    const BASELINE_EXPERIMENT_ID: &'static str = "experiment-baseline";

    fn new() -> Self {
        let test = TestDatabase::new();
        let root = test.project_root("campaign-project");
        register_project(&test.db, Self::PROJECT_ID, &root, "pa-campaign-project");
        Self {
            db: test.db.clone(),
            test,
            project_id: Self::PROJECT_ID.to_owned(),
            campaign_id: Self::CAMPAIGN_ID.to_owned(),
            experiment_id: Self::BASELINE_EXPERIMENT_ID.to_owned(),
        }
    }

    fn objective() -> ObjectiveSnapshot {
        ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "objective-digest".to_owned(),
        }
    }

    fn proposal(
        kind: ProposalKind,
        hypothesis: &str,
        source_experiment_id: Option<&str>,
        argv: &[&str],
    ) -> ValidatedProposal {
        Self::proposal_for_objective(
            kind,
            hypothesis,
            source_experiment_id,
            argv,
            "objective-digest",
        )
    }

    fn proposal_for_objective(
        kind: ProposalKind,
        hypothesis: &str,
        source_experiment_id: Option<&str>,
        argv: &[&str],
        objective_digest: &str,
    ) -> ValidatedProposal {
        Self::proposal_in_directory_for_objective(
            kind,
            hypothesis,
            source_experiment_id,
            argv,
            ".",
            objective_digest,
        )
    }

    fn proposal_in_directory_for_objective(
        kind: ProposalKind,
        hypothesis: &str,
        source_experiment_id: Option<&str>,
        argv: &[&str],
        working_directory: &str,
        objective_digest: &str,
    ) -> ValidatedProposal {
        let input = ProposalInput {
            kind,
            hypothesis: hypothesis.to_owned(),
            source_experiment_id: source_experiment_id.map(str::to_owned),
            argv: argv.iter().map(|argument| (*argument).to_owned()).collect(),
            working_directory: working_directory.to_owned(),
            expected_evidence: vec!["validation loss".to_owned()],
        };
        if source_experiment_id.is_none() {
            proposals::validate_initial_baseline(input, objective_digest).unwrap()
        } else {
            proposals::validate(input, objective_digest).unwrap()
        }
    }

    fn start_with_ids(
        db: &Db,
        campaign_id: &str,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<pueue_agent::db::ManagedSubmissionIntent, AppError> {
        let objective = Self::objective();
        let baseline = Self::proposal(
            ProposalKind::Experiment,
            "Measure the initial command",
            None,
            &["python", "train.py"],
        );
        let initial_argv = baseline.argv().to_vec();
        CampaignRepository::new(db).start_with_baseline(
            StartCampaignRequest {
                campaign_id,
                project_id: Self::PROJECT_ID,
                objective: &objective,
                initial_argv: &initial_argv,
                baseline: &baseline,
                submission_id,
                experiment_id,
                proposal_id,
                metadata: &json!({}),
                origin_agent_run_id: None,
                now,
            },
            limits,
        )
    }

    fn start(&self, limits: &CampaignLimits, now: i64) -> pueue_agent::db::ManagedSubmissionIntent {
        Self::start_with_ids(
            &self.test.db,
            Self::CAMPAIGN_ID,
            "proposal-baseline",
            Self::BASELINE_EXPERIMENT_ID,
            "submission-baseline",
            limits,
            now,
        )
        .unwrap()
    }

    fn accept(
        &self,
        proposal_id: &str,
        experiment_id: &str,
        submission_id: &str,
        proposal: &ValidatedProposal,
        limits: &CampaignLimits,
        now: i64,
    ) -> Result<ProposalAcceptance, AppError> {
        CampaignRepository::new(&self.test.db).accept_proposal(
            Self::CAMPAIGN_ID,
            proposal_id,
            experiment_id,
            submission_id,
            proposal,
            limits,
            now,
        )
    }

    fn finish_baseline(&self, task_id: i64, now: i64, outcome: ExperimentTerminalOutcome<'_>) {
        let repository = ExperimentRepository::new(&self.test.db);
        repository
            .mark_submitting(Self::BASELINE_EXPERIMENT_ID, now)
            .unwrap();
        repository
            .mark_accepted(
                Self::BASELINE_EXPERIMENT_ID,
                task_id,
                "pueue-task:v1:baseline",
                now + 1,
            )
            .unwrap();
        repository
            .project_terminal_submission(
                Self::BASELINE_EXPERIMENT_ID,
                task_id,
                outcome,
                now + 2,
            )
            .unwrap();
    }

    fn with_experiment(status: ExperimentStatus) -> Self {
        let harness = Self::new();
        harness.start(&CampaignLimits::default(), 100);
        let repository = ExperimentRepository::new(&harness.db);
        match status {
            ExperimentStatus::Reserved => {}
            ExperimentStatus::Submitting => {
                repository
                    .mark_submitting(&harness.experiment_id, 101)
                    .unwrap();
            }
            ExperimentStatus::Accepted
            | ExperimentStatus::Succeeded
            | ExperimentStatus::Failed
            | ExperimentStatus::Cancelled => {
                repository
                    .mark_submitting(&harness.experiment_id, 101)
                    .unwrap();
                repository
                    .mark_accepted(
                        &harness.experiment_id,
                        41,
                        "pueue-task:v1:decision-fixture",
                        102,
                    )
                    .unwrap();
                let outcome = match status {
                    ExperimentStatus::Succeeded => Some(ExperimentTerminalOutcome::Succeeded),
                    ExperimentStatus::Failed => Some(ExperimentTerminalOutcome::Failed {
                        failure_code: "exit_nonzero",
                        failure_fingerprint: "decision-fixture-fingerprint",
                    }),
                    ExperimentStatus::Cancelled => Some(ExperimentTerminalOutcome::Cancelled),
                    ExperimentStatus::Accepted => None,
                    _ => unreachable!(),
                };
                if let Some(outcome) = outcome {
                    repository
                        .project_terminal_submission(&harness.experiment_id, 41, outcome, 103)
                        .unwrap();
                }
            }
            ExperimentStatus::Unreconciled => {
                panic!("decision fixture does not create unreconciled experiments")
            }
        }
        harness
    }

    fn with_terminal_experiment(status: ExperimentStatus) -> Self {
        assert!(matches!(
            status,
            ExperimentStatus::Succeeded | ExperimentStatus::Failed | ExperimentStatus::Cancelled
        ));
        Self::with_experiment(status)
    }

    fn reserved_decision_attempt(
        &self,
    ) -> (
        pueue_agent::models::DecisionCycle,
        pueue_agent::db::DecisionReservation,
    ) {
        let repository = DecisionRepository::new(&self.db);
        let cycle = repository
            .ensure_cycle_for_terminal(&self.campaign_id, &self.experiment_id, 190)
            .unwrap();
        let reservation = repository
            .reserve_next_attempt(&self.project_id, &cycle.cycle_id, 191)
            .unwrap()
            .unwrap();
        (cycle, reservation)
    }

    fn scalar(&self, sql: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(sql, [], |row| row.get(0))
            .unwrap()
    }

    fn count(&self, table: &str) -> i64 {
        assert!(matches!(
            table,
            "campaigns" | "proposals" | "experiments" | "budget_reservations" | "submissions"
        ));
        self.test
            .db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))
            .unwrap()
    }

    fn concurrent_start_same_project(
        &self,
    ) -> Vec<Result<pueue_agent::db::ManagedSubmissionIntent, AppError>> {
        let barrier = Arc::new(Barrier::new(2));
        let handles = (0..2)
            .map(|ordinal| {
                let barrier = Arc::clone(&barrier);
                let db = self.test.db.clone();
                thread::spawn(move || {
                    barrier.wait();
                    Self::start_with_ids(
                        &db,
                        &format!("campaign-{ordinal}"),
                        &format!("proposal-{ordinal}"),
                        &format!("experiment-{ordinal}"),
                        &format!("submission-{ordinal}"),
                        &CampaignLimits::default(),
                        1_000,
                    )
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    }
}

fn canonical_v17_campaign_fixture() -> (TempDir, PathBuf) {
    let fixture = V15Fixture::with_submission("legacy-v17-submission");
    let V15Fixture { _temp, path } = fixture;
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(&format!(
            r#"
            {CAMPAIGNS_V16_SQL};
            {PROPOSALS_V16_SQL};
            {EXPERIMENTS_V16_SQL};
            {BUDGET_RESERVATIONS_V16_SQL};
            CREATE UNIQUE INDEX campaigns_one_live_project_idx
                ON campaigns(project_id) WHERE state <> 'retired';
            CREATE INDEX campaigns_state_next_eligible_idx
                ON campaigns(state, next_eligible_at, campaign_id);
            CREATE INDEX proposals_campaign_status_created_idx
                ON proposals(campaign_id, status, created_at, proposal_id);
            CREATE INDEX experiments_campaign_status_created_idx
                ON experiments(campaign_id, status, created_at, experiment_id);
            CREATE INDEX experiments_pueue_task_lookup_idx
                ON experiments(pueue_task_id, task_signature);
            CREATE INDEX budget_reservations_campaign_dimension_window_idx
                ON budget_reservations(campaign_id, dimension, window_started_at, window_ends_at, reservation_id);
            ALTER TABLE events ADD COLUMN campaign_id TEXT
                REFERENCES campaigns(campaign_id) ON DELETE CASCADE;
            ALTER TABLE events ADD COLUMN experiment_id TEXT
                REFERENCES experiments(experiment_id) ON DELETE SET NULL;
            CREATE INDEX events_campaign_status_not_before_idx
                ON events(campaign_id, status, not_before, event_id);
            PRAGMA user_version = 17;
            "#
        ))
        .unwrap();
    drop(connection);
    (_temp, path)
}

mod decision_schema {
    use super::*;

    #[test]
    fn v17_migrates_decision_cycles_and_attempts_atomically() {
        let (_temp, path) = canonical_v17_campaign_fixture();
        let db = Db::open(&path).unwrap();
        let connection = db.connect().unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            18
        );
        assert_eq!(
            table_columns(&connection, "decision_cycles")
                .iter()
                .map(|column| column.0.as_str())
                .collect::<Vec<_>>(),
            vec![
                "cycle_id",
                "campaign_id",
                "source_experiment_id",
                "state",
                "next_wake_at",
                "consecutive_failed_attempts",
                "last_decision_kind",
                "last_failure_code",
                "last_failure_summary",
                "created_at",
                "updated_at"
            ]
        );
        assert_eq!(table_foreign_keys(&connection, "decision_attempts").len(), 2);
    }

    #[test]
    fn current_v18_rejects_extra_event_kind_without_repair() {
        let test = TestDatabase::new();
        test.db
            .connect()
            .unwrap()
            .execute_batch(
                r#"
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master
                   SET sql = replace(
                       sql,
                       '''campaign_decision''',
                       '''campaign_decision'', ''rogue_kind'''
                   )
                 WHERE type = 'table' AND name = 'events';
                PRAGMA writable_schema = OFF;
                "#,
            )
            .unwrap();

        let error = Db::open(&test.path).unwrap_err();
        assert!(matches!(
            error,
            AppError::Runtime {
                operation: "verify SQLite v18 decision schema"
            }
        ));
        let sql: String = Connection::open(&test.path)
            .unwrap()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(sql.contains("'rogue_kind'"));
    }

    #[test]
    fn malformed_v17_event_kind_is_rejected_without_repair() {
        let (_temp, path) = canonical_v17_campaign_fixture();
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                r#"
                PRAGMA writable_schema = ON;
                UPDATE sqlite_master
                   SET sql = replace(
                       sql,
                       '''operator_wake''',
                       '''operator_wake'', ''rogue_kind'''
                   )
                 WHERE type = 'table' AND name = 'events';
                PRAGMA writable_schema = OFF;
                "#,
            )
            .unwrap();

        assert!(Db::open(&path).is_err());
        let sql: String = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(sql.contains("'rogue_kind'"));
    }
}

mod decision_cycle {
    use super::*;

    #[test]
    fn terminal_experiment_creates_one_cycle_and_one_active_attempt() {
        let harness =
            CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Succeeded);
        let first = Db::open(harness.db.path()).unwrap();
        let second = Db::open(harness.db.path()).unwrap();
        let cycle = DecisionRepository::new(&first)
            .ensure_cycle_for_terminal(&harness.campaign_id, &harness.experiment_id, 200)
            .unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let results = std::thread::scope(|scope| {
            [&first, &second]
                .into_iter()
                .map(|db| {
                    let barrier = std::sync::Arc::clone(&barrier);
                    let project_id = harness.project_id.clone();
                    let cycle_id = cycle.cycle_id.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        DecisionRepository::new(db)
                            .reserve_next_attempt(&project_id, &cycle_id, 201)
                            .unwrap()
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|result| result.is_some()).count(), 1);
        assert_eq!(
            harness.scalar("SELECT COUNT(*) FROM decision_attempts"),
            1
        );
    }

    #[test]
    fn cross_campaign_or_nonterminal_decision_lineage_rolls_back() {
        let harness = CampaignDbHarness::with_experiment(ExperimentStatus::Accepted);
        let error = DecisionRepository::new(&harness.db)
            .ensure_cycle_for_terminal(&harness.campaign_id, &harness.experiment_id, 200)
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Validation {
                field: "source_experiment_id",
                ..
            }
        ));
        assert_eq!(harness.scalar("SELECT COUNT(*) FROM decision_cycles"), 0);
    }

    #[test]
    fn valid_wait_resets_failed_attempts_and_requires_finite_wake() {
        let harness = CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Failed);
        let (cycle, attempt) = harness.reserved_decision_attempt();
        let waiting = DecisionRepository::new(&harness.db)
            .mark_waiting(&cycle.cycle_id, attempt.attempt_number, 260, 200)
            .unwrap();
        assert_eq!(waiting.state, DecisionCycleState::Waiting);
        assert_eq!(waiting.next_wake_at, Some(260));
        assert_eq!(waiting.consecutive_failed_attempts, 0);
    }

    #[test]
    fn stale_attempt_cannot_overwrite_a_newer_active_attempt() {
        let harness =
            CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Succeeded);
        let repository = DecisionRepository::new(&harness.db);
        let (cycle, first_attempt) = harness.reserved_decision_attempt();
        repository
            .mark_waiting(&cycle.cycle_id, first_attempt.attempt_number, 260, 200)
            .unwrap();
        assert_eq!(repository.due_cycles(260, 10).unwrap().len(), 1);
        let second_attempt = repository
            .reserve_next_attempt(&harness.project_id, &cycle.cycle_id, 261)
            .unwrap()
            .unwrap();

        let error = repository
            .mark_waiting(&cycle.cycle_id, first_attempt.attempt_number, 300, 262)
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Validation {
                field: "decision_attempt",
                ..
            }
        ));
        let state: DecisionCycleState = harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state FROM decision_cycles WHERE cycle_id = ?1",
                [&cycle.cycle_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, DecisionCycleState::Analyzing);
        assert_eq!(second_attempt.attempt_number, 2);
    }

    #[test]
    fn reserved_attempt_cannot_complete_without_a_proposal_decision() {
        let harness =
            CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Succeeded);
        let (cycle, attempt) = harness.reserved_decision_attempt();

        let error = DecisionRepository::new(&harness.db)
            .mark_completed(&cycle.cycle_id, attempt.attempt_number, 200)
            .unwrap_err();
        assert!(matches!(
            error,
            AppError::Validation {
                field: "decision_attempt",
                ..
            }
        ));
        let state: DecisionCycleState = harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state FROM decision_cycles WHERE cycle_id = ?1",
                [&cycle.cycle_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, DecisionCycleState::Analyzing);
    }

    #[test]
    fn final_attempt_failure_while_paused_stays_paused_and_resume_reveals_degradation() {
        let harness =
            CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Failed);
        let repository = DecisionRepository::new(&harness.db);
        let (cycle, attempt) = harness.reserved_decision_attempt();
        let campaigns = CampaignRepository::new(&harness.db);
        let paused = campaigns.pause(&harness.project_id, 192).unwrap();
        assert_eq!(paused.state, CampaignState::Paused);

        let limits = CampaignLimits {
            max_decision_attempts_per_cycle: 1,
            ..CampaignLimits::default()
        };
        let degraded_cycle = repository
            .fail_attempt(
                None,
                &cycle.cycle_id,
                attempt.attempt_number,
                "decision_missing",
                "analysis produced no decision",
                limits,
                193,
            )
            .unwrap();
        assert_eq!(degraded_cycle.state, DecisionCycleState::Degraded);
        let still_paused = campaigns.find_by_id(&harness.campaign_id).unwrap().unwrap();
        assert_eq!(still_paused.state, CampaignState::Paused);
        assert_eq!(still_paused.state_reason.as_deref(), Some("operator_paused"));

        let resumed = campaigns.resume(&harness.project_id, 194).unwrap();
        assert_eq!(resumed.state, CampaignState::Degraded);
        assert_eq!(
            resumed.state_reason.as_deref(),
            Some("decision_attempts_exhausted")
        );
    }
}

mod decision_context {
    use super::*;

    #[test]
    fn decision_context_is_deterministic_bounded_and_uses_persisted_objective() {
        let harness =
            CampaignDbHarness::with_terminal_experiment(ExperimentStatus::Succeeded);
        let project = ProjectRepository::new(&harness.db)
            .find_by_id(&harness.project_id)
            .unwrap()
            .unwrap();
        fs::write(
            project.root_path.join("STATE.md"),
            "edited STATE objective\nRAW_FULL_LOG_SECRET\n",
        )
        .unwrap();
        fs::create_dir_all(project.root_path.join("metrics")).unwrap();
        fs::write(
            project.root_path.join("metrics/full.log"),
            "RAW_FULL_LOG_SECRET\n",
        )
        .unwrap();
        harness
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns SET objective_text = 'persisted objective' WHERE campaign_id = ?1",
                [&harness.campaign_id],
            )
            .unwrap();
        harness
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE submissions SET metadata_json = ?1 WHERE submission_id = (
                     SELECT submission_id FROM experiments WHERE experiment_id = ?2
                 )",
                params![
                    r#"{"proposal_raw_metadata":"PROPOSAL_RAW_METADATA_SECRET"}"#,
                    harness.experiment_id,
                ],
            )
            .unwrap();
        InterventionRepository::new(&harness.db)
            .insert_pending(
                &harness.project_id,
                "continue carefully OPENAI_API_KEY=CREDENTIAL_ENV_SECRET",
                180,
            )
            .unwrap();

        let (_cycle, reservation) = harness.reserved_decision_attempt();
        let root_anchor = ProjectRootAnchor::resolve(&project.root_path).unwrap();
        let pueue_tasks = [DecisionPueueTaskProjection {
            task_id: 41,
            task_signature: "pueue-task:v1:decision-fixture".to_owned(),
            group: "pa-campaign-project".to_owned(),
            state: "done".to_owned(),
            enqueued_at: Some(100),
            started_at: Some(101),
            ended_at: Some(103),
            exit_code: Some(0),
        }];
        let request = DecisionEvidenceRequest {
            reservation: &reservation,
            root_anchor: &root_anchor,
            pueue_tasks: &pueue_tasks,
            observed_at: 200,
        };
        let builder = DecisionEvidenceBuilder::new(&harness.db);

        let first = builder.build(&request).unwrap();
        let second = builder.build(&request).unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.json, second.json);
        assert_eq!(
            first.digest,
            format!(
                "{:x}",
                <sha2::Sha256 as sha2::Digest>::digest(first.json.as_bytes())
            )
        );
        assert!(first.json.len() <= MAX_DECISION_CONTEXT_BYTES);
        assert!(first.json.contains("persisted objective"));
        assert!(!first.json.contains("edited STATE objective"));
        assert!(!first.json.contains("CREDENTIAL_ENV_SECRET"));
        assert!(!first.json.contains("RAW_FULL_LOG_SECRET"));
        assert!(!first.json.contains("PROPOSAL_RAW_METADATA_SECRET"));

        DecisionRepository::new(&harness.db)
            .store_evidence(&reservation, &first.json, &first.digest, 201)
            .unwrap();
    }
}

struct V15Fixture {
    _temp: TempDir,
    path: PathBuf,
}

impl V15Fixture {
    fn with_submission(submission_id: &str) -> Self {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("schema-v15.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                PRAGMA foreign_keys = ON;

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
                    execution_kind TEXT,
                    executable_path TEXT,
                    executable_identity TEXT,
                    policy_code TEXT,
                    failure_stage TEXT,
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

                CREATE TABLE batch_requests (
                    request_id TEXT PRIMARY KEY CHECK (length(request_id) BETWEEN 1 AND 128),
                    project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                    manifest_hash TEXT NOT NULL CHECK (length(manifest_hash) BETWEEN 1 AND 128),
                    status TEXT NOT NULL CHECK (status IN (
                        'pending', 'dispatching', 'accepted', 'partial', 'failed', 'completed'
                    )),
                    lease_until INTEGER,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    last_error TEXT CHECK (last_error IS NULL OR length(last_error) <= 2048),
                    lease_token TEXT CHECK (lease_token IS NULL OR length(lease_token) <= 128),
                    CHECK (
                        (status = 'dispatching' AND lease_until IS NOT NULL)
                        OR status = 'accepted'
                        OR (status <> 'dispatching' AND lease_until IS NULL)
                    )
                );

                CREATE TABLE batch_jobs (
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

                CREATE TABLE agent_run_id_sequence (
                    sequence_id INTEGER PRIMARY KEY CHECK (sequence_id = 1),
                    last_run_id INTEGER NOT NULL CHECK (
                        last_run_id >= 0 AND last_run_id <= 9223372036854775806
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
                CREATE INDEX agent_run_events_event_idx ON agent_run_events(event_id);
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
                CREATE INDEX batch_requests_project_status_idx
                    ON batch_requests(project_id, status, updated_at, request_id);
                CREATE INDEX batch_requests_lease_idx
                    ON batch_requests(status, lease_until, project_id, request_id);
                CREATE INDEX batch_jobs_request_status_idx
                    ON batch_jobs(request_id, status, ordinal, job_id);

                INSERT INTO agent_run_id_sequence (sequence_id, last_run_id) VALUES (1, 0);
                PRAGMA user_version = 15;
                "#,
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO projects (
                    project_id, root_path, pueue_group, config_path,
                    enabled, paused, created_at, updated_at
                 ) VALUES ('legacy-project', '/tmp/legacy-project', 'pa-legacy-project',
                    '/tmp/legacy-project/config.toml', 1, 0, 100, 100)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at, status, kind, metadata_json
                 ) VALUES (?1, 'legacy-project', '[\"python\",\"train.py\"]', 101,
                    'accepted', 'experiment', '{}')",
                [submission_id],
            )
            .unwrap();
        drop(connection);
        Self { _temp: temp, path }
    }

    fn open_and_migrate(&self) -> Db {
        Db::open(&self.path).unwrap()
    }
}

const CAMPAIGNS_V16_SQL: &str = r#"
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
)
"#;

const PROPOSALS_V16_SQL: &str = r#"
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
)
"#;

const EXPERIMENTS_V16_SQL: &str = r#"
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
)
"#;

const BUDGET_RESERVATIONS_V16_SQL: &str = r#"
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
)
"#;

fn compact_schema_sql(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .to_ascii_lowercase()
}

fn table_columns(connection: &Connection, table: &str) -> Vec<(String, String, i64, i64)> {
    connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(1)?, row.get(2)?, row.get(3)?, row.get(5)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn table_foreign_keys(
    connection: &Connection,
    table: &str,
) -> BTreeSet<(String, String, String, String)> {
    connection
        .prepare(&format!("PRAGMA foreign_key_list({table})"))
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(2)?, row.get(3)?, row.get(4)?, row.get(6)?))
        })
        .unwrap()
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap()
}

fn expected_foreign_keys(
    values: &[(&str, &str, &str, &str)],
) -> BTreeSet<(String, String, String, String)> {
    values
        .iter()
        .map(|(table, from, to, on_delete)| {
            (
                (*table).to_owned(),
                (*from).to_owned(),
                (*to).to_owned(),
                (*on_delete).to_owned(),
            )
        })
        .collect()
}

fn assert_current_campaign_schema_rejected(path: &Path) {
    let error = match Db::open(path) {
        Err(error) => error,
        Ok(_) => panic!("malformed current campaign schema was accepted"),
    };
    assert!(matches!(
        &error,
        AppError::Runtime {
            operation: "verify SQLite v16 campaign schema"
        }
    ));
    let rendered = error.render();
    assert!(rendered.len() <= 240);
    assert!(!rendered.contains(path.to_string_lossy().as_ref()));
}

fn mutate_current_campaign_schema(sql: &str) -> TestDatabase {
    let test = TestDatabase::new();
    test.db.connect().unwrap().execute_batch(sql).unwrap();
    test
}

fn remove_campaign_schema_for_legacy_fixture(connection: &Connection) {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = OFF;
             DROP TABLE budget_reservations;
             DROP TABLE experiments;
             DROP TABLE proposals;
             DROP TABLE campaigns;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();
}

fn register_project(db: &Db, project_id: &str, root: &Path, group: &str) {
    let project = NewProject::new(
        project_id,
        root,
        group,
        root.join(".pueue-agent/config.toml"),
        100,
    );
    ProjectRepository::new(db).register(&project).unwrap();
}

fn insert_event(db: &Db, project_id: &str, dedup_key: &str, not_before: i64) -> i64 {
    let event = NewEvent::new(
        project_id,
        EventKind::TaskFinished,
        dedup_key,
        json!({"task_id": 41}),
        not_before,
        100,
    );
    EventRepository::new(db)
        .insert_idempotent(&event)
        .unwrap()
        .event_id
}

fn bind_starting_run(test: &TestDatabase, dedup_key: &str) -> (i64, i64) {
    let root = test.project_root(dedup_key);
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", dedup_key, 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                format!("/tmp/{dedup_key}.log"),
            ),
            &[event_id],
        )
        .unwrap();
    (run.run_id, event_id)
}

fn create_legacy_schema_without_active_agent_index(path: &Path, version: i64) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(&format!(
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

            PRAGMA user_version = {version};
            "#
        ))
        .unwrap();
}

fn schema_v13_with_run() -> (TempDir, PathBuf) {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("schema-v13.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

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

            CREATE INDEX events_project_status_not_before_idx
                ON events(project_id, status, not_before, event_id);
            CREATE INDEX agent_runs_project_status_idx
                ON agent_runs(project_id, status, started_at);
            CREATE UNIQUE INDEX agent_runs_one_active_per_project_idx
                ON agent_runs(project_id)
                WHERE status IN ('starting', 'running');

            INSERT INTO projects (
                project_id, root_path, pueue_group, config_path,
                enabled, paused, halted_reason, created_at, updated_at
            ) VALUES (
                'v13-projection-project', '/tmp/v13-projection-project',
                'pa-v13-projection', '/tmp/v13-projection-project/config.toml',
                1, 0, NULL, 90, 90
            );
            INSERT INTO events (
                event_id, project_id, kind, dedup_key, payload_json, status,
                attempts, not_before, lease_until, created_at, completed_at, last_error
            ) VALUES (
                7, 'v13-projection-project', 'task_finished', 'v13-run', '{}',
                'completed', 1, 100, NULL, 100, 101, NULL
            );
            INSERT INTO agent_runs (
                run_id, project_id, primary_event_id, pid, status, started_at,
                finished_at, exit_code, log_path, last_error, launch_gate_state,
                context_mode, context_session_id, context_lineage_json
            ) VALUES (
                9, 'v13-projection-project', 7, 4242, 'completed', 101,
                111, 0, '/tmp/v13-projection.log', NULL, 'released',
                'fresh', NULL, '[]'
            );
            INSERT INTO agent_run_events (project_id, run_id, event_id)
            VALUES ('v13-projection-project', 9, 7);

            PRAGMA user_version = 13;
            "#,
        )
        .unwrap();
    drop(connection);
    (temp, path)
}

fn schema_with_execution_projection_columns(
    definitions: &[&str],
    version: i64,
) -> (TempDir, PathBuf) {
    let (temp, path) = schema_v13_with_run();
    let connection = Connection::open(&path).unwrap();
    let alters = definitions
        .iter()
        .map(|definition| format!("ALTER TABLE agent_runs ADD COLUMN {definition};"))
        .collect::<Vec<_>>()
        .join("\n");
    connection
        .execute_batch(&format!("{alters}\nPRAGMA user_version = {version};"))
        .unwrap();
    drop(connection);
    (temp, path)
}

fn execution_projection_column_info(path: &Path) -> Vec<(String, String, i64)> {
    let connection = Connection::open(path).unwrap();
    let mut statement = connection.prepare("PRAGMA table_info(agent_runs)").unwrap();
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    columns
        .into_iter()
        .filter(|(name, _, _)| {
            [
                "execution_kind",
                "executable_path",
                "executable_identity",
                "policy_code",
                "failure_stage",
            ]
            .contains(&name.as_str())
        })
        .collect()
}

fn assert_malformed_execution_projection_rejected(path: &Path) {
    let error = match Db::open(path) {
        Err(error) => error,
        Ok(_) => panic!("malformed execution projection schema was accepted"),
    };
    assert!(matches!(
        &error,
        AppError::Runtime {
            operation: "verify SQLite v14 agent run execution projection schema"
        }
    ));
    let rendered = error.render();
    assert!(rendered.len() <= 240);
    assert!(!rendered.contains(path.to_string_lossy().as_ref()));
}

fn open_v10_operator_log_fixture() -> (TempDir, PathBuf) {
    let temp = TempDir::new().unwrap();
    let path = temp.path().join("operator-logs-v10.sqlite3");
    Db::open(&path).unwrap();

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            r#"
            DROP INDEX operator_logs_project_created_idx;
            DROP TABLE operator_logs;
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
            PRAGMA user_version = 10;
            "#,
        )
        .unwrap();
    for (offset, action) in ["pause", "resume", "halt", "disable", "remove"]
        .into_iter()
        .enumerate()
    {
        connection
            .execute(
                "INSERT INTO operator_logs (
                    project_id, pueue_group, action, details_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    "project-a",
                    "pa-project",
                    action,
                    format!(r#"{{"legacy_action":"{action}"}}"#),
                    100 + offset as i64,
                ],
            )
            .unwrap();
    }
    drop(connection);

    (temp, path)
}

#[test]
fn latest_campaign_schema_installs_exact_tables_constraints_indexes_and_foreign_keys() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();

    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);

    for (table, expected_sql, expected_columns) in [
        (
            "campaigns",
            CAMPAIGNS_V16_SQL,
            vec![
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
        ),
        (
            "proposals",
            PROPOSALS_V16_SQL,
            vec![
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
        ),
        (
            "experiments",
            EXPERIMENTS_V16_SQL,
            vec![
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
        ),
        (
            "budget_reservations",
            BUDGET_RESERVATIONS_V16_SQL,
            vec![
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
        ),
    ] {
        let actual_sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(compact_schema_sql(&actual_sql), compact_schema_sql(expected_sql));
        assert_eq!(
            table_columns(&connection, table),
            expected_columns
                .into_iter()
                .map(|(name, declared_type, not_null, primary_key)| {
                    (
                        name.to_owned(),
                        declared_type.to_owned(),
                        not_null,
                        primary_key,
                    )
                })
                .collect::<Vec<_>>()
        );
    }

    assert_eq!(
        table_foreign_keys(&connection, "campaigns"),
        expected_foreign_keys(&[
            ("projects", "project_id", "project_id", "CASCADE"),
            (
                "experiments",
                "baseline_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ])
    );
    assert_eq!(
        table_foreign_keys(&connection, "proposals"),
        expected_foreign_keys(&[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            (
                "experiments",
                "source_experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ])
    );
    assert_eq!(
        table_foreign_keys(&connection, "experiments"),
        expected_foreign_keys(&[
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
        ])
    );
    assert_eq!(
        table_foreign_keys(&connection, "budget_reservations"),
        expected_foreign_keys(&[
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            (
                "experiments",
                "experiment_id",
                "experiment_id",
                "RESTRICT",
            ),
        ])
    );
    assert_eq!(
        table_foreign_keys(&connection, "events"),
        expected_foreign_keys(&[
            ("projects", "project_id", "project_id", "CASCADE"),
            ("campaigns", "campaign_id", "campaign_id", "CASCADE"),
            (
                "experiments",
                "experiment_id",
                "experiment_id",
                "SET NULL",
            ),
        ])
    );
    let event_columns = table_columns(&connection, "events");
    assert!(event_columns.contains(&("campaign_id".to_owned(), "TEXT".to_owned(), 0, 0)));
    assert!(event_columns.contains(&("experiment_id".to_owned(), "TEXT".to_owned(), 0, 0)));

    for (name, sql) in [
        (
            "campaigns_one_live_project_idx",
            "CREATE UNIQUE INDEX campaigns_one_live_project_idx ON campaigns(project_id) WHERE state <> 'retired'",
        ),
        (
            "campaigns_state_next_eligible_idx",
            "CREATE INDEX campaigns_state_next_eligible_idx ON campaigns(state, next_eligible_at, campaign_id)",
        ),
        (
            "proposals_campaign_status_created_idx",
            "CREATE INDEX proposals_campaign_status_created_idx ON proposals(campaign_id, status, created_at, proposal_id)",
        ),
        (
            "experiments_campaign_status_created_idx",
            "CREATE INDEX experiments_campaign_status_created_idx ON experiments(campaign_id, status, created_at, experiment_id)",
        ),
        (
            "experiments_pueue_task_lookup_idx",
            "CREATE INDEX experiments_pueue_task_lookup_idx ON experiments(pueue_task_id, task_signature)",
        ),
        (
            "budget_reservations_campaign_dimension_window_idx",
            "CREATE INDEX budget_reservations_campaign_dimension_window_idx ON budget_reservations(campaign_id, dimension, window_started_at, window_ends_at, reservation_id)",
        ),
        (
            "events_campaign_status_not_before_idx",
            "CREATE INDEX events_campaign_status_not_before_idx ON events(campaign_id, status, not_before, event_id)",
        ),
    ] {
        let actual_sql: String = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [name],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(compact_schema_sql(&actual_sql), compact_schema_sql(sql));
    }
}

#[test]
fn campaign_models_use_typed_exact_database_values() {
    for (actual, expected) in [
        (CampaignState::Active.as_str(), "active"),
        (CampaignState::BudgetWaiting.as_str(), "budget_waiting"),
        (
            CampaignState::GoalReachedPendingReview.as_str(),
            "goal_reached_pending_review",
        ),
        (CampaignState::Paused.as_str(), "paused"),
        (CampaignState::Degraded.as_str(), "degraded"),
        (CampaignState::Halted.as_str(), "halted"),
        (CampaignState::Retired.as_str(), "retired"),
        (ProposalKind::Experiment.as_str(), "experiment"),
        (ProposalKind::Repair.as_str(), "repair"),
        (ProposalKind::BroaderSearch.as_str(), "broader_search"),
        (ProposalKind::Recipe.as_str(), "recipe"),
        (ProposalKind::CodeChange.as_str(), "code_change"),
        (ProposalKind::DataEvaluation.as_str(), "data_evaluation"),
        (ProposalStatus::Pending.as_str(), "pending"),
        (ProposalStatus::Accepted.as_str(), "accepted"),
        (ProposalStatus::Rejected.as_str(), "rejected"),
        (ExperimentStatus::Reserved.as_str(), "reserved"),
        (ExperimentStatus::Submitting.as_str(), "submitting"),
        (ExperimentStatus::Accepted.as_str(), "accepted"),
        (ExperimentStatus::Unreconciled.as_str(), "unreconciled"),
        (ExperimentStatus::Succeeded.as_str(), "succeeded"),
        (ExperimentStatus::Failed.as_str(), "failed"),
        (ExperimentStatus::Cancelled.as_str(), "cancelled"),
        (BudgetDimension::Experiment.as_str(), "experiment"),
        (BudgetDimension::AgentRun.as_str(), "agent_run"),
        (BudgetDimension::CodeChange.as_str(), "code_change"),
        (BudgetReservationStatus::Reserved.as_str(), "reserved"),
        (BudgetReservationStatus::Consumed.as_str(), "consumed"),
        (BudgetReservationStatus::Released.as_str(), "released"),
    ] {
        assert_eq!(actual, expected);
    }

    let campaign = Campaign {
        campaign_id: "campaign-1".to_owned(),
        project_id: "project-1".to_owned(),
        objective_text: "reduce validation loss".to_owned(),
        objective_digest: "objective-digest".to_owned(),
        initial_argv: vec!["python".to_owned(), "train.py".to_owned()],
        state: CampaignState::Active,
        state_reason: None,
        baseline_experiment_id: Some("experiment-1".to_owned()),
        next_eligible_at: None,
        created_at: 100,
        updated_at: 100,
    };
    let proposal = Proposal {
        proposal_id: "proposal-1".to_owned(),
        campaign_id: campaign.campaign_id.clone(),
        kind: ProposalKind::Experiment,
        status: ProposalStatus::Accepted,
        hypothesis: "baseline".to_owned(),
        source_experiment_id: None,
        argv: campaign.initial_argv.clone(),
        working_directory: ".".to_owned(),
        expected_evidence: vec!["validation loss".to_owned()],
        canonical_digest: "proposal-digest".to_owned(),
        reject_reason: None,
        created_at: 100,
        updated_at: 100,
    };
    let experiment = Experiment {
        experiment_id: "experiment-1".to_owned(),
        campaign_id: campaign.campaign_id.clone(),
        proposal_id: proposal.proposal_id.clone(),
        submission_id: "submission-1".to_owned(),
        parent_experiment_id: None,
        attempt: 0,
        status: ExperimentStatus::Reserved,
        pueue_task_id: None,
        task_signature: None,
        failure_code: None,
        failure_fingerprint: None,
        created_at: 100,
        updated_at: 100,
        finished_at: None,
    };
    let reservation = BudgetReservation {
        reservation_id: "reservation-1".to_owned(),
        campaign_id: campaign.campaign_id.clone(),
        experiment_id: Some(experiment.experiment_id.clone()),
        dimension: BudgetDimension::Experiment,
        subject_key: experiment.experiment_id.clone(),
        status: BudgetReservationStatus::Reserved,
        window_started_at: 100,
        window_ends_at: 200,
        created_at: 100,
        updated_at: 100,
    };

    assert_eq!(proposal.campaign_id, campaign.campaign_id);
    assert_eq!(experiment.proposal_id, proposal.proposal_id);
    assert_eq!(reservation.experiment_id, Some(experiment.experiment_id));
}

#[test]
fn parallel_baseline_start_creates_exactly_one_live_campaign_and_reservation() {
    let harness = CampaignDbHarness::new();
    let results = harness.concurrent_start_same_project();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(harness.count("campaigns"), 1);
    assert_eq!(harness.count("proposals"), 1);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("budget_reservations"), 1);
    assert_eq!(harness.count("submissions"), 1);
}

#[test]
fn campaign_objective_baseline_rejects_a_proposal_validated_for_another_objective() {
    let harness = CampaignDbHarness::new();
    let objective = CampaignDbHarness::objective();
    let baseline = CampaignDbHarness::proposal_for_objective(
        ProposalKind::Experiment,
        "Baseline tied to a changed objective",
        None,
        &["python", "train.py"],
        "changed-objective-digest",
    );
    let initial_argv = baseline.argv().to_vec();

    let error = CampaignRepository::new(&harness.test.db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: CampaignDbHarness::CAMPAIGN_ID,
                project_id: CampaignDbHarness::PROJECT_ID,
                objective: &objective,
                initial_argv: &initial_argv,
                baseline: &baseline,
                submission_id: "submission-baseline",
                experiment_id: CampaignDbHarness::BASELINE_EXPERIMENT_ID,
                proposal_id: "proposal-baseline",
                metadata: &json!({}),
                origin_agent_run_id: None,
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Validation {
            field: "baseline.objective_digest",
            ..
        }
    ));
    assert_eq!(harness.count("campaigns"), 0);
    assert_eq!(harness.count("proposals"), 0);
    assert_eq!(harness.count("experiments"), 0);
    assert_eq!(harness.count("budget_reservations"), 0);
    assert_eq!(harness.count("submissions"), 0);
}

#[test]
fn campaign_objective_acceptance_rejects_a_proposal_validated_for_another_objective() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let proposal = CampaignDbHarness::proposal_for_objective(
        ProposalKind::Experiment,
        "Proposal tied to a changed objective",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
        "changed-objective-digest",
    );

    let error = harness
        .accept(
            "proposal-changed-objective",
            "experiment-changed-objective",
            "submission-changed-objective",
            &proposal,
            &CampaignLimits::default(),
            120,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Validation {
            field: "proposal.objective_digest",
            ..
        }
    ));
    assert_eq!(harness.count("campaigns"), 1);
    assert_eq!(harness.count("proposals"), 1);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("budget_reservations"), 1);
    assert_eq!(harness.count("submissions"), 1);
}

#[test]
fn campaign_atomic_pending_code_change_does_not_consume_an_accepted_cycle_slot() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let code_change = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Change the training implementation later",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v2"],
    );
    assert!(matches!(
        harness
        .accept(
            "proposal-code-pending",
            "experiment-code-pending",
            "submission-code-pending",
            &code_change,
            &CampaignLimits::default(),
            120,
        )
        .unwrap(),
        ProposalAcceptance::PendingCodeChange
    ));
    let code_change_reservations: i64 = harness
        .test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = 'code_change'
               AND subject_key = 'proposal-code-pending' AND status = 'consumed'",
            [CampaignDbHarness::CAMPAIGN_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(code_change_reservations, 1);

    let experiment = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Accept an experiment from the same decision source",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );
    let accepted = harness
        .accept(
            "proposal-after-code",
            "experiment-after-code",
            "submission-after-code",
            &experiment,
            &CampaignLimits::default(),
            121,
        )
        .unwrap()
        .accepted()
        .unwrap();

    assert_eq!(accepted.proposal.status, ProposalStatus::Accepted);
    assert_eq!(harness.count("proposals"), 3);
    assert_eq!(harness.count("experiments"), 2);
    assert_eq!(harness.count("budget_reservations"), 3);
    assert_eq!(harness.count("submissions"), 2);
}

#[test]
fn campaign_atomic_parallel_limit_one_acceptance_creates_one_experiment() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);

    let mut setup_limits = CampaignLimits::default();
    setup_limits.max_parallel_experiments = 2;
    setup_limits.max_proposals_per_cycle = 2;
    let seed = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Create a second completed source",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--seed", "2"],
    );
    let seed_intent = harness
        .accept(
            "proposal-seed",
            "experiment-seed",
            "submission-seed",
            &seed,
            &setup_limits,
            120,
        )
        .unwrap()
        .accepted()
        .unwrap();
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(&seed_intent.experiment.experiment_id, 121)
        .unwrap();
    experiments
        .mark_accepted(
            &seed_intent.experiment.experiment_id,
            42,
            "pueue-task:v1:seed",
            122,
        )
        .unwrap();
    experiments
        .project_terminal_submission(
            &seed_intent.experiment.experiment_id,
            42,
            ExperimentTerminalOutcome::Succeeded,
            123,
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let proposals = [
        CampaignDbHarness::proposal(
            ProposalKind::Experiment,
            "First parallel contender",
            Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
            &["python", "train.py", "--lr", "0.1"],
        ),
        CampaignDbHarness::proposal(
            ProposalKind::Experiment,
            "Second parallel contender",
            Some("experiment-seed"),
            &["python", "train.py", "--lr", "0.2"],
        ),
    ];
    let handles = proposals
        .into_iter()
        .enumerate()
        .map(|(ordinal, proposal)| {
            let db = harness.test.db.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let limits = CampaignLimits::default();
                barrier.wait();
                CampaignRepository::new(&db).accept_proposal(
                    CampaignDbHarness::CAMPAIGN_ID,
                    &format!("proposal-race-{ordinal}"),
                    &format!("experiment-race-{ordinal}"),
                    &format!("submission-race-{ordinal}"),
                    &proposal,
                    &limits,
                    130,
                )
            })
        })
        .collect::<Vec<_>>();
    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(harness.count("proposals"), 3);
    assert_eq!(harness.count("experiments"), 3);
    assert_eq!(harness.count("submissions"), 3);
    assert_eq!(harness.count("budget_reservations"), 3);
}

#[test]
fn rolling_budget_reopens_at_exact_24_hour_boundary() {
    const DAY: i64 = 24 * 60 * 60;
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_new_experiments_per_24h = 1;
    harness.start(&limits, 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let proposal = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Use the released rolling slot",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );

    assert!(matches!(
        harness
        .accept(
            "proposal-before-boundary",
            "experiment-before-boundary",
            "submission-before-boundary",
            &proposal,
            &limits,
            100 + DAY - 1,
        )
        .unwrap(),
        ProposalAcceptance::BudgetWaiting {
            next_eligible_at: 86_500
        }
    ));
    assert!(CampaignRepository::new(&harness.test.db)
        .wake_eligible_campaigns(100 + DAY)
        .unwrap()
        .contains(&CampaignDbHarness::CAMPAIGN_ID.to_owned()));
    let accepted = harness
        .accept(
            "proposal-at-boundary",
            "experiment-at-boundary",
            "submission-at-boundary",
            &proposal,
            &limits,
            100 + DAY,
        )
        .unwrap()
        .accepted()
        .unwrap();
    assert_eq!(accepted.experiment.experiment_id, "experiment-at-boundary");
}

#[test]
fn positive_experiment_budget_exhaustion_enters_a_finite_wait_state() {
    const DAY: i64 = 24 * 60 * 60;
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_new_experiments_per_24h = 1;
    harness.start(&limits, 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let proposal = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Wait for the next rolling experiment slot",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );

    let _ = harness.accept(
        "proposal-wait",
        "experiment-wait",
        "submission-wait",
        &proposal,
        &limits,
        100 + DAY - 1,
    );

    let campaign = CampaignRepository::new(&harness.test.db)
        .find_by_id(CampaignDbHarness::CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::BudgetWaiting);
    assert_eq!(campaign.next_eligible_at, Some(100 + DAY));
    assert_eq!(
        campaign.state_reason.as_deref(),
        Some("experiment_budget_exhausted")
    );
    assert_eq!(harness.count("proposals"), 1);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("submissions"), 1);
}

#[test]
fn campaign_atomic_duplicate_digest_returns_existing_intent_without_new_rows() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let proposal = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Try a lower learning rate",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );
    let first = harness
        .accept(
            "proposal-first",
            "experiment-first",
            "submission-first",
            &proposal,
            &CampaignLimits::default(),
            120,
        )
        .unwrap()
        .accepted()
        .unwrap();
    let duplicate = harness
        .accept(
            "proposal-duplicate",
            "experiment-duplicate",
            "submission-duplicate",
            &proposal,
            &CampaignLimits::default(),
            121,
        )
        .unwrap()
        .accepted()
        .unwrap();

    assert_eq!(duplicate.proposal.proposal_id, first.proposal.proposal_id);
    assert_eq!(duplicate.experiment.experiment_id, first.experiment.experiment_id);
    assert_eq!(duplicate.submission.submission_id, first.submission.submission_id);
    assert_eq!(harness.count("proposals"), 2);
    assert_eq!(harness.count("experiments"), 2);
    assert_eq!(harness.count("budget_reservations"), 2);
    assert_eq!(harness.count("submissions"), 2);
}

#[test]
fn campaign_atomic_submission_insert_failure_rolls_back_all_owned_rows() {
    let harness = CampaignDbHarness::new();
    harness
        .test
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_campaign_submission
             BEFORE INSERT ON submissions
             BEGIN
                 SELECT RAISE(ABORT, 'injected campaign submission failure');
             END;",
        )
        .unwrap();

    assert!(CampaignDbHarness::start_with_ids(
        &harness.test.db,
        CampaignDbHarness::CAMPAIGN_ID,
        "proposal-baseline",
        CampaignDbHarness::BASELINE_EXPERIMENT_ID,
        "submission-baseline",
        &CampaignLimits::default(),
        100,
    )
    .is_err());
    assert_eq!(harness.count("campaigns"), 0);
    assert_eq!(harness.count("proposals"), 0);
    assert_eq!(harness.count("experiments"), 0);
    assert_eq!(harness.count("budget_reservations"), 0);
    assert_eq!(harness.count("submissions"), 0);
}

#[test]
fn campaign_atomic_project_state_is_revalidated_before_baseline_writes() {
    let harness = CampaignDbHarness::new();
    ProjectRepository::new(&harness.test.db)
        .pause(CampaignDbHarness::PROJECT_ID, 99)
        .unwrap();

    assert!(CampaignDbHarness::start_with_ids(
        &harness.test.db,
        CampaignDbHarness::CAMPAIGN_ID,
        "proposal-baseline",
        CampaignDbHarness::BASELINE_EXPERIMENT_ID,
        "submission-baseline",
        &CampaignLimits::default(),
        100,
    )
    .is_err());
    assert_eq!(harness.count("campaigns"), 0);
    assert_eq!(harness.count("proposals"), 0);
    assert_eq!(harness.count("experiments"), 0);
    assert_eq!(harness.count("budget_reservations"), 0);
    assert_eq!(harness.count("submissions"), 0);
}

#[test]
fn campaign_atomic_campaign_state_is_revalidated_before_proposal_writes() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    harness
        .test
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET state = 'paused' WHERE campaign_id = ?1",
            [CampaignDbHarness::CAMPAIGN_ID],
        )
        .unwrap();
    let proposal = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "This proposal must not race a pause",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );

    assert!(harness
        .accept(
            "proposal-paused",
            "experiment-paused",
            "submission-paused",
            &proposal,
            &CampaignLimits::default(),
            120,
        )
        .is_err());
    assert_eq!(harness.count("proposals"), 1);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("budget_reservations"), 1);
    assert_eq!(harness.count("submissions"), 1);
}

#[test]
fn campaign_agent_decision_never_reuses_a_reservation_while_paused() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let repository = CampaignRepository::new(&harness.test.db);
    assert!(matches!(
        repository
            .reserve_agent_decision(
                CampaignDbHarness::CAMPAIGN_ID,
                "existing-decision",
                &CampaignLimits::default(),
                101,
            )
            .unwrap(),
        AgentDecisionReservation::Reserved(_)
    ));
    repository
        .pause(CampaignDbHarness::PROJECT_ID, 102)
        .unwrap();

    let existing = repository
        .reserve_agent_decision(
            CampaignDbHarness::CAMPAIGN_ID,
            "existing-decision",
            &CampaignLimits::default(),
            103,
        )
        .unwrap();
    let unique = repository
        .reserve_agent_decision(
            CampaignDbHarness::CAMPAIGN_ID,
            "unique-decision",
            &CampaignLimits::default(),
            103,
        )
        .unwrap();

    assert!(!matches!(existing, AgentDecisionReservation::Reserved(_)));
    assert!(!matches!(unique, AgentDecisionReservation::Reserved(_)));
    assert_eq!(
        harness
            .test
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                 WHERE campaign_id = ?1 AND dimension = 'agent_run'",
                [CampaignDbHarness::CAMPAIGN_ID],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn reserved_submission_cannot_begin_after_campaign_or_project_authority_is_lost() {
    let campaign_paused = CampaignDbHarness::new();
    campaign_paused.start(&CampaignLimits::default(), 100);
    CampaignRepository::new(&campaign_paused.test.db)
        .pause(CampaignDbHarness::PROJECT_ID, 101)
        .unwrap();
    assert!(ExperimentRepository::new(&campaign_paused.test.db)
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 102)
        .is_err());
    assert_eq!(
        ExperimentRepository::new(&campaign_paused.test.db)
            .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );

    let project_paused = CampaignDbHarness::new();
    project_paused.start(&CampaignLimits::default(), 100);
    ProjectRepository::new(&project_paused.test.db)
        .pause(CampaignDbHarness::PROJECT_ID, 101)
        .unwrap();
    assert!(ExperimentRepository::new(&project_paused.test.db)
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 102)
        .is_err());
    assert_eq!(
        ExperimentRepository::new(&project_paused.test.db)
            .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );
}

#[test]
fn campaign_atomic_cross_campaign_source_is_rejected_without_any_insert() {
    let first = CampaignDbHarness::new();
    first.start(&CampaignLimits::default(), 100);
    first.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let second_root = first.test.project_root("campaign-project-b");
    register_project(
        &first.test.db,
        "campaign-project-b",
        &second_root,
        "pa-campaign-project-b",
    );
    let objective = CampaignDbHarness::objective();
    let baseline = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Second campaign baseline",
        None,
        &["python", "other.py"],
    );
    let initial_argv = baseline.argv().to_vec();
    let second = CampaignRepository::new(&first.test.db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: "campaign-2",
                project_id: "campaign-project-b",
                objective: &objective,
                initial_argv: &initial_argv,
                baseline: &baseline,
                submission_id: "submission-campaign-2",
                experiment_id: "experiment-campaign-2",
                proposal_id: "proposal-campaign-2",
                metadata: &json!({}),
                origin_agent_run_id: None,
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap();
    let second_experiments = ExperimentRepository::new(&first.test.db);
    second_experiments
        .mark_submitting(&second.experiment.experiment_id, 110)
        .unwrap();
    second_experiments
        .mark_accepted(
            &second.experiment.experiment_id,
            42,
            "pueue-task:v1:second",
            111,
        )
        .unwrap();
    second_experiments
        .project_terminal_submission(
            &second.experiment.experiment_id,
            42,
            ExperimentTerminalOutcome::Succeeded,
            112,
        )
        .unwrap();
    let before = (
        first.count("proposals"),
        first.count("experiments"),
        first.count("budget_reservations"),
        first.count("submissions"),
    );
    let proposal = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Cross-campaign lineage must fail closed",
        Some("experiment-campaign-2"),
        &["python", "train.py", "--lr", "0.02"],
    );

    assert!(first
        .accept(
            "proposal-cross-campaign",
            "experiment-cross-campaign",
            "submission-cross-campaign",
            &proposal,
            &CampaignLimits::default(),
            120,
        )
        .is_err());
    assert_eq!(
        (
            first.count("proposals"),
            first.count("experiments"),
            first.count("budget_reservations"),
            first.count("submissions"),
        ),
        before
    );
}

#[test]
fn campaign_atomic_proposal_cycle_limit_rejects_before_insert() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let first = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "First decision",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.01"],
    );
    harness
        .accept(
            "proposal-cycle-first",
            "experiment-cycle-first",
            "submission-cycle-first",
            &first,
            &CampaignLimits::default(),
            120,
        )
        .unwrap();
    let second = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Second decision from the same source",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--lr", "0.02"],
    );

    assert!(harness
        .accept(
            "proposal-cycle-second",
            "experiment-cycle-second",
            "submission-cycle-second",
            &second,
            &CampaignLimits::default(),
            121,
        )
        .is_err());
    assert_eq!(harness.count("proposals"), 2);
    assert_eq!(harness.count("experiments"), 2);
}

#[test]
fn campaign_atomic_same_spec_retry_limit_is_finite() {
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_same_spec_retries = 1;
    harness.start(&limits, 100);
    harness.finish_baseline(
        41,
        110,
        ExperimentTerminalOutcome::Failed {
            failure_code: "exit_nonzero",
            failure_fingerprint: "fingerprint-a",
        },
    );
    let retry = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "Retry the exact baseline spec",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py"],
    );
    let retry_intent = harness
        .accept(
            "proposal-retry-1",
            "experiment-retry-1",
            "submission-retry-1",
            &retry,
            &limits,
            120,
        )
        .unwrap()
        .accepted()
        .unwrap();
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(&retry_intent.experiment.experiment_id, 121)
        .unwrap();
    experiments
        .mark_accepted(
            &retry_intent.experiment.experiment_id,
            42,
            "pueue-task:v1:retry-1",
            122,
        )
        .unwrap();
    experiments
        .project_terminal_submission(
            &retry_intent.experiment.experiment_id,
            42,
            ExperimentTerminalOutcome::Failed {
                failure_code: "exit_nonzero",
                failure_fingerprint: "fingerprint-a",
            },
            123,
        )
        .unwrap();
    let excess = CampaignDbHarness::proposal(
        ProposalKind::Experiment,
        "A second exact retry must be rejected",
        Some("experiment-retry-1"),
        &["python", "train.py"],
    );

    assert!(harness
        .accept(
            "proposal-retry-2",
            "experiment-retry-2",
            "submission-retry-2",
            &excess,
            &limits,
            130,
        )
        .is_err());
    assert_eq!(harness.count("experiments"), 2);
}

#[test]
fn same_argv_in_a_different_working_directory_is_a_distinct_specification() {
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_same_spec_retries = 0;
    harness.start(&limits, 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let proposal = CampaignDbHarness::proposal_in_directory_for_objective(
        ProposalKind::Experiment,
        "Run the same command in a distinct normalized directory",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py"],
        "variant",
        "objective-digest",
    );

    let accepted = harness
        .accept(
            "proposal-directory",
            "experiment-directory",
            "submission-directory",
            &proposal,
            &limits,
            120,
        )
        .unwrap()
        .accepted()
        .unwrap();

    assert_eq!(accepted.proposal.working_directory, "variant");
    assert_eq!(accepted.experiment.attempt, 0);
}

#[test]
fn campaign_atomic_repair_limit_uses_trusted_source_fingerprint() {
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_repairs_per_failure_fingerprint = 1;
    harness.start(&limits, 100);
    harness.finish_baseline(
        41,
        110,
        ExperimentTerminalOutcome::Failed {
            failure_code: "oom",
            failure_fingerprint: "trusted-oom-fingerprint",
        },
    );
    let repair = CampaignDbHarness::proposal(
        ProposalKind::Repair,
        "Reduce the batch size",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--batch-size", "8"],
    );
    let repair_intent = harness
        .accept(
            "proposal-repair-1",
            "experiment-repair-1",
            "submission-repair-1",
            &repair,
            &limits,
            120,
        )
        .unwrap()
        .accepted()
        .unwrap();
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(&repair_intent.experiment.experiment_id, 121)
        .unwrap();
    experiments
        .mark_accepted(
            &repair_intent.experiment.experiment_id,
            42,
            "pueue-task:v1:repair-1",
            122,
        )
        .unwrap();
    experiments
        .project_terminal_submission(
            &repair_intent.experiment.experiment_id,
            42,
            ExperimentTerminalOutcome::Failed {
                failure_code: "oom",
                failure_fingerprint: "trusted-oom-fingerprint",
            },
            123,
        )
        .unwrap();
    let excess = CampaignDbHarness::proposal(
        ProposalKind::Repair,
        "A second repair for the same failure is capped",
        Some("experiment-repair-1"),
        &["python", "train.py", "--batch-size", "4"],
    );

    assert!(harness
        .accept(
            "proposal-repair-2",
            "experiment-repair-2",
            "submission-repair-2",
            &excess,
            &limits,
            130,
        )
        .is_err());
    assert_eq!(harness.count("experiments"), 2);
}

#[test]
fn rolling_budget_code_change_pending_proposals_consume_exact_window_slots() {
    const DAY: i64 = 24 * 60 * 60;
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_code_change_proposals_per_24h = 1;
    limits.max_proposals_per_cycle = 3;
    harness.start(&limits, 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let first = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Change the training implementation",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v2"],
    );
    assert!(matches!(
        harness
        .accept(
            "proposal-code-1",
            "experiment-code-1",
            "submission-code-1",
            &first,
            &limits,
            120,
        )
        .unwrap(),
        ProposalAcceptance::PendingCodeChange
    ));
    let before_boundary = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Another code change before expiry",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v3"],
    );
    assert!(matches!(
        harness
        .accept(
            "proposal-code-2",
            "experiment-code-2",
            "submission-code-2",
            &before_boundary,
            &limits,
            120 + DAY - 1,
        )
        .unwrap(),
        ProposalAcceptance::BudgetWaiting {
            next_eligible_at: 86_520
        }
    ));
    assert!(CampaignRepository::new(&harness.test.db)
        .wake_eligible_campaigns(120 + DAY)
        .unwrap()
        .contains(&CampaignDbHarness::CAMPAIGN_ID.to_owned()));
    let at_boundary = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Code change after exact expiry",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v4"],
    );
    assert!(matches!(
        harness
        .accept(
            "proposal-code-3",
            "experiment-code-3",
            "submission-code-3",
            &at_boundary,
            &limits,
            120 + DAY,
        )
        .unwrap(),
        ProposalAcceptance::PendingCodeChange
    ));
    assert_eq!(harness.count("proposals"), 3);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("submissions"), 1);
    assert_eq!(harness.count("budget_reservations"), 3);
}

#[test]
fn positive_code_change_budget_exhaustion_enters_a_finite_wait_state() {
    const DAY: i64 = 24 * 60 * 60;
    let harness = CampaignDbHarness::new();
    let mut limits = CampaignLimits::default();
    limits.max_code_change_proposals_per_24h = 1;
    limits.max_proposals_per_cycle = 3;
    harness.start(&limits, 100);
    harness.finish_baseline(41, 110, ExperimentTerminalOutcome::Succeeded);
    let first = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Use the only code-change slot",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v2"],
    );
    harness
        .accept(
            "proposal-code-first",
            "experiment-code-first",
            "submission-code-first",
            &first,
            &limits,
            120,
        )
        .unwrap();
    let second = CampaignDbHarness::proposal(
        ProposalKind::CodeChange,
        "Wait for another code-change slot",
        Some(CampaignDbHarness::BASELINE_EXPERIMENT_ID),
        &["python", "train.py", "--implementation", "v3"],
    );

    let _ = harness.accept(
        "proposal-code-wait",
        "experiment-code-wait",
        "submission-code-wait",
        &second,
        &limits,
        120 + DAY - 1,
    );

    let campaign = CampaignRepository::new(&harness.test.db)
        .find_by_id(CampaignDbHarness::CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::BudgetWaiting);
    assert_eq!(campaign.next_eligible_at, Some(120 + DAY));
    assert_eq!(
        campaign.state_reason.as_deref(),
        Some("code_change_budget_exhausted")
    );
    assert_eq!(harness.count("proposals"), 2);
    assert_eq!(harness.count("experiments"), 1);
    assert_eq!(harness.count("submissions"), 1);
}

#[test]
fn campaign_atomic_external_transitions_are_exact_and_idempotent() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);

    let submitting = experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 110)
        .unwrap();
    assert_eq!(submitting.status, ExperimentStatus::Submitting);
    assert!(experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 111)
        .is_err());

    let accepted = experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "pueue-task:v1:baseline",
            112,
        )
        .unwrap();
    assert_eq!(accepted.status, ExperimentStatus::Accepted);
    let replay = experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "pueue-task:v1:baseline",
            113,
        )
        .unwrap();
    assert_eq!(replay.updated_at, accepted.updated_at);
    assert!(experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            99,
            "pueue-task:v1:conflict",
            114,
        )
        .is_err());
    let stored = experiments
        .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(stored.pueue_task_id, Some(41));
    let submission = SubmissionRepository::new(&harness.test.db)
        .find_by_id("submission-baseline")
        .unwrap()
        .unwrap();
    assert_eq!(submission.pueue_task_id, Some(41));
    assert_eq!(submission.status, SubmissionStatus::Accepted);
}

#[test]
fn campaign_atomic_unreconciled_transition_updates_both_rows_and_replays_exactly() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 110)
        .unwrap();
    let unreconciled = experiments
        .mark_unreconciled(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            "pueue_add_timeout",
            111,
        )
        .unwrap();
    let replay = experiments
        .mark_unreconciled(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            "pueue_add_timeout",
            112,
        )
        .unwrap();

    assert_eq!(unreconciled.status, ExperimentStatus::Unreconciled);
    assert_eq!(replay.updated_at, unreconciled.updated_at);
    assert!(experiments
        .mark_unreconciled(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            "different_reason",
            113,
        )
        .is_err());
    let submission = SubmissionRepository::new(&harness.test.db)
        .find_by_id("submission-baseline")
        .unwrap()
        .unwrap();
    assert_eq!(submission.status, SubmissionStatus::Unreconciled);
}

#[test]
fn campaign_atomic_terminal_projection_consumes_reservation_and_rejects_conflicts() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 110)
        .unwrap();
    experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "pueue-task:v1:baseline",
            111,
        )
        .unwrap();
    let terminal = experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            ExperimentTerminalOutcome::Failed {
                failure_code: "exit_nonzero",
                failure_fingerprint: "failure-fingerprint",
            },
            112,
        )
        .unwrap();
    let replay = experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            ExperimentTerminalOutcome::Failed {
                failure_code: "exit_nonzero",
                failure_fingerprint: "failure-fingerprint",
            },
            113,
        )
        .unwrap();

    assert_eq!(terminal.status, ExperimentStatus::Failed);
    assert_eq!(terminal.failure_fingerprint.as_deref(), Some("failure-fingerprint"));
    assert_eq!(replay.updated_at, terminal.updated_at);
    assert!(experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            ExperimentTerminalOutcome::Succeeded,
            114,
        )
        .is_err());
    assert!(experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            99,
            ExperimentTerminalOutcome::Failed {
                failure_code: "exit_nonzero",
                failure_fingerprint: "failure-fingerprint",
            },
            115,
        )
        .is_err());
    let reservation_status: BudgetReservationStatus = harness
        .test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [CampaignDbHarness::BASELINE_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_status, BudgetReservationStatus::Consumed);
}

#[test]
fn campaign_atomic_terminal_projection_revalidates_owned_submission_identity() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 110)
        .unwrap();
    experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "pueue-task:v1:baseline",
            111,
        )
        .unwrap();
    SubmissionRepository::new(&harness.test.db)
        .mark_accepted("submission-baseline", 99, "pueue-task:v1:drifted")
        .unwrap();

    assert!(experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            ExperimentTerminalOutcome::Succeeded,
            112,
        )
        .is_err());
    assert_eq!(
        experiments
            .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
    let reservation_status: BudgetReservationStatus = harness
        .test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [CampaignDbHarness::BASELINE_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_status, BudgetReservationStatus::Reserved);
}

#[test]
fn v15_migrates_campaign_tables_without_claiming_legacy_submissions() {
    let fixture = V15Fixture::with_submission("legacy-submission");
    let _db = fixture.open_and_migrate();
    let connection = Connection::open(&fixture.path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let campaigns: i64 = connection
        .query_row("SELECT COUNT(*) FROM campaigns", [], |row| row.get(0))
        .unwrap();
    let legacy: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM submissions WHERE submission_id = 'legacy-submission'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let projects: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM projects WHERE project_id = 'legacy-project'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    assert_eq!(campaigns, 0);
    assert_eq!(legacy, 1);
    assert_eq!(projects, 1);
}

#[test]
fn v16_migration_quarantines_accepted_managed_provisional_identities() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 101)
        .unwrap();
    experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "provisional-submit:v1:legacy-managed",
            102,
        )
        .unwrap();
    let submissions = SubmissionRepository::new(&harness.test.db);
    for (submission_id, kind, task_id, task_signature) in [
        (
            "standalone-control",
            SubmissionKind::Control,
            42,
            "provisional-submit:v1:standalone-control",
        ),
        (
            "standalone-batch-job",
            SubmissionKind::Experiment,
            43,
            "provisional-submit:v1:standalone-batch-job",
        ),
    ] {
        submissions
            .insert_idempotent(&NewSubmission::with_kind_metadata(
                submission_id,
                CampaignDbHarness::PROJECT_ID,
                vec!["python".to_owned(), "ordinary.py".to_owned()],
                103,
                kind,
                json!({"source": "ordinary"}),
                None,
            ))
            .unwrap();
        submissions
            .mark_accepted(submission_id, task_id, task_signature)
            .unwrap();
    }
    harness
        .test
        .db
        .connect()
        .unwrap()
        .execute_batch("PRAGMA user_version = 16;")
        .unwrap();

    Db::open(&harness.test.path).unwrap();

    let experiment = experiments
        .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
        .unwrap()
        .unwrap();
    let submission = SubmissionRepository::new(&harness.test.db)
        .find_by_id("submission-baseline")
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
    assert_eq!(
        experiment.failure_code.as_deref(),
        Some("legacy_provisional_task_identity")
    );
    assert_eq!(submission.status, SubmissionStatus::Unreconciled);
    let reservation_status: BudgetReservationStatus = harness
        .test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [CampaignDbHarness::BASELINE_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_status, BudgetReservationStatus::Reserved);
    for (submission_id, task_id, task_signature) in [
        (
            "standalone-control",
            42,
            "provisional-submit:v1:standalone-control",
        ),
        (
            "standalone-batch-job",
            43,
            "provisional-submit:v1:standalone-batch-job",
        ),
    ] {
        let ordinary = submissions.find_by_id(submission_id).unwrap().unwrap();
        assert_eq!(ordinary.status, SubmissionStatus::Accepted);
        assert_eq!(ordinary.pueue_task_id, Some(task_id));
        assert_eq!(ordinary.task_signature.as_deref(), Some(task_signature));
    }
}

#[test]
fn v16_migration_quarantines_terminal_managed_provisional_identity_fail_closed() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 101)
        .unwrap();
    experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "provisional-submit:v1:legacy-terminal",
            102,
        )
        .unwrap();
    experiments
        .project_terminal_submission(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            ExperimentTerminalOutcome::Failed {
                failure_code: "exit_nonzero",
                failure_fingerprint: "legacy-fingerprint",
            },
            103,
        )
        .unwrap();
    harness
        .test
        .db
        .connect()
        .unwrap()
        .execute_batch("PRAGMA user_version = 16;")
        .unwrap();

    Db::open(&harness.test.path).unwrap();

    let experiment = experiments
        .find_by_id(CampaignDbHarness::BASELINE_EXPERIMENT_ID)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
    assert_eq!(
        experiment.failure_code.as_deref(),
        Some("legacy_provisional_task_identity")
    );
    assert_eq!(experiment.failure_fingerprint, None);
    let submission = SubmissionRepository::new(&harness.test.db)
        .find_by_id("submission-baseline")
        .unwrap()
        .unwrap();
    assert_eq!(submission.status, SubmissionStatus::Unreconciled);
    let reservation_status: BudgetReservationStatus = harness
        .test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE experiment_id = ?1",
            [CampaignDbHarness::BASELINE_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_status, BudgetReservationStatus::Consumed);
}

#[test]
fn current_schema_verifier_rejects_a_linked_accepted_provisional_submission() {
    let harness = CampaignDbHarness::new();
    harness.start(&CampaignLimits::default(), 100);
    let experiments = ExperimentRepository::new(&harness.test.db);
    experiments
        .mark_submitting(CampaignDbHarness::BASELINE_EXPERIMENT_ID, 101)
        .unwrap();
    experiments
        .mark_accepted(
            CampaignDbHarness::BASELINE_EXPERIMENT_ID,
            41,
            "provisional-submit:v1:linked-verifier-gap",
            102,
        )
        .unwrap();
    harness
        .test
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE experiments
             SET status = 'unreconciled', failure_code = 'fixture'
             WHERE experiment_id = ?1",
            [CampaignDbHarness::BASELINE_EXPERIMENT_ID],
        )
        .unwrap();

    assert!(matches!(
        Db::open(&harness.test.path),
        Err(AppError::Runtime {
            operation: "verify SQLite v17 managed task identity quarantine",
        })
    ));
}

#[test]
fn campaign_schema_v15_migration_rolls_back_every_new_object_on_failure() {
    let fixture = V15Fixture::with_submission("rollback-submission");
    Connection::open(&fixture.path)
        .unwrap()
        .execute_batch("CREATE TABLE proposals (wrong_column TEXT);")
        .unwrap();

    assert!(Db::open(&fixture.path).is_err());

    let connection = Connection::open(&fixture.path).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        15
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'campaigns'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM submissions WHERE submission_id = 'rollback-submission'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn campaign_schema_current_v16_rejects_a_missing_state_check() {
    let test = mutate_current_campaign_schema(
        r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(
               sql,
               'state TEXT NOT NULL CHECK (state IN (
        ''active'',''budget_waiting'',''goal_reached_pending_review'',''paused'',
        ''degraded'',''halted'',''retired''
    ))',
               'state TEXT NOT NULL'
           )
         WHERE type = 'table' AND name = 'campaigns';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 16;
        "#,
    );

    assert_current_campaign_schema_rejected(&test.path);
}

#[test]
fn campaign_schema_current_v16_rejects_case_changed_state_literal_without_repair() {
    let test = mutate_current_campaign_schema(
        r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(sql, '''active''', '''ACTIVE''')
         WHERE type = 'table' AND name = 'campaigns';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 16;
        "#,
    );

    assert_current_campaign_schema_rejected(&test.path);

    let campaign_sql: String = Connection::open(&test.path)
        .unwrap()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'campaigns'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(campaign_sql.contains("'ACTIVE'"));
}

#[test]
fn campaign_schema_current_v16_rejects_wrong_nullability() {
    let test = mutate_current_campaign_schema(
        r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(sql, 'objective_text TEXT NOT NULL', 'objective_text TEXT')
         WHERE type = 'table' AND name = 'campaigns';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 16;
        "#,
    );

    assert_current_campaign_schema_rejected(&test.path);
}

#[test]
fn campaign_schema_current_v16_rejects_missing_partial_unique_index_without_repair() {
    let test = mutate_current_campaign_schema(
        "DROP INDEX IF EXISTS campaigns_one_live_project_idx; PRAGMA user_version = 16;",
    );

    assert_current_campaign_schema_rejected(&test.path);

    let connection = Connection::open(&test.path).unwrap();
    let index_exists: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'index' AND name = 'campaigns_one_live_project_idx'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!index_exists);
}

#[test]
fn campaign_schema_current_v16_rejects_case_changed_partial_index_literal_without_repair() {
    let test = mutate_current_campaign_schema(
        r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(sql, '''retired''', '''RETIRED''')
         WHERE type = 'index' AND name = 'campaigns_one_live_project_idx';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 16;
        "#,
    );

    assert_current_campaign_schema_rejected(&test.path);

    let index_sql: String = Connection::open(&test.path)
        .unwrap()
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index'
             AND name = 'campaigns_one_live_project_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(index_sql.contains("'RETIRED'"));
}

#[test]
fn campaign_schema_current_v16_rejects_missing_foreign_key() {
    let test = mutate_current_campaign_schema(
        r#"
        PRAGMA writable_schema = ON;
        UPDATE sqlite_master
           SET sql = replace(
               sql,
               'project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE',
               'project_id TEXT NOT NULL'
           )
         WHERE type = 'table' AND name = 'campaigns';
        PRAGMA writable_schema = OFF;
        PRAGMA user_version = 16;
        "#,
    );

    assert_current_campaign_schema_rejected(&test.path);
}

#[test]
fn open_configures_sqlite_and_installs_all_tables() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();

    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .unwrap();
    let busy_timeout: i64 = connection
        .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert!(busy_timeout >= 5_000);

    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap();
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for required in [
        "projects",
        "events",
        "integration_events",
        "incidents",
        "agent_runs",
        "agent_run_id_sequence",
        "agent_run_events",
        "submissions",
        "termination_requests",
        "task_observations",
        "operator_logs",
        "interventions",
        "batch_requests",
        "batch_jobs",
    ] {
        assert!(
            names.iter().any(|name| name == required),
            "missing {required}"
        );
    }

    let intervention_columns = connection
        .prepare("PRAGMA table_info(interventions)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(intervention_columns
        .iter()
        .any(|name| name == "insertion_sequence"));
    let sequence_index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'interventions_project_sequence_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sequence_index_count, 1);

    drop(statement);
    drop(connection);
    Db::open(&test.path).unwrap();
}

#[test]
fn latest_schema_rejects_agent_run_sequence_below_existing_runs() {
    let test = TestDatabase::new();
    let (run_id, _) = bind_starting_run(&test, "invalid-run-id-sequence");
    assert!(run_id > 0);
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_run_id_sequence SET last_run_id = 0 WHERE sequence_id = 1",
            [],
        )
        .unwrap();
    let error = Db::open(&test.path).unwrap_err();
    assert!(matches!(
        error,
        AppError::Runtime {
            operation: "validate SQLite agent run ID sequence"
        }
    ), "unexpected error: {error:?}");
}

#[test]
fn v14_to_v15_seeds_committed_run_high_water_and_allocates_the_next_id() {
    let test = TestDatabase::new();
    let (first_run_id, first_event_id) = bind_starting_run(&test, "v15-sequence-seed");
    assert_eq!(first_run_id, 1);
    test.db
        .connect()
        .unwrap()
        .execute_batch(&format!(
            "UPDATE agent_runs SET status = 'completed', finished_at = 120 WHERE run_id = {first_run_id};
             UPDATE events SET status = 'completed', completed_at = 120 WHERE event_id = {first_event_id};
             DROP TABLE agent_run_id_sequence;
             PRAGMA user_version = 14;"
        ))
        .unwrap();

    let migrated = Db::open(&test.path).unwrap();
    let seeded: i64 = migrated
        .connect()
        .unwrap()
        .query_row(
            "SELECT last_run_id FROM agent_run_id_sequence WHERE sequence_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(seeded, first_run_id);

    let second_event_id = insert_event(&migrated, "project-a", "v15-next-run", 130);
    let second_run = AgentRunRepository::new(&migrated)
        .insert(&NewAgentRun::new(
            "project-a",
            second_event_id,
            None,
            AgentRunStatus::Starting,
            140,
            "/tmp/v15-next-run.log",
        ))
        .unwrap();
    assert_eq!(second_run.run_id, first_run_id + 1);
}

#[test]
fn operator_log_migration_preserves_rows_and_allows_cancel() {
    let (_database, path) = open_v10_operator_log_fixture();

    let db = Db::open(&path).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);

    let rows = connection
        .prepare(
            "SELECT action, details_json FROM operator_logs
             ORDER BY created_at, log_id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("pause".to_owned(), r#"{"legacy_action":"pause"}"#.to_owned()),
            ("resume".to_owned(), r#"{"legacy_action":"resume"}"#.to_owned()),
            ("halt".to_owned(), r#"{"legacy_action":"halt"}"#.to_owned()),
            ("disable".to_owned(), r#"{"legacy_action":"disable"}"#.to_owned()),
            ("remove".to_owned(), r#"{"legacy_action":"remove"}"#.to_owned()),
        ]
    );

    let index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'operator_logs_project_created_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(index_count, 1);

    connection
        .execute(
            "INSERT INTO operator_logs (
                project_id, pueue_group, action, details_json, created_at
             ) VALUES (?1, ?2, 'cancel', ?3, ?4)",
            params!["project-a", "pa-project", "{}", 200],
        )
        .unwrap();
}

#[test]
fn task_cancellation_log_persists_bounded_redacted_details() {
    let test = TestDatabase::new();
    let root = test.project_root("cancel-log-project");
    let project = NewProject::new(
        "project-a",
        &root,
        "pa-project",
        root.join(".pueue-agent/config.toml"),
        100,
    );
    let project = ProjectRepository::new(&test.db).register(&project).unwrap();

    ProjectRepository::new(&test.db)
        .record_task_cancellation(
            &project,
            41,
            "signature --token SIGNATURE_SECRET",
            "Running --token REQUESTED_SECRET",
            "kill",
            "Canceled --token FINAL_SECRET",
            &format!(
                "operator request --token REASON_SECRET {}",
                "x".repeat(400)
            ),
            200,
        )
        .unwrap();

    let (stored_project, stored_group, action, details_json, created_at):
        (String, String, String, String, i64) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT project_id, pueue_group, action, details_json, created_at
             FROM operator_logs",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(stored_project, "project-a");
    assert_eq!(stored_group, "pa-project");
    assert_eq!(action, "cancel");
    assert_eq!(created_at, 200);

    let details: serde_json::Value = serde_json::from_str(&details_json).unwrap();
    assert_eq!(details["task_id"], 41);
    assert_eq!(details["action"], "kill");
    for key in ["task_signature", "requested_state", "final_state", "reason"] {
        let value = details[key].as_str().unwrap();
        assert!(value.len() <= 240, "{key} was not bounded: {value}");
        assert!(!value.contains("SECRET"), "{key} leaked a secret: {value}");
        assert!(value.contains("[REDACTED]"), "{key} was not redacted: {value}");
    }
}

#[test]
fn v7_event_check_migrates_to_v8_preserving_events_foreign_keys_and_indexes() {
    let test = TestDatabase::new();
    let root = test.project_root("v7-project");
    register_project(&test.db, "v7-project", &root, "pa-v7-project");
    insert_event(&test.db, "v7-project", "before-v8", 100);
    let connection = test.db.connect().unwrap();
    remove_campaign_schema_for_legacy_fixture(&connection);
    connection.execute_batch(
        "PRAGMA writable_schema = ON;
         UPDATE sqlite_master
            SET sql = replace(sql, '''termination_failed'', ''operator_wake''', '''termination_failed''')
          WHERE type = 'table' AND name = 'events';
         PRAGMA writable_schema = OFF;
         PRAGMA user_version = 7;",
    ).unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    connection.execute(
        "INSERT INTO events (project_id, kind, dedup_key, payload_json, status, attempts, not_before, created_at)
         VALUES ('v7-project', 'operator_wake', 'wake-v8', '{}', 'pending', 0, 100, 100)",
        [],
    ).unwrap();
    connection.execute(
        "INSERT INTO events (project_id, kind, dedup_key, payload_json, status, attempts, not_before, created_at)
         VALUES ('v7-project', 'task_finished', 'finished-v8', '{}', 'pending', 0, 100, 100)",
        [],
    ).unwrap();
    let event_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE project_id = 'v7-project'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 3);
    assert!(connection.execute("INSERT INTO events (project_id, kind, dedup_key, payload_json, status, attempts, not_before, created_at) VALUES ('missing', 'operator_wake', 'foreign', '{}', 'pending', 0, 100, 100)", []).is_err());
    for index in ["events_claimable_idx", "events_project_status_idx"] {
        let found: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(found, 1, "missing {index}");
    }
    drop(connection);
    Db::open(&test.path).unwrap();
}

#[test]
fn schema_v12_migration_adds_event_run_ack_states_and_rejects_unknown_status() {
    let test = TestDatabase::new();
    let root = test.project_root("v12-project");
    register_project(&test.db, "v12-project", &root, "pa-v12-project");
    let event_id = insert_event(&test.db, "v12-project", "before-v13", 100);
    let connection = test.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'",
            params![
                "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter'",
                "'pending', 'claimed', 'completed', 'retry_wait', 'failed'",
            ],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = OFF; PRAGMA user_version = 12;")
        .unwrap();
    drop(connection);
    let before = test.db.connect().unwrap().query_row(
        "SELECT project_id, kind, dedup_key, payload_json, status, attempts,
                not_before, lease_until, created_at, completed_at, last_error
         FROM events WHERE event_id = ?1",
        [event_id],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
            ))
        },
    )
    .unwrap();
    let before_version: i64 = test
        .db
        .connect()
        .unwrap()
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(before_version, 12);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    let event_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'events'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    for status in [
        "pending",
        "claimed",
        "in_flight",
        "dispatched",
        "completed",
        "retry_wait",
        "failed",
        "dead_letter",
    ] {
        assert!(
            event_sql.contains(&format!("'{status}'")),
            "missing event status {status}"
        );
    }
    let error = connection
        .execute(
            "INSERT INTO events (
                 project_id, kind, dedup_key, payload_json, status, attempts, not_before, created_at
             ) VALUES ('v12-project', 'task_finished', 'unknown-status', '{}', 'unknown', 0, 100, 100)",
            [],
        )
        .unwrap_err();
    assert!(error.to_string().contains("CHECK constraint failed"));

    let after = connection
        .query_row(
            "SELECT project_id, kind, dedup_key, payload_json, status, attempts,
                    not_before, lease_until, created_at, completed_at, last_error
             FROM events WHERE event_id = ?1",
            [event_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(after, before);
    for index in [
        "events_claimable_idx",
        "events_project_status_idx",
        "events_project_status_not_before_idx",
    ] {
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                [index],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "missing {index}");
    }
}

#[test]
fn fresh_schema_creates_events_project_status_not_before_index() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    let index_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'events_project_status_not_before_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        index_sql
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase(),
        "create index events_project_status_not_before_idx on events(project_id, status, not_before, event_id)"
    );
}

#[test]
fn v14_adds_projection_and_preserves_v13_rows() {
    let (_temp, path) = schema_v13_with_run();
    let pre_migration = Connection::open(&path).unwrap();
    let columns = pre_migration
        .prepare("PRAGMA table_info(agent_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!columns.iter().any(|column| {
        [
            "execution_kind",
            "executable_path",
            "executable_identity",
            "policy_code",
            "failure_stage",
        ]
        .contains(&column.as_str())
    }));
    assert_eq!(
        pre_migration
            .query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        13
    );
    drop(pre_migration);

    let migrated = Db::open(&path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    let columns = connection
        .prepare("PRAGMA table_info(agent_runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for name in [
        "execution_kind",
        "executable_path",
        "executable_identity",
        "policy_code",
        "failure_stage",
    ] {
        assert!(columns.iter().any(|column| column == name), "missing {name}");
    }
    for forbidden in ["prompt", "argv", "environment", "credentials"] {
        assert!(
            !columns.iter().any(|column| column.contains(forbidden)),
            "unexpected secret-bearing column {forbidden}"
        );
    }
    let preserved: (i64, String, i64, Option<String>, Option<String>, Option<String>) = connection
        .query_row(
            "SELECT run_id, status, started_at,
                    execution_kind, executable_path, executable_identity
             FROM agent_runs WHERE run_id = 9",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        preserved,
        (9, "completed".to_owned(), 101, None, None, None)
    );
    let preserved_link: (String, i64, i64) = connection
        .query_row(
            "SELECT project_id, run_id, event_id FROM agent_run_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        preserved_link,
        ("v13-projection-project".to_owned(), 9, 7)
    );
    let foreign_key_violations: i64 = connection
        .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(foreign_key_violations, 0);
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    for index in [
        "agent_runs_project_status_idx",
        "agent_runs_one_active_per_project_idx",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1
                 )",
                [index],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "missing {index}");
    }
}

#[test]
fn current_v14_rejects_non_nullable_execution_projection_column() {
    let (_temp, path) = schema_with_execution_projection_columns(
        &[
            "execution_kind TEXT NOT NULL DEFAULT ''",
            "executable_path TEXT",
            "executable_identity TEXT",
            "policy_code TEXT",
            "failure_stage TEXT",
        ],
        14,
    );

    assert_malformed_execution_projection_rejected(&path);
}

#[test]
fn current_v14_rejects_non_text_execution_projection_column() {
    let (_temp, path) = schema_with_execution_projection_columns(
        &[
            "execution_kind TEXT",
            "executable_path TEXT",
            "executable_identity BLOB",
            "policy_code TEXT",
            "failure_stage TEXT",
        ],
        14,
    );

    assert_malformed_execution_projection_rejected(&path);
}

#[test]
fn malformed_partial_projection_rolls_back_without_adding_missing_columns() {
    let (_temp, path) = schema_with_execution_projection_columns(
        &[
            "execution_kind TEXT NOT NULL DEFAULT ''",
            "executable_path TEXT",
        ],
        13,
    );

    assert_malformed_execution_projection_rejected(&path);

    let connection = Connection::open(&path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 13);
    assert_eq!(
        execution_projection_column_info(&path),
        vec![
            ("execution_kind".to_owned(), "TEXT".to_owned(), 1),
            ("executable_path".to_owned(), "TEXT".to_owned(), 0),
        ]
    );
}

#[test]
fn current_v14_repairs_missing_columns_when_existing_projection_shape_is_valid() {
    let (_temp, path) = schema_with_execution_projection_columns(
        &["execution_kind text", "executable_path TeXt"],
        14,
    );

    Db::open(&path).unwrap();

    let columns = execution_projection_column_info(&path);
    assert_eq!(columns.len(), 5);
    for (_, declared_type, not_null) in columns {
        assert!(declared_type.eq_ignore_ascii_case("TEXT"));
        assert_eq!(not_null, 0);
    }
}

#[test]
fn execution_projection_round_trips_through_binding_transaction_and_legacy_runs_are_empty() {
    let test = TestDatabase::new();
    let root = test.project_root("projection-round-trip");
    register_project(&test.db, "project-a", &root, "pa-projection-round-trip");
    let event_id = insert_event(&test.db, "project-a", "projection-round-trip", 100);
    EventRepository::new(&test.db).claim_batch(100, 200, 1).unwrap();
    let projection = ExecutionProjection::new(
        "codex",
        "/trusted/bin/codex",
        "device=1;inode=2;owner=3;mode=493",
    )
    .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/projection-round-trip.log",
            )
            .with_execution(projection.clone()),
            &[event_id],
        )
        .unwrap();
    assert_eq!(run.execution_kind.as_deref(), Some("codex"));
    assert_eq!(run.executable_path.as_deref(), Some("/trusted/bin/codex"));
    assert_eq!(run.executable_identity.as_deref(), Some("device=1;inode=2;owner=3;mode=493"));
    let linked = AgentRunRepository::new(&test.db)
        .find_by_event("project-a", event_id, 4)
        .unwrap();
    assert_eq!(linked, vec![run.clone()]);
    assert_eq!(
        AgentRunRepository::new(&test.db)
            .find_active_by_project("project-a")
            .unwrap(),
        Some(run.clone())
    );
    assert_eq!(
        AgentRunRepository::new(&test.db)
            .list_by_project("project-a", 4)
            .unwrap(),
        vec![run.clone()]
    );
    let lineage = RunLineageRepository::new(&test.db)
        .list_by_project("project-a", 4)
        .unwrap()
        .into_iter()
        .find(|lineage| lineage.run_id == Some(run.run_id))
        .unwrap();
    assert_eq!(lineage.execution_kind.as_deref(), Some("codex"));
    assert_eq!(lineage.executable_path.as_deref(), Some("/trusted/bin/codex"));
    assert_eq!(
        lineage.executable_identity.as_deref(),
        Some("device=1;inode=2;owner=3;mode=493")
    );

    let legacy_event_id = insert_event(&test.db, "project-a", "legacy-projection", 101);
    let legacy = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            legacy_event_id,
            None,
            AgentRunStatus::Completed,
            111,
            "/tmp/legacy-projection.log",
        ))
        .unwrap();
    assert_eq!(legacy.execution_kind, None);
    assert_eq!(legacy.executable_path, None);
    assert_eq!(legacy.executable_identity, None);

    let direct_event_id = insert_event(&test.db, "project-a", "direct-projection", 102);
    let direct = AgentRunRepository::new(&test.db)
        .insert(
            &NewAgentRun::new(
                "project-a",
                direct_event_id,
                None,
                AgentRunStatus::Completed,
                112,
                "/tmp/direct-projection.log",
            )
            .with_execution(
                ExecutionProjection::new(
                    "custom",
                    "/trusted/bin/custom",
                    "device=4;inode=5;owner=6;mode=493",
                )
                .unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(direct.execution_kind.as_deref(), Some("custom"));
    assert_eq!(direct.executable_path.as_deref(), Some("/trusted/bin/custom"));
    assert_eq!(
        direct.executable_identity.as_deref(),
        Some("device=4;inode=5;owner=6;mode=493")
    );
}

#[test]
fn policy_blocked_counts_are_project_scoped_and_exclude_other_errors() {
    let test = TestDatabase::new();
    let root_a = test.project_root("policy-count-a");
    let root_b = test.project_root("policy-count-b");
    register_project(&test.db, "project-a", &root_a, "pa-policy-count-a");
    register_project(&test.db, "project-b", &root_b, "pb-policy-count-b");
    let events = EventRepository::new(&test.db);
    let project_a_policy = insert_event(&test.db, "project-a", "policy-count-a", 100);
    let project_a_other = insert_event(&test.db, "project-a", "policy-count-other", 100);
    let forged_pending = insert_event(&test.db, "project-a", "policy-count-forged", 100);
    let project_b_policy = insert_event(&test.db, "project-b", "policy-count-b", 100);
    events.claim_batch(100, 200, 8).unwrap();
    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );
    events
        .dead_letter_claimed_without_run("project-a", &[project_a_policy], 101, &violation)
        .unwrap();
    events
        .dead_letter_claimed_without_run("project-b", &[project_b_policy], 101, &violation)
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', lease_until = NULL, last_error = 'ordinary failure' WHERE event_id = ?1",
            [project_a_other],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'pending', lease_until = NULL, last_error = 'policy_blocked:unsafe_codex_argument' WHERE event_id = ?1",
            [forged_pending],
        )
        .unwrap();
    assert!(inferred_pre_binding_policy_code(
        &events.find_by_id(forged_pending).unwrap().unwrap(),
        false,
    )
    .is_none());

    assert_eq!(
        events.policy_blocked_counts("project-a").unwrap(),
        BTreeMap::from([("unsafe_codex_argument".to_owned(), 1)]),
    );
    assert_eq!(
        events.policy_blocked_counts("project-b").unwrap(),
        BTreeMap::from([("unsafe_codex_argument".to_owned(), 1)]),
    );
    let pre_binding = RunLineageRepository::new(&test.db)
        .list_by_project("project-a", 8)
        .unwrap()
        .into_iter()
        .find(|lineage| lineage.event_id == Some(project_a_policy))
        .unwrap();
    assert_eq!(pre_binding.run_id, None);
    assert_eq!(pre_binding.policy_code.as_deref(), Some("unsafe_codex_argument"));
    assert_eq!(pre_binding.failure_stage.as_deref(), Some("pre_binding"));
}

#[test]
fn bounded_run_lineages_do_not_infer_pre_binding_for_linked_older_runs() {
    let test = TestDatabase::new();
    let root = test.project_root("linked-older-run");
    register_project(&test.db, "project-a", &root, "pa-linked-older-run");
    let linked_event = insert_event(&test.db, "project-a", "linked-policy-event", 300);
    let newer_run_event = insert_event(&test.db, "project-a", "newer-run-event", 200);
    EventRepository::new(&test.db).claim_batch(400, 500, 8).unwrap();
    let runs = AgentRunRepository::new(&test.db);
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-a", linked_event, None, AgentRunStatus::Completed, 100,
            root.join(".pueue-agent/logs/linked.log"),
        ),
        &[linked_event],
    )
    .unwrap();
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-a", newer_run_event, None, AgentRunStatus::Starting, 200,
            root.join(".pueue-agent/logs/newer.log"),
        ),
        &[newer_run_event],
    )
    .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET created_at = 300, status = 'dead_letter', last_error = 'policy_blocked:unsafe_codex_argument' WHERE event_id = ?1",
            [linked_event],
        )
        .unwrap();
    assert!(inferred_pre_binding_policy_code(
        &EventRepository::new(&test.db)
            .find_by_id(linked_event)
            .unwrap()
            .unwrap(),
        true,
    )
    .is_none());

    let lineages = RunLineageRepository::new(&test.db)
        .list_by_project("project-a", 1)
        .unwrap();
    assert!(lineages.iter().all(|lineage| lineage.event_id != Some(linked_event)));
}

#[test]
fn execution_projection_rejects_ambiguous_or_oversized_audit_facts() {
    let exact_path = format!("/{}", "実".repeat((MAX_EXECUTABLE_PATH_BYTES - 1) / 3));
    assert_eq!(exact_path.len(), MAX_EXECUTABLE_PATH_BYTES);
    let exact_identity = format!("i{}", "実".repeat((MAX_EXECUTABLE_IDENTITY_BYTES - 1) / 3));
    assert_eq!(exact_identity.len(), MAX_EXECUTABLE_IDENTITY_BYTES);

    let exact = ExecutionProjection::new("custom", &exact_path, &exact_identity).unwrap();
    assert_eq!(exact.execution_kind(), "custom");
    assert_eq!(exact.executable_path(), exact_path);
    assert_eq!(exact.executable_identity(), exact_identity);

    for invalid in [
        ExecutionProjection::new("unknown", "/trusted/bin/agent", "identity"),
        ExecutionProjection::new("codex", "relative/agent", "identity"),
        ExecutionProjection::new("codex", "", "identity"),
        ExecutionProjection::new(
            "codex",
            format!("{exact_path}x"),
            exact_identity.clone(),
        ),
        ExecutionProjection::new(
            "codex",
            exact_path.clone(),
            format!("{exact_identity}x"),
        ),
        ExecutionProjection::new("codex", "/trusted/bin/agent\0spoofed", "identity"),
        ExecutionProjection::new("codex", "/trusted/bin/agent", "identity\nspoofed"),
    ] {
        assert!(invalid.is_err());
    }
}

#[test]
fn policy_finalization_persists_only_bounded_policy_fields_and_preserves_projection() {
    let test = TestDatabase::new();
    let root = test.project_root("projection-policy-finalization");
    register_project(&test.db, "project-a", &root, "pa-projection-policy-finalization");
    let event_id = insert_event(&test.db, "project-a", "projection-policy-finalization", 100);
    EventRepository::new(&test.db).claim_batch(100, 200, 1).unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/projection-policy-finalization.log",
            )
            .with_execution(
                ExecutionProjection::new(
                    "custom",
                    "/trusted/bin/custom",
                    "device=9;inode=8;owner=7;mode=493",
                )
                .unwrap(),
            ),
            &[event_id],
        )
        .unwrap();
    AgentRunRepository::new(&test.db)
        .fail_before_gate_release_with_policy(
            "project-a",
            run.run_id,
            120,
            "ignored policy detail",
            PolicyViolation::new(
                PolicyViolationCode::UnsafeCodexArgument,
                PolicyViolationStage::RunBoundPreMarker,
            ),
        )
        .unwrap();
    let values: (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT execution_kind, executable_path, executable_identity, policy_code, failure_stage
             FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        values,
        (
            Some("custom".to_owned()),
            Some("/trusted/bin/custom".to_owned()),
            Some("device=9;inode=8;owner=7;mode=493".to_owned()),
            Some("unsafe_codex_argument".to_owned()),
            Some("run_bound_pre_marker".to_owned()),
        )
    );
}

#[test]
fn current_v13_reopen_repairs_missing_event_status_not_before_index() {
    let test = TestDatabase::new();
    test.db
        .connect()
        .unwrap()
        .execute("DROP INDEX events_project_status_not_before_idx", [])
        .unwrap();

    Db::open(&test.path).unwrap();

    let exists: bool = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'index' AND name = 'events_project_status_not_before_idx'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(exists);
}

#[test]
fn latest_campaign_schema_repair_preserves_user_version() {
    let test = TestDatabase::new();
    test.db
        .connect()
        .unwrap()
        .execute("DROP INDEX events_project_status_not_before_idx", [])
        .unwrap();

    Db::open(&test.path).unwrap();

    let connection = Connection::open(&test.path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let index_exists: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_master
                 WHERE type = 'index' AND name = 'events_project_status_not_before_idx'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    assert!(index_exists);
}

#[test]
fn current_v13_reopen_repairs_malformed_event_status_not_before_index() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    connection
        .execute("DROP INDEX events_project_status_not_before_idx", [])
        .unwrap();
    connection
        .execute(
            "CREATE INDEX events_project_status_not_before_idx
             ON events(project_id, status, event_id)",
            [],
        )
        .unwrap();
    drop(connection);

    Db::open(&test.path).unwrap();

    let index_sql: String = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT sql FROM sqlite_master
             WHERE type = 'index' AND name = 'events_project_status_not_before_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        index_sql
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase(),
        "create index events_project_status_not_before_idx on events(project_id, status, not_before, event_id)"
    );
}

#[test]
fn current_v13_reopen_rejects_malformed_events_status_check() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'",
            params![
                "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter'",
                "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter', 'unexpected'",
            ],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = OFF; PRAGMA user_version = 13;")
        .unwrap();
    drop(connection);

    assert!(Db::open(&test.path).is_err());
}

#[test]
fn transition_many_rejects_ack_owned_states_before_sql_and_redacts_legacy_errors() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "legacy-transition", 100);
    let repository = EventRepository::new(&test.db);

    for status in [
        EventStatus::InFlight,
        EventStatus::Dispatched,
        EventStatus::DeadLetter,
    ] {
        assert!(matches!(
            repository.transition_many(&[], status, 101, None, None),
            Err(AppError::Validation { .. })
        ));
        assert!(matches!(
            repository.transition_many(&[event_id], status, 101, None, None),
            Err(AppError::Validation { .. })
        ));
        assert_eq!(
            repository.find_by_id(event_id).unwrap().unwrap().status,
            EventStatus::Pending
        );
    }

    let reason = format!(
        "legacy failure --password SECRET [31m{}\u{0007}",
        "detail ".repeat(100)
    );
    repository
        .transition_many(
            &[event_id],
            EventStatus::Failed,
            101,
            None,
            Some(&reason),
        )
        .unwrap();
    let stored = repository
        .find_by_id(event_id)
        .unwrap()
        .unwrap()
        .last_error
        .unwrap();
    assert!(stored.len() <= 240);
    assert!(stored.contains("[REDACTED]"));
    assert!(!stored.contains("SECRET"));
    assert!(!stored.chars().any(char::is_control));
}

#[test]
fn reservation_token_attachment_requires_unbound_rows_and_attaches_all_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let first = interventions
        .insert_pending("project-a", "first", 100)
        .unwrap();
    let second = interventions
        .insert_pending("project-a", "second", 101)
        .unwrap();
    let reservation = interventions
        .reserve_pending("project-a", "multi-row-token", 102, 300, 2, 1024)
        .unwrap();
    assert_eq!(reservation.items.len(), 2);

    let first_event = insert_event(&test.db, "project-a", "reservation-first", 100);
    let second_event = insert_event(&test.db, "project-a", "reservation-second", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 300, 2)
        .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                first_event,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/multi-row-reservation.log",
            ),
            &[first_event, second_event],
            Some("multi-row-token"),
        )
        .unwrap();
    let attached: Vec<(String, Option<i64>)> = test
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT intervention_id, agent_run_id FROM interventions
             WHERE intervention_id IN (?1, ?2) ORDER BY intervention_id",
        )
        .unwrap()
        .query_map(params![first.intervention_id, second.intervention_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(attached.len(), 2);
    assert!(attached
        .iter()
        .all(|(_, agent_run_id)| *agent_run_id == Some(run.run_id)));
    assert!(attached
        .iter()
        .any(|(intervention_id, _)| intervention_id == &first.intervention_id));
    assert!(attached
        .iter()
        .any(|(intervention_id, _)| intervention_id == &second.intervention_id));

    let conflict = TestDatabase::new();
    let conflict_root = conflict.project_root("project");
    register_project(&conflict.db, "project-a", &conflict_root, "pa-project");
    let conflict_intervention = InterventionRepository::new(&conflict.db)
        .insert_pending("project-a", "conflicting", 100)
        .unwrap();
    InterventionRepository::new(&conflict.db)
        .reserve_pending("project-a", "reused-token", 101, 300, 1, 1024)
        .unwrap();
    let owner_event = insert_event(&conflict.db, "project-a", "owner-event", 100);
    let owner_run = AgentRunRepository::new(&conflict.db)
        .insert(&NewAgentRun::new(
            "project-a",
            owner_event,
            None,
            AgentRunStatus::Completed,
            102,
            "/tmp/reservation-owner.log",
        ))
        .unwrap();
    conflict
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET agent_run_id = ?1 WHERE intervention_id = ?2",
            params![owner_run.run_id, conflict_intervention.intervention_id],
        )
        .unwrap();
    conflict
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'completed', completed_at = 103
             WHERE event_id = ?1",
            [owner_event],
        )
        .unwrap();
    let new_event = insert_event(&conflict.db, "project-a", "conflict-event", 100);
    EventRepository::new(&conflict.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let result = AgentRunRepository::new(&conflict.db).insert_with_events_and_reservation(
        &NewAgentRun::new(
            "project-a",
            new_event,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/reservation-conflict.log",
        ),
        &[new_event],
        Some("reused-token"),
    );
    assert!(matches!(result, Err(AppError::Validation { .. })));
    let state: (EventStatus, i64) = conflict
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT events.status,
                    (SELECT COUNT(*) FROM agent_runs WHERE primary_event_id = ?1)
             FROM events WHERE event_id = ?1",
            [new_event],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Claimed, 0));
    let owner_id: Option<i64> = conflict
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_run_id FROM interventions WHERE intervention_id = ?1",
            [conflict_intervention.intervention_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(owner_id, Some(owner_run.run_id));
}

#[test]
fn schema_v12_migration_rejects_extra_event_status_even_when_ack_literals_are_present() {
    let test = TestDatabase::new();
    let root = test.project_root("v12-malformed-project");
    register_project(
        &test.db,
        "v12-malformed-project",
        &root,
        "pa-v12-malformed-project",
    );
    insert_event(&test.db, "v12-malformed-project", "malformed-v13", 100);

    let connection = test.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE sqlite_master
                SET sql = replace(sql, ?1, ?2)
              WHERE type = 'table' AND name = 'events'",
            params![
                "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter'",
                "'pending', 'claimed', 'in_flight', 'dispatched',\n                    'completed', 'retry_wait', 'failed', 'dead_letter', 'unexpected'",
            ],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA writable_schema = OFF; PRAGMA user_version = 12;")
        .unwrap();
    drop(connection);

    assert!(
        Db::open(&test.path).is_err(),
        "migration must reject a non-canonical event status CHECK"
    );
}

#[test]
fn readonly_open_does_not_migrate_or_create_database_state() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    connection
        .execute("DROP INDEX events_project_status_idx", [])
        .unwrap();
    let before_schema_cookie: i64 = connection
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    let before_user_version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let before_index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'events_project_status_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before_index_count, 0);

    let readonly = Db::open_read_only(&test.path).unwrap();
    let readonly_connection = readonly.connect().unwrap();
    let _: i64 = readonly_connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    drop(readonly_connection);

    let after = test.db.connect().unwrap();
    let after_schema_cookie: i64 = after
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    let after_user_version: i64 = after
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let after_index_count: i64 = after
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'events_project_status_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(after_schema_cookie, before_schema_cookie);
    assert_eq!(after_user_version, before_user_version);
    assert_eq!(after_index_count, before_index_count);

    let missing = test._temp.path().join("missing/state.sqlite3");
    assert!(Db::open_read_only(&missing).is_err());
    assert!(!missing.exists());
}

#[test]
fn readonly_open_rejects_write_pragmas_and_statements() {
    let test = TestDatabase::new();
    let readonly = Db::open_read_only(&test.path).unwrap();
    let connection = readonly.connect().unwrap();

    assert!(connection.execute("UPDATE projects SET paused = 1", []).is_err());
    assert!(connection
        .execute("CREATE TABLE should_not_exist (id INTEGER)", [])
        .is_err());
    assert!(connection.execute("PRAGMA user_version = 99", []).is_err());
}

#[test]
fn repeated_current_schema_open_does_not_rebuild_intervention_indexes() {
    let test = TestDatabase::new();
    let before = test
        .db
        .connect()
        .unwrap()
        .query_row("PRAGMA schema_version", [], |row| row.get::<_, i64>(0))
        .unwrap();

    Db::open(&test.path).unwrap();

    let connection = test.db.connect().unwrap();
    let after: i64 = connection
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    let sequence_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'interventions_project_sequence_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let status_sql: String = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'interventions_project_status_created_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(after, before);
    let compact_sql = |sql: &str| sql.split_whitespace().collect::<Vec<_>>().join(" ");
    assert_eq!(
        compact_sql(&sequence_sql).to_ascii_lowercase(),
        "create unique index interventions_project_sequence_idx on interventions(project_id, insertion_sequence)"
    );
    assert_eq!(
        compact_sql(&status_sql).to_ascii_lowercase(),
        "create index interventions_project_status_created_idx on interventions(project_id, status, insertion_sequence, intervention_id)"
    );
}

#[test]
fn current_schema_reopen_does_not_recreate_submission_indexes() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    connection
        .execute_batch(
            "DROP INDEX submissions_project_kind_status_idx;
             DROP INDEX submissions_project_origin_agent_run_idx;",
        )
        .unwrap();
    drop(connection);

    Db::open(&test.path).unwrap();

    let connection = test.db.connect().unwrap();
    for index in [
        "submissions_project_kind_status_idx",
        "submissions_project_origin_agent_run_idx",
    ] {
        let exists: bool = connection
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1
                 )",
                [index],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!exists, "current-schema reopen recreated {index}");
    }
}

#[test]
fn concurrent_current_schema_opens_complete_without_migration_work() {
    let test = TestDatabase::new();
    let path = Arc::new(test.path.clone());
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = Arc::clone(&path);
            thread::spawn(move || {
                barrier.wait();
                Db::open(path.as_ref()).map(|_| ())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }
}

#[test]
fn concurrent_first_opens_apply_migration_once() {
    let temp = TempDir::new().unwrap();
    let path = Arc::new(temp.path().join("fresh.sqlite3"));
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = Arc::clone(&path);
            thread::spawn(move || {
                barrier.wait();
                Db::open(path.as_ref()).map(|_| ())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let db = Db::open(&path).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
}

#[test]
fn existing_v6_interventions_are_backfilled_with_project_sequences() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let connection = test.db.connect().unwrap();
    remove_campaign_schema_for_legacy_fixture(&connection);
    connection
        .execute_batch(
            r#"
        DROP TABLE interventions;
        CREATE TABLE interventions (
            intervention_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
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
                REFERENCES agent_runs(project_id, run_id) ON DELETE SET NULL
        );
        CREATE INDEX interventions_project_status_created_idx
            ON interventions(project_id, status, created_at, intervention_id);
        CREATE INDEX interventions_reservation_lease_idx
            ON interventions(status, lease_expires_at, reservation_token);
        INSERT INTO interventions (
            intervention_id, project_id, message, status, created_at
        ) VALUES ('old-first', 'project-a', 'first', 'pending', 100);
        INSERT INTO interventions (
            intervention_id, project_id, message, status, created_at
        ) VALUES ('old-second', 'project-a', 'second', 'pending', 100);
        PRAGMA user_version = 6;
        "#,
        )
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let listed = InterventionRepository::new(&migrated)
        .list("project-a", InterventionStatus::Pending, 8)
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|item| (item.intervention_id.as_str(), item.insertion_sequence))
            .collect::<Vec<_>>(),
        vec![("old-first", 1), ("old-second", 2)]
    );
}

#[test]
fn schema_v6_migration_backfills_submission_kind_and_metadata_defaults() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    SubmissionRepository::new(&test.db)
        .insert_idempotent(&NewSubmission::new(
            "legacy-submission",
            "project-a",
            vec!["python".to_owned(), "train.py".to_owned()],
            100,
        ))
        .unwrap();
    let connection = test.db.connect().unwrap();
    remove_campaign_schema_for_legacy_fixture(&connection);
    connection
        .execute_batch(
            r#"
            DROP INDEX submissions_project_origin_agent_run_idx;
            DROP INDEX submissions_project_kind_status_idx;
            ALTER TABLE submissions RENAME TO submissions_v7;
            CREATE TABLE submissions (
                submission_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                argv_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                pueue_task_id INTEGER,
                task_signature TEXT,
                status TEXT NOT NULL
            );
            INSERT INTO submissions (
                submission_id, project_id, argv_json, created_at,
                pueue_task_id, task_signature, status
            ) SELECT submission_id, project_id, argv_json, created_at,
                pueue_task_id, task_signature, status
            FROM submissions_v7;
            DROP TABLE submissions_v7;
            PRAGMA user_version = 6;
            "#,
        )
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let columns = connection
        .prepare("PRAGMA table_info(submissions)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    assert!(columns.iter().any(|column| column == "kind"));
    assert!(columns.iter().any(|column| column == "metadata_json"));
    assert!(columns.iter().any(|column| column == "origin_agent_run_id"));
    let origin_foreign_key_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM pragma_foreign_key_list('submissions')
             WHERE \"table\" = 'agent_runs'
               AND \"from\" = 'origin_agent_run_id'
               AND \"to\" = 'run_id'
               AND on_delete = 'RESTRICT'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(origin_foreign_key_count, 1);
    drop(connection);

    let submission = SubmissionRepository::new(&migrated)
        .find_by_id("legacy-submission")
        .unwrap()
        .unwrap();
    assert_eq!(submission.kind, SubmissionKind::Experiment);
    assert_eq!(submission.metadata, json!({}));
    assert_eq!(submission.origin_agent_run_id, None);

    let event_id = insert_event(&migrated, "project-a", "v7-origin", 101);
    let run = AgentRunRepository::new(&migrated)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Running,
            101,
            "/tmp/agent.log",
        ))
        .unwrap();
    SubmissionRepository::new(&migrated)
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "v7-origin",
            "project-a",
            vec!["python".to_owned()],
            101,
            SubmissionKind::Control,
            json!({"stage": "bootstrap"}),
            Some(run.run_id),
        ))
        .unwrap();
    let connection = migrated.connect().unwrap();
    remove_campaign_schema_for_legacy_fixture(&connection);
    connection
        .execute_batch(
            r#"
            PRAGMA foreign_keys = OFF;
            DROP INDEX IF EXISTS submissions_project_origin_agent_run_idx;
            DROP INDEX IF EXISTS submissions_project_kind_status_idx;
            DROP INDEX IF EXISTS submissions_project_status_idx;
            ALTER TABLE submissions RENAME TO submissions_v7_with_compliant_origin_fk;
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
                FOREIGN KEY (origin_agent_run_id)
                    REFERENCES agent_runs(run_id) ON DELETE RESTRICT,
                FOREIGN KEY (origin_agent_run_id, project_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE RESTRICT
            );
            INSERT INTO submissions SELECT * FROM submissions_v7_with_compliant_origin_fk;
            DROP TABLE submissions_v7_with_compliant_origin_fk;
            PRAGMA user_version = 7;
            PRAGMA foreign_keys = ON;
            "#,
        )
        .unwrap();
    drop(connection);

    let rebuilt = Db::open(&test.path).unwrap();
    let preserved = SubmissionRepository::new(&rebuilt)
        .find_by_id("v7-origin")
        .unwrap()
        .unwrap();
    assert_eq!(preserved.kind, SubmissionKind::Control);
    assert_eq!(preserved.metadata, json!({"stage": "bootstrap"}));
    assert_eq!(preserved.origin_agent_run_id, Some(run.run_id));
    let connection = rebuilt.connect().unwrap();
    let composite_origin_foreign_key_count: i64 = connection
        .query_row(
            "SELECT COUNT(*)
             FROM pragma_foreign_key_list('submissions') AS project_fk
             JOIN pragma_foreign_key_list('submissions') AS origin_fk
               ON project_fk.id = origin_fk.id
             WHERE project_fk.seq = 0
               AND project_fk.\"table\" = 'agent_runs'
               AND project_fk.\"from\" = 'project_id'
               AND project_fk.\"to\" = 'project_id'
               AND project_fk.on_delete = 'RESTRICT'
               AND origin_fk.seq = 1
               AND origin_fk.\"from\" = 'origin_agent_run_id'
               AND origin_fk.\"to\" = 'run_id'
               AND origin_fk.on_delete = 'RESTRICT'
               AND 2 = (
                   SELECT COUNT(*)
                   FROM pragma_foreign_key_list('submissions') AS fk_part
                   WHERE fk_part.id = project_fk.id
               )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(composite_origin_foreign_key_count, 1);
    drop(connection);
    assert!(rebuilt
        .connect()
        .unwrap()
        .execute("DELETE FROM agent_runs WHERE run_id = ?1", [run.run_id])
        .is_err());
    assert_eq!(
        SubmissionRepository::new(&rebuilt)
            .find_by_id("v7-origin")
            .unwrap()
            .unwrap()
            .origin_agent_run_id,
        Some(run.run_id)
    );
}

#[test]
fn control_submissions_do_not_consume_experiment_guardrail() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let connection = test.db.connect().unwrap();
    let _ = connection.execute(
        "ALTER TABLE submissions ADD COLUMN kind TEXT NOT NULL DEFAULT 'experiment'",
        [],
    );
    connection
        .execute(
            "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, pueue_task_id, task_signature, status, kind)
             VALUES ('experiment', 'project-a', '[\"python\"]', 100, 1, 'experiment-task', 'accepted', 'experiment')",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, pueue_task_id, task_signature, status, kind)
             VALUES ('control', 'project-a', '[\"python\"]', 101, 2, 'control-task', 'accepted', 'control')",
            [],
        )
        .unwrap();
    drop(connection);

    assert_eq!(
        SubmissionRepository::new(&test.db)
            .count_started_or_accepted("project-a")
            .unwrap(),
        1
    );
}

#[test]
fn submissions_are_scoped_by_project_and_origin_agent_run() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-project-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-project-b");
    let event_a = insert_event(&test.db, "project-a", "origin-a", 100);
    let event_b = insert_event(&test.db, "project-b", "origin-b", 100);
    let run_a = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_a,
            None,
            AgentRunStatus::Running,
            100,
            "/tmp/agent-a.log",
        ))
        .unwrap();
    let run_b = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-b",
            event_b,
            None,
            AgentRunStatus::Running,
            100,
            "/tmp/agent-b.log",
        ))
        .unwrap();
    let repository = SubmissionRepository::new(&test.db);
    repository
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "submission-a",
            "project-a",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Experiment,
            json!({"variant": "a"}),
            Some(run_a.run_id),
        ))
        .unwrap();
    repository
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "submission-b",
            "project-b",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Control,
            json!({"variant": "b"}),
            Some(run_b.run_id),
        ))
        .unwrap();

    assert_eq!(
        repository
            .list_by_origin_agent_run("project-a", run_a.run_id, 10)
            .unwrap()
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-a"]
    );
    assert!(repository
        .list_by_origin_agent_run("project-a", run_b.run_id, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn runs_repository_scopes_lineage_and_keeps_incomplete_submissions() {
    let test = TestDatabase::new();
    let root_a = test.project_root("runs-a");
    let root_b = test.project_root("runs-b");
    register_project(&test.db, "runs-a", &root_a, "pa-runs-a");
    register_project(&test.db, "runs-b", &root_b, "pa-runs-b");
    let event_a = insert_event(&test.db, "runs-a", "runs-a-event", 100);
    let event_b = insert_event(&test.db, "runs-b", "runs-b-event", 100);
    let run_a = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "runs-a",
            event_a,
            None,
            AgentRunStatus::Completed,
            101,
            "/tmp/a.log",
        ))
        .unwrap();
    let run_b = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "runs-b",
            event_b,
            None,
            AgentRunStatus::Completed,
            102,
            "/tmp/b.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&test.db);
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "runs-a-accepted",
            "runs-a",
            vec!["python".to_owned()],
            103,
            SubmissionKind::Experiment,
            json!({"prompt": "never project"}),
            Some(run_a.run_id),
        ))
        .unwrap();
    submissions
        .mark_accepted("runs-a-accepted", 41, "runs-a-task")
        .unwrap();
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "runs-a-pending",
            "runs-a",
            vec!["python".to_owned()],
            104,
            SubmissionKind::Control,
            json!({}),
            Some(run_a.run_id),
        ))
        .unwrap();
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "runs-b-accepted",
            "runs-b",
            vec!["python".to_owned()],
            105,
            SubmissionKind::Experiment,
            json!({}),
            Some(run_b.run_id),
        ))
        .unwrap();
    submissions
        .mark_accepted("runs-b-accepted", 99, "runs-b-task")
        .unwrap();

    let lineages = pueue_agent::db::RunLineageRepository::new(&test.db)
        .list_by_project("runs-a", 8)
        .unwrap();
    assert_eq!(lineages.len(), 1);
    assert_eq!(lineages[0].event_id, Some(event_a));
    assert_eq!(lineages[0].run_id, Some(run_a.run_id));
    assert_eq!(lineages[0].event_kind, Some(EventKind::TaskFinished));
    assert_eq!(lineages[0].submissions.len(), 2);
    assert_eq!(lineages[0].submissions[0].submission_id, "runs-a-pending");
    assert_eq!(lineages[0].submissions[0].pueue_task_id, None);
    assert_eq!(lineages[0].submissions[1].pueue_task_id, Some(41));

    let readonly = Db::open_read_only(&test.path).unwrap();
    let before_schema_version: i64 = test
        .db
        .connect()
        .unwrap()
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    let mut cursor = FollowCursor::default();
    let initial = collect_fresh(
        pueue_agent::db::RunLineageRepository::new(&readonly)
            .list_by_project("runs-a", 8)
            .unwrap(),
        &mut cursor,
        8,
    );
    assert_eq!(initial.len(), 1);
    let repeated = collect_fresh(
        pueue_agent::db::RunLineageRepository::new(&readonly)
            .list_by_project("runs-a", 8)
            .unwrap(),
        &mut cursor,
        8,
    );
    assert!(repeated.is_empty());
    assert!(readonly
        .connect()
        .unwrap()
        .execute("UPDATE projects SET paused = 1", [])
        .is_err());
    let after_schema_version: i64 = test
        .db
        .connect()
        .unwrap()
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after_schema_version, before_schema_version);
}

#[test]
fn runs_repository_keeps_old_submission_for_latest_run_when_limit_is_one() {
    let test = TestDatabase::new();
    let root = test.project_root("runs-limit");
    register_project(&test.db, "runs-limit", &root, "pa-runs-limit");
    let event = insert_event(&test.db, "runs-limit", "runs-limit-event", 200);
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "runs-limit",
            event,
            None,
            AgentRunStatus::Completed,
            200,
            "/tmp/runs-limit.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&test.db);
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "runs-limit-old",
            "runs-limit",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Experiment,
            json!({}),
            Some(run.run_id),
        ))
        .unwrap();
    submissions
        .insert_idempotent(&NewSubmission::new(
            "runs-limit-new-orphan",
            "runs-limit",
            vec!["python".to_owned()],
            300,
        ))
        .unwrap();

    let lineages = pueue_agent::db::RunLineageRepository::new(&test.db)
        .list_by_project("runs-limit", 1)
        .unwrap();

    assert_eq!(lineages.len(), 1);
    assert_eq!(lineages[0].run_id, Some(run.run_id));
    assert_eq!(
        lineages[0]
            .submissions
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["runs-limit-old"]
    );
}

#[test]
fn follow_limit_one_reaches_every_submission_on_one_run_through_repository() {
    let test = TestDatabase::new();
    let root = test.project_root("follow-page");
    register_project(&test.db, "follow-page", &root, "pa-follow-page");
    let event = insert_event(&test.db, "follow-page", "follow-page-event", 200);
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "follow-page",
            event,
            None,
            AgentRunStatus::Running,
            200,
            "/tmp/follow-page.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&test.db);
    for (submission_id, created_at) in [("new", 200), ("old", 100)] {
        submissions
            .insert_idempotent(&NewSubmission::with_kind_metadata(
                submission_id,
                "follow-page",
                vec!["python".to_owned()],
                created_at,
                SubmissionKind::Experiment,
                json!({}),
                Some(run.run_id),
            ))
            .unwrap();
    }

    let repository = pueue_agent::db::RunLineageRepository::new(&test.db);
    let mut cursor = FollowCursor::default();
    let first_lineages = repository
        .list_by_project_follow(
            "follow-page",
            1,
            cursor.submission_after(),
            cursor.submission_head(),
        )
        .unwrap();
    assert_eq!(first_lineages[0].submissions.len(), 1);
    let first = collect_fresh(first_lineages, &mut cursor, 1);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].submissions.len(), 1);
    let first_submission = first[0].submissions[0].submission_id.clone();
    assert_eq!(first_submission, "new");

    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "head-new",
            "follow-page",
            vec!["python".to_owned()],
            300,
            SubmissionKind::Experiment,
            json!({}),
            Some(run.run_id),
        ))
        .unwrap();

    let second = collect_fresh(
        repository
            .list_by_project_follow(
                "follow-page",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].submissions.len(), 1);
    let second_submission = second[0].submissions[0].submission_id.as_str();
    assert_ne!(first_submission, second_submission);
    assert_eq!(second_submission, "old");

    let third = collect_fresh(
        repository
            .list_by_project_follow(
                "follow-page",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(third.len(), 1);
    assert_eq!(third[0].submissions[0].submission_id, "head-new");
}

#[test]
fn follow_limit_one_pages_originless_submissions_and_keeps_the_stream_active() {
    let test = TestDatabase::new();
    let root = test.project_root("originless-follow");
    register_project(&test.db, "originless-follow", &root, "pa-originless-follow");
    let submissions = SubmissionRepository::new(&test.db);
    for (submission_id, created_at) in [("originless-new", 200), ("originless-old", 100)] {
        submissions
            .insert_idempotent(&NewSubmission::with_kind_metadata(
                submission_id,
                "originless-follow",
                vec!["python".to_owned()],
                created_at,
                SubmissionKind::Experiment,
                json!({}),
                None,
            ))
            .unwrap();
    }

    let repository = pueue_agent::db::RunLineageRepository::new(&test.db);
    let mut cursor = FollowCursor::default();
    let first = collect_fresh(
        repository
            .list_by_project_follow(
                "originless-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(first[0].submissions[0].submission_id, "originless-new");

    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "originless-head",
            "originless-follow",
            vec!["python".to_owned()],
            300,
            SubmissionKind::Experiment,
            json!({}),
            None,
        ))
        .unwrap();

    let second = collect_fresh(
        repository
            .list_by_project_follow(
                "originless-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(second[0].submissions[0].submission_id, "originless-old");

    submissions
        .mark_accepted("originless-old", 88, "sig-originless-old")
        .unwrap();
    let third = collect_fresh(
        repository
            .list_by_project_follow(
                "originless-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(third[0].submissions[0].submission_id, "originless-old");
    assert_eq!(third[0].submissions[0].pueue_task_id, Some(88));

    let fourth = collect_fresh(
        repository
            .list_by_project_follow(
                "originless-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(fourth[0].submissions[0].submission_id, "originless-head");
}

#[test]
fn follow_limit_one_does_not_starve_originless_stream_when_a_run_is_selected() {
    let test = TestDatabase::new();
    let root = test.project_root("mixed-follow");
    register_project(&test.db, "mixed-follow", &root, "pa-mixed-follow");
    let event = insert_event(&test.db, "mixed-follow", "mixed-follow-event", 100);
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "mixed-follow",
            event,
            None,
            AgentRunStatus::Running,
            100,
            "/tmp/mixed-follow.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&test.db);
    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "run-new",
            "mixed-follow",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Experiment,
            json!({}),
            Some(run.run_id),
        ))
        .unwrap();
    for (submission_id, created_at) in [("originless-new", 200), ("originless-old", 100)] {
        submissions
            .insert_idempotent(&NewSubmission::with_kind_metadata(
                submission_id,
                "mixed-follow",
                vec!["python".to_owned()],
                created_at,
                SubmissionKind::Experiment,
                json!({}),
                None,
            ))
            .unwrap();
    }

    let repository = pueue_agent::db::RunLineageRepository::new(&test.db);
    let mut cursor = FollowCursor::default();
    let first = collect_fresh(
        repository
            .list_by_project_follow(
                "mixed-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(first[0].submissions[0].submission_id, "run-new");

    let second = collect_fresh(
        repository
            .list_by_project_follow(
                "mixed-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(second[0].submissions[0].submission_id, "originless-new");

    submissions
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "originless-head",
            "mixed-follow",
            vec!["python".to_owned()],
            300,
            SubmissionKind::Experiment,
            json!({}),
            None,
        ))
        .unwrap();

    let third = collect_fresh(
        repository
            .list_by_project_follow(
                "mixed-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(third[0].submissions[0].submission_id, "originless-old");

    let fourth = collect_fresh(
        repository
            .list_by_project_follow(
                "mixed-follow",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap(),
        &mut cursor,
        1,
    );
    assert_eq!(fourth[0].submissions[0].submission_id, "originless-head");
}

#[test]
fn follow_lineage_pages_all_submissions_beyond_one_internal_page() {
    let test = TestDatabase::new();
    let root = test.project_root("follow-pages");
    register_project(&test.db, "follow-pages", &root, "pa-follow-pages");
    let event = insert_event(&test.db, "follow-pages", "follow-pages-event", 200);
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "follow-pages",
            event,
            None,
            AgentRunStatus::Running,
            200,
            "/tmp/follow-pages.log",
        ))
        .unwrap();
    let submissions = SubmissionRepository::new(&test.db);
    for index in 0..1001 {
        submissions
            .insert_idempotent(&NewSubmission::with_kind_metadata(
                format!("submission-{index:04}"),
                "follow-pages",
                vec!["python".to_owned()],
                index,
                SubmissionKind::Experiment,
                json!({}),
                Some(run.run_id),
            ))
            .unwrap();
    }

    let repository = pueue_agent::db::RunLineageRepository::new(&test.db);
    let normal = repository.list_by_project("follow-pages", 1).unwrap();
    assert_eq!(normal.len(), 1);
    assert_eq!(normal[0].submissions.len(), 1);

    let mut cursor = FollowCursor::default();
    let mut observed = std::collections::BTreeSet::new();
    for _ in 0..1001 {
        let lineages = repository
            .list_by_project_follow(
                "follow-pages",
                1,
                cursor.submission_after(),
                cursor.submission_head(),
            )
            .unwrap();
        assert!(lineages[0].submissions.len() <= pueue_agent::db::MAX_FOLLOW_LINEAGE_SUBMISSIONS);
        let fresh = collect_fresh(lineages, &mut cursor, 1);
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].submissions.len(), 1);
        observed.insert(fresh[0].submissions[0].submission_id.clone());
    }

    assert_eq!(observed.len(), 1001);
}

#[test]
fn follow_cursor_deduplicates_orders_and_respects_limit() {
    let mut cursor = FollowCursor::default();
    let first = pueue_agent::db::RunLineageCursor::new(100, 1, None, None);
    let second = pueue_agent::db::RunLineageCursor::new(101, 2, Some("sub-2".to_owned()), Some(42));
    let duplicate = first.clone();

    assert!(cursor.observe(&second));
    assert!(cursor.observe(&first));
    assert!(!cursor.observe(&duplicate));
    assert_eq!(cursor.take_ordered(1), vec![first]);
    assert_eq!(cursor.take_ordered(8), vec![second]);
}

#[test]
fn submission_rejects_a_missing_origin_agent_run_through_repository_and_sql() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");

    let error = SubmissionRepository::new(&test.db)
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "missing-origin",
            "project-a",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Experiment,
            json!({}),
            Some(999),
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "origin_agent_run_id",
            ..
        }
    ));
    assert!(SubmissionRepository::new(&test.db)
        .find_by_id("missing-origin")
        .unwrap()
        .is_none());
    assert!(test
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO submissions (
                submission_id, project_id, argv_json, created_at,
                pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
             ) VALUES (
                'missing-origin-sql', 'project-a', '[\"python\"]', 100,
                NULL, NULL, 'pending', 'experiment', '{}', 999
             )",
            [],
        )
        .is_err());
}

#[test]
fn submission_rejects_an_origin_agent_run_from_another_project() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-project-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-project-b");
    let event_b = insert_event(&test.db, "project-b", "origin-b", 100);
    let run_b = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-b",
            event_b,
            None,
            AgentRunStatus::Running,
            100,
            "/tmp/agent-b.log",
        ))
        .unwrap();

    let error = SubmissionRepository::new(&test.db)
        .insert_idempotent(&NewSubmission::with_kind_metadata(
            "cross-project-origin",
            "project-a",
            vec!["python".to_owned()],
            100,
            SubmissionKind::Experiment,
            json!({}),
            Some(run_b.run_id),
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        AppError::Validation {
            field: "origin_agent_run_id",
            ..
        }
    ));
    assert!(SubmissionRepository::new(&test.db)
        .find_by_id("cross-project-origin")
        .unwrap()
        .is_none());
    assert!(test
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO submissions (
                submission_id, project_id, argv_json, created_at,
                pueue_task_id, task_signature, status, kind, metadata_json, origin_agent_run_id
             ) VALUES (
                'cross-project-origin-sql', 'project-a', '[\"python\"]', 100,
                NULL, NULL, 'pending', 'experiment', '{}', ?1
             )",
            [run_b.run_id],
        )
        .is_err());
}

#[test]
fn malformed_submission_metadata_fails_closed_when_reading_from_database() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let connection = test.db.connect().unwrap();
    let _ = connection.execute(
        "ALTER TABLE submissions ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'",
        [],
    );
    connection
        .execute(
            "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, pueue_task_id, task_signature, status, metadata_json)
             VALUES ('bad-metadata', 'project-a', '[\"python\"]', 100, NULL, NULL, 'pending', 'not-json')",
            [],
        )
        .unwrap();
    drop(connection);

    assert!(SubmissionRepository::new(&test.db)
        .find_by_id("bad-metadata")
        .is_err());
}

#[test]
fn schema_v5_migration_preserves_projects_and_events_and_adds_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "v5-event", 100);
    let connection = test.db.connect().unwrap();
    remove_campaign_schema_for_legacy_fixture(&connection);
    connection
        .execute_batch("DROP TABLE IF EXISTS interventions; PRAGMA user_version = 5;")
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    let intervention_table_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'interventions'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let migrated_intervention_columns = connection
        .prepare("PRAGMA table_info(interventions)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(migrated_intervention_columns
        .iter()
        .any(|name| name == "insertion_sequence"));
    let sequence_index_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'interventions_project_sequence_idx'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sequence_index_count, 1);
    let preserved_event_id: i64 = connection
        .query_row(
            "SELECT event_id FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    let preserved_project_id: String = connection
        .query_row(
            "SELECT project_id FROM projects WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    assert_eq!(intervention_table_count, 1);
    assert_eq!(preserved_event_id, event_id);
    assert_eq!(preserved_project_id, "project-a");
}

#[test]
fn interventions_validate_messages_and_list_fifo_with_project_scope() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-project-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-project-b");
    let repository = InterventionRepository::new(&test.db);

    assert!(repository.insert_pending("project-a", "   ", 100).is_err());
    assert!(repository
        .insert_pending("project-a", &"a".repeat(MAX_INTERVENTION_BYTES + 1), 100)
        .is_err());

    let inserted = (0..8)
        .map(|index| {
            repository
                .insert_pending("project-a", &format!("instruction-{index}"), 100)
                .unwrap()
        })
        .collect::<Vec<_>>();
    let other = repository
        .insert_pending("project-b", "foreign instruction", 100)
        .unwrap();

    let connection = test.db.connect().unwrap();
    let stored_sequences = connection
        .prepare(
            "SELECT insertion_sequence FROM interventions
             WHERE project_id = 'project-a' ORDER BY insertion_sequence ASC",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(stored_sequences, (1..=8).collect::<Vec<_>>());

    let listed = repository
        .list("project-a", InterventionStatus::Pending, 8)
        .unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        inserted
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>()
    );
    assert!(listed.iter().all(|item| item.project_id == "project-a"));
    assert!(!listed
        .iter()
        .any(|item| item.intervention_id == other.intervention_id));
    let counts = repository.count_by_project("project-a").unwrap();
    assert_eq!(counts.pending, 8);
    assert_eq!(counts.reserved, 0);
    assert_eq!(counts.applied, 0);
}

#[test]
fn interventions_reserve_fifo_with_count_and_byte_bounds() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = InterventionRepository::new(&test.db);
    let first = repository
        .insert_pending("project-a", "first", 100)
        .unwrap();
    let second = repository
        .insert_pending("project-a", "second", 100)
        .unwrap();
    let overflow = repository
        .insert_pending("project-a", "overflow", 100)
        .unwrap();

    let reservation = repository
        .reserve_pending(
            "project-a",
            "reservation-a",
            200,
            300,
            2,
            "firstsecond".len(),
        )
        .unwrap();
    assert_eq!(reservation.token, "reservation-a");
    assert_eq!(
        reservation
            .items
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            first.intervention_id.as_str(),
            second.intervention_id.as_str()
        ]
    );
    assert!(reservation
        .items
        .iter()
        .all(|item| item.status == InterventionStatus::Reserved
            && item.reservation_token.as_deref() == Some("reservation-a")));
    assert_eq!(
        repository
            .list("project-a", InterventionStatus::Pending, 8)
            .unwrap()
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![overflow.intervention_id.as_str()]
    );

    for index in 0..MAX_INTERVENTIONS_PER_RUN {
        repository
            .insert_pending("project-a", &format!("capped-{index}"), 102)
            .unwrap();
    }
    let capped = repository
        .reserve_pending(
            "project-a",
            "reservation-b",
            201,
            301,
            MAX_INTERVENTIONS_PER_RUN + 1,
            MAX_INTERVENTION_BYTES_PER_RUN + 1,
        )
        .unwrap();
    assert_eq!(capped.items.len(), MAX_INTERVENTIONS_PER_RUN);
    assert_eq!(
        repository
            .list(
                "project-a",
                InterventionStatus::Pending,
                MAX_INTERVENTIONS_PER_RUN
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn interventions_apply_release_and_expiry_respect_project_and_reservation_ownership() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-project-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-project-b");
    let repository = InterventionRepository::new(&test.db);
    let applied = repository
        .insert_pending("project-a", "apply me", 100)
        .unwrap();
    let released = repository
        .insert_pending("project-a", "release me", 101)
        .unwrap();
    let foreign = repository
        .insert_pending("project-b", "foreign", 100)
        .unwrap();
    repository
        .reserve_pending("project-a", "reservation-a", 200, 300, 8, 1024)
        .unwrap();
    repository
        .reserve_pending("project-b", "reservation-b", 200, 300, 8, 1024)
        .unwrap();

    let event_id = insert_event(&test.db, "project-a", "intervention-apply-run", 100);
    let applied_run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            200,
            "/tmp/intervention-run.log",
        ))
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET agent_run_id = ?1
             WHERE intervention_id = ?2 AND project_id = ?3 AND reservation_token = ?4",
            params![
                applied_run.run_id,
                applied.intervention_id,
                "project-a",
                "reservation-a"
            ],
        )
        .unwrap();

    assert_eq!(
        repository
            .mark_applied_for_run("project-b", applied_run.run_id, 250)
            .unwrap(),
        0
    );
    assert_eq!(
        repository
            .mark_applied_for_run("project-a", applied_run.run_id, 250)
            .unwrap(),
        1
    );
    AgentRunRepository::new(&test.db)
        .finish(
            applied_run.run_id,
            AgentRunStatus::Completed,
            251,
            Some(0),
            None,
        )
        .unwrap();
    let release_event_id = insert_event(&test.db, "project-a", "intervention-release-run", 101);
    let release_run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            release_event_id,
            None,
            AgentRunStatus::Starting,
            252,
            "/tmp/intervention-release.log",
        ))
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET agent_run_id = ?1
             WHERE intervention_id = ?2 AND project_id = ?3 AND reservation_token = ?4",
            params![
                release_run.run_id,
                released.intervention_id,
                "project-a",
                "reservation-a"
            ],
        )
        .unwrap();
    assert_eq!(
        repository
            .release_for_run("project-b", release_run.run_id)
            .unwrap(),
        0
    );
    assert_eq!(
        repository
            .release_for_run("project-a", release_run.run_id)
            .unwrap(),
        1
    );
    assert_eq!(
        repository
            .list("project-a", InterventionStatus::Applied, 8)
            .unwrap()
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![applied.intervention_id.as_str()]
    );
    assert_eq!(
        repository
            .list("project-a", InterventionStatus::Pending, 8)
            .unwrap()
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![released.intervention_id.as_str()]
    );
    assert_eq!(
        repository
            .list("project-b", InterventionStatus::Reserved, 8)
            .unwrap()
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![foreign.intervention_id.as_str()]
    );
    assert_eq!(repository.recover_expired(299).unwrap(), 0);
    assert_eq!(repository.recover_expired(300).unwrap(), 1);
    assert_eq!(
        repository
            .list("project-b", InterventionStatus::Pending, 8)
            .unwrap()
            .iter()
            .map(|item| item.intervention_id.as_str())
            .collect::<Vec<_>>(),
        vec![foreign.intervention_id.as_str()]
    );
}

#[test]
fn periodic_intervention_recovery_leaves_expired_attached_reservations_untouched() {
    let test = TestDatabase::new();
    let root = test.project_root("project-a");
    register_project(&test.db, "project-a", &root, "pa-project-a");
    let intervention = InterventionRepository::new(&test.db)
        .insert_pending("project-a", "attached instruction", 100)
        .unwrap();
    InterventionRepository::new(&test.db)
        .reserve_pending("project-a", "attached-token", 100, 101, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "attached-recovery", 100);
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            Some(4242),
            AgentRunStatus::Running,
            100,
            "/tmp/attached-recovery.log",
        ))
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET agent_run_id = ?1 WHERE intervention_id = ?2",
            params![run.run_id, intervention.intervention_id],
        )
        .unwrap();

    assert_eq!(
        InterventionRepository::new(&test.db)
            .recover_expired_unattached(101)
            .unwrap(),
        0
    );
    let state = InterventionRepository::new(&test.db)
        .list("project-a", InterventionStatus::Reserved, 8)
        .unwrap();
    assert_eq!(state.len(), 1);
    assert_eq!(state[0].agent_run_id, Some(run.run_id));
}

#[test]
fn interventions_bind_to_runs_and_apply_or_release_in_agent_run_transactions() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let applied = interventions
        .insert_pending("project-a", "apply transactionally", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "apply-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "apply-transaction", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);

    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/apply-transaction.log",
            ),
            &[event_id],
            Some("apply-token"),
        )
        .unwrap();
    let reserved_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&applied.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        reserved_state,
        (InterventionStatus::Reserved, Some(run.run_id))
    );

    let running = runs
        .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 130)
        .unwrap();
    assert_eq!(running.status, AgentRunStatus::Running);
    assert_eq!(running.pid, Some(4242));
    let applied_state: (InterventionStatus, Option<i64>, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id, applied_at
             FROM interventions WHERE intervention_id = ?1",
            [&applied.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        applied_state,
        (InterventionStatus::Applied, Some(run.run_id), Some(130))
    );
    runs.finish_and_release_interventions(
        "project-a",
        run.run_id,
        AgentRunStatus::Completed,
        140,
        Some(0),
        None,
    )
    .unwrap();
    assert_eq!(
        interventions
            .list("project-a", InterventionStatus::Applied, 8)
            .unwrap()
            .len(),
        1
    );

    let released = interventions
        .insert_pending("project-a", "release transactionally", 150)
        .unwrap();
    interventions
        .reserve_pending("project-a", "release-token", 160, 260, 1, 1024)
        .unwrap();
    let release_event_id = insert_event(&test.db, "project-a", "release-transaction", 150);
    EventRepository::new(&test.db)
        .claim_batch(150, 260, 1)
        .unwrap();
    let release_run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                release_event_id,
                None,
                AgentRunStatus::Starting,
                170,
                "/tmp/release-transaction.log",
            ),
            &[release_event_id],
            Some("release-token"),
        )
        .unwrap();
    runs.finish_and_release_interventions(
        "project-a",
        release_run.run_id,
        AgentRunStatus::Failed,
        180,
        None,
        Some("spawn failed"),
    )
    .unwrap();
    let released_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&released.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(released_state, (InterventionStatus::Pending, None));
}

#[test]
fn interventions_mark_running_and_application_are_atomic() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "fail atomically", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "atomic-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "atomic-transaction", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/atomic-transaction.log",
            ),
            &[event_id],
            Some("atomic-token"),
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_intervention_application
             BEFORE UPDATE OF status ON interventions
             WHEN OLD.status = 'reserved' AND NEW.status = 'applied'
             BEGIN
                 SELECT RAISE(ABORT, 'injected intervention application failure');
             END;",
        )
        .unwrap();

    assert!(runs
        .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 130)
        .is_err());

    let run_state: (AgentRunStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, pid FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(run_state, (AgentRunStatus::Starting, None));
    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        intervention_state,
        (InterventionStatus::Reserved, Some(run.run_id))
    );
}

#[test]
fn pre_release_gate_failure_requeues_applied_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "pre-release failure", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "pre-release-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "pre-release-gate", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/pre-release-gate.log",
            ),
            &[event_id],
            Some("pre-release-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();

    runs
        .fail_before_gate_release_with_policy(
            "project-a",
            run.run_id,
            140,
            "gate EOF",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap();

    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let run_state: (AgentRunStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let event_state: (EventStatus, Option<i64>, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, not_before, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(intervention_state, (InterventionStatus::Pending, None));
    assert_eq!(run_state, (AgentRunStatus::Failed, "failed".to_owned()));
    assert_eq!(event_state, (EventStatus::RetryWait, Some(200), None));
}

#[test]
fn pre_marker_policy_failure_dead_letters_and_releases_applied_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("policy-pre-marker");
    register_project(&test.db, "project-a", &root, "pa-policy-pre-marker");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "release on policy block", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "policy-pre-marker-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "policy-pre-marker", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/policy-pre-marker.log",
            ),
            &[event_id],
            Some("policy-pre-marker-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::RunBoundPreMarker,
    );
    runs.fail_before_gate_release_with_policy("project-a", run.run_id, 140, "ignored detail", &violation)
        .unwrap();

    let state: (AgentRunStatus, EventStatus, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, agent_runs.last_error
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Failed,
            EventStatus::DeadLetter,
            Some("policy_blocked:unsafe_codex_argument".to_owned()),
        )
    );
    assert_eq!(intervention.intervention_id, interventions
        .list("project-a", InterventionStatus::Pending, 8)
        .unwrap()[0]
        .intervention_id);
}

#[test]
fn post_marker_policy_failure_dead_letters_and_retains_applied_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("policy-post-marker");
    register_project(&test.db, "project-a", &root, "pa-policy-post-marker");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "retain after marker", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "policy-post-marker-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "policy-post-marker", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/policy-post-marker.log",
            ),
            &[event_id],
            Some("policy-post-marker-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let violation = PolicyViolation::new(
        PolicyViolationCode::AnchorReplaced,
        PolicyViolationStage::PostMarker,
    );
    runs.finish_after_marker_policy_failure("project-a", run.run_id, 140, &violation)
        .unwrap();

    let state: (AgentRunStatus, EventStatus, InterventionStatus, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, interventions.status, agent_runs.last_error
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             JOIN interventions ON interventions.agent_run_id = agent_runs.run_id
             WHERE agent_runs.run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Failed,
            EventStatus::DeadLetter,
            InterventionStatus::Applied,
            Some("policy_blocked:anchor_replaced".to_owned()),
        )
    );
    assert_eq!(intervention.intervention_id, interventions
        .list("project-a", InterventionStatus::Applied, 8)
        .unwrap()[0]
        .intervention_id);
}

#[test]
fn pending_marker_policy_failure_dead_letters_and_releases_only_reserved_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("pending-marker-policy");
    register_project(&test.db, "project-a", &root, "pa-pending-marker-policy");
    let runs = AgentRunRepository::new(&test.db);
    let events = EventRepository::new(&test.db);
    let interventions = InterventionRepository::new(&test.db);
    let event_id = insert_event(&test.db, "project-a", "pending-marker-policy", 100);
    events.claim_batch(100, 160, 10).unwrap();
    let reserved = interventions
        .insert_pending("project-a", "reserved", 90)
        .unwrap();
    let reservation = interventions
        .reserve_pending("project-a", "pending-marker-token", 95, 155, 1, 8)
        .unwrap();
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                100,
                test._temp.path().join("agent.log"),
            ),
            &[event_id],
            Some(&reservation.token),
        )
        .unwrap();
    let violation = pueue_agent::execution_policy::PolicyViolation::new(
        pueue_agent::execution_policy::PolicyViolationCode::NativeGateFailed,
        pueue_agent::execution_policy::PolicyViolationStage::PostMarker,
    );

    let finished = runs
        .finish_pending_marker_policy_failure("project-a", run.run_id, 140, &violation)
        .unwrap();

    assert_eq!(finished.status, AgentRunStatus::Failed);
    assert_eq!(finished.launch_gate_state.as_str(), "failed");
    assert_eq!(finished.policy_code.as_deref(), Some("native_gate_failed"));
    assert_eq!(finished.failure_stage.as_deref(), Some("post_marker"));
    let event = events.find_by_id(event_id).unwrap().unwrap();
    assert_eq!(event.status, EventStatus::DeadLetter);
    assert_eq!(event.attempts, 1);
    let intervention = interventions
        .list(
            "project-a",
            InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )
        .unwrap()
        .into_iter()
        .find(|item| item.intervention_id == reserved.intervention_id)
        .unwrap();
    assert_eq!(intervention.agent_run_id, None);
}

#[test]
fn pending_marker_policy_failure_rejects_non_pending_or_applied_state_atomically() {
    let test = TestDatabase::new();
    let root = test.project_root("pending-marker-policy-invalid");
    register_project(
        &test.db,
        "project-a",
        &root,
        "pa-pending-marker-policy-invalid",
    );
    let runs = AgentRunRepository::new(&test.db);
    let events = EventRepository::new(&test.db);
    let interventions = InterventionRepository::new(&test.db);
    let event_id = insert_event(&test.db, "project-a", "pending-marker-policy-invalid", 100);
    events.claim_batch(100, 160, 10).unwrap();
    let intervention = interventions
        .insert_pending("project-a", "must remain applied", 90)
        .unwrap();
    let reservation = interventions
        .reserve_pending("project-a", "pending-marker-invalid", 95, 155, 1, 1024)
        .unwrap();
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                100,
                test._temp.path().join("agent-invalid.log"),
            ),
            &[event_id],
            Some(&reservation.token),
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET status = 'applied', applied_at = 101
             WHERE intervention_id = ?1",
            [&intervention.intervention_id],
        )
        .unwrap();
    let violation = pueue_agent::execution_policy::PolicyViolation::new(
        pueue_agent::execution_policy::PolicyViolationCode::NativeGateFailed,
        pueue_agent::execution_policy::PolicyViolationStage::PostMarker,
    );

    assert!(runs
        .record_pending_marker_policy_evidence("project-a", run.run_id, &violation)
        .is_err());
    let evidence_state: (Option<String>, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT policy_code, failure_stage FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(evidence_state, (None, None));
    assert!(runs
        .finish_pending_marker_policy_failure("project-a", run.run_id, 140, &violation)
        .is_err());
    let state: (AgentRunStatus, String, EventStatus, InterventionStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, agent_runs.launch_gate_state, events.status,
                    interventions.status
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             JOIN interventions ON interventions.agent_run_id = agent_runs.run_id
             WHERE agent_runs.run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Starting,
            "pending".to_owned(),
            EventStatus::InFlight,
            InterventionStatus::Applied,
        ),
    );

    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET status = 'reserved', applied_at = NULL
             WHERE intervention_id = ?1",
            [&intervention.intervention_id],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    assert!(runs
        .record_pending_marker_policy_evidence("project-a", run.run_id, &violation)
        .is_err());
    let evidence_state: (Option<String>, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT policy_code, failure_stage FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(evidence_state, (None, None));
    assert!(runs
        .finish_pending_marker_policy_failure("project-a", run.run_id, 141, &violation)
        .is_err());
    let state: (AgentRunStatus, String, EventStatus, InterventionStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, agent_runs.launch_gate_state, events.status,
                    interventions.status
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             JOIN interventions ON interventions.agent_run_id = agent_runs.run_id
             WHERE agent_runs.run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Starting,
            "release_requested".to_owned(),
            EventStatus::InFlight,
            InterventionStatus::Reserved,
        ),
    );
}

#[test]
fn startup_recovery_preserves_durable_pending_marker_policy_evidence() {
    let test = TestDatabase::new();
    let root = test.project_root("pending-marker-recovery");
    register_project(&test.db, "project-a", &root, "pa-pending-marker-recovery");
    let events = EventRepository::new(&test.db);
    let event_id = insert_event(&test.db, "project-a", "pending-marker-recovery", 100);
    events.claim_batch(100, 160, 10).unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                100,
                root.join(".pueue-agent/logs/agent.log"),
            ),
            &[event_id],
            None,
        )
        .unwrap();
    let violation = pueue_agent::execution_policy::PolicyViolation::new(
        pueue_agent::execution_policy::PolicyViolationCode::NativeGateFailed,
        pueue_agent::execution_policy::PolicyViolationStage::PostMarker,
    );
    runs.record_pending_marker_policy_evidence("project-a", run.run_id, &violation)
        .unwrap();
    runs.record_pending_marker_policy_evidence("project-a", run.run_id, &violation)
        .unwrap();
    let different_violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PostMarker,
    );
    assert!(runs
        .record_pending_marker_policy_evidence(
            "project-a",
            run.run_id,
            &different_violation,
        )
        .is_err());
    let durable_state: (AgentRunStatus, String, EventStatus, Option<String>, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, agent_runs.launch_gate_state, events.status,
                    agent_runs.policy_code, agent_runs.failure_stage
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run.run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        durable_state,
        (
            AgentRunStatus::Starting,
            "pending".to_owned(),
            EventStatus::InFlight,
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        ),
    );

    let recovery = runs
        .recover_interrupted(
            200,
            "daemon restart",
            &std::collections::BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 4 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();

    assert_eq!(recovery.dead_lettered_events, 1);
    assert_eq!(recovery.requeued_events, 0);
    let event = events.find_by_id(event_id).unwrap().unwrap();
    assert_eq!(event.status, EventStatus::DeadLetter);
    let run_state: (AgentRunStatus, Option<String>, Option<String>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, policy_code, failure_stage FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        run_state,
        (
            AgentRunStatus::Failed,
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        )
    );
}

#[test]
fn startup_recovery_rejects_marker_evidence_for_the_wrong_gate_phase_atomically() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "startup-marker-phase-mismatch");
    let runs = AgentRunRepository::new(&test.db);
    let policies = BTreeMap::from([(
        "project-a".to_owned(),
        RetryPolicy { max_retries: 2 },
    )]);

    assert!(runs
        .recover_interrupted(
            140,
            "daemon restarted",
            &policies,
            &BTreeSet::new(),
            &BTreeSet::from([run_id]),
        )
        .is_err());
    assert!(runs
        .recover_interrupted(
            140,
            "daemon restarted",
            &policies,
            &BTreeSet::from([run_id]),
            &BTreeSet::from([run_id]),
        )
        .is_err());
    assert_eq!(
        runs.find_active_by_project("project-a")
            .unwrap()
            .unwrap()
            .status,
        AgentRunStatus::Starting,
    );
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::InFlight,
    );

    runs.mark_running_and_apply_interventions("project-a", run_id, 42_424, 130)
        .unwrap();
    assert!(runs
        .recover_interrupted(
            140,
            "daemon restarted",
            &policies,
            &BTreeSet::from([run_id]),
            &BTreeSet::new(),
        )
        .is_err());
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::InFlight,
    );
}

#[test]
fn startup_recovery_rejects_partial_pending_marker_policy_evidence_atomically() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "startup-partial-marker-evidence");
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET policy_code = 'native_gate_failed' WHERE run_id = ?1",
            [run_id],
        )
        .unwrap();

    assert!(AgentRunRepository::new(&test.db)
        .recover_interrupted(
            140,
            "daemon restarted",
            &BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 2 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .is_err());

    let state: (AgentRunStatus, String, Option<String>, Option<String>, EventStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, agent_runs.launch_gate_state,
                    agent_runs.policy_code, agent_runs.failure_stage, events.status
             FROM agent_runs
             JOIN agent_run_events ON agent_run_events.project_id = agent_runs.project_id
                AND agent_run_events.run_id = agent_runs.run_id
             JOIN events ON events.project_id = agent_run_events.project_id
                AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1 AND events.event_id = ?2",
            params![run_id, event_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Starting,
            "pending".to_owned(),
            Some("native_gate_failed".to_owned()),
            None,
            EventStatus::InFlight,
        ),
    );
}

#[test]
fn startup_recovery_requeues_applied_interventions_after_release_request_before_ack() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "release request recovery", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "release-request-recovery", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "release-request-recovery", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                test._temp.path().join("release-request-recovery.log"),
            ),
            &[event_id],
            Some("release-request-recovery"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4245, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();

    runs.recover_interrupted(
        140,
        "daemon restarted before launch gate acknowledgement",
        &BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 1 },
        )]),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
        .unwrap();

    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let run_state: (AgentRunStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(intervention_state, (InterventionStatus::Pending, None));
    assert_eq!(run_state, (AgentRunStatus::Failed, "failed".to_owned()));
}

#[test]
fn startup_recovery_retries_pre_marker_inflight_events() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "startup-pre-marker-retry");
    let runs = AgentRunRepository::new(&test.db);

    let recovery = runs
        .recover_interrupted(
            140,
            "daemon restarted",
            &BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 2 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();

    assert_eq!(recovery.failed_runs, 1);
    assert_eq!(recovery.requeued_events, 1);
    assert_eq!(recovery.dead_lettered_events, 0);
    let event = EventRepository::new(&test.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::RetryWait);
    let reason = event.last_error.unwrap();
    assert!(reason.contains("pre-marker"));
    assert!(!reason.contains("execution outcome unknown"));
    assert_eq!(
        test.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, finished_at FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get::<_, AgentRunStatus>(0)?, row.get::<_, Option<i64>>(1)?)),
            )
            .unwrap(),
        (AgentRunStatus::Failed, Some(140))
    );
}

#[test]
fn startup_recovery_dead_letters_marker_released_and_dispatched_events() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "startup-dispatched-unknown");
    let runs = AgentRunRepository::new(&test.db);
    runs.mark_gate_release_requested("project-a", run_id).unwrap();
    runs.acknowledge_dispatch("project-a", run_id).unwrap();

    let recovery = runs
        .recover_interrupted(
            140,
            "restart_interruption: execution outcome unknown",
            &BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 99 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();

    assert_eq!(recovery.dead_lettered_events, 1);
    assert_eq!(recovery.requeued_events, 0);
    assert_eq!(EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap().status, EventStatus::DeadLetter);
    assert_eq!(
        test.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get::<_, AgentRunStatus>(0)?, row.get::<_, String>(1)?)),
            )
            .unwrap(),
        (AgentRunStatus::Failed, "released".to_owned())
    );
}

#[test]
fn startup_recovery_leaves_unexpired_unbound_claim_until_lease_expiry() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "startup-unbound-claim", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let before = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();

    AgentRunRepository::new(&test.db)
        .recover_interrupted(
            140,
            "restart_interruption: execution outcome unknown",
            &BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 0 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();
    let during = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
    assert_eq!(during.status, EventStatus::Claimed);
    assert_eq!(during.attempts, before.attempts);
    assert_eq!(during.lease_until, Some(200));

    EventRepository::new(&test.db)
        .recover_expired_claims(201)
        .unwrap();
    let after = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Pending);
    assert_eq!(after.attempts, 0);
    assert_eq!(after.lease_until, None);
}

#[test]
fn startup_recovery_never_infers_marker_evidence_from_an_ambient_path() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "startup-directory-marker", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let log_path = test._temp.path().join("startup-directory-marker.log");
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::create_dir_all(&marker_path).unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();

    runs.recover_interrupted(
        140,
        "daemon restarted",
        &BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 99 },
        )]),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .unwrap();

    let event = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
    assert_eq!(event.status, EventStatus::RetryWait);
    assert!(event.not_before > 100);
    let reason = event.last_error.unwrap();
    assert!(reason.contains("pre-marker"));
}

#[test]
fn startup_recovery_rejects_unexpected_linked_claimed_state_atomically() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "startup-linked-claim", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            120,
            test._temp.path().join("startup-linked-claim.log"),
        ))
        .unwrap();
    runs.attach_event(run.run_id, event_id).unwrap();

    assert!(runs
        .recover_interrupted(
            140,
            "daemon restarted",
            &BTreeMap::from([(
                "project-a".to_owned(),
                RetryPolicy { max_retries: 1 },
            )]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .is_err());
    let event = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
    assert_eq!(event.status, EventStatus::Claimed);
    assert_eq!(event.lease_until, Some(200));
    assert_eq!(
        runs.find_active_by_project("project-a").unwrap().unwrap().status,
        AgentRunStatus::Starting
    );
}

#[test]
fn startup_recovery_promotes_marker_confirmed_release_request_and_retains_applied_interventions() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "marker-confirmed release", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "marker-confirmed-release", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "marker-confirmed-release", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let log_path = test._temp.path().join("marker-confirmed-release.log");
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                log_path.clone(),
            ),
            &[event_id],
            Some("marker-confirmed-release"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4246, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    fs::write(&marker_path, b"started\n").unwrap();

    runs.recover_interrupted(
        140,
        "daemon restarted after child spawn",
        &BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 1 },
        )]),
        &BTreeSet::new(),
        &BTreeSet::from([run.run_id]),
    )
        .unwrap();

    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let run_state: (AgentRunStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        intervention_state,
        (InterventionStatus::Applied, Some(run.run_id))
    );
    assert_eq!(run_state, (AgentRunStatus::Failed, "released".to_owned()));
}

#[test]
fn mark_gate_released_rejects_a_run_without_a_release_request() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "gate-release-failure", 100);
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            100,
            "/tmp/gate-release-failure.log",
        ))
        .unwrap();

    assert!(runs.mark_gate_released("project-a", run.run_id).is_err());
    let gate_state: String = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(gate_state, "pending");
}

#[test]
fn startup_recovery_requeues_an_applied_intervention_before_gate_release() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "restart before release", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "restart-before-release", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "restart-before-release", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/restart-before-release.log",
            ),
            &[event_id],
            Some("restart-before-release"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4243, 130)
        .unwrap();

    runs.recover_interrupted(
        140,
        "daemon restarted before gate release",
        &BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 1 },
        )]),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
        .unwrap();

    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let run_state: (AgentRunStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(intervention_state, (InterventionStatus::Pending, None));
    assert_eq!(run_state, (AgentRunStatus::Failed, "failed".to_owned()));
}

#[test]
fn confirmed_gate_release_does_not_requeue_applied_interventions_on_recovery() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "confirmed release", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "confirmed-release-token", 110, 210, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "confirmed-release", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                120,
                "/tmp/confirmed-release.log",
            ),
            &[event_id],
            Some("confirmed-release-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4244, 130)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.mark_gate_released("project-a", run.run_id).unwrap();

    runs.recover_interrupted(
        140,
        "daemon restarted after gate release",
        &BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 1 },
        )]),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
        .unwrap();

    let intervention_state: (InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
            [&intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let run_state: (AgentRunStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        intervention_state,
        (InterventionStatus::Applied, Some(run.run_id))
    );
    assert_eq!(run_state, (AgentRunStatus::Failed, "released".to_owned()));
}

#[test]
fn schema_v4_migration_preserves_termination_requests_and_adds_dispatching_status() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "v4-migration",
            100,
        ))
        .unwrap()
        .incident;
    let request = TerminationRequestRepository::new(&test.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "migration test",
            100,
            Some(220),
        ))
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute_batch(
            "PRAGMA foreign_keys = OFF;
             DROP TABLE budget_reservations;
             DROP TABLE experiments;
             DROP TABLE proposals;
             DROP TABLE campaigns;
             DROP TABLE interventions;
             PRAGMA user_version = 4;
             PRAGMA foreign_keys = ON;",
        )
        .unwrap();

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    connection
        .execute(
            "UPDATE termination_requests SET status = 'dispatching' WHERE request_id = ?1",
            [request.request_id],
        )
        .unwrap();
    let stored_status: TerminationRequestStatus = connection
        .query_row(
            "SELECT status FROM termination_requests WHERE request_id = ?1",
            [request.request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_status, TerminationRequestStatus::Dispatching);
}

#[test]
fn legacy_migrations_create_active_agent_unique_index() {
    for version in [1, 2] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join(format!("legacy-v{version}.sqlite3"));
        create_legacy_schema_without_active_agent_index(&path, version);

        let db = Db::open(&path).unwrap();
        let connection = db.connect().unwrap();
        let migrated_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        let index_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'agent_runs_one_active_per_project_idx'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migrated_version, LATEST_SCHEMA_VERSION);
        assert_eq!(index_count, 1);
        drop(connection);

        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        register_project(&db, "project-a", &root, "pa-project");
        let first_event = insert_event(&db, "project-a", "first-run", 100);
        let second_event = insert_event(&db, "project-a", "second-run", 101);
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO agent_runs (
                    project_id, primary_event_id, pid, status, started_at, log_path
                 ) VALUES (?1, ?2, NULL, 'starting', ?3, ?4)",
                params!["project-a", first_event, 100, "/tmp/agent-1.log"],
            )
            .unwrap();
        assert!(
            connection
                .execute(
                    "INSERT INTO agent_runs (
                        project_id, primary_event_id, pid, status, started_at, log_path
                     ) VALUES (?1, ?2, NULL, 'running', ?3, ?4)",
                    params!["project-a", second_event, 101, "/tmp/agent-2.log"],
                )
                .is_err(),
            "legacy v{version} migration must enforce one active agent per project"
        );
    }
}

#[test]
fn project_registration_rejects_duplicate_canonical_root_and_group() {
    let test = TestDatabase::new();
    let first_root = test.project_root("first");
    let second_root = test.project_root("second");
    register_project(&test.db, "project-a", &first_root, "pa-first");

    let same_root = NewProject::new(
        "project-b",
        first_root.join("."),
        "pa-second",
        first_root.join(".pueue-agent/other.toml"),
        101,
    );
    let root_error = ProjectRepository::new(&test.db)
        .register(&same_root)
        .unwrap_err();
    assert!(root_error.to_string().contains("root_path"));

    let same_group = NewProject::new(
        "project-c",
        &second_root,
        "pa-first",
        second_root.join(".pueue-agent/config.toml"),
        101,
    );
    let group_error = ProjectRepository::new(&test.db)
        .register(&same_group)
        .unwrap_err();
    assert!(group_error.to_string().contains("pueue_group"));
}

#[test]
fn project_lookup_returns_registered_projects_by_group_and_canonical_root() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");

    let repository = ProjectRepository::new(&test.db);
    let by_group = repository.find_by_group("pa-project").unwrap().unwrap();
    assert_eq!(by_group.project_id, "project-a");
    assert_eq!(by_group.root_path, fs::canonicalize(&root).unwrap());

    let by_root = repository.find_by_root(&root.join(".")).unwrap().unwrap();
    assert_eq!(by_root.project_id, "project-a");
    assert!(repository.find_by_group("missing-group").unwrap().is_none());
    assert!(repository
        .find_by_root(&test._temp.path().join("missing"))
        .unwrap()
        .is_none());
}

#[test]
fn event_insert_is_idempotent_and_foreign_keys_are_enforced() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");

    let first = NewEvent::new(
        "project-a",
        EventKind::TaskFinished,
        "task_finished:41",
        json!({"result": "success"}),
        100,
        100,
    );
    let duplicate = NewEvent::new(
        "project-a",
        EventKind::TaskFailed,
        "task_finished:41",
        json!({"result": "changed"}),
        500,
        500,
    );
    let repository = EventRepository::new(&test.db);
    let inserted = repository.insert_idempotent(&first).unwrap();
    let original = repository.insert_idempotent(&duplicate).unwrap();

    assert_eq!(inserted.event_id, original.event_id);
    assert_eq!(original.kind, EventKind::TaskFinished);
    assert_eq!(original.payload, json!({"result": "success"}));
    assert_eq!(original.status, EventStatus::Pending);

    let missing_project =
        NewEvent::new("missing", EventKind::Crash, "crash:1", json!({}), 100, 100);
    assert!(repository.insert_idempotent(&missing_project).is_err());
}

#[test]
fn claim_batch_claims_only_eligible_pending_and_retry_events() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let pending_id = insert_event(&test.db, "project-a", "pending", 100);
    let future_id = insert_event(&test.db, "project-a", "future", 201);
    let retry_id = insert_event(&test.db, "project-a", "retry", 100);

    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'retry_wait' WHERE event_id = ?1",
            [retry_id],
        )
        .unwrap();

    let claimed = EventRepository::new(&test.db)
        .claim_batch(200, 260, 10)
        .unwrap();
    let claimed_ids = claimed
        .iter()
        .map(|event| event.event_id)
        .collect::<Vec<_>>();
    assert_eq!(claimed_ids, vec![pending_id, retry_id]);
    assert!(claimed
        .iter()
        .all(|event| event.status == EventStatus::Claimed
            && event.lease_until == Some(260)
            && event.attempts == 1));

    let future_status: String = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM events WHERE event_id = ?1",
            [future_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(future_status, "pending");
}

#[test]
fn two_connections_cannot_claim_the_same_events() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    insert_event(&test.db, "project-a", "first", 100);
    insert_event(&test.db, "project-a", "second", 100);

    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = test.path.clone();
            thread::spawn(move || {
                let db = Db::open(&path).unwrap();
                barrier.wait();
                EventRepository::new(&db)
                    .claim_batch(100, 200, 10)
                    .unwrap()
                    .len()
            })
        })
        .collect::<Vec<_>>();

    let mut counts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    counts.sort_unstable();
    assert_eq!(counts, vec![0, 2]);
}

#[test]
fn expired_claim_is_recovered_after_restart_and_can_be_reclaimed() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "restart", 100);
    let first_claim = EventRepository::new(&test.db)
        .claim_batch(100, 110, 1)
        .unwrap();
    assert_eq!(first_claim[0].event_id, event_id);

    let restarted = Db::open(&test.path).unwrap();
    let repository = EventRepository::new(&restarted);
    assert_eq!(repository.recover_expired_claims(109).unwrap(), 0);
    assert_eq!(repository.recover_expired_claims(111).unwrap(), 1);

    let reclaimed = repository.claim_batch(111, 150, 1).unwrap();
    assert_eq!(reclaimed[0].event_id, event_id);
    assert_eq!(reclaimed[0].attempts, 1);
}

#[test]
fn expired_unbound_claim_is_requeued_without_consuming_an_attempt() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "expired-unbound", 100);
    let event = EventRepository::new(&test.db)
        .claim_batch(100, 110, 1)
        .unwrap();
    assert_eq!(event[0].attempts, 1);
    assert_eq!(EventRepository::new(&test.db).recover_expired_claims(110).unwrap(), 1);
    let state: (EventStatus, i64, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, attempts, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Pending, 0, None));
}

#[test]
fn unexpired_unbound_claim_is_left_for_lease_owner() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "unexpired-unbound", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 110, 1)
        .unwrap();
    assert_eq!(EventRepository::new(&test.db).recover_expired_claims(109).unwrap(), 0);
    let state: (EventStatus, i64, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, attempts, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Claimed, 1, Some(110)));
}

#[test]
fn resolve_claimed_without_run_applies_retry_policy_in_a_real_transaction() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "resolve-without-run", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();

    let changed = EventRepository::new(&test.db)
        .resolve_claimed_without_run(
            "project-a",
            &[event_id],
            300,
            "--password SECRET pre-binding crash",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap();
    assert_eq!(changed, 1);
    let state: (EventStatus, Option<i64>, Option<i64>, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, not_before, lease_until, last_error
             FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(state.0, EventStatus::RetryWait);
    assert_eq!(state.1, Some(360));
    assert_eq!(state.2, None);
    assert!(state.3.contains("[REDACTED]"));
    assert!(!state.3.contains("SECRET"));
}

#[test]
fn policy_blocked_claim_dead_letters_without_retry_or_run() {
    let test = TestDatabase::new();
    let root = test.project_root("policy-blocked-claim");
    register_project(&test.db, "project-a", &root, "pa-project");
    let first = insert_event(&test.db, "project-a", "policy-blocked-first", 100);
    let second = insert_event(&test.db, "project-a", "policy-blocked-second", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 300, 2)
        .unwrap();

    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );
    let changed = EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[first, second], 200, &violation)
        .unwrap();
    assert_eq!(changed, 2);
    for event_id in [first, second] {
        let event = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
        assert_eq!(event.status, EventStatus::DeadLetter);
        assert_eq!(event.lease_until, None);
        assert_eq!(event.last_error.as_deref(), Some("policy_blocked:unsafe_codex_argument"));
    }
    assert!(AgentRunRepository::new(&test.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[test]
fn policy_blocked_claim_validation_is_atomic_for_grouped_and_foreign_inputs() {
    let test = TestDatabase::new();
    let root_a = test.project_root("policy-atomic-a");
    let root_b = test.project_root("policy-atomic-b");
    register_project(&test.db, "project-a", &root_a, "pa-policy-atomic");
    register_project(&test.db, "project-b", &root_b, "pb-policy-atomic");
    let first = insert_event(&test.db, "project-a", "policy-atomic-first", 100);
    let _second = insert_event(&test.db, "project-a", "policy-atomic-second", 100);
    let foreign = insert_event(&test.db, "project-b", "policy-atomic-foreign", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 3)
        .unwrap();
    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );

    for ids in [[first, first], [first, 999_999], [first, foreign]] {
        assert!(EventRepository::new(&test.db)
            .dead_letter_claimed_without_run("project-a", &ids, 200, &violation)
            .is_err());
        for event_id in ids {
            if let Some(event) = EventRepository::new(&test.db).find_by_id(event_id).unwrap() {
                assert_eq!(event.status, EventStatus::Claimed);
            }
        }
    }

    let pending = insert_event(&test.db, "project-a", "policy-atomic-pending", 100);
    test.db
        .connect()
        .unwrap()
        .execute("UPDATE events SET not_before = 999 WHERE event_id = ?1", [pending])
        .unwrap();
    assert!(EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[pending], 200, &violation)
        .is_err());
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(pending)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Pending
    );

    let linked = insert_event(&test.db, "project-a", "policy-atomic-linked", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            linked,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/policy-atomic-linked.log",
        ))
        .unwrap();
    AgentRunRepository::new(&test.db)
        .attach_event(run.run_id, linked)
        .unwrap();
    assert!(EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[linked], 200, &violation)
        .is_err());
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(linked)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Claimed
    );
}

#[test]
fn policy_blocked_claim_rejects_expired_or_boundary_lease_atomically() {
    let test = TestDatabase::new();
    let root = test.project_root("policy-lease-validation");
    register_project(&test.db, "project-a", &root, "pa-policy-lease-validation");
    let expired = insert_event(&test.db, "project-a", "policy-expired-lease", 100);
    let valid = insert_event(&test.db, "project-a", "policy-valid-lease", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 250, 2)
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute("UPDATE events SET lease_until = 199 WHERE event_id = ?1", [expired])
        .unwrap();

    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );
    assert!(EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[expired, valid], 200, &violation)
        .is_err());
    for event_id in [expired, valid] {
        let event = EventRepository::new(&test.db).find_by_id(event_id).unwrap().unwrap();
        assert_eq!(event.status, EventStatus::Claimed);
    }

    let exact = insert_event(&test.db, "project-a", "policy-exact-lease", 100);
    EventRepository::new(&test.db)
        .claim_batch(200, 250, 1)
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute("UPDATE events SET lease_until = 200 WHERE event_id = ?1", [exact])
        .unwrap();
    assert!(EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[exact], 200, &violation)
        .is_err());
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(exact)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Claimed
    );
}

#[test]
fn policy_blocked_claim_rejects_null_lease_before_mutation() {
    let test = TestDatabase::new();
    let root = test.project_root("policy-null-lease");
    register_project(&test.db, "project-a", &root, "pa-policy-null-lease");
    let event_id = insert_event(&test.db, "project-a", "policy-null-lease", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 250, 1)
        .unwrap();
    let connection = test.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON;")
        .unwrap();
    connection
        .execute("UPDATE events SET lease_until = NULL WHERE event_id = ?1", [event_id])
        .unwrap();
    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );
    assert!(EventRepository::new(&test.db)
        .dead_letter_claimed_without_run("project-a", &[event_id], 200, &violation)
        .is_err());
    assert_eq!(
        EventRepository::new(&test.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
            .status,
        EventStatus::Claimed
    );
}

#[test]
fn event_repository_rejects_oversized_claim_and_policy_batches() {
    let test = TestDatabase::new();
    assert!(matches!(
        EventRepository::new(&test.db).claim_batch(100, 200, MAX_EVENT_LIST_LIMIT + 1),
        Err(AppError::Configuration {
            field: "event_claim_limit"
        })
    ));
    let violation = PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    );
    let ids = vec![1_i64; MAX_EVENT_LIST_LIMIT + 1];
    assert!(matches!(
        EventRepository::new(&test.db)
            .dead_letter_claimed_without_run("project-a", &ids, 200, &violation),
        Err(AppError::Validation {
            field: "event_ids",
            ..
        })
    ));
}

#[test]
fn resolve_claimed_without_run_rejects_a_claim_linked_to_a_run() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "linked-claim-resolution", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/linked-claim-resolution.log",
        ))
        .unwrap();
    AgentRunRepository::new(&test.db)
        .attach_event(run.run_id, event_id)
        .unwrap();

    let error = EventRepository::new(&test.db)
        .resolve_claimed_without_run(
            "project-a",
            &[event_id],
            300,
            "binding crash",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (EventStatus, i64, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, attempts, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Claimed, 1, Some(200)));
}

#[test]
fn recover_expired_claims_leaves_an_expired_claim_linked_to_a_run_untouched() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "linked-expired-claim", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 110, 1)
        .unwrap();
    let run = AgentRunRepository::new(&test.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/linked-expired-claim.log",
        ))
        .unwrap();
    AgentRunRepository::new(&test.db)
        .attach_event(run.run_id, event_id)
        .unwrap();

    assert_eq!(EventRepository::new(&test.db).recover_expired_claims(111).unwrap(), 0);
    let state: (EventStatus, i64, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, attempts, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Claimed, 1, Some(110)));
}

#[test]
fn resolve_claimed_without_run_rejects_duplicate_event_ids_atomically() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "duplicate-claim-resolution", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let error = EventRepository::new(&test.db)
        .resolve_claimed_without_run(
            "project-a",
            &[event_id, event_id],
            300,
            "duplicate event id",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (EventStatus, i64, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, attempts, lease_until FROM events WHERE event_id = ?1",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (EventStatus::Claimed, 1, Some(200)));
}

#[test]
fn resolve_claimed_without_run_rejects_missing_event_without_resolving_prior_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "missing-claim-resolution", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let error = EventRepository::new(&test.db)
        .resolve_claimed_without_run(
            "project-a",
            &[event_id, 999_999],
            300,
            "missing event",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let status: EventStatus = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, EventStatus::Claimed);
}

#[test]
fn resolve_claimed_without_run_rejects_a_non_claimed_event() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "pending-claim-resolution", 100);
    let error = EventRepository::new(&test.db)
        .resolve_claimed_without_run(
            "project-a",
            &[event_id],
            300,
            "wrong state",
            RetryPolicy { max_retries: 2 },
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let status: EventStatus = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, EventStatus::Pending);
}

#[test]
fn generic_finalizer_requires_released_gate_and_dispatched_events() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "generic-phase-guard");
    let runs = AgentRunRepository::new(&test.db);
    let error = runs
        .finish_and_resolve_events(
            "project-a",
            run_id,
            AgentRunStatus::Failed,
            300,
            Some(1),
            Some("wrong phase"),
            EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));

    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET launch_gate_state = 'release_requested'
             WHERE run_id = ?1",
            [run_id],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dispatched' WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    let error = runs
        .finish_and_resolve_events(
            "project-a",
            run_id,
            AgentRunStatus::Failed,
            300,
            Some(1),
            Some("wrong phase"),
            EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (AgentRunStatus, EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, agent_runs.launch_gate_state
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_run_events.run_id = agent_runs.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, EventStatus::Dispatched, "release_requested".to_owned()));
}

#[test]
fn marker_failure_finalizer_requires_release_requested_gate_and_inflight_events() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "marker-phase-guard");
    let runs = AgentRunRepository::new(&test.db);
    let error = runs
        .finish_after_marker_failure("project-a", run_id, 300, "marker failure")
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));

    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET launch_gate_state = 'release_requested'
             WHERE run_id = ?1",
            [run_id],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dispatched' WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    let error = runs
        .finish_after_marker_failure("project-a", run_id, 300, "marker failure")
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (AgentRunStatus, EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, agent_runs.launch_gate_state
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_runs.run_id = agent_run_events.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, EventStatus::Dispatched, "release_requested".to_owned()));
}

#[test]
fn pre_release_policy_finalizer_requires_inflight_events() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "pre-release-phase-guard");
    let runs = AgentRunRepository::new(&test.db);
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET launch_gate_state = 'release_requested'
             WHERE run_id = ?1",
            [run_id],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dispatched' WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    let error = runs
        .fail_before_gate_release_with_policy(
            "project-a",
            run_id,
            300,
            "pre-release phase mismatch",
            RetryPolicy { max_retries: 0 },
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (AgentRunStatus, EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, agent_runs.launch_gate_state
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_runs.run_id = agent_run_events.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, EventStatus::Dispatched, "release_requested".to_owned()));
}

#[test]
fn finalizer_rejects_non_terminal_target_status_without_mutation() {
    let test = TestDatabase::new();
    let (run_id, event_id) = bind_starting_run(&test, "terminal-status-guard");
    let runs = AgentRunRepository::new(&test.db);
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dispatched' WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    let error = runs
        .finish_and_resolve_events(
            "project-a",
            run_id,
            AgentRunStatus::Starting,
            300,
            None,
            Some("invalid terminal target"),
            EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let state: (AgentRunStatus, EventStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_runs.run_id = agent_run_events.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, EventStatus::Dispatched));
}

#[test]
fn agent_run_binding_moves_claimed_events_to_in_flight_atomically() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let first_event = insert_event(&test.db, "project-a", "binding-first", 100);
    let second_event = insert_event(&test.db, "project-a", "binding-second", 100);
    let claimed = EventRepository::new(&test.db)
        .claim_batch(100, 200, 2)
        .unwrap();
    assert_eq!(claimed.len(), 2);

    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                first_event,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/binding.log",
            ),
            &[first_event, second_event],
            None,
        )
        .unwrap();
    let connection = test.db.connect().unwrap();
    let event_states: Vec<(i64, EventStatus, Option<i64>)> = connection
        .prepare(
            "SELECT event_id, status, lease_until FROM events
             WHERE event_id IN (?1, ?2) ORDER BY event_id",
        )
        .unwrap()
        .query_map(params![first_event, second_event], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        event_states,
        vec![
            (first_event, EventStatus::InFlight, None),
            (second_event, EventStatus::InFlight, None),
        ]
    );
    let run_status: AgentRunStatus = connection
        .query_row(
            "SELECT status FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(run_status, AgentRunStatus::Starting);
    let link_projects: Vec<String> = connection
        .prepare(
            "SELECT project_id FROM agent_run_events
             WHERE run_id = ?1 ORDER BY event_id",
        )
        .unwrap()
        .query_map([run.run_id], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(link_projects, vec!["project-a", "project-a"]);

    let rollback_test = TestDatabase::new();
    let rollback_root = rollback_test.project_root("project");
    register_project(
        &rollback_test.db,
        "project-a",
        &rollback_root,
        "pa-project",
    );
    let first_event = insert_event(
        &rollback_test.db,
        "project-a",
        "binding-rollback-first",
        100,
    );
    let second_event = insert_event(
        &rollback_test.db,
        "project-a",
        "binding-rollback-second",
        100,
    );
    EventRepository::new(&rollback_test.db)
        .claim_batch(100, 200, 2)
        .unwrap();
    let second_trigger = rollback_test.db.connect().unwrap();
    second_trigger
        .execute_batch(&format!(
            "CREATE TRIGGER reject_second_event_binding
             BEFORE INSERT ON agent_run_events
             WHEN NEW.event_id = {second_event}
             BEGIN
                 SELECT RAISE(ABORT, 'injected second event binding failure');
             END;",
        ))
        .unwrap();
    drop(second_trigger);

    let failed = AgentRunRepository::new(&rollback_test.db).insert_with_events_and_reservation(
        &NewAgentRun::new(
            "project-a",
            first_event,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/binding-rollback.log",
        ),
        &[first_event, second_event],
        None,
    );
    assert!(failed.is_err());
    let rollback_states: Vec<(i64, EventStatus, Option<i64>)> = rollback_test
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT event_id, status, lease_until FROM events
             WHERE event_id IN (?1, ?2) ORDER BY event_id",
        )
        .unwrap()
        .query_map(params![first_event, second_event], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rollback_states,
        vec![
            (first_event, EventStatus::Claimed, Some(200)),
            (second_event, EventStatus::Claimed, Some(200)),
        ]
    );
    let rollback_run_count: i64 = rollback_test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE primary_event_id = ?1",
            [first_event],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(rollback_run_count, 0);
}

#[test]
fn dispatch_ack_moves_only_project_owned_inflight_events() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let event_a = insert_event(&test.db, "project-a", "dispatch-a", 100);
    let event_b = insert_event(&test.db, "project-b", "dispatch-b", 100);
    let events = EventRepository::new(&test.db);
    events.claim_batch(100, 200, 1).unwrap();
    events.claim_batch(100, 200, 1).unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run_a = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_a,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/dispatch-a.log",
            ),
            &[event_a],
        )
        .unwrap();
    let run_b = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                event_b,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/dispatch-b.log",
            ),
            &[event_b],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run_a.run_id)
        .unwrap();
    runs.mark_gate_release_requested("project-b", run_b.run_id)
        .unwrap();

    assert_eq!(runs.acknowledge_dispatch("project-a", run_a.run_id).unwrap(), 1);
    let own_state: (EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT events.status, agent_runs.launch_gate_state
             FROM events JOIN agent_run_events USING (project_id, event_id)
             JOIN agent_runs USING (project_id, run_id)
             WHERE agent_runs.run_id = ?1",
            [run_a.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(own_state, (EventStatus::Dispatched, "released".to_owned()));

    let error = runs
        .acknowledge_dispatch("project-a", run_b.run_id)
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));
    let foreign_state: (EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT events.status, agent_runs.launch_gate_state
             FROM events JOIN agent_run_events USING (project_id, event_id)
             JOIN agent_runs USING (project_id, run_id)
             WHERE agent_runs.run_id = ?1",
            [run_b.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(foreign_state, (EventStatus::InFlight, "release_requested".to_owned()));
}

#[test]
fn failed_group_resolves_events_independently_by_attempt() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let first_event = insert_event(&test.db, "project-a", "failure-first", 100);
    let second_event = insert_event(&test.db, "project-a", "failure-second", 100);
    let events = EventRepository::new(&test.db);
    events.claim_batch(100, 200, 2).unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET attempts = 3 WHERE event_id = ?1",
            [second_event],
        )
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                first_event,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/failure-group.log",
            ),
            &[first_event, second_event],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.acknowledge_dispatch("project-a", run.run_id).unwrap();
    runs.finish_and_resolve_events(
        "project-a",
        run.run_id,
        AgentRunStatus::Failed,
        200,
        Some(1),
        Some("group failed"),
        EventResolution::RetryPolicy(RetryPolicy { max_retries: 2 }),
    )
    .unwrap();

    let states: Vec<(i64, EventStatus, Option<i64>, Option<i64>)> = test
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT event_id, status, not_before, lease_until FROM events
             WHERE event_id IN (?1, ?2) ORDER BY event_id",
        )
        .unwrap()
        .query_map(params![first_event, second_event], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        states,
        vec![
            (first_event, EventStatus::RetryWait, Some(260), None),
            (second_event, EventStatus::DeadLetter, Some(100), None),
        ]
    );
}

#[test]
fn finish_and_resolve_events_completes_only_after_run_success() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let applied = interventions
        .insert_pending("project-a", "keep audit record", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "applied-token", 101, 300, 1, 1024)
        .unwrap();
    let reserved = interventions
        .insert_pending("project-a", "return to pending", 102)
        .unwrap();
    interventions
        .reserve_pending("project-a", "reserved-token", 103, 300, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "success-event", 100);
    let events = EventRepository::new(&test.db);
    events.claim_batch(100, 200, 1).unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/success-event.log",
            ),
            &[event_id],
            Some("applied-token"),
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET agent_run_id = ?1
             WHERE intervention_id = ?2",
            params![run.run_id, reserved.intervention_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 120)
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET status = 'reserved', applied_at = NULL,
             reserved_at = 103, lease_expires_at = 300,
             reservation_token = 'reserved-token' WHERE intervention_id = ?1",
            [&reserved.intervention_id],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.acknowledge_dispatch("project-a", run.run_id).unwrap();

    runs.finish_and_resolve_events(
        "project-a",
        run.run_id,
        AgentRunStatus::Completed,
        200,
        Some(0),
        None,
        EventResolution::RetryPolicy(RetryPolicy { max_retries: 2 }),
    )
    .unwrap();
    let committed: (AgentRunStatus, EventStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_run_events.run_id = agent_runs.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.project_id = ?1 AND agent_runs.run_id = ?2",
            params!["project-a", run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(committed, (AgentRunStatus::Completed, EventStatus::Completed));
    let intervention_states: Vec<(String, InterventionStatus, Option<i64>)> = test
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT intervention_id, status, agent_run_id FROM interventions
             WHERE intervention_id IN (?1, ?2) ORDER BY intervention_id",
        )
        .unwrap()
        .query_map(params![applied.intervention_id, reserved.intervention_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(intervention_states.contains(&(
        applied.intervention_id,
        InterventionStatus::Applied,
        Some(run.run_id),
    )));
    assert!(intervention_states.contains(&(
        reserved.intervention_id,
        InterventionStatus::Pending,
        None,
    )));
}

#[test]
fn finish_and_resolve_events_rolls_back_run_events_and_interventions_on_sql_failure() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "rollback intervention", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "rollback-token", 101, 300, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "rollback-finalizer", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/rollback-finalizer.log",
            ),
            &[event_id],
            Some("rollback-token"),
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.acknowledge_dispatch("project-a", run.run_id).unwrap();
    test.db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_finalizer_event_update
             BEFORE UPDATE OF status ON events
             WHEN OLD.status = 'dispatched' AND NEW.status = 'completed'
             BEGIN
                 SELECT RAISE(ABORT, 'injected event finalization failure');
             END;",
        )
        .unwrap();

    assert!(runs
        .finish_and_resolve_events(
            "project-a",
            run.run_id,
            AgentRunStatus::Completed,
            200,
            Some(0),
            None,
            EventResolution::RetryPolicy(RetryPolicy { max_retries: 2 }),
        )
        .is_err());
    let state: (AgentRunStatus, EventStatus, InterventionStatus) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, interventions.status
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_run_events.run_id = agent_runs.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             JOIN interventions
               ON interventions.project_id = agent_runs.project_id
              AND interventions.agent_run_id = agent_runs.run_id
             WHERE agent_runs.run_id = ?1 AND interventions.intervention_id = ?2",
            params![run.run_id, intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Starting,
            EventStatus::Dispatched,
            InterventionStatus::Reserved,
        )
    );
}

#[test]
fn post_marker_finalizer_dead_letters_without_consuming_attempts() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let interventions = InterventionRepository::new(&test.db);
    let intervention = interventions
        .insert_pending("project-a", "retain applied audit", 100)
        .unwrap();
    interventions
        .reserve_pending("project-a", "post-marker-token", 101, 300, 1, 1024)
        .unwrap();
    let event_id = insert_event(&test.db, "project-a", "post-marker-finalizer", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute("UPDATE events SET attempts = 3 WHERE event_id = ?1", [event_id])
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/post-marker-finalizer.log",
            ),
            &[event_id],
            Some("post-marker-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 4242, 120)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.acknowledge_dispatch("project-a", run.run_id).unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET launch_gate_state = 'release_requested'
             WHERE run_id = ?1",
            [run.run_id],
        )
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'in_flight' WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();

    runs.finish_after_marker_failure("project-a", run.run_id, 200, "post_marker_dispatch_ack")
        .unwrap();
    let state: (AgentRunStatus, EventStatus, i64, InterventionStatus, Option<i64>) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, events.attempts,
                    interventions.status, interventions.agent_run_id
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_run_events.run_id = agent_runs.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             JOIN interventions
               ON interventions.project_id = agent_runs.project_id
              AND interventions.agent_run_id = agent_runs.run_id
             WHERE agent_runs.run_id = ?1 AND interventions.intervention_id = ?2",
            params![run.run_id, intervention.intervention_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Failed,
            EventStatus::DeadLetter,
            3,
            InterventionStatus::Applied,
            Some(run.run_id),
        )
    );
}

#[test]
fn cross_project_dispatch_and_finish_are_rejected() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let event_b = insert_event(&test.db, "project-b", "cross-project-finalizer", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run_b = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                event_b,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/cross-project-finalizer.log",
            ),
            &[event_b],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-b", run_b.run_id)
        .unwrap();
    let dispatch_error = runs
        .acknowledge_dispatch("project-a", run_b.run_id)
        .unwrap_err();
    assert!(matches!(dispatch_error, AppError::Validation { .. }));
    let finish_error = runs
        .finish_and_resolve_events(
            "project-a",
            run_b.run_id,
            AgentRunStatus::Failed,
            200,
            Some(1),
            Some("foreign run"),
            EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
        )
        .unwrap_err();
    assert!(matches!(finish_error, AppError::Validation { .. }));
    let state: (AgentRunStatus, EventStatus, String) = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT agent_runs.status, events.status, agent_runs.launch_gate_state
             FROM agent_runs JOIN agent_run_events
               ON agent_run_events.project_id = agent_runs.project_id
              AND agent_run_events.run_id = agent_runs.run_id
             JOIN events
               ON events.project_id = agent_run_events.project_id
              AND events.event_id = agent_run_events.event_id
             WHERE agent_runs.run_id = ?1",
            [run_b.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, EventStatus::InFlight, "release_requested".to_owned()));
}

#[test]
fn event_terminal_error_is_bounded_and_redacted_at_repository_boundary() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "bounded-error", 100);
    EventRepository::new(&test.db)
        .claim_batch(100, 200, 1)
        .unwrap();
    let runs = AgentRunRepository::new(&test.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                110,
                "/tmp/bounded-error.log",
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    runs.acknowledge_dispatch("project-a", run.run_id).unwrap();
    let reason = format!("--password SECRET {}", "x".repeat(1_000));
    runs.finish_and_resolve_events(
        "project-a",
        run.run_id,
        AgentRunStatus::Failed,
        200,
        Some(1),
        Some(&reason),
        EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
    )
    .unwrap();
    let stored: String = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT last_error FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(stored.len() <= 240);
    assert!(stored.contains("[REDACTED]"));
    assert!(!stored.contains("SECRET"));
}

#[test]
fn resolved_incident_can_recur_without_duplicate_active_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = IncidentRepository::new(&test.db);

    let first = NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 100);
    let opened = repository.upsert_active(&first).unwrap();
    assert_eq!(opened.transition, IncidentTransition::Opened);

    let unchanged = repository.upsert_active(&first).unwrap();
    assert_eq!(unchanged.incident.incident_id, opened.incident.incident_id);
    assert_eq!(unchanged.transition, IncidentTransition::Unchanged);

    let updated_input =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 101);
    let updated = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(updated.incident.incident_id, opened.incident.incident_id);
    assert_eq!(updated.transition, IncidentTransition::Unchanged);
    assert_eq!(updated.incident.last_seen_at, 101);

    assert_eq!(
        repository
            .resolve(opened.incident.incident_id, 102)
            .unwrap(),
        IncidentTransition::Resolved
    );
    let stale = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(stale.transition, IncidentTransition::Unchanged);
    assert_eq!(stale.incident.incident_id, opened.incident.incident_id);
    assert_eq!(stale.incident.status, IncidentStatus::Resolved);

    let at_resolution =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 102);
    let same_time = repository.upsert_active(&at_resolution).unwrap();
    assert_eq!(same_time.transition, IncidentTransition::Unchanged);
    assert_eq!(same_time.incident.incident_id, opened.incident.incident_id);

    let later_input =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 103);
    let recurrence = repository.upsert_active(&later_input).unwrap();
    assert_eq!(recurrence.transition, IncidentTransition::Opened);
    assert_ne!(recurrence.incident.incident_id, opened.incident.incident_id);

    let delayed_stale = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(delayed_stale.transition, IncidentTransition::Unchanged);
    assert_eq!(
        delayed_stale.incident.incident_id,
        opened.incident.incident_id
    );
    assert_eq!(delayed_stale.incident.status, IncidentStatus::Resolved);

    let connection = test.db.connect().unwrap();
    let active_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM incidents
             WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
               AND status IN ('open', 'acknowledged')",
            params!["project-a", "pattern", "nan-loss:abc"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_count, 1);
}

#[test]
fn active_incident_reports_updated_only_when_task_identity_changes() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = IncidentRepository::new(&test.db);

    let opened = repository
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-41"),
            "nan-loss:abc",
            100,
        ))
        .unwrap();
    let updated = repository
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-42"),
            "nan-loss:abc",
            101,
        ))
        .unwrap();

    assert_eq!(updated.incident.incident_id, opened.incident.incident_id);
    assert_eq!(updated.incident.task_key.as_deref(), Some("task-42"));
    assert_eq!(updated.incident.last_seen_at, 101);
    assert_eq!(updated.transition, IncidentTransition::Updated);
}

#[test]
fn cross_project_foreign_keys_reject_agent_run_relationships() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let event_a = insert_event(&test.db, "project-a", "event-a", 100);
    let event_b = insert_event(&test.db, "project-b", "event-b", 100);

    let connection = test.db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?3, ?4)",
            params!["project-a", event_a, 100, "/tmp/agent-a.log"],
        )
        .unwrap();
    let run_a = connection.last_insert_rowid();

    assert!(connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?3, ?4)",
            params!["project-a", event_b, 100, "/tmp/agent-cross-event.log"],
        )
        .is_err());

    assert!(connection
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)",
            params!["project-a", run_a, event_b],
        )
        .is_err());

    assert!(connection
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)",
            params!["project-b", run_a, event_b],
        )
        .is_err());
}

#[test]
fn cross_project_foreign_key_rejects_termination_request_incident() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");

    let incident_a = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "fingerprint-a",
            100,
        ))
        .unwrap()
        .incident
        .incident_id;
    let connection = test.db.connect().unwrap();
    assert!(connection
        .execute(
            "INSERT INTO termination_requests (
                incident_id, project_id, task_signature, reason, status, requested_at
             ) VALUES (?1, ?2, ?3, ?4, 'requested', ?5)",
            params![incident_a, "project-b", "signature-b", "cross-project", 100],
        )
        .is_err());
}

#[test]
fn only_one_active_agent_run_is_allowed_per_project() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let first_event = insert_event(&test.db, "project-a", "first-run", 100);
    let second_event = insert_event(&test.db, "project-a", "second-run", 101);
    let connection = test.db.connect().unwrap();

    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, log_path
             ) VALUES (?1, ?2, NULL, 'starting', ?3, ?4)",
            params!["project-a", first_event, 100, "/tmp/agent-1.log"],
        )
        .unwrap();
    assert!(connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, log_path
             ) VALUES (?1, ?2, NULL, 'running', ?3, ?4)",
            params!["project-a", second_event, 101, "/tmp/agent-2.log"],
        )
        .is_err());

    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?4, ?5)",
            params!["project-a", second_event, 101, 102, "/tmp/agent-2.log"],
        )
        .unwrap();
}

#[test]
fn agent_run_insert_with_events_persists_the_run_and_all_event_links() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let primary_event_id = insert_event(&test.db, "project-a", "primary-event", 100);
    let related_event_id = insert_event(&test.db, "project-a", "related-event", 101);
    EventRepository::new(&test.db)
        .claim_batch(101, 200, 2)
        .unwrap();

    let run = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                primary_event_id,
                None,
                AgentRunStatus::Starting,
                102,
                "/tmp/agent.log",
            ),
            &[primary_event_id, related_event_id],
        )
        .unwrap();

    let connection = test.db.connect().unwrap();
    let mut statement = connection
        .prepare(
            "SELECT event_id FROM agent_run_events
             WHERE run_id = ?1 ORDER BY event_id",
        )
        .unwrap();
    let event_ids = statement
        .query_map([run.run_id], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(event_ids, vec![primary_event_id, related_event_id]);
}

#[test]
fn agent_run_insert_with_events_rolls_back_when_an_event_attachment_is_invalid() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let primary_event_id = insert_event(&test.db, "project-a", "primary-event", 100);
    let cross_project_event_id = insert_event(&test.db, "project-b", "cross-project-event", 101);
    let events = EventRepository::new(&test.db);
    events.claim_batch(100, 200, 1).unwrap();
    events.claim_batch(100, 200, 1).unwrap();

    let error = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                primary_event_id,
                None,
                AgentRunStatus::Starting,
                102,
                "/tmp/agent.log",
            ),
            &[primary_event_id, cross_project_event_id],
        )
        .unwrap_err();
    assert!(matches!(error, AppError::Validation { .. }));

    let connection = test.db.connect().unwrap();
    let run_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| row.get(0))
        .unwrap();
    let attachment_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM agent_run_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(run_count, 0);
    assert_eq!(attachment_count, 0);
}

#[test]
fn typed_repositories_round_trip_future_task_records() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "typed-records", 100);

    let submission_repository = SubmissionRepository::new(&test.db);
    let submission = NewSubmission::new(
        "submission-1",
        "project-a",
        vec!["python".to_owned(), "train.py".to_owned()],
        100,
    );
    let inserted_submission = submission_repository
        .insert_idempotent(&submission)
        .unwrap();
    let duplicate_submission = submission_repository
        .insert_idempotent(&NewSubmission {
            status: SubmissionStatus::Accepted,
            ..submission.clone()
        })
        .unwrap();
    assert_eq!(
        inserted_submission.submission_id,
        duplicate_submission.submission_id
    );
    assert_eq!(duplicate_submission.status, SubmissionStatus::Pending);
    assert_eq!(
        submission_repository
            .find_by_id("submission-1")
            .unwrap()
            .unwrap()
            .argv,
        vec!["python".to_owned(), "train.py".to_owned()]
    );

    let agent_run_repository = AgentRunRepository::new(&test.db);
    let run = agent_run_repository
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            100,
            "/tmp/agent.log",
        ))
        .unwrap();
    assert_eq!(
        agent_run_repository
            .find_active_by_project("project-a")
            .unwrap()
            .unwrap()
            .run_id,
        run.run_id
    );
    agent_run_repository
        .attach_event(run.run_id, event_id)
        .unwrap();

    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "typed-fingerprint",
            100,
        ))
        .unwrap()
        .incident;
    let termination_repository = TerminationRequestRepository::new(&test.db);
    let request = NewTerminationRequest::new(
        incident.incident_id,
        "project-a",
        "signature-a",
        "fatal pattern",
        100,
        Some(110),
    );
    let inserted_request = termination_repository.insert_idempotent(&request).unwrap();
    let duplicate_request = termination_repository.insert_idempotent(&request).unwrap();
    assert_eq!(inserted_request.request_id, duplicate_request.request_id);
    assert_eq!(
        duplicate_request.status,
        TerminationRequestStatus::Requested
    );
    assert_eq!(
        termination_repository
            .find_by_id(inserted_request.request_id)
            .unwrap()
            .unwrap(),
        inserted_request
    );

    let observation_repository = TaskObservationRepository::new(&test.db);
    let observation = NewTaskObservation::new(
        "project-a",
        "signature-a",
        41,
        "pa-project",
        vec!["python".to_owned(), "train.py".to_owned()],
        "running",
        None,
        Some(101),
        None,
        None,
        101,
    );
    observation_repository.upsert(&observation).unwrap();
    let updated_observation = NewTaskObservation {
        state: "finished".to_owned(),
        observed_at: 102,
        ..observation
    };
    let stored = observation_repository.upsert(&updated_observation).unwrap();
    assert_eq!(stored.state, "finished");
    assert_eq!(
        observation_repository
            .find("project-a", "signature-a")
            .unwrap()
            .unwrap()
            .observed_at,
        102
    );
    assert_eq!(
        observation_repository
            .first_observed_at("project-a", "signature-a")
            .unwrap(),
        Some(101)
    );
}

#[test]
fn task_observation_migration_backfills_first_observed_at_without_changing_latest() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    TaskObservationRepository::new(&test.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            "signature-a",
            41,
            "pa-project",
            vec!["python".to_owned(), "train.py".to_owned()],
            "running",
            None,
            None,
            None,
            None,
            2_000,
        ))
        .unwrap();

    let connection = Connection::open(&test.path).unwrap();
    connection
        .execute_batch(
            r#"
            ALTER TABLE task_observations RENAME TO task_observations_v11;
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
            INSERT INTO task_observations (
                project_id, task_signature, pueue_task_id, pueue_group, command_json,
                state, enqueued_at, started_at, ended_at, result, observed_at
            )
            SELECT project_id, task_signature, pueue_task_id, pueue_group, command_json,
                   state, enqueued_at, started_at, ended_at, result, observed_at
            FROM task_observations_v11;
            DROP TABLE task_observations_v11;
            PRAGMA user_version = 11;
            "#,
        )
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let observations = TaskObservationRepository::new(&migrated);
    assert_eq!(
        observations
            .first_observed_at("project-a", "signature-a")
            .unwrap(),
        Some(2_000)
    );
    assert_eq!(
        observations
            .find("project-a", "signature-a")
            .unwrap()
            .unwrap()
            .observed_at,
        2_000
    );
}

#[test]
fn diagnostics_filters_events_by_project_kind_status_and_bounded_deterministic_order() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let repository = EventRepository::new(&test.db);

    let task_failed = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "task-failed",
            json!({"task_id": 41}),
            100,
            100,
        ))
        .unwrap();
    let first_crash = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "first-crash",
            json!({"task_id": 41}),
            200,
            200,
        ))
        .unwrap();
    let second_crash = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "second-crash",
            json!({"task_id": 41}),
            200,
            200,
        ))
        .unwrap();
    repository
        .insert_idempotent(&NewEvent::new(
            "project-b",
            EventKind::Crash,
            "foreign-crash",
            json!({"task_id": 41}),
            300,
            300,
        ))
        .unwrap();
    repository
        .transition_many(
            &[first_crash.event_id],
            EventStatus::Completed,
            400,
            None,
            None,
        )
        .unwrap();

    let crashes = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(Some(EventKind::Crash), None, 10),
        )
        .unwrap();
    assert_eq!(
        crashes
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        vec![second_crash.event_id, first_crash.event_id]
    );
    assert!(crashes
        .iter()
        .all(|event| event.project_id == "project-a" && event.kind == EventKind::Crash));

    let completed = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(None, Some(EventStatus::Completed), 10),
        )
        .unwrap();
    assert_eq!(
        completed
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        vec![first_crash.event_id]
    );
    assert_ne!(task_failed.status, EventStatus::Completed);

    for index in 0..=MAX_EVENT_LIST_LIMIT {
        repository
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::Stalled,
                format!("bounded-crash-{index}"),
                json!({"task_id": index}),
                1,
                1,
            ))
            .unwrap();
    }
    let bounded = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(None, None, MAX_EVENT_LIST_LIMIT + 1),
        )
        .unwrap();
    assert_eq!(bounded.len(), MAX_EVENT_LIST_LIMIT);
    assert!(repository
        .list_filtered("project-a", &EventFilter::new(None, None, 0))
        .unwrap()
        .is_empty());
}

#[test]
fn diagnostics_scopes_task_relations_by_project_and_stable_signature() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let observations = TaskObservationRepository::new(&test.db);

    for (project_id, signature, group, observed_at) in [
        ("project-a", "signature-old", "pa-a", 100),
        ("project-a", "signature-alpha", "pa-a", 200),
        ("project-a", "signature-beta", "pa-a", 200),
        ("project-b", "signature-beta", "pa-b", 300),
    ] {
        observations
            .upsert(&NewTaskObservation::new(
                project_id,
                signature,
                41,
                group,
                vec!["python".to_owned(), "train.py".to_owned()],
                "running",
                None,
                Some(observed_at),
                None,
                None,
                observed_at,
            ))
            .unwrap();
    }
    assert_eq!(
        observations
            .find_by_pueue_task("project-a", 41, 10)
            .unwrap()
            .iter()
            .map(|observation| observation.task_signature.as_str())
            .collect::<Vec<_>>(),
        vec!["signature-beta", "signature-alpha", "signature-old"]
    );
    assert_eq!(
        observations
            .find_by_pueue_task("project-a", 41, 1)
            .unwrap()
            .iter()
            .map(|observation| observation.task_signature.as_str())
            .collect::<Vec<_>>(),
        vec!["signature-beta"]
    );
    assert!(observations
        .find_by_pueue_task("project-a", 41, 0)
        .unwrap()
        .is_empty());

    let submissions = SubmissionRepository::new(&test.db);
    for (submission_id, project_id, signature) in [
        ("submission-old", "project-a", "signature-old"),
        ("submission-beta", "project-a", "signature-beta"),
        ("submission-beta-2", "project-a", "signature-beta"),
        ("submission-foreign", "project-b", "signature-beta"),
    ] {
        submissions
            .insert_idempotent(&NewSubmission::new(
                submission_id,
                project_id,
                vec!["python".to_owned(), "train.py".to_owned()],
                100,
            ))
            .unwrap();
        submissions
            .mark_accepted(submission_id, 41, signature)
            .unwrap();
    }
    assert_eq!(
        submissions
            .find_by_task_signature("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-beta-2", "submission-beta"]
    );
    assert_eq!(
        submissions
            .find_by_task_signature("project-a", "signature-beta", 1)
            .unwrap()
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-beta-2"]
    );
    assert!(submissions
        .find_by_task_signature("project-a", "signature-beta", 0)
        .unwrap()
        .is_empty());
    assert!(submissions
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|submission| submission.project_id == "project-a"));

    let incidents = IncidentRepository::new(&test.db);
    let incident_old = incidents
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("signature-old"),
            "old-fingerprint",
            100,
        ))
        .unwrap()
        .incident;
    let incident_beta = incidents
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("signature-beta"),
            "beta-fingerprint",
            200,
        ))
        .unwrap()
        .incident;
    let incident_beta_second = incidents
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("signature-beta"),
            "beta-fingerprint-2",
            200,
        ))
        .unwrap()
        .incident;
    let foreign_incident = incidents
        .upsert_active(&NewIncident::new(
            "project-b",
            "pattern",
            Some("signature-beta"),
            "foreign-fingerprint",
            300,
        ))
        .unwrap()
        .incident;
    assert_eq!(
        incidents
            .find_by_task_key("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|incident| incident.incident_id)
            .collect::<Vec<_>>(),
        vec![incident_beta_second.incident_id, incident_beta.incident_id]
    );
    assert_eq!(
        incidents
            .find_by_task_key("project-a", "signature-beta", 1)
            .unwrap()
            .iter()
            .map(|incident| incident.incident_id)
            .collect::<Vec<_>>(),
        vec![incident_beta_second.incident_id]
    );
    assert!(incidents
        .find_by_task_key("project-a", "signature-beta", 0)
        .unwrap()
        .is_empty());
    assert!(incidents
        .find_by_project_and_id("project-a", foreign_incident.incident_id)
        .unwrap()
        .is_none());
    assert!(incidents
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|incident| incident.project_id == "project-a"));

    let terminations = TerminationRequestRepository::new(&test.db);
    let request_old = terminations
        .insert_idempotent(&NewTerminationRequest::new(
            incident_old.incident_id,
            "project-a",
            "signature-old",
            "diagnostic relation",
            100,
            None,
        ))
        .unwrap();
    let request_beta = terminations
        .insert_idempotent(&NewTerminationRequest::new(
            incident_beta.incident_id,
            "project-a",
            "signature-beta",
            "diagnostic relation",
            200,
            None,
        ))
        .unwrap();
    let request_beta_second = terminations
        .insert_idempotent(&NewTerminationRequest::new(
            incident_beta_second.incident_id,
            "project-a",
            "signature-beta",
            "diagnostic relation",
            200,
            None,
        ))
        .unwrap();
    let request_foreign = terminations
        .insert_idempotent(&NewTerminationRequest::new(
            foreign_incident.incident_id,
            "project-b",
            "signature-beta",
            "diagnostic relation",
            300,
            None,
        ))
        .unwrap();
    assert_eq!(
        terminations
            .find_by_task_signature("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|request| request.request_id)
            .collect::<Vec<_>>(),
        vec![request_beta_second.request_id, request_beta.request_id,]
    );
    assert_eq!(
        terminations
            .find_by_task_signature("project-a", "signature-beta", 1)
            .unwrap()
            .iter()
            .map(|request| request.request_id)
            .collect::<Vec<_>>(),
        vec![request_beta_second.request_id]
    );
    assert!(terminations
        .find_by_task_signature("project-a", "signature-beta", 0)
        .unwrap()
        .is_empty());
    assert!(request_old.request_id < request_beta.request_id);
    assert!(request_beta.request_id < request_beta_second.request_id);
    assert!(request_beta_second.request_id < request_foreign.request_id);
    assert!(terminations
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|request| request.project_id == "project-a"));

    let event_a = insert_event(&test.db, "project-a", "agent-run-a", 100);
    let event_b = insert_event(&test.db, "project-b", "agent-run-b", 100);
    let agent_runs = AgentRunRepository::new(&test.db);
    let first_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_a,
            None,
            AgentRunStatus::Starting,
            200,
            "/tmp/agent-a-first.log",
        ))
        .unwrap();
    agent_runs
        .finish(
            first_run.run_id,
            AgentRunStatus::Completed,
            201,
            Some(0),
            None,
        )
        .unwrap();
    agent_runs.attach_event(first_run.run_id, event_a).unwrap();
    let second_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_a,
            None,
            AgentRunStatus::Starting,
            200,
            "/tmp/agent-a-second.log",
        ))
        .unwrap();
    agent_runs
        .finish(
            second_run.run_id,
            AgentRunStatus::Completed,
            201,
            Some(0),
            None,
        )
        .unwrap();
    agent_runs.attach_event(second_run.run_id, event_a).unwrap();
    let foreign_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-b",
            event_b,
            None,
            AgentRunStatus::Starting,
            300,
            "/tmp/agent-b.log",
        ))
        .unwrap();
    assert_eq!(
        agent_runs
            .list_by_project("project-a", 1)
            .unwrap()
            .iter()
            .map(|run| run.run_id)
            .collect::<Vec<_>>(),
        vec![second_run.run_id]
    );
    assert!(!agent_runs
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .any(|run| run.run_id == foreign_run.run_id));
    assert_eq!(
        agent_runs
            .find_by_event("project-a", event_a, 10)
            .unwrap()
            .iter()
            .map(|run| run.run_id)
            .collect::<Vec<_>>(),
        vec![second_run.run_id, first_run.run_id]
    );
    assert_eq!(
        agent_runs
            .find_by_event("project-a", event_a, 1)
            .unwrap()
            .iter()
            .map(|run| run.run_id)
            .collect::<Vec<_>>(),
        vec![second_run.run_id]
    );
    assert!(agent_runs
        .find_by_event("project-a", event_a, 0)
        .unwrap()
        .is_empty());
    assert!(agent_runs
        .find_by_event("project-b", event_a, 10)
        .unwrap()
        .is_empty());
}

#[test]
fn submission_repository_tracks_acceptance_and_unreconciled_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = SubmissionRepository::new(&test.db);

    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-pending",
            "project-a",
            vec!["python".to_owned(), "pending.py".to_owned()],
            100,
        ))
        .unwrap();
    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-failed",
            "project-a",
            vec!["python".to_owned(), "failed.py".to_owned()],
            101,
        ))
        .unwrap();
    repository
        .transition_status("submission-failed", SubmissionStatus::Failed)
        .unwrap();
    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-accepted",
            "project-a",
            vec!["python".to_owned(), "accepted.py".to_owned()],
            102,
        ))
        .unwrap();
    let accepted = repository
        .mark_accepted("submission-accepted", 41, "project-a:41")
        .unwrap();
    assert_eq!(accepted.status, SubmissionStatus::Accepted);
    assert_eq!(accepted.pueue_task_id, Some(41));
    assert_eq!(accepted.task_signature.as_deref(), Some("project-a:41"));

    let initially_unreconciled = repository.find_unreconciled("project-a").unwrap();
    assert_eq!(
        initially_unreconciled
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-pending"]
    );

    repository
        .transition_status("submission-accepted", SubmissionStatus::Unreconciled)
        .unwrap();
    let unreconciled = repository.find_unreconciled("project-a").unwrap();
    assert_eq!(
        unreconciled
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-pending", "submission-accepted"]
    );

    repository
        .transition_status("submission-pending", SubmissionStatus::Adopted)
        .unwrap();
    assert_eq!(repository.find_unreconciled("project-a").unwrap().len(), 2);
}

#[test]
fn termination_request_repository_transitions_and_filters_pending_requests() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "termination-lifecycle",
            100,
        ))
        .unwrap()
        .incident;
    let repository = TerminationRequestRepository::new(&test.db);

    let request = repository
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "fatal pattern",
            100,
            Some(110),
        ))
        .unwrap();
    let sent = repository
        .transition_status(request.request_id, TerminationRequestStatus::Sent)
        .unwrap();
    assert_eq!(sent.status, TerminationRequestStatus::Sent);
    assert_eq!(repository.find_pending("project-a").unwrap().len(), 1);

    let confirmed = repository
        .update_result(
            request.request_id,
            TerminationRequestStatus::Confirmed,
            Some(120),
            None,
        )
        .unwrap();
    assert_eq!(confirmed.status, TerminationRequestStatus::Confirmed);
    assert_eq!(confirmed.confirmed_at, Some(120));
    assert!(repository.find_pending("project-a").unwrap().is_empty());
}

#[test]
fn expired_dispatch_claim_cannot_complete_a_reclaimed_request() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "dispatch-lease",
            100,
        ))
        .unwrap()
        .incident;
    let repository = TerminationRequestRepository::new(&test.db);
    let request = repository
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "fatal pattern",
            100,
            None,
        ))
        .unwrap();
    let first_claim = repository
        .claim_for_dispatch(request.request_id, 100, 200)
        .unwrap()
        .unwrap();
    let second_claim = repository
        .claim_for_dispatch(request.request_id, 201, 301)
        .unwrap()
        .unwrap();

    assert!(repository
        .mark_dispatched_if_current(
            request.request_id,
            first_claim.dispatch_lease_until.unwrap(),
            320
        )
        .unwrap()
        .is_none());
    assert!(repository
        .finish_dispatch_if_current(
            request.request_id,
            first_claim.dispatch_lease_until.unwrap(),
            TerminationRequestStatus::Failed,
            Some("stale failure"),
        )
        .unwrap()
        .is_none());
    let sent = repository
        .mark_dispatched_if_current(
            request.request_id,
            second_claim.dispatch_lease_until.unwrap(),
            321,
        )
        .unwrap()
        .unwrap();
    assert_eq!(sent.status, TerminationRequestStatus::Sent);
    assert_eq!(sent.grace_until, Some(321));
}

#[test]
fn all_event_kind_and_status_values_round_trip_through_sqlite() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    for kind in [
        EventKind::TaskFinished,
        EventKind::TaskFailed,
        EventKind::Crash,
        EventKind::Stalled,
        EventKind::DeepCheck,
        EventKind::AutoKilled,
        EventKind::TerminationFailed,
        EventKind::OperatorWake,
        EventKind::CampaignDecision,
    ] {
        let value: EventKind = connection
            .query_row("SELECT ?1", [kind], |row| row.get(0))
            .unwrap();
        assert_eq!(value, kind);
    }

    for status in [
        EventStatus::Pending,
        EventStatus::Claimed,
        EventStatus::Completed,
        EventStatus::RetryWait,
        EventStatus::Failed,
        EventStatus::InFlight,
        EventStatus::Dispatched,
        EventStatus::DeadLetter,
    ] {
        let value: EventStatus = connection
            .query_row("SELECT ?1", [status], |row| row.get(0))
            .unwrap();
        assert_eq!(value, status);
    }
}

fn sample_batch_request(project_id: &str, request_id: &str) -> NewBatchRequest {
    NewBatchRequest::new(
        request_id,
        project_id,
        "sha256:batch-manifest-v1",
        vec![
            NewBatchJob::new(
                "job-a",
                0,
                SubmissionKind::Experiment,
                vec!["python".to_owned(), "train-a.py".to_owned()],
                json!({"name": "a"}),
            ),
            NewBatchJob::new(
                "job-b",
                1,
                SubmissionKind::Experiment,
                vec!["python".to_owned(), "train-b.py".to_owned()],
                json!({"name": "b"}),
            ),
            NewBatchJob::new(
                "job-c",
                2,
                SubmissionKind::Control,
                vec!["true".to_owned()],
                json!({"name": "c"}),
            ),
        ],
        100,
    )
}

#[test]
fn batch_v9_migration_preserves_projects_and_installs_bounded_tables() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-v9-project");
    register_project(&test.db, "batch-v9-project", &root, "pa-batch-v9-project");

    let connection = test.db.connect().unwrap();
    connection
        .execute_batch(
            "DROP TABLE batch_jobs;
             DROP TABLE batch_requests;
             PRAGMA user_version = 8;",
        )
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    assert_eq!(
        connection
            .query_row(
                "SELECT project_id FROM projects WHERE project_id = 'batch-v9-project'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "batch-v9-project"
    );
    for table in ["batch_requests", "batch_jobs"] {
        assert_eq!(
            connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1
                     )",
                    [table],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            1
        );
    }
    assert!(connection
        .execute(
            "INSERT INTO batch_requests (
                 request_id, project_id, manifest_hash, status,
                 lease_until, created_at, updated_at, last_error
             ) VALUES ('too-long', 'batch-v9-project', ?1, 'pending', NULL, 1, 1, NULL)",
            ["x".repeat(129)],
        )
        .is_err());
    assert!(connection
        .execute(
            "INSERT INTO batch_requests (
                 request_id, project_id, manifest_hash, status,
                 lease_until, created_at, updated_at, last_error
             ) VALUES ('wrong-project', 'missing', 'sha256:ok', 'pending', NULL, 1, 1, NULL)",
            [],
        )
        .is_err());
}

#[test]
fn batch_v10_migration_adds_lease_token_to_a_v9_database() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-v10-project");
    register_project(&test.db, "batch-v10-project", &root, "pa-batch-v10-project");

    let connection = test.db.connect().unwrap();
    let has_lease_token: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pragma_table_info('batch_requests')
                 WHERE name = 'lease_token'
             )",
            [],
            |row| row.get(0),
        )
        .unwrap();
    if has_lease_token {
        connection
            .execute("ALTER TABLE batch_requests DROP COLUMN lease_token", [])
            .unwrap();
    }
    connection
        .execute_batch("PRAGMA user_version = 9;")
        .unwrap();
    drop(connection);

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, LATEST_SCHEMA_VERSION);
    let columns = connection
        .prepare("PRAGMA table_info(batch_requests)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(columns.iter().any(|column| column == "lease_token"));
    assert_eq!(
        connection
            .query_row(
                "SELECT project_id FROM projects WHERE project_id = 'batch-v10-project'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "batch-v10-project"
    );
}

#[test]
fn batch_create_or_get_is_idempotent_and_project_scoped() {
    let test = TestDatabase::new();
    let root_a = test.project_root("batch-project-a");
    let root_b = test.project_root("batch-project-b");
    register_project(&test.db, "batch-project-a", &root_a, "pa-batch-a");
    register_project(&test.db, "batch-project-b", &root_b, "pa-batch-b");
    let repository = BatchRepository::new(&test.db);
    let request = sample_batch_request("batch-project-a", "request-1");

    let first = repository.create_or_get(&request).unwrap();
    let second = repository.create_or_get(&request).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.jobs.len(), 3);
    assert_eq!(
        test.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM batch_jobs WHERE request_id = 'request-1'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        3
    );

    let mut wrong_project = request.clone();
    wrong_project.project_id = "batch-project-b".to_owned();
    assert!(repository.create_or_get(&wrong_project).is_err());
    let mut wrong_manifest = request;
    wrong_manifest.manifest_hash = "sha256:different".to_owned();
    assert!(repository.create_or_get(&wrong_manifest).is_err());
}

#[test]
fn batch_rejects_duplicate_job_ids_before_writing_any_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-duplicate-job");
    register_project(
        &test.db,
        "batch-duplicate-job",
        &root,
        "pa-batch-duplicate-job",
    );
    let repository = BatchRepository::new(&test.db);
    let mut request = sample_batch_request("batch-duplicate-job", "request-duplicate");
    request.jobs[1].job_id = request.jobs[0].job_id.clone();

    assert!(repository.create_or_get(&request).is_err());
    let connection = test.db.connect().unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM batch_requests WHERE request_id = 'request-duplicate'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn batch_claim_installs_a_dispatch_lease_and_is_single_owner() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-claim");
    register_project(&test.db, "batch-claim", &root, "pa-batch-claim");
    let repository = BatchRepository::new(&test.db);
    repository
        .create_or_get(&sample_batch_request("batch-claim", "request-claim"))
        .unwrap();

    let claimed = repository
        .claim("batch-claim", "request-claim", 100, 110)
        .unwrap()
        .unwrap();
    assert_eq!(claimed.status, BatchStatus::Dispatching);
    assert_eq!(claimed.lease_until, Some(110));
    assert!(claimed
        .jobs
        .iter()
        .all(|job| job.status == BatchJobStatus::Dispatching));
    assert!(repository
        .claim("batch-claim", "request-claim", 101, 111)
        .unwrap()
        .is_none());
}

#[test]
fn batch_partial_failure_preserves_accepted_jobs_and_leaves_later_jobs_unsubmitted() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-partial");
    register_project(&test.db, "batch-partial", &root, "pa-batch-partial");
    let repository = BatchRepository::new(&test.db);
    repository
        .create_or_get(&sample_batch_request("batch-partial", "request-partial"))
        .unwrap();
    let claimed = repository
        .claim("batch-partial", "request-partial", 100, 110)
        .unwrap()
        .unwrap();
    let lease_token = claimed.lease_token.as_deref().unwrap();

    repository
        .record_job_result(
            "batch-partial",
            "request-partial",
            "job-a",
            lease_token,
            BatchJobResult::Accepted {
                pueue_task_id: 41,
                submission_id: "submission-a".to_owned(),
            },
            101,
        )
        .unwrap();
    let partial = repository
        .record_job_result(
            "batch-partial",
            "request-partial",
            "job-b",
            lease_token,
            BatchJobResult::Failed {
                error: "ambiguous add response".to_owned(),
            },
            102,
        )
        .unwrap();

    assert_eq!(partial.status, BatchStatus::Partial);
    assert_eq!(partial.lease_until, None);
    assert_eq!(
        partial.last_error.as_deref(),
        Some("ambiguous add response")
    );
    assert_eq!(partial.jobs[0].status, BatchJobStatus::Accepted);
    assert_eq!(partial.jobs[0].pueue_task_id, Some(41));
    assert_eq!(
        partial.jobs[0].submission_id.as_deref(),
        Some("submission-a")
    );
    assert_eq!(partial.jobs[1].status, BatchJobStatus::Failed);
    assert_eq!(partial.jobs[2].status, BatchJobStatus::Pending);

    let replayed = repository
        .record_job_result(
            "batch-partial",
            "request-partial",
            "job-b",
            lease_token,
            BatchJobResult::Failed {
                error: "ambiguous add response".to_owned(),
            },
            103,
        )
        .unwrap();
    assert_eq!(replayed, partial);

    let conflicting_replay = repository.record_job_result(
        "batch-partial",
        "request-partial",
        "job-b",
        lease_token,
        BatchJobResult::Failed {
            error: "different failure".to_owned(),
        },
        104,
    );
    assert!(conflicting_replay.is_err());
    assert_eq!(
        repository.find("batch-partial", "request-partial").unwrap(),
        Some(partial)
    );
}

#[test]
fn batch_recovery_never_retries_an_accepted_job() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-recovery");
    register_project(&test.db, "batch-recovery", &root, "pa-batch-recovery");
    let repository = BatchRepository::new(&test.db);
    repository
        .create_or_get(&sample_batch_request("batch-recovery", "request-recovery"))
        .unwrap();
    let claimed = repository
        .claim("batch-recovery", "request-recovery", 100, 110)
        .unwrap()
        .unwrap();
    let lease_token = claimed.lease_token.as_deref().unwrap();
    repository
        .record_job_result(
            "batch-recovery",
            "request-recovery",
            "job-a",
            lease_token,
            BatchJobResult::Accepted {
                pueue_task_id: 73,
                submission_id: "submission-a".to_owned(),
            },
            101,
        )
        .unwrap();

    let recovered = repository.recover_expired("batch-recovery", 111).unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status, BatchStatus::Accepted);
    assert_eq!(recovered[0].jobs[0].status, BatchJobStatus::Accepted);
    assert_eq!(recovered[0].jobs[0].pueue_task_id, Some(73));
    assert_eq!(recovered[0].jobs[1].status, BatchJobStatus::Pending);
    assert_eq!(recovered[0].jobs[2].status, BatchJobStatus::Pending);

    let reclaimed = repository
        .claim("batch-recovery", "request-recovery", 112, 122)
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.jobs[0].status, BatchJobStatus::Accepted);
    assert_eq!(reclaimed.jobs[1].status, BatchJobStatus::Dispatching);
    assert_eq!(reclaimed.jobs[2].status, BatchJobStatus::Dispatching);
}

#[test]
fn batch_find_and_result_updates_cannot_cross_project_boundaries() {
    let test = TestDatabase::new();
    let root_a = test.project_root("batch-scope-a");
    let root_b = test.project_root("batch-scope-b");
    register_project(&test.db, "batch-scope-a", &root_a, "pa-batch-scope-a");
    register_project(&test.db, "batch-scope-b", &root_b, "pa-batch-scope-b");
    let repository = BatchRepository::new(&test.db);
    repository
        .create_or_get(&sample_batch_request("batch-scope-a", "request-scope"))
        .unwrap();

    assert!(repository
        .find("batch-scope-b", "request-scope")
        .unwrap()
        .is_none());
    assert!(repository
        .claim("batch-scope-b", "request-scope", 100, 110)
        .unwrap()
        .is_none());
    assert!(repository
        .record_job_result(
            "batch-scope-b",
            "request-scope",
            "job-a",
            "wrong-project-token",
            BatchJobResult::Accepted {
                pueue_task_id: 1,
                submission_id: "wrong-project".to_owned(),
            },
            101,
        )
        .is_err());
}

#[test]
fn batch_completed_result_replay_is_idempotent_after_lease_clear() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-completed");
    register_project(&test.db, "batch-completed", &root, "pa-batch-completed");
    let repository = BatchRepository::new(&test.db);
    let mut request = sample_batch_request("batch-completed", "request-completed");
    request.jobs.truncate(1);
    repository.create_or_get(&request).unwrap();
    let claimed = repository
        .claim("batch-completed", "request-completed", 100, 110)
        .unwrap()
        .unwrap();
    let lease_token = claimed.lease_token.clone().unwrap();

    let result = BatchJobResult::Accepted {
        pueue_task_id: 99,
        submission_id: "submission-completed".to_owned(),
    };
    let completed = repository
        .record_job_result(
            "batch-completed",
            "request-completed",
            "job-a",
            &lease_token,
            result.clone(),
            101,
        )
        .unwrap();
    assert_eq!(completed.status, BatchStatus::Completed);

    let replayed = repository
        .record_job_result(
            "batch-completed",
            "request-completed",
            "job-a",
            &lease_token,
            result,
            102,
        )
        .unwrap();
    assert_eq!(replayed, completed);

    let conflicting_replay = repository.record_job_result(
        "batch-completed",
        "request-completed",
        "job-a",
        &lease_token,
        BatchJobResult::Accepted {
            pueue_task_id: 100,
            submission_id: "different-submission".to_owned(),
        },
        103,
    );
    assert!(conflicting_replay.is_err());
    let persisted = repository
        .find("batch-completed", "request-completed")
        .unwrap()
        .unwrap();
    assert_eq!(persisted, completed);
    assert_eq!(persisted.lease_token, None);
}

#[test]
fn batch_stale_worker_token_is_rejected_after_lease_recovery() {
    let test = TestDatabase::new();
    let root = test.project_root("batch-stale-worker");
    register_project(
        &test.db,
        "batch-stale-worker",
        &root,
        "pa-batch-stale-worker",
    );
    let repository = BatchRepository::new(&test.db);
    let mut request = sample_batch_request("batch-stale-worker", "request-stale-worker");
    request.jobs.truncate(1);
    repository.create_or_get(&request).unwrap();

    let claim_a = repository
        .claim("batch-stale-worker", "request-stale-worker", 100, 110)
        .unwrap()
        .unwrap();
    let token_a = claim_a.lease_token.clone().unwrap();
    repository
        .recover_expired("batch-stale-worker", 111)
        .unwrap();
    let claim_b = repository
        .claim("batch-stale-worker", "request-stale-worker", 112, 122)
        .unwrap()
        .unwrap();
    let token_b = claim_b.lease_token.clone().unwrap();
    assert_ne!(token_a, token_b);

    let stale = repository.record_job_result(
        "batch-stale-worker",
        "request-stale-worker",
        "job-a",
        &token_a,
        BatchJobResult::Accepted {
            pueue_task_id: 41,
            submission_id: "stale-submission".to_owned(),
        },
        113,
    );
    assert!(stale.is_err());
    let still_dispatching = repository
        .find("batch-stale-worker", "request-stale-worker")
        .unwrap()
        .unwrap();
    assert_eq!(
        still_dispatching.jobs[0].status,
        BatchJobStatus::Dispatching
    );
    assert_eq!(still_dispatching.jobs[0].pueue_task_id, None);
    assert_eq!(still_dispatching.jobs[0].submission_id, None);

    let accepted = repository
        .record_job_result(
            "batch-stale-worker",
            "request-stale-worker",
            "job-a",
            &token_b,
            BatchJobResult::Accepted {
                pueue_task_id: 42,
                submission_id: "current-submission".to_owned(),
            },
            114,
        )
        .unwrap();
    assert_eq!(accepted.status, BatchStatus::Completed);
    assert_eq!(accepted.jobs[0].pueue_task_id, Some(42));
    assert_eq!(
        accepted.jobs[0].submission_id.as_deref(),
        Some("current-submission")
    );
}
