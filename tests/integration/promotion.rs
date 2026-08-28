use std::fs;

use pueue_agent::{
    db::{CampaignRepository, Db, MetricsRepository, ProjectRepository, StartCampaignRequest, migrations},
    execution_policy::CampaignLimits,
    models::{ExperimentMetricsRow, ExperimentStatus, MetricDirection, ObjectiveMetric, ProposalKind},
    promotion::{evaluate, PromotionOutcome},
    proposals::{self, ProposalInput},
    state::ObjectiveSnapshot,
};
use rusqlite::{params, OptionalExtension};
use serde_json::json;
use tempfile::TempDir;

const CAMPAIGN_ID: &str = "campaign-promotion";
const PROJECT_ID: &str = "project-a";
const BASELINE_EXPERIMENT_ID: &str = "promo-experiment-baseline";
const BASELINE_SUBMISSION_ID: &str = "promo-submission-baseline";
const BASELINE_PROPOSAL_ID: &str = "promo-proposal-baseline";
const CHALLENGER_EXPERIMENT_ID: &str = "promo-experiment-challenger";

fn minimize_metric() -> ObjectiveMetric {
    ObjectiveMetric {
        name: "loss".to_owned(),
        direction: MetricDirection::Minimize,
        min_delta: Some(0.01),
    }
}

fn maximize_metric() -> ObjectiveMetric {
    ObjectiveMetric {
        name: "score".to_owned(),
        direction: MetricDirection::Maximize,
        min_delta: None,
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&pueue_agent::models::NewProject::new(
                PROJECT_ID,
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self { _temp: temp, db }
    }

    fn start_campaign(&self, objective_metric: Option<&ObjectiveMetric>) {
        let objective = ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "objective-digest".to_owned(),
        };
        let argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--name".to_owned(),
            "experiment".to_owned(),
        ];
        let proposal = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: CAMPAIGN_ID,
                    project_id: PROJECT_ID,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: BASELINE_SUBMISSION_ID,
                    experiment_id: BASELINE_EXPERIMENT_ID,
                    proposal_id: BASELINE_PROPOSAL_ID,
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
    }

    fn seed_metrics(&self, experiment_id: &str, value: Option<f64>) {
        MetricsRepository::upsert(
            &self.db,
            &ExperimentMetricsRow {
                experiment_id: experiment_id.to_owned(),
                source: "manifest".to_owned(),
                primary_metric_name: Some("loss".to_owned()),
                primary_metric_value: value,
                metrics_json: "{}".to_owned(),
                artifact_defect: None,
                created_at: 150,
                updated_at: 150,
                evaluated_at: None,
            },
        )
        .unwrap();
    }

    fn add_experiment(&self, experiment_id: &str) {
        self.db.connect().unwrap().execute_batch(&format!(
            "INSERT INTO submissions (
                 submission_id, project_id, argv_json, created_at, pueue_task_id,
                 task_signature, status, kind, metadata_json, origin_agent_run_id
             )
             SELECT '{experiment_id}-submission', project_id, argv_json, created_at + 1,
                    pueue_task_id, task_signature, status, kind, metadata_json,
                    origin_agent_run_id
             FROM submissions WHERE submission_id = '{BASELINE_SUBMISSION_ID}';
             INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis, source_experiment_id,
                 argv_json, working_directory, expected_evidence_json, canonical_digest,
                 reject_reason, created_at, updated_at
             )
             SELECT '{experiment_id}-proposal', campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, '{experiment_id}-proposal-digest',
                    reject_reason, created_at + 1, updated_at + 1
             FROM proposals WHERE proposal_id = '{BASELINE_PROPOSAL_ID}';
             INSERT INTO experiments (
                 experiment_id, campaign_id, proposal_id, submission_id,
                 parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                 failure_code, failure_fingerprint, created_at, updated_at, finished_at
             )
             SELECT '{experiment_id}', campaign_id, '{experiment_id}-proposal',
                    '{experiment_id}-submission', parent_experiment_id, attempt + 1, status,
                    pueue_task_id, task_signature, failure_code, failure_fingerprint,
                    created_at + 1, updated_at + 1, finished_at
             FROM experiments WHERE experiment_id = '{BASELINE_EXPERIMENT_ID}';",
        ))
        .unwrap();
    }

    fn set_promotion_state(&self, current_best: Option<&str>, plateau_count: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns
                 SET current_best_experiment_id = ?1, plateau_count = ?2
                 WHERE campaign_id = ?3",
                params![current_best, plateau_count, CAMPAIGN_ID],
            )
            .unwrap();
    }

    fn set_campaign_state(&self, state: &str) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns SET state = ?1 WHERE campaign_id = ?2",
                params![state, CAMPAIGN_ID],
            )
            .unwrap();
    }

    fn promotion_row(&self) -> (Option<String>, i64) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT current_best_experiment_id, plateau_count
                 FROM campaigns WHERE campaign_id = ?1",
                [CAMPAIGN_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn strategy_refresh_wakes(&self) -> Vec<(String, String)> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare(
                "SELECT dedup_key, status FROM events
                 WHERE project_id = ?1 AND kind = 'operator_wake'
                   AND dedup_key LIKE 'strategy-refresh:v1:%'
                 ORDER BY created_at, event_id",
            )
            .unwrap();
        statement
            .query_map([PROJECT_ID], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn wake_payload(&self, dedup_key: &str) -> (Option<String>, serde_json::Value) {
        let (campaign_id, payload): (Option<String>, String) = self
            .db
            .connect()
            .unwrap()
            .query_row(                "SELECT campaign_id, payload_json FROM events
                 WHERE project_id = ?1 AND dedup_key = ?2",
                params![PROJECT_ID, dedup_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        (campaign_id, serde_json::from_str(&payload).unwrap())
    }
}

fn run_non_improvement(
    harness: &Harness,
    index: usize,
    limits: &CampaignLimits,
) -> PromotionOutcome {
    let experiment_id = format!("{CHALLENGER_EXPERIMENT_ID}-{index}");
    harness.add_experiment(&experiment_id);
    harness.seed_metrics(&experiment_id, Some(2.0));
    evaluate(
        &harness.db,
        CAMPAIGN_ID,
        &experiment_id,
        ExperimentStatus::Succeeded,
        limits,
        300 + index as i64,
    )
    .unwrap()
}

#[test]
fn improvement_updates_current_best_and_resets_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 3);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::Improved);
    assert_eq!(
        harness.promotion_row(),
        (Some(CHALLENGER_EXPERIMENT_ID.to_owned()), 0)
    );
}

#[test]
fn non_improvement_increments_plateau_without_moving_the_best() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(2.0));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(harness.promotion_row(), (None, 1));
}

#[test]
fn exactly_at_delta_is_not_an_improvement() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.99));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 1)
    );
}

#[test]
fn metric_less_campaign_skips_evaluation() {
    let harness = Harness::new();
    harness.start_campaign(None);
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);
    assert_eq!(harness.promotion_row(), (None, 0));
}

#[test]
fn inactive_campaign_skips_evaluation() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_campaign_state("goal_reached_pending_review");

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);
    assert_eq!(harness.promotion_row(), (None, 0));
    assert!(harness.strategy_refresh_wakes().is_empty());
}

#[test]
fn baseline_first_establishes_and_anchors_the_comparison() {
    let harness = Harness::new();
    harness.start_campaign(Some(&maximize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(10.0));

    let baseline_outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        BASELINE_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        200,
    ).unwrap();

    assert_eq!(baseline_outcome, PromotionOutcome::BaselineEstablished);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 0)
    );

    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(12.0));

    let challenger_outcome =
        evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(challenger_outcome, PromotionOutcome::Improved);
    assert_eq!(
        harness.promotion_row(),
        (Some(CHALLENGER_EXPERIMENT_ID.to_owned()), 0)
    );
}

#[test]
fn succeeded_without_primary_metric_counts_toward_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, None);
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 1)
    );
}

#[test]
fn failed_and_cancelled_experiments_skip_evaluation() {
    for status in [ExperimentStatus::Failed, ExperimentStatus::Cancelled] {
        let harness = Harness::new();
        harness.start_campaign(Some(&minimize_metric()));
        harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
        harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
        harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
        harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 2);

        let outcome = evaluate(
            &harness.db,
            CAMPAIGN_ID,
            CHALLENGER_EXPERIMENT_ID,
            status,
            &CampaignLimits::default(),
            300,
        )
        .unwrap();

        assert_eq!(outcome, PromotionOutcome::SkippedNoMetric);
        assert_eq!(
            harness.promotion_row(),
            (Some(BASELINE_EXPERIMENT_ID.to_owned()), 2)
        );
    }
}

#[test]
fn three_consecutive_non_improvements_emit_exactly_one_strategy_refresh_wake() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..3 {
        let outcome = run_non_improvement(&harness, index, &limits);
        assert_eq!(outcome, PromotionOutcome::NotImproved);
    }

    let wake_key = format!("strategy-refresh:v1:{CAMPAIGN_ID}:1");
    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![(wake_key.clone(), "pending".to_owned())]
    );
    let (campaign_lineage, payload) = harness.wake_payload(&wake_key);
    assert_eq!(campaign_lineage.as_deref(), Some(CAMPAIGN_ID));
    assert_eq!(payload["source"], "promotion");
    assert_eq!(payload["reason"], "plateau_threshold_reached");
    assert_eq!(payload["plateau_count"], 3);
    assert_eq!(payload["round"], 1);
    assert_eq!(
        harness.promotion_row(),
        (None, 3),
        "the campaign stays active and keeps counting"
    );
}

#[test]
fn fourth_non_improvement_emits_no_additional_wake() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..4 {
        let outcome = run_non_improvement(&harness, index, &limits);
        assert_eq!(outcome, PromotionOutcome::NotImproved);
    }

    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![(
            format!("strategy-refresh:v1:{CAMPAIGN_ID}:1"),
            "pending".to_owned()
        )]
    );
    assert_eq!(harness.promotion_row(), (None, 4));
}

#[test]
fn improvement_resets_the_plateau_and_the_next_round_wakes_with_round_two() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..3 {
        assert_eq!(
            run_non_improvement(&harness, index, &limits),
            PromotionOutcome::NotImproved
        );
    }
    let improver = format!("{CHALLENGER_EXPERIMENT_ID}-improver");
    harness.add_experiment(&improver);
    harness.seed_metrics(&improver, Some(0.5));
    assert_eq!(
        evaluate(
            &harness.db,
            CAMPAIGN_ID,
            &improver,
            ExperimentStatus::Succeeded,
            &limits,
            400,
        )
        .unwrap(),
        PromotionOutcome::Improved
    );
    for index in 10..13 {
        assert_eq!(
            run_non_improvement(&harness, index, &limits),
            PromotionOutcome::NotImproved
        );
    }

    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![
            (
                format!("strategy-refresh:v1:{CAMPAIGN_ID}:1"),
                "pending".to_owned()
            ),
            (
                format!("strategy-refresh:v1:{CAMPAIGN_ID}:2"),
                "pending".to_owned()
            ),
        ]
    );
    let (_, payload) = harness.wake_payload(&format!("strategy-refresh:v1:{CAMPAIGN_ID}:2"));
    assert_eq!(payload["round"], 2);
    assert_eq!(harness.promotion_row(), (Some(improver), 3));
}

/// Crash-window mutation freeze: first terminal evidence is frozen on ingestion.
/// A crash after terminal projection but before evaluation cannot let a mutated
/// manifest overwrite the frozen evidence. Genuine I/O failures leave the
/// experiment accepted and retryable.
#[test]
fn crash_window_mutation_freeze() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    // First evaluation - evidence is frozen
    let outcome1 = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap();
    assert_eq!(outcome1, PromotionOutcome::Improved);

    // Simulate a mutated manifest trying to overwrite - attempt to insert new metrics
    // with a different value. The frozen evidence should prevent this.
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(2.0)); // Different value

    // Re-evaluation should be idempotent and return the same outcome
    let outcome2 = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        400,
    )
    .unwrap();
    assert_eq!(outcome2, PromotionOutcome::Improved);

    // Plateau should not have changed (no double-counting)
    assert_eq!(harness.promotion_row(), (Some(CHALLENGER_EXPERIMENT_ID.to_owned()), 0));
}

/// Passive audit idempotency: promotion audit marker is emitted as 'completed'
/// in the same transaction, never pending or scheduler-dispatchable.
/// Retries are idempotent via dedup_key.
#[test]
fn passive_audit_idempotent_no_dispatch() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    // First evaluation - should emit audit marker
    evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap();

    // Check that exactly one promotion audit event was created with status 'completed'
    let connection = harness.db.connect().unwrap();
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'
               AND status = 'completed'",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "exactly one completed promotion audit event");

    // Retry evaluation - should be idempotent, no additional audit event
    evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        400,
    )
    .unwrap();

    let count_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'
               AND status = 'completed'",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count_after, 1, "retry should not create duplicate audit event");

    // Ensure no 'pending' promotion audit events exist
    let pending_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'
               AND status = 'pending'",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending_count, 0, "promotion audit should never be pending");
}

/// Missing metrics row: evaluation should settle the evaluated marker but
/// change no plateau or state.
#[test]
fn missing_metrics_row_settles_marker_no_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    // Do NOT add metrics for challenger - simulates missing row
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    // No seed_metrics call for challenger

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoMetric);

    // Plateau should NOT have changed (still 0)
    assert_eq!(harness.promotion_row(), (None, 0));

    // But evaluated_at should be set (marker settled)
    let evaluated: Option<Option<String>> = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert!(evaluated.flatten().is_some(), "evaluated_at marker should be settled");
}

/// Inactive campaign or no objective: evaluation should still settle the
/// evaluated marker but change no plateau or state.
#[test]
fn inactive_campaign_settles_marker_no_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    // Deactivate campaign
    harness.set_campaign_state("paused");

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);

    // Plateau should NOT have changed (still 0)
    assert_eq!(harness.promotion_row(), (None, 0));

    // But evaluated_at should be set (marker settled)
    let evaluated: Option<Option<String>> = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert!(evaluated.flatten().is_some(), "evaluated_at marker should be settled");
}

/// v24-to-v25 backfill: terminal experiments get evaluated_at set to their
/// updated_at timestamp, non-terminal experiments remain NULL.
#[test]
fn v24_to_v25_backfill_terminal_experiments_evaluated_nonterminal_null() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();

    // Create a v24 database (schema version 24)
    let connection = db.connect().unwrap();
    connection.execute_batch("PRAGMA user_version = 24;").unwrap();

    // Create minimal v24 schema for campaigns and experiment_metrics
    connection.execute_batch(
        "CREATE TABLE campaigns (
            campaign_id TEXT PRIMARY KEY,
            project_id TEXT NOT NULL,
            objective_text TEXT NOT NULL,
            objective_digest TEXT NOT NULL,
            initial_argv_json TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN (
                'active','budget_waiting','goal_reached_pending_review','paused',
                'degraded','halted','retired'
            )),
            state_reason TEXT,
            baseline_experiment_id TEXT,
            next_eligible_at INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            objective_metric_json TEXT,
            current_best_experiment_id TEXT,
            plateau_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE experiments (
            experiment_id TEXT PRIMARY KEY,
            campaign_id TEXT NOT NULL,
            proposal_id TEXT NOT NULL,
            submission_id TEXT NOT NULL,
            parent_experiment_id TEXT,
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
            finished_at INTEGER
        );
        CREATE TABLE experiment_metrics (
            experiment_id TEXT PRIMARY KEY REFERENCES experiments(experiment_id) ON DELETE CASCADE,
            source TEXT NOT NULL CHECK (source IN ('manifest')),
            primary_metric_name TEXT,
            primary_metric_value REAL,
            metrics_json TEXT NOT NULL DEFAULT '{}',
            artifact_defect TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        );",
    ).unwrap();

    // Insert terminal and non-terminal experiments with metrics
    connection.execute(
        "INSERT INTO campaigns (campaign_id, project_id, objective_text, objective_digest,
            initial_argv_json, state, created_at, updated_at)
         VALUES ('campaign-test', 'project-a', 'obj', 'digest', '[]', 'active', 100, 100)",
        [],
    ).unwrap();

    // Terminal experiment (succeeded) - should get evaluated_at backfilled
    connection.execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id,
            attempt, status, created_at, updated_at, finished_at)
         VALUES ('exp-terminal', 'campaign-test', 'prop-1', 'sub-1', 0, 'succeeded', 150, 150, 200)",
        [],
    ).unwrap();
    connection.execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name,
            primary_metric_value, metrics_json, artifact_defect, created_at, updated_at)
         VALUES ('exp-terminal', 'manifest', 'loss', 0.5, '{}', NULL, 150, 150)",
        [],
    ).unwrap();

    // Non-terminal experiment (accepted) - should remain NULL
    connection.execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id,
            attempt, status, created_at, updated_at, finished_at)
         VALUES ('exp-nonterminal', 'campaign-test', 'prop-2', 'sub-2', 0, 'accepted', 150, 150, NULL)",
        [],
    ).unwrap();
    connection.execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name,
            primary_metric_value, metrics_json, artifact_defect, created_at, updated_at)
         VALUES ('exp-nonterminal', 'manifest', 'loss', 0.3, '{}', NULL, 150, 150)",
        [],
    ).unwrap();

    // Run migration to v25
    {
        let mut conn = db.connect().unwrap();
        migrations::migrate(&mut conn).unwrap();
    }

    // Verify v25 schema version
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 25);

    // Terminal experiment should have evaluated_at = updated_at (as TEXT)
    let terminal_evaluated: Option<Option<String>> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'exp-terminal'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(terminal_evaluated.flatten().as_deref(), Some("150"));

    // Non-terminal experiment should have evaluated_at = NULL
    let nonterminal_evaluated: Option<Option<String>> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'exp-nonterminal'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert_eq!(nonterminal_evaluated.flatten(), None);
}

#[test]
fn promotion_audit_is_passive_completed_and_idempotent() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::Improved);

    let connection = harness.db.connect().unwrap();
    let event: (String, String) = connection
        .query_row(
            "SELECT dedup_key, status FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'
             ORDER BY created_at, event_id",
            [PROJECT_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(event.1, "completed", "promotion audit must be completed, not pending");
    assert_eq!(event.0, format!("promotion:v1:{CAMPAIGN_ID}:{CHALLENGER_EXPERIMENT_ID}"));

    let _ = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        310,
    ).unwrap();

    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key = ?2",
            params![PROJECT_ID, format!("promotion:v1:{CAMPAIGN_ID}:{CHALLENGER_EXPERIMENT_ID}")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "promotion audit must be idempotent on retry");
}

#[test]
fn promotion_audit_not_emitted_for_baseline() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        BASELINE_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        200,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::BaselineEstablished);

    let connection = harness.db.connect().unwrap();
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "baseline promotion must not emit audit marker");
}

#[test]
fn evaluation_fail_closed_missing_metrics_row() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    // Do NOT seed metrics for challenger - simulating missing row

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::SkippedNoMetric);

    let connection = harness.db.connect().unwrap();
    let (best, plateau): (Option<String>, i64) = connection
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(best, Some(BASELINE_EXPERIMENT_ID.to_owned()), "baseline must remain best");
    assert_eq!(plateau, 0, "plateau must not change when metrics row is missing");

    let evaluated_at: Option<i64> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics
             WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert!(evaluated_at.is_some(), "evaluated_at must be set even for missing row");
}

#[test]
fn evaluation_settles_marker_for_inactive_campaign() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_campaign_state("paused");

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);

    let connection = harness.db.connect().unwrap();
    let evaluated_at: Option<i64> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics
             WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert!(evaluated_at.is_some(), "inactive campaign must still settle evaluated_at marker");

    let (best, plateau): (Option<String>, i64) = connection
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(best, Some(BASELINE_EXPERIMENT_ID.to_owned()));
    assert_eq!(plateau, 0, "plateau must not change for inactive campaign");
}

#[test]
fn evaluation_settles_marker_for_campaign_without_objective() {
    let harness = Harness::new();
    harness.start_campaign(None);
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);

    let connection = harness.db.connect().unwrap();
    let evaluated_at: Option<i64> = connection
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics
             WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert!(evaluated_at.is_some(), "campaign without objective must still settle evaluated_at marker");
}

#[test]
fn v24_to_v25_backfill_terminal_experiments_evaluated() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    let connection = db.connect().unwrap();

    connection.execute_batch(
        "CREATE TABLE campaigns (
             campaign_id TEXT PRIMARY KEY,
             project_id TEXT NOT NULL,
             objective_text TEXT NOT NULL,
             objective_digest TEXT NOT NULL,
             initial_argv_json TEXT NOT NULL,
             state TEXT NOT NULL CHECK (state IN ('active','paused')),
             state_reason TEXT,
             baseline_experiment_id TEXT,
             next_eligible_at INTEGER,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL,
             objective_metric_json TEXT,
             current_best_experiment_id TEXT,
             plateau_count INTEGER NOT NULL DEFAULT 0
         );
         CREATE TABLE experiments (
             experiment_id TEXT PRIMARY KEY,
             campaign_id TEXT NOT NULL,
             proposal_id TEXT NOT NULL,
             submission_id TEXT NOT NULL,
             parent_experiment_id TEXT,
             attempt INTEGER NOT NULL,
             status TEXT NOT NULL CHECK (status IN ('succeeded','failed','cancelled','accepted','reserved','submitting','unreconciled')),
             pueue_task_id INTEGER,
             task_signature TEXT,
             failure_code TEXT,
             failure_fingerprint TEXT,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL,
             finished_at INTEGER
         );
         CREATE TABLE experiment_metrics (
             experiment_id TEXT PRIMARY KEY REFERENCES experiments(experiment_id) ON DELETE CASCADE,
             source TEXT NOT NULL CHECK (source IN ('manifest')),
             primary_metric_name TEXT,
             primary_metric_value REAL,
             metrics_json TEXT NOT NULL DEFAULT '{}',
             artifact_defect TEXT,
             created_at INTEGER NOT NULL,
             updated_at INTEGER NOT NULL
         );"
    ).unwrap();

    connection.execute(
        "INSERT INTO campaigns (campaign_id, project_id, objective_text, objective_digest, initial_argv_json, state, created_at, updated_at) VALUES ('c1', 'p1', 'obj', 'digest', '[]', 'active', 100, 100)",
        [],
    ).unwrap();
    connection.execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id, attempt, status, created_at, updated_at, finished_at) VALUES
         ('term1', 'c1', 'prop1', 'sub1', 0, 'succeeded', 200, 200, 250),
         ('term2', 'c1', 'prop2', 'sub2', 0, 'failed', 300, 300, 350),
         ('nonterm1', 'c1', 'prop3', 'sub3', 0, 'accepted', 400, 400, NULL),
         ('nonterm2', 'c1', 'prop4', 'sub4', 0, 'reserved', 500, 500, NULL)",
        [],
    ).unwrap();
    connection.execute(
        "INSERT INTO experiment_metrics (experiment_id, source, created_at, updated_at) VALUES
         ('term1', 'manifest', 200, 200),
         ('term2', 'manifest', 300, 300),
         ('nonterm1', 'manifest', 400, 400),
         ('nonterm2', 'manifest', 500, 500)",
        [],
    ).unwrap();

    connection.execute_batch("PRAGMA user_version = 24;").unwrap();

    let db2 = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    let conn2 = db2.connect().unwrap();
    let version: i64 = conn2.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
    assert_eq!(version, 25);

    let term1_eval: Option<i64> = conn2
        .query_row("SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'term1'", [], |r| r.get(0))
        .unwrap();
    let term2_eval: Option<i64> = conn2
        .query_row("SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'term2'", [], |r| r.get(0))
        .unwrap();
    let nonterm1_eval: Option<i64> = conn2
        .query_row("SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'nonterm1'", [], |r| r.get(0))
        .unwrap();
    let nonterm2_eval: Option<i64> = conn2
        .query_row("SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = 'nonterm2'", [], |r| r.get(0))
        .unwrap();

    assert!(term1_eval.is_some(), "terminal succeeded must be evaluated");
    assert!(term2_eval.is_some(), "terminal failed must be evaluated");
    assert!(nonterm1_eval.is_none(), "accepted must remain NULL");
    assert!(nonterm2_eval.is_none(), "reserved must remain NULL");
}

#[test]
fn evaluation_idempotent_already_evaluated() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let _ = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        310,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::Improved);

    let connection = harness.db.connect().unwrap();
    let (best, plateau): (Option<String>, i64) = connection
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(best, Some(CHALLENGER_EXPERIMENT_ID.to_owned()));
    assert_eq!(plateau, 0, "re-evaluation must not change plateau");
}
