use std::fs;

use pueue_agent::{
    db::{CampaignRepository, Db, MetricsRepository, ProjectRepository, StartCampaignRequest},
    execution_policy::CampaignLimits,
    models::{ExperimentMetricsRow, ExperimentStatus, MetricDirection, ObjectiveMetric, ProposalKind},
    promotion::{evaluate, PromotionOutcome},
    proposals::{self, ProposalInput},
    state::ObjectiveSnapshot,
};
use rusqlite::params;
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
