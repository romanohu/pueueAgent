use std::fs;

use pueue_agent::{
    campaign,
    db::{
        CampaignRepository, Db, DecisionRepository, EventRepository, ExperimentRepository,
        MetricsRepository, ProjectRepository, StartCampaignRequest,
    },
    decision::DecisionCoordinator,
    decision_protocol::parse_and_validate_decision,
    execution_policy::CampaignLimits,
    models::{
        CampaignState, DecisionCycleState, EventKind, ExperimentMetricsRow,
        ExperimentTerminalOutcome, NewProject,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    state::ObjectiveSnapshot,
};
use rusqlite::params;
use serde_json::json;
use tempfile::TempDir;

struct DummyPueue;

#[async_trait::async_trait]
impl PueueApi for DummyPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, pueue_agent::AppError> {
        Ok(Vec::new())
    }
    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, pueue_agent::AppError> {
        panic!("add not expected for goal path")
    }
    async fn kill(&self, _task_id: i64) -> Result<(), pueue_agent::AppError> {
        Ok(())
    }
    async fn remove(&self, _task_id: i64) -> Result<(), pueue_agent::AppError> {
        Ok(())
    }
    async fn ensure_group(&self, _group: &str) -> Result<(), pueue_agent::AppError> {
        Ok(())
    }
}

const PROJECT_ID: &str = "project-a";
const CAMPAIGN_ID: &str = "campaign-goal";
const BASELINE_EXPT: &str = "goal-baseline";
const BASELINE_SUB: &str = "goal-submission-baseline";
const BASELINE_PROP: &str = "goal-proposal-baseline";

fn new_harness() -> (TempDir, Db, std::path::PathBuf) {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    ProjectRepository::new(&db)
        .register(&NewProject::new(
            PROJECT_ID,
            &root,
            "pa-project",
            root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    (temp, db, root)
}

fn start_campaign(db: &Db) {
    let objective = ObjectiveSnapshot {
        text: "Reach goal".to_owned(),
        digest: "objective-digest".to_owned(),
    };
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: pueue_agent::models::ProposalKind::Experiment,
            hypothesis: "Establish baseline".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    CampaignRepository::new(db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: CAMPAIGN_ID,
                project_id: PROJECT_ID,
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: BASELINE_SUB,
                experiment_id: BASELINE_EXPT,
                proposal_id: BASELINE_PROP,
                metadata: &json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap();
}

fn seed_metrics(db: &Db, experiment_id: &str) {
    MetricsRepository::upsert(
        db,
        &ExperimentMetricsRow {
            experiment_id: experiment_id.to_owned(),
            source: "manifest".to_owned(),
            primary_metric_name: Some("loss".to_owned()),
            primary_metric_value: Some(0.12),
            metrics_json: json!({"loss": 0.12}).to_string(),
            artifact_defect: None,
            created_at: 150,
            updated_at: 150,
        },
    )
    .unwrap();
}

fn terminalize_baseline(db: &Db) {
    let repo = ExperimentRepository::new(db);
    repo.mark_submitting(BASELINE_EXPT, 110).unwrap();
    repo.mark_accepted(BASELINE_EXPT, 41, "task-sig-41", 120)
        .unwrap();
    repo.project_terminal_submission(BASELINE_EXPT, 41, ExperimentTerminalOutcome::Succeeded, 130)
        .unwrap();
}

fn build_decision_context(
    db: &Db,
    reservation: &pueue_agent::db::DecisionReservation,
) -> (String, String) {
    use sha2::{Digest, Sha256};
    let campaign = CampaignRepository::new(db)
        .find_by_id(&reservation.campaign_id)
        .unwrap()
        .unwrap();
    let experiment = ExperimentRepository::new(db)
        .find_by_id(&reservation.source_experiment_id)
        .unwrap()
        .unwrap();
    let context = json!({
        "schema_version": 1,
        "objective": {
            "text": campaign.objective_text,
            "digest": campaign.objective_digest
        },
        "source_experiment": {
            "experiment_id": experiment.experiment_id,
            "proposal_id": experiment.proposal_id,
            "proposal_kind": "experiment",
            "status": experiment.status.as_str(),
            "attempt": experiment.attempt,
            "command_digest": "dummy-command-digest",
            "failure_code": experiment.failure_code,
            "failure_fingerprint": experiment.failure_fingerprint,
            "created_at": experiment.created_at,
            "updated_at": experiment.updated_at,
            "finished_at": experiment.finished_at
        },
        "terminal_observation": {
            "task_id": 41,
            "task_signature": "task-sig-41",
            "state": "done",
            "enqueued_at": 110,
            "started_at": 120,
            "ended_at": 130,
            "exit_code": 0
        },
        "recent_outcomes": {
            "proposals": [],
            "experiments": []
        },
        "budgets": {
            "campaign_state": "active",
            "next_eligible_at": null,
            "rolling_usage": {},
            "experiment_counts": {}
        },
        "intervention": {
            "pending": []
        },
        "artifact_hints": []
    });
    let json = serde_json::to_string(&context).unwrap();
    let digest = format!("{:x}", Sha256::digest(json.as_bytes()));
    (json, digest)
}

#[tokio::test]
async fn e2e_goal_reached_via_coordinator_parks_and_reject_targets_exact_event() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    terminalize_baseline(&db);

    let decisions = DecisionRepository::new(&db);
    let cycle = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, BASELINE_EXPT, 130)
        .unwrap();
    let event = pueue_agent::models::NewEvent::new(
        PROJECT_ID,
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        json!({
            "source": "terminal_experiment",
            "cycle_id": cycle.cycle_id,
            "source_experiment_id": BASELINE_EXPT,
            "terminal_observation": {
                "task_id": 41,
                "task_signature": "task-sig-41",
                "group": "pa-project",
                "state": "Done",
                "enqueued_at": 110,
                "started_at": 120,
                "ended_at": 130,
                "exit_code": 0,
            },
        }),
        130,
        130,
    )
    .with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(BASELINE_EXPT.to_owned()));
    let (_, event) = decisions
        .publish_terminal_cycle_event(CAMPAIGN_ID, BASELINE_EXPT, &event, 130)
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE events SET status='completed', completed_at=135, attempts=1 WHERE event_id=?1",
            params![event.event_id],
        )
        .unwrap();
    let reservation = decisions
        .reserve_next_attempt(PROJECT_ID, &cycle.cycle_id, 140)
        .unwrap()
        .unwrap();
    let (context_json, context_digest) = build_decision_context(&db, &reservation);
    decisions
        .store_evidence(&reservation, &context_json, &context_digest, 140)
        .unwrap();
    let run_id = {
        let conn = db.connect().unwrap();
        conn.execute(
            "INSERT INTO agent_runs (run_id, project_id, primary_event_id, status, started_at, log_path, launch_gate_state) VALUES (9001, ?1, ?2, 'running', 140, '/tmp/goal.log', 'released')",
            params![PROJECT_ID, event.event_id],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id) VALUES (?1, 9001, ?2)",
            params![PROJECT_ID, event.event_id],
        )
        .unwrap();
        9001
    };
    decisions.bind_agent_run(&reservation, run_id, 141).unwrap();
    let goal_json = json!({
        "schema_version": 1,
        "decision": "goal_reached",
        "evidence_ref": BASELINE_EXPT
    })
    .to_string();
    let decision = parse_and_validate_decision(
        goal_json.as_bytes(),
        "objective-digest",
        CampaignLimits::default(),
    )
    .unwrap();
    decisions
        .store_decision(
            run_id,
            &goal_json,
            decision.canonical_digest(),
            "goal_reached",
            142,
        )
        .unwrap();

    let dummy = DummyPueue;
    let coordinator = DecisionCoordinator::new(&db, &dummy, CampaignLimits::default());
    let report = coordinator.apply_ready(150, 10).await.unwrap();
    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.waits_scheduled, 0);

    let campaign = CampaignRepository::new(&db)
        .find_by_id(CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::GoalReachedPendingReview);
    let cycle_after = decisions
        .find_cycle_for_source(CAMPAIGN_ID, BASELINE_EXPT)
        .unwrap()
        .unwrap();
    assert_eq!(cycle_after.state, DecisionCycleState::Completed);
    assert_eq!(
        cycle_after.last_decision_kind.as_deref(),
        Some("goal_reached")
    );

    let project = ProjectRepository::new(&db)
        .find_by_id(PROJECT_ID)
        .unwrap()
        .unwrap();
    let human = campaign::render_status_for_project(&db, &project, false).unwrap();
    assert!(human.contains("goal_reached_pending_review"), "{human}");
    assert!(human.contains(BASELINE_EXPT) || human.contains("goal_reached"));

    let plateau_before: i64 = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT plateau_count FROM campaigns WHERE campaign_id=?1",
            params![CAMPAIGN_ID],
            |row| row.get(0),
        )
        .unwrap();
    let completed_at_before: Option<i64> = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT completed_at FROM events WHERE event_id=?1",
            params![event.event_id],
            |row| row.get(0),
        )
        .unwrap();

    let rejected = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("needs more evidence"), 160)
        .unwrap();
    assert_eq!(rejected.state, CampaignState::Active);
    assert_eq!(
        rejected.state_reason.as_deref(),
        Some("goal_claim_rejected")
    );

    let (status, last_error, completed_at_after): (String, Option<String>, Option<i64>) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, last_error, completed_at FROM events WHERE event_id=?1",
            params![event.event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "dead_letter");
    assert_eq!(last_error.as_deref(), Some("goal_claim_rejected"));
    assert_eq!(completed_at_after, completed_at_before);

    let plateau_after: i64 = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT plateau_count FROM campaigns WHERE campaign_id=?1",
            params![CAMPAIGN_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(plateau_after, plateau_before);

    let (action, details): (String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT action, details_json FROM operator_logs ORDER BY log_id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(action, "resume");
    let details_json: serde_json::Value = serde_json::from_str(&details).unwrap();
    assert_eq!(details_json["review"], "reject");
    assert_eq!(details_json["reason"], "goal_claim_rejected");
    assert_eq!(details_json["note"], "needs more evidence");

    let log_count_before: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    let second = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("second"), 170)
        .unwrap();
    assert_eq!(second.state, CampaignState::Active);
    let log_count_after: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(log_count_after, log_count_before);
}

#[tokio::test]
async fn cross_campaign_evidence_is_rejected_and_does_not_park() {
    let temp = TempDir::new().unwrap();
    let root_a = temp.path().join("project-a");
    let root_b = temp.path().join("project-b");
    for root in [&root_a, &root_b] {
        fs::create_dir_all(root).unwrap();
    }
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    for (project_id, root, group) in [
        (PROJECT_ID, &root_a, "pa-project"),
        ("project-b", &root_b, "pa-project-b"),
    ] {
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                project_id,
                root,
                group,
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
    }
    // Campaign A
    start_campaign(&db);
    terminalize_baseline(&db);
    seed_metrics(&db, BASELINE_EXPT);
    // Campaign B with its own experiment and metrics
    let objective = ObjectiveSnapshot {
        text: "Reach goal".to_owned(),
        digest: "objective-digest-b".to_owned(),
    };
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: pueue_agent::models::ProposalKind::Experiment,
            hypothesis: "Establish baseline".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    CampaignRepository::new(&db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: "campaign-b",
                project_id: "project-b",
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: "sub-b",
                experiment_id: "expt-b",
                proposal_id: "prop-b",
                metadata: &json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap();
    MetricsRepository::upsert(
        &db,
        &ExperimentMetricsRow {
            experiment_id: "expt-b".to_owned(),
            source: "manifest".to_owned(),
            primary_metric_name: Some("loss".to_owned()),
            primary_metric_value: Some(0.5),
            metrics_json: json!({"loss": 0.5}).to_string(),
            artifact_defect: None,
            created_at: 150,
            updated_at: 150,
        },
    )
    .unwrap();

    let decisions = DecisionRepository::new(&db);
    let cycle = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, BASELINE_EXPT, 130)
        .unwrap();
    let event = pueue_agent::models::NewEvent::new(
        PROJECT_ID,
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        json!({
            "source": "terminal_experiment",
            "cycle_id": cycle.cycle_id,
            "source_experiment_id": BASELINE_EXPT,
        }),
        130,
        130,
    )
    .with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(BASELINE_EXPT.to_owned()));
    let (_, event) = decisions
        .publish_terminal_cycle_event(CAMPAIGN_ID, BASELINE_EXPT, &event, 130)
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE events SET status='completed', completed_at=135 WHERE event_id=?1",
            params![event.event_id],
        )
        .unwrap();
    let reservation = decisions
        .reserve_next_attempt(PROJECT_ID, &cycle.cycle_id, 140)
        .unwrap()
        .unwrap();
    let (context_json, context_digest) = build_decision_context(&db, &reservation);
    decisions
        .store_evidence(&reservation, &context_json, &context_digest, 140)
        .unwrap();
    let run_id = 9100;
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO agent_runs (run_id, project_id, primary_event_id, status, started_at, log_path, launch_gate_state) VALUES (?1, ?2, ?3, 'running', 140, '/tmp/cross.log', 'released')",
            params![run_id, PROJECT_ID, event.event_id],
        )
        .unwrap();
    decisions.bind_agent_run(&reservation, run_id, 141).unwrap();
    // Use evidence from other campaign (expt-b) which should be rejected.
    let goal_json = json!({
        "schema_version": 1,
        "decision": "goal_reached",
        "evidence_ref": "expt-b"
    })
    .to_string();
    let decision = parse_and_validate_decision(
        goal_json.as_bytes(),
        "objective-digest",
        CampaignLimits::default(),
    )
    .unwrap();
    decisions
        .store_decision(
            run_id,
            &goal_json,
            decision.canonical_digest(),
            "goal_reached",
            142,
        )
        .unwrap();

    let dummy = DummyPueue;
    let coordinator = DecisionCoordinator::new(&db, &dummy, CampaignLimits::default());
    let report = coordinator.apply_ready(150, 10).await.unwrap();
    // First failure does not yet degrade the cycle (threshold 3), but it exercises the counter path.
    assert_eq!(report.degraded, 0);
    let campaign = CampaignRepository::new(&db)
        .find_by_id(CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::Active);
    let cycle_after = decisions
        .find_cycle_for_source(CAMPAIGN_ID, BASELINE_EXPT)
        .unwrap()
        .unwrap();
    assert_ne!(cycle_after.state, DecisionCycleState::Completed);
    assert_eq!(cycle_after.consecutive_failed_attempts, 1);
}

#[tokio::test]
async fn invalid_evidence_degrades_via_counter_not_only_parse() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    terminalize_baseline(&db);
    // No metrics row for evidence -> invalid
    let decisions = DecisionRepository::new(&db);
    let cycle = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, BASELINE_EXPT, 130)
        .unwrap();
    let event = pueue_agent::models::NewEvent::new(
        PROJECT_ID,
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        json!({
            "source": "terminal_experiment",
            "cycle_id": cycle.cycle_id,
            "source_experiment_id": BASELINE_EXPT,
        }),
        130,
        130,
    )
    .with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(BASELINE_EXPT.to_owned()));
    let (_, event) = decisions
        .publish_terminal_cycle_event(CAMPAIGN_ID, BASELINE_EXPT, &event, 130)
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE events SET status='completed', completed_at=135 WHERE event_id=?1",
            params![event.event_id],
        )
        .unwrap();
    let reservation = decisions
        .reserve_next_attempt(PROJECT_ID, &cycle.cycle_id, 140)
        .unwrap()
        .unwrap();
    let (context_json, context_digest) = build_decision_context(&db, &reservation);
    decisions
        .store_evidence(&reservation, &context_json, &context_digest, 140)
        .unwrap();
    let run_id = 9200;
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO agent_runs (run_id, project_id, primary_event_id, status, started_at, log_path, launch_gate_state) VALUES (?1, ?2, ?3, 'running', 140, '/tmp/invalid.log', 'released')",
            params![run_id, PROJECT_ID, event.event_id],
        )
        .unwrap();
    decisions.bind_agent_run(&reservation, run_id, 141).unwrap();
    let goal_json = json!({
        "schema_version": 1,
        "decision": "goal_reached",
        "evidence_ref": "nonexistent-evidence"
    })
    .to_string();
    let decision = parse_and_validate_decision(
        goal_json.as_bytes(),
        "objective-digest",
        CampaignLimits::default(),
    )
    .unwrap();
    decisions
        .store_decision(
            run_id,
            &goal_json,
            decision.canonical_digest(),
            "goal_reached",
            142,
        )
        .unwrap();

    let dummy = DummyPueue;
    let coordinator = DecisionCoordinator::new(&db, &dummy, CampaignLimits::default());
    let report = coordinator.apply_ready(150, 10).await.unwrap();
    assert_eq!(report.degraded, 0);
    let cycle_after = decisions
        .find_cycle_for_source(CAMPAIGN_ID, BASELINE_EXPT)
        .unwrap()
        .unwrap();
    assert_eq!(cycle_after.consecutive_failed_attempts, 1);
    assert_ne!(
        cycle_after.last_decision_kind.as_deref(),
        Some("goal_reached")
    );
}

#[test]
fn accept_retires_with_exact_action_and_idempotent_log() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 140);
    let before: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    let campaign = CampaignRepository::new(&db)
        .review_accept(PROJECT_ID, Some("looks good"), 150)
        .unwrap();
    assert_eq!(campaign.state, CampaignState::Retired);
    assert_eq!(campaign.state_reason.as_deref(), Some("goal_accepted"));
    let after: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after, before + 1);
    let (action, details): (String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT action, details_json FROM operator_logs ORDER BY log_id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(action, "halt");
    let value: serde_json::Value = serde_json::from_str(&details).unwrap();
    assert_eq!(value["review"], "accept");
    assert_eq!(value["reason"], "goal_accepted");
    assert_eq!(value["note"], "looks good");
    let second = CampaignRepository::new(&db)
        .review_accept(PROJECT_ID, None, 160)
        .unwrap();
    assert_eq!(second.state, CampaignState::Retired);
    let after_second: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after_second, after);
}

#[test]
fn reject_requires_exact_event_and_is_idempotent_without_duplicate_log() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 140);
    let cycle = DecisionRepository::new(&db)
        .find_cycle_for_source(CAMPAIGN_ID, BASELINE_EXPT)
        .unwrap()
        .unwrap();
    let cycle_id = cycle.cycle_id.clone();
    let event = EventRepository::new(&db)
        .find_by_id(
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT event_id FROM events WHERE dedup_key=?1 AND campaign_id=?2 AND experiment_id=?3",
                    params![
                        format!("campaign-decision:v1:{cycle_id}"),
                        CAMPAIGN_ID,
                        BASELINE_EXPT
                    ],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
        )
        .unwrap()
        .unwrap();

    let completed_at_before: Option<i64> = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT completed_at FROM events WHERE event_id=?1",
            params![event.event_id],
            |row| row.get(0),
        )
        .unwrap();
    let log_before: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();

    let rejected = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("needs more"), 150)
        .unwrap();
    assert_eq!(rejected.state, CampaignState::Active);
    let (status, last_error, completed_at): (String, Option<String>, Option<i64>) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, last_error, completed_at FROM events WHERE event_id=?1",
            params![event.event_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "dead_letter");
    assert_eq!(last_error.as_deref(), Some("goal_claim_rejected"));
    assert_eq!(completed_at, completed_at_before);
    let (action, details): (String, String) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT action, details_json FROM operator_logs ORDER BY log_id DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(action, "resume");
    let value: serde_json::Value = serde_json::from_str(&details).unwrap();
    assert_eq!(value["review"], "reject");
    assert_eq!(value["reason"], "goal_claim_rejected");
    let log_after: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(log_after, log_before + 1);

    let second = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, None, 160)
        .unwrap();
    assert_eq!(second.state, CampaignState::Active);
    let log_final: i64 = db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM operator_logs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(log_final, log_after);
}

#[test]
fn ordinary_resume_or_retire_cannot_bypass_review() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 200);
    assert!(CampaignRepository::new(&db)
        .resume(PROJECT_ID, 300)
        .is_err());
    assert!(CampaignRepository::new(&db)
        .retire(PROJECT_ID, 300)
        .is_err());
}

#[test]
fn status_human_text_shows_pending_review() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 200);
    let project = ProjectRepository::new(&db)
        .find_by_id(PROJECT_ID)
        .unwrap()
        .unwrap();
    let human = campaign::render_status_for_project(&db, &project, false).unwrap();
    assert!(human.contains("goal_reached_pending_review"), "{human}");
    assert!(human.contains("state_reason"), "{human}");
    let json = campaign::render_status_for_project(&db, &project, true).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["state"], "goal_reached_pending_review");
}

#[test]
fn cli_parser_accept_reject_expose_note_and_common_options() {
    use clap::Parser;
    use pueue_agent::cli::{CampaignReviewAction, Cli, Command};

    let cli = Cli::try_parse_from([
        "pueue-agent",
        "campaign",
        "review",
        "accept",
        "--note",
        "looks good",
        "--json",
        "/tmp/project",
    ])
    .unwrap();
    match cli.command {
        Command::Campaign(args) => match args.action {
            pueue_agent::cli::CampaignAction::Review(review) => match review.action {
                CampaignReviewAction::Accept(leaf) => {
                    assert_eq!(leaf.note.as_deref(), Some("looks good"));
                    assert!(leaf.json);
                    assert_eq!(leaf.project_root.unwrap().to_str().unwrap(), "/tmp/project");
                }
                _ => panic!("expected accept"),
            },
            _ => panic!("expected review"),
        },
        _ => panic!("expected campaign"),
    }

    let cli = Cli::try_parse_from([
        "pueue-agent",
        "campaign",
        "review",
        "reject",
        "--note",
        "needs work",
        "--pueue-config",
        "/tmp/pueue.yml",
        "/tmp/project",
    ])
    .unwrap();
    match cli.command {
        Command::Campaign(args) => match args.action {
            pueue_agent::cli::CampaignAction::Review(review) => match review.action {
                CampaignReviewAction::Reject(leaf) => {
                    assert_eq!(leaf.note.as_deref(), Some("needs work"));
                    assert_eq!(
                        leaf.pueue_config.unwrap().to_str().unwrap(),
                        "/tmp/pueue.yml"
                    );
                }
                _ => panic!("expected reject"),
            },
            _ => panic!("expected review"),
        },
        _ => panic!("expected campaign"),
    }

    let help = assert_cmd::Command::cargo_bin("pueue-agent")
        .unwrap()
        .args(["campaign", "review", "accept", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    let text = String::from_utf8_lossy(&help.stdout);
    assert!(text.contains("--note"), "{text}");
    assert!(text.contains("--json"), "{text}");
    assert!(text.contains("PROJECT_ROOT"), "{text}");
}

#[test]
fn note_bounds_are_enforced() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 200);
    assert!(CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("bad\x02note"), 300)
        .is_err());
    let long = "y".repeat(5000);
    assert!(CampaignRepository::new(&db)
        .review_accept(PROJECT_ID, Some(&long), 300)
        .is_err());
}

fn park_via_real_claim(db: &Db, run_id: i64, now_complete: i64) {
    terminalize_baseline(db);
    let decisions = DecisionRepository::new(db);
    let cycle = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, BASELINE_EXPT, 130)
        .unwrap();
    let event = pueue_agent::models::NewEvent::new(
        PROJECT_ID,
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        json!({
            "source": "terminal_experiment",
            "cycle_id": cycle.cycle_id,
            "source_experiment_id": BASELINE_EXPT,
            "terminal_observation": {
                "task_id": 41,
                "task_signature": "task-sig-41",
                "group": "pa-project",
                "state": "Done",
                "enqueued_at": 110,
                "started_at": 120,
                "ended_at": 130,
                "exit_code": 0,
            },
        }),
        130,
        130,
    )
    .with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(BASELINE_EXPT.to_owned()));
    let (_, event) = decisions
        .publish_terminal_cycle_event(CAMPAIGN_ID, BASELINE_EXPT, &event, 130)
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE events SET status='completed', completed_at=135, attempts=1 WHERE event_id=?1",
            params![event.event_id],
        )
        .unwrap();
    let reservation = decisions
        .reserve_next_attempt(PROJECT_ID, &cycle.cycle_id, 140)
        .unwrap()
        .unwrap();
    let (context_json, context_digest) = build_decision_context(db, &reservation);
    decisions
        .store_evidence(&reservation, &context_json, &context_digest, 140)
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO agent_runs (run_id, project_id, primary_event_id, status, started_at, log_path, launch_gate_state) VALUES (?1, ?2, ?3, 'running', 140, '/tmp/goal-helper.log', 'released')",
            params![run_id, PROJECT_ID, event.event_id],
        )
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id) VALUES (?1, ?2, ?3)",
            params![PROJECT_ID, run_id, event.event_id],
        )
        .unwrap();
    decisions.bind_agent_run(&reservation, run_id, 141).unwrap();
    let goal_json = json!({
        "schema_version": 1,
        "decision": "goal_reached",
        "evidence_ref": BASELINE_EXPT
    })
    .to_string();
    let decision = parse_and_validate_decision(
        goal_json.as_bytes(),
        "objective-digest",
        CampaignLimits::default(),
    )
    .unwrap();
    decisions
        .store_decision(
            run_id,
            &goal_json,
            decision.canonical_digest(),
            "goal_reached",
            142,
        )
        .unwrap();
    decisions
        .complete_goal_claim_atomically(
            &cycle.cycle_id,
            reservation.attempt_number,
            BASELINE_EXPT,
            now_complete,
        )
        .unwrap();
}

fn create_second_experiment(
    db: &Db,
    proposal_id: &str,
    experiment_id: &str,
    submission_id: &str,
    hypothesis: &str,
    argv_extra: &str,
    task_id: i64,
    task_sig: &str,
    now: i64,
) -> String {
    let proposal = proposals::validate(
        ProposalInput {
            kind: pueue_agent::models::ProposalKind::Experiment,
            hypothesis: hypothesis.to_owned(),
            source_experiment_id: Some(BASELINE_EXPT.to_owned()),
            argv: vec!["python".to_owned(), "train.py".to_owned(), argv_extra.to_owned()],
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        "objective-digest",
    )
    .unwrap();
    let intent = CampaignRepository::new(db)
        .accept_proposal(
            CAMPAIGN_ID,
            proposal_id,
            experiment_id,
            submission_id,
            &proposal,
            &CampaignLimits::default(),
            now,
        )
        .unwrap()
        .accepted()
        .unwrap();
    let eid = intent.experiment.experiment_id.clone();
    let repo = ExperimentRepository::new(db);
    repo.mark_submitting(&eid, now + 1).unwrap();
    repo.mark_accepted(&eid, task_id, task_sig, now + 2).unwrap();
    repo.project_terminal_submission(&eid, task_id, ExperimentTerminalOutcome::Succeeded, now + 3)
        .unwrap();
    MetricsRepository::upsert(
        db,
        &ExperimentMetricsRow {
            experiment_id: eid.clone(),
            source: "manifest".to_owned(),
            primary_metric_name: Some("loss".to_owned()),
            primary_metric_value: Some(0.1),
            metrics_json: serde_json::json!({"loss": 0.1}).to_string(),
            artifact_defect: None,
            created_at: now + 3,
            updated_at: now + 3,
        },
    )
    .unwrap();
    eid
}

fn park_claim(db: &Db, experiment_id: &str, run_id: i64, task_id: i64, task_sig: &str, now: i64) -> i64 {
    let decisions = DecisionRepository::new(db);
    let cycle = decisions.ensure_cycle_for_terminal(CAMPAIGN_ID, experiment_id, now).unwrap();
    let event = pueue_agent::models::NewEvent::new(
        PROJECT_ID,
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        serde_json::json!({"source":"terminal_experiment","cycle_id":cycle.cycle_id,"source_experiment_id":experiment_id,"terminal_observation":{"task_id":task_id,"task_signature":task_sig,"group":"pa-project","state":"Done","enqueued_at":now-2,"started_at":now-1,"ended_at":now,"exit_code":0}}),
        now, now,
    ).with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(experiment_id.to_owned()));
    let (_, event) = decisions.publish_terminal_cycle_event(CAMPAIGN_ID, experiment_id, &event, now).unwrap();
    db.connect().unwrap().execute("UPDATE events SET status='completed', completed_at=?2, attempts=1 WHERE event_id=?1", rusqlite::params![event.event_id, now+1]).unwrap();
    let reservation = decisions.reserve_next_attempt(PROJECT_ID, &cycle.cycle_id, now+1).unwrap().unwrap();
    let (ctx_json, ctx_digest) = build_decision_context(db, &reservation);
    decisions.store_evidence(&reservation, &ctx_json, &ctx_digest, now+1).unwrap();
    db.connect().unwrap().execute("INSERT INTO agent_runs (run_id, project_id, primary_event_id, status, started_at, log_path, launch_gate_state) VALUES (?1, ?2, ?3, 'running', ?4, '/tmp/goal2.log', 'released')", rusqlite::params![run_id, PROJECT_ID, event.event_id, now+1]).unwrap();
    db.connect().unwrap().execute("INSERT INTO agent_run_events (project_id, run_id, event_id) VALUES (?1, ?2, ?3)", rusqlite::params![PROJECT_ID, run_id, event.event_id]).unwrap();
    decisions.bind_agent_run(&reservation, run_id, now+2).unwrap();
    let goal_json = serde_json::json!({"schema_version":1,"decision":"goal_reached","evidence_ref":experiment_id}).to_string();
    let decision = parse_and_validate_decision(goal_json.as_bytes(), "objective-digest", CampaignLimits::default()).unwrap();
    decisions.store_decision(run_id, &goal_json, decision.canonical_digest(), "goal_reached", now+3).unwrap();
    decisions.complete_goal_claim_atomically(&cycle.cycle_id, reservation.attempt_number, experiment_id, now+4).unwrap();
    event.event_id
}


#[test]
fn reject_fails_closed_when_ambiguous_multiple_completed_claimants_exist() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    // Create two distinct experiments with metrics and terminal cycles.
    // Second experiment via proposal.
    terminalize_baseline(&db);
    let second_experiment_id = create_second_experiment(
        &db,
        "proposal-second",
        "experiment-second",
        "submission-second",
        "second experiment",
        "--second",
        42,
        "task-sig-42",
        131,
    );

    // Create two completed goal cycles via direct SQL fixture and their completed events.
    // This simulates ambiguous state where schema permits two completed claimants.
    let decisions = DecisionRepository::new(&db);
    let cycle_a = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, BASELINE_EXPT, 134)
        .unwrap();
    let cycle_b = decisions
        .ensure_cycle_for_terminal(CAMPAIGN_ID, &second_experiment_id, 134)
        .unwrap();
    // Manually complete both cycles as goal_reached (bypass normal flow for ambiguity fixture).
    db.connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles SET state='completed', last_decision_kind='goal_reached', updated_at=140 WHERE cycle_id=?1",
            params![cycle_a.cycle_id],
        )
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles SET state='completed', last_decision_kind='goal_reached', updated_at=141 WHERE cycle_id=?1",
            params![cycle_b.cycle_id],
        )
        .unwrap();
    // Park campaign via SQL (scheduler fixture style allowed).
    db.connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET state='goal_reached_pending_review', state_reason='goal_reached:fixture', updated_at=142 WHERE campaign_id=?1",
            params![CAMPAIGN_ID],
        )
        .unwrap();
    for cycle in [&cycle_a, &cycle_b] {
        let source = if cycle.cycle_id == cycle_a.cycle_id {
            BASELINE_EXPT
        } else {
            second_experiment_id.as_str()
        };
        let event = EventRepository::new(&db)
            .insert_idempotent(
                &pueue_agent::models::NewEvent::new(
                    PROJECT_ID,
                    EventKind::CampaignDecision,
                    format!("campaign-decision:v1:{}", cycle.cycle_id),
                    json!({"source":"terminal_experiment","cycle_id":cycle.cycle_id,"source_experiment_id":source}),
                    135,
                    135,
                )
                .with_campaign_lineage(CAMPAIGN_ID.to_owned(), Some(source.to_owned())),
            )
            .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE events SET status='completed', completed_at=135 WHERE event_id=?1",
                params![event.event_id],
            )
            .unwrap();
    }

    let result = CampaignRepository::new(&db).review_reject(PROJECT_ID, Some("ambiguous"), 150);
    assert!(
        result.is_err(),
        "ambiguous multiple completed claimants must fail closed, got {result:?}"
    );
    let campaign = CampaignRepository::new(&db)
        .find_by_id(CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::GoalReachedPendingReview);
}

#[test]
fn reject_second_claim_only_dead_letters_second_exact_event() {
    let (_temp, db, _root) = new_harness();
    start_campaign(&db);
    seed_metrics(&db, BASELINE_EXPT);
    park_via_real_claim(&db, 9001, 145);
    let first_cycle = DecisionRepository::new(&db)
        .find_cycle_for_source(CAMPAIGN_ID, BASELINE_EXPT)
        .unwrap()
        .unwrap();
    let first_event_id: i64 = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT event_id FROM events WHERE dedup_key=?1 AND campaign_id=?2 AND experiment_id=?3",
            params![
                format!("campaign-decision:v1:{}", first_cycle.cycle_id),
                CAMPAIGN_ID,
                BASELINE_EXPT
            ],
            |row| row.get(0),
        )
        .unwrap();
    let rejected = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("first reject"), 150)
        .unwrap();
    assert_eq!(rejected.state, CampaignState::Active);
    let (first_status, _first_completed): (String, Option<i64>) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, completed_at FROM events WHERE event_id=?1",
            params![first_event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_status, "dead_letter");
    // Complete the first agent run so the per-project active-run uniqueness allows a second.
    db.connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status='completed', finished_at=150 WHERE run_id=9001",
            [],
        )
        .unwrap();

    // Create second valid goal claim from a second terminal experiment.
    let second_experiment_id = create_second_experiment(
        &db,
        "proposal-second-reclaim",
        "experiment-second-reclaim",
        "submission-second-reclaim",
        "second experiment for re-claim",
        "--second-reclaim",
        43,
        "task-sig-43",
        151,
    );

    let event2_id = park_claim(&db, &second_experiment_id, 9002, 43, "task-sig-43", 154);
    let campaign = CampaignRepository::new(&db)
        .find_by_id(CAMPAIGN_ID)
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::GoalReachedPendingReview);

    let second_rejected = CampaignRepository::new(&db)
        .review_reject(PROJECT_ID, Some("second reject"), 160)
        .unwrap();
    assert_eq!(second_rejected.state, CampaignState::Active);
    let (first_status_after, _): (String, Option<String>) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, last_error FROM events WHERE event_id=?1",
            params![first_event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_status_after, "dead_letter");
    let (second_status, second_last_error, second_completed): (
        String,
        Option<String>,
        Option<i64>,
    ) = db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, last_error, completed_at FROM events WHERE event_id=?1",
            params![event2_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(second_status, "dead_letter");
    assert_eq!(second_last_error.as_deref(), Some("goal_claim_rejected"));
    assert_eq!(second_completed, Some(155));
}
