use std::{
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    db::{
        AgentRunRepository, CampaignRepository, Db, EventRepository, ExperimentRepository,
        IncidentRepository, ProjectRepository, ResearchRepository, StartCampaignRequest,
        TerminationRequestRepository,
    },
    environment::PrivateRunTemp,
    execution_policy::{CampaignLimits, ResolvedExecutionPolicy},
    models::{
        AgentRunStatus, ExecutionProjection, NewAgentRun, NewIncident, NewProject,
        NewTerminationRequest, ProposalKind, TerminationRequestStatus,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, task_signature, Reconciler},
    research_actions::advance_research_actions,
    research_evidence::build_research_evidence,
    retry::{EventResolution, RetryPolicy},
    state::ObjectiveSnapshot,
    termination::TerminationManager,
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

#[derive(Clone)]
struct DelayedKillPueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    add_calls: Arc<Mutex<Vec<Vec<OsString>>>>,
}

impl DelayedKillPueue {
    fn new(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            add_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }

    fn add_calls(&self) -> Vec<Vec<OsString>> {
        self.add_calls.lock().unwrap().clone()
    }

    fn task(&self, id: i64) -> PueueTask {
        self.tasks
            .lock()
            .unwrap()
            .iter()
            .find(|task| task.id == id)
            .cloned()
            .expect("fixture task")
    }

    fn remove_task(&self, id: i64) {
        self.tasks
            .lock()
            .unwrap()
            .retain(|candidate| candidate.id != id);
    }
}

#[async_trait]
impl PueueApi for DelayedKillPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
        self.add_calls.lock().unwrap().push(args.to_vec());
        panic!("research action fixture must not submit before confirmed stop")
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        // A successful kill command is intentionally not a terminal observation.
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("research action fixture must not remove tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("research action fixture must not create groups")
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
    policy: Arc<ResolvedExecutionPolicy>,
    pueue: DelayedKillPueue,
    project_id: String,
    campaign_id: String,
    experiment_id: String,
    task_id: i64,
}

fn running_task(group: &str, id: i64) -> PueueTask {
    PueueTask {
        id,
        group: group.to_owned(),
        command: "python train.py --name experiment".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("100".to_owned()),
        ended_at: None,
        result: None,
    }
}

impl Harness {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let project_root = temp.path().join("project-a");
        let service_root = project_root.join(".pueue-agent");
        fs::create_dir_all(service_root.join("logs")).unwrap();
        fs::set_permissions(&project_root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&service_root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(service_root.join("logs"), fs::Permissions::from_mode(0o700)).unwrap();
        let project_root = fs::canonicalize(&project_root).unwrap();
        let config_path = service_root.join("config.toml");
        fs::write(
            &config_path,
            r#"project_id = "project-a"
pueue_group = "pa-project"

[agent]
program = "codex"
args = ["{prompt}"]
timeout_minutes = 10
max_retries = 1

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 4096
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
        )
        .unwrap();
        fs::write(service_root.join("logs/41.log"), "epoch 1 loss 0.52\n").unwrap();

        let policy = execution_policy_fixture::resolved_policy(
            temp.path(),
            &[("project-a", &project_root, Path::new("codex"))],
        );
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &project_root,
                "pa-project",
                &config_path,
                100,
            ))
            .unwrap();

        let objective = ObjectiveSnapshot {
            text: "Reach validation loss below 0.20".to_owned(),
            digest: "objective-digest-research-actions".to_owned(),
        };
        let argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--name".to_owned(),
            "experiment".to_owned(),
        ];
        let baseline = proposals::validate_initial_baseline(
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
        let campaign_id = "campaign-research-actions".to_owned();
        let experiment_id = "research-actions-experiment".to_owned();
        CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: &campaign_id,
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: "research-actions-submission",
                    experiment_id: &experiment_id,
                    proposal_id: "research-actions-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
        let task = running_task("pa-project", 41);
        let managed = managed_task_run_signature(&task).unwrap();
        ExperimentRepository::new(&db)
            .mark_submitting(&experiment_id, 101)
            .unwrap();
        ExperimentRepository::new(&db)
            .mark_accepted(&experiment_id, 41, &managed, 102)
            .unwrap();
        let pueue = DelayedKillPueue::new(vec![task.clone()]);
        Reconciler::new(&db, pueue.clone())
            .with_campaign_limits(CampaignLimits::default())
            .run_once_at(103)
            .await
            .unwrap();

        ResearchRepository::new(&db)
            .ensure_campaign(&campaign_id)
            .unwrap();
        ResearchRepository::new(&db)
            .schedule_running(&campaign_id, 100, 1, 200)
            .unwrap();
        let claimed = ResearchRepository::new(&db)
            .claim_due(&campaign_id, &experiment_id, &managed, 300)
            .unwrap()
            .expect("baseline research review");
        let reservation = match CampaignRepository::new(&db)
            .reserve_agent_run(
                &campaign_id,
                &format!("research:{}:attempt:1", claimed.review_id),
                &CampaignLimits::default(),
                301,
            )
            .unwrap()
        {
            pueue_agent::db::AgentDecisionReservation::Reserved(reservation) => reservation,
            other => panic!("research fixture budget must admit: {other:?}"),
        };
        let claimed = ResearchRepository::new(&db)
            .prepare_attempt(
                &claimed.review_id,
                &reservation.reservation_id,
                CampaignLimits::default().max_decision_attempts_per_cycle,
                302,
            )
            .unwrap()
            .expect("attempt one must be admitted");
        let evidence = build_research_evidence(&db, &claimed, 303).unwrap();

        let event_id = ResearchRepository::new(&db)
            .event_id(&claimed.review_id)
            .unwrap();
        EventRepository::new(&db)
            .claim_by_id("project-a", event_id, 304)
            .unwrap()
            .expect("research event must be claimed before native binding");
        let run = AgentRunRepository::new(&db)
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a",
                    event_id,
                    None,
                    AgentRunStatus::Starting,
                    304,
                    service_root.join("logs/research-run.log"),
                )
                .with_execution(
                    ExecutionProjection::new("campaign_research", "/bin/echo", "fixture").unwrap(),
                ),
                &[event_id],
            )
            .unwrap();
        let session_id = "11111111-1111-4111-8111-111111111111".to_owned();
        let binding = pueue_agent::db::ResearchLaunchBinding {
            review_id: claimed.review_id.clone(),
            campaign_id: claimed.campaign_id.clone(),
            experiment_id: claimed.experiment_id.clone(),
            attempt: claimed.attempt,
            session_generation: claimed.session_generation,
            prior_session_generation: claimed.session_generation,
            session_id: session_id.clone(),
            prior_session_id: None,
            context_json: evidence.json.clone(),
            context_digest: evidence.digest.clone(),
            budget_reservation_id: reservation.reservation_id.clone(),
            recovery_reason: None,
        };
        ResearchRepository::new(&db)
            .bind_agent_run(&binding, run.run_id, "project-a", 305)
            .unwrap();
        let verified_root = policy
            .project_root_anchor(&project_root)
            .unwrap()
            .verify_identity()
            .unwrap();
        let mut private_temp = PrivateRunTemp::create(&verified_root, run.run_id).unwrap();
        let recovery_identity = private_temp.recovery_identity(&verified_root).unwrap();
        ResearchRepository::new(&db)
            .record_native_recovery_authority(&binding, run.run_id, &recovery_identity, true, 306)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 9_041, 307)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        AgentRunRepository::new(&db)
            .acknowledge_dispatch("project-a", run.run_id)
            .unwrap();
        ResearchRepository::new(&db)
            .confirm_agent_run_session(&binding, run.run_id, &session_id, 308)
            .unwrap();
        let answer = json!({
            "schema_version": 1,
            "review_id": claimed.review_id,
            "experiment_id": claimed.experiment_id,
            "context_digest": evidence.digest,
            "action": "stop_and_next",
            "reason": "the baseline should be pruned",
            "evidence_refs": [format!("research:{}", claimed.review_id)],
            "notes": "save this bounded advice",
            "next_direction": "try a smaller learning rate",
            "checkpoint": null,
        })
        .to_string();
        ResearchRepository::new(&db)
            .finish_agent_run(&binding, run.run_id, &session_id, &answer, false, 309)
            .unwrap();
        AgentRunRepository::new(&db)
            .finish_and_resolve_events(
                "project-a",
                run.run_id,
                AgentRunStatus::Completed,
                310,
                Some(0),
                None,
                EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
            )
            .unwrap();
        private_temp.cleanup_contents_before(None).unwrap();
        let cleaned_identity = private_temp.recovery_identity(&verified_root).unwrap();
        ResearchRepository::new(&db)
            .mark_native_cleanup_complete(&binding, run.run_id, &cleaned_identity, true, 311)
            .unwrap();

        Self {
            _temp: temp,
            db,
            policy,
            pueue,
            project_id: "project-a".to_owned(),
            campaign_id,
            experiment_id,
            task_id: 41,
        }
    }

    fn set_answer_action(&self, action: &str) {
        let review = ResearchRepository::new(&self.db)
            .recent(&self.campaign_id, 1)
            .unwrap()
            .pop()
            .unwrap();
        let mut answer: serde_json::Value =
            serde_json::from_str(review.response_json.as_deref().unwrap()).unwrap();
        answer["action"] = json!(action);
        if action == "continue" {
            answer["reason"] = json!("continue with the current direction");
            answer["notes"] = json!("bounded continuation advice");
            answer["next_direction"] = serde_json::Value::Null;
            answer["checkpoint"] = serde_json::Value::Null;
        }
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE research_reviews SET response_json = ?1 WHERE review_id = ?2",
                rusqlite::params![answer.to_string(), review.review_id],
            )
            .unwrap();
    }

    fn set_task(&self, task: PueueTask) {
        let mut tasks = self.pueue.tasks.lock().unwrap();
        let stored = tasks
            .iter_mut()
            .find(|candidate| candidate.id == task.id)
            .expect("fixture task");
        *stored = task;
    }

    fn insert_deferred_rotation_review(&self, suffix: &str) -> String {
        let campaign_id = format!("rotation-campaign-{suffix}");
        let proposal_id = format!("rotation-proposal-{suffix}");
        let submission_id = format!("rotation-submission-{suffix}");
        let experiment_id = format!("rotation-experiment-{suffix}");
        let event_key = format!("rotation-event-{suffix}");
        let review_id = format!("rotation-review-{suffix}");
        let connection = self.db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, state_reason, baseline_experiment_id,
                    next_eligible_at, created_at, updated_at
                 ) VALUES (?1, ?2, 'rotation', ?3,
                           '[]', 'retired', NULL, NULL, NULL, 50, 50)",
                rusqlite::params![
                    campaign_id,
                    self.project_id,
                    format!("rotation-digest-{suffix}")
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO proposals (
                    proposal_id, campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, canonical_digest, reject_reason,
                    created_at, updated_at
                 ) VALUES (?1, ?2, 'experiment',
                           'pending', 'deferred rotation owner', NULL, '[]', '.',
                           '[]', ?3, NULL, 50, 50)",
                rusqlite::params![
                    proposal_id,
                    format!("rotation-campaign-{suffix}"),
                    format!("rotation-proposal-digest-{suffix}")
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at,
                    pueue_task_id, task_signature, status, kind, metadata_json,
                    origin_agent_run_id
                 ) VALUES (?1, ?2, '[]', 50, NULL, NULL,
                           'pending', 'experiment', '{}', NULL)",
                rusqlite::params![submission_id, self.project_id],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO experiments (
                    experiment_id, campaign_id, proposal_id, submission_id,
                    parent_experiment_id, attempt, status, pueue_task_id,
                    task_signature, failure_code, failure_fingerprint, created_at,
                    updated_at, finished_at, resume_of_experiment_id,
                    checkpoint_note, code_change_run_id, code_revision_sha
                 ) VALUES (?1, ?2, ?3, ?4, NULL, 0,
                           'reserved', NULL, NULL, NULL, NULL, 50, 50, NULL,
                           NULL, NULL, NULL, NULL)",
                rusqlite::params![
                    experiment_id,
                    format!("rotation-campaign-{suffix}"),
                    format!("rotation-proposal-{suffix}"),
                    format!("rotation-submission-{suffix}"),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO events (
                    project_id, campaign_id, experiment_id, kind, dedup_key,
                    payload_json, status, attempts, not_before, lease_until,
                    created_at, completed_at, last_error
                 ) VALUES (?1, ?2, ?3,
                           'campaign_research', ?4, '{}', 'pending',
                           0, 50, NULL, 50, NULL, NULL)",
                rusqlite::params![
                    self.project_id,
                    format!("rotation-campaign-{suffix}"),
                    format!("rotation-experiment-{suffix}"),
                    event_key,
                ],
            )
            .unwrap();
        let event_id: i64 = connection
            .query_row(
                "SELECT event_id FROM events WHERE dedup_key = ?1",
                [&event_key],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO research_reviews (
                    review_id, campaign_id, experiment_id, task_signature, attempt,
                    state, operation_stage, agent_run_id, context_json, context_digest,
                    response_json, termination_request_id, successor_experiment_id,
                    evidence_schema_version, session_generation, event_id, not_before,
                    notes_json, failure_code, decision_cycle_id, checkpoint_json,
                    created_at, started_at, finished_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, 1,
                           'ready', 'intent', NULL, NULL, NULL, NULL, NULL, NULL,
                           1, 0, ?5, 50, NULL, NULL, NULL, NULL, 50, NULL, NULL, 50)",
                rusqlite::params![
                    review_id,
                    format!("rotation-campaign-{suffix}"),
                    format!("rotation-experiment-{suffix}"),
                    format!("pueue-managed-run:v1:rotation-{suffix}"),
                    event_id,
                ],
            )
            .unwrap();
        review_id
    }
}

#[tokio::test]
async fn delayed_kill_exit_zero_does_not_create_successor_or_cycle() {
    let harness = Harness::new().await;
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .expect("research action pass");
    assert_eq!(advanced, 1);

    let review = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    let request_id = review
        .termination_request_id
        .expect("durable termination intent");
    assert_eq!(review.operation_stage.as_deref(), Some("intent"));
    assert_eq!(harness.pueue.kill_calls(), Vec::<i64>::new());

    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .expect("kill dispatch");

    let review = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(harness.pueue.kill_calls(), vec![harness.task_id]);
    assert_eq!(review.operation_stage.as_deref(), Some("stop_requested"));
    assert_eq!(harness.pueue.task(harness.task_id).state, "Running");
    assert!(harness.pueue.add_calls().is_empty());
    let connection = harness.db.connect().unwrap();
    let successors: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE resume_of_experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    let cycles: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM decision_cycles WHERE source_experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successors, 0);
    assert_eq!(cycles, 0);
}

#[tokio::test]
async fn confirmed_stop_progression_attaches_once_after_reconcile_even_when_interval_disabled() {
    let harness = Harness::new().await;
    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 1);
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();

    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.status, TerminationRequestStatus::Sent);
    assert!(request.grace_until.is_some());
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .find(&review.review_id)
            .unwrap()
            .operation_stage
            .as_deref(),
        Some("stop_requested")
    );

    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);
    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(500)
        .await
        .unwrap();

    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.status, TerminationRequestStatus::Confirmed);
    let review_after_reconcile = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(
        review_after_reconcile.operation_stage.as_deref(),
        Some("stop_requested")
    );
    let connection = harness.db.connect().unwrap();
    let cycles: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM decision_cycles WHERE source_experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    let decision_events: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE campaign_id = ?1 AND kind = 'campaign_decision'",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cycles, 1);
    assert_eq!(decision_events, 0);

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    let advanced = advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 600, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 1);

    let attached = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(attached.state, "completed");
    assert!(attached.operation_stage.is_none());
    assert!(attached.successor_experiment_id.is_none());
    assert_eq!(attached.termination_request_id, Some(request_id));
    let attached_cycle: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    let event_status: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM events
             WHERE campaign_id = ?1 AND kind = 'campaign_decision'",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!attached_cycle.is_empty());
    assert_eq!(event_status, "pending");

    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(700)
        .await
        .unwrap();
    let replayed = advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 800, 1)
        .await
        .unwrap();
    assert_eq!(replayed, 0);
    let connection = harness.db.connect().unwrap();
    let cycle_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM decision_cycles WHERE source_experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    let event_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE campaign_id = ?1 AND kind = 'campaign_decision'",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cycle_count, 1);
    assert_eq!(event_count, 1);
}

#[tokio::test]
async fn bounded_open_action_scan_reaches_newer_owner_after_deferred_owner() {
    let harness = Harness::new().await;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
            .await
            .unwrap(),
        1
    );
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);
    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(500)
        .await
        .unwrap();

    let deferred_review_id = harness.insert_deferred_rotation_review("single");
    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 600, 1)
            .await
            .unwrap(),
        1
    );

    let deferred = ResearchRepository::new(&harness.db)
        .find(&deferred_review_id)
        .unwrap();
    assert_eq!(deferred.state, "ready");
    assert_eq!(deferred.operation_stage.as_deref(), Some("intent"));
    let completed = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(completed.state, "completed");
    assert!(completed.operation_stage.is_none());
}

#[tokio::test]
async fn rotated_open_action_scan_reaches_owner_after_full_deferred_prefix() {
    let harness = Harness::new().await;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
            .await
            .unwrap(),
        1
    );
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);
    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(500)
        .await
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET updated_at = 500 WHERE review_id = ?1",
            [&review.review_id],
        )
        .unwrap();

    let prefix_ids = (0..32)
        .map(|index| harness.insert_deferred_rotation_review(&format!("prefix-{index:02}")))
        .collect::<Vec<_>>();
    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 600, 1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 601, 1)
            .await
            .unwrap(),
        1
    );

    for review_id in prefix_ids {
        let prefix_review = ResearchRepository::new(&harness.db)
            .find(&review_id)
            .unwrap();
        assert_eq!(prefix_review.state, "ready");
        assert_eq!(prefix_review.operation_stage.as_deref(), Some("intent"));
    }
    let completed = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(completed.state, "completed");
}

#[tokio::test]
async fn sent_request_repairs_intent_after_stage_update_crash() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET operation_stage = 'intent'
             WHERE review_id = ?1 AND termination_request_id = ?2",
            rusqlite::params![review.review_id, request_id],
        )
        .unwrap();

    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);
    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(500)
        .await
        .unwrap();
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .find(&review.review_id)
            .unwrap()
            .operation_stage
            .as_deref(),
        Some("intent")
    );

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 600, 1)
            .await
            .unwrap(),
        1
    );
    let completed = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(completed.state, "completed");
    assert!(completed.operation_stage.is_none());
    assert_eq!(completed.termination_request_id, Some(request_id));
}

#[tokio::test]
async fn undispatched_intent_natural_terminal_discards_without_cycle() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 500, 1)
            .await
            .unwrap(),
        1
    );
    let discarded = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(discarded.state, "discarded");
    assert!(discarded.operation_stage.is_none());
    assert_eq!(discarded.termination_request_id, Some(request_id));
    let connection = harness.db.connect().unwrap();
    let cycles: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM decision_cycles WHERE source_experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(cycles, 0);
}

#[tokio::test]
async fn requested_research_termination_defers_a_nonterminal_paused_task() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Queued".to_owned();
    harness.set_task(task);

    let outcome = TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        pueue_agent::termination::TerminationOutcome::PendingConfirmation
    );
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.status, TerminationRequestStatus::Requested);
    assert!(request.grace_until.is_none());
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .find(&review.review_id)
            .unwrap()
            .operation_stage
            .as_deref(),
        Some("intent")
    );
}

#[tokio::test]
async fn prefix_shaped_unbound_termination_keeps_generic_nonrunning_semantics() {
    let harness = Harness::new().await;
    let raw_signature = task_signature(&harness.pueue.task(harness.task_id));
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            harness.project_id.clone(),
            "operator",
            Some(format!("task:{}", harness.task_id)),
            "prefix-shaped-unbound-request",
            400,
        ))
        .unwrap();
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident.incident_id,
            harness.project_id.clone(),
            raw_signature,
            "research_action:unbound:operator-request",
            400,
            None,
        ))
        .unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Queued".to_owned();
    harness.set_task(task);

    let outcome = TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request.request_id)
        .await
        .unwrap();
    assert_eq!(
        outcome,
        pueue_agent::termination::TerminationOutcome::AlreadyTerminal
    );
    let stored = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request.request_id)
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, TerminationRequestStatus::Confirmed);
    assert_eq!(
        stored.last_error.as_deref(),
        Some("task signature is no longer active")
    );
}

#[tokio::test]
async fn missing_target_after_undispatched_confirmation_discards_open_owner() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    harness.pueue.remove_task(harness.task_id);

    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let request = TerminationRequestRepository::new(&harness.db)
        .find_by_id(request_id)
        .unwrap()
        .unwrap();
    assert_eq!(request.status, TerminationRequestStatus::Confirmed);
    assert!(request.grace_until.is_none());
    assert!(request.last_error.is_some());

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 500, 1)
            .await
            .unwrap(),
        1
    );
    let discarded = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(discarded.state, "discarded");
    assert!(discarded.operation_stage.is_none());
    assert_eq!(discarded.termination_request_id, Some(request_id));
}

#[tokio::test]
async fn missing_target_with_corrupt_owner_stays_open_after_undispatched_confirmation() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    let event_id = ResearchRepository::new(&harness.db)
        .event_id(&review.review_id)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'pending', completed_at = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    harness.pueue.remove_task(harness.task_id);

    TerminationManager::new(&harness.db, harness.pueue.clone())
        .execute(request_id)
        .await
        .unwrap();
    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 500, 1)
            .await
            .unwrap(),
        0
    );
    let retained = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(retained.state, "ready");
    assert_eq!(retained.operation_stage.as_deref(), Some("intent"));
    assert_eq!(retained.termination_request_id, Some(request_id));
}

#[tokio::test]
async fn confirmed_without_dispatch_proof_does_not_discard_possible_kill_owner() {
    let harness = Harness::new().await;
    advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let request_id = review.termination_request_id.unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE termination_requests
             SET status = 'confirmed', grace_until = NULL,
                 confirmed_at = ?1, last_error = 'dispatch outcome unknown'
             WHERE request_id = ?2",
            rusqlite::params![500, request_id],
        )
        .unwrap();
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Killed".to_owned();
    task.ended_at = Some("500".to_owned());
    task.result = Some(json!({"Success": 0}));
    harness.set_task(task);

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 500, 1)
            .await
            .unwrap(),
        0
    );
    let preserved = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(preserved.state, "ready");
    assert_eq!(preserved.operation_stage.as_deref(), Some("intent"));
}

#[tokio::test]
async fn valid_continue_saves_bounded_advice_and_next_due_without_stop() {
    let harness = Harness::new().await;
    harness.set_answer_action("continue");

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 1);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "completed");
    assert!(review.operation_stage.is_none());
    assert!(review.termination_request_id.is_none());
    let notes: serde_json::Value = {
        let connection = harness.db.connect().unwrap();
        let value: String = connection
            .query_row(
                "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
                [&review.review_id],
                |row| row.get(0),
            )
            .unwrap();
        serde_json::from_str(&value).unwrap()
    };
    assert_eq!(notes["saved_advice"], "bounded continuation advice");
    let next_due: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT next_due_at FROM campaign_research WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(next_due, 400 + 30 * 60);
}

#[tokio::test]
async fn disabled_research_interval_defers_ready_action() {
    let harness = Harness::new().await;
    let mut policy = (*harness.policy).clone();
    policy.campaign_limits.research_interval_minutes = 0;

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.operation_stage.is_none());
}

#[tokio::test]
async fn natural_terminal_target_discards_ready_action_without_kill() {
    let harness = Harness::new().await;
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Done".to_owned();
    task.ended_at = Some("400".to_owned());
    task.result = Some(json!(0));
    harness.set_task(task);

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 1);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "discarded");
    assert_eq!(review.operation_stage, None);
    assert_eq!(review.termination_request_id, None);
}

#[tokio::test]
async fn nonterminal_nonrunning_target_defers_ready_action_without_discarding() {
    let harness = Harness::new().await;
    let mut task = harness.pueue.task(harness.task_id);
    task.state = "Queued".to_owned();
    harness.set_task(task);

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.operation_stage.is_none());
}

#[tokio::test]
async fn reused_task_id_discards_stale_ready_action_without_kill() {
    let harness = Harness::new().await;
    let mut task = harness.pueue.task(harness.task_id);
    task.command = "python train.py --name reused-target".to_owned();
    harness.set_task(task);

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 1);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "discarded");
    assert_eq!(review.operation_stage, None);
    assert_eq!(review.termination_request_id, None);
}

#[tokio::test]
async fn paused_campaign_defers_ready_action() {
    let harness = Harness::new().await;
    CampaignRepository::new(&harness.db)
        .pause(&harness.project_id, 400)
        .unwrap();

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.operation_stage.is_none());
}

#[tokio::test]
async fn goal_review_campaign_defers_ready_action() {
    let harness = Harness::new().await;
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns
             SET state = 'goal_reached_pending_review', state_reason = 'goal_reached'
             WHERE campaign_id = ?1",
            [&harness.campaign_id],
        )
        .unwrap();

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.operation_stage.is_none());
}

#[tokio::test]
async fn health_owned_ready_action_defers_without_relabeling_health_stop() {
    let harness = Harness::new().await;
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE running_health
             SET state = 'action_pending', updated_at = ?2
             WHERE experiment_id = ?1",
            rusqlite::params![harness.experiment_id, 400,],
        )
        .unwrap();

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    let health_state: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT state FROM running_health WHERE experiment_id = ?1",
            [&harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(health_state, "action_pending");
}

#[tokio::test]
async fn exhausted_next_agent_budget_defers_stop_without_reservation_or_kill() {
    let harness = Harness::new().await;
    let mut policy = (*harness.policy).clone();
    policy.campaign_limits.max_agent_runs_per_hour = 1;

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.termination_request_id.is_none());
    let request_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM termination_requests WHERE project_id = ?1",
            [&harness.project_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(request_count, 0);
}

#[tokio::test]
async fn exhausted_next_experiment_budget_defers_stop_without_reservation_or_kill() {
    let harness = Harness::new().await;
    let mut policy = (*harness.policy).clone();
    policy.campaign_limits.max_new_experiments_per_24h = 1;

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.termination_request_id.is_none());
}

#[tokio::test]
async fn ready_action_rejects_an_unexpected_existing_termination_request() {
    let harness = Harness::new().await;
    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let raw_signature = task_signature(&harness.pueue.task(harness.task_id));
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            harness.project_id.clone(),
            "research_action",
            Some(format!("task:{}", harness.task_id)),
            "research-action-existing-request",
            400,
        ))
        .unwrap();
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident.incident_id,
            harness.project_id.clone(),
            raw_signature,
            "research_action:unexpected:stale request",
            400,
            None,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET termination_request_id = ?1
             WHERE review_id = ?2",
            rusqlite::params![request.request_id, review.review_id],
        )
        .unwrap();

    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 401, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);
    assert!(harness.pueue.kill_calls().is_empty());
    let review = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(review.state, "ready");
    assert!(review.operation_stage.is_none());
    assert_eq!(review.termination_request_id, Some(request.request_id));
}

#[tokio::test]
async fn ready_action_rolls_back_incident_and_request_when_binding_cas_loses() {
    let harness = Harness::new().await;
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER research_action_force_cas_miss
             AFTER INSERT ON termination_requests
             BEGIN
                 UPDATE research_reviews
                 SET state = 'completed', finished_at = updated_at
                 WHERE state = 'ready' AND operation_stage IS NULL;
             END;",
        )
        .unwrap();

    let review = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    let advanced = advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
        .await
        .unwrap();
    assert_eq!(advanced, 0);

    let connection = harness.db.connect().unwrap();
    let request_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM termination_requests WHERE project_id = ?1",
            [&harness.project_id],
            |row| row.get(0),
        )
        .unwrap();
    let incident_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM incidents WHERE project_id = ?1",
            [&harness.project_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(request_count, 0);
    assert_eq!(incident_count, 0);
    let review_after = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(review_after.state, "ready");
    assert!(review_after.operation_stage.is_none());
    assert!(review_after.termination_request_id.is_none());
}
