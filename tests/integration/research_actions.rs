use std::{
    ffi::OsString,
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
#[cfg(target_os = "linux")]
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    codex_command::CodexCapabilities,
    execution_policy::{load_existing_policy, PolicyLoadInput},
    scheduler::{Scheduler, SchedulerConfig},
};
use pueue_agent::{
    db::{
        AgentRunRepository, CampaignRepository, Db, DecisionRepository, EventRepository,
        DecisionReservation, ExperimentRepository, IncidentRepository, ProjectRepository,
        ResearchRepository, StartCampaignRequest, TerminationRequestRepository,
    },
    decision::DecisionCoordinator,
    decision_evidence::{
        DecisionEvidenceBuilder, DecisionEvidenceRequest, DecisionPueueTaskProjection,
    },
    decision_protocol::parse_and_validate_decision,
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
#[cfg(target_os = "linux")]
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

#[derive(Clone)]
struct DelayedKillPueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    kill_error: Arc<Mutex<Option<String>>>,
    add_calls: Arc<Mutex<Vec<Vec<OsString>>>>,
}

impl DelayedKillPueue {
    fn new(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            kill_error: Arc::new(Mutex::new(None)),
            add_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn set_kill_error(&self, message: impl Into<String>) {
        *self.kill_error.lock().unwrap() = Some(message.into());
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
        if self.kill_error.lock().unwrap().is_some() {
            return Err(AppError::Runtime {
                operation: "research action fixture kill",
            });
        }
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
    #[cfg(target_os = "linux")]
    fn native_decision_policy(&self) -> Arc<ResolvedExecutionPolicy> {
        let fixture_root = fs::canonicalize(self._temp.path()).unwrap();
        let trusted_dir = fixture_root.join("execution-policy-bin");
        let fake_codex = trusted_dir.join("research-action-native-codex");
        let fake_codex_source = trusted_dir.join("research-action-native-codex.rs");
        fs::write(
            &fake_codex_source,
            r##"use std::{env, fs, process::exit};

fn main() {
    let mut args = env::args().skip(1);
    let mut output = None;
    while let Some(argument) = args.next() {
        if argument == "--output-last-message" {
            output = args.next();
        }
    }
    let Some(output) = output else { exit(71); };
    fs::write(
        output,
        br#"{"schema_version":1,"decision":"wait","proposal":null,"reason":"native fixture wait","requested_wait_minutes":1,"expected_evidence":[],"evidence_ref":null}"#,
    )
    .unwrap();
}
"##,
        )
        .unwrap();
        let build = std::process::Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&fake_codex)
            .arg(&fake_codex_source)
            .output()
            .unwrap();
        assert!(
            build.status.success(),
            "native decision fixture failed to compile: {}",
            String::from_utf8_lossy(&build.stderr)
        );
        fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();

        let launcher = self.policy.launcher_anchor.canonical_path.clone();
        let policy_path = fixture_root
            .join("execution-policy-state")
            .join("execution-policy.toml");
        let body = fs::read_to_string(&policy_path).unwrap();
        let old_codex = format!("codex = {:?}", launcher.display().to_string());
        let new_codex = format!("codex = {:?}", fake_codex.display().to_string());
        assert!(body.contains(&old_codex), "fixture codex anchor must be replaceable");
        fs::write(&policy_path, body.replacen(&old_codex, &new_codex, 1)).unwrap();

        let trusted_path = std::env::join_paths([trusted_dir]).unwrap();
        Arc::new(
            load_existing_policy(&PolicyLoadInput {
                state_dir: fixture_root.join("execution-policy-state"),
                project_roots: self.policy.project_roots.clone(),
                inherited_path: trusted_path,
                startup_environment: self.policy.startup_environment.clone(),
                codex_home: self.policy.codex_home.clone(),
                pueue_config: self.policy.pueue_config_anchor.canonical_path.clone(),
                launcher_path: launcher,
            })
            .unwrap(),
        )
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

    async fn prepare_sent_handoff(&self) -> pueue_agent::db::ResearchReview {
        assert_eq!(
            advance_research_actions(&self.db, &self.pueue, &self.policy, 400, 1)
                .await
                .unwrap(),
            1
        );
        let review = ResearchRepository::new(&self.db)
            .recent(&self.campaign_id, 1)
            .unwrap()
            .pop()
            .unwrap();
        let request_id = review.termination_request_id.unwrap();
        TerminationManager::new(&self.db, self.pueue.clone())
            .execute(request_id)
            .await
            .unwrap();
        let request = TerminationRequestRepository::new(&self.db)
            .find_by_id(request_id)
            .unwrap()
            .unwrap();
        assert_eq!(request.status, TerminationRequestStatus::Sent);
        review
    }

    async fn prepare_stored_decision_attempt(
        &self,
    ) -> (
        pueue_agent::db::ResearchReview,
        DecisionReservation,
        String,
        i64,
        PueueTask,
    ) {
        let review = self.prepare_sent_handoff().await;
        let mut task = self.pueue.task(self.task_id);
        task.state = "Killed".to_owned();
        task.ended_at = Some("500".to_owned());
        task.result = Some(json!({"Success": 0}));
        self.set_task(task.clone());
        Reconciler::new(&self.db, self.pueue.clone())
            .with_campaign_limits(CampaignLimits::default())
            .run_once_at(500)
            .await
            .unwrap();

        let mut disabled_policy = (*self.policy).clone();
        disabled_policy.campaign_limits.research_interval_minutes = 0;
        assert_eq!(
            advance_research_actions(&self.db, &self.pueue, &disabled_policy, 600, 1)
                .await
                .unwrap(),
            1
        );
        let attached = ResearchRepository::new(&self.db)
            .find(&review.review_id)
            .unwrap();
        assert_eq!(attached.state, "completed");

        let connection = self.db.connect().unwrap();
        let cycle_id: String = connection
            .query_row(
                "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
                [&review.review_id],
                |row| row.get(0),
            )
            .unwrap();
        let decision_event_id: i64 = connection
            .query_row(
                "SELECT event_id FROM events
                 WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
                   AND kind = 'campaign_decision'",
                rusqlite::params![self.project_id, self.campaign_id, self.experiment_id],
                |row| row.get(0),
            )
            .unwrap();
        drop(connection);

        EventRepository::new(&self.db)
            .claim_by_id(&self.project_id, decision_event_id, 601)
            .unwrap()
            .expect("attached decision event must be claimable");
        let reservation = DecisionRepository::new(&self.db)
            .reserve_next_attempt(&self.project_id, &cycle_id, 602)
            .unwrap()
            .expect("terminal cycle must reserve one decision attempt");
        let project = ProjectRepository::new(&self.db)
            .find_by_id(&self.project_id)
            .unwrap()
            .unwrap();
        let root_anchor = self
            .policy
            .project_root_anchor(&project.root_path)
            .unwrap();
        let task_signature = managed_task_run_signature(&task).unwrap();
        let projection = DecisionPueueTaskProjection {
            task_id: task.id,
            task_signature,
            group: task.group.clone(),
            state: task.state.clone(),
            enqueued_at: task
                .enqueued_at
                .as_deref()
                .and_then(|value| value.parse::<i64>().ok()),
            started_at: task
                .started_at
                .as_deref()
                .and_then(|value| value.parse::<i64>().ok()),
            ended_at: task
                .ended_at
                .as_deref()
                .and_then(|value| value.parse::<i64>().ok()),
            exit_code: Some(0),
        };
        let context = DecisionEvidenceBuilder::new(&self.db)
            .build(&DecisionEvidenceRequest {
                reservation: &reservation,
                root_anchor: &root_anchor,
                pueue_tasks: &[projection],
                observed_at: 603,
            })
            .unwrap();
        DecisionRepository::new(&self.db)
            .store_evidence(&reservation, &context.json, &context.digest, 604)
            .unwrap();

        (
            review,
            reservation,
            cycle_id,
            decision_event_id,
            task,
        )
    }

    fn persist_decision_outcome(
        &self,
        reservation: &DecisionReservation,
        event_id: i64,
        decision_json: &str,
        decision_kind: &str,
        now: i64,
    ) -> String {
        let project = ProjectRepository::new(&self.db)
            .find_by_id(&self.project_id)
            .unwrap()
            .unwrap();
        let run = AgentRunRepository::new(&self.db)
            .insert_with_events(
                &NewAgentRun::new(
                    &self.project_id,
                    event_id,
                    None,
                    AgentRunStatus::Starting,
                    now,
                    project
                        .root_path
                        .join(".pueue-agent/logs/decision-outcome.log"),
                )
                .with_execution(
                    ExecutionProjection::new("codex", "/bin/echo", "fixture")
                        .unwrap(),
                ),
                &[event_id],
            )
            .unwrap();
        DecisionRepository::new(&self.db)
            .bind_agent_run(reservation, run.run_id, now + 1)
            .unwrap();
        AgentRunRepository::new(&self.db)
            .mark_running_and_apply_interventions(
                &self.project_id,
                run.run_id,
                9_999,
                now + 2,
            )
            .unwrap();
        AgentRunRepository::new(&self.db)
            .mark_gate_release_requested(&self.project_id, run.run_id)
            .unwrap();
        AgentRunRepository::new(&self.db)
            .acknowledge_dispatch(&self.project_id, run.run_id)
            .unwrap();

        let validated = parse_and_validate_decision(
            decision_json.as_bytes(),
            "objective-digest-research-actions",
            CampaignLimits::default(),
        )
        .unwrap();
        let digest = validated.canonical_digest().to_owned();
        DecisionRepository::new(&self.db)
            .store_decision(run.run_id, decision_json, &digest, decision_kind, now + 3)
            .unwrap();
        AgentRunRepository::new(&self.db)
            .finish_and_resolve_events(
                &self.project_id,
                run.run_id,
                AgentRunStatus::Completed,
                now + 4,
                Some(0),
                None,
                EventResolution::RetryPolicy(RetryPolicy { max_retries: 0 }),
            )
            .unwrap();
        digest
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

    fn insert_deferred_ready_rotation_review(&self, suffix: &str) -> String {
        let review_id = self.insert_deferred_rotation_review(suffix);
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE research_reviews
                 SET operation_stage = NULL
                 WHERE review_id = ?1",
                [&review_id],
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
async fn owned_kill_timeout_keeps_research_owner_open_without_progression() {
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
    let reservations_before: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(
        TerminationManager::new(&harness.db, harness.pueue.clone())
            .execute(request_id)
            .await
            .unwrap(),
        pueue_agent::termination::TerminationOutcome::PendingConfirmation
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE termination_requests SET grace_until = 0 WHERE request_id = ?1",
            [request_id],
        )
        .unwrap();
    assert_eq!(
        TerminationManager::new(&harness.db, harness.pueue.clone())
            .execute(request_id)
            .await
            .unwrap(),
        pueue_agent::termination::TerminationOutcome::TimedOut
    );

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
    assert_eq!(retained.operation_stage.as_deref(), Some("stop_requested"));
    assert_eq!(retained.termination_request_id, Some(request_id));
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
    let reservations_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successors, 0);
    assert_eq!(cycles, 0);
    assert_eq!(reservations_after, reservations_before);
    assert!(harness.pueue.add_calls().is_empty());
}

#[tokio::test]
async fn owned_kill_error_keeps_research_owner_open_without_progression() {
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
    let reservations_before: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    harness.pueue.set_kill_error("fixture kill failed");

    assert_eq!(
        TerminationManager::new(&harness.db, harness.pueue.clone())
            .execute(request_id)
            .await
            .unwrap(),
        pueue_agent::termination::TerminationOutcome::Failed
    );

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
    let reservations_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successors, 0);
    assert_eq!(cycles, 0);
    assert_eq!(reservations_after, reservations_before);
    assert!(harness.pueue.add_calls().is_empty());
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
    assert_eq!(attached.context_digest, review.context_digest);
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
async fn confirmed_research_handoff_feeds_claimed_decision_context_v2_without_submission() {
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
    harness.set_task(task.clone());
    Reconciler::new(&harness.db, harness.pueue.clone())
        .with_campaign_limits(CampaignLimits::default())
        .run_once_at(500)
        .await
        .unwrap();

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(
            &harness.db,
            &harness.pueue,
            &disabled_policy,
            600,
            1,
        )
        .await
        .unwrap(),
        1
    );
    let attached = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(attached.state, "completed");
    assert_eq!(attached.context_digest, review.context_digest);
    let connection = harness.db.connect().unwrap();
    let cycle_id: String = connection
        .query_row(
            "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    let decision_event_id: i64 = connection
        .query_row(
            "SELECT event_id FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);

    let claimed_event = EventRepository::new(&harness.db)
        .claim_by_id(&harness.project_id, decision_event_id, 601)
        .unwrap()
        .expect("attached decision event must be claimable");
    assert_eq!(claimed_event.event_id, decision_event_id);
    let reservation = DecisionRepository::new(&harness.db)
        .reserve_next_attempt(&harness.project_id, &cycle_id, 602)
        .unwrap()
        .expect("terminal cycle must reserve one decision attempt");
    let project = ProjectRepository::new(&harness.db)
        .find_by_id(&harness.project_id)
        .unwrap()
        .unwrap();
    let root_anchor = harness
        .policy
        .project_root_anchor(&project.root_path)
        .unwrap();
    let task_signature = managed_task_run_signature(&task).unwrap();
    let projection = DecisionPueueTaskProjection {
        task_id: task.id,
        task_signature,
        group: task.group.clone(),
        state: task.state.clone(),
        enqueued_at: task
            .enqueued_at
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok()),
        started_at: task
            .started_at
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok()),
        ended_at: task
            .ended_at
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok()),
        exit_code: Some(0),
    };
    let context = DecisionEvidenceBuilder::new(&harness.db)
        .build(&DecisionEvidenceRequest {
            reservation: &reservation,
            root_anchor: &root_anchor,
            pueue_tasks: &[projection],
            observed_at: 603,
        })
        .unwrap();
    DecisionRepository::new(&harness.db)
        .store_evidence(&reservation, &context.json, &context.digest, 604)
        .unwrap();

    let (state, schema_version, stored_json, stored_digest): (String, i64, String, String) =
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state, context_schema_version, context_json, context_digest
                 FROM decision_attempts
                 WHERE cycle_id = ?1 AND attempt_number = ?2",
                rusqlite::params![reservation.cycle_id, reservation.attempt_number],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    assert_eq!(state, "evidence_ready");
    assert_eq!(schema_version, 2);
    assert_eq!(stored_json, context.json);
    assert_eq!(stored_digest, context.digest);
    let stored: serde_json::Value = serde_json::from_str(&stored_json).unwrap();
    assert_eq!(stored["schema_version"], 2);
    assert_eq!(stored["research"]["review_id"], review.review_id);
    assert_eq!(
        stored["research"]["reason"],
        "the baseline should be pruned"
    );
    assert_eq!(
        stored["research"]["next_direction"],
        "try a smaller learning rate"
    );
    assert!(stored["research"]["recent_advice"]
        .as_array()
        .unwrap()
        .iter()
        .any(|advice| {
            advice["evidence_ref"] == format!("research:{}:note", review.review_id)
                && advice["notes"] == "save this bounded advice"
        }));

    let connection = harness.db.connect().unwrap();
    let event_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    let attempt_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = ?1",
            [&reservation.cycle_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 1);
    assert_eq!(attempt_count, 1);
    assert!(harness.pueue.add_calls().is_empty());
}

#[tokio::test]
async fn stored_research_wait_schedules_bounded_wake_without_releasing_attachment() {
    let harness = Harness::new().await;
    let (review, reservation, cycle_id, event_id, _task) =
        harness.prepare_stored_decision_attempt().await;
    let wait_json = json!({
        "schema_version": 1,
        "decision": "wait",
        "proposal": null,
        "reason": "wait for the next bounded observation",
        "requested_wait_minutes": 1,
        "expected_evidence": ["next checkpoint"],
        "evidence_ref": null,
    })
    .to_string();
    harness.persist_decision_outcome(
        &reservation,
        event_id,
        &wait_json,
        "wait",
        700,
    );

    let before = harness.db.connect().unwrap().query_row(
        "SELECT
             (SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM experiment_metrics m
                JOIN experiments e ON e.experiment_id = m.experiment_id
              WHERE e.campaign_id = ?1)",
        [&harness.campaign_id],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
    ).unwrap();

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, CampaignLimits::default())
        .apply_ready(800, 1)
        .await
        .unwrap();
    assert_eq!(report.waits_scheduled, 1);
    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(report.degraded, 0);

    let cycle = DecisionRepository::new(&harness.db)
        .find_cycle_for_source(&harness.campaign_id, &harness.experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(cycle.cycle_id, cycle_id);
    assert_eq!(cycle.state, pueue_agent::models::DecisionCycleState::Waiting);
    assert_eq!(cycle.next_wake_at, Some(860));
    let attached = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(attached.state, "completed");
    assert!(attached.successor_experiment_id.is_none());
    let persisted_cycle_id: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted_cycle_id, cycle_id);

    let after = harness.db.connect().unwrap().query_row(
        "SELECT
             (SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM experiment_metrics m
                JOIN experiments e ON e.experiment_id = m.experiment_id
              WHERE e.campaign_id = ?1)",
        [&harness.campaign_id],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
    ).unwrap();
    assert_eq!(after, before);

    let attempt_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = ?1",
            [&cycle_id],
            |row| row.get(0),
        )
        .unwrap();
    let event_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_count, 1);
    assert_eq!(event_count, 1);
    assert!(harness.pueue.add_calls().is_empty());

    let replay = DecisionCoordinator::new(&harness.db, &harness.pueue, CampaignLimits::default())
        .apply_ready(801, 1)
        .await
        .unwrap();
    assert_eq!(replay.waits_scheduled, 0);
    assert_eq!(replay.proposals_applied, 0);
    assert_eq!(replay.deferred, 0);
    assert_eq!(replay.degraded, 0);
    let replay_cycle = DecisionRepository::new(&harness.db)
        .find_cycle_for_source(&harness.campaign_id, &harness.experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(replay_cycle, cycle);
    let replay_attempt_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = ?1",
            [&cycle_id],
            |row| row.get(0),
        )
        .unwrap();
    let replay_event_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(replay_attempt_count, 1);
    assert_eq!(replay_event_count, 1);
    assert_eq!(harness.pueue.add_calls().len(), 0);
}

#[tokio::test]
async fn rejected_persisted_proposal_keeps_research_attachment_without_side_effects() {
    let harness = Harness::new().await;
    let (review, reservation, cycle_id, event_id, _task) =
        harness.prepare_stored_decision_attempt().await;
    let rejected_json = json!({
        "schema_version": 1,
        "decision": "proposal",
        "proposal": {
            "kind": "experiment",
            "hypothesis": "proposal with the wrong terminal source",
            "source_experiment_id": "foreign-source-experiment",
            "argv": ["python", "train.py", "--lr", "0.001"],
            "working_directory": ".",
            "expected_evidence": ["validation loss"]
        },
        "reason": null,
        "requested_wait_minutes": null,
        "expected_evidence": null,
        "evidence_ref": null,
    })
    .to_string();
    harness.persist_decision_outcome(
        &reservation,
        event_id,
        &rejected_json,
        "proposal",
        700,
    );

    let before = harness.db.connect().unwrap().query_row(
        "SELECT
             (SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM experiment_metrics m
                JOIN experiments e ON e.experiment_id = m.experiment_id
              WHERE e.campaign_id = ?1),
             (SELECT COUNT(*) FROM proposals WHERE campaign_id = ?1)",
        [&harness.campaign_id],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    ).unwrap();

    let report = DecisionCoordinator::new(&harness.db, &harness.pueue, CampaignLimits::default())
        .apply_ready(800, 1)
        .await
        .unwrap();
    assert_eq!(report.waits_scheduled, 0);
    assert_eq!(report.proposals_applied, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(report.degraded, 0);

    let cycle = DecisionRepository::new(&harness.db)
        .find_cycle_for_source(&harness.campaign_id, &harness.experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(cycle.cycle_id, cycle_id);
    assert_eq!(cycle.state, pueue_agent::models::DecisionCycleState::Pending);
    assert!(cycle.next_wake_at.is_none());
    assert_eq!(cycle.last_failure_code.as_deref(), Some("decision_rejected"));
    let attached = ResearchRepository::new(&harness.db)
        .find(&review.review_id)
        .unwrap();
    assert_eq!(attached.state, "completed");
    assert!(attached.successor_experiment_id.is_none());
    let persisted_cycle_id: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted_cycle_id, cycle_id);

    let after = harness.db.connect().unwrap().query_row(
        "SELECT
             (SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1),
             (SELECT COUNT(*) FROM experiment_metrics m
                JOIN experiments e ON e.experiment_id = m.experiment_id
              WHERE e.campaign_id = ?1),
             (SELECT COUNT(*) FROM proposals WHERE campaign_id = ?1)",
        [&harness.campaign_id],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    ).unwrap();
    assert_eq!(after, before);

    let attempt_state: (String, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT state, failure_code FROM decision_attempts
             WHERE cycle_id = ?1 AND attempt_number = 1",
            [&cycle_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(attempt_state, ("failed".to_owned(), Some("decision_rejected".to_owned())));
    let event_state: (String, Option<i64>, Option<i64>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, completed_at, lease_until, last_error FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(event_state.0, "pending");
    assert!(event_state.1.is_none());
    assert!(event_state.2.is_none());
    assert!(event_state.3.is_none());
    assert!(harness.pueue.add_calls().is_empty());

    let replay = DecisionCoordinator::new(&harness.db, &harness.pueue, CampaignLimits::default())
        .apply_ready(801, 1)
        .await
        .unwrap();
    assert_eq!(replay.waits_scheduled, 0);
    assert_eq!(replay.proposals_applied, 0);
    assert_eq!(replay.deferred, 0);
    assert_eq!(replay.degraded, 0);
    let replay_attempt_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = ?1",
            [&cycle_id],
            |row| row.get(0),
        )
        .unwrap();
    let replay_event_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND campaign_id = ?2 AND experiment_id = ?3
               AND kind = 'campaign_decision'",
            rusqlite::params![harness.project_id, harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(replay_attempt_count, 1);
    assert_eq!(replay_event_count, 1);
    assert_eq!(harness.pueue.add_calls().len(), 0);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn native_scheduler_runs_one_confirmed_research_decision_and_replays_without_second_run() {
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
    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &disabled_policy, 600, 1)
            .await
            .unwrap(),
        1
    );
    let runs_before: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1",
            [&harness.project_id],
            |row| row.get(0),
        )
        .unwrap();
    let policy = harness.native_decision_policy();
    let runner = AgentRunner::new(
        AgentRunnerConfig::production().with_codex_capabilities(CodexCapabilities::all()),
        policy,
    );
    let mut scheduler = Scheduler::new(
        harness.db.clone(),
        runner,
        SchedulerConfig {
            now: 700,
            lease_seconds: 60,
            claim_limit: 100,
        },
    );
    let mut report = scheduler.tick().await.unwrap();
    assert_eq!(report.started.len(), 1);
    assert_eq!(report.started[0].mode, "campaign_decision");
    let mut started = report.started.pop().unwrap();
    let started_run_id = started.run_id;
    assert_eq!(
        started.handle.wait(&harness.db, 701).await.unwrap(),
        AgentRunStatus::Completed
    );

    let cycle_id: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT decision_cycle_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    let attempt_snapshot: (
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    ) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT state, agent_run_id, decision_kind, decision_json, decision_digest,
                    context_schema_version, context_json, context_digest
             FROM decision_attempts WHERE cycle_id = ?1 AND attempt_number = 1",
            [&cycle_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(attempt_snapshot.0, "decided");
    assert_eq!(attempt_snapshot.1, Some(started_run_id));
    assert_eq!(attempt_snapshot.2.as_deref(), Some("wait"));
    let expected_decision =
        r#"{"schema_version":1,"decision":"wait","proposal":null,"reason":"native fixture wait","requested_wait_minutes":1,"expected_evidence":[],"evidence_ref":null}"#;
    assert_eq!(attempt_snapshot.3.as_deref(), Some(expected_decision));
    let validated = parse_and_validate_decision(
        expected_decision.as_bytes(),
        "objective-digest-research-actions",
        CampaignLimits::default(),
    )
    .unwrap();
    assert_eq!(
        attempt_snapshot.4.as_deref(),
        Some(validated.canonical_digest())
    );
    assert_eq!(attempt_snapshot.5, Some(2));
    let context_json = attempt_snapshot.6.as_deref().unwrap();
    let context_digest = attempt_snapshot.7.as_deref().unwrap();
    assert_eq!(format!("{:x}", Sha256::digest(context_json.as_bytes())), context_digest);
    let stored: serde_json::Value = serde_json::from_str(context_json).unwrap();
    assert_eq!(stored["schema_version"], 2);
    assert_eq!(stored["research"]["review_id"], review.review_id);
    assert_eq!(stored["source_experiment"]["experiment_id"], harness.experiment_id);

    let attempt_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = ?1",
            [&cycle_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_count, 1);
    let event_snapshot: (String, Option<i64>, i64, Option<i64>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, completed_at, attempts, lease_until, last_error
             FROM events
             WHERE campaign_id = ?1 AND experiment_id = ?2 AND kind = 'campaign_decision'",
            rusqlite::params![harness.campaign_id, harness.experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(event_snapshot.0, "completed");
    assert!(event_snapshot.1.is_some());
    assert_eq!(event_snapshot.2, 1);
    assert!(event_snapshot.3.is_none());
    assert!(event_snapshot.4.is_none());
    let event_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE campaign_id = ?1 AND experiment_id = ?2 AND kind = 'campaign_decision'",
            rusqlite::params![harness.campaign_id, harness.experiment_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(event_count, 1);
    let run_snapshot: (String, Option<i64>, Option<i64>, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, finished_at, exit_code, launch_gate_state
             FROM agent_runs WHERE run_id = ?1",
            [started_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(run_snapshot.0, "completed");
    assert!(run_snapshot.1.is_some());
    assert_eq!(run_snapshot.2, Some(0));
    assert_eq!(run_snapshot.3, "released");
    let private_temp_path = harness.policy.project_roots[0]
        .join(".pueue-agent/tmp")
        .join(started_run_id.to_string());
    assert!(private_temp_path.is_dir());
    assert!(fs::read_dir(&private_temp_path).unwrap().next().is_none());
    assert_eq!(
        AgentRunRepository::new(&harness.db)
            .count_by_project(&harness.project_id)
            .unwrap() as i64,
        runs_before + 1
    );
    assert!(harness.pueue.add_calls().is_empty());

    let replay = scheduler.tick().await.unwrap();
    assert!(replay.started.is_empty());
    let replay_attempt_snapshot: (
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<String>,
    ) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT state, agent_run_id, decision_kind, decision_json, decision_digest,
                    context_schema_version, context_json, context_digest
             FROM decision_attempts WHERE cycle_id = ?1 AND attempt_number = 1",
            [&cycle_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .unwrap();
    let replay_event_snapshot: (String, Option<i64>, i64, Option<i64>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, completed_at, attempts, lease_until, last_error
             FROM events
             WHERE campaign_id = ?1 AND experiment_id = ?2 AND kind = 'campaign_decision'",
            rusqlite::params![harness.campaign_id, harness.experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    let replay_run_snapshot: (String, Option<i64>, Option<i64>, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, finished_at, exit_code, launch_gate_state
             FROM agent_runs WHERE run_id = ?1",
            [started_run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(replay_attempt_snapshot, attempt_snapshot);
    assert_eq!(replay_event_snapshot, event_snapshot);
    assert_eq!(replay_run_snapshot, run_snapshot);
    assert_eq!(
        AgentRunRepository::new(&harness.db)
            .count_by_project(&harness.project_id)
            .unwrap() as i64,
        runs_before + 1
    );
    assert!(harness.pueue.add_calls().is_empty());
}

#[tokio::test]
async fn pause_after_sent_keeps_confirmed_research_handoff_durable() {
    let harness = Harness::new().await;
    let review = harness.prepare_sent_handoff().await;
    let request_id = review.termination_request_id.unwrap();
    let connection = harness.db.connect().unwrap();
    let experiments_before: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    let reservations_before: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    drop(connection);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE projects SET paused = 1 WHERE project_id = ?1",
            [&harness.project_id],
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
        TerminationRequestRepository::new(&harness.db)
            .find_by_id(request_id)
            .unwrap()
            .unwrap()
            .status,
        TerminationRequestStatus::Confirmed
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
    assert!(completed.successor_experiment_id.is_none());
    let connection = harness.db.connect().unwrap();
    let successors: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE resume_of_experiment_id = ?1",
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
    let experiments_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    let reservations_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(successors, 0);
    assert_eq!(event_count, 1);
    assert_eq!(experiments_after, experiments_before);
    assert_eq!(reservations_after, reservations_before);
    assert!(harness.pueue.add_calls().is_empty());
}

#[tokio::test]
async fn budget_spent_during_kill_keeps_confirmed_research_handoff_without_replacement() {
    let harness = Harness::new().await;
    let review = harness.prepare_sent_handoff().await;
    let request_id = review.termination_request_id.unwrap();
    let budget = CampaignRepository::new(&harness.db)
        .reserve_agent_run(
            &harness.campaign_id,
            "research-action-budget-spent-during-kill",
            &CampaignLimits::default(),
            450,
        )
        .unwrap();
    assert!(matches!(
        budget,
        pueue_agent::db::AgentDecisionReservation::Reserved(_)
    ));
    let connection = harness.db.connect().unwrap();
    let experiments_before: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    let reservations_before: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    let live_agent_run_reservations: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = 'agent_run'
               AND status IN ('reserved', 'consumed')
               AND window_started_at <= ?2 AND window_ends_at > ?2",
            rusqlite::params![harness.campaign_id, 600],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(live_agent_run_reservations, 2);
    drop(connection);

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
        TerminationRequestRepository::new(&harness.db)
            .find_by_id(request_id)
            .unwrap()
            .unwrap()
            .status,
        TerminationRequestStatus::Confirmed
    );

    let mut disabled_policy = (*harness.policy).clone();
    disabled_policy.campaign_limits.research_interval_minutes = 0;
    disabled_policy.campaign_limits.max_agent_runs_per_hour = 2;
    assert_eq!(
        disabled_policy.campaign_limits.max_agent_runs_per_hour,
        live_agent_run_reservations as u32
    );
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
    assert!(completed.successor_experiment_id.is_none());
    let connection = harness.db.connect().unwrap();
    let experiments_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM experiments WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    let reservations_after: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = ?1",
            [&harness.campaign_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(experiments_after, experiments_before);
    assert_eq!(reservations_after, reservations_before);
    assert!(harness.pueue.add_calls().is_empty());
}

#[tokio::test]
async fn bounded_ready_action_scan_reaches_newer_owner_after_deferred_owner() {
    let harness = Harness::new().await;
    harness.set_answer_action("continue");
    let deferred_review_id = harness.insert_deferred_ready_rotation_review("ready-single");

    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 400, 1)
            .await
            .unwrap(),
        1
    );
    let deferred = ResearchRepository::new(&harness.db)
        .find(&deferred_review_id)
        .unwrap();
    assert_eq!(deferred.state, "ready");
    assert!(deferred.operation_stage.is_none());
    let completed = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(completed.state, "completed");
}

#[tokio::test]
async fn rotated_ready_action_scan_reaches_owner_after_full_deferred_prefix() {
    let harness = Harness::new().await;
    harness.set_answer_action("continue");
    let prefix_ids = (0..32)
        .map(|index| {
            harness.insert_deferred_ready_rotation_review(&format!("ready-prefix-{index:02}"))
        })
        .collect::<Vec<_>>();

    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 600, 1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        advance_research_actions(&harness.db, &harness.pueue, &harness.policy, 601, 1)
            .await
            .unwrap(),
        1
    );

    for review_id in prefix_ids {
        let prefix_review = ResearchRepository::new(&harness.db)
            .find(&review_id)
            .unwrap();
        assert_eq!(prefix_review.state, "ready");
        assert!(prefix_review.operation_stage.is_none());
    }
    let completed = ResearchRepository::new(&harness.db)
        .recent(&harness.campaign_id, 1)
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(completed.state, "completed");
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
