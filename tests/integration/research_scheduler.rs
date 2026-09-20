//! Task 4 scheduler contract tests.

use std::{collections::{BTreeMap, BTreeSet}, fs, path::Path};

#[cfg(unix)]
use std::{os::unix::fs::PermissionsExt, process::Command};

use pueue_agent::{
    db::{
        AgentDecisionReservation, AgentRunRepository, CampaignRepository, Db, EventRepository,
        ExperimentRepository,
        ProjectRepository, ResearchRepository, StartCampaignRequest, TaskObservationRepository,
    },
    execution_policy::CampaignLimits,
    models::{
        AgentContextMode, AgentRunStatus, EventStatus, ExecutionProjection, NewAgentRun,
        NewProject, NewTaskObservation, ProposalKind,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, task_signature, Reconciler},
    research::recover_research,
    research_evidence::build_research_evidence,
    retry::RetryPolicy,
    state::ObjectiveSnapshot,
    AppError,
};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use pueue_agent::db::next_research_due;

#[derive(Clone)]
struct ResearchSchedulerPueue {
    tasks: Vec<PueueTask>,
}

impl ResearchSchedulerPueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self { tasks }
    }
}

#[async_trait]
impl PueueApi for ResearchSchedulerPueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(self.tasks.clone())
    }

    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        panic!("research identity fixture must not submit a Pueue task")
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("research identity fixture must not kill a Pueue task")
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("research identity fixture must not remove a Pueue task")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("research identity fixture must not create a Pueue group")
    }
}

#[test]
fn zero_interval_disables_research() {
    assert_eq!(next_research_due(1_000, 0).expect("zero interval is valid"), None);
}

#[test]
fn due_time_uses_minutes_without_drift() {
    assert_eq!(next_research_due(1_000, 30).expect("interval is valid"), Some(2_800));
}

#[test]
fn due_time_reports_timestamp_overflow() {
    let error = next_research_due(i64::MAX, 1).expect_err("overflow must be rejected");
    assert!(error.to_string().contains("research.next_due_at"));
}

#[tokio::test]
async fn reconciler_live_identity_feeds_research_schedule_and_evidence() {
    let (fixture, task, managed_signature) = reconciled_identity_fixture();
    let live_signature = task_signature(&task);
    assert_ne!(managed_signature, live_signature);
    assert_eq!(
        TaskObservationRepository::new(&fixture.db)
            .find("research-scheduler-project", &managed_signature)
            .expect("read pre-reconcile managed observation"),
        None,
        "the fixture must not install a synthetic managed observation alias"
    );

    let report = Reconciler::new(
        &fixture.db,
        ResearchSchedulerPueue::with_tasks(vec![task.clone()]),
    )
    .run_once_at(1_001)
    .await
    .expect("reconcile live task");
    assert_eq!(report.observed_task_count, 1);
    let live_observation = TaskObservationRepository::new(&fixture.db)
        .find("research-scheduler-project", &live_signature)
        .expect("read reconciled live observation")
        .expect("reconciler must persist the live observation");
    assert_eq!(live_observation.pueue_task_id, task.id);
    assert_eq!(live_observation.started_at, Some(1_000));
    assert!(
        TaskObservationRepository::new(&fixture.db)
            .find("research-scheduler-project", &managed_signature)
            .expect("read managed observation after reconcile")
            .is_none(),
        "only the reconciler-produced live identity may be present"
    );

    let repository = ResearchRepository::new(&fixture.db);
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 1_001, 1)
            .expect("schedule reconciled running campaign"),
        1
    );
    let review = repository
        .claim_due_campaigns(2_800, 1)
        .expect("claim reconciled campaign")
        .into_iter()
        .next()
        .expect("reconciled campaign must produce a due review");
    let evidence = build_research_evidence(&fixture.db, &review, 2_800)
        .expect("research evidence must use the reconciled live identity");
    assert!(!evidence.json.is_empty());
    assert_eq!(
        format!("{:x}", Sha256::digest(evidence.json.as_bytes())),
        evidence.digest
    );
}

#[tokio::test]
async fn reconciler_does_not_reuse_numeric_identity_after_live_task_changes() {
    let (fixture, original_task, managed_signature) = reconciled_identity_fixture();
    let original_live_signature = task_signature(&original_task);
    Reconciler::new(
        &fixture.db,
        ResearchSchedulerPueue::with_tasks(vec![original_task.clone()]),
    )
    .run_once_at(1_001)
    .await
    .expect("reconcile original live task");
    ResearchRepository::new(&fixture.db)
        .schedule_running(&fixture.campaign_id, 1_000, 30, 2_799)
        .expect("establish a due research schedule from the valid snapshot");

    let mut changed_task = original_task.clone();
    changed_task.command = "python changed.py".to_owned();
    changed_task.enqueued_at = Some("901".to_owned());
    changed_task.started_at = Some("1001".to_owned());
    let changed_live_signature = task_signature(&changed_task);
    assert_ne!(original_live_signature, changed_live_signature);
    assert_ne!(
        managed_signature,
        managed_task_run_signature(&changed_task).expect("changed managed task identity")
    );
    Reconciler::new(
        &fixture.db,
        ResearchSchedulerPueue::with_tasks(vec![changed_task]),
    )
    .run_once_at(1_002)
    .await
    .expect("reconcile changed live task");

    let observations = TaskObservationRepository::new(&fixture.db);
    assert!(
        observations
            .find("research-scheduler-project", &original_live_signature)
            .expect("read original live observation")
            .is_some(),
        "the original observation remains historical evidence"
    );
    assert!(
        observations
            .find("research-scheduler-project", &changed_live_signature)
            .expect("read changed live observation")
            .is_some(),
        "the changed live identity must be observed separately"
    );

    let repository = ResearchRepository::new(&fixture.db);
    assert!(
        repository
            .claim_due(
                &fixture.campaign_id,
                &fixture.experiment_id,
                &managed_signature,
                2_800,
            )
            .expect("claim after live identity change")
            .is_none(),
        "the due claim must revalidate the changed live identity"
    );
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 1_002, 1)
            .expect("schedule after live identity change"),
        0,
        "a changed task with the same numeric id/group must not revive the old campaign"
    );
    assert!(
        repository
            .claim_due_campaigns(2_800, 1)
            .expect("claim due campaign after live identity change")
            .is_empty(),
        "the stale campaign must not consume a research claim"
    );
}

#[tokio::test]
async fn reconciler_skips_changed_prefix_for_later_live_campaign() {
    let (fixture, original_task, original_managed_signature) = reconciled_identity_fixture();
    let original_live_signature = task_signature(&original_task);
    Reconciler::new(
        &fixture.db,
        ResearchSchedulerPueue::with_tasks(vec![original_task.clone()]),
    )
    .run_once_at(1_001)
    .await
    .expect("reconcile original live task");

    let later_label = "later-reconciled";
    let later_task = PueueTask {
        id: 42,
        group: format!("research-scheduler-{later_label}-group"),
        command: "python later.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("901".to_owned()),
        started_at: Some("1001".to_owned()),
        ended_at: None,
        result: None,
    };
    let (later_campaign_id, later_experiment_id, later_managed_signature) =
        add_reconciled_campaign(&fixture, &later_task, later_label, 900);
    let mut changed_task = original_task;
    changed_task.command = "python changed.py".to_owned();
    changed_task.enqueued_at = Some("902".to_owned());
    changed_task.started_at = Some("1001".to_owned());
    let changed_live_signature = task_signature(&changed_task);
    assert_ne!(original_live_signature, changed_live_signature);
    assert_ne!(
        original_managed_signature,
        managed_task_run_signature(&changed_task).expect("changed managed task identity")
    );

    Reconciler::new(
        &fixture.db,
        ResearchSchedulerPueue::with_tasks(vec![changed_task, later_task.clone()]),
    )
    .run_once_at(1_002)
    .await
    .expect("reconcile changed prefix and later live task");

    let repository = ResearchRepository::new(&fixture.db);
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 1_002, 1)
            .expect("schedule later valid campaign behind changed prefix"),
        1,
        "an invalid oldest identity must not consume the bounded scheduler prefix"
    );
    assert_eq!(
        repository
            .state(&later_campaign_id)
            .expect("read later research state")
            .next_due_at,
        Some(2_801)
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("read changed-prefix research state")
            .next_due_at,
        None,
        "the changed prefix must not be scheduled by the live scheduler"
    );
    repository
        .schedule_running(&fixture.campaign_id, 1_000, 30, 2_801)
        .expect("establish a due state for the changed prefix claim check");
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("read due changed-prefix research state")
            .next_due_at,
        Some(2_800)
    );
    assert_eq!(
        repository
            .claim_due_campaigns(2_801, 1)
            .expect("claim later valid campaign behind changed prefix")
            .into_iter()
            .next()
            .map(|review| review.experiment_id),
        Some(later_experiment_id)
    );
    assert!(
        TaskObservationRepository::new(&fixture.db)
            .find("research-scheduler-project", &later_managed_signature)
            .expect("read later managed observation alias")
            .is_none(),
        "the Reconciler must continue to publish only its raw live identity"
    );
}

#[tokio::test]
async fn equal_latest_lifecycle_snapshots_fail_closed_for_claim_and_evidence() {
    let (fixture, running_task, managed_signature) = reconciled_identity_fixture();
    let mut terminal_task = running_task.clone();
    terminal_task.state = "Done".to_owned();
    terminal_task.ended_at = Some("1100".to_owned());
    let observations = TaskObservationRepository::new(&fixture.db);
    for task in [&running_task, &terminal_task] {
        observations
            .upsert(&NewTaskObservation::new(
                "research-scheduler-project",
                &task_signature(task),
                task.id,
                &task.group,
                vec![task.command.clone()],
                &task.state,
                task.enqueued_at.as_deref().and_then(|value| value.parse().ok()),
                task.started_at.as_deref().and_then(|value| value.parse().ok()),
                task.ended_at.as_deref().and_then(|value| value.parse().ok()),
                None,
                1_001,
            ))
            .expect("tied lifecycle observation");
    }
    let repository = ResearchRepository::new(&fixture.db);
    repository
        .schedule_running(&fixture.campaign_id, 1_000, 30, 2_799)
        .expect("establish due state for tied observations");
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 1_001, 1)
            .expect("schedule tied lifecycle observations"),
        0,
        "a running/terminal tie must not enter the bounded schedule"
    );
    assert!(
        repository
            .claim_due(
                &fixture.campaign_id,
                &fixture.experiment_id,
                &managed_signature,
                2_800,
            )
            .expect("claim tied lifecycle observations")
            .is_none(),
        "a tied lifecycle snapshot must not be claimable"
    );

    let (evidence_fixture, running_task, managed_signature) = reconciled_identity_fixture();
    let evidence_observations = TaskObservationRepository::new(&evidence_fixture.db);
    evidence_observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &task_signature(&running_task),
            running_task.id,
            &running_task.group,
            vec![running_task.command.clone()],
            &running_task.state,
            Some(900),
            Some(1_000),
            None,
            None,
            1_001,
        ))
        .expect("running evidence observation");
    let evidence_repository = ResearchRepository::new(&evidence_fixture.db);
    evidence_repository
        .schedule_running(&evidence_fixture.campaign_id, 1_000, 30, 2_799)
        .expect("schedule earlier evidence review");
    let review = evidence_repository
        .claim_due(
            &evidence_fixture.campaign_id,
            &evidence_fixture.experiment_id,
            &managed_signature,
            2_800,
        )
        .expect("claim earlier evidence review")
        .expect("earlier evidence review must be claimable");
    let mut terminal_task = running_task;
    terminal_task.state = "Done".to_owned();
    terminal_task.ended_at = Some("1100".to_owned());
    evidence_observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &task_signature(&terminal_task),
            terminal_task.id,
            &terminal_task.group,
            vec![terminal_task.command.clone()],
            &terminal_task.state,
            Some(900),
            Some(1_000),
            Some(1_100),
            None,
            1_001,
        ))
        .expect("terminal tie for earlier evidence review");
    let error = build_research_evidence(&evidence_fixture.db, &review, 2_800)
        .expect_err("evidence must reject tied current lifecycle observations");
    assert!(error.to_string().contains("ambiguous current task observations"));
}

struct SchedulerFixture {
    _temp: TempDir,
    db: Db,
    campaign_id: String,
    experiment_id: String,
    task_signature: String,
    observation_signature: String,
}

fn fixture() -> SchedulerFixture {
    fixture_with_task(&scheduler_task(), true)
}

fn fixture_with_task(task: &PueueTask, seed_observation: bool) -> SchedulerFixture {
    let temp = tempfile::tempdir().expect("fixture directory");
    let root = temp.path().join("project");
    fs::create_dir_all(&root).expect("project root");
    let config_path = root.join("config.toml");
    fs::write(&config_path, "fixture").expect("project config");
    let db = Db::open(&temp.path().join("state.sqlite3")).expect("database");
    let managed_signature = managed_task_run_signature(task).expect("managed task identity");
    let observation_signature = task_signature(task);
    let project_id = "research-scheduler-project";
    let campaign_id = "research-scheduler-campaign";
    let experiment_id = "research-scheduler-experiment";
    let submission_id = "research-scheduler-submission";
    let proposal_id = "research-scheduler-proposal";
    ProjectRepository::new(&db)
        .register(&NewProject::new(
            project_id,
            root,
            "research-scheduler-group",
            config_path,
            900,
        ))
        .expect("project registration");
    let objective = ObjectiveSnapshot {
        text: "Improve the bounded objective".to_owned(),
        digest: "research-scheduler-objective".to_owned(),
    };
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let baseline = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "measure the baseline".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: vec!["loss".to_owned()],
        },
        &objective.digest,
    )
    .expect("baseline proposal");
    CampaignRepository::new(&db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id,
                project_id,
                objective: &objective,
                initial_argv: &argv,
                baseline: &baseline,
                submission_id,
                experiment_id,
                proposal_id,
                metadata: &serde_json::json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: 900,
            },
            &CampaignLimits::default(),
        )
        .expect("campaign creation");
    ExperimentRepository::new(&db)
        .mark_submitting(experiment_id, 901)
        .expect("submission intent");
    ExperimentRepository::new(&db)
        .mark_accepted(experiment_id, task.id, &managed_signature, 902)
        .expect("accepted experiment");
    if seed_observation {
        TaskObservationRepository::new(&db)
            .upsert(&NewTaskObservation::new(
                project_id,
                &observation_signature,
                task.id,
                &task.group,
                vec![task.command.clone()],
                &task.state,
                task.enqueued_at.as_deref().and_then(|value| value.parse().ok()),
                task.started_at.as_deref().and_then(|value| value.parse().ok()),
                task.ended_at.as_deref().and_then(|value| value.parse().ok()),
                task.result
                    .as_ref()
                    .map(serde_json::to_string)
                    .transpose()
                    .expect("serialize fixture task result"),
                1_001,
            ))
            .expect("running observation");
    }
    ResearchRepository::new(&db)
        .ensure_campaign(campaign_id)
        .expect("research state");
    SchedulerFixture {
        _temp: temp,
        db,
        campaign_id: campaign_id.to_owned(),
        experiment_id: experiment_id.to_owned(),
        task_signature: managed_signature,
        observation_signature,
    }
}

fn scheduler_task() -> PueueTask {
    PueueTask {
        id: 41,
        group: "research-scheduler-group".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("900".to_owned()),
        started_at: Some("1000".to_owned()),
        ended_at: None,
        result: None,
    }
}

fn reconciled_identity_fixture() -> (SchedulerFixture, PueueTask, String) {
    let task = scheduler_task();
    let managed_signature = managed_task_run_signature(&task).expect("managed task identity");
    let fixture = fixture_with_task(&task, false);
    (fixture, task, managed_signature)
}

fn add_reconciled_campaign(
    fixture: &SchedulerFixture,
    task: &PueueTask,
    label: &str,
    now: i64,
) -> (String, String, String) {
    let root = fixture._temp.path().join(label).join("project");
    fs::create_dir_all(&root).expect("later project root");
    let config_path = root.join("config.toml");
    fs::write(&config_path, "fixture").expect("later project config");
    let project_id = format!("research-scheduler-{label}-project");
    let pueue_group = format!("research-scheduler-{label}-group");
    ProjectRepository::new(&fixture.db)
        .register(&NewProject::new(
            &project_id,
            root,
            &pueue_group,
            config_path,
            now,
        ))
        .expect("later project registration");
    let campaign_id = format!("research-scheduler-{label}-campaign");
    let experiment_id = format!("research-scheduler-{label}-experiment");
    let submission_id = format!("research-scheduler-{label}-submission");
    let proposal_id = format!("research-scheduler-{label}-proposal");
    let objective = ObjectiveSnapshot {
        text: format!("Improve the bounded {label} objective"),
        digest: format!("research-scheduler-{label}-objective"),
    };
    let argv = task
        .command
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: format!("measure the {label} baseline"),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: vec!["loss".to_owned()],
        },
        &objective.digest,
    )
    .expect("baseline proposal");
    CampaignRepository::new(&fixture.db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: &campaign_id,
                project_id: &project_id,
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: &submission_id,
                experiment_id: &experiment_id,
                proposal_id: &proposal_id,
                metadata: &serde_json::json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now,
            },
            &CampaignLimits::default(),
        )
        .expect("later campaign creation");
    ExperimentRepository::new(&fixture.db)
        .mark_submitting(&experiment_id, now + 1)
        .expect("later submission intent");
    let managed_signature = managed_task_run_signature(task).expect("later managed identity");
    ExperimentRepository::new(&fixture.db)
        .mark_accepted(&experiment_id, task.id, &managed_signature, now + 2)
        .expect("later accepted experiment");
    let observation_signature = task_signature(task);
    TaskObservationRepository::new(&fixture.db)
        .upsert(&NewTaskObservation::new(
            &project_id,
            &observation_signature,
            task.id,
            &pueue_group,
            vec![task.command.clone()],
            &task.state,
            task.enqueued_at.as_deref().and_then(|value| value.parse().ok()),
            task.started_at.as_deref().and_then(|value| value.parse().ok()),
            task.ended_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            now + 3,
        ))
        .expect("later running observation");
    ResearchRepository::new(&fixture.db)
        .ensure_campaign(&campaign_id)
        .expect("later research state");
    (campaign_id, experiment_id, managed_signature)
}

fn seed_active_research_outcome(
    fixture: &SchedulerFixture,
    state: &str,
) -> (String, i64, i64) {
    let repository = ResearchRepository::new(&fixture.db);
    repository
        .schedule_running(&fixture.campaign_id, 1_000, 30, 2_799)
        .expect("schedule research review");
    let review = repository
        .claim_due(
            &fixture.campaign_id,
            &fixture.experiment_id,
            &fixture.task_signature,
            2_800,
        )
        .expect("claim research review")
        .expect("research review must be claimable");
    let event_id = repository
        .event_id(&review.review_id)
        .expect("research event");
    EventRepository::new(&fixture.db)
        .claim_by_id("research-scheduler-project", event_id, 2_900)
        .expect("claim research event")
        .expect("research event must be pending");
    let execution = ExecutionProjection::new("campaign_research", "/bin/sh", "fixture")
        .expect("research execution projection");
    let run = AgentRunRepository::new(&fixture.db)
        .insert_with_events(
            &NewAgentRun::with_context(
                "research-scheduler-project",
                event_id,
                None,
                AgentRunStatus::Starting,
                2_901,
                fixture._temp.path().join("agent.log"),
                AgentContextMode::Fresh,
                None,
                Vec::new(),
            )
            .with_execution(execution),
            &[event_id],
        )
        .expect("insert active research run");
    let context_json = "{}";
    let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
    let session_id = "11111111-1111-4111-8111-111111111111";
    let response_json = serde_json::json!({
        "schema_version": 1,
        "review_id": review.review_id,
        "experiment_id": review.experiment_id,
        "context_digest": context_digest,
        "action": "continue",
        "reason": "recovered response",
        "evidence_refs": ["test"],
        "notes": "recovered",
        "next_direction": null,
        "checkpoint": null,
    })
    .to_string();
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE agent_runs
             SET status = 'running', pid = 4242, launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run.run_id],
        )
        .expect("mark research run active");
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE campaign_research
             SET session_id = ?1, session_generation = 0
             WHERE campaign_id = ?2",
            rusqlite::params![session_id, &fixture.campaign_id],
        )
        .expect("persist research session lineage");
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE research_reviews
             SET attempt = 1, state = ?1, agent_run_id = ?2,
                 context_json = ?3, context_digest = ?4, response_json = ?5,
                 failure_code = ?6, started_at = 2_901, finished_at = 2_902,
                 not_before = 2_902, updated_at = 2_902
             WHERE review_id = ?7",
            rusqlite::params![
                state,
                run.run_id,
                context_json,
                context_digest,
                (state == "ready").then_some(response_json),
                (state == "retry_wait").then_some("research_output_invalid"),
                &review.review_id,
            ],
        )
        .expect("persist research outcome boundary");
    seed_native_recovery_authority(fixture, &review.review_id, run.run_id, "pending");
    (review.review_id, run.run_id, event_id)
}

fn seed_native_recovery_authority(
    fixture: &SchedulerFixture,
    review_id: &str,
    run_id: i64,
    cleanup_phase: &str,
) {
    let authority = serde_json::json!({
        "version": 1,
        "run_id": run_id,
        "review_id": review_id,
        "campaign_id": fixture.campaign_id,
        "experiment_id": fixture.experiment_id,
        "attempt": 1,
        "session_generation": 0,
        "fresh_launch": true,
        "session_id": "11111111-1111-4111-8111-111111111111",
        "service_root_identity": {
            "device": 1,
            "inode": 2,
            "owner": 3,
            "mode": 448,
            "resolution": "fixture-root",
        },
        "temp_identity": {
            "device": 1,
            "inode": 4,
            "owner": 3,
            "mode": 448,
            "mount": [1, 2],
            "service_identity": {"device": 1, "inode": 5, "owner": 3, "mode": 448},
            "parent_identity": {"device": 1, "inode": 6, "owner": 3, "mode": 448},
        },
        "cleanup": {"phase": cleanup_phase},
    });
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET notes_json = ?1 WHERE review_id = ?2 AND agent_run_id = ?3",
            rusqlite::params![
                serde_json::json!({
                    "native_recovery": authority,
                    "session_binding": "confirmed",
                    "planned_session_id": "11111111-1111-4111-8111-111111111111",
                    "confirmed_session_id": "11111111-1111-4111-8111-111111111111",
                })
                .to_string(),
                review_id,
                run_id,
            ],
        )
        .expect("seed native recovery authority");
}

fn set_fixture_native_cleanup_phase(fixture: &SchedulerFixture, review_id: &str, phase: &str) {
    let notes_json: String = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
            [review_id],
            |row| row.get(0),
        )
        .expect("read fixture recovery authority");
    let mut notes: serde_json::Value = serde_json::from_str(&notes_json).expect("fixture notes");
    notes["native_recovery"]["cleanup"]["phase"] = serde_json::json!(phase);
    if phase == "complete" {
        notes["native_recovery"]["cleanup"]["completed_at"] = serde_json::json!(3_000);
    } else {
        notes["native_recovery"]["cleanup"]
            .as_object_mut()
            .expect("fixture cleanup object")
            .remove("completed_at");
    }
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET notes_json = ?1 WHERE review_id = ?2",
            rusqlite::params![notes.to_string(), review_id],
        )
        .expect("update fixture cleanup phase");
}

#[test]
fn controlled_clock_claims_once_at_the_due_boundary() {
    let fixture = fixture();
    let repository = ResearchRepository::new(&fixture.db);
    repository
        .schedule_running(&fixture.campaign_id, 1_000, 30, 2_799)
        .expect("schedule due review");
    assert!(repository
        .claim_due(
            &fixture.campaign_id,
            &fixture.experiment_id,
            &fixture.task_signature,
            2_799,
        )
        .expect("before due claim")
        .is_none());
    let claimed_from_due_queue = repository
        .claim_due_campaigns(2_800, 1)
        .expect("due campaign claim")
        .into_iter()
        .next()
        .expect("due queue must claim the running campaign");
    assert_eq!(claimed_from_due_queue.attempt, 0);
    assert!(repository
        .claim_due(
            &fixture.campaign_id,
            &fixture.experiment_id,
            &fixture.task_signature,
            2_800,
        )
        .expect("duplicate claim")
        .is_none());
}

fn add_running_campaign(
    db: &Db,
    parent: &Path,
    label: &str,
    started_at: i64,
) -> (String, String, String) {
    let root = parent.join(label).join("project");
    fs::create_dir_all(&root).expect("project root");
    let config_path = root.join("config.toml");
    fs::write(&config_path, "fixture").expect("project config");
    let project_id = format!("research-scheduler-{label}-project");
    let campaign_id = format!("research-scheduler-{label}-campaign");
    let experiment_id = format!("research-scheduler-{label}-experiment");
    let submission_id = format!("research-scheduler-{label}-submission");
    let proposal_id = format!("research-scheduler-{label}-proposal");
    let pueue_group = format!("research-scheduler-{label}-group");
    let task = PueueTask {
        id: started_at + 41,
        group: pueue_group.clone(),
        command: format!("python train-{label}.py"),
        state: "Running".to_owned(),
        enqueued_at: Some(started_at.to_string()),
        started_at: Some(started_at.to_string()),
        ended_at: None,
        result: None,
    };
    ProjectRepository::new(db)
        .register(&NewProject::new(
            &project_id,
            root,
            &pueue_group,
            config_path,
            started_at,
        ))
        .expect("project registration");
    let objective = ObjectiveSnapshot {
        text: format!("Improve the bounded {label} objective"),
        digest: format!("research-scheduler-{label}-objective"),
    };
    let argv = vec!["python".to_owned(), format!("train-{label}.py")];
    let baseline = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: format!("measure the {label} baseline"),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: vec!["loss".to_owned()],
        },
        &objective.digest,
    )
    .expect("baseline proposal");
    CampaignRepository::new(db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: &campaign_id,
                project_id: &project_id,
                objective: &objective,
                initial_argv: &argv,
                baseline: &baseline,
                submission_id: &submission_id,
                experiment_id: &experiment_id,
                proposal_id: &proposal_id,
                metadata: &serde_json::json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: started_at,
            },
            &CampaignLimits::default(),
        )
        .expect("campaign creation");
    ExperimentRepository::new(db)
        .mark_submitting(&experiment_id, started_at + 1)
        .expect("submission intent");
    let managed_signature = managed_task_run_signature(&task).expect("managed task identity");
    ExperimentRepository::new(db)
        .mark_accepted(&experiment_id, task.id, &managed_signature, started_at + 2)
        .expect("accepted experiment");
    let observation_signature = task_signature(&task);
    TaskObservationRepository::new(db)
        .upsert(&NewTaskObservation::new(
            &project_id,
            &observation_signature,
            task.id,
            &pueue_group,
            vec![task.command.clone()],
            &task.state,
            Some(started_at),
            task.started_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            None,
            started_at + 3,
        ))
        .expect("running observation");
    (campaign_id, experiment_id, managed_signature)
}

#[test]
fn bounded_running_campaign_scheduling_advances_past_already_scheduled_campaigns() {
    let fixture = fixture();
    let second = add_running_campaign(&fixture.db, fixture._temp.path(), "later", 2_000);
    let repository = ResearchRepository::new(&fixture.db);

    assert_eq!(
        repository
            .schedule_running_campaigns(30, 2_000, 1)
            .expect("first bounded schedule"),
        1
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("first research state")
            .next_due_at,
        Some(2_800)
    );
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 2_000, 1)
            .expect("second bounded schedule"),
        1
    );
    assert_eq!(
        repository
            .state(&second.0)
            .expect("later research state")
            .next_due_at,
        Some(3_800)
    );
}

#[test]
fn missing_started_at_uses_first_confirmed_running_observation() {
    let fixture = fixture();
    let mut queued_task = scheduler_task();
    queued_task.state = "Queued".to_owned();
    queued_task.started_at = None;
    let mut running_task = queued_task.clone();
    running_task.state = "Running".to_owned();
    running_task.started_at = None;
    let mut authoritative_task = running_task.clone();
    authoritative_task.started_at = Some("3900".to_owned());
    let mut terminal_task = running_task.clone();
    terminal_task.state = "Done".to_owned();
    terminal_task.started_at = Some("3800".to_owned());
    let queued_signature = task_signature(&queued_task);
    let running_signature = task_signature(&running_task);
    let authoritative_signature = task_signature(&authoritative_task);
    let terminal_signature = task_signature(&terminal_task);
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "DELETE FROM task_observations WHERE project_id = ?1 AND task_signature = ?2",
            rusqlite::params!["research-scheduler-project", &fixture.observation_signature],
        )
        .expect("remove the fixture's initial running observation");
    let observations = TaskObservationRepository::new(&fixture.db);
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &queued_signature,
            queued_task.id,
            &queued_task.group,
            vec![queued_task.command.clone()],
            &queued_task.state,
            Some(900),
            queued_task.started_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            None,
            1_000,
        ))
        .expect("queued observation");
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &running_signature,
            running_task.id,
            &running_task.group,
            vec![running_task.command.clone()],
            &running_task.state,
            Some(900),
            None,
            None,
            None,
            4_000,
        ))
        .expect("first running observation");

    let repository = ResearchRepository::new(&fixture.db);
    assert_eq!(
        repository
            .schedule_running_campaigns(30, 4_000, 1)
            .expect("schedule after first running observation"),
        1
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("research state")
            .next_due_at,
        Some(5_800)
    );
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &running_signature,
            running_task.id,
            &running_task.group,
            vec![running_task.command.clone()],
            &running_task.state,
            Some(900),
            None,
            None,
            None,
            5_000,
        ))
        .expect("repeated running observation");
    assert_eq!(
        observations
            .find("research-scheduler-project", &running_signature)
            .expect("read repeated observation")
            .expect("observation remains persisted")
            .started_at,
        None
    );
    assert_eq!(
        observations
            .first_observed_at("research-scheduler-project", &running_signature)
            .expect("read first running observation time"),
        Some(4_000)
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("research state after repeated observation")
            .next_due_at,
        Some(5_800)
    );
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &authoritative_signature,
            authoritative_task.id,
            &authoritative_task.group,
            vec![authoritative_task.command.clone()],
            &authoritative_task.state,
            Some(900),
            authoritative_task.started_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            None,
            5_001,
        ))
        .expect("authoritative native start timestamp");
    assert_eq!(
        observations
            .find("research-scheduler-project", &authoritative_signature)
            .expect("read authoritative observation")
            .expect("observation remains persisted")
            .started_at,
        Some(3_900)
    );
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &terminal_signature,
            terminal_task.id,
            &terminal_task.group,
            vec![terminal_task.command.clone()],
            &terminal_task.state,
            Some(900),
            terminal_task.started_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            None,
            6_000,
        ))
        .expect("authoritative terminal start timestamp");
    assert_eq!(
        observations
            .find("research-scheduler-project", &terminal_signature)
            .expect("read terminal observation")
            .expect("terminal observation remains persisted")
            .started_at,
        Some(3_800)
    );
}

#[test]
fn retry_admission_moves_native_recovery_proof_into_history_and_clears_top_level() {
    let fixture = fixture();
    let (review_id, run_id, _event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    let authority = serde_json::json!({
        "version": 1,
        "run_id": run_id,
        "review_id": review_id,
        "campaign_id": fixture.campaign_id,
        "experiment_id": fixture.experiment_id,
        "attempt": 1,
        "session_generation": 0,
        "session_id": "11111111-1111-4111-8111-111111111111",
        "fresh_launch": true,
        "service_root_identity": {
            "device": 1,
            "inode": 2,
            "owner": 3,
            "mode": 448,
            "resolution": "fixture-root",
        },
        "temp_identity": {
            "device": 1,
            "inode": 4,
            "owner": 3,
            "mode": 448,
            "mount": [1, 2],
            "service_identity": {
                "device": 1,
                "inode": 5,
                "owner": 3,
                "mode": 448,
            },
            "parent_identity": {
                "device": 1,
                "inode": 6,
                "owner": 3,
                "mode": 448,
            },
        },
        "cleanup": {"phase": "complete", "completed_at": 3_000},
    });
    let original_notes = serde_json::json!({
        "business_note": "preserve this note",
        "native_recovery": authority,
        "session_binding": "confirmed",
        "planned_session_id": "11111111-1111-4111-8111-111111111111",
        "confirmed_session_id": "11111111-1111-4111-8111-111111111111",
    });
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET notes_json = ?1 WHERE review_id = ?2",
            rusqlite::params![original_notes.to_string(), &review_id],
        )
        .expect("seed native recovery proof");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'failed' WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal retry owner");
    set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");

    let reservation = match CampaignRepository::new(&fixture.db)
        .reserve_agent_run(
            &fixture.campaign_id,
            &format!("research:{review_id}:attempt:2"),
            &CampaignLimits::default(),
            3_001,
        )
        .expect("reserve next research attempt")
    {
        AgentDecisionReservation::Reserved(reservation) => reservation,
        other => panic!("next research attempt must reserve a budget slot: {other:?}"),
    };
    let admitted = ResearchRepository::new(&fixture.db)
        .prepare_attempt(&review_id, &reservation.reservation_id, 3, 3_002)
        .expect("retry admission");
    assert!(admitted.is_some(), "the bounded retry must be admitted");

    let notes_json: String = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
            [&review_id],
            |row| row.get(0),
        )
        .expect("read admitted retry notes");
    let notes: serde_json::Value = serde_json::from_str(&notes_json).expect("notes object");
    assert_eq!(
        notes.get("business_note"),
        original_notes.get("business_note")
    );
    assert!(
        notes.get("native_recovery").is_none(),
        "the next attempt must not inherit the prior run's top-level generation proof"
    );
    let history = notes
        .get("retry_history")
        .and_then(serde_json::Value::as_array)
        .expect("the prior attempt must be retained in retry history");
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].get("native_recovery"),
        original_notes.get("native_recovery")
    );
    let reservation_status: String = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE reservation_id = ?1",
            [&reservation.reservation_id],
            |row| row.get(0),
        )
        .expect("read retry reservation");
    assert_eq!(reservation_status, "consumed");
}

fn prepare_due_secondary_campaign(fixture: &SchedulerFixture, now: i64) -> String {
    let (campaign_id, _experiment_id, _task_signature) =
        add_running_campaign(&fixture.db, fixture._temp.path(), "secondary", 2_000);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .schedule_running_campaigns(1, now, 8)
            .expect("schedule secondary campaign"),
        1
    );
    campaign_id
}

fn make_old_review_ineligible(
    fixture: &SchedulerFixture,
    event_not_before: i64,
    project_paused: bool,
    owner_status: &str,
) -> (String, i64) {
    let (review_id, run_id, event_id) = seed_active_research_outcome(fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events
             SET status = 'retry_wait', not_before = ?1, lease_until = NULL
             WHERE event_id = ?2",
            rusqlite::params![event_not_before, event_id],
        )
        .expect("make old event retryable");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE projects SET paused = ?1 WHERE project_id = ?2",
            rusqlite::params![project_paused, "research-scheduler-project"],
        )
        .expect("set old project pause state");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = ?1, launch_gate_state = 'released'
             WHERE run_id = ?2",
            rusqlite::params![owner_status, run_id],
        )
        .expect("set old owner state");
    if owner_status != "running" {
        set_fixture_native_cleanup_phase(fixture, &review_id, "complete");
    }
    (review_id, run_id)
}

#[test]
fn launch_queue_skips_paused_old_review_before_claiming_new_campaign() {
    let fixture = fixture();
    let (_old_review_id, _old_run_id) = make_old_review_ineligible(&fixture, 2_800, true, "failed");
    let secondary_campaign = prepare_due_secondary_campaign(&fixture, 3_000);
    let repository = ResearchRepository::new(&fixture.db);

    assert!(
        repository
            .due_launch_reviews(3_000, 1, 3)
            .expect("launch candidates")
            .is_empty(),
        "a paused old review must not consume the bounded launch prefix"
    );
    let claims = repository
        .claim_due_campaigns(3_000, 1)
        .expect("claim eligible new campaign");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].campaign_id, secondary_campaign);
}

#[test]
fn launch_queue_skips_review_whose_event_wake_is_still_future() {
    let fixture = fixture();
    let (_old_review_id, _old_run_id) = make_old_review_ineligible(&fixture, 5_000, false, "failed");
    let secondary_campaign = prepare_due_secondary_campaign(&fixture, 3_000);
    let repository = ResearchRepository::new(&fixture.db);

    assert!(
        repository
            .due_launch_reviews(3_000, 1, 3)
            .expect("launch candidates")
            .is_empty(),
        "an event wake in the future must not consume the bounded launch prefix"
    );
    let claims = repository
        .claim_due_campaigns(3_000, 1)
        .expect("claim eligible new campaign");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].campaign_id, secondary_campaign);
}

#[test]
fn launch_queue_skips_active_old_owner_before_claiming_new_campaign() {
    let fixture = fixture();
    let (_old_review_id, _old_run_id) = make_old_review_ineligible(&fixture, 2_800, false, "running");
    let secondary_campaign = prepare_due_secondary_campaign(&fixture, 3_000);
    let repository = ResearchRepository::new(&fixture.db);

    assert!(
        repository
            .due_launch_reviews(3_000, 1, 3)
            .expect("launch candidates")
            .is_empty(),
        "an active old owner must not consume the bounded launch prefix"
    );
    let claims = repository
        .claim_due_campaigns(3_000, 1)
        .expect("claim eligible new campaign");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].campaign_id, secondary_campaign);
}

#[test]
fn launch_queue_skips_unbound_review_when_project_has_other_active_run() {
    let fixture = fixture();
    let (review_id, run_id) = make_old_review_ineligible(&fixture, 2_800, false, "running");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET agent_run_id = NULL
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("unbound the old review");
    let secondary_campaign = prepare_due_secondary_campaign(&fixture, 3_000);
    let repository = ResearchRepository::new(&fixture.db);

    assert!(
        repository
            .due_launch_reviews(3_000, 1, 3)
            .expect("launch candidates")
            .is_empty(),
        "an unbound review must not consume the prefix while its project has an active run"
    );
    let claims = repository
        .claim_due_campaigns(3_000, 1)
        .expect("claim eligible new campaign");
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].campaign_id, secondary_campaign);
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("active project run")
            .expect("active project run row")
            .status,
        AgentRunStatus::Running
    );
}

#[test]
fn retry_owner_rejects_foreign_native_recovery_proof() {
    let fixture = fixture();
    let (review_id, run_id, _event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs
             SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal retry owner");
    set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaign_research
             SET session_id = ?1
             WHERE campaign_id = ?2",
            rusqlite::params![
                "22222222-2222-4222-8222-222222222222",
                &fixture.campaign_id,
            ],
        )
        .expect("seed confirmed native session");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(
                 json_set(notes_json, '$.planned_session_id', ?1),
                 '$.confirmed_session_id', ?2
             )
             WHERE review_id = ?3",
            rusqlite::params![
                "11111111-1111-4111-8111-111111111111",
                "22222222-2222-4222-8222-222222222222",
                &review_id,
            ],
        )
        .expect("seed confirmed session notes");
    let repository = ResearchRepository::new(&fixture.db);
    assert!(repository.retry_owner_ready(&review_id).expect("valid proof"));

    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.native_recovery.version', 2)
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("substitute proof version");
    assert!(!repository
        .retry_owner_ready(&review_id)
        .expect("version-substituted proof readiness"));
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.native_recovery.version', 1)
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("restore proof version");

    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.planned_session_id', 'foreign')
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("substitute planned session");
    assert!(!repository
        .retry_owner_ready(&review_id)
        .expect("session-substituted proof readiness"));
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.planned_session_id', ?1)
             WHERE review_id = ?2",
            rusqlite::params![
                "11111111-1111-4111-8111-111111111111",
                &review_id,
            ],
        )
        .expect("restore planned session");

    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.session_binding', 'pending')
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("substitute session binding");
    assert!(!repository
        .retry_owner_ready(&review_id)
        .expect("binding-substituted proof readiness"));
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.session_binding', 'confirmed')
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("restore session binding");

    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.native_recovery.run_id', ?1)
             WHERE review_id = ?2",
            rusqlite::params![run_id + 1, &review_id],
        )
        .expect("substitute proof owner");
    assert!(!repository
        .retry_owner_ready(&review_id)
        .expect("foreign proof readiness"));
}

#[test]
fn launch_queue_retains_terminal_capped_review_for_settlement() {
    let fixture = fixture();
    let (review_id, run_id) = make_old_review_ineligible(&fixture, 2_800, true, "failed");
    let event_id = ResearchRepository::new(&fixture.db)
        .event_id(&review_id)
        .expect("capped review event");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET attempt = 3, failure_code = 'research_output_invalid'
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("seed capped attempt");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET notes_json = json_set(notes_json, '$.native_recovery.attempt', 3)
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("align capped authority attempt");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', not_before = 0, lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed terminal research event");
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("capped owner")
            .expect("capped owner row")
            .status,
        AgentRunStatus::Failed
    );

    let candidates = ResearchRepository::new(&fixture.db)
        .due_launch_reviews(3_000, 1, 3)
        .expect("terminal settlement candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].review_id, review_id);
}

#[test]
fn attempt_cap_settlement_rolls_back_all_rows_on_event_failure() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal cap owner");
    set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed terminal research event");
    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail_cap_event_update
             BEFORE UPDATE OF status ON events
             WHEN OLD.event_id = {event_id} AND NEW.status = 'failed'
             BEGIN SELECT RAISE(ABORT, 'injected cap event failure'); END;"
        ))
        .expect("install cap event failure");

    let failed = ResearchRepository::new(&fixture.db).settle_attempt_limit(
        &review_id,
        "retry_wait",
        1,
        Some(run_id),
        EventStatus::DeadLetter,
        1,
        3_001,
    );
    assert!(failed.is_err(), "event failure must abort the cap transaction");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("review after rollback")
            .state,
        "retry_wait"
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("campaign after rollback")
            .blocked_reason,
        None
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("event after rollback")
            .expect("event row")
            .status,
        EventStatus::DeadLetter
    );

    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_cap_event_update")
        .expect("remove cap event failure");
    assert!(
        ResearchRepository::new(&fixture.db)
            .settle_attempt_limit(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                1,
                3_002,
            )
            .expect("cap settlement")
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("settled review")
            .state,
        "blocked"
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("settled event")
            .expect("event row")
            .status,
        EventStatus::Failed
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .settle_attempt_limit(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                1,
                3_003,
            )
            .expect("repeated cap settlement"),
        false
    );
}

#[test]
fn attempt_cap_settlement_rejects_stale_attempt_without_mutation() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal cap owner");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed terminal research event");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET attempt = 2 WHERE review_id = ?1",
            [&review_id],
        )
        .expect("mutate review attempt after selection");

    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .settle_attempt_limit(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                1,
                3_001,
            )
            .expect("stale cap settlement"),
        false
    );
    let review = ResearchRepository::new(&fixture.db)
        .find(&review_id)
        .expect("unchanged review");
    assert_eq!(review.state, "retry_wait");
    assert_eq!(review.attempt, 2);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("unchanged campaign")
            .blocked_reason,
        None
    );
}

#[test]
fn attempt_cap_settlement_rejects_immutable_review_and_event_phases() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal cap owner");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed terminal research event");

    assert!(
        ResearchRepository::new(&fixture.db)
            .settle_attempt_limit(
                &review_id,
                "ready",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                1,
                3_001,
            )
            .is_err(),
        "cap settlement must reject immutable ready review state"
    );
    assert!(
        ResearchRepository::new(&fixture.db)
            .settle_attempt_limit(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::Completed,
                1,
                3_001,
            )
            .is_err(),
        "cap settlement must reject unrelated completed event state"
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("unchanged review")
            .state,
        "retry_wait"
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("unchanged event")
            .expect("event row")
            .status,
        EventStatus::DeadLetter
    );
}

fn seed_retry_settlement_case(
    failure_code: &str,
    event_status: &str,
) -> (SchedulerFixture, String, i64, i64) {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal retry owner");
    set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET failure_code = ?1 WHERE review_id = ?2",
            rusqlite::params![failure_code, &review_id],
        )
        .expect("seed retry settlement reason");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = ?1,
                 lease_until = CASE WHEN ?1 = 'claimed' THEN 4_000 ELSE NULL END,
                 not_before = 0
             WHERE event_id = ?2",
            rusqlite::params![event_status, event_id],
        )
        .expect("seed retry settlement event");
    (fixture, review_id, run_id, event_id)
}

#[test]
fn unsafe_retry_settlement_rolls_back_all_rows_on_event_failure() {
    let (fixture, review_id, run_id, event_id) =
        seed_retry_settlement_case("research_session_unsafe", "dead_letter");
    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail_unsafe_event_update
             BEFORE UPDATE OF status ON events
             WHEN OLD.event_id = {event_id} AND NEW.status = 'failed'
             BEGIN SELECT RAISE(ABORT, 'injected unsafe event failure'); END;"
        ))
        .expect("install unsafe event failure");

    let failed = ResearchRepository::new(&fixture.db).settle_retry_failure(
        &review_id,
        "retry_wait",
        1,
        Some(run_id),
        EventStatus::DeadLetter,
        "research_session_unsafe",
        3_001,
    );
    assert!(failed.is_err(), "event failure must abort unsafe settlement");
    let repository = ResearchRepository::new(&fixture.db);
    let review = repository.find(&review_id).expect("review after rollback");
    assert_eq!(review.state, "retry_wait");
    assert_eq!(
        repository
            .retry_failure_code(&review_id)
            .expect("retry reason after rollback")
            .as_deref(),
        Some("research_session_unsafe")
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("campaign after rollback")
            .blocked_reason,
        None
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("event after rollback")
            .expect("event row")
            .status,
        EventStatus::DeadLetter
    );

    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_unsafe_event_update")
        .expect("remove unsafe event failure");
    assert!(
        repository
            .settle_retry_failure(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                "research_session_unsafe",
                3_002,
            )
            .expect("unsafe settlement")
    );
    let settled = repository.find(&review_id).expect("settled review");
    assert_eq!(settled.state, "blocked");
    assert_eq!(
        repository
            .retry_failure_code(&review_id)
            .expect("settled retry reason")
            .as_deref(),
        Some("research_session_unsafe")
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("settled campaign")
            .blocked_reason
            .as_deref(),
        Some("research_session_unsafe")
    );
    let settled_event = EventRepository::new(&fixture.db)
        .find_by_id(event_id)
        .expect("settled event")
        .expect("event row");
    assert_eq!(settled_event.status, EventStatus::Failed);
    assert_eq!(settled_event.last_error.as_deref(), Some("research_session_unsafe"));
    assert_eq!(
        repository
            .settle_retry_failure(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::DeadLetter,
                "research_session_unsafe",
                3_003,
            )
            .expect("repeated unsafe settlement"),
        false
    );
}

#[test]
fn policy_retry_settlement_preserves_distinct_reason() {
    let (fixture, review_id, run_id, event_id) =
        seed_retry_settlement_case("research_policy_blocked", "retry_wait");
    let repository = ResearchRepository::new(&fixture.db);
    assert!(
        repository
            .settle_retry_failure(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::RetryWait,
                "research_policy_blocked",
                3_001,
            )
            .expect("policy settlement")
    );
    assert_eq!(
        repository
            .find(&review_id)
            .expect("settled policy review")
            .state,
        "blocked"
    );
    assert_eq!(
        repository
            .retry_failure_code(&review_id)
            .expect("settled policy reason")
            .as_deref(),
        Some("research_policy_blocked")
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("settled policy campaign")
            .blocked_reason
            .as_deref(),
        Some("research_policy_blocked")
    );
    let event = EventRepository::new(&fixture.db)
        .find_by_id(event_id)
        .expect("settled policy event")
        .expect("event row");
    assert_eq!(event.status, EventStatus::Failed);
    assert_eq!(event.last_error.as_deref(), Some("research_policy_blocked"));
}

#[test]
fn pre_admission_policy_settlement_preserves_exact_prior_failure() {
    let (fixture, review_id, run_id, event_id) =
        seed_retry_settlement_case("research_output_invalid", "claimed");
    let repository = ResearchRepository::new(&fixture.db);
    assert!(
        repository
            .settle_pre_admission_failure(
                &review_id,
                "retry_wait",
                1,
                Some(run_id),
                EventStatus::Claimed,
                Some("research_output_invalid"),
                "research_policy_blocked",
                3_001,
            )
            .expect("pre-admission policy settlement")
    );
    assert_eq!(
        repository
            .state(&fixture.campaign_id)
            .expect("policy campaign")
            .blocked_reason
            .as_deref(),
        Some("research_policy_blocked")
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("policy event")
            .expect("event row")
            .last_error
            .as_deref(),
        Some("research_policy_blocked")
    );
}

#[test]
fn unbound_safety_settlement_rejects_bound_or_unclaimed_inputs() {
    let (fixture, review_id, run_id, event_id) =
        seed_retry_settlement_case("research_session_unsafe", "dead_letter");
    let repository = ResearchRepository::new(&fixture.db);
    assert!(repository
        .settle_unbound_failure(
            &review_id,
            "retry_wait",
            1,
            Some(run_id),
            EventStatus::DeadLetter,
            "research_session_unsafe",
            3_001,
        )
        .is_err());
    assert!(repository
        .settle_unbound_failure(
            &review_id,
            "pending",
            1,
            Some(run_id),
            EventStatus::Claimed,
            "research_session_unsafe",
            3_001,
        )
        .is_err());
    assert!(repository
        .settle_unbound_failure(
            &review_id,
            "pending",
            1,
            None,
            EventStatus::DeadLetter,
            "research_session_unsafe",
            3_001,
        )
        .is_err());
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("unchanged event")
            .expect("event row")
            .status,
        EventStatus::DeadLetter
    );
}

#[test]
fn retry_settlement_rejects_stale_attempt_owner_and_event_without_mutation() {
    for mutation in ["attempt", "owner", "event", "reason"] {
        let (fixture, review_id, run_id, event_id) =
            seed_retry_settlement_case("research_session_unsafe", "dead_letter");
        match mutation {
            "attempt" => fixture
                .db
                .connect()
                .unwrap()
                .execute(
                    "UPDATE research_reviews SET attempt = 2 WHERE review_id = ?1",
                    [&review_id],
                )
                .expect("mutate retry attempt after selection"),
            "owner" => fixture
                .db
                .connect()
                .unwrap()
                .execute(
                    "UPDATE agent_runs SET status = 'running' WHERE run_id = ?1",
                    [run_id],
                )
                .expect("mutate retry owner after selection"),
            "event" => fixture
                .db
                .connect()
                .unwrap()
                .execute(
                    "UPDATE events SET status = 'failed' WHERE event_id = ?1",
                    [event_id],
                )
                .expect("mutate retry event after selection"),
            "reason" => fixture
                .db
                .connect()
                .unwrap()
                .execute(
                    "UPDATE research_reviews SET failure_code = NULL WHERE review_id = ?1",
                    [&review_id],
                )
                .expect("mutate retry reason after selection"),
            _ => unreachable!(),
        };
        let repository = ResearchRepository::new(&fixture.db);
        assert_eq!(
            repository
                .settle_retry_failure(
                    &review_id,
                    "retry_wait",
                    1,
                    Some(run_id),
                    EventStatus::DeadLetter,
                    "research_session_unsafe",
                    3_001,
                )
                .expect("stale retry settlement"),
            false,
            "{mutation} mutation must make settlement stale"
        );
        assert_eq!(
            repository.find(&review_id).expect("unchanged review").state,
            "retry_wait"
        );
        assert_eq!(
            repository
                .state(&fixture.campaign_id)
                .expect("unchanged campaign")
                .blocked_reason,
            None
        );
    }
}

#[tokio::test]
async fn startup_recovery_preserves_ready_research_outcome_until_run_finalization() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "ready");
    let policies = BTreeMap::from([(
        "research-scheduler-project".to_owned(),
        RetryPolicy { max_retries: 0 },
    )]);
    let empty_markers = BTreeSet::new();
    let recovery = AgentRunRepository::new(&fixture.db)
        .recover_interrupted_with_marker_evidence(
            3_000,
            "test restart",
            &policies,
            &empty_markers,
            &empty_markers,
            &empty_markers,
            &empty_markers,
        )
        .expect("valid ready research lineage must survive generic recovery");
    assert_eq!(recovery.preserved_research_run_ids, vec![run_id]);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("ready review")
            .state,
        "ready"
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("campaign research state")
            .blocked_reason,
        None
    );

    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("ready event")
            .expect("ready event row")
            .status,
        EventStatus::InFlight
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("research run")
            .expect("research run row")
            .status,
        AgentRunStatus::Running
    );
}

#[tokio::test]
async fn startup_recovery_preserves_classified_research_failure_for_retry() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    let policies = BTreeMap::from([(
        "research-scheduler-project".to_owned(),
        RetryPolicy { max_retries: 0 },
    )]);
    let empty_markers = BTreeSet::new();
    let recovery = AgentRunRepository::new(&fixture.db)
        .recover_interrupted_with_marker_evidence(
            3_000,
            "test restart",
            &policies,
            &empty_markers,
            &empty_markers,
            &empty_markers,
            &empty_markers,
        )
        .expect("valid retry-wait research lineage must survive generic recovery");
    assert_eq!(recovery.preserved_research_run_ids, vec![run_id]);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("retry review")
            .state,
        "retry_wait"
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("campaign research state")
            .blocked_reason,
        None
    );

    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("retry event")
            .expect("retry event row")
            .status,
        EventStatus::InFlight
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("research run")
            .expect("research run row")
            .status,
        AgentRunStatus::Running
    );
}

#[tokio::test]
async fn research_recovery_preserves_due_review_and_event_wakes() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
    set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs
             SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal retry owner");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET not_before = 260, updated_at = 260
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("seed durable review wake");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events
             SET status = 'retry_wait', not_before = 260, lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed durable event wake");

    recover_research(&fixture.db, 260, CampaignLimits::default())
        .await
        .expect("startup research recovery");

    let review = ResearchRepository::new(&fixture.db)
        .find(&review_id)
        .expect("read recovered review");
    let review_not_before: i64 = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT not_before FROM research_reviews WHERE review_id = ?1",
            [&review_id],
            |row| row.get(0),
        )
        .expect("read recovered review wake");
    let event = EventRepository::new(&fixture.db)
        .find_by_id(event_id)
        .expect("read recovered event")
        .expect("research event row");
    assert_eq!(review_not_before, 260, "recovery must preserve the durable review wake");
    assert_eq!(event.not_before, 260, "recovery must preserve the durable event wake");
    assert_eq!(review.state, "retry_wait");
    assert_eq!(event.status, EventStatus::RetryWait);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .due_launch_reviews(260, 1, 3)
            .expect("due research reviews")
            .iter()
            .map(|candidate| candidate.review_id.as_str())
            .collect::<Vec<_>>(),
        vec![review_id.as_str()],
        "a due retry must remain eligible after startup recovery"
    );
}

#[tokio::test]
async fn research_recovery_uses_attempt_backoff_once_for_terminal_owner() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "running");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET attempt = 2,
                 notes_json = json_set(notes_json, '$.native_recovery.attempt', 2)
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("seed second research attempt");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs
             SET status = 'failed', launch_gate_state = 'released'
             WHERE run_id = ?1",
            [run_id],
        )
        .expect("seed terminal owner");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events
             SET status = 'in_flight', not_before = 0, lease_until = NULL
             WHERE event_id = ?1",
            [event_id],
        )
        .expect("seed in-flight event");

    recover_research(&fixture.db, 3_000, CampaignLimits::default())
        .await
        .expect("recover terminal research owner");
    let first_wakes: (i64, i64) = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT review.not_before, event.not_before
             FROM research_reviews AS review
             JOIN events AS event ON event.event_id = review.event_id
             WHERE review.review_id = ?1",
            [&review_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read first recovery wakes");
    assert_eq!(first_wakes, (3_120, 3_120));

    recover_research(&fixture.db, 3_120, CampaignLimits::default())
        .await
        .expect("repeat research recovery");
    let second_wakes: (i64, i64) = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT review.not_before, event.not_before
             FROM research_reviews AS review
             JOIN events AS event ON event.event_id = review.event_id
             WHERE review.review_id = ?1",
            [&review_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read repeated recovery wakes");
    assert_eq!(second_wakes, first_wakes);
}

#[tokio::test]
async fn research_recovery_leaves_unsafe_and_capped_retries_for_atomic_settlement() {
    for (failure_code, capped) in [
        ("research_session_unsafe", false),
        ("research_output_invalid", true),
    ] {
        let fixture = fixture();
        let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "retry_wait");
        set_fixture_native_cleanup_phase(&fixture, &review_id, "complete");
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE agent_runs
                 SET status = 'failed', launch_gate_state = 'released'
                 WHERE run_id = ?1",
                [run_id],
            )
            .expect("seed terminal retry owner");
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE research_reviews
                 SET attempt = CASE WHEN ?1 THEN 3 ELSE attempt END,
                     failure_code = ?2, not_before = 260, updated_at = 260,
                     notes_json = CASE WHEN ?1
                         THEN json_set(notes_json, '$.native_recovery.attempt', 3)
                         ELSE notes_json END
                 WHERE review_id = ?3",
                rusqlite::params![capped, failure_code, &review_id],
            )
            .expect("seed typed retry boundary");
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events
                 SET status = 'dead_letter', not_before = 260, lease_until = NULL
                 WHERE event_id = ?1",
                [event_id],
            )
            .expect("seed terminal retry event");

        recover_research(&fixture.db, 260, CampaignLimits::default())
            .await
            .expect("startup research recovery");

        let review = ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("read recovered typed retry");
        let event = EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("read recovered typed event")
            .expect("typed research event row");
        assert_eq!(review.state, "retry_wait", "{failure_code} must remain settleable");
        assert_eq!(event.status, EventStatus::DeadLetter);
        assert_eq!(
            ResearchRepository::new(&fixture.db)
                .due_launch_reviews(260, 1, 3)
                .expect("typed retry candidates")
                .iter()
                .map(|candidate| candidate.review_id.as_str())
                .collect::<Vec<_>>(),
            vec![review_id.as_str()],
            "{failure_code} must remain visible to atomic settlement"
        );
    }
}

#[tokio::test]
async fn standalone_research_recovery_requires_startup_owner_evidence() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "running");
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE agent_runs SET pid = NULL WHERE run_id = ?1",
            [run_id],
        )
        .expect("remove startup-only process identity from standalone fixture");
    let reservation_id = match CampaignRepository::new(&fixture.db)
        .reserve_agent_run(
            &fixture.campaign_id,
            &format!("research:{review_id}:attempt:1"),
            &CampaignLimits::default(),
            2_902,
        )
        .expect("reserve standalone research budget")
    {
        AgentDecisionReservation::Reserved(reservation) => reservation.reservation_id,
        other => panic!("standalone research budget must reserve: {other:?}"),
    };
    let review_before = ResearchRepository::new(&fixture.db)
        .find(&review_id)
        .expect("standalone review before recovery");
    let run_before = AgentRunRepository::new(&fixture.db)
        .find_by_id(run_id)
        .expect("standalone run before recovery")
        .expect("standalone run row");
    let event_before = EventRepository::new(&fixture.db)
        .find_by_id(event_id)
        .expect("standalone event before recovery")
        .expect("standalone event row");
    let state_before = ResearchRepository::new(&fixture.db)
        .state(&fixture.campaign_id)
        .expect("standalone session before recovery");
    let notes_before: String = fixture
        .db
        .connect()
        .expect("notes database connection")
        .query_row(
            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
            [&review_id],
            |row| row.get(0),
        )
        .expect("standalone notes before recovery");
    let reservation_before: (String, String) = fixture
        .db
        .connect()
        .expect("reservation database connection")
        .query_row(
            "SELECT reservation_id, status FROM budget_reservations
             WHERE reservation_id = ?1",
            [&reservation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("standalone reservation before recovery");

    recover_research(&fixture.db, 3_000, CampaignLimits::default())
        .await
        .expect("standalone research recovery pass");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("standalone review after recovery"),
        review_before
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("standalone event after recovery")
            .expect("standalone event row"),
        event_before
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("standalone run after recovery")
            .expect("standalone run row"),
        run_before
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("standalone session after recovery"),
        state_before
    );
    let notes_after: String = fixture
        .db
        .connect()
        .expect("notes database connection")
        .query_row(
            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
            [&review_id],
            |row| row.get(0),
        )
        .expect("standalone notes after recovery");
    assert_eq!(notes_after, notes_before);
    let reservation_after: (String, String) = fixture
        .db
        .connect()
        .expect("reservation database connection")
        .query_row(
            "SELECT reservation_id, status FROM budget_reservations
             WHERE reservation_id = ?1",
            [&reservation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("standalone reservation after recovery");
    assert_eq!(reservation_after, reservation_before);
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("standalone review remains bound")
            .agent_run_id,
        Some(run_id)
    );
}

#[cfg(unix)]
#[tokio::test]
async fn live_research_owner_remains_bound_across_recovery_poll() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "running");
    let mut child = Command::new("sleep")
        .arg("5")
        .spawn()
        .expect("live owner fixture");
    let pid = i64::from(child.id());
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE agent_runs SET pid = ?1 WHERE run_id = ?2",
            rusqlite::params![pid, run_id],
        )
        .expect("persist live owner pid");

    recover_research(&fixture.db, 3_000, CampaignLimits::default())
        .await
        .expect("research live owner recovery");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("live review")
            .state,
        "running"
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("live event")
            .expect("live event row")
            .status,
        EventStatus::InFlight
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("live run")
            .expect("live run row")
            .status,
        AgentRunStatus::Running
    );
    child.kill().expect("stop live owner fixture");
    child.wait().expect("reap live owner fixture");
}

#[cfg(unix)]
#[tokio::test]
async fn unsafe_research_temp_retains_owner_instead_of_following_symlink() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "running");
    let temp_root = fixture
        ._temp
        .path()
        .join("project")
        .join(".pueue-agent")
        .join("tmp");
    fs::create_dir_all(&temp_root).expect("research temp root");
    for path in [
        temp_root.parent().expect("service root"),
        temp_root.as_path(),
    ] {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .expect("research temp permissions");
    }
    let outside = fixture._temp.path().join("outside");
    fs::create_dir(&outside).expect("outside directory");
    let sentinel = outside.join("sentinel");
    fs::write(&sentinel, b"retain").expect("outside sentinel");
    std::os::unix::fs::symlink(&outside, temp_root.join(run_id.to_string()))
        .expect("unsafe research temp symlink");
    let child = Command::new("sh")
        .args(["-c", "exit 0"])
        .spawn()
        .expect("dead owner fixture");
    let pid = i64::from(child.id());
    child.wait_with_output().expect("reap dead owner fixture");
    fixture
        .db
        .connect()
        .expect("database connection")
        .execute(
            "UPDATE agent_runs SET pid = ?1 WHERE run_id = ?2",
            rusqlite::params![pid, run_id],
        )
        .expect("persist dead owner pid");

    recover_research(&fixture.db, 3_000, CampaignLimits::default())
        .await
        .expect("research unsafe temp recovery");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("retained review")
            .state,
        "running"
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("retained event")
            .expect("retained event row")
            .status,
        EventStatus::InFlight
    );
    assert_eq!(fs::read(&sentinel).expect("outside sentinel"), b"retain");
}
const I4_FIXTURE_SESSION_ID: &str = "11111111-1111-4111-8111-111111111111";

fn i4_research_binding(
    fixture: &SchedulerFixture,
    review_id: &str,
    budget_reservation_id: &str,
) -> pueue_agent::db::ResearchLaunchBinding {
    let context_json = "{}".to_owned();
    let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
    pueue_agent::db::ResearchLaunchBinding {
        review_id: review_id.to_owned(),
        campaign_id: fixture.campaign_id.clone(),
        experiment_id: fixture.experiment_id.clone(),
        attempt: 1,
        session_generation: 0,
        prior_session_generation: 0,
        session_id: I4_FIXTURE_SESSION_ID.to_owned(),
        prior_session_id: Some(I4_FIXTURE_SESSION_ID.to_owned()),
        context_json,
        context_digest,
        budget_reservation_id: budget_reservation_id.to_owned(),
        recovery_reason: None,
    }
}

fn i4_fixture_identity() -> pueue_agent::environment::PrivateRunTempRecoveryIdentityV1 {
    use pueue_agent::environment::{
        PrivateRunTempRecoveryDirectoryIdentity, PrivateRunTempRecoveryRootIdentity,
        PrivateRunTempRecoveryTempIdentity,
    };
    pueue_agent::environment::PrivateRunTempRecoveryIdentityV1 {
        service_root_identity: PrivateRunTempRecoveryRootIdentity {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 448,
            resolution_fingerprint: "fixture-root".to_owned(),
        },
        temp_identity: PrivateRunTempRecoveryTempIdentity {
            device: 1,
            inode: 4,
            owner: 3,
            mode: 448,
            mount_identity: [1, 2],
            service_identity: PrivateRunTempRecoveryDirectoryIdentity {
                device: 1,
                inode: 5,
                owner: 3,
                mode: 448,
            },
            parent_identity: PrivateRunTempRecoveryDirectoryIdentity {
                device: 1,
                inode: 6,
                owner: 3,
                mode: 448,
            },
        },
    }
}

fn i4_reserve_initial_attempt(fixture: &SchedulerFixture, review_id: &str) -> String {
    match CampaignRepository::new(&fixture.db)
        .reserve_agent_run(
            &fixture.campaign_id,
            &format!("research:{review_id}:attempt:1"),
            &CampaignLimits::default(),
            2_902,
        )
        .expect("reserve I4 research attempt")
    {
        AgentDecisionReservation::Reserved(reservation) => reservation.reservation_id,
        other => panic!("I4 research attempt must reserve a budget slot: {other:?}"),
    }
}

fn i4_seed_bound_research(
    fixture: &SchedulerFixture,
    fresh_launch: bool,
) -> (
    String,
    i64,
    i64,
    String,
    pueue_agent::db::ResearchLaunchBinding,
) {
    let (review_id, run_id, event_id) = seed_active_research_outcome(fixture, "running");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET attempt = 0, state = 'pending', agent_run_id = NULL,
                 context_json = NULL, context_digest = NULL, response_json = NULL,
                 failure_code = NULL, started_at = NULL, finished_at = NULL,
                 notes_json = NULL, not_before = 2_902, updated_at = 2_902
             WHERE review_id = ?1",
            [&review_id],
        )
        .expect("reset I4 review before real bind");
    let reservation_id = i4_reserve_initial_attempt(fixture, &review_id);
    let admitted = ResearchRepository::new(&fixture.db)
        .prepare_attempt(&review_id, &reservation_id, 3, 2_902)
        .expect("admit I4 research attempt")
        .expect("I4 research attempt must be admitted");
    assert_eq!(admitted.attempt, 1);
    let binding = i4_research_binding(fixture, &review_id, &reservation_id);
    ResearchRepository::new(&fixture.db)
        .bind_agent_run(&binding, run_id, "research-scheduler-project", 2_903)
        .expect("bind I4 research attempt");
    ResearchRepository::new(&fixture.db)
        .record_native_recovery_authority(
            &binding,
            run_id,
            &i4_fixture_identity(),
            fresh_launch,
            2_904,
        )
        .expect("record I4 native authority");
    if !fresh_launch {
        ResearchRepository::new(&fixture.db)
            .confirm_agent_run_session(&binding, run_id, I4_FIXTURE_SESSION_ID, 2_905)
            .expect("confirm I4 resumed session");
    }
    (review_id, run_id, event_id, reservation_id, binding)
}

fn i4_set_ready_result(
    fixture: &SchedulerFixture,
    review_id: &str,
    binding: &pueue_agent::db::ResearchLaunchBinding,
) {
    let response_json = serde_json::json!({
        "schema_version": 1,
        "review_id": review_id,
        "experiment_id": fixture.experiment_id.clone(),
        "context_digest": binding.context_digest.clone(),
        "action": "continue",
        "reason": "recovered response",
        "evidence_refs": ["test"],
        "notes": "recovered",
        "next_direction": null,
        "checkpoint": null,
    })
    .to_string();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews
             SET state = 'ready', response_json = ?1, failure_code = NULL,
                 finished_at = 2_906, not_before = 2_906, updated_at = 2_906
             WHERE review_id = ?2",
            rusqlite::params![response_json, review_id],
        )
        .expect("seed I4 ready result");
}

fn i4_reservation_status(fixture: &SchedulerFixture, reservation_id: &str) -> String {
    fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM budget_reservations WHERE reservation_id = ?1",
            [reservation_id],
            |row| row.get(0),
        )
        .expect("read I4 reservation status")
}

fn i4_update_notes(
    fixture: &SchedulerFixture,
    review_id: &str,
    update: impl FnOnce(&mut serde_json::Value),
) {
    let notes_json: String = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
            [review_id],
            |row| row.get(0),
        )
        .expect("read I4 research notes");
    let mut notes: serde_json::Value = serde_json::from_str(&notes_json).expect("I4 notes JSON");
    update(&mut notes);
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE research_reviews SET notes_json = ?1 WHERE review_id = ?2",
            rusqlite::params![notes.to_string(), review_id],
        )
        .expect("update I4 research notes");
}

fn i4_startup_recovery(
    fixture: &SchedulerFixture,
) -> Result<pueue_agent::db::AgentRunRecovery, pueue_agent::AppError> {
    let policies = BTreeMap::from([(
        "research-scheduler-project".to_owned(),
        RetryPolicy { max_retries: 0 },
    )]);
    let empty_markers = BTreeSet::new();
    AgentRunRepository::new(&fixture.db).recover_interrupted_with_marker_evidence(
        3_000,
        "test restart",
        &policies,
        &empty_markers,
        &empty_markers,
        &empty_markers,
        &empty_markers,
    )
}

#[tokio::test]
async fn startup_recovery_accepts_fresh_failed_research_after_session_clear() {
    let fixture = fixture();
    let (review_id, run_id, event_id, reservation_id, binding) =
        i4_seed_bound_research(&fixture, true);
    ResearchRepository::new(&fixture.db)
        .fail_agent_run_and_clear_session(&binding, run_id, None, "research_output_invalid", 2_903)
        .expect("persist genuine fresh failure window");

    let recovery = i4_startup_recovery(&fixture).expect("startup recovery");
    assert_eq!(recovery.preserved_research_run_ids, vec![run_id]);
    assert_eq!(recovery.failed_runs, 0);
    assert_eq!(recovery.requeued_events, 0);
    assert_eq!(recovery.dead_lettered_events, 0);
    assert_eq!(i4_reservation_status(&fixture, &reservation_id), "consumed");
    let review = ResearchRepository::new(&fixture.db)
        .find(&review_id)
        .expect("fresh failed review");
    assert_eq!(review.state, "retry_wait");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .review_failure_code(&review_id)
            .expect("fresh failure code"),
        Some("research_output_invalid".to_owned())
    );
    let campaign = ResearchRepository::new(&fixture.db)
        .state(&fixture.campaign_id)
        .expect("fresh campaign state");
    assert_eq!(campaign.session_id, None);
    assert_eq!(campaign.blocked_reason, None);
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("fresh research run")
            .expect("fresh research run row")
            .status,
        AgentRunStatus::Running
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("fresh research event")
            .expect("fresh research event row")
            .status,
        EventStatus::InFlight
    );
}

#[tokio::test]
async fn startup_recovery_preserves_unsafe_research_failure_as_typed_outcome() {
    let fixture = fixture();
    let (review_id, run_id, event_id, reservation_id, binding) =
        i4_seed_bound_research(&fixture, false);
    i4_update_notes(&fixture, &review_id, |notes| {
        notes["business_note"] = serde_json::json!("retain this note");
    });
    ResearchRepository::new(&fixture.db)
        .fail_agent_run(&binding, run_id, "research_session_unsafe", 2_903)
        .expect("persist unsafe research failure window");

    let recovery = i4_startup_recovery(&fixture).expect("startup recovery");
    assert_eq!(recovery.preserved_research_run_ids, vec![run_id]);
    assert_eq!(recovery.failed_runs, 0);
    assert_eq!(i4_reservation_status(&fixture, &reservation_id), "consumed");
    let review = ResearchRepository::new(&fixture.db)
        .find(&review_id)
        .expect("unsafe research review");
    assert_eq!(review.state, "retry_wait");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .review_failure_code(&review_id)
            .expect("unsafe failure code"),
        Some("research_session_unsafe".to_owned())
    );
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .state(&fixture.campaign_id)
            .expect("unsafe campaign state")
            .blocked_reason,
        None
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("unsafe research run")
            .expect("unsafe research run row")
            .status,
        AgentRunStatus::Running
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("unsafe research event")
            .expect("unsafe research event row")
            .status,
        EventStatus::InFlight
    );
}

#[tokio::test]
async fn startup_recovery_preserves_research_owner_for_missing_malformed_or_foreign_authority() {
    for mutation in ["missing", "malformed", "foreign"] {
        let fixture = fixture();
        let (review_id, run_id, event_id, reservation_id, binding) =
            i4_seed_bound_research(&fixture, false);
        i4_set_ready_result(&fixture, &review_id, &binding);
        i4_update_notes(&fixture, &review_id, |notes| {
            notes["business_note"] = serde_json::json!("retain this note");
            match mutation {
                "missing" => {
                    notes
                        .as_object_mut()
                        .expect("I4 notes object")
                        .remove("native_recovery");
                    notes["legacy_recovery"] = serde_json::json!("v0");
                }
                "malformed" => {
                    notes["native_recovery"]["unexpected"] = serde_json::json!(true);
                }
                "foreign" => {
                    notes["native_recovery"]["run_id"] = serde_json::json!(run_id + 1);
                }
                _ => unreachable!(),
            }
        });
        let before = ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("research review before fail-closed recovery");
        let notes_before: String = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get(0),
            )
            .expect("notes before fail-closed recovery");

        let recovery = i4_startup_recovery(&fixture).expect("startup recovery");
        assert_eq!(
            recovery.preserved_research_run_ids,
            vec![run_id],
            "{mutation}"
        );
        assert_eq!(recovery.failed_runs, 0, "{mutation}");
        assert_eq!(recovery.requeued_events, 0, "{mutation}");
        assert_eq!(recovery.dead_lettered_events, 0, "{mutation}");
        assert_eq!(
            i4_reservation_status(&fixture, &reservation_id),
            "consumed",
            "{mutation} reservation"
        );
        let after = ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("research review after fail-closed recovery");
        assert_eq!(after.state, before.state, "{mutation} state");
        assert_eq!(
            after.response_json, before.response_json,
            "{mutation} response"
        );
        assert_eq!(
            after.context_json, before.context_json,
            "{mutation} context"
        );
        assert_eq!(
            after.context_digest, before.context_digest,
            "{mutation} digest"
        );
        let notes_after: String = fixture
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
                [&review_id],
                |row| row.get(0),
            )
            .expect("notes after fail-closed recovery");
        assert_eq!(notes_after, notes_before, "{mutation} notes");
        assert_eq!(
            ResearchRepository::new(&fixture.db)
                .state(&fixture.campaign_id)
                .expect("campaign after fail-closed recovery")
                .blocked_reason
                .as_deref(),
            Some("research_recovery_required"),
            "{mutation} recovery reason"
        );
        assert_eq!(
            AgentRunRepository::new(&fixture.db)
                .find_by_id(run_id)
                .expect("research run after fail-closed recovery")
                .expect("research run row")
                .status,
            AgentRunStatus::Running,
            "{mutation} run ownership"
        );
        assert_eq!(
            EventRepository::new(&fixture.db)
                .find_by_id(event_id)
                .expect("research event after fail-closed recovery")
                .expect("research event row")
                .status,
            EventStatus::InFlight,
            "{mutation} event ownership"
        );
    }
}
