//! Task 4 scheduler contract tests.

use std::{collections::{BTreeMap, BTreeSet}, fs, path::Path};

#[cfg(unix)]
use std::{os::unix::fs::PermissionsExt, process::Command};

use pueue_agent::{
    db::{
        AgentRunRepository, CampaignRepository, Db, EventRepository, ExperimentRepository,
        ProjectRepository, ResearchRepository, StartCampaignRequest, TaskObservationRepository,
    },
    execution_policy::CampaignLimits,
    models::{
        AgentContextMode, AgentRunStatus, EventStatus, ExecutionProjection, NewAgentRun,
        NewProject, NewTaskObservation, ProposalKind,
    },
    proposals::{self, ProposalInput},
    research::recover_research,
    retry::RetryPolicy,
    state::ObjectiveSnapshot,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

use pueue_agent::db::next_research_due;

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

struct SchedulerFixture {
    _temp: TempDir,
    db: Db,
    campaign_id: String,
    experiment_id: String,
    task_signature: String,
}

fn fixture() -> SchedulerFixture {
    let temp = tempfile::tempdir().expect("fixture directory");
    let root = temp.path().join("project");
    fs::create_dir_all(&root).expect("project root");
    let config_path = root.join("config.toml");
    fs::write(&config_path, "fixture").expect("project config");
    let db = Db::open(&temp.path().join("state.sqlite3")).expect("database");
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
    let task_signature = "research-scheduler-task:v1";
    ExperimentRepository::new(&db)
        .mark_accepted(experiment_id, 41, task_signature, 902)
        .expect("accepted experiment");
    TaskObservationRepository::new(&db)
        .upsert(&NewTaskObservation::new(
            project_id,
            task_signature,
            41,
            "research-scheduler-group",
            argv,
            "Running",
            Some(900),
            Some(1_000),
            None,
            None,
            1_001,
        ))
        .expect("running observation");
    ResearchRepository::new(&db)
        .ensure_campaign(campaign_id)
        .expect("research state");
    SchedulerFixture {
        _temp: temp,
        db,
        campaign_id: campaign_id.to_owned(),
        experiment_id: experiment_id.to_owned(),
        task_signature: task_signature.to_owned(),
    }
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
    (review.review_id, run.run_id, event_id)
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
    ProjectRepository::new(db)
        .register(&NewProject::new(
            &project_id,
            root,
            format!("research-scheduler-{label}-group"),
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
    let task_signature = format!("research-scheduler-task:v1:{label}");
    ExperimentRepository::new(db)
        .mark_accepted(&experiment_id, started_at + 41, &task_signature, started_at + 2)
        .expect("accepted experiment");
    TaskObservationRepository::new(db)
        .upsert(&NewTaskObservation::new(
            &project_id,
            &task_signature,
            started_at + 41,
            format!("research-scheduler-{label}-group"),
            argv,
            "Running",
            Some(started_at),
            Some(started_at),
            None,
            None,
            started_at + 3,
        ))
        .expect("running observation");
    (campaign_id, experiment_id, task_signature)
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
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "DELETE FROM task_observations WHERE project_id = ?1 AND task_signature = ?2",
            rusqlite::params!["research-scheduler-project", &fixture.task_signature],
        )
        .expect("remove the fixture's initial running observation");
    let observations = TaskObservationRepository::new(&fixture.db);
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &fixture.task_signature,
            41,
            "research-scheduler-group",
            vec!["python".to_owned(), "train.py".to_owned()],
            "Queued",
            None,
            None,
            None,
            None,
            1_000,
        ))
        .expect("queued observation");
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &fixture.task_signature,
            41,
            "research-scheduler-group",
            vec!["python".to_owned(), "train.py".to_owned()],
            "Running",
            None,
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
            &fixture.task_signature,
            41,
            "research-scheduler-group",
            vec!["python".to_owned(), "train.py".to_owned()],
            "Running",
            None,
            None,
            None,
            None,
            5_000,
        ))
        .expect("repeated running observation");
    assert_eq!(
        observations
            .find("research-scheduler-project", &fixture.task_signature)
            .expect("read repeated observation")
            .expect("observation remains persisted")
            .started_at,
        Some(4_000)
    );
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &fixture.task_signature,
            41,
            "research-scheduler-group",
            vec!["python".to_owned(), "train.py".to_owned()],
            "Running",
            None,
            Some(3_900),
            None,
            None,
            5_001,
        ))
        .expect("authoritative native start timestamp");
    assert_eq!(
        observations
            .find("research-scheduler-project", &fixture.task_signature)
            .expect("read authoritative observation")
            .expect("observation remains persisted")
            .started_at,
        Some(3_900)
    );
    observations
        .upsert(&NewTaskObservation::new(
            "research-scheduler-project",
            &fixture.task_signature,
            41,
            "research-scheduler-group",
            vec!["python".to_owned(), "train.py".to_owned()],
            "Done",
            None,
            Some(3_800),
            None,
            None,
            6_000,
        ))
        .expect("authoritative terminal start timestamp");
    assert_eq!(
        observations
            .find("research-scheduler-project", &fixture.task_signature)
            .expect("read terminal observation")
            .expect("terminal observation remains persisted")
            .started_at,
        Some(3_800)
    );
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

#[cfg(unix)]
fn prepare_research_temp(fixture: &SchedulerFixture, run_id: i64) -> std::path::PathBuf {
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
    let run_temp = temp_root.join(run_id.to_string());
    fs::create_dir(&run_temp).expect("research run temp");
    fs::set_permissions(&run_temp, fs::Permissions::from_mode(0o700))
        .expect("research run temp permissions");
    run_temp
}

#[cfg(unix)]
#[tokio::test]
async fn dead_research_owner_is_retried_after_process_group_and_temp_proof() {
    let fixture = fixture();
    let (review_id, run_id, event_id) = seed_active_research_outcome(&fixture, "running");
    let run_temp = prepare_research_temp(&fixture, run_id);
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
        .expect("research startup owner recovery");
    assert_eq!(
        ResearchRepository::new(&fixture.db)
            .find(&review_id)
            .expect("recovered review")
            .state,
        "retry_wait"
    );
    assert_eq!(
        EventRepository::new(&fixture.db)
            .find_by_id(event_id)
            .expect("recovered event")
            .expect("recovered event row")
            .status,
        EventStatus::RetryWait
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(run_id)
            .expect("recovered run")
            .expect("recovered run row")
            .status,
        AgentRunStatus::Failed
    );
    assert!(run_temp.exists(), "recovery keeps the verified run directory");
    assert_eq!(
        fs::read_dir(run_temp)
            .expect("read cleaned run directory")
            .count(),
        0
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
