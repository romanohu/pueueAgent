//! Task 4 scheduler contract tests.

use std::{fs, path::Path};

use pueue_agent::{
    db::{
        CampaignRepository, Db, ExperimentRepository, ProjectRepository, ResearchRepository,
        StartCampaignRequest, TaskObservationRepository,
    },
    execution_policy::CampaignLimits,
    models::{NewProject, NewTaskObservation, ProposalKind},
    proposals::{self, ProposalInput},
    state::ObjectiveSnapshot,
};
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
}
