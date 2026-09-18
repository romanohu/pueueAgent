//! Task 4 scheduler contract tests.

use std::fs;

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
