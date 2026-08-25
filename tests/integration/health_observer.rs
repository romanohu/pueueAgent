use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    db::{
        CampaignRepository, Db, ExperimentRepository, HealthRepository, ProjectRepository,
        StartCampaignRequest,
    },
    execution_policy::CampaignLimits,
    health::{HealthEngine, HealthReport},
    models::{ExperimentStatus, HealthState, NewProject, ProposalKind},
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, Reconciler},
    state::ObjectiveSnapshot,
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
        }
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        panic!("observer fixtures must not submit Pueue tasks")
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("the observer session must not kill Pueue tasks")
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("observer fixtures must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("observer fixtures must not provision Pueue groups")
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
}

const PROJECT_CONFIG_TOML: &str = r#"
project_id = "{project_id}"
pueue_group = "{group}"

[agent]
program = "/bin/echo"
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
"#;

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let harness = Self { _temp: temp, db };
        harness.setup_project("project-a", "pa-project");
        harness
    }

    fn root(&self, project_id: &str) -> std::path::PathBuf {
        self._temp.path().join(project_id)
    }

    fn log_path(&self, project_id: &str, task_id: i64) -> std::path::PathBuf {
        self.root(project_id)
            .join(".pueue-agent/logs")
            .join(format!("{task_id}.log"))
    }

    fn setup_project(&self, project_id: &str, group: &str) {
        let root = self.root(project_id);
        fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        fs::write(
            root.join(".pueue-agent/config.toml"),
            PROJECT_CONFIG_TOML
                .replace("{project_id}", project_id)
                .replace("{group}", group),
        )
        .unwrap();
        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                project_id,
                &root,
                group,
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
    }

    fn accepted_campaign_experiment(
        &self,
        project_id: &str,
        group: &str,
        suffix: &str,
        task_id: i64,
    ) -> String {
        let objective = ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: format!("objective-digest-{suffix}"),
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
        let experiment_id_value = format!("health-experiment-{suffix}");
        let intent = CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: &format!("campaign-health-{suffix}"),
                    project_id,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: &format!("health-submission-{suffix}"),
                    experiment_id: &experiment_id_value,
                    proposal_id: &format!("health-proposal-{suffix}"),
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
        let experiment_id = intent.experiment.experiment_id;
        let experiments = ExperimentRepository::new(&self.db);
        experiments.mark_submitting(&experiment_id, 101).unwrap();
        experiments
            .mark_accepted(
                &experiment_id,
                task_id,
                &managed_task_run_signature(&running_task(group, task_id, "100")).unwrap(),
                102,
            )
            .unwrap();
        experiment_id
    }

    async fn reconcile_tasks(&self, tasks: Vec<PueueTask>, now: i64) {
        Reconciler::new(&self.db, FakePueue::with_tasks(tasks))
            .run_once_at(now)
            .await
            .unwrap();
    }

    fn backdate_last_observed(&self, experiment_id: &str, last_observed_at: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE running_health SET last_observed_at = ?1 WHERE experiment_id = ?2",
                rusqlite::params![last_observed_at, experiment_id],
            )
            .unwrap();
    }

    fn run_engine(&self, tasks: &[PueueTask], now: i64) -> HealthReport {
        let projects = ProjectRepository::new(&self.db).list_enabled().unwrap();
        HealthEngine::run_once(&self.db, &projects, tasks, &CampaignLimits::default(), now).unwrap()
    }

    fn agent_run_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| row.get(0))
            .unwrap()
    }
}

fn running_task(group: &str, id: i64, enqueued_at: &str) -> PueueTask {
    PueueTask {
        id,
        group: group.to_owned(),
        command: "python train.py --name experiment".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some(enqueued_at.to_owned()),
        started_at: Some(enqueued_at.to_owned()),
        ended_at: None,
        result: None,
    }
}

fn done_task(group: &str, id: i64, enqueued_at: &str) -> PueueTask {
    PueueTask {
        state: "Done".to_owned(),
        ended_at: Some(enqueued_at.to_owned()),
        result: Some(json!("Success")),
        ..running_task(group, id, enqueued_at)
    }
}

#[tokio::test]
async fn observer_records_healthy_experiments_without_agents() {
    let harness = Harness::new();
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);

    harness
        .reconcile_tasks(vec![running_task("pa-project", 41, "100")], 200)
        .await;

    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .expect("first running observation registers the health row");
    assert_eq!(row.state, HealthState::Healthy);
    assert_eq!(row.last_observed_at, 200);
    assert_eq!(row.signal_summary_json, "[]");

    harness.backdate_last_observed(&experiment_id, 100);
    let report = harness.run_engine(&[running_task("pa-project", 41, "100")], 10_000);

    assert_eq!(report.observed, 1);
    assert_eq!(report.escalated, 0);
    assert_eq!(report.executed_actions, 0);
    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, HealthState::Healthy);
    assert_eq!(row.last_observed_at, 10_000);
    assert_eq!(row.observation_count, 1);
    assert_eq!(row.signal_summary_json, "[]");
    assert_eq!(harness.agent_run_count(), 0);
}

#[tokio::test]
async fn repeated_same_class_signals_escalate_to_suspicious() {
    let harness = Harness::new();
    fs::write(
        harness.log_path("project-a", 41),
        "torch.cuda.OutOfMemoryError: CUDA out of memory\n",
    )
    .unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(vec![running_task("pa-project", 41, "100")], 200)
        .await;

    harness.backdate_last_observed(&experiment_id, 5_000);
    let first = harness.run_engine(&[running_task("pa-project", 41, "100")], 10_000);
    assert_eq!(first.observed, 1);
    assert_eq!(first.escalated, 0);
    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, HealthState::Healthy);
    let first_summary: Vec<serde_json::Value> =
        serde_json::from_str(&row.signal_summary_json).unwrap();
    assert_eq!(first_summary.len(), 1);
    assert_eq!(first_summary[0]["class"], json!("oom"));

    harness.backdate_last_observed(&experiment_id, 15_000);
    let second = harness.run_engine(&[running_task("pa-project", 41, "100")], 20_000);
    assert_eq!(second.observed, 1);
    assert_eq!(second.escalated, 1);
    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, HealthState::Suspicious);
    assert_eq!(row.last_observed_at, 20_000);
    let second_summary: Vec<serde_json::Value> =
        serde_json::from_str(&row.signal_summary_json).unwrap();
    assert_eq!(second_summary.len(), 2);
    assert_eq!(second_summary[1]["class"], json!("oom"));

    harness.backdate_last_observed(&experiment_id, 25_000);
    let third = harness.run_engine(&[running_task("pa-project", 41, "100")], 30_000);
    assert_eq!(third.escalated, 0);
    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, HealthState::Suspicious);
}

#[tokio::test]
async fn paused_project_defers_observations() {
    let harness = Harness::new();
    harness.setup_project("project-b", "pb-project");
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    fs::write(harness.log_path("project-b", 42), "epoch 1 loss 0.61\n").unwrap();
    let experiment_a = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    let experiment_b = harness.accepted_campaign_experiment("project-b", "pb-project", "b", 42);
    harness
        .reconcile_tasks(
            vec![
                running_task("pa-project", 41, "100"),
                running_task("pb-project", 42, "100"),
            ],
            200,
        )
        .await;

    ProjectRepository::new(&harness.db)
        .pause("project-a", 250)
        .unwrap();
    harness.backdate_last_observed(&experiment_a, 100);
    harness.backdate_last_observed(&experiment_b, 100);

    let report = harness.run_engine(
        &[
            running_task("pa-project", 41, "100"),
            running_task("pb-project", 42, "100"),
        ],
        10_000,
    );

    assert_eq!(report.observed, 1);
    assert_eq!(report.escalated, 0);
    let row_a = HealthRepository::get(&harness.db, &experiment_a)
        .unwrap()
        .unwrap();
    assert_eq!(row_a.last_observed_at, 100);
    assert_eq!(row_a.signal_summary_json, "[]");
    assert_eq!(row_a.observation_count, 0);
    let row_b = HealthRepository::get(&harness.db, &experiment_b)
        .unwrap()
        .unwrap();
    assert_eq!(row_b.last_observed_at, 10_000);
    assert_eq!(row_b.observation_count, 1);
}

#[tokio::test]
async fn terminal_projection_deletes_health_row() {
    let harness = Harness::new();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(vec![running_task("pa-project", 41, "100")], 200)
        .await;
    assert!(
        HealthRepository::get(&harness.db, &experiment_id)
            .unwrap()
            .is_some(),
        "running observation registers the health row"
    );

    harness
        .reconcile_tasks(vec![done_task("pa-project", 41, "100")], 300)
        .await;

    assert!(
        HealthRepository::get(&harness.db, &experiment_id)
            .unwrap()
            .is_none(),
        "terminal projection removes the health row"
    );
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Succeeded
    );
}
