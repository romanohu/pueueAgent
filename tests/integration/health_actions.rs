use std::{
    fs,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use pueue_agent::{
    db::{
        CampaignRepository, Db, ExperimentRepository, HealthRepository, ProjectRepository,
        StartCampaignRequest, TerminationRequestRepository,
    },
    execution_policy::CampaignLimits,
    health::HealthEngine,
    models::{ExperimentStatus, HealthState, NewProject, ProposalKind},
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, Reconciler},
    state::ObjectiveSnapshot,
    termination::TerminationManager,
    AppError,
};
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }

    fn push_task(&self, task: PueueTask) {
        self.tasks.lock().unwrap().push(task);
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, _args: &[std::ffi::OsString]) -> Result<i64, AppError> {
        panic!("health action fixtures must not submit Pueue tasks directly")
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(task) = tasks.iter_mut().find(|task| task.id == task_id) {
            task.state = "Killed".to_owned();
            task.ended_at = Some("201".to_owned());
        }
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("health action fixtures must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("health action fixtures must not provision Pueue groups")
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
    fake_pueue: FakePueue,
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

fn killed_task(group: &str, id: i64, enqueued_at: &str) -> PueueTask {
    PueueTask {
        state: "Killed".to_owned(),
        ended_at: Some("201".to_owned()),
        ..running_task(group, id, enqueued_at)
    }
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let harness = Self {
            _temp: temp,
            db,
            fake_pueue: FakePueue::with_tasks(vec![running_task("pa-project", 41, "100")]),
        };
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

    fn set_action_pending(&self, experiment_id: &str, recommended_action: &str) {
        let diagnosis = json!({
            "root_cause_class": "oom",
            "confidence": 0.9,
            "recommended_action": recommended_action,
            "summary": "gpu exhausted",
        });
        HealthRepository::store_diagnosis(&self.db, experiment_id, &diagnosis, 240).unwrap();
        HealthRepository::set_state(
            &self.db,
            experiment_id,
            HealthState::ActionPending,
            241,
        )
        .unwrap();
    }

    async fn reconcile_tasks(&self, tasks: Vec<PueueTask>, limits: &CampaignLimits, now: i64) {
        Reconciler::new(&self.db, FakePueue::with_tasks(tasks))
            .with_campaign_limits(*limits)
            .run_once_at(now)
            .await
            .unwrap();
    }

    fn pending_request_ids(&self) -> Vec<i64> {
        TerminationRequestRepository::new(&self.db)
            .find_pending("project-a")
            .unwrap()
            .into_iter()
            .map(|request| request.request_id)
            .collect()
    }

    async fn execute_pending(&self, limits: &CampaignLimits, now: i64) -> usize {
        let projects = ProjectRepository::new(&self.db).list_enabled().unwrap();
        HealthEngine::execute_pending(
            &self.db,
            &self.fake_pueue,
            &projects,
            limits,
            now,
        )
        .await
        .unwrap()
    }

    /// Drive the standard termination pipeline for the single open request:
    /// this is what the daemon's `run_termination` pass does every tick.
    async fn run_termination_pass(&self) {
        for request_id in self.pending_request_ids() {
            TerminationManager::new(&self.db, self.fake_pueue.clone())
                .execute(request_id)
                .await
                .unwrap();
        }
    }

    /// Promote a reserved resume successor to an accepted experiment bound to
    /// a fresh running Pueue task, as the daemon's reserved-intent dispatcher
    /// plus Pueue would.
    fn promote_successor_to_task(&self, experiment_id: &str, task_id: i64) {
        let experiments = ExperimentRepository::new(&self.db);
        experiments.mark_submitting(experiment_id, 500).unwrap();
        experiments
            .mark_accepted(
                experiment_id,
                task_id,
                &managed_task_run_signature(&running_task("pa-project", task_id, "100")).unwrap(),
                510,
            )
            .unwrap();
        self.fake_pueue.push_task(running_task("pa-project", task_id, "100"));
    }

    fn event_count(&self, kind: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM events WHERE project_id = ?1 AND kind = ?2",
                rusqlite::params!["project-a", kind],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn experiment_ids_resuming(&self, source_experiment_id: &str) -> Vec<String> {
        self.db
            .connect()
            .unwrap()
            .prepare("SELECT experiment_id FROM experiments WHERE resume_of_experiment_id = ?1 ORDER BY experiment_id")
            .unwrap()
            .query_map([source_experiment_id], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn submission_argv(&self, experiment_id: &str) -> Vec<String> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT s.argv_json FROM experiments e JOIN submissions s USING (submission_id)
                 WHERE e.experiment_id = ?1",
                [experiment_id],
                |row| row.get::<_, String>(0),
            )
            .map(|json| serde_json::from_str(&json).unwrap())
            .unwrap()
    }

    fn campaign_state(&self, campaign_id: &str) -> String {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state FROM campaigns WHERE campaign_id = ?1",
                [campaign_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn experiment_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM experiments", [], |row| row.get(0))
            .unwrap()
    }
}

#[tokio::test]
async fn continue_resets_to_healthy_without_any_kill() {
    let harness = Harness::new();
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(
            vec![running_task("pa-project", 41, "100")],
            &CampaignLimits::default(),
            200,
        )
        .await;
    harness.set_action_pending(&experiment_id, "continue");

    let executed = harness.execute_pending(&CampaignLimits::default(), 300).await;

    assert_eq!(executed, 1);
    let row = HealthRepository::get(&harness.db, &experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, HealthState::Healthy);
    assert_eq!(harness.fake_pueue.kill_calls(), Vec::<i64>::new());
    assert_eq!(harness.event_count("termination_failed"), 0);
}

#[tokio::test]
async fn diagnosed_kill_completes_full_chain_without_seeded_requests() {
    let harness = Harness::new();
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(
            vec![running_task("pa-project", 41, "100")],
            &CampaignLimits::default(),
            200,
        )
        .await;
    harness.set_action_pending(&experiment_id, "kill_and_resume");

    // No incident or termination request exists: the executor opens the
    // request itself instead of relying on the legacy kill-pattern route.
    let limits = CampaignLimits::default();
    let executed = harness.execute_pending(&limits, 300).await;

    assert_eq!(executed, 1, "the executor opened the termination request");
    assert_eq!(harness.pending_request_ids().len(), 1);
    assert!(
        harness.fake_pueue.kill_calls().is_empty(),
        "request creation must defer the kill to the termination pass"
    );
    assert_eq!(harness.event_count("termination_failed"), 0);

    // Repeat passes stay idempotent while the request is in flight.
    let executed = harness.execute_pending(&limits, 320).await;
    assert_eq!(executed, 0);
    assert_eq!(harness.pending_request_ids().len(), 1);

    // The standard termination pipeline performs the kill exactly once.
    harness.run_termination_pass().await;
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);

    // The killed projection confirms the request and inserts the successor.
    harness
        .reconcile_tasks(vec![killed_task("pa-project", 41, "100")], &limits, 400)
        .await;

    let old = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(old.status, ExperimentStatus::Cancelled);
    let successors = harness.experiment_ids_resuming(&experiment_id);
    assert_eq!(successors.len(), 1);
    let successor = ExperimentRepository::new(&harness.db)
        .find_by_id(&successors[0])
        .unwrap()
        .unwrap();
    assert_eq!(successor.status, ExperimentStatus::Reserved);
    assert_eq!(
        successor.parent_experiment_id.as_deref(),
        Some(experiment_id.as_str())
    );
    let checkpoint_note: Option<String> = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT checkpoint_note FROM experiments WHERE experiment_id = ?1",
            [&successors[0]],
            |row| row.get(0),
        )
        .unwrap();
    assert!(checkpoint_note.is_some());
    assert_eq!(
        harness.submission_argv(&successors[0]),
        harness.submission_argv(&experiment_id),
        "the successor must reuse the source argv"
    );
    assert!(
        HealthRepository::get(&harness.db, &experiment_id)
            .unwrap()
            .is_none(),
        "the terminal projection removes the consumed health row"
    );

    harness
        .reconcile_tasks(vec![killed_task("pa-project", 41, "100")], &limits, 420)
        .await;
    assert_eq!(
        harness.experiment_ids_resuming(&experiment_id).len(),
        1,
        "repeat projections must not duplicate the successor"
    );
    assert_eq!(harness.experiment_count(), 2);
}

#[tokio::test]
async fn confirmed_request_keeps_executor_from_reopening_the_pipeline() {
    let harness = Harness::new();
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(
            vec![running_task("pa-project", 41, "100")],
            &CampaignLimits::default(),
            200,
        )
        .await;
    harness.set_action_pending(&experiment_id, "kill_and_resume");

    let limits = CampaignLimits::default();
    harness.execute_pending(&limits, 300).await;
    let request_id = harness.pending_request_ids()[0];
    TerminationRequestRepository::new(&harness.db)
        .transition_status(request_id, pueue_agent::models::TerminationRequestStatus::Confirmed)
        .unwrap();

    let executed = harness.execute_pending(&limits, 340).await;

    assert_eq!(
        executed, 0,
        "a confirmed request defers to the terminal projection hook"
    );
    assert!(harness.fake_pueue.kill_calls().is_empty());
    assert_eq!(harness.event_count("termination_failed"), 0);
}

#[tokio::test]
async fn exhausted_live_repair_budget_escalates_instead_of_resubmitting() {
    let harness = Harness::new();
    fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
    let experiment_id = harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
    harness
        .reconcile_tasks(
            vec![running_task("pa-project", 41, "100")],
            &CampaignLimits::default(),
            200,
        )
        .await;
    harness.set_action_pending(&experiment_id, "kill_and_resume");

    let limits = CampaignLimits {
        max_live_repairs: 0,
        ..CampaignLimits::default()
    };
    harness.execute_pending(&limits, 250).await;
    harness.run_termination_pass().await;
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);

    harness
        .reconcile_tasks(vec![killed_task("pa-project", 41, "100")], &limits, 300)
        .await;

    assert_eq!(
        harness.experiment_count(),
        1,
        "exhausted live-repair budget must not resubmit"
    );
    assert_eq!(
        harness.campaign_state("campaign-health-a"),
        "degraded",
        "escalation degrades the campaign"
    );
    assert_eq!(harness.event_count("operator_wake"), 1);
}

/// The chain-depth cap counts every resume repair descended from the origin
/// experiment, so E→R1→R2 stops at `max_live_repairs = 2` even though each
/// generation only ever sees its direct child.
#[tokio::test]
async fn resume_chain_depth_is_capped_across_all_descendants() {
    for max_live_repairs in [0u32, 1, 2, 3] {
        let harness = Harness::new();
        fs::write(harness.log_path("project-a", 41), "epoch 1 loss 0.52\n").unwrap();
        let limits = CampaignLimits {
            max_live_repairs,
            max_same_spec_retries: 8,
            ..CampaignLimits::default()
        };
        let mut current_experiment =
            harness.accepted_campaign_experiment("project-a", "pa-project", "a", 41);
        let mut current_task = 41;
        let mut next_task = 42;
        harness
            .reconcile_tasks(vec![running_task("pa-project", 41, "100")], &limits, 200)
            .await;
        let mut now = 600;

        for generation in 0..=max_live_repairs {
            harness.set_action_pending(&current_experiment, "kill_and_resume");
            let executed = harness.execute_pending(&limits, now).await;
            assert_eq!(executed, 1, "generation {generation} opened its request");
            harness.run_termination_pass().await;
            assert_eq!(
                harness.fake_pueue.kill_calls().last(),
                Some(&current_task),
                "generation {generation} killed exactly its own task"
            );
            now += 100;
            harness
                .reconcile_tasks(
                    vec![killed_task("pa-project", current_task, "100")],
                    &limits,
                    now,
                )
                .await;

            let successors = harness.experiment_ids_resuming(&current_experiment);
            if generation < max_live_repairs {
                assert_eq!(
                    successors.len(),
                    1,
                    "chain depth {generation} < {max_live_repairs} must resume"
                );
                let successor = successors[0].clone();
                current_task = next_task;
                next_task += 1;
                harness.promote_successor_to_task(&successor, current_task);
                now += 100;
                harness
                    .reconcile_tasks(
                        vec![running_task("pa-project", current_task, "100")],
                        &limits,
                        now,
                    )
                    .await;
                current_experiment = successor;
            } else {
                assert_eq!(
                    successors.len(),
                    0,
                    "chain depth reached max_live_repairs={max_live_repairs}: must escalate"
                );
                assert_eq!(
                    harness.campaign_state("campaign-health-a"),
                    "degraded",
                    "depth-capped chains degrade the campaign"
                );
                assert_eq!(
                    harness.event_count("operator_wake"),
                    1,
                    "depth cap emits exactly one operator wake"
                );
            }
        }
    }
}

