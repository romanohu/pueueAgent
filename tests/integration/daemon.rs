use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[cfg(target_os = "linux")]
use std::process::Command;

use async_trait::async_trait;
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    db::{
        AgentRunRepository, CampaignRepository, Db, DecisionRepository,
        EventRepository, ExperimentRepository, InterventionRepository, ProjectRepository,
        StartCampaignRequest,
    },
    daemon::{Daemon, DaemonConfig, DaemonReport},
    execution_policy::CampaignLimits,
    interventions::InterventionStatus,
    models::{
        AgentContextMode, AgentRunStatus, CampaignState, DecisionAttemptState, DecisionCycleState,
        EventKind, EventStatus, ExperimentStatus, ExperimentTerminalOutcome, NewAgentRun, NewEvent,
        NewProject, ProposalKind,
    },
    proposals::{self, ProposalInput},
    pueue::{PueueApi, PueueTask},
    retry::RetryPolicy,
    AppError,
};

#[cfg(target_os = "linux")]
use pueue_agent::{
    agent::AgentHandle,
    db::{
        CodeChangeRepository, HealthRepository, ResearchRepository, TaskObservationRepository,
        ResearchLaunchBinding,
    },
    environment::PrivateRunTemp,
    models::{
        CodeChangeCheckStatus, ExecutionProjection, HealthState,
        NewTaskObservation, SignalSummaryEntry,
    },
};
#[cfg(target_os = "linux")]
use pueue_agent::research_evidence::build_research_evidence;

#[cfg(target_os = "linux")]
use pueue_agent::{
    code_change::{
        prepare_code_change_worktree_for_run, reopen_code_change_worktree_for_run,
        CodeChangeCoordinator, ProposedCheck, PYTHON_PYTEST_CHECK, RUST_CHECK, UV_PYTEST_CHECK,
    },
    config::{self, AgentConfig},
    execution_policy::{
        load_existing_policy, resolve_project_policy, NetworkMode, PolicyLoadInput,
        PolicyViolationCode, PolicyViolationDetail, PolicyViolationStage, ProjectRootAnchor,
        ResolvedProjectExecutionPolicy, StartupEnvironment, TempUnsafeReason,
    },
    models::{NewCodeChangeRun, Project},
};
use sha2::{Digest, Sha256};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
use std::os::unix::fs::{symlink, PermissionsExt};

#[cfg(unix)]
#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

#[derive(Clone)]
struct FakePueue {
    tasks: Arc<Mutex<Vec<PueueTask>>>,
    status_calls: Arc<Mutex<usize>>,
    add_calls: Arc<Mutex<Vec<Vec<OsString>>>>,
    kill_calls: Arc<Mutex<Vec<i64>>>,
    status_observed: Arc<Notify>,
    pause_next_add: Arc<AtomicBool>,
    add_observed: Arc<Notify>,
    add_release: Arc<Notify>,
}

impl FakePueue {
    fn with_tasks(tasks: Vec<PueueTask>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(tasks)),
            status_calls: Arc::new(Mutex::new(0)),
            add_calls: Arc::new(Mutex::new(Vec::new())),
            kill_calls: Arc::new(Mutex::new(Vec::new())),
            status_observed: Arc::new(Notify::new()),
            pause_next_add: Arc::new(AtomicBool::new(false)),
            add_observed: Arc::new(Notify::new()),
            add_release: Arc::new(Notify::new()),
        }
    }

    fn status_calls(&self) -> usize {
        *self.status_calls.lock().unwrap()
    }

    fn add_calls(&self) -> Vec<Vec<OsString>> {
        self.add_calls.lock().unwrap().clone()
    }

    fn set_tasks(&self, tasks: Vec<PueueTask>) {
        *self.tasks.lock().unwrap() = tasks;
    }

    fn kill_calls(&self) -> Vec<i64> {
        self.kill_calls.lock().unwrap().clone()
    }

    async fn wait_for_status(&self) {
        self.status_observed.notified().await;
    }

    fn pause_next_add(&self) {
        self.pause_next_add.store(true, Ordering::SeqCst);
    }

    async fn wait_for_add(&self) {
        self.add_observed.notified().await;
    }

    fn release_add(&self) {
        self.add_release.notify_one();
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        *self.status_calls.lock().unwrap() += 1;
        self.status_observed.notify_waiters();
        Ok(self.tasks.lock().unwrap().clone())
    }

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
        self.add_calls.lock().unwrap().push(args.to_vec());
        if self.pause_next_add.swap(false, Ordering::SeqCst) {
            self.add_observed.notify_one();
            self.add_release.notified().await;
        }
        let group = args
            .windows(2)
            .find(|pair| pair[0] == "-g")
            .map(|pair| pair[1].to_string_lossy().into_owned())
            .unwrap_or_default();
        let command = args
            .iter()
            .position(|argument| argument == "--")
            .map(|separator| {
                args[separator + 1..]
                    .iter()
                    .map(|argument| argument.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_default();
        self.tasks.lock().unwrap().push(PueueTask {
            id: 42,
            group,
            command,
            state: "Queued".to_owned(),
            enqueued_at: Some("200".to_owned()),
            started_at: None,
            ended_at: None,
            result: None,
        });
        Ok(42)
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.kill_calls.lock().unwrap().push(task_id);
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("daemon loop must not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("daemon loop must not provision Pueue groups")
    }
}

fn accepts_api<P: PueueApi>(_api: &P) {}

#[test]
fn daemon_fake_preserves_the_pueue_api_contract() {
    accepts_api(&FakePueue::with_tasks(Vec::new()));
}

struct DaemonHarness {
    temp: TempDir,
    db: Db,
    fake_pueue: FakePueue,
    now: i64,
}

impl DaemonHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        let fake_pueue = FakePueue::with_tasks(vec![running_task()]);
        let harness = Self {
            temp,
            db,
            fake_pueue,
            now: 200,
        };
        harness.register_project("project-a", "pa-project", "/bin/echo");
        harness
    }

    fn root(&self, project_id: &str) -> PathBuf {
        self.temp.path().join(project_id)
    }

    fn registered_root(&self, project_id: &str) -> PathBuf {
        ProjectRepository::new(&self.db)
            .find_by_id(project_id)
            .unwrap()
            .unwrap()
            .root_path
    }

    fn register_project(&self, project_id: &str, group: &str, program: &str) {
        self.register_project_with_agent(project_id, group, program, &["{prompt}"], 1);
    }

    fn register_project_with_agent(
        &self,
        project_id: &str,
        group: &str,
        program: &str,
        args: &[&str],
        timeout_minutes: u32,
    ) {
        let root = self.root(project_id);
        fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        #[cfg(unix)]
        {
            fs::set_permissions(root.join(".pueue-agent/logs"), fs::Permissions::from_mode(0o700))
                .unwrap();
            fs::set_permissions(root.join(".pueue-agent"), fs::Permissions::from_mode(0o700))
                .unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::write(
            root.join(".pueue-agent/logs/41.log"),
            "CUDA out of memory\n",
        )
        .unwrap();
        fs::write(root.join(".pueue-agent/STATE.md"), "state").unwrap();
        fs::write(root.join(".pueue-agent/instructions.md"), "instructions").unwrap();
        fs::write(
            root.join(".pueue-agent/config.toml"),
            format!(
                r#"
project_id = "{project_id}"
pueue_group = "{group}"

[agent]
program = "{program}"
args = [{args}]
timeout_minutes = {timeout_minutes}
max_retries = 1

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 4096
extra_log_paths = []

[[check.patterns]]
name = "oom"
regex = "CUDA out of memory"
action = "kill"
confirm_matches = 1

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
                args = toml_string_array(args),
            ),
        )
        .unwrap();

        if ProjectRepository::new(&self.db)
            .find_by_id(project_id)
            .unwrap()
            .is_some()
        {
            return;
        }

        ProjectRepository::new(&self.db)
            .register(&NewProject::new(
                project_id,
                &root,
                group,
                root.join(".pueue-agent/config.toml"),
                self.now,
            ))
            .unwrap();
    }

    fn daemon(&self) -> Daemon<FakePueue> {
        self.daemon_at(self.now)
    }

    fn runner(&self) -> AgentRunner {
        AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            self.policy(),
        )
    }

    fn policy(&self) -> Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy> {
        let projects = ProjectRepository::new(&self.db).list_all().unwrap();
        let owned = projects
            .iter()
            .map(|project| {
                (
                    project.project_id.clone(),
                    project.root_path.clone(),
                    execution_policy_fixture::prepare_configured_program(
                        self.temp.path(),
                        &project.project_id,
                        &project.config_path,
                    ),
                )
            })
            .collect::<Vec<_>>();
        let borrowed = owned
            .iter()
            .map(|(project_id, root, program)| (project_id.as_str(), root.as_path(), program.as_path()))
            .collect::<Vec<_>>();
        execution_policy_fixture::resolved_policy(self.temp.path(), &borrowed)
    }

    fn daemon_at(&self, now: i64) -> Daemon<FakePueue> {
        Daemon::new(
            self.db.clone(),
            self.fake_pueue.clone(),
            self.policy(),
            self.runner(),
            DaemonConfig {
                interval: Duration::from_millis(10),
                lease_seconds: 60,
                claim_limit: 100,
                now_override: Some(now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
    }

    fn running_task_with_deep_check_interval(interval_minutes: u32) -> Self {
        let harness = Self::new();
        let config_path = harness.root("project-a").join(".pueue-agent/config.toml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(
            &config_path,
            config.replace(
                "deep_check_interval_minutes = 0",
                &format!("deep_check_interval_minutes = {interval_minutes}"),
            ),
        )
        .unwrap();
        harness
    }

    async fn run_once_at(&self, now: i64) -> DaemonReport {
        self.daemon_at(now).run_once().await.unwrap()
    }

    fn campaign_experiment(&self) -> String {
        self.campaign_experiment_for(
            "project-a",
            "daemon-campaign",
            "daemon-campaign-experiment",
        )
    }

    fn campaign_experiment_for(
        &self,
        project_id: &str,
        campaign_id: &str,
        experiment_id: &str,
    ) -> String {
        let objective = pueue_agent::state::ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: format!("{campaign_id}-objective-digest"),
        };
        let argv = vec!["python".to_owned(), "train.py".to_owned()];
        let proposal = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish the initial campaign baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: "nested".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        fs::create_dir_all(self.root(project_id).join("nested")).unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id,
                    project_id,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: &format!("{campaign_id}-submission"),
                    experiment_id,
                    proposal_id: &format!("{campaign_id}-proposal"),
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap()
            .experiment
            .experiment_id
    }

    async fn restart_at(&self, now: i64) -> Result<DaemonReport, AppError> {
        self.daemon_at(now).run_once().await
    }

    fn enqueue(&self, kind: EventKind, project_id: &str, dedup_key: &str) -> i64 {
        EventRepository::new(&self.db)
            .insert_idempotent(&NewEvent::new(
                project_id,
                kind,
                dedup_key,
                json!({"source": "test"}),
                self.now,
                self.now,
            ))
            .unwrap()
            .event_id
    }

    fn event_status(&self, event_id: i64) -> EventStatus {
        EventRepository::new(&self.db)
            .find_by_id(event_id)
            .unwrap()
            .unwrap()
            .status
    }

    fn project_event_count(&self, kind: EventKind) -> i64 {
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

    fn insert_active_agent_run(&self) {
        let event_id = self.enqueue(EventKind::TaskFailed, "project-a", "active-agent-run");
        self.insert_active_run("project-a", event_id, AgentRunStatus::Running);
    }

    fn observation_count(&self) -> i64 {
        self.count("task_observations")
    }

    fn incident_count(&self) -> i64 {
        self.count("incidents")
    }

    fn agent_run_count(&self) -> u32 {
        AgentRunRepository::new(&self.db)
            .count_by_project("project-a")
            .unwrap()
    }

    fn agent_run_statuses(&self) -> Vec<AgentRunStatus> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare("SELECT status FROM agent_runs ORDER BY run_id")
            .unwrap();
        statement
            .query_map([], |row| row.get::<_, AgentRunStatus>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    async fn wait_for_active_agent(&self) {
        let repository = AgentRunRepository::new(&self.db);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if repository
                    .find_active_by_project("project-a")
                    .unwrap()
                    .is_some()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("agent should start");
    }

    #[cfg(unix)]
    async fn wait_for_native_dispatch(&self, event_id: i64) {
        // Native lifecycle readiness is production-bounded at 30 seconds. Give
        // the scheduler and SQLite status observation a small margin without
        // including this setup in the shutdown-deadline measurement below.
        let setup_timeout = Duration::from_secs(35);
        if tokio::time::timeout(setup_timeout, async {
            while self.event_status(event_id) != EventStatus::Dispatched {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_err()
        {
            let event_status = self.event_status(event_id);
            let run = AgentRunRepository::new(&self.db)
                .find_active_by_project("project-a")
                .unwrap();
            panic!(
                "native dispatch setup exceeded {setup_timeout:?}: event_status={event_status:?}, run_status={:?}, launch_gate_state={:?}",
                run.as_ref().map(|run| &run.status),
                run.as_ref().map(|run| run.launch_gate_state.as_str()),
            );
        }
    }

    fn count(&self, table: &str) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    fn pause_project(&self, project_id: &str) {
        ProjectRepository::new(&self.db)
            .pause(project_id, self.now)
            .unwrap();
    }

    fn claim_with_lease(&self, event_id: i64, lease_until: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events SET status = 'claimed', lease_until = ?1 WHERE event_id = ?2",
                rusqlite::params![lease_until, event_id],
            )
            .unwrap();
    }

    fn insert_active_run(
        &self,
        project_id: &str,
        primary_event_id: i64,
        status: AgentRunStatus,
    ) -> i64 {
        AgentRunRepository::new(&self.db)
            .insert(&NewAgentRun::with_context(
                project_id,
                primary_event_id,
                (status == AgentRunStatus::Running).then_some(42_424),
                status,
                self.now - 10,
                self.registered_root(project_id).join(format!(
                    ".pueue-agent/logs/agent-190-{primary_event_id}.log"
                )),
                AgentContextMode::Fresh,
                None,
                vec![primary_event_id.to_string()],
            ))
            .unwrap()
            .run_id
    }

    fn agent_run_state(&self, run_id: i64) -> (AgentRunStatus, Option<i64>, Option<String>) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, finished_at, last_error FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn reserve_intervention(&self, message: &str, token: &str, lease_expires_at: i64) -> String {
        let repository = InterventionRepository::new(&self.db);
        let intervention_id = repository
            .insert_pending("project-a", message, self.now - 20)
            .unwrap()
            .intervention_id;
        repository
            .reserve_pending(
                "project-a",
                token,
                self.now - 10,
                lease_expires_at,
                1,
                message.len(),
            )
            .unwrap();
        intervention_id
    }

    fn attach_intervention(&self, intervention_id: &str, run_id: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE interventions SET agent_run_id = ?1
                 WHERE project_id = 'project-a' AND intervention_id = ?2",
                rusqlite::params![run_id, intervention_id],
            )
            .unwrap();
    }

    fn intervention_state(&self, intervention_id: &str) -> (InterventionStatus, Option<i64>) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, agent_run_id FROM interventions WHERE intervention_id = ?1",
                [intervention_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }
}

fn toml_string_array(values: &[&str]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

fn running_task() -> PueueTask {
    PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    }
}

#[cfg(target_os = "linux")]
fn running_task_for(id: i64, group: &str) -> PueueTask {
    let mut task = running_task();
    task.id = id;
    task.group = group.to_owned();
    task
}

#[tokio::test]
async fn decision_recovery_marks_a_terminal_agent_without_output_missing_before_scheduling() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();
    let experiments = ExperimentRepository::new(&harness.db);
    experiments.mark_submitting(&experiment_id, 110).unwrap();
    experiments
        .mark_accepted(
            &experiment_id,
            42,
            "pueue-task:v1:decision-recovery-source",
            120,
        )
        .unwrap();
    experiments
        .project_terminal_submission(
            &experiment_id,
            42,
            ExperimentTerminalOutcome::Succeeded,
            150,
        )
        .unwrap();
    let decisions = DecisionRepository::new(&harness.db);
    let cycle = decisions
        .ensure_cycle_for_terminal("daemon-campaign", &experiment_id, 160)
        .unwrap();
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle.cycle_id, 170)
        .unwrap()
        .unwrap();
    let campaign = CampaignRepository::new(&harness.db)
        .find_by_id("daemon-campaign")
        .unwrap()
        .unwrap();
    let source = experiments.find_by_id(&experiment_id).unwrap().unwrap();
    let context_json = json!({
        "schema_version": 1,
        "objective": {
            "text": campaign.objective_text,
            "digest": campaign.objective_digest,
        },
        "source_experiment": {
            "experiment_id": source.experiment_id,
            "proposal_id": source.proposal_id,
            "proposal_kind": "experiment",
            "status": source.status,
            "attempt": source.attempt,
            "command_digest": "daemon-decision-command-digest",
            "failure_code": source.failure_code,
            "failure_fingerprint": source.failure_fingerprint,
            "created_at": source.created_at,
            "updated_at": source.updated_at,
            "finished_at": source.finished_at,
        },
        "terminal_observation": {
            "task_id": source.pueue_task_id,
            "task_signature": source.task_signature,
            "state": "succeeded",
            "enqueued_at": 110,
            "started_at": 120,
            "ended_at": 150,
            "exit_code": 0,
        },
        "recent_outcomes": {"proposals": [], "experiments": []},
        "budgets": {
            "campaign_state": campaign.state,
            "next_eligible_at": null,
            "rolling_usage": {},
            "experiment_counts": {},
        },
        "intervention": {"pending": []},
        "artifact_hints": [],
    })
    .to_string();
    let context_digest = format!("{:x}", Sha256::digest(context_json.as_bytes()));
    decisions
        .store_evidence(&reservation, &context_json, &context_digest, 171)
        .unwrap();
    let event = NewEvent::new(
        "project-a",
        EventKind::CampaignDecision,
        format!("campaign-decision:v1:{}", cycle.cycle_id),
        json!({
            "source": "terminal_experiment",
            "cycle_id": cycle.cycle_id,
            "source_experiment_id": experiment_id,
        }),
        172,
        172,
    )
    .with_campaign_lineage("daemon-campaign", Some(experiment_id.clone()));
    let (_, event) = decisions
        .publish_terminal_cycle_event("daemon-campaign", &experiment_id, &event, 172)
        .unwrap();
    EventRepository::new(&harness.db)
        .claim_batch(172, 232, 1)
        .unwrap()
        .into_iter()
        .find(|claimed| claimed.event_id == event.event_id)
        .expect("exact campaign decision event must be claimed");
    let run_id = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::with_context(
                "project-a",
                event.event_id,
                Some(42_424),
                AgentRunStatus::Running,
                173,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-173-{}.log",
                    event.event_id
                )),
                AgentContextMode::Fresh,
                None,
                vec![cycle.cycle_id.clone()],
            ),
            &[event.event_id],
        )
        .unwrap()
        .run_id;
    decisions
        .bind_agent_run(&reservation, run_id, 173)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER keep_recovered_decision_not_due
             AFTER UPDATE OF status ON events
             WHEN OLD.event_id = {} AND NEW.status = 'pending'
             BEGIN
                 UPDATE events SET not_before = 260 WHERE event_id = NEW.event_id;
             END;",
            event.event_id
        ))
        .unwrap();

    let report = harness.restart_at(200).await.unwrap();

    let (attempt_state, failure_code): (DecisionAttemptState, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT state, failure_code FROM decision_attempts
             WHERE cycle_id = ?1 AND attempt_number = ?2",
            rusqlite::params![reservation.cycle_id, reservation.attempt_number],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(attempt_state, DecisionAttemptState::Failed);
    assert_eq!(failure_code.as_deref(), Some("decision_missing"));
    assert_eq!(report.decision_recovery.missing, 1);
    assert_eq!(
        DecisionRepository::new(&harness.db)
            .find_cycle_for_source("daemon-campaign", &experiment_id)
            .unwrap()
            .unwrap()
            .state,
        DecisionCycleState::Pending
    );
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, attempts FROM events WHERE event_id = ?1",
                [event.event_id],
                |row| Ok((row.get::<_, EventStatus>(0)?, row.get::<_, i64>(1)?)),
            )
            .unwrap(),
        (EventStatus::Pending, 0)
    );
    assert_eq!(harness.agent_run_count(), 1);
}

#[tokio::test]
async fn campaign_recovery_resumes_only_reserved_intents_with_the_stored_working_directory() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();

    harness.restart_at(200).await.unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Accepted);
    assert_eq!(harness.count("experiments"), 1);
    let add_calls = harness.fake_pueue.add_calls();
    assert_eq!(add_calls.len(), 1);
    let expected_runtime = pueue_agent::environment::campaign_experiment_runtime_argv(
        &harness.registered_root("project-a"),
        "daemon-campaign",
        "daemon-campaign-experiment",
        &["python".to_owned(), "train.py".to_owned()],
    );
    let mut expected = vec![
        OsString::from("-g"),
        OsString::from("pa-project"),
        OsString::from("--working-directory"),
        harness.registered_root("project-a").join("nested").into_os_string(),
        OsString::from("--"),
    ];
    expected.extend(expected_runtime);
    assert_eq!(add_calls[0], expected);
}

#[tokio::test]
async fn campaign_recovery_defers_paused_intent_then_dispatches_once_after_resume() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();
    CampaignRepository::new(&harness.db)
        .pause("project-a", 150)
        .unwrap();

    harness.restart_at(200).await.unwrap();

    assert!(harness.fake_pueue.add_calls().is_empty());
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved
    );

    CampaignRepository::new(&harness.db)
        .resume("project-a", 201)
        .unwrap();
    harness.restart_at(202).await.unwrap();

    assert_eq!(harness.fake_pueue.add_calls().len(), 1);
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
}

async fn assert_campaign_recovery_defers_post_snapshot_authority_loss(authority_loss: &str) {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pa-project-b", "/bin/echo");
    harness.campaign_experiment();
    let deferred_experiment = harness.campaign_experiment_for(
        "project-b",
        "zz-daemon-campaign",
        "zz-daemon-campaign-experiment",
    );
    harness.fake_pueue.pause_next_add();
    let mut daemon = harness.daemon_at(200);
    let first_tick = tokio::spawn(async move {
        let result = daemon.run_once().await;
        (daemon, result)
    });
    harness.fake_pueue.wait_for_add().await;

    match authority_loss {
        "pause" => {
            ProjectRepository::new(&harness.db)
                .pause("project-b", 201)
                .unwrap();
        }
        "disable" => {
            ProjectRepository::new(&harness.db)
                .disable("project-b", 201, &[])
                .unwrap();
        }
        "budget_waiting" => {
            let campaigns = CampaignRepository::new(&harness.db);
            for index in 0..=6 {
                campaigns
                    .reserve_agent_decision(
                        "zz-daemon-campaign",
                        &format!("post-snapshot-decision-{index}"),
                        &CampaignLimits::default(),
                        201,
                    )
                    .unwrap();
            }
            assert_eq!(
                campaigns
                    .find_by_id("zz-daemon-campaign")
                    .unwrap()
                    .unwrap()
                    .state,
                CampaignState::BudgetWaiting
            );
        }
        _ => unreachable!(),
    }
    harness.fake_pueue.release_add();

    let (mut daemon, result) = first_tick.await.unwrap();
    result.unwrap();
    assert_eq!(
        ExperimentRepository::new(&harness.db)
            .find_by_id(&deferred_experiment)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Reserved,
    );
    assert_eq!(
        harness
            .fake_pueue
            .add_calls()
            .iter()
            .filter(|args| args.get(1).is_some_and(|group| group == "pa-project-b"))
            .count(),
        0,
    );
    daemon.run_once().await.unwrap();
}

#[tokio::test]
async fn campaign_recovery_defers_post_snapshot_pause_without_stopping_the_daemon() {
    assert_campaign_recovery_defers_post_snapshot_authority_loss("pause").await;
}

#[tokio::test]
async fn campaign_recovery_defers_post_snapshot_disable_without_stopping_the_daemon() {
    assert_campaign_recovery_defers_post_snapshot_authority_loss("disable").await;
}

#[tokio::test]
async fn campaign_recovery_defers_post_snapshot_budget_wait_without_stopping_the_daemon() {
    assert_campaign_recovery_defers_post_snapshot_authority_loss("budget_waiting").await;
}

#[tokio::test]
async fn campaign_recovery_quarantines_submitting_without_readding() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&experiment_id, 150)
        .unwrap();

    harness.restart_at(200).await.unwrap();

    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Unreconciled);
    assert!(harness.fake_pueue.add_calls().is_empty());
    assert_eq!(harness.count("experiments"), 1);
}

#[tokio::test]
async fn campaign_recovery_never_readds_unreconciled_or_accepted_intents() {
    for target in [ExperimentStatus::Unreconciled, ExperimentStatus::Accepted] {
        let harness = DaemonHarness::new();
        let experiment_id = harness.campaign_experiment();
        let experiments = ExperimentRepository::new(&harness.db);
        experiments.mark_submitting(&experiment_id, 150).unwrap();
        match target {
            ExperimentStatus::Unreconciled => {
                experiments
                    .mark_unreconciled(&experiment_id, "pueue_add_unknown", 151)
                    .unwrap();
            }
            ExperimentStatus::Accepted => {
                experiments
                    .mark_accepted(
                        &experiment_id,
                        42,
                        "provisional-submit:v1:group=pa-project:task-id=42:intent=daemon-campaign-submission",
                        151,
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }

        harness.restart_at(200).await.unwrap();

        assert_eq!(
            ExperimentRepository::new(&harness.db)
                .find_by_id(&experiment_id)
                .unwrap()
                .unwrap()
                .status,
            target
        );
        assert!(harness.fake_pueue.add_calls().is_empty());
        assert_eq!(harness.count("experiments"), 1);
    }
}

#[tokio::test]
async fn campaign_budget_wake_ignores_zero_code_change_limit_without_reservations() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();
    let experiments = ExperimentRepository::new(&harness.db);
    experiments.mark_submitting(&experiment_id, 100).unwrap();
    experiments
        .mark_accepted(
            &experiment_id,
            42,
            "provisional-submit:v1:group=pa-project:task-id=42:intent=daemon-campaign-submission",
            100,
        )
        .unwrap();
    let campaigns = CampaignRepository::new(&harness.db);
    for index in 0..6 {
        campaigns
            .reserve_agent_decision(
                "daemon-campaign",
                &format!("decision-{index}"),
                &CampaignLimits::default(),
                100,
            )
            .unwrap();
    }
    campaigns
        .reserve_agent_decision(
            "daemon-campaign",
            "decision-seven",
            &CampaignLimits::default(),
            100,
        )
        .unwrap();
    let waiting = campaigns
        .find_by_id("daemon-campaign")
        .unwrap()
        .unwrap();
    assert_eq!(waiting.state, CampaignState::BudgetWaiting);
    assert_eq!(waiting.next_eligible_at, Some(3_700));

    let mut policy = harness.policy();
    Arc::get_mut(&mut policy)
        .unwrap()
        .campaign_limits
        .max_code_change_proposals_per_24h = 0;
    let runner = AgentRunner::new(
        AgentRunnerConfig::production()
            .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
        policy.clone(),
    );
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        policy,
        runner,
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(3_700),
            shutdown_grace_period: Duration::from_secs(30),
        },
    );

    daemon.run_once().await.unwrap();

    let campaign = campaigns
        .find_by_id("daemon-campaign")
        .unwrap()
        .unwrap();
    assert_eq!(campaign.state, CampaignState::Active);
    assert_eq!(campaign.next_eligible_at, None);
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: std::os::raw::c_int, signal: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    unsafe { kill(pid, 0) == 0 }
}

#[cfg(unix)]
fn create_cleanup_depth_overflow(run_temp: &PathBuf) -> PathBuf {
    let mut nested = run_temp.clone();
    for index in 0..=pueue_agent::environment::MAX_PRIVATE_TEMP_CLEANUP_DEPTH + 1 {
        nested.push(format!("cleanup-level-{index}"));
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::write(nested.join("retained-leaf"), b"owned").unwrap();
    nested.parent().unwrap().to_path_buf()
}

#[cfg(target_os = "linux")]
fn compile_sleeping_codex_fixture(target: &std::path::Path) {
    let source = target.with_extension("rs");
    fs::write(
        &source,
        r#"
use std::{process, thread, time::Duration};

fn main() {
    thread::sleep(Duration::from_secs(2));
    process::exit(17);
}
"#,
    )
    .unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated sleeping Codex fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(target_os = "linux")]
struct ResearchCrashFixturePaths {
    invocation: PathBuf,
    target_ready: PathBuf,
    target_release: PathBuf,
    descendant_pid: PathBuf,
    descendant_ready: PathBuf,
    descendant_release: PathBuf,
    controller_ready: PathBuf,
    crash_now: PathBuf,
}

#[cfg(target_os = "linux")]
impl ResearchCrashFixturePaths {
    fn new(root: &Path) -> Self {
        Self {
            invocation: root.join("research-crash-fixture.invocation"),
            target_ready: root.join("research-crash-fixture.target-ready"),
            target_release: root.join("research-crash-fixture.target-release"),
            descendant_pid: root.join("research-crash-fixture.descendant-pid"),
            descendant_ready: root.join("research-crash-fixture.descendant-ready"),
            descendant_release: root.join("research-crash-fixture.descendant-release"),
            controller_ready: root.join("research-crash-fixture.controller-ready"),
            crash_now: root.join("research-crash-fixture.crash-now"),
        }
    }
}

#[cfg(target_os = "linux")]
fn compile_research_crash_codex_fixture(target: &Path, paths: &ResearchCrashFixturePaths) {
    let source = target.with_extension("rs");
    fs::write(
        &source,
        format!(
            r#"use std::{{env, fs, process::Command, thread, time::Duration}};

fn wait_for(path: &str) {{
    while !std::path::Path::new(path).is_file() {{
        thread::sleep(Duration::from_millis(10));
    }}
}}

fn main() {{
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("--fixture-descendant") {{
        fs::write({descendant_ready:?}, "ready").unwrap();
        wait_for({descendant_release:?});
        return;
    }}

    if !std::path::Path::new({invocation:?}).exists() {{
        fs::write({invocation:?}, "first").unwrap();
        let child = Command::new(env::current_exe().unwrap())
            .arg("--fixture-descendant")
            .spawn()
            .unwrap();
        fs::write({descendant_pid:?}, child.id().to_string()).unwrap();
        fs::write({target_ready:?}, "ready").unwrap();
        wait_for({target_release:?});
    }}
}}
"#,
            invocation = paths.invocation.display().to_string(),
            target_ready = paths.target_ready.display().to_string(),
            target_release = paths.target_release.display().to_string(),
            descendant_pid = paths.descendant_pid.display().to_string(),
            descendant_ready = paths.descendant_ready.display().to_string(),
            descendant_release = paths.descendant_release.display().to_string(),
        ),
    )
    .unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated research crash Codex failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(target_os = "linux")]
fn prepare_healthy_research_fixture(harness: &DaemonHarness) {
    prepare_healthy_research_fixture_for(harness, "project-a", 41);
}

#[cfg(target_os = "linux")]
fn prepare_healthy_research_fixture_for(harness: &DaemonHarness, project_id: &str, task_id: i64) {
    let config_path = harness.root(project_id).join(".pueue-agent/config.toml");
    let body = fs::read_to_string(&config_path).unwrap();
    assert!(body.contains(r#"program = "/bin/echo""#));
    let body = body.replace(r#"program = "/bin/echo""#, r#"program = "codex""#);
    let oom_pattern = r#"[[check.patterns]]
name = "oom"
regex = "CUDA out of memory"
action = "kill"
confirm_matches = 1

"#;
    assert!(body.contains(oom_pattern));
    let body = body.replace(oom_pattern, "");
    fs::write(&config_path, body).unwrap();
    let config = config::load(&config_path).unwrap();
    assert_eq!(config.agent.program, "codex");
    assert!(config.check.patterns.is_empty());
    fs::write(
        harness
            .root(project_id)
            .join(format!(".pueue-agent/logs/{task_id}.log")),
        "validation loss 0.52\n",
    )
    .unwrap();
}

#[cfg(target_os = "linux")]
fn research_policy_with_codex_fixture(
    harness: &DaemonHarness,
    codex: &std::path::Path,
) -> Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy> {
    // Reuse the daemon fixture's policy setup so the bootstrap launcher stays
    // the real descriptor-bound pueue-agent executable. Only the built-in
    // Codex executable is replaced with the bounded sleeping fixture.
    let _ = harness.policy();
    let fixture_root = fs::canonicalize(harness.temp.path()).unwrap();
    let state_dir = fixture_root.join("execution-policy-state");
    let policy_path = state_dir.join("execution-policy.toml");
    let body = fs::read_to_string(&policy_path).unwrap();
    let replacement = format!("codex = {:?}", codex.display().to_string());
    let mut replaced = false;
    let body = body
        .lines()
        .map(|line| {
            if !replaced && line.starts_with("codex = ") {
                replaced = true;
                replacement.clone()
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(replaced, "policy fixture must declare a built-in Codex executable");
    fs::write(&policy_path, format!("{body}\n")).unwrap();
    fs::set_permissions(&policy_path, fs::Permissions::from_mode(0o600)).unwrap();

    research_policy_from_fixture_paths(&harness.db, &fixture_root)
}

#[cfg(target_os = "linux")]
fn research_policy_from_fixture_paths(
    db: &Db,
    fixture_root: &Path,
) -> Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy> {
    let fixture_root = fs::canonicalize(fixture_root).unwrap();
    let state_dir = fixture_root.join("execution-policy-state");
    let trusted_dir = fixture_root.join("execution-policy-bin");

    let project_roots = ProjectRepository::new(db)
        .list_all()
        .unwrap()
        .into_iter()
        .map(|project| project.root_path)
        .collect();
    let inherited_path = std::env::join_paths([trusted_dir.clone()]).unwrap();
    let policy = load_existing_policy(&PolicyLoadInput {
        state_dir,
        project_roots,
        inherited_path,
        startup_environment: StartupEnvironment::from_pairs([
            ("HOME", "/fixture"),
            ("AWS_SECRET_ACCESS_KEY", "fixture-aws-secret"),
            ("WANDB_API_KEY", "fixture-wandb-key"),
            ("SSH_AUTH_SOCK", "/fixture/ssh-agent.sock"),
        ]),
        codex_home: fixture_root.join("execution-policy-codex-home"),
        pueue_config: fixture_root.join("execution-policy-pueue.yml"),
        launcher_path: trusted_dir.join("pueue-agent-launcher"),
    })
    .unwrap();
    Arc::new(policy)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn startup_temp_inventory_rejects_symlink_weak_and_over_limit_without_mutation() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    {
        let harness = DaemonHarness::new();
        harness.register_project_with_agent(
            "project-b",
            "pb-project",
            "/bin/sh",
            &["-c", "sleep 1"],
            1,
        );
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let outside = harness.temp.path().join("outside-retained");
        fs::create_dir(&outside).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
        let symlinked = tmp.join("999");
        symlink(&outside, &symlinked).unwrap();
        let symlink_event =
            harness.enqueue(EventKind::TaskFailed, "project-a", "startup-symlink");
        let unrelated_event =
            harness.enqueue(EventKind::TaskFailed, "project-b", "startup-unrelated");

        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(symlink_event), EventStatus::DeadLetter);
        assert_eq!(harness.event_status(unrelated_event), EventStatus::Dispatched);
        assert!(symlinked.is_symlink());
        assert!(outside.is_dir());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while harness.event_status(unrelated_event) != EventStatus::Completed {
            assert!(
                tokio::time::Instant::now() < deadline,
                "unrelated project did not drain"
            );
            daemon.run_once().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    {
        let harness = DaemonHarness::new();
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        let weak = tmp.join("999");
        fs::create_dir(&weak).unwrap();
        fs::set_permissions(&weak, fs::Permissions::from_mode(0o755)).unwrap();
        let weak_event = harness.enqueue(EventKind::TaskFailed, "project-a", "startup-weak");
        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(weak_event), EventStatus::DeadLetter);
        assert_eq!(
            fs::metadata(&weak).unwrap().permissions().mode() & 0o777,
            0o755
        );
        assert!(weak.is_dir());
    }

    {
        let harness = DaemonHarness::new();
        let tmp = harness.root("project-a").join(".pueue-agent/tmp");
        fs::create_dir_all(&tmp).unwrap();
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
        for run_id in 1..=pueue_agent::environment::MAX_PRIVATE_TEMP_GENERATIONS + 1 {
            let generation = tmp.join(run_id.to_string());
            fs::create_dir(&generation).unwrap();
            fs::set_permissions(&generation, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let over_limit_count = fs::read_dir(&tmp).unwrap().count();
        let over_limit_event =
            harness.enqueue(EventKind::TaskFailed, "project-a", "startup-over-limit");
        let mut daemon = harness.daemon_at(harness.now);
        daemon.run_once().await.unwrap();
        assert_eq!(harness.event_status(over_limit_event), EventStatus::DeadLetter);
        assert_eq!(fs::read_dir(&tmp).unwrap().count(), over_limit_count);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn daemon_keeps_running_after_temp_inventory_violation_until_cancel() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let tmp = harness.root("project-a").join(".pueue-agent/tmp");
    fs::create_dir_all(&tmp).unwrap();
    fs::set_permissions(&tmp, fs::Permissions::from_mode(0o700)).unwrap();
    let outside = harness.temp.path().join("outside-daemon-run");
    fs::create_dir(&outside).unwrap();
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
    let retained = tmp.join("999");
    symlink(&outside, &retained).unwrap();
    let unsafe_event = harness.enqueue(EventKind::TaskFailed, "project-a", "daemon-unsafe");
    let unrelated_event = harness.enqueue(EventKind::TaskFailed, "project-b", "daemon-unrelated");

    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let mut daemon = harness.daemon_at(harness.now);
    let task = tokio::spawn(async move { daemon.run(task_shutdown).await });
    // Native lifecycle setup includes compiling the generated fixture agent;
    // use the same production-bounded readiness contract as the other daemon
    // lifecycle tests instead of a short wall-clock bound around setup.
    harness.wait_for_native_dispatch(unrelated_event).await;
    let completion = tokio::time::timeout(Duration::from_secs(35), async {
        while harness.event_status(unrelated_event) != EventStatus::Completed {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    completion.expect("unrelated project should complete while daemon remains running");
    assert_eq!(harness.event_status(unsafe_event), EventStatus::DeadLetter);
    assert!(!task.is_finished());

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .expect("daemon should stop after explicit cancellation")
        .unwrap()
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_pending_project_defers_without_attempt_while_other_project_dispatches() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let first_event = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-blocked-first");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);

    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(first_event) != EventStatus::Completed {
        assert!(Instant::now() < terminal_deadline, "project-a did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_subtree.is_dir());

    let blocked_event = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-blocked-new");
    let other_event = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-blocked-other");
    let intervention_id = InterventionRepository::new(&harness.db)
        .insert_pending("project-a", "must remain pending", harness.now)
        .unwrap();

    daemon.run_once().await.unwrap();
    let blocked = EventRepository::new(&harness.db)
        .find_by_id(blocked_event)
        .unwrap()
        .unwrap();
    assert_eq!(blocked.status, EventStatus::RetryWait);
    assert_eq!(blocked.not_before, 260);
    assert_eq!(blocked.attempts, 0);
    assert_eq!(harness.event_status(other_event), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let intervention = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id, attempts FROM interventions WHERE intervention_id = ?1",
            [intervention_id.intervention_id.as_str()],
            |row| Ok((
                row.get::<_, pueue_agent::interventions::InterventionStatus>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            )),
        )
        .unwrap();
    assert_eq!(
        intervention,
        (
            pueue_agent::interventions::InterventionStatus::Pending,
            None,
            0,
        )
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_retry_is_fair_across_projects_and_finishes_after_fault_removal() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let event_a = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-fair-a");
    let event_b = harness.enqueue(EventKind::TaskFinished, "project-b", "cleanup-fair-b");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let run_id = |project_id: &str| {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT run_id FROM agent_runs WHERE project_id = ?1 ORDER BY run_id DESC LIMIT 1",
                [project_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let run_a = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id("project-a").to_string());
    let run_b = harness
        .root("project-b")
        .join(".pueue-agent/tmp")
        .join(run_id("project-b").to_string());
    let overflow_a = create_cleanup_depth_overflow(&run_a);
    let overflow_b = create_cleanup_depth_overflow(&run_b);

    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(event_a) != EventStatus::Completed
        || harness.event_status(event_b) != EventStatus::Completed
    {
        assert!(Instant::now() < terminal_deadline, "agents did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_a.is_dir());
    assert!(overflow_b.is_dir());

    for project_id in ["project-a", "project-b"] {
        let config_path = harness.root(project_id).join(".pueue-agent/config.toml");
        let config = fs::read_to_string(&config_path).unwrap();
        fs::write(config_path, config.replace("sleep 1", "sleep 30")).unwrap();
    }

    let retry_event_a = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-fair-retry-a");
    let retry_event_b = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-fair-retry-b");
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(retry_event_a), EventStatus::RetryWait);
    assert_eq!(harness.event_status(retry_event_b), EventStatus::RetryWait);

    fs::remove_dir_all(overflow_a).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_a.join("cleanup-level-0").exists());
    assert!(run_b.join("cleanup-level-0").is_dir());

    fs::remove_dir_all(overflow_b).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_b.join("cleanup-level-0").exists());
    let mut resumed = harness.daemon_at(260);
    resumed.run_once().await.unwrap();
    assert_eq!(harness.event_status(retry_event_a), EventStatus::Dispatched);
    assert_eq!(harness.event_status(retry_event_b), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id IN ('project-a', 'project-b')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        4
    );
    for project_id in ["project-a", "project-b"] {
        assert_eq!(
            harness
                .db
                .connect()
                .unwrap()
                .query_row(
                    "SELECT COUNT(*) FROM agent_runs WHERE project_id = ?1",
                    [project_id],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            2,
            "each project retains its original terminal run and one retry run",
        );
    }
    let retry_bindings = harness
        .db
        .connect()
        .unwrap()
        .prepare(
            "SELECT events.project_id, MAX(agent_run_events.run_id)
             FROM events
             JOIN agent_run_events
               ON agent_run_events.project_id = events.project_id
              AND agent_run_events.event_id = events.event_id
             WHERE events.event_id IN (?1, ?2)
             GROUP BY events.event_id
             ORDER BY events.event_id",
        )
        .unwrap()
        .query_map(rusqlite::params![retry_event_a, retry_event_b], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(retry_bindings.len(), 2);
    assert_eq!(retry_bindings[0].0, "project-a");
    assert_eq!(retry_bindings[1].0, "project-b");
    assert!(retry_bindings.iter().all(|(_, run_id)| run_id.is_some()));
    assert_ne!(retry_bindings[0].1, retry_bindings[1].1);
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_retains_temp_cleanup_when_deadline_expires() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "cleanup-shutdown-deadline");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(150),
        },
    );
    daemon.run_once().await.unwrap();

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);
    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    while harness.event_status(event_id) != EventStatus::Completed {
        assert!(Instant::now() < terminal_deadline, "agent did not reach terminal persistence");
        daemon.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(overflow_subtree.is_dir());

    let shutdown = CancellationToken::new();
    shutdown.cancel();
    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("shutdown cleanup must remain bounded");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
    daemon.run_once().await.unwrap();
    assert!(!run_temp.join("cleanup-level-0").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn bound_cleanup_pending_project_defers_without_attempt_while_other_project_dispatches() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let initial_event = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-pending-first");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_bound_cleanup_pending_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-a' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup pending dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_bound_cleanup_pending_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup pending finalizer failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(initial_event), EventStatus::InFlight);

    let run_id = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs WHERE project_id = 'project-a' ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_bound_cleanup_pending_dispatch_ack;
             DROP TRIGGER reject_bound_cleanup_pending_finalizer;",
        )
        .unwrap();

    let blocked_event = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-pending-new");
    let other_event = harness.enqueue(EventKind::TaskFailed, "project-b", "bound-cleanup-pending-other");
    let intervention_id = InterventionRepository::new(&harness.db)
        .insert_pending("project-a", "must remain pending", harness.now)
        .unwrap();

    assert!(daemon.run_once().await.is_ok());
    let blocked = EventRepository::new(&harness.db)
        .find_by_id(blocked_event)
        .unwrap()
        .unwrap();
    assert_eq!(blocked.status, EventStatus::RetryWait);
    assert_eq!(blocked.not_before, 260);
    assert_eq!(blocked.attempts, 0);
    assert_eq!(harness.event_status(other_event), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let intervention = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, agent_run_id, attempts FROM interventions WHERE intervention_id = ?1",
            [intervention_id.intervention_id.as_str()],
            |row| Ok((
                row.get::<_, pueue_agent::interventions::InterventionStatus>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, i64>(2)?,
            )),
        )
        .unwrap();
    assert_eq!(
        intervention,
        (
            pueue_agent::interventions::InterventionStatus::Pending,
            None,
            0,
        )
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("bound cleanup owner shutdown must remain bounded")
        .unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn second_daemon_defers_research_retry_until_first_cleanup_owner_releases() {
    let harness = DaemonHarness::new();
    prepare_healthy_research_fixture(&harness);
    harness.register_project("project-b", "pb-project", "codex");
    let experiment_id = harness.campaign_experiment();
    let task = running_task();
    let live_task_signature = pueue_agent::reconcile::task_signature(&task);
    let task_signature = pueue_agent::reconcile::managed_task_run_signature(&task)
        .expect("managed research task identity");
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&experiment_id, 190)
        .unwrap();
    ExperimentRepository::new(&harness.db)
        .mark_accepted(&experiment_id, task.id, &task_signature, 191)
        .unwrap();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            &live_task_signature,
            task.id,
            &task.group,
            vec![task.command.clone()],
            "Running",
            Some(100),
            Some(101),
            None,
            None,
            harness.now,
        ))
        .unwrap();
    ResearchRepository::new(&harness.db)
        .ensure_campaign("daemon-campaign")
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaign_research SET next_due_at = ?1 WHERE campaign_id = ?2",
            rusqlite::params![harness.now, "daemon-campaign"],
        )
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .claim_due(
            "daemon-campaign",
            &experiment_id,
            &task_signature,
            harness.now,
        )
        .unwrap()
        .expect("the seeded running campaign must claim one research review");
    let experiment = ExperimentRepository::new(&harness.db)
        .find_by_id(&experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(experiment.status, ExperimentStatus::Accepted);
    assert_eq!(experiment.task_signature.as_deref(), Some(task_signature.as_str()));
    assert_eq!(review.experiment_id, experiment_id);
    assert_eq!(review.task_signature, task_signature);

    let codex = harness
        .temp
        .path()
        .join("execution-policy-bin/research-codex");
    fs::create_dir_all(codex.parent().unwrap()).unwrap();
    compile_sleeping_codex_fixture(&codex);
    let policy = research_policy_with_codex_fixture(&harness, &codex);
    let make_daemon = |now: i64| {
        let runner = AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            Arc::clone(&policy),
        );
        Daemon::new(
            harness.db.clone(),
            harness.fake_pueue.clone(),
            Arc::clone(&policy),
            runner,
            DaemonConfig {
                interval: Duration::from_millis(10),
                lease_seconds: 60,
                claim_limit: 1,
                now_override: Some(now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
    };

    let mut first_daemon = make_daemon(harness.now);
    let first_report = Box::pin(first_daemon.run_once()).await.unwrap();
    assert_eq!(first_report.research_started, 1);
    let mut second_daemon = Box::new(make_daemon(harness.now + 60));
    let early_report = Box::pin(second_daemon.run_once()).await.unwrap();
    assert_eq!(early_report.research_started, 0);
    assert_eq!(early_report.diagnoses, 0);
    let run_id: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT run_id FROM agent_runs
             WHERE execution_kind = 'campaign_research'
             ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let run_temp = harness
        .root("project-a")
        .join(".pueue-agent/tmp")
        .join(run_id.to_string());
    let overflow_subtree = create_cleanup_depth_overflow(&run_temp);

    let terminal_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if ResearchRepository::new(&harness.db)
            .find(&review.review_id)
            .unwrap()
            .state
            == "retry_wait"
        {
            break;
        }
        assert!(
            Instant::now() < terminal_deadline,
            "first research owner did not reach retained cleanup"
        );
        Box::pin(first_daemon.run_once()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(overflow_subtree.is_dir());

    HealthRepository::ensure_running(
        &harness.db,
        "project-a",
        "daemon-campaign",
        &experiment_id,
        task.id,
        harness.now,
    )
    .unwrap();
    for observed_at in [harness.now - 2, harness.now - 1] {
        HealthRepository::record_observation(
            &harness.db,
            &experiment_id,
            observed_at,
            SignalSummaryEntry {
                class: "oom".to_owned(),
                source: "fixture".to_owned(),
                evidence_digest: format!("cleanup-fixture-{observed_at}"),
                observed_at,
            },
        )
        .unwrap();
    }
    HealthRepository::set_state(&harness.db, &experiment_id, HealthState::Suspicious, harness.now)
        .unwrap();
    let health_pending_before = health_admission_snapshot(&harness.db, "project-a");
    let assert_cleanup_depth_error = |result: Result<DaemonReport, AppError>, label: &str| {
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("{label}: daemon pass unexpectedly succeeded"),
        };
        match error {
            AppError::PolicyViolation { violation } => {
                assert_eq!(violation.code, PolicyViolationCode::TempUnsafe, "{label}");
                assert_eq!(
                    violation.stage,
                    PolicyViolationStage::RunBoundPreMarker,
                    "{label}"
                );
                assert_eq!(
                    violation.detail,
                    PolicyViolationDetail::TempUnsafe(TempUnsafeReason::DepthLimit),
                    "{label}"
                );
            }
            other => panic!("{label}: unexpected cleanup error: {other:?}"),
        }
    };

    // The second daemon was already started while the first owner was live.
    // Its later pass must still observe the durable cleanup boundary.
    assert_cleanup_depth_error(
        Box::pin(second_daemon.run_once()).await,
        "warmup must return the retained cleanup depth error",
    );
    assert_eq!(
        health_admission_snapshot(&harness.db, "project-a"),
        health_pending_before
    );
    assert_eq!(
        HealthRepository::get(&harness.db, &experiment_id)
            .unwrap()
            .unwrap()
            .state,
        HealthState::Suspicious
    );

    let later_experiment_id = harness.campaign_experiment_for(
        "project-b",
        "later-health-campaign",
        "later-health-experiment",
    );
    let mut later_task = running_task();
    later_task.id = 142;
    later_task.group = "pb-project".to_owned();
    let later_task_signature = pueue_agent::reconcile::managed_task_run_signature(&later_task)
        .expect("managed later research task identity");
    harness
        .fake_pueue
        .set_tasks(vec![running_task(), later_task.clone()]);
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&later_experiment_id, 202)
        .unwrap();
    ExperimentRepository::new(&harness.db)
        .mark_accepted(&later_experiment_id, later_task.id, &later_task_signature, 203)
        .unwrap();
    HealthRepository::ensure_running(
        &harness.db,
        "project-b",
        "later-health-campaign",
        &later_experiment_id,
        later_task.id,
        harness.now + 1,
    )
    .unwrap();
    HealthRepository::record_observation(
        &harness.db,
        &later_experiment_id,
        harness.now,
        SignalSummaryEntry {
            class: "oom".to_owned(),
            source: "fixture".to_owned(),
            evidence_digest: "later-health-fixture".to_owned(),
            observed_at: harness.now,
        },
    )
    .unwrap();
    HealthRepository::set_state(
        &harness.db,
        &later_experiment_id,
        HealthState::Suspicious,
        harness.now + 1,
    )
    .unwrap();
    let health_later_before = health_admission_snapshot(&harness.db, "project-b");

    let research_run_count = || {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE execution_kind = 'campaign_research'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let reservation_count = || {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                 WHERE campaign_id = ?1 AND dimension = 'agent_run'
                   AND subject_key LIKE ?2",
                rusqlite::params![
                    "daemon-campaign",
                    format!("research:{}:%", review.review_id),
                ],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let research_run_count_before = research_run_count();
    let reservation_count_before = reservation_count();
    let retry_wakes: (i64, i64) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT review.not_before, event.not_before
             FROM research_reviews AS review
             JOIN events AS event ON event.event_id = review.event_id
             WHERE review.review_id = ?1",
            [&review.review_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(retry_wakes, (harness.now + 60, harness.now + 60));

    assert_cleanup_depth_error(
        Box::pin(second_daemon.run_once()).await,
        "blocked pass must return the retained cleanup depth error",
    );
    assert_eq!(research_run_count(), research_run_count_before);
    assert_eq!(reservation_count(), reservation_count_before);
    assert_eq!(
        health_admission_snapshot(&harness.db, "project-a"),
        health_pending_before
    );
    let health_later_after = health_admission_snapshot(&harness.db, "project-b");
    assert_eq!(health_later_after.0, health_later_before.0 + 1);
    assert_eq!(health_later_after.1, health_later_before.1 + 1);
    assert_eq!(health_later_after.2, health_later_before.2 + 1);
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .find(&review.review_id)
            .unwrap()
            .state,
        "retry_wait"
    );

    fs::remove_dir_all(overflow_subtree).unwrap();
    let released_report = Box::pin(first_daemon.run_once()).await.unwrap();
    assert_eq!(
        released_report.research_started,
        0,
        "cleanup release at the old clock must not launch before the durable wake"
    );
    assert_eq!(research_run_count(), research_run_count_before);
    assert_eq!(reservation_count(), reservation_count_before);

    let released_snapshot = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(released_snapshot.cleanup_phase.as_deref(), Some("complete"));
    let retry_report = Box::pin(second_daemon.run_once()).await.unwrap();
    assert_eq!(
        retry_report.research_started,
        1,
        "research retry was not admitted after cleanup release: started={} deferred={} blocked={}; {}",
        retry_report.research_started,
        retry_report.research_deferred,
        retry_report.research_blocked,
        research_retry_diagnostic(&harness.db, &review.review_id)
    );
    assert_eq!(research_run_count(), research_run_count_before + 1);
    assert_eq!(reservation_count(), reservation_count_before + 1);

    let active_research_count = || {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE execution_kind = 'campaign_research'
                   AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let settle_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if active_research_count() == 0 {
            break;
        }
        assert!(Instant::now() < settle_deadline, "research retry did not settle");
        Box::pin(first_daemon.run_once()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let health_eligible_before = health_admission_snapshot(&harness.db, "project-a");
    let diagnosis_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let report = Box::pin(second_daemon.run_once()).await.unwrap();
        let health_after = health_admission_snapshot(&harness.db, "project-a");
        if health_after.0 > health_eligible_before.0 {
            assert_eq!(report.diagnoses, 1);
            assert_eq!(health_after.1, health_eligible_before.1 + 1);
            assert_eq!(health_after.2, health_eligible_before.2 + 1);
            break;
        }
        assert!(
            Instant::now() < diagnosis_deadline,
            "health diagnosis did not become eligible after cleanup completion"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let diagnosis_settle_deadline = Instant::now() + Duration::from_secs(10);
    while harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM agent_runs
             WHERE project_id = 'project-a' AND execution_kind = 'diagnosis'
               AND status IN ('starting', 'running')",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
        > 0
    {
        assert!(
            Instant::now() < diagnosis_settle_deadline,
            "eligible health diagnosis did not settle"
        );
        Box::pin(second_daemon.run_once()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    HealthRepository::set_state(&harness.db, &experiment_id, HealthState::Healthy, harness.now)
        .unwrap();
}

#[cfg(target_os = "linux")]
fn process_group_exists(pid: i32) -> bool {
    unsafe extern "C" {
        fn kill(pid: std::os::raw::c_int, signal: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    pid > 1 && unsafe { kill(-pid, 0) == 0 }
}

#[cfg(target_os = "linux")]
fn process_group_id(pid: i32) -> Option<i32> {
    unsafe extern "C" {
        fn getpgid(pid: std::os::raw::c_int) -> std::os::raw::c_int;
    }
    let group = unsafe { getpgid(pid) };
    (group > 1).then_some(group)
}

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
struct ResearchCrashSnapshot {
    run_count: i64,
    reservation_count: i64,
    review_state: String,
    event_status: String,
    agent_run_id: Option<i64>,
    run_status: Option<String>,
    run_pid: Option<i64>,
    gate_state: Option<String>,
    cleanup_phase: Option<String>,
}

#[cfg(target_os = "linux")]
fn research_crash_snapshot(db: &Db, review_id: &str) -> ResearchCrashSnapshot {
    let connection = db.connect().unwrap();
    let (review_state, event_id, agent_run_id, notes_json): (String, i64, Option<i64>, String) =
        connection
            .query_row(
                "SELECT state, event_id, agent_run_id, notes_json
                 FROM research_reviews WHERE review_id = ?1",
                [review_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    let event_status: String = connection
        .query_row(
            "SELECT status FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    let run_details = agent_run_id.map(|run_id| {
        connection
            .query_row(
                "SELECT status, pid, launch_gate_state FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    });
    let run_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs WHERE execution_kind = 'campaign_research'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let reservation_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE dimension = 'agent_run' AND subject_key LIKE ?1",
            [format!("research:{review_id}:%")],
            |row| row.get(0),
        )
        .unwrap();
    let cleanup_phase = serde_json::from_str::<serde_json::Value>(&notes_json)
        .ok()
        .and_then(|notes| {
            notes
                .get("native_recovery")
                .and_then(|authority| authority.get("cleanup"))
                .and_then(|cleanup| cleanup.get("phase"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
    ResearchCrashSnapshot {
        run_count,
        reservation_count,
        review_state,
        event_status,
        agent_run_id,
        run_status: run_details.as_ref().map(|details| details.0.clone()),
        run_pid: run_details.as_ref().and_then(|details| details.1),
        gate_state: run_details.map(|details| details.2),
        cleanup_phase,
    }
}

#[cfg(target_os = "linux")]
fn research_retry_diagnostic(db: &Db, review_id: &str) -> String {
    let connection = db.connect().unwrap();
    let row: (
        String,
        i64,
        Option<i64>,
        Option<String>,
        Option<i64>,
        i64,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
        Option<i64>,
        Option<String>,
    ) = connection
        .query_row(
            "SELECT review.state, review.attempt, review.agent_run_id,
                    review.failure_code, review.not_before,
                    review.event_id, event.status, event.not_before,
                    run.status, run.launch_gate_state, run.pid,
                    run.execution_kind,
                    state.session_generation, state.session_id,
                    json_extract(review.notes_json, '$.native_recovery.cleanup.completed_at'),
                    json_extract(review.notes_json, '$.native_recovery.cleanup.phase')
             FROM research_reviews AS review
             JOIN events AS event ON event.event_id = review.event_id
             LEFT JOIN agent_runs AS run ON run.run_id = review.agent_run_id
             LEFT JOIN campaign_research AS state
               ON state.campaign_id = review.campaign_id
             WHERE review.review_id = ?1",
            [review_id],
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
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                ))
            },
        )
        .unwrap();
    let retry_owner_ready = ResearchRepository::new(db)
        .retry_owner_ready(review_id)
        .map(|ready| ready.to_string())
        .unwrap_or_else(|error| format!("error:{error}"));
    let evidence = ResearchRepository::new(db)
        .find(review_id)
        .and_then(|review| build_research_evidence(db, &review, row.4.unwrap_or(0)))
        .map(|_| "ok".to_owned())
        .unwrap_or_else(|error| format!("error:{error}"));
    let unbound_or_incomplete_top_level_projects = connection
        .prepare(
            "SELECT DISTINCT owner.project_id
             FROM agent_runs AS owner
             LEFT JOIN research_reviews AS bound_review
               ON bound_review.agent_run_id = owner.run_id
             WHERE owner.execution_kind = 'campaign_research'
               AND (
                   bound_review.review_id IS NULL
                   OR json_extract(bound_review.notes_json, '$.native_recovery.cleanup.phase') IS NULL
                   OR json_extract(bound_review.notes_json, '$.native_recovery.cleanup.phase') <> 'complete'
               )
             ORDER BY owner.project_id",
        )
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut diagnostic = format!(
        "review state={} attempt={} run={:?} failure={:?} wake={:?} event={} event_status={} event_wake={:?}; run_status={:?} gate={:?} pid={:?} kind={:?} cleanup_at={:?}; session_gen={:?} session={:?} cleanup_phase={:?}; unbound_or_incomplete_top_level_projects={unbound_or_incomplete_top_level_projects:?}; retry_owner_ready={retry_owner_ready}; evidence={evidence}",
        row.0,
        row.1,
        row.2,
        row.3,
        row.4,
        row.5,
        row.6,
        row.7,
        row.8,
        row.9,
        row.10,
        row.11,
        row.14,
        row.12,
        row.13,
        row.15,
    );
    diagnostic.truncate(2_000);
    diagnostic
}

#[cfg(target_os = "linux")]
fn health_admission_snapshot(db: &Db, project_id: &str) -> (i64, i64, i64) {
    let connection = db.connect().unwrap();
    let diagnosis_runs = connection
        .query_row(
            "SELECT COUNT(*) FROM agent_runs
             WHERE project_id = ?1 AND execution_kind = 'diagnosis'",
            [project_id],
            |row| row.get(0),
        )
        .unwrap();
    let diagnosis_events = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'health_diagnosis'",
            [project_id],
            |row| row.get(0),
        )
        .unwrap();
    let private_temp_generations = fs::read_dir(
        PathBuf::from(
            ProjectRepository::new(db)
                .find_by_id(project_id)
                .unwrap()
                .unwrap()
                .root_path,
        )
        .join(".pueue-agent/tmp"),
    )
    .map(|entries| entries.count() as i64)
    .unwrap_or(0);
    (
        diagnosis_runs,
        diagnosis_events,
        private_temp_generations,
    )
}

#[cfg(target_os = "linux")]
#[derive(Debug, PartialEq, Eq)]
struct StartupRecoveryDbSnapshot {
    review: Vec<String>,
    event: Vec<String>,
    run: Vec<String>,
    campaign_research: Vec<String>,
    reservations: Vec<Vec<String>>,
}

#[cfg(target_os = "linux")]
fn snapshot_row_values(row: &rusqlite::Row<'_>) -> rusqlite::Result<Vec<String>> {
    (0..row.as_ref().column_count())
        .map(|index| Ok(format!("{:?}", row.get_ref(index)?.to_owned())))
        .collect()
}

#[cfg(target_os = "linux")]
fn startup_recovery_db_snapshot(
    db: &Db,
    campaign_id: &str,
    review_id: &str,
    event_id: i64,
    run_id: i64,
) -> StartupRecoveryDbSnapshot {
    let connection = db.connect().unwrap();
    let review = connection
        .query_row(
            "SELECT * FROM research_reviews WHERE review_id = ?1",
            [review_id],
            snapshot_row_values,
        )
        .unwrap();
    let event = connection
        .query_row(
            "SELECT * FROM events WHERE event_id = ?1",
            [event_id],
            snapshot_row_values,
        )
        .unwrap();
    let run = connection
        .query_row(
            "SELECT * FROM agent_runs WHERE run_id = ?1",
            [run_id],
            snapshot_row_values,
        )
        .unwrap();
    let campaign_research = connection
        .query_row(
            "SELECT * FROM campaign_research WHERE campaign_id = ?1",
            [campaign_id],
            snapshot_row_values,
        )
        .unwrap();
    let mut statement = connection
        .prepare(
            "SELECT * FROM budget_reservations
             WHERE campaign_id = ?1 ORDER BY reservation_id",
        )
        .unwrap();
    let reservations = statement
        .query_map([campaign_id], snapshot_row_values)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    StartupRecoveryDbSnapshot {
        review,
        event,
        run,
        campaign_research,
        reservations,
    }
}

#[cfg(target_os = "linux")]
struct SeededPidlessResearchOwner {
    project_id: String,
    campaign_id: String,
    experiment_id: String,
    task_id: i64,
    review_id: String,
    event_id: i64,
    run_id: i64,
    run_temp: PathBuf,
    marker_path: PathBuf,
    snapshot: StartupRecoveryDbSnapshot,
}

#[cfg(target_os = "linux")]
fn seed_pidless_research_owner(harness: &DaemonHarness) -> SeededPidlessResearchOwner {
    let task = running_task();
    seed_pidless_research_owner_for(
        harness,
        "project-a",
        "daemon-campaign",
        "daemon-campaign-experiment",
        &task,
    )
}

#[cfg(target_os = "linux")]
fn seed_pidless_research_owner_for(
    harness: &DaemonHarness,
    project_id: &str,
    campaign_id: &str,
    experiment_id: &str,
    task: &PueueTask,
) -> SeededPidlessResearchOwner {
    let experiment_id = harness.campaign_experiment_for(project_id, campaign_id, experiment_id);
    let live_task_signature = pueue_agent::reconcile::task_signature(task);
    let task_signature = pueue_agent::reconcile::managed_task_run_signature(task)
        .expect("managed research task identity");
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&experiment_id, 190)
        .unwrap();
    ExperimentRepository::new(&harness.db)
        .mark_accepted(&experiment_id, task.id, &task_signature, 191)
        .unwrap();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            project_id,
            &live_task_signature,
            task.id,
            &task.group,
            vec![task.command.clone()],
            &task.state,
            task.enqueued_at.as_deref().and_then(|value| value.parse().ok()),
            task.started_at.as_deref().and_then(|value| value.parse().ok()),
            None,
            None,
            harness.now,
        ))
        .unwrap();
    let campaign_id = campaign_id.to_owned();
    ResearchRepository::new(&harness.db)
        .ensure_campaign(&campaign_id)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaign_research SET next_due_at = ?1 WHERE campaign_id = ?2",
            rusqlite::params![harness.now, campaign_id],
        )
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .claim_due(
            &campaign_id,
            &experiment_id,
            &task_signature,
            harness.now,
        )
        .unwrap()
        .expect("seeded running campaign must claim a research review");
    let event_id: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT event_id FROM research_reviews WHERE review_id = ?1",
            [&review.review_id],
            |row| row.get(0),
        )
        .unwrap();
    EventRepository::new(&harness.db)
        .claim_by_id(project_id, event_id, harness.now + 60)
        .unwrap()
        .expect("seeded research event must be claimable");
    let reservation = match CampaignRepository::new(&harness.db)
        .reserve_agent_run(
            &campaign_id,
            &format!("research:{}:attempt:1", review.review_id),
            &CampaignLimits::default(),
            harness.now,
        )
        .unwrap()
    {
        pueue_agent::db::AgentDecisionReservation::Reserved(reservation) => reservation,
        pueue_agent::db::AgentDecisionReservation::BudgetWaiting { .. }
        | pueue_agent::db::AgentDecisionReservation::Deferred { .. } => {
            panic!("seeded campaign reservation must be admitted")
        }
    };
    let admitted = ResearchRepository::new(&harness.db)
        .prepare_attempt(
            &review.review_id,
            &reservation.reservation_id,
            CampaignLimits::default().max_decision_attempts_per_cycle,
            harness.now,
        )
        .unwrap()
        .expect("seeded research attempt must be admitted");
    let evidence = build_research_evidence(&harness.db, &admitted, harness.now).unwrap();
    let binding = ResearchLaunchBinding {
        review_id: admitted.review_id.clone(),
        campaign_id: admitted.campaign_id.clone(),
        experiment_id: admitted.experiment_id.clone(),
        attempt: admitted.attempt,
        session_generation: admitted.session_generation,
        prior_session_generation: admitted.session_generation,
        session_id: "11111111-1111-4111-8111-111111111111".to_owned(),
        prior_session_id: None,
        context_json: evidence.json,
        context_digest: evidence.digest,
        budget_reservation_id: reservation.reservation_id.clone(),
        recovery_reason: None,
    };
    let policy = harness.policy();
    let runner = AgentRunner::new(
        AgentRunnerConfig::production()
            .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
        Arc::clone(&policy),
    );
    let project = ProjectRepository::new(&harness.db)
        .find_by_id(project_id)
        .unwrap()
        .unwrap();
    let project_config = config::load(&project.config_path).unwrap();
    let project_policy = runner
        .resolve_project_policy(&project, &project_config)
        .unwrap();
    let log_path = project
        .root_path
        .join(pueue_agent::agent::relative_log_path(event_id, harness.now));
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::with_context(
                &project.project_id,
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now,
                &log_path,
                AgentContextMode::Fresh,
                None,
                vec![event_id.to_string()],
            )
            .with_execution(
                ExecutionProjection::new("campaign_research", "/bin/echo", "fixture")
                    .unwrap(),
            ),
            &[event_id],
        )
        .unwrap();
    ResearchRepository::new(&harness.db)
        .bind_agent_run(&binding, run.run_id, &project.project_id, harness.now)
        .unwrap();
    let verified_root = project_policy.root_anchor.verify_identity().unwrap();
    let temp = PrivateRunTemp::create(&verified_root, run.run_id).unwrap();
    let recovery_identity = temp.recovery_identity(&verified_root).unwrap();
    ResearchRepository::new(&harness.db)
        .record_native_recovery_authority(
            &binding,
            run.run_id,
            &recovery_identity,
            true,
            harness.now,
        )
        .unwrap();
    let run_temp = temp.path().to_path_buf();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    assert!(!marker_path.exists());
    let snapshot = startup_recovery_db_snapshot(
        &harness.db,
        &campaign_id,
        &admitted.review_id,
        event_id,
        run.run_id,
    );
    SeededPidlessResearchOwner {
        project_id: project_id.to_owned(),
        campaign_id,
        experiment_id,
        task_id: task.id,
        review_id: admitted.review_id,
        event_id,
        run_id: run.run_id,
        run_temp,
        marker_path,
        snapshot,
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn startup_pidless_absent_recovery_rolls_back_cleanup_and_cas_failures() {
    let harness = DaemonHarness::new();
    prepare_healthy_research_fixture(&harness);
    let seeded = seed_pidless_research_owner(&harness);
    let baseline = research_crash_snapshot(&harness.db, &seeded.review_id);
    let overflow_subtree = create_cleanup_depth_overflow(&seeded.run_temp);
    let mut daemon = harness.daemon();

    assert!(
        daemon.run_once().await.is_err(),
        "cleanup overflow must fail before startup-owner retirement"
    );
    assert_eq!(
        startup_recovery_db_snapshot(
            &harness.db,
            &seeded.campaign_id,
            &seeded.review_id,
            seeded.event_id,
            seeded.run_id,
        ),
        seeded.snapshot
    );
    assert!(overflow_subtree.is_dir());

    fs::remove_dir_all(&overflow_subtree).unwrap();
    let cleanup_sentinel = seeded.run_temp.join("late-cas-sentinel");
    fs::write(&cleanup_sentinel, b"cleanup-before-cas").unwrap();
    let escaped_review_id = seeded.review_id.replace('\'', "''");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER reject_pidless_recovery_review_cas
             BEFORE UPDATE OF failure_code ON research_reviews
             WHEN OLD.review_id = '{escaped_review_id}'
                  AND NEW.failure_code = 'research_interrupted'
             BEGIN
                 SELECT RAISE(ABORT, 'injected startup recovery review CAS failure');
             END;"
        ))
        .unwrap();
    assert!(
        daemon.run_once().await.is_err(),
        "late startup-owner CAS failure must be returned after cleanup"
    );
    assert_eq!(
        startup_recovery_db_snapshot(
            &harness.db,
            &seeded.campaign_id,
            &seeded.review_id,
            seeded.event_id,
            seeded.run_id,
        ),
        seeded.snapshot
    );
    assert!(!cleanup_sentinel.exists());
    assert!(
        fs::read_dir(&seeded.run_temp)
            .unwrap()
            .next()
            .is_none()
    );
    assert_eq!(
        research_crash_snapshot(&harness.db, &seeded.review_id)
            .cleanup_phase
            .as_deref(),
        Some("pending")
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_pidless_recovery_review_cas;")
        .unwrap();

    let retired = daemon
        .run_once()
        .await
        .expect("startup owner should retire after both faults are removed");
    assert_eq!(retired.research_started, 0);
    let retired_snapshot = research_crash_snapshot(&harness.db, &seeded.review_id);
    assert_eq!(retired_snapshot.review_state, "retry_wait");
    assert_eq!(retired_snapshot.event_status, "retry_wait");
    assert_eq!(retired_snapshot.run_status.as_deref(), Some("failed"));
    assert_eq!(retired_snapshot.run_pid, None);
    assert_eq!(retired_snapshot.gate_state.as_deref(), Some("failed"));
    assert_eq!(retired_snapshot.cleanup_phase.as_deref(), Some("complete"));
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .state(&seeded.campaign_id)
            .unwrap()
            .session_id,
        None
    );
    assert_eq!(retired_snapshot.run_count, baseline.run_count);
    assert_eq!(retired_snapshot.reservation_count, baseline.reservation_count);
    assert!(!seeded.marker_path.exists());
    let retired_db_snapshot = startup_recovery_db_snapshot(
        &harness.db,
        &seeded.campaign_id,
        &seeded.review_id,
        seeded.event_id,
        seeded.run_id,
    );

    let after_idempotent = daemon
        .run_once()
        .await
        .expect("retired startup owner should be absent on the next pass");
    assert_eq!(after_idempotent.research_started, 0);
    let after_snapshot = research_crash_snapshot(&harness.db, &seeded.review_id);
    assert_eq!(after_snapshot.run_count, retired_snapshot.run_count);
    assert_eq!(after_snapshot.reservation_count, retired_snapshot.reservation_count);
    assert_eq!(after_snapshot.cleanup_phase, retired_snapshot.cleanup_phase);
    assert_eq!(
        startup_recovery_db_snapshot(
            &harness.db,
            &seeded.campaign_id,
            &seeded.review_id,
            seeded.event_id,
            seeded.run_id,
        ),
        retired_db_snapshot
    );
}

#[cfg(target_os = "linux")]
async fn assert_pidless_marker_retains_owner(marker_contents: &[u8], label: &str) {
    let harness = DaemonHarness::new();
    prepare_healthy_research_fixture(&harness);
    let seeded = seed_pidless_research_owner(&harness);
    fs::write(&seeded.marker_path, marker_contents).unwrap();
    fs::set_permissions(&seeded.marker_path, fs::Permissions::from_mode(0o600)).unwrap();
    let sentinel = seeded.run_temp.join(format!("{label}-retained"));
    fs::write(&sentinel, b"startup-marker-owner-retained").unwrap();
    let baseline = research_crash_snapshot(&harness.db, &seeded.review_id);
    let mut daemon = harness.daemon();

    for pass in 0..2 {
        let report = daemon
            .run_once()
            .await
            .unwrap_or_else(|error| panic!("{label} marker pass {pass} must retain owner: {error}"));
        assert_eq!(report.research_started, 0, "{label} marker pass {pass}");
        assert_eq!(
            startup_recovery_db_snapshot(
                &harness.db,
                &seeded.campaign_id,
                &seeded.review_id,
                seeded.event_id,
                seeded.run_id,
            ),
            seeded.snapshot,
            "{label} marker pass {pass} must preserve all linked durable state"
        );
        assert_eq!(
            research_crash_snapshot(&harness.db, &seeded.review_id),
            baseline,
            "{label} marker pass {pass} must not retire the owner"
        );
        assert_eq!(fs::read(&seeded.marker_path).unwrap(), marker_contents);
        assert!(sentinel.is_file(), "{label} marker pass {pass} cleaned the generation");
        assert!(seeded.run_temp.is_dir());
        assert_eq!(
            AgentRunRepository::new(&harness.db)
                .find_by_id(seeded.run_id)
                .unwrap()
                .unwrap()
                .pid,
            None
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn startup_pidless_research_marker_uncertainty_never_retires_owner() {
    assert_pidless_marker_retains_owner(b"authorized\n", "valid").await;
    assert_pidless_marker_retains_owner(b"invalid\n", "indeterminate").await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn startup_pidless_recovery_rejects_late_backward_status_adoption() {
    let harness = DaemonHarness::new();
    prepare_healthy_research_fixture(&harness);
    let seeded = seed_pidless_research_owner(&harness);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs
             SET status = 'running', launch_gate_state = 'pending'
             WHERE run_id = ?1",
            [seeded.run_id],
        )
        .unwrap();
    let mut daemon = harness.daemon();

    let first_report = Box::pin(daemon.run_once()).await.unwrap();
    assert_eq!(first_report.research_started, 0);
    assert_eq!(first_report.diagnoses, 0);
    let first_snapshot = research_crash_snapshot(&harness.db, &seeded.review_id);
    assert_eq!(first_snapshot.run_status.as_deref(), Some("running"));
    assert_eq!(first_snapshot.run_pid, None);
    assert_eq!(first_snapshot.gate_state.as_deref(), Some("pending"));
    assert_eq!(first_snapshot.cleanup_phase.as_deref(), Some("pending"));
    assert!(seeded.run_temp.is_dir());
    assert!(!seeded.marker_path.exists());

    let sentinel = seeded.run_temp.join("backward-status-retained");
    fs::write(&sentinel, b"retain-original-generation").unwrap();
    let first_db_snapshot = startup_recovery_db_snapshot(
        &harness.db,
        &seeded.campaign_id,
        &seeded.review_id,
        seeded.event_id,
        seeded.run_id,
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs SET status = 'starting' WHERE run_id = ?1",
            [seeded.run_id],
        )
        .unwrap();

    let second_report = Box::pin(daemon.run_once()).await.unwrap();
    assert_eq!(second_report.research_started, 0);
    assert_eq!(second_report.diagnoses, 0);
    let second_snapshot = research_crash_snapshot(&harness.db, &seeded.review_id);
    assert_eq!(second_snapshot.review_state, first_snapshot.review_state);
    assert_eq!(second_snapshot.event_status, first_snapshot.event_status);
    assert_eq!(second_snapshot.agent_run_id, first_snapshot.agent_run_id);
    assert_eq!(second_snapshot.run_count, first_snapshot.run_count);
    assert_eq!(second_snapshot.reservation_count, first_snapshot.reservation_count);
    assert_eq!(second_snapshot.run_status.as_deref(), Some("starting"));
    assert_eq!(second_snapshot.run_pid, None);
    assert_eq!(second_snapshot.gate_state.as_deref(), Some("pending"));
    assert_eq!(second_snapshot.cleanup_phase.as_deref(), Some("pending"));
    assert!(seeded.run_temp.is_dir());
    assert_eq!(fs::read(&sentinel).unwrap(), b"retain-original-generation");
    assert!(!seeded.marker_path.exists());
    let second_db_snapshot = startup_recovery_db_snapshot(
        &harness.db,
        &seeded.campaign_id,
        &seeded.review_id,
        seeded.event_id,
        seeded.run_id,
    );
    assert_ne!(second_db_snapshot, first_db_snapshot);
    assert_eq!(second_db_snapshot.review, first_db_snapshot.review);
    assert_eq!(second_db_snapshot.event, first_db_snapshot.event);
    assert_eq!(second_db_snapshot.reservations, first_db_snapshot.reservations);
    assert_eq!(
        ResearchRepository::new(&harness.db)
            .state(&seeded.campaign_id)
            .unwrap()
            .blocked_reason
            .as_deref(),
        Some("research_recovery_required")
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn startup_pidless_recovery_claim_limit_does_not_starve_behind_retained_owner() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pb-project", "/bin/echo");
    harness.register_project("project-c", "pc-project", "/bin/echo");
    let task_a = running_task();
    let task_b = running_task_for(142, "pb-project");
    let task_c = running_task_for(143, "pc-project");
    prepare_healthy_research_fixture_for(&harness, "project-a", task_a.id);
    prepare_healthy_research_fixture_for(&harness, "project-b", task_b.id);
    prepare_healthy_research_fixture_for(&harness, "project-c", task_c.id);
    harness
        .fake_pueue
        .set_tasks(vec![task_a.clone(), task_b.clone(), task_c.clone()]);

    let owners = [
        seed_pidless_research_owner_for(
            &harness,
            "project-a",
            "fairness-campaign-a",
            "fairness-experiment-a",
            &task_a,
        ),
        seed_pidless_research_owner_for(
            &harness,
            "project-b",
            "fairness-campaign-b",
            "fairness-experiment-b",
            &task_b,
        ),
        seed_pidless_research_owner_for(
            &harness,
            "project-c",
            "fairness-campaign-c",
            "fairness-experiment-c",
            &task_c,
        ),
    ];
    fs::write(&owners[0].marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&owners[0].marker_path, fs::Permissions::from_mode(0o600)).unwrap();
    // The recovery cursor must advance past a retained prefix. With
    // claim_limit=1, the expected selections are A (retained), B (retired),
    // A (retained), then C (retired).
    let blocked_owner = &owners[2];
    HealthRepository::ensure_running(
        &harness.db,
        &blocked_owner.project_id,
        &blocked_owner.campaign_id,
        &blocked_owner.experiment_id,
        blocked_owner.task_id,
        harness.now,
    )
    .unwrap();
    for observed_at in [harness.now - 2, harness.now - 1] {
        HealthRepository::record_observation(
            &harness.db,
            &blocked_owner.experiment_id,
            observed_at,
            SignalSummaryEntry {
                class: "oom".to_owned(),
                source: "fixture".to_owned(),
                evidence_digest: format!("startup-owner-{observed_at}"),
                observed_at,
            },
        )
        .unwrap();
    }
    HealthRepository::set_state(
        &harness.db,
        &blocked_owner.experiment_id,
        HealthState::Suspicious,
        harness.now,
    )
    .unwrap();

    let owner_snapshots = owners
        .iter()
        .map(|owner| research_crash_snapshot(&harness.db, &owner.review_id))
        .collect::<Vec<_>>();
    let research_run_count = || {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE execution_kind = 'campaign_research'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let research_reservation_count = || {
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM budget_reservations
                 WHERE dimension = 'agent_run' AND subject_key LIKE 'research:%'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
    };
    let research_run_count_before = research_run_count();
    let research_reservation_count_before = research_reservation_count();
    let health_blocked_before = health_admission_snapshot(&harness.db, &blocked_owner.project_id);

    let policy = harness.policy();
    let make_daemon = || {
        let runner = AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            Arc::clone(&policy),
        );
        Daemon::new(
            harness.db.clone(),
            harness.fake_pueue.clone(),
            Arc::clone(&policy),
            runner,
            DaemonConfig {
                interval: Duration::from_millis(10),
                lease_seconds: 60,
                claim_limit: 1,
                now_override: Some(harness.now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
    };
    let mut daemon = make_daemon();

    // Each daemon pass polls the retained startup owners at both lifecycle
    // boundaries, so two bounded passes cover A, B, A, C.
    for pass in 0..2 {
        let report = Box::pin(daemon.run_once())
            .await
            .unwrap_or_else(|error| panic!("startup-owner fairness pass {pass} failed: {error}"));
        assert_eq!(report.research_started, 0, "fairness pass {pass}");
        assert_eq!(report.diagnoses, 0, "fairness pass {pass}");
        assert_eq!(research_run_count(), research_run_count_before);
        assert_eq!(
            research_reservation_count(),
            research_reservation_count_before
        );
        assert_eq!(
            health_admission_snapshot(&harness.db, &blocked_owner.project_id),
            health_blocked_before,
            "the retained project's suspicious health must stay blocked while startup owners remain"
        );
        assert_eq!(
            research_crash_snapshot(&harness.db, &owners[0].review_id),
            owner_snapshots[0],
            "retained prefix owner must remain unchanged on pass {pass}"
        );
        assert_eq!(fs::read(&owners[0].marker_path).unwrap(), b"authorized\n");
        if pass == 0 {
            assert_eq!(
                research_crash_snapshot(&harness.db, &blocked_owner.review_id),
                owner_snapshots[2],
                "the suspicious owner must remain blocked behind the retained prefix"
            );
        }
    }

    assert_eq!(
        research_crash_snapshot(&harness.db, &owners[0].review_id),
        owner_snapshots[0]
    );
    assert_eq!(fs::read(&owners[0].marker_path).unwrap(), b"authorized\n");
    assert!(owners[0].run_temp.is_dir());
    for (index, owner) in owners.iter().enumerate().skip(1) {
        let snapshot = research_crash_snapshot(&harness.db, &owner.review_id);
        assert_eq!(snapshot.review_state, "retry_wait");
        assert_eq!(snapshot.event_status, "retry_wait");
        assert_eq!(snapshot.run_status.as_deref(), Some("failed"));
        assert_eq!(snapshot.run_pid, None);
        assert_eq!(snapshot.gate_state.as_deref(), Some("failed"));
        assert_eq!(snapshot.cleanup_phase.as_deref(), Some("complete"));
        assert_eq!(snapshot.run_count, owner_snapshots[index].run_count);
        assert_eq!(snapshot.reservation_count, owner_snapshots[index].reservation_count);
    }
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE execution_kind = 'campaign_research'
                   AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
    assert_eq!(
        HealthRepository::get(&harness.db, &blocked_owner.experiment_id)
            .unwrap()
            .unwrap()
            .state,
        HealthState::Suspicious
    );

    HealthRepository::set_state(
        &harness.db,
        &blocked_owner.experiment_id,
        HealthState::Healthy,
        harness.now,
    )
    .unwrap();
    let retired_snapshots = owners
        .iter()
        .map(|owner| research_crash_snapshot(&harness.db, &owner.review_id))
        .collect::<Vec<_>>();
    let report = Box::pin(daemon.run_once()).await.unwrap();
    assert_eq!(report.research_started, 0);
    assert_eq!(report.diagnoses, 0);
    assert_eq!(
        owners
            .iter()
            .map(|owner| research_crash_snapshot(&harness.db, &owner.review_id))
            .collect::<Vec<_>>(),
        retired_snapshots,
        "retired startup owners must be idempotent on the next pass"
    );
    assert_eq!(research_run_count(), research_run_count_before);
    assert_eq!(
        research_reservation_count(),
        research_reservation_count_before
    );
}

#[cfg(target_os = "linux")]
struct ResearchCrashScope {
    db: Db,
    paths: ResearchCrashFixturePaths,
    controller: Option<std::process::Child>,
    helper_pid: Option<i32>,
    cleanup_needed: bool,
}

#[cfg(target_os = "linux")]
impl Drop for ResearchCrashScope {
    fn drop(&mut self) {
        let _ = fs::write(&self.paths.target_release, b"release\n");
        let _ = fs::write(&self.paths.descendant_release, b"release\n");
        if self.cleanup_needed {
            let pid = self.helper_pid.or_else(|| {
                self.db.connect().ok().and_then(|connection| {
                    connection
                        .query_row(
                            "SELECT pid FROM agent_runs
                             WHERE execution_kind = 'campaign_research'
                             ORDER BY run_id DESC LIMIT 1",
                            [],
                            |row| row.get::<_, Option<i64>>(0),
                        )
                        .ok()
                        .flatten()
                        .and_then(|pid| pid.try_into().ok())
                })
            });
            if let Some(pid) = pid {
                unsafe extern "C" {
                    fn kill(
                        pid: std::os::raw::c_int,
                        signal: std::os::raw::c_int,
                    ) -> std::os::raw::c_int;
                }
                if pid > 1 {
                    unsafe {
                        let _ = kill(-pid, 9);
                    }
                }
            }
        }
        if let Some(mut controller) = self.controller.take() {
            let _ = controller.kill();
            let _ = controller.wait();
        }
    }
}

#[cfg(target_os = "linux")]
impl ResearchCrashScope {
    fn disarm(&mut self) {
        self.cleanup_needed = false;
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore]
fn research_controller_crash_subprocess() {
    let db_path = PathBuf::from(
        std::env::var_os("PUEUE_AGENT_TEST_CRASH_DB").expect("crash controller database path"),
    );
    let fixture_root = PathBuf::from(
        std::env::var_os("PUEUE_AGENT_TEST_CRASH_ROOT").expect("crash controller fixture root"),
    );
    let review_id = std::env::var("PUEUE_AGENT_TEST_CRASH_REVIEW")
        .expect("crash controller review id");
    let controller_ready = PathBuf::from(
        std::env::var_os("PUEUE_AGENT_TEST_CRASH_READY").expect("crash controller ready path"),
    );
    let crash_now = PathBuf::from(
        std::env::var_os("PUEUE_AGENT_TEST_CRASH_NOW").expect("crash controller crash path"),
    );
    let paths = ResearchCrashFixturePaths::new(&fixture_root);
    let db = Db::open(&db_path).expect("open shared research database");
    let policy = research_policy_from_fixture_paths(&db, &fixture_root);
    let runner = AgentRunner::new(
        AgentRunnerConfig::production()
            .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
        Arc::clone(&policy),
    );
    let mut daemon = Daemon::new(
        db.clone(),
        FakePueue::with_tasks(vec![running_task()]),
        Arc::clone(&policy),
        runner,
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(200),
            shutdown_grace_period: Duration::from_secs(30),
        },
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build crash controller runtime");
    runtime.block_on(async {
        let report = daemon
            .run_once()
            .await
            .expect("crash controller must launch research");
        assert_eq!(report.research_started, 1);
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let fixture_ready = paths.target_ready.is_file()
                && paths.descendant_ready.is_file()
                && paths.descendant_pid.is_file();
            let native_ready = db
                .connect()
                .ok()
                .and_then(|connection| {
                    connection
                        .query_row(
                            "SELECT run.run_id, run.pid, run.launch_gate_state, run.log_path
                             FROM research_reviews AS review
                             JOIN agent_runs AS run ON run.run_id = review.agent_run_id
                             WHERE review.review_id = ?1
                               AND run.execution_kind = 'campaign_research'",
                            [&review_id],
                            |row| {
                                Ok((
                                    row.get::<_, i64>(0)?,
                                    row.get::<_, Option<i64>>(1)?,
                                    row.get::<_, String>(2)?,
                                    row.get::<_, String>(3)?,
                                ))
                            },
                        )
                        .ok()
                })
                .is_some_and(|(_run_id, pid, gate, log_path)| {
                    pid.is_some()
                        && gate == "released"
                        && PathBuf::from(format!("{log_path}.gate-started")).is_file()
                });
            let authority_pending = db
                .connect()
                .ok()
                .and_then(|connection| {
                    connection
                        .query_row(
                            "SELECT notes_json FROM research_reviews WHERE review_id = ?1",
                            [&review_id],
                            |row| row.get::<_, String>(0),
                        )
                        .ok()
                })
                .and_then(|notes| serde_json::from_str::<serde_json::Value>(&notes).ok())
                .and_then(|notes| notes.get("native_recovery").cloned())
                .and_then(|authority| authority.get("cleanup").cloned())
                .and_then(|cleanup| cleanup.get("phase").cloned())
                .is_some_and(|phase| phase == serde_json::json!("pending"));
            if fixture_ready && native_ready && authority_pending {
                fs::write(&controller_ready, b"ready\n").expect("publish controller readiness");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "real research helper did not reach crash fixture readiness"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        while !crash_now.is_file() {
            assert!(
                Instant::now() < deadline + Duration::from_secs(30),
                "parent did not request the controller crash"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    std::process::exit(97);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn research_controller_crash_restarts_only_after_group_quiescence() {
    let harness = DaemonHarness::new();
    prepare_healthy_research_fixture(&harness);
    let experiment_id = harness.campaign_experiment();
    let task = running_task();
    let live_task_signature = pueue_agent::reconcile::task_signature(&task);
    let task_signature = pueue_agent::reconcile::managed_task_run_signature(&task)
        .expect("managed research task identity");
    ExperimentRepository::new(&harness.db)
        .mark_submitting(&experiment_id, 190)
        .unwrap();
    ExperimentRepository::new(&harness.db)
        .mark_accepted(&experiment_id, task.id, &task_signature, 191)
        .unwrap();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            &live_task_signature,
            task.id,
            &task.group,
            vec![task.command.clone()],
            "Running",
            Some(100),
            Some(101),
            None,
            None,
            harness.now,
        ))
        .unwrap();
    ResearchRepository::new(&harness.db)
        .ensure_campaign("daemon-campaign")
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaign_research SET next_due_at = ?1 WHERE campaign_id = ?2",
            rusqlite::params![harness.now, "daemon-campaign"],
        )
        .unwrap();
    let review = ResearchRepository::new(&harness.db)
        .claim_due(
            "daemon-campaign",
            &experiment_id,
            &task_signature,
            harness.now,
        )
        .unwrap()
        .expect("the seeded running campaign must claim one research review");

    let fixture_root = fs::canonicalize(harness.temp.path()).unwrap();
    let paths = ResearchCrashFixturePaths::new(&fixture_root);
    let codex = fixture_root.join("execution-policy-bin/research-crash-codex");
    fs::create_dir_all(codex.parent().unwrap()).unwrap();
    compile_research_crash_codex_fixture(&codex, &paths);
    let policy = research_policy_with_codex_fixture(&harness, &codex);
    let db_path = fixture_root.join("state.sqlite3");
    let mut scope = ResearchCrashScope {
        db: harness.db.clone(),
        paths,
        controller: None,
        helper_pid: None,
        cleanup_needed: true,
    };
    let controller_exe = std::env::current_exe().unwrap();
    scope.controller = Some(
        Command::new(controller_exe)
            .args([
                "--ignored",
                "--exact",
                "research_controller_crash_subprocess",
                "--nocapture",
            ])
            .env("PUEUE_AGENT_TEST_CRASH_DB", &db_path)
            .env("PUEUE_AGENT_TEST_CRASH_ROOT", &fixture_root)
            .env("PUEUE_AGENT_TEST_CRASH_REVIEW", &review.review_id)
            .env("PUEUE_AGENT_TEST_CRASH_READY", &scope.paths.controller_ready)
            .env("PUEUE_AGENT_TEST_CRASH_NOW", &scope.paths.crash_now)
            .spawn()
            .expect("spawn separate research controller"),
    );

    let ready_deadline = Instant::now() + Duration::from_secs(35);
    while !scope.paths.controller_ready.is_file() {
        if let Some(status) = scope.controller.as_mut().unwrap().try_wait().unwrap() {
            panic!("research controller exited before readiness: {status}");
        }
        assert!(
            Instant::now() < ready_deadline,
            "research controller did not publish readiness"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let helper_pid: i32 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT pid FROM agent_runs
             WHERE execution_kind = 'campaign_research'
             ORDER BY run_id DESC LIMIT 1",
            [],
            |row| row.get::<_, Option<i64>>(0),
        )
        .unwrap()
        .expect("controller must persist the native helper pid")
        .try_into()
        .unwrap();
    let descendant_pid: i32 = fs::read_to_string(&scope.paths.descendant_pid)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    scope.helper_pid = Some(helper_pid);
    assert!(process_exists(helper_pid));
    assert!(process_exists(descendant_pid));
    assert!(process_group_exists(helper_pid));
    assert_eq!(process_group_id(descendant_pid), Some(helper_pid));

    fs::write(&scope.paths.crash_now, b"crash\n").unwrap();
    let crash_deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = scope.controller.as_mut().unwrap().try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < crash_deadline, "controller did not exit after crash request");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(status.code(), Some(97));

    let make_daemon = |now: i64| {
        let runner = AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            Arc::clone(&policy),
        );
        Daemon::new(
            harness.db.clone(),
            harness.fake_pueue.clone(),
            Arc::clone(&policy),
            runner,
            DaemonConfig {
                interval: Duration::from_millis(10),
                lease_seconds: 60,
                claim_limit: 100,
                now_override: Some(now),
                shutdown_grace_period: Duration::from_secs(30),
            },
        )
    };
    let mut daemon = make_daemon(harness.now);
    let live_snapshot = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(live_snapshot.cleanup_phase.as_deref(), Some("pending"));
    assert_eq!(live_snapshot.run_pid, Some(i64::from(helper_pid)));
    assert_eq!(live_snapshot.gate_state.as_deref(), Some("released"));
    assert_eq!(daemon.run_once().await.unwrap().research_started, 0);
    assert_eq!(research_crash_snapshot(&harness.db, &review.review_id), live_snapshot);

    fs::write(&scope.paths.target_release, b"release\n").unwrap();
    let leader_deadline = Instant::now() + Duration::from_secs(10);
    while process_exists(helper_pid) {
        assert!(Instant::now() < leader_deadline, "native helper leader did not exit");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(process_exists(descendant_pid));
    assert!(process_group_exists(helper_pid));
    assert_eq!(process_group_id(descendant_pid), Some(helper_pid));
    let leader_dead_snapshot = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(daemon.run_once().await.unwrap().research_started, 0);
    assert_eq!(
        research_crash_snapshot(&harness.db, &review.review_id),
        leader_dead_snapshot
    );

    fs::write(&scope.paths.descendant_release, b"release\n").unwrap();
    let group_deadline = Instant::now() + Duration::from_secs(10);
    while process_exists(helper_pid) || process_group_exists(helper_pid) {
        assert!(Instant::now() < group_deadline, "native research group did not quiesce");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    scope.helper_pid = None;
    scope.cleanup_needed = false;
    let first_cleanup_report = daemon.run_once().await.unwrap();
    assert_eq!(first_cleanup_report.research_started, 0);
    let retired_snapshot = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(retired_snapshot.review_state, "retry_wait");
    assert_eq!(retired_snapshot.cleanup_phase.as_deref(), Some("complete"));
    assert_ne!(retired_snapshot.run_status.as_deref(), Some("starting"));
    assert_ne!(retired_snapshot.run_status.as_deref(), Some("running"));

    let (review_not_before, event_not_before): (Option<i64>, Option<i64>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT review.not_before, event.not_before
             FROM research_reviews AS review
             JOIN events AS event ON event.event_id = review.event_id
             WHERE review.review_id = ?1",
            [&review.review_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let retry_now = review_not_before
        .into_iter()
        .chain(event_not_before)
        .max()
        .unwrap_or(harness.now)
        .max(harness.now);
    let mut retry_daemon = make_daemon(retry_now);
    let before_retry = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(before_retry.review_state, "retry_wait");
    assert_eq!(before_retry.event_status, "retry_wait");
    let retry_report = retry_daemon.run_once().await.unwrap();
    assert_eq!(
        retry_report.research_started,
        1,
        "research retry was not admitted after native group quiescence: started={} deferred={} blocked={}; {}",
        retry_report.research_started,
        retry_report.research_deferred,
        retry_report.research_blocked,
        research_retry_diagnostic(&harness.db, &review.review_id)
    );
    let after_retry = research_crash_snapshot(&harness.db, &review.review_id);
    assert_eq!(after_retry.run_count, before_retry.run_count + 1);
    assert_eq!(
        after_retry.reservation_count,
        before_retry.reservation_count + 1
    );
    assert_eq!(retry_daemon.run_once().await.unwrap().research_started, 0);
    assert_eq!(
        research_crash_snapshot(&harness.db, &review.review_id).run_count,
        after_retry.run_count
    );
    scope.disarm();
}

#[tokio::test]
async fn daemon_run_once_invokes_reconciliation_detection_termination_and_scheduler() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    let scheduled = harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.reconciliation.observed_task_count, 1);
    assert_eq!(harness.fake_pueue.status_calls(), 2);
    assert_eq!(harness.observation_count(), 1);
    assert_eq!(harness.incident_count(), 1);
    assert_eq!(harness.fake_pueue.kill_calls(), vec![41]);
    assert_eq!(harness.agent_run_count(), 1);
    assert_eq!(harness.event_status(scheduled), EventStatus::Dispatched);
    assert_eq!(report.finished_agents, 0);
}

#[tokio::test]
async fn daemon_schedules_deep_check_after_interval_for_running_task() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    let report = harness.run_once_at(3_700).await;

    assert_eq!(report.scheduled_deep_checks, 1);
    assert_eq!(harness.project_event_count(EventKind::DeepCheck), 1);
}

#[tokio::test]
async fn daemon_does_not_schedule_deep_check_while_agent_is_active() {
    let harness = DaemonHarness::running_task_with_deep_check_interval(30);
    harness.fake_pueue.set_tasks(Vec::new());
    let mut daemon = harness.daemon_at(3_700);
    daemon.run_once().await.unwrap();
    harness.fake_pueue.set_tasks(vec![running_task()]);
    harness.insert_active_agent_run();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.scheduled_deep_checks, 0);
    assert_eq!(harness.project_event_count(EventKind::DeepCheck), 0);
}

#[tokio::test]
async fn daemon_shutdown_is_graceful() {
    let harness = DaemonHarness::new();
    let mut daemon = harness.daemon();
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.fake_pueue.wait_for_status().await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), join)
        .await
        .expect("daemon should stop within the graceful shutdown timeout")
        .expect("daemon task should not panic");

    result.unwrap();
}

#[tokio::test]
async fn daemon_shutdown_drains_child_agent_that_finishes_promptly() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 0.1"],
        1,
    );
    harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");
    let mut daemon = harness.daemon();
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("daemon should drain promptly")
        .expect("daemon task should not panic")
        .unwrap();

    assert_eq!(
        harness.agent_run_statuses(),
        vec![AgentRunStatus::TimedOut]
    );
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn daemon_shutdown_bounds_long_running_child_agent_and_marks_it_terminal() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    harness.enqueue(EventKind::DeepCheck, "project-a", "deep-check");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(3),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), join)
        .await
        .expect("daemon should not hang indefinitely on a long-running child")
        .expect("daemon task should not panic")
        .unwrap();

    assert_eq!(harness.agent_run_statuses(), vec![AgentRunStatus::TimedOut]);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn daemon_shutdown_retains_handle_when_finalizer_exhausts_grace() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-finalizer");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_shutdown_terminal_run
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected shutdown finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(100),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    shutdown.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), join)
        .await
        .expect("daemon should return after bounded shutdown grace")
        .expect("daemon task should not panic");
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::Dispatched);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn daemon_shutdown_database_lock_respects_global_deadline_and_retains_terminal_outcome() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-database-lock");
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_secs(60),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(1_500),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let result = daemon.run(shutdown).await;
            (daemon, result)
        }
    });
    harness.wait_for_active_agent().await;
    harness.wait_for_native_dispatch(event_id).await;
    let lock = harness.db.connect().unwrap();
    lock.execute_batch("BEGIN IMMEDIATE;").unwrap();

    let started = Instant::now();
    shutdown.cancel();
    let (mut daemon, result) = join.await.expect("daemon task should not panic");
    assert!(result.is_err());
    assert!(
        started.elapsed() < Duration::from_millis(2_500),
        "SQLite finalization must honor the shared shutdown deadline",
    );
    assert_eq!(harness.event_status(event_id), EventStatus::Dispatched);
    drop(lock);

    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_attempts_later_agent_after_first_finalizer_persists() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_a = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-round-robin-a");
    let event_b = harness.enqueue(EventKind::DeepCheck, "project-b", "shutdown-round-robin-b");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_a_shutdown_finalizer
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.project_id = 'project-a' AND NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected persistent project-a finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(100),
        },
    );
    daemon.run_once().await.unwrap();
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let started = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(5), daemon.run(shutdown))
        .await
        .expect("all agent owners must receive a shutdown attempt");
    assert!(
        started.elapsed() < Duration::from_millis(750),
        "global shutdown grace must bound the whole retained-owner pass"
    );
    assert!(result.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::Dispatched);
    assert_eq!(harness.event_status(event_b), EventStatus::RetryWait);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM agent_runs WHERE project_id = 'project-b'",
                [],
                |row| row.get::<_, AgentRunStatus>(0),
            )
            .unwrap(),
        AgentRunStatus::TimedOut
    );
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_retries_transient_finalizer_failure_with_same_handle() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    let event_id = harness.enqueue(EventKind::DeepCheck, "project-a", "shutdown-retry");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_one_shutdown_finalizer
             BEFORE UPDATE OF status ON agent_runs
             WHEN NEW.status = 'timed_out'
             BEGIN
                 SELECT RAISE(ABORT, 'injected one-shot shutdown finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(3),
        },
    );
    let shutdown = CancellationToken::new();
    let join = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { daemon.run(shutdown).await }
    });

    harness.wait_for_active_agent().await;
    let pid: i32 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT pid FROM agent_runs WHERE status IN ('starting', 'running')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let drop_trigger = tokio::spawn({
        let db = harness.db.clone();
        async move {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if !process_exists(pid) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("agent process should terminate during shutdown");
            tokio::time::sleep(Duration::from_millis(100)).await;
            db.connect()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_one_shutdown_finalizer;")
                .unwrap();
        }
    });

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(4), join)
        .await
        .expect("daemon should retry the transient shutdown finalizer failure")
        .expect("daemon task should not panic")
        .unwrap();
    drop_trigger.await.unwrap();

    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_statuses(), vec![AgentRunStatus::TimedOut]);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn daemon_retains_and_retries_unresolved_bound_cleanup_after_scheduler_error() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "runner-restore");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_runner_restore_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_runner_restore_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected finalizer failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await;
    assert!(first.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_runner_restore_dispatch_ack;
             DROP TRIGGER reject_runner_restore_finalizer;",
        )
        .unwrap();
    let second = daemon.run_once().await;
    assert!(second.is_ok(), "runner and cleanup owner should survive scheduler error");
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_shutdown_keeps_bound_cleanup_after_repeated_finalizer_failure() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "bound-cleanup-shutdown");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_bound_cleanup_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_bound_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected bound cleanup finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_millis(150),
        },
    );

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(3), daemon.run(shutdown))
        .await
        .expect("bound cleanup shutdown retry must remain bounded")
        .is_err());
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_some());

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "DROP TRIGGER reject_bound_cleanup_dispatch_ack;
             DROP TRIGGER reject_bound_cleanup_finalizer;",
        )
        .unwrap();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert!(AgentRunRepository::new(&harness.db)
        .find_active_by_project("project-a")
        .unwrap()
        .is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_error_drain_preserves_started_and_bound_cleanup_owners_together() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let started_event = harness.enqueue(EventKind::TaskFinished, "project-a", "mixed-started");
    let cleanup_event = harness.enqueue(EventKind::TaskFailed, "project-b", "mixed-cleanup");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_b_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-b' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_project_b_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected cleanup finalizer failure');
             END;",
        )
        .unwrap();
    let mut daemon = Daemon::new(
        harness.db.clone(),
        harness.fake_pueue.clone(),
        harness.policy(),
        harness.runner(),
        DaemonConfig {
            interval: Duration::from_millis(10),
            lease_seconds: 60,
            claim_limit: 100,
            now_override: Some(harness.now),
            shutdown_grace_period: Duration::from_secs(5),
        },
    );
    let drop_trigger = tokio::spawn({
        let db = harness.db.clone();
        async move {
            let pid = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let pid = db
                        .connect()
                        .unwrap()
                        .query_row(
                            "SELECT pid FROM agent_runs
                             WHERE project_id = 'project-b' AND pid IS NOT NULL",
                            [],
                            |row| row.get::<_, i32>(0),
                        )
                        .ok();
                    if let Some(pid) = pid {
                        break pid;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("project-b cleanup child should start");
            tokio::time::timeout(Duration::from_secs(5), async {
                while process_exists(pid) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("project-b cleanup child should terminate");
            tokio::time::sleep(Duration::from_millis(100)).await;
            db.connect()
                .unwrap()
                .execute_batch("DROP TRIGGER reject_project_b_cleanup_finalizer;")
                .unwrap();
        }
    });

    let result = tokio::time::timeout(
        Duration::from_secs(15),
        daemon.run(CancellationToken::new()),
    )
    .await
    .expect("daemon error drain must remain bounded");
    assert!(result.is_err(), "the original scheduler error must remain visible");
    drop_trigger.await.unwrap();
    assert_eq!(harness.event_status(started_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(cleanup_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_cleanup_queue_retries_each_owner_without_loss_across_ticks() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let event_a = harness.enqueue(EventKind::TaskFailed, "project-a", "cleanup-queue-a");
    let event_b = harness.enqueue(EventKind::TaskFailed, "project-b", "cleanup-queue-b");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_cleanup_queue_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_cleanup_queue_a_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-a cleanup failure');
             END;
             CREATE TRIGGER reject_cleanup_queue_b_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b cleanup failure');
             END;",
        )
        .unwrap();
    let mut daemon = harness.daemon();

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::InFlight);
    assert_eq!(harness.event_status(event_b), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cleanup_queue_b_finalizer;")
        .unwrap();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(event_a), EventStatus::InFlight);
    assert_eq!(harness.event_status(event_b), EventStatus::DeadLetter);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cleanup_queue_a_finalizer;")
        .unwrap();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(event_a), EventStatus::DeadLetter);
    assert_eq!(harness.event_status(event_b), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0,
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_poll_attempts_cleanup_when_an_agent_finalizer_fails_in_the_same_tick() {
    let harness = DaemonHarness::new();
    let release_path = harness.temp.path().join("cross-poll-agent-release");
    let release_text = release_path.to_str().unwrap();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["--wait-for-release", release_text],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/bin/sh",
        &["-c", "sleep 30"],
        1,
    );
    let agent_event = harness.enqueue(EventKind::TaskFinished, "project-a", "cross-poll-agent");
    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !PathBuf::from(format!("{}.ready", release_path.display())).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("project-a fixture should report readiness");

    let cleanup_event = harness.enqueue(EventKind::TaskFailed, "project-b", "cross-poll-cleanup");
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_cross_poll_agent_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-a'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-a finalizer failure');
             END;
             CREATE TRIGGER reject_cross_poll_cleanup_dispatch_ack
             BEFORE UPDATE OF launch_gate_state ON agent_runs
             WHEN NEW.project_id = 'project-b' AND NEW.launch_gate_state = 'released'
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b dispatch acknowledgement failure');
             END;
             CREATE TRIGGER reject_cross_poll_cleanup_finalizer
             BEFORE UPDATE OF status ON events
             WHEN NEW.project_id = 'project-b'
                  AND NEW.status IN ('completed', 'retry_wait', 'dead_letter')
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b cleanup failure');
             END;",
        )
        .unwrap();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    assert_eq!(harness.event_status(cleanup_event), EventStatus::InFlight);
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_cross_poll_cleanup_finalizer;")
        .unwrap();
    fs::write(&release_path, b"release").unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(agent_event), EventStatus::Dispatched);
    assert_eq!(
        harness.event_status(cleanup_event),
        EventStatus::DeadLetter,
        "a failing agent finalizer must not starve a healthy cleanup owner",
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_retains_started_agent_when_a_later_project_scheduler_error_occurs() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 1"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/path/that/does/not/exist/pueue-agent",
        &["{prompt}"],
        1,
    );
    let successful_event = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "mixed-success",
    );
    let failed_event = harness.enqueue(EventKind::TaskFailed, "project-b", "mixed-error");

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await;
    assert!(first.is_err());
    assert_eq!(harness.event_status(successful_event), EventStatus::Dispatched);
    assert_eq!(harness.event_status(failed_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    while harness.event_status(successful_event) == EventStatus::Dispatched {
        assert!(Instant::now() < deadline, "retained agent did not reach terminal state");
        tokio::time::sleep(Duration::from_millis(100)).await;
        daemon.run_once().await.unwrap();
    }
    assert_eq!(harness.event_status(successful_event), EventStatus::Completed);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE project_id = 'project-a' AND status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn daemon_run_drains_started_agent_before_returning_scheduler_error() {
    let harness = DaemonHarness::new();
    harness.register_project_with_agent(
        "project-a",
        "pa-project",
        "/bin/sh",
        &["-c", "sleep 10"],
        1,
    );
    harness.register_project_with_agent(
        "project-b",
        "pb-project",
        "/path/that/does/not/exist/pueue-agent",
        &["{prompt}"],
        1,
    );
    let successful_event = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "run-mixed-success",
    );
    let failed_event = harness.enqueue(EventKind::TaskFailed, "project-b", "run-mixed-error");

    let mut daemon = harness.daemon();
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        daemon.run(CancellationToken::new()),
    )
    .await
    .expect("daemon should drain retained agents before returning");
    let error = result.expect_err("later scheduler failure should remain visible");
    assert!(error.to_string().contains("policy_blocked"));
    assert_eq!(harness.event_status(successful_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(failed_event), EventStatus::DeadLetter);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM agent_runs
                 WHERE status IN ('starting', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    let pid: i32 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT pid FROM agent_runs WHERE project_id = 'project-a'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!process_exists(pid));
}

#[tokio::test]
async fn injected_shutdown_signal_cancels_daemon_token() {
    let shutdown = CancellationToken::new();
    let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(pueue_agent::daemon::cancel_token_on_shutdown_signal(
        shutdown.clone(),
        async move {
            let _ = receiver.await;
        },
    ));

    assert!(!shutdown.is_cancelled());
    sender.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), shutdown.cancelled())
        .await
        .expect("injected signal should cancel token");
    task.await.unwrap();
}

#[tokio::test]
async fn daemon_restart_recovers_expired_claims_into_a_finite_paused_wait() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(
        EventKind::TaskFinished,
        "project-a",
        "finished-before-crash",
    );
    harness.claim_with_lease(event_id, harness.now - 1);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::RetryWait);
    assert_eq!(event.not_before, harness.now + 60);
    assert_eq!(event.attempts, 0);
    assert_eq!(event.lease_until, None);
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_confirms_release_marker_through_project_root_descriptor() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "descriptor-marker");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(pueue_agent::agent::relative_log_path(event_id, 190));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    let gate: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(gate, "released");
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_closes_pending_marker_evidence_crash_window() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "pending-marker-crash-window");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(pueue_agent::agent::relative_log_path(event_id, 190));
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(report.requeued_agent_events, 0);
    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::DeadLetter);
    assert_eq!(event.attempts, 0);
    assert!(marker_path.exists());
    let state: (AgentRunStatus, String, Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state, policy_code, failure_stage
             FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            AgentRunStatus::Failed,
            "failed".to_owned(),
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        ),
    );
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_retries_pre_marker_and_dead_letters_post_marker() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pa-project-b", "/bin/echo");
    harness.pause_project("project-a");
    harness.pause_project("project-b");

    let pre_event = harness.enqueue(EventKind::TaskFailed, "project-a", "pre-marker-recovery");
    let post_event = harness.enqueue(EventKind::TaskFailed, "project-b", "post-marker-recovery");
    for event_id in [pre_event, post_event] {
        harness.claim_with_lease(event_id, harness.now + 600);
    }

    let runs = AgentRunRepository::new(&harness.db);
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-a",
            pre_event,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.registered_root("project-a").join(format!(
                ".pueue-agent/logs/agent-190-{pre_event}.log"
            )),
        ),
        &[pre_event],
    )
    .unwrap();

    let interventions = InterventionRepository::new(&harness.db);
    let intervention_id = interventions
        .insert_pending("project-b", "already delivered", harness.now - 20)
        .unwrap()
        .intervention_id;
    interventions
        .reserve_pending(
            "project-b",
            "post-marker-token",
            harness.now - 10,
            harness.now + 600,
            1,
            "already delivered".len(),
        )
        .unwrap();
    let post_log = harness
        .registered_root("project-b")
        .join(pueue_agent::agent::relative_log_path(post_event, 190));
    let post_run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-b",
                post_event,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &post_log,
            ),
            &[post_event],
            Some("post-marker-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions(
        "project-b",
        post_run.run_id,
        42_424,
        harness.now - 5,
    )
    .unwrap();
    runs.mark_gate_release_requested("project-b", post_run.run_id)
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", post_log.display()));
    fs::write(&marker_path, b"authorized\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.requeued_agent_events, 1);
    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.event_status(pre_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(post_event), EventStatus::DeadLetter);
    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Applied, Some(post_run.run_id))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_dead_letters_indeterminate_relative_marker_without_releasing_intervention() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "indeterminate-marker");
    harness.claim_with_lease(event_id, harness.now + 600);

    let intervention_id =
        harness.reserve_intervention("already delivered", "indeterminate-token", harness.now + 600);
    let log_path = harness
        .registered_root("project-a")
        .join(pueue_agent::agent::relative_log_path(event_id, 190));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
            Some("indeterminate-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"not an authorization marker\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(report.requeued_agent_events, 0);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Applied, Some(run.run_id))
    );
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_dead_letters_indeterminate_pending_marker_as_post_marker_policy() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "pending-indeterminate");
    harness.claim_with_lease(event_id, harness.now + 600);
    let log_path = harness
        .registered_root("project-a")
        .join(pueue_agent::agent::relative_log_path(event_id, 190));
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"not an authorization marker\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    let state: (String, Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT launch_gate_state, policy_code, failure_stage
             FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            "failed".to_owned(),
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        )
    );
}

#[cfg(unix)]
#[tokio::test]
async fn recovery_dead_letters_running_pending_indeterminate_marker_and_retains_applied_intervention() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "running-pending-marker");
    harness.claim_with_lease(event_id, harness.now + 600);
    let intervention_id = harness.reserve_intervention(
        "already delivered",
        "running-pending-token",
        harness.now + 600,
    );
    let log_path = harness
        .registered_root("project-a")
        .join(pueue_agent::agent::relative_log_path(event_id, 190));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
            Some("running-pending-token"),
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    let marker_path = PathBuf::from(format!("{}.gate-started", log_path.display()));
    fs::write(&marker_path, b"not an authorization marker\n").unwrap();
    fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o600)).unwrap();

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(report.requeued_agent_events, 0);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Applied, Some(run.run_id))
    );
    let state: (String, Option<String>, Option<String>) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT launch_gate_state, policy_code, failure_stage
             FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        state,
        (
            "released".to_owned(),
            Some("native_gate_failed".to_owned()),
            Some("post_marker".to_owned()),
        )
    );
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_rejects_symlinked_log_directory_without_database_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "symlinked-log-dir");
    harness.claim_with_lease(event_id, harness.now + 600);
    let project_root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = project_root.join(pueue_agent::agent::relative_log_path(event_id, 190));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let mut daemon = harness.daemon();
    let logs = harness.root("project-a").join(".pueue-agent/logs");
    let retained_logs = harness.root("project-a").join(".pueue-agent/logs-retained");
    fs::rename(&logs, &retained_logs).unwrap();
    symlink(&retained_logs, &logs).unwrap();

    assert!(daemon.run_once().await.is_err());

    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let state: (AgentRunStatus, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Running, "release_requested".to_owned()));
}

#[cfg(unix)]
#[tokio::test]
async fn startup_recovery_rejects_replaced_project_root_without_database_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "replaced-root");
    harness.claim_with_lease(event_id, harness.now + 600);
    let root = ProjectRepository::new(&harness.db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap()
        .root_path;
    let log_path = root.join(pueue_agent::agent::relative_log_path(event_id, 190));
    let runs = AgentRunRepository::new(&harness.db);
    let run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42_424, harness.now - 5)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id)
        .unwrap();
    let mut daemon = harness.daemon();
    let config = fs::read(root.join(".pueue-agent/config.toml")).unwrap();
    let retained_root = harness.temp.path().join("project-a-retained");
    fs::rename(&root, &retained_root).unwrap();
    fs::create_dir_all(root.join(".pueue-agent")).unwrap();
    fs::write(root.join(".pueue-agent/config.toml"), config).unwrap();

    assert!(daemon.run_once().await.is_err());

    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let state: (AgentRunStatus, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Running, "release_requested".to_owned()));
}

#[tokio::test]
async fn intervention_recovery_returns_an_expired_unattached_reservation_to_pending() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let intervention_id =
        harness.reserve_intervention("expired instruction", "expired-token", harness.now);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn later_daemon_tick_recovers_unattached_intervention_after_startup_recovery() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let intervention_id =
        harness.reserve_intervention("expires after startup", "later-token", harness.now + 1);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Reserved, None)
    );

    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE interventions SET lease_expires_at = ?1 WHERE intervention_id = ?2",
            rusqlite::params![harness.now - 1, intervention_id],
        )
        .unwrap();

    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn intervention_recovery_applies_a_reserved_row_attached_to_a_run_with_a_pid_once() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "delivered-before-crash");
    let run_id = harness.insert_active_run("project-a", event_id, AgentRunStatus::Running);
    let intervention_id =
        harness.reserve_intervention("already delivered", "live-token", harness.now + 600);
    harness.attach_intervention(&intervention_id, run_id);

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();
    daemon.run_once().await.unwrap();
    let mut restarted_daemon = harness.daemon();
    restarted_daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn intervention_recovery_releases_a_reserved_row_attached_to_a_failed_pre_spawn_run() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "failed-before-spawn");
    let run_id = harness.insert_active_run("project-a", event_id, AgentRunStatus::Starting);
    let intervention_id =
        harness.reserve_intervention("not delivered", "failed-token", harness.now + 600);
    harness.attach_intervention(&intervention_id, run_id);
    AgentRunRepository::new(&harness.db)
        .finish(
            run_id,
            AgentRunStatus::Failed,
            harness.now - 5,
            None,
            Some("spawn failed"),
        )
        .unwrap();

    let mut daemon = harness.daemon();
    daemon.run_once().await.unwrap();

    assert_eq!(
        harness.intervention_state(&intervention_id),
        (InterventionStatus::Pending, None)
    );
}

#[tokio::test]
async fn first_daemon_cycle_recovers_persisted_runs_and_only_their_claimed_events_once() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pa-project-b", "/bin/echo");
    harness.pause_project("project-a");
    harness.pause_project("project-b");

    let starting_event = harness.enqueue(EventKind::TaskFailed, "project-a", "starting-event");
    let running_event = harness.enqueue(EventKind::TaskFailed, "project-b", "running-event");
    let completed_event = harness.enqueue(EventKind::TaskFinished, "project-a", "completed-event");
    let unrelated_claim = harness.enqueue(EventKind::DeepCheck, "project-a", "unrelated-claim");
    for event_id in [starting_event, running_event, unrelated_claim] {
        harness.claim_with_lease(event_id, harness.now + 600);
    }
    EventRepository::new(&harness.db)
        .transition_many(
            &[completed_event],
            EventStatus::Completed,
            harness.now - 5,
            None,
            None,
        )
        .unwrap();

    let runs = AgentRunRepository::new(&harness.db);
    let starting_run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                starting_event,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{starting_event}.log"
                )),
            ),
            &[starting_event],
        )
        .unwrap()
        .run_id;
    let running_run = runs
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                running_event,
                None,
                AgentRunStatus::Running,
                harness.now - 10,
                harness.registered_root("project-b").join(format!(
                    ".pueue-agent/logs/agent-190-{running_event}.log"
                )),
            ),
            &[running_event],
        )
        .unwrap()
        .run_id;

    let mut daemon = harness.daemon();
    let first = daemon.run_once().await.unwrap();
    let second = daemon.run_once().await.unwrap();
    let mut restarted_daemon = harness.daemon();
    let repeated_recovery = restarted_daemon.run_once().await.unwrap();

    assert_eq!(first.recovered_agent_runs, 2);
    assert_eq!(first.requeued_agent_events, 1);
    assert_eq!(first.dead_lettered_agent_events, 1);
    assert_eq!(second.recovered_agent_runs, 0);
    assert_eq!(second.requeued_agent_events, 0);
    assert_eq!(repeated_recovery.recovered_agent_runs, 0);
    assert_eq!(repeated_recovery.requeued_agent_events, 0);
    for run_id in [starting_run, running_run] {
        let (status, finished_at, reason) = harness.agent_run_state(run_id);
        assert_eq!(status, AgentRunStatus::Failed);
        assert_eq!(finished_at, Some(harness.now));
        assert!(reason
            .as_deref()
            .is_some_and(|reason| reason.contains("daemon restart")));
    }
    assert_eq!(harness.event_status(starting_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(running_event), EventStatus::DeadLetter);
    for event_id in [starting_event, running_event] {
        assert_eq!(
            EventRepository::new(&harness.db)
                .find_by_id(event_id)
                .unwrap()
                .unwrap()
                .lease_until,
            None
        );
    }
    assert_eq!(
        harness.event_status(completed_event),
        EventStatus::Completed
    );
    let unrelated = EventRepository::new(&harness.db)
        .find_by_id(unrelated_claim)
        .unwrap()
        .unwrap();
    assert_eq!(unrelated.status, EventStatus::Claimed);
    assert_eq!(unrelated.lease_until, Some(harness.now + 600));
}

#[tokio::test]
async fn startup_recovery_is_atomic_and_retried_after_a_database_failure() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "atomic-recovery");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run_id = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap()
        .run_id;
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_restart_recovery
             BEFORE UPDATE OF status ON agent_runs
             WHEN OLD.status IN ('starting', 'running') AND NEW.status = 'failed'
             BEGIN
                 SELECT RAISE(ABORT, 'injected recovery failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    let event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event.status, EventStatus::InFlight);
    assert_eq!(event.lease_until, None);
    assert_eq!(harness.agent_run_state(run_id).0, AgentRunStatus::Starting);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_restart_recovery;")
        .unwrap();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(report.requeued_agent_events, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_state(run_id).0, AgentRunStatus::Failed);
}

#[tokio::test]
async fn startup_recovery_loads_disabled_project_config() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pb-project", "/bin/echo");
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE projects SET enabled = 0, paused = 1 WHERE project_id = 'project-b'",
            [],
        )
        .unwrap();
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-b", "disabled-recovery");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-b",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-b").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap();

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Failed);
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
}

#[tokio::test]
async fn startup_recovery_rejects_project_identity_mismatch_before_mutation() {
    let harness = DaemonHarness::new();
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::TaskFailed, "project-a", "identity-mismatch");
    harness.claim_with_lease(event_id, harness.now + 600);
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap();
    let mut daemon = harness.daemon();
    let config_path = harness.root("project-a").join(".pueue-agent/config.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        config.replace("pueue_group = \"pa-project\"", "pueue_group = \"wrong-group\""),
    )
    .unwrap();

    assert!(daemon.run_once().await.is_err());
    let unchanged_event = EventRepository::new(&harness.db)
        .find_by_id(event_id)
        .unwrap()
        .unwrap();
    assert_eq!(unchanged_event.status, EventStatus::InFlight);
    assert_eq!(unchanged_event.lease_until, None);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Starting);

    fs::write(&config_path, config).unwrap();
    let report = daemon.run_once().await.unwrap();
    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
    assert_eq!(harness.agent_run_state(run.run_id).0, AgentRunStatus::Failed);
}

#[tokio::test]
async fn startup_recovery_commits_projects_independently_and_retries_failed_project() {
    let harness = DaemonHarness::new();
    harness.register_project("project-b", "pb-project", "/bin/echo");
    harness.pause_project("project-a");
    harness.pause_project("project-b");
    let first_event = harness.enqueue(EventKind::TaskFailed, "project-a", "project-a-recovery");
    let second_event = harness.enqueue(EventKind::TaskFailed, "project-b", "project-b-recovery");
    harness.claim_with_lease(first_event, harness.now + 600);
    harness.claim_with_lease(second_event, harness.now + 600);
    let runs = AgentRunRepository::new(&harness.db);
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-a",
            first_event,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.registered_root("project-a").join(format!(
                ".pueue-agent/logs/agent-190-{first_event}.log"
            )),
        ),
        &[first_event],
    )
    .unwrap();
    runs.insert_with_events(
        &NewAgentRun::new(
            "project-b",
            second_event,
            None,
            AgentRunStatus::Starting,
            harness.now - 10,
            harness.registered_root("project-b").join(format!(
                ".pueue-agent/logs/agent-190-{second_event}.log"
            )),
        ),
        &[second_event],
    )
    .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER reject_project_b_recovery
             BEFORE UPDATE OF status ON agent_runs
             WHEN OLD.project_id = 'project-b' AND NEW.status = 'failed'
             BEGIN
                 SELECT RAISE(ABORT, 'injected project-b recovery failure');
             END;",
        )
        .unwrap();

    let mut daemon = harness.daemon();
    assert!(daemon.run_once().await.is_err());
    assert_eq!(harness.event_status(first_event), EventStatus::RetryWait);
    assert_eq!(harness.event_status(second_event), EventStatus::InFlight);

    harness
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER reject_project_b_recovery;")
        .unwrap();
    let report = daemon.run_once().await.unwrap();
    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(harness.event_status(second_event), EventStatus::RetryWait);
}

#[tokio::test]
async fn startup_recovery_runs_before_the_first_scheduling_pass() {
    let harness = DaemonHarness::new();
    let event_id = harness.enqueue(EventKind::TaskFinished, "project-a", "restart-dispatch");
    harness.claim_with_lease(event_id, harness.now + 600);
    let interrupted = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Running,
                harness.now - 10,
                harness.registered_root("project-a").join(format!(
                    ".pueue-agent/logs/agent-190-{event_id}.log"
                )),
            ),
            &[event_id],
        )
        .unwrap()
        .run_id;

    let mut daemon = harness.daemon();
    let report = daemon.run_once().await.unwrap();

    assert_eq!(report.recovered_agent_runs, 1);
    assert_eq!(report.dead_lettered_agent_events, 1);
    assert_eq!(harness.agent_run_count(), 1);
    assert_eq!(
        harness.agent_run_state(interrupted).0,
        AgentRunStatus::Failed
    );
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_worktree_lifecycle_preserves_original_and_cleans_owned_candidate() {
    let temp = TempDir::new().unwrap();
    let fixture_root = fs::canonicalize(temp.path()).unwrap();
    let project_root = fixture_root.join("project");
    fs::create_dir(&project_root).unwrap();
    fs::create_dir(project_root.join(".pueue-agent")).unwrap();
    fs::set_permissions(
        &project_root,
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::set_permissions(
        project_root.join(".pueue-agent"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    fs::write(
        project_root.join(".gitignore"),
        ".pueue-agent/\n.env\ncredentials.json\n",
    )
    .unwrap();
    fs::write(project_root.join("model.py"), "score = 1\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", "."]);
    run_git(&["commit", "-q", "-m", "base"]);
    let original_main = String::from_utf8(run_git(&["rev-parse", "refs/heads/main"]).stdout)
        .unwrap()
        .trim()
        .to_owned();

    // The Linux native launcher requires an ELF target for execveat(AT_EMPTY_PATH).
    // Keep the policy fixture's launcher but replace its optional Git fixture with
    // the startup-pinned system Git before resolving the immutable policy.
    let trusted_git = fixture_root.join("execution-policy-bin/git");
    fs::create_dir_all(trusted_git.parent().unwrap()).unwrap();
    fs::copy("/usr/bin/git", &trusted_git).unwrap();
    fs::set_permissions(&trusted_git, fs::Permissions::from_mode(0o700)).unwrap();
    let codex_program = PathBuf::from("codex");
    let policy = execution_policy_fixture::resolved_policy(
        &fixture_root,
        &[("project-a", &project_root, codex_program.as_path())],
    );
    let project = Project {
        project_id: "project-a".to_owned(),
        root_path: fs::canonicalize(&project_root).unwrap(),
        pueue_group: "pa-test".to_owned(),
        config_path: project_root.join(".pueue-agent/config.toml"),
        enabled: true,
        paused: false,
        halted_reason: None,
        created_at: 0,
        updated_at: 0,
    };
    let config = pueue_agent::config::ProjectConfig {
        project_id: "project-a".to_owned(),
        pueue_group: "pa-test".to_owned(),
        agent: pueue_agent::config::AgentConfig {
            program: "codex".to_owned(),
            args: vec!["{prompt}".to_owned()],
            timeout_minutes: 1,
            max_retries: 0,
            context: AgentContextMode::Fresh,
            execution: pueue_agent::config::AgentExecutionConfig {
                network: NetworkMode::Enabled,
            },
            codex: pueue_agent::config::AgentCodexConfig {
                model: None,
                reasoning_effort: None,
            },
        },
        check: pueue_agent::config::CheckConfig {
            interval_minutes: 1,
            deep_check_interval_minutes: 0,
            stall_minutes: 1,
            log_tail_bytes: 1,
            extra_log_paths: Vec::new(),
            patterns: Vec::new(),
            stall: pueue_agent::config::StallConfig {
                action: pueue_agent::config::PatternAction::Notify,
                kill_after_minutes: 0,
            },
        },
        guardrails: pueue_agent::config::GuardrailsConfig {
            max_consecutive_failures: 1,
            max_experiments: 1,
            max_agent_runs: 1,
        },
    };
    let original = resolve_project_policy(&policy, &project, &config).unwrap();
    // Establish the durable run and one terminal authoritative editor lineage
    // before opening the candidate.  Task 3 preparation is DB-authorized;
    // cleanup must not infer liveness from an absent joined row.
    let cleanup_db = Db::open(&fixture_root.join("cleanup.sqlite3")).unwrap();
    let cleanup_connection = cleanup_db.connect().unwrap();
    cleanup_connection
        .execute(
            "INSERT INTO projects (project_id, root_path, pueue_group, config_path,
                                   enabled, paused, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 1, 0, 1, 1)",
            rusqlite::params![
                "project-a",
                project_root.to_string_lossy(),
                "pa-test",
                project_root.join(".pueue-agent/config.toml").to_string_lossy(),
            ],
        )
        .unwrap();
    cleanup_connection
        .execute(
            "INSERT INTO campaigns (campaign_id, project_id, objective_text,
                                    objective_digest, initial_argv_json, state,
                                    base_revision_sha, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, 1, 1)",
            rusqlite::params![
                "campaign-a",
                "project-a",
                "cleanup",
                "digest",
                "[]",
                &original_main,
            ],
        )
        .unwrap();
    cleanup_connection
        .execute(
            "INSERT INTO proposals (proposal_id, campaign_id, kind, status,
                                    hypothesis, argv_json, working_directory,
                                    expected_evidence_json, canonical_digest,
                                    created_at, updated_at)
             VALUES (?1, ?2, 'code_change', 'accepted', ?3, ?4, '.', ?5, ?6, 1, 1)",
            rusqlite::params!["proposal-a", "campaign-a", "cleanup", "[]", "[]", "proposal"],
        )
        .unwrap();
    let run_id = "cleanup-run";
    cleanup_connection
        .execute(
            "INSERT INTO code_change_runs (
                 code_change_run_id, proposal_id, campaign_id, state, base_sha,
                 candidate_sha, candidate_ref, best_ref, worktree_id,
                 worktree_relative_path, editor_attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'reserved', ?4, NULL, ?5, ?6, ?7, ?8, 0, 1, 1)",
            rusqlite::params![
                run_id,
                "proposal-a",
                "campaign-a",
                original_main,
                pueue_agent::code_change::candidate_ref("campaign-a", "proposal-a").unwrap(),
                "campaign/campaign-a/best",
                run_id,
                ".pueue-agent/worktrees/campaign-a/proposal-a",
            ],
        )
        .unwrap();
    drop(cleanup_connection);
    let event = EventRepository::new(&cleanup_db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::CodeChange,
            "task3-editor-lineage",
            json!({}),
            1,
            1,
        ))
        .unwrap();
    let editor_run = AgentRunRepository::new(&cleanup_db)
        .insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            None,
            AgentRunStatus::Completed,
            1,
            fixture_root.join("editor.log"),
        ))
        .unwrap();
    let candidate_base = pueue_agent::code_change::prepare_code_change_worktree_for_run(
        &policy,
        &project,
        &original,
        &cleanup_db,
        run_id,
    )
    .await
    .unwrap();
    let candidate_path = candidate_base.path().to_owned();
    let candidate_git = candidate_path.join(".git");
    let candidate_git_backup = candidate_path.join(".git.original");
    fs::rename(&candidate_git, &candidate_git_backup).unwrap();
    fs::write(
        &candidate_git,
        format!("gitdir: {}\n", project_root.join(".git").display()),
    )
    .unwrap();
    fs::write(candidate_path.join("model.py"), "score = 99\n").unwrap();
    let mut candidate = candidate_base;
    let redirected = candidate.verify().await;
    assert!(redirected.is_err(), "candidate admin redirect must be rejected");
    assert_eq!(
        String::from_utf8(run_git(&["rev-parse", "refs/heads/main"]).stdout)
            .unwrap()
            .trim(),
        original_main
    );
    assert_eq!(
        fs::read_to_string(project_root.join("model.py")).unwrap(),
        "score = 1\n"
    );
    fs::remove_file(&candidate_git).unwrap();
    fs::rename(candidate_git_backup, &candidate_git).unwrap();
    let admin_path = PathBuf::from(
        String::from_utf8(fs::read(&candidate_git).unwrap())
            .unwrap()
            .trim()
            .strip_prefix("gitdir:")
            .unwrap()
            .trim(),
    );
    let worktree_config = admin_path.join("config.worktree");
    fs::write(&worktree_config, "[include]\npath = /tmp/sentinel\n").unwrap();
    let config_channel = candidate.verify().await;
    assert!(config_channel.is_err(), "linked worktree config channels must be rejected");
    fs::remove_file(worktree_config).unwrap();
    fs::write(candidate_path.join(".env"), "secret=must-not-disappear\n").unwrap();
    let ignored_secret = candidate.verify().await;
    assert!(ignored_secret.is_err(), "ignored credentials must be rejected");
    fs::remove_file(candidate_path.join(".env")).unwrap();
    fs::create_dir(candidate_path.join(".pueue-agent")).unwrap();
    fs::write(candidate_path.join(".pueue-agent/state"), "state").unwrap();
    let ignored_service = candidate.verify().await;
    assert!(ignored_service.is_err(), "ignored service state must be rejected");
    fs::remove_file(candidate_path.join(".pueue-agent/state")).unwrap();
    fs::remove_dir(candidate_path.join(".pueue-agent")).unwrap();
    fs::write(candidate_path.join("model.py"), "score = 2\n").unwrap();
    let facts = candidate.verify().await.unwrap();
    assert_eq!(facts.file_count, 1);
    assert!(facts.diff_bytes > 0);
    let candidate_sha = candidate.commit().await.unwrap();
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        "campaign-a",
        "proposal-a",
    )
    .unwrap();
    let observed_ref = String::from_utf8(
        run_git(&["rev-parse", &format!("refs/heads/{candidate_ref}")]).stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(observed_ref, candidate_sha);
    assert_eq!(
        String::from_utf8(run_git(&["rev-parse", "refs/heads/main"]).stdout)
            .unwrap()
            .trim(),
        original_main
    );
    assert_eq!(fs::read_to_string(project_root.join("model.py")).unwrap(), "score = 1\n");
    // Record the committed candidate and terminal editor attempt through the
    // durable run row before exercising public best-ref and cleanup paths.
    let cleanup_connection = cleanup_db.connect().unwrap();
    cleanup_connection
        .execute(
            "UPDATE code_change_runs
             SET state = 'candidate_ready', candidate_sha = ?1,
                 updated_at = 2
             WHERE code_change_run_id = ?2 AND candidate_sha IS NULL",
            rusqlite::params![candidate_sha, run_id],
        )
        .unwrap();
    cleanup_connection
        .execute(
            "INSERT INTO code_change_editor_attempts (
                 code_change_run_id, attempt, agent_run_id, editor_session_id,
                 status, result_digest, started_at, finished_at)
             VALUES (?1, 1, ?2, 'editor-session', 'ready', 'digest', 1, 2)",
            rusqlite::params![run_id, editor_run.run_id],
        )
        .unwrap();
    drop(cleanup_connection);
    let authorization =
        pueue_agent::code_change::CodeChangeCleanupAuthorization::load(&cleanup_db, "cleanup-run")
            .unwrap();
    candidate
        .update_best_ref_cas(&authorization, None)
        .await
        .unwrap();
    let best_ref = pueue_agent::code_change::best_ref("campaign-a").unwrap();
    assert_eq!(
        String::from_utf8(run_git(&["rev-parse", &format!("refs/heads/{best_ref}")]).stdout)
            .unwrap()
            .trim(),
        candidate_sha
    );
    assert!(candidate.update_best_ref_cas(&authorization, None).await.is_err());
    candidate.cleanup(&authorization).await.unwrap();
    assert!(!candidate_path.exists());
    assert_eq!(
        String::from_utf8(run_git(&["rev-parse", &format!("refs/heads/{candidate_ref}")]).stdout)
            .unwrap()
            .trim(),
        candidate_sha
    );
    let leaked_indexes = fs::read_dir(fixture_root.join("execution-policy-state"))
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".code-change-index-")
        })
        .count();
    assert_eq!(leaked_indexes, 0, "temporary candidate indexes must be cleaned");
}

fn seed_code_change_editor_recovery_fixture(
    harness: &DaemonHarness,
    execution_kind: &str,
    bind_attempt: bool,
) -> (i64, String, i64) {
    harness.pause_project("project-a");
    let event_id = harness.enqueue(EventKind::CodeChange, "project-a", "editor-recovery");
    harness.claim_with_lease(event_id, harness.now + 600);
    let log_path = harness
        .registered_root("project-a")
        .join(".pueue-agent/logs/editor-recovery.log");
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event_id,
                None,
                AgentRunStatus::Starting,
                harness.now - 10,
                &log_path,
            ),
            &[event_id],
        )
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_runs
             SET execution_kind = ?1, executable_path = '/bin/echo',
                 executable_identity = 'fixture'
             WHERE run_id = ?2",
            rusqlite::params![execution_kind, run.run_id],
        )
        .unwrap();

    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO campaigns (
                 campaign_id, project_id, objective_text, objective_digest,
                 initial_argv_json, state, created_at, updated_at
             ) VALUES ('editor-campaign', 'project-a', 'editor', 'editor-digest', '[]', 'active', 1, 1)",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis, argv_json,
                 working_directory, expected_evidence_json, canonical_digest,
                 created_at, updated_at
             ) VALUES ('editor-proposal', 'editor-campaign', 'code_change', 'accepted',
                       'editor', '[]', '.', '[]', 'editor-proposal-digest', 1, 1)",
            [],
        )
        .unwrap();
    let code_change_run_id = "editor-code-change".to_owned();
    connection
        .execute(
            "INSERT INTO code_change_runs (
                 code_change_run_id, proposal_id, campaign_id, state, base_sha,
                 candidate_ref, best_ref, worktree_id, worktree_relative_path,
                 editor_session_id, editor_attempts, created_at, updated_at
             ) VALUES (?1, 'editor-proposal', 'editor-campaign', 'editing',
                       '0000000000000000000000000000000000000000',
                       'campaign/editor-campaign/candidate/editor-proposal',
                       'campaign/editor-campaign/best',
                       'editor-worktree', '.pueue-agent/worktrees/editor',
                       'editor-session', 1, 1, 1)",
            [&code_change_run_id],
        )
        .unwrap();
    if bind_attempt {
        connection
            .execute(
                "INSERT INTO code_change_editor_attempts (
                     code_change_run_id, attempt, agent_run_id, editor_session_id,
                     status, started_at
                 ) VALUES (?1, 1, ?2, 'editor-session', 'reserved', 1)",
                rusqlite::params![code_change_run_id, run.run_id],
            )
            .unwrap();
    }
    (run.run_id, code_change_run_id, event_id)
}

#[tokio::test]
async fn code_change_editor_is_preserved_from_generic_startup_recovery() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);

    let policies = BTreeMap::from([(
        "project-a".to_owned(),
        RetryPolicy { max_retries: 1 },
    )]);
    let recovery = AgentRunRepository::new(&harness.db)
        .recover_interrupted_with_marker_evidence(
            harness.now,
            "daemon restart",
            &policies,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .unwrap();

    assert_eq!(recovery.failed_runs, 0);
    assert_eq!(recovery.preserved_code_change_editors, 1);
    assert_eq!(harness.event_status(event_id), EventStatus::InFlight);
    let state: (AgentRunStatus, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status, launch_gate_state FROM agent_runs WHERE run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, (AgentRunStatus::Starting, "pending".to_owned()));
    let attempt_status: String = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM code_change_editor_attempts
             WHERE code_change_run_id = ?1 AND attempt = 1",
            [&code_change_run_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempt_status, "reserved");
}

#[cfg(all(unix, target_os = "linux"))]
fn startup_editor_runner(
    harness: &DaemonHarness,
) -> (
    Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy>,
    AgentRunner,
) {
    let policy = harness.policy();
    let runner = AgentRunner::new(
        AgentRunnerConfig::production().with_codex_capabilities(
            pueue_agent::codex_command::CodexCapabilities::all(),
        ),
        Arc::clone(&policy),
    );
    (policy, runner)
}

#[cfg(all(unix, target_os = "linux"))]
fn startup_editor_retry_policies(max_retries: u32) -> BTreeMap<String, RetryPolicy> {
    BTreeMap::from([(
        "project-a".to_owned(),
        RetryPolicy { max_retries },
    )])
}

#[cfg(all(unix, target_os = "linux"))]
const VALID_EDITOR_RESULT_DIGEST: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[cfg(all(unix, target_os = "linux"))]
fn update_startup_editor_fixture(
    harness: &DaemonHarness,
    run_id: i64,
    code_change_run_id: &str,
    event_id: i64,
    agent_status: &str,
    launch_gate_state: &str,
    event_status: &str,
    attempt_status: &str,
    failure_code: Option<&str>,
) {
    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "UPDATE agent_runs
             SET status = ?1, pid = 4242, launch_gate_state = ?2
             WHERE run_id = ?3",
            rusqlite::params![agent_status, launch_gate_state, run_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE events SET status = ?1, lease_until = NULL WHERE event_id = ?2",
            rusqlite::params![event_status, event_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE code_change_editor_attempts
             SET status = ?1, result_digest = CASE WHEN ?1 = 'ready' THEN ?4 ELSE NULL END,
                 failure_code = ?2,
                 failure_summary = CASE WHEN ?1 = 'failed' THEN COALESCE(?2, 'startup fixture failure') ELSE NULL END,
                 finished_at = CASE WHEN ?1 IN ('ready', 'failed') THEN 199 ELSE NULL END
             WHERE code_change_run_id = ?3 AND attempt = 1",
            rusqlite::params![
                attempt_status,
                failure_code,
                code_change_run_id,
                VALID_EDITOR_RESULT_DIGEST,
            ],
        )
        .unwrap();
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_reuses_terminal_validated_output_without_new_agent() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "ready",
        None,
    );
    let before_agent_runs = harness.count("agent_runs");
    let (policy, runner) = startup_editor_runner(&harness);

    let report = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now,
        &[run_id],
        &startup_editor_retry_policies(1),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .unwrap();

    assert_eq!(report.started.len(), 0);
    assert_eq!(harness.count("agent_runs"), before_agent_runs);
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
    assert_eq!(
        AgentRunRepository::new(&harness.db)
            .find_by_id(run_id)
            .unwrap()
            .unwrap()
            .status,
        AgentRunStatus::Completed
    );
    let attempt = CodeChangeRepository::new(&harness.db)
        .find_editor_attempt(&code_change_run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "ready");
    assert_eq!(
        attempt.result_digest.as_deref(),
        Some(VALID_EDITOR_RESULT_DIGEST)
    );
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .find_by_id(&code_change_run_id)
            .unwrap()
            .unwrap()
            .editor_attempts,
        1
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_recovery_runs_before_normal_dispatch() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "ready",
        None,
    );
    let before_agent_runs = harness.count("agent_runs");

    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.preserved_code_change_editors, 1);
    assert_eq!(report.code_changes.started, 0);
    assert_eq!(report.code_changes.advanced, 1);
    assert_eq!(harness.count("agent_runs"), before_agent_runs);
    assert!(harness.fake_pueue.add_calls().is_empty());
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
    assert_eq!(
        AgentRunRepository::new(&harness.db)
            .find_by_id(run_id)
            .unwrap()
            .unwrap()
            .status,
        AgentRunStatus::Completed
    );
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .find_editor_attempt(&code_change_run_id, 1)
            .unwrap()
            .unwrap()
            .status,
        "ready"
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_finishes_certain_pre_marker_failure_once() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    let (policy, runner) = startup_editor_runner(&harness);

    let first = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now,
        &[run_id],
        &startup_editor_retry_policies(1),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .unwrap();
    assert_eq!(first.started.len(), 0);
    let after_first = CodeChangeRepository::new(&harness.db)
        .find_editor_attempt(&code_change_run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(after_first.status, "failed");
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .find_by_id(&code_change_run_id)
            .unwrap()
            .unwrap()
            .editor_attempts,
        1
    );
    let first_finished_at = after_first.finished_at;
    let first_event_status = harness.event_status(event_id);

    let second = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now + 1,
        &[run_id],
        &startup_editor_retry_policies(1),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .unwrap();
    assert_eq!(second.started.len(), 0);
    let after_second = CodeChangeRepository::new(&harness.db)
        .find_editor_attempt(&code_change_run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(after_second.status, "failed");
    assert_eq!(after_second.finished_at, first_finished_at);
    assert_eq!(harness.event_status(event_id), first_event_status);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_quarantines_post_release_uncertainty_without_output() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "running",
        None,
    );
    let (policy, runner) = startup_editor_runner(&harness);

    let report = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now,
        &[run_id],
        &startup_editor_retry_policies(1),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .unwrap();

    assert_eq!(report.started.len(), 0);
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .find_by_id(&code_change_run_id)
            .unwrap()
            .unwrap()
            .state,
        pueue_agent::models::CodeChangeState::RecoveryRequired
    );
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
    assert_eq!(
        AgentRunRepository::new(&harness.db)
            .find_by_id(run_id)
            .unwrap()
            .unwrap()
            .status,
        AgentRunStatus::Failed
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_quarantines_indeterminate_pending_without_output() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "pending",
        "in_flight",
        "running",
        None,
    );
    let (policy, runner) = startup_editor_runner(&harness);
    let indeterminate = BTreeSet::from([run_id]);

    let report = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now,
        &[run_id],
        &startup_editor_retry_policies(1),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &indeterminate,
        &BTreeSet::new(),
    )
    .await
    .unwrap();

    assert_eq!(report.started.len(), 0);
    assert_eq!(
        CodeChangeRepository::new(&harness.db)
            .find_by_id(&code_change_run_id)
            .unwrap()
            .unwrap()
            .state,
        pueue_agent::models::CodeChangeState::RecoveryRequired
    );
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_recovers_uncertain_crash_prefixes() {
    const UNCERTAIN_RECOVERY_CODE: &str = "editor_startup_uncertain";
    const UNCERTAIN_RECOVERY_SUMMARY: &str =
        "editor startup execution outcome is unknown after restart";

    {
        let harness = DaemonHarness::new();
        let (run_id, code_change_run_id, event_id) =
            seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
        update_startup_editor_fixture(
            &harness,
            run_id,
            &code_change_run_id,
            event_id,
            "running",
            "released",
            "dispatched",
            "failed",
            Some(UNCERTAIN_RECOVERY_CODE),
        );
        let before_agent_runs = harness.count("agent_runs");
        let (policy, runner) = startup_editor_runner(&harness);

        let report = CodeChangeCoordinator::new(
            &harness.db,
            &runner,
            &policy,
            CampaignLimits::default(),
        )
        .recover_startup_editors(
            harness.now,
            &[run_id],
            &startup_editor_retry_policies(1),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
        .unwrap();

        assert_eq!(report.started.len(), 0);
        assert_eq!(harness.count("agent_runs"), before_agent_runs);
        assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
        assert_eq!(
            CodeChangeRepository::new(&harness.db)
                .find_by_id(&code_change_run_id)
                .unwrap()
                .unwrap()
                .state,
            pueue_agent::models::CodeChangeState::RecoveryRequired
        );
        assert_eq!(
            AgentRunRepository::new(&harness.db)
                .find_by_id(run_id)
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Failed
        );
        assert_eq!(
            CodeChangeRepository::new(&harness.db)
                .find_editor_attempt(&code_change_run_id, 1)
                .unwrap()
                .unwrap()
                .failure_code
                .as_deref(),
            Some(UNCERTAIN_RECOVERY_CODE)
        );
    }

    {
        let harness = DaemonHarness::new();
        let (run_id, code_change_run_id, event_id) =
            seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
        update_startup_editor_fixture(
            &harness,
            run_id,
            &code_change_run_id,
            event_id,
            "running",
            "released",
            "dispatched",
            "failed",
            Some(UNCERTAIN_RECOVERY_CODE),
        );
        harness
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE code_change_runs
                 SET state = 'recovery_required', rejection_code = ?1,
                     rejection_summary = ?2
                 WHERE code_change_run_id = ?3",
                rusqlite::params![
                    UNCERTAIN_RECOVERY_CODE,
                    UNCERTAIN_RECOVERY_SUMMARY,
                    code_change_run_id,
                ],
            )
            .unwrap();
        let policies = BTreeMap::from([(
            "project-a".to_owned(),
            RetryPolicy { max_retries: 1 },
        )]);
        let recovery = AgentRunRepository::new(&harness.db)
            .recover_interrupted_with_marker_evidence(
                harness.now,
                "daemon restart",
                &policies,
                &BTreeSet::new(),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &BTreeSet::new(),
            )
            .unwrap();
        assert_eq!(recovery.preserved_code_change_editors, 1);
        assert_eq!(recovery.preserved_code_change_editor_run_ids, vec![run_id]);
        assert_eq!(harness.event_status(event_id), EventStatus::Dispatched);
        let before_agent_runs = harness.count("agent_runs");
        let (policy, runner) = startup_editor_runner(&harness);

        let report = CodeChangeCoordinator::new(
            &harness.db,
            &runner,
            &policy,
            CampaignLimits::default(),
        )
        .recover_startup_editors(
            harness.now,
            &recovery.preserved_code_change_editor_run_ids,
            &startup_editor_retry_policies(1),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
        .unwrap();

        assert_eq!(report.started.len(), 0);
        assert_eq!(harness.count("agent_runs"), before_agent_runs);
        assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
        assert_eq!(
            CodeChangeRepository::new(&harness.db)
                .find_by_id(&code_change_run_id)
                .unwrap()
                .unwrap()
                .state,
            pueue_agent::models::CodeChangeState::RecoveryRequired
        );
        assert_eq!(
            AgentRunRepository::new(&harness.db)
                .find_by_id(run_id)
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Failed
        );
        let attempt = CodeChangeRepository::new(&harness.db)
            .find_editor_attempt(&code_change_run_id, 1)
            .unwrap()
            .unwrap();
        assert_eq!(attempt.status, "failed");
        assert_eq!(
            attempt.failure_code.as_deref(),
            Some(UNCERTAIN_RECOVERY_CODE)
        );
    }

    {
        let harness = DaemonHarness::new();
        let (run_id, code_change_run_id, event_id) =
            seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
        update_startup_editor_fixture(
            &harness,
            run_id,
            &code_change_run_id,
            event_id,
            "running",
            "released",
            "dispatched",
            "failed",
            Some("editor_exit"),
        );
        harness
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE events SET attempts = 1 WHERE event_id = ?1",
                [event_id],
            )
            .unwrap();
        let attempts: i64 = harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT attempts FROM events WHERE event_id = ?1",
                [event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempts, 1);
        let before_agent_runs = harness.count("agent_runs");
        let (policy, runner) = startup_editor_runner(&harness);

        let report = CodeChangeCoordinator::new(
            &harness.db,
            &runner,
            &policy,
            CampaignLimits::default(),
        )
        .recover_startup_editors(
            harness.now,
            &[run_id],
            &startup_editor_retry_policies(1),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .await
        .unwrap();

        assert_eq!(report.started.len(), 0);
        assert_eq!(harness.count("agent_runs"), before_agent_runs);
        assert_eq!(harness.event_status(event_id), EventStatus::RetryWait);
        assert_eq!(
            CodeChangeRepository::new(&harness.db)
                .find_by_id(&code_change_run_id)
                .unwrap()
                .unwrap()
                .state,
            pueue_agent::models::CodeChangeState::Editing
        );
        assert_eq!(
            AgentRunRepository::new(&harness.db)
                .find_by_id(run_id)
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Failed
        );
        assert_eq!(
            CodeChangeRepository::new(&harness.db)
                .find_editor_attempt(&code_change_run_id, 1)
                .unwrap()
                .unwrap()
                .failure_code
                .as_deref(),
            Some("editor_exit")
        );
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_startup_dead_letters_after_configured_retry_budget() {
    let harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "failed",
        Some("editor_exit"),
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET attempts = 1 WHERE event_id = ?1",
            [event_id],
        )
        .unwrap();
    let attempts: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT attempts FROM events WHERE event_id = ?1",
            [event_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(attempts, 1);
    let (policy, runner) = startup_editor_runner(&harness);

    let report = CodeChangeCoordinator::new(
        &harness.db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .recover_startup_editors(
        harness.now,
        &[run_id],
        &startup_editor_retry_policies(0),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .unwrap();

    assert_eq!(report.started.len(), 0);
    assert_eq!(harness.event_status(event_id), EventStatus::DeadLetter);
}

#[cfg(all(unix, target_os = "linux"))]
#[test]
fn code_change_editor_recovery_binding_rejects_incoherent_terminal_payloads() {
    let ready_harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&ready_harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &ready_harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "ready",
        None,
    );
    ready_harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_editor_attempts
             SET result_digest = 'not-a-sha256-digest'
             WHERE code_change_run_id = ?1 AND attempt = 1",
            [&code_change_run_id],
        )
        .unwrap();
    let ready_error = AgentRunRepository::new(&ready_harness.db)
        .recover_interrupted_with_marker_evidence(
            ready_harness.now,
            "daemon restart",
            &BTreeMap::from([("project-a".to_owned(), RetryPolicy { max_retries: 1 })]),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .expect_err("ready attempt without a result digest must fail closed");
    assert!(matches!(
        ready_error,
        AppError::Validation {
            field: "code_change_editor_attempt",
            ..
        }
    ));

    let failed_harness = DaemonHarness::new();
    let (run_id, code_change_run_id, event_id) =
        seed_code_change_editor_recovery_fixture(&failed_harness, "code_change_editor", true);
    update_startup_editor_fixture(
        &failed_harness,
        run_id,
        &code_change_run_id,
        event_id,
        "running",
        "released",
        "dispatched",
        "failed",
        Some("editor_exit"),
    );
    failed_harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_editor_attempts
             SET failure_summary = NULL
             WHERE code_change_run_id = ?1 AND attempt = 1",
            [&code_change_run_id],
        )
        .unwrap();
    let failed_error = AgentRunRepository::new(&failed_harness.db)
        .recover_interrupted_with_marker_evidence(
            failed_harness.now,
            "daemon restart",
            &BTreeMap::from([("project-a".to_owned(), RetryPolicy { max_retries: 1 })]),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .expect_err("failed attempt without failure metadata must fail closed");
    assert!(matches!(
        failed_error,
        AppError::Validation {
            field: "code_change_editor_attempt",
            ..
        }
    ));
}

#[tokio::test]
async fn startup_recovery_reconciles_submission_boundary_before_normal_dispatch() {
    let harness = DaemonHarness::new();
    let experiment_id = harness.campaign_experiment();
    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "UPDATE experiments
             SET status = 'submitting', pueue_task_id = NULL, task_signature = NULL
             WHERE experiment_id = ?1",
            [&experiment_id],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE submissions
             SET status = 'pending', pueue_task_id = NULL, task_signature = NULL
             WHERE submission_id = (SELECT submission_id FROM experiments WHERE experiment_id = ?1)",
            [&experiment_id],
        )
        .unwrap();
    let report = harness.daemon().run_once().await.unwrap();

    assert_eq!(report.scheduler.started.len(), 0);
    assert!(harness.fake_pueue.add_calls().is_empty());
    let statuses: (String, String) = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT experiments.status, submissions.status
             FROM experiments JOIN submissions USING (submission_id)
             WHERE experiments.experiment_id = ?1",
            [&experiment_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(statuses, ("unreconciled".to_owned(), "unreconciled".to_owned()));
}

#[tokio::test]
async fn code_change_editor_recovery_rejects_unbound_execution_identity() {
    let harness = DaemonHarness::new();
    seed_code_change_editor_recovery_fixture(&harness, "code_change_editor", false);

    assert!(harness.daemon().run_once().await.is_err());
}

#[tokio::test]
async fn code_change_editor_recovery_rejects_non_editor_execution_identity() {
    let harness = DaemonHarness::new();
    seed_code_change_editor_recovery_fixture(&harness, "codex", true);

    assert!(harness.daemon().run_once().await.is_err());
}

#[cfg(all(unix, target_os = "linux"))]
fn compile_code_change_editor_fixture(
    target: &std::path::Path,
    invocation_log: &std::path::Path,
    behavior_path: &std::path::Path,
) {
    let source = target.with_extension("rs");
    let state_literal = format!("{:?}", invocation_log.to_string_lossy());
    let behavior_literal = format!("{:?}", behavior_path.to_string_lossy());
    let source_body = r##"
use std::{env, fs, path::Path, process, thread, time::Duration};

fn main() {
    let state_path = Path::new(__STATE_PATH__);
    let behavior_path = Path::new(__BEHAVIOR_PATH__);
    let invocation = fs::read_to_string(state_path)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    fs::write(state_path, invocation.to_string()).unwrap();
    let capture_path = state_path.with_extension("log");
    let mode = env::var("PUEUE_AGENT_EDITOR_MODE").unwrap_or_default();
    let session = env::var("PUEUE_AGENT_EDITOR_SESSION_ID").unwrap_or_default();
    let mut capture = fs::read_to_string(&capture_path).unwrap_or_default();
    capture.push_str(&format!("mode={mode};session={session}\n"));
    fs::write(capture_path, capture).unwrap();
    let behavior = fs::read_to_string(behavior_path).unwrap_or_default();
    match behavior.trim() {
        "timeout" => {
            thread::sleep(Duration::from_secs(30));
            return;
        }
        "malformed" => {
            let output = env::var("PUEUE_AGENT_EDITOR_OUTPUT").unwrap();
            fs::write(output, br#"{malformed-editor"#).unwrap();
            return;
        }
        "oversized" => {
            let output = env::var("PUEUE_AGENT_EDITOR_OUTPUT").unwrap();
            fs::write(output, vec![b'x'; 65 * 1024 + 1]).unwrap();
            return;
        }
        "ready" => {
            let output = env::var("PUEUE_AGENT_EDITOR_OUTPUT").unwrap();
            fs::write(
                output,
                br#"{"schema_version":1,"status":"ready","summary":"editor prepared candidate","proposed_checks":[{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"."}]}"#,
            )
            .unwrap();
            return;
        }
        "ready-two" => {
            let output = env::var("PUEUE_AGENT_EDITOR_OUTPUT").unwrap();
            fs::write(
                output,
                br#"{"schema_version":1,"status":"ready","summary":"editor prepared candidate","proposed_checks":[{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"."},{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"nested"}]}"#,
            )
            .unwrap();
            return;
        }
        "fail" => process::exit(17),
        _ => {}
    }
    if invocation == 1 {
        process::exit(17);
    }
    let output = env::var("PUEUE_AGENT_EDITOR_OUTPUT").unwrap();
    fs::write(
        output,
        br#"{"schema_version":1,"status":"cannot_apply","summary":"editor cannot apply","proposed_checks":[]}"#,
    )
    .unwrap();
}
"##
    .replace("__STATE_PATH__", &state_literal)
    .replace("__BEHAVIOR_PATH__", &behavior_literal);
    fs::write(&source, source_body).unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated editor fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(all(unix, target_os = "linux"))]
fn compile_code_change_check_fixture(
    target: &std::path::Path,
    invocation_state: &std::path::Path,
    behavior_path: &std::path::Path,
) {
    let source = target.with_extension("rs");
    let state_literal = format!("{:?}", invocation_state.to_string_lossy());
    let behavior_literal = format!("{:?}", behavior_path.to_string_lossy());
    let source_body = r##"
use std::{env, fs, path::Path, process, thread, time::Duration};

fn main() {
    let state_path = Path::new(__STATE_PATH__);
    let behavior_path = Path::new(__BEHAVIOR_PATH__);
    let invocation = fs::read_to_string(state_path)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .unwrap_or(0)
        + 1;
    fs::write(state_path, invocation.to_string()).unwrap();
    match fs::read_to_string(behavior_path).unwrap().trim() {
        "fail-first" if invocation == 1 => process::exit(17),
        "timeout" => thread::sleep(Duration::from_secs(30)),
        "overflow" => {
            let output = vec![b'x'; 33 * 1024];
            print!("{}", String::from_utf8(output.clone()).unwrap());
            eprintln!("{}", String::from_utf8(output).unwrap());
        }
        "mutate" => fs::write("base.txt", b"check-mutated\n").unwrap(),
        _ => {}
    }
}
"##
    .replace("__STATE_PATH__", &state_literal)
    .replace("__BEHAVIOR_PATH__", &behavior_literal);
    fs::write(&source, source_body).unwrap();
    let output = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "generated check fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}

#[cfg(all(unix, target_os = "linux"))]
struct CodeChangeEditorFixture {
    _temp: TempDir,
    db: Db,
    project: Project,
    candidate_policy: ResolvedProjectExecutionPolicy,
    config: AgentConfig,
    runner: AgentRunner,
    policy: Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy>,
    campaign_id: String,
    run_id: String,
}

#[cfg(all(unix, target_os = "linux"))]
fn code_change_editor_fixture(behavior: &str) -> CodeChangeEditorFixture {
    code_change_editor_fixture_with_check_behavior(behavior, None)
}

#[cfg(all(unix, target_os = "linux"))]
fn code_change_editor_fixture_with_check_behavior(
    behavior: &str,
    check_behavior: Option<&str>,
) -> CodeChangeEditorFixture {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let fixture_root = fs::canonicalize(temp.path()).unwrap();
    let project_root = fixture_root.join("project");
    let service_dir = project_root.join(".pueue-agent");
    let trusted_bin = fixture_root.join("trusted-bin");
    for directory in [&project_root, &service_dir, &trusted_bin] {
        fs::create_dir_all(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let logs_dir = service_dir.join("logs");
    fs::create_dir(&logs_dir).unwrap();
    fs::set_permissions(&logs_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let editor = trusted_bin.join("editor");
    let invocation_state = fixture_root.join("editor-invocations.state");
    let behavior_path = fixture_root.join("editor-behavior");
    fs::write(&behavior_path, behavior).unwrap();
    compile_code_change_editor_fixture(&editor, &invocation_state, &behavior_path);
    let config_path = service_dir.join("config.toml");
    fs::write(
        &config_path,
        format!(
            r#"project_id = "editor-project"
pueue_group = "editor-project"

[agent]
program = {:?}
args = ["{{prompt}}"]
timeout_minutes = 1
max_retries = 0

[agent.execution]
network = "enabled"

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
            editor.display().to_string()
        ),
    )
    .unwrap();
    let trusted_git = fixture_root.join("execution-policy-bin/git");
    fs::create_dir_all(trusted_git.parent().unwrap()).unwrap();
    fs::copy("/usr/bin/git", &trusted_git).unwrap();
    fs::set_permissions(&trusted_git, fs::Permissions::from_mode(0o700)).unwrap();
    let mut real_toolchain_bin = None;
    if let Some(check_behavior) = check_behavior {
        if check_behavior == "real-profiles" {
            let locate = |names: &[&str]| {
                names
                    .iter()
                    .find_map(|name| {
                        let output = Command::new("/bin/sh")
                            .args(["-c", &format!("command -v {name}")])
                            .output()
                            .expect("locate real code-change profile tool");
                        output.status.success().then(|| {
                            fs::canonicalize(
                                String::from_utf8(output.stdout)
                                    .expect("tool path must be UTF-8")
                                    .trim(),
                            )
                            .expect("real code-change profile tool must resolve")
                        })
                    })
                    .unwrap_or_else(|| panic!("required code-change profile tool is missing: {names:?}"))
            };
            let cargo_source = locate(&["cargo"]);
            real_toolchain_bin = Some(
                cargo_source
                    .parent()
                    .expect("real Cargo must have a toolchain bin directory")
                    .to_owned(),
            );
            let python_source = locate(&["python", "python3"]);
            for (target, source) in [
                ("cargo", cargo_source.clone()),
                ("uv", locate(&["uv"])),
                ("python", python_source.clone()),
            ] {
                let target = fixture_root.join("execution-policy-bin").join(target);
                fs::copy(source, &target).expect("copy real profile tool anchor");
                fs::set_permissions(&target, fs::Permissions::from_mode(0o700))
                    .expect("secure real profile tool anchor");
            }
            let python3 = fixture_root.join("execution-policy-bin/python3");
            fs::copy(python_source, &python3).expect("copy real Python 3 tool");
            fs::set_permissions(&python3, fs::Permissions::from_mode(0o700))
                .expect("secure real Python 3 tool");
            let cc_source = locate(&["cc"]);
            let cc_source = cc_source
                .to_str()
                .expect("real native compiler path must be UTF-8")
                .replace('"', "\\\"");
            let cc = fixture_root.join("execution-policy-bin/cc");
            fs::write(&cc, format!("#!/bin/sh\nexec \"{cc_source}\" \"$@\"\n"))
                .expect("write native compiler forwarder");
            fs::set_permissions(&cc, fs::Permissions::from_mode(0o700))
                .expect("secure native compiler forwarder");
        } else {
            let check = fixture_root.join("execution-policy-bin/cargo");
            let check_invocation_state = fixture_root.join("check-invocations.state");
            let check_behavior_path = fixture_root.join("check-behavior");
            fs::write(&check_behavior_path, check_behavior).unwrap();
            compile_code_change_check_fixture(
                &check,
                &check_invocation_state,
                &check_behavior_path,
            );
        }
    }
    let extra_trusted_paths = real_toolchain_bin.as_deref().into_iter().collect::<Vec<_>>();
    let policy = execution_policy_fixture::resolved_policy_with_trusted_paths(
        &fixture_root,
        &[("editor-project", &project_root, &editor)],
        &extra_trusted_paths,
    );
    let db = Db::open(&fixture_root.join("state.sqlite3")).unwrap();
    let project = ProjectRepository::new(&db)
        .register(&NewProject::new(
            "editor-project",
            fs::canonicalize(&project_root).unwrap(),
            "editor-project",
            &config_path,
            1,
        ))
        .unwrap();
    let project_config = config::load(&config_path).unwrap();
    let config = project_config.agent.clone();
    let original = resolve_project_policy(&policy, &project, &project_config).unwrap();
    let campaign_id = "editor-campaign".to_owned();
    let proposal_id = "editor-proposal";
    let run_id = "editor-run".to_owned();
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO campaigns (
                 campaign_id, project_id, objective_text, objective_digest,
                 initial_argv_json, state, created_at, updated_at
             ) VALUES (?1, ?2, 'editor objective', 'editor-digest', '[]', 'active', 1, 1)",
            rusqlite::params![campaign_id, project.project_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis, argv_json,
                 working_directory, expected_evidence_json, canonical_digest,
                 created_at, updated_at
             ) VALUES (?1, ?2, 'code_change', 'accepted', 'editor hypothesis',
                       '[]', '.', '[]', 'editor-proposal-digest', 1, 1)",
            rusqlite::params![proposal_id, campaign_id],
        )
        .unwrap();
    drop(connection);
    CodeChangeRepository::new(&db)
        .create_pending(&NewCodeChangeRun::new(
            &run_id,
            proposal_id,
            &campaign_id,
            "0000000000000000000000000000000000000000",
            pueue_agent::code_change::candidate_ref(&campaign_id, proposal_id).unwrap(),
            pueue_agent::code_change::best_ref(&campaign_id).unwrap(),
            &run_id,
            ".pueue-agent/worktrees/editor-campaign/editor-proposal",
            1,
        ))
        .unwrap();
    CodeChangeRepository::new(&db)
        .transition(
            &run_id,
            pueue_agent::models::CodeChangeState::Reserved,
            pueue_agent::models::CodeChangeState::PreparingWorktree,
            2,
        )
        .unwrap();
    CodeChangeRepository::new(&db)
        .transition(
            &run_id,
            pueue_agent::models::CodeChangeState::PreparingWorktree,
            pueue_agent::models::CodeChangeState::Editing,
            3,
        )
        .unwrap();
    let candidate_root = fixture_root
        .join("execution-policy-state/worktrees")
        .join(&campaign_id)
        .join(proposal_id);
    fs::create_dir_all(&candidate_root).unwrap();
    for directory in [
        fixture_root.join("execution-policy-state/worktrees"),
        fixture_root.join("execution-policy-state/worktrees").join(&campaign_id),
        candidate_root.clone(),
    ] {
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut candidate_policy = original;
    candidate_policy.root_anchor = ProjectRootAnchor::resolve(&candidate_root).unwrap();
    let runner = AgentRunner::new(
        AgentRunnerConfig::production()
            .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
        Arc::clone(&policy),
    );
    CodeChangeEditorFixture {
        _temp: temp,
        db,
        project,
        candidate_policy,
        config,
        runner,
        policy,
        campaign_id,
        run_id,
    }
}

#[cfg(all(unix, target_os = "linux"))]
async fn spawn_editor_fixture_attempt(
    runner: &AgentRunner,
    db: &Db,
    project: &Project,
    candidate_policy: &ResolvedProjectExecutionPolicy,
    config: &AgentConfig,
    code_change_run_id: &str,
    campaign_id: &str,
    attempt: i64,
    context: AgentContextMode,
    now: i64,
) -> AgentHandle {
    spawn_editor_fixture_attempt_result(
        runner,
        db,
        project,
        candidate_policy,
        config,
        code_change_run_id,
        campaign_id,
        attempt,
        context,
        now,
    )
    .await
    .expect("editor fixture launch")
}

#[cfg(all(unix, target_os = "linux"))]
async fn spawn_editor_fixture_attempt_result(
    runner: &AgentRunner,
    db: &Db,
    project: &Project,
    candidate_policy: &ResolvedProjectExecutionPolicy,
    config: &AgentConfig,
    code_change_run_id: &str,
    campaign_id: &str,
    attempt: i64,
    context: AgentContextMode,
    now: i64,
) -> Result<AgentHandle, pueue_agent::agent::AgentSpawnError> {
    let event = EventRepository::new(db)
        .insert_idempotent(
            &NewEvent::new(
                project.project_id.clone(),
                EventKind::CodeChange,
                format!("editor-fixture:{code_change_run_id}:{attempt}"),
                json!({
                    "code_change_run_id": code_change_run_id,
                    "attempt": attempt,
                }),
                now,
                now,
            )
            .with_campaign_lineage(campaign_id, Option::<String>::None),
        )
        .unwrap();
    let event = EventRepository::new(db)
        .claim_by_id(&project.project_id, event.event_id, now + 600)
        .unwrap()
        .expect("editor fixture event should be claimable");
    let run_id_guard = runner
        .try_acquire_run_id_admission_guard(db)
        .unwrap()
        .expect("editor fixture run ID guard");
    let project_lock = runner
        .try_acquire_project_admission_lock(candidate_policy)
        .unwrap()
        .expect("editor fixture candidate lock");
    let mut config = config.clone();
    config.context = context;
    runner
        .spawn_code_change_editor(
            db,
            project,
            candidate_policy,
            &config,
            RetryPolicy { max_retries: 0 },
            event.event_id,
            &[event.event_id],
            code_change_run_id,
            attempt,
            "bounded editor fixture prompt",
            now,
            run_id_guard,
            project_lock,
        )
        .await
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_public_spawn_binds_fresh_resume_and_rejects_third() {
    let temp = TempDir::new().unwrap();
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let fixture_root = fs::canonicalize(temp.path()).unwrap();
    let project_root = fixture_root.join("project");
    let service_dir = project_root.join(".pueue-agent");
    let trusted_bin = fixture_root.join("trusted-bin");
    for directory in [&project_root, &service_dir, &trusted_bin] {
        fs::create_dir_all(directory).unwrap();
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fs::create_dir(service_dir.join("logs")).unwrap();
    fs::set_permissions(service_dir.join("logs"), fs::Permissions::from_mode(0o700)).unwrap();
    let editor = trusted_bin.join("editor");
    let invocation_state = fixture_root.join("editor-invocations.state");
    let behavior_path = fixture_root.join("editor-behavior");
    fs::write(&behavior_path, b"").unwrap();
    compile_code_change_editor_fixture(&editor, &invocation_state, &behavior_path);
    let config_path = service_dir.join("config.toml");
    fs::write(
        &config_path,
        format!(
            r#"project_id = "editor-project"
pueue_group = "editor-project"

[agent]
program = {:?}
args = ["{{prompt}}"]
timeout_minutes = 1
max_retries = 0

[agent.execution]
network = "enabled"

[check]
interval_minutes = 10
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 1024
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 3
max_experiments = 20
max_agent_runs = 10
"#,
            editor.display().to_string()
        ),
    )
    .unwrap();

    let policy = execution_policy_fixture::resolved_policy(
        &fixture_root,
        &[("editor-project", &project_root, &editor)],
    );
    let db = Db::open(&fixture_root.join("state.sqlite3")).unwrap();
    let project = ProjectRepository::new(&db)
        .register(&NewProject::new(
            "editor-project",
            fs::canonicalize(&project_root).unwrap(),
            "editor-project",
            &config_path,
            1,
        ))
        .unwrap();
    let project_config = config::load(&config_path).unwrap();
    let config = project_config.agent.clone();
    let original = resolve_project_policy(&policy, &project, &project_config).unwrap();

    let campaign_id = "editor-campaign";
    let proposal_id = "editor-proposal";
    let run_id = "editor-run";
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO campaigns (
                 campaign_id, project_id, objective_text, objective_digest,
                 initial_argv_json, state, created_at, updated_at
             ) VALUES (?1, ?2, 'editor objective', 'editor-digest', '[]', 'active', 1, 1)",
            rusqlite::params![campaign_id, project.project_id],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis, argv_json,
                 working_directory, expected_evidence_json, canonical_digest,
                 created_at, updated_at
             ) VALUES (?1, ?2, 'code_change', 'accepted', 'editor hypothesis',
                       '[]', '.', '[]', 'editor-proposal-digest', 1, 1)",
            rusqlite::params![proposal_id, campaign_id],
        )
        .unwrap();
    drop(connection);
    CodeChangeRepository::new(&db)
        .create_pending(&NewCodeChangeRun::new(
            run_id,
            proposal_id,
            campaign_id,
            "0000000000000000000000000000000000000000",
            pueue_agent::code_change::candidate_ref(campaign_id, proposal_id).unwrap(),
            pueue_agent::code_change::best_ref(campaign_id).unwrap(),
            run_id,
            ".pueue-agent/worktrees/editor-campaign/editor-proposal",
            1,
        ))
        .unwrap();
    CodeChangeRepository::new(&db)
        .transition(run_id, pueue_agent::models::CodeChangeState::Reserved,
            pueue_agent::models::CodeChangeState::PreparingWorktree, 2)
        .unwrap();
    CodeChangeRepository::new(&db)
        .transition(run_id, pueue_agent::models::CodeChangeState::PreparingWorktree,
            pueue_agent::models::CodeChangeState::Editing, 3)
        .unwrap();

    let candidate_root = fixture_root
        .join("execution-policy-state/worktrees")
        .join(campaign_id)
        .join(proposal_id);
    fs::create_dir_all(&candidate_root).unwrap();
    for directory in [
        fixture_root.join("execution-policy-state/worktrees"),
        fixture_root.join("execution-policy-state/worktrees").join(campaign_id),
        candidate_root.clone(),
    ] {
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut candidate_policy = original.clone();
    candidate_policy.root_anchor = ProjectRootAnchor::resolve(&candidate_root).unwrap();
    let runner = AgentRunner::new(
        AgentRunnerConfig::production()
            .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
        Arc::clone(&policy),
    );
    db.connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET state = 'paused' WHERE campaign_id = ?1",
            [campaign_id],
        )
        .unwrap();
    let coordinator_report = CodeChangeCoordinator::new(
        &db,
        &runner,
        &policy,
        CampaignLimits::default(),
    )
    .advance_ready(4, 10)
    .await
    .unwrap();
    assert_eq!(coordinator_report.deferred, 1);
    db.connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET state = 'active' WHERE campaign_id = ?1",
            [campaign_id],
        )
        .unwrap();

    let mut first = spawn_editor_fixture_attempt(
        &runner,
        &db,
        &project,
        &candidate_policy,
        &config,
        run_id,
        campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await;
    assert_eq!(first.wait(&db, 11).await.unwrap(), AgentRunStatus::Failed);
    let first_run = AgentRunRepository::new(&db)
        .find_by_id(first.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(first_run.context_mode, AgentContextMode::Fresh);
    let run = CodeChangeRepository::new(&db).find_by_id(run_id).unwrap().unwrap();
    let session = run.editor_session_id.clone().expect("fresh session binding");
    let first_attempt = CodeChangeRepository::new(&db)
        .find_editor_attempt(run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(first_attempt.status, "failed");
    assert_eq!(first_attempt.editor_session_id, session);

    let mut second = spawn_editor_fixture_attempt(
        &runner,
        &db,
        &project,
        &candidate_policy,
        &config,
        run_id,
        campaign_id,
        2,
        AgentContextMode::Resume {
            session_id: session.clone(),
        },
        12,
    )
    .await;
    let second_run_id = second.run_id;
    let third_event = EventRepository::new(&db)
        .insert_idempotent(&NewEvent::new(
            project.project_id.clone(),
            EventKind::CodeChange,
            "editor-fixture:third",
            json!({}),
            13,
            13,
        ))
        .unwrap();
    EventRepository::new(&db)
        .claim_by_id(&project.project_id, third_event.event_id, 613)
        .unwrap()
        .expect("third-launch event should be claimable");
    let third_guard = runner
        .try_acquire_run_id_admission_guard(&db)
        .unwrap()
        .expect("third-launch run ID guard");
    let third_lock = runner
        .try_acquire_project_admission_lock(&candidate_policy)
        .unwrap()
        .expect("third-launch candidate lock");
    let third = runner
        .spawn_code_change_editor(
            &db,
            &project,
            &candidate_policy,
            &config,
            RetryPolicy { max_retries: 0 },
            third_event.event_id,
            &[third_event.event_id],
            run_id,
            3,
            "third attempt must be rejected",
            13,
            third_guard,
            third_lock,
        )
        .await;
    assert!(third.is_err(), "the editor attempt bound is exactly two");
    assert_eq!(
        AgentRunRepository::new(&db).count_by_project(&project.project_id).unwrap(),
        2
    );
    assert_eq!(
        CodeChangeRepository::new(&db).list_editor_attempts(run_id).unwrap().len(),
        2
    );

    assert_eq!(second.wait(&db, 14).await.unwrap(), AgentRunStatus::Completed);
    let second_attempt = CodeChangeRepository::new(&db)
        .find_editor_attempt(run_id, 2)
        .unwrap()
        .unwrap();
    assert_eq!(second_attempt.status, "failed");
    assert_eq!(second_attempt.failure_code.as_deref(), Some("cannot_apply"));
    assert_eq!(second_attempt.editor_session_id, session);
    let final_run = CodeChangeRepository::new(&db).find_by_id(run_id).unwrap().unwrap();
    assert_eq!(final_run.state, pueue_agent::models::CodeChangeState::Rejected);
    assert!(final_run.cleanup_completed_at.is_none());
    assert!(CodeChangeRepository::new(&db)
        .list_recoverable(100)
        .unwrap()
        .iter()
        .any(|run| {
            run.code_change_run_id == run_id
                && run.state == pueue_agent::models::CodeChangeState::Rejected
                && run.cleanup_completed_at.is_none()
        }));
    assert!(EventRepository::new(&db)
        .recent_events(&project.project_id, 100)
        .unwrap()
        .iter()
        .any(|event| {
            event.dedup_key == format!("code-change:v1:{run_id}:rejected:2")
                && event.status == EventStatus::Completed
        }));
    assert_eq!(
        AgentRunRepository::new(&db)
            .find_by_id(second_run_id)
            .unwrap()
            .unwrap()
            .context_mode,
        AgentContextMode::Resume {
            session_id: session.clone(),
        }
    );
    let capture = fs::read_to_string(invocation_state.with_extension("log")).unwrap();
    assert!(capture.contains("mode=fresh;session="));
    assert!(capture.contains(&format!("mode=resume;session={session}")));
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_post_binding_setup_failure_finishes_reserved_attempt() {
    let fixture = code_change_editor_fixture("fail");
    let blocked_tmp = fixture
        .project
        .root_path
        .join(".pueue-agent")
        .join("tmp");
    fs::write(&blocked_tmp, b"not a directory").unwrap();

    let error = match spawn_editor_fixture_attempt_result(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await
    {
        Ok(_) => panic!("post-binding setup failure should reject the launch"),
        Err(error) => error,
    };
    assert!(matches!(
        error.stage,
        pueue_agent::agent::AgentSpawnStage::RunBoundPreMarker {
            resolved: true,
            ..
        }
    ));
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .expect("editor attempt reservation");
    assert_eq!(attempt.status, "failed");
    assert_eq!(attempt.failure_code.as_deref(), Some("editor_launch"));
    assert!(attempt.finished_at.is_some());
    let first_failure_code = attempt.failure_code.clone();
    let first_failure_summary = attempt.failure_summary.clone();
    let first_finished_at = attempt.finished_at;
    assert!(!CodeChangeRepository::new(&fixture.db)
        .fail_editor_attempt_for_agent_run(
            attempt.agent_run_id,
            "replayed_editor_launch",
            "replayed failure must not overwrite the first terminal result",
            99,
        )
        .unwrap());
    let replayed_attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .expect("replayed editor attempt");
    assert_eq!(replayed_attempt.failure_code, first_failure_code);
    assert_eq!(replayed_attempt.failure_summary, first_failure_summary);
    assert_eq!(replayed_attempt.finished_at, first_finished_at);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_post_binding_finalization_failure_retains_cleanup_owner() {
    let fixture = code_change_editor_fixture("fail");
    let blocked_tmp = fixture
        .project
        .root_path
        .join(".pueue-agent")
        .join("tmp");
    fs::write(&blocked_tmp, b"not a directory").unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_editor_agent_finalization
             BEFORE UPDATE OF status ON agent_runs
             WHEN OLD.execution_kind = 'code_change_editor' AND NEW.status = 'failed'
             BEGIN SELECT RAISE(ABORT, 'injected editor agent-run finalizer failure'); END;",
        )
        .unwrap();

    let error = match spawn_editor_fixture_attempt_result(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await
    {
        Ok(_) => panic!("finalization failure should retain cleanup authority"),
        Err(error) => error,
    };
    assert!(matches!(
        error.stage,
        pueue_agent::agent::AgentSpawnStage::RunBoundPreMarker {
            resolved: false,
            ..
        }
    ));
    let mut cleanup = error
        .cleanup
        .expect("editor finalization failure must retain cleanup authority");
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .expect("editor attempt reservation");
    assert_eq!(attempt.status, "failed");
    assert_eq!(attempt.failure_code.as_deref(), Some("editor_launch"));
    let first_failure_code = attempt.failure_code.clone();
    let first_failure_summary = attempt.failure_summary.clone();
    let first_finished_at = attempt.finished_at;
    let agent_run = AgentRunRepository::new(&fixture.db)
        .find_by_id(attempt.agent_run_id)
        .unwrap()
        .expect("bound editor agent run");
    assert!(matches!(
        agent_run.status,
        AgentRunStatus::Starting | AgentRunStatus::Running
    ));

    fixture
        .db
        .connect()
        .unwrap()
        .execute_batch("DROP TRIGGER fail_editor_agent_finalization")
        .unwrap();
    cleanup.retry(&fixture.db, 12).await.unwrap();

    let finalized_run = AgentRunRepository::new(&fixture.db)
        .find_by_id(attempt.agent_run_id)
        .unwrap()
        .expect("finalized editor agent run");
    assert_eq!(finalized_run.status, AgentRunStatus::Failed);
    assert!(AgentRunRepository::new(&fixture.db)
        .find_active_by_project(&fixture.project.project_id)
        .unwrap()
        .is_none());
    let replayed_attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .expect("replayed editor attempt");
    assert_eq!(replayed_attempt.status, "failed");
    assert_eq!(replayed_attempt.failure_code, first_failure_code);
    assert_eq!(replayed_attempt.failure_summary, first_failure_summary);
    assert_eq!(replayed_attempt.finished_at, first_finished_at);
}

#[cfg(all(unix, target_os = "linux"))]
fn secure_editor_git_fixture_tree(path: &std::path::Path) {
    let metadata = fs::symlink_metadata(path).unwrap();
    let mode = if metadata.is_dir() { 0o700 } else { 0o600 };
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    if metadata.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            secure_editor_git_fixture_tree(&entry.unwrap().path());
        }
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_prebinding_failure_resolves_event_without_duplicate_budget() {
    let fixture = code_change_editor_fixture("fail");
    let project_root = fixture.project.root_path.clone();
    fs::write(project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
    fs::write(project_root.join("base.txt"), b"base\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", ".gitignore", "base.txt"]);
    run_git(&["commit", "-q", "-m", "base"]);
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![&base_sha, &fixture.campaign_id],
        )
        .unwrap();
    secure_editor_git_fixture_tree(&project_root.join(".git"));
    fs::set_permissions(
        project_root.join(".gitignore"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::set_permissions(project_root.join("base.txt"), fs::Permissions::from_mode(0o600))
        .unwrap();

    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::remove_dir_all(candidate_root).unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'reserved', base_sha = ?1,
                 candidate_ref = ?2, best_ref = ?3, updated_at = 4
             WHERE code_change_run_id = ?4",
            rusqlite::params![
                base_sha,
                "campaign/editor-campaign/candidate/editor-proposal",
                "campaign/editor-campaign/best",
                &fixture.run_id,
            ],
        )
        .unwrap();
    let guard_path = fixture
        .db
        .path()
        .parent()
        .expect("editor fixture state directory")
        .join("upgrade.lock.guard");
    let guard_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(guard_path)
        .unwrap();
    use std::os::fd::AsRawFd;
    unsafe extern "C" {
        fn flock(file_descriptor: std::os::raw::c_int, operation: std::os::raw::c_int)
            -> std::os::raw::c_int;
    }
    assert_eq!(unsafe { flock(guard_file.as_raw_fd(), 2) }, 0);

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(10, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 1);
    assert_eq!(
        CodeChangeRepository::new(&fixture.db)
            .find_by_id(&fixture.run_id)
            .unwrap()
            .unwrap()
            .state,
        pueue_agent::models::CodeChangeState::RecoveryRequired
    );
    let event = EventRepository::new(&fixture.db)
        .recent_events(&fixture.project.project_id, 100)
        .unwrap()
        .into_iter()
        .find(|event| event.dedup_key == "code-change-editor:v1:editor-run:1")
        .expect("editor event");
    assert_eq!(event.status, EventStatus::DeadLetter);
    assert_eq!(event.attempts, 1);
    let reservation_count: i64 = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = 'agent_run' AND subject_key = ?2",
            rusqlite::params![&fixture.campaign_id, "code-change-editor:v1:editor-run:1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_count, 1);
    let second = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(11, 10)
    .await
    .unwrap();
    assert_eq!(second.deferred, 0);
    let reservation_count_after_retry: i64 = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM budget_reservations
             WHERE campaign_id = ?1 AND dimension = 'agent_run' AND subject_key = ?2",
            rusqlite::params![&fixture.campaign_id, "code-change-editor:v1:editor-run:1"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reservation_count_after_retry, 1);
    assert!(CodeChangeRepository::new(&fixture.db)
        .list_editor_attempts(&fixture.run_id)
        .unwrap()
        .is_empty());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_ready_output_persists_checks_before_cleanup() {
    let fixture = code_change_editor_fixture("ready");
    let mut editor = spawn_editor_fixture_attempt(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await;
    assert_eq!(editor.wait(&fixture.db, 11).await.unwrap(), AgentRunStatus::Completed);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "ready");
    assert!(attempt.result_digest.is_some());
    assert_eq!(attempt.failure_code, None);
    let check: (String, String) = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT source, status FROM code_change_checks
             WHERE code_change_run_id = ?1 AND attempt = 1 AND ordinal = 0",
            [&fixture.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(check, ("editor".to_owned(), "reserved".to_owned()));
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(editor.run_id)
            .unwrap()
            .unwrap()
            .status,
        AgentRunStatus::Completed
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn git_diff_only_is_rejected() {
    let fixture = code_change_editor_fixture("ready");
    let project_root = fixture.project.root_path.clone();
    fs::write(project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
    fs::write(project_root.join("base.txt"), b"base\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", ".gitignore", "base.txt"]);
    run_git(&["commit", "-q", "-m", "base"]);
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![&base_sha, &fixture.campaign_id],
        )
        .unwrap();
    secure_editor_git_fixture_tree(&project_root.join(".git"));
    fs::set_permissions(
        project_root.join(".gitignore"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::set_permissions(project_root.join("base.txt"), fs::Permissions::from_mode(0o600))
        .unwrap();

    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::remove_dir_all(candidate_root).unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'reserved', base_sha = ?1,
                 candidate_ref = ?2, best_ref = ?3, updated_at = 4
             WHERE code_change_run_id = ?4",
            rusqlite::params![
                base_sha,
                "campaign/editor-campaign/candidate/editor-proposal",
                "campaign/editor-campaign/best",
                &fixture.run_id,
            ],
        )
        .unwrap();

    let mut report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(10, 10)
    .await
    .unwrap();
    assert_eq!(report.started.len(), 1);
    let mut started = report.started.pop().unwrap();
    assert_eq!(
        started.handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    drop(started);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "ready");
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "DELETE FROM code_change_checks
             WHERE code_change_run_id = ?1 AND attempt = 1",
            [&fixture.run_id],
        )
        .unwrap();

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(12, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 1);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Rejected);
    assert_eq!(run.candidate_sha, None);

    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let candidate_ref_check = Command::new("/usr/bin/git")
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/heads/{candidate_ref}"),
        ])
        .current_dir(&project_root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(!candidate_ref_check.status.success());
}

#[cfg(all(unix, target_os = "linux"))]
async fn prepared_code_change_reopen_fixture() -> (
    CodeChangeEditorFixture,
    ResolvedProjectExecutionPolicy,
    PathBuf,
    String,
) {
    prepared_code_change_reopen_fixture_with_behaviors("ready", None).await
}

#[cfg(all(unix, target_os = "linux"))]
async fn prepared_code_change_reopen_fixture_with_check_behavior(
    check_behavior: Option<&str>,
) -> (
    CodeChangeEditorFixture,
    ResolvedProjectExecutionPolicy,
    PathBuf,
    String,
) {
    prepared_code_change_reopen_fixture_with_behaviors("ready-two", check_behavior).await
}

#[cfg(all(unix, target_os = "linux"))]
async fn prepared_code_change_reopen_fixture_with_behaviors(
    editor_behavior: &str,
    check_behavior: Option<&str>,
) -> (
    CodeChangeEditorFixture,
    ResolvedProjectExecutionPolicy,
    PathBuf,
    String,
) {
    let fixture = code_change_editor_fixture_with_check_behavior(editor_behavior, check_behavior);
    let project_root = fixture.project.root_path.clone();
    fs::write(project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
    fs::write(project_root.join("base.txt"), b"base\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", ".gitignore", "base.txt"]);
    run_git(&["commit", "-q", "-m", "base"]);
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![&base_sha, &fixture.campaign_id],
        )
        .unwrap();
    secure_editor_git_fixture_tree(&project_root.join(".git"));
    fs::set_permissions(
        project_root.join(".gitignore"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::set_permissions(project_root.join("base.txt"), fs::Permissions::from_mode(0o600))
        .unwrap();

    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::remove_dir_all(&candidate_root).unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'reserved', base_sha = ?1,
                 candidate_ref = ?2, best_ref = ?3, updated_at = 4
             WHERE code_change_run_id = ?4",
            rusqlite::params![
                base_sha,
                "campaign/editor-campaign/candidate/editor-proposal",
                "campaign/editor-campaign/best",
                &fixture.run_id,
            ],
        )
        .unwrap();
    let project_config = config::load(&fixture.project.config_path).unwrap();
    let original_policy = resolve_project_policy(&fixture.policy, &fixture.project, &project_config)
        .unwrap();
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Reserved,
            pueue_agent::models::CodeChangeState::PreparingWorktree,
            5,
        )
        .unwrap();
    let candidate = prepare_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    drop(candidate);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::PreparingWorktree,
            pueue_agent::models::CodeChangeState::Editing,
            6,
        )
        .unwrap();
    (fixture, original_policy, project_root, base_sha)
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_check_stops_after_first_project_failure() {
    let (fixture, _original_policy, _project_root, _base_sha) =
        prepared_code_change_reopen_fixture_with_check_behavior(Some("fail-first")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    fs::create_dir(candidate_root.join("nested")).unwrap();
    fs::set_permissions(
        candidate_root.join("nested"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();
    let coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    );
    let mut started = coordinator.advance_ready(10, 10).await.unwrap().started;
    assert_eq!(started.len(), 1);
    assert_eq!(
        started.pop().unwrap().handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    let report = coordinator.advance_ready(12, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 1);
    assert_eq!(report.rejected, 0);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
    assert_eq!(run.candidate_sha, None);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "ready");
    assert_eq!(attempt.failure_code.as_deref(), Some("check_failed"));
    let checks = CodeChangeRepository::new(&fixture.db)
        .list_checks(&fixture.run_id, 1)
        .unwrap();
    assert_eq!(checks.len(), 3);
    assert_eq!(checks[0].status, pueue_agent::models::CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].status, pueue_agent::models::CodeChangeCheckStatus::Failed);
    assert_eq!(checks[2].status, pueue_agent::models::CodeChangeCheckStatus::Reserved);
    assert_eq!(checks[1].summary.as_deref(), Some("check returned non-zero"));
    assert_eq!(checks[2].summary, None);
    assert_eq!(checks[2].started_at, None);
    let invocation_count = fs::read_to_string(fixture._temp.path().join("check-invocations.state"))
        .unwrap();
    assert_eq!(invocation_count.trim(), "1");
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_check_timeout_is_persisted_without_waiting_a_minute() {
    let (fixture, _original_policy, _project_root, _base_sha) =
        prepared_code_change_reopen_fixture_with_check_behavior(Some("fail-first")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    fs::create_dir(candidate_root.join("nested")).unwrap();
    fs::set_permissions(
        candidate_root.join("nested"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();

    let coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    );
    let mut started = coordinator.advance_ready(10, 10).await.unwrap().started;
    assert_eq!(started.len(), 1);
    assert_eq!(
        started.pop().unwrap().handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    let first_round = coordinator.advance_ready(12, 10).await.unwrap();
    assert_eq!(first_round.advanced, 1);

    // The first ordinary round leaves the supervisor result durable. Reset
    // only the project rows so a short in-memory CheckRunner timeout can
    // exercise the timeout branch without re-running the supervisor check.
    fs::write(fixture._temp.path().join("check-behavior"), "timeout").unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_checks
             SET status = 'reserved', output_digest = NULL, summary = NULL,
                 started_at = NULL, finished_at = NULL
             WHERE code_change_run_id = ?1 AND attempt = 1 AND ordinal = 1",
            [&fixture.run_id],
        )
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_editor_attempts
             SET failure_code = NULL, failure_summary = NULL
             WHERE code_change_run_id = ?1 AND attempt = 1",
            [&fixture.run_id],
        )
        .unwrap();
    CodeChangeRepository::new(&fixture.db)
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Editing,
            pueue_agent::models::CodeChangeState::Checking,
            13,
        )
        .unwrap();

    let short_coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .with_code_change_check_timeout_for_test(Duration::from_millis(500));
    let report = short_coordinator.advance_ready(14, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 1);
    assert_eq!(report.rejected, 0);

    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
    assert_eq!(run.candidate_sha, None);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.failure_code.as_deref(), Some("check_failed"));
    let checks = CodeChangeRepository::new(&fixture.db)
        .list_checks(&fixture.run_id, 1)
        .unwrap();
    assert_eq!(checks.len(), 3);
    assert_eq!(checks[0].status, CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].status, CodeChangeCheckStatus::TimedOut);
    assert_eq!(checks[1].summary.as_deref(), Some("check timed out"));
    assert_eq!(checks[1].output_digest, None);
    assert_eq!(checks[2].status, CodeChangeCheckStatus::Reserved);
    assert_eq!(checks[2].started_at, None);
    assert_eq!(checks[2].finished_at, None);
    assert_eq!(
        fs::read_to_string(fixture._temp.path().join("check-invocations.state"))
            .unwrap()
            .trim(),
        "2"
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_check_rejects_combined_output_overflow() {
    let (fixture, _original_policy, _project_root, _base_sha) =
        prepared_code_change_reopen_fixture_with_check_behavior(Some("overflow")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    fs::create_dir(candidate_root.join("nested")).unwrap();
    fs::set_permissions(
        candidate_root.join("nested"),
        fs::Permissions::from_mode(0o700),
    )
    .unwrap();

    let coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    );
    let mut started = coordinator.advance_ready(10, 10).await.unwrap().started;
    assert_eq!(started.len(), 1);
    assert_eq!(
        started.pop().unwrap().handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    let report = coordinator.advance_ready(12, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 1);
    assert_eq!(report.rejected, 0);

    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
    assert_eq!(run.candidate_sha, None);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.failure_code.as_deref(), Some("check_failed"));
    let checks = CodeChangeRepository::new(&fixture.db)
        .list_checks(&fixture.run_id, 1)
        .unwrap();
    assert_eq!(checks.len(), 3);
    assert_eq!(checks[0].status, CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].status, CodeChangeCheckStatus::Failed);
    assert_eq!(
        checks[1].summary.as_deref(),
        Some("check output exceeded limit")
    );
    assert_eq!(checks[1].output_digest, None);
    assert_eq!(checks[2].status, CodeChangeCheckStatus::Reserved);
    assert_eq!(checks[2].started_at, None);
    assert_eq!(checks[2].finished_at, None);
    assert_eq!(
        fs::read_to_string(fixture._temp.path().join("check-invocations.state"))
            .unwrap()
            .trim(),
        "1"
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_check_rejects_tracked_file_mutation_after_project_check() {
    let (fixture, _original_policy, project_root, _base_sha) =
        prepared_code_change_reopen_fixture_with_behaviors("ready", Some("mutate")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    let original_candidate = b"candidate\n";
    fs::write(candidate_root.join("base.txt"), original_candidate).unwrap();

    let coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    );
    let mut started = coordinator.advance_ready(10, 10).await.unwrap().started;
    assert_eq!(started.len(), 1);
    assert_eq!(
        started.pop().unwrap().handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    let report = coordinator.advance_ready(12, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 1);
    assert_eq!(report.rejected, 0);

    let mutated = fs::read(candidate_root.join("base.txt")).unwrap();
    assert_ne!(mutated, original_candidate);
    assert_eq!(mutated, b"check-mutated\n");
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
    assert_eq!(run.candidate_sha, None);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.failure_code.as_deref(), Some("check_failed"));
    let checks = CodeChangeRepository::new(&fixture.db)
        .list_checks(&fixture.run_id, 1)
        .unwrap();
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].status, CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].status, CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].summary.as_deref(), Some("check passed"));
    assert!(checks[1].output_digest.is_some());

    for reference in [
        pueue_agent::code_change::candidate_ref(&fixture.campaign_id, "editor-proposal")
            .unwrap(),
        pueue_agent::code_change::best_ref(&fixture.campaign_id).unwrap(),
    ] {
        let reference_check = Command::new("/usr/bin/git")
            .args(["show-ref", "--verify", "--quiet", &format!("refs/heads/{reference}")])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(!reference_check.status.success(), "unexpected ref: {reference}");
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editing_passed_checks_commits_candidate() {
    let (fixture, _original_policy, project_root, base_sha) =
        prepared_code_change_reopen_fixture_with_behaviors("ready", Some("pass")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();

    let coordinator = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    );
    let mut report = coordinator.advance_ready(10, 10).await.unwrap();
    assert_eq!(report.started.len(), 1);
    let mut started = report.started.pop().unwrap();
    assert_eq!(
        started.handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    drop(started);

    let report = coordinator.advance_ready(12, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.advanced, 1);

    let repository = CodeChangeRepository::new(&fixture.db);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(
        run.state,
        pueue_agent::models::CodeChangeState::CandidateReady
    );
    let candidate_sha = run.candidate_sha.clone().unwrap();
    let checks = repository.list_checks(&fixture.run_id, 1).unwrap();
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].source, "supervisor");
    assert_eq!(checks[0].status, CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].source, "editor");
    assert_eq!(checks[1].status, CodeChangeCheckStatus::Passed);
    assert_eq!(
        fs::read_to_string(fixture._temp.path().join("check-invocations.state"))
            .unwrap()
            .trim(),
        "1"
    );

    let candidate_ref = "refs/heads/campaign/editor-campaign/candidate/editor-proposal";
    let ref_sha = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", candidate_ref])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(ref_sha, candidate_sha);
    let parent_sha = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", &format!("{candidate_sha}^")])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(parent_sha, base_sha);
    let main_head = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(main_head, base_sha);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
#[ignore = "requires real Cargo, uv, Python, and pytest installations"]
async fn code_change_supported_profiles_run_with_owned_outputs() {
    let (fixture, original_policy, _project_root, _base_sha) =
        prepared_code_change_reopen_fixture_with_behaviors("ready", Some("real-profiles")).await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    fs::create_dir(candidate_root.join("src")).unwrap();
    fs::write(
        candidate_root.join("Cargo.toml"),
        r#"[package]
name = "phase5-profile-fixture"
version = "0.1.0"
edition = "2021"
"#,
    )
    .unwrap();
    fs::write(candidate_root.join("src/lib.rs"), "pub fn profile_fixture() {}\n").unwrap();
    let lock = Command::new("cargo")
        .arg("generate-lockfile")
        .current_dir(&candidate_root)
        .output()
        .expect("real Cargo is required for the supported-profile regression");
    assert!(
        lock.status.success(),
        "cargo generate-lockfile failed: {}",
        String::from_utf8_lossy(&lock.stderr)
    );
    fs::create_dir(candidate_root.join("tests")).unwrap();
    fs::write(
        candidate_root.join("tests/profile_test.py"),
        "def test_profile():\n    assert True\n",
    )
    .unwrap();
    fs::write(
        candidate_root.join("pyproject.toml"),
        r#"[project]
name = "phase5-profile-fixture"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = ["pytest==8.4.2"]

[tool.uv]
package = false
"#,
    )
    .unwrap();
    let lock = Command::new("uv")
        .arg("lock")
        .current_dir(&candidate_root)
        .output()
        .expect("real uv is required for the supported-profile regression");
    assert!(
        lock.status.success(),
        "uv lock failed: {}",
        String::from_utf8_lossy(&lock.stderr)
    );

    let mut candidate = reopen_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    let before = candidate.verify().await.unwrap();
    let checks = [
        ("cargo", RUST_CHECK),
        ("uv", UV_PYTEST_CHECK),
        ("python", PYTHON_PYTEST_CHECK),
    ]
    .into_iter()
    .map(|(source, argv)| ProposedCheck {
        source: source.to_owned(),
        argv: argv.iter().map(|arg| (*arg).to_owned()).collect(),
        working_directory: ".".to_owned(),
    })
    .collect::<Vec<_>>();
    let mut digests = Vec::with_capacity(checks.len());
    for check in &checks {
        let profile_digest = match candidate
            .run_checks(std::slice::from_ref(check))
            .await
        {
            Ok(digest) => digest,
            Err(error) => {
                let preserved_root = fixture._temp.keep();
                panic!(
                    "{} profile failed ({error:?}); fixture root preserved at {}",
                    check.source,
                    preserved_root.display()
                );
            }
        };
        digests.extend(profile_digest);
    }
    assert_eq!(digests.len(), checks.len());
    assert!(digests.iter().all(|digest| !digest.is_empty()));
    assert_eq!(candidate.verify().await.unwrap(), before);

    for path in ["target", ".venv", ".pytest_cache", "__pycache__"] {
        assert!(
            !candidate_root.join(path).exists(),
            "supported profile left candidate output at {path}"
        );
    }
    let check_root = fixture
        ._temp
        .path()
        .join("execution-policy-state/code-change-checks/adhoc/attempt-1");
    assert!(
        !check_root.join("check-1").exists(),
        "check output scope was not cleaned"
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_reopen_rejects_tampered_persisted_git_baseline() {
    for mutation in ["protected-ref", "remote-config", "missing-baseline"] {
        let (fixture, original_policy, project_root, base_sha) =
            prepared_code_change_reopen_fixture().await;
        match mutation {
            "protected-ref" => {
                let output = Command::new("git")
                    .args(["update-ref", "refs/heads/uncontrolled", &base_sha])
                    .current_dir(&project_root)
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .output()
                    .unwrap();
                assert!(output.status.success());
            }
            "remote-config" => {
                let output = Command::new("git")
                    .args(["config", "remote.origin.url", "https://example.invalid/repo"])
                    .current_dir(&project_root)
                    .env("GIT_CONFIG_NOSYSTEM", "1")
                    .env("GIT_CONFIG_GLOBAL", "/dev/null")
                    .output()
                    .unwrap();
                assert!(output.status.success());
            }
            "missing-baseline" => {
                fixture
                    .db
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE code_change_runs SET protected_ref_digest = NULL
                         WHERE code_change_run_id = ?1",
                        [&fixture.run_id],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let reopened = reopen_code_change_worktree_for_run(
            &fixture.policy,
            &fixture.project,
            &original_policy,
            &fixture.db,
            &fixture.run_id,
        )
        .await;
        assert!(reopened.is_err(), "{mutation} must fail closed");
        let run = CodeChangeRepository::new(&fixture.db)
            .find_by_id(&fixture.run_id)
            .unwrap()
            .unwrap();
        assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
        assert_eq!(run.candidate_sha, None);
        assert!(fixture.candidate_policy.root_anchor.canonical_path.is_dir());
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_checking_restart_resumes_persisted_plan_without_duplicate_rows() {
    let fixture = code_change_editor_fixture("ready");
    let project_root = fixture.project.root_path.clone();
    fs::write(project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
    fs::write(project_root.join("base.txt"), b"base\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", ".gitignore", "base.txt"]);
    run_git(&["commit", "-q", "-m", "base"]);
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![&base_sha, &fixture.campaign_id],
        )
        .unwrap();
    secure_editor_git_fixture_tree(&project_root.join(".git"));
    fs::set_permissions(
        project_root.join(".gitignore"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::set_permissions(project_root.join("base.txt"), fs::Permissions::from_mode(0o600))
        .unwrap();

    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::remove_dir_all(&candidate_root).unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'reserved', base_sha = ?1,
                 candidate_ref = ?2, best_ref = ?3, updated_at = 4
             WHERE code_change_run_id = ?4",
            rusqlite::params![
                base_sha,
                "campaign/editor-campaign/candidate/editor-proposal",
                "campaign/editor-campaign/best",
                &fixture.run_id,
            ],
        )
        .unwrap();

    let mut report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(10, 10)
    .await
    .unwrap();
    let mut started = report.started.pop().unwrap();
    assert_eq!(
        started.handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    drop(started);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "ready");

    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    let project_config = config::load(&fixture.project.config_path).unwrap();
    let original_policy = resolve_project_policy(&fixture.policy, &fixture.project, &project_config)
        .unwrap();
    let mut candidate = reopen_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    let facts = candidate.verify().await.unwrap();
    drop(candidate);
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Editing,
            pueue_agent::models::CodeChangeState::Checking,
            12,
        )
        .unwrap();
    repository
        .record_checked_diff(
            &fixture.run_id,
            facts.persisted_digest(),
            i64::try_from(facts.file_count).unwrap(),
            i64::try_from(facts.diff_bytes).unwrap(),
            12,
        )
        .unwrap();
    let before = repository.list_checks(&fixture.run_id, 1).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].source, "editor");

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(13, 10)
    .await
    .unwrap();
    assert_eq!(report.started.len(), 0);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Editing);
    assert_eq!(run.diff_digest, None);
    assert_eq!(run.changed_file_count, None);
    assert_eq!(run.diff_bytes, None);
    let checks = repository.list_checks(&fixture.run_id, 1).unwrap();
    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].ordinal, 0);
    assert_eq!(checks[0].status, pueue_agent::models::CodeChangeCheckStatus::Passed);
    assert_eq!(checks[1].ordinal, 1);
    assert_eq!(checks[1].status, pueue_agent::models::CodeChangeCheckStatus::Failed);
    assert_eq!(
        checks[1].summary.as_deref(),
        Some("check returned non-zero")
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_committing_restart_recommits_dirty_candidate_without_ref() {
    let fixture = code_change_editor_fixture("ready");
    let project_root = fixture.project.root_path.clone();
    fs::write(project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
    fs::write(project_root.join("base.txt"), b"base\n").unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run_git(&["init", "-q", "-b", "main"]);
    run_git(&["config", "user.name", "fixture"]);
    run_git(&["config", "user.email", "fixture@example.invalid"]);
    run_git(&["add", ".gitignore", "base.txt"]);
    run_git(&["commit", "-q", "-m", "base"]);
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET base_revision_sha = ?1 WHERE campaign_id = ?2",
            rusqlite::params![&base_sha, &fixture.campaign_id],
        )
        .unwrap();
    secure_editor_git_fixture_tree(&project_root.join(".git"));
    fs::set_permissions(
        project_root.join(".gitignore"),
        fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    fs::set_permissions(project_root.join("base.txt"), fs::Permissions::from_mode(0o600))
        .unwrap();

    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::remove_dir_all(&candidate_root).unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'reserved', base_sha = ?1,
                 candidate_ref = ?2, best_ref = ?3, updated_at = 4
             WHERE code_change_run_id = ?4",
            rusqlite::params![
                base_sha,
                "campaign/editor-campaign/candidate/editor-proposal",
                "campaign/editor-campaign/best",
                &fixture.run_id,
            ],
        )
        .unwrap();

    let mut report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(10, 10)
    .await
    .unwrap();
    let mut started = report.started.pop().unwrap();
    assert_eq!(
        started.handle.wait(&fixture.db, 11).await.unwrap(),
        AgentRunStatus::Completed
    );
    drop(started);
    assert_eq!(
        CodeChangeRepository::new(&fixture.db)
            .find_editor_attempt(&fixture.run_id, 1)
            .unwrap()
            .unwrap()
            .status,
        "ready"
    );

    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    let project_config = config::load(&fixture.project.config_path).unwrap();
    let original_policy = resolve_project_policy(&fixture.policy, &fixture.project, &project_config)
        .unwrap();
    let mut candidate = reopen_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    let facts = candidate.verify().await.unwrap();
    drop(candidate);
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Editing,
            pueue_agent::models::CodeChangeState::Checking,
            12,
        )
        .unwrap();
    repository
        .record_checked_diff(
            &fixture.run_id,
            facts.persisted_digest(),
            i64::try_from(facts.file_count).unwrap(),
            i64::try_from(facts.diff_bytes).unwrap(),
            12,
        )
        .unwrap();
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Checking,
            pueue_agent::models::CodeChangeState::Committing,
            13,
        )
        .unwrap();

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(14, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(report.advanced, 1);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(
        run.state,
        pueue_agent::models::CodeChangeState::CandidateReady
    );
    let candidate_sha = run.candidate_sha.clone().unwrap();
    assert_eq!(run.diff_digest.as_deref(), Some(facts.persisted_digest()));
    assert_eq!(run.changed_file_count, Some(facts.file_count as i64));
    assert_eq!(run.diff_bytes, Some(facts.diff_bytes as i64));
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let ref_sha = String::from_utf8(
        run_git(&["rev-parse", &format!("refs/heads/{candidate_ref}")]).stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(ref_sha, candidate_sha);
    assert_eq!(
        String::from_utf8(run_git(&["rev-parse", &format!("{candidate_sha}^")]).stdout)
            .unwrap()
            .trim(),
        base_sha
    );
    let checks = repository.list_checks(&fixture.run_id, 1).unwrap();
    assert_eq!(checks.len(), 1);
}

#[cfg(all(unix, target_os = "linux"))]
async fn committed_code_change_restart_fixture() -> (
    CodeChangeEditorFixture,
    PathBuf,
    String,
    String,
    i64,
    i64,
) {
    let (fixture, original_policy, project_root, _base_sha) =
        prepared_code_change_reopen_fixture().await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    let mut candidate = reopen_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    let facts = candidate.verify().await.unwrap();
    let digest = facts.persisted_digest().to_owned();
    let file_count = i64::try_from(facts.file_count).unwrap();
    let diff_bytes = i64::try_from(facts.diff_bytes).unwrap();
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Editing,
            pueue_agent::models::CodeChangeState::Checking,
            12,
        )
        .unwrap();
    repository
        .record_checked_diff(
            &fixture.run_id,
            &digest,
            file_count,
            diff_bytes,
            12,
        )
        .unwrap();
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Checking,
            pueue_agent::models::CodeChangeState::Committing,
            13,
        )
        .unwrap();
    let candidate_sha = candidate.commit().await.unwrap();
    drop(candidate);
    (
        fixture,
        project_root,
        candidate_sha,
        digest,
        file_count,
        diff_bytes,
    )
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_candidate_commit_uses_fixed_identity_and_message() {
    let (fixture, project_root, candidate_sha, _digest, _file_count, _diff_bytes) =
        committed_code_change_restart_fixture().await;
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    let metadata = String::from_utf8(
        run_git(&[
            "show",
            "-s",
            "--format=%an%x00%ae%x00%at%x00%aI%x00%cn%x00%ce%x00%ct%x00%cI%x00%s%x00%B",
            &candidate_sha,
        ])
        .stdout,
    )
    .unwrap();
    let fields = metadata.trim_end().split('\0').collect::<Vec<_>>();
    assert_eq!(fields.len(), 10);
    assert_eq!(fields[0], "pueue-agent");
    assert_eq!(fields[1], "pueue-agent@localhost");
    assert_eq!(fields[2], "946684800");
    assert!(matches!(
        fields[3],
        "2000-01-01T00:00:00Z" | "2000-01-01T00:00:00+00:00"
    ));
    assert_eq!(fields[4], "pueue-agent");
    assert_eq!(fields[5], "pueue-agent@localhost");
    assert_eq!(fields[6], "946684800");
    assert!(matches!(
        fields[7],
        "2000-01-01T00:00:00Z" | "2000-01-01T00:00:00+00:00"
    ));
    assert_eq!(fields[8], "pueue-agent code-change candidate");
    assert_eq!(fields[9], "pueue-agent code-change candidate");

    let parent = String::from_utf8(
        run_git(&["rev-parse", &format!("{candidate_sha}^")]).stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let base_sha = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert_eq!(parent, base_sha);
    let candidate_tree = String::from_utf8(
        run_git(&["rev-parse", &format!("{candidate_sha}^{{tree}}")]).stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let base_tree = String::from_utf8(run_git(&["rev-parse", &format!("{base_sha}^{{tree}}")]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert_ne!(candidate_tree, base_tree);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.candidate_sha, None);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_committing_restart_rejects_competing_candidate_ref() {
    let (fixture, original_policy, project_root, base_sha) =
        prepared_code_change_reopen_fixture().await;
    let candidate_root = fixture.candidate_policy.root_anchor.canonical_path.clone();
    fs::write(candidate_root.join("base.txt"), b"candidate\n").unwrap();
    let mut candidate = reopen_code_change_worktree_for_run(
        &fixture.policy,
        &fixture.project,
        &original_policy,
        &fixture.db,
        &fixture.run_id,
    )
    .await
    .unwrap();
    let facts = candidate.verify().await.unwrap();
    drop(candidate);
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Editing,
            pueue_agent::models::CodeChangeState::Checking,
            12,
        )
        .unwrap();
    repository
        .record_checked_diff(
            &fixture.run_id,
            facts.persisted_digest(),
            i64::try_from(facts.file_count).unwrap(),
            i64::try_from(facts.diff_bytes).unwrap(),
            12,
        )
        .unwrap();
    repository
        .transition(
            &fixture.run_id,
            pueue_agent::models::CodeChangeState::Checking,
            pueue_agent::models::CodeChangeState::Committing,
            13,
        )
        .unwrap();
    let run_git = |args: &[&str]| {
        let output = Command::new("git")
            .args(args)
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?}: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let candidate_ref = format!("refs/heads/{candidate_ref}");
    // Install a competing ref before the commit/publish attempt. The
    // committing path must fail closed without replacing this existing ref.
    run_git(&["update-ref", &candidate_ref, &base_sha]);

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(14, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 1);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::RecoveryRequired);
    assert_eq!(run.rejection_code.as_deref(), Some("worktree_recovery_required"));
    assert_eq!(run.candidate_sha, None);
    let preserved_ref = String::from_utf8(run_git(&["rev-parse", &candidate_ref]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert_eq!(preserved_ref, base_sha);
    let base_after = String::from_utf8(run_git(&["rev-parse", "HEAD"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert_eq!(base_after, base_sha);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_committing_restart_recovers_existing_ref_before_record() {
    let (fixture, project_root, candidate_sha, digest, file_count, diff_bytes) =
        committed_code_change_restart_fixture().await;
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let candidate_ref = format!("refs/heads/{candidate_ref}");
    let read_ref = || {
        let output = Command::new("git")
            .args(["rev-parse", candidate_ref.as_str()])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    assert_eq!(read_ref(), candidate_sha);
    let repository = CodeChangeRepository::new(&fixture.db);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(run.state, pueue_agent::models::CodeChangeState::Committing);
    assert_eq!(run.candidate_sha, None);

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(14, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(report.advanced, 1);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(
        run.state,
        pueue_agent::models::CodeChangeState::CandidateReady
    );
    assert_eq!(run.candidate_sha.as_deref(), Some(candidate_sha.as_str()));
    assert_eq!(run.diff_digest.as_deref(), Some(digest.as_str()));
    assert_eq!(run.changed_file_count, Some(file_count));
    assert_eq!(run.diff_bytes, Some(diff_bytes));
    assert_eq!(read_ref(), candidate_sha);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_committing_restart_after_candidate_record_is_idempotent() {
    let (fixture, project_root, candidate_sha, digest, file_count, diff_bytes) =
        committed_code_change_restart_fixture().await;
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .record_candidate(
            &fixture.run_id,
            &candidate_sha,
            &digest,
            file_count,
            diff_bytes,
            14,
        )
        .unwrap();
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let candidate_ref = format!("refs/heads/{candidate_ref}");
    let read_ref = || {
        let output = Command::new("git")
            .args(["rev-parse", candidate_ref.as_str()])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    let ref_before = read_ref();
    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(15, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(report.advanced, 1);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(
        run.state,
        pueue_agent::models::CodeChangeState::CandidateReady
    );
    assert_eq!(run.candidate_sha.as_deref(), Some(candidate_sha.as_str()));
    assert_eq!(read_ref(), ref_before);

    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(16, 10)
    .await
    .unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 0);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(read_ref(), ref_before);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_committing_restart_rejects_persisted_diff_mismatch() {
    let (fixture, project_root, candidate_sha, _digest, _file_count, _diff_bytes) =
        committed_code_change_restart_fixture().await;
    let repository = CodeChangeRepository::new(&fixture.db);
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs SET diff_digest = ?1
             WHERE code_change_run_id = ?2",
            rusqlite::params!["f".repeat(64), &fixture.run_id],
        )
        .unwrap();
    let candidate_ref = pueue_agent::code_change::candidate_ref(
        &fixture.campaign_id,
        "editor-proposal",
    )
    .unwrap();
    let candidate_ref = format!("refs/heads/{candidate_ref}");
    let read_ref = || {
        let output = Command::new("git")
            .args(["rev-parse", candidate_ref.as_str()])
            .current_dir(&project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    assert_eq!(read_ref(), candidate_sha);
    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(15, 10)
    .await
    .unwrap();
    assert_eq!(report.rejected, 1);
    let run = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(
        run.state,
        pueue_agent::models::CodeChangeState::RecoveryRequired
    );
    assert_eq!(run.candidate_sha, None);
    assert_eq!(read_ref(), candidate_sha);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_timeout_persists_bounded_failure_before_cleanup() {
    let fixture = code_change_editor_fixture("timeout");
    let mut editor = spawn_editor_fixture_attempt(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await;
    assert_eq!(editor.timeout_now(&fixture.db, 11).await.unwrap(), AgentRunStatus::TimedOut);
    let attempt = CodeChangeRepository::new(&fixture.db)
        .find_editor_attempt(&fixture.run_id, 1)
        .unwrap()
        .unwrap();
    assert_eq!(attempt.status, "failed");
    assert_eq!(attempt.failure_code.as_deref(), Some("editor_timeout"));
    assert_eq!(
        CodeChangeRepository::new(&fixture.db)
            .find_by_id(&fixture.run_id)
            .unwrap()
            .unwrap()
            .state,
        pueue_agent::models::CodeChangeState::Editing
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_malformed_and_oversized_outputs_fail_closed() {
    for behavior in ["malformed", "oversized"] {
        let fixture = code_change_editor_fixture(behavior);
        let mut editor = spawn_editor_fixture_attempt(
            &fixture.runner,
            &fixture.db,
            &fixture.project,
            &fixture.candidate_policy,
            &fixture.config,
            &fixture.run_id,
            &fixture.campaign_id,
            1,
            AgentContextMode::Fresh,
            10,
        )
        .await;
        assert_eq!(
            editor.wait(&fixture.db, 11).await.unwrap(),
            AgentRunStatus::Completed,
            "editor process should terminate normally for {behavior}"
        );
        let attempt = CodeChangeRepository::new(&fixture.db)
            .find_editor_attempt(&fixture.run_id, 1)
            .unwrap()
            .unwrap();
        assert_eq!(attempt.status, "failed");
        assert_eq!(attempt.failure_code.as_deref(), Some("editor_output_invalid"));
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_editor_first_and_second_process_failures_reject_once_and_restart_without_reset() {
    let fixture = code_change_editor_fixture("fail");
    let mut first = spawn_editor_fixture_attempt(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        1,
        AgentContextMode::Fresh,
        10,
    )
    .await;
    assert_eq!(first.wait(&fixture.db, 11).await.unwrap(), AgentRunStatus::Failed);
    let session = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap()
        .editor_session_id
        .unwrap();
    let mut second = spawn_editor_fixture_attempt(
        &fixture.runner,
        &fixture.db,
        &fixture.project,
        &fixture.candidate_policy,
        &fixture.config,
        &fixture.run_id,
        &fixture.campaign_id,
        2,
        AgentContextMode::Resume {
            session_id: session.clone(),
        },
        12,
    )
    .await;
    assert_eq!(second.wait(&fixture.db, 13).await.unwrap(), AgentRunStatus::Failed);
    let attempts = CodeChangeRepository::new(&fixture.db)
        .list_editor_attempts(&fixture.run_id)
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].failure_code.as_deref(), Some("editor_exit"));
    assert_eq!(attempts[1].failure_code.as_deref(), Some("editor_exit"));
    assert_eq!(attempts[0].editor_session_id, attempts[1].editor_session_id);
    assert_eq!(
        CodeChangeRepository::new(&fixture.db)
            .find_by_id(&fixture.run_id)
            .unwrap()
            .unwrap()
            .state,
        pueue_agent::models::CodeChangeState::Rejected
    );
    let report = CodeChangeCoordinator::new(
        &fixture.db,
        &fixture.runner,
        &fixture.policy,
        CampaignLimits::default(),
    )
    .advance_ready(14, 10)
    .await
    .unwrap();
    assert!(report.started.is_empty());
    assert_eq!(
        CodeChangeRepository::new(&fixture.db)
            .list_editor_attempts(&fixture.run_id)
            .unwrap()
            .len(),
        2,
        "restart must not reset a terminal editor attempt count"
    );
    assert_eq!(
        AgentRunRepository::new(&fixture.db)
            .find_by_id(second.run_id)
            .unwrap()
            .unwrap()
            .context_mode,
        AgentContextMode::Resume { session_id: session }
    );
}
