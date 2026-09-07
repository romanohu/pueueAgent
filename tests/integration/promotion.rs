use std::fs;

#[cfg(all(unix, target_os = "linux"))]
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use pueue_agent::{
    db::{
        CampaignRepository, Db, MetricsRepository, ProjectRepository, StartCampaignRequest,
    },
    execution_policy::CampaignLimits,
    models::{ExperimentMetricsRow, ExperimentStatus, MetricDirection, ObjectiveMetric, ProposalKind},
    promotion::{evaluate, preview_code_candidate, PromotionOutcome},
    proposals::{self, ProposalInput},
    state::ObjectiveSnapshot,
};

#[cfg(all(unix, target_os = "linux"))]
use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    code_change::{
        best_ref, candidate_ref, prepare_code_change_worktree_for_run, CodeChangeCoordinator,
    },
    config,
    db::{
        AgentRunRepository, CodeChangeRepository, EventRepository, ExperimentRepository,
        NewCodeChangeCheck, TaskObservationRepository,
    },
    execution_policy::{resolve_project_policy, ResolvedExecutionPolicy, VerifiedWorkingDirectory},
    models::{
        AgentRunStatus, CodeChangeState, EventKind, NewAgentRun, NewCodeChangeRun, NewEvent,
        NewProject, NewTaskObservation,
    },
    pueue::{PueueApi, PueueTask},
    reconcile::{managed_task_run_signature, task_signature, Reconciler},
    AppError,
};
#[cfg(all(unix, target_os = "linux"))]
use async_trait::async_trait;
use rusqlite::params;
use serde_json::json;
use tempfile::TempDir;

#[cfg(all(unix, target_os = "linux"))]
#[path = "../support/execution_policy_fixture.rs"]
mod execution_policy_fixture;

const CAMPAIGN_ID: &str = "campaign-promotion";
const PROJECT_ID: &str = "project-a";
const BASELINE_EXPERIMENT_ID: &str = "promo-experiment-baseline";
const BASELINE_SUBMISSION_ID: &str = "promo-submission-baseline";
const BASELINE_PROPOSAL_ID: &str = "promo-proposal-baseline";
const CHALLENGER_EXPERIMENT_ID: &str = "promo-experiment-challenger";
const OTHER_CAMPAIGN_ID: &str = "campaign-promotion-other";
const OTHER_EXPERIMENT_ID: &str = "promo-experiment-other";

fn minimize_metric() -> ObjectiveMetric {
    ObjectiveMetric {
        name: "loss".to_owned(),
        direction: MetricDirection::Minimize,
        min_delta: Some(0.01),
    }
}

fn maximize_metric() -> ObjectiveMetric {
    ObjectiveMetric {
        name: "score".to_owned(),
        direction: MetricDirection::Maximize,
        min_delta: None,
    }
}

struct Harness {
    _temp: TempDir,
    db: Db,
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        #[cfg(unix)]
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        #[cfg(unix)]
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&pueue_agent::models::NewProject::new(
                PROJECT_ID,
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self { _temp: temp, db }
    }

    fn start_campaign(&self, objective_metric: Option<&ObjectiveMetric>) {
        self.try_start_campaign(objective_metric).unwrap();
    }

    fn try_start_campaign(
        &self,
        objective_metric: Option<&ObjectiveMetric>,
    ) -> Result<(), pueue_agent::AppError> {
        let objective = ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "objective-digest".to_owned(),
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
        )?;
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: CAMPAIGN_ID,
                    project_id: PROJECT_ID,
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: BASELINE_SUBMISSION_ID,
                    experiment_id: BASELINE_EXPERIMENT_ID,
                    proposal_id: BASELINE_PROPOSAL_ID,
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            ?;
        Ok(())
    }

    fn seed_metrics(&self, experiment_id: &str, value: Option<f64>) {
        MetricsRepository::upsert(
            &self.db,
            &ExperimentMetricsRow {
                experiment_id: experiment_id.to_owned(),
                source: "manifest".to_owned(),
                primary_metric_name: Some("loss".to_owned()),
                primary_metric_value: value,
                metrics_json: "{}".to_owned(),
                artifact_defect: None,
                created_at: 150,
                updated_at: 150,
                evaluated_at: None,
            },
        )
        .unwrap();
    }

    fn add_experiment(&self, experiment_id: &str) {
        self.db.connect().unwrap().execute_batch(&format!(
            "INSERT INTO submissions (
                 submission_id, project_id, argv_json, created_at, pueue_task_id,
                 task_signature, status, kind, metadata_json, origin_agent_run_id
             )
             SELECT '{experiment_id}-submission', project_id, argv_json, created_at + 1,
                    pueue_task_id, task_signature, status, kind, metadata_json,
                    origin_agent_run_id
             FROM submissions WHERE submission_id = '{BASELINE_SUBMISSION_ID}';
             INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis, source_experiment_id,
                 argv_json, working_directory, expected_evidence_json, canonical_digest,
                 reject_reason, created_at, updated_at
             )
             SELECT '{experiment_id}-proposal', campaign_id, kind, status, hypothesis,
                    source_experiment_id, argv_json, working_directory,
                    expected_evidence_json, '{experiment_id}-proposal-digest',
                    reject_reason, created_at + 1, updated_at + 1
             FROM proposals WHERE proposal_id = '{BASELINE_PROPOSAL_ID}';
             INSERT INTO experiments (
                 experiment_id, campaign_id, proposal_id, submission_id,
                 parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                 failure_code, failure_fingerprint, created_at, updated_at, finished_at
             )
             SELECT '{experiment_id}', campaign_id, '{experiment_id}-proposal',
                    '{experiment_id}-submission', parent_experiment_id, attempt + 1, status,
                    pueue_task_id, task_signature, failure_code, failure_fingerprint,
                    created_at + 1, updated_at + 1, finished_at
             FROM experiments WHERE experiment_id = '{BASELINE_EXPERIMENT_ID}';",
        ))
        .unwrap();
    }

    fn set_promotion_state(&self, current_best: Option<&str>, plateau_count: i64) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns
                 SET current_best_experiment_id = ?1, plateau_count = ?2
                 WHERE campaign_id = ?3",
                params![current_best, plateau_count, CAMPAIGN_ID],
            )
            .unwrap();
    }

    fn set_campaign_state(&self, state: &str) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns SET state = ?1 WHERE campaign_id = ?2",
                params![state, CAMPAIGN_ID],
            )
            .unwrap();
    }

    fn add_other_campaign_experiment(&self) {
        let connection = self.db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO campaigns (
                    campaign_id, project_id, objective_text, objective_digest,
                    initial_argv_json, state, baseline_experiment_id, next_eligible_at,
                    objective_metric_json, current_best_experiment_id, plateau_count,
                    created_at, updated_at
                 ) VALUES (?1, ?2, 'other objective', 'other-digest', '[]', 'retired',
                           NULL, NULL, ?3, NULL, 0, 100, 100)",
                params![
                    OTHER_CAMPAIGN_ID,
                    PROJECT_ID,
                    serde_json::to_string(&minimize_metric()).unwrap(),
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at, pueue_task_id,
                    task_signature, status, kind, metadata_json, origin_agent_run_id
                 )
                 SELECT ?1, project_id, argv_json, created_at + 1, pueue_task_id,
                        task_signature, status, kind, metadata_json, origin_agent_run_id
                 FROM submissions WHERE submission_id = ?2",
                params![format!("{OTHER_EXPERIMENT_ID}-submission"), BASELINE_SUBMISSION_ID],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO proposals (
                    proposal_id, campaign_id, kind, status, hypothesis, source_experiment_id,
                    argv_json, working_directory, expected_evidence_json, canonical_digest,
                    reject_reason, created_at, updated_at
                 )
                 SELECT ?1, ?2, kind, status, hypothesis, source_experiment_id, argv_json,
                        working_directory, expected_evidence_json, ?3, reject_reason,
                        created_at + 1, updated_at + 1
                 FROM proposals WHERE proposal_id = ?4",
                params![
                    format!("{OTHER_EXPERIMENT_ID}-proposal"),
                    OTHER_CAMPAIGN_ID,
                    format!("{OTHER_EXPERIMENT_ID}-digest"),
                    BASELINE_PROPOSAL_ID,
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO experiments (
                    experiment_id, campaign_id, proposal_id, submission_id,
                    parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                    failure_code, failure_fingerprint, created_at, updated_at, finished_at
                 )
                 SELECT ?1, ?2, ?3, ?4, parent_experiment_id, attempt, status,
                        pueue_task_id, task_signature, failure_code, failure_fingerprint,
                        created_at + 1, updated_at + 1, finished_at
                 FROM experiments WHERE experiment_id = ?5",
                params![
                    OTHER_EXPERIMENT_ID,
                    OTHER_CAMPAIGN_ID,
                    format!("{OTHER_EXPERIMENT_ID}-proposal"),
                    format!("{OTHER_EXPERIMENT_ID}-submission"),
                    BASELINE_EXPERIMENT_ID,
                ],
            )
            .unwrap();
    }

    fn set_baseline_experiment_id(&self, experiment_id: Option<&str>) {
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE campaigns SET baseline_experiment_id = ?1 WHERE campaign_id = ?2",
                params![experiment_id, CAMPAIGN_ID],
            )
            .unwrap();
    }

    fn campaign_evaluation_row(
        &self,
        campaign_id: &str,
    ) -> (Option<String>, Option<String>, i64) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT baseline_experiment_id, current_best_experiment_id, plateau_count
                 FROM campaigns WHERE campaign_id = ?1",
                [campaign_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn promotion_row(&self) -> (Option<String>, i64) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT current_best_experiment_id, plateau_count
                 FROM campaigns WHERE campaign_id = ?1",
                [CAMPAIGN_ID],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn evaluated_at(&self, experiment_id: &str) -> Option<String> {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = ?1",
                [experiment_id],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn strategy_refresh_wakes(&self) -> Vec<(String, String)> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare(
                "SELECT dedup_key, status FROM events
                 WHERE project_id = ?1 AND kind = 'operator_wake'
                   AND dedup_key LIKE 'strategy-refresh:v1:%'
                 ORDER BY created_at, event_id",
            )
            .unwrap();
        statement
            .query_map([PROJECT_ID], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn wake_payload(&self, dedup_key: &str) -> (Option<String>, serde_json::Value) {
        let (campaign_id, payload): (Option<String>, String) = self
            .db
            .connect()
            .unwrap()
            .query_row(                "SELECT campaign_id, payload_json FROM events
                 WHERE project_id = ?1 AND dedup_key = ?2",
                params![PROJECT_ID, dedup_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        (campaign_id, serde_json::from_str(&payload).unwrap())
    }
}

fn run_non_improvement(
    harness: &Harness,
    index: usize,
    limits: &CampaignLimits,
) -> PromotionOutcome {
    let experiment_id = format!("{CHALLENGER_EXPERIMENT_ID}-{index}");
    harness.add_experiment(&experiment_id);
    harness.seed_metrics(&experiment_id, Some(2.0));
    evaluate(
        &harness.db,
        CAMPAIGN_ID,
        &experiment_id,
        ExperimentStatus::Succeeded,
        limits,
        300 + index as i64,
    )
    .unwrap()
}

#[test]
fn improvement_updates_current_best_and_resets_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 3);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::Improved);
    assert_eq!(
        harness.promotion_row(),
        (Some(CHALLENGER_EXPERIMENT_ID.to_owned()), 0)
    );
}

#[test]
fn code_change_candidate_preview_is_side_effect_free_and_bounded() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 3);

    let candidate_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let old_best_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let mut connection = harness.db.connect().unwrap();
    let transaction = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let plan = preview_code_candidate(
        &transaction,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        Some(old_best_sha),
        candidate_sha,
    )
    .unwrap();

    assert_eq!(plan.outcome, PromotionOutcome::Improved);
    assert_eq!(plan.expected_current_best_experiment_id.as_deref(), Some(BASELINE_EXPERIMENT_ID));
    assert_eq!(plan.expected_old_sha.as_deref(), Some(old_best_sha));
    assert_eq!(plan.candidate_sha.as_deref(), Some(candidate_sha));
    let promotion_state: (Option<String>, i64) = transaction
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        promotion_state,
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 3)
    );
    let evaluated_at: Option<String> = transaction
        .query_row(
            "SELECT evaluated_at FROM experiment_metrics WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(evaluated_at, None);
    transaction.rollback().unwrap();
}

#[cfg(all(unix, target_os = "linux"))]
struct FixturePueue {
    task: PueueTask,
}

#[cfg(all(unix, target_os = "linux"))]
#[async_trait]
impl PueueApi for FixturePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(vec![self.task.clone()])
    }

    async fn add(&self, _args: &[OsString]) -> Result<i64, AppError> {
        panic!("promotion fixture does not submit Pueue tasks")
    }

    async fn kill(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("promotion fixture does not kill Pueue tasks")
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        panic!("promotion fixture does not remove Pueue tasks")
    }

    async fn ensure_group(&self, _group: &str) -> Result<(), AppError> {
        panic!("promotion fixture does not create Pueue groups")
    }
}

#[cfg(all(unix, target_os = "linux"))]
struct CodeChangePromotionFixture {
    _temp: TempDir,
    db: Db,
    policy: Arc<ResolvedExecutionPolicy>,
    runner: AgentRunner,
    project_root: PathBuf,
    candidate_path: PathBuf,
    run_id: String,
    campaign_id: String,
    proposal_id: String,
    baseline_experiment_id: String,
    experiment_id: String,
    base_sha: String,
    candidate_sha: String,
    best_ref: String,
}

#[cfg(all(unix, target_os = "linux"))]
impl CodeChangePromotionFixture {
    async fn new() -> Self {
        Self::new_with_terminal_submission(true).await
    }

    async fn new_nonterminal() -> Self {
        Self::new_with_terminal_submission(false).await
    }

    async fn new_with_terminal_submission(terminal_submission: bool) -> Self {
        Self::new_with_terminal_submission_and_origin(terminal_submission, true).await
    }

    async fn new_without_origin() -> Self {
        Self::new_with_terminal_submission_and_origin(true, false).await
    }

    async fn new_with_terminal_submission_and_origin(
        terminal_submission: bool,
        persist_origin: bool,
    ) -> Self {
        let temp = TempDir::new().unwrap();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let fixture_root = fs::canonicalize(temp.path()).unwrap();
        let project_root = fixture_root.join("project");
        let service_dir = project_root.join(".pueue-agent");
        fs::create_dir_all(&service_dir).unwrap();
        fs::set_permissions(&project_root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&service_dir, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(&project_root.join(".gitignore"), ".pueue-agent/\n").unwrap();
        fs::write(project_root.join("base.txt"), b"base\n").unwrap();
        fs::write(
            service_dir.join("config.toml"),
            r#"project_id = "promotion-project"
pueue_group = "promotion-project"

[agent]
program = "codex"
args = ["{prompt}"]
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
        )
        .unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("/usr/bin/git")
                .args(args)
                .current_dir(&project_root)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", "/dev/null")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            output
        };
        git(&["init", "-q", "-b", "main"]);
        git(&["config", "user.name", "fixture"]);
        git(&["config", "user.email", "fixture@example.invalid"]);
        git(&["add", "."]);
        git(&["commit", "-q", "-m", "base"]);
        let base_sha = String::from_utf8(git(&["rev-parse", "HEAD"]).stdout)
            .unwrap()
            .trim()
            .to_owned();

        let trusted_git = fixture_root.join("execution-policy-bin/git");
        fs::create_dir_all(trusted_git.parent().unwrap()).unwrap();
        fs::copy("/usr/bin/git", &trusted_git).unwrap();
        fs::set_permissions(&trusted_git, fs::Permissions::from_mode(0o700)).unwrap();

        let policy = execution_policy_fixture::resolved_policy(
            &fixture_root,
            &[("promotion-project", &project_root, Path::new("codex"))],
        );
        let db = Db::open(&fixture_root.join("state.sqlite3")).unwrap();
        let project = ProjectRepository::new(&db)
            .register(&NewProject::new(
                "promotion-project",
                fs::canonicalize(&project_root).unwrap(),
                "promotion-project",
                &service_dir.join("config.toml"),
                1,
            ))
            .unwrap();
        let project_config = config::load(&project.config_path).unwrap();
        let original_policy =
            resolve_project_policy(&policy, &project, &project_config).unwrap();
        let runner = AgentRunner::new(
            AgentRunnerConfig::production()
                .with_codex_capabilities(pueue_agent::codex_command::CodexCapabilities::all()),
            Arc::clone(&policy),
        );

        let campaign_id = "promotion-code-campaign".to_owned();
        let baseline_experiment_id = "promotion-code-baseline".to_owned();
        let baseline_proposal_id = "promotion-code-baseline-proposal";
        let baseline_submission_id = "promotion-code-baseline-submission";
        let objective = ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "promotion-code-objective-digest".to_owned(),
        };
        let argv = vec!["python".to_owned(), "train.py".to_owned()];
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish baseline".to_owned(),
                source_experiment_id: None,
                argv: argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        let limits = CampaignLimits::default();
        CampaignRepository::new(&db)
            .start_with_baseline_at_revision(
                StartCampaignRequest {
                    campaign_id: &campaign_id,
                    project_id: "promotion-project",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &baseline,
                    submission_id: baseline_submission_id,
                    experiment_id: &baseline_experiment_id,
                    proposal_id: baseline_proposal_id,
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: Some(&minimize_metric()),
                    now: 100,
                },
                &limits,
                Some(&base_sha),
            )
            .unwrap();
        db.connect()
            .unwrap()
            .execute(
                "UPDATE experiments SET status = 'succeeded', finished_at = 110
                 WHERE experiment_id = ?1",
                [baseline_experiment_id.as_str()],
            )
            .unwrap();
        MetricsRepository::upsert(
            &db,
            &ExperimentMetricsRow {
                experiment_id: baseline_experiment_id.clone(),
                source: "manifest".to_owned(),
                primary_metric_name: Some("loss".to_owned()),
                primary_metric_value: Some(1.0),
                metrics_json: "{}".to_owned(),
                artifact_defect: None,
                created_at: 110,
                updated_at: 110,
                evaluated_at: None,
            },
        )
        .unwrap();

        let proposal_id = "promotion-code-proposal".to_owned();
        let run_id = "promotion-code-run".to_owned();
        let experiment_id = "promotion-code-experiment".to_owned();
        let submission_id = "promotion-code-submission".to_owned();
        let code_proposal = proposals::validate(
            ProposalInput {
                kind: ProposalKind::CodeChange,
                hypothesis: "Improve the implementation".to_owned(),
                source_experiment_id: Some(baseline_experiment_id.clone()),
                argv,
                working_directory: ".".to_owned(),
                expected_evidence: Vec::new(),
            },
            &objective.digest,
        )
        .unwrap();
        let candidate_ref_name = candidate_ref(&campaign_id, &proposal_id).unwrap();
        let best_ref_name = best_ref(&campaign_id).unwrap();
        let new_run = NewCodeChangeRun::new(
            &run_id,
            &proposal_id,
            &campaign_id,
            &base_sha,
            &candidate_ref_name,
            &best_ref_name,
            &run_id,
            format!(".pueue-agent/worktrees/{campaign_id}/{proposal_id}"),
            120,
        );
        CampaignRepository::new(&db)
            .accept_code_change_proposal(
                &campaign_id,
                &proposal_id,
                &experiment_id,
                &submission_id,
                &code_proposal,
                &limits,
                120,
                Some(&new_run),
                None,
            )
            .unwrap();

        let repository = CodeChangeRepository::new(&db);
        repository
            .transition(
                &run_id,
                CodeChangeState::Reserved,
                CodeChangeState::PreparingWorktree,
                121,
            )
            .unwrap();
        let mut candidate = prepare_code_change_worktree_for_run(
            &policy,
            &project,
            &original_policy,
            &db,
            &run_id,
        )
        .await
        .unwrap();
        repository
            .transition(
                &run_id,
                CodeChangeState::PreparingWorktree,
                CodeChangeState::Editing,
                122,
        )
        .unwrap();
        repository
            .transition(
                &run_id,
                CodeChangeState::Editing,
                CodeChangeState::Checking,
                123,
            )
            .unwrap();
        fs::write(candidate.path().join("base.txt"), b"candidate\n").unwrap();
        let facts = candidate.verify().await.unwrap();
        repository
            .record_checked_diff(
                &run_id,
                facts.persisted_digest(),
                facts.file_count as i64,
                facts.diff_bytes as i64,
                124,
            )
            .unwrap();
        repository
            .transition(
                &run_id,
                CodeChangeState::Checking,
                CodeChangeState::Committing,
                125,
            )
            .unwrap();
        let candidate_sha = candidate.commit().await.unwrap();
        repository
            .record_candidate(
                &run_id,
                &candidate_sha,
                facts.persisted_digest(),
                facts.file_count as i64,
                facts.diff_bytes as i64,
                126,
            )
            .unwrap();
        let candidate_path = candidate.path().to_owned();
        let runtime_root = candidate_path.join(".pueue-agent");
        let results = runtime_root.join("results");
        let artifacts = runtime_root.join("artifacts");
        let artifact_run = artifacts.join(&experiment_id);
        for directory in [&runtime_root, &results, &artifacts, &artifact_run] {
            fs::create_dir_all(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let manifest = results.join(format!("{experiment_id}.json"));
        fs::write(
            &manifest,
            format!(
                r#"{{"schema_version":1,"experiment_id":"{experiment_id}","metrics":{{"loss":0.5}}}}"#
            ),
        )
        .unwrap();
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600)).unwrap();
        repository
            .transition(
                &run_id,
                CodeChangeState::Committing,
                CodeChangeState::CandidateReady,
                127,
            )
            .unwrap();
        let candidate_working_directory_identity =
            VerifiedWorkingDirectory::root(candidate.root()).unwrap().identity();
        drop(candidate);

        CampaignRepository::new(&db)
            .accept_code_change_candidate(
                &run_id,
                &experiment_id,
                &submission_id,
                128,
                &limits,
            )
            .unwrap()
            .accepted()
            .unwrap();
        repository
            .record_candidate_working_directory_identity(
                &run_id,
                &experiment_id,
                candidate_working_directory_identity,
                128,
            )
            .unwrap();
        let experiments = ExperimentRepository::new(&db);
        experiments.mark_submitting(&experiment_id, 129).unwrap();
        let terminal_task = PueueTask {
            id: 42,
            group: "promotion-project".to_owned(),
            command: "python train.py".to_owned(),
            state: "Done".to_owned(),
            enqueued_at: Some("129".to_owned()),
            started_at: Some("130".to_owned()),
            ended_at: Some("131".to_owned()),
            result: Some(json!({"Success": 0})),
        };
        let task_signature = managed_task_run_signature(&terminal_task).unwrap();
        experiments
            .mark_accepted(&experiment_id, 42, &task_signature, 130)
            .unwrap();
        if terminal_submission {
            Reconciler::new(
                &db,
                FixturePueue {
                    task: PueueTask {
                        state: "Running".to_owned(),
                        ended_at: None,
                        result: None,
                        ..terminal_task.clone()
                    },
                },
            )
            .with_execution_policy(Arc::clone(&policy))
            .run_once_at(130)
            .await
            .unwrap();
            Reconciler::new(&db, FixturePueue { task: terminal_task })
                .with_execution_policy(Arc::clone(&policy))
                .run_once_at(131)
                .await
                .unwrap();
            if persist_origin {
                let origin_event = EventRepository::new(&db)
                    .insert_idempotent(
                        &NewEvent::new(
                            "promotion-project",
                            EventKind::CodeChange,
                            format!("cleanup-origin:{experiment_id}"),
                            json!({"experiment_id": experiment_id, "run_id": run_id}),
                            131,
                            131,
                        )
                        .with_campaign_lineage(campaign_id.clone(), Some(experiment_id.clone())),
                    )
                    .unwrap();
                let origin_run = AgentRunRepository::new(&db)
                    .insert(&NewAgentRun::new(
                        "promotion-project",
                        origin_event.event_id,
                        None,
                        AgentRunStatus::Completed,
                        131,
                        fixture_root.join("cleanup-origin.log"),
                    ))
                    .unwrap();
                db.connect()
                    .unwrap()
                    .execute(
                        "UPDATE submissions SET origin_agent_run_id = ?1
                         WHERE submission_id = ?2",
                        params![origin_run.run_id, submission_id],
                    )
                    .unwrap();
            }
        }
        db.connect()
            .unwrap()
            .execute(
                "UPDATE campaigns
                 SET current_best_experiment_id = ?1, plateau_count = 2
                 WHERE campaign_id = ?2",
                params![baseline_experiment_id, campaign_id],
            )
            .unwrap();
        MetricsRepository::upsert(
            &db,
            &ExperimentMetricsRow {
                experiment_id: experiment_id.clone(),
                source: "manifest".to_owned(),
                primary_metric_name: Some("loss".to_owned()),
                primary_metric_value: Some(0.5),
                metrics_json: "{}".to_owned(),
                artifact_defect: None,
                created_at: 132,
                updated_at: 132,
                evaluated_at: None,
            },
        )
        .unwrap();
        git_update_ref(&project_root, &best_ref_name, &base_sha, None);

        Self {
            _temp: temp,
            db,
            policy,
            runner,
            project_root,
            candidate_path,
            run_id,
            campaign_id,
            proposal_id,
            baseline_experiment_id,
            experiment_id,
            base_sha,
            candidate_sha,
            best_ref: best_ref_name,
        }
    }

    fn coordinator(&self) -> CodeChangeCoordinator<'_> {
        self.coordinator_with_limits(CampaignLimits::default())
    }

    fn coordinator_with_limits(&self, limits: CampaignLimits) -> CodeChangeCoordinator<'_> {
        CodeChangeCoordinator::new(
            &self.db,
            &self.runner,
            &self.policy,
            limits,
        )
    }

    fn candidate_path(&self) -> PathBuf {
        self.candidate_path.clone()
    }

    fn read_named_ref(&self, reference: &str) -> Option<String> {
        let output = Command::new("/usr/bin/git")
            .args(["rev-parse", "--verify", &format!("{reference}^{{commit}}")])
            .current_dir(&self.project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        output.status.success().then(|| {
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .to_owned()
        })
    }

    async fn new_pre_candidate_rejected() -> Self {
        let fixture = Self::new().await;
        let candidate_path = fixture.candidate_path();
        let output = Command::new("/usr/bin/git")
            .args(["worktree", "remove", "--force", candidate_path.to_str().unwrap()])
            .current_dir(&fixture.project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git worktree remove: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let candidate_ref_name = candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap();
        git_delete_ref(&fixture.project_root, &candidate_ref_name);
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE code_change_runs
                 SET state = 'rejected', candidate_sha = NULL, diff_digest = NULL,
                     changed_file_count = NULL, diff_bytes = NULL, experiment_id = NULL,
                     rejection_code = 'cannot_apply', rejection_summary = 'pre-candidate rejection',
                     promotion_outcome = NULL,
                     promotion_expected_best_experiment_id = NULL,
                     promotion_expected_old_sha = NULL, promotion_target_sha = NULL,
                     cleanup_completed_at = NULL, state_root_identity = NULL,
                     worktrees_identity = NULL, campaign_identity = NULL,
                     candidate_root_identity = NULL, candidate_admin_identity = NULL,
                     candidate_common_identity = NULL, candidate_admin_path = NULL,
                     candidate_common_path = NULL, protected_ref_digest = NULL,
                     candidate_working_directory_identity = NULL, remote_config_digest = NULL,
                     updated_at = 140
                 WHERE code_change_run_id = ?1",
                params![&fixture.run_id],
            )
            .unwrap();
        fixture
    }

    async fn new_rejected_with_owned_candidate() -> Self {
        let fixture = Self::new().await;
        let candidate_path = fixture.candidate_path();
        let reset = Command::new("/usr/bin/git")
            .args(["reset", "--hard", &fixture.base_sha])
            .current_dir(&candidate_path)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        assert!(
            reset.status.success(),
            "git reset candidate: {}",
            String::from_utf8_lossy(&reset.stderr)
        );
        let candidate_ref_name = candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap();
        git_delete_ref(&fixture.project_root, &candidate_ref_name);
        let editor_event = EventRepository::new(&fixture.db)
            .insert_idempotent(&NewEvent::new(
                "promotion-project",
                EventKind::CodeChange,
                "cleanup-precommit-editor",
                json!({"code_change_run_id": fixture.run_id, "attempt": 1}),
                140,
                140,
            ))
            .unwrap();
        let editor_run = AgentRunRepository::new(&fixture.db)
            .insert(&NewAgentRun::new(
                "promotion-project",
                editor_event.event_id,
                None,
                AgentRunStatus::Completed,
                140,
                fixture._temp.path().join("cleanup-precommit-editor.log"),
            ))
            .unwrap();
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "UPDATE code_change_runs
                 SET state = 'rejected', candidate_sha = NULL, diff_digest = NULL,
                     changed_file_count = NULL, diff_bytes = NULL, experiment_id = NULL,
                     editor_attempts = 1, editor_session_id = 'cleanup-precommit-session',
                     rejection_code = 'candidate_rejected',
                     rejection_summary = 'candidate rejected after preparation',
                     promotion_outcome = NULL,
                     promotion_expected_best_experiment_id = NULL,
                     promotion_expected_old_sha = NULL, promotion_target_sha = NULL,
                     cleanup_completed_at = NULL, updated_at = 140
                 WHERE code_change_run_id = ?1",
                params![&fixture.run_id],
            )
            .unwrap();
        fixture
            .db
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO code_change_editor_attempts (
                     code_change_run_id, attempt, agent_run_id, editor_session_id,
                     status, started_at, finished_at
                 ) VALUES (?1, 1, ?2, 'cleanup-precommit-session', 'failed', 140, 141)",
                params![&fixture.run_id, editor_run.run_id],
            )
            .unwrap();
        fixture
    }

    fn read_ref(&self) -> Option<String> {
        let output = Command::new("/usr/bin/git")
            .args(["rev-parse", "--verify", &format!("refs/heads/{}^{{commit}}", self.best_ref)])
            .current_dir(&self.project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        output.status.success().then(|| {
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .to_owned()
        })
    }

    fn campaign_promotion_state(&self) -> (Option<String>, i64) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT current_best_experiment_id, plateau_count
                 FROM campaigns WHERE campaign_id = ?1",
                [&self.campaign_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn strategy_refresh_wakes(&self) -> Vec<(String, String)> {
        let connection = self.db.connect().unwrap();
        let mut statement = connection
            .prepare(
                "SELECT dedup_key, status FROM events
                 WHERE project_id = 'promotion-project'
                   AND kind = 'operator_wake'
                   AND dedup_key LIKE 'strategy-refresh:v1:%'
                 ORDER BY created_at, event_id",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    fn ref_log_lines(&self) -> usize {
        let output = Command::new("/usr/bin/git")
            .args(["reflog", "show", &format!("refs/heads/{}", self.best_ref)])
            .current_dir(&self.project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap();
        if !output.status.success() {
            return 0;
        }
        String::from_utf8(output.stdout).unwrap().lines().count()
    }
}

#[cfg(all(unix, target_os = "linux"))]
fn git_update_ref(root: &Path, reference: &str, new_sha: &str, old_sha: Option<&str>) {
    let old = old_sha.unwrap_or("0000000000000000000000000000000000000000");
    let output = Command::new("/usr/bin/git")
        .args(["update-ref", "--create-reflog", &format!("refs/heads/{reference}"), new_sha, old])
        .current_dir(root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git update-ref {reference}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(all(unix, target_os = "linux"))]
fn git_delete_ref(root: &Path, reference: &str) {
    let output = Command::new("/usr/bin/git")
        .args(["update-ref", "-d", &format!("refs/heads/{reference}")])
        .current_dir(root)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git delete-ref {reference}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(all(unix, target_os = "linux"))]
fn code_change_promotion_intent(
    fixture: &CodeChangePromotionFixture,
    expected_old_sha: Option<&str>,
) {
    CodeChangeRepository::new(&fixture.db)
        .prepare_code_promotion(
            &fixture.run_id,
            ExperimentStatus::Succeeded,
            &CampaignLimits::default(),
            expected_old_sha,
            140,
        )
        .unwrap();
}

#[cfg(all(unix, target_os = "linux", debug_assertions))]
#[tokio::test]
async fn code_change_cas_failure_keeps_database_best() {
    let fixture = CodeChangePromotionFixture::new().await;
    code_change_promotion_intent(&fixture, Some(&fixture.base_sha));
    let conflicting_sha = fixture.candidate_sha.clone();
    let conflict_object = {
        let output = Command::new("/usr/bin/git")
            .args([
                "commit-tree",
                &format!("{}^{{tree}}", fixture.base_sha),
                "-p",
                &fixture.base_sha,
                "-m",
                "unrelated best ref",
            ])
            .current_dir(&fixture.project_root)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_AUTHOR_NAME", "fixture")
            .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
            .env("GIT_COMMITTER_NAME", "fixture")
            .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
            .output()
            .unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    };
    assert_ne!(conflicting_sha, conflict_object);
    let before_cas_ref_mutations = fixture.ref_log_lines();

    let coordinator = fixture
        .coordinator()
        .with_pre_cas_best_ref_swap_for_test(conflict_object.clone());
    let report = coordinator.advance_ready(141, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.rejected, 1);
    assert_eq!(coordinator.pre_cas_update_ref_attempts_for_test(), 1);
    assert_eq!(fixture.ref_log_lines(), before_cas_ref_mutations + 1);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, CodeChangeState::RecoveryRequired);
    assert_eq!(
        run.rejection_code.as_deref(),
        Some("promotion_best_ref_conflict")
    );
    assert_eq!(
        fixture.campaign_promotion_state().0,
        Some(fixture.baseline_experiment_id.clone())
    );
    assert_eq!(fixture.read_ref(), Some(conflict_object));
    assert_ne!(fixture.read_ref(), Some(fixture.candidate_sha.clone()));
    assert_eq!(
        MetricsRepository::get(&fixture.db, &fixture.experiment_id)
            .unwrap()
            .unwrap()
            .evaluated_at,
        None
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_nonterminal_submission_defers_without_projection() {
    let fixture = CodeChangePromotionFixture::new_nonterminal().await;
    assert_eq!(
        ExperimentRepository::new(&fixture.db)
            .find_by_id(&fixture.experiment_id)
            .unwrap()
            .unwrap()
            .status,
        ExperimentStatus::Accepted
    );
    let before_run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    let before_campaign = fixture.campaign_promotion_state();
    let before_ref = fixture.read_ref();
    let before_evaluated_at = MetricsRepository::get(&fixture.db, &fixture.experiment_id)
        .unwrap()
        .unwrap()
        .evaluated_at;
    let report = fixture.coordinator().advance_ready(141, 10).await.unwrap();
    assert_eq!(report.started.len(), 0);
    assert_eq!(report.advanced, 0);
    assert_eq!(report.rejected, 0);
    assert_eq!(report.deferred, 1);
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run, before_run);
    assert_eq!(fixture.campaign_promotion_state(), before_campaign);
    assert_eq!(fixture.read_ref(), before_ref);
    assert_eq!(
        MetricsRepository::get(&fixture.db, &fixture.experiment_id)
            .unwrap()
            .unwrap()
            .evaluated_at,
        before_evaluated_at
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_restart_after_ref_cas_finishes_database_projection() {
    let fixture = CodeChangePromotionFixture::new().await;
    code_change_promotion_intent(&fixture, Some(&fixture.base_sha));
    let before_recovery_ref_mutations = fixture.ref_log_lines();
    git_update_ref(
        &fixture.project_root,
        &fixture.best_ref,
        &fixture.candidate_sha,
        Some(&fixture.base_sha),
    );
    let after_simulated_cas_ref_mutations = fixture.ref_log_lines();
    assert!(after_simulated_cas_ref_mutations > before_recovery_ref_mutations);

    let report = fixture
        .coordinator()
        .recover_interrupted(141, 10)
        .await
        .unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(fixture.read_ref(), Some(fixture.candidate_sha.clone()));
    assert_eq!(
        fixture.campaign_promotion_state().0,
        Some(fixture.experiment_id.clone())
    );
    let metrics = MetricsRepository::get(&fixture.db, &fixture.experiment_id)
        .unwrap()
        .unwrap();
    assert!(metrics.evaluated_at.is_some());
    let events_before_retry: i64 = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE dedup_key LIKE 'code-change:v1:%:evaluated:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let report = fixture
        .coordinator()
        .recover_interrupted(142, 10)
        .await
        .unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(fixture.ref_log_lines(), after_simulated_cas_ref_mutations);
    let metrics_after_retry = MetricsRepository::get(&fixture.db, &fixture.experiment_id)
        .unwrap()
        .unwrap();
    assert_eq!(metrics_after_retry.evaluated_at, metrics.evaluated_at);
    let events_after_retry: i64 = fixture
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE dedup_key LIKE 'code-change:v1:%:evaluated:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(events_after_retry, events_before_retry);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_non_improvement_uses_configured_plateau_threshold() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET plateau_count = 1 WHERE campaign_id = ?1",
            [&fixture.campaign_id],
        )
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE experiment_metrics
             SET primary_metric_value = 2.0
             WHERE experiment_id = ?1",
            [&fixture.experiment_id],
        )
        .unwrap();
    let limits = CampaignLimits {
        plateau_threshold: 2,
        ..CampaignLimits::default()
    };

    let report = fixture
        .coordinator_with_limits(limits)
        .advance_ready(141, 10)
        .await
        .unwrap();

    assert_eq!(report.advanced, 1);
    assert_eq!(fixture.campaign_promotion_state().1, 2);
    assert_eq!(
        fixture.strategy_refresh_wakes(),
        vec![(
            format!("strategy-refresh:v1:{}:1", fixture.campaign_id),
            "pending".to_owned()
        )]
    );
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_evaluated_cleanup_removes_owned_worktree_and_preserves_refs_and_metadata() {
    let fixture = CodeChangePromotionFixture::new().await;
    let coordinator = fixture.coordinator();
    let first = coordinator.advance_ready(141, 10).await.unwrap();
    assert_eq!(first.rejected, 0);
    assert_eq!(first.advanced, 1);
    let repository = CodeChangeRepository::new(&fixture.db);
    let pending = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(pending.state, CodeChangeState::CleanupPending);
    assert!(pending.cleanup_completed_at.is_none());
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.is_dir());
    let candidate_ref_name = format!(
        "refs/heads/{}",
        candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap()
    );
    let main_before_cleanup = fixture.read_named_ref("refs/heads/main");
    let best_before_cleanup = fixture.read_ref();
    let candidate_before_cleanup = fixture.read_named_ref(&candidate_ref_name);
    assert_eq!(candidate_before_cleanup, Some(fixture.candidate_sha.clone()));

    let second = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(second.rejected, 0);
    assert_eq!(second.deferred, 0);
    assert_eq!(second.advanced, 1);
    assert!(!candidate_path.exists());
    assert_eq!(fixture.read_named_ref("refs/heads/main"), main_before_cleanup);
    assert_eq!(fixture.read_ref(), best_before_cleanup);
    assert_eq!(fixture.read_named_ref(&candidate_ref_name), candidate_before_cleanup);
    let completed = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(completed.state, CodeChangeState::Completed);
    assert!(completed.cleanup_completed_at.is_some());
    assert_eq!(completed.candidate_sha, pending.candidate_sha);
    assert_eq!(completed.candidate_ref, pending.candidate_ref);
    assert_eq!(completed.best_ref, pending.best_ref);
    assert_eq!(completed.worktree_id, pending.worktree_id);
    assert_eq!(completed.worktree_relative_path, pending.worktree_relative_path);
    assert_eq!(completed.diff_digest, pending.diff_digest);
    assert_eq!(completed.changed_file_count, pending.changed_file_count);
    assert_eq!(completed.diff_bytes, pending.diff_bytes);
    assert_eq!(completed.state_root_identity, pending.state_root_identity);
    assert_eq!(completed.candidate_admin_identity, pending.candidate_admin_identity);
    assert_eq!(completed.candidate_common_identity, pending.candidate_common_identity);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_evaluated_cleanup_accepts_normal_originless_candidate_submission() {
    let fixture = CodeChangePromotionFixture::new_without_origin().await;
    let coordinator = fixture.coordinator();
    let first = coordinator.advance_ready(141, 10).await.unwrap();
    assert_eq!(first.rejected, 0);
    assert_eq!(first.advanced, 1);
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.is_dir());
    let pending = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(pending.state, CodeChangeState::CleanupPending);

    let second = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(second.rejected, 0);
    assert_eq!(second.deferred, 0);
    assert_eq!(second.advanced, 1);
    assert!(!candidate_path.exists());
    let completed = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, CodeChangeState::Completed);
    assert!(completed.cleanup_completed_at.is_some());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_next_candidate_uses_promoted_best_and_preserves_original_base() {
    let fixture = CodeChangePromotionFixture::new().await;
    let first = fixture.coordinator().advance_ready(141, 10).await.unwrap();
    assert_eq!(first.rejected, 0);
    assert_eq!(first.advanced, 1);
    let second = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(second.rejected, 0);
    assert_eq!(second.advanced, 1);
    assert_eq!(fixture.read_ref(), Some(fixture.candidate_sha.clone()));
    assert_eq!(fixture.read_named_ref("refs/heads/main"), Some(fixture.base_sha.clone()));

    let proposal_id = "promotion-code-next-proposal".to_owned();
    let run_id = "promotion-code-next-run".to_owned();
    let experiment_id = "promotion-code-next-experiment".to_owned();
    let submission_id = "promotion-code-next-submission".to_owned();
    let proposal = proposals::validate(
        ProposalInput {
            kind: ProposalKind::CodeChange,
            hypothesis: "Improve the promoted implementation".to_owned(),
            source_experiment_id: Some(fixture.experiment_id.clone()),
            argv: vec!["python".to_owned(), "train.py".to_owned()],
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        "promotion-code-objective-digest",
    )
    .unwrap();
    let candidate_ref_name = candidate_ref(&fixture.campaign_id, &proposal_id).unwrap();
    let run = NewCodeChangeRun::new(
        &run_id,
        &proposal_id,
        &fixture.campaign_id,
        &fixture.candidate_sha,
        &candidate_ref_name,
        &fixture.best_ref,
        &run_id,
        format!(".pueue-agent/worktrees/{}/{proposal_id}", fixture.campaign_id),
        150,
    );
    CampaignRepository::new(&fixture.db)
        .accept_code_change_proposal(
            &fixture.campaign_id,
            &proposal_id,
            &experiment_id,
            &submission_id,
            &proposal,
            &CampaignLimits::default(),
            150,
            Some(&run),
            None,
        )
        .unwrap();
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .transition(
            &run_id,
            CodeChangeState::Reserved,
            CodeChangeState::PreparingWorktree,
            151,
        )
        .unwrap();

    let project = ProjectRepository::new(&fixture.db)
        .find_by_id("promotion-project")
        .unwrap()
        .unwrap();
    let project_config = config::load(&project.config_path).unwrap();
    let original_policy = resolve_project_policy(&fixture.policy, &project, &project_config)
        .unwrap();
    let candidate = prepare_code_change_worktree_for_run(
        &fixture.policy,
        &project,
        &original_policy,
        &fixture.db,
        &run_id,
    )
    .await
    .unwrap();
    let candidate_head = String::from_utf8(
        Command::new("/usr/bin/git")
            .args(["rev-parse", "HEAD^{commit}"])
            .current_dir(candidate.path())
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    assert_eq!(candidate_head, fixture.candidate_sha);
    assert_eq!(fixture.read_named_ref("refs/heads/main"), Some(fixture.base_sha.clone()));
    assert_eq!(repository.find_by_id(&run_id).unwrap().unwrap().base_sha, fixture.candidate_sha);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_check_replay_ambiguity_is_persisted_as_recovery() {
    let fixture = CodeChangePromotionFixture::new().await;
    let candidate_path = fixture.candidate_path();
    let reset = Command::new("/usr/bin/git")
        .args(["reset", "--hard", &fixture.base_sha])
        .current_dir(&candidate_path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        reset.status.success(),
        "git reset candidate: {}",
        String::from_utf8_lossy(&reset.stderr)
    );
    let candidate_ref_name = candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap();
    git_delete_ref(&fixture.project_root, &candidate_ref_name);
    fs::write(candidate_path.join("base.txt"), b"changed during replay\n").unwrap();

    let event = EventRepository::new(&fixture.db)
        .insert_idempotent(&NewEvent::new(
            "promotion-project",
            EventKind::CodeChange,
            "replay-ambiguous-editor",
            json!({"code_change_run_id": fixture.run_id, "attempt": 1}),
            140,
            140,
        ))
        .unwrap();
    let editor_run = AgentRunRepository::new(&fixture.db)
        .insert(&NewAgentRun::new(
            "promotion-project",
            event.event_id,
            None,
            AgentRunStatus::Completed,
            140,
            fixture._temp.path().join("replay-ambiguous-editor.log"),
        ))
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE code_change_runs
             SET state = 'checking', candidate_sha = NULL,
                 diff_digest = ?1, changed_file_count = 1, diff_bytes = 1,
                 editor_attempts = 1, editor_session_id = 'replay-session',
                 updated_at = 140
             WHERE code_change_run_id = ?2",
            params!["a".repeat(64), &fixture.run_id],
        )
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO code_change_editor_attempts (
                 code_change_run_id, attempt, agent_run_id, editor_session_id,
                 status, result_digest, started_at, finished_at
             ) VALUES (?1, 1, ?2, 'replay-session', 'ready', 'editor-ready', 140, 140)",
            params![&fixture.run_id, editor_run.run_id],
        )
        .unwrap();
    let repository = CodeChangeRepository::new(&fixture.db);
    repository
        .replace_attempt_checks(
            &fixture.run_id,
            1,
            &[NewCodeChangeCheck::new(
                1,
                0,
                "supervisor",
                vec!["git".to_owned(), "diff".to_owned()],
                ".",
            )],
            140,
        )
        .unwrap();

    let report = fixture.coordinator().advance_ready(141, 10).await.unwrap();
    assert_eq!(report.rejected, 1);
    let recovered = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(recovered.state, CodeChangeState::RecoveryRequired);
    assert_eq!(
        recovered.rejection_code.as_deref(),
        Some("check_round_recovery_required")
    );
    assert!(candidate_path.exists());

    let replay = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(replay.advanced, 0);
    assert_eq!(replay.rejected, 0);
    assert_eq!(replay.deferred, 0);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_pre_candidate_rejected_cleanup_uses_authority_and_retains_refs() {
    let fixture = CodeChangePromotionFixture::new_pre_candidate_rejected().await;
    let candidate_path = fixture.candidate_path();
    assert!(!candidate_path.exists());
    let candidate_ref_name = format!(
        "refs/heads/{}",
        candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap()
    );
    assert_eq!(fixture.read_named_ref(&candidate_ref_name), None);
    let main_before_cleanup = fixture.read_named_ref("refs/heads/main");
    let best_before_cleanup = fixture.read_ref();

    let report = fixture.coordinator().advance_ready(141, 10).await.unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(report.advanced, 1);
    let completed = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, CodeChangeState::Rejected);
    assert!(completed.cleanup_completed_at.is_some());
    assert_eq!(fixture.read_named_ref("refs/heads/main"), main_before_cleanup);
    assert_eq!(fixture.read_ref(), best_before_cleanup);
    assert_eq!(fixture.read_named_ref(&candidate_ref_name), None);
    assert!(!candidate_path.exists());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_rejected_cleanup_removes_owned_worktree_and_retains_candidate_ref() {
    let fixture = CodeChangePromotionFixture::new_rejected_with_owned_candidate().await;
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.exists());
    let candidate_ref_name = format!(
        "refs/heads/{}",
        candidate_ref(&fixture.campaign_id, &fixture.proposal_id).unwrap()
    );
    assert_eq!(fixture.read_named_ref(&candidate_ref_name), None);
    let main_before_cleanup = fixture.read_named_ref("refs/heads/main");
    let best_before_cleanup = fixture.read_ref();

    let report = fixture.coordinator().advance_ready(141, 10).await.unwrap();
    assert_eq!(report.rejected, 0);
    assert_eq!(report.deferred, 0);
    assert_eq!(report.advanced, 1);
    let completed = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, CodeChangeState::Rejected);
    assert!(completed.cleanup_completed_at.is_some());
    assert!(!candidate_path.exists());
    assert_eq!(fixture.read_named_ref("refs/heads/main"), main_before_cleanup);
    assert_eq!(fixture.read_ref(), best_before_cleanup);
    assert_eq!(fixture.read_named_ref(&candidate_ref_name), None);
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_path_replacement_fails_closed_without_mutating_replacement() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let repository = CodeChangeRepository::new(&fixture.db);
    let pending = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert_eq!(pending.state, CodeChangeState::CleanupPending);
    let candidate_path = fixture.candidate_path();
    let retained_path = candidate_path.with_extension("retained");
    fs::rename(&candidate_path, &retained_path).unwrap();
    fs::create_dir(&candidate_path).unwrap();
    fs::set_permissions(&candidate_path, fs::Permissions::from_mode(0o700)).unwrap();
    let replacement_marker = candidate_path.join("replacement-marker");
    fs::write(&replacement_marker, b"replacement must survive").unwrap();
    let main_before = fixture.read_named_ref("refs/heads/main");
    let best_before = fixture.read_ref();

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.rejected + report.deferred, 1);
    assert_eq!(fs::read(&replacement_marker).unwrap(), b"replacement must survive");
    assert!(retained_path.exists());
    assert_eq!(fixture.read_named_ref("refs/heads/main"), main_before);
    assert_eq!(fixture.read_ref(), best_before);
    let after = repository.find_by_id(&fixture.run_id).unwrap().unwrap();
    assert!(matches!(after.state, CodeChangeState::CleanupPending | CodeChangeState::RecoveryRequired));
    assert!(after.cleanup_completed_at.is_none());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_while_exact_editor_process_is_live() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let event = EventRepository::new(&fixture.db)
        .insert_idempotent(&NewEvent::new(
            "promotion-project",
            EventKind::CodeChange,
            "cleanup-live-editor",
            json!({"code_change_run_id": "promotion-code-run", "attempt": 1}),
            140,
            140,
        ))
        .unwrap();
    let agent_run = AgentRunRepository::new(&fixture.db)
        .insert(&NewAgentRun::new(
            "promotion-project",
            event.event_id,
            Some(std::process::id() as i64),
            AgentRunStatus::Running,
            140,
            fixture._temp.path().join("cleanup-live-editor.log"),
        ))
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO code_change_editor_attempts (
                 code_change_run_id, attempt, agent_run_id, editor_session_id,
                 status, started_at
             ) VALUES (?1, 1, ?2, 'cleanup-live-session', 'running', 140)",
            params![&fixture.run_id, agent_run.run_id],
        )
        .unwrap();
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.exists());
    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(candidate_path.exists());
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, CodeChangeState::CleanupPending);
    assert!(run.cleanup_completed_at.is_none());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_without_terminal_task_observation() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "DELETE FROM task_observations
             WHERE project_id = 'promotion-project' AND pueue_task_id = 42",
            [],
        )
        .unwrap();
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.exists());

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(candidate_path.exists());
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, CodeChangeState::CleanupPending);
    assert!(run.cleanup_completed_at.is_none());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_nonterminal_task_observation() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE task_observations SET state = 'queued'
             WHERE project_id = 'promotion-project' AND pueue_task_id = 42",
            [],
        )
        .unwrap();
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.exists());

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(candidate_path.exists());
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, CodeChangeState::CleanupPending);
    assert!(run.cleanup_completed_at.is_none());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_reused_task_identity_history() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let reused_task = pueue_agent::pueue::PueueTask {
        id: 42,
        group: "promotion-project".to_owned(),
        command: "python other.py".to_owned(),
        state: "Done".to_owned(),
        enqueued_at: Some("999".to_owned()),
        started_at: Some("1000".to_owned()),
        ended_at: Some("1001".to_owned()),
        result: Some(json!({"Success": 0})),
    };
    TaskObservationRepository::new(&fixture.db)
        .upsert(&NewTaskObservation::new(
            "promotion-project",
            task_signature(&reused_task),
            reused_task.id,
            reused_task.group.clone(),
            vec![reused_task.command.clone()],
            reused_task.state.clone(),
            Some(999),
            Some(1000),
            Some(1001),
            Some(r#"{"Success":0}"#.to_owned()),
            140,
        ))
        .unwrap();

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(fixture.candidate_path().exists());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_latest_live_task_observation() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let live_task = pueue_agent::pueue::PueueTask {
        id: 42,
        group: "promotion-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("129".to_owned()),
        started_at: Some("130".to_owned()),
        ended_at: None,
        result: None,
    };
    TaskObservationRepository::new(&fixture.db)
        .upsert(&NewTaskObservation::new(
            "promotion-project",
            task_signature(&live_task),
            live_task.id,
            live_task.group.clone(),
            vec![live_task.command.clone()],
            live_task.state.clone(),
            Some(129),
            Some(130),
            None,
            None,
            140,
        ))
        .unwrap();

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(fixture.candidate_path().exists());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_tied_live_task_observation() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let live_task = pueue_agent::pueue::PueueTask {
        id: 42,
        group: "promotion-project".to_owned(),
        command: "python train.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("129".to_owned()),
        started_at: Some("130".to_owned()),
        ended_at: None,
        result: None,
    };
    TaskObservationRepository::new(&fixture.db)
        .upsert(&NewTaskObservation::new(
            "promotion-project",
            task_signature(&live_task),
            live_task.id,
            live_task.group.clone(),
            vec![live_task.command.clone()],
            live_task.state.clone(),
            Some(129),
            Some(130),
            None,
            None,
            131,
        ))
        .unwrap();

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(fixture.candidate_path().exists());
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_corrupt_task_observation_fields() {
    for corruption in ["malformed_key", "stored_timestamp", "command", "enqueue"] {
        let fixture = CodeChangePromotionFixture::new().await;
        fixture.coordinator().advance_ready(141, 10).await.unwrap();
        match corruption {
            "malformed_key" => {
                fixture
                    .db
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE task_observations
                         SET task_signature = 'pueue-task:v1:not-json'
                         WHERE project_id = 'promotion-project'
                           AND pueue_task_id = 42
                           AND state = 'Done'",
                        [],
                    )
                    .unwrap();
            }
            "stored_timestamp" => {
                fixture
                    .db
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE task_observations SET ended_at = 999
                         WHERE project_id = 'promotion-project'
                           AND pueue_task_id = 42
                           AND state = 'Done'",
                        [],
                    )
                    .unwrap();
            }
            "command" => {
                fixture
                    .db
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE task_observations SET command_json = '[\"python other.py\"]'
                         WHERE project_id = 'promotion-project'
                           AND pueue_task_id = 42
                           AND state = 'Done'",
                        [],
                    )
                    .unwrap();
            }
            "enqueue" => {
                let changed_task = PueueTask {
                    id: 42,
                    group: "promotion-project".to_owned(),
                    command: "python train.py".to_owned(),
                    state: "Done".to_owned(),
                    enqueued_at: Some("999".to_owned()),
                    started_at: Some("130".to_owned()),
                    ended_at: Some("131".to_owned()),
                    result: Some(json!({"Success": 0})),
                };
                fixture
                    .db
                    .connect()
                    .unwrap()
                    .execute(
                        "UPDATE task_observations
                         SET task_signature = ?1, enqueued_at = 999
                         WHERE project_id = 'promotion-project'
                           AND pueue_task_id = 42
                           AND state = 'Done'",
                        [task_signature(&changed_task)],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
        assert_eq!(report.advanced, 0, "corruption={corruption}");
        assert_eq!(report.deferred + report.rejected, 1, "corruption={corruption}");
        assert!(fixture.candidate_path().exists(), "corruption={corruption}");
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_accepts_complete_128_observation_history_and_rejects_truncation() {
    for extra_running_rows in [126, 127] {
        let fixture = CodeChangePromotionFixture::new().await;
        fixture.coordinator().advance_ready(141, 10).await.unwrap();
        for started_at in 0..extra_running_rows {
            let running_task = PueueTask {
                id: 42,
                group: "promotion-project".to_owned(),
                command: "python train.py".to_owned(),
                state: "Running".to_owned(),
                enqueued_at: Some("129".to_owned()),
                started_at: Some(started_at.to_string()),
                ended_at: None,
                result: None,
            };
            TaskObservationRepository::new(&fixture.db)
                .upsert(&NewTaskObservation::new(
                    "promotion-project",
                    task_signature(&running_task),
                    running_task.id,
                    running_task.group.clone(),
                    vec![running_task.command.clone()],
                    running_task.state.clone(),
                    Some(129),
                    Some(started_at),
                    None,
                    None,
                    started_at,
                ))
                .unwrap();
        }

        let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
        if extra_running_rows == 126 {
            assert_eq!(report.advanced, 1, "extra_running_rows={extra_running_rows}");
            assert_eq!(report.deferred + report.rejected, 0);
            assert!(!fixture.candidate_path().exists());
            let run = CodeChangeRepository::new(&fixture.db)
                .find_by_id(&fixture.run_id)
                .unwrap()
                .unwrap();
            assert_eq!(run.state, CodeChangeState::Completed);
            assert!(run.cleanup_completed_at.is_some());
        } else {
            assert_eq!(report.advanced, 0, "extra_running_rows={extra_running_rows}");
            assert_eq!(report.deferred + report.rejected, 1);
            assert!(fixture.candidate_path().exists());
        }
    }
}

#[cfg(all(unix, target_os = "linux"))]
#[tokio::test]
async fn code_change_cleanup_defers_for_dead_pid_running_editor() {
    let fixture = CodeChangePromotionFixture::new().await;
    fixture.coordinator().advance_ready(141, 10).await.unwrap();
    let event = EventRepository::new(&fixture.db)
        .insert_idempotent(&NewEvent::new(
            "promotion-project",
            EventKind::CodeChange,
            "cleanup-dead-editor",
            json!({"code_change_run_id": fixture.run_id, "attempt": 1}),
            140,
            140,
        ))
        .unwrap();
    let editor_run = AgentRunRepository::new(&fixture.db)
        .insert(&NewAgentRun::new(
            "promotion-project",
            event.event_id,
            Some(2_000_000_000),
            AgentRunStatus::Running,
            140,
            fixture._temp.path().join("cleanup-dead-editor.log"),
        ))
        .unwrap();
    fixture
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO code_change_editor_attempts (
                 code_change_run_id, attempt, agent_run_id, editor_session_id,
                 status, started_at
             ) VALUES (?1, 1, ?2, 'cleanup-dead-editor-session', 'running', 140)",
            params![&fixture.run_id, editor_run.run_id],
        )
        .unwrap();
    let candidate_path = fixture.candidate_path();
    assert!(candidate_path.exists());

    let report = fixture.coordinator().advance_ready(142, 10).await.unwrap();
    assert_eq!(report.advanced, 0);
    assert_eq!(report.deferred + report.rejected, 1);
    assert!(candidate_path.exists());
    let run = CodeChangeRepository::new(&fixture.db)
        .find_by_id(&fixture.run_id)
        .unwrap()
        .unwrap();
    assert_eq!(run.state, CodeChangeState::CleanupPending);
    assert!(run.cleanup_completed_at.is_none());
}

#[test]
fn non_improvement_increments_plateau_without_moving_the_best() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(2.0));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(harness.promotion_row(), (None, 1));
}

#[test]
fn exactly_at_delta_is_not_an_improvement() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.99));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 1)
    );
}

#[test]
fn metric_less_campaign_skips_evaluation() {
    let harness = Harness::new();
    harness.start_campaign(None);
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);
    assert_eq!(harness.promotion_row(), (None, 0));
    assert!(harness.evaluated_at(CHALLENGER_EXPERIMENT_ID).is_some());
}

#[test]
fn inactive_campaign_skips_evaluation() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_campaign_state("goal_reached_pending_review");

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::SkippedNoObjective);
    assert_eq!(harness.promotion_row(), (None, 0));
    assert!(harness.strategy_refresh_wakes().is_empty());
    assert!(harness.evaluated_at(CHALLENGER_EXPERIMENT_ID).is_some());
}

#[test]
fn baseline_first_establishes_and_anchors_the_comparison() {
    let harness = Harness::new();
    harness.start_campaign(Some(&maximize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(10.0));

    let baseline_outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        BASELINE_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        200,
    ).unwrap();

    assert_eq!(baseline_outcome, PromotionOutcome::BaselineEstablished);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 0)
    );

    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(12.0));

    let challenger_outcome =
        evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(challenger_outcome, PromotionOutcome::Improved);
    assert_eq!(
        harness.promotion_row(),
        (Some(CHALLENGER_EXPERIMENT_ID.to_owned()), 0)
    );
}

#[test]
fn succeeded_without_primary_metric_counts_toward_plateau() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, None);
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 1)
    );
}

#[test]
fn failed_and_cancelled_experiments_skip_evaluation() {
    for status in [ExperimentStatus::Failed, ExperimentStatus::Cancelled] {
        let harness = Harness::new();
        harness.start_campaign(Some(&minimize_metric()));
        harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
        harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
        harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
        harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 2);

        let outcome = evaluate(
            &harness.db,
            CAMPAIGN_ID,
            CHALLENGER_EXPERIMENT_ID,
            status,
            &CampaignLimits::default(),
            300,
        )
        .unwrap();

        assert_eq!(outcome, PromotionOutcome::SkippedNoMetric);
        assert_eq!(
            harness.promotion_row(),
            (Some(BASELINE_EXPERIMENT_ID.to_owned()), 2)
        );
    }
}

#[test]
fn three_consecutive_non_improvements_emit_exactly_one_strategy_refresh_wake() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..3 {
        let outcome = run_non_improvement(&harness, index, &limits);
        assert_eq!(outcome, PromotionOutcome::NotImproved);
    }

    let wake_key = format!("strategy-refresh:v1:{CAMPAIGN_ID}:1");
    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![(wake_key.clone(), "pending".to_owned())]
    );
    let (campaign_lineage, payload) = harness.wake_payload(&wake_key);
    assert_eq!(campaign_lineage.as_deref(), Some(CAMPAIGN_ID));
    assert_eq!(payload["source"], "promotion");
    assert_eq!(payload["reason"], "plateau_threshold_reached");
    assert_eq!(payload["plateau_count"], 3);
    assert_eq!(payload["round"], 1);
    assert_eq!(
        harness.promotion_row(),
        (None, 3),
        "the campaign stays active and keeps counting"
    );
}

#[test]
fn fourth_non_improvement_emits_no_additional_wake() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..4 {
        let outcome = run_non_improvement(&harness, index, &limits);
        assert_eq!(outcome, PromotionOutcome::NotImproved);
    }

    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![(
            format!("strategy-refresh:v1:{CAMPAIGN_ID}:1"),
            "pending".to_owned()
        )]
    );
    assert_eq!(harness.promotion_row(), (None, 4));
}

#[test]
fn improvement_resets_the_plateau_and_the_next_round_wakes_with_round_two() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    let limits = CampaignLimits::default();

    for index in 0..3 {
        assert_eq!(
            run_non_improvement(&harness, index, &limits),
            PromotionOutcome::NotImproved
        );
    }
    let improver = format!("{CHALLENGER_EXPERIMENT_ID}-improver");
    harness.add_experiment(&improver);
    harness.seed_metrics(&improver, Some(0.5));
    assert_eq!(
        evaluate(
            &harness.db,
            CAMPAIGN_ID,
            &improver,
            ExperimentStatus::Succeeded,
            &limits,
            400,
        )
        .unwrap(),
        PromotionOutcome::Improved
    );
    for index in 10..13 {
        assert_eq!(
            run_non_improvement(&harness, index, &limits),
            PromotionOutcome::NotImproved
        );
    }

    assert_eq!(
        harness.strategy_refresh_wakes(),
        vec![
            (
                format!("strategy-refresh:v1:{CAMPAIGN_ID}:1"),
                "pending".to_owned()
            ),
            (
                format!("strategy-refresh:v1:{CAMPAIGN_ID}:2"),
                "pending".to_owned()
            ),
        ]
    );
    let (_, payload) = harness.wake_payload(&format!("strategy-refresh:v1:{CAMPAIGN_ID}:2"));
    assert_eq!(payload["round"], 2);
    assert_eq!(harness.promotion_row(), (Some(improver), 3));
}

#[test]
fn promotion_audit_is_passive_completed_and_idempotent() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let dedup_key = format!("promotion:v1:{CAMPAIGN_ID}:{CHALLENGER_EXPERIMENT_ID}");
    let payload = json!({
        "source": "promotion",
        "reason": "improved",
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": CHALLENGER_EXPERIMENT_ID,
    });
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO events (
                 project_id, campaign_id, experiment_id, kind, dedup_key, payload_json,
                 status, attempts, not_before, lease_until, created_at, completed_at, last_error
             ) VALUES (?1, ?2, ?3, 'operator_wake', ?4, ?5, 'pending', 0, 250, NULL, 250, NULL, NULL)",
            params![
                PROJECT_ID,
                CAMPAIGN_ID,
                CHALLENGER_EXPERIMENT_ID,
                dedup_key,
                payload.to_string(),
            ],
        )
        .unwrap();

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::Improved);

    let connection = harness.db.connect().unwrap();
    let event: (String, String, Option<i64>) = connection
        .query_row(
            "SELECT dedup_key, status, completed_at FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'
             ORDER BY created_at, event_id",
            [PROJECT_ID],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(event.1, "completed", "promotion audit must be completed, not pending");
    assert_eq!(event.0, dedup_key);
    assert_eq!(event.2, Some(300));

    let open_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND dedup_key = ?2
               AND status IN ('pending', 'claimed', 'retry_wait')",
            params![PROJECT_ID, event.0.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(open_count, 0, "promotion audit must not remain claimable");

    let _ = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        310,
    ).unwrap();

    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key = ?2",
            params![PROJECT_ID, event.0.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1, "promotion audit must be idempotent on retry");
}

#[test]
fn promotion_audit_collision_with_mismatched_payload_fails_closed() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let dedup_key = format!("promotion:v1:{CAMPAIGN_ID}:{CHALLENGER_EXPERIMENT_ID}");
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO events (
                 project_id, campaign_id, experiment_id, kind, dedup_key, payload_json,
                 status, attempts, not_before, lease_until, created_at, completed_at, last_error
             ) VALUES (?1, ?2, ?3, 'operator_wake', ?4, ?5, 'completed', 0, 250, NULL, 250, 250, NULL)",
            params![
                PROJECT_ID,
                CAMPAIGN_ID,
                CHALLENGER_EXPERIMENT_ID,
                dedup_key,
                json!({
                    "source": "promotion",
                    "reason": "wrong",
                    "campaign_id": CAMPAIGN_ID,
                    "experiment_id": CHALLENGER_EXPERIMENT_ID,
                })
                .to_string(),
            ],
        )
        .unwrap();

    let error = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        pueue_agent::AppError::Validation {
            field: "event.payload",
            ..
        }
    ));
    assert_eq!(harness.promotion_row(), (Some(BASELINE_EXPERIMENT_ID.to_owned()), 0));
}

#[test]
fn promotion_audit_not_emitted_for_baseline() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        BASELINE_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        200,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::BaselineEstablished);

    let connection = harness.db.connect().unwrap();
    let count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM events
             WHERE project_id = ?1 AND kind = 'operator_wake'
               AND dedup_key LIKE 'promotion:v1:%'",
            [PROJECT_ID],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0, "baseline promotion must not emit audit marker");
}

#[test]
fn evaluation_fails_closed_when_metrics_row_is_missing() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    // Do NOT seed metrics for challenger - simulating missing row

    let error = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        pueue_agent::AppError::Validation {
            field: "experiment_id",
            ..
        }
    ));

    let connection = harness.db.connect().unwrap();
    let (best, plateau): (Option<String>, i64) = connection
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(best, None, "missing metrics must not establish a best");
    assert_eq!(plateau, 0, "plateau must not change when metrics row is missing");
    assert!(MetricsRepository::get(&harness.db, CHALLENGER_EXPERIMENT_ID)
        .unwrap()
        .is_none());
}

#[test]
fn evaluation_idempotent_already_evaluated() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);

    let _ = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    ).unwrap();

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        310,
    ).unwrap();
    assert_eq!(outcome, PromotionOutcome::Improved);

    let connection = harness.db.connect().unwrap();
    let (best, plateau): (Option<String>, i64) = connection
        .query_row(
            "SELECT current_best_experiment_id, plateau_count
             FROM campaigns WHERE campaign_id = ?1",
            [CAMPAIGN_ID],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(best, Some(CHALLENGER_EXPERIMENT_ID.to_owned()));
    assert_eq!(plateau, 0, "re-evaluation must not change plateau");
}

#[test]
fn evaluation_rejects_candidate_from_another_campaign_without_mutation() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_other_campaign_experiment();
    harness.seed_metrics(OTHER_EXPERIMENT_ID, Some(0.1));
    let campaign_before = harness.campaign_evaluation_row(CAMPAIGN_ID);
    let other_before = harness.campaign_evaluation_row(OTHER_CAMPAIGN_ID);
    let event_count_before: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();

    let error = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        OTHER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        pueue_agent::AppError::Validation {
            field: "experiment_id",
            ..
        }
    ));
    assert_eq!(harness.campaign_evaluation_row(CAMPAIGN_ID), campaign_before);
    assert_eq!(
        harness.campaign_evaluation_row(OTHER_CAMPAIGN_ID),
        other_before
    );
    assert_eq!(harness.evaluated_at(OTHER_EXPERIMENT_ID), None);
    let event_count_after: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(event_count_after, event_count_before);
}

#[test]
fn evaluation_rejects_cross_campaign_persisted_comparison_pointers_before_mutation() {
    for pointer in ["current_best", "baseline"] {
        let harness = Harness::new();
        harness.start_campaign(Some(&minimize_metric()));
        harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
        harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
        harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
        harness.add_other_campaign_experiment();
        harness.seed_metrics(OTHER_EXPERIMENT_ID, Some(0.1));
        if pointer == "current_best" {
            harness.set_promotion_state(Some(OTHER_EXPERIMENT_ID), 2);
        } else {
            harness.set_baseline_experiment_id(Some(OTHER_EXPERIMENT_ID));
        }
        let before = harness.campaign_evaluation_row(CAMPAIGN_ID);

        let error = evaluate(
            &harness.db,
            CAMPAIGN_ID,
            CHALLENGER_EXPERIMENT_ID,
            ExperimentStatus::Succeeded,
            &CampaignLimits::default(),
            300,
        )
        .unwrap_err();

        assert!(matches!(
            error,
            pueue_agent::AppError::Validation {
                field: "current_best_experiment_id" | "baseline_experiment_id",
                ..
            }
        ));
        assert_eq!(harness.campaign_evaluation_row(CAMPAIGN_ID), before);
        assert_eq!(harness.evaluated_at(CHALLENGER_EXPERIMENT_ID), None);
    }
}

#[test]
fn defect_metric_value_cannot_promote_a_best_experiment() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(0.5));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE experiment_metrics
             SET artifact_defect = 'result_invalid', primary_metric_value = 0.5
             WHERE experiment_id = ?1",
            [CHALLENGER_EXPERIMENT_ID],
        )
        .unwrap();

    let outcome = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap();

    assert_eq!(outcome, PromotionOutcome::NotImproved);
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 1)
    );
    assert!(harness.evaluated_at(CHALLENGER_EXPERIMENT_ID).is_some());
}

#[test]
fn campaign_rejects_negative_objective_metric_delta_before_insert() {
    let harness = Harness::new();
    let metric = ObjectiveMetric {
        name: "loss".to_owned(),
        direction: MetricDirection::Minimize,
        min_delta: Some(-0.5),
    };

    let error = harness.try_start_campaign(Some(&metric)).unwrap_err();

    assert!(matches!(
        error,
        pueue_agent::AppError::Validation {
            field: "objective_metric.min_delta",
            ..
        }
    ));
    let campaign_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM campaigns", [], |row| row.get(0))
        .unwrap();
    assert_eq!(campaign_count, 0);
}

#[test]
fn persisted_negative_delta_fails_closed_before_worse_result_promotion() {
    let harness = Harness::new();
    harness.start_campaign(Some(&minimize_metric()));
    harness.seed_metrics(BASELINE_EXPERIMENT_ID, Some(1.0));
    harness.add_experiment(CHALLENGER_EXPERIMENT_ID);
    harness.seed_metrics(CHALLENGER_EXPERIMENT_ID, Some(1.2));
    harness.set_promotion_state(Some(BASELINE_EXPERIMENT_ID), 0);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE campaigns
             SET objective_metric_json = ?1
             WHERE campaign_id = ?2",
            params![
                json!({
                    "name": "loss",
                    "direction": "minimize",
                    "min_delta": -0.5,
                })
                .to_string(),
                CAMPAIGN_ID,
            ],
        )
        .unwrap();

    let error = evaluate(
        &harness.db,
        CAMPAIGN_ID,
        CHALLENGER_EXPERIMENT_ID,
        ExperimentStatus::Succeeded,
        &CampaignLimits::default(),
        300,
    )
    .unwrap_err();

    assert!(matches!(
        error,
        pueue_agent::AppError::Validation {
            field: "objective_metric.min_delta",
            ..
        }
    ));
    assert_eq!(
        harness.promotion_row(),
        (Some(BASELINE_EXPERIMENT_ID.to_owned()), 0)
    );
    assert_eq!(harness.evaluated_at(CHALLENGER_EXPERIMENT_ID), None);
    let event_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(event_count, 0);
}
