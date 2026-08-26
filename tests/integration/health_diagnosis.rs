

#![cfg(target_os = "linux")]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use pueue_agent::{
    agent::{AgentRunner, AgentRunnerConfig},
    config,
    db::{
        CampaignRepository, Db, ExperimentRepository, HealthRepository,
        ProjectRepository, StartCampaignRequest,
    },
    execution_policy::{
        load_existing_policy, CampaignLimits, PolicyLoadInput, StartupEnvironment,
    },
    health_diagnosis::run_due_diagnoses,
    models::{EventKind, EventStatus, HealthState, NewProject, ProposalKind},
    proposals::{self, ProposalInput},
    state::ObjectiveSnapshot,
};
use serde_json::json;
use tempfile::TempDir;

#[path = "../support/execution_policy_fixture.rs"]
#[allow(dead_code)]
mod execution_policy_fixture;

const NOW: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiagnoseOutput {
    Valid,
    Malformed,
}

struct StartedDiagnosis {
    run_id: i64,
    primary_event_id: i64,
    handle: pueue_agent::agent::AgentHandle,
}

struct DiagnosisHarness {
    _temp: TempDir,
    db: Db,
    policy: Arc<pueue_agent::execution_policy::ResolvedExecutionPolicy>,
    experiment_id: String,
    capture_path: PathBuf,
}

impl DiagnosisHarness {
    fn new(output: DiagnoseOutput) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let fixture_root = fs::canonicalize(temp.path()).unwrap();
        let project_root = fixture_root.join("project");
        let service_dir = project_root.join(".pueue-agent");
        let trusted_bin = fixture_root.join("trusted-bin");
        let policy_state = fixture_root.join("policy-state");
        let codex_home = fixture_root.join("codex-home");
        for directory in [
            &project_root,
            &service_dir,
            &trusted_bin,
            &policy_state,
            &codex_home,
        ] {
            fs::create_dir_all(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::create_dir_all(service_dir.join("logs")).unwrap();
        fs::write(service_dir.join("STATE.md"), "fixture state\n").unwrap();

        fs::write(
            service_dir.join("logs/41.log"),
            "epoch 1 loss 0.52\ntorch.cuda.OutOfMemoryError: CUDA out of memory\n",
        )
        .unwrap();

        let capture_path = fixture_root.join("diagnosis-capture.txt");
        let codex = trusted_bin.join("codex");
        compile_diagnosis_codex(&trusted_bin, &codex, &capture_path, output);
        let pueue = trusted_bin.join("pueue");
        fs::copy(&codex, &pueue).unwrap();
        fs::set_permissions(&pueue, fs::Permissions::from_mode(0o700)).unwrap();
        let launcher = trusted_bin.join("pueue-agent-launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher).unwrap();
        fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700)).unwrap();
        let pueue_config = fixture_root.join("pueue.yml");
        fs::write(&pueue_config, "fixture: true\n").unwrap();
        fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();

        let config_path = service_dir.join("config.toml");
        fs::write(
            &config_path,
            format!(
                r#"project_id = "diagnosis-project"
pueue_group = "diagnosis-project"

[agent]
program = "codex"
args = ["{{prompt}}"]
timeout_minutes = 1
max_retries = 0

[agent.execution]
network = "enabled"

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
"#
            ),
        )
        .unwrap();
        config::load(&config_path).unwrap();

        fs::write(
            policy_state.join("execution-policy.toml"),
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_bin.display().to_string(),
                codex.display().to_string(),
                pueue.display().to_string(),
            ),
        )
        .unwrap();
        fs::set_permissions(
            policy_state.join("execution-policy.toml"),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let policy = Arc::new(
            load_existing_policy(&PolicyLoadInput {
                state_dir: policy_state,
                project_roots: vec![fs::canonicalize(&project_root).unwrap()],
                inherited_path: trusted_bin.clone().into_os_string(),
                startup_environment: StartupEnvironment::from_pairs([
                    ("HOME", "/fixture"),
                    ("OPENAI_API_KEY", "fixture-openai-secret"),
                ]),
                codex_home,
                pueue_config,
                launcher_path: launcher,
            })
            .unwrap(),
        );

        let db = Db::open(&fixture_root.join("state.sqlite3")).unwrap();
        let project = ProjectRepository::new(&db)
            .register(&NewProject::new(
                "diagnosis-project",
                fs::canonicalize(&project_root).unwrap(),
                "diagnosis-project",
                config_path,
                100,
            ))
            .unwrap();

        let objective = ObjectiveSnapshot {
            text: "Improve the validation result safely.\n".to_owned(),
            digest: "diagnosis-objective-digest".to_owned(),
        };
        let initial_argv = vec!["python".to_owned(), "train.py".to_owned()];
        let baseline = proposals::validate_initial_baseline(
            ProposalInput {
                kind: ProposalKind::Experiment,
                hypothesis: "Establish a baseline".to_owned(),
                source_experiment_id: None,
                argv: initial_argv.clone(),
                working_directory: ".".to_owned(),
                expected_evidence: vec!["validation loss".to_owned()],
            },
            &objective.digest,
        )
        .unwrap();
        CampaignRepository::new(&db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "diagnosis-campaign",
                    project_id: &project.project_id,
                    objective: &objective,
                    initial_argv: &initial_argv,
                    baseline: &baseline,
                    submission_id: "diagnosis-submission",
                    experiment_id: "diagnosis-experiment",
                    proposal_id: "diagnosis-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 100,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
        let experiments = ExperimentRepository::new(&db);
        experiments
            .mark_submitting("diagnosis-experiment", 101)
            .unwrap();
        experiments
            .mark_accepted(
                "diagnosis-experiment",
                41,
                "diagnosis-task-signature",
                102,
            )
            .unwrap();

        HealthRepository::ensure_running(&db, "diagnosis-project", "diagnosis-campaign", "diagnosis-experiment", 41, 103)
            .unwrap();
        for observed_at in [104_i64, 105] {
            HealthRepository::record_observation(
                &db,
                "diagnosis-experiment",
                observed_at,
                pueue_agent::models::SignalSummaryEntry {
                    class: "oom".to_owned(),
                    source: "builtin_probe".to_owned(),
                    evidence_digest: format!("oom-digest-{observed_at}"),
                    observed_at,
                },
            )
            .unwrap();
        }
        HealthRepository::set_state(&db, "diagnosis-experiment", HealthState::Suspicious, 106)
            .unwrap();

        Self {
            _temp: temp,
            db,
            policy,
            experiment_id: "diagnosis-experiment".to_owned(),
            capture_path,
        }
    }

    fn runner(&self) -> AgentRunner {
        AgentRunner::new(AgentRunnerConfig::production(), Arc::clone(&self.policy))
    }

    async fn spawn_once(&self, runner: &AgentRunner) -> Option<StartedDiagnosis> {
        let mut report = run_due_diagnoses(&self.db, runner, 4, NOW).await.unwrap();
        assert_eq!(report.failed_spawns, 0);
        assert!(report.started.len() <= 1);
        let started = report.started.pop()?;
        Some(StartedDiagnosis {
            run_id: started.run_id,
            primary_event_id: started.primary_event_id,
            handle: started.handle,
        })
    }

    fn row_state(&self) -> HealthState {
        HealthRepository::get(&self.db, &self.experiment_id)
            .unwrap()
            .unwrap()
            .state
    }

    fn diagnosis_json(&self) -> serde_json::Value {
        let raw = HealthRepository::get(&self.db, &self.experiment_id)
            .unwrap()
            .unwrap()
            .diagnosis_json;
        serde_json::from_str(raw.as_deref().unwrap_or("null")).unwrap()
    }

    fn run_status(&self, run_id: i64) -> (String, Option<String>) {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status, last_error FROM agent_runs WHERE run_id = ?1",
                [run_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .unwrap()
    }

    fn event_status(&self, event_id: i64) -> EventStatus {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM events WHERE event_id = ?1 AND kind = ?2",
                rusqlite::params![event_id, EventKind::HealthDiagnosis],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn capture(&self, field: &str) -> Option<String> {
        fs::read_to_string(&self.capture_path)
            .ok()
            .and_then(|contents| {
                contents.lines().find_map(|line| {
                    line.split_once('=')
                        .filter(|(name, _)| *name == field)
                        .map(|(_, value)| value.to_owned())
                })
            })
    }
}

#[tokio::test]
async fn suspicious_row_spawns_one_diagnosis_agent_with_schema_argv() {
    let harness = DiagnosisHarness::new(DiagnoseOutput::Valid);
    let runner = harness.runner();

    let mut first = harness.spawn_once(&runner).await.expect("one diagnosis run");

    let repeat = harness.spawn_once(&runner).await;
    assert!(
        repeat.is_none(),
        "a diagnosing row must not spawn a second agent"
    );

    let connection = harness.db.connect().unwrap();
    let (execution_kind, status): (Option<String>, String) = connection
        .query_row(
            "SELECT execution_kind, status FROM agent_runs WHERE run_id = ?1",
            [first.run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    drop(connection);
    assert_eq!(execution_kind.as_deref(), Some("diagnosis"));
    assert_eq!(status, "running");
    assert_eq!(harness.row_state(), HealthState::Diagnosing);

    let schema_arg = harness.capture("schema_arg").expect("schema argv captured");
    assert_eq!(schema_arg, "/dev/fd/11/health-diagnosis-schema.json");
    let output_arg = harness.capture("output_arg").expect("output argv captured");
    assert_eq!(output_arg, "/dev/fd/11/health-diagnosis.json");
    assert_eq!(harness.capture("sandbox_read_only").as_deref(), Some("true"));

    let _status = first.handle.wait(&harness.db, NOW + 1).await.unwrap();
}

#[tokio::test]
async fn valid_diagnosis_persists_and_moves_action_pending() {
    let harness = DiagnosisHarness::new(DiagnoseOutput::Valid);
    let runner = harness.runner();

    let mut started = harness.spawn_once(&runner).await.expect("one diagnosis run");
    let event_id = started.primary_event_id;
    let status = started.handle.wait(&harness.db, NOW + 1).await.unwrap();
    assert_eq!(status, pueue_agent::models::AgentRunStatus::Completed);

    assert_eq!(harness.row_state(), HealthState::ActionPending);
    let diagnosis = harness.diagnosis_json();
    assert_eq!(diagnosis["root_cause_class"], json!("oom"));
    assert_eq!(diagnosis["confidence"], json!(0.9));
    assert_eq!(diagnosis["recommended_action"], json!("kill_and_resume"));
    assert_eq!(diagnosis["summary"], json!("gpu exhausted"));
    assert_eq!(harness.event_status(event_id), EventStatus::Completed);
}

#[tokio::test]
async fn malformed_diagnosis_retries_then_dead_letters_row_to_suspicious() {
    let harness = DiagnosisHarness::new(DiagnoseOutput::Malformed);
    let runner = harness.runner();

    let mut dead_lettered_events = Vec::new();
    for round in 1..=3 {
        let mut started = harness
            .spawn_once(&runner)
            .await
            .unwrap_or_else(|| panic!("round {round} must spawn one diagnosis run"));
        dead_lettered_events.push(started.primary_event_id);
        let status = started.handle.wait(&harness.db, NOW + round).await.unwrap();
        assert_eq!(status, pueue_agent::models::AgentRunStatus::Failed);
    }

    assert_eq!(harness.row_state(), HealthState::Suspicious);
    assert_eq!(harness.diagnosis_json(), json!({"attempt": 3}));

    for (index, event_id) in dead_lettered_events.iter().enumerate() {
        assert_eq!(
            harness.event_status(*event_id),
            EventStatus::DeadLetter,
            "event {index}"
        );
    }
    for (index, event_id) in dead_lettered_events.iter().enumerate() {
        let run_id: i64 = harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT run_id FROM agent_run_events WHERE event_id = ?1",
                [event_id],
                |row| row.get(0),
            )
            .unwrap_or_else(|_| panic!("event {index} must own an agent run"));
        let (status, last_error) = harness.run_status(run_id);
        assert_eq!(status, "failed", "run {index}");
        assert_eq!(last_error.as_deref(), Some("health_diagnosis_missing"));
    }

    let fourth = harness.spawn_once(&runner).await;
    assert!(
        fourth.is_none(),
        "the attempt cap must stop further diagnosis spawns"
    );
}

fn compile_diagnosis_codex(
    trusted_bin: &Path,
    target: &Path,
    capture_path: &Path,
    output: DiagnoseOutput,
) {
    let source = trusted_bin.join(format!("codex-{output:?}.rs"));
    fs::write(
        &source,
        format!(
            r##"use std::{{env, fs, path::Path, process::exit}};

fn pair<'a>(args: &'a [String], name: &str) -> Option<&'a str> {{
    args.windows(2).find(|pair| pair[0] == name).map(|pair| pair[1].as_str())
}}

fn main() {{
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args == ["--version"] {{ println!("codex-cli 0.148.0"); return; }}
    if args == ["--help"] {{
        println!("--strict-config --sandbox read-only workspace-write --ask-for-approval never");
        return;
    }}
    if args == ["exec", "--help"] {{
        println!("--ignore-user-config --ignore-rules --strict-config --output-schema --output-last-message");
        return;
    }}
    let configs = args.windows(2).filter_map(|pair| (pair[0] == "-c").then_some(pair[1].as_str())).collect::<Vec<_>>();
    if pair(&args, "--sandbox").is_some()
        || !configs.contains(&"permissions.pueue_agent_decision.extends=\":read-only\"")
        || !configs.contains(&"default_permissions=\"pueue_agent_decision\"")
        || configs.iter().any(|value| value.starts_with("sandbox_workspace_write."))
    {{
        fs::write("forbidden-write", b"unsafe").unwrap();
    }}
    let network = configs.iter().find_map(|value| value.strip_prefix("permissions.pueue_agent_decision.network.enabled=")).unwrap_or("missing");
    let read_only = configs.contains(&"permissions.pueue_agent_decision.extends=\":read-only\"")
        && configs.contains(&"default_permissions=\"pueue_agent_decision\"");
    let mut capture = format!("network_access={{network}}\nsandbox_read_only={{read_only}}\n");
    if let Some(schema) = pair(&args, "--output-schema") {{ capture.push_str(&format!("schema_arg={{schema}}\n")); }}
    if let Some(value) = pair(&args, "--output-last-message") {{ capture.push_str(&format!("output_arg={{value}}\n")); }}
    fs::write({capture_path:?}, capture).unwrap();

    let schema = pair(&args, "--output-schema").unwrap();
    let output = pair(&args, "--output-last-message").unwrap();
    if !Path::new(schema).is_file() || !Path::new(output).is_file() {{ exit(71); }}
    match {output:?} {{
        "Valid" => fs::write(output, r#"{{"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"gpu exhausted"}}"#).unwrap(),
        "Malformed" => fs::write(output, "{{malformed-diagnosis").unwrap(),
        _ => exit(72),
    }}
}}
"##,
            capture_path = capture_path,
            output = format!("{output:?}"),
        ),
    )
    .unwrap();
    let compiled = Command::new("rustc")
        .args(["--edition=2021", "-o"])
        .arg(target)
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "generated diagnosis Codex failed: {}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    fs::set_permissions(target, fs::Permissions::from_mode(0o700)).unwrap();
}
