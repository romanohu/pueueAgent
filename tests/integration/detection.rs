use std::{
    fs,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use pueue_agent::{
    config::{CheckConfig, PatternAction, PatternConfig, StallConfig},
    db::{Db, EventRepository, ProjectRepository},
    detect::{Detector, Observation, ObservationState},
    execution_policy::{LogUnsafeReason, PolicyViolationCode, PolicyViolationDetail},
    incidents::IncidentStore,
    logs::LogSnapshot,
    models::{EventKind, IncidentStatus, IncidentTransition, NewProject},
    pueue::PueueTask,
    reconcile::{task_incident_key, task_signature},
};
use tempfile::TempDir;

struct Harness {
    _temp: TempDir,
    db: Db,
    project_root: PathBuf,
    task_log_dir: PathBuf,
}

impl Harness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let project_root = temp.path().join("project");
        let task_log_dir = temp.path().join("pueue-logs");
        fs::create_dir_all(&project_root).unwrap();
        fs::create_dir_all(&task_log_dir).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &project_root,
                "pa-project",
                project_root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self {
            _temp: temp,
            db,
            project_root,
            task_log_dir,
        }
    }

    fn detector(&self) -> Detector {
        Detector::for_project("project-a", &self.project_root, &self.task_log_dir)
    }

    fn persistent_detector(&self) -> Detector {
        self.detector().with_incident_db(self.db.clone())
    }

    fn store(&self) -> IncidentStore<'_> {
        IncidentStore::new(&self.db)
    }

    fn active_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM incidents
                 WHERE project_id = ?1 AND status IN ('open', 'acknowledged')",
                ["project-a"],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn resolved_count(&self) -> i64 {
        self.db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM incidents
                 WHERE project_id = ?1 AND status = ?2",
                rusqlite::params!["project-a", IncidentStatus::Resolved],
                |row| row.get(0),
            )
            .unwrap()
    }
}

fn check_config(log_tail_bytes: u32) -> CheckConfig {
    CheckConfig {
        interval_minutes: 1,
        deep_check_interval_minutes: 0,
        stall_minutes: 5,
        log_tail_bytes,
        extra_log_paths: Vec::new(),
        patterns: Vec::new(),
        stall: StallConfig {
            action: PatternAction::Notify,
            kill_after_minutes: 0,
        },
    }
}

fn task() -> PueueTask {
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

fn terminal_task() -> PueueTask {
    let mut task = task();
    task.state = "Done".to_owned();
    task.ended_at = Some("300".to_owned());
    task
}

fn task_log_path(log_dir: &Path, task_id: i64) -> PathBuf {
    log_dir.join(format!("{task_id}.log"))
}

fn snapshot_modified_seconds(path: &Path) -> i64 {
    let snapshot = LogSnapshot::read_tail(path, 64).unwrap();
    i64::try_from(snapshot.modified_at_nanos.unwrap() / 1_000_000_000).unwrap()
}

#[test]
fn log_snapshot_read_boundary_rejects_out_of_range_tail() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"bounded").unwrap();
    for value in [0, 1_048_577] {
        let error = LogSnapshot::read_tail(temp.path(), value).unwrap_err();
        assert!(error.to_string().contains("check.log_tail_bytes"));
    }
}

#[cfg(unix)]
#[test]
fn descriptor_tail_read_preserves_caller_offset() {
    let temp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(temp.path(), b"0123456789abcdef").unwrap();
    let mut file = std::fs::File::open(temp.path()).unwrap();
    file.seek(SeekFrom::Start(5)).unwrap();
    let caller_offset = file.stream_position().unwrap();

    let snapshot = LogSnapshot::read_tail_from_file(&file, 4).unwrap();

    assert_eq!(snapshot.byte_size, 16);
    assert_eq!(snapshot.evidence, "cdef");
    assert!(snapshot.fingerprint.starts_with("log:v1:size=16:mtime="));
    assert!(snapshot.fingerprint.ends_with(":tail=ce57cb90f6547719"));
    assert_eq!(file.stream_position().unwrap(), caller_offset);
}

#[cfg(unix)]
#[test]
fn public_path_tail_read_rejects_a_symlink_before_metadata_or_read() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target.log");
    let link = temp.path().join("link.log");
    std::fs::write(&target, b"target").unwrap();
    std::os::unix::fs::symlink(&target, &link).unwrap();
    assert!(matches!(
        LogSnapshot::read_tail(&link, 64),
        Err(pueue_agent::AppError::PolicyViolation {
            violation: pueue_agent::execution_policy::PolicyViolation {
                code: PolicyViolationCode::LogUnsafe,
                detail: PolicyViolationDetail::LogUnsafe(LogUnsafeReason::Symlink),
                ..
            }
        })
    ));
}

fn nan_observation(task: &PueueTask) -> Observation {
    Observation::pattern(
        "project-a",
        task_incident_key(task),
        "nan-loss",
        PatternAction::Wake,
        3,
        "loss: NaN\nloss: NaN\nloss: NaN",
        200,
    )
}

#[test]
fn identical_nan_observations_update_one_incident() {
    let harness = Harness::new();
    let task = task();
    let store = harness.store();

    let first = store.observe(nan_observation(&task)).unwrap();
    let second = store.observe(nan_observation(&task)).unwrap();

    assert_eq!(first, IncidentTransition::Opened);
    assert_eq!(second, IncidentTransition::Unchanged);
    assert_eq!(harness.active_count(), 1);
}

#[test]
fn repeated_wake_observation_creates_one_durable_agent_event() {
    let harness = Harness::new();
    let task = task();
    let store = harness.store();

    store.observe(nan_observation(&task)).unwrap();
    store.observe(nan_observation(&task)).unwrap();

    let events = EventRepository::new(&harness.db)
        .recent_events("project-a", 10)
        .unwrap();
    let crash_events = events
        .iter()
        .filter(|event| event.kind == EventKind::Crash)
        .collect::<Vec<_>>();
    assert_eq!(crash_events.len(), 1);
    assert_eq!(crash_events[0].payload["source"], "incident_detector");
    assert_eq!(crash_events[0].payload["action"], "wake");
}

#[test]
fn terminal_task_observation_resolves_incident_opened_while_running() {
    let harness = Harness::new();
    let running = task();
    let terminal = terminal_task();
    let store = harness.store();

    assert_eq!(
        store.observe(nan_observation(&running)).unwrap(),
        IncidentTransition::Opened
    );
    let terminal_recovery = store
        .observe(Observation::task_terminal(
            "project-a",
            task_incident_key(&terminal),
            300,
        ))
        .unwrap();

    assert_eq!(terminal_recovery, IncidentTransition::Resolved);
    assert_eq!(harness.active_count(), 0);
    assert_eq!(harness.resolved_count(), 1);
}

#[test]
fn pattern_requires_configured_confirmation_count_and_bounded_evidence() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(
        &log_path,
        "loss: NaN\nthis line is outside the evidence tail\nloss: NaN\nloss: NaN\n",
    )
    .unwrap();
    let mut config = check_config(30);
    config.patterns.push(PatternConfig {
        name: "nan-loss".to_owned(),
        regex: "loss: NaN".to_owned(),
        action: PatternAction::Wake,
        confirm_matches: 3,
    });

    let observations = harness.detector().inspect_task(&task, &config).unwrap();
    assert!(observations.is_empty());

    fs::write(&log_path, "loss: NaN\nloss: NaN\nloss: NaN\n").unwrap();
    let observations = harness.detector().inspect_task(&task, &config).unwrap();

    assert_eq!(observations.len(), 1);
    let observation = &observations[0];
    assert_eq!(observation.pattern_name(), Some("nan-loss"));
    assert_eq!(observation.action(), PatternAction::Wake);
    assert_eq!(observation.confirmation_count(), Some(3));
    assert!(observation.evidence().len() <= usize::try_from(config.log_tail_bytes).unwrap());
}

#[test]
fn unchanged_running_task_emits_notify_at_stall_threshold() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let modified_at = snapshot_modified_seconds(&log_path);
    let config = check_config(64);

    let before_threshold = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 5 * 60 - 1)
        .unwrap();
    let at_threshold = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 5 * 60)
        .unwrap();

    assert!(before_threshold.is_empty());
    assert_eq!(at_threshold.len(), 1);
    assert_eq!(at_threshold[0].kind(), "stalled");
    assert_eq!(at_threshold[0].state(), &ObservationState::Active);
    assert_eq!(at_threshold[0].action(), PatternAction::Notify);
    assert_eq!(
        at_threshold[0].task_signature(),
        Some(task_signature(&task).as_str())
    );
    assert!(at_threshold[0].evidence().len() <= 64);
}

#[test]
fn stalled_wake_policy_creates_one_durable_agent_event() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let modified_at = snapshot_modified_seconds(&log_path);
    let mut config = check_config(64);
    config.stall.action = PatternAction::Wake;

    let observation = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 5 * 60)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    harness.store().observe(observation).unwrap();

    let events = EventRepository::new(&harness.db)
        .recent_events("project-a", 10)
        .unwrap();
    let stalled_events = events
        .iter()
        .filter(|event| event.kind == EventKind::Stalled)
        .collect::<Vec<_>>();
    assert_eq!(stalled_events.len(), 1);
    assert_eq!(stalled_events[0].payload["action"], "wake");
}

#[test]
fn default_stall_policy_never_creates_a_termination_request() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let modified_at = snapshot_modified_seconds(&log_path);
    let config = check_config(64);

    let observation = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 5 * 60)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    harness.store().observe(observation).unwrap();

    let request_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM termination_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(request_count, 0);
}

#[test]
fn stalled_kill_policy_waits_for_additional_delay() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let modified_at = snapshot_modified_seconds(&log_path);
    let mut config = check_config(64);
    config.stall.action = PatternAction::Kill;
    config.stall.kill_after_minutes = 3;

    let at_stall_threshold = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 5 * 60)
        .unwrap();
    let before_kill_delay = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 8 * 60 - 1)
        .unwrap();
    let at_kill_delay = harness
        .detector()
        .inspect_task_at(&task, &config, modified_at + 8 * 60)
        .unwrap();

    assert!(at_stall_threshold.is_empty());
    assert!(before_kill_delay.is_empty());
    assert_eq!(at_kill_delay.len(), 1);
    assert_eq!(at_kill_delay[0].kind(), "stalled");
    assert_eq!(at_kill_delay[0].action(), PatternAction::Kill);
    assert_eq!(
        at_kill_delay[0].task_signature(),
        Some(task_signature(&task).as_str())
    );

    harness
        .store()
        .observe(at_kill_delay.into_iter().next().unwrap())
        .unwrap();
    let request_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM termination_requests", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(request_count, 1);
}

#[test]
fn changed_snapshot_emits_recovery_after_detector_restart() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let first_modified_at = snapshot_modified_seconds(&log_path);
    let config = check_config(64);

    let stalled = harness
        .persistent_detector()
        .inspect_task_at(&task, &config, first_modified_at + 5 * 60)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        harness.store().observe(stalled).unwrap(),
        IncidentTransition::Opened
    );

    fs::write(&log_path, "epoch 1\nepoch 2\n").unwrap();
    let changed_modified_at = snapshot_modified_seconds(&log_path);
    let recovered = harness
        .persistent_detector()
        .inspect_task_at(&task, &config, changed_modified_at + 1)
        .unwrap();

    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].kind(), "stalled");
    assert_eq!(recovered[0].state(), &ObservationState::Recovered);
    assert_eq!(
        recovered[0].task_signature(),
        Some(task_signature(&task).as_str())
    );
    assert_eq!(
        harness
            .store()
            .observe(recovered.into_iter().next().unwrap())
            .unwrap(),
        IncidentTransition::Resolved
    );
    assert_eq!(harness.active_count(), 0);
    assert_eq!(harness.resolved_count(), 1);
}

#[test]
fn repeated_stalled_snapshot_is_unchanged_but_log_growth_resolves_it() {
    let harness = Harness::new();
    let task = task();
    let log_path = task_log_path(&harness.task_log_dir, task.id);
    fs::write(&log_path, "epoch 1\n").unwrap();
    let first_snapshot = LogSnapshot::read_tail(&log_path, 64).unwrap();
    let store = harness.store();

    let opened = store
        .observe(Observation::stalled(
            "project-a",
            task_incident_key(&task),
            first_snapshot.clone(),
            PatternAction::Notify,
            200,
        ))
        .unwrap();
    let unchanged = store
        .observe(Observation::stalled(
            "project-a",
            task_incident_key(&task),
            first_snapshot,
            PatternAction::Notify,
            201,
        ))
        .unwrap();

    thread::sleep(Duration::from_millis(5));
    fs::write(&log_path, "epoch 1\nepoch 2\n").unwrap();
    let grown_snapshot = LogSnapshot::read_tail(&log_path, 64).unwrap();
    let resolved = store
        .observe(Observation::stalled_recovered(
            "project-a",
            task_incident_key(&task),
            grown_snapshot,
            202,
        ))
        .unwrap();

    assert_eq!(opened, IncidentTransition::Opened);
    assert_eq!(unchanged, IncidentTransition::Unchanged);
    assert_eq!(resolved, IncidentTransition::Resolved);
    assert_eq!(harness.active_count(), 0);
    assert_eq!(harness.resolved_count(), 1);
}

#[test]
fn extra_logs_are_project_relative_and_reject_path_traversal_after_canonicalization() {
    let harness = Harness::new();
    let task = task();
    fs::create_dir_all(harness.project_root.join("logs")).unwrap();
    fs::write(
        harness.project_root.join("logs/train.log"),
        "CUDA out of memory\n",
    )
    .unwrap();
    let outside = harness._temp.path().join("outside.log");
    fs::write(&outside, "CUDA out of memory\n").unwrap();
    let escape_link = harness.project_root.join("logs/escape.log");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &escape_link).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_file(&outside, &escape_link).unwrap();

    let mut config = check_config(64);
    config.extra_log_paths = vec![PathBuf::from("logs/train.log")];
    config.patterns.push(PatternConfig {
        name: "cuda-oom".to_owned(),
        regex: "CUDA.*out of memory".to_owned(),
        action: PatternAction::Kill,
        confirm_matches: 1,
    });

    let observations = harness.detector().inspect_task(&task, &config).unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(
        observations[0].source_path(),
        Some(Path::new("logs/train.log"))
    );

    config.extra_log_paths = vec![PathBuf::from("logs/escape.log")];
    let error = harness.detector().inspect_task(&task, &config).unwrap_err();
    assert!(error.to_string().contains("extra_log_paths"));
}

#[test]
fn taskless_recovery_does_not_resolve_unrelated_extra_log_incident() {
    let harness = Harness::new();
    let store = harness.store();
    let active_snapshot = LogSnapshot {
        byte_size: 19,
        modified_at_nanos: Some(1),
        fingerprint: "log:v1:active-extra".to_owned(),
        evidence: "CUDA out of memory\n".to_owned(),
    };
    let unrelated_recovered_snapshot = LogSnapshot {
        byte_size: 11,
        modified_at_nanos: Some(2),
        fingerprint: "log:v1:other-extra".to_owned(),
        evidence: "loss normal\n".to_owned(),
    };

    let opened = store
        .observe(Observation::extra_log_pattern(
            "project-a",
            PathBuf::from("logs/train.log"),
            "cuda-oom",
            PatternAction::Kill,
            1,
            active_snapshot,
            200,
        ))
        .unwrap();
    let unrelated_recovery = store
        .observe(Observation::extra_log_pattern_recovered(
            "project-a",
            PathBuf::from("logs/other.log"),
            "cuda-oom",
            unrelated_recovered_snapshot,
            201,
        ))
        .unwrap();

    assert_eq!(opened, IncidentTransition::Opened);
    assert_eq!(unrelated_recovery, IncidentTransition::Unchanged);
    assert_eq!(harness.active_count(), 1);
    assert_eq!(harness.resolved_count(), 0);
}

#[test]
fn task_id_reuse_keeps_incidents_separate_by_stable_task_incident_key() {
    let harness = Harness::new();
    let first_task = task();
    let mut reused_task = task();
    reused_task.started_at = Some("200".to_owned());
    let store = harness.store();

    assert_eq!(
        store.observe(nan_observation(&first_task)).unwrap(),
        IncidentTransition::Opened
    );
    let reused = Observation::pattern(
        "project-a",
        task_incident_key(&reused_task),
        "nan-loss",
        PatternAction::Wake,
        3,
        "loss: NaN\nloss: NaN\nloss: NaN",
        201,
    );
    assert_eq!(store.observe(reused).unwrap(), IncidentTransition::Opened);

    let active_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM incidents
             WHERE project_id = ?1 AND kind = ?2 AND status IN ('open', 'acknowledged')",
            ["project-a", "pattern"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_count, 2);
}
