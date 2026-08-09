use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use pueue_agent::{
    config::{CheckConfig, PatternAction, PatternConfig, StallConfig},
    db::{Db, ProjectRepository},
    detect::{Detector, Observation},
    incidents::IncidentStore,
    logs::LogSnapshot,
    models::{IncidentStatus, IncidentTransition, NewProject},
    pueue::PueueTask,
    reconcile::task_signature,
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
        Detector::new(&self.project_root, &self.task_log_dir)
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
        deep_check_every: 3,
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

fn task_log_path(log_dir: &Path, task_id: i64) -> PathBuf {
    log_dir.join(format!("{task_id}.log"))
}

fn nan_observation(task: &PueueTask) -> Observation {
    Observation::pattern(
        "project-a",
        task_signature(task),
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
            task_signature(&task),
            first_snapshot.clone(),
            PatternAction::Notify,
            200,
        ))
        .unwrap();
    let unchanged = store
        .observe(Observation::stalled(
            "project-a",
            task_signature(&task),
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
            task_signature(&task),
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
fn task_id_reuse_keeps_incidents_separate_by_full_task_signature() {
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
        task_signature(&reused_task),
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
