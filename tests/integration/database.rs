use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
};

use pueue_agent::{
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, ProjectRepository,
        SubmissionRepository, TaskObservationRepository, TerminationRequestRepository,
    },
    models::{
        AgentRunStatus, EventKind, EventStatus, IncidentStatus, IncidentTransition, NewAgentRun,
        NewEvent, NewIncident, NewProject, NewSubmission, NewTaskObservation,
        NewTerminationRequest, SubmissionStatus, TerminationRequestStatus,
    },
};
use rusqlite::params;
use serde_json::json;
use tempfile::TempDir;

struct TestDatabase {
    _temp: TempDir,
    path: PathBuf,
    db: Db,
}

impl TestDatabase {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("state.sqlite3");
        let db = Db::open(&path).unwrap();
        Self {
            _temp: temp,
            path,
            db,
        }
    }

    fn project_root(&self, name: &str) -> PathBuf {
        let root = self._temp.path().join(name);
        fs::create_dir_all(&root).unwrap();
        root
    }
}

fn register_project(db: &Db, project_id: &str, root: &Path, group: &str) {
    let project = NewProject::new(
        project_id,
        root,
        group,
        root.join(".pueue-agent/config.toml"),
        100,
    );
    ProjectRepository::new(db).register(&project).unwrap();
}

fn insert_event(db: &Db, project_id: &str, dedup_key: &str, not_before: i64) -> i64 {
    let event = NewEvent::new(
        project_id,
        EventKind::TaskFinished,
        dedup_key,
        json!({"task_id": 41}),
        not_before,
        100,
    );
    EventRepository::new(db)
        .insert_idempotent(&event)
        .unwrap()
        .event_id
}

#[test]
fn open_configures_sqlite_and_installs_all_tables() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();

    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .unwrap();
    let busy_timeout: i64 = connection
        .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
        .unwrap();
    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert!(busy_timeout >= 5_000);

    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .unwrap();
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for required in [
        "projects",
        "events",
        "incidents",
        "agent_runs",
        "agent_run_events",
        "submissions",
        "termination_requests",
        "task_observations",
    ] {
        assert!(
            names.iter().any(|name| name == required),
            "missing {required}"
        );
    }

    drop(statement);
    drop(connection);
    Db::open(&test.path).unwrap();
}

#[test]
fn project_registration_rejects_duplicate_canonical_root_and_group() {
    let test = TestDatabase::new();
    let first_root = test.project_root("first");
    let second_root = test.project_root("second");
    register_project(&test.db, "project-a", &first_root, "pa-first");

    let same_root = NewProject::new(
        "project-b",
        first_root.join("."),
        "pa-second",
        first_root.join(".pueue-agent/other.toml"),
        101,
    );
    let root_error = ProjectRepository::new(&test.db)
        .register(&same_root)
        .unwrap_err();
    assert!(root_error.to_string().contains("root_path"));

    let same_group = NewProject::new(
        "project-c",
        &second_root,
        "pa-first",
        second_root.join(".pueue-agent/config.toml"),
        101,
    );
    let group_error = ProjectRepository::new(&test.db)
        .register(&same_group)
        .unwrap_err();
    assert!(group_error.to_string().contains("pueue_group"));
}

#[test]
fn project_lookup_returns_registered_projects_by_group_and_canonical_root() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");

    let repository = ProjectRepository::new(&test.db);
    let by_group = repository.find_by_group("pa-project").unwrap().unwrap();
    assert_eq!(by_group.project_id, "project-a");
    assert_eq!(by_group.root_path, fs::canonicalize(&root).unwrap());

    let by_root = repository.find_by_root(&root.join(".")).unwrap().unwrap();
    assert_eq!(by_root.project_id, "project-a");
    assert!(repository.find_by_group("missing-group").unwrap().is_none());
    assert!(repository
        .find_by_root(&test._temp.path().join("missing"))
        .unwrap()
        .is_none());
}

#[test]
fn event_insert_is_idempotent_and_foreign_keys_are_enforced() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");

    let first = NewEvent::new(
        "project-a",
        EventKind::TaskFinished,
        "task_finished:41",
        json!({"result": "success"}),
        100,
        100,
    );
    let duplicate = NewEvent::new(
        "project-a",
        EventKind::TaskFailed,
        "task_finished:41",
        json!({"result": "changed"}),
        500,
        500,
    );
    let repository = EventRepository::new(&test.db);
    let inserted = repository.insert_idempotent(&first).unwrap();
    let original = repository.insert_idempotent(&duplicate).unwrap();

    assert_eq!(inserted.event_id, original.event_id);
    assert_eq!(original.kind, EventKind::TaskFinished);
    assert_eq!(original.payload, json!({"result": "success"}));
    assert_eq!(original.status, EventStatus::Pending);

    let missing_project =
        NewEvent::new("missing", EventKind::Crash, "crash:1", json!({}), 100, 100);
    assert!(repository.insert_idempotent(&missing_project).is_err());
}

#[test]
fn claim_batch_claims_only_eligible_pending_and_retry_events() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let pending_id = insert_event(&test.db, "project-a", "pending", 100);
    let future_id = insert_event(&test.db, "project-a", "future", 201);
    let retry_id = insert_event(&test.db, "project-a", "retry", 100);

    test.db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'retry_wait' WHERE event_id = ?1",
            [retry_id],
        )
        .unwrap();

    let claimed = EventRepository::new(&test.db)
        .claim_batch(200, 260, 10)
        .unwrap();
    let claimed_ids = claimed
        .iter()
        .map(|event| event.event_id)
        .collect::<Vec<_>>();
    assert_eq!(claimed_ids, vec![pending_id, retry_id]);
    assert!(claimed
        .iter()
        .all(|event| event.status == EventStatus::Claimed
            && event.lease_until == Some(260)
            && event.attempts == 1));

    let future_status: String = test
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT status FROM events WHERE event_id = ?1",
            [future_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(future_status, "pending");
}

#[test]
fn two_connections_cannot_claim_the_same_events() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    insert_event(&test.db, "project-a", "first", 100);
    insert_event(&test.db, "project-a", "second", 100);

    let barrier = Arc::new(Barrier::new(2));
    let handles = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = test.path.clone();
            thread::spawn(move || {
                let db = Db::open(&path).unwrap();
                barrier.wait();
                EventRepository::new(&db)
                    .claim_batch(100, 200, 10)
                    .unwrap()
                    .len()
            })
        })
        .collect::<Vec<_>>();

    let mut counts = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    counts.sort_unstable();
    assert_eq!(counts, vec![0, 2]);
}

#[test]
fn expired_claim_is_recovered_after_restart_and_can_be_reclaimed() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "restart", 100);
    let first_claim = EventRepository::new(&test.db)
        .claim_batch(100, 110, 1)
        .unwrap();
    assert_eq!(first_claim[0].event_id, event_id);

    let restarted = Db::open(&test.path).unwrap();
    let repository = EventRepository::new(&restarted);
    assert_eq!(repository.recover_expired_claims(109).unwrap(), 0);
    assert_eq!(repository.recover_expired_claims(111).unwrap(), 1);

    let reclaimed = repository.claim_batch(111, 150, 1).unwrap();
    assert_eq!(reclaimed[0].event_id, event_id);
    assert_eq!(reclaimed[0].attempts, 2);
}

#[test]
fn resolved_incident_can_recur_without_duplicate_active_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = IncidentRepository::new(&test.db);

    let first = NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 100);
    let opened = repository.upsert_active(&first).unwrap();
    assert_eq!(opened.transition, IncidentTransition::Opened);

    let unchanged = repository.upsert_active(&first).unwrap();
    assert_eq!(unchanged.incident.incident_id, opened.incident.incident_id);
    assert_eq!(unchanged.transition, IncidentTransition::Unchanged);

    let updated_input =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 101);
    let updated = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(updated.incident.incident_id, opened.incident.incident_id);
    assert_eq!(updated.transition, IncidentTransition::Unchanged);
    assert_eq!(updated.incident.last_seen_at, 101);

    assert_eq!(
        repository
            .resolve(opened.incident.incident_id, 102)
            .unwrap(),
        IncidentTransition::Resolved
    );
    let stale = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(stale.transition, IncidentTransition::Unchanged);
    assert_eq!(stale.incident.incident_id, opened.incident.incident_id);
    assert_eq!(stale.incident.status, IncidentStatus::Resolved);

    let at_resolution =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 102);
    let same_time = repository.upsert_active(&at_resolution).unwrap();
    assert_eq!(same_time.transition, IncidentTransition::Unchanged);
    assert_eq!(same_time.incident.incident_id, opened.incident.incident_id);

    let later_input =
        NewIncident::new("project-a", "pattern", Some("task-41"), "nan-loss:abc", 103);
    let recurrence = repository.upsert_active(&later_input).unwrap();
    assert_eq!(recurrence.transition, IncidentTransition::Opened);
    assert_ne!(recurrence.incident.incident_id, opened.incident.incident_id);

    let delayed_stale = repository.upsert_active(&updated_input).unwrap();
    assert_eq!(delayed_stale.transition, IncidentTransition::Unchanged);
    assert_eq!(
        delayed_stale.incident.incident_id,
        opened.incident.incident_id
    );
    assert_eq!(delayed_stale.incident.status, IncidentStatus::Resolved);

    let connection = test.db.connect().unwrap();
    let active_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM incidents
             WHERE project_id = ?1 AND kind = ?2 AND fingerprint = ?3
               AND status IN ('open', 'acknowledged')",
            params!["project-a", "pattern", "nan-loss:abc"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(active_count, 1);
}

#[test]
fn active_incident_reports_updated_only_when_task_identity_changes() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = IncidentRepository::new(&test.db);

    let opened = repository
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-41"),
            "nan-loss:abc",
            100,
        ))
        .unwrap();
    let updated = repository
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-42"),
            "nan-loss:abc",
            101,
        ))
        .unwrap();

    assert_eq!(updated.incident.incident_id, opened.incident.incident_id);
    assert_eq!(updated.incident.task_key.as_deref(), Some("task-42"));
    assert_eq!(updated.incident.last_seen_at, 101);
    assert_eq!(updated.transition, IncidentTransition::Updated);
}

#[test]
fn cross_project_foreign_keys_reject_agent_run_relationships() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let event_a = insert_event(&test.db, "project-a", "event-a", 100);
    let event_b = insert_event(&test.db, "project-b", "event-b", 100);

    let connection = test.db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?3, ?4)",
            params!["project-a", event_a, 100, "/tmp/agent-a.log"],
        )
        .unwrap();
    let run_a = connection.last_insert_rowid();

    assert!(connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?3, ?4)",
            params!["project-a", event_b, 100, "/tmp/agent-cross-event.log"],
        )
        .is_err());

    assert!(connection
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)",
            params!["project-a", run_a, event_b],
        )
        .is_err());

    assert!(connection
        .execute(
            "INSERT INTO agent_run_events (project_id, run_id, event_id)
             VALUES (?1, ?2, ?3)",
            params!["project-b", run_a, event_b],
        )
        .is_err());
}

#[test]
fn cross_project_foreign_key_rejects_termination_request_incident() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");

    let incident_a = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "fingerprint-a",
            100,
        ))
        .unwrap()
        .incident
        .incident_id;
    let connection = test.db.connect().unwrap();
    assert!(connection
        .execute(
            "INSERT INTO termination_requests (
                incident_id, project_id, task_signature, reason, status, requested_at
             ) VALUES (?1, ?2, ?3, ?4, 'requested', ?5)",
            params![incident_a, "project-b", "signature-b", "cross-project", 100],
        )
        .is_err());
}

#[test]
fn only_one_active_agent_run_is_allowed_per_project() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let first_event = insert_event(&test.db, "project-a", "first-run", 100);
    let second_event = insert_event(&test.db, "project-a", "second-run", 101);
    let connection = test.db.connect().unwrap();

    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, log_path
             ) VALUES (?1, ?2, NULL, 'starting', ?3, ?4)",
            params!["project-a", first_event, 100, "/tmp/agent-1.log"],
        )
        .unwrap();
    assert!(connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, log_path
             ) VALUES (?1, ?2, NULL, 'running', ?3, ?4)",
            params!["project-a", second_event, 101, "/tmp/agent-2.log"],
        )
        .is_err());

    connection
        .execute(
            "INSERT INTO agent_runs (
                project_id, primary_event_id, pid, status, started_at, finished_at, log_path
             ) VALUES (?1, ?2, NULL, 'completed', ?3, ?4, ?5)",
            params!["project-a", second_event, 101, 102, "/tmp/agent-2.log"],
        )
        .unwrap();
}

#[test]
fn typed_repositories_round_trip_future_task_records() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let event_id = insert_event(&test.db, "project-a", "typed-records", 100);

    let submission_repository = SubmissionRepository::new(&test.db);
    let submission = NewSubmission::new(
        "submission-1",
        "project-a",
        vec!["python".to_owned(), "train.py".to_owned()],
        100,
    );
    let inserted_submission = submission_repository
        .insert_idempotent(&submission)
        .unwrap();
    let duplicate_submission = submission_repository
        .insert_idempotent(&NewSubmission {
            status: SubmissionStatus::Accepted,
            ..submission.clone()
        })
        .unwrap();
    assert_eq!(
        inserted_submission.submission_id,
        duplicate_submission.submission_id
    );
    assert_eq!(duplicate_submission.status, SubmissionStatus::Pending);
    assert_eq!(
        submission_repository
            .find_by_id("submission-1")
            .unwrap()
            .unwrap()
            .argv,
        vec!["python".to_owned(), "train.py".to_owned()]
    );

    let agent_run_repository = AgentRunRepository::new(&test.db);
    let run = agent_run_repository
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            100,
            "/tmp/agent.log",
        ))
        .unwrap();
    assert_eq!(
        agent_run_repository
            .find_active_by_project("project-a")
            .unwrap()
            .unwrap()
            .run_id,
        run.run_id
    );
    agent_run_repository
        .attach_event(run.run_id, event_id)
        .unwrap();

    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "typed-fingerprint",
            100,
        ))
        .unwrap()
        .incident;
    let termination_repository = TerminationRequestRepository::new(&test.db);
    let request = NewTerminationRequest::new(
        incident.incident_id,
        "project-a",
        "signature-a",
        "fatal pattern",
        100,
        Some(110),
    );
    let inserted_request = termination_repository.insert_idempotent(&request).unwrap();
    let duplicate_request = termination_repository.insert_idempotent(&request).unwrap();
    assert_eq!(inserted_request.request_id, duplicate_request.request_id);
    assert_eq!(
        duplicate_request.status,
        TerminationRequestStatus::Requested
    );
    assert_eq!(
        termination_repository
            .find_by_id(inserted_request.request_id)
            .unwrap()
            .unwrap(),
        inserted_request
    );

    let observation_repository = TaskObservationRepository::new(&test.db);
    let observation = NewTaskObservation::new(
        "project-a",
        "signature-a",
        41,
        "pa-project",
        vec!["python".to_owned(), "train.py".to_owned()],
        "running",
        None,
        Some(101),
        None,
        None,
        101,
    );
    observation_repository.upsert(&observation).unwrap();
    let updated_observation = NewTaskObservation {
        state: "finished".to_owned(),
        observed_at: 102,
        ..observation
    };
    let stored = observation_repository.upsert(&updated_observation).unwrap();
    assert_eq!(stored.state, "finished");
    assert_eq!(
        observation_repository
            .find("project-a", "signature-a")
            .unwrap()
            .unwrap()
            .observed_at,
        102
    );
}

#[test]
fn all_event_kind_and_status_values_round_trip_through_sqlite() {
    let test = TestDatabase::new();
    let connection = test.db.connect().unwrap();
    for kind in [
        EventKind::TaskFinished,
        EventKind::TaskFailed,
        EventKind::Crash,
        EventKind::Stalled,
        EventKind::DeepCheck,
        EventKind::AutoKilled,
        EventKind::TerminationFailed,
    ] {
        let value: EventKind = connection
            .query_row("SELECT ?1", [kind], |row| row.get(0))
            .unwrap();
        assert_eq!(value, kind);
    }

    for status in [
        EventStatus::Pending,
        EventStatus::Claimed,
        EventStatus::Completed,
        EventStatus::RetryWait,
        EventStatus::Failed,
    ] {
        let value: EventStatus = connection
            .query_row("SELECT ?1", [status], |row| row.get(0))
            .unwrap();
        assert_eq!(value, status);
    }
}
