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
    diagnostics::{EventFilter, MAX_EVENT_LIST_LIMIT},
    models::{
        AgentRunStatus, EventKind, EventStatus, IncidentStatus, IncidentTransition, NewAgentRun,
        NewEvent, NewIncident, NewProject, NewSubmission, NewTaskObservation,
        NewTerminationRequest, SubmissionStatus, TerminationRequestStatus,
    },
};
use rusqlite::{params, Connection};
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

fn create_legacy_schema_without_active_agent_index(path: &Path, version: i64) {
    let connection = Connection::open(path).unwrap();
    connection
        .execute_batch(&format!(
            r#"
            CREATE TABLE projects (
                project_id TEXT PRIMARY KEY,
                root_path TEXT NOT NULL UNIQUE,
                pueue_group TEXT NOT NULL UNIQUE,
                config_path TEXT NOT NULL,
                enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
                paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
                halted_reason TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE events (
                event_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                kind TEXT NOT NULL CHECK (kind IN (
                    'task_finished', 'task_failed', 'crash', 'stalled',
                    'deep_check', 'auto_killed', 'termination_failed'
                )),
                dedup_key TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN (
                    'pending', 'claimed', 'completed', 'retry_wait', 'failed'
                )),
                attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                not_before INTEGER NOT NULL,
                lease_until INTEGER,
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                last_error TEXT,
                UNIQUE(project_id, dedup_key),
                UNIQUE(project_id, event_id),
                CHECK (
                    (status = 'claimed' AND lease_until IS NOT NULL)
                    OR (status <> 'claimed' AND lease_until IS NULL)
                )
            );

            CREATE TABLE agent_runs (
                run_id INTEGER PRIMARY KEY,
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                primary_event_id INTEGER NOT NULL,
                pid INTEGER,
                status TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                finished_at INTEGER,
                exit_code INTEGER,
                log_path TEXT NOT NULL,
                last_error TEXT,
                UNIQUE(project_id, run_id),
                FOREIGN KEY(project_id, primary_event_id)
                    REFERENCES events(project_id, event_id) ON DELETE RESTRICT
            );

            CREATE TABLE agent_run_events (
                project_id TEXT NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
                run_id INTEGER NOT NULL,
                event_id INTEGER NOT NULL,
                PRIMARY KEY(run_id, event_id),
                FOREIGN KEY(project_id, run_id)
                    REFERENCES agent_runs(project_id, run_id) ON DELETE CASCADE,
                FOREIGN KEY(project_id, event_id)
                    REFERENCES events(project_id, event_id) ON DELETE RESTRICT
            );

            PRAGMA user_version = {version};
            "#
        ))
        .unwrap();
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
        "integration_events",
        "incidents",
        "agent_runs",
        "agent_run_events",
        "submissions",
        "termination_requests",
        "task_observations",
        "operator_logs",
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
fn concurrent_first_opens_apply_migration_once() {
    let temp = TempDir::new().unwrap();
    let path = Arc::new(temp.path().join("fresh.sqlite3"));
    let barrier = Arc::new(Barrier::new(8));
    let handles = (0..8)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = Arc::clone(&path);
            thread::spawn(move || {
                barrier.wait();
                Db::open(path.as_ref()).map(|_| ())
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let db = Db::open(&path).unwrap();
    let connection = db.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 5);
}

#[test]
fn schema_v4_migration_preserves_termination_requests_and_adds_dispatching_status() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "v4-migration",
            100,
        ))
        .unwrap()
        .incident;
    let request = TerminationRequestRepository::new(&test.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "migration test",
            100,
            Some(220),
        ))
        .unwrap();
    test.db
        .connect()
        .unwrap()
        .execute("PRAGMA user_version = 4", [])
        .unwrap();

    let migrated = Db::open(&test.path).unwrap();
    let connection = migrated.connect().unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 5);
    connection
        .execute(
            "UPDATE termination_requests SET status = 'dispatching' WHERE request_id = ?1",
            [request.request_id],
        )
        .unwrap();
    let stored_status: TerminationRequestStatus = connection
        .query_row(
            "SELECT status FROM termination_requests WHERE request_id = ?1",
            [request.request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_status, TerminationRequestStatus::Dispatching);
}

#[test]
fn legacy_migrations_create_active_agent_unique_index() {
    for version in [1, 2] {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join(format!("legacy-v{version}.sqlite3"));
        create_legacy_schema_without_active_agent_index(&path, version);

        let db = Db::open(&path).unwrap();
        let connection = db.connect().unwrap();
        let migrated_version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        let index_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'agent_runs_one_active_per_project_idx'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(migrated_version, 5);
        assert_eq!(index_count, 1);
        drop(connection);

        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        register_project(&db, "project-a", &root, "pa-project");
        let first_event = insert_event(&db, "project-a", "first-run", 100);
        let second_event = insert_event(&db, "project-a", "second-run", 101);
        let connection = db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO agent_runs (
                    project_id, primary_event_id, pid, status, started_at, log_path
                 ) VALUES (?1, ?2, NULL, 'starting', ?3, ?4)",
                params!["project-a", first_event, 100, "/tmp/agent-1.log"],
            )
            .unwrap();
        assert!(
            connection
                .execute(
                    "INSERT INTO agent_runs (
                        project_id, primary_event_id, pid, status, started_at, log_path
                     ) VALUES (?1, ?2, NULL, 'running', ?3, ?4)",
                    params!["project-a", second_event, 101, "/tmp/agent-2.log"],
                )
                .is_err(),
            "legacy v{version} migration must enforce one active agent per project"
        );
    }
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
fn agent_run_insert_with_events_persists_the_run_and_all_event_links() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let primary_event_id = insert_event(&test.db, "project-a", "primary-event", 100);
    let related_event_id = insert_event(&test.db, "project-a", "related-event", 101);

    let run = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                primary_event_id,
                None,
                AgentRunStatus::Starting,
                102,
                "/tmp/agent.log",
            ),
            &[primary_event_id, related_event_id],
        )
        .unwrap();

    let connection = test.db.connect().unwrap();
    let mut statement = connection
        .prepare(
            "SELECT event_id FROM agent_run_events
             WHERE run_id = ?1 ORDER BY event_id",
        )
        .unwrap();
    let event_ids = statement
        .query_map([run.run_id], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(event_ids, vec![primary_event_id, related_event_id]);
}

#[test]
fn agent_run_insert_with_events_rolls_back_when_an_event_attachment_is_invalid() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let primary_event_id = insert_event(&test.db, "project-a", "primary-event", 100);
    let cross_project_event_id = insert_event(&test.db, "project-b", "cross-project-event", 101);

    let error = AgentRunRepository::new(&test.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                primary_event_id,
                None,
                AgentRunStatus::Starting,
                102,
                "/tmp/agent.log",
            ),
            &[primary_event_id, cross_project_event_id],
        )
        .unwrap_err();
    assert!(error.to_string().contains("attach event to agent run"));

    let connection = test.db.connect().unwrap();
    let run_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| row.get(0))
        .unwrap();
    let attachment_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM agent_run_events", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(run_count, 0);
    assert_eq!(attachment_count, 0);
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
fn diagnostics_filters_events_by_project_kind_status_and_bounded_deterministic_order() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let repository = EventRepository::new(&test.db);

    let task_failed = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "task-failed",
            json!({"task_id": 41}),
            100,
            100,
        ))
        .unwrap();
    let first_crash = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "first-crash",
            json!({"task_id": 41}),
            200,
            200,
        ))
        .unwrap();
    let second_crash = repository
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "second-crash",
            json!({"task_id": 41}),
            200,
            200,
        ))
        .unwrap();
    repository
        .insert_idempotent(&NewEvent::new(
            "project-b",
            EventKind::Crash,
            "foreign-crash",
            json!({"task_id": 41}),
            300,
            300,
        ))
        .unwrap();
    repository
        .transition_many(
            &[first_crash.event_id],
            EventStatus::Completed,
            400,
            None,
            None,
        )
        .unwrap();

    let crashes = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(Some(EventKind::Crash), None, 10),
        )
        .unwrap();
    assert_eq!(
        crashes
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        vec![second_crash.event_id, first_crash.event_id]
    );
    assert!(crashes
        .iter()
        .all(|event| event.project_id == "project-a" && event.kind == EventKind::Crash));

    let completed = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(None, Some(EventStatus::Completed), 10),
        )
        .unwrap();
    assert_eq!(
        completed
            .iter()
            .map(|event| event.event_id)
            .collect::<Vec<_>>(),
        vec![first_crash.event_id]
    );
    assert_ne!(task_failed.status, EventStatus::Completed);

    for index in 0..=MAX_EVENT_LIST_LIMIT {
        repository
            .insert_idempotent(&NewEvent::new(
                "project-a",
                EventKind::Stalled,
                format!("bounded-crash-{index}"),
                json!({"task_id": index}),
                1,
                1,
            ))
            .unwrap();
    }
    let bounded = repository
        .list_filtered(
            "project-a",
            &EventFilter::new(None, None, MAX_EVENT_LIST_LIMIT + 1),
        )
        .unwrap();
    assert_eq!(bounded.len(), MAX_EVENT_LIST_LIMIT);
    assert!(repository
        .list_filtered("project-a", &EventFilter::new(None, None, 0))
        .unwrap()
        .is_empty());
}

#[test]
fn diagnostics_scopes_task_relations_by_project_and_stable_signature() {
    let test = TestDatabase::new();
    let project_a_root = test.project_root("project-a");
    let project_b_root = test.project_root("project-b");
    register_project(&test.db, "project-a", &project_a_root, "pa-a");
    register_project(&test.db, "project-b", &project_b_root, "pa-b");
    let observations = TaskObservationRepository::new(&test.db);

    for (project_id, signature, group, observed_at) in [
        ("project-a", "signature-old", "pa-a", 100),
        ("project-a", "signature-alpha", "pa-a", 200),
        ("project-a", "signature-beta", "pa-a", 200),
        ("project-b", "signature-beta", "pa-b", 300),
    ] {
        observations
            .upsert(&NewTaskObservation::new(
                project_id,
                signature,
                41,
                group,
                vec!["python".to_owned(), "train.py".to_owned()],
                "running",
                None,
                Some(observed_at),
                None,
                None,
                observed_at,
            ))
            .unwrap();
    }
    assert_eq!(
        observations
            .find_by_pueue_task("project-a", 41, 10)
            .unwrap()
            .iter()
            .map(|observation| observation.task_signature.as_str())
            .collect::<Vec<_>>(),
        vec!["signature-beta", "signature-alpha", "signature-old"]
    );

    let submissions = SubmissionRepository::new(&test.db);
    for (submission_id, project_id, signature) in [
        ("submission-old", "project-a", "signature-old"),
        ("submission-beta", "project-a", "signature-beta"),
        ("submission-foreign", "project-b", "signature-beta"),
    ] {
        submissions
            .insert_idempotent(&NewSubmission::new(
                submission_id,
                project_id,
                vec!["python".to_owned(), "train.py".to_owned()],
                100,
            ))
            .unwrap();
        submissions
            .mark_accepted(submission_id, 41, signature)
            .unwrap();
    }
    assert_eq!(
        submissions
            .find_by_task_signature("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-beta"]
    );
    assert!(submissions
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|submission| submission.project_id == "project-a"));

    let incidents = IncidentRepository::new(&test.db);
    let incident_old = incidents
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("signature-old"),
            "old-fingerprint",
            100,
        ))
        .unwrap()
        .incident;
    let incident_beta = incidents
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("signature-beta"),
            "beta-fingerprint",
            200,
        ))
        .unwrap()
        .incident;
    let foreign_incident = incidents
        .upsert_active(&NewIncident::new(
            "project-b",
            "pattern",
            Some("signature-beta"),
            "foreign-fingerprint",
            300,
        ))
        .unwrap()
        .incident;
    assert_eq!(
        incidents
            .find_by_task_key("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|incident| incident.incident_id)
            .collect::<Vec<_>>(),
        vec![incident_beta.incident_id]
    );
    assert!(incidents
        .find_by_project_and_id("project-a", foreign_incident.incident_id)
        .unwrap()
        .is_none());
    assert!(incidents
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|incident| incident.project_id == "project-a"));

    let terminations = TerminationRequestRepository::new(&test.db);
    for (incident_id, project_id, signature, requested_at) in [
        (incident_old.incident_id, "project-a", "signature-old", 100),
        (
            incident_beta.incident_id,
            "project-a",
            "signature-beta",
            200,
        ),
        (
            foreign_incident.incident_id,
            "project-b",
            "signature-beta",
            300,
        ),
    ] {
        terminations
            .insert_idempotent(&NewTerminationRequest::new(
                incident_id,
                project_id,
                signature,
                "diagnostic relation",
                requested_at,
                None,
            ))
            .unwrap();
    }
    assert_eq!(
        terminations
            .find_by_task_signature("project-a", "signature-beta", 10)
            .unwrap()
            .iter()
            .map(|request| request.incident_id)
            .collect::<Vec<_>>(),
        vec![incident_beta.incident_id]
    );
    assert!(terminations
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .all(|request| request.project_id == "project-a"));

    let event_a = insert_event(&test.db, "project-a", "agent-run-a", 100);
    let event_b = insert_event(&test.db, "project-b", "agent-run-b", 100);
    let agent_runs = AgentRunRepository::new(&test.db);
    let first_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_a,
            None,
            AgentRunStatus::Starting,
            100,
            "/tmp/agent-a-first.log",
        ))
        .unwrap();
    agent_runs
        .finish(
            first_run.run_id,
            AgentRunStatus::Completed,
            101,
            Some(0),
            None,
        )
        .unwrap();
    let second_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-a",
            event_a,
            None,
            AgentRunStatus::Starting,
            200,
            "/tmp/agent-a-second.log",
        ))
        .unwrap();
    agent_runs
        .finish(
            second_run.run_id,
            AgentRunStatus::Completed,
            201,
            Some(0),
            None,
        )
        .unwrap();
    agent_runs.attach_event(second_run.run_id, event_a).unwrap();
    let foreign_run = agent_runs
        .insert(&NewAgentRun::new(
            "project-b",
            event_b,
            None,
            AgentRunStatus::Starting,
            300,
            "/tmp/agent-b.log",
        ))
        .unwrap();
    assert_eq!(
        agent_runs
            .list_by_project("project-a", 1)
            .unwrap()
            .iter()
            .map(|run| run.run_id)
            .collect::<Vec<_>>(),
        vec![second_run.run_id]
    );
    assert!(!agent_runs
        .list_by_project("project-a", 10)
        .unwrap()
        .iter()
        .any(|run| run.run_id == foreign_run.run_id));
    assert_eq!(
        agent_runs
            .find_by_event("project-a", event_a, 10)
            .unwrap()
            .iter()
            .map(|run| run.run_id)
            .collect::<Vec<_>>(),
        vec![second_run.run_id]
    );
}

#[test]
fn submission_repository_tracks_acceptance_and_unreconciled_rows() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let repository = SubmissionRepository::new(&test.db);

    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-pending",
            "project-a",
            vec!["python".to_owned(), "pending.py".to_owned()],
            100,
        ))
        .unwrap();
    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-failed",
            "project-a",
            vec!["python".to_owned(), "failed.py".to_owned()],
            101,
        ))
        .unwrap();
    repository
        .transition_status("submission-failed", SubmissionStatus::Failed)
        .unwrap();
    repository
        .insert_idempotent(&NewSubmission::new(
            "submission-accepted",
            "project-a",
            vec!["python".to_owned(), "accepted.py".to_owned()],
            102,
        ))
        .unwrap();
    let accepted = repository
        .mark_accepted("submission-accepted", 41, "project-a:41")
        .unwrap();
    assert_eq!(accepted.status, SubmissionStatus::Accepted);
    assert_eq!(accepted.pueue_task_id, Some(41));
    assert_eq!(accepted.task_signature.as_deref(), Some("project-a:41"));

    let initially_unreconciled = repository.find_unreconciled("project-a").unwrap();
    assert_eq!(
        initially_unreconciled
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-pending"]
    );

    repository
        .transition_status("submission-accepted", SubmissionStatus::Unreconciled)
        .unwrap();
    let unreconciled = repository.find_unreconciled("project-a").unwrap();
    assert_eq!(
        unreconciled
            .iter()
            .map(|submission| submission.submission_id.as_str())
            .collect::<Vec<_>>(),
        vec!["submission-pending", "submission-accepted"]
    );

    repository
        .transition_status("submission-pending", SubmissionStatus::Adopted)
        .unwrap();
    assert_eq!(repository.find_unreconciled("project-a").unwrap().len(), 2);
}

#[test]
fn termination_request_repository_transitions_and_filters_pending_requests() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "termination-lifecycle",
            100,
        ))
        .unwrap()
        .incident;
    let repository = TerminationRequestRepository::new(&test.db);

    let request = repository
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "fatal pattern",
            100,
            Some(110),
        ))
        .unwrap();
    let sent = repository
        .transition_status(request.request_id, TerminationRequestStatus::Sent)
        .unwrap();
    assert_eq!(sent.status, TerminationRequestStatus::Sent);
    assert_eq!(repository.find_pending("project-a").unwrap().len(), 1);

    let confirmed = repository
        .update_result(
            request.request_id,
            TerminationRequestStatus::Confirmed,
            Some(120),
            None,
        )
        .unwrap();
    assert_eq!(confirmed.status, TerminationRequestStatus::Confirmed);
    assert_eq!(confirmed.confirmed_at, Some(120));
    assert!(repository.find_pending("project-a").unwrap().is_empty());
}

#[test]
fn expired_dispatch_claim_cannot_complete_a_reclaimed_request() {
    let test = TestDatabase::new();
    let root = test.project_root("project");
    register_project(&test.db, "project-a", &root, "pa-project");
    let incident = IncidentRepository::new(&test.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "pattern",
            Some("task-a"),
            "dispatch-lease",
            100,
        ))
        .unwrap()
        .incident;
    let repository = TerminationRequestRepository::new(&test.db);
    let request = repository
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "signature-a",
            "fatal pattern",
            100,
            None,
        ))
        .unwrap();
    let first_claim = repository
        .claim_for_dispatch(request.request_id, 100, 200)
        .unwrap()
        .unwrap();
    let second_claim = repository
        .claim_for_dispatch(request.request_id, 201, 301)
        .unwrap()
        .unwrap();

    assert!(repository
        .mark_dispatched_if_current(
            request.request_id,
            first_claim.dispatch_lease_until.unwrap(),
            320
        )
        .unwrap()
        .is_none());
    assert!(repository
        .finish_dispatch_if_current(
            request.request_id,
            first_claim.dispatch_lease_until.unwrap(),
            TerminationRequestStatus::Failed,
            Some("stale failure"),
        )
        .unwrap()
        .is_none());
    let sent = repository
        .mark_dispatched_if_current(
            request.request_id,
            second_claim.dispatch_lease_until.unwrap(),
            321,
        )
        .unwrap()
        .unwrap();
    assert_eq!(sent.status, TerminationRequestStatus::Sent);
    assert_eq!(sent.grace_until, Some(321));
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
