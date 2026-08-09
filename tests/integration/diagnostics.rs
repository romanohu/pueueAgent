use std::fs;

use pueue_agent::{
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, ProjectRepository,
        TerminationRequestRepository,
    },
    diagnostics::render_project_status_json,
    models::{
        AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent, NewIncident, NewProject,
        NewTerminationRequest, TerminationRequestStatus,
    },
    pueue::PueueTask,
    service::ServiceStatus,
    status::{PueueSnapshot, StatusInput},
};
use serde_json::{json, Value};
use tempfile::TempDir;

struct DiagnosticsHarness {
    _temp: TempDir,
    db: Db,
}

impl DiagnosticsHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("project");
        fs::create_dir_all(&root).unwrap();
        let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
        ProjectRepository::new(&db)
            .register(&NewProject::new(
                "project-a",
                &root,
                "pa-project",
                root.join(".pueue-agent/config.toml"),
                100,
            ))
            .unwrap();
        Self { _temp: temp, db }
    }

    fn project(&self) -> pueue_agent::models::Project {
        ProjectRepository::new(&self.db)
            .find_by_id("project-a")
            .unwrap()
            .unwrap()
    }

    fn input(&self, pueue: PueueSnapshot) -> StatusInput {
        StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue,
        }
    }
}

#[test]
fn status_json_projects_bounded_diagnostics_without_payloads_or_transcripts() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "fixture-event",
            json!({"transcript": "hidden transcript payload", "raw": {"large": "payload"}}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db)
        .transition_many(
            &[event.event_id],
            EventStatus::Failed,
            102,
            None,
            Some("agent command failed after a bounded diagnostic"),
        )
        .unwrap();
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "fatal-pattern",
            Some("task-signature"),
            "fixture-incident",
            103,
        ))
        .unwrap()
        .incident;
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "task-signature",
            "hidden termination payload",
            104,
            None,
        ))
        .unwrap();
    TerminationRequestRepository::new(&harness.db)
        .update_result(
            request.request_id,
            TerminationRequestStatus::Failed,
            None,
            Some("Pueue kill failed because the task already exited"),
        )
        .unwrap();
    AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event.event_id,
            Some(4321),
            AgentRunStatus::Running,
            105,
            "/tmp/hidden-codex-transcript.log",
        ))
        .unwrap();

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: format!("python train.py --token secret{}", "x".repeat(400)),
            state: "Running".to_owned(),
            enqueued_at: Some("100".to_owned()),
            started_at: Some("101".to_owned()),
            ended_at: None,
            result: None,
        }])),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["project"]["project_id"], "project-a");
    assert_eq!(value["daemon"]["status"], "running");
    assert_eq!(value["pueue"]["status"], "ok");
    assert_eq!(value["pueue"]["active_task_count"], 1);
    assert_eq!(value["events"]["counts"]["failed"], 1);
    assert_eq!(value["events"]["recent"][0]["event_id"], event.event_id);
    assert_eq!(value["incidents"]["counts"]["open"], 1);
    assert_eq!(
        value["incidents"]["recent"][0]["incident_id"],
        incident.incident_id
    );
    assert_eq!(value["termination"]["counts"]["failed"], 1);
    assert_eq!(
        value["termination"]["recent"][0]["request_id"],
        request.request_id
    );
    assert_eq!(value["agent_runs"]["counts"]["active"], 1);
    assert_eq!(value["agent_runs"]["recent"][0]["status"], "running");
    assert_eq!(value["policy"]["status"], "not_configured");
    assert_eq!(value["resource"]["status"], "not_configured");
    assert!(value["events"]["recent"][0].get("payload").is_none());
    assert!(value["termination"]["recent"][0].get("reason").is_none());
    assert!(value["agent_runs"]["recent"][0]
        .get("context_lineage")
        .is_none());
    assert!(
        value["pueue"]["active_tasks"][0]["command_summary"]
            .as_str()
            .unwrap()
            .len()
            <= 240
    );
    assert!(!rendered.contains("hidden transcript payload"));
    assert!(!rendered.contains("hidden termination payload"));
    assert!(!rendered.contains("hidden-codex-transcript"));
}

#[test]
fn status_json_marks_pueue_errors_without_reporting_idle_tasks() {
    let harness = DiagnosticsHarness::new();

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Error("Pueue status unavailable".to_owned())),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(value["pueue"]["status"], "error");
    assert_eq!(value["pueue"]["error_summary"], "Pueue status unavailable");
    assert!(value["pueue"].get("active_task_count").is_none());
    assert!(value["pueue"].get("active_tasks").is_none());
}
