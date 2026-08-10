use std::fs;

use pueue_agent::{
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, InterventionRepository,
        ProjectRepository, TerminationRequestRepository,
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
fn status_json_counts_project_interventions_without_exposing_message_bodies() {
    let harness = DiagnosticsHarness::new();
    let interventions = InterventionRepository::new(&harness.db);
    let reserved = interventions
        .insert_pending("project-a", "hidden prompt-like value reserved", 100)
        .unwrap();
    let applied = interventions
        .insert_pending("project-a", "hidden prompt-like value applied", 101)
        .unwrap();
    let pending = interventions
        .insert_pending("project-a", "hidden prompt-like value pending", 102)
        .unwrap();
    interventions
        .reserve_pending("project-a", "reserved-token", 103, 203, 1, 4 * 1024)
        .unwrap();
    interventions
        .reserve_pending("project-a", "applied-token", 104, 204, 1, 4 * 1024)
        .unwrap();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "intervention-status-counts",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Starting,
                105,
                "/tmp/intervention-status-counts.log",
            ),
            &[],
            Some("applied-token"),
        )
        .unwrap();
    assert_eq!(
        interventions
            .mark_applied_for_run("project-a", run.run_id, 106)
            .unwrap(),
        1
    );

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(value["interventions"]["counts"]["pending"], 1);
    assert_eq!(value["interventions"]["counts"]["reserved"], 1);
    assert_eq!(value["interventions"]["counts"]["applied"], 1);
    assert!(!rendered.contains(&pending.message));
    assert!(!rendered.contains(&reserved.message));
    assert!(!rendered.contains(&applied.message));
}

#[test]
fn status_json_projects_bounded_diagnostics_without_payloads_or_transcripts() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "fixture-event",
            json!({
                "prompt": "hidden prompt payload",
                "transcript": "hidden transcript payload",
                "log_path": "/tmp/hidden-event.log",
                "raw": {"large": "payload"}
            }),
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
            Some("short failure"),
        )
        .unwrap();
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "é".repeat(200),
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
            Some("Pueue kill failed at /Users/secret/project --token very-secret prompt hidden"),
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
    let incident_kind = value["incidents"]["recent"][0]["kind"].as_str().unwrap();
    assert!(incident_kind.len() <= 240);
    assert!(incident_kind.is_char_boundary(incident_kind.len()));
    assert!(incident_kind.ends_with("..."));
    assert_eq!(value["termination"]["counts"]["failed"], 1);
    assert_eq!(
        value["termination"]["recent"][0]["request_id"],
        request.request_id
    );
    assert_eq!(value["agent_runs"]["counts"]["active"], 1);
    assert_eq!(value["agent_runs"]["recent"][0]["status"], "running");
    assert_eq!(value["policy"], json!({}));
    assert_eq!(value["resource"], json!({}));
    assert!(value["events"]["recent"][0].get("payload").is_none());
    assert!(value["termination"]["recent"][0].get("reason").is_none());
    assert!(value["agent_runs"]["recent"][0]
        .get("context_lineage")
        .is_none());
    assert_eq!(
        value["pueue"]["active_tasks"][0]["command_summary"],
        "python"
    );
    assert_eq!(
        value["events"]["recent"][0]["error_category"],
        "event_processing"
    );
    assert_eq!(
        value["termination"]["recent"][0]["error_category"],
        "termination_dispatch"
    );
    assert_ne!(
        value["events"]["recent"][0]["error_summary"],
        "short failure"
    );
    for summary in [
        &value["events"]["recent"][0]["error_summary"],
        &value["termination"]["recent"][0]["error_summary"],
    ] {
        let summary = summary.as_str().unwrap();
        assert!(summary.len() <= 240);
        assert!(!summary.contains("very-secret"));
        assert!(!summary.contains("/Users/secret"));
        assert!(!summary.contains("prompt hidden"));
    }
    assert!(!rendered.contains("hidden transcript payload"));
    assert!(!rendered.contains("hidden prompt payload"));
    assert!(!rendered.contains("hidden-event.log"));
    assert!(!rendered.contains("hidden termination payload"));
    assert!(!rendered.contains("hidden-codex-transcript"));
    assert!(!rendered.contains("very-secret"));
    assert!(!rendered.contains("/Users/secret"));
}

#[test]
fn status_json_marks_pueue_errors_without_reporting_idle_tasks() {
    let harness = DiagnosticsHarness::new();

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Error(format!(
            "Pueue status failed at /Users/secret --token very-secret {}",
            "é".repeat(200)
        ))),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(value["pueue"]["status"], "error");
    assert_eq!(value["pueue"]["error_category"], "pueue_status");
    let error_summary = value["pueue"]["error_summary"].as_str().unwrap();
    assert!(error_summary.len() <= 240);
    assert!(error_summary.is_char_boundary(error_summary.len()));
    assert_eq!(error_summary, "Pueue status command failed");
    assert!(!error_summary.contains("very-secret"));
    assert!(!error_summary.contains("/Users/secret"));
    assert!(value["pueue"].get("active_task_count").is_none());
    assert!(value["pueue"].get("active_tasks").is_none());
}

#[test]
fn status_json_limits_active_tasks_after_deterministic_sorting() {
    let harness = DiagnosticsHarness::new();
    let tasks = (1..=12)
        .map(|task_id| PueueTask {
            id: task_id,
            group: "pa-project".to_owned(),
            command: format!("TOKEN=abc123-{task_id} /opt/secret/python --token secret-{task_id}"),
            state: "Running".to_owned(),
            enqueued_at: Some(task_id.to_string()),
            started_at: Some(task_id.to_string()),
            ended_at: None,
            result: None,
        })
        .collect();

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(tasks)),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let active_tasks = value["pueue"]["active_tasks"].as_array().unwrap();

    assert_eq!(value["pueue"]["active_task_count"], 12);
    assert_eq!(value["pueue"]["returned_count"], 8);
    assert_eq!(value["pueue"]["truncated"], true);
    assert_eq!(active_tasks.len(), 8);
    assert_eq!(active_tasks[0]["task_id"], 12);
    assert_eq!(active_tasks[7]["task_id"], 5);
    assert!(active_tasks
        .iter()
        .all(|task| task["command_summary"] == "python"));
    assert!(!rendered.contains("abc123"));
    assert!(!rendered.contains("/opt/secret"));
}

#[test]
fn status_json_hides_shell_wrappers_and_options_when_executable_is_ambiguous() {
    let harness = DiagnosticsHarness::new();
    let tasks = vec![
        PueueTask {
            id: 50,
            group: "pa-project".to_owned(),
            command: "env TOKEN=abc123 /opt/secret/python --token secret".to_owned(),
            state: "Running".to_owned(),
            enqueued_at: Some("50".to_owned()),
            started_at: Some("50".to_owned()),
            ended_at: None,
            result: None,
        },
        PueueTask {
            id: 51,
            group: "pa-project".to_owned(),
            command: "--shell /opt/secret/python --token secret".to_owned(),
            state: "Running".to_owned(),
            enqueued_at: Some("51".to_owned()),
            started_at: Some("51".to_owned()),
            ended_at: None,
            result: None,
        },
    ];

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(tasks)),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert!(value["pueue"]["active_tasks"]
        .as_array()
        .unwrap()
        .iter()
        .all(|task| task["command_summary"] == "unknown"));
    assert!(!rendered.contains("abc123"));
    assert!(!rendered.contains("/opt/secret"));
    assert!(!rendered.contains("secret"));
}
