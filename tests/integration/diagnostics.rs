use std::fs;

use pueue_agent::{
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, InterventionRepository,
        ProjectRepository, TaskObservationRepository, TerminationRequestRepository,
    },
    diagnostics::{
        render_doctor_report, render_events, render_incident_explanation,
        render_project_status_json, render_task_inspection, DoctorExternal, EventFilter,
    },
    models::{
        AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent, NewIncident, NewProject,
        NewTaskObservation, NewTerminationRequest, TerminationRequestStatus,
    },
    output::redact_sensitive_text,
    pueue::PueueTask,
    service::{ServicePaths, ServiceStatus},
    status::{render_project_status_compact, PueueSnapshot, StatusInput},
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
fn redact_sensitive_text_removes_flag_bearer_and_credential_values() {
    let rendered = redact_sensitive_text(
        "train --token very-secret --access-token separated-token \
         --access-token=equal-token --api-key abc123 --password hunter2 --secret hidden \
         Authorization: Bearer bearer-secret AWS_ACCESS_KEY_ID=AKIA123 \
         ACCESS_KEY=access-secret AWS_SECRET_ACCESS_KEY=environment-secret --lr 0.001",
    );

    for secret in [
        "very-secret",
        "separated-token",
        "equal-token",
        "abc123",
        "hunter2",
        "hidden",
        "bearer-secret",
        "AKIA123",
        "access-secret",
        "environment-secret",
    ] {
        assert!(
            !rendered.contains(secret),
            "redaction leaked {secret}: {rendered}"
        );
    }
    assert_eq!(
        rendered,
        "train --token [REDACTED] --access-token [REDACTED] --access-token=[REDACTED] --api-key [REDACTED] --password [REDACTED] --secret [REDACTED] Authorization: Bearer [REDACTED] AWS_ACCESS_KEY_ID=[REDACTED] ACCESS_KEY=[REDACTED] AWS_SECRET_ACCESS_KEY=[REDACTED] --lr 0.001"
    );
}

#[test]
fn redact_sensitive_text_consumes_quoted_values_as_whole_credentials() {
    let rendered = redact_sensitive_text(
        r#"run --password "first second" --prompt 'prompt first second' AWS_SECRET_ACCESS_KEY="secret first second" --lr 0.001"#,
    );

    for leaked in [
        "first second",
        "prompt first second",
        "secret first second",
        "second\"",
        "second'",
    ] {
        assert!(
            !rendered.contains(leaked),
            "quoted value leaked: {rendered}"
        );
    }
    assert_eq!(
        rendered,
        "run --password [REDACTED] --prompt [REDACTED] AWS_SECRET_ACCESS_KEY=[REDACTED] --lr 0.001"
    );
}

#[test]
fn bounded_redacted_text_removes_control_and_ansi_sequences_before_bounding() {
    let rendered =
        pueue_agent::output::bounded_redacted_text("prefix\x1b[31mhidden\x1b[0m\n\t\u{0007}suffix");

    assert_eq!(rendered, "prefixhidden suffix");
    assert!(!rendered.chars().any(char::is_control));
    assert!(!rendered.contains("[31m"));
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
fn compact_status_contains_only_bounded_operational_summaries() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "compact-status",
            json!({"prompt": "hidden prompt payload"}),
            100,
            100,
        ))
        .unwrap();
    AgentRunRepository::new(&harness.db)
        .insert_with_events_and_reservation(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Running,
                101,
                "/tmp/hidden-prompt.log",
            ),
            &[],
            None,
        )
        .unwrap();

    let rendered = render_project_status_compact(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: "python train.py --prompt hidden prompt payload".to_owned(),
            state: "running".to_owned(),
            enqueued_at: None,
            started_at: None,
            ended_at: None,
            result: None,
        }])),
    )
    .unwrap();

    for section in [
        "pueue-agent",
        "daemon:",
        "pueue:",
        "experiments:",
        "agent_runs:",
        "events:",
        "guardrails:",
        "summary:",
    ] {
        assert!(
            rendered.contains(section),
            "missing section {section}: {rendered}"
        );
    }
    for secret in [
        "hidden prompt payload",
        "python train.py",
        "/tmp/hidden-prompt.log",
    ] {
        assert!(
            !rendered.contains(secret),
            "compact output leaked {secret}: {rendered}"
        );
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

#[test]
fn status_json_bounds_and_sanitizes_pueue_timestamps() {
    let harness = DiagnosticsHarness::new();
    let oversized = format!("{}\n\t\x1b[31m", "timestamp-".repeat(80));
    let task = PueueTask {
        id: 77,
        group: "pa-project".to_owned(),
        command: "python job.py".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some(oversized.clone()),
        started_at: Some(oversized),
        ended_at: None,
        result: None,
    };

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![task])),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    for field in ["enqueued_at", "started_at"] {
        let timestamp = value["pueue"]["active_tasks"][0][field].as_str().unwrap();
        assert!(timestamp.len() <= 240);
        assert!(timestamp.chars().all(|character| !character.is_control()));
    }
}

#[test]
fn status_json_bounds_and_sanitizes_pueue_state() {
    let harness = DiagnosticsHarness::new();
    let state = format!("{}\n\t\x1b[31m", "running-".repeat(80));
    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![PueueTask {
            id: 78,
            group: "pa-project".to_owned(),
            command: "python job.py".to_owned(),
            state,
            enqueued_at: Some("1".to_owned()),
            started_at: Some("2".to_owned()),
            ended_at: None,
            result: None,
        }])),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let state = value["pueue"]["active_tasks"][0]["state"].as_str().unwrap();
    assert!(state.len() <= 240);
    assert!(state.chars().all(|character| !character.is_control()));
}

#[test]
fn events_projection_filters_project_events_and_emits_bounded_fields() {
    let harness = DiagnosticsHarness::new();
    let crash = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "diagnostic-crash",
            json!({"prompt": "hidden"}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db)
        .transition_many(
            &[crash.event_id],
            EventStatus::Failed,
            101,
            None,
            Some("unbounded internal failure detail"),
        )
        .unwrap();
    EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "diagnostic-finished",
            json!({}),
            102,
            102,
        ))
        .unwrap();

    let rendered = render_events(
        &harness.db,
        &harness.project(),
        &EventFilter::new(Some(EventKind::Crash), Some(EventStatus::Failed), 1),
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let events = value["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_id"], crash.event_id);
    assert_eq!(events[0]["kind"], "crash");
    assert_eq!(events[0]["status"], "failed");
    assert!(events[0]["error_summary"].as_str().unwrap().len() <= 240);
    assert!(!rendered.contains("hidden"));
    assert!(!rendered.contains("unbounded internal failure detail"));
}

#[test]
fn task_inspection_stays_project_scoped_and_keeps_stable_signature_history_together() {
    let harness = DiagnosticsHarness::new();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            "stable-signature-a",
            41,
            "pa-project",
            vec!["python".to_owned(), "train.py".to_owned()],
            "running",
            Some(10),
            Some(11),
            None,
            None,
            100,
        ))
        .unwrap();
    let foreign_root = harness._temp.path().join("project-b");
    fs::create_dir_all(&foreign_root).unwrap();
    ProjectRepository::new(&harness.db)
        .register(&NewProject::new(
            "project-b",
            &foreign_root,
            "pb-project",
            foreign_root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-b",
            "foreign-signature",
            41,
            "pb-project",
            vec!["python".to_owned(), "foreign.py".to_owned()],
            "running",
            Some(20),
            Some(21),
            None,
            None,
            120,
        ))
        .unwrap();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            "reused-signature-a",
            41,
            "pa-project",
            vec!["python".to_owned(), "old.py".to_owned()],
            "done",
            Some(1),
            Some(2),
            Some(3),
            Some("0".to_owned()),
            90,
        ))
        .unwrap();

    let rendered = render_task_inspection(&harness.db, &harness.project(), 41, true).unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(value["latest"]["task_signature"], "stable-signature-a");
    assert_eq!(value["history"].as_array().unwrap().len(), 1);
    assert_eq!(value["history"][0]["task_signature"], "stable-signature-a");
    assert!(!rendered.contains("reused-signature-a"));
    assert!(!rendered.contains("foreign-signature"));
    assert!(!rendered.contains("old.py"));
}

#[test]
fn task_inspection_bounds_agent_runs_before_accumulating_per_event_history() {
    let harness = DiagnosticsHarness::new();
    TaskObservationRepository::new(&harness.db)
        .upsert(&NewTaskObservation::new(
            "project-a",
            "bounded-agent-history",
            41,
            "pa-project",
            vec!["python".to_owned(), "train.py".to_owned()],
            "done",
            Some(10),
            Some(11),
            Some(12),
            Some("0".to_owned()),
            100,
        ))
        .unwrap();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "bounded-agent-history-event",
            json!({"task_signature": "bounded-agent-history"}),
            100,
            100,
        ))
        .unwrap();
    for run_number in 0..65 {
        AgentRunRepository::new(&harness.db)
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Completed,
                    200 + run_number,
                    format!("/tmp/bounded-agent-history-{run_number}.log"),
                ),
                &[event.event_id],
            )
            .unwrap();
    }

    let rendered = render_task_inspection(&harness.db, &harness.project(), 41, true).unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    assert!(value["agent_runs"].as_array().unwrap().len() <= 64);
}

#[test]
fn incident_explanation_is_deterministic_and_rejects_unknown_incidents() {
    let harness = DiagnosticsHarness::new();
    let incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-a",
            "oom",
            Some("stable-signature-a"),
            "diagnostic-explain",
            100,
        ))
        .unwrap()
        .incident;
    EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            format!("incident-wake:v1:incident={}", incident.incident_id),
            json!({"incident_id": incident.incident_id, "task_signature": "stable-signature-a"}),
            101,
            101,
        ))
        .unwrap();
    let request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            incident.incident_id,
            "project-a",
            "stable-signature-a",
            "kill after repeated failure",
            102,
            None,
        ))
        .unwrap();

    let foreign_root = harness._temp.path().join("project-b");
    fs::create_dir_all(&foreign_root).unwrap();
    ProjectRepository::new(&harness.db)
        .register(&NewProject::new(
            "project-b",
            &foreign_root,
            "pb-project",
            foreign_root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    let foreign_incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-b",
            "foreign",
            Some("foreign-signature"),
            "foreign-explain",
            103,
        ))
        .unwrap()
        .incident;

    let rendered =
        render_incident_explanation(&harness.db, &harness.project(), incident.incident_id, true)
            .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let chain = value["chain"].as_array().unwrap();
    let stages = chain
        .iter()
        .map(|step| step["stage"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        stages,
        vec![
            "observation",
            "incident_transition",
            "event",
            "policy",
            "approval",
            "pueue_action"
        ]
    );
    assert_eq!(value["policy"]["status"], "not_configured");
    assert_eq!(value["approval"]["status"], "not_configured");
    assert_eq!(value["pueue_action"][0]["request_id"], request.request_id);
    assert!(render_incident_explanation(&harness.db, &harness.project(), 999_999, true).is_err());
    assert!(render_incident_explanation(
        &harness.db,
        &harness.project(),
        foreign_incident.incident_id,
        true
    )
    .is_err());
}

#[test]
fn doctor_projection_reports_unavailable_integrations_as_errors_without_repairing_leases() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFailed,
            "expired-doctor-event",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'claimed', lease_until = 99 WHERE event_id = ?1",
            [event.event_id],
        )
        .unwrap();

    let paths = ServicePaths {
        release_binary: std::path::PathBuf::from("/missing/pueue-agent"),
        pueue_config: std::path::PathBuf::from("/missing/pueue.yml"),
        state_dir: std::path::PathBuf::from("/state"),
        working_dir: harness.project().root_path,
        path_env: "/usr/bin:/bin".to_owned(),
    };
    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &paths,
        DoctorExternal {
            pueue: Err("Pueue unavailable".to_owned()),
            service: Err("service status unavailable".to_owned()),
            callback: Err("callback config unavailable".to_owned()),
        },
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let checks = value["checks"].as_array().unwrap();
    assert!(checks.iter().any(|check| check["status"] == "error"));
    assert!(checks.iter().any(|check| check["name"] == "pueue.status"));
    assert!(checks.iter().any(|check| check["name"] == "leases.expired"));
    let event_after = EventRepository::new(&harness.db)
        .find_by_id(event.event_id)
        .unwrap()
        .unwrap();
    assert_eq!(event_after.status, EventStatus::Claimed);
    assert_eq!(event_after.lease_until, Some(99));
}

#[test]
fn doctor_expired_lease_check_is_scoped_to_the_requested_project() {
    let harness = DiagnosticsHarness::new();
    let foreign_root = harness._temp.path().join("project-b");
    fs::create_dir_all(&foreign_root).unwrap();
    ProjectRepository::new(&harness.db)
        .register(&NewProject::new(
            "project-b",
            &foreign_root,
            "pb-project",
            foreign_root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    let foreign_event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-b",
            EventKind::TaskFailed,
            "foreign-expired-event",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'claimed', lease_until = 99 WHERE event_id = ?1",
            [foreign_event.event_id],
        )
        .unwrap();
    InterventionRepository::new(&harness.db)
        .insert_pending("project-b", "foreign expired intervention", 100)
        .unwrap();
    InterventionRepository::new(&harness.db)
        .reserve_pending("project-b", "foreign-intervention-token", 100, 199, 1, 1024)
        .unwrap();
    let foreign_incident = IncidentRepository::new(&harness.db)
        .upsert_active(&NewIncident::new(
            "project-b",
            "foreign",
            Some("foreign-task"),
            "foreign-doctor",
            100,
        ))
        .unwrap()
        .incident;
    let foreign_request = TerminationRequestRepository::new(&harness.db)
        .insert_idempotent(&NewTerminationRequest::new(
            foreign_incident.incident_id,
            "project-b",
            "foreign-task",
            "foreign request",
            100,
            None,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE termination_requests SET dispatch_lease_until = 99 WHERE request_id = ?1",
            [foreign_request.request_id],
        )
        .unwrap();

    let paths = ServicePaths {
        release_binary: std::path::PathBuf::from("/missing/pueue-agent"),
        pueue_config: std::path::PathBuf::from("/missing/pueue.yml"),
        state_dir: std::path::PathBuf::from("/state"),
        working_dir: harness.project().root_path,
        path_env: "/usr/bin:/bin".to_owned(),
    };
    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &paths,
        DoctorExternal {
            pueue: Ok(Vec::new()),
            service: Ok(ServiceStatus::Stopped),
            callback: Ok(None),
        },
        200,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let lease_check = value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "leases.expired")
        .unwrap();
    assert_eq!(lease_check["status"], "ok");
}
