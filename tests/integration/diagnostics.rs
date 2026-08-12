use std::fs;

#[cfg(unix)]
use std::os::unix::fs::symlink;

use pueue_agent::{
    cli::Cli,
    db::{
        AgentRunRepository, Db, EventRepository, IncidentRepository, InterventionRepository,
        ProjectRepository, TaskObservationRepository, TerminationRequestRepository,
        LATEST_SCHEMA_VERSION,
    },
    diagnostics::{
        build_doctor_report, render_doctor_report, render_doctor_report_value, render_events,
        render_incident_explanation, render_project_status_json, render_task_inspection,
        DoctorExternal, EventFilter, MAX_EVENT_LIST_LIMIT,
    },
    models::{
        AgentRunStatus, EventKind, EventStatus, NewAgentRun, NewEvent, NewIncident, NewProject,
        NewTaskObservation, NewTerminationRequest, TerminationRequestStatus,
    },
    output::redact_sensitive_text,
    pueue::PueueTask,
    service::{ServicePaths, ServiceStatus},
    status::{render_project_status, render_project_status_compact, PueueSnapshot, StatusInput},
};
use clap::Parser;
use rusqlite::params;
use serde_json::{json, Value};
use tempfile::TempDir;

#[test]
fn bounded_redaction_removes_bare_provider_tokens_but_keeps_normal_reason() {
    let value = pueue_agent::output::bounded_redacted_text(
        "inspect ghp_abcdefghijklmnopqrstuvwxyz123456 and sk-abcdefghijklmnopqrstuvwxyz123456",
    );
    assert!(!value.contains("ghp_abcdefghijklmnopqrstuvwxyz123456"));
    assert!(!value.contains("sk-abcdefghijklmnopqrstuvwxyz123456"));
    assert!(
        pueue_agent::output::bounded_redacted_text("inspect current loss")
            .contains("inspect current loss")
    );
}

fn canonical_state_json() -> Value {
    json!({
        "schema_version": 1,
        "current_facts": ["campaign active"],
        "historical_facts": ["campaign started"],
        "next_action": "inspect current loss",
        "budgets": {
            "max_experiments": 3,
            "max_agent_runs": 4,
            "max_consecutive_failures": 2
        },
        "active_lineage": {
            "event_id": 17,
            "run_id": 23,
            "submission_ids": ["submission-1"],
            "task_ids": [41]
        }
    })
}

fn doctor_paths(harness: &DiagnosticsHarness) -> ServicePaths {
    ServicePaths {
        release_binary: std::path::PathBuf::from("/missing/pueue-agent"),
        pueue_config: std::path::PathBuf::from("/missing/pueue.yml"),
        state_dir: std::path::PathBuf::from("/state"),
        working_dir: harness.project().root_path,
        path_env: "/usr/bin:/bin".to_owned(),
    }
}

fn doctor_external() -> DoctorExternal {
    DoctorExternal {
        pueue: Ok(Vec::new()),
        service: Ok(ServiceStatus::Stopped),
        callback: Ok(None),
    }
}

fn state_check<'a>(value: &'a Value, name: &str) -> &'a Value {
    value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == name)
        .unwrap_or_else(|| panic!("missing doctor check {name}: {value}"))
}

#[test]
fn canonical_state_doctor_rejects_duplicate_current_facts() {
    let harness = DiagnosticsHarness::new();
    let mut state = canonical_state_json();
    state["current_facts"] = json!(["campaign active", "campaign active"]);
    fs::create_dir_all(harness.project().root_path.join(".pueue-agent")).unwrap();
    fs::write(
        harness.project().root_path.join(".pueue-agent/state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");
    assert_eq!(check["status"], "error");
    assert!(check["summary"].as_str().unwrap().contains("duplicate"));
}

#[test]
fn canonical_state_doctor_rejects_invalid_budget_values_and_oversized_state() {
    let harness = DiagnosticsHarness::new();
    fs::create_dir_all(harness.project().root_path.join(".pueue-agent")).unwrap();
    let mut state = canonical_state_json();
    state["budgets"]["max_experiments"] = json!(-1);
    fs::write(
        harness.project().root_path.join(".pueue-agent/state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let invalid_budget = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let invalid_budget: Value = serde_json::from_str(&invalid_budget).unwrap();
    assert_eq!(
        state_check(&invalid_budget, "state.schema")["status"],
        "error"
    );

    state = canonical_state_json();
    state["current_facts"] = json!(["x".repeat(70_000)]);
    fs::write(
        harness.project().root_path.join(".pueue-agent/state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
    let oversized = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let oversized: Value = serde_json::from_str(&oversized).unwrap();
    assert_eq!(state_check(&oversized, "state.schema")["status"], "error");
}

#[test]
fn canonical_state_doctor_rejects_duplicate_lineage_ids() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    let mut state = canonical_state_json();
    state["active_lineage"]["submission_ids"] = json!(["submission-1", "submission-1"]);
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");
    assert_eq!(check["status"], "error");
    assert!(check["summary"].as_str().unwrap().contains("duplicate"));

    state = canonical_state_json();
    state["active_lineage"]["task_ids"] = json!([41, 41]);
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(state_check(&value, "state.schema")["status"], "error");
}

#[test]
fn canonical_state_doctor_rejects_normalized_current_fact_duplicates() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    let mut state = canonical_state_json();
    state["current_facts"] = json!([" Campaign ACTIVE ", "campaign  active"]);
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");
    assert_eq!(check["status"], "error");
    assert!(check["summary"].as_str().unwrap().contains("duplicate"));
}

#[test]
fn canonical_state_doctor_rejects_unknown_budget_keys() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    let mut state = canonical_state_json();
    state["budgets"]["unexpected_budget"] = json!(1);
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");
    assert_eq!(check["status"], "error");
    assert!(check["summary"].as_str().unwrap().contains("unknown"));
}

#[cfg(unix)]
#[test]
fn canonical_state_doctor_reports_dangling_symlink_as_error() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    symlink(
        state_dir.join("missing-state-target"),
        state_dir.join("state.json"),
    )
    .unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");
    assert_eq!(check["status"], "error");
    assert!(!check["summary"].as_str().unwrap().contains("missing"));
}

#[test]
fn canonical_state_doctor_reports_state_directory_as_error() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    fs::create_dir(state_dir.join("state.json")).unwrap();

    let rendered = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let check = state_check(&value, "state.schema");

    assert_eq!(check["status"], "error");
    assert!(!check["summary"].as_str().unwrap().contains("missing"));
}

#[test]
fn canonical_state_doctor_projects_summary_and_state_md_contradiction_as_warning() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&canonical_state_json()).unwrap(),
    )
    .unwrap();
    fs::write(state_dir.join("STATE.md"), "campaign stopped\n").unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    let json = render_doctor_report_value(&report, true).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let consistency = state_check(&value, "state.consistency");
    assert_eq!(consistency["status"], "warning");
    assert!(consistency["summary"]
        .as_str()
        .unwrap()
        .contains("campaign stopped"));
    assert!(consistency["summary"]
        .as_str()
        .unwrap()
        .contains("active lineage"));

    let text = render_doctor_report_value(&report, false).unwrap();
    assert!(text.contains("state.consistency: warning"));
    assert!(text.contains("current_facts=1"));
    assert!(text.contains("active_lineage=1"));
    assert!(!text.contains("prompt"));
    assert!(!text.contains("transcript"));
}

#[test]
fn canonical_state_doctor_uses_normalized_current_fact_for_consistency() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    let mut state = canonical_state_json();
    state["current_facts"] = json!(["  Campaign ACTIVE  "]);
    state["active_lineage"] = json!({
        "event_id": null,
        "run_id": null,
        "submission_ids": [],
        "task_ids": []
    });
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();
    fs::write(state_dir.join("STATE.md"), "## Campaign Stopped\n").unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    let value: Value =
        serde_json::from_str(&render_doctor_report_value(&report, true).unwrap()).unwrap();

    let consistency = state_check(&value, "state.consistency");
    assert_eq!(consistency["status"], "warning");
}

#[test]
fn canonical_state_doctor_ignores_historical_markdown_prose() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&canonical_state_json()).unwrap(),
    )
    .unwrap();
    fs::write(
        state_dir.join("STATE.md"),
        "The previous campaign stopped after an old run; the current work is active.\n",
    )
    .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    let value: Value =
        serde_json::from_str(&render_doctor_report_value(&report, true).unwrap()).unwrap();

    assert_eq!(state_check(&value, "state.consistency")["status"], "ok");
}

#[test]
fn canonical_state_doctor_reports_actual_sqlite_schema_version_in_json_and_text() {
    let harness = DiagnosticsHarness::new();
    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    let json = render_doctor_report_value(&report, true).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let schema = state_check(&value, "schema.version");
    assert_eq!(schema["status"], "ok");
    assert!(schema["summary"]
        .as_str()
        .unwrap()
        .contains(&LATEST_SCHEMA_VERSION.to_string()));
    assert!(render_doctor_report_value(&report, false)
        .unwrap()
        .contains("schema.version: ok"));
}

#[test]
fn cli_output_contract_status_has_header_ids_states_summary_and_pure_json() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "status-output-contract",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    let input = harness.input(PueueSnapshot::Tasks(vec![PueueTask {
        id: 41,
        group: "pa-project".to_owned(),
        command: "python train.py --lr 0.001".to_owned(),
        state: "Running".to_owned(),
        enqueued_at: Some("100".to_owned()),
        started_at: Some("101".to_owned()),
        ended_at: None,
        result: None,
    }]));

    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    assert!(human.starts_with("pueue-agent status"), "{human}");
    assert!(human.contains("task=41"), "{human}");
    assert!(
        human.contains(&format!("event={}", event.event_id)),
        "{human}"
    );
    assert!(human
        .split_whitespace()
        .any(|field| field == "state=running"));
    assert!(human.contains("summary:"), "{human}");

    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();
    assert!(json.starts_with('{'), "{json}");
    assert!(!json.contains("pueue-agent"), "{json}");
    assert!(!json.contains('\x1b'), "{json}");
    let _: Value = serde_json::from_str(&json).unwrap();
}

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
fn redact_sensitive_text_consumes_spaced_assignment_values_as_whole_credentials() {
    let rendered = redact_sensitive_text(
        r#"run AWS_SECRET_ACCESS_KEY = "first second" password = 'third fourth' --password = fifth --api-key = "six seven" --lr 0.001"#,
    );

    for leaked in [
        "first second",
        "third fourth",
        "fifth",
        "six seven",
        "second\"",
        "fourth'",
    ] {
        assert!(
            !rendered.contains(leaked),
            "spaced assignment leaked: {rendered}"
        );
    }
    assert_eq!(
        rendered,
        "run AWS_SECRET_ACCESS_KEY = [REDACTED] password = [REDACTED] --password = [REDACTED] --api-key = [REDACTED] --lr 0.001"
    );
}

#[test]
fn redact_sensitive_text_consumes_independent_assignment_values_until_boundary() {
    let rendered =
        redact_sensitive_text("run CREDENTIAL = secret Authorization = Basic secret --lr 0.001");

    for leaked in ["secret", "Basic"] {
        assert!(
            !rendered.contains(leaked),
            "independent assignment leaked: {rendered}"
        );
    }
    assert_eq!(
        rendered,
        "run CREDENTIAL = [REDACTED] Authorization = [REDACTED] --lr 0.001"
    );
}

#[test]
fn redact_sensitive_text_keeps_equals_inside_quoted_assignment_values_redacted() {
    let rendered = redact_sensitive_text(r#"CREDENTIAL = "secret=still-secret" --lr 0.001"#);

    assert!(!rendered.contains("secret=still-secret"));
    assert_eq!(rendered, "CREDENTIAL = [REDACTED] --lr 0.001");
}

#[test]
fn redact_sensitive_text_keeps_equals_inside_unquoted_assignment_values_redacted() {
    let rendered = redact_sensitive_text("AWS_SECRET_ACCESS_KEY = abc=def --lr 0.001");
    assert!(!rendered.contains("abc=def"));
    assert_eq!(rendered, "AWS_SECRET_ACCESS_KEY = [REDACTED] --lr 0.001");

    let chained =
        redact_sensitive_text("AWS_SECRET_ACCESS_KEY = abc=def password = second --lr 0.001");
    assert!(!chained.contains("abc=def"));
    assert!(!chained.contains("second"));
    assert_eq!(
        chained,
        "AWS_SECRET_ACCESS_KEY = [REDACTED] password = [REDACTED] --lr 0.001"
    );

    let quoted = redact_sensitive_text(r#"AWS_SECRET_ACCESS_KEY = "abc=def" --lr 0.001"#);
    assert!(!quoted.contains("abc=def"));
    assert_eq!(quoted, "AWS_SECRET_ACCESS_KEY = [REDACTED] --lr 0.001");

    assert_eq!(
        redact_sensitive_text("run --lr 0.001 scale=abc"),
        "run --lr 0.001 scale=abc"
    );
}

#[test]
fn redact_sensitive_text_redacts_inline_assignments_until_argument_boundary() {
    let rendered =
        redact_sensitive_text("Authorization=Bearer secret --lr 0.001 AWS_SECRET=foo bar --x");

    for secret in ["Bearer", "secret", "foo", "bar"] {
        assert!(
            !rendered.contains(secret),
            "inline assignment leaked {secret}: {rendered}"
        );
    }
    assert_eq!(
        rendered,
        "Authorization=[REDACTED] --lr 0.001 AWS_SECRET=[REDACTED] --x"
    );

    let json = redact_sensitive_text(r#"curl -H '{"Authorization":"Bearer SECRET"}' --lr 0.001"#);
    assert!(!json.contains("SECRET"));
    assert!(json.contains("--lr 0.001"));

    let colon = redact_sensitive_text("Authorization: Bearer secret --lr 0.001");
    assert_eq!(colon, "Authorization: Bearer [REDACTED] --lr 0.001");
}

#[test]
fn redact_sensitive_text_redacts_structured_quoted_authorization_headers() {
    let cases = [
        r#"curl -H '{"Authorization":"Bearer SECRET"}'"#,
        r#"curl -H '{"Authorization": "Bearer SECRET"}'"#,
        r#"curl -H {"Authorization": Basic SECRET}"#,
        r#"curl -H {"credential": first second}"#,
    ];

    for input in cases {
        let rendered = redact_sensitive_text(input);
        for secret in ["SECRET", "first", "second"] {
            assert!(
                !rendered.contains(secret),
                "structured credential leaked {secret}: {rendered}"
            );
        }
    }
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
fn redact_sensitive_session_id_field_preserves_label_but_hides_values() {
    let inline = redact_sensitive_text("agent.context.session_id=REALVALUE");
    assert!(!inline.contains("REALVALUE"));
    assert!(inline.contains("[REDACTED]"));

    let spaced_assignment = redact_sensitive_text("agent.context.session_id = REALVALUE");
    assert!(spaced_assignment.contains("agent.context.session_id"));
    assert!(spaced_assignment.contains("[REDACTED]"));
    assert!(!spaced_assignment.contains("REALVALUE"));

    let next_token = redact_sensitive_text("agent.context.session_id REALVALUE");
    assert!(next_token.contains("agent.context.session_id"));
    assert!(next_token.contains("[REDACTED]"));
    assert!(!next_token.contains("REALVALUE"));

    let punctuation = redact_sensitive_text("agent.context.session_id: REALVALUE");
    assert!(punctuation.contains("agent.context.session_id:"));
    assert!(punctuation.contains("[REDACTED]"));
    assert!(!punctuation.contains("REALVALUE"));

    let label_only = redact_sensitive_text("invalid field `agent.context.session_id`");
    assert!(label_only.contains("agent.context.session_id"));
    assert!(!label_only.contains("REALVALUE"));
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
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'in_flight' WHERE event_id = ?1",
            [event.event_id],
        )
        .unwrap();

    let retry_wait = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "intervention-status-retry-wait",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db)
        .transition_many(
            &[retry_wait.event_id],
            EventStatus::RetryWait,
            106,
            Some(200),
            Some("retry wait detail"),
        )
        .unwrap();
    let dispatched = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "intervention-status-dispatched",
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
            "UPDATE events SET status = 'dispatched' WHERE event_id = ?1",
            [dispatched.event_id],
        )
        .unwrap();
    let dead_letter = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "intervention-status-dead-letter",
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
            "UPDATE events SET status = 'dead_letter', completed_at = ?2,
                    last_error = ?3 WHERE event_id = ?1",
            params![dead_letter.event_id, 106, "dead letter detail"],
        )
        .unwrap();

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
    assert_eq!(value["events"]["counts"]["in_flight"], 1);
    assert_eq!(value["events"]["counts"]["dispatched"], 1);
    assert_eq!(value["events"]["counts"]["retry_wait"], 1);
    assert_eq!(value["events"]["counts"]["dead_letter"], 1);
    assert!(!rendered.contains(&pending.message));
    assert!(!rendered.contains(&reserved.message));
    assert!(!rendered.contains(&applied.message));
}

#[test]
fn status_human_and_compact_counts_include_ack_states() {
    let harness = DiagnosticsHarness::new();
    let retry_wait = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "status-human-retry-wait",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db)
        .transition_many(
            &[retry_wait.event_id],
            EventStatus::RetryWait,
            100,
            Some(200),
            None,
        )
        .unwrap();
    let in_flight = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "status-human-in-flight",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    let dispatched = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "status-human-dispatched",
            json!({}),
            100,
            100,
        ))
        .unwrap();
    let dead_letter = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "status-human-dead-letter",
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
            "UPDATE events SET status = CASE event_id
                WHEN ?1 THEN 'in_flight'
                WHEN ?2 THEN 'dispatched'
                WHEN ?3 THEN 'dead_letter'
             END
             WHERE event_id IN (?1, ?2, ?3)",
            params![in_flight.event_id, dispatched.event_id, dead_letter.event_id],
        )
        .unwrap();

    let input = harness.input(PueueSnapshot::Tasks(vec![]));
    for rendered in [
        render_project_status(&harness.db, &harness.project(), &input).unwrap(),
        render_project_status_compact(&harness.db, &harness.project(), &input).unwrap(),
    ] {
        let event_line = rendered
            .lines()
            .find(|line| line.starts_with("events: "))
            .unwrap();
        for field in [
            "retry_wait=1",
            "in_flight=1",
            "dispatched=1",
            "failed=0",
            "dead_letter=1",
        ] {
            assert!(event_line.contains(field), "missing {field}: {event_line}");
        }
    }
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
fn compact_status_bounds_and_redacts_project_id() {
    let harness = DiagnosticsHarness::new();
    let mut project = harness.project();
    project.project_id = format!(
        "AWS_SECRET_ACCESS_KEY=COMPACT_PROJECT_SECRET {}",
        "project".repeat(400)
    );

    let rendered = render_project_status_compact(
        &harness.db,
        &project,
        &harness.input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();

    let project_line = rendered
        .lines()
        .find(|line| line.starts_with("pueue-agent status project="))
        .unwrap();
    assert!(project_line.len() <= "pueue-agent status project=".len() + 243);
    assert!(!project_line.contains("COMPACT_PROJECT_SECRET"));
}

#[test]
fn status_human_bounds_and_redacts_guardrail_config_error() {
    let harness = DiagnosticsHarness::new();
    let mut project = harness.project();
    let config_path = harness._temp.path().join("invalid-config.toml");
    fs::write(
        &config_path,
        "[guardrails]\nmax_agent_runs = \"GUARDRAIL_SECRET --password hidden\"\n",
    )
    .unwrap();
    project.config_path = config_path;

    let rendered = pueue_agent::status::render_project_status(
        &harness.db,
        &project,
        &harness.input(PueueSnapshot::Tasks(vec![])),
    )
    .unwrap();

    let guardrail_line = rendered
        .lines()
        .find(|line| line.starts_with("guardrails: error: "))
        .unwrap();
    assert!(guardrail_line.len() <= "guardrails: error: ".len() + 243);
    assert!(!guardrail_line.contains("GUARDRAIL_SECRET"));
    assert!(!guardrail_line.contains("hidden"));
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

    let project = harness.project();
    let expected_root_path = project.root_path.to_string_lossy().into_owned();
    let rendered = render_project_status_json(
        &harness.db,
        &project,
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
    assert_eq!(value["project"]["root_path"], expected_root_path);
    assert_eq!(value["project"]["pueue_group"], "pa-project");
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
fn status_json_task_command_projection_redacts_structured_authorization_headers() {
    let harness = DiagnosticsHarness::new();
    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(vec![PueueTask {
            id: 41,
            group: "pa-project".to_owned(),
            command: r#"curl -H '{"Authorization":"Bearer SECRET"}' --lr 0.001"#.to_owned(),
            state: "Running".to_owned(),
            enqueued_at: None,
            started_at: None,
            ended_at: None,
            result: None,
        }])),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();

    assert_eq!(value["pueue"]["active_tasks"][0]["command_summary"], "curl");
    assert!(!rendered.contains("SECRET"));
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
            Some(&"unbounded internal failure detail ".repeat(100)),
        )
        .unwrap();
    let dispatched = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "diagnostic-dispatched",
            json!({"prompt": "hidden dispatched prompt"}),
            102,
            102,
        ))
        .unwrap();
    let claimed = EventRepository::new(&harness.db)
        .claim_batch(102, 202, 1)
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                dispatched.event_id,
                None,
                AgentRunStatus::Running,
                103,
                "/tmp/diagnostic-run.log",
            ),
            &[dispatched.event_id],
        )
        .unwrap();
    let dispatch_error = format!(
        "dispatch failed --token hidden-token \x1b[31m{}\x1b[0m",
        "detail ".repeat(100)
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dispatched', attempts = 2,
                last_error = ?2, not_before = 123
             WHERE event_id = ?1",
            params![dispatched.event_id, dispatch_error],
        )
        .unwrap();
    let dead_letter = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "diagnostic-dead-letter",
            json!({"prompt": "hidden dead-letter prompt"}),
            103,
            103,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', completed_at = ?2,
                    last_error = ?3 WHERE event_id = ?1",
            params![dead_letter.event_id, 104, &"dead-letter error ".repeat(100)],
        )
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
    assert!(events[0]["last_error"].as_str().unwrap().len() <= 240);
    assert!(!rendered.contains("hidden"));
    assert!(!rendered.contains(&"unbounded internal failure detail ".repeat(100)));

    let rendered = render_events(
        &harness.db,
        &harness.project(),
        &EventFilter::new(None, Some(EventStatus::DeadLetter), 10),
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let dead_letter_projection = &value["events"][0];
    assert_eq!(dead_letter_projection["status"], "dead_letter");
    assert_eq!(dead_letter_projection["attempts"], 0);
    assert_eq!(dead_letter_projection["not_before"], 103);
    assert!(dead_letter_projection["last_error"].as_str().unwrap().len() <= 240);
    assert_eq!(dead_letter_projection["run_id"], Value::Null);
    assert!(!rendered.contains("hidden dead-letter prompt"));
    assert!(!rendered.contains("dead-letter error ".repeat(100).as_str()));

    let rendered = render_events(
        &harness.db,
        &harness.project(),
        &EventFilter::new(None, Some(EventStatus::Dispatched), 10),
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let dispatched_projection = &value["events"][0];
    assert_eq!(dispatched_projection["run_id"], run.run_id);
    assert_eq!(dispatched_projection["attempts"], 2);
    assert_eq!(dispatched_projection["not_before"], 123);
    assert!(dispatched_projection["last_error"].as_str().unwrap().len() <= 240);
    assert!(!rendered.contains("hidden dispatched prompt"));
    assert!(!rendered.contains("hidden-token"));
    assert!(!rendered.contains('\x1b'));
    assert!(
        dispatched_projection["last_error"]
            .as_str()
            .unwrap()
            .contains("[REDACTED]")
    );

    assert!(Cli::try_parse_from(["pueue-agent", "events", "--status", "dead-letter"]).is_ok());
    assert!(Cli::try_parse_from(["pueue-agent", "events", "--status", "dead_letter"]).is_err());
}

#[test]
fn latest_run_id_is_project_scoped_and_deterministic() {
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

    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "latest-run-project-a",
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
            "UPDATE events SET status = 'claimed', lease_until = 300 WHERE event_id = ?1",
            [event.event_id],
        )
        .unwrap();
    let first = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Completed,
                200,
                "/tmp/latest-first.log",
            ),
            &[event.event_id],
        )
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'claimed', lease_until = 300 WHERE event_id = ?1",
            [event.event_id],
        )
        .unwrap();
    let second = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Completed,
                200,
                "/tmp/latest-second.log",
            ),
            &[event.event_id],
        )
        .unwrap();
    assert!(second.run_id > first.run_id);
    assert_eq!(
        EventRepository::new(&harness.db)
            .latest_run_id("project-a", event.event_id)
            .unwrap(),
        Some(second.run_id)
    );
    assert_eq!(
        EventRepository::new(&harness.db)
            .latest_run_id("project-b", event.event_id)
            .unwrap(),
        None
    );
    assert_eq!(
        EventRepository::new(&harness.db)
            .latest_run_id("project-a", 999_999)
            .unwrap(),
        None
    );
    let bulk = EventRepository::new(&harness.db)
        .latest_run_ids("project-a", &[event.event_id, event.event_id, 999_999])
        .unwrap();
    assert_eq!(bulk.get(&event.event_id), Some(&second.run_id));
    assert!(!bulk.contains_key(&999_999));
    assert!(EventRepository::new(&harness.db)
        .latest_run_ids("project-b", &[event.event_id])
        .unwrap()
        .is_empty());
    assert!(EventRepository::new(&harness.db)
        .latest_run_ids("project-a", &[])
        .unwrap()
        .is_empty());

    let overflow_event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "latest-run-overflow-event",
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
            "UPDATE events SET status = 'claimed', lease_until = 300 WHERE event_id = ?1",
            [overflow_event.event_id],
        )
        .unwrap();
    let overflow_run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                overflow_event.event_id,
                None,
                AgentRunStatus::Completed,
                300,
                "/tmp/latest-overflow.log",
            ),
            &[overflow_event.event_id],
        )
        .unwrap();
    let mut duplicate_heavy_request = vec![event.event_id; MAX_EVENT_LIST_LIMIT];
    duplicate_heavy_request.push(overflow_event.event_id);
    let duplicate_heavy = EventRepository::new(&harness.db)
        .latest_run_ids("project-a", &duplicate_heavy_request)
        .unwrap();
    assert_eq!(
        duplicate_heavy.get(&overflow_event.event_id),
        Some(&overflow_run.run_id)
    );
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
        let run = AgentRunRepository::new(&harness.db)
            .insert(&NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Completed,
                200 + run_number,
                format!("/tmp/bounded-agent-history-{run_number}.log"),
            ))
            .unwrap();
        AgentRunRepository::new(&harness.db)
            .attach_event(run.run_id, event.event_id)
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
fn doctor_reports_dead_letter_ack_consistency_and_restart_uncertainty_without_repair() {
    let harness = DiagnosticsHarness::new();
    let dead_letter = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "doctor-dead-letter",
            json!({"prompt": "hidden doctor prompt"}),
            100,
            100,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', completed_at = ?2,
                    last_error = ?3 WHERE event_id = ?1",
            params![
                dead_letter.event_id,
                101,
                format!(
                    "restart_interruption: execution outcome unknown --password hidden \x1b[31m{}\x1b[0m",
                    "reason ".repeat(100)
                )
            ],
        )
        .unwrap();
    let pre_marker_dead_letter = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "doctor-pre-marker-dead-letter",
            json!({}),
            101,
            101,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'dead_letter', completed_at = ?2,
                    last_error = ?3 WHERE event_id = ?1",
            params![
                pre_marker_dead_letter.event_id,
                102,
                "restart_interruption: pre-marker execution not confirmed (retry limit)"
            ],
        )
        .unwrap();
    let unlinked = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "doctor-unlinked-in-flight",
            json!({"prompt": "hidden in-flight prompt"}),
            102,
            102,
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE events SET status = 'in_flight', lease_until = NULL WHERE event_id = ?1",
            [unlinked.event_id],
        )
        .unwrap();

    let before_dead_letter = EventRepository::new(&harness.db)
        .find_by_id(dead_letter.event_id)
        .unwrap()
        .unwrap();
    let before_unlinked = EventRepository::new(&harness.db)
        .find_by_id(unlinked.event_id)
        .unwrap()
        .unwrap();
    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        200,
    )
    .unwrap();

    let ack_consistency = report
        .checks
        .iter()
        .find(|check| check.name == "events.ack_consistency")
        .unwrap();
    assert_eq!(ack_consistency.status, pueue_agent::diagnostics::DoctorCheckStatus::Error);
    let dead_letter_check = report
        .checks
        .iter()
        .find(|check| check.name == "events.dead_letter")
        .unwrap();
    assert_eq!(dead_letter_check.status, pueue_agent::diagnostics::DoctorCheckStatus::Warning);
    assert!(dead_letter_check.remediation.contains("dead-letter"));
    let restart_uncertain = report
        .checks
        .iter()
        .find(|check| check.name == "events.restart_uncertain")
        .unwrap();
    assert_eq!(restart_uncertain.status, pueue_agent::diagnostics::DoctorCheckStatus::Warning);
    assert!(restart_uncertain.summary.contains("1 restart-uncertain"));
    assert!(restart_uncertain.summary.contains("restart_interruption"));
    assert!(restart_uncertain.summary.contains("execution outcome unknown"));
    assert!(!restart_uncertain.summary.contains("hidden"));
    assert!(!restart_uncertain.summary.contains("doctor prompt"));
    assert!(!restart_uncertain.summary.contains("pre-marker"));
    assert!(restart_uncertain.summary.len() <= 240);
    assert!(!restart_uncertain.summary.chars().any(char::is_control));

    let after_dead_letter = EventRepository::new(&harness.db)
        .find_by_id(dead_letter.event_id)
        .unwrap()
        .unwrap();
    let after_unlinked = EventRepository::new(&harness.db)
        .find_by_id(unlinked.event_id)
        .unwrap()
        .unwrap();
    assert_eq!(before_dead_letter.status, after_dead_letter.status);
    assert_eq!(before_dead_letter.lease_until, after_dead_letter.lease_until);
    assert_eq!(before_unlinked.status, after_unlinked.status);
    assert_eq!(before_unlinked.lease_until, after_unlinked.lease_until);
}

#[test]
fn doctor_reports_missing_submission_kind_or_origin_indexes() {
    let harness = DiagnosticsHarness::new();
    harness
        .db
        .connect()
        .unwrap()
        .execute("DROP INDEX submissions_project_origin_agent_run_idx", [])
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
        100,
        true,
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let index_check = value["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|check| check["name"] == "schema.indexes")
        .unwrap();
    assert_eq!(index_check["status"], "error");
}

#[test]
fn doctor_reports_missing_and_present_event_status_not_before_index() {
    let harness = DiagnosticsHarness::new();
    let present = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    assert_eq!(
        present
            .checks
            .iter()
            .find(|check| check.name == "schema.indexes")
            .unwrap()
            .status,
        pueue_agent::diagnostics::DoctorCheckStatus::Ok
    );

    harness
        .db
        .connect()
        .unwrap()
        .execute("DROP INDEX events_project_status_not_before_idx", [])
        .unwrap();
    let missing = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
    )
    .unwrap();
    assert_eq!(
        missing
            .checks
            .iter()
            .find(|check| check.name == "schema.indexes")
            .unwrap()
            .status,
        pueue_agent::diagnostics::DoctorCheckStatus::Error
    );
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
