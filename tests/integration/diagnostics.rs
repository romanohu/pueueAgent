use std::fs;

#[cfg(unix)]
use std::os::unix::fs::symlink;

use pueue_agent::{
    cli::Cli,
    db::{
        AgentRunRepository, CampaignRepository, Db, DecisionRepository, EventRepository,
        ExperimentRepository, IncidentRepository, InterventionRepository, ProjectRepository,
        StartCampaignRequest, TaskObservationRepository, TerminationRequestRepository,
        LATEST_SCHEMA_VERSION,
    },
    diagnostics::{
        build_doctor_report, build_doctor_report_with_policy, render_doctor_report,
        render_doctor_report_value, render_events,
        render_incident_explanation, render_project_status_json, render_task_inspection,
        DoctorCheckStatus, DoctorExternal, EventFilter, MAX_EVENT_LIST_LIMIT,
    },
    execution_policy::{
        CampaignLimits, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
        StartupEnvironment,
    },
    models::{
        AgentRunStatus, EventKind, EventStatus, ExecutionProjection, NewAgentRun, NewEvent,
        NewIncident, NewProject, NewTaskObservation, NewTerminationRequest, ProposalKind,
        TerminationRequestStatus,
    },
    output::redact_sensitive_text,
    pueue::PueueTask,
    proposals::{self, ProposalInput},
    service::{ServicePaths, ServiceStatus},
    status::{render_project_status, render_project_status_compact, PueueSnapshot, StatusInput},
    runs::render_runs,
};
use clap::Parser;
use rusqlite::params;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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
        "schema_version": 2,
        "current_facts": ["campaign active"],
        "historical_facts": ["campaign started"],
        "next_action": "inspect current loss",
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
        execution_policy: std::path::PathBuf::from("/state/execution-policy.toml"),
        working_dir: harness.project().root_path,
        home: std::path::PathBuf::from("/home/fixture"),
        codex_home: std::path::PathBuf::from("/home/fixture/.codex"),
        path_env: "/usr/bin:/bin".to_owned(),
        startup_environment: StartupEnvironment::default(),
    }
}

fn doctor_external() -> DoctorExternal {
    DoctorExternal {
        pueue: Ok(Vec::new()),
        service: Ok(ServiceStatus::Stopped),
        callback: Ok(None),
    }
}

#[test]
fn doctor_reports_fixed_pueue_bounds_without_output() {
    let harness = DiagnosticsHarness::new();
    let paths = doctor_paths(&harness);
    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &paths,
        doctor_external(),
        100,
    )
    .unwrap();

    let check = report
        .checks
        .iter()
        .find(|check| check.name == "pueue.bounds")
        .expect("doctor must report fixed Pueue process bounds");
    assert_eq!(check.status, DoctorCheckStatus::Ok);
    assert_eq!(
        check.summary,
        "Pueue commands use timeout=30s and independent stdout/stderr caps=65536 bytes"
    );
    assert!(!check.summary.contains("credential"));
    assert!(!check.summary.contains("fixture-output"));

    let rendered = render_doctor_report_value(&report, false).unwrap();
    assert!(rendered.contains(
        "Pueue commands use timeout=30s and independent stdout/stderr caps=65536 bytes"
    ));
    assert!(!rendered.contains("fixture-output"));
}

#[test]
fn missing_pueue_config_is_rendered_as_a_degraded_doctor_error() {
    let harness = DiagnosticsHarness::new();
    let policy = Err(PolicyViolation::new(
        PolicyViolationCode::AnchorMissing,
        PolicyViolationStage::Startup,
    ));
    let report = build_doctor_report_with_policy(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        &policy,
    )
    .unwrap();

    let check = report
        .checks
        .iter()
        .find(|check| check.name == "pueue.config")
        .expect("doctor must report an unavailable Pueue profile");
    assert_eq!(check.status, DoctorCheckStatus::Error);
}

#[test]
fn doctor_rejects_a_project_contained_pueue_config_with_anchor_semantics() {
    let harness = DiagnosticsHarness::new();
    let mut paths = doctor_paths(&harness);
    paths.pueue_config = harness.project().config_path;
    let policy = Err(PolicyViolation::new(
        PolicyViolationCode::PolicyMissing,
        PolicyViolationStage::Startup,
    ));
    let report = build_doctor_report_with_policy(
        &harness.db,
        &harness.project(),
        &paths,
        doctor_external(),
        100,
        &policy,
    )
    .unwrap();

    let check = report
        .checks
        .iter()
        .find(|check| check.name == "pueue.config")
        .expect("doctor must inspect the Pueue profile independently of policy availability");
    assert_eq!(check.status, DoctorCheckStatus::Error);
}

#[test]
fn doctor_rejects_a_pueue_config_inside_another_registered_project() {
    let harness = DiagnosticsHarness::new();
    let other_root = harness._temp.path().join("other-project");
    fs::create_dir_all(&other_root).unwrap();
    let other_config = other_root.join("pueue.yml");
    fs::write(&other_config, "fixture: true\n").unwrap();
    ProjectRepository::new(&harness.db)
        .register(&NewProject::new(
            "project-b",
            &other_root,
            "pa-project-b",
            other_root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    let mut paths = doctor_paths(&harness);
    paths.pueue_config = other_config;
    let policy = Err(PolicyViolation::new(
        PolicyViolationCode::PolicyMissing,
        PolicyViolationStage::Startup,
    ));
    let report = build_doctor_report_with_policy(
        &harness.db,
        &harness.project(),
        &paths,
        doctor_external(),
        100,
        &policy,
    )
    .unwrap();

    let check = report
        .checks
        .iter()
        .find(|check| check.name == "pueue.config")
        .expect("doctor must inspect against every registered project root");
    assert_eq!(check.status, DoctorCheckStatus::Error);
}

#[test]
fn doctor_pueue_bounds_source_derives_its_summary_from_production_constants() {
    let source = include_str!("../../src/diagnostics.rs");
    let summary_start = source
        .find("let pueue_bounds_summary = format!(")
        .expect("doctor must construct the Pueue bounds summary");
    let bounds_end = source[summary_start..]
        .find("checks.extend(execution_doctor_checks")
        .map(|offset| summary_start + offset)
        .expect("Pueue bounds check must precede execution policy diagnostics");
    let bounds = &source[summary_start..bounds_end];

    assert!(bounds.contains("PUEUE_TIMEOUT.as_secs()"));
    assert!(bounds.contains("MAX_PUEUE_OUTPUT_BYTES"));
    assert!(bounds.contains("doctor_ok_with_typed_summary("));
    assert!(bounds.contains("\"pueue.bounds\""));
    assert!(!bounds.contains("timeout=30s"));
    assert!(!bounds.contains("caps=65536 bytes"));
}

#[test]
fn doctor_execution_anchors_include_the_complete_pueue_boundary() {
    let source = include_str!("../../src/diagnostics.rs");
    let checks_start = source
        .find("fn execution_doctor_checks(")
        .expect("doctor must define execution boundary checks");
    let checks = &source[checks_start..];

    for anchor in [
        "policy.codex_anchor.verify_identity()",
        "policy.launcher_anchor.verify_identity()",
        "policy.pueue_anchor.verify_identity()",
        ".pueue_config_anchor\n                    .verify_identity(&policy.project_roots)",
    ] {
        assert!(checks.contains(anchor), "missing doctor anchor: {anchor}");
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
fn canonical_state_doctor_rejects_v2_budgets_and_oversized_state() {
    let harness = DiagnosticsHarness::new();
    fs::create_dir_all(harness.project().root_path.join(".pueue-agent")).unwrap();
    let mut state = canonical_state_json();
    state["budgets"] = json!({"max_experiments": 1});
    fs::write(
        harness.project().root_path.join(".pueue-agent/state.json"),
        serde_json::to_vec(&state).unwrap(),
    )
    .unwrap();

    let v2_budget = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        100,
        true,
    )
    .unwrap();
    let v2_budget: Value = serde_json::from_str(&v2_budget).unwrap();
    assert_eq!(
        state_check(&v2_budget, "state.schema")["status"],
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
fn canonical_state_doctor_rejects_v2_budget_fields_regardless_of_key() {
    let harness = DiagnosticsHarness::new();
    let state_dir = harness.project().root_path.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    let mut state = canonical_state_json();
    state["budgets"] = json!({"unexpected_budget": 1});
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
    assert!(check["summary"].as_str().unwrap().contains("budgets"));
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
fn doctor_reports_healthy_agent_run_id_sequence_without_exposing_values() {
    let harness = DiagnosticsHarness::new();
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
    let check = state_check(&value, "schema.agent_run_id_sequence");
    assert_eq!(check["status"], "ok");
    assert!(!check["summary"].as_str().unwrap().contains("last_run_id"));
    assert!(!check["summary"].as_str().unwrap().contains("0"));
}

#[test]
fn doctor_reports_missing_agent_run_id_sequence_row_without_aborting() {
    let harness = DiagnosticsHarness::new();
    harness
        .db
        .connect()
        .unwrap()
        .execute("DELETE FROM agent_run_id_sequence", [])
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
    assert_eq!(
        state_check(&value, "schema.agent_run_id_sequence")["status"],
        "error"
    );
    assert_eq!(state_check(&value, "schema.tables")["status"], "ok");
}

#[test]
fn doctor_reports_missing_agent_run_id_sequence_table_without_aborting() {
    let harness = DiagnosticsHarness::new();
    harness
        .db
        .connect()
        .unwrap()
        .execute("DROP TABLE agent_run_id_sequence", [])
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
    assert_eq!(state_check(&value, "schema.tables")["status"], "error");
    assert_eq!(
        state_check(&value, "schema.agent_run_id_sequence")["status"],
        "error"
    );
}

#[test]
fn doctor_reports_agent_run_id_sequence_floor_below_existing_run() {
    let harness = DiagnosticsHarness::new();
    let event_id = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::TaskFinished,
            "doctor-sequence-floor",
            json!({"task_id": 44}),
            100,
            100,
        ))
        .unwrap()
        .event_id;
    AgentRunRepository::new(&harness.db)
        .insert(&NewAgentRun::new(
            "project-a",
            event_id,
            None,
            AgentRunStatus::Starting,
            110,
            "/tmp/doctor-sequence-floor.log",
        ))
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE agent_run_id_sequence SET last_run_id = 0 WHERE sequence_id = 1",
            [],
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
    assert_eq!(
        state_check(&value, "schema.agent_run_id_sequence")["status"],
        "error"
    );
}

#[test]
fn doctor_reports_agent_run_id_sequence_above_allocatable_limit() {
    let harness = DiagnosticsHarness::new();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON")
        .unwrap();
    connection
        .execute(
            "UPDATE agent_run_id_sequence SET last_run_id = ?1 WHERE sequence_id = 1",
            [i64::MAX],
        )
        .unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = OFF")
        .unwrap();
    drop(connection);
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
    assert_eq!(
        state_check(&value, "schema.agent_run_id_sequence")["status"],
        "error"
    );
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

    fn start_campaign(&self) -> String {
        let state_dir = self.project().root_path.join(".pueue-agent");
        fs::create_dir_all(&state_dir).unwrap();
        fs::write(
            state_dir.join("STATE.md"),
            "Reach SECRET_OBJECTIVE validation loss below 0.20\n",
        )
        .unwrap();
        let objective = pueue_agent::state::load_objective(&self.project().root_path).unwrap();
        let argv = vec![
            "python".to_owned(),
            "train.py".to_owned(),
            "--token".to_owned(),
            "SECRET_ARGV".to_owned(),
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
        )
        .unwrap();
        CampaignRepository::new(&self.db)
            .start_with_baseline(
                StartCampaignRequest {
                    campaign_id: "diagnostics-campaign",
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: &argv,
                    baseline: &proposal,
                    submission_id: "diagnostics-campaign-submission",
                    experiment_id: "diagnostics-campaign-experiment",
                    proposal_id: "diagnostics-campaign-proposal",
                    metadata: &json!({}),
                    origin_agent_run_id: None,
                    now: unix_now(),
                },
                &CampaignLimits::default(),
            )
            .unwrap()
            .campaign
            .campaign_id
    }

    fn start_terminal_decision(&self, now: i64) -> (String, String) {
        let campaign_id = self.start_campaign();
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE experiments
                 SET status = 'failed', failure_code = 'fixture_failure',
                     failure_fingerprint = 'fixture-fingerprint', finished_at = ?1,
                     updated_at = ?1
                 WHERE experiment_id = 'diagnostics-campaign-experiment'",
                [now],
            )
            .unwrap();
        let cycle_id = DecisionRepository::new(&self.db)
            .ensure_cycle_for_terminal(&campaign_id, "diagnostics-campaign-experiment", now)
            .unwrap()
            .cycle_id;
        (campaign_id, cycle_id)
    }

    fn add_terminal_decision_cycle(
        &self,
        campaign_id: &str,
        suffix: &str,
        attempt: i64,
        terminal_at: i64,
        state: &str,
        next_wake_at: Option<i64>,
        cycle_updated_at: i64,
    ) -> (String, String) {
        let submission_id = format!("diagnostics-{suffix}-submission");
        let experiment_id = format!("diagnostics-{suffix}-experiment");
        let connection = self.db.connect().unwrap();
        connection
            .execute(
                "INSERT INTO submissions (
                    submission_id, project_id, argv_json, created_at, pueue_task_id,
                    task_signature, status, kind, metadata_json, origin_agent_run_id
                 )
                 SELECT ?1, project_id, argv_json, ?2, NULL, NULL, 'pending', kind,
                        metadata_json, origin_agent_run_id
                 FROM submissions
                 WHERE submission_id = 'diagnostics-campaign-submission'",
                params![submission_id, terminal_at],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO experiments (
                    experiment_id, campaign_id, proposal_id, submission_id,
                    parent_experiment_id, attempt, status, pueue_task_id, task_signature,
                    failure_code, failure_fingerprint, created_at, updated_at, finished_at
                 )
                 SELECT ?1, campaign_id, proposal_id, ?2, NULL, ?3, 'failed', NULL, NULL,
                        'fixture_failure', ?4, ?5, ?5, ?5
                 FROM experiments
                 WHERE experiment_id = 'diagnostics-campaign-experiment'",
                params![
                    experiment_id,
                    submission_id,
                    attempt,
                    format!("fixture-{suffix}-fingerprint"),
                    terminal_at,
                ],
            )
            .unwrap();
        drop(connection);
        let cycle_id = DecisionRepository::new(&self.db)
            .ensure_cycle_for_terminal(campaign_id, &experiment_id, terminal_at)
            .unwrap()
            .cycle_id;
        self.db
            .connect()
            .unwrap()
            .execute(
                "UPDATE decision_cycles
                 SET state = ?1, next_wake_at = ?2, updated_at = ?3
                 WHERE cycle_id = ?4",
                params![state, next_wake_at, cycle_updated_at, cycle_id],
            )
            .unwrap();
        (experiment_id, cycle_id)
    }

    fn write_project_config(&self, timeout_minutes: u32) {
        let config = include_str!("../../templates/config.toml")
            .replace("{{PROJECT_ID}}", "project-a")
            .replace("{{PUEUE_GROUP}}", "pa-project")
            .replace(
                "timeout_minutes = 60",
                &format!("timeout_minutes = {timeout_minutes}"),
            );
        fs::write(self.project().config_path, config).unwrap();
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[test]
fn campaign_status_human_compact_and_json_project_bounded_campaign_state() {
    let harness = DiagnosticsHarness::new();
    let campaign_id = harness.start_campaign();
    let experiments = ExperimentRepository::new(&harness.db);
    experiments
        .mark_submitting("diagnostics-campaign-experiment", unix_now())
        .unwrap();
    experiments
        .mark_unreconciled(
            "diagnostics-campaign-experiment",
            "pueue_add_unknown",
            unix_now(),
        )
        .unwrap();
    CampaignRepository::new(&harness.db)
        .reserve_agent_decision(
            &campaign_id,
            "diagnostics-decision",
            &CampaignLimits::default(),
            unix_now(),
        )
        .unwrap();
    let input = harness.input(PueueSnapshot::Tasks(Vec::new()));

    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    let compact =
        render_project_status_compact(&harness.db, &harness.project(), &input).unwrap();
    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();
    for rendered in [&human, &compact, &json] {
        assert!(rendered.contains("campaign"), "{rendered}");
        assert!(rendered.contains("active"), "{rendered}");
        assert!(rendered.contains("objective_digest"), "{rendered}");
        assert!(rendered.contains("unreconciled"), "{rendered}");
        assert!(rendered.contains("agent_run"), "{rendered}");
        assert!(!rendered.contains("SECRET_OBJECTIVE"), "{rendered}");
        assert!(!rendered.contains("SECRET_ARGV"), "{rendered}");
    }
    let value: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["campaign"]["campaign_id"], campaign_id);
    assert_eq!(value["campaign"]["experiment_counts"]["unreconciled"], 1);
    assert_eq!(value["campaign"]["rolling_usage"]["agent_run"], 1);
    assert_eq!(value["campaign"]["unreconciled_count"], 1);
    assert!(value["campaign"].as_object().unwrap().contains_key("decision"));
    assert!(value["campaign"]["decision"].is_null());
    let campaign_json =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), true)
            .unwrap();
    assert!(serde_json::from_str::<Value>(&campaign_json).unwrap()["decision"].is_null());
}

#[test]
fn decision_status_prefers_the_active_analysis_over_a_newer_pending_cycle() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (campaign_id, active_cycle_id) = harness.start_terminal_decision(now - 200);
    DecisionRepository::new(&harness.db)
        .reserve_next_attempt("project-a", &active_cycle_id, now - 190)
        .unwrap()
        .unwrap();
    let (_, newer_cycle_id) = harness.add_terminal_decision_cycle(
        &campaign_id,
        "newer-pending",
        1,
        now - 100,
        "pending",
        None,
        now + 100,
    );

    let input = harness.input(PueueSnapshot::Tasks(Vec::new()));
    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    let compact = render_project_status_compact(&harness.db, &harness.project(), &input).unwrap();
    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();
    let campaign_human =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), false)
            .unwrap();
    let campaign_json =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), true)
            .unwrap();

    for rendered in [&human, &compact, &json, &campaign_human, &campaign_json] {
        assert!(rendered.contains(&active_cycle_id), "{rendered}");
        assert!(!rendered.contains(&newer_cycle_id), "{rendered}");
    }
    assert_eq!(
        serde_json::from_str::<Value>(&json).unwrap()["campaign"]["decision"]["state"],
        "analyzing"
    );
}

#[test]
fn decision_status_uses_scheduler_due_and_source_terminal_order() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (campaign_id, future_wait_cycle_id) = harness.start_terminal_decision(now - 300);
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles
             SET state = 'waiting', next_wake_at = ?1, updated_at = ?2
             WHERE cycle_id = ?3",
            params![now + 600, now + 300, future_wait_cycle_id],
        )
        .unwrap();
    let (_, oldest_due_cycle_id) = harness.add_terminal_decision_cycle(
        &campaign_id,
        "oldest-due",
        1,
        now - 200,
        "pending",
        None,
        now + 200,
    );
    let (_, newer_due_cycle_id) = harness.add_terminal_decision_cycle(
        &campaign_id,
        "newer-due",
        2,
        now - 100,
        "pending",
        None,
        now + 400,
    );

    let input = harness.input(PueueSnapshot::Tasks(Vec::new()));
    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    let compact = render_project_status_compact(&harness.db, &harness.project(), &input).unwrap();
    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();
    let campaign_json =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), true)
            .unwrap();

    for rendered in [&human, &compact, &json, &campaign_json] {
        assert!(rendered.contains(&oldest_due_cycle_id), "{rendered}");
        assert!(!rendered.contains(&future_wait_cycle_id), "{rendered}");
        assert!(!rendered.contains(&newer_due_cycle_id), "{rendered}");
    }
}

#[test]
fn decision_status_projects_active_cycle_without_raw_evidence() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (campaign_id, cycle_id) = harness.start_terminal_decision(now);
    let decisions = DecisionRepository::new(&harness.db);
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    let next_wake_at = now + 600;
    decisions
        .mark_waiting(&cycle_id, reservation.attempt_number, next_wake_at, now + 2)
        .unwrap();

    let context_json = r#"{"prompt":"raw-prompt-secret","log":"raw-log-secret","objective":"raw-objective-secret"}"#;
    let decision_json = r#"{"decision":"wait","reason":"raw-decision-secret"}"#;
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_attempts
             SET context_schema_version = 1, context_json = ?1, context_digest = ?2,
                 decision_json = ?3, decision_digest = ?4
             WHERE cycle_id = ?5 AND attempt_number = ?6",
            params![
                context_json,
                format!("{:x}", Sha256::digest(context_json.as_bytes())),
                decision_json,
                format!("{:x}", Sha256::digest(decision_json.as_bytes())),
                cycle_id,
                reservation.attempt_number,
            ],
        )
        .unwrap();

    let input = harness.input(PueueSnapshot::Tasks(Vec::new()));
    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    let compact = render_project_status_compact(&harness.db, &harness.project(), &input).unwrap();
    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();
    let campaign_human =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), false)
            .unwrap();
    let campaign_json =
        pueue_agent::campaign::render_status_for_project(&harness.db, &harness.project(), true)
            .unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let decision = &value["campaign"]["decision"];
    let campaign_value: Value = serde_json::from_str(&campaign_json).unwrap();

    assert_eq!(value["campaign"]["campaign_id"], campaign_id);
    assert_eq!(decision["cycle_id"], cycle_id);
    assert_eq!(
        decision["source_experiment_id"],
        "diagnostics-campaign-experiment"
    );
    assert_eq!(decision["state"], "waiting");
    assert_eq!(decision["attempt_count"], 1);
    assert_eq!(decision["last_decision_kind"], "wait");
    assert_eq!(decision["next_wake_at"], next_wake_at);
    assert_eq!(campaign_value["decision"]["state"], "waiting");
    assert_eq!(campaign_value["decision"]["attempt_count"], 1);
    for absent in [
        "context_json",
        "context_digest",
        "decision_json",
        "decision_digest",
        "prompt",
        "transcript",
        "environment",
        "argv",
        "log",
        "objective",
    ] {
        assert!(
            decision.get(absent).is_none(),
            "unexpected {absent}: {json}"
        );
    }
    for rendered in [&human, &compact, &json, &campaign_human, &campaign_json] {
        assert!(rendered.contains("decision"), "{rendered}");
        for secret in [
            "raw-prompt-secret",
            "raw-log-secret",
            "raw-objective-secret",
            "raw-decision-secret",
        ] {
            assert!(!rendered.contains(secret), "leaked {secret}: {rendered}");
        }
    }
}

#[test]
fn decision_status_bounds_and_redacts_failure_facts() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let decisions = DecisionRepository::new(&harness.db);
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    let mut limits = CampaignLimits::default();
    limits.max_decision_attempts_per_cycle = 1;
    decisions
        .fail_attempt(
            None,
            &cycle_id,
            reservation.attempt_number,
            "decision_invalid",
            "OPENAI_API_KEY=decision-failure-secret ",
            limits,
            now + 2,
        )
        .unwrap();

    let rendered = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    let value: Value = serde_json::from_str(&rendered).unwrap();
    let decision = &value["campaign"]["decision"];
    assert_eq!(decision["state"], "degraded");
    assert_eq!(decision["failure_code"], "decision_invalid");
    assert!(decision["failure_summary"]
        .as_str()
        .unwrap()
        .contains("[REDACTED]"));
    assert!(!rendered.contains("decision-failure-secret"));
}

#[test]
fn decision_doctor_is_healthy_without_cycles_for_legacy_project() {
    let harness = DiagnosticsHarness::new();
    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        unix_now(),
    )
    .unwrap();
    let checks = report
        .checks
        .iter()
        .filter(|check| check.name.starts_with("decision."))
        .collect::<Vec<_>>();

    assert!(!checks.is_empty());
    assert!(
        checks
            .iter()
            .all(|check| check.status == DoctorCheckStatus::Ok),
        "{checks:?}"
    );
}

#[test]
fn decision_doctor_reports_orphan_cycle_lineage_without_repair() {
    let harness = DiagnosticsHarness::new();
    let campaign_id = harness.start_campaign();
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys = OFF;")
        .unwrap();
    connection
        .execute(
            "INSERT INTO decision_cycles (
                cycle_id, campaign_id, source_experiment_id, state, next_wake_at,
                consecutive_failed_attempts, created_at, updated_at
             ) VALUES ('orphan-decision-cycle', ?1, 'missing-experiment', 'pending',
                       NULL, 0, 100, 100)",
            [&campaign_id],
        )
        .unwrap();
    drop(connection);

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        unix_now(),
    )
    .unwrap();
    let check = report
        .checks
        .iter()
        .find(|check| check.name == "decision.lineage")
        .unwrap();
    assert_eq!(check.status, DoctorCheckStatus::Error);
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM decision_cycles WHERE cycle_id = 'orphan-decision-cycle'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        1
    );
}

#[test]
fn decision_doctor_reports_duplicate_active_attempts() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    DecisionRepository::new(&harness.db)
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "INSERT INTO decision_attempts (
                cycle_id, attempt_number, state, created_at
             ) VALUES (?1, 2, 'reserved', ?2)",
            params![cycle_id, now + 2],
        )
        .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 3,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.active_attempts")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn decision_doctor_reports_overdue_running_analysis() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now - 180);
    harness.write_project_config(1);
    let decisions = DecisionRepository::new(&harness.db);
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle_id, now - 179)
        .unwrap()
        .unwrap();
    let context_json = "{}";
    decisions
        .store_evidence(
            &reservation,
            context_json,
            &format!("{:x}", Sha256::digest(context_json.as_bytes())),
            now - 178,
        )
        .unwrap();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::CampaignDecision,
            "overdue-decision-run",
            json!({}),
            now - 180,
            now - 180,
        ))
        .unwrap();
    EventRepository::new(&harness.db)
        .claim_batch(now - 179, now + 60, 1)
        .unwrap();
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                Some(1234),
                AgentRunStatus::Running,
                now - 178,
                "/tmp/overdue-decision.log",
            ),
            &[event.event_id],
        )
        .unwrap();
    decisions
        .bind_agent_run(&reservation, run.run_id, now - 178)
        .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.running_attempts")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn decision_doctor_reports_waiting_cycle_without_wake() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let decisions = DecisionRepository::new(&harness.db);
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    decisions
        .mark_waiting(&cycle_id, reservation.attempt_number, now + 600, now + 2)
        .unwrap();
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_cycles SET next_wake_at = NULL WHERE cycle_id = ?1",
            [&cycle_id],
        )
        .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 3,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.wait_wake")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn decision_doctor_reports_digest_mismatch_without_payload_or_repair() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let decisions = DecisionRepository::new(&harness.db);
    let reservation = decisions
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    let context_json = r#"{"prompt":"digest-raw-prompt-secret"}"#;
    decisions
        .store_evidence(&reservation, context_json, &"0".repeat(64), now + 2)
        .unwrap();
    let decision_json = r#"{"decision":"wait","reason":"digest-raw-decision-secret"}"#;
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_attempts
             SET decision_json = ?1, decision_digest = ?2
             WHERE cycle_id = ?3 AND attempt_number = ?4",
            params![
                decision_json,
                "1".repeat(64),
                cycle_id,
                reservation.attempt_number,
            ],
        )
        .unwrap();
    let before = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT context_digest, decision_digest FROM decision_attempts
             WHERE cycle_id = ?1 AND attempt_number = ?2",
            params![cycle_id, reservation.attempt_number],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 3,
    )
    .unwrap();
    let check = report
        .checks
        .iter()
        .find(|check| check.name == "decision.digests")
        .unwrap();
    assert_eq!(check.status, DoctorCheckStatus::Error);
    let rendered = render_doctor_report_value(&report, true).unwrap();
    assert!(!rendered.contains("digest-raw-prompt-secret"));
    assert!(!rendered.contains("digest-raw-decision-secret"));
    let after = harness
        .db
        .connect()
        .unwrap()
        .query_row(
            "SELECT context_digest, decision_digest FROM decision_attempts
             WHERE cycle_id = ?1 AND attempt_number = ?2",
            params![cycle_id, reservation.attempt_number],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap();
    assert_eq!(after, before);
}

#[test]
fn decision_doctor_reports_malformed_rows_as_typed_check_without_repair() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch("PRAGMA ignore_check_constraints = ON;")
        .unwrap();
    connection
        .execute(
            "UPDATE decision_cycles SET state = 'malformed-state' WHERE cycle_id = ?1",
            [&cycle_id],
        )
        .unwrap();
    drop(connection);

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 1,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.rows")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT state FROM decision_cycles WHERE cycle_id = ?1",
                [&cycle_id],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "malformed-state"
    );
}

#[test]
fn decision_doctor_caps_attempt_probes_at_the_policy_limit_plus_one() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let connection = harness.db.connect().unwrap();
    for attempt_number in 1..=4 {
        connection
            .execute(
                "INSERT INTO decision_attempts (
                    cycle_id, attempt_number, state, created_at, finished_at
                 ) VALUES (?1, ?2, 'failed', ?3, ?3)",
                params![cycle_id, attempt_number, now + attempt_number],
            )
            .unwrap();
    }
    drop(connection);

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 10,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.rows")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn decision_doctor_caps_cycle_probes_at_the_policy_limit_plus_one() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (campaign_id, _) = harness.start_terminal_decision(now - 100);
    harness.add_terminal_decision_cycle(
        &campaign_id,
        "parallel-cycle-overflow",
        1,
        now - 50,
        "pending",
        None,
        now - 50,
    );

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.rows")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn decision_doctor_reports_blob_in_text_payload_as_typed_checks_without_repair() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let reservation = DecisionRepository::new(&harness.db)
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    let connection = harness.db.connect().unwrap();
    connection
        .execute(
            "UPDATE decision_attempts
             SET context_schema_version = 1,
                 context_json = CAST(x'ff005345435245545f5041594c4f4144' AS BLOB),
                 context_digest = ?1
             WHERE cycle_id = ?2 AND attempt_number = ?3",
            params!["0".repeat(64), cycle_id, reservation.attempt_number],
        )
        .unwrap();
    drop(connection);

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 2,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.rows")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.digests")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
    assert_eq!(
        harness
            .db
            .connect()
            .unwrap()
            .query_row(
                "SELECT typeof(context_json) FROM decision_attempts
                 WHERE cycle_id = ?1 AND attempt_number = ?2",
                params![cycle_id, reservation.attempt_number],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "blob"
    );
    assert!(!render_doctor_report_value(&report, true)
        .unwrap()
        .contains("SECRET_PAYLOAD"));
}

#[test]
fn decision_doctor_applies_payload_bounds_in_bytes() {
    let harness = DiagnosticsHarness::new();
    let now = unix_now();
    let (_, cycle_id) = harness.start_terminal_decision(now);
    let reservation = DecisionRepository::new(&harness.db)
        .reserve_next_attempt("project-a", &cycle_id, now + 1)
        .unwrap()
        .unwrap();
    let oversized_multibyte_context = "あ".repeat(50_000);
    let digest = format!(
        "{:x}",
        Sha256::digest(oversized_multibyte_context.as_bytes())
    );
    harness
        .db
        .connect()
        .unwrap()
        .execute(
            "UPDATE decision_attempts
             SET context_schema_version = 1, context_json = ?1, context_digest = ?2
             WHERE cycle_id = ?3 AND attempt_number = ?4",
            params![
                oversized_multibyte_context,
                digest,
                cycle_id,
                reservation.attempt_number,
            ],
        )
        .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        now + 2,
    )
    .unwrap();
    assert_eq!(
        report
            .checks
            .iter()
            .find(|check| check.name == "decision.rows")
            .unwrap()
            .status,
        DoctorCheckStatus::Error
    );
}

#[test]
fn campaign_status_is_absent_for_legacy_projects() {
    let harness = DiagnosticsHarness::new();
    let input = harness.input(PueueSnapshot::Tasks(Vec::new()));

    let human = render_project_status(&harness.db, &harness.project(), &input).unwrap();
    let compact =
        render_project_status_compact(&harness.db, &harness.project(), &input).unwrap();
    let json = render_project_status_json(&harness.db, &harness.project(), &input).unwrap();

    assert!(!human.lines().any(|line| line.starts_with("campaign:")));
    assert!(!compact.lines().any(|line| line.starts_with("campaign:")));
    assert!(serde_json::from_str::<Value>(&json).unwrap().get("campaign").is_none());
}

#[test]
fn campaign_doctor_reports_invariants_and_objective_digest_drift_without_repair() {
    let harness = DiagnosticsHarness::new();
    let campaign_id = harness.start_campaign();
    let stored_digest = CampaignRepository::new(&harness.db)
        .find_by_id(&campaign_id)
        .unwrap()
        .unwrap()
        .objective_digest;
    let connection = harness.db.connect().unwrap();
    connection
        .execute_batch(
            "UPDATE campaigns
             SET baseline_experiment_id = NULL, state = 'budget_waiting', next_eligible_at = NULL;
             UPDATE experiments
             SET status = 'unreconciled', pueue_task_id = 41, task_signature = 'experiment-signature';
             UPDATE submissions
             SET status = 'accepted', pueue_task_id = 41, task_signature = 'submission-signature';
             INSERT INTO budget_reservations (
                 reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
                 window_started_at, window_ends_at, created_at, updated_at
             ) VALUES (
                 'orphan-code-change', 'diagnostics-campaign', NULL, 'code_change',
                 'missing-proposal', 'consumed', 100, 200, 100, 100
             );",
        )
        .unwrap();
    fs::write(
        harness.project().root_path.join(".pueue-agent/STATE.md"),
        "A different objective that must not replace the snapshot\n",
    )
    .unwrap();

    let report = build_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        unix_now(),
    )
    .unwrap();
    let status = |name: &str| {
        report
            .checks
            .iter()
            .find(|check| check.name == name)
            .unwrap()
            .status
    };
    assert_eq!(status("campaign.live_count"), DoctorCheckStatus::Ok);
    assert_eq!(status("campaign.baseline_linkage"), DoctorCheckStatus::Error);
    assert_eq!(status("campaign.orphan_reservations"), DoctorCheckStatus::Error);
    assert_eq!(status("campaign.task_identity"), DoctorCheckStatus::Error);
    assert_eq!(status("campaign.submission_boundaries"), DoctorCheckStatus::Warning);
    assert_eq!(status("campaign.budget_wake"), DoctorCheckStatus::Error);
    assert_eq!(status("campaign.objective_digest"), DoctorCheckStatus::Warning);
    assert_eq!(
        CampaignRepository::new(&harness.db)
            .find_by_id(&campaign_id)
            .unwrap()
            .unwrap()
            .objective_digest,
        stored_digest
    );
}

#[test]
fn no_run_pre_binding_policy_blocks_project_through_all_diagnostics() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "pre-binding-diagnostic-policy-block",
            json!({"prompt": "never render this prompt", "OPENAI_API_KEY": "never render this secret"}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db).claim_batch(100, 200, 1).unwrap();
    EventRepository::new(&harness.db)
        .dead_letter_claimed_without_run(
            "project-a",
            &[event.event_id],
            110,
            &PolicyViolation::new(
                PolicyViolationCode::UnsafeCodexArgument,
                PolicyViolationStage::PreBinding,
            ),
        )
        .unwrap();

    let events = render_events(
        &harness.db,
        &harness.project(),
        &EventFilter::new(None, None, 8),
        true,
    )
    .unwrap();
    let run_list = render_runs(&harness.db, &harness.project(), 8, true).unwrap();
    let status = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    let human_status = render_project_status(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    let doctor = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        120,
        true,
    )
    .unwrap();

    for projection in [&events, &run_list, &status, &doctor] {
        assert!(projection.contains("unsafe_codex_argument"), "{projection}");
        assert!(projection.contains("pre_binding"), "{projection}");
        assert!(!projection.contains("never render this prompt"), "{projection}");
        assert!(!projection.contains("never render this secret"), "{projection}");
        assert!(!projection.contains("OPENAI_API_KEY"), "{projection}");
    }
    let event_value: Value = serde_json::from_str(&events).unwrap();
    assert_eq!(event_value["events"][0]["run_id"], Value::Null);
    let runs_value: Value = serde_json::from_str(&run_list).unwrap();
    assert_eq!(runs_value["runs"][0]["run_id"], Value::Null);
    assert!(human_status.contains("policy_code=unsafe_codex_argument"));
    assert!(human_status.contains("failure_stage=pre_binding"));
}

#[test]
fn execution_projections_show_policy_code_stage_and_path_without_secrets() {
    let harness = DiagnosticsHarness::new();
    let event = EventRepository::new(&harness.db)
        .insert_idempotent(&NewEvent::new(
            "project-a",
            EventKind::Crash,
            "execution-projection-policy-block",
            json!({"prompt": "never render this prompt", "OPENAI_API_KEY": "never render this secret"}),
            100,
            100,
        ))
        .unwrap();
    EventRepository::new(&harness.db).claim_batch(100, 200, 1).unwrap();
    let run = AgentRunRepository::new(&harness.db)
        .insert_with_events(
            &NewAgentRun::new(
                "project-a",
                event.event_id,
                None,
                AgentRunStatus::Starting,
                110,
                harness.project().root_path.join(".pueue-agent/logs/agent.log"),
            )
            .with_execution(
                ExecutionProjection::new("codex", "/usr/local/bin/codex", "device=1,inode=2")
                    .unwrap(),
            ),
            &[event.event_id],
        )
        .unwrap();
    let runs = AgentRunRepository::new(&harness.db);
    runs.mark_running_and_apply_interventions("project-a", run.run_id, 42, 120)
        .unwrap();
    runs.mark_gate_release_requested("project-a", run.run_id).unwrap();
    runs.fail_before_gate_release_with_policy(
        "project-a",
        run.run_id,
        130,
        "ignored secret-bearing detail",
        &PolicyViolation::new(
            PolicyViolationCode::UnsafeCodexArgument,
            PolicyViolationStage::RunBoundPreMarker,
        ),
    )
    .unwrap();

    let events = render_events(
        &harness.db,
        &harness.project(),
        &EventFilter::new(None, None, 8),
        true,
    )
    .unwrap();
    let run_list = render_runs(&harness.db, &harness.project(), 8, true).unwrap();
    let status = render_project_status_json(
        &harness.db,
        &harness.project(),
        &harness.input(PueueSnapshot::Tasks(Vec::new())),
    )
    .unwrap();
    let doctor = render_doctor_report(
        &harness.db,
        &harness.project(),
        &doctor_paths(&harness),
        doctor_external(),
        140,
        true,
    )
    .unwrap();

    for projection in [&events, &run_list, &status, &doctor] {
        assert!(projection.contains("unsafe_codex_argument"), "{projection}");
        assert!(!projection.contains("never render this prompt"), "{projection}");
        assert!(!projection.contains("never render this secret"), "{projection}");
        assert!(!projection.contains("OPENAI_API_KEY"), "{projection}");
    }
    for projection in [&events, &run_list, &status, &doctor] {
        assert!(projection.contains("run_bound_pre_marker"), "{projection}");
        assert!(projection.contains("/usr/local/bin/codex"), "{projection}");
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
    let cases = [
        (
            "agent.context.session_id=REAL_INLINE_EQUAL",
            Some("REAL_INLINE_EQUAL"),
        ),
        (
            "agent.context.session_id= REAL_TRAILING_EQUAL",
            Some("REAL_TRAILING_EQUAL"),
        ),
        (
            "agent.context.session_id = REAL_SPACED_EQUAL",
            Some("REAL_SPACED_EQUAL"),
        ),
        (
            "agent.context.session_id : REAL_SPACED_COLON",
            Some("REAL_SPACED_COLON"),
        ),
        (
            "agent.context.session_id := REAL_SPACED_COLON_EQUAL",
            Some("REAL_SPACED_COLON_EQUAL"),
        ),
        (
            "agent.context.session_id:=REAL_INLINE_COLON_EQUAL",
            Some("REAL_INLINE_COLON_EQUAL"),
        ),
        (
            "agent.context.session_id REAL_NEXT_TOKEN",
            Some("REAL_NEXT_TOKEN"),
        ),
        (
            "agent.context.session_id . REAL_SEPARATE_DOT",
            Some("REAL_SEPARATE_DOT"),
        ),
        (
            "agent.context.session_id \":\" REAL_QUOTED_SEPARATOR",
            Some("REAL_QUOTED_SEPARATOR"),
        ),
        (
            "agent.context.session_id , REAL_PUNCTUATION_SEPARATOR",
            Some("REAL_PUNCTUATION_SEPARATOR"),
        ),
        (
            "agent.context.session_id . \"REAL NAKED MULTIWORD\"",
            Some("REAL NAKED MULTIWORD"),
        ),
        (
            "agent.context.session_id = \"REAL QUOTED VALUE\" trailing",
            Some("REAL QUOTED VALUE"),
        ),
        ("agent.context.session_id, is required", None),
        ("invalid field `agent.context.session_id`", None),
    ];
    for (input, real_value) in cases {
        let rendered = redact_sensitive_text(input);
        assert!(
            rendered.contains("agent.context.session_id"),
            "session-id field label was lost: {rendered}"
        );
        if let Some(real_value) = real_value {
            assert!(
                rendered.contains("[REDACTED]"),
                "session-id value was not marked redacted: {rendered}"
            );
            assert!(
                !rendered.contains(real_value),
                "session-id value leaked: {rendered}"
            );
        }
    }

    let boundary = redact_sensitive_text("agent.context.session_id VALUE --lr 0.1");
    assert!(boundary.contains("agent.context.session_id"));
    assert!(boundary.contains("[REDACTED]"));
    assert!(!boundary.contains("VALUE"));
    assert!(boundary.contains("--lr 0.1"));
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
        execution_policy: std::path::PathBuf::from("/state/execution-policy.toml"),
        working_dir: harness.project().root_path,
        home: std::path::PathBuf::from("/home/fixture"),
        codex_home: std::path::PathBuf::from("/home/fixture/.codex"),
        path_env: "/usr/bin:/bin".to_owned(),
        startup_environment: StartupEnvironment::default(),
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
        execution_policy: std::path::PathBuf::from("/state/execution-policy.toml"),
        working_dir: harness.project().root_path,
        home: std::path::PathBuf::from("/home/fixture"),
        codex_home: std::path::PathBuf::from("/home/fixture/.codex"),
        path_env: "/usr/bin:/bin".to_owned(),
        startup_environment: StartupEnvironment::default(),
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
        execution_policy: std::path::PathBuf::from("/state/execution-policy.toml"),
        working_dir: harness.project().root_path,
        home: std::path::PathBuf::from("/home/fixture"),
        codex_home: std::path::PathBuf::from("/home/fixture/.codex"),
        path_env: "/usr/bin:/bin".to_owned(),
        startup_environment: StartupEnvironment::default(),
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
