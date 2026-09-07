use std::fs;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use pueue_agent::{
    code_change::{best_ref, candidate_ref},
    db::{CampaignRepository, CodeChangeRepository, Db, ProjectRepository, StartCampaignRequest},
    diagnostics::render_project_status_json,
    execution_policy::CampaignLimits,
    models::{MetricDirection, NewCodeChangeRun, ObjectiveMetric, ProposalKind},
    proposals::{self, ProposalInput},
    service::ServiceStatus,
    status::{render_project_status, PueueSnapshot, StatusInput},
};
use serde_json::{json, Value};
use tempfile::TempDir;

fn harness_with_metric() -> (TempDir, Db) {
    let temp = TempDir::new().unwrap();
    #[cfg(unix)]
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let state_dir = root.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    ProjectRepository::new(&db)
        .register(&pueue_agent::models::NewProject::new(
            "project-a",
            &root,
            "pa-project",
            root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    fs::write(state_dir.join("STATE.md"), "Reach loss below 0.2\n").unwrap();
    let objective = pueue_agent::state::load_objective(&root).unwrap();
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "baseline".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    let metric = ObjectiveMetric {
        name: "loss".to_owned(),
        direction: MetricDirection::Minimize,
        min_delta: None,
    };
    CampaignRepository::new(&db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: "campaign-eval",
                project_id: "project-a",
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: "submission-baseline",
                experiment_id: "exp-baseline",
                proposal_id: "proposal-baseline",
                metadata: &json!({}),
                origin_agent_run_id: None,
                objective_metric: Some(&metric),
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap();
    (temp, db)
}

fn harness_without_metric() -> (TempDir, Db) {
    let temp = TempDir::new().unwrap();
    #[cfg(unix)]
    fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let state_dir = root.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
    #[cfg(unix)]
    fs::set_permissions(&state_dir, fs::Permissions::from_mode(0o700)).unwrap();
    let db = Db::open(&temp.path().join("state.sqlite3")).unwrap();
    ProjectRepository::new(&db)
        .register(&pueue_agent::models::NewProject::new(
            "project-a",
            &root,
            "pa-project",
            root.join(".pueue-agent/config.toml"),
            100,
        ))
        .unwrap();
    fs::write(state_dir.join("STATE.md"), "Reach loss below 0.2\n").unwrap();
    let objective = pueue_agent::state::load_objective(&root).unwrap();
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "baseline".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    CampaignRepository::new(&db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: "campaign-eval",
                project_id: "project-a",
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: "submission-baseline",
                experiment_id: "exp-baseline",
                proposal_id: "proposal-baseline",
                metadata: &json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: 100,
            },
            &CampaignLimits::default(),
        )
        .unwrap();
    (temp, db)
}

fn seed_code_change_status_projection(db: &Db) {
    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis,
                 source_experiment_id, argv_json, working_directory,
                 expected_evidence_json, canonical_digest, reject_reason,
                 created_at, updated_at
             ) VALUES (
                 'status-code-proposal', 'campaign-eval', 'code_change', 'accepted',
                 'bounded status fixture', 'exp-baseline', '[\"python\",\"train.py\"]', '.',
                 '[]', 'status-code-proposal-digest', NULL, 101, 101
             )",
            [],
        )
        .unwrap();
    drop(connection);

    let base_sha = "a".repeat(40);
    let candidate_sha = "b".repeat(40);
    let run = CodeChangeRepository::new(db)
        .create_pending(
            &NewCodeChangeRun::new(
                "status-code-run",
                "status-code-proposal",
                "campaign-eval",
                base_sha.clone(),
                candidate_ref("campaign-eval", "status-code-proposal").unwrap(),
                best_ref("campaign-eval").unwrap(),
                "status-code-worktree",
                ".pueue-agent/worktrees/campaign-eval/status-code-proposal",
                102,
            ),
        )
        .unwrap();

    let connection = db.connect().unwrap();
    connection
        .execute(
            "INSERT INTO submissions (
                 submission_id, project_id, argv_json, created_at, pueue_task_id,
                 task_signature, status, kind, metadata_json, origin_agent_run_id
             ) VALUES (
                 'status-code-submission', 'project-a', '[\"python\",\"train.py\"]',
                 103, 77, 'status-code-task-signature', 'accepted', 'code_change', '{}', NULL
             )",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO experiments (
                 experiment_id, campaign_id, proposal_id, submission_id, parent_experiment_id,
                 attempt, status, pueue_task_id, task_signature, failure_code,
                 failure_fingerprint, created_at, updated_at, finished_at,
                 code_change_run_id, code_revision_sha
             ) VALUES (
                 'status-code-experiment', 'campaign-eval', 'status-code-proposal',
                 'status-code-submission', NULL, 1, 'succeeded', 77,
                 'status-code-task-signature', NULL, NULL, 103, 200, 200, ?1, ?2
             )",
            rusqlite::params![run.code_change_run_id, candidate_sha],
        )
        .unwrap();
    connection
        .execute(
            "UPDATE code_change_runs
             SET state = 'evaluated', candidate_sha = ?1, experiment_id = ?2,
                 editor_attempts = 2, diff_digest = ?3, changed_file_count = 1,
                 diff_bytes = 42, updated_at = 201
             WHERE code_change_run_id = ?4",
            rusqlite::params![candidate_sha, "status-code-experiment", "d".repeat(64), run.code_change_run_id.as_str()],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO code_change_checks (
                 code_change_run_id, attempt, ordinal, source, argv_json,
                 working_directory, status, output_digest, summary, started_at, finished_at
             ) VALUES (?1, 2, 0, 'discovered', '[\"pytest\",\"tests/smoke\"]', '.',
                       'failed', ?2, 'project check failed', 180, 181)",
            rusqlite::params![run.code_change_run_id.as_str(), "e".repeat(64)],
        )
        .unwrap();

    let states = [
        "reserved",
        "preparing_worktree",
        "editing",
        "checking",
        "committing",
        "candidate_ready",
        "experiment_submitted",
        "evaluated",
        "cleanup_pending",
        "completed",
    ];
    for (index, state) in states.into_iter().enumerate() {
        connection
            .execute(
                "INSERT INTO events (
                     project_id, campaign_id, experiment_id, kind, dedup_key,
                     payload_json, status, attempts, not_before, lease_until,
                     created_at, completed_at, last_error
                 ) VALUES (
                     'project-a', 'campaign-eval', NULL, 'code_change', ?1, ?2,
                     'completed', 0, ?3, NULL, ?3, ?3, NULL
                 )",
                rusqlite::params![
                    format!("status-code-transition-{index}"),
                    serde_json::json!({
                        "code_change_run_id": run.code_change_run_id.as_str(),
                        "state": state,
                        "attempt": index,
                        "reason_code": "check_failed",
                        "raw_diff": "SECRET_RAW_DIFF",
                        "prompt": "SECRET_PROMPT"
                    })
                    .to_string(),
                    300 + index as i64,
                ],
            )
            .unwrap();
    }
}

fn seed_newer_code_change_status_events(db: &Db) {
    let connection = db.connect().unwrap();
    connection
        .execute(
            "UPDATE code_change_runs
             SET state = 'completed', cleanup_completed_at = 310, updated_at = 310
             WHERE code_change_run_id = 'status-code-run'",
            [],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO proposals (
                 proposal_id, campaign_id, kind, status, hypothesis,
                 source_experiment_id, argv_json, working_directory,
                 expected_evidence_json, canonical_digest, reject_reason,
                 created_at, updated_at
             ) VALUES (
                 'status-code-newer-proposal', 'campaign-eval', 'code_change', 'accepted',
                 'newer bounded status fixture', 'exp-baseline', '[\"python\",\"train.py\"]', '.',
                 '[]', 'status-code-newer-proposal-digest', NULL, 399, 399
             )",
            [],
        )
        .unwrap();
    drop(connection);

    let run = CodeChangeRepository::new(db)
        .create_pending(&NewCodeChangeRun::new(
            "status-code-newer-run",
            "status-code-newer-proposal",
            "campaign-eval",
            "c".repeat(40),
            candidate_ref("campaign-eval", "status-code-newer-proposal").unwrap(),
            best_ref("campaign-eval").unwrap(),
            "status-code-newer-worktree",
            ".pueue-agent/worktrees/campaign-eval/status-code-newer-proposal",
            400,
        ))
        .unwrap();

    let connection = db.connect().unwrap();
    for index in 1..=9 {
        connection
            .execute(
                "INSERT INTO events (
                     project_id, campaign_id, experiment_id, kind, dedup_key,
                     payload_json, status, attempts, not_before, lease_until,
                     created_at, completed_at, last_error
                 ) VALUES (
                     'project-a', 'campaign-eval', NULL, 'code_change', ?1, ?2,
                     'completed', 0, ?3, NULL, ?3, ?3, NULL
                 )",
                rusqlite::params![
                    format!("status-code-newer-transition-{index}"),
                    serde_json::json!({
                        "code_change_run_id": run.code_change_run_id.as_str(),
                        "state": "checking",
                        "reason_code": "check_failed",
                        "raw_diff": "SECRET_NEWER_RAW_DIFF",
                        "prompt": "SECRET_NEWER_PROMPT"
                    })
                    .to_string(),
                    400 + index as i64,
                ],
            )
            .unwrap();
    }
}

#[test]
fn status_json_projects_bounded_code_change_without_sensitive_fields() {
    let (_temp, db) = harness_with_metric();
    seed_code_change_status_projection(&db);
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();

    let json = render_project_status_json(
        &db,
        &project,
        &StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue: PueueSnapshot::Tasks(vec![]),
            now_override: Some(500),
        },
    )
    .unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let run = &value["code_changes"][0];
    assert_eq!(run["state"], "evaluated");
    assert_eq!(run["attempts"], 2);
    assert_eq!(run["base_sha"], "aaaaaaaa");
    assert_eq!(run["candidate_sha"], "bbbbbbbb");
    assert_eq!(run["experiment_id"], "status-code-experiment");
    assert_eq!(run["task_id"], 77);
    assert_eq!(run["failed_check"]["status"], "failed");
    assert_eq!(run["failed_check"]["summary"], "project check failed");
    assert_eq!(run["next_action"], "cleanup");
    assert_eq!(run["cleanup_pending"], true);
    assert_eq!(run["transitions"].as_array().unwrap().len(), 8);
    assert_eq!(run["transitions"][0]["stage"], "completed");
    assert_eq!(run["transitions"][0]["reason"], "check_failed");

    let raw_argv = "pytest";
    let base_sha = "a".repeat(40);
    let candidate_sha = "b".repeat(40);
    let diff_digest = "d".repeat(64);
    let output_digest = "e".repeat(64);
    for secret in [
        "SECRET_RAW_DIFF",
        "SECRET_PROMPT",
        raw_argv,
        diff_digest.as_str(),
        output_digest.as_str(),
        base_sha.as_str(),
        candidate_sha.as_str(),
    ] {
        assert!(!json.contains(secret), "status leaked {secret}: {json}");
    }
}

#[test]
fn status_json_keeps_bounded_transitions_per_code_change_run() {
    let (_temp, db) = harness_with_metric();
    seed_code_change_status_projection(&db);
    seed_newer_code_change_status_events(&db);
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();

    let json = render_project_status_json(
        &db,
        &project,
        &StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue: PueueSnapshot::Tasks(vec![]),
            now_override: Some(500),
        },
    )
    .unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let older_run = value["code_changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|run| run["candidate_sha"] == "bbbbbbbb")
        .unwrap();
    assert_eq!(older_run["transitions"].as_array().unwrap().len(), 8);
    assert_eq!(older_run["transitions"][0]["stage"], "completed");
}

#[test]
fn status_json_surfaces_timed_out_code_change_check() {
    let (_temp, db) = harness_with_metric();
    seed_code_change_status_projection(&db);
    db.connect()
        .unwrap()
        .execute(
            "UPDATE code_change_checks
             SET status = 'timed_out', summary = 'project check timed out'
             WHERE code_change_run_id = 'status-code-run' AND attempt = 2 AND ordinal = 0",
            [],
        )
        .unwrap();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();

    let json = render_project_status_json(
        &db,
        &project,
        &StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue: PueueSnapshot::Tasks(vec![]),
            now_override: Some(500),
        },
    )
    .unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let run = &value["code_changes"][0];
    assert_eq!(run["failed_check"]["status"], "timed_out");
    assert_eq!(run["failed_check"]["summary"], "project check timed out");
}

#[test]
fn status_campaign_includes_best_and_plateau_lines() {
    let (_temp, db) = harness_with_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();

    db.connect().unwrap().execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES (?1,'manifest','loss',0.5,'{}',NULL,100,100)",
        ["exp-baseline"],
    ).unwrap();
    db.connect().unwrap().execute(
        "UPDATE campaigns SET current_best_experiment_id='exp-baseline', plateau_count=2 WHERE campaign_id='campaign-eval'",
        [],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, status, kind, metadata_json) VALUES ('sub-challenger','project-a','[]',101,'pending','experiment','{}')",
        [],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id, attempt, status, created_at, updated_at) VALUES ('exp-challenger','campaign-eval','proposal-baseline','sub-challenger',1,'succeeded',101,101)",
        [],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES ('exp-challenger','manifest','loss',0.4,'{}',NULL,101,101)",
        [],
    ).unwrap();
    db.connect().unwrap().execute(
        "UPDATE campaigns SET current_best_experiment_id='exp-challenger' WHERE campaign_id='campaign-eval'",
        [],
    ).unwrap();

    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![]),
        now_override: Some(200),
    };
    let human = render_project_status(&db, &project, &input).unwrap();
    let human_compact =
        pueue_agent::status::render_project_status_compact(&db, &project, &input).unwrap();
    // deterministic short-id: first 8 chars of exp-challenger -> exp-chal
    let expected_best = "best: id=exp-chal value=0.4 metric=loss";
    let expected_plateau = "plateau: count=2";
    for rendered in [&human, &human_compact] {
        assert!(
            rendered.contains(expected_best),
            "human must contain exact best line {expected_best}: {rendered}"
        );
        assert!(
            rendered.contains(expected_plateau),
            "human must contain exact plateau line {expected_plateau}: {rendered}"
        );
        // ensure not using full id in human
        assert!(
            !rendered.contains("best: id=exp-challenger "),
            "human best must use short id, not full: {rendered}"
        );
    }

    let json = render_project_status_json(&db, &project, &input).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(
        value["campaign"]["best_experiment_id"], "exp-challenger",
        "json best_experiment_id must be full id: {json}"
    );
    assert_eq!(
        value["campaign"]["best_metric_name"], "loss",
        "json best_metric_name: {json}"
    );
    assert_eq!(
        value["campaign"]["best_metric_value"], 0.4,
        "json best_metric_value: {json}"
    );
    assert_eq!(
        value["campaign"]["plateau_count"], 2,
        "json plateau_count: {json}"
    );
    assert!(
        value["campaign"].get("best").is_none(),
        "json must not contain unrequested alias best: {json}"
    );
}

#[test]
fn status_campaign_without_objective_has_no_evaluation_surface() {
    let (_temp, db) = harness_without_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![]),
        now_override: Some(200),
    };
    let human = render_project_status(&db, &project, &input).unwrap();
    let human_compact =
        pueue_agent::status::render_project_status_compact(&db, &project, &input).unwrap();
    for rendered in [&human, &human_compact] {
        assert!(
            !rendered.contains("best:"),
            "metric-less human must not contain best:: {rendered}"
        );
        assert!(
            !rendered.contains("plateau:"),
            "metric-less human must not contain plateau:: {rendered}"
        );
    }
    let json = render_project_status_json(&db, &project, &input).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    assert!(
        value["campaign"].get("best_experiment_id").is_none(),
        "metric-less json must not have best_experiment_id: {json}"
    );
    assert!(
        value["campaign"].get("best_metric_name").is_none(),
        "metric-less json must not have best_metric_name: {json}"
    );
    assert!(
        value["campaign"].get("best_metric_value").is_none(),
        "metric-less json must not have best_metric_value: {json}"
    );
    assert!(
        value["campaign"].get("plateau_count").is_none(),
        "metric-less json must not have plateau_count: {json}"
    );
    assert!(
        value.get("evaluation").is_none(),
        "metric-less json must not have top-level evaluation: {json}"
    );
}

#[test]
fn diagnostics_lists_metrics_rows_capped_at_50() {
    let (_temp, db) = harness_with_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();

    // create exactly 60 valid rows without colliding baseline attempt (baseline 0, challenger 1 reserved, so start at 10)
    for i in 0..60 {
        let exp_id = format!("exp-metrics-{i:02}");
        let sub_id = format!("sub-metrics-{i:02}");
        let attempt = i as i64 + 10;
        let updated_at = 200 + i as i64; // deterministic newest is 59
        db.connect().unwrap().execute(
            "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, status, kind, metadata_json) VALUES (?1,'project-a','[]',?2,'pending','experiment','{}')",
            rusqlite::params![sub_id, updated_at],
        ).unwrap();
        db.connect().unwrap().execute(
            "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id, attempt, status, created_at, updated_at) VALUES (?1,'campaign-eval','proposal-baseline',?2,?3,'succeeded',?4,?4)",
            rusqlite::params![exp_id, sub_id, attempt, updated_at],
        ).unwrap();
        db.connect().unwrap().execute(
            "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES (?1,'manifest','loss',?2,'{\"loss\":1.0}',NULL,?3,?4)",
            rusqlite::params![exp_id, i as f64 * 0.1, updated_at, updated_at],
        ).unwrap();
    }

    let json = render_project_status_json(
        &db,
        &project,
        &StatusInput {
            daemon_health: ServiceStatus::Running,
            pueue: PueueSnapshot::Tasks(vec![]),
            now_override: Some(500),
        },
    )
    .unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    let evaluation = value
        .get("evaluation")
        .expect("evaluation must be present when metrics exist: {json}");
    let recent = evaluation
        .get("recent")
        .and_then(|v| v.as_array())
        .expect("evaluation.recent must be array");
    assert_eq!(
        recent.len(),
        50,
        "must be capped at exactly 50, got {}: {json}",
        recent.len()
    );
    // deterministic newest/tie ordering: updated_at DESC, experiment_id DESC -> exp-metrics-59 is newest
    assert_eq!(
        recent[0]["experiment_id"], "exp-metrics-59",
        "newest must be 59"
    );
    assert_eq!(
        recent[49]["experiment_id"], "exp-metrics-10",
        "oldest kept must be 10"
    );
    // representative fields: source, created/updated timestamps, no raw metrics_json
    for row in recent {
        assert_eq!(row["source"], "manifest", "source missing: {row}");
        assert!(
            row.get("created_at").is_some() && row["created_at"].is_number(),
            "created_at missing: {row}"
        );
        assert!(
            row.get("updated_at").is_some() && row["updated_at"].is_number(),
            "updated_at missing: {row}"
        );
        assert!(
            row.get("metrics_json").is_none(),
            "raw metrics_json must be omitted: {row}"
        );
        assert!(
            row.get("primary_metric_name").is_some(),
            "primary_metric_name missing: {row}"
        );
        assert!(
            row.get("primary_metric_value").is_some(),
            "primary_metric_value missing: {row}"
        );
    }
}

#[test]
fn retired_metric_campaign_does_not_leak_into_new_metric_less_campaign() {
    let (_temp, db) = harness_with_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    // create metrics row for first campaign so it would leak if scoped by project
    db.connect()
        .unwrap()
        .execute(
            "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES ('exp-baseline','manifest','loss',0.5,'{}',NULL,100,100)",
            [],
        )
        .unwrap();
    db.connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET current_best_experiment_id='exp-baseline', plateau_count=1 WHERE campaign_id='campaign-eval'",
            [],
        )
        .unwrap();
    // retire first campaign
    db.connect()
        .unwrap()
        .execute(
            "UPDATE campaigns SET state='retired', state_reason='retired' WHERE campaign_id='campaign-eval'",
            [],
        )
        .unwrap();
    // start newer metric-less campaign
    let root = project.root_path.clone();
    let objective = pueue_agent::state::load_objective(&root).unwrap();
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let proposal = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "baseline2".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    CampaignRepository::new(&db)
        .start_with_baseline(
            StartCampaignRequest {
                campaign_id: "campaign-eval-2",
                project_id: "project-a",
                objective: &objective,
                initial_argv: &argv,
                baseline: &proposal,
                submission_id: "submission-baseline-2",
                experiment_id: "exp-baseline-2",
                proposal_id: "proposal-baseline-2",
                metadata: &json!({}),
                origin_agent_run_id: None,
                objective_metric: None,
                now: 200,
            },
            &CampaignLimits::default(),
        )
        .unwrap();

    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![]),
        now_override: Some(300),
    };
    let human = render_project_status(&db, &project, &input).unwrap();
    let human_compact =
        pueue_agent::status::render_project_status_compact(&db, &project, &input).unwrap();
    for rendered in [&human, &human_compact] {
        assert!(
            !rendered.contains("best:"),
            "leaked metric campaign must not show best: for new metric-less campaign: {rendered}"
        );
        assert!(
            !rendered.contains("plateau:"),
            "leaked metric campaign must not show plateau: for new metric-less campaign: {rendered}"
        );
    }
    let json = render_project_status_json(&db, &project, &input).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    assert!(
        value["campaign"].get("best_experiment_id").is_none(),
        "leaked best_experiment_id: {json}"
    );
    assert!(
        value["campaign"].get("plateau_count").is_none(),
        "leaked plateau_count: {json}"
    );
    assert!(
        value.get("evaluation").is_none(),
        "leaked evaluation from retired campaign: {json}"
    );
}

#[test]
fn malformed_cross_campaign_best_pointer_is_not_exposed() {
    let (_temp, db) = harness_with_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    // create second campaign with its own metric row
    let root = project.root_path.clone();
    let objective = pueue_agent::state::load_objective(&root).unwrap();
    let argv = vec!["python".to_owned(), "train.py".to_owned()];
    let _proposal2 = proposals::validate_initial_baseline(
        ProposalInput {
            kind: ProposalKind::Experiment,
            hypothesis: "baseline other".to_owned(),
            source_experiment_id: None,
            argv: argv.clone(),
            working_directory: ".".to_owned(),
            expected_evidence: Vec::new(),
        },
        &objective.digest,
    )
    .unwrap();
    // need to retire first campaign to allow second baseline, then recreate first as active? Instead create second via direct SQL to keep first active
    // Insert second campaign directly with state retired to avoid live-campaign check, then point first's best to its experiment
    let now = 150;
    db.connect().unwrap().execute(
        "INSERT INTO campaigns (campaign_id, project_id, objective_text, objective_digest, initial_argv_json, state, state_reason, baseline_experiment_id, next_eligible_at, objective_metric_json, current_best_experiment_id, plateau_count, created_at, updated_at) VALUES ('campaign-other','project-a','obj','digest','[]','retired',NULL,NULL,NULL,NULL,NULL,0,?1,?1)",
        [now],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, status, kind, metadata_json) VALUES ('sub-other','project-a','[]',?1,'pending','experiment','{}')",
        [now],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO proposals (proposal_id, campaign_id, kind, status, hypothesis, argv_json, working_directory, expected_evidence_json, canonical_digest, created_at, updated_at) VALUES ('prop-other','campaign-other','experiment','accepted','h','[]','.', '[]','digest-other',?1,?1)",
        [now],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id, attempt, status, created_at, updated_at) VALUES ('exp-other','campaign-other','prop-other','sub-other',0,'succeeded',?1,?1)",
        [now],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES ('exp-other','manifest','loss',0.1,'{}',NULL,?1,?1)",
        [now],
    ).unwrap();
    // point first campaign's best to cross-campaign experiment
    db.connect().unwrap().execute(
        "UPDATE campaigns SET current_best_experiment_id='exp-other', plateau_count=1 WHERE campaign_id='campaign-eval'",
        [],
    ).unwrap();
    // also need metric for baseline to not confuse, but cross pointer is the best
    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![]),
        now_override: Some(300),
    };
    let human = render_project_status(&db, &project, &input).unwrap();
    // human best should be none, not the cross-campaign id
    assert!(
        human.contains("best: none") || !human.contains("exp-other") && !human.contains("exp-ot"),
        "cross-campaign best must not be exposed in human: {human}"
    );
    let json = render_project_status_json(&db, &project, &input).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    assert!(
        value["campaign"].get("best_experiment_id").is_none(),
        "cross-campaign best must not be exposed in JSON: {json}"
    );
    assert!(
        value["campaign"].get("best_metric_name").is_none(),
        "cross-campaign metric name must not be exposed: {json}"
    );
    assert!(
        value["campaign"].get("best_metric_value").is_none(),
        "cross-campaign metric value must not be exposed: {json}"
    );
    // plateau should remain? Finding 2 says do not retain unvalidated best ID, but plateau may remain? Spec says project best ID only when metrics row belongs to same campaign – plateau is independent? However we still should keep plateau? The test only checks best fields absence.
}

#[test]
fn non_ascii_best_id_does_not_panic_and_uses_char_short_id() {
    let (_temp, db) = harness_with_metric();
    let project = ProjectRepository::new(&db)
        .find_by_id("project-a")
        .unwrap()
        .unwrap();
    let non_ascii = "éééééééééé-extra";
    // create experiment with non-ASCII id
    db.connect().unwrap().execute(
        "INSERT INTO submissions (submission_id, project_id, argv_json, created_at, status, kind, metadata_json) VALUES ('sub-nonascii','project-a','[]',200,'pending','experiment','{}')",
        [],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiments (experiment_id, campaign_id, proposal_id, submission_id, attempt, status, created_at, updated_at) VALUES (?1,'campaign-eval','proposal-baseline','sub-nonascii',2,'succeeded',200,200)",
        rusqlite::params![non_ascii],
    ).unwrap();
    db.connect().unwrap().execute(
        "INSERT INTO experiment_metrics (experiment_id, source, primary_metric_name, primary_metric_value, metrics_json, artifact_defect, created_at, updated_at) VALUES (?1,'manifest','loss',0.9,'{}',NULL,200,200)",
        rusqlite::params![non_ascii],
    ).unwrap();
    db.connect().unwrap().execute(
        "UPDATE campaigns SET current_best_experiment_id=?1, plateau_count=0 WHERE campaign_id='campaign-eval'",
        rusqlite::params![non_ascii],
    ).unwrap();
    let input = StatusInput {
        daemon_health: ServiceStatus::Running,
        pueue: PueueSnapshot::Tasks(vec![]),
        now_override: Some(300),
    };
    // should not panic
    let human = render_project_status(&db, &project, &input).unwrap();
    // first 8 characters = 8 times é
    let expected_short = "éééééééé";
    assert!(
        human.contains(&format!("best: id={expected_short}")),
        "non-ASCII short id must be first 8 chars: {human}"
    );
    let json = render_project_status_json(&db, &project, &input).unwrap();
    let value: Value = serde_json::from_str(&json).unwrap();
    assert_eq!(value["campaign"]["best_experiment_id"], non_ascii, "JSON must retain full non-ASCII id");
}
