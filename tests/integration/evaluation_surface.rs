use std::fs;

use pueue_agent::{
    db::{CampaignRepository, Db, ProjectRepository, StartCampaignRequest},
    diagnostics::render_project_status_json,
    execution_policy::CampaignLimits,
    models::{MetricDirection, ObjectiveMetric, ProposalKind},
    proposals::{self, ProposalInput},
    service::ServiceStatus,
    status::{render_project_status, PueueSnapshot, StatusInput},
};
use serde_json::{json, Value};
use tempfile::TempDir;

fn harness_with_metric() -> (TempDir, Db) {
    let temp = TempDir::new().unwrap();
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
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
    let state_dir = root.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
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
    let root = temp.path().join("project");
    fs::create_dir_all(&root).unwrap();
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
    let state_dir = root.join(".pueue-agent");
    fs::create_dir_all(&state_dir).unwrap();
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
