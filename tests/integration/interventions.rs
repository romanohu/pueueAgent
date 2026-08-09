use std::{
    fs,
    path::{Path, PathBuf},
};

use assert_cmd::Command;
use pueue_agent::{
    config,
    db::{Db, InterventionRepository, ProjectRepository},
    interventions::MAX_INTERVENTIONS_PER_RUN,
    models::NewProject,
};
use serde_json::Value;
use tempfile::TempDir;

struct SteerHarness {
    temp: TempDir,
    state_dir: PathBuf,
    first_root: PathBuf,
    second_root: PathBuf,
    db: Db,
}

impl SteerHarness {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let state_dir = temp.path().join("state");
        let first_root = initialize_project(temp.path(), "first");
        let second_root = initialize_project(temp.path(), "second");
        let db = Db::open(&state_dir.join("state.sqlite3")).unwrap();

        for root in [&first_root, &second_root] {
            let project = config::load(&root.join(".pueue-agent/config.toml")).unwrap();
            ProjectRepository::new(&db)
                .register(&NewProject::new(
                    project.project_id,
                    root,
                    project.pueue_group,
                    root.join(".pueue-agent/config.toml"),
                    100,
                ))
                .unwrap();
        }

        Self {
            temp,
            state_dir,
            first_root,
            second_root,
            db,
        }
    }

    fn command(&self, project_root: &Path) -> Command {
        let mut command = Command::cargo_bin("pueue-agent").unwrap();
        command
            .env("PUEUE_AGENT_STATE_DIR", &self.state_dir)
            .current_dir(project_root);
        command
    }

    fn project_id(&self, project_root: &Path) -> String {
        config::load(&project_root.join(".pueue-agent/config.toml"))
            .unwrap()
            .project_id
    }
}

fn initialize_project(parent: &Path, name: &str) -> PathBuf {
    let root = parent.join(name);
    fs::create_dir(&root).unwrap();
    pueue_agent::init::run(&root).unwrap();
    root
}

#[test]
fn steer_queues_the_exact_message_without_starting_an_agent_or_pueue() {
    let harness = SteerHarness::new();
    let marker = harness.temp.path().join("pueue-invoked");
    let bin_dir = harness.temp.path().join("bin");
    fs::create_dir(&bin_dir).unwrap();
    let fake_pueue = bin_dir.join("pueue");
    fs::write(
        &fake_pueue,
        "#!/bin/sh\nprintf invoked > \"$PUEUE_STEER_MARKER\"\nexit 1\n",
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        fs::set_permissions(&fake_pueue, fs::Permissions::from_mode(0o755)).unwrap();
    }

    let message = "次の実験では learning rate を下げる";
    let output = harness
        .command(&harness.first_root)
        .args(["steer", "--", message])
        .env("PUEUE_STEER_MARKER", &marker)
        .env("PATH", &bin_dir)
        .output()
        .unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let intervention_id = stdout
        .strip_prefix("queued intervention: ")
        .and_then(|value| value.trim().strip_suffix('\n').or(Some(value.trim())))
        .expect("steer should return a queued intervention ID");
    assert!(!intervention_id.is_empty());
    assert!(!marker.exists());

    let project_id = harness.project_id(&harness.first_root);
    let queued = InterventionRepository::new(&harness.db)
        .list(
            &project_id,
            pueue_agent::interventions::InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )
        .unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].intervention_id, intervention_id);
    assert_eq!(queued[0].message, message);

    let agent_run_count: i64 = harness
        .db
        .connect()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM agent_runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(agent_run_count, 0);
}

#[test]
fn steer_rejects_blank_and_over_limit_messages() {
    let harness = SteerHarness::new();

    for message in ["   ".to_owned(), "x".repeat(4 * 1024 + 1)] {
        let output = harness
            .command(&harness.first_root)
            .args(["steer", "--", &message])
            .output()
            .unwrap();

        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("configuration error in intervention_message"));
    }

    let project_id = harness.project_id(&harness.first_root);
    assert!(InterventionRepository::new(&harness.db)
        .list(
            &project_id,
            pueue_agent::interventions::InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )
        .unwrap()
        .is_empty());
}

#[test]
fn steer_json_reports_the_pending_intervention_and_accepts_hyphenated_text() {
    let harness = SteerHarness::new();
    let message = "- keep the next intervention focused";

    let output = harness
        .command(&harness.first_root)
        .args(["steer", "--json", "--", message])
        .output()
        .unwrap();

    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["project_id"], harness.project_id(&harness.first_root));
    assert_eq!(body["status"], "pending");
    assert!(body["intervention_id"].is_string());

    let project_id = harness.project_id(&harness.first_root);
    let queued = InterventionRepository::new(&harness.db)
        .list(
            &project_id,
            pueue_agent::interventions::InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )
        .unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].message, message);
}

#[test]
fn steer_list_json_is_project_scoped_and_bounded() {
    let harness = SteerHarness::new();
    let first_id = harness.project_id(&harness.first_root);
    let second_id = harness.project_id(&harness.second_root);

    for index in 0..=MAX_INTERVENTIONS_PER_RUN {
        InterventionRepository::new(&harness.db)
            .insert_pending(
                &first_id,
                &format!("first project intervention {index}"),
                100 + index as i64,
            )
            .unwrap();
    }
    InterventionRepository::new(&harness.db)
        .insert_pending(&second_id, "second project secret", 200)
        .unwrap();

    let output = harness
        .command(&harness.first_root)
        .args(["steer", "list", "--json"])
        .output()
        .unwrap();

    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["schema_version"], 1);
    assert_eq!(body["project_id"], first_id);
    let interventions = body["interventions"]
        .as_array()
        .expect("JSON list should contain interventions");
    assert_eq!(interventions.len(), MAX_INTERVENTIONS_PER_RUN);
    assert!(interventions.iter().all(|item| {
        item["intervention_id"].is_string()
            && item["status"] == "pending"
            && item["created_at"].is_i64()
            && item["message"]
                .as_str()
                .is_some_and(|message| message.len() <= 4 * 1024)
    }));
    assert!(interventions
        .iter()
        .all(|item| item["message"] != "second project secret"));
}
