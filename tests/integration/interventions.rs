use std::{
    fs,
    path::{Path, PathBuf},
};

use assert_cmd::Command;
use pueue_agent::{
    config,
    db::{Db, InterventionRepository, ProjectRepository},
    interventions::{MAX_INTERVENTIONS_PER_RUN, MAX_INTERVENTION_BYTES},
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
fn steer_accepts_the_byte_limit_and_joins_message_arguments_with_spaces() {
    let harness = SteerHarness::new();
    let exact_limit_message = "x".repeat(MAX_INTERVENTION_BYTES);
    let exact_limit_output = harness
        .command(&harness.first_root)
        .args(["steer", "--", &exact_limit_message])
        .output()
        .unwrap();
    assert!(exact_limit_output.status.success());

    let joined_output = harness
        .command(&harness.first_root)
        .args(["steer", "--", "first", "second", "third"])
        .output()
        .unwrap();
    assert!(joined_output.status.success());

    let project_id = harness.project_id(&harness.first_root);
    let queued = InterventionRepository::new(&harness.db)
        .list(
            &project_id,
            pueue_agent::interventions::InterventionStatus::Pending,
            MAX_INTERVENTIONS_PER_RUN,
        )
        .unwrap();
    assert_eq!(queued.len(), 2);
    assert_eq!(queued[0].message, exact_limit_message);
    assert_eq!(queued[1].message, "first second third");
}

#[test]
fn steer_list_text_and_json_are_fifo_bounded_and_project_scoped() {
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
    assert_eq!(
        interventions
            .iter()
            .map(|item| item["message"].as_str().unwrap())
            .collect::<Vec<_>>(),
        (0..MAX_INTERVENTIONS_PER_RUN)
            .map(|index| format!("first project intervention {index}"))
            .collect::<Vec<_>>()
    );
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

    let text_output = harness
        .command(&harness.first_root)
        .args(["steer", "list"])
        .output()
        .unwrap();

    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).unwrap();
    let lines = text.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), MAX_INTERVENTIONS_PER_RUN);
    assert!(lines[0].ends_with("first project intervention 0"));
    assert!(lines[MAX_INTERVENTIONS_PER_RUN - 1].ends_with(&format!(
        "first project intervention {}",
        MAX_INTERVENTIONS_PER_RUN - 1
    )));
    assert!(lines
        .iter()
        .all(|line| !line.contains("second project secret")));
}

#[test]
fn steer_list_text_and_json_strip_controls() {
    let harness = SteerHarness::new();
    let message = "line\n\t\x1b[31mred";
    let project_id = harness.project_id(&harness.first_root);
    InterventionRepository::new(&harness.db)
        .insert_pending(&project_id, message, 300)
        .unwrap();

    let text_output = harness
        .command(&harness.first_root)
        .args(["steer", "list"])
        .output()
        .unwrap();
    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).unwrap();
    assert!(text.contains("line red"));
    assert!(!text.contains(message));
    assert!(!text.contains('\x1b'));

    let json_output = harness
        .command(&harness.first_root)
        .args(["steer", "list", "--json"])
        .output()
        .unwrap();
    assert!(json_output.status.success());
    let body: Value = serde_json::from_slice(&json_output.stdout).unwrap();
    assert_eq!(body["interventions"][0]["message"], "line red");
    assert!(!String::from_utf8_lossy(&json_output.stdout).contains('\x1b'));
}

#[test]
fn steer_list_redacts_and_bounds_message_in_human_and_json_output() {
    let harness = SteerHarness::new();
    let project_id = harness.project_id(&harness.first_root);
    let message = format!(
        "analysis --access-token separate-secret AWS_ACCESS_KEY_ID=AKIASECRET {}",
        "x".repeat(300)
    );
    InterventionRepository::new(&harness.db)
        .insert_pending(&project_id, &message, 300)
        .unwrap();

    let text_output = harness
        .command(&harness.first_root)
        .args(["steer", "list"])
        .output()
        .unwrap();
    assert!(text_output.status.success());
    let text = String::from_utf8(text_output.stdout).unwrap();
    assert!(!text.contains("separate-secret"));
    assert!(!text.contains("AKIASECRET"));
    assert!(text.contains("[REDACTED]"));
    assert!(!text.contains('\x1b'));

    let json_output = harness
        .command(&harness.first_root)
        .args(["steer", "list", "--json"])
        .output()
        .unwrap();
    assert!(json_output.status.success());
    let body: Value = serde_json::from_slice(&json_output.stdout).unwrap();
    let rendered_message = body["interventions"][0]["message"].as_str().unwrap();
    assert!(rendered_message.len() <= 240);
    assert!(!rendered_message.contains("separate-secret"));
    assert!(!rendered_message.contains("AKIASECRET"));
    assert!(!String::from_utf8_lossy(&json_output.stdout).contains('\x1b'));
}
