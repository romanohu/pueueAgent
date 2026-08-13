#![cfg(unix)]

use std::{
    collections::BTreeSet,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use pueue_agent::execution_policy::{
    load_existing_policy, PolicyLoadInput, ResolvedExecutionPolicy, StartupEnvironment,
};

pub fn resolved_policy(
    fixture_root: &Path,
    projects: &[(&str, &Path, &Path)],
) -> Arc<ResolvedExecutionPolicy> {
    let fixture_root = fs::canonicalize(fixture_root).expect("canonical fixture root");
    let state_dir = fixture_root.join("execution-policy-state");
    let trusted_dir = fixture_root.join("execution-policy-bin");
    let codex_home = fixture_root.join("execution-policy-codex-home");
    for directory in [&state_dir, &trusted_dir, &codex_home] {
        fs::create_dir_all(directory).expect("create policy fixture directory");
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .expect("secure policy fixture directory");
    }

    let launcher = trusted_dir.join("pueue-agent-launcher");
    if !launcher.exists() {
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher)
            .expect("copy descriptor-bound launcher fixture");
    }
    fs::set_permissions(&launcher, fs::Permissions::from_mode(0o700))
        .expect("secure launcher fixture");
    let pueue_config = fixture_root.join("execution-policy-pueue.yml");
    fs::write(&pueue_config, "fixture: true\n").expect("write pueue fixture config");
    fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600))
        .expect("secure pueue fixture config");

    let mut trusted_path = BTreeSet::from([trusted_dir.clone()]);
    let mut project_entries = String::new();
    let mut project_roots = Vec::new();
    for (project_id, root, program) in projects {
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))
            .expect("secure policy fixture project root");
        fs::set_permissions(
            root.join(".pueue-agent"),
            fs::Permissions::from_mode(0o700),
        )
        .expect("secure policy fixture service directory");
        let root = fs::canonicalize(root).expect("canonical policy fixture project root");
        project_roots.push(root);
        if *program == Path::new("codex") {
            continue;
        }
        let program = fs::canonicalize(program).expect("canonical policy fixture agent");
        trusted_path.insert(program.parent().expect("agent parent").to_owned());
        project_entries.push_str(&format!(
            "\n[projects.{project_id:?}]\ncustom_agent = {:?}\n",
            program.display().to_string(),
        ));
    }
    let joined_path = std::env::join_paths(&trusted_path).expect("join trusted fixture path");
    fs::write(
        state_dir.join("execution-policy.toml"),
        format!(
            "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n{}",
            joined_path.to_string_lossy(),
            launcher.display().to_string(),
            launcher.display().to_string(),
            project_entries,
        ),
    )
    .expect("write execution policy fixture");
    fs::set_permissions(
        state_dir.join("execution-policy.toml"),
        fs::Permissions::from_mode(0o600),
    )
    .expect("secure execution policy fixture");

    Arc::new(
        load_existing_policy(&PolicyLoadInput {
            state_dir,
            project_roots,
            inherited_path: joined_path,
            startup_environment: StartupEnvironment::from_pairs([("HOME", "/fixture")]),
            codex_home,
            pueue_config,
            launcher_path: launcher,
        })
        .expect("resolve execution policy fixture"),
    )
}

#[allow(dead_code)]
pub fn prepare_configured_program(
    fixture_root: &Path,
    project_id: &str,
    config_path: &Path,
) -> PathBuf {
    let Ok(config) = pueue_agent::config::load(config_path) else {
        return PathBuf::from("codex");
    };
    let configured = PathBuf::from(&config.agent.program);
    if configured == Path::new("codex") || !configured.is_absolute() || !configured.is_file() {
        return PathBuf::from("codex");
    }
    let base = fs::canonicalize(fixture_root).expect("canonical fixture program root");
    let trusted = base.join("execution-policy-bin");
    fs::create_dir_all(&trusted).expect("create fixture agent directory");
    fs::set_permissions(&trusted, fs::Permissions::from_mode(0o700))
        .expect("secure fixture agent directory");
    let target = trusted.join(format!("agent-{project_id}"));
    if configured != target {
        let original = fs::read_to_string(&configured).unwrap_or_default();
        let custom_capture = if original.contains("PUEUE_AGENT_RUN_ID") {
            original
                .lines()
                .find_map(|line| line.rsplit_once('>').map(|(_, path)| path.trim().to_owned()))
        } else {
            None
        };
        let default_exit = configured
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "false")
            .then_some(1)
            .unwrap_or(0);
        let capture_expression = custom_capture
            .as_deref()
            .map(|path| format!("Some({path:?})"))
            .unwrap_or_else(|| "None::<&str>".to_owned());
        let source = trusted.join(format!("agent-{project_id}.rs"));
        fs::write(
            &source,
            format!(
                r#"use std::{{env, fs, process::{{Command, exit}}, thread, time::Duration}};
fn main() {{
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.first().map(String::as_str) == Some("--fixture-descendant") {{
        extern "C" {{ fn signal(number: i32, handler: usize) -> usize; }}
        unsafe {{ let _ = signal(15, 1); }}
        loop {{ thread::sleep(Duration::from_millis(25)); }}
    }}
    if args.first().map(String::as_str) == Some("--wait-for-release") {{
        let release = args.get(1).expect("release fixture path");
        fs::write(format!("{{release}}.ready"), b"ready").unwrap();
        while !std::path::Path::new(release).exists() {{
            thread::sleep(Duration::from_millis(10));
        }}
        return;
    }}
    if let Some(index) = args.iter().position(|arg| arg == "--background-exit") {{
        let pid_path = args.get(index + 1).expect("background pid path");
        let child = Command::new(env::current_exe().unwrap()).arg("--fixture-descendant").spawn().unwrap();
        fs::write(pid_path, child.id().to_string()).unwrap();
        return;
    }}
    if let Some(path) = {capture_expression} {{
        let value = format!("{{}}:{{}}", env::var("PUEUE_AGENT_RUN_ID").unwrap(), env::var("PUEUE_AGENT_PROJECT_ID").unwrap());
        fs::write(path, value).unwrap();
        return;
    }}
    if args.first().map(String::as_str) == Some("-c") {{
        let script = args.get(1).map(String::as_str).unwrap_or("");
        if script.contains("descendant.pid") {{
            let mut child = Command::new(env::current_exe().unwrap()).arg("--fixture-descendant").spawn().unwrap();
            fs::write(".pueue-agent/logs/descendant.pid", child.id().to_string()).unwrap();
            let _ = child.wait();
            return;
        }}
        if let Some(code) = script.strip_prefix("exit ") {{ exit(code.trim().parse().unwrap()); }}
        if let Some(seconds) = script.strip_prefix("sleep ") {{
            thread::sleep(Duration::from_secs_f64(seconds.trim().parse().unwrap()));
            return;
        }}
    }}
    exit({default_exit});
}}
"#
            ),
        )
        .expect("write generated fixture agent");
        let output = Command::new("rustc")
            .args(["--edition=2021", "-o"])
            .arg(&target)
            .arg(&source)
            .output()
            .expect("compile generated fixture agent");
        assert!(
            output.status.success(),
            "generated fixture agent failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fs::set_permissions(&target, fs::Permissions::from_mode(0o700))
            .expect("secure fixture agent");
        let body = fs::read_to_string(config_path).expect("read fixture config for enrollment");
        let replacement = format!("program = {:?}", target.display().to_string());
        let mut replaced = false;
        let body = body
            .lines()
            .map(|line| {
                if !replaced && line.trim_start().starts_with("program = ") {
                    replaced = true;
                    replacement.as_str()
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(config_path, format!("{body}\n")).expect("bind fixture config to enrollment");
    }
    target
}
