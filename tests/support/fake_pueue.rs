use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use async_trait::async_trait;
use pueue_agent::{
    execution_policy::{
        load_existing_policy, PolicyLoadInput, ResolvedExecutionPolicy, StartupEnvironment,
    },
    pueue::{PueueApi, PueueError, PueueTask},
    AppError,
};
use tempfile::TempDir;
use tokio::sync::Notify;

#[derive(Clone)]
pub struct FakePueue {
    state: Arc<FakePueueState>,
}

struct FakePueueState {
    tasks: Mutex<Vec<PueueTask>>,
    add_task_id: Mutex<i64>,
    add_calls: Mutex<Vec<Vec<OsString>>>,
    kill_calls: Mutex<Vec<i64>>,
    group_calls: Mutex<Vec<String>>,
    fail_add: AtomicBool,
    pause_add: AtomicBool,
    add_entered: Notify,
    add_release: Notify,
}

impl FakePueue {
    pub fn new() -> Self {
        Self {
            state: Arc::new(FakePueueState {
                tasks: Mutex::new(Vec::new()),
                add_task_id: Mutex::new(41),
                add_calls: Mutex::new(Vec::new()),
                kill_calls: Mutex::new(Vec::new()),
                group_calls: Mutex::new(Vec::new()),
                fail_add: AtomicBool::new(false),
                pause_add: AtomicBool::new(false),
                add_entered: Notify::new(),
                add_release: Notify::new(),
            }),
        }
    }

    pub fn with_add_task_id(self, task_id: i64) -> Self {
        *self.state.add_task_id.lock().unwrap() = task_id;
        self
    }

    pub fn with_add_failure(self) -> Self {
        self.state.fail_add.store(true, Ordering::SeqCst);
        self
    }

    pub fn pause_add(&self) {
        self.state.pause_add.store(true, Ordering::SeqCst);
    }

    pub async fn wait_for_add(&self) {
        self.state.add_entered.notified().await;
    }

    pub fn release_add(&self) {
        self.state.add_release.notify_one();
    }

    pub fn last_add_args(&self) -> Vec<OsString> {
        self.state
            .add_calls
            .lock()
            .unwrap()
            .last()
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait]
impl PueueApi for FakePueue {
    async fn status_json(&self) -> Result<Vec<PueueTask>, AppError> {
        Ok(self.state.tasks.lock().unwrap().clone())
    }

    async fn add(&self, args: &[OsString]) -> Result<i64, AppError> {
        self.state.add_calls.lock().unwrap().push(args.to_vec());
        if self.state.pause_add.load(Ordering::SeqCst) {
            self.state.add_entered.notify_one();
            self.state.add_release.notified().await;
        }
        if self.state.fail_add.load(Ordering::SeqCst) {
            return Err(PueueError::CommandFailed {
                operation: "add",
                exit_code: Some(7),
                stdout: b"partial output".to_vec(),
                stderr: b"daemon unavailable".to_vec(),
            }
            .into());
        }
        Ok(*self.state.add_task_id.lock().unwrap())
    }

    async fn kill(&self, task_id: i64) -> Result<(), AppError> {
        self.state.kill_calls.lock().unwrap().push(task_id);
        Ok(())
    }

    async fn remove(&self, _task_id: i64) -> Result<(), AppError> {
        Ok(())
    }

    async fn ensure_group(&self, group: &str) -> Result<(), AppError> {
        self.state
            .group_calls
            .lock()
            .unwrap()
            .push(group.to_owned());
        Ok(())
    }
}

pub struct FakePueueCommand {
    _temp: TempDir,
    policy: Arc<ResolvedExecutionPolicy>,
    capture_path: PathBuf,
}

const GENERATED_PUEUE_SOURCE: &str = r#"
use std::{
    env,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::Write,
    os::unix::ffi::OsStrExt,
    path::Path,
};

fn main() {
    // Record only user-visible arguments. argv[0] is the executable identity
    // supplied to exec and must never be mistaken for a Pueue option.
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    let mut capture = OpenOptions::new()
        .append(true)
        .open(__CAPTURE_PATH__)
        .expect("open argv capture");
    for argument in &arguments {
        capture.write_all(argument.as_bytes()).expect("capture argument");
        capture.write_all(&[0]).expect("capture separator");
    }
    capture.write_all(b"\n").expect("capture invocation");

    let operation_index = arguments
        .iter()
        .position(|argument| matches!(argument.to_str(), Some("status" | "add" | "group" | "kill" | "remove")))
        .expect("Pueue operation");
    let operation = arguments[operation_index].to_string_lossy();
    let group_subcommand = arguments
        .iter()
        .skip(operation_index + 1)
        .find(|argument| !matches!(argument.to_str(), Some("-j" | "--json")))
        .map(|argument| argument.to_string_lossy().into_owned())
        .unwrap_or_default();
    let fail_operation = __FAIL_OPERATION__;
    if operation == fail_operation || (operation == "group" && group_subcommand == "add" && fail_operation == "group-add") {
        std::io::stdout().write_all(b"partial output").expect("write failure stdout");
        std::io::stderr().write_all(b"daemon unavailable").expect("write failure stderr");
        std::process::exit(7);
    }

    match operation.as_ref() {
        "status" => std::io::stdout().write_all(&fs::read(__STATUS_PATH__).expect("read status fixture")).expect("write status fixture"),
        "add" => std::io::stdout().write_all(&fs::read(__ADD_PATH__).expect("read add fixture")).expect("write add fixture"),
        "group" if group_subcommand == "add" => {}
        "group" => {
            let index = fs::read_to_string(__GROUP_INDEX_PATH__)
                .expect("read group fixture index")
                .parse::<usize>()
                .expect("parse group fixture index");
            let group_path = Path::new(__GROUP_LIST_DIR__).join(format!("{index}.json"));
            std::io::stdout().write_all(&fs::read(group_path).expect("read group fixture")).expect("write group fixture");
            if index < __LAST_GROUP_LIST_INDEX__ {
                fs::write(__GROUP_INDEX_PATH__, (index + 1).to_string()).expect("advance group fixture");
            }
        }
        "kill" | "remove" => {}
        _ => std::process::exit(9),
    }
}
"#;

impl FakePueueCommand {
    pub fn new(status_stdout: &str, add_stdout: &str, fail_operation: Option<&str>) -> Self {
        Self::new_with_group_lists(
            status_stdout,
            add_stdout,
            &[r#"{"default":{"parallel_tasks":1}}"#],
            fail_operation,
        )
    }

    pub fn new_with_group_lists(
        status_stdout: &str,
        add_stdout: &str,
        group_lists: &[&str],
        fail_operation: Option<&str>,
    ) -> Self {
        let temp = TempDir::new().unwrap();
        let base = fs::canonicalize(temp.path()).unwrap();
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_dir = base.join("trusted");
        let codex_home = base.join("codex-home");
        let group_list_dir = base.join("group-lists");
        for directory in [&state_dir, &project_root, &trusted_dir, &codex_home, &group_list_dir] {
            fs::create_dir(directory).unwrap();
            set_mode(directory, 0o700);
        }

        let executable = trusted_dir.join("pueue");
        let capture_path = base.join("args.bin");
        let status_path = base.join("status.json");
        let add_path = base.join("add.txt");
        let group_index_path = base.join("group-list-index.txt");
        fs::write(&status_path, status_stdout).unwrap();
        fs::write(&add_path, add_stdout).unwrap();
        fs::write(&capture_path, "").unwrap();
        fs::write(&group_index_path, "0").unwrap();
        fs::create_dir_all(&group_list_dir).unwrap();
        for (index, group_list) in group_lists.iter().enumerate() {
            fs::write(group_list_dir.join(format!("{index}.json")), group_list).unwrap();
        }
        let last_group_list_index = group_lists.len().saturating_sub(1);

        let source_path = base.join("fake-pueue.rs");
        let source = GENERATED_PUEUE_SOURCE
            .replace("__CAPTURE_PATH__", &rust_string(&capture_path))
            .replace("__STATUS_PATH__", &rust_string(&status_path))
            .replace("__ADD_PATH__", &rust_string(&add_path))
            .replace("__GROUP_INDEX_PATH__", &rust_string(&group_index_path))
            .replace("__GROUP_LIST_DIR__", &rust_string(&group_list_dir))
            .replace("__LAST_GROUP_LIST_INDEX__", &last_group_list_index.to_string())
            .replace("__FAIL_OPERATION__", &format!("{:?}", fail_operation.unwrap_or("")));
        fs::write(&source_path, source).unwrap();
        let output = Command::new("rustc")
            .args(["--edition=2021", "-O", "-o"])
            .arg(&executable)
            .arg(&source_path)
            .output()
            .expect("compile generated Pueue fixture");
        assert!(
            output.status.success(),
            "generated Pueue fixture failed to compile: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        make_executable(&executable);

        let codex = trusted_dir.join("codex");
        fs::copy(&executable, &codex).unwrap();
        make_executable(&codex);
        let launcher = trusted_dir.join("launcher");
        fs::copy(env!("CARGO_BIN_EXE_pueue-agent"), &launcher).unwrap();
        make_executable(&launcher);
        let pueue_config = base.join("pueue.yml");
        fs::write(&pueue_config, b"fixture-config-fd9\n").unwrap();
        set_mode(&pueue_config, 0o600);
        fs::write(
            state_dir.join("execution-policy.toml"),
            format!(
                "version = 1\ntrusted_path = {:?}\n\n[executables]\ncodex = {:?}\npueue = {:?}\n",
                trusted_dir.display().to_string(),
                codex.display().to_string(),
                executable.display().to_string(),
            ),
        )
        .unwrap();
        set_mode(&state_dir.join("execution-policy.toml"), 0o600);
        let policy = load_existing_policy(&PolicyLoadInput {
            state_dir,
            project_roots: vec![project_root],
            inherited_path: trusted_dir.into_os_string(),
            startup_environment: StartupEnvironment::from_pairs([("HOME", base.as_os_str())]),
            codex_home,
            pueue_config,
            launcher_path: launcher,
        })
        .unwrap();

        Self {
            _temp: temp,
            policy: Arc::new(policy),
            capture_path,
        }
    }

    pub fn policy(&self) -> Arc<ResolvedExecutionPolicy> {
        Arc::clone(&self.policy)
    }

    pub fn captured_args(&self) -> Vec<OsString> {
        self.captured_invocations()
            .last()
            .cloned()
            .unwrap_or_default()
    }

    pub fn captured_invocations(&self) -> Vec<Vec<OsString>> {
        let bytes = fs::read(&self.capture_path).unwrap();
        bytes
            .split(|byte| *byte == b'\n')
            .filter(|invocation| !invocation.is_empty())
            .map(|invocation| {
                invocation
                    .split(|byte| *byte == 0)
                    .filter(|argument| !argument.is_empty())
                    .map(|argument| String::from_utf8(argument.to_vec()).unwrap().into())
                    .collect()
            })
            .collect()
    }
}

fn rust_string(path: &Path) -> String {
    format!("{:?}", path.to_string_lossy())
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) {}
