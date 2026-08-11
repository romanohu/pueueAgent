use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use pueue_agent::{
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
    executable: PathBuf,
    capture_path: PathBuf,
}

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
        let executable = temp.path().join("fake-pueue");
        let capture_path = temp.path().join("args.bin");
        let status_path = temp.path().join("status.json");
        let add_path = temp.path().join("add.txt");
        let group_index_path = temp.path().join("group-list-index.txt");
        let group_list_dir = temp.path().join("group-lists");
        fs::write(&status_path, status_stdout).unwrap();
        fs::write(&add_path, add_stdout).unwrap();
        fs::write(&capture_path, "").unwrap();
        fs::write(&group_index_path, "0").unwrap();
        fs::create_dir_all(&group_list_dir).unwrap();
        for (index, group_list) in group_lists.iter().enumerate() {
            fs::write(group_list_dir.join(format!("{index}.json")), group_list).unwrap();
        }
        let last_group_list_index = group_lists.len().saturating_sub(1);

        let script = format!(
            r#"#!/bin/sh
set -eu
capture_path={capture_path}
status_path={status_path}
add_path={add_path}
group_index_path={group_index_path}
group_list_dir={group_list_dir}
last_group_list_index={last_group_list_index}
fail_operation={fail_operation}
operation=""
group_subcommand=""
for argument in "$@"; do
    printf '%s\0' "$argument" >> "$capture_path"
    if [ -z "$operation" ]; then
        case "$argument" in
            status|add|group|kill|remove)
                operation="$argument"
                ;;
        esac
    elif [ "$operation" = "group" ] && [ -z "$group_subcommand" ]; then
        case "$argument" in
            -j|--json) ;;
            *) group_subcommand="$argument" ;;
        esac
    fi
done
printf '\n' >> "$capture_path"
if [ "$operation" = "$fail_operation" ]; then
    printf 'partial output'
    printf 'daemon unavailable' >&2
    exit 7
fi
case "$operation" in
    status) /bin/cat "$status_path" ;;
    add) /bin/cat "$add_path" ;;
    group)
        if [ "$group_subcommand" = "add" ]; then
            if [ "$fail_operation" = "group-add" ]; then
                printf 'partial output'
                printf 'daemon unavailable' >&2
                exit 7
            fi
            exit 0
        fi
        index=$(/bin/cat "$group_index_path")
        /bin/cat "$group_list_dir/$index.json"
        if [ "$index" -lt "$last_group_list_index" ]; then
            next_index=$((index + 1))
            printf '%s' "$next_index" > "$group_index_path"
        fi
        ;;
    kill) : ;;
    remove) : ;;
    *) printf 'missing operation' >&2; exit 9 ;;
esac
"#,
            capture_path = shell_quote(&capture_path),
            status_path = shell_quote(&status_path),
            add_path = shell_quote(&add_path),
            group_index_path = shell_quote(&group_index_path),
            group_list_dir = shell_quote(&group_list_dir),
            last_group_list_index = last_group_list_index,
            fail_operation = shell_quote(Path::new(fail_operation.unwrap_or(""))),
        );
        fs::write(&executable, script).unwrap();
        make_executable(&executable);

        Self {
            _temp: temp,
            executable,
            capture_path,
        }
    }

    pub fn executable(&self) -> &Path {
        &self.executable
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

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}
