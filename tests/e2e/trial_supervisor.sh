#!/usr/bin/env bash
set -euo pipefail

if [ "$(uname -s)" != "Linux" ]; then
  echo "Trial E2E FAIL: real-Pueue trial acceptance requires Linux" >&2
  exit 1
fi
umask 077

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
BUILD_HOME="$HOME"
BUILD_CARGO_HOME="$(printenv CARGO_HOME 2>/dev/null || true)"
BUILD_RUSTUP_HOME="$(printenv RUSTUP_HOME 2>/dev/null || true)"
[ -n "$BUILD_CARGO_HOME" ] || BUILD_CARGO_HOME="$BUILD_HOME/.cargo"
[ -n "$BUILD_RUSTUP_HOME" ] || BUILD_RUSTUP_HOME="$BUILD_HOME/.rustup"
ORIGINAL_PATH="$PATH"
REAL_PUEUE="$(command -v pueue || true)"
REAL_PUEUED="$(command -v pueued || true)"
REAL_PYTHON="$(command -v python3 || command -v python || true)"
REAL_RUSTC="$(command -v rustc || true)"
CARGO_BIN="$(command -v cargo || true)"
PUEUE_VERSION=""
PUEUED_VERSION=""

die() {
  echo "Trial E2E FAIL: $*" >&2
  exit 1
}

[ -n "$REAL_PUEUE" ] || die "pueue is required"
[ -n "$REAL_PUEUED" ] || die "pueued is required"
[ -n "$REAL_PYTHON" ] || die "python is required"
[ -n "$REAL_RUSTC" ] || die "rustc is required"
[ -n "$CARGO_BIN" ] || die "cargo is required"
PUEUE_VERSION="$("$REAL_PUEUE" --version 2>&1)"
PUEUED_VERSION="$("$REAL_PUEUED" --version 2>&1)"
case "$PUEUE_VERSION" in
  *4.0.4*) ;;
  *) die "Pueue 4.0.4 is required (found: $PUEUE_VERSION)" ;;
esac
case "$PUEUED_VERSION" in
  *4.0.4*) ;;
  *) die "Pueued 4.0.4 is required (found: $PUEUED_VERSION)" ;;
esac

CARGO_TARGET_DIR="$(printenv CARGO_TARGET_DIR 2>/dev/null || true)"
if [ -z "$CARGO_TARGET_DIR" ]; then
  CARGO_TARGET_DIR="$REPO_ROOT/target"
elif [[ "$CARGO_TARGET_DIR" != /* ]]; then
  CARGO_TARGET_DIR="$(pwd -P)/$CARGO_TARGET_DIR"
fi
export CARGO_TARGET_DIR
PA_BIN="$CARGO_TARGET_DIR/release/pueue-agent"

WORK="$(mktemp -d /tmp/pa-trial-e2e.XXXXXX)"
chmod 700 "$WORK"
WORK_IDENTITY="$(stat -c '%d:%i:%u:%a' "$WORK")"
CONTROL="$WORK/control"
EVIDENCE="$WORK/evidence"
BIN="$WORK/bin"
PROJECT="$WORK/project"
PUEUE_DIR="$WORK/pueue"
RUNTIME="$WORK/runtime"
HOME="$WORK/home"
CODEX_HOME="$WORK/codex-home"
STATE_HOME="$WORK/state"
STATE_DIR="$STATE_HOME/pueue-agent"
PUEUE_CONFIG="$WORK/pueue.yml"
STATE_DB="$STATE_DIR/state.sqlite3"
PROJECT_ID=""
PROJECT_GROUP=""
PUEUED_PID=""
LAST_TRIAL_ID=""
LAST_TASK_ID=""
LAST_GROUP=""
KEEP_WORK=1

mkdir -m 700 "$CONTROL" "$EVIDENCE" "$BIN" "$PROJECT" "$PUEUE_DIR" \
  "$RUNTIME" "$HOME" "$CODEX_HOME" "$STATE_HOME" "$STATE_DIR"
export HOME CODEX_HOME
export XDG_RUNTIME_DIR="$RUNTIME" XDG_STATE_HOME="$STATE_HOME"
export PUEUE_AGENT_STATE_DIR="$STATE_DIR"
export PUEUE_CONFIG="$PUEUE_CONFIG" PUEUE_CONFIG_PATH="$PUEUE_CONFIG"
export PYTHONDONTWRITEBYTECODE=1

capture_owned_pueued_identity() {
  "$REAL_PYTHON" - "$PUEUED_PID" "$REAL_PUEUED" "$PUEUE_CONFIG" \
    "$CONTROL/owned-pueued.identity.json" <<'PY'
import json
import os
import sys
from pathlib import Path

pid = int(sys.argv[1])
expected_exe = os.path.realpath(sys.argv[2])
expected_config = os.path.realpath(sys.argv[3])
proc = Path("/proc") / str(pid)
stat_text = (proc / "stat").read_text(encoding="ascii")
fields = stat_text.rsplit(")", 1)[1].split()
argv = [part.decode(errors="replace") for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
exe = os.path.realpath(str(proc / "exe"))
uid_line = next(line for line in (proc / "status").read_text(encoding="ascii").splitlines() if line.startswith("Uid:"))
euid = int(uid_line.split()[2])
starttime = int(fields[19])
if exe != expected_exe or expected_config not in argv or euid != os.geteuid():
    raise SystemExit("pueued PID does not match the exact executable/config/user identity")
identity = {
    "pid": pid,
    "starttime": starttime,
    "euid": euid,
    "exe": exe,
    "config": expected_config,
    "argv": argv,
}
Path(sys.argv[4]).write_text(json.dumps(identity, sort_keys=True) + "\n", encoding="utf-8")
PY
}

verify_owned_pueued_identity() {
  "$REAL_PYTHON" - "$CONTROL/owned-pueued.identity.json" <<'PY'
import json
import os
import sys
from pathlib import Path

identity = json.load(open(sys.argv[1], encoding="utf-8"))
proc = Path("/proc") / str(identity["pid"])
if not proc.exists():
    raise SystemExit("captured pueued identity is absent")
fields = (proc / "stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()
argv = [part.decode(errors="replace") for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
uid_line = next(line for line in (proc / "status").read_text(encoding="ascii").splitlines() if line.startswith("Uid:"))
euid = int(uid_line.split()[2])
exe = os.path.realpath(str(proc / "exe"))
if (
    int(fields[19]) != identity["starttime"]
    or euid != identity["euid"]
    or exe != identity["exe"]
    or argv != identity["argv"]
    or identity["config"] not in argv
):
    raise SystemExit("captured pueued identity changed")
PY
}

stop_owned_pueued() {
  [ -n "$PUEUED_PID" ] || return 0
  [ -f "$CONTROL/owned-pueued.identity.json" ] || {
    echo "private pueued PID has no captured identity; signaling and wait are refused" >&2
    return 1
  }
  set +e
  "$REAL_PYTHON" - "$CONTROL/owned-pueued.identity.json" <<'PY'
import json
import os
import select
import signal
import sys
from pathlib import Path

identity = json.load(open(sys.argv[1], encoding="utf-8"))
pid = identity["pid"]
proc = Path("/proc") / str(pid)

def captured_state():
    if not proc.exists():
        return "gone"
    try:
        fields = (proc / "stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()
        argv = [part.decode(errors="replace") for part in (proc / "cmdline").read_bytes().split(b"\0") if part]
        uid_line = next(line for line in (proc / "status").read_text(encoding="ascii").splitlines() if line.startswith("Uid:"))
        euid = int(uid_line.split()[2])
        exe = os.path.realpath(str(proc / "exe"))
    except (OSError, StopIteration, IndexError):
        return "unknown"
    if int(fields[19]) != identity["starttime"]:
        return "gone"
    if (
        euid != identity["euid"]
        or exe != identity["exe"]
        or argv != identity["argv"]
        or identity["config"] not in argv
    ):
        return "changed"
    return "same"

state = captured_state()
if state == "gone":
    raise SystemExit(0)
if state != "same":
    raise SystemExit(f"owned pueued identity is {state}; preserving fixture without signal or wait")
if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
    raise SystemExit("pidfd signaling is unavailable; preserving the owned fixture")
try:
    pidfd = os.pidfd_open(pid, 0)
except ProcessLookupError:
    raise SystemExit(0)
try:
    state = captured_state()
    if state == "gone":
        raise SystemExit(0)
    if state != "same":
        raise SystemExit(f"owned pueued identity is {state} after pidfd_open; preserving without signal or wait")
    signal.pidfd_send_signal(pidfd, signal.SIGTERM, None, 0)
    poller = select.poll()
    poller.register(pidfd, select.POLLIN)
    if not poller.poll(5000):
        signal.pidfd_send_signal(pidfd, signal.SIGKILL, None, 0)
        if not poller.poll(5000):
            raise SystemExit("owned pueued did not exit after pidfd-bound termination")
finally:
    os.close(pidfd)
PY
  local stop_status=$?
  set -e
  [ "$stop_status" -eq 0 ] || return "$stop_status"
  wait "$PUEUED_PID" 2>/dev/null || true
  set +e
  "$REAL_PYTHON" - "$CONTROL/owned-pueued.identity.json" <<'PY'
import json
import sys
import time
from pathlib import Path

identity = json.load(open(sys.argv[1], encoding="utf-8"))
proc = Path("/proc") / str(identity["pid"])
for _ in range(100):
    if not proc.exists():
        raise SystemExit(0)
    try:
        fields = (proc / "stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()
        starttime = int(fields[19])
    except (OSError, IndexError):
        raise SystemExit(0)
    if starttime != identity["starttime"]:
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit("captured pueued identity still exists after pidfd exit and child reap")
PY
  local absence_status=$?
  set -e
  [ "$absence_status" -eq 0 ] || return "$absence_status"
  PUEUED_PID=""
  return 0
}

retain_on_failure() {
  local status=$?
  trap - EXIT INT TERM
  if ! stop_owned_pueued; then
    if [ -n "$PUEUED_PID" ] && [ ! -f "$CONTROL/owned-pueued.identity.json" ]; then
      echo "Trial E2E private pueued may remain live (uncaptured PID=$PUEUED_PID); no signal or wait attempted" >&2
    else
      echo "Trial E2E cleanup could not verify the captured pueued identity; no unsafe signal or wait attempted" >&2
    fi
    echo "Trial E2E private fixture retained: $WORK" >&2
  fi
  if [ "$KEEP_WORK" -eq 1 ]; then
    echo "Trial E2E fixture retained: $WORK" >&2
    echo "Trial E2E last identifiers: trial=$LAST_TRIAL_ID task=$LAST_TASK_ID group=$LAST_GROUP" >&2
  fi
  exit "$status"
}
trap retain_on_failure EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

record() {
  printf '%s\n' "$*" | tee -a "$EVIDENCE/run.log"
}

safe_remove_work() {
  "$REAL_PYTHON" - "$WORK" "$WORK_IDENTITY" "$(id -u)" <<'PY'
import os
import sys
import stat

path = os.path.abspath(sys.argv[1])
expected_device, expected_inode, expected_uid, expected_mode = sys.argv[2].split(":")
if not path.startswith("/tmp/pa-trial-e2e.") or os.path.islink(path):
    raise SystemExit("unsafe trial fixture cleanup path")
if os.path.realpath(path) != path or path == "/tmp":
    raise SystemExit("trial fixture path identity changed")
metadata = os.lstat(path)
if not stat.S_ISDIR(metadata.st_mode):
    raise SystemExit("trial fixture root is not a directory")
if (
    metadata.st_dev != int(expected_device)
    or metadata.st_ino != int(expected_inode)
    or metadata.st_uid != int(expected_uid)
    or stat.S_IMODE(metadata.st_mode) != int(expected_mode, 8)
):
    raise SystemExit("trial fixture root ownership or identity changed")
PY
  rm -rf -- "$WORK"
}

build_proxy() {
  local source="$WORK/trial-pueue-proxy.rs"
  cat > "$source" <<EOF
use std::{
    env,
    fs::{self, OpenOptions},
    io::{self, Write},
    process::{Command, ExitStatus, Output},
};

const REAL_PUEUE: &str = r#"$REAL_PUEUE"#;
const REAL_PYTHON: &str = r#"$REAL_PYTHON"#;
const PUEUE_CONFIG: &str = r#"$PUEUE_CONFIG"#;
const WORK: &str = r#"$WORK"#;

fn trace(line: &str) {
    let path = format!("{WORK}/control/proxy-trace.log");
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(file, "{line}").unwrap();
}

fn real(args: &[String]) -> Output {
    Command::new(REAL_PUEUE)
        .arg("--config")
        .arg(PUEUE_CONFIG)
        .args(args)
        .output()
        .expect("run the private-profile real Pueue binary")
}

fn exit_code(status: ExitStatus) -> i32 {
    status.code().unwrap_or(1)
}

fn emit(output: Output) -> i32 {
    let _ = io::stdout().write_all(&output.stdout);
    let _ = io::stderr().write_all(&output.stderr);
    exit_code(output.status)
}

fn operation_index(args: &[String]) -> usize {
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--config" || args[index] == "-c" {
            index = index.saturating_add(2);
        } else if args[index].starts_with('-') {
            index = index.saturating_add(1);
        } else {
            break;
        }
    }
    index
}

fn group_argument(args: &[String], start: usize) -> Option<String> {
    let mut index = start;
    while index + 1 < args.len() {
        if args[index] == "--" {
            break;
        }
        if args[index] == "-g" || args[index] == "--group" {
            return Some(args[index + 1].clone());
        }
        index += 1;
    }
    None
}

fn remember_task_add(group: &str, output: &Output) -> Option<String> {
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let task_id = stdout.trim();
    if task_id.parse::<i64>().ok().filter(|id| *id >= 0).is_none() {
        return None;
    }
    trace(&format!("task_add\t{group}\t{task_id}"));
    let path = format!("{WORK}/control/task-ids-by-group.log");
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(file, "{group}\t{task_id}").unwrap();
    Some(task_id.to_owned())
}

fn known_task_id(group: &str) -> Option<String> {
    let path = format!("{WORK}/control/task-ids-by-group.log");
    let contents = fs::read_to_string(path).ok()?;
    contents.lines().filter_map(|line| {
        let (candidate_group, candidate_id) = line.split_once('\t')?;
        (candidate_group == group).then(|| candidate_id.to_owned())
    }).last()
}

fn group_for_task_id(task_id: &str) -> Option<String> {
    let path = format!("{WORK}/control/task-ids-by-group.log");
    let contents = fs::read_to_string(path).ok()?;
    contents.lines().filter_map(|line| {
        let (candidate_group, candidate_id) = line.split_once('\t')?;
        (candidate_id == task_id).then(|| candidate_group.to_owned())
    }).last()
}

fn inject_extra_task_before_remove(task_id: &str) {
    let marker = format!("{WORK}/control/add-extra-task-on-next-trial-remove");
    if !std::path::Path::new(&marker).exists() {
        return;
    }
    let Some(group) = group_for_task_id(task_id) else {
        return;
    };
    if !group.starts_with("pueue-agent-trial-") {
        return;
    }
    if fs::remove_file(&marker).is_err() {
        trace(&format!("extra_task_marker_remove_failed\t{group}\t{task_id}"));
        std::process::exit(76);
    }
    let output = real(&[
        "add".to_owned(),
        "--print-task-id".to_owned(),
        "--group".to_owned(),
        group.clone(),
        "--".to_owned(),
        "/bin/true".to_owned(),
    ]);
    if !output.status.success() {
        trace(&format!("extra_task_add_failed\t{group}\t{task_id}"));
        let _ = io::stderr().write_all(&output.stderr);
        std::process::exit(77);
    }
    let Some(extra_id) = remember_task_add(&group, &output) else {
        trace(&format!("extra_task_id_missing\t{group}\t{task_id}"));
        std::process::exit(78);
    };
    trace(&format!("extra_task_injected\t{group}\t{task_id}\t{extra_id}"));
}

fn safe_group_removal(group: &str) -> bool {
    let status_path = format!("{WORK}/control/remove-status.json");
    let groups_path = format!("{WORK}/control/remove-groups.json");
    let status_output = real(&["status".to_owned(), "--json".to_owned()]);
    if !status_output.status.success() || fs::write(&status_path, &status_output.stdout).is_err() {
        return false;
    }
    let groups_output = real(&["group".to_owned(), "-j".to_owned()]);
    if !groups_output.status.success() || fs::write(&groups_path, &groups_output.stdout).is_err() {
        return false;
    }
    let Some(expected_id) = known_task_id(group) else {
        return false;
    };
    let guard = r#"
import json, sys
group, expected, status_path, groups_path = sys.argv[1:]
status = json.load(open(status_path, encoding="utf-8"))
groups = json.load(open(groups_path, encoding="utf-8"))
if group not in groups or not isinstance(status.get("tasks"), dict):
    raise SystemExit(2)
tasks = status["tasks"]
if expected in tasks or any(
    isinstance(task, dict) and str(task.get("id", key)) == expected
    for key, task in tasks.items()
):
    raise SystemExit(3)
for task in tasks.values():
    if isinstance(task, dict) and task.get("group") == group:
        raise SystemExit(4)
"#;
    let result = Command::new(REAL_PYTHON)
        .arg("-c")
        .arg(guard)
        .arg(group)
        .arg(expected_id.clone())
        .arg(status_path)
        .arg(groups_path)
        .status();
    if !matches!(result, Ok(status) if status.success()) {
        return false;
    }
    trace(&format!("group_remove_safe\t{group}\t{expected_id}"));
    true
}

fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let index = operation_index(&args);
    let operation = args.get(index).map(String::as_str).unwrap_or("");
    let subcommand = args.get(index + 1).map(String::as_str).unwrap_or("");
    let operation_args = args.get(index..).unwrap_or(&[]).to_vec();

    if operation == "add" {
        let group = group_argument(&args, index + 1).unwrap_or_else(|| "default".to_owned());
        let pause_path = format!("{WORK}/control/pause-next-task-add");
        if std::path::Path::new(&pause_path).exists() {
            let _ = fs::remove_file(&pause_path);
            let paused = real(&[
                "pause".to_owned(),
                "--group".to_owned(),
                group.clone(),
            ]);
            if !paused.status.success() {
                trace(&format!("pause_failed\t{group}"));
                std::process::exit(emit(paused));
            }
            trace(&format!("pause_group\t{group}"));
        }
        let output = real(&operation_args);
        let task_id = remember_task_add(&group, &output);
        let lost_path = format!("{WORK}/control/suppress-next-task-add-result");
        if std::path::Path::new(&lost_path).exists() {
            let _ = fs::remove_file(&lost_path);
            if output.status.success() && task_id.is_some() {
                trace(&format!("task_add_result_lost\t{group}\t{}", task_id.unwrap()));
                let _ = io::stderr().write_all(b"trial fixture injected a lost add response\n");
                std::process::exit(17);
            }
        }
        std::process::exit(emit(output));
    }

    if operation == "group" && subcommand == "add" {
        let group = args.get(index + 2).cloned().unwrap_or_default();
        trace(&format!("group_add_attempt\t{group}"));
        let collision_path = format!("{WORK}/control/collide-next-trial-group");
        if group.starts_with("pueue-agent-trial-")
            && std::path::Path::new(&collision_path).exists()
        {
            let _ = fs::remove_file(&collision_path);
            let injected = real(&[
                "group".to_owned(),
                "add".to_owned(),
                group.clone(),
            ]);
            if !injected.status.success() {
                trace(&format!("collision_create_failed\t{group}"));
                std::process::exit(emit(injected));
            }
            trace(&format!("collision_created\t{group}"));
        }
        let output = real(&operation_args);
        if output.status.success() {
            trace(&format!("group_add_done\t{group}"));
        } else {
            trace(&format!("group_add_failed\t{group}"));
        }
        std::process::exit(emit(output));
    }

    if operation == "pause" {
        let group = group_argument(&args, index + 1).unwrap_or_else(|| "all".to_owned());
        trace(&format!("external_pause_command\t{group}"));
    }

    if operation == "remove" {
        let task_id = args.get(index + 1).cloned().unwrap_or_default();
        let group = group_for_task_id(&task_id).unwrap_or_else(|| "unknown".to_owned());
        inject_extra_task_before_remove(&task_id);
        trace(&format!("task_remove\t{task_id}\t{group}"));
    }

    if operation == "kill" {
        let task_id = args.get(index + 1).cloned().unwrap_or_default();
        let group = group_for_task_id(&task_id).unwrap_or_else(|| "unknown".to_owned());
        trace(&format!("task_kill\t{task_id}\t{group}"));
    }

    if operation == "group" && subcommand == "remove" {
        let group = args.get(index + 2).cloned().unwrap_or_default();
        trace(&format!(
            "group_remove_attempt\t{group}\t{}",
            known_task_id(&group).unwrap_or_else(|| "unknown".to_owned())
        ));
        if !safe_group_removal(&group) {
            trace(&format!("group_remove_refused\t{group}"));
            eprintln!("trial fixture refused group removal without a fresh empty snapshot");
            std::process::exit(75);
        }
        let output = real(&operation_args);
        if output.status.success() {
            trace(&format!("group_remove_done\t{group}"));
        }
        std::process::exit(emit(output));
    }

    std::process::exit(emit(real(&operation_args)));
}
EOF
  "$REAL_RUSTC" --edition=2021 -C debuginfo=0 -o "$BIN/pueue" "$source"
  chmod 700 "$BIN/pueue"
}

install_fixture_commands() {
  cp -L "$REAL_PYTHON" "$BIN/python"
  cp -L "$REAL_PYTHON" "$BIN/python3"
  chmod 700 "$BIN/python" "$BIN/python3"
  cat > "$BIN/codex" <<EOF
#!/bin/sh
printf '%s\n' "ordinary agent launch blocked by trial harness" >> "$CONTROL/codex-launch.log"
exit 90
EOF
  cat > "$BIN/systemctl" <<EOF
#!/bin/sh
printf '%s\n' "\$*" >> "$CONTROL/systemctl.log"
case " \$* " in
  *LoadState*) printf '%s\n' loaded ;;
  *" is-active "*) printf '%s\n' active ;;
esac
exit 0
EOF
  chmod 700 "$BIN/codex" "$BIN/systemctl"
}

write_pueue_config() {
  cat > "$PUEUE_CONFIG" <<EOF
shared:
  pueue_directory: "$PUEUE_DIR"
  use_unix_socket: true
  unix_socket_path: "$WORK/pueue.socket"
daemon:
  callback: "'$PA_BIN' event callback --task-id '{{ id }}'"
  shell_command: ["/bin/sh", "-c", "{{ pueue_command_string }}"]
EOF
  chmod 600 "$PUEUE_CONFIG"
  local expected_line="  callback: \"'$PA_BIN' event callback --task-id '{{ id }}'\""
  grep -Fxq "$expected_line" "$PUEUE_CONFIG" || die "canonical implicit callback was not seeded"
  ! grep -Eq "callback:.*--group" "$PUEUE_CONFIG" || die "callback seed forced an explicit group"
}

start_owned_pueued() {
  "$REAL_PUEUED" --config "$PUEUE_CONFIG" > "$WORK/pueued.log" 2>&1 &
  PUEUED_PID=$!
  local attempt=0
  while [ "$attempt" -lt 200 ]; do
    if "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/ready-status.json" 2>/dev/null; then
      break
    fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/ready-status.json" 2>/dev/null \
    || die "private-profile Pueue daemon did not answer status"
  capture_owned_pueued_identity || die "could not capture the exact private-profile daemon identity"
  verify_owned_pueued_identity || die "private-profile daemon identity changed during startup"
  if [ -f "$RUNTIME/pueue.pid" ]; then
    [ "$(sed -n '1p' "$RUNTIME/pueue.pid")" = "$PUEUED_PID" ] \
      || die "Pueue runtime PID marker disagrees with the owned foreground daemon"
    cp "$RUNTIME/pueue.pid" "$CONTROL/owned-pueued.pid"
  fi
  record "Pueue version: $PUEUE_VERSION"
  record "Pueued version: $PUEUED_VERSION"
  record "Owned pueued PID: $PUEUED_PID"
}

write_policy() {
  cat > "$STATE_DIR/execution-policy.toml" <<EOF
version = 1
trusted_path = "$BIN"

[defaults]
network = "enabled"

[campaign]
max_parallel_experiments = 1
max_new_experiments_per_24h = 24
max_agent_runs_per_hour = 30
max_code_change_proposals_per_24h = 10
max_same_spec_retries = 2
max_repairs_per_failure_fingerprint = 2
max_proposals_per_cycle = 1
observer_interval_minutes = 1
research_interval_minutes = 1
max_decision_attempts_per_cycle = 3
max_decision_wait_minutes = 1440

[executables]
codex = "codex"
pueue = "pueue"
EOF
  chmod 600 "$STATE_DIR/execution-policy.toml"
}

prepare_project() {
  (
    umask 022
    "$PA_BIN" init "$PROJECT" > "$EVIDENCE/init.stdout" 2> "$EVIDENCE/init.stderr"
  ) || die "ordinary init under umask 022 failed"
  local service_dir="$PROJECT/.pueue-agent"
  local service_mode
  service_mode="$(stat -c '%a' "$service_dir")"
  [ "$service_mode" = 755 ] || die "ordinary init service container mode was $service_mode, expected 755"
  [ "$(stat -c '%u' "$service_dir")" = "$(id -u)" ] \
    || die "ordinary init service container is not owned by the test user"
  PROJECT_ID="$(sed -n 's/^project_id = "\([^"]*\)"$/\1/p' "$service_dir/config.toml")"
  PROJECT_GROUP="$(sed -n 's/^pueue_group = "\([^"]*\)"$/\1/p' "$service_dir/config.toml")"
  [ -n "$PROJECT_ID" ] && [ -n "$PROJECT_GROUP" ] || die "init did not emit project identity and group"
  cat > "$service_dir/STATE.md" <<'EOF'
# Disposable trial objective

Validate a bounded, one-command trial using a private Pueue profile. Preserve the registered project group and create no campaign state.
EOF
  printf 'stage3 trial fixture v1\n' > "$PROJECT/trial-input.txt"
  chmod 600 "$service_dir/STATE.md" "$PROJECT/trial-input.txt"
  write_policy
  "$PA_BIN" enable --pueue-config "$PUEUE_CONFIG" "$PROJECT" \
    > "$EVIDENCE/enable.stdout" 2> "$EVIDENCE/enable.stderr" \
    || die "enable did not verify the preseeded callback and install the private user-service definition"
  [ -f "$PUEUE_CONFIG" ] || die "enable removed the Pueue config"
  local expected_line="  callback: \"'$PA_BIN' event callback --task-id '{{ id }}'\""
  grep -Fxq "$expected_line" "$PUEUE_CONFIG" || die "enable changed the canonical callback"
  ! grep -Eq "callback:.*--group" "$PUEUE_CONFIG" || die "enable forced an explicit callback group"
  verify_owned_pueued_identity \
    || die "enable restarted, stopped, or replaced the private Pueue daemon identity"
  [ "$(stat -c '%a' "$service_dir")" = 755 ] \
    || die "service container was chmodded instead of accepted under ordinary init mode"
  record "Initialized ordinary project under umask 022; service container remains 0755"
  record "Enabled project $PROJECT_ID with registered group $PROJECT_GROUP; pueued PID unchanged"
}

snapshot_database() {
  local destination="$1"
  "$REAL_PYTHON" - "$STATE_DB" "$destination" <<'PY'
import base64
import json
import sqlite3
import sys
from pathlib import Path

database = Path(sys.argv[1]).resolve(strict=True)
destination = Path(sys.argv[2])
connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
connection.execute("PRAGMA query_only=ON")
connection.execute("BEGIN")
tables = [
    row[0]
    for row in connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
]
required = {
    "campaigns",
    "proposals",
    "experiments",
    "budget_reservations",
    "submissions",
    "agent_runs",
    "research_reviews",
    "events",
    "task_observations",
    "integration_events",
}
missing = sorted(required - set(tables))
if missing:
    raise SystemExit(f"missing protected database tables: {missing}")

def normalize(value):
    if isinstance(value, bytes):
        return {"blob_base64": base64.b64encode(value).decode("ascii")}
    return value

snapshot = {}
for table in tables:
    quoted = '"' + table.replace('"', '""') + '"'
    columns = list(connection.execute(f"PRAGMA table_info({quoted})"))
    rows = [
        [normalize(value) for value in row]
        for row in connection.execute(f"SELECT * FROM {quoted}")
    ]
    rows.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")))
    snapshot[table] = {
        "columns": [list(column) for column in columns],
        "rows": rows,
    }
for table in sorted(required):
    if snapshot[table]["rows"]:
        raise SystemExit(f"protected table unexpectedly contains rows: {table}")
destination.write_text(
    json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
connection.close()
PY
}

capture_registered_task_ids() {
  local status_json="$1"
  local destination="$2"
  "$REAL_PYTHON" - "$status_json" "$PROJECT_GROUP" "$destination" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
group = sys.argv[2]
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit("Pueue status has no tasks map")
ids = []
for key, task in tasks.items():
    if not isinstance(task, dict):
        raise SystemExit("Pueue status task is not an object")
    if task.get("group") == group:
        task_id = task.get("id", key)
        ids.append(str(task_id))
ids.sort(key=int)
open(sys.argv[3], "w", encoding="utf-8").write("\n".join(ids) + ("\n" if ids else ""))
PY
}

capture_registered_group_entry() {
  local label="$1"
  local destination="$2"
  local groups_path="$CONTROL/$label.groups.json"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group -j > "$groups_path" \
    || die "$label: could not capture Pueue group list"
  "$REAL_PYTHON" - "$groups_path" "$PROJECT_GROUP" "$destination" <<'PY'
import json
import sys

groups = json.load(open(sys.argv[1], encoding="utf-8"))
group = sys.argv[2]
if not isinstance(groups, dict) or group not in groups:
    raise SystemExit("registered project group is absent from the Pueue group list")
with open(sys.argv[3], "w", encoding="utf-8") as output:
    output.write(json.dumps(groups[group], sort_keys=True, separators=(",", ":")) + "\n")
PY
}

check_database_unchanged() {
  local label="$1"
  local status_file="$2"
  snapshot_database "$CONTROL/$label.database.json" \
    || die "$label: database snapshot failed"
  cmp -s "$CONTROL/db-baseline.json" "$CONTROL/$label.database.json" \
    || die "$label: a service database row changed"
  capture_registered_task_ids "$status_file" "$CONTROL/$label.registered-task-ids"
  cmp -s "$CONTROL/registered-task-ids" "$CONTROL/$label.registered-task-ids" \
    || die "$label: registered project group task IDs changed"
  capture_registered_group_entry "$label" "$CONTROL/$label.registered-group-entry.json"
  cmp -s "$CONTROL/registered-group-entry.json" "$CONTROL/$label.registered-group-entry.json" \
    || die "$label: registered project group presence or entry changed"
  [ ! -e "$CONTROL/codex-launch.log" ] \
    || die "$label: ordinary agent launch was attempted"
  [ "$(stat -c '%a' "$PROJECT/.pueue-agent")" = 755 ] \
    || die "$label: trial changed the ordinary init service container mode"
}

check_after_case() {
  local label="$1"
  local status_file="$CONTROL/$label.final-status.json"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$status_file" \
    || die "$label: final Pueue status failed"
  check_database_unchanged "$label" "$status_file"
  "$REAL_PYTHON" - "$status_file" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit("Pueue status has no tasks map")
left = [
    str(task.get("id", key)) + ":" + str(task.get("group", ""))
    for key, task in tasks.items()
    if isinstance(task, dict) and str(task.get("group", "")).startswith("pueue-agent-trial-")
]
if left:
    raise SystemExit("trial tasks remain: " + ",".join(left))
PY
}

launch_trial() {
  local label="$1"
  local mode="$2"
  local timeout="$3"
  local case_dir="$CONTROL/case-$label"
  mkdir -m 700 "$case_dir"
  (
    set +e
    cd "$PROJECT" || exit 91
    "$PA_BIN" trial \
      --timeout-seconds "$timeout" \
      --metric-name fixture_loss \
      --metric-direction minimize \
      --metric-min-delta 0.1 \
      --json \
      -- "$REAL_PYTHON" "$SCRIPT_DIR/trial_experiment.py" "$mode" \
      --expected-cwd "$PROJECT" \
      --input "$PROJECT/trial-input.txt" \
      --run-dir "$case_dir" \
      > "$EVIDENCE/$label.stdout" 2> "$EVIDENCE/$label.stderr"
    local status=$?
    printf '%s\n' "$status" > "$EVIDENCE/$label.exit"
    exit 0
  )
}

read_report() {
  local label="$1"
  local report_path="$EVIDENCE/$label.report.json"
  "$REAL_PYTHON" - "$EVIDENCE/$label.stdout" "$report_path" "$EVIDENCE/$label.ids" <<'PY'
import json
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text(encoding="utf-8")
try:
    value = json.loads(source)
except json.JSONDecodeError as error:
    raise SystemExit(f"CLI stdout is not exactly one JSON TrialReport: {error}") from error
expected = {
    "schema_version",
    "trial_id",
    "task_id",
    "group",
    "outcome",
    "terminal",
    "manifest",
    "metric_count",
    "selected_metric_name",
    "selected_metric_value",
    "task_cleanup",
    "group_cleanup",
    "output_cleanup",
}
if not isinstance(value, dict) or set(value) != expected:
    raise SystemExit("JSON TrialReport field set does not match the closed contract")
Path(sys.argv[2]).write_text(
    json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
Path(sys.argv[3]).write_text(
    "\n".join(
        [
            str(value["trial_id"]),
            "-" if value["task_id"] is None else str(value["task_id"]),
            str(value["group"]),
            str(value["outcome"]),
        ]
    )
    + "\n",
    encoding="utf-8",
)
PY
  LAST_TRIAL_ID="$(sed -n '1p' "$EVIDENCE/$label.ids")"
  LAST_TASK_ID="$(sed -n '2p' "$EVIDENCE/$label.ids")"
  LAST_GROUP="$(sed -n '3p' "$EVIDENCE/$label.ids")"
  [ -n "$LAST_TRIAL_ID" ] && [ -n "$LAST_GROUP" ] \
    || die "$label: TrialReport identity fields are incomplete"
}

assert_report() {
  local label="$1"
  local kind="$2"
  read_report "$label"
  "$REAL_PYTHON" - "$EVIDENCE/$label.report.json" "$kind" <<'PY'
import json
import math
import sys
import uuid

report = json.load(open(sys.argv[1], encoding="utf-8"))
kind = sys.argv[2]
try:
    trial_id = str(uuid.UUID(report["trial_id"]))
except (ValueError, TypeError, KeyError):
    raise SystemExit("trial_id is not a UUID")
if report["schema_version"] != 1 or report["trial_id"] != trial_id:
    raise SystemExit("TrialReport schema or canonical trial UUID is invalid")
expected_group = "pueue-agent-trial-" + trial_id.replace("-", "")
if report["group"] != expected_group:
    raise SystemExit("group does not match trial_id.simple()")
task_id = report["task_id"]
if task_id is not None and (not isinstance(task_id, int) or task_id < 0):
    raise SystemExit("task_id is not a non-negative integer or null")
cleanup = (report["task_cleanup"], report["group_cleanup"], report["output_cleanup"])
if kind == "success":
    if report["outcome"] != "succeeded" or report["terminal"] != "succeeded" or report["manifest"] != "valid":
        raise SystemExit("success TrialReport did not classify the task and manifest")
    if report["metric_count"] != 1 or report["selected_metric_name"] != "fixture_loss":
        raise SystemExit("success TrialReport metric projection is invalid")
    if not math.isclose(report["selected_metric_value"], 0.25, rel_tol=0, abs_tol=1e-12):
        raise SystemExit("success TrialReport metric value is invalid")
    if cleanup != ("confirmed", "confirmed", "confirmed") or task_id is None:
        raise SystemExit("success TrialReport cleanup was not fully confirmed")
elif kind == "timeout":
    if report["outcome"] != "timed_out" or report["terminal"] is not None:
        raise SystemExit("timeout TrialReport has the wrong outcome or terminal class")
    if cleanup != ("confirmed", "confirmed", "confirmed") or task_id is None:
        raise SystemExit("timeout TrialReport cleanup was not fully confirmed")
elif kind == "add_uncertain":
    if report["outcome"] != "add_uncertain" or report["terminal"] is not None or report["manifest"] != "not_read":
        raise SystemExit("lost-add TrialReport did not preserve queued add uncertainty")
    if cleanup != ("confirmed", "confirmed", "confirmed") or task_id is None:
        raise SystemExit("lost-add exact recovery did not clean all exact resources")
elif kind == "collision":
    if report["outcome"] != "group_creation_failed" or task_id is not None:
        raise SystemExit("pre-existing nonce group was adopted or misreported")
    if report["group_cleanup"] == "confirmed" or report["output_cleanup"] != "confirmed":
        raise SystemExit("unowned group or output cleanup status is inaccurate")
elif kind == "extra_task":
    if report["outcome"] != "cleanup_uncertain" or report["terminal"] != "succeeded" or report["manifest"] != "valid":
        raise SystemExit("extra-task cleanup uncertainty was not reported")
    if cleanup != ("confirmed", "retained", "retained") or task_id is None:
        raise SystemExit("extra-task report did not retain group and output with task ID")
else:
    raise SystemExit("unknown report assertion")
PY
}

assert_fixture_process_gone() {
  local label="$1"
  local case_dir="$CONTROL/case-$label"
  "$REAL_PYTHON" - "$case_dir/fixture.pid" "$SCRIPT_DIR/trial_experiment.py" <<'PY'
import sys
import time
from pathlib import Path

pid_path = Path(sys.argv[1])
if not pid_path.exists():
    raise SystemExit(0)
pid = int(pid_path.read_text(encoding="ascii").strip())
script = sys.argv[2].encode()
for _ in range(100):
    proc = Path("/proc") / str(pid)
    if not proc.exists():
        raise SystemExit(0)
    try:
        stat_text = (proc / "stat").read_text(encoding="ascii")
        state = stat_text.rsplit(")", 1)[1].split()[0]
        command = (proc / "cmdline").read_bytes()
    except (OSError, IndexError):
        time.sleep(0.05)
        continue
    if script not in command or state in {"Z", "X"}:
        raise SystemExit(0)
    time.sleep(0.05)
raise SystemExit("owned trial fixture process is still live after cleanup")
PY
}

assert_no_output_generation() {
  local trial_id="$1"
  "$REAL_PYTHON" - "$PROJECT" "$trial_id" <<'PY'
import sys
import uuid
from pathlib import Path

project = Path(sys.argv[1]).resolve(strict=True)
trial_id = str(uuid.UUID(sys.argv[2]))
generation = project / ".pueue-agent" / "trials" / trial_id
if generation.exists() or generation.is_symlink():
    raise SystemExit("trial generation survived confirmed output cleanup")
trials = generation.parent
if trials.exists():
    if trials.is_symlink() or not trials.is_dir():
        raise SystemExit("trials parent is not a plain directory")
    if any(trials.iterdir()):
        raise SystemExit("another trial generation remains")
PY
}

assert_success_fixture_evidence() {
  local label="$1"
  local case_dir="$CONTROL/case-$label"
  "$REAL_PYTHON" - "$EVIDENCE/$label.report.json" "$case_dir/fixture-evidence.json" "$PROJECT" <<'PY'
import hashlib
import json
import math
import sys
import uuid
from pathlib import Path

report = json.load(open(sys.argv[1], encoding="utf-8"))
fixture = json.load(open(sys.argv[2], encoding="utf-8"))
project = Path(sys.argv[3]).resolve(strict=True)
trial_id = str(uuid.UUID(report["trial_id"]))
for key in ("campaign_id", "experiment_id"):
    if str(uuid.UUID(fixture[key])) != fixture[key]:
        raise SystemExit(f"{key} was not a canonical UUID")
if fixture["generation_id"] != trial_id:
    raise SystemExit("fixture output generation did not match TrialReport trial_id")
generation = project / ".pueue-agent" / "trials" / trial_id
if fixture["result_path"] != str(generation / "result.json"):
    raise SystemExit("fixture result path was not the private trial path")
if fixture["artifact_dir"] != str(generation / "artifacts"):
    raise SystemExit("fixture artifact path was not the private trial path")
if [fixture[key] for key in ("service_mode", "trials_mode", "generation_mode", "artifact_mode", "result_mode")] != [0o755, 0o700, 0o700, 0o700, 0o600]:
    raise SystemExit("ordinary-init/service and private-output mode contract changed")
encoded = json.dumps(
    {
        "schema_version": 1,
        "experiment_id": fixture["experiment_id"],
        "metrics": {"fixture_loss": 0.25},
    },
    allow_nan=False,
    separators=(",", ":"),
).encode("utf-8")
if fixture["result_sha256"] != hashlib.sha256(encoded).hexdigest():
    raise SystemExit("fixture result hash does not match the exact bounded manifest")
if fixture["artifact_sha256"] != hashlib.sha256(b"bounded trial artifact\n").hexdigest():
    raise SystemExit("fixture artifact hash does not match the exact artifact bytes")
if not math.isclose(report["selected_metric_value"], 0.25, rel_tol=0, abs_tol=1e-12):
    raise SystemExit("report metric and fixture manifest disagree")
PY
}

wait_for_trial_state() {
  local label="$1"
  local desired="$2"
  local status_path="$CONTROL/$label.observed-status.json"
  local observed_path="$CONTROL/$label.observed-task"
  local attempt=0
  while [ "$attempt" -lt 240 ]; do
    "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$status_path" 2>/dev/null || true
    if [ -s "$status_path" ]; then
      if "$REAL_PYTHON" - "$status_path" "$desired" "$observed_path" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
desired = sys.argv[2].lower()
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit(1)
matches = []
for key, task in tasks.items():
    if not isinstance(task, dict):
        continue
    group = task.get("group", "")
    if not isinstance(group, str) or not group.startswith("pueue-agent-trial-"):
        continue
    details = task.get("status")
    if not isinstance(details, dict) or len(details) != 1:
        continue
    state = next(iter(details)).lower()
    task_id = task.get("id", key)
    matches.append((str(task_id), group, state))
if len(matches) != 1 or matches[0][2] != desired:
    raise SystemExit(1)
open(sys.argv[3], "w", encoding="utf-8").write("\n".join(matches[0]) + "\n")
PY
      then
        return 0
      fi
    fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  return 1
}

assert_group_absent() {
  local group="$1"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group -j > "$CONTROL/groups.json" \
    || die "could not read Pueue group snapshot"
  "$REAL_PYTHON" - "$CONTROL/groups.json" "$group" <<'PY'
import json
import sys
groups = json.load(open(sys.argv[1], encoding="utf-8"))
if sys.argv[2] in groups:
    raise SystemExit("trial nonce group remains after confirmed cleanup")
PY
}

assert_exact_task_absent() {
  local task_id="$1"
  local destination="$2"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$destination" \
    || die "could not read fresh global status for exact task absence"
  "$REAL_PYTHON" - "$destination" "$task_id" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit("Pueue status has no tasks map")
if sys.argv[2] in tasks or any(
    isinstance(task, dict) and str(task.get("id", key)) == sys.argv[2]
    for key, task in tasks.items()
):
    raise SystemExit("exact task still exists in fresh global status")
PY
}

assert_no_nonce_groups() {
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group -j > "$CONTROL/final-groups.json" \
    || die "could not read final Pueue groups"
  "$REAL_PYTHON" - "$CONTROL/final-groups.json" <<'PY'
import json
import sys
groups = json.load(open(sys.argv[1], encoding="utf-8"))
if not isinstance(groups, dict):
    raise SystemExit("Pueue group list is not an object")
remaining = [name for name in groups if isinstance(name, str) and name.startswith("pueue-agent-trial-")]
if remaining:
    raise SystemExit("nonce groups remain: " + ",".join(sorted(remaining)))
PY
}

assert_group_exists_empty() {
  local group="$1"
  local expected_id="-"
  if [ "$#" -gt 1 ]; then expected_id="$2"; fi
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/group-check-status.json" \
    || die "could not read global Pueue status before group cleanup"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group -j > "$CONTROL/group-check-groups.json" \
    || die "could not read Pueue groups before group cleanup"
  "$REAL_PYTHON" - "$CONTROL/group-check-status.json" "$CONTROL/group-check-groups.json" "$group" "$expected_id" <<'PY'
import json
import sys

status = json.load(open(sys.argv[1], encoding="utf-8"))
groups = json.load(open(sys.argv[2], encoding="utf-8"))
group, expected_id = sys.argv[3:]
if group not in groups:
    raise SystemExit("fixture-owned nonce group disappeared before cleanup")
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit("global Pueue status has no tasks map")
if expected_id != "-" and (
    expected_id in tasks
    or any(
        isinstance(task, dict) and str(task.get("id", key)) == expected_id
        for key, task in tasks.items()
    )
):
    raise SystemExit("exact trial task remains globally before group cleanup")
remaining = [
    str(task.get("id", key))
    for key, task in tasks.items()
    if isinstance(task, dict) and task.get("group") == group
]
if remaining:
    raise SystemExit("nonce group is not empty: " + ",".join(remaining))
PY
}

remove_empty_fixture_group() {
  local group="$1"
  local expected_id="-"
  if [ "$#" -gt 1 ]; then expected_id="$2"; fi
  assert_group_exists_empty "$group" "$expected_id"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group remove "$group" \
    > "$CONTROL/manual-group-remove.stdout" 2> "$CONTROL/manual-group-remove.stderr" \
    || die "could not remove the exact empty fixture nonce group"
  assert_group_absent "$group"
}

assert_proxy_cleanup_order() {
  local label="$1"
  local report="$EVIDENCE/$label.report.json"
  "$REAL_PYTHON" - "$CONTROL/proxy-trace.log" "$report" "$label" <<'PY'
import json
import sys

events = [line.rstrip("\n").split("\t") for line in open(sys.argv[1], encoding="utf-8")]
report = json.load(open(sys.argv[2], encoding="utf-8"))
label = sys.argv[3]
group = report["group"]
task_id = str(report["task_id"])

def indices(name, predicate):
    return [index for index, event in enumerate(events) if len(event) >= 2 and event[0] == name and predicate(event)]

adds = indices("task_add", lambda event: event[1] == group and len(event) >= 3 and event[2] == task_id)
removes = indices("task_remove", lambda event: len(event) == 3 and event[1] == task_id and event[2] == group)
safe = indices("group_remove_safe", lambda event: len(event) >= 3 and event[1] == group and event[2] == task_id)
done = indices("group_remove_done", lambda event: event[1] == group)
if len(adds) != 1 or len(removes) != 1 or len(safe) != 1 or len(done) != 1:
    raise SystemExit("proxy did not record one exact add/remove/group-cleanup proof")
if not adds[0] < removes[0] < safe[0] < done[0]:
    raise SystemExit("group removal was not after exact task removal and fresh empty status")
pauses = indices("pause_group", lambda event: event[1] == group)
if label in {"queued-timeout", "lost-add"}:
    if len(pauses) != 1 or pauses[0] > adds[0]:
        raise SystemExit("queued timeout or lost-add case did not pause only its exact nonce group")
elif pauses:
    raise SystemExit("unexpected fixture group pause outside queued timeout or lost-add")
external_pauses = indices("external_pause_command", lambda event: True)
if external_pauses:
    raise SystemExit("product issued a Pueue pause command outside the scoped fixture pause")
if label == "running-timeout":
    kills = indices("task_kill", lambda event: len(event) == 3 and event[1] == task_id and event[2] == group)
    if not kills or not kills[0] < removes[0]:
        raise SystemExit("running timeout did not kill the exact task before removal")
if label == "lost-add":
    lost = indices("task_add_result_lost", lambda event: len(event) >= 3 and event[1] == group and event[2] == task_id)
    group_adds = indices("group_add_done", lambda event: event[1] == group)
    pauses = indices("pause_group", lambda event: event[1] == group)
    if len(lost) != 1 or len(group_adds) != 1 or not group_adds[0] < lost[0]:
        raise SystemExit("lost-add injection did not forward group add before intercepting task add")
    if len(pauses) != 1 or pauses[0] > adds[0]:
        raise SystemExit("lost-add recovery task was not held queued in only its exact nonce group")
PY
}

assert_collision_proxy_behavior() {
  local group="$1"
  "$REAL_PYTHON" - "$CONTROL/proxy-trace.log" "$group" <<'PY'
import sys
events = [line.rstrip("\n").split("\t") for line in open(sys.argv[1], encoding="utf-8")]
group = sys.argv[2]
collision = [event for event in events if event[0] == "collision_created" and event[1] == group]
failed = [event for event in events if event[0] == "group_add_failed" and event[1] == group]
task_adds = [event for event in events if event[0] == "task_add" and event[1] == group]
group_removes = [event for event in events if event[0] == "group_remove_attempt" and event[1] == group]
if len(collision) != 1 or len(failed) != 1 or task_adds or group_removes:
    raise SystemExit("pre-existing nonce group was adopted, task-added, or removed")
PY
}

assert_no_product_group_remove() {
  local group="$1"
  "$REAL_PYTHON" - "$CONTROL/proxy-trace.log" "$group" <<'PY'
import sys
events = [line.rstrip("\n").split("\t") for line in open(sys.argv[1], encoding="utf-8")]
group = sys.argv[2]
if any(event[0] == "group_remove_attempt" and len(event) >= 2 and event[1] == group for event in events):
    raise SystemExit("product invoked group remove while an extra task was present")
PY
}

capture_extra_task_id() {
  local group="$1"
  local trial_task_id="$2"
  local destination="$3"
  "$REAL_PYTHON" - "$CONTROL/proxy-trace.log" "$group" "$trial_task_id" "$destination" <<'PY'
import sys

events = [line.rstrip("\n").split("\t") for line in open(sys.argv[1], encoding="utf-8")]
group, trial_id, destination = sys.argv[2:]
matches = [
    event
    for event in events
    if event[0] == "extra_task_injected"
    and len(event) == 4
    and event[1] == group
    and event[2] == trial_id
]
if len(matches) != 1 or matches[0][3] == trial_id:
    raise SystemExit("extra task identity was not recorded exactly once")
injected_index = events.index(matches[0])
removed = [
    index
    for index, event in enumerate(events)
    if event[0] == "task_remove" and len(event) == 3 and event[1] == trial_id and event[2] == group
]
if len(removed) != 1 or injected_index >= removed[0]:
    raise SystemExit("extra task was not injected before removal of the exact trial task")
with open(destination, "w", encoding="ascii") as output:
    output.write(matches[0][3] + "\n")
PY
}

wait_for_task_id_terminal() {
  local task_id="$1"
  local group="$2"
  local status_path="$CONTROL/extra-task-status.json"
  local attempt=0
  while [ "$attempt" -lt 200 ]; do
    "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$status_path" 2>/dev/null || true
    if [ -s "$status_path" ] && "$REAL_PYTHON" - "$status_path" "$task_id" "$group" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
for key, task in status.get("tasks", {}).items():
    if not isinstance(task, dict):
        continue
    if str(task.get("id", key)) != sys.argv[2] or task.get("group") != sys.argv[3]:
        continue
    details = task.get("status")
    if isinstance(details, dict) and len(details) == 1:
        if next(iter(details)).lower() in {"done", "failed", "killed", "finished", "success"}:
            raise SystemExit(0)
raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 0.05
    attempt=$((attempt + 1))
  done
  return 1
}

assert_extra_group_remove_rejected() {
  local group="$1"
  local extra_id="$2"
  set +e
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group remove "$group" \
    > "$CONTROL/nonempty-group-remove.stdout" 2> "$CONTROL/nonempty-group-remove.stderr"
  local status=$?
  set -e
  [ "$status" -ne 0 ] || die "Pueue 4.0.4 unexpectedly removed a nonempty group"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/nonempty-after-reject.json" \
    || die "status failed after Pueue rejected a nonempty group removal"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" group -j > "$CONTROL/nonempty-groups-after-reject.json" \
    || die "group list failed after Pueue rejected a nonempty group removal"
  "$REAL_PYTHON" - "$CONTROL/nonempty-after-reject.json" "$CONTROL/nonempty-groups-after-reject.json" "$group" "$extra_id" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
groups = json.load(open(sys.argv[2], encoding="utf-8"))
group, task_id = sys.argv[3:]
if group not in groups:
    raise SystemExit("nonempty group remove unexpectedly deleted the group")
tasks = status.get("tasks")
if not isinstance(tasks, dict) or task_id not in tasks:
    raise SystemExit("nonempty group remove lost the extra task")
task = tasks[task_id]
if not isinstance(task, dict) or task.get("group") != group:
    raise SystemExit("Pueue 4.0.4 moved the extra task out of the nonempty group")
PY
}

remove_retained_output() {
  local trial_id="$1"
  "$REAL_PYTHON" - "$PROJECT" "$trial_id" <<'PY'
import shutil
import sys
import uuid
from pathlib import Path

project = Path(sys.argv[1]).resolve(strict=True)
trial_id = str(uuid.UUID(sys.argv[2]))
service = project / ".pueue-agent"
trials = service / "trials"
generation = trials / trial_id
if service.is_symlink() or trials.is_symlink() or generation.is_symlink():
    raise SystemExit("retained output contains a symlink; preserving the fixture")
if generation.resolve(strict=True).parent != trials.resolve(strict=True):
    raise SystemExit("retained output escaped the exact trial generation")
children = list(trials.iterdir())
if children != [generation]:
    raise SystemExit("retained output parent contains unexpected generations")
shutil.rmtree(generation)
if not any(trials.iterdir()):
    trials.rmdir()
PY
}

assert_retained_output_identity() {
  local label="$1"
  "$REAL_PYTHON" - "$EVIDENCE/$label.report.json" "$CONTROL/case-$label/fixture-evidence.json" "$PROJECT" <<'PY'
import hashlib
import json
import os
import stat
import sys
import uuid
from pathlib import Path

report = json.load(open(sys.argv[1], encoding="utf-8"))
fixture = json.load(open(sys.argv[2], encoding="utf-8"))
project = Path(sys.argv[3]).resolve(strict=True)
trial_id = str(uuid.UUID(report["trial_id"]))
if report["task_id"] is None or fixture["generation_id"] != trial_id:
    raise SystemExit("retained output identity does not match the exact reported trial")
generation = project / ".pueue-agent" / "trials" / trial_id
trials = generation.parent
artifacts = generation / "artifacts"
result = generation / "result.json"
artifact = artifacts / "fixture.txt"
for path in (trials, generation, artifacts, result, artifact):
    if path.is_symlink():
        raise SystemExit("retained output contains a symlink")
if sorted(path.name for path in trials.iterdir()) != [trial_id]:
    raise SystemExit("retained output parent contains an unrelated generation")
if sorted(path.name for path in generation.iterdir()) != ["artifacts", "result.json"]:
    raise SystemExit("retained generation has unexpected entries")
if sorted(path.name for path in artifacts.iterdir()) != ["fixture.txt"]:
    raise SystemExit("retained artifact directory has unexpected entries")
for path, mode in ((trials, 0o700), (generation, 0o700), (artifacts, 0o700), (result, 0o600), (artifact, 0o600)):
    metadata = path.lstat()
    expected_type = stat.S_ISDIR(metadata.st_mode) if path.is_dir() else stat.S_ISREG(metadata.st_mode)
    if not expected_type or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != mode:
        raise SystemExit(f"retained output mode/owner/type mismatch: {path.name}")
if result.stat().st_nlink != 1:
    raise SystemExit("retained result is hardlinked")
payload = result.read_bytes()
manifest = json.loads(payload)
if manifest != {
    "schema_version": 1,
    "experiment_id": fixture["experiment_id"],
    "metrics": {"fixture_loss": 0.25},
}:
    raise SystemExit("retained result manifest is not the exact fixture payload")
if hashlib.sha256(payload).hexdigest() != fixture["result_sha256"]:
    raise SystemExit("retained result content digest changed")
artifact_bytes = artifact.read_bytes()
if artifact_bytes != b"bounded trial artifact\n" or hashlib.sha256(artifact_bytes).hexdigest() != fixture["artifact_sha256"]:
    raise SystemExit("retained artifact content or digest changed")
PY
}

wait_for_fixture_started() {
  local label="$1"
  local case_dir="$CONTROL/case-$label"
  local attempt=0
  while [ "$attempt" -lt 100 ]; do
    [ -f "$case_dir/started" ] && return 0
    sleep 0.05
    attempt=$((attempt + 1))
  done
  return 1
}

report_task_state() {
  local label="$1"
  local observed="$CONTROL/$label.observed-task"
  [ -s "$observed" ] || die "$label: trial task state was not observed"
  LAST_TASK_ID="$(sed -n '1p' "$observed")"
  LAST_GROUP="$(sed -n '2p' "$observed")"
}

start_and_wait_for_cli() {
  local label="$1"
  local pid="$2"
  set +e
  wait "$pid"
  local wait_status=$?
  set -e
  [ "$wait_status" -eq 0 ] || die "$label: CLI harness child did not finish its report capture"
  [ -f "$EVIDENCE/$label.exit" ] || die "$label: CLI exit status was not captured"
}

assert_exit_status() {
  local label="$1"
  local expected="$2"
  local actual
  actual="$(sed -n '1p' "$EVIDENCE/$label.exit")"
  [ "$actual" = "$expected" ] || die "$label: expected CLI exit $expected, found $actual"
}

run_success_case() {
  local label="$1"
  launch_trial "$label" success 30 &
  local cli_pid=$!
  start_and_wait_for_cli "$label" "$cli_pid"
  assert_exit_status "$label" 0
  assert_report "$label" success
  assert_success_fixture_evidence "$label"
  assert_fixture_process_gone "$label"
  assert_no_output_generation "$LAST_TRIAL_ID"
  check_after_case "$label"
  assert_proxy_cleanup_order "$label"
  record "Trial case $label PASS (trial=$LAST_TRIAL_ID task=$LAST_TASK_ID group=$LAST_GROUP)"
}

assert_captured_pueued_absent() {
  "$REAL_PYTHON" - "$CONTROL/owned-pueued.identity.json" <<'PY'
import json
import sys
from pathlib import Path

identity = json.load(open(sys.argv[1], encoding="utf-8"))
proc = Path("/proc") / str(identity["pid"])
if proc.exists():
    try:
        fields = (proc / "stat").read_text(encoding="ascii").rsplit(")", 1)[1].split()
        starttime = int(fields[19])
    except (OSError, IndexError):
        starttime = None
    if starttime == identity["starttime"]:
        raise SystemExit("captured owned pueued identity remains after shutdown")
PY
  [ ! -e "$WORK/pueue.socket" ] || die "owned Pueue socket remains after daemon exit"
  [ ! -e "$PUEUE_DIR/pueue.pid" ] || die "owned Pueue profile PID marker remains after daemon exit"
  [ ! -e "$RUNTIME/pueue.pid" ] || die "owned runtime PID marker remains after daemon exit"
}

assert_no_nonce_tasks() {
  local status_path="$CONTROL/final-status.json"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$status_path" \
    || die "could not read final global Pueue status"
  "$REAL_PYTHON" - "$status_path" <<'PY'
import json
import sys
status = json.load(open(sys.argv[1], encoding="utf-8"))
tasks = status.get("tasks")
if not isinstance(tasks, dict):
    raise SystemExit("final Pueue status has no tasks map")
remaining = [
    str(task.get("id", key)) + ":" + str(task.get("group", ""))
    for key, task in tasks.items()
    if isinstance(task, dict) and str(task.get("group", "")).startswith("pueue-agent-trial-")
]
if remaining:
    raise SystemExit("trial tasks remain before daemon cleanup: " + ",".join(remaining))
PY
  assert_no_nonce_groups
}

build_proxy
install_fixture_commands
export PATH="$BIN:$ORIGINAL_PATH"
write_pueue_config
start_owned_pueued
prepare_project

"$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/initial-status.json" \
  || die "could not capture baseline Pueue status"
snapshot_database "$CONTROL/db-baseline.json" \
  || die "could not capture the protected service database baseline"
capture_registered_task_ids "$CONTROL/initial-status.json" "$CONTROL/registered-task-ids"
capture_registered_group_entry initial "$CONTROL/registered-group-entry.json"
record "Protected service tables are empty; baseline registered-group task IDs captured"

run_success_case success-one
SUCCESS_ONE_TRIAL="$LAST_TRIAL_ID"
SUCCESS_ONE_TASK_ID="$LAST_TASK_ID"
SUCCESS_ONE_GROUP="$LAST_GROUP"
SUCCESS_ONE_EXPERIMENT="$(sed -n 's/.*"experiment_id":"\([^"]*\)".*/\1/p' "$CONTROL/case-success-one/fixture-evidence.json")"
SUCCESS_ONE_CAMPAIGN="$(sed -n 's/.*"campaign_id":"\([^"]*\)".*/\1/p' "$CONTROL/case-success-one/fixture-evidence.json")"
run_success_case success-two
SUCCESS_TWO_TRIAL="$LAST_TRIAL_ID"
SUCCESS_TWO_TASK_ID="$LAST_TASK_ID"
SUCCESS_TWO_GROUP="$LAST_GROUP"
SUCCESS_TWO_EXPERIMENT="$(sed -n 's/.*"experiment_id":"\([^"]*\)".*/\1/p' "$CONTROL/case-success-two/fixture-evidence.json")"
SUCCESS_TWO_CAMPAIGN="$(sed -n 's/.*"campaign_id":"\([^"]*\)".*/\1/p' "$CONTROL/case-success-two/fixture-evidence.json")"
"$REAL_PYTHON" - "$SUCCESS_ONE_TRIAL" "$SUCCESS_ONE_TASK_ID" "$SUCCESS_ONE_GROUP" "$SUCCESS_ONE_EXPERIMENT" "$SUCCESS_ONE_CAMPAIGN" \
  "$SUCCESS_TWO_TRIAL" "$SUCCESS_TWO_TASK_ID" "$SUCCESS_TWO_GROUP" "$SUCCESS_TWO_EXPERIMENT" "$SUCCESS_TWO_CAMPAIGN" <<'PY'
import sys
first = sys.argv[1:6]
second = sys.argv[6:11]
if any(not value for value in first + second):
    raise SystemExit("successful trial did not preserve all unique identities")
try:
    first_task = int(first[1])
    second_task = int(second[1])
except ValueError:
    raise SystemExit("successful trial task IDs are not integers")
if first_task < 0 or second_task < 0:
    raise SystemExit("successful trial task IDs are negative")
if first[0] == second[0] or first[2] == second[2] or first[3] == second[3] or first[4] == second[4]:
    raise SystemExit("two successful trials reused trial/group/experiment/campaign identity")
if "pueue-agent-trial-" + first[0].replace("-", "") != first[2]:
    raise SystemExit("first successful trial group does not bind to its simple UUID")
if "pueue-agent-trial-" + second[0].replace("-", "") != second[2]:
    raise SystemExit("second successful trial group does not bind to its simple UUID")
PY

: > "$CONTROL/pause-next-task-add"
launch_trial queued-timeout queued-timeout 1 &
QUEUED_CLI_PID=$!
wait_for_trial_state queued-timeout queued \
  || die "queued-timeout: task never appeared queued in its nonce group"
[ ! -e "$CONTROL/case-queued-timeout/started" ] \
  || die "queued-timeout: paused fixture unexpectedly started"
start_and_wait_for_cli queued-timeout "$QUEUED_CLI_PID"
assert_exit_status queued-timeout 1
assert_report queued-timeout timeout
assert_fixture_process_gone queued-timeout
assert_no_output_generation "$LAST_TRIAL_ID"
check_after_case queued-timeout
assert_proxy_cleanup_order queued-timeout
record "Trial case queued-timeout PASS (trial=$LAST_TRIAL_ID task=$LAST_TASK_ID group=$LAST_GROUP)"

launch_trial running-timeout running-timeout 1 &
RUNNING_CLI_PID=$!
wait_for_fixture_started running-timeout \
  || die "running-timeout: fixture did not start before its deadline"
wait_for_trial_state running-timeout running \
  || die "running-timeout: task never appeared running in its nonce group"
start_and_wait_for_cli running-timeout "$RUNNING_CLI_PID"
assert_exit_status running-timeout 1
assert_report running-timeout timeout
assert_fixture_process_gone running-timeout
assert_no_output_generation "$LAST_TRIAL_ID"
check_after_case running-timeout
assert_proxy_cleanup_order running-timeout
record "Trial case running-timeout PASS (trial=$LAST_TRIAL_ID task=$LAST_TASK_ID group=$LAST_GROUP)"

: > "$CONTROL/suppress-next-task-add-result"
: > "$CONTROL/pause-next-task-add"
launch_trial lost-add queued-timeout 30 &
LOST_ADD_CLI_PID=$!
start_and_wait_for_cli lost-add "$LOST_ADD_CLI_PID"
assert_exit_status lost-add 1
assert_report lost-add add_uncertain
[ ! -e "$CONTROL/case-lost-add/started" ] \
  || die "lost-add: paused exact-recovery task unexpectedly started its fixture"
assert_fixture_process_gone lost-add
assert_no_output_generation "$LAST_TRIAL_ID"
check_after_case lost-add
assert_proxy_cleanup_order lost-add
record "Trial case lost-add PASS (trial=$LAST_TRIAL_ID task=$LAST_TASK_ID group=$LAST_GROUP)"

: > "$CONTROL/collide-next-trial-group"
launch_trial collision success 30 &
COLLISION_CLI_PID=$!
start_and_wait_for_cli collision "$COLLISION_CLI_PID"
assert_exit_status collision 1
assert_report collision collision
assert_collision_proxy_behavior "$LAST_GROUP"
assert_no_output_generation "$LAST_TRIAL_ID"
check_after_case collision
remove_empty_fixture_group "$LAST_GROUP"
check_after_case collision-group-operator-cleanup
record "Trial case nonce-group-collision PASS (trial=$LAST_TRIAL_ID group=$LAST_GROUP)"

: > "$CONTROL/add-extra-task-on-next-trial-remove"
launch_trial extra-task extra-task 30 &
EXTRA_CLI_PID=$!
wait_for_fixture_started extra-task \
  || die "extra-task: fixture did not start before release"
wait_for_trial_state extra-task running \
  || die "extra-task: trial task never appeared running"
: > "$CONTROL/case-extra-task/release"
start_and_wait_for_cli extra-task "$EXTRA_CLI_PID"
assert_exit_status extra-task 1
assert_report extra-task extra_task
assert_success_fixture_evidence extra-task
assert_fixture_process_gone extra-task
assert_no_product_group_remove "$LAST_GROUP"
capture_extra_task_id "$LAST_GROUP" "$LAST_TASK_ID" "$CONTROL/extra-task.id"
EXTRA_TASK_ID="$(sed -n '1p' "$CONTROL/extra-task.id")"
[ -n "$EXTRA_TASK_ID" ] || die "extra-task: injected task ID was empty"
assert_retained_output_identity extra-task
wait_for_task_id_terminal "$EXTRA_TASK_ID" "$LAST_GROUP" \
  || die "extra-task: injected Pueue task did not reach a terminal state"
check_database_unchanged extra-task-before-operator-cleanup \
  "$CONTROL/extra-task-status.json"
assert_exact_task_absent "$LAST_TASK_ID" "$CONTROL/extra-trial-task-absent.json"
assert_extra_group_remove_rejected "$LAST_GROUP" "$EXTRA_TASK_ID"
"$REAL_PUEUE" --config "$PUEUE_CONFIG" remove "$EXTRA_TASK_ID" \
  > "$CONTROL/extra-task-remove.stdout" 2> "$CONTROL/extra-task-remove.stderr" \
  || die "could not remove the exact disposable extra task"
remove_empty_fixture_group "$LAST_GROUP" "$LAST_TASK_ID"
remove_retained_output "$LAST_TRIAL_ID"
assert_exact_task_absent "$LAST_TASK_ID" "$CONTROL/extra-trial-task-absent-after-operator-cleanup.json"
assert_no_output_generation "$LAST_TRIAL_ID"
check_after_case extra-task-operator-cleanup
record "Trial case extra-task-retained PASS (trial=$LAST_TRIAL_ID task=$LAST_TASK_ID extra_task=$EXTRA_TASK_ID group=$LAST_GROUP)"

assert_no_nonce_tasks
"$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json > "$CONTROL/final-status-for-database.json" \
  || die "could not capture final status for protected database comparison"
check_database_unchanged final "$CONTROL/final-status-for-database.json"
verify_owned_pueued_identity || die "owned daemon identity changed before shutdown"
record "All seven real-Pueue trial runs and exact cleanup checks passed"

stop_owned_pueued || die "could not pidfd-stop and reap the exact private Pueue daemon"
assert_captured_pueued_absent
[ ! -e "$CONTROL/codex-launch.log" ] || die "ordinary agent launch was attempted"
safe_remove_work
KEEP_WORK=0
trap - EXIT INT TERM
echo "Trial E2E PASS"
