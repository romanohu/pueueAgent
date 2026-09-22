#!/usr/bin/env bash
set -euo pipefail

# This is deliberately a separate profile from rust_supervisor.sh.  It is the
# Linux-gated acceptance harness for the native campaign-research path; every
# case owns its HOME, policy, SQLite database, Pueue profile, project, daemon,
# and evidence directory.
if [ "$(uname -s)" != "Linux" ]; then
  echo "Research E2E FAIL: real-Pueue research acceptance requires Linux" >&2
  exit 1
fi
umask 077

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
: "${HOME:?HOME is required}"
BUILD_HOME="$HOME"
BUILD_CARGO_HOME="${CARGO_HOME:-$BUILD_HOME/.cargo}"
BUILD_RUSTUP_HOME="${RUSTUP_HOME:-$BUILD_HOME/.rustup}"
ORIGINAL_PATH="${PATH:?PATH is required}"
REAL_PUEUE="$(command -v pueue || true)"
REAL_PUEUED="$(command -v pueued || true)"
REAL_GIT="$(command -v git || true)"
REAL_PYTHON="$(command -v python3 || command -v python || true)"
REAL_SQLITE="$(command -v sqlite3 || true)"
REAL_JQ="$(command -v jq || true)"
REAL_RUSTC="$(command -v rustc || true)"
REAL_BASH="$(command -v bash || true)"
CARGO_BIN="$(command -v cargo || true)"

die() {
  echo "Research E2E FAIL: $*" >&2
  exit 1
}

case_name=""
if [ "$#" -gt 0 ]; then
  case "$1" in
    --case)
      [ "$#" -eq 2 ] || die "--case requires exactly one case name"
      case_name="$2"
      ;;
    --help|-h)
      printf '%s\n' 'usage: bash tests/e2e/research_supervisor.sh [--case CASE]' \
        'default CASE set: continue stop_and_next checkpoint missing_session' \
        '  restart_review_running restart_answer_ready restart_stop_pending' \
        '  restart_stop_confirmed restart_successor_submitting add_reconcile' \
        '  failure_malformed failure_timeout failure_cap failure_unsafe_session failure_unknown_kill'
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
fi

required_cases=(
  continue
  stop_and_next
  checkpoint
  missing_session
  restart_review_running
  restart_answer_ready
  restart_stop_pending
  restart_stop_confirmed
  restart_successor_submitting
  add_reconcile
  failure_malformed
  failure_timeout
  failure_cap
  failure_unsafe_session
  failure_unknown_kill
)

if [ -z "$case_name" ] && [ -n "${PUEUE_AGENT_TASK8_CASE:-}" ]; then
  case_name="$PUEUE_AGENT_TASK8_CASE"
fi

if [ -z "$case_name" ]; then
  [ -n "$REAL_BASH" ] || die "bash is required for the default case fan-out"
  for selected_case in "${required_cases[@]}"; do
    if ! "$REAL_BASH" "$0" --case "$selected_case"; then
      die "case failed: $selected_case"
    fi
  done
  exit 0
fi

case "$case_name" in
  continue|stop_and_next|checkpoint|missing_session|restart_review_running|\
  restart_answer_ready|restart_stop_pending|restart_stop_confirmed|\
  restart_successor_submitting|add_reconcile|failure_malformed|failure_timeout|failure_cap|\
  failure_unsafe_session|failure_unknown_kill)
    ;;
  *) die "unknown case: $case_name" ;;
esac

[ -n "$REAL_PUEUE" ] || die "pueue is required"
[ -n "$REAL_PUEUED" ] || die "pueued is required"
[ -n "$REAL_GIT" ] || die "git is required"
[ -n "$REAL_PYTHON" ] || die "python is required"
[ -n "$REAL_SQLITE" ] || die "sqlite3 is required"
[ -n "$REAL_JQ" ] || die "jq is required"
[ -n "$REAL_RUSTC" ] || die "rustc is required"
[ -n "$CARGO_BIN" ] || die "cargo is required"

if [ -z "${CARGO_TARGET_DIR:-}" ]; then
  CARGO_TARGET_DIR="$REPO_ROOT/target"
elif [[ "$CARGO_TARGET_DIR" != /* ]]; then
  CARGO_TARGET_DIR="$(pwd -P)/$CARGO_TARGET_DIR"
fi
export CARGO_TARGET_DIR
PA_BIN="$CARGO_TARGET_DIR/release/pueue-agent"

WORK="$(mktemp -d "/tmp/pa-research-e2e.${case_name}.XXXXXX")"
CONTROL="$WORK/control"
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
AGENT_TIMEOUT_MINUTES=5
REPORT="$WORK/evidence.log"
DAEMON_LOG="$WORK/daemon.log"
PUEUED_LOG="$WORK/pueued.log"
DAEMON_PID=""
RUN_ID_LOCK_PID=""
RUN_ID_LOCK_PARENT_IDENTITY=""
PUEUED_PID=""
PROJECT_ID=""
GROUP=""
CAMPAIGN_ID=""
SOURCE_EXPERIMENT_ID=""
SOURCE_TASK_ID=""
TASK_IDS="$WORK/task-ids.log"
BARRIER_PIDS="$WORK/barrier-pids.log"
RESEARCH_CONTROL="$HOME/.pueue-agent/research-control"

export HOME CODEX_HOME XDG_RUNTIME_DIR="$RUNTIME" XDG_STATE_HOME="$STATE_HOME"
export PUEUE_AGENT_STATE_DIR="$STATE_DIR" PUEUE_CONFIG_PATH="$PUEUE_CONFIG"
export PYTHONDONTWRITEBYTECODE=1
export PUEUE_AGENT_TEST_AGENT_LOG="$WORK/agent-calls.log"
export PUEUE_AGENT_TEST_AGENT_STATE="$WORK/agent-state"
export OPENAI_API_KEY="research-fixture-key-must-not-reach-child"
export AWS_SECRET_ACCESS_KEY="research-fixture-secret-must-not-reach-child"
export WANDB_API_KEY="research-fixture-wandb-must-not-reach-child"
export SSH_AUTH_SOCK="research-fixture-ssh-must-not-reach-child"

record() {
  printf '%s\n' "$*" >> "$REPORT"
  printf '%s\n' "$*"
}

pid_is_alive() {
  local pid="$1"
  case "$pid" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "$pid" -gt 1 ] || return 1
  kill -0 "$pid" 2>/dev/null
}

require_owned_pid() {
  local pid="$1"
  case "$pid" in
    ''|*[!0-9]*) die "owned PID marker was not numeric" ;;
  esac
  [ "$pid" -gt 1 ] || die "owned PID marker was unsafe"
}

wait_pid_gone() {
  local pid="$1"
  local deadline=$(( $(date +%s) + 20 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    pid_is_alive "$pid" || return 0
    sleep 0.1
  done
  return 1
}

terminate_exact_pid() {
  local pid="$1"
  case "$pid" in
    ''|*[!0-9]*) return 0 ;;
  esac
  [ "$pid" -gt 1 ] || return 0
  if pid_is_alive "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
    if ! wait_pid_gone "$pid"; then
      kill -KILL "$pid" 2>/dev/null || true
      wait_pid_gone "$pid" || true
    fi
  fi
}

cleanup() {
  local status=$?
  local cleanup_incomplete=0
  local daemon_pid=""
  local run_id_lock_pid=""
  local barrier_pid=""
  local task_id=""
  local state=""
  if [ "$#" -eq 1 ]; then
    status="$1"
  fi
  trap - EXIT TERM INT

  # Daemon first: a graceful stop gives the native owner a chance to release
  # its exact child handles.  Crash cases clear DAEMON_PID after SIGKILL/reap.
  if [ -n "$DAEMON_PID" ]; then
    daemon_pid="$DAEMON_PID"
    case "$daemon_pid" in
      ''|*[!0-9]*) cleanup_incomplete=1; daemon_pid="" ;;
      *)
        if [ "$daemon_pid" -le 1 ]; then
          cleanup_incomplete=1
          daemon_pid=""
        fi
        ;;
    esac
    if [ -n "$daemon_pid" ]; then
      if pid_is_alive "$daemon_pid"; then
        kill -TERM "$daemon_pid" 2>/dev/null || true
        if ! wait_pid_gone "$daemon_pid"; then
          kill -KILL "$daemon_pid" 2>/dev/null || true
          wait_pid_gone "$daemon_pid" || true
        fi
      fi
      pid_is_alive "$daemon_pid" && cleanup_incomplete=1
      wait "$daemon_pid" 2>/dev/null || true
    fi
    DAEMON_PID=""
  fi

  if [ -n "$RUN_ID_LOCK_PID" ]; then
    run_id_lock_pid="$RUN_ID_LOCK_PID"
    case "$run_id_lock_pid" in
      ''|*[!0-9]*) cleanup_incomplete=1; run_id_lock_pid="" ;;
      *)
        if [ "$run_id_lock_pid" -le 1 ]; then
          cleanup_incomplete=1
          run_id_lock_pid=""
        fi
        ;;
    esac
    if [ -n "$run_id_lock_pid" ]; then
      if pid_is_alive "$run_id_lock_pid"; then
        kill -TERM "$run_id_lock_pid" 2>/dev/null || true
        if ! wait_pid_gone "$run_id_lock_pid"; then
          kill -KILL "$run_id_lock_pid" 2>/dev/null || true
          wait_pid_gone "$run_id_lock_pid" || true
        fi
      fi
      wait "$run_id_lock_pid" 2>/dev/null || true
      pid_is_alive "$run_id_lock_pid" && cleanup_incomplete=1
    fi
    RUN_ID_LOCK_PID=""
    RUN_ID_LOCK_PARENT_IDENTITY=""
  fi

  # Never release a mutating proxy during cleanup.  Terminate only PIDs that a
  # harness-owned barrier wrote to its marker file.
  if [ -f "$BARRIER_PIDS" ]; then
    while IFS= read -r barrier_pid; do
      terminate_exact_pid "$barrier_pid"
      pid_is_alive "$barrier_pid" && cleanup_incomplete=1
    done < "$BARRIER_PIDS"
  fi

  # Make cleanup's own delegated Pueue operations pass through the proxy.  No
  # release is sent to an already-blocked mutating call; those exact PIDs were
  # terminated above.
  rm -f -- "$CONTROL/kill-target" "$CONTROL/add-arm" \
    "$CONTROL/add-suppress-result" "$CONTROL/status-arm"

  # Kill only task IDs recorded from this profile's submit/add output, and
  # only after the daemon and named barriers have gone away.
  if [ -n "$PUEUE_CONFIG" ] && [ -f "$PUEUE_CONFIG" ] && [ -f "$TASK_IDS" ]; then
    while IFS= read -r task_id; do
      case "$task_id" in
        ''|*[!0-9]*) continue ;;
      esac
      state="$(pueue_task_status "$task_id")"
      case "$state" in
        Running|Queued|Stashed|Locked|Paused)
          "$REAL_PUEUE" --config "$PUEUE_CONFIG" kill "$task_id" >/dev/null 2>&1 || true
          if ! wait_owned_task_terminal "$task_id"; then
            cleanup_incomplete=1
            record "CLEANUP_TASK_REMAINS task=$task_id state=$(pueue_task_status "$task_id")"
          fi
          ;;
        __missing__|Done|Success|Failed|Killed|Errored|FailedToSpawn|DependencyFailed)
          ;;
        __query_error__|__invalid_status__|*)
          cleanup_incomplete=1
          record "CLEANUP_TASK_STATUS_UNKNOWN task=$task_id state=$state"
          ;;
      esac
    done < "$TASK_IDS"
  fi

  if [ -f "$PUEUE_CONFIG" ]; then
    "$REAL_PUEUE" --config "$PUEUE_CONFIG" shutdown >/dev/null 2>&1 || true
  fi
  if [ -z "$PUEUED_PID" ] && [ -f "$RUNTIME/pueue.pid" ]; then
    PUEUED_PID="$(sed -n '1p' "$RUNTIME/pueue.pid" 2>/dev/null || true)"
  fi
  if [ -n "$PUEUED_PID" ]; then
    case "$PUEUED_PID" in
      ''|*[!0-9]*) cleanup_incomplete=1; PUEUED_PID="" ;;
      *)
        if [ "$PUEUED_PID" -le 1 ]; then
          cleanup_incomplete=1
          PUEUED_PID=""
        else
          terminate_exact_pid "$PUEUED_PID"
          pid_is_alive "$PUEUED_PID" && cleanup_incomplete=1
          wait "$PUEUED_PID" 2>/dev/null || true
        fi
        ;;
    esac
  fi

  [ "$cleanup_incomplete" -eq 0 ] || status=1
  if [ "$status" -eq 0 ]; then
    rm -rf "$WORK"
  else
    record "FAILURE_EVIDENCE workdir=$WORK"
    for log in "$DAEMON_LOG" "$PUEUED_LOG" "$REPORT"; do
      [ -f "$log" ] || continue
      printf '%s\n' "--- $log ---" >&2
      sed -n '1,260p' "$log" >&2 || true
    done
  fi
  return "$status"
}

handle_signal() {
  local status="$1"
  trap - EXIT TERM INT
  cleanup "$status" || true
  exit "$status"
}
trap 'handle_signal 143' TERM
trap 'handle_signal 130' INT
trap cleanup EXIT

pueue_task_status() {
  local task_id="$1"
  local json
  if ! json="$("$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json 2>/dev/null)"; then
    printf '%s\n' __query_error__
    return 0
  fi
  printf '%s\n' "$json" \
    | "$REAL_JQ" -r --arg id "$task_id" '
        if (.tasks | type) != "object" then
          error("Pueue status tasks is not an object")
        elif (.tasks | has($id)) | not then
          "__missing__"
        else
          .tasks[$id].status
          | if type == "object" then
              if (keys | length) == 1 then keys[0] else "__invalid_status__" end
            elif type == "string" then
              .
            else
              "__invalid_status__"
            end
        end' \
      2>/dev/null || printf '%s\n' __query_error__
}

wait_owned_task_terminal() {
  local task_id="$1"
  local deadline=$(( $(date +%s) + 30 ))
  local state=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(pueue_task_status "$task_id")"
    case "$state" in
      Running|Queued|Stashed|Locked|Paused|__query_error__|__invalid_status__|"") sleep 0.2 ;;
      __missing__|Done|Success|Failed|Killed|Errored|FailedToSpawn|DependencyFailed) return 0 ;;
      *) sleep 0.2 ;;
    esac
  done
  return 1
}

readonly_sql() {
  local query="$1"
  local first
  first="$(printf '%s' "$query" | sed -E 's/^[[:space:]]+//')"
  case "$first" in
    SELECT[[:space:]]*|SELECT|WITH[[:space:]]*|WITH) ;;
    *) die "read-only SQL rejected non-SELECT/WITH query" ;;
  esac
  "$REAL_SQLITE" -readonly -cmd '.timeout 5000' "$STATE_DB" \
    "PRAGMA query_only=ON; $query"
}

wait_for_sql() {
  local query="$1"
  local expected="$2"
  local label="$3"
  local timeout="${4:-180}"
  local deadline=$(( $(date +%s) + timeout ))
  local actual=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    actual="$(readonly_sql "$query" 2>/dev/null || true)"
    [ "$actual" = "$expected" ] && return 0
    if [ -n "$DAEMON_PID" ] && ! pid_is_alive "$DAEMON_PID"; then
      wait "$DAEMON_PID" 2>/dev/null || true
      die "$label (daemon exited; see $DAEMON_LOG)"
    fi
    sleep 0.2
  done
  record "SQL_TIMEOUT label=$label expected=$expected actual=$actual query=$query"
  die "$label"
}

wait_for_sql_any() {
  local query="$1"
  local wanted="$2"
  local label="$3"
  local timeout="${4:-180}"
  local deadline=$(( $(date +%s) + timeout ))
  local actual=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    actual="$(readonly_sql "$query" 2>/dev/null || true)"
    case ",$wanted," in
      *",$actual,"*) return 0 ;;
    esac
    sleep 0.2
  done
  record "SQL_TIMEOUT label=$label wanted=$wanted actual=$actual query=$query"
  die "$label"
}

wait_for_task_state() {
  local task_id="$1"
  local wanted="$2"
  local timeout="${3:-90}"
  local deadline=$(( $(date +%s) + timeout ))
  local state=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(pueue_task_status "$task_id")"
    [ "$state" = "$wanted" ] && return 0
    sleep 0.2
  done
  record "PUEUE_STATE_TIMEOUT task=$task_id wanted=$wanted actual=$state"
  die "task $task_id did not reach $wanted"
}

wait_for_task_terminal() {
  local task_id="$1"
  local timeout="${2:-240}"
  local deadline=$(( $(date +%s) + timeout ))
  local state=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$(pueue_task_status "$task_id")"
    case "$state" in
      Running|Queued|Stashed|Locked|Paused|__query_error__|__invalid_status__|"") ;;
      __missing__) die "task $task_id disappeared before terminal observation" ;;
      Done|Success|Failed|Killed|Errored|FailedToSpawn|DependencyFailed)
        record "PUEUE_TERMINAL task=$task_id state=$state"
        return 0
        ;;
      *) ;;
    esac
    sleep 0.2
  done
  die "task $task_id did not become terminal"
}

wait_for_marker() {
  local marker="$1"
  local label="$2"
  local timeout="${3:-20}"
  local deadline=$(( $(date +%s) + timeout ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    [ -s "$marker" ] && return 0
    sleep 0.01
  done
  die "$label marker was not reached: $marker"
}

register_task() {
  local role="$1"
  local task_id="$2"
  case "$task_id" in
    ''|*[!0-9]*) die "$role did not produce one numeric task ID" ;;
  esac
  printf '%s\n' "$task_id" >> "$TASK_IDS"
  record "TASK role=$role id=$task_id group=$GROUP"
}

submission_task_id() {
  local summary="$1"
  printf '%s\n' "$summary" | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^task=[0-9]+$/) { sub(/^task=/, "", $i); print $i } }'
}

write_project_config() {
  cat > "$PROJECT/.pueue-agent/config.toml" <<EOF
project_id = "$PROJECT_ID"
pueue_group = "$GROUP"

[agent]
program = "$BIN/fake-agent"
args = ["{prompt}"]
timeout_minutes = $AGENT_TIMEOUT_MINUTES
max_retries = 2

[agent.context]
mode = "fresh"

[check]
interval_minutes = 1
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 4096
extra_log_paths = []

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 100
max_experiments = 100
max_agent_runs = 30
EOF
  chmod 600 "$PROJECT/.pueue-agent/config.toml"
}

write_policy() {
  mkdir -p "$STATE_DIR"
  chmod 700 "$STATE_HOME" "$STATE_DIR"
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
git = "git"
python = "python"

[projects."$PROJECT_ID"]
custom_agent = "$BIN/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]
EOF
  chmod 600 "$STATE_DIR/execution-policy.toml"
}

build_native_runner() {
  local output="$1"
  local fixture="$2"
  local crate_name="${fixture//-/_}"
  crate_name="${crate_name//./_}"
  local source="$WORK/native-${fixture}.rs"
  cat > "$source" <<EOF
use std::{env, os::unix::process::CommandExt, path::PathBuf, process::Command};
fn main() {
    let script = PathBuf::from("$BIN/$fixture");
    let error = Command::new("/bin/bash")
        .arg(script)
        .args(env::args_os().skip(1))
        .exec();
    eprintln!("failed to execute fixture: {error}");
    std::process::exit(127);
}
EOF
  CARGO_HOME="$BUILD_CARGO_HOME" RUSTUP_HOME="$BUILD_RUSTUP_HOME" \
    "$REAL_RUSTC" --edition=2021 --crate-name "native_${crate_name}" -O -o "$output" "$source"
}

build_pueue_proxy() {
  local source="$WORK/pueue-proxy.rs"
  cat > "$source" <<EOF
use std::{env, fs::{self, OpenOptions}, io::Write, process::Command, thread, time::Duration};

fn write_line(path: &str, line: &str) {
    if let Some(parent) = std::path::Path::new(path).parent() { let _ = fs::create_dir_all(parent); }
    let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
    writeln!(file, "{line}").unwrap();
}
fn wait_for(path: &str, timeout_ms: u64) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_millis(timeout_ms) {
        if std::path::Path::new(path).exists() { return true; }
        thread::sleep(Duration::from_millis(10));
    }
    false
}
fn ready_review() -> Option<String> {
    let query = r#"PRAGMA query_only=ON;
SELECT review.review_id
  FROM research_reviews AS review
  JOIN campaigns AS campaign ON campaign.campaign_id = review.campaign_id
  JOIN projects AS project ON project.project_id = campaign.project_id
  JOIN campaign_research AS research_state
    ON research_state.campaign_id = review.campaign_id
  JOIN agent_runs AS run ON run.run_id = review.agent_run_id
 WHERE review.state = 'ready'
   AND review.operation_stage IS NULL
   AND review.termination_request_id IS NULL
   AND review.decision_cycle_id IS NULL
   AND review.successor_experiment_id IS NULL
   AND review.context_json IS NOT NULL
   AND review.response_json IS NOT NULL
   AND review.event_id IS NOT NULL
   AND campaign.state = 'active'
   AND project.enabled = 1
   AND project.paused = 0
   AND project.halted_reason IS NULL
   AND run.project_id = campaign.project_id
   AND run.execution_kind = 'campaign_research'
   AND run.status IN ('completed', 'failed', 'timed_out', 'cancelled')
   AND run.launch_gate_state IN ('released', 'failed')
   AND json_valid(review.notes_json) = 1
   AND json_extract(review.notes_json, '$.native_recovery.cleanup.phase') = 'complete'
   AND json_extract(review.notes_json, '$.native_recovery.version') = 1
   AND json_extract(review.notes_json, '$.native_recovery.run_id') = review.agent_run_id
   AND json_extract(review.notes_json, '$.native_recovery.review_id') = review.review_id
   AND json_extract(review.notes_json, '$.native_recovery.campaign_id') = review.campaign_id
   AND json_extract(review.notes_json, '$.native_recovery.experiment_id') = review.experiment_id
   AND json_extract(review.notes_json, '$.native_recovery.attempt') = review.attempt
   AND json_extract(review.notes_json, '$.native_recovery.session_generation') = review.session_generation
   AND json_type(review.notes_json, '$.native_recovery.session_id') = 'text'
   AND json_extract(review.notes_json, '$.native_recovery.session_id') <> ''
   AND research_state.session_generation = review.session_generation
   AND ((json_extract(review.notes_json, '$.native_recovery.fresh_launch') = 0
         AND research_state.session_id = json_extract(review.notes_json, '$.native_recovery.session_id'))
     OR (json_extract(review.notes_json, '$.native_recovery.fresh_launch') = 1
         AND research_state.session_id IS NOT NULL
         AND json_extract(review.notes_json, '$.session_binding') = 'confirmed'
         AND json_extract(review.notes_json, '$.planned_session_id') = json_extract(review.notes_json, '$.native_recovery.session_id')
         AND json_extract(review.notes_json, '$.confirmed_session_id') = research_state.session_id))
 ORDER BY review.created_at, review.review_id
 LIMIT 2;"#;
    let output = Command::new("$REAL_SQLITE")
        .args(["-readonly", "-cmd", ".timeout 5000", "$STATE_DB", query])
        .output()
        .ok()?;
    if !output.status.success() { return None; }
    let ids = String::from_utf8(output.stdout).ok()?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if ids.len() == 1 { ids.into_iter().next() } else { None }
}
fn main() {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    let operation_index = args.iter().position(|arg| arg == "add" || arg == "kill" || arg == "status");
    let operation = operation_index.map(|index| args[index].to_string_lossy().into_owned()).unwrap_or_default();
    let rendered = args.iter().map(|arg| arg.to_string_lossy().into_owned()).collect::<Vec<_>>().join(" ");
    let work = "$WORK";
    if operation == "kill" {
        write_line(&format!("{work}/pueue-kills.log"), &rendered);
        let target = operation_index.and_then(|index| args.get(index + 1)).map(|arg| arg.to_string_lossy().into_owned()).unwrap_or_default();
        let configured = fs::read_to_string(format!("{work}/control/kill-target")).unwrap_or_default().trim().to_owned();
        if !configured.is_empty() && target == configured {
            write_line(&format!("{work}/control/kill-pids"), &std::process::id().to_string());
            write_line(&format!("{work}/control/kill-entered"), &std::process::id().to_string());
            if !wait_for(&format!("{work}/control/kill-release"), 120_000) { std::process::exit(75); }
        }
    }
    if operation == "add" {
        if std::path::Path::new(&format!("{work}/control/add-arm")).exists() {
            write_line(&format!("{work}/pueue-add-argv.log"), "ADD_BEGIN");
            for (index, arg) in args.iter().enumerate() { write_line(&format!("{work}/pueue-add-argv.log"), &format!("ADD_ARG_{}={}", index + 1, arg.to_string_lossy())); }
            write_line(&format!("{work}/pueue-add-argv.log"), "ADD_END");
            write_line(&format!("{work}/control/add-pids"), &std::process::id().to_string());
            write_line(&format!("{work}/control/add-entered"), &std::process::id().to_string());
            if !wait_for(&format!("{work}/control/add-release"), 120_000) { std::process::exit(75); }
        }
    }
    if operation == "status" && args.iter().any(|arg| arg == "--json") {
        let output = Command::new("$REAL_PUEUE").args(&args).output().expect("run real Pueue");
        if std::path::Path::new(&format!("{work}/control/status-arm")).exists() {
            if let Some(review_id) = ready_review() {
                write_line(&format!("{work}/control/status-pids"), &std::process::id().to_string());
                write_line(&format!("{work}/control/status-review-id"), &review_id);
                write_line(&format!("{work}/control/status-entered"), &std::process::id().to_string());
                if !wait_for(&format!("{work}/control/status-release"), 120_000) { std::process::exit(75); }
            }
        }
        let _ = std::io::stdout().write_all(&output.stdout);
        let _ = std::io::stderr().write_all(&output.stderr);
        std::process::exit(output.status.code().unwrap_or(125));
    }
    if operation == "add" && std::path::Path::new(&format!("{work}/control/add-suppress-result")).exists() {
        let output = Command::new("$REAL_PUEUE").args(&args).output().expect("run real Pueue");
        if !output.status.success() {
            let _ = std::io::stdout().write_all(&output.stdout);
            let _ = std::io::stderr().write_all(&output.stderr);
            std::process::exit(output.status.code().unwrap_or(125));
        }
        // The real add has already committed its external task. Suppress the
        // returned task-id/output and surface an unknown result to production,
        // which must reconcile the exact task instead of blindly adding again.
        std::process::exit(17);
    }
    let status = Command::new("$REAL_PUEUE").args(&args).status().expect("run real Pueue");
    std::process::exit(status.code().unwrap_or(125));
}
EOF
  CARGO_HOME="$BUILD_CARGO_HOME" RUSTUP_HOME="$BUILD_RUSTUP_HOME" \
    "$REAL_RUSTC" --edition=2021 --crate-name pueue_research_proxy -O -o "$BIN/pueue" "$source"
}

build_fixture_tools() {
  mkdir -p "$BIN" "$CONTROL" "$HOME/.pueue-agent" "$RESEARCH_CONTROL" "$CODEX_HOME" "$RUNTIME" "$PUEUE_DIR"
  chmod 700 "$HOME" "$HOME/.pueue-agent" "$CODEX_HOME" "$RUNTIME" "$PUEUE_DIR"
  cp "$REPO_ROOT/tests/support/fake_agent.sh" "$BIN/fake-agent.sh"
  cp "$REPO_ROOT/tests/support/fake_codex.sh" "$BIN/fake-codex.sh"
  chmod 700 "$BIN/fake-agent.sh" "$BIN/fake-codex.sh"
  cat > "$BIN/codex-wrapper.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
fake_codex="${BASH_SOURCE[0]%/*}/fake-codex"
output=""
research=0
decision=0
expect_output=0
for argument in "$@"; do
  if [ "$expect_output" -eq 1 ]; then
    output="$argument"
    expect_output=0
  fi
  if [ "$argument" = "--output-last-message" ]; then
    expect_output=1
  fi
done
case "$output" in
  */research.json|research.json) research=1 ;;
  */decision.json|decision.json) decision=1 ;;
esac
if [ "$decision" -eq 1 ] && [ -f "$HOME/.pueue-agent/decision-control.json" ]; then
  invoked="$(jq -er '.invoked_path' "$HOME/.pueue-agent/decision-control.json")"
  release="$(jq -er '.release_path' "$HOME/.pueue-agent/decision-control.json")"
  printf '%s\n' "$$" > "$invoked"
  printf '%s\n' "$$" >> "$HOME/../research-barrier-pids.log"
  released=0
  for _ in $(seq 1 12000); do
    if [ -e "$release" ]; then released=1; break; fi
    sleep 0.01
  done
  [ "$released" -eq 1 ] || exit 75
fi
if [ "$research" -eq 1 ] && [ -f "$HOME/.pueue-agent/research-failure-mode" ]; then
  failure_mode="$(sed -n '1p' "$HOME/.pueue-agent/research-failure-mode")"
  case "$failure_mode" in
    timeout)
      sleep 125
      exit 75
      ;;
    *)
      "$fake_codex" "$@"
      status=$?
      if [ "$status" -eq 0 ]; then
        case "$failure_mode" in
          malformed) printf '%s\n' '{malformed-research' > "$output" ;;
          cap) head -c 1048577 /dev/zero | tr '\0' x > "$output" ;;
        esac
      fi
      exit "$status"
      ;;
  esac
fi
exec "$fake_codex" "$@"
EOF
  chmod 700 "$BIN/codex-wrapper.sh"
  # The runner is the verified native executable; it then enters the shell
  # fixture through an absolute path.  This works with /proc/self/fd launch.
  build_native_runner "$BIN/fake-agent" "fake-agent.sh"
  build_native_runner "$BIN/fake-codex" "fake-codex.sh"
  build_native_runner "$BIN/codex" "codex-wrapper.sh"
  build_pueue_proxy

  for command_path in /bin/bash /bin/cat /bin/chmod /bin/date /bin/dirname \
      /bin/find /bin/head /bin/mkdir /bin/rm /bin/sleep /bin/stat /bin/tr \
      /usr/bin/awk /usr/bin/basename /usr/bin/id /usr/bin/seq /usr/bin/sed \
      /usr/bin/sha256sum; do
    [ -x "$command_path" ] || continue
    cp "$command_path" "$BIN/$(basename "$command_path")"
  done
  cp "$REAL_JQ" "$BIN/jq"
  cp "$REAL_GIT" "$BIN/git"
  cp "$REAL_PYTHON" "$BIN/python"
  chmod 700 "$BIN"/*
  export PATH="$BIN:$ORIGINAL_PATH"
}

start_pueue() {
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
  "$REAL_PUEUED" --config "$PUEUE_CONFIG" -d > "$PUEUED_LOG" 2>&1
  for _ in $(seq 1 150); do
    "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json >/dev/null 2>&1 && break
    sleep 0.1
  done
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json >/dev/null 2>&1 \
    || die "isolated pueued did not start"
  PUEUED_PID="$(sed -n '1p' "$RUNTIME/pueue.pid" 2>/dev/null || true)"
  case "$PUEUED_PID" in
    ''|*[!0-9]*) die "isolated pueued did not publish an owned numeric PID" ;;
  esac
  [ "$PUEUED_PID" -gt 1 ] || die "isolated pueued published an unsafe PID"
  record "PUEUED_PID=$PUEUED_PID profile=$PUEUE_CONFIG"
}

start_systemctl_fixture() {
  cat > "$BIN/systemctl" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *--property=LoadState*) printf '%s\n' loaded ;;
  *is-active*) printf '%s\n' active ;;
esac
exit 0
EOF
  chmod 700 "$BIN/systemctl"
}

prepare_project() {
  mkdir -p "$PROJECT"
  "$PA_BIN" init "$PROJECT" >/dev/null
  PROJECT_ID="$(sed -n 's/^project_id = "\([^"]*\)"$/\1/p' "$PROJECT/.pueue-agent/config.toml")"
  GROUP="$(sed -n 's/^pueue_group = "\([^"]*\)"$/\1/p' "$PROJECT/.pueue-agent/config.toml")"
  [ -n "$PROJECT_ID" ] && [ -n "$GROUP" ] || die "init did not emit project/group"
  write_project_config
  cat > "$PROJECT/.gitignore" <<'EOF'
.pueue-agent/
__pycache__/
.pytest_cache/
EOF
  cp "$REPO_ROOT/tests/e2e/research_experiment/train.py" "$PROJECT/train.py"
  cat > "$PROJECT/.pueue-agent/STATE.md" <<EOF
PUEUE_AGENT_E2E_RESEARCH_STOP
Run a bounded real CPU research campaign and preserve the source checkout.
EOF
  chmod 600 "$PROJECT/train.py" "$PROJECT/.gitignore" "$PROJECT/.pueue-agent/STATE.md"
  "$REAL_GIT" init --quiet -b main "$PROJECT"
  "$REAL_GIT" -C "$PROJECT" config user.name "Pueue Agent Research E2E"
  "$REAL_GIT" -C "$PROJECT" config user.email "pueue-agent-research-e2e@example.invalid"
  "$REAL_GIT" -C "$PROJECT" add .gitignore train.py
  "$REAL_GIT" -C "$PROJECT" commit --quiet -m "research CPU learner baseline"
  write_policy
  "$PA_BIN" enable --pueue-config "$PUEUE_CONFIG" "$PROJECT" >/dev/null
  record "PROJECT_ID=$PROJECT_ID GROUP=$GROUP PROJECT=$PROJECT"
}

setup_case() {
  mkdir -p "$WORK"
  build_fixture_tools
  CARGO_HOME="$BUILD_CARGO_HOME" RUSTUP_HOME="$BUILD_RUSTUP_HOME" \
    "$CARGO_BIN" build --quiet --locked --offline --release --manifest-path "$REPO_ROOT/Cargo.toml"
  [ -x "$PA_BIN" ] || die "release pueue-agent binary is missing"
  start_systemctl_fixture
  start_pueue
  prepare_project
}

write_research_scenario() {
  local action="$1"
  local session_id="$2"
  local invoked="${3:-}"
  local release="${4:-}"
  local next_direction="${5:-minimize}"
  local control_json='{}'
  if [ -n "$invoked" ]; then
    control_json="$($REAL_JQ -cn --arg invoked "$invoked" --arg release "$release" '{invoked_path:$invoked,release_path:$release}')"
  fi
  "$REAL_JQ" -cn --arg session "$session_id" --arg action "$action" \
    --arg reason "research fixture action $action" \
    --arg notes "saved advice for $action" --arg direction "$next_direction" \
    --argjson control "$control_json" \
    '{session_id:$session,action:$action,reason:$reason,notes:$notes,next_direction:$direction,control:$control}' \
    > "$HOME/.pueue-agent/research-scenario.json"
  chmod 600 "$HOME/.pueue-agent/research-scenario.json"
}

submit_source() {
  local steps="${1:-180}"
  local delay="${2:-1}"
  local summary
  summary="$(cd "$PROJECT" && "$PA_BIN" submit --metric-name loss --metric-direction minimize -- \
    python train.py --steps "$steps" --learning-rate 0.02 --step-delay "$delay" \
    --checkpoint-dir .pueue-agent/artifacts)"
  SOURCE_TASK_ID="$(submission_task_id "$summary")"
  register_task source "$SOURCE_TASK_ID"
  CAMPAIGN_ID="$(readonly_sql "SELECT campaign_id FROM campaigns WHERE project_id = '$PROJECT_ID' AND state <> 'retired'")"
  wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND pueue_task_id = $SOURCE_TASK_ID" "1" "source experiment row"
  SOURCE_EXPERIMENT_ID="$(readonly_sql "SELECT experiment_id FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND pueue_task_id = $SOURCE_TASK_ID")"
  [ -n "$CAMPAIGN_ID" ] && [ -n "$SOURCE_EXPERIMENT_ID" ] || die "source identity missing"
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  record "SOURCE campaign=$CAMPAIGN_ID experiment=$SOURCE_EXPERIMENT_ID task=$SOURCE_TASK_ID"
}

start_daemon() {
  : > "$DAEMON_LOG"
  "$PA_BIN" daemon --foreground --pueue-config "$PUEUE_CONFIG" > "$DAEMON_LOG" 2>&1 &
  DAEMON_PID=$!
  case "$DAEMON_PID" in ''|*[!0-9]*) die "daemon PID was not numeric" ;; esac
  [ "$DAEMON_PID" -gt 1 ] || die "daemon PID was unsafe"
  record "DAEMON_START pid=$DAEMON_PID"
}

stop_daemon_graceful() {
  [ -n "$DAEMON_PID" ] || return 0
  local old_pid="$DAEMON_PID"
  if pid_is_alive "$old_pid"; then
    kill -TERM "$old_pid" 2>/dev/null || true
    if ! wait_pid_gone "$old_pid"; then
      kill -KILL "$old_pid" 2>/dev/null || true
      wait_pid_gone "$old_pid" || true
    fi
  fi
  wait "$old_pid" 2>/dev/null || true
  DAEMON_PID=""
}

crash_daemon_exact() {
  local old_pid="$DAEMON_PID"
  case "$old_pid" in ''|*[!0-9]*) die "crash boundary has no daemon PID" ;; esac
  [ "$old_pid" -gt 1 ] || die "crash boundary has an unsafe daemon PID"
  kill -KILL "$old_pid" 2>/dev/null || true
  wait "$old_pid" 2>/dev/null || true
  DAEMON_PID=""
  record "DAEMON_HARD_CRASH pid=$old_pid signal=SIGKILL reaped=true"
}

release_marker() {
  local path="$1"
  : > "$path"
}

record_barrier_pid_file() {
  local path="$1"
  [ -f "$path" ] || return 0
  while IFS= read -r pid; do
    case "$pid" in ''|*[!0-9]*) die "barrier marker contained a nonnumeric PID" ;; esac
    [ "$pid" -gt 1 ] || die "barrier marker contained an unsafe PID"
    printf '%s\n' "$pid" >> "$BARRIER_PIDS"
  done < "$path"
}

session_files_for() {
  local session_id="$1"
  find "$CODEX_HOME/sessions" -type f ! -type l -name "*-$session_id.jsonl" -print 2>/dev/null
}

session_file_for() {
  local session_id="$1"
  session_files_for "$session_id" | sed -n '1p'
}

session_file_count() {
  local session_id="$1"
  session_files_for "$session_id" | awk 'NF { count++ } END { print count + 0 }'
}

checkpoint_info() {
  local experiment_id="$1"
  local minimum_step="${2:-1}"
  local root="$PROJECT/.pueue-agent/artifacts/$experiment_id"
  local path=""
  local step=""
  path="$(find "$root" -maxdepth 1 -type f -name 'step-*.json' -print 2>/dev/null \
    | "$REAL_PYTHON" -c 'import pathlib,sys; p=[pathlib.Path(x) for x in sys.stdin.read().splitlines()]; p.sort(key=lambda x:int(x.stem.split("-")[1])); print(p[-1] if p else "")')"
  [ -n "$path" ] || return 1
  step="$($REAL_JQ -er '.step' "$path")"
  [ "$step" -ge "$minimum_step" ] || return 1
  printf '%s\t%s\t%s\n' "$path" "$step" "$($REAL_JQ -er '.weight' "$path")"
}

wait_for_checkpoint() {
  local experiment_id="$1"
  local minimum_step="$2"
  local timeout="${3:-120}"
  local deadline=$(( $(date +%s) + timeout ))
  local line=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    line="$(checkpoint_info "$experiment_id" "$minimum_step" || true)"
    if [ -n "$line" ]; then
      printf '%s\n' "$line"
      return 0
    fi
    sleep 0.2
  done
  die "source checkpoint did not reach step $minimum_step"
}

checkpoint_digest() {
  local path="$1"
  "$REAL_PYTHON" - "$path" <<'PY'
import hashlib
import pathlib
import sys
print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())
PY
}

heldout_loss() {
  local path="$1"
  "$REAL_PYTHON" - "$path" <<'PY'
import json
import math
import pathlib
import sys
value = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
weight = value["weight"]
xs = (-0.75, -0.25, 0.25, 0.75)
loss = sum((weight * x - 2.0 * x) ** 2 for x in xs) / len(xs)
assert math.isfinite(loss)
print(repr(loss))
PY
}

result_manifest_loss() {
  local experiment_id="$1"
  "$REAL_JQ" -er '.metrics.loss' "$PROJECT/.pueue-agent/results/$experiment_id.json"
}

assert_float_equal() {
  local expected="$1"
  local actual="$2"
  "$REAL_PYTHON" - "$expected" "$actual" <<'PY'
import math
import sys
expected = float(sys.argv[1])
actual = float(sys.argv[2])
if not math.isfinite(expected) or not math.isfinite(actual) or abs(expected - actual) > 1e-12:
    raise SystemExit(1)
PY
}

experiment_count() {
  readonly_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID'"
}

proposal_count() {
  readonly_sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$CAMPAIGN_ID'"
}

submission_count() {
  readonly_sql "SELECT COUNT(*) FROM submissions WHERE project_id = '$PROJECT_ID'"
}

assert_add_argv_for_experiment() {
  local experiment_id="$1"
  local submission_argv runtime_argv expected_argv captured_argv
  local add_log="$WORK/pueue-add-argv.log"
  [ -f "$add_log" ] || die "real add argv evidence is missing"
  [ "$(grep -c '^ADD_BEGIN$' "$add_log")" = 1 ] || die "expected exactly one captured add begin"
  [ "$(grep -c '^ADD_END$' "$add_log")" = 1 ] || die "expected exactly one captured add end"
  submission_argv="$(readonly_sql "SELECT argv_json FROM submissions WHERE submission_id = (SELECT submission_id FROM experiments WHERE experiment_id = '$experiment_id')")"
  runtime_argv="$($REAL_JQ -cn --arg experiment "$experiment_id" --arg campaign "$CAMPAIGN_ID" --arg project "$PROJECT" --argjson user_argv "$submission_argv" \
    '["/usr/bin/env", ("PUEUE_AGENT_EXPERIMENT_ID=" + $experiment), ("PUEUE_AGENT_CAMPAIGN_ID=" + $campaign), ("PUEUE_AGENT_RESULT_PATH=" + $project + "/.pueue-agent/results/" + $experiment + ".json"), ("PUEUE_AGENT_ARTIFACT_DIR=" + $project + "/.pueue-agent/artifacts/" + $experiment)] + $user_argv')"
  expected_argv="$($REAL_JQ -cn --arg group "$GROUP" --arg cwd "$PROJECT" --argjson argv "$runtime_argv" \
    '["--config", "/dev/fd/9", "add", "--print-task-id", "-g", $group, "--working-directory", $cwd, "--escape", "--"] + $argv')"
  captured_argv="$(sed -n 's/^ADD_ARG_[0-9]*=//p' "$add_log" | "$REAL_JQ" -Rsc 'split("\n") | map(select(length > 0))')"
  [ "$(printf '%s' "$captured_argv" | "$REAL_JQ" -cS '.')" = "$(printf '%s' "$expected_argv" | "$REAL_JQ" -cS '.')" ] \
    || die "captured add argv did not match production runtime argv wrapper"
  record "ADD_ARGV_MATCH experiment=$experiment_id submission_argv=$submission_argv runtime_argv=$runtime_argv"
}

assert_source_unchanged() {
  local before="$1"
  [ "$($REAL_GIT -C "$PROJECT" rev-parse refs/heads/main)" = "$before" ] \
    || die "source main changed"
  [ "$($REAL_GIT -C "$PROJECT" hash-object "$PROJECT/train.py")" = "$2" ] \
    || die "source train.py changed"
}

await_first_review() {
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "first research review" "420"
  record "FIRST_REVIEW $(readonly_sql "SELECT review_id || ':' || state || ':' || session_generation FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
}

run_continue_case() {
  local first_session second_session source_task_signature checkpoint_line checkpoint_path checkpoint_step checkpoint_weight
  local checkpoint_after_line checkpoint_after_path checkpoint_after_step checkpoint_after_weight
  local main_before source_before
  setup_case
  write_research_scenario continue 11111111-1111-4111-8111-111111111111
  submit_source 300 1
  main_before="$(git -C "$PROJECT" rev-parse refs/heads/main)"
  source_before="$(git -C "$PROJECT" hash-object "$PROJECT/train.py")"
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  checkpoint_line="$(wait_for_checkpoint "$SOURCE_EXPERIMENT_ID" 2 120)"
  [ -n "$checkpoint_line" ] || die "real source checkpoint did not appear"
  checkpoint_path="${checkpoint_line%%$'\t'*}"
  checkpoint_step="$(printf '%s' "$checkpoint_line" | cut -f2)"
  checkpoint_weight="$(printf '%s' "$checkpoint_line" | cut -f3)"
  [ "$checkpoint_step" -ge 2 ] || die "continue checkpoint did not independently observe step advance"
  record "CONTINUE_CHECKPOINT path=$checkpoint_path step=$checkpoint_step weight=$checkpoint_weight digest=$(checkpoint_digest "$checkpoint_path")"
  source_task_signature="$(readonly_sql "SELECT task_signature FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  [ -n "$source_task_signature" ] || die "continue source task signature was not durably observed"
  await_first_review
  wait_for_task_state "$SOURCE_TASK_ID" Running 30
  [ "$(readonly_sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")" = "$SOURCE_TASK_ID" ] \
    || die "continue changed the source task identity"
  [ "$(readonly_sql "SELECT task_signature FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")" = "$source_task_signature" ] \
    || die "continue changed the source task signature"
  checkpoint_after_line="$(wait_for_checkpoint "$SOURCE_EXPERIMENT_ID" "$((checkpoint_step + 1))" 120)"
  checkpoint_after_path="${checkpoint_after_line%%$'\t'*}"
  checkpoint_after_step="$(printf '%s' "$checkpoint_after_line" | cut -f2)"
  checkpoint_after_weight="$(printf '%s' "$checkpoint_after_line" | cut -f3)"
  [ "$checkpoint_after_step" -gt "$checkpoint_step" ] || die "continue did not advance to a later real checkpoint"
  record "CONTINUE_AFTER_CHECKPOINT path=$checkpoint_after_path step=$checkpoint_after_step weight=$checkpoint_after_weight digest=$(checkpoint_digest "$checkpoint_after_path") task=$SOURCE_TASK_ID signature=$source_task_signature"
  first_session="$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")"
  [ "$first_session" = 11111111-1111-4111-8111-111111111111 ] || die "first research session identity mismatch"
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "2" "second research review" "420"
  second_session="$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")"
  [ "$second_session" = "$first_session" ] || die "continue did not reuse exact session"
  [ "$(grep -c '^RESEARCH_INVOCATION ' "$WORK/home/../research-codex-calls.log")" = 2 ] \
    || die "continue did not invoke research twice"
  grep -q 'mode=resume session_id=11111111-1111-4111-8111-111111111111' "$WORK/home/../research-codex-calls.log" \
    || die "second review did not use exact resume session"
  [ "$(proposal_count)" = 1 ] || die "continue created an unexpected proposal"
  [ "$(experiment_count)" = 1 ] || die "continue created an unexpected experiment"
  [ "$(submission_count)" = 1 ] || die "continue created an unexpected submission"
  readonly_sql "SELECT notes_json FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" \
    | grep -q 'saved_advice' || die "continue did not persist advice"
  assert_source_unchanged "$main_before" "$source_before"
  record "CASE continue PASS exact_session=true reviews=2 proposals=1 experiments=1 submissions=1"
}

run_missing_session_case() {
  local session_id=22222222-2222-4222-9222-222222222222
  local replacement_id=33333333-3333-4333-a333-333333333333
  local session_path replacement_session_path notes_before notes_after budget_before budget_after review_calls replacement_session_count
  setup_case
  # The first identity is valid; the harness removes exactly its disposable
  # session JSONL after the completed review, then asks production recovery to
  # establish a fresh generation. The fixture never edits SQLite.
  write_research_scenario continue "$session_id"
  submit_source 300 1
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  await_first_review
  notes_before="$(readonly_sql "SELECT notes_json FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
  budget_before="$(readonly_sql "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = '$CAMPAIGN_ID' AND dimension = 'agent_run' AND status = 'consumed'")"
  [ "$budget_before" -ge 1 ] || die "missing-session case did not consume the first research budget"
  printf '%s' "$notes_before" | grep -q 'saved_advice' || die "first review advice was not persisted before session removal"
  session_path="$(session_file_for "$session_id")"
  [ -n "$session_path" ] || die "completed research did not leave owned session artifact"
  rm -f -- "$session_path"
  [ ! -e "$session_path" ] || die "owned session artifact was not removed"
  write_research_scenario continue "$replacement_id"
  wait_for_sql "SELECT session_generation FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'" "1" "missing-session generation increment" "420"
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "2" "missing-session fresh review" "420"
  [ "$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")" = "$replacement_id" ] \
    || die "missing-session recovery did not persist fresh UUID"
  [ "$(readonly_sql "SELECT session_generation FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")" = 1 ] \
    || die "missing-session recovery did not create a fresh generation"
  [ "$replacement_id" != "$session_id" ] || die "missing-session replacement reused the removed identity"
  replacement_session_count="$(session_file_count "$replacement_id")"
  [ "$replacement_session_count" = 1 ] \
    || die "missing-session recovery did not create exactly one fresh session artifact"
  replacement_session_path="$(session_file_for "$replacement_id")"
  [ -n "$replacement_session_path" ] || die "missing-session recovery did not select its sole fresh session artifact"
  notes_after="$(readonly_sql "SELECT notes_json FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
  [ "$notes_after" = "$notes_before" ] || die "missing-session recovery changed saved notes"
  printf '%s' "$notes_after" | grep -q 'saved_advice' || die "missing-session recovery lost saved advice"
  budget_after="$(readonly_sql "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = '$CAMPAIGN_ID' AND dimension = 'agent_run' AND status = 'consumed'")"
  [ "$budget_after" -gt "$budget_before" ] || die "missing-session recovery reset or lost consumed research budget"
  review_calls="$(grep -c '^RESEARCH_INVOCATION ' "$WORK/home/../research-codex-calls.log")"
  [ "$review_calls" = 2 ] || die "missing-session recovery launched an unexpected number of reviews"
  [ "$(experiment_count)" = 1 ] || die "missing-session recovery created extra experiment"
  [ "$(submission_count)" = 1 ] || die "missing-session recovery created extra submission"
  record "CASE missing_session PASS removed_session=$session_id fresh_session=$replacement_id generation=1 notes_retained=true consumed_budget_before=$budget_before consumed_budget_after=$budget_after"
}

arm_kill_barrier() {
  printf '%s\n' "$SOURCE_TASK_ID" > "$CONTROL/kill-target"
  : > "$CONTROL/kill-release-unused"
}

wait_for_kill_barrier() {
  wait_for_marker "$CONTROL/kill-entered" "stop-pending kill" 240
  record_barrier_pid_file "$CONTROL/kill-pids"
}

start_run_id_admission_lock() {
  local entered="$CONTROL/run-id-admission-entered"
  local marker_pid marker_device marker_inode state_parent_identity
  [ -z "$RUN_ID_LOCK_PID" ] || die "run-ID admission lock helper is already active"
  rm -f -- "$entered" "$CONTROL/run-id-admission-release" \
    "$CONTROL/run-id-admission-released"
  state_parent_identity="$("$REAL_PYTHON" -c '
import os, sys
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
fd = os.open(os.path.dirname(os.path.abspath(sys.argv[1])), flags)
st = os.fstat(fd)
print(f"{st.st_dev}|{st.st_ino}")
os.close(fd)
' "$STATE_DB")" || die "could not identify the exact state database parent"
  "$REAL_PYTHON" - "$STATE_DB" "$entered" \
    "$CONTROL/run-id-admission-release" "$CONTROL/run-id-admission-released" 480 \
    > "$WORK/run-id-admission-lock.log" 2>&1 <<'RUN_ID_LOCK_HELPER' &
import fcntl
import os
import sys
import time

db_path, entered_path, release_path, released_path, timeout_text = sys.argv[1:]
flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
state_parent_fd = os.open(os.path.dirname(os.path.abspath(db_path)), flags)
state_parent = os.fstat(state_parent_fd)
fcntl.flock(state_parent_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

def write_marker(path, payload):
    marker_fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        encoded = (payload + "\n").encode("ascii")
        if os.write(marker_fd, encoded) != len(encoded):
            raise OSError("short marker write")
        os.fsync(marker_fd)
    finally:
        os.close(marker_fd)

write_marker(entered_path, f"{os.getpid()}|{state_parent.st_dev}|{state_parent.st_ino}")
deadline = time.monotonic() + float(timeout_text)
while not os.path.exists(release_path):
    if time.monotonic() >= deadline:
        raise SystemExit(75)
    time.sleep(0.05)
fcntl.flock(state_parent_fd, fcntl.LOCK_UN)
os.close(state_parent_fd)
write_marker(released_path, str(os.getpid()))
RUN_ID_LOCK_HELPER
  RUN_ID_LOCK_PID="$!"
  require_owned_pid "$RUN_ID_LOCK_PID"
  wait_for_marker "$entered" "run-ID admission lock acquisition" 10
  IFS='|' read -r marker_pid marker_device marker_inode < "$entered"
  [ "$marker_pid" = "$RUN_ID_LOCK_PID" ] \
    || die "run-ID admission lock marker did not identify its direct child"
  [ "$marker_device|$marker_inode" = "$state_parent_identity" ] \
    || die "run-ID admission lock did not hold the exact state database parent"
  pid_is_alive "$RUN_ID_LOCK_PID" \
    || die "run-ID admission lock helper exited before the crash boundary"
  RUN_ID_LOCK_PARENT_IDENTITY="$state_parent_identity"
  record "RUN_ID_ADMISSION_LOCK phase=confirmed-handoff-gate pid=$RUN_ID_LOCK_PID parent_device_inode=$RUN_ID_LOCK_PARENT_IDENTITY"
}

release_run_id_admission_lock() {
  local lock_pid="$RUN_ID_LOCK_PID"
  local marker_pid marker_device marker_inode wait_status=0
  require_owned_pid "$lock_pid"
  IFS='|' read -r marker_pid marker_device marker_inode < "$CONTROL/run-id-admission-entered"
  [ "$marker_pid" = "$lock_pid" ] \
    || die "run-ID admission lock owner changed before release"
  [ "$marker_device|$marker_inode" = "$RUN_ID_LOCK_PARENT_IDENTITY" ] \
    || die "run-ID admission lock parent identity changed before release"
  : > "$CONTROL/run-id-admission-release"
  wait_for_marker "$CONTROL/run-id-admission-released" "run-ID admission lock release" 10
  [ "$(sed -n '1p' "$CONTROL/run-id-admission-released")" = "$lock_pid" ] \
    || die "run-ID admission lock release marker did not identify its direct child"
  wait "$lock_pid" || wait_status=$?
  RUN_ID_LOCK_PID=""
  RUN_ID_LOCK_PARENT_IDENTITY=""
  [ "$wait_status" -eq 0 ] \
    || die "run-ID admission lock helper exited with status $wait_status"
  record "RUN_ID_ADMISSION_LOCK_RELEASED pid=$lock_pid reaped=true"
}

release_kill_and_restart() {
  local old_proxy_pid
  old_proxy_pid="$(sed -n '1p' "$CONTROL/kill-pids")"
  require_owned_pid "$old_proxy_pid"
  crash_daemon_exact
  terminate_exact_pid "$old_proxy_pid"
  rm -f -- "$CONTROL/kill-target"
  record "STOP_PENDING_PREDELEGATION delegated=false proxy_pid=$old_proxy_pid"
  start_daemon
}

await_stop_confirmed() {
  local allow_successor_stage="${1:-false}"
  local operation_stage
  wait_for_task_terminal "$SOURCE_TASK_ID" 180
  wait_for_sql "SELECT status FROM termination_requests WHERE project_id = '$PROJECT_ID' ORDER BY request_id DESC LIMIT 1" "confirmed" "termination confirmation" "240"
  if [ "$allow_successor_stage" = true ]; then
    wait_for_sql_any "SELECT operation_stage FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "stop_confirmed,successor_reserved" "checkpoint stop confirmation stage" "240"
    operation_stage="$(readonly_sql "SELECT operation_stage FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
    case "$operation_stage" in
      stop_confirmed|successor_reserved) ;;
      *) die "checkpoint stop confirmation advanced to unexpected stage: $operation_stage" ;;
    esac
  else
    wait_for_sql "SELECT COUNT(*) FROM research_reviews AS review JOIN termination_requests AS request ON request.request_id = review.termination_request_id JOIN decision_cycles AS cycle ON cycle.cycle_id = review.decision_cycle_id WHERE review.campaign_id = '$CAMPAIGN_ID' AND review.experiment_id = '$SOURCE_EXPERIMENT_ID' AND review.state = 'completed' AND review.operation_stage IS NULL AND review.successor_experiment_id IS NULL AND request.project_id = '$PROJECT_ID' AND request.status = 'confirmed' AND cycle.campaign_id = review.campaign_id AND cycle.source_experiment_id = review.experiment_id" "1" "research stop confirmation handoff" "240"
  fi
}

wait_for_skipped_no_metric_successor() {
  local successor_experiment_id="$1"
  wait_for_sql "SELECT COUNT(*)
    FROM campaigns AS campaign
    JOIN experiments AS source
      ON source.experiment_id = campaign.baseline_experiment_id
     AND source.campaign_id = campaign.campaign_id
    JOIN experiment_metrics AS source_metric
      ON source_metric.experiment_id = source.experiment_id
    JOIN experiments AS successor
      ON successor.experiment_id = '$successor_experiment_id'
     AND successor.campaign_id = campaign.campaign_id
     AND successor.parent_experiment_id = source.experiment_id
    JOIN experiment_metrics AS successor_metric
      ON successor_metric.experiment_id = successor.experiment_id
    WHERE campaign.campaign_id = '$CAMPAIGN_ID'
      AND campaign.project_id = '$PROJECT_ID'
      AND campaign.baseline_experiment_id = '$SOURCE_EXPERIMENT_ID'
      AND source.experiment_id = '$SOURCE_EXPERIMENT_ID'
      AND (SELECT COUNT(*)
           FROM experiments AS child
           WHERE child.campaign_id = '$CAMPAIGN_ID'
      AND child.parent_experiment_id = '$SOURCE_EXPERIMENT_ID') = 1
      AND source.status IN ('failed', 'cancelled')
      AND source_metric.primary_metric_name IS NULL
      AND source_metric.primary_metric_value IS NULL
      AND source_metric.artifact_defect = 'result_missing'
      AND source_metric.evaluated_at IS NOT NULL
      AND successor.status = 'succeeded'
      AND successor_metric.primary_metric_name = 'loss'
      AND successor_metric.primary_metric_value IS NOT NULL
      AND successor_metric.artifact_defect IS NULL
      AND successor_metric.evaluated_at IS NOT NULL
      AND campaign.current_best_experiment_id IS NULL
      AND campaign.plateau_count = 0" \
    "1" "successor evaluated without promotion" "30"
  [ "$(readonly_sql "SELECT COUNT(*) FROM events
      WHERE project_id = '$PROJECT_ID'
        AND campaign_id = '$CAMPAIGN_ID'
        AND kind = 'operator_wake'
        AND dedup_key = 'promotion:v1:$CAMPAIGN_ID:$successor_experiment_id'")" = 0 ] \
    || die "successor unexpectedly emitted a promotion event"
}

run_stop_and_next_case() {
  local main_before source_before successor_experiment_id
  local source_checkpoint_line source_checkpoint_path source_checkpoint_step source_checkpoint_weight
  local successor_checkpoint_line successor_checkpoint_path successor_checkpoint_step successor_checkpoint_weight
  setup_case
  write_research_scenario stop_and_next 33333333-3333-4333-a333-333333333333
  submit_source 360 1
  main_before="$(git -C "$PROJECT" rev-parse refs/heads/main)"
  source_before="$(git -C "$PROJECT" hash-object "$PROJECT/train.py")"
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  source_checkpoint_line="$(wait_for_checkpoint "$SOURCE_EXPERIMENT_ID" 2 120)"
  source_checkpoint_path="${source_checkpoint_line%%$'\t'*}"
  source_checkpoint_step="$(printf '%s' "$source_checkpoint_line" | cut -f2)"
  source_checkpoint_weight="$(printf '%s' "$source_checkpoint_line" | cut -f3)"
  [ "$source_checkpoint_step" -ge 2 ] || die "stop-and-next did not observe multi-step source progress before kill"
  record "STOP_SOURCE_CHECKPOINT path=$source_checkpoint_path step=$source_checkpoint_step weight=$source_checkpoint_weight digest=$(checkpoint_digest "$source_checkpoint_path")"
  wait_for_sql "SELECT operation_stage FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "intent" "stop intent before real kill" "30"
  [ "$(experiment_count)" = 1 ] || die "successor existed before confirmed kill"
  [ "$(pueue_group_count)" = 1 ] || die "Pueue add occurred before confirmed kill"
  "$REAL_JQ" -cn --arg invoked "$CONTROL/decision-entered" --arg release "$CONTROL/decision-release" \
    '{invoked_path:$invoked,release_path:$release}' > "$HOME/.pueue-agent/decision-control.json"
  chmod 600 "$HOME/.pueue-agent/decision-control.json"
  release_kill_and_restart
  await_stop_confirmed
  # The wrapper holds the fresh terminal decision before it can emit a
  # proposal, proving the externally observable confirmed boundary.
  wait_for_marker "$CONTROL/decision-entered" "stop-confirmed decision" 120
  record_barrier_pid_file "$HOME/../research-barrier-pids.log"
  [ "$(experiment_count)" = 1 ] || die "candidate admitted before terminal decision release"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'")" = 0 ] \
    || die "terminal decision cycle completed while held"
  release_marker "$CONTROL/decision-release"
  rm -f -- "$HOME/.pueue-agent/decision-control.json"
  wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "1" "one successor experiment" "240"
  successor_task="$(readonly_sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  register_task successor "$successor_task"
  successor_experiment_id="$(readonly_sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $successor_task")"
  wait_for_task_terminal "$successor_task" 180
  successor_checkpoint_line="$(wait_for_checkpoint "$successor_experiment_id" 1 30)"
  successor_checkpoint_path="${successor_checkpoint_line%%$'\t'*}"
  successor_checkpoint_step="$(printf '%s' "$successor_checkpoint_line" | cut -f2)"
  successor_checkpoint_weight="$(printf '%s' "$successor_checkpoint_line" | cut -f3)"
  case "$successor_checkpoint_path" in
    "$PROJECT/.pueue-agent/artifacts/$successor_experiment_id"/step-*.json) ;;
    *) die "stop-and-next successor wrote checkpoint outside its experiment namespace" ;;
  esac
  [ "$successor_checkpoint_step" -ge 1 ] || die "stop-and-next successor checkpoint did not show learning progress"
  record "STOP_SUCCESSOR_CHECKPOINT path=$successor_checkpoint_path step=$successor_checkpoint_step weight=$successor_checkpoint_weight digest=$(checkpoint_digest "$successor_checkpoint_path")"
  wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = (SELECT experiment_id FROM experiments WHERE pueue_task_id = $successor_task)" "1" "successor manifest metric" "120"
  wait_for_skipped_no_metric_successor "$successor_experiment_id"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'confirmed'")" = 1 ] \
    || die "stop-and-next did not confirm exactly one termination"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND source_experiment_id = '$SOURCE_EXPERIMENT_ID'")" = 1 ] \
    || die "stop-and-next did not attach one terminal cycle"
  assert_source_unchanged "$main_before" "$source_before"
  record "CASE stop_and_next PASS source_terminal=true source_checkpoint_step=$source_checkpoint_step confirmed_kills=1 cycles=1 successor=1 successor_checkpoint_step=$successor_checkpoint_step manifest_metric=true promotion=skipped_no_metric promoted_successor=false"
}

pueue_group_count() {
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" status --json \
    | "$REAL_JQ" -r --arg group "$GROUP" '[.tasks | to_entries[] | select(.value.group == $group)] | length'
}

run_checkpoint_case() {
  local checkpoint_line checkpoint_json checkpoint_root_path checkpoint_argv_path checkpoint_path
  local checkpoint_step checkpoint_weight checkpoint_digest_value checkpoint_expected_digest
  local checkpoint_inode loader_ref candidate_ref checkpoint_note retained_argv_json retained_path retained_digest
  local successor_experiment_id successor_task successor_parent successor_resume successor_argv
  local successor_line successor_path successor_loss successor_manifest_loss successor_digest source_terminal_status
  local main_before source_before
  setup_case
  write_research_scenario resume_from_checkpoint 44444444-4444-4444-a444-444444444444
  submit_source 240 1
  main_before="$(git -C "$PROJECT" rev-parse refs/heads/main)"
  source_before="$(git -C "$PROJECT" hash-object "$PROJECT/train.py")"
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state IN ('running','ready','completed')" "1" "checkpoint research review" "240"
  checkpoint_line="$(wait_for_checkpoint "$SOURCE_EXPERIMENT_ID" 1 120)"
  [ -n "$checkpoint_line" ] || die "checkpoint case did not publish a nonzero source checkpoint"
  wait_for_kill_barrier
  : > "$CONTROL/add-arm"
  release_marker "$CONTROL/kill-release"
  rm -f -- "$CONTROL/kill-target"
  await_stop_confirmed true
  wait_for_marker "$CONTROL/add-entered" "checkpoint successor pre-add" 120
  record_barrier_pid_file "$CONTROL/add-pids"
  wait_for_sql "SELECT status FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "submitting" "checkpoint successor reserved before add" "30"
  [ "$(pueue_group_count)" = 1 ] || die "checkpoint successor was added before the submitting barrier"
  checkpoint_json="$(readonly_sql "SELECT checkpoint_json FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
  [ -n "$checkpoint_json" ] || die "checkpoint answer did not retain prepared checkpoint JSON"
  checkpoint_root_path="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.source_checkpoint.root_relative_path')"
  checkpoint_argv_path="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.source_checkpoint.argv_path')"
  case "$checkpoint_root_path" in
    ".pueue-agent/artifacts/$SOURCE_EXPERIMENT_ID"/step-*.json) ;;
    *) die "checkpoint answer selected a path outside the source artifact namespace" ;;
  esac
  checkpoint_path="$PROJECT/$checkpoint_root_path"
  [ -f "$checkpoint_path" ] || die "prepared checkpoint path is not a real source artifact"
  checkpoint_step="$($REAL_JQ -er '.step' "$checkpoint_path")"
  checkpoint_weight="$($REAL_JQ -er '.weight' "$checkpoint_path")"
  [ "$checkpoint_step" -gt 0 ] || die "prepared checkpoint step is zero"
  checkpoint_digest_value="$(checkpoint_digest "$checkpoint_path")"
  checkpoint_expected_digest="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.source_checkpoint.sha256')"
  [ "$checkpoint_digest_value" = "$checkpoint_expected_digest" ] || die "prepared checkpoint digest was not independently verified"
  checkpoint_inode="$(stat -c '%i' "$checkpoint_path" 2>/dev/null || stat -f '%i' "$checkpoint_path")"
  retained_path="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.retained_checkpoint.relative_path')"
  case "$retained_path" in
    "research-checkpoints/$CAMPAIGN_ID/"*/checkpoint) ;;
    *) die "prepared checkpoint retained path is outside the campaign state namespace" ;;
  esac
  retained_path="$STATE_DIR/$retained_path"
  [ -f "$retained_path" ] || die "prepared checkpoint retained file is missing"
  retained_digest="$(checkpoint_digest "$retained_path")"
  [ "$retained_digest" = "$checkpoint_expected_digest" ] || die "retained checkpoint digest differs from source artifact"
  loader_ref="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.loader.reference')"
  candidate_ref="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -er '.source_checkpoint.reference')"
  printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -e --arg loader "$loader_ref" --arg candidate "$candidate_ref" \
    '.request.path == (.source_checkpoint.argv_path) and .request.working_directory == "." and .request.support_evidence_refs == [$loader, $candidate]' \
    >/dev/null || die "prepared checkpoint request/support references do not match its selected source"
  record "CHECKPOINT_SOURCE path=$checkpoint_path argv_path=$checkpoint_argv_path step=$checkpoint_step weight=$checkpoint_weight digest=$checkpoint_digest_value inode=$checkpoint_inode heldout_loss=$(heldout_loss "$checkpoint_path")"
  release_marker "$CONTROL/add-release"
  rm -f -- "$CONTROL/add-arm"
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "checkpoint answer" "360"

  successor_experiment_id="$(readonly_sql "SELECT successor_experiment_id FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
  [ -n "$successor_experiment_id" ] || die "checkpoint review did not persist a successor graph edge"
  successor_parent="$(readonly_sql "SELECT parent_experiment_id FROM experiments WHERE experiment_id = '$successor_experiment_id'")"
  successor_resume="$(readonly_sql "SELECT resume_of_experiment_id FROM experiments WHERE experiment_id = '$successor_experiment_id'")"
  [ "$successor_parent" = "$SOURCE_EXPERIMENT_ID" ] || die "checkpoint successor parent graph is incorrect"
  [ "$successor_resume" = "$SOURCE_EXPERIMENT_ID" ] || die "checkpoint successor resume graph is incorrect"
  checkpoint_note="$(readonly_sql "SELECT checkpoint_note FROM experiments WHERE experiment_id = '$successor_experiment_id'")"
  case "$checkpoint_note" in
    research-checkpoint:*) ;;
    *) die "checkpoint successor did not retain a prepared checkpoint note" ;;
  esac
  successor_task="$(readonly_sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$successor_experiment_id'")"
  register_task checkpoint-successor "$successor_task"
  successor_argv="$(readonly_sql "SELECT argv_json FROM submissions WHERE submission_id = (SELECT submission_id FROM experiments WHERE experiment_id = '$successor_experiment_id')")"
  retained_argv_json="$(printf '%s\n' "$checkpoint_json" | "$REAL_JQ" -cS '.retained_argv')"
  [ "$(printf '%s\n' "$successor_argv" | "$REAL_JQ" -cS '.')" = "$retained_argv_json" ] \
    || die "checkpoint successor argv did not retain the verified supervisor path and source argv"
  wait_for_task_terminal "$successor_task" 300
  wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$successor_experiment_id'" "1" "checkpoint successor metric" "120"
  successor_line="$(wait_for_checkpoint "$successor_experiment_id" "$checkpoint_step" 30)"
  successor_path="${successor_line%%$'\t'*}"
  case "$successor_path" in
    "$PROJECT/.pueue-agent/artifacts/$successor_experiment_id"/step-*.json) ;;
    *) die "checkpoint successor wrote outside its own artifact namespace" ;;
  esac
  successor_digest="$(checkpoint_digest "$successor_path")"
  successor_loss="$(heldout_loss "$successor_path")"
  successor_manifest_loss="$(result_manifest_loss "$successor_experiment_id")"
  assert_float_equal "$successor_manifest_loss" "$successor_loss" \
    || die "checkpoint successor manifest loss did not match independently recomputed final loss"
  "$REAL_PUEUE" --config "$PUEUE_CONFIG" log --full "$successor_task" > "$WORK/checkpoint-successor.log" 2>&1 \
    || die "checkpoint successor log could not be read"
  "$REAL_PYTHON" - "$checkpoint_digest_value" "$checkpoint_step" "$checkpoint_weight" "$WORK/checkpoint-successor.log" <<'PY'
import re
import sys
expected_digest, expected_step, expected_weight, log_path = sys.argv[1:]
text = open(log_path, encoding="utf-8").read()
match = re.search(r"resume-load checkpointdigest=([0-9a-f]+) step=(\d+) weight=([^\s]+)", text)
if match is None:
    raise SystemExit("successor did not emit resume-load evidence")
if match.group(1) != expected_digest or match.group(2) != expected_step:
    raise SystemExit("successor loaded checkpoint identity differs from the selected source")
if abs(float(match.group(3)) - float(expected_weight)) > 1e-12:
    raise SystemExit("successor loaded checkpoint weight differs from the selected source")
if "step=0 loss=" in text:
    raise SystemExit("successor emitted a cold-start step")
PY
  source_terminal_status="$(readonly_sql "SELECT status FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments AS source JOIN campaigns AS campaign ON campaign.campaign_id = source.campaign_id JOIN research_reviews AS review ON review.campaign_id = source.campaign_id AND review.experiment_id = source.experiment_id JOIN termination_requests AS request ON request.request_id = review.termination_request_id JOIN task_observations AS observation ON observation.project_id = campaign.project_id AND observation.pueue_task_id = source.pueue_task_id WHERE source.experiment_id = '$SOURCE_EXPERIMENT_ID' AND source.campaign_id = '$CAMPAIGN_ID' AND campaign.project_id = '$PROJECT_ID' AND source.status IN ('failed', 'cancelled') AND request.project_id = '$PROJECT_ID' AND request.status = 'confirmed' AND observation.ended_at IS NOT NULL AND (lower(observation.state) = 'killed' OR (lower(observation.state) = 'done' AND json_valid(observation.result) = 1 AND lower(json_extract(observation.result, '$')) = 'killed'))")" = 1 ] \
    || die "killed source did not settle with confirmed native kill evidence"
  [ "$(checkpoint_digest "$checkpoint_path")" = "$checkpoint_digest_value" ] \
    || die "source checkpoint bytes changed after resume"
  [ "$(stat -c '%i' "$checkpoint_path" 2>/dev/null || stat -f '%i' "$checkpoint_path")" = "$checkpoint_inode" ] \
    || die "source checkpoint inode changed after resume"
  wait_for_skipped_no_metric_successor "$successor_experiment_id"
  assert_source_unchanged "$main_before" "$source_before"
  record "CASE checkpoint PASS source_checkpoint_step=$checkpoint_step source_digest=$checkpoint_digest_value retained_digest=$retained_digest successor=$successor_experiment_id successor_step=$(printf '%s' "$successor_line" | cut -f2) successor_digest=$successor_digest successor_loss=$successor_loss manifest_loss=$successor_manifest_loss source_killed=true source_terminal_status=$source_terminal_status resume_load=true cold_start=false source_unchanged=true promotion=skipped_no_metric promoted_successor=false"
}

run_review_running_restart_case() {
  local invoked="$RESEARCH_CONTROL/research-invoked" release="$RESEARCH_CONTROL/research-release"
  local first_review_id first_agent_run_id first_planned_session first_generation
  local first_binding first_run_status second_session_id second_binding
  setup_case
  write_research_scenario continue 55555555-5555-4555-a555-555555555555 "$invoked" "$release"
  submit_source 240 1
  start_daemon
  wait_for_marker "$invoked" "review-running" 120
  record_barrier_pid_file "$RESEARCH_CONTROL/research-invoked"
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'running'" "1" "review-running durable owner" "3"
  wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID' AND status = 'running'" "1" "review-running agent row" "3"
  first_review_id="$(readonly_sql "SELECT review_id FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'running'")"
  first_agent_run_id="$(readonly_sql "SELECT agent_run_id FROM research_reviews WHERE review_id = '$first_review_id'")"
  first_planned_session="$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")"
  first_generation="$(readonly_sql "SELECT session_generation FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")"
  first_binding="$(readonly_sql "SELECT json_extract(notes_json, '$.session_binding') FROM research_reviews WHERE review_id = '$first_review_id'")"
  [ -n "$first_review_id" ] && [ -n "$first_agent_run_id" ] && [ -n "$first_planned_session" ] \
    || die "review-running boundary lost its pending research identity"
  [ "$first_generation" = 0 ] || die "review-running fresh launch changed generation before confirmation"
  [ "$first_binding" = pending ] || die "review-running boundary was not a pending session binding"
  crash_daemon_exact
  old_pid="$(sed -n '1p' "$RESEARCH_CONTROL/research-invoked")"
  require_owned_pid "$old_pid"
  terminate_exact_pid "$old_pid"
  rm -f -- "$invoked" "$release"
  write_research_scenario continue 66666666-6666-4666-a666-666666666666 "$RESEARCH_CONTROL/research-invoked-2" "$RESEARCH_CONTROL/research-release-2"
  start_daemon
  wait_for_marker "$RESEARCH_CONTROL/research-invoked-2" "review-running fresh generation" 120
  release_marker "$RESEARCH_CONTROL/research-release-2"
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "review-running recovery" "240"
  wait_for_sql "SELECT session_generation FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'" "0" "review-running session generation" "120"
  second_session_id="$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")"
  second_binding="$(readonly_sql "SELECT json_extract(notes_json, '$.session_binding') FROM research_reviews WHERE review_id = '$first_review_id'")"
  first_run_status="$(readonly_sql "SELECT status FROM agent_runs WHERE run_id = $first_agent_run_id")"
  [ "$second_session_id" = 66666666-6666-4666-a666-666666666666 ] \
    || die "review-running recovery did not confirm the replacement session"
  [ "$second_session_id" != "$first_planned_session" ] \
    || die "review-running recovery reused the crashed pending session identity"
  [ "$second_binding" = confirmed ] || die "review-running recovery did not persist confirmed replacement binding"
  case "$first_run_status" in
    failed|timed_out|cancelled) ;;
    *) die "review-running crashed run retained unexpected status: $first_run_status" ;;
  esac
  [ "$(grep -c '^RESEARCH_INVOCATION ' "$WORK/home/../research-codex-calls.log")" = 2 ] \
    || die "review-running recovery launched an unexpected number of research calls"
  [ "$(experiment_count)" = 1 ] && [ "$(submission_count)" = 1 ] \
    || die "review-running recovery created duplicate external experiment work"
  record "CASE restart_review_running PASS old_daemon_sigkill_reaped=true pending_session_retired=true replacement_session_distinct=true session_generation=0 first_run_status=$first_run_status research_invocations=2 external_work_unchanged=true"
}

run_answer_ready_case() {
  local status_pid ready_review_id proxy_review_id ready_proof_count
  setup_case
  write_research_scenario continue 66666666-6666-4666-a666-666666666666
  : > "$CONTROL/status-arm"
  submit_source 180 1
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_marker "$CONTROL/status-entered" "answer-ready status proxy" 240
  record_barrier_pid_file "$CONTROL/status-pids"
  status_pid="$(sed -n '1p' "$CONTROL/status-pids")"
  require_owned_pid "$status_pid"
  ready_review_id="$(readonly_sql "SELECT review_id FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'ready' AND operation_stage IS NULL AND termination_request_id IS NULL AND decision_cycle_id IS NULL AND successor_experiment_id IS NULL")"
  proxy_review_id="$(sed -n '1p' "$CONTROL/status-review-id")"
  [ -n "$ready_review_id" ] && [ "$ready_review_id" = "$proxy_review_id" ] \
    || die "answer-ready proxy did not hold the exact durable ready review"
  ready_proof_count="$(readonly_sql "SELECT COUNT(*) FROM research_reviews AS review JOIN agent_runs AS run ON run.run_id = review.agent_run_id WHERE review.review_id = '$ready_review_id' AND review.state = 'ready' AND review.operation_stage IS NULL AND review.termination_request_id IS NULL AND review.decision_cycle_id IS NULL AND review.successor_experiment_id IS NULL AND json_valid(review.notes_json) = 1 AND json_extract(review.notes_json, '$.native_recovery.cleanup.phase') = 'complete' AND run.status IN ('completed', 'failed', 'timed_out', 'cancelled') AND run.launch_gate_state IN ('released', 'failed')")"
  [ "$ready_proof_count" = 1 ] || die "answer-ready proxy lacked native cleanup proof"
  [ "$(experiment_count)" = 1 ] || die "answer-ready boundary admitted a successor"
  [ "$(submission_count)" = 1 ] || die "answer-ready boundary created a submission"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID'")" = 0 ] || die "answer-ready boundary requested a kill"
  crash_daemon_exact
  terminate_exact_pid "$status_pid"
  rm -f -- "$CONTROL/status-arm" "$CONTROL/status-release"
  start_daemon
  wait_for_sql "SELECT COUNT(*) FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "answer-ready restart recovery" "240"
  [ "$(grep -c '^RESEARCH_INVOCATION ' "$WORK/home/../research-codex-calls.log")" = 1 ] \
    || die "answer-ready restart launched a duplicate research call"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID'")" = 1 ] || die "answer-ready restart created a successor"
  record "CASE restart_answer_ready PASS exact_ready_review=$ready_review_id native_cleanup_complete=true daemon_sigkill_reaped=true duplicate_research=0 successor=0"
}

run_stop_pending_restart_case() {
  setup_case
  write_research_scenario stop_and_next 77777777-7777-4777-a777-777777777777
  submit_source 360 1
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  wait_for_sql "SELECT operation_stage FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "intent" "stop-pending restart intent" "30"
  old_proxy_pid="$(sed -n '1p' "$CONTROL/kill-pids")"
  require_owned_pid "$old_proxy_pid"
  crash_daemon_exact
  terminate_exact_pid "$old_proxy_pid"
  rm -f -- "$CONTROL/kill-target"
  start_daemon
  wait_for_task_terminal "$SOURCE_TASK_ID" 180
  wait_for_sql "SELECT status FROM termination_requests WHERE project_id = '$PROJECT_ID' ORDER BY request_id DESC LIMIT 1" "confirmed" "stop-pending restart confirmation" "240"
  record "CASE restart_stop_pending PASS predelegation_kill=false confirmed_after_restart=true"
}

run_stop_confirmed_restart_case() {
  local fresh_decision_pid successor_task successor_experiment_id decision_cycle_id decision_event_id
  local decision_event_key decision_pid_log="$HOME/../research-barrier-pids.log"
  local nonempty_temp_entry temp_root="$PROJECT/.pueue-agent/tmp"
  setup_case
  write_research_scenario stop_and_next 88888888-8888-4888-a888-888888888888
  submit_source 360 1
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  start_run_id_admission_lock
  release_kill_and_restart
  await_stop_confirmed
  decision_cycle_id="$(readonly_sql "SELECT decision_cycle_id FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' AND experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  [ -n "$decision_cycle_id" ] || die "stop-confirmed handoff did not bind its decision cycle"
  decision_event_key="campaign-decision:v1:$decision_cycle_id"
  decision_event_id="$(readonly_sql "SELECT event_id FROM events WHERE project_id = '$PROJECT_ID' AND kind = 'campaign_decision' AND dedup_key = '$decision_event_key'")"
  [ -n "$decision_event_id" ] || die "stop-confirmed handoff did not create its exact decision event"
  wait_for_sql "SELECT COUNT(*) FROM events AS event JOIN decision_cycles AS cycle ON cycle.cycle_id = '$decision_cycle_id' AND cycle.campaign_id = '$CAMPAIGN_ID' AND cycle.source_experiment_id = '$SOURCE_EXPERIMENT_ID' AND cycle.state = 'pending' WHERE event.project_id = '$PROJECT_ID' AND event.event_id = $decision_event_id AND event.kind = 'campaign_decision' AND event.dedup_key = '$decision_event_key' AND event.status = 'retry_wait' AND event.attempts = 0 AND event.not_before > CAST(strftime('%s','now') AS INTEGER)" "1" "future decision retry deferred by held run-ID admission lock" "240"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = '$decision_cycle_id'")" = 0 ] \
    || die "stop-confirmed boundary reserved a decision attempt before the crash"
  [ "$(readonly_sql "SELECT COUNT(*) FROM budget_reservations WHERE campaign_id = '$CAMPAIGN_ID' AND dimension = 'agent_run' AND subject_key LIKE 'campaign-decision-attempt:v1:$decision_cycle_id:%'")" = 0 ] \
    || die "stop-confirmed boundary reserved generic decision agent-run budget before the crash"
  [ "$(readonly_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID' AND primary_event_id = $decision_event_id")" = 0 ] \
    || die "stop-confirmed boundary started a generic decision agent before the crash"
  if [ -d "$temp_root" ]; then
    nonempty_temp_entry="$(find "$temp_root" -mindepth 2 -maxdepth 2 -print -quit)" \
      || die "could not inspect the private run-temp generations while admission was held"
  else
    nonempty_temp_entry=""
  fi
  [ -z "$nonempty_temp_entry" ] \
    || die "stop-confirmed boundary populated a private run-temp generation: $nonempty_temp_entry"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'confirmed'")" = 1 ] \
    || die "restart stop-confirmed duplicated kill"
  [ "$(experiment_count)" = 1 ] || die "restart stop-confirmed admitted successor too early"
  [ "$(pueue_task_status "$SOURCE_TASK_ID")" != __missing__ ] || die "stop-confirmed source task disappeared"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_cycles WHERE cycle_id = '$decision_cycle_id' AND state = 'completed'")" = 0 ] \
    || die "stop-confirmed decision completed before its crash boundary"
  pid_is_alive "$RUN_ID_LOCK_PID" \
    || die "run-ID admission lock owner exited before the confirmed handoff crash"

  # Crash after the durable confirmed research handoff but before generic
  # decision admission.  The run-ID guard blocks temp preflight and spawn;
  # startup must then see the empty temp root and admit one fresh decision.
  crash_daemon_exact
  release_run_id_admission_lock
  : > "$decision_pid_log"
  "$REAL_JQ" -cn --arg invoked "$CONTROL/decision-entered-2" --arg release "$CONTROL/decision-release-2" \
    '{invoked_path:$invoked,release_path:$release}' > "$HOME/.pueue-agent/decision-control.json"
  chmod 600 "$HOME/.pueue-agent/decision-control.json"
  start_daemon
  wait_for_marker "$CONTROL/decision-entered-2" "stop-confirmed fresh decision generation" 720
  record_barrier_pid_file "$decision_pid_log"
  fresh_decision_pid="$(sed -n '1p' "$decision_pid_log")"
  require_owned_pid "$fresh_decision_pid"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'confirmed'")" = 1 ] \
    || die "stop-confirmed restart duplicated the confirmed termination"
  [ "$(experiment_count)" = 1 ] || die "stop-confirmed restart admitted successor before fresh decision release"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'")" = 0 ] \
    || die "fresh stop-confirmed decision completed before release"
  release_marker "$CONTROL/decision-release-2"
  rm -f -- "$HOME/.pueue-agent/decision-control.json"
  wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "stop-confirmed restart cycle" "240"
  wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "1" "stop-confirmed restart successor" "240"
  successor_task="$(readonly_sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  register_task successor "$successor_task"
  successor_experiment_id="$(readonly_sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $successor_task")"
  wait_for_task_terminal "$successor_task" 180
  wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$successor_experiment_id'" "1" "stop-confirmed restart successor metric" "120"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'confirmed'")" = 1 ] \
    || die "stop-confirmed restart ended with duplicate confirmed termination"
  record "CASE restart_stop_confirmed PASS crash_after_confirmed_handoff_before_decision=true run_id_lock_deferred_event=true fresh_barrier_generation=true one_cycle=true one_confirmed_kill=true successor=1 successor_metric=true"
}

run_successor_submitting_restart_case() {
  local group_before submission_before post_crash_status
  local baseline_submission_id successor_experiment successor_submission_id successor_argv_before
  local successor_submission_status_before successor_submission_status_after
  setup_case
  write_research_scenario stop_and_next 99999999-9999-4999-a999-999999999999
  submit_source 360 1
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  "$REAL_JQ" -cn --arg invoked "$CONTROL/decision-entered" --arg release "$CONTROL/decision-release" \
    '{invoked_path:$invoked,release_path:$release}' > "$HOME/.pueue-agent/decision-control.json"
  chmod 600 "$HOME/.pueue-agent/decision-control.json"
  release_kill_and_restart
  await_stop_confirmed
  wait_for_marker "$CONTROL/decision-entered" "successor-submitting decision" 120
  : > "$CONTROL/add-arm"
  release_marker "$CONTROL/decision-release"
  rm -f -- "$HOME/.pueue-agent/decision-control.json"
  wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "successor-submitting decision" "120"
  wait_for_marker "$CONTROL/add-entered" "successor-submitting pre-add" 120
  record_barrier_pid_file "$CONTROL/add-pids"
  wait_for_sql "SELECT status FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "submitting" "reserved successor before add" "30"
  successor_experiment="$(readonly_sql "SELECT experiment_id FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  [ -n "$successor_experiment" ] || die "successor-submitting case lost reserved successor identity"
  baseline_submission_id="$(readonly_sql "SELECT submission_id FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  successor_submission_id="$(readonly_sql "SELECT submission_id FROM experiments WHERE experiment_id = '$successor_experiment'")"
  [ -n "$baseline_submission_id" ] && [ -n "$successor_submission_id" ] \
    || die "successor-submitting case lost durable submission identities"
  [ "$baseline_submission_id" != "$successor_submission_id" ] \
    || die "successor-submitting case reused the baseline submission identity"
  successor_submission_status_before="$(readonly_sql "SELECT status FROM submissions WHERE submission_id = '$successor_submission_id'")"
  [ "$successor_submission_status_before" = pending ] \
    || die "successor-submitting submission was not pending before external add: $successor_submission_status_before"
  successor_argv_before="$(readonly_sql "SELECT argv_json FROM submissions WHERE submission_id = '$successor_submission_id'")"
  [ -n "$successor_argv_before" ] || die "successor-submitting submission argv was not durable"
  assert_add_argv_for_experiment "$successor_experiment"
  group_before="$(pueue_group_count)"
  submission_before="$(submission_count)"
  [ "$group_before" = 1 ] || die "predelegation add created a Pueue task"
  [ "$submission_before" = 2 ] || die "predelegation add did not retain baseline plus successor submissions"
  old_proxy_pid="$(sed -n '1p' "$CONTROL/add-pids")"
  require_owned_pid "$old_proxy_pid"
  crash_daemon_exact
  terminate_exact_pid "$old_proxy_pid"
  rm -f -- "$CONTROL/add-arm"
  start_daemon
  wait_for_sql "SELECT status FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "unreconciled" "successor conservative crash recovery" "180"
  post_crash_status="$(readonly_sql "SELECT status FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  sleep 5
  [ "$(pueue_group_count)" = "$group_before" ] || die "predelegation crash recovery blindly re-added successor"
  [ "$(submission_count)" = "$submission_before" ] || die "predelegation crash recovery created a submission"
  [ "$(readonly_sql "SELECT submission_id FROM experiments WHERE experiment_id = '$SOURCE_EXPERIMENT_ID'")" = "$baseline_submission_id" ] \
    || die "predelegation crash recovery changed the baseline submission identity"
  [ "$(readonly_sql "SELECT submission_id FROM experiments WHERE experiment_id = '$successor_experiment'")" = "$successor_submission_id" ] \
    || die "predelegation crash recovery changed the successor submission identity"
  [ "$(readonly_sql "SELECT argv_json FROM submissions WHERE submission_id = '$successor_submission_id'")" = "$successor_argv_before" ] \
    || die "predelegation crash recovery changed the successor submission argv"
  successor_submission_status_after="$(readonly_sql "SELECT status FROM submissions WHERE submission_id = '$successor_submission_id'")"
  case "$successor_submission_status_after" in
    pending|unreconciled) ;;
    *) die "predelegation crash recovery produced unexpected successor submission status: $successor_submission_status_after" ;;
  esac
  [ "$(grep -c '^ADD_BEGIN$' "$WORK/pueue-add-argv.log")" = 1 ] || die "predelegation crash recovery attempted a second external add"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")" = 1 ] \
    || die "predelegation crash recovery duplicated successor reservation"
  record "CASE restart_successor_submitting PASS add_delegated=false successor_experiment=$successor_experiment successor_submission=$successor_submission_id pre_add_status=$successor_submission_status_before post_crash=$post_crash_status post_crash_submission_status=$successor_submission_status_after pueue_tasks_unchanged=true submissions_count=2 submissions_unchanged=true"
}

run_add_reconcile_case() {
  local kill_proxy_pid add_proxy_pid successor_task successor_experiment
  local status_before status_after status_after_suppressed successor_task_before
  local successor_observed_before_restart successor_observed_first
  setup_case
  write_research_scenario stop_and_next bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb
  submit_source 240 1
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  kill_proxy_pid="$(sed -n '1p' "$CONTROL/kill-pids")"
  [ "$kill_proxy_pid" -gt 1 ] || die "real kill barrier did not publish an owned PID"
  # Arm the terminal decision before releasing the real kill.  Confirmation
  # and decision scheduling can occur in one daemon tick, so arming afterward
  # would leave the delegated-add ambiguity boundary racy.
  "$REAL_JQ" -cn --arg invoked "$CONTROL/decision-entered" --arg release "$CONTROL/decision-release" \
    '{invoked_path:$invoked,release_path:$release}' > "$HOME/.pueue-agent/decision-control.json"
  chmod 600 "$HOME/.pueue-agent/decision-control.json"
  release_marker "$CONTROL/kill-release"
  rm -f -- "$CONTROL/kill-target"
  await_stop_confirmed

  wait_for_marker "$CONTROL/decision-entered" "add-reconcile decision" 120
  : > "$CONTROL/add-arm"
  : > "$CONTROL/add-suppress-result"
  release_marker "$CONTROL/decision-release"
  rm -f -- "$HOME/.pueue-agent/decision-control.json"
  wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID' AND state = 'completed'" "1" "add-reconcile decision" "120"
  wait_for_marker "$CONTROL/add-entered" "real add before result suppression" 120
  record_barrier_pid_file "$CONTROL/add-pids"
  add_proxy_pid="$(sed -n '1p' "$CONTROL/add-pids")"
  [ "$add_proxy_pid" -gt 1 ] || die "real add barrier did not publish an owned PID"
  # Resolve the one reserved successor while the external add is still held.
  # Its task ID must remain NULL after production receives the suppressed
  # result; production must retain the conservative unknown-add state.
  successor_experiment="$(readonly_sql "SELECT experiment_id FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")"
  [ -n "$successor_experiment" ] || die "result-suppressed add lost the reserved successor identity before delegation"
  successor_task_before="$(readonly_sql "SELECT COALESCE(pueue_task_id, '') FROM experiments WHERE experiment_id = '$successor_experiment'")"
  [ -z "$successor_task_before" ] || die "reserved successor was bound before the real add"
  status_before="$(readonly_sql "SELECT status FROM experiments WHERE experiment_id = '$successor_experiment'")"
  [ "$status_before" = submitting ] || die "reserved successor was not held in submitting before the real add: $status_before"
  assert_add_argv_for_experiment "$successor_experiment"
  release_marker "$CONTROL/add-release"
  wait_for_sql "SELECT status FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'" "unreconciled" "result-suppressed add reconciliation marker" "120"
  status_after_suppressed="$(readonly_sql "SELECT status FROM experiments WHERE experiment_id = '$successor_experiment'")"
  [ "$status_after_suppressed" = unreconciled ] || die "result-suppressed add did not retain conservative unreconciled status"
  [ "$(pueue_group_count)" = 2 ] || die "result-suppressed add did not create exactly one real Pueue task"
  successor_task="$($REAL_PUEUE --config "$PUEUE_CONFIG" status --json \
    | "$REAL_JQ" -er --arg group "$GROUP" --arg source "$SOURCE_TASK_ID" \
      '[.tasks | to_entries[] | select(.value.group == $group and .key != $source) | .key] | if length == 1 then .[0] else error("expected exactly one external successor task") end')"
  register_task successor "$successor_task"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")" = 1 ] \
    || die "result-suppressed add did not retain exactly one successor experiment"
  [ "$(grep -c '^ADD_BEGIN$' "$WORK/pueue-add-argv.log")" = 1 ] \
    || die "result-suppressed add did not capture exactly one external add"
  rm -f -- "$CONTROL/add-arm" "$CONTROL/add-suppress-result"
  crash_daemon_exact
  terminate_exact_pid "$add_proxy_pid"
  successor_observed_before_restart="$(readonly_sql "SELECT COALESCE(MAX(observed_at), -1) FROM task_observations WHERE project_id = '$PROJECT_ID' AND pueue_task_id = $successor_task AND pueue_group = '$GROUP'")"
  [ "$successor_observed_before_restart" -ge -1 ] || die "result-suppressed add lacked an observation timestamp floor"
  start_daemon
  wait_for_sql "SELECT CASE WHEN COALESCE(MAX(observed_at), -1) > $successor_observed_before_restart THEN 1 ELSE 0 END FROM task_observations WHERE project_id = '$PROJECT_ID' AND pueue_task_id = $successor_task AND pueue_group = '$GROUP'" "1" "result-suppressed first post-restart observation" "240"
  successor_observed_first="$(readonly_sql "SELECT COALESCE(MAX(observed_at), -1) FROM task_observations WHERE project_id = '$PROJECT_ID' AND pueue_task_id = $successor_task AND pueue_group = '$GROUP'")"
  [ "$successor_observed_first" -gt "$successor_observed_before_restart" ] || die "result-suppressed first post-restart observation did not advance"
  wait_for_task_terminal "$successor_task" 180
  wait_for_sql "SELECT CASE WHEN COALESCE(MAX(observed_at), -1) > $successor_observed_first THEN 1 ELSE 0 END FROM task_observations WHERE project_id = '$PROJECT_ID' AND pueue_task_id = $successor_task AND pueue_group = '$GROUP'" "1" "result-suppressed completed post-restart reconciliation pass" "240"
  [ "$(readonly_sql "SELECT COUNT(*) FROM task_observations WHERE project_id = '$PROJECT_ID' AND pueue_task_id = $successor_task AND pueue_group = '$GROUP' AND ended_at IS NOT NULL AND lower(state) = 'done' AND json_valid(result) = 1 AND lower(json_extract(result, '$')) = 'success'")" = 1 ] \
    || die "result-suppressed add lacked exact terminal external task observation"
  status_after="$(readonly_sql "SELECT status FROM experiments WHERE experiment_id = '$successor_experiment'")"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments AS experiment JOIN submissions AS submission USING (submission_id) WHERE experiment.experiment_id = '$successor_experiment' AND experiment.status = 'unreconciled' AND experiment.failure_code = 'pueue_add_unknown' AND experiment.pueue_task_id IS NULL AND experiment.task_signature IS NULL AND experiment.finished_at IS NULL AND submission.status = 'unreconciled' AND submission.pueue_task_id IS NULL AND submission.task_signature IS NULL")" = 1 ] \
    || die "result-suppressed add did not retain durable managed unreconciled state"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$successor_experiment'")" = 0 ] \
    || die "result-suppressed unbound external result was ingested as a successor metric"
  [ "$(readonly_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_ID' AND parent_experiment_id = '$SOURCE_EXPERIMENT_ID'")" = 1 ] \
    || die "result-suppressed recovery duplicated the successor experiment"
  [ "$(experiment_count)" = 2 ] || die "result-suppressed recovery changed experiment cardinality"
  [ "$(pueue_group_count)" = 2 ] || die "result-suppressed recovery re-added the real successor"
  [ "$(submission_count)" = 2 ] || die "result-suppressed recovery created a duplicate submission"
  [ "$(grep -c '^ADD_BEGIN$' "$WORK/pueue-add-argv.log")" = 1 ] \
    || die "result-suppressed recovery attempted a second external add"
  record "CASE add_reconcile PASS real_add=true result_suppressed=true pre_delegation_status=$status_before post_add_status=$status_after_suppressed post_restart_status=$status_after reconciliation_required=true exact_external_task=$successor_task external_task_terminal=true external_task_observed_after_restart=true successor_metric=false duplicate_add=false experiments=2 submissions=2 add_begin=1"
}

run_failure_case() {
  local failure_mode="$1"
  local source_steps=180
  AGENT_TIMEOUT_MINUTES=5
  if [ "$failure_mode" = timeout ]; then
    AGENT_TIMEOUT_MINUTES=1
    source_steps=240
  fi
  setup_case
  write_research_scenario continue aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa
  printf '%s\n' "$failure_mode" > "$HOME/.pueue-agent/research-failure-mode"
  chmod 600 "$HOME/.pueue-agent/research-failure-mode"
  submit_source "$source_steps" 1
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  if [ "$failure_mode" = timeout ]; then
    wait_for_sql "SELECT failure_code FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "research_timeout" "research timeout failure code" "240"
    wait_for_sql_any "SELECT state FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "retry_wait,blocked,discarded" "research timeout settlement" "30"
  else
    wait_for_sql_any "SELECT state FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "retry_wait,blocked,discarded" "research failure settlement $failure_mode" "300"
  fi
  [ "$(experiment_count)" = 1 ] || die "$failure_mode failure created successor"
  [ "$(submission_count)" = 1 ] || die "$failure_mode failure created extra submission"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID'")" = 0 ] || die "$failure_mode failure requested kill"
  wait_for_task_state "$SOURCE_TASK_ID" Running 30
  record "CASE failure_$failure_mode PASS learner_running=true successor=0 termination_requests=0 failure_code=$(readonly_sql "SELECT failure_code FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1")"
}

run_unsafe_session_case() {
  local session_id=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa
  local session_path invocation_count unsafe_mode
  setup_case
  write_research_scenario continue "$session_id"
  submit_source 240 1
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  await_first_review
  session_path="$(session_file_for "$session_id")"
  [ -n "$session_path" ] || die "unsafe-session case did not establish an owned session"
  chmod 000 "$session_path"
  unsafe_mode="$(stat -c '%a' "$session_path" 2>/dev/null || stat -f '%Lp' "$session_path")"
  case "$unsafe_mode" in
    0|000) ;;
    *) die "unsafe-session fixture did not make only the owned artifact unreadable: mode=$unsafe_mode" ;;
  esac
  write_research_scenario continue "$session_id"
  wait_for_sql "SELECT state FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at DESC, review_id DESC LIMIT 1" "blocked" "unsafe-session block" "420"
  wait_for_sql "SELECT failure_code FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at DESC, review_id DESC LIMIT 1" "research_session_unsafe" "unsafe-session failure code" "30"
  invocation_count="$(grep -c '^RESEARCH_INVOCATION ' "$WORK/home/../research-codex-calls.log")"
  [ "$invocation_count" = 1 ] || die "unsafe owned session launched a fresh or second Codex run"
  [ "$(readonly_sql "SELECT session_id FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")" = "$session_id" ] \
    || die "unsafe owned session changed campaign identity"
  [ "$(readonly_sql "SELECT session_generation FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID'")" = 0 ] \
    || die "unsafe owned session created a fresh generation"
  [ "$(experiment_count)" = 1 ] || die "unsafe session created successor"
  [ "$(readonly_sql "SELECT COUNT(*) FROM campaign_research WHERE campaign_id = '$CAMPAIGN_ID' AND session_id IS NOT NULL")" = 1 ] || die "unsafe session lost campaign owner"
  record "CASE failure_unsafe_session PASS owned_session=$session_id mode=$unsafe_mode unreadable=true fresh_fallback=false invocation_count=1 successor=0"
}

run_unknown_kill_case() {
  local kill_proxy_pid
  setup_case
  write_research_scenario stop_and_next bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb
  submit_source 240 1
  arm_kill_barrier
  start_daemon
  wait_for_task_state "$SOURCE_TASK_ID" Running 120
  wait_for_kill_barrier
  wait_for_sql "SELECT operation_stage FROM research_reviews WHERE campaign_id = '$CAMPAIGN_ID' ORDER BY created_at, review_id LIMIT 1" "intent" "unknown-kill stop intent" "30"
  record_barrier_pid_file "$CONTROL/kill-pids"
  kill_proxy_pid="$(sed -n '1p' "$CONTROL/kill-pids")"
  require_owned_pid "$kill_proxy_pid"
  wait_for_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'failed' AND last_error LIKE '%timed out%'" "1" "unconfirmed kill timeout" "60"
  wait_for_task_state "$SOURCE_TASK_ID" Running 30
  [ "$(experiment_count)" = 1 ] || die "failed kill created successor"
  [ "$(submission_count)" = 1 ] || die "failed kill created an extra submission"
  [ "$(readonly_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_ID'")" = 0 ] || die "failed kill attached a decision cycle"
  [ "$(readonly_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID' AND status = 'confirmed'")" = 0 ] || die "failed kill was incorrectly confirmed"
  record "CASE failure_unknown_kill PASS unconfirmed_kill=true request_failed=true timeout_error=true learner_running=true successor=0 cycle=0"
}

record "CASE_BEGIN $case_name"
case "$case_name" in
  continue) run_continue_case ;;
  stop_and_next) run_stop_and_next_case ;;
  checkpoint) run_checkpoint_case ;;
  missing_session) run_missing_session_case ;;
  restart_review_running) run_review_running_restart_case ;;
  restart_answer_ready) run_answer_ready_case ;;
  restart_stop_pending) run_stop_pending_restart_case ;;
  restart_stop_confirmed) run_stop_confirmed_restart_case ;;
  restart_successor_submitting) run_successor_submitting_restart_case ;;
  add_reconcile) run_add_reconcile_case ;;
  failure_malformed) run_failure_case malformed ;;
  failure_timeout) run_failure_case timeout ;;
  failure_cap) run_failure_case cap ;;
  failure_unsafe_session) run_unsafe_session_case ;;
  failure_unknown_kill) run_unknown_kill_case ;;
esac
record "CASE_END $case_name"
