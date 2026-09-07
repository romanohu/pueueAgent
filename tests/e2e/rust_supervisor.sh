#!/usr/bin/env bash
set -eu

if [ "$(uname -s)" != "Linux" ]; then
  echo "Rust E2E FAIL: real-Pueue campaign acceptance requires Linux" >&2
  exit 1
fi
umask 077

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PA_BIN="$REPO_ROOT/bin/pueue-agent"
REAL_PUEUE="$(command -v pueue)"
REAL_PUEUED="$(command -v pueued)"
REAL_GIT="$(command -v git || true)"
REAL_PYTHON="$(command -v python3 || command -v python || true)"
ORIGINAL_HOME="${HOME:?HOME is required}"
CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
case "$CARGO_TARGET_DIR" in
  /*) : ;;
  *) CARGO_TARGET_DIR="$(pwd)/$CARGO_TARGET_DIR" ;;
esac
export CARGO_TARGET_DIR
WORK="$(mktemp -d /tmp/pa-rust-e2e.XXXXXX)"
XDG_RUNTIME_DIR="$WORK/runtime"
DAEMON_PID=""
PUEUED_PID=""

fail() {
  echo "Rust E2E FAIL: $*" >&2
  exit 1
}

source "$REPO_ROOT/tests/support/agent_call_count.sh"

cleanup() {
  if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  "$REAL_PUEUE" --config "$WORK/pueue.yml" shutdown >/dev/null 2>&1 || true
  if [ -z "$PUEUED_PID" ] && [ -f "$XDG_RUNTIME_DIR/pueue.pid" ]; then
    PUEUED_PID="$(cat "$XDG_RUNTIME_DIR/pueue.pid" 2>/dev/null || true)"
  fi
  case "$PUEUED_PID" in
    ''|*[!0-9]*) ;;
    *)
      for _ in $(seq 50); do
        kill -0 "$PUEUED_PID" 2>/dev/null || break
        sleep 0.1
      done
      if kill -0 "$PUEUED_PID" 2>/dev/null; then
        kill -TERM "$PUEUED_PID" 2>/dev/null || true
        for _ in $(seq 50); do
          kill -0 "$PUEUED_PID" 2>/dev/null || break
          sleep 0.1
        done
      fi
      if kill -0 "$PUEUED_PID" 2>/dev/null; then
        kill -KILL "$PUEUED_PID" 2>/dev/null || true
      fi
      ;;
  esac
  rm -rf "$WORK"
}
trap cleanup EXIT

sql() {
  sqlite3 -cmd '.timeout 5000' "$STATE_DB" "$1"
}

toml_value() {
  key="$1"
  path="$2"
  awk -F ' = ' -v key="$key" '$1 == key { gsub(/"/, "", $2); print $2; exit }' "$path"
}

wait_for_sql() {
  query="$1"
  expected="$2"
  label="$3"
  # Decision cycles advance across multiple production daemon passes (the
  # foreground daemon defaults to a 60-second interval). Decisions observed
  # beside failure events additionally defer by one whole lease window, so
  # this waiter must span the deferral plus spawn/reap/apply passes. Budget
  # wall-clock seconds rather than iterations: loaded hosts stretch every
  # polling query.
  local deadline=$(( $(date +%s) + 900 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    actual="$(sql "$query")"
    if [ "$actual" = "$expected" ]; then
      return 0
    fi
    if [ -n "$DAEMON_PID" ] && ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      wait "$DAEMON_PID" || true
      cat "$WORK/daemon.log" >&2
      fail "$label (daemon exited early; see $WORK/daemon.log)"
    fi
    sleep 0.1
  done
  "$REAL_PUEUE" --config "$WORK/pueue.yml" status --json >&2 || true
  sql "SELECT event_id, project_id, kind, status, not_before FROM events ORDER BY event_id" >&2 || true
  fail "$label (expected $(printf '%q' "$expected"), got $(printf '%q' "$(sql "$query")"))"
}

wait_for_agent_calls() {
  project_id="$1"
  expected="$2"
  label="$3"
  local deadline=$(( $(date +%s) + 600 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    actual="$(agent_call_count_for_project "$project_id")"
    if [ "$actual" = "$expected" ]; then
      return 0
    fi
    if [ -n "$DAEMON_PID" ] && ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      wait "$DAEMON_PID" || true
      fail "$label (daemon exited early; see $WORK/daemon.log)"
    fi
    sleep 0.1
  done
  fail "$label (expected $expected agent calls, got $actual)"
}

wait_for_codex_call() {
  label="$1"
  local deadline=$(( $(date +%s) + 300 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ -s "$PUEUE_AGENT_TEST_CODEX_LOG" ]; then
      return 0
    fi
    if [ -n "$DAEMON_PID" ] && ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      wait "$DAEMON_PID" || true
      cat "$WORK/daemon.log" >&2
      fail "$label (daemon exited early; see $WORK/daemon.log)"
    fi
    sleep 0.1
  done
  fail "$label (Codex process was not invoked)"
}

start_daemon() {
  : > "$WORK/daemon.log"
  "$PA_BIN" daemon --foreground --pueue-config "$WORK/pueue.yml" \
    > "$WORK/daemon.log" 2>&1 &
  DAEMON_PID=$!
}

stop_daemon() {
  [ -n "$DAEMON_PID" ] || return 0
  if kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill -TERM "$DAEMON_PID"
  fi
  daemon_status=0
  wait "$DAEMON_PID" || daemon_status=$?
  if [ "$daemon_status" -ne 0 ] && [ "$daemon_status" -ne 143 ]; then
    cat "$WORK/daemon.log" >&2
    if [ -f "${STATE_DB:-}" ]; then
      sqlite3 "$STATE_DB" \
        "SELECT event_id, project_id, kind, status, dedup_key, payload_json, last_error FROM events ORDER BY event_id" >&2 || true
      sqlite3 "$STATE_DB" \
        "SELECT run_id, status, failure_stage, policy_code FROM agent_runs ORDER BY run_id" >&2 || true
    fi
    fail "daemon shutdown failed; see $WORK/daemon.log"
  fi
  DAEMON_PID=""
}

wait_for_task_state() {
  task_id="$1"
  wanted="$2"
  local deadline=$(( $(date +%s) + 90 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$($REAL_PUEUE --config "$WORK/pueue.yml" status --json \
      | jq -r --arg id "$task_id" '.tasks[$id].status | keys[0]' 2>/dev/null || true)"
    if [ "$state" = "$wanted" ]; then
      return 0
    fi
    sleep 0.1
  done
  "$REAL_PUEUE" --config "$WORK/pueue.yml" status --json >&2 || true
  fail "task $task_id did not reach $wanted"
}

wait_for_task_terminal() {
  task_id="$1"
  local deadline=$(( $(date +%s) + 180 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    state="$($REAL_PUEUE --config "$WORK/pueue.yml" status --json \
      | jq -r --arg id "$task_id" '.tasks[$id].status | keys[0]' 2>/dev/null || true)"
    if [ "$state" != "Running" ] && [ "$state" != "Queued" ] && [ "$state" != "Stashed" ] && [ "$state" != "null" ]; then
      return 0
    fi
    sleep 0.1
  done
  fail "task $task_id did not become terminal"
}

pueue_group_task_count() {
  group="$1"
  "$REAL_PUEUE" --config "$WORK/pueue.yml" status --json \
    | jq -r --arg group "$group" \
      '[.tasks | to_entries[] | select(.value.group == $group)] | length'
}

pueue_add_call_count() {
  if [ ! -f "$WORK/pueue-add-argv.log" ]; then
    printf '%s\n' 0
    return
  fi
  grep -c '^ADD_BEGIN$' "$WORK/pueue-add-argv.log" || true
}

record_task_id() {
  role="$1"
  task_id="$2"
  case "$task_id" in
    ''|*[!0-9]*) fail "$role did not resolve to one numeric task ID" ;;
  esac
  printf 'TASK_ID role=%s id=%s\n' "$role" "$task_id" >> "$WORK/task-ids.log"
}

decision_call_count() {
  source_experiment_id="$1"
  if [ ! -f "$PUEUE_AGENT_TEST_CODEX_LOG" ]; then
    printf '%s\n' 0
    return
  fi
  grep -Fc "DECISION_INVOCATION source_experiment_id=$source_experiment_id " \
    "$PUEUE_AGENT_TEST_CODEX_LOG" || true
}

git_ref_sha() {
  repo="$1"
  reference="$2"
  git -C "$repo" rev-parse --verify "$reference" 2>/dev/null || true
}

git_diff_digest() {
  repo="$1"
  base_sha="$2"
  candidate_sha="$3"
  diff_path="$WORK/code-change-diff-$candidate_sha"
  git -C "$repo" diff-tree --binary "$base_sha" "$candidate_sha" > "$diff_path"
  "$REAL_PYTHON" -c 'import hashlib, pathlib, sys; print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())' "$diff_path"
}

assert_ml_original_unchanged() {
  [ "$(git -C "$PROJECT_M" rev-parse refs/heads/main)" = "$ML_MAIN_SHA" ] \
    || fail "code-change scenario changed original main SHA"
  [ "$(git -C "$PROJECT_M" hash-object "$PROJECT_M/model.py")" = "$ML_FILE_DIGEST" ] \
    || fail "code-change scenario changed original model.py"
  [ "$(git -C "$PROJECT_M" config --local --get-regexp '^remote\.' || true)" = "$ML_REMOTE_CONFIG" ] \
    || fail "code-change scenario changed remote configuration"
  [ "$(git -C "$PROJECT_M" worktree list --porcelain)" = "$ML_WORKTREES_BEFORE" ] \
    || fail "code-change scenario changed unrelated worktree registrations"
}

insert_campaign_boundary() {
  project_id="$1"
  campaign_id="$2"
  experiment_status="$3"
  now="$(date +%s)"
  window_ends_at=$((now + 86400))
  proposal_id="$campaign_id-proposal"
  experiment_id="$campaign_id-experiment"
  submission_id="$campaign_id-submission"
  argv_json="[\"/bin/sh\",\"$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh\"]"

  sql "INSERT INTO campaigns (
         campaign_id, project_id, objective_text, objective_digest, initial_argv_json,
         state, baseline_experiment_id, created_at, updated_at
       ) VALUES (
         '$campaign_id', '$project_id', 'Reach validation loss below 0.20',
         '$campaign_id-objective-digest', '$argv_json', 'active', NULL, $now, $now
       );
       INSERT INTO proposals (
         proposal_id, campaign_id, kind, status, hypothesis, argv_json, working_directory,
         expected_evidence_json, canonical_digest, created_at, updated_at
       ) VALUES (
         '$proposal_id', '$campaign_id', 'experiment', 'accepted',
         'Establish the initial campaign baseline', '$argv_json', '.', '[]',
         '$campaign_id-canonical-digest', $now, $now
       );
       INSERT INTO submissions (
         submission_id, project_id, argv_json, created_at, status, kind, metadata_json
       ) VALUES (
         '$submission_id', '$project_id', '$argv_json', $now, 'pending', 'experiment', '{}'
       );
       INSERT INTO experiments (
         experiment_id, campaign_id, proposal_id, submission_id, attempt, status,
         created_at, updated_at
       ) VALUES (
         '$experiment_id', '$campaign_id', '$proposal_id', '$submission_id', 0,
         '$experiment_status', $now, $now
       );
       INSERT INTO budget_reservations (
         reservation_id, campaign_id, experiment_id, dimension, subject_key, status,
         window_started_at, window_ends_at, created_at, updated_at
       ) VALUES (
         'experiment:$experiment_id', '$campaign_id', '$experiment_id', 'experiment',
         '$experiment_id', 'reserved', $now, $window_ends_at, $now, $now
       );
       UPDATE campaigns SET baseline_experiment_id = '$experiment_id'
       WHERE campaign_id = '$campaign_id';"
}

submission_task_id() {
  summary="$1"
  task_id="$(printf '%s\n' "$summary" | awk '{ for (i = 1; i <= NF; i++) if ($i ~ /^task=[0-9]+$/) { sub(/^task=/, "", $i); print $i } }')"
  case "$task_id" in
    ''|*[!0-9]*) fail "submit did not emit exactly one numeric task field: $summary" ;;
  esac
  printf '%s\n' "$task_id"
}

write_config() {
  root="$1"
  project_id="$2"
  group="$3"
  agent_program="$4"
  max_agent_runs="$5"
  context_mode="${6:-fresh}"
  context_session_id="${7:-}"
  context_session_line=""
  if [ -n "$context_session_id" ]; then
    context_session_line="session_id = \"$context_session_id\""
  fi
  cat > "$root/.pueue-agent/config.toml" <<EOF
project_id = "$project_id"
pueue_group = "$group"

[agent]
program = "$agent_program"
args = ["{prompt}"]
timeout_minutes = 5
max_retries = 2

[agent.context]
mode = "$context_mode"
$context_session_line

[check]
interval_minutes = 1
deep_check_interval_minutes = 0
stall_minutes = 30
log_tail_bytes = 4096
extra_log_paths = []

[[check.patterns]]
name = "fatal-loss"
regex = "FATAL_LOSS"
action = "kill"
confirm_matches = 1

[check.stall]
action = "notify"
kill_after_minutes = 0

[guardrails]
max_consecutive_failures = 100
max_experiments = 100
max_agent_runs = $max_agent_runs
EOF
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v sqlite3 >/dev/null 2>&1 || fail "sqlite3 is required"
[ -n "$REAL_GIT" ] || fail "git is required"
[ -n "$REAL_PYTHON" ] || fail "python is required"

export HOME="$WORK/home"
export CARGO_HOME="${CARGO_HOME:-$ORIGINAL_HOME/.cargo}"
export CODEX_HOME="$WORK/codex-home"
export XDG_RUNTIME_DIR
export XDG_STATE_HOME="$WORK/state"
export PUEUE_AGENT_STATE_DIR="$WORK/state/pueue-agent"
export PUEUE_CONFIG_PATH="$WORK/pueue.yml"
export PUEUE_AGENT_TEST_AGENT_LOG="$WORK/agent-calls.log"
export PUEUE_AGENT_TEST_AGENT_STATE="$WORK/agent-state"
export PUEUE_AGENT_TEST_CODEX_LOG="$WORK/codex-calls.log"
export PUEUE_AGENT_TEST_CODEX_ENV_NAMES="$WORK/codex-env-names.log"
export OPENAI_API_KEY="campaign-openai-key-must-not-reach-decision"
export AWS_SECRET_ACCESS_KEY="campaign-credential-must-not-reach-agent"
export WANDB_API_KEY="campaign-wandb-key-must-not-reach-agent"
export SSH_AUTH_SOCK="campaign-ssh-socket-must-not-reach-agent"
# Keep interpreter caches out of the immutable candidate worktree.  The
# candidate boundary permits only the supervisor-owned result/artifact tree;
# Python bytecode is an incidental fixture output, not experiment evidence.
export PYTHONDONTWRITEBYTECODE=1
mkdir -p "$HOME" "$CODEX_HOME" "$XDG_RUNTIME_DIR" "$WORK/bin" "$WORK/pueue"
chmod 700 "$XDG_RUNTIME_DIR"
: > "$WORK/defer-kill"

cat > "$WORK/bin/pueue-proxy.rs" <<'EOF'
use std::{env, fs::OpenOptions, io::Write, process::Command};

fn main() {
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    let operation = arguments
        .iter()
        .find(|argument| argument.as_os_str() == "add" || argument.as_os_str() == "kill")
        .and_then(|argument| argument.to_str())
        .unwrap_or("");
    if operation == "kill" {
        let rendered = arguments
            .iter()
            .map(|argument| argument.to_string_lossy())
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(
            OpenOptions::new().create(true).append(true).open("__PUEUE_AGENT_E2E_WORK__/pueue-kills.log").unwrap(),
            "{rendered}",
        )
        .unwrap();
        if std::path::Path::new("__PUEUE_AGENT_E2E_WORK__/defer-kill").exists() {
            return;
        }
    }
    if operation == "add" {
        let mut capture = OpenOptions::new()
            .create(true)
            .append(true)
            .open("__PUEUE_AGENT_E2E_WORK__/pueue-add-argv.log")
            .unwrap();
        writeln!(capture, "ADD_BEGIN").unwrap();
        for (index, argument) in arguments.iter().enumerate() {
            writeln!(capture, "ADD_ARG_{}={}", index + 1, argument.to_string_lossy()).unwrap();
        }
        writeln!(capture, "ADD_END").unwrap();
    }
    let status = Command::new("__PUEUE_AGENT_E2E_REAL_PUEUE__")
        .args(&arguments)
        .status()
        .expect("run real Pueue");
    if operation == "add"
        && std::path::Path::new("__PUEUE_AGENT_E2E_WORK__/add-uncertain").exists()
    {
        std::process::exit(17);
    }
    std::process::exit(status.code().unwrap_or(125));
}
EOF
sed \
  -e "s|__PUEUE_AGENT_E2E_WORK__|$WORK|g" \
  -e "s|__PUEUE_AGENT_E2E_REAL_PUEUE__|$REAL_PUEUE|g" \
  "$WORK/bin/pueue-proxy.rs" > "$WORK/bin/pueue-proxy.rendered.rs"
rustc --edition=2021 --crate-name pueue_proxy -O \
  -o "$WORK/bin/pueue" "$WORK/bin/pueue-proxy.rendered.rs"
cat > "$WORK/bin/capture-agent-environment.sh" <<'EOF'
#!/usr/bin/env bash
set -eu
capture="${HOME:?HOME is required}/../captured-agent-environment"
if [ -n "${AWS_SECRET_ACCESS_KEY+x}" ] || [ -n "${WANDB_API_KEY+x}" ] \
  || [ -n "${SSH_AUTH_SOCK+x}" ]; then
  printf '%s\n' 'credential-value-present' >> "$capture"
fi
printf '%s\n' "${PUEUE_AGENT_RUN_ID:?missing run ID}:${PUEUE_AGENT_PROJECT_ID:?missing project ID}" \
  >> "$capture"
EOF
cp "$REPO_ROOT/tests/support/fake_agent.sh" "$WORK/bin/fake-agent.sh"
cp "$REPO_ROOT/tests/support/fake_codex.sh" "$WORK/bin/codex.sh"
cp "$(command -v jq)" "$WORK/bin/jq"
cp "$REAL_GIT" "$WORK/bin/git"
cp "$REAL_PYTHON" "$WORK/bin/python"
cat > "$WORK/bin/native-script-runner.rs" <<'EOF'
use std::{env, os::unix::process::CommandExt, path::PathBuf, process::Command};

fn main() {
    // Verified launchers execute this helper through /proc/self/fd magic
    // links, so procfs self paths and argv[0] cannot identify this helper
    // or locate the sibling script. The harness bakes the stable fixture
    // directory and script name in at build time.
    let directory = PathBuf::from("__PUEUE_AGENT_E2E_FIXTURE_DIR__");
    let mut script = directory.join("__PUEUE_AGENT_E2E_FIXTURE_NAME__");
    script.set_extension("sh");
    let error = Command::new("/bin/bash")
        .arg(script)
        .args(env::args_os().skip(1))
        .exec();
    eprintln!("failed to execute fixture script: {error}");
    std::process::exit(127);
}
EOF
for fixture in fake-agent capture-agent-environment codex; do
  sed \
    -e "s|__PUEUE_AGENT_E2E_FIXTURE_DIR__|$WORK/bin|g" \
    -e "s|__PUEUE_AGENT_E2E_FIXTURE_NAME__|$fixture|g" \
    "$WORK/bin/native-script-runner.rs" > "$WORK/bin/native-script-runner.$fixture.rs"
  rustc --edition=2021 --crate-name native_script_runner -O \
    -o "$WORK/bin/$fixture" "$WORK/bin/native-script-runner.$fixture.rs"
done
cp /usr/bin/bash /usr/bin/cat "$(command -v dirname)" "$(command -v mkdir)" /usr/bin/sleep "$WORK/bin/"
cat > "$WORK/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$WORK/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *--property=LoadState*) echo loaded ;;
  *is-active*) echo active ;;
esac
exit 0
EOF
chmod +x "$WORK/bin/pueue" "$WORK/bin/bash" "$WORK/bin/cat" "$WORK/bin/dirname" "$WORK/bin/mkdir" "$WORK/bin/sleep" \
  "$WORK/bin/git" "$WORK/bin/python" \
  "$WORK/bin/jq" "$WORK/bin/fake-agent" "$WORK/bin/capture-agent-environment" "$WORK/bin/codex" \
  "$WORK/bin/launchctl" "$WORK/bin/systemctl"
export PATH="$WORK/bin:$PATH"

cat > "$WORK/pueue.yml" <<EOF
shared:
  pueue_directory: "$WORK/pueue"
  use_unix_socket: true
  unix_socket_path: "$WORK/pueue.socket"
daemon:
  callback: null
  shell_command: ["/bin/sh", "-c", "{{ pueue_command_string }}"]
EOF
chmod 600 "$WORK/pueue.yml"

cargo build --quiet --offline --manifest-path "$REPO_ROOT/Cargo.toml"
"$PA_BIN" --help | grep -q "SQLite-backed Pueue agent supervisor" \
  || fail "bin/pueue-agent is not the Rust development launcher"

"$REAL_PUEUED" --config "$WORK/pueue.yml" -d >"$WORK/pueued.log" 2>&1
for _ in $(seq 100); do
  "$REAL_PUEUE" --config "$WORK/pueue.yml" status --json >/dev/null 2>&1 && break
  sleep 0.1
done
if ! "$REAL_PUEUE" --config "$WORK/pueue.yml" status --json >/dev/null 2>&1; then
  cat "$WORK/pueued.log" >&2
  fail "isolated pueued did not start"
fi
PUEUED_PID="$(cat "$XDG_RUNTIME_DIR/pueue.pid")"
case "$PUEUED_PID" in
  ''|*[!0-9]*) fail "isolated pueued did not publish a numeric PID" ;;
esac

PROJECT_A="$WORK/a/shared"
PROJECT_B="$WORK/b/shared"
PROJECT_C="$WORK/c/campaign"
PROJECT_D="$WORK/d/campaign"
PROJECT_E="$WORK/e/trusted-failure"
PROJECT_F="$WORK/f/untrusted-failure"
PROJECT_G="$WORK/g/wait-once"
PROJECT_H="$WORK/h/invalid-three"
PROJECT_I="$WORK/i/running-health"
PROJECT_J="$WORK/j/restart-diagnosis"
PROJECT_K="$WORK/k/manifest-promotion"
PROJECT_L="$WORK/l/goal-review"
PROJECT_M="$WORK/m/python-ml-code-change"
mkdir -p "$PROJECT_A" "$PROJECT_B" "$PROJECT_C" "$PROJECT_D" \
  "$PROJECT_E" "$PROJECT_F" "$PROJECT_G" "$PROJECT_H" "$PROJECT_I" "$PROJECT_J" "$PROJECT_K" "$PROJECT_L" "$PROJECT_M"
PROJECT_A_CANONICAL="$(cd "$PROJECT_A" && pwd -P)"
"$PA_BIN" init "$PROJECT_A"
"$PA_BIN" init "$PROJECT_B"
"$PA_BIN" init "$PROJECT_C"
"$PA_BIN" init "$PROJECT_D"
"$PA_BIN" init "$PROJECT_E"
"$PA_BIN" init "$PROJECT_F"
"$PA_BIN" init "$PROJECT_G"
"$PA_BIN" init "$PROJECT_H"
"$PA_BIN" init "$PROJECT_I"
"$PA_BIN" init "$PROJECT_J"
"$PA_BIN" init "$PROJECT_K"
"$PA_BIN" init "$PROJECT_L"
"$PA_BIN" init "$PROJECT_M"

CONFIG_A="$PROJECT_A/.pueue-agent/config.toml"
CONFIG_B="$PROJECT_B/.pueue-agent/config.toml"
CONFIG_C="$PROJECT_C/.pueue-agent/config.toml"
CONFIG_D="$PROJECT_D/.pueue-agent/config.toml"
CONFIG_E="$PROJECT_E/.pueue-agent/config.toml"
CONFIG_F="$PROJECT_F/.pueue-agent/config.toml"
CONFIG_G="$PROJECT_G/.pueue-agent/config.toml"
CONFIG_H="$PROJECT_H/.pueue-agent/config.toml"
CONFIG_I="$PROJECT_I/.pueue-agent/config.toml"
CONFIG_J="$PROJECT_J/.pueue-agent/config.toml"
CONFIG_K="$PROJECT_K/.pueue-agent/config.toml"
CONFIG_L="$PROJECT_L/.pueue-agent/config.toml"
CONFIG_M="$PROJECT_M/.pueue-agent/config.toml"
[ -f "$CONFIG_A" ] && [ -f "$CONFIG_B" ] && [ -f "$CONFIG_C" ] && [ -f "$CONFIG_D" ] \
  && [ -f "$CONFIG_E" ] && [ -f "$CONFIG_F" ] && [ -f "$CONFIG_G" ] && [ -f "$CONFIG_H" ] \
  && [ -f "$CONFIG_I" ] && [ -f "$CONFIG_J" ] && [ -f "$CONFIG_K" ] && [ -f "$CONFIG_L" ] \
  && [ -f "$CONFIG_M" ] \
  || fail "init did not create TOML configuration"
PROJECT_ID_A="$(toml_value project_id "$CONFIG_A")"
PROJECT_ID_B="$(toml_value project_id "$CONFIG_B")"
PROJECT_ID_C="$(toml_value project_id "$CONFIG_C")"
PROJECT_ID_D="$(toml_value project_id "$CONFIG_D")"
PROJECT_ID_E="$(toml_value project_id "$CONFIG_E")"
PROJECT_ID_F="$(toml_value project_id "$CONFIG_F")"
PROJECT_ID_G="$(toml_value project_id "$CONFIG_G")"
PROJECT_ID_H="$(toml_value project_id "$CONFIG_H")"
PROJECT_ID_I="$(toml_value project_id "$CONFIG_I")"
PROJECT_ID_J="$(toml_value project_id "$CONFIG_J")"
PROJECT_ID_K="$(toml_value project_id "$CONFIG_K")"
PROJECT_ID_L="$(toml_value project_id "$CONFIG_L")"
PROJECT_ID_M="$(toml_value project_id "$CONFIG_M")"
GROUP_A="$(toml_value pueue_group "$CONFIG_A")"
GROUP_B="$(toml_value pueue_group "$CONFIG_B")"
GROUP_C="$(toml_value pueue_group "$CONFIG_C")"
GROUP_D="$(toml_value pueue_group "$CONFIG_D")"
GROUP_E="$(toml_value pueue_group "$CONFIG_E")"
GROUP_F="$(toml_value pueue_group "$CONFIG_F")"
GROUP_G="$(toml_value pueue_group "$CONFIG_G")"
GROUP_H="$(toml_value pueue_group "$CONFIG_H")"
GROUP_I="$(toml_value pueue_group "$CONFIG_I")"
GROUP_J="$(toml_value pueue_group "$CONFIG_J")"
GROUP_K="$(toml_value pueue_group "$CONFIG_K")"
GROUP_L="$(toml_value pueue_group "$CONFIG_L")"
GROUP_M="$(toml_value pueue_group "$CONFIG_M")"
[ "$PROJECT_ID_A" != "$PROJECT_ID_B" ] || fail "same-basename projects reused project_id"
[ "$GROUP_A" != "$GROUP_B" ] || fail "same-basename projects reused Pueue group"

write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_B" "$PROJECT_ID_B" "$GROUP_B" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_C" "$PROJECT_ID_C" "$GROUP_C" "$WORK/bin/capture-agent-environment" 20
write_config "$PROJECT_D" "$PROJECT_ID_D" "$GROUP_D" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_E" "$PROJECT_ID_E" "$GROUP_E" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_F" "$PROJECT_ID_F" "$GROUP_F" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_G" "$PROJECT_ID_G" "$GROUP_G" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_H" "$PROJECT_ID_H" "$GROUP_H" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_I" "$PROJECT_ID_I" "$GROUP_I" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_J" "$PROJECT_ID_J" "$GROUP_J" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_K" "$PROJECT_ID_K" "$GROUP_K" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_L" "$PROJECT_ID_L" "$GROUP_L" "$WORK/bin/fake-agent" 20
write_config "$PROJECT_M" "$PROJECT_ID_M" "$GROUP_M" "$WORK/bin/fake-agent" 20
printf '%s\n' 'Keep the supervisor fixture healthy while validating task recovery.' \
  > "$PROJECT_A/.pueue-agent/STATE.md"
printf '%s\n' 'Keep callback and reconciliation processing idempotent.' \
  > "$PROJECT_B/.pueue-agent/STATE.md"
printf '%s\n' 'Reach validation loss below 0.20 without changing the dataset.' \
  > "$PROJECT_C/.pueue-agent/STATE.md"
printf '%s\n' 'Reach validation loss below 0.25 without changing the dataset.' \
  > "$PROJECT_D/.pueue-agent/STATE.md"
printf '%s\n' 'Recover one trusted terminal failure with a bounded repair.' \
  > "$PROJECT_E/.pueue-agent/STATE.md"
printf '%s\n' 'Choose a non-repair experiment when failure trust is absent.' \
  > "$PROJECT_F/.pueue-agent/STATE.md"
printf '%s\n' 'PUEUE_AGENT_E2E_WAIT_ONCE' > "$PROJECT_G/.pueue-agent/STATE.md"
printf '%s\n' 'PUEUE_AGENT_E2E_INVALID_THREE' > "$PROJECT_H/.pueue-agent/STATE.md"
printf '%s\n' 'Keep the diagnosed OOM experiment observable for the running-health machine.' \
  > "$PROJECT_I/.pueue-agent/STATE.md"
printf '%s\n' 'Keep the restart-diagnosis experiment observable across daemon restarts.' \
  > "$PROJECT_J/.pueue-agent/STATE.md"
printf '%s\n' 'Reach validation loss below 0.20 with promotion' \
  > "$PROJECT_K/.pueue-agent/STATE.md"
printf '%s\n' 'PUEUE_AGENT_E2E_GOAL' > "$PROJECT_L/.pueue-agent/STATE.md"
printf '%s\n' 'PUEUE_AGENT_E2E_CODE_CHANGE_SUCCESS' > "$PROJECT_M/.pueue-agent/STATE.md"

mkdir -p "$XDG_STATE_HOME"
chmod 700 "$XDG_STATE_HOME"
mkdir -m 700 "$PUEUE_AGENT_STATE_DIR"
cat > "$PUEUE_AGENT_STATE_DIR/execution-policy.toml" <<EOF
version = 1
trusted_path = "$WORK/bin"

[defaults]
network = "enabled"

[campaign]
max_parallel_experiments = 1
max_new_experiments_per_24h = 24
max_agent_runs_per_hour = 6
max_code_change_proposals_per_24h = 10
max_same_spec_retries = 2
max_repairs_per_failure_fingerprint = 2
max_proposals_per_cycle = 1
observer_interval_minutes = 1
max_decision_attempts_per_cycle = 3
max_decision_wait_minutes = 1440

[executables]
codex = "codex"
pueue = "pueue"
git = "git"
python = "python"

[projects."$PROJECT_ID_A"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_B"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_C"]
custom_agent = "$WORK/bin/capture-agent-environment"

[projects."$PROJECT_ID_D"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_E"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_F"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_G"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_H"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_I"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_J"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_K"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_L"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]

[projects."$PROJECT_ID_M"]
custom_agent = "$WORK/bin/fake-agent"
agent_environment_allow = ["PUEUE_AGENT_TEST_AGENT_LOG", "PUEUE_AGENT_TEST_AGENT_STATE", "PUEUE_AGENT_TEST_AGENT_MODE"]
EOF
chmod 600 "$PUEUE_AGENT_STATE_DIR/execution-policy.toml"

"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_A"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_B"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_C"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_D"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_E"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_F"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_G"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_H"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_I"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_J"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_K"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_L"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_M"
STATE_DB="$XDG_STATE_HOME/pueue-agent/state.sqlite3"
[ "$(sql 'SELECT COUNT(*) FROM projects')" = "13" ] || fail "projects were not registered"

# The code-change gate uses one disposable Git/Python repository.  Its local
# bare remote is only metadata for the protected-remote invariant; no command
# below pushes or mutates it.
ML_REMOTE="$WORK/m/python-ml-remote.git"
ML_UNRELATED="$WORK/m/python-ml-unrelated"
cat > "$PROJECT_M/.gitignore" <<'EOF'
.pueue-agent/
__pycache__/
.pytest_cache/
EOF
cat > "$PROJECT_M/model.py" <<'EOF'
def score():
    return 0
EOF
cat > "$PROJECT_M/test_model.py" <<'EOF'
from model import score


def test_smoke():
    assert score() == 1
EOF
cat > "$PROJECT_M/pytest.ini" <<'EOF'
[pytest]
addopts = -q
EOF
cat > "$PROJECT_M/train.py" <<'EOF'
import json
import os

from model import score


loss = 0.25 if score() == 1 else 1.0
result_path = os.environ["PUEUE_AGENT_RESULT_PATH"]
os.makedirs(os.path.dirname(result_path), exist_ok=True)
with open(result_path, "w", encoding="utf-8") as result_file:
    json.dump(
        {
            "schema_version": 1,
            "experiment_id": os.environ["PUEUE_AGENT_EXPERIMENT_ID"],
            "metrics": {"loss": loss},
        },
        result_file,
    )
print(f"loss={loss}")
EOF
cat > "$PROJECT_M/train_oom.py" <<'EOF'
import sys


print("CUDA out of memory", file=sys.stderr)
sys.exit(137)
EOF
cat > "$PROJECT_M/train_internal.py" <<'EOF'
import sys


print("internal error in candidate runtime", file=sys.stderr)
sys.exit(70)
EOF
git init --quiet -b main "$PROJECT_M"
git -C "$PROJECT_M" config user.name "Pueue Agent E2E"
git -C "$PROJECT_M" config user.email "pueue-agent-e2e@example.invalid"
git init --quiet --bare "$ML_REMOTE"
git -C "$PROJECT_M" remote add origin "$ML_REMOTE"
git -C "$PROJECT_M" add .gitignore model.py test_model.py pytest.ini train.py train_oom.py train_internal.py
git -C "$PROJECT_M" commit --quiet -m "baseline ML fixture"
git -C "$PROJECT_M" worktree add --quiet --detach "$ML_UNRELATED" HEAD
ML_MAIN_SHA="$(git -C "$PROJECT_M" rev-parse refs/heads/main)"
ML_FILE_DIGEST="$(git -C "$PROJECT_M" hash-object "$PROJECT_M/model.py")"
ML_REMOTE_CONFIG="$(git -C "$PROJECT_M" config --local --get-regexp '^remote\.' || true)"
ML_WORKTREES_BEFORE="$(git -C "$PROJECT_M" worktree list --porcelain)"

# Healthy monitoring performs reconciliation without spending agent tokens.
start_daemon
sleep 0.2
stop_daemon
[ ! -f "$PUEUE_AGENT_TEST_AGENT_LOG" ] || fail "healthy monitoring started an agent"

# One default submit creates exactly one durable campaign baseline and one real Pueue task.
campaign_summary="$(cd "$PROJECT_C" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
campaign_task="$(submission_task_id "$campaign_summary")"
record_task_id "success-source" "$campaign_task"
CAMPAIGN_C="$(sql "SELECT campaign_id FROM campaigns WHERE project_id = '$PROJECT_ID_C' AND state <> 'retired'")"
[ -n "$CAMPAIGN_C" ] || fail "default submit did not create a live campaign"
for table in campaigns proposals experiments budget_reservations submissions; do
  case "$table" in
    campaigns|submissions)
      count="$(sql "SELECT COUNT(*) FROM $table WHERE project_id = '$PROJECT_ID_C'")"
      ;;
    *)
      count="$(sql "SELECT COUNT(*) FROM $table WHERE campaign_id = '$CAMPAIGN_C'")"
      ;;
  esac
  [ "$count" = "1" ] || fail "managed baseline created $count $table rows instead of one"
done
[ "$(pueue_group_task_count "$GROUP_C")" = "1" ] \
  || fail "managed baseline did not create exactly one Pueue task"
[ "$(sql "SELECT COUNT(DISTINCT pueue_task_id) FROM experiments WHERE campaign_id = '$CAMPAIGN_C'")" = "1" ] \
  || fail "managed baseline did not retain one stable external task identity"

# A live campaign rejects both direct submission interfaces before any durable or external write.
cat > "$WORK/rejected-batch.json" <<EOF
{"jobs":[{"id":"second","argv":["/bin/sh","$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh"]}]}
EOF
before_submissions="$(sql "SELECT COUNT(*) FROM submissions WHERE project_id = '$PROJECT_ID_C'")"
before_batches="$(sql "SELECT COUNT(*) FROM batch_requests WHERE project_id = '$PROJECT_ID_C'")"
before_batch_jobs="$(sql "SELECT COUNT(*) FROM batch_jobs")"
before_tasks="$(pueue_group_task_count "$GROUP_C")"
if (cd "$PROJECT_C" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh") \
  >"$WORK/rejected-submit.out" 2>"$WORK/rejected-submit.err"; then
  fail "second direct submit was accepted during a live campaign"
fi
if (cd "$PROJECT_C" && "$PA_BIN" submit-batch \
  --request-id 22222222-2222-4222-8222-222222222222 \
  --manifest "$WORK/rejected-batch.json") \
  >"$WORK/rejected-batch.out" 2>"$WORK/rejected-batch.err"; then
  fail "batch submit was accepted during a live campaign"
fi
[ "$(sql "SELECT COUNT(*) FROM submissions WHERE project_id = '$PROJECT_ID_C'")" = "$before_submissions" ] \
  || fail "rejected direct submit created a submission row"
[ "$(sql "SELECT COUNT(*) FROM batch_requests WHERE project_id = '$PROJECT_ID_C'")" = "$before_batches" ] \
  || fail "rejected batch created a batch row"
[ "$(sql "SELECT COUNT(*) FROM batch_jobs")" = "$before_batch_jobs" ] \
  || fail "rejected batch created a batch job row"
for table in campaigns proposals experiments budget_reservations; do
  case "$table" in
    campaigns)
      count="$(sql "SELECT COUNT(*) FROM campaigns WHERE project_id = '$PROJECT_ID_C'")"
      ;;
    *)
      count="$(sql "SELECT COUNT(*) FROM $table WHERE campaign_id = '$CAMPAIGN_C'")"
      ;;
  esac
  [ "$count" = "1" ] || fail "rejected submit changed the managed $table row count to $count"
done
[ "$(pueue_group_task_count "$GROUP_C")" = "$before_tasks" ] \
  || fail "rejected submit created a Pueue task"

# Restarting an accepted intent never performs a second external add.
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_C" >/dev/null
start_daemon
sleep 0.2
stop_daemon
[ "$(pueue_group_task_count "$GROUP_C")" = "1" ] \
  || fail "accepted campaign restart duplicated the Pueue task"
[ "$(sql "SELECT COUNT(DISTINCT pueue_task_id) FROM experiments WHERE campaign_id = '$CAMPAIGN_C'")" = "1" ] \
  || fail "accepted campaign restart changed its stable task identity"

# Terminal success creates one decision cycle and one accepted experiment proposal.
wait_for_task_terminal "$campaign_task"
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_C" >/dev/null
success_add_before="$(pueue_add_call_count)"
SUCCESS_SOURCE="$(sql "SELECT baseline_experiment_id FROM campaigns WHERE campaign_id = '$CAMPAIGN_C'")"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$SUCCESS_SOURCE'" "succeeded" \
  "campaign baseline was not projected terminal"
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_C' AND source_experiment_id = '$SUCCESS_SOURCE' AND state = 'completed'" "1" \
  "terminal success decision did not complete"
wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_C' AND parent_experiment_id = '$SUCCESS_SOURCE' AND status = 'accepted'" "1" \
  "terminal success decision did not accept one child experiment"
wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_C' AND status = 'completed'" "2" \
  "campaign decision and terminal event agents did not complete"
stop_daemon
[ -s "$WORK/captured-agent-environment" ] || fail "fake agent did not capture its sanitized environment"
! grep -Eq 'credential-value-present|campaign-credential-must-not-reach-agent|campaign-wandb-key-must-not-reach-agent|campaign-ssh-socket-must-not-reach-agent' \
  "$WORK/captured-agent-environment" \
  || fail "network-enabled agent inherited an unlisted credential value"
SUCCESS_CHILD_TASK="$(sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_C' AND parent_experiment_id = '$SUCCESS_SOURCE'")"
record_task_id "success-child" "$SUCCESS_CHILD_TASK"
[ "$(sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_C' AND source_experiment_id = '$SUCCESS_SOURCE'")" = "1" ] \
  || fail "terminal success created more than one decision cycle"
[ "$(sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$CAMPAIGN_C' AND source_experiment_id = '$SUCCESS_SOURCE' AND kind = 'experiment' AND status = 'accepted'")" = "1" ] \
  || fail "terminal success did not accept exactly one experiment proposal"
[ "$(decision_call_count "$SUCCESS_SOURCE")" = "1" ] \
  || fail "terminal success did not invoke exactly one decision agent"
[ "$(pueue_add_call_count)" = "$((success_add_before + 1))" ] \
  || fail "terminal success did not perform exactly one child Pueue add"
start_daemon
sleep 0.2
stop_daemon
[ "$(pueue_add_call_count)" = "$((success_add_before + 1))" ] \
  || fail "terminal success restart duplicated the child Pueue add"
grep -F 'sandbox_read_only=true' "$PUEUE_AGENT_TEST_CODEX_LOG" >/dev/null \
  || fail "decision Codex did not use the read-only permission profile"
grep -F 'network_access=true' "$PUEUE_AGENT_TEST_CODEX_LOG" >/dev/null \
  || fail "decision Codex did not retain enabled network policy"
! grep -Eq 'OPENAI_API_KEY|AWS_SECRET_ACCESS_KEY|SSH_AUTH_SOCK' \
  "$PUEUE_AGENT_TEST_CODEX_ENV_NAMES" \
  || fail "decision Codex inherited a credential environment name"
grep -Eq '^ADD_ARG_[0-9]+=/bin/sleep$' "$WORK/pueue-add-argv.log" \
  || fail "decision proposal did not pass literal child argv to Pueue"

# A due rolling budget wait keeps a finite wake and becomes active after expiry advances.
budget_now="$(date +%s)"
budget_end=$((budget_now + 3600))
for ordinal in 1 2 3 4 5 6; do
  sql "INSERT OR IGNORE INTO budget_reservations (
         reservation_id, campaign_id, dimension, subject_key, status,
         window_started_at, window_ends_at, created_at, updated_at
       ) VALUES (
         'e2e-agent-budget-$ordinal', '$CAMPAIGN_C', 'agent_run',
         'e2e-agent-budget-$ordinal', 'consumed', $budget_now, $budget_end,
         $budget_now, $budget_now
       )"
done
sql "UPDATE campaigns SET state = 'budget_waiting', state_reason = 'agent_run_budget_exhausted',
       next_eligible_at = $((budget_now - 1)) WHERE campaign_id = '$CAMPAIGN_C'"
start_daemon
wait_for_sql "SELECT CASE WHEN next_eligible_at > $budget_now THEN 1 ELSE 0 END FROM campaigns WHERE campaign_id = '$CAMPAIGN_C'" "1" \
  "budget wait did not retain a finite next wake"
stop_daemon
sql "UPDATE budget_reservations
     SET window_started_at = $((budget_now - 2)), window_ends_at = $((budget_now - 1))
     WHERE campaign_id = '$CAMPAIGN_C' AND dimension = 'agent_run';
     UPDATE campaigns SET next_eligible_at = $((budget_now - 1)) WHERE campaign_id = '$CAMPAIGN_C';"
start_daemon
wait_for_sql "SELECT state FROM campaigns WHERE campaign_id = '$CAMPAIGN_C'" "active" \
  "expired rolling budget did not reactivate the campaign"
stop_daemon

# Retirement requires terminal experiments, so stop the accepted success
# child through the real scheduler and wait for its durable projection.
# Pueue v4 reports killed tasks as Done with result Killed, which the
# reconciler projects as a failed experiment.
"$REAL_PUEUE" --config "$WORK/pueue.yml" kill "$SUCCESS_CHILD_TASK" >/dev/null
wait_for_task_terminal "$SUCCESS_CHILD_TASK"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE pueue_task_id = $SUCCESS_CHILD_TASK" "failed" \
  "killed success child was not projected failed"
stop_daemon
"$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_C" >/dev/null

# Reserved restart recovery adds once; a second accepted restart adds nothing.
RESERVED_CAMPAIGN="e2e-reserved-boundary"
insert_campaign_boundary "$PROJECT_ID_C" "$RESERVED_CAMPAIGN" "reserved"
RESERVED_SOURCE="$RESERVED_CAMPAIGN-experiment"
reserved_before="$(pueue_group_task_count "$GROUP_C")"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$RESERVED_SOURCE'" "accepted" \
  "reserved restart did not resume its durable intent"
stop_daemon
[ "$(pueue_group_task_count "$GROUP_C")" = "$((reserved_before + 1))" ] \
  || fail "reserved restart did not perform exactly one Pueue add"
reserved_task="$(sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$RESERVED_SOURCE'")"
start_daemon
sleep 0.2
stop_daemon
[ "$(pueue_group_task_count "$GROUP_C")" = "$((reserved_before + 1))" ] \
  || fail "accepted restart repeated the recovered Pueue add"
[ "$(sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$RESERVED_SOURCE'")" = "$reserved_task" ] \
  || fail "accepted restart changed recovered task identity"
wait_for_task_terminal "$reserved_task"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$RESERVED_SOURCE'" "succeeded" \
  "recovered reserved experiment was not projected terminal"
stop_daemon
"$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_C" >/dev/null

# A submitting restart is quarantined and never reaches Pueue again.
SUBMITTING_CAMPAIGN="e2e-submitting-boundary"
insert_campaign_boundary "$PROJECT_ID_C" "$SUBMITTING_CAMPAIGN" "submitting"
SUBMITTING_SOURCE="$SUBMITTING_CAMPAIGN-experiment"
submitting_before="$(pueue_group_task_count "$GROUP_C")"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$SUBMITTING_SOURCE'" "unreconciled" \
  "submitting restart was not quarantined"
stop_daemon
[ "$(pueue_group_task_count "$GROUP_C")" = "$submitting_before" ] \
  || fail "submitting restart performed an unsafe second Pueue add"

# A real Pueue add followed by a failed response remains unreconciled across restart.
: > "$WORK/add-uncertain"
uncertain_before="$(pueue_group_task_count "$GROUP_D")"
if (cd "$PROJECT_D" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh") \
  >"$WORK/uncertain-submit.out" 2>"$WORK/uncertain-submit.err"; then
  fail "uncertain Pueue add unexpectedly returned success"
fi
rm -f "$WORK/add-uncertain"
UNCERTAIN_CAMPAIGN="$(sql "SELECT campaign_id FROM campaigns WHERE project_id = '$PROJECT_ID_D' AND state <> 'retired'")"
UNCERTAIN_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE campaign_id = '$UNCERTAIN_CAMPAIGN' AND parent_experiment_id IS NULL")"
[ "$(sql "SELECT status FROM experiments WHERE experiment_id = '$UNCERTAIN_SOURCE'")" = "unreconciled" ] \
  || fail "uncertain Pueue add did not persist unreconciled"
[ "$(pueue_group_task_count "$GROUP_D")" = "$((uncertain_before + 1))" ] \
  || fail "uncertain fixture did not create exactly one external Pueue task"
start_daemon
sleep 0.2
stop_daemon
[ "$(sql "SELECT status FROM experiments WHERE experiment_id = '$UNCERTAIN_SOURCE'")" = "unreconciled" ] \
  || fail "restart changed the unreconciled quarantine"
[ "$(pueue_group_task_count "$GROUP_D")" = "$((uncertain_before + 1))" ] \
  || fail "restart re-added an unreconciled Pueue task"

# Scenario C: manifest promotion – submit with metric, task writes manifest then succeeds.
metric_summary="$(cd "$PROJECT_K" && "$PA_BIN" submit --metric-name loss --metric-direction minimize -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_metrics.sh")"
metric_task="$(submission_task_id "$metric_summary")"
record_task_id "metric-promotion-source" "$metric_task"
CAMPAIGN_K="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $metric_task")"
METRIC_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $metric_task")"
wait_for_task_terminal "$metric_task"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$METRIC_SOURCE'" "succeeded" \
  "metric promotion baseline was not projected succeeded"
wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$METRIC_SOURCE'" "1" \
  "metric promotion did not persist metrics row"
wait_for_sql "SELECT artifact_defect FROM experiment_metrics WHERE experiment_id = '$METRIC_SOURCE'" "" \
  "metric promotion had unexpected artifact_defect"
wait_for_sql "SELECT current_best_experiment_id FROM campaigns WHERE campaign_id = '$CAMPAIGN_K'" "$METRIC_SOURCE" \
  "metric promotion did not set current_best"
wait_for_sql "SELECT plateau_count FROM campaigns WHERE campaign_id = '$CAMPAIGN_K'" "0" \
  "metric promotion did not reset plateau"
stop_daemon
# Verify status shows best and plateau, and diagnostics caps metrics at 50.
"$PA_BIN" status --pueue-config "$WORK/pueue.yml" "$PROJECT_K" | grep -q "best:" \
  || fail "metric promotion status missing best:"
"$PA_BIN" status --pueue-config "$WORK/pueue.yml" "$PROJECT_K" | grep -q "plateau:" \
  || fail "metric promotion status missing plateau:"
# Bound this campaign before later scenarios.
"$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_K" >/dev/null
[ "$(sql "SELECT state FROM campaigns WHERE campaign_id = '$CAMPAIGN_K'")" = "retired" ] \
  || fail "metric promotion retire did not retire campaign"

# Task 7: a real-Pueue code-change campaign edits a detached candidate, retries
# one failed pytest round in the same editor session, evaluates the candidate
# manifest, and cleans up without touching the original checkout.  The same
# durable coordinator is reused for the rejection and runtime-failure cases.
run_code_change_case() {
  marker="$1"
  expected_argv_json="$2"
  outcome="$3"
  printf '%s\n' "$marker" > "$PROJECT_M/.pueue-agent/STATE.md"

  baseline_summary="$(cd "$PROJECT_M" && "$PA_BIN" submit --metric-name loss --metric-direction minimize -- python train.py)"
  baseline_task="$(submission_task_id "$baseline_summary")"
  record_task_id "code-change-$outcome-source" "$baseline_task"
  wait_for_task_terminal "$baseline_task"
  campaign_id="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $baseline_task")"
  source_experiment_id="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $baseline_task")"
  [ -n "$campaign_id" ] && [ -n "$source_experiment_id" ] \
    || fail "$outcome fixture did not create its baseline campaign"
  candidate_add_before="$(pueue_add_call_count)"
  best_before="$(git_ref_sha "$PROJECT_M" "refs/heads/campaign/$campaign_id/best")"
  [ -z "$best_before" ] || fail "$outcome campaign unexpectedly had a best ref before code change"

  # Keep the source projection and its decision agent in one daemon lifetime.
  # Restarting immediately after the source metric can reuse a same-second
  # gate path while the decision child is still in flight.
  start_daemon
  wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$source_experiment_id'" "succeeded" \
    "$outcome baseline was not projected succeeded"
  wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$source_experiment_id' AND artifact_defect IS NULL" "1" \
    "$outcome baseline did not persist its metric"
  wait_for_sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$campaign_id' AND kind = 'code_change' AND status = 'pending'" "1" \
    "$outcome did not expose one pending code-change proposal"
  stop_daemon
  [ "$(sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$campaign_id' AND kind = 'code_change'")" = "1" ] \
    || fail "$outcome created more than one code-change proposal"
  run_id="$(sql "SELECT code_change_run_id FROM code_change_runs WHERE campaign_id = '$campaign_id'")"
  proposal_id="$(sql "SELECT proposal_id FROM code_change_runs WHERE code_change_run_id = '$run_id'")"
  [ -n "$run_id" ] && [ -n "$proposal_id" ] \
    || fail "$outcome pending proposal did not reserve one code-change run"
  [ "$(sql "SELECT state FROM code_change_runs WHERE code_change_run_id = '$run_id'")" = "reserved" ] \
    || fail "$outcome pending proposal was not durably reserved"
  [ "$(sql "SELECT argv_json FROM proposals WHERE proposal_id = '$proposal_id'")" = "$expected_argv_json" ] \
    || fail "$outcome proposal argv was not pinned to the expected candidate runtime"

  start_daemon
  if [ "$outcome" = "success" ] || [ "$outcome" = "second-check-fail" ]; then
    wait_for_sql "SELECT COUNT(*) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id' AND attempt = 1 AND status = 'ready' AND failure_code = 'check_failed'" "1" \
      "$outcome did not record the first failed pytest round"
  fi
  if [ "$outcome" = "second-check-fail" ]; then
    wait_for_sql "SELECT COUNT(*) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id'" "2" \
      "second-check-fail did not use exactly two editor attempts"
    wait_for_sql "SELECT state FROM code_change_runs WHERE code_change_run_id = '$run_id'" "rejected" \
      "second-check-fail did not reject after its second failed check"
    wait_for_sql "SELECT cleanup_completed_at IS NOT NULL FROM code_change_runs WHERE code_change_run_id = '$run_id'" "1" \
      "second-check-fail did not complete owned-worktree cleanup"
    stop_daemon
    [ "$(sql "SELECT status FROM proposals WHERE proposal_id = '$proposal_id'")" = "pending" ] \
      || fail "second-check-fail changed its pending proposal unexpectedly"
    [ "$(sql "SELECT COUNT(*) FROM experiments WHERE code_change_run_id = '$run_id'")" = "0" ] \
      || fail "second-check-fail created a candidate experiment"
    [ -z "$(sql "SELECT candidate_sha FROM code_change_runs WHERE code_change_run_id = '$run_id'")" ] \
      || fail "second-check-fail created a candidate commit"
    candidate_ref="$(sql "SELECT candidate_ref FROM code_change_runs WHERE code_change_run_id = '$run_id'")"
    [ -z "$(git_ref_sha "$PROJECT_M" "refs/heads/$candidate_ref")" ] \
      || fail "second-check-fail created a candidate ref"
    [ -z "$(git_ref_sha "$PROJECT_M" "refs/heads/campaign/$campaign_id/best")" ] \
      || fail "second-check-fail created or moved best"
    [ "$(sql "SELECT current_best_experiment_id FROM campaigns WHERE campaign_id = '$campaign_id'")" = "$source_experiment_id" ] \
      || fail "second-check-fail changed current_best"
    [ "$(sql "SELECT COUNT(DISTINCT editor_session_id) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id'")" = "1" ] \
      || fail "second-check-fail did not resume one editor session"
    assert_ml_original_unchanged
    "$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_M" >/dev/null
    return
  fi

  wait_for_sql "SELECT COUNT(*) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id'" \
    "$([ "$outcome" = "success" ] && printf 2 || printf 1)" \
    "$outcome did not use the expected number of editor attempts"
  wait_for_sql "SELECT COUNT(*) FROM experiments WHERE code_change_run_id = '$run_id' AND pueue_task_id IS NOT NULL" "1" \
    "$outcome did not submit exactly one candidate experiment"
  stop_daemon
  "$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_M" >/dev/null

  candidate_experiment_id="$(sql "SELECT experiment_id FROM experiments WHERE code_change_run_id = '$run_id'")"
  candidate_task="$(sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$candidate_experiment_id'")"
  record_task_id "code-change-$outcome-candidate" "$candidate_task"
  [ "$(pueue_add_call_count)" = "$((candidate_add_before + 1))" ] \
    || fail "$outcome did not perform exactly one candidate Pueue add"
  candidate_worktree_relative_path="$(sql "SELECT worktree_relative_path FROM code_change_runs WHERE code_change_run_id = '$run_id'")"
  case "$candidate_worktree_relative_path" in
    .pueue-agent/worktrees/*)
      candidate_worktree_relative_path="${candidate_worktree_relative_path#.pueue-agent/}"
      ;;
    *)
      fail "$outcome persisted an unexpected candidate worktree namespace"
      ;;
  esac
  candidate_path="$PUEUE_AGENT_STATE_DIR/$candidate_worktree_relative_path"
  candidate_cwd_count="$(awk -v path="$candidate_path" '
    /^ADD_BEGIN$/ { in_block = 1; expect_path = 0; next }
    /^ADD_END$/ { in_block = 0; expect_path = 0; next }
    !in_block { next }
    {
      if (expect_path) {
        split($0, current_parts, "=")
        split(current_parts[1], current_index_parts, "_")
        if (current_index_parts[3] == cwd_index + 1 && current_parts[2] == path) {
          count++
        }
        expect_path = 0
      }
      if ($0 ~ /^ADD_ARG_[0-9]+=--working-directory$/) {
        split($0, cwd_parts, "=")
        split(cwd_parts[1], cwd_index_parts, "_")
        cwd_index = cwd_index_parts[3]
        expect_path = 1
      }
    }
    END { print count + 0 }
  ' "$WORK/pueue-add-argv.log")"
  [ "$candidate_cwd_count" = "1" ] \
    || fail "$outcome candidate Pueue add did not use its persisted candidate cwd"
  [ "$(sql "SELECT code_revision_sha = (SELECT candidate_sha FROM code_change_runs WHERE code_change_run_id = '$run_id') FROM experiments WHERE experiment_id = '$candidate_experiment_id'")" = "1" ] \
    || fail "$outcome candidate experiment was not pinned to the persisted candidate SHA"
  [ "$(sql "SELECT argv_json FROM submissions WHERE submission_id = (SELECT submission_id FROM experiments WHERE experiment_id = '$candidate_experiment_id')")" = "$expected_argv_json" ] \
    || fail "$outcome candidate submission changed the proposal argv"
  wait_for_task_terminal "$candidate_task"

  start_daemon
  if [ "$outcome" = "success" ]; then
    wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$candidate_experiment_id'" "succeeded" \
      "successful candidate was not projected succeeded"
    wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$candidate_experiment_id' AND primary_metric_name = 'loss' AND primary_metric_value = 0.25 AND artifact_defect IS NULL" "1" \
      "successful candidate did not persist its improved manifest metric"
    wait_for_sql "SELECT COUNT(*) FROM code_change_checks WHERE code_change_run_id = '$run_id' AND attempt = 2 AND source = 'discovered' AND status = 'passed' AND output_digest IS NOT NULL" "1" \
      "successful candidate did not pass exactly one Python project check"
    wait_for_sql "SELECT CASE WHEN diff_digest IS NOT NULL AND length(diff_digest) = 64 THEN 1 ELSE 0 END FROM code_change_runs WHERE code_change_run_id = '$run_id'" "1" \
      "successful candidate did not persist its final checked diff digest"
  else
    wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$candidate_experiment_id'" "failed" \
      "$outcome candidate runtime failure was not projected"
    wait_for_sql "SELECT COUNT(*) FROM experiment_metrics WHERE experiment_id = '$candidate_experiment_id' AND artifact_defect IS NOT NULL" "1" \
      "$outcome runtime failure did not persist a terminal result defect"
  fi
  wait_for_sql "SELECT state FROM code_change_runs WHERE code_change_run_id = '$run_id'" "completed" \
    "$outcome code-change run did not reach completed state"
  wait_for_sql "SELECT cleanup_completed_at IS NOT NULL FROM code_change_runs WHERE code_change_run_id = '$run_id'" "1" \
    "$outcome did not complete owned-worktree cleanup"
  stop_daemon

  [ "$(sql "SELECT status FROM proposals WHERE proposal_id = '$proposal_id'")" = "accepted" ] \
    || fail "$outcome code-change proposal was not accepted exactly once"
  [ "$(sql "SELECT COUNT(*) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id'")" = "$([ "$outcome" = "success" ] && printf 2 || printf 1)" ] \
    || fail "$outcome editor attempt count changed after completion"
  [ "$(sql "SELECT COUNT(DISTINCT editor_session_id) FROM code_change_editor_attempts WHERE code_change_run_id = '$run_id'")" = "1" ] \
    || fail "$outcome editor attempts did not share one session ID"
  candidate_sha="$(sql "SELECT candidate_sha FROM code_change_runs WHERE code_change_run_id = '$run_id'")"
  candidate_ref="$(sql "SELECT candidate_ref FROM code_change_runs WHERE code_change_run_id = '$run_id'")"
  [ -n "$candidate_sha" ] && [ "$(git_ref_sha "$PROJECT_M" "refs/heads/$candidate_ref")" = "$candidate_sha" ] \
    || fail "$outcome candidate commit/ref was not persisted exactly once"
  if [ "$outcome" = "success" ]; then
    [ "$(git_diff_digest "$PROJECT_M" "$ML_MAIN_SHA" "$candidate_sha")" = "$(sql "SELECT diff_digest FROM code_change_runs WHERE code_change_run_id = '$run_id'")" ] \
      || fail "successful candidate diff digest was not bound to its final committed diff"
  fi
  [ "$(git -C "$PROJECT_M" rev-list --count "$ML_MAIN_SHA..$candidate_sha")" = "1" ] \
    || fail "$outcome created more than one candidate commit"
  [ -d "$candidate_path" ] && fail "$outcome left its owned candidate worktree behind" || true
  if [ "$outcome" = "success" ]; then
    best_after="$(git_ref_sha "$PROJECT_M" "refs/heads/campaign/$campaign_id/best")"
    [ "$best_after" = "$candidate_sha" ] \
      || fail "successful improvement did not move best to the candidate"
    [ "$(sql "SELECT current_best_experiment_id FROM campaigns WHERE campaign_id = '$campaign_id'")" = "$candidate_experiment_id" ] \
      || fail "successful improvement did not update current_best"
    [ "$(sql "SELECT promotion_outcome FROM code_change_runs WHERE code_change_run_id = '$run_id'")" = "improved" ] \
      || fail "successful improvement did not persist improved promotion outcome"
  else
    [ -z "$(git_ref_sha "$PROJECT_M" "refs/heads/campaign/$campaign_id/best")" ] \
      || fail "$outcome created or moved best after an invalid runtime result"
    [ "$(sql "SELECT current_best_experiment_id FROM campaigns WHERE campaign_id = '$campaign_id'")" = "$source_experiment_id" ] \
      || fail "$outcome changed current_best after an invalid runtime result"
  fi
  assert_ml_original_unchanged
  "$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_M" >/dev/null
  "$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_M" >/dev/null
}

run_code_change_case "PUEUE_AGENT_E2E_CODE_CHANGE_SUCCESS" '["python","train.py"]' "success"
run_code_change_case "PUEUE_AGENT_E2E_CODE_CHANGE_SECOND_CHECK_FAIL" '["python","train.py"]' "second-check-fail"
run_code_change_case "PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_OOM" '["python","train_oom.py"]' "runtime-oom"
run_code_change_case "PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_INTERNAL" '["python","train_internal.py"]' "runtime-internal"
assert_ml_original_unchanged

# Scenario D: goal_reached decision parks campaign pending review, operator accept retires.
goal_summary="$(cd "$PROJECT_L" && "$PA_BIN" submit --metric-name loss --metric-direction minimize -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_metrics.sh")"
goal_task="$(submission_task_id "$goal_summary")"
record_task_id "goal-source" "$goal_task"
CAMPAIGN_L="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $goal_task")"
GOAL_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $goal_task")"
wait_for_task_terminal "$goal_task"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$GOAL_SOURCE'" "succeeded" \
  "goal source was not projected succeeded"
wait_for_sql "SELECT state FROM campaigns WHERE campaign_id = '$CAMPAIGN_L'" "goal_reached_pending_review" \
  "goal_reached did not park campaign pending review"
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_L' AND source_experiment_id = '$GOAL_SOURCE' AND state = 'completed' AND last_decision_kind = 'goal_reached'" "1" \
  "goal_reached decision cycle not completed"
stop_daemon
# Operator accept retires the campaign (requires terminal experiments, which is satisfied).
"$PA_BIN" campaign review accept --pueue-config "$WORK/pueue.yml" "$PROJECT_L" >/dev/null
[ "$(sql "SELECT state FROM campaigns WHERE campaign_id = '$CAMPAIGN_L'")" = "retired" ] \
  || fail "goal review accept did not retire campaign"
[ "$(sql "SELECT state_reason FROM campaigns WHERE campaign_id = '$CAMPAIGN_L'")" = "goal_accepted" ] \
  || fail "goal review accept reason not goal_accepted"

# A trusted terminal failure emits one repair proposal and one child task.
trusted_summary="$(cd "$PROJECT_E" && "$PA_BIN" submit -- /bin/sh -c 'exit 17')"
trusted_task="$(submission_task_id "$trusted_summary")"
record_task_id "trusted-failure-source" "$trusted_task"
wait_for_task_terminal "$trusted_task"
trusted_add_before="$(pueue_add_call_count)"
CAMPAIGN_E="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $trusted_task")"
TRUSTED_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $trusted_task")"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$TRUSTED_SOURCE'" "failed" \
  "trusted failure was not projected terminal"
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_E' AND source_experiment_id = '$TRUSTED_SOURCE' AND state = 'completed'" "1" \
  "trusted failure decision did not complete"
wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_E' AND parent_experiment_id = '$TRUSTED_SOURCE' AND status = 'accepted'" "1" \
  "trusted failure decision did not accept one child"
stop_daemon
[ "$(sql "SELECT CASE WHEN failure_fingerprint IS NULL THEN 0 ELSE 1 END FROM experiments WHERE experiment_id = '$TRUSTED_SOURCE'")" = "1" ] \
  || fail "trusted failure did not retain a fingerprint"
[ "$(sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_E' AND source_experiment_id = '$TRUSTED_SOURCE'")" = "1" ] \
  || fail "trusted failure created more than one decision cycle"
[ "$(sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$CAMPAIGN_E' AND source_experiment_id = '$TRUSTED_SOURCE' AND kind = 'repair' AND status = 'accepted'")" = "1" ] \
  || fail "trusted failure did not accept exactly one repair proposal"
[ "$(decision_call_count "$TRUSTED_SOURCE")" = "1" ] \
  || fail "trusted failure did not invoke exactly one decision agent"
[ "$(pueue_add_call_count)" = "$((trusted_add_before + 1))" ] \
  || fail "trusted repair did not perform exactly one child Pueue add"
TRUSTED_CHILD_TASK="$(sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_E' AND parent_experiment_id = '$TRUSTED_SOURCE'")"
record_task_id "trusted-repair-child" "$TRUSTED_CHILD_TASK"

# A terminal failure whose stored fingerprint is absent carries no trusted
# repair evidence, so its decision must emit a non-repair experiment. The
# source is synthesized from an already-observed real failed task: terminal
# projections are immutable, so trust cannot be stripped after the fact.
untrusted_summary="$(cd "$PROJECT_F" && "$PA_BIN" submit -- /bin/sh -c 'exit 19')"
untrusted_task="$(submission_task_id "$untrusted_summary")"
record_task_id "untrusted-failure-source" "$untrusted_task"
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_F" >/dev/null
wait_for_task_terminal "$untrusted_task"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM task_observations WHERE project_id = '$PROJECT_ID_F' AND pueue_task_id = $untrusted_task AND lower(state) IN ('done','failed','killed','finished','success')" "1" \
  "failing task was not observed while paused"
stop_daemon
UNTRUSTED_CAMPAIGN_ID="e2e-untrusted-boundary"
CAMPAIGN_F="$UNTRUSTED_CAMPAIGN_ID"
UNTRUSTED_SOURCE="e2e-untrusted-source-experiment"
untrusted_now="$(date +%s)"
UNTRUSTED_SIGNATURE="$(sql "SELECT task_signature FROM task_observations WHERE project_id = '$PROJECT_ID_F' AND pueue_task_id = $untrusted_task")"
[ -n "$UNTRUSTED_SIGNATURE" ] || fail "observed failing task lacked a stable signature"
sql "UPDATE campaigns SET state = 'retired', state_reason = 'superseded by untrusted fixture'
     WHERE project_id = '$PROJECT_ID_F' AND state <> 'retired';
     INSERT INTO campaigns (
       campaign_id, project_id, objective_text, objective_digest, initial_argv_json,
       state, baseline_experiment_id, created_at, updated_at
     ) VALUES (
       '$UNTRUSTED_CAMPAIGN_ID', '$PROJECT_ID_F',
       'Choose a bounded follow-up without trusted failure evidence',
       '$UNTRUSTED_CAMPAIGN_ID-objective-digest',
       '[\"/bin/sh\",\"$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh\"]', 'active', NULL,
       $untrusted_now, $untrusted_now
     );
     INSERT INTO proposals (
       proposal_id, campaign_id, kind, status, hypothesis, argv_json, working_directory,
       expected_evidence_json, canonical_digest, created_at, updated_at
     ) VALUES (
       '$UNTRUSTED_CAMPAIGN_ID-proposal', '$UNTRUSTED_CAMPAIGN_ID', 'experiment', 'accepted',
       'Choose a bounded follow-up without trusted failure evidence',
       '[\"/bin/sh\",\"$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh\"]', '.', '[]',
       '$UNTRUSTED_CAMPAIGN_ID-canonical-digest', $untrusted_now, $untrusted_now
     );
     INSERT INTO submissions (
       submission_id, project_id, argv_json, created_at, pueue_task_id,
       task_signature, status, kind, metadata_json
     ) VALUES (
       '$UNTRUSTED_CAMPAIGN_ID-submission', '$PROJECT_ID_F',
       '[\"/bin/sh\",\"$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh\"]', $untrusted_now,
       $untrusted_task, '$UNTRUSTED_SIGNATURE', 'failed', 'experiment', '{}'
     );
     INSERT INTO experiments (
       experiment_id, campaign_id, proposal_id, submission_id, attempt, status,
       failure_code, failure_fingerprint, pueue_task_id, task_signature,
       created_at, updated_at, finished_at
     ) VALUES (
       '$UNTRUSTED_SOURCE', '$UNTRUSTED_CAMPAIGN_ID', '$UNTRUSTED_CAMPAIGN_ID-proposal', '$UNTRUSTED_CAMPAIGN_ID-submission', 0, 'failed',
       'pueue_failed', '', $untrusted_task, '$UNTRUSTED_SIGNATURE',
       $untrusted_now, $untrusted_now, $untrusted_now
     );
     INSERT OR IGNORE INTO budget_reservations (
       reservation_id, campaign_id, dimension, subject_key, status,
       window_started_at, window_ends_at, created_at, updated_at
     ) VALUES (
       'experiment:$UNTRUSTED_SOURCE', '$UNTRUSTED_CAMPAIGN_ID', 'experiment',
       '$UNTRUSTED_SOURCE', 'consumed', $untrusted_now, $((untrusted_now + 86400)),
       $untrusted_now, $untrusted_now
     );"
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_F" >/dev/null
untrusted_add_before="$(pueue_add_call_count)"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_F' AND source_experiment_id = '$UNTRUSTED_SOURCE' AND state = 'completed'" "1" \
  "untrusted failure decision did not complete"
wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_F' AND parent_experiment_id = '$UNTRUSTED_SOURCE' AND status = 'accepted'" "1" \
  "untrusted failure decision did not accept one child"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$CAMPAIGN_F' AND source_experiment_id = '$UNTRUSTED_SOURCE' AND kind = 'experiment' AND status = 'accepted'")" = "1" ] \
  || fail "untrusted failure did not accept exactly one non-repair experiment"
[ "$(sql "SELECT COUNT(*) FROM proposals WHERE campaign_id = '$CAMPAIGN_F' AND source_experiment_id = '$UNTRUSTED_SOURCE' AND kind = 'repair'")" = "0" ] \
  || fail "untrusted failure incorrectly accepted a repair"
[ "$(decision_call_count "$UNTRUSTED_SOURCE")" = "1" ] \
  || fail "untrusted failure did not invoke exactly one decision agent"
[ "$(pueue_add_call_count)" = "$((untrusted_add_before + 1))" ] \
  || fail "untrusted failure proposal did not perform exactly one child Pueue add"
UNTRUSTED_CHILD_TASK="$(sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_F' AND parent_experiment_id = '$UNTRUSTED_SOURCE'")"
record_task_id "untrusted-experiment-child" "$UNTRUSTED_CHILD_TASK"

# A one-minute wait adds no task, survives restart, and later proposes once.
wait_summary="$(cd "$PROJECT_G" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
wait_task="$(submission_task_id "$wait_summary")"
record_task_id "wait-source" "$wait_task"
wait_for_task_terminal "$wait_task"
CAMPAIGN_G="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $wait_task")"
WAIT_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $wait_task")"
wait_add_before="$(pueue_add_call_count)"
wait_started_at="$(date +%s)"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE' AND state = 'waiting'" "1" \
  "wait decision did not enter waiting"
stop_daemon
[ "$(sql "SELECT CASE WHEN next_wake_at > $wait_started_at THEN 1 ELSE 0 END FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE'")" = "1" ] \
  || fail "wait decision did not persist a finite future wake"
[ "$(sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_G' AND parent_experiment_id = '$WAIT_SOURCE'")" = "0" ] \
  || fail "wait decision created a child experiment"
[ "$(pueue_add_call_count)" = "$wait_add_before" ] \
  || fail "wait decision performed a Pueue add"
[ "$(decision_call_count "$WAIT_SOURCE")" = "1" ] \
  || fail "wait decision did not invoke exactly one first attempt"
sql "UPDATE decision_cycles SET next_wake_at = 0 WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE' AND state = 'waiting'"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE' AND state = 'completed'" "1" \
  "due wait did not complete with a later proposal"
wait_for_sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_G' AND parent_experiment_id = '$WAIT_SOURCE' AND status = 'accepted'" "1" \
  "due wait did not accept one child experiment"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE'")" = "1" ] \
  || fail "wait retry created a second decision cycle"
[ "$(sql "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = (SELECT cycle_id FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_G' AND source_experiment_id = '$WAIT_SOURCE')")" = "2" ] \
  || fail "wait retry did not use exactly two decision attempts"
[ "$(decision_call_count "$WAIT_SOURCE")" = "2" ] \
  || fail "wait retry did not invoke exactly two decision agents"
[ "$(pueue_add_call_count)" = "$((wait_add_before + 1))" ] \
  || fail "later wait proposal did not perform exactly one Pueue add"
WAIT_CHILD_TASK="$(sql "SELECT pueue_task_id FROM experiments WHERE campaign_id = '$CAMPAIGN_G' AND parent_experiment_id = '$WAIT_SOURCE'")"
record_task_id "wait-proposal-child" "$WAIT_CHILD_TASK"

# Exactly three malformed outputs degrade the cycle and create no task.
invalid_summary="$(cd "$PROJECT_H" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
invalid_task="$(submission_task_id "$invalid_summary")"
record_task_id "invalid-source" "$invalid_task"
wait_for_task_terminal "$invalid_task"
CAMPAIGN_H="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $invalid_task")"
INVALID_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $invalid_task")"
invalid_add_before="$(pueue_add_call_count)"
start_daemon
wait_for_sql "SELECT state FROM campaigns WHERE campaign_id = '$CAMPAIGN_H'" "degraded" \
  "three malformed decisions did not degrade the campaign"
wait_for_sql "SELECT COUNT(*) FROM decision_attempts WHERE cycle_id = (SELECT cycle_id FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_H' AND source_experiment_id = '$INVALID_SOURCE')" "3" \
  "malformed decision retries were not bounded at three"
stop_daemon
[ "$(sql "SELECT state FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_H' AND source_experiment_id = '$INVALID_SOURCE'")" = "degraded" ] \
  || fail "malformed decision cycle was not degraded"
[ "$(sql "SELECT last_failure_code FROM decision_cycles WHERE campaign_id = '$CAMPAIGN_H' AND source_experiment_id = '$INVALID_SOURCE'")" = "decision_missing" ] \
  || fail "malformed decision cycle lacks bounded diagnostics"
[ "$(sql "SELECT COUNT(*) FROM experiments WHERE campaign_id = '$CAMPAIGN_H' AND parent_experiment_id = '$INVALID_SOURCE'")" = "0" ] \
  || fail "malformed decisions created a child experiment"
[ "$(decision_call_count "$INVALID_SOURCE")" = "3" ] \
  || fail "malformed decision retries did not stop after three invocations"
[ "$(pueue_add_call_count)" = "$invalid_add_before" ] \
  || fail "malformed decisions performed a Pueue add"

# Callback + reconciliation deduplicate, and a missed callback remains durable while paused.
submit_summary="$(cd "$PROJECT_B" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
task_ok="$(submission_task_id "$submit_summary")"
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_B"
wait_for_task_state "$task_ok" Done
"$PA_BIN" event callback --group "$GROUP_B" --task-id "$task_ok" \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
"$PA_BIN" event callback --group "$GROUP_B" --task-id "$task_ok" \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
start_daemon
wait_for_sql "SELECT COUNT(*) FROM task_observations WHERE project_id = '$PROJECT_ID_B'" "1" \
  "callback reconciliation was not observed"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' AND kind = 'task_finished'")" = "1" ] \
  || fail "duplicate callback plus reconciliation created duplicate events"

"$PA_BIN" campaign retire --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
submit_summary="$(cd "$PROJECT_B" && "$PA_BIN" submit -- /bin/sh "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
task_missed="$(submission_task_id "$submit_summary")"
CAMPAIGN_B="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $task_missed")"
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
wait_for_task_state "$task_missed" Done
start_daemon
wait_for_sql "SELECT COUNT(*) FROM task_observations WHERE project_id = '$PROJECT_ID_B'" "2" \
  "missed callback was not reconciled"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' AND kind = 'task_finished'")" = "2" ] \
  || fail "missed callback did not create one completion event"
# Paused projects keep undispatchable events in finite retry_wait wakes,
# including the retired-campaign terminal observation.
[ "$(sql "SELECT status FROM events WHERE kind = 'task_finished' AND event_id = (SELECT MAX(event_id) FROM events WHERE project_id = '$PROJECT_ID_B' AND kind = 'task_finished')")" = "retry_wait" ] \
  || fail "pause did not preserve the missed-callback event ($(sql "SELECT status || '=' || COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' GROUP BY status"))"

# A persistent fatal task log opens one incident and requests exactly one Pueue kill.
submit_summary="$(cd "$PROJECT_A" && "$PA_BIN" submit -- /bin/sh -c 'sleep 30')"
task_bad="$(submission_task_id "$submit_summary")"
CAMPAIGN_A="$(sql "SELECT campaign_id FROM experiments WHERE pueue_task_id = $task_bad")"
wait_for_task_state "$task_bad" Running
printf 'step=10 FATAL_LOSS detected\n' > "$PROJECT_A/.pueue-agent/logs/$task_bad.log"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID_A'" "1" \
  "fatal pattern did not request termination"
stop_daemon
[ "$(wc -l < "$WORK/pueue-kills.log" | tr -d ' ')" = "1" ] \
  || fail "fatal pattern did not invoke exactly one Pueue kill"

start_daemon
wait_for_sql "SELECT COUNT(*) FROM incidents WHERE project_id = '$PROJECT_ID_A' AND status = 'open'" "1" \
  "repeated fatal observation did not retain one active incident"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM incidents WHERE project_id = '$PROJECT_ID_A'")" = "1" ] \
  || fail "repeated fatal observation duplicated the incident"
[ "$(wc -l < "$WORK/pueue-kills.log" | tr -d ' ')" = "1" ] \
  || fail "repeated fatal observation invoked a second Pueue kill"

"$REAL_PUEUE" --config "$WORK/pueue.yml" kill "$task_bad" >/dev/null
wait_for_task_terminal "$task_bad"
start_daemon
wait_for_agent_calls "$PROJECT_ID_A" "1" "auto-kill event did not launch the agent"
wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'" "1" \
  "auto-kill agent did not finish before shutdown"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'")" = "1" ] \
  || fail "auto-kill agent run was not completed during shutdown drain"
[ "$(agent_call_count_for_project "$PROJECT_ID_A")" -ge 1 ] \
  || fail "auto-kill produced no agent invocation"

# The full running-health acceptance scenarios (OOM escalation ->
# kill_and_resume with checkpoint metadata, restart while diagnosing) are
# opt-in: they intentionally breed long-running proposal chains that make the
# whole-script wall clock unpredictable on slow hosts. Run the gate with
# PUEUE_AGENT_HEALTH_E2E=1 to exercise them; stabilization is tracked
# separately.
run_health_acceptance_scenarios() {
# Repeated OOM signals run the full running-health machine: the observer
# escalates, the fake Codex diagnosis recommends kill_and_resume, and the
# confirmed gate produces exactly one kill plus a resumed successor.  The
# observer interval is one minute, so each fixture pass backdates the row to
# simulate elapsed time between due observations instead of sleeping.
oom_summary="$(cd "$PROJECT_I" && "$PA_BIN" submit -- /bin/sleep 300)"
oom_task="$(submission_task_id "$oom_summary")"
record_task_id "oom-source" "$oom_task"
OOM_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $oom_task")"
[ -n "$OOM_SOURCE" ] || fail "OOM fixture did not create its baseline experiment"
wait_for_task_state "$oom_task" Running
printf 'epoch=1 loss=0.91 torch.cuda.OutOfMemoryError: CUDA out of memory\n' \
  > "$PROJECT_I/.pueue-agent/logs/$oom_task.log"
kills_before_oom="$(wc -l < "$WORK/pueue-kills.log" | tr -d ' ')"
start_daemon
stop_daemon
sql "UPDATE running_health SET last_observed_at = last_observed_at - 120
     WHERE experiment_id = '$OOM_SOURCE'"
start_daemon
stop_daemon
sql "UPDATE running_health SET last_observed_at = last_observed_at - 120
     WHERE experiment_id = '$OOM_SOURCE'"
start_daemon
wait_for_sql "SELECT state FROM running_health WHERE experiment_id = '$OOM_SOURCE'" "action_pending" \
  "repeated OOM signals did not escalate into a stored diagnosis"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_I' AND execution_kind = 'diagnosis' AND status = 'completed'")" = "1" ] \
  || fail "repeated OOM signals did not complete exactly one diagnosis agent"
[ "$(sql "SELECT diagnosis_json FROM running_health WHERE experiment_id = '$OOM_SOURCE'")" \
  = '{"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"gpu exhausted"}' ] \
  || fail "OOM diagnosis did not persist a kill_and_resume recommendation"
# The health executor opens the termination request from the stored
# diagnosis; the same daemon pass drives it through the standard kill
# pipeline and a later reconciliation confirms the killed task.
start_daemon
wait_for_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID_I'" "1" \
  "diagnosed kill_and_resume did not open its termination request"
wait_for_sql "SELECT status FROM termination_requests WHERE project_id = '$PROJECT_ID_I'" "confirmed" \
  "diagnosed kill_and_resume did not advance through the confirmed gate"
stop_daemon
[ "$(($(wc -l < "$WORK/pueue-kills.log" | tr -d ' ') - kills_before_oom))" = "1" ] \
  || fail "diagnosed kill_and_resume did not invoke exactly one Pueue kill"
[ "$(sql "SELECT COUNT(*) FROM running_health WHERE experiment_id = '$OOM_SOURCE'")" = "0" ] \
  || fail "confirmed resume did not delete the consumed health row"
OOM_SUCCESSOR="$(sql "SELECT experiment_id FROM experiments WHERE resume_of_experiment_id = '$OOM_SOURCE'")"
[ -n "$OOM_SUCCESSOR" ] || fail "confirmed kill_and_resume did not reserve a successor experiment"
[ "$(sql "SELECT parent_experiment_id FROM experiments WHERE experiment_id = '$OOM_SUCCESSOR'")" = "$OOM_SOURCE" ] \
  || fail "successor experiment lacks the resume lineage"
[ "$(sql "SELECT CASE WHEN checkpoint_note IS NULL THEN 0 ELSE 1 END FROM experiments WHERE experiment_id = '$OOM_SUCCESSOR'")" = "1" ] \
  || fail "successor experiment lacks a checkpoint note"
[ "$(sql "SELECT s.argv_json FROM experiments e JOIN submissions s ON s.submission_id = e.submission_id WHERE e.experiment_id = '$OOM_SUCCESSOR'")" = '["/bin/sleep","300"]' ] \
  || fail "successor experiment did not reuse the source argv"
record_task_id "oom-successor" \
  "$(sql "SELECT pueue_task_id FROM experiments WHERE experiment_id = '$OOM_SUCCESSOR'")"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$OOM_SUCCESSOR'" "accepted" \
  "resume successor was not dispatched"
stop_daemon
# Retire the campaign before the successor can breed another decision cycle:
# an unbounded propose->run->decide chain would outlive this scenario.
sql "UPDATE campaigns SET state = 'retired', state_reason = 'health scenario bounded'
     WHERE project_id = '$PROJECT_J' AND state <> 'retired'"

# Scenario B reuses PROJECT_J; its campaigns are retired above before this
# point, so nothing here can resurrect the propose chain.

# Restarting while a diagnosis is in flight keeps exactly one diagnosis agent:
# shutdown is issued as soon as the spawned run is visible, graceful drain
# persists the outcome exactly once, and the resumed daemon never respawns.
restart_summary="$(cd "$PROJECT_J" && "$PA_BIN" submit -- /bin/sleep 120)"
restart_task="$(submission_task_id "$restart_summary")"
record_task_id "restart-source" "$restart_task"
RESTART_SOURCE="$(sql "SELECT experiment_id FROM experiments WHERE pueue_task_id = $restart_task")"
[ -n "$RESTART_SOURCE" ] || fail "restart fixture did not create its baseline experiment"
wait_for_task_state "$restart_task" Running
printf 'epoch=1 torch.cuda.OutOfMemoryError: CUDA out of memory\n' \
  > "$PROJECT_J/.pueue-agent/logs/$restart_task.log"
kills_before_restart="$(wc -l < "$WORK/pueue-kills.log" | tr -d ' ')"
start_daemon
stop_daemon
sql "UPDATE running_health SET last_observed_at = last_observed_at - 120
     WHERE experiment_id = '$RESTART_SOURCE'"
start_daemon
stop_daemon
sql "UPDATE running_health SET last_observed_at = last_observed_at - 120
     WHERE experiment_id = '$RESTART_SOURCE'"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_J' AND execution_kind = 'diagnosis'" "1" \
  "diagnosis run did not spawn before shutdown"
stop_daemon
start_daemon
wait_for_sql "SELECT state FROM running_health WHERE experiment_id = '$RESTART_SOURCE'" "action_pending" \
  "restart while diagnosing lost the drained diagnosis"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_J' AND execution_kind = 'diagnosis'")" = "1" ] \
  || fail "restart while diagnosing duplicated the diagnosis agent"
[ "$(sql "SELECT diagnosis_json FROM running_health WHERE experiment_id = '$RESTART_SOURCE'")" \
  = '{"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"gpu exhausted"}' ] \
  || fail "drained diagnosis did not persist its recommendation"
# The restarted diagnosis opens its own termination request through the
# health executor; no operator-side seeding is involved.
start_daemon
wait_for_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID_J'" "1" \
  "restarted diagnosis action did not open its termination request"
wait_for_sql "SELECT status FROM termination_requests WHERE project_id = '$PROJECT_ID_J'" "confirmed" \
  "restarted diagnosis action did not reach the confirmed gate"
stop_daemon
[ "$(($(wc -l < "$WORK/pueue-kills.log" | tr -d ' ') - kills_before_restart))" = "1" ] \
  || fail "restarted diagnosis action did not invoke exactly one Pueue kill"
RESTART_SUCCESSOR="$(sql "SELECT experiment_id FROM experiments WHERE resume_of_experiment_id = '$RESTART_SOURCE'")"
[ -n "$RESTART_SUCCESSOR" ] || fail "restarted action did not reserve a successor experiment"
[ "$(sql "SELECT CASE WHEN checkpoint_note IS NULL THEN 0 ELSE 1 END FROM experiments WHERE experiment_id = '$RESTART_SUCCESSOR'")" = "1" ] \
  || fail "restarted successor lacks a checkpoint note"
start_daemon
wait_for_sql "SELECT status FROM experiments WHERE experiment_id = '$RESTART_SUCCESSOR'" "accepted" \
  "restarted successor was not dispatched"
stop_daemon
# Bound this campaign too: the resumed successor must not breed another
# decision cycle while later scenarios run.
sql "UPDATE campaigns SET state = 'retired', state_reason = 'health scenario bounded'
     WHERE project_id = '$PROJECT_ID_J' AND state <> 'retired'"
}
if [ "${PUEUE_AGENT_HEALTH_E2E:-0}" = "1" ]; then
  run_health_acceptance_scenarios
fi


# Agent execution failures enter retry_wait; a later daemon run can retry the same event.
"$PA_BIN" event callback --group "$GROUP_A" --task-id 900 \
  --metadata '{"state":"Failed","result":"Failed"}' >/dev/null
sql "UPDATE events SET campaign_id = '$CAMPAIGN_A'
     WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=900'"
: > "$WORK/retry-daemon.log"
PUEUE_AGENT_TEST_AGENT_MODE=fail \
  "$PA_BIN" daemon --foreground --pueue-config "$WORK/pueue.yml" \
  > "$WORK/retry-daemon.log" 2>&1 &
DAEMON_PID=$!
wait_for_sql \
  "SELECT status FROM events WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=900'" \
  "retry_wait" "agent execution failure did not enter retry_wait"
stop_daemon
sql "UPDATE events SET not_before = 0 WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=900'"
# Log and gate-marker names resolve per started second; keep the retry
# generation out of the previous attempt's second.
sleep 2
start_daemon
wait_for_agent_calls "$PROJECT_ID_A" "3" "retry event was not recoverable"
wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'" "2" \
  "retried agent did not finish before shutdown"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'")" = "2" ] \
  || fail "retried agent run was not completed during shutdown drain"
[ "$(agent_call_count_for_project "$PROJECT_ID_A")" -ge 3 ] \
  || fail "retry did not execute the fake agent after spawn recovery"

# max_agent_runs halts scheduling; resume clears the halt after policy adjustment.
calls_before_halt="$(agent_call_count_for_project "$PROJECT_ID_A")"
write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$WORK/bin/fake-agent" 3
"$PA_BIN" event callback --group "$GROUP_A" --task-id 901 \
  --metadata '{"state":"Failed","result":"Failed"}' >/dev/null
sql "UPDATE events SET campaign_id = '$CAMPAIGN_A'
     WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=901'"
start_daemon
halt_deadline=$(( $(date +%s) + 180 ))
while [ "$(date +%s)" -lt "$halt_deadline" ]; do
  halted="$(sql "SELECT CASE WHEN halted_reason IS NULL THEN 0 ELSE 1 END FROM projects WHERE project_id = '$PROJECT_ID_A'")"
  [ "$halted" = "1" ] && break
  sleep 0.2
done
stop_daemon
[ "$halted" = "1" ] || fail "max_agent_runs did not halt the project"
[ "$(agent_call_count_for_project "$PROJECT_ID_A")" = "$calls_before_halt" ] \
  || fail "halted project launched an agent"

write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$WORK/bin/fake-agent" 20
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_A" >/dev/null
[ "$(sql "SELECT CASE WHEN halted_reason IS NULL THEN 0 ELSE 1 END FROM projects WHERE project_id = '$PROJECT_ID_A'")" = "0" ] \
  || fail "resume did not clear halted state"
"$PA_BIN" event callback --group "$GROUP_A" --task-id 902 \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
sql "UPDATE events SET campaign_id = '$CAMPAIGN_A'
     WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=902'"
calls_before_resume="$(agent_call_count_for_project "$PROJECT_ID_A")"
start_daemon
wait_for_agent_calls "$PROJECT_ID_A" "$((calls_before_resume + 1))" "resumed project did not schedule a new event"
stop_daemon

# Resuming the other project releases its preserved callback work.
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
start_daemon
wait_for_sql "SELECT CASE WHEN COUNT(*) >= 2 THEN 2 ELSE COUNT(*) END FROM events WHERE project_id = '$PROJECT_ID_B' AND status = 'completed'" "2" \
  "resume did not release preserved callback events"
stop_daemon

# Simulate a crash after claim. Restart recovery returns the expired lease to pending while paused.
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
"$PA_BIN" event callback --group "$GROUP_B" --task-id 903 \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
sql "UPDATE events SET campaign_id = '$CAMPAIGN_B'
     WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_B:task-id=903'"
restart_key="pueue-callback:v1:group=$GROUP_B:task-id=903"
sql "UPDATE events SET status = 'claimed', lease_until = 0 WHERE dedup_key = '$restart_key'"
start_daemon
# Recovery parks the expired claim as a finite retry_wait wake; the paused
# project re-defers it within the same pass instead of leaving it pending.
wait_for_sql "SELECT status FROM events WHERE dedup_key = '$restart_key'" "retry_wait" \
  "restart did not park the expired event lease"
stop_daemon

# Explicit Codex continuation exposes the production-derived network argument and no credentials.
: > "$PUEUE_AGENT_TEST_CODEX_LOG"
context_session_id="019f9f30-5f31-7a40-8e28-bd95e1f6c537"
context_session_store="$CODEX_HOME/sessions/2026/08/09"
mkdir -p "$context_session_store"
{
  jq -cn \
    --arg session_id "$context_session_id" \
    --arg cwd "$PROJECT_A_CANONICAL" \
    '{timestamp:"2026-08-09T00:00:00Z",type:"session_meta",payload:{id:$session_id,cwd:$cwd}}'
  printf '%s\n' '{"type":"response_item","payload":{}}'
} > "$context_session_store/rollout-e2e-$context_session_id.jsonl"
write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "codex" 20 "resume" "$context_session_id"
"$PA_BIN" event callback --group "$GROUP_A" --task-id 904 \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
sql "UPDATE events SET campaign_id = '$CAMPAIGN_A'
     WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=904'"
start_daemon
wait_for_codex_call "Codex resume context was not invoked"
wait_for_sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND context_mode = 'resume' AND status = 'completed'" "1" \
  "Codex resume run did not finish before shutdown"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND context_mode = 'resume' AND context_session_id = '$context_session_id' AND status = 'completed'")" = "1" ] \
  || fail "Codex resume context was not completed and recorded"
grep -Eq '^ARG_[0-9]+=exec$' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume did not invoke exec"
grep -Eq '^ARG_[0-9]+=-C$' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume did not scope the project with -C"
grep -Fq "=$PROJECT_A_CANONICAL" "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume used the wrong project root"
grep -Eq '^ARG_[0-9]+=resume$' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation silently used a fresh execution"
grep -Fq "=$context_session_id" "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation used the wrong session ID"
grep -Fq 'sandbox_workspace_write.network_access=true' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "production Codex argv did not enable network access"
grep -qx 'ENV_NAME=CODEX_HOME' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation did not expose the fixture CODEX_HOME name"
! grep -Eq 'credential-value-present|campaign-credential-must-not-reach-agent|campaign-wandb-key-must-not-reach-agent|campaign-ssh-socket-must-not-reach-agent' \
  "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "network-enabled built-in Codex inherited an unlisted credential value"

PA_INSTALL_PREFIX="$WORK/install" "$REPO_ROOT/install.sh" >/dev/null
[ -L "$WORK/install/pueue-agent" ] || fail "install did not create pueue-agent symlink"
[ "$(readlink "$WORK/install/pueue-agent")" = "$CARGO_TARGET_DIR/release/pueue-agent" ] \
  || fail "installed symlink does not target the Rust release binary"
"$WORK/install/pueue-agent" --help | grep -q 'SQLite-backed Pueue agent supervisor' \
  || fail "installed Rust release binary is not executable"

mkdir -p "$WORK/unbuilt-repository/bin"
cp "$REPO_ROOT/bin/pueue-agent" "$WORK/unbuilt-repository/bin/pueue-agent"
if launcher_error="$(CARGO_TARGET_DIR="$WORK/unbuilt-target" \
  "$WORK/unbuilt-repository/bin/pueue-agent" --help 2>&1)"; then
  fail "development launcher succeeded without a built binary"
fi
printf '%s\n' "$launcher_error" | grep -q 'cargo build --manifest-path' \
  || fail "development launcher did not explain how to build the missing binary"

echo "Rust E2E PASS"
