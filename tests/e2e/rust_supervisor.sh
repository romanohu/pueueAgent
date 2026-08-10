#!/usr/bin/env bash
set -eu

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PA_BIN="$REPO_ROOT/bin/pueue-agent"
REAL_PUEUE="$(command -v pueue)"
REAL_PUEUED="$(command -v pueued)"
ORIGINAL_HOME="${HOME:?HOME is required}"
WORK="$(mktemp -d /tmp/pa-rust-e2e.XXXXXX)"
DAEMON_PID=""

fail() {
  echo "Rust E2E FAIL: $*" >&2
  exit 1
}

cleanup() {
  if [ -n "$DAEMON_PID" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
    kill -TERM "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  "$REAL_PUEUE" --config "$WORK/pueue.yml" shutdown >/dev/null 2>&1 || true
  if [ -f "$WORK/pueue/pueue.pid" ]; then
    pueue_pid="$(cat "$WORK/pueue/pueue.pid" 2>/dev/null || true)"
    if [ -n "$pueue_pid" ]; then
      for _ in $(seq 50); do
        kill -0 "$pueue_pid" 2>/dev/null || break
        sleep 0.1
      done
    fi
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

sql() {
  sqlite3 "$STATE_DB" "$1"
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
  for _ in $(seq 100); do
    actual="$(sql "$query")"
    if [ "$actual" = "$expected" ]; then
      return 0
    fi
    if [ -n "$DAEMON_PID" ] && ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      wait "$DAEMON_PID" || true
      fail "$label (daemon exited early; see $WORK/daemon.log)"
    fi
    sleep 0.1
  done
  fail "$label (expected $expected, got $(sql "$query"))"
}

wait_for_agent_calls() {
  expected="$1"
  label="$2"
  for _ in $(seq 100); do
    actual=0
    if [ -f "$PUEUE_AGENT_TEST_AGENT_LOG" ]; then
      actual="$(grep -c '^CALL ' "$PUEUE_AGENT_TEST_AGENT_LOG" || true)"
    fi
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
  for _ in $(seq 100); do
    if [ -s "$PUEUE_AGENT_TEST_CODEX_LOG" ]; then
      return 0
    fi
    if [ -n "$DAEMON_PID" ] && ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      wait "$DAEMON_PID" || true
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
    fail "daemon shutdown failed; see $WORK/daemon.log"
  fi
  DAEMON_PID=""
}

wait_for_task_state() {
  task_id="$1"
  wanted="$2"
  for _ in $(seq 100); do
    state="$($REAL_PUEUE --config "$WORK/pueue.yml" status --json \
      | jq -r --arg id "$task_id" '.tasks[$id].status | keys[0]' 2>/dev/null || true)"
    if [ "$state" = "$wanted" ]; then
      return 0
    fi
    sleep 0.1
  done
  fail "task $task_id did not reach $wanted"
}

wait_for_task_terminal() {
  task_id="$1"
  for _ in $(seq 100); do
    state="$($REAL_PUEUE --config "$WORK/pueue.yml" status --json \
      | jq -r --arg id "$task_id" '.tasks[$id].status | keys[0]' 2>/dev/null || true)"
    if [ "$state" != "Running" ] && [ "$state" != "Queued" ] && [ "$state" != "Stashed" ] && [ "$state" != "null" ]; then
      return 0
    fi
    sleep 0.1
  done
  fail "task $task_id did not become terminal"
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
timeout_minutes = 1
max_retries = 2

[agent.context]
mode = "$context_mode"
$context_session_line

[check]
interval_minutes = 1
deep_check_every = 100
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

export HOME="$WORK/home"
export CARGO_HOME="${CARGO_HOME:-$ORIGINAL_HOME/.cargo}"
export CODEX_HOME="$WORK/codex-home"
export XDG_STATE_HOME="$WORK/state"
export PUEUE_AGENT_STATE_DIR="$WORK/state/pueue-agent"
export PUEUE_CONFIG_PATH="$WORK/pueue.yml"
export PUEUE_AGENT_TEST_AGENT_LOG="$WORK/agent-calls.log"
export PUEUE_AGENT_TEST_AGENT_STATE="$WORK/agent-state"
export PUEUE_AGENT_TEST_CODEX_LOG="$WORK/codex-calls.log"
export PUEUE_AGENT_E2E_REAL_PUEUE="$REAL_PUEUE"
export PUEUE_AGENT_E2E_KILL_LOG="$WORK/pueue-kills.log"
export PUEUE_AGENT_E2E_DEFER_KILL=1
mkdir -p "$HOME" "$CODEX_HOME" "$WORK/bin" "$WORK/pueue"

cat > "$WORK/bin/pueue" <<'EOF'
#!/usr/bin/env bash
set -eu
for argument in "$@"; do
  if [ "$argument" = "kill" ]; then
    printf '%s\n' "$*" >> "$PUEUE_AGENT_E2E_KILL_LOG"
    if [ "${PUEUE_AGENT_E2E_DEFER_KILL:-0}" = "1" ]; then
      exit 0
    fi
    break
  fi
done
exec "$PUEUE_AGENT_E2E_REAL_PUEUE" "$@"
EOF
cat > "$WORK/bin/launchctl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$WORK/bin/systemctl" <<'EOF'
#!/usr/bin/env bash
if [ "${3:-}" = "is-active" ] || [ "${2:-}" = "is-active" ]; then
  echo active
fi
exit 0
EOF
chmod +x "$WORK/bin/pueue" "$WORK/bin/launchctl" "$WORK/bin/systemctl"
ln -s "$REPO_ROOT/tests/support/fake_codex.sh" "$WORK/bin/codex"
export PATH="$WORK/bin:$PATH"

cat > "$WORK/pueue.yml" <<EOF
shared:
  pueue_directory: "$WORK/pueue"
  use_unix_socket: true
  unix_socket_path: "$WORK/pueue.socket"
daemon:
  callback: null
EOF

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

PROJECT_A="$WORK/a/shared"
PROJECT_B="$WORK/b/shared"
mkdir -p "$PROJECT_A" "$PROJECT_B"
PROJECT_A_CANONICAL="$(cd "$PROJECT_A" && pwd -P)"
"$PA_BIN" init "$PROJECT_A"
"$PA_BIN" init "$PROJECT_B"

CONFIG_A="$PROJECT_A/.pueue-agent/config.toml"
CONFIG_B="$PROJECT_B/.pueue-agent/config.toml"
[ -f "$CONFIG_A" ] && [ -f "$CONFIG_B" ] || fail "init did not create TOML configuration"
PROJECT_ID_A="$(toml_value project_id "$CONFIG_A")"
PROJECT_ID_B="$(toml_value project_id "$CONFIG_B")"
GROUP_A="$(toml_value pueue_group "$CONFIG_A")"
GROUP_B="$(toml_value pueue_group "$CONFIG_B")"
[ "$PROJECT_ID_A" != "$PROJECT_ID_B" ] || fail "same-basename projects reused project_id"
[ "$GROUP_A" != "$GROUP_B" ] || fail "same-basename projects reused Pueue group"

write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$REPO_ROOT/tests/support/fake_agent.sh" 20
write_config "$PROJECT_B" "$PROJECT_ID_B" "$GROUP_B" "$REPO_ROOT/tests/support/fake_agent.sh" 20

"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_A"
"$PA_BIN" enable --pueue-config "$WORK/pueue.yml" "$PROJECT_B"
STATE_DB="$XDG_STATE_HOME/pueue-agent/state.sqlite3"
[ "$(sql 'SELECT COUNT(*) FROM projects')" = "2" ] || fail "projects were not registered"

# Healthy monitoring performs reconciliation without spending agent tokens.
start_daemon
sleep 0.2
stop_daemon
[ ! -f "$PUEUE_AGENT_TEST_AGENT_LOG" ] || fail "healthy monitoring started an agent"

# Callback + reconciliation deduplicate, and a missed callback remains durable while paused.
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_B"
submit_summary="$(cd "$PROJECT_B" && "$PA_BIN" submit -- "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
task_ok="$(submission_task_id "$submit_summary")"
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

submit_summary="$(cd "$PROJECT_B" && "$PA_BIN" submit -- "$REPO_ROOT/tests/e2e/fake_experiments/train_ok.sh")"
task_missed="$(submission_task_id "$submit_summary")"
wait_for_task_state "$task_missed" Done
start_daemon
wait_for_sql "SELECT COUNT(*) FROM task_observations WHERE project_id = '$PROJECT_ID_B'" "2" \
  "missed callback was not reconciled"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' AND kind = 'task_finished'")" = "2" ] \
  || fail "missed callback did not create one completion event"
[ "$(sql "SELECT COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' AND status = 'pending'")" = "2" ] \
  || fail "pause did not preserve pending callback events"

# A persistent fatal task log opens one incident and requests exactly one Pueue kill.
submit_summary="$(cd "$PROJECT_A" && "$PA_BIN" submit -- /bin/sh -c 'sleep 30')"
task_bad="$(submission_task_id "$submit_summary")"
wait_for_task_state "$task_bad" Running
printf 'step=10 FATAL_LOSS detected\n' > "$PROJECT_A/.pueue-agent/logs/$task_bad.log"
start_daemon
wait_for_sql "SELECT COUNT(*) FROM termination_requests WHERE project_id = '$PROJECT_ID_A'" "1" \
  "fatal pattern did not request termination"
stop_daemon
[ "$(wc -l < "$PUEUE_AGENT_E2E_KILL_LOG" | tr -d ' ')" = "1" ] \
  || fail "fatal pattern did not invoke exactly one Pueue kill"

start_daemon
wait_for_sql "SELECT COUNT(*) FROM incidents WHERE project_id = '$PROJECT_ID_A' AND status = 'open'" "1" \
  "repeated fatal observation did not retain one active incident"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM incidents WHERE project_id = '$PROJECT_ID_A'")" = "1" ] \
  || fail "repeated fatal observation duplicated the incident"
[ "$(wc -l < "$PUEUE_AGENT_E2E_KILL_LOG" | tr -d ' ')" = "1" ] \
  || fail "repeated fatal observation invoked a second Pueue kill"

PUEUE_AGENT_E2E_DEFER_KILL=0 "$REAL_PUEUE" --config "$WORK/pueue.yml" kill "$task_bad" >/dev/null
wait_for_task_terminal "$task_bad"
start_daemon
wait_for_agent_calls "1" "auto-kill event did not launch the agent"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'")" = "1" ] \
  || fail "auto-kill agent run was not completed during shutdown drain"
[ "$(grep -c '^CALL ' "$PUEUE_AGENT_TEST_AGENT_LOG")" = "1" ] \
  || fail "auto-kill should produce exactly one agent invocation"

# Spawn failures enter retry_wait; a later daemon run can retry the same event.
write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$WORK/missing-agent" 20
"$PA_BIN" event callback --group "$GROUP_A" --task-id 900 \
  --metadata '{"state":"Failed","result":"Failed"}' >/dev/null
if "$PA_BIN" daemon --foreground --pueue-config "$WORK/pueue.yml" \
  > "$WORK/retry-daemon.log" 2>&1; then
  fail "missing agent executable should fail the daemon cycle"
fi
[ "$(sql "SELECT status FROM events WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=900'")" = "retry_wait" ] \
  || fail "agent spawn failure did not enter retry_wait"
write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$REPO_ROOT/tests/support/fake_agent.sh" 20
sql "UPDATE events SET not_before = 0 WHERE dedup_key = 'pueue-callback:v1:group=$GROUP_A:task-id=900'"
start_daemon
wait_for_agent_calls "2" "retry event was not recoverable"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND status = 'completed'")" = "2" ] \
  || fail "retried agent run was not completed during shutdown drain"
[ "$(grep -c '^CALL ' "$PUEUE_AGENT_TEST_AGENT_LOG")" = "2" ] \
  || fail "retry should execute the fake agent once after spawn recovery"

# max_agent_runs halts scheduling; resume clears the halt after policy adjustment.
write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$REPO_ROOT/tests/support/fake_agent.sh" 3
"$PA_BIN" event callback --group "$GROUP_A" --task-id 901 \
  --metadata '{"state":"Failed","result":"Failed"}' >/dev/null
start_daemon
for _ in $(seq 100); do
  halted="$(sql "SELECT CASE WHEN halted_reason IS NULL THEN 0 ELSE 1 END FROM projects WHERE project_id = '$PROJECT_ID_A'")"
  [ "$halted" = "1" ] && break
  sleep 0.1
done
stop_daemon
[ "$halted" = "1" ] || fail "max_agent_runs did not halt the project"
[ "$(grep -c '^CALL ' "$PUEUE_AGENT_TEST_AGENT_LOG")" = "2" ] \
  || fail "halted project launched an agent"

write_config "$PROJECT_A" "$PROJECT_ID_A" "$GROUP_A" "$REPO_ROOT/tests/support/fake_agent.sh" 20
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_A" >/dev/null
[ "$(sql "SELECT CASE WHEN halted_reason IS NULL THEN 0 ELSE 1 END FROM projects WHERE project_id = '$PROJECT_ID_A'")" = "0" ] \
  || fail "resume did not clear halted state"
"$PA_BIN" event callback --group "$GROUP_A" --task-id 902 \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
start_daemon
wait_for_agent_calls "3" "resumed project did not schedule a new event"
stop_daemon

# Resuming the other project consumes its coalesced pending callback events.
"$PA_BIN" resume --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
start_daemon
wait_for_sql "SELECT COUNT(*) FROM events WHERE project_id = '$PROJECT_ID_B' AND status = 'completed'" "2" \
  "resume did not release preserved pending events"
stop_daemon

# Simulate a crash after claim. Restart recovery returns the expired lease to pending while paused.
"$PA_BIN" pause --pueue-config "$WORK/pueue.yml" "$PROJECT_B" >/dev/null
"$PA_BIN" event callback --group "$GROUP_B" --task-id 903 \
  --metadata '{"state":"Done","result":"Success"}' >/dev/null
restart_key="pueue-callback:v1:group=$GROUP_B:task-id=903"
sql "UPDATE events SET status = 'claimed', lease_until = 0 WHERE dedup_key = '$restart_key'"
start_daemon
wait_for_sql "SELECT status FROM events WHERE dedup_key = '$restart_key'" "pending" \
  "restart did not recover the expired event lease"
stop_daemon

# Explicit Codex continuation reaches the process boundary and is recorded in SQLite.
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
start_daemon
wait_for_codex_call "Codex resume context was not invoked"
stop_daemon
[ "$(sql "SELECT COUNT(*) FROM agent_runs WHERE project_id = '$PROJECT_ID_A' AND context_mode = 'resume' AND context_session_id = '$context_session_id' AND status = 'completed'")" = "1" ] \
  || fail "Codex resume context was not completed and recorded"
grep -qx 'ARG_1=exec' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume did not invoke exec"
grep -qx 'ARG_2=-C' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume did not scope the project with -C"
grep -qx "ARG_3=$PROJECT_A_CANONICAL" "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex resume used the wrong project root"
grep -qx 'ARG_4=resume' "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation silently used a fresh execution"
grep -qx "ARG_5=$context_session_id" "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation used the wrong session ID"
grep -qx "CODEX_HOME=$CODEX_HOME" "$PUEUE_AGENT_TEST_CODEX_LOG" \
  || fail "Codex continuation did not inherit the fixture CODEX_HOME"

PA_INSTALL_PREFIX="$WORK/install" "$REPO_ROOT/install.sh" >/dev/null
[ -L "$WORK/install/pueue-agent" ] || fail "install did not create pueue-agent symlink"
[ "$(readlink "$WORK/install/pueue-agent")" = "$REPO_ROOT/target/release/pueue-agent" ] \
  || fail "installed symlink does not target the Rust release binary"
"$WORK/install/pueue-agent" --help | grep -q 'SQLite-backed Pueue agent supervisor' \
  || fail "installed Rust release binary is not executable"

mkdir -p "$WORK/unbuilt-repository/bin"
cp "$REPO_ROOT/bin/pueue-agent" "$WORK/unbuilt-repository/bin/pueue-agent"
if launcher_error="$("$WORK/unbuilt-repository/bin/pueue-agent" --help 2>&1)"; then
  fail "development launcher succeeded without a built binary"
fi
printf '%s\n' "$launcher_error" | grep -q 'cargo build --manifest-path' \
  || fail "development launcher did not explain how to build the missing binary"

echo "Rust E2E PASS"
