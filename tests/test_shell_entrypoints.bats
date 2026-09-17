setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
  SIGNAL_CHILD_PID=""
  SIGNAL_CHILD_PID_FILE=""
  SIGNAL_CLEANED=0
  SIGNAL_RUSTC_MARKER=""
  SIGNAL_RUSTC_PID=""
  SIGNAL_RUNNER_LOG=""
  SIGNAL_SUPERVISOR_PID=""
  SIGNAL_WORK_PATH=""
}

teardown() {
  signal_fixture_cleanup
}

signal_pid_valid() {
  case "${1:-}" in
    ''|0|1|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

signal_fixture_cleanup() {
  [ "${SIGNAL_CLEANED:-0}" -eq 0 ] || return 0
  if ! signal_pid_valid "${SIGNAL_CHILD_PID:-}" &&
    [ -n "${SIGNAL_CHILD_PID_FILE:-}" ]; then
    for _ in $(seq 50); do
      if [ -s "$SIGNAL_CHILD_PID_FILE" ]; then
        SIGNAL_CHILD_PID="$(<"$SIGNAL_CHILD_PID_FILE")"
        break
      fi
      if signal_pid_valid "${SIGNAL_SUPERVISOR_PID:-}" &&
        ! kill -0 "$SIGNAL_SUPERVISOR_PID" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
  fi
  if ! signal_pid_valid "${SIGNAL_RUSTC_PID:-}" &&
    [ -n "${SIGNAL_RUSTC_MARKER:-}" ]; then
    if [ -s "$SIGNAL_RUSTC_MARKER" ]; then
      SIGNAL_RUSTC_PID="$(<"$SIGNAL_RUSTC_MARKER")"
    fi
  fi

  if signal_pid_valid "${SIGNAL_SUPERVISOR_PID:-}"; then
    kill -TERM "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
  fi
  if signal_pid_valid "${SIGNAL_CHILD_PID:-}"; then
    kill -TERM -- "-${SIGNAL_CHILD_PID}" 2>/dev/null || true
  fi
  if signal_pid_valid "${SIGNAL_RUSTC_PID:-}"; then
    kill -TERM "$SIGNAL_RUSTC_PID" 2>/dev/null || true
  fi

  for _ in $(seq 50); do
    remaining=0
    if signal_pid_valid "${SIGNAL_SUPERVISOR_PID:-}" &&
      kill -0 "$SIGNAL_SUPERVISOR_PID" 2>/dev/null; then
      remaining=1
    fi
    if signal_pid_valid "${SIGNAL_CHILD_PID:-}" &&
      kill -0 -- "-${SIGNAL_CHILD_PID}" 2>/dev/null; then
      remaining=1
    fi
    if signal_pid_valid "${SIGNAL_RUSTC_PID:-}" &&
      kill -0 "$SIGNAL_RUSTC_PID" 2>/dev/null; then
      remaining=1
    fi
    [ "$remaining" -eq 0 ] && break
    sleep 0.1
  done

  if signal_pid_valid "${SIGNAL_CHILD_PID:-}"; then
    kill -KILL -- "-${SIGNAL_CHILD_PID}" 2>/dev/null || true
  fi
  if signal_pid_valid "${SIGNAL_RUSTC_PID:-}"; then
    kill -KILL "$SIGNAL_RUSTC_PID" 2>/dev/null || true
  fi
  if signal_pid_valid "${SIGNAL_SUPERVISOR_PID:-}"; then
    kill -KILL "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
    wait "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
  fi
  if [ -n "${SIGNAL_WORK_PATH:-}" ] && [ -d "$SIGNAL_WORK_PATH" ]; then
    rm -rf "$SIGNAL_WORK_PATH"
  fi
  SIGNAL_CLEANED=1
}

run_signal_retention_probe() {
  signal_name="$1"
  expected_status="$2"
  readiness_mode="${3:-normal}"
  case "$signal_name" in
    TERM|INT) ;;
    *) return 64 ;;
  esac

  fake_bin="$BATS_TEST_TMPDIR/signal-bin-$signal_name-$readiness_mode"
  mkdir -p "$fake_bin"
  work_marker="$BATS_TEST_TMPDIR/signal-work-path-$signal_name-$readiness_mode"
  child_pid_file="$BATS_TEST_TMPDIR/signal-child-pid-$signal_name-$readiness_mode"
  rustc_marker="$BATS_TEST_TMPDIR/signal-rustc-pid-$signal_name-$readiness_mode"
  real_mktemp="$(command -v mktemp)"
  real_python3="$(command -v python3)"
  printf '%s\n' '#!/usr/bin/env bash' \
    "work_path=\"\$($real_mktemp \"\$@\")\"" \
    "printf '%s\\n' \"\$work_path\" > \"$work_marker\"" \
    'printf "%s\n" "$work_path"' > "$fake_bin/mktemp"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "%s\n" Linux' > "$fake_bin/uname"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/pueue"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/pueued"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/git"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/python3"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/jq"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/sqlite3"
  printf '%s\n' '#!/usr/bin/env bash' \
    "printf '%s\\n' \"\$\$\" > \"$rustc_marker\"" \
    'while :; do sleep 1; done' > "$fake_bin/rustc"
  chmod +x "$fake_bin"/*

  SIGNAL_CHILD_PID_FILE="$child_pid_file"
  SIGNAL_RUSTC_MARKER="$rustc_marker"
  SIGNAL_RUNNER_LOG="$BATS_TEST_TMPDIR/signal-runner-$signal_name-$readiness_mode.log"
  env PATH="$fake_bin:/usr/bin:/bin" \
    PUEUE_AGENT_SIGNAL_CHILD_PID_FILE="$child_pid_file" \
    "$real_python3" "$REPO_ROOT/tests/support/signal_supervisor.py" \
    /bin/bash "$REPO_ROOT/tests/e2e/rust_supervisor.sh" \
    >"$SIGNAL_RUNNER_LOG" 2>&1 &
  SIGNAL_SUPERVISOR_PID=$!

  for _ in $(seq 50); do
    if [ -s "$child_pid_file" ]; then
      SIGNAL_CHILD_PID="$(<"$child_pid_file")"
      break
    fi
    sleep 0.1
  done
  [ -n "$SIGNAL_CHILD_PID" ]
  for _ in $(seq 50); do
    if [ -s "$work_marker" ]; then
      SIGNAL_WORK_PATH="$(<"$work_marker")"
      break
    fi
    sleep 0.1
  done
  [ -n "$SIGNAL_WORK_PATH" ]

  if [ "$readiness_mode" = "fail-before-rustc-pid" ]; then
    for _ in $(seq 50); do
      [ -s "$rustc_marker" ] && break
      sleep 0.1
    done
    [ -s "$rustc_marker" ]
    # Deliberately return before assigning SIGNAL_RUSTC_PID.  Teardown must
    # discover it from the marker and still terminate the private child group.
    return 73
  fi

  for _ in $(seq 50); do
    if [ -s "$rustc_marker" ]; then
      SIGNAL_RUSTC_PID="$(<"$rustc_marker")"
      break
    fi
    sleep 0.1
  done
  [ -n "$SIGNAL_RUSTC_PID" ]

  kill -"$signal_name" "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
  supervisor_status=125
  supervisor_exited=0
  for _ in $(seq 50); do
    if ! kill -0 "$SIGNAL_SUPERVISOR_PID" 2>/dev/null; then
      supervisor_exited=1
      break
    fi
    sleep 0.1
  done
  if [ "$supervisor_exited" -eq 0 ]; then
    kill -KILL "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
    wait "$SIGNAL_SUPERVISOR_PID" || supervisor_status=$?
  elif wait "$SIGNAL_SUPERVISOR_PID"; then
    supervisor_status=0
  else
    supervisor_status=$?
  fi

  [ "$supervisor_status" -eq "$expected_status" ]
  [ -d "$SIGNAL_WORK_PATH" ]
  grep -F "Rust E2E retained WORK after failure: $SIGNAL_WORK_PATH" "$SIGNAL_RUNNER_LOG"
  signal_fixture_cleanup
}

@test "real Pueue campaign acceptance is explicitly Linux-only" {
  fake_bin="$BATS_TEST_TMPDIR/non-linux-bin"
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' 'echo Darwin' > "$fake_bin/uname"
  chmod +x "$fake_bin/uname"

  run env PATH="$fake_bin:/usr/bin:/bin" bash "$REPO_ROOT/tests/e2e/rust_supervisor.sh"

  [ "$status" -eq 1 ]
  [[ "$output" == *"real-Pueue campaign acceptance requires Linux"* ]]
}

@test "failed real Pueue harness retains its private diagnostics worktree" {
  fake_bin="$BATS_TEST_TMPDIR/failure-bin"
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "%s\\n" Linux' > "$fake_bin/uname"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/pueue"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/pueued"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/git"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/python3"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/jq"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/sqlite3"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 73' > "$fake_bin/rustc"
  chmod +x "$fake_bin"/*

  run env PATH="$fake_bin:/usr/bin:/bin" \
    bash "$REPO_ROOT/tests/e2e/rust_supervisor.sh"

  [ "$status" -ne 0 ]
  retained_work="$(printf '%s\n' "$output" | sed -n 's/^Rust E2E retained WORK after failure: //p')"
  [ -n "$retained_work" ]
  [ -d "$retained_work" ]
  rm -rf "$retained_work"
}

@test "TERM-terminated real Pueue harness retains diagnostics and status" {
  run_signal_retention_probe TERM 143
}

@test "INT-terminated real Pueue harness retains diagnostics and status" {
  run_signal_retention_probe INT 130
}

@test "signal probe bounds cleanup after readiness failure" {
  started_at="$(date +%s)"
  if run_signal_retention_probe TERM 143 fail-before-rustc-pid; then
    probe_status=0
  else
    probe_status=$?
  fi
  signal_fixture_cleanup
  elapsed=$(( $(date +%s) - started_at ))

  [ "$probe_status" -eq 73 ]
  [ -s "$SIGNAL_RUSTC_MARKER" ]
  [ -n "$SIGNAL_RUSTC_PID" ]
  if kill -0 "$SIGNAL_RUSTC_PID" 2>/dev/null; then
    false
  fi
  [ "$elapsed" -lt 15 ]
}

@test "signal supervisor preserves a child wrong-exit status after TERM" {
  child_script="$BATS_TEST_TMPDIR/signal-wrong-exit-child.py"
  child_pid_file="$BATS_TEST_TMPDIR/signal-wrong-exit-child.pid"
  supervisor_log="$BATS_TEST_TMPDIR/signal-wrong-exit-supervisor.log"
  real_python3="$(command -v python3)"
  printf '%s\n' \
    'import signal' \
    '' \
    'def exit_with_wrong_status(_signum, _frame):' \
    '    raise SystemExit(42)' \
    '' \
    'signal.signal(signal.SIGTERM, exit_with_wrong_status)' \
    'signal.pause()' > "$child_script"

  SIGNAL_CHILD_PID_FILE="$child_pid_file"
  SIGNAL_RUNNER_LOG="$supervisor_log"
  env PUEUE_AGENT_SIGNAL_CHILD_PID_FILE="$child_pid_file" \
    "$real_python3" "$REPO_ROOT/tests/support/signal_supervisor.py" \
    "$real_python3" -B "$child_script" \
    >"$supervisor_log" 2>&1 &
  SIGNAL_SUPERVISOR_PID=$!

  for _ in $(seq 50); do
    if [ -s "$child_pid_file" ]; then
      SIGNAL_CHILD_PID="$(<"$child_pid_file")"
      break
    fi
    sleep 0.1
  done
  [ -n "$SIGNAL_CHILD_PID" ]

  kill -TERM "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
  supervisor_exited=0
  for _ in $(seq 50); do
    if ! kill -0 "$SIGNAL_SUPERVISOR_PID" 2>/dev/null; then
      supervisor_exited=1
      break
    fi
    sleep 0.1
  done
  if [ "$supervisor_exited" -eq 0 ]; then
    kill -KILL "$SIGNAL_SUPERVISOR_PID" 2>/dev/null || true
  fi
  if wait "$SIGNAL_SUPERVISOR_PID"; then
    supervisor_status=0
  else
    supervisor_status=$?
  fi

  [ "$supervisor_status" -eq 42 ]
  signal_fixture_cleanup
}

@test "real Pueue harness asserts the production-derived Codex network argument" {
  awk '
    index($0, "sandbox_workspace_write.network_access=true") &&
      index($0, "PUEUE_AGENT_TEST_CODEX_LOG") { found = 1 }
    END { exit !found }
  ' "$REPO_ROOT/tests/e2e/rust_supervisor.sh"
}

@test "real Pueue harness scopes experiment status polls to one source" {
  run grep -F 'SELECT status FROM experiments WHERE campaign_id =' \
    "$REPO_ROOT/tests/e2e/rust_supervisor.sh"

  [ "$status" -ne 0 ]
}

@test "development launcher explains how to build a missing Rust binary" {
  fake_repo="$BATS_TEST_TMPDIR/repository"
  mkdir -p "$fake_repo/bin"
  cp "$REPO_ROOT/bin/pueue-agent" "$fake_repo/bin/pueue-agent"

  output_file="$BATS_TEST_TMPDIR/launcher-output"
  if env -u CARGO_TARGET_DIR "$fake_repo/bin/pueue-agent" --help >"$output_file" 2>&1; then
    status=0
  else
    status=$?
  fi
  output="$(<"$output_file")"

  [ "$status" -eq 1 ]
  [[ "$output" == *"Rust development binary is missing"* ]]
  [[ "$output" == *"cargo build --manifest-path"* ]]
}

@test "development launcher preserves argument boundaries" {
  fake_repo="$BATS_TEST_TMPDIR/repository"
  mkdir -p "$fake_repo/bin" "$fake_repo/target/debug"
  cp "$REPO_ROOT/bin/pueue-agent" "$fake_repo/bin/pueue-agent"
  printf '%s\n' '#!/usr/bin/env bash' "printf '<%s>\\n' \"\$@\"" \
    > "$fake_repo/target/debug/pueue-agent"
  chmod +x "$fake_repo/target/debug/pueue-agent"

  output_file="$BATS_TEST_TMPDIR/launcher-output"
  if env -u CARGO_TARGET_DIR \
    "$fake_repo/bin/pueue-agent" submit -- python train.py --name "a b; echo no" \
    >"$output_file" 2>&1; then
    status=0
  else
    status=$?
  fi
  expected_file="$BATS_TEST_TMPDIR/launcher-expected"
  printf '%s\n' '<submit>' '<-->' '<python>' '<train.py>' '<--name>' '<a b; echo no>' \
    > "$expected_file"

  [ "$status" -eq 0 ]
  diff -u "$expected_file" "$output_file"
}

@test "development launcher honors an absolute Cargo target directory" {
  fake_repo="$BATS_TEST_TMPDIR/repository"
  cargo_target="$BATS_TEST_TMPDIR/cargo-target"
  mkdir -p "$fake_repo/bin" "$cargo_target/debug"
  cp "$REPO_ROOT/bin/pueue-agent" "$fake_repo/bin/pueue-agent"
  printf '%s\n' '#!/usr/bin/env bash' "printf '<%s>\\n' \"\$@\"" \
    > "$cargo_target/debug/pueue-agent"
  chmod +x "$cargo_target/debug/pueue-agent"

  run env CARGO_TARGET_DIR="$cargo_target" \
    "$fake_repo/bin/pueue-agent" status --json

  [ "$status" -eq 0 ]
  [ "$output" = $'<status>\n<--json>' ]
}

@test "fake agent records literal argv boundaries and allowlisted environment names only" {
  log="$BATS_TEST_TMPDIR/fake-agent.log"

  if env PUEUE_AGENT_TEST_AGENT_LOG="$log" \
    HOME="/fixture/home" PATH="$PATH" OPENAI_API_KEY="never-record-this" \
    "$REPO_ROOT/tests/support/fake_agent.sh" \
    "literal; no shell" "two words" '--looks-like-an-option'; then
    status=0
  else
    status=$?
  fi

  [ "$status" -eq 0 ]
  grep -Fx 'ARGC=3' "$log"
  grep -Fx 'ARGV[1]=<literal; no shell>' "$log"
  grep -Fx 'ARGV[2]=<two words>' "$log"
  grep -Fx 'ARGV[3]=<--looks-like-an-option>' "$log"
  grep -Fx 'ENV_NAME=HOME' "$log"
  grep -Fx 'ENV_NAME=PATH' "$log"
  ! grep -q 'OPENAI_API_KEY\|never-record-this\|/fixture/home' "$log"
}

@test "project-scoped agent call counting handles interleaving and exact IDs" {
  source "$REPO_ROOT/tests/support/agent_call_count.sh"
  mixed_log="$BATS_TEST_TMPDIR/mixed-agent-calls.log"
  cat > "$mixed_log" <<'EOF'
CALL 1
CALL 2
PROJECT_ID=project-a
RUN_ID=run-a
EDITOR_MODE=fresh
PROJECT_ID=project-b
RUN_ID=run-b
CALL 3
PROJECT_ID=project-ab
RUN_ID=run-ab
CALL 4
PROJECT_ID=project-a
RUN_ID=run-a2
CALL 5
PROJECT_ID=project-a
RUN_ID=run-a3
EOF
  PUEUE_AGENT_TEST_AGENT_LOG="$mixed_log"
  [ "$(agent_call_count_for_project project-a)" = "3" ]
  [ "$(agent_call_count_for_project project-ab)" = "1" ]
  [ "$(agent_call_count_for_project project-b)" = "1" ]
  PUEUE_AGENT_TEST_AGENT_LOG="$BATS_TEST_TMPDIR/missing-agent-calls.log"
  [ "$(agent_call_count_for_project project-a)" = "0" ]

  fake_log="$BATS_TEST_TMPDIR/fake-agent-project.log"
  if env PUEUE_AGENT_TEST_AGENT_LOG="$fake_log" \
    PUEUE_AGENT_PROJECT_ID=project-a PUEUE_AGENT_RUN_ID=run-a \
    OPENAI_API_KEY=secret-value \
    "$REPO_ROOT/tests/support/fake_agent.sh" "editor-marker"; then
    status=0
  else
    status=$?
  fi
  [ "$status" -eq 0 ]
  [ "$(grep -c '^PROJECT_ID=project-a$' "$fake_log")" = "1" ]
  ! grep -Eq 'secret-value|OPENAI_API_KEY' "$fake_log"
}

@test "learning editor fixture uses the pinned Python check under a narrow PATH" {
  fixture="$BATS_TEST_TMPDIR/learning-editor"
  fake_bin="$BATS_TEST_TMPDIR/learning-editor-bin"
  mkdir -p "$fixture" "$fake_bin"
  cp "$REPO_ROOT/tests/e2e/learning_experiment/model.py" "$fixture/model.py"
  printf '%s\n' '#!/bin/sh' "exec \"$(command -v python3)\" \"\$@\"" > "$fake_bin/python"
  chmod +x "$fake_bin/python"
  before="$BATS_TEST_TMPDIR/learning-model-before.py"
  cp "$fixture/model.py" "$before"
  editor_output="$fixture/editor.json"
  log="$BATS_TEST_TMPDIR/learning-editor.log"
  state="$BATS_TEST_TMPDIR/learning-editor.state"

  run env PATH="$fake_bin" \
    PUEUE_AGENT_TEST_AGENT_LOG="$log" \
    PUEUE_AGENT_TEST_AGENT_STATE="$state" \
    PUEUE_AGENT_EDITOR_OUTPUT="$editor_output" \
    PUEUE_AGENT_EDITOR_MODE=fresh \
    /bin/bash -c 'cd "$1" && exec /bin/bash "$2" "learning prompt PUEUE_AGENT_E2E_LEARNING"' \
    bash "$fixture" "$REPO_ROOT/tests/support/fake_agent.sh"

  [ "$status" -eq 0 ]
  jq -e '.status == "ready" and .proposed_checks[0].source == "python" and .proposed_checks[0].argv == ["python", "-m", "pytest"]' "$editor_output"
  python3 - "$before" "$fixture/model.py" <<'PY'
from pathlib import Path
import sys

before = Path(sys.argv[1]).read_bytes()
after = Path(sys.argv[2]).read_bytes()
expected = before.replace(b"LEARNING_RATE = 0.001\n", b"LEARNING_RATE = 0.05\n")
assert before.count(b"LEARNING_RATE = 0.001\n") == 1
assert after == expected
PY
}

@test "fake Codex records safe argv only and no environment values or prompt" {
  log="$BATS_TEST_TMPDIR/fake-codex.log"
  env_names="$BATS_TEST_TMPDIR/fake-codex-env-names.log"

  if env PUEUE_AGENT_TEST_CODEX_LOG="$log" CODEX_HOME="/fixture/codex-home" \
    PUEUE_AGENT_TEST_CODEX_ENV_NAMES="$env_names" \
    OPENAI_API_KEY="never-record-this" "$REPO_ROOT/tests/support/fake_codex.sh" \
    exec -- "PROMPT_MUST_NOT_BE_RECORDED literal; no shell"; then
    status=0
  else
    status=$?
  fi

  [ "$status" -eq 0 ]
  grep -Fx 'ENV_NAME=CODEX_HOME' "$log"
  grep -Fx 'ARGC=3' "$log"
  grep -Fx 'ARG_1=exec' "$log"
  grep -Fx 'ARG_2=--' "$log"
  ! grep -q 'ARG_3=\|PROMPT_MUST_NOT_BE_RECORDED\|OPENAI_API_KEY\|never-record-this\|/fixture/codex-home' "$log"
}

@test "fake Codex decision captures security fields and environment names without payloads" {
  capture="$BATS_TEST_TMPDIR/decision-capture.log"
  captured_env_names="$BATS_TEST_TMPDIR/decision-env-names.log"
  codex_home="$BATS_TEST_TMPDIR/codex-home"
  schema="$BATS_TEST_TMPDIR/decision-schema.json"
  decision_output="$BATS_TEST_TMPDIR/decision.json"
  mkdir -p "$codex_home"
  printf '%s\n' '{}' > "$schema"
  : > "$decision_output"
  chmod 600 "$schema" "$decision_output"
  prompt='PROMPT_MUST_NOT_BE_CAPTURED
{"schema_version":1,"objective":{"text":"fixture objective","digest":"objective-digest"},"source_experiment":{"experiment_id":"experiment-1","status":"succeeded","failure_fingerprint":null}}'

  run env -u OPENAI_API_KEY -u AWS_SECRET_ACCESS_KEY -u SSH_AUTH_SOCK \
    PUEUE_AGENT_TEST_CODEX_LOG="$capture" \
    PUEUE_AGENT_TEST_CODEX_ENV_NAMES="$captured_env_names" \
    CODEX_HOME="$codex_home" \
    "$REPO_ROOT/tests/support/fake_codex.sh" \
    --ask-for-approval never exec --ignore-user-config --ignore-rules --strict-config \
    --output-schema "$schema" --output-last-message "$decision_output" \
    -c 'permissions.pueue_agent_decision.extends=":read-only"' \
    -c 'permissions.pueue_agent_decision.network.enabled=true' \
    -c 'default_permissions="pueue_agent_decision"' -- "$prompt"

  [ "$status" -eq 0 ]
  run grep -F 'sandbox_read_only=true' "$capture"
  [ "$status" -eq 0 ]
  run grep -F 'network_access=true' "$capture"
  [ "$status" -eq 0 ]
  run grep -E 'OPENAI_API_KEY|AWS_SECRET_ACCESS_KEY|SSH_AUTH_SOCK' "$captured_env_names"
  [ "$status" -ne 0 ]
  jq -e '.decision == "proposal" and .proposal.kind == "experiment" and .proposal.source_experiment_id == "experiment-1"' "$decision_output"
  ! grep -q 'PROMPT_MUST_NOT_BE_CAPTURED\|fixture objective\|"decision"' "$capture" "$captured_env_names"
}

@test "fake Codex decision modes are deterministic and bounded" {
  capture="$BATS_TEST_TMPDIR/decision-modes-capture.log"
  captured_env_names="$BATS_TEST_TMPDIR/decision-modes-env-names.log"
  codex_home="$BATS_TEST_TMPDIR/decision-modes-codex-home"
  schema="$BATS_TEST_TMPDIR/decision-modes-schema.json"
  decision_output="$BATS_TEST_TMPDIR/decision-modes-output.json"
  mkdir -p "$codex_home"
  printf '%s\n' '{}' > "$schema"
  : > "$decision_output"
  chmod 600 "$schema" "$decision_output"

  invoke_decision() {
    source_experiment_id="$1"
    source_status="$2"
    failure_fingerprint="$3"
    objective="$4"
    context="$(jq -cn \
      --arg source_experiment_id "$source_experiment_id" \
      --arg source_status "$source_status" \
      --arg failure_fingerprint "$failure_fingerprint" \
      --arg objective "$objective" \
      '{schema_version:1,objective:{text:$objective,digest:"objective-digest"},source_experiment:{experiment_id:$source_experiment_id,status:$source_status,failure_fingerprint:(if $failure_fingerprint == "" then null else $failure_fingerprint end)}}')"
    prompt="DECISION_PROMPT_MUST_NOT_BE_CAPTURED
$context"
    env -u OPENAI_API_KEY -u AWS_SECRET_ACCESS_KEY -u SSH_AUTH_SOCK \
      PUEUE_AGENT_TEST_CODEX_LOG="$capture" \
      PUEUE_AGENT_TEST_CODEX_ENV_NAMES="$captured_env_names" \
      CODEX_HOME="$codex_home" \
      "$REPO_ROOT/tests/support/fake_codex.sh" \
      --ask-for-approval never exec --ignore-user-config --ignore-rules --strict-config \
      --output-schema "$schema" --output-last-message "$decision_output" \
      -c 'permissions.pueue_agent_decision.extends=":read-only"' \
      -c 'permissions.pueue_agent_decision.network.enabled=true' \
      -c 'default_permissions="pueue_agent_decision"' -- "$prompt"
  }

  invoke_decision "trusted-failure" "failed" "trusted-fingerprint" "repair fixture"
  jq -e '.decision == "proposal" and .proposal.kind == "repair"' "$decision_output"

  invoke_decision "untrusted-failure" "failed" "" "non-repair fixture"
  jq -e '.decision == "proposal" and .proposal.kind == "experiment"' "$decision_output"

  invoke_decision "wait-source" "succeeded" "" "PUEUE_AGENT_E2E_WAIT_ONCE"
  jq -e '.decision == "wait" and .requested_wait_minutes == 1' "$decision_output"
  invoke_decision "wait-source" "succeeded" "" "PUEUE_AGENT_E2E_WAIT_ONCE"
  jq -e '.decision == "proposal" and .proposal.kind == "experiment"' "$decision_output"

  for attempt in 1 2 3; do
    invoke_decision "invalid-source" "succeeded" "" "PUEUE_AGENT_E2E_INVALID_THREE"
    if jq -e . "$decision_output" >/dev/null 2>&1; then
      false
    fi
  done
  invoke_decision "invalid-source" "succeeded" "" "PUEUE_AGENT_E2E_INVALID_THREE"
  jq -e '.decision == "proposal"' "$decision_output"

  [ "$(grep -Fc 'DECISION_INVOCATION source_experiment_id=wait-source' "$capture")" -eq 2 ]
  [ "$(grep -Fc 'DECISION_INVOCATION source_experiment_id=invalid-source' "$capture")" -eq 4 ]
  ! grep -q 'DECISION_PROMPT_MUST_NOT_BE_CAPTURED\|trusted-fingerprint\|"decision"' \
    "$capture" "$captured_env_names"
}

@test "fake agent failure sleep and exit fixtures are bounded" {
  log="$BATS_TEST_TMPDIR/fake-agent.log"

  if env PUEUE_AGENT_TEST_AGENT_LOG="$log" PUEUE_AGENT_TEST_AGENT_MODE=fail \
    "$REPO_ROOT/tests/support/fake_agent.sh"; then
    status=0
  else
    status=$?
  fi
  [ "$status" -eq 17 ]

  if env PUEUE_AGENT_TEST_AGENT_LOG="$log" PUEUE_AGENT_TEST_AGENT_MODE=sleep \
    PUEUE_AGENT_TEST_AGENT_SLEEP_SECONDS=3 "$REPO_ROOT/tests/support/fake_agent.sh"; then
    status=0
  else
    status=$?
  fi
  [ "$status" -eq 64 ]

  if env PUEUE_AGENT_TEST_AGENT_LOG="$log" PUEUE_AGENT_TEST_AGENT_MODE=exit \
    PUEUE_AGENT_TEST_AGENT_EXIT_CODE=23 "$REPO_ROOT/tests/support/fake_agent.sh"; then
    status=0
  else
    status=$?
  fi
  [ "$status" -eq 23 ]
  grep -Fx 'MODE=fail' "$log"
  grep -Fx 'MODE=exit' "$log"
}

@test "CI runtime acceptance exercises native gate Codex rejection auth filtering and caps" {
  cargo test --manifest-path "$REPO_ROOT/Cargo.toml" --test native_agent_gate \
    durable_marker_precedes_release_and_agent_output_uses_verified_log -- --exact
  cargo test --manifest-path "$REPO_ROOT/Cargo.toml" --test scheduler \
    unsafe_codex_argument_dead_letters_before_reservation_without_agent_run -- --exact
  cargo test --manifest-path "$REPO_ROOT/Cargo.toml" --test codex_security \
    missing_capability_fails_closed_and_auth_names_never_enter_filters -- --exact
  cargo test --manifest-path "$REPO_ROOT/Cargo.toml" --test codex_security \
    private_temp_inventory_rejects_generation_overflow_without_mutation -- --exact
}

@test "installer builds the locked release and links the Rust binary" {
  fake_bin="$BATS_TEST_TMPDIR/bin"
  prefix="$BATS_TEST_TMPDIR/install"
  cargo_log="$BATS_TEST_TMPDIR/cargo.log"
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' \
    "printf '%s\\n' \"\$*\" > \"\$TEST_CARGO_LOG\"" \
    > "$fake_bin/cargo"
  chmod +x "$fake_bin/cargo"

  if env -u CARGO_TARGET_DIR TEST_CARGO_LOG="$cargo_log" PA_INSTALL_PREFIX="$prefix" \
    PATH="$fake_bin:/usr/bin:/bin" bash "$REPO_ROOT/install.sh"; then
    status=0
  else
    status=$?
  fi

  [ "$status" -eq 0 ]
  [ -L "$prefix/pueue-agent" ]
  [ "$(readlink "$prefix/pueue-agent")" = "$REPO_ROOT/target/release/pueue-agent" ]
  [ "$(cat "$cargo_log")" = "build --locked --release --manifest-path $REPO_ROOT/Cargo.toml" ]
}

@test "installer honors an absolute Cargo target directory" {
  fake_bin="$BATS_TEST_TMPDIR/bin"
  prefix="$BATS_TEST_TMPDIR/install"
  cargo_target="$BATS_TEST_TMPDIR/cargo-target"
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' 'exit 0' > "$fake_bin/cargo"
  chmod +x "$fake_bin/cargo"

  run env CARGO_TARGET_DIR="$cargo_target" PA_INSTALL_PREFIX="$prefix" \
    PATH="$fake_bin:/usr/bin:/bin" bash "$REPO_ROOT/install.sh"

  [ "$status" -eq 0 ]
  [ -L "$prefix/pueue-agent" ]
  [ "$(readlink "$prefix/pueue-agent")" = "$cargo_target/release/pueue-agent" ]
}
