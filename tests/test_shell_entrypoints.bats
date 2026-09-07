setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
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
