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

@test "fake Codex records literal selected argv and no environment values" {
  log="$BATS_TEST_TMPDIR/fake-codex.log"

  if env PUEUE_AGENT_TEST_CODEX_LOG="$log" CODEX_HOME="/fixture/codex-home" \
    OPENAI_API_KEY="never-record-this" "$REPO_ROOT/tests/support/fake_codex.sh" \
    exec -- "literal; no shell"; then
    status=0
  else
    status=$?
  fi

  [ "$status" -eq 0 ]
  grep -Fx 'ENV_NAME=CODEX_HOME' "$log"
  grep -Fx 'ARGC=3' "$log"
  grep -Fx 'ARG_1=exec' "$log"
  grep -Fx 'ARG_2=--' "$log"
  grep -Fx 'ARG_3=literal; no shell' "$log"
  ! grep -q 'OPENAI_API_KEY\|never-record-this\|/fixture/codex-home' "$log"
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
