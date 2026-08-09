setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/.." && pwd)"
}

@test "development launcher explains how to build a missing Rust binary" {
  fake_repo="$BATS_TEST_TMPDIR/repository"
  mkdir -p "$fake_repo/bin"
  cp "$REPO_ROOT/bin/pueue-agent" "$fake_repo/bin/pueue-agent"

  run "$fake_repo/bin/pueue-agent" --help

  [ "$status" -eq 1 ]
  [[ "$output" == *"Rust development binary is missing"* ]]
  [[ "$output" == *"cargo build --manifest-path"* ]]
}

@test "development launcher preserves argument boundaries" {
  fake_repo="$BATS_TEST_TMPDIR/repository"
  mkdir -p "$fake_repo/bin" "$fake_repo/target/debug"
  cp "$REPO_ROOT/bin/pueue-agent" "$fake_repo/bin/pueue-agent"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "<%s>\\n" "$@"' \
    > "$fake_repo/target/debug/pueue-agent"
  chmod +x "$fake_repo/target/debug/pueue-agent"

  run "$fake_repo/bin/pueue-agent" submit -- python train.py --name "a b; echo no"

  [ "$status" -eq 0 ]
  [ "${lines[0]}" = "<submit>" ]
  [ "${lines[1]}" = "<-->" ]
  [ "${lines[2]}" = "<python>" ]
  [ "${lines[3]}" = "<train.py>" ]
  [ "${lines[4]}" = "<--name>" ]
  [ "${lines[5]}" = "<a b; echo no>" ]
}

@test "installer builds the locked release and links the Rust binary" {
  fake_bin="$BATS_TEST_TMPDIR/bin"
  prefix="$BATS_TEST_TMPDIR/install"
  cargo_log="$BATS_TEST_TMPDIR/cargo.log"
  mkdir -p "$fake_bin"
  printf '%s\n' '#!/usr/bin/env bash' 'printf "%s\\n" "$*" > "$TEST_CARGO_LOG"' \
    > "$fake_bin/cargo"
  chmod +x "$fake_bin/cargo"

  run env TEST_CARGO_LOG="$cargo_log" PA_INSTALL_PREFIX="$prefix" \
    PATH="$fake_bin:/usr/bin:/bin" bash "$REPO_ROOT/install.sh"

  [ "$status" -eq 0 ]
  [ -L "$prefix/pueue-agent" ]
  [ "$(readlink "$prefix/pueue-agent")" = "$REPO_ROOT/target/release/pueue-agent" ]
  [ "$(cat "$cargo_log")" = "build --locked --release --manifest-path $REPO_ROOT/Cargo.toml" ]
}
