#!/usr/bin/env bash
# Hermetic installer/launcher checks. No Rust, Pueue daemon, or network is used.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pueue-agent-shell.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_contains() {
  grep -Fq -- "$2" "$1" || fail "$1 does not contain: $2"
}

new_fixture() {
  CASE_ROOT="$(mktemp -d "$WORK/case.XXXXXX")"
  FIXTURE_REPO="$CASE_ROOT/repository with spaces"
  export HOME="$CASE_ROOT/home" PA_INSTALL_PREFIX="$CASE_ROOT/install bin"
  export FAKE_CARGO_LOG="$CASE_ROOT/cargo.log"
  unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR FAKE_CARGO_FAIL FAKE_CARGO_NO_BINARY
  mkdir -p "$FIXTURE_REPO/bin" "$HOME" "$CASE_ROOT/work" "$CASE_ROOT/tools"
  cp "$REPO_ROOT/install.sh" "$FIXTURE_REPO/install.sh"
  cp "$REPO_ROOT/bin/pueue-agent" "$FIXTURE_REPO/bin/pueue-agent"
  : > "$FIXTURE_REPO/Cargo.toml"
  # Restrict PATH so a real cargo/pueue installation can never be invoked.
  for tool in bash dirname readlink mkdir ln chmod; do
    ln -s "$(command -v "$tool")" "$CASE_ROOT/tools/$tool"
  done
  cat > "$CASE_ROOT/tools/cargo" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "$FAKE_CARGO_LOG"
[ "${FAKE_CARGO_FAIL:-0}" = 0 ] || exit "$FAKE_CARGO_FAIL"
manifest=''
target="${CARGO_TARGET_DIR:-${CARGO_BUILD_TARGET_DIR:-}}"
while [ "$#" -gt 0 ]; do
  case "$1" in
    --manifest-path) manifest="$2"; shift ;;
    --target-dir) target="$2"; shift ;;
  esac
  shift
done
[ -f "$manifest" ] || { echo 'manifest not found' >&2; exit 2; }
[ -n "$target" ] || target="$(dirname "$manifest")/target"
[ "${FAKE_CARGO_NO_BINARY:-0}" = 0 ] || exit 0
mkdir -p "$target/release"
printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$@"\n' > "$target/release/pueue-agent"
chmod +x "$target/release/pueue-agent"
STUB
  chmod +x "$CASE_ROOT/tools/cargo"
  cd "$CASE_ROOT/work"
}

install_fixture() {
  PATH="$CASE_ROOT/tools" "$BASH" "$FIXTURE_REPO/install.sh" > "$CASE_ROOT/output" 2>&1
}

assert_installed() {
  [ -L "$PA_INSTALL_PREFIX/pueue-agent" ] || fail 'installer did not create a link'
  [ "$(readlink "$PA_INSTALL_PREFIX/pueue-agent")" = "$1/release/pueue-agent" ] \
    || fail 'installer linked the wrong target'
  [ -x "$PA_INSTALL_PREFIX/pueue-agent" ] || fail 'installed link is not executable'
  assert_contains "$FAKE_CARGO_LOG" '--locked'
  assert_contains "$FAKE_CARGO_LOG" '--release'
  assert_contains "$FAKE_CARGO_LOG" "$FIXTURE_REPO/Cargo.toml"
}

install_default_prefix() {
  new_fixture
  unset PA_INSTALL_PREFIX
  install_fixture
  PA_INSTALL_PREFIX="$HOME/.local/bin"
  assert_installed "$FIXTURE_REPO/target"
  assert_contains "$CASE_ROOT/output" 'pueue was not found'
}

install_absolute_target() {
  new_fixture
  export CARGO_TARGET_DIR="$CASE_ROOT/absolute target"
  install_fixture
  assert_installed "$CARGO_TARGET_DIR"
}

install_relative_target() {
  new_fixture
  export CARGO_TARGET_DIR='relative target'
  install_fixture
  assert_installed "$CASE_ROOT/work/relative target"
}

install_configured_target() {
  new_fixture
  # Cargo also supports this configuration environment variable. The installer
  # must select its advertised target directory explicitly rather than guessing.
  export CARGO_BUILD_TARGET_DIR="$CASE_ROOT/configured target"
  install_fixture
  assert_installed "$FIXTURE_REPO/target"
}

install_replaces_directory_symlink() {
  new_fixture
  mkdir -p "$PA_INSTALL_PREFIX" "$CASE_ROOT/unrelated directory"
  ln -s "$CASE_ROOT/unrelated directory" "$PA_INSTALL_PREFIX/pueue-agent"
  install_fixture
  assert_installed "$FIXTURE_REPO/target"
  [ ! -e "$CASE_ROOT/unrelated directory/pueue-agent" ] \
    || fail 'installer wrote through the old directory symlink'
}

install_rejects_real_directory() {
  new_fixture
  mkdir -p "$PA_INSTALL_PREFIX/pueue-agent"
  printf 'keep\n' > "$PA_INSTALL_PREFIX/pueue-agent/sentinel"
  if install_fixture; then fail 'installer accepted a directory as the executable'; fi
  [ "$(cat "$PA_INSTALL_PREFIX/pueue-agent/sentinel")" = keep ] \
    || fail 'installer damaged an existing directory'
  [ ! -e "$PA_INSTALL_PREFIX/pueue-agent/pueue-agent" ] \
    || fail 'installer wrote inside the existing directory'
}

install_rejects_missing_artifact() {
  new_fixture
  export FAKE_CARGO_NO_BINARY=1
  if install_fixture; then fail 'installer succeeded without a build artifact'; fi
  [ ! -L "$PA_INSTALL_PREFIX/pueue-agent" ] || fail 'installer created a dangling link'
}

install_preserves_existing_on_build_failure() {
  new_fixture
  mkdir -p "$PA_INSTALL_PREFIX"
  printf 'keep\n' > "$PA_INSTALL_PREFIX/pueue-agent"
  export FAKE_CARGO_FAIL=17
  if install_fixture; then fail 'installer ignored cargo failure'; fi
  [ "$(cat "$PA_INSTALL_PREFIX/pueue-agent")" = keep ] \
    || fail 'failed build replaced the installed executable'
}

install_requires_cargo() {
  new_fixture
  rm "$CASE_ROOT/tools/cargo"
  if install_fixture; then fail 'installer succeeded without cargo'; fi
  assert_contains "$CASE_ROOT/output" 'Rust toolchain (cargo) is required'
}

install_through_symlink() {
  new_fixture
  mkdir -p "$CASE_ROOT/links"
  ln -s '../repository with spaces/install.sh' "$CASE_ROOT/links/install"
  PATH="$CASE_ROOT/tools" "$BASH" "$CASE_ROOT/links/install" > "$CASE_ROOT/output" 2>&1
  assert_installed "$FIXTURE_REPO/target"
}

write_debug_binary() {
  mkdir -p "$1/debug"
  cat > "$1/debug/pueue-agent" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@"
exit 23
STUB
  chmod +x "$1/debug/pueue-agent"
}

assert_launcher() {
  local launcher="$1" status=0
  PATH="$CASE_ROOT/tools" "$BASH" "$launcher" 'argument with spaces' '' '--literal' \
    > "$CASE_ROOT/output" 2>&1 || status=$?
  [ "$status" = 23 ] || fail "launcher returned $status instead of the binary status"
  printf 'argument with spaces\n\n--literal\n' > "$CASE_ROOT/expected"
  cmp "$CASE_ROOT/expected" "$CASE_ROOT/output" || fail 'launcher changed arguments'
}

launcher_default_target() {
  new_fixture
  write_debug_binary "$FIXTURE_REPO/target"
  assert_launcher "$FIXTURE_REPO/bin/pueue-agent"
}

launcher_relative_target() {
  new_fixture
  export CARGO_TARGET_DIR='debug target'
  write_debug_binary "$CASE_ROOT/work/$CARGO_TARGET_DIR"
  assert_launcher "$FIXTURE_REPO/bin/pueue-agent"
}

launcher_absolute_target() {
  new_fixture
  export CARGO_TARGET_DIR="$CASE_ROOT/debug target"
  write_debug_binary "$CARGO_TARGET_DIR"
  assert_launcher "$FIXTURE_REPO/bin/pueue-agent"
}

launcher_symlink_chain() {
  new_fixture
  write_debug_binary "$FIXTURE_REPO/target"
  mkdir -p "$CASE_ROOT/links"
  ln -s '../repository with spaces/bin/pueue-agent' "$CASE_ROOT/links/inner"
  ln -s 'inner' "$CASE_ROOT/links/outer"
  assert_launcher "$CASE_ROOT/links/outer"
}

launcher_missing_binary() {
  new_fixture
  if PATH="$CASE_ROOT/tools" "$BASH" "$FIXTURE_REPO/bin/pueue-agent" \
      > "$CASE_ROOT/output" 2>&1; then fail 'launcher succeeded without a binary'; fi
  assert_contains "$CASE_ROOT/output" 'Rust development binary is missing'
  assert_contains "$CASE_ROOT/output" 'cargo build --manifest-path'
}

passed=0
failed=0
for test in \
  install_default_prefix install_absolute_target install_relative_target \
  install_configured_target install_replaces_directory_symlink install_rejects_real_directory \
  install_rejects_missing_artifact install_preserves_existing_on_build_failure \
  install_requires_cargo install_through_symlink \
  launcher_default_target launcher_relative_target launcher_absolute_target \
  launcher_symlink_chain launcher_missing_binary; do
  # Run outside an `if` condition so Bash does not disable errexit in the test.
  set +e
  (set -e; "$test")
  status=$?
  set -e
  if [ "$status" = 0 ]; then
    printf 'PASS: %s\n' "$test"
    passed=$((passed + 1))
  else
    printf 'FAIL: %s\n' "$test" >&2
    failed=$((failed + 1))
  fi
done
printf '%s passed; %s failed\n' "$passed" "$failed"
[ "$failed" = 0 ]
