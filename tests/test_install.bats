load helpers/setup

@test "install.sh creates symlink under prefix" {
  export PA_INSTALL_PREFIX="$BATS_TEST_TMPDIR/bin"
  run bash "$REPO_ROOT/install.sh"
  [ "$status" -eq 0 ]
  [ -L "$PA_INSTALL_PREFIX/pueue-agent" ]
  run "$PA_INSTALL_PREFIX/pueue-agent"
  [[ "$output" == *"Usage:"* ]]
}

@test "install.sh is idempotent" {
  export PA_INSTALL_PREFIX="$BATS_TEST_TMPDIR/bin"
  bash "$REPO_ROOT/install.sh"
  run bash "$REPO_ROOT/install.sh"
  [ "$status" -eq 0 ]
}
