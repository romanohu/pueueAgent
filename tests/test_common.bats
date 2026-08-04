load helpers/setup

@test "unknown subcommand prints usage and exits 1" {
  run "$PA_BIN" no-such-command
  [ "$status" -eq 1 ]
  [[ "$output" == *"Usage:"* ]]
}

@test "no subcommand prints usage and exits 1" {
  run "$PA_BIN"
  [ "$status" -eq 1 ]
}

@test "pa_find_project finds .pueue-agent upward from cwd" {
  proj="$(make_project)"
  mkdir -p "$proj/src/deep"
  run bash -c "source '$REPO_ROOT/lib/common.sh' && cd '$proj/src/deep' && pa_find_project"
  [ "$status" -eq 0 ]
  [ "$output" = "$proj" ]
}

@test "pa_find_project fails when absent" {
  run bash -c "source '$REPO_ROOT/lib/common.sh' && cd '$BATS_TEST_TMPDIR' && pa_find_project"
  [ "$status" -eq 1 ]
}

@test "pa_log appends timestamped line" {
  proj="$(make_project)"
  bash -c "source '$REPO_ROOT/lib/common.sh' && pa_set_project '$proj' && pa_log 'hello world'"
  grep -q "hello world" "$proj/.pueue-agent/logs/runtime.log"
}
