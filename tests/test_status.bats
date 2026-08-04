load helpers/setup

setup() {
  proj="$(make_project)"
  logs="$proj/.pueue-agent/logs"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"
}

@test "status shows counters and halted reason" {
  echo 2 > "$logs/consec_failures"
  echo 5 > "$logs/experiment_count"
  echo "連続失敗" > "$logs/halted"
  run "$PA_BIN" status "$proj"
  [ "$status" -eq 0 ]
  [[ "$output" == *"HALTED"* ]]
  [[ "$output" == *"連続失敗"* ]]
  [[ "$output" == *"consecutive failures: 2"* ]]
  [[ "$output" == *"experiments: 5"* ]]
}

@test "status shows unread notifications and marks them seen" {
  bash -c "source '$REPO_ROOT/lib/common.sh' && source '$REPO_ROOT/lib/notify.sh' && \
    pa_set_project '$proj' && pa_notify task_finished 'exp done'"
  run "$PA_BIN" status "$proj"
  [[ "$output" == *"exp done"* ]]
  run "$PA_BIN" status "$proj"
  [[ "$output" != *"exp done"* ]]
}

@test "resume clears halted state and failure counter" {
  echo reason > "$logs/halted"
  echo 3 > "$logs/consec_failures"
  run "$PA_BIN" resume "$proj"
  [ ! -f "$logs/halted" ]
  [ "$(cat "$logs/consec_failures")" = "0" ]
}

@test "resume when not halted says so" {
  run "$PA_BIN" resume "$proj"
  [[ "$output" == *"not halted"* ]]
}
