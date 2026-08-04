load helpers/setup

setup() {
  proj="$(make_project)"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  # config の agent.command をモックに差し替え
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
  logs="$proj/.pueue-agent/logs"
}

@test "crash mode launches agent with crash prompt and increments failures" {
  run "$PA_BIN" wake crash "$proj" 7 "Failed:1"
  [ "$status" -eq 0 ]
  grep -q "PROMPT:" "$MOCK_AGENT_LOG"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  grep -q "task id: 7" "$MOCK_AGENT_LOG"
  [ "$(cat "$logs/consec_failures")" = "1" ]
}

@test "halts without launching agent when failures reach max, but reports event as consumed (0)" {
  echo 2 > "$logs/consec_failures"   # max は 3 (template default)
  run "$PA_BIN" wake crash "$proj" 8 "Failed:1"
  [ "$status" -eq 0 ]                # ガードレール停止発火 = イベントは消費された
  [ ! -f "$MOCK_AGENT_LOG" ]         # agent は起動されない
  [ -f "$logs/halted" ]
  grep -q "halted" "$logs/../logs/notifications.log"
}

@test "task_finished Success resets failures and increments experiment count" {
  echo 2 > "$logs/consec_failures"
  run "$PA_BIN" wake task_finished "$proj" 9 "Success"
  [ "$(cat "$logs/consec_failures")" = "0" ]
  [ "$(cat "$logs/experiment_count")" = "1" ]
  grep -q "mode: task_finished" "$MOCK_AGENT_LOG"
}

@test "halts when experiment count reaches max" {
  echo 20 > "$logs/experiment_count"  # max_experiments = 20 で 21 個目
  run "$PA_BIN" wake task_finished "$proj" 10 "Success"
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ -f "$logs/halted" ]
}

@test "does nothing when halted, and reports the event as not-consumed (3)" {
  echo "manual" > "$logs/halted"
  run "$PA_BIN" wake deep_check "$proj"
  [ "$status" -eq 3 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "lock prevents concurrent wake, and reports the event as not-consumed (3)" {
  mkdir -p "$logs/lock"
  echo $$ > "$logs/lock/pid"         # 生きている PID = agent 稼働中とみなす
  run "$PA_BIN" wake deep_check "$proj"
  [ "$status" -eq 3 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "stale lock is reclaimed" {
  mkdir -p "$logs/lock"
  echo 99999999 > "$logs/lock/pid"   # 存在しない PID
  run "$PA_BIN" wake deep_check "$proj"
  grep -q "mode: deep_check" "$MOCK_AGENT_LOG"
}

@test "agent failure is retried then halts with notification" {
  export MOCK_AGENT_EXIT=1           # 常に失敗 (max_retries=2 → 計3回試行)
  run "$PA_BIN" wake deep_check "$proj"
  [ "$(grep -c PROMPT "$MOCK_AGENT_LOG")" -eq 3 ]
  [ -f "$logs/halted" ]
  grep -q "agent_error" "$logs/notifications.log"
}

@test "prompt contains instructions and state references" {
  run "$PA_BIN" wake deep_check "$proj"
  grep -q ".pueue-agent/instructions.md" "$MOCK_AGENT_LOG"
  grep -q ".pueue-agent/STATE.md" "$MOCK_AGENT_LOG"
}

@test "invalid mode fails" {
  run "$PA_BIN" wake bogus "$proj"
  [ "$status" -eq 1 ]
}
