load helpers/setup

setup() {
  proj="$(make_project)"
  logs="$proj/.pueue-agent/logs"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export MOCK_AGENT_LOG="$BATS_TEST_TMPDIR/agent_calls.log"
  export PA_TASK_LOG_DIR="$BATS_TEST_TMPDIR/task_logs"
  mkdir -p "$PA_TASK_LOG_DIR"
  sed -i.bak \
    "s|^  command: .*|  command: \"$REPO_ROOT/tests/helpers/mock_agent.sh {prompt}\"|" \
    "$proj/.pueue-agent/config.yml"
}

# フィクスチャ生成ヘルパ (実測した pueue 4.0.4 の形状)
running_status() {  # $1=task_id
  cat > "$MOCK_PUEUE_STATUS_JSON" <<EOF
{"tasks":{"$1":{"id":$1,"group":"pa-proj","command":"python train.py",
 "status":{"Running":{"enqueued_at":"x","start":"x"}}}}}
EOF
}
failed_status() {   # $1=task_id
  cat > "$MOCK_PUEUE_STATUS_JSON" <<EOF
{"tasks":{"$1":{"id":$1,"group":"pa-proj","command":"python train.py",
 "status":{"Done":{"enqueued_at":"x","start":"x","end":"x","result":{"Failed":1}}}}}}
EOF
}
empty_status() { echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"; }

@test "healthy running task: no agent launch, counter increments" {
  running_status 3
  echo "step 100 loss 0.5" > "$PA_TASK_LOG_DIR/3.log"
  run "$PA_BIN" sentinel "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ "$(cat "$logs/check_count")" = "1" ]
}

@test "no tasks at all: exits quietly without counting" {
  empty_status
  run "$PA_BIN" sentinel "$proj"
  [ "$status" -eq 0 ]
  [ ! -f "$MOCK_AGENT_LOG" ]
  [ ! -f "$logs/check_count" ]
}

@test "failed task triggers crash wake once and is deduped" {
  failed_status 4
  echo "Traceback ..." > "$PA_TASK_LOG_DIR/4.log"
  "$PA_BIN" sentinel "$proj"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
  # 2回目は handled 済みなので起動しない
  rm "$MOCK_AGENT_LOG"
  "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
}

@test "error pattern in running task output triggers crash wake" {
  running_status 5
  printf 'step 10\nloss: NaN\n' > "$PA_TASK_LOG_DIR/5.log"
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: crash" "$MOCK_AGENT_LOG"
}

@test "stalled output triggers stalled wake" {
  running_status 6
  echo "step 1" > "$PA_TASK_LOG_DIR/6.log"
  # 前回スナップショット: 同サイズ・31分前 (stall_minutes=30)
  size=$(wc -c < "$PA_TASK_LOG_DIR/6.log" | tr -d ' ')
  echo "$size $(( $(date +%s) - 1860 ))" > "$logs/progress_6"
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: stalled" "$MOCK_AGENT_LOG"
}

@test "growing output updates snapshot, no wake" {
  running_status 6
  echo "step 1 more output" > "$PA_TASK_LOG_DIR/6.log"
  echo "1 $(( $(date +%s) - 1860 ))" > "$logs/progress_6"   # サイズが変わっている
  run "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
  # スナップショットが更新されている
  size=$(wc -c < "$PA_TASK_LOG_DIR/6.log" | tr -d ' ')
  [[ "$(cat "$logs/progress_6")" == "$size "* ]]
}

@test "deep check fires on Nth healthy check and resets counter" {
  running_status 7
  echo ok > "$PA_TASK_LOG_DIR/7.log"
  echo 5 > "$logs/check_count"   # deep_check_every=6 → 今回が6回目
  run "$PA_BIN" sentinel "$proj"
  grep -q "mode: deep_check" "$MOCK_AGENT_LOG"
  [ "$(cat "$logs/check_count")" = "0" ]
}

@test "other groups' tasks are ignored" {
  cat > "$MOCK_PUEUE_STATUS_JSON" <<'EOF'
{"tasks":{"9":{"id":9,"group":"other","command":"x",
 "status":{"Done":{"result":{"Failed":1}}}}}}
EOF
  run "$PA_BIN" sentinel "$proj"
  [ ! -f "$MOCK_AGENT_LOG" ]
}
