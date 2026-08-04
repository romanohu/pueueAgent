load helpers/setup

# make_project の派生: 任意のディレクトリ・group 名でプロジェクトを作る
# (前方一致するパスを持つ複数プロジェクトのテスト用)
make_project_at() {  # $1=dir $2=group
  local dir="$1" group="$2"
  mkdir -p "$dir/.pueue-agent/logs"
  sed "s/^  group: .*/  group: \"$group\"/" "$REPO_ROOT/templates/config.yml" \
    > "$dir/.pueue-agent/config.yml"
  cp "$REPO_ROOT/templates/STATE.md" "$dir/.pueue-agent/STATE.md"
  sed "s/{{GROUP}}/$group/g" "$REPO_ROOT/templates/instructions.md" \
    > "$dir/.pueue-agent/instructions.md"
  echo "$dir"
}

setup() {
  proj="$(make_project)"
  export PA_REGISTRY="$BATS_TEST_TMPDIR/registry"
  export PA_CRONTAB_BIN="$REPO_ROOT/tests/helpers/mock_crontab.sh"
  export MOCK_CRONTAB_FILE="$BATS_TEST_TMPDIR/crontab"
  export PA_PUEUE_BIN="$REPO_ROOT/tests/helpers/mock_pueue.sh"
  export MOCK_PUEUE_STATUS_JSON="$BATS_TEST_TMPDIR/status.json"
  export PA_PUEUE_CONFIG="$BATS_TEST_TMPDIR/pueue.yml"
  echo '{"tasks":{}}' > "$MOCK_PUEUE_STATUS_JSON"
  cat > "$PA_PUEUE_CONFIG" <<'EOF'
daemon:
  callback: null
  callback_log_lines: 10
EOF
}

@test "enable registers cron, registry, and patches callback" {
  run "$PA_BIN" enable "$proj"
  [ "$status" -eq 0 ]
  grep -q "sentinel '$proj'" "$MOCK_CRONTAB_FILE"
  grep -q "pueue-agent:$proj" "$MOCK_CRONTAB_FILE"
  grep -q "\*/10 \* \* \* \*" "$MOCK_CRONTAB_FILE"
  grep -q $'^pa-proj\t' "$PA_REGISTRY"
  grep -q "pueue-agent' callback {{ id }} {{ group }}" "$PA_PUEUE_CONFIG"
  [[ "$output" == *"pueued"*  ]]   # 再起動の案内
}

@test "enable is idempotent (no duplicate cron lines)" {
  "$PA_BIN" enable "$proj"
  "$PA_BIN" enable "$proj"
  [ "$(grep -c "pueue-agent:$proj" "$MOCK_CRONTAB_FILE")" -eq 1 ]
}

@test "enable warns when callback is set to something else" {
  sed -i.bak 's|callback: null|callback: "notify-send hi"|' "$PA_PUEUE_CONFIG"
  run "$PA_BIN" enable "$proj"
  [ "$status" -eq 0 ]
  [[ "$output" == *"warning"* ]]
  grep -q 'callback: "notify-send hi"' "$PA_PUEUE_CONFIG"   # 上書きしない
}

@test "disable removes cron and registry" {
  "$PA_BIN" enable "$proj"
  run "$PA_BIN" disable "$proj"
  ! grep -q "pueue-agent:$proj" "$MOCK_CRONTAB_FILE"
  ! grep -q $'^pa-proj\t' "$PA_REGISTRY"
}

@test "disable keeps other projects' cron entries" {
  "$PA_BIN" enable "$proj"
  echo "* * * * * other-job # pueue-agent:/other/proj" >> "$MOCK_CRONTAB_FILE"
  "$PA_BIN" disable "$proj"
  grep -q "/other/proj" "$MOCK_CRONTAB_FILE"
}

@test "submit adds task to project group" {
  export MOCK_PUEUE_CALLS="$BATS_TEST_TMPDIR/pueue_calls"
  run bash -c "cd '$proj' && '$PA_BIN' submit -- python train.py --lr 0.1"
  [ "$status" -eq 0 ]
  grep -q -- "add -g pa-proj -- python train.py --lr 0.1" "$MOCK_PUEUE_CALLS"
}

@test "disable does not strip a cron entry whose path is a prefix of another project's path" {
  short="$(make_project_at "$BATS_TEST_TMPDIR/aa" "pa-aa")"
  long="$(make_project_at "$BATS_TEST_TMPDIR/aab" "pa-aab")"
  "$PA_BIN" enable "$short"
  "$PA_BIN" enable "$long"
  "$PA_BIN" disable "$short"
  ! grep -q "pueue-agent:${short}\$" "$MOCK_CRONTAB_FILE"
  grep -q "pueue-agent:${long}\$" "$MOCK_CRONTAB_FILE"
}

@test "disable keeps cron/registry cleanup even when pueue group remove fails (tasks remain)" {
  export MOCK_PUEUE_CALLS="$BATS_TEST_TMPDIR/pueue_calls"
  export MOCK_PUEUE_GROUP_FAIL=1
  "$PA_BIN" enable "$proj"
  run "$PA_BIN" disable "$proj"
  [ "$status" -eq 0 ]
  echo "$output" | grep -qi warning
  ! grep -q "pueue-agent:$proj" "$MOCK_CRONTAB_FILE"
  ! grep -q $'^pa-proj\t' "$PA_REGISTRY"
}
