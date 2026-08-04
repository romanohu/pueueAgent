# 共通テストセットアップ。各 .bats から load される。
REPO_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
PA_BIN="$REPO_ROOT/bin/pueue-agent"

# 一時プロジェクトを作る: .pueue-agent/ 入りのディレクトリ (group は pa-proj に設定)
make_project() {
  local dir="$BATS_TEST_TMPDIR/proj"
  mkdir -p "$dir/.pueue-agent/logs"
  sed 's/^  group: .*/  group: "pa-proj"/' "$REPO_ROOT/templates/config.yml" \
    > "$dir/.pueue-agent/config.yml"
  cp "$REPO_ROOT/templates/STATE.md" "$dir/.pueue-agent/STATE.md"
  sed 's/{{GROUP}}/pa-proj/g' "$REPO_ROOT/templates/instructions.md" \
    > "$dir/.pueue-agent/instructions.md"
  echo "$dir"
}
