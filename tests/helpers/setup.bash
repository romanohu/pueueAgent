# 共通テストセットアップ。各 .bats から load される。
REPO_ROOT="$(cd "$(dirname "$BATS_TEST_FILENAME")/.." && pwd)"
PA_BIN="$REPO_ROOT/bin/pueue-agent"

# 一時プロジェクトを作る: .pueue-agent/ 入りのディレクトリ (group は pa-proj に設定)
make_project() {
  local dir="$BATS_TEST_TMPDIR/proj"
  mkdir -p "$dir/.pueue-agent/logs"
  if [ -f "$REPO_ROOT/templates/config.yml" ]; then
    sed 's/^  group: .*/  group: "pa-proj"/' "$REPO_ROOT/templates/config.yml" \
      > "$dir/.pueue-agent/config.yml"
  fi
  echo "$dir"
}
