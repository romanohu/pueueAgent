load helpers/setup

setup() {
  target="$BATS_TEST_TMPDIR/myrepo"
  mkdir -p "$target"
  git -C "$target" init -q
}

@test "init creates .pueue-agent with all files" {
  run "$PA_BIN" init --agent-cmd 'echo {prompt}' --git-mode commit "$target"
  [ "$status" -eq 0 ]
  [ -f "$target/.pueue-agent/config.yml" ]
  [ -f "$target/.pueue-agent/STATE.md" ]
  [ -f "$target/.pueue-agent/instructions.md" ]
  [ -d "$target/.pueue-agent/logs" ]
}

@test "init writes agent command and derived group into config" {
  "$PA_BIN" init --agent-cmd 'codex exec {prompt}' --git-mode commit "$target"
  grep -q 'command: "codex exec {prompt}"' "$target/.pueue-agent/config.yml"
  grep -q 'group: "pa-myrepo"' "$target/.pueue-agent/config.yml"
}

@test "git-mode ignore appends to .gitignore" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  grep -qx '.pueue-agent/' "$target/.gitignore"
}

@test "git-mode mixed ignores state and logs only" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode mixed "$target"
  grep -qx '.pueue-agent/STATE.md' "$target/.gitignore"
  grep -qx '.pueue-agent/logs/' "$target/.gitignore"
}

@test "init twice does not duplicate gitignore entries and refuses overwrite" {
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  run "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode ignore "$target"
  [ "$status" -eq 1 ]
  [ "$(grep -cx '.pueue-agent/' "$target/.gitignore")" -eq 1 ]
}

@test "group name is sanitized" {
  weird="$BATS_TEST_TMPDIR/My_Weird Repo!"
  mkdir -p "$weird"
  "$PA_BIN" init --agent-cmd 'x {prompt}' --git-mode commit "$weird"
  grep -q 'group: "pa-my-weird-repo"' "$weird/.pueue-agent/config.yml"
}
