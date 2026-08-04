#!/usr/bin/env bash
# pueue-agent init: プロジェクトに .pueue-agent/ を生成する

pa_gitignore_add() {  # 重複なしで .gitignore に1行追記
  local root="$1" entry="$2"
  grep -qx "$entry" "$root/.gitignore" 2>/dev/null && return 0
  echo "$entry" >> "$root/.gitignore"
}

pa_sanitize_group() {
  echo "$1" | tr '[:upper:]' '[:lower:]' | sed -e 's/[^a-z0-9]\{1,\}/-/g' -e 's/^-//' -e 's/-$//'
}

pa_cmd_init() {
  local agent_cmd="" git_mode="" group="" target=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --agent-cmd) agent_cmd="$2"; shift 2 ;;
      --git-mode)  git_mode="$2";  shift 2 ;;
      --group)     group="$2";     shift 2 ;;
      *)           target="$1";    shift ;;
    esac
  done
  target="${target:-$PWD}"
  target="$(cd "$target" && pwd)" || pa_die "no such dir: $target"
  [ -d "$target/.pueue-agent" ] && pa_die "already initialized: $target/.pueue-agent"

  # 対話フォールバック(TTY のみ)
  if [ -z "$agent_cmd" ]; then
    if [ -t 0 ]; then
      printf 'agent コマンド (例: claude -p {prompt} --permission-mode acceptEdits): ' >&2
      read -r agent_cmd
    else
      pa_die "--agent-cmd is required (non-interactive)"
    fi
  fi
  case "$agent_cmd" in
    *'{prompt}'*) : ;;
    *) pa_die "agent command must contain {prompt}" ;;
  esac
  if [ -z "$git_mode" ]; then
    if [ -t 0 ]; then
      printf 'git 管理 [commit/ignore/mixed] (default: mixed): ' >&2
      read -r git_mode
      git_mode="${git_mode:-mixed}"
    else
      pa_die "--git-mode is required (non-interactive)"
    fi
  fi
  case "$git_mode" in commit|ignore|mixed) : ;; *) pa_die "invalid --git-mode: $git_mode" ;; esac

  [ -n "$group" ] || group="pa-$(pa_sanitize_group "$(basename "$target")")"

  mkdir -p "$target/.pueue-agent/logs"
  # config: テンプレートに agent command と group を差し込む
  sed \
    -e "s|^  command: .*|  command: \"$(printf '%s' "$agent_cmd" | sed 's/[&|]/\\&/g')\"|" \
    -e "s|^  group: .*|  group: \"$group\"|" \
    "$PA_ROOT/templates/config.yml" > "$target/.pueue-agent/config.yml"
  cp "$PA_ROOT/templates/STATE.md" "$target/.pueue-agent/STATE.md"
  sed -e "s|{{GROUP}}|$group|g" "$PA_ROOT/templates/instructions.md" \
    > "$target/.pueue-agent/instructions.md"

  case "$git_mode" in
    ignore) pa_gitignore_add "$target" ".pueue-agent/" ;;
    mixed)  pa_gitignore_add "$target" ".pueue-agent/STATE.md"
            pa_gitignore_add "$target" ".pueue-agent/logs/" ;;
  esac

  echo "initialized $target/.pueue-agent (group: $group)"
  echo "next steps:"
  echo "  1. .pueue-agent/STATE.md に実験の目的・方針を書く"
  echo "  2. pueue-agent enable   # 監視を有効化"
  echo "  3. pueue-agent submit -- <実験コマンド>"
}
