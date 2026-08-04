#!/usr/bin/env bash
# pueue-agent 共通関数。他の lib/*.sh と bin/pueue-agent から source される。

pa_die() {
  echo "pueue-agent: error: $*" >&2
  exit 1
}

# プロジェクトルート探索: 引数 or $PWD から上へ .pueue-agent/ を探す
pa_find_project() {
  local dir="${1:-$PWD}"
  dir="$(cd "$dir" 2>/dev/null && pwd)" || return 1
  while [ "$dir" != "/" ]; do
    if [ -d "$dir/.pueue-agent" ]; then
      echo "$dir"
      return 0
    fi
    dir="$(dirname "$dir")"
  done
  return 1
}

pa_set_project() {
  PA_PROJECT="$(cd "$1" && pwd)" || pa_die "no such project dir: $1"
  PA_DIR="$PA_PROJECT/.pueue-agent"
  [ -d "$PA_DIR" ] || pa_die "not initialized: $PA_DIR missing (run: pueue-agent init)"
  mkdir -p "$PA_DIR/logs"
}

pa_log() {
  local ts
  ts="$(date '+%Y-%m-%dT%H:%M:%S')"
  echo "$ts $*" >> "$PA_DIR/logs/runtime.log"
}
