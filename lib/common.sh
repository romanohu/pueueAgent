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

# config.yml (YAMLサブセット) から section.key のスカラ値を読む
# usage: pa_config section.key [default]
pa_config() {
  local key="$1" default="${2-}"
  local section="${key%%.*}" name="${key#*.}"
  local file="$PA_DIR/config.yml" val
  [ -f "$file" ] || pa_die "config not found: $file"
  val=$(awk -v section="$section" -v name="$name" '
    /^[^ #]/ { in_section = ($0 == section ":") ; next }
    in_section {
      line = $0
      sub(/^  /, "", line)
      if (index(line, name ":") == 1) {
        sub(/^[^:]*:[ ]*/, "", line)
        sub(/[ ]*(#.*)?$/, "", line)      # 行末コメント除去
        gsub(/^"|"$/, "", line)           # 引用符除去
        print line
        exit
      }
    }
  ' "$file")
  if [ -n "$val" ]; then echo "$val"; else echo "$default"; fi
}

# リスト値を1行1要素で出力。 "key: []" は空。
pa_config_list() {
  local key="$1"
  local section="${key%%.*}" name="${key#*.}"
  local file="$PA_DIR/config.yml"
  [ -f "$file" ] || pa_die "config not found: $file"
  awk -v section="$section" -v name="$name" '
    /^[^ #]/ { in_section = ($0 == section ":"); in_list = 0; next }
    in_section {
      line = $0
      sub(/^  /, "", line)
      if (index(line, name ":") == 1) {
        rest = line
        sub(/^[^:]*:[ ]*/, "", rest)
        in_list = (rest == "" || rest == "[]") ? (rest == "") : 0
        next
      }
      if (in_list && line ~ /^  - /) {
        sub(/^  - /, "", line)
        gsub(/^"|"$/, "", line)
        print line
        next
      }
      if (line !~ /^  / ) in_list = 0
    }
  ' "$file"
}
