#!/usr/bin/env bash
set -eu
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${PA_INSTALL_PREFIX:-$HOME/.local/bin}"

command -v jq >/dev/null 2>&1 || { echo "error: jq が必要です" >&2; exit 1; }
command -v pueue >/dev/null 2>&1 || echo "warning: pueue が見つかりません(サーバー側では必須)" >&2

mkdir -p "$PREFIX"
ln -sf "$REPO_ROOT/bin/pueue-agent" "$PREFIX/pueue-agent"
echo "installed: $PREFIX/pueue-agent"
case ":$PATH:" in
  *":$PREFIX:"*) : ;;
  *) echo "note: $PREFIX が PATH に含まれていません" ;;
esac
