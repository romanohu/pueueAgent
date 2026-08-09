#!/usr/bin/env bash
set -eu
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PREFIX="${PA_INSTALL_PREFIX:-$HOME/.local/bin}"

command -v cargo >/dev/null 2>&1 || {
  echo "error: Rust toolchain (cargo) is required" >&2
  exit 1
}
command -v pueue >/dev/null 2>&1 || echo "warning: pueue was not found; it is required at runtime" >&2

cargo build --locked --release --manifest-path "$REPO_ROOT/Cargo.toml"

mkdir -p "$PREFIX"
ln -sf "$REPO_ROOT/target/release/pueue-agent" "$PREFIX/pueue-agent"
echo "installed: $PREFIX/pueue-agent"
case ":$PATH:" in
  *":$PREFIX:"*) : ;;
  *) echo "note: $PREFIX が PATH に含まれていません" ;;
esac
