#!/usr/bin/env bash
set -eu

SOURCE="$0"
while [ -L "$SOURCE" ]; do
  DIR="$(cd "$(dirname "$SOURCE")" && pwd)"
  SOURCE="$(readlink "$SOURCE")"
  case "$SOURCE" in /*) : ;; *) SOURCE="$DIR/$SOURCE" ;; esac
done
REPO_ROOT="$(cd "$(dirname "$SOURCE")" && pwd)"
PREFIX="${PA_INSTALL_PREFIX:-$HOME/.local/bin}"
DESTINATION="$PREFIX/pueue-agent"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
case "$TARGET_DIR" in
  /*) : ;;
  *) TARGET_DIR="$(pwd)/$TARGET_DIR" ;;
esac

command -v cargo >/dev/null 2>&1 || {
  echo "error: Rust toolchain (cargo) is required" >&2
  exit 1
}
command -v pueue >/dev/null 2>&1 || echo "warning: pueue was not found; it is required at runtime" >&2

if [ -d "$DESTINATION" ] && [ ! -L "$DESTINATION" ]; then
  echo "error: install destination is a directory: $DESTINATION" >&2
  exit 1
fi

# Keep Cargo's output in sync with the installed link even when a user-wide
# Cargo configuration specifies another build.target-dir.
cargo build --locked --release --manifest-path "$REPO_ROOT/Cargo.toml" --target-dir "$TARGET_DIR"
BINARY="$TARGET_DIR/release/pueue-agent"
if [ ! -x "$BINARY" ]; then
  echo "error: Rust release binary is missing or not executable: $BINARY" >&2
  exit 1
fi

mkdir -p "$PREFIX"
# Do not follow an old installation link if it points to a directory.
ln -sfn "$BINARY" "$DESTINATION"
echo "installed: $DESTINATION"
case ":$PATH:" in
  *":$PREFIX:"*) : ;;
  *) echo "note: $PREFIX が PATH に含まれていません" ;;
esac
