#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"$SCRIPT_DIR/rust_supervisor.sh" "$@"
"$SCRIPT_DIR/research_supervisor.sh" "$@"
exec "$SCRIPT_DIR/trial_supervisor.sh" "$@"
