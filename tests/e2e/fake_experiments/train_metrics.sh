#!/usr/bin/env bash
set -eu
# Write a manifest for the current experiment task. Uses env injected by campaign.
if [ -z "${PUEUE_AGENT_EXPERIMENT_ID:-}" ] || [ -z "${PUEUE_AGENT_RESULT_PATH:-}" ]; then
  echo "missing PUEUE_AGENT_EXPERIMENT_ID or RESULT_PATH" >&2
  exit 1
fi
mkdir -p "$(dirname "$PUEUE_AGENT_RESULT_PATH")"
# Use loss 0.42 as deterministic metric.
cat > "$PUEUE_AGENT_RESULT_PATH" <<EOF
{"schema_version":1,"experiment_id":"$PUEUE_AGENT_EXPERIMENT_ID","metrics":{"loss":0.42,"accuracy":0.91}}
EOF
echo "step 1 loss 0.42"
sleep 0.5
echo "final loss 0.42"
