#!/usr/bin/env bash
set -eu

capture="${PUEUE_AGENT_TEST_CODEX_LOG:-${HOME:?HOME is required}/../codex-calls.log}"
captured_env_names="${PUEUE_AGENT_TEST_CODEX_ENV_NAMES:-${HOME:?HOME is required}/../codex-env-names.log}"

if [ "$#" -eq 1 ] && [ "$1" = "--version" ]; then
  printf '%s\n' 'codex-cli 0.148.0'
  exit 0
fi
if [ "$#" -eq 1 ] && [ "$1" = "--help" ]; then
  printf '%s\n' '--strict-config --sandbox read-only workspace-write --ask-for-approval never'
  exit 0
fi
if [ "$#" -eq 2 ] && [ "$1" = "exec" ] && [ "$2" = "--help" ]; then
  printf '%s\n' '--ignore-user-config --ignore-rules --strict-config --output-schema --output-last-message'
  exit 0
fi

output=""
prompt=""
read_only_profile=0
default_read_only_profile=0
network_access="missing"
after_separator=0
expect_output=0
{
  if [ -n "${CODEX_HOME+x}" ]; then
    printf 'ENV_NAME=CODEX_HOME\n'
  fi
  printf 'ARGC=%s\n' "$#"
  index=1
  for argument in "$@"; do
    if [ "$after_separator" -eq 1 ]; then
      prompt="$argument"
      break
    fi
    printf 'ARG_%s=%s\n' "$index" "$argument"
    if [ "$expect_output" -eq 1 ]; then
      output="$argument"
      expect_output=0
    fi
    case "$argument" in
      --output-last-message)
        expect_output=1
        ;;
      'permissions.pueue_agent_decision.extends=":read-only"')
        read_only_profile=1
        ;;
      'default_permissions="pueue_agent_decision"')
        default_read_only_profile=1
        ;;
      permissions.pueue_agent_decision.network.enabled=*)
        network_access="${argument#*=}"
        ;;
      --)
        after_separator=1
        ;;
    esac
    index=$((index + 1))
  done
} >> "$capture"

[ -n "$output" ] || exit 0
[ -n "$prompt" ] || exit 70

context="${prompt#*$'\n'}"
source_experiment_id="$(printf '%s' "$context" | jq -er '.source_experiment.experiment_id')"
source_status="$(printf '%s' "$context" | jq -er '.source_experiment.status')"
failure_fingerprint="$(printf '%s' "$context" | jq -r '.source_experiment.failure_fingerprint // ""')"
objective="$(printf '%s' "$context" | jq -er '.objective.text')"

call_number=1
if [ -f "$capture" ]; then
  while IFS= read -r line; do
    case "$line" in
      "DECISION_INVOCATION source_experiment_id=$source_experiment_id "*)
        call_number=$((call_number + 1))
        ;;
    esac
  done < "$capture"
fi
{
  printf 'DECISION_INVOCATION source_experiment_id=%s call=%s\n' \
    "$source_experiment_id" "$call_number"
  if [ "$read_only_profile" -eq 1 ] && [ "$default_read_only_profile" -eq 1 ]; then
    printf 'sandbox_read_only=true\n'
  else
    printf 'sandbox_read_only=false\n'
  fi
  printf 'network_access=%s\n' "$network_access"
} >> "$capture"
while IFS= read -r name; do
  printf '%s\n' "$name"
done < <(compgen -e) >> "$captured_env_names"

case "$objective" in
  *PUEUE_AGENT_E2E_INVALID_THREE*)
    if [ "$call_number" -le 3 ]; then
      printf '%s\n' '{malformed-decision' > "$output"
      exit 0
    fi
    ;;
  *PUEUE_AGENT_E2E_WAIT_ONCE*)
    if [ "$call_number" -eq 1 ]; then
      jq -cn '{schema_version:1,decision:"wait",proposal:null,reason:"await one bounded observation window",requested_wait_minutes:1,expected_evidence:["next bounded observation"]}' \
        > "$output"
      exit 0
    fi
    ;;
esac

proposal_kind="experiment"
hypothesis="Run one bounded follow-up experiment"
if [ "$source_status" = "failed" ] && [ -n "$failure_fingerprint" ]; then
  proposal_kind="repair"
  hypothesis="Retry the trusted failure with a bounded repair"
fi
jq -cn \
  --arg kind "$proposal_kind" \
  --arg hypothesis "$hypothesis" \
  --arg source_experiment_id "$source_experiment_id" \
  '{schema_version:1,decision:"proposal",proposal:{kind:$kind,hypothesis:$hypothesis,source_experiment_id:$source_experiment_id,argv:["/bin/sleep","3600"],working_directory:".",expected_evidence:["bounded follow-up task identity"]},reason:null,requested_wait_minutes:null,expected_evidence:null}' \
  > "$output"
