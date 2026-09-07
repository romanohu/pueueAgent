#!/usr/bin/env bash
set -eu

# Capability probes run under a cleared environment with no HOME, while
# supervised decision agents receive the sanitized HOME whose parent is the
# harness work directory, so the fallback lands on the asserted log paths.
capture="${PUEUE_AGENT_TEST_CODEX_LOG:-}"
captured_env_names="${PUEUE_AGENT_TEST_CODEX_ENV_NAMES:-}"
if [ -z "$capture" ] && [ -n "${HOME:-}" ]; then
  capture="$HOME/../codex-calls.log"
fi
if [ -z "$captured_env_names" ] && [ -n "${HOME:-}" ]; then
  captured_env_names="$HOME/../codex-env-names.log"
fi

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
if [ -n "$capture" ]; then
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
fi

[ -n "$output" ] || exit 0
[ -n "$prompt" ] || exit 70

# Code-change editor fixture mode: the supervisor supplies a private,
# descriptor-backed editor output path.  Keep this branch before decision
# context parsing because editor prompts are not decision-context JSON.
editor_artifact=0
case "$output" in
  */editor.json) editor_artifact=1 ;;
esac
if [ "$editor_artifact" -eq 1 ] || [ -n "${PUEUE_AGENT_EDITOR_OUTPUT:-}" ]; then
  editor_output="$output"
  [ -n "$editor_output" ] || editor_output="${PUEUE_AGENT_EDITOR_OUTPUT:-}"
  [ -n "$editor_output" ] || exit 70
  if [ -n "${PUEUE_AGENT_TEST_EDITOR_CAPTURE:-}" ]; then
    {
      printf 'EDITOR_INVOCATION output=%s\n' "$editor_output"
      if [ -n "${PUEUE_AGENT_EDITOR_SESSION_ID+x}" ]; then
        printf 'EDITOR_SESSION_ID=%s\n' "$PUEUE_AGENT_EDITOR_SESSION_ID"
      fi
    } >> "$PUEUE_AGENT_TEST_EDITOR_CAPTURE"
  fi
  case "${PUEUE_AGENT_TEST_EDITOR_OUTPUT_MODE:-ready}" in
    malformed)
      printf '%s\n' '{malformed-editor' > "$editor_output"
      exit 0
      ;;
    oversized)
      head -c 65537 /dev/zero | tr '\0' 'x' > "$editor_output"
      exit 0
      ;;
    cannot_apply)
      jq -cn '{schema_version:1,status:"cannot_apply",summary:"editor cannot apply the requested change",proposed_checks:[]}' > "$editor_output"
      exit 0
      ;;
    fail)
      exit 17
      ;;
    *)
      editor_scenario=""
      case "$prompt" in
        *PUEUE_AGENT_E2E_CODE_CHANGE_SUCCESS*) editor_scenario="success" ;;
        *PUEUE_AGENT_E2E_CODE_CHANGE_SECOND_CHECK_FAIL*) editor_scenario="second_check_fail" ;;
        *PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_OOM*) editor_scenario="runtime_oom" ;;
        *PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_INTERNAL*) editor_scenario="runtime_internal" ;;
      esac
      case "$editor_scenario:${PUEUE_AGENT_EDITOR_MODE:-fresh}" in
        success:fresh|second_check_fail:fresh|second_check_fail:resume)
          printf '%s\n' 'def score():' '    return 2' > model.py
          ;;
        success:resume|runtime_oom:fresh|runtime_internal:fresh)
          printf '%s\n' 'def score():' '    return 1' > model.py
          if [ "$editor_scenario" = "success" ] && [ "${PUEUE_AGENT_EDITOR_MODE:-fresh}" = "resume" ]; then
            printf '%s\n' '# corrected candidate' >> model.py
          fi
          ;;
      esac
      if [ -n "$editor_scenario" ]; then
        jq -cn '{schema_version:1,status:"ready",summary:"editor prepared Python candidate",proposed_checks:[{source:"python",argv:["python","-m","pytest"],working_directory:"."}]}' > "$editor_output"
      else
        jq -cn '{schema_version:1,status:"ready",summary:"editor prepared candidate",proposed_checks:[{source:"cargo",argv:["cargo","test","--all-targets","--","--test-threads=1"],working_directory:"."}]}' > "$editor_output"
      fi
      exit 0
      ;;
  esac
fi

# Diagnosis-agent fixture mode: triggered by argv containing
# --output-last-message plus PUEUE_AGENT_TEST_DIAGNOSE_MODE=1, or by a
# health-diagnosis output artifact because the supervised Codex environment
# strips extra variables.  It must run before the decision context parsing
# below because diagnosis prompts carry a different evidence bundle.
diagnose_artifact=0
case "$output" in
  */health-diagnosis.json) diagnose_artifact=1 ;;
esac
if [ "${PUEUE_AGENT_TEST_DIAGNOSE_MODE:-}" = "1" ] || [ "$diagnose_artifact" -eq 1 ]; then
  if [ -n "${PUEUE_AGENT_TEST_DIAGNOSE_CAPTURE:-}" ]; then
    printf 'DIAGNOSE_INVOCATION output=%s\n' "$output" \
      >> "$PUEUE_AGENT_TEST_DIAGNOSE_CAPTURE"
  fi
  case "${PUEUE_AGENT_TEST_DIAGNOSE_OUTPUT:-valid}" in
    malformed)
      printf '%s\n' '{malformed-diagnosis' > "$output"
      exit 0
      ;;
    *)
      printf '%s\n' '{"root_cause_class":"oom","confidence":0.9,"recommended_action":"kill_and_resume","summary":"gpu exhausted"}' > "$output"
      exit 0
      ;;
  esac
fi

context="${prompt#*$'\n'}"
source_experiment_id="$(printf '%s' "$context" | jq -er '.source_experiment.experiment_id')"
source_status="$(printf '%s' "$context" | jq -er '.source_experiment.status')"
failure_fingerprint="$(printf '%s' "$context" | jq -r '.source_experiment.failure_fingerprint // ""')"
objective="$(printf '%s' "$context" | jq -er '.objective.text')"

call_number=1
if [ -n "$capture" ]; then
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
fi
if [ -n "$captured_env_names" ]; then
  while IFS= read -r name; do
    printf '%s\n' "$name"
  done < <(compgen -e) >> "$captured_env_names"
fi

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
  *PUEUE_AGENT_E2E_GOAL*)
    jq -cn --arg ref "$source_experiment_id" '{schema_version:1,decision:"goal_reached",evidence_ref:$ref,proposal:null,reason:null,requested_wait_minutes:null,expected_evidence:null}' \
      > "$output"
    exit 0
    ;;
esac

code_change_argv='["python","train.py"]'
case "$objective" in
  *PUEUE_AGENT_E2E_CODE_CHANGE_SUCCESS*) ;;
  *PUEUE_AGENT_E2E_CODE_CHANGE_SECOND_CHECK_FAIL*) ;;
  *PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_OOM*) code_change_argv='["python","train_oom.py"]' ;;
  *PUEUE_AGENT_E2E_CODE_CHANGE_RUNTIME_INTERNAL*) code_change_argv='["python","train_internal.py"]' ;;
  *) code_change_argv="" ;;
esac
if [ -n "$code_change_argv" ]; then
  jq -cn \
    --arg source_experiment_id "$source_experiment_id" \
    --argjson argv "$code_change_argv" \
    '{schema_version:1,decision:"proposal",proposal:{kind:"code_change",hypothesis:"make the deterministic smoke check pass",source_experiment_id:$source_experiment_id,argv:$argv,working_directory:".",expected_evidence:["loss"]},reason:null,requested_wait_minutes:null,expected_evidence:null,evidence_ref:null}' \
    > "$output"
  exit 0
fi

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
