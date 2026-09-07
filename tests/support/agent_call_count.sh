agent_call_count_for_project() {
  project_id="$1"
  if [ ! -f "$PUEUE_AGENT_TEST_AGENT_LOG" ]; then
    printf '0\n'
    return 0
  fi
  awk -v expected_project="$project_id" \
    '$0 == "PROJECT_ID=" expected_project { count++ } END { print count + 0 }' \
    "$PUEUE_AGENT_TEST_AGENT_LOG"
}
