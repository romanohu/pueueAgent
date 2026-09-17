# Task 3 implementation report

## Session ownership probe milestone

Implemented in `src/codex_session.rs`:

- Added `OwnedSessionProbe::{Owned, Missing}` and public `probe_owned_session`.
- Preserved the existing `verify_project_ownership` behavior while introducing a typed
  traversal result so only a safe, complete no-match is `Missing`.
- Kept malformed, foreign, invalid, unreadable, and traversal-limit cases as policy
  errors.
- Added focused coverage for empty stores, archived owned metadata, malformed metadata,
  foreign cwd, invalid IDs, unreadable homes, and the traversal limit.

TDD evidence (before the milestone commit):

- RED: `cargo test --lib codex_session` — exit 101; compile errors reported the missing
  `probe_owned_session` function and `OwnedSessionProbe` type referenced by the new tests.
- GREEN: `cargo test --lib codex_session` — exit 0; **19 passed, 0 failed, 324 filtered**.
- Scoped review: `git diff --check` — exit 0; only `src/codex_session.rs` was staged for the
  milestone commit.

Milestone commit: `c7fb89938fa75b507e58cf6861b968de75724c24`.

## Native-role workflow correction

The native-role implementation was initially written before its required
Task3.3 RED integration tests. Per the TDD workflow correction, that entire
uncommitted native-role slice was discarded with `apply_patch`; it was not
tested or retained as implementation evidence. No native-role RED command was
run before the discard. A fresh implementation must begin with
`tests/integration/research_agent.rs` and its Cargo registration, then record
the actual native-launch RED failure before production changes.

## Remaining implementation

Native `Research` role, campaign-owned session binding/persistence/finalization, focused
native integration coverage, and final verification remain in progress.

## Native RED stage (tests prepared; Linux controller run requested)

The scoped test-only change adds `tests/integration/research_agent.rs` and registers
`research_agent` in `Cargo.toml`.  The fixture follows `health_diagnosis.rs`: it creates
real SQLite campaign/review/evidence rows, claims the review event, reserves the
agent-run budget slot, acquires the existing run/project admission guards, and invokes
the future `AgentRunner::spawn_research` boundary.  The executable is a compiled Rust
fixture enrolled in the trusted execution policy; no shell pretending to be Codex is
used.

The tests are named for the production mistakes they catch:

- `research_first_native_launch_is_read_only_schema_bound_and_does_not_log_transcript`
  catches launching the ordinary/custom agent, missing the research schema/output
  descriptors, using a write-capable Codex profile, leaking the enabled-network policy,
  exposing the Codex API key, or copying the untrusted evidence/transcript into public
  logs.  It also requires `execution_kind = campaign_research` and first-launch fresh
  context.
- `research_changed_experiment_in_same_campaign_exactly_resumes_owned_session` catches
  using `resume_latest`, changing the session when only the experiment changed, and
  losing the campaign-owned session across a second review.
- `research_new_campaign_starts_a_distinct_fresh_session` catches reusing another
  campaign's session and treating a new campaign as an exact resume.

The fixture captures the actual native argv, prompt, sanitized environment, schema path,
output path, read-only permission profile, and network override.  It writes owned
session metadata and a schema-valid research answer so finalization can be exercised
once the production role exists.

The following Task 3.5 behaviors are intentionally not claimed by this RED slice and
need later tests: binding/session-generation failpoints (before binding,
after-binding/before-gate, after-child-exit/before-DB, after-DB/before-handle-cleanup),
partial or malformed output, timeout, wrong-session ownership, failed finalization,
retained cleanup/retry, no surviving unbound child, durable response identity/digest
validation, and restart recovery.

Controller command requested on Linux (the local macOS target compiles the test target
with zero tests and is not RED evidence):

```text
cargo test --locked --offline --test research_agent
```

At this checkpoint the expected first RED is an absent production `spawn_research`
launch API (and its related native research role), not a fixture compile failure.  No
production files were changed.
