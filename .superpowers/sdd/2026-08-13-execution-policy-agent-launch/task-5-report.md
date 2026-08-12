# Task 5 report: policy violation classification and direct dead-letter

## RED

- Added `policy_blocked_claim_dead_letters_without_retry_or_run`.
- Initial focused run failed to compile because `EventRepository::dead_letter_claimed_without_run` was not implemented.

## GREEN

- Added bounded `EventResolution::PolicyBlocked { code, stage }` classification.
- Added `AgentSpawnError.policy` and scheduler event-boundary handling for direct policy dead-letter.
- Added atomic `dead_letter_claimed_without_run` validation and transition; attempts are ignored, leases are cleared, and no run is created.
- Added policy-aware pre-marker and post-marker run finalization, preserving intervention semantics.
- Focused database policy tests: PASS (4 tests).
- Scheduler integration tests: PASS (45 tests).
- `cargo test --all-targets`: PASS.
- `git diff --check`: PASS.
- `cargo fmt --all -- --check`: unavailable (`cargo fmt` is not installed in the host toolchain).

## Commit

Pending commit: `feat: direct-dead-letter policy violations`.
