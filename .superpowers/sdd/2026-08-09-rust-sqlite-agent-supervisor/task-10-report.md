## Task 10 Report: Status, pause/resume, and operator visibility

### Summary

Implemented operator-facing status rendering and project state transitions for the Rust/SQLite supervisor:

- Added `src/status.rs` for status text generation and pause/resume/disable helpers.
- Wired CLI `status`, `pause`, `resume`, and `disable` to the SQLite-backed project state.
- Added `disable --remove` as the explicit group-reservation release operation. Plain `disable` keeps the project row and Pueue group reserved.
- Added transactional repository transitions for pause, resume, halt, disable, and remove.
- Changed daemon automatic termination selection to only active projects (`enabled && !paused && halted_reason IS NULL`).
- Added focused operator integration tests covering:
  - failed termination visibility without reporting idle;
  - Pueue integration error visibility without reporting no active tasks;
  - pause blocking event claims and automatic termination until resume;
  - disable preserving group reservation;
  - explicit remove releasing the group reservation without controlling Pueue tasks.

### TDD / Red Result

Command:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test operator_commands
```

Result: failed as expected before implementation.

Failure:

```text
error[E0432]: unresolved import `pueue_agent::status`
```

This was the expected missing-feature failure for the new operator status module/API.

### Final Verification

Focused operator tests:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test operator_commands
```

Result:

```text
test result: ok. 5 passed; 0 failed; 0 ignored
```

Full offline all-target/all-features:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --all-targets --all-features
```

Result: all tests passed.

```text
125 passed; 0 failed
```

Formatting:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --all -- --check
```

Result: passed.

Clippy:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo clippy --offline --all-targets --all-features -- -D warnings
```

Result: passed.

Diff check:

```bash
git diff --check
```

Result: passed.

### Requirement Coverage

- Status exposes daemon health, project state, active tasks, event counts/recent kinds, integration errors, open incidents, termination request states, agent run states, guardrail counters, configured Codex context mode, and latest context lineage.
- Status does not print event payloads, so transcript-like payload content is not dumped.
- Pueue status integration errors render as `pueue: error: ...` and do not render `active_tasks: 0` or idle.
- Failed termination requests and `termination_failed` events remain visible in status.
- Pause is transactional and blocks new event claims through the existing claim eligibility query.
- Pause also blocks automatic termination because daemon termination now iterates only active projects.
- Resume transactionally clears `paused` and `halted_reason`, making pending events eligible again.
- Plain disable transactionally sets `enabled = 0` and `paused = 1` while preserving the project row and group reservation.
- Explicit `disable --remove` releases the SQLite group reservation but does not control Pueue tasks.
- Pueue task control remains through the existing adapter only.

### Risks / Notes

- `status` output is human-readable text, not a stable machine-readable schema.
- `disable --remove` releases the supervisor’s SQLite group reservation even if Pueue still has unresolved tasks; this is intentionally explicit and does not kill/remove Pueue tasks.
- CLI `status` uses the platform service manager health check. If the platform service query itself errors, the command returns that error.

## Fix Round 1 Report

### Summary

Addressed reviewer findings:

- Added durable SQLite `operator_logs` table and schema migration to version 4.
- Wrote operator log rows inside the same project-transition transaction for `pause`, `halt`, `resume`, `disable`, and `remove`.
- Kept remove logs durable after project deletion by storing `project_id`/`pueue_group` as log fields without a project foreign key.
- Made `disable_project` explicitly compute unresolved non-terminal Pueue task IDs for the project group before choosing the state transition.
- Plain disable always keeps the group reservation, for both unresolved and empty Pueue snapshots.
- Explicit remove remains the only release path and logs the intentional release even when unresolved tasks exist.

### Red Result

Command:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test operator_commands
```

Result: failed as expected before implementation.

```text
running 7 tests
test result: FAILED. 3 passed; 4 failed
```

Failure cause:

```text
no such table: operator_logs
```

This proved the new tests were checking durable database log rows, not CLI println output.

### Verification

Focused operator tests:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --test operator_commands
```

Result:

```text
test result: ok. 7 passed; 0 failed; 0 ignored
```

Focused database/operator migration check:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --test database --test operator_commands
```

Result:

```text
database: 18 passed; 0 failed
operator_commands: 7 passed; 0 failed
```

Full offline all-target/all-features:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --offline --all-targets --all-features
```

Result: all listed suites passed.

```text
127 passed; 0 failed
```

Formatting:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo fmt --all -- --check
```

Result: passed.

Clippy:

```bash
PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo clippy --offline --all-targets --all-features -- -D warnings
```

Result: passed.

Diff check:

```bash
git diff --check
```

Result: passed.

### Risks / Notes

- Existing SQLite databases migrate from schema version 3 to 4 by adding `operator_logs`; versions 1 and 2 also migrate directly to 4.
- `operator_logs` intentionally does not reference `projects` so remove operations can delete a project row while preserving the operator audit trail.
- Plain disable still disables the project when Pueue status is unavailable and keeps the group reservation; `disable --remove` still returns the Pueue status error before attempting release.
