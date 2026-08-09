# Task 7 report: policy-controlled Pueue termination

## Summary

Implemented policy-controlled termination requests and execution around the Pueue task-control boundary.

- Added `src/termination.rs` with:
  - `TerminationPolicy`
  - `TerminationManager::request`
  - `TerminationManager::execute`
  - `TerminationOutcome`
  - terminal-observation helpers for auto-kill confirmation
- Added `tests/integration/termination.rs` and registered it in `Cargo.toml`.
- Extended observations to preserve both:
  - stable incident task keys for recovery across lifecycle transitions
  - full Pueue task signatures for termination revalidation
- Integrated kill policy with `IncidentStore`.
- Integrated terminal reconciliation with confirmed auto-kill requests.
- Kept all task termination through `PueueApi::kill`; no raw OS signal, process group, or shell termination path was added.

## Behavior implemented

### Policy and request creation

- `TerminationPolicy` maps only active `PatternAction::Kill` task observations with a full task signature to termination.
- Notify and Wake observations do not create termination requests.
- Extra-log observations without a task signature do not create termination requests.
- `IncidentStore` creates an idempotent termination request after the active incident upsert.
- The request stores a JSON reason containing:
  - incident/observation kind
  - matched pattern name
  - policy action
  - confirmation count
  - bounded evidence, capped to 1024 chars
  - source path when present

### Execution

- `TerminationManager::execute`:
  - loads the request and registered project
  - reads fresh authoritative Pueue status before kill
  - requires exact registered group match
  - requires exact full running task signature match
  - requires the task state to be `Running`
  - transitions the request to `sent` before invoking Pueue
  - invokes only `PueueApi::kill(task.id)`
- Status errors are propagated and leave the request in its prior status.
- Failed Pueue kills mark the request `failed`, write `last_error`, and create an idempotent `termination_failed` event.
- Successful Pueue kill acceptance leaves the request `sent`; the request is confirmed only when reconciliation later observes a terminal matching task.

### Reconciliation

- Reconciliation checks terminal tasks for matching sent/confirmed termination requests.
- Matching uses the full request signature when exact, and otherwise compares the original signature identity fields:
  - group
  - task ID
  - enqueued timestamp
  - started timestamp
- This avoids numeric-ID-only matching while still allowing a running signature to match the later terminal state.
- Matching terminal tasks materialize `auto_killed` instead of generic `task_failed`, and confirm the request.
- Existing unknown-group handling is unchanged.

## Tests added

`tests/integration/termination.rs` covers:

- policy maps only explicit kill observations to termination
- explicit kill policy kills only the matching running task
- duplicate observations/request cycles do not duplicate kill calls
- stalled/default notify behavior does not kill
- full signature revalidation blocks stale task identity
- status errors are not treated as idle or safe-to-kill
- failed Pueue kill remains visible and does not create `auto_killed`

## Verification commands

The Rust toolchain is not available in this execution environment. Both sandbox and escalated host attempts returned `command not found`.

Attempted:

```text
cargo test --test termination
zsh:1: command not found: cargo

cargo test --all-targets
zsh:1: command not found: cargo

cargo test --offline --test termination
zsh:1: command not found: cargo

cargo test --offline --all-targets
zsh:1: command not found: cargo

cargo fmt --check
zsh:1: command not found: cargo

cargo clippy --all-targets --all-features -- -D warnings
zsh:1: command not found: cargo

rustfmt --check src/termination.rs tests/integration/termination.rs
zsh:1: command not found: rustfmt
```

Static checks that did run:

```text
git diff --check
exit 0

rg -n "std::process::Command|tokio::process::Command|libc::kill|nix::sys::signal|killpg|process_group|sh -c|bash -c|zsh -c" src/termination.rs src/incidents.rs src/reconcile.rs src/pueue.rs tests/integration/termination.rs
src/pueue.rs:7:use tokio::process::Command
```

The `tokio::process::Command` result is the existing Pueue adapter boundary in `src/pueue.rs`; no new shell, signal, process-group, or raw OS kill path was introduced.

## Risks and follow-ups

- Rust tests, formatting, and clippy could not be executed here because Cargo/rustfmt are not installed or not on PATH.
- The implementation should be verified in an environment with the Rust toolchain before merging.
- `TerminationOutcome::TimedOut` is modeled but no timeout mechanism is implemented in Task 7; timeout scheduling/polling remains follow-up work.
- `auto_killed` is emitted only after terminal reconciliation observes the task. If Pueue accepts `kill` but the task remains non-terminal for a while, the request remains `sent` and visible for later cycles.
- Terminal matching intentionally compares original full-signature identity fields except state/end timestamp so a running request can match its later terminal observation without relying on numeric ID alone.

## Fix round: reviewer blocking findings

### Summary

Addressed the four reviewer findings with focused lifecycle and reuse regressions:

- Added guarded SQLite status transitions for termination requests.
- Changed `execute` so only `requested -> sent` claim winners invoke `PueueApi::kill`.
- Changed `sent` execution to wait for reconciliation or time out; it never invokes kill again.
- Kept `pueue kill` outside SQLite transactions.
- Changed terminal reconciliation to consider only `sent` requests for `auto_killed`.
- Added sent-request timeout handling that marks `timed_out`, emits `termination_failed`, and does not call Pueue again.
- Tightened running-to-terminal fallback matching to require present and equal `enqueued_at` and `started_at`.

### Changed files

- `src/db/repositories.rs`
  - Added guarded compare-and-set style update helpers:
    - `transition_status_if_current`
    - `update_result_if_current`
- `src/termination.rs`
  - Added requested-only claim before kill.
  - Added sent timeout handling via `grace_until`.
  - Excluded confirmed/already-terminal requests from auto-kill reconciliation.
  - Added timeout `termination_failed` event materialization from stored request identity.
  - Required present lifecycle timestamps for terminal fallback matching.
- `tests/integration/termination.rs`
  - Added regressions for duplicate sent execution, already-terminal/no-kill reconciliation, sent timeout, and missing-timestamp task-ID reuse fallback.

### Tests and command results

Rust toolchain commands could not run in this environment because `cargo` is not installed or not on PATH:

```text
cargo test --test termination
zsh:1: command not found: cargo

cargo test --offline --test termination
zsh:1: command not found: cargo

cargo test --all-targets
zsh:1: command not found: cargo

cargo test --offline --all-targets
zsh:1: command not found: cargo

cargo fmt --check
zsh:1: command not found: cargo

cargo clippy --all-targets --all-features -- -D warnings
zsh:1: command not found: cargo

cargo clippy --offline --all-targets --all-features -- -D warnings
zsh:1: command not found: cargo
```

Static checks that did run:

```text
git diff --check
exit 0

rg -n "std::process::Command|tokio::process::Command|libc::kill|nix::sys::signal|killpg|process_group|sh -c|bash -c|zsh -c" src/termination.rs src/incidents.rs src/reconcile.rs src/pueue.rs tests/integration/termination.rs
src/pueue.rs:7:use tokio::process::Command;
```

The `tokio::process::Command` match is the existing Pueue adapter boundary. No raw OS signal, process-group kill, shell execution, or agent-start path was added.

### Commit

- `fix: harden termination request lifecycle`

### Remaining risks

- The new tests, formatting, and clippy need to be run in an environment with the Rust toolchain.
- `TerminationOutcome::Confirmed` still represents "kill accepted or sent request still awaiting reconciliation" in `execute`; durable `auto_killed` confirmation remains reconciliation-owned.

## Fix round 2: sent-only result transitions

### Summary

Addressed the scoped re-review race around confirmation/failure result updates:

- Changed terminal auto-kill confirmation to use `sent -> confirmed` compare-and-set.
- Changed reconciliation to confirm the request before materializing `auto_killed`; if timeout/failure wins the CAS race, reconciliation falls back to the normal terminal event instead of emitting `auto_killed`.
- Preserved idempotency for already-confirmed auto-kill reconciliation by treating matching `confirmed` requests with no `last_error` as prior successful auto-kill confirmations.
- Changed the kill-error path to use `sent -> failed` compare-and-set after the external `pueue kill` await. If a concurrent timeout or confirmation wins while kill is awaiting, the late kill error no longer overwrites that state and no inconsistent `termination_failed` event is inserted.
- Kept timeout handling on guarded `sent -> timed_out`.
- Kept `pueue kill` outside SQLite transactions, and did not add raw signals, shell execution, process-group termination, or agent starts.

### Changed files

- `src/termination.rs`
  - Added `AutoKillConfirmation` to distinguish fresh confirmation, already-confirmed idempotency, and stale non-sent requests.
  - Guarded auto-kill confirmation with `update_result_if_current(sent, confirmed, ...)`.
  - Guarded late kill failure with `update_result_if_current(sent, failed, ...)`.
  - Reloaded the current request state when guarded transitions lose a race.
  - Included only `sent` or prior successful `confirmed` matching requests in terminal auto-kill reconciliation candidates.
- `src/reconcile.rs`
  - Reordered terminal handling so auto-kill confirmation happens before `auto_killed` event materialization.
  - Emits `auto_killed` only when confirmation wins or is already confirmed idempotently; emits normal terminal events when a stale sent request lost to timeout/failure.
- `tests/integration/termination.rs`
  - Added a regression that `confirm_auto_kill_terminal_observation` does not overwrite a concurrently timed-out request.
  - Added a deterministic async race where `execute` is blocked in external kill, reconciliation confirms the sent request, and a late kill error cannot overwrite confirmation or create `termination_failed`.
  - Added a deterministic async race where `execute` is blocked in external kill, timeout wins the sent request, and a late kill error cannot overwrite `timed_out` or create an extra failure transition.
  - Extended the existing duplicate-cycle regression to assert a confirmed auto-kill does not later create a generic `task_failed` event.

### Tests and command results

The Rust toolchain is still not available in this execution environment (`cargo`, `rustfmt`, and `rustup` are not installed or not on PATH), so Rust tests, formatting, and clippy could not be executed here.

Attempted:

```text
cargo test --test termination
zsh:1: command not found: cargo

cargo test --offline --test termination
zsh:1: command not found: cargo

cargo test --offline --all-targets
zsh:1: command not found: cargo

cargo test --all-targets
zsh:1: command not found: cargo

cargo fmt --check
zsh:1: command not found: cargo

cargo clippy --offline --all-targets --all-features -- -D warnings
zsh:1: command not found: cargo

rustfmt --check src/termination.rs src/reconcile.rs tests/integration/termination.rs
zsh:1: command not found: rustfmt
```

Static checks that did run:

```text
git diff --check
exit 0

rg -n "std::process::Command|tokio::process::Command|libc::kill|nix::sys::signal|killpg|process_group|sh -c|bash -c|zsh -c|AgentRunRepository|start_agent|spawn_agent" src/termination.rs src/incidents.rs src/reconcile.rs src/pueue.rs tests/integration/termination.rs
src/pueue.rs:7:use tokio::process::Command;
```

The `tokio::process::Command` match is the existing Pueue adapter boundary. No raw OS signal, process-group kill, shell execution, or agent-start path was added.

### Remaining risks

- The new concurrency regressions and formatting need to be run in an environment with the Rust toolchain before merging.
- The fix relies on `last_error IS NULL` to distinguish prior successful auto-kill confirmation from requested-stage already-terminal confirmation; current requested-stage confirmations write a non-null explanatory `last_error`.
