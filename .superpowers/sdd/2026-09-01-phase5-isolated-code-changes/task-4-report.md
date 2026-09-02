# Task 4 report: resumable code-change editors

Date: 2026-09-02
Base: `748fe01866f17cbe0866078409dd9b27e681d4d7`

## RED evidence

The required editor fixtures and focused tests were added before the
production editor implementation. The first credible security RED was:

```text
cargo test --test codex_security code_change_editor -- --test-threads=1
1 selected, failed
ExecutionProjection rejected `code_change_editor` as an invalid execution kind
```

The first daemon RED invocation selected the three editor recovery tests but
stopped in the shared `DaemonHarness` at `RootChanged/Startup`: the roko test
umask created fixture roots as `0775`. This was fixture setup failure rather
than feature evidence, so the test roots and service/log directories were
secured to `0700` under `cfg(test)` before rerunning. The recovery inverse
tests then reached the missing editor-binding behavior. The preservation
fixture was corrected to bind the run and attempt to the same durable session
before the GREEN rerun.

## GREEN evidence

Linux/roko focused results (single-threaded unless noted):

- `cargo test --test daemon code_change_editor -- --test-threads=1`: 8 passed.
- `cargo test --test daemon code_change_editor_is_preserved_from_generic_startup_recovery -- --exact --test-threads=1`: 1 passed.
- `cargo test --test codex_security code_change_editor -- --test-threads=1`: 3 passed.
- `cargo test --test database code_change_editor -- --test-threads=1`: 3 passed.
- `cargo test --test database code_change_finish_methods_require_terminal_statuses -- --exact --test-threads=1`: 1 passed.
- `cargo test agent:: -- --test-threads=1`: 11 passed, 2 ignored.
- `cargo test --test native_agent_gate -- --test-threads=1`: 11 passed.
- `cargo test --test scheduler -- --test-threads=1`: 84 passed.
- `cargo test --lib code_change:: -- --test-threads=1`: 28 passed.
- `cargo test --test daemon code_change_worktree -- --test-threads=1`: 1 passed.
- `cargo test --test database -- --test-threads=1`: 239 passed.
- `cargo check --tests -j1`: passed with the pre-existing
  `EntryMountPrecheck` dead-code warning only.
- `git diff --check`: passed.

The process-level editor matrix covers fresh and exact same-session resume,
ready persistence with proposed checks, cannot-apply, malformed and oversized
output, timeout, first and second process failure, terminal-output restart
idempotence, and third-attempt rejection. Codex/custom argv and environment
security, durable attempt binding, and generic recovery preservation plus both
inverse contradictions are covered by the focused integration tests.

## Implementation decisions

- `AgentRunRole::CodeChangeEditor` and the exact
  `code_change_editor` execution projection are persisted before native launch.
  The editor attempt is reserved and bound transactionally before marker or
  launch-gate release.
- Built-in Codex and custom editors share bounded schema/output transport.
  Attempt 1 is fresh; only attempt 2 may resume the exact candidate-root-owned
  session. `ResumeLatest`, third launches, malformed output, and oversized
  output fail closed. Only digests, bounded summaries, statuses, and validated
  proposed checks are persisted.
- `CodeChangeCoordinator::advance_ready` owns the bounded editor state machine
  through worktree preparation and terminal editor persistence. Daemon polling
  retains the returned `AgentHandle` and cleanup owner, and editor budget
  reservations use finite per-attempt keys.
- Generic startup recovery preserves only an exact editor execution-kind,
  campaign/project, attempt/session, and active-status binding. Mismatches are
  identity contradictions; ordinary Standard/Decision/Diagnosis recovery is
  unchanged.
- Additional `cfg(test)` directory-mode hardening only addresses the roko
  `umask 0002` fixture environment; production path policy is unchanged.

No local Cargo/rustc run is claimed because the macOS host has the known
loader stall.
