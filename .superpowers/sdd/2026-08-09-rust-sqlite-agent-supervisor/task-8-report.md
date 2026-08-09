# Task 8 Report: Guardrails, leased scheduler, and agent lifecycle

## Summary

Implemented Task 8 only:

- Added `src/scheduler.rs` with `Scheduler::tick`, lease recovery, per-project coalescing, priority dispatch, bounded prompts, and active-agent exclusion.
- Added `src/guardrails.rs` with `Guardrails::check` and `Allow` / `Pause` / `Halt` decisions for max agent runs, max experiments, and max consecutive failures.
- Added `src/agent.rs` with direct `tokio::process::Command` argv execution, no shell wrapper, PID/log/timeout handle tracking, and Codex fresh/resume/resume-latest argv construction.
- Added `[agent.context]` config support with default `fresh`, explicit `resume { session_id }`, and opt-in `resume_latest`.
- Added SQLite schema v3 columns for agent-run context mode, session id, and lineage JSON. Existing v2 rows migrate to `fresh` with empty lineage.
- Added scheduler/config integration tests covering priority coalescing, active-agent exclusion, lease recovery, retry wait, guardrail halt/pause cases, Codex resume argv, bounded prompt references, and SQLite context persistence.

## Commands and results

Baseline / red / verification commands were attempted, but this environment does not have a Rust toolchain on PATH:

| Command | Result |
|---|---|
| `cargo test --all-targets` | Failed before running: `zsh:1: command not found: cargo` |
| `cargo test --test scheduler` | Failed before running: `zsh:1: command not found: cargo` |
| `cargo test --test scheduler` with escalated sandbox | Failed before running: `zsh:1: command not found: cargo` |
| `cargo fmt --check` | Failed before running: `zsh:1: command not found: cargo` |
| `cargo clippy --all-targets -- -D warnings` | Failed before running: `zsh:1: command not found: cargo` |
| `git diff --check` | Passed, exit 0 |
| `rg -n "bash -c\|sh -c\|std::process::Command\|Command::new\\(\".*sh\|rm -rf\|kill -" src tests/integration/scheduler.rs -g '!target/**'` | No matches |

## TDD notes

- Added Task 8 scheduler/config tests first.
- Attempted the required red run with `cargo test --test scheduler`; the command could not start because `cargo` is not installed or not on PATH.
- Implementation proceeded after recording that blocker. The Rust test suite still needs to be run in an environment with Cargo.

## Risks / follow-up

- Not compiler-verified in this environment due to missing `cargo`, `rustc`, `rustfmt`, and `rust-analyzer`.
- `resume_latest` records mode and event lineage, but cannot know a concrete Codex session id until Codex reports one; no transcript is copied into SQLite.
- Agent timeout cleanup uses `tokio::process::Child::kill()` for the agent process. No raw OS signal path was added for Pueue task termination.

## Fix round 1

### Summary

- Added Unix agent process session setup with best-effort `setsid()` before spawning the agent process.
- Changed agent timeout cleanup to terminate the agent process group with TERM, then KILL, then fall back to `Child::kill()` where process groups are unavailable or cleanup fails. Pueue task termination remains through the Pueue adapter only.
- Changed scheduler config-load failures after claim into visible failed event transitions with the configuration error preserved in `events.last_error`; explicit resume configuration still fails and does not fall back to fresh.
- Ensured SQLite migrations idempotently create `agent_runs_one_active_per_project_idx` on v1->v3, v2->v3, v0->v3, and already-v3 opens.
- Added regression coverage for invalid resume config claims, full agent descendant cleanup on timeout, and legacy migration active-agent uniqueness.

### Red commands

All commands used:

`PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin`

| Command | Result |
|---|---|
| `cargo test --test scheduler invalid_resume_config_does_not_leave_claimed_event_stranded -- --nocapture` | RED: failed with event left `Claimed` instead of `Failed` |
| `cargo test --test scheduler agent_timeout_terminates_descendant_agent_processes -- --nocapture` | RED: failed because descendant `sleep` process remained after timeout |
| `cargo test --test database legacy_migrations_create_active_agent_unique_index -- --nocapture` | RED: failed because legacy migration left index count `0` |

### Verification commands

All commands used:

`PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin`

| Command | Result |
|---|---|
| `cargo test --test scheduler invalid_resume_config_does_not_leave_claimed_event_stranded -- --nocapture` | Passed: 1 passed |
| `cargo test --test scheduler agent_timeout_terminates_descendant_agent_processes -- --nocapture` | Passed: 1 passed |
| `cargo test --test database legacy_migrations_create_active_agent_unique_index -- --nocapture` | Passed: 1 passed |
| `cargo test --test scheduler` | Passed: 11 passed |
| `cargo test --test config` | Passed: 26 passed |
| `cargo test --test database` | Passed: 18 passed |
| `cargo fmt --check` | Initially failed; after `cargo fmt`, passed |
| `cargo test --offline --all-targets --all-features` | Passed: 98 passed across all integration/unit targets |
| `cargo clippy --offline --all-targets --all-features -- -D warnings` | Passed |
| `git diff --check` | Passed |
| `rg -n "kill\\(\|SIGTERM\|SIGKILL\|setsid\|pre_exec\|process_group\|Command::new\\(\\\".*sh\|sh -c\|bash -c" src tests/integration -g '!target/**'` | Only agent process cleanup/test helpers and existing Pueue adapter abstractions matched; no raw Pueue task signal path was added |

### Risks / notes

- Unix process-group cleanup is best-effort: `setsid()` is attempted in `pre_exec`; if unavailable or cleanup signaling fails, timeout cleanup falls back to killing the direct child.
- Non-Unix platforms retain the direct-child kill fallback.
- The process-tree regression is Unix-only because it verifies POSIX process-group behavior.
