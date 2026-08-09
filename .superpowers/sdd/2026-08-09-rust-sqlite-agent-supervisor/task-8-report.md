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
