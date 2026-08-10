# Human Intervention Final Review Remediation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the valid whole-branch review findings for the human-intervention and observability implementation at `c7b3ac1` without weakening existing safety behavior.

**Architecture:** Keep project predicates and durable SQLite transitions at repository boundaries. Recovery runs on every scheduler tick, but only unattached expired intervention reservations are requeued there; startup run recovery remains responsible for attached reservations. Unix is the supported agent-launch platform: execution is behind a fixed-argv stdin release gate whose byte is written only after the PID/running/intervention transaction commits. Non-Unix launchers fail explicitly because this guarantee is unavailable.

**Tech Stack:** Rust 2021, Tokio, rusqlite bundled SQLite, Clap, serde/serde_json, POSIX `sh` on Unix, Bats, ShellCheck.

## Global Constraints

- Use TDD for every behavior: add a focused failing test, run and record the expected RED result, implement the smallest fix, then run and record GREEN.
- Never mechanically requeue attached/live intervention reservations.
- Never interpolate configured agent programs or arguments into shell source; pass them as positional arguments.
- Diagnostics are project-scoped, read-only where specified, deterministic, and bounded.
- Preserve stored and JSON steer messages; escape control characters only in text list output.
- Do not edit `.superpowers/sdd` ledgers.

## Tasks

### Task 1: Tick recovery

**Files:** `src/scheduler.rs`, `src/db/repositories.rs`, `tests/integration/scheduler.rs`, `tests/integration/database.rs`.

- [ ] Add a later-tick integration test and repository predicate test covering an expired reservation with `agent_run_id IS NULL`, plus attached/live and attached/failed controls; run the focused test and capture RED.
- [ ] Add a scheduler-tick recovery call and a repository method/query that limits periodic recovery to unattached expired reservations; run focused scheduler/database tests and capture GREEN.

### Task 2: Durable Unix launch gate

**Files:** `src/agent.rs`, `tests/integration/scheduler.rs`, unit tests as needed.

- [ ] Add deterministic gate tests proving EOF exits without invoking the configured agent and a release byte permits fixed argv; run RED.
- [ ] Implement the POSIX gate with piped stdin, keep configured command/args as positional argv, write the release byte only after `mark_running_and_apply_interventions` commits, and reject non-Unix launchers explicitly; test spawn failure and process-tree cleanup; run GREEN.

### Task 3: Functional diagnostics

**Files:** `src/main.rs`, `src/diagnostics.rs`, `src/cli.rs`, repositories/models as required, `tests/integration/diagnostics.rs`, `tests/integration/cli_help.rs`.

- [ ] Add behavioral CLI/integration tests for events, inspect, explain, and doctor, including project isolation, unknown IDs, limits, JSON/text output, read-only doctor checks, typed warning/error statuses, and bounded output; run RED.
- [ ] Implement the handlers from existing repositories and service/Pueue abstractions; run GREEN and preserve existing status behavior.

### Task 4: Performance and output hardening

**Files:** `src/db/migrations.rs`, `src/diagnostics.rs`, `src/main.rs`, `src/interventions.rs`, tests.

- [ ] Add repeated-open/index SQL regression, oversized/control Pueue fixtures, and text steer escaping tests; run RED.
- [ ] Make migration repair conditional, normalize/bound Pueue values, escape text steer controls, and remove the redundant sort closure; run GREEN.

### Task 5: Verification and handoff

- [ ] Run focused RED/GREEN records and all required Rust, clippy, Bats, ShellCheck, format, and diff checks.
- [ ] Write `.superpowers/sdd/2026-08-10-human-interventions/final-fix-report.md` without changing any ledger.
- [ ] Review the complete diff and commit all remediation changes with `fix: close whole-branch review findings`.
