# Task 4 report — due research scheduler, bounded retries, daemon recovery

## Result

Implemented 4.1–4.5 in commit `d1794b4` (`feat: schedule and recover bounded research reviews`).

- Durable scheduling reads reconciled running observations, uses `started_at` with `first_observed_at` fallback, honors the 0–1440 minute interval, and claims one authoritative running experiment at the due boundary.
- Dedicated `run_due_research` owns CampaignResearch events, project/run-ID admission, campaign budget reservations keyed as `research:{review_id}:attempt:{attempt}`, native binding, retry wake times, bounded attempts, and report/cleanup ownership.
- Native policy/session failures block; malformed output/timeout/terminal child failures use bounded redacted retry codes and no learning termination.
- Generic scheduler batching/prompt dispatch remains excluded for CampaignResearch.
- Generic startup recovery validates research lineage separately from code-change editors, preserves valid live research runs, blocks corrupt bindings safely, and `recover_research` reconciles pending claims, terminal child death, response-persisted rows, and bounded retry wakeups.
- Daemon invokes research recovery after generic native ownership reconciliation and runs due research after health/termination and retained-owner polling.

## RED / GREEN evidence

Required test-only RED checkpoint:

```text
commit 91f3146 test: add research scheduler RED contracts
cargo test --test research_scheduler
error[E0432]: unresolved import `pueue_agent::db::next_research_due`
```

GREEN checks:

```text
cargo check --locked --offline --lib
Finished `dev` profile

cargo test --test research_scheduler --locked --offline
4 passed

cargo test --test database research --locked --offline --no-fail-fast
34 passed

cargo test --test research_agent --locked --offline
0 passed (Linux-only integration target on this macOS host)

cargo test --lib scheduler::tests::campaign_research_is_not_a_legacy_agent_dispatch_mode --locked --offline
1 passed

cargo test --test daemon daemon_run_once_invokes_reconciliation_detection_termination_and_scheduler --locked --offline
1 passed
```

## Files and review notes

Changed `src/research.rs`, `src/daemon.rs`, `src/db/research.rs`, `src/db/repositories.rs`, `src/db/mod.rs`, `src/lib.rs`, `src/scheduler.rs`, `src/agent.rs`, `Cargo.toml`, and `tests/integration/research_scheduler.rs`.

`src/agent.rs` adds only the context-selection seam needed by the coordinator: owned sessions resume exactly; a securely missing session remains eligible for the existing fresh reconstruction path. Existing Task3 native APIs and tests were preserved.

Self-review: SQLite remains authoritative; no generic intervention token is passed; reservation IDs are consumed and never refunded; pending session nonce and confirmed session ID remain distinct; active/unknown PID, gate, and cleanup ownership is never relaunched. No merge, push, or main-worktree changes were made.

## Concerns / controller follow-up

- Native research integration is Linux-only and therefore produced zero tests on this macOS host. The controller must run the committed RED checkpoint and the GREEN `research_agent`/scheduler launch and restart coverage on isolated Linux state before accepting native behavior.
- The repository’s broad lib/integration suites have pre-existing environment-sensitive failures (native executable anchors, fixture permissions, and migration fixtures); focused Task4/database checks above pass. `cargo fmt --check` also reports pre-existing whole-repository formatting drift.

