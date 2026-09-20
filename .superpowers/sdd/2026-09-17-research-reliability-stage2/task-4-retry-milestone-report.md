# Task 4 retry milestone

This test-only milestone extends `research_coordinator_retries_bound_malformed_output_at_the_wake_boundary` through all three bounded native attempts. It verifies that each attempt has a distinct consumed campaign reservation, that the next binding preserves every earlier run ID, failure code, context JSON, and context digest in `retry_history`, and that the fourth wake blocks without launching or consuming a fourth reservation.

The test source compiles with `cargo check --test research_agent` on the host. The integration crate is Linux-only, so the host run executes zero tests; native execution remains a Linux gate owned by the parent.

Prior evidence carried into this checkpoint:

- Linux RED at `cad07db2c26262cfe93b4dbd93585beeb56e6701`: `research_scheduler` ran 11 tests, with 6 passing and 5 intended failures.
- Product milestone `351b943` passed `cargo check --locked --offline --all-targets`.
- Host `cargo test --test research_scheduler -- --nocapture` passed 10 tests; the remaining intentional failure is the deferred I3 dead-owner recovery case.

Pending verification is the new Linux retry-cap/history test and any product adjustment it exposes at the fourth wake boundary.
