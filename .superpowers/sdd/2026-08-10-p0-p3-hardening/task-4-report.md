# Task 4 report: submit CLI, metadata validation, and agent origin

## Implementation commit

- `78748ec232bf592d61b7102c632c43f7a809e7b4 feat: add typed submission metadata`

## Changed files

- `src/agent.rs`
- `src/cli.rs`
- `src/main.rs`
- `src/models.rs`
- `src/submit.rs`
- `tests/integration/cli_help.rs`
- `tests/integration/pueue_adapter.rs`
- `tests/integration/scheduler.rs`

## Behavior delivered

- `submit --kind` defaults to `experiment` and accepts explicit `control`.
- `--metadata PATH` and `--metadata-json JSON` are exclusive. Metadata is limited to a JSON object of at most 16 KiB, depth 8, 32 keys/object, 64-byte keys, 1024-byte strings, and 64 array items.
- Validation runs before the submission intent or `pueue add`; metadata is stored only in SQLite and never forwarded in the Pueue argument vector.
- Submit output now has a bounded human projection (`submission`, `task`, `kind`, `group`, `state`) and an equivalent JSON projection. Raw metadata is omitted.
- Agent children receive `PUEUE_AGENT_RUN_ID` and `PUEUE_AGENT_PROJECT_ID`. Submit accepts origin only when both values are valid, project-matching, and identify the active project run; otherwise it fails closed. Normal submits have no origin.

## TDD evidence

- RED: `cargo test --all-targets submit_` failed because `SubmitOptions`, metadata loading, and options-aware submit were absent.
- RED: the origin environment test failed because its parsing helper was absent.
- RED: the output projection test failed because `render_submission` was absent.
- GREEN: focused submit, metadata, origin, output, help, and agent-environment tests passed after the minimum implementation.

## Verification

- `PATH=/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin cargo test --all-targets` — passed.
- `git diff --check` — passed.
- `cargo fmt --check` — reports only the pre-existing Task 1 formatting change at `tests/integration/database.rs:282`. That file was not modified by Task 4; all Task 4 Rust files were formatted with the supplied stable toolchain.

## Remaining concerns

- The repository-wide formatting check remains red until the pre-existing `tests/integration/database.rs:282` formatting issue is addressed in its owning task.

## Review follow-up

### Follow-up commit

- `5ad8b0598730395789972bd07af7a3d3296f3b66 fix: harden submission metadata boundary`

### Additional changed files

- `src/submit.rs`
- `tests/integration/pueue_adapter.rs`
- `tests/e2e/rust_supervisor.sh`

### Corrections delivered

- The Rust supervisor E2E now extracts the numeric `task=<id>` field from the five-field human submit summary before its three Pueue task-state call sites.
- File metadata uses `File::open` and a bounded `Read::take(16 KiB + 1)` read. Inline input is bounded before parsing.
- One shared byte-limit rule now validates both loader inputs and a public `SubmitOptions` value after JSON serialization, before submission insertion or `Pueue.add`.
- The integration test proves a structurally valid but serialized-oversize object is rejected with no stored submission and no `Pueue.add` invocation.

### Review TDD evidence

- RED: `run_with_options_rejects_oversized_metadata_before_pueue_add` accepted a 32-key × 1024-byte metadata object and invoked the fake Pueue adapter.
- GREEN: the same focused test passed after serialized-byte validation was shared by loader and public submit boundary.
- The metadata loader focused test passed for exclusive source selection, non-object values, all structural bounds, oversized inline input, and an oversized file.

### Follow-up verification

- `cargo test --test pueue_adapter run_with_options_rejects_oversized_metadata_before_pueue_add` — 1 passed.
- `cargo test --test pueue_adapter metadata_loader_rejects_conflicting_or_out_of_bounds_values` — 1 passed.
- `cargo test --all-targets` — 267 passed, 0 failed (current HEAD).
- `shellcheck --shell=bash tests/e2e/rust_supervisor.sh` — passed.
- Rust supervisor E2E was not rerun in the current environment because `pueue`/`pueued` are unavailable. The summary task-ID extraction implementation was inspected, and ShellCheck passed; this report does not assert a Rust supervisor PASS.
- `git diff --check` — passed.
- `cargo fmt --check` — still reports only the unchanged Task 1 line in `tests/integration/database.rs:282`.
