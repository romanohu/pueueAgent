# Task 2 Report: Common CLI Formatter and Bounded Redaction

## Implementation

- Added `src/output.rs` as the shared output boundary.
  - `OutputMode::{Human, Json}` and `OutputTarget::{Terminal, Pipe}` select ANSI output only for human-mode TTY output when `NO_COLOR` is unset.
  - `render_id(kind, id)` centralizes human identifiers while preserving the existing `task 41` text contract.
  - `format_state(state)` preserves existing lower-case state text for non-TTY output and applies ANSI color only to recognized human-mode TTY states.
  - `redact_sensitive_text` removes values for `--token`, `--api-key`, `--password`, `--secret`, `Authorization: Bearer`, and credential-like `KEY=value` entries including `AWS_SECRET_ACCESS_KEY`.
  - `bounded_redacted_text` applies redaction before the existing 240-byte, UTF-8-safe output bound.
- Registered the module from `src/lib.rs`.
- Routed status task commands, Pueue error text, and failed termination reasons through bounded redaction.  The existing text output format remains unchanged when stdout is not a TTY.
- Routed diagnostics' shared bounded-text path through the common redactor. Existing JSON fields and schema version remain unchanged; prompt/transcript/payload projections remain omitted.
- Added integration coverage for every requested credential syntax and for no ANSI in JSON/pipe modes.

## Review Fixes

- Updated `steer list` human and JSON output to use `bounded_redacted_text` for the existing `message` field. The field remains present for schema compatibility, but its value is redacted, control/ANSI-free, and bounded to the common limit.
- Removed the private `escape_text` implementation from `src/main.rs`; control and ANSI handling now lives in the shared formatter.
- Extended the common redactor to cover separated and `=` forms of token flags such as `--access-token`, `AWS_ACCESS_KEY_ID`, `ACCESS_KEY`, and existing secret-key patterns.
- Added CSI/OSC ANSI stripping and control-character normalization before token redaction and truncation.
- Removed diagnostics-local truncation and token/path redaction helpers. Diagnostics now retain only their projection choices and call `bounded_redacted_text`; human diagnostic IDs also use `render_id`.
- Added real CLI contract tests for `steer list` pipe and JSON output, including ANSI input, credential values, and the common bound.
- Replaced whitespace-only redaction with a quote-aware lexer. Single-quoted, double-quoted, and unquoted values are consumed as one token for separated flags and `KEY=value` credentials, preventing suffix leakage.
- Applied the common bound to human `status` output for `halted_reason`, configured Codex session text, and joined context lineage. Human event and termination IDs now use the shared `render_id` formatter.
- Applied `bounded_redacted_text` to the human `root:` line without changing JSON fields or the stored path meaning. Path projection now runs before credential-like `KEY=value` matching, so a path segment such as `AWS_SECRET_ACCESS_KEY=...` is not exposed.
- Applied `bounded_redacted_text` to human `project:` and `group:` values, and to the successful `init` root message. JSON schema and stored/semantic project identity values remain unchanged.
- Applied `bounded_redacted_text` to compact human status' `pueue-agent: <project_id>` header while preserving that header format.
- Routed `guardrail_lines` configuration errors through a shared bounded/redacted projection. The integration contract test covers malformed configuration output; a unit test exercises a synthetic long credential-bearing error because the current config loader normalizes its real errors to fixed variants.
- Extended the redaction state machine for `KEY = value` and flag/marker `key = value` forms. The independent `=` token and its next lexer token, including quoted values, are consumed as one redaction unit; existing `KEY=value`, separated flag, Bearer, and quoted forms remain covered.
- Preserved quote metadata in the shared lexer so an `=` inside a quoted value is not treated as a new assignment boundary. An independent assignment now remains redacted through the next clear unquoted token boundary, or to end of input when no boundary exists; `Authorization = Basic secret` therefore does not expose `Basic` or its suffix.
- Extended structured `Authorization`/credential value handling beyond the Bearer-only branch. JSON-like keys with surrounding `{}`, quotes, or other punctuation now enter the shared assignment-boundary redaction state for non-Bearer schemes and multi-token values; key normalization removes structural punctuation before credential classification.
- Extended the sensitive `split_once('=')` path to start the shared assignment-redaction state after replacing the inline value. `Authorization=Bearer secret` and `AWS_SECRET=foo bar` now consume later tokens through the next clear argument boundary while preserving `--lr 0.001`; structured JSON/colon values also stop before following flags.
- Removed the unsafe generic `token.value.contains('=')` assignment boundary. Unquoted `abc=def` remains inside the active credential value; boundaries are now flags, independent `KEY =` sequences, or a subsequent inline assignment whose key is itself credential/flag-sensitive. This preserves existing adjacent `ACCESS_KEY=...` projections while preventing `abc=def` suffix leakage.
- Routed human CLI success messages for `pause`, `resume`, `disable` (reserved), and `disable --remove` (released) through `bounded_redacted_text` for project and group values. The existing prefixes and wording remain unchanged.
- Applied `bounded_redacted_text` to the complete joined `termination_errors` line after preserving the existing per-request projection. The request count/status line and individual request/error semantics remain unchanged.
- Applied `bounded_redacted_text` to human task states before `format_state`, preserving normal lower-case state labels and the existing ANSI/`NO_COLOR` policy while preventing long, control-bearing, or credential-like states from reaching output.
- Routed top-level `run` errors in `src/main.rs` through `bounded_redacted_text` before stderr emission. Existing concise error meaning remains readable; long init/config/path messages are bounded, control-free, and redacted.
- Split `Cli::try_parse()` error handling by Clap error kind: `DisplayHelp` and `DisplayVersion` retain `error.print()` and their complete output, while invalid parse errors use `bounded_redacted_text` on the rendered error and keep the existing exit-code mapping.

### `steer list --json` contract

The JSON output retains `schema_version`, `project_id`, `interventions`, and the existing intervention field names (`intervention_id`, `status`, `created_at`, `message`). The `message` field is intentionally a bounded/redacted default-output projection, not an unrestricted copy of stored text: the existing FIFO/project-scoping test verifies that an ordinary short safe message remains unchanged, while the control-character and sensitive/long-message tests verify projection to safe output. Stored intervention messages are unchanged.

The existing JSON schemas for `status`, `events`, `inspect`, `explain`, and `doctor` remain unchanged. Existing identity fields, including `project_id`, `group`, and `root_path` where present, retain their stored configuration values; the status JSON regression test explicitly checks `project_id`, `root_path`, and `pueue_group`. These identity fields are not free-text prompt/transcript, raw Pueue payload, or credential-bearing env/value fields, so they are intentionally not passed through the human-output redactor. Free-text JSON `message` remains the exception and uses the bounded/redacted projection required by Global Constraints.

The review RED cycle was verified with the new `--access-token`/AWS test failing before the implementation (`separated-token` was present in output), followed by GREEN focused tests after the shared formatter changes.

The final-review RED cycle was also verified before implementation: the quoted-value regression test exposed leaked suffixes (`second"`, `first second'`, and `second"`), the human-status regression test exposed an unbounded `halted:` line, and the shared-ID regression assertion exposed the old `event #...` form. All three became GREEN after the quote-aware lexer, common bounds, and `render_id` changes.

The additional root-path RED cycle was verified before implementation: the new human-status test failed on the unbounded `root:` line. After routing the line through the formatter, it exposed a credential-like key embedded in a path; moving path projection ahead of credential matching made the test GREEN while retaining the path field's meaning and leaving JSON unchanged.

The project/group/init RED cycle was verified before implementation: the status project/group test and init success-output test both failed on unbounded raw values. The common formatter changes made both GREEN.

The compact-status RED cycle was verified before implementation: `compact_status_bounds_and_redacts_project_id` failed on the unbounded `pueue-agent:` header. The guardrail helper test first produced a missing-helper compile RED; no raw guardrail leakage log is claimed for that phase because `config::load` currently maps malformed configuration to fixed `AppError` variants. The malformed-config status contract and synthetic long credential-bearing projection test are now GREEN.

The latest redaction RED log showed the spaced-assignment leak directly: `AWS_SECRET_ACCESS_KEY [REDACTED] first second`, `password [REDACTED] third fourth`, and equivalent flag forms. The state-machine fix now consumes the independent `=` and its quoted/unquoted value as one redaction unit while preserving existing `KEY=value`, separated-flag, Bearer, and quoted-value behavior.

The latest JSON identity check confirms that `status --json` still emits the original stored `project_id`, `root_path`, and `pueue_group`; these are configuration identity fields, not redaction targets.

The current review RED cycle was verified before implementation. The independent-assignment test failed with leaked `secret` after `CREDENTIAL =` and `Authorization =`; the quoted-equals test failed with `CREDENTIAL = secret=[REDACTED]`; the CLI success-output contract failed on an overlong raw line; and the joined termination-error contract failed because the combined line exceeded the common bound. All four regressions are GREEN after the lexer/state-machine, CLI projection, and whole-line formatter changes.

The structured-credential RED cycle was also verified before implementation: the new JSON-like header test failed with `curl -H {credential: [REDACTED] second}`, proving that the second token after a punctuation-wrapped credential key escaped. The state now consumes the remainder through the next clear boundary. The status JSON task-command projection test confirms that the same shared path does not expose the structured Bearer secret and still emits the existing `command_summary` value.

The JSON boundary remains deliberate: `status`, `events`, `inspect`, `explain`, and `doctor` keep their existing schemas and identity field meanings (`project_id`, `group`, `root_path`, and related stored configuration identity). These values are not free-text prompt/transcript, raw Pueue payload, or credential-bearing env/value data, so status JSON retains them. Free-text `steer list --json` messages remain bounded/redacted projections while preserving the existing field name and safe short message values.

The latest status/error RED cycle was verified before implementation: the human task-state test failed on an overlong raw task line containing `TASK_STATE_SECRET`, and the CLI init error test failed on an overlong raw stderr path containing `AWS_SECRET_ACCESS_KEY=INIT_SECRET`. Both are GREEN after routing through the shared formatter.

The latest Clap RED cycle was verified before implementation: `events --limit AWS_SECRET_ACCESS_KEY=CLI_PARSE_SECRET` exposed `CLI_PARSE_SECRET` in stderr, while the same invalid parse with a 1,000-character value exceeded the output bound. After the error-kind split, the invalid parse retains exit code 2 and is bounded/redacted; the existing help contract remains complete. The current CLI definition does not declare a `--version` flag, so no new version surface was introduced; the `DisplayVersion` branch remains preserved for Clap configurations that provide it.

The latest inline-assignment RED cycle was verified before implementation: the regression output contained `Authorization=[REDACTED] secret` and `AWS_SECRET=[REDACTED] bar`. After starting assignment redaction from the sensitive `KEY=value` branch and adding a structured-value boundary check, the inline, JSON, colon, and ordinary `--lr 0.001` assertions are GREEN.

The latest unquoted-equals RED cycle was verified before implementation: `AWS_SECRET_ACCESS_KEY = abc=def --lr 0.001` exposed `abc=def`. Removing the generic equals boundary initially made an existing adjacent `ACCESS_KEY=...` projection assertion fail; limiting inline boundaries to sensitive keys/flags restored that contract while keeping ordinary `abc=def` inside the redacted value. Unquoted, quoted, chained independent assignments, and normal arguments are now GREEN.

Final-review fix commit: `3afe5f5a46ee71c9cea2f2113f736d5ce92f0385` (`fix: close quoted cli redaction leaks`).

Additional root-path fix commit: `8f2991e7106eeaeb262550cf114d23e5ef2b7fa4` (`fix: sanitize human project root output`).

Additional project/group/init fix commit: `9ced6e5ad4e2aff50451358f366af856b642963e` (`fix: bound human project identity output`).

Fifth review-fix commit: `3208744aacd67ec1d82cabb927ed9bf3bb82a9b5` (`fix: bound compact status and guardrail errors`).

Files changed in this fifth commit: `src/status.rs`, `tests/integration/diagnostics.rs`. The Task 2 contract note was also added to `task-2-brief.md`.

Latest critical redaction fix commit: `ac121e6c64d6d1cd2cfbdf0d3745cc95cc1b9e8a` (`fix: redact spaced sensitive assignments`).

Files changed in this latest commit: `src/output.rs`, `tests/integration/diagnostics.rs`. The JSON identity/projection contract note remains in `task-2-brief.md`.

Current review-fix commit: `d2c9b5ac407bd16ca6b1f833f91d14c4af1b66f1` (`fix: harden cli redaction and status output`).

Files changed in the current review fix: `src/main.rs`, `src/output.rs`, `src/status.rs`, `tests/integration/diagnostics.rs`, `tests/integration/init.rs`, and `tests/integration/operator_commands.rs`.

Current structured-credential fix commit: `2e5a99766072423c986293c7a0e130cd83a50ef2` (`fix: redact structured credential headers`).

Files changed in this additional fix: `src/output.rs` and `tests/integration/diagnostics.rs`.

Current status/error hardening commit: `b2bc2b18f7ce2624a4b0b5e23f49d2af7cf57a86` (`fix: bound task states and cli errors`).

Files changed in this additional fix: `src/main.rs`, `src/status.rs`, `tests/integration/init.rs`, and `tests/integration/operator_commands.rs`.

Current Clap parse-error hardening commit: `d56480436d7c103cc85fb6b3c712a62532c16881` (`fix: sanitize clap parse errors`).

Files changed in this additional fix: `src/main.rs` and `tests/integration/cli_help.rs`.

Current inline-assignment redaction commit: `2667a42e349e775f6ba07a326ece917486595330` (`fix: redact inline sensitive assignments`).

Files changed in this additional fix: `src/output.rs` and `tests/integration/diagnostics.rs`.

Current unquoted-equals redaction commit: `a191ea5fe7dac0faefdc3aa072a19d67ec9a1bae` (`fix: keep equals inside redacted values`).

Files changed in this additional fix: `src/output.rs` and `tests/integration/diagnostics.rs`.

## TDD Evidence

- The initial formatter bootstrap did have a compile RED (`E0432`, `pueue_agent::output` missing); that is retained only as historical setup evidence, not as evidence of a redaction-output failure.
- Actual redaction/output RED evidence was observed in the review cycles: separated `--access-token` output contained `separated-token`; quoted-value output exposed suffixes such as `second"` and `first second'`; the root/project/group/init status tests exposed unbounded raw lines; and the compact project-header test exposed an unbounded `pueue-agent:` line.
- The latest status/error tests produced actual product RED logs before implementation: `status_human_bounds_and_redacts_task_state` failed its task-line bound, and `init_error_output_bounds_and_redacts_long_credential_like_paths` failed its stderr bound. No synthetic failure is claimed for these cases.
- The latest Clap parse-error regression produced an actual product RED log before implementation: `invalid_cli_parse_errors_redact_and_bound_values_while_help_stays_complete` failed because `CLI_PARSE_SECRET` remained in stderr. No synthetic failure is claimed.
- The latest inline-assignment regression produced an actual product RED log before implementation: `redact_sensitive_text_redacts_inline_assignments_until_argument_boundary` failed with leaked `secret` after `Authorization=[REDACTED]` and leaked `bar` after `AWS_SECRET=[REDACTED]`. No synthetic failure is claimed.
- The latest unquoted-equals regression produced an actual product RED log before implementation: `redact_sensitive_text_keeps_equals_inside_unquoted_assignment_values_redacted` failed because `abc=def` remained in the rendered output. A subsequent existing-contract failure was also observed and resolved by the sensitive-key/flag-only inline boundary restriction.
- For the current guardrail change, the helper test initially produced a missing-helper compile RED. No raw guardrail leakage log is claimed because `config::load` maps malformed configuration and read failures to fixed `AppError` variants. The integration malformed-config contract and synthetic `AppError::Message` long credential-bearing projection test cover the intended boundary without inventing a loader error.
- After the minimal common-formatter changes, the focused tests and full suite became GREEN.

## Verification

- Required toolchain PATH was used for every Cargo command: `/private/tmp/pueue-agent-rustup/toolchains/stable-aarch64-apple-darwin/bin:/usr/bin:/bin`.
- `cargo test --test diagnostics redact_sensitive_text`: passed (8 tests).
- `cargo test --test diagnostics status_json_task_command_projection_redacts_structured_authorization_headers`: passed (1 test).
- `cargo test --test diagnostics`: passed (26 tests).
- `cargo test --test init`: passed (7 tests).
- `cargo test --test operator_commands`: passed (13 tests).
- `cargo test --test cli_help`: passed (10 tests).
- `cargo test --all-targets`: passed (250 tests; 0 failures).
- `git diff --check`: passed.
- `cargo fmt --check`: exit 1 only because of the pre-existing Task 1 formatting difference at `tests/integration/database.rs:281`. That unrelated file was left untouched; all Task 2 files were individually rustfmt-formatted.

## Compatibility and Concerns

- No JSON schema fields, schema versions, or raw-Pueue/prompt/transcript exclusion rules were changed.
- ANSI is intentionally absent from JSON and non-TTY output. Human TTY output can use color unless `NO_COLOR` is set.
- A direct `NO_COLOR` environment mutation test was not added because the environment is process-wide; the existing JSON/pipe ANSI contract tests remain in place.
- Redaction is intentionally bounded and token-oriented: it protects the required CLI flags, Bearer credentials, and credential-like environment assignments without attempting to parse arbitrary shell syntax. Commands that can contain secrets in unsupported bespoke encodings should not be emitted as diagnostics.
- Independent assignment redaction treats the next unquoted flag, unquoted assignment, or `key =` sequence as a boundary. If no such boundary exists, the remainder of the input is redacted; this conservative rule can hide adjacent free text after a credential-like assignment, but avoids suffix leakage.
