# Stage 3 Setup/Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` or `superpowers:executing-plans` task by task. Every product task uses RED → GREEN → focused regression → independent review → isolated commit.

**Goal:** campaign作成前にobjectiveとinstalled Codexをdoctorで検証し、real Pueue上の短いnon-campaign trialで実argv/environment/result manifestを安全に確認できるようにする。

**Architecture:** doctorは既存validatorとbounded Codex probeを読み取り専用projectionとして追加する。trialはread-only DB、project admission lock、identityを保持するprivate output、未登録nonce Pueue group、exact task state machineを組み合わせ、campaign lineageやbudgetを一切作らずに1 taskだけを実行・回収する。

**Tech Stack:** Rust 2021、clap、tokio、serde/serde_json、uuid、既存Pueue process adapter、既存descriptor-relative filesystem primitive。新しいcrate、config、SQLite migrationは追加しない。

**Spec:** [Stage 3 setup/readiness設計](../specs/2026-09-20-research-reliability-stage3-readiness-design.md)

## Global constraints and workflow

- 実装開始条件はStage 2の全受入・Linux gate完了。現在のStage 2 worktreeへStage 3 product変更を混ぜない。
- 各実装担当はLuna-max（`gpt-5.6-luna`, reasoning effort `max`）。同じ差分を書いていない別agentが仕様reviewとcode reviewを行う。
- root/controllerだけがcommitを作る。共有index、main、remoteをworkerは操作しない。
- implementation worktreeは受理済みStage 2 HEADから分離し、各taskを小さいcommitにする。
- routine decisionsは本planの値を使う。command名`trial`、実行timeout既定60秒/最大300秒、cleanup 30秒、poll 250 ms、output非保持について追加のユーザーapproval待ちは作らない。
- doctorは読み取り専用。trialは明示CLI invocationの1回だけを実行し、自動submit/campaign開始をしない。
- campaign DB rows、budget、registered project group、既存Pueue taskを変更しない。
- Linux E2Eは専用state/config/daemon/disposable projectだけを使う。利用者のlive project、既存service、既存experimentでtrialを実行しない。
- 全体format、無関係refactor、互換layer、汎用runner、設定追加を行わない。

## Task map

| Task | Files | Checkpoint |
| --- | --- | --- |
| 1 | `src/diagnostics.rs`, `tests/integration/diagnostics.rs` | no-campaign objective readiness |
| 2 | `src/codex_command.rs`, `src/diagnostics.rs`, `src/main.rs`, `tests/integration/codex_security.rs`, `tests/integration/diagnostics.rs` | installed Codex doctor |
| 3 | `src/environment.rs`, `src/result_manifest.rs`, unit tests | private output + shared runtime/classifier |
| 4 | `src/pueue.rs`, `src/trial.rs`, `src/lib.rs`, unit tests, `tests/integration/pueue_adapter.rs` | nonce group + task lifecycle |
| 5 | `src/cli.rs`, `src/main.rs`, `tests/integration/cli_help.rs`, `tests/integration/operator_commands.rs` | CLI/report/DB boundary |
| 6 | `docs/getting-started-ja.md`, `docs/commands-ja.md`, `docs/troubleshooting-ja.md`, `tests/e2e/`, verification report | isolated real-Pueue acceptance |

Tasks 1 and 2 form Stage 3A. Task 3 precedes 4, and 4 precedes 5. Task 6 runs only after all focused reviews are clean. No new Cargo test target is needed: unit tests stay with their modules and CLI integration extends existing targets.

## Task 1: Objective readiness in doctor

**Files:** Modify `src/diagnostics.rs`; test `tests/integration/diagnostics.rs`.

- [ ] **1.1 RED: add no-campaign objective tests.** Extend `DiagnosticsHarness` cases so `state.objective` is error for generated placeholder, missing, >16 KiB, invalid UTF-8, control characters, and heading/table-only content; valid meaningful content is ok. Assert summaries contain no source bytes.
- [ ] **1.2 RED: protect active behavior.** With one live campaign, assert there is no `state.objective` check and existing `campaign.objective_digest` remains ok for a match and warning for changed/unreadable content.
- [ ] **1.3 Run focused RED.**

```bash
cargo test --locked --offline --test diagnostics doctor_reports_objective_readiness_without_live_campaign -- --exact
cargo test --locked --offline --test diagnostics doctor_keeps_active_campaign_objective_digest_semantics -- --exact
```

Expected RED is missing `state.objective`; existing unrelated failures do not count.

- [ ] **1.4 Implement one check.** In `build_doctor_report_with_policy_and_roots`, use the already computed live-campaign projection. When count is zero, push one check from `state::load_objective`. Use stable bounded summary/remediation and never include objective/error text.
- [ ] **1.5 GREEN and read-only regression.** Run the two tests plus all diagnostics. Add a test that snapshots relevant table counts and objective bytes/metadata before/after report construction.

```bash
cargo test --locked --offline --test diagnostics doctor_
cargo test --locked --offline --test diagnostics
git diff --check
```

- [ ] **1.6 Independent review.** Confirm no active-campaign severity change, no duplicate parser, no writes. Root commits only Task 1 files.

## Task 2: Installed Codex capability in doctor

**Files:** Modify `src/codex_command.rs`, `src/diagnostics.rs`, `src/main.rs`; test `tests/integration/codex_security.rs`, `tests/integration/diagnostics.rs`.

### Interface

Add to diagnostics:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoctorAgentRuntime {
    Supported,
    Blocked { code: PolicyViolationCode },
    SkippedPolicyUnavailable,
}

pub async fn probe_doctor_agent_runtime(
    policy: &Result<ResolvedExecutionPolicy, PolicyViolation>,
) -> DoctorAgentRuntime;
```

Add `agent_runtime: DoctorAgentRuntime` to `DoctorExternal`. Keep report builders synchronous.

- [ ] **2.1 RED: parser/probe contract.** Add exact `--json` lookalike, each missing required flag, old/minimum version, nonzero, timeout, oversize, and replaced pinned anchor cases. Verify probe output is typed and no child process group survives.
- [ ] **2.2 RED: doctor projection.** Test Supported=`ok`, Blocked=`error` with only policy code, and policy unavailable=`warning` without executing a fake ambient Codex. Include a custom standard-agent policy whose global Codex anchor is supported.
- [ ] **2.3 Run RED.**

```bash
cargo test --locked --offline --test codex_security capability
cargo test --locked --offline --test diagnostics doctor_reports_installed_codex_runtime
```

- [ ] **2.4 Implement async projection.** Reuse `probe_installed_codex_capabilities(&policy.codex_anchor)`. Convert successful decision-only capability without `supports_research_policy()` to `Blocked { UnsafeCodexArgument }`. Do not make raw probe output public.
- [ ] **2.5 Wire command.** In `commands::doctor`, await the projection after policy load and before building `DoctorExternal`. Add `execution.agent_runtime` immediately after `execution.anchors`. Update existing `DoctorExternal` fixtures explicitly.
- [ ] **2.6 GREEN/regression.**

```bash
cargo test --locked --offline --test codex_security
cargo test --locked --offline --test diagnostics
cargo check --locked --offline --all-targets
git diff --check
```

- [ ] **2.7 Independent review.** Check global anchor selection, exact research capability, bounded output, non-Linux fail-closed behavior, and launch-time probe retention. Root commits Task 2 separately.

## Task 3: Identity-bound private trial output and shared manifest parser

**Files:** Modify `src/environment.rs`, `src/result_manifest.rs`; module unit tests only.

### Interfaces

Add `PrivateTrialOutput` with retained project/service/trials/generation/result/artifact descriptors and creation identities. Add `experiment_runtime_argv_with_outputs` and make existing `campaign_experiment_runtime_argv` delegate without changing its output. Expose only a crate-private pure manifest classification.

- [ ] **3.1 RED: output creation/read/cleanup.** Tests cover owner-only generation mode, canonical UUID generation, exact env paths, initially absent result/artifact paths, child-created result, same-directory atomic result rename, 16 KiB boundary, and explicit cleanup removing only the created generation.
- [ ] **3.2 RED: substitution safety.** Replace generation, trials parent, service parent, and project root one at a time. Cover result symlink, directory/special file, hardlink, wrong owner/mode, mount mismatch where Linux fixture supports it, non-UTF8 project root, depth/entry/byte limits. Every mismatch must reject read/cleanup and preserve replacement objects.
- [ ] **3.3 RED: shared argv/classifier parity.** Assert existing campaign runtime argv bytes remain identical. Feed the same valid/invalid bytes through campaign row conversion and trial projection: oversize, malformed JSON, wrong schema/ID, empty/nonempty metrics, and valid multi-metric values.
- [ ] **3.4 Run RED.**

```bash
cargo test --locked --offline --lib environment::tests::private_trial
cargo test --locked --offline --lib result_manifest::tests
```

- [ ] **3.5 Implement by reuse.** Use existing no-follow openat, mount identity, directory identity, bounded entry walk, and cleanup helpers. Do not generalize `PrivateRunTemp` public API or alter recovery identity. Precreate only the private generation; after terminal, open child-created `result.json` relative to its retained descriptor and validate the final file.
- [ ] **3.6 GREEN/regression.**

```bash
cargo test --locked --offline --lib environment::tests
cargo test --locked --offline --lib result_manifest::tests
cargo test --locked --offline --test reconciliation result_
cargo check --locked --offline --all-targets
git diff --check
```

- [ ] **3.7 Independent security review.** Review root/parent/generation/file substitution, arbitrary root bytes, descriptor read, bounded cleanup, and unchanged campaign argv. Root commits only Task 3.

## Task 4: Ephemeral Pueue group and bounded trial coordinator

**Files:** Modify `src/pueue.rs`, `src/lib.rs`; create `src/trial.rs`; test module unit tests and `tests/integration/pueue_adapter.rs`.

### Pueue boundary

Add default-deny methods to `PueueApi` so unrelated fakes need no behavior change; implement them in `CommandPueue` and trial fakes:

```rust
async fn group_exists(&self, group: &str) -> Result<bool, AppError>;
async fn create_group_exclusive(&self, group: &str) -> Result<(), AppError>;
async fn remove_group(&self, group: &str) -> Result<(), AppError>;
```

The default methods return a bounded unsupported configuration error. `CommandPueue` validates group names, uses existing `group -j`, and calls direct `group add` without race-adopting an existing group. Ownership requires pre-list absence, a successful add response, and post-list presence. It calls direct `group remove` only when the coordinator has established ownership and emptiness.

### Coordinator boundary

```rust
pub struct TrialOptions {
    pub argv: Vec<String>,
    pub objective_metric: Option<ObjectiveMetric>,
    pub timeout: Duration,
}

pub struct TrialReport { /* bounded serializable fields from the spec */ }

pub async fn run_with<P: PueueApi + ?Sized>(
    db: &Db,
    project: &Project,
    policy: Arc<ResolvedExecutionPolicy>,
    pueue: &P,
    options: &TrialOptions,
    cancellation: CancellationToken,
) -> Result<TrialReport, AppError>;
```

- [ ] **4.1 RED: group adapter.** Test invalid name rejection; absent→exclusive create→present; pre-existing group rejected without add; add error never becomes adopted success; empty owned group removal; remove error; post-remove absence. Assert exact argv and no shell.
- [ ] **4.2 RED: preflight zero mutation.** With a scripted fake, invalid objective/command, disabled registration, root replacement, live campaign, and lock contention make zero output dirs, group calls, and task add calls.
- [ ] **4.3 RED: success lifecycle.** Script queued→running→terminal success, write through the declared result path, then assert exact task ID/group/canonical command on every snapshot, existing `result_is_failure` semantics, valid manifest, one remove, task absence, group re-list empty, group removal, group absence, output cleanup, and successful report.
- [ ] **4.4 RED: failure/timeout cleanup.** Cover command failure, invalid/missing/empty manifest, selected metric missing, queued timeout (remove), running timeout (kill→terminal/absent→remove), kill/remove transient error with status confirmation, identity mismatch, status error, group cleanup error, output substitution, and cleanup limit failure.
- [ ] **4.5 RED: group removal safety.** Put an extra task in the owned nonce group after the trial task becomes terminal. Confirm the coordinator neither calls `group remove` nor moves that task to default. Only exact trial removal followed by a fresh empty-group snapshot permits removal.
- [ ] **4.6 RED: ambiguous add.** Simulate add returning error before enqueue, after exact unique enqueue, duplicate identity, and status unavailable. Recover/clean exactly one matching task; never kill/remove a mismatched task; retain output/group and report uncertainty when ownership cannot be proved.
- [ ] **4.7 RED: cancellation.** Inject a cancellation future rather than process-global test signal. Confirm the same running cleanup path and bounded deadline. The CLI wiring in Task 5 supplies `tokio::signal::ctrl_c`.
- [ ] **4.8 Implement finite state machine.** Use one `tokio::time::Instant` execution deadline and one 30-second cleanup deadline. Wrap each awaited Pueue operation by remaining time; do not reset on each poll. Poll interval is 250 ms.
- [ ] **4.9 GREEN/regression.**

```bash
cargo test --locked --offline --lib trial::tests
cargo test --locked --offline --test pueue_adapter group_
cargo test --locked --offline --test pueue_adapter
cargo check --locked --offline --all-targets
git diff --check
```

- [ ] **4.10 Independent review.** Check proof that this invocation created the group, no existing group adoption/deletion, no group removal with tasks, add ambiguity, exact task matching, kill confirmation, output cleanup ordering, absolute bounds, and no repository writes. Root commits Task 4.

## Task 5: CLI, rendering, and DB boundary

**Files:** Modify `src/cli.rs`, `src/main.rs`; test `tests/integration/cli_help.rs`, `tests/integration/operator_commands.rs`.

- [ ] **5.1 RED: clap contract.** Parse `trial -- command`, metric flags, JSON, 1/60/300 seconds. Reject 0/301, incomplete metric flags, invalid delta, empty command, and non-UTF8 argv at the command boundary.
- [ ] **5.2 RED: command integration.** Extend the existing disposable binary/Pueue fixture. The child asserts its working directory, reads fixture input data, checks all four environment names, confirms result/artifact paths belong to the unique private trial root, writes a valid exact-ID manifest, and exits 0.
- [ ] **5.3 RED: isolation.** Before/after each CLI run, compare counts and contents for campaigns, proposals, experiments, submissions, reservations, agent runs, research reviews, Events, and task observations. Permit only the known unknown-group integration event when the configured callback is exercised. Assert no ordinary agent child launch and no task in the registered project group.
- [ ] **5.4 RED: bounded reports.** Human and JSON success/failure contain stable state, task/group/output cleanup and selected metric only. Seed command/environment/manifest with a secret sentinel and assert it never appears. JSON failure still exits 1 after confirmed cleanup.
- [ ] **5.5 Implement CLI.** Add `Command::Trial(TrialArgs)`, `commands::trial`, a shared metric-flag conversion helper used by submit and trial, 1..=300 parser, and renderer. Resolve current project read-only; bridge `tokio::signal::ctrl_c` to the coordinator's `CancellationToken` and stop that monitor after return. Do not add project-root/Pueue-config override or retention flags.
- [ ] **5.6 GREEN/regression.**

```bash
cargo test --locked --offline --test cli_help trial
cargo test --locked --offline --test operator_commands trial
cargo test --locked --offline --test cli_help
cargo test --locked --offline --test operator_commands
cargo check --locked --offline --all-targets
git diff --check
```

- [ ] **5.7 Independent review.** Confirm explicit invocation only, no user confirmation flow, no campaign start, no DB writer, stable output bounds, Ctrl-C cleanup, and no sensitive output. Root commits Task 5.

## Task 6: Documentation and isolated Linux acceptance

**Files:** Modify `docs/getting-started-ja.md`, `docs/commands-ja.md`, `docs/troubleshooting-ja.md`; add the smallest trial fixture under existing `tests/e2e/`; create `docs/report/2026-09-20-research-reliability-stage3-verification.md`.

- [ ] **6.1 Documentation.** Explain `doctor` before first submit, `state.objective`, `execution.agent_runtime`, exact trial syntax/default/max, manifest schema, dedicated group, automatic cleanup, exit behavior, and recovery identifiers on uncertain cleanup. State that trial does not create or spend campaign state and does not submit automatically.
- [ ] **6.2 Disposable real-Pueue E2E.** Reuse the existing E2E bootstrap. Create isolated state/config/project and a stdlib script that verifies cwd/input/env, writes manifest/artifact, and supports success/queued timeout/running timeout modes. Do not use a live user project or existing Pueue profile.
- [ ] **6.3 Assert artifacts and persistence.** After success and both timeout modes, assert no trial task/group/output remains, no registered-group task was created, no campaign/budget/ordinary Event/task-observation rows changed, and a repeat run uses different IDs. If callbacks are enabled, assert only bounded unknown-group integration events.
- [ ] **6.4 Verify actual Pueue group semantics.** On the supported Linux Pueue version, prove an existing nonce-named group is rejected by `create_group_exclusive`; prove successful create is attributable to this invocation; prove `group remove` is never issued before exact trial removal plus a fresh empty-group listing; confirm successful removal leaves the group absent.
- [ ] **6.5 Focused Linux gate at exact commit.** In an isolated Linux checkout with offline dependencies:

```bash
cargo fmt --check
cargo check --locked --offline --all-targets
cargo check --locked --offline --all-targets --release
cargo test --locked --offline --test diagnostics
cargo test --locked --offline --test codex_security
cargo test --locked --offline --test pueue_adapter
cargo test --locked --offline --test cli_help
cargo test --locked --offline --test operator_commands
cargo test --locked --offline --test reconciliation result_
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
git diff --check
```

- [ ] **6.6 Full Linux gate if focused gate is green.** Run `cargo test --locked --offline --all-targets -- --test-threads=1`. Record exact commit, host, command, exit code, test counts, skipped checks, and any pre-existing failures. Do not describe macOS compile-only evidence as Linux runtime evidence.
- [ ] **6.7 Final independent review.** One reviewer checks spec compliance and another checks the final combined diff/security invariants. Fixes repeat focused tests and review. Root creates the final Stage 3 commit; merge/push/release remain outside this plan.

## Completion evidence

Stage 3 is complete only when the verification report demonstrates all of the following at one exact commit:

- no-campaign objective and installed Codex checks are accurate and read-only;
- real Pueue success, queued timeout, running timeout, and add ambiguity follow the bounded lifecycle;
- real child sees expected argv/environment/cwd and produces a production-classified manifest;
- campaign lineage/budget and registered project group remain untouched;
- no ordinary Event/task observation/agent launch comes from the nonce trial group;
- exact task/group/output cleanup succeeds, or the failure report truthfully identifies retained resources;
- focused and full isolated Linux gates are reported without touching live services.
