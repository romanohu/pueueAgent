# Phase 1 managed-campaign safety final fix report

Date: 2026-08-20

Review base: `6cf7ee95b65f4a0d906c2c9a588950d27b54a19a`

## Result

All eight Important findings were reproduced or demonstrated against the review base and were
valid. This wave closes them without adding a proposal loop, periodic observer, goal evaluator,
autonomous code-change path, or worktree execution. The implementation remains a Phase 1 safety
core: the first accepted submission establishes a durable campaign and baseline; later work is
limited to explicitly accepted durable intents and event-driven agent decisions.

## Finding verdicts and RED evidence

1. **Accepted managed task identity — valid, fixed.** The base constructed a provisional identity
   from group and reusable numeric task ID and allowed reconciliation to equate it with an observed
   task. The RED ID-reuse scenario terminal-projected the wrong same-group task. Submission now
   requires exactly one returned numeric ID, matching group/command, and a parseable Pueue enqueue
   timestamp before storing a bounded `pueue-managed-run:v1` digest. Missing, invalid, duplicate,
   or mismatched observations remain unreconciled. Schema v17 conservatively quarantines every
   legacy provisional managed identity, including already-terminal experiments, without releasing
   consumed reservations.

2. **Failure fingerprints represented task instances — valid, fixed.** RED coverage showed two
   equivalent managed failures with different task IDs/timestamps, and later different volatile
   result messages, produced different fingerprints. Reconciliation now hashes normalized,
   bounded cause evidence (recognized numeric exit code); unclassified free-form results fail
   closed to one bounded unclassified cause rather than minting retry-specific identities.

3. **Persisted objective was not authoritative at runtime — valid, fixed.** RED prompt coverage
   showed the SQLite snapshot was absent and mutable `STATE.md` remained behavioral authority. A
   second RED used a maximum-size objective made entirely of JSON-escaped characters and showed
   its tail was truncated. Campaign prompts now carry the persisted objective snapshot, campaign
   ID/digest, budget state, and explicit on-disk non-authority. The campaign-only prompt bound is
   large enough for the maximum valid escaped objective plus its fixed envelope. Template language
   names SQLite-backed prompt context as authority.

4. **Events were rebound to the current live campaign — valid, fixed.** The base schema had no
   campaign/experiment event lineage, and the RED retirement/restart scenario charged an old event
   to the replacement campaign. Schema v17 adds trusted lineage and a campaign/status/wake index;
   terminal materialization records exact lineage, safely upgrades an existing unlineaged canonical
   event after acceptance, and detects conflicts. Projection occurs before terminal event
   materialization. Scheduler admission gates exact event lineage, drains retired/mismatched
   campaign work, and re-reads/re-gates the live campaign after acquiring the project admission
   lock so a concurrent baseline cannot launch an unbudgeted legacy event.

5. **Lifecycle state did not reliably suppress agent dispatch — valid, fixed.** RED coverage showed
   an existing same-key reservation was reused after pause and a new paused key raised validation
   through the daemon. State classification now precedes idempotent reuse. Finite budget waits use
   `RetryWait`; paused/degraded/halted/review states defer without consuming attempts. Claim SQL
   excludes projects with a non-Active live campaign to prevent claim-limit starvation, while the
   repository and post-lock scheduler checks remain authoritative. Suppressive project and
   campaign lifecycle transitions share the admission lock; guardrail lock contention defers the
   claimed events for retry instead of failing the daemon.

6. **Reserved recovery ignored current authority and root identity — valid, fixed.** RED recovery
   tests showed paused intents could enter `Submitting` and a replaced root could reach Pueue add.
   Production recovery uses the service-start-pinned root anchor, verifies it before and after
   acquiring the project admission lock, and retains verified-root/admission authority across the
   authoritative `Reserved -> Submitting` transition, Pueue add, observed identity, and durable
   result. The transition atomically rechecks project enabled/pause/halt, campaign Active state,
   root/group, and lineage. Recovery processes one bounded snapshot per daemon tick; a busy lock is
   skipped without spin or daemon failure and becomes eligible on a later tick.

7. **Positive rolling exhaustion never entered finite budget waiting — valid, fixed.** The RED
   experiment-limit test encoded the prior validation error and Active state; equivalent RED was
   added for code-change limits. Proposal acceptance now returns a typed `ProposalAcceptance`,
   atomically records dimension-specific `budget_waiting` reason and the earliest live reservation
   expiry, creates no intent while exhausted, and replays the durable wait on retries. Wake at
   expiry reactivates the campaign. A zero code-change limit remains a disabled capability with no
   fabricated wake.

8. **Campaign/control/batch admission was not serialized — valid, fixed.** Deterministic blocking
   Pueue RED barriers allowed the losing side to persist/add after the other path's state check.
   Baseline, direct control, and batch now use the same per-project admission lock and pinned root,
   hold it through final campaign-state recheck, durable claim/intent, external add, and durable
   result. Barrier GREEN coverage establishes one in-flight admission and no losing-side rows/adds.

## Minor findings

- Managed add preflight now uses the exact eleven-item framing overhead: 245 user argv entries are
  accepted and 246 are rejected before persistence or add.
- Same-spec identity includes normalized working directory.
- Active-campaign steer precedence occurs before metadata/origin validation and is repeated under
  the admission lock.
- User-facing replacement language is truthful for Phase 1: agents may record a bounded
  recommendation/proposal in `state.json` but must not submit a replacement.
- The optional inspection-coverage expansion was not needed by a touched production path and was
  intentionally left outside this fix wave.

## Architecture and invariants

- SQLite owns immutable campaign objective, lineage, state, and rolling-budget authority.
- Only Active campaigns may create/reuse dispatch reservations or bind managed submissions.
- Project admission is a nonblocking, verified-root, per-project serialization boundary. No SQLite
  transaction is held across an async Pueue call.
- A Pueue side effect is never treated as accepted until a unique, stable, externally observed run
  identity is persisted. Unknown/ambiguous outcomes are unreconciled and are never automatically
  re-added.
- Event lineage is captured at materialization and checked again under admission immediately before
  reservation/spawn.
- Positive rolling exhaustion always has a finite durable wake; zero-disabled capabilities do not.
- All prompt/error/event evidence remains bounded and redacted; task instance identity is separate
  from normalized failure-cause identity.

## Changed files

- `src/batches.rs`
- `src/campaign.rs`
- `src/daemon.rs`
- `src/db/campaigns.rs`
- `src/db/migrations.rs`
- `src/db/mod.rs`
- `src/db/repositories.rs`
- `src/events.rs`
- `src/execution_policy.rs`
- `src/main.rs`
- `src/models.rs`
- `src/proposals.rs`
- `src/reconcile.rs`
- `src/scheduler.rs`
- `src/submit.rs`
- `templates/instructions.md`
- `tests/integration/daemon.rs`
- `tests/integration/database.rs`
- `tests/integration/init.rs`
- `tests/integration/pueue_adapter.rs`
- `tests/integration/reconciliation.rs`
- `tests/integration/scheduler.rs`
- `tests/support/fake_pueue.rs`
- `.superpowers/sdd/2026-08-17-zero-adapter-campaign-safety-core/final-fix-report.md`

The user-owned untracked `docs/report/` tree was not read, modified, or staged.

## Verification evidence

Focused RED tests were run before their corresponding production changes. After implementation,
the changed boundaries were exercised by the following GREEN suites/targets:

- `cargo test --test database` — 163 passed, 0 failed.
- `cargo test --test scheduler` — 68 passed, 0 failed; the later maximum-escaped-objective focused
  regression also passed.
- `cargo test --test reconciliation` — 25 passed, 0 failed; the later normalized volatile-result
  focused regression also passed.
- Focused daemon reserved-recovery pause/resume test — 1 passed, 0 failed.
- Focused Pueue adapter campaign-pause recovery, pinned-root replacement, baseline/control/batch
  admission barriers, and argv-boundary tests — passed.
- Focused init template contract — passed.
- `cargo check --all-targets` — passed.
- `cargo check --release --all-targets` — passed.
- `git diff --check` — passed.

## Baseline and platform limitations

The broader Darwin runs exposed only independently reproduced baseline/platform failures:

- `pueue_adapter`: 67 passed and three native deadline tests failed both at the review head and
  after this wave.
- `daemon`: 46 passed and two shutdown/RetryWait expectation failures were reproduced at the review
  head; the changed reserved-recovery test is GREEN.
- `init`: 16 passed and one unrelated unreadable-policy fixture failed at the review head; the new
  template contract is GREEN.
- The pre-wave full suite was 182 passed, 1 failed, 12 ignored; the failure is the known Darwin
  `ENOTDIR` execution-policy case.

Supported Linux with real Pueue remains a separate pending environment gate. This report does not
claim supported-Linux real-Pueue GREEN from Darwin.

## Self-review

The final diff review found and closed additional interactions before completion: volatile failure
messages, JSON expansion of the maximum objective, duplicate/invalid observed task identities,
lineage upgrade of a fast terminal event, post-lock campaign revalidation, paused-claim starvation,
durable BudgetWaiting replay, terminal provisional-identity migration, lifecycle/admission
linearization, guardrail lock contention, and bounded daemon recovery under admission contention.

Production CLI and daemon paths always use service-start-pinned anchors. The legacy unanchored
library helpers remain compatibility/test seams; they still verify the current registered root and
take the same admission lock, but callers that need service-start replacement detection must use
the explicit `*_with_root_anchor` entrypoints. Intervention rows remain project-scoped by the Phase
1 schema; exact campaign lineage for pending operator interventions should be considered with any
future campaign-scoped intervention schema change, not by adding an autonomous loop here.

## Commit

The implementation and this report are committed together with subject
`fix: close managed campaign safety gaps`. The exact commit hash is reported in the final handoff
because a commit cannot contain its own content hash.

## Additional human-authorized residual fix wave

This additional wave was explicitly authorized after the final-wave cap. It reviewed the four
load-bearing residual Important findings against `56c59fa21a27cc17eb992bc55d66c86f860de80c` and
found all four valid.

### Residual verdicts and RED/GREEN evidence

1. **Schema v17 quarantined ordinary provisional submissions — valid, fixed.** The migration RED
   left accepted standalone control and batch-style rows `unreconciled`; a separate current-schema
   RED showed the verifier accepted a linked managed provisional submission because it tested the
   nonexistent `kind = 'campaign'` value. Migration and verification now classify managed
   submissions solely by `experiments.submission_id` linkage. Accepted unlinked control and batch
   rows retain status and identity. Linked accepted and terminal managed rows fail closed; failure
   fingerprints are cleared, while Reserved and Consumed reservation states are retained as
   appropriate.

2. **Trusted terminal lineage could not revive a scheduler-drained event — valid, fixed.** The RED
   reconciled a terminal task while its experiment was Submitting, let the scheduler complete the
   unlineaged event as `campaign_lineage_missing`, accepted the task, and reconciled again; the only
   event remained Completed. Terminal replacement now atomically requeues that same row only when
   it has newly trusted campaign and experiment lineage and no `agent_run_events` binding. GREEN
   coverage proves one row, the same event ID, Pending status, zero attempts, cleared lease,
   completion, and error fields, and exactly one dispatchable lineaged event.

3. **Reserved recovery treated ordinary authority loss as daemon-fatal — valid, fixed.** Three
   deterministic post-snapshot REDs let project pause, project disable, or finite BudgetWaiting win
   before the second reserved intent acquired admission. Each propagated validation out of the
   daemon tick. The coordinator now returns a typed `Submitted` or `Deferred` result. Its
   transactional Reserved transition first rejects experiment/submission invariant corruption,
   then defers ordinary campaign or project authority loss without changing the experiment or
   calling Pueue. GREEN coverage proves the affected intent remains Reserved, the affected group
   receives zero adds, and the daemon remains usable on the next tick for all three lifecycle
   states.

4. **Scheduler, direct control, and batch trusted stale project authority — valid, fixed.** The
   direct-control RED accepted a pre-paused project, while FIFO scheduler and batch RED barriers let
   pause or disable commit after the initial Project snapshot but before project admission; the
   scheduler started a run and the batch persisted/added using stale state. All three paths now
   reread Project while holding admission, require enabled, unpaused, unhalted authority, reject
   root/group identity drift, and use the refreshed Project. Final GREEN coverage uses exact
   lifecycle-before-admission barriers for all three paths and proves scheduler deferral with zero
   attempts/runs and direct/batch rejection with zero durable rows and zero Pueue adds.

### Additional-wave changed files

- `src/batches.rs`
- `src/campaign.rs`
- `src/daemon.rs`
- `src/db/campaigns.rs`
- `src/db/migrations.rs`
- `src/db/repositories.rs`
- `src/scheduler.rs`
- `src/submit.rs`
- `tests/integration/daemon.rs`
- `tests/integration/database.rs`
- `tests/integration/pueue_adapter.rs`
- `tests/integration/scheduler.rs`
- `tests/support/config_read_barrier.rs`
- `.superpowers/sdd/2026-08-17-zero-adapter-campaign-safety-core/final-fix-report.md`

### Additional-wave verification

Focused REDs were observed before the corresponding production edits. After implementation:

- `cargo test --test database` — 165 passed, 0 failed.
- `cargo test --test reconciliation` — 27 passed, 0 failed.
- `cargo test --test scheduler` — 71 passed, 0 failed.
- The three focused daemon post-snapshot authority-loss tests — 3 passed, 0 failed.
- The focused direct-control and batch lifecycle-before-admission tests — 2 passed, 0 failed.
- The focused terminal-lineage requeue test — 1 passed, 0 failed.
- `cargo check --all-targets` — passed.
- `cargo check --release --all-targets` — passed.
- `git diff --check` — passed.

The full daemon integration binary had 51 passes and the pre-existing shutdown/RetryWait failure;
the full Pueue-adapter binary had 71 passes and the pre-existing Darwin native-deadline failure.
The single requested broad run stopped in the untouched library binary with 182 passes, two Darwin
platform failures, and 12 ignored tests: the existing execution-policy `ENOTDIR` case and an
`EPERM` process-group probe case in untouched code. Cargo therefore did not proceed to the
integration binaries in that broad invocation. No production code implicated by either library
failure was changed in this wave. The repository-wide formatter check also continues to report
broad pre-existing formatting drift, so no unrelated formatting rewrite was applied.

### Remaining concerns

Supported-Linux real-Pueue verification remains the same external pending gate. This wave adds no
Phase 2 proposal loop, observer, evaluator, autonomous code-change path, or worktree execution.
The user-owned untracked `docs/report/` tree was not read, modified, or staged. The additional wave
is committed with subject `fix: close residual campaign races`; the exact commit hash is reported
in the handoff.

## Supported-Linux real-Pueue acceptance wave

The user authorized updating and testing an isolated checkout on host `roko`. The existing remote
checkout and its unrelated branch were not modified. Verification used Ubuntu 24.04 x86_64,
Pueue 4.0.4, Rust/Cargo 1.97, the isolated worktree
`/home/romanohu/project/.worktrees/pueue-agent-campaign-phase1-linux`, `umask 077`, an owner-only
TMPDIR, and an owner-only `CARGO_TARGET_DIR`.

Initial broad Linux failures were caused by the host's ambient `0002` umask and group-writable
build/temp parents. Re-running with the production security contract (`077` plus owner-only roots)
made the unchanged security checks pass. The real-Pueue E2E then exposed five genuine acceptance
gaps, each preserved as RED evidence before its fix:

1. The custom-profile fake returned an invented add ID but no status identity, so managed submit
   correctly quarantined it. The fixture now persists monotonically increasing task IDs and returns
   a matching queued task identity.
2. Pueue's official `FailedToSpawn`, `Errored`, and `DependencyFailed` terminal variants were not
   classified as failures. The classifier now covers every official non-success Pueue 4.0.4
   variant in string and externally tagged object form; unit and managed-reconciliation coverage
   prove `FailedToSpawn` produces a failed experiment rather than success.
3. `AgentRunnerConfig::production()` supplied no Codex capabilities, so real built-in Codex was
   always rejected as `unsafe_codex_argument`. Production now enables the complete forced policy
   surface; strict immutable argv/config, session ownership, and unsafe-argument validation remain
   mandatory. A direct unit RED/GREEN and the real resume E2E cover this wiring.
4. The Linux harness inherited Pueue's PATH-resolved default shell while production intentionally
   supplies a minimal sanitized PATH. The isolated Pueue config now pins the official absolute
   `/bin/sh -c {{ pueue_command_string }}` form, has a private runtime directory, and uses compiled
   fixture launchers rather than shebang executables that cannot satisfy the native `execveat`
   boundary. Synthetic callback events are explicitly attached to their durable campaign before
   scheduler dispatch, and an agent failure is observed as durable `retry_wait` while the daemon
   remains available.
5. The development launcher and installer ignored an absolute `CARGO_TARGET_DIR`, selecting or
   linking a different artifact. Both now honor the Cargo target directory, with separate tests
   proving both explicit and default behavior.

Fresh supported-Linux evidence after the final edits:

- `tests/e2e/run.sh` with real isolated pueued/SQLite/native launch — `Rust E2E PASS`.
- `cargo test --all-targets -- --test-threads=1` — exit 0.
- Pueue result-variant unit regression — 1 passed, 0 failed.
- managed `FailedToSpawn` reconciliation regression — 1 passed, 0 failed.
- production Codex capability regression — 1 passed, 0 failed.
- `bats tests/test_shell_entrypoints.bats` — 11 passed, 0 failed.
- `cargo check --all-targets` and `cargo check --release --all-targets` — passed.
- `bash -n` and `git diff --check` — passed.

Two Linux-only warnings in untouched process/mount cfg branches remain pre-existing and were not
reformatted or refactored. `cargo fmt --all -- --check` was attempted on `roko`, but that toolchain
does not include the `fmt` subcommand; shellcheck is also unavailable in the isolated Devbox.
`git diff --check` remains clean. The user-owned untracked `docs/report/` tree was not read,
modified, or staged. The acceptance-wave commit hash is pending the final independent diff review.

### Acceptance-wave independent review

The first independent review found two Important E2E portability defects: the final installer
assertion dereferenced an unset or relative `CARGO_TARGET_DIR`, and fixture creation still depended
on the caller's umask. It also noted early cleanup could observe an unset runtime directory and an
isolated pueued could outlive a failed client shutdown. The harness now sets `umask 077` itself,
normalizes and exports Cargo's default/relative/absolute target location before any use, initializes
the runtime directory before installing the EXIT trap, and retains a validated pueued PID for
bounded shutdown fallback.

Fresh review-condition proof ran the full real-Pueue E2E with the outer shell deliberately set to
`umask 0002` and `CARGO_TARGET_DIR` explicitly unset; it completed with `Rust E2E PASS`. The final
independent re-review verdict is **APPROVE**, with no Critical or Important finding in the tracked
acceptance-wave diff.
