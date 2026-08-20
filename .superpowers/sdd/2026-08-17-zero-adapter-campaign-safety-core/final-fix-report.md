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
