# Zero-adapter isolated code changes design

Date: 2026-09-01
Phase: 5 (isolated agent edits, candidate commits, and revision-pinned experiments)
Base: `main` at `4193bbc` (schema v25)

## 1. Purpose

Phase 4 can evaluate an experiment and promote its result, but campaign agents
still cannot test a hypothesis that requires a source-code change. A
`code_change` proposal is accepted into a pending state and then rejected by
the runtime. Users must currently provide an external controller to edit a
repository, run checks, make a commit, and submit the resulting experiment.

Phase 5 connects the existing pending proposal and rolling-budget path to a
durable internal pipeline. After the ordinary `pueue-agent submit`, a campaign
may select `code_change`; pueue-agent then creates an isolated worktree, asks a
dedicated editor agent to make the change, validates and commits the candidate,
submits an experiment pinned to that commit, and reuses Phase 4 evaluation.
No repository-specific adapter or intermediate controller is required.

## 2. Requirements

1. A valid `code_change` decision enters an asynchronous, restart-safe pipeline
   instead of being rejected by the runtime.
2. Every edit occurs in a service-owned Git worktree created from a fixed,
   clean base commit. The original working tree and protected branch remain
   unchanged.
3. Built-in Codex and enrolled custom agents use one structured editor
   contract. Each proposal starts a fresh session; the one permitted correction
   attempt resumes that same session.
4. The supervisor, not the editor, validates the diff, runs required checks,
   creates the candidate commit, and owns all Git ref mutations.
5. A candidate experiment runs from an immutable worktree at the persisted
   candidate SHA. Proposal, commit, experiment, Pueue task, metric, and
   promotion lineage remain queryable.
6. Candidate promotion updates only local campaign refs. Phase 5 never merges,
   rebases, pushes, opens a PR, or modifies `main` automatically.
7. Interrupted editing, checking, committing, submission, evaluation, and
   cleanup can be reconciled without duplicate edits, commits, or Pueue tasks.
8. OOM, internal failure, timeout, cancellation, invalid metrics, and failed
   checks cannot promote a candidate.
9. Existing non-code proposals and campaigns without code changes retain their
   current behavior.

## 3. Non-goals

- OS namespace, container, VM, or untrusted-agent isolation. That is Phase 6.
- Automatic merge to the user's protected branch, remote push, or PR creation.
- General environment or dataset versioning beyond the candidate Git SHA.
- An arbitrary shell-script build system or a plugin framework for check
  discovery. Phase 5 starts with deterministic Rust/Python profiles plus
  policy-valid editor proposals.
- Multiple concurrent code-change pipelines within one campaign.
- Replacing Phase 3 health monitoring or Phase 4 metric evaluation.

## 4. Invariants and trust boundary

- SQLite is authoritative for pipeline state, budget, ownership, and lineage.
- Git is authoritative only for object IDs and exact local ref values. State is
  never reconstructed from branch names alone.
- Pueue is authoritative only for the task identified by the persisted task ID
  and task signature.
- The startup-pinned project root is read-only to the code-change pipeline. A
  service-owned worktree root is its only source-editing root.
- A candidate commit is created only from the persisted base SHA after all
  required checks pass.
- An experiment is submitted only after its candidate SHA and ref are durable.
- `main`, the checked-out source branch, unrelated worktrees, and remote refs
  are outside pipeline ownership.
- Raw diffs, complete command output, prompts, conversations, and credentials
  are not stored in SQLite or human-readable audit events.

An enrolled custom editor is a trusted executable in Phase 5. pueue-agent
removes credentials, constrains its declared working root, and validates every
resulting byte before use, but cannot prevent a malicious executable from
escaping at the OS level without the Phase 6 isolation backend.

## 5. Architecture and ownership

```text
validated code_change decision
  -> atomic proposal + code-change budget admission
  -> CodeChangeCoordinator
       -> WorktreeManager (fixed base, service-owned path)
       -> EditorRunner (fresh session, at most one resume)
       -> CandidateValidator / CheckRunner
       -> CandidateRepository (commit + local candidate ref)
       -> existing campaign submission path (candidate cwd + SHA)
  -> existing running-health and terminal projection
  -> existing Phase 4 metric evaluation
       -> CandidateRepository (compare-and-swap best ref on improvement)
  -> descriptor-owned worktree cleanup
```

`CodeChangeCoordinator` advances one durable transition at a time. It does not
hold a database transaction while an agent, Git command, check, or Pueue call
runs. Before and after each external operation it records an intent/result and
revalidates the operation's exact identity.

`WorktreeManager` is the only component that may create or remove Phase 5
worktrees. `CandidateRepository` is the only component that may create commits
or mutate `refs/heads/campaign/...`. The existing campaign submission service
remains the only component that may add a Pueue task.

## 6. Durable state machine

Each pending code-change proposal has exactly one pipeline row.

```text
reserved
  -> preparing_worktree
  -> editing
  -> checking
  -> committing
  -> candidate_ready
  -> experiment_submitted
  -> evaluated
  -> cleanup_pending
  -> completed

pre-candidate failure ---------------------------> rejected
uncertain or contradictory external state ------> recovery_required
```

State meanings:

- `reserved`: the existing proposal and rolling code-change reservation are
  durable, but no filesystem mutation has started.
- `preparing_worktree`: base, repository identity, and owned destination are
  fixed; worktree creation may be reconciled.
- `editing`: one editor attempt is reserved or running.
- `checking`: editor output was accepted and supervisor checks are running.
- `committing`: checks passed and the exact tree intended for the candidate is
  recorded; commit/ref creation may be reconciled.
- `candidate_ready`: candidate SHA and local candidate ref are verified.
- `experiment_submitted`: the experiment intent is linked to the candidate and
  the existing Pueue submission state machine owns dispatch/reconciliation.
- `evaluated`: the experiment is terminal and Phase 4 evaluation, including a
  possible best-ref update, is durable.
- `cleanup_pending`: no live process may use the worktree; owned cleanup can be
  retried.
- `completed`: cleanup is confirmed while commit/ref/lineage metadata remains.
- `rejected`: no candidate experiment was admitted. A bounded reason is stored
  and cleanup is still completed before the row becomes terminal.
- `recovery_required`: observed Git, filesystem, DB, or Pueue state contradicts
  the recorded identity. Automatic advancement and promotion stop.

Rejected cleanup is represented by a cleanup flag/timestamp on the terminal
row rather than creating a second public rejection state. Diagnostics report a
rejected row whose cleanup is still pending.

## 7. Data model (schema v26)

Schema v26 adds a code-change pipeline table, a bounded check table, and an
explicit candidate revision on experiments. Exact column names may follow
existing repository conventions, but the represented invariants are fixed.

```sql
CREATE TABLE code_change_runs (
    code_change_run_id TEXT PRIMARY KEY,
    proposal_id TEXT NOT NULL UNIQUE REFERENCES proposals(proposal_id),
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
    state TEXT NOT NULL CHECK (state IN (
        'reserved', 'preparing_worktree', 'editing', 'checking', 'committing',
        'candidate_ready', 'experiment_submitted', 'evaluated',
        'cleanup_pending', 'completed', 'rejected', 'recovery_required'
    )),
    base_sha TEXT NOT NULL,
    candidate_sha TEXT,
    candidate_ref TEXT NOT NULL,
    best_ref TEXT NOT NULL,
    worktree_id TEXT NOT NULL UNIQUE,
    worktree_relative_path TEXT NOT NULL UNIQUE,
    editor_session_id TEXT,
    editor_attempts INTEGER NOT NULL DEFAULT 0,
    diff_digest TEXT,
    changed_file_count INTEGER,
    diff_bytes INTEGER,
    experiment_id TEXT UNIQUE REFERENCES experiments(experiment_id),
    rejection_code TEXT,
    rejection_summary TEXT,
    cleanup_completed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX code_change_one_live_per_campaign
ON code_change_runs(campaign_id)
WHERE state NOT IN ('completed', 'rejected');

CREATE TABLE code_change_editor_attempts (
    code_change_run_id TEXT NOT NULL REFERENCES code_change_runs(code_change_run_id),
    attempt INTEGER NOT NULL CHECK (attempt IN (1, 2)),
    agent_run_id INTEGER NOT NULL UNIQUE REFERENCES agent_runs(run_id),
    editor_session_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'running', 'ready', 'failed')),
    result_digest TEXT,
    failure_code TEXT,
    failure_summary TEXT,
    started_at INTEGER,
    finished_at INTEGER,
    PRIMARY KEY(code_change_run_id, attempt)
);

CREATE TABLE code_change_checks (
    code_change_run_id TEXT NOT NULL REFERENCES code_change_runs(code_change_run_id),
    attempt INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('supervisor', 'discovered', 'editor')),
    argv_json TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'passed', 'failed', 'timed_out')),
    output_digest TEXT,
    summary TEXT,
    started_at INTEGER,
    finished_at INTEGER,
    PRIMARY KEY(code_change_run_id, attempt, ordinal)
);

ALTER TABLE experiments ADD COLUMN code_change_run_id TEXT
    REFERENCES code_change_runs(code_change_run_id);
ALTER TABLE experiments ADD COLUMN code_revision_sha TEXT;
ALTER TABLE campaigns ADD COLUMN base_revision_sha TEXT;
```

The implementation must add checks equivalent to:

- SHA values are canonical lowercase full object IDs, not rev expressions.
- candidate SHA is absent before commit and present from `candidate_ready`.
- editor attempts are in `0..=2` and never reset during restart recovery.
- an experiment link and revision are both present from submission onward.
- ref names are derived only from validated internal campaign/proposal IDs.
- summaries and JSON fields use the project's existing byte/count bounds.

The pipeline stores identifiers, exact SHA/ref values, an owned relative
worktree locator, attempt/check status, output and diff digests, bounded
summaries, diff counts, experiment/task lineage, rejection/cleanup facts, and
timestamps. It does not store a raw diff, raw log, or editor conversation.

Campaign base revision does not need a new mutable `current code` column. The
coordinator resolves the local campaign best ref when admitting a proposal and
persists the resulting full SHA as the immutable `base_sha`; before a best ref
exists it uses the nullable `base_revision_sha` pinned at campaign start. A new
campaign in a non-Git or dirty project remains usable for non-code experiments
but leaves this column NULL, making later code-change admission fail closed.

## 8. Admission and concurrency

The decision protocol permits `ProposalKind::CodeChange` with the same source
experiment, objective digest, canonical digest, evidence, argv, and working
directory validation used by other proposals. Existing `PendingCodeChange`
admission and `code_change` rolling reservation are reused.

Additional admission checks are fail closed:

1. Campaign policy permits code changes. The shipped default permits them but
   retains hard service-owned limits.
2. No other live code-change pipeline exists for the campaign.
3. The project is a verified Git worktree with a clean, committed campaign
   start revision and a resolvable current-best/base SHA.
4. The proposal working directory is a normalized descendant that will exist
   within the candidate worktree.

A non-Git repository, dirty/unfixed campaign base, unavailable Git executable,
or invalid base rejects only that `code_change` proposal. The campaign remains
active and may continue non-code experiments.

The code-change reservation is consumed at proposal acceptance, as in schema
v25. A later precondition, edit, or check rejection does not return the slot;
this prevents unlimited invalid edit attempts. The correction attempt does not
consume another code-change slot, but every editor start consumes the normal
agent-run budget. Candidate experiment creation reserves the normal experiment
slot and follows existing `budget_waiting` wake behavior.

## 9. Worktree and Git lifecycle

The worktree root is a service-owned state directory outside the user's source
tree. Its locator is composed only from internal IDs and stored as an owned
relative path; deletion never accepts an editor- or repository-supplied path.

Preparation performs the following checks and operations:

1. Pin the repository identity and full base SHA.
2. Verify the original project worktree is still the expected clean campaign
   base boundary. Unrelated registered worktrees are never pruned.
3. Create a detached worktree at the exact base SHA using argv-form Git.
4. Open/validate the destination through the existing no-follow,
   descriptor-owned filesystem boundary before publishing ownership.
5. Re-read `HEAD`, repository common directory, and cleanliness before editing.
6. Snapshot protected refs and remote configuration so every post-editor check
   can detect a contract violation. A mismatch is quarantined and is never
   automatically rolled back or overwritten.

After checks pass, the supervisor stages only validated paths and creates one
intentional commit with a fixed pueue-agent author/committer identity. User Git
credentials, signing programs, hooks, aliases, pager, and global mutation
settings are not inherited. Commit recovery compares the recorded tree/base
with existing objects; it never blindly creates a second commit.

The supervisor creates the local candidate ref:

```text
campaign/<campaign-id>/candidate/<proposal-id>
```

using exact expected-old/new object IDs. The worktree is then made immutable
from the coordinator's perspective: it must remain clean and at candidate SHA
through experiment termination. A rejected evaluated candidate retains its
commit and local candidate ref for reproduction. Its worktree is removed only
after terminal evaluation is durable.

Cleanup validates DB ownership, repository identity, exact worktree path, and
absence of a live experiment/process before removing it. It does not run broad
`git worktree prune`, follow symlinks, delete an unknown path, or touch another
tool's worktree. Failed cleanup remains retryable and visible to diagnostics.

## 10. Editor protocol

The editor receives a bounded, supervisor-authored context containing:

- schema version and code-change/proposal identifiers;
- objective, validated hypothesis, source experiment, and bounded evidence;
- exact base SHA and dedicated worktree root;
- requested experiment argv/working directory;
- protected paths, maximum changed files/diff bytes, and time/resource limits;
- instruction to edit only, not commit, mutate refs, submit Pueue, or change
  service policy/state.

The result is a private, size-bounded JSON object:

```json
{
  "schema_version": 1,
  "status": "ready",
  "summary": "bounded description",
  "proposed_checks": [
    {"argv": ["python", "-m", "pytest", "tests/smoke"], "cwd": "."}
  ]
}
```

`status` is `ready` or `cannot_apply`. Unknown fields, invalid UTF-8, excess
entries/bytes, absolute/traversing cwd, empty argv, shell-form commands, or an
unapproved executable reject the result. A valid result is only a request to
inspect the worktree; it is never evidence that the edit or checks succeeded.

Every proposal begins with a fresh editor session. If editor execution or any
required check fails, the supervisor supplies a bounded failure/check summary
and resumes the same session once. It then reruns the entire diff validation and
check set. There are at most two editor attempts in total. Failure of the second
attempt rejects the proposal. Restart recovery cannot reset or multiply this
limit.

Built-in Codex and enrolled custom agents use this same transport and schema.
Credential, SSH agent, Git authentication/signing, and unrelated service state
are removed from their environment. Network is allowed by default, subject to
the service execution policy. Phase 5 relies on an enrolled custom executable
being trusted; full host containment is explicitly deferred. Native editor and
candidate checks must never run as root, and operator documentation recommends
a dedicated unprivileged service account until Phase 6 isolation is available.

## 11. Diff validation and required checks

Before every check round the supervisor independently enumerates the diff from
the exact base SHA and validates:

- worktree/repository identity, `HEAD`, and absence of unexpected Git state;
- no path traversal, symlink escape, submodule boundary mutation, or nested
  repository substitution;
- no `.git`, pueue-agent state/policy, credential-like, or other protected path;
- service-policy maximum changed-file count and total diff bytes;
- a stable diff digest used to bind the check round to the committed tree.

Checks use argv arrays only, never a shell. Executable, cwd, environment,
timeout, output, process, and resource bounds are validated before start. A
non-zero exit, signal, timeout, OOM-like termination, output-policy failure, or
identity change fails the check round and terminates its process group through
the existing execution boundary.

The required set is:

1. supervisor-owned `git diff --check` for every candidate;
2. deterministic discovery from exact repository markers;
3. zero or more valid editor-proposed checks allowed by service policy.

Initial discovery supports conservative profiles:

- Rust: a root `Cargo.toml` selects
  `cargo test --all-targets -- --test-threads=1`.
- Python: `pytest.ini` or a parsed `[tool.pytest.ini_options]` table in
  `pyproject.toml` establishes pytest usage. With `uv.lock` the profile is
  `uv run pytest`; otherwise it is `python -m pytest`.

Profile selection and argv construction are code-owned and deterministic.
Repository files may select a known profile but may not inject arguments.
Editor-proposed checks may add coverage but cannot weaken or remove discovered
checks. At least one project-specific discovered or valid proposed check must
pass in addition to `git diff --check`; otherwise the proposal is rejected.

Immediately before commit, the supervisor re-enumerates the diff and requires
the same digest that passed checks. Any edit after checking sends the run back
to validation or rejects it; unchecked bytes are never committed.

## 12. Candidate experiment and evaluation lineage

The proposal's validated experiment argv and relative working directory are
submitted through the existing campaign intent protocol, but cwd is resolved
inside the candidate worktree. The experiment row records
`code_change_run_id` and the full `code_revision_sha`. Submission preflight and
post-add identity validation include the candidate root/SHA identity so a task
cannot silently run from the original checkout.

The candidate worktree remains present and at the candidate `HEAD` while the
Pueue task is live. "Immutable" in Phase 5 means the coordinator performs no
further source edits and the experiment is logically bound to that commit;
without Phase 6 read-only mounts, a task could still mutate its checkout.
Before evaluation/promotion, the supervisor therefore verifies that `HEAD`, the
index, and every tracked source path still match the candidate tree. Any tracked
mutation makes the result non-promotable. Untracked result/artifact files are
handled through the existing bounded artifact paths and never change revision
identity. Phase 3 observes runtime health normally. OOM, internal error,
timeout, confirmed cancellation, and other terminal failures are projected
normally and mark the candidate non-promotable.

Phase 4 remains authoritative for metric validity and improvement. On a valid
improvement, the coordinator updates the local best ref:

```text
campaign/<campaign-id>/best
```

with an expected-old SHA and candidate new SHA (`git update-ref` compare and
swap). No-improvement or invalid/missing evidence leaves best unchanged. A CAS
failure never overwrites the observed ref and moves the run to reconciliation.
Git ref mutation and SQLite evaluation cannot be one transaction, so the
coordinator records an intent, performs conditional `update-ref`, then verifies
the exact ref before committing the DB result. Restart follows the same
verification path.

Only one code-change pipeline is live per campaign in Phase 5, limiting best
ref races. Other experiments may continue when ordinary campaign parallel and
budget rules permit; each experiment retains its own explicit revision lineage.

## 13. Crash recovery and idempotency

Daemon startup and periodic reconciliation inspect nonterminal runs in stable
order. Each state has an observation-based recovery rule:

- worktree absent before publication: create it; unexpected owned path:
  `recovery_required`;
- editor agent run terminal with a validated result: continue without rerun;
  uncertain/running identity: use existing agent-run reconciliation;
- completed checks with the same diff digest: reuse them; changed digest:
  invalidate and start the permitted correction path;
- candidate object/ref already equal to persisted identity: continue; a
  different ref/tree: `recovery_required`;
- linked submission with persisted Pueue identity: reuse it; existing
  reserved/submitting/unreconciled intent is handled by the existing submission
  reconciler; never call add merely because the pipeline restarted;
- best ref already at candidate after a recorded promotion intent: verify and
  finish the DB projection; an unrelated value is not overwritten;
- cleanup target absent after ownership verification: mark cleanup complete;
  an unowned or mismatched target is not removed.

External timeouts are not proof of failure. An uncertain Git or Pueue mutation
is resolved by reading exact durable identities before any retry. Contradiction
produces a bounded incident/diagnostic and prevents promotion, while ordinary
editor/check failure follows the finite two-attempt rejection path.

## 14. Status, events, and diagnostics

No new mandatory command or repository configuration is introduced. Existing
campaign status and diagnostic surfaces gain bounded projections:

- current pipeline state, editor attempt count, base/candidate abbreviated SHA,
  linked experiment/task, and next action;
- editor/check/commit/submission/evaluation/promotion/cleanup audit events;
- failed check name/status and bounded summary, with output/diff digests;
- orphaned owned worktree, missing/mismatched candidate ref, stale pipeline,
  experiment revision mismatch, CAS conflict, and cleanup failure diagnostics.

Human-readable output must not expose prompts, raw diffs, raw environment,
credentials, or full logs. JSON output uses stable identifiers and enums so an
operator can inspect or recover a quarantined run without parsing prose.

## 15. Compatibility and rollout

- Schema migration from v25 creates empty Phase 5 tables/columns and preserves
  all existing campaigns, proposals, experiments, and budgets.
- Existing non-code proposals take their current direct admission/submission
  path unchanged.
- Existing v25 campaigns have no trustworthy campaign-start revision and keep
  `base_revision_sha` NULL. They continue normally, but code-change proposals
  fail closed; starting a new campaign records the required clean SHA. A
  schema-v25 pending code-change proposal is therefore rejected with a bounded
  legacy-base reason rather than binding it to an inferred later checkout.
- Code changes are enabled in the shipped campaign defaults, while service
  policy retains finite code-change/agent/experiment budgets, diff caps,
  protected paths, executable policy, and required-check rules.
- The daemon processes a bounded number of state transitions per tick so a
  large backlog cannot starve monitoring or ordinary submissions.
- Linux is the supported execution target. macOS may compile or run unit tests
  but is not promoted to an equivalent security/runtime support boundary.

## 16. Verification strategy

Unit tests cover:

- state transitions and terminal/idempotent cases;
- editor JSON bounds, fresh/resumed session selection, and total attempt limit;
- internal ref/path derivation and invalid identifier rejection;
- diff/protected-path/file-count/byte-count validation;
- deterministic Cargo/Python profile discovery and proposed-check validation;
- promotion eligibility and expected-old/new ref decisions.

Database and integration tests cover:

- v25-to-v26 migration and schema invariants;
- concurrent proposal admission and one-live-code-change enforcement;
- budget consumption, editor agent-run accounting, and experiment reservation;
- restart at every external-operation boundary without duplicate commit/task;
- fake editor success, `cannot_apply`, malformed output, timeout, first-round
  failure then successful resume, and second-round rejection;
- Git non-repository/dirty base, ref mismatch, post-check mutation, commit
  recovery, promotion CAS conflict, and descriptor-owned cleanup;
- candidate Pueue cwd/SHA binding, Phase 3 OOM/error projection, Phase 4 metric
  promotion/no-promotion, and original worktree/`main` invariants;
- security regressions for traversal, symlink, nested repo/submodule, shell
  argv, unapproved executable, environment credential removal, and output caps;
- unchanged non-code campaign behavior.

The Linux acceptance run uses the test-only checkout on `roko`, updates it to
the exact Phase 5 revision, and runs the full Rust/integration suites plus a
real-Pueue end-to-end campaign in a disposable sample Git ML repository. It
must demonstrate editor retry, required checks, candidate commit/ref, an
experiment running from the candidate SHA, evaluation, safe cleanup, and an
unchanged original checkout/`main`. No development or authoritative source
state lives on `roko`; it is only a verification host.

## 17. Acceptance criteria

1. One ordinary submit can autonomously reach a successful candidate experiment
   after an agent decides that a source edit is required.
2. Every candidate experiment has a persisted proposal/base/candidate/experiment
   lineage and actually runs at the recorded candidate SHA.
3. A failed edit/check receives at most one same-session correction, and a
   second failure is durably rejected.
4. No candidate is committed without `git diff --check` and at least one
   successful project-specific check bound to the final diff digest.
5. Restart injection at every state produces neither duplicate commits nor
   duplicate Pueue tasks.
6. Runtime failure or non-improvement does not move campaign best; a valid
   improvement moves only the local campaign best ref by compare-and-swap.
7. Original working tree, protected branch, unrelated worktrees, and remotes
   remain byte/ref unchanged in success and failure tests.
8. Owned worktrees are retained while needed, cleaned after terminal results,
   and never cleaned through an unverified or broad path operation.
9. Status and diagnostics explain every active, rejected, inconsistent, or
   cleanup-pending pipeline without disclosing raw sensitive material.
