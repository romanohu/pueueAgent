# Zero-adapter autonomous completion loop design

Date: 2026-08-21

Status: approved for implementation planning

## 1. Purpose

Phase 2 turns the Phase 1 campaign safety core into an autonomous terminal-result loop. After the
operator starts the baseline with one `pueue-agent submit`, a terminal experiment creates a durable
decision cycle. A read-only analysis agent inspects bounded evidence and returns either one
structured proposal or one finite wait decision. The existing campaign coordinator remains the only
component allowed to validate, reserve, and submit the next experiment.

The design keeps the zero-adapter property. An arbitrary ML repository does not need a repository-
specific controller or a mandatory result manifest before the first autonomous cycle can run.

## 2. Scope

Phase 2 includes:

- terminal success and terminal failure decision cycles;
- bounded evidence collection from canonical campaign state and the registered project;
- a fresh, read-only analysis agent run;
- structured `proposal` and `wait` decisions;
- proposal validation through the existing `ProposalInput` and `CampaignCoordinator` boundary;
- finite waits, idle recovery, budget waits, and restart recovery;
- bounded campaign status and doctor projections;
- Linux real-Pueue acceptance for success, failure, wait, and restart paths.

Phase 2 does not include:

- periodic observation of a running experiment;
- OOM, NaN, worker-loss, or staleness diagnosis before Pueue becomes terminal;
- automatic cancellation or early stopping;
- authoritative goal evaluation or automatic campaign retirement;
- source-code edits, worktree creation, commits, or merge automation;
- a mandatory result manifest, optimizer, or portable GPU accounting backend.

The Phase 2 decision runner is supported on Linux. A platform that cannot provide the verified
private-temp descendant namespace and a forced read-only analysis sandbox fails closed instead of
falling back to a mutable pathname.

Running health and the resumable 30-minute observer remain Phase 3. Goal review and quantitative
promotion remain a later phase. Isolated code changes remain Phase 5.

## 3. Design choice

The implementation uses durable decision cycles rather than extending the generic event state or
keeping one long-lived campaign agent.

```text
experiment terminal
  -> reserve one decision cycle
  -> collect bounded evidence
  -> run one read-only analysis agent
  -> validate one structured decision
       |-- proposal -> existing coordinator -> verified Pueue add
       `-- wait     -> finite next_wake_at
```

SQLite is the source of truth. Conversation history is not authoritative. Each agent run starts with
a reconstructed context bundle, so a daemon restart does not require a previous Codex session.

## 4. Domain model

Schema version 18 adds two tables.

### 4.1 `decision_cycles`

One row represents the autonomous decision associated with one terminal experiment.

Required fields:

- `cycle_id`;
- `campaign_id`;
- `source_experiment_id`;
- `state`;
- `next_wake_at`;
- `consecutive_failed_attempts`;
- bounded last-decision category and failure projection;
- `created_at` and `updated_at`.

`(campaign_id, source_experiment_id)` is unique. The source experiment must belong to the campaign
and be terminal. A legacy or unlineaged terminal event cannot create a managed cycle.

Cycle states are:

- `pending`: a new attempt may be reserved;
- `analyzing`: one attempt owns evidence collection or an analysis agent run;
- `waiting`: a valid finite wait was accepted;
- `completed`: a proposal was durably accepted by the existing coordinator;
- `degraded`: bounded decision failures were exhausted or a durable invariant failed.

### 4.2 `decision_attempts`

One row represents one evidence bundle and one analysis agent result.

Required fields:

- `cycle_id` and monotonic `attempt_number`;
- `state`;
- `context_schema_version`, bounded `context_json`, and `context_digest`;
- optional linked `agent_run_id`;
- optional bounded `decision_json`, `decision_digest`, and decision kind;
- typed failure category and bounded failure summary;
- `created_at`, `started_at`, and `finished_at`.

`(cycle_id, attempt_number)` is unique. A cycle has at most one active attempt. An `agent_run_id`
can bind to at most one decision attempt.

Attempt states are:

- `reserved`;
- `evidence_ready`;
- `running`;
- `decided`;
- `failed`.

## 5. Invariants and ordering

The repositories enforce the following under `BEGIN IMMEDIATE` transactions:

1. A terminal experiment creates at most one cycle.
2. A campaign runs at most one analysis agent at a time.
3. A cycle has at most one active attempt.
4. The project and campaign must be enabled, unpaused, unhalted, live, and `active` before reserving
   an attempt or accepting a proposal.
5. Pause, disable, halt, retire, attempt reservation, decision acceptance, and experiment admission
   share the existing project admission lock.
6. The source experiment, objective digest, project identity, campaign state, failure fingerprint,
   and working directory are re-read inside the acceptance transaction.
7. Supervisor-generated cycle, proposal, experiment, submission, and idempotency identifiers are not
   accepted from agent output.
8. Pending, analyzing, and waiting cycles count as live automation, so the idle watchdog does not
   create duplicate work.
9. Multiple terminal experiments are ordered by stable terminal time and experiment ID. Later cycles
   remain pending until the campaign's current analysis attempt finishes.
10. No filesystem, Pueue, or process operation runs inside a SQLite writer transaction.

## 6. Evidence bundle

The supervisor reconstructs a bounded context bundle for every attempt. It contains only the facts
needed to make the next decision:

- the immutable objective text and digest stored by the campaign;
- the source experiment, proposal kind, terminal state, and bounded command projection;
- verified Pueue task identity, exit status, and relevant timestamps;
- bounded and redacted log tails or detector evidence;
- a trusted failure fingerprint when one exists;
- recent accepted and rejected proposal digests;
- recent terminal experiment summaries and negative results;
- current campaign state, rolling budget projection, and operator intervention;
- bounded project-relative hints for likely metric, checkpoint, and artifact files.

The project root is available read-only to the analysis agent, so the bundle does not recursively
copy arbitrary repository contents. Descriptor-relative discovery is bounded by entry count, depth,
field size, and total serialized size. Symlinks, mount changes, special files, owner mismatches, and
paths outside the pinned project root are not followed.

The complete serialized context is capped at 128 KiB. Raw transcripts, complete logs, credentials,
environment values, and unbounded artifact contents are neither prompted nor persisted. The context
schema version and SHA-256 digest are stored with the attempt.

## 7. Analysis agent execution

The decision runner is distinct from an experiment or code-change runner. Phase 2 uses the pinned
built-in Codex executable for decisions, even when the experiment itself uses a custom executable.
It must positively verify support for the forced read-only sandbox and the resolved network mode.
Missing capabilities are a policy failure; there is no custom-agent or weaker-sandbox fallback.

- The pinned project root is read-only.
- Only the verified private run temp is writable.
- Network follows the resolved service policy and is enabled by default.
- Credential and authentication environment variables remain excluded.
- The agent cannot change context mode or choose a session ID.
- Fresh context is the default; SQLite evidence is authoritative.
- Phase 2 does not permit source edits, commits, worktrees, or direct Pueue submission.

The runner supplies the immutable objective, policy summary, evidence bundle, and exact decision
schema. Custom executables remain available for experiment execution but do not become Phase 2
decision agents.

## 8. Structured decision output

The agent writes exactly one `decision.json` under its verified private temp. The supervisor reads it
through the retained directory capability using descriptor-relative, no-follow operations. It never
reopens an ambient pathname.

The file must be a same-owner regular file with exact mode `0600`, link count one, and size at most
128 KiB. Unknown fields, trailing data, duplicate fields, control characters, invalid UTF-8, partial
writes, symlinks, hard-link identity changes, and replacement races are rejected.

### 8.1 Proposal decision

```json
{
  "schema_version": 1,
  "decision": "proposal",
  "proposal": {
    "kind": "experiment",
    "hypothesis": "Reduce the learning rate after the baseline regression.",
    "source_experiment_id": "experiment-123",
    "argv": ["python", "train.py", "--lr", "0.001"],
    "working_directory": ".",
    "expected_evidence": ["validation metric"]
  }
}
```

The proposal payload reuses `ProposalInput` with `deny_unknown_fields`. `code_change` is rejected in
Phase 2. `repair` requires the source experiment to have a trusted failure fingerprint. The existing
proposal digest, same-spec retry, repair, parallel, rolling experiment, and accepted-per-cycle limits
remain authoritative.

### 8.2 Wait decision

```json
{
  "schema_version": 1,
  "decision": "wait",
  "reason": "Artifact publication has not completed.",
  "requested_wait_minutes": 30,
  "expected_evidence": ["checkpoint metadata"]
}
```

The reason and expected evidence are bounded non-secret text. The requested duration is a request,
not authority. The supervisor validates it against a service-owned finite range and stores the
absolute `next_wake_at`. A valid wait finishes the attempt, resets the consecutive decision-failure
counter, and moves the cycle to `waiting`. When due, the same cycle returns to `pending` and receives
a freshly reconstructed evidence bundle.

## 9. Service-owned limits

Phase 2 extends `CampaignLimits` with:

- `max_decision_attempts_per_cycle`, default `3`, range `1..=10`;
- `max_decision_wait_minutes`, default `1_440`, range `1..=10_080`.

`max_decision_attempts_per_cycle` limits consecutive unsuccessful attempts. A valid proposal or wait
resets the failure counter. Every analysis agent run also consumes the existing rolling agent-run
budget. This prevents invalid-output hot loops while allowing a long campaign to make multiple valid
wait decisions.

## 10. Decision handling

### 10.1 Proposal

After parsing, the supervisor reloads the source experiment and validates `ProposalInput` against the
campaign's immutable objective digest. It generates all durable IDs and calls the existing campaign
coordinator. A proposal cycle is completed only after the proposal and experiment intent are durable.

If the coordinator enters `budget_waiting`, the decision remains durable and resumes through the
existing finite budget wake. If Pueue add becomes ambiguous, existing `submitting` and `unreconciled`
semantics apply. The decision loop never retries an unknown add.

### 10.2 Wait

A valid wait creates no proposal, experiment, submission, budget reservation, or Pueue task. The
cycle's finite wake is indexed and restored on daemon restart. Operator wake may make a waiting cycle
due earlier, but it does not override campaign state, admission locks, or rolling budgets.

### 10.3 Missing or invalid decision

Missing, malformed, unsafe, or permanently invalid output records a typed `decision_missing` or
rejection result. A new attempt receives the bounded rejection reason. When consecutive unsuccessful
attempts reach the service-owned limit, the campaign becomes `degraded`; it does not spin or silently
stop.

## 11. Error policy

- transient evidence I/O: bounded backoff without duplicating the attempt;
- agent launch failure or timeout: failed attempt and bounded retry;
- agent ownership uncertainty: no replacement run until existing lifecycle recovery resolves;
- invalid decision file: `decision_missing`;
- duplicate proposal: no new experiment; include the existing outcome in the next context;
- rolling budget exhaustion: existing `budget_waiting` state and finite wake;
- project or campaign authority change: defer without accepting output;
- ambiguous Pueue add: `unreconciled`, never automatic re-add;
- database or policy invariant failure: fail closed and move the campaign to `degraded` or `halted`;
- retry exhaustion: `degraded` with bounded operator diagnostics.

Phase 2 does not convert an agent goal claim into campaign completion. The agent must choose another
evaluation proposal or a finite wait until the later goal-review phase exists. Only a human retires a
campaign.

## 12. Restart recovery

Daemon startup performs existing agent-run and experiment-intent recovery before decision recovery.

- `pending`: reserve the next attempt when campaign authority and budget permit;
- `analyzing/reserved`: resume evidence construction idempotently;
- `analyzing/evidence_ready`: schedule the linked analysis event once;
- `analyzing/running`: defer to existing AgentRun ownership and terminal recovery;
- terminal agent with retained output: validate and persist the decision before private-temp cleanup;
- lost or unprovable output: record `decision_missing`; never infer a proposal;
- `decided/proposal`: rerun the coordinator with the persisted idempotency key;
- `waiting`: restore the indexed absolute wake;
- `completed` or `degraded`: no autonomous replay.

Crash after Pueue add remains covered by the Phase 1 external-effect state machine. Phase 2 does not
introduce a second submission implementation.

## 13. Observability

`campaign status` adds a bounded decision projection:

- active cycle ID and source experiment ID;
- cycle state and attempt count;
- last decision category;
- next wake time;
- bounded failure category and summary.

`doctor` checks:

- orphan or cross-campaign cycle lineage;
- duplicate active attempts or agent bindings;
- overdue running analysis attempts;
- waiting cycles without a finite wake;
- context and decision digest integrity;
- degraded campaigns with missing diagnostics.

Human and JSON outputs exclude raw evidence, prompt text, transcripts, credentials, environment
values, and complete command lines.

## 14. Testing

Unit and integration coverage must include:

- decision JSON round trips and rejection of unknown fields, oversize values, control characters,
  trailing data, partial files, symlinks, special files, and replacement races;
- schema 17-to-18 migration, current-schema verification, rollback, foreign keys, and indexes;
- terminal-cycle and active-attempt uniqueness under two-connection races;
- success terminal to proposal to next experiment;
- failure terminal to trusted-fingerprint repair;
- repair rejection when no trusted fingerprint exists;
- finite wait with no Pueue add and a later due attempt;
- invalid output to bounded retries to `degraded`;
- pause, disable, halt, retire, and root-replacement races;
- restart at every attempt, decision, coordinator, and Pueue add boundary;
- no duplicate agent run, proposal, experiment, or Pueue add;
- code-change rejection, default network access, and absent credentials;
- bounded status and doctor projections without secret or raw-output leakage.

Linux real-Pueue acceptance uses an adapter-free synthetic ML repository with no result manifest. One
initial submit must exercise success, failure, and wait decisions, including daemon restarts, while
proving that each source experiment produces at most one accepted next experiment.

Required completion gates are:

```bash
cargo check --all-targets
cargo check --release --all-targets
cargo test --all-targets -- --test-threads=1
bats tests/test_shell_entrypoints.bats
tests/e2e/run.sh
```

The Linux real-Pueue gate runs on `roko`. A non-Linux host cannot be used to claim that acceptance.

## 15. Documentation changes

Implementation updates must explain:

- that Phase 2 automatically creates the next non-code experiment after terminal results;
- how finite wait and `degraded` differ;
- why direct Pueue submission and source edits remain forbidden;
- which status and doctor fields diagnose the current decision cycle;
- that running health, periodic observation, goal review, and code changes remain later phases.

## 16. Acceptance criteria

Phase 2 is complete when all of the following hold:

1. An operator can initialize, enable, and submit one baseline in an arbitrary registered ML
   repository without adding a controller or mandatory manifest.
2. Terminal success and terminal failure each create one durable decision cycle.
3. A valid proposal reaches the existing coordinator and starts at most one next experiment.
4. A valid wait creates no experiment and wakes at a finite durable time.
5. Invalid decisions cannot spin indefinitely and eventually make the campaign diagnosably degraded.
6. Pause, disable, halt, retire, budgets, and restart boundaries cannot produce duplicate analysis or
   experiment tasks.
7. The analysis agent cannot write the project or inherit credentials; network remains enabled by
   default policy.
8. Linux real-Pueue acceptance and all repository verification gates pass.
