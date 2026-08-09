# pueue-agent Rust + SQLite Supervisor Design

Date: 2026-08-09
Status: Approved for specification review

## 1. Purpose

`pueue-agent` manages long-running machine-learning experiments executed by
Pueue and wakes a headless coding agent only when meaningful work is needed.
The system must support multiple projects on one Pueue daemon while keeping
normal monitoring token-free.

The redesign replaces per-project shell state coordination with one Rust
supervisor and a shared SQLite event store. The public workflow remains a
small CLI around `init`, `enable`, `submit`, `status`, `pause`, and `resume`.

## 2. Goals

- Monitor multiple projects concurrently through one supervisor per Pueue
  daemon.
- Keep ordinary health checks local and token-free.
- Start at most one coding agent per project at a time.
- Make callback and polling delivery durable and idempotent.
- Survive supervisor restarts without losing pending events.
- Enforce project isolation and globally unique Pueue groups.
- Preserve `STATE.md` as the agent-readable campaign memory.
- Keep experiment submission simple:

  ```text
  pueue-agent submit -- python train.py --lr 0.001
  ```

- Produce a deployable Rust binary suitable for a user-level systemd or
  launchd service.

## 3. Non-goals

- Replacing Pueue as the task scheduler.
- Direct API integrations for coding agents.
- Cross-machine orchestration.
- A web dashboard.
- Automatic killing of Pueue tasks as part of anomaly detection.
- Inferring experiment quality with an LLM during cheap health checks.

## 4. Architecture

One `pueue-agentd` supervisor runs for a Pueue daemon. The CLI and daemon are
subcommands of the same Rust binary.

```text
Pueue callback ───────┐
                      │
Periodic reconciliation ──> SQLite event store ──> scheduler ──> agent process
                      │                                      │
                      └──────── Pueue status/logs <───────────┘
```

### Components

#### Rust CLI

The CLI performs short-lived operations:

- `init`: create project configuration and a stable project ID.
- `enable`: register the project and ensure the supervisor integration exists.
- `disable`: unregister the project and remove its schedule.
- `submit`: resolve the current project group and invoke `pueue add`.
- `event`: record a callback or detector event and return quickly.
- `status`: query supervisor health, projects, tasks, events, and guardrails.
- `pause` / `resume`: control project dispatch without deleting events.
- `daemon`: run the supervisor loop.

#### Supervisor

The supervisor owns:

- Pueue reconciliation for all enabled projects.
- Event deduplication and incident state.
- Agent scheduling, cooldown, retry, timeout, and leases.
- Guardrail counters and halted state.
- Recovery of work left by a previous supervisor process.

#### Pueue integration

The callback is a fast event-ingestion path. It must not invoke the coding
agent or perform long analysis. The supervisor periodically queries Pueue for
the authoritative task state, so a missed callback is recoverable.

## 5. Project identity and isolation

Each initialized project receives a stable random `project_id`. The project
also has a configured Pueue group.

The central database enforces:

- `project_id` is unique.
- Canonical project root is unique.
- Pueue group is unique across registered projects. A disabled project keeps
  its group reservation until its remaining Pueue tasks are reconciled or the
  project is explicitly removed.

The default group must not be based only on the directory basename. A short
project ID suffix may be used for readability, for example
`pa-myrepo-8f31c2`.

Every event, task signature, incident, agent run, and guardrail counter is
scoped by `project_id`.

## 6. SQLite database

The database is stored under the user state directory, preferably
`$XDG_STATE_HOME/pueue-agent/state.sqlite3`, with the platform-appropriate
fallback on macOS.

SQLite is configured with WAL mode, a busy timeout, foreign keys, and explicit
transactions.

### Tables

#### `projects`

- `project_id TEXT PRIMARY KEY`
- `root_path TEXT NOT NULL UNIQUE`
- `pueue_group TEXT NOT NULL UNIQUE`
- `config_path TEXT NOT NULL`
- `enabled INTEGER NOT NULL`
- `paused INTEGER NOT NULL`
- `halted_reason TEXT`
- `created_at INTEGER NOT NULL`
- `updated_at INTEGER NOT NULL`

#### `events`

- `event_id INTEGER PRIMARY KEY`
- `project_id TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `dedup_key TEXT NOT NULL`
- `payload_json TEXT NOT NULL`
- `status TEXT NOT NULL`
- `attempts INTEGER NOT NULL DEFAULT 0`
- `not_before INTEGER NOT NULL`
- `lease_until INTEGER`
- `created_at INTEGER NOT NULL`
- `completed_at INTEGER`
- `last_error TEXT`
- `UNIQUE(project_id, dedup_key)`

Event status is one of `pending`, `claimed`, `completed`, `retry_wait`, or
`failed`.

#### `incidents`

- `incident_id INTEGER PRIMARY KEY`
- `project_id TEXT NOT NULL`
- `kind TEXT NOT NULL`
- `task_key TEXT`
- `fingerprint TEXT NOT NULL`
- `status TEXT NOT NULL`
- `first_seen_at INTEGER NOT NULL`
- `last_seen_at INTEGER NOT NULL`
- `acknowledged_at INTEGER`
- `resolved_at INTEGER`

An active incident is unique by `(project_id, kind, fingerprint)`. This is
implemented as a partial unique index over incidents whose status is `open` or
`acknowledged`, so a later occurrence after resolution can create a new
incident row.

Incident status is one of `open`, `acknowledged`, or `resolved`.

#### `agent_runs`

- `run_id INTEGER PRIMARY KEY`
- `project_id TEXT NOT NULL`
- `primary_event_id INTEGER NOT NULL`
- `pid INTEGER`
- `status TEXT NOT NULL`
- `started_at INTEGER NOT NULL`
- `finished_at INTEGER`
- `exit_code INTEGER`
- `log_path TEXT NOT NULL`
- `last_error TEXT`

#### `agent_run_events`

Associates every event coalesced into an agent run:

- `run_id INTEGER NOT NULL`
- `event_id INTEGER NOT NULL`
- `PRIMARY KEY(run_id, event_id)`

This keeps one agent run capable of handling several pending events without
losing event-level completion state.

#### `submissions`

Tracks calls to `pueue-agent submit` independently from task completion:

- `submission_id TEXT PRIMARY KEY`
- `project_id TEXT NOT NULL`
- `argv_json TEXT NOT NULL`
- `created_at INTEGER NOT NULL`
- `pueue_task_id INTEGER`
- `task_signature TEXT`
- `status TEXT NOT NULL`

The submit path records an intent before invoking Pueue and attaches the task
ID after a successful `pueue add`. If the process exits between those steps,
reconciliation attempts to adopt the matching task by project group,
command, and creation window. Ambiguous matches remain visible as an
unreconciled submission instead of being silently counted.

#### `task_observations`

Stores the latest authoritative Pueue observation for each task signature,
including task ID, group, enqueue/start/end timestamps, result, and current
state. The task signature must include more than the numeric Pueue task ID so
IDs reused after a Pueue state reset do not collide.

## 7. Event and scheduling model

### Event ingestion

The callback records a lightweight event containing project group, task ID,
and callback metadata. It returns without starting an agent.

The reconciliation loop queries Pueue once per interval for all enabled
groups, updates `task_observations`, and materializes authoritative events for
task completion, failure, and disappearance.

### Event keys

Event keys are stable and idempotent:

- task completion/failure: task signature plus result
- running-task anomaly: project, task signature, detector, and log snapshot
- extra log anomaly: project, path, detector, and content fingerprint
- deep check: project and scheduled time bucket

The same observation must not create a second pending event while its incident
is already open or acknowledged.

### Claiming

The scheduler claims events in a short SQLite transaction. A claim records a
lease expiration. Agent execution happens outside the transaction. Completion
or retry is written in a second transaction.

On supervisor restart, expired claims return to `pending`.

### Coalescing

Multiple events for one project may be combined into one agent run. Urgent
events have priority in this order:

1. task failure or crash
2. stalled task
3. task completion
4. deep check

The prompt contains all claimed event contexts, not just one task ID.

## 8. Anomaly detection

The cheap detector only reads Pueue status and bounded log tails.

- A matching error pattern opens or updates one incident.
- A stalled detector uses a task snapshot containing at least byte size and
  modification time. It must not repeatedly open the same incident while the
  snapshot is unchanged.
- A changed log snapshot or a new task signature can create a new incident.
- `extra_log_paths` use the same fingerprinting and incident lifecycle as task
  logs.

Detection does not kill or modify a Pueue task. The coding agent may decide
what intervention is appropriate.

## 9. Agent execution

The agent command is represented as an executable plus argument vector, not a
shell command string. `{prompt}` is replaced inside one argument before
`std::process::Command` is invoked.

Shell pipelines require an explicit user-owned wrapper script.

Each run records stdout/stderr in a project log path and records its PID,
exit status, timeout, and associated event IDs. The project lease prevents a
second agent run while one is active.

An agent is instructed to:

1. read `instructions.md` and `STATE.md`;
2. inspect the referenced Pueue tasks and repository;
3. make only permitted changes;
4. submit new experiments through `pueue-agent submit`;
5. update `STATE.md` before exiting.

The supervisor treats a zero exit code as successful agent execution, not as
proof that a new experiment was submitted. Task submission is separately
observed and counted.

## 10. Guardrails and token control

Guardrails are stored per project and enforced before an agent is claimed.

- `max_consecutive_failures`: counts intervention events without an intervening
  successful task completion.
- `max_experiments`: counts tasks accepted or started for the project, not only
  successful completions.
- `max_agent_runs`: optional cap on agent invocations.
- cooldown after agent failure or repeated incidents.
- one active agent per project.
- optional global agent concurrency limit.

When a guardrail halts a project, events remain in SQLite. `resume` clears the
halted state and re-evaluates pending events rather than losing them.

Normal health checks never invoke an agent. Deep checks are time- or count-
scheduled and are suppressed when an urgent event is pending or an agent is
already active.

## 11. Configuration and user experience

Project configuration moves to a real structured format, preferably TOML for
the initial Rust implementation. The project still contains:

```text
.pueue-agent/
  config.toml
  STATE.md
  instructions.md
  logs/
```

Typical commands remain:

```text
pueue-agent init --agent codex
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
pueue-agent status
pueue-agent pause
pueue-agent resume
```

`status` shows supervisor health, project state, active tasks, pending events,
open incidents, active agent runs, and guardrail counters.

The supervisor is normally silent. Human-visible output is reserved for
agent starts, incidents, guardrail halts, and recovery failures.

## 12. Service and environment integration

The supervisor must run as a user service, not depend on the interactive shell
environment. The service configuration must explicitly provide the Pueue
configuration path, PATH, project state directory, and agent executable
environment.

`enable` must verify the service is reachable before reporting success.
Callback installation and project registration must be recoverable and must
not report a fully enabled state after a partial failure.

## 13. Failure handling

- A missed callback is recovered by reconciliation.
- A malformed or unavailable Pueue response is logged as an integration error,
  not interpreted as an idle project.
- A busy SQLite database is retried with bounded backoff.
- A crashed supervisor leaves claimed events recoverable through leases.
- A missing project path disables dispatch for that project and reports a
  visible error.
- Agent timeout and non-zero exit use retry backoff; repeated failure halts the
  project and preserves the event context.

## 14. Security and trust boundary

The tool is intended for trusted repositories, but the boundary must be
explicit. Agent processes can read repository files and may edit code or
submit Pueue tasks.

The default process launcher must avoid shell interpretation. Any shell wrapper
must be explicitly configured. Logs and `STATE.md` are agent inputs and should
be treated as potentially untrusted text; they are not enforcement mechanisms.

## 15. Migration plan

1. Add the Rust workspace, CLI skeleton, SQLite migrations, and project
   registration.
2. Add `submit`, `status`, and event ingestion while retaining Bash callback
   and sentinel wrappers.
3. Add reconciliation, incident deduplication, and scheduler leases.
4. Move agent execution and guardrails into the supervisor.
5. Add systemd/launchd service installation and health checks.
6. Remove the Bash registry, PID lock, hand-written YAML parser, and per-project
   cron scheduling.
7. Keep `STATE.md`, `instructions.md`, and the visible CLI workflow stable.

## 16. Verification strategy

### Unit tests

- SQLite migrations and uniqueness constraints.
- Event insertion idempotency.
- Concurrent event claims.
- Lease expiry and recovery.
- Incident fingerprinting and resolution.
- Guardrail counters and pause/resume behavior.
- Command argument construction without shell interpolation.

### Integration tests

- Multiple projects with similar directory names.
- Concurrent callback and reconciliation for the same task.
- Pueue daemon restart and missed callback.
- Supervisor restart during an active agent run.
- Repeated `NaN` and stalled observations.
- Agent timeout, retry, and halt.
- Successful `submit` followed by a supervisor crash before task metadata is
  persisted.

### Acceptance criteria

- Two projects with the same basename never share a Pueue group or event.
- The same Pueue completion produces at most one agent run.
- Repeated identical anomaly observations produce at most one incident and do
  not repeatedly consume tokens.
- Restarting the supervisor does not lose pending events.
- Normal monitoring produces zero agent invocations.
- `pueue-agent submit -- <command...>` submits through the project group and
  records the task for guardrail accounting.
