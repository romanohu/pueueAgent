# Execution Security Design

Date: 2026-08-13
Status: Design approved

## Goal and scope

Make the supervisor the owner of agent/Pueue execution security. Project
configuration may request a run and narrow selected options, but may not choose
an untrusted executable, shell, security mode, environment, writable root, or
session belonging to another project.

This slice covers daemon-startup trust anchors; immutable resolved policies;
Codex argument, network, environment, filesystem, and session rules; a native
shell-free launch gate; bounded Pueue control commands; safe log handling;
policy-aware failure classification; local diagnostics; migration; and
host-independent Cargo/Bats tests. It does not add Slack, webhooks, or any
other external notification.

## Current state and architecture

The following names are the existing implementation seams.

- `src/main.rs::commands::daemon` opens `paths::state_db_path`, constructs
  `CommandPueue::new("pueue", fixed_args)`, constructs
  `AgentRunner::new(AgentRunnerConfig::production())`, and starts `Daemon::run`.
- `src/service.rs::ServicePaths::from_environment` derives state directory,
  Pueue config, working directory, release binary, and service `PATH`.
  Generated systemd/launchd definitions pass `PATH` and state-directory
  environment but do not anchor executable identity.
- `src/config.rs` parses `ProjectConfig` with `deny_unknown_fields`. Current
  `AgentConfig` has `program`, `args`, `timeout_minutes`, `max_retries`, and
  `AgentContextMode` (`fresh`, `resume`, `resume_latest`). Existing
  `agent.program = "codex"` remains the compatibility spelling.
- `src/scheduler.rs::Scheduler::tick` claims/groups events, loads project
  config, applies guardrails, reserves interventions, builds the bounded
  prompt, and calls `AgentRunner::spawn`. It distinguishes pre-binding,
  run-bound pre-marker, and post-marker `AgentSpawnError` stages.
- `src/agent.rs::AgentRunner::command_for` builds an argv vector, but current
  Unix launch uses `/bin/sh` and `LAUNCH_GATE_SCRIPT`. The script performs a
  PATH/executable check, starts the child, creates the marker, prints
  `released`, and waits. `process_tree::configure_agent_command` calls
  `setsid` while ignoring its return value; cleanup sends TERM then KILL to a
  process group with a child-kill fallback.
- `src/codex_session.rs::verify_project_ownership` verifies explicit UUID
  sessions and project-owned metadata. Current `resume_latest` emits
  `codex exec -C <root> resume --last <prompt>` and lets Codex choose a session.
- `src/pueue.rs::CommandPueue` defaults to bare `pueue` and uses Tokio
  `.output()` without a timeout, process-group setup, or output cap.
- `src/logs.rs::LogSnapshot::read_tail` uses metadata/open/seek/read by path.
  `src/detect.rs::Detector` canonicalizes extra logs and opens them later by
  path. Agent logs use ordinary create/append opens; `check.log_tail_bytes`
  currently accepts any positive value.
- `src/models.rs`, `src/db/repositories.rs`, `src/daemon.rs`, and the event-run
  acknowledgement design already provide durable `in_flight`, `dispatched`,
  marker, finalization, restart recovery, and intervention semantics.
  `src/status.rs` and `src/diagnostics.rs` provide bounded local projections.

Preserve this durable protocol: claim ->
`insert_with_events_and_reservation` -> `in_flight` -> native marker/release ->
`acknowledge_dispatch` -> `dispatched` -> process finalizer ->
`completed`/`retry_wait`/`dead_letter`. A marker is not successful work
evidence. Active runs retain their resolved policy through finalizer retries.

## Threat model and trust boundaries

The daemon service account, its trusted binary, the service-owned state
directory, and the daemon startup environment are trusted. The service `PATH`
is trusted only while resolving startup anchors. Pueue is external; this design
constrains supervisor-launched Pueue commands but not Pueue internals.

Project config/files, instructions/state text, interventions, event payloads,
Pueue command/output text, project-root entries, executable/log symlinks, agent
descendants, and inherited environment variables are untrusted. The attacker
may write the project root/config but not service-owned policy or trusted
installations. Service-account compromise is out of scope.

## Security invariants

1. At daemon startup, bare `codex` and `pueue` resolve once from trusted
   service `PATH` to canonical absolute regular executables. Each anchor
   records Unix device/inode/owner/mode identity and resolution/symlink
   fingerprint where available. Every PATH component and resolved anchor must
   be outside all project roots and must not be group/other writable.
2. Target and resolution path are revalidated immediately before the launch
   marker. Binary/symlink replacement fails closed and requires daemon restart;
   it is never dynamically re-resolved.
3. `ResolvedExecutionPolicy` is daemon-global and immutable. Each project gets
   an immutable `ResolvedProjectExecutionPolicy`; each active run retains its
   clone, including executable identity and private temp directory.
4. Built-in Codex/Pueue are allowed by secure default. A custom agent is
   allowed only as a canonical absolute executable explicitly assigned to that
   project in the service-owned allowlist. An executable inside project root
   is forbidden.
5. Unix agent launch is direct and shell-free. `setsid` failure is fatal before
   exec. Timeout/cancel/shutdown sends TERM, waits boundedly, then KILLs the
   recorded process group.
6. Codex is fixed to `workspace-write` and `approval_policy=never`. Project
   args cannot request danger-full-access, sandbox bypass, add-dir, cwd/
   working-directory, security-setting, or network overrides. Safe model and
   reasoning args remain supported. Project-scoped `.codex` configuration,
   hooks, and MCP declarations cannot become an alternate execution-policy
   channel.
7. Outbound network is enabled by default. A project may only disable it; it
   cannot re-enable a service-disabled setting. If the installed Codex CLI
   cannot express the resolved mode, dispatch is blocked.
8. Agent/task processes use `env_clear`/default-deny. Codex may write only the
   project root and its per-run private `TMPDIR`; global `/tmp` is excluded.
9. Agent logs are owner-only regular files opened no-follow. Task/extra logs
   are opened descriptor-relative beneath project root, not from a prior
   `canonicalize` result.
10. Policy violations are nonretryable and linked events go directly to
    `dead_letter` with `policy_blocked:<code>`. Transient pre-exec OS/resource
    failures retain retry. Post-marker uncertainty always dead-letters.
11. No credential value, environment map, prompt, transcript, or raw command
    output is persisted in SQLite, run records, logs, or diagnostics.

## Policy ownership and configuration

Add a focused policy module (proposed `src/execution_policy.rs`) with these
contractual concepts:

`ExecutableAnchor { canonical_path, identity, resolution_fingerprint }`;
`ResolvedExecutionPolicy { codex_anchor, pueue_anchor, launcher_anchor,
trusted_path, startup environment held in memory, custom allowlist, fixed
Pueue/log limits }`; and `ResolvedProjectExecutionPolicy { project_id,
canonical root identity, agent anchor/kind, Codex security/network mode,
agent environment-name allowlist, task environment-name allowlist }`.

The daemon resolves the global policy before constructing `Daemon`.
`Scheduler::tick` resolves the project policy before intervention reservation;
`AgentRunner::spawn` receives it and never consults ambient PATH or a
privileged file. Active `AgentHandle` retains it.

Service installation records canonical `HOME`, `CODEX_HOME` (explicit or
derived once), trusted `PATH`, `PUEUE_AGENT_STATE_DIR`, and the Pueue config
path in the systemd/launchd definition. It never embeds API keys, tokens, proxy
credentials, or the captured environment map in the service definition.

The privileged file is `<service-state-dir>/execution-policy.toml`, where the
state directory is the parent selected by `paths::state_db_path`, outside the
project root. The directory must be service-owned and not group/other
writable. The file is a service-owned regular file with owner-only mode (0600
on Unix), opened no-follow. Writes use an owner-only same-directory temporary
file, `fsync`, atomic rename, and directory `fsync`.

Proposed schema:

```toml
version = 1
trusted_path = "/usr/local/bin:/usr/bin:/bin" # defaults to service PATH

[defaults]
network = "enabled" # enabled | disabled; defaults to enabled

[executables]
codex = "codex"
pueue = "pueue"

[projects."project-id"]
# custom_agent = "/opt/pueue-agent/bin/project-agent"
agent_environment_allow = []
task_environment_allow = ["DATASET_ROOT", "NVIDIA_VISIBLE_DEVICES"]
```

The project table is empty by default. Custom values must be canonical,
absolute, regular, executable, outside the registered project root, and
fingerprinted at startup. Environment entries are names only. Unknown
fields/versions, weak permissions, and privilege-expanding settings outside
this file are rejected. A missing file yields secure Codex+Pueue default (no
custom agents or added environment names, network enabled) and is created
atomically by `enable` or first daemon startup; a weak existing file is
rejected, not repaired.

Every `trusted_path` component must be absolute, canonical, a directory,
outside every registered project root, and not group/other writable. These
checks apply equally to built-in Codex/Pueue resolution and custom anchors.
The service never treats a project-controlled directory as trusted merely
because it appeared in inherited `PATH`.

Existing project TOML stays valid. Add only narrowing options:

```toml
[agent.execution]
network = "enabled" # enabled | disabled; default enabled
```

The project may change `enabled` to `disabled`, but cannot override a
service-level `disabled` default. Environment admission is intentionally not
present in project TOML because the agent can edit that file. Custom
`agent.program` remains parse-compatible but is `policy_blocked` until the
same project is enrolled in the service-owned project table. Bare/relative
names, stale anchors, and paths under project root are blocked.
`check.log_tail_bytes` must be 1..=1,048,576 bytes; other `check.*` and
`guardrails.*` behavior is unchanged.

## Codex and session policy

The Codex adapter uses the startup anchor and constructs final argv itself. It
sets canonical project root as cwd/context, forces workspace-write and
approval-policy never using supported installed-CLI options, applies resolved
network mode, creates private temp, and sets `TMPDIR`, `TMP`, `TEMP`. Before
adding supervisor options it rejects exact/short/`--key=value` equivalents for
danger-full-access, sandbox bypass, add-dir, cwd/`-C`, approval/security,
network, and other security-setting overrides. An explicit allowlist preserves
model/reasoning-effort and other safe behavior controls; unknown future
security flags are not implicitly safe.

The adapter also forces the project trust/config layer that skips
project-scoped `.codex` configuration, hooks, and MCP declarations. The
service-owned Codex profile is the only privileged Codex configuration source;
project `AGENTS.md` and ordinary prompt/context files remain available as
untrusted instructions. This prevents a writable project from installing an
alternate executable through Codex configuration.

Explicit `resume` continues `codex_session::verify_project_ownership`:
normalized UUID, matching metadata ID, absolute metadata cwd, and cwd beneath
canonical project root.

`resume_latest` becomes a bounded supervisor-local operation. A helper scans
local `sessions` and `archived_sessions`, verifies candidate ownership, skips
foreign or malformed candidates, and chooses the latest verified same-project
session with deterministic timestamp/ID/path tie-breaking. It passes the
explicit session ID; never emits `--last` and never silently starts fresh. No
valid same-project candidate is `policy_blocked:session_missing`; duplicate or
ownership-ambiguous metadata is `policy_blocked:session_not_owned`.

## Environment and writable roots

At startup capture allowed names and source values; values remain in memory.
Every child begins with `env_clear`.

Codex baseline is only `HOME`, fixed `CODEX_HOME`, fixed trusted `PATH`, fixed
locale, private temp variables, explicitly admitted proxy/certificate
variables, and Codex authentication variables required by the installation.
Run/project IDs may be added as non-secret metadata. The Codex adapter enforces
its shell-environment policy so Codex task subprocesses receive a smaller
minimal shell baseline plus names in the service-owned
`task_environment_allow`, with values copied from startup. Codex authentication
names are hard-denied even when listed. Store names only, never values.

Custom agents receive only their service-owned `agent_environment_allow` plus
the non-secret baseline. Their descendants may inherit that same environment;
separating a custom agent from its own descendants requires the later OS
sandbox layer. Enrolling a custom executable therefore grants an explicit
service-operator capability and is never inferred from project configuration.

Create `.pueue-agent/tmp/<run-id>` (or equivalent under project execution
area), owner-only 0700, remove after terminal finalization. It is not `/tmp`,
not shared, and no add-dir or second project root is granted.

## Native launcher and lifecycle

Replace `/bin/sh` and `LAUNCH_GATE_SCRIPT` with a private native mode in the
trusted Rust executable (for example `pueue-agent __launch-agent`), hidden from
normal help and not project-configurable. It receives anchored paths/argv but
no credential values in argv. It revalidates root, executable/symlink identity,
Codex/session ownership, secure log, environment, and temp facts, calls
`setsid` and aborts on error, opens secure logs, and waits for the exact release
byte. The executable is opened no-follow and its identity is verified on that
descriptor. The gate forks a child that is blocked on a private release pipe
before any target code can execute. It then atomically creates the mode-0600
marker and releases the child to execute the already verified descriptor with
an argv vector (`fexecve`/equivalent), never a path, shell, or PATH lookup. A
close-on-exec status pipe confirms successful exec before the gate writes
`released\n`; the gate remains group leader and waits for the target.

This deliberately strengthens the marker contract. The marker means target
execution was durably authorized and may have begun, not that useful work or
even exec success is proven. If the gate dies before marker creation, the
blocked child observes pipe closure and exits without executing, so pre-marker
retry is safe. If it dies after marker creation, recovery dead-letters
conservatively. Marker creation failure kills the still-blocked child and is a
pre-marker failure. Fork/exec/ack failure after marker is post-marker uncertain
and never retryable.

`AgentRunner::spawn` preserves binding, PID/intervention commit, marker/release
ack, `AgentRunRepository::acknowledge_dispatch`, and handle return ordering.
`AgentHandle::{poll,wait,timeout_now}` retains policy through finalization.
Marker/release is never completion evidence. Timeout/cancel/shutdown uses TERM
then bounded wait then KILL for the process group.

## Pueue and logs

Construct `CommandPueue` with startup-resolved canonical absolute Pueue path,
preserving fixed `--config` args. Before `status`, `add`, `kill`, `remove`, or
`group`, revalidate identity, use default-deny noncredential environment, and
establish a process group with mandatory successful `setsid`. Use fixed 30
second per-command timeout and 64 KiB independent stdout/stderr caps; timeout
cleanup is TERM then KILL. Continue direct argv; never build a shell command.
Pueue launch uses the same verified-descriptor execution helper, without the
agent marker/release protocol, so revalidation and process creation are not a
path-based check/use pair.
The configured Pueue YAML path must canonicalize to a service-owned regular
file outside every project root and have no group/other write bits. Validate
`pueue_group` against a bounded `[A-Za-z0-9][A-Za-z0-9._-]*` grammar before it
can reach callback registration or Pueue argv.

Enforce 1 MiB in `config::load` and `LogSnapshot::read_tail`. Agent logs use
Unix no-follow/close-on-exec, create/append, regular-file, service-owner, and
verify that no group/other permission bits are set. Symlink/device/directory/
weak files are
`policy_blocked:agent_log_unsafe`.

Task/extra paths are relative only. Open a project-root directory descriptor
and walk components descriptor-relatively with no-follow (`openat`-style) and
regular-file checks; read metadata and bytes from that same descriptor. Reject
absolute paths, `..`, symlinks, `/tmp`, and outside-root paths. Replace
`Detector::canonical_extra_log_path`; canonicalize-then-open is insufficient.

## Failure classification and data flow

| Condition | Result and retry contract |
| --- | --- |
| Missing/weak/unknown service policy or unresolved startup anchor | Daemon remains fail-closed; no event retry until fixed/restarted. |
| Unenrolled custom agent, project-root path, unsafe Codex arg, forbidden env/name/network override | Direct `dead_letter`, `policy_blocked:<code>`; no retry; release pre-check reservation. |
| Root/executable/symlink/session/log/temp change before marker | Direct policy dead-letter; never re-resolve; pre-marker intervention release. |
| `setsid` failure | Direct policy dead-letter; fatal pre-exec; do not exec. |
| Native gate spawn, secure-log/temp setup, blocked-child fork, or marker failure before release | Existing `fail_before_gate_release_with_policy` and `RetryPolicy`; target code cannot have executed. |
| Marker exists and child release/exec/ack/finalizer is uncertain | Existing post-marker finalizer; dead-letter regardless of attempts; applied intervention retained. |
| Nonzero exit, timeout, cancel, shutdown | Existing retry/dead-letter policy; TERM then KILL process group. |
| Pueue timeout/output overflow | Bounded control error, group cleanup, no implicit event transition or notification. |

Policy codes are bounded/redacted, never raw argv or secrets. A claimed event's
current attempt remains recorded for audit, but a policy failure bypasses the
retry calculation and transitions directly to `dead_letter`. Existing
executed-possibility intervention semantics remain: reserved work releases
before execution evidence; applied work is retained once execution could have
started.

Data flow is: daemon startup -> secure policy file/PATH anchors -> immutable
global policy -> project resolution -> policy block dead-letter or intervention
reservation -> in-flight binding -> native revalidation/marker -> dispatched
ack -> process finalizer. An active run never rebuilds policy from project TOML
or ambient environment.

## Diagnostics and persistence

Extend local `status`, `events`, `runs`, and `doctor` human/JSON projections
with project ID/root, resolved executable path, policy code, and stage
(`pre_binding`, `run_bound_pre_marker`, `post_marker`, `dispatched`,
`finalized`). Do not expose environment values, auth values, prompt,
transcript, or raw output.

Add read-only doctor checks for service policy ownership/mode/schema/default,
anchor identity/replacement, project-root policy resolution, policy-blocked
counts/codes, log cap/unsafe paths, Pueue bounds/process-group capability, and
run ack/policy-stage consistency. Doctor never repairs, retries, enrolls,
changes policy, or notifies externally.

SQLite is currently schema v13. Add schema v14 nullable columns to
`agent_runs`: `execution_kind`, `executable_path`, `executable_identity`,
`policy_code`, and `failure_stage`. They contain only bounded non-secret
projections. A pre-binding policy block has no run, so its code/stage remains
in the existing event `last_error`. The full immutable policy and environment
stay only in active memory, not SQLite. Existing event/run ack and dead-letter
fields remain compatible. Repository writers set the execution projection in
the same transaction that binds a run, and finalizers update only policy code
and stage as necessary.

## Migration and compatibility

- Existing Codex configs, fresh sessions, owned explicit resumes, and safe
  model/reasoning args continue to work.
- Existing `resume_latest` configs parse but use an owned explicit session ID;
  there is no `--last` or fresh fallback.
- Existing custom-agent configs become `policy_blocked` until service-owner
  enrollment; project files cannot self-enroll.
- Missing policy atomically creates secure Codex+Pueue defaults; weak files
  fail closed. Existing Pueue config path/fixed args remain usable.
- Unix Linux/macOS are supported for agent execution. Non-Unix must report
  unsupported execution and must not use the old direct-spawn fallback.
- Unknown policy fields, weak permissions, project-root executable paths,
  forbidden args, and stale identities are hard errors.

## Testing requirements

Tests are host-independent: no real Codex/Pueue, network, or host `/bin`
layout. Extend `tests/support/fake_agent.sh`, `fake_codex.sh`,
`fake_pueue.rs`, existing integration tests, and
`tests/test_shell_entrypoints.bats` with generated fixtures.

Cargo tests cover: secure default atomic creation/owner/perms/unknown fields;
trusted PATH resolution and dev/inode/mode plus symlink replacement;
custom-agent enrollment/project-root denial/direct dead-letter/no retry;
unsafe Codex args versus safe model/reasoning; workspace-write,
approval-never, network default and disable; owned `resume_latest` and no
fallback; agent/task environment allowlists and auth noninheritance with no
credential persistence; native no-shell launch, exact two-stage ack,
pre-marker revalidation, setsid failure, TERM/KILL cleanup, retry, and
shutdown; canonical Pueue timeout/cleanup/output; 1 MiB cap and secure/
descriptor-relative log reads; grouped-event/intervention semantics; and
bounded status/doctor project/path/code/stage output without notification.

Bats proves shell metacharacters remain literal, the native launcher is the
only gate, unsafe Codex options are rejected, task env excludes Codex auth,
and log/output caps hold. Required checks are `cargo fmt --all -- --check`,
`cargo test --all-targets`, `git diff --check`, and
`tests/test_shell_entrypoints.bats`.

## Out of scope and acceptance

Container/OS sandboxing, seccomp, macOS sandbox profiles, stronger
filesystem/network isolation, and fully detached-process containment are
follow-up work. Arbitrary shell strings, project-controlled privilege policy,
credential persistence, transcripts/raw output, Slack, webhooks, email, and
external delivery are excluded.

Acceptance: startup anchors Codex/Pueue immutably; replacement fails closed
until restart; Codex/Pueue defaults and existing Codex configs work; custom
agents require enrollment; Codex security/network/env/session rules hold; the
native launcher replaces `/bin/sh` with durable two-stage ack and process-group
cleanup; Pueue and logs are bounded/safe; policy violations dead-letter without
retry while transient pre-exec failures retry; post-marker uncertainty and
intervention semantics remain correct; local diagnostics expose project/path/
code/stage; and the host-independent Cargo/Bats suite passes.
