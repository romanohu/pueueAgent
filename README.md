# pueue-agent

`pueue-agent` is a Rust and SQLite supervisor for long-running experiments managed by
[Pueue](https://github.com/Nukesor/pueue). It keeps normal monitoring token-free,
starts a coding agent only for durable events, and can optionally ask Pueue to stop a
task after a configured fatal condition is confirmed.

One supervisor serves one Pueue daemon or profile. Projects remain isolated by a
generated `project_id` and a dedicated Pueue group, so repositories with the same
directory name do not share events or task ownership.

## How it works

```text
human or agent
  └─ pueue-agent submit -- <command...>
       └─ pueue add -g <project-group> --escape -- <command...>

pueued
  ├─ callback ───────────────┐
  └─ status reconciliation ──┼─> SQLite events/incidents/submissions
                             │
Rust supervisor              │
  ├─ bounded log detection ──┤
  ├─ optional pueue kill ────┤
  └─ leased event scheduler ─┴─> at most one agent per project
```

SQLite is the source of truth for project registration, callbacks, reconciled tasks,
incidents, termination requests, event leases, and agent runs. A missed callback is
recovered from Pueue status, and an expired event lease is made pending again after a
restart.

## Requirements and installation

- Rust stable with Cargo
- `pueue` and `pueued`
- systemd user services on Linux, or launchd on macOS

```bash
git clone <repo>
cd pueueAgent
./install.sh
```

The installer builds `target/release/pueue-agent` with the locked dependency set and
creates `~/.local/bin/pueue-agent` as a symlink to that binary. Set
`PA_INSTALL_PREFIX` to choose another bin directory.

For repository development, build once and use the launcher in `bin/`:

```bash
cargo build
bin/pueue-agent --help
```

The development launcher fails with a build command if `target/debug/pueue-agent`
does not exist.

## Quick start

```bash
cd your-ml-repo
pueue-agent init
$EDITOR .pueue-agent/STATE.md
$EDITOR .pueue-agent/config.toml
pueue-agent enable
pueue-agent submit -- python train.py --lr 0.001
```

`submit` records intent in SQLite before it runs `pueue add`. It preserves argument
boundaries and is the supported submission path for humans and agents. Do not use raw
`pueue add` for supervised experiments, because that bypasses submission accounting.

Common operator commands:

```bash
pueue-agent status
pueue-agent pause
pueue-agent resume
pueue-agent disable
pueue-agent disable --remove   # explicitly release the registration/group reservation
```

`pause` preserves pending events while blocking new agent starts and automatic
termination. `resume` makes preserved events eligible again. Plain `disable` keeps the
Pueue group reserved; `--remove` is an explicit registration removal and refuses a
Pueue status error.

## Project files and shared experiment context

`pueue-agent init` creates:

```text
.pueue-agent/
  config.toml
  STATE.md
  instructions.md
  logs/
```

`STATE.md` is the durable experiment notebook shared across agent runs. Put the goal,
constraints, experiment history, findings, artifact paths, and next plan there.
`instructions.md` defines the agent workflow. The supervisor adds a bounded summary of
the triggering events and references to both files; it does not copy full conversation
transcripts into SQLite.

## Configuration

Configuration is TOML at `.pueue-agent/config.toml`. The generated template is
[`templates/config.toml`](templates/config.toml).

| Key | Purpose |
| --- | --- |
| `project_id` | Stable generated project identity. |
| `pueue_group` | Dedicated Pueue group, derived from the project name and ID suffix. |
| `agent.program` / `agent.args` | Executable and argument vector; `{prompt}` is replaced inside each argument. |
| `agent.timeout_minutes` | Agent process timeout. The full agent process group is cleaned up. |
| `agent.max_retries` | Retry limit for launch failures. |
| `agent.context.mode` | `fresh`, `resume`, or `resume_latest`; default is `fresh`. |
| `check.interval_minutes` | Supervisor reconciliation interval. |
| `check.log_tail_bytes` | Maximum bytes read from each monitored log tail. |
| `check.extra_log_paths` | Additional project-relative log files to inspect. |
| `check.patterns` | Named regex, confirmation count, and `notify`/`wake`/`kill` action. |
| `check.stall` | Stalled-output action; default is `notify`. |
| `guardrails.*` | Consecutive failure, experiment, and agent-run limits. |

Unknown keys and invalid ranges are rejected rather than ignored.

### Opt-in Codex conversation continuation

Fresh context is the default:

```toml
[agent]
program = "codex"
args = ["exec", "{prompt}"]

[agent.context]
mode = "fresh"
```

To continue a specific existing local Codex session:

```toml
[agent.context]
mode = "resume"
session_id = "019..."
```

This maps to `codex exec -C <project-root> resume <session-id> <prompt>`. To opt into
the latest project-scoped session instead:

```toml
[agent.context]
mode = "resume_latest"
```

This maps to `codex exec -C <project-root> resume --last <prompt>`. Continuation modes
are accepted only with `agent.program = "codex"`. A missing or invalid requested
session is recorded as an agent-run failure; the supervisor never silently falls back
to a fresh session.

## Anomaly detection and automatic termination

Patterns are confirmed against bounded log tails. `notify` records the incident,
`wake` records it for agent intervention, and `kill` creates an idempotent termination
request:

```toml
[[check.patterns]]
name = "fatal-loss"
regex = "NaN loss persisted|FATAL_LOSS"
action = "kill"
confirm_matches = 2
```

Automatic termination is opt-in. Before invoking `pueue kill <task-id>`, the
supervisor fetches fresh Pueue status and revalidates the project group and full task
signature. It never sends an OS signal to the experiment task. Repeated identical
observations keep one active incident and one termination request; kill failures and
timeouts remain visible and do not trigger a second agent automatically.

By default, task-scoped logs are read from `.pueue-agent/logs/<task-id>.log` or
`.pueue-agent/logs/task_<task-id>.log`. Use `check.extra_log_paths` for stable
project-relative training logs.

## Service and state locations

`enable` registers the project, creates its Pueue group, installs one daemon-scoped
Pueue callback, installs the user service, and verifies service health. Service files
contain an explicit binary path, Pueue config path, `PATH`, state directory, and
working directory; they do not depend on interactive shell startup files.

The SQLite database uses `XDG_STATE_HOME/pueue-agent/state.sqlite3` when
`XDG_STATE_HOME` is absolute. Otherwise it uses the platform state directory. Set
`PUEUE_AGENT_STATE_DIR` for the service state directory.

## Migrating from the Bash/YAML version

The Rust supervisor does not import the old global text registry, PID locks, cron
entries, or `.pueue-agent/config.yml` automatically.

The obsolete Bash supervisor and its cron/sentinel test suite were removed after the
Rust E2E scenario reached parity. `bin/pueue-agent` is now only a development launcher
for the Rust binary; it is not a second supervisor implementation.

1. With the old version, disable each project or remove its `pueue-agent sentinel`
   cron entry.
2. Install the Rust release with `./install.sh`.
3. Run `pueue-agent init` in each existing project. Existing `STATE.md` and
   `instructions.md` are preserved; a new `config.toml` is created.
4. Translate the old agent command into `agent.program` plus `agent.args`, and review
   detector actions. `kill` remains opt-in.
5. Run `pueue-agent enable` for every project and check `pueue-agent status`.

Keep a backup of the old YAML and registry until the projects appear correctly in the
SQLite-backed status output.
