# Instructions for the experiment agent

You manage experiments supervised by `pueue-agent`. The launch prompt contains a
bounded event summary and references to durable project context.

## Required workflow

1. Read `.pueue-agent/instructions.md`, then `.pueue-agent/STATE.md`.
2. Inspect only the tasks, logs, metrics, and artifacts associated with this project.
3. Record the diagnosis, experiment result, artifact paths, and next plan in
   `.pueue-agent/STATE.md` before exiting.
4. If the repository uses Git, commit intentional source changes with a message that
   explains what changed and why.
5. Submit every supervised experiment with:

   ```bash
   pueue-agent submit -- <command...>
   ```

   Do not call raw `pueue add`; it bypasses SQLite submission accounting.

## Dispatch modes

- `crash`, `failure`, or `stalled`: inspect the bounded evidence and relevant logs,
  determine the cause, make the smallest justified correction, update `STATE.md`, and
  submit a replacement experiment only when the configured constraints allow it.
- `deep_check`: inspect metrics and artifacts for meaningful progress. If healthy,
  append a concise health record to `STATE.md`. If unhealthy, handle it like a crash.
- `completion`: summarize the result and decide whether the next experiment is
  justified. Stop when the goal is reached or evidence indicates no useful next step.

## Context and safety

- The supervisor may start a fresh Codex session or explicitly resume an existing one,
  according to `.pueue-agent/config.toml`. Do not change the context mode or session ID.
- `STATE.md` is the durable context shared across fresh and resumed agent runs. Do not
  assume a conversation transcript exists in SQLite.
- A detector with explicit `action = "kill"` may already have asked Pueue to stop the
  failed task. Check current Pueue state before proposing or submitting a replacement.
- Do not change Pueue groups, interfere with another group, bypass `pueue-agent submit`,
  or modify `.pueue-agent/config.toml`.
- Do not exceed the goals, constraints, or guardrails recorded in `STATE.md`.
