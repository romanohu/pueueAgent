# Final Security Review Fixes Design

Date: 2026-08-15
Status: Approved direction; implementation pending

## Goal and release boundary

Close the actionable findings from the whole-branch security review without
weakening the execution-policy, native-launch, private-temp, or bounded-Pueue
contracts.

This slice fixes seven Important findings and three small fail-closed gaps:

- recover and pin one authoritative Pueue profile for every production
  command;
- reject lexical symlinks in the Pueue config input while retaining a useful
  degraded doctor report when the config is unavailable;
- remove raw Pueue group text from the shell callback template;
- apply one absolute 30-second Pueue operation deadline across launch,
  release, exec proof, acknowledgement, wait, and output collection;
- reject commands whose final native argv would exceed the protocol limit
  before persisting a submission intent;
- pass the private run directory to the native target as a fixed verified
  descriptor and use only the descriptor-backed target reference for
  `TMPDIR` and Codex writable-root configuration;
- reject non-object Pueue group JSON, report all Pueue anchors in doctor, and
  remove failed policy-publication temporaries through their parent
  descriptor.

The review also found that a descendant can leave a Unix process group with
`setsid` or `setpgid`. A process group is therefore not non-escapable
containment. That issue is intentionally **not** declared fixed here. The
branch remains unready for integration after this slice until a separate
containment design is approved and implemented. We do not disable macOS,
weaken cleanup checks, or silently redefine enrolled targets as trusted in
order to make this review pass.

## Considered approaches

### 1. Patch all findings while continuing to treat PGID drain as containment

Rejected. It would retain current macOS behavior, but the security claim would
still be false for a target or descendant that creates a new session or
process group. Terminal persistence and private-temp reclamation could race a
surviving process.

### 2. Disable agent execution wherever a non-escapable backend is unavailable

Rejected for this slice. Linux cgroup v2 is not universally delegated to an
unprivileged user, and macOS has no equivalent public, unprivileged CLI API
that can simply replace the current process-group protocol. Applying this
choice now would make the supported macOS deployment fail closed for every
agent run without delivering an approved replacement architecture.

### 3. Close the independent findings, keep the branch unintegrated, and
design containment separately

Selected. The independent fixes reduce real attack and reliability surface,
are testable under the current architecture, and do not pretend to solve the
remaining kernel-level ownership problem. A follow-up design must choose and
prove a supported containment backend before this branch can merge or ship.

## Authoritative Pueue profile resolution

Every command that constructs `ServicePaths`, loads execution policy, or
constructs `CommandPueue` uses one resolver with this precedence:

1. explicit `--pueue-config`;
2. explicit `PUEUE_CONFIG` captured from the process environment;
3. the config path pinned in the installed systemd/launchd definition;
4. the platform default under the verified `HOME`.

Submit and submit-batch gain no duplicate resolution logic. They call the
same resolver even though they do not expose a new CLI flag. Status, doctor,
operator, daemon, enable, disable, and upgrade use the same function. An
explicit value that is invalid never falls back to a lower-precedence source.
The selected value is compared with the already-loaded policy anchor; a
different profile fails closed rather than talking to a second Pueue daemon.

The installed service definition remains the durable profile discovery
source. No credential, environment map, or config content is copied into the
execution-policy TOML or SQLite.

## Lexical Pueue config and degraded doctor behavior

`ServicePaths` preserves the selected absolute lexical config path until the
policy loader opens it component by component with `O_NOFOLLOW`. It no longer
canonicalizes the caller's path before that walk. The input must be absolute,
must contain only root plus normal components, and must equal the canonical
path obtained for the opened generation. A symlink in any component, a
project-contained config, weak metadata, or an identity change fails closed.
After successful anchoring, all consumers use the anchor's canonical path.

Doctor is the only degraded path. It may retain a bounded lexical path for a
missing config so that it can emit structured `pueue.config`, policy, callback,
and Pueue-health failures. It does not create, repair, canonicalize, or open a
replacement config and does not construct a Pueue adapter without a valid
policy.

## Shell-free callback data flow

The installed Pueue callback command contains only the verified launcher and
the numeric `{{ id }}` placeholder. It never interpolates `{{ group }}` into a
shell command.

`pueue-agent event callback --task-id <id>` resolves the current task through
the configured, policy-backed Pueue adapter, obtains its group from typed
status JSON, validates the group with the existing closed grammar, and only
then records the callback event. A missing, ambiguous, malformed, or
unregistered task fails with a bounded error and creates no event. The
existing explicit `--group` CLI form may remain for direct operator/testing
use, but the installed callback never uses it.

Task IDs are parsed as nonnegative integers before any lookup. Raw Pueue
output, group text, and config values are not included in public errors.

## One Pueue operation deadline

`PueueProcessRunner` creates one absolute deadline immediately before the
final config revalidation and native launch. The same deadline is threaded
through:

- helper bootstrap and target readiness;
- release-gate write;
- exec proof;
- exact release acknowledgement;
- stdout/stderr collection and leader wait.

Each operation consumes only its remaining time. A deadline exhausted in any
phase maps to the same typed `PueueError::Timeout { operation }`. Cleanup still
uses its independent bounded safety grace so expiry cannot abandon an owned
process group; diagnostics describe 30 seconds as the operation deadline, not
as a per-phase timeout.

Existing agent launch keeps its phase-specific lifecycle deadlines. The new
absolute-deadline API is explicit and used by the Pueue adapter only, avoiding
a silent semantic change to the durable agent marker protocol.

## Pre-persistence native argv validation

The Pueue adapter exposes a pure validator that computes the exact final argv
count and encoded byte budget for `add`: verified executable display name,
fixed `--config /dev/fd/9`, operation name, group flags, separator/escape
flags, and user command.

Submit and batch paths call this validator before inserting submission intent
or mutating batch state. `CommandPueue::add` calls the same validator
defensively before launch. There is one source of truth for protocol limits;
tests pin the maximum accepted boundary and the first rejected boundary.
Oversize input returns bounded validation and leaves SQLite unchanged.

## Descriptor-bound private temp in the target ABI

`PrivateRunTemp` exposes only a crate-private verified capability containing a
cloned directory descriptor and its recorded identity. Native launch assigns
it one fixed target descriptor slot, distinct from project root, log, and
Pueue config slots. The control frame declares the role and expected identity;
the helper verifies the received descriptor and revalidates it immediately
before target creation.

Only the target slot is inherited across exec. Agent environment generation
sets `TMPDIR`, `TMP`, and `TEMP` to the fixed `/dev/fd/<slot>` reference.
`CodexArgvBuilder` uses that same fixed reference for the sole extra writable
root. It no longer accepts an arbitrary private-temp pathname from the agent
runner.

The supervisor keeps the original `PrivateRunTemp` descriptor for terminal
cleanup. Replacing `.pueue-agent/tmp/<run-id>` after spawn cannot redirect
target writes: the target and cleanup both refer to the original opened
generation. A deterministic integration test swaps the pathname between
blocked spawn and release, then proves the target writes only through the old
descriptor and release remains fail-closed if descriptor identity changes.

This closes pathname-generation substitution. It does not prove that every
descendant was contained; reclamation remains subject to the separate
containment blocker described above.

## Small fail-closed corrections

- Pueue group-list JSON is deserialized as an object/map. Arrays, strings,
  numbers, booleans, and null return `InvalidGroupJson`; they never cause a
  speculative `group add`.
- Doctor's `execution.anchors` check covers Codex, launcher, Pueue executable,
  and Pueue config identity.
- Failed policy publication unlinks the random temporary basename with
  `unlinkat` through the already-opened state-directory descriptor, followed
  by directory sync where mutation occurred. It never calls cwd-relative
  `remove_file`.

## Error handling and durable-state rules

- Config discovery, lexical-path, callback lookup, argv validation, and
  deadline failures are bounded and redact raw values.
- No new policy failure is persisted for an operator/Pueue control error.
- Submission validation happens before DB insertion.
- Callback lookup failure creates no event.
- Timeout and output failures retain the existing TERM/KILL/reap ordering;
  reader errors cannot replace a primary timeout/output-limit diagnosis.
- Private-temp descriptor failure is pre-release for a blocked target; after a
  durable marker it retains the existing conservative post-marker
  classification.

## Test strategy

Tests are written and observed RED before production changes. Required cases:

1. enable with a custom Pueue profile, then submit, submit-batch, status, and
   doctor without repeating the flag; all use the installed canonical profile;
2. explicit conflicting config and symlink-component config fail before DB,
   Pueue, callback, or service mutation;
3. missing config still produces a read-only doctor report;
4. a quote-bearing external group cannot appear in the callback command, and
   task-ID lookup validates the returned group before event insertion;
5. helper readiness/exec/ack delays share one absolute test deadline and do
   not accumulate phase budgets;
6. the maximum final Pueue argv is accepted and max-plus-one is rejected
   before any submission/batch row is inserted;
7. private-temp pathname replacement cannot redirect target writes, and FD
   role/identity/trailing-protocol tests remain exact;
8. non-object group JSON is rejected, all four anchors are reported, and a
   forced policy-publication failure removes only the descriptor-relative
   temporary;
9. `cargo check --all-targets`, `cargo test --all-targets --no-run`, shell
   syntax checks, static no-bare-Pueue checks, and `git diff --check` pass.

Runtime tests that cannot enter Rust because of the documented macOS dyld host
condition are reported as unavailable, never as GREEN. No credential or
runtime-state artifact is committed.

## Deferred containment acceptance gate

Before integration, a separate approved design must demonstrate all of the
following on every supported execution platform:

1. a target and arbitrary descendant cannot escape the supervisor-owned
   containment with `setsid`, `setpgid`, double-fork, or reparenting;
2. timeout and normal completion can prove the containment empty after KILL;
3. uncertainty retains authority and prevents terminal persistence and temp
   reclamation;
4. the mechanism is available to the unprivileged service account without an
   undocumented weaker fallback;
5. real regressions cover deliberate daemonization on Linux and macOS.

Until that gate is met, this branch may accumulate reviewed commits but must
not be merged, pushed for release, or described as production-ready.
