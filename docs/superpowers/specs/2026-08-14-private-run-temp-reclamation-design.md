# Private Run Temp Reclamation Design

## Context and goal

`PrivateRunTemp` currently creates an exclusive owner-only
`.pueue-agent/tmp/<run-id>` directory and retains descriptors for the fixed
parent and the exact run-directory generation. Cleanup deliberately returns
`TempUnsafe`; `Drop` never mutates the filesystem. That closed a pathname
replacement race, but successful runs can retain unbounded temporary data.

Add a service-owned reclamation boundary that reclaims the contents of the
exact descriptor-owned generation after the agent process group is completely
drained and the terminal database transition is durable. A cleanup failure
must retain authority for retry and must prevent another agent for that project
from being admitted. The implementation must not follow symlinks, reopen the
run path ambiently, expose credentials or runtime contents, or weaken daemon
shutdown deadlines.

This is a soft supervisor quota and bounded reclamation mechanism, not an OS
filesystem quota. Hard prevention of a running process exceeding a byte limit
would require a separate filesystem/container quota facility and is outside
this slice.

## Considered approaches

### 1. Descriptor-owned content reclamation, retaining the run directory

This is the selected approach. The supervisor walks from the retained run
directory descriptor, audits a bounded tree without following symlinks, and
then removes children bottom-up. It never removes or renames the top-level
`<run-id>` entry. A pathname replacement therefore cannot cause cleanup to
switch to a later run-directory generation.

The trade-off is that empty `0700` run directories remain as generation
records. They do not count toward the retained byte quota, but enumeration is
still bounded and a project fails closed if the generation-entry bound is
exceeded. Offline/operator compaction of empty generations is a separate
maintenance feature.

### 2. Rename the run directory to an unpredictable quarantine name

Rejected. Linux and macOS do not expose a portable atomic operation meaning
"rename this directory entry only if it still names this inode." A same-UID
parent mutation between identity inspection and rename can move the wrong
generation. Random destination names and no-replace rename do not close the
source-side race.

### 3. Keep all temporary trees and rely on manual cleanup

Rejected as the steady-state design. It preserves the security boundary but
allows ordinary successful runs to consume disk without bound and eventually
prevents the research loop from making progress.

## Security and lifecycle boundary

Content mutation is authorized only when all of the following are true:

1. The `NativeAgentChild` process group has been terminated or observed
   terminal, all descendants have received the checked TERM/KILL drain, and
   the owned leader has been reaped. Unknown process-observation or signal
   errors retain process-group ownership and prohibit temp cleanup.
2. The terminal `agent_runs`/event/intervention transaction has committed.
   A database finalizer failure retains the existing `AgentHandle` and does
   not begin cleanup.
3. The retained run-directory descriptor still has the original device/inode,
   owner, type, and `0700` mode. Cleanup does not require the original
   `<run-id>` pathname to resolve to that descriptor and never opens that path.
4. No daemon-controlled agent process remains able to mutate the tree. If a
   tree entry changes identity or type between audit and removal, cleanup
   aborts and retains the handle. The supervisor does not claim protection
   against an unrelated malicious same-UID process racing every individual
   `unlinkat`; such a process is outside the daemon-controlled process-group
   boundary. Parent/run-generation replacement remains protected by the
   retained descriptors and top-level no-removal rule.

`Drop` remains non-mutating. Every deletion is explicit, result-bearing, and
performed by the daemon while it still owns the cleanup handle.

## Descriptor traversal and limits

The cleanup API supports Linux and macOS and fails closed as
`UnsupportedPlatform` elsewhere. Linux cleanup and inventory require kernel
5.8 or newer: the implementation requires `openat2(RESOLVE_NO_XDEV)` (kernel
5.6) and `statx` mount IDs (kernel 5.8), and reports `UnsupportedPlatform`
without a weaker fallback when either exact capability is unavailable. It uses
only descriptor-relative operations:

- duplicate a directory FD and enumerate it through `fdopendir`/`readdir`;
- skip only `.` and `..`, reject invalid/NUL names, and never create a path;
- inspect entries with `fstatat(..., AT_SYMLINK_NOFOLLOW)` plus an exact mount
  identity (`statx` mount ID on Linux, no-follow FSID on macOS);
- on Linux, open child directories directly with
  `openat2(O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC, RESOLVE_NO_XDEV)`; on macOS,
  compare the no-follow entry FSID before descriptor open and recheck the
  descriptor FSID after open; then verify owner, `0700` mode, type, and
  device/inode;
- treat regular files, symlinks, sockets, FIFOs, and device entries as leaves;
  no leaf is opened or read;
- revalidate mount identity, entry identity, and type immediately before
  `unlinkat`; unlink symlinks as links and never follow their targets;
- remove audited children bottom-up and `fsync` each modified directory;
- never call `unlinkat(..., AT_REMOVEDIR)` for the retained run root.

Cleanup has an audit phase before the first mutation. The audit fails without
deleting anything when any bound is exceeded. Fixed limits are:

- maximum nesting below the run root: 32 directories;
- maximum visited entries per run tree: 4096;
- maximum allocated bytes per run tree: 1 GiB, computed with checked
  `st_blocks * 512` accumulation;
- maximum run-generation entries examined during admission/startup inventory:
  4096.

After mutation starts, an I/O or identity failure may leave a partially cleaned
tree. The retained descriptor and cleanup state remain retryable; a retry
re-audits the remaining tree and is idempotent. Counts, paths, names, and file
contents are never persisted or included in public errors. Diagnostics expose
only a bounded typed reason such as `entry_limit`, `depth_limit`, `byte_limit`,
`identity_changed`, or `io_failure`.

## Ownership and daemon data flow

`PrivateRunTemp` gains an explicit bounded content-cleanup operation and a
small typed report. `RetainedLaunchAuthority` is not replaced by `Released`
immediately after terminal persistence. Instead, `AgentHandle` records that
the terminal transaction is durable and enters a cleanup-only state:

1. poll/wait/timeout obtains and caches the process outcome;
2. the existing finalizer transaction commits exactly once/idempotently;
3. the same handle calls descriptor-owned temp cleanup;
4. success releases global policy, project policy, execution projection, and
   temp descriptors, and reports the agent finished;
5. failure keeps the terminal-persisted handle for another bounded cleanup
   attempt without repeating process signals or changing terminal DB state.

The daemon attempts every retained cleanup owner fairly once per poll pass and
keeps the first error only for bounded diagnostics. A cleanup-pending handle's
project ID is supplied to scheduler admission, so other projects continue but
the affected project cannot bind another agent. The scheduler releases any
already claimed event for a cleanup-blocked project without consuming an
attempt or reserving an intervention.

Shutdown passes the existing absolute deadline into cleanup. It does not alter
the one-second process-group TERM grace or the global shutdown deadline. If the
deadline expires before cleanup completes, the handle/tree remain retained and
shutdown reports an unresolved cleanup instead of claiming success.

## Restart and quota behavior

In-memory retained cleanup ownership is strongest because it includes the
original run FD and proof that the child group was reaped. A daemon crash loses
that proof. Startup therefore never automatically deletes a crash-retained
tree.

Before admitting a project, the runner descriptor-walks the fixed
`.pueue-agent/tmp` directory through the verified project-root capability. It
performs a bounded, no-follow inventory of numeric run-generation directories:

- empty valid `0700` generations are allowed and contribute zero retained
  bytes;
- a nonempty generation without a live in-memory cleanup owner is conservative
  crash-retained evidence and blocks that project's agent admission with
  `TempUnsafe`;
- an invalid entry, symlink, weak directory, inventory overflow, or byte/entry
  quota overflow also blocks admission;
- inventory is observational only and never removes a startup entry.

This makes crash behavior fail closed and bounds future disk growth: a project
with unresolved retained data cannot start another writer. Automated recovery
of crash-retained data would require a durable, PID-reuse-safe proof that the
old process group is gone; that is not available portably on both Linux and
macOS and is outside this slice.

## Error and persistence semantics

- Temp cleanup is post-terminal housekeeping, not an execution failure. It
  must not rewrite a Completed/Failed/TimedOut run or retry/dead-letter events
  a second time.
- Pre-admission inventory failure is a typed `TempUnsafe/PreBinding` policy
  block and follows the existing direct dead-letter/no-run path.
- Cleanup errors contain no raw paths, entry names, byte contents, prompts,
  argv, environment, or credentials.
- No new SQLite column stores filesystem contents or policy snapshots. Cleanup
  ownership is memory-only; crash-retained trees are rediscovered through the
  verified descriptor inventory.
- Empty top-level run directories are intentionally retained. The status and
  doctor projections may report bounded counts/reasons in later diagnostics
  work, but this task adds no broad diagnostics surface.

## Verification

Tests use real directories and generated native targets, not shell cleanup.
They must cover:

- audit RED/GREEN at exact depth, entry, byte, and generation bounds;
- regular files, nested directories, symlinks, FIFO/socket/device-style leaf
  handling without following or reading targets;
- replacement of the visible `<run-id>` path never affects the retained
  generation or replacement generation;
- entry identity/type change aborts cleanup and leaves authority retryable;
- no mutation before process-group reap or before terminal DB persistence;
- DB finalizer retry remains on the same handle and cleanup happens once after
  commit;
- cleanup failure blocks only that project while another project dispatches;
- shutdown deadline retains unresolved cleanup without changing production
  process deadlines;
- restart inventory allows empty generations and blocks nonempty, symlinked,
  weak, or over-limit generations without mutation;
- non-Unix builds fail closed at the public boundary;
- host-independent `cargo check --all-targets`, focused suites, full
  `cargo test --all-targets`, and `git diff --check` pass before completion.
