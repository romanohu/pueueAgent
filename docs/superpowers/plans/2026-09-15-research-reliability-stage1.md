# Research Reliability Stage One Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align runtime role instructions, let operators explicitly update recognized project instructions without losing custom text, and exercise candidate promotion using measured CPU training loss.

**Architecture:** Keep the existing supervisor, role schemas, launch paths, and SQLite authority. Add one narrow instruction updater with pure content classification separated from private filesystem publication; extend the existing real-Pueue E2E using a deterministic learning fixture, not a repository-specific controller.

**Tech Stack:** Existing Rust/Tokio/clap/sha2/libc, Python standard library, existing pytest check discovery, Bats and real Pueue on Linux.

## Global Constraints

- Approved spec: `docs/superpowers/specs/2026-09-15-research-reliability-stage1-design.md`; implement stage 1 only.
- Linuxを正式な実行・検証対象とする。
- networkの既定許可は維持する。認証情報の継承許可とは区別する。
- SQLiteの目的・予算・lineageを正本とし、agentや指示ファイルから変更させない。
- MLリポジトリ固有の中間controllerを要求しない。
- MLリポジトリのmainへの自動merge/pushは導入しない。
- rokoはテスト専用とし、既存の実験やサービスには干渉しない。
- Implementers are Luna-max; work only in `.worktrees/research-reliability-stage1` on `codex/research-reliability-stage1`. No merge or push. Use focused commits and preserve unrelated changes.
- Use apply_patch for repository edits. Test first and report RED/GREEN evidence. Do not broaden dependencies, reformat unrelated source, migrate SQLite, or change session policies.
- Project instructions and role prompt contract tests must exercise the production boundary (init output or actual prompt construction/launch); do not grep source files and call that behavior verification. Fake agents establish protocol/control-plane behavior, not actual LLM reasoning ability.
- All update input and candidate output must be UTF-8 and at most 64 KiB. New backup directories are 0700; new backup and instruction files are 0600.
- Existing unknown/custom-edited instructions are conflicts, not an invitation to guess a merge. Read-only preview never creates files, SQLite state, locks, backups, or Pueue tasks.
- Skill implementer/reviewer reports and progress live only in this plan's ignored `.superpowers/sdd/2026-09-15-research-reliability-stage1/` directory.

## File and Interface Map

- `templates/instructions.md`: current marked role-aware instructions used by init.
- `templates/legacy/instructions-v0.md`: immutable exact distribution at `5d1a8e0`, needed for real legacy migration, not a second active prompt.
- `src/agent.rs`, `src/scheduler.rs`, `src/code_change.rs`: targeted role prompt changes only where the actual emitted contract needs clarification.
- `src/instructions.rs`: known-version matching, bounded candidate construction, token, update result and public entry point.
- `src/instructions_file.rs`: private no-follow reading, locking, backups and atomic publication, registered privately in `src/lib.rs`.
- `src/cli.rs`, `src/main.rs`, `src/lib.rs`: small command and module wiring.
- `tests/integration/instructions.rs`: real temporary-project/CLI update tests, registered in `Cargo.toml`.
- Existing init, cli_help, scheduler and agent tests: preserve existing contracts and test new emitted role boundaries.
- `tests/e2e/learning_experiment/`: portable `model.py`, `train.py`, `test_model.py`, `pytest.ini`; training and evaluator used in the new disposable Git project.
- `tests/e2e/rust_supervisor.sh`, `tests/support/fake_codex.sh`: additive learning campaign case; keep existing fixed-score/failure/recovery cases intact.
- Japanese commands/getting-started/troubleshooting and operations docs: updater, manual conflict recovery, precise test coverage.

---

### Task 1: Align Role Instructions and Preserve a Known Legacy Distribution

**Files:** Modify `templates/instructions.md`, `src/agent.rs`, `src/scheduler.rs`, `src/code_change.rs` only for needed emitted-role text; tests in `tests/integration/init.rs`, `tests/integration/cli_help.rs`, `tests/integration/scheduler.rs`, existing agent/code-change unit tests. Create `templates/legacy/instructions-v0.md`.

**Interfaces:** Produces the exact legacy distribution file and new template enclosed by `<!-- pueue-agent:instructions v1 begin -->` and `<!-- pueue-agent:instructions v1 end -->`. Do not change public agent/schema APIs or introduce an instruction-update command in this task.

- [ ] **Step 1: Preserve migration evidence and write failing boundary tests.** Read the legacy file with `git show 5d1a8e0:templates/instructions.md`, then create the immutable legacy copy with apply_patch. Extend the actual init test to compare the materialized generated instructions with the distribution and exercise preservation of a user-owned existing file. Exercise production prompt builders/launch capture with managed and nonmanaged contexts. Confirm emitted managed instructions explicitly keep source edits, commits and direct submission out of the Standard role; decision can propose code changes without editing; goal claims require schema evidence; editor cannot own commits/ref updates. Update the old Phase-2-only cli_help assertion as part of this contract, not a source-text-only test.

```rust
#[test]
fn init_preserves_custom_instructions_byte_for_byte() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path().join("experiment");
    let state = root.join(".pueue-agent");
    std::fs::create_dir_all(&state).unwrap();
    let custom = b"custom project rules\r\nDo not change the dataset.\n";
    std::fs::write(state.join("instructions.md"), custom).unwrap();
    assert!(init(&root).status.success());
    assert_eq!(std::fs::read(state.join("instructions.md")).unwrap(), custom);
}
```

This preservation case may already pass and remains a regression guard; record a RED from the new generated/role-boundary case before editing the active template or runtime prompt.

- [ ] **Step 2: Run the new focused cases before implementation.** Use `cargo test --test init instructions -- --test-threads=1`, `cargo test --test scheduler prompt -- --test-threads=1` and the named new unit cases. Record exact expected failures; unsupported Linux launch tests on macOS are not the intended RED.
- [ ] **Step 3: Update the marked template and emitted role instructions.** Keep a single readable document with Standard, Decision, Diagnosis and Editor sections. Remove the blanket instruction to modify/commit in managed campaigns and the obsolete ban on proposing code changes. Keep legacy nonmanaged behavior explicitly scoped. Use the actual output schema vocabulary; no new JSON decision fields. Preserve the instructions priority: immutable SQLite objective and supplied schema/policy govern, project text is context. Example decision contract text to integrate into the existing builder:

```text
Return exactly one JSON decision matching the supplied schema: proposal,
finite wait, or goal_reached with evidence. A code_change proposal requests
supervisor-owned editing; it does not authorize you to edit source, commit,
modify project state, or submit or terminate a Pueue task yourself.
```

Keep diagnosis actions and editor output under their existing schemas. Do not embed arbitrary instructions.md in dedicated role prompts. In managed Standard dispatch clarify advisory-only behavior at the emitted prompt boundary, without granting new tool permissions in any role.

- [ ] **Step 4: Verify targeted suites and self-review.** Run `cargo test --test init --test cli_help --test config -- --test-threads=1`, focused changed unit/prompt cases, `cargo check --all-targets`, and `git diff --check`. Test actual capture/launch paths on Linux when required. Check the legacy copy byte-for-byte against `git show`, and report changes to every touched prompt. Do not claim actual LLM compliance from a fake-agent test.
- [ ] **Step 5: Commit only this task's files.** Subject: `fix: align campaign agent role instructions`. Report RED/GREEN, known platform limitations, exact SHA, and self-review concerns to the task report. Task-scoped spec and quality review must pass before Task 2.

### Task 2: Deliver Explicit, Backup-Protected Instruction Updates

**Files:** Create `src/instructions.rs`, `src/instructions_file.rs`, `tests/integration/instructions.rs`; modify `src/cli.rs`, `src/main.rs`, `src/lib.rs`, `Cargo.toml`, `docs/commands-ja.md`, `docs/getting-started-ja.md`, `docs/troubleshooting-ja.md`.

**Interfaces:** Consumes Task 1's two template files. Public entry point and response:

```rust
pub enum UpdateStatus { Current, UpdateAvailable, Updated }
pub struct InstructionsUpdate {
    pub status: UpdateStatus,
    pub preview_token: Option<String>,
    pub before_sha256: String,
    pub after_sha256: String,
    pub backup_path: Option<std::path::PathBuf>,
    pub diff: Option<String>,
}
pub fn update(
    project_root: &std::path::Path,
    apply_token: Option<&str>,
) -> Result<InstructionsUpdate, crate::AppError>;
pub fn render_update(result: &InstructionsUpdate) -> String;
```

The filesystem module is private and owns file descriptors, original identity and checked publication. Do not expose file mutation hooks publicly for tests. Unit-test private fault points under `cfg(test)`; integration tests use actual CLI and files. The public response must not carry unbounded raw old/custom contents for error logging.

- [ ] **Step 1: Write failing real CLI tests.** Build initialized temporary projects, replace only the test instructions with the known legacy fixture, and invoke the new command through assert_cmd. First RED should be unrecognized `instructions` command. Add this concrete round-trip structure, expanding assertions for all unrelated state files:

```rust
let preview = assert_cmd::Command::cargo_bin("pueue-agent").unwrap()
    .args(["instructions", "update"]).arg(&root).output().unwrap();
assert!(preview.status.success());
assert_eq!(std::fs::read(&instruction_path).unwrap(), original_bytes);
assert!(!root.join(".pueue-agent/instructions.backups").exists());
let text = String::from_utf8(preview.stdout).unwrap();
let token = text.lines().find_map(|line| line.strip_prefix("preview_token: ")).unwrap();
let apply = assert_cmd::Command::cargo_bin("pueue-agent").unwrap()
    .args(["instructions", "update", "--apply", token]).arg(&root)
    .output().unwrap();
assert!(apply.status.success());
let updated = std::fs::read(&instruction_path).unwrap();
assert!(updated.starts_with(b"custom prefix\r\n"));
assert!(updated.ends_with(b"custom suffix\n"));
```

Use literal custom bytes around the old distribution. Validate backup content independently against saved original bytes, private permissions, no SQLite/Pueue creation, repeat-apply no-op, token invalidation after editing/custom text changes, and root binding across two identical projects.

- [ ] **Step 2: Implement pure bounded classification and tokens with unit tests.** Match exact known template bytes once, preserve prefix/suffix bytes, reject extra/malformed markers and mixed legacy/new blocks before returning Current. Reject invalid UTF-8, unknown/customized distributions, empty/missing input, duplicates and input/candidate over 65536 bytes. Legacy is only the frozen `5d1a8e0` version. Use SHA-256 domain separation and length-prefixed root/old/new bytes:

```rust
use sha2::{Digest, Sha256};
let mut digest = Sha256::new();
digest.update(b"pueue-agent:instructions-update:v1\0");
for part in [root_bytes, before_bytes, after_bytes] {
    digest.update((part.len() as u64).to_be_bytes());
    digest.update(part);
}
let token = format!("{:x}", digest.finalize());
```

Canonicalize the project path before token calculation; use lossless Unix OsStr bytes. Invalid platform paths or unsupported filesystem guarantees fail explicitly, not through lossy path collisions. Already-current files return Current without writes even for an old token. Check unsafe path conditions before accepting a no-op. Keep recognizable error labels such as `instructions: conflict` in bounded AppError messages; don't print file contents in errors.

- [ ] **Step 3: Implement no-follow reading and protected publication.** Preview performs only bounded reads of an initialized project's config presence and instructions; do not call service bootstrap. For apply, serialize updater invocations using a private stable lock file and flock; preserve the lock inode rather than unlinking a live lock. Reuse existing project anchor/openat patterns from project_logs/environment when accessible; keep any new helper private and limited to this command.

```rust
// Use descriptor-relative opens beneath the verified project state directory.
let flags = libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK;
// After open/fstat: require regular file, current uid, nlink == 1,
// bounded length, and matching path identity. Reject writable-by-others
// parent directories and any symlink below the canonical project root.
```

Check the token before creating backup/candidate files. Before replacement recheck original content/identity and parent identities. Backups go to `.pueue-agent/instructions.backups/<OLD_SHA256>.md`, mode0600 in owned mode0700 directory, no-follow/create-new. Existing backups are only reusable if bytes, owner/type/link count and mode are safe and match the original. Never truncate or overwrite a conflicting backup.

Write and sync original backup, sync its directory, write and sync a private create-new candidate in the instructions parent, then checked rename and parent sync. Do not truncate instructions in place. Track whether publication occurred so a post-rename sync failure reports uncertain durability, not unchanged/success. Cleanup only owned temporary files; retain backup. Reject original hardlinks, symlink/FIFO/device paths, path swaps and detectable concurrent content edits. Document that noncooperating external editors must not race the updater; this is not OS containment.

- [ ] **Step 4: Wire the CLI and bounded operator output.** Add `Instructions` command with `Update` subcommand and `--apply <PREVIEW_TOKEN>` optional string. Follow existing current-directory resolution; don't initialize SQLite or service paths. Output `status:`, digest fields, `preview_token:` when available, bounded diff and the next command. Redact terminal control characters/secrets using existing output policy, explicitly indicate truncation/redaction; do not redact the machine token. Print backup path and next-run activation note after apply. Exit0 for current/update_available/updated, exit1 for conflict/unsafe/I/O, clap exit2 for missing --apply value. New helper signatures:

```rust
#[derive(Debug, clap::Args)]
pub struct InstructionsArgs {
    #[command(subcommand)]
    pub action: InstructionsAction,
}
#[derive(Debug, clap::Subcommand)]
pub enum InstructionsAction { Update(InstructionsUpdateArgs) }
#[derive(Debug, clap::Args)]
pub struct InstructionsUpdateArgs {
    #[arg(long, value_name = "PREVIEW_TOKEN")]
    pub apply: Option<String>,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<std::path::PathBuf>,
}
```

- [ ] **Step 5: Cover failure boundaries and documentation.** Test duplicate/malformed markers, customized managed body, missing file, UTF-8/size limits, oversized candidate after replacement, original/parent/backup/lock symlinks, original hardlink/FIFO, preexisting unsafe backup, token/content/path mismatch, backup write/sync failure, candidate write/sync failure, rename failure, post-rename sync failure, and two concurrent cooperating applies. Assert original bytes preserved before publication and retained backup after any publication uncertainty. Use bounded timeouts for FIFO/lock tests. Keep fault injection private `cfg(test)` and avoid environment-controlled production failpoints.

Document preview/apply syntax, no automatic update, exact known-version matching, conflict manual procedure (retain custom text outside markers), backup location/private permissions, human-controlled restore while no concurrent edit/run reads the file, and that active agents are not retroactively changed. Do not invent an automatic rollback or template framework.

- [ ] **Step 6: Run, self-review and commit.** Run `cargo test --test instructions --test init --test cli_help -- --test-threads=1`, `cargo test --lib instructions -- --test-threads=1`, `cargo check --all-targets`, changed-file format checks, `git diff --check`. Run Linux-only race/identity tests on roko at final gate, recording any unexecuted local cases explicitly. Commit `feat: add explicit backed-up instruction updates`. Report RED/GREEN, exact commands/counts and file permissions verification; task review must approve before Task 3.

### Task 3: Exercise Real CPU Learning Through Candidate Promotion

**Files:** Create `tests/e2e/learning_experiment/model.py`, `train.py`, `test_model.py`, `pytest.ini`; modify `tests/e2e/rust_supervisor.sh`, `tests/support/fake_codex.sh`, `docs/operations-ja.md`. Preserve all existing fixed-score and fault scenarios.

**Interfaces:** Fixture `model.py` defines `LEARNING_RATE = 0.001`, `train(learning_rate=LEARNING_RATE, steps=100)` returning a scalar weight, and `evaluate(weight)` returning measured MSE. Candidate changes only learning rate to0.05. `train.py` writes schema_version1/current experiment ID/metrics.loss through the existing environment contract. Candidate trainer tests are discovered via pytest.ini using existing policy tools.

- [ ] **Step 1: Write a failing portable learning test before the fixture implementation.** Use fixed data for y=2x, initial weight0, training x=(-1,-0.5,0.5,1) and evaluation x=(-0.75,-0.25,0.25,0.75). Test one training step with rate0.1 gives weight0.25, zero steps gives weight0, and rate0.05 after100 steps yields lower held-out MSE than rate0.001. Use `unittest` classes so `python3 -m unittest discover -s tests/e2e/learning_experiment -p 'test_*.py'` and pytest both work. The RED must be missing training behavior, not missing pytest/dependencies.

```python
def train(learning_rate=LEARNING_RATE, steps=100):
    weight = 0.0
    xs = (-1.0, -0.5, 0.5, 1.0)
    for _ in range(steps):
        gradient = sum(2.0 * (weight * x - 2.0 * x) * x for x in xs) / len(xs)
        weight -= learning_rate * gradient
    return weight

def evaluate(weight):
    xs = (-0.75, -0.25, 0.25, 0.75)
    return sum((weight * x - 2.0 * x) ** 2 for x in xs) / len(xs)
```

No random generator is needed; data, initial state and step count are literal and deterministic. Validate finite numbers. Include a guard that evaluation data and implementation have not changed in the candidate, using independent baseline digests/checks in the E2E harness rather than trusting the agent.

- [ ] **Step 2: Implement the trainer and output contract.** Compute loss from train/evaluate and encode with `json.dumps(..., allow_nan=False)` before opening the result file. Preserve an existing result inode with os.open O_WRONLY|O_CREAT|O_TRUNC mode0600 and write via fdopen; don't rename result files. Exercise subprocess training into a temporary output file; assert experiment ID, finite measured loss, private creation permissions, existing inode preservation and no tracked-source writes. Use a main guard so imports don't train/write.

- [ ] **Step 3: Add the learning E2E to the existing isolated harness.** Create a new disposable Git project under the harness WORK path, copy the fixture files, configure existing native Python/pytest tools, initialize/enable/submit via ordinary CLI. Extend the fake-agent boundary to return one code_change that changes only LEARNING_RATE, and an editor result in the existing schema; then return finite wait after candidate completion so the test does not loop. Fake behavior must be gated to the learning fixture and leave existing scenarios unchanged.

```text
ordinary metric-aware submit
  -> baseline trains and emits measured loss
  -> fake decision requests a learning-rate change
  -> real coordinator/editor gate/check/commit
  -> real Pueue candidate trains and emits measured loss
  -> real evaluation/promotion updates local best
```

Do not insert a test controller between supervisor and ML project. Reuse existing SQL/status observation helpers only as assertions, not to fake evaluation or promotion. Keep restart helpers and bounded wait_for_sql style already in the harness.

- [ ] **Step 4: Assert the real state and restart invariants.** Check baseline/candidate loss ordering and independent numeric tolerances, metrics row belongs to the submitted experiment, candidate SHA equals promoted best SHA, only learning rate changed, and both tasks are uniquely linked. Snapshot Pueue task count/experiment IDs/promotion count/best SHA before restart; restart only the test daemon, and assert these identities/counts unchanged after reconciliation. Check source main/remote/unrelated worktree protections as in the existing candidate gate. Preserve all prior failure tests, and describe in operations docs that training/Pueue are real while decisions/edit suggestions are controlled test doubles.
- [ ] **Step 5: Run local fixture checks and Linux E2E, then commit.** Run the unittest command, `bash -n tests/e2e/rust_supervisor.sh tests/support/fake_codex.sh`, `bats tests/test_shell_entrypoints.bats`, `git diff --check`; exercise `tests/e2e/run.sh` on the exact committed source snapshot at roko using only a dedicated test profile. Report any unavailable host dependency, never replace a failed real-learning test with a fixed metric. Commit `test: cover measured learning campaign promotion`; review the complete task diff and test evidence.

## Whole-Branch Acceptance and Handoff

- [ ] Controller verifies clean feature HEAD and records all per-task commits/review verdicts in this plan's ledger. No task is complete while Important/Critical review findings remain unresolved.
- [ ] Check roko `/home/romanohu/project/pueueAgent` read-only before syncing. Preserve unrelated user changes; if that checkout is dirty or has new work, use an explicitly scoped disposable test copy within the authorized test area instead of overwriting. Never stop existing services or touch existing Pueue tasks.
- [ ] On the exact tested source snapshot run `cargo fmt --check`, `cargo check --all-targets`, `cargo check --all-targets --release`, `cargo test --all-targets -- --test-threads=1`, `bats tests/test_shell_entrypoints.bats`, `tests/e2e/run.sh`. Record SHA, versions, exit codes and counts. Inspect baseline formatting differences before any mechanical formatter; do not silently rewrite unrelated files to pass a preexisting format gate.
- [ ] Dispatch one whole-branch code review using the SDD final-review protocol. Address findings with the prescribed fix wave and scoped re-review. Preserve detailed evidence in a committed concise verification report under `docs/report/2026-09-15-research-reliability-stage1-verification.md` before removing only disposable plan scratch.
- [ ] Leave the tested branch/worktree available for user review; no automatic merge/push. Report stage1 status distinctly from stages2–5, and do not begin the next stage's implementation without its design gate.
