//! Pure protocol and repository-shape validation for code-change proposals.
//!
//! The stateful worktree, editor, and check runners are added in a later
//! phase.  This module intentionally keeps the admission-facing data small and
//! deterministic so it can be validated before any child process is started.

use std::{
    collections::BTreeSet,
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    sync::{atomic::{AtomicBool, AtomicU64, Ordering}, Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use rusqlite::OptionalExtension;

use crate::{
    environment::SanitizedEnvironment,
    execution_policy::{
        CampaignLimits, CodeChangeTool, ExecutableAnchor, ExecutableIdentity, ProjectRootAnchor,
        ResolvedExecutionPolicy, ResolvedProjectExecutionPolicy, VerifiedProjectRoot,
        VerifiedWorkingDirectory,
    },
    models::Project,
    AppError,
};

use crate::{
    db::{CodeChangeRepository, Db},
    models::{CodeChangeRun, CodeChangeState, ExperimentStatus},
};

#[cfg(unix)]
use crate::process::{
    spawn_verified_command_before_classified, terminate_process_group_before,
    GIT_ADMIN_FD, GIT_COMMON_DIR_FD, ProcessGroupRequirement, PROJECT_ROOT_FD,
    VerifiedChildIo, VerifiedCommandSpec, VerifiedGitDirectories,
};

#[cfg(unix)]
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
        os::unix::{ffi::{OsStrExt, OsStringExt}, fs::{FileExt, MetadataExt, OpenOptionsExt}},
};

#[cfg(not(unix))]
use crate::execution_policy::{PolicyViolation, PolicyViolationCode, PolicyViolationStage};

const MAX_GIT_OUTPUT_BYTES: usize = 1024 * 1024;
#[allow(dead_code)]
const MAX_CHECK_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_WORKTREE_ID_BYTES: usize = 128;
const MAX_STATUS_PATHS: usize = 50;
const MAX_IGNORED_SCAN_ENTRIES: usize = 512;
const MAX_IGNORED_SCAN_DEPTH: usize = 32;
const MAX_IGNORED_SCAN_BYTES: u64 = 500_000;
static TEMP_INDEX_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The only candidate-change facts that are eligible for durable projection.
/// Paths are retained only in the in-memory validation operation; callers must
/// persist the counts and digests, never this list.
#[derive(Clone, PartialEq, Eq)]
pub struct DiffFacts {
    pub transient_paths: Vec<PathBuf>,
    pub file_count: usize,
    pub diff_bytes: usize,
    pub tree_sha: String,
    pub digest: String,
}

impl std::fmt::Debug for DiffFacts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DiffFacts")
            .field("transient_path_count", &self.transient_paths.len())
            .field("file_count", &self.file_count)
            .field("diff_bytes", &self.diff_bytes)
            .field("tree_sha", &self.tree_sha)
            .field("digest", &self.digest)
            .finish()
    }
}

impl DiffFacts {
    pub fn persisted_digest(&self) -> &str {
        &self.digest
    }
}

/// Opaque ownership of one detached candidate worktree.  Its root and path
/// are only produced by the descriptor-checked worktree manager.
#[cfg(unix)]
pub struct VerifiedCodeChangeWorktree {
    manager: WorktreeManager,
    candidate: VerifiedProjectRoot,
    diff_facts: Option<DiffFacts>,
    candidate_sha: Option<String>,
}

#[cfg(unix)]
impl std::fmt::Debug for VerifiedCodeChangeWorktree {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedCodeChangeWorktree")
            .field("path", &self.manager.worktree_path)
            .field("candidate_sha_present", &self.candidate_sha.is_some())
            .finish()
    }
}

#[cfg(unix)]
impl VerifiedCodeChangeWorktree {
    pub fn path(&self) -> &Path {
        &self.manager.worktree_path
    }

    pub fn root(&self) -> &VerifiedProjectRoot {
        &self.candidate
    }

    pub fn diff_facts(&self) -> Option<&DiffFacts> {
        self.diff_facts.as_ref()
    }

    pub async fn verify(&mut self) -> Result<DiffFacts, AppError> {
        let facts = self.manager.verify(&self.candidate).await?;
        self.diff_facts = Some(facts.clone());
        Ok(facts)
    }

    pub async fn run_checks(
        &mut self,
        checks: &[ProposedCheck],
    ) -> Result<Vec<String>, AppError> {
        let runner = CheckRunner {
            manager: &self.manager,
            candidate: &self.candidate,
        };
        runner.run(&self.candidate, checks).await
    }

    pub async fn commit(&mut self) -> Result<String, AppError> {
        let expected = self.diff_facts.as_ref().ok_or(AppError::Validation {
            field: "code_change.diff",
            message: "must pass candidate validation before commit",
        })?;
        let fresh = CandidateValidator::new(&self.manager, &self.candidate)?
            .verify()
            .await?;
        if fresh != *expected {
            return Err(recovery_required());
        }
        let sha = CandidateRepository::new(&self.manager, &self.candidate)?
            .commit_candidate(&fresh)
            .await?;
        self.candidate_sha = Some(sha.clone());
        Ok(sha)
    }

    pub async fn cleanup(
        self,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        self.manager
            .cleanup_authorized(
                Some(&self.candidate),
                self.candidate_sha.as_deref(),
                authorization,
            )
            .await
    }

    /// Perform the narrowly-scoped Task 3 best-ref compare-and-swap.  The
    /// durable authorization is reloaded by the manager immediately before
    /// the ref boundary; callers cannot supply a campaign/ref independently.
    pub async fn update_best_ref_cas(
        &mut self,
        authorization: &CodeChangeCleanupAuthorization,
        expected_old_sha: Option<&str>,
    ) -> Result<(), AppError> {
        let candidate_sha = self.candidate_sha.clone().ok_or(AppError::Validation {
            field: "code_change.candidate_sha",
            message: "must commit the candidate before updating best",
        })?;
        let run = authorization.fresh_run()?;
        self.manager
            .verify_cleanup_run(&run, Some(&candidate_sha), Some(&self.candidate), authorization)?;
        CandidateRepository::new(&self.manager, &self.candidate)?
            .update_best_ref_cas_authorized(
                authorization,
                &candidate_sha,
                expected_old_sha,
            )
            .await
    }
}

/// A durable, one-run cleanup capability.  It intentionally contains a
/// clone of the database handle rather than a caller-supplied row: cleanup
/// reloads the row immediately before authorization and therefore cannot be
/// authorized by stale in-memory state.
#[derive(Clone)]
pub struct CodeChangeCleanupAuthorization {
    db: Db,
    run_id: String,
}

impl std::fmt::Debug for CodeChangeCleanupAuthorization {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodeChangeCleanupAuthorization")
            .field("run_id", &self.run_id)
            .finish()
    }
}

impl CodeChangeCleanupAuthorization {
    pub fn load(db: &Db, run_id: &str) -> Result<Self, AppError> {
        if run_id.is_empty() {
            return Err(recovery_required());
        }
        CodeChangeRepository::new(db)
            .find_by_id(run_id)?
            .ok_or_else(recovery_required)?;
        Ok(Self {
            db: db.clone(),
            run_id: run_id.to_owned(),
        })
    }

    fn fresh_run(&self) -> Result<CodeChangeRun, AppError> {
        CodeChangeRepository::new(&self.db)
            .find_by_id(&self.run_id)?
            .ok_or_else(recovery_required)
    }
}

/// Public Task 3 facade for a durably reserved code-change run.  The caller
/// supplies only the startup policy/project capabilities and a run ID; all
/// campaign/proposal/worktree identifiers and the base revision come from a
/// fresh database query.
#[cfg(unix)]
pub async fn prepare_code_change_worktree_for_run(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    db: &Db,
    run_id: &str,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    let run = CodeChangeCleanupAuthorization::load(db, run_id)?.fresh_run()?;
    verify_durable_run_scope(db, &run, project)?;
    if run.state != CodeChangeState::Reserved
        && run.state != CodeChangeState::PreparingWorktree
        && run.state != CodeChangeState::Editing
    {
        return Err(validation(
            "code_change.state",
            "is not a live preparation state",
        ));
    }
    let expected_relative = owned_worktree_relative_path(&run.campaign_id, &run.proposal_id)?;
    let expected_candidate_ref =
        format!("refs/heads/{}", candidate_ref(&run.campaign_id, &run.proposal_id)?);
    let expected_best_ref = format!("refs/heads/{}", best_ref(&run.campaign_id)?);
    if Path::new(&run.worktree_relative_path) != expected_relative.as_path()
        || run.worktree_id != run.code_change_run_id
        || run.candidate_ref != expected_candidate_ref
        || run.best_ref != expected_best_ref
    {
        return Err(recovery_required());
    }
    let manager = WorktreeManager::new_for_run(
        policy,
        project,
        original,
        &run.campaign_id,
        &run.proposal_id,
        &run.base_sha,
        &run.worktree_id,
        &expected_relative,
    )?;
    let candidate = finish_prepared_manager(manager, policy, project, original).await?;
    let proof = candidate.manager.durable_ownership_proof(&candidate.candidate)?;
    let ownership_result = CodeChangeRepository::new(db).record_worktree_ownership(
        run_id,
        &proof.state_root_identity,
        &proof.worktrees_identity,
        &proof.campaign_identity,
        &proof.candidate_root_identity,
        &proof.candidate_admin_identity,
        &proof.candidate_common_identity,
        &proof.candidate_admin_path,
        &proof.candidate_common_path,
        unix_timestamp()?,
    );
    if let Err(error) = ownership_result {
        return Err(match candidate
            .manager
            .cleanup(Some(&candidate.candidate), None, false)
            .await
        {
            Ok(()) => error,
            Err(cleanup) => cleanup,
        });
    }
    Ok(candidate)
}

#[cfg(unix)]
async fn finish_prepared_manager(
    mut manager: WorktreeManager,
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    manager.inspect_base().await?;
    let candidate = manager.prepare().await?;
    if let Err(error) = policy.for_code_change_worktree(project, original, &candidate) {
        let original = AppError::from(error);
        return Err(match manager.cleanup(Some(&candidate), None, false).await {
            Ok(()) => original,
            Err(cleanup) => cleanup,
        });
    }
    let validator = match CandidateValidator::new(&manager, &candidate) {
        Ok(validator) => validator,
        Err(error) => {
            return Err(match manager.cleanup(Some(&candidate), None, false).await {
                Ok(()) => error,
                Err(cleanup) => cleanup,
            });
        }
    };
    if let Err(error) = validator.verify().await {
        return Err(match manager.cleanup(Some(&candidate), None, false).await {
            Ok(()) => error,
            Err(cleanup) => cleanup,
        });
    }
    Ok(VerifiedCodeChangeWorktree {
        manager,
        candidate,
        diff_facts: None,
        candidate_sha: None,
    })
}

pub const RUST_CHECK: &[&str] = &["cargo", "test", "--all-targets", "--", "--test-threads=1"];
pub const UV_PYTEST_CHECK: &[&str] = &["uv", "run", "pytest"];
pub const PYTHON_PYTEST_CHECK: &[&str] = &["python", "-m", "pytest"];

fn pinned_git_argv(args: &[OsString]) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(args.len() + 22);
    for argument in [
        "git",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.pager=cat",
        "-c",
        "credential.helper=",
        "-c",
        "core.askPass=",
        "-c",
        "core.sshCommand=",
        "-c",
        "commit.gpgSign=false",
        "-c",
        "tag.gpgSign=false",
        "-c",
        "user.signingKey=",
        "--no-pager",
    ] {
        argv.push(OsString::from(argument));
    }
    argv.extend_from_slice(args);
    argv
}

#[cfg(unix)]
fn git_descriptor_path(fd: i32) -> String {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        format!("/proc/self/fd/{fd}")
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        format!("/dev/fd/{fd}")
    }
}

#[cfg(unix)]
fn git_descriptor_environment(
    admin_fd: i32,
    common_fd: i32,
    worktree_fd: i32,
) -> std::collections::BTreeMap<String, String> {
    [
        ("GIT_DIR".to_owned(), git_descriptor_path(admin_fd)),
        ("GIT_COMMON_DIR".to_owned(), git_descriptor_path(common_fd)),
        ("GIT_WORK_TREE".to_owned(), git_descriptor_path(worktree_fd)),
    ]
    .into_iter()
    .collect()
}

#[cfg(unix)]
fn apply_git_descriptor_environment(environment: &mut SanitizedEnvironment) {
    for (name, value) in git_descriptor_environment(
        GIT_ADMIN_FD,
        GIT_COMMON_DIR_FD,
        PROJECT_ROOT_FD,
    ) {
        environment.with_generated(&name, OsString::from(value));
    }
}

#[cfg(unix)]
fn is_worktree_administration(args: &[OsString]) -> bool {
    matches!(
        args.get(0).map(OsString::as_os_str),
        Some(value) if value == OsStr::new("worktree")
    ) && matches!(
        args.get(1).map(OsString::as_os_str),
        Some(value) if value == OsStr::new("add") || value == OsStr::new("remove")
    )
}

#[cfg(unix)]
fn descriptor_worktree_leaf(leaf: &str) -> OsString {
    // The parent descriptor is installed as the command cwd.  Git receives
    // only the manager-validated single leaf, so it cannot rediscover a
    // replaceable absolute candidate pathname.
    OsString::from(leaf)
}

const MAX_EDITOR_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_EDITOR_SUMMARY_BYTES: usize = 64 * 1024;
const MAX_CHECK_SOURCE_BYTES: usize = 64;
const MAX_CHECK_ARG_BYTES: usize = 4 * 1024;
const MAX_CHECK_ARG_COUNT: usize = 32;
const MAX_GIT_REF_COMPONENT_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EditorOutput {
    pub schema_version: u32,
    pub status: String,
    pub summary: String,
    pub proposed_checks: Vec<ProposedCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedCheck {
    pub source: String,
    pub argv: Vec<String>,
    pub working_directory: String,
}

pub fn canonical_full_sha(value: &str) -> Result<String, AppError> {
    if (value.len() != 40 && value.len() != 64)
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(validation(
            "code_change.sha",
            "must be a lowercase full object ID",
        ));
    }
    Ok(value.to_owned())
}

pub fn validate_full_sha(value: &str) -> Result<(), AppError> {
    canonical_full_sha(value).map(|_| ())
}

pub fn candidate_ref(campaign_id: &str, proposal_id: &str) -> Result<String, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    validate_internal_id("proposal_id", proposal_id)?;
    let campaign_component = git_ref_component(campaign_id)?;
    let proposal_component = git_ref_component(proposal_id)?;
    Ok(format!(
        "campaign/{}/candidate/{}",
        campaign_component, proposal_component,
    ))
}

pub fn best_ref(campaign_id: &str) -> Result<String, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    Ok(format!("campaign/{}/best", git_ref_component(campaign_id)?))
}

pub fn owned_worktree_relative_path(
    campaign_id: &str,
    proposal_id: &str,
) -> Result<PathBuf, AppError> {
    validate_internal_id("campaign_id", campaign_id)?;
    validate_internal_id("proposal_id", proposal_id)?;
    Ok(PathBuf::from(".pueue-agent")
        .join("worktrees")
        .join(campaign_id)
        .join(proposal_id))
}

pub fn parse_editor_output(
    input: &[u8],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<EditorOutput, AppError> {
    if input.len() > MAX_EDITOR_OUTPUT_BYTES {
        return Err(validation(
            "code_change.editor_output",
            "exceeds the bounded editor output size",
        ));
    }
    let output: EditorOutput = serde_json::from_slice(input)
        .map_err(|_| validation("code_change.editor_output", "must be strict editor JSON"))?;
    validate_editor_output(&output, root, limits, available_tools)?;
    Ok(output)
}

pub fn parse_editor_json(
    input: &[u8],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<EditorOutput, AppError> {
    parse_editor_output(input, root, limits, available_tools)
}

pub fn validate_editor_output(
    output: &EditorOutput,
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<(), AppError> {
    if output.schema_version != 1 {
        return Err(validation(
            "code_change.schema_version",
            "must be version 1",
        ));
    }
    if output.status != "ready" && output.status != "cannot_apply" {
        return Err(validation(
            "code_change.status",
            "must be ready or cannot_apply",
        ));
    }
    if output.summary.len() > MAX_EDITOR_SUMMARY_BYTES {
        return Err(validation(
            "code_change.summary",
            "exceeds the bounded summary size",
        ));
    }
    validate_proposed_checks(&output.proposed_checks, root, limits, available_tools)?;
    if output.status == "ready" && output.proposed_checks.is_empty() {
        return Err(validation(
            "code_change.proposed_checks",
            "ready output must include a project check",
        ));
    }
    Ok(())
}

pub fn validate_proposed_checks(
    checks: &[ProposedCheck],
    root: &Path,
    limits: &CampaignLimits,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<(), AppError> {
    if checks.len() > limits.max_code_change_checks as usize {
        return Err(validation(
            "code_change.proposed_checks",
            "exceeds the bounded check count",
        ));
    }
    if !root.is_absolute() {
        return Err(validation(
            "code_change.root",
            "must be an absolute project root",
        ));
    }
    for check in checks {
        if check.source.is_empty() || check.source.len() > MAX_CHECK_SOURCE_BYTES {
            return Err(validation(
                "code_change_check.source",
                "must be bounded non-empty text",
            ));
        }
        validate_relative_working_directory(&check.working_directory)?;
        if check.argv.is_empty() || check.argv.len() > MAX_CHECK_ARG_COUNT {
            return Err(validation(
                "code_change_check.argv",
                "must be a bounded non-empty argv",
            ));
        }
        if check.argv.iter().any(|arg| {
            arg.is_empty() || arg.len() > MAX_CHECK_ARG_BYTES || arg.chars().any(char::is_control)
        }) {
            return Err(validation(
                "code_change_check.argv",
                "contains an invalid argument",
            ));
        }
        if shell_argv(&check.argv) {
            return Err(validation(
                "code_change_check.argv",
                "shell command execution is not permitted",
            ));
        }
        let tool = tool_for_program(&check.argv[0]).ok_or_else(|| {
            validation(
                "code_change_check.argv",
                "must start with a pinned code-change tool",
            )
        })?;
        if !available_tools.contains(&tool) {
            return Err(validation(
                "code_change_check.argv",
                "requested tool is not available in the startup policy",
            ));
        }
        if check.source
            != match tool {
                CodeChangeTool::Git => "git",
                CodeChangeTool::Cargo => "cargo",
                CodeChangeTool::Uv => "uv",
                CodeChangeTool::Python => "python",
            }
        {
            return Err(validation(
                "code_change_check.source",
                "does not match the pinned check tool",
            ));
        }
        let expected = match tool {
            CodeChangeTool::Cargo => RUST_CHECK,
            CodeChangeTool::Uv => UV_PYTEST_CHECK,
            CodeChangeTool::Python => PYTHON_PYTEST_CHECK,
            CodeChangeTool::Git => &[],
        };
        if tool == CodeChangeTool::Git {
            return Err(validation(
                "code_change_check.argv",
                "Git is reserved for the coordinator diff check",
            ));
        }
        if !check
            .argv
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
        {
            return Err(validation(
                "code_change_check.argv",
                "must use the fixed project-check argv",
            ));
        }
    }
    Ok(())
}

pub fn discover_project_checks(
    root: &Path,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<Vec<ProposedCheck>, AppError> {
    if !root.is_absolute() {
        return Err(validation("code_change.root", "must be absolute"));
    }
    let mut checks = Vec::new();
    if root.join("Cargo.toml").is_file() && available_tools.contains(&CodeChangeTool::Cargo) {
        checks.push(ProposedCheck {
            source: "cargo".to_owned(),
            argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
            working_directory: ".".to_owned(),
        });
    }
    let pytest = root.join("pytest.ini").is_file() || pyproject_has_pytest(root);
    if pytest {
        let (tool, argv) = if root.join("uv.lock").is_file() {
            if available_tools.contains(&CodeChangeTool::Uv) {
                ("uv", UV_PYTEST_CHECK)
            } else {
                ("", &[] as &[&str])
            }
        } else if available_tools.contains(&CodeChangeTool::Python) {
            ("python", PYTHON_PYTEST_CHECK)
        } else {
            ("", &[] as &[&str])
        };
        if !tool.is_empty() {
            checks.push(ProposedCheck {
                source: tool.to_owned(),
                argv: argv.iter().map(|arg| (*arg).to_owned()).collect(),
                working_directory: ".".to_owned(),
            });
        }
    }
    Ok(checks)
}

pub fn discover_checks(
    root: &Path,
    available_tools: &BTreeSet<CodeChangeTool>,
) -> Result<Vec<ProposedCheck>, AppError> {
    discover_project_checks(root, available_tools)
}

pub fn is_protected_path(path: &Path) -> bool {
    path.components().any(|component| {
        let Component::Normal(name) = component else {
            return false;
        };
        let lower = name.to_string_lossy().to_ascii_lowercase();
        matches!(
            lower.as_str(),
            ".git" | ".pueue-agent" | "execution-policy.toml" | ".env"
                    | ".env.local"
                    | ".env.production"
                    | ".aws"
                    | ".ssh"
                    | "credentials"
                    | "credentials.json"
                    | "secrets"
                    | "id_rsa"
                    | "id_ed25519"
        )
            || lower.ends_with(".pem")
            || lower.ends_with(".p12")
            || lower.ends_with(".pfx")
            || lower.ends_with(".key")
            || lower.contains("credential")
            || lower.contains("secret")
    })
}

pub fn validate_protected_paths(paths: &[PathBuf]) -> Result<(), AppError> {
    if paths.iter().any(|path| is_protected_path(path)) {
        Err(validation(
            "code_change.diff_paths",
            "contains a protected service path",
        ))
    } else {
        Ok(())
    }
}

pub fn validate_diff_limits(
    changed_files: usize,
    diff_bytes: usize,
    limits: &CampaignLimits,
) -> Result<(), AppError> {
    if changed_files > MAX_STATUS_PATHS
        || changed_files > limits.max_code_change_changed_files as usize
    {
        return Err(validation(
            "code_change.changed_files",
            "exceeds the configured changed-file cap",
        ));
    }
    if diff_bytes > 500_000 || diff_bytes > limits.max_code_change_diff_bytes as usize {
        return Err(validation(
            "code_change.diff_bytes",
            "exceeds the configured diff-byte cap",
        ));
    }
    Ok(())
}

pub fn has_project_check(checks: &[ProposedCheck]) -> bool {
    checks.iter().any(|check| {
        check.argv == RUST_CHECK
            || check.argv == UV_PYTEST_CHECK
            || check.argv == PYTHON_PYTEST_CHECK
    })
}

#[derive(Clone)]
#[cfg(unix)]
struct GitBaseline {
    #[cfg(unix)]
    repository: GitRepositoryProof,
    common_directory: PathBuf,
    protected_ref_digest: String,
    remote_config_digest: String,
}

#[cfg(unix)]
#[derive(Clone)]
struct GitDirectoryProof {
    path: PathBuf,
    identity: ExecutableIdentity,
    directory: Arc<File>,
}

#[cfg(unix)]
#[derive(Clone)]
#[derive(Copy)]
enum GitPointerKind {
    GitDir,
    Path,
}

#[cfg(unix)]
#[derive(Clone)]
struct GitFileProof {
    path: PathBuf,
    identity: ExecutableIdentity,
    digest: String,
    target: Option<PathBuf>,
    target_base: Option<PathBuf>,
    pointer_kind: Option<GitPointerKind>,
    file: Arc<File>,
}

#[cfg(unix)]
#[derive(Clone)]
enum GitEntryProof {
    Directory(GitDirectoryProof),
    File(GitFileProof),
}

#[cfg(unix)]
#[derive(Clone)]
struct GitRepositoryProof {
    root_git: GitEntryProof,
    admin: GitDirectoryProof,
    common: GitDirectoryProof,
    admin_path: PathBuf,
    common_path: PathBuf,
    admin_gitdir: Option<GitFileProof>,
    commondir: Option<GitFileProof>,
    common_config: GitFileProof,
    admin_config: Option<GitFileProof>,
    worktree_config: Option<GitFileProof>,
    expected_root: PathBuf,
    expected_admin_name: Option<OsString>,
}

#[cfg(unix)]
#[derive(Clone)]
struct WorktreeParentProof {
    state_root_path: PathBuf,
    state_root_identity: ExecutableIdentity,
    state_root: Arc<File>,
    worktrees_path: PathBuf,
    worktrees_identity: ExecutableIdentity,
    worktrees: Arc<File>,
    campaign_path: PathBuf,
    campaign_identity: ExecutableIdentity,
    campaign: Arc<File>,
}

#[cfg(unix)]
impl WorktreeParentProof {
    fn retain(policy: &ResolvedExecutionPolicy, campaign_id: &str) -> Result<Self, AppError> {
        policy.verify_code_change_state_root()?;
        let state_root = policy.code_change_state_root_directory();
        let state_root_identity = directory_identity(&state_root)?;
        let worktrees = Arc::new(open_or_create_directory_at(
            &state_root,
            OsStr::new("worktrees"),
        )?);
        let worktrees_path = policy.code_change_state_root_path().join("worktrees");
        let worktrees_identity = directory_identity(&worktrees)?;
        let campaign = Arc::new(open_or_create_directory_at(
            &worktrees,
            OsStr::new(campaign_id),
        )?);
        let campaign_path = worktrees_path.join(campaign_id);
        let campaign_identity = directory_identity(&campaign)?;
        Ok(Self {
            state_root_path: policy.code_change_state_root_path().to_owned(),
            state_root_identity,
            state_root,
            worktrees_path,
            worktrees_identity,
            worktrees,
            campaign_path,
            campaign_identity,
            campaign,
        })
    }

    fn revalidate(
        &self,
        policy: &ResolvedExecutionPolicy,
        campaign_id: &str,
    ) -> Result<File, AppError> {
        policy.verify_code_change_state_root()?;
        if policy.code_change_state_root_path() != self.state_root_path {
            return Err(recovery_required());
        }
        if directory_identity(&self.state_root)? != self.state_root_identity {
            return Err(recovery_required());
        }
        let worktrees = open_existing_directory_at(&self.state_root, OsStr::new("worktrees"))
            .map_err(|_| recovery_required())?;
        if self.worktrees_path != self.state_root_path.join("worktrees")
            || directory_identity(&worktrees)? != self.worktrees_identity
            || directory_identity(&self.worktrees)? != self.worktrees_identity
        {
            return Err(recovery_required());
        }
        let campaign = open_existing_directory_at(&worktrees, OsStr::new(campaign_id))
            .map_err(|_| recovery_required())?;
        if self.campaign_path != self.worktrees_path.join(campaign_id)
            || directory_identity(&campaign)? != self.campaign_identity
            || directory_identity(&self.campaign)? != self.campaign_identity
        {
            return Err(recovery_required());
        }
        Ok(campaign)
    }
}

#[cfg(unix)]
impl GitRepositoryProof {
    fn capture_original(root: &VerifiedProjectRoot) -> Result<Self, AppError> {
        Self::capture(root, None, None)
    }

    fn capture_candidate(
        root: &VerifiedProjectRoot,
        original: &Self,
    ) -> Result<Self, AppError> {
        let candidate = Self::capture(
            root,
            Some(&original.common_path),
            root.anchor.canonical_path.file_name(),
        )?;
        if candidate.admin_path == original.admin_path {
            return Err(recovery_required());
        }
        Ok(candidate)
    }

    fn capture(
        root: &VerifiedProjectRoot,
        expected_common: Option<&PathBuf>,
        expected_admin_name: Option<&OsStr>,
    ) -> Result<Self, AppError> {
        let root_git_path = root.anchor.canonical_path.join(".git");
        if path_has_symlink_component(&root_git_path)? {
            return Err(recovery_required());
        }
        let root_git_metadata = fs::symlink_metadata(&root_git_path).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                validation("code_change.git", "Git metadata is missing")
            } else {
                recovery_required()
            }
        })?;
        let (root_git, admin_path) = if root_git_metadata.is_dir() {
            (
                GitEntryProof::Directory(open_git_directory(&root_git_path)?),
                root_git_path.clone(),
            )
        } else if root_git_metadata.is_file() {
            let file = open_git_file(
                &root_git_path,
                Some(&root.anchor.canonical_path),
                Some(GitPointerKind::GitDir),
            )?;
            let target = file.target.clone().ok_or_else(recovery_required)?;
            (GitEntryProof::File(file), target)
        } else {
            return Err(validation(
                "code_change.git",
                "Git metadata must be a regular file or directory",
            ));
        };
        let admin = open_git_directory(&admin_path)?;
        let commondir_path = admin_path.join("commondir");
        let commondir = match fs::symlink_metadata(&commondir_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(recovery_required())
            }
            Ok(metadata) if metadata.is_file() => {
                let file = open_git_file(
                    &commondir_path,
                    Some(&admin_path),
                    Some(GitPointerKind::Path),
                )?;
                Some(file)
            }
            Ok(_) => {
                return Err(validation(
                    "git.metadata",
                    "Git common-directory file must be regular",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(recovery_required()),
        };
        let common_path = commondir
            .as_ref()
            .and_then(|file| file.target.clone())
            .unwrap_or_else(|| admin_path.clone());
        let common = open_git_directory(&common_path)?;
        let admin_gitdir_path = admin_path.join("gitdir");
        let admin_gitdir = match fs::symlink_metadata(&admin_gitdir_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(recovery_required())
            }
            Ok(metadata) if metadata.is_file() => {
                Some(open_git_file(
                    &admin_gitdir_path,
                    Some(&admin_path),
                    Some(GitPointerKind::Path),
                )?)
            }
            Ok(_) => {
                return Err(validation(
                    "git.metadata",
                    "Git worktree gitdir file must be regular",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(recovery_required()),
        };
        let worktree_config_path = admin_path.join("config.worktree");
        let worktree_config = match fs::symlink_metadata(&worktree_config_path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(recovery_required())
            }
            Ok(metadata) if metadata.is_file() => {
                Some(open_git_file(&worktree_config_path, None, None)?)
            }
            Ok(_) => {
                return Err(validation(
                    "git.config",
                    "Git worktree configuration must be regular",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(recovery_required()),
        };
        if let Some(expected) = expected_common {
            if common.path != *expected {
                return Err(recovery_required());
            }
        }
        if let Some(expected) = expected_admin_name {
            let expected_parent = common.path.join("worktrees");
            if admin_path.parent() != Some(expected_parent.as_path())
                || admin_path.file_name() != Some(expected)
            {
                return Err(recovery_required());
            }
            let candidate_git = match &root_git {
                GitEntryProof::File(file) => file,
                GitEntryProof::Directory(_) => return Err(recovery_required()),
            };
            if candidate_git.target.as_deref() != Some(admin_path.as_path()) {
                return Err(recovery_required());
            }
            if admin_gitdir
                .as_ref()
                .and_then(|file| file.target.as_deref())
                != Some(root_git_path.as_path())
            {
                return Err(recovery_required());
            }
            if commondir.is_none() {
                // A linked candidate must retain Git's exact common-dir
                // backpointer; a missing file would change administration.
                return Err(recovery_required());
            }
        }
        if matches!(&root_git, GitEntryProof::File(_)) {
            if admin_gitdir
                .as_ref()
                .and_then(|file| file.target.as_deref())
                != Some(root_git_path.as_path())
                || commondir.is_none()
            {
                return Err(recovery_required());
            }
        }
        let common_config = open_git_file(&common.path.join("config"), None, None)?;
        let admin_config_path = admin_path.join("config");
        let admin_config = match existing_regular_path(&admin_config_path)? {
            Some(path) => Some(open_git_file(&path, None, None)?),
            None => None,
        };
        validate_git_config_proof(&common_config, None)?;
        if let Some(config) = &admin_config {
            validate_git_config_proof(config, Some(&root.anchor.canonical_path))?;
        }
        if let Some(config) = &worktree_config {
            validate_git_config_proof(config, Some(&root.anchor.canonical_path))?;
        }
        Ok(Self {
            root_git,
            admin,
            common,
            admin_path,
            common_path,
            admin_gitdir,
            commondir,
            common_config,
            admin_config,
            worktree_config,
            expected_root: root.anchor.canonical_path.clone(),
            expected_admin_name: expected_admin_name.map(OsStr::to_os_string),
        })
    }

    fn revalidate(&self, root: &VerifiedProjectRoot) -> Result<(), AppError> {
        if root.anchor.canonical_path != self.expected_root {
            return Err(recovery_required());
        }
        let root_metadata = root.directory.metadata().map_err(|_| recovery_required())?;
        if !root_metadata.is_dir()
            || !secure_metadata_for_git(&root_metadata)
            || executable_identity_from_metadata(&root_metadata) != root.anchor.identity
        {
            return Err(recovery_required());
        }
        revalidate_git_entry(&self.root_git)?;
        revalidate_git_directory(&self.admin)?;
        revalidate_git_directory(&self.common)?;
        if self.admin.path != self.admin_path || self.common.path != self.common_path {
            return Err(recovery_required());
        }
        if self.expected_admin_name.is_some() {
            let expected_parent = self.common_path.join("worktrees");
            if self.admin_path.parent() != Some(expected_parent.as_path())
                || self.admin_path.file_name() != self.expected_admin_name.as_deref()
            {
                return Err(recovery_required());
            }
            if !matches!(&self.root_git, GitEntryProof::File(_)) {
                return Err(recovery_required());
            }
        }
        revalidate_git_file(&self.common_config, None)?;
        validate_git_config_proof(&self.common_config, None)?;
        if let Some(config) = &self.admin_config {
            revalidate_git_file(config, None)?;
            validate_git_config_proof(config, Some(&self.expected_root))?;
        }
        if let Some(file) = &self.admin_gitdir {
            revalidate_git_file(file, Some(&self.admin_path))?;
            if (self.expected_admin_name.is_some()
                || matches!(&self.root_git, GitEntryProof::File(_)))
                && file.target.as_deref()
                    != Some(root.anchor.canonical_path.join(".git").as_path())
            {
                return Err(recovery_required());
            }
        } else if existing_regular_path(&self.admin_path.join("gitdir"))?.is_some()
            || matches!(&self.root_git, GitEntryProof::File(_))
        {
            return Err(recovery_required());
        }
        if let Some(file) = &self.commondir {
            revalidate_git_file(file, Some(&self.admin_path))?;
            if file.target.as_deref() != Some(self.common_path.as_path()) {
                return Err(recovery_required());
            }
        } else if existing_regular_path(&self.admin_path.join("commondir"))?.is_some()
            || matches!(&self.root_git, GitEntryProof::File(_))
        {
            return Err(recovery_required());
        }
        match (&self.worktree_config, existing_regular_path(&self.admin_path.join("config.worktree"))?) {
            (Some(expected), Some(current)) => {
                if current != expected.path {
                    return Err(recovery_required());
                }
                revalidate_git_file(expected, None)?;
                validate_git_config_proof(expected, Some(&root.anchor.canonical_path))?;
            }
            (None, None) => {}
            _ => return Err(recovery_required()),
        }
        if self.admin_config.is_none()
            && existing_regular_path(&self.admin_path.join("config"))?.is_some()
        {
            return Err(recovery_required());
        }
        if let GitEntryProof::File(file) = &self.root_git {
            revalidate_git_file(file, Some(&root.anchor.canonical_path))?;
            if self.expected_admin_name.is_some()
                && file.target.as_deref() != Some(self.admin_path.as_path())
            {
                return Err(recovery_required());
            }
        }
        Ok(())
    }

    /// Revalidate only the descriptors retained by the manager.  This is the
    /// missing-target path: Git may have removed the candidate pathname and
    /// its administration entries, so reopening those pathnames would turn a
    /// legitimate idempotent cleanup into a pathname race.  The retained
    /// descriptors still prove that none of the backpointers/configuration
    /// bytes were replaced before the entries disappeared.
    fn revalidate_retained_descriptors(&self, root: &VerifiedProjectRoot) -> Result<(), AppError> {
        let metadata = root.directory.metadata().map_err(|_| recovery_required())?;
        if !metadata.is_dir()
            || !secure_metadata_for_git(&metadata)
            || executable_identity_from_metadata(&metadata) != root.anchor.identity
        {
            return Err(recovery_required());
        }
        revalidate_git_entry_descriptor(&self.root_git)?;
        revalidate_git_directory_descriptor(&self.admin)?;
        revalidate_git_directory_descriptor(&self.common)?;
        for file in [self.admin_gitdir.as_ref(), self.commondir.as_ref()] {
            if let Some(file) = file {
                revalidate_git_file_descriptor(file)?;
            }
        }
        revalidate_git_file_descriptor(&self.common_config)?;
        for file in [self.admin_config.as_ref(), self.worktree_config.as_ref()] {
            if let Some(file) = file {
                revalidate_git_file_descriptor(file)?;
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
struct WorktreeManager {
    policy: ResolvedExecutionPolicy,
    project: Project,
    original: ResolvedProjectExecutionPolicy,
    campaign_id: String,
    proposal_id: String,
    worktree_id: String,
    worktree_relative_path: PathBuf,
    base_sha: String,
    worktree_path: PathBuf,
    candidate_identity: Option<ExecutableIdentity>,
    object_id_len: Option<usize>,
    #[cfg(unix)]
    original_repository: Option<GitRepositoryProof>,
    #[cfg(unix)]
    candidate_repository: Option<GitRepositoryProof>,
    #[cfg(unix)]
    worktree_parents: Option<WorktreeParentProof>,
    baseline: Option<GitBaseline>,
}

struct DurableOwnershipProof {
    state_root_identity: String,
    worktrees_identity: String,
    campaign_identity: String,
    candidate_root_identity: String,
    candidate_admin_identity: String,
    candidate_common_identity: String,
    candidate_admin_path: String,
    candidate_common_path: String,
}

#[cfg(unix)]
impl WorktreeManager {
    fn new_for_run(
        policy: &ResolvedExecutionPolicy,
        project: &Project,
        original: &ResolvedProjectExecutionPolicy,
        campaign_id: &str,
        proposal_id: &str,
        base_sha: &str,
        worktree_id: &str,
        worktree_relative_path: &Path,
    ) -> Result<Self, AppError> {
        Self::new_with_ownership(
            policy,
            project,
            original,
            campaign_id,
            proposal_id,
            base_sha,
            worktree_id,
            worktree_relative_path,
        )
    }

    fn new_with_ownership(
        policy: &ResolvedExecutionPolicy,
        project: &Project,
        original: &ResolvedProjectExecutionPolicy,
        campaign_id: &str,
        proposal_id: &str,
        base_sha: &str,
        worktree_id: &str,
        worktree_relative_path: &Path,
    ) -> Result<Self, AppError> {
        validate_internal_id("campaign_id", campaign_id)?;
        validate_internal_id("proposal_id", proposal_id)?;
        validate_internal_id("worktree_id", worktree_id)?;
        canonical_full_sha(base_sha)?;
        if worktree_relative_path != owned_worktree_relative_path(campaign_id, proposal_id)? {
            return Err(validation(
                "code_change.worktree_relative_path",
                "does not identify the exact campaign proposal worktree",
            ));
        }
        if original.project_id != project.project_id {
            return Err(validation(
                "code_change.project",
                "does not match the original project policy",
            ));
        }
        let worktree_path = policy
            .code_change_state_root_path()
            .join("worktrees")
            .join(campaign_id)
            .join(proposal_id);
        Ok(Self {
            policy: policy.clone(),
            project: project.clone(),
            original: original.clone(),
            campaign_id: campaign_id.to_owned(),
            proposal_id: proposal_id.to_owned(),
            worktree_id: worktree_id.to_owned(),
            worktree_relative_path: worktree_relative_path.to_owned(),
            base_sha: base_sha.to_owned(),
            worktree_path,
            candidate_identity: None,
            object_id_len: None,
            #[cfg(unix)]
            original_repository: None,
            #[cfg(unix)]
            candidate_repository: None,
            #[cfg(unix)]
            worktree_parents: None,
            baseline: None,
        })
    }

    #[cfg(unix)]
    fn durable_ownership_proof(
        &self,
        candidate: &VerifiedProjectRoot,
    ) -> Result<DurableOwnershipProof, AppError> {
        let parents = self.worktree_parents.as_ref().ok_or_else(recovery_required)?;
        let repository = self
            .candidate_repository
            .as_ref()
            .ok_or_else(recovery_required)?;
        let candidate_metadata = candidate.directory.metadata().map_err(|_| recovery_required())?;
        if !candidate_metadata.is_dir()
            || executable_identity_from_metadata(&candidate_metadata) != candidate.anchor.identity
        {
            return Err(recovery_required());
        }
        // Read identities from the retained descriptors rather than
        // reopening replaceable administration pathnames.  This remains
        // usable for an authorized idempotent cleanup after the worktree
        // pathname has disappeared.
        if directory_identity(&repository.admin.directory)? != repository.admin.identity
            || directory_identity(&repository.common.directory)? != repository.common.identity
        {
            return Err(recovery_required());
        }
        Ok(DurableOwnershipProof {
            state_root_identity: identity_token(parents.state_root_identity),
            worktrees_identity: identity_token(parents.worktrees_identity),
            campaign_identity: identity_token(parents.campaign_identity),
            candidate_root_identity: identity_token(candidate.anchor.identity),
            candidate_admin_identity: identity_token(repository.admin.identity),
            candidate_common_identity: identity_token(repository.common.identity),
            candidate_admin_path: repository.admin_path.to_string_lossy().into_owned(),
            candidate_common_path: repository.common_path.to_string_lossy().into_owned(),
        })
    }

    async fn inspect_base(&mut self) -> Result<(), AppError> {
        self.policy.verify_code_change_state_root()?;
        let original_root = self.original.root_anchor.verify_identity()?;
        if original_root.anchor.canonical_path != self.project.root_path {
            return Err(validation(
                "code_change.root",
                "does not match the registered project root",
            ));
        }
        validate_local_git_metadata(&original_root.anchor.canonical_path)?;
        #[cfg(unix)]
        {
            self.original_repository = Some(GitRepositoryProof::capture_original(&original_root)?);
        }
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        let top_level = self
            .git(
                &original_root,
                &working_directory,
                &["rev-parse", "--show-toplevel"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&top_level, "inspect Git project root")?;
        let top_level = bounded_utf8_line(&top_level.stdout, "git project root")?;
        let top_level = fs::canonicalize(top_level).map_err(|_| AppError::Validation {
            field: "code_change.git",
            message: "Git project root is not readable",
        })?;
        if top_level != original_root.anchor.canonical_path {
            return Err(validation(
                "code_change.git",
                "Git project root does not match the registered root",
            ));
        }

        let revision = self
            .git(
                &original_root,
                &working_directory,
                &["rev-parse", "--verify", "HEAD^{commit}"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&revision, "inspect Git base revision")?;
        let revision = bounded_utf8_line(&revision.stdout, "Git base revision")?;
        if revision != self.base_sha {
            return Err(validation(
                "code_change.base_sha",
                "does not match the current project HEAD",
            ));
        }
        let object_format = self
            .git(
                &original_root,
                &working_directory,
                &["rev-parse", "--show-object-format"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&object_format, "inspect Git object format")?;
        let object_format = bounded_utf8_line(&object_format.stdout, "Git object format")?;
        let object_id_len = match object_format {
            "sha1" => 40,
            "sha256" => 64,
            _ => {
                return Err(validation(
                    "code_change.git",
                    "Git object format is unsupported",
                ))
            }
        };
        if self.base_sha.len() != object_id_len {
            return Err(validation(
                "code_change.base_sha",
                "does not match the repository object ID length",
            ));
        }
        self.object_id_len = Some(object_id_len);

        let status = self
            .git(
                &original_root,
                &working_directory,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&status, "inspect Git base status")?;
        if !status.stdout.is_empty() {
            return Err(validation(
                "code_change.base",
                "must be clean before preparing a candidate",
            ));
        }

        let common = inspect_common_directory(self, &original_root, &working_directory).await?;
        let owned_ref = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?,
        );
        let best_ref_name = format!("refs/heads/{}", best_ref(&self.campaign_id)?);
        let controlled_refs = [owned_ref.as_str(), best_ref_name.as_str()];
        let protected_ref_digest = self
            .protected_ref_digest(&original_root, &working_directory, &controlled_refs)
            .await?;
        let existing_candidate_ref = self
            .git(
                &original_root,
                &working_directory,
                &["show-ref", "--verify", "--quiet", &owned_ref],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if existing_candidate_ref.success {
            return Err(validation(
                "code_change.ref",
                "candidate ref already exists",
            ));
        }
        if existing_candidate_ref.exit_code != Some(1) {
            return Err(validation(
                "code_change.ref",
                "candidate ref could not be inspected",
            ));
        }
        let remote_config_digest = self.remote_config_digest(&original_root, &working_directory).await?;
        self.baseline = Some(GitBaseline {
            #[cfg(unix)]
            repository: self
                .original_repository
                .as_ref()
                .ok_or(AppError::Runtime {
                    operation: "retain original Git administration proof",
                })?
                .clone(),
            common_directory: common,
            protected_ref_digest,
            remote_config_digest,
        });
        Ok(())
    }

    async fn prepare(&mut self) -> Result<VerifiedProjectRoot, AppError> {
        let baseline = self.baseline.clone().ok_or(AppError::Runtime {
            operation: "prepare code-change worktree before base inspection",
        })?;
        self.policy.verify_code_change_state_root()?;
        #[cfg(unix)]
        {
            self.worktree_parents = Some(WorktreeParentProof::retain(
                &self.policy,
                &self.campaign_id,
            )?);
        }
        #[cfg(not(unix))]
        ensure_state_worktree_parents(&self.policy, &self.campaign_id)?;
        let candidate_parent = self
            .policy
            .code_change_state_root_path()
            .join("worktrees")
            .join(&self.campaign_id);
        if path_has_symlink_component(&candidate_parent)? {
            return Err(recovery_required());
        }
        match fs::symlink_metadata(&self.worktree_path) {
            Ok(_) => {
                return Err(validation(
                    "code_change.worktree",
                    "owned candidate path already exists",
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(AppError::Runtime {
                    operation: "inspect owned candidate worktree path",
                })
            }
        }
        let original_root = self.original.root_anchor.verify_identity()?;
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        #[cfg(unix)]
        let path = descriptor_worktree_leaf(&self.proposal_id);
        #[cfg(not(unix))]
        let path = self.worktree_path.as_os_str().to_os_string();
        let base = self.base_sha.clone();
        let args = vec![
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("--detach"),
            path,
            OsString::from(base),
        ];
        #[cfg(unix)]
        let candidate_parent = open_worktree_parent(self)?.ok_or_else(recovery_required)?;
        if let Err(error) = self.validate_original_state().await {
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        #[cfg(unix)]
        let output = self
            .git_os_with_worktree_parent(
                &original_root,
                &working_directory,
                &args,
                MAX_GIT_OUTPUT_BYTES,
                Some(candidate_parent),
            )
            .await;
        #[cfg(not(unix))]
        let output = self
            .git_os(&original_root, &working_directory, &args, MAX_GIT_OUTPUT_BYTES)
            .await;
        let output = output?;
        if !output.success {
            let error = validation(
                "code_change.worktree",
                "pinned Git could not create the detached candidate",
            );
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        if let Err(error) = self.validate_original_state().await {
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        let candidate_anchor = match ProjectRootAnchor::resolve(&self.worktree_path) {
            Ok(anchor) => anchor,
            Err(error) => return Err(self.cleanup_after_prepare_error(error.into()).await),
        };
        let candidate = match candidate_anchor.verify_identity() {
            Ok(candidate) => candidate,
            Err(error) => return Err(self.cleanup_after_prepare_error(error.into()).await),
        };
        #[cfg(unix)]
        {
            let original_repository = baseline.repository.clone();
            let repository = match GitRepositoryProof::capture_candidate(
                &candidate,
                &original_repository,
            ) {
                Ok(repository) => repository,
                Err(error) => return Err(self.cleanup_after_prepare_error(error).await),
            };
            self.candidate_repository = Some(repository);
        }
        self.candidate_identity = Some(candidate.anchor.identity);
        if candidate.anchor.canonical_path != self.worktree_path {
            let error = validation(
                "code_change.worktree",
                "candidate path escaped the retained state root",
            );
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        if baseline.common_directory
            != match inspect_common_directory(self, &original_root, &working_directory).await {
                Ok(common) => common,
                Err(error) => return Err(self.cleanup_after_prepare_error(error).await),
            }
        {
            let error = validation(
                "code_change.git",
                "Git common directory changed while preparing the candidate",
            );
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        let owned_ref = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?,
        );
        let best_ref_name = format!("refs/heads/{}", best_ref(&self.campaign_id)?);
        let controlled_refs = [owned_ref.as_str(), best_ref_name.as_str()];
        let protected_ref_digest = match self
            .protected_ref_digest(&original_root, &working_directory, &controlled_refs)
            .await
        {
            Ok(digest) => digest,
            Err(error) => return Err(self.cleanup_after_prepare_error(error).await),
        };
        if protected_ref_digest != baseline.protected_ref_digest {
            let error = recovery_required();
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        let remote_config_digest = match self
            .remote_config_digest(&original_root, &working_directory)
            .await
        {
            Ok(digest) => digest,
            Err(error) => return Err(self.cleanup_after_prepare_error(error).await),
        };
        if remote_config_digest != baseline.remote_config_digest {
            let error = recovery_required();
            return Err(self.cleanup_after_prepare_error(error).await);
        }
        let parents = self.worktree_parents.as_ref().ok_or_else(recovery_required)?;
        let candidate = match candidate.mark_code_change_owned(
            self.policy.code_change_state_root_identity(),
            parents.worktrees_identity,
            parents.campaign_identity,
            parents.state_root.clone(),
            parents.worktrees.clone(),
            parents.campaign.clone(),
            &self.worktree_path,
            &self.worktree_relative_path,
            &self.campaign_id,
            &self.proposal_id,
            &self.worktree_id,
        ) {
            Ok(candidate) => candidate,
            Err(error) => return Err(self.cleanup_after_prepare_error(error.into()).await),
        };
        Ok(candidate)
    }

    async fn cleanup_after_prepare_error(&self, original: AppError) -> AppError {
        match self.cleanup(None, None, false).await {
            Ok(()) => original,
            Err(cleanup) => cleanup,
        }
    }

    async fn verify(&mut self, candidate: &VerifiedProjectRoot) -> Result<DiffFacts, AppError> {
        let validator = CandidateValidator::new(self, candidate)?;
        validator.verify().await
    }

    async fn cleanup(
        &self,
        candidate: Option<&VerifiedProjectRoot>,
        expected_candidate_sha: Option<&str>,
        allow_missing_target: bool,
    ) -> Result<(), AppError> {
        self.policy.verify_code_change_state_root()?;
        let expected_path = self
            .policy
            .code_change_state_root_path()
            .join("worktrees")
            .join(&self.campaign_id)
            .join(&self.proposal_id);
        if expected_path != self.worktree_path {
            return Err(validation(
                "code_change.cleanup",
                "candidate path is not owned by this run",
            ));
        }
        if let Some(candidate) = candidate {
            if candidate.anchor.canonical_path != expected_path {
                return Err(recovery_required());
            }
            candidate.verify_code_change_ownership(
                self.policy.code_change_state_root_identity(),
                self.worktree_parents
                    .as_ref()
                    .map(|parents| parents.worktrees_identity)
                    .ok_or_else(recovery_required)?,
                self.worktree_parents
                    .as_ref()
                    .map(|parents| parents.campaign_identity)
                    .ok_or_else(recovery_required)?,
                &expected_path,
                &self.worktree_relative_path,
                &self.campaign_id,
                &self.proposal_id,
                &self.worktree_id,
            )?;
        } else if self.candidate_identity.is_none() {
            // A cleanup without a candidate capability is only valid while
            // rolling back a preparation that never produced a verified root.
            // Durable cleanup callers must retain the manager-issued root
            // capability above; a missing DB ownership row is not success.
            if expected_candidate_sha.is_some() {
                return Err(recovery_required());
            }
        }
        #[cfg(unix)]
        let candidate_parent = open_worktree_parent(self)?;
        #[cfg(unix)]
        let Some(candidate_parent) = candidate_parent else {
            return Err(recovery_required());
        };
        #[cfg(unix)]
        let candidate_parent_identity = directory_identity(&candidate_parent)?;
        #[cfg(unix)]
        if !directory_entry_exists(&candidate_parent, OsStr::new(&self.proposal_id))? {
            self.validate_original_state().await?;
            if let Some(repository) = self.candidate_repository.as_ref() {
                // Preserve the descriptor-backed proof even though the
                // candidate pathname itself is gone.  Any changed retained
                // metadata is an orphan/replacement and must fail closed.
                repository.revalidate_retained_descriptors(
                    candidate.ok_or_else(recovery_required)?,
                )?;
            }
            if self.candidate_ref_exists().await?
                || self.candidate_admin_registered_elsewhere()?
            {
                return Err(recovery_required());
            }
            return if allow_missing_target {
                Ok(())
            } else {
                Err(recovery_required())
            };
        }
        if path_has_symlink_component(&self.worktree_path)? {
            return Err(recovery_required());
        }
        let expected_identity = candidate
            .map(|candidate| candidate.anchor.identity)
            .or(self.candidate_identity);
        let original_root = self.original.root_anchor.verify_identity()?;
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        let current = match fs::symlink_metadata(&self.worktree_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(recovery_required());
                }
                Some(ProjectRootAnchor::resolve(&self.worktree_path)?.verify_identity()?)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => return Err(recovery_required()),
        };
        if let Some(current) = current.as_ref() {
            if expected_identity != Some(current.anchor.identity) {
                return Err(recovery_required());
            }
            self.validate_git_boundary(current)?;
            validate_local_git_metadata(&current.anchor.canonical_path)?;
            let current_common = inspect_common_directory(
                self,
                current,
                &VerifiedWorkingDirectory::root(current)?,
            )
            .await?;
            if self
                .baseline
                .as_ref()
                .is_some_and(|baseline| baseline.common_directory != current_common)
            {
                return Err(recovery_required());
            }
            let excluded_ref = format!(
                "refs/heads/{}",
                candidate_ref(&self.campaign_id, &self.proposal_id)?,
            );
            let best_ref_name = format!("refs/heads/{}", best_ref(&self.campaign_id)?);
            let controlled_refs = [excluded_ref.as_str(), best_ref_name.as_str()];
            let current_refs = self
                .protected_ref_digest(
                    &original_root,
                    &working_directory,
                    &controlled_refs,
                )
                .await?;
            if self
                .baseline
                .as_ref()
                .is_some_and(|baseline| baseline.protected_ref_digest != current_refs)
            {
                return Err(recovery_required());
            }
            let current_remote = self
                .remote_config_digest(&original_root, &working_directory)
                .await?;
            if self
                .baseline
                .as_ref()
                .is_some_and(|baseline| baseline.remote_config_digest != current_remote)
            {
                return Err(recovery_required());
            }
            let expected_sha = expected_candidate_sha.unwrap_or(&self.base_sha);
            let actual = self
                .git(
                    current,
                    &VerifiedWorkingDirectory::root(current)?,
                    &["rev-parse", "--verify", "HEAD^{commit}"],
                    MAX_GIT_OUTPUT_BYTES,
                )
                .await?;
            require_success(&actual, "verify candidate HEAD before cleanup")?;
            if bounded_utf8_line(&actual.stdout, "candidate HEAD")? != expected_sha {
                return Err(recovery_required());
            }
            if let Some(candidate_sha) = expected_candidate_sha {
                let reference = format!(
                    "refs/heads/{}",
                    candidate_ref(&self.campaign_id, &self.proposal_id)?
                );
                let candidate_ref_output = self
                    .git(
                        current,
                        &VerifiedWorkingDirectory::root(current)?,
                        &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                        MAX_GIT_OUTPUT_BYTES,
                    )
                    .await?;
                require_success(&candidate_ref_output, "verify candidate ref before cleanup")?;
                if bounded_utf8_line(&candidate_ref_output.stdout, "candidate ref before cleanup")?
                    != candidate_sha
                {
                    return Err(recovery_required());
                }
            }
        }
        if current.is_none() {
            // The durable parent entry proves that Git still registered a
            // candidate administration directory.  A missing worktree root
            // in that state is a moved/orphaned target, not an idempotent
            // successful cleanup.
            return if allow_missing_target {
                Ok(())
            } else {
                Err(recovery_required())
            };
        }
        let current_before_remove = ProjectRootAnchor::resolve(&self.worktree_path)?
            .verify_identity()?;
        if expected_identity != Some(current_before_remove.anchor.identity) {
            return Err(recovery_required());
        }
        self.validate_git_boundary(&current_before_remove)?;
        self.validate_original_state().await?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let candidate_directory = current_before_remove
            .directory
            .try_clone()
            .map_err(|_| recovery_required())?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        if directory_identity(&candidate_directory)? != current_before_remove.anchor.identity {
            return Err(recovery_required());
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let cleanup_path = descriptor_worktree_leaf(&self.proposal_id);
        #[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
        let cleanup_path = descriptor_worktree_leaf(&self.proposal_id);
        #[cfg(not(unix))]
        let cleanup_path = self.worktree_path.as_os_str().to_os_string();
        let cleanup_args = vec![
            OsString::from("worktree"),
            OsString::from("remove"),
            OsString::from("--force"),
            cleanup_path,
        ];
        #[cfg(any(target_os = "linux", target_os = "android"))]
        let output = self
            .git_os_with_worktree_parent(
                &original_root,
                &working_directory,
                &cleanup_args,
                MAX_GIT_OUTPUT_BYTES,
                Some(candidate_parent.try_clone().map_err(|_| recovery_required())?),
            )
            .await?;
        #[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
        let output = self
            .git_os(
                &original_root,
                &working_directory,
                &cleanup_args,
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        #[cfg(not(unix))]
        let output = self
            .git_os(
                &original_root,
                &working_directory,
                &cleanup_args,
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success {
            return Err(recovery_required());
        }
        if let Some(candidate_sha) = expected_candidate_sha {
            self.remove_candidate_ref(candidate_sha).await?;
        }
        self.validate_original_state().await?;
        #[cfg(unix)]
        {
            let reopened_parent = open_worktree_parent(self)?
                .ok_or_else(recovery_required)?;
            if directory_identity(&reopened_parent)? != candidate_parent_identity {
                return Err(recovery_required());
            }
            if directory_entry_exists(&reopened_parent, OsStr::new(&self.proposal_id))? {
                return Err(recovery_required());
            }
            Ok(())
        }
        #[cfg(not(unix))]
        match fs::symlink_metadata(&self.worktree_path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Ok(_) | Err(_) => Err(recovery_required()),
        }
    }

    async fn remove_candidate_ref(&self, expected_sha: &str) -> Result<(), AppError> {
        let reference = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?
        );
        let original_root = self.original.root_anchor.verify_identity()?;
        let output = self
            .git(
                &original_root,
                &VerifiedWorkingDirectory::root(&original_root)?,
                &["update-ref", "-d", &reference, expected_sha],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success {
            return Err(recovery_required());
        }
        let remaining = self
            .git(
                &original_root,
                &VerifiedWorkingDirectory::root(&original_root)?,
                &["show-ref", "--verify", "--quiet", &reference],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if remaining.success || remaining.exit_code != Some(1) {
            return Err(recovery_required());
        }
        Ok(())
    }

    async fn candidate_ref_exists(&self) -> Result<bool, AppError> {
        let reference = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?
        );
        let original_root = self.original.root_anchor.verify_identity()?;
        let output = self
            .git(
                &original_root,
                &VerifiedWorkingDirectory::root(&original_root)?,
                &["show-ref", "--verify", "--quiet", &reference],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if output.success {
            return Ok(true);
        }
        if output.exit_code == Some(1) {
            return Ok(false);
        }
        Err(recovery_required())
    }

    #[cfg(unix)]
    fn candidate_admin_registered_elsewhere(&self) -> Result<bool, AppError> {
        let Some(repository) = self.original_repository.as_ref() else {
            return Err(recovery_required());
        };
        let worktrees = match open_existing_directory_at(
            &repository.common.directory,
            OsStr::new("worktrees"),
        ) {
            Ok(worktrees) => worktrees,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(_) => return Err(recovery_required()),
        };
        let mut count = 0usize;
        for entry in
            fs::read_dir(descriptor_path(&worktrees)).map_err(|_| recovery_required())?
        {
            count = count.saturating_add(1);
            if count > MAX_STATUS_PATHS {
                return Err(recovery_required());
            }
            let entry = entry.map_err(|_| recovery_required())?;
            let name = entry.file_name();
            let admin = match open_existing_directory_at(&worktrees, &name) {
                Ok(admin) => admin,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(_) => return Err(recovery_required()),
            };
            let Some(gitdir) =
                read_bounded_entry_at(&admin, OsStr::new("gitdir"), "git.metadata")?
            else {
                continue;
            };
            let target = parse_git_pointer(&gitdir, &descriptor_path(&admin), GitPointerKind::Path)?;
            if target == self.worktree_path.join(".git") {
                return Ok(true);
            }
        }
        Ok(false)
    }

    #[cfg(not(unix))]
    fn candidate_admin_registered_elsewhere(&self) -> Result<bool, AppError> {
        Err(recovery_required())
    }

    async fn cleanup_authorized(
        &self,
        candidate: Option<&VerifiedProjectRoot>,
        expected_candidate_sha: Option<&str>,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let run = authorization.fresh_run()?;
        self.verify_cleanup_run(&run, expected_candidate_sha, candidate, authorization)?;
        self.cleanup(candidate, expected_candidate_sha, true).await?;
        // Re-query after the filesystem boundary and persist the outcome
        // through the repository.  A concurrent DB change therefore cannot
        // be hidden by an in-memory authorization row.
        let latest = authorization.fresh_run()?;
        self.verify_cleanup_run(&latest, expected_candidate_sha, candidate, authorization)?;
        CodeChangeRepository::new(&authorization.db)
            .finish_cleanup(&latest.code_change_run_id, unix_timestamp()?)?;
        Ok(())
    }

    fn verify_cleanup_run(
        &self,
        run: &CodeChangeRun,
        expected_candidate_sha: Option<&str>,
        candidate: Option<&VerifiedProjectRoot>,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let expected_candidate_ref = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?,
        );
        let expected_best_ref =
            format!("refs/heads/{}", best_ref(&self.campaign_id)?);
        if run.campaign_id != self.campaign_id
            || run.proposal_id != self.proposal_id
            || run.worktree_id != self.worktree_id
            || Path::new(&run.worktree_relative_path) != self.worktree_relative_path.as_path()
            || run.base_sha != self.base_sha
            || run.candidate_sha.as_deref() != expected_candidate_sha
            || run.candidate_ref != expected_candidate_ref
            || run.best_ref != expected_best_ref
        {
            return Err(recovery_required());
        }
        verify_durable_run_scope(&authorization.db, run, &self.project)?;
        self.verify_durable_ownership_proof(run, candidate)?;
        if !matches!(
            run.state,
            CodeChangeState::CandidateReady
                | CodeChangeState::ExperimentSubmitted
                | CodeChangeState::Evaluated
                | CodeChangeState::CleanupPending
                | CodeChangeState::Completed
                | CodeChangeState::Rejected
                | CodeChangeState::RecoveryRequired
        ) {
            return Err(recovery_required());
        }
        if let Some(experiment_id) = run.experiment_id.as_deref() {
            let experiment = crate::db::ExperimentRepository::new(&authorization.db)
                .find_by_id(experiment_id)?
                .ok_or_else(recovery_required)?;
            if experiment.campaign_id != run.campaign_id
                || experiment.proposal_id != run.proposal_id
                || experiment.code_change_run_id.as_deref() != Some(&run.code_change_run_id)
                || !matches!(
                    experiment.status,
                    ExperimentStatus::Succeeded
                        | ExperimentStatus::Failed
                        | ExperimentStatus::Cancelled
                )
            {
                return Err(recovery_required());
            }
        }
        let connection = authorization.db.connect()?;
        let mut authoritative_rows = 0usize;
        if let Some(experiment_id) = run.experiment_id.as_deref() {
            let mut statement = connection
                .prepare(
                    "SELECT ar.status, ar.pid, s.project_id, ar.project_id
                     FROM experiments AS e
                     JOIN submissions AS s ON s.submission_id = e.submission_id
                     JOIN agent_runs AS ar ON ar.run_id = s.origin_agent_run_id
                     WHERE e.experiment_id = ?1",
                )
                .map_err(crate::db::database_error("inspect code-change experiment liveness"))?;
            let rows = statement
                .query_map([experiment_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(crate::db::database_error("read code-change experiment liveness"))?;
            let mut lineage_rows = 0;
            for row in rows {
                lineage_rows += 1;
                let (status, pid, submission_project, agent_project) = row
                    .map_err(crate::db::database_error("read code-change experiment liveness"))?;
                if submission_project != self.project.project_id
                    || agent_project != self.project.project_id
                {
                    return Err(recovery_required());
                }
                if !matches!(
                    status.as_str(),
                    "starting"
                        | "running"
                        | "completed"
                        | "failed"
                        | "timed_out"
                        | "cancelled"
                ) {
                    return Err(recovery_required());
                }
                if matches!(status.as_str(), "starting" | "running") && pid.is_none() {
                    return Err(recovery_required());
                }
                if pid.is_some_and(process_is_alive) {
                    return Err(recovery_required());
                }
            }
            if lineage_rows != 1 {
                // A terminal experiment without a resolvable origin run is
                // not enough durable evidence that no live process remains.
                return Err(recovery_required());
            }
            authoritative_rows = authoritative_rows.saturating_add(lineage_rows);
        }
        let mut statement = connection
            .prepare(
                "SELECT a.status, r.pid, r.project_id, r.run_id
                 FROM code_change_editor_attempts AS a
                 LEFT JOIN agent_runs AS r ON r.run_id = a.agent_run_id
                 WHERE a.code_change_run_id = ?1",
            )
            .map_err(crate::db::database_error("inspect code-change editor liveness"))?;
        let rows = statement
            .query_map([&run.code_change_run_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })
            .map_err(crate::db::database_error("read code-change editor liveness"))?;
        for row in rows {
            let (status, pid, agent_project, agent_run_id) = row
                .map_err(crate::db::database_error("read code-change editor liveness"))?;
            if agent_run_id.is_none() || agent_project.as_deref() != Some(self.project.project_id.as_str()) {
                return Err(recovery_required());
            }
            if !matches!(status.as_str(), "reserved" | "running" | "ready" | "failed") {
                return Err(recovery_required());
            }
            if matches!(status.as_str(), "reserved" | "running") && pid.is_none() {
                return Err(recovery_required());
            }
            if pid.is_some_and(process_is_alive) {
                return Err(recovery_required());
            }
            authoritative_rows = authoritative_rows.saturating_add(1);
        }
        if authoritative_rows == 0 {
            // Absence of an experiment/editor lineage row is not evidence
            // that no process is live; cleanup must fail closed.
            return Err(recovery_required());
        }
        Ok(())
    }

    #[cfg(unix)]
    fn verify_durable_ownership_proof(
        &self,
        run: &CodeChangeRun,
        candidate: Option<&VerifiedProjectRoot>,
    ) -> Result<(), AppError> {
        let candidate = candidate.ok_or_else(recovery_required)?;
        let proof = self.durable_ownership_proof(candidate)?;
        if run.state_root_identity.as_deref() != Some(proof.state_root_identity.as_str())
            || run.worktrees_identity.as_deref() != Some(proof.worktrees_identity.as_str())
            || run.campaign_identity.as_deref() != Some(proof.campaign_identity.as_str())
            || run.candidate_root_identity.as_deref()
                != Some(proof.candidate_root_identity.as_str())
            || run.candidate_admin_identity.as_deref()
                != Some(proof.candidate_admin_identity.as_str())
            || run.candidate_common_identity.as_deref()
                != Some(proof.candidate_common_identity.as_str())
            || run.candidate_admin_path.as_deref() != Some(proof.candidate_admin_path.as_str())
            || run.candidate_common_path.as_deref() != Some(proof.candidate_common_path.as_str())
        {
            return Err(recovery_required());
        }
        Ok(())
    }

    async fn git(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        args: &[&str],
        cap: usize,
    ) -> Result<BoundedToolOutput, AppError> {
        let args = args.iter().map(|arg| OsString::from(*arg)).collect::<Vec<_>>();
        self.git_os(root, working_directory, &args, cap).await
    }

    async fn git_os(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        args: &[OsString],
        cap: usize,
    ) -> Result<BoundedToolOutput, AppError> {
        self.git_os_with_worktree_parent(root, working_directory, args, cap, None)
            .await
    }

    async fn git_os_with_worktree_parent(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        args: &[OsString],
        cap: usize,
        owned_worktree_parent: Option<File>,
    ) -> Result<BoundedToolOutput, AppError> {
        self.validate_git_boundary(root)?;
        let argv = args.to_vec();
        let environment = SanitizedEnvironment::for_code_change_tool(&self.policy)?;
        let worktree_administration = is_worktree_administration(args);
        if !worktree_administration && owned_worktree_parent.is_some() {
            return Err(recovery_required());
        }
        let parent = if worktree_administration {
            match owned_worktree_parent {
                Some(parent) => Some(parent),
                None => Some(open_worktree_parent(self)?.ok_or_else(recovery_required)?),
            }
        } else {
            None
        };
        if let Some(parent) = parent.as_ref() {
            let expected = self
                .worktree_parents
                .as_ref()
                .ok_or_else(recovery_required)?
                .campaign_identity;
            if directory_identity(parent)? != expected {
                return Err(recovery_required());
            }
        }
        let command_working_directory = if let Some(parent) = parent.as_ref() {
            let parent_path = self
                .worktree_parents
                .as_ref()
                .ok_or_else(recovery_required)?
                .campaign_path
                .clone();
            VerifiedWorkingDirectory::from_owned_descriptor(
                parent.try_clone().map_err(|_| recovery_required())?,
                parent_path,
                root.anchor.identity,
            )?
        } else {
            working_directory.try_clone().map_err(|_| recovery_required())?
        };
        self.run_git_owned(
            root,
            &command_working_directory,
            argv,
            cap,
            environment,
            None,
            parent,
            "pinned Git",
        )
        .await
    }

    async fn run_git_owned(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        args: Vec<OsString>,
        cap: usize,
        mut environment: SanitizedEnvironment,
        temporary_index: Option<Arc<OwnedTemporaryIndex>>,
        worktree_parent: Option<File>,
        summary: &'static str,
    ) -> Result<BoundedToolOutput, AppError> {
        if let Some(index) = temporary_index.as_ref() {
            index.verify_before_command()?;
        }
        self.validate_git_boundary(root)?;
        let anchor = self.policy.code_change_git_anchor().ok_or(validation(
            "code_change.git",
            "pinned Git is unavailable",
        ))?.clone();
        apply_git_descriptor_environment(&mut environment);
        let git_directories = self.git_directories(root, worktree_parent)?;
        let result = BoundedToolRunner::new(&self.policy, cap)
            .run(
                anchor,
                root,
                working_directory,
                pinned_git_argv(&args),
                environment,
                summary,
                temporary_index.clone(),
                Some(git_directories),
            )
            .await;
        let index_state = match temporary_index.as_ref() {
            Some(index) => index.record_after_command(),
            None => Ok(()),
        };
        let boundary = self.validate_git_boundary(root);
        match (result, index_state, boundary) {
            (_, Err(error), _) => Err(error),
            (Err(error), Ok(()), Ok(())) => Err(error),
            (Ok(output), Ok(()), Ok(())) => Ok(output),
            (_, Ok(()), Err(error)) => Err(error),
        }
    }

    #[cfg(unix)]
    fn git_directories(
        &self,
        root: &VerifiedProjectRoot,
        worktree_parent: Option<File>,
    ) -> Result<VerifiedGitDirectories, AppError> {
        let proof = if root.anchor.canonical_path == self.project.root_path {
            self.original_repository.as_ref()
        } else if root.anchor.canonical_path == self.worktree_path {
            self.candidate_repository.as_ref()
        } else {
            None
        }
        .ok_or_else(recovery_required)?;
        proof.revalidate(root)?;
        let admin = proof
            .admin
            .directory
            .try_clone()
            .map_err(|_| recovery_required())?;
        let common = proof
            .common
            .directory
            .try_clone()
            .map_err(|_| recovery_required())?;
        let worktree_parent_identity = worktree_parent
            .as_ref()
            .map(directory_identity)
            .transpose()?;
        Ok(VerifiedGitDirectories {
            admin,
            admin_identity: proof.admin.identity,
            common,
            common_identity: proof.common.identity,
            worktree_parent,
            worktree_parent_identity,
        })
    }

    fn validate_git_boundary(&self, root: &VerifiedProjectRoot) -> Result<(), AppError> {
        #[cfg(unix)]
        {
            let proof = if root.anchor.canonical_path == self.project.root_path {
                self.original_repository.as_ref()
            } else if root.anchor.canonical_path == self.worktree_path {
                self.candidate_repository.as_ref()
            } else {
                None
            }
            .ok_or_else(recovery_required)?;
            proof.revalidate(root)
        }
        #[cfg(not(unix))]
        {
            let _ = root;
            Ok(())
        }
    }

    fn zero_object_id(&self) -> Result<String, AppError> {
        let length = self.object_id_len.ok_or(AppError::Runtime {
            operation: "read Git object ID format before ref mutation",
        })?;
        Ok("0".repeat(length))
    }

    async fn protected_ref_digest(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        excluded_refs: &[&str],
    ) -> Result<String, AppError> {
        let output = self
            .git(
                root,
                working_directory,
                &["for-each-ref", "--format=%(refname)%00%(objectname)%00"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "inspect protected Git refs")?;
        digest_protected_refs(&output.stdout, excluded_refs)
    }

    async fn remote_config_digest(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
    ) -> Result<String, AppError> {
        let output = self
            .git(
                root,
                working_directory,
                &["config", "--null", "--local", "--get-regexp", "^remote\\."],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if output.success {
            return Ok(sha256_hex(&output.stdout));
        }
        if output.exit_code == Some(1) && output.stdout.is_empty() {
            return Ok(sha256_hex(&[]));
        }
        Err(validation(
            "code_change.git",
            "remote configuration could not be inspected",
        ))
    }

    async fn validate_original_state(&self) -> Result<(), AppError> {
        let baseline = self.baseline.as_ref().ok_or(AppError::Runtime {
            operation: "validate original Git state before base inspection",
        })?;
        let original_root = self.original.root_anchor.verify_identity()?;
        self.validate_git_boundary(&original_root)?;
        validate_local_git_metadata(&original_root.anchor.canonical_path)?;
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        let head = self
            .git(
                &original_root,
                &working_directory,
                &["rev-parse", "--verify", "HEAD^{commit}"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&head, "verify original Git HEAD")?;
        if bounded_utf8_line(&head.stdout, "original HEAD")? != self.base_sha {
            return Err(recovery_required());
        }
        let status = self
            .git(
                &original_root,
                &working_directory,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&status, "verify original Git cleanliness")?;
        if !status.stdout.is_empty() {
            return Err(recovery_required());
        }
        let common = inspect_common_directory(self, &original_root, &working_directory).await?;
        if common != baseline.common_directory {
            return Err(recovery_required());
        }
        let excluded_ref = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?,
        );
        let best_ref_name = format!("refs/heads/{}", best_ref(&self.campaign_id)?);
        let controlled_refs = [excluded_ref.as_str(), best_ref_name.as_str()];
        let refs = self
            .protected_ref_digest(&original_root, &working_directory, &controlled_refs)
            .await?;
        if refs != baseline.protected_ref_digest {
            return Err(recovery_required());
        }
        let remote = self
            .remote_config_digest(&original_root, &working_directory)
            .await?;
        if remote != baseline.remote_config_digest {
            return Err(recovery_required());
        }
        Ok(())
    }
}

#[cfg(unix)]
struct CandidateValidator<'a> {
    manager: &'a WorktreeManager,
    candidate: &'a VerifiedProjectRoot,
}

#[cfg(unix)]
impl<'a> CandidateValidator<'a> {
    fn new(
        manager: &'a WorktreeManager,
        candidate: &'a VerifiedProjectRoot,
    ) -> Result<Self, AppError> {
        if candidate.anchor.canonical_path != manager.worktree_path {
            return Err(recovery_required());
        }
        candidate.verify_code_change_ownership(
            manager.policy.code_change_state_root_identity(),
            manager
                .worktree_parents
                .as_ref()
                .map(|parents| parents.worktrees_identity)
                .ok_or_else(recovery_required)?,
            manager
                .worktree_parents
                .as_ref()
                .map(|parents| parents.campaign_identity)
                .ok_or_else(recovery_required)?,
            &manager.worktree_path,
            &manager.worktree_relative_path,
            &manager.campaign_id,
            &manager.proposal_id,
            &manager.worktree_id,
        )?;
        Ok(Self { manager, candidate })
    }

    async fn verify(&self) -> Result<DiffFacts, AppError> {
        self.manager.policy.verify_code_change_state_root()?;
        self.manager.validate_original_state().await?;
        let candidate = self.candidate.anchor.verify_identity()?;
        self.manager.validate_git_boundary(&candidate)?;
        let working_directory = VerifiedWorkingDirectory::root(&candidate)?;
        validate_local_git_metadata(&candidate.anchor.canonical_path)?;
        let baseline = self.manager.baseline.as_ref().ok_or(AppError::Runtime {
            operation: "verify candidate before base inspection",
        })?;
        let common = inspect_common_directory(
            self.manager,
            &candidate,
            &working_directory,
        )
        .await?;
        if common != baseline.common_directory {
            return Err(recovery_required());
        }
        let head = self
            .manager
            .git(
                &candidate,
                &working_directory,
                &["rev-parse", "--verify", "HEAD^{commit}"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&head, "verify candidate base")?;
        if bounded_utf8_line(&head.stdout, "candidate HEAD")? != self.manager.base_sha {
            return Err(recovery_required());
        }
        let remote = self
            .manager
            .remote_config_digest(&candidate, &working_directory)
            .await?;
        if remote != baseline.remote_config_digest {
            return Err(recovery_required());
        }
        let candidate_ref_name = format!(
            "refs/heads/{}",
            candidate_ref(&self.manager.campaign_id, &self.manager.proposal_id)?,
        );
        let best_ref_name = format!("refs/heads/{}", best_ref(&self.manager.campaign_id)?);
        let controlled_refs = [candidate_ref_name.as_str(), best_ref_name.as_str()];
        let refs = self
            .manager
            .protected_ref_digest(
                &candidate,
                &working_directory,
                &controlled_refs,
            )
            .await?;
        if refs != baseline.protected_ref_digest {
            return Err(recovery_required());
        }
        let paths = self.changed_paths(&candidate, &working_directory).await?;
        validate_protected_paths(&paths)?;
        self.validate_ignored_protected_paths(&candidate, &working_directory)
            .await?;
        validate_no_nested_repositories(&candidate.anchor.canonical_path, &paths)?;
        self.validate_no_submodules(&candidate, &working_directory, &paths)
            .await?;
        let repository = CandidateRepository::new(self.manager, &candidate)?;
        let facts = repository.diff_facts(&paths).await?;
        validate_diff_limits(facts.file_count, facts.diff_bytes, &self.manager.policy.campaign_limits)?;
        self.manager.validate_original_state().await?;
        let final_candidate = self.candidate.anchor.verify_identity()?;
        self.manager.validate_git_boundary(&final_candidate)?;
        Ok(facts)
    }

    async fn changed_paths(
        &self,
        candidate: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
    ) -> Result<Vec<PathBuf>, AppError> {
        let output = self
            .manager
            .git(
                candidate,
                working_directory,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "enumerate candidate changes")?;
        parse_status_paths(&output.stdout)
    }

    async fn validate_no_submodules(
        &self,
        candidate: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        paths: &[PathBuf],
    ) -> Result<(), AppError> {
        if paths.is_empty() {
            return Ok(());
        }
        let mut args = vec![
            OsString::from("ls-files"),
            OsString::from("--stage"),
            OsString::from("-z"),
            OsString::from("--"),
        ];
        args.extend(paths.iter().map(|path| path.as_os_str().to_os_string()));
        let output = self
            .manager
            .git_os(candidate, working_directory, &args, MAX_GIT_OUTPUT_BYTES)
            .await?;
        require_success(&output, "inspect candidate submodules")?;
        for record in output.stdout.split(|byte| *byte == 0) {
            if record.is_empty() {
                continue;
            }
            let mode = record
                .split(|byte| *byte == b' ')
                .next()
                .unwrap_or_default();
            if mode == b"160000" {
                return Err(validation(
                    "code_change.diff_paths",
                    "submodule changes are not allowed",
                ));
            }
        }
        Ok(())
    }

    async fn validate_ignored_protected_paths(
        &self,
        candidate: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
    ) -> Result<(), AppError> {
        let output = self
            .manager
            .git(
                candidate,
                working_directory,
                &[
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--ignored=matching",
                    "--untracked-files=all",
                ],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "inspect ignored candidate paths")?;
        let ignored = validate_ignored_status_paths(&output.stdout)?;
        validate_ignored_tree(&candidate.anchor.canonical_path, &ignored)?;
        validate_no_nested_repositories(&candidate.anchor.canonical_path, &ignored)
    }
}

#[cfg(unix)]
struct CandidateRepository<'a> {
    manager: &'a WorktreeManager,
    candidate: &'a VerifiedProjectRoot,
}

#[cfg(unix)]
impl<'a> CandidateRepository<'a> {
    fn new(
        manager: &'a WorktreeManager,
        candidate: &'a VerifiedProjectRoot,
    ) -> Result<Self, AppError> {
        if candidate.anchor.canonical_path != manager.worktree_path {
            return Err(recovery_required());
        }
        Ok(Self { manager, candidate })
    }

    async fn diff_facts(&self, paths: &[PathBuf]) -> Result<DiffFacts, AppError> {
        self.manager.validate_original_state().await?;
        let index = Arc::new(OwnedTemporaryIndex::create(&self.manager.policy)?);
        let working_directory = VerifiedWorkingDirectory::root(self.candidate)?;
        let mut environment = SanitizedEnvironment::for_code_change_tool(&self.manager.policy)?;
        environment.with_generated("GIT_INDEX_FILE", index.path.clone());
        let root = self.candidate.try_clone()?;
        let read_tree = self
            .run_git_metadata(
                &root,
                &working_directory,
                &environment,
                &[
                    OsString::from("read-tree"),
                    OsString::from(self.manager.base_sha.clone()),
                ],
                Some(index.clone()),
            )
            .await?;
        require_success(&read_tree, "construct candidate index")?;
        let mut add_args = vec![
            OsString::from("add"),
            OsString::from("-A"),
            OsString::from("--"),
        ];
        add_args.extend(paths.iter().map(|path| path.as_os_str().to_os_string()));
        let add = self
            .run_git_metadata(
                &root,
                &working_directory,
                &environment,
                &add_args,
                Some(index.clone()),
            )
            .await?;
        require_success(&add, "stage candidate changes")?;
        let tree = self
            .run_git_metadata(
                &root,
                &working_directory,
                &environment,
                &[OsString::from("write-tree")],
                Some(index.clone()),
            )
            .await?;
        require_success(&tree, "write candidate tree")?;
        let tree_sha = canonical_full_sha(bounded_utf8_line(&tree.stdout, "candidate tree")?)?;
        self.manager.validate_original_state().await?;
        let diff = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["diff-tree", "--binary", &self.manager.base_sha, &tree_sha],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&diff, "read candidate diff")?;
        self.manager.validate_original_state().await?;
        let mut sorted_paths = paths.to_vec();
        sorted_paths.sort();
        Ok(DiffFacts {
            transient_paths: sorted_paths,
            file_count: paths.len(),
            diff_bytes: diff.stdout.len(),
            tree_sha,
            digest: sha256_hex(&diff.stdout),
        })
    }

    async fn run_git_metadata(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        environment: &SanitizedEnvironment,
        args: &[OsString],
        temporary_index: Option<Arc<OwnedTemporaryIndex>>,
    ) -> Result<BoundedToolOutput, AppError> {
        self.manager
            .run_git_owned(
                root,
                working_directory,
                args.to_vec(),
                MAX_GIT_OUTPUT_BYTES,
                environment.clone(),
                temporary_index,
                None,
                "Git candidate metadata",
            )
            .await
    }

    async fn commit_candidate(&self, expected: &DiffFacts) -> Result<String, AppError> {
        self.manager.validate_original_state().await?;
        let current = self.diff_facts(&expected.transient_paths).await?;
        if current != *expected {
            return Err(recovery_required());
        }
        let working_directory = VerifiedWorkingDirectory::root(self.candidate)?;
        let mut environment = SanitizedEnvironment::for_code_change_tool(&self.manager.policy)?;
        for (name, value) in [
            ("GIT_AUTHOR_NAME", "pueue-agent"),
            ("GIT_AUTHOR_EMAIL", "pueue-agent@localhost"),
            ("GIT_COMMITTER_NAME", "pueue-agent"),
            ("GIT_COMMITTER_EMAIL", "pueue-agent@localhost"),
            ("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z"),
            ("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z"),
        ] {
            environment.with_generated(name, OsString::from(value));
        }
        let anchor = self.manager.policy.code_change_git_anchor().ok_or(validation(
            "code_change.git",
            "pinned Git is unavailable",
        ))?;
        let root = self.candidate.try_clone()?;
        self.manager.validate_git_boundary(&root)?;
        apply_git_descriptor_environment(&mut environment);
        let git_directories = self.manager.git_directories(&root, None)?;
        let output = BoundedToolRunner::new(&self.manager.policy, MAX_GIT_OUTPUT_BYTES)
                .run(
                    anchor.clone(),
                    &root,
                    &working_directory,
                pinned_git_argv(&[
                    OsString::from("commit-tree"),
                    OsString::from(expected.tree_sha.clone()),
                    OsString::from("-p"),
                    OsString::from(self.manager.base_sha.clone()),
                    OsString::from("-m"),
                    OsString::from("pueue-agent code-change candidate"),
                ]),
                environment,
                "Git candidate commit",
                None,
                Some(git_directories),
            )
            .await?;
        self.manager.validate_git_boundary(&root)?;
        require_success(&output, "commit candidate tree")?;
        self.manager.validate_original_state().await?;
        let candidate_sha = canonical_full_sha(bounded_utf8_line(&output.stdout, "candidate commit")?)?;
        let tree = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["rev-parse", "--verify", &format!("{candidate_sha}^{{tree}}")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&tree, "verify candidate tree")?;
        if bounded_utf8_line(&tree.stdout, "candidate tree")? != expected.tree_sha {
            return Err(recovery_required());
        }
        let parent = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["rev-parse", "--verify", &format!("{candidate_sha}^")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&parent, "verify candidate parent")?;
        if bounded_utf8_line(&parent.stdout, "candidate parent")? != self.manager.base_sha {
            return Err(recovery_required());
        }
        self.manager.validate_original_state().await?;
        self.ensure_candidate_ref(&candidate_sha).await?;
        self.verify_candidate_ref(&candidate_sha).await?;
        self.manager.validate_original_state().await?;
        self.manager.validate_git_boundary(self.candidate)?;
        let reset = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["reset", "--hard", &candidate_sha],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&reset, "move candidate worktree to commit")?;
        let status = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&status, "verify clean candidate worktree")?;
        if !status.stdout.is_empty() {
            return Err(recovery_required());
        }
        let head = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["rev-parse", "--verify", "HEAD^{commit}"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&head, "verify final candidate HEAD")?;
        if bounded_utf8_line(&head.stdout, "final candidate HEAD")? != candidate_sha {
            return Err(recovery_required());
        }
        let final_candidate = self.candidate.anchor.verify_identity()?;
        self.manager.validate_git_boundary(&final_candidate)?;
        self.manager.validate_original_state().await?;
        Ok(candidate_sha)
    }

    async fn ensure_candidate_ref(&self, candidate_sha: &str) -> Result<(), AppError> {
        self.manager.validate_original_state().await?;
        let zero = self.manager.zero_object_id()?;
        if candidate_sha.len() != zero.len() {
            return Err(validation(
                "code_change.sha",
                "candidate object ID does not match the repository format",
            ));
        }
        let reference = format!("refs/heads/{}", candidate_ref(&self.manager.campaign_id, &self.manager.proposal_id)?);
        let output = self
            .manager
            .git(
                self.candidate,
                &VerifiedWorkingDirectory::root(self.candidate)?,
                &["update-ref", &reference, candidate_sha, &zero],
                MAX_GIT_OUTPUT_BYTES,
            )
        .await?;
        if output.success {
            return self.manager.validate_original_state().await;
        }
        self.manager.validate_original_state().await?;
        let existing = self
            .manager
            .git(
                self.candidate,
                &VerifiedWorkingDirectory::root(self.candidate)?,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if existing.success && bounded_utf8_line(&existing.stdout, "candidate ref")? == candidate_sha {
            self.manager.validate_original_state().await?;
            Ok(())
        } else {
            Err(recovery_required())
        }
    }

    #[allow(dead_code)]
    async fn verify_candidate_ref(&self, candidate_sha: &str) -> Result<(), AppError> {
        self.manager.validate_original_state().await?;
        let reference = format!("refs/heads/{}", candidate_ref(&self.manager.campaign_id, &self.manager.proposal_id)?);
        let output = self
            .manager
            .git(
                self.candidate,
                &VerifiedWorkingDirectory::root(self.candidate)?,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "verify candidate ref")?;
        if bounded_utf8_line(&output.stdout, "candidate ref")? != candidate_sha {
            return Err(recovery_required());
        }
        self.manager.validate_original_state().await?;
        Ok(())
    }

    async fn update_best_ref_cas_authorized(
        &self,
        authorization: &CodeChangeCleanupAuthorization,
        new_sha: &str,
        expected_old_sha: Option<&str>,
    ) -> Result<(), AppError> {
        let run = authorization.fresh_run()?;
        self.manager
            .verify_cleanup_run(&run, Some(new_sha), Some(self.candidate), authorization)?;
        self.manager.validate_original_state().await?;
        canonical_full_sha(new_sha)?;
        let zero = self.manager.zero_object_id()?;
        if new_sha.len() != zero.len() {
            return Err(validation(
                "code_change.sha",
                "candidate object ID does not match the repository format",
            ));
        }
        let expected = match expected_old_sha {
            Some(value) => {
                canonical_full_sha(value)?;
                if value.len() != zero.len() {
                    return Err(validation(
                        "code_change.sha",
                        "expected object ID does not match the repository format",
                    ));
                }
                value.to_owned()
            }
            None => zero.clone(),
        };
        self.verify_candidate_ref(new_sha).await?;
        let original_root = self.manager.original.root_anchor.verify_identity()?;
        let original_cwd = VerifiedWorkingDirectory::root(&original_root)?;
        let reference = format!("refs/heads/{}", best_ref(&self.manager.campaign_id)?);
        let current = self
            .read_ref(&original_root, &original_cwd, &reference)
            .await?;
        match expected_old_sha {
            Some(expected) if current.as_deref() != Some(expected) => {
                return Err(recovery_required());
            }
            None if current.is_some() => return Err(recovery_required()),
            _ => {}
        }
        let output = self
            .manager
            .git(
                &original_root,
                &original_cwd,
                &["update-ref", &reference, new_sha, &expected],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success {
            return Err(recovery_required());
        }
        let post = match self
            .read_ref(&original_root, &original_cwd, &reference)
            .await
        {
            Ok(post) => post,
            Err(error) => {
                let rollback = self
                    .manager
                    .git(
                        &original_root,
                        &original_cwd,
                        &["update-ref", &reference, &expected, new_sha],
                        MAX_GIT_OUTPUT_BYTES,
                    )
                    .await
                    .ok()
                    .filter(|output| output.success)
                    .is_some();
                if !rollback {
                    return Err(recovery_required());
                }
                return Err(error);
            }
        };
        if post.as_deref() != Some(new_sha) {
            // A concurrent writer changed the ref between the CAS and the
            // postcondition read.  Restore only our exact value with another
            // CAS; never overwrite the concurrent writer's value.
            let _ = self
                .manager
                .git(
                    &original_root,
                    &original_cwd,
                    &["update-ref", &reference, &expected, new_sha],
                    MAX_GIT_OUTPUT_BYTES,
                )
                .await;
            return Err(recovery_required());
        }
        if let Err(error) = self.verify_candidate_ref(new_sha).await {
            let rollback = self
                .manager
                .git(
                    &original_root,
                    &original_cwd,
                    &["update-ref", &reference, &expected, new_sha],
                    MAX_GIT_OUTPUT_BYTES,
                )
                .await
                .ok()
                .filter(|output| output.success)
                .is_some();
            if !rollback {
                return Err(recovery_required());
            }
            return Err(error);
        }
        let boundary = self.manager.validate_original_state().await;
        if let Err(error) = boundary {
            let rollback = self
                .manager
                .git(
                    &original_root,
                    &original_cwd,
                    &["update-ref", &reference, &expected, new_sha],
                    MAX_GIT_OUTPUT_BYTES,
                )
                .await
                .ok()
                .filter(|output| output.success)
                .is_some();
            if !rollback {
                return Err(recovery_required());
            }
            return Err(error);
        }
        self.manager.validate_original_state().await
    }

    async fn read_ref(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        reference: &str,
    ) -> Result<Option<String>, AppError> {
        let output = self
            .manager
            .git(
                root,
                working_directory,
                &["show-ref", "--verify", "--quiet", reference],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success && output.exit_code == Some(1) {
            return Ok(None);
        }
        if !output.success {
            return Err(recovery_required());
        }
        let output = self
            .manager
            .git(
                root,
                working_directory,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "read best ref")?;
        Ok(Some(bounded_utf8_line(&output.stdout, "best ref")?.to_owned()))
    }
}

/// The only child result retained by the code-change coordinator.  The raw
/// streams are consumed by the immediate parser or digest operation and are
/// never included in an error, event, model, or durable record.
#[cfg(unix)]
struct BoundedToolOutput {
    success: bool,
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    output_digest: String,
    summary: &'static str,
}

#[cfg(unix)]
impl std::fmt::Debug for BoundedToolOutput {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedToolOutput")
            .field("success", &self.success)
            .field("exit_code", &self.exit_code)
            .field("stdout_len", &self.stdout.len())
            .field("stderr_len", &self.stderr.len())
            .field("output_digest", &self.output_digest)
            .field("summary", &self.summary)
            .finish()
    }
}

#[cfg(unix)]
#[derive(Clone)]
struct BoundedToolRunner {
    policy: ResolvedExecutionPolicy,
    output_limit: usize,
}

#[cfg(unix)]
struct ToolProcessLease {
    child: Option<crate::process::VerifiedChild>,
    temporary_index: Option<Arc<OwnedTemporaryIndex>>,
}

#[cfg(unix)]
impl ToolProcessLease {
    fn new(
        child: crate::process::VerifiedChild,
        temporary_index: Option<Arc<OwnedTemporaryIndex>>,
    ) -> Self {
        Self {
            child: Some(child),
            temporary_index,
        }
    }

    fn child_mut(&mut self) -> &mut crate::process::VerifiedChild {
        self.child.as_mut().expect("tool process lease owns child")
    }

    fn take_child(&mut self) -> crate::process::VerifiedChild {
        self.child.take().expect("tool process lease owns child")
    }
}

#[cfg(unix)]
impl Drop for ToolProcessLease {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let temporary_index = self.temporary_index.take();
        // Cancellation cannot leave a detached task holding the Git index
        // pathname.  Reap the complete owned group synchronously before the
        // temporary owner is dropped; retain the index for recovery if the
        // kernel does not prove quiescence.
        let result = child.terminate_and_reap_blocking();
        if result.is_err() {
            if let Some(index) = temporary_index.as_ref() {
                index.retain_for_recovery();
            }
        }
        drop(temporary_index);
    }
}

#[cfg(unix)]
impl BoundedToolRunner {
    fn new(policy: &ResolvedExecutionPolicy, output_limit: usize) -> Self {
        Self {
            policy: policy.clone(),
            output_limit,
        }
    }

    async fn run(
        &self,
        executable: ExecutableAnchor,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        argv: Vec<OsString>,
        environment: SanitizedEnvironment,
        summary: &'static str,
        temporary_index: Option<Arc<OwnedTemporaryIndex>>,
        git_directories: Option<VerifiedGitDirectories>,
    ) -> Result<BoundedToolOutput, AppError> {
        let root = root.try_clone()?;
        let working_directory = working_directory.try_clone()?;
        let runner = self.clone();
        runner
            .run_owned(
                executable,
                root,
                working_directory,
                argv,
                environment,
                summary,
                temporary_index,
                git_directories,
            )
            .await
    }

    async fn run_owned(
        &self,
        executable: ExecutableAnchor,
        root: VerifiedProjectRoot,
        working_directory: VerifiedWorkingDirectory,
        argv: Vec<OsString>,
        environment: SanitizedEnvironment,
        summary: &'static str,
        _temporary_index: Option<Arc<OwnedTemporaryIndex>>,
        git_directories: Option<VerifiedGitDirectories>,
    ) -> Result<BoundedToolOutput, AppError> {
        if argv.is_empty() || self.output_limit == 0 {
            return Err(validation(
                "code_change.tool",
                "must have a non-empty argv and positive output bound",
            ));
        }
        self.policy.verify_code_change_state_root()?;
        root.anchor.verify_identity()?;
        if working_directory.root_identity() != root.anchor.identity {
            return Err(recovery_required());
        }
        let working_metadata = working_directory.directory.metadata().map_err(|_| {
            AppError::Runtime {
                operation: "verify code-change tool working directory",
            }
        })?;
        if !working_metadata.is_dir()
            || executable_identity_from_metadata(&working_metadata)
                != working_directory.identity()
        {
            return Err(recovery_required());
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(
                u64::from(self.policy.campaign_limits.code_change_check_timeout_minutes)
                    .saturating_mul(60),
            ))
            .ok_or(AppError::Runtime {
                operation: "start bounded code-change tool deadline",
            })?;
        let child = match spawn_verified_command_before_classified(
            VerifiedCommandSpec {
                launcher: self.policy.launcher_anchor.clone(),
                executable,
                argv,
                working_directory: Some(working_directory),
                environment,
                process_group: ProcessGroupRequirement::Required,
                start_suspended: true,
                project_root: Some(root),
                pueue_config: None,
                git_directories,
                child_io: VerifiedChildIo::Capture,
            },
            deadline,
        )
        .await
        {
            Ok(child) => child,
            Err(crate::process::SpawnVerifiedCommandBeforeError::Launch(error)) => {
                return Err(error)
            }
            Err(crate::process::SpawnVerifiedCommandBeforeError::Cleanup(error)) => {
                return Err(error)
            }
        };
        let mut lease = ToolProcessLease::new(child, _temporary_index);

        if let Err(error) = lease.child_mut().release_before(deadline) {
            return Err(cleanup_tool_failure(lease.child_mut(), error, deadline).await);
        }
        if let Err(error) = lease.child_mut().confirm_exec_before(deadline).await {
            return Err(cleanup_tool_failure(lease.child_mut(), error, deadline).await);
        }
        if let Err(error) = lease.child_mut().wait_for_release_ack_before(deadline).await {
            return Err(cleanup_tool_failure(lease.child_mut(), error, deadline).await);
        }
        let stdout = match lease.child_mut().take_stdout() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(lease.child_mut(), error, deadline).await),
        };
        let stderr = match lease.child_mut().take_stderr() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(lease.child_mut(), error, deadline).await),
        };

        let used = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stdout_task = tokio::spawn(read_tool_output(stdout, self.output_limit, used.clone()));
        let stderr_task = tokio::spawn(read_tool_output(stderr, self.output_limit, used));
        let result = collect_tool_output(lease.child_mut(), stdout_task, stderr_task, deadline).await;
        let (status, stdout, stderr) = match result {
            Ok(value) => value,
            Err(mut pending) => {
                let cleanup = terminate_process_group_before(lease.child_mut(), deadline).await;
                pending.finish().await;
                if let Err(error) = cleanup {
                    return Err(error);
                }
                return Err(match pending.failure {
                    ToolCollectionFailure::Timeout => AppError::Runtime {
                        operation: "code-change tool timeout",
                    },
                    ToolCollectionFailure::OutputLimit => AppError::Validation {
                        field: "code_change.tool_output",
                        message: "exceeds the bounded tool output size",
                    },
                    ToolCollectionFailure::Reader => AppError::Runtime {
                        operation: "read code-change tool output",
                    },
                    ToolCollectionFailure::Wait => AppError::Runtime {
                        operation: "wait for code-change tool",
                    },
                });
            }
        };
        let _child = lease.take_child();
        let output_digest = digest_tool_output(&stdout, &stderr);
        Ok(BoundedToolOutput {
            success: status.success(),
            exit_code: status.code(),
            stdout,
            stderr,
            output_digest,
            summary,
        })
    }
}

/// Runs only the fixed, startup-pinned project checks admitted by the editor
/// protocol.  The result is a bounded digest list; check output is discarded
/// as soon as the caller has enough information to classify the outcome.
#[allow(dead_code)]
#[cfg(unix)]
struct CheckRunner<'a> {
    manager: &'a WorktreeManager,
    candidate: &'a VerifiedProjectRoot,
}

#[allow(dead_code)]
#[cfg(unix)]
impl<'a> CheckRunner<'a> {
    async fn run(
        &self,
        candidate: &VerifiedProjectRoot,
        checks: &[ProposedCheck],
    ) -> Result<Vec<String>, AppError> {
        let validator = CandidateValidator::new(self.manager, self.candidate)?;
        validator.verify().await?;
        let available = [
            CodeChangeTool::Cargo,
            CodeChangeTool::Uv,
            CodeChangeTool::Python,
        ]
        .into_iter()
        .filter(|tool| self.manager.policy.code_change_tool(*tool).is_some())
        .collect::<BTreeSet<_>>();
        validate_proposed_checks(
            checks,
            self.candidate.anchor.canonical_path.as_path(),
            &self.manager.policy.campaign_limits,
            &available,
        )?;
        if candidate.anchor.canonical_path != self.candidate.anchor.canonical_path {
            return Err(recovery_required());
        }
        let candidate = self.candidate.anchor.verify_identity()?;
        self.manager.validate_git_boundary(&candidate)?;
        let mut digests = Vec::with_capacity(checks.len());
        for check in checks {
            let tool = tool_for_program(&check.argv[0]).ok_or_else(|| {
                validation(
                    "code_change_check.argv",
                    "must start with a pinned code-change tool",
                )
            })?;
            let executable = self.manager.policy.code_change_tool(tool).ok_or_else(|| {
                validation(
                    "code_change_check.argv",
                    "requested tool is not available in the startup policy",
                )
            })?;
            let working_directory = VerifiedWorkingDirectory::open_descendant(
                &candidate,
                Path::new(&check.working_directory),
            )?;
            self.manager.validate_git_boundary(&candidate)?;
            let environment = SanitizedEnvironment::for_code_change_tool(&self.manager.policy)?;
            let output = BoundedToolRunner::new(&self.manager.policy, MAX_CHECK_OUTPUT_BYTES)
                .run(
                    executable.clone(),
                    &candidate,
                    &working_directory,
                    check.argv.iter().map(OsString::from).collect(),
                    environment,
                    "project check",
                    None,
                    None,
                )
                .await;
            let boundary = self.manager.validate_git_boundary(&candidate);
            let output = match (output, boundary) {
                (_, Err(error)) => return Err(error),
                (Err(error), Ok(())) => return Err(error),
                (Ok(output), Ok(())) => output,
            };
            self.manager.validate_original_state().await?;
            if !output.success {
                return Err(AppError::Runtime {
                    operation: "code-change project check failed",
                });
            }
            digests.push(output.output_digest);
        }
        self.manager.validate_git_boundary(&candidate)?;
        validator.verify().await?;
        Ok(digests)
    }
}

#[cfg(unix)]
#[derive(Debug)]
enum ToolReadFailure {
    Io,
    Limit,
}

#[cfg(unix)]
async fn read_tool_output<R>(
    mut reader: R,
    limit: usize,
    used: Arc<std::sync::atomic::AtomicUsize>,
) -> Result<Vec<u8>, ToolReadFailure>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut bytes = Vec::with_capacity(limit.min(8192));
    let mut buffer = [0u8; 8192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .map_err(|_| ToolReadFailure::Io)?;
        if count == 0 {
            return Ok(bytes);
        }
        loop {
            let prior = used.load(Ordering::Relaxed);
            let next = prior.checked_add(count).ok_or(ToolReadFailure::Limit)?;
            if next > limit {
                return Err(ToolReadFailure::Limit);
            }
            if used
                .compare_exchange_weak(prior, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
        bytes.extend_from_slice(&buffer[..count]);
    }
}

#[cfg(unix)]
#[derive(Debug)]
enum ToolCollectionFailure {
    Timeout,
    OutputLimit,
    Reader,
    Wait,
}

#[cfg(unix)]
struct PendingToolCollection {
    failure: ToolCollectionFailure,
    stdout_task: Option<tokio::task::JoinHandle<Result<Vec<u8>, ToolReadFailure>>>,
    stderr_task: Option<tokio::task::JoinHandle<Result<Vec<u8>, ToolReadFailure>>>,
}

#[cfg(unix)]
impl PendingToolCollection {
    async fn finish(&mut self) {
        for task in [&mut self.stdout_task, &mut self.stderr_task] {
            let Some(mut task) = task.take() else {
                continue;
            };
            if tokio::time::timeout(Duration::from_secs(1), &mut task)
                .await
                .is_err()
            {
                // The task owns no process authority.  Abort only after the
                // process group has already been terminated.
                task.abort();
                let _ = task.await;
            }
        }
    }
}

#[cfg(unix)]
async fn collect_tool_output(
    child: &mut crate::process::VerifiedChild,
    mut stdout_task: tokio::task::JoinHandle<Result<Vec<u8>, ToolReadFailure>>,
    mut stderr_task: tokio::task::JoinHandle<Result<Vec<u8>, ToolReadFailure>>,
    deadline: Instant,
) -> Result<(std::process::ExitStatus, Vec<u8>, Vec<u8>), PendingToolCollection> {
    let mut wait = Box::pin(child.wait());
    let mut timeout = Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)));
    let mut status = None;
    let mut stdout = None;
    let mut stderr = None;
    let mut stdout_joined = false;
    let mut stderr_joined = false;
    let result = loop {
        if status.is_some() && stdout.is_some() && stderr.is_some() {
            break Ok((
                status.take().expect("tool status was checked"),
                stdout.take().expect("tool stdout was checked"),
                stderr.take().expect("tool stderr was checked"),
            ));
        }
        tokio::select! {
            result = &mut wait, if status.is_none() => {
                match result { Ok(value) => status = Some(value), Err(_) => break Err(ToolCollectionFailure::Wait) }
            }
            result = &mut stdout_task, if stdout.is_none() => {
                stdout_joined = true;
                match result {
                    Ok(Ok(value)) => stdout = Some(value),
                    Ok(Err(ToolReadFailure::Limit)) => break Err(ToolCollectionFailure::OutputLimit),
                    Ok(Err(ToolReadFailure::Io)) | Err(_) => break Err(ToolCollectionFailure::Reader),
                }
            }
            result = &mut stderr_task, if stderr.is_none() => {
                stderr_joined = true;
                match result {
                    Ok(Ok(value)) => stderr = Some(value),
                    Ok(Err(ToolReadFailure::Limit)) => break Err(ToolCollectionFailure::OutputLimit),
                    Ok(Err(ToolReadFailure::Io)) | Err(_) => break Err(ToolCollectionFailure::Reader),
                }
            }
            _ = &mut timeout => break Err(ToolCollectionFailure::Timeout),
        }
    };
    match result {
        Ok(value) => Ok(value),
        Err(failure) => Err(PendingToolCollection {
            failure,
            stdout_task: (!stdout_joined).then_some(stdout_task),
            stderr_task: (!stderr_joined).then_some(stderr_task),
        }),
    }
}

#[cfg(unix)]
async fn cleanup_tool_failure(
    child: &mut crate::process::VerifiedChild,
    original: AppError,
    deadline: Instant,
) -> AppError {
    match terminate_process_group_before(child, deadline).await {
        Ok(()) => original,
        Err(error) => error,
    }
}

#[cfg(unix)]
fn digest_tool_output(stdout: &[u8], stderr: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(stdout);
    hasher.update([0u8]);
    hasher.update(stderr);
    format!("{:x}", hasher.finalize())
}

fn executable_identity_from_metadata(metadata: &fs::Metadata) -> ExecutableIdentity {
    #[cfg(unix)]
    {
        ExecutableIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            owner: metadata.uid(),
            mode: metadata.mode() & 0o7777,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        ExecutableIdentity {
            device: 0,
            inode: 0,
            owner: 0,
            mode: 0,
        }
    }
}

#[cfg(unix)]
fn require_success(output: &BoundedToolOutput, _operation: &'static str) -> Result<(), AppError> {
    if output.success {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "code-change tool returned failure",
        })
    }
}

#[cfg(unix)]
fn bounded_utf8_line<'a>(bytes: &'a [u8], _summary: &'static str) -> Result<&'a str, AppError> {
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(AppError::Validation {
            field: "code_change.git_output",
            message: "exceeds the bounded Git output size",
        });
    }
    let line = std::str::from_utf8(bytes).map_err(|_| AppError::Validation {
        field: "code_change.git_output",
        message: "contains invalid Git output",
    })?;
    let line = line.trim_end_matches(['\r', '\n']);
    if line.is_empty() || line.contains(['\r', '\n']) {
        return Err(AppError::Validation {
            field: "code_change.git_output",
            message: "must contain one bounded line",
        });
    }
    Ok(line)
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn recovery_required() -> AppError {
    AppError::Runtime {
        operation: "recover code-change ownership",
    }
}

fn identity_token(identity: ExecutableIdentity) -> String {
    format!(
        "{}:{}:{}:{}",
        identity.device, identity.inode, identity.owner, identity.mode
    )
}

/// Public Task 3 recovery entry point.  The coordinator uses the repository
/// query rather than an in-memory list so recovery decisions are durable and
/// bounded by the caller's requested limit.
pub fn list_recoverable_code_change_runs(
    db: &Db,
    limit: usize,
) -> Result<Vec<CodeChangeRun>, AppError> {
    CodeChangeRepository::new(db).list_recoverable(limit)
}

fn unix_timestamp() -> Result<i64, AppError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .map_err(|_| AppError::Runtime {
            operation: "represent code-change lifecycle timestamp",
        })
}

#[cfg(unix)]
fn process_is_alive(pid: i64) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    if pid <= 0 {
        return true;
    }
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    matches!(
        io::Error::last_os_error().raw_os_error(),
        Some(libc::EPERM)
    )
}

#[cfg(not(unix))]
fn process_is_alive(_pid: i64) -> bool {
    true
}

fn verify_durable_run_scope(
    db: &Db,
    run: &CodeChangeRun,
    project: &Project,
) -> Result<(), AppError> {
    let connection = db.connect()?;
    let scope = connection
        .query_row(
            "SELECT p.campaign_id, p.kind, c.project_id, pr.root_path
             FROM proposals AS p
             JOIN campaigns AS c ON c.campaign_id = p.campaign_id
             JOIN projects AS pr ON pr.project_id = c.project_id
             WHERE p.proposal_id = ?1 AND p.campaign_id = ?2",
            [&run.proposal_id, &run.campaign_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            },
        )
        .optional()
        .map_err(crate::db::database_error("verify code-change run ownership"))?
        .ok_or_else(recovery_required)?;
    if scope.0 != run.campaign_id
        || scope.1 != "code_change"
        || scope.2 != project.project_id
    {
        return Err(recovery_required());
    }
    let database_root = fs::canonicalize(scope.3).map_err(|_| recovery_required())?;
    if database_root != project.root_path {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn inspect_common_directory<'a>(
    manager: &'a WorktreeManager,
    root: &'a VerifiedProjectRoot,
    working_directory: &'a VerifiedWorkingDirectory,
) -> impl std::future::Future<Output = Result<PathBuf, AppError>> + 'a {
    async move {
        let output = manager
            .git(
                root,
                working_directory,
                &["rev-parse", "--git-common-dir"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "inspect Git common directory")?;
        let text = bounded_utf8_line(&output.stdout, "Git common directory")?;
        let path = Path::new(text);
        let path = if path.is_absolute() {
            path.to_owned()
        } else {
            root.anchor.canonical_path.join(path)
        };
        if path_has_symlink_component(&path)? {
            return Err(validation(
                "code_change.git",
                "Git common directory contains a symlink",
            ));
        }
        let canonical = fs::canonicalize(&path).map_err(|_| AppError::Validation {
            field: "code_change.git",
            message: "Git common directory is not readable",
        })?;
        if !canonical.is_dir() {
            return Err(validation(
                "code_change.git",
                "Git common directory is not a directory",
            ));
        }
        Ok(canonical)
    }
}

#[cfg(unix)]
fn open_git_directory(path: &Path) -> Result<GitDirectoryProof, AppError> {
    if path_has_symlink_component(path)? {
        return Err(recovery_required());
    }
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path).map_err(|_| recovery_required())?;
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_dir() || !secure_metadata_for_git(&metadata) {
        return Err(recovery_required());
    }
    Ok(GitDirectoryProof {
        path: path.to_owned(),
        identity: executable_identity_from_metadata(&metadata),
        directory: Arc::new(file),
    })
}

#[cfg(unix)]
fn open_git_file(
    path: &Path,
    target_base: Option<&Path>,
    pointer_kind: Option<GitPointerKind>,
) -> Result<GitFileProof, AppError> {
    if path_has_symlink_component(path)? {
        return Err(recovery_required());
    }
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    let file = options.open(path).map_err(|_| recovery_required())?;
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_file() || !secure_metadata_for_git(&metadata) {
        return Err(recovery_required());
    }
    let bytes = read_bounded_descriptor(&file, "git.metadata")?;
    let target = match (target_base, pointer_kind) {
        (Some(base), Some(kind)) => Some(parse_git_pointer(&bytes, base, kind)?),
        (None, None) => None,
        _ => return Err(recovery_required()),
    };
    Ok(GitFileProof {
        path: path.to_owned(),
        identity: executable_identity_from_metadata(&metadata),
        digest: sha256_hex(&bytes),
        target,
        target_base: target_base.map(Path::to_owned),
        pointer_kind,
        file: Arc::new(file),
    })
}

#[cfg(unix)]
fn secure_metadata_for_git(metadata: &fs::Metadata) -> bool {
    metadata.uid() == unsafe { libc::geteuid() as u32 } && metadata.mode() & 0o022 == 0
}

#[cfg(unix)]
fn read_bounded_descriptor(file: &File, field: &'static str) -> Result<Vec<u8>, AppError> {
    let mut bytes = Vec::new();
    let mut offset = 0u64;
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let count = file
            .read_at(&mut chunk, offset)
            .map_err(|_| AppError::Runtime {
                operation: "read local Git metadata",
            })?;
        if count == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..count]);
        if bytes.len() > MAX_GIT_OUTPUT_BYTES {
            return Err(validation(field, "exceeds the bounded Git metadata size"));
        }
        offset = offset.saturating_add(count as u64);
    }
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(field, "exceeds the bounded Git metadata size"));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn parse_git_pointer(
    bytes: &[u8],
    base: &Path,
    kind: GitPointerKind,
) -> Result<PathBuf, AppError> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        validation("git.metadata", "Git metadata pointer must be valid UTF-8")
    })?;
    if text.contains('\0') {
        return Err(validation(
            "git.metadata",
            "Git metadata pointer contains NUL",
        ));
    }
    let text = text.strip_suffix('\n').unwrap_or(text);
    if text.contains(['\r', '\n']) {
        return Err(validation(
            "git.metadata",
            "Git metadata pointer must contain one line",
        ));
    }
    let value = match kind {
        GitPointerKind::GitDir => text
            .strip_prefix("gitdir:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                validation(
                    "git.metadata",
                    "Git worktree metadata must identify a Git directory",
                )
            })?,
        GitPointerKind::Path => text.trim(),
    };
    if value.is_empty() {
        return Err(validation(
            "git.metadata",
            "Git metadata pointer must not be empty",
        ));
    }
    let path = Path::new(value);
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        base.join(path)
    };
    if path_has_symlink_component_relaxed(&path)? {
        return Err(recovery_required());
    }
    let canonical = fs::canonicalize(&path).map_err(|_| recovery_required())?;
    let metadata = fs::symlink_metadata(&canonical).map_err(|_| recovery_required())?;
    if !metadata.is_dir() {
        return Err(recovery_required());
    }
    Ok(canonical)
}

#[cfg(unix)]
fn revalidate_git_directory(proof: &GitDirectoryProof) -> Result<(), AppError> {
    let current = open_git_directory(&proof.path)?;
    let metadata = proof.directory.metadata().map_err(|_| recovery_required())?;
    if proof.identity != current.identity
        || proof.identity != executable_identity_from_metadata(&metadata)
    {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn revalidate_git_file(
    proof: &GitFileProof,
    _target_base: Option<&Path>,
) -> Result<(), AppError> {
    let current = open_git_file(
        &proof.path,
        proof.target_base.as_deref(),
        proof.pointer_kind,
    )?;
    let metadata = proof.file.metadata().map_err(|_| recovery_required())?;
    if proof.identity != current.identity
        || proof.identity != executable_identity_from_metadata(&metadata)
        || proof.digest != current.digest
        || proof.target != current.target
    {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn revalidate_git_entry(proof: &GitEntryProof) -> Result<(), AppError> {
    match proof {
        GitEntryProof::Directory(directory) => revalidate_git_directory(directory),
        GitEntryProof::File(file) => revalidate_git_file(file, file.target_base.as_deref()),
    }
}

#[cfg(unix)]
fn revalidate_git_directory_descriptor(proof: &GitDirectoryProof) -> Result<(), AppError> {
    let metadata = proof.directory.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_dir()
        || !secure_metadata_for_git(&metadata)
        || proof.identity != executable_identity_from_metadata(&metadata)
    {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn revalidate_git_file_descriptor(proof: &GitFileProof) -> Result<(), AppError> {
    let metadata = proof.file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_file()
        || !secure_metadata_for_git(&metadata)
        || proof.identity != executable_identity_from_metadata(&metadata)
    {
        return Err(recovery_required());
    }
    let bytes = read_bounded_descriptor(&proof.file, "git.metadata")?;
    if sha256_hex(&bytes) != proof.digest {
        return Err(recovery_required());
    }
    let target = match (proof.target_base.as_deref(), proof.pointer_kind) {
        (Some(base), Some(kind)) => Some(parse_git_pointer(&bytes, base, kind)?),
        (None, None) => None,
        _ => return Err(recovery_required()),
    };
    if target != proof.target {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn revalidate_git_entry_descriptor(proof: &GitEntryProof) -> Result<(), AppError> {
    match proof {
        GitEntryProof::Directory(directory) => revalidate_git_directory_descriptor(directory),
        GitEntryProof::File(file) => revalidate_git_file_descriptor(file),
    }
}

#[cfg(unix)]
fn existing_regular_path(path: &Path) -> Result<Option<PathBuf>, AppError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(recovery_required()),
        Ok(metadata) if metadata.is_file() => Ok(Some(path.to_owned())),
        Ok(_) => Err(validation(
            "git.config",
            "Git configuration must be a regular file",
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(recovery_required()),
    }
}

#[cfg(unix)]
fn validate_git_config_proof(
    proof: &GitFileProof,
    expected_worktree: Option<&Path>,
) -> Result<(), AppError> {
    let bytes = read_git_file(proof)?;
    if bytes.is_empty() {
        return Err(validation(
            "git.config",
            "validated Git configuration must be nonempty",
        ));
    }
    validate_git_config_bytes(
        &bytes,
        proof.path.parent().unwrap_or_else(|| Path::new("/")),
        expected_worktree,
    )
}

#[cfg(unix)]
fn read_git_file(proof: &GitFileProof) -> Result<Vec<u8>, AppError> {
    read_bounded_descriptor(&proof.file, "git.metadata")
}

#[cfg(unix)]
fn validate_git_config_bytes(
    bytes: &[u8],
    config_directory: &Path,
    expected_worktree: Option<&Path>,
) -> Result<(), AppError> {
    if bytes.windows(3).any(|window| window == b"\xef\xbb\xbf") {
        return Err(validation(
            "git.config",
            "local Git configuration contains a byte-order mark",
        ));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| validation("git.config", "local Git configuration must be UTF-8"))?;
    let mut section = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if line.ends_with('\\') {
            return Err(validation(
                "git.config",
                "local Git configuration cannot continue a value",
            ));
        }
        if let Some(header) = line.strip_prefix('[') {
            let end = header
                .find(']')
                .ok_or_else(|| validation("git.config", "local Git configuration has an invalid section"))?;
            if !header[end + 1..].trim().is_empty() {
                return Err(validation(
                    "git.config",
                    "local Git configuration has trailing section data",
                ));
            }
            section = header[..end]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase();
            if matches!(section.as_str(), "filter" | "include" | "includeif") {
                return Err(validation(
                    "git.config",
                    "local Git configuration contains an execution channel",
                ));
            }
            continue;
        }
        let key = line
            .split(['=', ' ', '\t'])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if key.is_empty() {
            return Err(validation(
                "git.config",
                "local Git configuration has an invalid key",
            ));
        }
        let full = if section.is_empty() {
            key.clone()
        } else {
            format!("{section}.{key}")
        };
        if full == "core.worktree" {
            let value = config_value(line).ok_or_else(|| {
                validation("git.config", "core.worktree must name the exact worktree")
            })?;
            let path = Path::new(value);
            let path = if path.is_absolute() {
                path.to_owned()
            } else {
                config_directory.join(path)
            };
            let canonical = fs::canonicalize(&path).map_err(|_| recovery_required())?;
            if expected_worktree != Some(canonical.as_path()) {
                return Err(recovery_required());
            }
            continue;
        }
        if full == "extensions.worktreeconfig" {
            let value = config_value(line).unwrap_or_default().to_ascii_lowercase();
            if value != "true" && value != "false" {
                return Err(validation(
                    "git.config",
                    "extensions.worktreeConfig must be boolean",
                ));
            }
            continue;
        }
        if full == "core.hookspath"
            || full == "core.fsmonitor"
            || full == "core.sshcommand"
            || full == "core.gitproxy"
            || full == "core.askpass"
            || full == "core.pager"
            || full == "core.editor"
            || full == "sequence.editor"
            || full == "commit.gpgsign"
            || full == "tag.gpgsign"
            || full == "user.signingkey"
            || full.starts_with("credential.")
            || full.starts_with("gpg.")
            || full.starts_with("pager.")
            || full.starts_with("mergetool.")
            || full.starts_with("filter.")
            || full.starts_with("include")
            || full.starts_with("diff.")
                && (full.ends_with(".external")
                    || full.ends_with(".textconv")
                    || full.ends_with(".command"))
            || full.starts_with("submodule.") && full.ends_with(".update")
            || full.starts_with("http.") && full.contains("extraheader")
        {
            return Err(validation(
                "git.config",
                "local Git configuration contains an execution or credential channel",
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn config_value(line: &str) -> Option<&str> {
    line.split_once('=')
        .map(|(_, value)| value.trim())
        .or_else(|| line.split_whitespace().nth(1))
        .filter(|value| !value.is_empty())
}

fn read_bounded_file(path: &Path, field: &'static str) -> Result<Vec<u8>, AppError> {
    #[cfg(unix)]
    let file = {
        let mut options = fs::OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options.open(path).map_err(|_| AppError::Runtime {
            operation: "read local Git metadata",
        })?
    };
    #[cfg(not(unix))]
    let mut file = fs::File::open(path).map_err(|_| AppError::Runtime {
        operation: "read local Git metadata",
    })?;
    let metadata = file.metadata().map_err(|_| AppError::Runtime {
        operation: "read local Git metadata",
    })?;
    if !metadata.is_file() {
        return Err(validation(field, "must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take((MAX_GIT_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| AppError::Runtime {
            operation: "read local Git metadata",
        })?;
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(field, "exceeds the bounded Git metadata size"));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn read_bounded_entry_at(
    parent: &File,
    name: &OsStr,
    field: &'static str,
) -> Result<Option<Vec<u8>>, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation(field, "metadata entry name contains NUL"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
    };
    if fd < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(recovery_required());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_file() {
        return Err(validation(field, "metadata entry must be a regular file"));
    }
    read_bounded_descriptor(&file, field).map(Some)
}

fn path_has_symlink_component(path: &Path) -> Result<bool, AppError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(Path::new("/")),
            Component::CurDir => continue,
            Component::ParentDir => {
                return Err(validation(
                    "code_change.path",
                    "path must not traverse a parent",
                ))
            }
            Component::Normal(name) => current.push(name),
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(AppError::Runtime {
                    operation: "inspect code-change path",
                })
            }
        }
    }
    Ok(false)
}

fn path_has_symlink_component_relaxed(path: &Path) -> Result<bool, AppError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(Path::new("/")),
            Component::CurDir => continue,
            Component::ParentDir => {
                if !current.pop() {
                    return Err(validation(
                        "code_change.path",
                        "Git metadata path escapes its parent",
                    ));
                }
            }
            Component::Normal(name) => current.push(name),
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(AppError::Runtime {
                    operation: "inspect code-change path",
                })
            }
        }
    }
    Ok(false)
}

fn validate_local_git_metadata(root: &Path) -> Result<(), AppError> {
    let dot_git = root.join(".git");
    let metadata = fs::symlink_metadata(&dot_git).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            validation("code_change.git", "project is not a Git worktree")
        } else {
            AppError::Runtime {
                operation: "inspect local Git metadata",
            }
        }
    })?;
    if metadata.file_type().is_symlink() {
        return Err(validation(
            "code_change.git",
            "local Git metadata must not be a symlink",
        ));
    }
    let git_dir = if metadata.is_dir() {
        dot_git
    } else if metadata.is_file() {
        let bytes = read_bounded_file(&dot_git, "git.metadata")?;
        let text = std::str::from_utf8(&bytes).map_err(|_| {
            validation("git.metadata", "worktree Git metadata must be valid UTF-8")
        })?;
        let gitdir = text
            .lines()
            .find_map(|line| line.trim().strip_prefix("gitdir:"))
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains('\0'))
            .ok_or_else(|| {
                validation(
                    "git.metadata",
                    "worktree Git metadata must identify a Git directory",
                )
            })?;
        let path = Path::new(gitdir);
        if path.is_absolute() {
            path.to_owned()
        } else {
            root.join(path)
        }
    } else {
        return Err(validation(
            "git.metadata",
            "local Git metadata must be a directory or worktree file",
        ));
    };
    if path_has_symlink_component_relaxed(&git_dir)? {
        return Err(validation(
            "git.metadata",
            "Git metadata path must not contain a symlink",
        ));
    }
    let git_dir = fs::canonicalize(&git_dir).map_err(|_| AppError::Runtime {
        operation: "resolve local Git metadata",
    })?;
    if !git_dir.is_dir() {
        return Err(validation(
            "git.metadata",
            "Git metadata path must be a directory",
        ));
    }
    let common_dir = git_dir.join("commondir");
    let common_dir = match fs::symlink_metadata(&common_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(validation(
                "git.metadata",
                "Git common-directory file must not be a symlink",
            ))
        }
        Ok(metadata) if metadata.is_file() => {
            let bytes = read_bounded_file(&common_dir, "git.metadata")?;
            let value = std::str::from_utf8(&bytes)
                .map_err(|_| validation("git.metadata", "Git common-directory file is invalid"))?
                .trim();
            if value.is_empty() || value.contains('\0') {
                return Err(validation(
                    "git.metadata",
                    "Git common-directory file is invalid",
                ));
            }
            let path = Path::new(value);
            if path.is_absolute() {
                path.to_owned()
            } else {
                git_dir.join(path)
            }
        }
        Ok(_) => {
            return Err(validation(
                "git.metadata",
                "Git common-directory file must be regular",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => git_dir.clone(),
        Err(_) => {
            return Err(AppError::Runtime {
                operation: "inspect Git common directory metadata",
            })
        }
    };
    if path_has_symlink_component_relaxed(&common_dir)? {
        return Err(validation(
            "git.metadata",
            "Git common directory must not contain a symlink",
        ));
    }
    let common_dir = fs::canonicalize(&common_dir).map_err(|_| AppError::Runtime {
        operation: "resolve Git common directory metadata",
    })?;
    let config = common_dir.join("config");
    validate_local_git_config(&config)
}

fn validate_local_git_config(path: &Path) -> Result<(), AppError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            validation("git.config", "local Git configuration is missing")
        } else {
            AppError::Runtime {
                operation: "inspect local Git configuration",
            }
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(validation(
            "git.config",
            "local Git configuration must be a regular file",
        ));
    }
    let bytes = read_bounded_file(path, "git.config")?;
    if bytes.windows(3).any(|window| window == b"\xef\xbb\xbf") {
        return Err(validation(
            "git.config",
            "local Git configuration contains a byte-order mark",
        ));
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| validation("git.config", "local Git configuration must be UTF-8"))?;
    let mut section = String::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[') {
            let end = header
                .find(']')
                .ok_or_else(|| validation("git.config", "local Git configuration has an invalid section"))?;
            if !header[end + 1..].trim().is_empty() {
                return Err(validation(
                    "git.config",
                    "local Git configuration has trailing section data",
                ));
            }
            section = header[..end]
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase();
            if matches!(section.as_str(), "filter" | "include" | "includeif") {
                return Err(validation(
                    "git.config",
                    "local Git configuration contains an execution channel",
                ));
            }
            continue;
        }
        let key = line
            .split(['=', ' ', '\t'])
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if key.is_empty() {
            return Err(validation(
                "git.config",
                "local Git configuration has an invalid key",
            ));
        }
        let full = if section.is_empty() {
            key.clone()
        } else {
            format!("{section}.{key}")
        };
        if full == "core.worktree"
            || full.starts_with("filter.")
            || full.starts_with("include")
            || full.starts_with("credential.")
            || full == "core.hookspath"
            || full == "core.fsmonitor"
            || full == "core.sshcommand"
            || full == "core.gitproxy"
            || full == "commit.gpgsign"
            || full == "tag.gpgsign"
            || full.starts_with("gpg.")
            || full.starts_with("diff.") && full.ends_with(".external")
            || full.starts_with("mergetool.")
        {
            return Err(validation(
                "git.config",
                "local Git configuration contains an execution or credential channel",
            ));
        }
    }
    Ok(())
}

fn digest_protected_refs(bytes: &[u8], excluded_refs: &[&str]) -> Result<String, AppError> {
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(
            "code_change.git_refs",
            "exceeds the bounded Git output size",
        ));
    }
    let mut values = Vec::new();
    let mut fields = bytes.split(|byte| *byte == 0);
    loop {
        let Some(reference) = fields.next() else {
            break;
        };
        if reference.is_empty() {
            continue;
        }
        let object = fields.next().ok_or_else(|| {
            validation("code_change.git_refs", "Git ref output is malformed")
        })?;
        if object.is_empty() {
            return Err(validation(
                "code_change.git_refs",
                "Git ref output is malformed",
            ));
        }
        let reference = std::str::from_utf8(reference).map_err(|_| {
            validation("code_change.git_refs", "Git ref output is not UTF-8")
        })?;
        let object = std::str::from_utf8(object).map_err(|_| {
            validation("code_change.git_refs", "Git ref output is not UTF-8")
        })?;
        if !reference.starts_with("refs/") {
            return Err(validation(
                "code_change.git_refs",
                "Git ref output contains an invalid ref",
            ));
        }
        canonical_full_sha(object)?;
        if !excluded_refs.iter().any(|excluded| *excluded == reference) {
            values.push((reference.to_owned(), object.to_owned()));
        }
    }
    values.sort();
    let mut encoded = Vec::new();
    for (reference, object) in values {
        encoded.extend_from_slice(reference.as_bytes());
        encoded.push(0);
        encoded.extend_from_slice(object.as_bytes());
        encoded.push(0);
    }
    Ok(sha256_hex(&encoded))
}

fn parse_status_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(
            "code_change.status",
            "exceeds the bounded Git output size",
        ));
    }
    let mut records = bytes.split(|byte| *byte == 0).peekable();
    let mut paths = BTreeSet::new();
    while let Some(record) = records.next() {
        if record.is_empty() {
            continue;
        }
        if record.len() < 4 || record[2] != b' ' {
            return Err(validation(
                "code_change.status",
                "Git status output is malformed",
            ));
        }
        insert_status_path(&mut paths, &record[3..])?;
        if matches!(record[0], b'R' | b'C') || matches!(record[1], b'R' | b'C') {
            let renamed_from = records.next().ok_or_else(|| {
                validation("code_change.status", "Git rename output is malformed")
            })?;
            insert_status_path(&mut paths, renamed_from)?;
        }
        if paths.len() > MAX_STATUS_PATHS {
            return Err(validation(
                "code_change.status",
                "exceeds the bounded changed-file count",
            ));
        }
    }
    Ok(paths.into_iter().collect())
}

fn validate_ignored_status_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(
            "code_change.status",
            "exceeds the bounded Git output size",
        ));
    }
    let mut ignored_paths = Vec::new();
    for record in bytes.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        if record.len() < 3 || &record[..3] != b"!! " {
            continue;
        }
        let mut record_paths = BTreeSet::new();
        insert_status_path(&mut record_paths, &record[3..])?;
        if record_paths.iter().any(|path| is_protected_path(path)) {
            return Err(validation(
                "code_change.diff_paths",
                "contains an ignored protected service path",
            ));
        }
        ignored_paths.extend(record_paths);
        if ignored_paths.len() > MAX_STATUS_PATHS {
            return Err(validation(
                "code_change.status",
                "exceeds the bounded ignored-path count",
            ));
        }
    }
    Ok(ignored_paths)
}

struct IgnoredScanBudget {
    entries: usize,
    bytes: u64,
}

#[cfg(unix)]
fn validate_ignored_tree(root: &Path, paths: &[PathBuf]) -> Result<(), AppError> {
    if path_has_symlink_component(root)? {
        return Err(recovery_required());
    }
    let root_directory = open_ignored_directory(root)?;
    let mut budget = IgnoredScanBudget { entries: 0, bytes: 0 };
    for path in paths {
        if path.is_absolute()
            || path
                .components()
                .any(|component| {
                    matches!(
                        component,
                        Component::ParentDir | Component::RootDir | Component::CurDir
                    )
                })
        {
            return Err(recovery_required());
        }
        if is_protected_path(path) {
            return Err(validation(
                "code_change.diff_paths",
                "contains an ignored protected service path",
            ));
        }
        let mut directory = root_directory
            .try_clone()
            .map_err(|_| recovery_required())?;
        let mut relative = PathBuf::new();
        let mut components = path.components().peekable();
        while let Some(Component::Normal(name)) = components.next() {
            relative.push(name);
            let entry = match open_ignored_entry_at(&directory, name)? {
                Some(entry) => entry,
                None => return Err(recovery_required()),
            };
            match entry {
                IgnoredEntry::Symlink => {
                    account_ignored_symlink(&mut budget)?;
                    break;
                }
                IgnoredEntry::File(metadata) => {
                    if components.peek().is_some() {
                        return Err(recovery_required());
                    }
                    account_ignored_entry(&metadata, &mut budget)?;
                }
                IgnoredEntry::Directory(child) => {
                    if components.peek().is_none() {
                        scan_ignored_directory(&child, &relative, 0, &mut budget)?;
                    } else {
                        directory = child;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_ignored_tree(_root: &Path, paths: &[PathBuf]) -> Result<(), AppError> {
    validate_ignored_status_paths(
        &paths
            .iter()
            .flat_map(|path| {
                let mut record = b"!! ".to_vec();
                record.extend_from_slice(path.to_string_lossy().as_bytes());
                record.push(0);
                record
            })
            .collect::<Vec<_>>(),
    )
    .map(|_| ())
}

fn account_ignored_entry(
    metadata: &fs::Metadata,
    budget: &mut IgnoredScanBudget,
) -> Result<(), AppError> {
    budget.entries = budget.entries.saturating_add(1);
    if budget.entries > MAX_IGNORED_SCAN_ENTRIES {
        return Err(validation(
            "code_change.ignored_paths",
            "exceeds the bounded ignored-tree entry count",
        ));
    }
    if metadata.is_file() {
        budget.bytes = budget.bytes.saturating_add(metadata.len());
        if budget.bytes > MAX_IGNORED_SCAN_BYTES {
            return Err(validation(
                "code_change.ignored_paths",
                "exceeds the bounded ignored-tree byte count",
            ));
        }
    }
    Ok(())
}

#[cfg(unix)]
enum IgnoredEntry {
    Symlink,
    File(fs::Metadata),
    Directory(File),
}

#[cfg(unix)]
fn account_ignored_symlink(budget: &mut IgnoredScanBudget) -> Result<(), AppError> {
    budget.entries = budget.entries.saturating_add(1);
    if budget.entries > MAX_IGNORED_SCAN_ENTRIES {
        return Err(validation(
            "code_change.ignored_paths",
            "exceeds the bounded ignored-tree entry count",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_ignored_entry_at(
    parent: &File,
    name: &OsStr,
) -> Result<Option<IgnoredEntry>, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.path", "ignored path contains NUL"))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(recovery_required());
    }
    let stat = unsafe { stat.assume_init() };
    let kind = stat.st_mode as u32 & libc::S_IFMT as u32;
    if kind == libc::S_IFLNK as u32 {
        return Ok(Some(IgnoredEntry::Symlink));
    }
    if kind == libc::S_IFDIR as u32 {
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(recovery_required());
        }
        let directory = unsafe { File::from_raw_fd(fd) };
        let metadata = directory.metadata().map_err(|_| recovery_required())?;
        if !secure_owned_directory(&metadata) {
            return Err(recovery_required());
        }
        return Ok(Some(IgnoredEntry::Directory(directory)));
    }
    if kind != libc::S_IFREG as u32 {
        // Never open devices, FIFOs, or sockets while auditing ignored paths;
        // opening one could block or trigger unrelated kernel behavior.
        return Err(recovery_required());
    }
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
    };
    if fd < 0 {
        return Err(recovery_required());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if metadata.file_type().is_symlink() {
        return Ok(Some(IgnoredEntry::Symlink));
    }
    Ok(Some(IgnoredEntry::File(metadata)))
}

#[cfg(unix)]
fn scan_ignored_directory(
    directory: &File,
    relative: &Path,
    depth: usize,
    budget: &mut IgnoredScanBudget,
) -> Result<(), AppError> {
    if depth > MAX_IGNORED_SCAN_DEPTH {
        return Err(validation(
            "code_change.ignored_paths",
            "exceeds the bounded ignored-tree depth",
        ));
    }
    let metadata = directory.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_directory(&metadata) {
        return Err(recovery_required());
    }
    account_ignored_entry(&metadata, budget)?;
    for entry in fs::read_dir(descriptor_path(directory)).map_err(|_| recovery_required())? {
        let entry = entry.map_err(|_| recovery_required())?;
        let name = entry.file_name();
        let child_relative = relative.join(&name);
        if is_protected_path(&child_relative) {
            return Err(validation(
                "code_change.diff_paths",
                "contains an ignored protected service path",
            ));
        }
        let child = open_ignored_entry_at(directory, &name)?
            .ok_or_else(recovery_required)?;
        match child {
            IgnoredEntry::Symlink => account_ignored_symlink(budget)?,
            IgnoredEntry::Directory(child) => {
                scan_ignored_directory(&child, &child_relative, depth + 1, budget)?;
            }
            IgnoredEntry::File(metadata) => account_ignored_entry(&metadata, budget)?,
        }
    }
    Ok(())
}

#[cfg(unix)]
fn open_ignored_directory(path: &Path) -> Result<File, AppError> {
    let descriptor_backed = path.starts_with("/proc/self/fd") || path.starts_with("/dev/fd");
    if !descriptor_backed && path_has_symlink_component(path)? {
        return Err(recovery_required());
    }
    let mut options = fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let directory = options.open(path).map_err(|_| recovery_required())?;
    let metadata = directory.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_directory(&metadata) {
        return Err(recovery_required());
    }
    Ok(directory)
}

#[cfg(not(unix))]
fn open_ignored_directory(path: &Path) -> Result<fs::File, AppError> {
    fs::File::open(path).map_err(|_| recovery_required())
}

#[cfg(unix)]
fn descriptor_path(file: &File) -> PathBuf {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        PathBuf::from(format!("/dev/fd/{}", file.as_raw_fd()))
    }
}

#[cfg(not(unix))]
fn descriptor_path(file: &fs::File) -> PathBuf {
    let _ = file;
    PathBuf::new()
}

fn insert_status_path(paths: &mut BTreeSet<PathBuf>, bytes: &[u8]) -> Result<(), AppError> {
    if bytes.is_empty() {
        return Err(validation(
            "code_change.status",
            "Git status contains an empty path",
        ));
    }
    #[cfg(unix)]
    let path = PathBuf::from(OsString::from_vec(bytes.to_vec()));
    #[cfg(not(unix))]
    let path = PathBuf::from(
        std::str::from_utf8(bytes)
            .map_err(|_| validation("code_change.status", "Git status path is invalid"))?,
    );
    if path.is_absolute()
        || path
            .components()
            .any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::CurDir
                )
            })
    {
        return Err(validation(
            "code_change.status",
            "Git status contains a traversing path",
        ));
    }
    paths.insert(path);
    Ok(())
}

fn validate_no_nested_repositories(root: &Path, paths: &[PathBuf]) -> Result<(), AppError> {
    for path in paths {
        let mut current = root.to_owned();
        let components = path.components().collect::<Vec<_>>();
        for (index, component) in components.iter().enumerate() {
            let Component::Normal(name) = component else {
                continue;
            };
            current.push(name);
            let metadata = match fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(_) => return Err(recovery_required()),
            };
            if metadata.file_type().is_symlink() {
                return Err(recovery_required());
            }
            if metadata.is_dir() && index + 1 < components.len() {
                let nested_git = current.join(".git");
                if let Ok(nested) = fs::symlink_metadata(&nested_git) {
                    if nested.file_type().is_symlink() || nested.is_dir() || nested.is_file() {
                        return Err(validation(
                            "code_change.diff_paths",
                            "nested repositories are not allowed",
                        ));
                    }
                }
            }
        }
        if let Ok(metadata) = fs::symlink_metadata(&current) {
            if metadata.file_type().is_symlink() {
                return Err(recovery_required());
            }
            if metadata.is_dir() {
                let nested_git = current.join(".git");
                if let Ok(nested) = fs::symlink_metadata(&nested_git) {
                    if nested.file_type().is_symlink() || nested.is_dir() || nested.is_file() {
                        return Err(validation(
                            "code_change.diff_paths",
                            "nested repositories are not allowed",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_state_worktree_parents(
    _policy: &ResolvedExecutionPolicy,
    _campaign_id: &str,
) -> Result<(), AppError> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::NativeGate,
    )
    .into())
}

#[cfg(unix)]
fn secure_owned_directory(metadata: &fs::Metadata) -> bool {
    metadata.is_dir()
        && metadata.uid() == unsafe { libc::geteuid() as u32 }
        && metadata.mode() & 0o022 == 0
}

#[cfg(unix)]
fn open_or_create_directory_at(parent: &File, name: &OsStr) -> Result<File, AppError> {
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| {
        validation("code_change.path", "directory component contains NUL")
    })?;
    for _ in 0..2 {
        let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW;
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, 0) };
        if fd >= 0 {
            let file = unsafe { File::from_raw_fd(fd) };
            let metadata = file.metadata().map_err(|_| AppError::Runtime {
                operation: "verify code-change state directory",
            })?;
            if !secure_owned_directory(&metadata) {
                return Err(recovery_required());
            }
            return Ok(file);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ENOENT) {
            return Err(AppError::Runtime {
                operation: "open code-change state directory",
            });
        }
        let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EEXIST) {
                return Err(AppError::Runtime {
                    operation: "create code-change state directory",
                });
            }
        }
    }
    Err(recovery_required())
}

#[cfg(unix)]
fn open_existing_directory_at(parent: &File, name: &OsStr) -> io::Result<File> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL directory component"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let directory = unsafe { File::from_raw_fd(fd) };
    let metadata = directory.metadata()?;
    if !secure_owned_directory(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unowned code-change directory",
        ));
    }
    Ok(directory)
}

#[cfg(unix)]
fn open_worktree_parent(manager: &WorktreeManager) -> Result<Option<File>, AppError> {
    let Some(parents) = manager.worktree_parents.as_ref() else {
        return Ok(None);
    };
    Ok(Some(parents.revalidate(&manager.policy, &manager.campaign_id)?))
}

#[cfg(unix)]
fn directory_entry_exists(parent: &File, name: &OsStr) -> Result<bool, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.path", "directory component contains NUL"))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(recovery_required())
    }
}

#[cfg(unix)]
fn directory_identity(directory: &File) -> Result<ExecutableIdentity, AppError> {
    let metadata = directory.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_directory(&metadata) {
        return Err(recovery_required());
    }
    Ok(executable_identity_from_metadata(&metadata))
}

#[cfg(unix)]
struct OwnedTemporaryIndex {
    _file: File,
    parent: Arc<File>,
    directory: Arc<File>,
    directory_name: OsString,
    directory_identity: ExecutableIdentity,
    name: OsString,
    path: PathBuf,
    identity: Mutex<Option<ExecutableIdentity>>,
    lock_identity: Mutex<Option<ExecutableIdentity>>,
    unproven: AtomicBool,
}

#[cfg(unix)]
impl OwnedTemporaryIndex {
    fn create(policy: &ResolvedExecutionPolicy) -> Result<Self, AppError> {
        policy.verify_code_change_state_root()?;
        let parent = policy.code_change_state_root_directory();
        for _ in 0..8 {
            let suffix = TEMP_INDEX_COUNTER.fetch_add(1, Ordering::Relaxed);
            let directory_name = OsString::from(format!(
                ".code-change-index-{}-{}",
                std::process::id(),
                suffix
            ));
            let directory_name_c = std::ffi::CString::new(directory_name.as_bytes()).map_err(|_| {
                validation("code_change.index", "temporary index name is invalid")
            })?;
            let created = unsafe {
                libc::mkdirat(
                    parent.as_raw_fd(),
                    directory_name_c.as_ptr(),
                    0o700,
                )
            };
            if created < 0 {
                if io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(AppError::Runtime {
                    operation: "create code-change temporary index directory",
                });
            }
            let directory = match open_existing_directory_at(&parent, &directory_name) {
                Ok(directory) => Arc::new(directory),
                Err(_) => {
                    // Reopen by the manager's unique name solely to prove
                    // the directory identity before attempting its removal;
                    // a failed open otherwise leaves a recovery artifact.
                    if let Ok(directory) = open_existing_directory_at(&parent, &directory_name)
                    {
                        if let Ok(identity) = directory_identity(&directory) {
                            let _ = remove_owned_temp_directory(
                                &parent,
                                &directory_name,
                                identity,
                            );
                        }
                    }
                    return Err(recovery_required());
                }
            };
            let name = OsString::from("index");
            let name_c = std::ffi::CString::new(name.as_bytes()).map_err(|_| {
                validation("code_change.index", "temporary index name is invalid")
            })?;
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name_c.as_ptr(),
                    libc::O_RDWR
                        | libc::O_CREAT
                        | libc::O_EXCL
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                    0o600,
                )
            };
            if fd < 0 {
                if let Ok(identity) = directory_identity(&directory) {
                    let _ = remove_owned_temp_directory(&parent, &directory_name, identity);
                }
                return Err(AppError::Runtime {
                    operation: "create code-change temporary index",
                });
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let metadata = match file.metadata() {
                Ok(metadata) => metadata,
                Err(_) => {
                    // Without metadata for the descriptor we cannot prove
                    // that the pathname still names the file we created.
                    // Retain the private directory for recovery instead of
                    // deleting a same-name replacement.
                    drop(file);
                    return Err(recovery_required());
                }
            };
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() as u32 }
                || metadata.mode() & 0o077 != 0
            {
                let identity = executable_identity_from_metadata(&metadata);
                drop(file);
                let _ = remove_owned_temp_entry(&directory, &name, Some(identity));
                let _ = remove_owned_temp_directory(
                    &parent,
                    &directory_name,
                    directory_identity(&directory).unwrap_or(identity),
                );
                return Err(recovery_required());
            }
            let identity = executable_identity_from_metadata(&metadata);
            let directory_identity = directory_identity(&directory)?;
            let path = policy
                .code_change_state_root_path()
                .join(&directory_name)
                .join(&name);
            return Ok(Self {
                _file: file,
                parent,
                directory,
                directory_name,
                directory_identity,
                name,
                path,
                identity: Mutex::new(Some(identity)),
                lock_identity: Mutex::new(None),
                unproven: AtomicBool::new(false),
            });
        }
        Err(AppError::Runtime {
            operation: "allocate code-change temporary index",
        })
    }

    fn verify_before_command(&self) -> Result<(), AppError> {
        if self.unproven.load(Ordering::Acquire) {
            return Err(recovery_required());
        }
        if directory_identity(&self.directory)
            .map_err(|error| {
                self.unproven.store(true, Ordering::Release);
                error
            })?
            != self.directory_identity
        {
            self.unproven.store(true, Ordering::Release);
            return Err(recovery_required());
        }
        let current = temporary_entry_identity(&self.directory, &self.name)
            .map_err(|error| {
                self.unproven.store(true, Ordering::Release);
                error
            })?
            .ok_or_else(|| {
                self.unproven.store(true, Ordering::Release);
                recovery_required()
            })?;
        if Some(current)
            != *self
                .identity
                .lock()
                .map_err(|_| recovery_required())?
        {
            self.unproven.store(true, Ordering::Release);
            return Err(recovery_required());
        }
        let current_lock = temporary_entry_identity(&self.directory, OsStr::new("index.lock"))
            .map_err(|error| {
                self.unproven.store(true, Ordering::Release);
                error
            })?;
        if let Some(current_lock) = current_lock {
            let expected = *self
                .lock_identity
                .lock()
                .map_err(|_| recovery_required())?;
            if expected != Some(current_lock) {
                self.unproven.store(true, Ordering::Release);
                return Err(recovery_required());
            }
        }
        Ok(())
    }

    fn record_after_command(&self) -> Result<(), AppError> {
        if directory_identity(&self.directory)
            .map_err(|error| {
                self.unproven.store(true, Ordering::Release);
                error
            })?
            != self.directory_identity
        {
            self.unproven.store(true, Ordering::Release);
            return Err(recovery_required());
        }
        let current_index = temporary_entry_identity(&self.directory, &self.name).map_err(|error| {
            self.unproven.store(true, Ordering::Release);
            error
        })?;
        if let Some(current) = current_index {
            let expected = *self
                .identity
                .lock()
                .map_err(|_| recovery_required())?;
            if expected != Some(current)
                && !temporary_index_has_git_signature(&self.directory, &self.name)?
            {
                // Git legitimately replaces index with index.lock and then
                // renames the lock into place.  Only a bounded, structurally
                // valid Git index can be adopted as that rename; an unknown
                // same-name inode remains retained for recovery.
                self.unproven.store(true, Ordering::Release);
                return Err(recovery_required());
            }
            *self
                .identity
                .lock()
                .map_err(|_| recovery_required())? = Some(current);
        }
        let current_lock = temporary_entry_identity(&self.directory, OsStr::new("index.lock"))
            .map_err(|error| {
                self.unproven.store(true, Ordering::Release);
                error
            })?;
        if let Some(current_lock) = current_lock {
            let expected = *self
                .lock_identity
                .lock()
                .map_err(|_| recovery_required())?;
            if expected != Some(current_lock) {
                self.unproven.store(true, Ordering::Release);
                return Err(recovery_required());
            }
        }
        Ok(())
    }

    fn retain_for_recovery(&self) {
        self.unproven.store(true, Ordering::Release);
    }
}

#[cfg(unix)]
impl Drop for OwnedTemporaryIndex {
    fn drop(&mut self) {
        if self.unproven.load(Ordering::Acquire)
            || directory_identity(&self.directory).ok() != Some(self.directory_identity)
        {
            return;
        }
        let identity = self.identity.lock().ok().and_then(|identity| *identity);
        let lock_identity = self
            .lock_identity
            .lock()
            .ok()
            .and_then(|identity| *identity);
        let index_removed = identity.is_some_and(|identity| {
            remove_owned_temp_entry(&self.directory, &self.name, Some(identity))
        });
        let lock_removed = lock_identity.is_some_and(|identity| {
            remove_owned_temp_entry(&self.directory, OsStr::new("index.lock"), Some(identity))
        });
        let index_clear = match temporary_entry_identity(&self.directory, &self.name) {
            Ok(None) => true,
            Ok(Some(_)) | Err(_) => false,
        };
        let lock_clear = match temporary_entry_identity(&self.directory, OsStr::new("index.lock"))
        {
            Ok(None) => true,
            Ok(Some(_)) | Err(_) => false,
        };
        if (index_removed || index_clear) && (lock_removed || lock_clear) {
            let _ = remove_owned_temp_directory(
                &self.parent,
                &self.directory_name,
                self.directory_identity,
            );
        }
    }
}

#[cfg(unix)]
fn remove_owned_temp_entry(
    parent: &File,
    name: &OsStr,
    initial_identity: Option<ExecutableIdentity>,
) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        return quarantine_owned_temp_entry(parent, name, initial_identity, false);
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
    let Ok(name) = std::ffi::CString::new(name.as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        return false;
    }
    let stat = unsafe { stat.assume_init() };
    let file_type = stat.st_mode as u32 & libc::S_IFMT as u32;
    if file_type != libc::S_IFREG as u32 || stat.st_uid != unsafe { libc::geteuid() as u32 } {
        return false;
    }
    let private_mode = stat.st_mode as u32 & 0o077 == 0;
    if let Some(initial_identity) = initial_identity {
        let current_identity = ExecutableIdentity {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
            owner: stat.st_uid,
            mode: stat.st_mode as u32 & 0o7777,
        };
        if current_identity != initial_identity || !private_mode {
            return false;
        }
    } else if !private_mode {
        return false;
    }
    unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), 0) == 0 }
    }
}

#[cfg(unix)]
fn remove_owned_temp_directory(
    parent: &File,
    name: &OsStr,
    expected_identity: ExecutableIdentity,
) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        return quarantine_owned_temp_entry(parent, name, Some(expected_identity), true);
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
    let Ok(name) = std::ffi::CString::new(name.as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } < 0
    {
        return false;
    }
    let stat = unsafe { stat.assume_init() };
    let identity = ExecutableIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino as u64,
        owner: stat.st_uid,
        mode: stat.st_mode as u32 & 0o7777,
    };
    if stat.st_mode as u32 & libc::S_IFMT as u32 != libc::S_IFDIR as u32
        || !secure_owned_directory_mode(stat.st_mode as u32)
        || identity != expected_identity
    {
        return false;
    }
    unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) == 0 }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn quarantine_owned_temp_entry(
    parent: &File,
    name: &OsStr,
    expected_identity: Option<ExecutableIdentity>,
    directory: bool,
) -> bool {
    let Ok(source) = std::ffi::CString::new(name.as_bytes()) else {
        return false;
    };
    let quarantine_name = OsString::from(format!(
        ".code-change-quarantine-{}-{}",
        std::process::id(),
        TEMP_INDEX_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let Ok(quarantine) = std::ffi::CString::new(quarantine_name.as_bytes()) else {
        return false;
    };
    // renameat2(RENAME_NOREPLACE) moves the exact current directory entry
    // into a manager-generated capability name atomically.  If a concurrent
    // actor replaced the source, the moved inode is inspected and restored;
    // it is never unlinked merely because the source name matched.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            source.as_ptr(),
            parent.as_raw_fd(),
            quarantine.as_ptr(),
            1u32,
        )
    };
    if result != 0 {
        return false;
    }
    let identity = match if directory {
        open_existing_directory_at(parent, &quarantine_name)
            .map_err(|_| recovery_required())
            .and_then(|entry| directory_identity(&entry))
    } else {
        match temporary_entry_identity(parent, &quarantine_name) {
            Ok(Some(identity)) => Ok(identity),
            Ok(None) => Err(recovery_required()),
            Err(error) => Err(error),
        }
    } {
        Ok(identity) => identity,
        _ => {
            restore_quarantined_entry(parent, &quarantine_name, name);
            return false;
        }
    };
    if expected_identity != Some(identity)
        || (directory && !entry_is_directory(parent, &quarantine_name))
        || (!directory && entry_is_directory(parent, &quarantine_name))
    {
        restore_quarantined_entry(parent, &quarantine_name, name);
        return false;
    }
    let removed = unsafe {
        libc::unlinkat(
            parent.as_raw_fd(),
            quarantine.as_ptr(),
            if directory { libc::AT_REMOVEDIR } else { 0 },
        ) == 0
    };
    if !removed {
        restore_quarantined_entry(parent, &quarantine_name, name);
    }
    removed
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn entry_is_directory(parent: &File, name: &OsStr) -> bool {
    let Ok(name) = std::ffi::CString::new(name.as_bytes()) else {
        return false;
    };
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return false;
    }
    unsafe { stat.assume_init() }.st_mode as u32 & libc::S_IFMT as u32 == libc::S_IFDIR as u32
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn restore_quarantined_entry(parent: &File, quarantine: &OsStr, original: &OsStr) {
    let Ok(quarantine) = std::ffi::CString::new(quarantine.as_bytes()) else {
        return;
    };
    let Ok(original) = std::ffi::CString::new(original.as_bytes()) else {
        return;
    };
    unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            parent.as_raw_fd(),
            quarantine.as_ptr(),
            parent.as_raw_fd(),
            original.as_ptr(),
            1u32,
        );
    }
}

#[cfg(unix)]
fn secure_owned_directory_mode(mode: u32) -> bool {
    mode & 0o077 == 0
}

#[cfg(unix)]
fn temporary_entry_identity(
    parent: &File,
    name: &OsStr,
) -> Result<Option<ExecutableIdentity>, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.index", "temporary index name is invalid"))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::NotFound {
            return Ok(None);
        }
        return Err(recovery_required());
    }
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode as u32 & libc::S_IFMT as u32 != libc::S_IFREG as u32
        || stat.st_uid != unsafe { libc::geteuid() as u32 }
        || stat.st_mode as u32 & 0o077 != 0
    {
        return Err(recovery_required());
    }
    Ok(Some(ExecutableIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino as u64,
        owner: stat.st_uid,
        mode: stat.st_mode as u32 & 0o7777,
    }))
}

#[cfg(unix)]
fn temporary_index_has_git_signature(parent: &File, name: &OsStr) -> Result<bool, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.index", "temporary index name is invalid"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(recovery_required());
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_file() || metadata.len() > MAX_GIT_OUTPUT_BYTES as u64 {
        return Ok(false);
    }
    let mut header = [0u8; 12];
    let read = file.read_at(&mut header, 0).map_err(|_| recovery_required())?;
    if read != header.len() || &header[..4] != b"DIRC" {
        return Ok(false);
    }
    let version = u32::from_be_bytes(header[4..8].try_into().unwrap());
    let _entries = u32::from_be_bytes(header[8..12].try_into().unwrap());
    Ok((2..=4).contains(&version)
        && metadata.len() >= 12 + 20)
}

fn validate_internal_id(field: &'static str, value: &str) -> Result<(), AppError> {
    if value.is_empty()
        || value.len() > MAX_WORKTREE_ID_BYTES
        || value == "."
        || value == ".."
        || value.bytes().any(|byte| {
            !matches!(
                byte,
                b'a'..=b'z'
                    | b'A'..=b'Z'
                    | b'0'..=b'9'
                    | b'_'
                    | b'-'
                    | b'.'
                    | b':'
            )
        })
    {
        return Err(validation(field, "must be a safe internal identifier"));
    }
    Ok(())
}

fn git_ref_component(value: &str) -> Result<String, AppError> {
    if value
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-'))
    {
        return Ok(value.to_owned());
    }
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b'-') {
            encoded.push(byte as char);
        } else {
            encoded.push('=');
            encoded.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            encoded.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
    }
    if encoded.is_empty() || encoded.len() > MAX_GIT_REF_COMPONENT_BYTES {
        return Err(validation(
            "code_change.ref",
            "encoded ref component exceeds the bounded size",
        ));
    }
    Ok(encoded)
}

fn validate_relative_working_directory(value: &str) -> Result<(), AppError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.chars().any(char::is_control)
        || path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err(validation(
            "code_change_check.working_directory",
            "must be a relative non-traversing path",
        ));
    }
    Ok(())
}

fn shell_argv(argv: &[String]) -> bool {
    let basename = Path::new(&argv[0])
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    matches!(
        basename,
        "sh" | "bash" | "dash" | "zsh" | "fish" | "cmd" | "powershell" | "pwsh"
    ) || argv.iter().any(|arg| {
        matches!(arg.as_str(), "-c" | "-Command" | "/c" | "/C")
            || arg
                .chars()
                .any(|character| matches!(character, ';' | '&' | '|' | '$' | '`' | '<' | '>'))
    })
}

fn tool_for_program(program: &str) -> Option<CodeChangeTool> {
    match Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some("git") => Some(CodeChangeTool::Git),
        Some("cargo") => Some(CodeChangeTool::Cargo),
        Some("uv") => Some(CodeChangeTool::Uv),
        Some("python") | Some("python3") => Some(CodeChangeTool::Python),
        _ => None,
    }
}

fn pyproject_has_pytest(root: &Path) -> bool {
    let path = root.join("pyproject.toml");
    let Ok(contents) = fs::read_to_string(path) else {
        return false;
    };
    toml::from_str::<toml::Value>(&contents)
        .ok()
        .is_some_and(|value| {
            value
                .get("tool")
                .and_then(|tool| tool.get("pytest"))
                .and_then(|pytest| pytest.get("ini_options"))
                .is_some()
        })
}

fn validation(field: &'static str, message: &'static str) -> AppError {
    AppError::Validation { field, message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt;

    #[cfg(unix)]
    fn test_owned_index(root: &Path) -> OwnedTemporaryIndex {
        let parent = File::open(root).unwrap();
        let directory_name = OsString::from(".owned-index");
        fs::create_dir(root.join(&directory_name)).unwrap();
        let directory = Arc::new(File::open(root.join(&directory_name)).unwrap());
        let name = OsString::from("index");
        let path = root.join(&directory_name).join(&name);
        let file = fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        let identity = executable_identity_from_metadata(&file.metadata().unwrap());
        let directory_identity = directory_identity(&directory).unwrap();
        OwnedTemporaryIndex {
            _file: file,
            parent: Arc::new(parent),
            directory,
            directory_name,
            directory_identity,
            name,
            path,
            identity: Mutex::new(Some(identity)),
            lock_identity: Mutex::new(None),
            unproven: AtomicBool::new(false),
        }
    }

    fn tools(tools: &[CodeChangeTool]) -> BTreeSet<CodeChangeTool> {
        tools.iter().copied().collect()
    }

    #[test]
    fn validates_canonical_sha_and_internal_refs() {
        let sha = "a".repeat(40);
        assert_eq!(canonical_full_sha(&sha).unwrap(), sha);
        assert!(canonical_full_sha(&"A".repeat(40)).is_err());
        assert!(canonical_full_sha(&"a".repeat(39)).is_err());
        assert_eq!(
            candidate_ref("campaign-1", "proposal-1").unwrap(),
            "campaign/campaign-1/candidate/proposal-1"
        );
        assert_eq!(best_ref("campaign-1").unwrap(), "campaign/campaign-1/best");
    }

    #[test]
    fn owned_path_and_editor_json_reject_unsafe_inputs() {
        assert_eq!(
            owned_worktree_relative_path("campaign-1", "proposal-1").unwrap(),
            PathBuf::from(".pueue-agent/worktrees/campaign-1/proposal-1")
        );
        let root = tempdir().unwrap();
        let limits = CampaignLimits::default();
        let available = tools(&[CodeChangeTool::Cargo]);
        let valid = format!(
            r#"{{"schema_version":1,"status":"ready","summary":"ok","proposed_checks":[{{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"."}}]}}"#
        );
        assert!(parse_editor_output(valid.as_bytes(), root.path(), &limits, &available).is_ok());
        let absolute = format!(
            r#"{{"schema_version":1,"status":"ready","summary":"ok","proposed_checks":[{{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"/tmp"}}]}}"#
        );
        assert!(
            parse_editor_output(absolute.as_bytes(), root.path(), &limits, &available).is_err()
        );
        let unknown = valid.replace("\"summary\":\"ok\"", "\"summary\":\"ok\",\"extra\":true");
        assert!(parse_editor_output(unknown.as_bytes(), root.path(), &limits, &available).is_err());
        assert!(
            parse_editor_output(&vec![b'x'; 65 * 1024], root.path(), &limits, &available).is_err()
        );
    }

    #[test]
    fn checks_reject_shell_and_too_many_entries() {
        let root = tempdir().unwrap();
        let limits = CampaignLimits::default();
        let available = tools(&[CodeChangeTool::Cargo]);
        let shell = ProposedCheck {
            source: "shell".to_owned(),
            argv: vec!["sh".to_owned(), "-c".to_owned(), "cargo test".to_owned()],
            working_directory: ".".to_owned(),
        };
        assert!(validate_proposed_checks(&[shell], root.path(), &limits, &available).is_err());
        let checks = (0..=limits.max_code_change_checks)
            .map(|_| ProposedCheck {
                source: "cargo".to_owned(),
                argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
                working_directory: ".".to_owned(),
            })
            .collect::<Vec<_>>();
        assert!(validate_proposed_checks(&checks, root.path(), &limits, &available).is_err());
    }

    #[test]
    fn discovery_is_deterministic_and_uv_lock_selects_uv() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("Cargo.toml"), "[package]\nname='x'\n").unwrap();
        fs::write(
            root.path().join("pyproject.toml"),
            "[tool.pytest.ini_options]\naddopts='-q'\n",
        )
        .unwrap();
        fs::write(root.path().join("uv.lock"), "version = 1\n").unwrap();
        let checks = discover_project_checks(
            root.path(),
            &tools(&[
                CodeChangeTool::Cargo,
                CodeChangeTool::Uv,
                CodeChangeTool::Python,
            ]),
        )
        .unwrap();
        assert_eq!(checks.len(), 2);
        assert_eq!(
            checks[0].argv,
            RUST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            checks[1].argv,
            UV_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn protected_paths_and_diff_caps_are_rejected() {
        assert!(is_protected_path(Path::new(".git/index")));
        assert!(is_protected_path(Path::new(".pueue-agent/state")));
        assert!(!is_protected_path(Path::new("src/lib.rs")));
        assert!(validate_protected_paths(&[PathBuf::from(".git/index")]).is_err());
        let limits = CampaignLimits::default();
        assert!(validate_diff_limits(51, 0, &limits).is_err());
        assert!(validate_diff_limits(0, 500_001, &limits).is_err());
    }

    #[test]
    fn ignored_status_rejects_protected_paths_with_bounded_records() {
        assert!(validate_ignored_status_paths(b"!! .env\0").is_err());
        assert!(validate_ignored_status_paths(b"!! nested/\0").is_ok());
        assert_eq!(
            validate_ignored_status_paths(b"!! build/\0M  src/lib.rs\0").unwrap(),
            vec![PathBuf::from("build/")]
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_config_proof_rejects_execution_channels_and_wrong_worktree() {
        let root = tempdir().unwrap();
        let worktree = root.path().join("candidate");
        fs::create_dir(&worktree).unwrap();
        assert!(validate_git_config_bytes(
            b"[include]\npath = /tmp/config\n",
            root.path(),
            None,
        )
        .is_err());
        assert!(validate_git_config_bytes(
            b"[filter \"smudge\"]\ncommand = /bin/sh\n",
            root.path(),
            None,
        )
        .is_err());
        assert!(validate_git_config_bytes(
            b"[diff \"external\"]\nexternal = /bin/sh\n",
            root.path(),
            None,
        )
        .is_err());
        assert!(validate_git_config_bytes(
            b"[core]\nworktree = /tmp/other\n",
            root.path(),
            Some(&worktree),
        )
        .is_err());
        assert!(validate_git_config_bytes(
            b"[core]\nworktree = /tmp/other\n",
            root.path(),
            None,
        )
        .is_err());
        assert!(validate_git_config_bytes(
            format!("[core]\nworktree = {}\n", worktree.display()).as_bytes(),
            root.path(),
            Some(&worktree),
        )
        .is_ok());
        assert!(validate_git_config_bytes(
            b"[remote \"origin\"]\nurl = https://example.invalid/repo.git\n",
            root.path(),
            None,
        )
        .is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn owned_temporary_index_drop_retains_unproven_renamed_index_and_cleans_lock() {
        let root = tempdir().unwrap();
        let path = root.path().join(".owned-index/index");
        let lock_path = root.path().join(".owned-index/index.lock");
        let index = test_owned_index(root.path());
        fs::write(&lock_path, b"lock").unwrap();
        *index.lock_identity.lock().unwrap() = temporary_entry_identity(
            &index.directory,
            OsStr::new("index.lock"),
        )
        .unwrap();
        let replacement = root.path().join("replacement-index");
        fs::write(&replacement, b"DIRC").unwrap();
        fs::rename(replacement, &path).unwrap();
        drop(index);
        assert!(path.exists());
        assert!(!lock_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn git_metadata_reads_are_offset_independent_and_nonempty() {
        let root = tempdir().unwrap();
        let path = root.path().join("config");
        fs::write(&path, b"[remote \"origin\"]\nurl = https://example.invalid/repo\n")
            .unwrap();
        let proof = open_git_file(&path, None, None).unwrap();
        let first = read_git_file(&proof).unwrap();
        let second = read_git_file(&proof).unwrap();
        assert!(!first.is_empty());
        assert_eq!(first, second);
    }

    #[cfg(unix)]
    #[test]
    fn owned_temporary_index_unproven_replacement_is_retained() {
        let root = tempdir().unwrap();
        let path = root.path().join(".owned-index/index");
        let index = test_owned_index(root.path());
        let replacement = root.path().join("replacement-index");
        fs::write(&replacement, b"DIRC-unrelated replacement").unwrap();
        fs::rename(replacement, &path).unwrap();
        drop(index);
        assert!(path.exists(), "an unproven replacement must not be deleted");
    }

    #[cfg(unix)]
    #[test]
    fn owned_temporary_index_record_after_command_rejects_same_name_replacement() {
        let root = tempdir().unwrap();
        let path = root.path().join(".owned-index/index");
        let index = test_owned_index(root.path());
        let replacement = root.path().join("replacement-index");
        fs::write(&replacement, b"untrusted replacement").unwrap();
        fs::rename(replacement, &path).unwrap();
        assert!(index.record_after_command().is_err());
        assert!(path.exists(), "an untrusted replacement must be retained");
    }

    #[cfg(unix)]
    #[test]
    fn ignored_status_walk_rejects_recursive_protected_descendants() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("build")).unwrap();
        fs::write(root.path().join("build/.env"), b"secret").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        fs::remove_file(root.path().join("build/.env")).unwrap();
        fs::write(root.path().join("build/.ENV"), b"secret").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        fs::remove_file(root.path().join("build/.ENV")).unwrap();
        fs::create_dir(root.path().join("build/nested")).unwrap();
        fs::write(root.path().join("build/nested/credentials.txt"), b"secret").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        fs::remove_file(root.path().join("build/nested/credentials.txt")).unwrap();
        fs::write(root.path().join("build/nested/.git"), b"gitdir: nowhere\n").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ignored_status_walk_does_not_follow_symlinked_directories() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        fs::write(outside.path().join("credentials.json"), b"secret").unwrap();
        fs::create_dir(root.path().join("build")).unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("build/outside")).unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ignored_status_walk_fails_closed_on_recursive_bounds() {
        let root = tempdir().unwrap();
        let build = root.path().join("build");
        fs::create_dir(&build).unwrap();
        for index in 0..=MAX_IGNORED_SCAN_ENTRIES {
            fs::write(build.join(format!("artifact-{index}")), b"x").unwrap();
        }
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        let bytes = root.path().join("bytes");
        fs::create_dir(&bytes).unwrap();
        fs::write(
            bytes.join("large"),
            vec![b'x'; (MAX_IGNORED_SCAN_BYTES + 1) as usize],
        )
        .unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("bytes/")]).is_err());
    }

    #[test]
    fn protected_ref_digest_excludes_only_controlled_candidate_and_best() {
        let initial = b"refs/heads/campaign/a/candidate/p\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0refs/heads/campaign/a/best\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0refs/heads/main\0cccccccccccccccccccccccccccccccccccccccc\0";
        let controlled_changed = b"refs/heads/campaign/a/candidate/p\0dddddddddddddddddddddddddddddddddddddddd\0refs/heads/campaign/a/best\0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\0refs/heads/main\0cccccccccccccccccccccccccccccccccccccccc\0";
        let unrelated_changed = b"refs/heads/campaign/a/candidate/p\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0refs/heads/campaign/a/best\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0refs/heads/main\0ffffffffffffffffffffffffffffffffffffffff\0";
        let excluded = [
            "refs/heads/campaign/a/candidate/p",
            "refs/heads/campaign/a/best",
        ];
        assert_eq!(
            digest_protected_refs(initial, &excluded).unwrap(),
            digest_protected_refs(controlled_changed, &excluded).unwrap()
        );
        assert_ne!(
            digest_protected_refs(initial, &excluded).unwrap(),
            digest_protected_refs(unrelated_changed, &excluded).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_descriptor_environment_uses_fixed_rights() {
        let environment = git_descriptor_environment(13, 14, 7);
        assert_eq!(
            environment.get("GIT_DIR"),
            Some(&git_descriptor_path(13))
        );
        assert_eq!(
            environment.get("GIT_COMMON_DIR"),
            Some(&git_descriptor_path(14))
        );
        assert_eq!(
            environment.get("GIT_WORK_TREE"),
            Some(&git_descriptor_path(7))
        );
        assert!(!environment.values().any(|value| value.contains(".git")));
    }

    #[cfg(unix)]
    #[test]
    fn worktree_administration_uses_only_a_single_relative_leaf() {
        assert_eq!(descriptor_worktree_leaf("proposal"), OsString::from("proposal"));
    }

    #[test]
    fn cleanup_authorization_requires_a_fresh_durable_run_row() {
        let root = tempdir().unwrap();
        let db = crate::db::Db::open(&root.path().join("agent.sqlite")).unwrap();
        assert!(CodeChangeCleanupAuthorization::load(&db, "missing-run").is_err());
        assert!(list_recoverable_code_change_runs(&db, 10).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_liveness_probe_rejects_live_and_invalid_process_ids() {
        assert!(process_is_alive(std::process::id() as i64));
        assert!(process_is_alive(0));
        assert!(process_is_alive(-1));
    }
}
