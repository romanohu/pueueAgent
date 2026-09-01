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
    sync::{atomic::{AtomicU64, Ordering}, Arc},
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};

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

#[cfg(unix)]
use crate::process::{
    spawn_verified_command_before_classified, terminate_process_group_before,
    ProcessGroupRequirement, VerifiedChildIo, VerifiedCommandSpec,
};

#[cfg(unix)]
use std::{
    fs::File,
    os::fd::{AsRawFd, FromRawFd},
    os::unix::{ffi::{OsStrExt, OsStringExt}, fs::{MetadataExt, OpenOptionsExt}},
};

#[cfg(not(unix))]
use crate::execution_policy::{PolicyViolation, PolicyViolationCode, PolicyViolationStage};

const MAX_GIT_OUTPUT_BYTES: usize = 1024 * 1024;
#[allow(dead_code)]
const MAX_CHECK_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_WORKTREE_ID_BYTES: usize = 128;
const MAX_STATUS_PATHS: usize = 50;
const ZERO_SHA: &str = "0000000000000000000000000000000000000000";
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
pub struct VerifiedCodeChangeWorktree {
    manager: WorktreeManager,
    candidate: VerifiedProjectRoot,
    diff_facts: Option<DiffFacts>,
    candidate_sha: Option<String>,
}

impl std::fmt::Debug for VerifiedCodeChangeWorktree {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedCodeChangeWorktree")
            .field("path", &self.manager.worktree_path)
            .field("candidate_sha_present", &self.candidate_sha.is_some())
            .finish()
    }
}

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

    pub async fn cleanup(self) -> Result<(), AppError> {
        self.manager
            .cleanup(
                Some(&self.candidate),
                self.candidate_sha.as_deref(),
                false,
            )
            .await
    }
}

/// Prepare one owned, detached candidate under the startup-retained state
/// root. This is the narrow public entry point used by the later coordinator;
/// all Git and filesystem ownership remains in the private manager types.
pub async fn prepare_code_change_worktree(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    campaign_id: &str,
    proposal_id: &str,
    base_sha: &str,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    let mut manager = WorktreeManager::new(
        policy,
        project,
        original,
        campaign_id,
        proposal_id,
        base_sha,
    )?;
    manager.inspect_base().await?;
    let candidate = manager.prepare().await?;
    if let Err(error) = policy.for_code_change_worktree(project, original, &candidate) {
        let original = AppError::from(error);
        return Err(match manager.cleanup(Some(&candidate), None, false).await {
            Ok(()) => original,
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
        name == ".git"
            || name == ".pueue-agent"
            || name == "execution-policy.toml"
            || matches!(
                lower.as_str(),
                ".env"
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

struct GitBaseline {
    common_directory: PathBuf,
    protected_ref_digest: String,
    remote_config_digest: String,
}

struct WorktreeManager {
    policy: ResolvedExecutionPolicy,
    project: Project,
    original: ResolvedProjectExecutionPolicy,
    campaign_id: String,
    proposal_id: String,
    base_sha: String,
    worktree_path: PathBuf,
    candidate_identity: Option<ExecutableIdentity>,
    baseline: Option<GitBaseline>,
}

impl WorktreeManager {
    fn new(
        policy: &ResolvedExecutionPolicy,
        project: &Project,
        original: &ResolvedProjectExecutionPolicy,
        campaign_id: &str,
        proposal_id: &str,
        base_sha: &str,
    ) -> Result<Self, AppError> {
        validate_internal_id("campaign_id", campaign_id)?;
        validate_internal_id("proposal_id", proposal_id)?;
        canonical_full_sha(base_sha)?;
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
            base_sha: base_sha.to_owned(),
            worktree_path,
            candidate_identity: None,
            baseline: None,
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
        let protected_ref_digest = self
            .protected_ref_digest(&original_root, &working_directory, Some(&owned_ref))
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
            common_directory: common,
            protected_ref_digest,
            remote_config_digest,
        });
        Ok(())
    }

    async fn prepare(&mut self) -> Result<VerifiedProjectRoot, AppError> {
        let baseline = self.baseline.as_ref().ok_or(AppError::Runtime {
            operation: "prepare code-change worktree before base inspection",
        })?;
        self.policy.verify_code_change_state_root()?;
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
        let path = self.worktree_path.as_os_str().to_os_string();
        let base = self.base_sha.clone();
        let args = vec![
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("--detach"),
            path,
            OsString::from(base),
        ];
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
        let candidate_anchor = match ProjectRootAnchor::resolve(&self.worktree_path) {
            Ok(anchor) => anchor,
            Err(error) => return Err(self.cleanup_after_prepare_error(error.into()).await),
        };
        let candidate = match candidate_anchor.verify_identity() {
            Ok(candidate) => candidate,
            Err(error) => return Err(self.cleanup_after_prepare_error(error.into()).await),
        };
        self.candidate_identity = Some(candidate.anchor.identity);
        if !candidate.anchor.canonical_path.starts_with(
            self.policy.code_change_state_root_path().join("worktrees"),
        ) {
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
        let protected_ref_digest = match self
            .protected_ref_digest(&original_root, &working_directory, Some(&owned_ref))
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
        live_experiment: bool,
    ) -> Result<(), AppError> {
        if live_experiment {
            return Err(validation(
                "code_change.cleanup",
                "cannot clean a live candidate experiment",
            ));
        }
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
        #[cfg(unix)]
        let candidate_parent = open_worktree_parent(self)?;
        #[cfg(unix)]
        let Some(candidate_parent) = candidate_parent else {
            return Ok(());
        };
        #[cfg(unix)]
        let candidate_parent_identity = directory_identity(&candidate_parent)?;
        #[cfg(unix)]
        if !directory_entry_exists(&candidate_parent, OsStr::new(&self.proposal_id))? {
            return Ok(());
        }
        if let Some(candidate) = candidate {
            if candidate.anchor.canonical_path != expected_path {
                return Err(recovery_required());
            }
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
            let current_refs = self
                .protected_ref_digest(
                    &original_root,
                    &working_directory,
                    Some(&excluded_ref),
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
        }
        if current.is_none() {
            return Ok(());
        }
        let output = self
            .git_os(
                &original_root,
                &working_directory,
                &[
                    OsString::from("worktree"),
                    OsString::from("remove"),
                    OsString::from("--force"),
                    self.worktree_path.as_os_str().to_os_string(),
                ],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success {
            return Err(recovery_required());
        }
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
        let anchor = self.policy.code_change_git_anchor().ok_or(validation(
            "code_change.git",
            "pinned Git is unavailable",
        ))?;
        let mut argv = vec![
            OsString::from("git"),
            OsString::from("-c"),
            OsString::from("core.hooksPath=/dev/null"),
            OsString::from("-c"),
            OsString::from("core.fsmonitor=false"),
        ];
        argv.extend_from_slice(args);
        let environment = SanitizedEnvironment::for_code_change_tool(&self.policy)?;
        BoundedToolRunner::new(&self.policy, cap)
            .run(anchor.clone(), root, working_directory, argv, environment, "pinned Git")
            .await
    }

    async fn protected_ref_digest(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        candidate_ref: Option<&str>,
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
        digest_protected_refs(&output.stdout, candidate_ref)
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
}

struct CandidateValidator<'a> {
    manager: &'a WorktreeManager,
    candidate: &'a VerifiedProjectRoot,
}

impl<'a> CandidateValidator<'a> {
    fn new(
        manager: &'a WorktreeManager,
        candidate: &'a VerifiedProjectRoot,
    ) -> Result<Self, AppError> {
        if candidate.anchor.canonical_path != manager.worktree_path {
            return Err(recovery_required());
        }
        Ok(Self { manager, candidate })
    }

    async fn verify(&self) -> Result<DiffFacts, AppError> {
        self.manager.policy.verify_code_change_state_root()?;
        let candidate = self.candidate.anchor.verify_identity()?;
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
        let refs = self
            .manager
            .protected_ref_digest(
                &candidate,
                &working_directory,
                Some(&format!("refs/heads/{}", candidate_ref(&self.manager.campaign_id, &self.manager.proposal_id)?)),
            )
            .await?;
        if refs != baseline.protected_ref_digest {
            return Err(recovery_required());
        }
        let paths = self.changed_paths(&candidate, &working_directory).await?;
        validate_protected_paths(&paths)?;
        validate_no_nested_repositories(&candidate.anchor.canonical_path, &paths)?;
        self.validate_no_submodules(&candidate, &working_directory, &paths)
            .await?;
        let repository = CandidateRepository::new(self.manager, &candidate)?;
        let facts = repository.diff_facts(&paths).await?;
        validate_diff_limits(facts.file_count, facts.diff_bytes, &self.manager.policy.campaign_limits)?;
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
            OsString::from("git"),
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
}

struct CandidateRepository<'a> {
    manager: &'a WorktreeManager,
    candidate: &'a VerifiedProjectRoot,
}

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
        let index = OwnedTemporaryIndex::create(&self.manager.policy)?;
        let working_directory = VerifiedWorkingDirectory::root(self.candidate)?;
        let mut environment = SanitizedEnvironment::for_code_change_tool(&self.manager.policy)?;
        environment.with_generated("GIT_INDEX_FILE", index.path.clone());
        let git = self.manager.policy.code_change_git_anchor().ok_or(validation(
            "code_change.git",
            "pinned Git is unavailable",
        ))?;
        let root = self.candidate.try_clone()?;
        let run = |args: Vec<OsString>| async {
            BoundedToolRunner::new(&self.manager.policy, MAX_GIT_OUTPUT_BYTES)
                .run(
                    git.clone(),
                    &root,
                    &working_directory,
                    args,
                    environment.clone(),
                    "Git candidate metadata",
                )
                .await
        };
        let read_tree = run(vec![
            OsString::from("git"),
            OsString::from("read-tree"),
            OsString::from(self.manager.base_sha.clone()),
        ])
        .await?;
        require_success(&read_tree, "construct candidate index")?;
        let mut add_args = vec![OsString::from("git"), OsString::from("add"), OsString::from("-A"), OsString::from("--")];
        add_args.extend(paths.iter().map(|path| path.as_os_str().to_os_string()));
        let add = run(add_args).await?;
        require_success(&add, "stage candidate changes")?;
        let tree = run(vec![OsString::from("git"), OsString::from("write-tree")]).await?;
        require_success(&tree, "write candidate tree")?;
        let tree_sha = canonical_full_sha(bounded_utf8_line(&tree.stdout, "candidate tree")?)?;
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

    async fn commit_candidate(&self, expected: &DiffFacts) -> Result<String, AppError> {
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
        let output = BoundedToolRunner::new(&self.manager.policy, MAX_GIT_OUTPUT_BYTES)
            .run(
                anchor.clone(),
                &root,
                &working_directory,
                vec![
                    OsString::from("git"),
                    OsString::from("commit-tree"),
                    OsString::from(expected.tree_sha.clone()),
                    OsString::from("-p"),
                    OsString::from(self.manager.base_sha.clone()),
                    OsString::from("-m"),
                    OsString::from("pueue-agent code-change candidate"),
                ],
                environment,
                "Git candidate commit",
            )
            .await?;
        require_success(&output, "commit candidate tree")?;
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
        self.ensure_candidate_ref(&candidate_sha).await?;
        self.verify_candidate_ref(&candidate_sha).await?;
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
        Ok(candidate_sha)
    }

    async fn ensure_candidate_ref(&self, candidate_sha: &str) -> Result<(), AppError> {
        let reference = format!("refs/heads/{}", candidate_ref(&self.manager.campaign_id, &self.manager.proposal_id)?);
        let output = self
            .manager
            .git(
                self.candidate,
                &VerifiedWorkingDirectory::root(self.candidate)?,
                &["update-ref", &reference, candidate_sha, ZERO_SHA],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if output.success {
            return Ok(());
        }
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
            Ok(())
        } else {
            Err(recovery_required())
        }
    }

    #[allow(dead_code)]
    async fn verify_candidate_ref(&self, candidate_sha: &str) -> Result<(), AppError> {
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
        Ok(())
    }

    #[allow(dead_code)]
    async fn update_best_ref_cas(
        &self,
        new_sha: &str,
        expected_old_sha: Option<&str>,
    ) -> Result<(), AppError> {
        canonical_full_sha(new_sha)?;
        let expected = expected_old_sha.unwrap_or(ZERO_SHA);
        if expected != ZERO_SHA {
            canonical_full_sha(expected)?;
        }
        let reference = format!("refs/heads/{}", best_ref(&self.manager.campaign_id)?);
        let output = self
            .manager
            .git(
                self.candidate,
                &VerifiedWorkingDirectory::root(self.candidate)?,
                &["update-ref", &reference, new_sha, expected],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if output.success {
            Ok(())
        } else {
            Err(recovery_required())
        }
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
    ) -> Result<BoundedToolOutput, AppError> {
        let root = root.try_clone()?;
        let working_directory = working_directory.try_clone()?;
        let runner = self.clone();
        tokio::spawn(async move {
            runner
                .run_owned(
                    executable,
                    root,
                    working_directory,
                    argv,
                    environment,
                    summary,
                )
                .await
        })
        .await
        .map_err(|_| AppError::Runtime {
            operation: "retain code-change tool cleanup",
        })?
    }

    async fn run_owned(
        &self,
        executable: ExecutableAnchor,
        root: VerifiedProjectRoot,
        working_directory: VerifiedWorkingDirectory,
        argv: Vec<OsString>,
        environment: SanitizedEnvironment,
        summary: &'static str,
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
        let mut child = match spawn_verified_command_before_classified(
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

        if let Err(error) = child.release_before(deadline) {
            return Err(cleanup_tool_failure(&mut child, error, deadline).await);
        }
        if let Err(error) = child.confirm_exec_before(deadline).await {
            return Err(cleanup_tool_failure(&mut child, error, deadline).await);
        }
        if let Err(error) = child.wait_for_release_ack_before(deadline).await {
            return Err(cleanup_tool_failure(&mut child, error, deadline).await);
        }
        let stdout = match child.take_stdout() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(&mut child, error, deadline).await),
        };
        let stderr = match child.take_stderr() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(&mut child, error, deadline).await),
        };

        let used = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let stdout_task = tokio::spawn(read_tool_output(stdout, self.output_limit, used.clone()));
        let stderr_task = tokio::spawn(read_tool_output(stderr, self.output_limit, used));
        let result = collect_tool_output(&mut child, stdout_task, stderr_task, deadline).await;
        let (status, stdout, stderr) = match result {
            Ok(value) => value,
            Err(mut pending) => {
                let cleanup = terminate_process_group_before(&mut child, deadline).await;
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
struct CheckRunner<'a> {
    policy: &'a ResolvedExecutionPolicy,
}

#[allow(dead_code)]
impl<'a> CheckRunner<'a> {
    async fn run(
        &self,
        candidate: &VerifiedProjectRoot,
        checks: &[ProposedCheck],
    ) -> Result<Vec<String>, AppError> {
        let available = [
            CodeChangeTool::Cargo,
            CodeChangeTool::Uv,
            CodeChangeTool::Python,
        ]
        .into_iter()
        .filter(|tool| self.policy.code_change_tool(*tool).is_some())
        .collect::<BTreeSet<_>>();
        validate_proposed_checks(
            checks,
            candidate.anchor.canonical_path.as_path(),
            &self.policy.campaign_limits,
            &available,
        )?;
        let candidate = candidate.anchor.verify_identity()?;
        let mut digests = Vec::with_capacity(checks.len());
        for check in checks {
            let tool = tool_for_program(&check.argv[0]).ok_or_else(|| {
                validation(
                    "code_change_check.argv",
                    "must start with a pinned code-change tool",
                )
            })?;
            let executable = self.policy.code_change_tool(tool).ok_or_else(|| {
                validation(
                    "code_change_check.argv",
                    "requested tool is not available in the startup policy",
                )
            })?;
            let working_directory = VerifiedWorkingDirectory::open_descendant(
                &candidate,
                Path::new(&check.working_directory),
            )?;
            let environment = SanitizedEnvironment::for_code_change_tool(self.policy)?;
            let output = BoundedToolRunner::new(self.policy, MAX_CHECK_OUTPUT_BYTES)
                .run(
                    executable.clone(),
                    &candidate,
                    &working_directory,
                    check.argv.iter().map(OsString::from).collect(),
                    environment,
                    "project check",
                )
                .await?;
            if !output.success {
                return Err(AppError::Runtime {
                    operation: "code-change project check failed",
                });
            }
            digests.push(output.output_digest);
        }
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

fn require_success(output: &BoundedToolOutput, _operation: &'static str) -> Result<(), AppError> {
    if output.success {
        Ok(())
    } else {
        Err(AppError::Runtime {
            operation: "code-change tool returned failure",
        })
    }
}

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
        if full.starts_with("filter.")
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

fn digest_protected_refs(bytes: &[u8], excluded_ref: Option<&str>) -> Result<String, AppError> {
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
        if excluded_ref != Some(reference) {
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
            .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
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

#[cfg(unix)]
fn ensure_state_worktree_parents(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
) -> Result<(), AppError> {
    let state = policy.code_change_state_root_directory();
    let worktrees = open_or_create_directory_at(&state, OsStr::new("worktrees"))?;
    let _campaign = open_or_create_directory_at(&worktrees, OsStr::new(campaign_id))?;
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
    let state = manager.policy.code_change_state_root_directory();
    let worktrees = match open_existing_directory_at(&state, OsStr::new("worktrees")) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(recovery_required()),
    };
    match open_existing_directory_at(&worktrees, OsStr::new(&manager.campaign_id)) {
        Ok(directory) => Ok(Some(directory)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(recovery_required()),
    }
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
    name: OsString,
    path: PathBuf,
    identity: ExecutableIdentity,
}

#[cfg(unix)]
impl OwnedTemporaryIndex {
    fn create(policy: &ResolvedExecutionPolicy) -> Result<Self, AppError> {
        policy.verify_code_change_state_root()?;
        let parent = policy.code_change_state_root_directory();
        for _ in 0..8 {
            let suffix = TEMP_INDEX_COUNTER.fetch_add(1, Ordering::Relaxed);
            let name = OsString::from(format!(".code-change-index-{}-{}", std::process::id(), suffix));
            let name_c = std::ffi::CString::new(name.as_bytes()).map_err(|_| {
                validation("code_change.index", "temporary index name is invalid")
            })?;
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
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
                if io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(AppError::Runtime {
                    operation: "create code-change temporary index",
                });
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let metadata = file.metadata().map_err(|_| AppError::Runtime {
                operation: "verify code-change temporary index",
            })?;
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() as u32 }
                || metadata.mode() & 0o077 != 0
            {
                return Err(recovery_required());
            }
            let identity = executable_identity_from_metadata(&metadata);
            let path = policy.code_change_state_root_path().join(&name);
            return Ok(Self {
                _file: file,
                parent,
                name,
                path,
                identity,
            });
        }
        Err(AppError::Runtime {
            operation: "allocate code-change temporary index",
        })
    }
}

#[cfg(unix)]
impl Drop for OwnedTemporaryIndex {
    fn drop(&mut self) {
        let name = match std::ffi::CString::new(self.name.as_bytes()) {
            Ok(name) => name,
            Err(_) => return,
        };
        let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
        let result = unsafe {
            libc::fstatat(
                self.parent.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result < 0 {
            return;
        }
        let stat = unsafe { stat.assume_init() };
        let current = ExecutableIdentity {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
            owner: stat.st_uid,
            mode: stat.st_mode as u32 & 0o7777,
        };
        if current != self.identity
            || stat.st_mode as u32 & libc::S_IFMT as u32 != libc::S_IFREG as u32
        {
            return;
        }
        let _ = unsafe { libc::unlinkat(self.parent.as_raw_fd(), name.as_ptr(), 0) };
    }
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
}
