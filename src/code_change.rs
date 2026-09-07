//! Pure protocol and repository-shape validation for code-change proposals.
//!
//! The stateful worktree, editor, and check runners are added in a later
//! phase.  This module intentionally keeps the admission-facing data small and
//! deterministic so it can be validated before any child process is started.

use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::{CStr, OsStr, OsString},
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
    agent::{
        AgentHandle, AgentRunner, AgentSpawnError, AgentSpawnStage, BoundCleanupHandle,
    },
    config,
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
    db::{
        AgentDecisionReservation, AgentRunRepository, CampaignRepository, CodeChangeRepository, Db,
        EventRepository, ExperimentRepository, NewCodeChangeCheck, ProjectRepository,
        ProposalRepository, SubmissionRepository, TaskObservationRepository,
    },
    models::{
        AgentContextMode, AgentRunStatus, CampaignState, CodeChangeCheck, CodeChangeCheckStatus,
        CodeChangeRun, CodeChangeState, EventKind, EventStatus, NewEvent, ExperimentStatus,
        TaskObservation,
    },
    pueue::PueueTask,
    reconcile::{managed_task_run_signature, parse_timestamp, task_signature},
    retry::RetryPolicy,
    output::bounded_redacted_text,
};

use crate::promotion::PromotionOutcome;

#[cfg(unix)]
use crate::process::{
    spawn_verified_command_before_classified, terminate_process_group_before,
    GIT_ADMIN_FD, GIT_COMMON_DIR_FD, ProcessGroupRequirement, PROJECT_ROOT_FD,
    VerifiedChildIo, VerifiedCommandSpec, VerifiedGitDirectories,
};
#[cfg(any(target_os = "linux", target_os = "android"))]
use crate::process::GIT_WORKTREE_PARENT_FD;

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
const MAX_CHECK_OUTPUT_DEPTH: usize = 32;
// Supported Cargo all-target and uv project profiles can retain several build
// products and environments. Keep each service-owned scope bounded while
// allowing those profiles to complete; this is still an explicit finite cap,
// not an ignored-path allowlist.
const MAX_CHECK_OUTPUT_ENTRIES: usize = 16_384;
const MAX_CHECK_OUTPUT_ALLOCATED_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_RUNTIME_OUTPUT_ENTRIES: usize = 16_384;
const MAX_RUNTIME_OUTPUT_ALLOCATED_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_WORKTREE_ID_BYTES: usize = 128;
const MAX_STATUS_PATHS: usize = 50;
const MAX_CLEANUP_TASK_OBSERVATIONS: usize = 128;
const MAX_IGNORED_SCAN_ENTRIES: usize = 512;
const MAX_IGNORED_SCAN_DEPTH: usize = 32;
const MAX_IGNORED_SCAN_BYTES: u64 = 500_000;
const MAX_TERMINAL_RESULT_MANIFEST_BYTES: u64 = 16 * 1024;
const RUNTIME_SERVICE_DIRECTORY: &str = ".pueue-agent";
const RUNTIME_RESULTS_DIRECTORY: &str = "results";
const RUNTIME_ARTIFACTS_DIRECTORY: &str = "artifacts";
const RUNTIME_OUTPUTS_DIRECTORY: &str = "runtime";
const RUNTIME_OUTPUT_DIRECTORY_NAMES: &[&str] = &[
    "tmp",
    "cargo-target",
    "uv-venv",
    "uv-cache",
    "uv-python",
    "pytest-cache",
];
const RUNTIME_DIRECTORY_MODE: u32 = 0o700;
const RUNTIME_MANIFEST_MODE: u32 = 0o600;
static TEMP_INDEX_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TerminalResultOutputStatus {
    Ready,
    Missing,
    Invalid,
}

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
    terminal_result_outputs: Option<BoundTerminalResultOutputs>,
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

    pub fn candidate_sha(&self) -> Option<&str> {
        self.candidate_sha.as_deref()
    }

    pub(crate) fn terminal_result_output_status(&self) -> Option<TerminalResultOutputStatus> {
        self.terminal_result_outputs
            .as_ref()
            .map(BoundTerminalResultOutputs::status)
    }

    pub(crate) fn terminal_result_manifest_bytes(&self) -> Option<&[u8]> {
        self.terminal_result_outputs
            .as_ref()
            .and_then(BoundTerminalResultOutputs::manifest_bytes)
    }

    /// Prepare the exact runtime output tree used by a submitted candidate.
    /// The retained descriptors and identities are held across the external
    /// Pueue boundary so path replacements are detected before acceptance.
    pub(crate) fn prepare_runtime_outputs(
        &self,
        experiment_id: &str,
    ) -> Result<PreparedRuntimeOutputs, AppError> {
        PreparedRuntimeOutputs::prepare(&self.candidate, experiment_id)
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
            check_timeout_override: None,
        };
        runner.run(&self.candidate, checks).await
    }

    async fn run_check_round(
        &mut self,
        db: &Db,
        run_id: &str,
        attempt: i64,
        editor_checks: &[ProposedCheck],
        check_timeout_override: Option<Duration>,
    ) -> Result<Option<CheckRoundResult>, AppError> {
        let expected = self.verify().await?;
        let checked_file_count = i64::try_from(expected.file_count).map_err(|_| {
            AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            }
        })?;
        let checked_diff_bytes = i64::try_from(expected.diff_bytes).map_err(|_| {
            AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            }
        })?;
        let reconciled = CodeChangeRepository::new(db).record_checked_diff_for_round(
            run_id,
            attempt,
            expected.persisted_digest(),
            checked_file_count,
            checked_diff_bytes,
            unix_timestamp()?,
        )?;
        if reconciled.state == CodeChangeState::Editing {
            return Ok(None);
        }
        let runner = CheckRunner {
            manager: &self.manager,
            candidate: &self.candidate,
            check_timeout_override,
        };
        runner
            .run_all(
                db,
                run_id,
                attempt,
                &expected,
                editor_checks,
            )
            .await
            .map(Some)
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

    async fn update_best_ref_cas_for_promotion(
        &mut self,
        db: &Db,
        run_id: &str,
        expected_old_sha: Option<&str>,
        test_replacement_sha: Option<&str>,
        test_attempt_counter: Option<Arc<AtomicU64>>,
    ) -> Result<(), AppError> {
        let candidate_sha = self.candidate_sha.clone().ok_or(AppError::Validation {
            field: "code_change.candidate_sha",
            message: "must commit the candidate before updating best",
        })?;
        let authorization = CodeChangeCleanupAuthorization::load(db, run_id)?;
        let run = authorization.fresh_run()?;
        if run.state != CodeChangeState::ExperimentSubmitted
            || run.code_change_run_id != run_id
            || run.candidate_sha.as_deref() != Some(candidate_sha.as_str())
            || run.cleanup_completed_at.is_some()
        {
            return Err(recovery_required());
        }
        verify_durable_run_scope(db, &run, &self.manager.project)?;
        self.manager
            .verify_durable_ownership_proof(&run, Some(&self.candidate))?;
        CandidateRepository::new(&self.manager, &self.candidate)?
            .update_best_ref_cas_for_promotion(
                &candidate_sha,
                expected_old_sha,
                test_replacement_sha,
                test_attempt_counter,
            )
            .await
    }

    async fn best_ref_sha(&self) -> Result<Option<String>, AppError> {
        self.manager.validate_original_state().await?;
        let original_root = self.manager.original.root_anchor.verify_identity()?;
        let original_cwd = VerifiedWorkingDirectory::root(&original_root)?;
        let reference = format!("refs/heads/{}", best_ref(&self.manager.campaign_id)?);
        CandidateRepository::new(&self.manager, &self.candidate)?
            .read_ref(&original_root, &original_cwd, &reference)
            .await
    }

    async fn verify_promotion_candidate(
        &self,
        run: &CodeChangeRun,
        experiment_id: &str,
    ) -> Result<(), AppError> {
        let candidate_sha = self
            .candidate_sha
            .as_deref()
            .ok_or_else(recovery_required)?;
        self.manager.validate_original_state().await?;
        if self.manager.candidate_ref_sha().await?.as_deref() != Some(candidate_sha) {
            return Err(recovery_required());
        }
        let (facts, _) = CandidateRepository::new(&self.manager, &self.candidate)?
            .committed_diff_facts_for_result(candidate_sha, experiment_id)
            .await?;
        let changed_file_count = i64::try_from(facts.file_count).map_err(|_| {
            AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            }
        })?;
        let diff_bytes = i64::try_from(facts.diff_bytes).map_err(|_| AppError::Validation {
            field: "code_change.diff",
            message: "counts cannot be represented",
        })?;
        if run.diff_digest.as_deref() != Some(facts.persisted_digest())
            || run.changed_file_count != Some(changed_file_count)
            || run.diff_bytes != Some(diff_bytes)
        {
            return Err(recovery_required());
        }
        self.manager.validate_original_state().await
    }

    /// Revalidate the candidate root, its immutable revision, and the
    /// descriptor-bound proposal cwd immediately before or after a Pueue
    /// add.  The pathname is reopened after the external boundary so a
    /// replacement cannot be mistaken for the directory held by this
    /// capability.
    #[cfg(unix)]
    pub async fn reverify_submission_boundary(
        &self,
        working_directory: &VerifiedWorkingDirectory,
    ) -> Result<(), AppError> {
        let candidate = self.candidate.anchor.verify_identity()?;
        if candidate.anchor.identity != self.candidate.anchor.identity
            || candidate.anchor.canonical_path != self.manager.worktree_path
        {
            return Err(recovery_required());
        }
        working_directory.reverify_under_root(&candidate)?;
        self.manager.validate_git_boundary(&self.candidate)?;
        self.manager.validate_original_state().await?;
        let candidate_sha = self.candidate_sha.as_deref().ok_or_else(recovery_required)?;
        if self.manager.candidate_ref_sha().await?.as_deref() != Some(candidate_sha) {
            return Err(recovery_required());
        }
        let facts = CandidateRepository::new(&self.manager, &self.candidate)?
            .committed_diff_facts(candidate_sha)
            .await?;
        if self.diff_facts.as_ref().is_some_and(|expected| expected != &facts) {
            return Err(recovery_required());
        }
        Ok(())
    }

    /// Revalidate a candidate immediately after the Pueue add while the
    /// supervisor-owned runtime tree is still in its pre-result state.  This
    /// boundary must not require the terminal-result binding retained during
    /// later ingestion, but it permits only descriptor-bound runtime output
    /// and known verified Python check caches before rechecking the committed
    /// candidate facts.
    pub(crate) async fn reverify_submission_runtime_boundary(
        &self,
        working_directory: &VerifiedWorkingDirectory,
        runtime: &PreparedRuntimeOutputs,
    ) -> Result<(), AppError> {
        let candidate = self.candidate.anchor.verify_identity()?;
        if candidate.anchor.identity != self.candidate.anchor.identity
            || candidate.anchor.canonical_path != self.manager.worktree_path
        {
            return Err(recovery_required());
        }
        working_directory.reverify_under_root(&candidate)?;
        self.manager.validate_git_boundary(&candidate)?;
        self.manager.validate_original_state().await?;
        let candidate_sha = self
            .candidate_sha
            .as_deref()
            .ok_or_else(recovery_required)?;
        if self.manager.candidate_ref_sha().await?.as_deref() != Some(candidate_sha) {
            return Err(recovery_required());
        }
        let facts = CandidateRepository::new(&self.manager, &self.candidate)?
            .committed_diff_facts_for_submission_runtime(candidate_sha)
            .await?;
        if self
            .diff_facts
            .as_ref()
            .is_some_and(|expected| expected != &facts)
        {
            return Err(recovery_required());
        }
        runtime.reverify(&candidate)?;
        Ok(())
    }

    /// Revalidate a submitted candidate immediately around terminal-result
    /// ingestion.  Runtime output is handled by a separate, narrower
    /// allowlist; the ordinary submission boundary remains strict so a
    /// candidate cannot acquire service files before it is submitted.
    pub(crate) async fn reverify_result_ingestion_boundary(
        &self,
        working_directory: &VerifiedWorkingDirectory,
        experiment_id: &str,
    ) -> Result<TerminalResultOutputStatus, AppError> {
        let candidate = self.candidate.anchor.verify_identity()?;
        if candidate.anchor.identity != self.candidate.anchor.identity
            || candidate.anchor.canonical_path != self.manager.worktree_path
        {
            return Err(recovery_required());
        }
        working_directory.reverify_under_root(&candidate)?;
        self.manager.validate_git_boundary(&candidate)?;
        self.manager.validate_original_state().await?;
        let candidate_sha = self.candidate_sha.as_deref().ok_or_else(recovery_required)?;
        if self.manager.candidate_ref_sha().await?.as_deref() != Some(candidate_sha) {
            return Err(recovery_required());
        }
        let (facts, terminal_result_outputs) =
            CandidateRepository::new(&self.manager, &self.candidate)?
                .committed_diff_facts_for_result(candidate_sha, experiment_id)
                .await?;
        if self.diff_facts.as_ref().is_some_and(|expected| expected != &facts) {
            return Err(recovery_required());
        }
        let bound = self
            .terminal_result_outputs
            .as_ref()
            .ok_or_else(recovery_required)?;
        if bound.status() != terminal_result_outputs.status()
            || bound.reverify(&self.candidate.directory).is_err()
        {
            return Err(recovery_required());
        }
        Ok(terminal_result_outputs.status())
    }
}

/// Descriptor-bound runtime output locations for one accepted experiment.
/// The result manifest is created before Pueue starts the task so a task using
/// ordinary file creation APIs cannot inherit a group-writable umask mode.
#[cfg(unix)]
pub(crate) struct PreparedRuntimeOutputs {
    _service: File,
    _results: File,
    _artifacts: File,
    runtime: File,
    _runtime_experiment: File,
    artifact_directory: File,
    result_manifest: File,
    service_identity: ExecutableIdentity,
    results_identity: ExecutableIdentity,
    artifacts_identity: ExecutableIdentity,
    runtime_identity: ExecutableIdentity,
    runtime_experiment_identity: ExecutableIdentity,
    artifact_identity: ExecutableIdentity,
    result_manifest_identity: ExecutableIdentity,
    experiment_id: String,
    runtime_path: PathBuf,
}

#[cfg(unix)]
struct BoundTerminalResultOutputs {
    root_identity: ExecutableIdentity,
    service: File,
    service_identity: ExecutableIdentity,
    results: BoundTerminalResults,
    artifacts: File,
    artifacts_identity: ExecutableIdentity,
    artifact_directory: File,
    artifact_identity: ExecutableIdentity,
    experiment_id: String,
}

#[cfg(unix)]
enum BoundTerminalResults {
    Missing,
    InvalidFile {
        file: File,
        identity: ExecutableIdentity,
    },
    Directory {
        directory: File,
        identity: ExecutableIdentity,
        manifest: BoundTerminalManifest,
    },
}

#[cfg(unix)]
enum BoundTerminalManifest {
    Missing,
    Invalid {
        file: File,
        identity: ExecutableIdentity,
        length: u64,
    },
    Ready {
        file: File,
        identity: ExecutableIdentity,
        bytes: Vec<u8>,
    },
}

#[cfg(unix)]
impl BoundTerminalResultOutputs {
    fn status(&self) -> TerminalResultOutputStatus {
        match &self.results {
            BoundTerminalResults::Missing | BoundTerminalResults::InvalidFile { .. } => {
                TerminalResultOutputStatus::Invalid
            }
            BoundTerminalResults::Directory { manifest, .. } => match manifest {
                BoundTerminalManifest::Missing => TerminalResultOutputStatus::Missing,
                BoundTerminalManifest::Invalid { .. } => TerminalResultOutputStatus::Invalid,
                BoundTerminalManifest::Ready { .. } => TerminalResultOutputStatus::Ready,
            },
        }
    }

    fn manifest_bytes(&self) -> Option<&[u8]> {
        match &self.results {
            BoundTerminalResults::Directory {
                manifest: BoundTerminalManifest::Ready { bytes, .. },
                ..
            } => Some(bytes),
            _ => None,
        }
    }

    fn reverify(&self, root: &File) -> Result<(), AppError> {
        let root_metadata = root.metadata().map_err(|_| recovery_required())?;
        if !secure_owned_directory(&root_metadata)
            || executable_identity_from_metadata(&root_metadata) != self.root_identity
        {
            return Err(recovery_required());
        }
        if verify_runtime_directory(&self.service)? != self.service_identity
            || verify_runtime_directory(&self.artifacts)? != self.artifacts_identity
            || verify_runtime_directory(&self.artifact_directory)? != self.artifact_identity
        {
            return Err(recovery_required());
        }

        let service = open_runtime_directory_at(root, OsStr::new(RUNTIME_SERVICE_DIRECTORY))
            .map_err(|_| recovery_required())?;
        if verify_runtime_directory(&service)? != self.service_identity {
            return Err(recovery_required());
        }
        let (results, _artifacts, artifacts_identity, _artifact_directory, artifact_identity) =
            bind_terminal_result_service(&service, &self.experiment_id)?;
        if artifacts_identity != self.artifacts_identity
            || artifact_identity != self.artifact_identity
        {
            return Err(recovery_required());
        }
        self.results.reverify(&results)?;
        Ok(())
    }
}

#[cfg(unix)]
impl BoundTerminalResults {
    fn reverify(&self, current: &Self) -> Result<(), AppError> {
        match (self, current) {
            (Self::Missing, Self::Missing) => Ok(()),
            (
                Self::InvalidFile {
                    file,
                    identity,
                },
                Self::InvalidFile {
                    file: current_file,
                    identity: current_identity,
                },
            ) if identity == current_identity => {
                let retained = verify_runtime_result_file(file)?;
                let current = verify_runtime_result_file(current_file)?;
                if retained == *identity && current == *identity {
                    Ok(())
                } else {
                    Err(recovery_required())
                }
            }
            (
                Self::Directory {
                    directory,
                    identity,
                    manifest,
                },
                Self::Directory {
                    directory: current_directory,
                    identity: current_identity,
                    manifest: current_manifest,
                },
            ) if identity == current_identity => {
                if verify_runtime_directory(directory)? != *identity
                    || verify_runtime_directory(current_directory)? != *identity
                {
                    return Err(recovery_required());
                }
                manifest.reverify(current_manifest)
            }
            _ => Err(recovery_required()),
        }
    }
}

#[cfg(unix)]
impl BoundTerminalManifest {
    fn reverify(&self, current: &Self) -> Result<(), AppError> {
        match (self, current) {
            (Self::Missing, Self::Missing) => Ok(()),
            (
                Self::Invalid {
                    file,
                    identity,
                    length,
                },
                Self::Invalid {
                    file: current_file,
                    identity: current_identity,
                    length: current_length,
                },
            ) if identity == current_identity && length == current_length => {
                if verify_runtime_manifest(file)? != *identity
                    || verify_runtime_manifest(current_file)? != *identity
                {
                    return Err(recovery_required());
                }
                Ok(())
            }
            (
                Self::Ready {
                    file,
                    identity,
                    bytes,
                },
                Self::Ready {
                    file: current_file,
                    identity: current_identity,
                    bytes: current_bytes,
                },
            ) if identity == current_identity => {
                if verify_runtime_manifest(file)? != *identity
                    || verify_runtime_manifest(current_file)? != *identity
                {
                    return Err(recovery_required());
                }
                let retained_bytes = read_runtime_manifest_snapshot(file)?;
                let current_bytes_from_descriptor = read_runtime_manifest_snapshot(current_file)?;
                if retained_bytes == *bytes
                    && current_bytes_from_descriptor == *bytes
                    && current_bytes == bytes
                {
                    Ok(())
                } else {
                    Err(recovery_required())
                }
            }
            _ => Err(recovery_required()),
        }
    }
}

#[cfg(unix)]
impl PreparedRuntimeOutputs {
    fn prepare(
        root: &VerifiedProjectRoot,
        experiment_id: &str,
    ) -> Result<Self, AppError> {
        validate_internal_id("experiment_id", experiment_id)?;
        let service = open_or_create_runtime_directory_at(
            &root.directory,
            OsStr::new(RUNTIME_SERVICE_DIRECTORY),
        )?;
        let results = open_or_create_runtime_directory_at(
            &service.0,
            OsStr::new(RUNTIME_RESULTS_DIRECTORY),
        )?;
        let artifacts = open_or_create_runtime_directory_at(
            &service.0,
            OsStr::new(RUNTIME_ARTIFACTS_DIRECTORY),
        )?;
        let artifact_directory =
            open_or_create_runtime_directory_at(&artifacts.0, OsStr::new(experiment_id))?;
        let runtime = open_or_create_runtime_directory_at(
            &service.0,
            OsStr::new(RUNTIME_OUTPUTS_DIRECTORY),
        )?;
        let runtime_experiment = create_new_directory_at(&runtime.0, OsStr::new(experiment_id))?;
        for name in RUNTIME_OUTPUT_DIRECTORY_NAMES {
            open_or_create_runtime_directory_at(&runtime_experiment, OsStr::new(name))?;
        }
        let result_name = OsString::from(format!("{experiment_id}.json"));
        let result_manifest = open_or_create_runtime_manifest_at(&results.0, &result_name)?;
        let runtime_path = root
            .anchor
            .canonical_path
            .join(RUNTIME_SERVICE_DIRECTORY)
            .join(RUNTIME_OUTPUTS_DIRECTORY)
            .join(experiment_id);
        let runtime_experiment_identity = directory_identity(&runtime_experiment)?;
        let prepared = Self {
            _service: service.0,
            _results: results.0,
            _artifacts: artifacts.0,
            runtime: runtime.0,
            _runtime_experiment: runtime_experiment,
            artifact_directory: artifact_directory.0,
            result_manifest: result_manifest.0,
            service_identity: service.1,
            results_identity: results.1,
            artifacts_identity: artifacts.1,
            runtime_identity: runtime.1,
            runtime_experiment_identity,
            artifact_identity: artifact_directory.1,
            result_manifest_identity: result_manifest.1,
            experiment_id: experiment_id.to_owned(),
            runtime_path,
        };
        prepared.reverify(root)?;
        prepared.require_empty_before_add()?;
        Ok(prepared)
    }

    fn require_empty_before_add(&self) -> Result<(), AppError> {
        let manifest = self
            .result_manifest
            .metadata()
            .map_err(|_| recovery_required())?;
        if manifest.len() != 0 {
            return Err(recovery_required());
        }
        let mut entries = fs::read_dir(descriptor_path(&self.artifact_directory))
            .map_err(|_| recovery_required())?;
        if let Some(entry) = entries.next() {
            entry.map_err(|_| recovery_required())?;
            return Err(recovery_required());
        }
        verify_runtime_output_scope(
            &self.runtime,
            self.runtime_identity,
            &self.experiment_id,
            Some(self.runtime_experiment_identity),
            true,
        )?;
        Ok(())
    }

    pub(crate) fn append_runtime_environment(
        &self,
        argv: &mut Vec<OsString>,
    ) -> Result<(), AppError> {
        if argv.len() < 1 + 4 || argv[0] != OsString::from("/usr/bin/env") {
            return Err(recovery_required());
        }
        let mut assignments = Vec::new();
        for (name, value) in [
            ("TMPDIR", check_output_path(&self.runtime_path, "tmp")?),
            ("TMP", check_output_path(&self.runtime_path, "tmp")?),
            ("TEMP", check_output_path(&self.runtime_path, "tmp")?),
            (
                "CARGO_TARGET_DIR",
                check_output_path(&self.runtime_path, "cargo-target")?,
            ),
            (
                "UV_PROJECT_ENVIRONMENT",
                check_output_path(&self.runtime_path, "uv-venv")?,
            ),
            (
                "UV_CACHE_DIR",
                check_output_path(&self.runtime_path, "uv-cache")?,
            ),
            (
                "UV_PYTHON_INSTALL_DIR",
                check_output_path(&self.runtime_path, "uv-python")?,
            ),
        ] {
            let mut assignment = OsString::from(name);
            assignment.push("=");
            assignment.push(value);
            assignments.push(assignment);
        }
        let cache = check_output_path(&self.runtime_path, "pytest-cache")?;
        let cache = cache.to_str().ok_or(validation(
            "code_change.runtime_output",
            "owned pytest cache path must be UTF-8",
        ))?;
        if cache.chars().any(char::is_control) {
            return Err(validation(
                "code_change.runtime_output",
                "owned pytest cache path contains control characters",
            ));
        }
        let escaped = cache.replace('\\', "\\\\").replace('"', "\\\"");
        let pytest = OsString::from("PYTHONDONTWRITEBYTECODE=1");
        assignments.push(pytest);
        let mut pytest = OsString::from("PYTEST_ADDOPTS=-o \"");
        pytest.push("cache_dir=");
        pytest.push(escaped);
        pytest.push("\"");
        assignments.push(pytest);
        argv.splice(1 + 4..1 + 4, assignments);
        Ok(())
    }

    pub(crate) fn reverify(&self, root: &VerifiedProjectRoot) -> Result<(), AppError> {
        let root_metadata = root.directory.metadata().map_err(|_| recovery_required())?;
        if !secure_owned_directory(&root_metadata)
            || executable_identity_from_metadata(&root_metadata) != root.anchor.identity
        {
            return Err(recovery_required());
        }
        let service = open_runtime_directory_at(
            &root.directory,
            OsStr::new(RUNTIME_SERVICE_DIRECTORY),
        )
        .map_err(|_| recovery_required())?;
        if verify_runtime_directory(&service)? != self.service_identity {
            return Err(recovery_required());
        }
        let service_entries = check_output_directory_entries(&service)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected_service_entries = [
            RUNTIME_RESULTS_DIRECTORY,
            RUNTIME_ARTIFACTS_DIRECTORY,
            RUNTIME_OUTPUTS_DIRECTORY,
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<BTreeSet<_>>();
        if service_entries != expected_service_entries {
            return Err(recovery_required());
        }
        let results = open_runtime_directory_at(&service, OsStr::new(RUNTIME_RESULTS_DIRECTORY))
            .map_err(|_| recovery_required())?;
        if verify_runtime_directory(&results)? != self.results_identity {
            return Err(recovery_required());
        }
        let artifacts = open_runtime_directory_at(&service, OsStr::new(RUNTIME_ARTIFACTS_DIRECTORY))
            .map_err(|_| recovery_required())?;
        if verify_runtime_directory(&artifacts)? != self.artifacts_identity {
            return Err(recovery_required());
        }
        let runtime = open_runtime_directory_at(&service, OsStr::new(RUNTIME_OUTPUTS_DIRECTORY))
            .map_err(|_| recovery_required())?;
        verify_runtime_output_boundary(
            &runtime,
            self.runtime_identity,
            &self._runtime_experiment,
            self.runtime_experiment_identity,
            &self.experiment_id,
        )?;
        let artifact_directory =
            open_runtime_directory_at(&artifacts, OsStr::new(&self.experiment_id))
                .map_err(|_| recovery_required())?;
        if verify_runtime_directory(&artifact_directory)? != self.artifact_identity {
            return Err(recovery_required());
        }
        let result_name = OsString::from(format!("{}.json", self.experiment_id));
        let result_manifest = open_runtime_manifest_at(&results, &result_name)?;
        if result_manifest.1 != self.result_manifest_identity {
            return Err(recovery_required());
        }
        let result_name = OsString::from(format!("{}.json", self.experiment_id));
        if check_output_directory_entries(&results)? != vec![result_name] {
            return Err(recovery_required());
        }
        Ok(())
    }
}

/// One editor launch owned by the daemon.  The worktree capability itself is
/// deliberately not retained here: the candidate path is durably owned by
/// the code-change run and is reopened with descriptor checks on the next
/// coordinator pass.
pub struct StartedCodeChangeEditor {
    pub run_id: i64,
    pub primary_event_id: i64,
    pub code_change_run_id: String,
    pub attempt: i64,
    pub event_ids: Vec<i64>,
    pub handle: AgentHandle,
}

/// Bounded accounting for one code-change advancement pass.  Agent handles
/// and cleanup owners are returned to the daemon so it remains the sole
/// owner of live native processes and private temporary directories.
pub struct CodeChangeReport {
    pub started: Vec<StartedCodeChangeEditor>,
    pub cleanup: Vec<BoundCleanupHandle>,
    pub advanced: usize,
    pub deferred: usize,
    pub rejected: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupEditorMarkerEvidence {
    Absent,
    ConfirmedPending,
    ConfirmedReleaseRequested,
    IndeterminatePending,
    IndeterminateReleaseRequested,
}

impl Default for CodeChangeReport {
    fn default() -> Self {
        Self {
            started: Vec::new(),
            cleanup: Vec::new(),
            advanced: 0,
            deferred: 0,
            rejected: 0,
        }
    }
}

impl std::fmt::Debug for CodeChangeReport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodeChangeReport")
            .field("started", &self.started.len())
            .field("cleanup", &self.cleanup.len())
            .field("advanced", &self.advanced)
            .field("deferred", &self.deferred)
            .field("rejected", &self.rejected)
            .finish()
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromotionRefEvidence {
    ExpectedOld,
    Candidate,
    Unrelated,
}

#[cfg(unix)]
pub struct CodeChangeCoordinator<'a> {
    db: &'a Db,
    runner: &'a AgentRunner,
    policy: &'a ResolvedExecutionPolicy,
    limits: CampaignLimits,
    lease_seconds: i64,
    check_timeout_override: Option<Duration>,
    #[cfg(debug_assertions)]
    pre_cas_best_ref_swap_for_test: Arc<Mutex<Option<String>>>,
    #[cfg(debug_assertions)]
    pre_cas_update_ref_attempts_for_test: Arc<AtomicU64>,
}

#[cfg(unix)]
impl<'a> CodeChangeCoordinator<'a> {
    pub fn new(
        db: &'a Db,
        runner: &'a AgentRunner,
        policy: &'a ResolvedExecutionPolicy,
        limits: CampaignLimits,
    ) -> Self {
        Self {
            db,
            runner,
            policy,
            limits,
            lease_seconds: 600,
            check_timeout_override: None,
            #[cfg(debug_assertions)]
            pre_cas_best_ref_swap_for_test: Arc::new(Mutex::new(None)),
            #[cfg(debug_assertions)]
            pre_cas_update_ref_attempts_for_test: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_lease_seconds(mut self, lease_seconds: i64) -> Self {
        self.lease_seconds = lease_seconds.max(1);
        self
    }

    /// Test-only timeout seam for the bounded CheckRunner integration path.
    /// Production construction always uses the startup-pinned policy timeout.
    #[doc(hidden)]
    pub fn with_code_change_check_timeout_for_test(mut self, timeout: Duration) -> Self {
        self.check_timeout_override = Some(timeout);
        self
    }

    /// Test-only seam for a competing best-ref update after promotion intent
    /// is observed and immediately before the coordinator's CAS helper.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn with_pre_cas_best_ref_swap_for_test(self, replacement_sha: String) -> Self {
        *self
            .pre_cas_best_ref_swap_for_test
            .lock()
            .expect("code-change test hook mutex") = Some(replacement_sha);
        self
    }

    /// Return the number of production best-ref CAS commands attempted by the
    /// test seam on this coordinator.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn pre_cas_update_ref_attempts_for_test(&self) -> u64 {
        self.pre_cas_update_ref_attempts_for_test
            .load(Ordering::SeqCst)
    }

    #[cfg(debug_assertions)]
    fn take_pre_cas_best_ref_swap_for_test(&self) -> Option<String> {
        let mut hook = self
            .pre_cas_best_ref_swap_for_test
            .lock()
            .expect("code-change test hook mutex");
        hook.take()
    }

    #[cfg(not(debug_assertions))]
    fn take_pre_cas_best_ref_swap_for_test(&self) -> Option<String> {
        None
    }

    #[cfg(debug_assertions)]
    fn pre_cas_update_ref_attempt_counter(&self) -> Option<Arc<AtomicU64>> {
        Some(Arc::clone(&self.pre_cas_update_ref_attempts_for_test))
    }

    #[cfg(not(debug_assertions))]
    fn pre_cas_update_ref_attempt_counter(&self) -> Option<Arc<AtomicU64>> {
        None
    }

    pub async fn recover_startup_editors(
        &self,
        now: i64,
        preserved_agent_run_ids: &[i64],
        retry_policies: &BTreeMap<String, RetryPolicy>,
        _confirmed_pending_marker_ids: &BTreeSet<i64>,
        _confirmed_release_requested_ids: &BTreeSet<i64>,
        _indeterminate_pending_marker_ids: &BTreeSet<i64>,
        _indeterminate_release_requested_ids: &BTreeSet<i64>,
    ) -> Result<CodeChangeReport, AppError> {
        let code_change_repository = CodeChangeRepository::new(self.db);
        let agent_run_repository = AgentRunRepository::new(self.db);
        let mut report = CodeChangeReport::default();
        const PRE_MARKER_FAILURE_CODE: &str = "editor_launch";
        const PRE_MARKER_FAILURE_SUMMARY: &str =
            "editor startup did not reach the execution marker";
        const UNCERTAIN_FAILURE_CODE: &str = "editor_startup_uncertain";
        const UNCERTAIN_FAILURE_SUMMARY: &str =
            "editor startup execution outcome is unknown after restart";

        for agent_run_id in preserved_agent_run_ids {
            let Some((code_change_run_id, attempt)) = code_change_repository
                .find_editor_attempt_for_agent_run(*agent_run_id)?
            else {
                continue;
            };
            let Some(agent_run) = agent_run_repository.find_by_id(*agent_run_id)? else {
                continue;
            };
            if !matches!(
                agent_run.status,
                AgentRunStatus::Starting | AgentRunStatus::Running
            ) {
                continue;
            }
            let retry_policy = retry_policies
                .get(&agent_run.project_id)
                .copied()
                .ok_or(AppError::Validation {
                    field: "project_id",
                    message: "startup recovery retry policy is missing for code-change editor project",
                })?;

            let marker_evidence = match (
                _confirmed_pending_marker_ids.contains(agent_run_id),
                _confirmed_release_requested_ids.contains(agent_run_id),
                _indeterminate_pending_marker_ids.contains(agent_run_id),
                _indeterminate_release_requested_ids.contains(agent_run_id),
            ) {
                (false, false, false, false) => StartupEditorMarkerEvidence::Absent,
                (true, false, false, false) => {
                    StartupEditorMarkerEvidence::ConfirmedPending
                }
                (false, true, false, false) => {
                    StartupEditorMarkerEvidence::ConfirmedReleaseRequested
                }
                (false, false, true, false) => {
                    StartupEditorMarkerEvidence::IndeterminatePending
                }
                (false, false, false, true) => {
                    StartupEditorMarkerEvidence::IndeterminateReleaseRequested
                }
                _ => {
                    return Err(AppError::Validation {
                        field: "launch_gate_state",
                        message: "startup marker evidence is contradictory",
                    });
                }
            };
            let startup_uncertain = !matches!(marker_evidence, StartupEditorMarkerEvidence::Absent)
                || matches!(
                    agent_run.launch_gate_state.as_str(),
                    "released" | "release_requested"
                );
            if attempt.status == "failed"
                && attempt.failure_code.as_deref() == Some(UNCERTAIN_FAILURE_CODE)
            {
                code_change_repository.require_recovery(
                    &code_change_run_id,
                    UNCERTAIN_FAILURE_CODE,
                    UNCERTAIN_FAILURE_SUMMARY,
                    now,
                )?;
                agent_run_repository.finish_code_change_editor_startup_uncertain(
                    &agent_run.project_id,
                    *agent_run_id,
                    now,
                    UNCERTAIN_FAILURE_SUMMARY,
                )?;
                report.advanced += 1;
                continue;
            }
            if startup_uncertain && matches!(attempt.status.as_str(), "reserved" | "running") {
                if !code_change_repository.fail_editor_attempt_for_agent_run(
                    *agent_run_id,
                    UNCERTAIN_FAILURE_CODE,
                    UNCERTAIN_FAILURE_SUMMARY,
                    now,
                )? {
                    continue;
                }
                code_change_repository.require_recovery(
                    &code_change_run_id,
                    UNCERTAIN_FAILURE_CODE,
                    UNCERTAIN_FAILURE_SUMMARY,
                    now,
                )?;
                agent_run_repository.finish_code_change_editor_startup_uncertain(
                    &agent_run.project_id,
                    *agent_run_id,
                    now,
                    UNCERTAIN_FAILURE_SUMMARY,
                )?;
                report.advanced += 1;
                continue;
            }

            let (status, exit_code, last_error) = match attempt.status.as_str() {
                "ready" => (AgentRunStatus::Completed, Some(0), None),
                "failed" => (
                    AgentRunStatus::Failed,
                    None,
                    attempt.failure_summary.as_deref(),
                ),
                "reserved" | "running"
                    if agent_run.launch_gate_state == "pending"
                        && !(_confirmed_pending_marker_ids.contains(agent_run_id)
                            || _confirmed_release_requested_ids.contains(agent_run_id)
                            || _indeterminate_pending_marker_ids.contains(agent_run_id)
                            || _indeterminate_release_requested_ids.contains(agent_run_id)) =>
                {
                    if !code_change_repository.fail_editor_attempt_for_agent_run(
                        *agent_run_id,
                        PRE_MARKER_FAILURE_CODE,
                        PRE_MARKER_FAILURE_SUMMARY,
                        now,
                    )? {
                        continue;
                    }
                    (AgentRunStatus::Failed, None, Some(PRE_MARKER_FAILURE_SUMMARY))
                }
                _ => {
                    report.deferred += 1;
                    continue;
                }
            };

            agent_run_repository.finish_code_change_editor_startup_terminal(
                &agent_run.project_id,
                *agent_run_id,
                status,
                now,
                exit_code,
                last_error,
                retry_policy,
            )?;
            report.advanced += 1;
        }

        Ok(report)
    }

    /// Advance the bounded code-change lifecycle.  Every external operation
    /// is preceded by a fresh DB/state-root read and every launch is bound to
    /// an attempt before native gate release.
    pub async fn advance_ready(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<CodeChangeReport, AppError> {
        let repository = CodeChangeRepository::new(self.db);
        let runs = repository.list_recoverable(limit)?;
        let mut report = CodeChangeReport::default();
        for run in runs {
            if run.state == CodeChangeState::ExperimentSubmitted {
                self.advance_submitted_code_change(&run, now, &mut report)
                    .await?;
                continue;
            }
            if run.state == CodeChangeState::Evaluated {
                match repository.transition(
                    &run.code_change_run_id,
                    CodeChangeState::Evaluated,
                    CodeChangeState::CleanupPending,
                    now,
                ) {
                    Ok(_) => report.advanced += 1,
                    Err(_) => report.deferred += 1,
                }
                continue;
            }
            if matches!(run.state, CodeChangeState::CleanupPending | CodeChangeState::Rejected) {
                self.advance_cleanup_code_change(&run, now, &mut report)
                    .await?;
                continue;
            }
            // Rejected runs with a NULL cleanup marker remain recoverable as
            // the durable Task 6 cleanup schedule.  Task 4 ends after
            // terminal editor persistence and must not reopen or clean them.
            if !matches!(
                run.state,
                CodeChangeState::Reserved
                    | CodeChangeState::PreparingWorktree
                    | CodeChangeState::Editing
                    | CodeChangeState::Checking
                    | CodeChangeState::Committing
            ) {
                continue;
            }

            let Some(campaign) = CampaignRepository::new(self.db).find_by_id(&run.campaign_id)?
            else {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "campaign_missing",
                    "code-change campaign is missing",
                    now,
                )?;
                report.rejected += 1;
                continue;
            };
            let Some(project) = ProjectRepository::new(self.db).find_by_id(&campaign.project_id)?
            else {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "project_missing",
                    "code-change project is missing",
                    now,
                )?;
                report.rejected += 1;
                continue;
            };
            if campaign.state != CampaignState::Active
                || !project.enabled
                || project.paused
                || project.halted_reason.is_some()
            {
                report.deferred += 1;
                continue;
            }
            if AgentRunRepository::new(self.db)
                .find_active_by_project(&project.project_id)?
                .is_some()
            {
                report.deferred += 1;
                continue;
            }

            let project_config = match config::load(&project.config_path) {
                Ok(config) => config,
                Err(_) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "project_config_invalid",
                        "code-change project configuration could not be loaded",
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
            };
            let original_policy = match self.runner.resolve_project_policy(&project, &project_config)
            {
                Ok(policy) => policy,
                Err(_) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "execution_policy_invalid",
                        "code-change execution policy could not be resolved",
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
            };
            let proposal = ProposalRepository::new(self.db)
                .find_for_campaign(&run.campaign_id, &run.proposal_id)?
                .ok_or(AppError::Validation {
                    field: "code_change.proposal_id",
                    message: "code-change proposal is missing from its campaign",
                })?;

            let mut candidate = match run.state {
                CodeChangeState::Reserved | CodeChangeState::PreparingWorktree => {
                    let Some(project_lock) = self
                        .runner
                        .try_acquire_project_admission_lock(&original_policy)
                        .map_err(AppError::from)?
                    else {
                        report.deferred += 1;
                        continue;
                    };
                    if run.state == CodeChangeState::Reserved {
                        repository.transition(
                            &run.code_change_run_id,
                            CodeChangeState::Reserved,
                            CodeChangeState::PreparingWorktree,
                            now,
                        )?;
                    }
                    let candidate = prepare_code_change_worktree_for_run(
                        self.policy,
                        &project,
                        &original_policy,
                        self.db,
                        &run.code_change_run_id,
                    )
                    .await;
                    drop(project_lock);
                    match candidate {
                        Ok(candidate) => candidate,
                        Err(error) => {
                            repository.require_recovery(
                                &run.code_change_run_id,
                                "worktree_recovery_required",
                                &bounded_redacted_text(&error.to_string()),
                                now,
                            )?;
                            report.rejected += 1;
                            continue;
                        }
                    }
                }
                CodeChangeState::Editing
                | CodeChangeState::Checking
                | CodeChangeState::Committing => {
                    match reopen_code_change_worktree_for_run(
                        self.policy,
                        &project,
                        &original_policy,
                        self.db,
                        &run.code_change_run_id,
                    )
                    .await
                    {
                        Ok(candidate) => candidate,
                        Err(error) => {
                            repository.require_recovery(
                                &run.code_change_run_id,
                                "worktree_recovery_required",
                                &bounded_redacted_text(&error.to_string()),
                                now,
                            )?;
                            report.rejected += 1;
                            continue;
                        }
                    }
                }
                _ => continue,
            };

            if matches!(
                run.state,
                CodeChangeState::Reserved | CodeChangeState::PreparingWorktree
            ) {
                repository.transition(
                    &run.code_change_run_id,
                    CodeChangeState::PreparingWorktree,
                    CodeChangeState::Editing,
                    now,
                )?;
            }
            let candidate_policy = match self
                .policy
                .for_code_change_worktree(&project, &original_policy, candidate.root())
            {
                Ok(policy) => policy,
                Err(_) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "candidate_policy_invalid",
                        "candidate root failed execution-policy rebinding",
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
            };

            if run.state == CodeChangeState::Committing {
                match self
                    .resume_committing_candidate(
                        &repository,
                        &run,
                        &mut candidate,
                        now,
                        &mut report,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(error) => {
                        repository.require_recovery(
                            &run.code_change_run_id,
                            "worktree_recovery_required",
                            &bounded_redacted_text(&error.to_string()),
                            now,
                        )?;
                        report.rejected += 1;
                    }
                }
                continue;
            }

            let attempts = repository.list_editor_attempts(&run.code_change_run_id)?;
            if attempts.len() > 2 || run.editor_attempts > 2 {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "editor_attempt_overflow",
                    "code-change editor attempts exceed the bounded two-attempt policy",
                    now,
                )?;
                report.rejected += 1;
                continue;
            }
            if let Some(last) = attempts.last() {
                if matches!(run.state, CodeChangeState::Editing | CodeChangeState::Checking)
                    && last.status == "ready"
                    && last.failure_code.as_deref() != Some("check_failed")
                {
                    let rows = repository.list_checks(&run.code_change_run_id, last.attempt)?;
                    let editor_checks = editor_project_checks(&rows)?;
                    if let Err(error) = self
                        .advance_check_round(
                        &repository,
                        &run,
                        last.attempt,
                        &mut candidate,
                        &editor_checks,
                        self.check_timeout_override,
                        now,
                        &mut report,
                    )
                        .await
                    {
                        repository.require_recovery(
                            &run.code_change_run_id,
                            "check_round_recovery_required",
                            &bounded_redacted_text(&error.to_string()),
                            now,
                        )?;
                        report.rejected += 1;
                    }
                    continue;
                }
            }
            let attempt = match attempts.last() {
                None if run.editor_attempts == 0 => 1,
                None => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "editor_attempt_binding_missing",
                        "editor attempt counter has no durable attempt row",
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
                Some(attempt)
                    if matches!(attempt.status.as_str(), "reserved" | "running") => {
                        report.deferred += 1;
                        continue;
                    }
                Some(attempt) if attempt.status == "ready" => {
                    if attempt.attempt == 1
                        && attempt.failure_code.as_deref() == Some("check_failed")
                    {
                        2
                    } else {
                        // Task 4 ends at terminal editor persistence.  Task 5
                        // consumes the accepted output and advances to checks.
                        report.deferred += 1;
                        continue;
                    }
                }
                Some(attempt) if attempt.status == "failed" && attempt.attempt == 1 => {
                    if attempt.failure_code.as_deref() == Some("cannot_apply")
                        || attempt.failure_code.as_deref() == Some("editor_session_missing")
                    {
                        repository.reject(
                            &run.code_change_run_id,
                            attempt.failure_code.as_deref().unwrap_or("cannot_apply"),
                            attempt
                                .failure_summary
                                .as_deref()
                                .unwrap_or("editor cannot apply the requested change"),
                            now,
                        )?;
                        report.rejected += 1;
                        continue;
                    }
                    2
                }
                Some(attempt) if attempt.status == "failed" && attempt.attempt == 2 => {
                    repository.reject(
                        &run.code_change_run_id,
                        attempt.failure_code.as_deref().unwrap_or("editor_failed"),
                        attempt
                            .failure_summary
                            .as_deref()
                            .unwrap_or("second editor attempt failed"),
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
                Some(_) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "editor_attempt_state_invalid",
                        "code-change editor attempt has an invalid terminal state",
                        now,
                    )?;
                    report.rejected += 1;
                    continue;
                }
            };
            if attempt == 2 && run.editor_attempts != 1 {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "editor_attempt_counter_mismatch",
                    "resume attempt does not match the durable attempt counter",
                    now,
                )?;
                report.rejected += 1;
                continue;
            }
            let session_id = if attempt == 2 {
                run.editor_session_id.clone().ok_or(AppError::Validation {
                    field: "code_change.editor_session_id",
                    message: "resume attempt requires one durable session ID",
                })?
            } else {
                run.editor_session_id.clone().unwrap_or_default()
            };
            let prompt = editor_prompt(&campaign, &proposal, &run, candidate.path())?;
            let mut editor_config = project_config.agent.clone();
            editor_config.context = if attempt == 1 {
                AgentContextMode::Fresh
            } else {
                AgentContextMode::Resume { session_id }
            };

            let Some(run_id_guard) = self
                .runner
                .try_acquire_run_id_admission_guard(self.db)
                .map_err(AppError::from)?
            else {
                report.deferred += 1;
                continue;
            };
            let Some(project_lock) = self
                .runner
                .try_acquire_project_admission_lock(&candidate_policy)
                .map_err(AppError::from)?
            else {
                report.deferred += 1;
                continue;
            };
            let event = NewEvent::new(
                project.project_id.clone(),
                EventKind::CodeChange,
                format!(
                    "code-change-editor:v1:{}:{}",
                    run.code_change_run_id, attempt
                ),
                serde_json::json!({
                    "code_change_run_id": run.code_change_run_id,
                    "campaign_id": run.campaign_id,
                    "proposal_id": run.proposal_id,
                    "attempt": attempt,
                }),
                now,
                now,
            )
            .with_campaign_lineage(run.campaign_id.clone(), Option::<String>::None);
            let event = EventRepository::new(self.db).insert_idempotent(&event)?;
            let event = if matches!(event.status, EventStatus::Pending | EventStatus::RetryWait) {
                EventRepository::new(self.db)
                    .claim_by_id(
                        &project.project_id,
                        event.event_id,
                        now.checked_add(self.lease_seconds).ok_or(AppError::Validation {
                            field: "lease_seconds",
                            message: "cannot represent editor event lease",
                        })?,
                    )?
            } else {
                None
            };
            let Some(event) = event else {
                drop((run_id_guard, project_lock));
                report.deferred += 1;
                continue;
            };
            let budget_key = format!(
                "code-change-editor:v1:{}:{}",
                run.code_change_run_id, attempt
            );
            match CampaignRepository::new(self.db).reserve_agent_run(
                &run.campaign_id,
                &budget_key,
                &self.limits,
                now,
            )? {
                AgentDecisionReservation::Reserved(_) => {}
                AgentDecisionReservation::BudgetWaiting { next_eligible_at } => {
                    EventRepository::new(self.db).transition_many(
                        &[event.event_id],
                        EventStatus::RetryWait,
                        now,
                        Some(next_eligible_at),
                        None,
                    )?;
                    report.deferred += 1;
                    continue;
                }
                AgentDecisionReservation::Deferred { .. } => {
                    EventRepository::new(self.db).defer_claimed(&[event.event_id])?;
                    report.deferred += 1;
                    continue;
                }
            }
            let started = self
                .runner
                .spawn_code_change_editor(
                    self.db,
                    &project,
                    &candidate_policy,
                    &editor_config,
                    RetryPolicy {
                        max_retries: project_config.agent.max_retries,
                    },
                    event.event_id,
                    &[event.event_id],
                    &run.code_change_run_id,
                    attempt,
                    &prompt,
                    now,
                    run_id_guard,
                    project_lock,
                )
                .await;
            match started {
                Ok(handle) => {
                    report.advanced += 1;
                    report.started.push(StartedCodeChangeEditor {
                        run_id: handle.run_id,
                        primary_event_id: event.event_id,
                        code_change_run_id: run.code_change_run_id.clone(),
                        attempt,
                        event_ids: vec![event.event_id],
                        handle,
                    });
                }
                Err(error) => {
                    let AgentSpawnError {
                        stage,
                        source,
                        policy,
                        cleanup,
                    } = error;
                    if let Some(cleanup) = cleanup {
                        report.cleanup.push(cleanup);
                    }
                    if matches!(stage, AgentSpawnStage::PreBinding) {
                        let events = EventRepository::new(self.db);
                        let retry_policy = RetryPolicy {
                            max_retries: project_config.agent.max_retries,
                        };
                        let terminal_resolution = policy.is_some()
                            || matches!(
                                crate::retry::retry_decision(event.attempts, now, retry_policy),
                                crate::retry::RetryDecision::DeadLetter
                            );
                        // Record a terminal code-change state before the
                        // unbound event is dead-lettered.  If the process
                        // stops between these independent repository calls,
                        // startup still sees a terminal run rather than an
                        // unclaimable editing row.
                        if terminal_resolution {
                            repository.require_recovery(
                                &run.code_change_run_id,
                                "editor_launch_failed",
                                &bounded_redacted_text(&source.to_string()),
                                now,
                            )?;
                        }
                        if let Some(violation) = policy.as_ref() {
                            events.dead_letter_claimed_without_run(
                                &project.project_id,
                                &[event.event_id],
                                now,
                                violation,
                            )?;
                        } else {
                            events.resolve_claimed_without_run(
                                &project.project_id,
                                &[event.event_id],
                                now,
                                &source.to_string(),
                                retry_policy,
                            )?;
                        }
                        let resolved_event = events
                            .find_by_id(event.event_id)?
                            .ok_or(AppError::Validation {
                                field: "event_id",
                                message: "editor event disappeared during launch failure resolution",
                            })?;
                        if resolved_event.status == EventStatus::DeadLetter {
                            if !terminal_resolution {
                                repository.require_recovery(
                                    &run.code_change_run_id,
                                    "editor_launch_failed",
                                    &bounded_redacted_text(&source.to_string()),
                                    now,
                                )?;
                            }
                            report.rejected += 1;
                        } else {
                            report.deferred += 1;
                        }
                    } else {
                        report.deferred += 1;
                    }
                }
            }
        }
        Ok(report)
    }

    /// Reconcile terminal code-change submissions and cleanup after a restart.
    /// This entry point is intentionally separate from editor advancement so
    /// restart recovery can consume a persisted promotion intent without
    /// launching another agent.
    pub async fn recover_interrupted(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<CodeChangeReport, AppError> {
        let repository = CodeChangeRepository::new(self.db);
        let runs = repository.list_recoverable(limit)?;
        let mut report = CodeChangeReport::default();
        for run in runs {
            if run.state == CodeChangeState::ExperimentSubmitted {
                self.advance_submitted_code_change(&run, now, &mut report)
                    .await?;
                continue;
            }
            if run.state == CodeChangeState::Evaluated {
                match repository.transition(
                    &run.code_change_run_id,
                    CodeChangeState::Evaluated,
                    CodeChangeState::CleanupPending,
                    now,
                ) {
                    Ok(_) => report.advanced += 1,
                    Err(_) => report.deferred += 1,
                }
                continue;
            }
            if matches!(run.state, CodeChangeState::CleanupPending | CodeChangeState::Rejected) {
                self.advance_cleanup_code_change(&run, now, &mut report)
                    .await?;
            }
        }
        Ok(report)
    }

    async fn advance_cleanup_code_change(
        &self,
        run: &CodeChangeRun,
        _now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        let authorization = match CodeChangeCleanupAuthorization::load(
            self.db,
            &run.code_change_run_id,
        ) {
            Ok(authorization) => authorization,
            Err(_) => {
                report.deferred += 1;
                return Ok(());
            }
        };
        let run = match authorization.fresh_run() {
            Ok(run) => run,
            Err(_) => {
                report.deferred += 1;
                return Ok(());
            }
        };
        if !matches!(run.state, CodeChangeState::CleanupPending | CodeChangeState::Rejected)
            || run.cleanup_completed_at.is_some()
        {
            return Ok(());
        }
        let Some(campaign) = CampaignRepository::new(self.db).find_by_id(&run.campaign_id)? else {
            report.deferred += 1;
            return Ok(());
        };
        let Some(project) = ProjectRepository::new(self.db).find_by_id(&campaign.project_id)? else {
            report.deferred += 1;
            return Ok(());
        };
        let project_config = match config::load(&project.config_path) {
            Ok(config) => config,
            Err(_) => {
                report.deferred += 1;
                return Ok(());
            }
        };
        let original_policy = match self.runner.resolve_project_policy(&project, &project_config) {
            Ok(policy) => policy,
            Err(_) => {
                report.deferred += 1;
                return Ok(());
            }
        };

        let cleanup = if run.candidate_sha.is_some() {
            let candidate = match run.experiment_id.as_deref() {
                Some(experiment_id) => {
                    reopen_code_change_result_for_run(
                        self.policy,
                        &project,
                        &original_policy,
                        self.db,
                        &run.code_change_run_id,
                        experiment_id,
                    )
                    .await
                }
                None => {
                    reopen_code_change_candidate_for_run(
                        self.policy,
                        &project,
                        &original_policy,
                        self.db,
                        &run.code_change_run_id,
                    )
                    .await
                }
            };
            match candidate {
                Ok(candidate) => candidate.cleanup(&authorization).await,
                Err(error) => Err(error),
            }
        } else if run.state == CodeChangeState::Rejected
            && run.state_root_identity.is_some()
        {
            match reopen_code_change_worktree_for_run(
                self.policy,
                &project,
                &original_policy,
                self.db,
                &run.code_change_run_id,
            )
            .await
            {
                Ok(candidate) => candidate.cleanup(&authorization).await,
                Err(error) => Err(error),
            }
        } else {
            let expected_relative = match owned_worktree_relative_path(
                &run.campaign_id,
                &run.proposal_id,
            ) {
                Ok(path) => path,
                Err(_error) => {
                    report.deferred += 1;
                    return Ok(());
                }
            };
            let expected_candidate_ref = match candidate_ref(&run.campaign_id, &run.proposal_id) {
                Ok(reference) => reference,
                Err(_error) => {
                    report.deferred += 1;
                    return Ok(());
                }
            };
            let expected_best_ref = match best_ref(&run.campaign_id) {
                Ok(reference) => reference,
                Err(_error) => {
                    report.deferred += 1;
                    return Ok(());
                }
            };
            if Path::new(&run.worktree_relative_path) != expected_relative.as_path()
                || run.worktree_id != run.code_change_run_id
                || run.candidate_ref != expected_candidate_ref
                || run.best_ref != expected_best_ref
            {
                Err(recovery_required())
            } else {
                let original_base_sha = match campaign_start_base_sha(self.db, &run.campaign_id) {
                    Ok(sha) => sha,
                    Err(_) => {
                        report.deferred += 1;
                        return Ok(());
                    }
                };
                match WorktreeManager::new_for_run(
                    self.policy,
                    &project,
                    &original_policy,
                    &run.campaign_id,
                    &run.proposal_id,
                    &run.base_sha,
                    &original_base_sha,
                    &run.worktree_id,
                    &expected_relative,
                ) {
                    Ok(manager) => manager
                        .cleanup_pre_candidate_authorized(&authorization)
                        .await,
                    Err(error) => Err(error),
                }
            }
        };
        match cleanup {
            Ok(()) => report.advanced += 1,
            Err(_) => report.deferred += 1,
        }
        Ok(())
    }

    async fn advance_submitted_code_change(
        &self,
        run: &CodeChangeRun,
        now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        let repository = CodeChangeRepository::new(self.db);
        let Some(campaign) = CampaignRepository::new(self.db).find_by_id(&run.campaign_id)? else {
            repository.require_recovery(
                &run.code_change_run_id,
                "campaign_missing",
                "code-change campaign is missing",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        };
        let Some(project) = ProjectRepository::new(self.db).find_by_id(&campaign.project_id)? else {
            repository.require_recovery(
                &run.code_change_run_id,
                "project_missing",
                "code-change project is missing",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        };
        let project_config = match config::load(&project.config_path) {
            Ok(config) => config,
            Err(_) => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "project_config_invalid",
                    "code-change project configuration could not be loaded",
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        let original_policy = match self.runner.resolve_project_policy(&project, &project_config) {
            Ok(policy) => policy,
            Err(_) => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "execution_policy_invalid",
                    "code-change execution policy could not be resolved",
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        let Some(experiment_id) = run.experiment_id.as_deref() else {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_source_invalid",
                "code-change promotion has no linked experiment",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        };
        let Some(experiment) = ExperimentRepository::new(self.db).find_by_id(experiment_id)? else {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_source_invalid",
                "code-change promotion experiment is missing",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        };
        let Some(proposal) =
            ProposalRepository::new(self.db).find_for_campaign(&run.campaign_id, &run.proposal_id)?
        else {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_source_invalid",
                "code-change promotion proposal is missing",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        };
        let source_lineage_valid = proposal.source_experiment_id.is_some()
            && experiment.campaign_id == run.campaign_id
            && experiment.proposal_id == run.proposal_id
            && experiment.code_change_run_id.as_deref() == Some(&run.code_change_run_id)
            && experiment.code_revision_sha.as_deref() == run.candidate_sha.as_deref()
            && experiment.parent_experiment_id.as_deref()
                == proposal.source_experiment_id.as_deref();
        if !source_lineage_valid {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_source_invalid",
                "code-change promotion experiment does not match its terminal source",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        }
        if !is_terminal_experiment_status(experiment.status) {
            report.deferred += 1;
            return Ok(());
        }
        let mut candidate = match reopen_code_change_result_for_run(
            self.policy,
            &project,
            &original_policy,
            self.db,
            &run.code_change_run_id,
            experiment_id,
        )
        .await
        {
            Ok(candidate) => candidate,
            Err(error) => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_candidate_invalid",
                    &bounded_redacted_text(&error.to_string()),
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        if let Err(error) = candidate
            .verify_promotion_candidate(run, experiment_id)
            .await
        {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_candidate_invalid",
                &bounded_redacted_text(&error.to_string()),
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        }
        let observed_best = match candidate.best_ref_sha().await {
            Ok(best) => best,
            Err(error) => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_best_ref_invalid",
                    &bounded_redacted_text(&error.to_string()),
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        let first_promotion_pass = run.promotion_outcome.is_none()
            && run.promotion_expected_best_experiment_id.is_none()
            && run.promotion_expected_old_sha.is_none()
            && run.promotion_target_sha.is_none();
        let expected_old_sha = if first_promotion_pass {
            observed_best.as_deref()
        } else {
            run.promotion_expected_old_sha.as_deref()
        };
        let run = match repository.prepare_code_promotion(
            &run.code_change_run_id,
            experiment.status,
            &self.limits,
            expected_old_sha,
            now,
        ) {
            Ok(run) => run,
            Err(error) => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_prepare_failed",
                    &bounded_redacted_text(&error.to_string()),
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        let outcome = match run.promotion_outcome.as_deref() {
            Some(outcome) => match PromotionOutcome::from_str(outcome) {
                Ok(outcome) => outcome,
                Err(error) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "promotion_intent_invalid",
                        &bounded_redacted_text(&error.to_string()),
                        now,
                    )?;
                    report.rejected += 1;
                    return Ok(());
                }
            },
            None => {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_intent_invalid",
                    "code-change promotion intent is incomplete",
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        };
        if outcome == PromotionOutcome::Improved {
            let candidate_sha = run.candidate_sha.as_deref().ok_or_else(recovery_required)?;
            let target_sha = run.promotion_target_sha.as_deref();
            if target_sha != Some(candidate_sha) {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_target_invalid",
                    "improved code-change intent does not target its candidate SHA",
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
            let classify = |actual: Option<&str>| {
                if actual == target_sha {
                    PromotionRefEvidence::Candidate
                } else if actual == run.promotion_expected_old_sha.as_deref() {
                    PromotionRefEvidence::ExpectedOld
                } else {
                    PromotionRefEvidence::Unrelated
                }
            };
            match classify(observed_best.as_deref()) {
                PromotionRefEvidence::Candidate => {}
                PromotionRefEvidence::ExpectedOld => {
                    let replacement_sha = self.take_pre_cas_best_ref_swap_for_test();
                    let attempt_counter = self.pre_cas_update_ref_attempt_counter();
                    if let Err(error) = candidate
                        .update_best_ref_cas_for_promotion(
                            self.db,
                            &run.code_change_run_id,
                            run.promotion_expected_old_sha.as_deref(),
                            replacement_sha.as_deref(),
                            attempt_counter,
                        )
                        .await
                    {
                        let after_cas = match candidate.best_ref_sha().await {
                            Ok(best) => best,
                            Err(read_error) => {
                                repository.require_recovery(
                                    &run.code_change_run_id,
                                    "promotion_best_ref_invalid",
                                    &bounded_redacted_text(&read_error.to_string()),
                                    now,
                                )?;
                                report.rejected += 1;
                                return Ok(());
                            }
                        };
                        match classify(after_cas.as_deref()) {
                            PromotionRefEvidence::Candidate => {}
                            PromotionRefEvidence::ExpectedOld => {
                                report.deferred += 1;
                                return Ok(());
                            }
                            PromotionRefEvidence::Unrelated => {
                                repository.require_recovery(
                                    &run.code_change_run_id,
                                    "promotion_best_ref_conflict",
                                    &bounded_redacted_text(&error.to_string()),
                                    now,
                                )?;
                                report.rejected += 1;
                                return Ok(());
                            }
                        }
                    }
                }
                PromotionRefEvidence::Unrelated => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "promotion_best_ref_conflict",
                        "best ref changed since code-change promotion intent",
                        now,
                    )?;
                    report.rejected += 1;
                    return Ok(());
                }
            }
            let final_best = match candidate.best_ref_sha().await {
                Ok(best) => best,
                Err(error) => {
                    repository.require_recovery(
                        &run.code_change_run_id,
                        "promotion_best_ref_invalid",
                        &bounded_redacted_text(&error.to_string()),
                        now,
                    )?;
                    report.rejected += 1;
                    return Ok(());
                }
            };
            if final_best.as_deref() != target_sha {
                repository.require_recovery(
                    &run.code_change_run_id,
                    "promotion_best_ref_conflict",
                    "best ref is not the exact persisted candidate SHA",
                    now,
                )?;
                report.rejected += 1;
                return Ok(());
            }
        }
        if let Err(error) = candidate
            .verify_promotion_candidate(&run, experiment_id)
            .await
        {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_candidate_invalid",
                &bounded_redacted_text(&error.to_string()),
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        }
        if let Err(error) = repository.finalize_code_promotion(
            &run.code_change_run_id,
            &self.limits,
            now,
        ) {
            repository.require_recovery(
                &run.code_change_run_id,
                "promotion_finalize_failed",
                &bounded_redacted_text(&error.to_string()),
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        }
        repository.transition(
            &run.code_change_run_id,
            CodeChangeState::Evaluated,
            CodeChangeState::CleanupPending,
            now,
        )?;
        report.advanced += 1;
        Ok(())
    }

    fn finish_failed_check_round(
        &self,
        repository: &CodeChangeRepository<'_>,
        run: &CodeChangeRun,
        attempt: i64,
        result: &CheckRoundResult,
        now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        if result.project_check_count == 0 {
            repository.reject(
                &run.code_change_run_id,
                "project_check_missing",
                "code-change requires at least one project check",
                now,
            )?;
            report.rejected += 1;
            return Ok(());
        }
        let summary = if !result.git_diff_passed {
            "Git diff check failed"
        } else if !result.all_project_checks_passed {
            "project check failed"
        } else {
            "candidate diff changed during checks"
        };
        match attempt {
            1 => {
                repository.retry_after_failed_checks(
                    &run.code_change_run_id,
                    attempt,
                    summary,
                    now,
                )?;
                report.advanced += 1;
                Ok(())
            }
            2 => {
                repository.reject(&run.code_change_run_id, "check_failed", summary, now)?;
                report.rejected += 1;
                Ok(())
            }
            _ => Err(recovery_required()),
        }
    }

    async fn commit_checked_candidate(
        &self,
        repository: &CodeChangeRepository<'_>,
        run: &CodeChangeRun,
        candidate: &mut VerifiedCodeChangeWorktree,
        now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        let facts = candidate
            .diff_facts()
            .cloned()
            .ok_or_else(recovery_required)?;
        let changed_file_count = i64::try_from(facts.file_count).map_err(|_| {
            AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            }
        })?;
        let diff_bytes = i64::try_from(facts.diff_bytes).map_err(|_| AppError::Validation {
            field: "code_change.diff",
            message: "counts cannot be represented",
        })?;
        match run.state {
            CodeChangeState::Checking => {
                repository.transition(
                    &run.code_change_run_id,
                    CodeChangeState::Checking,
                    CodeChangeState::Committing,
                    now,
                )?;
            }
            CodeChangeState::Committing if run.candidate_sha.is_none() => {}
            _ => return Err(recovery_required()),
        }
        let candidate_sha = candidate.commit().await?;
        let finished = unix_timestamp()?;
        repository.record_candidate(
            &run.code_change_run_id,
            &candidate_sha,
            facts.persisted_digest(),
            changed_file_count,
            diff_bytes,
            finished,
        )?;
        repository.transition(
            &run.code_change_run_id,
            CodeChangeState::Committing,
            CodeChangeState::CandidateReady,
            finished,
        )?;
        report.advanced += 1;
        Ok(())
    }

    async fn resume_committing_candidate(
        &self,
        repository: &CodeChangeRepository<'_>,
        run: &CodeChangeRun,
        candidate: &mut VerifiedCodeChangeWorktree,
        now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        let existing_ref = candidate.manager.candidate_ref_sha().await?;
        if let Some(candidate_sha) = existing_ref {
            if run
                .candidate_sha
                .as_deref()
                .is_some_and(|stored| stored != candidate_sha)
            {
                return Err(recovery_required());
            }
            let facts = CandidateRepository::new(&candidate.manager, &candidate.candidate)?
                .committed_diff_facts(&candidate_sha)
                .await?;
            let changed_file_count = i64::try_from(facts.file_count).map_err(|_| {
                AppError::Validation {
                    field: "code_change.diff",
                    message: "counts cannot be represented",
                }
            })?;
            let diff_bytes = i64::try_from(facts.diff_bytes).map_err(|_| AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            })?;
            if run.diff_digest.as_deref() != Some(facts.persisted_digest())
                || run.changed_file_count != Some(changed_file_count)
                || run.diff_bytes != Some(diff_bytes)
            {
                return Err(recovery_required());
            }
            let finished = unix_timestamp()?;
            repository.record_candidate(
                &run.code_change_run_id,
                &candidate_sha,
                facts.persisted_digest(),
                changed_file_count,
                diff_bytes,
                finished,
            )?;
            repository.transition(
                &run.code_change_run_id,
                CodeChangeState::Committing,
                CodeChangeState::CandidateReady,
                finished,
            )?;
            report.advanced += 1;
            return Ok(());
        }
        if run.candidate_sha.is_some() {
            return Err(recovery_required());
        }
        let facts = candidate.verify().await?;
        let changed_file_count = i64::try_from(facts.file_count).map_err(|_| {
            AppError::Validation {
                field: "code_change.diff",
                message: "counts cannot be represented",
            }
        })?;
        let diff_bytes = i64::try_from(facts.diff_bytes).map_err(|_| AppError::Validation {
            field: "code_change.diff",
            message: "counts cannot be represented",
        })?;
        if run.diff_digest.as_deref() != Some(facts.persisted_digest())
            || run.changed_file_count != Some(changed_file_count)
            || run.diff_bytes != Some(diff_bytes)
        {
            return Err(recovery_required());
        }
        self.commit_checked_candidate(repository, run, candidate, now, report)
            .await
    }

    async fn advance_check_round(
        &self,
        repository: &CodeChangeRepository<'_>,
        run: &CodeChangeRun,
        attempt: i64,
        candidate: &mut VerifiedCodeChangeWorktree,
        editor_checks: &[ProposedCheck],
        check_timeout_override: Option<Duration>,
        now: i64,
        report: &mut CodeChangeReport,
    ) -> Result<(), AppError> {
        let run = match run.state {
            CodeChangeState::Editing => {
                repository.transition(
                    &run.code_change_run_id,
                    CodeChangeState::Editing,
                    CodeChangeState::Checking,
                    now,
                )?
            }
            CodeChangeState::Checking => run.clone(),
            _ => return Err(recovery_required()),
        };
        let Some(result) = candidate
            .run_check_round(
                self.db,
                &run.code_change_run_id,
                attempt,
                editor_checks,
                check_timeout_override,
            )
            .await?
        else {
            report.advanced += 1;
            return Ok(());
        };
        if result.passed() {
            self.commit_checked_candidate(repository, &run, candidate, now, report)
                .await
        } else {
            self.finish_failed_check_round(repository, &run, attempt, &result, now, report)
        }
    }
}

fn editor_prompt(
    campaign: &crate::models::Campaign,
    proposal: &crate::models::Proposal,
    run: &CodeChangeRun,
    candidate_path: &Path,
) -> Result<String, AppError> {
    let candidate_path = candidate_path.to_str().ok_or(AppError::Validation {
        field: "code_change.worktree",
        message: "candidate root must be valid UTF-8 for the editor prompt",
    })?;
    let payload = serde_json::json!({
        "schema_version": 1,
        "objective": campaign.objective_text,
        "hypothesis": proposal.hypothesis,
        "proposal_id": proposal.proposal_id,
        "code_change_run_id": run.code_change_run_id,
        "base_sha": run.base_sha,
        "candidate_root": candidate_path,
        "working_directory": proposal.working_directory,
        "instructions": "Modify only the candidate root. Do not mutate the registered project, protected refs, remotes, or credentials. Return only strict editor JSON with status ready or cannot_apply and bounded proposed checks. Do not commit.",
    });
    let json = serde_json::to_string(&payload).map_err(|source| AppError::Serialization {
        operation: "serialize code-change editor prompt",
        source,
    })?;
    if json.len() > 48 * 1024 {
        return Err(AppError::Validation {
            field: "code_change.prompt",
            message: "editor prompt exceeds the bounded size",
        });
    }
    Ok(format!(
        "You are the supervisor-owned code-change editor. Follow this request exactly and emit the structured result.\n{json}"
    ))
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
    let expected_candidate_ref = candidate_ref(&run.campaign_id, &run.proposal_id)?;
    let expected_best_ref = best_ref(&run.campaign_id)?;
    if Path::new(&run.worktree_relative_path) != expected_relative.as_path()
        || run.worktree_id != run.code_change_run_id
        || run.candidate_ref != expected_candidate_ref
        || run.best_ref != expected_best_ref
    {
        return Err(recovery_required());
    }
    let original_base_sha = campaign_start_base_sha(db, &run.campaign_id)?;
    let manager = WorktreeManager::new_for_run(
        policy,
        project,
        original,
        &run.campaign_id,
        &run.proposal_id,
        &run.base_sha,
        &original_base_sha,
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
        &proof.protected_ref_digest,
        &proof.remote_config_digest,
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

/// Reopen the candidate worktree retained by an editing code-change run.
/// Unlike preparation this never creates a second worktree; it revalidates
/// the original Git boundary and descriptor identities before returning the
/// same owned candidate capability for a bounded resume attempt.
#[cfg(unix)]
pub async fn reopen_code_change_worktree_for_run(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    db: &Db,
    run_id: &str,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    let run = CodeChangeCleanupAuthorization::load(db, run_id)?.fresh_run()?;
    verify_durable_run_scope(db, &run, project)?;
    if !matches!(
        run.state,
        CodeChangeState::Editing
            | CodeChangeState::Checking
            | CodeChangeState::Committing
            | CodeChangeState::Rejected
    ) || (run.state == CodeChangeState::Rejected && run.candidate_sha.is_some())
    {
        return Err(validation(
            "code_change.state",
            "must be editing, checking, committing, or an uncommitted rejected run when reopening an editor worktree",
        ));
    }
    let expected_relative = owned_worktree_relative_path(&run.campaign_id, &run.proposal_id)?;
    let expected_candidate_ref = candidate_ref(&run.campaign_id, &run.proposal_id)?;
    let expected_best_ref = best_ref(&run.campaign_id)?;
    if Path::new(&run.worktree_relative_path) != expected_relative.as_path()
        || run.worktree_id != run.code_change_run_id
        || run.candidate_ref != expected_candidate_ref
        || run.best_ref != expected_best_ref
    {
        return Err(recovery_required());
    }
    let original_base_sha = campaign_start_base_sha(db, &run.campaign_id)?;
    let mut manager = WorktreeManager::new_for_run(
        policy,
        project,
        original,
        &run.campaign_id,
        &run.proposal_id,
        &run.base_sha,
        &original_base_sha,
        &run.worktree_id,
        &expected_relative,
    )?;
    let (protected_ref_digest, remote_config_digest) = match (
        run.protected_ref_digest.as_deref(),
        run.remote_config_digest.as_deref(),
    ) {
        (Some(protected_ref_digest), Some(remote_config_digest)) => {
            (protected_ref_digest, remote_config_digest)
        }
        _ => return Err(recovery_required()),
    };
    manager
        .inspect_base(
            Some((protected_ref_digest, remote_config_digest)),
            run.state == CodeChangeState::Committing,
        )
        .await?;
    let candidate = manager.retain_existing_candidate().await?;
    let proof = candidate.manager.durable_ownership_proof(&candidate.candidate)?;
    if !durable_ownership_matches_run(&run, &proof) {
        return Err(recovery_required());
    }
    Ok(candidate)
}

/// Reopen a candidate that has already been committed and published.  This
/// path is deliberately separate from editor/check recovery: it accepts only
/// the post-commit states and verifies the persisted commit, candidate ref,
/// and committed diff facts before exposing the worktree to Pueue.
#[cfg(unix)]
pub async fn reopen_code_change_candidate_for_run(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    db: &Db,
    run_id: &str,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    reopen_code_change_candidate_for_run_with_outputs(policy, project, original, db, run_id, None)
        .await
}

/// Reopen a candidate while ingesting the terminal result of its bound
/// experiment.  The candidate identity and committed diff remain subject to
/// the same checks as ordinary reopening; only descriptor-bound runtime
/// output and known verified Python check caches are accepted for ignored
/// paths.
#[cfg(unix)]
pub async fn reopen_code_change_result_for_run(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    db: &Db,
    run_id: &str,
    experiment_id: &str,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    validate_internal_id("experiment_id", experiment_id)?;
    reopen_code_change_candidate_for_run_with_outputs(
        policy,
        project,
        original,
        db,
        run_id,
        Some(experiment_id),
    )
    .await
}

#[cfg(unix)]
async fn reopen_code_change_candidate_for_run_with_outputs(
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
    db: &Db,
    run_id: &str,
    result_experiment_id: Option<&str>,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    let authorization = CodeChangeCleanupAuthorization::load(db, run_id)?;
    let run = authorization.fresh_run()?;
    verify_durable_run_scope(db, &run, project)?;
    let state_allowed = match result_experiment_id {
        Some(experiment_id) => {
            matches!(
                run.state,
                CodeChangeState::ExperimentSubmitted
                    | CodeChangeState::CleanupPending
                    | CodeChangeState::Rejected
            )
                && run.experiment_id.as_deref() == Some(experiment_id)
        }
        None => matches!(
            run.state,
            CodeChangeState::CandidateReady
                | CodeChangeState::ExperimentSubmitted
                | CodeChangeState::CleanupPending
                | CodeChangeState::Rejected
        ),
    };
    if !state_allowed {
        return Err(validation(
            "code_change.state",
            "must be candidate-ready, experiment-submitted, cleanup-pending, or rejected when reopening a candidate",
        ));
    }
    let candidate_sha = run.candidate_sha.as_deref().ok_or_else(recovery_required)?;
    canonical_full_sha(candidate_sha)?;
    let expected_relative = owned_worktree_relative_path(&run.campaign_id, &run.proposal_id)?;
    let expected_candidate_ref = candidate_ref(&run.campaign_id, &run.proposal_id)?;
    let expected_best_ref = best_ref(&run.campaign_id)?;
    if Path::new(&run.worktree_relative_path) != expected_relative.as_path()
        || run.worktree_id != run.code_change_run_id
        || run.candidate_ref != expected_candidate_ref
        || run.best_ref != expected_best_ref
    {
        return Err(recovery_required());
    }
    let original_base_sha = campaign_start_base_sha(db, &run.campaign_id)?;
    let mut manager = WorktreeManager::new_for_run(
        policy,
        project,
        original,
        &run.campaign_id,
        &run.proposal_id,
        &run.base_sha,
        &original_base_sha,
        &run.worktree_id,
        &expected_relative,
    )?;
    let (protected_ref_digest, remote_config_digest) = match (
        run.protected_ref_digest.as_deref(),
        run.remote_config_digest.as_deref(),
    ) {
        (Some(protected_ref_digest), Some(remote_config_digest)) => {
            (protected_ref_digest, remote_config_digest)
        }
        _ => return Err(recovery_required()),
    };
    manager
        .inspect_base(
            Some((protected_ref_digest, remote_config_digest)),
            true,
        )
        .await?;
    let mut candidate = manager.retain_existing_candidate().await?;
    let proof = candidate.manager.durable_ownership_proof(&candidate.candidate)?;
    if !durable_ownership_matches_run(&run, &proof) {
        return Err(recovery_required());
    }
    let ref_sha = candidate.manager.candidate_ref_sha().await?;
    if ref_sha.as_deref() != Some(candidate_sha) {
        return Err(recovery_required());
    }
    let repository = CandidateRepository::new(&candidate.manager, &candidate.candidate)?;
    let (facts, terminal_result_outputs) = match result_experiment_id {
        Some(experiment_id) => {
            let (facts, outputs) = repository
                .committed_diff_facts_for_result(candidate_sha, experiment_id)
                .await?;
            (facts, Some(outputs))
        }
        None => (
            repository.committed_diff_facts(candidate_sha).await?,
            None,
        ),
    };
    let changed_file_count = i64::try_from(facts.file_count).map_err(|_| AppError::Validation {
        field: "code_change.diff",
        message: "counts cannot be represented",
    })?;
    let diff_bytes = i64::try_from(facts.diff_bytes).map_err(|_| AppError::Validation {
        field: "code_change.diff",
        message: "counts cannot be represented",
    })?;
    if run.diff_digest.as_deref() != Some(facts.persisted_digest())
        || run.changed_file_count != Some(changed_file_count)
        || run.diff_bytes != Some(diff_bytes)
    {
        return Err(recovery_required());
    }
    candidate.candidate_sha = Some(candidate_sha.to_owned());
    candidate.terminal_result_outputs = terminal_result_outputs;
    Ok(candidate)
}

#[cfg(unix)]
async fn finish_prepared_manager(
    mut manager: WorktreeManager,
    policy: &ResolvedExecutionPolicy,
    project: &Project,
    original: &ResolvedProjectExecutionPolicy,
) -> Result<VerifiedCodeChangeWorktree, AppError> {
    manager.inspect_base(None, false).await?;
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
        terminal_result_outputs: None,
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

fn supervisor_diff_check_args(base_sha: &str) -> Vec<OsString> {
    vec![
        OsString::from("diff"),
        OsString::from("--cached"),
        OsString::from("--check"),
        OsString::from(base_sha),
    ]
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

#[cfg(unix)]
fn verify_cleanup_leaf_identity(
    parent: &File,
    leaf: &OsStr,
    expected: ExecutableIdentity,
) -> Result<(), AppError> {
    let current = open_existing_directory_at(parent, leaf).map_err(|_| recovery_required())?;
    if directory_identity(&current)? != expected {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn verify_cleanup_leaf_absent(parent: &File, leaf: &OsStr) -> Result<(), AppError> {
    if directory_entry_exists(parent, leaf)? {
        return Err(recovery_required());
    }
    Ok(())
}

const MAX_EDITOR_OUTPUT_BYTES: usize = 64 * 1024;
const MAX_EDITOR_SUMMARY_BYTES: usize = 64 * 1024;
const MAX_CHECK_SOURCE_BYTES: usize = 64;
const MAX_CHECK_ARG_BYTES: usize = 4 * 1024;
const MAX_CHECK_ARG_COUNT: usize = 32;
const MAX_PYPROJECT_BYTES: usize = 64 * 1024;
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
    let pytest = root.join("pytest.ini").is_file() || pyproject_has_pytest(root)?;
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

fn merge_project_checks(
    discovered: &[ProposedCheck],
    editor: &[ProposedCheck],
    max: usize,
) -> Result<Vec<ProposedCheck>, AppError> {
    let mut merged = Vec::new();
    for check in discovered.iter().chain(editor) {
        if merged.iter().any(|existing| existing == check) {
            continue;
        }
        if merged.len() >= max {
            return Err(validation(
                "code_change.proposed_checks",
                "exceeds the bounded check count",
            ));
        }
        merged.push(check.clone());
    }
    Ok(merged)
}

fn planned_check_rows(
    attempt: i64,
    base_sha: &str,
    discovered: &[ProposedCheck],
    editor: &[ProposedCheck],
    max_project_checks: usize,
) -> Result<Vec<NewCodeChangeCheck>, AppError> {
    let supervisor_argv = pinned_git_argv(&supervisor_diff_check_args(base_sha))
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| validation("code_change_check.argv", "must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = vec![NewCodeChangeCheck::new(
        attempt,
        0,
        "supervisor",
        supervisor_argv,
        ".",
    )];
    let merged = merge_project_checks(discovered, editor, max_project_checks)?;
    rows.extend(merged.into_iter().enumerate().map(|(ordinal, check)| {
        let source = if discovered.iter().any(|candidate| candidate == &check) {
            "discovered"
        } else {
            "editor"
        };
        NewCodeChangeCheck::new(
            attempt,
            ordinal as i64 + 1,
            source,
            check.argv.clone(),
            check.working_directory.clone(),
        )
    }));
    Ok(rows)
}

fn validate_persisted_check_plan(
    existing: &[CodeChangeCheck],
    planned: &[NewCodeChangeCheck],
) -> Result<(), AppError> {
    if existing.len() != planned.len()
        || existing.iter().zip(planned).any(|(existing, planned)| {
            existing.attempt != planned.attempt
                || existing.ordinal != planned.ordinal
                || existing.source != planned.source
                || existing.argv != planned.argv
                || existing.working_directory != planned.working_directory
        })
    {
        return Err(recovery_required());
    }

    let mut saw_reserved = false;
    let mut saw_terminal = false;
    for check in existing {
        match check.status {
            CodeChangeCheckStatus::Passed => {
                if saw_reserved || saw_terminal || check.output_digest.is_none() {
                    return Err(recovery_required());
                }
            }
            CodeChangeCheckStatus::Reserved => {
                saw_reserved = true;
            }
            CodeChangeCheckStatus::Failed | CodeChangeCheckStatus::TimedOut => {
                if saw_reserved || saw_terminal {
                    return Err(recovery_required());
                }
                saw_terminal = true;
            }
        }
    }
    Ok(())
}

fn editor_project_checks(
    rows: &[CodeChangeCheck],
) -> Result<Vec<ProposedCheck>, AppError> {
    rows.iter()
        .filter(|row| row.source == "editor")
        .map(|row| {
            if row.argv.is_empty() {
                return Err(recovery_required());
            }
            let source = match tool_for_program(&row.argv[0]) {
                Some(CodeChangeTool::Cargo) => "cargo",
                Some(CodeChangeTool::Uv) => "uv",
                Some(CodeChangeTool::Python) => "python",
                Some(CodeChangeTool::Git) | None => return Err(recovery_required()),
            };
            Ok(ProposedCheck {
                source: source.to_owned(),
                argv: row.argv.clone(),
                working_directory: row.working_directory.clone(),
            })
        })
        .collect()
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
enum GitPointerTarget {
    Directory,
    File,
}

#[cfg(unix)]
#[derive(Clone)]
#[derive(Copy)]
enum GitPointerKind {
    GitDir,
    Path(GitPointerTarget),
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
        let expected_admin_parent = original.common_path.join("worktrees");
        let candidate = Self::capture(
            root,
            Some(&original.common_path),
            Some(&expected_admin_parent),
        )?;
        if candidate.admin_path == original.admin_path {
            return Err(recovery_required());
        }
        Ok(candidate)
    }

    fn capture(
        root: &VerifiedProjectRoot,
        expected_common: Option<&Path>,
        expected_admin_parent: Option<&Path>,
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
                    Some(GitPointerKind::Path(GitPointerTarget::Directory)),
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
                    Some(GitPointerKind::Path(GitPointerTarget::File)),
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
        if let Some(expected_parent) = expected_admin_parent {
            if admin_path.parent() != Some(expected_parent) {
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
        let expected_admin_name = expected_admin_parent
            .map(|_| {
                admin_path
                    .file_name()
                    .map(OsStr::to_os_string)
                    .ok_or_else(recovery_required)
            })
            .transpose()?;
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
            // Git may sanitize the linked-worktree admin basename (for
            // example, replacing ':' in a proposal ID), so retain the
            // canonical name observed from the verified pointer.
            expected_admin_name,
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
    original_base_sha: String,
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
    protected_ref_digest: String,
    remote_config_digest: String,
}

fn durable_ownership_matches_run(run: &CodeChangeRun, proof: &DurableOwnershipProof) -> bool {
    run.state_root_identity.as_deref() == Some(proof.state_root_identity.as_str())
        && run.worktrees_identity.as_deref() == Some(proof.worktrees_identity.as_str())
        && run.campaign_identity.as_deref() == Some(proof.campaign_identity.as_str())
        && run.candidate_root_identity.as_deref() == Some(proof.candidate_root_identity.as_str())
        && run.candidate_admin_identity.as_deref() == Some(proof.candidate_admin_identity.as_str())
        && run.candidate_common_identity.as_deref() == Some(proof.candidate_common_identity.as_str())
        && run.candidate_admin_path.as_deref() == Some(proof.candidate_admin_path.as_str())
        && run.candidate_common_path.as_deref() == Some(proof.candidate_common_path.as_str())
        && run.protected_ref_digest.as_deref() == Some(proof.protected_ref_digest.as_str())
        && run.remote_config_digest.as_deref() == Some(proof.remote_config_digest.as_str())
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
        original_base_sha: &str,
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
            original_base_sha,
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
        original_base_sha: &str,
        worktree_id: &str,
        worktree_relative_path: &Path,
    ) -> Result<Self, AppError> {
        validate_internal_id("campaign_id", campaign_id)?;
        validate_internal_id("proposal_id", proposal_id)?;
        validate_internal_id("worktree_id", worktree_id)?;
        canonical_full_sha(base_sha)?;
        canonical_full_sha(original_base_sha)?;
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
            original_base_sha: original_base_sha.to_owned(),
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
        let baseline = self.baseline.as_ref().ok_or_else(recovery_required)?;
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
            protected_ref_digest: baseline.protected_ref_digest.clone(),
            remote_config_digest: baseline.remote_config_digest.clone(),
        })
    }

    async fn inspect_base(
        &mut self,
        expected_baseline: Option<(&str, &str)>,
        allow_candidate_ref: bool,
    ) -> Result<(), AppError> {
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
        if revision != self.original_base_sha {
            return Err(validation(
                "code_change.original_base_sha",
                "does not match the campaign-start project HEAD",
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
        if existing_candidate_ref.success && !allow_candidate_ref {
            return Err(validation(
                "code_change.ref",
                "candidate ref already exists",
            ));
        }
        if !existing_candidate_ref.success && existing_candidate_ref.exit_code != Some(1) {
            return Err(validation(
                "code_change.ref",
                "candidate ref could not be inspected",
            ));
        }
        let remote_config_digest = self.remote_config_digest(&original_root, &working_directory).await?;
        let (protected_ref_digest, remote_config_digest) = match expected_baseline {
            Some((expected_protected_ref_digest, expected_remote_config_digest)) => {
                if expected_protected_ref_digest != protected_ref_digest
                    || expected_remote_config_digest != remote_config_digest
                {
                    return Err(recovery_required());
                }
                (
                    expected_protected_ref_digest.to_owned(),
                    expected_remote_config_digest.to_owned(),
                )
            }
            None => (protected_ref_digest, remote_config_digest),
        };
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

    async fn retain_existing_candidate(mut self) -> Result<VerifiedCodeChangeWorktree, AppError> {
        let baseline = self.baseline.clone().ok_or(AppError::Runtime {
            operation: "reopen code-change worktree before base inspection",
        })?;
        self.policy.verify_code_change_state_root()?;
        self.worktree_parents = Some(WorktreeParentProof::retain(
            &self.policy,
            &self.campaign_id,
        )?);
        if path_has_symlink_component(&self.worktree_path)? {
            return Err(recovery_required());
        }
        let candidate = ProjectRootAnchor::resolve(&self.worktree_path)
            .map_err(AppError::from)?
            .verify_identity()
            .map_err(AppError::from)?;
        if candidate.anchor.canonical_path != self.worktree_path {
            return Err(recovery_required());
        }
        let repository = GitRepositoryProof::capture_candidate(
            &candidate,
            &baseline.repository,
        )?;
        self.candidate_repository = Some(repository);
        self.candidate_identity = Some(candidate.anchor.identity);
        let original_root = self.original.root_anchor.verify_identity()?;
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        let common_directory = inspect_common_directory(&self, &original_root, &working_directory).await?;
        if common_directory != baseline.common_directory {
            return Err(recovery_required());
        }
        let owned_ref = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?,
        );
        let best_ref_name = format!("refs/heads/{}", best_ref(&self.campaign_id)?);
        let protected_ref_digest = self
            .protected_ref_digest(
                &original_root,
                &working_directory,
                &[owned_ref.as_str(), best_ref_name.as_str()],
            )
            .await?;
        if protected_ref_digest != baseline.protected_ref_digest
            || self
                .remote_config_digest(&original_root, &working_directory)
                .await?
                != baseline.remote_config_digest
        {
            return Err(recovery_required());
        }
        let parents = self.worktree_parents.as_ref().ok_or_else(recovery_required)?;
        let candidate = candidate.mark_code_change_owned(
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
        )?;
        Ok(VerifiedCodeChangeWorktree {
            manager: self,
            candidate,
            diff_facts: None,
            candidate_sha: None,
            terminal_result_outputs: None,
        })
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
        #[cfg(any(target_os = "linux", target_os = "android"))]
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
            let candidate_ref = self.candidate_ref_sha().await?;
            validate_retained_candidate_ref(candidate_ref.as_deref(), expected_candidate_sha)?;
            if self.candidate_admin_registered_elsewhere()? {
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
            let candidate_ref = self.candidate_ref_sha().await?;
            validate_retained_candidate_ref(candidate_ref.as_deref(), expected_candidate_sha)?;
        }
        if current.is_none() {
            // The durable parent entry proves that Git still registered a
            // candidate administration directory.  A missing worktree root
            // in that state is a moved/orphaned target, not an idempotent
            // successful cleanup.
            return validate_disappeared_cleanup_target(allow_missing_target);
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
        verify_cleanup_leaf_identity(
            &candidate_parent,
            OsStr::new(&self.proposal_id),
            current_before_remove.anchor.identity,
        )?;
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let cleanup_path = OsString::from(git_descriptor_path(GIT_WORKTREE_PARENT_FD));
            let cleanup_args = vec![
                OsString::from("worktree"),
                OsString::from("remove"),
                OsString::from("--force"),
                cleanup_path,
            ];
            let output = self
                .git_os_with_worktree_parent_and_descriptor(
                    &original_root,
                    &working_directory,
                    &cleanup_args,
                    MAX_GIT_OUTPUT_BYTES,
                    Some(candidate_parent.try_clone().map_err(|_| recovery_required())?),
                    Some((candidate_directory, current_before_remove.anchor.identity)),
                )
                .await?;
            if !output.success {
                return Err(recovery_required());
            }
            self.validate_original_state().await?;
            let reopened_parent = open_worktree_parent(self)?.ok_or_else(recovery_required)?;
            if directory_identity(&reopened_parent)? != candidate_parent_identity {
                return Err(recovery_required());
            }
            verify_cleanup_leaf_absent(&reopened_parent, OsStr::new(&self.proposal_id))?;
            let candidate_ref = self.candidate_ref_sha().await?;
            validate_retained_candidate_ref(candidate_ref.as_deref(), expected_candidate_sha)?;
            self.validate_original_state().await?;
            Ok(())
        }
        #[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
        {
            Err(recovery_required())
        }
    }

    async fn candidate_ref_sha(&self) -> Result<Option<String>, AppError> {
        let reference = format!(
            "refs/heads/{}",
            candidate_ref(&self.campaign_id, &self.proposal_id)?
        );
        let original_root = self.original.root_anchor.verify_identity()?;
        let working_directory = VerifiedWorkingDirectory::root(&original_root)?;
        let existing = self
            .git(
                &original_root,
                &working_directory,
                &["show-ref", "--verify", "--quiet", &reference],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !existing.success && existing.exit_code == Some(1) {
            return Ok(None);
        }
        if !existing.success {
            return Err(recovery_required());
        }
        let output = self
            .git(
                &original_root,
                &working_directory,
                &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&output, "read candidate ref")?;
        let sha = bounded_utf8_line(&output.stdout, "candidate ref")?;
        canonical_full_sha(sha)?;
        Ok(Some(sha.to_owned()))
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
            let target = parse_git_pointer(
                &gitdir,
                &descriptor_path(&admin),
                GitPointerKind::Path(GitPointerTarget::File),
            )?;
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

    async fn cleanup_pre_candidate_authorized(
        mut self,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let run = authorization.fresh_run()?;
        self.verify_pre_candidate_cleanup_run(&run, authorization)?;
        self.inspect_base(None, false).await?;
        self.verify_pre_candidate_cleanup_target_absent()?;
        let latest = authorization.fresh_run()?;
        self.verify_pre_candidate_cleanup_run(&latest, authorization)?;
        CodeChangeRepository::new(&authorization.db)
            .finish_cleanup(&latest.code_change_run_id, unix_timestamp()?)?;
        Ok(())
    }

    fn verify_pre_candidate_cleanup_run(
        &self,
        run: &CodeChangeRun,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let expected_relative = owned_worktree_relative_path(&self.campaign_id, &self.proposal_id)?;
        let expected_candidate_ref = candidate_ref(&self.campaign_id, &self.proposal_id)?;
        let expected_best_ref = best_ref(&self.campaign_id)?;
        if run.code_change_run_id != authorization.run_id
            || run.campaign_id != self.campaign_id
            || run.proposal_id != self.proposal_id
            || run.worktree_id != self.worktree_id
            || Path::new(&run.worktree_relative_path) != expected_relative.as_path()
            || run.base_sha != self.base_sha
            || run.candidate_sha.is_some()
            || run.experiment_id.is_some()
            || run.candidate_ref != expected_candidate_ref
            || run.best_ref != expected_best_ref
            || run.diff_digest.is_some()
            || run.changed_file_count.is_some()
            || run.diff_bytes.is_some()
            || run.editor_attempts != 0
            || run.editor_session_id.is_some()
            || run.promotion_outcome.is_some()
            || run.promotion_expected_best_experiment_id.is_some()
            || run.promotion_expected_old_sha.is_some()
            || run.promotion_target_sha.is_some()
            || run.cleanup_completed_at.is_some()
            || run.state_root_identity.is_some()
            || run.worktrees_identity.is_some()
            || run.campaign_identity.is_some()
            || run.candidate_root_identity.is_some()
            || run.candidate_admin_identity.is_some()
            || run.candidate_common_identity.is_some()
            || run.candidate_admin_path.is_some()
            || run.candidate_common_path.is_some()
            || run.protected_ref_digest.is_some()
            || run.remote_config_digest.is_some()
            || run.candidate_working_directory_identity.is_some()
            || run.state != CodeChangeState::Rejected
        {
            return Err(recovery_required());
        }
        verify_durable_run_scope(&authorization.db, run, &self.project)?;
        validate_cleanup_state_for_mutation(run.state, run.cleanup_completed_at)?;
        if !CodeChangeRepository::new(&authorization.db)
            .list_editor_attempts(&run.code_change_run_id)?
            .is_empty()
        {
            return Err(recovery_required());
        }
        Ok(())
    }

    fn verify_pre_candidate_cleanup_target_absent(&self) -> Result<(), AppError> {
        self.policy.verify_code_change_state_root()?;
        if path_has_symlink_component(&self.worktree_path)? {
            return Err(recovery_required());
        }
        let state_root = self.policy.code_change_state_root_directory();
        let candidate_entry_exists = match open_existing_directory_at(
            &state_root,
            OsStr::new("worktrees"),
        ) {
            Ok(worktrees) => match open_existing_directory_at(&worktrees, OsStr::new(&self.campaign_id))
            {
                Ok(campaign) => directory_entry_exists(&campaign, OsStr::new(&self.proposal_id))?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                Err(_) => return Err(recovery_required()),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(_) => return Err(recovery_required()),
        };
        if candidate_entry_exists {
            return Err(recovery_required());
        }
        if self.candidate_admin_registered_elsewhere()? {
            return Err(recovery_required());
        }
        Ok(())
    }

    fn verify_cleanup_task_observations(
        &self,
        task_id: i64,
        task_signature: &str,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let observations = TaskObservationRepository::new(&authorization.db).find_by_pueue_task(
            &self.project.project_id,
            task_id,
            MAX_CLEANUP_TASK_OBSERVATIONS + 1,
        )?;
        if observations.is_empty() || observations.len() > MAX_CLEANUP_TASK_OBSERVATIONS {
            return Err(recovery_required());
        }

        for observation in &observations {
            if observation.project_id != self.project.project_id
                || observation.pueue_task_id != task_id
            {
                return Err(recovery_required());
            }
            let managed_identity = cleanup_task_observation_managed_identity(
                observation,
                &self.project.pueue_group,
            )
            .ok_or_else(recovery_required)?;
            if managed_identity != task_signature {
                return Err(recovery_required());
            }
        }

        let terminal_count = observations
            .iter()
            .filter(|observation| {
                observation.ended_at.is_some()
                    && is_terminal_task_observation_state(&observation.state)
            })
            .count();
        if terminal_count != 1 {
            return Err(recovery_required());
        }

        let latest_observed_at = observations
            .iter()
            .map(|observation| observation.observed_at)
            .max()
            .ok_or_else(recovery_required)?;
        let latest = observations
            .iter()
            .filter(|observation| observation.observed_at == latest_observed_at)
            .collect::<Vec<_>>();
        if latest.len() != 1
            || latest[0].ended_at.is_none()
            || !is_terminal_task_observation_state(&latest[0].state)
        {
            return Err(recovery_required());
        }
        Ok(())
    }

    fn verify_cleanup_run(
        &self,
        run: &CodeChangeRun,
        expected_candidate_sha: Option<&str>,
        candidate: Option<&VerifiedProjectRoot>,
        authorization: &CodeChangeCleanupAuthorization,
    ) -> Result<(), AppError> {
        let expected_candidate_ref = candidate_ref(&self.campaign_id, &self.proposal_id)?;
        let expected_best_ref = best_ref(&self.campaign_id)?;
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
        validate_cleanup_state_for_mutation(run.state, run.cleanup_completed_at)?;
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
            let submission = SubmissionRepository::new(&authorization.db)
                .find_by_id(&experiment.submission_id)?
                .ok_or_else(recovery_required)?;
            let task_id = experiment.pueue_task_id.ok_or_else(recovery_required)?;
            let task_signature = experiment
                .task_signature
                .as_deref()
                .ok_or_else(recovery_required)?;
            if submission.project_id != self.project.project_id
                || submission.pueue_task_id != Some(task_id)
                || submission.task_signature.as_deref() != Some(task_signature)
            {
                return Err(recovery_required());
            }
            self.verify_cleanup_task_observations(task_id, task_signature, authorization)?;
        }
        let connection = authorization.db.connect()?;
        let mut authoritative_rows = 0usize;
        if let Some(experiment_id) = run.experiment_id.as_deref() {
            let mut statement = connection
                .prepare(
                    "SELECT s.origin_agent_run_id, ar.status, ar.pid, s.project_id, ar.project_id
                     FROM experiments AS e
                     JOIN submissions AS s ON s.submission_id = e.submission_id
                     LEFT JOIN agent_runs AS ar ON ar.run_id = s.origin_agent_run_id
                     WHERE e.experiment_id = ?1",
                )
                .map_err(crate::db::database_error("inspect code-change experiment liveness"))?;
            let rows = statement
                .query_map([experiment_id], |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })
                .map_err(crate::db::database_error("read code-change experiment liveness"))?;
            let mut experiment_rows = 0;
            let mut lineage_rows = 0;
            for row in rows {
                experiment_rows += 1;
                let (origin_agent_run_id, status, pid, submission_project, agent_project) = row
                    .map_err(crate::db::database_error("read code-change experiment liveness"))?;
                if submission_project != self.project.project_id {
                    return Err(recovery_required());
                }
                if let Some(_origin_agent_run_id) = origin_agent_run_id {
                    lineage_rows += 1;
                    let status = status.ok_or_else(recovery_required)?;
                    if agent_project.as_deref() != Some(self.project.project_id.as_str()) {
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
                } else if status.is_some() || pid.is_some() || agent_project.is_some() {
                    return Err(recovery_required());
            }
            }
            if experiment_rows != 1 || lineage_rows > 1 {
                return Err(recovery_required());
            }
            // The terminal experiment/task/observation checks above are the
            // authoritative candidate evidence.  An origin agent run is an
            // additional liveness proof when one was persisted, but normal
            // candidate submissions intentionally have no origin run.
            authoritative_rows = authoritative_rows.saturating_add(experiment_rows);
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
            if !matches!(status.as_str(), "ready" | "failed") {
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
        self.git_os_with_worktree_parent_and_descriptor(
            root,
            working_directory,
            args,
            cap,
            owned_worktree_parent,
            None,
        )
        .await
    }

    async fn git_os_with_worktree_parent_and_descriptor(
        &self,
        root: &VerifiedProjectRoot,
        working_directory: &VerifiedWorkingDirectory,
        args: &[OsString],
        cap: usize,
        owned_worktree_parent: Option<File>,
        owned_worktree_descriptor: Option<(File, ExecutableIdentity)>,
    ) -> Result<BoundedToolOutput, AppError> {
        self.validate_git_boundary(root)?;
        let argv = args.to_vec();
        let environment = SanitizedEnvironment::for_code_change_tool(&self.policy)?;
        let worktree_administration = is_worktree_administration(args);
        if !worktree_administration
            && (owned_worktree_parent.is_some() || owned_worktree_descriptor.is_some())
        {
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
        let worktree_descriptor = match owned_worktree_descriptor {
            Some((descriptor, expected)) => {
                if directory_identity(&descriptor)? != expected {
                    return Err(recovery_required());
                }
                Some(descriptor)
            }
            None => parent,
        };
        self.run_git_owned(
            root,
            &command_working_directory,
            argv,
            cap,
            environment,
            None,
            worktree_descriptor,
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
        worktree_descriptor: Option<File>,
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
        let git_directories = self.git_directories(root, worktree_descriptor)?;
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
                None,
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
        worktree_descriptor: Option<File>,
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
        let worktree_parent_identity = worktree_descriptor
            .as_ref()
            .map(directory_identity)
            .transpose()?;
        Ok(VerifiedGitDirectories {
            admin,
            admin_identity: proof.admin.identity,
            common,
            common_identity: proof.common.identity,
            worktree_parent: worktree_descriptor,
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
        if bounded_utf8_line(&head.stdout, "original HEAD")? != self.original_base_sha {
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

    async fn validate_submission_runtime_outputs(
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
        require_success(&output, "inspect candidate runtime paths")?;
        let ignored = parse_ignored_status_paths(&output.stdout)?;
        let (service, caches): (Vec<_>, Vec<_>) = ignored.into_iter().partition(|path| {
            matches!(
                path.components().next(),
                Some(Component::Normal(name)) if name == OsStr::new(RUNTIME_SERVICE_DIRECTORY)
            )
        });
        if service
            .iter()
            .any(|path| path != Path::new(RUNTIME_SERVICE_DIRECTORY))
        {
            return Err(recovery_required());
        }
        if caches.iter().any(|path| !is_python_check_cache_path(path)) {
            return Err(recovery_required());
        }
        validate_ignored_tree(&candidate.anchor.canonical_path, &caches)?;
        validate_no_nested_repositories(&candidate.anchor.canonical_path, &caches)
    }

    async fn validate_terminal_result_outputs(
        &self,
        experiment_id: &str,
    ) -> Result<BoundTerminalResultOutputs, AppError> {
        let working_directory = VerifiedWorkingDirectory::root(self.candidate)?;
        let output = self
            .manager
            .git(
                self.candidate,
                &working_directory,
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
        require_success(&output, "inspect terminal result paths")?;
        let ignored = parse_ignored_status_paths(&output.stdout)?;
        let (_, caches): (Vec<_>, Vec<_>) = ignored.into_iter().partition(|path| {
            matches!(
                path.components().next(),
                Some(Component::Normal(name)) if name == OsStr::new(RUNTIME_SERVICE_DIRECTORY)
            )
        });
        if caches
            .iter()
            .any(|path| !is_python_check_cache_path(path))
        {
            return Err(recovery_required());
        }
        validate_ignored_tree(&self.candidate.anchor.canonical_path, &caches)?;
        validate_no_nested_repositories(&self.candidate.anchor.canonical_path, &caches)?;
        bind_terminal_result_outputs(&self.candidate.directory, experiment_id)
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

    async fn committed_diff_facts(&self, ref_sha: &str) -> Result<DiffFacts, AppError> {
        self.committed_diff_facts_with_runtime_outputs(ref_sha, None, false)
            .await
            .map(|(facts, _)| facts)
    }

    async fn committed_diff_facts_for_submission_runtime(
        &self,
        ref_sha: &str,
    ) -> Result<DiffFacts, AppError> {
        let (facts, _) = self
            .committed_diff_facts_with_runtime_outputs(ref_sha, None, true)
            .await?;
        Ok(facts)
    }

    async fn committed_diff_facts_for_result(
        &self,
        ref_sha: &str,
        experiment_id: &str,
    ) -> Result<(DiffFacts, BoundTerminalResultOutputs), AppError> {
        self.committed_diff_facts_with_runtime_outputs(ref_sha, Some(experiment_id), false)
            .await
            .and_then(|(facts, outputs)| {
                outputs
                    .ok_or_else(recovery_required)
                    .map(|outputs| (facts, outputs))
            })
    }

    async fn committed_diff_facts_with_runtime_outputs(
        &self,
        ref_sha: &str,
        result_experiment_id: Option<&str>,
        allow_service_runtime: bool,
    ) -> Result<(DiffFacts, Option<BoundTerminalResultOutputs>), AppError> {
        let ref_sha = canonical_full_sha(ref_sha)?;
        if self.manager.object_id_len != Some(ref_sha.len()) {
            return Err(recovery_required());
        }
        self.manager.validate_original_state().await?;
        let validator = CandidateValidator::new(self.manager, self.candidate)?;
        self.manager.validate_git_boundary(self.candidate)?;
        validate_local_git_metadata(&self.candidate.anchor.canonical_path)?;
        let working_directory = VerifiedWorkingDirectory::root(self.candidate)?;
        let status = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&status, "verify committed candidate cleanliness")?;
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
        require_success(&head, "verify committed candidate HEAD")?;
        if bounded_utf8_line(&head.stdout, "committed candidate HEAD")? != ref_sha {
            return Err(recovery_required());
        }
        let parent_ref = format!("{ref_sha}^");
        let parent = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["rev-parse", "--verify", &parent_ref],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&parent, "verify committed candidate parent")?;
        if bounded_utf8_line(&parent.stdout, "committed candidate parent")?
            != self.manager.base_sha
        {
            return Err(recovery_required());
        }
        let tree_ref = format!("{ref_sha}^{{tree}}");
        let tree = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["rev-parse", "--verify", &tree_ref],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&tree, "read committed candidate tree")?;
        let tree_sha = canonical_full_sha(bounded_utf8_line(
            &tree.stdout,
            "committed candidate tree",
        )?)?;
        if self.manager.object_id_len != Some(tree_sha.len()) {
            return Err(recovery_required());
        }
        let paths_output = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &[
                    "diff-tree",
                    "--name-only",
                    "-z",
                    "--no-commit-id",
                    "-r",
                    "--no-renames",
                    &self.manager.base_sha,
                    &tree_sha,
                ],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&paths_output, "enumerate committed candidate changes")?;
        let paths = parse_diff_paths(&paths_output.stdout)?;
        validate_protected_paths(&paths)?;
        validate_no_nested_repositories(&self.candidate.anchor.canonical_path, &paths)?;
        validator
            .validate_no_submodules(self.candidate, &working_directory, &paths)
            .await?;
        let terminal_result_outputs = match result_experiment_id {
            Some(experiment_id) => {
                Some(
                    validator
                        .validate_terminal_result_outputs(experiment_id)
                        .await?,
                )
            }
            None => {
                if allow_service_runtime {
                    validator
                        .validate_submission_runtime_outputs(
                            self.candidate,
                            &working_directory,
                        )
                        .await?;
                } else {
                    validator
                        .validate_ignored_protected_paths(self.candidate, &working_directory)
                        .await?;
                }
                None
            }
        };
        let diff = self
            .manager
            .git(
                self.candidate,
                &working_directory,
                &["diff-tree", "--binary", &self.manager.base_sha, &tree_sha],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        require_success(&diff, "read committed candidate diff")?;
        let file_count = paths.len();
        validate_diff_limits(file_count, diff.stdout.len(), &self.manager.policy.campaign_limits)?;
        self.manager.validate_original_state().await?;
        self.manager.validate_git_boundary(self.candidate)?;
        Ok((
            DiffFacts {
                transient_paths: paths,
                file_count,
                diff_bytes: diff.stdout.len(),
                tree_sha,
                digest: sha256_hex(&diff.stdout),
            },
            terminal_result_outputs,
        ))
    }

    async fn run_supervisor_diff_check(
        &self,
        expected: &DiffFacts,
    ) -> Result<BoundedToolOutput, AppError> {
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
        add_args.extend(
            expected
                .transient_paths
                .iter()
                .map(|path| path.as_os_str().to_os_string()),
        );
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
        let result = self
            .manager
            .run_git_owned(
                &root,
                &working_directory,
                supervisor_diff_check_args(&self.manager.base_sha),
                MAX_CHECK_OUTPUT_BYTES,
                environment.clone(),
                Some(index),
                None,
                "Git diff check",
            )
            .await?;
        self.manager.validate_original_state().await?;
        self.manager.validate_git_boundary(&root)?;
        Ok(result)
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
                None,
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

    #[cfg(debug_assertions)]
    async fn replace_best_ref_for_test(
        &self,
        replacement_sha: &str,
        expected_old_sha: Option<&str>,
    ) -> Result<(), AppError> {
        self.manager.validate_original_state().await?;
        canonical_full_sha(replacement_sha)?;
        let zero = self.manager.zero_object_id()?;
        let expected = match expected_old_sha {
            Some(value) => {
                canonical_full_sha(value)?;
                value.to_owned()
            }
            None => zero,
        };
        let original_root = self.manager.original.root_anchor.verify_identity()?;
        let original_cwd = VerifiedWorkingDirectory::root(&original_root)?;
        let reference = format!("refs/heads/{}", best_ref(&self.manager.campaign_id)?);
        let output = self
            .manager
            .git(
                &original_root,
                &original_cwd,
                &["update-ref", &reference, replacement_sha, &expected],
                MAX_GIT_OUTPUT_BYTES,
            )
            .await?;
        if !output.success {
            return Err(recovery_required());
        }
        self.manager.validate_original_state().await
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
        self.update_best_ref_cas_verified(new_sha, expected_old_sha, None, None)
            .await
    }

    async fn update_best_ref_cas_for_promotion(
        &self,
        new_sha: &str,
        expected_old_sha: Option<&str>,
        test_replacement_sha: Option<&str>,
        test_attempt_counter: Option<Arc<AtomicU64>>,
    ) -> Result<(), AppError> {
        self.update_best_ref_cas_verified(
            new_sha,
            expected_old_sha,
            test_replacement_sha,
            test_attempt_counter,
        )
        .await
    }

    async fn update_best_ref_cas_verified(
        &self,
        new_sha: &str,
        expected_old_sha: Option<&str>,
        _test_replacement_sha: Option<&str>,
        _test_attempt_counter: Option<Arc<AtomicU64>>,
    ) -> Result<(), AppError> {
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
        #[cfg(debug_assertions)]
        if let Some(replacement_sha) = _test_replacement_sha {
            self.replace_best_ref_for_test(replacement_sha, expected_old_sha)
                .await?;
        }
        #[cfg(debug_assertions)]
        if let Some(counter) = _test_attempt_counter.as_ref() {
            counter.fetch_add(1, Ordering::SeqCst);
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
struct CheckCompletion {
    status: CodeChangeCheckStatus,
    output_digest: Option<String>,
    summary: &'static str,
    passed: bool,
}

#[cfg(unix)]
fn classify_check_result(
    result: Result<BoundedToolOutput, AppError>,
) -> Result<CheckCompletion, AppError> {
    match result {
        Ok(output) if output.success => Ok(CheckCompletion {
            status: CodeChangeCheckStatus::Passed,
            output_digest: Some(output.output_digest),
            summary: "check passed",
            passed: true,
        }),
        Ok(output) => Ok(CheckCompletion {
            status: CodeChangeCheckStatus::Failed,
            output_digest: Some(output.output_digest),
            summary: "check returned non-zero",
            passed: false,
        }),
        Err(AppError::Runtime { operation }) if operation == "code-change tool timeout" => {
            Ok(CheckCompletion {
                status: CodeChangeCheckStatus::TimedOut,
                output_digest: None,
                summary: "check timed out",
                passed: false,
            })
        }
        Err(AppError::Validation { field, .. }) if field == "code_change.tool_output" => {
            Ok(CheckCompletion {
                status: CodeChangeCheckStatus::Failed,
                output_digest: None,
                summary: "check output exceeded limit",
                passed: false,
            })
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn persist_check_completion(
    repository: &CodeChangeRepository<'_>,
    run_id: &str,
    attempt: i64,
    ordinal: i64,
    started_at: i64,
    completion: CheckCompletion,
) -> Result<bool, AppError> {
    let finished_at = unix_timestamp()?;
    repository.finish_check(
        run_id,
        attempt,
        ordinal,
        completion.status,
        completion.output_digest.as_deref(),
        Some(completion.summary),
        Some(started_at),
        finished_at,
        finished_at,
    )?;
    Ok(completion.passed)
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
struct OwnedCheckOutputs {
    state_root: Arc<File>,
    state_root_identity: ExecutableIdentity,
    checks: Arc<File>,
    checks_identity: ExecutableIdentity,
    checks_name: OsString,
    run: Arc<File>,
    run_identity: ExecutableIdentity,
    run_name: OsString,
    attempt: Arc<File>,
    attempt_identity: ExecutableIdentity,
    attempt_name: OsString,
    root: Arc<File>,
    root_identity: ExecutableIdentity,
    root_name: OsString,
    path: PathBuf,
    unproven: AtomicBool,
}

#[cfg(unix)]
impl OwnedCheckOutputs {
    fn create(
        policy: &ResolvedExecutionPolicy,
        run_id: &str,
        attempt: i64,
        ordinal: i64,
        tool: CodeChangeTool,
    ) -> Result<Self, AppError> {
        validate_internal_id("code_change_run_id", run_id)?;
        if !(1..=2).contains(&attempt) || !(1..=8).contains(&ordinal) {
            return Err(validation(
                "code_change.check",
                "attempt and ordinal exceed the bounded check plan",
            ));
        }
        policy.verify_code_change_state_root()?;
        let state_root = policy.code_change_state_root_directory();
        let state_root_identity = directory_identity(&state_root)?;
        let checks_name = OsString::from("code-change-checks");
        let checks = Arc::new(open_or_create_directory_at(&state_root, &checks_name)?);
        let checks_identity = directory_identity(&checks)?;
        let run_name = OsString::from(run_id);
        let run = Arc::new(open_or_create_directory_at(&checks, &run_name)?);
        let run_identity = directory_identity(&run)?;
        let attempt_name = OsString::from(format!("attempt-{attempt}"));
        let attempt_directory = open_or_create_directory_at(&run, &attempt_name)?;
        let attempt_identity = directory_identity(&attempt_directory)?;
        let attempt = Arc::new(attempt_directory);
        let root_name = OsString::from(format!("check-{ordinal}"));
        let root_directory = create_new_directory_at(&attempt, &root_name)?;
        let root_identity = directory_identity(&root_directory)?;
        let root = Arc::new(root_directory);
        let path = policy
            .code_change_state_root_path()
            .join(&checks_name)
            .join(run_id)
            .join(&attempt_name)
            .join(&root_name);
        let outputs = Self {
            state_root,
            state_root_identity,
            checks,
            checks_identity,
            checks_name,
            run,
            run_identity,
            run_name,
            attempt,
            attempt_identity,
            attempt_name,
            root,
            root_identity,
            root_name,
            path,
            unproven: AtomicBool::new(false),
        };
        for name in check_output_directory_names(tool) {
            open_or_create_directory_at(&outputs.root, OsStr::new(name))?;
        }
        outputs.verify_before_command()?;
        Ok(outputs)
    }

    fn apply_environment(
        &self,
        environment: &mut SanitizedEnvironment,
        tool: CodeChangeTool,
    ) -> Result<(), AppError> {
        self.verify_before_command()?;
        apply_check_output_environment(environment, tool, &self.path)
    }

    fn verify_before_command(&self) -> Result<(), AppError> {
        if self.unproven.load(Ordering::Acquire) {
            return Err(recovery_required());
        }
        if let Err(error) = self.verify_location() {
            self.unproven.store(true, Ordering::Release);
            return Err(error);
        }
        let mut state = CheckOutputAuditState::default();
        let result = audit_check_output_directory(
            &self.root,
            0,
            self.root_identity.device,
            &mut state,
        );
        if let Err(error) = result {
            self.unproven.store(true, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    fn verify_location(&self) -> Result<(), AppError> {
        if directory_identity(&self.state_root)? != self.state_root_identity
            || directory_identity(&self.checks)? != self.checks_identity
            || directory_identity(&self.run)? != self.run_identity
            || directory_identity(&self.attempt)? != self.attempt_identity
            || directory_identity(&self.root)? != self.root_identity
        {
            return Err(recovery_required());
        }
        for (parent, name, identity) in [
            (
                &self.state_root,
                &self.checks_name,
                self.checks_identity,
            ),
            (&self.checks, &self.run_name, self.run_identity),
            (&self.run, &self.attempt_name, self.attempt_identity),
            (&self.attempt, &self.root_name, self.root_identity),
        ] {
            if check_output_entry_identity(parent, name)?.as_ref() != Some(&identity) {
                return Err(recovery_required());
            }
        }
        Ok(())
    }

    fn cleanup(&self) -> Result<(), AppError> {
        if self.unproven.load(Ordering::Acquire) {
            return Err(recovery_required());
        }
        let result = self.cleanup_inner();
        if result.is_err() {
            self.unproven.store(true, Ordering::Release);
        }
        result
    }

    fn cleanup_inner(&self) -> Result<(), AppError> {
        self.verify_location()?;
        let mut audit = CheckOutputAuditState::default();
        audit_check_output_directory(
            &self.root,
            0,
            self.root_identity.device,
            &mut audit,
        )?;
        let mut removal = CheckOutputAuditState::default();
        remove_check_output_directory(
            &self.root,
            0,
            self.root_identity.device,
            &mut removal,
        )?;
        self.verify_location()?;
        let mut remaining = CheckOutputAuditState::default();
        audit_check_output_directory(
            &self.root,
            0,
            self.root_identity.device,
            &mut remaining,
        )?;
        if remaining.entries != 0 {
            return Err(recovery_required());
        }
        if check_output_entry_identity(&self.attempt, &self.root_name)?.as_ref()
            != Some(&self.root_identity)
        {
            return Err(recovery_required());
        }
        unlink_check_output_entry(&self.attempt, &self.root_name, true)?;
        if check_output_entry_stat(&self.attempt, &self.root_name)?.is_some() {
            return Err(recovery_required());
        }
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for OwnedCheckOutputs {
    fn drop(&mut self) {
        if !self.unproven.load(Ordering::Acquire) {
            let _ = self.cleanup_inner();
        }
    }
}

#[cfg(unix)]
fn check_output_directory_names(tool: CodeChangeTool) -> &'static [&'static str] {
    match tool {
        CodeChangeTool::Cargo => &["tmp", "cargo-target"],
        CodeChangeTool::Uv => &["tmp", "uv-venv", "uv-cache", "uv-python", "pytest-cache"],
        CodeChangeTool::Python => &["tmp", "pytest-cache"],
        CodeChangeTool::Git => &["tmp"],
    }
}

#[cfg(unix)]
fn apply_check_output_environment(
    environment: &mut SanitizedEnvironment,
    tool: CodeChangeTool,
    root: &Path,
) -> Result<(), AppError> {
    if !root.is_absolute() {
        return Err(validation(
            "code_change.check_output",
            "owned check output path must be absolute",
        ));
    }
    let tmp = check_output_path(root, "tmp")?;
    for name in ["TMPDIR", "TMP", "TEMP"] {
        environment.with_generated(name, tmp.clone());
    }
    match tool {
        CodeChangeTool::Cargo => {
            environment.with_generated("CARGO_TARGET_DIR", check_output_path(root, "cargo-target")?);
        }
        CodeChangeTool::Uv => {
            environment.with_generated(
                "UV_PROJECT_ENVIRONMENT",
                check_output_path(root, "uv-venv")?,
            );
            environment.with_generated("UV_CACHE_DIR", check_output_path(root, "uv-cache")?);
            environment.with_generated(
                "UV_PYTHON_INSTALL_DIR",
                check_output_path(root, "uv-python")?,
            );
            apply_pytest_output_environment(environment, root)?;
        }
        CodeChangeTool::Python => {
            apply_pytest_output_environment(environment, root)?;
        }
        CodeChangeTool::Git => {}
    }
    Ok(())
}

#[cfg(unix)]
fn apply_pytest_output_environment(
    environment: &mut SanitizedEnvironment,
    root: &Path,
) -> Result<(), AppError> {
    environment.with_generated("PYTHONDONTWRITEBYTECODE", OsStr::new("1"));
    let cache = check_output_path(root, "pytest-cache")?;
    let cache = cache.to_str().ok_or(validation(
        "code_change.check_output",
        "owned pytest cache path must be UTF-8",
    ))?;
    if cache.chars().any(char::is_control) {
        return Err(validation(
            "code_change.check_output",
            "owned pytest cache path contains control characters",
        ));
    }
    let escaped = cache.replace('\\', "\\\\").replace('"', "\\\"");
    environment.with_generated(
        "PYTEST_ADDOPTS",
        OsString::from(format!("-o \"cache_dir={escaped}\"")),
    );
    Ok(())
}

#[cfg(unix)]
fn check_output_path(root: &Path, name: &str) -> Result<OsString, AppError> {
    let path = root.join(name);
    if path.components().any(|component| component == Component::ParentDir) {
        return Err(recovery_required());
    }
    Ok(path.into_os_string())
}

#[cfg(unix)]
fn verify_runtime_output_boundary(
    runtime: &File,
    runtime_identity: ExecutableIdentity,
    experiment: &File,
    experiment_identity: ExecutableIdentity,
    experiment_id: &str,
) -> Result<(), AppError> {
    if verify_runtime_directory(runtime)? != runtime_identity
        || verify_runtime_directory(experiment)? != experiment_identity
    {
        return Err(recovery_required());
    }
    let entries = check_output_directory_entries(runtime)?;
    if entries.len() != 1 || entries[0] != OsStr::new(experiment_id) {
        return Err(recovery_required());
    }
    let current = open_runtime_directory_at(runtime, OsStr::new(experiment_id))
        .map_err(|_| recovery_required())?;
    if verify_runtime_directory(&current)? != experiment_identity {
        return Err(recovery_required());
    }
    Ok(())
}

#[cfg(unix)]
fn verify_runtime_output_scope(
    runtime: &File,
    runtime_identity: ExecutableIdentity,
    experiment_id: &str,
    expected_experiment_identity: Option<ExecutableIdentity>,
    require_empty: bool,
) -> Result<(), AppError> {
    if verify_runtime_directory(runtime)? != runtime_identity {
        return Err(recovery_required());
    }
    let entries = check_output_directory_entries(runtime)?;
    if entries.len() != 1 || entries[0] != OsStr::new(experiment_id) {
        return Err(recovery_required());
    }
    let experiment = open_runtime_directory_at(runtime, OsStr::new(experiment_id))
        .map_err(|_| recovery_required())?;
    let experiment_identity = verify_runtime_directory(&experiment)?;
    if expected_experiment_identity.is_some_and(|expected| expected != experiment_identity) {
        return Err(recovery_required());
    }
    let expected = RUNTIME_OUTPUT_DIRECTORY_NAMES
        .iter()
        .map(|name| OsString::from(*name))
        .collect::<BTreeSet<_>>();
    let actual = check_output_directory_entries(&experiment)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(recovery_required());
    }
    let mut audit = CheckOutputAuditState::runtime();
    for name in RUNTIME_OUTPUT_DIRECTORY_NAMES {
        let output = open_runtime_directory_at(&experiment, OsStr::new(name))
            .map_err(|_| recovery_required())?;
        verify_runtime_directory(&output)?;
        audit_check_output_directory(&output, 0, experiment_identity.device, &mut audit)?;
        if require_empty && audit.entries != 0 {
            return Err(recovery_required());
        }
    }
    Ok(())
}

#[cfg(unix)]
struct CheckOutputAuditState {
    entries: usize,
    allocated_bytes: u64,
    max_entries: usize,
    max_bytes: u64,
}

#[cfg(unix)]
impl Default for CheckOutputAuditState {
    fn default() -> Self {
        Self {
            entries: 0,
            allocated_bytes: 0,
            max_entries: MAX_CHECK_OUTPUT_ENTRIES,
            max_bytes: MAX_CHECK_OUTPUT_ALLOCATED_BYTES,
        }
    }
}

#[cfg(unix)]
impl CheckOutputAuditState {
    fn runtime() -> Self {
        Self {
            max_entries: MAX_RUNTIME_OUTPUT_ENTRIES,
            max_bytes: MAX_RUNTIME_OUTPUT_ALLOCATED_BYTES,
            ..Self::default()
        }
    }
}

#[cfg(unix)]
fn audit_check_output_directory(
    directory: &File,
    depth: usize,
    root_device: u64,
    state: &mut CheckOutputAuditState,
) -> Result<(), AppError> {
    if depth > MAX_CHECK_OUTPUT_DEPTH {
        return Err(recovery_required());
    }
    for name in check_output_directory_entries(directory)? {
        state.entries = state.entries.saturating_add(1);
        if state.entries > state.max_entries {
            return Err(recovery_required());
        }
        let stat = check_output_entry_stat(directory, &name)?.ok_or_else(recovery_required)?;
        let file_type = stat.st_mode as u32 & libc::S_IFMT as u32;
        if stat.st_uid != unsafe { libc::geteuid() as u32 }
            || stat.st_dev as u64 != root_device
            || (file_type == libc::S_IFDIR as u32 && stat.st_mode as u32 & 0o022 != 0)
            || (file_type != libc::S_IFDIR as u32
                && file_type != libc::S_IFREG as u32
                && file_type != libc::S_IFLNK as u32)
        {
            return Err(recovery_required());
        }
        if file_type == libc::S_IFREG as u32 {
            if stat.st_nlink < 1 || stat.st_size < 0 {
                return Err(recovery_required());
            }
            state.allocated_bytes = state
                .allocated_bytes
                .checked_add(stat.st_size as u64)
                .ok_or_else(recovery_required)?;
            if state.allocated_bytes > state.max_bytes {
                return Err(recovery_required());
            }
        } else if file_type == libc::S_IFDIR as u32 {
            let child = open_existing_directory_at(directory, &name)
                .map_err(|_| recovery_required())?;
            audit_check_output_directory(&child, depth + 1, root_device, state)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn remove_check_output_directory(
    directory: &File,
    depth: usize,
    root_device: u64,
    state: &mut CheckOutputAuditState,
) -> Result<(), AppError> {
    if depth > MAX_CHECK_OUTPUT_DEPTH {
        return Err(recovery_required());
    }
    for name in check_output_directory_entries(directory)? {
        state.entries = state.entries.saturating_add(1);
        if state.entries > state.max_entries {
            return Err(recovery_required());
        }
        let initial = check_output_entry_stat(directory, &name)?.ok_or_else(recovery_required)?;
        let file_type = initial.st_mode as u32 & libc::S_IFMT as u32;
        if initial.st_uid != unsafe { libc::geteuid() as u32 }
            || initial.st_dev as u64 != root_device
            || (file_type == libc::S_IFDIR as u32 && initial.st_mode as u32 & 0o022 != 0)
            || (file_type != libc::S_IFDIR as u32
                && file_type != libc::S_IFREG as u32
                && file_type != libc::S_IFLNK as u32)
        {
            return Err(recovery_required());
        }
        if file_type == libc::S_IFREG as u32 {
            if initial.st_nlink < 1 || initial.st_size < 0 {
                return Err(recovery_required());
            }
            state.allocated_bytes = state
                .allocated_bytes
                .checked_add(initial.st_size as u64)
                .ok_or_else(recovery_required)?;
            if state.allocated_bytes > state.max_bytes {
                return Err(recovery_required());
            }
            let current = check_output_entry_stat(directory, &name)?.ok_or_else(recovery_required)?;
            if check_output_identity(&current) != check_output_identity(&initial) {
                return Err(recovery_required());
            }
            unlink_check_output_entry(directory, &name, false)?;
        } else if file_type == libc::S_IFDIR as u32 {
            let child = open_existing_directory_at(directory, &name)
                .map_err(|_| recovery_required())?;
            remove_check_output_directory(&child, depth + 1, root_device, state)?;
            let current = check_output_entry_stat(directory, &name)?.ok_or_else(recovery_required)?;
            if check_output_identity(&current) != check_output_identity(&initial) {
                return Err(recovery_required());
            }
            unlink_check_output_entry(directory, &name, true)?;
        } else {
            let current = check_output_entry_stat(directory, &name)?.ok_or_else(recovery_required)?;
            if check_output_identity(&current) != check_output_identity(&initial) {
                return Err(recovery_required());
            }
            unlink_check_output_entry(directory, &name, false)?;
        }
        if check_output_entry_stat(directory, &name)?.is_some() {
            return Err(recovery_required());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn check_output_directory_entries(directory: &File) -> Result<Vec<OsString>, AppError> {
    let dot = std::ffi::CString::new(".").expect("static directory component");
    let duplicate = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            dot.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
    };
    if duplicate < 0 {
        return Err(recovery_required());
    }
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        unsafe { libc::close(duplicate) };
        return Err(recovery_required());
    }
    let mut names = Vec::new();
    loop {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        unsafe {
            *libc::__errno_location() = 0;
        }
        #[cfg(target_os = "macos")]
        unsafe {
            *libc::__error() = 0;
        }
        let entry = unsafe { libc::readdir(stream) };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            unsafe { libc::closedir(stream) };
            if error.raw_os_error().is_some_and(|value| value != 0) {
                return Err(recovery_required());
            }
            return Ok(names);
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        if names.len() >= MAX_CHECK_OUTPUT_ENTRIES {
            unsafe { libc::closedir(stream) };
            return Err(recovery_required());
        }
        names.push(OsString::from_vec(name.to_bytes().to_vec()));
    }
}

#[cfg(unix)]
fn check_output_entry_stat(
    parent: &File,
    name: &OsStr,
) -> Result<Option<libc::stat>, AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.check_output", "entry name contains NUL"))?;
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
        return Ok(Some(unsafe { stat.assume_init() }));
    }
    if io::Error::last_os_error().kind() == io::ErrorKind::NotFound {
        Ok(None)
    } else {
        Err(recovery_required())
    }
}

#[cfg(unix)]
fn check_output_entry_identity(
    parent: &File,
    name: &OsStr,
) -> Result<Option<ExecutableIdentity>, AppError> {
    Ok(check_output_entry_stat(parent, name)?.map(|stat| check_output_identity(&stat)))
}

#[cfg(unix)]
fn check_output_identity(stat: &libc::stat) -> ExecutableIdentity {
    ExecutableIdentity {
        device: stat.st_dev as u64,
        inode: stat.st_ino as u64,
        owner: stat.st_uid as u32,
        mode: stat.st_mode as u32 & 0o7777,
    }
}

#[cfg(unix)]
fn unlink_check_output_entry(
    parent: &File,
    name: &OsStr,
    directory: bool,
) -> Result<(), AppError> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| validation("code_change.check_output", "entry name contains NUL"))?;
    let result = unsafe {
        libc::unlinkat(
            parent.as_raw_fd(),
            name.as_ptr(),
            if directory { libc::AT_REMOVEDIR } else { 0 },
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(recovery_required())
    }
}

#[cfg(unix)]
#[derive(Clone)]
struct BoundedToolRunner {
    policy: ResolvedExecutionPolicy,
    output_limit: usize,
    timeout: Option<Duration>,
}

#[cfg(unix)]
struct ToolProcessLease {
    child: Option<crate::process::VerifiedChild>,
    temporary_index: Option<Arc<OwnedTemporaryIndex>>,
    check_outputs: Option<OwnedCheckOutputs>,
}

#[cfg(unix)]
impl ToolProcessLease {
    fn new(
        child: crate::process::VerifiedChild,
        temporary_index: Option<Arc<OwnedTemporaryIndex>>,
        check_outputs: Option<OwnedCheckOutputs>,
    ) -> Self {
        Self {
            child: Some(child),
            temporary_index,
            check_outputs,
        }
    }

    fn child_mut(&mut self) -> &mut crate::process::VerifiedChild {
        self.child.as_mut().expect("tool process lease owns child")
    }

    fn take_child(&mut self) -> crate::process::VerifiedChild {
        self.child.take().expect("tool process lease owns child")
    }

    fn cleanup_check_outputs(&mut self) -> Result<(), AppError> {
        if let Some(outputs) = self.check_outputs.take() {
            outputs.cleanup()
        } else {
            Ok(())
}
    }
}

#[cfg(unix)]
impl Drop for ToolProcessLease {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let temporary_index = self.temporary_index.take();
        let check_outputs = self.check_outputs.take();
        // Cancellation cannot leave a detached task holding the Git index
        // pathname.  Reap the complete owned group synchronously before the
        // temporary owner is dropped; retain the index for recovery if the
        // kernel does not prove quiescence.
        let result = child.terminate_and_reap_blocking();
        if result.is_err() {
            if let Some(index) = temporary_index.as_ref() {
                index.retain_for_recovery();
            }
            if let Some(outputs) = check_outputs.as_ref() {
                outputs.unproven.store(true, Ordering::Release);
        }
        } else if let Some(outputs) = check_outputs.as_ref() {
            let _ = outputs.cleanup();
        }
        drop(temporary_index);
        drop(check_outputs);
    }
}

#[cfg(unix)]
impl BoundedToolRunner {
    fn new(policy: &ResolvedExecutionPolicy, output_limit: usize) -> Self {
        Self {
            policy: policy.clone(),
            output_limit,
            timeout: None,
        }
    }

    fn with_timeout(
        policy: &ResolvedExecutionPolicy,
        output_limit: usize,
        timeout: Duration,
    ) -> Self {
        Self {
            policy: policy.clone(),
            output_limit,
            timeout: Some(timeout),
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
        check_outputs: Option<OwnedCheckOutputs>,
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
                check_outputs,
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
        mut check_outputs: Option<OwnedCheckOutputs>,
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
            .checked_add(self.timeout.unwrap_or_else(|| {
                Duration::from_secs(
                    u64::from(self.policy.campaign_limits.code_change_check_timeout_minutes)
                        .saturating_mul(60),
                )
            }))
            .ok_or(AppError::Runtime {
                operation: "start bounded code-change tool deadline",
            })?;
        if let Some(outputs) = check_outputs.as_ref() {
            outputs.verify_before_command()?;
        }
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
                if let Some(outputs) = check_outputs.take() {
                    let _ = outputs.cleanup();
                }
                return Err(error)
            }
            Err(crate::process::SpawnVerifiedCommandBeforeError::Cleanup(error)) => {
                if let Some(outputs) = check_outputs.take() {
                    let _ = outputs.cleanup();
                }
                return Err(error)
            }
        };
        let mut lease = ToolProcessLease::new(child, _temporary_index, check_outputs);

        if let Err(error) = lease.child_mut().release_before(deadline) {
            return Err(cleanup_tool_failure(&mut lease, error, deadline).await);
        }
        if let Err(error) = lease.child_mut().confirm_exec_before(deadline).await {
            return Err(cleanup_tool_failure(&mut lease, error, deadline).await);
        }
        if let Err(error) = lease.child_mut().wait_for_release_ack_before(deadline).await {
            return Err(cleanup_tool_failure(&mut lease, error, deadline).await);
        }
        let stdout = match lease.child_mut().take_stdout() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(&mut lease, error, deadline).await),
        };
        let stderr = match lease.child_mut().take_stderr() {
            Ok(stream) => stream,
            Err(error) => return Err(cleanup_tool_failure(&mut lease, error, deadline).await),
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
                lease.cleanup_check_outputs()?;
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
        lease.cleanup_check_outputs()?;
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

struct CheckRoundResult {
    git_diff_passed: bool,
    project_check_count: usize,
    all_project_checks_passed: bool,
    final_diff_matches: bool,
}

impl CheckRoundResult {
    fn passed(&self) -> bool {
        self.git_diff_passed
            && self.project_check_count >= 1
            && self.all_project_checks_passed
            && self.final_diff_matches
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
    check_timeout_override: Option<Duration>,
}

#[allow(dead_code)]
#[cfg(unix)]
impl<'a> CheckRunner<'a> {
    async fn run_supervisor_check(
        &self,
        repository: &CodeChangeRepository<'_>,
        run_id: &str,
        attempt: i64,
        expected: &DiffFacts,
        row: &CodeChangeCheck,
    ) -> Result<bool, AppError> {
        if row.ordinal != 0 || row.source != "supervisor" {
            return Err(recovery_required());
        }
        match row.status {
            CodeChangeCheckStatus::Passed => return Ok(true),
            CodeChangeCheckStatus::Failed | CodeChangeCheckStatus::TimedOut => return Ok(false),
            CodeChangeCheckStatus::Reserved => {}
        }
        let started = unix_timestamp()?;
        let result = CandidateRepository::new(self.manager, self.candidate)?
            .run_supervisor_diff_check(expected)
            .await;
        let completion = classify_check_result(result)?;
        persist_check_completion(repository, run_id, attempt, row.ordinal, started, completion)
    }

    async fn run_project_checks(
        &self,
        repository: &CodeChangeRepository<'_>,
        run_id: &str,
        attempt: i64,
        checks: &[ProposedCheck],
        rows: &[CodeChangeCheck],
    ) -> Result<bool, AppError> {
        if checks.len() != rows.len() {
            return Err(recovery_required());
        }
        let candidate = self.candidate.anchor.verify_identity()?;
        self.manager.validate_git_boundary(&candidate)?;
        for (index, (check, row)) in checks.iter().zip(rows).enumerate() {
            if row.ordinal != index as i64 + 1
                || !matches!(row.source.as_str(), "discovered" | "editor")
            {
                return Err(recovery_required());
            }
            match row.status {
                CodeChangeCheckStatus::Passed => continue,
                CodeChangeCheckStatus::Failed | CodeChangeCheckStatus::TimedOut => return Ok(false),
                CodeChangeCheckStatus::Reserved => {
                    let started = unix_timestamp()?;
                    let result = self
                        .run_project_check(
                            &candidate,
                            check,
                            run_id,
                            attempt,
                            row.ordinal,
                        )
                        .await;
                    let completion = classify_check_result(result)?;
                    if !persist_check_completion(
                        repository,
                        run_id,
                        attempt,
                        row.ordinal,
                        started,
                        completion,
                    )? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    fn persisted_plan(
        &self,
        db: &Db,
        run_id: &str,
        attempt: i64,
        editor_checks: &[ProposedCheck],
    ) -> Result<(Vec<ProposedCheck>, Vec<CodeChangeCheck>), AppError> {
        let available = [
            CodeChangeTool::Cargo,
            CodeChangeTool::Uv,
            CodeChangeTool::Python,
        ]
        .into_iter()
        .filter(|tool| self.manager.policy.code_change_tool(*tool).is_some())
        .collect::<BTreeSet<_>>();
        let root = self.candidate.anchor.canonical_path.as_path();
        let limits = &self.manager.policy.campaign_limits;
        validate_proposed_checks(editor_checks, root, limits, &available)?;
        let discovered = discover_project_checks(root, &available)?;
        let merged = merge_project_checks(
            &discovered,
            editor_checks,
            limits.max_code_change_checks as usize,
        )?;
        let planned = planned_check_rows(
            attempt,
            &self.manager.base_sha,
            &discovered,
            editor_checks,
            limits.max_code_change_checks as usize,
        )?;
        let repository = CodeChangeRepository::new(db);
        let existing = repository.list_checks(run_id, attempt)?;
        let rows = if existing.is_empty()
            || existing.iter().all(|row| {
                row.source == "editor"
                    && row.status == CodeChangeCheckStatus::Reserved
                    && row.output_digest.is_none()
                    && row.summary.is_none()
                    && row.started_at.is_none()
                    && row.finished_at.is_none()
            })
        {
            repository.replace_attempt_checks(run_id, attempt, &planned, unix_timestamp()?)?
        } else {
            validate_persisted_check_plan(&existing, &planned)?;
            existing
        };
        Ok((merged, rows))
    }

    async fn run_all(
        &self,
        db: &Db,
        run_id: &str,
        attempt: i64,
        expected: &DiffFacts,
        editor_checks: &[ProposedCheck],
    ) -> Result<CheckRoundResult, AppError> {
        let validator = CandidateValidator::new(self.manager, self.candidate)?;
        let initial = validator.verify().await?;
        if initial != *expected {
            return Err(recovery_required());
        }
        let (checks, rows) = self.persisted_plan(db, run_id, attempt, editor_checks)?;
        let (supervisor_row, project_rows) = rows.split_first().ok_or_else(recovery_required)?;
        let repository = CodeChangeRepository::new(db);
        let git_diff_passed = self
            .run_supervisor_check(
                &repository,
                run_id,
                attempt,
                expected,
                supervisor_row,
            )
            .await?;
        if !git_diff_passed {
            return Ok(CheckRoundResult {
                git_diff_passed: false,
                project_check_count: checks.len(),
                all_project_checks_passed: false,
                final_diff_matches: false,
            });
        }
        let all_project_checks_passed = self
            .run_project_checks(
                &repository,
                run_id,
                attempt,
                &checks,
                project_rows,
            )
            .await?;
        if !all_project_checks_passed {
            return Ok(CheckRoundResult {
                git_diff_passed: true,
                project_check_count: checks.len(),
                all_project_checks_passed: false,
                final_diff_matches: false,
            });
        }
        let fresh = validator.verify().await?;
        let final_diff_matches = fresh == *expected;
        Ok(CheckRoundResult {
            git_diff_passed: true,
            project_check_count: checks.len(),
            all_project_checks_passed: true,
            final_diff_matches,
        })
    }

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
        for (index, check) in checks.iter().enumerate() {
            let output = self
                .run_project_check(&candidate, check, "adhoc", 1, index as i64 + 1)
                .await?;
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

    async fn run_project_check(
        &self,
        candidate: &VerifiedProjectRoot,
        check: &ProposedCheck,
        run_id: &str,
        attempt: i64,
        ordinal: i64,
    ) -> Result<BoundedToolOutput, AppError> {
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
            candidate,
            Path::new(&check.working_directory),
        )?;
        self.manager.validate_git_boundary(candidate)?;
        let mut environment = SanitizedEnvironment::for_code_change_tool(&self.manager.policy)?;
        let outputs = OwnedCheckOutputs::create(
            &self.manager.policy,
            run_id,
            attempt,
            ordinal,
            tool,
        )?;
        outputs.apply_environment(&mut environment, tool)?;
        let output_runner = self
            .check_timeout_override
            .map(|timeout| {
                BoundedToolRunner::with_timeout(
                    &self.manager.policy,
                    MAX_CHECK_OUTPUT_BYTES,
                    timeout,
                )
            })
            .unwrap_or_else(|| BoundedToolRunner::new(&self.manager.policy, MAX_CHECK_OUTPUT_BYTES));
        let output = output_runner
            .run(
                executable.clone(),
                candidate,
                &working_directory,
                check.argv.iter().map(OsString::from).collect(),
                environment,
                "project check",
                None,
                None,
                Some(outputs),
            )
            .await;
        let boundary = self.manager.validate_git_boundary(candidate);
        let output = match (output, boundary) {
            (_, Err(error)) => return Err(error),
            (Err(error), Ok(())) => return Err(error),
            (Ok(output), Ok(())) => output,
        };
        self.manager.validate_original_state().await?;
        Ok(output)
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
    lease: &mut ToolProcessLease,
    original: AppError,
    deadline: Instant,
) -> AppError {
    match terminate_process_group_before(lease.child_mut(), deadline).await {
        Ok(()) => match lease.cleanup_check_outputs() {
        Ok(()) => original,
            Err(error) => error,
        },
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

#[derive(Debug, Deserialize)]
struct CleanupTaskSignature {
    group: String,
    id: i64,
    enqueued_at: Option<String>,
    started_at: Option<String>,
    ended_at: Option<String>,
    state: String,
}

fn cleanup_task_timestamp_matches(raw: &Option<String>, stored: Option<i64>) -> bool {
    match (raw.as_deref(), stored) {
        (None, None) => true,
        (Some(raw), Some(stored)) => parse_timestamp(raw) == Some(stored),
        _ => false,
    }
}

fn cleanup_task_observation_managed_identity(
    observation: &TaskObservation,
    expected_group: &str,
) -> Option<String> {
    if observation.command.len() != 1 || observation.pueue_group != expected_group {
        return None;
    }
    let encoded = observation.task_signature.strip_prefix("pueue-task:v1:")?;
    let identity = serde_json::from_str::<CleanupTaskSignature>(encoded).ok()?;
    if identity.group != observation.pueue_group
        || identity.id != observation.pueue_task_id
        || !cleanup_task_timestamp_matches(&identity.enqueued_at, observation.enqueued_at)
        || !cleanup_task_timestamp_matches(&identity.started_at, observation.started_at)
        || !cleanup_task_timestamp_matches(&identity.ended_at, observation.ended_at)
        || identity.state != observation.state
    {
        return None;
    }
    let task = PueueTask {
        id: identity.id,
        group: identity.group,
        command: observation.command[0].clone(),
        state: identity.state,
        enqueued_at: identity.enqueued_at,
        started_at: identity.started_at,
        ended_at: identity.ended_at,
        result: None,
    };
    if task_signature(&task) != observation.task_signature {
        return None;
    }
    managed_task_run_signature(&task)
}

fn is_terminal_experiment_status(status: ExperimentStatus) -> bool {
    matches!(
        status,
        ExperimentStatus::Succeeded
            | ExperimentStatus::Failed
            | ExperimentStatus::Cancelled
    )
}

fn validate_disappeared_cleanup_target(_allow_missing_target: bool) -> Result<(), AppError> {
    Err(recovery_required())
}

fn validate_retained_candidate_ref(
    actual: Option<&str>,
    expected: Option<&str>,
) -> Result<(), AppError> {
    match (actual, expected) {
        (None, None) => Ok(()),
        (Some(actual), Some(expected)) if actual == expected => Ok(()),
        _ => Err(recovery_required()),
    }
}

fn validate_cleanup_state_for_mutation(
    state: CodeChangeState,
    cleanup_completed_at: Option<i64>,
) -> Result<(), AppError> {
    if cleanup_completed_at.is_some() {
        return Err(recovery_required());
    }
    if !matches!(
        state,
        CodeChangeState::CandidateReady
            | CodeChangeState::CleanupPending
            | CodeChangeState::Rejected
    ) {
        return Err(recovery_required());
    }
    Ok(())
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

fn is_terminal_task_observation_state(state: &str) -> bool {
    matches!(
        state.to_ascii_lowercase().as_str(),
        "done" | "failed" | "killed" | "finished" | "success"
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

fn campaign_start_base_sha(db: &Db, campaign_id: &str) -> Result<String, AppError> {
    let campaign = CampaignRepository::new(db)
        .find_by_id(campaign_id)?
        .ok_or_else(recovery_required)?;
    let base = campaign.base_revision_sha.ok_or_else(recovery_required)?;
    canonical_full_sha(&base)
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
        let retained_common = if text == git_descriptor_path(GIT_COMMON_DIR_FD) {
            let repository = if root.anchor.canonical_path == manager.project.root_path {
                manager.original_repository.as_ref()
            } else if root.anchor.canonical_path == manager.worktree_path {
                manager.candidate_repository.as_ref()
            } else {
                None
            };
            let repository = repository.ok_or_else(recovery_required)?;
            Some(&repository.common)
        } else {
            None
        };
        resolve_git_common_directory_output(text, &root.anchor.canonical_path, retained_common)
    }
}

#[cfg(unix)]
fn resolve_git_common_directory_output(
    text: &str,
    root_path: &Path,
    retained_common: Option<&GitDirectoryProof>,
) -> Result<PathBuf, AppError> {
    if text == git_descriptor_path(GIT_COMMON_DIR_FD) {
        let common = retained_common.ok_or_else(recovery_required)?;
        revalidate_git_directory_descriptor(common)?;
        return Ok(common.path.clone());
    }
    let path = Path::new(text);
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        root_path.join(path)
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
    let (value, target) = match kind {
        GitPointerKind::GitDir => (
            text.strip_prefix("gitdir:")
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    validation(
                        "git.metadata",
                        "Git worktree metadata must identify a Git directory",
                    )
                })?,
            GitPointerTarget::Directory,
        ),
        GitPointerKind::Path(target) => (text.trim(), target),
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
    let valid_target = match target {
        GitPointerTarget::Directory => metadata.is_dir(),
        GitPointerTarget::File => metadata.is_file(),
    };
    if !valid_target {
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
    let mut start = 0;
    while start < bytes.len() {
        let record_end = bytes[start..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|offset| start + offset)
            .ok_or_else(|| validation("code_change.git_refs", "Git ref output is malformed"))?;
        let record = &bytes[start..record_end];
        if record.is_empty() || record.last() != Some(&0) {
            return Err(validation(
                "code_change.git_refs",
                "Git ref output is malformed",
            ));
        }
        let fields = &record[..record.len() - 1];
        let separator = fields.iter().position(|byte| *byte == 0).ok_or_else(|| {
            validation("code_change.git_refs", "Git ref output is malformed")
        })?;
        let reference = &fields[..separator];
        let object = &fields[separator + 1..];
        if reference.is_empty() || object.is_empty() || object.contains(&0) {
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
        start = record_end + 1;
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

fn parse_diff_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
    if bytes.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(validation(
            "code_change.diff_paths",
            "exceeds the bounded Git output size",
        ));
    }
    let mut paths = BTreeSet::new();
    for path in bytes.split(|byte| *byte == 0) {
        if path.is_empty() {
            continue;
        }
        insert_status_path(&mut paths, path)?;
        if paths.len() > MAX_STATUS_PATHS {
            return Err(validation(
                "code_change.diff_paths",
                "exceeds the bounded changed-file count",
            ));
        }
    }
    Ok(paths.into_iter().collect())
}

fn validate_ignored_status_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
    let ignored_paths = parse_ignored_status_paths(bytes)?;
    if ignored_paths.iter().any(|path| is_protected_path(path)) {
        return Err(validation(
            "code_change.diff_paths",
            "contains an ignored protected service path",
        ));
    }
    Ok(ignored_paths)
}

fn parse_ignored_status_paths(bytes: &[u8]) -> Result<Vec<PathBuf>, AppError> {
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

fn is_python_check_cache_path(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(
            component,
            Component::Normal(name)
                if name == OsStr::new(".pytest_cache") || name == OsStr::new("__pycache__")
        )
    })
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

#[cfg(unix)]
fn bind_terminal_result_outputs(
    root: &File,
    experiment_id: &str,
) -> Result<BoundTerminalResultOutputs, AppError> {
    validate_internal_id("experiment_id", experiment_id)?;
    let root_metadata = root.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_directory(&root_metadata) {
        return Err(recovery_required());
    }
    let root_identity = executable_identity_from_metadata(&root_metadata);
    let service = open_runtime_directory_at(root, OsStr::new(RUNTIME_SERVICE_DIRECTORY))
        .map_err(|_| recovery_required())?;
    let service_identity = verify_runtime_directory(&service)?;
    let (results, artifacts, artifacts_identity, artifact_directory, artifact_identity) =
        bind_terminal_result_service(&service, experiment_id)?;
    Ok(BoundTerminalResultOutputs {
        root_identity,
        service,
        service_identity,
        results,
        artifacts,
        artifacts_identity,
        artifact_directory,
        artifact_identity,
        experiment_id: experiment_id.to_owned(),
    })
}

#[cfg(unix)]
fn bind_terminal_result_service(
    service: &File,
    experiment_id: &str,
) -> Result<
    (
        BoundTerminalResults,
        File,
        ExecutableIdentity,
        File,
        ExecutableIdentity,
    ),
    AppError,
> {
    let mut results = BoundTerminalResults::Missing;
    let mut artifacts = None;
    for entry in fs::read_dir(descriptor_path(service)).map_err(|_| recovery_required())? {
        let entry = entry.map_err(|_| recovery_required())?;
        let name = entry.file_name();
        if name == OsStr::new(RUNTIME_RESULTS_DIRECTORY) {
            results = bind_terminal_result_node(service, &name, experiment_id)?;
        } else if name == OsStr::new(RUNTIME_ARTIFACTS_DIRECTORY) {
            let directory = open_runtime_directory_at(service, &name)
                .map_err(|_| recovery_required())?;
            let identity = verify_runtime_directory(&directory)?;
            artifacts = Some((directory, identity));
        } else if name == OsStr::new(RUNTIME_OUTPUTS_DIRECTORY) {
            let directory = open_runtime_directory_at(service, &name)
                .map_err(|_| recovery_required())?;
            let identity = verify_runtime_directory(&directory)?;
            verify_runtime_output_scope(&directory, identity, experiment_id, None, false)?;
        } else {
            return Err(recovery_required());
        }
    }
    let (artifacts, artifacts_identity) = artifacts.ok_or_else(recovery_required)?;
    let (artifact_directory, artifact_identity) =
        validate_terminal_result_artifact_directory(&artifacts, experiment_id)?;
    Ok((
        results,
        artifacts,
        artifacts_identity,
        artifact_directory,
        artifact_identity,
    ))
}

#[cfg(unix)]
fn bind_terminal_result_node(
    service: &File,
    name: &OsStr,
    experiment_id: &str,
) -> Result<BoundTerminalResults, AppError> {
    match open_ignored_entry_at(service, name)? {
        None => Ok(BoundTerminalResults::Missing),
        Some(IgnoredEntry::Symlink) => Err(recovery_required()),
        Some(IgnoredEntry::File(_)) => {
            let (file, identity) = open_runtime_result_file_at(service, name)?;
            Ok(BoundTerminalResults::InvalidFile { file, identity })
        }
        Some(IgnoredEntry::Directory(directory)) => {
            let identity = verify_runtime_directory(&directory)?;
            let manifest = bind_terminal_result_manifest(&directory, experiment_id)?;
            Ok(BoundTerminalResults::Directory {
                directory,
                identity,
                manifest,
            })
        }
    }
}

#[cfg(unix)]
fn bind_terminal_result_manifest(
    directory: &File,
    experiment_id: &str,
) -> Result<BoundTerminalManifest, AppError> {
    let expected_name = OsString::from(format!("{experiment_id}.json"));
    let mut manifest = None;
    for entry in fs::read_dir(descriptor_path(directory)).map_err(|_| recovery_required())? {
        let entry = entry.map_err(|_| recovery_required())?;
        let name = entry.file_name();
        if name != expected_name {
            return Err(recovery_required());
        }
        let (file, identity) = open_runtime_manifest_at(directory, &name)?;
        let length = file
            .metadata()
            .map_err(|_| recovery_required())?
            .len();
        if length > MAX_TERMINAL_RESULT_MANIFEST_BYTES {
            if manifest
                .replace(BoundTerminalManifest::Invalid {
                    file,
                    identity,
                    length,
                })
                .is_some()
            {
                return Err(recovery_required());
            }
        } else {
            let bytes = read_runtime_manifest_snapshot(&file)?;
            if manifest
                .replace(BoundTerminalManifest::Ready {
                    file,
                    identity,
                    bytes,
                })
                .is_some()
            {
                return Err(recovery_required());
            }
        }
    }
    Ok(manifest.unwrap_or(BoundTerminalManifest::Missing))
}

#[cfg(unix)]
fn open_runtime_result_file_at(
    parent: &File,
    name: &OsStr,
) -> Result<(File, ExecutableIdentity), AppError> {
    let file = open_runtime_manifest_fd(parent, name, libc::O_RDONLY | libc::O_NONBLOCK)
        .map_err(|_| recovery_required())?;
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_result_file(&metadata) {
        return Err(recovery_required());
    }
    Ok((file, executable_identity_from_metadata(&metadata)))
}

#[cfg(unix)]
fn read_runtime_manifest_snapshot(file: &File) -> Result<Vec<u8>, AppError> {
    let before = file.metadata().map_err(|_| recovery_required())?;
    if before.len() > MAX_TERMINAL_RESULT_MANIFEST_BYTES {
        return Err(recovery_required());
    }
    let length = usize::try_from(before.len()).map_err(|_| recovery_required())?;
    let first = read_runtime_manifest_bytes(file, length)?;
    let after = file.metadata().map_err(|_| recovery_required())?;
    if after.len() != before.len() {
        return Err(recovery_required());
    }
    let second = read_runtime_manifest_bytes(file, length)?;
    if first != second {
        return Err(recovery_required());
    }
    Ok(first)
}

#[cfg(unix)]
fn read_runtime_manifest_bytes(file: &File, length: usize) -> Result<Vec<u8>, AppError> {
    let mut bytes = vec![0; length];
    let mut offset = 0;
    while offset < length {
        let read = file
            .read_at(&mut bytes[offset..], offset as u64)
            .map_err(|_| recovery_required())?;
        if read == 0 {
            return Err(recovery_required());
        }
        offset += read;
    }
    Ok(bytes)
}

#[cfg(unix)]
#[cfg(test)]
fn inspect_terminal_result_outputs(
    root: &File,
    experiment_id: &str,
) -> Result<TerminalResultOutputStatus, AppError> {
    Ok(bind_terminal_result_outputs(root, experiment_id)?.status())
}

#[cfg(unix)]
#[cfg(test)]
fn validate_terminal_result_outputs(root: &File, experiment_id: &str) -> Result<(), AppError> {
    match inspect_terminal_result_outputs(root, experiment_id)? {
        TerminalResultOutputStatus::Ready => Ok(()),
        TerminalResultOutputStatus::Missing | TerminalResultOutputStatus::Invalid => {
            Err(recovery_required())
        }
    }
}

#[cfg(unix)]
fn open_runtime_directory_at(parent: &File, name: &OsStr) -> io::Result<File> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL runtime path"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn verify_runtime_directory(directory: &File) -> Result<ExecutableIdentity, AppError> {
    let metadata = directory.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != RUNTIME_DIRECTORY_MODE
    {
        return Err(recovery_required());
    }
    Ok(executable_identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn mkdirat(parent: &File, name: &OsStr, mode: u32) -> io::Result<()> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL runtime path"))?;
    let result = unsafe {
        libc::mkdirat(
            parent.as_raw_fd(),
            name.as_ptr(),
            mode as libc::mode_t,
        )
    };
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn open_or_create_runtime_directory_at(
    parent: &File,
    name: &OsStr,
) -> Result<(File, ExecutableIdentity), AppError> {
    for _ in 0..2 {
        match open_runtime_directory_at(parent, name) {
            Ok(directory) => {
                let identity = verify_runtime_directory(&directory)?;
                return Ok((directory, identity));
            }
            Err(error) if error.raw_os_error() == Some(libc::ENOENT) => {
                match mkdirat(parent, name, RUNTIME_DIRECTORY_MODE) {
                    Ok(()) => continue,
                    Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
                    Err(_) => return Err(recovery_required()),
                }
            }
            Err(_) => return Err(recovery_required()),
        }
    }
    Err(recovery_required())
}

#[cfg(unix)]
fn open_runtime_manifest_fd(parent: &File, name: &OsStr, flags: i32) -> io::Result<File> {
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL runtime path"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(unix)]
fn verify_runtime_manifest(file: &File) -> Result<ExecutableIdentity, AppError> {
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != RUNTIME_MANIFEST_MODE
    {
        return Err(recovery_required());
    }
    Ok(executable_identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn verify_runtime_result_file(file: &File) -> Result<ExecutableIdentity, AppError> {
    let metadata = file.metadata().map_err(|_| recovery_required())?;
    if !secure_owned_result_file(&metadata) {
        return Err(recovery_required());
    }
    Ok(executable_identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn open_runtime_manifest_at(
    parent: &File,
    name: &OsStr,
) -> Result<(File, ExecutableIdentity), AppError> {
    let c_name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| recovery_required())?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            c_name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result < 0 {
        return Err(recovery_required());
    }
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode as u32 & libc::S_IFMT as u32 != libc::S_IFREG as u32 {
        return Err(recovery_required());
    }
    let file = open_runtime_manifest_fd(parent, name, libc::O_RDONLY | libc::O_NONBLOCK)
        .map_err(|_| recovery_required())?;
    let identity = verify_runtime_manifest(&file)?;
    Ok((file, identity))
}

#[cfg(unix)]
fn open_or_create_runtime_manifest_at(
    parent: &File,
    name: &OsStr,
) -> Result<(File, ExecutableIdentity), AppError> {
    for _ in 0..2 {
        match open_runtime_manifest_at(parent, name) {
            Ok(file) => return Ok(file),
            Err(_) => {
                let result = std::ffi::CString::new(name.as_bytes())
                    .map_err(|_| validation("code_change.path", "runtime path contains NUL"))?;
                let fd = unsafe {
                    libc::openat(
                        parent.as_raw_fd(),
                        result.as_ptr(),
                        libc::O_RDWR
                            | libc::O_CREAT
                            | libc::O_EXCL
                            | libc::O_CLOEXEC
                            | libc::O_NOFOLLOW,
                        RUNTIME_MANIFEST_MODE,
                    )
                };
                if fd >= 0 {
                    let file = unsafe { File::from_raw_fd(fd) };
                    let identity = verify_runtime_manifest(&file)?;
                    return Ok((file, identity));
                }
                let error = io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::EEXIST) {
                    continue;
                }
                return Err(recovery_required());
            }
        }
    }
    Err(recovery_required())
}

#[cfg(unix)]
fn validate_terminal_result_artifact_directory(
    directory: &File,
    experiment_id: &str,
) -> Result<(File, ExecutableIdentity), AppError> {
    let expected_name = OsStr::new(experiment_id);
    let mut artifact = None;
    for entry in fs::read_dir(descriptor_path(directory)).map_err(|_| recovery_required())? {
        let entry = entry.map_err(|_| recovery_required())?;
        let name = entry.file_name();
        if name != expected_name {
            return Err(recovery_required());
        }
        let current = match open_ignored_entry_at(directory, &name)? {
            Some(IgnoredEntry::Directory(directory)) => {
                let identity = verify_runtime_directory(&directory)?;
                (directory, identity)
            }
            Some(IgnoredEntry::Symlink | IgnoredEntry::File(_)) | None => {
                return Err(recovery_required())
            }
        };
        let mut budget = IgnoredScanBudget { entries: 0, bytes: 0 };
        scan_terminal_result_artifacts(&current.0, 0, &mut budget)?;
        artifact = Some(current);
    }
    artifact.ok_or_else(recovery_required)
}

#[cfg(unix)]
fn scan_terminal_result_artifacts(
    directory: &File,
    depth: usize,
    budget: &mut IgnoredScanBudget,
) -> Result<(), AppError> {
    if depth > MAX_IGNORED_SCAN_DEPTH {
        return Err(validation(
            "code_change.artifacts",
            "exceeds the bounded artifact directory depth",
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
        if is_protected_path(Path::new(&name)) {
            return Err(recovery_required());
        }
        let child = match open_ignored_entry_at(directory, &name)? {
            Some(child) => child,
            None => return Err(recovery_required()),
        };
        match child {
            IgnoredEntry::Symlink => return Err(recovery_required()),
            IgnoredEntry::File(metadata) => {
                if !secure_owned_result_file(&metadata) {
                    return Err(recovery_required());
                }
                account_ignored_entry(&metadata, budget)?;
            }
            IgnoredEntry::Directory(child) => {
                scan_terminal_result_artifacts(&child, depth + 1, budget)?;
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn secure_owned_result_file(metadata: &fs::Metadata) -> bool {
    metadata.is_file()
        && metadata.uid() == unsafe { libc::geteuid() as u32 }
        && metadata.mode() & 0o022 == 0
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
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
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
fn create_new_directory_at(parent: &File, name: &OsStr) -> Result<File, AppError> {
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| {
        validation("code_change.path", "directory component contains NUL")
    })?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result < 0 {
        return if io::Error::last_os_error().raw_os_error() == Some(libc::EEXIST) {
            Err(recovery_required())
        } else {
            Err(AppError::Runtime {
                operation: "create code-change output directory",
            })
        };
    }
    open_existing_directory_at(parent, OsStr::from_bytes(name.as_bytes()))
        .map_err(|_| recovery_required())
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

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
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

fn read_bounded_pyproject(path: &Path) -> Result<Option<Vec<u8>>, AppError> {
    #[cfg(unix)]
    let file = {
        let mut options = fs::OpenOptions::new();
        options.read(true).custom_flags(
            libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        );
        match options.open(path) {
            Ok(file) => file,
            Err(source)
                if source.kind() == io::ErrorKind::NotFound
                    || source.raw_os_error() == Some(libc::ELOOP) =>
            {
                return Ok(None)
            }
            Err(source) => {
                return Err(AppError::Io {
                    operation: "read pyproject.toml",
                    source,
                })
            }
        }
    };
    #[cfg(not(unix))]
    let mut file = {
        let metadata = match fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(AppError::Io {
                    operation: "read pyproject.toml",
                    source,
                })
            }
        };
        if !metadata.file_type().is_file() {
            return Ok(None);
        }
        fs::File::open(path).map_err(|source| AppError::Io {
            operation: "read pyproject.toml",
            source,
        })?
    };

    let metadata = file.metadata().map_err(|source| AppError::Io {
        operation: "read pyproject.toml",
        source,
    })?;
    if !metadata.is_file() {
        return Ok(None);
    }
    let mut contents = Vec::with_capacity(MAX_PYPROJECT_BYTES.min(16 * 1024));
    file.take((MAX_PYPROJECT_BYTES + 1) as u64)
        .read_to_end(&mut contents)
        .map_err(|source| AppError::Io {
            operation: "read pyproject.toml",
            source,
        })?;
    if contents.len() > MAX_PYPROJECT_BYTES {
        return Ok(None);
    }
    Ok(Some(contents))
}

fn pyproject_has_pytest(root: &Path) -> Result<bool, AppError> {
    let path = root.join("pyproject.toml");
    let Some(contents) = read_bounded_pyproject(&path)? else {
        return Ok(false);
    };
    let Ok(contents) = std::str::from_utf8(&contents) else {
        return Ok(false);
    };
    let Ok(value) = toml::from_str::<toml::Value>(contents) else {
        return Ok(false);
    };
    Ok(value
        .get("tool")
        .and_then(|tool| tool.get("pytest"))
        .and_then(|pytest| pytest.get("ini_options"))
        .is_some_and(|ini_options| matches!(ini_options, toml::Value::Table(_))))
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
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    fn secure_test_directory(path: &Path) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[cfg(unix)]
    fn secure_test_file(path: &Path) {
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[cfg(unix)]
    fn test_owned_index(root: &Path) -> OwnedTemporaryIndex {
        secure_test_directory(root);
        let parent = File::open(root).unwrap();
        let directory_name = OsString::from(".owned-index");
        fs::create_dir(root.join(&directory_name)).unwrap();
        secure_test_directory(&root.join(&directory_name));
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

    #[cfg(unix)]
    #[test]
    fn git_pointer_target_types_are_enforced() {
        let root = tempdir().unwrap();
        let directory = root.path().join("admin");
        fs::create_dir(&directory).unwrap();
        let file = root.path().join("candidate.git");
        fs::write(&file, b"gitdir\n").unwrap();

        assert!(parse_git_pointer(
            b"admin\n",
            root.path(),
            GitPointerKind::Path(GitPointerTarget::Directory),
        )
        .is_ok());
        assert!(parse_git_pointer(
            b"candidate.git\n",
            root.path(),
            GitPointerKind::Path(GitPointerTarget::Directory),
        )
        .is_err());
        assert!(parse_git_pointer(
            b"admin\n",
            root.path(),
            GitPointerKind::Path(GitPointerTarget::File),
        )
        .is_err());
        assert!(parse_git_pointer(
            b"candidate.git\n",
            root.path(),
            GitPointerKind::Path(GitPointerTarget::File),
        )
        .is_ok());
        assert!(parse_git_pointer(
            b"gitdir: admin\n",
            root.path(),
            GitPointerKind::GitDir,
        )
        .is_ok());
        assert!(parse_git_pointer(
            b"gitdir: candidate.git\n",
            root.path(),
            GitPointerKind::GitDir,
        )
        .is_err());
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
        let cannot_apply =
            br#"{"schema_version":1,"status":"cannot_apply","summary":"blocked","proposed_checks":[]}"#;
        assert!(parse_editor_output(cannot_apply, root.path(), &limits, &available).is_ok());
        assert!(parse_editor_output(br#"{malformed-editor"#, root.path(), &limits, &available).is_err());
        let absolute = format!(
            r#"{{"schema_version":1,"status":"ready","summary":"ok","proposed_checks":[{{"source":"cargo","argv":["cargo","test","--all-targets","--","--test-threads=1"],"working_directory":"/tmp"}}]}}"#
        );
        assert!(
            parse_editor_output(absolute.as_bytes(), root.path(), &limits, &available).is_err()
        );
        let traversal = valid.replace(
            "\"working_directory\":\".\"",
            "\"working_directory\":\"../src\"",
        );
        assert!(
            parse_editor_output(traversal.as_bytes(), root.path(), &limits, &available).is_err()
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

    #[cfg(unix)]
    #[test]
    fn project_check_environment_relocates_profile_outputs() {
        let root = Path::new("/private/service state/check");
        let mut cargo = SanitizedEnvironment::default();
        apply_check_output_environment(&mut cargo, CodeChangeTool::Cargo, root).unwrap();
        assert_eq!(cargo.get("CARGO_TARGET_DIR"), Some(root.join("cargo-target").as_os_str()));
        assert_eq!(cargo.get("TMPDIR"), Some(root.join("tmp").as_os_str()));

        let mut uv = SanitizedEnvironment::default();
        apply_check_output_environment(&mut uv, CodeChangeTool::Uv, root).unwrap();
        assert_eq!(uv.get("UV_PROJECT_ENVIRONMENT"), Some(root.join("uv-venv").as_os_str()));
        assert_eq!(uv.get("UV_CACHE_DIR"), Some(root.join("uv-cache").as_os_str()));
        assert_eq!(uv.get("PYTHONDONTWRITEBYTECODE"), Some(OsStr::new("1")));
        assert_eq!(
            uv.get("PYTEST_ADDOPTS"),
            Some(OsStr::new("-o \"cache_dir=/private/service state/check/pytest-cache\""))
        );

        let mut python = SanitizedEnvironment::default();
        apply_check_output_environment(&mut python, CodeChangeTool::Python, root).unwrap();
        assert_eq!(python.get("PYTHONDONTWRITEBYTECODE"), Some(OsStr::new("1")));
        assert_eq!(
            python.get("PYTEST_ADDOPTS"),
            Some(OsStr::new("-o \"cache_dir=/private/service state/check/pytest-cache\""))
        );
        assert!(python.get("CARGO_TARGET_DIR").is_none());
        assert!(python.get("UV_PROJECT_ENVIRONMENT").is_none());
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires real Cargo, uv, and Python installations"]
    fn real_tool_fixture_writes_only_owned_check_outputs() {
        use std::process::Command;

        let temp = tempdir().unwrap();
        secure_test_directory(temp.path());
        let state_path = temp.path().join("state");
        let checks_path = state_path.join("code-change-checks");
        let run_path = checks_path.join("run-1");
        let attempt_path = run_path.join("attempt-1");
        let root_path = attempt_path.join("check-1");
        for path in [&state_path, &checks_path, &run_path, &attempt_path, &root_path] {
            fs::create_dir(path).unwrap();
            secure_test_directory(path);
        }
        let state_root = Arc::new(File::open(&state_path).unwrap());
        let checks = Arc::new(File::open(&checks_path).unwrap());
        let run = Arc::new(File::open(&run_path).unwrap());
        let attempt = Arc::new(File::open(&attempt_path).unwrap());
        let root = Arc::new(File::open(&root_path).unwrap());
        let outputs = OwnedCheckOutputs {
            state_root_identity: directory_identity(&state_root).unwrap(),
            checks_identity: directory_identity(&checks).unwrap(),
            run_identity: directory_identity(&run).unwrap(),
            attempt_identity: directory_identity(&attempt).unwrap(),
            root_identity: directory_identity(&root).unwrap(),
            state_root,
            checks,
            checks_name: OsString::from("code-change-checks"),
            run,
            run_name: OsString::from("run-1"),
            attempt,
            attempt_name: OsString::from("attempt-1"),
            root,
            root_name: OsString::from("check-1"),
            path: root_path.clone(),
            unproven: AtomicBool::new(false),
        };

        let cargo_project = temp.path().join("cargo-project");
        fs::create_dir(&cargo_project).unwrap();
        secure_test_directory(&cargo_project);
        fs::create_dir(cargo_project.join("src")).unwrap();
        secure_test_directory(&cargo_project.join("src"));
        fs::write(
            cargo_project.join("Cargo.toml"),
            "[package]\nname = \"owned-check\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        secure_test_file(&cargo_project.join("Cargo.toml"));
        fs::write(cargo_project.join("src/lib.rs"), "pub fn check() {}\n").unwrap();
        secure_test_file(&cargo_project.join("src/lib.rs"));
        let cargo_home = temp.path().join("cargo-home");
        fs::create_dir(&cargo_home).unwrap();
        secure_test_directory(&cargo_home);
        let cargo_status = Command::new("cargo")
            .args(["check", "--offline", "--quiet"])
            .current_dir(&cargo_project)
            .env("CARGO_HOME", &cargo_home)
            .env("CARGO_TARGET_DIR", root_path.join("cargo-target"))
            .env("CARGO_NET_OFFLINE", "true")
            .status()
            .unwrap();
        assert!(cargo_status.success());
        assert!(root_path.join("cargo-target").is_dir());

        let uv_status = Command::new("uv")
            .args(["venv"])
            .arg(root_path.join("uv-venv"))
            .env("UV_CACHE_DIR", root_path.join("uv-cache"))
            .env("UV_PYTHON_INSTALL_DIR", root_path.join("uv-python"))
            .status()
            .unwrap();
        assert!(uv_status.success());
        assert!(root_path.join("uv-venv").is_dir());
        for path in [root_path.join("uv-cache"), root_path.join("uv-python")] {
            fs::create_dir_all(&path).unwrap();
            secure_test_directory(&path);
        }

        let python_project = temp.path().join("python-project");
        fs::create_dir(&python_project).unwrap();
        secure_test_directory(&python_project);
        fs::write(python_project.join("sample.py"), "value = 1\n").unwrap();
        secure_test_file(&python_project.join("sample.py"));
        let python_status = Command::new("python3")
            .args(["-c", "import sample; assert sample.value == 1"])
            .current_dir(&python_project)
            .env("PYTHONPATH", &python_project)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .status()
            .unwrap();
        assert!(python_status.success());
        assert!(!python_project.join("__pycache__").exists());

        fs::create_dir(root_path.join("pytest-cache")).unwrap();
        secure_test_directory(&root_path.join("pytest-cache"));
        fs::write(root_path.join("pytest-cache/CACHEDIR.TAG"), b"cache").unwrap();
        secure_test_file(&root_path.join("pytest-cache/CACHEDIR.TAG"));
        let outside = temp.path().join("outside");
        fs::write(&outside, b"retained").unwrap();
        std::os::unix::fs::symlink(&outside, root_path.join("uv-venv/outside-link")).unwrap();
        fs::hard_link(&outside, root_path.join("uv-cache/shared-entry")).unwrap();

        assert!(outputs.cleanup().is_ok());
        assert!(!root_path.exists());
        assert_eq!(fs::read(&outside).unwrap(), b"retained");
    }

    #[cfg(unix)]
    #[test]
    fn owned_check_outputs_cleanup_is_descriptor_bound_and_bounded() {
        let temp = tempdir().unwrap();
        let state_path = temp.path().join("state");
        let checks_path = state_path.join("code-change-checks");
        let run_path = checks_path.join("run-1");
        let attempt_path = run_path.join("attempt-1");
        let root_path = attempt_path.join("check-1");
        for path in [&state_path, &checks_path, &run_path, &attempt_path, &root_path] {
            fs::create_dir(path).unwrap();
            secure_test_directory(path);
        }
        let state_root = Arc::new(File::open(&state_path).unwrap());
        let checks = Arc::new(File::open(&checks_path).unwrap());
        let run = Arc::new(File::open(&run_path).unwrap());
        let attempt = Arc::new(File::open(&attempt_path).unwrap());
        let root = Arc::new(File::open(&root_path).unwrap());
        let outputs = OwnedCheckOutputs {
            state_root_identity: directory_identity(&state_root).unwrap(),
            checks_identity: directory_identity(&checks).unwrap(),
            run_identity: directory_identity(&run).unwrap(),
            attempt_identity: directory_identity(&attempt).unwrap(),
            root_identity: directory_identity(&root).unwrap(),
            state_root,
            checks,
            checks_name: OsString::from("code-change-checks"),
            run,
            run_name: OsString::from("run-1"),
            attempt,
            attempt_name: OsString::from("attempt-1"),
            root,
            root_name: OsString::from("check-1"),
            path: root_path.clone(),
            unproven: AtomicBool::new(false),
        };
        fs::create_dir(root_path.join("nested")).unwrap();
        secure_test_directory(&root_path.join("nested"));
        fs::write(root_path.join("nested/output"), b"owned output").unwrap();
        secure_test_file(&root_path.join("nested/output"));
        assert!(outputs.cleanup().is_ok());
        assert!(!root_path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn owned_check_outputs_cleanup_unlinks_links_without_following_targets() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let state_path = temp.path().join("state");
        let checks_path = state_path.join("code-change-checks");
        let run_path = checks_path.join("run-1");
        let attempt_path = run_path.join("attempt-1");
        let root_path = attempt_path.join("check-1");
        for path in [&state_path, &checks_path, &run_path, &attempt_path, &root_path] {
            fs::create_dir(path).unwrap();
            secure_test_directory(path);
        }
        let state_root = Arc::new(File::open(&state_path).unwrap());
        let checks = Arc::new(File::open(&checks_path).unwrap());
        let run = Arc::new(File::open(&run_path).unwrap());
        let attempt = Arc::new(File::open(&attempt_path).unwrap());
        let root = Arc::new(File::open(&root_path).unwrap());
        let outputs = OwnedCheckOutputs {
            state_root_identity: directory_identity(&state_root).unwrap(),
            checks_identity: directory_identity(&checks).unwrap(),
            run_identity: directory_identity(&run).unwrap(),
            attempt_identity: directory_identity(&attempt).unwrap(),
            root_identity: directory_identity(&root).unwrap(),
            state_root,
            checks,
            checks_name: OsString::from("code-change-checks"),
            run,
            run_name: OsString::from("run-1"),
            attempt,
            attempt_name: OsString::from("attempt-1"),
            root,
            root_name: OsString::from("check-1"),
            path: root_path.clone(),
            unproven: AtomicBool::new(false),
        };
        let outside = temp.path().join("outside");
        fs::write(&outside, b"must remain").unwrap();
        symlink(&outside, root_path.join("escape")).unwrap();
        let hardlink_source = temp.path().join("hardlink-source");
        fs::write(&hardlink_source, b"must remain too").unwrap();
        fs::hard_link(&hardlink_source, root_path.join("cache-entry")).unwrap();
        assert!(outputs.cleanup().is_ok());
        assert!(!root_path.exists());
        assert_eq!(fs::read(&outside).unwrap(), b"must remain");
        assert_eq!(fs::read(&hardlink_source).unwrap(), b"must remain too");
    }

    #[cfg(unix)]
    #[test]
    fn check_output_scope_creation_rejects_existing_root() {
        let temp = tempdir().unwrap();
        let parent_path = temp.path().join("attempt");
        fs::create_dir(&parent_path).unwrap();
        secure_test_directory(&parent_path);
        let root_path = parent_path.join("check-1");
        fs::create_dir(&root_path).unwrap();
        secure_test_directory(&root_path);
        let parent = File::open(&parent_path).unwrap();
        assert!(create_new_directory_at(&parent, OsStr::new("check-1")).is_err());
        assert!(root_path.is_dir());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn runtime_output_scope_allows_profile_sized_output_with_explicit_cap() {
        let temp = tempdir().unwrap();
        let runtime_path = temp.path().join("runtime");
        let experiment_path = runtime_path.join("experiment-1");
        fs::create_dir(&runtime_path).unwrap();
        fs::create_dir(&experiment_path).unwrap();
        secure_test_directory(&runtime_path);
        secure_test_directory(&experiment_path);
        for name in RUNTIME_OUTPUT_DIRECTORY_NAMES {
            let output = experiment_path.join(name);
            fs::create_dir(&output).unwrap();
            secure_test_directory(&output);
        }
        let cargo_target = experiment_path.join("cargo-target").join("profile-marker");
        let file = fs::File::create(cargo_target).unwrap();
        file.set_len(2 * 1024 * 1024 * 1024).unwrap();
        let runtime = File::open(&runtime_path).unwrap();
        let experiment = File::open(&experiment_path).unwrap();
        let runtime_identity = directory_identity(&runtime).unwrap();
        let experiment_identity = directory_identity(&experiment).unwrap();

        assert!(verify_runtime_output_scope(
            &runtime,
            runtime_identity,
            "experiment-1",
            Some(experiment_identity),
            false,
        )
        .is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn live_runtime_boundary_allows_transient_leaf_recreation_but_terminal_audit_does_not() {
        let temp = tempdir().unwrap();
        let runtime_path = temp.path().join("runtime");
        let experiment_path = runtime_path.join("experiment-1");
        fs::create_dir(&runtime_path).unwrap();
        fs::create_dir(&experiment_path).unwrap();
        secure_test_directory(&runtime_path);
        secure_test_directory(&experiment_path);
        for name in RUNTIME_OUTPUT_DIRECTORY_NAMES {
            let output = experiment_path.join(name);
            fs::create_dir(&output).unwrap();
            secure_test_directory(&output);
        }
        let runtime = File::open(&runtime_path).unwrap();
        let experiment = File::open(&experiment_path).unwrap();
        let runtime_identity = directory_identity(&runtime).unwrap();
        let experiment_identity = directory_identity(&experiment).unwrap();
        fs::remove_dir(experiment_path.join("uv-venv")).unwrap();

        assert!(verify_runtime_output_boundary(
            &runtime,
            runtime_identity,
            &experiment,
            experiment_identity,
            "experiment-1",
        )
        .is_ok());
        assert!(verify_runtime_output_scope(
            &runtime,
            runtime_identity,
            "experiment-1",
            Some(experiment_identity),
            false,
        )
        .is_err());
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
    fn discovery_with_uv_lock_does_not_fallback_to_python() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join("pyproject.toml"),
            "[tool.pytest.ini_options]\naddopts='-q'\n",
        )
        .unwrap();
        fs::write(root.path().join("uv.lock"), "version = 1\n").unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert!(checks.is_empty());
    }

    #[test]
    fn discovery_python_profile_requires_pytest_configuration() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join("pyproject.toml"),
            "[tool.pytest.ini_options]\naddopts='-q'\n",
        )
        .unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].argv,
            PYTHON_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn discovery_ignores_pyproject_without_pytest_configuration() {
        let root = tempdir().unwrap();
        fs::write(root.path().join("pyproject.toml"), "[tool.black]\nline-length=88\n").unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert!(checks.is_empty());
    }

    #[test]
    fn discovery_ignores_nonregular_pyproject() {
        let root = tempdir().unwrap();
        fs::create_dir(root.path().join("pyproject.toml")).unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert!(checks.is_empty());
    }

    #[test]
    fn discovery_ignores_malformed_and_oversized_pyproject() {
        let cases = [
            "[tool.pytest.ini_options\naddopts='-q'\n".as_bytes().to_vec(),
            vec![b'x'; MAX_PYPROJECT_BYTES + 1],
        ];
        for contents in cases {
            let root = tempdir().unwrap();
            fs::write(root.path().join("pyproject.toml"), contents).unwrap();

            let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
                .unwrap();
            assert!(checks.is_empty());
        }
    }

    #[test]
    fn discovery_uses_fixed_pytest_argv_without_executing_pyproject_values() {
        let root = tempdir().unwrap();
        let marker = root.path().join("not-created");
        fs::write(
            root.path().join("pyproject.toml"),
            format!(
                "[tool.pytest.ini_options]\naddopts='$(touch {})'\nmarkers=['gpu: use & accelerator; safe']\n",
                marker.display()
            ),
        )
        .unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].argv,
            PYTHON_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
        assert!(!marker.exists());
    }

    #[test]
    fn discovery_accepts_dependency_comparators_and_pytest_markers() {
        let root = tempdir().unwrap();
        fs::write(
            root.path().join("pyproject.toml"),
            "[project]\ndependencies = [\"torch>=2.0; python_version >= '3.10'\"]\n\n[tool.pytest.ini_options]\nmarkers = [\"gpu: accelerator > cpu; requires CUDA\"]\n",
        )
        .unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(
            checks[0].argv,
            PYTHON_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[cfg(unix)]
    #[test]
    fn discovery_does_not_follow_pyproject_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempdir().unwrap();
        let target = root.path().join("pytest.toml");
        fs::write(&target, "[tool.pytest.ini_options]\naddopts='-q'\n").unwrap();
        symlink(&target, root.path().join("pyproject.toml")).unwrap();

        let checks = discover_project_checks(root.path(), &tools(&[CodeChangeTool::Python]))
            .unwrap();
        assert!(checks.is_empty());
    }

    #[test]
    fn code_change_project_checks_deduplicate_in_stable_order() {
        let cargo = ProposedCheck {
            source: "cargo".to_owned(),
            argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
            working_directory: ".".to_owned(),
        };
        let python = ProposedCheck {
            source: "python".to_owned(),
            argv: PYTHON_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect(),
            working_directory: ".".to_owned(),
        };
        let discovered = vec![cargo.clone()];
        let editor = vec![cargo.clone(), python.clone()];
        let limits = CampaignLimits::default();

        assert_eq!(
            merge_project_checks(
                &discovered,
                &editor,
                limits.max_code_change_checks as usize,
            )
            .unwrap(),
            vec![cargo.clone(), python.clone()]
        );

        let limited = CampaignLimits {
            max_code_change_checks: 1,
            ..limits
        };
        assert!(
            merge_project_checks(
                &discovered,
                &editor,
                limited.max_code_change_checks as usize,
            )
            .is_err()
        );
    }

    #[test]
    fn code_change_planned_checks_are_durable_and_source_ordered() {
        let base_sha = "a".repeat(40);
        let cargo = ProposedCheck {
            source: "cargo".to_owned(),
            argv: RUST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
            working_directory: ".".to_owned(),
        };
        let python = ProposedCheck {
            source: "python".to_owned(),
            argv: PYTHON_PYTEST_CHECK
                .iter()
                .map(|arg| (*arg).to_owned())
                .collect(),
            working_directory: "tests".to_owned(),
        };
        let uv = ProposedCheck {
            source: "uv".to_owned(),
            argv: UV_PYTEST_CHECK.iter().map(|arg| (*arg).to_owned()).collect(),
            working_directory: ".".to_owned(),
        };
        let discovered = vec![cargo.clone()];
        let editor = vec![cargo.clone(), python.clone()];

        let rows = planned_check_rows(2, &base_sha, &discovered, &editor, 2).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].source, "supervisor");
        assert_eq!(rows[0].working_directory, ".");
        assert_eq!(
            rows[0].argv,
            pinned_git_argv(&supervisor_diff_check_args(&base_sha))
                .into_iter()
                .map(|argument| argument.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
        );
        assert_eq!(rows[1].source, "discovered");
        assert_eq!(rows[1].argv, cargo.argv);
        assert_eq!(rows[1].working_directory, cargo.working_directory);
        assert_eq!(rows[2].source, "editor");
        assert_eq!(rows[2].argv, python.argv);
        assert_eq!(rows[2].working_directory, python.working_directory);
        assert!(rows.iter().all(|row| {
            row.status == crate::models::CodeChangeCheckStatus::Reserved
        }));

        let editor_with_three_projects = vec![cargo, python, uv];
        assert!(
            planned_check_rows(2, &base_sha, &discovered, &editor_with_three_projects, 2).is_err()
        );
    }

    #[test]
    fn code_change_persisted_check_plan_requires_exact_ordered_prefix() {
        let planned = vec![
            NewCodeChangeCheck::new(
                1,
                0,
                "supervisor",
                vec!["git".to_owned(), "diff".to_owned()],
                ".",
            ),
            NewCodeChangeCheck::new(
                1,
                1,
                "discovered",
                vec!["cargo".to_owned(), "test".to_owned()],
                ".",
            ),
            NewCodeChangeCheck::new(
                1,
                2,
                "editor",
                vec!["cargo".to_owned(), "check".to_owned()],
                "src",
            ),
        ];
        let row = |planned: &NewCodeChangeCheck, status: CodeChangeCheckStatus| {
            let finished = !matches!(status, CodeChangeCheckStatus::Reserved);
            CodeChangeCheck {
                code_change_run_id: "run-1".to_owned(),
                attempt: planned.attempt,
                ordinal: planned.ordinal,
                source: planned.source.clone(),
                argv: planned.argv.clone(),
                working_directory: planned.working_directory.clone(),
                status,
                output_digest: finished.then(|| format!("digest-{}", planned.ordinal)),
                summary: None,
                started_at: finished.then_some(10),
                finished_at: finished.then_some(11),
            }
        };

        let exact_reserved = vec![
            row(&planned[0], CodeChangeCheckStatus::Reserved),
            row(&planned[1], CodeChangeCheckStatus::Reserved),
        ];
        assert!(validate_persisted_check_plan(&exact_reserved, &planned[..2]).is_ok());

        let passed = row(&planned[0], CodeChangeCheckStatus::Passed);
        assert!(validate_persisted_check_plan(
            &[passed.clone(), row(&planned[1], CodeChangeCheckStatus::Reserved)],
            &planned[..2],
        )
        .is_ok());
        assert!(validate_persisted_check_plan(
            &[
                passed.clone(),
                row(&planned[1], CodeChangeCheckStatus::Failed),
                row(&planned[2], CodeChangeCheckStatus::Reserved),
            ],
            &planned,
        )
        .is_ok());

        let mut wrong_source = exact_reserved.clone();
        wrong_source[1].source = "editor".to_owned();
        assert!(validate_persisted_check_plan(&wrong_source, &planned[..2]).is_err());
        assert!(validate_persisted_check_plan(&exact_reserved, &planned).is_err());

        assert!(validate_persisted_check_plan(
            &[
                row(&planned[0], CodeChangeCheckStatus::Reserved),
                row(&planned[1], CodeChangeCheckStatus::Passed),
            ],
            &planned[..2],
        )
        .is_err());
        assert!(validate_persisted_check_plan(
            &[
                row(&planned[0], CodeChangeCheckStatus::Failed),
                row(&planned[1], CodeChangeCheckStatus::Passed),
            ],
            &planned[..2],
        )
        .is_err());
        let mut passed_without_digest = passed;
        passed_without_digest.output_digest = None;
        assert!(validate_persisted_check_plan(
            &[
                passed_without_digest,
                row(&planned[1], CodeChangeCheckStatus::Reserved),
            ],
            &planned[..2],
        )
        .is_err());
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
    fn terminal_result_cache_allowlist_rejects_lookalikes_and_protected_contents() {
        for path in [
            ".pytest_cache.bak/results",
            "nested/__pycache__x/module.pyc",
            ".mypy_cache/results",
            "build/test-output",
        ] {
            assert!(
                !is_python_check_cache_path(Path::new(path)),
                "accepted cache lookalike {path}"
            );
        }
        for path in [".pytest_cache/results", "nested/__pycache__/module.pyc"] {
            assert!(is_python_check_cache_path(Path::new(path)));
        }

        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let cache = root.path().join(".pytest_cache");
        fs::create_dir(&cache).unwrap();
        secure_test_directory(&cache);
        let secret = cache.join(".env");
        fs::write(&secret, b"secret").unwrap();
        secure_test_file(&secret);
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from(".pytest_cache/")]).is_err());
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
        secure_test_file(&lock_path);
        *index.lock_identity.lock().unwrap() = temporary_entry_identity(
            &index.directory,
            OsStr::new("index.lock"),
        )
        .unwrap();
        let replacement = root.path().join("replacement-index");
        fs::write(&replacement, b"DIRC").unwrap();
        secure_test_file(&replacement);
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
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
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
        secure_test_file(&replacement);
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
        secure_test_file(&replacement);
        fs::rename(replacement, &path).unwrap();
        assert!(index.record_after_command().is_err());
        assert!(path.exists(), "an untrusted replacement must be retained");
    }

    #[cfg(unix)]
    #[test]
    fn ignored_status_walk_rejects_recursive_protected_descendants() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        fs::create_dir(root.path().join("build")).unwrap();
        secure_test_directory(&root.path().join("build"));
        fs::write(root.path().join("build/.env"), b"secret").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        fs::remove_file(root.path().join("build/.env")).unwrap();
        fs::write(root.path().join("build/.ENV"), b"secret").unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        fs::remove_file(root.path().join("build/.ENV")).unwrap();
        fs::create_dir(root.path().join("build/nested")).unwrap();
        secure_test_directory(&root.path().join("build/nested"));
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
        secure_test_directory(root.path());
        let outside = tempdir().unwrap();
        fs::write(outside.path().join("credentials.json"), b"secret").unwrap();
        fs::create_dir(root.path().join("build")).unwrap();
        secure_test_directory(&root.path().join("build"));
        std::os::unix::fs::symlink(outside.path(), root.path().join("build/outside")).unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn ignored_status_walk_fails_closed_on_recursive_bounds() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let build = root.path().join("build");
        fs::create_dir(&build).unwrap();
        secure_test_directory(&build);
        for index in 0..=MAX_IGNORED_SCAN_ENTRIES {
            fs::write(build.join(format!("artifact-{index}")), b"x").unwrap();
        }
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("build/")]).is_err());

        let bytes = root.path().join("bytes");
        fs::create_dir(&bytes).unwrap();
        secure_test_directory(&bytes);
        fs::write(
            bytes.join("large"),
            vec![b'x'; (MAX_IGNORED_SCAN_BYTES + 1) as usize],
        )
        .unwrap();
        assert!(validate_ignored_tree(root.path(), &[PathBuf::from("bytes/")]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn terminal_result_outputs_allow_only_bound_manifest_and_artifacts() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let service = root.path().join(".pueue-agent");
        let results = service.join("results");
        let artifacts = service.join("artifacts");
        let artifact_run = artifacts.join("experiment:run-1");
        fs::create_dir_all(&artifact_run).unwrap();
        fs::create_dir_all(&results).unwrap();
        secure_test_directory(&service);
        secure_test_directory(&results);
        secure_test_directory(&artifacts);
        secure_test_directory(&artifact_run);
        fs::write(results.join("experiment:run-1.json"), b"{}\n").unwrap();
        secure_test_file(&results.join("experiment:run-1.json"));
        fs::write(artifact_run.join("metrics.txt"), b"ok\n").unwrap();
        secure_test_file(&artifact_run.join("metrics.txt"));
        let root_directory = File::open(root.path()).unwrap();

        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_ok());

        let pytest_cache = root.path().join(".pytest_cache");
        let pycache = root.path().join("nested/__pycache__");
        fs::create_dir_all(&pytest_cache).unwrap();
        fs::create_dir_all(&pycache).unwrap();
        secure_test_directory(&pytest_cache);
        secure_test_directory(&pycache);
        fs::write(pytest_cache.join("CACHEDIR.TAG"), b"Signature: 8a477f597d28d172\n").unwrap();
        fs::write(pycache.join("module.cpython-311.pyc"), b"cache\n").unwrap();
        secure_test_file(&pytest_cache.join("CACHEDIR.TAG"));
        secure_test_file(&pycache.join("module.cpython-311.pyc"));
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_ok());

        fs::write(results.join("other-experiment.json"), b"{}\n").unwrap();
        secure_test_file(&results.join("other-experiment.json"));
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
        fs::remove_file(results.join("other-experiment.json")).unwrap();

        std::os::unix::fs::symlink(artifact_run.join("metrics.txt"), artifact_run.join("link"))
            .unwrap();
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
        fs::remove_file(artifact_run.join("link")).unwrap();

        fs::write(service.join("STATE.md"), b"must be rejected\n").unwrap();
        secure_test_file(&service.join("STATE.md"));
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn terminal_result_outputs_require_bound_nodes_and_exact_modes() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let service = root.path().join(".pueue-agent");
        let results = service.join("results");
        let artifacts = service.join("artifacts");
        let artifact_run = artifacts.join("experiment:run-1");
        let manifest = results.join("experiment:run-1.json");
        fs::create_dir_all(&artifact_run).unwrap();
        fs::create_dir_all(&results).unwrap();
        secure_test_directory(&service);
        secure_test_directory(&results);
        secure_test_directory(&artifacts);
        secure_test_directory(&artifact_run);
        fs::write(&manifest, b"{}\n").unwrap();
        secure_test_file(&manifest);
        let root_directory = File::open(root.path()).unwrap();

        for directory in [&service, &results, &artifacts, &artifact_run] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o750)).unwrap();
            assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
            secure_test_directory(directory);
        }
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
        secure_test_file(&manifest);

        fs::remove_file(&manifest).unwrap();
        assert!(validate_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());

        let missing_root = root.path().join("missing-service");
        fs::create_dir(&missing_root).unwrap();
        secure_test_directory(&missing_root);
        let missing_root_directory = File::open(&missing_root).unwrap();
        assert!(
            validate_terminal_result_outputs(&missing_root_directory, "experiment:run-1").is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn terminal_result_output_ingestion_classifies_missing_and_invalid_results() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let service = root.path().join(".pueue-agent");
        let results = service.join("results");
        let artifacts = service.join("artifacts");
        let artifact_run = artifacts.join("experiment:run-1");
        fs::create_dir_all(&artifact_run).unwrap();
        fs::create_dir_all(&results).unwrap();
        secure_test_directory(&service);
        secure_test_directory(&results);
        secure_test_directory(&artifacts);
        secure_test_directory(&artifact_run);
        let root_directory = File::open(root.path()).unwrap();

        assert_eq!(
            inspect_terminal_result_outputs(&root_directory, "experiment:run-1").unwrap(),
            TerminalResultOutputStatus::Missing
        );

        fs::remove_dir(&results).unwrap();
        fs::write(&results, b"not-a-directory").unwrap();
        secure_test_file(&results);
        assert_eq!(
            inspect_terminal_result_outputs(&root_directory, "experiment:run-1").unwrap(),
            TerminalResultOutputStatus::Invalid
        );

        fs::remove_file(&results).unwrap();
        assert_eq!(
            inspect_terminal_result_outputs(&root_directory, "experiment:run-1").unwrap(),
            TerminalResultOutputStatus::Invalid
        );
        let outside = tempdir().unwrap();
        secure_test_directory(outside.path());
        std::os::unix::fs::symlink(outside.path(), service.join("results")).unwrap();
        assert!(inspect_terminal_result_outputs(&root_directory, "experiment:run-1").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn bound_terminal_result_rejects_same_status_manifest_replacement() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let service = root.path().join(".pueue-agent");
        let results = service.join("results");
        let artifacts = service.join("artifacts");
        let artifact_run = artifacts.join("experiment:run-1");
        let manifest = results.join("experiment:run-1.json");
        fs::create_dir_all(&artifact_run).unwrap();
        fs::create_dir_all(&results).unwrap();
        secure_test_directory(&service);
        secure_test_directory(&results);
        secure_test_directory(&artifacts);
        secure_test_directory(&artifact_run);
        fs::write(
            &manifest,
            br#"{"schema_version":1,"experiment_id":"experiment:run-1","metrics":{"loss":0.1}}"#,
        )
        .unwrap();
        secure_test_file(&manifest);
        let root_directory = File::open(root.path()).unwrap();

        let binding = bind_terminal_result_outputs(&root_directory, "experiment:run-1").unwrap();
        assert_eq!(binding.status(), TerminalResultOutputStatus::Ready);
        assert!(std::str::from_utf8(binding.manifest_bytes().unwrap())
            .unwrap()
            .contains("0.1"));

        let replacement = results.join("replacement.json");
        fs::write(
            &replacement,
            br#"{"schema_version":1,"experiment_id":"experiment:run-1","metrics":{"loss":9.9}}"#,
        )
        .unwrap();
        secure_test_file(&replacement);
        fs::rename(&replacement, &manifest).unwrap();

        assert!(binding.reverify(&root_directory).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn bound_terminal_result_rejects_in_place_manifest_mutation() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let service = root.path().join(".pueue-agent");
        let results = service.join("results");
        let artifacts = service.join("artifacts");
        let artifact_run = artifacts.join("experiment:run-1");
        let manifest = results.join("experiment:run-1.json");
        fs::create_dir_all(&artifact_run).unwrap();
        fs::create_dir_all(&results).unwrap();
        secure_test_directory(&service);
        secure_test_directory(&results);
        secure_test_directory(&artifacts);
        secure_test_directory(&artifact_run);
        fs::write(
            &manifest,
            br#"{"schema_version":1,"experiment_id":"experiment:run-1","metrics":{"loss":0.1}}"#,
        )
        .unwrap();
        secure_test_file(&manifest);
        let root_directory = File::open(root.path()).unwrap();
        let binding = bind_terminal_result_outputs(&root_directory, "experiment:run-1").unwrap();

        fs::write(
            &manifest,
            br#"{"schema_version":1,"experiment_id":"experiment:run-1","metrics":{"loss":9.9}}"#,
        )
        .unwrap();
        secure_test_file(&manifest);

        assert!(binding.reverify(&root_directory).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn runtime_manifest_reverify_uses_read_only_nonblocking_descriptor() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let manifest = root.path().join("experiment:run-1.json");
        fs::write(&manifest, b"{}\n").unwrap();
        secure_test_file(&manifest);
        let parent = File::open(root.path()).unwrap();
        let (file, _) = open_runtime_manifest_at(&parent, OsStr::new("experiment:run-1.json"))
            .unwrap();
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        assert!(flags >= 0, "fcntl(F_GETFL) failed");
        assert_eq!(flags & libc::O_ACCMODE, libc::O_RDONLY);
        assert_ne!(flags & libc::O_NONBLOCK, 0);
    }

    #[cfg(unix)]
    #[test]
    fn runtime_manifest_fifo_replacement_is_rejected_within_bound() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let manifest = root.path().join("experiment:run-1.json");
        fs::write(&manifest, b"{}\n").unwrap();
        secure_test_file(&manifest);
        fs::remove_file(&manifest).unwrap();
        let name = std::ffi::CString::new(manifest.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let parent = File::open(root.path()).unwrap();
        let started = Instant::now();
        let result = open_runtime_manifest_at(&parent, OsStr::new("experiment:run-1.json"));
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn protected_ref_digest_excludes_only_controlled_candidate_and_best() {
        let initial = b"refs/heads/campaign/a/candidate/p\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\nrefs/heads/campaign/a/best\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0\nrefs/heads/main\0cccccccccccccccccccccccccccccccccccccccc\0\n";
        let controlled_changed = b"refs/heads/campaign/a/candidate/p\0dddddddddddddddddddddddddddddddddddddddd\0\nrefs/heads/campaign/a/best\0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\0\nrefs/heads/main\0cccccccccccccccccccccccccccccccccccccccc\0\n";
        let unrelated_changed = b"refs/heads/campaign/a/candidate/p\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\nrefs/heads/campaign/a/best\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0\nrefs/heads/main\0ffffffffffffffffffffffffffffffffffffffff\0\n";
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

    #[test]
    fn protected_ref_digest_requires_git_for_each_ref_record_framing() {
        let records = b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\nrefs/heads/dev\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0\n";
        let reordered = b"refs/heads/dev\0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\0\nrefs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\n";
        assert_eq!(
            digest_protected_refs(records, &[]).unwrap(),
            digest_protected_refs(reordered, &[]).unwrap()
        );
        for malformed in [
            b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0".as_slice(),
            b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n".as_slice(),
            b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\n\n".as_slice(),
            b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\nextra".as_slice(),
            b"refs/heads/main\0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\0\nrefs/heads/dev\0".as_slice(),
        ] {
            assert!(digest_protected_refs(malformed, &[]).is_err(), "{malformed:?}");
        }
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
    fn descriptor_backed_git_common_directory_output_uses_retained_proof() {
        let root = tempdir().unwrap();
        let common_path = root.path().join("common");
        fs::create_dir(&common_path).unwrap();
        fs::set_permissions(&common_path, fs::Permissions::from_mode(0o700)).unwrap();
        let directory = File::open(&common_path).unwrap();
        let proof = GitDirectoryProof {
            path: common_path.clone(),
            identity: executable_identity_from_metadata(&directory.metadata().unwrap()),
            directory: Arc::new(directory),
        };
        let descriptor = git_descriptor_path(GIT_COMMON_DIR_FD);
        assert_eq!(
            resolve_git_common_directory_output(&descriptor, root.path(), Some(&proof)).unwrap(),
            common_path
        );
        assert!(resolve_git_common_directory_output(
            "/proc/self/fd/999",
            root.path(),
            Some(&proof),
        )
        .is_err());
        assert!(resolve_git_common_directory_output(&descriptor, root.path(), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn worktree_administration_uses_only_a_single_relative_leaf() {
        assert_eq!(descriptor_worktree_leaf("proposal"), OsString::from("proposal"));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn cleanup_leaf_swap_is_recovery_and_preserves_replacement() {
        let root = tempdir().unwrap();
        secure_test_directory(root.path());
        let candidate = root.path().join("proposal");
        fs::create_dir(&candidate).unwrap();
        secure_test_directory(&candidate);
        let candidate_directory = File::open(&candidate).unwrap();
        let candidate_identity = directory_identity(&candidate_directory).unwrap();
        let replacement = root.path().join("replacement");
        fs::create_dir(&replacement).unwrap();
        secure_test_directory(&replacement);
        fs::write(replacement.join("marker"), b"replacement").unwrap();
        let parent = File::open(root.path()).unwrap();

        fs::rename(&candidate, root.path().join("proposal-old")).unwrap();
        fs::rename(&replacement, &candidate).unwrap();

        assert!(verify_cleanup_leaf_identity(
            &parent,
            OsStr::new("proposal"),
            candidate_identity,
        )
        .is_err());
        assert!(verify_cleanup_leaf_absent(&parent, OsStr::new("proposal")).is_err());
        assert_eq!(fs::read(candidate.join("marker")).unwrap(), b"replacement");
        assert_eq!(
            git_descriptor_path(GIT_WORKTREE_PARENT_FD),
            "/proc/self/fd/15"
        );
    }

    #[test]
    fn cleanup_authorization_requires_a_fresh_durable_run_row() {
        let root = tempdir().unwrap();
        let db = crate::db::Db::open(&root.path().join("agent.sqlite")).unwrap();
        assert!(CodeChangeCleanupAuthorization::load(&db, "missing-run").is_err());
        assert!(list_recoverable_code_change_runs(&db, 10).unwrap().is_empty());
    }

    #[test]
    fn recovery_required_cleanup_state_is_rejected_before_mutation() {
        let root = tempdir().unwrap();
        let marker = root.path().join("marker");
        fs::write(&marker, b"untouched").unwrap();
        assert!(
            validate_cleanup_state_for_mutation(CodeChangeState::RecoveryRequired, None).is_err()
        );
        assert_eq!(fs::read(&marker).unwrap(), b"untouched");
    }

    #[test]
    fn cleanup_target_disappearance_after_observation_requires_recovery() {
        assert!(validate_disappeared_cleanup_target(true).is_err());
    }

    #[test]
    fn cleanup_retained_candidate_ref_requires_exact_committed_sha() {
        let candidate_sha = "a".repeat(40);
        let wrong_sha = "b".repeat(40);
        assert!(validate_retained_candidate_ref(
            Some(&candidate_sha),
            Some(&candidate_sha),
        )
        .is_ok());
        assert!(validate_retained_candidate_ref(None, Some(&candidate_sha)).is_err());
        assert!(validate_retained_candidate_ref(Some(&wrong_sha), Some(&candidate_sha)).is_err());
    }

    #[test]
    fn cleanup_preparation_rollback_requires_absent_candidate_ref() {
        let candidate_sha = "a".repeat(40);
        assert!(validate_retained_candidate_ref(None, None).is_ok());
        assert!(validate_retained_candidate_ref(Some(&candidate_sha), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_liveness_probe_rejects_live_and_invalid_process_ids() {
        assert!(process_is_alive(std::process::id() as i64));
        assert!(process_is_alive(0));
        assert!(process_is_alive(-1));
    }

    #[test]
    fn cleanup_mutation_authorization_matches_durable_finalizer_matrix() {
        let cases = [
            (CodeChangeState::Reserved, None, false),
            (CodeChangeState::PreparingWorktree, None, false),
            (CodeChangeState::Editing, None, false),
            (CodeChangeState::Checking, None, false),
            (CodeChangeState::Committing, None, false),
            (CodeChangeState::CandidateReady, None, true),
            (CodeChangeState::CandidateReady, Some(123), false),
            (CodeChangeState::ExperimentSubmitted, None, false),
            (CodeChangeState::Evaluated, None, false),
            (CodeChangeState::CleanupPending, None, true),
            (CodeChangeState::Completed, None, false),
            (CodeChangeState::Completed, Some(123), false),
            (CodeChangeState::Rejected, None, true),
            (CodeChangeState::Rejected, Some(123), false),
            (CodeChangeState::RecoveryRequired, None, false),
        ];
        for (state, cleanup_completed_at, expected) in cases {
            assert_eq!(
                validate_cleanup_state_for_mutation(state, cleanup_completed_at).is_ok(),
                expected,
                "unexpected cleanup authorization for {state:?} with marker {cleanup_completed_at:?}",
            );
        }
    }

    #[test]
    fn code_change_check_round_requires_project_check() {
        let supervisor_only = CheckRoundResult {
            git_diff_passed: true,
            project_check_count: 0,
            all_project_checks_passed: true,
            final_diff_matches: true,
        };
        assert!(!supervisor_only.passed());

        let project_failure = CheckRoundResult {
            git_diff_passed: true,
            project_check_count: 1,
            all_project_checks_passed: false,
            final_diff_matches: true,
        };
        assert!(!project_failure.passed());
    }

    #[test]
    fn code_change_check_round_requires_stable_final_diff() {
        let final_diff_mismatch = CheckRoundResult {
            git_diff_passed: true,
            project_check_count: 1,
            all_project_checks_passed: true,
            final_diff_matches: false,
        };
        assert!(!final_diff_mismatch.passed());

        let accepted = CheckRoundResult {
            git_diff_passed: true,
            project_check_count: 1,
            all_project_checks_passed: true,
            final_diff_matches: true,
        };
        assert!(accepted.passed());
    }

    #[test]
    fn code_change_supervisor_diff_check_is_cached_and_base_bound() {
        let base_sha = "a".repeat(40);

        assert_eq!(
            supervisor_diff_check_args(&base_sha),
            vec![
                OsString::from("diff"),
                OsString::from("--cached"),
                OsString::from("--check"),
                OsString::from(base_sha),
            ]
        );
    }
}
