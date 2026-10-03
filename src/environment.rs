//! Explicit, default-deny child environments and private per-run temporary
//! directories.
//!
//! Environment values are kept as `OsString`s until the final process API
//! call.  Debug output intentionally contains names only.  Private run
//! directories are retained at their original unique paths: portable Unix
//! APIs do not provide an atomic conditional unlink/rename by inode, so a
//! cleanup pathname could otherwise mutate a replacement generation.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    ffi::{OsStr, OsString},
    fmt,
    fs::File,
    io,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::execution_policy::{
    ExecutableIdentity, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
    PolicyViolationDetail, ProjectRootAnchor, ResolvedExecutionPolicy,
    ResolvedProjectExecutionPolicy, TempUnsafeReason, VerifiedProjectRoot,
};
#[cfg(target_os = "linux")]
use std::sync::Mutex;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::io::Write as _;
#[cfg(target_os = "linux")]
use crate::decision_protocol::MAX_DECISION_BYTES;
#[cfg(unix)]
use crate::project_logs::LogFileIdentity;

pub use crate::execution_policy::StartupEnvironment;

const PRIVATE_TEMP_ROOT: &str = ".pueue-agent";
const PRIVATE_TEMP_DIR: &str = "tmp";
const RESULTS_DIRECTORY: &str = "results";
const ARTIFACTS_DIRECTORY: &str = "artifacts";
const MAX_RUN_ID_BYTES: usize = 20;

/// The project-relative service directory owned by this supervisor.
pub(crate) fn private_service_root() -> &'static str {
    PRIVATE_TEMP_ROOT
}

/// Names and values injected onto agent tasks that manage one campaign
/// experiment. Values are identifiers and derived paths only; the result and
/// artifact directories are created by the task itself, never pre-created.
pub fn campaign_experiment_task_environment(
    project_root: &Path,
    campaign_id: &str,
    experiment_id: &str,
) -> Vec<(String, OsString)> {
    let service_root = project_root.join(PRIVATE_TEMP_ROOT);
    vec![
        (
            "PUEUE_AGENT_EXPERIMENT_ID".to_owned(),
            OsString::from(experiment_id),
        ),
        (
            "PUEUE_AGENT_CAMPAIGN_ID".to_owned(),
            OsString::from(campaign_id),
        ),
        (
            "PUEUE_AGENT_RESULT_PATH".to_owned(),
            service_root
                .join(RESULTS_DIRECTORY)
                .join(format!("{experiment_id}.json"))
                .into_os_string(),
        ),
        (
            "PUEUE_AGENT_ARTIFACT_DIR".to_owned(),
            service_root
                .join(ARTIFACTS_DIRECTORY)
                .join(experiment_id)
                .into_os_string(),
        ),
    ]
}

/// Derive the fixed-layout paths for one private trial without touching the
/// filesystem.  Callers that need to validate the fully composed command can
/// do so before creating the trial generation.
pub(crate) fn private_trial_output_paths(
    project_root: &Path,
    trial_id: Uuid,
) -> (PathBuf, PathBuf) {
    let generation = project_root
        .join(PRIVATE_TEMP_ROOT)
        .join("trials")
        .join(OsString::from(trial_id.to_string()));
    (generation.join("result.json"), generation.join("artifacts"))
}

pub fn campaign_experiment_runtime_argv(
    project_root: &Path,
    campaign_id: &str,
    experiment_id: &str,
    user_argv: &[String],
) -> Vec<OsString> {
    let service_root = project_root.join(PRIVATE_TEMP_ROOT);
    experiment_runtime_argv_with_outputs(
        campaign_id,
        experiment_id,
        &service_root
            .join(RESULTS_DIRECTORY)
            .join(format!("{experiment_id}.json")),
        &service_root.join(ARTIFACTS_DIRECTORY).join(experiment_id),
        user_argv,
    )
}

pub(crate) fn experiment_runtime_argv_with_outputs(
    campaign_id: &str,
    experiment_id: &str,
    result_path: &Path,
    artifact_dir: &Path,
    user_argv: &[String],
) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(1 + 4 + user_argv.len());
    argv.push(OsString::from("/usr/bin/env"));
    let assignments = [
        ("PUEUE_AGENT_EXPERIMENT_ID", OsString::from(experiment_id)),
        ("PUEUE_AGENT_CAMPAIGN_ID", OsString::from(campaign_id)),
        (
            "PUEUE_AGENT_RESULT_PATH",
            result_path.as_os_str().to_os_string(),
        ),
        (
            "PUEUE_AGENT_ARTIFACT_DIR",
            artifact_dir.as_os_str().to_os_string(),
        ),
    ];
    for (name, value) in assignments {
        let mut assignment = OsString::from(name);
        assignment.push("=");
        assignment.push(value);
        argv.push(assignment);
    }
    argv.extend(user_argv.iter().map(OsString::from));
    argv
}

/// Descriptor-bound output capability for one non-campaign trial.
///
/// The generation is created before a child is launched; its result file and
/// artifact directory are deliberately left for that child to create.  All
/// later reads and cleanup use the retained descriptor chain and revalidate
/// the named chain before returning.
pub struct PrivateTrialOutput {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    root_anchor: ProjectRootAnchor,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    root: File,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    service: File,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    trials: File,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    generation: File,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    result_descriptor: RefCell<Option<RetainedTrialResult>>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    mount: MountIdentity,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    root_record: ResearchDirectoryRecord,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    service_record: ResearchDirectoryRecord,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    trials_record: ResearchDirectoryRecord,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    generation_record: ResearchDirectoryRecord,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    generation_name: OsString,
    result_path: PathBuf,
    artifact_dir: PathBuf,
    cleaned: bool,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct TrialOutputChain {
    trials: File,
    generation: Option<File>,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct RetainedTrialResult {
    file: File,
    snapshot: ResearchLeafSnapshot,
    digest: Option<String>,
}

impl PrivateTrialOutput {
    pub fn create(root: &VerifiedProjectRoot, trial_id: Uuid) -> Result<Self, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (root, trial_id);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let stage = PolicyViolationStage::PreBinding;
            let verified_root = root.anchor.verify_identity()?;
            let (result_path, artifact_dir) =
                private_trial_output_paths(&verified_root.anchor.canonical_path, trial_id);
            let root_mount = directory_mount_identity_at(&verified_root.directory, stage)?;
            let root_record = research_directory_record_at(&verified_root.directory, root_mount, stage)?;
            let service = open_or_create_private_temp_container(
                &verified_root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
            )
            .map_err(|violation| stage_violation(violation, stage))?;
            let service_record = research_directory_record_at(&service, root_mount, stage)?;
            let trials = open_or_create_retention_directory(
                &service,
                OsStr::new("trials"),
                root_mount,
                true,
                stage,
            )?;
            let trials_record = research_directory_record_at(&trials, root_mount, stage)?;
            let generation_name = OsString::from(trial_id.to_string());
            if open_optional_directory_on_mount(&trials, &generation_name, root_mount, stage)?.is_some() {
                return Err(temp_violation_at(TempUnsafeReason::ExistingEntry, stage));
            }
            mkdirat_private(&trials, &generation_name)?;
            let generation = open_directory_on_mount(&trials, &generation_name, root_mount, stage)?;
            validate_private_directory_at(&generation, stage)?;
            let generation_record = research_directory_record_at(&generation, root_mount, stage)?;
            trials.sync_all().map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
            let root_anchor = verified_root.anchor.clone();
            let output = Self {
                root_anchor,
                root: verified_root.directory,
                service,
                trials,
                generation,
                result_descriptor: RefCell::new(None),
                mount: root_mount,
                root_record,
                service_record,
                trials_record,
                generation_record,
                generation_name,
                result_path,
                artifact_dir,
                cleaned: false,
            };
            output.revalidate_chain(true)?;
            Ok(output)
        }
    }

    pub fn result_path(&self) -> &Path {
        &self.result_path
    }

    pub fn artifact_dir(&self) -> &Path {
        &self.artifact_dir
    }

    pub fn read_result_bounded(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_result_bounded_after_read(|| {})
    }

    fn read_result_bounded_after_read<F: FnOnce()>(
        &self,
        after_read: F,
    ) -> Result<Vec<u8>, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = after_read;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let chain = self.revalidate_chain(true)?;
            let retained_result =
                self.retained_result_for_generation(chain.generation.as_ref().unwrap())?;
            let result = match retained_result.as_ref() {
                Some((file, _, _)) => file.try_clone().map_err(|_| {
                    temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
                })?,
                None => open_research_file_on_mount(
                    chain.generation.as_ref().unwrap(),
                    OsStr::new("result.json"),
                    self.mount,
                    PolicyViolationStage::Finalized,
                )?,
            };
            let max_bytes = crate::result_manifest::MAX_RESULT_MANIFEST_BYTES as u64;
            let before = research_leaf_snapshot(
                &result,
                self.mount,
                u64::MAX,
                PolicyViolationStage::Finalized,
            )?;
            if retained_result
                .as_ref()
                .is_some_and(|(_, snapshot, _)| *snapshot != before)
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            if retained_result.is_none() {
                let retained_file = result.try_clone().map_err(|_| {
                    temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
                })?;
                *self.result_descriptor.borrow_mut() = Some(RetainedTrialResult {
                    file: retained_file,
                    snapshot: before,
                    digest: None,
                });
            }
            if before.links != 1 {
                return Err(temp_violation_at(
                    TempUnsafeReason::InvalidEntry,
                    PolicyViolationStage::Finalized,
                ));
            }
            if before.logical_bytes > crate::result_manifest::MAX_RESULT_MANIFEST_BYTES as u64 {
                return Err(temp_violation_at(
                    TempUnsafeReason::ByteLimit,
                    PolicyViolationStage::Finalized,
                ));
            }
            let before_digest =
                hash_research_file(&result, max_bytes, PolicyViolationStage::Finalized)?.1;
            if retained_result
                .as_ref()
                .is_some_and(|(_, snapshot, digest)| {
                    *snapshot != before
                        || digest
                            .as_ref()
                            .is_some_and(|expected| expected != &before_digest)
                })
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            if let Some(retained) = self.result_descriptor.borrow_mut().as_mut() {
                if retained.digest.is_none() {
                    retained.digest = Some(before_digest.clone());
                }
            }
            let size = usize::try_from(before.logical_bytes).map_err(|_| {
                temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
            })?;
            let mut bytes = vec![0_u8; size];
            read_research_bytes(&result, &mut bytes, PolicyViolationStage::Finalized)?;
            after_read();
            let after = research_leaf_snapshot(
                &result,
                self.mount,
                max_bytes + 1,
                PolicyViolationStage::Finalized,
            )?;
            let after_digest =
                hash_research_file(&result, max_bytes, PolicyViolationStage::Finalized)?.1;
            if before != after
                || before_digest != after_digest
                || format!("{:x}", Sha256::digest(&bytes)) != before_digest
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            let visible = open_research_file_on_mount(
                chain.generation.as_ref().unwrap(),
                OsStr::new("result.json"),
                self.mount,
                PolicyViolationStage::Finalized,
            )?;
            let visible_snapshot = research_leaf_snapshot(
                &visible,
                self.mount,
                max_bytes + 1,
                PolicyViolationStage::Finalized,
            )?;
            let visible_digest =
                hash_research_file(&visible, max_bytes, PolicyViolationStage::Finalized)?.1;
            if visible_snapshot != before || visible_digest != before_digest {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            self.revalidate_chain(true)?;
            Ok(bytes)
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn retained_result_for_generation(
        &self,
        generation: &File,
    ) -> Result<Option<(File, ResearchLeafSnapshot, Option<String>)>, PolicyViolation> {
        let Some(retained) = self.result_descriptor.borrow().as_ref().map(|retained| {
            (
                retained.file.try_clone(),
                retained.snapshot,
                retained.digest.clone(),
            )
        }) else {
            return Ok(None);
        };
        let retained_file = retained.0.map_err(|_| {
            temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
        })?;
        let retained_snapshot = research_leaf_snapshot(
            &retained_file,
            self.mount,
            u64::MAX,
            PolicyViolationStage::Finalized,
        )?;
        let visible = open_research_file_on_mount(
            generation,
            OsStr::new("result.json"),
            self.mount,
            PolicyViolationStage::Finalized,
        )?;
        let visible_snapshot = research_leaf_snapshot(
            &visible,
            self.mount,
            u64::MAX,
            PolicyViolationStage::Finalized,
        )?;
        if retained_snapshot != retained.1
            || visible_snapshot != retained.1
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        if let Some(expected_digest) = retained.2.as_ref() {
            let max_bytes = crate::result_manifest::MAX_RESULT_MANIFEST_BYTES as u64;
            let retained_digest =
                hash_research_file(&retained_file, max_bytes, PolicyViolationStage::Finalized)?.1;
            let visible_digest =
                hash_research_file(&visible, max_bytes, PolicyViolationStage::Finalized)?.1;
            if &retained_digest != expected_digest || &visible_digest != expected_digest {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
        }
        Ok(Some((retained_file, retained.1, retained.2)))
    }

    pub fn cleanup(&mut self) -> Result<TempCleanupReport, PolicyViolation> {
        self.cleanup_before(Instant::now() + std::time::Duration::from_secs(30))
    }

    pub(crate) fn cleanup_before(
        &mut self,
        cleanup_before: Instant,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        self.cleanup_before_with_hook(cleanup_before, || {})
    }

    fn cleanup_before_with_hook<F: FnOnce()>(
        &mut self,
        cleanup_before: Instant,
        before_generation_recheck: F,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (cleanup_before, before_generation_recheck);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if self.cleaned {
                return Ok(TempCleanupReport {
                    entries_removed: 0,
                    allocated_bytes_reclaimed: 0,
                });
            }
            let chain = self.revalidate_chain(true)?;
            check_cleanup_deadline(Some(cleanup_before))?;
            let _ = self.retained_result_for_generation(chain.generation.as_ref().unwrap())?;
            let mut state = AuditState::default();
            let entries = audit_trial_output_directory(
                chain.generation.as_ref().unwrap(),
                0,
                &mut state,
                Some(cleanup_before),
                self.mount,
            )?;
            let mut report = TempCleanupReport {
                entries_removed: 0,
                allocated_bytes_reclaimed: 0,
            };
            remove_audited_trial_output_entries(
                chain.generation.as_ref().unwrap(),
                &entries,
                &mut report,
                Some(cleanup_before),
                self.mount,
            )?;
            finish_cleanup_before_success(
                chain.generation.as_ref().unwrap(),
                Some(cleanup_before),
                #[cfg(all(test, unix))]
                None,
            )?;
            let chain = self.revalidate_chain(true)?;
            before_generation_recheck();
            check_cleanup_deadline(Some(cleanup_before))?;
            if directory_identity_at(
                &chain.generation.as_ref().unwrap(),
                PolicyViolationStage::Finalized,
            )? != (self.generation_record.device, self.generation_record.inode)
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            let named_generation = entry_metadata_at(
                &chain.trials,
                &self.generation_name,
                PolicyViolationStage::Finalized,
            )?;
            if named_generation.mount_identity != self.mount {
                return Err(temp_violation_at(
                    TempUnsafeReason::MountBoundary,
                    PolicyViolationStage::Finalized,
                ));
            }
            if named_generation.identity
                != (self.generation_record.device, self.generation_record.inode)
                || named_generation.kind != AuditedEntryKind::Directory
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            let visible_generation = open_directory_on_mount(
                &chain.trials,
                &self.generation_name,
                self.mount,
                PolicyViolationStage::Finalized,
            )?;
            validate_private_directory_at(&visible_generation, PolicyViolationStage::Finalized)?;
            if research_directory_record_at(
                &visible_generation,
                self.mount,
                PolicyViolationStage::Finalized,
            )? != self.generation_record
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            check_cleanup_deadline(Some(cleanup_before))?;
            unlinkat(&chain.trials, &self.generation_name, true)?;
            let _ = check_cleanup_deadline(Some(cleanup_before));
            sync_directory(
                &chain.trials,
                #[cfg(all(unix, test))]
                None,
            )?;
            check_cleanup_deadline(Some(cleanup_before))?;
            let parent = self.revalidate_chain(false)?;
            check_cleanup_deadline(Some(cleanup_before))?;
            if open_optional_directory_on_mount(
                &parent.trials,
                &self.generation_name,
                self.mount,
                PolicyViolationStage::Finalized,
            )?
            .is_some()
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::Finalized,
                ));
            }
            self.cleaned = true;
            Ok(report)
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn revalidate_chain(&self, require_generation: bool) -> Result<TrialOutputChain, PolicyViolation> {
        let stage = PolicyViolationStage::Finalized;
        let verified_root = self.root_anchor.verify_identity()?;
        let mount = directory_mount_identity_at(&verified_root.directory, stage)?;
        if mount != self.mount {
            return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
        }
        let root_record = research_directory_record_at(&verified_root.directory, mount, stage)?;
        if root_record != self.root_record
            || research_directory_record_at(&self.root, mount, stage)? != self.root_record
        {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        let service = open_directory_on_mount(&verified_root.directory, OsStr::new(PRIVATE_TEMP_ROOT), mount, stage)?;
        let service_record = research_directory_record_at(&service, mount, stage)?;
        if service_record != self.service_record
            || research_directory_record_at(&self.service, mount, stage)? != self.service_record
        {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        let trials = open_directory_on_mount(&service, OsStr::new("trials"), mount, stage)?;
        let trials_record = research_directory_record_at(&trials, mount, stage)?;
        if trials_record != self.trials_record
            || research_directory_record_at(&self.trials, mount, stage)? != self.trials_record
        {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        let generation = match open_optional_directory_on_mount(&trials, &self.generation_name, mount, stage)? {
            Some(generation) => {
                validate_private_directory_at(&generation, stage)?;
                let record = research_directory_record_at(&generation, mount, stage)?;
                if !require_generation {
                    return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
                }
                if Some(record.clone()) != Some(self.generation_record.clone())
                    || research_directory_record_at(&self.generation, mount, stage)? != self.generation_record
                {
                    return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
                }
                Some((generation, record))
            }
            None if require_generation => {
                return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
            }
            None => None,
        };
        Ok(TrialOutputChain {
            trials,
            generation: generation
                .as_ref()
                .map(|(file, _)| file.try_clone())
                .transpose()
                .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?,
        })
    }
}

/// The sole private-temp descriptor inherited by native agent targets.
pub const PRIVATE_TEMP_TARGET_FD: i32 = 11;
const PRIVATE_TEMP_TARGET_PATH: &str = "/dev/fd/11";

pub(crate) fn private_temp_target_path() -> &'static Path {
    Path::new(PRIVATE_TEMP_TARGET_PATH)
}

pub const MAX_PRIVATE_TEMP_CLEANUP_DEPTH: usize = 32;
pub const MAX_PRIVATE_TEMP_CLEANUP_ENTRIES: usize = 4096;
pub const MAX_PRIVATE_TEMP_ALLOCATED_BYTES: u64 = 1024 * 1024 * 1024;
pub const MAX_PRIVATE_TEMP_GENERATIONS: usize = 4096;
pub const MAX_PRIVATE_TEMP_RUN_ID: i64 = i64::MAX - 1;
const MAX_DECISION_ARTIFACT_SCAN_ENTRIES: usize = 4096;
#[cfg(target_os = "linux")]
const MAX_RESEARCH_STDOUT_BYTES: u64 = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_RESEARCH_STDERR_BYTES: u64 = 256 * 1024;
const RESEARCH_HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAX_RESEARCH_CHECKPOINT_CANDIDATES: usize = 4;
const MAX_RESEARCH_CHECKPOINT_DEPTH: usize = 4;
const MAX_RESEARCH_CHECKPOINT_ENTRIES: usize = 4096;
const MAX_RESEARCH_CHECKPOINT_FILE_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_RESEARCH_CHECKPOINT_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy)]
struct ResearchCheckpointLimits {
    max_files: usize,
    max_depth: usize,
    max_entries: usize,
    max_file_bytes: u64,
    max_total_bytes: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
const RESEARCH_CHECKPOINT_PRODUCTION_LIMITS: ResearchCheckpointLimits = ResearchCheckpointLimits {
    max_files: MAX_RESEARCH_CHECKPOINT_CANDIDATES,
    max_depth: MAX_RESEARCH_CHECKPOINT_DEPTH,
    max_entries: MAX_RESEARCH_CHECKPOINT_ENTRIES,
    max_file_bytes: MAX_RESEARCH_CHECKPOINT_FILE_BYTES,
    max_total_bytes: MAX_RESEARCH_CHECKPOINT_TOTAL_BYTES,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResearchDirectoryRecord {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) owner: u32,
    pub(crate) mode: u32,
    pub(crate) mount_identity: [u64; 2],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResearchFileRecord {
    pub(crate) relative_path: String,
    pub(crate) root: ResearchDirectoryRecord,
    pub(crate) parent: ResearchDirectoryRecord,
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) owner: u32,
    pub(crate) mode: u32,
    pub(crate) mount_identity: [u64; 2],
    pub(crate) logical_bytes: u64,
    pub(crate) allocated_bytes: u64,
    pub(crate) sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResearchFileDiscovery {
    pub(crate) records: Vec<ResearchFileRecord>,
    pub(crate) omitted_at_least: usize,
    pub(crate) complete: bool,
}

/// A source-file capability.  The descriptors and named-chain authority stay
/// private so callers can only use the bounded, revalidated read operation.
pub(crate) struct VerifiedResearchFile {
    file: File,
    root: File,
    parent: File,
    root_anchor: ProjectRootAnchor,
    components: Vec<OsString>,
    leaf_name: OsString,
    record: ResearchFileRecord,
    leaf_links: u64,
}

impl VerifiedResearchFile {
    pub(crate) fn record(&self) -> &ResearchFileRecord {
        &self.record
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResearchLeafSnapshot {
    device: u64,
    inode: u64,
    owner: u32,
    mode: u32,
    mount_identity: MountIdentity,
    logical_bytes: u64,
    allocated_bytes: u64,
    links: u64,
}

/// Open a regular source file below a startup-pinned project root and retain
/// the descriptor chain needed to revalidate every later read by name.
pub(crate) fn open_verified_research_file(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    relative: &Path,
    max_bytes: u64,
) -> Result<VerifiedResearchFile, PolicyViolation> {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (policy, root_anchor, relative, max_bytes);
        return Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::RunBoundPreMarker,
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let components = research_relative_components(relative)?;
        let registered = policy
            .project_root_anchor(&root_anchor.canonical_path)
            .map_err(|violation| stage_violation(violation, PolicyViolationStage::PreBinding))?;
        if registered != *root_anchor {
            return Err(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::PreBinding,
            ));
        }
        let verified_root = root_anchor.verify_identity()?;
        let root_mount = directory_mount_identity_at(
            &verified_root.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let root_record = research_directory_record_at(
            &verified_root.directory,
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if root_record.device != root_anchor.identity.device
            || root_record.inode != root_anchor.identity.inode
            || root_record.owner != root_anchor.identity.owner
            || root_record.mode != root_anchor.identity.mode
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }

        let mut current = verified_root
            .directory
            .try_clone()
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::RunBoundPreMarker))?;
        let mut parent_record = root_record.clone();
        for component in &components[..components.len() - 1] {
            let next = open_directory_on_mount(
                &current,
                component,
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let next_record = research_directory_record_at(
                &next,
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            current = next;
            parent_record = next_record;
        }

        let leaf_name = components.last().cloned().ok_or_else(|| {
            temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::RunBoundPreMarker,
            )
        })?;
        let file = open_research_file_on_mount(
            &current,
            &leaf_name,
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let before = research_leaf_snapshot(
            &file,
            root_mount,
            max_bytes,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let (hashed_bytes, digest) = hash_research_file(
            &file,
            max_bytes,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let after = research_leaf_snapshot(
            &file,
            root_mount,
            max_bytes,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if before != after || hashed_bytes != before.logical_bytes {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }

        let parent = current
            .try_clone()
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::RunBoundPreMarker))?;
        let root = verified_root
            .directory
            .try_clone()
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::RunBoundPreMarker))?;
        let record = ResearchFileRecord {
            relative_path: components
                .iter()
                .map(|component| component.to_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join("/"),
            root: root_record,
            parent: parent_record,
            device: before.device,
            inode: before.inode,
            owner: before.owner,
            mode: before.mode,
            mount_identity: before.mount_identity.0,
            logical_bytes: before.logical_bytes,
            allocated_bytes: before.allocated_bytes,
            sha256: digest,
        };
        let verified = VerifiedResearchFile {
            file,
            root,
            parent,
            root_anchor: root_anchor.clone(),
            components,
            leaf_name,
            record,
            leaf_links: before.links,
        };
        let _ = revalidate_research_file(&verified, max_bytes)?;
        Ok(verified)
    }
}

/// Read the complete source text from offset zero after bounded structural and
/// digest revalidation before and after the read.
pub(crate) fn read_verified_research_file(
    file: &VerifiedResearchFile,
    max_bytes: u64,
) -> Result<Vec<u8>, PolicyViolation> {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (file, max_bytes);
        return Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::Finalized,
        ));
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if file.record.logical_bytes > max_bytes {
            return Err(temp_violation_at(
                TempUnsafeReason::ByteLimit,
                PolicyViolationStage::Finalized,
            ));
        }
        let source = revalidate_research_file(file, max_bytes)?;
        let size = usize::try_from(file.record.logical_bytes).map_err(|_| {
            temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
        })?;
        let mut bytes = vec![0_u8; size];
        read_research_bytes(&source, &mut bytes, PolicyViolationStage::Finalized)?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if digest != file.record.sha256 {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        let _ = revalidate_research_file(file, max_bytes)?;
        Ok(bytes)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn record_verified_research_directory(
    _policy: &ResolvedExecutionPolicy,
    _root_anchor: &ProjectRootAnchor,
    _relative: &Path,
) -> Result<ResearchDirectoryRecord, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_root_for_scope(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    stage: PolicyViolationStage,
) -> Result<(File, MountIdentity, ResearchDirectoryRecord), PolicyViolation> {
    let registered = policy
        .project_root_anchor(&root_anchor.canonical_path)
        .map_err(|violation| stage_violation(violation, PolicyViolationStage::PreBinding))?;
    if registered != *root_anchor {
        return Err(PolicyViolation::new(
            PolicyViolationCode::RootChanged,
            PolicyViolationStage::PreBinding,
        ));
    }
    let verified = root_anchor.verify_identity()?;
    let mount = directory_mount_identity_at(&verified.directory, stage)?;
    let record = research_directory_record_at(&verified.directory, mount, stage)?;
    if record.device != root_anchor.identity.device
        || record.inode != root_anchor.identity.inode
        || record.owner != root_anchor.identity.owner
        || record.mode != root_anchor.identity.mode
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok((verified.directory, mount, record))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_open_directory_path(
    root: &File,
    mount: MountIdentity,
    components: &[OsString],
    stage: PolicyViolationStage,
) -> Result<(File, Vec<ResearchDirectoryRecord>), PolicyViolation> {
    let mut current = root
        .try_clone()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let mut records = vec![research_directory_record_at(&current, mount, stage)?];
    for component in components {
        let next = open_directory_on_mount(&current, component, mount, stage)?;
        let next_record = research_directory_record_at(&next, mount, stage)?;
        current = next;
        records.push(next_record);
    }
    Ok((current, records))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
enum ResearchDirectoryPath {
    Present((File, Vec<ResearchDirectoryRecord>)),
    Missing {
        component: usize,
        records: Vec<ResearchDirectoryRecord>,
    },
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_open_directory_path_allow_missing(
    root: &File,
    mount: MountIdentity,
    components: &[OsString],
    stage: PolicyViolationStage,
) -> Result<ResearchDirectoryPath, PolicyViolation> {
    let mut current = root
        .try_clone()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let mut records = vec![research_directory_record_at(&current, mount, stage)?];
    for (component, name) in components.iter().enumerate() {
        let Some(next) = open_optional_directory_on_mount(&current, name, mount, stage)? else {
            return Ok(ResearchDirectoryPath::Missing { component, records });
        };
        let next_record = research_directory_record_at(&next, mount, stage)?;
        current = next;
        records.push(next_record);
    }
    Ok(ResearchDirectoryPath::Present((current, records)))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_directory_components(relative: &Path) -> Result<Vec<OsString>, PolicyViolation> {
    if relative == Path::new(".") {
        return Ok(Vec::new());
    }
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    if relative
        .to_str()
        .is_some_and(|value| {
            value
                .split('/')
                .any(|component| component.is_empty() || component == "." || component == "..")
        })
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    let mut components = Vec::new();
    let mut bytes = 0usize;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::PreBinding,
            ));
        };
        let Some(name) = name.to_str() else {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::PreBinding,
            ));
        };
        if name.is_empty() {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::PreBinding,
            ));
        }
        bytes = bytes.checked_add(name.len() + 1).ok_or_else(|| {
            temp_violation_at(
                TempUnsafeReason::ByteLimit,
                PolicyViolationStage::PreBinding,
            )
        })?;
        if bytes > 4096 {
            return Err(temp_violation_at(
                TempUnsafeReason::ByteLimit,
                PolicyViolationStage::PreBinding,
            ));
        }
        components.push(OsString::from(name));
    }
    if components.is_empty() {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(components)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn record_verified_research_directory(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    relative: &Path,
) -> Result<ResearchDirectoryRecord, PolicyViolation> {
    record_verified_research_directory_with_hook(policy, root_anchor, relative, None)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn record_verified_research_directory_with_hook(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    relative: &Path,
    mut hook: Option<&mut dyn FnMut(&Path)>,
) -> Result<ResearchDirectoryRecord, PolicyViolation> {
    let stage = PolicyViolationStage::RunBoundPreMarker;
    let components = research_directory_components(relative)?;
    let (root, mount, root_record) = research_root_for_scope(policy, root_anchor, stage)?;
    let (_, records) = research_open_directory_path(&root, mount, &components, stage)?;
    if let Some(hook) = hook.as_deref_mut() {
        hook(relative);
    }
    let (fresh_root, fresh_mount, fresh_root_record) =
        research_root_for_scope(policy, root_anchor, stage)?;
    let (_, fresh_records) =
        research_open_directory_path(&fresh_root, fresh_mount, &components, stage)?;
    if fresh_mount != mount || fresh_root_record != root_record || fresh_records != records {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    records
        .last()
        .cloned()
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::IdentityChanged, stage))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn record_verified_research_directory_with_test_hook(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    relative: &Path,
    hook: &mut dyn FnMut(&Path),
) -> Result<ResearchDirectoryRecord, PolicyViolation> {
    record_verified_research_directory_with_hook(policy, root_anchor, relative, Some(hook))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn discover_research_checkpoint_files(
    _policy: &ResolvedExecutionPolicy,
    _root_anchor: &ProjectRootAnchor,
    _source_experiment_id: &str,
) -> Result<ResearchFileDiscovery, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct ResearchDiscoveryCandidate {
    relative_path: String,
    root: ResearchDirectoryRecord,
    parent: ResearchDirectoryRecord,
    parent_chain: Vec<ResearchDirectoryRecord>,
    snapshot: ResearchLeafSnapshot,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct ResearchDiscoveryEntry {
    kind: AuditedEntryKind,
    regular: bool,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    owner: u32,
    mode: u32,
    logical_bytes: u64,
    allocated_bytes: u64,
    links: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
enum ResearchDiscoveryEntryError {
    Missing,
    Violation(PolicyViolation),
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_discovery_entry_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<ResearchDiscoveryEntry, ResearchDiscoveryEntryError> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name_c = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| ResearchDiscoveryEntryError::Violation(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            stage,
        )))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if (unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name_c.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    }) != 0
    {
        let errno = io::Error::last_os_error().raw_os_error();
        if errno == Some(libc::ENOENT) {
            return Err(ResearchDiscoveryEntryError::Missing);
        }
        return Err(ResearchDiscoveryEntryError::Violation(temp_violation_at(
            TempUnsafeReason::IoFailure,
            stage,
        )));
    }
    let stat = unsafe { stat.assume_init() };
    let mount_identity = match entry_mount_identity_at(directory, name, stage) {
        Ok(mount_identity) => mount_identity,
        Err(error)
            if matches!(
                error.detail,
                PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
            ) && research_discovery_entry_missing(directory, name_c.as_c_str()) =>
        {
            return Err(ResearchDiscoveryEntryError::Missing);
        }
        Err(error) => return Err(ResearchDiscoveryEntryError::Violation(error)),
    };
    let mode = stat.st_mode & 0o7777;
    let kind = if stat.st_mode & libc::S_IFMT == libc::S_IFDIR {
        AuditedEntryKind::Directory
    } else {
        AuditedEntryKind::Leaf
    };
    let regular = stat.st_mode & libc::S_IFMT == libc::S_IFREG;
    let allocated_bytes = u64::try_from(stat.st_blocks)
        .ok()
        .and_then(|blocks| blocks.checked_mul(512))
        .ok_or_else(|| {
            ResearchDiscoveryEntryError::Violation(temp_violation_at(
                TempUnsafeReason::ByteLimit,
                stage,
            ))
        })?;
    let logical_bytes = u64::try_from(stat.st_size).map_err(|_| {
        ResearchDiscoveryEntryError::Violation(temp_violation_at(
            TempUnsafeReason::ByteLimit,
            stage,
        ))
    })?;
    Ok(ResearchDiscoveryEntry {
        kind,
        regular,
        identity: (stat.st_dev as u64, stat.st_ino as u64),
        mount_identity,
        owner: stat.st_uid,
        mode: mode as u32,
        logical_bytes,
        allocated_bytes,
        links: stat.st_nlink as u64,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_discovery_entry_missing(directory: &File, name: &std::ffi::CStr) -> bool {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    (unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    }) != 0
        && io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn collect_research_checkpoint_candidates(
    directory: &File,
    prefix: &str,
    depth: usize,
    parent_chain: &[ResearchDirectoryRecord],
    expected_mount: MountIdentity,
    limits: ResearchCheckpointLimits,
    state: &mut AuditState,
    candidates: &mut Vec<ResearchDiscoveryCandidate>,
    omitted_at_least: &mut usize,
    complete: &mut bool,
) -> Result<(), PolicyViolation> {
    if depth >= limits.max_depth {
        return Err(temp_violation_at(
            TempUnsafeReason::DepthLimit,
            PolicyViolationStage::RunBoundPreMarker,
        ));
    }
    let remaining = limits.max_entries.saturating_sub(state.entries);
    let mut entries = directory_entries(
        directory,
        remaining,
        TempUnsafeReason::EntryLimit,
        PolicyViolationStage::RunBoundPreMarker,
        None,
        Some(state),
        #[cfg(all(test, unix))]
        None,
    )?;
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    for listed in entries {
        let Some(name) = listed.name.to_str() else {
            if listed.kind == AuditedEntryKind::Directory {
                return Err(temp_violation_at(
                    TempUnsafeReason::InvalidEntry,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            *omitted_at_least = omitted_at_least.saturating_add(1);
            *complete = false;
            continue;
        };
        let metadata = match research_discovery_entry_at(
            directory,
            &listed.name,
            PolicyViolationStage::RunBoundPreMarker,
        ) {
            Ok(metadata) => metadata,
            Err(ResearchDiscoveryEntryError::Missing) => {
                if listed.kind == AuditedEntryKind::Leaf {
                    *omitted_at_least = omitted_at_least.saturating_add(1);
                    *complete = false;
                    continue;
                }
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            Err(ResearchDiscoveryEntryError::Violation(error)) => return Err(error),
        };
        if metadata.mount_identity != expected_mount || listed.mount_identity != expected_mount {
            return Err(temp_violation_at(
                TempUnsafeReason::MountBoundary,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        if metadata.kind != listed.kind || metadata.identity != listed.identity {
            if listed.kind == AuditedEntryKind::Leaf {
                *omitted_at_least = omitted_at_least.saturating_add(1);
                *complete = false;
                continue;
            }
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        if metadata.kind == AuditedEntryKind::Directory {
            let child_prefix = format!("{prefix}/{name}");
            if child_prefix.len() > 4096 {
                return Err(temp_violation_at(
                    TempUnsafeReason::ByteLimit,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let child = open_directory_on_mount(
                directory,
                &listed.name,
                expected_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let child_record = research_directory_record_at(
                &child,
                expected_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if child_record.device != metadata.identity.0
                || child_record.inode != metadata.identity.1
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let mut child_chain = parent_chain.to_vec();
            child_chain.push(child_record);
            collect_research_checkpoint_candidates(
                &child,
                &child_prefix,
                depth + 1,
                &child_chain,
                expected_mount,
                limits,
                state,
                candidates,
                omitted_at_least,
                complete,
            )?;
            continue;
        }
        let safe = metadata.regular
            && metadata.owner == unsafe { libc::geteuid() as u32 }
            && metadata.mode & 0o022 == 0
            && metadata.links == 1;
        if !safe
            || metadata.logical_bytes > limits.max_file_bytes
            || metadata.allocated_bytes > limits.max_file_bytes
            || depth >= limits.max_depth
        {
            *omitted_at_least = omitted_at_least.saturating_add(1);
            *complete = false;
            continue;
        }
        let relative_path = format!("{prefix}/{name}");
        if relative_path.len() > 4096 {
            *omitted_at_least = omitted_at_least.saturating_add(1);
            *complete = false;
            continue;
        }
        candidates.push(ResearchDiscoveryCandidate {
            relative_path,
            root: parent_chain
                .first()
                .cloned()
                .ok_or_else(|| {
                    temp_violation_at(TempUnsafeReason::IdentityChanged, PolicyViolationStage::RunBoundPreMarker)
                })?,
            parent: parent_chain
                .last()
                .cloned()
                .ok_or_else(|| {
                    temp_violation_at(TempUnsafeReason::IdentityChanged, PolicyViolationStage::RunBoundPreMarker)
                })?,
            parent_chain: parent_chain.to_vec(),
            snapshot: ResearchLeafSnapshot {
                device: metadata.identity.0,
                inode: metadata.identity.1,
                owner: metadata.owner,
                mode: metadata.mode,
                mount_identity: metadata.mount_identity,
                logical_bytes: metadata.logical_bytes,
                allocated_bytes: metadata.allocated_bytes,
                links: metadata.links,
            },
        });
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_discovery_candidate_matches(
    file: &VerifiedResearchFile,
    candidate: &ResearchDiscoveryCandidate,
) -> bool {
    let record = file.record();
    record.relative_path == candidate.relative_path
        && record.root == candidate.root
        && record.parent == candidate.parent
        && record.device == candidate.snapshot.device
        && record.inode == candidate.snapshot.inode
        && record.owner == candidate.snapshot.owner
        && record.mode == candidate.snapshot.mode
        && record.mount_identity == candidate.snapshot.mount_identity.0
        && record.logical_bytes == candidate.snapshot.logical_bytes
        && record.allocated_bytes == candidate.snapshot.allocated_bytes
        && file.leaf_links == candidate.snapshot.links
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_revalidate_candidate_parent(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    candidate: &ResearchDiscoveryCandidate,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let parent_path = Path::new(&candidate.relative_path)
        .parent()
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::IdentityChanged, stage))?;
    let components = research_relative_components(parent_path)?;
    let (root, mount, root_record) = research_root_for_scope(policy, root_anchor, stage)?;
    let (_, records) = research_open_directory_path(&root, mount, &components, stage)?;
    if mount != candidate.snapshot.mount_identity
        || root_record != candidate.root
        || records != candidate.parent_chain
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn discover_research_checkpoint_files_with_limits(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    source_experiment_id: &str,
    limits: ResearchCheckpointLimits,
    mut hook: Option<&mut dyn FnMut(&Path)>,
) -> Result<ResearchFileDiscovery, PolicyViolation> {
    validate_research_id(source_experiment_id)?;
    let stage = PolicyViolationStage::RunBoundPreMarker;
    let (root, mount, _root_record) = research_root_for_scope(policy, root_anchor, stage)?;
    let components = [
        OsString::from(PRIVATE_TEMP_ROOT),
        OsString::from(ARTIFACTS_DIRECTORY),
        OsString::from(source_experiment_id),
    ];
    let (scope, scope_records) = match research_open_directory_path_allow_missing(
        &root,
        mount,
        &components,
        stage,
    )? {
        ResearchDirectoryPath::Present(path) => path,
        ResearchDirectoryPath::Missing { component, records } => {
            if let Some(hook) = hook.as_deref_mut() {
                hook(Path::new(&format!(
                    "{PRIVATE_TEMP_ROOT}/{ARTIFACTS_DIRECTORY}/{source_experiment_id}"
                )));
            }
            let (fresh_root, fresh_mount, _) = research_root_for_scope(policy, root_anchor, stage)?;
            let fresh = research_open_directory_path_allow_missing(
                &fresh_root,
                fresh_mount,
                &components,
                stage,
            )?;
            if fresh_mount != mount
                || !matches!(
                    fresh,
                    ResearchDirectoryPath::Missing {
                        component: fresh_component,
                        records: fresh_records,
                    } if fresh_component == component && fresh_records == records
                )
            {
                return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
            }
            return Ok(ResearchFileDiscovery {
                records: Vec::new(),
                omitted_at_least: 0,
                complete: true,
            });
        }
    };
    let root_record = scope_records
        .first()
        .cloned()
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::IdentityChanged, stage))?;
    let mut state = AuditState::default();
    state.max_depth = limits.max_depth;
    state.max_entries = limits.max_entries;
    state.max_allocated_bytes = u64::MAX;
    let prefix = format!(
        "{PRIVATE_TEMP_ROOT}/{ARTIFACTS_DIRECTORY}/{source_experiment_id}"
    );
    let mut candidates = Vec::new();
    let mut omitted_at_least = 0usize;
    let mut complete = true;
    collect_research_checkpoint_candidates(
        &scope,
        &prefix,
        0,
        &scope_records,
        mount,
        limits,
        &mut state,
        &mut candidates,
        &mut omitted_at_least,
        &mut complete,
    )?;
    candidates.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let mut records = Vec::with_capacity(limits.max_files);
    let mut charged_logical_bytes = 0_u64;
    for candidate in candidates {
        if records.len() >= limits.max_files
            || charged_logical_bytes
                .checked_add(candidate.snapshot.logical_bytes)
                .is_none_or(|bytes| bytes > limits.max_total_bytes)
        {
            omitted_at_least = omitted_at_least.saturating_add(1);
            complete = false;
            continue;
        }
        charged_logical_bytes = charged_logical_bytes
            .checked_add(candidate.snapshot.logical_bytes)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
        if let Some(hook) = hook.as_deref_mut() {
            hook(Path::new(&candidate.relative_path));
        }
        let opened = open_verified_research_file(
            policy,
            root_anchor,
            Path::new(&candidate.relative_path),
            candidate.snapshot.logical_bytes,
        );
        match opened {
            Ok(file) => {
                research_revalidate_candidate_parent(policy, root_anchor, &candidate, stage)?;
                if file.record().allocated_bytes <= limits.max_file_bytes
                    && research_discovery_candidate_matches(&file, &candidate)
                {
                    records.push(file.record().clone());
                } else {
                    omitted_at_least = omitted_at_least.saturating_add(1);
                    complete = false;
                }
            }
            Err(error)
                if error.code == PolicyViolationCode::RootChanged
                    || error.code == PolicyViolationCode::UnsupportedPlatform
                    || matches!(
                        error.detail,
                        PolicyViolationDetail::TempUnsafe(
                            TempUnsafeReason::MountBoundary | TempUnsafeReason::EntryLimit
                        )
                    ) =>
            {
                return Err(error);
            }
            Err(_error) => {
                research_revalidate_candidate_parent(policy, root_anchor, &candidate, stage)?;
                omitted_at_least = omitted_at_least.saturating_add(1);
                complete = false;
            }
        }
    }
    let (fresh_root, fresh_mount, fresh_root_record) = research_root_for_scope(policy, root_anchor, stage)?;
    let fresh_scope_records = match research_open_directory_path_allow_missing(
        &fresh_root,
        fresh_mount,
        &components,
        stage,
    )? {
        ResearchDirectoryPath::Present((_, records)) => records,
        ResearchDirectoryPath::Missing { .. } => {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
    };
    if fresh_mount != mount
        || fresh_root_record != root_record
        || fresh_scope_records != scope_records
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(ResearchFileDiscovery {
        records,
        omitted_at_least,
        complete: complete && omitted_at_least == 0,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn discover_research_checkpoint_files(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    source_experiment_id: &str,
) -> Result<ResearchFileDiscovery, PolicyViolation> {
    discover_research_checkpoint_files_with_limits(
        policy,
        root_anchor,
        source_experiment_id,
        RESEARCH_CHECKPOINT_PRODUCTION_LIMITS,
        None,
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn discover_research_checkpoint_files_with_test_limits(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    source_experiment_id: &str,
    limits: ResearchCheckpointLimits,
) -> Result<ResearchFileDiscovery, PolicyViolation> {
    discover_research_checkpoint_files_with_limits(
        policy,
        root_anchor,
        source_experiment_id,
        limits,
        None,
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn discover_research_checkpoint_files_with_test_limits_and_hook(
    policy: &ResolvedExecutionPolicy,
    root_anchor: &ProjectRootAnchor,
    source_experiment_id: &str,
    limits: ResearchCheckpointLimits,
    hook: &mut dyn FnMut(&Path),
) -> Result<ResearchFileDiscovery, PolicyViolation> {
    discover_research_checkpoint_files_with_limits(
        policy,
        root_anchor,
        source_experiment_id,
        limits,
        Some(hook),
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_relative_components(relative: &Path) -> Result<Vec<OsString>, PolicyViolation> {
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::RunBoundPreMarker,
        ));
    }
    let mut components = Vec::new();
    let mut bytes = 0usize;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        };
        let Some(name) = name.to_str() else {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        };
        if name.is_empty() {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        bytes = bytes.checked_add(name.len() + 1).ok_or_else(|| {
            temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::RunBoundPreMarker)
        })?;
        if bytes > 4096 {
            return Err(temp_violation_at(
                TempUnsafeReason::ByteLimit,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        components.push(OsString::from(name));
    }
    if components.is_empty() {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::RunBoundPreMarker,
        ));
    }
    Ok(components)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_directory_record_at(
    directory: &File,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<ResearchDirectoryRecord, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let mount_identity = directory_mount_identity_at(directory, stage)?;
    let owner = metadata.uid();
    let mode = metadata.mode() & 0o7777;
    let links = metadata.nlink();
    if mount_identity != expected_mount
        || !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || owner != unsafe { libc::geteuid() as u32 }
        || mode & 0o022 != 0
        || links == 0
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(ResearchDirectoryRecord {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner,
        mode,
        mount_identity: mount_identity.0,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_leaf_snapshot(
    file: &File,
    expected_mount: MountIdentity,
    max_bytes: u64,
    stage: PolicyViolationStage,
) -> Result<ResearchLeafSnapshot, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let mount_identity = directory_mount_identity_at(file, stage)?;
    let owner = metadata.uid();
    let mode = metadata.mode() & 0o7777;
    let links = metadata.nlink();
    if mount_identity != expected_mount
        || !metadata.is_file()
        || metadata.file_type().is_symlink()
        || owner != unsafe { libc::geteuid() as u32 }
        || mode & 0o022 != 0
        || links != 1
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    let allocated_bytes = metadata.blocks().checked_mul(512).ok_or_else(|| {
        temp_violation_at(TempUnsafeReason::ByteLimit, stage)
    })?;
    if metadata.len() > max_bytes {
        return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
    }
    Ok(ResearchLeafSnapshot {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner,
        mode,
        mount_identity,
        logical_bytes: metadata.len(),
        allocated_bytes,
        links,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn hash_research_file(
    file: &File,
    max_bytes: u64,
    stage: PolicyViolationStage,
) -> Result<(u64, String), PolicyViolation> {
    use std::os::unix::fs::FileExt;

    hash_research_file_with_reader(max_bytes, stage, |buffer, offset| {
        file.read_at(buffer, offset)
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn hash_research_file_with_reader<F>(
    max_bytes: u64,
    stage: PolicyViolationStage,
    mut read_at: F,
) -> Result<(u64, String), PolicyViolation>
where
    F: FnMut(&mut [u8], u64) -> io::Result<usize>,
{
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; RESEARCH_HASH_BUFFER_BYTES];
    let mut offset = 0_u64;
    loop {
        if offset > max_bytes {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        let remaining_with_sentinel = max_bytes.saturating_sub(offset).saturating_add(1);
        let requested = usize::try_from(remaining_with_sentinel)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = read_at(&mut buffer[..requested], offset)
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
        if read == 0 {
            break;
        }
        offset = offset.checked_add(read as u64).ok_or_else(|| {
            temp_violation_at(TempUnsafeReason::ByteLimit, stage)
        })?;
        if offset > max_bytes {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        hasher.update(&buffer[..read]);
    }
    Ok((offset, format!("{:x}", hasher.finalize())))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn read_research_bytes(
    file: &File,
    bytes: &mut [u8],
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    use std::os::unix::fs::FileExt;

    let mut offset = 0usize;
    while offset < bytes.len() {
        let read = file
            .read_at(&mut bytes[offset..], offset as u64)
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
        if read == 0 {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        offset = offset.checked_add(read).ok_or_else(|| {
            temp_violation_at(TempUnsafeReason::ByteLimit, stage)
        })?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn research_leaf_matches_record(
    snapshot: ResearchLeafSnapshot,
    record: &ResearchFileRecord,
    expected_links: u64,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    if snapshot.device != record.device
        || snapshot.inode != record.inode
        || snapshot.owner != record.owner
        || snapshot.mode != record.mode
        || snapshot.mount_identity.0 != record.mount_identity
        || snapshot.logical_bytes != record.logical_bytes
        || snapshot.allocated_bytes != record.allocated_bytes
        || snapshot.links != expected_links
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn revalidate_research_file(
    file: &VerifiedResearchFile,
    max_bytes: u64,
) -> Result<File, PolicyViolation> {
    let stage = PolicyViolationStage::Finalized;
    file.root_anchor.verify_identity()?;
    let expected_mount = MountIdentity(file.record.root.mount_identity);
    let root_record = research_directory_record_at(&file.root, expected_mount, stage)?;
    if root_record != file.record.root {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let retained_parent = research_directory_record_at(&file.parent, expected_mount, stage)?;
    if retained_parent != file.record.parent {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let retained_leaf = research_leaf_snapshot(
        &file.file,
        expected_mount,
        max_bytes,
        stage,
    )?;
    research_leaf_matches_record(retained_leaf, &file.record, file.leaf_links, stage)?;
    let mut current = file
        .root
        .try_clone()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    for component in &file.components[..file.components.len() - 1] {
        let next = open_directory_on_mount(&current, component, expected_mount, stage)?;
        current = next;
    }
    let parent_record = research_directory_record_at(&current, expected_mount, stage)?;
    if parent_record != file.record.parent {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let leaf = open_research_file_on_mount(&current, &file.leaf_name, expected_mount, stage)?;
    let snapshot = research_leaf_snapshot(&leaf, expected_mount, max_bytes, stage)?;
    research_leaf_matches_record(snapshot, &file.record, file.leaf_links, stage)?;
    let (hashed_bytes, digest) = hash_research_file(&leaf, max_bytes, stage)?;
    if hashed_bytes != file.record.logical_bytes || digest != file.record.sha256 {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(leaf)
}

#[cfg(target_os = "linux")]
fn open_research_file_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let file = linux_openat2_no_xdev(
        parent,
        name,
        libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
    )
    .map_err(|failure| linux_syscall_violation(failure, stage))?;
    if directory_mount_identity_at(&file, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(file)
}

#[cfg(target_os = "macos")]
fn open_research_file_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    use std::{
        os::fd::{AsRawFd, FromRawFd},
        os::unix::ffi::OsStrExt,
    };
    if entry_mount_identity_at(parent, name, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let file = unsafe { File::from_raw_fd(fd) };
    if directory_mount_identity_at(&file, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(file)
}

const RESEARCH_CHECKPOINT_NAMESPACE: &str = "research-checkpoints";
const RESEARCH_CHECKPOINT_LEAF: &str = "checkpoint";
const RESEARCH_CHECKPOINT_TEMP_PREFIX: &str = ".checkpoint.tmp-";
static RESEARCH_CHECKPOINT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Validate an identifier before it reaches any descriptor-relative lookup.
/// The same narrow alphabet is used for campaign and review directory names,
/// so persisted records never carry path syntax or platform-specific bytes.
pub(crate) fn validate_research_id(value: &str) -> Result<(), PolicyViolation> {
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-')
        })
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct RetentionChain {
    root: File,
    namespace: File,
    campaign: File,
    review: File,
    mount: MountIdentity,
    root_record: ResearchDirectoryRecord,
    namespace_record: ResearchDirectoryRecord,
    campaign_record: ResearchDirectoryRecord,
    review_record: ResearchDirectoryRecord,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl RetentionChain {
    fn records_equal(&self, other: &Self) -> bool {
        self.mount == other.mount
            && self.root_record == other.root_record
            && self.namespace_record == other.namespace_record
            && self.campaign_record == other.campaign_record
            && self.review_record == other.review_record
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone)]
struct RetentionChainIdentity {
    mount: MountIdentity,
    root_record: ResearchDirectoryRecord,
    namespace_record: ResearchDirectoryRecord,
    campaign_record: ResearchDirectoryRecord,
    review_record: ResearchDirectoryRecord,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl RetentionChainIdentity {
    fn from_chain(chain: &RetentionChain) -> Self {
        Self {
            mount: chain.mount,
            root_record: chain.root_record.clone(),
            namespace_record: chain.namespace_record.clone(),
            campaign_record: chain.campaign_record.clone(),
            review_record: chain.review_record.clone(),
        }
    }

    fn matches_chain(&self, chain: &RetentionChain) -> bool {
        self.mount == chain.mount
            && self.root_record == chain.root_record
            && self.namespace_record == chain.namespace_record
            && self.campaign_record == chain.campaign_record
            && self.review_record == chain.review_record
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Debug)]
struct ResearchCampaignLock {
    file: File,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl ResearchCampaignLock {
    fn acquire(file: File, shared: bool, stage: PolicyViolationStage) -> Result<Self, PolicyViolation> {
        use std::os::fd::AsRawFd;
        let mode = if shared { libc::LOCK_SH } else { libc::LOCK_EX } | libc::LOCK_NB;
        if unsafe { libc::flock(file.as_raw_fd(), mode) } != 0 {
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        Ok(Self { file })
    }

    fn downgrade_shared(&mut self, stage: PolicyViolationStage) -> Result<(), PolicyViolation> {
        use std::os::fd::AsRawFd;
        if unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) } != 0 {
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for ResearchCampaignLock {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd;
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

/// A retained checkpoint capability.  The retained descriptor and shared
/// campaign lease remain private; callers receive only the strict record.
pub(crate) struct RetainedResearchFile {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    file: File,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    chain: RetentionChain,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    campaign_lock: ResearchCampaignLock,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    campaign_id: String,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    review_id: String,
    record: ResearchFileRecord,
}

impl RetainedResearchFile {
    pub(crate) fn record(&self) -> &ResearchFileRecord {
        &self.record
    }
}

/// Narrow test hooks let focused tests exercise failure paths without sleeps,
/// large quota allocations, or process-wide fault injection.
#[derive(Debug)]
struct RetentionTestHooks {
    quota_bytes: u64,
    fail_file_sync: bool,
    fail_parent_sync: bool,
    fail_rollback_parent_sync: bool,
    fail_publication: bool,
    fail_after_publication_validation: bool,
    replace_campaign_before_reopen: bool,
    write_attempts: usize,
    #[cfg(all(test, unix))]
    fail_downgrade: bool,
    #[cfg(all(test, unix))]
    hold_competing_reader: bool,
    #[cfg(all(test, unix))]
    competing_reader: Option<ResearchCampaignLock>,
    mutate_after_bytes: Option<u64>,
    mutation_file: Option<File>,
}

impl Default for RetentionTestHooks {
    fn default() -> Self {
        Self {
            quota_bytes: MAX_PRIVATE_TEMP_ALLOCATED_BYTES,
            fail_file_sync: false,
            fail_parent_sync: false,
            fail_rollback_parent_sync: false,
            fail_publication: false,
            fail_after_publication_validation: false,
            replace_campaign_before_reopen: false,
            write_attempts: 0,
            #[cfg(all(test, unix))]
            fail_downgrade: false,
            #[cfg(all(test, unix))]
            hold_competing_reader: false,
            #[cfg(all(test, unix))]
            competing_reader: None,
            mutate_after_bytes: None,
            mutation_file: None,
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn retain_verified_research_file(
    _policy: &ResolvedExecutionPolicy,
    _campaign_id: &str,
    _review_id: &str,
    _source: &VerifiedResearchFile,
) -> Result<RetainedResearchFile, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn reopen_retained_research_file(
    _policy: &ResolvedExecutionPolicy,
    _campaign_id: &str,
    _review_id: &str,
    _expected: &ResearchFileRecord,
) -> Result<RetainedResearchFile, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn reverify_retained_research_file(
    _policy: &ResolvedExecutionPolicy,
    _retained: &RetainedResearchFile,
) -> Result<(), PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub(crate) fn cleanup_retained_research_file(
    _policy: &ResolvedExecutionPolicy,
    _campaign_id: &str,
    _review_id: &str,
    _expected: &ResearchFileRecord,
) -> Result<(), PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        PolicyViolationStage::PreBinding,
    ))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn retain_verified_research_file(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    source: &VerifiedResearchFile,
) -> Result<RetainedResearchFile, PolicyViolation> {
    let mut hooks = RetentionTestHooks::default();
    retain_verified_research_file_impl(policy, campaign_id, review_id, source, &mut hooks)
}

#[cfg(test)]
fn retain_verified_research_file_with_test_hooks(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    source: &VerifiedResearchFile,
    hooks: &mut RetentionTestHooks,
) -> Result<RetainedResearchFile, PolicyViolation> {
    retain_verified_research_file_impl(policy, campaign_id, review_id, source, hooks)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_stage() -> PolicyViolationStage {
    PolicyViolationStage::Finalized
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_state_root(
    policy: &ResolvedExecutionPolicy,
    stage: PolicyViolationStage,
) -> Result<(File, MountIdentity, ResearchDirectoryRecord), PolicyViolation> {
    policy.verify_code_change_state_root()?;
    let root = policy
        .code_change_state_root_directory()
        .try_clone()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let mount = directory_mount_identity_at(&root, stage)?;
    let record = research_directory_record_at(&root, mount, stage)?;
    let identity = policy.code_change_state_root_identity();
    if record.device != identity.device
        || record.inode != identity.inode
        || record.owner != identity.owner
        || record.mode != identity.mode
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok((root, mount, record))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_or_create_retention_directory(
    parent: &File,
    name: &OsStr,
    mount: MountIdentity,
    create: bool,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let directory = match open_optional_directory_on_mount(parent, name, mount, stage)? {
        Some(directory) => directory,
        None if create => {
            use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
            let name_c = std::ffi::CString::new(name.as_bytes())
                .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
            let created = unsafe { libc::mkdirat(parent.as_raw_fd(), name_c.as_ptr(), 0o700) };
            if created == 0 {
                parent
                    .sync_all()
                    .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
            } else if io::Error::last_os_error().raw_os_error() != Some(libc::EEXIST) {
                return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
            }
            open_directory_on_mount(parent, name, mount, stage)?
        }
        None => return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage)),
    };
    let record = research_directory_record_at(&directory, mount, stage)?;
    if record.mode != 0o700 {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(directory)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct RetentionCampaignBase {
    root: File,
    namespace: File,
    campaign: File,
    mount: MountIdentity,
    root_record: ResearchDirectoryRecord,
    namespace_record: ResearchDirectoryRecord,
    campaign_record: ResearchDirectoryRecord,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl RetentionCampaignBase {
    fn records_equal(&self, other: &Self) -> bool {
        self.mount == other.mount
            && self.root_record == other.root_record
            && self.namespace_record == other.namespace_record
            && self.campaign_record == other.campaign_record
    }

    fn with_review(
        self,
        review: File,
        review_record: ResearchDirectoryRecord,
    ) -> RetentionChain {
        RetentionChain {
            root: self.root,
            namespace: self.namespace,
            campaign: self.campaign,
            review,
            mount: self.mount,
            root_record: self.root_record,
            namespace_record: self.namespace_record,
            campaign_record: self.campaign_record,
            review_record,
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_retention_campaign(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    create: bool,
    stage: PolicyViolationStage,
) -> Result<RetentionCampaignBase, PolicyViolation> {
    validate_research_id(campaign_id)?;
    let (root, mount, root_record) = retention_state_root(policy, stage)?;
    let namespace = open_or_create_retention_directory(
        &root,
        OsStr::new(RESEARCH_CHECKPOINT_NAMESPACE),
        mount,
        create,
        stage,
    )?;
    let campaign = open_or_create_retention_directory(
        &namespace,
        OsStr::new(campaign_id),
        mount,
        create,
        stage,
    )?;
    let campaign_record = research_directory_record_at(&campaign, mount, stage)?;
    if campaign_record.mode != 0o700 {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    let namespace_record = research_directory_record_at(&namespace, mount, stage)?;
    Ok(RetentionCampaignBase {
        root,
        namespace,
        campaign,
        mount,
        root_record,
        namespace_record,
        campaign_record,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_retention_chain(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    create: bool,
    stage: PolicyViolationStage,
) -> Result<RetentionChain, PolicyViolation> {
    validate_research_id(campaign_id)?;
    validate_research_id(review_id)?;
    let base = open_retention_campaign(policy, campaign_id, create, stage)?;
    let review = open_or_create_retention_directory(
        &base.campaign,
        OsStr::new(review_id),
        base.mount,
        create,
        stage,
    )?;
    let review_record = research_directory_record_at(&review, base.mount, stage)?;
    if review_record.mode != 0o700 {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(base.with_review(review, review_record))
}

#[cfg(all(test, unix))]
fn replace_retention_campaign_for_test(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    _stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    use std::os::unix::fs::PermissionsExt;

    let namespace = policy
        .code_change_state_root_path()
        .join(RESEARCH_CHECKPOINT_NAMESPACE);
    let campaign = namespace.join(campaign_id);
    let retired = namespace.join(".campaign-replaced");
    std::fs::rename(&campaign, &retired)
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure))?;
    std::fs::create_dir(&campaign)
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure))?;
    std::fs::set_permissions(&campaign, std::fs::Permissions::from_mode(0o700))
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure))?;
    let namespace_file = File::open(namespace)
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure))?;
    namespace_file
        .sync_all()
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn acquire_retention_namespace(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    create: bool,
    shared: bool,
    stage: PolicyViolationStage,
    hooks: Option<&mut RetentionTestHooks>,
) -> Result<(RetentionChain, ResearchCampaignLock), PolicyViolation> {
    let before = match open_retention_chain(policy, campaign_id, review_id, false, stage) {
        Ok(chain) => Some(chain),
        Err(_error) if create => None,
        Err(error) => return Err(error),
    };
    let campaign_base = open_retention_campaign(policy, campaign_id, create, stage)?;
    let lock_directory = open_directory_on_mount(
        &campaign_base.namespace,
        OsStr::new(campaign_id),
        campaign_base.mount,
        stage,
    )?;
    let lock_record = research_directory_record_at(&lock_directory, campaign_base.mount, stage)?;
    if lock_record != campaign_base.campaign_record {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let lock = ResearchCampaignLock::acquire(lock_directory, shared, stage)?;
    #[cfg(not(all(test, unix)))]
    let _ = hooks;
    #[cfg(all(test, unix))]
    let mut hooks = hooks;
    #[cfg(all(test, unix))]
    if hooks
        .as_deref()
        .is_some_and(|hooks| hooks.replace_campaign_before_reopen)
    {
        if let Some(hooks) = hooks.as_deref_mut() {
            hooks.replace_campaign_before_reopen = false;
        }
        replace_retention_campaign_for_test(policy, campaign_id, stage)?;
    }
    let current_campaign = open_retention_campaign(policy, campaign_id, false, stage)?;
    if !campaign_base.records_equal(&current_campaign) {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let after = if before.is_some() {
        open_retention_chain(policy, campaign_id, review_id, false, stage)?
    } else {
        let review = open_or_create_retention_directory(
            &current_campaign.campaign,
            OsStr::new(review_id),
            current_campaign.mount,
            true,
            stage,
        )?;
        let review_record = research_directory_record_at(&review, current_campaign.mount, stage)?;
        if review_record.mode != 0o700 {
            return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
        }
        current_campaign.with_review(review, review_record)
    };
    if let Some(before) = before {
        if !before.records_equal(&after) {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
    }
    Ok((after, lock))
}

#[cfg(all(test, unix))]
fn downgrade_retention_campaign_for_test(
    campaign_lock: &mut ResearchCampaignLock,
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    use std::os::fd::AsRawFd;

    if !hooks.fail_downgrade {
        return campaign_lock.downgrade_shared(stage);
    }
    hooks.fail_downgrade = false;
    if unsafe { libc::flock(campaign_lock.file.as_raw_fd(), libc::LOCK_UN) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    if hooks.hold_competing_reader {
        let base = open_retention_campaign(policy, campaign_id, false, stage)?;
        let reader = ResearchCampaignLock::acquire(base.campaign, true, stage)?;
        hooks.competing_reader = Some(reader);
    }
    Err(temp_violation_at(TempUnsafeReason::IoFailure, stage))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct RetentionInventory {
    logical_bytes: u64,
    allocated_bytes: u64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_temp_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else { return false };
    let Some(hex) = name.strip_prefix(RESEARCH_CHECKPOINT_TEMP_PREFIX) else {
        return false;
    };
    hex.len() == 32 && hex.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_inventory(
    chain: &RetentionChain,
    quota_bytes: u64,
    stage: PolicyViolationStage,
) -> Result<RetentionInventory, PolicyViolation> {
    let campaign_entries = directory_entries(
        &chain.campaign,
        MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
        TempUnsafeReason::EntryLimit,
        stage,
        None,
        None,
        #[cfg(all(test, unix))]
        None,
    )?;
    let mut logical_bytes = 0_u64;
    let mut allocated_bytes = 0_u64;
    let mut entries_seen = 0_usize;
    for review_entry in campaign_entries {
        entries_seen = entries_seen
            .checked_add(1)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::EntryLimit, stage))?;
        if entries_seen > MAX_PRIVATE_TEMP_CLEANUP_ENTRIES {
            return Err(temp_violation_at(TempUnsafeReason::EntryLimit, stage));
        }
        if review_entry.kind != AuditedEntryKind::Directory
            || review_entry.mount_identity != chain.mount
            || validate_research_id(review_entry.name.to_str().unwrap_or_default()).is_err()
        {
            return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
        }
        let review = open_directory_on_mount(
            &chain.campaign,
            &review_entry.name,
            chain.mount,
            stage,
        )?;
        let review_record = research_directory_record_at(&review, chain.mount, stage)?;
        if review_record.mode != 0o700
            || (review_record.device, review_record.inode) != review_entry.identity
        {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        let entries = directory_entries(
            &review,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            TempUnsafeReason::EntryLimit,
            stage,
            None,
            None,
            #[cfg(all(test, unix))]
            None,
        )?;
        for entry in entries {
            entries_seen = entries_seen
                .checked_add(1)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::EntryLimit, stage))?;
            if entries_seen > MAX_PRIVATE_TEMP_CLEANUP_ENTRIES {
                return Err(temp_violation_at(TempUnsafeReason::EntryLimit, stage));
            }
            if entry.kind != AuditedEntryKind::Leaf
                || entry.mount_identity != chain.mount
                || (entry.name != OsStr::new(RESEARCH_CHECKPOINT_LEAF)
                    && !retention_temp_name(&entry.name))
            {
                return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
            }
            let file = open_research_file_on_mount(&review, &entry.name, chain.mount, stage)?;
            let snapshot = research_leaf_snapshot(&file, chain.mount, quota_bytes, stage)?;
            if snapshot.mode != 0o600
                || snapshot.links != 1
                || (snapshot.device, snapshot.inode) != entry.identity
            {
                return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
            }
            logical_bytes = logical_bytes
                .checked_add(snapshot.logical_bytes)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
            allocated_bytes = allocated_bytes
                .checked_add(snapshot.allocated_bytes)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
            if logical_bytes > quota_bytes || allocated_bytes > quota_bytes {
                return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
            }
        }
    }
    Ok(RetentionInventory {
        logical_bytes,
        allocated_bytes,
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_leaf_entry(
    review: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<Option<EntryMetadata>, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name_c = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            review.as_raw_fd(),
            name_c.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        if io::Error::last_os_error().raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    Ok(Some(entry_metadata_at(review, name, stage)?))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn generated_retention_temp_name() -> OsString {
    let counter = RESEARCH_CHECKPOINT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let value = now ^ ((std::process::id() as u128) << 64) ^ counter as u128;
    OsString::from(format!("{RESEARCH_CHECKPOINT_TEMP_PREFIX}{value:032x}"))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_retention_temp(
    review: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    use std::{os::fd::{AsRawFd, FromRawFd}, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let fd = unsafe {
        libc::openat(
            review.as_raw_fd(),
            name.as_ptr(),
            libc::O_WRONLY
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_sync_file(
    file: &File,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    if hooks.fail_file_sync {
        hooks.fail_file_sync = false;
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    file.sync_all()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_sync_directory(
    directory: &File,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    if hooks.fail_parent_sync {
        hooks.fail_parent_sync = false;
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    directory
        .sync_all()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cleanup_owned_retention_temp(
    review: &File,
    temp_name: &OsStr,
    temp: &File,
    mount: MountIdentity,
    stage: PolicyViolationStage,
) {
    use std::os::unix::fs::MetadataExt;
    let Ok(entry) = entry_metadata_at(review, temp_name, stage) else {
        return;
    };
    let Ok(metadata) = temp.metadata() else {
        return;
    };
    if entry.kind != AuditedEntryKind::Leaf
        || entry.mount_identity != mount
        || entry.identity != (metadata.dev(), metadata.ino())
    {
        return;
    }
    if unlinkat(review, temp_name, false).is_ok() {
        let _ = review.sync_all();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_allocation_unit(
    file: &File,
    stage: PolicyViolationStage,
) -> Result<u64, PolicyViolation> {
    use std::os::fd::AsRawFd;

    let mut statistics = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    if unsafe { libc::fstatvfs(file.as_raw_fd(), statistics.as_mut_ptr()) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let statistics = unsafe { statistics.assume_init() };
    let unit = statistics.f_frsize as u64;
    let unit = if unit != 0 {
        unit
    } else {
        statistics.f_bsize as u64
    };
    if unit == 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    Ok(unit)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_round_up(
    value: u64,
    unit: u64,
    stage: PolicyViolationStage,
) -> Result<u64, PolicyViolation> {
    if unit == 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let adjusted = value
        .checked_add(unit - 1)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    adjusted
        .checked_div(unit)
        .and_then(|quotient| quotient.checked_mul(unit))
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[derive(Clone, Copy)]
struct OwnedRetentionPublication {
    device: u64,
    inode: u64,
    mount: MountIdentity,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_entry_is_owned(
    directory: &File,
    name: &OsStr,
    publication: OwnedRetentionPublication,
    stage: PolicyViolationStage,
) -> Result<bool, PolicyViolation> {
    let Some(entry) = retention_leaf_entry(directory, name, stage)? else {
        return Ok(false);
    };
    Ok(entry.kind == AuditedEntryKind::Leaf
        && entry.mount_identity == publication.mount
        && entry.identity == (publication.device, publication.inode))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retained_publication_recovery_required(stage: PolicyViolationStage) -> PolicyViolation {
    temp_violation_at(
        TempUnsafeReason::RetainedPublicationRecoveryRequired,
        stage,
    )
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rollback_owned_retention_publication_durable(
    review: &File,
    temp_name: &OsStr,
    temp: &File,
    publication: OwnedRetentionPublication,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let final_owned = retention_entry_is_owned(
        review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        publication,
        stage,
    )
    .map_err(|_| retained_publication_recovery_required(stage))?;
    if !final_owned {
        return Err(retained_publication_recovery_required(stage));
    }
    unlinkat(review, OsStr::new(RESEARCH_CHECKPOINT_LEAF), false)
        .map_err(|_| retained_publication_recovery_required(stage))?;
    let temp_owned = retention_entry_is_owned(review, temp_name, publication, stage)
        .map_err(|_| retained_publication_recovery_required(stage))?;
    if temp_owned {
        unlinkat(review, temp_name, false)
            .map_err(|_| retained_publication_recovery_required(stage))?;
    }
    if hooks.fail_rollback_parent_sync {
        hooks.fail_rollback_parent_sync = false;
        return Err(retained_publication_recovery_required(stage));
    }
    retention_sync_directory(review, hooks, stage)
        .map_err(|_| retained_publication_recovery_required(stage))?;
    let _ = temp;
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retain_postpublication_failure<T>(
    error: PolicyViolation,
    review: &File,
    temp_name: &OsStr,
    temp: &File,
    publication: OwnedRetentionPublication,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<T, PolicyViolation> {
    if rollback_owned_retention_publication_durable(
        review,
        temp_name,
        temp,
        publication,
        hooks,
        stage,
    )
    .is_err()
    {
        Err(retained_publication_recovery_required(stage))
    } else {
        Err(error)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_publication_failure(
    error: PolicyViolation,
    review: &File,
    temp_name: &OsStr,
    temp: &File,
    publication: OwnedRetentionPublication,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> PolicyViolation {
    if rollback_owned_retention_publication_durable(
        review,
        temp_name,
        temp,
        publication,
        hooks,
        stage,
    )
    .is_err()
    {
        retained_publication_recovery_required(stage)
    } else {
        error
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn rollback_after_downgrade_failure(
    error: PolicyViolation,
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    expected_chain: &RetentionChainIdentity,
    rollback_review: &File,
    temp_name: &OsStr,
    temp: &File,
    publication: OwnedRetentionPublication,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<RetainedResearchFile, PolicyViolation> {
    let Ok((current_chain, _rollback_lock)) = acquire_retention_namespace(
        policy,
        campaign_id,
        review_id,
        false,
        false,
        stage,
        None,
    ) else {
        return Err(retained_publication_recovery_required(stage));
    };
    if !expected_chain.matches_chain(&current_chain) {
        return Err(retained_publication_recovery_required(stage));
    }
    let current_named_chain = match open_retention_chain(policy, campaign_id, review_id, false, stage) {
        Ok(chain) => chain,
        Err(_) => return Err(retained_publication_recovery_required(stage)),
    };
    if !expected_chain.matches_chain(&current_named_chain) {
        return Err(retained_publication_recovery_required(stage));
    }
    let review_record = match research_directory_record_at(
        rollback_review,
        expected_chain.mount,
        stage,
    ) {
        Ok(record) => record,
        Err(_) => return Err(retained_publication_recovery_required(stage)),
    };
    if review_record != expected_chain.review_record {
        return Err(retained_publication_recovery_required(stage));
    }
    match retention_entry_is_owned(
        rollback_review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        publication,
        stage,
    ) {
        Ok(true) => {}
        Ok(false) | Err(_) => return Err(retained_publication_recovery_required(stage)),
    }
    if rollback_owned_retention_publication_durable(
        rollback_review,
        temp_name,
        temp,
        publication,
        hooks,
        stage,
    )
    .is_err()
    {
        return Err(retained_publication_recovery_required(stage));
    }
    Err(error)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn copy_research_source(
    source: &VerifiedResearchFile,
    temp: &mut File,
    max_bytes: u64,
    existing_logical_bytes: u64,
    existing_allocated_bytes: u64,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<(u64, String), PolicyViolation> {
    use std::os::unix::fs::FileExt;
    use std::os::unix::fs::MetadataExt;
    let _ = revalidate_research_file(source, max_bytes)?;
    let allocation_unit = retention_allocation_unit(temp, stage)?;
    let logical_limit = max_bytes
        .checked_sub(existing_logical_bytes)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    let allocated_limit = max_bytes
        .checked_sub(existing_allocated_bytes)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    if source.record.logical_bytes > logical_limit
        || source.record.allocated_bytes > allocated_limit
    {
        return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; RESEARCH_HASH_BUFFER_BYTES];
    let mut offset = 0_u64;
    loop {
        let read = source
            .file
            .read_at(&mut buffer, offset)
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
        if read == 0 {
            break;
        }
        let next_offset = offset
            .checked_add(read as u64)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
        if next_offset > logical_limit {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        let current_allocated = temp
            .metadata()
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?
            .blocks()
            .checked_mul(512)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
        let dense_end = retention_round_up(next_offset, allocation_unit, stage)?;
        let projected_allocated = current_allocated.max(dense_end);
        if existing_allocated_bytes
            .checked_add(projected_allocated)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        hooks.write_attempts = hooks
            .write_attempts
            .checked_add(1)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::EntryLimit, stage))?;
        temp.write_all(&buffer[..read])
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
        let written_allocated = temp
            .metadata()
            .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?
            .blocks()
            .checked_mul(512)
            .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
        if existing_allocated_bytes
            .checked_add(written_allocated)
            .is_none_or(|bytes| bytes > max_bytes)
        {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        hasher.update(&buffer[..read]);
        offset = next_offset;
        if offset > max_bytes {
            return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
        }
        if hooks
            .mutate_after_bytes
            .is_some_and(|limit| offset >= limit)
        {
            if let Some(file) = hooks.mutation_file.as_ref() {
                file.write_at(b"mutate", 0)
                    .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
            }
            hooks.mutate_after_bytes = None;
            hooks.mutation_file = None;
        }
    }
    if offset != source.record.logical_bytes {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let digest = format!("{:x}", hasher.finalize());
    if digest != source.record.sha256 {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    retention_sync_file(temp, hooks, stage)?;
    let synced_allocated = temp
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    if existing_allocated_bytes
        .checked_add(synced_allocated)
        .is_none_or(|bytes| bytes > max_bytes)
    {
        return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
    }
    let _ = revalidate_research_file(source, max_bytes)?;
    Ok((offset, digest))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retention_record(
    chain: &RetentionChain,
    campaign_id: &str,
    review_id: &str,
    snapshot: ResearchLeafSnapshot,
    digest: String,
) -> ResearchFileRecord {
    ResearchFileRecord {
        relative_path: format!(
            "{RESEARCH_CHECKPOINT_NAMESPACE}/{campaign_id}/{review_id}/{RESEARCH_CHECKPOINT_LEAF}"
        ),
        root: chain.root_record.clone(),
        parent: chain.review_record.clone(),
        device: snapshot.device,
        inode: snapshot.inode,
        owner: snapshot.owner,
        mode: snapshot.mode,
        mount_identity: snapshot.mount_identity.0,
        logical_bytes: snapshot.logical_bytes,
        allocated_bytes: snapshot.allocated_bytes,
        sha256: digest,
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn expected_retention_path(
    campaign_id: &str,
    review_id: &str,
    expected: &ResearchFileRecord,
) -> Result<(), PolicyViolation> {
    let expected_path = format!(
        "{RESEARCH_CHECKPOINT_NAMESPACE}/{campaign_id}/{review_id}/{RESEARCH_CHECKPOINT_LEAF}"
    );
    if expected.relative_path != expected_path {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn publish_retention_temp(
    review: &File,
    temp_name: &OsStr,
    temp: &File,
    temp_snapshot: ResearchLeafSnapshot,
    mount: MountIdentity,
    hooks: &mut RetentionTestHooks,
    stage: PolicyViolationStage,
) -> Result<OwnedRetentionPublication, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    if hooks.fail_publication {
        hooks.fail_publication = false;
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let checkpoint = std::ffi::CString::new(RESEARCH_CHECKPOINT_LEAF)
        .expect("literal contains no NUL");
    let temp_name_c = std::ffi::CString::new(temp_name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    if unsafe {
        libc::linkat(
            review.as_raw_fd(),
            temp_name_c.as_ptr(),
            review.as_raw_fd(),
            checkpoint.as_ptr(),
            0,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        let reason = if error.raw_os_error() == Some(libc::EEXIST) {
            TempUnsafeReason::ExistingEntry
        } else {
            TempUnsafeReason::IoFailure
        };
        return Err(temp_violation_at(reason, stage));
    }
    let publication = OwnedRetentionPublication {
        device: temp_snapshot.device,
        inode: temp_snapshot.inode,
        mount,
    };
    let entry = match entry_metadata_at(review, temp_name, stage) {
        Ok(entry) => entry,
        Err(error) => {
            let error = retention_publication_failure(
                error,
                review,
                temp_name,
                temp,
                publication,
                hooks,
                stage,
            );
            return Err(error);
        }
    };
    let metadata = match temp.metadata() {
        Ok(metadata) => metadata,
        Err(_) => {
            let error = retention_publication_failure(
                temp_violation_at(TempUnsafeReason::IoFailure, stage),
                review,
                temp_name,
                temp,
                publication,
                hooks,
                stage,
            );
            return Err(error);
        }
    };
    use std::os::unix::fs::MetadataExt;
    if entry.mount_identity != mount
        || entry.kind != AuditedEntryKind::Leaf
        || entry.identity != (metadata.dev(), metadata.ino())
        || entry.identity != (publication.device, publication.inode)
    {
        let error = retention_publication_failure(
            temp_violation_at(TempUnsafeReason::IdentityChanged, stage),
            review,
            temp_name,
            temp,
            publication,
            hooks,
            stage,
        );
        return Err(error);
    }
    if let Err(error) = unlinkat(review, temp_name, false) {
        let error = retention_publication_failure(
            error,
            review,
            temp_name,
            temp,
            publication,
            hooks,
            stage,
        );
        return Err(error);
    }
    if let Err(error) = retention_sync_directory(review, hooks, stage) {
        let error = retention_publication_failure(
            error,
            review,
            temp_name,
            temp,
            publication,
            hooks,
            stage,
        );
        return Err(error);
    }
    Ok(publication)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn retain_verified_research_file_impl(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    source: &VerifiedResearchFile,
    hooks: &mut RetentionTestHooks,
) -> Result<RetainedResearchFile, PolicyViolation> {
    validate_research_id(campaign_id)?;
    validate_research_id(review_id)?;
    let stage = retention_stage();
    let quota = hooks.quota_bytes.min(MAX_PRIVATE_TEMP_ALLOCATED_BYTES);
    let (mut chain, campaign_lock) = acquire_retention_namespace(
        policy,
        campaign_id,
        review_id,
        true,
        false,
        stage,
        Some(hooks),
    )?;
    let inventory = retention_inventory(&chain, quota, stage)?;
    let source_leaf = research_leaf_snapshot(
        &source.file,
        MountIdentity(source.record.mount_identity),
        quota,
        stage,
    )?;
    if source_leaf.logical_bytes != source.record.logical_bytes
        || source_leaf.allocated_bytes != source.record.allocated_bytes
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let logical_with_source = inventory
        .logical_bytes
        .checked_add(source_leaf.logical_bytes)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    let allocated_with_source = inventory
        .allocated_bytes
        .checked_add(source_leaf.allocated_bytes)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    if logical_with_source > quota || allocated_with_source > quota {
        return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
    }
    let temp_name = generated_retention_temp_name();
    let mut temp = open_retention_temp(&chain.review, &temp_name, stage)?;
    let (logical_bytes, digest) = match copy_research_source(
        source,
        &mut temp,
        quota,
        inventory.logical_bytes,
        inventory.allocated_bytes,
        hooks,
        stage,
    ) {
        Ok(result) => result,
        Err(error) => {
            cleanup_owned_retention_temp(
                &chain.review,
                &temp_name,
                &temp,
                chain.mount,
                stage,
            );
            return Err(error);
        }
    };
    let temp_snapshot = match research_leaf_snapshot(
        &temp,
        chain.mount,
        quota,
        stage,
    ) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            cleanup_owned_retention_temp(
                &chain.review,
                &temp_name,
                &temp,
                chain.mount,
                stage,
            );
            return Err(error);
        }
    };
    if temp_snapshot.mode != 0o600
        || temp_snapshot.links != 1
        || temp_snapshot.logical_bytes != logical_bytes
    {
        cleanup_owned_retention_temp(
            &chain.review,
            &temp_name,
            &temp,
            chain.mount,
            stage,
        );
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    if inventory
        .logical_bytes
        .checked_add(temp_snapshot.logical_bytes)
        .is_none_or(|bytes| bytes > quota)
        || inventory
            .allocated_bytes
            .checked_add(temp_snapshot.allocated_bytes)
            .is_none_or(|bytes| bytes > quota)
    {
        cleanup_owned_retention_temp(
            &chain.review,
            &temp_name,
            &temp,
            chain.mount,
            stage,
        );
        return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
    }
    let pre_publish_chain = match open_retention_chain(policy, campaign_id, review_id, false, stage) {
        Ok(chain) => chain,
        Err(error) => {
            cleanup_owned_retention_temp(
                &chain.review,
                &temp_name,
                &temp,
                chain.mount,
                stage,
            );
            return Err(error);
        }
    };
    if !chain.records_equal(&pre_publish_chain) {
        cleanup_owned_retention_temp(
            &chain.review,
            &temp_name,
            &temp,
            chain.mount,
            stage,
        );
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    chain = pre_publish_chain;
    let publication = match publish_retention_temp(
        &chain.review,
        &temp_name,
        &temp,
        temp_snapshot,
        chain.mount,
        hooks,
        stage,
    ) {
        Ok(publication) => publication,
        Err(error) => {
            cleanup_owned_retention_temp(
                &chain.review,
                &temp_name,
                &temp,
                chain.mount,
                stage,
            );
            return Err(error);
        }
    };
    let final_file = match open_research_file_on_mount(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        chain.mount,
        stage,
    ) {
        Ok(file) => file,
        Err(error) => {
            return retain_postpublication_failure(
                error,
                &chain.review,
                &temp_name,
                &temp,
                publication,
                hooks,
                stage,
            );
        }
    };
    let final_snapshot = match research_leaf_snapshot(&final_file, chain.mount, quota, stage) {
        Ok(snapshot) => snapshot,
        Err(error) => {
            return retain_postpublication_failure(
                error,
                &chain.review,
                &temp_name,
                &temp,
                publication,
                hooks,
                stage,
            );
        }
    };
    if final_snapshot.mode != 0o600
        || final_snapshot.links != 1
        || final_snapshot.device != temp_snapshot.device
        || final_snapshot.inode != temp_snapshot.inode
        || final_snapshot.owner != temp_snapshot.owner
        || final_snapshot.mount_identity != temp_snapshot.mount_identity
        || final_snapshot.logical_bytes != temp_snapshot.logical_bytes
        || final_snapshot.allocated_bytes != temp_snapshot.allocated_bytes
    {
        return retain_postpublication_failure(
            temp_violation_at(TempUnsafeReason::InvalidEntry, stage),
            &chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    let (hashed_bytes, hashed_digest) = match hash_research_file(&final_file, quota, stage) {
        Ok(result) => result,
        Err(error) => {
            return retain_postpublication_failure(
                error,
                &chain.review,
                &temp_name,
                &temp,
                publication,
                hooks,
                stage,
            );
        }
    };
    if hashed_bytes != logical_bytes || hashed_digest != digest {
        return retain_postpublication_failure(
            temp_violation_at(TempUnsafeReason::IdentityChanged, stage),
            &chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    if hooks.fail_after_publication_validation {
        hooks.fail_after_publication_validation = false;
        return retain_postpublication_failure(
            temp_violation_at(TempUnsafeReason::IoFailure, stage),
            &chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    if let Err(error) = revalidate_research_file(source, quota) {
        return retain_postpublication_failure(
            error,
            &chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    let post_chain = match open_retention_chain(policy, campaign_id, review_id, false, stage) {
        Ok(chain) => chain,
        Err(error) => {
            return retain_postpublication_failure(
                error,
                &chain.review,
                &temp_name,
                &temp,
                publication,
                hooks,
                stage,
            );
        }
    };
    if !chain.records_equal(&post_chain) {
        return retain_postpublication_failure(
            temp_violation_at(TempUnsafeReason::IdentityChanged, stage),
            &chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    chain = post_chain;
    let record = retention_record(&chain, campaign_id, review_id, final_snapshot, digest);
    let rollback_chain = RetentionChainIdentity::from_chain(&chain);
    let rollback_review = match chain.review.try_clone() {
        Ok(review) => review,
        Err(_) => {
            return retain_postpublication_failure(
                temp_violation_at(TempUnsafeReason::IoFailure, stage),
                &chain.review,
                &temp_name,
                &temp,
                publication,
                hooks,
                stage,
            );
        }
    };
    let mut retained = RetainedResearchFile {
        file: final_file,
        chain,
        campaign_lock,
        campaign_id: campaign_id.to_owned(),
        review_id: review_id.to_owned(),
        record,
    };
    if let Err(error) = reverify_retained_research_file(policy, &retained) {
        return retain_postpublication_failure(
            error,
            &retained.chain.review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    #[cfg(all(test, unix))]
    let downgrade_result = downgrade_retention_campaign_for_test(
        &mut retained.campaign_lock,
        policy,
        campaign_id,
        hooks,
        stage,
    );
    #[cfg(not(all(test, unix)))]
    let downgrade_result = retained.campaign_lock.downgrade_shared(stage);
    if let Err(error) = downgrade_result {
        drop(retained);
        return rollback_after_downgrade_failure(
            error,
            policy,
            campaign_id,
            review_id,
            &rollback_chain,
            &rollback_review,
            &temp_name,
            &temp,
            publication,
            hooks,
            stage,
        );
    }
    Ok(retained)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn reverify_retained_research_file(
    policy: &ResolvedExecutionPolicy,
    retained: &RetainedResearchFile,
) -> Result<(), PolicyViolation> {
    let stage = retention_stage();
    policy.verify_code_change_state_root()?;
    let current_root = research_directory_record_at(
        &retained.chain.root,
        retained.chain.mount,
        stage,
    )?;
    if current_root != retained.chain.root_record || current_root != retained.record.root {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let current_namespace = research_directory_record_at(
        &retained.chain.namespace,
        retained.chain.mount,
        stage,
    )?;
    let current_campaign = research_directory_record_at(
        &retained.chain.campaign,
        retained.chain.mount,
        stage,
    )?;
    let current_review = research_directory_record_at(
        &retained.chain.review,
        retained.chain.mount,
        stage,
    )?;
    if current_namespace != retained.chain.namespace_record
        || current_campaign != retained.chain.campaign_record
        || current_review != retained.chain.review_record
        || current_review != retained.record.parent
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let fresh = open_retention_chain(
        policy,
        &retained.campaign_id,
        &retained.review_id,
        false,
        stage,
    )?;
    if !retained.chain.records_equal(&fresh) {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let snapshot = research_leaf_snapshot(
        &retained.file,
        retained.chain.mount,
        retained.record.logical_bytes,
        stage,
    )?;
    research_leaf_matches_record(snapshot, &retained.record, 1, stage)?;
    let (bytes, digest) = hash_research_file(&retained.file, retained.record.logical_bytes, stage)?;
    if bytes != retained.record.logical_bytes || digest != retained.record.sha256 {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let named = open_research_file_on_mount(
        &fresh.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        fresh.mount,
        stage,
    )?;
    let named_snapshot = research_leaf_snapshot(
        &named,
        fresh.mount,
        retained.record.logical_bytes,
        stage,
    )?;
    research_leaf_matches_record(named_snapshot, &retained.record, 1, stage)?;
    let (named_bytes, named_digest) = hash_research_file(&named, retained.record.logical_bytes, stage)?;
    if named_bytes != bytes || named_digest != digest {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn reopen_retained_research_file(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    expected: &ResearchFileRecord,
) -> Result<RetainedResearchFile, PolicyViolation> {
    validate_research_id(campaign_id)?;
    validate_research_id(review_id)?;
    expected_retention_path(campaign_id, review_id, expected)?;
    let stage = retention_stage();
    let (chain, campaign_lock) = acquire_retention_namespace(
        policy,
        campaign_id,
        review_id,
        false,
        true,
        stage,
        None,
    )?;
    let _ = retention_inventory(&chain, MAX_PRIVATE_TEMP_ALLOCATED_BYTES, stage)?;
    if expected.root != chain.root_record || expected.parent != chain.review_record {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let file = open_research_file_on_mount(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        chain.mount,
        stage,
    )?;
    let snapshot = research_leaf_snapshot(&file, chain.mount, expected.logical_bytes, stage)?;
    research_leaf_matches_record(snapshot, expected, 1, stage)?;
    let (bytes, digest) = hash_research_file(&file, expected.logical_bytes, stage)?;
    if bytes != expected.logical_bytes || digest != expected.sha256 {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let retained = RetainedResearchFile {
        file,
        chain,
        campaign_lock,
        campaign_id: campaign_id.to_owned(),
        review_id: review_id.to_owned(),
        record: expected.clone(),
    };
    reverify_retained_research_file(policy, &retained)?;
    Ok(retained)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn cleanup_retained_research_file(
    policy: &ResolvedExecutionPolicy,
    campaign_id: &str,
    review_id: &str,
    expected: &ResearchFileRecord,
) -> Result<(), PolicyViolation> {
    validate_research_id(campaign_id)?;
    validate_research_id(review_id)?;
    expected_retention_path(campaign_id, review_id, expected)?;
    let stage = retention_stage();
    let (chain, _campaign_lock) = acquire_retention_namespace(
        policy,
        campaign_id,
        review_id,
        false,
        false,
        stage,
        None,
    )?;
    let _ = retention_inventory(&chain, MAX_PRIVATE_TEMP_ALLOCATED_BYTES, stage)?;
    if expected.root != chain.root_record || expected.parent != chain.review_record {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    if retention_leaf_entry(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        stage,
    )?
    .is_none()
    {
        let mut hooks = RetentionTestHooks::default();
        retention_sync_directory(&chain.review, &mut hooks, stage)?;
        if retention_leaf_entry(
            &chain.review,
            OsStr::new(RESEARCH_CHECKPOINT_LEAF),
            stage,
        )?
        .is_some()
        {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        let after = open_retention_chain(policy, campaign_id, review_id, false, stage)?;
        if !chain.records_equal(&after) {
            return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
        }
        return Ok(());
    }
    let file = open_research_file_on_mount(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        chain.mount,
        stage,
    )?;
    let snapshot = research_leaf_snapshot(&file, chain.mount, expected.logical_bytes, stage)?;
    research_leaf_matches_record(snapshot, expected, 1, stage)?;
    let (bytes, digest) = hash_research_file(&file, expected.logical_bytes, stage)?;
    if bytes != expected.logical_bytes || digest != expected.sha256 {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let before_unlink = open_retention_chain(policy, campaign_id, review_id, false, stage)?;
    if !chain.records_equal(&before_unlink) {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    let chain = before_unlink;
    let entry = entry_metadata_at(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        stage,
    )?;
    if entry.mount_identity != chain.mount
        || entry.kind != AuditedEntryKind::Leaf
        || entry.identity != (snapshot.device, snapshot.inode)
    {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    unlinkat(
        &chain.review,
        OsStr::new(RESEARCH_CHECKPOINT_LEAF),
        false,
    )?;
    let mut hooks = RetentionTestHooks::default();
    retention_sync_directory(&chain.review, &mut hooks, stage)?;
    let after = open_retention_chain(policy, campaign_id, review_id, false, stage)?;
    if !chain.records_equal(&after) {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(())
}

/// Metadata-only evidence discovered relative to a retained project-root
/// descriptor. File contents are deliberately outside this projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DecisionArtifactHint {
    pub path: String,
    pub size: u64,
    pub mtime: i64,
}

/// Discover bounded regular-file metadata without reopening the project by
/// pathname or following symlinks, special files, or mount changes.
pub fn collect_decision_artifact_hints(
    root_anchor: &crate::execution_policy::ProjectRootAnchor,
    max_hints: usize,
    max_depth: usize,
    max_field_bytes: usize,
) -> Result<Vec<DecisionArtifactHint>, crate::AppError> {
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (root_anchor, max_hints, max_depth, max_field_bytes);
        return Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .into());
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if max_hints == 0 || max_depth == 0 || max_field_bytes == 0 {
            return Ok(Vec::new());
        }
        let verified = root_anchor.verify_identity()?;
        let root_mount = directory_mount_identity_at(
            &verified.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = ArtifactHintScan {
            entries: 0,
            hints: Vec::with_capacity(max_hints.min(64)),
            max_hints,
            max_depth,
            max_field_bytes,
            root_owner: root_anchor.identity.owner,
            root_mount,
        };
        let result = scan_artifact_hints(&verified.directory, "", 0, &mut state);
        finish_artifact_scan_with_mount_reader(
            root_anchor,
            root_mount,
            result,
            |directory| {
                directory_mount_identity_at(
                    directory,
                    PolicyViolationStage::RunBoundPreMarker,
                )
            },
        )?;
        state.hints.sort_unstable_by(|left, right| {
            left.path
                .cmp(&right.path)
                .then_with(|| left.size.cmp(&right.size))
                .then_with(|| left.mtime.cmp(&right.mtime))
        });
        state.hints.truncate(max_hints);
        Ok(state.hints)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn finish_artifact_scan_with_mount_reader<F>(
    root_anchor: &crate::execution_policy::ProjectRootAnchor,
    captured_mount: MountIdentity,
    scan_result: Result<(), PolicyViolation>,
    mount_reader: F,
) -> Result<(), PolicyViolation>
where
    F: FnOnce(&File) -> Result<MountIdentity, PolicyViolation>,
{
    let verified = root_anchor.verify_identity()?;
    if mount_reader(&verified.directory)? != captured_mount {
        return Err(temp_violation(TempUnsafeReason::MountBoundary));
    }
    scan_result
}

const BASELINE_NAMES: &[&str] = &[
    "HOME",
    "PATH",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "TMPDIR",
    "TMP",
    "TEMP",
    "PUEUE_AGENT_RUN_ID",
    "PUEUE_AGENT_PROJECT_ID",
];

const PROXY_AND_CERT_NAMES: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "AWS_CA_BUNDLE",
];

const CODEX_AUTH_NAMES: &[&str] = &[
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "CODEX_AUTH_TOKEN",
];

pub(crate) fn is_proxy_or_cert_name(name: &str) -> bool {
    PROXY_AND_CERT_NAMES.contains(&name)
}

/// Names used by the Codex shell environment filter.  This deliberately
/// contains a fixed non-secret baseline even when a project has no task
/// allowlist, so an empty filter can never accidentally mean "inherit all".
pub(crate) fn shell_baseline_names() -> &'static [&'static str] {
    BASELINE_NAMES
}

/// Conservative authentication-name denial.  The explicit names cover known
/// credentials; the suffix/prefix rules cover newly introduced credential
/// variables without relying on a single heuristic.
pub(crate) fn is_auth_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "OPENAI_API_KEY"
            | "CODEX_API_KEY"
            | "CODEX_AUTH_TOKEN"
            | "CODEX_HOME"
            | "AWS_ACCESS_KEY_ID"
            | "AWS_SECRET_ACCESS_KEY"
            | "GITHUB_TOKEN"
            | "GH_TOKEN"
            | "NPM_TOKEN"
            | "AUTH_TOKEN"
            | "ACCESS_TOKEN"
            | "REFRESH_TOKEN"
            | "API_KEY"
            | "GOOGLE_APPLICATION_CREDENTIALS"
            | "DOCKER_AUTH_CONFIG"
            | "REGISTRY_AUTH_FILE"
            | "KUBECONFIG"
            | "AZURE_CLIENT_ID"
            | "AZURE_CLIENT_SECRET"
            | "AWS_SESSION_TOKEN"
            | "AWS_SECURITY_TOKEN"
            | "AWS_PROFILE"
            | "AWS_SHARED_CREDENTIALS_FILE"
            | "AWS_CONFIG_FILE"
            | "AWS_WEB_IDENTITY_TOKEN_FILE"
            | "AWS_ROLE_ARN"
            | "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"
            | "AWS_CONTAINER_CREDENTIALS_FULL_URI"
            | "GOOGLE_CLOUD_KEYFILE_JSON"
            | "GOOGLE_GHA_CREDS_PATH"
            | "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES"
            | "CLOUDSDK_AUTH_ACCESS_TOKEN"
            | "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE"
            | "CLOUDSDK_CONFIG"
            | "BOTO_CONFIG"
            | "AZURE_TENANT_ID"
            | "AZURE_SUBSCRIPTION_ID"
            | "AZURE_FEDERATED_TOKEN_FILE"
            | "AZURE_USERNAME"
            | "AZURE_PASSWORD"
            | "AZURE_CONFIG_DIR"
            | "AZURE_AUTH_LOCATION"
            | "AZURE_STORAGE_KEY"
            | "AZURE_STORAGE_CONNECTION_STRING"
            | "DOCKER_CONFIG"
            | "CONTAINER_AUTH_FILE"
            | "BUILDAH_AUTH"
            | "HELM_KUBECONTEXT"
            | "HELM_CONFIG_HOME"
            | "HELM_DATA_HOME"
            | "HELM_CACHE_HOME"
            | "SSH_AUTH_SOCK"
            | "GIT_ASKPASS"
            | "GIT_SSH_COMMAND"
            | "GIT_CREDENTIAL_HELPER"
            | "GCM_INTERACTIVE"
    ) || upper.ends_with("_API_KEY")
        || upper.ends_with("_TOKEN")
        || upper.ends_with("_SECRET")
        || upper.ends_with("_PASSWORD")
        || upper.ends_with("_PRIVATE_KEY")
        || upper.ends_with("_CREDENTIAL")
        || upper.ends_with("_CREDENTIALS")
        || upper.starts_with("GIT_CONFIG_")
        || upper.starts_with("GCM_")
        || upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("PRIVATE_KEY")
        || upper.starts_with("AWS_SECRET_")
        || upper.starts_with("AZURE_CLIENT_SECRET")
        || upper.starts_with("AZURE_STORAGE_")
        || (upper.contains("STORAGE")
            && (upper.contains("KEY")
                || upper.contains("SECRET")
                || upper.contains("TOKEN")
                || upper.contains("CREDENTIAL")
                || upper.contains("PASSWORD")
                || upper.contains("CONNECTION")))
}

/// A captured, explicit child environment.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SanitizedEnvironment {
    values: BTreeMap<OsString, OsString>,
}

impl SanitizedEnvironment {
    pub(crate) fn entries(&self) -> impl Iterator<Item = (&OsStr, &OsStr)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
    }

    pub fn get(&self, name: &str) -> Option<&OsStr> {
        self.values.get(OsStr::new(name)).map(OsString::as_os_str)
    }

    pub fn names(&self) -> impl Iterator<Item = &OsStr> {
        self.values.keys().map(OsString::as_os_str)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Replace the command's inherited environment and then add exactly the
    /// captured/generated values in this object.
    pub fn apply<C: EnvironmentCommand>(&self, command: &mut C) {
        command.environment_clear();
        for (name, value) in &self.values {
            command.environment_set(name, value);
        }
    }

    pub fn for_codex_agent(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, true)?;
        environment.insert_generated("CODEX_HOME", policy.codex_home.as_os_str());
        for name in CODEX_AUTH_NAMES {
            if let Some(value) = startup.get(name) {
                environment.insert_os(OsString::from(name), value.to_os_string());
            }
        }
        Ok(environment)
    }

    pub(crate) fn for_codex_decision(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, true)?;
        environment.insert_generated("CODEX_HOME", policy.codex_home.as_os_str());
        Ok(environment)
    }

    pub fn for_custom_agent(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, false)?;
        environment.copy_allowlist(startup, &policy.agent_environment_allow);
        Ok(environment)
    }

    pub fn for_codex_task(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::project_baseline(startup, policy, run_id, false)?;
        environment.copy_allowlist(startup, &policy.task_environment_allow);
        Ok(environment)
    }

    /// Environment for a code-change editor.  The editor receives only
    /// supervisor-generated protocol values in addition to the normal
    /// built-in/custom baseline; descriptor-relative paths prevent a custom
    /// editor from selecting an arbitrary output file.
    pub fn for_code_change_editor(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
        session_id: &str,
        resume: bool,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = match policy.agent_kind {
            crate::execution_policy::AgentKind::BuiltInCodex => {
                Self::for_codex_agent(startup, policy, run_id)?
            }
            crate::execution_policy::AgentKind::Custom => {
                Self::for_custom_agent(startup, policy, run_id)?
            }
        };
        if session_id.is_empty()
            || session_id.len() > 128
            || session_id.chars().any(char::is_control)
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::EnvironmentName,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        environment.insert_generated(
            "PUEUE_AGENT_EDITOR_MODE",
            OsStr::new(if resume { "resume" } else { "fresh" }),
        );
        environment.insert_generated(
            "PUEUE_AGENT_EDITOR_SESSION_ID",
            OsString::from(session_id),
        );
        environment.insert_generated(
            "PUEUE_AGENT_EDITOR_SCHEMA",
            private_temp_target_path().join("editor-schema.json").into_os_string(),
        );
        environment.insert_generated(
            "PUEUE_AGENT_EDITOR_OUTPUT",
            private_temp_target_path().join("editor.json").into_os_string(),
        );
        Ok(environment)
    }

    pub fn for_pueue(policy: &ResolvedExecutionPolicy) -> Result<Self, PolicyViolation> {
        let environment = Self::default_baseline(
            &policy.startup_environment,
            &policy.trusted_path,
            None,
            None,
            false,
        )?;
        // The caller launches the startup-pinned executable itself.  Keep the
        // anchor in this API to make it impossible to accidentally substitute
        // an ambient Pueue path while preparing the environment, but do not
        // expose the path as a child variable.
        let _ = &policy.pueue_anchor;
        Ok(environment)
    }

    /// Environment for service-owned Git/tool invocations.  It starts from
    /// the same default-deny baseline as Pueue and adds only fixed controls
    /// that disable configuration, authentication, paging, and signing
    /// channels.  Startup credentials are never copied here.
    pub fn for_code_change_tool(
        policy: &ResolvedExecutionPolicy,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::for_pueue(policy)?;
        for (name, value) in [
            ("LANG", "C"),
            ("LC_ALL", "C"),
            ("GIT_CONFIG_NOSYSTEM", "1"),
            ("GIT_CONFIG_SYSTEM", "/dev/null"),
            ("GIT_CONFIG_GLOBAL", "/dev/null"),
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_PAGER", "cat"),
            ("PAGER", "cat"),
            ("GIT_ASKPASS", "/bin/false"),
            ("SSH_ASKPASS", "/bin/false"),
            ("GIT_EDITOR", "/bin/false"),
            ("GIT_SEQUENCE_EDITOR", "/bin/false"),
        ] {
            environment.insert_generated(name, OsStr::new(value));
        }
        Ok(environment)
    }

    pub(crate) fn with_generated(&mut self, name: &str, value: impl Into<OsString>) {
        self.insert_generated(name, value);
    }

    fn project_baseline(
        startup: &StartupEnvironment,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
        include_proxy_cert: bool,
    ) -> Result<Self, PolicyViolation> {
        validate_run_id(run_id)?;
        Self::default_baseline(
            startup,
            &policy.trusted_path,
            Some(OsStr::new(PRIVATE_TEMP_TARGET_PATH)),
            Some((run_id, policy.project_id.as_str())),
            include_proxy_cert,
        )
    }

    fn default_baseline(
        startup: &StartupEnvironment,
        trusted_path: &[PathBuf],
        temp_path: Option<&OsStr>,
        ids: Option<(i64, &str)>,
        include_proxy_cert: bool,
    ) -> Result<Self, PolicyViolation> {
        let mut environment = Self::default();
        for name in ["HOME"] {
            environment.copy_startup_name(startup, name);
        }
        if trusted_path.is_empty() {
            if let Some(path) = startup.get("PATH") {
                environment.insert_os(OsString::from("PATH"), path.to_os_string());
            }
        } else {
            environment.insert_generated("PATH", join_trusted_path(trusted_path)?);
        }
        for name in ["LANG", "LC_ALL", "LC_CTYPE"] {
            environment.insert_generated(name, OsStr::new("C"));
        }
        if let Some(temp_path) = temp_path {
            for name in ["TMPDIR", "TMP", "TEMP"] {
                environment.insert_generated(name, temp_path);
            }
        } else {
            for name in ["TMPDIR", "TMP", "TEMP"] {
                environment.copy_startup_name(startup, name);
            }
        }
        if include_proxy_cert {
            for name in PROXY_AND_CERT_NAMES {
                environment.copy_startup_name(startup, name);
            }
        }
        if let Some((run_id, project_id)) = ids {
            environment.insert_generated("PUEUE_AGENT_RUN_ID", OsString::from(run_id.to_string()));
            environment.insert_generated("PUEUE_AGENT_PROJECT_ID", OsString::from(project_id));
        }
        Ok(environment)
    }

    fn copy_allowlist(&mut self, startup: &StartupEnvironment, allowlist: &BTreeSet<String>) {
        for name in allowlist {
            if !is_auth_name(name)
                && !is_proxy_or_cert_name(name)
            {
                if let Some(value) = startup.get(name) {
                    self.insert_os(OsString::from(name), value.to_os_string());
                }
            }
        }
    }

    fn copy_startup_name(&mut self, startup: &StartupEnvironment, name: &str) {
        if let Some(value) = startup.get(name) {
            self.insert_os(OsString::from(name), value.to_os_string());
        }
    }

    fn insert_generated(&mut self, name: &str, value: impl Into<OsString>) {
        self.values.insert(OsString::from(name), value.into());
    }

    pub(crate) fn apply_campaign_experiment_variables(
        &mut self,
        variables: &[(String, OsString)],
    ) {
        for (name, value) in variables {
            self.insert_generated(name, value.as_os_str());
        }
    }

    fn insert_os(&mut self, name: OsString, value: OsString) {
        self.values.insert(name, value);
    }
}

impl fmt::Debug for SanitizedEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SanitizedEnvironment")
            .field("names", &self.values.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl fmt::Display for SanitizedEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "sanitized environment ({} names)", self.values.len())
    }
}

/// The small command surface needed by `SanitizedEnvironment::apply`.
pub trait EnvironmentCommand {
    fn environment_clear(&mut self);
    fn environment_set(&mut self, name: &OsStr, value: &OsStr);
}

impl EnvironmentCommand for std::process::Command {
    fn environment_clear(&mut self) {
        self.env_clear();
    }

    fn environment_set(&mut self, name: &OsStr, value: &OsStr) {
        self.env(name, value);
    }
}

impl EnvironmentCommand for tokio::process::Command {
    fn environment_clear(&mut self) {
        self.env_clear();
    }

    fn environment_set(&mut self, name: &OsStr, value: &OsStr) {
        self.env(name, value);
    }
}

fn join_trusted_path(paths: &[PathBuf]) -> Result<OsString, PolicyViolation> {
    std::env::join_paths(paths).map_err(|_| environment_error())
}

fn validate_run_id(run_id: i64) -> Result<(), PolicyViolation> {
    let text = run_id.to_string();
    if run_id <= 0 || run_id > MAX_PRIVATE_TEMP_RUN_ID || text.len() > MAX_RUN_ID_BYTES {
        Err(PolicyViolation::new(
            PolicyViolationCode::TempUnsafe,
            PolicyViolationStage::RunBoundPreMarker,
        ))
    } else {
        Ok(())
    }
}

fn environment_error() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::EnvironmentName,
        PolicyViolationStage::PreBinding,
    )
}

fn temp_error() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::TempUnsafe,
        PolicyViolationStage::RunBoundPreMarker,
    )
}

pub struct ProjectAdmissionLock {
    directory: File,
}

/// Cross-process serialization for the durable run-ID floor and allocator.
/// The guard flocks a fresh descriptor opened relative to the retained
/// database-parent capability, so no replaceable lock-file leaf is trusted.
/// The descriptor is close-on-exec and never held across native launch.
pub struct RunIdAdmissionGuard {
    file: File,
}

impl RunIdAdmissionGuard {
    pub(crate) fn try_acquire(
        parent: &File,
    ) -> Result<Option<Self>, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = parent;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            let name = std::ffi::CString::new(".").expect("literal contains no NUL");
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY
                        | libc::O_DIRECTORY
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::PreBinding,
                ));
            }
            let file = unsafe { File::from_raw_fd(fd) };
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if result == 0 {
                return Ok(Some(Self { file }));
            }
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            Err(PolicyViolation::new(PolicyViolationCode::TempUnsafe, PolicyViolationStage::PreBinding))
        }
    }
}

impl Drop for RunIdAdmissionGuard {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

impl ProjectAdmissionLock {
    pub(crate) fn try_acquire(
        root: &VerifiedProjectRoot,
    ) -> Result<Option<Self>, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::{AsRawFd, FromRawFd};
            let name = std::ffi::CString::new(".").expect("literal contains no NUL");
            let fd = unsafe {
                libc::openat(
                    root.directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY
                        | libc::O_DIRECTORY
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW,
                )
            };
            if fd < 0 {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::PreBinding,
                ));
            }
            let directory = unsafe { File::from_raw_fd(fd) };
            let result = unsafe {
                libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB)
            };
            if result == 0 {
                return Ok(Some(Self { directory }));
            }
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EWOULDBLOCK) {
                return Ok(None);
            }
            Err(PolicyViolation::new(
                PolicyViolationCode::TempUnsafe,
                PolicyViolationStage::PreBinding,
            ))
        }
    }
}

impl Drop for ProjectAdmissionLock {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.directory.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

/// An exclusive, owner-only per-run directory.
pub struct PrivateRunTemp {
    name: OsString,
    path: PathBuf,
    parent: File,
    directory: File,
    identity: (u64, u64),
    recovery_identity: PrivateRunTempRecoveryIdentityV1,
    #[cfg(target_os = "linux")]
    decision_output_anchor: Mutex<Option<DecisionOutputAnchor>>,
    #[cfg(target_os = "linux")]
    research_stdout_anchor: Mutex<Option<DecisionOutputAnchor>>,
    #[cfg(target_os = "linux")]
    research_stderr_anchor: Mutex<Option<DecisionOutputAnchor>>,
}

/// The immutable filesystem identity needed to reopen one native research
/// generation after a supervisor restart.  The V1 type is intentionally
/// path-free: the startup root anchor supplies the trusted pathname and the
/// identities prove that every reopened descriptor is the original object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateRunTempRecoveryIdentityV1 {
    pub service_root_identity: PrivateRunTempRecoveryRootIdentity,
    pub temp_identity: PrivateRunTempRecoveryTempIdentity,
}

impl PrivateRunTempRecoveryIdentityV1 {
    pub const VERSION: u8 = 1;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateRunTempRecoveryRootIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub mode: u32,
    #[serde(rename = "resolution")]
    pub resolution_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateRunTempRecoveryTempIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub mode: u32,
    #[serde(rename = "mount")]
    pub mount_identity: [u64; 2],
    pub service_identity: PrivateRunTempRecoveryDirectoryIdentity,
    pub parent_identity: PrivateRunTempRecoveryDirectoryIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateRunTempRecoveryDirectoryIdentity {
    pub device: u64,
    pub inode: u64,
    pub owner: u32,
    pub mode: u32,
}

/// A cleanup-only capability for a previously recorded native research
/// generation.  It deliberately has no output anchors and cannot create or
/// adopt a new generation.
#[derive(Debug)]
pub struct RecoveredPrivateRunTempCleanup {
    anchor: crate::execution_policy::ProjectRootAnchor,
    expected: PrivateRunTempRecoveryIdentityV1,
    name: OsString,
    service: File,
    parent: File,
    directory: File,
}

/// An opaque, verified directory capability for the native target's private
/// temporary directory role. It has no raw-descriptor or path constructor.
/// This capability proves only the directory itself; descriptor-relative
/// descendant containment remains outside this launch ABI's scope.
pub(crate) struct VerifiedPrivateTemp {
    pub(crate) directory: File,
    pub(crate) identity: ExecutableIdentity,
    run_id: i64,
    #[cfg(unix)]
    pub(crate) research_streams: Option<VerifiedResearchStreams>,
}

#[cfg(unix)]
pub(crate) struct VerifiedResearchStreams {
    pub(crate) stdout: File,
    pub(crate) stdout_identity: LogFileIdentity,
    pub(crate) stderr: File,
    pub(crate) stderr_identity: LogFileIdentity,
}

impl VerifiedPrivateTemp {
    pub(crate) fn target_path(&self) -> &'static Path {
        private_temp_target_path()
    }
}

impl fmt::Debug for VerifiedPrivateTemp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedPrivateTemp")
            .field("role", &"private-temp-target")
            .field("run_id", &self.run_id)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempCleanupReport {
    pub entries_removed: usize,
    pub allocated_bytes_reclaimed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TempInventoryReport {
    pub generations: usize,
    pub retained_nonempty_generations: usize,
    pub retained_allocated_bytes: u64,
    pub max_generation_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct TempGenerationFloorReport {
    pub max_generation_id: Option<i64>,
}

impl TempInventoryReport {
    pub(crate) fn validate_durable_high_water(
        &self,
        durable_run_id_high_water: i64,
    ) -> Result<(), PolicyViolation> {
        validate_generation_high_water(self.max_generation_id, durable_run_id_high_water)
    }
}

impl TempGenerationFloorReport {
    pub(crate) fn validate_durable_high_water(
        &self,
        durable_run_id_high_water: i64,
    ) -> Result<(), PolicyViolation> {
        validate_generation_high_water(self.max_generation_id, durable_run_id_high_water)
    }
}

impl fmt::Debug for PrivateRunTemp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateRunTemp")
            .field("path", &self.path)
            .field("run_id", &self.name)
            .finish()
    }
}

impl PrivateRunTemp {
    /// Inspect top-level generation IDs for validation against the durable run
    /// ID high-water. Filesystem entries are never allocator authority and
    /// this observation never mutates global state.
    pub(crate) fn inspect_generation_floor(
        root: &VerifiedProjectRoot,
    ) -> Result<TempGenerationFloorReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let root_mount = directory_mount_identity_at(
                &root.directory,
                PolicyViolationStage::PreBinding,
            )?;
            let service = match open_optional_directory_on_mount(
                &root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_temp_container_at(
                        &directory,
                        PolicyViolationStage::PreBinding,
                    )?;
                    directory
                }
                None => {
                    return Ok(TempGenerationFloorReport::default());
                }
            };
            let tmp = match open_optional_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_directory_at(&directory, PolicyViolationStage::PreBinding)?;
                    directory
                }
                None => {
                    return Ok(TempGenerationFloorReport::default());
                }
            };
            let entries = directory_entries(
                &tmp,
                MAX_PRIVATE_TEMP_GENERATIONS,
                TempUnsafeReason::GenerationLimit,
                PolicyViolationStage::PreBinding,
                None,
                None,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut report = TempGenerationFloorReport::default();
            for entry in entries {
                if entry.mount_identity != root_mount {
                    return Err(temp_violation_at(
                        TempUnsafeReason::MountBoundary,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let name = entry.name.to_str().ok_or_else(|| {
                    temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    )
                })?;
                let generation_id = name
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0 && *value <= MAX_PRIVATE_TEMP_RUN_ID)
                    .filter(|value| name == value.to_string())
                    .ok_or_else(|| {
                        temp_violation_at(
                            TempUnsafeReason::InvalidEntry,
                            PolicyViolationStage::PreBinding,
                        )
                    })?;
                if !matches!(entry.kind, AuditedEntryKind::Directory) {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let generation = open_directory_on_mount(
                    &tmp,
                    &entry.name,
                    root_mount,
                    PolicyViolationStage::PreBinding,
                )?;
                validate_private_directory_at(&generation, PolicyViolationStage::PreBinding)?;
                if directory_identity_at(&generation, PolicyViolationStage::PreBinding)?
                    != entry.identity
                {
                    return Err(temp_violation_at(
                        TempUnsafeReason::IdentityChanged,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                report.max_generation_id = Some(
                    report
                        .max_generation_id
                        .unwrap_or_default()
                        .max(generation_id),
                );
            }
            Ok(report)
        }
    }

    pub fn create(root: &VerifiedProjectRoot, run_id: i64) -> Result<Self, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (root, run_id);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_run_id(run_id)?;
            let verified_root = root.anchor.verify_identity()?;
            let owner = verified_root.directory.try_clone().map_err(|_| temp_error())?;
            let root_mount = directory_mount_identity_at(
                &owner,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service =
                open_or_create_private_temp_container(&owner, OsStr::new(PRIVATE_TEMP_ROOT))?;
            let tmp = open_or_create_directory(&service, OsStr::new(PRIVATE_TEMP_DIR))?;
            let name = OsString::from(run_id.to_string());
            mkdirat_private(&tmp, &name)?;
            let directory = open_directory_nofollow(&tmp, &name).map_err(map_temp_io)?;
            validate_private_directory(&directory)?;
            directory.sync_all().map_err(|_| temp_error())?;
            tmp.sync_all().map_err(|_| temp_error())?;
            let identity = directory_identity(&directory)?;
            let service_identity = recovery_directory_identity_at(
                &service,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let parent_identity = recovery_directory_identity_at(
                &tmp,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let temp_identity = recovery_temp_identity_at(
                &directory,
                root_mount,
                service_identity,
                parent_identity,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service_root_identity = recovery_root_identity(&verified_root.anchor);
            let path = verified_root
                .anchor
                .canonical_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR)
                .join(&name);
            let temp = Self {
                name,
                path,
                parent: tmp,
                directory,
                identity,
                recovery_identity: PrivateRunTempRecoveryIdentityV1 {
                    service_root_identity,
                    temp_identity,
                },
                #[cfg(target_os = "linux")]
                decision_output_anchor: Mutex::new(None),
                #[cfg(target_os = "linux")]
                research_stdout_anchor: Mutex::new(None),
                #[cfg(target_os = "linux")]
                research_stderr_anchor: Mutex::new(None),
            };
            temp.recovery_identity(&verified_root)?;
            Ok(temp)
        }
    }

    /// Capture the immutable identities needed to reopen this exact
    /// generation after a native supervisor restart.  The current startup
    /// root anchor is verified before any descendant identity is recorded.
    pub fn recovery_identity(
        &self,
        root: &VerifiedProjectRoot,
    ) -> Result<PrivateRunTempRecoveryIdentityV1, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (self, root);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let verified_root = root.anchor.verify_identity()?;
            if recovery_root_identity(&verified_root.anchor)
                != self.recovery_identity.service_root_identity
            {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let expected_path = verified_root
                .anchor
                .canonical_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR)
                .join(&self.name);
            if self.path != expected_path {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }

            let root_mount = directory_mount_identity_at(
                &verified_root.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service = open_directory_on_mount(
                &verified_root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_temp_container_at(
                &service,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service_identity = recovery_directory_identity_at(
                &service,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if service_identity != self.recovery_identity.temp_identity.service_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let parent = open_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_directory_at(&parent, PolicyViolationStage::RunBoundPreMarker)?;
            let parent_identity = recovery_directory_identity_at(
                &parent,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if parent_identity != self.recovery_identity.temp_identity.parent_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            if directory_identity_at(&parent, PolicyViolationStage::RunBoundPreMarker)?
                != directory_identity(&self.parent)?
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let directory = open_directory_on_mount(
                &parent,
                &self.name,
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_directory_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if directory_identity_at(&directory, PolicyViolationStage::RunBoundPreMarker)?
                != self.identity
            {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let temp_identity = recovery_temp_identity_at(
                &directory,
                root_mount,
                service_identity,
                parent_identity,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if temp_identity != self.recovery_identity.temp_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            Ok(self.recovery_identity.clone())
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn prepare_decision_schema(
        &self,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        self.prepare_named_output("decision-schema.json", "decision.json", schema)
    }

    pub(crate) fn prepare_health_diagnosis_schema(
        &self,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        self.prepare_named_output(
            "health-diagnosis-schema.json",
            "health-diagnosis.json",
            schema,
        )
    }

    pub(crate) fn prepare_research_schema(
        &self,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        self.prepare_named_output("research-schema.json", "research.json", schema)?;
        #[cfg(not(target_os = "linux"))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(target_os = "linux")]
        {
            self.revalidate_current()?;
            let stdout = create_private_decision_file(
                &self.directory,
                OsStr::new("research-stdout.jsonl"),
                &[],
            )?;
            let stderr = create_private_decision_file(
                &self.directory,
                OsStr::new("research-stderr.log"),
                &[],
            )?;
            *self
                .research_stdout_anchor
                .lock()
                .map_err(|_| temp_error())? = Some(stdout);
            *self
                .research_stderr_anchor
                .lock()
                .map_err(|_| temp_error())? = Some(stderr);
            self.directory.sync_all().map_err(|_| temp_error())?;
            self.revalidate_current()
        }
    }

    pub(crate) fn prepare_editor_schema(&self, schema: &[u8]) -> Result<(), PolicyViolation> {
        self.prepare_named_output("editor-schema.json", "editor.json", schema)
    }

    fn prepare_named_output(
        &self,
        schema_name: &str,
        output_name: &str,
        schema: &[u8],
    ) -> Result<(), PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (schema_name, output_name, schema);
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(target_os = "linux")]
        {
            self.revalidate_current()?;
            drop(create_private_decision_file(
                &self.directory,
                OsStr::new(schema_name),
                schema,
            )?);
            let output = create_private_decision_file(
                &self.directory,
                OsStr::new(output_name),
                &[],
            )?;
            *self
                .decision_output_anchor
                .lock()
                .map_err(|_| temp_error())? = Some(output);
            self.directory.sync_all().map_err(|_| temp_error())?;
            self.revalidate_current()
        }
    }

    pub(crate) fn read_decision_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("decision.json")
    }

    pub(crate) fn read_health_diagnosis_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("health-diagnosis.json")
    }

    pub(crate) fn read_research_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("research.json")
    }

    pub(crate) fn read_research_stdout(&self) -> Result<Vec<u8>, PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ));
        }
        #[cfg(target_os = "linux")]
        {
            self.read_anchored_output(
                "research-stdout.jsonl",
                &self.research_stdout_anchor,
                MAX_RESEARCH_STDOUT_BYTES,
            )
        }
    }

    #[allow(dead_code)]
    pub(crate) fn read_research_stderr(&self) -> Result<Vec<u8>, PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ));
        }
        #[cfg(target_os = "linux")]
        {
            self.read_anchored_output(
                "research-stderr.log",
                &self.research_stderr_anchor,
                MAX_RESEARCH_STDERR_BYTES,
            )
        }
    }

    pub(crate) fn read_editor_output(&self) -> Result<Vec<u8>, PolicyViolation> {
        self.read_named_output("editor.json")
    }

    fn read_named_output(&self, output_name: &str) -> Result<Vec<u8>, PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = output_name;
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::Finalized,
            ))
        }
        #[cfg(target_os = "linux")]
        {
            self.read_anchored_output(
                output_name,
                &self.decision_output_anchor,
                MAX_DECISION_BYTES as u64,
            )
        }
    }

    #[cfg(target_os = "linux")]
    fn read_anchored_output(
        &self,
        output_name: &str,
        anchor_mutex: &Mutex<Option<DecisionOutputAnchor>>,
        max_size: u64,
    ) -> Result<Vec<u8>, PolicyViolation> {
        self.revalidate_current()?;
        let anchor = anchor_mutex.lock().map_err(|_| temp_error())?;
        let anchor = anchor.as_ref().ok_or_else(temp_error)?;
        let expected = anchor.identity;
        let parent_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::Finalized,
        )?;
        if entry_mount_identity_at(
            &self.directory,
            OsStr::new(output_name),
            PolicyViolationStage::Finalized,
        )? != parent_mount
        {
            return Err(temp_violation_at(
                TempUnsafeReason::MountBoundary,
                PolicyViolationStage::Finalized,
            ));
        }
        let before = validate_decision_output_file(&anchor.file, expected, parent_mount, max_size)?;
        let size = usize::try_from(before.size).map_err(|_| {
            temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
        })?;
        let bytes = read_decision_output_file(&anchor.file, size)?;
        let after = validate_decision_output_file(&anchor.file, expected, parent_mount, max_size)?;
        if before != after {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        let visible = artifact_entry_metadata_at(&self.directory, OsStr::new(output_name))?;
        if visible.identity != expected
            || visible.mount_identity != parent_mount
            || visible.owner != unsafe { libc::geteuid() as u32 }
            || visible.mode & 0o7777 != 0o600
            || visible.size != before.size
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        self.revalidate_current()?;
        Ok(bytes)
    }

    /// Clone and revalidate the retained directory capability for target use.
    pub(crate) fn verified_target(&self) -> Result<VerifiedPrivateTemp, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let directory = self.directory.try_clone().map_err(|_| temp_error())?;
            let metadata = directory.metadata().map_err(|_| temp_error())?;
            let identity = verified_private_temp_identity(&metadata, self.identity)?;
            let run_id = self
                .name
                .to_str()
                .and_then(|name| name.parse::<i64>().ok())
                .ok_or_else(temp_error)?;
            Ok(VerifiedPrivateTemp {
                directory,
                identity,
                run_id,
                #[cfg(target_os = "linux")]
                research_streams: None,
                #[cfg(all(unix, not(target_os = "linux")))]
                research_streams: None,
            })
        }
    }

    pub(crate) fn verified_target_for_research(
        &self,
    ) -> Result<VerifiedPrivateTemp, PolicyViolation> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = self;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(target_os = "linux")]
        {
            let mut target = self.verified_target()?;
            self.revalidate_current()?;
            let parent_mount = directory_mount_identity_at(
                &self.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let stdout = self.verified_research_stream(
                &self.research_stdout_anchor,
                MAX_RESEARCH_STDOUT_BYTES,
                parent_mount,
            )?;
            let stderr = self.verified_research_stream(
                &self.research_stderr_anchor,
                MAX_RESEARCH_STDERR_BYTES,
                parent_mount,
            )?;
            target.research_streams = Some(VerifiedResearchStreams {
                stdout: stdout.0,
                stdout_identity: stdout.1,
                stderr: stderr.0,
                stderr_identity: stderr.1,
            });
            Ok(target)
        }
    }

    #[cfg(target_os = "linux")]
    fn verified_research_stream(
        &self,
        anchor_mutex: &Mutex<Option<DecisionOutputAnchor>>,
        max_size: u64,
        parent_mount: MountIdentity,
    ) -> Result<(File, LogFileIdentity), PolicyViolation> {
        let anchor = anchor_mutex.lock().map_err(|_| temp_error())?;
        let anchor = anchor.as_ref().ok_or_else(temp_error)?;
        validate_decision_output_file(&anchor.file, anchor.identity, parent_mount, max_size)?;
        let file = anchor.file.try_clone().map_err(|_| temp_error())?;
        let identity = LogFileIdentity::from_open_descriptor(&file).map_err(|_| temp_error())?;
        Ok((file, identity))
    }

    pub fn cleanup_contents_before(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = deadline;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            check_cleanup_deadline(deadline)?;
            validate_private_directory(&self.directory)?;
            if directory_identity(&self.directory)? != self.identity {
                return Err(temp_violation(TempUnsafeReason::IdentityChanged));
            }
            let root_mount = directory_mount_identity_at(
                &self.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let mut state = AuditState::default();
            let entries = audit_directory(
                &self.directory,
                0,
                &mut state,
                deadline,
                PolicyViolationStage::RunBoundPreMarker,
                root_mount,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut report = TempCleanupReport {
                entries_removed: 0,
                allocated_bytes_reclaimed: 0,
            };
            remove_audited_entries(
                &self.directory,
                &entries,
                &mut report,
                deadline,
                root_mount,
            )?;
            finish_cleanup_before_success(
                &self.directory,
                deadline,
                #[cfg(all(test, unix))]
                None,
            )?;
            Ok(report)
        }
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_test_hook<F: FnOnce()>(
        &mut self,
        deadline: Option<Instant>,
        audit_hook: F,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            None,
        )?;
        audit_hook();
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        let mut test_state = CleanupTestState::default();
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            false,
            Some(&mut test_state),
            None,
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut test_state),
        )?;
        Ok(report)
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_test_state(
        &mut self,
        deadline: Option<Instant>,
        test_state: &mut CleanupTestState,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            None,
        )?;
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            false,
            Some(&mut *test_state),
            None,
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut *test_state),
        )?;
        Ok(report)
    }

    #[cfg(all(test, unix))]
    fn cleanup_contents_before_with_mount_test_state(
        &mut self,
        deadline: Option<Instant>,
        mount_test_state: &mut MountBoundaryTestState,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        validate_private_directory(&self.directory)?;
        if directory_identity(&self.directory)? != self.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        let root_mount = directory_mount_identity_at(
            &self.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let mut state = AuditState::default();
        let entries = audit_directory(
            &self.directory,
            0,
            &mut state,
            deadline,
            PolicyViolationStage::RunBoundPreMarker,
            root_mount,
            Some(&mut *mount_test_state),
        )?;
        let mut report = TempCleanupReport {
            entries_removed: 0,
            allocated_bytes_reclaimed: 0,
        };
        let mut cleanup_test_state = CleanupTestState::default();
        remove_audited_entries_impl(
            &self.directory,
            &entries,
            &mut report,
            deadline,
            root_mount,
            false,
            Some(&mut cleanup_test_state),
            Some(&mut *mount_test_state),
        )?;
        finish_cleanup_before_success(
            &self.directory,
            deadline,
            Some(&mut cleanup_test_state),
        )?;
        Ok(report)
    }

    pub fn inspect_capacity(
        root: &VerifiedProjectRoot,
    ) -> Result<TempInventoryReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = root;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::PreBinding,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let root_mount = directory_mount_identity_at(
                &root.directory,
                PolicyViolationStage::PreBinding,
            )?;
            let service = match open_optional_directory_on_mount(
                &root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_temp_container_at(
                        &directory,
                        PolicyViolationStage::PreBinding,
                    )?;
                    directory
                }
                None => {
                    return Ok(TempInventoryReport {
                        generations: 0,
                        retained_nonempty_generations: 0,
                        retained_allocated_bytes: 0,
                        max_generation_id: None,
                    });
                }
            };
            let tmp = match open_optional_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::PreBinding,
            )? {
                Some(directory) => {
                    validate_private_directory_at(&directory, PolicyViolationStage::PreBinding)?;
                    directory
                }
                None => {
                    return Ok(TempInventoryReport {
                        generations: 0,
                        retained_nonempty_generations: 0,
                        retained_allocated_bytes: 0,
                        max_generation_id: None,
                    });
                }
            };
            let entries = directory_entries(
                &tmp,
                MAX_PRIVATE_TEMP_GENERATIONS,
                TempUnsafeReason::GenerationLimit,
                PolicyViolationStage::PreBinding,
                None,
                None,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut state = AuditState::default();
            let mut report = TempInventoryReport {
                generations: 0,
                retained_nonempty_generations: 0,
                retained_allocated_bytes: 0,
                max_generation_id: None,
            };
            for entry in entries {
                if entry.mount_identity != root_mount {
                    return Err(temp_violation_at(
                        TempUnsafeReason::MountBoundary,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let name = entry.name.to_str().ok_or_else(|| {
                    temp_violation_at(TempUnsafeReason::InvalidEntry, PolicyViolationStage::PreBinding)
                })?;
                let generation_id = name
                    .parse::<i64>()
                    .ok()
                    .filter(|value| *value > 0 && *value <= MAX_PRIVATE_TEMP_RUN_ID)
                    .filter(|value| name == value.to_string());
                if generation_id.is_none() {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                if !matches!(entry.kind, AuditedEntryKind::Directory) {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let generation = open_directory_on_mount(
                    &tmp,
                    &entry.name,
                    root_mount,
                    PolicyViolationStage::PreBinding,
                )?;
                validate_private_directory_at(&generation, PolicyViolationStage::PreBinding)?;
                if directory_identity_at(&generation, PolicyViolationStage::PreBinding)?
                    != entry.identity
                {
                    return Err(temp_violation_at(
                        TempUnsafeReason::IdentityChanged,
                        PolicyViolationStage::PreBinding,
                    ));
                }
                let children = audit_directory(
                    &generation,
                    0,
                    &mut state,
                    None,
                    PolicyViolationStage::PreBinding,
                    root_mount,
                    #[cfg(all(test, unix))]
                    None,
                )
                .map_err(|error| stage_violation(error, PolicyViolationStage::PreBinding))?;
                report.generations += 1;
                report.max_generation_id = Some(
                    report
                        .max_generation_id
                        .unwrap_or_default()
                        .max(generation_id.expect("validated generation ID")),
                );
                let allocated = children
                    .iter()
                    .try_fold(0_u64, |total, child| {
                        total
                            .checked_add(
                                child.allocated_total(PolicyViolationStage::PreBinding)?,
                            )
                            .ok_or_else(|| {
                                temp_violation_at(
                                    TempUnsafeReason::ByteLimit,
                                    PolicyViolationStage::PreBinding,
                                )
                            })
                    })?;
                report.retained_allocated_bytes = report
                    .retained_allocated_bytes
                    .checked_add(allocated)
                    .ok_or_else(|| {
                        temp_violation_at(
                            TempUnsafeReason::ByteLimit,
                            PolicyViolationStage::PreBinding,
                        )
                    })?;
                if !children.is_empty() {
                    return Err(temp_violation_at(
                        TempUnsafeReason::InvalidEntry,
                        PolicyViolationStage::PreBinding,
                    ));
                }
            }
            Ok(report)
        }
    }

    /// Prove that the retained descriptor still names the exact owner-only
    /// generation visible under the fixed private-temp parent.
    pub fn revalidate_current(&self) -> Result<(), PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ))
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_private_directory(&self.directory)?;
            if directory_identity(&self.directory)? != self.identity {
                return Err(temp_error());
            }
            let current = open_directory_nofollow(&self.parent, &self.name).map_err(map_temp_io)?;
            validate_private_directory(&current)?;
            if directory_identity(&current)? != self.identity {
                return Err(temp_error());
            }
            Ok(())
        }
    }

    /// Cleanup is intentionally unsupported.  Retaining the complete run
    /// directory is the only portable way to ensure a concurrent pathname
    /// replacement can never be mutated by this handle.  Callers receive a
    /// bounded policy error while the original generation remains available as
    /// a diagnostic/runtime artifact.
    pub fn cleanup(&mut self) -> Result<(), PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let _ = self;
            Err(temp_error())
        }
    }
}

impl Drop for PrivateRunTemp {
    fn drop(&mut self) {}
}

impl RecoveredPrivateRunTempCleanup {
    /// Reopen only the generation described by the startup-pinned identity.
    /// This constructor never creates a missing directory and never learns
    /// expected identities from the descriptors it opens.
    pub fn open(
        startup_verified_service_root: &VerifiedProjectRoot,
        run_id: i64,
        expected: &PrivateRunTempRecoveryIdentityV1,
    ) -> Result<Self, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (startup_verified_service_root, run_id, expected);
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            validate_run_id(run_id)?;
            if expected.service_root_identity
                != recovery_root_identity(&startup_verified_service_root.anchor)
            {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::RootChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let verified_root = startup_verified_service_root.anchor.verify_identity()?;
            let root_mount = directory_mount_identity_at(
                &verified_root.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service = open_directory_on_mount(
                &verified_root.directory,
                OsStr::new(PRIVATE_TEMP_ROOT),
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_temp_container_at(
                &service,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let service_identity = recovery_directory_identity_at(
                &service,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if service_identity != expected.temp_identity.service_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let parent = open_directory_on_mount(
                &service,
                OsStr::new(PRIVATE_TEMP_DIR),
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_directory_at(&parent, PolicyViolationStage::RunBoundPreMarker)?;
            let parent_identity = recovery_directory_identity_at(
                &parent,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if parent_identity != expected.temp_identity.parent_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            let name = OsString::from(run_id.to_string());
            let directory = open_directory_on_mount(
                &parent,
                &name,
                root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            validate_private_directory_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            let actual = recovery_temp_identity_at(
                &directory,
                root_mount,
                service_identity.clone(),
                parent_identity.clone(),
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if actual != expected.temp_identity {
                return Err(temp_violation_at(
                    TempUnsafeReason::IdentityChanged,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
            }
            Ok(Self {
                anchor: verified_root.anchor,
                expected: expected.clone(),
                name,
                service,
                parent,
                directory,
            })
        }
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn revalidate_current(&self) -> Result<MountIdentity, PolicyViolation> {
        let verified_root = self.anchor.verify_identity()?;
        let root_mount = directory_mount_identity_at(
            &verified_root.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let service = open_directory_on_mount(
            &verified_root.directory,
            OsStr::new(PRIVATE_TEMP_ROOT),
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        validate_private_temp_container_at(
            &service,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let service_identity = recovery_directory_identity_at(
            &service,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if service_identity != self.expected.temp_identity.service_identity
            || service_identity != recovery_directory_identity_at(
                &self.service,
                PolicyViolationStage::RunBoundPreMarker,
            )?
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        let parent = open_directory_on_mount(
            &service,
            OsStr::new(PRIVATE_TEMP_DIR),
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        validate_private_directory_at(&parent, PolicyViolationStage::RunBoundPreMarker)?;
        let parent_identity = recovery_directory_identity_at(
            &parent,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if parent_identity != self.expected.temp_identity.parent_identity
            || parent_identity != recovery_directory_identity_at(
                &self.parent,
                PolicyViolationStage::RunBoundPreMarker,
            )?
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        let directory = open_directory_on_mount(
            &parent,
            &self.name,
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        validate_private_directory_at(
            &directory,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        let actual = recovery_temp_identity_at(
            &directory,
            root_mount,
            service_identity,
            parent_identity,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if actual != self.expected.temp_identity
            || actual != recovery_temp_identity_at(
                &self.directory,
                MountIdentity(self.expected.temp_identity.mount_identity),
                self.expected.temp_identity.service_identity.clone(),
                self.expected.temp_identity.parent_identity.clone(),
                PolicyViolationStage::RunBoundPreMarker,
            )?
        {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        Ok(root_mount)
    }

    /// Run the existing bounded audit/removal protocol against the retained
    /// original descriptor, with identity checks bracketing the operation.
    pub fn cleanup_contents_before(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<TempCleanupReport, PolicyViolation> {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = deadline;
            return Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                PolicyViolationStage::RunBoundPreMarker,
            ));
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let root_mount = self.revalidate_current()?;
            check_cleanup_deadline(deadline)?;
            let mut state = AuditState::default();
            let entries = audit_directory(
                &self.directory,
                0,
                &mut state,
                deadline,
                PolicyViolationStage::RunBoundPreMarker,
                root_mount,
                #[cfg(all(test, unix))]
                None,
            )?;
            let mut report = TempCleanupReport {
                entries_removed: 0,
                allocated_bytes_reclaimed: 0,
            };
            remove_audited_entries(
                &self.directory,
                &entries,
                &mut report,
                deadline,
                root_mount,
            )?;
            finish_cleanup_before_success(
                &self.directory,
                deadline,
                #[cfg(all(test, unix))]
                None,
            )?;
            self.revalidate_current()?;
            Ok(report)
        }
    }
}

#[cfg(target_os = "linux")]
struct DecisionOutputAnchor {
    file: File,
    identity: (u64, u64),
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, PartialEq, Eq)]
struct DecisionFileSnapshot {
    identity: (u64, u64),
    owner: u32,
    mode: u32,
    links: u64,
    size: u64,
}

#[cfg(target_os = "linux")]
fn create_private_decision_file(
    directory: &File,
    name: &OsStr,
    contents: &[u8],
) -> Result<DecisionOutputAnchor, PolicyViolation> {
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};

    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDWR
                | libc::O_CREAT
                | libc::O_EXCL
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        return Err(temp_violation(TempUnsafeReason::InvalidEntry));
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|_| temp_error())?;
    let metadata = file.metadata().map_err(|_| temp_error())?;
    if !metadata.is_file()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(temp_violation(TempUnsafeReason::InvalidEntry));
    }
    Ok(DecisionOutputAnchor {
        file,
        identity: (metadata.dev(), metadata.ino()),
    })
}

#[cfg(target_os = "linux")]
fn read_decision_output_file(file: &File, size: usize) -> Result<Vec<u8>, PolicyViolation> {
    use std::os::unix::fs::FileExt;

    let mut bytes = vec![0; size];
    let mut offset = 0;
    while offset < size {
        let read = file
            .read_at(&mut bytes[offset..], offset as u64)
            .map_err(|_| {
                temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
            })?;
        if read == 0 {
            return Err(temp_violation_at(
                TempUnsafeReason::IdentityChanged,
                PolicyViolationStage::Finalized,
            ));
        }
        offset = offset.checked_add(read).ok_or_else(|| {
            temp_violation_at(TempUnsafeReason::ByteLimit, PolicyViolationStage::Finalized)
        })?;
    }
    Ok(bytes)
}

#[cfg(target_os = "linux")]
fn validate_decision_output_file(
    file: &File,
    expected: (u64, u64),
    expected_mount: MountIdentity,
    max_size: u64,
) -> Result<DecisionFileSnapshot, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(|_| {
        temp_violation_at(TempUnsafeReason::IoFailure, PolicyViolationStage::Finalized)
    })?;
    let snapshot = DecisionFileSnapshot {
        identity: (metadata.dev(), metadata.ino()),
        owner: metadata.uid(),
        mode: metadata.mode() & 0o7777,
        links: metadata.nlink(),
        size: metadata.size(),
    };
    if !metadata.is_file()
        || snapshot.identity != expected
        || snapshot.owner != unsafe { libc::geteuid() as u32 }
        || snapshot.mode != 0o600
        || snapshot.links != 1
        || snapshot.size > max_size
        || directory_mount_identity_at(file, PolicyViolationStage::Finalized)? != expected_mount
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::Finalized,
        ));
    }
    Ok(snapshot)
}

#[cfg(unix)]
#[derive(Debug)]
struct AuditedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
    children: Vec<AuditedEntry>,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditedEntryKind {
    Directory,
    Leaf,
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MountIdentity([u64; 2]);

#[cfg(target_os = "linux")]
const LINUX_STATX_BASIC_STATS: u32 = 0x0000_07ff;
#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
const LINUX_STATX_MNT_ID: u32 = 0x0000_1000;

/// The stable 256-byte Linux kernel `struct statx` ABI subset used here.
/// Keeping this local avoids the libc wrapper/type availability boundary on
/// older glibc and on libc crate musl/uClibc configurations.
#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
#[repr(C, align(8))]
struct LinuxStatxBuffer {
    stx_mask: u32,
    before_mnt_id: [u8; 140],
    stx_mnt_id: u64,
    remaining: [u8; 104],
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
const _: () = {
    assert!(std::mem::size_of::<LinuxStatxBuffer>() == 256);
    assert!(std::mem::align_of::<LinuxStatxBuffer>() == 8);
    assert!(std::mem::offset_of!(LinuxStatxBuffer, stx_mask) == 0);
    assert!(std::mem::offset_of!(LinuxStatxBuffer, stx_mnt_id) == 144);
};

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LinuxSyscallFailure {
    Retry,
    Missing,
    MountBoundary,
    UnsupportedPlatform,
    IoFailure,
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn classify_linux_openat2_errno(errno: libc::c_int) -> LinuxSyscallFailure {
    match errno {
        libc::EINTR => LinuxSyscallFailure::Retry,
        libc::ENOENT => LinuxSyscallFailure::Missing,
        libc::EXDEV | libc::ELOOP => LinuxSyscallFailure::MountBoundary,
        libc::ENOSYS | libc::EINVAL | libc::E2BIG => {
            LinuxSyscallFailure::UnsupportedPlatform
        }
        _ => LinuxSyscallFailure::IoFailure,
    }
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn classify_linux_statx_errno(errno: libc::c_int) -> LinuxSyscallFailure {
    match errno {
        libc::EINTR => LinuxSyscallFailure::Retry,
        libc::ENOSYS => LinuxSyscallFailure::UnsupportedPlatform,
        _ => LinuxSyscallFailure::IoFailure,
    }
}

#[cfg(any(target_os = "linux", all(test, target_os = "macos")))]
fn parse_linux_statx_mount_identity(
    statx: &LinuxStatxBuffer,
) -> Result<MountIdentity, LinuxSyscallFailure> {
    if statx.stx_mask & LINUX_STATX_MNT_ID == 0 {
        return Err(LinuxSyscallFailure::UnsupportedPlatform);
    }
    Ok(MountIdentity([statx.stx_mnt_id, 0]))
}

#[cfg(unix)]
impl AuditedEntry {
    fn allocated_total(&self, stage: PolicyViolationStage) -> Result<u64, PolicyViolation> {
        self.children.iter().try_fold(self.allocated_bytes, |total, child| {
            total
                .checked_add(child.allocated_total(stage)?)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))
        })
    }
}

#[cfg(unix)]
struct AuditState {
    entries: usize,
    allocated_bytes: u64,
    max_depth: usize,
    max_entries: usize,
    max_allocated_bytes: u64,
    #[cfg(test)]
    test_deadline_after_entries: Option<usize>,
}

#[cfg(unix)]
impl Default for AuditState {
    fn default() -> Self {
        Self {
            entries: 0,
            allocated_bytes: 0,
            max_depth: MAX_PRIVATE_TEMP_CLEANUP_DEPTH,
            max_entries: MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            max_allocated_bytes: MAX_PRIVATE_TEMP_ALLOCATED_BYTES,
            #[cfg(test)]
            test_deadline_after_entries: None,
        }
    }
}

#[cfg(all(unix, test))]
impl AuditState {
    fn with_limits(max_depth: usize, max_entries: usize, max_allocated_bytes: u64) -> Self {
        Self {
            entries: 0,
            allocated_bytes: 0,
            max_depth,
            max_entries,
            max_allocated_bytes,
            test_deadline_after_entries: None,
        }
    }
}

#[cfg(all(unix, test))]
#[derive(Default)]
struct CleanupTestState {
    expire_after_recursion: bool,
    expire_before_sync: bool,
    deadline_crossed: bool,
    fail_after_first_unlink: bool,
    fail_sync: bool,
    sync_attempts: usize,
    sync_completed: bool,
}

#[cfg(all(unix, test))]
#[derive(Default)]
struct MountBoundaryTestState {
    mismatch_at_directory_open: bool,
    mismatch_before_unlink: bool,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OptionalOpenStep {
    EntryMountPrecheck,
    SecureOpen,
    DescriptorMountRecheck,
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[derive(Default)]
struct OptionalOpenTestState {
    steps: Vec<OptionalOpenStep>,
}

#[cfg(all(unix, test))]
struct DirectoryEntriesTestState {
    fail_after_metadata: Option<usize>,
    close_counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(unix)]
fn check_audit_deadline(
    deadline: Option<Instant>,
    state: Option<&AuditState>,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    #[cfg(not(test))]
    let _ = state;
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    #[cfg(test)]
    if state.is_some_and(|state| {
        state
            .test_deadline_after_entries
            .is_some_and(|limit| state.entries >= limit)
    }) {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn audit_trial_output_directory(
    directory: &File,
    depth: usize,
    state: &mut AuditState,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
) -> Result<Vec<AuditedEntry>, PolicyViolation> {
    let entries = audit_directory_with_child_mode(
        directory,
        depth,
        state,
        deadline,
        PolicyViolationStage::Finalized,
        root_mount,
        true,
        #[cfg(all(test, unix))]
        None,
    )?;
    for entry in &entries {
        let invalid_shape = match entry.name.as_os_str() {
            name if name == OsStr::new("result.json") => {
                entry.kind != AuditedEntryKind::Leaf
            }
            name if name == OsStr::new("artifacts") => {
                entry.kind != AuditedEntryKind::Directory
            }
            _ => false,
        };
        if invalid_shape {
            return Err(temp_violation_at(
                TempUnsafeReason::InvalidEntry,
                PolicyViolationStage::Finalized,
            ));
        }
    }
    Ok(entries)
}

#[cfg(unix)]
fn audit_directory(
    directory: &File,
    depth: usize,
    state: &mut AuditState,
    deadline: Option<Instant>,
    stage: PolicyViolationStage,
    root_mount: MountIdentity,
    #[cfg(all(test, unix))] mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<Vec<AuditedEntry>, PolicyViolation> {
    audit_directory_with_child_mode(
        directory,
        depth,
        state,
        deadline,
        stage,
        root_mount,
        false,
        #[cfg(all(test, unix))]
        mount_test_state,
    )
}

#[cfg(unix)]
fn audit_directory_with_child_mode(
    directory: &File,
    depth: usize,
    state: &mut AuditState,
    deadline: Option<Instant>,
    stage: PolicyViolationStage,
    root_mount: MountIdentity,
    allow_safe_child_directories: bool,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<Vec<AuditedEntry>, PolicyViolation> {
    check_audit_deadline(deadline, Some(state), stage)?;
    if depth > state.max_depth {
        return Err(temp_violation_at(TempUnsafeReason::DepthLimit, stage));
    }
    let mut entries = Vec::new();
    let remaining = state.max_entries.saturating_sub(state.entries);
    for listed in directory_entries(
        directory,
        remaining,
        TempUnsafeReason::EntryLimit,
        stage,
        deadline,
        Some(state),
        #[cfg(all(test, unix))]
        None,
    )? {
        check_audit_deadline(deadline, Some(state), stage)?;
        if listed.mount_identity != root_mount {
            return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
        }
        let children = if listed.kind == AuditedEntryKind::Directory {
            if depth == state.max_depth {
                return Err(temp_violation_at(TempUnsafeReason::DepthLimit, stage));
            }
            #[cfg(all(test, unix))]
            if mount_test_state.as_ref().is_some_and(|test_state| {
                test_state.mismatch_at_directory_open
            }) {
                if let Some(test_state) = mount_test_state.as_deref_mut() {
                    test_state.mismatch_at_directory_open = false;
                }
                return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
            }
            let child = open_directory_on_mount(directory, &listed.name, root_mount, stage)?;
            if allow_safe_child_directories {
                validate_safe_trial_child_directory(&child, stage)?;
            } else {
                validate_private_directory_at(&child, stage)?;
            }
            if directory_identity_at(&child, stage)? != listed.identity {
                return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
            }
            audit_directory_with_child_mode(
                &child,
                depth + 1,
                state,
                deadline,
                stage,
                root_mount,
                allow_safe_child_directories,
                #[cfg(all(test, unix))]
                mount_test_state.as_deref_mut(),
            )?
        } else {
            if allow_safe_child_directories {
                validate_trial_output_leaf_at(
                    directory,
                    &listed.name,
                    listed.identity,
                    root_mount,
                    stage,
                )?;
            }
            Vec::new()
        };
        entries.push(AuditedEntry {
            name: listed.name,
            identity: listed.identity,
            mount_identity: listed.mount_identity,
            kind: listed.kind,
            allocated_bytes: listed.allocated_bytes,
            children,
        });
    }
    Ok(entries)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn validate_trial_output_leaf_at(
    directory: &File,
    name: &OsStr,
    expected_identity: (u64, u64),
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let leaf = open_research_file_on_mount(directory, name, expected_mount, stage)?;
    let snapshot = research_leaf_snapshot(&leaf, expected_mount, u64::MAX, stage)?;
    if (snapshot.device, snapshot.inode) != expected_identity {
        return Err(temp_violation_at(TempUnsafeReason::IdentityChanged, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn remove_audited_entries(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
) -> Result<(), PolicyViolation> {
    #[cfg(all(test, unix))]
    let mut test_state = CleanupTestState::default();
    remove_audited_entries_impl(
        directory,
        entries,
        report,
        deadline,
        root_mount,
        false,
        #[cfg(all(test, unix))]
        Some(&mut test_state),
        #[cfg(all(test, unix))]
        None,
    )
}

#[cfg(unix)]
fn remove_audited_trial_output_entries(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
) -> Result<(), PolicyViolation> {
    #[cfg(all(test, unix))]
    let mut test_state = CleanupTestState::default();
    remove_audited_entries_impl(
        directory,
        entries,
        report,
        deadline,
        root_mount,
        true,
        #[cfg(all(test, unix))]
        Some(&mut test_state),
        #[cfg(all(test, unix))]
        None,
    )
}

#[cfg(unix)]
fn remove_audited_entries_impl(
    directory: &File,
    entries: &[AuditedEntry],
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    root_mount: MountIdentity,
    allow_safe_child_directories: bool,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<(), PolicyViolation> {
    let mut modified = false;
    for entry in entries {
        if let Err(error) = remove_audited_entry(
            directory,
            entry,
            report,
            deadline,
            &mut modified,
            root_mount,
            allow_safe_child_directories,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
            #[cfg(all(test, unix))]
            mount_test_state.as_deref_mut(),
        ) {
            if modified {
                if let Err(sync_error) = sync_directory(
                    directory,
                    #[cfg(all(test, unix))]
                    test_state.as_deref_mut(),
                ) {
                    return Err(sync_error);
                }
            }
            return Err(error);
        }
    }
    if modified {
        sync_directory(
            directory,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline_after_sync(
            deadline,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
    }
    Ok(())
}

#[cfg(unix)]
fn finish_cleanup_before_success(
    directory: &File,
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    check_cleanup_deadline(deadline)?;
    sync_directory(
        directory,
        #[cfg(all(test, unix))]
        test_state.as_deref_mut(),
    )?;
    check_cleanup_deadline_after_sync(
        deadline,
        #[cfg(all(test, unix))]
        test_state.as_deref_mut(),
    )
}

#[cfg(unix)]
fn remove_audited_entry(
    directory: &File,
    entry: &AuditedEntry,
    report: &mut TempCleanupReport,
    deadline: Option<Instant>,
    modified: &mut bool,
    root_mount: MountIdentity,
    allow_safe_child_directories: bool,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
    #[cfg(all(test, unix))] mut mount_test_state: Option<&mut MountBoundaryTestState>,
) -> Result<(), PolicyViolation> {
    check_cleanup_deadline(deadline)?;
    let current = entry_metadata(directory, &entry.name)?;
    if entry.mount_identity != root_mount || current.mount_identity != root_mount {
        return Err(temp_violation(TempUnsafeReason::MountBoundary));
    }
    if current.identity != entry.identity
        || current.kind != entry.kind
        || current.mount_identity != entry.mount_identity
    {
        return Err(temp_violation(TempUnsafeReason::IdentityChanged));
    }
    if entry.kind == AuditedEntryKind::Directory {
        check_cleanup_deadline(deadline)?;
        let child = open_directory_on_mount(
            directory,
            &entry.name,
            root_mount,
            PolicyViolationStage::RunBoundPreMarker,
        )?;
        if allow_safe_child_directories {
            validate_safe_trial_child_directory(&child, PolicyViolationStage::RunBoundPreMarker)?;
        } else {
            validate_private_directory(&child)?;
        }
        if directory_identity(&child)? != entry.identity {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        remove_audited_entries_impl(
            &child,
            &entry.children,
            report,
            deadline,
            root_mount,
            allow_safe_child_directories,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
            #[cfg(all(test, unix))]
            mount_test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline_after_recursion(
            deadline,
            #[cfg(all(test, unix))]
            test_state.as_deref_mut(),
        )?;
        check_cleanup_deadline(deadline)?;
        let current = entry_metadata(directory, &entry.name)?;
        if entry.mount_identity != root_mount || current.mount_identity != root_mount {
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        if current.identity != entry.identity
            || current.kind != AuditedEntryKind::Directory
            || current.mount_identity != entry.mount_identity
        {
            return Err(temp_violation(TempUnsafeReason::IdentityChanged));
        }
        #[cfg(all(test, unix))]
        if mount_test_state.as_ref().is_some_and(|test_state| {
            test_state.mismatch_before_unlink
        }) {
            if let Some(test_state) = mount_test_state.as_deref_mut() {
                test_state.mismatch_before_unlink = false;
            }
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        check_cleanup_deadline(deadline)?;
        unlinkat(directory, &entry.name, true)?;
    } else {
        #[cfg(all(test, unix))]
        if mount_test_state.as_ref().is_some_and(|test_state| {
            test_state.mismatch_before_unlink
        }) {
            if let Some(test_state) = mount_test_state.as_deref_mut() {
                test_state.mismatch_before_unlink = false;
            }
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        if allow_safe_child_directories {
            validate_trial_output_leaf_at(
                directory,
                &entry.name,
                entry.identity,
                root_mount,
                PolicyViolationStage::Finalized,
            )?;
        }
        check_cleanup_deadline(deadline)?;
        unlinkat(directory, &entry.name, false)?;
    }
    *modified = true;
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state.as_deref_mut() {
        if test_state.fail_after_first_unlink {
            test_state.fail_after_first_unlink = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    report.entries_removed = report
        .entries_removed
        .checked_add(1)
        .ok_or_else(|| temp_violation(TempUnsafeReason::EntryLimit))?;
    report.allocated_bytes_reclaimed = report
        .allocated_bytes_reclaimed
        .checked_add(entry.allocated_bytes)
        .ok_or_else(|| temp_violation(TempUnsafeReason::ByteLimit))?;
    Ok(())
}

#[cfg(unix)]
fn check_cleanup_deadline(deadline: Option<Instant>) -> Result<(), PolicyViolation> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(temp_violation(TempUnsafeReason::IoFailure))
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn check_cleanup_deadline_after_recursion(
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state {
        if test_state.expire_after_recursion {
            test_state.expire_after_recursion = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    check_cleanup_deadline(deadline)
}

#[cfg(unix)]
fn sync_directory(
    directory: &File,
    #[cfg(all(test, unix))] mut test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state.as_deref_mut() {
        test_state.sync_attempts += 1;
        if test_state.expire_before_sync {
            test_state.expire_before_sync = false;
            test_state.deadline_crossed = true;
        }
        if test_state.fail_sync {
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    let result = directory
        .sync_all()
        .map_err(|_| temp_violation(TempUnsafeReason::IoFailure));
    #[cfg(all(unix, test))]
    if result.is_ok() {
        if let Some(test_state) = test_state.as_deref_mut() {
            test_state.sync_completed = true;
        }
    }
    result
}

#[cfg(unix)]
fn check_cleanup_deadline_after_sync(
    deadline: Option<Instant>,
    #[cfg(all(test, unix))] test_state: Option<&mut CleanupTestState>,
) -> Result<(), PolicyViolation> {
    #[cfg(all(unix, test))]
    if let Some(test_state) = test_state {
        if test_state.deadline_crossed {
            test_state.deadline_crossed = false;
            return Err(temp_violation(TempUnsafeReason::IoFailure));
        }
    }
    check_cleanup_deadline(deadline)
}

#[cfg(unix)]
#[derive(Debug)]
struct ListedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
}

#[cfg(unix)]
struct OwnedDirectoryStream {
    stream: *mut libc::DIR,
    #[cfg(all(test, unix))]
    close_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[cfg(unix)]
impl OwnedDirectoryStream {
    fn open(
        fd: libc::c_int,
        stage: PolicyViolationStage,
        #[cfg(all(test, unix))]
        close_counter: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    ) -> Result<Self, PolicyViolation> {
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            unsafe { libc::close(fd) };
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        Ok(Self {
            stream,
            #[cfg(all(test, unix))]
            close_counter,
        })
    }

    fn as_ptr(&self) -> *mut libc::DIR {
        self.stream
    }
}

#[cfg(unix)]
impl Drop for OwnedDirectoryStream {
    fn drop(&mut self) {
        if self.stream.is_null() {
            return;
        }
        unsafe { libc::closedir(self.stream) };
        self.stream = std::ptr::null_mut();
        #[cfg(all(test, unix))]
        if let Some(counter) = self.close_counter.as_ref() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactHintScan {
    entries: usize,
    hints: Vec<DecisionArtifactHint>,
    max_hints: usize,
    max_depth: usize,
    max_field_bytes: usize,
    root_owner: u32,
    root_mount: MountIdentity,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactListedEntry {
    name: OsString,
    identity: (u64, u64),
    mount_identity: MountIdentity,
    owner: u32,
    mode: libc::mode_t,
    size: u64,
    mtime: i64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct ArtifactEntryMetadata {
    identity: (u64, u64),
    mount_identity: MountIdentity,
    owner: u32,
    mode: libc::mode_t,
    size: u64,
    mtime: i64,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn scan_artifact_hints(
    directory: &File,
    prefix: &str,
    directory_depth: usize,
    state: &mut ArtifactHintScan,
) -> Result<(), PolicyViolation> {
    let mut entries = artifact_directory_entries(directory, state)?;
    entries.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    for entry in entries {
        if entry.mount_identity != state.root_mount {
            return Err(temp_violation(TempUnsafeReason::MountBoundary));
        }
        if entry.owner != state.root_owner {
            continue;
        }
        let Some(name) = entry.name.to_str() else {
            continue;
        };
        let entry_depth = directory_depth
            .checked_add(1)
            .ok_or_else(|| temp_violation(TempUnsafeReason::DepthLimit))?;
        if entry_depth > state.max_depth {
            continue;
        }
        let separator_bytes = usize::from(!prefix.is_empty());
        let path_bytes = prefix
            .len()
            .checked_add(separator_bytes)
            .and_then(|bytes| bytes.checked_add(name.len()))
            .ok_or_else(|| temp_violation(TempUnsafeReason::ByteLimit))?;
        if path_bytes > state.max_field_bytes {
            continue;
        }
        let is_directory = entry.mode & libc::S_IFMT == libc::S_IFDIR;
        let is_regular = entry.mode & libc::S_IFMT == libc::S_IFREG;
        if is_directory {
            if entry_depth == state.max_depth {
                continue;
            }
            let child = open_directory_on_mount(
                directory,
                &entry.name,
                state.root_mount,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            if directory_identity_at(&child, PolicyViolationStage::RunBoundPreMarker)?
                != entry.identity
            {
                return Err(temp_violation(TempUnsafeReason::IdentityChanged));
            }
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            scan_artifact_hints(&child, &path, entry_depth, state)?;
        } else if is_regular && state.hints.len() < state.max_hints {
            let path = if prefix.is_empty() {
                name.to_owned()
            } else {
                format!("{prefix}/{name}")
            };
            state.hints.push(DecisionArtifactHint {
                path,
                size: entry.size,
                mtime: entry.mtime,
            });
        }
    }
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn artifact_directory_entries(
    directory: &File,
    state: &mut ArtifactHintScan,
) -> Result<Vec<ArtifactListedEntry>, PolicyViolation> {
    use std::{ffi::CStr, os::unix::ffi::OsStrExt};

    let duplicate = duplicate_directory_fd(directory, PolicyViolationStage::RunBoundPreMarker)?;
    if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
        unsafe { libc::close(duplicate) };
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    let stream = OwnedDirectoryStream::open(
        duplicate,
        PolicyViolationStage::RunBoundPreMarker,
        #[cfg(all(test, unix))]
        None,
    )?;
    let remaining = MAX_DECISION_ARTIFACT_SCAN_ENTRIES.saturating_sub(state.entries);
    let mut entries = Vec::with_capacity(remaining.min(64));
    loop {
        clear_errno(PolicyViolationStage::RunBoundPreMarker)?;
        let entry = unsafe { libc::readdir(stream.as_ptr()) };
        if entry.is_null() {
            let errno = io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno != 0 {
                return Err(temp_violation(TempUnsafeReason::IoFailure));
            }
            break;
        }
        let dirent = unsafe { &*entry };
        let name_bytes = unsafe { CStr::from_ptr(dirent.d_name.as_ptr()) }.to_bytes();
        if name_bytes == b"." || name_bytes == b".." || name_bytes.is_empty() {
            continue;
        }
        state.entries = state
            .entries
            .checked_add(1)
            .ok_or_else(|| temp_violation(TempUnsafeReason::EntryLimit))?;
        if state.entries > MAX_DECISION_ARTIFACT_SCAN_ENTRIES {
            return Err(temp_violation(TempUnsafeReason::EntryLimit));
        }
        if name_bytes.len() > state.max_field_bytes {
            continue;
        }
        let name = OsStr::from_bytes(name_bytes);
        let metadata = artifact_entry_metadata_at(directory, name)?;
        entries.push(ArtifactListedEntry {
            name: name.to_os_string(),
            identity: metadata.identity,
            mount_identity: metadata.mount_identity,
            owner: metadata.owner,
            mode: metadata.mode,
            size: metadata.size,
            mtime: metadata.mtime,
        });
    }
    Ok(entries)
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn artifact_entry_metadata_at(
    directory: &File,
    name: &OsStr,
) -> Result<ArtifactEntryMetadata, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};

    let mount_identity = entry_mount_identity_at(
        directory,
        name,
        PolicyViolationStage::RunBoundPreMarker,
    )?;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(ArtifactEntryMetadata {
        identity: (stat.st_dev as u64, stat.st_ino as u64),
        mount_identity,
        owner: stat.st_uid,
        mode: stat.st_mode,
        size: u64::try_from(stat.st_size).unwrap_or(0),
        mtime: stat.st_mtime,
    })
}

#[cfg(unix)]
fn directory_entries(
    directory: &File,
    max_entries: usize,
    overflow_reason: TempUnsafeReason,
    stage: PolicyViolationStage,
    deadline: Option<Instant>,
    mut audit_state: Option<&mut AuditState>,
    #[cfg(all(test, unix))] test_state: Option<&mut DirectoryEntriesTestState>,
) -> Result<Vec<ListedEntry>, PolicyViolation> {
    use std::ffi::CStr;
    use std::os::unix::ffi::OsStrExt;
    check_audit_deadline(deadline, audit_state.as_ref().map(|state| &**state), stage)?;
    let duplicate = duplicate_directory_fd(directory, stage)?;
    if let Err(error) = check_audit_deadline(
        deadline,
        audit_state.as_ref().map(|state| &**state),
        stage,
    ) {
        unsafe { libc::close(duplicate) };
        return Err(error);
    }
    if unsafe { libc::lseek(duplicate, 0, libc::SEEK_SET) } < 0 {
        unsafe { libc::close(duplicate) };
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    #[cfg(all(test, unix))]
    let close_counter = test_state
        .as_ref()
        .map(|state| std::sync::Arc::clone(&state.close_counter));
    let stream = OwnedDirectoryStream::open(
        duplicate,
        stage,
        #[cfg(all(test, unix))]
        close_counter,
    )?;
    let mut result = Vec::with_capacity(max_entries.min(64));
    let mut count = 0_usize;
    loop {
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        clear_errno(stage)?;
        let entry = unsafe { libc::readdir(stream.as_ptr()) };
        if entry.is_null() {
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if errno != 0 {
                return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
            }
            break;
        }
        let dirent = unsafe { &*entry };
        let name_bytes = unsafe { CStr::from_ptr(dirent.d_name.as_ptr()) }.to_bytes();
        if name_bytes == b"." || name_bytes == b".." || name_bytes.is_empty() {
            continue;
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| temp_violation_at(overflow_reason, stage))?;
        if count > max_entries {
            return Err(temp_violation_at(overflow_reason, stage));
        }
        let name = OsStr::from_bytes(name_bytes).to_os_string();
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        let listed = entry_metadata_at(directory, &name, stage)?;
        if let Err(error) = check_audit_deadline(
            deadline,
            audit_state.as_ref().map(|state| &**state),
            stage,
        ) {
            return Err(error);
        }
        #[cfg(all(test, unix))]
        if test_state.as_ref().is_some_and(|state| {
            state
                .fail_after_metadata
                .is_some_and(|limit| count >= limit)
        }) {
            return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
        }
        if let Some(state) = audit_state.as_mut() {
            let state = &mut **state;
            state.entries = state
                .entries
                .checked_add(1)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::EntryLimit, stage))?;
            if state.entries > state.max_entries {
                return Err(temp_violation_at(TempUnsafeReason::EntryLimit, stage));
            }
            state.allocated_bytes = state
                .allocated_bytes
                .checked_add(listed.allocated_bytes)
                .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
            if state.allocated_bytes > state.max_allocated_bytes {
                return Err(temp_violation_at(TempUnsafeReason::ByteLimit, stage));
            }
        }
        result.push(ListedEntry {
            name,
            identity: listed.identity,
            mount_identity: listed.mount_identity,
            kind: listed.kind,
            allocated_bytes: listed.allocated_bytes,
        });
    }
    Ok(result)
}

#[cfg(unix)]
fn duplicate_directory_fd(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<libc::c_int, PolicyViolation> {
    use std::os::fd::AsRawFd;
    let fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if fd < 0 {
        Err(temp_violation_at(TempUnsafeReason::IoFailure, stage))
    } else {
        Ok(fd)
    }
}

#[cfg(unix)]
fn clear_errno(stage: PolicyViolationStage) -> Result<(), PolicyViolation> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    let _ = stage;
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = 0;
        return Ok(())
    }
    #[cfg(target_os = "macos")]
    unsafe {
        *libc::__error() = 0;
        return Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            stage,
        ))
    }
}

#[cfg(target_os = "linux")]
fn directory_mount_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    linux_mount_identity_at(directory.as_raw_fd(), OsStr::new(""), libc::AT_EMPTY_PATH, stage)
}

#[cfg(target_os = "linux")]
fn entry_mount_identity_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    linux_mount_identity_at(
        directory.as_raw_fd(),
        name,
        libc::AT_SYMLINK_NOFOLLOW,
        stage,
    )
}

#[cfg(target_os = "linux")]
fn linux_mount_identity_at(
    directory_fd: libc::c_int,
    name: &OsStr,
    flags: libc::c_int,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let mut statx = std::mem::MaybeUninit::<LinuxStatxBuffer>::zeroed();
    loop {
        let result = unsafe {
            libc::syscall(
                libc::SYS_statx,
                directory_fd,
                name.as_ptr(),
                flags,
                LINUX_STATX_BASIC_STATS | LINUX_STATX_MNT_ID,
                statx.as_mut_ptr(),
            )
        };
        if result == 0 {
            break;
        }
        let errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        match classify_linux_statx_errno(errno) {
            LinuxSyscallFailure::Retry => continue,
            LinuxSyscallFailure::UnsupportedPlatform => {
                return Err(PolicyViolation::new(
                    PolicyViolationCode::UnsupportedPlatform,
                    stage,
                ));
            }
            LinuxSyscallFailure::Missing
            | LinuxSyscallFailure::MountBoundary
            | LinuxSyscallFailure::IoFailure => {
                return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
            }
        }
    }
    let statx = unsafe { statx.assume_init() };
    match parse_linux_statx_mount_identity(&statx) {
        Ok(identity) => Ok(identity),
        Err(LinuxSyscallFailure::UnsupportedPlatform) => Err(PolicyViolation::new(
            PolicyViolationCode::UnsupportedPlatform,
            stage,
        )),
        Err(_) => Err(temp_violation_at(TempUnsafeReason::IoFailure, stage)),
    }
}

#[cfg(target_os = "macos")]
fn directory_mount_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let stat = unsafe { stat.assume_init() };
    Ok(macos_mount_identity(&stat.f_fsid))
}

#[cfg(target_os = "macos")]
fn entry_mount_identity_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    match macos_entry_mount_identity_io(directory, name) {
        Ok(identity) => Ok(identity),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::ENOTSUP)
            ) => Err(PolicyViolation::new(
                PolicyViolationCode::UnsupportedPlatform,
                stage,
            )),
        Err(_) => Err(temp_violation_at(TempUnsafeReason::IoFailure, stage)),
    }
}

#[cfg(target_os = "macos")]
fn macos_entry_mount_identity_io(
    directory: &File,
    name: &OsStr,
) -> io::Result<MountIdentity> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    #[repr(C)]
    struct FsidAttributeBuffer {
        length: u32,
        fsid: [i32; 2],
    }
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let mut attributes = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: libc::ATTR_CMN_FSID,
        volattr: 0,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut buffer = FsidAttributeBuffer {
        length: 0,
        fsid: [0; 2],
    };
    loop {
        let result = unsafe {
            libc::getattrlistat(
                directory.as_raw_fd(),
                name.as_ptr(),
                &mut attributes as *mut _ as *mut libc::c_void,
                &mut buffer as *mut _ as *mut libc::c_void,
                std::mem::size_of::<FsidAttributeBuffer>(),
                libc::FSOPT_NOFOLLOW as libc::c_ulong,
            )
        };
        if result == 0 {
            break;
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error);
    }
    if buffer.length < std::mem::size_of::<FsidAttributeBuffer>() as u32 {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    Ok(MountIdentity([
        buffer.fsid[0] as u32 as u64,
        buffer.fsid[1] as u32 as u64,
    ]))
}

#[cfg(target_os = "macos")]
fn macos_mount_identity(fsid: &libc::fsid_t) -> MountIdentity {
    let values = unsafe { std::mem::transmute_copy::<libc::fsid_t, [i32; 2]>(fsid) };
    MountIdentity([values[0] as u32 as u64, values[1] as u32 as u64])
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn directory_mount_identity_at(
    _directory: &File,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn entry_mount_identity_at(
    _directory: &File,
    _name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<MountIdentity, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(unix)]
struct EntryMetadata {
    identity: (u64, u64),
    mount_identity: MountIdentity,
    kind: AuditedEntryKind,
    allocated_bytes: u64,
}

#[cfg(unix)]
fn entry_metadata(directory: &File, name: &OsStr) -> Result<EntryMetadata, PolicyViolation> {
    entry_metadata_at(
        directory,
        name,
        PolicyViolationStage::RunBoundPreMarker,
    )
}

#[cfg(unix)]
fn entry_metadata_at(
    directory: &File,
    name: &OsStr,
    stage: PolicyViolationStage,
) -> Result<EntryMetadata, PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let mount_identity = entry_mount_identity_at(directory, name, stage)?;
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation_at(TempUnsafeReason::InvalidEntry, stage))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstatat(directory.as_raw_fd(), name.as_ptr(), stat.as_mut_ptr(), libc::AT_SYMLINK_NOFOLLOW) } != 0 {
        return Err(temp_violation_at(TempUnsafeReason::IoFailure, stage));
    }
    let stat = unsafe { stat.assume_init() };
    let kind = if (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR {
        AuditedEntryKind::Directory
    } else {
        AuditedEntryKind::Leaf
    };
    let allocated_bytes = (stat.st_blocks as u64)
        .checked_mul(512)
        .ok_or_else(|| temp_violation_at(TempUnsafeReason::ByteLimit, stage))?;
    Ok(EntryMetadata {
        identity: (stat.st_dev as u64, stat.st_ino as u64),
        mount_identity,
        kind,
        allocated_bytes,
    })
}

#[cfg(unix)]
fn unlinkat(directory: &File, name: &OsStr, directory_entry: bool) -> Result<(), PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| temp_violation(TempUnsafeReason::InvalidEntry))?;
    let flags = if directory_entry { libc::AT_REMOVEDIR } else { 0 };
    if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), flags) } != 0 {
        return Err(temp_violation(TempUnsafeReason::IoFailure));
    }
    Ok(())
}

fn temp_violation(reason: TempUnsafeReason) -> PolicyViolation {
    temp_violation_at(reason, PolicyViolationStage::RunBoundPreMarker)
}

fn validate_generation_high_water(
    max_generation_id: Option<i64>,
    durable_run_id_high_water: i64,
) -> Result<(), PolicyViolation> {
    if !(0..=MAX_PRIVATE_TEMP_RUN_ID).contains(&durable_run_id_high_water)
        || max_generation_id.is_some_and(|generation_id| {
            generation_id <= 0
                || generation_id > MAX_PRIVATE_TEMP_RUN_ID
                || generation_id > durable_run_id_high_water
        })
    {
        return Err(temp_violation_at(
            TempUnsafeReason::InvalidEntry,
            PolicyViolationStage::PreBinding,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn open_or_create_private_temp_container(
    parent: &File,
    name: &OsStr,
) -> Result<File, PolicyViolation> {
    match open_directory_nofollow(parent, name) {
        Ok(directory) => {
            validate_private_temp_container_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            Ok(directory)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mkdirat_private(parent, name)?;
            let directory = open_directory_nofollow(parent, name).map_err(map_temp_io)?;
            validate_private_temp_container_at(
                &directory,
                PolicyViolationStage::RunBoundPreMarker,
            )?;
            Ok(directory)
        }
        Err(error) => Err(map_temp_io(error)),
    }
}

#[cfg(unix)]
fn open_or_create_directory(parent: &File, name: &OsStr) -> Result<File, PolicyViolation> {
    match open_directory_nofollow(parent, name) {
        Ok(directory) => {
            validate_private_directory(&directory)?;
            Ok(directory)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            mkdirat_private(parent, name)?;
            let directory = open_directory_nofollow(parent, name).map_err(map_temp_io)?;
            validate_private_directory(&directory)?;
            Ok(directory)
        }
        Err(error) => Err(map_temp_io(error)),
    }
}

#[cfg(unix)]
fn validate_private_temp_container_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o022 != 0
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_safe_trial_child_directory(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    use std::os::unix::fs::MetadataExt;
    let mode = metadata.mode() & 0o7777;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || mode & 0o7022 != 0
        || mode & 0o700 != 0o700
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_directory(directory: &File) -> Result<(), PolicyViolation> {
    validate_private_directory_at(directory, PolicyViolationStage::RunBoundPreMarker)
}

#[cfg(unix)]
fn validate_private_directory_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(), PolicyViolation> {
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    use std::os::unix::fs::MetadataExt;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() as u32 }
        || metadata.mode() & 0o7777 != 0o700
    {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(())
}

#[cfg(unix)]
fn verified_private_temp_identity(
    metadata: &std::fs::Metadata,
    expected: (u64, u64),
) -> Result<ExecutableIdentity, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let identity = ExecutableIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode() & 0o7777,
    };
    if !metadata.is_dir()
        || identity.owner != unsafe { libc::geteuid() as u32 }
        || identity.mode != 0o700
        || (identity.device, identity.inode) != expected
    {
        return Err(temp_error());
    }
    Ok(identity)
}

#[cfg(unix)]
fn recovery_root_identity(
    anchor: &crate::execution_policy::ProjectRootAnchor,
) -> PrivateRunTempRecoveryRootIdentity {
    PrivateRunTempRecoveryRootIdentity {
        device: anchor.identity.device,
        inode: anchor.identity.inode,
        owner: anchor.identity.owner,
        mode: anchor.identity.mode,
        resolution_fingerprint: anchor.resolution_fingerprint.clone(),
    }
}

#[cfg(unix)]
fn recovery_directory_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<PrivateRunTempRecoveryDirectoryIdentity, PolicyViolation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    let identity = PrivateRunTempRecoveryDirectoryIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        owner: metadata.uid(),
        mode: metadata.mode() & 0o7777,
    };
    if !metadata.is_dir() || identity.owner != unsafe { libc::geteuid() as u32 } {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    Ok(identity)
}

#[cfg(unix)]
fn recovery_temp_identity_at(
    directory: &File,
    mount_identity: MountIdentity,
    service_identity: PrivateRunTempRecoveryDirectoryIdentity,
    parent_identity: PrivateRunTempRecoveryDirectoryIdentity,
    stage: PolicyViolationStage,
) -> Result<PrivateRunTempRecoveryTempIdentity, PolicyViolation> {
    let identity = recovery_directory_identity_at(directory, stage)?;
    if identity.mode != 0o700 {
        return Err(temp_violation_at(TempUnsafeReason::InvalidEntry, stage));
    }
    if directory_mount_identity_at(directory, stage)? != mount_identity {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(PrivateRunTempRecoveryTempIdentity {
        device: identity.device,
        inode: identity.inode,
        owner: identity.owner,
        mode: identity.mode,
        mount_identity: mount_identity.0,
        service_identity,
        parent_identity,
    })
}

#[cfg(unix)]
fn directory_identity(directory: &File) -> Result<(u64, u64), PolicyViolation> {
    directory_identity_at(directory, PolicyViolationStage::RunBoundPreMarker)
}

#[cfg(unix)]
fn directory_identity_at(
    directory: &File,
    stage: PolicyViolationStage,
) -> Result<(u64, u64), PolicyViolation> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory
        .metadata()
        .map_err(|_| temp_violation_at(TempUnsafeReason::IoFailure, stage))?;
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(unix)]
fn mkdirat_private(parent: &File, name: &OsStr) -> Result<(), PolicyViolation> {
    use std::{os::fd::AsRawFd, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| temp_error())?;
    let result = unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) };
    if result == 0 {
        // Persist the newly visible directory before opening or validating it.
        // If this fails, leave the directory untouched for diagnostics.
        parent.sync_all().map_err(|_| temp_error())?;
        Ok(())
    } else {
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::AlreadyExists {
            Err(temp_error())
        } else {
            Err(map_temp_io(error))
        }
    }
}

#[cfg(unix)]
fn open_directory_nofollow(parent: &File, name: &OsStr) -> io::Result<File> {
    use std::{os::fd::{AsRawFd, FromRawFd}, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "NUL"))?;
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(fd) })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn open_optional_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<Option<File>, PolicyViolation> {
    open_optional_directory_on_mount_impl(
        parent,
        name,
        expected_mount,
        stage,
        #[cfg(test)]
        None,
    )
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
fn open_optional_directory_on_mount_with_test_state(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    test_state: &mut OptionalOpenTestState,
) -> Result<Option<File>, PolicyViolation> {
    open_optional_directory_on_mount_impl(
        parent,
        name,
        expected_mount,
        stage,
        Some(test_state),
    )
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct LinuxOpenHow {
    flags: u64,
    mode: u64,
    resolve: u64,
}

#[cfg(target_os = "linux")]
const LINUX_RESOLVE_NO_XDEV: u64 = 0x01;

#[cfg(target_os = "linux")]
fn linux_open_directory_no_xdev(
    parent: &File,
    name: &OsStr,
) -> Result<File, LinuxSyscallFailure> {
    linux_openat2_no_xdev(
        parent,
        name,
        libc::O_RDONLY
            | libc::O_DIRECTORY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK,
    )
}

#[cfg(target_os = "linux")]
fn linux_openat2_no_xdev(
    parent: &File,
    name: &OsStr,
    flags: libc::c_int,
) -> Result<File, LinuxSyscallFailure> {
    use std::{os::fd::{AsRawFd, FromRawFd}, os::unix::ffi::OsStrExt};
    let name = std::ffi::CString::new(name.as_bytes())
        .map_err(|_| LinuxSyscallFailure::IoFailure)?;
    let how = LinuxOpenHow {
        flags: flags as u64,
        mode: 0,
        resolve: LINUX_RESOLVE_NO_XDEV,
    };
    loop {
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                parent.as_raw_fd(),
                name.as_ptr(),
                &how as *const LinuxOpenHow,
                std::mem::size_of::<LinuxOpenHow>(),
            )
        };
        if fd >= 0 {
            return Ok(unsafe { File::from_raw_fd(fd as libc::c_int) });
        }
        let errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        match classify_linux_openat2_errno(errno) {
            LinuxSyscallFailure::Retry => continue,
            failure => return Err(failure),
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_syscall_violation(
    failure: LinuxSyscallFailure,
    stage: PolicyViolationStage,
) -> PolicyViolation {
    match failure {
        LinuxSyscallFailure::MountBoundary => {
            temp_violation_at(TempUnsafeReason::MountBoundary, stage)
        }
        LinuxSyscallFailure::UnsupportedPlatform => {
            PolicyViolation::new(PolicyViolationCode::UnsupportedPlatform, stage)
        }
        LinuxSyscallFailure::Retry
        | LinuxSyscallFailure::Missing
        | LinuxSyscallFailure::IoFailure => {
            temp_violation_at(TempUnsafeReason::IoFailure, stage)
        }
    }
}

#[cfg(target_os = "linux")]
fn open_optional_directory_on_mount_impl(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    #[cfg(test)] mut test_state: Option<&mut OptionalOpenTestState>,
) -> Result<Option<File>, PolicyViolation> {
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::SecureOpen);
    }
    let directory = match linux_open_directory_no_xdev(parent, name) {
        Ok(directory) => directory,
        Err(LinuxSyscallFailure::Missing) => return Ok(None),
        Err(failure) => return Err(linux_syscall_violation(failure, stage)),
    };
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::DescriptorMountRecheck);
    }
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(Some(directory))
}

#[cfg(target_os = "linux")]
fn open_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let directory = linux_open_directory_no_xdev(parent, name)
        .map_err(|failure| linux_syscall_violation(failure, stage))?;
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(directory)
}

#[cfg(target_os = "macos")]
fn macos_mount_identity_violation(
    error: io::Error,
    stage: PolicyViolationStage,
) -> PolicyViolation {
    match error.raw_os_error() {
        Some(libc::ENOSYS) | Some(libc::ENOTSUP) => {
            PolicyViolation::new(PolicyViolationCode::UnsupportedPlatform, stage)
        }
        Some(libc::ELOOP) => temp_violation_at(TempUnsafeReason::MountBoundary, stage),
        _ => temp_violation_at(TempUnsafeReason::IoFailure, stage),
    }
}

#[cfg(target_os = "macos")]
fn open_optional_directory_on_mount_impl(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
    #[cfg(test)] mut test_state: Option<&mut OptionalOpenTestState>,
) -> Result<Option<File>, PolicyViolation> {
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::EntryMountPrecheck);
    }
    let entry_mount = match macos_entry_mount_identity_io(parent, name) {
        Ok(identity) => identity,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(macos_mount_identity_violation(error, stage)),
    };
    if entry_mount != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::SecureOpen);
    }
    let directory = open_directory_nofollow(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    #[cfg(test)]
    if let Some(state) = test_state.as_deref_mut() {
        state.steps.push(OptionalOpenStep::DescriptorMountRecheck);
    }
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(Some(directory))
}

#[cfg(target_os = "macos")]
fn open_directory_on_mount(
    parent: &File,
    name: &OsStr,
    expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    let entry_mount = macos_entry_mount_identity_io(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    if entry_mount != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    let directory = open_directory_nofollow(parent, name)
        .map_err(|error| macos_mount_identity_violation(error, stage))?;
    if directory_mount_identity_at(&directory, stage)? != expected_mount {
        return Err(temp_violation_at(TempUnsafeReason::MountBoundary, stage));
    }
    Ok(directory)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn open_directory_on_mount(
    _parent: &File,
    _name: &OsStr,
    _expected_mount: MountIdentity,
    stage: PolicyViolationStage,
) -> Result<File, PolicyViolation> {
    Err(PolicyViolation::new(
        PolicyViolationCode::UnsupportedPlatform,
        stage,
    ))
}

#[cfg(unix)]
fn map_temp_io(error: io::Error) -> PolicyViolation {
    map_temp_io_at(error, PolicyViolationStage::RunBoundPreMarker)
}

fn map_temp_io_at(error: io::Error, stage: PolicyViolationStage) -> PolicyViolation {
    let _ = error;
    temp_violation_at(TempUnsafeReason::IoFailure, stage)
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::{
        ffi::CString,
        fs,
        os::unix::ffi::OsStrExt,
        os::unix::fs::{symlink, FileTypeExt, PermissionsExt},
        time::Duration,
    };
    use uuid::Uuid;

    fn test_temp(run_id: i64) -> (tempfile::TempDir, PrivateRunTemp) {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let temp = PrivateRunTemp::create(&root, run_id).unwrap();
        (holder, temp)
    }

    fn recovery_temp(
        run_id: i64,
    ) -> (
        tempfile::TempDir,
        VerifiedProjectRoot,
        PrivateRunTemp,
        PrivateRunTempRecoveryIdentityV1,
    ) {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let temp = PrivateRunTemp::create(&root, run_id).unwrap();
        let expected = temp.recovery_identity(&root).unwrap();
        (holder, root, temp, expected)
    }

    #[test]
    fn private_trial_output_creates_owner_only_generation_and_reads_atomic_result() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let trial_id = Uuid::new_v4();

        let mut output = PrivateTrialOutput::create(&root, trial_id).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        assert_eq!(
            generation.file_name().unwrap(),
            OsStr::new(&trial_id.to_string())
        );
        assert_eq!(
            Uuid::parse_str(generation.file_name().unwrap().to_str().unwrap()).unwrap(),
            trial_id
        );
        assert_eq!(output.result_path(), generation.join("result.json"));
        assert_eq!(output.artifact_dir(), generation.join("artifacts"));
        assert!(!output.result_path().exists());
        assert!(!output.artifact_dir().exists());
        assert!(output.result_descriptor.borrow().is_none());
        let trials = generation.parent().unwrap();
        let service = trials.parent().unwrap();
        for directory in [&root_path, service, trials, &generation] {
            assert_eq!(
                fs::metadata(directory).unwrap().permissions().mode() & 0o7777,
                0o700
            );
        }
        let generation_mode = fs::metadata(&generation).unwrap().permissions().mode() & 0o7777;
        assert_eq!(generation_mode, 0o700);

        let sibling_id = Uuid::new_v4();
        assert_ne!(sibling_id, trial_id);
        let sibling_generation = trials.join(sibling_id.to_string());
        fs::create_dir(&sibling_generation).unwrap();
        fs::set_permissions(&sibling_generation, fs::Permissions::from_mode(0o700)).unwrap();
        let sibling_sentinel = sibling_generation.join("sentinel");
        fs::write(&sibling_sentinel, b"neighbor").unwrap();
        fs::set_permissions(&sibling_sentinel, fs::Permissions::from_mode(0o600)).unwrap();

        let temporary_result = generation.join(".result.tmp");
        fs::write(&temporary_result, b"atomic-result").unwrap();
        fs::set_permissions(&temporary_result, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(&temporary_result, output.result_path()).unwrap();
        assert_eq!(output.read_result_bounded().unwrap(), b"atomic-result");
        assert!(output.result_descriptor.borrow().is_some());

        output.cleanup().unwrap();
        assert!(!generation.exists());
        assert!(service.exists());
        assert!(trials.exists());
        assert_eq!(fs::read(&sibling_sentinel).unwrap(), b"neighbor");
    }

    #[test]
    fn private_trial_output_rejects_unsafe_service_parents_without_mutation() {
        for mode in [0o720, 0o702] {
            let holder = tempfile::tempdir().unwrap();
            let root_path = holder.path().join("project");
            fs::create_dir(&root_path).unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
            let service = root_path.join(PRIVATE_TEMP_ROOT);
            fs::create_dir(&service).unwrap();
            fs::set_permissions(&service, fs::Permissions::from_mode(mode)).unwrap();
            let sentinel = service.join("sentinel");
            fs::write(&sentinel, b"preserve writable service").unwrap();
            let root_path = fs::canonicalize(root_path).unwrap();
            let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
            let root = anchor.verify_identity().unwrap();

            let error = match PrivateTrialOutput::create(&root, Uuid::new_v4()) {
                Ok(_) => panic!("writable service parent was accepted"),
                Err(error) => error,
            };

            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(error.stage, PolicyViolationStage::PreBinding);
            assert_eq!(
                error.detail,
                PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry)
            );
            assert_eq!(fs::metadata(&service).unwrap().permissions().mode() & 0o777, mode);
            assert_eq!(fs::read(&sentinel).unwrap(), b"preserve writable service");
            assert!(!service.join("trials").exists());
        }

        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        let outside_service = holder.path().join("outside-service");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&outside_service).unwrap();
        fs::set_permissions(&outside_service, fs::Permissions::from_mode(0o700)).unwrap();
        let sentinel = outside_service.join("sentinel");
        fs::write(&sentinel, b"preserve symlink target").unwrap();
        let service = root_path.join(PRIVATE_TEMP_ROOT);
        symlink(&outside_service, &service).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();

        let error = match PrivateTrialOutput::create(&root, Uuid::new_v4()) {
            Ok(_) => panic!("symlink service parent was accepted"),
            Err(error) => error,
        };

        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(error.stage, PolicyViolationStage::PreBinding);
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        );
        assert!(fs::symlink_metadata(&service)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read_link(&service).unwrap(), outside_service);
        assert_eq!(fs::read(&sentinel).unwrap(), b"preserve symlink target");
        assert!(!outside_service.join("trials").exists());
    }

    #[test]
    fn private_trial_output_rejects_result_substitution_and_preserves_it() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let outside = holder.path().join("outside-result");
        fs::write(&outside, b"outside").unwrap();
        symlink(&outside, output.result_path()).unwrap();

        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert!(fs::symlink_metadata(output.result_path())
            .unwrap()
            .file_type()
            .is_symlink());

        let mut hardlink_output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let hardlink_source = holder.path().join("hardlink-source");
        fs::write(&hardlink_source, b"hardlink").unwrap();
        fs::set_permissions(&hardlink_source, fs::Permissions::from_mode(0o600)).unwrap();
        fs::hard_link(&hardlink_source, hardlink_output.result_path()).unwrap();
        assert!(hardlink_output.read_result_bounded().is_err());
        assert!(hardlink_output.cleanup().is_err());
        assert!(hardlink_output.result_path().exists());

        let mut wrong_mode_output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        fs::write(wrong_mode_output.result_path(), b"wrong mode").unwrap();
        fs::set_permissions(
            wrong_mode_output.result_path(),
            fs::Permissions::from_mode(0o660),
        )
        .unwrap();
        assert!(wrong_mode_output.read_result_bounded().is_err());
        assert!(wrong_mode_output.cleanup().is_err());
        assert!(wrong_mode_output.result_path().exists());

        let mut directory_output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        fs::create_dir(directory_output.result_path()).unwrap();
        fs::set_permissions(
            directory_output.result_path(),
            fs::Permissions::from_mode(0o700),
        )
        .unwrap();
        assert!(directory_output.read_result_bounded().is_err());
        assert!(directory_output.cleanup().is_err());
        assert!(directory_output.result_path().is_dir());

        let mut fifo_output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let fifo_path = CString::new(fifo_output.result_path().as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
        assert!(fifo_output.read_result_bounded().is_err());
        assert!(fifo_output.cleanup().is_err());
        assert!(fs::symlink_metadata(fifo_output.result_path())
            .unwrap()
            .file_type()
            .is_fifo());

        let mut wrong_owner_output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        fs::write(wrong_owner_output.result_path(), b"wrong owner").unwrap();
        fs::set_permissions(
            wrong_owner_output.result_path(),
            fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let wrong_owner_path =
            CString::new(wrong_owner_output.result_path().as_os_str().as_bytes()).unwrap();
        let wrong_owner = unsafe { libc::geteuid() }.wrapping_add(1);
        if unsafe { libc::chown(wrong_owner_path.as_ptr(), wrong_owner, !0) } == 0 {
            assert!(wrong_owner_output.read_result_bounded().is_err());
            assert!(wrong_owner_output.cleanup().is_err());
            assert!(wrong_owner_output.result_path().exists());
        } else {
            eprintln!("skipping wrong-owner leaf assertion because chown is not permitted");
        }
    }

    #[test]
    fn private_trial_output_rejects_result_replacement_after_open() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let original = output.result_path().to_owned();
        let retired = generation.join("result.retired");
        let replacement = generation.join(".result.replacement");
        fs::write(&original, b"original result").unwrap();
        fs::set_permissions(&original, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&replacement, b"replacement result").unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();

        let result = output.read_result_bounded_after_read(|| {
            fs::rename(&original, &retired).unwrap();
            fs::rename(&replacement, &original).unwrap();
        });
        assert!(result.is_err());
        assert_eq!(fs::read(&original).unwrap(), b"replacement result");
        assert_eq!(fs::read(&retired).unwrap(), b"original result");
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&original).unwrap(), b"replacement result");
        assert_eq!(fs::read(&retired).unwrap(), b"original result");
    }

    #[test]
    fn private_trial_output_retains_result_identity_for_later_read_and_cleanup() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let result_path = output.result_path().to_owned();
        fs::write(&result_path, b"original result").unwrap();
        fs::set_permissions(&result_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(output.read_result_bounded().unwrap(), b"original result");

        fs::remove_file(&result_path).unwrap();
        fs::write(&result_path, b"replacement result").unwrap();
        fs::set_permissions(&result_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&result_path).unwrap(), b"replacement result");
        assert!(result_path.parent().unwrap().exists());
    }

    #[test]
    fn private_trial_output_enforces_result_cap_and_cleans_child_artifacts() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let oversized = vec![b'x'; crate::result_manifest::MAX_RESULT_MANIFEST_BYTES + 1];
        fs::write(output.result_path(), &oversized).unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        assert!(output.read_result_bounded().is_err());
        assert!(output.result_path().exists());
        output.cleanup().unwrap();

        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let exact = vec![b'x'; crate::result_manifest::MAX_RESULT_MANIFEST_BYTES];
        fs::write(output.result_path(), &exact).unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(output.read_result_bounded().unwrap(), exact);
        fs::create_dir(output.artifact_dir()).unwrap();
        fs::set_permissions(output.artifact_dir(), fs::Permissions::from_mode(0o700)).unwrap();
        let artifact = output.artifact_dir().join("child.bin");
        fs::write(&artifact, b"child artifact").unwrap();
        fs::set_permissions(&artifact, fs::Permissions::from_mode(0o600)).unwrap();
        let report = output.cleanup().unwrap();
        assert_eq!(report.entries_removed, 3);
        assert!(!generation.exists());
    }

    #[test]
    fn private_trial_output_cleanup_removes_invalid_result_and_abandoned_temp_safely() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let trials = generation.parent().unwrap().to_owned();
        let service = trials.parent().unwrap().to_owned();
        let oversized = vec![b'x'; crate::result_manifest::MAX_RESULT_MANIFEST_BYTES + 1];
        fs::write(output.result_path(), oversized).unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        let abandoned_temp = generation.join(".result.tmp");
        fs::write(&abandoned_temp, b"partially written manifest").unwrap();
        fs::set_permissions(&abandoned_temp, fs::Permissions::from_mode(0o600)).unwrap();
        let arbitrary_temp = generation.join(".result.tmp-atomic-writer-42");
        fs::write(&arbitrary_temp, b"partially written manifest").unwrap();
        fs::set_permissions(&arbitrary_temp, fs::Permissions::from_mode(0o600)).unwrap();

        let sibling = trials.join(Uuid::new_v4().to_string());
        fs::create_dir(&sibling).unwrap();
        fs::set_permissions(&sibling, fs::Permissions::from_mode(0o700)).unwrap();
        let sibling_sentinel = sibling.join("sentinel");
        fs::write(&sibling_sentinel, b"preserve neighbor").unwrap();
        fs::set_permissions(&sibling_sentinel, fs::Permissions::from_mode(0o600)).unwrap();

        assert!(output.read_result_bounded().is_err());
        let report = output.cleanup().unwrap();
        assert_eq!(report.entries_removed, 3);
        assert!(!generation.exists());
        assert!(service.is_dir());
        assert!(trials.is_dir());
        assert_eq!(fs::read(sibling_sentinel).unwrap(), b"preserve neighbor");
    }

    #[test]
    fn private_trial_output_preserves_replacement_after_oversized_read() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let original = output.result_path().to_owned();
        let retired = original.with_file_name("result.retired");
        let replacement = original.with_file_name(".result.replacement");
        fs::write(
            &original,
            vec![
                b'x';
                crate::result_manifest::MAX_RESULT_MANIFEST_BYTES + 1
            ],
        )
        .unwrap();
        fs::set_permissions(&original, fs::Permissions::from_mode(0o600)).unwrap();

        assert!(output.read_result_bounded().is_err());
        fs::rename(&original, &retired).unwrap();
        fs::write(&replacement, b"replacement result").unwrap();
        fs::set_permissions(&replacement, fs::Permissions::from_mode(0o600)).unwrap();
        fs::rename(&replacement, &original).unwrap();

        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&original).unwrap(), b"replacement result");
        assert!(retired.exists());
    }

    #[test]
    fn private_trial_output_cleanup_accepts_default_artifact_directory_modes() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let artifacts = output.artifact_dir();
        let nested = artifacts.join("checkpoints");
        fs::create_dir(artifacts).unwrap();
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(artifacts, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o755)).unwrap();
        let leaf = nested.join("history.csv");
        fs::write(&leaf, b"step,loss\n1,0.5\n").unwrap();
        fs::set_permissions(&leaf, fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(
            fs::metadata(&generation).unwrap().permissions().mode() & 0o7777,
            0o700
        );
        assert_eq!(
            fs::metadata(artifacts).unwrap().permissions().mode() & 0o7777,
            0o755
        );
        assert_eq!(
            fs::metadata(&nested).unwrap().permissions().mode() & 0o7777,
            0o755
        );

        let report = output.cleanup().unwrap();
        assert_eq!(report.entries_removed, 3);
        assert!(!generation.exists());
        assert!(root_path.join(PRIVATE_TEMP_ROOT).is_dir());
    }

    #[test]
    fn private_trial_output_rejects_generation_replacement_without_touching_it() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let retired = generation.with_file_name("retired-generation");
        fs::rename(&generation, &retired).unwrap();
        fs::create_dir(&generation).unwrap();
        fs::set_permissions(&generation, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(generation.join("sentinel"), b"replacement").unwrap();
        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(generation.join("sentinel")).unwrap(), b"replacement");
    }

    #[test]
    fn private_trial_output_rechecks_generation_name_before_cleanup_unlink() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let retired = generation.with_file_name("retired-generation");
        let replacement = generation.clone();

        let result =
            output.cleanup_before_with_hook(Instant::now() + Duration::from_secs(30), || {
                fs::rename(&generation, &retired).unwrap();
                fs::create_dir(&replacement).unwrap();
                fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
            });
        assert!(result.is_err());
        assert!(generation.is_dir());
        assert!(retired.is_dir());
        assert!(output.read_result_bounded().is_err());
    }

    #[test]
    fn private_trial_output_rejects_project_root_replacement() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let retired = root_path.with_file_name("retired-project");
        fs::rename(&root_path, &retired).unwrap();
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement_service = root_path.join(PRIVATE_TEMP_ROOT);
        let replacement_trials = replacement_service.join("trials");
        fs::create_dir(&replacement_service).unwrap();
        fs::set_permissions(&replacement_service, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&replacement_trials).unwrap();
        fs::set_permissions(&replacement_trials, fs::Permissions::from_mode(0o700)).unwrap();
        let sentinel = replacement_trials.join("replacement-sentinel");
        fs::write(&sentinel, b"replacement").unwrap();

        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), b"replacement");
        assert!(retired.join(PRIVATE_TEMP_ROOT).exists());
    }

    #[test]
    fn private_trial_output_rejects_service_parent_replacement() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let service = root_path.join(PRIVATE_TEMP_ROOT);
        let retired = service.with_file_name("retired-service");
        fs::rename(&service, &retired).unwrap();
        fs::create_dir(&service).unwrap();
        fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement_trials = service.join("trials");
        fs::create_dir(&replacement_trials).unwrap();
        fs::set_permissions(&replacement_trials, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement_generation =
            replacement_trials.join(output.result_path().parent().unwrap().file_name().unwrap());
        fs::create_dir(&replacement_generation).unwrap();
        fs::set_permissions(&replacement_generation, fs::Permissions::from_mode(0o700)).unwrap();
        let sentinel = replacement_generation.join("sentinel");
        fs::write(&sentinel, b"replacement").unwrap();

        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), b"replacement");
        assert!(retired.exists());
    }

    #[test]
    fn private_trial_output_rejects_trials_parent_replacement() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let trials = output.result_path().parent().unwrap().parent().unwrap().to_owned();
        let retired = trials.with_file_name("trials.retired");
        fs::rename(&trials, &retired).unwrap();
        fs::create_dir(&trials).unwrap();
        fs::set_permissions(&trials, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement_generation = trials.join(output.result_path().parent().unwrap().file_name().unwrap());
        fs::create_dir(&replacement_generation).unwrap();
        fs::set_permissions(&replacement_generation, fs::Permissions::from_mode(0o700)).unwrap();

        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert!(replacement_generation.exists());
        let sentinel = replacement_generation.join("sentinel");
        fs::write(&sentinel, b"replacement").unwrap();
        assert!(output.read_result_bounded().is_err());
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), b"replacement");
    }

    #[test]
    fn experiment_runtime_argv_with_outputs_preserves_campaign_bytes() {
        let project = Path::new("/private/project");
        let result = project.join(".pueue-agent").join("results").join("e.json");
        let artifacts = project.join(".pueue-agent").join("artifacts").join("e");
        let user = vec!["python".to_owned(), "train.py".to_owned()];
        let campaign = campaign_experiment_runtime_argv(project, "c", "e", &user);
        let shared = experiment_runtime_argv_with_outputs("c", "e", &result, &artifacts, &user);
        assert_eq!(campaign, shared);
        assert_eq!(
            campaign,
            vec![
                OsString::from("/usr/bin/env"),
                OsString::from("PUEUE_AGENT_EXPERIMENT_ID=e"),
                OsString::from("PUEUE_AGENT_CAMPAIGN_ID=c"),
                OsString::from(
                    "PUEUE_AGENT_RESULT_PATH=/private/project/.pueue-agent/results/e.json"
                ),
                OsString::from(
                    "PUEUE_AGENT_ARTIFACT_DIR=/private/project/.pueue-agent/artifacts/e"
                ),
                OsString::from("python"),
                OsString::from("train.py"),
            ]
        );
    }

    #[test]
    #[cfg(unix)]
    fn private_trial_output_paths_and_runtime_argv_preserve_non_utf8_root_bytes() {
        use std::os::unix::ffi::OsStringExt;

        let holder = tempfile::tempdir().unwrap();
        let raw_root = holder
            .path()
            .join(OsString::from_vec(vec![b'p', b'r', b'o', b'j', 0xff]));
        let trial_id = Uuid::from_u128(0x102030405060708090a0b0c0d0e0f000);
        let (result_path, artifact_dir) = private_trial_output_paths(&raw_root, trial_id);

        let generation = raw_root
            .join(PRIVATE_TEMP_ROOT)
            .join("trials")
            .join(trial_id.to_string());
        assert_eq!(result_path, generation.join("result.json"));
        assert_eq!(artifact_dir, generation.join("artifacts"));
        assert!(!raw_root.exists());
        assert!(!raw_root.join(PRIVATE_TEMP_ROOT).exists());

        let trial_argv = experiment_runtime_argv_with_outputs(
            "campaign",
            "experiment",
            &result_path,
            &artifact_dir,
            &["python".to_owned(), "train.py".to_owned()],
        );
        assert_eq!(trial_argv[0], OsStr::new("/usr/bin/env"));
        assert_eq!(
            trial_argv[1],
            OsStr::new("PUEUE_AGENT_EXPERIMENT_ID=experiment")
        );
        assert_eq!(
            trial_argv[2],
            OsStr::new("PUEUE_AGENT_CAMPAIGN_ID=campaign")
        );
        assert_eq!(
            trial_argv[3].as_os_str().as_bytes(),
            [
                b"PUEUE_AGENT_RESULT_PATH=".as_slice(),
                result_path.as_os_str().as_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            trial_argv[4].as_os_str().as_bytes(),
            [
                b"PUEUE_AGENT_ARTIFACT_DIR=".as_slice(),
                artifact_dir.as_os_str().as_bytes(),
            ]
            .concat()
        );
        assert_eq!(trial_argv[5], OsStr::new("python"));
        assert_eq!(trial_argv[6], OsStr::new("train.py"));

        let campaign_result = raw_root
            .join(PRIVATE_TEMP_ROOT)
            .join(RESULTS_DIRECTORY)
            .join("e.json");
        let campaign_artifact = raw_root
            .join(PRIVATE_TEMP_ROOT)
            .join(ARTIFACTS_DIRECTORY)
            .join("e");
        let campaign_argv = campaign_experiment_runtime_argv(
            &raw_root,
            "c",
            "e",
            &["python".to_owned(), "train.py".to_owned()],
        );
        assert_eq!(campaign_argv[0], OsStr::new("/usr/bin/env"));
        assert_eq!(campaign_argv[1], OsStr::new("PUEUE_AGENT_EXPERIMENT_ID=e"));
        assert_eq!(campaign_argv[2], OsStr::new("PUEUE_AGENT_CAMPAIGN_ID=c"));
        assert_eq!(
            campaign_argv[3].as_os_str().as_bytes(),
            [
                b"PUEUE_AGENT_RESULT_PATH=".as_slice(),
                campaign_result.as_os_str().as_bytes(),
            ]
            .concat()
        );
        assert_eq!(
            campaign_argv[4].as_os_str().as_bytes(),
            [
                b"PUEUE_AGENT_ARTIFACT_DIR=".as_slice(),
                campaign_artifact.as_os_str().as_bytes(),
            ]
            .concat()
        );
        assert_eq!(campaign_argv[5], OsStr::new("python"));
        assert_eq!(campaign_argv[6], OsStr::new("train.py"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn private_trial_output_reads_and_cleans_under_non_utf8_project_root() {
        use std::os::unix::ffi::OsStringExt;

        let holder = tempfile::tempdir().unwrap();
        let root_path = holder
            .path()
            .join(OsString::from_vec(vec![b'p', b'r', b'o', b'j', 0xff]));
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let trial_id = Uuid::from_u128(0x102030405060708090a0b0c0d0e0f000);
        let (expected_result, expected_artifact) = private_trial_output_paths(&root_path, trial_id);
        let mut output = PrivateTrialOutput::create(&root, trial_id).unwrap();

        assert_eq!(output.result_path(), expected_result.as_path());
        assert_eq!(output.artifact_dir(), expected_artifact.as_path());
        assert!(!output.result_path().exists());
        assert!(!output.artifact_dir().exists());
        fs::write(output.result_path(), b"non-UTF-8-root result").unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(
            output.read_result_bounded().unwrap(),
            b"non-UTF-8-root result"
        );
        let generation = output.result_path().parent().unwrap().to_owned();
        output.cleanup().unwrap();
        assert!(!generation.exists());
        assert!(generation.parent().unwrap().exists());
        assert!(generation.parent().unwrap().parent().unwrap().exists());
    }

    #[test]
    fn private_trial_output_accepts_verified_project_root_with_read_permissions() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o755)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();
        let trials = generation.parent().unwrap();
        let service = trials.parent().unwrap();
        assert_eq!(
            fs::metadata(&root_path).unwrap().permissions().mode() & 0o7777,
            0o755
        );
        for directory in [service, trials, generation.as_path()] {
            assert_eq!(
                fs::metadata(directory).unwrap().permissions().mode() & 0o7777,
                0o700
            );
        }

        fs::write(output.result_path(), b"accepted result").unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(output.read_result_bounded().unwrap(), b"accepted result");
        output.cleanup().unwrap();
        assert!(!generation.exists());
        assert!(root_path.join(PRIVATE_TEMP_ROOT).is_dir());
    }

    #[test]
    fn private_trial_output_cleanup_uses_bounded_audit_limits() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let artifacts = output.artifact_dir();
        fs::create_dir(artifacts).unwrap();
        fs::set_permissions(artifacts, fs::Permissions::from_mode(0o700)).unwrap();
        let mut depth_limited =
            AuditState::with_limits(0, MAX_PRIVATE_TEMP_CLEANUP_ENTRIES, u64::MAX);
        let error = audit_trial_output_directory(
            &output.generation,
            0,
            &mut depth_limited,
            Some(Instant::now() + Duration::from_secs(30)),
            output.mount,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::DepthLimit)
        );
        assert!(artifacts.exists());

        let mut entry_limited =
            AuditState::with_limits(MAX_PRIVATE_TEMP_CLEANUP_DEPTH, 0, u64::MAX);
        let error = audit_trial_output_directory(
            &output.generation,
            0,
            &mut entry_limited,
            Some(Instant::now() + Duration::from_secs(30)),
            output.mount,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit)
        );
        assert!(artifacts.exists());

        fs::write(
            output.result_path(),
            vec![0xa5; crate::result_manifest::MAX_RESULT_MANIFEST_BYTES],
        )
        .unwrap();
        fs::set_permissions(output.result_path(), fs::Permissions::from_mode(0o600)).unwrap();
        let mut byte_limited = AuditState::with_limits(
            MAX_PRIVATE_TEMP_CLEANUP_DEPTH,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            0,
        );
        let error = audit_trial_output_directory(
            &output.generation,
            0,
            &mut byte_limited,
            Some(Instant::now() + Duration::from_secs(30)),
            output.mount,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit)
        );
        assert!(output.result_path().exists());
        output.cleanup().unwrap();
    }

    #[test]
    fn private_trial_output_cleanup_before_uses_the_supplied_deadline() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        let generation = output.result_path().parent().unwrap().to_owned();

        assert!(output
            .cleanup_before(Instant::now() - Duration::from_secs(1))
            .is_err());
        assert!(generation.exists());
        output.cleanup().unwrap();
        assert!(!generation.exists());
    }

    #[test]
    fn hash_research_file_caps_growth_reads_to_limit_plus_one() {
        let limit = crate::result_manifest::MAX_RESULT_MANIFEST_BYTES;
        let mut contents = vec![0xa5; limit - 2];
        let mut requested_lengths = Vec::new();
        let mut total_read = 0_usize;
        let mut grew = false;
        let error = hash_research_file_with_reader(
            limit as u64,
            PolicyViolationStage::Finalized,
            |buffer, offset| {
                requested_lengths.push(buffer.len());
                let offset = usize::try_from(offset).unwrap();
                if offset >= contents.len() {
                    return Ok(0);
                }
                let count = buffer.len().min(contents.len() - offset);
                buffer[..count].copy_from_slice(&contents[offset..offset + count]);
                total_read += count;
                if !grew {
                    grew = true;
                    contents.extend(vec![0x5a; RESEARCH_HASH_BUFFER_BYTES * 2]);
                }
                Ok(count)
            },
        )
        .unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit)
        );
        assert_eq!(total_read, limit + 1);
        assert_eq!(requested_lengths, vec![limit + 1, 3]);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn private_trial_output_rejects_mounted_artifact_directory_with_safe_shape() {
        struct MountGuard {
            target: CString,
            mounted: bool,
        }

        impl Drop for MountGuard {
            fn drop(&mut self) {
                if self.mounted {
                    unsafe { libc::umount(self.target.as_ptr()) };
                }
            }
        }

        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let mut output = PrivateTrialOutput::create(&root, Uuid::new_v4()).unwrap();
        fs::create_dir(output.artifact_dir()).unwrap();
        fs::set_permissions(output.artifact_dir(), fs::Permissions::from_mode(0o700)).unwrap();

        let mut baseline_audit = AuditState::default();
        assert!(audit_trial_output_directory(
            &output.generation,
            0,
            &mut baseline_audit,
            Some(Instant::now() + Duration::from_secs(30)),
            output.mount,
        )
        .is_ok());

        let target = CString::new(output.artifact_dir().as_os_str().as_bytes()).unwrap();
        let tmpfs = CString::new("tmpfs").unwrap();
        if unsafe {
            libc::mount(
                std::ptr::null(),
                target.as_ptr(),
                tmpfs.as_ptr(),
                0,
                std::ptr::null(),
            )
        } != 0
        {
            let error = std::io::Error::last_os_error();
            if matches!(
                error.raw_os_error(),
                Some(libc::EPERM) | Some(libc::EACCES) | Some(libc::ENODEV) | Some(libc::ENOSYS)
            ) {
                eprintln!("skipping mount-boundary assertion: {error}");
                fs::remove_dir(output.artifact_dir()).unwrap();
                output.cleanup().unwrap();
                return;
            }
            panic!("temporary tmpfs mount failed: {error}");
        }
        let mut mount = MountGuard {
            target,
            mounted: true,
        };
        let sentinel = output.artifact_dir().join("sentinel");
        fs::write(&sentinel, b"mounted replacement").unwrap();

        let mut mounted_audit = AuditState::default();
        let error = audit_trial_output_directory(
            &output.generation,
            0,
            &mut mounted_audit,
            Some(Instant::now() + Duration::from_secs(30)),
            output.mount,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
        );
        assert!(output.cleanup().is_err());
        assert_eq!(fs::read(&sentinel).unwrap(), b"mounted replacement");
        assert_eq!(unsafe { libc::umount(mount.target.as_ptr()) }, 0);
        mount.mounted = false;
        fs::remove_dir(output.artifact_dir()).unwrap();
        output.cleanup().unwrap();
    }


    #[test]
    fn recovery_cleanup_reopens_exact_original_generation() {
        let (_holder, root, temp, expected) = recovery_temp(740);
        let payload = temp.path().join("payload");
        fs::write(&payload, b"original").unwrap();

        let mut recovered =
            RecoveredPrivateRunTempCleanup::open(&root, 740, &expected).unwrap();
        drop(temp);
        let report = recovered.cleanup_contents_before(None).unwrap();

        assert_eq!(report.entries_removed, 1);
        assert!(!payload.exists());
    }

    #[test]
    fn recovery_open_rejects_root_and_generation_replacements_without_touching_sentinels() {
        {
            let (holder, root, temp, expected) = recovery_temp(741);
            let root_path = root.anchor.canonical_path.clone();
            let retired = holder.path().join("retired-root");
            fs::rename(&root_path, &retired).unwrap();
            fs::create_dir(&root_path).unwrap();
            fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
            let replacement = root_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR)
                .join("741");
            fs::create_dir_all(&replacement).unwrap();
            fs::set_permissions(
                root_path.join(PRIVATE_TEMP_ROOT),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(
                root_path.join(PRIVATE_TEMP_ROOT).join(PRIVATE_TEMP_DIR),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = replacement.join("sentinel");
            fs::write(&sentinel, b"replacement-root").unwrap();

            let error =
                RecoveredPrivateRunTempCleanup::open(&root, 741, &expected).unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::RootChanged);
            assert_eq!(fs::read(&sentinel).unwrap(), b"replacement-root");
            drop(temp);
        }

        {
            let (_holder, root, temp, expected) = recovery_temp(742);
            let original = temp.path().to_path_buf();
            let retired = original.with_extension("retired");
            fs::rename(&original, &retired).unwrap();
            fs::create_dir(&original).unwrap();
            fs::set_permissions(&original, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = original.join("sentinel");
            fs::write(&sentinel, b"replacement-generation").unwrap();

            let error =
                RecoveredPrivateRunTempCleanup::open(&root, 742, &expected).unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"replacement-generation");
            drop(temp);
        }
    }

    #[test]
    fn recovery_open_rejects_symlink_mode_and_mount_mismatch_without_cleanup() {
        {
            let (_holder, root, temp, expected) = recovery_temp(743);
            let original = temp.path().to_path_buf();
            let retired = original.with_extension("retired");
            fs::rename(&original, &retired).unwrap();
            let target = retired.with_extension("target");
            fs::create_dir(&target).unwrap();
            fs::set_permissions(&target, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = target.join("sentinel");
            fs::write(&sentinel, b"symlink-target").unwrap();
            symlink(&target, &original).unwrap();

            let error =
                RecoveredPrivateRunTempCleanup::open(&root, 743, &expected).unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"symlink-target");
            drop(temp);
        }

        {
            let (_holder, root, temp, expected) = recovery_temp(744);
            let sentinel = temp.path().join("sentinel");
            fs::write(&sentinel, b"mode-mismatch").unwrap();
            fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o1700)).unwrap();

            let error =
                RecoveredPrivateRunTempCleanup::open(&root, 744, &expected).unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"mode-mismatch");
            drop(temp);
        }

        {
            let (_holder, root, temp, mut expected) = recovery_temp(745);
            let sentinel = temp.path().join("sentinel");
            fs::write(&sentinel, b"mount-mismatch").unwrap();
            expected.temp_identity.mount_identity[0] =
                expected.temp_identity.mount_identity[0].wrapping_add(1);

            let error =
                RecoveredPrivateRunTempCleanup::open(&root, 745, &expected).unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"mount-mismatch");
            drop(temp);
        }
    }

    #[test]
    fn recovery_open_rejects_changed_private_temp_parent_without_touching_sentinel() {
        {
            let (holder, root, temp, expected) = recovery_temp(746);
            let service = root.anchor.canonical_path.join(PRIVATE_TEMP_ROOT);
            let retired = holder.path().join("retired-service");
            fs::rename(&service, &retired).unwrap();
            fs::create_dir(&service).unwrap();
            fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
            let replacement = service.join(PRIVATE_TEMP_DIR).join("746");
            fs::create_dir_all(&replacement).unwrap();
            fs::set_permissions(
                service.join(PRIVATE_TEMP_DIR),
                fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            fs::set_permissions(&replacement, fs::Permissions::from_mode(0o700)).unwrap();
            let sentinel = replacement.join("sentinel");
            fs::write(&sentinel, b"replacement-parent").unwrap();

            let error = RecoveredPrivateRunTempCleanup::open(&root, 746, &expected)
                .unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"replacement-parent");
            drop(temp);
        }

        {
            let (holder, root, temp, expected) = recovery_temp(747);
            let parent = root
                .anchor
                .canonical_path
                .join(PRIVATE_TEMP_ROOT)
                .join(PRIVATE_TEMP_DIR);
            let retired = holder.path().join("retired-temp-parent");
            fs::rename(&parent, &retired).unwrap();
            fs::create_dir(&parent).unwrap();
            fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
            let moved = parent.join("747");
            fs::rename(retired.join("747"), &moved).unwrap();
            let sentinel = moved.join("sentinel");
            fs::write(&sentinel, b"replacement-temp-parent").unwrap();

            let error = RecoveredPrivateRunTempCleanup::open(&root, 747, &expected)
                .unwrap_err();
            assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
            assert_eq!(fs::read(&sentinel).unwrap(), b"replacement-temp-parent");
            drop(temp);
        }
    }

    #[test]
    fn recovery_identity_rejects_service_substitution_before_publishing() {
        let (holder, root, temp, _expected) = recovery_temp(748);
        let service = root.anchor.canonical_path.join(PRIVATE_TEMP_ROOT);
        let retired = holder.path().join("retired-service");
        fs::rename(&service, &retired).unwrap();
        fs::create_dir(&service).unwrap();
        fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
        fs::rename(
            retired.join(PRIVATE_TEMP_DIR),
            service.join(PRIVATE_TEMP_DIR),
        )
        .unwrap();
        let sentinel = service
            .join(PRIVATE_TEMP_DIR)
            .join("748")
            .join("sentinel");
        fs::write(&sentinel, b"substituted-service").unwrap();

        let error = temp.recovery_identity(&root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(fs::read(&sentinel).unwrap(), b"substituted-service");
        drop(temp);
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn recovery_identity_is_path_free_and_strictly_serialized() {
        #[cfg(target_os = "linux")]
        use std::os::unix::ffi::OsStringExt;

        let holder = tempfile::tempdir().unwrap();
        #[cfg(target_os = "linux")]
        let root_path = holder.path().join(OsString::from_vec(vec![b'p', 0xff]));
        #[cfg(target_os = "macos")]
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let temp = PrivateRunTemp::create(&root, 749).unwrap();
        let expected = temp.recovery_identity(&root).unwrap();
        let encoded = serde_json::to_value(&expected).unwrap();
        let decoded: PrivateRunTempRecoveryIdentityV1 =
            serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded, expected);

        let mut extra = encoded;
        extra["temp_identity"]["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<PrivateRunTempRecoveryIdentityV1>(extra).is_err());
    }

    #[test]
    #[cfg(unix)]
    fn campaign_experiment_runtime_argv_preserves_arbitrary_root_bytes() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let raw_root = OsString::from_vec(vec![b'/', b't', b'm', b'p', 0xff, 0xfe]);
        let root = Path::new(&raw_root);
        let argv = campaign_experiment_runtime_argv(
            root,
            "camp-id",
            "exp-id",
            &["echo".to_owned(), "hi".to_owned()],
        );
        assert_eq!(argv[0], OsString::from("/usr/bin/env"));
        assert_eq!(argv.len(), 1 + 4 + 2);
        // result path is third assignment (index 3)
        let result_assignment = &argv[3];
        assert!(result_assignment.as_bytes().contains(&0xff));
        assert!(result_assignment.as_bytes().contains(&0xfe));
        // artifact dir is fourth assignment
        let artifact_assignment = &argv[4];
        assert!(artifact_assignment.as_bytes().contains(&0xff));
    }

    #[test]
    fn duplicate_directory_fd_sets_cloexec_atomically() {
        let (_holder, temp) = test_temp(701);
        let duplicate = duplicate_directory_fd(
            &temp.directory,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .unwrap();
        let flags = unsafe { libc::fcntl(duplicate, libc::F_GETFD) };
        unsafe { libc::close(duplicate) };
        assert_ne!(flags & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn verified_target_rejects_private_directory_special_bits() {
        let (_holder, temp) = test_temp(702);
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o1700)).unwrap();

        let error = temp.verified_target().unwrap_err();

        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
    }

    #[test]
    fn verified_private_temp_identity_uses_one_validated_metadata_snapshot() {
        use std::os::unix::fs::MetadataExt;

        let (_holder, temp) = test_temp(703);
        let metadata = temp.directory.metadata().unwrap();

        let identity = verified_private_temp_identity(&metadata, temp.identity).unwrap();

        assert_eq!(identity.device, metadata.dev());
        assert_eq!(identity.inode, metadata.ino());
        assert_eq!(identity.owner, metadata.uid());
        assert_eq!(identity.mode, metadata.mode() & 0o7777);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn retained_decision_output_anchor_prevents_inode_reuse_and_rejects_replacement() {
        use std::{
            io::Write as _,
            os::unix::fs::{MetadataExt, OpenOptionsExt},
        };

        let (_holder, temp) = test_temp(704);
        temp.prepare_decision_schema(b"{}").unwrap();
        let original_identity = temp
            .decision_output_anchor
            .lock()
            .unwrap()
            .as_ref()
            .map(|anchor| anchor.identity)
            .unwrap();
        let output = temp.path().join("decision.json");
        fs::remove_file(&output).unwrap();
        let mut replacement = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&output)
            .unwrap();
        replacement.write_all(b"replacement").unwrap();
        replacement.sync_all().unwrap();
        let metadata = replacement.metadata().unwrap();

        assert_ne!(original_identity, (metadata.dev(), metadata.ino()));
        let error = temp.read_decision_output().unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(
                TempUnsafeReason::InvalidEntry | TempUnsafeReason::IdentityChanged
            )
        ));
    }

    #[test]
    fn inspect_capacity_rejects_maximum_sqlite_generation_id() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        let retained = PrivateRunTemp::create(&root, MAX_PRIVATE_TEMP_RUN_ID).unwrap();
        let impossible = retained
            .path()
            .parent()
            .unwrap()
            .join(i64::MAX.to_string());
        fs::create_dir(&impossible).unwrap();
        fs::set_permissions(&impossible, fs::Permissions::from_mode(0o700)).unwrap();
        let error = PrivateRunTemp::inspect_capacity(&root).unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry)
        );
    }

    #[test]
    fn inspect_capacity_accepts_highest_allocatable_generation_id() {
        let holder = tempfile::tempdir().unwrap();
        let root_path = holder.path().join("project");
        fs::create_dir(&root_path).unwrap();
        fs::set_permissions(&root_path, fs::Permissions::from_mode(0o700)).unwrap();
        let root_path = fs::canonicalize(root_path).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root_path).unwrap();
        let root = anchor.verify_identity().unwrap();
        PrivateRunTemp::create(&root, i64::MAX - 1).unwrap();
        let report = PrivateRunTemp::inspect_capacity(&root).unwrap();
        assert_eq!(report.max_generation_id, Some(i64::MAX - 1));
    }

    #[test]
    fn directory_entries_enforces_remaining_limit_before_collecting_more() {
        let (_holder, temp) = test_temp(702);
        fs::write(temp.path.join("one"), b"1").unwrap();
        fs::write(temp.path.join("two"), b"2").unwrap();
        let error = directory_entries(
            &temp.directory,
            1,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit));
        assert!(temp.path.join("one").exists());
        assert!(temp.path.join("two").exists());
    }

    #[test]
    fn audit_byte_limit_is_checked_before_any_removal() {
        let (_holder, temp) = test_temp(703);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        assert_eq!(AuditState::default().max_allocated_bytes, MAX_PRIVATE_TEMP_ALLOCATED_BYTES);
        let mut state = AuditState::with_limits(
            MAX_PRIVATE_TEMP_CLEANUP_DEPTH,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            0,
        );
        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            None,
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit));
        assert!(temp.path.join("payload").exists());
    }

    #[test]
    fn audited_entry_allocation_uses_checked_addition() {
        let entry = AuditedEntry {
            name: OsString::from("parent"),
            identity: (1, 1),
            mount_identity: MountIdentity([1, 0]),
            kind: AuditedEntryKind::Directory,
            allocated_bytes: u64::MAX,
            children: vec![AuditedEntry {
                name: OsString::from("child"),
                identity: (1, 2),
                mount_identity: MountIdentity([1, 0]),
                kind: AuditedEntryKind::Leaf,
                allocated_bytes: 1,
                children: Vec::new(),
            }],
        };
        let error = entry
            .allocated_total(PolicyViolationStage::RunBoundPreMarker)
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit));
    }

    #[test]
    fn test_hook_identity_change_aborts_and_retry_remains_authorized() {
        let (holder, mut temp) = test_temp(704);
        let original = temp.path.join("original");
        fs::write(&original, b"original").unwrap();
        let retired = holder.path().join("retired");
        let replacement = original.clone();
        let replacement_for_hook = replacement.clone();
        let error = temp
            .cleanup_contents_before_with_test_hook(None, move || {
                fs::rename(&original, &retired).unwrap();
                fs::write(&replacement_for_hook, b"replacement").unwrap();
            })
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged));
        assert_eq!(fs::read(&replacement).unwrap(), b"replacement");
        temp.cleanup_contents_before(None).unwrap();
        let remaining: Vec<_> = fs::read_dir(temp.path()).unwrap().collect();
        assert!(remaining.is_empty(), "remaining entries: {remaining:?}");
    }

    #[test]
    fn mount_identity_mismatch_at_directory_open_preserves_tree_and_retry_authority() {
        let (_holder, mut temp) = test_temp(713);
        let nested = temp.path.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(nested.join("payload"), b"preserved").unwrap();
        let mut state = MountBoundaryTestState {
            mismatch_at_directory_open: true,
            ..MountBoundaryTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_mount_test_state(None, &mut state)
            .unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
        );
        assert_eq!(fs::read(nested.join("payload")).unwrap(), b"preserved");
        temp.cleanup_contents_before(None).unwrap();
        assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    }

    #[test]
    fn mount_identity_mismatch_before_unlink_deletes_nothing_and_remains_retryable() {
        let (_holder, mut temp) = test_temp(714);
        fs::write(temp.path.join("first"), b"first").unwrap();
        fs::write(temp.path.join("second"), b"second").unwrap();
        let mut state = MountBoundaryTestState {
            mismatch_before_unlink: true,
            ..MountBoundaryTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_mount_test_state(None, &mut state)
            .unwrap_err();

        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
        );
        assert_eq!(fs::read(temp.path.join("first")).unwrap(), b"first");
        assert_eq!(fs::read(temp.path.join("second")).unwrap(), b"second");
        temp.cleanup_contents_before(None).unwrap();
        assert!(fs::read_dir(temp.path()).unwrap().next().is_none());
    }

    #[test]
    fn optional_mount_open_checks_entry_before_descriptor_and_missing_is_empty() {
        let holder = tempfile::tempdir().unwrap();
        let parent = File::open(holder.path()).unwrap();
        let expected_mount = directory_mount_identity_at(
            &parent,
            PolicyViolationStage::PreBinding,
        )
        .unwrap();
        let mut missing_state = OptionalOpenTestState::default();

        let missing = open_optional_directory_on_mount_with_test_state(
            &parent,
            OsStr::new("missing"),
            expected_mount,
            PolicyViolationStage::PreBinding,
            &mut missing_state,
        )
        .unwrap();

        assert!(missing.is_none());
        #[cfg(target_os = "macos")]
        assert_eq!(
            missing_state.steps,
            vec![OptionalOpenStep::EntryMountPrecheck]
        );
        #[cfg(target_os = "linux")]
        assert_eq!(missing_state.steps, vec![OptionalOpenStep::SecureOpen]);

        let present = holder.path().join("present");
        fs::create_dir(&present).unwrap();
        fs::set_permissions(&present, fs::Permissions::from_mode(0o700)).unwrap();
        let mut present_state = OptionalOpenTestState::default();
        let opened = open_optional_directory_on_mount_with_test_state(
            &parent,
            OsStr::new("present"),
            expected_mount,
            PolicyViolationStage::PreBinding,
            &mut present_state,
        )
        .unwrap();

        assert!(opened.is_some());
        #[cfg(target_os = "macos")]
        assert_eq!(
            present_state.steps,
            vec![
                OptionalOpenStep::EntryMountPrecheck,
                OptionalOpenStep::SecureOpen,
                OptionalOpenStep::DescriptorMountRecheck,
            ]
        );
        #[cfg(target_os = "linux")]
        assert_eq!(
            present_state.steps,
            vec![
                OptionalOpenStep::SecureOpen,
                OptionalOpenStep::DescriptorMountRecheck,
            ]
        );
    }

    #[test]
    fn linux_statx_buffer_parser_requires_mount_id_and_reads_kernel_offsets() {
        let with_mount_id = LinuxStatxBuffer {
            stx_mask: LINUX_STATX_MNT_ID,
            before_mnt_id: [0; 140],
            stx_mnt_id: 73,
            remaining: [0; 104],
        };
        assert_eq!(
            parse_linux_statx_mount_identity(&with_mount_id),
            Ok(MountIdentity([73, 0]))
        );

        let without_mount_id = LinuxStatxBuffer {
            stx_mask: 0,
            before_mnt_id: [0; 140],
            stx_mnt_id: 91,
            remaining: [0; 104],
        };
        assert_eq!(
            parse_linux_statx_mount_identity(&without_mount_id),
            Err(LinuxSyscallFailure::UnsupportedPlatform)
        );
    }

    #[test]
    fn linux_openat2_errno_classifier_preserves_security_and_capability_meaning() {
        for errno in [libc::EXDEV, libc::ELOOP] {
            assert_eq!(
                classify_linux_openat2_errno(errno),
                LinuxSyscallFailure::MountBoundary
            );
        }
        for errno in [libc::ENOSYS, libc::EINVAL, libc::E2BIG] {
            assert_eq!(
                classify_linux_openat2_errno(errno),
                LinuxSyscallFailure::UnsupportedPlatform
            );
        }
        assert_eq!(
            classify_linux_openat2_errno(libc::ENOENT),
            LinuxSyscallFailure::Missing
        );
        assert_eq!(
            classify_linux_openat2_errno(libc::EINTR),
            LinuxSyscallFailure::Retry
        );
        assert_eq!(
            classify_linux_openat2_errno(libc::EACCES),
            LinuxSyscallFailure::IoFailure
        );
    }

    #[test]
    fn linux_statx_errno_classifier_retries_and_reports_capability() {
        assert_eq!(
            classify_linux_statx_errno(libc::EINTR),
            LinuxSyscallFailure::Retry
        );
        assert_eq!(
            classify_linux_statx_errno(libc::ENOSYS),
            LinuxSyscallFailure::UnsupportedPlatform
        );
        assert_eq!(
            classify_linux_statx_errno(libc::EACCES),
            LinuxSyscallFailure::IoFailure
        );
    }

    #[test]
    fn expired_deadline_performs_no_mutation() {
        let (_holder, mut temp) = test_temp(705);
        let file = temp.path.join("payload");
        fs::write(&file, b"payload").unwrap();
        let error = temp
            .cleanup_contents_before(Some(Instant::now() - Duration::from_secs(1)))
            .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(fs::read(&file).unwrap(), b"payload");
    }

    #[test]
    fn nested_audit_budget_bounds_retained_entries_before_recursing() {
        let (_holder, temp) = test_temp(706);
        let first = temp.path.join("first");
        let second = temp.path.join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        fs::set_permissions(&first, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&second, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(first.join("nested"), b"one").unwrap();
        fs::write(second.join("nested"), b"two").unwrap();
        let mut state = AuditState::with_limits(32, 2, MAX_PRIVATE_TEMP_ALLOCATED_BYTES);

        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            None,
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit));
        assert!(state.entries <= state.max_entries);
    }

    #[test]
    fn audit_deadline_is_checked_inside_readdir_and_fstat_loop() {
        let (_holder, temp) = test_temp(707);
        fs::write(temp.path.join("one"), b"one").unwrap();
        fs::write(temp.path.join("two"), b"two").unwrap();
        let mut state = AuditState::default();
        state.test_deadline_after_entries = Some(1);

        let error = audit_directory(
            &temp.directory,
            0,
            &mut state,
            Some(Instant::now() + Duration::from_secs(60)),
            PolicyViolationStage::RunBoundPreMarker,
            directory_mount_identity_at(
                &temp.directory,
                PolicyViolationStage::RunBoundPreMarker,
            )
            .unwrap(),
            None,
        )
        .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(temp.path.join("one").exists());
        assert!(temp.path.join("two").exists());
    }

    #[test]
    fn deadline_after_recursive_cleanup_prevents_parent_unlink() {
        let (_holder, mut temp) = test_temp(708);
        let nested = temp.path.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(nested.join("payload"), b"payload").unwrap();
        let mut test_state = CleanupTestState {
            expire_after_recursion: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(nested.exists());
        assert!(!nested.join("payload").exists());
    }

    #[test]
    fn modified_directory_is_synced_before_cleanup_error_returns() {
        let (_holder, mut temp) = test_temp(709);
        fs::write(temp.path.join("first"), b"first").unwrap();
        fs::write(temp.path.join("second"), b"second").unwrap();
        let mut test_state = CleanupTestState {
            fail_after_first_unlink: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(test_state.sync_attempts > 0);
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    }

    #[test]
    fn cleanup_deadline_error_during_sync_remains_typed() {
        let (_holder, mut temp) = test_temp(710);
        fs::write(temp.path().join("first"), b"first").unwrap();
        fs::write(temp.path().join("second"), b"second").unwrap();
        let mut test_state = CleanupTestState {
            fail_after_first_unlink: true,
            expire_before_sync: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(test_state.sync_attempts, 1);
    }

    #[test]
    fn deadline_crossing_after_single_unlink_still_syncs_before_error() {
        let (_holder, mut temp) = test_temp(712);
        let payload = temp.path.join("payload");
        fs::write(&payload, b"payload").unwrap();
        let mut test_state = CleanupTestState {
            expire_before_sync: true,
            ..CleanupTestState::default()
        };

        let error = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();

        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(!payload.exists());
        assert_eq!(test_state.sync_attempts, 1);
        assert!(test_state.sync_completed);
        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();
        assert_eq!(test_state.sync_attempts, 2);
    }

    #[test]
    fn directory_entries_closes_owned_dir_once_on_error_and_success() {
        use std::sync::{atomic::AtomicUsize, Arc};

        let (_holder, temp) = test_temp(713);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        let close_counter = Arc::new(AtomicUsize::new(0));
        let mut error_state = DirectoryEntriesTestState {
            fail_after_metadata: Some(1),
            close_counter: Arc::clone(&close_counter),
        };
        let error = directory_entries(
            &temp.directory,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            Some(&mut error_state),
        )
        .unwrap_err();
        assert_eq!(error.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(close_counter.load(std::sync::atomic::Ordering::SeqCst), 1);

        let success_counter = Arc::new(AtomicUsize::new(0));
        let mut success_state = DirectoryEntriesTestState {
            fail_after_metadata: None,
            close_counter: Arc::clone(&success_counter),
        };
        directory_entries(
            &temp.directory,
            MAX_PRIVATE_TEMP_CLEANUP_ENTRIES,
            TempUnsafeReason::EntryLimit,
            PolicyViolationStage::RunBoundPreMarker,
            None,
            None,
            Some(&mut success_state),
        )
        .unwrap();
        assert_eq!(success_counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn successful_cleanup_syncs_modified_parent_before_return() {
        let (_holder, mut temp) = test_temp(711);
        fs::write(temp.path.join("payload"), b"payload").unwrap();
        let mut test_state = CleanupTestState::default();

        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();

        assert!(test_state.sync_attempts > 0);
    }

    #[test]
    fn empty_retry_must_sync_after_prior_final_unlink_sync_failure() {
        let (_holder, mut temp) = test_temp(714);
        let payload = temp.path.join("payload");
        fs::write(&payload, b"payload").unwrap();
        let mut test_state = CleanupTestState {
            fail_sync: true,
            ..CleanupTestState::default()
        };

        let first = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();
        assert_eq!(first.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert!(!payload.exists());
        assert_eq!(test_state.sync_attempts, 1);

        let second = temp
            .cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap_err();
        assert_eq!(second.detail, PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure));
        assert_eq!(test_state.sync_attempts, 2);

        test_state.fail_sync = false;
        temp.cleanup_contents_before_with_test_state(None, &mut test_state)
            .unwrap();
        assert_eq!(test_state.sync_attempts, 3);
    }

    #[test]
    fn decision_artifact_root_mount_revalidation_precedes_success_and_saved_scan_error() {
        let holder = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(holder.path()).unwrap();
        let anchor = crate::execution_policy::ProjectRootAnchor::resolve(&root).unwrap();
        let captured = MountIdentity([41, 0]);
        let changed = MountIdentity([42, 0]);

        for scan_result in [
            Ok(()),
            Err(temp_violation(TempUnsafeReason::EntryLimit)),
        ] {
            let error = finish_artifact_scan_with_mount_reader(
                &anchor,
                captured,
                scan_result,
                |_| Ok(changed),
            )
            .unwrap_err();
            assert_eq!(
                error.detail,
                PolicyViolationDetail::TempUnsafe(TempUnsafeReason::MountBoundary)
            );
        }

        let error = finish_artifact_scan_with_mount_reader(
            &anchor,
            captured,
            Err(temp_violation(TempUnsafeReason::EntryLimit)),
            |_| Ok(captured),
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::EntryLimit)
        );
    }

    fn research_policy_fixture() -> (
        tempfile::TempDir,
        crate::execution_policy::ResolvedExecutionPolicy,
        crate::execution_policy::ProjectRootAnchor,
    ) {
        let temporary = tempfile::tempdir().unwrap();
        let base = fs::canonicalize(temporary.path()).unwrap();
        let state_dir = base.join("state");
        let project_root = base.join("project");
        let trusted_bin = base.join("trusted-bin");
        let codex_home = base.join("codex-home");
        for directory in [&state_dir, &project_root, &trusted_bin, &codex_home] {
            fs::create_dir(directory).unwrap();
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        for name in ["codex", "pueue", "launcher"] {
            let path = trusted_bin.join(name);
            fs::write(&path, b"fixture").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let pueue_config = base.join("pueue.yml");
        fs::write(&pueue_config, b"fixture: true\n").unwrap();
        fs::set_permissions(&pueue_config, fs::Permissions::from_mode(0o600)).unwrap();
        let input = crate::execution_policy::PolicyLoadInput {
            state_dir,
            project_roots: vec![project_root.clone()],
            inherited_path: trusted_bin.into_os_string(),
            startup_environment: crate::execution_policy::StartupEnvironment::from_pairs([
                ("HOME", base.as_os_str()),
                ("PUEUE_AGENT_STATE_DIR", base.join("state").as_os_str()),
            ]),
            codex_home,
            pueue_config,
            launcher_path: base.join("trusted-bin/launcher"),
        };
        let policy = crate::execution_policy::load_or_create_policy(&input).unwrap();
        let anchor = policy.project_root_anchor(&project_root).unwrap();
        (temporary, policy, anchor)
    }

    fn research_artifact_fixture(
        policy: &crate::execution_policy::ResolvedExecutionPolicy,
        experiment_id: &str,
    ) -> std::path::PathBuf {
        let artifact_root = policy
            .project_root_anchor(&policy.project_roots[0])
            .unwrap()
            .canonical_path
            .join(PRIVATE_TEMP_ROOT)
            .join(ARTIFACTS_DIRECTORY)
            .join(experiment_id);
        fs::create_dir_all(&artifact_root).unwrap();
        for directory in [
            artifact_root.parent().unwrap(),
            artifact_root.parent().unwrap().parent().unwrap(),
            &artifact_root,
        ] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700)).unwrap();
        }
        artifact_root
    }

    #[test]
    fn verified_research_directory_records_root_and_nested_cwd() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let nested = anchor.canonical_path.join("runs").join("a");
        fs::create_dir_all(&nested).unwrap();
        fs::set_permissions(anchor.canonical_path.join("runs"), fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();

        let root_record = record_verified_research_directory(&policy, &anchor, Path::new(".")).unwrap();
        let nested_record =
            record_verified_research_directory(&policy, &anchor, Path::new("runs/a")).unwrap();
        assert_ne!(root_record.inode, nested_record.inode);
        let metadata = fs::metadata(&nested).unwrap();
        assert_eq!(nested_record.inode, {
            use std::os::unix::fs::MetadataExt;
            metadata.ino()
        });
        assert!(record_verified_research_directory(&policy, &anchor, Path::new("runs/./a")).is_err());
        assert!(record_verified_research_directory(&policy, &anchor, Path::new("../a")).is_err());
        assert!(record_verified_research_directory(&policy, &anchor, &anchor.canonical_path).is_err());
    }

    #[test]
    fn verified_research_directory_rejects_anchor_replacement() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let retired = anchor.canonical_path.with_extension("retired");
        fs::rename(&anchor.canonical_path, &retired).unwrap();
        fs::create_dir(&anchor.canonical_path).unwrap();
        fs::set_permissions(&anchor.canonical_path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(record_verified_research_directory(&policy, &anchor, Path::new(".")).is_err());
        fs::remove_dir(&anchor.canonical_path).unwrap();
        fs::rename(retired, &anchor.canonical_path).unwrap();
    }

    #[test]
    fn verified_research_directory_rejects_cwd_and_parent_replacement_between_walks() {
        for replace_parent in [true, false] {
            let (temporary, policy, anchor) = research_policy_fixture();
            let runs = anchor.canonical_path.join("runs");
            let cwd = runs.join("a");
            fs::create_dir(&runs).unwrap();
            fs::set_permissions(&runs, fs::Permissions::from_mode(0o700)).unwrap();
            fs::create_dir(&cwd).unwrap();
            fs::set_permissions(&cwd, fs::Permissions::from_mode(0o700)).unwrap();
            let retired = temporary.path().join(if replace_parent {
                "retired-cwd-parent"
            } else {
                "retired-cwd"
            });
            let mut hook = |relative: &Path| {
                if relative == Path::new("runs/a") {
                    let replaced = if replace_parent { &runs } else { &cwd };
                    fs::rename(replaced, &retired).unwrap();
                    fs::create_dir(replaced).unwrap();
                    fs::set_permissions(replaced, fs::Permissions::from_mode(0o700)).unwrap();
                    if replace_parent {
                        let replacement_cwd = replaced.join("a");
                        fs::create_dir(&replacement_cwd).unwrap();
                        fs::set_permissions(
                            replacement_cwd,
                            fs::Permissions::from_mode(0o700),
                        )
                        .unwrap();
                    }
                }
            };
            let error = record_verified_research_directory_with_test_hook(
                &policy,
                &anchor,
                Path::new("runs/a"),
                &mut hook,
            )
            .unwrap_err();
            assert_eq!(
                error.detail,
                PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
            );
        }
    }

    #[test]
    fn checkpoint_discovery_is_scoped_sorted_and_keeps_same_bytes_distinct() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-1");
        for name in ["z.txt", "a.txt"] {
            let path = artifact_root.join(name);
            fs::write(&path, b"same bytes").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let unrelated = anchor.canonical_path.join(".git");
        fs::create_dir(&unrelated).unwrap();
        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o700)).unwrap();
        for index in 0..32 {
            let path = unrelated.join(format!("unrelated-{index}"));
            fs::write(&path, b"unrelated").unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let discovery = discover_research_checkpoint_files(&policy, &anchor, "source-1").unwrap();
        assert!(discovery.complete);
        assert_eq!(discovery.omitted_at_least, 0);
        assert_eq!(discovery.records.len(), 2);
        assert!(discovery.records[0].relative_path.ends_with("/a.txt"));
        assert!(discovery.records[1].relative_path.ends_with("/z.txt"));
        assert_eq!(discovery.records[0].sha256, discovery.records[1].sha256);
        assert_ne!(discovery.records[0].relative_path, discovery.records[1].relative_path);
        assert!(discover_research_checkpoint_files(&policy, &anchor, ".").is_err());
        assert!(discover_research_checkpoint_files(&policy, &anchor, "../source-1").is_err());
        let absent = discover_research_checkpoint_files(&policy, &anchor, "other").unwrap();
        assert!(absent.records.is_empty());
        assert_eq!(absent.omitted_at_least, 0);
        assert!(absent.complete);
    }

    #[test]
    fn checkpoint_discovery_omits_unsafe_oversize_and_counted_candidates() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-2");
        for index in 0..5 {
            let path = artifact_root.join(format!("candidate-{index}.txt"));
            fs::write(&path, format!("candidate-{index}")).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let unsafe_path = artifact_root.join("unsafe.txt");
        fs::write(&unsafe_path, b"unsafe").unwrap();
        fs::set_permissions(&unsafe_path, fs::Permissions::from_mode(0o622)).unwrap();
        let oversized = artifact_root.join("oversized.bin");
        let oversized_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&oversized)
            .unwrap();
        oversized_file.set_len(MAX_RESEARCH_CHECKPOINT_FILE_BYTES + 1).unwrap();
        oversized_file.sync_all().unwrap();
        fs::set_permissions(&oversized, fs::Permissions::from_mode(0o600)).unwrap();
        let discovery = discover_research_checkpoint_files(&policy, &anchor, "source-2").unwrap();
        assert!(!discovery.complete);
        assert!(discovery.omitted_at_least >= 2);
        assert_eq!(discovery.records.len(), MAX_RESEARCH_CHECKPOINT_CANDIDATES);
        assert!(discovery.records.iter().all(|record| !record.relative_path.ends_with("unsafe.txt")));
        assert!(discovery.records.iter().all(|record| !record.relative_path.ends_with("oversized.bin")));
    }

    #[test]
    fn checkpoint_discovery_small_test_limits_bound_entries_and_total_bytes() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-3");
        for (name, bytes) in [("a.txt", b"123456".as_slice()), ("b.txt", b"abcdef".as_slice())] {
            let path = artifact_root.join(name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let limits = ResearchCheckpointLimits {
            max_files: 4,
            max_depth: 4,
            max_entries: 4096,
            max_file_bytes: 4096,
            max_total_bytes: 8,
        };
        let discovery = discover_research_checkpoint_files_with_test_limits(
            &policy,
            &anchor,
            "source-3",
            limits,
        )
        .unwrap();
        assert_eq!(discovery.records.len(), 1);
        assert_eq!(discovery.omitted_at_least, 1);
        assert!(!discovery.complete);

        let entry_limited = ResearchCheckpointLimits {
            max_entries: 1,
            ..limits
        };
        assert!(discover_research_checkpoint_files_with_test_limits(
            &policy,
            &anchor,
            "source-3",
            entry_limited,
        )
        .is_err());
    }

    #[test]
    fn checkpoint_discovery_does_not_refund_a_grown_selected_leaf() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-growth");
        for (name, bytes) in [("a.txt", b"aa".as_slice()), ("b.txt", b"bb".as_slice()),
            ("c.txt", b"cc".as_slice())]
        {
            let path = artifact_root.join(name);
            fs::write(&path, bytes).unwrap();
            fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let limits = ResearchCheckpointLimits {
            max_files: 4,
            max_depth: 4,
            max_entries: 4096,
            max_file_bytes: 4096,
            max_total_bytes: 4,
        };
        let mut hook = |relative: &Path| {
            if relative.ends_with("a.txt") {
                fs::write(anchor.canonical_path.join(relative), b"aaa").unwrap();
            }
        };

        let discovery = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-growth",
            limits,
            &mut hook,
        )
        .unwrap();

        assert_eq!(
            discovery
                .records
                .iter()
                .map(|record| record.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec![".pueue-agent/artifacts/source-growth/b.txt"]
        );
        assert_eq!(discovery.omitted_at_least, 2);
        assert!(!discovery.complete);
        assert!(discovery.records.iter().map(|record| record.logical_bytes).sum::<u64>() <= 4);
    }

    #[test]
    fn checkpoint_discovery_omits_same_size_leaf_replacement_without_refund() {
        let (temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-replacement");
        let path = artifact_root.join("a.txt");
        fs::write(&path, b"same").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let retired = temporary.path().join("retired-a.txt");
        let mut hook = |relative: &Path| {
            if relative.ends_with("a.txt") {
                let current = anchor.canonical_path.join(relative);
                fs::rename(&current, &retired).unwrap();
                fs::write(&current, b"same").unwrap();
                fs::set_permissions(current, fs::Permissions::from_mode(0o600)).unwrap();
            }
        };
        let limits = ResearchCheckpointLimits {
            max_files: 4,
            max_depth: 4,
            max_entries: 4096,
            max_file_bytes: 4096,
            max_total_bytes: 4,
        };

        let discovery = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-replacement",
            limits,
            &mut hook,
        )
        .unwrap();

        assert!(discovery.records.is_empty());
        assert_eq!(discovery.omitted_at_least, 1);
        assert!(!discovery.complete);
    }

    #[test]
    fn checkpoint_discovery_rejects_unscannable_empty_subtrees() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-depth");
        let mut deep = artifact_root.clone();
        for index in 0..4 {
            deep = deep.join(format!("d{index}"));
            fs::create_dir(&deep).unwrap();
            fs::set_permissions(&deep, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let depth_error = discover_research_checkpoint_files(&policy, &anchor, "source-depth")
            .unwrap_err();
        assert_eq!(
            depth_error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::DepthLimit)
        );

        let path_holder = tempfile::tempdir().unwrap();
        let path_directory = path_holder.path().join("scope");
        fs::create_dir(&path_directory).unwrap();
        fs::set_permissions(&path_directory, fs::Permissions::from_mode(0o700)).unwrap();
        let child = path_directory.join("child");
        fs::create_dir(&child).unwrap();
        fs::set_permissions(&child, fs::Permissions::from_mode(0o700)).unwrap();
        let path_directory = File::open(path_directory).unwrap();
        let mount = directory_mount_identity_at(
            &path_directory,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .unwrap();
        let parent_record = research_directory_record_at(
            &path_directory,
            mount,
            PolicyViolationStage::RunBoundPreMarker,
        )
        .unwrap();
        let mut state = AuditState::default();
        let mut candidates = Vec::new();
        let mut omitted = 0;
        let mut complete = true;
        let path_error = collect_research_checkpoint_candidates(
            &path_directory,
            &"x".repeat(4091),
            0,
            std::slice::from_ref(&parent_record),
            mount,
            ResearchCheckpointLimits {
                max_files: 4,
                max_depth: 4,
                max_entries: 4096,
                max_file_bytes: 4096,
                max_total_bytes: 4096,
            },
            &mut state,
            &mut candidates,
            &mut omitted,
            &mut complete,
        )
        .unwrap_err();
        assert_eq!(
            path_error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit)
        );
    }

    #[test]
    fn checkpoint_discovery_missing_scope_components_is_empty() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let missing_namespace = discover_research_checkpoint_files(&policy, &anchor, "source-1")
            .unwrap();
        assert!(missing_namespace.records.is_empty());
        assert_eq!(missing_namespace.omitted_at_least, 0);
        assert!(missing_namespace.complete);

        let service = anchor.canonical_path.join(PRIVATE_TEMP_ROOT);
        fs::create_dir(&service).unwrap();
        fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
        let missing_artifacts = discover_research_checkpoint_files(&policy, &anchor, "source-1")
            .unwrap();
        assert!(missing_artifacts.records.is_empty());
        assert_eq!(missing_artifacts.omitted_at_least, 0);
        assert!(missing_artifacts.complete);

        let artifacts = service.join(ARTIFACTS_DIRECTORY);
        fs::create_dir(&artifacts).unwrap();
        fs::set_permissions(&artifacts, fs::Permissions::from_mode(0o700)).unwrap();
        let missing_source = discover_research_checkpoint_files(&policy, &anchor, "source-1")
            .unwrap();
        assert!(missing_source.records.is_empty());
        assert_eq!(missing_source.omitted_at_least, 0);
        assert!(missing_source.complete);
    }

    #[test]
    fn checkpoint_discovery_missing_scope_replacement_preserves_prefix_identity() {
        let (temporary, policy, anchor) = research_policy_fixture();
        let service = anchor.canonical_path.join(PRIVATE_TEMP_ROOT);
        let artifacts = service.join(ARTIFACTS_DIRECTORY);
        fs::create_dir(&service).unwrap();
        fs::set_permissions(&service, fs::Permissions::from_mode(0o700)).unwrap();
        fs::create_dir(&artifacts).unwrap();
        fs::set_permissions(&artifacts, fs::Permissions::from_mode(0o700)).unwrap();
        let retired = temporary.path().join("retired-artifacts-prefix");
        let mut hook = |relative: &Path| {
            if relative == Path::new(".pueue-agent/artifacts/source-missing-prefix") {
                fs::rename(&artifacts, &retired).unwrap();
                fs::create_dir(&artifacts).unwrap();
                fs::set_permissions(&artifacts, fs::Permissions::from_mode(0o700)).unwrap();
            }
        };

        let error = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-missing-prefix",
            RESEARCH_CHECKPOINT_PRODUCTION_LIMITS,
            &mut hook,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
        );
    }

    #[test]
    fn checkpoint_discovery_rejects_scope_parent_replacement_after_inventory() {
        let (temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-parent-replacement");
        let path = artifact_root.join("a.txt");
        fs::write(&path, b"same").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let retired = temporary.path().join("retired-source-parent");
        let mut hook = |relative: &Path| {
            if relative.ends_with("a.txt") {
                let current = anchor.canonical_path.join(relative);
                let current_root = current.parent().unwrap();
                fs::rename(current_root, &retired).unwrap();
                fs::create_dir(current_root).unwrap();
                fs::set_permissions(current_root, fs::Permissions::from_mode(0o700)).unwrap();
                fs::write(current_root.join("a.txt"), b"same").unwrap();
                fs::set_permissions(
                    current_root.join("a.txt"),
                    fs::Permissions::from_mode(0o600),
                )
                .unwrap();
            }
        };
        let limits = ResearchCheckpointLimits {
            max_files: 4,
            max_depth: 4,
            max_entries: 4096,
            max_file_bytes: 4096,
            max_total_bytes: 4096,
        };

        let error = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-parent-replacement",
            limits,
            &mut hook,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
        );
    }

    #[test]
    fn checkpoint_discovery_rejects_nested_parent_replacement_before_omission() {
        let (temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-nested-parent-replacement");
        let nested = artifact_root.join("nested");
        fs::create_dir(&nested).unwrap();
        fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
        let path = nested.join("a.txt");
        fs::write(&path, b"same").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let retired = temporary.path().join("retired-nested-parent");
        let mut hook = |relative: &Path| {
            if relative.ends_with("nested/a.txt") {
                fs::rename(&nested, &retired).unwrap();
                fs::create_dir(&nested).unwrap();
                fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
                let replacement = nested.join("a.txt");
                fs::write(&replacement, b"same").unwrap();
                fs::set_permissions(replacement, fs::Permissions::from_mode(0o600)).unwrap();
            }
        };

        let error = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-nested-parent-replacement",
            RESEARCH_CHECKPOINT_PRODUCTION_LIMITS,
            &mut hook,
        )
        .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
        );
    }

    #[test]
    fn checkpoint_discovery_omits_leaf_disappearance_after_parent_revalidation() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-leaf-disappearance");
        let path = artifact_root.join("a.txt");
        fs::write(&path, b"same").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let mut hook = |relative: &Path| {
            if relative.ends_with("a.txt") {
                fs::remove_file(anchor.canonical_path.join(relative)).unwrap();
            }
        };

        let discovery = discover_research_checkpoint_files_with_test_limits_and_hook(
            &policy,
            &anchor,
            "source-leaf-disappearance",
            RESEARCH_CHECKPOINT_PRODUCTION_LIMITS,
            &mut hook,
        )
        .unwrap();
        assert!(discovery.records.is_empty());
        assert_eq!(discovery.omitted_at_least, 1);
        assert!(!discovery.complete);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn checkpoint_discovery_rejects_non_utf8_subtree_but_omits_non_utf8_leaf() {
        use std::os::unix::ffi::OsStringExt;

        let (_temporary, policy, anchor) = research_policy_fixture();
        let directory_root = research_artifact_fixture(&policy, "source-non-utf8-directory");
        let non_utf8_directory = directory_root.join(std::ffi::OsString::from_vec(vec![
            b'd', 0xff,
        ]));
        fs::create_dir(&non_utf8_directory).unwrap();
        fs::set_permissions(&non_utf8_directory, fs::Permissions::from_mode(0o700)).unwrap();
        let error = discover_research_checkpoint_files(&policy, &anchor, "source-non-utf8-directory")
            .unwrap_err();
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::InvalidEntry)
        );

        let leaf_root = research_artifact_fixture(&policy, "source-non-utf8-leaf");
        let non_utf8_leaf = leaf_root.join(std::ffi::OsString::from_vec(vec![b'f', 0xff]));
        fs::write(&non_utf8_leaf, b"leaf").unwrap();
        fs::set_permissions(&non_utf8_leaf, fs::Permissions::from_mode(0o600)).unwrap();
        let discovery = discover_research_checkpoint_files(
            &policy,
            &anchor,
            "source-non-utf8-leaf",
        )
        .unwrap();
        assert!(discovery.records.is_empty());
        assert_eq!(discovery.omitted_at_least, 1);
        assert!(!discovery.complete);
    }

    #[test]
    fn checkpoint_discovery_omits_symlink_and_fifo_leaves() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let artifact_root = research_artifact_fixture(&policy, "source-types");
        let regular = artifact_root.join("regular.txt");
        fs::write(&regular, b"regular").unwrap();
        fs::set_permissions(&regular, fs::Permissions::from_mode(0o600)).unwrap();
        let symlink_path = artifact_root.join("symlink");
        symlink(&regular, &symlink_path).unwrap();
        let fifo = artifact_root.join("fifo");
        let fifo_name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);

        let discovery = discover_research_checkpoint_files(&policy, &anchor, "source-types")
            .unwrap();
        assert_eq!(discovery.records.len(), 1);
        assert!(discovery.records[0].relative_path.ends_with("regular.txt"));
        assert!(discovery.omitted_at_least >= 2);
        assert!(!discovery.complete);
    }

    #[test]
    fn checkpoint_discovery_rejects_symlinked_scope_component() {
        let (temporary, policy, anchor) = research_policy_fixture();
        let outside = temporary.path().join("outside-scope");
        fs::create_dir(&outside).unwrap();
        fs::set_permissions(&outside, fs::Permissions::from_mode(0o700)).unwrap();
        let service = anchor.canonical_path.join(PRIVATE_TEMP_ROOT);
        symlink(&outside, &service).unwrap();

        let error = discover_research_checkpoint_files(&policy, &anchor, "source-unsafe")
            .unwrap_err();
        assert_eq!(error.code, PolicyViolationCode::TempUnsafe);
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(
                TempUnsafeReason::MountBoundary
                    | TempUnsafeReason::InvalidEntry
                    | TempUnsafeReason::IoFailure
            )
        ));
    }

    fn assert_retention_review_has_no_outputs(
        policy: &crate::execution_policy::ResolvedExecutionPolicy,
        campaign_id: &str,
        review_id: &str,
    ) {
        let review = policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE)
            .join(campaign_id)
            .join(review_id);
        if !review.exists() {
            return;
        }
        assert!(!review.join(RESEARCH_CHECKPOINT_LEAF).exists());
        for entry in fs::read_dir(review).unwrap() {
            let name = entry.unwrap().file_name();
            assert!(!retention_temp_name(&name));
        }
    }

    fn expect_retention_error(
        result: Result<RetainedResearchFile, PolicyViolation>,
    ) -> PolicyViolation {
        match result {
            Ok(_) => panic!("retention unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    fn named_retention_record(
        policy: &crate::execution_policy::ResolvedExecutionPolicy,
        campaign_id: &str,
        review_id: &str,
    ) -> ResearchFileRecord {
        let stage = retention_stage();
        let chain = open_retention_chain(policy, campaign_id, review_id, false, stage).unwrap();
        let file = open_research_file_on_mount(
            &chain.review,
            OsStr::new(RESEARCH_CHECKPOINT_LEAF),
            chain.mount,
            stage,
        )
        .unwrap();
        let snapshot = research_leaf_snapshot(&file, chain.mount, MAX_PRIVATE_TEMP_ALLOCATED_BYTES, stage).unwrap();
        let (bytes, digest) = hash_research_file(&file, MAX_PRIVATE_TEMP_ALLOCATED_BYTES, stage).unwrap();
        assert_eq!(bytes, snapshot.logical_bytes);
        retention_record(&chain, campaign_id, review_id, snapshot, digest)
    }

    #[test]
    fn verified_research_source_reads_repeatably_with_bounded_hash() {
        use sha2::{Digest, Sha256};

        let (_temporary, policy, anchor) = research_policy_fixture();
        let path = anchor.canonical_path.join("source.txt");
        let bytes = b"research source bytes\n";
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let verified = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), bytes.len() as u64)
            .unwrap();
        assert_eq!(verified.record().relative_path, "source.txt");
        assert_eq!(verified.record().logical_bytes, bytes.len() as u64);
        assert_eq!(verified.record().sha256, format!("{:x}", Sha256::digest(bytes)));
        let unrelated = anchor.canonical_path.join("unrelated");
        fs::create_dir(&unrelated).unwrap();
        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o700)).unwrap();
        assert_eq!(read_verified_research_file(&verified, bytes.len() as u64).unwrap(), bytes);
        assert_eq!(read_verified_research_file(&verified, bytes.len() as u64).unwrap(), bytes);
    }

    #[test]
    fn verified_research_source_enforces_cap_at_open_and_read() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let path = anchor.canonical_path.join("source.txt");
        fs::write(&path, b"12345").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        assert!(open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 4).is_err());
        let verified = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 5).unwrap();
        assert!(read_verified_research_file(&verified, 4).is_err());
        assert_eq!(read_verified_research_file(&verified, 5).unwrap(), b"12345");
    }

    #[test]
    fn verified_research_source_rejects_unsafe_paths_types_links_and_permissions() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let regular = anchor.canonical_path.join("source.txt");
        fs::write(&regular, b"source").unwrap();
        fs::set_permissions(&regular, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(open_verified_research_file(&policy, &anchor, Path::new("/source.txt"), 64).is_err());
        assert!(open_verified_research_file(&policy, &anchor, Path::new("../source.txt"), 64).is_err());

        let symlink_path = anchor.canonical_path.join("symlink");
        symlink(&regular, &symlink_path).unwrap();
        assert!(open_verified_research_file(&policy, &anchor, Path::new("symlink"), 64).is_err());

        let hard_link = anchor.canonical_path.join("hard-link");
        fs::hard_link(&regular, &hard_link).unwrap();
        assert!(open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 64).is_err());
        fs::remove_file(&hard_link).unwrap();

        fs::set_permissions(&regular, fs::Permissions::from_mode(0o620)).unwrap();
        assert!(open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 64).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let fifo = anchor.canonical_path.join("fifo");
            let name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            assert!(open_verified_research_file(&policy, &anchor, Path::new("fifo"), 64).is_err());
        }

        let outside = tempfile::tempdir().unwrap();
        fs::set_permissions(outside.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let outside_source = outside.path().join("source.txt");
        fs::write(&outside_source, b"outside").unwrap();
        fs::set_permissions(&outside_source, fs::Permissions::from_mode(0o600)).unwrap();
        let outside_anchor = crate::execution_policy::ProjectRootAnchor::resolve(
            &fs::canonicalize(outside.path()).unwrap(),
        )
        .unwrap();
        assert!(open_verified_research_file(&policy, &outside_anchor, Path::new("source.txt"), 64).is_err());
    }

    #[test]
    fn research_source_records_are_strictly_serialized() {
        let directory = ResearchDirectoryRecord {
            device: 1,
            inode: 2,
            owner: 3,
            mode: 0o700,
            mount_identity: [4, 5],
        };
        let record = ResearchFileRecord {
            relative_path: "source.txt".to_owned(),
            root: directory.clone(),
            parent: directory,
            device: 6,
            inode: 7,
            owner: 3,
            mode: 0o600,
            mount_identity: [8, 9],
            logical_bytes: 10,
            allocated_bytes: 512,
            sha256: "a".repeat(64),
        };
        let mut encoded = serde_json::to_value(&record).unwrap();
        encoded["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ResearchFileRecord>(encoded).is_err());
    }

    #[test]
    fn verified_research_source_rejects_parent_root_leaf_and_same_inode_changes() {
        use std::io::{Seek, SeekFrom, Write};

        let (_temporary, policy, anchor) = research_policy_fixture();
        let parent = anchor.canonical_path.join("nested");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let path = parent.join("source.txt");
        fs::write(&path, b"source").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();

        let verified = open_verified_research_file(&policy, &anchor, Path::new("nested/source.txt"), 64).unwrap();
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"mutate").unwrap();
        file.sync_all().unwrap();
        assert!(read_verified_research_file(&verified, 64).is_err());

        fs::write(&path, b"source").unwrap();
        let verified = open_verified_research_file(&policy, &anchor, Path::new("nested/source.txt"), 64).unwrap();
        let retired_parent = anchor.canonical_path.join("nested.retired");
        fs::rename(&parent, &retired_parent).unwrap();
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(parent.join("source.txt"), b"replacement").unwrap();
        fs::set_permissions(parent.join("source.txt"), fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read_verified_research_file(&verified, 64).is_err());

        fs::rename(&parent, anchor.canonical_path.join("nested.replacement")).unwrap();
        fs::rename(&retired_parent, &parent).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let verified = open_verified_research_file(&policy, &anchor, Path::new("nested/source.txt"), 64).unwrap();
        let retired_leaf = parent.join("source.retired");
        fs::rename(&path, &retired_leaf).unwrap();
        fs::write(&path, b"replacement").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(read_verified_research_file(&verified, 64).is_err());

        fs::remove_file(&path).unwrap();
        fs::rename(&retired_leaf, &path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let verified = open_verified_research_file(&policy, &anchor, Path::new("nested/source.txt"), 64).unwrap();
        let retired_root = anchor.canonical_path.with_extension("retired");
        fs::rename(&anchor.canonical_path, &retired_root).unwrap();
        fs::create_dir(&anchor.canonical_path).unwrap();
        fs::set_permissions(&anchor.canonical_path, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(read_verified_research_file(&verified, 64).is_err());
    }

    #[test]
    fn retained_research_file_round_trips_after_source_read_and_cleans_up() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        assert_eq!(read_verified_research_file(&source, 128).unwrap(), b"checkpoint bytes\n");

        let retained = retain_verified_research_file(&policy, "campaign-a", "review-a", &source).unwrap();
        let expected = retained.record().clone();
        assert_eq!(
            expected.relative_path,
            "research-checkpoints/campaign-a/review-a/checkpoint"
        );
        reverify_retained_research_file(&policy, &retained).unwrap();
        let reopened = reopen_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
        reverify_retained_research_file(&policy, &reopened).unwrap();
        drop(reopened);
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
        cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
        assert!(reopen_retained_research_file(&policy, "campaign-a", "review-a", &expected).is_err());
    }

    #[test]
    fn retained_research_file_rejects_invalid_ids_tampering_and_overwrite() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"first").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        for invalid in ["", ".", "..", "campaign/review", "a\n"] {
            assert!(retain_verified_research_file(&policy, invalid, "review-a", &source).is_err());
            assert!(retain_verified_research_file(&policy, "campaign-a", invalid, &source).is_err());
        }

        let retained = retain_verified_research_file(&policy, "campaign-a", "review-a", &source).unwrap();
        let expected = retained.record().clone();
        drop(retained);
        fs::write(&source_path, b"second").unwrap();
        let source2 = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        let overwrite_error = expect_retention_error(retain_verified_research_file(
            &policy,
            "campaign-a",
            "review-a",
            &source2,
        ));
        assert_eq!(
            overwrite_error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ExistingEntry)
        );

        let mut tampered = expected.clone();
        tampered.sha256 = "0".repeat(64);
        assert!(reopen_retained_research_file(&policy, "campaign-a", "review-a", &tampered).is_err());
        assert!(cleanup_retained_research_file(&policy, "campaign-a", "review-a", &tampered).is_err());
        let reopened = reopen_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
        drop(reopened);
        cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
    }

    #[test]
    fn retained_research_file_revalidates_same_inode_and_named_chain_changes() {
        use std::io::{Seek, SeekFrom, Write};

        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"stable").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        let retained = retain_verified_research_file(&policy, "campaign-a", "review-a", &source).unwrap();
        let expected = retained.record().clone();
        let retained_path = policy
            .code_change_state_root_path()
            .join(&expected.relative_path);
        let mut file = std::fs::OpenOptions::new().write(true).open(&retained_path).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(b"mutate").unwrap();
        file.sync_all().unwrap();
        assert!(reverify_retained_research_file(&policy, &retained).is_err());
        assert!(cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).is_err());
        drop(retained);

        let _ = fs::remove_file(&retained_path);
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        let retained = retain_verified_research_file(&policy, "campaign-a", "review-a", &source).unwrap();
        let expected = retained.record().clone();
        let review_dir = retained_path.parent().unwrap();
        let retired = review_dir.join("checkpoint.retired");
        fs::rename(&retained_path, &retired).unwrap();
        fs::write(&retained_path, b"replacement").unwrap();
        fs::set_permissions(&retained_path, fs::Permissions::from_mode(0o600)).unwrap();
        assert!(reverify_retained_research_file(&policy, &retained).is_err());
        drop(retained);
        fs::remove_file(&retained_path).unwrap();
        fs::rename(&retired, &retained_path).unwrap();
        cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
    }

    #[test]
    fn retained_reader_defers_cleanup_and_other_review_counts_quota() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"123456").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        let allocation_unit = source
            .record()
            .allocated_bytes
            .max(source.record().logical_bytes)
            .max(1);
        let mut hooks = RetentionTestHooks {
            quota_bytes: allocation_unit.saturating_mul(3),
            ..RetentionTestHooks::default()
        };
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-a",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        assert!(cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).is_err());
        let source2 = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 128).unwrap();
        let lock_contention = match retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-b",
            &source2,
            &mut hooks,
        ) {
            Ok(_) => panic!("campaign reader lock unexpectedly admitted a writer"),
            Err(error) => error,
        };
        assert!(matches!(
            lock_contention.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        drop(retained);
        let reopened = reopen_retained_research_file(
            &policy,
            "campaign-a",
            "review-a",
            &expected,
        )
        .unwrap();
        drop(reopened);
        let retained_b = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-b",
            &source2,
            &mut hooks,
        )
        .unwrap();
        let expected_b = retained_b.record().clone();
        drop(retained_b);

        let review_b = policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE)
            .join("campaign-a")
            .join("review-b");
        let explicit_temp = review_b.join(format!(
            "{RESEARCH_CHECKPOINT_TEMP_PREFIX}00000000000000000000000000000001"
        ));
        fs::write(&explicit_temp, b"temp!!").unwrap();
        fs::set_permissions(&explicit_temp, fs::Permissions::from_mode(0o600)).unwrap();
        let quota_error = match retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-c",
            &source2,
            &mut hooks,
        ) {
            Ok(_) => panic!("quota admitted an additional checkpoint"),
            Err(error) => error,
        };
        assert!(matches!(
            quota_error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit)
        ));
        assert!(!policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE)
            .join("campaign-a")
            .join("review-c")
            .join(RESEARCH_CHECKPOINT_LEAF)
            .exists());
        cleanup_retained_research_file(&policy, "campaign-a", "review-a", &expected).unwrap();
        cleanup_retained_research_file(&policy, "campaign-a", "review-b", &expected_b).unwrap();
        fs::remove_file(explicit_temp).unwrap();
    }

    #[test]
    fn retained_copy_rejects_source_mutation_and_injected_publication_failures() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        let source_bytes = vec![b'x'; RESEARCH_HASH_BUFFER_BYTES + 8];
        fs::write(&source_path, &source_bytes).unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            mutate_after_bytes: Some(RESEARCH_HASH_BUFFER_BYTES as u64),
            mutation_file: Some(std::fs::OpenOptions::new().write(true).open(&source_path).unwrap()),
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
        ));
        assert!(hooks.mutate_after_bytes.is_none());
        assert!(hooks.mutation_file.is_none());
        assert_retention_review_has_no_outputs(&policy, "campaign-a", "review-a");

        fs::write(&source_path, &source_bytes).unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            fail_file_sync: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-b",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(!hooks.fail_file_sync);
        assert_retention_review_has_no_outputs(&policy, "campaign-a", "review-b");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-b",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-a", "review-b", &expected).unwrap();

        let mut hooks = RetentionTestHooks {
            fail_publication: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-c",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(!hooks.fail_publication);
        assert_retention_review_has_no_outputs(&policy, "campaign-a", "review-c");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-c",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-a", "review-c", &expected).unwrap();
    }

    #[test]
    fn retained_publication_rolls_back_owned_final_after_parent_and_post_validation_failures() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();

        let mut hooks = RetentionTestHooks {
            fail_parent_sync: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-parent-sync",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(!hooks.fail_parent_sync);
        assert_retention_review_has_no_outputs(&policy, "campaign-a", "review-parent-sync");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-parent-sync",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-a", "review-parent-sync", &expected).unwrap();

        let mut hooks = RetentionTestHooks {
            fail_after_publication_validation: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-post-validation",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(!hooks.fail_after_publication_validation);
        assert_retention_review_has_no_outputs(&policy, "campaign-a", "review-post-validation");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-a",
            "review-post-validation",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-a", "review-post-validation", &expected).unwrap();
    }

    #[test]
    fn retained_missing_review_rejects_campaign_replacement_before_creation() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            replace_campaign_before_reopen: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-replaced-test",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IdentityChanged)
        ));
        assert!(!hooks.replace_campaign_before_reopen);
        let namespace = policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE);
        let named_campaign = namespace.join("campaign-replaced-test");
        let retired_campaign = namespace.join(".campaign-replaced");
        assert!(named_campaign.is_dir());
        assert!(!named_campaign.join("review-a").exists());
        assert!(!retired_campaign.join("review-a").exists());
    }

    #[test]
    fn retained_reverify_rejects_named_campaign_replacement_and_cleanup_recovers() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let retained = retain_verified_research_file(&policy, "campaign-chain", "review-a", &source).unwrap();
        let expected = retained.record().clone();
        let namespace = policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE);
        let campaign = namespace.join("campaign-chain");
        let retired = namespace.join("campaign-chain.retired");
        fs::rename(&campaign, &retired).unwrap();
        fs::create_dir(&campaign).unwrap();
        fs::set_permissions(&campaign, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(reverify_retained_research_file(&policy, &retained).is_err());
        drop(retained);
        assert!(cleanup_retained_research_file(&policy, "campaign-chain", "review-a", &expected).is_err());
        fs::remove_dir(&campaign).unwrap();
        fs::rename(&retired, &campaign).unwrap();
        cleanup_retained_research_file(&policy, "campaign-chain", "review-a", &expected).unwrap();
    }

    #[test]
    fn failed_downgrade_with_competing_reader_preserves_leaf_for_cleanup() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            fail_downgrade: true,
            hold_competing_reader: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-late-reader",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(
                TempUnsafeReason::RetainedPublicationRecoveryRequired
            )
        ));
        assert!(!hooks.fail_downgrade);
        assert!(hooks.competing_reader.is_some());
        assert!(policy
            .code_change_state_root_path()
            .join(RESEARCH_CHECKPOINT_NAMESPACE)
            .join("campaign-late-reader")
            .join("review-a")
            .join(RESEARCH_CHECKPOINT_LEAF)
            .exists());
        let expected = named_retention_record(&policy, "campaign-late-reader", "review-a");
        let reader = reopen_retained_research_file(
            &policy,
            "campaign-late-reader",
            "review-a",
            &expected,
        )
        .unwrap();
        let cleanup_error = cleanup_retained_research_file(
            &policy,
            "campaign-late-reader",
            "review-a",
            &expected,
        )
        .unwrap_err();
        assert!(matches!(
            cleanup_error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(policy
            .code_change_state_root_path()
            .join(&expected.relative_path)
            .exists());
        drop(reader);
        hooks.competing_reader.take();
        cleanup_retained_research_file(
            &policy,
            "campaign-late-reader",
            "review-a",
            &expected,
        )
        .unwrap();
        assert!(!policy
            .code_change_state_root_path()
            .join(&expected.relative_path)
            .exists());
    }

    #[test]
    fn failed_downgrade_without_reader_rolls_back_and_retry_succeeds() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            fail_downgrade: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-late-retry",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::IoFailure)
        ));
        assert!(!hooks.fail_downgrade);
        assert_retention_review_has_no_outputs(&policy, "campaign-late-retry", "review-a");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-late-retry",
            "review-a",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-late-retry", "review-a", &expected).unwrap();
    }

    #[test]
    fn retained_publication_removal_durability_failure_requires_recovery() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("source.txt");
        fs::write(&source_path, b"checkpoint bytes\n").unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("source.txt"), 1 << 20).unwrap();
        let mut hooks = RetentionTestHooks {
            fail_after_publication_validation: true,
            fail_rollback_parent_sync: true,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-durability",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert_eq!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(
                TempUnsafeReason::RetainedPublicationRecoveryRequired
            )
        );
        assert!(!hooks.fail_after_publication_validation);
        assert!(!hooks.fail_rollback_parent_sync);
        assert_retention_review_has_no_outputs(&policy, "campaign-durability", "review-a");
        let retained = retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-durability",
            "review-a",
            &source,
            &mut hooks,
        )
        .unwrap();
        let expected = retained.record().clone();
        drop(retained);
        cleanup_retained_research_file(&policy, "campaign-durability", "review-a", &expected).unwrap();
    }

    #[test]
    fn retained_sparse_partial_block_is_rejected_before_write_attempt() {
        let (_temporary, policy, anchor) = research_policy_fixture();
        let source_path = anchor.canonical_path.join("sparse-source.bin");
        let sparse = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&source_path)
            .unwrap();
        sparse.set_len(100).unwrap();
        sparse.sync_all().unwrap();
        fs::set_permissions(&source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let source = open_verified_research_file(&policy, &anchor, Path::new("sparse-source.bin"), 1 << 20).unwrap();
        let allocation_unit = retention_allocation_unit(&source.file, retention_stage()).unwrap();
        let quota = source
            .record()
            .allocated_bytes
            .max(source.record().logical_bytes.saturating_mul(2))
            .max(1);
        let rounded = retention_round_up(source.record().logical_bytes, allocation_unit, retention_stage()).unwrap();
        assert!(
            source
                .record()
                .allocated_bytes
                .checked_add(rounded)
                .is_some_and(|bytes| bytes > quota),
            "fixture must exercise conservative allocation rounding"
        );
        let mut hooks = RetentionTestHooks {
            quota_bytes: quota,
            ..RetentionTestHooks::default()
        };
        let error = expect_retention_error(retain_verified_research_file_with_test_hooks(
            &policy,
            "campaign-sparse",
            "review-a",
            &source,
            &mut hooks,
        ));
        assert!(matches!(
            error.detail,
            PolicyViolationDetail::TempUnsafe(TempUnsafeReason::ByteLimit)
        ));
        assert_eq!(hooks.write_attempts, 0);
        assert_retention_review_has_no_outputs(&policy, "campaign-sparse", "review-a");
    }
}

fn temp_violation_at(reason: TempUnsafeReason, stage: PolicyViolationStage) -> PolicyViolation {
    PolicyViolation::with_detail(
        PolicyViolationCode::TempUnsafe,
        stage,
        PolicyViolationDetail::TempUnsafe(reason),
    )
}

fn stage_violation(mut violation: PolicyViolation, stage: PolicyViolationStage) -> PolicyViolation {
    violation.stage = stage;
    violation
}
