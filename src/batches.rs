use std::{
    collections::HashSet,
    ffi::OsString,
    fs::File,
    io::Read,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    config,
    db::{BatchRepository, CampaignRepository, Db, ProjectRepository, SubmissionRepository},
    environment::ProjectAdmissionLock,
    execution_policy::{ProjectRootAnchor, VerifiedProjectRoot},
    models::{
        BatchJob, BatchJobStatus, BatchRequest, BatchStatus, NewBatchJob, NewBatchRequest,
        NewSubmission,
    },
    output::{bounded_redacted_text, format_state, human_header, human_summary},
    project,
    pueue::{validate_add_argv, PueueApi},
    submit, AppError,
};

pub const MAX_BATCH_REQUEST_ID_BYTES: usize = 128;
pub const MAX_BATCH_PROJECT_ID_BYTES: usize = 128;
pub const MAX_BATCH_JOB_ID_BYTES: usize = 128;
pub const MAX_BATCH_MANIFEST_HASH_BYTES: usize = 128;
pub const MAX_BATCH_ERROR_BYTES: usize = 2_048;
pub const MAX_BATCH_ARGV_JSON_BYTES: usize = 64 * 1024;
pub const MAX_BATCH_METADATA_JSON_BYTES: usize = 16 * 1024;
pub const MAX_BATCH_JOBS: usize = 128;
pub const MAX_BATCH_MANIFEST_BYTES: usize = 1024 * 1024;
pub const BATCH_DISPATCH_LEASE_SECONDS: i64 = 300;

struct BatchAdmission {
    _verified_root: VerifiedProjectRoot,
    _lock: ProjectAdmissionLock,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchManifest {
    pub manifest_hash: String,
    pub jobs: Vec<NewBatchJob>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBatchManifest {
    jobs: Vec<RawBatchJob>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawBatchJob {
    id: String,
    argv: Vec<String>,
    #[serde(default)]
    kind: Option<crate::models::SubmissionKind>,
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Serialize)]
struct CanonicalBatchManifest {
    jobs: Vec<CanonicalBatchJob>,
}

#[derive(Debug, Serialize)]
struct CanonicalBatchJob {
    id: String,
    argv: Vec<String>,
    kind: crate::models::SubmissionKind,
    metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchJobResult {
    Accepted {
        pueue_task_id: i64,
        submission_id: String,
    },
    Failed {
        error: String,
    },
}

pub fn load_manifest(path: &Path) -> Result<BatchManifest, AppError> {
    let file = File::open(path).map_err(|source| AppError::Io {
        operation: "open batch manifest",
        source,
    })?;
    let mut input = Vec::with_capacity(MAX_BATCH_MANIFEST_BYTES.min(64 * 1024));
    file.take((MAX_BATCH_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut input)
        .map_err(|source| AppError::Io {
            operation: "read batch manifest",
            source,
        })?;
    if input.len() > MAX_BATCH_MANIFEST_BYTES {
        return Err(AppError::Validation {
            field: "manifest",
            message: "must not exceed 1 MiB",
        });
    }

    let raw = serde_json::from_slice::<RawBatchManifest>(&input).map_err(|source| {
        AppError::Serialization {
            operation: "parse batch manifest",
            source,
        }
    })?;
    let jobs = raw
        .jobs
        .into_iter()
        .enumerate()
        .map(|(ordinal, raw)| {
            let metadata = raw.metadata.unwrap_or_else(|| json!({}));
            submit::validate_metadata(&metadata).map_err(|error| match error {
                AppError::Validation { message, .. } => AppError::Validation {
                    field: "metadata",
                    message,
                },
                other => other,
            })?;
            Ok(NewBatchJob::new(
                raw.id,
                i64::try_from(ordinal).map_err(|_| AppError::Validation {
                    field: "ordinal",
                    message: "is outside the supported range",
                })?,
                raw.kind
                    .unwrap_or(crate::models::SubmissionKind::Experiment),
                raw.argv,
                metadata,
            ))
        })
        .collect::<Result<Vec<_>, AppError>>()?;

    let canonical = CanonicalBatchManifest {
        jobs: jobs
            .iter()
            .map(|job| CanonicalBatchJob {
                id: job.job_id.clone(),
                argv: job.argv.clone(),
                kind: job.kind,
                metadata: job.metadata.clone(),
            })
            .collect(),
    };
    let canonical_json =
        serde_json::to_vec(&canonical).map_err(|source| AppError::Serialization {
            operation: "serialize canonical batch manifest",
            source,
        })?;
    let manifest_hash = format!("fnv1a64:{:016x}", fnv1a64(&canonical_json));
    let request = NewBatchRequest::new(
        "manifest-validation",
        "manifest-validation",
        &manifest_hash,
        jobs.clone(),
        0,
    );
    validate_new_batch(&request)?;
    Ok(BatchManifest {
        manifest_hash,
        jobs,
    })
}

pub async fn run_with<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    request_id: &str,
    manifest_path: &Path,
    requested_group: Option<&str>,
    pueue: &P,
) -> Result<BatchRequest, AppError> {
    run_with_inner(
        db,
        project_root,
        request_id,
        manifest_path,
        requested_group,
        pueue,
        None,
    )
    .await
}

pub async fn run_with_root_anchor<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    request_id: &str,
    manifest_path: &Path,
    requested_group: Option<&str>,
    pueue: &P,
    root_anchor: ProjectRootAnchor,
) -> Result<BatchRequest, AppError> {
    run_with_inner(
        db,
        project_root,
        request_id,
        manifest_path,
        requested_group,
        pueue,
        Some(root_anchor),
    )
    .await
}

async fn run_with_inner<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    request_id: &str,
    manifest_path: &Path,
    requested_group: Option<&str>,
    pueue: &P,
    root_anchor: Option<ProjectRootAnchor>,
) -> Result<BatchRequest, AppError> {
    Uuid::parse_str(request_id).map_err(|_| AppError::Validation {
        field: "request_id",
        message: "must be a valid UUID",
    })?;

    let root = project::find_root(project_root)?;
    let registered = ProjectRepository::new(db)
        .find_by_root(&root)?
        .ok_or(AppError::Runtime {
            operation: "submit batch for an unregistered project",
        })?;
    if !registered.enabled {
        return Err(AppError::Runtime {
            operation: "submit batch for a disabled project",
        });
    }

    let configured = config::load(&registered.config_path)?;
    if configured.project_id != registered.project_id {
        return Err(AppError::Configuration {
            field: "project_id registration",
        });
    }
    if configured.pueue_group != registered.pueue_group {
        return Err(AppError::Configuration {
            field: "pueue_group registration",
        });
    }
    if requested_group.is_some_and(|group| group != registered.pueue_group) {
        return Err(AppError::Validation {
            field: "group",
            message: "must exactly match the registered project group",
        });
    }
    if CampaignRepository::new(db)
        .find_live_by_project(&registered.project_id)?
        .is_some()
    {
        return Err(AppError::Validation {
            field: "submit-batch",
            message: "a managed campaign is active; use pueue-agent steer",
        });
    }
    let group = requested_group.unwrap_or(&registered.pueue_group);
    let manifest = load_manifest(manifest_path)?;
    for job in &manifest.jobs {
        validate_add_argv(&pueue_add_args(group, &job.argv))?;
    }
    let root_anchor = match root_anchor {
        Some(root_anchor) => root_anchor,
        None => ProjectRootAnchor::resolve(&registered.root_path).map_err(AppError::from)?,
    };
    if root_anchor.canonical_path != registered.root_path {
        return Err(AppError::Validation {
            field: "submit-batch.project_root",
            message: "must match the startup-pinned project root",
        });
    }
    let _admission = acquire_batch_admission(&root_anchor)?;
    if CampaignRepository::new(db)
        .find_live_by_project(&registered.project_id)?
        .is_some()
    {
        return Err(AppError::Validation {
            field: "submit-batch",
            message: "a managed campaign is active; use pueue-agent steer",
        });
    }
    let now = unix_timestamp()?;
    let request = NewBatchRequest::new(
        request_id,
        registered.project_id.clone(),
        manifest.manifest_hash,
        manifest.jobs,
        now,
    );
    let repository = BatchRepository::new(db);
    repository.create_or_get(&request)?;
    repository.recover_expired(&registered.project_id, now)?;

    let Some(claimed) = repository.claim(
        &registered.project_id,
        request_id,
        now,
        now.saturating_add(BATCH_DISPATCH_LEASE_SECONDS),
    )?
    else {
        return repository
            .find(&registered.project_id, request_id)?
            .ok_or(AppError::Runtime {
                operation: "read durable batch result",
            });
    };
    let lease_token = claimed.lease_token.clone().ok_or(AppError::Runtime {
        operation: "read claimed batch lease token",
    })?;
    let submission_repository = SubmissionRepository::new(db);
    let mut result = claimed;

    let dispatch_jobs = result
        .jobs
        .iter()
        .filter(|job| job.status == BatchJobStatus::Dispatching)
        .cloned()
        .collect::<Vec<_>>();
    for job in dispatch_jobs {
        let add_args = pueue_add_args(group, &job.argv);
        validate_add_argv(&add_args)?;

        let submission_id = Uuid::new_v4().to_string();
        submission_repository.insert_idempotent(&NewSubmission::with_kind_metadata(
            submission_id.clone(),
            registered.project_id.clone(),
            job.argv.clone(),
            now,
            job.kind,
            job.metadata.clone(),
            None,
        ))?;

        match pueue.add(&add_args).await {
            Ok(task_id) => {
                result = repository.record_job_result(
                    &registered.project_id,
                    request_id,
                    &job.job_id,
                    &lease_token,
                    BatchJobResult::Accepted {
                        pueue_task_id: task_id,
                        submission_id: submission_id.clone(),
                    },
                    unix_timestamp()?,
                )?;
                let signature = format!(
                    "provisional-submit:v1:group={group}:task-id={task_id}:intent={submission_id}"
                );
                submission_repository.mark_accepted(&submission_id, task_id, &signature)?;
            }
            Err(error) => {
                let message = bounded_redacted_text(&error.render());
                let message = if message.is_empty() {
                    "pueue add failed".to_owned()
                } else {
                    message
                };
                result = repository.record_job_result(
                    &registered.project_id,
                    request_id,
                    &job.job_id,
                    &lease_token,
                    BatchJobResult::Failed { error: message },
                    unix_timestamp()?,
                )?;
                break;
            }
        }
    }

    Ok(result)
}

fn acquire_batch_admission(root_anchor: &ProjectRootAnchor) -> Result<BatchAdmission, AppError> {
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    let project_lock = ProjectAdmissionLock::try_acquire(&verified_root)
        .map_err(AppError::from)?
        .ok_or(AppError::Runtime {
            operation: "acquire project batch admission lock",
        })?;
    let verified_root = root_anchor.verify_identity().map_err(AppError::from)?;
    Ok(BatchAdmission {
        _verified_root: verified_root,
        _lock: project_lock,
    })
}

fn pueue_add_args(group: &str, argv: &[String]) -> Vec<OsString> {
    let mut add_args = Vec::with_capacity(argv.len() + 3);
    add_args.push(OsString::from("-g"));
    add_args.push(OsString::from(group));
    add_args.push(OsString::from("--"));
    add_args.extend(argv.iter().cloned().map(OsString::from));
    add_args
}

pub fn render_batch(
    batch: &BatchRequest,
    group: &str,
    json_output: bool,
) -> Result<String, AppError> {
    let counts = batch_counts(batch);
    if json_output {
        let jobs = batch
            .jobs
            .iter()
            .map(|job| {
                json!({
                    "job_id": bounded_redacted_text(&job.job_id),
                    "ordinal": job.ordinal,
                    "status": job.status.as_str(),
                    "task_id": job.pueue_task_id,
                    "submission_id": job.submission_id.as_ref().map(|id| bounded_redacted_text(id)),
                    "error": job.last_error.as_ref().map(|error| bounded_redacted_text(error)),
                })
            })
            .collect::<Vec<_>>();
        return serde_json::to_string(&json!({
            "schema_version": 1,
            "request_id": batch.request_id,
            "project_id": batch.project_id,
            "group": bounded_redacted_text(group),
            "status": batch.status.as_str(),
            "counts": {
                "accepted": counts.accepted,
                "failed": counts.failed,
                "pending": counts.pending,
            },
            "jobs": jobs,
        }))
        .map_err(|source| AppError::Serialization {
            operation: "render batch JSON",
            source,
        });
    }

    let mut lines = vec![
        human_header("submit-batch", &batch.project_id),
        format!(
            "batch={} request={} group={}",
            bounded_redacted_text(&batch.request_id),
            bounded_redacted_text(&batch.request_id),
            bounded_redacted_text(group)
        ),
        format!("state={}", format_state(batch.status.as_str())),
        format!(
            "accepted={} failed={} pending={}",
            counts.accepted, counts.failed, counts.pending
        ),
    ];
    for job in &batch.jobs {
        let mut line = format!(
            "job={} ordinal={} state={}",
            bounded_redacted_text(&job.job_id),
            job.ordinal,
            format_state(job.status.as_str())
        );
        if let Some(task_id) = job.pueue_task_id {
            line.push_str(&format!(" task={task_id}"));
        }
        if let Some(error) = &job.last_error {
            line.push_str(&format!(" error={}", bounded_redacted_text(error)));
        }
        lines.push(line);
    }
    lines.push(human_summary(format!(
        "batch {} accepted={} failed={} pending={}",
        batch.status.as_str(),
        counts.accepted,
        counts.failed,
        counts.pending
    )));
    Ok(lines.join("\n"))
}

#[derive(Debug, Clone, Copy)]
struct BatchCounts {
    accepted: usize,
    failed: usize,
    pending: usize,
}

fn batch_counts(batch: &BatchRequest) -> BatchCounts {
    let mut counts = BatchCounts {
        accepted: 0,
        failed: 0,
        pending: 0,
    };
    for job in &batch.jobs {
        match job.status {
            BatchJobStatus::Accepted => counts.accepted += 1,
            BatchJobStatus::Failed => counts.failed += 1,
            BatchJobStatus::Pending | BatchJobStatus::Dispatching => counts.pending += 1,
        }
    }
    counts
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn unix_timestamp() -> Result<i64, AppError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read the system clock for batch submission",
        })?
        .as_secs();
    i64::try_from(seconds).map_err(|_| AppError::Runtime {
        operation: "represent the batch submission timestamp",
    })
}

pub(crate) fn validate_new_batch(request: &NewBatchRequest) -> Result<(), AppError> {
    validate_bounded_nonempty(
        "request_id",
        &request.request_id,
        MAX_BATCH_REQUEST_ID_BYTES,
    )?;
    validate_bounded_nonempty(
        "project_id",
        &request.project_id,
        MAX_BATCH_PROJECT_ID_BYTES,
    )?;
    validate_bounded_nonempty(
        "manifest_hash",
        &request.manifest_hash,
        MAX_BATCH_MANIFEST_HASH_BYTES,
    )?;
    if request.jobs.is_empty() || request.jobs.len() > MAX_BATCH_JOBS {
        return Err(AppError::Validation {
            field: "jobs",
            message: "must contain between 1 and 128 jobs",
        });
    }

    let mut job_ids = HashSet::with_capacity(request.jobs.len());
    let mut ordinals = HashSet::with_capacity(request.jobs.len());
    for job in &request.jobs {
        validate_bounded_nonempty("job_id", &job.job_id, MAX_BATCH_JOB_ID_BYTES)?;
        if job.ordinal < 0 {
            return Err(AppError::Validation {
                field: "ordinal",
                message: "must be non-negative",
            });
        }
        if !job_ids.insert(&job.job_id) {
            return Err(AppError::Validation {
                field: "job_id",
                message: "must be unique within a batch",
            });
        }
        if !ordinals.insert(job.ordinal) {
            return Err(AppError::Validation {
                field: "ordinal",
                message: "must be unique within a batch",
            });
        }
        if job.argv.is_empty() {
            return Err(AppError::Validation {
                field: "argv",
                message: "must not be empty",
            });
        }
        let argv_json =
            serde_json::to_vec(&job.argv).map_err(|source| AppError::Serialization {
                operation: "serialize batch job arguments",
                source,
            })?;
        if argv_json.len() > MAX_BATCH_ARGV_JSON_BYTES {
            return Err(AppError::Validation {
                field: "argv",
                message: "serialized arguments exceed the batch limit",
            });
        }
        if !job.metadata.is_object() {
            return Err(AppError::Validation {
                field: "metadata",
                message: "must be a JSON object",
            });
        }
        let metadata_json =
            serde_json::to_vec(&job.metadata).map_err(|source| AppError::Serialization {
                operation: "serialize batch job metadata",
                source,
            })?;
        if metadata_json.len() > MAX_BATCH_METADATA_JSON_BYTES {
            return Err(AppError::Validation {
                field: "metadata",
                message: "serialized metadata exceeds the batch limit",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_error(error: &str) -> Result<(), AppError> {
    validate_bounded_nonempty("error", error, MAX_BATCH_ERROR_BYTES)
}

pub(crate) fn validate_accepted_result(
    pueue_task_id: i64,
    submission_id: &str,
) -> Result<(), AppError> {
    if pueue_task_id < 0 {
        return Err(AppError::Validation {
            field: "pueue_task_id",
            message: "must be non-negative",
        });
    }
    validate_bounded_nonempty("submission_id", submission_id, MAX_BATCH_JOB_ID_BYTES)
}

pub(crate) fn derive_request_status(jobs: &[BatchJob]) -> BatchStatus {
    if jobs
        .iter()
        .all(|job| job.status == BatchJobStatus::Accepted)
    {
        return BatchStatus::Completed;
    }
    if jobs.iter().any(|job| job.status == BatchJobStatus::Failed) {
        if jobs
            .iter()
            .any(|job| job.status == BatchJobStatus::Accepted)
        {
            BatchStatus::Partial
        } else {
            BatchStatus::Failed
        }
    } else if jobs
        .iter()
        .any(|job| job.status == BatchJobStatus::Accepted)
    {
        BatchStatus::Accepted
    } else if jobs
        .iter()
        .any(|job| job.status == BatchJobStatus::Dispatching)
    {
        BatchStatus::Dispatching
    } else {
        BatchStatus::Pending
    }
}

fn validate_bounded_nonempty(
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), AppError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(AppError::Validation {
            field,
            message: "must be non-empty and within the size limit",
        });
    }
    Ok(())
}
