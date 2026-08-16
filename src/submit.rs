use std::{
    env,
    ffi::OsString,
    fs::File,
    io::Read,
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    campaign::CampaignCoordinator,
    config,
    db::{AgentRunRepository, CampaignRepository, Db, ProjectRepository, SubmissionRepository},
    execution_policy::{load_existing_policy, CampaignLimits},
    models::{NewSubmission, Submission, SubmissionKind},
    output::{bounded_redacted_text, format_state, human_header, human_summary},
    paths, project,
    pueue::{configured_pueue, validate_add_argv, PueueApi},
    service::ServicePaths,
    state,
    AppError,
};

use serde_json::Value;

const MAX_METADATA_BYTES: usize = 16 * 1024;
const MAX_METADATA_DEPTH: usize = 8;
const MAX_METADATA_OBJECT_KEYS: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 64;
const MAX_METADATA_STRING_BYTES: usize = 1024;
const MAX_METADATA_ARRAY_ITEMS: usize = 64;

#[derive(Debug, Clone)]
pub struct SubmitOptions {
    pub kind: SubmissionKind,
    pub metadata: Value,
    pub origin_agent_run_id: Option<i64>,
}

impl SubmitOptions {
    pub fn new(kind: SubmissionKind, metadata: Value, origin_agent_run_id: Option<i64>) -> Self {
        Self {
            kind,
            metadata,
            origin_agent_run_id,
        }
    }
}

impl Default for SubmitOptions {
    fn default() -> Self {
        Self::new(
            SubmissionKind::Experiment,
            Value::Object(Default::default()),
            None,
        )
    }
}

pub async fn run(project_root: &Path, args: &[OsString]) -> Result<Submission, AppError> {
    let root = project::find_root(project_root)?;
    let service_paths = ServicePaths::from_environment(&root, None)?;
    let state_db = paths::state_db_path()?;
    let read_db = Db::open_read_only(&state_db)?;
    let registered = ProjectRepository::new(&read_db)
        .find_by_root(&root)?
        .ok_or(AppError::Runtime {
            operation: "submit for an unregistered project",
        })?;
    let project_roots = ProjectRepository::new(&read_db)
        .list_all()?
        .into_iter()
        .map(|project| project.root_path)
        .collect();
    let launcher_path = env::current_exe().map_err(|source| AppError::Io {
        operation: "resolve submit launcher",
        source,
    })?;
    let policy = Arc::new(load_existing_policy(&service_paths.policy_load_input(
        project_roots,
        launcher_path,
    ))?);
    let limits = policy.campaign_limits;
    let pueue = configured_pueue(policy)?;
    drop(read_db);
    let db = Db::open(&state_db)?;
    let options = SubmitOptions::new(
        SubmissionKind::Experiment,
        Value::Object(Default::default()),
        origin_from_environment(&registered.project_id)?,
    );
    run_with_options(&db, project_root, args, &options, &limits, &pueue).await
}

pub async fn run_with<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    args: &[OsString],
    limits: &CampaignLimits,
    pueue: &P,
) -> Result<Submission, AppError> {
    run_with_options(
        db,
        project_root,
        args,
        &SubmitOptions::default(),
        limits,
        pueue,
    )
    .await
}

pub async fn run_with_options<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    args: &[OsString],
    options: &SubmitOptions,
    limits: &CampaignLimits,
    pueue: &P,
) -> Result<Submission, AppError> {
    if args.is_empty() {
        return Err(AppError::Configuration {
            field: "submission.command",
        });
    }

    let root = project::find_root(project_root)?;
    let registered = ProjectRepository::new(db)
        .find_by_root(&root)?
        .ok_or(AppError::Runtime {
            operation: "submit for an unregistered project",
        })?;
    if !registered.enabled {
        return Err(AppError::Runtime {
            operation: "submit for a disabled project",
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
    validate_metadata(&options.metadata)?;
    validate_active_origin(db, &registered.project_id, options.origin_agent_run_id)?;
    if CampaignRepository::new(db)
        .find_live_by_project(&registered.project_id)?
        .is_some()
    {
        return Err(AppError::Validation {
            field: "submit",
            message: "a managed campaign is active; use pueue-agent steer",
        });
    }

    let argv = args
        .iter()
        .map(|argument| {
            argument
                .to_str()
                .map(ToOwned::to_owned)
                .ok_or(AppError::Configuration {
                    field: "submission.command encoding",
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let created_at = unix_timestamp()?;
    if options.kind == SubmissionKind::Experiment {
        let objective = state::load_objective(&root)?;
        return CampaignCoordinator::new(db, pueue, *limits)
            .start_baseline(
                &registered,
                &objective,
                &argv,
                &options.metadata,
                options.origin_agent_run_id,
                created_at,
            )
            .await;
    }

    let mut add_args = Vec::with_capacity(args.len() + 3);
    add_args.push(OsString::from("-g"));
    add_args.push(registered.pueue_group.clone().into());
    add_args.push(OsString::from("--"));
    add_args.extend_from_slice(args);
    validate_add_argv(&add_args)?;

    let submission_id = Uuid::new_v4().to_string();
    let intent = NewSubmission::with_kind_metadata(
        submission_id.clone(),
        registered.project_id,
        argv,
        created_at,
        options.kind,
        options.metadata.clone(),
        options.origin_agent_run_id,
    );
    let repository = SubmissionRepository::new(db);
    repository.insert_idempotent(&intent)?;

    let task_id = pueue.add(&add_args).await?;
    let task_signature =
        provisional_task_signature(&registered.pueue_group, task_id, &submission_id);
    repository.mark_accepted(&submission_id, task_id, &task_signature)
}

pub fn load_metadata(
    metadata_path: Option<&Path>,
    metadata_json: Option<&str>,
) -> Result<Value, AppError> {
    let input = match (metadata_path, metadata_json) {
        (Some(_), Some(_)) => {
            return Err(AppError::Validation {
                field: "submit.metadata",
                message: "--metadata and --metadata-json cannot be used together",
            });
        }
        (Some(path), None) => read_metadata_file(path)?,
        (None, Some(json)) => json.as_bytes().to_vec(),
        (None, None) => return Ok(Value::Object(Default::default())),
    };
    validate_metadata_bytes(&input)?;
    let parsed = serde_json::from_slice(&input).map_err(|source| AppError::Serialization {
        operation: "parse submission metadata",
        source,
    })?;
    validate_metadata(&parsed)?;
    Ok(parsed)
}

pub fn origin_from_environment(project_id: &str) -> Result<Option<i64>, AppError> {
    let run_id = env::var_os("PUEUE_AGENT_RUN_ID");
    let origin_project_id = env::var_os("PUEUE_AGENT_PROJECT_ID");
    match (run_id, origin_project_id) {
        (None, None) => Ok(None),
        (Some(run_id), Some(origin_project_id)) => origin_from_values(
            Some(
                run_id
                    .to_str()
                    .ok_or_else(|| metadata_validation("agent run ID must be UTF-8"))?,
            ),
            Some(
                origin_project_id
                    .to_str()
                    .ok_or_else(|| metadata_validation("agent project ID must be UTF-8"))?,
            ),
            project_id,
        ),
        _ => Err(metadata_validation(
            "agent origin environment is incomplete",
        )),
    }
}

pub fn origin_from_values(
    run_id: Option<&str>,
    origin_project_id: Option<&str>,
    project_id: &str,
) -> Result<Option<i64>, AppError> {
    match (run_id, origin_project_id) {
        (None, None) => Ok(None),
        (Some(run_id), Some(origin_project_id)) => {
            let run_id = run_id
                .parse::<i64>()
                .ok()
                .filter(|run_id| *run_id > 0)
                .ok_or_else(|| metadata_validation("agent run ID must be a positive integer"))?;
            if origin_project_id != project_id {
                return Err(metadata_validation(
                    "agent project ID does not match the submission project",
                ));
            }
            Ok(Some(run_id))
        }
        _ => Err(metadata_validation(
            "agent origin environment is incomplete",
        )),
    }
}

pub fn render_submission(
    submission: &Submission,
    group: &str,
    json: bool,
) -> Result<String, AppError> {
    let task_id = submission.pueue_task_id.ok_or(AppError::Runtime {
        operation: "read accepted Pueue task ID",
    })?;
    if json {
        Ok(serde_json::json!({
            "submission_id": submission.submission_id,
            "task_id": task_id,
            "kind": submission.kind.as_str(),
            "group": group,
            "state": submission.status.as_str(),
        })
        .to_string())
    } else {
        Ok([
            human_header("submit", &submission.project_id),
            format!(
                "sub={} task={} kind={} group={} state={}",
                bounded_redacted_text(&submission.submission_id),
                task_id,
                submission.kind.as_str(),
                bounded_redacted_text(group),
                format_state(submission.status.as_str()),
            ),
            human_summary(format!("submission {}", submission.status.as_str())),
        ]
        .join("\n"))
    }
}

fn validate_active_origin(
    db: &Db,
    project_id: &str,
    origin_agent_run_id: Option<i64>,
) -> Result<(), AppError> {
    let Some(origin_agent_run_id) = origin_agent_run_id else {
        return Ok(());
    };
    let active = AgentRunRepository::new(db).find_active_by_project(project_id)?;
    if active.is_some_and(|run| run.run_id == origin_agent_run_id) {
        Ok(())
    } else {
        Err(AppError::Validation {
            field: "origin_agent_run_id",
            message: "must identify the active agent run in the submission project",
        })
    }
}

pub(crate) fn validate_metadata(metadata: &Value) -> Result<(), AppError> {
    let serialized = serde_json::to_vec(metadata).map_err(|source| AppError::Serialization {
        operation: "serialize submission metadata",
        source,
    })?;
    validate_metadata_bytes(&serialized)?;
    if !metadata.is_object() {
        return Err(metadata_validation("must be a JSON object"));
    }
    validate_metadata_value(metadata, 1)
}

fn read_metadata_file(path: &Path) -> Result<Vec<u8>, AppError> {
    let file = File::open(path).map_err(|source| AppError::Io {
        operation: "open submission metadata",
        source,
    })?;
    let mut input = Vec::with_capacity(MAX_METADATA_BYTES + 1);
    file.take((MAX_METADATA_BYTES + 1) as u64)
        .read_to_end(&mut input)
        .map_err(|source| AppError::Io {
            operation: "read submission metadata",
            source,
        })?;
    validate_metadata_bytes(&input)?;
    Ok(input)
}

fn validate_metadata_bytes(input: &[u8]) -> Result<(), AppError> {
    if input.len() > MAX_METADATA_BYTES {
        return Err(metadata_validation("must not exceed 16 KiB"));
    }
    Ok(())
}

fn validate_metadata_value(value: &Value, depth: usize) -> Result<(), AppError> {
    if depth > MAX_METADATA_DEPTH {
        return Err(metadata_validation("must not exceed depth 8"));
    }
    match value {
        Value::Object(object) => {
            if object.len() > MAX_METADATA_OBJECT_KEYS {
                return Err(metadata_validation("objects must contain at most 32 keys"));
            }
            for (key, child) in object {
                if key.len() > MAX_METADATA_KEY_BYTES {
                    return Err(metadata_validation("keys must not exceed 64 bytes"));
                }
                validate_metadata_value(child, depth + 1)?;
            }
        }
        Value::Array(items) => {
            if items.len() > MAX_METADATA_ARRAY_ITEMS {
                return Err(metadata_validation("arrays must contain at most 64 items"));
            }
            for child in items {
                validate_metadata_value(child, depth + 1)?;
            }
        }
        Value::String(text) if text.len() > MAX_METADATA_STRING_BYTES => {
            return Err(metadata_validation("strings must not exceed 1024 bytes"));
        }
        _ => {}
    }
    Ok(())
}

fn metadata_validation(message: &'static str) -> AppError {
    AppError::Validation {
        field: "submit.metadata",
        message,
    }
}

/// Builds the submit-time task identity placeholder.
///
/// This signature is deliberately provisional: reconciliation replaces it with
/// the authoritative enqueue/start/end Pueue task signature once a status
/// observation is available. Include the submission intent ID so two accepted
/// submissions cannot collide if Pueue reuses a numeric task ID.
fn provisional_task_signature(group: &str, task_id: i64, submission_id: &str) -> String {
    format!("provisional-submit:v1:group={group}:task-id={task_id}:intent={submission_id}")
}

fn unix_timestamp() -> Result<i64, AppError> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read the system clock for submission",
        })?
        .as_secs();
    i64::try_from(seconds).map_err(|_| AppError::Runtime {
        operation: "represent the submission timestamp",
    })
}
