use std::{
    ffi::OsString,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use uuid::Uuid;

use crate::{
    config,
    db::{Db, ProjectRepository, SubmissionRepository},
    models::{NewSubmission, Submission},
    paths, project,
    pueue::{CommandPueue, PueueApi},
    AppError,
};

pub async fn run(project_root: &Path, args: &[OsString]) -> Result<Submission, AppError> {
    let db = Db::open(&paths::state_db_path()?)?;
    run_with(&db, project_root, args, &CommandPueue::default()).await
}

pub async fn run_with<P: PueueApi + ?Sized>(
    db: &Db,
    project_root: &Path,
    args: &[OsString],
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
    let submission_id = Uuid::new_v4().to_string();
    let intent = NewSubmission::new(
        submission_id.clone(),
        registered.project_id,
        argv,
        created_at,
    );
    let repository = SubmissionRepository::new(db);
    repository.insert_idempotent(&intent)?;

    let mut add_args = Vec::with_capacity(args.len() + 3);
    add_args.push(OsString::from("-g"));
    add_args.push(registered.pueue_group.clone().into());
    add_args.push(OsString::from("--"));
    add_args.extend_from_slice(args);

    let task_id = pueue.add(&add_args).await?;
    let task_signature =
        provisional_task_signature(&registered.pueue_group, task_id, &submission_id);
    repository.mark_accepted(&submission_id, task_id, &task_signature)
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
