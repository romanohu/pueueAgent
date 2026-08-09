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
    let task_signature = format!("{}:{task_id}", registered.pueue_group);
    repository.mark_accepted(&submission_id, task_id, &task_signature)
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
