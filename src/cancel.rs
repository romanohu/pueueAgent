use crate::{
    db::{Db, EventRepository, ProjectRepository, TaskObservationRepository},
    models::{EventKind, NewEvent, Project, TaskObservation},
    output::{bounded_redacted_text, format_state, human_header, human_summary, render_id},
    pueue::{PueueApi, PueueTask},
    reconcile::task_signature,
    AppError,
};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelResult {
    pub task_id: i64,
    pub task_signature: String,
    pub requested_state: String,
    pub final_observed_state: Option<String>,
    pub action: &'static str,
    pub kill_sent: bool,
}

pub async fn cancel_task_with(
    db: &Db,
    project: &Project,
    pueue: &impl PueueApi,
    task_id: i64,
    now: i64,
) -> Result<CancelResult, AppError> {
    let tasks = pueue.status_json().await?;
    let matching_tasks = tasks
        .iter()
        .filter(|task| task.id == task_id)
        .collect::<Vec<_>>();
    if matching_tasks.len() != 1 {
        return Err(AppError::Runtime {
            operation: "revalidate task cancellation target",
        });
    }

    let task = matching_tasks[0];
    if task.group != project.pueue_group
        || !matches!(task.state.to_ascii_lowercase().as_str(), "queued" | "running")
    {
        return Err(AppError::Runtime {
            operation: "validate task cancellation target",
        });
    }

    let signature = task_signature(task);
    let Some(target_identity) = cancellation_identity_for_task(task) else {
        return Err(AppError::Runtime {
            operation: "validate task cancellation identity",
        });
    };
    let observed = TaskObservationRepository::new(db).find_by_pueue_task(
        &project.project_id,
        task_id,
        100,
    )?;
    if !observed.is_empty() {
        if !observed
            .iter()
            .filter_map(observation_cancellation_identity)
            .any(|identity| identity == target_identity)
        {
            return Err(AppError::Runtime {
                operation: "revalidate task cancellation signature",
            });
        }
    }

    let action = if task.state.eq_ignore_ascii_case("running") {
        "kill"
    } else {
        "remove"
    };
    let requested_action = match action {
        "kill" => "kill_requested",
        "remove" => "remove_requested",
        _ => unreachable!("validated cancellation action"),
    };

    let repository = ProjectRepository::new(db);
    repository.record_task_cancellation(
        project,
        task_id,
        &signature,
        &task.state,
        action,
        requested_action,
        "operator cancellation requested",
        now,
    )?;

    let action_result = match action {
        "kill" => pueue.kill(task_id).await,
        "remove" => pueue.remove(task_id).await,
        _ => unreachable!("validated cancellation action"),
    };
    let final_status = pueue.status_json().await;
    let observed_final_state = final_status
        .as_ref()
        .ok()
        .and_then(|tasks| final_state_for(tasks, Some(&target_identity)));
    let termination_confirmed = match (action, &action_result, &final_status) {
        ("remove", Ok(()), Ok(tasks)) => matching_tasks_for(tasks, &target_identity).is_empty(),
        ("kill", Ok(()), Ok(tasks)) => {
            let matching = matching_tasks_for(tasks, &target_identity);
            matching.len() == 1 && matching[0].is_terminal()
        }
        _ => false,
    };
    let final_observed_state = if termination_confirmed
        && action == "remove"
        && observed_final_state.is_none()
    {
        Some("Removed".to_owned())
    } else {
        observed_final_state
    };
    let result_reason = match (&action_result, &final_status) {
        (Ok(()), Ok(_)) if termination_confirmed => {
            "operator cancellation result observed".to_owned()
        }
        (Ok(()), Ok(_)) => match (action, final_observed_state.as_deref()) {
            ("remove", Some(state)) => format!(
                "termination failure: queued task was not removed; final state={state}"
            ),
            ("remove", None) => {
                "termination failure: queued task removal was not confirmed".to_owned()
            }
            ("kill", Some(state)) => format!(
                "termination failure: kill succeeded but task remained nonterminal; final state={state}"
            ),
            ("kill", None) => {
                "termination failure: terminal state was not confirmed after kill".to_owned()
            }
            _ => unreachable!("validated cancellation action"),
        },
        (Err(error), Ok(_)) => format!("Pueue {action} failed: {error}"),
        (Ok(()), Err(error)) => format!("post-{action} Pueue status failed: {error}"),
        (Err(action_error), Err(status_error)) => {
            format!("Pueue {action} failed: {action_error}; post-{action} status failed: {status_error}")
        }
    };
    repository.record_task_cancellation(
        project,
        task_id,
        &signature,
        &task.state,
        action,
        final_observed_state.as_deref().unwrap_or("unobserved"),
        &result_reason,
        now,
    )?;

    if !termination_confirmed {
        let _ = EventRepository::new(db).insert_idempotent(&NewEvent::new(
            &project.project_id,
            EventKind::TerminationFailed,
            format!("termination:operator-cancel:v1:task={task_id}:at={now}"),
            serde_json::json!({
                "source": "operator_cancel",
                "task_id": task_id,
                "task_signature": bounded_redacted_text(&signature),
                "action": action,
                "requested_state": bounded_redacted_text(&task.state),
                "final_state": final_observed_state
                    .as_deref()
                    .map(bounded_redacted_text),
                "error": bounded_redacted_text(&result_reason),
            }),
            now,
            now,
        ))?;
    }

    action_result?;
    final_status?;
    if !termination_confirmed {
        return Err(AppError::Runtime {
            operation: "confirm task cancellation termination",
        });
    }

    Ok(CancelResult {
        task_id,
        task_signature: signature,
        requested_state: task.state.clone(),
        final_observed_state,
        action,
        kill_sent: action == "kill",
    })
}

pub fn render_cancel_result(project: &Project, result: &CancelResult, json: bool) -> String {
    let final_state = result.final_observed_state.as_deref();
    if json {
        return serde_json::json!({
            "schema_version": 1,
            "project_id": project.project_id,
            "task_id": result.task_id,
            "action": result.action,
            "kill_sent": result.kill_sent,
            "state": final_state.map(bounded_redacted_text),
        })
        .to_string();
    }

    let state = final_state.unwrap_or("unobserved");
    format!(
        "{}\n{} state={}\n{}",
        human_header("cancel", &project.project_id),
        format!("{} action={}", render_id("task", result.task_id), result.action),
        format_state(&bounded_redacted_text(state)),
        human_summary("Pueue task cancellation requested"),
    )
}

fn final_state_for(tasks: &[PueueTask], target_identity: Option<&str>) -> Option<String> {
    let target_identity = target_identity?;
    let matching = matching_tasks_for(tasks, target_identity);
    (matching.len() == 1).then(|| matching[0].state.clone())
}

fn matching_tasks_for<'a>(tasks: &'a [PueueTask], target_identity: &str) -> Vec<&'a PueueTask> {
    tasks
        .iter()
        .filter(|task| cancellation_identity_for_task(task).as_deref() == Some(target_identity))
        .collect()
}

fn cancellation_identity_for_task(task: &PueueTask) -> Option<String> {
    let enqueued_at = task.enqueued_at.as_deref()?;
    Some(cancellation_identity_from_fields(
        &task.group,
        task.id,
        enqueued_at,
    ))
}

fn observation_cancellation_identity(observation: &TaskObservation) -> Option<String> {
    let encoded = observation
        .task_signature
        .strip_prefix("pueue-task:v1:")?;
    let identity = serde_json::from_str::<StatefulTaskSignature>(encoded).ok()?;
    let enqueued_at = identity.enqueued_at.as_deref()?;
    Some(cancellation_identity_from_fields(
        &identity.group,
        identity.id,
        enqueued_at,
    ))
}

fn cancellation_identity_from_fields(group: &str, task_id: i64, enqueued_at: &str) -> String {
    let identity = serde_json::json!({
        "group": group,
        "id": task_id,
        "enqueued_at": enqueued_at,
    });
    format!("pueue-cancel-task:v1:{identity}")
}

#[derive(Debug, Deserialize)]
struct StatefulTaskSignature {
    group: String,
    id: i64,
    enqueued_at: Option<String>,
}
