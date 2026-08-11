use crate::{
    db::{Db, ProjectRepository, TaskObservationRepository},
    models::{Project, TaskObservation},
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
    let target_identity = cancellation_identity_for_task(task);
    let observed = TaskObservationRepository::new(db).find_by_pueue_task(
        &project.project_id,
        task_id,
        100,
    )?;
    if !observed.is_empty()
        && !observed
            .iter()
            .filter_map(observation_cancellation_identity)
            .any(|identity| identity == target_identity)
    {
        return Err(AppError::Runtime {
            operation: "revalidate task cancellation signature",
        });
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
    let final_observed_state = final_status
        .as_ref()
        .ok()
        .and_then(|tasks| final_state_for(tasks, &target_identity));
    let result_reason = match (&action_result, &final_status) {
        (Ok(()), Ok(_)) => "operator cancellation result observed".to_owned(),
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

    action_result?;
    final_status?;

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

fn final_state_for(tasks: &[PueueTask], target_identity: &str) -> Option<String> {
    let matching = tasks
        .iter()
        .filter(|task| cancellation_identity_for_task(task) == target_identity)
        .collect::<Vec<_>>();
    (matching.len() == 1).then(|| matching[0].state.clone())
}

fn cancellation_identity_for_task(task: &PueueTask) -> String {
    cancellation_identity_from_fields(&task.group, task.id, task.enqueued_at.as_deref())
}

fn observation_cancellation_identity(observation: &TaskObservation) -> Option<String> {
    let encoded = observation
        .task_signature
        .strip_prefix("pueue-task:v1:")?;
    let identity = serde_json::from_str::<StatefulTaskSignature>(encoded).ok()?;
    Some(cancellation_identity_from_fields(
        &identity.group,
        identity.id,
        identity.enqueued_at.as_deref(),
    ))
}

fn cancellation_identity_from_fields(
    group: &str,
    task_id: i64,
    enqueued_at: Option<&str>,
) -> String {
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
