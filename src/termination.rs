use serde_json::json;

use crate::{
    config::PatternAction,
    db::{Db, EventRepository, IncidentRepository, ProjectRepository, TerminationRequestRepository},
    detect::{Observation, ObservationState},
    models::{EventKind, NewEvent, NewTerminationRequest, TerminationRequestStatus},
    pueue::PueueApi,
    reconcile::task_signature,
    AppError,
};

pub type TerminationRequestId = i64;

#[derive(Debug, Default, Clone, Copy)]
pub struct TerminationPolicy;

impl TerminationPolicy {
    pub fn should_kill(&self, observation: &Observation) -> bool {
        observation.state() == &ObservationState::Active
            && observation.action() == PatternAction::Kill
            && observation.task_signature().is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminationOutcome {
    Confirmed,
    TimedOut,
    Failed,
    AlreadyTerminal,
}

pub struct TerminationManager<'db, P = NoPueue> {
    db: &'db Db,
    pueue: P,
}

impl<'db> TerminationManager<'db, NoPueue> {
    pub fn new_without_pueue(db: &'db Db) -> Self {
        Self { db, pueue: NoPueue }
    }
}

impl<'db, P> TerminationManager<'db, P> {
    pub fn new(db: &'db Db, pueue: P) -> Self {
        Self { db, pueue }
    }

    pub fn request(
        &self,
        incident_id: i64,
        task_signature: impl Into<String>,
    ) -> Result<TerminationRequestId, AppError> {
        self.request_with_reason(
            incident_id,
            task_signature,
            "policy requested termination",
            unix_timestamp()?,
            None,
        )
    }

    pub fn request_with_reason(
        &self,
        incident_id: i64,
        task_signature: impl Into<String>,
        reason: impl Into<String>,
        requested_at: i64,
        grace_until: Option<i64>,
    ) -> Result<TerminationRequestId, AppError> {
        let incident = IncidentRepository::new(self.db)
            .find_by_id(incident_id)?
            .ok_or(AppError::Runtime {
                operation: "request termination for missing incident",
            })?;
        let request = NewTerminationRequest::new(
            incident.incident_id,
            incident.project_id,
            task_signature.into(),
            reason.into(),
            requested_at,
            grace_until,
        );
        Ok(TerminationRequestRepository::new(self.db)
            .insert_idempotent(&request)?
            .request_id)
    }
}

impl<'db, P> TerminationManager<'db, P>
where
    P: PueueApi,
{
    pub async fn execute(
        &self,
        request_id: TerminationRequestId,
    ) -> Result<TerminationOutcome, AppError> {
        let repository = TerminationRequestRepository::new(self.db);
        let request = repository
            .find_by_id(request_id)?
            .ok_or(AppError::Runtime {
                operation: "execute missing termination request",
            })?;
        match request.status {
            TerminationRequestStatus::Confirmed => return Ok(TerminationOutcome::Confirmed),
            TerminationRequestStatus::TimedOut => return Ok(TerminationOutcome::TimedOut),
            TerminationRequestStatus::Failed => return Ok(TerminationOutcome::Failed),
            TerminationRequestStatus::Requested | TerminationRequestStatus::Sent => {}
        }

        let project = ProjectRepository::new(self.db)
            .find_by_id(&request.project_id)?
            .ok_or(AppError::Runtime {
                operation: "find project for termination request",
            })?;
        let tasks = self.pueue.status_json().await?;
        let matching = tasks
            .iter()
            .find(|task| {
                task.group.as_str() == project.pueue_group.as_str()
                    && task_signature(task).as_str() == request.task_signature.as_str()
            })
            .cloned();
        let Some(task) = matching else {
            repository.update_result(
                request.request_id,
                TerminationRequestStatus::Confirmed,
                Some(unix_timestamp()?),
                Some("task signature is no longer active"),
            )?;
            return Ok(TerminationOutcome::AlreadyTerminal);
        };
        if !task.is_running() {
            repository.update_result(
                request.request_id,
                TerminationRequestStatus::Confirmed,
                Some(unix_timestamp()?),
                Some("task is already terminal or non-running"),
            )?;
            return Ok(TerminationOutcome::AlreadyTerminal);
        }

        repository.transition_status(request.request_id, TerminationRequestStatus::Sent)?;
        let kill_result = self.pueue.kill(task.id).await;
        match kill_result {
            Ok(()) => Ok(TerminationOutcome::Confirmed),
            Err(error) => {
                let message = error.to_string();
                let now = unix_timestamp()?;
                repository.update_result(
                    request.request_id,
                    TerminationRequestStatus::Failed,
                    None,
                    Some(&message),
                )?;
                insert_termination_failed_event(self.db, &request, &task, &message, now)?;
                Ok(TerminationOutcome::Failed)
            }
        }
    }
}

pub fn auto_kill_request_for_terminal_task(
    db: &Db,
    project_id: &str,
    task: &crate::pueue::PueueTask,
) -> Result<Option<crate::models::TerminationRequest>, AppError> {
    let requests = TerminationRequestRepository::new(db).find_by_project(project_id)?;
    Ok(requests.into_iter().find(|request| {
        matches!(
            request.status,
            TerminationRequestStatus::Sent | TerminationRequestStatus::Confirmed
        ) && request_matches_terminal_task(request, task)
    }))
}

pub fn confirm_auto_kill_terminal_observation(
    db: &Db,
    request_id: TerminationRequestId,
    confirmed_at: i64,
) -> Result<(), AppError> {
    let repository = TerminationRequestRepository::new(db);
    let request = repository
        .find_by_id(request_id)?
        .ok_or(AppError::Runtime {
            operation: "confirm missing termination request",
        })?;
    if request.status != TerminationRequestStatus::Confirmed {
        repository.update_result(
            request.request_id,
            TerminationRequestStatus::Confirmed,
            Some(confirmed_at),
            None,
        )?;
    }
    Ok(())
}

fn request_matches_terminal_task(
    request: &crate::models::TerminationRequest,
    task: &crate::pueue::PueueTask,
) -> bool {
    if request.task_signature.as_str() == task_signature(task).as_str() {
        return true;
    }
    let Some(identity) = request
        .task_signature
        .strip_prefix("pueue-task:v1:")
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
    else {
        return false;
    };
    identity.get("group").and_then(|value| value.as_str()) == Some(task.group.as_str())
        && identity.get("id").and_then(|value| value.as_i64()) == Some(task.id)
        && optional_string(identity.get("enqueued_at")) == task.enqueued_at.as_deref()
        && optional_string(identity.get("started_at")) == task.started_at.as_deref()
}

fn optional_string(value: Option<&serde_json::Value>) -> Option<&str> {
    value.and_then(|value| {
        if value.is_null() {
            None
        } else {
            value.as_str()
        }
    })
}

pub fn insert_termination_failed_event(
    db: &Db,
    request: &crate::models::TerminationRequest,
    task: &crate::pueue::PueueTask,
    error: &str,
    now: i64,
) -> Result<(), AppError> {
    let event = NewEvent::new(
        &request.project_id,
        EventKind::TerminationFailed,
        format!(
            "termination:{}:v1:request={}:signature={}",
            EventKind::TerminationFailed,
            request.request_id, request.task_signature
        ),
        json!({
            "source": "termination_manager",
            "request_id": request.request_id,
            "incident_id": request.incident_id,
            "task_signature": &request.task_signature,
            "task_id": task.id,
            "group": &task.group,
            "state": &task.state,
            "error": error,
        }),
        now,
        now,
    );
    let _ = EventRepository::new(db).insert_idempotent(&event)?;
    Ok(())
}

fn unix_timestamp() -> Result<i64, AppError> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read current time",
        })?;
    i64::try_from(duration.as_secs()).map_err(|_| AppError::Runtime {
        operation: "convert current time",
    })
}

#[derive(Debug, Clone, Copy)]
pub struct NoPueue;
