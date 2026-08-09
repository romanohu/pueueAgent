use serde_json::json;

use crate::{
    config::PatternAction,
    db::{
        Db, EventRepository, IncidentRepository, ProjectRepository, TerminationRequestRepository,
    },
    detect::{Observation, ObservationState},
    models::{EventKind, NewEvent, NewTerminationRequest, TerminationRequestStatus},
    pueue::PueueApi,
    reconcile::task_signature,
    AppError,
};

pub type TerminationRequestId = i64;

pub(crate) const DEFAULT_CONFIRMATION_GRACE_SECONDS: i64 = 120;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoKillConfirmation {
    Confirmed,
    AlreadyConfirmed,
    NotSent,
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
            TerminationRequestStatus::Sent => {
                return finish_sent_request(self.db, &repository, &request);
            }
            TerminationRequestStatus::Requested => {}
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
            let now = unix_timestamp()?;
            return confirm_requested_already_terminal(
                self.db,
                &repository,
                request.request_id,
                now,
                "task signature is no longer active",
            );
        };
        if !task.is_running() {
            let now = unix_timestamp()?;
            return confirm_requested_already_terminal(
                self.db,
                &repository,
                request.request_id,
                now,
                "task is already terminal or non-running",
            );
        }

        let Some(claimed_request) = repository.transition_status_if_current(
            request.request_id,
            TerminationRequestStatus::Requested,
            TerminationRequestStatus::Sent,
        )?
        else {
            let current = repository
                .find_by_id(request.request_id)?
                .ok_or(AppError::Runtime {
                    operation: "reload concurrently claimed termination request",
                })?;
            return outcome_for_non_requested(self.db, &repository, &current);
        };

        let kill_result = self.pueue.kill(task.id).await;
        match kill_result {
            Ok(()) => Ok(TerminationOutcome::Confirmed),
            Err(error) => {
                let message = error.to_string();
                if let Some(failed_request) = repository.update_result_if_current(
                    claimed_request.request_id,
                    TerminationRequestStatus::Sent,
                    TerminationRequestStatus::Failed,
                    None,
                    Some(&message),
                )? {
                    let now = unix_timestamp()?;
                    insert_termination_failed_event(
                        self.db,
                        &failed_request,
                        &task,
                        &message,
                        now,
                    )?;
                    return Ok(TerminationOutcome::Failed);
                }
                let current = repository.find_by_id(claimed_request.request_id)?.ok_or(
                    AppError::Runtime {
                        operation: "reload concurrently completed termination request",
                    },
                )?;
                outcome_for_non_requested(self.db, &repository, &current)
            }
        }
    }
}

fn confirm_requested_already_terminal(
    db: &Db,
    repository: &TerminationRequestRepository<'_>,
    request_id: i64,
    confirmed_at: i64,
    message: &str,
) -> Result<TerminationOutcome, AppError> {
    if repository
        .update_result_if_current(
            request_id,
            TerminationRequestStatus::Requested,
            TerminationRequestStatus::Confirmed,
            Some(confirmed_at),
            Some(message),
        )?
        .is_some()
    {
        return Ok(TerminationOutcome::AlreadyTerminal);
    }
    let current = repository
        .find_by_id(request_id)?
        .ok_or(AppError::Runtime {
            operation: "reload concurrently completed termination request",
        })?;
    outcome_for_non_requested(db, repository, &current)
}

fn outcome_for_non_requested(
    db: &Db,
    repository: &TerminationRequestRepository<'_>,
    request: &crate::models::TerminationRequest,
) -> Result<TerminationOutcome, AppError> {
    match request.status {
        TerminationRequestStatus::Confirmed => Ok(TerminationOutcome::Confirmed),
        TerminationRequestStatus::TimedOut => Ok(TerminationOutcome::TimedOut),
        TerminationRequestStatus::Failed => Ok(TerminationOutcome::Failed),
        TerminationRequestStatus::Sent => finish_sent_request(db, repository, request),
        TerminationRequestStatus::Requested => Ok(TerminationOutcome::AlreadyTerminal),
    }
}

fn finish_sent_request(
    db: &Db,
    repository: &TerminationRequestRepository<'_>,
    request: &crate::models::TerminationRequest,
) -> Result<TerminationOutcome, AppError> {
    let now = unix_timestamp()?;
    if request
        .grace_until
        .is_some_and(|grace_until| grace_until <= now)
    {
        let message = "Pueue kill was not confirmed before grace timeout";
        if let Some(timed_out) = repository.update_result_if_current(
            request.request_id,
            TerminationRequestStatus::Sent,
            TerminationRequestStatus::TimedOut,
            None,
            Some(message),
        )? {
            insert_termination_failed_event_for_request(db, &timed_out, message, now)?;
        }
        return Ok(TerminationOutcome::TimedOut);
    }
    Ok(TerminationOutcome::Confirmed)
}

pub fn auto_kill_request_for_terminal_task(
    db: &Db,
    project_id: &str,
    task: &crate::pueue::PueueTask,
) -> Result<Option<crate::models::TerminationRequest>, AppError> {
    let requests = TerminationRequestRepository::new(db).find_by_project(project_id)?;
    Ok(requests.into_iter().find(|request| {
        (request.status == TerminationRequestStatus::Sent
            || (request.status == TerminationRequestStatus::Confirmed
                && request.last_error.is_none()))
            && request_matches_terminal_task(request, task)
    }))
}

pub fn confirm_auto_kill_terminal_observation(
    db: &Db,
    request_id: TerminationRequestId,
    confirmed_at: i64,
) -> Result<AutoKillConfirmation, AppError> {
    let repository = TerminationRequestRepository::new(db);
    if repository
        .update_result_if_current(
            request_id,
            TerminationRequestStatus::Sent,
            TerminationRequestStatus::Confirmed,
            Some(confirmed_at),
            None,
        )?
        .is_some()
    {
        return Ok(AutoKillConfirmation::Confirmed);
    }

    let request = repository
        .find_by_id(request_id)?
        .ok_or(AppError::Runtime {
            operation: "confirm missing termination request",
        })?;
    if request.status == TerminationRequestStatus::Confirmed {
        Ok(AutoKillConfirmation::AlreadyConfirmed)
    } else {
        Ok(AutoKillConfirmation::NotSent)
    }
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
        && required_matching_string(identity.get("enqueued_at"), task.enqueued_at.as_deref())
        && required_matching_string(identity.get("started_at"), task.started_at.as_deref())
}

fn required_matching_string(value: Option<&serde_json::Value>, candidate: Option<&str>) -> bool {
    value
        .and_then(|value| {
            if value.is_null() {
                None
            } else {
                value.as_str()
            }
        })
        .is_some_and(|value| Some(value) == candidate)
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
            request.request_id,
            request.task_signature
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

pub fn insert_termination_failed_event_for_request(
    db: &Db,
    request: &crate::models::TerminationRequest,
    error: &str,
    now: i64,
) -> Result<(), AppError> {
    let identity = task_signature_identity(&request.task_signature);
    let event = NewEvent::new(
        &request.project_id,
        EventKind::TerminationFailed,
        format!(
            "termination:{}:v1:request={}:signature={}",
            EventKind::TerminationFailed,
            request.request_id,
            request.task_signature
        ),
        json!({
            "source": "termination_manager",
            "request_id": request.request_id,
            "incident_id": request.incident_id,
            "task_signature": &request.task_signature,
            "task_id": identity
                .as_ref()
                .and_then(|identity| identity.get("id"))
                .and_then(|value| value.as_i64()),
            "group": identity
                .as_ref()
                .and_then(|identity| identity.get("group"))
                .and_then(|value| value.as_str()),
            "error": error,
        }),
        now,
        now,
    );
    let _ = EventRepository::new(db).insert_idempotent(&event)?;
    Ok(())
}

fn task_signature_identity(task_signature: &str) -> Option<serde_json::Value> {
    task_signature
        .strip_prefix("pueue-task:v1:")
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
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
