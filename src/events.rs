use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::{
    db::{CampaignRepository, Db, EventRepository, IntegrationEventRepository, ProjectRepository},
    models::{EventKind, IntegrationEventKind, NewEvent, NewIntegrationEvent},
    paths,
    pueue::PueueTask,
    pueue_security::validate_group,
    AppError,
};

pub type EventId = i64;

const MAX_OPERATOR_WAKE_REASON_BYTES: usize = 1024;

pub fn record_operator_wake_with(
    db: &Db,
    project_id: &str,
    reason: &str,
    now: i64,
) -> Result<EventId, AppError> {
    if reason.trim().is_empty() || reason.len() > MAX_OPERATOR_WAKE_REASON_BYTES {
        return Err(AppError::Configuration {
            field: "wake.reason",
        });
    }
    let reason = crate::output::bounded_redacted_text(reason);
    let mut event = NewEvent::new(
        project_id,
        EventKind::OperatorWake,
        format!("operator-wake:v1:{}", Uuid::new_v4()),
        json!({"source": "operator", "reason": reason}),
        now,
        now,
    );
    if let Some(campaign) = CampaignRepository::new(db).find_live_by_project(project_id)? {
        event = event.with_campaign_lineage(campaign.campaign_id, None::<String>);
    }
    let event = EventRepository::new(db).insert_idempotent(&event)?;
    Ok(event.event_id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackRecordResult {
    ProjectEvent {
        project_id: String,
        event_id: EventId,
    },
    UnknownGroup {
        group: String,
        integration_event_id: i64,
    },
}

impl CallbackRecordResult {
    pub fn event_id(&self) -> i64 {
        match self {
            Self::ProjectEvent { event_id, .. } => *event_id,
            Self::UnknownGroup {
                integration_event_id,
                ..
            } => *integration_event_id,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CallbackMetadata {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub result: Option<Value>,
}

impl CallbackMetadata {
    pub fn new<S>(state: Option<S>, result: Option<Value>) -> Self
    where
        S: Into<String>,
    {
        Self {
            state: state.map(Into::into),
            result,
        }
    }
}

pub fn callback_group_for_task<'a>(
    tasks: &'a [PueueTask],
    task_id: i64,
) -> Result<&'a str, AppError> {
    let mut matches = tasks.iter().filter(|task| task.id == task_id);
    let task = matches.next().ok_or(AppError::Validation {
        field: "callback.task_id",
        message: "was not found in the configured Pueue profile",
    })?;
    if matches.next().is_some() {
        return Err(AppError::Validation {
            field: "callback.task_id",
            message: "is ambiguous in the configured Pueue profile",
        });
    }
    validate_group(&task.group)?;
    Ok(&task.group)
}

pub fn record_callback(
    group: &str,
    task_id: i64,
    metadata: CallbackMetadata,
) -> Result<CallbackRecordResult, AppError> {
    let db = Db::open(&paths::state_db_path()?)?;
    record_callback_with(&db, group, task_id, metadata)
}

pub fn record_callback_with(
    db: &Db,
    group: &str,
    task_id: i64,
    metadata: CallbackMetadata,
) -> Result<CallbackRecordResult, AppError> {
    if group.is_empty() {
        return Err(AppError::Configuration {
            field: "callback.group",
        });
    }
    if task_id < 0 {
        return Err(AppError::Configuration {
            field: "callback.task_id",
        });
    }

    let Some(project) = ProjectRepository::new(db).find_by_group(group)? else {
        let now = unix_timestamp()?;
        let integration_event =
            IntegrationEventRepository::new(db).insert_idempotent(&NewIntegrationEvent::new(
                IntegrationEventKind::UnknownCallbackGroup,
                unknown_callback_dedup_key(group, task_id),
                json!({
                    "source": "pueue_callback",
                    "visibility": "integration_event",
                    "reason": "unknown_pueue_group",
                    "group": group,
                    "task_id": task_id,
                    "metadata": metadata,
                }),
                now,
            ))?;
        return Ok(CallbackRecordResult::UnknownGroup {
            group: group.to_owned(),
            integration_event_id: integration_event.integration_event_id,
        });
    };
    let kind = callback_kind(&metadata);
    let now = unix_timestamp()?;
    if let Some(existing) =
        EventRepository::new(db).find_terminal_by_pueue_task(&project.project_id, group, task_id)?
    {
        return Ok(CallbackRecordResult::ProjectEvent {
            project_id: project.project_id,
            event_id: existing.event_id,
        });
    }
    let payload = json!({
        "source": "pueue_callback",
        "group": group,
        "task_id": task_id,
        "metadata": metadata,
    });
    let project_id = project.project_id;
    let event = EventRepository::new(db).insert_idempotent(&NewEvent::new(
        project_id.clone(),
        kind,
        callback_dedup_key(group, task_id),
        payload,
        now,
        now,
    ))?;
    Ok(CallbackRecordResult::ProjectEvent {
        project_id,
        event_id: event.event_id,
    })
}

pub(crate) fn callback_dedup_key(group: &str, task_id: i64) -> String {
    format!("pueue-callback:v1:group={group}:task-id={task_id}")
}

fn unknown_callback_dedup_key(group: &str, task_id: i64) -> String {
    format!("pueue-callback-unknown-group:v1:group={group}:task-id={task_id}")
}

pub(crate) fn callback_kind(metadata: &CallbackMetadata) -> EventKind {
    if metadata
        .state
        .as_deref()
        .is_some_and(|state| matches!(state.to_ascii_lowercase().as_str(), "failed" | "killed"))
        || metadata.result.as_ref().is_some_and(result_is_failure)
    {
        EventKind::TaskFailed
    } else {
        EventKind::TaskFinished
    }
}

pub(crate) fn result_is_failure(result: &Value) -> bool {
    match result {
        Value::String(value) => matches!(value.to_ascii_lowercase().as_str(), "failed" | "killed"),
        Value::Object(object) => object.contains_key("Failed") || object.contains_key("Killed"),
        _ => false,
    }
}

fn unix_timestamp() -> Result<i64, AppError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read the system clock for callback ingestion",
        })?
        .as_secs()
        .try_into()
        .map_err(|_| AppError::Runtime {
            operation: "represent the callback timestamp",
        })
}
