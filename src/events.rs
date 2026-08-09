use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    db::{Db, EventRepository, ProjectRepository},
    models::{EventKind, NewEvent},
    paths, AppError,
};

pub type EventId = i64;

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

pub fn record_callback(
    group: &str,
    task_id: i64,
    metadata: CallbackMetadata,
) -> Result<EventId, AppError> {
    let db = Db::open(&paths::state_db_path()?)?;
    record_callback_with(&db, group, task_id, metadata)
}

pub fn record_callback_with(
    db: &Db,
    group: &str,
    task_id: i64,
    metadata: CallbackMetadata,
) -> Result<EventId, AppError> {
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

    let project = ProjectRepository::new(db)
        .find_by_group(group)?
        .ok_or_else(|| AppError::UnknownPueueGroup {
            group: group.to_owned(),
        })?;
    let kind = callback_kind(&metadata);
    let now = unix_timestamp()?;
    let payload = json!({
        "source": "pueue_callback",
        "group": group,
        "task_id": task_id,
        "metadata": metadata,
    });
    let event = EventRepository::new(db).insert_idempotent(&NewEvent::new(
        project.project_id,
        kind,
        callback_dedup_key(group, task_id),
        payload,
        now,
        now,
    ))?;
    Ok(event.event_id)
}

pub(crate) fn callback_dedup_key(group: &str, task_id: i64) -> String {
    format!("pueue-callback:v1:group={group}:task-id={task_id}")
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
