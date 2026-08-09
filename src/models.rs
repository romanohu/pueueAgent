use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    str::FromStr,
};

use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug)]
pub struct ModelEnumParseError {
    enum_name: &'static str,
    value: String,
}

impl Display for ModelEnumParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} database value {:?}",
            self.enum_name, self.value
        )
    }
}

impl Error for ModelEnumParseError {}

macro_rules! database_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ModelEnumParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    _ => Err(ModelEnumParseError {
                        enum_name: stringify!($name),
                        value: value.to_owned(),
                    }),
                }
            }
        }

        impl ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::Borrowed(ValueRef::Text(
                    self.as_str().as_bytes(),
                )))
            }
        }

        impl FromSql for $name {
            fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
                value
                    .as_str()?
                    .parse()
                    .map_err(|error| FromSqlError::Other(Box::new(error)))
            }
        }
    };
}

database_enum!(EventKind {
    TaskFinished => "task_finished",
    TaskFailed => "task_failed",
    Crash => "crash",
    Stalled => "stalled",
    DeepCheck => "deep_check",
    AutoKilled => "auto_killed",
    TerminationFailed => "termination_failed",
});

database_enum!(EventStatus {
    Pending => "pending",
    Claimed => "claimed",
    Completed => "completed",
    RetryWait => "retry_wait",
    Failed => "failed",
});

database_enum!(IncidentTransition {
    Opened => "opened",
    Updated => "updated",
    Unchanged => "unchanged",
    Resolved => "resolved",
});

database_enum!(IncidentStatus {
    Open => "open",
    Acknowledged => "acknowledged",
    Resolved => "resolved",
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub project_id: String,
    pub root_path: PathBuf,
    pub pueue_group: String,
    pub config_path: PathBuf,
    pub enabled: bool,
    pub paused: bool,
    pub halted_reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProject {
    pub project_id: String,
    pub root_path: PathBuf,
    pub pueue_group: String,
    pub config_path: PathBuf,
    pub enabled: bool,
    pub paused: bool,
    pub created_at: i64,
}

impl NewProject {
    pub fn new(
        project_id: impl Into<String>,
        root_path: impl Into<PathBuf>,
        pueue_group: impl Into<String>,
        config_path: impl Into<PathBuf>,
        created_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            root_path: root_path.into(),
            pueue_group: pueue_group.into(),
            config_path: config_path.into(),
            enabled: true,
            paused: false,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub event_id: i64,
    pub project_id: String,
    pub kind: EventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub status: EventStatus,
    pub attempts: i64,
    pub not_before: i64,
    pub lease_until: Option<i64>,
    pub created_at: i64,
    pub completed_at: Option<i64>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    pub project_id: String,
    pub kind: EventKind,
    pub dedup_key: String,
    pub payload: Value,
    pub not_before: i64,
    pub created_at: i64,
}

impl NewEvent {
    pub fn new(
        project_id: impl Into<String>,
        kind: EventKind,
        dedup_key: impl Into<String>,
        payload: Value,
        not_before: i64,
        created_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind,
            dedup_key: dedup_key.into(),
            payload,
            not_before,
            created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Incident {
    pub incident_id: i64,
    pub project_id: String,
    pub kind: String,
    pub task_key: Option<String>,
    pub fingerprint: String,
    pub status: IncidentStatus,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
    pub acknowledged_at: Option<i64>,
    pub resolved_at: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewIncident {
    pub project_id: String,
    pub kind: String,
    pub task_key: Option<String>,
    pub fingerprint: String,
    pub seen_at: i64,
}

impl NewIncident {
    pub fn new(
        project_id: impl Into<String>,
        kind: impl Into<String>,
        task_key: Option<impl Into<String>>,
        fingerprint: impl Into<String>,
        seen_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind: kind.into(),
            task_key: task_key.map(Into::into),
            fingerprint: fingerprint.into(),
            seen_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncidentUpdate {
    pub incident: Incident,
    pub transition: IncidentTransition,
}

pub(crate) fn path_text<'path>(
    path: &'path Path,
    field: &'static str,
) -> Result<&'path str, crate::AppError> {
    path.to_str()
        .ok_or(crate::AppError::Configuration { field })
}
