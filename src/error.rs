use std::io;

use thiserror::Error;

#[allow(dead_code)] // Command handlers intentionally become fallible in later tasks.
#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    PolicyViolation {
        violation: crate::execution_policy::PolicyViolation,
    },

    #[error("configuration error in {field}; update the project configuration and try again")]
    Configuration { field: &'static str },

    #[error(
        "Codex session `{session_id}` cannot be resumed: {reason}; explicit resume requires local metadata proving project ownership"
    )]
    CodexSessionMetadata {
        session_id: String,
        reason: &'static str,
    },

    #[error("failed to {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },

    #[error("database operation `{operation}` failed: {source}")]
    Database {
        operation: &'static str,
        #[source]
        source: rusqlite::Error,
    },

    #[error("database conflict on {field}; the value is already registered")]
    DatabaseConflict { field: &'static str },

    #[error("invalid {field}: {message}")]
    Validation {
        field: &'static str,
        message: &'static str,
    },

    #[error("unknown Pueue group `{group}`; callback was not associated with a project")]
    UnknownPueueGroup { group: String },

    #[error("agent start deferred while an upgrade is in progress")]
    UpgradeInProgress,

    #[error("failed to {operation}: {source}")]
    Serialization {
        operation: &'static str,
        #[source]
        source: serde_json::Error,
    },

    #[error(transparent)]
    Pueue(#[from] crate::pueue::PueueError),

    #[error("{message}")]
    Message { message: String },

    #[error("{operation} failed; inspect pueue-agent logs for details")]
    Runtime { operation: &'static str },
}

impl From<crate::execution_policy::PolicyViolation> for AppError {
    fn from(violation: crate::execution_policy::PolicyViolation) -> Self {
        Self::PolicyViolation { violation }
    }
}

impl AppError {
    pub fn render(&self) -> String {
        self.to_string()
    }
}
