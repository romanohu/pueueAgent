use std::io;

use thiserror::Error;

#[allow(dead_code)] // Command handlers intentionally become fallible in later tasks.
#[derive(Debug, Error)]
pub enum AppError {
    #[error("configuration error in {field}; update the project configuration and try again")]
    Configuration { field: &'static str },

    #[error("failed to {operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: io::Error,
    },

    #[error("{operation} failed; inspect pueue-agent logs for details")]
    Runtime { operation: &'static str },
}

impl AppError {
    pub fn render(&self) -> String {
        self.to_string()
    }
}
