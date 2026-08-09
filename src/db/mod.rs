mod migrations;
mod repositories;

use std::{fs, path::Path, path::PathBuf, time::Duration};

use rusqlite::Connection;

use crate::AppError;

pub use repositories::{
    AgentRunRepository, EventRepository, IncidentRepository, ProjectRepository,
    SubmissionRepository, TaskObservationRepository, TerminationRequestRepository,
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Db {
    path: PathBuf,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self, AppError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| AppError::Io {
                operation: "create database directory",
                source,
            })?;
        }

        let mut connection = open_connection(path)?;
        migrations::migrate(&mut connection)?;

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn connect(&self) -> Result<Connection, AppError> {
        open_connection(&self.path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn open_connection(path: &Path) -> Result<Connection, AppError> {
    let connection = Connection::open(path).map_err(|source| AppError::Database {
        operation: "open SQLite database",
        source,
    })?;
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|source| AppError::Database {
            operation: "configure SQLite busy timeout",
            source,
        })?;
    connection
        .pragma_update(None, "foreign_keys", "ON")
        .map_err(|source| AppError::Database {
            operation: "enable SQLite foreign keys",
            source,
        })?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|source| AppError::Database {
            operation: "enable SQLite WAL mode",
            source,
        })?;

    Ok(connection)
}

pub(crate) fn database_error(operation: &'static str) -> impl FnOnce(rusqlite::Error) -> AppError {
    move |source| AppError::Database { operation, source }
}
