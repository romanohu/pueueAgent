mod migrations;
mod repositories;

use std::{fs, path::Path, path::PathBuf, time::Duration};

use rusqlite::{Connection, OpenFlags};

use crate::AppError;

pub use repositories::{
    AgentRunRecovery, AgentRunRepository, EventRepository, IncidentRepository,
    IntegrationEventRepository, InterventionRepository, ProjectRepository, SubmissionRepository,
    TaskObservationRepository, TerminationRequestRepository,
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Db {
    path: PathBuf,
    read_only: bool,
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
            read_only: false,
        })
    }

    pub fn open_read_only(path: &Path) -> Result<Self, AppError> {
        let connection = open_read_only_connection(path)?;
        drop(connection);
        Ok(Self {
            path: path.to_path_buf(),
            read_only: true,
        })
    }

    pub fn connect(&self) -> Result<Connection, AppError> {
        if self.read_only {
            open_read_only_connection(&self.path)
        } else {
            open_connection(&self.path)
        }
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
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(|source| AppError::Database {
            operation: "read SQLite journal mode",
            source,
        })?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(|source| AppError::Database {
                operation: "enable SQLite WAL mode",
                source,
            })?;
    }

    Ok(connection)
}

fn open_read_only_connection(path: &Path) -> Result<Connection, AppError> {
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|source| {
            AppError::Database {
                operation: "open SQLite database read-only",
                source,
            }
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
            operation: "enable SQLite foreign keys for read-only connection",
            source,
        })?;
    Ok(connection)
}

pub(crate) fn database_error(operation: &'static str) -> impl FnOnce(rusqlite::Error) -> AppError {
    move |source| AppError::Database { operation, source }
}
