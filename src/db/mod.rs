mod migrations;
mod repositories;

pub use migrations::LATEST_SCHEMA_VERSION;

use std::{fs, path::Path, path::PathBuf, sync::Mutex, time::Duration};

use rusqlite::{Connection, OpenFlags};

use crate::AppError;

pub use repositories::{
    AgentRunRecovery, AgentRunRepository, BatchRepository, EventRepository, GateFailurePolicy,
    IncidentRepository, IntegrationEventRepository, InterventionRepository, ProjectRepository,
    RunLineage, RunLineageCursor, RunLineageRepository, SubmissionLineage, SubmissionPageCursor,
    SubmissionRepository, TaskObservationRepository, TerminationRequestRepository,
    MAX_FOLLOW_LINEAGE_SUBMISSIONS,
};

const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
static OPEN_INITIALIZATION_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone)]
pub struct Db {
    path: PathBuf,
    read_only: bool,
    busy_timeout: Duration,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self, AppError> {
        let _initialization = OPEN_INITIALIZATION_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|source| AppError::Io {
                operation: "create database directory",
                source,
            })?;
        }

        let mut connection = open_connection(path, BUSY_TIMEOUT)?;
        migrations::migrate(&mut connection)?;

        Ok(Self {
            path: path.to_path_buf(),
            read_only: false,
            busy_timeout: BUSY_TIMEOUT,
        })
    }

    pub fn open_read_only(path: &Path) -> Result<Self, AppError> {
        let connection = open_read_only_connection(path)?;
        drop(connection);
        Ok(Self {
            path: path.to_path_buf(),
            read_only: true,
            busy_timeout: BUSY_TIMEOUT,
        })
    }

    pub fn connect(&self) -> Result<Connection, AppError> {
        if self.read_only {
            open_read_only_connection(&self.path)
        } else {
            open_connection(&self.path, self.busy_timeout)
        }
    }

    pub(crate) fn with_busy_timeout(&self, busy_timeout: Duration) -> Self {
        Self {
            path: self.path.clone(),
            read_only: self.read_only,
            busy_timeout,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn open_connection(path: &Path, busy_timeout: Duration) -> Result<Connection, AppError> {
    let connection = Connection::open(path).map_err(|source| AppError::Database {
        operation: "open SQLite database",
        source,
    })?;
    connection
        .busy_timeout(busy_timeout)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_busy_timeout_does_not_mutate_the_original_database() {
        let temporary = tempfile::tempdir().unwrap();
        let db = Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let scoped = db.with_busy_timeout(Duration::from_millis(100));

        let scoped_timeout: i64 = scoped
            .connect()
            .unwrap()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        let original_timeout: i64 = db
            .connect()
            .unwrap()
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();

        assert_eq!(scoped_timeout, 100);
        assert_eq!(original_timeout, 5_000);
    }
}
