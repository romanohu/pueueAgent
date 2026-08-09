use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use regex_automata::meta::Regex;
use rusqlite::{params, OptionalExtension};

use crate::{
    config::{CheckConfig, PatternAction},
    db::{database_error, Db},
    logs::LogSnapshot,
    pueue::PueueTask,
    reconcile::task_incident_key,
    AppError,
};

pub const TASK_TERMINAL_RECOVERY_KIND: &str = "__task_terminal";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationState {
    Active,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    project_id: String,
    kind: String,
    task_key: Option<String>,
    fingerprint: String,
    seen_at: i64,
    state: ObservationState,
    task_signature: Option<String>,
    pattern_name: Option<String>,
    action: PatternAction,
    confirmation_count: Option<u32>,
    evidence: String,
    source_path: Option<PathBuf>,
}

impl Observation {
    #[allow(clippy::too_many_arguments)]
    pub fn pattern(
        project_id: impl Into<String>,
        task_signature: impl AsRef<str>,
        pattern_name: impl Into<String>,
        action: PatternAction,
        confirmation_count: u32,
        evidence: impl Into<String>,
        seen_at: i64,
    ) -> Self {
        let pattern_name = pattern_name.into();
        let task_signature = task_signature.as_ref().to_owned();
        Self {
            project_id: project_id.into(),
            kind: "pattern".to_owned(),
            task_key: Some(task_signature.clone()),
            fingerprint: format!("pattern:v1:task={task_signature}:name={pattern_name}"),
            seen_at,
            state: ObservationState::Active,
            task_signature: Some(task_signature),
            pattern_name: Some(pattern_name),
            action,
            confirmation_count: Some(confirmation_count),
            evidence: evidence.into(),
            source_path: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn task_pattern(
        project_id: impl Into<String>,
        task_key: impl Into<String>,
        task_signature: impl Into<String>,
        pattern_name: impl Into<String>,
        action: PatternAction,
        confirmation_count: u32,
        evidence: impl Into<String>,
        seen_at: i64,
    ) -> Self {
        let task_key = task_key.into();
        let pattern_name = pattern_name.into();
        Self {
            project_id: project_id.into(),
            kind: "pattern".to_owned(),
            task_key: Some(task_key.clone()),
            fingerprint: format!("pattern:v1:task={task_key}:name={pattern_name}"),
            seen_at,
            state: ObservationState::Active,
            task_signature: Some(task_signature.into()),
            pattern_name: Some(pattern_name),
            action,
            confirmation_count: Some(confirmation_count),
            evidence: evidence.into(),
            source_path: None,
        }
    }

    pub fn extra_log_pattern(
        project_id: impl Into<String>,
        relative_path: impl Into<PathBuf>,
        pattern_name: impl Into<String>,
        action: PatternAction,
        confirmation_count: u32,
        snapshot: LogSnapshot,
        seen_at: i64,
    ) -> Self {
        let relative_path = relative_path.into();
        let pattern_name = pattern_name.into();
        let fingerprint =
            extra_log_pattern_fingerprint(&relative_path, &pattern_name, &snapshot.fingerprint);
        Self {
            project_id: project_id.into(),
            kind: "pattern".to_owned(),
            task_key: None,
            fingerprint,
            seen_at,
            state: ObservationState::Active,
            task_signature: None,
            pattern_name: Some(pattern_name),
            action,
            confirmation_count: Some(confirmation_count),
            evidence: snapshot.evidence,
            source_path: Some(relative_path),
        }
    }

    pub fn extra_log_pattern_recovered(
        project_id: impl Into<String>,
        relative_path: impl Into<PathBuf>,
        pattern_name: impl Into<String>,
        snapshot: LogSnapshot,
        seen_at: i64,
    ) -> Self {
        let relative_path = relative_path.into();
        let pattern_name = pattern_name.into();
        let fingerprint =
            extra_log_pattern_fingerprint(&relative_path, &pattern_name, &snapshot.fingerprint);
        Self {
            project_id: project_id.into(),
            kind: "pattern".to_owned(),
            task_key: None,
            fingerprint,
            seen_at,
            state: ObservationState::Recovered,
            task_signature: None,
            pattern_name: Some(pattern_name),
            action: PatternAction::Notify,
            confirmation_count: None,
            evidence: snapshot.evidence,
            source_path: Some(relative_path),
        }
    }

    pub fn stalled(
        project_id: impl Into<String>,
        task_signature: impl AsRef<str>,
        snapshot: LogSnapshot,
        action: PatternAction,
        seen_at: i64,
    ) -> Self {
        let task_signature = task_signature.as_ref().to_owned();
        Self::task_stalled(
            project_id,
            &task_signature,
            task_signature.clone(),
            snapshot,
            action,
            seen_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn task_stalled(
        project_id: impl Into<String>,
        task_key: impl Into<String>,
        task_signature: impl Into<String>,
        snapshot: LogSnapshot,
        action: PatternAction,
        seen_at: i64,
    ) -> Self {
        let task_key = task_key.into();
        Self {
            project_id: project_id.into(),
            kind: "stalled".to_owned(),
            task_key: Some(task_key.clone()),
            fingerprint: stalled_observation_fingerprint(&task_key, &snapshot),
            seen_at,
            state: ObservationState::Active,
            task_signature: Some(task_signature.into()),
            pattern_name: None,
            action,
            confirmation_count: None,
            evidence: snapshot.evidence,
            source_path: None,
        }
    }

    pub fn stalled_recovered(
        project_id: impl Into<String>,
        task_signature: impl AsRef<str>,
        snapshot: LogSnapshot,
        seen_at: i64,
    ) -> Self {
        let task_signature = task_signature.as_ref().to_owned();
        Self::task_stalled_recovered(
            project_id,
            &task_signature,
            task_signature.clone(),
            snapshot,
            seen_at,
        )
    }

    pub fn task_stalled_recovered(
        project_id: impl Into<String>,
        task_key: impl Into<String>,
        task_signature: impl Into<String>,
        snapshot: LogSnapshot,
        seen_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind: "stalled".to_owned(),
            task_key: Some(task_key.into()),
            fingerprint: format!("stalled-recovered:v1:snapshot={}", snapshot.fingerprint),
            seen_at,
            state: ObservationState::Recovered,
            task_signature: Some(task_signature.into()),
            pattern_name: None,
            action: PatternAction::Notify,
            confirmation_count: None,
            evidence: snapshot.evidence,
            source_path: None,
        }
    }

    pub fn task_terminal(
        project_id: impl Into<String>,
        task_signature: impl AsRef<str>,
        seen_at: i64,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            kind: TASK_TERMINAL_RECOVERY_KIND.to_owned(),
            task_key: Some(task_signature.as_ref().to_owned()),
            fingerprint: "task-terminal:v1".to_owned(),
            seen_at,
            state: ObservationState::Recovered,
            task_signature: Some(task_signature.as_ref().to_owned()),
            pattern_name: None,
            action: PatternAction::Notify,
            confirmation_count: None,
            evidence: String::new(),
            source_path: None,
        }
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn task_key(&self) -> Option<&str> {
        self.task_key.as_deref()
    }

    pub fn task_signature(&self) -> Option<&str> {
        self.task_signature.as_deref()
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn seen_at(&self) -> i64 {
        self.seen_at
    }

    pub fn state(&self) -> &ObservationState {
        &self.state
    }

    pub fn pattern_name(&self) -> Option<&str> {
        self.pattern_name.as_deref()
    }

    pub fn action(&self) -> PatternAction {
        self.action
    }

    pub fn confirmation_count(&self) -> Option<u32> {
        self.confirmation_count
    }

    pub fn evidence(&self) -> &str {
        &self.evidence
    }

    pub fn source_path(&self) -> Option<&Path> {
        self.source_path.as_deref()
    }
}

fn extra_log_pattern_fingerprint(
    relative_path: &Path,
    pattern_name: &str,
    snapshot_fingerprint: &str,
) -> String {
    format!(
        "extra-log-pattern:v1:path={}:name={}:snapshot={}",
        relative_path.display(),
        pattern_name,
        snapshot_fingerprint
    )
}

fn stall_progress_fingerprint(snapshot: &LogSnapshot) -> String {
    format!(
        "size={}:mtime={}",
        snapshot.byte_size,
        snapshot
            .modified_at_nanos
            .map(|value| value.to_string())
            .unwrap_or_else(|| "unknown".to_owned())
    )
}

fn stalled_observation_fingerprint(task_key: &str, snapshot: &LogSnapshot) -> String {
    format!(
        "stalled:v2:task={task_key}:snapshot={}",
        stall_progress_fingerprint(snapshot)
    )
}

#[derive(Debug, Clone)]
pub struct Detector {
    project_id: Option<String>,
    project_root: PathBuf,
    task_log_dir: PathBuf,
    incident_db: Option<Db>,
}

impl Detector {
    pub fn new(project_root: impl Into<PathBuf>, task_log_dir: impl Into<PathBuf>) -> Self {
        Self {
            project_id: None,
            project_root: project_root.into(),
            task_log_dir: task_log_dir.into(),
            incident_db: None,
        }
    }

    pub fn for_project(
        project_id: impl Into<String>,
        project_root: impl Into<PathBuf>,
        task_log_dir: impl Into<PathBuf>,
    ) -> Self {
        Self {
            project_id: Some(project_id.into()),
            project_root: project_root.into(),
            task_log_dir: task_log_dir.into(),
            incident_db: None,
        }
    }

    pub fn with_incident_db(mut self, db: Db) -> Self {
        self.incident_db = Some(db);
        self
    }

    pub fn inspect_task(
        &self,
        task: &PueueTask,
        config: &CheckConfig,
    ) -> Result<Vec<Observation>, AppError> {
        self.inspect_task_at(task, config, unix_timestamp()?)
    }

    pub fn inspect_task_at(
        &self,
        task: &PueueTask,
        config: &CheckConfig,
        seen_at: i64,
    ) -> Result<Vec<Observation>, AppError> {
        let mut observations = Vec::new();
        let project_id = self.project_id.as_deref().unwrap_or(task.group.as_str());
        let task_key = task_incident_key(task);
        let signature = crate::reconcile::task_signature(task);
        if let Some(snapshot) = self.read_task_snapshot(task.id, config.log_tail_bytes)? {
            observations.extend(task_pattern_observations(
                project_id,
                task_key.as_str(),
                signature.as_str(),
                &snapshot,
                config,
                seen_at,
            )?);
            let active_stall = self.active_stall_fingerprint(project_id, &task_key)?;
            let current_stall_fingerprint = stalled_observation_fingerprint(&task_key, &snapshot);
            if active_stall
                .as_deref()
                .is_some_and(|fingerprint| fingerprint != current_stall_fingerprint)
            {
                observations.push(Observation::task_stalled_recovered(
                    project_id,
                    &task_key,
                    &signature,
                    snapshot.clone(),
                    seen_at,
                ));
            } else if let Some(stalled) = task_stall_observation(
                project_id, &task_key, &signature, task, &snapshot, config, seen_at,
            ) {
                observations.push(stalled);
            }
        }

        for relative_path in &config.extra_log_paths {
            let path = self.canonical_extra_log_path(relative_path)?;
            let snapshot = LogSnapshot::read_tail(&path, config.log_tail_bytes)?;
            observations.extend(pattern_observations(
                project_id,
                None,
                Some(relative_path),
                &snapshot,
                config,
                seen_at,
            )?);
        }

        Ok(observations)
    }

    fn read_task_snapshot(
        &self,
        task_id: i64,
        tail_bytes: u32,
    ) -> Result<Option<LogSnapshot>, AppError> {
        for candidate in [
            self.task_log_dir.join(format!("{task_id}.log")),
            self.task_log_dir.join(format!("task_{task_id}.log")),
        ] {
            match LogSnapshot::read_tail(&candidate, tail_bytes) {
                Ok(snapshot) => return Ok(Some(snapshot)),
                Err(AppError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    fn active_stall_fingerprint(
        &self,
        project_id: &str,
        task_key: &str,
    ) -> Result<Option<String>, AppError> {
        let Some(db) = &self.incident_db else {
            return Ok(None);
        };
        db.connect()?
            .query_row(
                "SELECT fingerprint FROM incidents
                 WHERE project_id = ?1 AND kind = 'stalled' AND task_key = ?2
                   AND status IN ('open', 'acknowledged')
                 ORDER BY last_seen_at DESC, incident_id DESC
                 LIMIT 1",
                params![project_id, task_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(database_error("find active stalled incident"))
    }

    fn canonical_extra_log_path(&self, relative_path: &Path) -> Result<PathBuf, AppError> {
        if relative_path.is_absolute() {
            return Err(AppError::Configuration {
                field: "check.extra_log_paths",
            });
        }
        let root = fs::canonicalize(&self.project_root).map_err(|source| AppError::Io {
            operation: "canonicalize project root",
            source,
        })?;
        let path = fs::canonicalize(root.join(relative_path)).map_err(|source| AppError::Io {
            operation: "canonicalize extra log path",
            source,
        })?;
        if !path.starts_with(&root) {
            return Err(AppError::Configuration {
                field: "check.extra_log_paths",
            });
        }
        Ok(path)
    }
}

fn task_stall_observation(
    project_id: &str,
    task_key: &str,
    task_signature: &str,
    task: &PueueTask,
    snapshot: &LogSnapshot,
    config: &CheckConfig,
    seen_at: i64,
) -> Option<Observation> {
    if !task.is_running() {
        return None;
    }
    let modified_at = snapshot.modified_at_nanos? / 1_000_000_000;
    let seen_at = u128::try_from(seen_at).ok()?;
    let unchanged_seconds = seen_at.saturating_sub(modified_at);
    let action_delay_minutes = if config.stall.action == PatternAction::Kill {
        config.stall.kill_after_minutes
    } else {
        0
    };
    let observation_threshold_seconds =
        (u128::from(config.stall_minutes) + u128::from(action_delay_minutes)) * 60;
    if unchanged_seconds < observation_threshold_seconds {
        return None;
    }

    Some(Observation::task_stalled(
        project_id,
        task_key,
        task_signature,
        snapshot.clone(),
        config.stall.action,
        i64::try_from(seen_at).ok()?,
    ))
}

fn task_pattern_observations(
    project_id: &str,
    task_key: &str,
    task_signature: &str,
    snapshot: &LogSnapshot,
    config: &CheckConfig,
    seen_at: i64,
) -> Result<Vec<Observation>, AppError> {
    let mut observations = Vec::new();
    let tail = snapshot.evidence.as_str();
    for pattern in &config.patterns {
        let regex = Regex::new(&pattern.regex).map_err(|_| AppError::Configuration {
            field: "check.patterns.regex",
        })?;
        let count = regex.find_iter(tail.as_bytes()).count();
        if count >= usize::try_from(pattern.confirm_matches).unwrap_or(usize::MAX) {
            observations.push(Observation::task_pattern(
                project_id,
                task_key,
                task_signature,
                &pattern.name,
                pattern.action,
                pattern.confirm_matches,
                snapshot.evidence.clone(),
                seen_at,
            ));
        }
    }
    Ok(observations)
}

fn pattern_observations(
    project_id: &str,
    task_signature: Option<&str>,
    relative_path: Option<&PathBuf>,
    snapshot: &LogSnapshot,
    config: &CheckConfig,
    seen_at: i64,
) -> Result<Vec<Observation>, AppError> {
    config
        .patterns
        .iter()
        .map(|pattern| {
            let count = count_matches(&snapshot.evidence, &pattern.regex)?;
            if count < pattern.confirm_matches {
                return Ok(None);
            }
            if let Some(task_signature) = task_signature {
                return Ok(Some(Observation::pattern(
                    project_id,
                    task_signature,
                    &pattern.name,
                    pattern.action,
                    count,
                    snapshot.evidence.clone(),
                    seen_at,
                )));
            }
            Ok(relative_path.map(|relative_path| {
                Observation::extra_log_pattern(
                    project_id,
                    relative_path.clone(),
                    &pattern.name,
                    pattern.action,
                    count,
                    snapshot.clone(),
                    seen_at,
                )
            }))
        })
        .filter_map(Result::transpose)
        .collect()
}

fn count_matches(haystack: &str, pattern: &str) -> Result<u32, AppError> {
    let regex = Regex::new(pattern).map_err(|_| AppError::Configuration {
        field: "check.patterns.regex",
    })?;
    Ok(u32::try_from(regex.find_iter(haystack).count()).unwrap_or(u32::MAX))
}

fn unix_timestamp() -> Result<i64, AppError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read current system time",
        })?;
    i64::try_from(duration.as_secs()).map_err(|_| AppError::Runtime {
        operation: "convert current system time",
    })
}
