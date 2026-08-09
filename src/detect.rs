use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use regex_automata::meta::Regex;

use crate::{
    config::{CheckConfig, PatternAction},
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
        Self {
            project_id: project_id.into(),
            kind: "stalled".to_owned(),
            task_key: Some(task_signature.clone()),
            fingerprint: format!(
                "stalled:v1:task={task_signature}:snapshot={}",
                snapshot.fingerprint
            ),
            seen_at,
            state: ObservationState::Active,
            task_signature: Some(task_signature),
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
        Self {
            project_id: project_id.into(),
            kind: "stalled".to_owned(),
            task_key: Some(task_signature.clone()),
            fingerprint: format!("stalled-recovered:v1:snapshot={}", snapshot.fingerprint),
            seen_at,
            state: ObservationState::Recovered,
            task_signature: Some(task_signature),
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

#[derive(Debug, Clone)]
pub struct Detector {
    project_id: Option<String>,
    project_root: PathBuf,
    task_log_dir: PathBuf,
}

impl Detector {
    pub fn new(project_root: impl Into<PathBuf>, task_log_dir: impl Into<PathBuf>) -> Self {
        Self {
            project_id: None,
            project_root: project_root.into(),
            task_log_dir: task_log_dir.into(),
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
        }
    }

    pub fn inspect_task(
        &self,
        task: &PueueTask,
        config: &CheckConfig,
    ) -> Result<Vec<Observation>, AppError> {
        let mut observations = Vec::new();
        let project_id = self.project_id.as_deref().unwrap_or(task.group.as_str());
        let task_key = task_incident_key(task);
        let signature = crate::reconcile::task_signature(task);
        let seen_at = unix_timestamp()?;
        if let Some(snapshot) = self.read_task_snapshot(task.id, config.log_tail_bytes)? {
            observations.extend(task_pattern_observations(
                project_id,
                task_key.as_str(),
                signature.as_str(),
                &snapshot,
                config,
                seen_at,
            )?);
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
