use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{config::GuardrailsConfig, AppError};

pub const CANONICAL_STATE_SCHEMA_VERSION: u32 = 2;
pub const MAX_STATE_BYTES: usize = 64 * 1024;
pub const MAX_STATE_DEPTH: usize = 32;
pub const MAX_CURRENT_FACTS: usize = 64;
pub const MAX_HISTORICAL_FACTS: usize = 256;
pub const MAX_FACT_BYTES: usize = 1_024;
pub const MAX_NEXT_ACTION_BYTES: usize = 2_048;
const MAX_LEGACY_BUDGETS: usize = 32;
const MAX_LEGACY_BUDGET_KEY_BYTES: usize = 64;
const MAX_LEGACY_BUDGET_VALUE: u64 = 1_000_000;
pub const MAX_LINEAGE_IDS: usize = 64;
pub const MAX_LINEAGE_ID_BYTES: usize = 128;
pub const MAX_STATE_MARKDOWN_BYTES: usize = 64 * 1024;
pub const MAX_OBJECTIVE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalState {
    pub schema_version: u32,
    pub current_facts: Vec<String>,
    pub historical_facts: Vec<String>,
    pub next_action: String,
    pub active_lineage: ActiveLineage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCanonicalState {
    schema_version: u32,
    current_facts: Vec<String>,
    historical_facts: Vec<String>,
    next_action: String,
    budgets: Option<BTreeMap<String, u64>>,
    active_lineage: ActiveLineage,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ObjectiveSnapshot {
    pub text: String,
    pub digest: String,
}

impl std::fmt::Debug for ObjectiveSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectiveSnapshot")
            .field("bytes", &self.text.len())
            .field("digest", &self.digest)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveLineage {
    pub event_id: Option<i64>,
    pub run_id: Option<i64>,
    pub submission_ids: Vec<String>,
    pub task_ids: Vec<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateWarning {
    pub code: &'static str,
    pub summary: String,
}

impl std::fmt::Display for StateWarning {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.summary)
    }
}

pub fn path(project_root: &Path) -> PathBuf {
    project_root.join(".pueue-agent/state.json")
}

pub fn load(path: &Path) -> Result<CanonicalState, AppError> {
    let bytes = read_bounded(path, MAX_STATE_BYTES, "open canonical state", "state.json")?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|source| AppError::Serialization {
            operation: "parse canonical state",
            source,
        })?;
    validate_depth(&value, 1)?;
    let has_budgets = value
        .as_object()
        .ok_or_else(|| invalid_state("canonical state must be a JSON object"))?
        .contains_key("budgets");
    let raw: RawCanonicalState =
        serde_json::from_value(value).map_err(|source| AppError::Serialization {
            operation: "decode canonical state schema",
            source,
        })?;
    let state = match raw.schema_version {
        1 => {
            if let Some(budgets) = raw.budgets.as_ref() {
                validate_legacy_budgets(budgets)?;
            }
            CanonicalState {
                schema_version: CANONICAL_STATE_SCHEMA_VERSION,
                current_facts: raw.current_facts,
                historical_facts: raw.historical_facts,
                next_action: raw.next_action,
                active_lineage: raw.active_lineage,
            }
        }
        CANONICAL_STATE_SCHEMA_VERSION => {
            if has_budgets {
                return Err(invalid_state("schema v2 must not contain budgets"));
            }
            CanonicalState {
                schema_version: raw.schema_version,
                current_facts: raw.current_facts,
                historical_facts: raw.historical_facts,
                next_action: raw.next_action,
                active_lineage: raw.active_lineage,
            }
        }
        _ => return Err(invalid_state("unsupported schema_version")),
    };
    state.validate()
}

pub fn load_if_present(path: &Path) -> Result<Option<CanonicalState>, AppError> {
    match fs::symlink_metadata(path) {
        Ok(_) => load(path).map(Some),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(AppError::Io {
            operation: "inspect canonical state",
            source,
        }),
    }
}

pub fn load_state_markdown(path: &Path) -> Result<String, AppError> {
    let bytes = read_bounded(
        path,
        MAX_STATE_MARKDOWN_BYTES,
        "read supplementary STATE.md",
        "STATE.md",
    )?;
    String::from_utf8(bytes).map_err(|source| AppError::Io {
        operation: "decode supplementary STATE.md",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

pub fn load_objective(project_root: &Path) -> Result<ObjectiveSnapshot, AppError> {
    let path = project_root.join(".pueue-agent/STATE.md");
    let bytes = read_bounded(
        &path,
        MAX_OBJECTIVE_BYTES,
        "read campaign objective",
        "STATE.md",
    )?;
    let source = String::from_utf8(bytes).map_err(|source| AppError::Io {
        operation: "decode campaign objective",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })?;
    let text = source.replace("\r\n", "\n");
    validate_objective_text(&text)?;
    use sha2::{Digest, Sha256};
    let digest = format!("{:x}", Sha256::digest(text.as_bytes()));
    Ok(ObjectiveSnapshot { text, digest })
}

pub fn load_effective_guardrails(
    path: &Path,
    configured: &GuardrailsConfig,
) -> Result<GuardrailsConfig, AppError> {
    let _ = load_if_present(path)?;
    Ok(configured.clone())
}

pub fn check_consistency(state: &CanonicalState, state_markdown: &str) -> Vec<StateWarning> {
    let current_fact_active = state.has_current_fact("campaign active");
    let current_fact_stopped = state.has_current_fact("campaign stopped");
    let canonical_active = state.active_lineage.is_active() || current_fact_active;
    let canonical_stopped =
        current_fact_stopped || (!state.active_lineage.is_active() && !current_fact_active);
    let mut warnings = Vec::new();
    if canonical_active && has_markdown_sentinel(state_markdown, "campaign stopped") {
        warnings.push(StateWarning {
            code: "state_md.sentinel_contradiction",
            summary: format!(
                "STATE.md contains campaign stopped while canonical state has active lineage; {}",
                state.summary()
            ),
        });
    }
    if canonical_stopped && has_markdown_sentinel(state_markdown, "campaign active") {
        warnings.push(StateWarning {
            code: "state_md.sentinel_contradiction",
            summary: format!(
                "STATE.md contains campaign active while canonical state has no active lineage; {}",
                state.summary()
            ),
        });
    }
    warnings
}

impl CanonicalState {
    pub fn summary(&self) -> String {
        format!(
            "current_facts={} historical_facts={} next_action={} active_lineage={}",
            self.current_facts.len(),
            self.historical_facts.len(),
            usize::from(!self.next_action.is_empty()),
            usize::from(self.active_lineage.is_active()),
        )
    }

    fn has_current_fact(&self, sentinel: &str) -> bool {
        self.current_facts
            .iter()
            .any(|fact| normalize_sentinel_text(fact) == sentinel)
    }

    fn validate(self) -> Result<Self, AppError> {
        if self.schema_version != CANONICAL_STATE_SCHEMA_VERSION {
            return Err(invalid_state("unsupported schema_version"));
        }
        validate_facts(
            "current_facts",
            &self.current_facts,
            MAX_CURRENT_FACTS,
            true,
        )?;
        validate_facts(
            "historical_facts",
            &self.historical_facts,
            MAX_HISTORICAL_FACTS,
            false,
        )?;
        validate_text(
            "next_action",
            &self.next_action,
            MAX_NEXT_ACTION_BYTES,
            true,
        )?;
        self.active_lineage.validate()?;
        Ok(self)
    }
}

impl ActiveLineage {
    fn is_active(&self) -> bool {
        self.event_id.is_some()
            || self.run_id.is_some()
            || !self.submission_ids.is_empty()
            || !self.task_ids.is_empty()
    }

    fn validate(&self) -> Result<(), AppError> {
        if self.event_id.is_some_and(|id| id < 0) || self.run_id.is_some_and(|id| id < 0) {
            return Err(invalid_state("active lineage IDs must be non-negative"));
        }
        if self.submission_ids.len() > MAX_LINEAGE_IDS || self.task_ids.len() > MAX_LINEAGE_IDS {
            return Err(invalid_state(
                "active lineage exceeds the maximum number of IDs",
            ));
        }
        let mut submission_ids = BTreeSet::new();
        for submission_id in &self.submission_ids {
            validate_text(
                "active lineage submission ID",
                submission_id,
                MAX_LINEAGE_ID_BYTES,
                true,
            )?;
            if !submission_ids.insert(submission_id) {
                return Err(invalid_state("duplicate active lineage submission ID"));
            }
        }
        let mut task_ids = BTreeSet::new();
        if self.task_ids.iter().any(|id| *id < 0) {
            return Err(invalid_state(
                "active lineage task IDs must be non-negative",
            ));
        }
        for task_id in &self.task_ids {
            if !task_ids.insert(task_id) {
                return Err(invalid_state("duplicate active lineage task ID"));
            }
        }
        Ok(())
    }
}

fn validate_legacy_budgets(budgets: &BTreeMap<String, u64>) -> Result<(), AppError> {
    if budgets.len() > MAX_LEGACY_BUDGETS {
        return Err(invalid_state(
            "legacy budgets exceed the maximum number of entries",
        ));
    }
    for (key, value) in budgets {
        validate_text("legacy budget key", key, MAX_LEGACY_BUDGET_KEY_BYTES, true)?;
        if *value > MAX_LEGACY_BUDGET_VALUE {
            return Err(invalid_state("legacy budget value exceeds the supported bound"));
        }
    }
    Ok(())
}

fn has_markdown_sentinel(markdown: &str, sentinel: &str) -> bool {
    markdown
        .lines()
        .any(|line| normalized_markdown_line(line).is_some_and(|line| line == sentinel))
}

fn normalized_markdown_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let content = if trimmed.starts_with('#') {
        let hash_count = trimmed.bytes().take_while(|byte| *byte == b'#').count();
        if hash_count == trimmed.len() || !trimmed.as_bytes()[hash_count].is_ascii_whitespace() {
            return None;
        }
        trimmed[hash_count..].trim()
    } else {
        trimmed
    };
    let normalized = normalize_sentinel_text(content);
    match normalized.as_str() {
        "campaign active" | "campaign stopped" => Some(normalized),
        _ => None,
    }
}

fn normalize_sentinel_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn read_bounded(
    path: &Path,
    maximum: usize,
    operation: &'static str,
    field: &'static str,
) -> Result<Vec<u8>, AppError> {
    let file = File::open(path).map_err(|source| AppError::Io { operation, source })?;
    let mut bytes = Vec::with_capacity(maximum.min(16 * 1024));
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| AppError::Io { operation, source })?;
    if bytes.len() > maximum {
        return Err(AppError::Validation {
            field,
            message: "content exceeds the supported bound",
        });
    }
    Ok(bytes)
}

fn validate_objective_text(text: &str) -> Result<(), AppError> {
    if text == include_str!("../templates/STATE.md") || text.contains("(目的をここに書く)") {
        return Err(invalid_objective("replace the generated objective template"));
    }
    if text
        .chars()
        .any(|character| {
            character == '\0' || (character.is_control() && !character.is_whitespace())
        })
    {
        return Err(invalid_objective("objective contains unsafe control characters"));
    }
    if !has_meaningful_objective_line(text) {
        return Err(invalid_objective("objective must contain a meaningful line"));
    }
    Ok(())
}

fn has_meaningful_objective_line(text: &str) -> bool {
    let visible = strip_html_comments(text);
    let lines = visible.lines().collect::<Vec<_>>();
    lines.iter().enumerate().any(|(index, line)| {
        is_meaningful_objective_line(line)
            && !lines
                .get(index + 1)
                .is_some_and(|next| is_markdown_table_separator(next))
    })
}

fn is_meaningful_objective_line(line: &str) -> bool {
    let trimmed = line.trim();
    !trimmed.is_empty()
        && !trimmed.starts_with('#')
        && !(trimmed.starts_with('|') && trimmed.ends_with('|'))
        && !is_markdown_table_separator(trimmed)
}

fn strip_html_comments(text: &str) -> String {
    let mut visible = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(start) = remaining.find("<!--") {
        visible.push_str(&remaining[..start]);
        let after_start = &remaining[start + "<!--".len()..];
        match after_start.find("-->") {
            Some(end) => remaining = &after_start[end + "-->".len()..],
            None => return visible,
        }
    }
    visible.push_str(remaining);
    visible
}

fn is_markdown_table_separator(line: &str) -> bool {
    let trimmed = line.trim().trim_matches('|');
    !trimmed.is_empty()
        && trimmed.split('|').all(|cell| {
            let cell = cell.trim();
            !cell.is_empty()
                && cell.contains('-')
                && cell.chars().all(|character| matches!(character, '-' | ':'))
        })
}

fn invalid_objective(message: &'static str) -> AppError {
    AppError::Validation {
        field: "STATE.md",
        message,
    }
}

fn validate_depth(value: &Value, depth: usize) -> Result<(), AppError> {
    if depth > MAX_STATE_DEPTH {
        return Err(invalid_state(
            "JSON object depth exceeds the supported bound",
        ));
    }
    match value {
        Value::Array(values) => values
            .iter()
            .try_for_each(|value| validate_depth(value, depth + 1)),
        Value::Object(values) => values
            .values()
            .try_for_each(|value| validate_depth(value, depth + 1)),
        _ => Ok(()),
    }
}

fn validate_facts(
    field: &'static str,
    facts: &[String],
    maximum: usize,
    reject_duplicates: bool,
) -> Result<(), AppError> {
    if facts.len() > maximum {
        return Err(invalid_state(match field {
            "current_facts" => "current_facts exceed the supported bound",
            _ => "historical_facts exceed the supported bound",
        }));
    }
    let mut seen = BTreeSet::new();
    for fact in facts {
        validate_text(field, fact, MAX_FACT_BYTES, true)?;
        if reject_duplicates && !seen.insert(normalize_sentinel_text(fact)) {
            return Err(invalid_state("duplicate current fact"));
        }
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    maximum: usize,
    required: bool,
) -> Result<(), AppError> {
    if required && value.trim().is_empty() {
        return Err(invalid_state(match field {
            "next_action" => "next_action must not be empty",
            "budget key" => "budget keys must not be empty",
            "active lineage submission ID" => "active lineage submission IDs must not be empty",
            _ => "state text must not be empty",
        }));
    }
    if value.len() > maximum {
        return Err(invalid_state("state text exceeds the supported bound"));
    }
    Ok(())
}

fn invalid_state(message: &'static str) -> AppError {
    AppError::Validation {
        field: "state.json",
        message,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{validate_depth, MAX_STATE_DEPTH};

    fn value_at_depth(depth: usize) -> Value {
        let mut value = Value::Null;
        for _ in 1..depth {
            value = Value::Array(vec![value]);
        }
        value
    }

    #[test]
    fn canonical_state_depth_boundary_accepts_32_and_rejects_33() {
        assert!(validate_depth(&value_at_depth(32), 1).is_ok());
        assert!(validate_depth(&value_at_depth(33), 1).is_err());
        assert_eq!(MAX_STATE_DEPTH, 32);
    }
}
