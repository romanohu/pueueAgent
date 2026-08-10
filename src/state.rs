use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AppError;

pub const CANONICAL_STATE_SCHEMA_VERSION: u32 = 1;
pub const MAX_STATE_BYTES: usize = 64 * 1024;
pub const MAX_STATE_DEPTH: usize = 8;
pub const MAX_CURRENT_FACTS: usize = 64;
pub const MAX_HISTORICAL_FACTS: usize = 256;
pub const MAX_FACT_BYTES: usize = 1_024;
pub const MAX_NEXT_ACTION_BYTES: usize = 2_048;
pub const MAX_BUDGETS: usize = 32;
pub const MAX_BUDGET_KEY_BYTES: usize = 64;
pub const MAX_BUDGET_VALUE: u64 = 1_000_000;
pub const MAX_LINEAGE_IDS: usize = 64;
pub const MAX_LINEAGE_ID_BYTES: usize = 128;
pub const MAX_STATE_MARKDOWN_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalState {
    pub schema_version: u32,
    pub current_facts: Vec<String>,
    pub historical_facts: Vec<String>,
    pub next_action: String,
    pub budgets: BTreeMap<String, u64>,
    pub active_lineage: ActiveLineage,
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
    let bytes = read_bounded(path, MAX_STATE_BYTES, "open canonical state")?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|source| AppError::Serialization {
            operation: "parse canonical state",
            source,
        })?;
    validate_depth(&value, 1)?;
    let state: CanonicalState =
        serde_json::from_value(value).map_err(|source| AppError::Serialization {
            operation: "decode canonical state schema",
            source,
        })?;
    Ok(state.validate()?)
}

pub fn load_state_markdown(path: &Path) -> Result<String, AppError> {
    let bytes = read_bounded(
        path,
        MAX_STATE_MARKDOWN_BYTES,
        "read supplementary STATE.md",
    )?;
    String::from_utf8(bytes).map_err(|source| AppError::Io {
        operation: "decode supplementary STATE.md",
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, source),
    })
}

pub fn check_consistency(state: &CanonicalState, state_markdown: &str) -> Vec<StateWarning> {
    let markdown = state_markdown.to_ascii_lowercase();
    let mut warnings = Vec::new();
    if state.active_lineage.is_active() && markdown.contains("campaign stopped") {
        warnings.push(StateWarning {
            code: "state_md.sentinel_contradiction",
            summary: format!(
                "STATE.md contains campaign stopped while canonical state has active lineage; {}",
                state.summary()
            ),
        });
    }
    if !state.active_lineage.is_active() && markdown.contains("campaign active") {
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
            "current_facts={} historical_facts={} next_action={} budgets={} active_lineage={}",
            self.current_facts.len(),
            self.historical_facts.len(),
            usize::from(!self.next_action.is_empty()),
            self.budgets.len(),
            usize::from(self.active_lineage.is_active()),
        )
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
        if self.budgets.len() > MAX_BUDGETS {
            return Err(invalid_state(
                "budgets exceed the maximum number of entries",
            ));
        }
        for (key, value) in &self.budgets {
            validate_text("budget key", key, MAX_BUDGET_KEY_BYTES, true)?;
            if *value > MAX_BUDGET_VALUE {
                return Err(invalid_state("budget value exceeds the supported bound"));
            }
        }
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
        for submission_id in &self.submission_ids {
            validate_text(
                "active lineage submission ID",
                submission_id,
                MAX_LINEAGE_ID_BYTES,
                true,
            )?;
        }
        if self.task_ids.iter().any(|id| *id < 0) {
            return Err(invalid_state(
                "active lineage task IDs must be non-negative",
            ));
        }
        Ok(())
    }
}

fn read_bounded(path: &Path, maximum: usize, operation: &'static str) -> Result<Vec<u8>, AppError> {
    let file = File::open(path).map_err(|source| AppError::Io { operation, source })?;
    let mut bytes = Vec::with_capacity(maximum.min(16 * 1024));
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| AppError::Io { operation, source })?;
    if bytes.len() > maximum {
        return Err(AppError::Validation {
            field: "state",
            message: "canonical state content exceeds the supported bound",
        });
    }
    Ok(bytes)
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
        if reject_duplicates && !seen.insert(fact.trim().to_owned()) {
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
