use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::AppError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectConfig {
    pub project_id: String,
    pub pueue_group: String,
    pub agent: AgentConfig,
    pub check: CheckConfig,
    pub guardrails: GuardrailsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConfig {
    pub program: String,
    pub args: Vec<String>,
    pub timeout_minutes: u32,
    pub max_retries: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConfig {
    pub interval_minutes: u32,
    pub deep_check_every: u32,
    pub deep_check_interval_minutes: u32,
    pub stall_minutes: u32,
    pub extra_log_paths: Vec<PathBuf>,
    pub patterns: Vec<PatternConfig>,
    pub stall: StallConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternConfig {
    pub name: String,
    pub regex: String,
    pub action: PatternAction,
    pub confirm_matches: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternAction {
    Notify,
    Wake,
    Kill,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StallConfig {
    pub action: PatternAction,
    pub kill_after_minutes: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardrailsConfig {
    pub max_consecutive_failures: u32,
    pub max_experiments: u32,
}

pub fn load(path: &Path) -> Result<ProjectConfig, AppError> {
    let contents = fs::read_to_string(path).map_err(|source| AppError::Io {
        operation: "read project configuration",
        source,
    })?;
    let raw: RawProjectConfig = toml::from_str(&contents).map_err(|_| AppError::Configuration {
        field: "config.toml",
    })?;

    raw.validate()
}

impl PatternAction {
    fn parse(value: &str, field: &'static str) -> Result<Self, AppError> {
        match value {
            "notify" => Ok(Self::Notify),
            "wake" => Ok(Self::Wake),
            "kill" => Ok(Self::Kill),
            _ => Err(AppError::Configuration { field }),
        }
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawProjectConfig {
    project_id: String,
    pueue_group: String,
    agent: RawAgentConfig,
    check: RawCheckConfig,
    guardrails: RawGuardrailsConfig,
}

impl RawProjectConfig {
    fn validate(self) -> Result<ProjectConfig, AppError> {
        required(&self.project_id, "project_id")?;
        required(&self.pueue_group, "pueue_group")?;

        Ok(ProjectConfig {
            project_id: self.project_id,
            pueue_group: self.pueue_group,
            agent: self.agent.validate()?,
            check: self.check.validate()?,
            guardrails: self.guardrails.validate()?,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawAgentConfig {
    program: String,
    args: Vec<String>,
    timeout_minutes: i64,
    max_retries: i64,
}

impl RawAgentConfig {
    fn validate(self) -> Result<AgentConfig, AppError> {
        required(&self.program, "agent.program")?;

        Ok(AgentConfig {
            program: self.program,
            args: self.args,
            timeout_minutes: positive(self.timeout_minutes, "agent.timeout_minutes")?,
            max_retries: non_negative(self.max_retries, "agent.max_retries")?,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawCheckConfig {
    interval_minutes: i64,
    deep_check_every: i64,
    deep_check_interval_minutes: i64,
    stall_minutes: i64,
    extra_log_paths: Vec<PathBuf>,
    patterns: Vec<RawPatternConfig>,
    stall: RawStallConfig,
}

impl RawCheckConfig {
    fn validate(self) -> Result<CheckConfig, AppError> {
        Ok(CheckConfig {
            interval_minutes: positive(self.interval_minutes, "check.interval_minutes")?,
            deep_check_every: positive(self.deep_check_every, "check.deep_check_every")?,
            deep_check_interval_minutes: non_negative(
                self.deep_check_interval_minutes,
                "check.deep_check_interval_minutes",
            )?,
            stall_minutes: positive(self.stall_minutes, "check.stall_minutes")?,
            extra_log_paths: self.extra_log_paths,
            patterns: self
                .patterns
                .into_iter()
                .map(RawPatternConfig::validate)
                .collect::<Result<Vec<_>, _>>()?,
            stall: self.stall.validate()?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawPatternConfig {
    #[serde(default)]
    name: String,
    #[serde(default)]
    regex: String,
    #[serde(default = "default_wake")]
    action: String,
    #[serde(default = "default_confirm_matches")]
    confirm_matches: i64,
}

impl RawPatternConfig {
    fn validate(self) -> Result<PatternConfig, AppError> {
        required(&self.regex, "check.patterns.regex")?;
        let action = PatternAction::parse(&self.action, "check.patterns.action")?;
        if action == PatternAction::Kill {
            required(&self.name, "check.patterns.name")?;
        }

        Ok(PatternConfig {
            name: self.name,
            regex: self.regex,
            action,
            confirm_matches: positive(self.confirm_matches, "check.patterns.confirm_matches")?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawStallConfig {
    #[serde(default = "default_notify")]
    action: String,
    #[serde(default)]
    kill_after_minutes: i64,
}

impl Default for RawStallConfig {
    fn default() -> Self {
        Self {
            action: default_notify(),
            kill_after_minutes: 0,
        }
    }
}

impl RawStallConfig {
    fn validate(self) -> Result<StallConfig, AppError> {
        let action = PatternAction::parse(&self.action, "check.stall.action")?;
        let kill_after_minutes =
            non_negative(self.kill_after_minutes, "check.stall.kill_after_minutes")?;
        if action == PatternAction::Kill && kill_after_minutes == 0 {
            return Err(AppError::Configuration {
                field: "check.stall.kill_after_minutes",
            });
        }

        Ok(StallConfig {
            action,
            kill_after_minutes,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct RawGuardrailsConfig {
    max_consecutive_failures: i64,
    max_experiments: i64,
}

impl RawGuardrailsConfig {
    fn validate(self) -> Result<GuardrailsConfig, AppError> {
        Ok(GuardrailsConfig {
            max_consecutive_failures: positive(
                self.max_consecutive_failures,
                "guardrails.max_consecutive_failures",
            )?,
            max_experiments: positive(self.max_experiments, "guardrails.max_experiments")?,
        })
    }
}

fn default_wake() -> String {
    "wake".to_owned()
}

fn default_notify() -> String {
    "notify".to_owned()
}

fn default_confirm_matches() -> i64 {
    1
}

fn required(value: &str, field: &'static str) -> Result<(), AppError> {
    if value.trim().is_empty() {
        return Err(AppError::Configuration { field });
    }

    Ok(())
}

fn positive(value: i64, field: &'static str) -> Result<u32, AppError> {
    if value <= 0 {
        return Err(AppError::Configuration { field });
    }

    u32::try_from(value).map_err(|_| AppError::Configuration { field })
}

fn non_negative(value: i64, field: &'static str) -> Result<u32, AppError> {
    if value < 0 {
        return Err(AppError::Configuration { field });
    }

    u32::try_from(value).map_err(|_| AppError::Configuration { field })
}
