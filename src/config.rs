use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    codex_session,
    execution_policy::NetworkMode,
    models::AgentContextMode,
    AppError,
};

pub const DEFAULT_LOG_TAIL_BYTES: u32 = 64 * 1024;
pub const MAX_LOG_TAIL_BYTES: u32 = 1_048_576;
pub const DEFAULT_MAX_AGENT_RUNS: u32 = 100;

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
    pub context: AgentContextMode,
    pub execution: AgentExecutionConfig,
    pub codex: AgentCodexConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentExecutionConfig {
    pub network: NetworkMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCodexConfig {
    pub model: Option<String>,
    pub reasoning_effort: Option<CodexReasoningEffort>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexReasoningEffort {
    Low,
    Medium,
    High,
    XHigh,
}

impl CodexReasoningEffort {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConfig {
    pub interval_minutes: u32,
    pub deep_check_interval_minutes: u32,
    pub stall_minutes: u32,
    pub log_tail_bytes: u32,
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
    pub max_agent_runs: u32,
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
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Notify => "notify",
            Self::Wake => "wake",
            Self::Kill => "kill",
        }
    }

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
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
struct RawAgentConfig {
    program: String,
    args: Vec<String>,
    timeout_minutes: i64,
    max_retries: i64,
    context: RawAgentContextConfig,
    execution: RawAgentExecutionConfig,
    codex: RawAgentCodexConfig,
}

impl RawAgentConfig {
    fn validate(self) -> Result<AgentConfig, AppError> {
        required(&self.program, "agent.program")?;
        let context = self.context.validate(&self.program)?;
        let execution = self.execution.validate()?;
        let codex = self.codex.validate(&self.program)?;

        Ok(AgentConfig {
            program: self.program,
            args: self.args,
            timeout_minutes: positive(self.timeout_minutes, "agent.timeout_minutes")?,
            max_retries: non_negative(self.max_retries, "agent.max_retries")?,
            context,
            execution,
            codex,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RawAgentExecutionConfig {
    #[serde(default = "default_network")]
    network: String,
}

impl Default for RawAgentExecutionConfig {
    fn default() -> Self {
        Self {
            network: default_network(),
        }
    }
}

impl RawAgentExecutionConfig {
    fn validate(self) -> Result<AgentExecutionConfig, AppError> {
        let network = match self.network.trim() {
            "enabled" => NetworkMode::Enabled,
            "disabled" => NetworkMode::Disabled,
            _ => {
                return Err(AppError::Configuration {
                    field: "agent.execution.network",
                })
            }
        };
        Ok(AgentExecutionConfig { network })
    }
}

fn default_network() -> String {
    "enabled".to_owned()
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawAgentCodexConfig {
    model: Option<String>,
    reasoning_effort: Option<String>,
}

impl RawAgentCodexConfig {
    fn validate(self, program: &str) -> Result<AgentCodexConfig, AppError> {
        if program != "codex"
            && (self.model.is_some() || self.reasoning_effort.is_some())
        {
            return Err(AppError::Configuration {
                field: "agent.codex",
            });
        }

        let model = self.model.map(|model| model.trim().to_owned());
        if model.as_deref().is_some_and(str::is_empty) {
            return Err(AppError::Configuration {
                field: "agent.codex.model",
            });
        }

        let reasoning_effort = self.reasoning_effort.map(|value| match value.trim() {
            "low" => Ok(CodexReasoningEffort::Low),
            "medium" => Ok(CodexReasoningEffort::Medium),
            "high" => Ok(CodexReasoningEffort::High),
            "xhigh" => Ok(CodexReasoningEffort::XHigh),
            _ => Err(AppError::Configuration {
                field: "agent.codex.reasoning_effort",
            }),
        }).transpose()?;

        Ok(AgentCodexConfig {
            model,
            reasoning_effort,
        })
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawAgentContextConfig {
    mode: String,
    session_id: Option<String>,
}

impl RawAgentContextConfig {
    fn validate(self, program: &str) -> Result<AgentContextMode, AppError> {
        let mode = if self.mode.trim().is_empty() {
            "fresh"
        } else {
            self.mode.trim()
        };

        let context = match mode {
            "fresh" => {
                if self
                    .session_id
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    return Err(AppError::Configuration {
                        field: "agent.context.session_id",
                    });
                }
                AgentContextMode::Fresh
            }
            "resume" => {
                let session_id = self
                    .session_id
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
                    .ok_or(AppError::Configuration {
                        field: "agent.context.session_id",
                    })?;
                let session_id = codex_session::normalize_session_id(&session_id)?;
                AgentContextMode::Resume { session_id }
            }
            "resume_latest" => {
                if self
                    .session_id
                    .as_ref()
                    .is_some_and(|value| !value.trim().is_empty())
                {
                    return Err(AppError::Configuration {
                        field: "agent.context.session_id",
                    });
                }
                AgentContextMode::ResumeLatest
            }
            _ => {
                return Err(AppError::Configuration {
                    field: "agent.context.mode",
                })
            }
        };

        if !matches!(context, AgentContextMode::Fresh) && program != "codex" {
            return Err(AppError::Configuration {
                field: "agent.context.mode",
            });
        }

        Ok(context)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct RawCheckConfig {
    interval_minutes: i64,
    deep_check_interval_minutes: i64,
    stall_minutes: i64,
    #[serde(default = "default_log_tail_bytes")]
    log_tail_bytes: i64,
    extra_log_paths: Vec<PathBuf>,
    patterns: Vec<RawPatternConfig>,
    stall: RawStallConfig,
}

impl RawCheckConfig {
    fn validate(self) -> Result<CheckConfig, AppError> {
        Ok(CheckConfig {
            interval_minutes: positive(self.interval_minutes, "check.interval_minutes")?,
            deep_check_interval_minutes: non_negative(
                self.deep_check_interval_minutes,
                "check.deep_check_interval_minutes",
            )?,
            stall_minutes: positive(self.stall_minutes, "check.stall_minutes")?,
            log_tail_bytes: bounded_log_tail(self.log_tail_bytes)?,
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
struct RawGuardrailsConfig {
    max_consecutive_failures: i64,
    max_experiments: i64,
    #[serde(default = "default_max_agent_runs")]
    max_agent_runs: i64,
}

impl RawGuardrailsConfig {
    fn validate(self) -> Result<GuardrailsConfig, AppError> {
        Ok(GuardrailsConfig {
            max_consecutive_failures: positive(
                self.max_consecutive_failures,
                "guardrails.max_consecutive_failures",
            )?,
            max_experiments: positive(self.max_experiments, "guardrails.max_experiments")?,
            max_agent_runs: positive(self.max_agent_runs, "guardrails.max_agent_runs")?,
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

fn default_log_tail_bytes() -> i64 {
    i64::from(DEFAULT_LOG_TAIL_BYTES)
}

pub(crate) fn bounded_log_tail(value: i64) -> Result<u32, AppError> {
    if !(1..=i64::from(MAX_LOG_TAIL_BYTES)).contains(&value) {
        return Err(AppError::Configuration {
            field: "check.log_tail_bytes",
        });
    }

    Ok(u32::try_from(value).expect("bounded log tail fits in u32"))
}

fn default_max_agent_runs() -> i64 {
    i64::from(DEFAULT_MAX_AGENT_RUNS)
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
