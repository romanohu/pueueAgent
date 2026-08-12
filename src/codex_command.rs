//! Construction of the supervisor-owned Codex command line.
//!
//! Project supplied `agent.args` is deliberately treated as a compatibility
//! envelope.  It is never copied to the child command.  The only accepted
//! forms are `{prompt}` and `exec {prompt}`; all command-line policy is built
//! from the resolved service policy below.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Component, Path},
};

use crate::{
    codex_session,
    config::AgentConfig,
    environment::{is_auth_name, shell_baseline_names},
    execution_policy::{
        AgentKind, NetworkMode, PolicyViolation, PolicyViolationCode, PolicyViolationStage,
        ResolvedProjectExecutionPolicy,
    },
    models::AgentContextMode,
    AppError,
};

const MAX_PRIVATE_RUN_COMPONENT_BYTES: usize = 128;
const MAX_PRIVATE_ROOT_COMPONENTS: usize = 16;

/// Capabilities discovered from the installed Codex CLI.
///
/// The adapter intentionally requires an explicit positive result for every
/// forced option.  A default value therefore cannot accidentally enable a
/// less secure invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodexCapabilities {
    pub workspace_write: bool,
    pub approval_never: bool,
    pub network_mode: bool,
    pub project_config_isolation: bool,
}

impl CodexCapabilities {
    pub const fn all() -> Self {
        Self {
            workspace_write: true,
            approval_never: true,
            network_mode: true,
            project_config_isolation: true,
        }
    }

    pub const fn none() -> Self {
        Self {
            workspace_write: false,
            approval_never: false,
            network_mode: false,
            project_config_isolation: false,
        }
    }

    pub const fn supports_forced_policy(self) -> bool {
        self.workspace_write
            && self.approval_never
            && self.network_mode
            && self.project_config_isolation
    }
}

/// A command builder bound to one resolved project policy.
#[derive(Clone, Debug)]
pub struct CodexArgvBuilder {
    policy: ResolvedProjectExecutionPolicy,
    capabilities: CodexCapabilities,
}

impl CodexArgvBuilder {
    pub fn new(
        policy: ResolvedProjectExecutionPolicy,
        capabilities: CodexCapabilities,
    ) -> Self {
        Self {
            policy,
            capabilities,
        }
    }

    pub fn policy(&self) -> &ResolvedProjectExecutionPolicy {
        &self.policy
    }

    pub fn capabilities(&self) -> CodexCapabilities {
        self.capabilities
    }

    /// Build final Codex argv.  The returned vector excludes the executable
    /// path; callers must use the startup-anchored Codex executable.
    pub fn build(
        &self,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &Path,
    ) -> Result<Vec<OsString>, PolicyViolation> {
        self.validate_capabilities()?;
        if self.policy.agent_kind != AgentKind::BuiltInCodex || config.program != "codex" {
            return Err(unsafe_argument());
        }
        validate_compatibility_args(&config.args)?;
        if prompt.contains('\0') {
            return Err(unsafe_argument());
        }

        let root = path_text(&self.policy.root_anchor.canonical_path)?;
        let private_tmp = validate_private_tmp(
            private_tmp,
            &self.policy.root_anchor.canonical_path,
            &self.policy.private_temp_relative_root,
        )?;
        let mut argv = vec![
            OsString::from("--ask-for-approval"),
            OsString::from("never"),
            OsString::from("exec"),
            OsString::from("--ignore-user-config"),
            OsString::from("--ignore-rules"),
            OsString::from("--strict-config"),
            OsString::from("--sandbox"),
            OsString::from("workspace-write"),
            OsString::from("-C"),
            OsString::from(root.clone()),
        ];

        if let Some(model) = config.codex.model.as_deref() {
            if model.is_empty() || model.contains('\0') {
                return Err(unsafe_argument());
            }
            argv.push(OsString::from("--model"));
            argv.push(OsString::from(model));
        }

        push_config(
            &mut argv,
            format!(
                "sandbox_workspace_write.network_access=\"{}\"",
                network_mode(self.policy.network)
            ),
        );
        push_config(&mut argv, "sandbox_workspace_write.exclude_slash_tmp=true".to_owned());
        push_config(
            &mut argv,
            "sandbox_workspace_write.exclude_tmpdir_env_var=true".to_owned(),
        );
        push_config(
            &mut argv,
            format!(
                "sandbox_workspace_write.writable_roots=[{}]",
                toml_quote(&private_tmp)
            ),
        );
        push_config(
            &mut argv,
            format!(
                "projects={{{}={{trust_level=\"untrusted\"}}}}",
                toml_quote(&root)
            ),
        );
        push_config(&mut argv, "allow_login_shell=false".to_owned());
        push_config(
            &mut argv,
            format!(
                "shell_environment_policy={{inherit=\"all\",ignore_default_excludes=false,experimental_use_profile=false,filters={{{}}}}}",
                environment_filters(&self.policy.task_environment_allow)?
            ),
        );

        if let Some(reasoning_effort) = config.codex.reasoning_effort {
            push_config(
                &mut argv,
                format!(
                    "model_reasoning_effort=\"{}\"",
                    reasoning_effort.as_str()
                ),
            );
        }

        match &config.context {
            AgentContextMode::Fresh => {}
            AgentContextMode::Resume { session_id } => {
                let owned = codex_session::verify_project_ownership(
                    &self.policy.codex_home,
                    &self.policy.root_anchor.canonical_path,
                    session_id,
                )
                .map_err(map_session_error)?;
                argv.push(OsString::from("resume"));
                argv.push(OsString::from(owned));
            }
            AgentContextMode::ResumeLatest => {
                let owned = codex_session::resolve_latest_owned_session(
                    &self.policy.codex_home,
                    &self.policy.root_anchor.canonical_path,
                )?;
                argv.push(OsString::from("resume"));
                argv.push(OsString::from(owned));
            }
        }
        argv.push(OsString::from("--"));
        argv.push(OsString::from(prompt));
        Ok(argv)
    }

    fn validate_capabilities(&self) -> Result<(), PolicyViolation> {
        self.capabilities
            .supports_forced_policy()
            .then_some(())
            .ok_or_else(unsafe_argument)
    }
}

fn validate_compatibility_args(args: &[String]) -> Result<(), PolicyViolation> {
    let valid = match args {
        [prompt] if prompt == "{prompt}" => true,
        [exec, prompt] if exec == "exec" && prompt == "{prompt}" => true,
        _ => false,
    };
    valid.then_some(()).ok_or_else(unsafe_argument)
}

fn validate_private_tmp(
    path: &Path,
    project_root: &Path,
    private_root: &Path,
) -> Result<String, PolicyViolation> {
    let private_root_components = private_root.components().collect::<Vec<_>>();
    if private_root.is_absolute()
        || private_root_components.is_empty()
        || private_root_components.len() > MAX_PRIVATE_ROOT_COMPONENTS
        || private_root_components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(unsafe_argument());
    }
    let fixed_root = project_root.join(private_root);
    let Some(run_component) = path.file_name().and_then(|value| value.to_str()) else {
        return Err(unsafe_argument());
    };
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(component, Component::CurDir | Component::ParentDir)
        })
        || path == Path::new("/")
        || path.parent() != Some(fixed_root.as_path())
        || run_component.is_empty()
        || run_component == "."
        || run_component == ".."
        || run_component.len() > MAX_PRIVATE_RUN_COMPONENT_BYTES
        || run_component.chars().any(char::is_control)
    {
        return Err(unsafe_argument());
    }
    path_text(path)
}

fn path_text(path: &Path) -> Result<String, PolicyViolation> {
    path.to_str().map(str::to_owned).ok_or_else(unsafe_argument)
}

fn push_config(argv: &mut Vec<OsString>, value: String) {
    argv.push(OsString::from("-c"));
    argv.push(OsString::from(value));
}

fn network_mode(network: NetworkMode) -> &'static str {
    match network {
        NetworkMode::Enabled => "enabled",
        NetworkMode::Disabled => "disabled",
    }
}

fn environment_filters(task_allow: &BTreeSet<String>) -> Result<String, PolicyViolation> {
    let mut names = shell_baseline_names()
        .iter()
        .copied()
        .filter(|name| !is_auth_name(name))
        .collect::<BTreeSet<_>>();
    names.extend(task_allow.iter().map(String::as_str));
    let mut entries = Vec::new();
    for name in names {
        if is_auth_name(name) {
            continue;
        }
        if !valid_environment_name(name) {
            return Err(PolicyViolation::new(
                PolicyViolationCode::EnvironmentName,
                PolicyViolationStage::PreBinding,
            ));
        }
        entries.push(format!("{}=\"include\"", name));
    }
    Ok(entries.join(","))
}

fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
        && name.len() <= 128
}

fn toml_quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('"');
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn unsafe_argument() -> PolicyViolation {
    PolicyViolation::new(
        PolicyViolationCode::UnsafeCodexArgument,
        PolicyViolationStage::PreBinding,
    )
}

fn map_session_error(error: AppError) -> PolicyViolation {
    let code = match error {
        AppError::CodexSessionMetadata { reason, .. }
            if reason.contains("not found") || reason.contains("cannot be found") =>
        {
            PolicyViolationCode::SessionMissing
        }
        AppError::CodexSessionMetadata { .. } => PolicyViolationCode::SessionNotOwned,
        _ => PolicyViolationCode::SessionNotOwned,
    };
    PolicyViolation::new(code, PolicyViolationStage::PreBinding)
}
