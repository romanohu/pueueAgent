//! Construction of the supervisor-owned Codex command line.
//!
//! Project supplied `agent.args` is deliberately treated as a compatibility
//! envelope.  It is never copied to the child command.  The only accepted
//! forms are `{prompt}` and `exec {prompt}`; all command-line policy is built
//! from the resolved service policy below.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::Path,
};

use crate::{
    codex_session,
    config::AgentConfig,
    environment::{
        is_auth_name, is_proxy_or_cert_name, private_temp_target_path, shell_baseline_names,
        VerifiedPrivateTemp,
    },
    execution_policy::{
        preflight_decision_runtime, AgentKind, NetworkMode, PolicyViolation,
        PolicyViolationCode, PolicyViolationStage, ResolvedProjectExecutionPolicy,
    },
    models::AgentContextMode,
    AppError,
};

#[cfg(target_os = "linux")]
use std::{os::fd::AsRawFd, os::unix::process::CommandExt, process::Stdio};
#[cfg(target_os = "linux")]
use tokio::io::AsyncReadExt;

/// Capabilities discovered from the installed Codex CLI.
///
/// The adapter intentionally requires an explicit positive result for every
/// forced option.  A default value therefore cannot accidentally enable a
/// less secure invocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodexCapabilities {
    pub workspace_write: bool,
    pub read_only: bool,
    pub approval_never: bool,
    pub network_mode: bool,
    pub project_config_isolation: bool,
    pub json_output_schema: bool,
    pub output_last_message: bool,
    pub permission_profiles: bool,
}

impl CodexCapabilities {
    pub const fn standard_policy() -> Self {
        Self {
            workspace_write: true,
            read_only: false,
            approval_never: true,
            network_mode: true,
            project_config_isolation: true,
            json_output_schema: false,
            output_last_message: false,
            permission_profiles: false,
        }
    }

    pub const fn all() -> Self {
        Self {
            workspace_write: true,
            read_only: true,
            approval_never: true,
            network_mode: true,
            project_config_isolation: true,
            json_output_schema: true,
            output_last_message: true,
            permission_profiles: true,
        }
    }

    pub const fn none() -> Self {
        Self {
            workspace_write: false,
            read_only: false,
            approval_never: false,
            network_mode: false,
            project_config_isolation: false,
            json_output_schema: false,
            output_last_message: false,
            permission_profiles: false,
        }
    }

    pub const fn supports_forced_policy(self) -> bool {
        self.workspace_write
            && self.approval_never
            && self.network_mode
            && self.project_config_isolation
    }

    pub const fn supports_decision_policy(self) -> bool {
        self.read_only
            && self.approval_never
            && self.network_mode
            && self.project_config_isolation
            && self.json_output_schema
            && self.output_last_message
            && self.permission_profiles
    }
}

#[cfg(any(test, target_os = "linux"))]
pub(crate) fn detect_codex_capabilities(
    version_output: &str,
    root_help: &str,
    exec_help: &str,
) -> Result<CodexCapabilities, PolicyViolation> {
    let permission_profiles = codex_version_at_least(version_output, (0, 138, 0));
    let capabilities = CodexCapabilities {
        workspace_write: root_help.contains("workspace-write"),
        read_only: root_help.contains("read-only"),
        approval_never: root_help.contains("--ask-for-approval") && root_help.contains("never"),
        network_mode: permission_profiles,
        project_config_isolation: root_help.contains("--strict-config")
            && exec_help.contains("--strict-config")
            && exec_help.contains("--ignore-user-config")
            && exec_help.contains("--ignore-rules"),
        json_output_schema: exec_help.contains("--output-schema"),
        output_last_message: exec_help.contains("--output-last-message"),
        permission_profiles,
    };
    capabilities
        .supports_decision_policy()
        .then_some(capabilities)
        .ok_or_else(unsafe_argument)
}

#[cfg(any(test, target_os = "linux"))]
fn codex_version_at_least(output: &str, minimum: (u64, u64, u64)) -> bool {
    output.split_ascii_whitespace().any(|token| {
        let token = token.trim_matches(|character: char| {
            !character.is_ascii_digit() && character != '.'
        });
        let mut parts = token.split('.');
        let version = (
            parts.next().and_then(|part| part.parse::<u64>().ok()),
            parts.next().and_then(|part| part.parse::<u64>().ok()),
            parts.next().and_then(|part| part.parse::<u64>().ok()),
        );
        matches!(version, (Some(major), Some(minor), Some(patch)) if (major, minor, patch) >= minimum)
    })
}

pub(crate) async fn probe_installed_codex_capabilities(
    anchor: &crate::execution_policy::ExecutableAnchor,
) -> Result<CodexCapabilities, PolicyViolation> {
    preflight_decision_runtime()?;
    #[cfg(not(target_os = "linux"))]
    {
        let _ = anchor;
        unreachable!("decision runtime preflight rejects non-Linux hosts")
    }
    #[cfg(target_os = "linux")]
    {
        let version = probe_pinned_codex(anchor, &["--version"]).await?;
        let root_help = probe_pinned_codex(anchor, &["--help"]).await?;
        let exec_help = probe_pinned_codex(anchor, &["exec", "--help"]).await?;
        detect_codex_capabilities(&version, &root_help, &exec_help)
    }
}

#[cfg(target_os = "linux")]
async fn probe_pinned_codex(
    anchor: &crate::execution_policy::ExecutableAnchor,
    args: &[&str],
) -> Result<String, PolicyViolation> {
    const MAX_PROBE_BYTES: u64 = 256 * 1024;
    const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

    let verified = anchor.verify_identity().map_err(pre_binding_violation)?;
    let program = format!("/proc/self/fd/{}", verified.file.as_raw_fd());
    let mut command = std::process::Command::new(program);
    command
        .args(args)
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command.process_group(0);
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    let mut child = command.spawn().map_err(|_| unsafe_argument())?;
    let pid = child.id().ok_or_else(unsafe_argument)? as libc::pid_t;
    let stdout = child.stdout.take().ok_or_else(unsafe_argument)?;
    let reader = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stdout
            .take(MAX_PROBE_BYTES + 1)
            .read_to_end(&mut bytes)
            .await
            .map(|_| bytes)
    });
    let status = match tokio::time::timeout(PROBE_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) | Err(_) => {
            unsafe { libc::kill(-pid, libc::SIGKILL); }
            let _ = child.kill().await;
            let _ = child.wait().await;
            reader.abort();
            return Err(unsafe_argument());
        }
    };
    let bytes = match tokio::time::timeout(PROBE_TIMEOUT, reader).await {
        Ok(Ok(Ok(bytes))) => bytes,
        Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
            unsafe { libc::kill(-pid, libc::SIGKILL); }
            return Err(unsafe_argument());
        }
    };
    if !status.success() || bytes.len() as u64 > MAX_PROBE_BYTES {
        return Err(unsafe_argument());
    }
    if unsafe { libc::kill(-pid, 0) } == 0 {
        unsafe { libc::kill(-pid, libc::SIGKILL); }
        return Err(unsafe_argument());
    }
    if std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH) {
        return Err(unsafe_argument());
    }
    anchor.verify_identity().map_err(pre_binding_violation)?;
    String::from_utf8(bytes).map_err(|_| unsafe_argument())
}

#[cfg(target_os = "linux")]
fn pre_binding_violation(mut violation: PolicyViolation) -> PolicyViolation {
    violation.stage = PolicyViolationStage::PreBinding;
    violation
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
    ) -> Result<Vec<OsString>, PolicyViolation> {
        self.build_for_private_temp_path(config, prompt, private_temp_target_path())
    }

    pub(crate) fn build_with_private_temp(
        &self,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &VerifiedPrivateTemp,
    ) -> Result<Vec<OsString>, PolicyViolation> {
        self.build_for_private_temp_path(config, prompt, private_tmp.target_path())
    }

    pub(crate) fn build_decision_with_private_temp(
        &self,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &VerifiedPrivateTemp,
    ) -> Result<Vec<OsString>, PolicyViolation> {
        self.preflight_decision(config, prompt)?;

        let root = path_text(&self.policy.root_anchor.canonical_path)?;
        let schema = path_text(&private_tmp.target_path().join("decision-schema.json"))?;
        let output = path_text(&private_tmp.target_path().join("decision.json"))?;
        let mut argv = vec![
            OsString::from("--ask-for-approval"),
            OsString::from("never"),
            OsString::from("exec"),
            OsString::from("--ignore-user-config"),
            OsString::from("--ignore-rules"),
            OsString::from("--strict-config"),
            OsString::from("-C"),
            OsString::from(root.clone()),
            OsString::from("--output-schema"),
            OsString::from(schema),
            OsString::from("--output-last-message"),
            OsString::from(output),
        ];

        push_codex_overrides(&mut argv, config)?;
        push_decision_permission_profile(&mut argv, self.policy.network);
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
                environment_filters(&BTreeSet::new())?
            ),
        );
        argv.push(OsString::from("--"));
        argv.push(OsString::from(prompt));
        Ok(argv)
    }

    fn build_for_private_temp_path(
        &self,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &Path,
    ) -> Result<Vec<OsString>, PolicyViolation> {
        self.preflight(config, prompt)?;

        let root = path_text(&self.policy.root_anchor.canonical_path)?;
        let private_tmp = path_text(private_tmp)?;
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

        push_codex_overrides(&mut argv, config)?;

        push_config(
            &mut argv,
            format!(
                "sandbox_workspace_write.network_access={}",
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

    /// Validate every launch property that does not depend on the run id.
    /// Scheduler admission calls this before reserving interventions or
    /// binding events; `build` calls the same method before materializing the
    /// final per-run temporary path.
    pub fn preflight(
        &self,
        config: &AgentConfig,
        prompt: &str,
    ) -> Result<(), PolicyViolation> {
        self.validate_capabilities()?;
        if self.policy.agent_kind != AgentKind::BuiltInCodex || config.program != "codex" {
            return Err(unsafe_argument());
        }
        validate_compatibility_args(&config.args)?;
        if prompt.contains('\0') {
            return Err(unsafe_argument());
        }
        match &config.context {
            AgentContextMode::Fresh => {}
            AgentContextMode::Resume { session_id } => {
                codex_session::verify_project_ownership(
                    &self.policy.codex_home,
                    &self.policy.root_anchor.canonical_path,
                    session_id,
                )
                .map_err(map_session_error)?;
            }
            AgentContextMode::ResumeLatest => {
                codex_session::resolve_latest_owned_session(
                    &self.policy.codex_home,
                    &self.policy.root_anchor.canonical_path,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn preflight_decision(
        &self,
        config: &AgentConfig,
        prompt: &str,
    ) -> Result<(), PolicyViolation> {
        preflight_decision_runtime()?;
        if self.policy.agent_kind != AgentKind::BuiltInCodex
            || !self.capabilities.supports_decision_policy()
            || prompt.contains('\0')
            || config
                .codex
                .model
                .as_deref()
                .is_some_and(|model| model.is_empty() || model.contains('\0'))
        {
            return Err(unsafe_argument());
        }
        Ok(())
    }

    fn validate_capabilities(&self) -> Result<(), PolicyViolation> {
        self.capabilities
            .supports_forced_policy()
            .then_some(())
            .ok_or_else(unsafe_argument)
    }
}

fn push_codex_overrides(
    argv: &mut Vec<OsString>,
    config: &AgentConfig,
) -> Result<(), PolicyViolation> {
    if let Some(model) = config.codex.model.as_deref() {
        if model.is_empty() || model.contains('\0') {
            return Err(unsafe_argument());
        }
        argv.push(OsString::from("--model"));
        argv.push(OsString::from(model));
    }
    if let Some(reasoning_effort) = config.codex.reasoning_effort {
        push_config(
            argv,
            format!(
                "model_reasoning_effort=\"{}\"",
                reasoning_effort.as_str()
            ),
        );
    }
    Ok(())
}

fn validate_compatibility_args(args: &[String]) -> Result<(), PolicyViolation> {
    let valid = match args {
        [prompt] if prompt == "{prompt}" => true,
        [exec, prompt] if exec == "exec" && prompt == "{prompt}" => true,
        _ => false,
    };
    valid.then_some(()).ok_or_else(unsafe_argument)
}

fn path_text(path: &Path) -> Result<String, PolicyViolation> {
    path.to_str().map(str::to_owned).ok_or_else(unsafe_argument)
}

fn push_config(argv: &mut Vec<OsString>, value: String) {
    argv.push(OsString::from("-c"));
    argv.push(OsString::from(value));
}

fn push_decision_permission_profile(argv: &mut Vec<OsString>, network: NetworkMode) {
    push_config(
        argv,
        "permissions.pueue_agent_decision.extends=\":read-only\"".to_owned(),
    );
    push_config(
        argv,
        format!(
            "permissions.pueue_agent_decision.network.enabled={}",
            network_mode(network)
        ),
    );
    push_config(
        argv,
        "default_permissions=\"pueue_agent_decision\"".to_owned(),
    );
}

fn network_mode(network: NetworkMode) -> &'static str {
    match network {
        NetworkMode::Enabled => "true",
        NetworkMode::Disabled => "false",
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
        if is_auth_name(name) || is_proxy_or_cert_name(name) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_permission_profile_extends_read_only_and_owns_network_policy() {
        for (network, expected) in [
            (NetworkMode::Enabled, "permissions.pueue_agent_decision.network.enabled=true"),
            (NetworkMode::Disabled, "permissions.pueue_agent_decision.network.enabled=false"),
        ] {
            let mut argv = Vec::new();
            push_decision_permission_profile(&mut argv, network);
            assert!(argv.iter().any(|value| {
                value == "permissions.pueue_agent_decision.extends=\":read-only\""
            }));
            assert!(argv.iter().any(|value| value == expected));
            assert!(argv.iter().any(|value| {
                value == "default_permissions=\"pueue_agent_decision\""
            }));
            assert!(!argv.iter().any(|value| {
                value == "--sandbox"
                    || value.to_string_lossy().starts_with("sandbox_workspace_write.")
            }));
        }
    }

    #[test]
    fn installed_codex_capabilities_require_supported_version_and_exact_help_surface() {
        let root_help = "--strict-config --sandbox read-only workspace-write --ask-for-approval never";
        let exec_help = "--ignore-user-config --ignore-rules --strict-config --output-schema --output-last-message";
        let capabilities = detect_codex_capabilities("codex-cli 0.148.0", root_help, exec_help)
            .unwrap();
        assert!(capabilities.supports_decision_policy());
        assert!(detect_codex_capabilities("codex-cli 0.137.9", root_help, exec_help).is_err());
        assert!(detect_codex_capabilities(
            "codex-cli 0.148.0",
            root_help,
            "--ignore-user-config --ignore-rules --strict-config --output-schema",
        )
        .is_err());
    }
}
