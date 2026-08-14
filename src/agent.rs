use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;

use crate::{
    codex_command::{CodexArgvBuilder, CodexCapabilities},
    config::{AgentConfig, ProjectConfig},
    db::{AgentRunRepository, GateFailurePolicy},
    environment::{
        PrivateRunTemp, ProjectAdmissionLock, RunIdAdmissionGuard, SanitizedEnvironment,
        TempInventoryReport,
    },
    execution_policy::{
        resolve_project_policy, AgentKind, PolicyViolation, PolicyViolationCode,
        PolicyViolationStage, ResolvedExecutionPolicy, ResolvedProjectExecutionPolicy,
    },
    interventions::InterventionReservation,
    models::{
        launch_gate_marker_path, AgentContextMode, AgentRunStatus, ExecutionProjection,
        NewAgentRun, Project,
    },
    native_launcher::{NativeAgentChild, NativeLaunchSpec, NativeLauncher},
    output::bounded_redacted_text,
    process::TerminalObservation,
    project_logs::{inspect_gate_marker, ProjectRootLogReader},
    retry::{EventResolution, RetryPolicy},
    upgrade::AgentStartUpgradeGuard,
    AppError,
};

#[derive(Debug, Clone)]
pub struct AgentRunnerConfig {
    codex_capabilities: CodexCapabilities,
}

impl AgentRunnerConfig {
    pub fn production() -> Self {
        Self {
            codex_capabilities: CodexCapabilities::none(),
        }
    }

    pub fn with_codex_capabilities(mut self, capabilities: CodexCapabilities) -> Self {
        self.codex_capabilities = capabilities;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentCommand {
    pub program: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentSpawnStage {
    PreBinding,
    RunBoundPreMarker { run_id: i64, resolved: bool },
    PostMarker { run_id: i64, resolved: bool },
}

#[derive(Debug)]
pub struct AgentSpawnError {
    pub stage: AgentSpawnStage,
    pub source: AppError,
    /// Present when `source` is a bounded policy classification.  The
    /// scheduler uses this field to select direct dead-letter semantics at
    /// the event boundary; ordinary AppError values retain retry behavior.
    pub policy: Option<PolicyViolation>,
    pub cleanup: Option<BoundCleanupHandle>,
}

impl std::fmt::Display for AgentSpawnError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for AgentSpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub struct AgentHandle {
    pub project_id: String,
    pub run_id: i64,
    child: NativeAgentChild,
    retained_authority: RetainedLaunchAuthority,
    pub pid: i64,
    pub timeout_deadline: Instant,
    pub log_path: PathBuf,
    pub retry_policy: RetryPolicy,
    terminal_outcome: Option<TerminalOutcome>,
    terminal_persistence: TerminalPersistence,
    process_proof: ProcessProof,
}

enum RetainedLaunchAuthority {
    Retained {
        global_policy: Arc<ResolvedExecutionPolicy>,
        project_policy: ResolvedProjectExecutionPolicy,
        temp: PrivateRunTemp,
        execution: ExecutionProjection,
    },
    Released,
    #[cfg(test)]
    Test,
}

enum BoundFinalizationIntent {
    PreMarker {
        reason: String,
        resolution: GateFailurePolicy,
    },
    PostMarkerPolicy {
        violation: PolicyViolation,
    },
    PostMarkerExecutionUnknown {
        reason: String,
    },
    PendingMarkerPolicy {
        violation: PolicyViolation,
    },
}

impl BoundFinalizationIntent {
    fn from_failure(source: &AppError, retry_policy: RetryPolicy) -> Self {
        match policy_from_error(source) {
            Some(violation)
                if matches!(
                    violation.stage,
                    PolicyViolationStage::PostMarker
                        | PolicyViolationStage::Dispatched
                        | PolicyViolationStage::Finalized
                ) => Self::PostMarkerPolicy { violation },
            Some(violation) => Self::PreMarker {
                reason: bounded_redacted_text(&source.to_string()),
                resolution: GateFailurePolicy::Policy(violation),
            },
            None => Self::PreMarker {
                reason: bounded_redacted_text(&source.to_string()),
                resolution: GateFailurePolicy::Retry(retry_policy),
            },
        }
    }

    fn is_post_marker(&self) -> bool {
        matches!(
            self,
            Self::PostMarkerPolicy { .. } | Self::PostMarkerExecutionUnknown { .. }
                | Self::PendingMarkerPolicy { .. }
        )
    }

    fn finalize(
        &self,
        db: &crate::db::Db,
        project_id: &str,
        run_id: i64,
        finished_at: i64,
    ) -> Result<(), AppError> {
        let repository = AgentRunRepository::new(db);
        match self {
            Self::PreMarker { reason, resolution } => {
                repository.fail_before_gate_release_with_policy(
                    project_id,
                    run_id,
                    finished_at,
                    reason,
                    *resolution,
                )?;
            }
            Self::PostMarkerPolicy { violation } => {
                repository.finish_after_marker_policy_failure(
                    project_id,
                    run_id,
                    finished_at,
                    violation,
                )?;
            }
            Self::PostMarkerExecutionUnknown { reason } => {
                repository.finish_after_marker_failure(
                    project_id,
                    run_id,
                    finished_at,
                    reason,
                )?;
            }
            Self::PendingMarkerPolicy { violation } => {
                repository.finish_pending_marker_policy_failure(
                    project_id,
                    run_id,
                    finished_at,
                    violation,
                )?;
            }
        }
        Ok(())
    }
}

pub struct BoundCleanupHandle {
    project_id: String,
    run_id: i64,
    intent: BoundFinalizationIntent,
    kind: BoundCleanupKind,
}

enum BoundCleanupKind {
    LiveChild {
        child: NativeAgentChild,
        retained_authority: RetainedLaunchAuthority,
        terminated: bool,
        finalized: bool,
    },
    PendingMarker,
}

impl std::fmt::Debug for BoundCleanupHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundCleanupHandle")
            .field("run_id", &self.run_id)
            .finish()
    }
}

impl BoundCleanupHandle {
    pub fn run_id(&self) -> i64 {
        self.run_id
    }

    pub(crate) fn cleanup_pending(&self) -> bool {
        matches!(
            &self.kind,
            BoundCleanupKind::LiveChild {
                finalized: true,
                retained_authority: RetainedLaunchAuthority::Retained { .. },
                ..
            }
        )
    }

    pub(crate) fn cleanup_blocked_project(&self) -> Option<&str> {
        self.cleanup_pending().then_some(self.project_id.as_str())
    }

    pub async fn retry(
        &mut self,
        db: &crate::db::Db,
        finished_at: i64,
    ) -> Result<(), AppError> {
        self.retry_inner(db, finished_at, None).await
    }

    pub(crate) async fn retry_before(
        &mut self,
        db: &crate::db::Db,
        finished_at: i64,
        deadline: Instant,
    ) -> Result<(), AppError> {
        self.retry_inner(db, finished_at, Some(deadline)).await
    }

    async fn retry_inner(
        &mut self,
        db: &crate::db::Db,
        finished_at: i64,
        deadline: Option<Instant>,
    ) -> Result<(), AppError> {
        if let BoundCleanupKind::LiveChild {
            child,
            terminated,
            ..
        } = &mut self.kind
        {
            if !*terminated {
                child.terminate().await?;
                *terminated = true;
            }
        }
        let needs_finalization = matches!(
            &self.kind,
            BoundCleanupKind::LiveChild {
                finalized: false,
                ..
            }
        ) || matches!(&self.kind, BoundCleanupKind::PendingMarker);
        if needs_finalization {
            let scoped_db = deadline_scoped_db(db, deadline)?;
            self.intent.finalize(
                scoped_db.as_ref().unwrap_or(db),
                &self.project_id,
                self.run_id,
                finished_at,
            )?;
            if let BoundCleanupKind::LiveChild { finalized, .. } = &mut self.kind {
                *finalized = true;
            }
        }
        if let BoundCleanupKind::LiveChild {
            retained_authority: RetainedLaunchAuthority::Retained { temp, .. },
            ..
        } = &mut self.kind
        {
            temp.cleanup_contents_before(deadline.map(Instant::into_std))
                .map_err(AppError::from)?;
        }
        if let BoundCleanupKind::LiveChild {
            retained_authority,
            ..
        } = &mut self.kind
        {
            *retained_authority = RetainedLaunchAuthority::Released;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum TerminalPersistence {
    Pending,
    Persisted(AgentRunStatus),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessProof {
    Owned,
    OwnershipLost,
}

#[derive(Debug, Clone)]
struct TerminalOutcome {
    status: AgentRunStatus,
    exit_code: Option<i64>,
    last_error: Option<String>,
}

pub struct AgentRunner {
    config: AgentRunnerConfig,
    policy: Arc<ResolvedExecutionPolicy>,
}

impl AgentRunner {
    pub fn new(config: AgentRunnerConfig, policy: Arc<ResolvedExecutionPolicy>) -> Self {
        Self { config, policy }
    }

    pub(crate) fn try_acquire_run_id_admission_guard(
        &self,
        db: &crate::db::Db,
    ) -> Result<Option<RunIdAdmissionGuard>, PolicyViolation> {
        RunIdAdmissionGuard::try_acquire(db.run_id_lock_parent())
    }

    pub fn resolve_project_policy(
        &self,
        project: &Project,
        config: &ProjectConfig,
    ) -> Result<ResolvedProjectExecutionPolicy, PolicyViolation> {
        resolve_project_policy(&self.policy, project, config)
    }

    pub fn preflight_private_temp_capacity(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        durable_run_id_high_water: i64,
    ) -> Result<TempInventoryReport, PolicyViolation> {
        let verified_root = policy
            .root_anchor
            .verify_identity()
            .map_err(|mut violation| {
                violation.stage = PolicyViolationStage::PreBinding;
                violation
            })?;
        let report = PrivateRunTemp::inspect_capacity(&verified_root)?;
        report.validate_durable_high_water(durable_run_id_high_water)?;
        Ok(report)
    }

    pub(crate) fn try_acquire_project_admission_lock(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
    ) -> Result<Option<ProjectAdmissionLock>, PolicyViolation> {
        let verified_root = policy
            .root_anchor
            .verify_identity()
            .map_err(|mut violation| {
                violation.stage = PolicyViolationStage::PreBinding;
                violation
            })?;
        ProjectAdmissionLock::try_acquire(&verified_root)
    }

    /// Resolve startup marker evidence through the immutable project-root
    /// capability. Stored absolute log paths are used only to prove they are
    /// the exact projection of a bounded production-relative name; they are
    /// never reopened.
    pub(crate) fn inspect_startup_gate_markers(
        &self,
        project: &Project,
        config: &ProjectConfig,
        candidates: &[(i64, String, PathBuf)],
    ) -> Result<BTreeSet<i64>, AppError> {
        let policy = self
            .resolve_project_policy(project, config)
            .map_err(AppError::from)?;
        let verified_root = policy.root_anchor.verify_identity().map_err(AppError::from)?;
        let reader = ProjectRootLogReader::from_verified(verified_root);
        reader.revalidate_root_path_identity()?;
        let mut confirmed = BTreeSet::new();
        for (run_id, _gate_state, stored_log_path) in candidates {
            let relative_log = recovery_relative_log_path(&policy, stored_log_path)?;
            let relative_marker = launch_gate_marker_path(&relative_log);
            if inspect_gate_marker(&reader, &relative_marker)?.is_some() {
                confirmed.insert(*run_id);
            }
        }
        reader.revalidate_root_path_identity()?;
        Ok(confirmed)
    }

    pub fn preflight_project_launch(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        prompt: &str,
    ) -> Result<(), PolicyViolation> {
        match policy.agent_kind {
            AgentKind::BuiltInCodex => {
                CodexArgvBuilder::new(policy.clone(), self.config.codex_capabilities)
                    .preflight(config, prompt)
            }
            AgentKind::Custom
                if matches!(config.context, AgentContextMode::Fresh)
                    && !prompt.contains('\0')
                    && config.args.iter().all(|argument| !argument.contains('\0')) =>
            {
                Ok(())
            }
            AgentKind::Custom => Err(PolicyViolation::new(
                crate::execution_policy::PolicyViolationCode::UnsafeCodexArgument,
                PolicyViolationStage::PreBinding,
            )),
        }
    }

    pub fn command_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &std::path::Path,
    ) -> Result<AgentCommand, AppError> {
        let program = policy
            .agent_anchor
            .canonical_path
            .to_str()
            .ok_or(AppError::Configuration {
                field: "agent.program",
            })?
            .to_owned();
        let args = match policy.agent_kind {
            AgentKind::BuiltInCodex => {
                CodexArgvBuilder::new(policy.clone(), self.config.codex_capabilities)
                    .build(config, prompt, private_tmp)
                    .map_err(AppError::from)?
                    .into_iter()
                    .map(|argument| {
                        argument
                            .into_string()
                            .map_err(|_| AppError::Configuration { field: "agent.args" })
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            AgentKind::Custom => {
                if !matches!(config.context, AgentContextMode::Fresh) {
                    return Err(PolicyViolation::new(
                        crate::execution_policy::PolicyViolationCode::UnsafeCodexArgument,
                        PolicyViolationStage::PreBinding,
                    )
                    .into());
                }
                config
                    .args
                    .iter()
                    .map(|argument| argument.replace("{prompt}", prompt))
                    .collect()
            }
        };
        Ok(AgentCommand { program, args })
    }

    fn execution_projection(
        policy: &ResolvedProjectExecutionPolicy,
    ) -> Result<ExecutionProjection, AppError> {
        let identity = policy.agent_anchor.identity;
        let identity = format!(
            "dev={};ino={};uid={};mode={:o}",
            identity.device, identity.inode, identity.owner, identity.mode
        );
        ExecutionProjection::new(
            match policy.agent_kind {
                AgentKind::BuiltInCodex => "codex",
                AgentKind::Custom => "custom",
            },
            policy.agent_anchor.canonical_path.to_str().ok_or(AppError::Configuration {
                field: "agent.program",
            })?,
            identity,
        )
    }

    fn environment_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<SanitizedEnvironment, PolicyViolation> {
        match policy.agent_kind {
            AgentKind::BuiltInCodex => SanitizedEnvironment::for_codex_agent(
                &self.policy.startup_environment,
                policy,
                run_id,
            ),
            AgentKind::Custom => SanitizedEnvironment::for_custom_agent(
                &self.policy.startup_environment,
                policy,
                run_id,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn spawn(
        &self,
        db: &crate::db::Db,
        project: &Project,
        project_policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        reservation: Option<&InterventionReservation>,
        prompt: &str,
        now: i64,
        run_id_guard: RunIdAdmissionGuard,
        project_lock: ProjectAdmissionLock,
    ) -> Result<AgentHandle, AgentSpawnError> {
        let agent_start_guard = AgentStartUpgradeGuard::acquire(db).map_err(pre_binding_error)?;
        self.preflight_project_launch(project_policy, config, prompt)
            .map_err(|error| pre_binding_error(error.into()))?;
        let relative_log_path = relative_log_path(primary_event_id, now);
        let relative_marker_path = launch_gate_marker_path(&relative_log_path);
        let log_path = project_policy
            .root_anchor
            .canonical_path
            .join(&relative_log_path);
        let execution = Self::execution_projection(project_policy).map_err(pre_binding_error)?;
        let repository = AgentRunRepository::new(db);
        let run = repository
            .insert_with_events_and_reservation_with_guard(
                &NewAgentRun::with_context(
                    &project.project_id,
                    primary_event_id,
                    None,
                    AgentRunStatus::Starting,
                    now,
                    &log_path,
                    config.context.clone(),
                    config.context.session_id().map(str::to_owned),
                    event_ids.iter().map(i64::to_string).collect(),
                )
                .with_execution(execution.clone()),
                event_ids,
                reservation.map(|reservation| reservation.token.as_str()),
                &run_id_guard,
            )
            .map_err(pre_binding_error)?;
        drop(agent_start_guard);
        let verified_root = project_policy
            .root_anchor
            .verify_identity()
            .map_err(|error| {
                resolve_bound_failure(
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error.into(),
                )
            })?;
        let temp = PrivateRunTemp::create(&verified_root, run.run_id)
            .map_err(|error| {
                resolve_bound_failure(
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error.into(),
                )
            })?;
        drop(run_id_guard);
        drop(project_lock);
        let command = self
            .command_for(project_policy, config, prompt, temp.path())
            .map_err(|error| {
                resolve_bound_failure(
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error,
                )
            })?;
        let environment = self
            .environment_for(project_policy, run.run_id)
            .map_err(|error| {
                resolve_bound_failure(
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error.into(),
                )
            })?;
        let mut argv = Vec::with_capacity(command.args.len() + 1);
        argv.push(OsString::from(&command.program));
        argv.extend(command.args.into_iter().map(OsString::from));
        let mut child = NativeLauncher::spawn(NativeLaunchSpec {
            launcher: self.policy.launcher_anchor.clone(),
            executable: project_policy.agent_anchor.clone(),
            argv,
            cwd: Some(project_policy.root_anchor.canonical_path.clone()),
            environment,
            project_root: verified_root,
            relative_log_path,
            relative_marker_path,
        })
        .map_err(|error| {
            resolve_native_spawn_failure(
                &repository,
                project,
                run.run_id,
                now,
                retry_policy,
                error,
            )
        })?;
        let pid = child.id();
        if let Err(error) = repository.mark_running_and_apply_interventions(
            &project.project_id,
            run.run_id,
            pid,
            now,
        ) {
            return Err(resolve_live_child_failure(
                db,
                &project.project_id,
                run.run_id,
                now,
                child,
                RetainedLaunchAuthority::Retained {
                    global_policy: self.policy.clone(),
                    project_policy: project_policy.clone(),
                    temp,
                    execution,
                },
                BoundFinalizationIntent::from_failure(&error, retry_policy),
                error,
            )
            .await);
        }
        if let Err(error) =
            repository.mark_gate_release_requested(&project.project_id, run.run_id)
        {
            return Err(resolve_live_child_failure(
                db,
                &project.project_id,
                run.run_id,
                now,
                child,
                RetainedLaunchAuthority::Retained {
                    global_policy: self.policy.clone(),
                    project_policy: project_policy.clone(),
                    temp,
                    execution,
                },
                BoundFinalizationIntent::from_failure(&error, retry_policy),
                error,
            )
            .await);
        }
        if let Err(error) = temp.revalidate_current() {
            let error = AppError::from(error);
            return Err(resolve_live_child_failure(
                db,
                &project.project_id,
                run.run_id,
                now,
                child,
                RetainedLaunchAuthority::Retained {
                    global_policy: self.policy.clone(),
                    project_policy: project_policy.clone(),
                    temp,
                    execution,
                },
                BoundFinalizationIntent::from_failure(&error, retry_policy),
                error,
            )
            .await);
        }
        let retained_authority = RetainedLaunchAuthority::Retained {
            global_policy: self.policy.clone(),
            project_policy: project_policy.clone(),
            temp,
            execution,
        };
        if let Err(error) = child.authorize_marker().await {
            return Err(resolve_live_child_failure(
                db,
                &project.project_id,
                run.run_id,
                now,
                child,
                retained_authority,
                BoundFinalizationIntent::from_failure(&error, retry_policy),
                error,
            )
            .await);
        }
        if let Err(error) = repository.acknowledge_dispatch(&project.project_id, run.run_id) {
            return Err(resolve_live_child_failure(
                db,
                &project.project_id,
                run.run_id,
                now,
                child,
                retained_authority,
                BoundFinalizationIntent::PostMarkerExecutionUnknown {
                    reason: "post_marker_dispatch_ack".to_owned(),
                },
                error,
            )
            .await);
        }

        Ok(AgentHandle {
            project_id: project.project_id.clone(),
            run_id: run.run_id,
            child,
            retained_authority,
            pid,
            timeout_deadline: Instant::now()
                + Duration::from_secs(u64::from(config.timeout_minutes) * 60),
            log_path,
            retry_policy,
            terminal_outcome: None,
            terminal_persistence: TerminalPersistence::Pending,
            process_proof: ProcessProof::Owned,
        })
    }
}

fn relative_log_path(primary_event_id: i64, now: i64) -> PathBuf {
    PathBuf::from(format!(
        ".pueue-agent/logs/agent-{now}-{primary_event_id}.log"
    ))
}

fn recovery_relative_log_path(
    policy: &ResolvedProjectExecutionPolicy,
    stored_log_path: &Path,
) -> Result<PathBuf, AppError> {
    let relative = stored_log_path
        .strip_prefix(&policy.root_anchor.canonical_path)
        .map_err(|_| {
            AppError::from(PolicyViolation::new(
                PolicyViolationCode::LogUnsafe,
                PolicyViolationStage::Startup,
            ))
        })?;
    let mut components = relative.components();
    let first = components.next().and_then(|component| match component {
        std::path::Component::Normal(value) => Some(value),
        _ => None,
    });
    let second = components.next().and_then(|component| match component {
        std::path::Component::Normal(value) => Some(value),
        _ => None,
    });
    let leaf = components.next().and_then(|component| match component {
        std::path::Component::Normal(value) => value.to_str(),
        _ => None,
    });
    let valid_leaf = leaf.is_some_and(|leaf| {
        leaf.len() <= 64
            && leaf
                .strip_prefix("agent-")
                .and_then(|value| value.strip_suffix(".log"))
                .and_then(|value| value.rsplit_once('-'))
                .is_some_and(|(started_at, event_id)| {
                    !started_at.is_empty()
                        && !event_id.is_empty()
                        && started_at.bytes().all(|byte| byte.is_ascii_digit())
                        && event_id.bytes().all(|byte| byte.is_ascii_digit())
                })
    });
    if first != Some(std::ffi::OsStr::new(".pueue-agent"))
        || second != Some(std::ffi::OsStr::new("logs"))
        || !valid_leaf
        || components.next().is_some()
        || policy
            .root_anchor
            .canonical_path
            .join(relative)
            .as_os_str()
            != stored_log_path.as_os_str()
    {
        return Err(PolicyViolation::new(
            PolicyViolationCode::LogUnsafe,
            PolicyViolationStage::Startup,
        )
        .into());
    }
    Ok(relative.to_owned())
}

fn pre_binding_error(source: AppError) -> AgentSpawnError {
    let policy = match &source {
        AppError::PolicyViolation { violation } => Some(*violation),
        _ => None,
    };
    AgentSpawnError {
        stage: AgentSpawnStage::PreBinding,
        source,
        policy,
        cleanup: None,
    }
}

fn policy_from_error(source: &AppError) -> Option<PolicyViolation> {
    match source {
        AppError::PolicyViolation { violation } => Some(*violation),
        _ => None,
    }
}

fn resolve_bound_failure(
    repository: &AgentRunRepository<'_>,
    project: &Project,
    run_id: i64,
    finished_at: i64,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    let post_marker = matches!(
        policy_from_error(&source).map(|violation| violation.stage),
        Some(
            PolicyViolationStage::PostMarker
                | PolicyViolationStage::Dispatched
                | PolicyViolationStage::Finalized
        )
    );
    if post_marker {
        resolve_post_marker_failure(
            repository,
            &project.project_id,
            run_id,
            finished_at,
            "post_marker_native_launch",
            source,
        )
    } else {
        let reason = source.to_string();
        resolve_pre_marker_failure(
            repository,
            &project.project_id,
            run_id,
            finished_at,
            &reason,
            policy,
            source,
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn resolve_live_child_failure(
    db: &crate::db::Db,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    child: NativeAgentChild,
    retained_authority: RetainedLaunchAuthority,
    intent: BoundFinalizationIntent,
    source: AppError,
) -> AgentSpawnError {
    let stage = if intent.is_post_marker() {
        AgentSpawnStage::PostMarker {
            run_id,
            resolved: false,
        }
    } else {
        AgentSpawnStage::RunBoundPreMarker {
            run_id,
            resolved: false,
        }
    };
    let policy = policy_from_error(&source);
    let terminated = child.termination_completed();
    let mut cleanup = BoundCleanupHandle {
        project_id: project_id.to_owned(),
        run_id,
        intent,
        kind: BoundCleanupKind::LiveChild {
            child,
            retained_authority,
            terminated,
            finalized: false,
        },
    };

    let termination_uncertain = match &cleanup.kind {
        BoundCleanupKind::LiveChild { child, .. } => child.termination_uncertain(),
        BoundCleanupKind::PendingMarker => false,
    };
    if !termination_uncertain {
        match cleanup.retry(db, finished_at).await {
            Ok(()) => {
                return AgentSpawnError {
                    stage: match stage {
                        AgentSpawnStage::RunBoundPreMarker { run_id, .. } => {
                            AgentSpawnStage::RunBoundPreMarker {
                                run_id,
                                resolved: true,
                            }
                        }
                        AgentSpawnStage::PostMarker { run_id, .. } => AgentSpawnStage::PostMarker {
                            run_id,
                            resolved: true,
                        },
                        AgentSpawnStage::PreBinding => unreachable!("bound cleanup stage"),
                    },
                    source,
                    policy,
                    cleanup: None,
                };
            }
            Err(cleanup_error) => {
                return AgentSpawnError {
                    stage,
                    source: cleanup_error,
                    policy,
                    cleanup: Some(cleanup),
                };
            }
        }
    }

    AgentSpawnError {
        stage,
        source,
        policy,
        cleanup: Some(cleanup),
    }
}

fn resolve_native_spawn_failure(
    repository: &AgentRunRepository<'_>,
    project: &Project,
    run_id: i64,
    finished_at: i64,
    retry_policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    let violation = policy_from_error(&source);
    if let Some(violation) = violation.filter(|violation| {
        matches!(
            violation.stage,
            PolicyViolationStage::PostMarker
                | PolicyViolationStage::Dispatched
                | PolicyViolationStage::Finalized
        )
    }) {
        let cleanup = || BoundCleanupHandle {
            project_id: project.project_id.clone(),
            run_id,
            intent: BoundFinalizationIntent::PendingMarkerPolicy { violation },
            kind: BoundCleanupKind::PendingMarker,
        };
        if let Err(evidence_error) = repository.record_pending_marker_policy_evidence(
            &project.project_id,
            run_id,
            &violation,
        ) {
            return AgentSpawnError {
                stage: AgentSpawnStage::PostMarker {
                    run_id,
                    resolved: false,
                },
                source: evidence_error,
                policy: Some(violation),
                cleanup: Some(cleanup()),
            };
        }
        return match repository.finish_pending_marker_policy_failure(
            &project.project_id,
            run_id,
            finished_at,
            &violation,
        ) {
            Ok(_) => AgentSpawnError {
                stage: AgentSpawnStage::PostMarker {
                    run_id,
                    resolved: true,
                },
                source,
                policy: Some(violation),
                cleanup: None,
            },
            Err(finalizer_error) => AgentSpawnError {
                stage: AgentSpawnStage::PostMarker {
                    run_id,
                    resolved: false,
                },
                source: finalizer_error,
                policy: Some(violation),
                cleanup: Some(cleanup()),
            },
        };
    }
    resolve_bound_failure(
        repository,
        project,
        run_id,
        finished_at,
        retry_policy,
        source,
    )
}

fn resolve_pre_marker_failure(
    repository: &AgentRunRepository<'_>,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    reason: &str,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    let violation = policy_from_error(&source);
    let result = match violation.as_ref() {
        Some(violation) => repository.fail_before_gate_release_with_policy(
            project_id,
            run_id,
            finished_at,
            reason,
            violation,
        ),
        None => repository.fail_before_gate_release_with_policy(
            project_id,
            run_id,
            finished_at,
            reason,
            policy,
        ),
    };
    match result {
        Ok(_) => AgentSpawnError {
            stage: AgentSpawnStage::RunBoundPreMarker {
                run_id,
                resolved: true,
            },
            source,
            policy: violation,
            cleanup: None,
        },
        Err(finalizer_error) => AgentSpawnError {
            stage: AgentSpawnStage::RunBoundPreMarker {
                run_id,
                resolved: false,
            },
            source: finalizer_error,
            policy: violation,
            cleanup: None,
        },
    }
}

fn resolve_post_marker_failure(
    repository: &AgentRunRepository<'_>,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    reason: &str,
    source: AppError,
) -> AgentSpawnError {
    let violation = policy_from_error(&source);
    let result = match violation.as_ref() {
        Some(violation) => repository.finish_after_marker_policy_failure(
            project_id,
            run_id,
            finished_at,
            violation,
        ),
        None => repository.finish_after_marker_failure(project_id, run_id, finished_at, reason),
    };
    match result {
        Ok(_) => AgentSpawnError {
            stage: AgentSpawnStage::PostMarker {
                run_id,
                resolved: true,
            },
            source,
            policy: violation,
            cleanup: None,
        },
        Err(finalizer_error) => AgentSpawnError {
            stage: AgentSpawnStage::PostMarker {
                run_id,
                resolved: false,
            },
            source: finalizer_error,
            policy: violation,
            cleanup: None,
        },
    }
}

impl AgentHandle {
    fn ownership_lost_error() -> AppError {
        AppError::Runtime {
            operation: "native child ownership was lost before process-group reap",
        }
    }

    fn mark_ownership_lost(&mut self) -> AppError {
        self.process_proof = ProcessProof::OwnershipLost;
        Self::ownership_lost_error()
    }

    fn classify_reap_error(&mut self, error: AppError) -> AppError {
        let ownership_lost = self.child.ownership_lost().unwrap_or(false);
        if ownership_lost {
            self.mark_ownership_lost()
        } else {
            error
        }
    }

    fn reject_ownership_lost(&self) -> Result<(), AppError> {
        if self.process_proof == ProcessProof::OwnershipLost {
            return Err(Self::ownership_lost_error());
        }
        Ok(())
    }

    pub(crate) fn cleanup_pending(&self) -> bool {
        matches!(self.terminal_persistence, TerminalPersistence::Persisted(_))
            && matches!(&self.retained_authority, RetainedLaunchAuthority::Retained { .. })
    }

    // Consumed by daemon admission blocking in Task 3.
    #[allow(dead_code)]
    pub(crate) fn cleanup_blocked_project(&self) -> Option<&str> {
        self.cleanup_pending().then_some(self.project_id.as_str())
    }

    fn finalize_terminal_outcome(
        &self,
        db: &crate::db::Db,
        now: i64,
        outcome: &TerminalOutcome,
    ) -> Result<AgentRunStatus, AppError> {
        AgentRunRepository::new(db).finish_and_resolve_events(
            &self.project_id,
            self.run_id,
            outcome.status,
            now,
            outcome.exit_code,
            outcome.last_error.as_deref(),
            EventResolution::RetryPolicy(self.retry_policy),
        )?;
        Ok(outcome.status)
    }

    fn persist_terminal_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        if let TerminalPersistence::Persisted(status) = &self.terminal_persistence {
            return Ok(*status);
        }
        let Some(outcome) = self.terminal_outcome.as_ref() else {
            return Err(AppError::Runtime {
                operation: "finalize missing agent process outcome",
            });
        };
        let status = self.finalize_terminal_outcome(db, now, outcome)?;
        self.terminal_persistence = TerminalPersistence::Persisted(status);
        Ok(status)
    }

    fn retry_terminal_cleanup(
        &mut self,
        deadline: Option<Instant>,
    ) -> Result<AgentRunStatus, AppError> {
        let status = match &self.terminal_persistence {
            TerminalPersistence::Pending => {
                return Err(AppError::Runtime {
                    operation: "cleanup agent process before terminal persistence",
                });
            }
            TerminalPersistence::Persisted(status) => *status,
        };
        if let RetainedLaunchAuthority::Retained { temp, .. } = &mut self.retained_authority {
            temp.cleanup_contents_before(deadline.map(Instant::into_std))
                .map_err(AppError::from)?;
        }
        // Release retained launch authority only after terminal persistence.
        let retained = std::mem::replace(
            &mut self.retained_authority,
            RetainedLaunchAuthority::Released,
        );
        if let RetainedLaunchAuthority::Retained {
            global_policy,
            project_policy,
            temp,
            execution,
        } = retained
        {
            drop((global_policy, project_policy, temp, execution));
        }
        Ok(status)
    }

    fn finalize_stored_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        self.persist_terminal_outcome(db, now)?;
        self.retry_terminal_cleanup(None)
    }

    fn finalize_stored_outcome_before(
        &mut self,
        db: &crate::db::Db,
        now: i64,
        deadline: Instant,
    ) -> Result<AgentRunStatus, AppError> {
        let scoped_db = deadline_scoped_db(db, Some(deadline))?;
        let db = scoped_db.as_ref().expect("deadline-scoped database");
        self.persist_terminal_outcome(db, now)?;
        self.retry_terminal_cleanup(Some(deadline))
    }

    fn store_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
        outcome: TerminalOutcome,
    ) -> Result<AgentRunStatus, AppError> {
        if self.terminal_outcome.is_none() {
            self.terminal_outcome = Some(outcome);
        }
        self.finalize_stored_outcome(db, now)
    }

    fn poll_finalize_stored_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<Option<AgentRunStatus>, AppError> {
        match self.finalize_stored_outcome(db, now) {
            Ok(status) => Ok(Some(status)),
            Err(_error) if self.cleanup_pending() => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn poll_store_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
        outcome: TerminalOutcome,
    ) -> Result<Option<AgentRunStatus>, AppError> {
        match self.store_outcome(db, now, outcome) {
            Ok(status) => Ok(Some(status)),
            Err(_error) if self.cleanup_pending() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub async fn poll(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<Option<AgentRunStatus>, AppError> {
        self.reject_ownership_lost()?;
        if self.terminal_outcome.is_some() {
            return self.poll_finalize_stored_outcome(db, now);
        }

        if Instant::now() >= self.timeout_deadline {
            self.child.terminate().await?;
            return self.poll_store_outcome(
                db,
                now,
                TerminalOutcome {
                    status: AgentRunStatus::TimedOut,
                    exit_code: None,
                    last_error: Some("agent timed out".to_owned()),
                },
            );
        }

        let exit = match self.child.terminal_observed() {
            Ok(TerminalObservation::Terminal) => {
                match self.child.reap_observed_terminal().await {
                    Ok(exit) => exit,
                    Err(error) => return Err(self.classify_reap_error(error)),
                }
            }
            Ok(TerminalObservation::Running) => return Ok(None),
            Ok(TerminalObservation::OwnershipLost) => {
                // ECHILD only proves that this process no longer owns the
                // leader. It does not prove that the process group is
                // drained, so retain the unresolved handle and all of its
                // cleanup authority rather than persisting or signalling.
                return Err(self.mark_ownership_lost());
            }
            Err(source) => {
                // An unknown observation failure is not a terminal outcome.
                // Retain the handle so a later poll can re-establish child
                // ownership; persisting Failed here would drop an executing
                // child without a safe process-group cleanup authority.
                return Err(source);
            }
        };

        let code = exit.code().map(i64::from);
        let status = if exit.success() {
            AgentRunStatus::Completed
        } else {
            AgentRunStatus::Failed
        };
        let last_error = (!exit.success()).then(|| {
            code.map_or_else(
                || "agent exited unsuccessfully".to_owned(),
                |code| format!("agent exited with code {code}"),
            )
        });
        self.poll_store_outcome(
            db,
            now,
            TerminalOutcome {
                status,
                exit_code: code,
                last_error,
            },
        )
    }

    pub async fn wait(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        self.reject_ownership_lost()?;
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome(db, now);
        }

        let remaining = self
            .timeout_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| Duration::from_secs(0));
        match tokio::time::timeout(remaining, self.child.wait_retaining_unknown()).await {
            Ok(Ok(Some(exit))) => {
                let code = exit.code().map(i64::from);
                let status = if exit.success() {
                    AgentRunStatus::Completed
                } else {
                    AgentRunStatus::Failed
                };
                let last_error = (!exit.success()).then(|| {
                    code.map_or_else(
                        || "agent exited unsuccessfully".to_owned(),
                        |code| format!("agent exited with code {code}"),
                    )
                });
                self.store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status,
                        exit_code: code,
                        last_error,
                    },
                )
            }
            Ok(Ok(None)) => Err(self.mark_ownership_lost()),
            Ok(Err(source)) => Err(self.classify_reap_error(source)),
            Err(_) => {
                self.child.terminate().await?;
                self.store_outcome(
                    db,
                    now,
                    TerminalOutcome {
                        status: AgentRunStatus::TimedOut,
                        exit_code: None,
                        last_error: Some("agent timed out".to_owned()),
                    },
                )
            }
        }
    }

    pub async fn timeout_now(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        self.reject_ownership_lost()?;
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome(db, now);
        }

        self.child.terminate().await?;
        self.store_outcome(
            db,
            now,
            TerminalOutcome {
                status: AgentRunStatus::TimedOut,
                exit_code: None,
                last_error: Some("agent timed out".to_owned()),
            },
        )
    }

    pub(crate) async fn timeout_now_before(
        &mut self,
        db: &crate::db::Db,
        now: i64,
        deadline: Instant,
    ) -> Result<AgentRunStatus, AppError> {
        self.reject_ownership_lost()?;
        if self.terminal_outcome.is_some() {
            return self.finalize_stored_outcome_before(db, now, deadline);
        }

        self.child.terminate().await?;
        if self.terminal_outcome.is_none() {
            self.terminal_outcome = Some(TerminalOutcome {
                status: AgentRunStatus::TimedOut,
                exit_code: None,
                last_error: Some("agent timed out".to_owned()),
            });
        }
        self.finalize_stored_outcome_before(db, now, deadline)
    }
}

fn deadline_scoped_db(
    db: &crate::db::Db,
    deadline: Option<Instant>,
) -> Result<Option<crate::db::Db>, AppError> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .ok_or(AppError::Runtime {
            operation: "agent finalization exceeded shutdown deadline",
        })?;
    if remaining.is_zero() {
        return Err(AppError::Runtime {
            operation: "agent finalization exceeded shutdown deadline",
        });
    }
    Ok(Some(db.with_busy_timeout(remaining)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "internal agent-handle subprocess entry"]
    fn timeout_retry_subprocess() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    #[ignore = "internal agent-handle terminal subprocess entry"]
    fn terminal_retry_subprocess() {}

    #[tokio::test]
    async fn timeout_termination_error_retains_db_state_and_same_handle_for_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::TaskFailed,
                "timeout-observation-error",
                serde_json::json!({}),
                1,
                1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(1, 100, 1)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    1,
                    root.join(".pueue-agent/logs/agent-handle.log"),
                ),
                &[event.event_id],
                None,
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 1)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        AgentRunRepository::new(&db)
            .acknowledge_dispatch("project-a", run.run_id)
            .unwrap();
        let (_child_root, child) =
            crate::native_launcher::test_native_child_with_termination_error(
                "agent::tests::timeout_retry_subprocess",
            );
        let mut handle = AgentHandle {
            project_id: "project-a".to_owned(),
            run_id: run.run_id,
            pid: child.id(),
            child,
            retained_authority: RetainedLaunchAuthority::Test,
            timeout_deadline: Instant::now(),
            log_path: root.join(".pueue-agent/logs/agent-handle.log"),
            retry_policy: RetryPolicy { max_retries: 1 },
            terminal_outcome: None,
            terminal_persistence: TerminalPersistence::Pending,
            process_proof: ProcessProof::Owned,
        };

        assert!(handle.timeout_now(&db, 2).await.is_err());
        assert!(handle.terminal_outcome.is_none());
        let state = AgentRunRepository::new(&db)
            .find_active_by_project("project-a")
            .unwrap()
            .unwrap();
        assert_eq!(state.status, AgentRunStatus::Running);
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event.event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::Dispatched,
        );

        assert_eq!(
            handle.timeout_now(&db, 3).await.unwrap(),
            AgentRunStatus::TimedOut,
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ownership_loss_remains_sticky_across_timeout_paths() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::TaskFailed,
                "ownership-loss-sticky",
                serde_json::json!({}),
                1,
                1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(1, 100, 1)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    1,
                    root.join(".pueue-agent/logs/agent-handle.log"),
                ),
                &[event.event_id],
                None,
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 1)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        AgentRunRepository::new(&db)
            .acknowledge_dispatch("project-a", run.run_id)
            .unwrap();
        let (_child_root, child) =
            crate::native_launcher::test_native_child_with_group_signal_error(
                "agent::tests::timeout_retry_subprocess",
            );
        let pid = child.id();
        let mut handle = AgentHandle {
            project_id: "project-a".to_owned(),
            run_id: run.run_id,
            pid,
            child,
            retained_authority: RetainedLaunchAuthority::Test,
            timeout_deadline: Instant::now() + Duration::from_secs(10),
            log_path: root.join(".pueue-agent/logs/agent-handle.log"),
            retry_policy: RetryPolicy { max_retries: 1 },
            terminal_outcome: None,
            terminal_persistence: TerminalPersistence::Pending,
            process_proof: ProcessProof::Owned,
        };

        unsafe extern "C" {
            fn kill(pid: libc::pid_t, signal: libc::c_int) -> libc::c_int;
            fn waitpid(
                pid: libc::pid_t,
                status: *mut libc::c_int,
                options: libc::c_int,
            ) -> libc::pid_t;
        }
        let group = libc::pid_t::try_from(pid).unwrap();
        assert_eq!(unsafe { kill(-group, libc::SIGKILL) }, 0);
        let mut status = 0;
        let mut ownership_disproved = false;
        for _ in 0..200 {
            let waited = unsafe { waitpid(group, &mut status, libc::WNOHANG) };
            if waited == group {
                ownership_disproved = true;
                break;
            }
            if waited < 0 {
                let code = std::io::Error::last_os_error().raw_os_error();
                assert!(matches!(code, Some(libc::ECHILD) | Some(libc::ESRCH)));
                ownership_disproved = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            ownership_disproved,
            "child ownership was not disproved before the deadline"
        );

        let ownership_lost = |error: AppError| {
            assert!(matches!(
                error,
                AppError::Runtime {
                    operation: "native child ownership was lost before process-group reap"
                }
            ));
        };
        ownership_lost(handle.wait(&db, 2).await.unwrap_err());
        ownership_lost(handle.timeout_now(&db, 3).await.unwrap_err());
        ownership_lost(
            handle
                .timeout_now_before(&db, 4, Instant::now() + Duration::from_secs(2))
                .await
                .unwrap_err(),
        );
        assert!(handle.terminal_outcome.is_none());
        assert!(matches!(
            handle.terminal_persistence,
            TerminalPersistence::Pending
        ));
        assert_eq!(
            AgentRunRepository::new(&db)
                .find_active_by_project("project-a")
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Running,
        );
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event.event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::Dispatched,
        );
    }

    #[cfg(unix)]
    fn ownership_loss_between_observation_fixture(
        child_fixture: (tempfile::TempDir, NativeAgentChild),
        dedup_key: &str,
    ) -> (
        tempfile::TempDir,
        tempfile::TempDir,
        crate::db::Db,
        i64,
        AgentHandle,
    ) {
        let (child_temp, child) = child_fixture;
        let project_temp = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&project_temp.path().join("state.sqlite3")).unwrap();
        let root = project_temp.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::TaskFailed,
                dedup_key,
                serde_json::json!({}),
                1,
                1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(1, 100, 1)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    1,
                    root.join(".pueue-agent/logs/agent-handle.log"),
                ),
                &[event.event_id],
                None,
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 1)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        AgentRunRepository::new(&db)
            .acknowledge_dispatch("project-a", run.run_id)
            .unwrap();
        let pid = child.id();
        let handle = AgentHandle {
            project_id: "project-a".to_owned(),
            run_id: run.run_id,
            pid,
            child,
            retained_authority: RetainedLaunchAuthority::Test,
            timeout_deadline: Instant::now() + Duration::from_secs(10),
            log_path: root.join(".pueue-agent/logs/agent-handle.log"),
            retry_policy: RetryPolicy { max_retries: 1 },
            terminal_outcome: None,
            terminal_persistence: TerminalPersistence::Pending,
            process_proof: ProcessProof::Owned,
        };
        (project_temp, child_temp, db, event.event_id, handle)
    }

    #[cfg(unix)]
    async fn reap_fixture_child(pid: i64) {
        unsafe extern "C" {
            fn waitpid(
                pid: libc::pid_t,
                status: *mut libc::c_int,
                options: libc::c_int,
            ) -> libc::pid_t;
        }
        let pid = libc::pid_t::try_from(pid).unwrap();
        let mut status = 0;
        for _ in 0..200 {
            let waited = unsafe { waitpid(pid, &mut status, libc::WNOHANG) };
            if waited == pid {
                return;
            }
            if waited < 0 {
                let code = std::io::Error::last_os_error().raw_os_error();
                assert!(matches!(code, Some(libc::ECHILD) | Some(libc::ESRCH)));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("fixture child was not reaped before the bounded deadline");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn poll_reap_ownership_loss_remains_sticky_across_timeout_paths() {
        let (_project_temp, _child_temp, db, event_id, mut handle) =
            ownership_loss_between_observation_fixture(
                crate::native_launcher::test_native_child_with_ownership_loss_before_reap(
                    "agent::tests::terminal_retry_subprocess",
                ),
                "poll-reap-ownership-loss",
            );
        let error = handle.poll(&db, 2).await.unwrap_err();
        assert!(matches!(
            error,
            AppError::Runtime {
                operation: "native child ownership was lost before process-group reap"
            }
        ));
        assert!(handle.timeout_now(&db, 3).await.is_err());
        assert!(handle
            .timeout_now_before(&db, 4, Instant::now() + Duration::from_secs(2))
            .await
            .is_err());
        assert!(handle.terminal_outcome.is_none());
        assert_eq!(
            AgentRunRepository::new(&db)
                .find_active_by_project("project-a")
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Running,
        );
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::Dispatched,
        );
        let pid = handle.pid;
        drop(handle);
        reap_fixture_child(pid).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wait_reap_ownership_loss_remains_sticky_across_timeout_paths() {
        let (_project_temp, _child_temp, db, event_id, mut handle) =
            ownership_loss_between_observation_fixture(
                crate::native_launcher::test_native_child_with_ownership_loss_before_reap(
                    "agent::tests::terminal_retry_subprocess",
                ),
                "wait-reap-ownership-loss",
            );
        let error = handle.wait(&db, 2).await.unwrap_err();
        assert!(matches!(
            error,
            AppError::Runtime {
                operation: "native child ownership was lost before process-group reap"
            }
        ));
        assert!(handle.timeout_now(&db, 3).await.is_err());
        assert!(handle
            .timeout_now_before(&db, 4, Instant::now() + Duration::from_secs(2))
            .await
            .is_err());
        assert!(handle.terminal_outcome.is_none());
        assert_eq!(
            AgentRunRepository::new(&db)
                .find_active_by_project("project-a")
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Running,
        );
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::Dispatched,
        );
        let pid = handle.pid;
        drop(handle);
        reap_fixture_child(pid).await;
    }

    #[tokio::test]
    async fn bound_cleanup_termination_error_retains_child_and_original_post_marker_intent() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::TaskFailed,
                "bound-cleanup-termination-error",
                serde_json::json!({}),
                1,
                1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(1, 100, 1)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    1,
                    root.join(".pueue-agent/logs/bound-cleanup.log"),
                ),
                &[event.event_id],
                None,
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 1)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        let (_child_root, child) =
            crate::native_launcher::test_native_child_with_termination_error(
                "agent::tests::timeout_retry_subprocess",
            );
        let mut cleanup = BoundCleanupHandle {
            project_id: "project-a".to_owned(),
            run_id: run.run_id,
            intent: BoundFinalizationIntent::PostMarkerExecutionUnknown {
                reason: "original_post_marker_failure".to_owned(),
            },
            kind: BoundCleanupKind::LiveChild {
                child,
                retained_authority: RetainedLaunchAuthority::Test,
                terminated: false,
                finalized: false,
            },
        };

        assert!(cleanup.retry(&db, 2).await.is_err());
        assert!(matches!(
            &cleanup.kind,
            BoundCleanupKind::LiveChild {
                terminated: false,
                ..
            }
        ));
        assert_eq!(
            AgentRunRepository::new(&db)
                .find_active_by_project("project-a")
                .unwrap()
                .unwrap()
                .status,
            AgentRunStatus::Running,
        );
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event.event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::InFlight,
        );

        cleanup.retry(&db, 3).await.unwrap();
        assert!(matches!(
            &cleanup.kind,
            BoundCleanupKind::LiveChild {
                terminated: true,
                ..
            }
        ));
        assert!(AgentRunRepository::new(&db)
            .find_active_by_project("project-a")
            .unwrap()
            .is_none());
        let event = crate::db::EventRepository::new(&db)
            .find_by_id(event.event_id)
            .unwrap()
            .unwrap();
        assert_eq!(event.status, crate::models::EventStatus::DeadLetter);
        assert_eq!(event.last_error.as_deref(), Some("original_post_marker_failure"));
    }

    #[tokio::test]
    async fn terminal_group_cleanup_error_retains_db_state_and_same_handle_for_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::TaskFailed,
                "terminal-cleanup-error",
                serde_json::json!({}),
                1,
                1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(1, 100, 1)
            .unwrap();
        let run = AgentRunRepository::new(&db)
            .insert_with_events_and_reservation(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    1,
                    root.join(".pueue-agent/logs/agent-handle.log"),
                ),
                &[event.event_id],
                None,
            )
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_running_and_apply_interventions("project-a", run.run_id, 4242, 1)
            .unwrap();
        AgentRunRepository::new(&db)
            .mark_gate_release_requested("project-a", run.run_id)
            .unwrap();
        AgentRunRepository::new(&db)
            .acknowledge_dispatch("project-a", run.run_id)
            .unwrap();
        let (_child_root, child) =
            crate::native_launcher::test_native_child_with_group_signal_error(
                "agent::tests::terminal_retry_subprocess",
            );
        let mut handle = AgentHandle {
            project_id: "project-a".to_owned(),
            run_id: run.run_id,
            pid: child.id(),
            child,
            retained_authority: RetainedLaunchAuthority::Test,
            timeout_deadline: Instant::now() + Duration::from_secs(10),
            log_path: root.join(".pueue-agent/logs/agent-handle.log"),
            retry_policy: RetryPolicy { max_retries: 1 },
            terminal_outcome: None,
            terminal_persistence: TerminalPersistence::Pending,
            process_proof: ProcessProof::Owned,
        };

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match handle.poll(&db, 2).await {
                Ok(None) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(_) => break,
                result => panic!("expected terminal cleanup error, got {result:?}"),
            }
        }
        assert!(handle.terminal_outcome.is_none());
        let state = AgentRunRepository::new(&db)
            .find_active_by_project("project-a")
            .unwrap()
            .unwrap();
        assert_eq!(state.status, AgentRunStatus::Running);
        assert_eq!(
            crate::db::EventRepository::new(&db)
                .find_by_id(event.event_id)
                .unwrap()
                .unwrap()
                .status,
            crate::models::EventStatus::Dispatched,
        );

        assert_eq!(
            handle.poll(&db, 3).await.unwrap(),
            Some(AgentRunStatus::Completed),
        );
    }
}
