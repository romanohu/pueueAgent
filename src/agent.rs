use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;
use uuid::Uuid;
use sha2::{Digest, Sha256};

use crate::{
    codex_command::{
        probe_installed_codex_capabilities, CodexArgvBuilder, CodexCapabilities,
    },
    config::{AgentConfig, ProjectConfig},
    db::{
        AgentRunRepository, CampaignRepository, CodeChangeRepository, DecisionRepository,
        DecisionReservation, GateFailurePolicy, NewCodeChangeCheck,
    },
    decision_evidence::DecisionContextBundle,
    decision_protocol::{parse_and_validate_decision, ValidatedDecision},
    environment::{
        PrivateRunTemp, ProjectAdmissionLock, RunIdAdmissionGuard, SanitizedEnvironment,
        TempInventoryReport, VerifiedPrivateTemp,
    },
    execution_policy::{
        resolve_decision_project_policy, resolve_project_policy, AgentKind, CodeChangeTool,
        PolicyViolation, CampaignLimits, PolicyViolationCode, PolicyViolationStage,
        ResolvedExecutionPolicy, ResolvedProjectExecutionPolicy,
    },
    interventions::InterventionReservation,
    health_diagnosis::{parse_and_validate_diagnosis, HEALTH_DIAGNOSIS_SCHEMA},
    models::{
        launch_gate_marker_path, AgentContextMode, AgentRunRole, AgentRunStatus,
        ExecutionProjection, HealthState, NewAgentRun, Project,
    },
    native_launcher::{NativeAgentChild, NativeLaunchSpec, NativeLauncher},
    output::bounded_redacted_text,
    process::TerminalObservation,
    project_logs::{
        inspect_startup_gate_marker, ProjectRootLogReader, StartupGateMarkerInspection,
    },
    retry::{EventResolution, RetryPolicy},
    upgrade::AgentStartUpgradeGuard,
    AppError,
};

const DECISION_OUTPUT_SCHEMA: &[u8] = br#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "decision", "proposal", "reason", "requested_wait_minutes", "expected_evidence", "evidence_ref"],
  "properties": {
    "schema_version": {"const": 1},
    "decision": {"enum": ["proposal", "wait", "goal_reached"]},
    "proposal": {
      "type": ["object", "null"],
      "additionalProperties": false,
      "required": ["kind", "hypothesis", "source_experiment_id", "argv", "working_directory", "expected_evidence"],
      "properties": {
        "kind": {"enum": ["experiment", "repair", "broader_search", "recipe", "code_change", "data_evaluation"]},
        "hypothesis": {"type": "string"},
        "source_experiment_id": {"type": ["string", "null"]},
        "argv": {"type": "array", "items": {"type": "string"}},
        "working_directory": {"type": "string"},
        "expected_evidence": {"type": "array", "items": {"type": "string"}}
      }
    },
    "reason": {"type": ["string", "null"]},
    "requested_wait_minutes": {"type": ["integer", "null"], "minimum": 1},
    "expected_evidence": {
      "type": ["array", "null"],
      "items": {"type": "string"}
    },
    "evidence_ref": {"type": ["string", "null"], "maxLength": 512}
  }
}"#;

const EDITOR_OUTPUT_SCHEMA: &[u8] = br#"{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "status", "summary", "proposed_checks"],
  "properties": {
    "schema_version": {"const": 1},
    "status": {"enum": ["ready", "cannot_apply"]},
    "summary": {"type": "string", "maxLength": 65536},
    "proposed_checks": {"type": "array"}
  }
}"#;

#[cfg(test)]
fn assert_strict_object_schema(schema: &serde_json::Value) {
    let Some(properties) = schema.get("properties") else { return; };
    assert_eq!(
        schema.get("additionalProperties"),
        Some(&serde_json::Value::Bool(false))
    );
    let properties = properties.as_object().unwrap();
    let required = schema["required"].as_array().unwrap();
    assert_eq!(required.len(), properties.len());
    for (name, property) in properties {
        assert!(required.iter().any(|required| required == name));
        assert_strict_object_schema(property);
    }
}

#[derive(Debug, Clone)]
pub struct AgentRunnerConfig {
    codex_capabilities: CodexCapabilities,
    decision_capabilities: DecisionCapabilitySource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecisionCapabilitySource {
    InstalledCli,
    Fixed(CodexCapabilities),
}

impl AgentRunnerConfig {
    pub fn production() -> Self {
        Self {
            codex_capabilities: CodexCapabilities::standard_policy(),
            decision_capabilities: DecisionCapabilitySource::InstalledCli,
        }
    }

    pub fn with_codex_capabilities(mut self, capabilities: CodexCapabilities) -> Self {
        self.codex_capabilities = capabilities;
        self.decision_capabilities = DecisionCapabilitySource::Fixed(capabilities);
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
    role: AgentRunRole,
    decision_persistence: Option<DecisionPersistence>,
    diagnosis_persistence: Option<DiagnosisPersistence>,
    editor_persistence: Option<EditorPersistence>,
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
                repository.record_pending_marker_policy_evidence(
                    project_id,
                    run_id,
                    violation,
                )?;
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
    decision_failure: Option<DecisionFailureContext>,
}

enum BoundCleanupKind {
    LiveChild {
        child: NativeAgentChild,
        retained_authority: RetainedLaunchAuthority,
        terminated: bool,
        finalized: bool,
    },
    RetainedTemp {
        retained_authority: RetainedLaunchAuthority,
        finalized: bool,
    },
    PendingFinalization,
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
                | BoundCleanupKind::RetainedTemp {
                    finalized: true,
                    retained_authority: RetainedLaunchAuthority::Retained { .. },
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
        self.retry_finalization_and_cleanup(db, finished_at, deadline)
    }

    fn retry_finalization_and_cleanup(
        &mut self,
        db: &crate::db::Db,
        finished_at: i64,
        deadline: Option<Instant>,
    ) -> Result<(), AppError> {
        let needs_finalization = matches!(
            &self.kind,
            BoundCleanupKind::LiveChild {
                finalized: false,
                ..
            }
        ) || matches!(
            &self.kind,
            BoundCleanupKind::RetainedTemp {
                finalized: false,
                ..
            }
        ) || matches!(
            &self.kind,
            BoundCleanupKind::PendingFinalization | BoundCleanupKind::PendingMarker
        );
        if needs_finalization {
            let scoped_db = deadline_scoped_db(db, deadline)?;
            let db = scoped_db.as_ref().unwrap_or(db);
            if let Some(decision_failure) = &self.decision_failure {
                decision_failure.persist(db, self.run_id, finished_at)?;
            }
            self.intent.finalize(
                db,
                &self.project_id,
                self.run_id,
                finished_at,
            )?;
            if let BoundCleanupKind::LiveChild { finalized, .. } = &mut self.kind {
                *finalized = true;
            }
            if let BoundCleanupKind::RetainedTemp { finalized, .. } = &mut self.kind {
                *finalized = true;
            }
        }
        match &mut self.kind {
            BoundCleanupKind::LiveChild {
                retained_authority: RetainedLaunchAuthority::Retained { temp, .. },
                ..
            }
            | BoundCleanupKind::RetainedTemp {
                retained_authority: RetainedLaunchAuthority::Retained { temp, .. },
                ..
            } => {
                temp.cleanup_contents_before(deadline.map(Instant::into_std))
                    .map_err(AppError::from)?;
            }
            BoundCleanupKind::LiveChild { .. }
            | BoundCleanupKind::RetainedTemp { .. }
            | BoundCleanupKind::PendingFinalization
            | BoundCleanupKind::PendingMarker => {}
        }
        match &mut self.kind {
            BoundCleanupKind::LiveChild {
                retained_authority,
                ..
            }
            | BoundCleanupKind::RetainedTemp {
                retained_authority,
                ..
            } => *retained_authority = RetainedLaunchAuthority::Released,
            BoundCleanupKind::PendingFinalization | BoundCleanupKind::PendingMarker => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum TerminalPersistence {
    Pending,
    Persisted(AgentRunStatus),
}

enum DecisionPersistence {
    Pending { objective_digest: String },
    Ready(PreparedDecision),
    Persisted,
}

enum DiagnosisPersistence {
    Pending,
    Persisted,
}

enum EditorPersistence {
    Pending {
        code_change_run_id: String,
        attempt: i64,
        session_id: String,
    },
    Persisted,
}

enum PreparedDecision {
    Valid {
        json: String,
        digest: String,
        kind: &'static str,
    },
    Invalid,
}

#[derive(Clone)]
struct DecisionFailureContext {
    cycle_id: String,
    attempt_number: i64,
    limits: CampaignLimits,
}

impl DecisionFailureContext {
    fn persist(&self, db: &crate::db::Db, run_id: i64, now: i64) -> Result<(), AppError> {
        DecisionRepository::new(db).fail_attempt(
            Some(run_id),
            &self.cycle_id,
            self.attempt_number,
            "decision_launch",
            "decision agent launch did not complete",
            self.limits,
            now,
        )?;
        Ok(())
    }
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

    pub fn try_acquire_run_id_admission_guard(
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

    pub fn try_acquire_project_admission_lock(
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
    ) -> Result<StartupGateMarkerEvidence, AppError> {
        let policy = self
            .resolve_project_policy(project, config)
            .map_err(AppError::from)?;
        let verified_root = policy.root_anchor.verify_identity().map_err(AppError::from)?;
        let reader = ProjectRootLogReader::from_verified(verified_root);
        reader.revalidate_root_path_identity()?;
        let mut evidence = StartupGateMarkerEvidence::default();
        for (run_id, _gate_state, stored_log_path) in candidates {
            let relative_log = recovery_relative_log_path(&policy, stored_log_path)?;
            let relative_marker = launch_gate_marker_path(&relative_log);
            match inspect_startup_gate_marker(&reader, &relative_marker)? {
                StartupGateMarkerInspection::Absent => {}
                StartupGateMarkerInspection::Valid => {
                    evidence.confirmed.insert(*run_id);
                }
                StartupGateMarkerInspection::Indeterminate => {
                    evidence.indeterminate.insert(*run_id);
                }
            }
        }
        reader.revalidate_root_path_identity()?;
        Ok(evidence)
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

    pub(crate) fn command_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &VerifiedPrivateTemp,
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
                    .build_with_private_temp(config, prompt, private_tmp)
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

    fn diagnosis_command_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &VerifiedPrivateTemp,
        capabilities: CodexCapabilities,
    ) -> Result<AgentCommand, AppError> {
        let program = policy
            .agent_anchor
            .canonical_path
            .to_str()
            .ok_or(AppError::Configuration {
                field: "agent.program",
            })?
            .to_owned();
        let args = CodexArgvBuilder::new(policy.clone(), capabilities)
            .build_health_diagnosis_with_private_temp(config, prompt, private_tmp)
            .map_err(AppError::from)?
            .into_iter()
            .map(|argument| {
                argument
                    .into_string()
                    .map_err(|_| AppError::Configuration { field: "agent.args" })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(AgentCommand { program, args })
    }

    fn decision_command_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        prompt: &str,
        private_tmp: &VerifiedPrivateTemp,
        capabilities: CodexCapabilities,
    ) -> Result<AgentCommand, AppError> {
        let program = policy
            .agent_anchor
            .canonical_path
            .to_str()
            .ok_or(AppError::Configuration {
                field: "agent.program",
            })?
            .to_owned();
        let args = CodexArgvBuilder::new(policy.clone(), capabilities)
            .build_decision_with_private_temp(config, prompt, private_tmp)
            .map_err(AppError::from)?
            .into_iter()
            .map(|argument| {
                argument
                    .into_string()
                    .map_err(|_| AppError::Configuration { field: "agent.args" })
            })
            .collect::<Result<Vec<_>, _>>()?;
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

    fn decision_environment_for(
        &self,
        policy: &ResolvedProjectExecutionPolicy,
        run_id: i64,
    ) -> Result<SanitizedEnvironment, PolicyViolation> {
        SanitizedEnvironment::for_codex_decision(
            &self.policy.startup_environment,
            policy,
            run_id,
        )
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
        self.spawn_with_role(
            db,
            project,
            project_policy,
            config,
            retry_policy,
            primary_event_id,
            event_ids,
            reservation,
            None,
            AgentRunRole::Standard,
            None,
            None,
            prompt,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_decision(
        &self,
        db: &crate::db::Db,
        project: &Project,
        project_policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        reservation: &DecisionReservation,
        context: &DecisionContextBundle,
        now: i64,
        run_id_guard: RunIdAdmissionGuard,
        project_lock: ProjectAdmissionLock,
    ) -> Result<AgentHandle, AgentSpawnError> {
        validate_project_decision_authority(project, project_policy)
            .map_err(|error| pre_binding_error(error.into()))?;
        let objective_digest = DecisionRepository::new(db)
            .validate_launch_context(
                &project.project_id,
                reservation,
                &context.json,
                &context.digest,
            )
            .map_err(pre_binding_error)?;
        let decision_policy = resolve_decision_project_policy(&self.policy, project_policy)
            .map_err(|error| pre_binding_error(error.into()))?;
        let decision_capabilities = match self.config.decision_capabilities {
            DecisionCapabilitySource::InstalledCli => {
                probe_installed_codex_capabilities(&decision_policy.agent_anchor)
                    .await
                    .map_err(|error| pre_binding_error(error.into()))?
            }
            DecisionCapabilitySource::Fixed(capabilities) => capabilities,
        };
        let prompt = decision_launch_prompt(context);
        self.spawn_with_role(
            db,
            project,
            &decision_policy,
            config,
            retry_policy,
            primary_event_id,
            event_ids,
            None,
            Some(reservation),
            AgentRunRole::Decision {
                cycle_id: reservation.cycle_id.clone(),
                attempt_number: reservation.attempt_number,
            },
            Some(objective_digest),
            Some(decision_capabilities),
            &prompt,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    }

    /// Launch one bounded, read-only diagnosis run for a suspicious
    /// running-health row.  The caller owns the already-claimed event and the
    /// health-row state transition to `Diagnosing`.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_diagnosis(
        &self,
        db: &crate::db::Db,
        project: &Project,
        project_policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        experiment_id: &str,
        evidence_json: &str,
        now: i64,
        run_id_guard: RunIdAdmissionGuard,
        project_lock: ProjectAdmissionLock,
    ) -> Result<AgentHandle, AgentSpawnError> {
        validate_project_decision_authority(project, project_policy)
            .map_err(|error| pre_binding_error(error.into()))?;
        let decision_policy = resolve_decision_project_policy(&self.policy, project_policy)
            .map_err(|error| pre_binding_error(error.into()))?;
        let decision_capabilities = match self.config.decision_capabilities {
            DecisionCapabilitySource::InstalledCli => {
                probe_installed_codex_capabilities(&decision_policy.agent_anchor)
                    .await
                    .map_err(|error| pre_binding_error(error.into()))?
            }
            DecisionCapabilitySource::Fixed(capabilities) => capabilities,
        };
        let prompt = diagnosis_launch_prompt(evidence_json);
        self.spawn_with_role(
            db,
            project,
            &decision_policy,
            config,
            retry_policy,
            primary_event_id,
            event_ids,
            None,
            None,
            AgentRunRole::Diagnosis {
                experiment_id: experiment_id.to_owned(),
            },
            None,
            Some(decision_capabilities),
            &prompt,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    }

    /// Launch an editor against a candidate worktree whose policy was
    /// produced by `ResolvedExecutionPolicy::for_code_change_worktree`.
    /// Attempt/session binding is completed by `spawn_with_role` before any
    /// native launch gate is released.
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_code_change_editor(
        &self,
        db: &crate::db::Db,
        project: &Project,
        candidate_policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        code_change_run_id: &str,
        attempt: i64,
        prompt: &str,
        now: i64,
        run_id_guard: RunIdAdmissionGuard,
        project_lock: ProjectAdmissionLock,
    ) -> Result<AgentHandle, AgentSpawnError> {
        let code_change = CodeChangeRepository::new(db)
            .find_by_id(code_change_run_id)
            .map_err(pre_binding_error)?
            .ok_or_else(|| {
                pre_binding_error(AppError::Validation {
                    field: "code_change_run_id",
                    message: "must identify an existing code-change run",
                })
            })?;
        let campaign = CampaignRepository::new(db)
            .find_by_id(&code_change.campaign_id)
            .map_err(pre_binding_error)?
            .ok_or_else(|| {
                pre_binding_error(AppError::Validation {
                    field: "code_change.campaign_id",
                    message: "must identify an existing code-change campaign",
                })
            })?;
        let expected_root = self
            .policy
            .code_change_state_root_path()
            .join("worktrees")
            .join(&code_change.campaign_id)
            .join(&code_change.proposal_id);
        if campaign.project_id != project.project_id
            || candidate_policy.project_id != project.project_id
            || code_change.state != crate::models::CodeChangeState::Editing
            || candidate_policy.root_anchor.canonical_path != expected_root
        {
            return Err(pre_binding_error(AppError::from(PolicyViolation::new(
                PolicyViolationCode::RootChanged,
                PolicyViolationStage::PreBinding,
            ))));
        }
        candidate_policy
            .root_anchor
            .verify_identity()
            .map_err(|mut violation| {
                violation.stage = PolicyViolationStage::PreBinding;
                pre_binding_error(violation.into())
            })?;
        let editor_capabilities = match candidate_policy.agent_kind {
            AgentKind::BuiltInCodex => Some(
                probe_installed_codex_capabilities(&candidate_policy.agent_anchor)
                    .await
                    .map_err(|error| pre_binding_error(error.into()))?,
            ),
            AgentKind::Custom => None,
        };
        self.spawn_with_role(
            db,
            project,
            candidate_policy,
            config,
            retry_policy,
            primary_event_id,
            event_ids,
            None,
            None,
            AgentRunRole::CodeChangeEditor {
                code_change_run_id: code_change_run_id.to_owned(),
                attempt,
            },
            None,
            editor_capabilities,
            prompt,
            now,
            run_id_guard,
            project_lock,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn spawn_with_role(
        &self,
        db: &crate::db::Db,
        project: &Project,
        project_policy: &ResolvedProjectExecutionPolicy,
        config: &AgentConfig,
        retry_policy: RetryPolicy,
        primary_event_id: i64,
        event_ids: &[i64],
        reservation: Option<&InterventionReservation>,
        decision_reservation: Option<&DecisionReservation>,
        role: AgentRunRole,
        objective_digest: Option<String>,
        decision_capabilities: Option<CodexCapabilities>,
        prompt: &str,
        now: i64,
        run_id_guard: RunIdAdmissionGuard,
        project_lock: ProjectAdmissionLock,
    ) -> Result<AgentHandle, AgentSpawnError> {
        let agent_start_guard = AgentStartUpgradeGuard::acquire(db).map_err(pre_binding_error)?;
        match &role {
            AgentRunRole::Standard => self.preflight_project_launch(project_policy, config, prompt),
            AgentRunRole::Decision { .. } | AgentRunRole::Diagnosis { .. } => {
                CodexArgvBuilder::new(
                    project_policy.clone(),
                    decision_capabilities.expect("decision launch capabilities were resolved"),
                )
                    .preflight_decision(config, prompt)
            }
            AgentRunRole::CodeChangeEditor { .. } => match project_policy.agent_kind {
                AgentKind::BuiltInCodex => CodexArgvBuilder::new(
                    project_policy.clone(),
                    decision_capabilities.unwrap_or(self.config.codex_capabilities),
                )
                .preflight_editor(config, prompt),
                AgentKind::Custom
                    if !prompt.contains('\0')
                        && config.args.iter().all(|argument| !argument.contains('\0'))
                        && !matches!(config.context, AgentContextMode::ResumeLatest) => Ok(()),
                AgentKind::Custom => Err(PolicyViolation::new(
                    crate::execution_policy::PolicyViolationCode::UnsafeCodexArgument,
                    PolicyViolationStage::PreBinding,
                )),
            },
        }
        .map_err(|error| pre_binding_error(error.into()))?;
        let relative_log_path = relative_log_path(primary_event_id, now);
        let relative_marker_path = launch_gate_marker_path(&relative_log_path);
        let log_path = project_policy
            .root_anchor
            .canonical_path
            .join(&relative_log_path);
        let execution = Self::execution_projection(project_policy).map_err(pre_binding_error)?;
        let execution = match &role {
            AgentRunRole::Diagnosis { .. } => ExecutionProjection::new(
                "diagnosis",
                execution.executable_path(),
                execution.executable_identity(),
            )
            .map_err(pre_binding_error)?,
            AgentRunRole::CodeChangeEditor { .. } => ExecutionProjection::new(
                "code_change_editor",
                execution.executable_path(),
                execution.executable_identity(),
            )
            .map_err(pre_binding_error)?,
            _ => execution,
        };
        let editor_session = match &role {
            AgentRunRole::CodeChangeEditor {
                code_change_run_id,
                attempt,
            } => {
                let code_change = CodeChangeRepository::new(db)
                    .find_by_id(code_change_run_id)
                    .map_err(pre_binding_error)?
                    .ok_or_else(|| {
                        pre_binding_error(AppError::Validation {
                            field: "code_change_run_id",
                            message: "must identify an existing code-change run",
                        })
                    })?;
                if !(1..=2).contains(attempt) {
                    return Err(pre_binding_error(AppError::Validation {
                        field: "code_change.attempt",
                        message: "must be one of the two bounded editor attempts",
                    }));
                }
                if *attempt == 1 && code_change.editor_attempts == 0 {
                    Some(
                        code_change
                            .editor_session_id
                            .unwrap_or_else(|| Uuid::new_v4().to_string()),
                    )
                } else if *attempt == code_change.editor_attempts + 1 {
                    Some(code_change.editor_session_id.ok_or_else(|| {
                        pre_binding_error(AppError::Validation {
                            field: "code_change.editor_session_id",
                            message: "resume attempt requires a persisted editor session",
                        })
                    })?)
                } else {
                    return Err(pre_binding_error(AppError::Validation {
                        field: "code_change.attempt",
                        message: "must launch only the next bounded editor attempt",
                    }));
                }
            }
            _ => None,
        };
        let repository = AgentRunRepository::new(db);
        let run_context = match &role {
            AgentRunRole::Standard | AgentRunRole::CodeChangeEditor { .. } => config.context.clone(),
            AgentRunRole::Decision { .. } | AgentRunRole::Diagnosis { .. } => {
                AgentContextMode::Fresh
            }
        };
        if let AgentRunRole::CodeChangeEditor { attempt, .. } = &role {
            let context_matches_attempt = match (*attempt, &run_context, editor_session.as_deref()) {
                (1, AgentContextMode::Fresh, Some(_)) => true,
                (2, AgentContextMode::Resume { session_id }, Some(expected)) => {
                    session_id == expected
                }
                _ => false,
            };
            if !context_matches_attempt {
                return Err(pre_binding_error(AppError::Validation {
                    field: "code_change.editor_context",
                    message: "editor attempt context must be fresh for attempt one or the exact persisted session for attempt two",
                }));
            }
        }
        let run = repository
            .insert_with_events_and_reservation_with_guard(
                &NewAgentRun::with_context(
                    &project.project_id,
                    primary_event_id,
                    None,
                    AgentRunStatus::Starting,
                    now,
                    &log_path,
                    run_context.clone(),
                    run_context.session_id().map(str::to_owned),
                    event_ids.iter().map(i64::to_string).collect(),
                )
                .with_execution(execution.clone()),
                event_ids,
                reservation.map(|reservation| reservation.token.as_str()),
                &run_id_guard,
            )
            .map_err(pre_binding_error)?;
        if let (AgentRunRole::CodeChangeEditor { code_change_run_id, attempt }, Some(session_id)) =
            (&role, editor_session.as_deref())
        {
            if let Err(error) = CodeChangeRepository::new(db).reserve_editor_attempt(
                code_change_run_id,
                *attempt,
                run.run_id,
                session_id,
                now,
            ) {
                return Err(resolve_bound_role_failure(
                    db,
                    None,
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error,
                ));
            }
        }
        if let Some(decision_reservation) = decision_reservation {
            if let Err(error) =
                DecisionRepository::new(db).bind_agent_run(decision_reservation, run.run_id, now)
            {
                return Err(resolve_unowned_decision_bind_failure(
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error,
                ));
            }
        }
        let decision_failure = decision_reservation.map(|reservation| DecisionFailureContext {
            cycle_id: reservation.cycle_id.clone(),
            attempt_number: reservation.attempt_number,
            limits: self.policy.campaign_limits,
        });
        drop(agent_start_guard);
        let verified_root = project_policy
            .root_anchor
            .verify_identity()
            .map_err(|error| {
                resolve_bound_role_failure(
                    db,
                    decision_failure.as_ref(),
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
                resolve_bound_role_failure(
                    db,
                    decision_failure.as_ref(),
                    &repository,
                    project,
                    run.run_id,
                    now,
                    retry_policy,
                    error.into(),
                )
            })?;
        if matches!(&role, AgentRunRole::Decision { .. }) {
            if let Err(error) = temp.prepare_decision_schema(DECISION_OUTPUT_SCHEMA) {
                let error = AppError::from(error);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        }
        if matches!(&role, AgentRunRole::Diagnosis { .. }) {
            if let Err(error) = temp.prepare_health_diagnosis_schema(HEALTH_DIAGNOSIS_SCHEMA) {
                let error = AppError::from(error);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        }
        if matches!(&role, AgentRunRole::CodeChangeEditor { .. }) {
            if let Err(error) = temp.prepare_editor_schema(EDITOR_OUTPUT_SCHEMA) {
                let error = AppError::from(error);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        }
        let private_temp_target = match temp.verified_target() {
            Ok(target) => target,
            Err(error) => {
                let error = AppError::from(error);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        };
        drop(run_id_guard);
        drop(project_lock);
        let command = match match &role {
            AgentRunRole::Standard => {
                self.command_for(project_policy, config, prompt, &private_temp_target)
            }
            AgentRunRole::Decision { .. } => self.decision_command_for(
                project_policy,
                config,
                prompt,
                &private_temp_target,
                decision_capabilities.expect("decision launch capabilities were resolved"),
            ),
            AgentRunRole::Diagnosis { .. } => self.diagnosis_command_for(
                project_policy,
                config,
                prompt,
                &private_temp_target,
                decision_capabilities.expect("decision launch capabilities were resolved"),
            ),
            AgentRunRole::CodeChangeEditor { .. } => {
                match project_policy.agent_kind {
                    AgentKind::BuiltInCodex => CodexArgvBuilder::new(
                        project_policy.clone(),
                        decision_capabilities.unwrap_or(self.config.codex_capabilities),
                    )
                    .build_editor_with_private_temp(config, prompt, &private_temp_target)
                    .map_err(AppError::from)
                    .and_then(|arguments| {
                        let program = project_policy
                            .agent_anchor
                            .canonical_path
                            .to_string_lossy()
                            .into_owned();
                        let args = arguments
                            .into_iter()
                            .map(|argument| {
                                argument.into_string().map_err(|_| {
                                    AppError::Configuration {
                                        field: "agent.args",
                                    }
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(AgentCommand { program, args })
                    }),
                    AgentKind::Custom => {
                        let program = project_policy
                            .agent_anchor
                            .canonical_path
                            .to_str()
                            .ok_or(AppError::Configuration {
                                field: "agent.program",
                            })
                            .map_err(pre_binding_error)?
                            .to_owned();
                        Ok(AgentCommand {
                            program,
                            args: config
                                .args
                                .iter()
                                .map(|argument| argument.replace("{prompt}", prompt))
                                .collect(),
                        })
                    }
                }
            }
        } {
            Ok(command) => command,
            Err(error) => {
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        };
        let mut environment = match match &role {
            AgentRunRole::Standard => self.environment_for(project_policy, run.run_id),
            AgentRunRole::Decision { .. } | AgentRunRole::Diagnosis { .. } => {
                self.decision_environment_for(project_policy, run.run_id)
            }
            AgentRunRole::CodeChangeEditor { .. } => {
                let session_id = editor_session.as_deref().ok_or(PolicyViolation::new(
                    PolicyViolationCode::SessionMissing,
                    PolicyViolationStage::RunBoundPreMarker,
                ));
                session_id.and_then(|session_id| {
                    SanitizedEnvironment::for_code_change_editor(
                        &self.policy.startup_environment,
                        project_policy,
                        run.run_id,
                        session_id,
                        matches!(&run_context, AgentContextMode::Resume { .. }),
                    )
                })
            }
        } {
            Ok(environment) => environment,
            Err(error) => {
                let error = AppError::from(error);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        };
        let campaign_lineage = match campaign_experiment_lineage(db, primary_event_id) {
            Ok(lineage) => lineage,
            Err(error) => {
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    BoundFinalizationIntent::from_failure(&error, retry_policy),
                    error,
                ));
            }
        };
        if let Some((campaign_id, experiment_id)) = campaign_lineage {
            environment.apply_campaign_experiment_variables(
                &crate::environment::campaign_experiment_task_environment(
                    &project_policy.root_anchor.canonical_path,
                    &campaign_id,
                    &experiment_id,
                ),
            );
        }
        let mut argv = Vec::with_capacity(command.args.len() + 1);
        argv.push(OsString::from(&command.program));
        argv.extend(command.args.into_iter().map(OsString::from));
        let mut child = match NativeLauncher::spawn_verified(
            NativeLaunchSpec {
                launcher: self.policy.launcher_anchor.clone(),
                executable: project_policy.agent_anchor.clone(),
                argv,
                cwd: Some(project_policy.root_anchor.canonical_path.clone()),
                environment,
                project_root: verified_root,
                relative_log_path,
                relative_marker_path,
            },
            private_temp_target,
        ) {
            Ok(child) => child,
            Err(error) => {
                let intent = native_spawn_finalization_intent(&error, retry_policy);
                return Err(resolve_retained_temp_failure(
                    db,
                    project,
                    run.run_id,
                    now,
                    RetainedLaunchAuthority::Retained {
                        global_policy: self.policy.clone(),
                        project_policy: project_policy.clone(),
                        temp,
                        execution,
                    },
                    decision_failure,
                    intent,
                    error,
                ));
            }
        };
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
                decision_failure.clone(),
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
                decision_failure.clone(),
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
                decision_failure.clone(),
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
                decision_failure.clone(),
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
                decision_failure.clone(),
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
            decision_persistence: objective_digest.map(|objective_digest| {
                DecisionPersistence::Pending { objective_digest }
            }),
            diagnosis_persistence: match &role {
                AgentRunRole::Diagnosis { .. } => Some(DiagnosisPersistence::Pending),
                _ => None,
            },
            editor_persistence: match (&role, editor_session) {
                (
                    AgentRunRole::CodeChangeEditor {
                        code_change_run_id,
                        attempt,
                    },
                    Some(session_id),
                ) => Some(EditorPersistence::Pending {
                    code_change_run_id: code_change_run_id.clone(),
                    attempt: *attempt,
                    session_id,
                }),
                _ => None,
            },
            role,
        })
    }
}

fn validate_project_decision_authority(
    project: &Project,
    policy: &ResolvedProjectExecutionPolicy,
) -> Result<(), PolicyViolation> {
    if project.project_id != policy.project_id
        || project.root_path != policy.root_anchor.canonical_path
    {
        return Err(PolicyViolation::new(
            PolicyViolationCode::RootChanged,
            PolicyViolationStage::PreBinding,
        ));
    }
    policy
        .root_anchor
        .verify_identity()
        .map(|_| ())
        .map_err(|mut violation| {
            violation.stage = PolicyViolationStage::PreBinding;
            violation
        })
}

#[derive(Default)]
pub(crate) struct StartupGateMarkerEvidence {
    pub(crate) confirmed: BTreeSet<i64>,
    pub(crate) indeterminate: BTreeSet<i64>,
}

pub fn relative_log_path(primary_event_id: i64, now: i64) -> PathBuf {
    PathBuf::from(format!(
        ".pueue-agent/logs/agent-{now}-{primary_event_id}.log"
    ))
}

fn decision_launch_prompt(context: &DecisionContextBundle) -> String {
    format!(
        "Analyze this supervisor-owned campaign context without modifying the project. Return exactly one JSON decision matching the supplied schema.\n{}",
        context.json
    )
}

fn diagnosis_launch_prompt(evidence_json: &str) -> String {
    format!(
        "Diagnose this running experiment's health signals without modifying anything. Return exactly one JSON diagnosis matching the supplied schema.\n{}",
        evidence_json
    )
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

/// Campaign-experiment lineage of the triggering event, when the run manages
/// one campaign experiment. Drives the result/artifact env injection.
fn campaign_experiment_lineage(
    db: &crate::db::Db,
    event_id: i64,
) -> Result<Option<(String, String)>, AppError> {
    use rusqlite::OptionalExtension;
    let connection = db.connect()?;
    connection
        .query_row(
            "SELECT campaign_id, experiment_id FROM events
             WHERE event_id = ?1
               AND campaign_id IS NOT NULL AND experiment_id IS NOT NULL",
            [event_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|source| AppError::Database {
            operation: "read campaign experiment event lineage",
            source,
        })
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

fn resolve_unowned_decision_bind_failure(
    repository: &AgentRunRepository<'_>,
    project: &Project,
    run_id: i64,
    finished_at: i64,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    // A failed bind never transferred decision-attempt ownership to this run.
    let intent = BoundFinalizationIntent::from_failure(&source, policy);
    let mut error = resolve_bound_failure(
        repository,
        project,
        run_id,
        finished_at,
        policy,
        source,
    );
    if matches!(
        error.stage,
        AgentSpawnStage::RunBoundPreMarker {
            resolved: false,
            ..
        } | AgentSpawnStage::PostMarker {
            resolved: false,
            ..
        }
    ) {
        error.cleanup = Some(BoundCleanupHandle {
            project_id: project.project_id.clone(),
            run_id,
            intent,
            kind: BoundCleanupKind::PendingFinalization,
            decision_failure: None,
        });
    }
    error
}

fn pending_decision_finalization_error(
    project: &Project,
    run_id: i64,
    decision_failure: DecisionFailureContext,
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
    AgentSpawnError {
        stage,
        policy: policy_from_error(&source),
        source,
        cleanup: Some(BoundCleanupHandle {
            project_id: project.project_id.clone(),
            run_id,
            intent,
            decision_failure: Some(decision_failure),
            kind: BoundCleanupKind::PendingMarker,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_retained_temp_failure(
    db: &crate::db::Db,
    project: &Project,
    run_id: i64,
    finished_at: i64,
    retained_authority: RetainedLaunchAuthority,
    decision_failure: Option<DecisionFailureContext>,
    intent: BoundFinalizationIntent,
    source: AppError,
) -> AgentSpawnError {
    let post_marker = intent.is_post_marker();
    let policy = policy_from_error(&source);
    let mut cleanup = BoundCleanupHandle {
        project_id: project.project_id.clone(),
        run_id,
        intent,
        decision_failure,
        kind: BoundCleanupKind::RetainedTemp {
            retained_authority,
            finalized: false,
        },
    };
    match cleanup.retry_finalization_and_cleanup(db, finished_at, None) {
        Ok(()) => AgentSpawnError {
            stage: if post_marker {
                AgentSpawnStage::PostMarker {
                    run_id,
                    resolved: true,
                }
            } else {
                AgentSpawnStage::RunBoundPreMarker {
                    run_id,
                    resolved: true,
                }
            },
            source,
            policy,
            cleanup: None,
        },
        Err(error) => AgentSpawnError {
            stage: if post_marker {
                AgentSpawnStage::PostMarker {
                    run_id,
                    resolved: false,
                }
            } else {
                AgentSpawnStage::RunBoundPreMarker {
                    run_id,
                    resolved: false,
                }
            },
            source: error,
            policy,
            cleanup: Some(cleanup),
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_bound_role_failure(
    db: &crate::db::Db,
    decision_failure: Option<&DecisionFailureContext>,
    repository: &AgentRunRepository<'_>,
    project: &Project,
    run_id: i64,
    finished_at: i64,
    policy: RetryPolicy,
    source: AppError,
) -> AgentSpawnError {
    if let Some(decision_failure) = decision_failure {
        if let Err(error) = decision_failure.persist(db, run_id, finished_at) {
            return pending_decision_finalization_error(
                project,
                run_id,
                decision_failure.clone(),
                BoundFinalizationIntent::from_failure(&source, policy),
                error,
            );
        }
    }
    resolve_bound_failure(
        repository,
        project,
        run_id,
        finished_at,
        policy,
        source,
    )
}

fn native_spawn_finalization_intent(
    source: &AppError,
    retry_policy: RetryPolicy,
) -> BoundFinalizationIntent {
    policy_from_error(source)
        .filter(|violation| {
            matches!(
                violation.stage,
                PolicyViolationStage::PostMarker
                    | PolicyViolationStage::Dispatched
                    | PolicyViolationStage::Finalized
            )
        })
        .map_or_else(
            || BoundFinalizationIntent::from_failure(source, retry_policy),
            |violation| BoundFinalizationIntent::PendingMarkerPolicy { violation },
        )
}

#[allow(clippy::too_many_arguments)]
async fn resolve_live_child_failure(
    db: &crate::db::Db,
    project_id: &str,
    run_id: i64,
    finished_at: i64,
    child: NativeAgentChild,
    retained_authority: RetainedLaunchAuthority,
    decision_failure: Option<DecisionFailureContext>,
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
        decision_failure,
        kind: BoundCleanupKind::LiveChild {
            child,
            retained_authority,
            terminated,
            finalized: false,
        },
    };

    let termination_uncertain = match &cleanup.kind {
        BoundCleanupKind::LiveChild { child, .. } => child.termination_uncertain(),
        BoundCleanupKind::RetainedTemp { .. }
        | BoundCleanupKind::PendingFinalization
        | BoundCleanupKind::PendingMarker => false,
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

    fn prepare_decision_outcome(&mut self) -> Result<(), AppError> {
        let objective_digest = match self.decision_persistence.as_ref() {
            Some(DecisionPersistence::Pending { objective_digest }) => objective_digest.clone(),
            Some(DecisionPersistence::Ready(_) | DecisionPersistence::Persisted) | None => {
                return Ok(())
            }
        };
        if !self
            .terminal_outcome
            .as_ref()
            .is_some_and(|outcome| outcome.status == AgentRunStatus::Completed)
        {
            self.decision_persistence =
                Some(DecisionPersistence::Ready(PreparedDecision::Invalid));
            return Ok(());
        }
        let (temp, limits) = match &self.retained_authority {
            RetainedLaunchAuthority::Retained {
                global_policy,
                temp,
                ..
            } => (temp, global_policy.campaign_limits),
            RetainedLaunchAuthority::Released => {
                return Err(AppError::Runtime {
                    operation: "read decision output after releasing private temp",
                })
            }
            #[cfg(test)]
            RetainedLaunchAuthority::Test => {
                return Err(AppError::Runtime {
                    operation: "test decision handle has no private temp",
                })
            }
        };
        let prepared = match temp.read_decision_output() {
            Ok(bytes) => match parse_and_validate_decision(&bytes, &objective_digest, limits) {
                Ok(decision) => PreparedDecision::Valid {
                    json: String::from_utf8(bytes).expect("validated JSON is UTF-8"),
                    digest: decision.canonical_digest().to_owned(),
                    kind: match decision {
                        ValidatedDecision::Proposal(_) => "proposal",
                        ValidatedDecision::Wait(_) => "wait",
                        ValidatedDecision::GoalReached(_) => "goal_reached",
                    },
                },
                Err(_) => PreparedDecision::Invalid,
            },
            Err(_) => PreparedDecision::Invalid,
        };
        self.decision_persistence = Some(DecisionPersistence::Ready(prepared));
        Ok(())
    }

    fn persist_decision_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<(), AppError> {
        let (cycle_id, attempt_number) = match &self.role {
            AgentRunRole::Standard => return Ok(()),
            AgentRunRole::Decision {
                cycle_id,
                attempt_number,
            } => (cycle_id.clone(), *attempt_number),
            AgentRunRole::Diagnosis { .. } | AgentRunRole::CodeChangeEditor { .. } => {
                return Ok(())
            }
        };
        if matches!(self.decision_persistence, Some(DecisionPersistence::Persisted)) {
            return Ok(());
        }
        self.prepare_decision_outcome()?;
        let limits = match &self.retained_authority {
            RetainedLaunchAuthority::Retained { global_policy, .. } => {
                global_policy.campaign_limits
            }
            RetainedLaunchAuthority::Released => {
                return Err(AppError::Runtime {
                    operation: "persist decision after releasing private temp",
                })
            }
            #[cfg(test)]
            RetainedLaunchAuthority::Test => {
                return Err(AppError::Runtime {
                    operation: "test decision handle has no persistence authority",
                })
            }
        };
        let repository = DecisionRepository::new(db);
        match self.decision_persistence.as_ref() {
            Some(DecisionPersistence::Ready(PreparedDecision::Valid {
                json,
                digest,
                kind,
            })) => {
                repository.store_decision(self.run_id, json, digest, kind, now)?;
            }
            Some(DecisionPersistence::Ready(PreparedDecision::Invalid)) => {
                repository.fail_attempt(
                    Some(self.run_id),
                    &cycle_id,
                    attempt_number,
                    "decision_missing",
                    "decision output was missing or failed secure validation",
                    limits,
                    now,
                )?;
            }
            Some(DecisionPersistence::Persisted) => return Ok(()),
            Some(DecisionPersistence::Pending { .. }) | None => {
                return Err(AppError::Runtime {
                    operation: "prepare decision outcome before persistence",
                })
            }
        }
        self.decision_persistence = Some(DecisionPersistence::Persisted);
        Ok(())
    }

    /// Persist the diagnosis outcome of a `Diagnosis` run onto its
    /// running-health row before terminal cleanup releases the private temp.
    fn persist_diagnosis_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<(), AppError> {
        let experiment_id = match &self.role {
            AgentRunRole::Diagnosis { experiment_id } => experiment_id.clone(),
            _ => return Ok(()),
        };
        if matches!(
            self.diagnosis_persistence,
            Some(DiagnosisPersistence::Persisted)
        ) {
            return Ok(());
        }
        let validated = if !self
            .terminal_outcome
            .as_ref()
            .is_some_and(|outcome| outcome.status == AgentRunStatus::Completed)
        {
            None
        } else {
            let temp = match &self.retained_authority {
                RetainedLaunchAuthority::Retained { temp, .. } => temp,
                RetainedLaunchAuthority::Released => {
                    return Err(AppError::Runtime {
                        operation: "read diagnosis output after releasing private temp",
                    })
                }
                #[cfg(test)]
                RetainedLaunchAuthority::Test => {
                    return Err(AppError::Runtime {
                        operation: "test diagnosis handle has no private temp",
                    })
                }
            };
            let parsed = match temp.read_health_diagnosis_output() {
                Ok(bytes) => parse_and_validate_diagnosis(&bytes).ok(),
                Err(_) => None,
            };
            parsed.map(|diagnosis| {
                serde_json::to_value(&diagnosis)
                    .expect("validated diagnosis always serializes")
            })
        };
        match validated {
            Some(value) => {
                crate::db::HealthRepository::store_diagnosis(db, &experiment_id, &value, now)?;
                crate::db::HealthRepository::set_state(
                    db,
                    &experiment_id,
                    HealthState::ActionPending,
                    now,
                )?;
            }
            None => {
                let attempts = match crate::db::HealthRepository::get(db, &experiment_id)? {
                    Some(row) => row.diagnosis_attempt_count() + 1,
                    None => 1,
                };
                let wrapper = serde_json::json!({ "attempt": attempts });
                crate::db::HealthRepository::store_diagnosis(
                    db,
                    &experiment_id,
                    &wrapper,
                    now,
                )?;
                crate::db::HealthRepository::set_state(
                    db,
                    &experiment_id,
                    HealthState::Suspicious,
                    now,
                )?;
                if let Some(outcome) = &mut self.terminal_outcome {
                    outcome.status = AgentRunStatus::Failed;
                    outcome.last_error = Some("health_diagnosis_missing".to_owned());
                }
            }
        }
        self.diagnosis_persistence = Some(DiagnosisPersistence::Persisted);
        Ok(())
    }

    /// Validate and durably persist the bounded editor result before the
    /// private descriptor is cleaned up.  Only the result digest and bounded
    /// summary cross the persistence boundary; raw JSON and credentials stay
    /// in the private run directory until cleanup.
    fn persist_editor_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<(), AppError> {
        let (code_change_run_id, attempt, reserved_session) =
            match self.editor_persistence.as_ref() {
                Some(EditorPersistence::Pending {
                    code_change_run_id,
                    attempt,
                    session_id,
                }) => (code_change_run_id.clone(), *attempt, session_id.clone()),
                Some(EditorPersistence::Persisted) | None => return Ok(()),
            };
        let limits = match &self.retained_authority {
            RetainedLaunchAuthority::Retained { global_policy, .. } => {
                global_policy.campaign_limits
            }
            RetainedLaunchAuthority::Released => {
                return Err(AppError::Runtime {
                    operation: "persist editor after releasing private temp",
                })
            }
            #[cfg(test)]
            RetainedLaunchAuthority::Test => {
                return Err(AppError::Runtime {
                    operation: "test editor handle has no private temp",
                })
            }
        };
        let (project_policy, global_policy) = match &self.retained_authority {
            RetainedLaunchAuthority::Retained {
                global_policy,
                project_policy,
                ..
            } => (project_policy, global_policy),
            _ => unreachable!("editor persistence authority checked above"),
        };

        let process_completed = self
            .terminal_outcome
            .as_ref()
            .is_some_and(|outcome| outcome.status == AgentRunStatus::Completed);
        let mut status = "failed";
        let mut digest = None;
        let mut failure_code = None;
        let mut failure_summary;
        let mut proposed_checks = Vec::new();
        let mut resolved_session = reserved_session.clone();
        if process_completed {
            let bytes = match &self.retained_authority {
                RetainedLaunchAuthority::Retained { temp, .. } => temp.read_editor_output(),
                _ => unreachable!("editor persistence authority checked above"),
            };
            let tools = [
                CodeChangeTool::Git,
                CodeChangeTool::Cargo,
                CodeChangeTool::Uv,
                CodeChangeTool::Python,
            ]
            .into_iter()
            .filter(|tool| global_policy.code_change_tool(*tool).is_some())
            .collect::<BTreeSet<_>>();
            let parsed = bytes.ok().and_then(|bytes| {
                crate::code_change::parse_editor_output(
                    &bytes,
                    &project_policy.root_anchor.canonical_path,
                    &limits,
                    &tools,
                )
                .ok()
                .map(|output| (bytes, output))
            });
            match parsed {
                Some((bytes, output)) if output.status == "ready" => {
                    status = "ready";
                    digest = Some(format!("{:x}", Sha256::digest(&bytes)));
                    failure_summary = Some(bounded_redacted_text(&output.summary));
                    proposed_checks = output
                        .proposed_checks
                        .iter()
                        .enumerate()
                        .map(|(ordinal, check)| {
                            NewCodeChangeCheck::new(
                                attempt,
                                ordinal as i64,
                                "editor",
                                check.argv.clone(),
                                check.working_directory.clone(),
                            )
                        })
                        .collect();
                }
                Some((bytes, output)) => {
                    digest = Some(format!("{:x}", Sha256::digest(&bytes)));
                    failure_code = Some("cannot_apply");
                    failure_summary = Some(bounded_redacted_text(&output.summary));
                    proposed_checks = output
                        .proposed_checks
                        .iter()
                        .enumerate()
                        .map(|(ordinal, check)| {
                            NewCodeChangeCheck::new(
                                attempt,
                                ordinal as i64,
                                "editor",
                                check.argv.clone(),
                                check.working_directory.clone(),
                            )
                        })
                        .collect();
                }
                None => {
                    failure_code = Some("editor_output_invalid");
                    failure_summary =
                        Some("editor output was missing or failed secure validation".to_owned());
                }
            }
        } else {
            let outcome = self
                .terminal_outcome
                .as_ref()
                .expect("terminal outcome checked before editor persistence");
            failure_code = Some(if outcome.status == AgentRunStatus::TimedOut {
                "editor_timeout"
            } else {
                "editor_exit"
            });
            failure_summary = outcome.last_error.clone();
        }

        // A built-in Codex attempt starts with a supervisor placeholder.  It
        // may only become a resumable lineage after the terminal pass proves
        // the newest session is owned by this candidate root.  Without that
        // proof, a failed first attempt must be rejected rather than retried
        // as an unowned fresh or guessed session.
        if attempt == 1 && project_policy.agent_kind == AgentKind::BuiltInCodex {
            match crate::codex_session::resolve_latest_owned_session(
                &global_policy.codex_home,
                &project_policy.root_anchor.canonical_path,
            ) {
                Ok(session) => resolved_session = session,
                Err(_) => {
                    status = "failed";
                    failure_code = Some("editor_session_missing");
                    failure_summary =
                        Some("no candidate-root-owned Codex session was proven".to_owned());
                }
            }
        }

        let repository = CodeChangeRepository::new(db);
        if resolved_session != reserved_session {
            repository.bind_editor_session(&code_change_run_id, attempt, &resolved_session, now)?;
        }
        repository.finish_editor_attempt_with_checks(
            &code_change_run_id,
            attempt,
            status,
            digest.as_deref(),
            failure_code,
            failure_summary.as_deref(),
            self.terminal_outcome.as_ref().map(|_| now),
            now,
            &proposed_checks,
            now,
        )?;
        if status == "failed"
            && (failure_code == Some("cannot_apply")
                || failure_code == Some("editor_session_missing")
                || attempt >= 2)
        {
            let reason = failure_code.unwrap_or("editor_failed");
            repository.reject(
                &code_change_run_id,
                reason,
                failure_summary
                    .as_deref()
                    .unwrap_or("code-change editor failed"),
                now,
            )?;
        }
        self.editor_persistence = Some(EditorPersistence::Persisted);
        Ok(())
    }

    fn persist_terminal_outcome(
        &mut self,
        db: &crate::db::Db,
        now: i64,
    ) -> Result<AgentRunStatus, AppError> {
        if let TerminalPersistence::Persisted(status) = &self.terminal_persistence {
            return Ok(*status);
        }
        if self.terminal_outcome.is_none() {
            return Err(AppError::Runtime {
                operation: "finalize missing agent process outcome",
            });
        }
        self.persist_decision_outcome(db, now)?;
        self.persist_diagnosis_outcome(db, now)?;
        self.persist_editor_outcome(db, now)?;
        let outcome = self
            .terminal_outcome
            .as_ref()
            .expect("terminal outcome checked before decision persistence");
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

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[cfg(unix)]
    fn secure_agent_fixture_root(temporary: &tempfile::TempDir, project_root: &Path) {
        let service_root = project_root.join(".pueue-agent");
        let logs_root = service_root.join("logs");
        for path in [
            temporary.path(),
            project_root,
            service_root.as_path(),
            logs_root.as_path(),
        ] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
    }

    #[test]
    fn decision_output_schema_is_a_supported_strict_root_object() {
        let schema: serde_json::Value = serde_json::from_slice(DECISION_OUTPUT_SCHEMA).unwrap();
        assert_eq!(schema.get("type").and_then(serde_json::Value::as_str), Some("object"));
        assert!(schema.get("oneOf").is_none());
        assert!(schema.get("anyOf").is_none());
        assert_strict_object_schema(&schema);
    }

    #[test]
    fn decision_output_schema_supports_goal_reached_with_evidence_ref() {
        let schema: serde_json::Value = serde_json::from_slice(DECISION_OUTPUT_SCHEMA).unwrap();
        let decision_enum = schema["properties"]["decision"]["enum"]
            .as_array()
            .expect("decision enum must be an array");
        assert!(
            decision_enum
                .iter()
                .any(|value| value.as_str() == Some("goal_reached")),
            "decision enum must include goal_reached"
        );
        let properties = schema["properties"].as_object().expect("properties must be an object");
        assert!(
            properties.contains_key("evidence_ref"),
            "properties must include evidence_ref"
        );
        let required = schema["required"].as_array().expect("required must be an array");
        assert!(
            required.iter().any(|value| value.as_str() == Some("evidence_ref")),
            "required must include evidence_ref"
        );
        let evidence_ref = &properties["evidence_ref"];
        let ty = evidence_ref.get("type").expect("evidence_ref must have a type");
        let ty_string = ty.to_string();
        assert!(
            ty_string.contains("string") && ty_string.contains("null"),
            "evidence_ref type must be string|null"
        );
        assert_eq!(
            evidence_ref.get("maxLength").and_then(|value| value.as_u64()),
            Some(512),
            "evidence_ref maxLength must be 512"
        );
        assert_strict_object_schema(&schema);
        let json = serde_json::json!({
            "schema_version": 1,
            "decision": "goal_reached",
            "proposal": null,
            "reason": null,
            "requested_wait_minutes": null,
            "expected_evidence": null,
            "evidence_ref": "experiment-123"
        });
        let bytes = serde_json::to_vec(&json).unwrap();
        let validated = crate::decision_protocol::parse_and_validate_decision(
            &bytes,
            "objective-digest",
            crate::execution_policy::CampaignLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            validated,
            crate::decision_protocol::ValidatedDecision::GoalReached(_)
        ));
    }

    #[test]
    fn production_runner_probes_decision_capabilities_instead_of_assuming_them() {
        assert_eq!(
            AgentRunnerConfig::production().codex_capabilities,
            CodexCapabilities::standard_policy()
        );
        assert_eq!(
            AgentRunnerConfig::production().decision_capabilities,
            DecisionCapabilitySource::InstalledCli
        );
    }

    #[test]
    #[ignore = "internal agent-handle subprocess entry"]
    fn timeout_retry_subprocess() {
        std::thread::sleep(Duration::from_secs(30));
    }

    #[test]
    #[ignore = "internal agent-handle terminal subprocess entry"]
    fn terminal_retry_subprocess() {}

    #[test]
    fn unowned_decision_bind_failure_terminalizes_only_the_new_agent_run() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        secure_agent_fixture_root(&temporary, &root);
        let project = crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a", &root, "pa-project-a",
                root.join(".pueue-agent/config.toml"), 1,
            ))
            .unwrap();
        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a", crate::models::EventKind::CampaignDecision,
                "bind-failure", serde_json::json!({}), 1, 1,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db).claim_batch(1, 100, 1).unwrap();
        let repository = AgentRunRepository::new(&db);
        let run = repository
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a", event.event_id, None, AgentRunStatus::Starting, 2,
                    root.join(".pueue-agent/logs/bind-failure.log"),
                ),
                &[event.event_id],
            )
            .unwrap();
        let error = resolve_unowned_decision_bind_failure(
            &repository, &project, run.run_id, 3, RetryPolicy { max_retries: 1 },
            AppError::Runtime { operation: "injected transient decision bind failure" },
        );
        assert_eq!(
            error.stage,
            AgentSpawnStage::RunBoundPreMarker { run_id: run.run_id, resolved: true }
        );
        assert_eq!(
            repository.list_by_project("project-a", 10).unwrap()[0].status,
            AgentRunStatus::Failed
        );
        let attempts: i64 = db.connect().unwrap()
            .query_row("SELECT COUNT(*) FROM decision_attempts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(attempts, 0);
    }

    #[tokio::test]
    async fn unowned_decision_bind_finalization_failure_retains_db_retry_authority() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        secure_agent_fixture_root(&temporary, &root);
        let project = crate::db::ProjectRepository::new(&db)
            .register(&crate::models::NewProject::new(
                "project-a",
                &root,
                "pa-project-a",
                root.join(".pueue-agent/config.toml"),
                1,
            ))
            .unwrap();
        let objective = crate::state::ObjectiveSnapshot {
            text: "Reach validation loss below 0.20\n".to_owned(),
            digest: "objective-digest".to_owned(),
        };
        let baseline = crate::proposals::validate_initial_baseline(
            crate::proposals::ProposalInput {
                kind: crate::models::ProposalKind::Experiment,
                hypothesis: "Measure the initial command".to_owned(),
                source_experiment_id: None,
                argv: vec!["python".to_owned(), "train.py".to_owned()],
                working_directory: ".".to_owned(),
                expected_evidence: vec!["validation loss".to_owned()],
            },
            &objective.digest,
        )
        .unwrap();
        crate::db::CampaignRepository::new(&db)
            .start_with_baseline(
                crate::db::StartCampaignRequest {
                    campaign_id: "campaign-a",
                    project_id: "project-a",
                    objective: &objective,
                    initial_argv: baseline.argv(),
                    baseline: &baseline,
                    submission_id: "submission-a",
                    experiment_id: "experiment-a",
                    proposal_id: "proposal-a",
                    metadata: &serde_json::json!({}),
                    origin_agent_run_id: None,
                    objective_metric: None,
                    now: 2,
                },
                &CampaignLimits::default(),
            )
            .unwrap();
        let experiments = crate::db::ExperimentRepository::new(&db);
        experiments.mark_submitting("experiment-a", 3).unwrap();
        experiments
            .mark_accepted("experiment-a", 41, "pueue-task:v1:bind-failure", 4)
            .unwrap();
        experiments
            .project_terminal_submission(
                "experiment-a",
                41,
                crate::models::ExperimentTerminalOutcome::Succeeded,
                5,
            )
            .unwrap();
        let decisions = DecisionRepository::new(&db);
        let cycle = decisions
            .ensure_cycle_for_terminal("campaign-a", "experiment-a", 6)
            .unwrap();
        let reservation = decisions
            .reserve_next_attempt("project-a", &cycle.cycle_id, 7)
            .unwrap()
            .unwrap();

        let event = crate::db::EventRepository::new(&db)
            .insert_idempotent(&crate::models::NewEvent::new(
                "project-a",
                crate::models::EventKind::CampaignDecision,
                "bind-finalization-failure",
                serde_json::json!({}),
                8,
                8,
            ))
            .unwrap();
        crate::db::EventRepository::new(&db)
            .claim_batch(8, 100, 1)
            .unwrap();
        let repository = AgentRunRepository::new(&db);
        let run = repository
            .insert_with_events(
                &NewAgentRun::new(
                    "project-a",
                    event.event_id,
                    None,
                    AgentRunStatus::Starting,
                    9,
                    root.join(".pueue-agent/logs/bind-finalization-failure.log"),
                ),
                &[event.event_id],
            )
            .unwrap();
        let bind_error = decisions.bind_agent_run(&reservation, run.run_id, 10).unwrap_err();
        db.connect()
            .unwrap()
            .execute_batch(&format!(
                "CREATE TRIGGER fail_unowned_bind_finalization
                 BEFORE UPDATE OF status ON agent_runs
                 WHEN OLD.run_id = {} AND NEW.status = 'failed'
                 BEGIN SELECT RAISE(ABORT, 'injected agent-run finalizer failure'); END;",
                run.run_id
            ))
            .unwrap();

        let mut error = resolve_unowned_decision_bind_failure(
            &repository,
            &project,
            run.run_id,
            11,
            RetryPolicy { max_retries: 1 },
            bind_error,
        );
        assert_eq!(
            error.stage,
            AgentSpawnStage::RunBoundPreMarker {
                run_id: run.run_id,
                resolved: false,
            }
        );
        let mut cleanup = error.cleanup.take().expect("DB retry authority retained");
        let read_attempt_state = || {
            db.connect()
                .unwrap()
                .query_row(
                    "SELECT state, agent_run_id, started_at, finished_at
                     FROM decision_attempts
                     WHERE cycle_id = ?1 AND attempt_number = ?2",
                    rusqlite::params![reservation.cycle_id, reservation.attempt_number],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, Option<i64>>(1)?,
                            row.get::<_, Option<i64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                        ))
                    },
                )
                .unwrap()
        };
        let attempt_before_retry = read_attempt_state();
        assert_eq!(attempt_before_retry.0, "reserved");
        assert_eq!(attempt_before_retry.1, None);
        assert_eq!(
            repository.list_by_project("project-a", 10).unwrap()[0].status,
            AgentRunStatus::Starting
        );

        db.connect()
            .unwrap()
            .execute_batch("DROP TRIGGER fail_unowned_bind_finalization")
            .unwrap();
        cleanup.retry(&db, 12).await.unwrap();

        let attempt_after_retry = read_attempt_state();
        assert_eq!(attempt_after_retry, attempt_before_retry);
        assert_eq!(
            repository.list_by_project("project-a", 10).unwrap()[0].status,
            AgentRunStatus::Failed
        );
        let event_status: String = db
            .connect()
            .unwrap()
            .query_row(
                "SELECT status FROM events WHERE event_id = ?1",
                [event.event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_status, "retry_wait");
    }

    #[tokio::test]
    async fn timeout_termination_error_retains_db_state_and_same_handle_for_retry() {
        let temporary = tempfile::tempdir().unwrap();
        let db = crate::db::Db::open(&temporary.path().join("state.sqlite3")).unwrap();
        let root = temporary.path().join("project");
        std::fs::create_dir_all(root.join(".pueue-agent/logs")).unwrap();
        secure_agent_fixture_root(&temporary, &root);
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
            role: AgentRunRole::Standard,
            decision_persistence: None,
            diagnosis_persistence: None,
            editor_persistence: None,
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
        secure_agent_fixture_root(&temporary, &root);
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
            role: AgentRunRole::Standard,
            decision_persistence: None,
            diagnosis_persistence: None,
            editor_persistence: None,
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
        secure_agent_fixture_root(&project_temp, &root);
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
            role: AgentRunRole::Standard,
            decision_persistence: None,
            diagnosis_persistence: None,
            editor_persistence: None,
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
        secure_agent_fixture_root(&temporary, &root);
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
            decision_failure: None,
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
        secure_agent_fixture_root(&temporary, &root);
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
            role: AgentRunRole::Standard,
            decision_persistence: None,
            diagnosis_persistence: None,
            editor_persistence: None,
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
