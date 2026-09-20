use std::{
    collections::{BTreeMap, BTreeSet},
    future::{poll_fn, Future},
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::{AgentHandle, AgentRunner, BoundCleanupHandle},
    campaign::{CampaignCoordinator, CampaignSubmission},
    config,
    db::{
        startup_research_owner_snapshot, AgentRunRepository, CampaignRepository, Db,
        DecisionRepository, ProjectRepository, ResearchRepository, StartupResearchOwner,
        TerminationRequestRepository,
    },
    decision::{DecisionCoordinator, DecisionLoopReport, DecisionRecoveryReport},
    detect::Detector,
    execution_policy::ResolvedExecutionPolicy,
    environment::RecoveredPrivateRunTempCleanup,
    health::{DetectionSignals, HealthEngine, HealthReport},
    health_diagnosis::run_due_diagnoses,
    incidents::IncidentStore,
    pueue::PueueApi,
    periodic::PeriodicDeepCheckScheduler,
    process::{startup_process_quiescence, StartupProcessQuiescence},
    reconcile::{ReconcileReport, Reconciler},
    research_actions::advance_research_actions,
    research::{recover_research, run_due_research_with_cleanup_blocked_projects},
    retry::RetryPolicy,
    scheduler::{Scheduler, SchedulerConfig, SchedulerReport},
    termination::{TerminationManager, TerminationOutcome},
    AppError,
};

#[cfg(unix)]
use crate::code_change::{list_recoverable_code_change_runs, CodeChangeCoordinator};
#[cfg(unix)]
use crate::models::CodeChangeState;

const DAEMON_RESTART_REASON: &str = "agent run interrupted by daemon restart";

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub interval: Duration,
    pub lease_seconds: i64,
    pub claim_limit: usize,
    pub now_override: Option<i64>,
    pub shutdown_grace_period: Duration,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            lease_seconds: 600,
            claim_limit: 100,
            now_override: None,
            shutdown_grace_period: Duration::from_secs(30),
        }
    }
}

#[derive(Default)]
pub struct DaemonReport {
    pub reconciliation: ReconcileReport,
    pub observations: usize,
    pub health: HealthReport,
    pub diagnoses: usize,
    pub termination_outcomes: Vec<TerminationOutcome>,
    pub scheduled_deep_checks: usize,
    pub scheduler: SchedulerReport,
    pub finished_agents: usize,
    pub recovered_agent_runs: usize,
    pub requeued_agent_events: usize,
    pub dead_lettered_agent_events: usize,
    pub preserved_code_change_editors: usize,
    pub preserved_research_runs: usize,
    pub research_started: usize,
    pub research_deferred: usize,
    pub research_blocked: usize,
    pub decision_recovery: DecisionRecoveryReport,
    pub decisions: DecisionLoopReport,
    pub code_changes: CodeChangeLoopReport,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CodeChangeLoopReport {
    pub started: usize,
    pub advanced: usize,
    pub deferred: usize,
    pub rejected: usize,
    pub cleanup: usize,
}

pub struct Daemon<P> {
    db: Db,
    pueue: P,
    policy: Arc<ResolvedExecutionPolicy>,
    runner: Option<AgentRunner>,
    config: DaemonConfig,
    active_agents: Vec<AgentHandle>,
    active_cleanups: Vec<BoundCleanupHandle>,
    startup_recovery_pending: bool,
    startup_research_owners: BTreeMap<i64, StartupResearchOwner>,
    startup_research_cursor: usize,
}

impl<P> Daemon<P>
where
    P: PueueApi + Clone,
{
    pub fn new(
        db: Db,
        pueue: P,
        policy: Arc<ResolvedExecutionPolicy>,
        runner: AgentRunner,
        config: DaemonConfig,
    ) -> Self {
        Self {
            db,
            pueue,
            policy,
            runner: Some(runner),
            config,
            active_agents: Vec::new(),
            active_cleanups: Vec::new(),
            startup_recovery_pending: true,
            startup_research_owners: BTreeMap::new(),
            startup_research_cursor: 0,
        }
    }

    pub async fn run(&mut self, shutdown: CancellationToken) -> Result<(), AppError> {
        self.run_once_or_drain_on_error().await?;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    self.drain_agents_on_shutdown().await?;
                    return Ok(());
                }
                () = tokio::time::sleep(self.config.interval) => {
                    self.run_once_or_drain_on_error().await?;
                }
            }
        }
    }

    async fn run_once_or_drain_on_error(&mut self) -> Result<DaemonReport, AppError> {
        match self.run_once().await {
            Ok(report) => Ok(report),
            Err(scheduler_error) => match self.drain_agents_on_shutdown().await {
                Ok(_) => Err(scheduler_error),
                Err(drain_error) => Err(drain_error),
            },
        }
    }

    pub async fn run_once(&mut self) -> Result<DaemonReport, AppError> {
        let now = self.now()?;
        let mut report = DaemonReport::default();

        if self.startup_recovery_pending {
            let (
                policies,
                confirmed_pending_marker_ids,
                confirmed_release_requested_ids,
                indeterminate_pending_marker_ids,
                indeterminate_release_requested_ids,
                absent_pending_marker_ids,
            ) = self.load_startup_recovery_inputs()?;
            let recovery = AgentRunRepository::new(&self.db)
                .recover_interrupted_with_marker_evidence(
                    now,
                    DAEMON_RESTART_REASON,
                    &policies,
                    &confirmed_pending_marker_ids,
                    &confirmed_release_requested_ids,
                    &indeterminate_pending_marker_ids,
                    &indeterminate_release_requested_ids,
                )?;
            recover_research(&self.db, now, self.policy.campaign_limits).await?;
            let startup_owners = startup_research_owner_snapshot(
                &self.db,
                &recovery.preserved_research_run_ids,
                &absent_pending_marker_ids,
            )?;
            let research_repository = crate::db::ResearchRepository::new(&self.db);
            for owner in startup_owners.values() {
                if owner.authority.is_none() {
                    research_repository.mark_startup_recovery_required(owner.run_id, now)?;
                }
            }
            self.startup_research_owners = startup_owners;
            self.startup_research_cursor = 0;

            #[cfg(unix)]
            {
                let runner = self.runner.as_ref().ok_or(AppError::Runtime {
                    operation: "borrow daemon code-change runner for startup recovery",
                })?;
                let mut code_changes = CodeChangeCoordinator::new(
                    &self.db,
                    runner,
                    self.policy.as_ref(),
                    self.policy.campaign_limits,
                )
                .with_lease_seconds(self.config.lease_seconds)
                .recover_startup_editors(
                    now,
                    &recovery.preserved_code_change_editor_run_ids,
                    &policies,
                    &confirmed_pending_marker_ids,
                    &confirmed_release_requested_ids,
                    &indeterminate_pending_marker_ids,
                    &indeterminate_release_requested_ids,
                )
                .await?;
                report.code_changes.started += code_changes.started.len();
                report.code_changes.advanced += code_changes.advanced;
                report.code_changes.deferred += code_changes.deferred;
                report.code_changes.rejected += code_changes.rejected;
                report.code_changes.cleanup += code_changes.cleanup.len();
                self.active_agents.extend(
                    code_changes
                        .started
                        .drain(..)
                        .map(|started| started.handle),
                );
                self.active_cleanups.extend(code_changes.cleanup.drain(..));
            }

            let campaigns = CampaignRepository::new(&self.db);
            campaigns.recover_submission_boundaries(now)?;

            #[cfg(unix)]
            {
                let runner = self.runner.as_ref().ok_or(AppError::Runtime {
                    operation: "borrow daemon code-change runner for interrupted recovery",
                })?;
                let mut code_changes = CodeChangeCoordinator::new(
                    &self.db,
                    runner,
                    self.policy.as_ref(),
                    self.policy.campaign_limits,
                )
                .with_lease_seconds(self.config.lease_seconds)
                .recover_interrupted(now, self.config.claim_limit)
                .await?;
                report.code_changes.started += code_changes.started.len();
                report.code_changes.advanced += code_changes.advanced;
                report.code_changes.deferred += code_changes.deferred;
                report.code_changes.rejected += code_changes.rejected;
                report.code_changes.cleanup += code_changes.cleanup.len();
                self.active_agents.extend(
                    code_changes
                        .started
                        .drain(..)
                        .map(|started| started.handle),
                );
                self.active_cleanups.extend(code_changes.cleanup.drain(..));
            }

            self.startup_recovery_pending = false;
            report.recovered_agent_runs = recovery.failed_runs;
            report.requeued_agent_events = recovery.requeued_events;
            report.dead_lettered_agent_events = recovery.dead_lettered_events;
            report.preserved_code_change_editors = recovery.preserved_code_change_editors;
            report.preserved_research_runs = recovery.preserved_research_runs;
        }

        self.dispatch_reserved_campaign_submissions(now).await?;

        report.decision_recovery = DecisionCoordinator::new(
            &self.db,
            &self.pueue,
            self.policy.campaign_limits,
        )
        .with_policy(self.policy.as_ref())
        .recover_interrupted(now, self.config.claim_limit)?;

        let reconciliation = Reconciler::new(&self.db, self.pueue.clone())
            .with_execution_policy(Arc::clone(&self.policy))
            .run_once_at(now)
            .await?;
        let (detection_signals, observations) = self.run_detection(&reconciliation).await?;
        report.observations = observations;
        let mut health = self.run_health_observer(&reconciliation, &detection_signals, now)?;
        health.executed_actions = self.run_health_actions(now).await?;
        report.health = health;
        let health_blocked_projects = self.cleanup_blocked_projects()?;
        report.diagnoses = self
            .run_health_diagnoses(now, &health_blocked_projects)
            .await?;
        report.termination_outcomes = self.run_termination().await?;
        report.scheduled_deep_checks = PeriodicDeepCheckScheduler::new(&self.db, now)
            .schedule(&reconciliation.observed_tasks)?;

        report.finished_agents += self.poll_retained_ownership_at(now).await?;
        let research = self.run_due_research(now).await?;
        report.research_started += research.0;
        report.research_deferred += research.1;
        report.research_blocked += research.2;
        let _research_actions = advance_research_actions(
            &self.db,
            &self.pueue,
            self.policy.as_ref(),
            now,
            self.config.claim_limit,
        )
        .await?;
        #[cfg(unix)]
        {
            let runner = self.runner.as_ref().ok_or(AppError::Runtime {
                operation: "borrow daemon code-change runner",
            })?;
            let mut code_changes = CodeChangeCoordinator::new(
                &self.db,
                runner,
                self.policy.as_ref(),
                self.policy.campaign_limits,
            )
            .with_lease_seconds(self.config.lease_seconds)
            .advance_ready(now, self.config.claim_limit)
            .await?;
            report.code_changes.started += code_changes.started.len();
            report.code_changes.advanced += code_changes.advanced;
            report.code_changes.deferred += code_changes.deferred;
            report.code_changes.rejected += code_changes.rejected;
            report.code_changes.cleanup += code_changes.cleanup.len();
            self.active_agents.extend(
                code_changes
                    .started
                    .drain(..)
                    .map(|started| started.handle),
            );
            self.active_cleanups.extend(code_changes.cleanup.drain(..));
        }
        report.decisions = DecisionCoordinator::new(
            &self.db,
            &self.pueue,
            self.policy.campaign_limits,
        )
        .with_policy(self.policy.as_ref())
        .apply_ready(now, self.config.claim_limit)
        .await?;
        DecisionRepository::new(&self.db).due_cycles(now, self.config.claim_limit)?;
        CampaignRepository::new(&self.db)
            .wake_eligible_campaigns_with_limits(&self.policy.campaign_limits, now)?;

        let cleanup_blocked_projects = self.cleanup_blocked_projects()?;
        let mut scheduler = Scheduler::new(
            self.db.clone(),
            self.runner.take().ok_or(AppError::Runtime {
                operation: "take daemon scheduler runner",
            })?,
            SchedulerConfig {
                now,
                lease_seconds: self.config.lease_seconds,
                claim_limit: self.config.claim_limit,
            },
        )
        .with_campaign_limits(self.policy.campaign_limits)
        .with_cleanup_blocked_projects(cleanup_blocked_projects);
        let scheduler_result = scheduler.tick().await;
        self.runner = Some(scheduler.into_runner());
        let mut scheduler_report = match scheduler_result {
            Ok(report) => report,
            Err(error) => {
                let (mut scheduler_report, source) = error.into_parts();
                self.active_agents.extend(
                    scheduler_report
                        .started
                        .drain(..)
                        .map(|started| started.handle),
                );
                self.active_cleanups
                    .extend(scheduler_report.cleanup.drain(..));
                // The scheduler error remains the public cause, but every
                // newly absorbed owner still receives one fair poll before
                // this tick returns. Any retained failure is retried by the
                // daemon error drain or the next explicit run_once call.
                let _ownership_result = self.poll_retained_ownership_at(now).await;
                return Err(source);
            }
        };
        self.active_agents.extend(
            scheduler_report
                .started
                .drain(..)
                .map(|started| started.handle),
        );
        self.active_cleanups
            .extend(scheduler_report.cleanup.drain(..));
        report.scheduler = scheduler_report;
        report.finished_agents += self.poll_retained_ownership_at(now).await?;
        report.reconciliation = reconciliation;
        Ok(report)
    }

    async fn dispatch_reserved_campaign_submissions(&self, now: i64) -> Result<(), AppError> {
        let campaigns = CampaignRepository::new(&self.db);
        #[cfg(unix)]
        for run in list_recoverable_code_change_runs(&self.db, 100)?
            .into_iter()
            .filter(|run| run.state == CodeChangeState::CandidateReady)
        {
            let Some(campaign) = campaigns.find_by_id(&run.campaign_id)? else {
                continue;
            };
            if campaign.state != crate::models::CampaignState::Active {
                continue;
            }
            let project = ProjectRepository::new(&self.db)
                .find_by_id(&campaign.project_id)?
                .ok_or(AppError::Runtime {
                    operation: "read candidate project during reserved submission dispatch",
                })?;
            if !project.enabled || project.paused || project.halted_reason.is_some() {
                continue;
            }
            let root_anchor = self
                .policy
                .project_root_anchor(&project.root_path)
                .map_err(AppError::from)?;
            let result = CampaignCoordinator::new(
                &self.db,
                &self.pueue,
                self.policy.campaign_limits,
            )
            .with_root_anchor(root_anchor)
            .with_execution_policy(self.policy.as_ref())
            .submit_candidate_intent(&run.code_change_run_id, &project, now)
            .await;
            match result {
                Ok(CampaignSubmission::Submitted(_)) | Ok(CampaignSubmission::Deferred) => {}
                Err(AppError::Runtime {
                    operation: "acquire project submission admission lock",
                }) => continue,
                Err(error) => return Err(error),
            }
        }
        let intents = campaigns.list_reserved_submission_intents(100)?;
        for intent in intents {
            let project = ProjectRepository::new(&self.db)
                .find_by_id(&intent.campaign.project_id)?
                .ok_or(AppError::Runtime {
                    operation: "read campaign project during reserved submission dispatch",
                })?;
            let root_anchor = self
                .policy
                .project_root_anchor(&project.root_path)
                .map_err(AppError::from)?;
            let result = CampaignCoordinator::new(
                &self.db,
                &self.pueue,
                self.policy.campaign_limits,
            )
            .with_root_anchor(root_anchor)
            .with_execution_policy(self.policy.as_ref())
            .submit_reserved_intent(&intent, &project, now)
            .await;
            match result {
                Ok(CampaignSubmission::Submitted(_)) | Ok(CampaignSubmission::Deferred) => {}
                Err(AppError::Runtime {
                    operation: "acquire project submission admission lock",
                }) => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub fn load_startup_retry_policies(&self) -> Result<BTreeMap<String, RetryPolicy>, AppError> {
        let projects = ProjectRepository::new(&self.db).list_all()?;
        let mut policies = BTreeMap::new();
        for project in projects {
            let project_config = config::load(&project.config_path)?;
            if project_config.project_id != project.project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "project config identity does not match the database project",
                });
            }
            if project_config.pueue_group != project.pueue_group {
                return Err(AppError::Validation {
                    field: "pueue_group",
                    message: "project config group does not match the database project",
                });
            }
            policies.insert(
                project.project_id,
                RetryPolicy {
                    max_retries: project_config.agent.max_retries,
                },
            );
        }
        Ok(policies)
    }

    fn load_startup_recovery_inputs(
        &self,
    ) -> Result<
        (
            BTreeMap<String, RetryPolicy>,
            BTreeSet<i64>,
            BTreeSet<i64>,
            BTreeSet<i64>,
            BTreeSet<i64>,
            BTreeSet<i64>,
        ),
        AppError,
    > {
        let projects = ProjectRepository::new(&self.db).list_all()?;
        let runner = self.runner.as_ref().ok_or(AppError::Runtime {
            operation: "read daemon scheduler runner for startup recovery",
        })?;
        let mut policies = BTreeMap::new();
        let mut confirmed_pending_marker_ids = BTreeSet::new();
        let mut confirmed_release_requested_ids = BTreeSet::new();
        let mut indeterminate_pending_marker_ids = BTreeSet::new();
        let mut indeterminate_release_requested_ids = BTreeSet::new();
        let mut absent_pending_marker_ids = BTreeSet::new();
        for project in projects {
            let project_config = config::load(&project.config_path)?;
            if project_config.project_id != project.project_id {
                return Err(AppError::Validation {
                    field: "project_id",
                    message: "project config identity does not match the database project",
                });
            }
            if project_config.pueue_group != project.pueue_group {
                return Err(AppError::Validation {
                    field: "pueue_group",
                    message: "project config group does not match the database project",
                });
            }
            policies.insert(
                project.project_id.clone(),
                RetryPolicy {
                    max_retries: project_config.agent.max_retries,
                },
            );
            let candidates = AgentRunRepository::new(&self.db)
                .list_startup_marker_candidates(&project.project_id)?;
            if !candidates.is_empty() {
                let marker_evidence = runner.inspect_startup_gate_markers(
                    &project,
                    &project_config,
                    &candidates,
                )?;
                for (run_id, gate_state, _) in candidates {
                    let confirmed = marker_evidence.confirmed.contains(&run_id);
                    let indeterminate = marker_evidence.indeterminate.contains(&run_id);
                    if gate_state == "pending" && marker_evidence.absent.contains(&run_id) {
                        absent_pending_marker_ids.insert(run_id);
                    }
                    if !confirmed && !indeterminate {
                        continue;
                    }
                    match gate_state.as_str() {
                        "pending" => {
                            if confirmed {
                                confirmed_pending_marker_ids.insert(run_id);
                            } else {
                                indeterminate_pending_marker_ids.insert(run_id);
                            }
                        }
                        "release_requested" => {
                            if confirmed {
                                confirmed_release_requested_ids.insert(run_id);
                            } else {
                                indeterminate_release_requested_ids.insert(run_id);
                            }
                        }
                        _ => {
                            return Err(AppError::Validation {
                                field: "launch_gate_state",
                                message: "startup marker candidate has an invalid gate state",
                            });
                        }
                    }
                }
            }
        }
        Ok((
            policies,
            confirmed_pending_marker_ids,
            confirmed_release_requested_ids,
            indeterminate_pending_marker_ids,
            indeterminate_release_requested_ids,
            absent_pending_marker_ids,
        ))
    }

    async fn run_detection(
        &self,
        reconciliation: &ReconcileReport,
    ) -> Result<(DetectionSignals, usize), AppError> {
        let projects = ProjectRepository::new(&self.db).list_enabled()?;
        let projects_by_group = projects
            .iter()
            .map(|project| (project.pueue_group.as_str(), project))
            .collect::<BTreeMap<_, _>>();
        let mut task_signals = DetectionSignals::new();
        let mut observations = 0;

        for task in reconciliation
            .observed_tasks
            .iter()
            .filter(|task| task.is_running())
        {
            let Some(project) = projects_by_group.get(task.group.as_str()) else {
                continue;
            };
            let project_config = config::load(&project.config_path)?;
            let detector = Detector::for_project(
                &project.project_id,
                &project.root_path,
                project.root_path.join(".pueue-agent/logs"),
            )
            .with_incident_db(self.db.clone());
            let inspection = detector.inspect_task_now(task, &project_config.check)?;
            let signals = task_signals.entry(task.id).or_default();
            for signal in inspection.signals {
                if !signals.contains(&signal) {
                    signals.push(signal);
                }
            }
            for observation in inspection.observations {
                IncidentStore::new(&self.db).observe(observation)?;
                observations += 1;
            }
        }

        Ok((task_signals, observations))
    }

    fn run_health_observer(
        &self,
        reconciliation: &ReconcileReport,
        detection_signals: &DetectionSignals,
        now: i64,
    ) -> Result<HealthReport, AppError> {
        let projects = ProjectRepository::new(&self.db).list_enabled()?;
        HealthEngine::run_once(
            &self.db,
            &projects,
            &reconciliation.observed_tasks,
            detection_signals,
            &self.policy.campaign_limits,
            now,
        )
    }

    /// Execute stored diagnoses on `ActionPending` running-health rows.
    async fn run_health_actions(&self, now: i64) -> Result<usize, AppError> {
        let projects = ProjectRepository::new(&self.db).list_enabled()?;
        HealthEngine::execute_pending(
            &self.db,
            &self.pueue,
            &projects,
            &self.policy.campaign_limits,
            now,
        )
        .await
    }

    fn cleanup_blocked_projects(&self) -> Result<BTreeSet<String>, AppError> {
        let mut blocked = ResearchRepository::new(&self.db)
            .native_cleanup_blocked_project_ids()?;
        blocked.extend(
            self.startup_research_owners
                .values()
                .map(|owner| owner.project_id.clone()),
        );
        blocked.extend(
            self.active_agents
                .iter()
                .filter_map(|agent| agent.cleanup_blocked_project().map(str::to_owned)),
        );
        blocked.extend(
            self.active_cleanups
                .iter()
                .filter_map(|cleanup| cleanup.cleanup_blocked_project().map(str::to_owned)),
        );
        Ok(blocked)
    }

    /// Spawn bounded diagnosis agents for suspicious running-health rows.
    async fn run_health_diagnoses(
        &mut self,
        now: i64,
        blocked_projects: &BTreeSet<String>,
    ) -> Result<usize, AppError> {
        let runner = self.runner.take().ok_or(AppError::Runtime {
            operation: "take daemon health-diagnosis runner",
        })?;
        let outcome = run_due_diagnoses(
            &self.db,
            &runner,
            self.config.claim_limit,
            now,
            blocked_projects,
        )
        .await;
        self.runner = Some(runner);
        let report = outcome?;
        let spawned = report.started.len();
        self.active_cleanups.extend(report.cleanups);
        self.active_agents
            .extend(report.started.into_iter().map(|started| started.handle));
        Ok(spawned)
    }

    async fn run_due_research(&mut self, now: i64) -> Result<(usize, usize, usize), AppError> {
        let cleanup_blocked_projects = self.cleanup_blocked_projects()?;
        let runner = self.runner.take().ok_or(AppError::Runtime {
            operation: "take daemon research runner",
        })?;
        let outcome = run_due_research_with_cleanup_blocked_projects(
            &self.db,
            &runner,
            self.policy.campaign_limits,
            now,
            self.config.claim_limit,
            &cleanup_blocked_projects,
        )
        .await;
        self.runner = Some(runner);
        let report = outcome?;
        let started = report.started.len();
        let deferred = report.deferred;
        let blocked = report.blocked;
        self.active_cleanups.extend(report.cleanups);
        self.active_agents.extend(report.started);
        Ok((started, deferred, blocked))
    }

    async fn run_termination(&self) -> Result<Vec<TerminationOutcome>, AppError> {
        let mut outcomes = Vec::new();
        for project in ProjectRepository::new(&self.db).list_active()? {
            for request in
                TerminationRequestRepository::new(&self.db).find_pending(&project.project_id)?
            {
                outcomes.push(
                    TerminationManager::new(&self.db, self.pueue.clone())
                        .execute(request.request_id)
                        .await?,
                );
            }
        }
        Ok(outcomes)
    }

    async fn poll_agents_at(&mut self, now: i64) -> Result<usize, AppError> {
        let mut finished = 0;
        let mut index = 0;
        let mut first_error = None;
        while index < self.active_agents.len() {
            match self.active_agents[index].poll(&self.db, now).await {
                Ok(Some(_)) => {
                    self.active_agents.swap_remove(index);
                    finished += 1;
                }
                Ok(None) => index += 1,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    index += 1;
                }
            }
        }
        first_error.map_or(Ok(finished), Err)
    }

    async fn poll_cleanups_at(&mut self, now: i64) -> Result<usize, AppError> {
        let mut finished = 0;
        let mut index = 0;
        let mut first_error = None;
        while index < self.active_cleanups.len() {
            match self.active_cleanups[index].retry(&self.db, now).await {
                Ok(()) => {
                    self.active_cleanups.swap_remove(index);
                    finished += 1;
                }
                Err(_error) if self.active_cleanups[index].cleanup_pending() => {
                    index += 1;
                }
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    index += 1;
                }
            }
        }
        first_error.map_or(Ok(finished), Err)
    }

    async fn poll_retained_ownership_at(&mut self, now: i64) -> Result<usize, AppError> {
        let (startup, mut first_error) = match self.poll_startup_research_owners_at(now).await {
            Ok(finished) => (finished, None),
            Err(error) => (0, Some(error)),
        };
        let agents = self.poll_agents_at(now).await;
        let cleanups = self.poll_cleanups_at(now).await;
        let finished_agents = match agents {
            Ok(finished) => finished,
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                0
            }
        };
        if let Err(error) = cleanups {
            if first_error.is_none() {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(finished_agents + startup), Err)
    }

    async fn poll_startup_research_owners_at(&mut self, now: i64) -> Result<usize, AppError> {
        let owner_ids = self
            .startup_research_owners
            .keys()
            .copied()
            .collect::<Vec<_>>();
        if owner_ids.is_empty() {
            self.startup_research_cursor = 0;
            return Ok(0);
        }
        let start = self.startup_research_cursor % owner_ids.len();
        let count = self.config.claim_limit.max(1).min(owner_ids.len());
        let owners = (0..count)
            .filter_map(|offset| {
                self.startup_research_owners
                    .get(&owner_ids[(start + offset) % owner_ids.len()])
                    .cloned()
            })
            .collect::<Vec<_>>();
        self.startup_research_cursor = (start + owners.len())
            % self.startup_research_owners.len().max(1);
        let mut retired = 0;
        let mut first_error = None;
        let repository = crate::db::ResearchRepository::new(&self.db);
        for startup_owner in owners {
            let owner_result = repository.startup_native_owner(
                startup_owner.run_id,
                startup_owner.marker_absent,
            );
            let Some(mut owner) = (match owner_result {
                Ok(owner) => owner,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            }) else {
                self.startup_research_owners.remove(&startup_owner.run_id);
                retired += 1;
                continue;
            };
            let authority_unchanged = startup_owner
                .authority
                .as_ref()
                .zip(owner.authority.as_ref())
                .is_some_and(|(original, current)| {
                    crate::db::native_research_authority_immutable_matches(original, current)
                });
            let authority_progress = startup_owner
                .authority
                .as_ref()
                .zip(owner.authority.as_ref())
                .is_some_and(|(previous, current)| {
                    (!previous.session_confirmed || current.session_confirmed)
                        && (!previous.cleanup_complete || current.cleanup_complete)
                });
            let safety_snapshot_unchanged = authority_unchanged
                && startup_owner.project_id == owner.project_id
                && startup_owner.review_id == owner.review_id
                && startup_owner.pid == owner.pid
                && startup_owner.log_path == owner.log_path;
            let lifecycle_unchanged_or_advanced =
                startup_owner_lifecycle_transition_allowed(&startup_owner, &owner);
            if !safety_snapshot_unchanged
                || !authority_progress
                || !lifecycle_unchanged_or_advanced
            {
                match repository.mark_startup_recovery_required(owner.run_id, now) {
                    Ok(_) => {}
                    Err(error) if first_error.is_none() => first_error = Some(error),
                    Err(_) => {}
                }
                // Keep the original safety evidence and immutable lineage as
                // the only recovery proof.  A changed PID or log path may be
                // a replacement generation, and a changed authority must
                // never become the next poll's baseline.
                self.startup_research_owners
                    .insert(startup_owner.run_id, startup_owner.clone());
                continue;
            }
            // Refresh only the one-poll mutable lifecycle state.  The run,
            // project/review lineage, PID, log path, marker evidence, and
            // strict authority remain pinned to the startup snapshot.
            owner.project_id = startup_owner.project_id.clone();
            owner.review_id = startup_owner.review_id.clone();
            owner.pid = startup_owner.pid;
            owner.log_path = startup_owner.log_path.clone();
            owner.marker_absent = startup_owner.marker_absent;
            owner.original_status = startup_owner.original_status.clone();
            owner.original_gate_state = startup_owner.original_gate_state.clone();
            owner.original_policy_code = startup_owner.original_policy_code.clone();
            owner.original_failure_stage = startup_owner.original_failure_stage.clone();
            self.startup_research_owners.insert(owner.run_id, owner.clone());
            let quiescent = if matches!(
                owner.status.as_str(),
                "completed" | "failed" | "timed_out" | "cancelled"
            ) {
                true
            } else if let Some(pid) = owner.pid {
                #[cfg(unix)]
                {
                    matches!(
                        startup_process_quiescence(pid),
                        StartupProcessQuiescence::Quiescent
                    )
                }
                #[cfg(not(unix))]
                {
                    let _ = pid;
                    false
                }
            } else {
                startup_owner_is_safe_pre_exec(&owner)
            };
            if !quiescent {
                continue;
            }
            let project_result = ProjectRepository::new(&self.db).find_by_id(&owner.project_id);
            let Some(project) = (match project_result {
                Ok(project) => project,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    continue;
                }
            }) else {
                if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                continue;
            };
            let project_config = match config::load(&project.config_path) {
                Ok(config) => config,
                Err(_) => {
                    if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    continue;
                }
            };
            let Some(runner) = self.runner.as_ref() else {
                if first_error.is_none() {
                    first_error = Some(AppError::Runtime {
                        operation: "borrow daemon runner for startup research recovery",
                    });
                }
                break;
            };
            let project_policy = match runner.resolve_project_policy(&project, &project_config) {
                Ok(policy) => policy,
                Err(_) => {
                    if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    continue;
                }
            };
            let project_lock = match runner.try_acquire_project_admission_lock(&project_policy) {
                Ok(lock) => lock,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(AppError::from(error));
                    }
                    continue;
                }
            };
            let Some(_project_lock) = project_lock else {
                continue;
            };
            if owner.pid.is_none() {
                let Some(log_path) = owner.log_path.as_ref() else {
                    if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    continue;
                };
                let marker = runner.inspect_startup_gate_markers(
                    &project,
                    &project_config,
                    &[(owner.run_id, owner.gate_state.clone(), log_path.clone())],
                );
                let marker_absent = match marker {
                    Ok(evidence) => evidence.absent.contains(&owner.run_id),
                    Err(_) => false,
                };
                if !marker_absent {
                    if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    continue;
                }
            }
            let mut cleanup = match owner.authority.as_ref() {
                Some(authority) => {
                    if authority.cleanup_complete {
                        None
                    } else {
                    let verified_root = match project_policy.root_anchor.verify_identity() {
                        Ok(root) => root,
                        Err(_) => {
                            if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                                if first_error.is_none() {
                                    first_error = Some(error);
                                }
                            }
                            continue;
                        }
                    };
                    match RecoveredPrivateRunTempCleanup::open(
                        &verified_root,
                        owner.run_id,
                        &authority.identity,
                    ) {
                        Ok(cleanup) => Some(cleanup),
                        Err(_) => {
                            if let Err(error) = repository.mark_startup_recovery_required(owner.run_id, now) {
                                if first_error.is_none() {
                                    first_error = Some(error);
                                }
                            }
                            continue;
                        }
                    }
                    }
                }
                None => None,
            };
            match repository.retire_startup_native_owner(
                &owner,
                cleanup.as_mut(),
                now,
            ) {
                Ok(true) => {
                    self.startup_research_owners.remove(&owner.run_id);
                    retired += 1;
                }
                Ok(false) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        first_error.map_or(Ok(retired), Err)
    }

    async fn drain_agents_on_shutdown(&mut self) -> Result<usize, AppError> {
        let deadline = Instant::now() + self.config.shutdown_grace_period;
        let mut finished = 0;
        loop {
            let now = self.now()?;
            let mut first_error = None;
            let mut attempts: Vec<Pin<Box<dyn Future<Output = ShutdownAttempt> + Send>>> =
                Vec::new();
            for mut cleanup in std::mem::take(&mut self.active_cleanups) {
                let db = self.db.clone();
                attempts.push(Box::pin(async move {
                    let result = tokio::time::timeout_at(
                        deadline,
                        cleanup.retry_before(&db, now, deadline),
                    )
                        .await
                        .map_err(|_| AppError::Runtime {
                            operation: "bound cleanup exceeded shutdown deadline",
                        })
                        .and_then(|result| result);
                    ShutdownAttempt::Cleanup(cleanup, result)
                }));
            }
            for mut agent in std::mem::take(&mut self.active_agents) {
                let db = self.db.clone();
                attempts.push(Box::pin(async move {
                    let result = tokio::time::timeout_at(
                        deadline,
                        agent.timeout_now_before(&db, now, deadline),
                    )
                        .await
                        .map_err(|_| AppError::Runtime {
                            operation: "agent shutdown exceeded shutdown deadline",
                        })
                        .and_then(|result| result.map(|_| ()));
                    ShutdownAttempt::Agent(agent, result)
                }));
            }
            for attempt in join_owned_attempts(attempts).await {
                match attempt {
                    ShutdownAttempt::Cleanup(cleanup, Ok(())) => {
                        drop(cleanup);
                        finished += 1;
                    }
                    ShutdownAttempt::Cleanup(cleanup, Err(error)) => {
                        self.active_cleanups.push(cleanup);
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    ShutdownAttempt::Agent(agent, Ok(())) => {
                        drop(agent);
                        finished += 1;
                    }
                    ShutdownAttempt::Agent(agent, Err(error)) => {
                        self.active_agents.push(agent);
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                }
            }

            if self.active_cleanups.is_empty() && self.active_agents.is_empty() {
                return Ok(finished);
            }
            if Instant::now() >= deadline {
                return Err(first_error.unwrap_or(AppError::Runtime {
                    operation: "drain retained agent ownership before shutdown deadline",
                }));
            }
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| Duration::from_secs(0));
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
    }

    fn now(&self) -> Result<i64, AppError> {
        if let Some(now) = self.config.now_override {
            return Ok(now);
        }
        unix_timestamp()
    }
}

enum ShutdownAttempt {
    Cleanup(BoundCleanupHandle, Result<(), AppError>),
    Agent(AgentHandle, Result<(), AppError>),
}

async fn join_owned_attempts<'a>(
    mut futures: Vec<Pin<Box<dyn Future<Output = ShutdownAttempt> + Send + 'a>>>,
) -> Vec<ShutdownAttempt> {
    let mut completed = Vec::with_capacity(futures.len());
    poll_fn(move |context| {
        let mut index = 0;
        while index < futures.len() {
            match futures[index].as_mut().poll(context) {
                Poll::Ready(output) => {
                    completed.push(output);
                    drop(futures.swap_remove(index));
                }
                Poll::Pending => index += 1,
            }
        }
        if futures.is_empty() {
            Poll::Ready(std::mem::take(&mut completed))
        } else {
            Poll::Pending
        }
    })
    .await
}

pub async fn cancel_token_on_shutdown_signal(
    shutdown: CancellationToken,
    signal: impl Future<Output = ()>,
) {
    signal.await;
    shutdown.cancel();
}

pub fn production_shutdown_token() -> CancellationToken {
    let shutdown = CancellationToken::new();
    tokio::spawn(cancel_token_on_shutdown_signal(
        shutdown.clone(),
        platform_shutdown_signal(),
    ));
    shutdown
}

#[cfg(unix)]
async fn platform_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let ctrl_c = tokio::signal::ctrl_c();
    let terminate = async {
        match signal(SignalKind::terminate()) {
            Ok(mut signal) => {
                let _ = signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(not(unix))]
async fn platform_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

fn unix_timestamp() -> Result<i64, AppError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| AppError::Runtime {
            operation: "read current daemon time",
        })?
        .as_secs()
        .try_into()
        .map_err(|_| AppError::Runtime {
            operation: "convert current daemon time",
        })
}

fn startup_owner_is_safe_pre_exec(owner: &StartupResearchOwner) -> bool {
    owner.marker_absent
        && owner.pid.is_none()
        && owner.status == "starting"
        && owner.gate_state == "pending"
        && owner.policy_code.is_none()
        && owner.failure_stage.is_none()
}

fn startup_owner_lifecycle_transition_allowed(
    original: &StartupResearchOwner,
    current: &StartupResearchOwner,
) -> bool {
    if original.pid != current.pid || original.log_path != current.log_path {
        return false;
    }
    if original.pid.is_none()
        && startup_owner_is_safe_pre_exec(current)
        && !startup_owner_initially_safe_pre_exec(original)
    {
        return false;
    }
    if !agent_status_transition_allowed(&original.status, &current.status) {
        return false;
    }
    if !launch_gate_transition_allowed(&original.gate_state, &current.gate_state) {
        return false;
    }
    if !research_review_transition_allowed(&original.review_state, &current.review_state) {
        return false;
    }
    if original.failure_code.is_some() && original.failure_code != current.failure_code {
        return false;
    }
    if original.failure_code.is_none()
        && current.failure_code.is_some()
        && !matches!(
            current.review_state.as_str(),
            "retry_wait" | "blocked" | "completed" | "discarded"
        )
    {
        return false;
    }
    for (original_code, current_code) in [
        (&original.policy_code, &current.policy_code),
        (&original.failure_stage, &current.failure_stage),
    ] {
        if original_code.is_some() && original_code != current_code {
            return false;
        }
        if original_code.is_none()
            && current_code.is_some()
            && !matches!(
                current.review_state.as_str(),
                "retry_wait" | "blocked" | "completed" | "discarded"
            )
        {
            return false;
        }
    }
    true
}

fn startup_owner_initially_safe_pre_exec(owner: &StartupResearchOwner) -> bool {
    owner.marker_absent
        && owner.pid.is_none()
        && owner.original_status == "starting"
        && owner.original_gate_state == "pending"
        && owner.original_policy_code.is_none()
        && owner.original_failure_stage.is_none()
}

fn agent_status_transition_allowed(original: &str, current: &str) -> bool {
    if original == current {
        return true;
    }
    match (original, current) {
        ("starting", "running")
        | ("starting", "completed" | "failed" | "timed_out" | "cancelled")
        | ("running", "completed" | "failed" | "timed_out" | "cancelled") => true,
        ("starting" | "running", _) => false,
        ("completed" | "failed" | "timed_out" | "cancelled", _) => false,
        _ => false,
    }
}

fn launch_gate_transition_allowed(original: &str, current: &str) -> bool {
    if original == current {
        return true;
    }
    matches!(
        (original, current),
        ("pending", "release_requested" | "released" | "failed")
            | ("release_requested", "released" | "failed")
    )
}

fn research_review_transition_allowed(original: &str, current: &str) -> bool {
    if original == current {
        return true;
    }
    matches!(
        (original, current),
        ("pending", "running")
            | ("running", "ready" | "retry_wait" | "blocked" | "completed" | "discarded")
            | ("ready", "completed" | "discarded")
            | ("retry_wait", "blocked" | "completed" | "discarded")
            | ("blocked", "completed" | "discarded")
    )
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn startup_owner(
        status: &str,
        gate_state: &str,
        review_state: &str,
        pid: Option<i64>,
        log_path: &str,
    ) -> StartupResearchOwner {
        StartupResearchOwner {
            run_id: 1,
            project_id: "project".to_owned(),
            review_id: "review".to_owned(),
            pid,
            status: status.to_owned(),
            gate_state: gate_state.to_owned(),
            policy_code: None,
            failure_stage: None,
            log_path: Some(PathBuf::from(log_path)),
            review_state: review_state.to_owned(),
            failure_code: None,
            notes_json: None,
            marker_absent: false,
            authority: None,
            original_status: status.to_owned(),
            original_gate_state: gate_state.to_owned(),
            original_policy_code: None,
            original_failure_stage: None,
        }
    }

    #[test]
    fn startup_owner_snapshot_rejects_replaced_pid_or_log_path() {
        let original = startup_owner("running", "pending", "running", Some(41), "/run/a.log");
        let mut replacement = original.clone();
        replacement.pid = Some(42);
        assert!(!startup_owner_lifecycle_transition_allowed(
            &original,
            &replacement
        ));

        replacement = original.clone();
        replacement.log_path = Some(PathBuf::from("/run/replaced.log"));
        assert!(!startup_owner_lifecycle_transition_allowed(
            &original,
            &replacement
        ));
    }

    #[test]
    fn startup_owner_lifecycle_only_advances_and_keeps_preexec_evidence() {
        let original = startup_owner("running", "pending", "running", None, "/run/a.log");
        let mut ready = original.clone();
        ready.review_state = "ready".to_owned();
        ready.gate_state = "released".to_owned();
        assert!(startup_owner_lifecycle_transition_allowed(&original, &ready));

        let mut regressed = ready.clone();
        regressed.review_state = "running".to_owned();
        assert!(!startup_owner_lifecycle_transition_allowed(
            &ready,
            &regressed
        ));

        let terminal = startup_owner("completed", "released", "completed", None, "/run/a.log");
        let mut alternate_terminal = terminal.clone();
        alternate_terminal.status = "failed".to_owned();
        assert!(!startup_owner_lifecycle_transition_allowed(
            &terminal,
            &alternate_terminal
        ));
        let mut failed_gate = terminal.clone();
        failed_gate.gate_state = "failed".to_owned();
        assert!(!startup_owner_lifecycle_transition_allowed(
            &terminal,
            &failed_gate
        ));

        let mut preexec = original.clone();
        preexec.status = "starting".to_owned();
        assert!(!startup_owner_lifecycle_transition_allowed(
            &original,
            &preexec
        ));
    }
}
