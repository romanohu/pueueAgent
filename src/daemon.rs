use std::{
    collections::{BTreeMap, BTreeSet},
    future::{poll_fn, Future},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::{AgentHandle, AgentRunner, BoundCleanupHandle},
    config,
    db::{AgentRunRepository, Db, ProjectRepository, TerminationRequestRepository},
    detect::Detector,
    incidents::IncidentStore,
    pueue::PueueApi,
    periodic::PeriodicDeepCheckScheduler,
    reconcile::{ReconcileReport, Reconciler},
    retry::RetryPolicy,
    scheduler::{Scheduler, SchedulerConfig, SchedulerReport},
    termination::{TerminationManager, TerminationOutcome},
    AppError,
};

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
    pub termination_outcomes: Vec<TerminationOutcome>,
    pub scheduled_deep_checks: usize,
    pub scheduler: SchedulerReport,
    pub finished_agents: usize,
    pub recovered_agent_runs: usize,
    pub requeued_agent_events: usize,
    pub dead_lettered_agent_events: usize,
}

pub struct Daemon<P> {
    db: Db,
    pueue: P,
    runner: Option<AgentRunner>,
    config: DaemonConfig,
    active_agents: Vec<AgentHandle>,
    active_cleanups: Vec<BoundCleanupHandle>,
    startup_recovery_pending: bool,
}

impl<P> Daemon<P>
where
    P: PueueApi + Clone,
{
    pub fn new(db: Db, pueue: P, runner: AgentRunner, config: DaemonConfig) -> Self {
        Self {
            db,
            pueue,
            runner: Some(runner),
            config,
            active_agents: Vec::new(),
            active_cleanups: Vec::new(),
            startup_recovery_pending: true,
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
            let (policies, confirmed_pending_marker_ids, confirmed_release_requested_ids) =
                self.load_startup_recovery_inputs()?;
            let recovery = AgentRunRepository::new(&self.db)
                .recover_interrupted(
                    now,
                    DAEMON_RESTART_REASON,
                    &policies,
                    &confirmed_pending_marker_ids,
                    &confirmed_release_requested_ids,
                )?;
            self.startup_recovery_pending = false;
            report.recovered_agent_runs = recovery.failed_runs;
            report.requeued_agent_events = recovery.requeued_events;
            report.dead_lettered_agent_events = recovery.dead_lettered_events;
        }

        report.finished_agents += self.poll_retained_ownership_at(now).await?;

        let reconciliation = Reconciler::new(&self.db, self.pueue.clone())
            .run_once_at(now)
            .await?;
        report.observations = self.run_detection(&reconciliation).await?;
        report.termination_outcomes = self.run_termination().await?;
        report.scheduled_deep_checks = PeriodicDeepCheckScheduler::new(&self.db, now)
            .schedule(&reconciliation.observed_tasks)?;

        let cleanup_blocked_projects = self
            .active_agents
            .iter()
            .filter_map(|agent| agent.cleanup_blocked_project().map(str::to_owned))
            .chain(
                self.active_cleanups
                    .iter()
                    .filter_map(|cleanup| cleanup.cleanup_blocked_project().map(str::to_owned)),
            )
            .collect::<BTreeSet<_>>();
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
                let confirmed = runner.inspect_startup_gate_markers(
                    &project,
                    &project_config,
                    &candidates,
                )?;
                for (run_id, gate_state, _) in candidates {
                    if !confirmed.contains(&run_id) {
                        continue;
                    }
                    match gate_state.as_str() {
                        "pending" => {
                            confirmed_pending_marker_ids.insert(run_id);
                        }
                        "release_requested" => {
                            confirmed_release_requested_ids.insert(run_id);
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
        ))
    }

    async fn run_detection(&self, reconciliation: &ReconcileReport) -> Result<usize, AppError> {
        let projects = ProjectRepository::new(&self.db).list_enabled()?;
        let projects_by_group = projects
            .iter()
            .map(|project| (project.pueue_group.as_str(), project))
            .collect::<BTreeMap<_, _>>();
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
            for observation in detector.inspect_task(task, &project_config.check)? {
                IncidentStore::new(&self.db).observe(observation)?;
                observations += 1;
            }
        }

        Ok(observations)
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
        let agents = self.poll_agents_at(now).await;
        let cleanups = self.poll_cleanups_at(now).await;
        match (agents, cleanups) {
            (Ok(finished_agents), Ok(_finished_cleanups)) => Ok(finished_agents),
            (Err(first_error), _) => Err(first_error),
            (Ok(_), Err(first_error)) => Err(first_error),
        }
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
