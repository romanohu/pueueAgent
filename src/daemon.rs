use std::{collections::BTreeMap, future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::{AgentHandle, AgentRunner},
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
            let policies = self.load_startup_retry_policies()?;
            let recovery = AgentRunRepository::new(&self.db)
                .recover_interrupted(now, DAEMON_RESTART_REASON, &policies)?;
            self.startup_recovery_pending = false;
            report.recovered_agent_runs = recovery.failed_runs;
            report.requeued_agent_events = recovery.requeued_events;
            report.dead_lettered_agent_events = recovery.dead_lettered_events;
        }

        report.finished_agents += self.poll_agents_at(now).await?;

        let reconciliation = Reconciler::new(&self.db, self.pueue.clone())
            .run_once_at(now)
            .await?;
        report.observations = self.run_detection(&reconciliation).await?;
        report.termination_outcomes = self.run_termination().await?;
        report.scheduled_deep_checks = PeriodicDeepCheckScheduler::new(&self.db, now)
            .schedule(&reconciliation.observed_tasks)?;

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
        );
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
                return Err(source);
            }
        };
        self.active_agents.extend(
            scheduler_report
                .started
                .drain(..)
                .map(|started| started.handle),
        );
        report.scheduler = scheduler_report;
        report.finished_agents += self.poll_agents_at(now).await?;
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
        while index < self.active_agents.len() {
            if self.active_agents[index]
                .poll(&self.db, now)
                .await?
                .is_some()
            {
                self.active_agents.swap_remove(index);
                finished += 1;
            } else {
                index += 1;
            }
        }
        Ok(finished)
    }

    async fn drain_agents_on_shutdown(&mut self) -> Result<usize, AppError> {
        let deadline = Instant::now() + self.config.shutdown_grace_period;
        let mut finished = 0;

        while let Some(mut agent) = self.active_agents.pop() {
            loop {
                let now = match self.now() {
                    Ok(now) => now,
                    Err(error) => {
                        self.active_agents.push(agent);
                        return Err(error);
                    }
                };
                match agent.timeout_now(&self.db, now).await {
                    Ok(_) => {
                        finished += 1;
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let remaining = deadline
                            .checked_duration_since(Instant::now())
                            .unwrap_or_else(|| Duration::from_secs(0));
                        tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
                        if Instant::now() >= deadline {
                            self.active_agents.push(agent);
                            return Err(error);
                        }
                    }
                    Err(error) => {
                        self.active_agents.push(agent);
                        return Err(error);
                    }
                }
            }
        }

        Ok(finished)
    }

    fn now(&self) -> Result<i64, AppError> {
        if let Some(now) = self.config.now_override {
            return Ok(now);
        }
        unix_timestamp()
    }
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
