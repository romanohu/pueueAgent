use std::{collections::BTreeMap, future::Future, time::Duration};

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    agent::{AgentHandle, AgentRunner},
    config,
    db::{Db, ProjectRepository, TerminationRequestRepository},
    detect::Detector,
    incidents::IncidentStore,
    pueue::PueueApi,
    reconcile::{ReconcileReport, Reconciler},
    scheduler::{Scheduler, SchedulerConfig, SchedulerReport},
    termination::{TerminationManager, TerminationOutcome},
    AppError,
};

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
    pub scheduler: SchedulerReport,
    pub finished_agents: usize,
}

pub struct Daemon<P> {
    db: Db,
    pueue: P,
    runner: Option<AgentRunner>,
    config: DaemonConfig,
    active_agents: Vec<AgentHandle>,
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
        }
    }

    pub async fn run(&mut self, shutdown: CancellationToken) -> Result<(), AppError> {
        self.run_once().await?;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    self.drain_agents_on_shutdown().await?;
                    return Ok(());
                }
                () = tokio::time::sleep(self.config.interval) => {
                    self.run_once().await?;
                }
            }
        }
    }

    pub async fn run_once(&mut self) -> Result<DaemonReport, AppError> {
        let mut report = DaemonReport::default();

        report.finished_agents += self.poll_agents().await?;

        let reconciliation = Reconciler::new(&self.db, self.pueue.clone())
            .run_once()
            .await?;
        report.observations = self.run_detection(&reconciliation).await?;
        report.termination_outcomes = self.run_termination().await?;

        let mut scheduler = Scheduler::new(
            self.db.clone(),
            self.runner.take().ok_or(AppError::Runtime {
                operation: "take daemon scheduler runner",
            })?,
            SchedulerConfig {
                now: self.now()?,
                lease_seconds: self.config.lease_seconds,
                claim_limit: self.config.claim_limit,
            },
        );
        let mut scheduler_report = scheduler.tick().await?;
        self.runner = Some(scheduler.into_runner());
        self.active_agents.extend(
            scheduler_report
                .started
                .drain(..)
                .map(|started| started.handle),
        );
        report.scheduler = scheduler_report;
        report.finished_agents += self.poll_agents().await?;
        report.reconciliation = reconciliation;
        Ok(report)
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
            );
            for observation in detector.inspect_task(task, &project_config.check)? {
                IncidentStore::new(&self.db).observe(observation)?;
                observations += 1;
            }
        }

        Ok(observations)
    }

    async fn run_termination(&self) -> Result<Vec<TerminationOutcome>, AppError> {
        let mut outcomes = Vec::new();
        for project in ProjectRepository::new(&self.db).list_enabled()? {
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

    async fn poll_agents(&mut self) -> Result<usize, AppError> {
        let now = self.now()?;
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
        let mut finished = self.poll_agents().await?;
        if self.active_agents.is_empty() {
            return Ok(finished);
        }

        let deadline = Instant::now() + self.config.shutdown_grace_period;
        while !self.active_agents.is_empty() && Instant::now() < deadline {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| Duration::from_secs(0));
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
            finished += self.poll_agents().await?;
        }

        while let Some(mut agent) = self.active_agents.pop() {
            agent.timeout_now(&self.db, self.now()?).await?;
            finished += 1;
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
