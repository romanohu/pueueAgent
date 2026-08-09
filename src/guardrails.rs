use crate::{
    config::GuardrailsConfig,
    db::{AgentRunRepository, EventRepository, ProjectRepository, SubmissionRepository},
    models::{Event, Project},
    AppError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchDecision {
    Allow,
    Pause(String),
    Halt(String),
}

pub struct Guardrails<'db> {
    db: &'db crate::db::Db,
    now: i64,
}

impl<'db> Guardrails<'db> {
    pub fn new(db: &'db crate::db::Db, now: i64) -> Self {
        Self { db, now }
    }

    pub fn check(
        &self,
        project: &Project,
        config: &GuardrailsConfig,
        event_batch: &[Event],
    ) -> Result<DispatchDecision, AppError> {
        if project.halted_reason.is_some() {
            return Ok(DispatchDecision::Halt(
                project.halted_reason.clone().unwrap_or_default(),
            ));
        }
        if project.paused {
            return Ok(DispatchDecision::Pause("project is paused".to_owned()));
        }

        let agent_runs = AgentRunRepository::new(self.db).count_by_project(&project.project_id)?;
        if agent_runs >= config.max_agent_runs {
            return Ok(DispatchDecision::Halt(format!(
                "max_agent_runs reached: {agent_runs}/{}",
                config.max_agent_runs
            )));
        }

        let experiments =
            SubmissionRepository::new(self.db).count_started_or_accepted(&project.project_id)?;
        if experiments >= config.max_experiments {
            return Ok(DispatchDecision::Pause(format!(
                "max_experiments reached: {experiments}/{}",
                config.max_experiments
            )));
        }

        let consecutive_failures = EventRepository::new(self.db).count_consecutive_failures(
            &project.project_id,
            config.max_consecutive_failures as usize + event_batch.len() + 1,
        )?;
        if consecutive_failures >= config.max_consecutive_failures {
            return Ok(DispatchDecision::Halt(format!(
                "max_consecutive_failures reached: {consecutive_failures}/{} at {}",
                config.max_consecutive_failures, self.now
            )));
        }

        Ok(DispatchDecision::Allow)
    }

    pub fn apply_pause(&self, project_id: &str) -> Result<Project, AppError> {
        ProjectRepository::new(self.db).pause(project_id, self.now)
    }

    pub fn apply_halt(&self, project_id: &str, reason: &str) -> Result<Project, AppError> {
        ProjectRepository::new(self.db).halt(project_id, reason, self.now)
    }
}
