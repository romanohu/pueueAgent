use serde_json::json;

use crate::{
    config,
    db::{AgentRunRepository, Db, EventRepository, ProjectRepository},
    models::{EventKind, NewEvent},
    pueue::PueueTask,
    reconcile::parse_timestamp,
    AppError,
};

const PERIODIC_DEEP_CHECK_DEDUP_PREFIX: &str = "periodic-deep-check:v1:";
const MAX_PERIODIC_TASK_IDS: usize = 64;

pub struct PeriodicDeepCheckScheduler<'db> {
    db: &'db Db,
    now: i64,
}

impl<'db> PeriodicDeepCheckScheduler<'db> {
    pub fn new(db: &'db Db, now: i64) -> Self {
        Self { db, now }
    }

    pub fn schedule(&self, tasks: &[PueueTask]) -> Result<usize, AppError> {
        let projects = ProjectRepository::new(self.db).list_enabled()?;
        let events = EventRepository::new(self.db);
        let agents = AgentRunRepository::new(self.db);
        let mut scheduled = 0;

        for project in projects {
            let check = config::load(&project.config_path)?.check;
            let running_tasks = tasks
                .iter()
                .filter(|task| task.group == project.pueue_group && task.is_running())
                .collect::<Vec<_>>();
            let oldest_running_task_started_at = running_tasks
                .iter()
                .filter_map(|task| {
                    Some(
                        task.started_at
                            .as_deref()
                            .and_then(parse_timestamp)
                            .unwrap_or(self.now),
                    )
                })
                .min();
            let interval_seconds = i64::from(check.deep_check_interval_minutes) * 60;
            let input = DeepCheckScheduleInput {
                interval_minutes: check.deep_check_interval_minutes,
                now: self.now,
                oldest_running_task_started_at,
                last_scheduled_at: events.latest_periodic_deep_check_at(&project.project_id)?,
                project_active: !project.paused && project.halted_reason.is_none(),
                has_running_task: !running_tasks.is_empty(),
                has_active_agent: agents.find_active_by_project(&project.project_id)?.is_some(),
                has_open_event: events.has_open_periodic_deep_check(&project.project_id)?,
            };
            if !should_schedule_deep_check(&input) {
                continue;
            }

            let mut task_ids = running_tasks.iter().map(|task| task.id).collect::<Vec<_>>();
            task_ids.sort_unstable();
            task_ids.truncate(MAX_PERIODIC_TASK_IDS);
            let dedup_key = format!(
                "{PERIODIC_DEEP_CHECK_DEDUP_PREFIX}{}",
                periodic_bucket(self.now, interval_seconds)
            );
            if events
                .find_by_dedup_key(&project.project_id, &dedup_key)?
                .is_some()
            {
                continue;
            }
            let event = NewEvent::new(
                &project.project_id,
                EventKind::DeepCheck,
                dedup_key,
                json!({
                    "source": "periodic",
                    "task_ids": task_ids,
                    "task_count": running_tasks.len(),
                    "scheduled_at": self.now,
                }),
                self.now,
                self.now,
            );
            let _ = events.insert_idempotent(&event)?;
            scheduled += 1;
        }

        Ok(scheduled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeepCheckScheduleInput {
    pub interval_minutes: u32,
    pub now: i64,
    pub oldest_running_task_started_at: Option<i64>,
    pub last_scheduled_at: Option<i64>,
    pub project_active: bool,
    pub has_running_task: bool,
    pub has_active_agent: bool,
    pub has_open_event: bool,
}

pub fn should_schedule_deep_check(input: &DeepCheckScheduleInput) -> bool {
    if input.interval_minutes == 0
        || !input.project_active
        || !input.has_running_task
        || input.has_active_agent
        || input.has_open_event
    {
        return false;
    }

    let interval = i64::from(input.interval_minutes) * 60;
    let anchor = input
        .last_scheduled_at
        .or(input.oldest_running_task_started_at)
        .unwrap_or(input.now);
    input.now.saturating_sub(anchor) >= interval
}

pub fn periodic_bucket(now: i64, interval_seconds: i64) -> i64 {
    now.div_euclid(interval_seconds.max(1))
}

#[cfg(test)]
mod tests {
    use super::{periodic_bucket, should_schedule_deep_check, DeepCheckScheduleInput};

    #[test]
    fn periodic_check_requires_active_running_task_and_enabled_interval() {
        let input = DeepCheckScheduleInput {
            interval_minutes: 30,
            now: 3_601,
            oldest_running_task_started_at: Some(1_801),
            last_scheduled_at: None,
            project_active: true,
            has_running_task: true,
            has_active_agent: false,
            has_open_event: false,
        };

        assert!(should_schedule_deep_check(&input));
        assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
            has_running_task: false,
            ..input
        }));
    }

    #[test]
    fn periodic_check_waits_for_interval_and_skips_active_or_open_work() {
        let base = DeepCheckScheduleInput {
            interval_minutes: 30,
            now: 2_000,
            oldest_running_task_started_at: Some(1_000),
            last_scheduled_at: Some(1_900),
            project_active: true,
            has_running_task: true,
            has_active_agent: false,
            has_open_event: false,
        };

        assert!(!should_schedule_deep_check(&base));
        assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
            has_active_agent: true,
            ..base
        }));
        assert!(!should_schedule_deep_check(&DeepCheckScheduleInput {
            has_open_event: true,
            ..base
        }));
        assert!(should_schedule_deep_check(&DeepCheckScheduleInput {
            now: 3_701,
            ..base
        }));
    }

    #[test]
    fn periodic_bucket_is_stable_for_the_same_interval() {
        assert_eq!(periodic_bucket(3_599, 1_800), 1);
        assert_eq!(periodic_bucket(3_600, 1_800), 2);
    }
}
