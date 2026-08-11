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
