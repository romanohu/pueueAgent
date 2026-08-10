use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use tokio::time::{self, Duration};

use crate::{
    db::{
        Db, RunLineage, RunLineageCursor, RunLineageRepository, SubmissionLineage,
        SubmissionPageCursor,
    },
    diagnostics::JSON_SCHEMA_VERSION,
    models::Project,
    output::{bounded_redacted_text, format_state, human_header, human_summary},
    AppError,
};

pub const DEFAULT_RUN_LIST_LIMIT: usize = 32;
pub const MAX_RUN_LIST_LIMIT: usize = 128;
pub const MAX_FOLLOW_CURSOR_ENTRIES: usize = MAX_RUN_LIST_LIMIT;
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FollowStream {
    Run(i64),
    Event(i64),
}

#[derive(Debug)]
pub struct FollowCursor {
    root_seen: BTreeMap<FollowStream, RunLineageCursor>,
    pending: BTreeSet<RunLineageCursor>,
    submission_after: BTreeMap<i64, SubmissionPageCursor>,
    submission_head: BTreeMap<i64, SubmissionPageCursor>,
    submission_after_task: BTreeMap<i64, Option<i64>>,
    submission_head_task: BTreeMap<i64, Option<i64>>,
    pending_limit: usize,
}

impl Default for FollowCursor {
    fn default() -> Self {
        Self {
            root_seen: BTreeMap::new(),
            pending: BTreeSet::new(),
            submission_after: BTreeMap::new(),
            submission_head: BTreeMap::new(),
            submission_after_task: BTreeMap::new(),
            submission_head_task: BTreeMap::new(),
            pending_limit: MAX_FOLLOW_CURSOR_ENTRIES,
        }
    }
}

impl FollowCursor {
    pub fn observe(&mut self, cursor: &RunLineageCursor) -> bool {
        let stream = stream_for_cursor(cursor);
        if self.root_seen.get(&stream) == Some(cursor) {
            return false;
        }
        if self.pending.len() >= self.pending_limit {
            return false;
        }
        self.root_seen.insert(stream, cursor.clone());
        self.pending.insert(cursor.clone())
    }

    fn begin_batch(&mut self, limit: usize) {
        self.pending_limit = limit.clamp(1, MAX_FOLLOW_CURSOR_ENTRIES);
    }

    fn observe_submission(&mut self, cursor: &RunLineageCursor) -> bool {
        let Some(submission_id) = cursor.submission_id.as_ref() else {
            return false;
        };
        let Some(created_at) = cursor.submission_created_at else {
            return false;
        };
        let page_cursor = SubmissionPageCursor {
            created_at,
            submission_id: submission_id.clone(),
        };
        if let Some(after) = self.submission_after.get(&cursor.run_id) {
            let is_new_head = self
                .submission_head
                .get(&cursor.run_id)
                .map_or(true, |head| page_cursor > *head);
            let is_after_task_update = page_cursor == *after
                && self.submission_after_task.get(&cursor.run_id) != Some(&cursor.task_id);
            let is_head_task_update = self.submission_head.get(&cursor.run_id)
                == Some(&page_cursor)
                && self.submission_head_task.get(&cursor.run_id) != Some(&cursor.task_id);
            if page_cursor >= *after
                && !is_new_head
                && !is_after_task_update
                && !is_head_task_update
            {
                return false;
            }
        }
        if self.pending.contains(cursor) || self.pending.len() >= self.pending_limit {
            return false;
        }
        self.pending.insert(cursor.clone())
    }

    fn advance_submission(&mut self, cursor: &RunLineageCursor) {
        let (Some(submission_id), Some(created_at)) =
            (cursor.submission_id.as_ref(), cursor.submission_created_at)
        else {
            return;
        };
        let page_cursor = SubmissionPageCursor {
            created_at,
            submission_id: submission_id.clone(),
        };
        match self.submission_after.get(&cursor.run_id) {
            Some(after) if page_cursor < *after => {
                self.submission_after
                    .insert(cursor.run_id, page_cursor.clone());
                self.submission_after_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            Some(after) if page_cursor == *after => {
                self.submission_after_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            None => {
                self.submission_after
                    .insert(cursor.run_id, page_cursor.clone());
                self.submission_after_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            Some(_) => {}
        }
        match self.submission_head.get(&cursor.run_id) {
            Some(head) if page_cursor > *head => {
                self.submission_head
                    .insert(cursor.run_id, page_cursor.clone());
                self.submission_head_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            Some(head) if page_cursor == *head => {
                self.submission_head_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            None => {
                self.submission_head.insert(cursor.run_id, page_cursor);
                self.submission_head_task
                    .insert(cursor.run_id, cursor.task_id);
            }
            Some(_) => {}
        }
    }

    fn retain_active(
        &mut self,
        active_runs: &BTreeSet<i64>,
        active_roots: &BTreeSet<FollowStream>,
    ) {
        self.submission_after
            .retain(|run_id, _| active_runs.contains(run_id));
        self.submission_head
            .retain(|run_id, _| active_runs.contains(run_id));
        self.submission_after_task
            .retain(|run_id, _| active_runs.contains(run_id));
        self.submission_head_task
            .retain(|run_id, _| active_runs.contains(run_id));
        self.root_seen
            .retain(|stream, _| active_roots.contains(stream));
        self.pending.retain(|cursor| {
            if cursor.submission_id.is_some() {
                active_runs.contains(&cursor.run_id)
            } else {
                active_roots.contains(&stream_for_cursor(cursor))
            }
        });
    }

    pub fn submission_after(&self) -> &BTreeMap<i64, SubmissionPageCursor> {
        &self.submission_after
    }

    pub fn submission_head(&self) -> &BTreeMap<i64, SubmissionPageCursor> {
        &self.submission_head
    }

    fn has_submission_continuation(&self, run_id: i64) -> bool {
        self.submission_after.contains_key(&run_id)
    }

    pub fn take_ordered(&mut self, limit: usize) -> Vec<RunLineageCursor> {
        let values = self.pending.iter().take(limit).cloned().collect::<Vec<_>>();
        for value in &values {
            self.pending.remove(value);
        }
        values
    }
}

#[derive(Serialize)]
struct RunsReport {
    schema_version: u32,
    project_id: String,
    runs: Vec<RunSummary>,
}

#[derive(Serialize)]
struct RunSummary {
    event: Option<EventSummary>,
    run_id: Option<i64>,
    mode: Option<String>,
    status: Option<String>,
    started_at: i64,
    submissions: Vec<SubmissionSummary>,
}

#[derive(Serialize)]
struct EventSummary {
    event_id: i64,
    kind: String,
    status: String,
}

#[derive(Serialize)]
struct SubmissionSummary {
    submission_id: String,
    kind: String,
    status: String,
    task_id: Option<i64>,
}

pub fn validate_limit(limit: usize) -> Result<usize, AppError> {
    if (1..=MAX_RUN_LIST_LIMIT).contains(&limit) {
        Ok(limit)
    } else {
        Err(AppError::Message {
            message: format!("runs limit must be between 1 and {MAX_RUN_LIST_LIMIT}"),
        })
    }
}

pub fn render_runs(
    db: &Db,
    project: &Project,
    limit: usize,
    json: bool,
) -> Result<String, AppError> {
    let lineages = RunLineageRepository::new(db).list_by_project(&project.project_id, limit)?;
    if json {
        return serde_json::to_string(&RunsReport {
            schema_version: JSON_SCHEMA_VERSION,
            project_id: bounded_redacted_text(&project.project_id),
            runs: lineages.iter().map(RunSummary::from).collect(),
        })
        .map_err(|source| AppError::Serialization {
            operation: "serialize runs diagnostics",
            source,
        });
    }
    Ok(render_human(&project.project_id, &lineages))
}

pub async fn follow_runs(
    db_path: &std::path::Path,
    project: &Project,
    limit: usize,
    json: bool,
) -> Result<(), AppError> {
    let mut cursor = FollowCursor::default();
    let mut interval = time::interval(FOLLOW_INTERVAL);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = interval.tick() => {
                let db = Db::open_read_only(db_path)?;
                let after = cursor.submission_after().clone();
                let head = cursor.submission_head().clone();
                let lineages = RunLineageRepository::new(&db).list_by_project_follow(
                    &project.project_id,
                    limit,
                    &after,
                    &head,
                )?;
                let fresh = collect_fresh(lineages, &mut cursor, limit);
                if fresh.is_empty() {
                    continue;
                }
                let rendered = if json {
                    serde_json::to_string(&RunsReport {
                        schema_version: JSON_SCHEMA_VERSION,
                        project_id: bounded_redacted_text(&project.project_id),
                        runs: fresh.iter().map(RunSummary::from).collect(),
                    }).map_err(|source| AppError::Serialization { operation: "serialize followed runs diagnostics", source })?
                } else {
                    render_human(&project.project_id, &fresh)
                };
                println!("{rendered}");
            }
        }
    }
}

pub fn collect_fresh(
    lineages: Vec<RunLineage>,
    cursor: &mut FollowCursor,
    limit: usize,
) -> Vec<RunLineage> {
    cursor.begin_batch(limit);
    let active_runs = lineages
        .iter()
        .filter_map(|lineage| {
            if !lineage.submissions.is_empty() {
                Some(lineage.run_id.unwrap_or_default())
            } else {
                lineage.run_id
            }
        })
        .collect();
    let active_roots = lineages
        .iter()
        .filter(|lineage| {
            lineage.submissions.is_empty()
                && lineage
                    .run_id
                    .map_or(true, |run_id| !cursor.has_submission_continuation(run_id))
        })
        .filter_map(RunLineage::root_cursor)
        .map(|cursor| stream_for_cursor(&cursor))
        .collect();
    cursor.retain_active(&active_runs, &active_roots);
    let mut cursor_sources = BTreeMap::new();
    for (lineage_index, lineage) in lineages.iter().enumerate() {
        let has_continuation = lineage
            .run_id
            .is_some_and(|run_id| cursor.has_submission_continuation(run_id));
        if lineage.submissions.is_empty() && !has_continuation {
            if let Some(cursor) = lineage.root_cursor() {
                cursor_sources.insert(cursor, (lineage_index, None));
            }
        }
        for (submission_index, submission) in lineage.submissions.iter().enumerate() {
            let lineage_cursor = RunLineageCursor::submission(
                lineage.started_at,
                lineage.run_id.unwrap_or_default(),
                submission.submission_id.clone(),
                submission.created_at,
                submission.pueue_task_id,
            );
            cursor_sources.insert(lineage_cursor, (lineage_index, Some(submission_index)));
        }
    }
    for value in cursor_sources.keys() {
        if value.submission_id.is_none() {
            cursor.observe(value);
        } else {
            cursor.observe_submission(value);
        }
    }

    let selected = cursor.take_ordered(limit);
    let mut fresh = Vec::<(RunLineageCursor, RunLineage)>::new();
    for value in selected {
        cursor.advance_submission(&value);
        let Some(&(lineage_index, submission_index)) = cursor_sources.get(&value) else {
            continue;
        };
        if let Some((_, lineage)) = fresh.iter_mut().find(|(first_cursor, _)| {
            first_cursor.started_at == value.started_at
                && first_cursor.run_id == value.run_id
                && first_cursor.event_id == value.event_id
        }) {
            if let Some(submission_index) = submission_index {
                lineage
                    .submissions
                    .push(lineages[lineage_index].submissions[submission_index].clone());
            }
            continue;
        }
        let mut lineage = lineages[lineage_index].clone();
        lineage.submissions = submission_index
            .map(|index| vec![lineage.submissions[index].clone()])
            .unwrap_or_default();
        fresh.push((value, lineage));
    }
    fresh.into_iter().map(|(_, lineage)| lineage).collect()
}

fn stream_for_cursor(cursor: &RunLineageCursor) -> FollowStream {
    cursor
        .event_id
        .filter(|_| cursor.run_id == 0 && cursor.submission_id.is_none())
        .map_or(FollowStream::Run(cursor.run_id), FollowStream::Event)
}

fn render_human(project_id: &str, lineages: &[RunLineage]) -> String {
    let mut lines = vec![human_header("runs", project_id)];
    lines.push("RUN EVENT MODE STATE EVENT_STATE SUBMISSION TASK".to_owned());
    lines.extend(lineages.iter().map(render_lineage));
    lines.push(human_summary(format!(
        "{} run lineage(s) shown",
        lineages.len()
    )));
    lines.join("\n")
}

fn render_lineage(lineage: &RunLineage) -> String {
    let run = lineage
        .run_id
        .map_or_else(|| "run=none".to_owned(), |id| format!("run={id}"));
    let event = lineage
        .event_id
        .map_or_else(|| "event=none".to_owned(), |id| format!("event={id}"));
    let event_kind = lineage
        .event_kind
        .map(|kind| kind.to_string())
        .unwrap_or_else(|| "none".to_owned());
    let event_state = lineage
        .event_status
        .map(|status| format_state(status.as_str()))
        .unwrap_or_else(|| "none".to_owned());
    let mode = lineage.mode.as_deref().unwrap_or("none");
    let run_state = lineage
        .run_status
        .map(|status| format_state(status.as_str()))
        .unwrap_or_else(|| "none".to_owned());
    let submissions = if lineage.submissions.is_empty() {
        "sub=none task=none".to_owned()
    } else {
        lineage
            .submissions
            .iter()
            .map(render_submission)
            .collect::<Vec<_>>()
            .join(" ")
    };
    format!(
        "{run} {event} mode={mode} state={run_state} event_kind={event_kind} event_state={event_state} {submissions}"
    )
}

fn render_submission(submission: &SubmissionLineage) -> String {
    let task = submission
        .pueue_task_id
        .map_or_else(|| "task=none".to_owned(), |id| format!("task={id}"));
    format!(
        "sub={} sub_kind={} sub_state={} {task}",
        bounded_redacted_text(&submission.submission_id),
        submission.kind,
        format_state(submission.status.as_str())
    )
}

impl From<&RunLineage> for RunSummary {
    fn from(lineage: &RunLineage) -> Self {
        Self {
            event: lineage.event_id.map(|event_id| EventSummary {
                event_id,
                kind: lineage
                    .event_kind
                    .map(|kind| kind.to_string())
                    .unwrap_or_else(|| "unknown".to_owned()),
                status: lineage
                    .event_status
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "unknown".to_owned()),
            }),
            run_id: lineage.run_id,
            mode: lineage.mode.as_deref().map(bounded_redacted_text),
            status: lineage.run_status.map(|status| status.to_string()),
            started_at: lineage.started_at,
            submissions: lineage
                .submissions
                .iter()
                .map(SubmissionSummary::from)
                .collect(),
        }
    }
}

impl From<&SubmissionLineage> for SubmissionSummary {
    fn from(submission: &SubmissionLineage) -> Self {
        Self {
            submission_id: bounded_redacted_text(&submission.submission_id),
            kind: submission.kind.to_string(),
            status: submission.status.to_string(),
            task_id: submission.pueue_task_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{RunLineage, SubmissionLineage},
        models::{SubmissionKind, SubmissionStatus},
    };

    use super::{collect_fresh, FollowCursor};

    fn lineage(run_id: i64, started_at: i64, submission_ids: &[&str]) -> RunLineage {
        RunLineage {
            event_id: Some(run_id),
            event_kind: None,
            event_status: None,
            run_id: Some(run_id),
            mode: None,
            run_status: None,
            started_at,
            submissions: submission_ids
                .iter()
                .map(|submission_id| SubmissionLineage {
                    submission_id: (*submission_id).to_owned(),
                    kind: SubmissionKind::Experiment,
                    status: SubmissionStatus::Pending,
                    created_at: 10,
                    pueue_task_id: None,
                })
                .collect(),
        }
    }

    fn event_only_lineage(event_id: i64, started_at: i64) -> RunLineage {
        RunLineage {
            event_id: Some(event_id),
            event_kind: None,
            event_status: None,
            run_id: None,
            mode: None,
            run_status: None,
            started_at,
            submissions: Vec::new(),
        }
    }

    #[test]
    fn collect_fresh_consumes_pending_cursors_in_order_and_limit_batches() {
        let first = lineage(1, 10, &["sub-a", "sub-b"]);
        let second = lineage(2, 20, &["sub-c"]);
        let mut cursor = FollowCursor::default();

        let first_batch = collect_fresh(vec![second.clone(), first.clone()], &mut cursor, 1);
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].run_id, Some(1));
        assert_eq!(first_batch[0].submissions[0].submission_id, "sub-a");

        let second_batch = collect_fresh(vec![second.clone(), first.clone()], &mut cursor, 1);
        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].run_id, Some(1));
        assert_eq!(second_batch[0].submissions[0].submission_id, "sub-b");

        let third_batch = collect_fresh(vec![second.clone(), first], &mut cursor, 1);
        assert_eq!(third_batch.len(), 1);
        assert_eq!(third_batch[0].run_id, Some(2));
        assert_eq!(third_batch[0].submissions[0].submission_id, "sub-c");

        assert!(collect_fresh(vec![second], &mut cursor, 1).is_empty());
    }

    #[test]
    fn collect_fresh_keeps_same_second_event_only_lineages_distinct() {
        let first = event_only_lineage(41, 100);
        let second = event_only_lineage(42, 100);
        let mut cursor = FollowCursor::default();

        let fresh = collect_fresh(vec![second, first], &mut cursor, 8);

        assert_eq!(fresh.len(), 2);
        assert_eq!(fresh[0].event_id, Some(41));
        assert_eq!(fresh[1].event_id, Some(42));
        assert!(collect_fresh(Vec::new(), &mut cursor, 8).is_empty());
    }

    #[test]
    fn collect_fresh_keeps_cursor_state_bounded_to_the_output_limit() {
        let lineage = RunLineage {
            event_id: Some(1),
            event_kind: None,
            event_status: None,
            run_id: Some(1),
            mode: None,
            run_status: None,
            started_at: 10,
            submissions: (0..1000)
                .map(|index| SubmissionLineage {
                    submission_id: format!("sub-{index:04}"),
                    kind: SubmissionKind::Experiment,
                    status: SubmissionStatus::Pending,
                    created_at: index,
                    pueue_task_id: None,
                })
                .collect(),
        };
        let mut cursor = FollowCursor::default();

        let fresh = collect_fresh(vec![lineage], &mut cursor, 1);

        assert_eq!(fresh.len(), 1);
        assert!(cursor.pending.len() <= 1);
        assert!(cursor.root_seen.len() <= 1);
    }

    #[test]
    fn collect_fresh_accepts_older_after_and_task_updates_at_boundaries() {
        let mut cursor = FollowCursor::default();
        let newest = lineage(1, 10, &["new"]);
        let first = collect_fresh(vec![newest.clone()], &mut cursor, 1);
        assert_eq!(first[0].submissions[0].submission_id, "new");

        let older = RunLineage {
            submissions: vec![SubmissionLineage {
                submission_id: "old".to_owned(),
                created_at: 1,
                kind: SubmissionKind::Experiment,
                status: SubmissionStatus::Pending,
                pueue_task_id: None,
            }],
            ..newest.clone()
        };
        let second = collect_fresh(vec![older], &mut cursor, 1);
        assert_eq!(second[0].submissions[0].submission_id, "old");

        let updated = RunLineage {
            submissions: vec![SubmissionLineage {
                submission_id: "old".to_owned(),
                created_at: 1,
                kind: SubmissionKind::Experiment,
                status: SubmissionStatus::Accepted,
                pueue_task_id: Some(7),
            }],
            ..newest
        };
        let third = collect_fresh(vec![updated], &mut cursor, 1);
        assert_eq!(third[0].submissions[0].pueue_task_id, Some(7));
    }
}
