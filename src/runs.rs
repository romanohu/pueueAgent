use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::Serialize;
use tokio::time::{self, Duration};

use crate::{
    db::{Db, RunLineage, RunLineageCursor, RunLineageRepository, SubmissionLineage},
    diagnostics::JSON_SCHEMA_VERSION,
    models::Project,
    output::{bounded_redacted_text, format_state},
    AppError,
};

pub const DEFAULT_RUN_LIST_LIMIT: usize = 32;
pub const MAX_RUN_LIST_LIMIT: usize = 128;
const FOLLOW_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Default)]
pub struct FollowCursor {
    seen: HashSet<RunLineageCursor>,
    pending: BTreeSet<RunLineageCursor>,
}

impl FollowCursor {
    pub fn observe(&mut self, cursor: &RunLineageCursor) -> bool {
        if !self.seen.insert(cursor.clone()) {
            return false;
        }
        self.pending.insert(cursor.clone())
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
                let lineages = RunLineageRepository::new(&db).list_by_project(&project.project_id, limit)?;
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
    let mut cursor_sources = BTreeMap::new();
    for (lineage_index, lineage) in lineages.iter().enumerate() {
        if lineage.run_id.is_some() || lineage.event_id.is_some() {
            cursor_sources.insert(
                RunLineageCursor::new(
                    lineage.started_at,
                    lineage.run_id.unwrap_or_default(),
                    None,
                    None,
                ),
                (lineage_index, None),
            );
        }
        for (submission_index, submission) in lineage.submissions.iter().enumerate() {
            cursor_sources.insert(
                RunLineageCursor::new(
                    lineage.started_at,
                    lineage.run_id.unwrap_or_default(),
                    Some(submission.submission_id.clone()),
                    submission.pueue_task_id,
                ),
                (lineage_index, Some(submission_index)),
            );
        }
    }
    for value in cursor_sources.keys() {
        cursor.observe(value);
    }

    let selected = cursor.take_ordered(limit);
    let mut fresh = Vec::<(RunLineageCursor, RunLineage)>::new();
    for value in selected {
        let Some(&(lineage_index, submission_index)) = cursor_sources.get(&value) else {
            continue;
        };
        if let Some((_, lineage)) = fresh.iter_mut().find(|(first_cursor, _)| {
            first_cursor.started_at == value.started_at && first_cursor.run_id == value.run_id
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

fn render_human(project_id: &str, lineages: &[RunLineage]) -> String {
    let mut lines = vec![format!(
        "pueue-agent runs project={} showing={}",
        bounded_redacted_text(project_id),
        lineages.len()
    )];
    lines.push("RUN EVENT EVENT_KIND EVENT_STATE MODE RUN_STATE SUBMISSION TASK".to_owned());
    lines.extend(lineages.iter().map(render_lineage));
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
        "{run} {event} event_kind={event_kind} event_state={event_state} mode={mode} run_state={run_state} {submissions}"
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
                    pueue_task_id: None,
                })
                .collect(),
        }
    }

    #[test]
    fn collect_fresh_consumes_pending_cursors_in_order_and_limit_batches() {
        let first = lineage(1, 10, &["sub-a", "sub-b"]);
        let second = lineage(2, 20, &["sub-c"]);
        let mut cursor = FollowCursor::default();

        let first_batch = collect_fresh(vec![second.clone(), first.clone()], &mut cursor, 2);
        assert_eq!(first_batch.len(), 1);
        assert_eq!(first_batch[0].run_id, Some(1));
        assert_eq!(first_batch[0].submissions[0].submission_id, "sub-a");

        let second_batch = collect_fresh(vec![second.clone(), first.clone()], &mut cursor, 2);
        assert_eq!(second_batch.len(), 2);
        assert_eq!(second_batch[0].run_id, Some(1));
        assert_eq!(second_batch[0].submissions[0].submission_id, "sub-b");
        assert_eq!(second_batch[1].run_id, Some(2));
        assert!(second_batch[1].submissions.is_empty());

        let third_batch = collect_fresh(vec![second.clone(), first], &mut cursor, 2);
        assert_eq!(third_batch.len(), 1);
        assert_eq!(third_batch[0].run_id, Some(2));
        assert_eq!(third_batch[0].submissions[0].submission_id, "sub-c");

        assert!(collect_fresh(vec![second], &mut cursor, 2).is_empty());
    }
}
