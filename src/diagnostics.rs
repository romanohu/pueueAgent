use crate::models::{EventKind, EventStatus};

pub const JSON_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_SUMMARY_LIMIT: usize = 8;
pub const DEFAULT_EVENT_LIST_LIMIT: usize = 100;
pub const MAX_EVENT_LIST_LIMIT: usize = 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventFilter {
    pub kind: Option<EventKind>,
    pub status: Option<EventStatus>,
    pub limit: usize,
}

impl EventFilter {
    pub fn new(kind: Option<EventKind>, status: Option<EventStatus>, limit: usize) -> Self {
        Self {
            kind,
            status,
            limit,
        }
    }
}
