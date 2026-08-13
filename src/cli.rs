use std::{ffi::OsString, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

use crate::models::{EventKind, EventStatus, SubmissionKind};

#[derive(Debug, Parser)]
#[command(name = "pueue-agent", about = "SQLite-backed Pueue agent supervisor")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Private supervisor bootstrap entry point. It accepts no options;
    /// launch data arrives over the inherited bootstrap socket.
    #[command(hide = true)]
    InternalLaunch,
    Init(InitArgs),
    Enable(ProjectArgs),
    Disable(DisableArgs),
    Cancel(CancelArgs),
    Submit(SubmitArgs),
    SubmitBatch(SubmitBatchArgs),
    Event(EventArgs),
    Status(StatusArgs),
    Events(EventsArgs),
    Runs(RunsArgs),
    Inspect(InspectArgs),
    Explain(ExplainArgs),
    Doctor(DoctorArgs),
    Pause(ProjectArgs),
    Resume(ProjectArgs),
    Steer(SteerArgs),
    Wake(WakeArgs),
    Version(VersionArgs),
    Upgrade(UpgradeArgs),
    Start(ServiceLifecycleArgs),
    Stop(ServiceLifecycleArgs),
    Daemon(DaemonArgs),
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ProjectArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct StatusArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub compact: bool,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct EventsArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long, value_name = "KIND")]
    pub kind: Option<EventKind>,
    #[arg(long, value_name = "STATUS")]
    pub status: Option<EventStatus>,
    #[arg(long, default_value_t = crate::diagnostics::DEFAULT_EVENT_LIST_LIMIT, value_name = "N")]
    pub limit: usize,
    #[arg(long)]
    pub json: bool,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct RunsArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(long)]
    pub follow: bool,
    #[arg(long, default_value_t = crate::runs::DEFAULT_RUN_LIST_LIMIT, value_name = "N")]
    pub limit: usize,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct InspectArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(value_name = "TASK_ID")]
    pub task_id: i64,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ExplainArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(value_name = "INCIDENT_ID")]
    pub incident_id: i64,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct DoctorArgs {
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct DisableArgs {
    #[arg(long)]
    pub remove: bool,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct CancelArgs {
    #[arg(long, value_name = "TASK_ID")]
    pub task_id: i64,
    #[arg(long)]
    pub json: bool,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[arg(long, value_enum, default_value_t = SubmissionKind::Experiment)]
    pub kind: SubmissionKind,
    #[arg(long, value_name = "PATH", conflicts_with = "metadata_json")]
    pub metadata: Option<PathBuf>,
    #[arg(long, value_name = "JSON", conflicts_with = "metadata")]
    pub metadata_json: Option<String>,
    #[arg(long)]
    pub json: bool,
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "COMMAND"
    )]
    pub command: Vec<OsString>,
}

#[derive(Debug, Args)]
pub struct SubmitBatchArgs {
    #[arg(long, value_name = "UUID")]
    pub request_id: Uuid,
    #[arg(long, value_name = "PATH")]
    pub manifest: PathBuf,
    #[arg(long, value_name = "GROUP")]
    pub group: Option<String>,
    #[arg(long)]
    pub json: bool,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
#[command(subcommand_negates_reqs = true)]
pub struct SteerArgs {
    #[command(subcommand)]
    pub action: Option<SteerAction>,
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "MESSAGE"
    )]
    pub message: Vec<String>,
    #[arg(long, global = true)]
    pub json: bool,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long, value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct WakeArgs {
    #[arg(long, value_name = "TEXT")]
    pub reason: String,
    #[arg(long)]
    pub json: bool,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct VersionArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct UpgradeArgs {
    #[arg(long, value_name = "SOURCE")]
    pub source: Option<PathBuf>,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ServiceLifecycleArgs {
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum SteerAction {
    List(SteerListArgs),
}

#[derive(Debug, Args)]
pub struct SteerListArgs {}

#[derive(Debug, Args)]
pub struct EventArgs {
    #[arg(value_name = "EVENT")]
    pub event: String,
    #[arg(long, value_name = "GROUP")]
    pub group: Option<String>,
    #[arg(long, value_name = "TASK_ID")]
    pub task_id: Option<i64>,
    #[arg(long, default_value = "{}", value_name = "JSON")]
    pub metadata: String,
}

#[derive(Debug, Args, Default)]
pub struct DaemonArgs {
    #[arg(long)]
    pub foreground: bool,
    #[arg(long, value_name = "PUEUE_CONFIG")]
    pub pueue_config: Option<PathBuf>,
}
