use std::{ffi::OsString, path::PathBuf};

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "pueue-agent", about = "SQLite-backed Pueue agent supervisor")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Init(InitArgs),
    Enable(ProjectArgs),
    Disable(ProjectArgs),
    Submit(SubmitArgs),
    Event(EventArgs),
    Status(ProjectArgs),
    Pause(ProjectArgs),
    Resume(ProjectArgs),
    Daemon(DaemonArgs),
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ProjectArgs {
    #[arg(value_name = "PROJECT_ROOT")]
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct SubmitArgs {
    #[arg(
        required = true,
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "COMMAND"
    )]
    pub command: Vec<OsString>,
}

#[derive(Debug, Args)]
pub struct EventArgs {
    #[arg(value_name = "EVENT")]
    pub event: String,
}

#[derive(Debug, Args, Default)]
pub struct DaemonArgs {
    #[arg(long)]
    pub foreground: bool,
}
