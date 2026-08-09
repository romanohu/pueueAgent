use std::process::ExitCode;

use clap::Parser;
use pueue_agent::{
    cli::{Cli, Command},
    AppError,
};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            let _ = error.print();
            return if exit_code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            };
        }
    };

    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", error.render());
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::Init(args) => commands::init(args),
        Command::Enable(args) => commands::enable(args),
        Command::Disable(args) => commands::disable(args),
        Command::Submit(args) => commands::submit(args).await,
        Command::Event(args) => commands::event(args),
        Command::Status(args) => commands::status(args),
        Command::Pause(args) => commands::pause(args),
        Command::Resume(args) => commands::resume(args),
        Command::Daemon(args) => commands::daemon(args),
    }
}

mod commands {
    use std::env;

    use pueue_agent::{
        cli::{DaemonArgs, EventArgs, InitArgs, ProjectArgs, SubmitArgs},
        events::{record_callback, CallbackMetadata},
        project, submit as submit_command, AppError,
    };

    pub fn init(_args: InitArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn enable(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn disable(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub async fn submit(args: SubmitArgs) -> Result<(), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = project::find_root(&current_dir)?;
        let submission = submit_command::run(&project_root, &args.command).await?;
        if let Some(task_id) = submission.pueue_task_id {
            println!("{task_id}");
        }
        Ok(())
    }

    pub fn event(args: EventArgs) -> Result<(), AppError> {
        if args.event != "callback" {
            return Ok(());
        }
        let group = args.group.as_deref().ok_or(AppError::Configuration {
            field: "callback.group",
        })?;
        let task_id = args.task_id.ok_or(AppError::Configuration {
            field: "callback.task_id",
        })?;
        let metadata =
            serde_json::from_str::<CallbackMetadata>(&args.metadata).map_err(|source| {
                AppError::Serialization {
                    operation: "parse callback metadata",
                    source,
                }
            })?;
        let event_id = record_callback(group, task_id, metadata)?;
        println!("{event_id}");
        Ok(())
    }

    pub fn status(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn pause(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn resume(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn daemon(_args: DaemonArgs) -> Result<(), AppError> {
        Ok(())
    }
}
