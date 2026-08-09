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
        Command::Daemon(args) => commands::daemon(args).await,
    }
}

mod commands {
    use std::{env, ffi::OsString};

    use pueue_agent::{
        agent::{AgentRunner, AgentRunnerConfig},
        cli::{DaemonArgs, EventArgs, InitArgs, ProjectArgs, SubmitArgs},
        daemon::{Daemon, DaemonConfig},
        db::Db,
        events::{record_callback, CallbackMetadata},
        paths, project,
        pueue::CommandPueue,
        service::{
            enable_with, EnableOptions, PueueConfigCallbackRegistry, ServiceManager, ServicePaths,
        },
        submit as submit_command, AppError,
    };
    use tokio_util::sync::CancellationToken;

    pub fn init(_args: InitArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn enable(args: ProjectArgs) -> Result<(), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = match args.project_root {
            Some(path) => project::find_root(&path)?,
            None => project::find_root(&current_dir)?,
        };
        let service_paths = ServicePaths::from_environment(&project_root, args.pueue_config)?;
        let db = Db::open(&paths::state_db_path()?)?;
        let options = EnableOptions {
            project_root,
            service_paths: service_paths.clone(),
            now: unix_timestamp()?,
        };
        let callbacks = PueueConfigCallbackRegistry::new(&service_paths.pueue_config);
        enable_with(&db, &options, &ServiceManager, &callbacks)
    }

    pub fn disable(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn status(_args: ProjectArgs) -> Result<(), AppError> {
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
        let result = record_callback(group, task_id, metadata)?;
        println!("{}", result.event_id());
        Ok(())
    }

    pub fn pause(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn resume(_args: ProjectArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub async fn daemon(args: DaemonArgs) -> Result<(), AppError> {
        let db = Db::open(&paths::state_db_path()?)?;
        let fixed_args = args
            .pueue_config
            .as_ref()
            .map(|path| vec![OsString::from("--config"), OsString::from(path.as_os_str())])
            .unwrap_or_default();
        let pueue = CommandPueue::new("pueue", fixed_args);
        let mut daemon = Daemon::new(
            db,
            pueue,
            AgentRunner::new(AgentRunnerConfig::production()),
            DaemonConfig::default(),
        );
        daemon.run(CancellationToken::new()).await
    }

    fn unix_timestamp() -> Result<i64, AppError> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| AppError::Runtime {
                operation: "read current command time",
            })?
            .as_secs()
            .try_into()
            .map_err(|_| AppError::Runtime {
                operation: "convert current command time",
            })
    }
}
