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
        Command::Enable(args) => commands::enable(args).await,
        Command::Disable(args) => commands::disable(args).await,
        Command::Submit(args) => commands::submit(args).await,
        Command::Event(args) => commands::event(args),
        Command::Status(args) => commands::status(args).await,
        Command::Events(args) => commands::events(args),
        Command::Inspect(args) => commands::inspect(args),
        Command::Explain(args) => commands::explain(args),
        Command::Doctor(args) => commands::doctor(args),
        Command::Pause(args) => commands::pause(args),
        Command::Resume(args) => commands::resume(args),
        Command::Steer(args) => commands::steer(args),
        Command::Daemon(args) => commands::daemon(args).await,
    }
}

mod commands {
    use std::{env, ffi::OsString};

    use pueue_agent::{
        agent::{AgentRunner, AgentRunnerConfig},
        cli::{
            DaemonArgs, DisableArgs, DoctorArgs, EventArgs, EventsArgs, ExplainArgs, InitArgs,
            InspectArgs, ProjectArgs, StatusArgs, SteerAction, SteerArgs, SubmitArgs,
        },
        daemon::{production_shutdown_token, Daemon, DaemonConfig},
        db::{Db, InterventionRepository, ProjectRepository},
        diagnostics::{render_project_status_json, EventFilter, MAX_EVENT_LIST_LIMIT},
        events::{record_callback, CallbackMetadata},
        interventions::{validate_message, InterventionStatus, MAX_INTERVENTIONS_PER_RUN},
        models::Project,
        paths, project,
        pueue::{CommandPueue, PueueApi},
        service::{
            enable_with, EnableOptions, PueueConfigCallbackRegistry, ServiceControl,
            ServiceManager, ServicePaths,
        },
        status::{self as status_command, DisableMode, PueueSnapshot, StatusInput},
        submit as submit_command, AppError,
    };

    pub fn init(args: InitArgs) -> Result<(), AppError> {
        let project_root = match args.project_root {
            Some(path) => path,
            None => env::current_dir().map_err(|source| AppError::Io {
                operation: "read current directory",
                source,
            })?,
        };
        let project_root = pueue_agent::init::run(&project_root)?;
        println!("initialized: {}", project_root.display());
        Ok(())
    }

    pub async fn enable(args: ProjectArgs) -> Result<(), AppError> {
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
        let pueue = CommandPueue::new(
            "pueue",
            vec![
                OsString::from("--config"),
                OsString::from(service_paths.pueue_config.as_os_str()),
            ],
        );
        enable_with(&db, &options, &ServiceManager, &callbacks, &pueue).await
    }

    pub async fn disable(args: DisableArgs) -> Result<(), AppError> {
        let (db, project, service_paths) =
            resolve_project(args.project_root, args.pueue_config.clone())?;
        let pueue = configured_pueue(&service_paths);
        let tasks = match pueue.status_json().await {
            Ok(tasks) => tasks,
            Err(error) if args.remove => return Err(error),
            Err(_) => Vec::new(),
        };
        let mode = if args.remove {
            DisableMode::Remove
        } else {
            DisableMode::KeepReservation
        };
        let project = status_command::disable_project(
            &db,
            &project.project_id,
            mode,
            &tasks,
            unix_timestamp()?,
        )?;
        match mode {
            DisableMode::KeepReservation => {
                println!(
                    "disabled: {} (group reserved: {})",
                    project.project_id, project.pueue_group
                );
            }
            DisableMode::Remove => {
                println!(
                    "removed: {} (group released: {})",
                    project.project_id, project.pueue_group
                );
            }
        }
        Ok(())
    }

    pub async fn status(args: StatusArgs) -> Result<(), AppError> {
        let StatusArgs {
            project_root,
            pueue_config,
            json,
        } = args;
        let (db, project, service_paths) = resolve_project(project_root, pueue_config)?;
        let pueue = configured_pueue(&service_paths);
        let pueue = match pueue.status_json().await {
            Ok(tasks) => PueueSnapshot::Tasks(tasks),
            Err(error) => PueueSnapshot::Error(error.render()),
        };
        let input = StatusInput {
            daemon_health: ServiceManager.status()?,
            pueue,
        };
        let rendered = if json {
            render_project_status_json(&db, &project, &input)?
        } else {
            status_command::render_project_status(&db, &project, &input)?
        };
        println!("{rendered}");
        Ok(())
    }

    pub fn events(args: EventsArgs) -> Result<(), AppError> {
        let limit = validate_event_limit(args.limit)?;
        let _filter = EventFilter::new(args.kind, args.status, limit);
        let _ = (args.project_root, args.pueue_config, args.json);
        Ok(())
    }

    pub fn inspect(args: InspectArgs) -> Result<(), AppError> {
        let _ = (
            args.project_root,
            args.pueue_config,
            args.task_id,
            args.json,
        );
        Ok(())
    }

    pub fn explain(args: ExplainArgs) -> Result<(), AppError> {
        let _ = (
            args.project_root,
            args.pueue_config,
            args.incident_id,
            args.json,
        );
        Ok(())
    }

    pub fn doctor(args: DoctorArgs) -> Result<(), AppError> {
        let _ = (args.project_root, args.pueue_config, args.json);
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

    pub fn pause(args: ProjectArgs) -> Result<(), AppError> {
        let (db, project, _) = resolve_project(args.project_root, args.pueue_config)?;
        let project = status_command::pause_project(&db, &project.project_id, unix_timestamp()?)?;
        println!("paused: {}", project.project_id);
        Ok(())
    }

    pub fn resume(args: ProjectArgs) -> Result<(), AppError> {
        let (db, project, _) = resolve_project(args.project_root, args.pueue_config)?;
        let project = status_command::resume_project(&db, &project.project_id, unix_timestamp()?)?;
        println!("resumed: {}", project.project_id);
        Ok(())
    }

    pub fn steer(args: SteerArgs) -> Result<(), AppError> {
        let SteerArgs {
            action,
            message,
            json,
            pueue_config,
            project_root,
        } = args;
        let (db, project, _) = resolve_project(project_root, pueue_config)?;

        match action {
            Some(SteerAction::List(_)) => {
                let interventions = InterventionRepository::new(&db).list(
                    &project.project_id,
                    InterventionStatus::Pending,
                    MAX_INTERVENTIONS_PER_RUN,
                )?;
                if json {
                    let interventions = interventions
                        .into_iter()
                        .map(|intervention| {
                            serde_json::json!({
                                "intervention_id": intervention.intervention_id,
                                "status": intervention.status.as_str(),
                                "created_at": intervention.created_at,
                                "message": intervention.message,
                            })
                        })
                        .collect::<Vec<_>>();
                    println!(
                        "{}",
                        serde_json::json!({
                            "schema_version": 1,
                            "project_id": project.project_id,
                            "interventions": interventions,
                        })
                    );
                } else {
                    for intervention in interventions {
                        println!(
                            "{}\t{}\t{}\t{}",
                            intervention.intervention_id,
                            intervention.status,
                            intervention.created_at,
                            intervention.message
                        );
                    }
                }
            }
            None => {
                let message = message.join(" ");
                validate_message(&message)?;
                let intervention = InterventionRepository::new(&db).insert_pending(
                    &project.project_id,
                    &message,
                    unix_timestamp()?,
                )?;
                if json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "schema_version": 1,
                            "intervention_id": intervention.intervention_id,
                            "project_id": intervention.project_id,
                            "status": intervention.status.as_str(),
                        })
                    );
                } else {
                    println!("queued intervention: {}", intervention.intervention_id);
                }
            }
        }

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
        daemon.run(production_shutdown_token()).await
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

    fn resolve_project(
        project_root: Option<std::path::PathBuf>,
        pueue_config: Option<std::path::PathBuf>,
    ) -> Result<(Db, Project, ServicePaths), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = match project_root {
            Some(path) => project::find_root(&path)?,
            None => project::find_root(&current_dir)?,
        };
        let service_paths = ServicePaths::from_environment(&project_root, pueue_config)?;
        let db = Db::open(&paths::state_db_path()?)?;
        let project = ProjectRepository::new(&db)
            .find_by_root(&project_root)?
            .ok_or(AppError::Runtime {
                operation: "find registered project",
            })?;
        Ok((db, project, service_paths))
    }

    fn configured_pueue(service_paths: &ServicePaths) -> CommandPueue {
        CommandPueue::new(
            "pueue",
            vec![
                OsString::from("--config"),
                OsString::from(service_paths.pueue_config.as_os_str()),
            ],
        )
    }

    fn validate_event_limit(limit: usize) -> Result<usize, AppError> {
        if (1..=MAX_EVENT_LIST_LIMIT).contains(&limit) {
            Ok(limit)
        } else {
            Err(AppError::Message {
                message: format!(
                    "diagnostic event limit must be between 1 and {MAX_EVENT_LIST_LIMIT}"
                ),
            })
        }
    }
}
