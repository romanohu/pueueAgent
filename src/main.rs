use std::process::ExitCode;

use clap::{error::ErrorKind, Parser};
use pueue_agent::{
    cli::{Cli, Command},
    output::bounded_redacted_text,
    AppError,
};

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = error.exit_code();
            match error.kind() {
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion => {
                    let _ = error.print();
                }
                _ => eprintln!("{}", bounded_redacted_text(&error.to_string())),
            }
            return if exit_code == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            };
        }
    };

    // The hidden helper must not initialize ordinary command state. It only
    // consumes the fixed inherited bootstrap ABI.
    if matches!(cli.command, Command::InternalLaunch) {
        return match pueue_agent::process::run_internal_launch() {
            Ok(()) => ExitCode::SUCCESS,
            #[cfg(unix)]
            Err(pueue_agent::process::BootstrapError::TargetExit(code)) => ExitCode::from(code),
            Err(_) => ExitCode::FAILURE,
        };
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => return ExitCode::FAILURE,
    };
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", bounded_redacted_text(&error.render()));
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::InternalLaunch => Err(AppError::Runtime {
            operation: "internal launch dispatch",
        }),
        Command::Init(args) => commands::init(args),
        Command::Enable(args) => commands::enable(args).await,
        Command::Disable(args) => commands::disable(args).await,
        Command::Cancel(args) => commands::cancel(args).await,
        Command::Submit(args) => commands::submit(args).await,
        Command::SubmitBatch(args) => commands::submit_batch(args).await,
        Command::Event(args) => commands::event(args).await,
        Command::Status(args) => commands::status(args).await,
        Command::Events(args) => commands::events(args),
        Command::Runs(args) => commands::runs(args).await,
        Command::Inspect(args) => commands::inspect(args),
        Command::Explain(args) => commands::explain(args),
        Command::Doctor(args) => commands::doctor(args).await,
        Command::Pause(args) => commands::pause(args),
        Command::Resume(args) => commands::resume(args),
        Command::Steer(args) => commands::steer(args),
        Command::Wake(args) => commands::wake(args),
        Command::Version(args) => commands::version(args),
        Command::Upgrade(args) => commands::upgrade(args).await,
        Command::Start(args) => commands::start(args),
        Command::Stop(args) => commands::stop(args),
        Command::Daemon(args) => commands::daemon(args).await,
        Command::Campaign(args) => commands::campaign(args),
        Command::Proposal(args) => commands::proposal(args),
        Command::Experiment(args) => commands::experiment(args),
    }
}

mod commands {
    use std::{env, path::PathBuf, sync::Arc};

    use pueue_agent::{
        agent::{AgentRunner, AgentRunnerConfig},
        cancel::{cancel_task_with, render_cancel_result},
        cli::{
            CampaignAction, CampaignArgs, CancelArgs, DaemonArgs, DisableArgs, DoctorArgs,
            EventArgs, EventsArgs, ExperimentAction, ExperimentArgs, ExplainArgs, InitArgs,
            InspectArgs, ProjectArgs, ProposalAction, ProposalArgs, RunsArgs,
            ServiceLifecycleArgs, StatusArgs, SteerAction, SteerArgs, SubmitArgs, SubmitBatchArgs,
            UpgradeArgs, VersionArgs, WakeArgs,
        },
        daemon::{production_shutdown_token, Daemon, DaemonConfig},
        db::{CampaignRepository, Db, InterventionRepository, ProjectRepository},
        diagnostics::{
            build_doctor_report_with_policy_and_roots, render_doctor_report_value,
            render_events, render_incident_explanation, render_project_status_json,
            render_task_inspection, DoctorExternal, EventFilter, MAX_EVENT_LIST_LIMIT,
        },
        events::{
            callback_group_for_task, record_callback, record_operator_wake_with, CallbackMetadata,
        },
        execution_policy::{
            load_existing_policy, load_or_create_policy, ResolvedExecutionPolicy,
        },
        interventions::{validate_message, InterventionStatus, MAX_INTERVENTIONS_PER_RUN},
        models::Project,
        output::{bounded_redacted_text, format_state, human_header, human_summary},
        paths, project,
        pueue::{configured_pueue, PueueApi},
        service::{
            enable_with, CallbackRegistry, EnableOptions,
            PueueConfigCallbackRegistry, ServiceControl, ServiceManager, ServicePaths,
            ServiceStatus,
        },
        status::{self as status_command, DisableMode, PueueSnapshot, StatusInput},
        submit as submit_command,
        upgrade::{
            self, resolve_source_root, ProcessUpgradeCommandRunner, ReloadingPueueHealth,
            UpgradeRunner,
        },
        version::{self, BuildInfo},
        AppError,
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
        let project_root = project_root.to_string_lossy();
        println!("initialized: {}", bounded_redacted_text(&project_root));
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
        let _ = pueue_agent::config::load(&project_root.join(".pueue-agent/config.toml"))?;
        let state_db = paths::state_db_path()?;
        let mut project_roots = registered_roots_if_present(&state_db)?;
        if !project_roots.iter().any(|root| root == &project_root) {
            project_roots.push(project_root.clone());
        }
        let policy = Arc::new(load_or_create_policy(&service_paths.policy_load_input(
            project_roots,
            current_launcher_path()?,
        ))?);
        let service_paths = service_paths.pin_to_policy(&policy)?;
        let pueue = configured_pueue(Arc::clone(&policy))?;
        let db = Db::open(&state_db)?;
        let options = EnableOptions {
            project_root,
            service_paths: service_paths.clone(),
            now: unix_timestamp()?,
        };
        let callbacks = PueueConfigCallbackRegistry::new(&service_paths.pueue_config);
        enable_with(
            &db,
            &options,
            policy.as_ref(),
            &ServiceManager,
            &callbacks,
            &pueue,
        )
        .await
    }

    pub async fn disable(args: DisableArgs) -> Result<(), AppError> {
        let (db, project, _service_paths, policy) =
            resolve_project(args.project_root, args.pueue_config.clone())?;
        let pueue = configured_pueue(policy)?;
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
                    bounded_redacted_text(&project.project_id),
                    bounded_redacted_text(&project.pueue_group)
                );
            }
            DisableMode::Remove => {
                println!(
                    "removed: {} (group released: {})",
                    bounded_redacted_text(&project.project_id),
                    bounded_redacted_text(&project.pueue_group)
                );
            }
        }
        Ok(())
    }

    pub async fn cancel(args: CancelArgs) -> Result<(), AppError> {
        let CancelArgs {
            task_id,
            json,
            pueue_config,
            project_root,
        } = args;
        let (db, project, _service_paths, policy) =
            resolve_project(project_root, pueue_config)?;
        let pueue = configured_pueue(policy)?;
        let result = cancel_task_with(&db, &project, &pueue, task_id, unix_timestamp()?).await?;
        println!("{}", render_cancel_result(&project, &result, json));
        Ok(())
    }

    pub async fn status(args: StatusArgs) -> Result<(), AppError> {
        let StatusArgs {
            project_root,
            pueue_config,
            json,
            compact,
        } = args;
        let (db, project, _service_paths, policy) =
            resolve_project_read_only(project_root, pueue_config)?;
        db.require_latest_schema()?;
        let pueue = configured_pueue(policy)?;
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
        } else if compact {
            status_command::render_project_status_compact(&db, &project, &input)?
        } else {
            status_command::render_project_status(&db, &project, &input)?
        };
        println!("{rendered}");
        Ok(())
    }

    pub fn events(args: EventsArgs) -> Result<(), AppError> {
        let limit = validate_event_limit(args.limit)?;
        let (db, project, _, _) =
            resolve_project_read_only(args.project_root, args.pueue_config)?;
        let filter = EventFilter::new(args.kind, args.status, limit);
        println!("{}", render_events(&db, &project, &filter, args.json)?);
        Ok(())
    }

    pub async fn runs(args: RunsArgs) -> Result<(), AppError> {
        let limit = pueue_agent::runs::validate_limit(args.limit)?;
        let (db, project, _, _) =
            resolve_project_read_only(args.project_root, args.pueue_config)?;
        if args.follow {
            return pueue_agent::runs::follow_runs(db.path(), &project, limit, args.json).await;
        }
        println!(
            "{}",
            pueue_agent::runs::render_runs(&db, &project, limit, args.json)?
        );
        Ok(())
    }

    pub fn inspect(args: InspectArgs) -> Result<(), AppError> {
        let (db, project, _, _) =
            resolve_project_read_only(args.project_root, args.pueue_config)?;
        println!(
            "{}",
            render_task_inspection(&db, &project, args.task_id, args.json)?
        );
        Ok(())
    }

    pub fn explain(args: ExplainArgs) -> Result<(), AppError> {
        let (db, project, _, _) =
            resolve_project_read_only(args.project_root, args.pueue_config)?;
        println!(
            "{}",
            render_incident_explanation(&db, &project, args.incident_id, args.json)?
        );
        Ok(())
    }

    pub async fn doctor(args: DoctorArgs) -> Result<(), AppError> {
        let (db, project, service_paths, project_roots) =
            resolve_project_doctor_read_only(args.project_root, args.pueue_config)?;
        db.require_latest_schema()?;
        let policy = load_existing_policy(&service_paths.policy_load_input(
            project_roots.clone(),
            current_launcher_path()?,
        ));
        let pueue_status = match &policy {
            Ok(policy) => match configured_pueue(Arc::new(policy.clone())) {
                Ok(pueue) => pueue.status_json().await.map_err(|error| error.render()),
                Err(error) => Err(error.render()),
            },
            Err(violation) => Err(format!("execution policy unavailable ({})", violation.code.as_str())),
        };
        let callbacks = PueueConfigCallbackRegistry::new(&service_paths.pueue_config);
        let external = DoctorExternal {
            pueue: pueue_status,
            service: ServiceManager.status().map_err(|error| error.render()),
            callback: callbacks.current_callback().map_err(|error| error.render()),
        };
        let report = build_doctor_report_with_policy_and_roots(
            &db,
            &project,
            &service_paths,
            external,
            unix_timestamp()?,
            &policy,
            &project_roots,
        )?;
        println!("{}", render_doctor_report_value(&report, args.json)?);
        if report.has_errors() {
            return Err(AppError::Message {
                message: "doctor found error checks; inspect the report".to_owned(),
            });
        }
        Ok(())
    }

    pub async fn submit(args: SubmitArgs) -> Result<(), AppError> {
        let SubmitArgs {
            kind,
            metadata,
            metadata_json,
            json,
            command,
        } = args;
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = project::find_root(&current_dir)?;
        let (db, registered, _service_paths, policy) =
            resolve_project(Some(project_root.clone()), None)?;
        if CampaignRepository::new(&db)
            .find_live_by_project(&registered.project_id)?
            .is_some()
        {
            return Err(AppError::Validation {
                field: "submit",
                message: "a managed campaign is active; use pueue-agent steer",
            });
        }
        let limits = policy.campaign_limits;
        let root_anchor = policy
            .project_root_anchor(&registered.root_path)
            .map_err(AppError::from)?;
        let pueue = configured_pueue(Arc::clone(&policy))?;
        let options = submit_command::SubmitOptions::new(
            kind,
            submit_command::load_metadata(metadata.as_deref(), metadata_json.as_deref())?,
            submit_command::origin_from_environment(&registered.project_id)?,
        );
        let submission = submit_command::run_with_options_with_root_anchor(
            &db,
            &project_root,
            &command,
            &options,
            &limits,
            &pueue,
            root_anchor,
        )
        .await?;
        println!(
            "{}",
            submit_command::render_submission(&submission, &registered.pueue_group, json)?
        );
        Ok(())
    }

    pub async fn submit_batch(args: SubmitBatchArgs) -> Result<(), AppError> {
        let (db, project, _service_paths, policy) = resolve_project(args.project_root, None)?;
        let root_anchor = policy
            .project_root_anchor(&project.root_path)
            .map_err(AppError::from)?;
        let pueue = configured_pueue(Arc::clone(&policy))?;
        let batch = pueue_agent::batches::run_with_root_anchor(
            &db,
            &project.root_path,
            &args.request_id.to_string(),
            &args.manifest,
            args.group.as_deref(),
            &pueue,
            root_anchor,
        )
        .await?;
        println!(
            "{}",
            pueue_agent::batches::render_batch(&batch, &project.pueue_group, args.json)?
        );
        Ok(())
    }

    pub async fn event(args: EventArgs) -> Result<(), AppError> {
        if args.event != "callback" {
            return Ok(());
        }
        let task_id = args.task_id.ok_or(AppError::Configuration {
            field: "callback.task_id",
        })?;
        if task_id < 0 {
            return Err(AppError::Configuration {
                field: "callback.task_id",
            });
        }
        let metadata =
            serde_json::from_str::<CallbackMetadata>(&args.metadata).map_err(|source| {
                AppError::Serialization {
                    operation: "parse callback metadata",
                    source,
                }
            })?;
        let group = match args.group {
            Some(group) => group,
            None => {
                let (db, _service_paths, policy) = resolve_callback_read_only()?;
                let pueue = configured_pueue(policy)?;
                let tasks = pueue.status_json().await?;
                let group = callback_group_for_task(&tasks, task_id)?;
                if ProjectRepository::new(&db).find_by_group(group)?.is_none() {
                    return Err(AppError::Validation {
                        field: "callback.group",
                        message: "is not registered in the configured Pueue profile",
                    });
                }
                group.to_owned()
            }
        };
        let result = record_callback(&group, task_id, metadata)?;
        println!("{}", result.event_id());
        Ok(())
    }

    pub fn pause(args: ProjectArgs) -> Result<(), AppError> {
        let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
        let project = status_command::pause_project(&db, &project.project_id, unix_timestamp()?)?;
        println!("paused: {}", bounded_redacted_text(&project.project_id));
        Ok(())
    }

    pub fn resume(args: ProjectArgs) -> Result<(), AppError> {
        let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
        let project = status_command::resume_project(&db, &project.project_id, unix_timestamp()?)?;
        println!("resumed: {}", bounded_redacted_text(&project.project_id));
        Ok(())
    }

    pub fn campaign(args: CampaignArgs) -> Result<(), AppError> {
        match args.action {
            CampaignAction::Status(args) => {
                let (db, project, _, _) =
                    resolve_project_read_only(args.project_root, args.pueue_config)?;
                db.require_latest_schema()?;
                println!(
                    "{}",
                    pueue_agent::campaign::render_status_for_project(&db, &project, args.json)?
                );
            }
            CampaignAction::Pause(args) => {
                let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::pause_for_project(
                        &db,
                        &project,
                        unix_timestamp()?,
                        args.json,
                    )?
                );
            }
            CampaignAction::Resume(args) => {
                let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::resume_for_project(
                        &db,
                        &project,
                        unix_timestamp()?,
                        args.json,
                    )?
                );
            }
            CampaignAction::Retire(args) => {
                let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::retire_for_project(
                        &db,
                        &project,
                        unix_timestamp()?,
                        args.json,
                    )?
                );
            }
        }
        Ok(())
    }

    pub fn proposal(args: ProposalArgs) -> Result<(), AppError> {
        match args.action {
            ProposalAction::List(args) => {
                let (db, project, _, _) =
                    resolve_project_read_only(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::render_proposals_for_project(
                        &db,
                        &project,
                        args.limit,
                        args.json,
                    )?
                );
            }
            ProposalAction::Inspect(args) => {
                let (db, project, _, _) =
                    resolve_project_read_only(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::render_proposal_for_project(
                        &db,
                        &project,
                        &args.proposal_id,
                        args.json,
                    )?
                );
            }
        }
        Ok(())
    }

    pub fn experiment(args: ExperimentArgs) -> Result<(), AppError> {
        match args.action {
            ExperimentAction::List(args) => {
                let (db, project, _, _) =
                    resolve_project_read_only(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::render_experiments_for_project(
                        &db,
                        &project,
                        args.limit,
                        args.json,
                    )?
                );
            }
            ExperimentAction::Inspect(args) => {
                let (db, project, _, _) =
                    resolve_project_read_only(args.project_root, args.pueue_config)?;
                println!(
                    "{}",
                    pueue_agent::campaign::render_experiment_for_project(
                        &db,
                        &project,
                        &args.experiment_id,
                        args.json,
                    )?
                );
            }
        }
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
        match action {
            Some(SteerAction::List(_)) => {
                let (db, project, _, _) =
                    resolve_project_read_only(project_root, pueue_config)?;
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
                                "message": bounded_redacted_text(&intervention.message),
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
                            bounded_redacted_text(&intervention.message)
                        );
                    }
                }
            }
            None => {
                let (db, project, _, _) = resolve_project(project_root, pueue_config)?;
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

    pub fn wake(args: WakeArgs) -> Result<(), AppError> {
        let (db, project, _, _) = resolve_project(args.project_root, args.pueue_config)?;
        let event_id =
            record_operator_wake_with(&db, &project.project_id, &args.reason, unix_timestamp()?)?;
        if args.json {
            println!(
                "{}",
                serde_json::json!({"schema_version": 1, "event_id": event_id, "project_id": project.project_id, "kind": "operator_wake", "status": "pending"})
            );
        } else {
            println!("{}", human_header("wake", &project.project_id));
            println!(
                "event={event_id} kind=operator_wake state={}",
                format_state("pending")
            );
            println!("{}", human_summary("operator wake queued"));
        }
        Ok(())
    }

    pub fn version(args: VersionArgs) -> Result<(), AppError> {
        println!("{}", version::render(BuildInfo::current()?, args.json)?);
        Ok(())
    }

    pub async fn upgrade(args: UpgradeArgs) -> Result<(), AppError> {
        let json = args.json;
        let current_exe = env::current_exe()
            .map_err(|source| AppError::Io {
                operation: "resolve current executable for upgrade",
                source,
            })
            .map_err(upgrade_diagnostic_error)?;
        let state_db = paths::state_db_path().map_err(upgrade_diagnostic_error)?;
        let working_dir = env::current_dir()
            .map_err(|source| AppError::Io {
                operation: "read upgrade working directory",
                source,
            })
            .map_err(upgrade_diagnostic_error)?;
        let service_paths = ServicePaths::from_environment(&working_dir, args.pueue_config.clone())
            .map_err(upgrade_diagnostic_error)?;
        let project_roots = registered_roots_if_present(&state_db)
            .map_err(upgrade_diagnostic_error)?;
        let policy_input = service_paths.policy_load_input(project_roots, current_exe.clone());
        let policy = Arc::new(
            load_existing_policy(&policy_input).map_err(upgrade_diagnostic_error)?,
        );
        let service_paths = service_paths
            .pin_to_policy(&policy)
            .map_err(upgrade_diagnostic_error)?;
        let pueue_health = ReloadingPueueHealth::new(policy_input);
        let env_source = env::var_os(upgrade::SOURCE_ROOT_ENV).map(PathBuf::from);
        let source = resolve_source_root(
            args.source.as_deref(),
            &current_exe,
            env_source.as_deref(),
        )
        .map_err(upgrade_diagnostic_error)?;
        let mut options = upgrade::UpgradeOptions::from_args(args);
        options.source = Some(source);
        options.release_binary = service_paths.release_binary;
        let service = ServiceManager;
        let commands = ProcessUpgradeCommandRunner;
        match UpgradeRunner::new(options, state_db, &service, &commands, &pueue_health)
            .run()
            .await
        {
            Ok(report) => {
                println!("{}", upgrade::render_report(&report, json)?);
                Ok(())
            }
            Err(failure) => {
                if let Some(report) = failure.report() {
                    println!("{}", upgrade::render_failure_report(report, json)?);
                }
                Err(upgrade_diagnostic_error(failure))
            }
        }
    }

    fn upgrade_diagnostic_error(error: impl std::fmt::Display) -> AppError {
        AppError::Message {
            message: format!(
                "{}; next diagnostic: pueue-agent version",
                bounded_redacted_text(&error.to_string())
            ),
        }
    }

    pub fn start(args: ServiceLifecycleArgs) -> Result<(), AppError> {
        let service = ServiceManager;
        service.start()?;
        if service.status()? != ServiceStatus::Running {
            return Err(AppError::Runtime {
                operation: "verify service started",
            });
        }
        print_service_lifecycle("start", "running", args.json);
        Ok(())
    }

    pub fn stop(args: ServiceLifecycleArgs) -> Result<(), AppError> {
        ServiceManager.stop()?;
        print_service_lifecycle("stop", "stopped", args.json);
        Ok(())
    }

    pub async fn daemon(args: DaemonArgs) -> Result<(), AppError> {
        let state_db = paths::state_db_path()?;
        let working_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read daemon working directory",
            source,
        })?;
        let service_paths = ServicePaths::from_environment(&working_dir, args.pueue_config)?;
        let project_roots = registered_roots_if_present(&state_db)?;
        let policy = Arc::new(load_or_create_policy(&service_paths.policy_load_input(
            project_roots,
            current_launcher_path()?,
        ))?);
        let pueue = configured_pueue(Arc::clone(&policy))?;
        let db = Db::open(&state_db)?;
        let runner = AgentRunner::new(AgentRunnerConfig::production(), Arc::clone(&policy));
        let mut daemon = Daemon::new(
            db,
            pueue,
            policy,
            runner,
            DaemonConfig::default(),
        );
        daemon.run(production_shutdown_token()).await
    }

    fn print_service_lifecycle(operation: &str, service: &str, json: bool) {
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "schema_version": 1,
                    "operation": operation,
                    "service": service,
                })
            );
        } else {
            println!("pueue-agent {operation}");
            println!("service: {}", format_state(service));
            println!("{}", human_summary(match operation {
                "start" => "service started",
                "stop" => "service stopped",
                _ => "service lifecycle operation completed",
            }));
        }
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
    ) -> Result<(Db, Project, ServicePaths, Arc<ResolvedExecutionPolicy>), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = match project_root {
            Some(path) => project::find_root(&path)?,
            None => project::find_root(&current_dir)?,
        };
        let service_paths = ServicePaths::from_environment(&project_root, pueue_config)?;
        let state_db = paths::state_db_path()?;
        let read_db = Db::open_read_only(&state_db)?;
        let project = ProjectRepository::new(&read_db)
            .find_by_root(&project_root)?
            .ok_or(AppError::Runtime {
                operation: "find registered project",
            })?;
        let project_roots = ProjectRepository::new(&read_db)
            .list_all()?
            .into_iter()
            .map(|registered| registered.root_path)
            .collect();
        let policy = Arc::new(load_existing_policy(&service_paths.policy_load_input(
            project_roots,
            current_launcher_path()?,
        ))?);
        let service_paths = service_paths.pin_to_policy(&policy)?;
        drop(read_db);
        let db = Db::open(&state_db)?;
        Ok((db, project, service_paths, policy))
    }

    fn resolve_project_read_only(
        project_root: Option<std::path::PathBuf>,
        pueue_config: Option<std::path::PathBuf>,
    ) -> Result<(Db, Project, ServicePaths, Arc<ResolvedExecutionPolicy>), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = match project_root {
            Some(path) => project::find_root(&path)?,
            None => project::find_root(&current_dir)?,
        };
        let service_paths = ServicePaths::from_environment(&project_root, pueue_config)?;
        let db = Db::open_read_only(&paths::state_db_path()?)?;
        let project = ProjectRepository::new(&db)
            .find_by_root(&project_root)?
            .ok_or(AppError::Runtime {
                operation: "find registered project",
            })?;
        let project_roots = ProjectRepository::new(&db)
            .list_all()?
            .into_iter()
            .map(|registered| registered.root_path)
            .collect();
        let policy = Arc::new(load_existing_policy(&service_paths.policy_load_input(
            project_roots,
            current_launcher_path()?,
        ))?);
        let service_paths = service_paths.pin_to_policy(&policy)?;
        Ok((db, project, service_paths, policy))
    }

    /// Resolve an installed numeric callback without consulting the ambient
    /// working directory. Pueue launches daemon callbacks without a project
    /// cwd, so the global state database is the only authoritative project
    /// index at this entry point.
    fn resolve_callback_read_only() -> Result<
        (Db, ServicePaths, Arc<ResolvedExecutionPolicy>),
        AppError,
    > {
        let db = Db::open_read_only(&paths::state_db_path()?)?;
        let projects = ProjectRepository::new(&db).list_all()?;
        let working_dir = projects
            .first()
            .map(|project| project.root_path.clone())
            .ok_or(AppError::Runtime {
                operation: "find registered callback project",
            })?;
        let project_roots = projects
            .into_iter()
            .map(|project| project.root_path)
            .collect();
        let service_paths = ServicePaths::from_environment(&working_dir, None)?;
        let policy = Arc::new(load_existing_policy(&service_paths.policy_load_input(
            project_roots,
            current_launcher_path()?,
        ))?);
        let service_paths = service_paths.pin_to_policy(&policy)?;
        Ok((db, service_paths, policy))
    }

    /// Doctor must report an unavailable policy rather than failing before
    /// diagnostics are assembled. This resolver opens only existing database
    /// state and does not pin service paths to a policy generation.
    fn resolve_project_doctor_read_only(
        project_root: Option<std::path::PathBuf>,
        pueue_config: Option<std::path::PathBuf>,
    ) -> Result<(Db, Project, ServicePaths, Vec<PathBuf>), AppError> {
        let current_dir = env::current_dir().map_err(|source| AppError::Io {
            operation: "read current directory",
            source,
        })?;
        let project_root = match project_root {
            Some(path) => project::find_root(&path)?,
            None => project::find_root(&current_dir)?,
        };
        let service_paths = ServicePaths::from_environment(&project_root, pueue_config)?;
        let db = Db::open_read_only(&paths::state_db_path()?)?;
        let project = ProjectRepository::new(&db)
            .find_by_root(&project_root)?
            .ok_or(AppError::Runtime {
                operation: "find registered project",
            })?;
        let project_roots = ProjectRepository::new(&db)
            .list_all()?
            .into_iter()
            .map(|registered| registered.root_path)
            .collect();
        Ok((db, project, service_paths, project_roots))
    }

    fn registered_roots_if_present(state_db: &std::path::Path) -> Result<Vec<PathBuf>, AppError> {
        if !state_db.is_file() {
            return Ok(Vec::new());
        }
        let db = Db::open_read_only(state_db)?;
        Ok(ProjectRepository::new(&db)
            .list_all()?
            .into_iter()
            .map(|project| project.root_path)
            .collect())
    }

    fn current_launcher_path() -> Result<PathBuf, AppError> {
        env::current_exe().map_err(|source| AppError::Io {
            operation: "resolve command launcher",
            source,
        })
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
