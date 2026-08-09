use std::process::ExitCode;

use clap::Parser;
use pueue_agent::{
    cli::{Cli, Command},
    AppError,
};

fn main() -> ExitCode {
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

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", error.render());
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), AppError> {
    match cli.command {
        Command::Init(args) => commands::init(args),
        Command::Enable(args) => commands::enable(args),
        Command::Disable(args) => commands::disable(args),
        Command::Submit(args) => commands::submit(args),
        Command::Event(args) => commands::event(args),
        Command::Status(args) => commands::status(args),
        Command::Pause(args) => commands::pause(args),
        Command::Resume(args) => commands::resume(args),
        Command::Daemon(args) => commands::daemon(args),
    }
}

mod commands {
    use pueue_agent::{
        cli::{DaemonArgs, EventArgs, InitArgs, ProjectArgs, SubmitArgs},
        AppError,
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

    pub fn submit(_args: SubmitArgs) -> Result<(), AppError> {
        Ok(())
    }

    pub fn event(_args: EventArgs) -> Result<(), AppError> {
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
