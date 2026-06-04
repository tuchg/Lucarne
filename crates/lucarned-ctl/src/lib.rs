pub mod args;
pub mod autostart;
pub mod doctor;
pub mod paths;
pub mod process;
#[cfg(feature = "updates")]
pub mod updates;

pub use args::{
    parse, usage, AutostartCommand, Command, ParseError, RemoteCommand, RemoteCommandOptions,
};

pub fn run(command: Command) -> Result<(), String> {
    match command {
        Command::RunDaemon => Err("daemon command is handled by lucarned".to_string()),
        Command::Init => Err("init command is handled by lucarned".to_string()),
        Command::Help => {
            print!("{}", args::usage());
            Ok(())
        }
        Command::Version => {
            println!("lucarned {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::Paths => {
            let info = paths::current_path_info(None)?;
            print!("{}", paths::format_path_info(&info));
            Ok(())
        }
        Command::Doctor => doctor::run_doctor(),
        Command::Update => update_requires_async_or_feature(),
        Command::Tui => Err("tui command is handled by lucarned".to_string()),
        Command::Remote(_) => Err("remote command is handled by lucarned".to_string()),
        Command::Autostart(command) => run_autostart(command),
    }
}

#[cfg(feature = "updates")]
pub async fn run_async(
    command: Command,
    client: &reqwest::Client,
    update_config: updates::UpdateConfig,
    update_config_warning: Option<String>,
) -> Result<(), String> {
    match command {
        Command::Doctor => {
            doctor::run_doctor_async(client, update_config, update_config_warning).await
        }
        Command::Update => updates::run_update_command(client, update_config).await,
        other => run(other),
    }
}

fn update_requires_async_or_feature() -> Result<(), String> {
    #[cfg(feature = "updates")]
    {
        Err("update command requires async lucarned-ctl runner".to_string())
    }
    #[cfg(not(feature = "updates"))]
    {
        Err("update command requires lucarned-ctl updates feature".to_string())
    }
}

fn run_autostart(command: AutostartCommand) -> Result<(), String> {
    match command {
        AutostartCommand::Install { start, bin } => {
            let info = paths::current_path_info(bin)?;
            let lucarned = info
                .lucarned
                .ok_or_else(|| "lucarned binary not found; pass --bin PATH".to_string())?;
            autostart::install(
                &autostart::AutostartPaths {
                    lucarned,
                    config_dir: info.config_dir,
                    log_dir: info.log_dir,
                },
                start,
            )
        }
        AutostartCommand::Uninstall { stop } => autostart::uninstall(stop),
        AutostartCommand::Start => autostart::start_service(),
        AutostartCommand::Stop => autostart::stop_service(),
        AutostartCommand::Status => autostart::status(),
    }
}
