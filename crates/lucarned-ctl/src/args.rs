use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    RunDaemon,
    Help,
    Version,
    Init,
    Paths,
    Doctor,
    Update,
    /// Launch the interactive full-screen TUI.
    Tui,
    Remote(RemoteCommand),
    Autostart(AutostartCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutostartCommand {
    Install { start: bool, bin: Option<PathBuf> },
    Uninstall { stop: bool },
    Start,
    Stop,
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteCommand {
    Start(RemoteCommandOptions),
    Stop(RemoteCommandOptions),
    Status(RemoteCommandOptions),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteCommandOptions {
    pub control_port: Option<u16>,
    pub json: bool,
    pub provider: Option<String>,
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

pub fn parse<I>(args: I) -> Result<Command, ParseError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let _program = args.next();
    let Some(first) = args.next() else {
        return Ok(Command::RunDaemon);
    };
    parse_first(first, args.collect())
}

fn parse_first(first: OsString, rest: Vec<OsString>) -> Result<Command, ParseError> {
    let first_text = first.to_string_lossy();
    match first_text.as_ref() {
        "help" | "--help" | "-h" => require_no_args(Command::Help, rest),
        "--version" | "version" => require_no_args(Command::Version, rest),
        "init" => require_no_args(Command::Init, rest),
        "paths" => require_no_args(Command::Paths, rest),
        "doctor" => require_no_args(Command::Doctor, rest),
        "update" => require_no_args(Command::Update, rest),
        "tui" => require_no_args(Command::Tui, rest),
        "remote" => parse_remote(rest),
        "autostart" => parse_autostart(rest),
        other => Err(ParseError::new(format!("unknown command: {other}"))),
    }
}

fn require_no_args(command: Command, rest: Vec<OsString>) -> Result<Command, ParseError> {
    if rest.is_empty() {
        Ok(command)
    } else {
        Err(ParseError::new(format!(
            "unexpected argument: {}",
            rest[0].to_string_lossy()
        )))
    }
}

fn parse_remote(args: Vec<OsString>) -> Result<Command, ParseError> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Err(ParseError::new("missing remote subcommand"));
    };
    let options = parse_remote_options(args.collect())?;
    match subcommand.to_string_lossy().as_ref() {
        "start" => Ok(Command::Remote(RemoteCommand::Start(options))),
        "stop" => {
            reject_start_only_remote_options(&options, "stop")?;
            Ok(Command::Remote(RemoteCommand::Stop(options)))
        }
        "status" => {
            reject_start_only_remote_options(&options, "status")?;
            Ok(Command::Remote(RemoteCommand::Status(options)))
        }
        other => Err(ParseError::new(format!(
            "unknown remote subcommand: {other}"
        ))),
    }
}

fn parse_remote_options(args: Vec<OsString>) -> Result<RemoteCommandOptions, ParseError> {
    let mut options = RemoteCommandOptions::default();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--json" => options.json = true,
            "--control-port" => {
                let Some(port) = iter.next() else {
                    return Err(ParseError::new("--control-port requires a port"));
                };
                options.control_port = Some(parse_control_port(&port)?);
            }
            "--provider" => {
                let Some(provider) = iter.next() else {
                    return Err(ParseError::new("--provider requires an id"));
                };
                let provider = provider.to_string_lossy().trim().to_string();
                if provider.is_empty() {
                    return Err(ParseError::new("--provider requires a non-empty id"));
                }
                options.provider = Some(provider);
            }
            "--field" => {
                let Some(pair) = iter.next() else {
                    return Err(ParseError::new("--field requires KEY=VALUE"));
                };
                let pair = pair.to_string_lossy();
                let Some((key, value)) = pair.split_once('=') else {
                    return Err(ParseError::new("--field requires KEY=VALUE"));
                };
                let key = key.trim();
                if key.is_empty() {
                    return Err(ParseError::new("--field requires a non-empty key"));
                }
                options.fields.insert(key.to_string(), value.to_string());
            }
            other => return Err(ParseError::new(format!("unexpected argument: {other}"))),
        }
    }
    Ok(options)
}

fn parse_control_port(raw: &OsString) -> Result<u16, ParseError> {
    raw.to_string_lossy()
        .parse::<u16>()
        .map_err(|_| ParseError::new(format!("invalid control port: {}", raw.to_string_lossy())))
}

fn reject_start_only_remote_options(
    options: &RemoteCommandOptions,
    subcommand: &str,
) -> Result<(), ParseError> {
    if options.provider.is_some() {
        return Err(ParseError::new(format!(
            "--provider is only valid for remote start, not remote {subcommand}"
        )));
    }
    if !options.fields.is_empty() {
        return Err(ParseError::new(format!(
            "--field is only valid for remote start, not remote {subcommand}"
        )));
    }
    Ok(())
}

fn parse_autostart(args: Vec<OsString>) -> Result<Command, ParseError> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Err(ParseError::new("missing autostart subcommand"));
    };
    match subcommand.to_string_lossy().as_ref() {
        "install" => parse_autostart_install(args.collect()),
        "uninstall" => parse_autostart_uninstall(args.collect()),
        "start" => require_no_os_args(AutostartCommand::Start, args.collect()),
        "stop" => require_no_os_args(AutostartCommand::Stop, args.collect()),
        "status" => require_no_os_args(AutostartCommand::Status, args.collect()),
        other => Err(ParseError::new(format!(
            "unknown autostart subcommand: {other}"
        ))),
    }
    .map(Command::Autostart)
}

fn require_no_os_args(
    command: AutostartCommand,
    rest: Vec<OsString>,
) -> Result<AutostartCommand, ParseError> {
    if rest.is_empty() {
        Ok(command)
    } else {
        Err(ParseError::new(format!(
            "unexpected argument: {}",
            rest[0].to_string_lossy()
        )))
    }
}

fn parse_autostart_install(args: Vec<OsString>) -> Result<AutostartCommand, ParseError> {
    let mut start = false;
    let mut bin = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--start" => start = true,
            "--bin" => {
                let Some(path) = iter.next() else {
                    return Err(ParseError::new("--bin requires a path"));
                };
                bin = Some(PathBuf::from(path));
            }
            other => return Err(ParseError::new(format!("unexpected argument: {other}"))),
        }
    }
    Ok(AutostartCommand::Install { start, bin })
}

fn parse_autostart_uninstall(args: Vec<OsString>) -> Result<AutostartCommand, ParseError> {
    let mut stop = false;
    for arg in args {
        match arg.to_string_lossy().as_ref() {
            "--stop" => stop = true,
            other => return Err(ParseError::new(format!("unexpected argument: {other}"))),
        }
    }
    Ok(AutostartCommand::Uninstall { stop })
}

pub fn usage() -> &'static str {
    "lucarned - lucarne daemon and local service manager\n\n\
Usage:\n\
  lucarned                         Run daemon\n\
  lucarned init                    Configure lucarned interactively\n\
  lucarned doctor                  Diagnose install and runtime state\n\
  lucarned update                  Check latest Lucarne release status\n\
  lucarned tui                     Launch the interactive terminal dashboard\n\
  lucarned remote start [--control-port PORT] [--provider ID] [--field KEY=VALUE] [--json]\n\
  lucarned remote stop [--control-port PORT] [--json]\n\
  lucarned remote status [--control-port PORT] [--json]\n\
  lucarned paths                   Print resolved paths\n\
  lucarned autostart install [--start] [--bin PATH]\n\
  lucarned autostart uninstall [--stop]\n\
  lucarned autostart start\n\
  lucarned autostart stop\n\
  lucarned autostart status\n\
  lucarned help\n\
  lucarned --version\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_words(words: &[&str]) -> Result<Command, ParseError> {
        parse(words.iter().map(OsString::from))
    }

    #[test]
    fn no_command_runs_daemon() {
        assert_eq!(parse_words(&["lucarned"]).unwrap(), Command::RunDaemon);
    }

    #[test]
    fn parses_top_level_commands() {
        assert_eq!(parse_words(&["lucarned", "init"]).unwrap(), Command::Init);
        assert_eq!(parse_words(&["lucarned", "paths"]).unwrap(), Command::Paths);
        assert_eq!(
            parse_words(&["lucarned", "doctor"]).unwrap(),
            Command::Doctor
        );
        assert_eq!(
            parse_words(&["lucarned", "update"]).unwrap(),
            Command::Update
        );
        assert_eq!(parse_words(&["lucarned", "tui"]).unwrap(), Command::Tui);
        assert_eq!(
            parse_words(&["lucarned", "remote", "status"]).unwrap(),
            Command::Remote(RemoteCommand::Status(RemoteCommandOptions::default()))
        );
        assert_eq!(
            parse_words(&["lucarned", "--version"]).unwrap(),
            Command::Version
        );
    }

    #[test]
    fn parses_remote_start_options() {
        let mut fields = BTreeMap::new();
        fields.insert("token".to_string(), "abc".to_string());
        fields.insert(
            "public_url".to_string(),
            "https://demo.example.test".to_string(),
        );
        assert_eq!(
            parse_words(&[
                "lucarned",
                "remote",
                "start",
                "--control-port",
                "7901",
                "--provider",
                "cloudflared",
                "--field",
                "token=abc",
                "--field",
                "public_url=https://demo.example.test",
                "--json",
            ])
            .unwrap(),
            Command::Remote(RemoteCommand::Start(RemoteCommandOptions {
                control_port: Some(7901),
                json: true,
                provider: Some("cloudflared".to_string()),
                fields,
            }))
        );
    }

    #[test]
    fn parses_autostart_install_flags() {
        assert_eq!(
            parse_words(&[
                "lucarned",
                "autostart",
                "install",
                "--start",
                "--bin",
                "/tmp/lucarned",
            ])
            .unwrap(),
            Command::Autostart(AutostartCommand::Install {
                start: true,
                bin: Some(PathBuf::from("/tmp/lucarned")),
            })
        );
    }

    #[test]
    fn parses_autostart_uninstall_stop() {
        assert_eq!(
            parse_words(&["lucarned", "autostart", "uninstall", "--stop"]).unwrap(),
            Command::Autostart(AutostartCommand::Uninstall { stop: true })
        );
    }

    #[test]
    fn usage_lists_update_command() {
        assert!(usage().contains("lucarned update"));
        assert!(usage().contains("lucarned remote start"));
    }

    #[test]
    fn rejects_unknown_command() {
        let err = parse_words(&["lucarned", "nope"]).unwrap_err();
        assert_eq!(err.message, "unknown command: nope");
    }

    #[test]
    fn rejects_missing_bin_path() {
        let err = parse_words(&["lucarned", "autostart", "install", "--bin"]).unwrap_err();
        assert_eq!(err.message, "--bin requires a path");
    }

    #[test]
    fn rejects_remote_start_only_flags_on_status() {
        let err = parse_words(&["lucarned", "remote", "status", "--field", "token=x"]).unwrap_err();
        assert_eq!(
            err.message,
            "--field is only valid for remote start, not remote status"
        );
    }

    #[test]
    fn rejects_invalid_remote_control_port() {
        let err =
            parse_words(&["lucarned", "remote", "stop", "--control-port", "nope"]).unwrap_err();
        assert_eq!(err.message, "invalid control port: nope");
    }
}
