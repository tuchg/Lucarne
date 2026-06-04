# lucarned Install Autostart Implementation Plan

> **Status note:** Original plan used a separate `lucarnedctl` binary. Implementation changed after size testing: control commands are now integrated into `lucarned`, backed by `crates/lucarned-ctl` as a library crate. Current releases ship one binary, `lucarned`; `lucarned-ctl` remains small/std-oriented by default, with update-check dependencies isolated behind its `updates` feature. Treat any unchecked `lucarnedctl` binary steps below as historical plan text, not current implementation instructions.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship cargo-dist installers plus built-in `lucarned` commands that manage user-level autostart and diagnostics on macOS, Windows, and Linux.

**Architecture:** `lucarned` remains the only shipped binary. `crates/lucarned-ctl` is a std-oriented library crate used by `lucarned` for parser, paths, doctor, autostart, and update commands. Platform autostart code is cfg-isolated and shells out to LaunchAgent/`launchctl`, Task Scheduler/`schtasks`, or systemd user/`systemctl --user`.

**Tech Stack:** Rust std, Cargo binary targets, cargo-dist shell/PowerShell installers, GitHub Actions, launchctl, schtasks, systemctl.

---

## File structure

- Modify: `Cargo.toml`
  - Add cargo-dist workspace config.
  - Add package metadata needed by cargo-dist.
- Create: `.cargo/config.toml`
  - Enable nightly build-dir layout for cargo commands invoked by generated release CI.
- Modify: `crates/lucarned/Cargo.toml`
  - Add explicit `[[bin]]` entries for `lucarned` and `lucarnedctl` if cargo-dist needs deterministic binary names.
  - Keep dependencies unchanged; `lucarnedctl` source must not import them.
- Create: `crates/lucarned/src/bin/lucarnedctl/main.rs`
  - Small CLI entrypoint; maps parsed commands to implementations.
- Create: `crates/lucarned/src/bin/lucarnedctl/args.rs`
  - Hand-written `std::env::args_os()` parser and usage text.
- Create: `crates/lucarned/src/bin/lucarnedctl/paths.rs`
  - Home/config/log/path/PATH resolution helpers.
- Create: `crates/lucarned/src/bin/lucarnedctl/process.rs`
  - Small `CommandSpec` and shell-out runner.
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/mod.rs`
  - Public autostart command facade plus cfg backend selection.
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/macos.rs`
  - LaunchAgent plist rendering and `launchctl` command specs.
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/windows.rs`
  - Task Scheduler `schtasks` command specs and `/TR` quoting.
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/linux.rs`
  - systemd user service rendering and `systemctl --user` command specs.
- Create: `crates/lucarned/src/bin/lucarnedctl/doctor.rs`
  - Read-only install/runtime diagnostics.
- Modify: `.github/workflows/release.yml`
  - Replace hand-written release workflow with cargo-dist generated workflow.
- Modify: `README.md`
  - Document cargo-dist installer commands and `lucarnedctl autostart install --start`.

---

### Task 1: Add std-only lucarnedctl parser and skeleton

**Files:**
- Create: `crates/lucarned/src/bin/lucarnedctl/main.rs`
- Create: `crates/lucarned/src/bin/lucarnedctl/args.rs`
- Modify: `crates/lucarned/Cargo.toml`

- [ ] **Step 1: Write failing parser tests**

Create `crates/lucarned/src/bin/lucarnedctl/args.rs` with parser types and tests first:

```rust
use std::{ffi::OsString, path::PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Help,
    Version,
    Paths,
    Doctor,
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
pub struct ParseError {
    pub message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

pub fn parse<I>(args: I) -> Result<Command, ParseError>
where
    I: IntoIterator<Item = OsString>,
{
    let mut args = args.into_iter();
    let _program = args.next();
    let Some(first) = args.next() else {
        return Ok(Command::Help);
    };
    parse_first(first, args.collect())
}

fn parse_first(first: OsString, rest: Vec<OsString>) -> Result<Command, ParseError> {
    let first_text = first.to_string_lossy();
    match first_text.as_ref() {
        "help" | "--help" | "-h" => require_no_args(Command::Help, rest),
        "--version" | "version" => require_no_args(Command::Version, rest),
        "paths" => require_no_args(Command::Paths, rest),
        "doctor" => require_no_args(Command::Doctor, rest),
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
        other => Err(ParseError::new(format!("unknown autostart subcommand: {other}"))),
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
    "lucarnedctl - install, diagnose, and manage lucarned autostart\n\n\
Usage:\n\
  lucarnedctl doctor\n\
  lucarnedctl paths\n\
  lucarnedctl autostart install [--start] [--bin PATH]\n\
  lucarnedctl autostart uninstall [--stop]\n\
  lucarnedctl autostart start\n\
  lucarnedctl autostart stop\n\
  lucarnedctl autostart status\n\
  lucarnedctl help\n\
  lucarnedctl --version\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_words(words: &[&str]) -> Result<Command, ParseError> {
        parse(words.iter().map(OsString::from))
    }

    #[test]
    fn no_command_prints_help() {
        assert_eq!(parse_words(&["lucarnedctl"]).unwrap(), Command::Help);
    }

    #[test]
    fn parses_top_level_commands() {
        assert_eq!(parse_words(&["lucarnedctl", "paths"]).unwrap(), Command::Paths);
        assert_eq!(parse_words(&["lucarnedctl", "doctor"]).unwrap(), Command::Doctor);
        assert_eq!(parse_words(&["lucarnedctl", "--version"]).unwrap(), Command::Version);
    }

    #[test]
    fn parses_autostart_install_flags() {
        assert_eq!(
            parse_words(&[
                "lucarnedctl",
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
            parse_words(&["lucarnedctl", "autostart", "uninstall", "--stop"]).unwrap(),
            Command::Autostart(AutostartCommand::Uninstall { stop: true })
        );
    }

    #[test]
    fn rejects_unknown_command() {
        let err = parse_words(&["lucarnedctl", "nope"]).unwrap_err();
        assert_eq!(err.message, "unknown command: nope");
    }

    #[test]
    fn rejects_missing_bin_path() {
        let err = parse_words(&["lucarnedctl", "autostart", "install", "--bin"]).unwrap_err();
        assert_eq!(err.message, "--bin requires a path");
    }
}
```

- [ ] **Step 2: Run parser tests and verify they fail before binary entrypoint exists**

Run:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl args::tests --quiet
```

Expected: FAIL because `src/bin/lucarnedctl/main.rs` does not exist yet.

- [ ] **Step 3: Add binary entrypoint**

Create `crates/lucarned/src/bin/lucarnedctl/main.rs`:

```rust
mod args;

use args::Command;

fn main() {
    let command = match args::parse(std::env::args_os()) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("error: {}\n", err.message);
            eprintln!("{}", args::usage());
            std::process::exit(2);
        }
    };

    match command {
        Command::Help => print!("{}", args::usage()),
        Command::Version => println!("lucarnedctl {}", env!("CARGO_PKG_VERSION")),
        Command::Paths => {
            eprintln!("error: paths command requires path module");
            std::process::exit(2);
        }
        Command::Doctor => {
            eprintln!("error: doctor command requires doctor module");
            std::process::exit(2);
        }
        Command::Autostart(_) => {
            eprintln!("error: autostart command requires autostart module");
            std::process::exit(2);
        }
    }
}
```

Add explicit bins to `crates/lucarned/Cargo.toml` after features:

```toml
[[bin]]
name = "lucarned"
path = "src/main.rs"

[[bin]]
name = "lucarnedctl"
path = "src/bin/lucarnedctl/main.rs"
```

- [ ] **Step 4: Run parser tests and basic binary smoke**

Run:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl args::tests --quiet
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl -- --version
```

Expected:

```text
6 passed
lucarnedctl 0.1.0
```

- [ ] **Step 5: Commit parser skeleton**

```sh
git add crates/lucarned/Cargo.toml crates/lucarned/src/bin/lucarnedctl
git commit -m "feat: add lucarnedctl command skeleton"
```

---

### Task 2: Add path resolution and command runner

**Files:**
- Create: `crates/lucarned/src/bin/lucarnedctl/paths.rs`
- Create: `crates/lucarned/src/bin/lucarnedctl/process.rs`
- Modify: `crates/lucarned/src/bin/lucarnedctl/main.rs`

- [ ] **Step 1: Write path and process tests**

Create `crates/lucarned/src/bin/lucarnedctl/process.rs`:

```rust
use std::{ffi::OsString, process::Command};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub program: OsString,
    pub args: Vec<OsString>,
}

impl CommandSpec {
    pub fn new(program: impl Into<OsString>) -> Self {
        Self { program: program.into(), args: Vec::new() }
    }

    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult {
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

pub fn run(spec: &CommandSpec) -> std::io::Result<CommandResult> {
    let output = Command::new(&spec.program).args(&spec.args).output()?;
    Ok(CommandResult {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}
```

Create `crates/lucarned/src/bin/lucarnedctl/paths.rs`:

```rust
use std::{env, ffi::OsString, path::{Path, PathBuf}};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathInfo {
    pub install_bin_dir: PathBuf,
    pub lucarned: Option<PathBuf>,
    pub lucarnedctl: PathBuf,
    pub config_dir: PathBuf,
    pub config_file: PathBuf,
    pub state_db: PathBuf,
    pub log_dir: PathBuf,
    pub autostart_kind: &'static str,
    pub autostart_entry: PathBuf,
}

pub fn current_path_info(explicit_lucarned: Option<PathBuf>) -> Result<PathInfo, String> {
    let lucarnedctl = env::current_exe().map_err(|err| format!("current exe: {err}"))?;
    let install_bin_dir = lucarnedctl
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "current exe has no parent".to_string())?;
    let lucarned = explicit_lucarned.or_else(|| sibling_lucarned(&lucarnedctl)).or_else(|| find_on_path(lucarned_name()));
    let config_dir = default_config_dir()?;
    let config_file = config_dir.join("lucarned.yaml");
    let state_db = config_dir.join("state.sqlite3");
    let log_dir = config_dir.join("logs");
    let (autostart_kind, autostart_entry) = autostart_location(&config_dir)?;
    Ok(PathInfo { install_bin_dir, lucarned, lucarnedctl, config_dir, config_file, state_db, log_dir, autostart_kind, autostart_entry })
}

pub fn format_path_info(info: &PathInfo) -> String {
    format!(
        "install_bin_dir={}\nlucarned={}\nlucarnedctl={}\nconfig_dir={}\nconfig_file={}\nstate_db={}\nlog_dir={}\nautostart_kind={}\nautostart_entry={}\n",
        info.install_bin_dir.display(),
        info.lucarned.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "<not-found>".to_string()),
        info.lucarnedctl.display(),
        info.config_dir.display(),
        info.config_file.display(),
        info.state_db.display(),
        info.log_dir.display(),
        info.autostart_kind,
        info.autostart_entry.display(),
    )
}

pub fn default_config_dir() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        let base = env::var_os("LOCALAPPDATA").or_else(|| env::var_os("USERPROFILE")).ok_or_else(|| "LOCALAPPDATA and USERPROFILE are not set".to_string())?;
        Ok(PathBuf::from(base).join("lucarned"))
    }
    #[cfg(not(windows))]
    {
        let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
        Ok(PathBuf::from(home).join(".lucarned"))
    }
}

pub fn find_on_path(name: &str) -> Option<PathBuf> {
    find_on_path_with(name, env::var_os("PATH"), env::var_os("PATHEXT"))
}

pub fn find_on_path_with(name: &str, path: Option<OsString>, pathext: Option<OsString>) -> Option<PathBuf> {
    let path = path?;
    let candidates = executable_candidates(name, pathext.as_deref());
    for dir in env::split_paths(&path) {
        for candidate in &candidates {
            let full = dir.join(candidate);
            if full.is_file() {
                return Some(full);
            }
        }
    }
    None
}

fn executable_candidates(name: &str, pathext: Option<&std::ffi::OsStr>) -> Vec<OsString> {
    #[cfg(windows)]
    {
        let path = Path::new(name);
        if path.extension().is_some() {
            return vec![OsString::from(name)];
        }
        let raw = pathext.and_then(|v| v.to_str()).unwrap_or(".COM;.EXE;.BAT;.CMD");
        let mut out = raw.split(';').filter(|s| !s.is_empty()).map(|ext| OsString::from(format!("{name}{ext}"))).collect::<Vec<_>>();
        out.push(OsString::from(name));
        out
    }
    #[cfg(not(windows))]
    {
        let _ = pathext;
        vec![OsString::from(name)]
    }
}

fn sibling_lucarned(current_exe: &Path) -> Option<PathBuf> {
    let sibling = current_exe.with_file_name(lucarned_name());
    sibling.is_file().then_some(sibling)
}

fn lucarned_name() -> &'static str {
    #[cfg(windows)]
    { "lucarned.exe" }
    #[cfg(not(windows))]
    { "lucarned" }
}

fn autostart_location(config_dir: &Path) -> Result<(&'static str, PathBuf), String> {
    #[cfg(target_os = "macos")]
    {
        let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
        Ok(("launchagent", PathBuf::from(home).join("Library/LaunchAgents/com.tuchg.lucarned.plist")))
    }
    #[cfg(windows)]
    {
        Ok(("scheduled-task", PathBuf::from("LucarneLucarned")))
    }
    #[cfg(target_os = "linux")]
    {
        let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
        Ok(("systemd-user", PathBuf::from(home).join(".config/systemd/user/lucarned.service")))
    }
    #[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
    {
        Ok(("unsupported", config_dir.join("autostart.unsupported")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_missing_lucarned_as_not_found() {
        let info = PathInfo {
            install_bin_dir: PathBuf::from("/opt/lucarne/bin"),
            lucarned: None,
            lucarnedctl: PathBuf::from("/opt/lucarne/bin/lucarnedctl"),
            config_dir: PathBuf::from("/home/me/.lucarned"),
            config_file: PathBuf::from("/home/me/.lucarned/lucarned.yaml"),
            state_db: PathBuf::from("/home/me/.lucarned/state.sqlite3"),
            log_dir: PathBuf::from("/home/me/.lucarned/logs"),
            autostart_kind: "systemd-user",
            autostart_entry: PathBuf::from("/home/me/.config/systemd/user/lucarned.service"),
        };
        assert!(format_path_info(&info).contains("lucarned=<not-found>"));
    }

    #[test]
    fn path_lookup_returns_none_without_path() {
        assert_eq!(find_on_path_with("lucarned", None, None), None);
    }
}
```

- [ ] **Step 2: Run path tests and verify compile errors for unwired modules**

Run:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl paths::tests process --quiet
```

Expected: FAIL because `main.rs` does not declare `paths` or `process` yet.

- [ ] **Step 3: Wire paths command in main**

Update `crates/lucarned/src/bin/lucarnedctl/main.rs`:

```rust
mod args;
mod paths;
mod process;

use args::Command;

fn main() {
    let command = match args::parse(std::env::args_os()) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("error: {}\n", err.message);
            eprintln!("{}", args::usage());
            std::process::exit(2);
        }
    };

    if let Err(err) = run(command) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Help => {
            print!("{}", args::usage());
            Ok(())
        }
        Command::Version => {
            println!("lucarnedctl {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::Paths => {
            let info = paths::current_path_info(None)?;
            print!("{}", paths::format_path_info(&info));
            Ok(())
        }
        Command::Doctor => Err("doctor command requires doctor module".to_string()),
        Command::Autostart(_) => Err("autostart command requires autostart module".to_string()),
    }
}
```

- [ ] **Step 4: Run tests and paths smoke**

Run:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl paths::tests process --quiet
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl -- paths
```

Expected: tests pass; `paths` prints keys `install_bin_dir=`, `lucarned=`, `config_dir=`, and `autostart_kind=`.

- [ ] **Step 5: Commit path layer**

```sh
git add crates/lucarned/src/bin/lucarnedctl
git commit -m "feat: add lucarnedctl path diagnostics"
```

---

### Task 3: Add cfg-isolated autostart backends

**Files:**
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/mod.rs`
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/macos.rs`
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/windows.rs`
- Create: `crates/lucarned/src/bin/lucarnedctl/autostart/linux.rs`
- Modify: `crates/lucarned/src/bin/lucarnedctl/main.rs`

- [ ] **Step 1: Write backend builders and tests**

Create `crates/lucarned/src/bin/lucarnedctl/autostart/mod.rs`:

```rust
use std::path::{Path, PathBuf};

use crate::process::{run, CommandResult, CommandSpec};

#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
use macos as platform;
#[cfg(windows)]
use windows as platform;
#[cfg(target_os = "linux")]
use linux as platform;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutostartPaths {
    pub lucarned: PathBuf,
    pub config_dir: PathBuf,
    pub log_dir: PathBuf,
}

pub fn install(paths: &AutostartPaths, start: bool) -> Result<(), String> {
    platform::install(paths)?;
    if start {
        start_service()?;
    }
    Ok(())
}

pub fn uninstall(stop: bool) -> Result<(), String> {
    if stop {
        let _ = stop_service();
    }
    platform::uninstall()
}

pub fn start_service() -> Result<(), String> {
    run_checked(platform::start_command())
}

pub fn stop_service() -> Result<(), String> {
    run_checked(platform::stop_command())
}

pub fn status() -> Result<(), String> {
    let result = run(&platform::status_command()).map_err(|err| err.to_string())?;
    print!("{}", result.stdout);
    eprint!("{}", result.stderr);
    if result.code == Some(0) { Ok(()) } else { Err(format!("autostart status failed with code {:?}", result.code)) }
}

fn run_checked(spec: CommandSpec) -> Result<(), String> {
    let result = run(&spec).map_err(|err| format!("{}: {err}", spec.program.to_string_lossy()))?;
    if result.code == Some(0) {
        Ok(())
    } else {
        Err(format_command_failure(&spec, &result))
    }
}

fn format_command_failure(spec: &CommandSpec, result: &CommandResult) -> String {
    let stderr = result.stderr.trim();
    if stderr.is_empty() {
        format!("{} failed with code {:?}", spec.program.to_string_lossy(), result.code)
    } else {
        format!("{} failed with code {:?}: {}", spec.program.to_string_lossy(), result.code, stderr)
    }
}
```

Create `crates/lucarned/src/bin/lucarnedctl/autostart/macos.rs`:

```rust
#![cfg(target_os = "macos")]

use std::{fs, path::PathBuf};

use super::AutostartPaths;
use crate::process::CommandSpec;

const LABEL: &str = "com.tuchg.lucarned";

pub fn install(paths: &AutostartPaths) -> Result<(), String> {
    fs::create_dir_all(paths.log_dir.as_path()).map_err(|err| format!("create log dir: {err}"))?;
    let plist = launch_agent_path()?;
    if let Some(parent) = plist.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("create launch agent dir: {err}"))?;
    }
    fs::write(&plist, render_plist(paths)).map_err(|err| format!("write {}: {err}", plist.display()))?;
    let _ = super::run_checked(bootout_command()?);
    super::run_checked(bootstrap_command(plist))
}

pub fn uninstall() -> Result<(), String> {
    let _ = super::run_checked(bootout_command()?);
    let plist = launch_agent_path()?;
    if plist.exists() {
        fs::remove_file(&plist).map_err(|err| format!("remove {}: {err}", plist.display()))?;
    }
    Ok(())
}

pub fn start_command() -> CommandSpec { CommandSpec::new("launchctl").arg("kickstart").arg("-k").arg(format!("gui/{}/{}", uid(), LABEL)) }
pub fn stop_command() -> CommandSpec { bootout_command().unwrap_or_else(|_| CommandSpec::new("launchctl").arg("bootout").arg(format!("gui/{}/{}", uid(), LABEL))) }
pub fn status_command() -> CommandSpec { CommandSpec::new("launchctl").arg("print").arg(format!("gui/{}/{}", uid(), LABEL)) }

fn bootstrap_command(plist: PathBuf) -> CommandSpec { CommandSpec::new("launchctl").arg("bootstrap").arg(format!("gui/{}", uid())).arg(plist.into_os_string()) }
fn bootout_command() -> Result<CommandSpec, String> { Ok(CommandSpec::new("launchctl").arg("bootout").arg(format!("gui/{}/{}", uid(), LABEL))) }
fn launch_agent_path() -> Result<PathBuf, String> { let home = std::env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?; Ok(PathBuf::from(home).join("Library/LaunchAgents/com.tuchg.lucarned.plist")) }
fn uid() -> String { std::env::var("UID").unwrap_or_else(|_| "501".to_string()) }

fn render_plist(paths: &AutostartPaths) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n\
<plist version=\"1.0\">\n\
<dict>\n\
  <key>Label</key><string>{}</string>\n\
  <key>ProgramArguments</key><array><string>{}</string></array>\n\
  <key>RunAtLoad</key><true/>\n\
  <key>KeepAlive</key><false/>\n\
  <key>StandardOutPath</key><string>{}</string>\n\
  <key>StandardErrorPath</key><string>{}</string>\n\
</dict>\n\
</plist>\n",
        LABEL,
        xml_escape(&paths.lucarned.display().to_string()),
        xml_escape(&paths.log_dir.join("launchd.out.log").display().to_string()),
        xml_escape(&paths.log_dir.join("launchd.err.log").display().to_string()),
    )
}

fn xml_escape(input: &str) -> String {
    input.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_escapes_paths() {
        let paths = AutostartPaths { lucarned: PathBuf::from("/tmp/A&B/lucarned"), config_dir: PathBuf::from("/tmp/config"), log_dir: PathBuf::from("/tmp/logs") };
        let plist = render_plist(&paths);
        assert!(plist.contains("/tmp/A&amp;B/lucarned"));
        assert!(plist.contains("com.tuchg.lucarned"));
    }
}
```

Create `crates/lucarned/src/bin/lucarnedctl/autostart/windows.rs`:

```rust
#![cfg(windows)]

use super::AutostartPaths;
use crate::process::CommandSpec;

const TASK_NAME: &str = "LucarneLucarned";

pub fn install(paths: &AutostartPaths) -> Result<(), String> {
    std::fs::create_dir_all(paths.log_dir.as_path()).map_err(|err| format!("create log dir: {err}"))?;
    super::run_checked(create_command(&paths.lucarned))
}

pub fn uninstall() -> Result<(), String> { super::run_checked(delete_command()) }
pub fn start_command() -> CommandSpec { CommandSpec::new("schtasks").arg("/Run").arg("/TN").arg(TASK_NAME) }
pub fn stop_command() -> CommandSpec { CommandSpec::new("schtasks").arg("/End").arg("/TN").arg(TASK_NAME) }
pub fn status_command() -> CommandSpec { CommandSpec::new("schtasks").arg("/Query").arg("/TN").arg(TASK_NAME).arg("/FO").arg("LIST") }

fn create_command(lucarned: &std::path::Path) -> CommandSpec {
    CommandSpec::new("schtasks")
        .arg("/Create")
        .arg("/SC")
        .arg("ONLOGON")
        .arg("/TN")
        .arg(TASK_NAME)
        .arg("/TR")
        .arg(task_action(lucarned))
        .arg("/RL")
        .arg("LIMITED")
        .arg("/F")
}

fn delete_command() -> CommandSpec { CommandSpec::new("schtasks").arg("/Delete").arg("/TN").arg(TASK_NAME).arg("/F") }

fn task_action(path: &std::path::Path) -> String {
    let raw = path.display().to_string();
    format!("\"{}\"", raw.replace('"', ""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_action_quotes_path_with_spaces() {
        assert_eq!(task_action(std::path::Path::new(r"C:\Users\A B\lucarned.exe")), r#""C:\Users\A B\lucarned.exe""#);
    }

    #[test]
    fn create_command_uses_current_user_logon_task() {
        let spec = create_command(std::path::Path::new(r"C:\Lucarne\lucarned.exe"));
        let args = spec.args.iter().map(|v| v.to_string_lossy().to_string()).collect::<Vec<_>>();
        assert!(args.contains(&"ONLOGON".to_string()));
        assert!(args.contains(&"LIMITED".to_string()));
        assert!(args.contains(&"LucarneLucarned".to_string()));
    }
}
```

Create `crates/lucarned/src/bin/lucarnedctl/autostart/linux.rs`:

```rust
#![cfg(target_os = "linux")]

use std::{fs, path::PathBuf};

use super::AutostartPaths;
use crate::process::CommandSpec;

const UNIT_NAME: &str = "lucarned.service";

pub fn install(paths: &AutostartPaths) -> Result<(), String> {
    fs::create_dir_all(paths.log_dir.as_path()).map_err(|err| format!("create log dir: {err}"))?;
    let unit = unit_path()?;
    if let Some(parent) = unit.parent() {
        fs::create_dir_all(parent).map_err(|err| format!("create systemd user dir: {err}"))?;
    }
    fs::write(&unit, render_unit(paths)).map_err(|err| format!("write {}: {err}", unit.display()))?;
    super::run_checked(systemctl(&["daemon-reload"]))?;
    super::run_checked(systemctl(&["enable", UNIT_NAME]))
}

pub fn uninstall() -> Result<(), String> {
    let _ = super::run_checked(systemctl(&["disable", UNIT_NAME]));
    let unit = unit_path()?;
    if unit.exists() {
        fs::remove_file(&unit).map_err(|err| format!("remove {}: {err}", unit.display()))?;
    }
    let _ = super::run_checked(systemctl(&["daemon-reload"]));
    Ok(())
}

pub fn start_command() -> CommandSpec { systemctl(&["start", UNIT_NAME]) }
pub fn stop_command() -> CommandSpec { systemctl(&["stop", UNIT_NAME]) }
pub fn status_command() -> CommandSpec { systemctl(&["status", UNIT_NAME, "--no-pager"]) }

fn systemctl(args: &[&str]) -> CommandSpec {
    let mut spec = CommandSpec::new("systemctl").arg("--user");
    for arg in args { spec = spec.arg(*arg); }
    spec
}

fn unit_path() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
    Ok(PathBuf::from(home).join(".config/systemd/user/lucarned.service"))
}

fn render_unit(paths: &AutostartPaths) -> String {
    format!(
        "[Unit]\nDescription=Lucarne daemon\n\n[Service]\nType=simple\nExecStart={}\nRestart=no\n\n[Install]\nWantedBy=default.target\n",
        systemd_quote(&paths.lucarned.display().to_string())
    )
}

fn systemd_quote(input: &str) -> String {
    if input.bytes().all(|b| b.is_ascii_alphanumeric() || b"/._-".contains(&b)) {
        input.to_string()
    } else {
        format!("\"{}\"", input.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_quotes_exec_start_with_spaces() {
        let paths = AutostartPaths { lucarned: PathBuf::from("/home/me/My Apps/lucarned"), config_dir: PathBuf::from("/home/me/.lucarned"), log_dir: PathBuf::from("/home/me/.lucarned/logs") };
        let unit = render_unit(&paths);
        assert!(unit.contains("ExecStart=\"/home/me/My Apps/lucarned\""));
        assert!(unit.contains("WantedBy=default.target"));
    }
}
```

- [ ] **Step 2: Wire autostart in main**

Update `crates/lucarned/src/bin/lucarnedctl/main.rs` to declare `autostart` and route commands:

```rust
mod args;
mod autostart;
mod paths;
mod process;

use args::{AutostartCommand, Command};

fn main() {
    let command = match args::parse(std::env::args_os()) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("error: {}\n", err.message);
            eprintln!("{}", args::usage());
            std::process::exit(2);
        }
    };

    if let Err(err) = run(command) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run(command: Command) -> Result<(), String> {
    match command {
        Command::Help => { print!("{}", args::usage()); Ok(()) }
        Command::Version => { println!("lucarnedctl {}", env!("CARGO_PKG_VERSION")); Ok(()) }
        Command::Paths => { let info = paths::current_path_info(None)?; print!("{}", paths::format_path_info(&info)); Ok(()) }
        Command::Doctor => Err("doctor command requires doctor module".to_string()),
        Command::Autostart(command) => run_autostart(command),
    }
}

fn run_autostart(command: AutostartCommand) -> Result<(), String> {
    match command {
        AutostartCommand::Install { start, bin } => {
            let info = paths::current_path_info(bin)?;
            let lucarned = info.lucarned.ok_or_else(|| "lucarned binary not found; pass --bin PATH".to_string())?;
            autostart::install(&autostart::AutostartPaths { lucarned, config_dir: info.config_dir, log_dir: info.log_dir }, start)
        }
        AutostartCommand::Uninstall { stop } => autostart::uninstall(stop),
        AutostartCommand::Start => autostart::start_service(),
        AutostartCommand::Stop => autostart::stop_service(),
        AutostartCommand::Status => autostart::status(),
    }
}
```

- [ ] **Step 3: Run target-local autostart tests**

Run on macOS/Linux host:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl autostart --quiet
```

Expected: tests for the current platform pass; cfg-disabled platform modules do not compile into this binary.

- [ ] **Step 4: Run Windows compile/test in VM**

In Windows VM:

```powershell
$env:CARGO_TARGET_DIR='C:\lucarne-target-ctl'
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl autostart --quiet
```

Expected: Windows autostart tests pass and no Unix backend imports compile.

- [ ] **Step 5: Commit autostart backends**

```sh
git add crates/lucarned/src/bin/lucarnedctl
git commit -m "feat: add lucarnedctl autostart backends"
```

---

### Task 4: Add doctor command

**Files:**
- Create: `crates/lucarned/src/bin/lucarnedctl/doctor.rs`
- Modify: `crates/lucarned/src/bin/lucarnedctl/main.rs`

- [ ] **Step 1: Write doctor module and tests**

Create `crates/lucarned/src/bin/lucarnedctl/doctor.rs`:

```rust
use std::path::Path;

use crate::{paths, process::{run, CommandSpec}};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckLevel { Ok, Warn, Fail }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check { pub level: CheckLevel, pub name: &'static str, pub message: String }

pub fn run_doctor() -> Result<(), String> {
    let checks = collect_checks()?;
    for check in &checks {
        let prefix = match check.level { CheckLevel::Ok => "ok", CheckLevel::Warn => "warn", CheckLevel::Fail => "fail" };
        println!("{prefix}: {}: {}", check.name, check.message);
    }
    if checks.iter().any(|check| check.level == CheckLevel::Fail) {
        Err("doctor found critical failures".to_string())
    } else {
        Ok(())
    }
}

fn collect_checks() -> Result<Vec<Check>, String> {
    let info = paths::current_path_info(None)?;
    let mut checks = Vec::new();
    match &info.lucarned {
        Some(path) => {
            checks.push(Check { level: CheckLevel::Ok, name: "lucarned", message: path.display().to_string() });
            checks.push(check_lucarned_help(path));
        }
        None => checks.push(Check { level: CheckLevel::Fail, name: "lucarned", message: "not found on PATH or next to lucarnedctl".to_string() }),
    }
    checks.push(path_exists_or_parent(&info.config_file, "config"));
    checks.push(dir_or_parent_writable(&info.log_dir, "logs"));
    checks.push(Check { level: CheckLevel::Ok, name: "autostart", message: format!("{} {}", info.autostart_kind, info.autostart_entry.display()) });
    for agent in ["codex", "claude", "pi", "gemini", "copilot"] {
        checks.push(optional_cli(agent));
    }
    Ok(checks)
}

fn check_lucarned_help(path: &Path) -> Check {
    let spec = CommandSpec::new(path.as_os_str()).arg("--help");
    match run(&spec) {
        Ok(result) if result.code == Some(0) => Check { level: CheckLevel::Ok, name: "lucarned-help", message: "lucarned --help succeeded".to_string() },
        Ok(result) => Check { level: CheckLevel::Fail, name: "lucarned-help", message: format!("lucarned --help exited with {:?}", result.code) },
        Err(err) => Check { level: CheckLevel::Fail, name: "lucarned-help", message: err.to_string() },
    }
}

fn path_exists_or_parent(path: &Path, name: &'static str) -> Check {
    if path.exists() {
        Check { level: CheckLevel::Ok, name, message: path.display().to_string() }
    } else if path.parent().is_some_and(|parent| parent.exists()) {
        Check { level: CheckLevel::Warn, name, message: format!("{} missing; parent exists", path.display()) }
    } else {
        Check { level: CheckLevel::Warn, name, message: format!("{} missing; run lucarned init or start lucarned once", path.display()) }
    }
}

fn dir_or_parent_writable(path: &Path, name: &'static str) -> Check {
    if path.is_dir() {
        Check { level: CheckLevel::Ok, name, message: path.display().to_string() }
    } else if path.parent().is_some_and(|parent| parent.exists()) {
        Check { level: CheckLevel::Warn, name, message: format!("{} missing; parent exists", path.display()) }
    } else {
        Check { level: CheckLevel::Warn, name, message: format!("{} missing", path.display()) }
    }
}

fn optional_cli(name: &'static str) -> Check {
    match paths::find_on_path(name) {
        Some(path) => Check { level: CheckLevel::Ok, name, message: path.display().to_string() },
        None => Check { level: CheckLevel::Warn, name, message: "optional agent CLI not found on PATH".to_string() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_config_is_warning() {
        let check = path_exists_or_parent(Path::new("/definitely/not/present/lucarned.yaml"), "config");
        assert_eq!(check.level, CheckLevel::Warn);
        assert_eq!(check.name, "config");
    }
}
```

- [ ] **Step 2: Wire doctor in main**

Update `crates/lucarned/src/bin/lucarnedctl/main.rs`:

```rust
mod args;
mod autostart;
mod doctor;
mod paths;
mod process;
```

Change doctor match arm:

```rust
Command::Doctor => doctor::run_doctor(),
```

- [ ] **Step 3: Run doctor tests and smoke**

Run:

```sh
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl doctor --quiet
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl -- doctor || true
```

Expected: tests pass; doctor prints ok/warn/fail lines. Smoke may exit non-zero if `lucarned` is not built next to `lucarnedctl` and not on PATH.

- [ ] **Step 4: Build lucarned and rerun doctor against sibling binary**

Run:

```sh
cargo +nightly build -Zbuild-dir-new-layout -p lucarned --bins --quiet
cargo +nightly run -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl -- doctor
```

Expected: doctor exits 0 if `lucarned --help` succeeds; optional missing agent CLIs are warnings.

- [ ] **Step 5: Commit doctor**

```sh
git add crates/lucarned/src/bin/lucarnedctl
git commit -m "feat: add lucarnedctl doctor"
```

---

### Task 5: Add cargo-dist release configuration

**Files:**
- Modify: `Cargo.toml`
- Create: `.cargo/config.toml`
- Modify: `.github/workflows/release.yml`

- [ ] **Step 1: Add Cargo nightly build-dir config**

Create `.cargo/config.toml`:

```toml
[unstable]
build-dir-new-layout = true
```

This lets cargo-dist generated CI use the new build-dir layout when it invokes `cargo` under nightly.

- [ ] **Step 2: Add cargo-dist config**

Append to root `Cargo.toml`:

```toml
[workspace.metadata.dist]
cargo-dist-version = "0.31.0"
ci = ["github"]
installers = ["shell", "powershell"]
targets = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "aarch64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
]
packages = ["lucarned"]
unix-archive = ".tar.gz"
windows-archive = ".zip"
checksum = "sha256"
install-path = "~/.lucarne/bin"
precise-builds = true
install-success-msg = "lucarned and lucarnedctl installed. Run: lucarnedctl autostart install --start"

[workspace.metadata.dist.dependencies.apt]
pkg-config = "*"
libssl-dev = { version = "*", stage = ["build", "run"] }
libsqlite3-dev = { version = "*", stage = ["build", "run"] }
```

- [ ] **Step 3: Install cargo-dist locally if missing**

Run:

```sh
if ! command -v dist >/dev/null 2>&1; then
  cargo +nightly install cargo-dist --version 0.31.0 --locked
fi
```

Expected: `dist --version` prints `cargo-dist 0.31.0`.

- [ ] **Step 4: Generate cargo-dist workflow**

Run:

```sh
dist generate --ci github
```

Expected: `.github/workflows/release.yml` changes to cargo-dist generated release workflow and references cargo-dist 0.31.0.

- [ ] **Step 5: Validate cargo-dist plan**

Run:

```sh
dist plan --tag v0.1.0
```

Expected output contains all of:

```text
lucarned
lucarnedctl
lucarned-installer.sh
lucarned-installer.ps1
x86_64-pc-windows-msvc
aarch64-pc-windows-msvc
aarch64-apple-darwin
x86_64-apple-darwin
x86_64-unknown-linux-gnu
aarch64-unknown-linux-gnu
```

- [ ] **Step 6: Validate local cargo-dist build for host target**

Run:

```sh
dist build --target "$(rustc -vV | awk '/host:/ {print $2}')" --artifacts local --tag v0.1.0
```

Expected: local artifacts include an archive containing both `lucarned` and `lucarnedctl`. Inspect with `tar -tf` or `unzip -l` depending on host archive type.

- [ ] **Step 7: Commit cargo-dist config**

```sh
git add Cargo.toml .cargo/config.toml .github/workflows/release.yml
git commit -m "ci: release lucarned with cargo-dist"
```

---

### Task 6: Update docs and verify size/boundaries

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-05-21-lucarnedctl-install-autostart-design.md` if implementation reveals a cargo-dist constraint that changes accepted design.

- [ ] **Step 1: Update README install section**

Add or replace installation docs in `README.md` with:

```md
## Install lucarned

macOS/Linux:

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.sh | sh
```

Windows PowerShell:

```powershell
powershell -c "irm https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.ps1 | iex"
```

Enable user-level background startup after installing:

```sh
lucarnedctl autostart install --start
```

Check installation state:

```sh
lucarnedctl doctor
lucarnedctl paths
lucarnedctl autostart status
```

`lucarnedctl` does not install or log in agent CLIs. It only reports whether optional agent CLIs are visible on PATH.
```

- [ ] **Step 2: Verify lucarnedctl source imports only std and sibling modules**

Run:

```sh
if grep -R "^use \|^extern crate" crates/lucarned/src/bin/lucarnedctl | grep -E "lucarne|tokio|serde|reqwest|rusqlite|clap|tracing"; then
  echo "unexpected lucarnedctl dependency import" >&2
  exit 1
fi
```

Expected: no output and exit 0.

- [ ] **Step 3: Verify lucarned still builds and tests**

Run:

```sh
cargo +nightly check -Zbuild-dir-new-layout -p lucarned --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --quiet
```

Expected: all pass.

- [ ] **Step 4: Measure lucarnedctl release size**

Run:

```sh
cargo +nightly build -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --release --quiet
find target -type f -path '*/release/lucarnedctl' -perm -111 -exec ls -lh {} \;
```

Expected: stripped release binary is small. If larger than 1 MiB, run `cargo +nightly bloat -Zbuild-dir-new-layout --release -p lucarned --bin lucarnedctl -n 20 --crates` and identify accidental linked crates before proceeding.

- [ ] **Step 5: Run Windows verification**

In Windows VM:

```powershell
$env:CARGO_TARGET_DIR='C:\lucarne-target-ctl'
cargo +nightly check -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
cargo +nightly build -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --release --quiet
```

Expected: all pass. Release binary size stays near the expected std-only range.

- [ ] **Step 6: Commit docs and verification fixes**

```sh
git add README.md docs/superpowers/specs/2026-05-21-lucarnedctl-install-autostart-design.md
git commit -m "docs: document cargo-dist installer flow"
```

Skip the commit if `git diff --cached --quiet` reports no staged changes.

---

### Task 7: Final PR update

**Files:**
- PR branch: `feature/lucarnedctl-install-autostart`

- [ ] **Step 1: Run final macOS verification**

Run:

```sh
cargo +nightly check -Zbuild-dir-new-layout -p lucarned --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --quiet
dist plan --tag v0.1.0
git diff --check
```

Expected: all commands pass.

- [ ] **Step 2: Run final Windows verification**

In Windows VM:

```powershell
$env:CARGO_TARGET_DIR='C:\lucarne-target-ctl'
cargo +nightly check -Zbuild-dir-new-layout -p lucarned --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
cargo +nightly test -Zbuild-dir-new-layout -p lucarned --quiet
```

Expected: all commands pass.

- [ ] **Step 3: Push branch and update PR**

Run:

```sh
git push origin feature/lucarnedctl-install-autostart
gh pr edit 2 --title "Add cargo-dist installers and lucarnedctl autostart" --body-file - <<'EOF'
## Summary
- add std-only lucarnedctl binary for paths, doctor, and user autostart management
- support macOS LaunchAgent, Windows Task Scheduler, and Linux systemd user backends
- configure cargo-dist generated shell and PowerShell installers for lucarned + lucarnedctl

## Verification
- cargo +nightly check -Zbuild-dir-new-layout -p lucarned --quiet
- cargo +nightly test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl --quiet
- cargo +nightly test -Zbuild-dir-new-layout -p lucarned --quiet
- dist plan --tag v0.1.0
- Windows: cargo +nightly check/test -Zbuild-dir-new-layout -p lucarned --bin lucarnedctl
EOF
```

- [ ] **Step 4: Mark PR ready after verification**

Run:

```sh
gh pr ready 2
```

Expected: PR #2 is no longer draft.
