# lucarned install and autostart design

## Purpose

Provide a first-party install and user-level background startup experience for `lucarned` on macOS, Windows, and Linux without requiring Homebrew, winget, choco, Scoop, or system package managers.

Use `cargo-dist` as the release packager and official installer generator. Keep one installed binary, `lucarned`, with built-in control commands backed by a small std-oriented helper library crate, `lucarned-ctl`.

> Current note: `lucarned-ctl` remains a library crate, not a shipped
> `lucarnedctl` binary. Its default build stays small and std-oriented, while
> the intentional `updates` feature adds `reqwest`, `semver`, `serde`,
> `serde_json`, and `tokio`; `lucarned` enables that feature for update checks.

## Goals

- Use `cargo-dist` generated installers as the official install entry:
  - POSIX shell: `curl .../lucarned-installer.sh | sh`
  - PowerShell: `irm .../lucarned-installer.ps1 | iex`
- Provide first-party autostart after install:
  - `lucarned autostart install --start`
- Current-user install only by default. No elevated/admin/root requirement.
- Current-user background management:
  - macOS: LaunchAgent.
  - Windows: Task Scheduler logon task.
  - Linux: systemd user service.
- `lucarned-ctl` stays small and std-oriented in its default build; update
  checks are isolated behind its `updates` feature.
- `lucarned` uses the ctl crate for command parsing/diagnostics/autostart but does not import adapter/core logic into that crate.
- Platform code is compiled only for its target with `#[cfg]`.
- Agent CLI installation and authentication stay out of scope. Diagnostics may report missing CLIs only.
- Remove `clap` from `lucarned`; command parsing is handwritten.

## Non-goals

- No Homebrew/winget/choco/Scoop dependency for primary install path.
- No Windows system service in the first version.
- No root-owned launch daemon or systemd system unit in the first version.
- No agent installation, agent login, token copy, or credential migration.
- No `clap`, `rusqlite`, `lucarne`, or adapter crates in `lucarned-ctl`; async
  HTTP/update dependencies are allowed only behind the `updates` feature.
- No separate `lucarnedctl` release binary in the first version. The separate crate preserves the option to add one later.

## Release/install architecture

`cargo-dist` is the primary release and install layer. It owns:

1. Release CI generation.
2. Target matrix builds.
3. Archives for macOS, Windows, and Linux targets.
4. Release checksums and metadata.
5. GitHub Release asset upload.
6. Generated shell and PowerShell installers.
7. PATH update behavior inside those installers.

Release archives contain one binary:

- `lucarned`

Official install commands use cargo-dist assets:

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.sh | sh
```

```powershell
powershell -c "irm https://github.com/tuchg/Lucarne/releases/latest/download/lucarned-installer.ps1 | iex"
```

After install, users enable background startup with:

```sh
lucarned autostart install --start
```

The first version does not wrap or fork cargo-dist installers. cargo-dist installers cannot run Lucarne-specific post-install logic, so autostart remains an explicit `lucarned` command.

## Install directories

Install directories follow cargo-dist `install-path` configuration and installer overrides. Desired defaults are user-level locations with no elevated privileges:

- macOS: `$HOME/.lucarne/bin`.
- Linux: `$HOME/.lucarne/bin`.
- Windows: cargo-dist user install path derived from the same `~/.lucarne/bin` configuration.

Autostart entries always use the absolute installed `lucarned` path resolved by `lucarned`, so daemon startup does not depend on PATH being updated in a new login shell.

PATH update behavior is cargo-dist behavior:

- POSIX installer writes shell environment setup according to cargo-dist rules.
- PowerShell installer updates `HKCU:\Environment\Path` according to cargo-dist rules.
- Install output explains that the current shell may need restart/source because child processes cannot mutate parent shell environment.

## `lucarned-ctl` crate

New library crate:

- Path: `crates/lucarned-ctl`
- Package: `lucarned-ctl`
- Library import: `lucarned_ctl`
- Source imports: Rust `std` by default; optional update modules may import the
  dependencies gated by the `updates` feature.

`lucarned` depends on `lucarned-ctl` and routes command-line control commands
through it. The ctl crate must not depend on `lucarned`, `lucarne`, channel
adapters, or SQLite; async runtime and HTTP client dependencies are confined to
the `updates` feature.

Command parser:

- Hand-written with `std::env::args_os()`.
- No `clap`.
- Unknown commands/flags print usage and return non-zero.
- `--help`, `help`, `-h`, and `--version` are supported.

Command surface:

```text
lucarned                         Run daemon
lucarned init                    Configure lucarned interactively
lucarned doctor                  Diagnose install and runtime state
lucarned paths                   Print resolved paths
lucarned autostart install [--start] [--bin PATH]
lucarned autostart uninstall [--stop]
lucarned autostart start
lucarned autostart stop
lucarned autostart status
lucarned help
lucarned --version
```

`--bin PATH` defaults to the current `lucarned` executable, then to `lucarned` found on PATH.

## Platform module layout

```text
crates/lucarned-ctl/src/
  lib.rs
  args.rs
  paths.rs
  doctor.rs
  process.rs
  autostart/mod.rs
  autostart/macos.rs
  autostart/windows.rs
  autostart/linux.rs
```

Compilation gates:

```rust
#[cfg(target_os = "macos")]
mod macos;
#[cfg(windows)]
mod windows;
#[cfg(target_os = "linux")]
mod linux;
```

Only the current target backend compiles into the binary using the crate.

## Autostart backends

### macOS

Use a current-user LaunchAgent.

- Label: `com.tuchg.lucarned`
- File: `~/Library/LaunchAgents/com.tuchg.lucarned.plist`
- Program: absolute `lucarned` path
- RunAtLoad: true
- KeepAlive: false
- StandardOutPath: `~/.lucarned/logs/launchd.out.log`
- StandardErrorPath: `~/.lucarned/logs/launchd.err.log`

Commands shell out to:

- install/start: `launchctl bootstrap gui/$UID <plist>` then `launchctl kickstart -k gui/$UID/com.tuchg.lucarned`
- stop/uninstall: `launchctl bootout gui/$UID/com.tuchg.lucarned`
- status: `launchctl print gui/$UID/com.tuchg.lucarned`

### Windows

Use a current-user Task Scheduler task, not a system service.

- Task name: `LucarneLucarned`
- Trigger: at current user logon
- Run level: limited/current user
- Action: absolute `lucarned.exe` path

Commands shell out to:

- install: `schtasks /Create /SC ONLOGON /TN LucarneLucarned /TR <quoted exe> /RL LIMITED /F`
- start: `schtasks /Run /TN LucarneLucarned`
- stop: `schtasks /End /TN LucarneLucarned`
- status: `schtasks /Query /TN LucarneLucarned /FO LIST`
- uninstall: `schtasks /Delete /TN LucarneLucarned /F`

Windows paths use `%LOCALAPPDATA%\lucarned` for config/state/log defaults to match `lucarned` host defaults.

### Linux

Use a current-user systemd unit.

- Unit file: `~/.config/systemd/user/lucarned.service`
- ExecStart: absolute `lucarned` path
- Restart: `no` in first version, matching macOS `KeepAlive=false`
- WantedBy: `default.target`

Commands shell out to:

- install: write unit, `systemctl --user daemon-reload`, `systemctl --user enable lucarned.service`
- start: `systemctl --user start lucarned.service`
- stop: `systemctl --user stop lucarned.service`
- status: `systemctl --user status lucarned.service --no-pager`
- uninstall: `systemctl --user disable lucarned.service`, remove unit, `daemon-reload`

If `systemctl --user` is unavailable or user manager is not running, return a clear unsupported-environment error with manual next steps.

## `paths`

`lucarned paths` prints resolved paths in stable text format:

```text
install_bin_dir=...
lucarned=...
current_exe=...
config_dir=...
config_file=...
state_db=...
log_dir=...
autostart_kind=...
autostart_entry=...
```

Path resolution mirrors `lucarned` defaults but is implemented in `lucarned-ctl` to avoid depending on daemon internals.

## `doctor`

`lucarned doctor` performs read-only checks and exits non-zero if critical install/runtime checks fail.

Checks:

- `lucarned` binary resolves.
- `lucarned --help` exits successfully.
- Config directory exists or can be created by `lucarned`.
- Config file exists; if missing, suggest `lucarned init` or first daemon run.
- State/log directories exist or parent directory is writable.
- Autostart entry status is installed/running/not-installed/error.
- Agent CLIs (`codex`, `claude`, `pi`, `gemini`, `copilot`) are either found on PATH or reported as optional missing dependencies.

Agent CLI checks never install or authenticate agents.

## Error handling

- Every shell-out captures exit status, stdout, and stderr.
- Failures include command name and stderr tail.
- Destructive commands require explicit subcommand (`uninstall`) but no interactive confirmation in first version so scripts can run unattended.
- `install --start` should leave the autostart entry installed even if immediate start fails, and report the start failure separately.
- `uninstall --stop` should attempt stop first, then remove entry; stop failure does not block removal unless the platform command requires it.

## Testing strategy

Unit tests:

- Hand-written argument parser.
- Path resolution with injected environment maps.
- Generated plist/systemd contents.
- Windows command-line quoting for `schtasks`.

Integration/manual smoke:

- `lucarned paths` runs on each target.
- `lucarned doctor` against a built `lucarned`.
- macOS: install autostart, start, status, stop, uninstall.
- Windows: create scheduled task, run, query, end, delete.
- Linux: write user unit, enable, start, status, stop, disable.

Cargo commands follow project rule:

```sh
cargo +nightly check -Zbuild-dir-new-layout -p lucarned
cargo +nightly test -Zbuild-dir-new-layout -p lucarned-ctl
cargo +nightly test -Zbuild-dir-new-layout -p lucarned
```

## Expected binary size

Merging ctl command handling into `lucarned` while removing `clap` is expected to be size-neutral or smaller than the prior `lucarned` binary. The ctl crate itself remains std-oriented by default and can later grow a standalone binary if needed.

## Acceptance criteria

- Release archives include `lucarned` for supported targets.
- `lucarned` supports `doctor`, `paths`, and `autostart` commands.
- `lucarned` no longer depends on `clap`.
- `lucarned-ctl` source imports no dependencies outside `std`.
- macOS, Windows, and Linux autostart backends are cfg-isolated.
- cargo-dist generated installers can install without Homebrew/winget/choco/Scoop.
- `lucarned autostart install --start` works on macOS, Windows, and Linux user sessions.
- `lucarned doctor` reports actionable install/autostart issues without mutating agent state.
