//! rmux CLI helpers used by the monitor and the `lucarned tui` sessions panel.
//!
//! The live monitor uses `rmux_sdk` for hot-path observation, but a few
//! operational actions still need the tmux-compatible rmux CLI: attach/detach,
//! archive capture, and `#{pane_current_path}` lookup. Keep binary resolution and
//! bounded process waits in one place so the daemon and TUI do not drift.

use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Output};
use std::time::{Duration, Instant};

/// Default timeout for blocking rmux CLI actions.
///
/// Archive/path/list actions should be quick. Attach is intentionally
/// interactive, so callers use [`run_status_interactive`] with a longer timeout.
pub const RMUX_CLI_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for interactive attach handoff. This bounds a hung child while still
/// allowing a normal operator attach/detach workflow.
pub const RMUX_ATTACH_TIMEOUT: Duration = Duration::from_secs(12 * 60 * 60);

/// Errors from binary resolution or CLI execution.
#[derive(Debug, thiserror::Error)]
pub enum RmuxCliError {
    #[error("{0}")]
    Resolve(String),
    #[error("rmux command `{command}` failed: {source}")]
    Io {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("rmux command `{command}` timed out after {timeout_secs}s")]
    Timeout { command: String, timeout_secs: u64 },
}

/// Resolve the rmux binary to a vetted path.
///
/// Resolution order:
/// 1. `~/.cargo/bin/rmux`, when it exists.
/// 2. Absolute `$PATH` entries containing `rmux`.
///
/// Relative `$PATH` entries and binaries in world-writable directories are
/// refused. If nothing is found, return bare `rmux` as a compatibility fallback:
/// manual/dev environments may still rely on platform launcher semantics. The
/// unsafe cases we can positively identify are rejected rather than spawned.
pub fn resolve_rmux_binary() -> Result<PathBuf, RmuxCliError> {
    if let Some(home) = std::env::var_os("HOME") {
        let p = PathBuf::from(home).join(".cargo/bin/rmux");
        if p.is_file() {
            vet_binary(&p)?;
            return Ok(p);
        }
    }

    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            if !dir.is_absolute() {
                continue;
            }
            let candidate = dir.join("rmux");
            if candidate.is_file() {
                vet_binary(&candidate)?;
                return Ok(candidate);
            }
        }
    }

    Ok(PathBuf::from("rmux"))
}

/// Resolve the rmux binary for display / argv construction.
pub fn rmux_binary_display() -> String {
    resolve_rmux_binary()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| "rmux".to_string())
}

/// Run `rmux <args>` inheriting stdio with the default non-interactive timeout.
pub fn run_status(args: &[&str]) -> Result<ExitStatus, RmuxCliError> {
    run_status_with_timeout(args, RMUX_CLI_TIMEOUT)
}

/// Run `rmux <args>` inheriting stdio with the long interactive attach timeout.
pub fn run_status_interactive(args: &[&str]) -> Result<ExitStatus, RmuxCliError> {
    run_status_with_timeout(args, RMUX_ATTACH_TIMEOUT)
}

/// Run `rmux <args>` and capture output with the default timeout.
pub fn output(args: &[&str]) -> Result<Output, RmuxCliError> {
    output_with_timeout(args, RMUX_CLI_TIMEOUT)
}

fn run_status_with_timeout(args: &[&str], timeout: Duration) -> Result<ExitStatus, RmuxCliError> {
    let bin = resolve_rmux_binary()?;
    let command = command_label(args);
    let mut child = std::process::Command::new(&bin)
        .args(args)
        .spawn()
        .map_err(|source| RmuxCliError::Io {
            command: command.clone(),
            source,
        })?;
    wait_status_timeout(&mut child, &command, timeout)
}

fn output_with_timeout(args: &[&str], timeout: Duration) -> Result<Output, RmuxCliError> {
    let bin = resolve_rmux_binary()?;
    let command = command_label(args);
    let mut child = std::process::Command::new(&bin)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|source| RmuxCliError::Io {
            command: command.clone(),
            source,
        })?;
    wait_status_timeout(&mut child, &command, timeout)?;
    child.wait_with_output().map_err(|source| RmuxCliError::Io {
        command: command.clone(),
        source,
    })
}

/// Async output helper for monitor paths that must not block the Tokio runtime.
pub async fn output_async(args: &[&str]) -> Result<Output, RmuxCliError> {
    let bin = resolve_rmux_binary()?;
    let command = command_label(args);
    let child = tokio::process::Command::new(&bin)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|source| RmuxCliError::Io {
            command: command.clone(),
            source,
        })?;
    match tokio::time::timeout(RMUX_CLI_TIMEOUT, child.wait_with_output()).await {
        Ok(Ok(out)) => Ok(out),
        Ok(Err(source)) => Err(RmuxCliError::Io { command, source }),
        Err(_) => Err(RmuxCliError::Timeout {
            command,
            timeout_secs: RMUX_CLI_TIMEOUT.as_secs(),
        }),
    }
}

fn wait_status_timeout(
    child: &mut std::process::Child,
    command: &str,
    timeout: Duration,
) -> Result<ExitStatus, RmuxCliError> {
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if start.elapsed() >= timeout => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(RmuxCliError::Timeout {
                    command: command.to_string(),
                    timeout_secs: timeout.as_secs(),
                });
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(source) => {
                return Err(RmuxCliError::Io {
                    command: command.to_string(),
                    source,
                })
            }
        }
    }
}

fn command_label(args: &[&str]) -> String {
    if args.is_empty() {
        "rmux".to_string()
    } else {
        format!("rmux {}", args.join(" "))
    }
}

fn vet_binary(path: &Path) -> Result<(), RmuxCliError> {
    let meta = std::fs::metadata(path).map_err(|err| {
        RmuxCliError::Resolve(format!("`{}` is not accessible: {err}", path.display()))
    })?;
    if !meta.is_file() {
        return Err(RmuxCliError::Resolve(format!(
            "`{}` is not a regular file",
            path.display()
        )));
    }
    if let Some(parent) = path.parent() {
        if is_world_writable(parent)? {
            return Err(RmuxCliError::Resolve(format!(
                "refusing to spawn `{}`: its directory `{}` is world-writable",
                path.display(),
                parent.display()
            )));
        }
    }
    Ok(())
}

#[cfg(unix)]
fn is_world_writable(dir: &Path) -> Result<bool, RmuxCliError> {
    use std::os::unix::fs::PermissionsExt;
    let meta = std::fs::metadata(dir).map_err(|err| {
        RmuxCliError::Resolve(format!("`{}` is not accessible: {err}", dir.display()))
    })?;
    Ok(meta.permissions().mode() & 0o002 != 0)
}

#[cfg(not(unix))]
fn is_world_writable(_dir: &Path) -> Result<bool, RmuxCliError> {
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn world_writable_binary_dir_is_refused() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let bin = dir.path().join("rmux");
        std::fs::write(&bin, b"#!/bin/sh\n").expect("write fake rmux");
        let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
        perms.set_mode(0o777);
        std::fs::set_permissions(dir.path(), perms).expect("chmod");

        let err = vet_binary(&bin).expect_err("world-writable dir rejected");
        assert!(err.to_string().contains("world-writable"), "got: {err}");
    }

    #[test]
    fn missing_binary_is_a_resolve_error() {
        let path = std::env::temp_dir().join("lucarne-no-such-rmux-binary");
        let err = vet_binary(&path).expect_err("missing binary rejected");
        assert!(matches!(err, RmuxCliError::Resolve(_)));
    }

    #[test]
    fn command_label_includes_args() {
        assert_eq!(
            command_label(&["attach-session", "-t", "work"]),
            "rmux attach-session -t work"
        );
    }
}
