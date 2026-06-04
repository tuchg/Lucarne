use std::{
    env,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathInfo {
    pub install_bin_dir: PathBuf,
    pub lucarned: Option<PathBuf>,
    pub current_exe: PathBuf,
    pub config_dir: PathBuf,
    pub config_file: PathBuf,
    pub state_db: PathBuf,
    pub log_dir: PathBuf,
    pub autostart_kind: &'static str,
    pub autostart_entry: PathBuf,
}

pub fn current_path_info(explicit_lucarned: Option<PathBuf>) -> Result<PathInfo, String> {
    let current_exe = env::current_exe().map_err(|err| format!("current exe: {err}"))?;
    let install_bin_dir = current_exe
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| "current exe has no parent".to_string())?;
    let lucarned = explicit_lucarned
        .or_else(|| sibling_lucarned(&current_exe))
        .or_else(|| find_on_path(lucarned_name()));
    let config_dir = default_config_dir()?;
    let config_file = config_dir.join("lucarned.yaml");
    let state_db = config_dir.join("state.sqlite3");
    let log_dir = config_dir.join("logs");
    let (autostart_kind, autostart_entry) = autostart_location(&config_dir)?;
    Ok(PathInfo {
        install_bin_dir,
        lucarned,
        current_exe,
        config_dir,
        config_file,
        state_db,
        log_dir,
        autostart_kind,
        autostart_entry,
    })
}

pub fn format_path_info(info: &PathInfo) -> String {
    format!(
        "install_bin_dir={}\nlucarned={}\ncurrent_exe={}\nconfig_dir={}\nconfig_file={}\nstate_db={}\nlog_dir={}\nautostart_kind={}\nautostart_entry={}\n",
        info.install_bin_dir.display(),
        info.lucarned
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<not-found>".to_string()),
        info.current_exe.display(),
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
        let base = env::var_os("LOCALAPPDATA")
            .or_else(|| env::var_os("USERPROFILE"))
            .ok_or_else(|| "LOCALAPPDATA and USERPROFILE are not set".to_string())?;
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

pub fn find_on_path_with(
    name: &str,
    path: Option<OsString>,
    pathext: Option<OsString>,
) -> Option<PathBuf> {
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

fn executable_candidates(name: &str, pathext: Option<&OsStr>) -> Vec<OsString> {
    #[cfg(windows)]
    {
        let path = Path::new(name);
        if path.extension().is_some() {
            return vec![OsString::from(name)];
        }
        let raw = pathext
            .and_then(OsStr::to_str)
            .unwrap_or(".COM;.EXE;.BAT;.CMD");
        let mut out = raw
            .split(';')
            .filter(|ext| !ext.is_empty())
            .map(|ext| OsString::from(format!("{name}{ext}")))
            .collect::<Vec<_>>();
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
    {
        "lucarned.exe"
    }
    #[cfg(not(windows))]
    {
        "lucarned"
    }
}

fn autostart_location(_config_dir: &Path) -> Result<(&'static str, PathBuf), String> {
    #[cfg(target_os = "macos")]
    {
        let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
        Ok((
            "launchagent",
            PathBuf::from(home).join("Library/LaunchAgents/com.tuchg.lucarned.plist"),
        ))
    }
    #[cfg(windows)]
    {
        Ok(("scheduled-task", PathBuf::from("LucarneLucarned")))
    }
    #[cfg(target_os = "linux")]
    {
        let home = env::var_os("HOME").ok_or_else(|| "HOME is not set".to_string())?;
        Ok((
            "systemd-user",
            PathBuf::from(home).join(".config/systemd/user/lucarned.service"),
        ))
    }
    #[cfg(not(any(target_os = "macos", windows, target_os = "linux")))]
    {
        Ok(("unsupported", _config_dir.join("autostart.unsupported")))
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
            current_exe: PathBuf::from("/opt/lucarne/bin/lucarned"),
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
