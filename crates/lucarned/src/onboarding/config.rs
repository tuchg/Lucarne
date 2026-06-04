use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;

const DEFAULT_STATE_DB: &str = "~/.lucarned/state.sqlite3";
const DEFAULT_LOG_FILTER: &str = "info,lucarne=debug,lucarned=debug";
const DEFAULT_STDERR_LOG_FILTER: &str = "warn";
const DEFAULT_LOG_DIR: &str = "~/.lucarned/logs";
const DEFAULT_HEALTH_ADDR: &str = "127.0.0.1:7766";
const DEFAULT_CONTEXT_EXPIRY_REMINDER_TEMPLATE: &str =
    "会话将在 {remaining_minutes} 分钟后到期，请回复以保持会话可用。";
const DEFAULT_RATE_LIMIT_INTERACTION_PROMPT: &str =
    "微信主动通知快到发送限制了，请回复任意消息以刷新会话。";

pub(crate) enum AgentSelection {
    All,
    None,
    Selected(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExistingConfigDefaults {
    pub(crate) agents: AgentSelection,
    pub(crate) telegram_enabled: Option<bool>,
    pub(crate) telegram_token: Option<String>,
    pub(crate) telegram_entry_chat_id: Option<i64>,
    pub(crate) wechat_enabled: Option<bool>,
    pub(crate) wechat_credential_path: Option<String>,
}

impl Default for ExistingConfigDefaults {
    fn default() -> Self {
        Self {
            agents: AgentSelection::All,
            telegram_enabled: None,
            telegram_token: None,
            telegram_entry_chat_id: None,
            wechat_enabled: None,
            wechat_credential_path: None,
        }
    }
}

impl ExistingConfigDefaults {
    pub(crate) fn from_yaml(raw: &str) -> Result<Self, serde_yaml::Error> {
        let value: serde_yaml::Value = serde_yaml::from_str(raw)?;
        let mut defaults = Self::default();

        if let Some(agents) = mapping_get(&value, "agents") {
            defaults.agents = match agents {
                serde_yaml::Value::Sequence(items) if items.is_empty() => AgentSelection::None,
                serde_yaml::Value::Sequence(items) => AgentSelection::Selected(
                    items
                        .iter()
                        .filter_map(|item| item.as_str().map(ToString::to_string))
                        .collect(),
                ),
                _ => AgentSelection::All,
            };
        }

        let telegram =
            mapping_get(&value, "channels").and_then(|channels| mapping_get(channels, "telegram"));
        if let Some(telegram) = telegram {
            defaults.telegram_enabled =
                mapping_get(telegram, "enabled").and_then(serde_yaml::Value::as_bool);
            defaults.telegram_token = mapping_get(telegram, "token")
                .and_then(serde_yaml::Value::as_str)
                .map(ToString::to_string);
            defaults.telegram_entry_chat_id =
                mapping_get(telegram, "entry_chat_id").and_then(serde_yaml::Value::as_i64);
        }

        let wechat =
            mapping_get(&value, "channels").and_then(|channels| mapping_get(channels, "wechat"));
        if let Some(wechat) = wechat {
            defaults.wechat_enabled =
                mapping_get(wechat, "enabled").and_then(serde_yaml::Value::as_bool);
            defaults.wechat_credential_path = mapping_get(wechat, "credential_path")
                .and_then(serde_yaml::Value::as_str)
                .map(ToString::to_string);
        }

        Ok(defaults)
    }
}

fn mapping_get<'a>(value: &'a serde_yaml::Value, key: &str) -> Option<&'a serde_yaml::Value> {
    value
        .as_mapping()?
        .get(&serde_yaml::Value::String(key.to_string()))
}

pub(crate) enum TelegramDraft {
    Disabled,
    Enabled {
        token: String,
        entry_chat_id: i64,
        bot_username: Option<String>,
    },
}

pub(crate) enum WechatDraft {
    Disabled {
        credential_path: String,
    },
    Enabled {
        credential_path: String,
        reused_existing_credentials: bool,
    },
}

pub(crate) struct InitConfigDraft {
    pub(crate) agents: AgentSelection,
    pub(crate) telegram: TelegramDraft,
    pub(crate) wechat: WechatDraft,
}

impl InitConfigDraft {
    pub(crate) fn render_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(&ConfigYaml::from(self))
    }

    pub(crate) fn redacted_summary(&self, config_path: &str) -> String {
        let mut lines = vec![format!("Config: {config_path}")];

        match &self.agents {
            AgentSelection::All => lines.push("Agents: all".to_string()),
            AgentSelection::None => lines.push("Agents: none".to_string()),
            AgentSelection::Selected(agents) => {
                lines.push(format!("Agents: {}", agents.join(", ")))
            }
        }

        match &self.telegram {
            TelegramDraft::Disabled => lines.push("Telegram: disabled".to_string()),
            TelegramDraft::Enabled {
                entry_chat_id,
                bot_username,
                ..
            } => {
                let bot = bot_username.as_deref().unwrap_or("unknown bot");
                lines.push(format!(
                    "Telegram: enabled ({bot}, chat {entry_chat_id}, token <redacted>)"
                ));
            }
        }

        match &self.wechat {
            WechatDraft::Disabled { credential_path } => {
                lines.push(format!("WeChat: disabled (credentials {credential_path})"))
            }
            WechatDraft::Enabled {
                credential_path,
                reused_existing_credentials,
            } => lines.push(format!(
                "WeChat: enabled (credentials {credential_path}, {})",
                if *reused_existing_credentials {
                    "reuse existing credentials if present; otherwise QR login required"
                } else {
                    "QR login required"
                }
            )),
        }

        lines.join("\n")
    }
}

pub(crate) fn write_config_with_backup(
    path: &Path,
    contents: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    if path.exists() {
        create_backup(path, parent)?;
    }

    let tmp_path = parent.join(format!(
        ".{}.tmp-{}-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("lucarned.yaml"),
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    ));
    // SEC-001: the config can hold provider secrets (tunnel token), so create the
    // tmp file owner-only (`0o600`) on unix before writing — the secret is never
    // briefly group/world-readable. Preserve the atomic tmp+rename below.
    let mut tmp = owner_only_options()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    tmp.write_all(contents.as_bytes())?;
    drop(tmp);
    if let Err(err) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err.into());
    }

    Ok(())
}

/// An [`OpenOptions`] pre-set to create owner-only (`0o600`) files on unix so the
/// secret-bearing config (and its backup) is never group/world-readable
/// (SEC-001). On non-unix there are no mode bits to set, so the platform default
/// is used.
fn owner_only_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn create_backup(path: &Path, parent: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("config path has no file name: {}", path.display()))?
        .to_string_lossy();

    for attempt in 0..1000u16 {
        let timestamp_nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let backup_path = parent.join(format!("{file_name}.bak-{timestamp_nanos}-{attempt}"));
        // SEC-001: the backup carries the same provider secrets as the config, so
        // create it owner-only (`0o600`) on unix too.
        match owner_only_options()
            .write(true)
            .create_new(true)
            .open(&backup_path)
        {
            Ok(mut backup) => {
                let mut source = File::open(path)?;
                io::copy(&mut source, &mut backup)?;
                return Ok(());
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err.into()),
        }
    }

    Err(format!("could not create unique backup for {}", path.display()).into())
}

#[derive(Serialize)]
struct ConfigYaml<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    agents: Option<&'a [String]>,
    state: StateYaml,
    logging: LoggingYaml,
    health: HealthYaml,
    updates: UpdatesYaml,
    turn: TurnYaml,
    session: SessionYaml,
    config: RuntimeConfigYaml,
    channels: ChannelsYaml<'a>,
}

impl<'a> From<&'a InitConfigDraft> for ConfigYaml<'a> {
    fn from(draft: &'a InitConfigDraft) -> Self {
        let agents = match &draft.agents {
            AgentSelection::All => None,
            AgentSelection::None => Some(&[][..]),
            AgentSelection::Selected(agents) => Some(agents.as_slice()),
        };

        Self {
            agents,
            state: StateYaml {
                db: DEFAULT_STATE_DB,
            },
            logging: LoggingYaml {
                filter: DEFAULT_LOG_FILTER,
                stderr_filter: DEFAULT_STDERR_LOG_FILTER,
                dir: DEFAULT_LOG_DIR,
                file: None,
                max_files: 16,
                buffered_lines: 1024,
            },
            health: HealthYaml {
                enabled: false,
                addr: DEFAULT_HEALTH_ADDR,
            },
            updates: UpdatesYaml {
                enabled: true,
                notify: true,
                check_interval_hours: 24,
                remind_interval_hours: 24,
                repository: "tuchg/Lucarne",
            },
            turn: TurnYaml {
                inactivity_secs: 1800,
                deadline_secs: 3600,
            },
            session: SessionYaml {
                idle_timeout_secs: 7200,
            },
            config: RuntimeConfigYaml {
                global: GlobalRuntimeConfigYaml {
                    bypass: false,
                    notifications: true,
                },
            },
            channels: ChannelsYaml {
                telegram: TelegramYaml::from(&draft.telegram),
                wechat: WechatYaml::from(&draft.wechat),
            },
        }
    }
}

#[derive(Serialize)]
struct StateYaml {
    db: &'static str,
}

#[derive(Serialize)]
struct LoggingYaml {
    filter: &'static str,
    stderr_filter: &'static str,
    dir: &'static str,
    file: Option<String>,
    max_files: u16,
    buffered_lines: u16,
}

#[derive(Serialize)]
struct HealthYaml {
    enabled: bool,
    addr: &'static str,
}

#[derive(Serialize)]
struct UpdatesYaml {
    enabled: bool,
    notify: bool,
    check_interval_hours: u8,
    remind_interval_hours: u8,
    repository: &'static str,
}

#[derive(Serialize)]
struct TurnYaml {
    inactivity_secs: u64,
    deadline_secs: u64,
}

#[derive(Serialize)]
struct SessionYaml {
    idle_timeout_secs: u64,
}

#[derive(Serialize)]
struct RuntimeConfigYaml {
    global: GlobalRuntimeConfigYaml,
}

#[derive(Serialize)]
struct GlobalRuntimeConfigYaml {
    bypass: bool,
    notifications: bool,
}

#[derive(Serialize)]
struct ChannelsYaml<'a> {
    telegram: TelegramYaml<'a>,
    wechat: WechatYaml<'a>,
}

#[derive(Serialize)]
struct TelegramYaml<'a> {
    enabled: bool,
    token: &'a str,
    entry_chat_id: Option<i64>,
}

impl<'a> From<&'a TelegramDraft> for TelegramYaml<'a> {
    fn from(draft: &'a TelegramDraft) -> Self {
        match draft {
            TelegramDraft::Disabled => Self {
                enabled: false,
                token: "",
                entry_chat_id: None,
            },
            TelegramDraft::Enabled {
                token,
                entry_chat_id,
                ..
            } => Self {
                enabled: true,
                token,
                entry_chat_id: Some(*entry_chat_id),
            },
        }
    }
}

#[derive(Serialize)]
struct WechatYaml<'a> {
    enabled: bool,
    credential_path: &'a str,
    force_login: bool,
    context: WechatContextYaml,
    rate_limit: WechatRateLimitYaml,
}

impl<'a> From<&'a WechatDraft> for WechatYaml<'a> {
    fn from(draft: &'a WechatDraft) -> Self {
        let (enabled, credential_path) = match draft {
            WechatDraft::Disabled { credential_path } => (false, credential_path.as_str()),
            WechatDraft::Enabled {
                credential_path, ..
            } => (true, credential_path.as_str()),
        };

        Self {
            enabled,
            credential_path,
            force_login: false,
            context: WechatContextYaml {
                ttl_secs: 7200,
                expiry_remind_before_secs: 300,
                expiry_reminder_template: DEFAULT_CONTEXT_EXPIRY_REMINDER_TEMPLATE,
            },
            rate_limit: WechatRateLimitYaml {
                retry_after_secs: 90,
                max_retries: 3,
                interaction_window_secs: 300,
                interaction_threshold: 6,
                interaction_prompt: DEFAULT_RATE_LIMIT_INTERACTION_PROMPT,
            },
        }
    }
}

#[derive(Serialize)]
struct WechatContextYaml {
    ttl_secs: u16,
    expiry_remind_before_secs: u16,
    expiry_reminder_template: &'static str,
}

#[derive(Serialize)]
struct WechatRateLimitYaml {
    retry_after_secs: u16,
    max_retries: u8,
    interaction_window_secs: u16,
    interaction_threshold: u8,
    interaction_prompt: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn current_dir_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn render_omits_agents_for_all_agents() {
        let draft = InitConfigDraft {
            agents: AgentSelection::All,
            telegram: TelegramDraft::Disabled,
            wechat: WechatDraft::Disabled {
                credential_path: "~/.lucarned/wechat-credentials.json".into(),
            },
        };

        let yaml = draft.render_yaml().expect("render yaml");

        assert!(
            !yaml.contains("agents:"),
            "all agents should omit agents key"
        );
        assert!(yaml.contains("state:"));
        assert!(yaml.contains("db: ~/.lucarned/state.sqlite3"));
        assert!(yaml.contains("stderr_filter: warn"));
        assert!(yaml.contains("turn:"));
        assert!(yaml.contains("inactivity_secs: 1800"));
        assert!(yaml.contains("deadline_secs: 3600"));
        assert!(yaml.contains("session:"));
        assert!(yaml.contains("idle_timeout_secs: 7200"));
        assert!(yaml.contains("config:"));
        assert!(yaml.contains("global:"));
        assert!(yaml.contains("bypass: false"));
        assert!(yaml.contains("notifications: true"));
        assert!(yaml.contains("updates:"));
        assert!(yaml.contains("check_interval_hours: 24"));
        assert!(yaml.contains("repository: tuchg/Lucarne"));
        assert!(yaml.contains("telegram:"));
        assert!(yaml.contains("wechat:"));
        assert!(yaml.contains("enabled: false"));
        assert!(yaml.contains("retry_after_secs: 90"));
        assert!(yaml.contains("max_retries: 3"));
        assert!(yaml.contains("interaction_window_secs: 300"));
        assert!(yaml.contains("interaction_threshold: 6"));
        assert!(yaml.contains(
            "interaction_prompt: 微信主动通知快到发送限制了，请回复任意消息以刷新会话。"
        ));
    }

    #[test]
    fn render_writes_empty_agents_for_no_agents() {
        let draft = InitConfigDraft {
            agents: AgentSelection::None,
            telegram: TelegramDraft::Disabled,
            wechat: WechatDraft::Disabled {
                credential_path: "~/.lucarned/wechat-credentials.json".into(),
            },
        };

        let yaml = draft.render_yaml().expect("render yaml");

        assert!(yaml.contains("agents: []"));
    }

    #[test]
    fn render_writes_selected_agents_and_enabled_channels() {
        let draft = InitConfigDraft {
            agents: AgentSelection::Selected(vec!["codex".into(), "pi".into()]),
            telegram: TelegramDraft::Enabled {
                token: "123456:secret".into(),
                entry_chat_id: 5504995202,
                bot_username: Some("lucarne_test_bot".into()),
            },
            wechat: WechatDraft::Enabled {
                credential_path: "~/.lucarned/wechat-credentials.json".into(),
                reused_existing_credentials: false,
            },
        };

        let yaml = draft.render_yaml().expect("render yaml");

        assert!(yaml.contains("agents:"));
        assert!(yaml.contains("- codex"));
        assert!(yaml.contains("- pi"));
        assert!(yaml.contains("token: 123456:secret"));
        assert!(yaml.contains("entry_chat_id: 5504995202"));
        assert!(yaml.contains("credential_path: ~/.lucarned/wechat-credentials.json"));
        assert!(yaml.contains("force_login: false"));
    }

    #[test]
    fn summary_redacts_telegram_token() {
        let draft = InitConfigDraft {
            agents: AgentSelection::Selected(vec!["codex".into()]),
            telegram: TelegramDraft::Enabled {
                token: "123456:secret".into(),
                entry_chat_id: 1,
                bot_username: Some("bot".into()),
            },
            wechat: WechatDraft::Disabled {
                credential_path: "~/.lucarned/wechat-credentials.json".into(),
            },
        };

        let summary = draft.redacted_summary("/tmp/lucarned.yaml");

        assert!(summary.contains("/tmp/lucarned.yaml"));
        assert!(summary.contains("Telegram: enabled"));
        assert!(summary.contains("bot"));
        assert!(summary.contains("chat 1"));
        assert!(!summary.contains("123456:secret"));
        assert!(summary.contains("<redacted>"));
    }

    #[test]
    fn existing_config_defaults_extracts_enabled_values() {
        let raw = r#"
agents:
  - codex
  - pi
channels:
  telegram:
    enabled: true
    token: existing-token
    entry_chat_id: 99
  wechat:
    enabled: true
    credential_path: ~/.lucarned/existing-wechat.json
"#;

        let defaults = ExistingConfigDefaults::from_yaml(raw).expect("extract defaults");

        assert_eq!(
            defaults.agents,
            AgentSelection::Selected(vec!["codex".into(), "pi".into()])
        );
        assert_eq!(defaults.telegram_enabled, Some(true));
        assert_eq!(defaults.telegram_token.as_deref(), Some("existing-token"));
        assert_eq!(defaults.telegram_entry_chat_id, Some(99));
        assert_eq!(defaults.wechat_enabled, Some(true));
        assert_eq!(
            defaults.wechat_credential_path.as_deref(),
            Some("~/.lucarned/existing-wechat.json")
        );
    }

    #[test]
    fn backup_and_atomic_write_preserves_previous_config() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        std::fs::write(&path, "old-config\n").expect("write old config");

        write_config_with_backup(&path, "new-config\n").expect("write config");

        assert_eq!(
            std::fs::read_to_string(&path).expect("read new config"),
            "new-config\n"
        );
        let backups = std::fs::read_dir(temp.path())
            .expect("read temp dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("lucarned.yaml.bak-"))
            .collect::<Vec<_>>();
        assert_eq!(backups.len(), 1);
        let backup_path = temp.path().join(&backups[0]);
        assert_eq!(
            std::fs::read_to_string(backup_path).expect("read backup"),
            "old-config\n"
        );
    }

    // SEC-001: the written config (and its backup) holds provider secrets, so on
    // unix both must be created owner-only (`0o600`) — never group/world-readable.
    #[cfg(unix)]
    #[test]
    fn written_config_and_backup_are_owner_only_0600() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        std::fs::write(&path, "old-secret\n").expect("write old config");

        write_config_with_backup(&path, "new-secret\n").expect("write config");

        // The replaced config file is 0600.
        let mode = std::fs::metadata(&path)
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "config must be written owner-only (0600)");

        // The backup of the previous contents is 0600 too.
        let backup = std::fs::read_dir(temp.path())
            .expect("read temp dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("lucarned.yaml.bak-"))
            })
            .expect("backup created");
        let backup_mode = std::fs::metadata(&backup)
            .expect("backup metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(backup_mode, 0o600, "backup must be owner-only (0600)");
    }

    #[test]
    fn atomic_write_without_existing_config_creates_parent_dirs() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("nested").join("lucarned.yaml");

        write_config_with_backup(&path, "new-config\n").expect("write config");

        assert_eq!(
            std::fs::read_to_string(&path).expect("read new config"),
            "new-config\n"
        );
        let entries = std::fs::read_dir(path.parent().unwrap())
            .expect("read config dir")
            .count();
        assert_eq!(entries, 1, "new file only, no backup without old config");
    }

    #[test]
    fn relative_path_in_current_dir_writes_config() {
        let _guard = current_dir_lock().lock().expect("cwd lock");
        let original_dir = std::env::current_dir().expect("current dir");
        let temp = tempfile::tempdir().expect("temp dir");
        std::env::set_current_dir(temp.path()).expect("set temp dir");

        let result = write_config_with_backup(Path::new("lucarned.yaml"), "new-config\n");
        let restore_result = std::env::set_current_dir(original_dir);

        restore_result.expect("restore current dir");
        result.expect("write config");
        assert_eq!(
            std::fs::read_to_string(temp.path().join("lucarned.yaml")).expect("read config"),
            "new-config\n"
        );
    }

    #[test]
    fn repeated_writes_create_distinct_backups() {
        let temp = tempfile::tempdir().expect("temp dir");
        let path = temp.path().join("lucarned.yaml");
        std::fs::write(&path, "first\n").expect("write initial config");

        write_config_with_backup(&path, "second\n").expect("write second config");
        write_config_with_backup(&path, "third\n").expect("write third config");

        let mut backup_contents = std::fs::read_dir(temp.path())
            .expect("read temp dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("lucarned.yaml.bak-"))
            })
            .map(|path| std::fs::read_to_string(path).expect("read backup"))
            .collect::<Vec<_>>();
        backup_contents.sort();

        assert_eq!(backup_contents, vec!["first\n", "second\n"]);
        assert_eq!(
            std::fs::read_to_string(&path).expect("read current config"),
            "third\n"
        );
    }
}
