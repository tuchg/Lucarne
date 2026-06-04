#![allow(
    clippy::io_other_error,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_get_then_check
)]

use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use lucarne::{default_lucarned_home_dir, default_state_db_path, CoreOptions, LucarneCore};
use lucarne_adapter::{
    default_http_client, AdapterConfig, AdapterContext, AdapterError, AdapterPlugin,
    AdapterRegistry, AdapterResult, AdapterStatusReader, AdapterSupervisorHandle,
    AdapterSupervisorOptions, GlobalConfigPersistence, GlobalConfigUpdate, SystemNotification,
    SystemNotificationBus, SystemUpdateNotification,
};
use lucarne_telegram::telegram_plugin;
use lucarne_wechat::wechat_plugin;
use lucarned_ctl::updates::{UpdateNotice, UpdateRuntime, UpdateStateStore};
#[cfg(feature = "terminal-gateway")]
use remote_config::RemoteFileConfig;
use serde::Deserialize;
use tokio::sync::watch;
use tracing::{info, warn};
use tracing_appender::{
    non_blocking::NonBlockingBuilder,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    prelude::*,
};

mod health;
mod onboarding;
#[cfg(feature = "terminal-gateway")]
mod remote;
#[cfg(feature = "terminal-gateway")]
mod remote_cli;
#[cfg(feature = "terminal-gateway")]
mod remote_config;
#[cfg(feature = "tui")]
mod tui;

const DEFAULT_LOG_BUFFERED_LINES: usize = 1024;
const DEFAULT_LOG_MAX_FILES: usize = 16;
const DEFAULT_HEALTH_ADDR: &str = "127.0.0.1:7766";
const DEFAULT_LUCARNED_CONFIG: &str = r#"agents:
  - claude
  - codex
  - copilot
  - gemini
  - pi

state:
  db: ~/.lucarned/state.sqlite3

logging:
  filter: "info,lucarne=debug,lucarned=debug"
  stderr_filter: warn
  dir: ~/.lucarned/logs
  file: null
  max_files: 16
  buffered_lines: 1024

health:
  enabled: false
  addr: 127.0.0.1:7766

remote:
  enabled: false
  provider: cloudflared
  gateway_addr: 127.0.0.1:7800
  control_addr: 127.0.0.1:7801
  auth_token: null
  readonly_token: null
  insecure: false
  # Generic per-provider field maps keyed by provider id (H6c). The selected
  # `provider` above picks which block is used; the daemon passes the fields
  # through to the provider verbatim, so a new backend (frp / relay) is just a
  # new providers.<id> block — no daemon change.
  providers:
    cloudflared:
      token: null
      public_url: null
      binary_path: null
    # Reserved backends (NOT implemented this release — adapter seams only):
    # frp:   { server_addr: "", token: "" }
    # relay: { url: "" }

updates:
  enabled: true
  notify: true
  check_interval_hours: 24
  remind_interval_hours: 24
  repository: tuchg/Lucarne

turn:
  inactivity_secs: 1800
  deadline_secs: 3600

session:
  idle_timeout_secs: 7200

config:
  global:
    bypass: false
    notifications: true

channels:
  telegram:
    enabled: false
    token: ""
    entry_chat_id: null

  wechat:
    enabled: false
    credential_path: ~/.lucarned/wechat-credentials.json
    force_login: false
    context:
      ttl_secs: 7200
      expiry_remind_before_secs: 300
      expiry_reminder_template: "会话将在 {remaining_minutes} 分钟后到期，请回复以保持会话可用。"
    rate_limit:
      retry_after_secs: 90
      max_retries: 3
      interaction_window_secs: 300
      interaction_threshold: 6
      interaction_prompt: "微信主动通知快到发送限制了，请回复任意消息以刷新会话。"
"#;

fn default_lucarned_config() -> std::borrow::Cow<'static, str> {
    #[cfg(windows)]
    {
        std::borrow::Cow::Owned(
            DEFAULT_LUCARNED_CONFIG
                .replace("  db: ~/.lucarned/state.sqlite3", "  db: null")
                .replace("  dir: ~/.lucarned/logs", "  dir: null")
                .replace(
                    "    credential_path: ~/.lucarned/wechat-credentials.json",
                    "    credential_path: null",
                ),
        )
    }
    #[cfg(not(windows))]
    {
        std::borrow::Cow::Borrowed(DEFAULT_LUCARNED_CONFIG)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = match lucarned_ctl::parse(std::env::args_os()) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("error: {}\n", err.message);
            eprintln!("{}", lucarned_ctl::usage());
            std::process::exit(2);
        }
    };
    match command {
        lucarned_ctl::Command::RunDaemon => run_daemon().await,
        lucarned_ctl::Command::Init => onboarding::run_interactive_init().await,
        lucarned_ctl::Command::Doctor | lucarned_ctl::Command::Update => {
            run_ctl_network_command(command).await
        }
        lucarned_ctl::Command::Remote(command) => run_remote_command(command),
        // The TUI is fully synchronous and uses `reqwest::blocking` for its
        // loopback control-plane calls. `reqwest::blocking` builds and drops its
        // own current-thread Tokio runtime; dropping a runtime inside this
        // `#[tokio::main]` async context panics ("Cannot drop a runtime in a
        // context where blocking is not allowed"). Run the whole TUI on a
        // dedicated OS thread with no ambient runtime so the blocking client is
        // safe. The panic hook installed in `tui::run` is process-global, so a
        // panic on that thread still restores the terminal.
        lucarned_ctl::Command::Tui => run_tui_command(),
        command => lucarned_ctl::run(command)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err).into()),
    }
}

#[cfg(feature = "terminal-gateway")]
fn run_remote_command(
    command: lucarned_ctl::RemoteCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    std::thread::spawn(move || remote_cli::run_remote_command(command))
        .join()
        .unwrap_or_else(|_| Err("remote command thread panicked".to_string()))
        .map_err(|err| -> Box<dyn std::error::Error> {
            std::io::Error::new(std::io::ErrorKind::Other, err).into()
        })
}

#[cfg(not(feature = "terminal-gateway"))]
fn run_remote_command(
    _command: lucarned_ctl::RemoteCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    Err("`lucarned remote` requires the `terminal-gateway` Cargo feature".into())
}

#[cfg(feature = "tui")]
fn run_tui_command() -> Result<(), Box<dyn std::error::Error>> {
    std::thread::spawn(tui::run)
        .join()
        .unwrap_or_else(|_| Err("tui thread panicked".to_string()))
        .map_err(|err| -> Box<dyn std::error::Error> {
            std::io::Error::new(std::io::ErrorKind::Other, err).into()
        })
}

#[cfg(not(feature = "tui"))]
fn run_tui_command() -> Result<(), Box<dyn std::error::Error>> {
    Err("`lucarned tui` requires the `tui` Cargo feature".into())
}

async fn run_ctl_network_command(
    command: lucarned_ctl::Command,
) -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();
    let (file_config, update_config_warning) = load_ctl_update_config();
    let update_config = update_config_from_file(&file_config);
    let client = default_http_client()?;
    lucarned_ctl::run_async(command, &client, update_config, update_config_warning)
        .await
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err).into())
}

async fn run_daemon() -> Result<(), Box<dyn std::error::Error>> {
    lucarne::memory_profile_snapshot!("lucarned.main.start");
    dotenvy::dotenv().ok();
    lucarne::memory_profile_snapshot!("lucarned.main.after_dotenv");
    let config_path = resolve_config_path()?;
    let file_config = LucarnedFileConfig::from_path_opt(config_path.as_deref())?;
    init_tracing(&file_config)?;
    lucarne::memory_profile_snapshot!("lucarned.main.after_tracing");

    let config = AdapterConfig::from_env_and_file(std::env::vars(), config_path.as_deref())?;
    lucarne::memory_profile_snapshot!("lucarned.main.after_config_load");
    let health_addr = health_addr_from_config(&file_config)?;

    let config = Arc::new(config);
    let mut registry = AdapterRegistry::default();
    let enabled_adapter_count =
        usize::from(register_if_enabled(&mut registry, wechat_plugin(), &config))
            + usize::from(register_if_enabled(
                &mut registry,
                telegram_plugin(),
                &config,
            ));
    lucarne::memory_profile_snapshot!("lucarned.main.after_register_adapters");

    // H5 + cold-daemon lazy start: when the terminal gateway product capability
    // is built, resolve remote config before the idle-exit decision so the
    // off-tunnel control plane can stay up for `lucarned remote start`.
    let remote_cfg = remote_config_from_config(&file_config)?;
    let remote_enabled = remote_cfg.is_some();

    if enabled_adapter_count == 0 && health_addr.is_none() && !remote_enabled {
        lucarne::memory_profile_snapshot!("lucarned.main.no_enabled_adapters");
        info!(
            config_path = ?config_path,
            "no adapters enabled; edit lucarned config to enable a channel"
        );
        return Ok(());
    }

    let state_db_path = state_db_path_from_config(&file_config)?;
    lucarne::memory_profile_snapshot!("lucarned.main.before_open_sqlite");
    let core = open_core_from_config(&state_db_path, &file_config)?;
    apply_global_config_from_file(&core, &file_config)?;
    lucarne::memory_profile_snapshot!("lucarned.main.after_open_sqlite");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let global_config_persistence = config_path.as_ref().map(|path| {
        Arc::new(YamlGlobalConfigPersistence::new(path.clone())) as Arc<dyn GlobalConfigPersistence>
    });
    let http_client = default_http_client()?;
    let system_notifications = SystemNotificationBus::new(32);
    let update_config = update_config_from_file(&file_config);
    let update_state_path = update_state_path();
    let mut update_runtime =
        UpdateRuntime::new(update_config, UpdateStateStore::new(update_state_path));

    let (mut adapter_supervisor, adapter_status_reader) = if enabled_adapter_count > 0 {
        lucarne::memory_profile_snapshot!("lucarned.main.before_supervise_enabled");
        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core: Arc::clone(&core),
                    config: Arc::clone(&config),
                    shutdown: shutdown_rx,
                    http_client: http_client.clone(),
                    system_notifications: system_notifications.clone(),
                    global_config_persistence: global_config_persistence.clone(),
                },
                AdapterSupervisorOptions::default(),
            )
            .await?;
        lucarne::memory_profile_snapshot!("lucarned.main.after_supervise_enabled");
        let status_reader = supervisor.status_reader();
        (Some(supervisor), status_reader)
    } else {
        (None, AdapterStatusReader::empty())
    };

    if let Some(addr) = health_addr {
        let listener = health::bind_health_listener(addr).await?;
        let addr = listener.local_addr()?;
        let health_state = health::HealthState::new(
            Arc::clone(&core),
            adapter_status_reader.clone(),
            state_db_path.clone(),
        );
        let health_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            if let Err(err) = health::serve_health(listener, health_state, health_shutdown).await {
                warn!(error = %err, "lucarned health server stopped");
            }
        });
        info!(addr = %addr, "lucarned health server started");
    }

    // Remote-access subsystem (Locked decision L6: daemon owns the tunnel
    // lifecycle). Cold-daemon lazy start: the control plane (port 7801) is served
    // from boot regardless of `remote.enabled` — `remote_config_from_config` always
    // resolves to `Some` (resolved BEFORE the idle-exit check — H5). The gateway +
    // tunnel come up lazily on the first `lucarned remote start`, OR immediately
    // when `autostart` (`remote.enabled:true`) is set. Wired to the daemon
    // shutdown signal so a running tunnel is torn down gracefully.
    #[cfg(feature = "terminal-gateway")]
    if let Some(remote_cfg) = remote_cfg {
        match remote::spawn_remote_subsystem(
            remote_cfg,
            core.control_plane_store(),
            shutdown_tx.subscribe(),
        )
        .await
        {
            Ok(handle) => match handle.public_url {
                Some(public_url) => info!(
                    provider = %handle.provider,
                    public_url = %public_url,
                    "lucarned remote tunnel started"
                ),
                None => info!(
                    "lucarned remote control plane ready; tunnel idle — run `lucarned remote start` to open public access"
                ),
            },
            Err(err) => {
                warn!(error = %err, "lucarned remote subsystem failed to start");
            }
        }
    }
    #[cfg(not(feature = "terminal-gateway"))]
    let _ = remote_cfg;

    info!(
        adapters = adapter_status_reader.snapshot().len(),
        config_path = ?config_path,
        "lucarned started supervised adapter tasks"
    );
    lucarne::memory_profile_snapshot!("lucarned.main.before_wait");

    let fatal_error = wait_for_shutdown_or_adapter_fatal(
        adapter_supervisor.as_mut(),
        &mut update_runtime,
        &http_client,
        &system_notifications,
    )
    .await;
    if let Some(fatal) = fatal_error {
        let _ = shutdown_tx.send(true);
        return Err(format!("adapter {} fatal: {}", fatal.id, fatal.error).into());
    }
    let _ = shutdown_tx.send(true);
    info!(
        adapters = adapter_status_reader.snapshot().len(),
        "lucarned shutdown requested"
    );
    Ok(())
}

fn init_tracing(file_config: &LucarnedFileConfig) -> Result<(), Box<dyn std::error::Error>> {
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.start");
    let file_filter_spec = log_filter_spec(file_config);
    let stderr_filter_spec = stderr_log_filter_spec(file_config);
    let stderr_filter = parse_log_filter(&stderr_filter_spec);
    let file_filter = parse_log_filter(&file_filter_spec);
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.after_filters");

    let config = log_file_config_from_config(file_config);
    let log_target = lucarne_log_file_target(file_config)?;
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.before_file_appender");
    let file_appender = lucarne_file_appender(&log_target, config)?;
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.after_file_appender");
    let (file_writer, guard) = NonBlockingBuilder::default()
        .buffered_lines_limit(config.buffered_lines)
        .thread_name("lucarned-log")
        .finish(file_appender);
    Box::leak(Box::new(guard));
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.after_nonblocking");

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_writer(std::io::stderr)
        .with_filter(stderr_filter);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_target(true)
        .with_writer(file_writer)
        .with_filter(file_filter);
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.after_layers");

    let _ = tracing_subscriber::registry()
        .with(stderr_layer)
        .with(file_layer)
        .try_init();
    lucarne::memory_profile_snapshot!("lucarned.init_tracing.after_try_init");
    info!(
        target = %log_target.display(),
        max_files = config.max_files,
        buffered_lines = config.buffered_lines,
        "lucarned file logging enabled"
    );
    Ok(())
}

async fn wait_for_shutdown_or_adapter_fatal(
    mut adapter_supervisor: Option<&mut AdapterSupervisorHandle>,
    update_runtime: &mut UpdateRuntime,
    http_client: &reqwest::Client,
    system_notifications: &SystemNotificationBus,
) -> Option<lucarne_adapter::AdapterFatal> {
    loop {
        if let Some(adapter_supervisor) = adapter_supervisor.as_deref_mut() {
            tokio::select! {
                fatal = adapter_supervisor.next_fatal() => return fatal,
                signal = tokio::signal::ctrl_c() => {
                    if let Err(err) = signal {
                        warn!(error = %err, "failed to wait for ctrl-c signal; shutting down");
                    }
                    return None;
                }
                tick = update_runtime.next_tick(http_client, env!("CARGO_PKG_VERSION")), if update_runtime.enabled() => {
                    handle_update_runtime_tick(system_notifications, update_runtime, tick);
                }
            }
        } else {
            tokio::select! {
                signal = tokio::signal::ctrl_c() => {
                    if let Err(err) = signal {
                        warn!(error = %err, "failed to wait for ctrl-c signal; shutting down");
                    }
                    return None;
                }
                tick = update_runtime.next_tick(http_client, env!("CARGO_PKG_VERSION")), if update_runtime.enabled() => {
                    handle_update_runtime_tick(system_notifications, update_runtime, tick);
                }
            }
        }
    }
}

fn handle_update_runtime_tick(
    system_notifications: &SystemNotificationBus,
    update_runtime: &UpdateRuntime,
    tick: lucarned_ctl::updates::UpdateRuntimeTick,
) {
    for warning in tick.warnings {
        warn!(message = %warning, "update check warning");
    }
    send_update_system_notification(system_notifications, update_runtime, tick.notice);
}

fn send_update_system_notification(
    system_notifications: &SystemNotificationBus,
    update_runtime: &UpdateRuntime,
    notice: Option<UpdateNotice>,
) {
    if let Some(notice) = notice {
        let notification = SystemNotification::UpdateAvailable(SystemUpdateNotification {
            current_version: notice.current_version.clone(),
            latest_version: notice.latest_version.clone(),
            release_name: notice.release_name.clone(),
            release_url: notice.release_url.clone(),
            published_at: notice.published_at.clone(),
            body_markdown: notice.body_markdown.clone(),
            install_hint: notice.install_hint.clone(),
        });
        let receivers = system_notifications.send(notification);
        if receivers > 0 {
            update_runtime.record_notice_delivered(&notice);
            info!(receivers, "sent update system notification");
        } else {
            warn!(
                latest_version = %notice.latest_version,
                "update notification skipped because no adapters were subscribed"
            );
        }
    }
}

fn register_if_enabled<P>(registry: &mut AdapterRegistry, plugin: P, config: &AdapterConfig) -> bool
where
    P: AdapterPlugin + 'static,
{
    let enabled = plugin.enabled(config);
    if enabled {
        registry.register(plugin);
        true
    } else {
        tracing::debug!(
            target: "lucarned",
            adapter_id = plugin.id(),
            adapter_name = plugin.name(),
            "adapter plugin skipped disabled"
        );
        false
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct LucarnedFileConfig {
    #[serde(default)]
    agents: Option<Vec<String>>,
    #[serde(default)]
    state: StateFileConfig,
    #[serde(default)]
    logging: LoggingFileConfig,
    #[serde(default)]
    health: HealthFileConfig,
    #[cfg(feature = "terminal-gateway")]
    #[serde(default)]
    remote: RemoteFileConfig,
    #[serde(default)]
    updates: UpdateFileConfig,
    #[serde(default)]
    turn: TurnFileConfig,
    #[serde(default)]
    session: SessionFileConfig,
    #[serde(default, rename = "config")]
    runtime_config: RuntimeFileConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct StateFileConfig {
    db: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct LoggingFileConfig {
    filter: Option<String>,
    stderr_filter: Option<String>,
    dir: Option<String>,
    file: Option<String>,
    max_files: Option<usize>,
    buffered_lines: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct HealthFileConfig {
    enabled: Option<bool>,
    addr: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct UpdateFileConfig {
    enabled: Option<bool>,
    notify: Option<bool>,
    check_interval_hours: Option<u64>,
    remind_interval_hours: Option<u64>,
    repository: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TurnFileConfig {
    inactivity_secs: Option<u64>,
    deadline_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct SessionFileConfig {
    idle_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct RuntimeFileConfig {
    #[serde(default)]
    global: GlobalRuntimeFileConfig,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct GlobalRuntimeFileConfig {
    bypass: Option<bool>,
    notifications: Option<bool>,
}

impl LucarnedFileConfig {
    fn from_path_opt(path: Option<&Path>) -> Result<Self, Box<dyn std::error::Error>> {
        let Some(path) = path else {
            return Ok(Self::default());
        };
        let raw = std::fs::read_to_string(path)
            .map_err(|err| format!("failed to read lucarned config {}: {err}", path.display()))?;
        Self::from_yaml_str(&raw).map_err(|err| {
            format!("failed to parse lucarned config {}: {err}", path.display()).into()
        })
    }

    fn from_yaml_str(raw: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LogFileConfig {
    buffered_lines: usize,
    max_files: usize,
}

impl Default for LogFileConfig {
    fn default() -> Self {
        Self {
            buffered_lines: DEFAULT_LOG_BUFFERED_LINES,
            max_files: DEFAULT_LOG_MAX_FILES,
        }
    }
}

fn open_core_from_config(
    path: impl AsRef<Path>,
    config: &LucarnedFileConfig,
) -> Result<Arc<LucarneCore>, Box<dyn std::error::Error>> {
    let options = core_options_from_config(config)?;
    match config.agents.as_ref() {
        Some(agents) => Ok(LucarneCore::open_sqlite_with_provider_filter_and_options(
            path, agents, options,
        )?),
        None => Ok(LucarneCore::open_sqlite_with_options(path, options)?),
    }
}

fn apply_global_config_from_file(
    core: &LucarneCore,
    config: &LucarnedFileConfig,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut settings = core.system_settings();
    let original = settings.clone();
    if let Some(enabled) = config.runtime_config.global.bypass {
        settings.session.force_bypass_permissions = enabled;
    }
    if let Some(enabled) = config.runtime_config.global.notifications {
        settings.notifications.enabled = enabled;
    }
    if settings == original {
        return Ok(false);
    }
    core.set_system_settings(settings)?;
    Ok(true)
}

fn core_options_from_config(
    config: &LucarnedFileConfig,
) -> Result<CoreOptions, Box<dyn std::error::Error>> {
    let defaults = CoreOptions::default();
    let options = CoreOptions {
        turn_inactivity: env_secs("LUCARNE_TURN_INACTIVITY_SECS")
            .or(config.turn.inactivity_secs)
            .map(Duration::from_secs)
            .unwrap_or(defaults.turn_inactivity),
        turn_deadline: env_secs("LUCARNE_TURN_DEADLINE_SECS")
            .or(config.turn.deadline_secs)
            .map(Duration::from_secs)
            .unwrap_or(defaults.turn_deadline),
        session_idle_timeout: env_secs("LUCARNE_SESSION_IDLE_TIMEOUT_SECS")
            .or(config.session.idle_timeout_secs)
            .map(Duration::from_secs)
            .unwrap_or(defaults.session_idle_timeout),
    };
    validate_core_options(&options)?;
    Ok(options)
}

fn env_secs(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

fn update_config_from_file(config: &LucarnedFileConfig) -> lucarned_ctl::updates::UpdateConfig {
    update_config_from_file_with_env(config, |name| std::env::var(name).ok())
}

fn update_state_path() -> PathBuf {
    default_lucarned_home_dir()
        .map(|home| home.join("update-state.json"))
        .unwrap_or_else(|| PathBuf::from("update-state.json"))
}

fn update_config_from_file_with_env<F>(
    config: &LucarnedFileConfig,
    env: F,
) -> lucarned_ctl::updates::UpdateConfig
where
    F: Fn(&str) -> Option<String>,
{
    let defaults = lucarned_ctl::updates::UpdateConfig::default();
    lucarned_ctl::updates::UpdateConfig {
        enabled: env_bool(&env, "LUCARNED_UPDATES_ENABLED")
            .or(config.updates.enabled)
            .unwrap_or(defaults.enabled),
        notify: env_bool(&env, "LUCARNED_UPDATES_NOTIFY")
            .or(config.updates.notify)
            .unwrap_or(defaults.notify),
        check_interval: env_hours(&env, "LUCARNED_UPDATES_CHECK_INTERVAL_HOURS")
            .or(config.updates.check_interval_hours)
            .map(hours_to_duration)
            .unwrap_or(defaults.check_interval),
        remind_interval: env_hours(&env, "LUCARNED_UPDATES_REMIND_INTERVAL_HOURS")
            .or(config.updates.remind_interval_hours)
            .map(hours_to_duration)
            .unwrap_or(defaults.remind_interval),
        repository: env("LUCARNED_UPDATES_REPOSITORY")
            .or_else(|| config.updates.repository.clone())
            .unwrap_or(defaults.repository),
        startup_delay: defaults.startup_delay,
    }
}

fn env_bool<F>(env: &F, name: &str) -> Option<bool>
where
    F: Fn(&str) -> Option<String>,
{
    env(name).as_deref().and_then(parse_bool)
}

fn env_hours<F>(env: &F, name: &str) -> Option<u64>
where
    F: Fn(&str) -> Option<String>,
{
    env(name)?.parse().ok()
}

fn hours_to_duration(hours: u64) -> Duration {
    Duration::from_secs(hours.max(1).saturating_mul(60 * 60))
}

fn validate_core_options(options: &CoreOptions) -> Result<(), Box<dyn std::error::Error>> {
    if options.turn_inactivity.is_zero() {
        return Err("turn.inactivity_secs must be greater than zero".into());
    }
    if options.turn_deadline.is_zero() {
        return Err("turn.deadline_secs must be greater than zero".into());
    }
    if options.session_idle_timeout.is_zero() {
        return Err("session.idle_timeout_secs must be greater than zero".into());
    }
    if options.turn_deadline < options.turn_inactivity {
        return Err(
            "turn.deadline_secs must be greater than or equal to turn.inactivity_secs".into(),
        );
    }
    Ok(())
}

fn state_db_path_from_config(
    config: &LucarnedFileConfig,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    std::env::var("LUCARNE_STATE_DB")
        .ok()
        .map(PathBuf::from)
        .or_else(|| config.state.db.as_deref().map(expand_home_path))
        .or_else(default_state_db_path)
        .ok_or_else(|| "LUCARNE_STATE_DB default path unavailable".into())
}

fn health_addr_from_config(
    config: &LucarnedFileConfig,
) -> Result<Option<std::net::SocketAddr>, Box<dyn std::error::Error>> {
    let enabled = std::env::var("LUCARNED_HEALTH_ENABLED")
        .ok()
        .as_deref()
        .and_then(parse_bool)
        .or(config.health.enabled)
        .unwrap_or(false);
    if !enabled {
        return Ok(None);
    }

    let addr = std::env::var("LUCARNED_HEALTH_ADDR")
        .ok()
        .or_else(|| config.health.addr.clone())
        .unwrap_or_else(|| DEFAULT_HEALTH_ADDR.to_string());
    health::parse_health_addr(&addr)
        .map(Some)
        .map_err(Into::into)
}

#[cfg(feature = "terminal-gateway")]
fn remote_config_from_config(
    config: &LucarnedFileConfig,
) -> Result<Option<remote::RemoteRuntimeConfig>, Box<dyn std::error::Error>> {
    remote_config::remote_config_from_config(&config.remote)
}

#[cfg(not(feature = "terminal-gateway"))]
fn remote_config_from_config(
    _config: &LucarnedFileConfig,
) -> Result<Option<()>, Box<dyn std::error::Error>> {
    Ok(None)
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn log_filter_spec(config: &LucarnedFileConfig) -> String {
    std::env::var("RUST_LOG")
        .ok()
        .or_else(|| config.logging.filter.clone())
        .unwrap_or_else(default_log_filter_spec)
}

fn stderr_log_filter_spec(config: &LucarnedFileConfig) -> String {
    std::env::var("LUCARNE_STDERR_LOG")
        .ok()
        .or_else(|| config.logging.stderr_filter.clone())
        .unwrap_or_else(|| "warn".to_string())
}

fn default_log_filter_spec() -> String {
    "info,lucarne=debug,lucarne::core_service=debug,lucarne::control_plane=debug,lucarne_adapter=debug,lucarne_channel=debug,lucarne_telegram=debug,lucarne_wechat=debug,wechat_ilink=debug,lucarned=debug,agent_sessions=debug"
        .to_string()
}

fn log_file_config_from_config(config: &LucarnedFileConfig) -> LogFileConfig {
    LogFileConfig {
        buffered_lines: config
            .logging
            .buffered_lines
            .unwrap_or(DEFAULT_LOG_BUFFERED_LINES),
        max_files: config.logging.max_files.unwrap_or(DEFAULT_LOG_MAX_FILES),
    }
}

fn parse_log_filter(filter_spec: &str) -> Targets {
    filter_spec
        .parse::<Targets>()
        .unwrap_or_else(|_| Targets::new().with_default(LevelFilter::INFO))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LogFileTarget {
    File(PathBuf),
    Directory(PathBuf),
}

impl LogFileTarget {
    fn display(&self) -> String {
        match self {
            Self::File(path) => path.display().to_string(),
            Self::Directory(path) => format!("{}/lucarned.YYYY-MM-DD.log", path.display()),
        }
    }
}

fn lucarne_file_appender(
    target: &LogFileTarget,
    config: LogFileConfig,
) -> std::io::Result<RollingFileAppender> {
    match target {
        LogFileTarget::File(path) => explicit_file_appender(path),
        LogFileTarget::Directory(path) => daily_directory_appender(path, config),
    }
}

fn explicit_file_appender(path: &Path) -> std::io::Result<RollingFileAppender> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let filename = path
        .file_name()
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing log file name")
        })?
        .to_string_lossy()
        .into_owned();

    RollingFileAppender::builder()
        .rotation(Rotation::NEVER)
        .filename_prefix(filename)
        .build(directory)
        .map_err(log_init_error)
}

fn daily_directory_appender(
    directory: &Path,
    config: LogFileConfig,
) -> std::io::Result<RollingFileAppender> {
    RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("lucarned")
        .filename_suffix("log")
        .max_log_files(config.max_files)
        .build(directory)
        .map_err(log_init_error)
}

fn log_init_error(err: tracing_appender::rolling::InitError) -> std::io::Error {
    std::io::Error::other(err)
}

fn lucarne_log_file_target(
    config: &LucarnedFileConfig,
) -> Result<LogFileTarget, Box<dyn std::error::Error>> {
    if let Ok(path) = std::env::var("LUCARNE_LOG_FILE") {
        return Ok(LogFileTarget::File(PathBuf::from(path)));
    }
    if let Ok(path) = std::env::var("LUCARNE_LOG_DIR") {
        return Ok(LogFileTarget::Directory(PathBuf::from(path)));
    }
    let home = default_lucarned_home_dir().ok_or("LUCARNE_LOG_DIR default path unavailable")?;
    log_file_target_from_config(config, &home)
}

fn log_file_target_from_config(
    config: &LucarnedFileConfig,
    default_home: &Path,
) -> Result<LogFileTarget, Box<dyn std::error::Error>> {
    if let Some(path) = config.logging.file.as_deref() {
        return Ok(LogFileTarget::File(expand_home_path(path)));
    }
    if let Some(path) = config.logging.dir.as_deref() {
        return Ok(LogFileTarget::Directory(expand_home_path(path)));
    }
    Ok(default_log_file_target_in(default_home))
}

fn default_log_file_target_in(home: &Path) -> LogFileTarget {
    LogFileTarget::Directory(home.join("logs"))
}

fn resolve_config_path() -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    if let Some(path) = explicit_config_path() {
        return Ok(Some(path));
    }
    let Some(home) = default_lucarned_home_dir() else {
        return Ok(None);
    };
    let path = default_config_path_in(&home);
    ensure_default_config_file(&path)?;
    Ok(Some(path))
}

fn load_ctl_update_config() -> (LucarnedFileConfig, Option<String>) {
    let Some(path) = existing_config_path_for_ctl_command() else {
        return (LucarnedFileConfig::default(), None);
    };
    match LucarnedFileConfig::from_path_opt(Some(&path)) {
        Ok(config) => (config, None),
        Err(err) => (
            LucarnedFileConfig::default(),
            Some(format!(
                "{} could not be read as lucarned config: {err}",
                path.display()
            )),
        ),
    }
}

fn existing_config_path_for_ctl_command() -> Option<PathBuf> {
    explicit_config_path().or_else(|| {
        default_lucarned_home_dir()
            .map(|home| default_config_path_in(&home))
            .filter(|path| path.exists())
    })
}

fn explicit_config_path() -> Option<PathBuf> {
    std::env::var("LUCARNE_CONFIG")
        .or_else(|_| std::env::var("LUCARNED_CONFIG"))
        .ok()
        .map(PathBuf::from)
}

fn ensure_default_config_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => file.write_all(default_lucarned_config().as_bytes())?,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn default_config_path_in(home: &Path) -> PathBuf {
    home.join("lucarned.yaml")
}

fn expand_home_path(value: &str) -> PathBuf {
    if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = user_home_dir() {
            return home.join(rest);
        }
    }
    if value == "~" {
        if let Some(home) = user_home_dir() {
            return home;
        }
    }
    PathBuf::from(value)
}

fn user_home_dir() -> Option<PathBuf> {
    user_home_dir_from_env(EnvReader)
}

#[cfg(not(windows))]
fn user_home_dir_from_env(env: impl Env) -> Option<PathBuf> {
    env.var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(windows)]
fn user_home_dir_from_env(env: impl Env) -> Option<PathBuf> {
    env.var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            env.var_os("USERPROFILE")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        })
        .or_else(|| {
            let drive = env.var_os("HOMEDRIVE").filter(|value| !value.is_empty())?;
            let path = env.var_os("HOMEPATH").filter(|value| !value.is_empty())?;
            Some(PathBuf::from(format!(
                "{}{}",
                drive.to_string_lossy(),
                path.to_string_lossy()
            )))
        })
}

trait Env: Copy {
    fn var_os(self, name: &str) -> Option<std::ffi::OsString>;
}

#[derive(Clone, Copy)]
struct EnvReader;

impl Env for EnvReader {
    fn var_os(self, name: &str) -> Option<std::ffi::OsString> {
        std::env::var_os(name)
    }
}

#[derive(Clone, Debug)]
struct YamlGlobalConfigPersistence {
    path: PathBuf,
}

impl YamlGlobalConfigPersistence {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl GlobalConfigPersistence for YamlGlobalConfigPersistence {
    fn persist_global_config(&self, update: GlobalConfigUpdate) -> AdapterResult<()> {
        persist_global_config_to_yaml(&self.path, update).map_err(|err| {
            AdapterError::permanent(format!(
                "failed to write lucarned config {}: {err}",
                self.path.display()
            ))
        })
    }
}

fn persist_global_config_to_yaml(
    path: &Path,
    update: GlobalConfigUpdate,
) -> Result<(), Box<dyn std::error::Error>> {
    let raw = std::fs::read_to_string(path)?;
    let yaml = update_global_config_yaml(&raw, update)?;
    onboarding::config::write_config_with_backup(path, &yaml)
}

fn update_global_config_yaml(
    raw: &str,
    update: GlobalConfigUpdate,
) -> Result<String, serde_yaml::Error> {
    let mut value = serde_yaml::from_str::<serde_yaml::Value>(raw)?;
    let root = ensure_yaml_mapping(&mut value);
    let config = ensure_yaml_child_mapping(root, "config");
    let global = ensure_yaml_child_mapping(config, "global");
    global.insert(
        serde_yaml::Value::String("bypass".to_string()),
        serde_yaml::Value::Bool(update.bypass),
    );
    global.insert(
        serde_yaml::Value::String("notifications".to_string()),
        serde_yaml::Value::Bool(update.notifications),
    );
    serde_yaml::to_string(&value)
}

fn ensure_yaml_mapping(value: &mut serde_yaml::Value) -> &mut serde_yaml::Mapping {
    if !matches!(value, serde_yaml::Value::Mapping(_)) {
        *value = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
    }
    match value {
        serde_yaml::Value::Mapping(mapping) => mapping,
        _ => unreachable!("value was forced to mapping"),
    }
}

fn ensure_yaml_child_mapping<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    key: &str,
) -> &'a mut serde_yaml::Mapping {
    let yaml_key = serde_yaml::Value::String(key.to_string());
    if !matches!(mapping.get(&yaml_key), Some(serde_yaml::Value::Mapping(_))) {
        mapping.insert(
            yaml_key.clone(),
            serde_yaml::Value::Mapping(serde_yaml::Mapping::new()),
        );
    }
    match mapping.get_mut(&yaml_key) {
        Some(serde_yaml::Value::Mapping(child)) => child,
        _ => unreachable!("child was forced to mapping"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    use std::collections::BTreeMap;
    use std::{
        ffi::OsString,
        io::Write,
        sync::{Mutex, OnceLock},
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvRestore {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..).rev() {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn with_env<R>(vars: &[(&'static str, Option<&str>)], f: impl FnOnce() -> R) -> R {
        let _guard = env_lock().lock().expect("env lock");
        let restore = EnvRestore {
            saved: vars
                .iter()
                .map(|(key, _)| (*key, std::env::var_os(key)))
                .collect(),
        };
        for (key, value) in vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        let result = f();
        drop(restore);
        result
    }

    #[cfg(windows)]
    #[derive(Clone, Copy)]
    struct MapEnv<'a>(&'a BTreeMap<&'a str, &'a str>);

    #[cfg(windows)]
    impl Env for MapEnv<'_> {
        fn var_os(self, name: &str) -> Option<OsString> {
            self.0.get(name).map(OsString::from)
        }
    }

    #[cfg(windows)]
    #[test]
    fn expand_home_path_uses_home_drive_and_home_path_on_windows() {
        let env = BTreeMap::from([("HOMEDRIVE", r"C:"), ("HOMEPATH", r"\Users\alice")]);
        assert_eq!(
            user_home_dir_from_env(MapEnv(&env)),
            Some(PathBuf::from(r"C:\Users\alice"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn expand_home_path_ignores_empty_home_drive_and_home_path_on_windows() {
        let env = BTreeMap::from([("HOMEDRIVE", ""), ("HOMEPATH", r"\Users\alice")]);
        assert_eq!(user_home_dir_from_env(MapEnv(&env)), None);
    }

    #[test]
    fn cli_parses_init_subcommand() {
        let command = lucarned_ctl::parse([
            std::ffi::OsString::from("lucarned"),
            std::ffi::OsString::from("init"),
        ])
        .expect("parse init cli");
        assert!(matches!(command, lucarned_ctl::Command::Init));
    }

    #[test]
    fn cli_defaults_to_daemon_without_subcommand() {
        let command =
            lucarned_ctl::parse([std::ffi::OsString::from("lucarned")]).expect("parse daemon cli");
        assert!(matches!(command, lucarned_ctl::Command::RunDaemon));
    }

    #[test]
    fn cli_rejects_unknown_subcommand() {
        let err = lucarned_ctl::parse([
            std::ffi::OsString::from("lucarned"),
            std::ffi::OsString::from("configure"),
        ])
        .expect_err("unknown command fails");
        assert_eq!(err.message, "unknown command: configure");
    }

    #[test]
    fn default_log_file_config_bounds_memory_and_disk_growth() {
        let config = LogFileConfig::default();

        assert_eq!(config.buffered_lines, 1024);
        assert_eq!(config.max_files, 16);
    }

    #[test]
    fn tokio_runtime_uses_two_worker_threads() {
        let source = include_str!("main.rs");
        let compact_source = source
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();

        assert!(compact_source.contains("#[tokio::main(flavor=\"multi_thread\",worker_threads=2)]"));
    }

    #[test]
    fn tui_dispatch_runs_on_a_dedicated_thread_off_the_tokio_runtime() {
        // Regression guard for the `lucarned tui` startup panic
        // ("Cannot drop a runtime in a context where blocking is not allowed"):
        // the TUI uses `reqwest::blocking`, which builds+drops its own runtime,
        // so it MUST NOT run inside the `#[tokio::main]` async context. The Tui
        // dispatch arm must hand off to a dedicated OS thread.
        let source = include_str!("main.rs");
        let compact_source = source
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();

        assert!(
            compact_source.contains("Command::Tui=>run_tui_command()")
                && compact_source.contains("fnrun_tui_command()->Result<(),Box<dynstd::error::Error>>{std::thread::spawn(tui::run)"),
            "the `Command::Tui` arm must run `tui::run` on a dedicated thread, \
             not inside the tokio runtime"
        );
    }

    #[test]
    fn memory_profile_snapshots_are_feature_gated() {
        let manifest = include_str!("../Cargo.toml");
        let source = include_str!("main.rs");
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(source);

        let lucarne_lib = include_str!("../../lucarne/src/lib.rs");
        let lucarne_observability = include_str!("../../lucarne/src/observability.rs");

        assert!(manifest.contains("memory-profiling = [\"lucarne/memory-profiling\"]"));
        assert!(lucarne_lib.contains("macro_rules! memory_profile_snapshot"));
        assert!(lucarne_lib.contains("#[cfg(feature = \"memory-profiling\")]"));
        assert!(production_source
            .contains("lucarne::memory_profile_snapshot!(\"lucarned.main.start\")"));
        assert!(production_source.contains(
            "lucarne::memory_profile_snapshot!(\"lucarned.init_tracing.after_nonblocking\")"
        ));
        assert!(lucarne_observability.contains("LUCARNE_MEMORY_PROFILE_PAUSE_MS"));
    }

    #[test]
    fn default_config_exposes_separate_stderr_filter() {
        assert!(DEFAULT_LUCARNED_CONFIG.contains("stderr_filter: warn"));
    }

    #[test]
    fn default_config_exposes_wechat_rate_limit_knobs() {
        assert!(DEFAULT_LUCARNED_CONFIG.contains("retry_after_secs: 90"));
        assert!(DEFAULT_LUCARNED_CONFIG.contains("max_retries: 3"));
        assert!(DEFAULT_LUCARNED_CONFIG.contains("interaction_window_secs: 300"));
        assert!(DEFAULT_LUCARNED_CONFIG.contains("interaction_threshold: 6"));
        assert!(DEFAULT_LUCARNED_CONFIG.contains(
            "interaction_prompt: \"微信主动通知快到发送限制了，请回复任意消息以刷新会话。\""
        ));
    }

    #[test]
    fn default_log_target_uses_lucarned_home_logs_dir() {
        let temp_dir = tempfile::tempdir().expect("create temp home dir");

        assert_eq!(
            default_log_file_target_in(temp_dir.path()),
            LogFileTarget::Directory(temp_dir.path().join("logs"))
        );
    }

    #[test]
    fn default_adapter_config_path_uses_lucarned_yaml() {
        let temp_dir = tempfile::tempdir().expect("create temp config dir");

        assert_eq!(
            default_config_path_in(temp_dir.path()),
            temp_dir.path().join("lucarned.yaml")
        );
    }

    #[test]
    fn missing_default_config_file_is_bootstrapped() {
        let temp_dir = tempfile::tempdir().expect("create temp config dir");
        let config_path = default_config_path_in(temp_dir.path());

        ensure_default_config_file(&config_path).expect("bootstrap default config");

        let raw = std::fs::read_to_string(&config_path).expect("read bootstrapped config");
        assert!(raw.contains("enabled: false"));
        assert!(raw.contains("health:"));
        assert!(raw.contains("turn:"));
        assert!(raw.contains("inactivity_secs: 1800"));
        assert!(raw.contains("deadline_secs: 3600"));
        assert!(raw.contains("session:"));
        assert!(raw.contains("idle_timeout_secs: 7200"));
        assert!(raw.contains("config:"));
        assert!(raw.contains("global:"));
        assert!(raw.contains("bypass: false"));
        assert!(raw.contains("notifications: true"));
        assert!(raw.contains("updates:"));
        assert!(raw.contains("check_interval_hours: 24"));
        assert!(raw.contains("repository: tuchg/Lucarne"));
        assert!(raw.contains("agents:"));
        assert!(raw.contains("  - claude"));
        assert!(raw.contains("  - codex"));
        assert!(raw.contains("  - copilot"));
        assert!(raw.contains("  - gemini"));
        assert!(raw.contains("  - pi"));
        assert!(raw.contains("telegram:"));
        assert!(raw.contains("wechat:"));
        #[cfg(windows)]
        {
            assert!(raw.contains("  db: null"));
            assert!(raw.contains("  dir: null"));
            assert!(raw.contains("    credential_path: null"));
        }
        #[cfg(not(windows))]
        {
            assert!(raw.contains("  db: ~/.lucarned/state.sqlite3"));
            assert!(raw.contains("  dir: ~/.lucarned/logs"));
            assert!(raw.contains("    credential_path: ~/.lucarned/wechat-credentials.json"));
        }

        let daemon_config = LucarnedFileConfig::from_yaml_str(&raw).expect("parse daemon config");
        assert_eq!(daemon_config.health.enabled, Some(false));
        #[cfg(windows)]
        {
            assert_eq!(daemon_config.state.db, None);
            assert_eq!(daemon_config.logging.dir, None);
        }
        #[cfg(not(windows))]
        {
            assert_eq!(
                daemon_config.state.db.as_deref(),
                Some("~/.lucarned/state.sqlite3")
            );
            assert_eq!(
                daemon_config.logging.dir.as_deref(),
                Some("~/.lucarned/logs")
            );
        }
        assert_eq!(daemon_config.runtime_config.global.bypass, Some(false));
        assert_eq!(
            daemon_config.runtime_config.global.notifications,
            Some(true)
        );
        assert_eq!(daemon_config.updates.enabled, Some(true));
        assert_eq!(daemon_config.updates.notify, Some(true));
        assert_eq!(daemon_config.updates.check_interval_hours, Some(24));
        assert_eq!(daemon_config.updates.remind_interval_hours, Some(24));
        assert_eq!(
            daemon_config.updates.repository.as_deref(),
            Some("tuchg/Lucarne")
        );

        let adapter_config =
            AdapterConfig::from_env_and_file(Vec::<(String, String)>::new(), Some(&config_path))
                .expect("parse adapter config");
        assert_eq!(adapter_config.channel_enabled("telegram"), Some(false));
        assert_eq!(adapter_config.channel_enabled("wechat"), Some(false));
    }

    #[test]
    fn existing_default_config_file_is_not_overwritten() {
        let temp_dir = tempfile::tempdir().expect("create temp config dir");
        let config_path = default_config_path_in(temp_dir.path());
        std::fs::write(&config_path, "channels: {}\n").expect("write existing config");

        ensure_default_config_file(&config_path).expect("bootstrap default config");

        assert_eq!(
            std::fs::read_to_string(&config_path).expect("read existing config"),
            "channels: {}\n"
        );
    }

    #[test]
    fn lucarned_file_config_parses_core_daemon_settings() {
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
state:
  db: ~/.lucarned/custom.sqlite3
logging:
  filter: info,lucarned=debug
  stderr_filter: warn,lucarned=info
  dir: ~/.lucarned/custom-logs
  max_files: 7
  buffered_lines: 64
health:
  enabled: true
  addr: 127.0.0.1:7766
"#,
        )
        .expect("parse lucarned config");

        assert_eq!(
            config.state.db.as_deref(),
            Some("~/.lucarned/custom.sqlite3")
        );
        assert_eq!(
            config.logging.filter.as_deref(),
            Some("info,lucarned=debug")
        );
        assert_eq!(
            config.logging.stderr_filter.as_deref(),
            Some("warn,lucarned=info")
        );
        assert_eq!(
            config.logging.dir.as_deref(),
            Some("~/.lucarned/custom-logs")
        );
        assert_eq!(config.logging.max_files, Some(7));
        assert_eq!(config.logging.buffered_lines, Some(64));
        assert_eq!(config.health.enabled, Some(true));
        assert_eq!(config.health.addr.as_deref(), Some("127.0.0.1:7766"));
    }

    #[test]
    fn lucarned_file_config_parses_update_settings() {
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
updates:
  enabled: false
  notify: false
  check_interval_hours: 6
  remind_interval_hours: 12
  repository: owner/project
"#,
        )
        .expect("parse lucarned update config");

        assert_eq!(config.updates.enabled, Some(false));
        assert_eq!(config.updates.notify, Some(false));
        assert_eq!(config.updates.check_interval_hours, Some(6));
        assert_eq!(config.updates.remind_interval_hours, Some(12));
        assert_eq!(config.updates.repository.as_deref(), Some("owner/project"));
    }

    #[test]
    fn update_config_defaults_clamps_hours_and_honors_env_overrides() {
        let defaults = update_config_from_file_with_env(&LucarnedFileConfig::default(), |_| None);
        assert!(defaults.enabled);
        assert!(defaults.notify);
        assert_eq!(defaults.check_interval, Duration::from_secs(24 * 60 * 60));
        assert_eq!(defaults.remind_interval, Duration::from_secs(24 * 60 * 60));
        assert_eq!(defaults.repository, "tuchg/Lucarne");

        let config = LucarnedFileConfig::from_yaml_str(
            r#"
updates:
  enabled: false
  notify: false
  check_interval_hours: 0
  remind_interval_hours: 2
  repository: file/repo
"#,
        )
        .expect("parse update config");
        let resolved = update_config_from_file_with_env(&config, |name| match name {
            "LUCARNED_UPDATES_ENABLED" => Some("true".to_string()),
            "LUCARNED_UPDATES_REPOSITORY" => Some("env/repo".to_string()),
            _ => None,
        });

        assert!(resolved.enabled);
        assert!(!resolved.notify);
        assert_eq!(resolved.check_interval, Duration::from_secs(60 * 60));
        assert_eq!(resolved.remind_interval, Duration::from_secs(2 * 60 * 60));
        assert_eq!(resolved.repository, "env/repo");
    }

    #[tokio::test]
    async fn core_open_uses_all_agents_when_filter_missing() {
        let temp_dir = tempfile::tempdir().expect("create temp state dir");
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
state:
  db: ~/.lucarned/state.sqlite3
"#,
        )
        .expect("parse unfiltered config");

        let core = open_core_from_config(temp_dir.path().join("state.sqlite3"), &config)
            .expect("open unfiltered core");
        let unfiltered = LucarneCore::open_sqlite(temp_dir.path().join("unfiltered.sqlite3"))
            .expect("open directly unfiltered core");

        assert_eq!(core.provider_ids(), unfiltered.provider_ids());
    }

    #[tokio::test]
    async fn core_open_uses_agent_filter_from_config() {
        let temp_dir = tempfile::tempdir().expect("create temp state dir");
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
agents:
  - codex
  - missing-provider
  - pi
"#,
        )
        .expect("parse filtered config");

        let core = open_core_from_config(temp_dir.path().join("state.sqlite3"), &config)
            .expect("open filtered core");

        assert_eq!(core.provider_ids(), &["codex", "pi"]);
        assert_eq!(core.history_provider_ids(), &["codex", "pi"]);
    }

    #[test]
    fn lucarned_file_config_parses_agent_filter() {
        let missing = LucarnedFileConfig::from_yaml_str(
            r#"
state:
  db: ~/.lucarned/state.sqlite3
"#,
        )
        .expect("parse missing agents config");
        assert_eq!(missing.agents, None);

        let subset = LucarnedFileConfig::from_yaml_str(
            r#"
agents:
  - codex
  - pi
"#,
        )
        .expect("parse subset agents config");
        assert_eq!(subset.agents, Some(vec!["codex".into(), "pi".into()]));

        let empty =
            LucarnedFileConfig::from_yaml_str("agents: []\n").expect("parse empty agents config");
        assert_eq!(empty.agents, Some(Vec::new()));
    }

    #[test]
    fn lucarned_file_config_parses_core_timeouts() {
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
turn:
  inactivity_secs: 1800
  deadline_secs: 3600
session:
  idle_timeout_secs: 7200
"#,
        )
        .expect("parse timeout config");

        assert_eq!(config.turn.inactivity_secs, Some(1800));
        assert_eq!(config.turn.deadline_secs, Some(3600));
        assert_eq!(config.session.idle_timeout_secs, Some(7200));
    }

    #[test]
    fn lucarned_file_config_parses_optional_global_runtime_config() {
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
config:
  global:
    bypass: true
    notifications: false
"#,
        )
        .expect("parse runtime config");

        assert_eq!(config.runtime_config.global.bypass, Some(true));
        assert_eq!(config.runtime_config.global.notifications, Some(false));

        let missing = LucarnedFileConfig::from_yaml_str("channels: {}\n")
            .expect("parse missing runtime config");
        assert_eq!(missing.runtime_config.global.bypass, None);
        assert_eq!(missing.runtime_config.global.notifications, None);
    }

    #[tokio::test]
    async fn apply_global_config_from_file_skips_unchanged_defaults() {
        let temp_dir = tempfile::tempdir().expect("create temp state dir");
        let core =
            LucarneCore::open_sqlite(temp_dir.path().join("state.sqlite3")).expect("open core");
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
config:
  global:
    bypass: false
    notifications: true
"#,
        )
        .expect("parse default runtime config");

        assert!(!apply_global_config_from_file(&core, &config).expect("apply default config"));
        assert!(!core.system_settings().session.force_bypass_permissions);
        assert!(core.system_settings().notifications.enabled);

        let changed = LucarnedFileConfig::from_yaml_str(
            r#"
config:
  global:
    bypass: true
    notifications: false
"#,
        )
        .expect("parse changed runtime config");
        assert!(apply_global_config_from_file(&core, &changed).expect("apply changed config"));
        assert!(core.system_settings().session.force_bypass_permissions);
        assert!(!core.system_settings().notifications.enabled);
        assert!(!apply_global_config_from_file(&core, &changed).expect("reapply changed config"));
    }

    #[tokio::test]
    async fn apply_global_config_from_file_overrides_only_explicit_values() {
        let temp_dir = tempfile::tempdir().expect("create temp state dir");
        let core =
            LucarneCore::open_sqlite(temp_dir.path().join("state.sqlite3")).expect("open core");
        core.set_force_bypass_permissions(true)
            .expect("set initial bypass");
        core.set_global_notifications_enabled(false)
            .expect("set initial notifications");

        let missing = LucarnedFileConfig::from_yaml_str("channels: {}\n")
            .expect("parse missing runtime config");
        apply_global_config_from_file(&core, &missing).expect("apply missing config");
        assert!(core.system_settings().session.force_bypass_permissions);
        assert!(!core.system_settings().notifications.enabled);

        let explicit = LucarnedFileConfig::from_yaml_str(
            r#"
config:
  global:
    bypass: false
    notifications: true
"#,
        )
        .expect("parse explicit runtime config");
        apply_global_config_from_file(&core, &explicit).expect("apply explicit config");
        assert!(!core.system_settings().session.force_bypass_permissions);
        assert!(core.system_settings().notifications.enabled);
    }

    #[test]
    fn update_global_config_yaml_upserts_global_values_and_preserves_channels() {
        let raw = r#"
channels:
  telegram:
    enabled: true
    token: keep-me
config:
  other: keep
"#;

        let updated = update_global_config_yaml(
            raw,
            GlobalConfigUpdate {
                bypass: true,
                notifications: false,
            },
        )
        .expect("update yaml");
        let value: serde_json::Value = serde_yaml::from_str(&updated).expect("parse updated yaml");

        assert_eq!(
            value
                .pointer("/channels/telegram/token")
                .and_then(serde_json::Value::as_str),
            Some("keep-me")
        );
        assert_eq!(
            value
                .pointer("/config/other")
                .and_then(serde_json::Value::as_str),
            Some("keep")
        );
        assert_eq!(
            value
                .pointer("/config/global/bypass")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            value
                .pointer("/config/global/notifications")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn yaml_global_config_persistence_writes_backup_and_config_file() {
        let temp_dir = tempfile::tempdir().expect("create temp config dir");
        let config_path = temp_dir.path().join("lucarned.yaml");
        std::fs::write(&config_path, "channels:\n  wechat:\n    enabled: true\n")
            .expect("write config");
        let persistence = YamlGlobalConfigPersistence::new(config_path.clone());

        persistence
            .persist_global_config(GlobalConfigUpdate {
                bypass: true,
                notifications: false,
            })
            .expect("persist global config");

        let updated = std::fs::read_to_string(&config_path).expect("read updated config");
        let value: serde_json::Value = serde_yaml::from_str(&updated).expect("parse updated yaml");
        assert_eq!(
            value
                .pointer("/config/global/bypass")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            value
                .pointer("/config/global/notifications")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert!(
            std::fs::read_dir(temp_dir.path())
                .expect("read temp dir")
                .any(|entry| entry
                    .expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .starts_with("lucarned.yaml.bak-")),
            "persisting should create a backup for existing config"
        );
    }

    #[test]
    fn core_options_defaults_match_timeout_policy() {
        let options = CoreOptions::default();

        assert_eq!(options.turn_inactivity, Duration::from_secs(1800));
        assert_eq!(options.turn_deadline, Duration::from_secs(3600));
        assert_eq!(options.session_idle_timeout, Duration::from_secs(7200));
    }

    #[test]
    fn core_timeout_validation_rejects_deadline_before_inactivity() {
        let options = CoreOptions {
            turn_inactivity: Duration::from_secs(3600),
            turn_deadline: Duration::from_secs(1800),
            session_idle_timeout: Duration::from_secs(7200),
        };

        let err = validate_core_options(&options).expect_err("invalid timeout ordering");
        assert!(err.to_string().contains("deadline_secs"));
    }

    #[test]
    fn health_config_requires_enabled_gate() {
        let default_config = LucarnedFileConfig::default();
        assert_eq!(
            health_addr_from_config(&default_config).expect("default health config"),
            None
        );

        let addr_only = LucarnedFileConfig::from_yaml_str(
            r#"
health:
  addr: 127.0.0.1:7766
"#,
        )
        .expect("parse addr-only health config");
        assert_eq!(
            health_addr_from_config(&addr_only).expect("addr-only health config"),
            None
        );

        let enabled = LucarnedFileConfig::from_yaml_str(
            r#"
health:
  enabled: true
"#,
        )
        .expect("parse enabled health config");
        assert_eq!(
            health_addr_from_config(&enabled).expect("enabled health config"),
            Some("127.0.0.1:7766".parse().unwrap())
        );
    }

    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn default_config_parses_including_remote_providers() {
        // Regression guard: the bootstrapped default config (which writes
        // remote.providers.cloudflared.{token,public_url,binary_path}: null)
        // must round-trip through the real parse path.
        let cfg = LucarnedFileConfig::from_yaml_str(default_lucarned_config().as_ref())
            .expect("default lucarned config must parse, including remote.providers with nulls");
        assert_eq!(cfg.remote.enabled, Some(false));

        // Regression guard (live-test bug): YAML `null` provider fields MUST
        // resolve to ABSENT — never the literal string "null". Otherwise a
        // quick-tunnel default (token/public_url: null) is misread as a named
        // tunnel and `cloudflared` validation fails ("invalid public_url").
        let resolved = remote_config::resolve_provider_fields(&cfg.remote, "cloudflared");
        assert!(
            !resolved.contains_key("token"),
            "null token must be absent, got {:?}",
            resolved.get("token")
        );
        assert!(
            !resolved.contains_key("public_url"),
            "null public_url must be absent, got {:?}",
            resolved.get("public_url")
        );
    }

    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_file_config_parses_full_section() {
        // H6c: the generic `providers:` map is the new shape.
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
remote:
  enabled: true
  provider: cloudflared
  gateway_addr: 127.0.0.1:7810
  control_addr: 127.0.0.1:7811
  auth_token: secret-token
  readonly_token: readonly-secret-token
  insecure: false
  providers:
    cloudflared:
      token: cf-token
      public_url: https://tunnel.example.com
      binary_path: /usr/local/bin/cloudflared
"#,
        )
        .expect("parse remote config");

        assert_eq!(config.remote.enabled, Some(true));
        assert_eq!(config.remote.provider.as_deref(), Some("cloudflared"));
        assert_eq!(
            config.remote.gateway_addr.as_deref(),
            Some("127.0.0.1:7810")
        );
        assert_eq!(
            config.remote.control_addr.as_deref(),
            Some("127.0.0.1:7811")
        );
        assert_eq!(config.remote.auth_token.as_deref(), Some("secret-token"));
        assert_eq!(
            config.remote.readonly_token.as_deref(),
            Some("readonly-secret-token")
        );
        assert_eq!(config.remote.insecure, Some(false));
        let cf = config
            .remote
            .providers
            .get("cloudflared")
            .expect("cloudflared provider block");
        assert_eq!(cf.get("token").and_then(|v| v.as_deref()), Some("cf-token"));
        assert_eq!(
            cf.get("public_url").and_then(|v| v.as_deref()),
            Some("https://tunnel.example.com")
        );
        assert_eq!(
            cf.get("binary_path").and_then(|v| v.as_deref()),
            Some("/usr/local/bin/cloudflared")
        );
        // resolve_provider_fields surfaces the concrete values for the provider.
        let resolved = remote_config::resolve_provider_fields(&config.remote, "cloudflared");
        assert_eq!(resolved.get("token").map(String::as_str), Some("cf-token"));
        assert_eq!(
            resolved.get("public_url").map(String::as_str),
            Some("https://tunnel.example.com")
        );

        // Back-compat: the legacy provider-owned `cloudflare:` compat section
        // still parses without daemon-owned concrete provider structs (H6c).
        let legacy = LucarnedFileConfig::from_yaml_str(
            r#"
remote:
  enabled: true
  cloudflare:
    token: legacy-token
    public_url: https://legacy.example.com
"#,
        )
        .expect("parse legacy remote config");
        assert_eq!(
            legacy
                .remote
                .extra_sections
                .get("cloudflare")
                .and_then(|section| section.get("token"))
                .and_then(serde_json::Value::as_str),
            Some("legacy-token")
        );
        assert_eq!(
            legacy
                .remote
                .extra_sections
                .get("cloudflare")
                .and_then(|section| section.get("public_url"))
                .and_then(serde_json::Value::as_str),
            Some("https://legacy.example.com")
        );

        // Missing section → all-None defaults (serde(default)), like health.
        let missing =
            LucarnedFileConfig::from_yaml_str("channels: {}\n").expect("parse missing remote");
        assert_eq!(missing.remote.enabled, None);
        assert_eq!(missing.remote.provider, None);
        assert_eq!(missing.remote.control_addr, None);
        assert!(missing.remote.providers.is_empty());
        assert!(!missing.remote.extra_sections.contains_key("cloudflare"));
    }

    // H6c: the generic provider_fields map is resolved with provider-declared
    // compat sections merged first and `providers.<id>` merged over them. The
    // daemon never interprets a provider-specific field name.
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_resolves_generic_provider_fields() {
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_PROVIDER", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
            ],
            || {
                // Generic providers map alone → fields pass through verbatim.
                let generic = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  provider: cloudflared
  providers:
    cloudflared:
      token: gen-token
      public_url: https://gen.example.com
"#,
                )
                .expect("parse generic remote config");
                let resolved = remote_config_from_config(&generic)
                    .expect("generic remote config")
                    .expect("enabled → Some");
                assert_eq!(
                    resolved.provider_fields.get("token").map(String::as_str),
                    Some("gen-token")
                );
                assert_eq!(
                    resolved
                        .provider_fields
                        .get("public_url")
                        .map(String::as_str),
                    Some("https://gen.example.com")
                );

                // Legacy cloudflare block alone → still resolved (back-compat).
                let legacy = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  cloudflare:
    token: legacy-token
    binary_path: /usr/local/bin/cloudflared
"#,
                )
                .expect("parse legacy remote config");
                let resolved = remote_config_from_config(&legacy)
                    .expect("legacy remote config")
                    .expect("enabled → Some");
                assert_eq!(
                    resolved.provider_fields.get("token").map(String::as_str),
                    Some("legacy-token")
                );
                assert_eq!(
                    resolved
                        .provider_fields
                        .get("binary_path")
                        .map(String::as_str),
                    Some("/usr/local/bin/cloudflared")
                );

                // Both set → providers.<id> wins over the legacy block per key.
                let both = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  cloudflare:
    token: legacy-token
    binary_path: /legacy/cloudflared
  providers:
    cloudflared:
      token: generic-token
"#,
                )
                .expect("parse merged remote config");
                let resolved = remote_config_from_config(&both)
                    .expect("merged remote config")
                    .expect("enabled → Some");
                // providers.cloudflared.token overrides the legacy token.
                assert_eq!(
                    resolved.provider_fields.get("token").map(String::as_str),
                    Some("generic-token")
                );
                // A legacy-only key with no generic override is preserved.
                assert_eq!(
                    resolved
                        .provider_fields
                        .get("binary_path")
                        .map(String::as_str),
                    Some("/legacy/cloudflared")
                );

                // A non-cloudflared provider ignores the legacy cloudflare block.
                let other = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  provider: frp
  cloudflare:
    token: should-be-ignored
  providers:
    frp:
      server_addr: relay.example.com
"#,
                )
                .expect("parse other-provider config");
                let resolved = remote_config_from_config(&other)
                    .expect("other-provider remote config")
                    .expect("enabled → Some");
                assert_eq!(resolved.provider, "frp");
                assert!(
                    resolved.provider_fields.get("token").is_none(),
                    "legacy cloudflare block must not bleed into a non-cloudflared provider"
                );
                assert_eq!(
                    resolved
                        .provider_fields
                        .get("server_addr")
                        .map(String::as_str),
                    Some("relay.example.com")
                );
            },
        );
    }
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_requires_enabled_gate_and_honors_env() {
        // Cold-daemon lazy start: lucarned ALWAYS resolves to `Some` (the
        // control plane is served from boot); `remote.enabled` now drives
        // `autostart`, not whether the subsystem exists.
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_PROVIDER", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
                ("LUCARNED_REMOTE_INSECURE", None),
            ],
            || {
                // Default (no remote section) → still Some, with autostart=false
                // (the control plane is up but the tunnel stays idle).
                let default_config = LucarnedFileConfig::default();
                let resolved = remote_config_from_config(&default_config)
                    .expect("default remote config")
                    .expect("lucarned always resolves to Some (lazy start)");
                assert!(
                    !resolved.autostart,
                    "remote disabled (default) → control plane ready, tunnel idle (autostart=false)"
                );
                // The control info is still resolved so `lucarned remote start` works.
                assert_eq!(
                    resolved.control_addr,
                    remote_config::DEFAULT_REMOTE_CONTROL_ADDR.parse().unwrap()
                );

                // Enabled in file, defaults fill provider + loopback gateway addr
                // + the distinct off-tunnel control addr (SEC-002), and autostart
                // is true.
                let enabled = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
"#,
                )
                .expect("parse enabled remote config");
                let resolved = remote_config_from_config(&enabled)
                    .expect("enabled remote config")
                    .expect("enabled → Some");
                assert!(resolved.autostart, "remote.enabled:true → autostart=true");
                assert_eq!(resolved.provider, remote_config::DEFAULT_REMOTE_PROVIDER);
                assert_eq!(
                    resolved.gateway_addr,
                    remote_config::DEFAULT_REMOTE_GATEWAY_ADDR.parse().unwrap()
                );
                assert_eq!(
                    resolved.control_addr,
                    remote_config::DEFAULT_REMOTE_CONTROL_ADDR.parse().unwrap()
                );
                // SEC-002: control plane must be on a DISTINCT port from the gateway.
                assert_ne!(resolved.control_addr.port(), resolved.gateway_addr.port());
                assert!(resolved.auth_token.is_none());
                assert!(!resolved.insecure);
            },
        );

        // Env overrides win over file values (and can enable autostart even when
        // the file disables it).
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", Some("true")),
                ("LUCARNED_REMOTE_PROVIDER", Some("cloudflared")),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", Some("127.0.0.1:7900")),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                (
                    "LUCARNED_REMOTE_AUTH_TOKEN",
                    Some("env-token-0123456789abcdef0123456789"),
                ),
                ("LUCARNED_REMOTE_INSECURE", Some("true")),
            ],
            || {
                let disabled_file = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: false
  gateway_addr: 127.0.0.1:7810
"#,
                )
                .expect("parse remote config");
                let resolved = remote_config_from_config(&disabled_file)
                    .expect("env-enabled remote config")
                    .expect("env enabled → Some");
                assert!(
                    resolved.autostart,
                    "LUCARNED_REMOTE_ENABLED=true overrides remote.enabled:false → autostart=true"
                );
                assert_eq!(resolved.gateway_addr, "127.0.0.1:7900".parse().unwrap());
                // No control addr configured → derived as gateway port + 1.
                assert_eq!(resolved.control_addr, "127.0.0.1:7901".parse().unwrap());
                assert_eq!(
                    resolved.auth_token.as_deref(),
                    Some("env-token-0123456789abcdef0123456789")
                );
                assert!(resolved.insecure);
            },
        );
    }

    // SEC-002: a control_addr sharing the gateway port is rejected (it would
    // re-expose the control plane on the tunnel).
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_rejects_control_addr_sharing_gateway_port() {
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
            ],
            || {
                let same_port = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  gateway_addr: 127.0.0.1:7800
  control_addr: 127.0.0.1:7800
"#,
                )
                .expect("parse shared-port remote config");
                assert!(
                    remote_config_from_config(&same_port).is_err(),
                    "control plane sharing the gateway port must be rejected"
                );
            },
        );
    }

    // SEC-008: a weak explicit auth_token (whitespace / too short) fails closed
    // at config resolution.
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_rejects_weak_auth_token() {
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
            ],
            || {
                let short = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  auth_token: "abc123"
"#,
                )
                .expect("parse short-token remote config");
                assert!(
                    remote_config_from_config(&short).is_err(),
                    "a <32-char auth_token must be rejected (SEC-008)"
                );

                // A strong token (≥32 chars) is accepted.
                let strong = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  auth_token: "0123456789abcdef0123456789abcdef"
"#,
                )
                .expect("parse strong-token remote config");
                let resolved = remote_config_from_config(&strong)
                    .expect("strong token accepted")
                    .expect("enabled → Some");
                assert_eq!(
                    resolved.auth_token.as_deref(),
                    Some("0123456789abcdef0123456789abcdef")
                );
            },
        );
    }

    // SEC-013: a read-only token is resolved when set and validated like the
    // full token (weak ⇒ rejected). Absent ⇒ no read-only tier.
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_resolves_and_validates_readonly_token() {
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
                ("LUCARNED_REMOTE_READONLY_TOKEN", None),
            ],
            || {
                // No readonly token configured → None (no read-only tier).
                let none = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
"#,
                )
                .expect("parse remote config");
                let resolved = remote_config_from_config(&none)
                    .expect("enabled remote config")
                    .expect("enabled → Some");
                assert!(resolved.readonly_token.is_none());

                // A strong readonly token (≥32 chars) is accepted and surfaced.
                let strong = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  auth_token: "0123456789abcdef0123456789abcdef"
  readonly_token: "readonly-0123456789abcdef0123456789"
"#,
                )
                .expect("parse strong-readonly remote config");
                let resolved = remote_config_from_config(&strong)
                    .expect("strong readonly accepted")
                    .expect("enabled → Some");
                assert_eq!(
                    resolved.readonly_token.as_deref(),
                    Some("readonly-0123456789abcdef0123456789")
                );

                // A weak readonly token (<32 chars) is rejected (fail-closed).
                let weak = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  auth_token: "0123456789abcdef0123456789abcdef"
  readonly_token: "short"
"#,
                )
                .expect("parse weak-readonly remote config");
                assert!(
                    remote_config_from_config(&weak).is_err(),
                    "a <32-char readonly_token must be rejected (SEC-008/SEC-013)"
                );
            },
        );
    }
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_config_rejects_non_loopback_gateway_addr() {
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
            ],
            || {
                let public = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
  gateway_addr: 0.0.0.0:7800
"#,
                )
                .expect("parse public-bind remote config");
                // Loopback hardening (L3): a non-loopback gateway bind is refused.
                assert!(remote_config_from_config(&public).is_err());
            },
        );
    }

    #[test]
    fn log_filter_defaults_stderr_to_warn() {
        with_env(&[("RUST_LOG", None), ("LUCARNE_STDERR_LOG", None)], || {
            let config = LucarnedFileConfig::default();

            assert_eq!(log_filter_spec(&config), default_log_filter_spec());
            assert_eq!(stderr_log_filter_spec(&config), "warn");
        });
    }

    #[test]
    fn log_filter_uses_rust_log_for_file_and_dedicated_sources_for_stderr() {
        with_env(
            &[
                ("RUST_LOG", Some("debug,lucarned=trace")),
                ("LUCARNE_STDERR_LOG", None),
            ],
            || {
                let config = LucarnedFileConfig::from_yaml_str(
                    r#"
logging:
  filter: info,lucarned=debug
  stderr_filter: warn,lucarned=info
"#,
                )
                .expect("parse lucarned config");

                assert_eq!(log_filter_spec(&config), "debug,lucarned=trace");
                assert_eq!(stderr_log_filter_spec(&config), "warn,lucarned=info");
            },
        );

        with_env(
            &[("LUCARNE_STDERR_LOG", Some("error,lucarned=warn"))],
            || {
                let config = LucarnedFileConfig::from_yaml_str(
                    r#"
logging:
  stderr_filter: warn,lucarned=info
"#,
                )
                .expect("parse lucarned config");

                assert_eq!(stderr_log_filter_spec(&config), "error,lucarned=warn");
            },
        );
    }

    #[test]
    fn log_config_uses_yaml_when_env_absent() {
        let temp_dir = tempfile::tempdir().expect("create temp home dir");
        let config = LucarnedFileConfig::from_yaml_str(
            r#"
logging:
  dir: logs/custom
  max_files: 3
  buffered_lines: 32
"#,
        )
        .expect("parse lucarned config");

        assert_eq!(
            log_file_target_from_config(&config, temp_dir.path()).expect("log target"),
            LogFileTarget::Directory(PathBuf::from("logs/custom"))
        );
        assert_eq!(log_file_config_from_config(&config).max_files, 3);
        assert_eq!(log_file_config_from_config(&config).buffered_lines, 32);
    }

    #[test]
    fn daemon_registers_adapter_plugins_only_after_enabled_check() {
        let source = include_str!("main.rs");
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(source);

        let compact_source = production_source
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();

        assert!(compact_source.contains("register_if_enabled(&mutregistry,wechat_plugin(),&config"));
        assert!(
            compact_source.contains("register_if_enabled(&mutregistry,telegram_plugin(),&config")
        );
        assert!(production_source.contains("adapter plugin skipped disabled"));
        assert!(!production_source.contains("registry.register(wechat_plugin())"));
        assert!(!production_source.contains("registry.register(telegram_plugin())"));
    }

    #[test]
    fn daemon_exits_idle_before_opening_core_or_http_client() {
        let source = include_str!("main.rs");
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(source);
        let run_daemon_source = production_source
            .split("async fn run_daemon()")
            .nth(1)
            .and_then(|rest| rest.split("fn init_tracing").next())
            .expect("run_daemon body");
        let idle_exit = run_daemon_source
            .find("no adapters enabled; edit lucarned config to enable a channel")
            .expect("idle exit guidance log");
        let open_core = run_daemon_source
            .find("open_core_from_config")
            .expect("core open");
        let http_client = run_daemon_source
            .find("default_http_client()")
            .expect("http client creation");

        assert!(run_daemon_source.contains("enabled_adapter_count == 0 && health_addr.is_none()"));
        // H5: the idle-exit must also account for an enabled remote subsystem so
        // a "remote only" config does not early-exit before the tunnel starts.
        assert!(run_daemon_source.contains("&& !remote_enabled"));
        assert!(idle_exit < open_core);
        assert!(idle_exit < http_client);
    }

    // H5 + cold-daemon lazy start: when the terminal gateway product capability
    // is built, remote config is resolved BEFORE the idle-exit decision and
    // resolves to `Some` so the control plane is served from boot and the daemon
    // does NOT idle-exit.
    #[cfg(feature = "terminal-gateway")]
    #[test]
    fn remote_only_config_keeps_daemon_from_idle_exit() {
        // Source-level: remote_cfg is resolved before the idle-exit guidance log.
        let source = include_str!("main.rs");
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(source);
        let run_daemon_source = production_source
            .split("async fn run_daemon()")
            .nth(1)
            .and_then(|rest| rest.split("fn init_tracing").next())
            .expect("run_daemon body");
        let resolve = run_daemon_source
            .find("remote_config_from_config(&file_config)")
            .expect("remote config resolved in run_daemon");
        let idle_exit = run_daemon_source
            .find("no adapters enabled; edit lucarned config to enable a channel")
            .expect("idle exit guidance log");
        assert!(
            resolve < idle_exit,
            "remote config must be resolved before the idle-exit decision (H5)"
        );

        // Behavioral: even a remote-disabled config (no health, no channels)
        // resolves to Some in lucarned (cold-daemon lazy start: the control
        // plane is served from boot) → remote_enabled true → no idle exit.
        with_env(
            &[
                ("LUCARNED_REMOTE_ENABLED", None),
                ("LUCARNED_REMOTE_GATEWAY_ADDR", None),
                ("LUCARNED_REMOTE_CONTROL_ADDR", None),
                ("LUCARNED_REMOTE_AUTH_TOKEN", None),
                ("LUCARNED_HEALTH_ENABLED", None),
            ],
            || {
                let remote_only = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: true
"#,
                )
                .expect("parse remote-only config");
                // No health enabled.
                assert!(health_addr_from_config(&remote_only)
                    .expect("health addr")
                    .is_none());
                // Remote autostart on → Some → the idle-exit guard's
                // `!remote_enabled` is false, so the daemon stays up.
                assert!(remote_config_from_config(&remote_only)
                    .expect("remote config")
                    .is_some());

                // Cold daemon (remote disabled): STILL resolves to Some so the
                // control plane is up and `lucarned remote start` can reach it — the
                // daemon must not idle-exit even with the tunnel idle.
                let cold = LucarnedFileConfig::from_yaml_str(
                    r#"
remote:
  enabled: false
"#,
                )
                .expect("parse cold remote config");
                let resolved = remote_config_from_config(&cold)
                    .expect("cold remote config")
                    .expect("lucarned resolves to Some even when disabled");
                assert!(
                    !resolved.autostart,
                    "remote.enabled:false → control plane ready, tunnel idle"
                );
            },
        );
    }

    #[test]
    fn daemon_creates_system_notification_bus_and_drives_update_runtime() {
        let source = include_str!("main.rs");
        let production_source = source.split("#[cfg(test)]").next().unwrap_or(source);
        let run_daemon_source = production_source
            .split("async fn run_daemon()")
            .nth(1)
            .and_then(|rest| rest.split("fn init_tracing").next())
            .expect("run_daemon body");
        let wait_source = production_source
            .split("async fn wait_for_shutdown_or_adapter_fatal")
            .nth(1)
            .and_then(|rest| rest.split("fn register_if_enabled").next())
            .expect("wait loop body");

        assert!(run_daemon_source.contains("SystemNotificationBus::new(32)"));
        assert!(run_daemon_source.contains("system_notifications.clone()"));
        assert!(run_daemon_source.contains("UpdateRuntime::new"));
        assert!(run_daemon_source.contains("UpdateStateStore::new(update_state_path)"));
        assert!(run_daemon_source.contains("http_client.clone()"));
        assert!(wait_source.contains("update_runtime.next_tick"));
        assert!(wait_source.contains("SystemNotification::UpdateAvailable"));
        assert!(wait_source.contains("system_notifications.send"));
        assert!(wait_source.contains("sent update system notification"));
        assert!(!wait_source.contains("tokio::spawn"));
    }

    #[test]
    fn log_writer_uses_bounded_buffer_and_daily_file_rotation() {
        let source = include_str!("main.rs");
        let compact_source = source
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        let bounded_builder = concat!("NonBlockingBuilder", "::default()");
        let bounded_limit = concat!(".buffered_lines_limit", "(config.buffered_lines)");

        assert!(source.contains(bounded_builder));
        assert!(compact_source.contains(bounded_limit));
        assert!(source.contains("Rotation::DAILY"));
        assert!(source.contains(".filename_prefix(\"lucarned\")"));
        assert!(source.contains(".filename_suffix(\"log\")"));
        assert!(source.contains(".max_log_files(config.max_files)"));
    }

    #[test]
    fn daily_file_appender_writes_dated_log_in_directory() {
        let temp_dir = tempfile::tempdir().expect("create temp log dir");
        let config = LogFileConfig {
            buffered_lines: 8,
            max_files: 2,
        };
        let target = LogFileTarget::Directory(temp_dir.path().to_path_buf());
        let mut appender = lucarne_file_appender(&target, config).expect("create log appender");

        writeln!(appender, "first line").expect("write first log line");
        appender.flush().expect("flush first log line");

        let files = std::fs::read_dir(temp_dir.path())
            .expect("read log dir")
            .map(|entry| entry.expect("log file").file_name())
            .collect::<Vec<_>>();
        assert!(files.iter().any(|name| {
            let name = name.to_string_lossy();
            name.starts_with("lucarned.") && name.ends_with(".log")
        }));
    }

    #[test]
    fn explicit_log_file_target_writes_exact_file() {
        let temp_dir = tempfile::tempdir().expect("create temp log dir");
        let log_path = temp_dir.path().join("custom.log");
        let config = LogFileConfig {
            buffered_lines: 8,
            max_files: 2,
        };
        let target = LogFileTarget::File(log_path.clone());
        let mut appender = lucarne_file_appender(&target, config).expect("create log appender");

        writeln!(appender, "first line").expect("write first log line");
        appender.flush().expect("flush first log line");

        assert!(log_path.exists());
    }
}
