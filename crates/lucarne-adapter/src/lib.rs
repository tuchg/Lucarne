#![allow(clippy::explicit_auto_deref, clippy::too_many_arguments)]

use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use lucarne::LucarneCore;
use serde::Deserialize;
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::{AbortHandle, JoinHandle},
};
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Default)]
pub struct AdapterConfig {
    env: BTreeMap<String, String>,
    file: AdapterFileConfig,
    file_values: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AdapterFileConfig {
    #[serde(default)]
    channels: BTreeMap<String, AdapterChannelConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AdapterChannelConfig {
    enabled: Option<bool>,
    #[serde(default, flatten)]
    values: BTreeMap<String, serde_yaml::Value>,
}

impl AdapterFileConfig {
    fn to_channel_values(&self) -> BTreeMap<String, String> {
        let mut values = BTreeMap::new();
        for (channel, config) in &self.channels {
            for (field, value) in &config.values {
                collect_channel_value(&mut values, channel, field, value);
            }
        }
        values
    }
}

fn collect_channel_value(
    values: &mut BTreeMap<String, String>,
    channel: &str,
    field: &str,
    value: &serde_yaml::Value,
) {
    if let serde_yaml::Value::Mapping(mapping) = value {
        for (key, value) in mapping {
            let Some(key) = key.as_str() else {
                continue;
            };
            collect_channel_value(values, channel, &format!("{field}.{key}"), value);
        }
        return;
    }

    if let Some(value) = yaml_value_to_string(field, value) {
        values.insert(channel_value_key(channel, field), value);
    }
}

fn yaml_value_to_string(field: &str, value: &serde_yaml::Value) -> Option<String> {
    let value = match value {
        serde_yaml::Value::Bool(value) => value.to_string(),
        serde_yaml::Value::Number(value) => value.to_string(),
        serde_yaml::Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                return None;
            }
            if is_path_field(field) {
                expand_home_path(value)
            } else {
                value.to_string()
            }
        }
        serde_yaml::Value::Sequence(values) => values
            .iter()
            .filter_map(|value| yaml_scalar_to_string(field, value))
            .collect::<Vec<_>>()
            .join(","),
        _ => return None,
    };
    (!value.is_empty()).then_some(value)
}

fn yaml_scalar_to_string(field: &str, value: &serde_yaml::Value) -> Option<String> {
    match value {
        serde_yaml::Value::Bool(value) => Some(value.to_string()),
        serde_yaml::Value::Number(value) => Some(value.to_string()),
        serde_yaml::Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                None
            } else if is_path_field(field) {
                Some(expand_home_path(value))
            } else {
                Some(value.to_string())
            }
        }
        _ => None,
    }
}

fn is_path_field(field: &str) -> bool {
    field == "path" || field.ends_with("_path")
}

fn channel_value_key(channel: &str, field: &str) -> String {
    format!("{channel}.{field}")
}

fn expand_home_path(value: &str) -> String {
    if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest).to_string_lossy().into_owned();
        }
    }
    if value == "~" {
        if let Some(home) = home_dir() {
            return home.to_string_lossy().into_owned();
        }
    }
    value.to_string()
}

fn home_dir() -> Option<std::path::PathBuf> {
    home_dir_from_env(EnvReader)
}

#[cfg(not(windows))]
fn home_dir_from_env(env: impl Env) -> Option<std::path::PathBuf> {
    env.var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
}

#[cfg(windows)]
fn home_dir_from_env(env: impl Env) -> Option<std::path::PathBuf> {
    env.var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            env.var_os("USERPROFILE")
                .filter(|value| !value.is_empty())
                .map(std::path::PathBuf::from)
        })
        .or_else(|| {
            let drive = env.var_os("HOMEDRIVE").filter(|value| !value.is_empty())?;
            let path = env.var_os("HOMEPATH").filter(|value| !value.is_empty())?;
            Some(std::path::PathBuf::from(format!(
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

impl AdapterConfig {
    pub fn from_env<I, K, V>(vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            env: vars
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            file: AdapterFileConfig::default(),
            file_values: BTreeMap::new(),
        }
    }

    pub fn from_env_and_file<I, K, V>(vars: I, path: Option<&Path>) -> Result<Self, AdapterError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut config = Self::from_env(vars);
        if let Some(path) = path {
            let raw = std::fs::read_to_string(path).map_err(|err| {
                AdapterError::permanent(format!(
                    "failed to read adapter config {}: {err}",
                    path.display()
                ))
            })?;
            config.file = serde_yaml::from_str(&raw).map_err(|err| {
                AdapterError::permanent(format!(
                    "failed to parse adapter config {}: {err}",
                    path.display()
                ))
            })?;
            config.file_values = config.file.to_channel_values();
        }
        Ok(config)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.env.get(key).map(String::as_str)
    }

    pub fn channel_value(&self, env_key: &str, channel: &str, field: &str) -> Option<&str> {
        self.env
            .get(env_key)
            .or_else(|| self.file_values.get(&channel_value_key(channel, field)))
            .map(String::as_str)
    }

    pub fn channel_enabled(&self, channel: &str) -> Option<bool> {
        self.channel_enabled_from_env(channel).or_else(|| {
            self.file
                .channels
                .get(channel)
                .and_then(|channel| channel.enabled)
        })
    }

    fn channel_enabled_from_env(&self, channel: &str) -> Option<bool> {
        let upper = channel.to_ascii_uppercase().replace('-', "_");
        let keys = [
            format!("LUCARNE_{upper}_ENABLED"),
            format!("LUCARNE_CHANNEL_{upper}_ENABLED"),
        ];
        for key in keys {
            if let Some(value) = self.get(&key) {
                match parse_bool(value) {
                    Some(enabled) => return Some(enabled),
                    None => warn!(
                        target: "lucarne_adapter",
                        key,
                        value,
                        "ignoring invalid channel enablement value"
                    ),
                }
            }
        }
        None
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GlobalConfigUpdate {
    pub bypass: bool,
    pub notifications: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemNotification {
    UpdateAvailable(SystemUpdateNotification),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemUpdateNotification {
    pub current_version: String,
    pub latest_version: String,
    pub release_name: String,
    pub release_url: String,
    pub published_at: Option<String>,
    pub body_markdown: String,
    pub install_hint: String,
}

#[derive(Clone)]
pub struct SystemNotificationBus {
    tx: broadcast::Sender<SystemNotification>,
}

pub struct SystemNotificationReceiver {
    rx: broadcast::Receiver<SystemNotification>,
}

impl SystemNotificationBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    pub fn subscribe(&self) -> SystemNotificationReceiver {
        SystemNotificationReceiver {
            rx: self.tx.subscribe(),
        }
    }

    pub fn send(&self, notification: SystemNotification) -> usize {
        self.tx.send(notification).unwrap_or(0)
    }
}

impl SystemNotificationReceiver {
    pub async fn recv(&mut self) -> Result<SystemNotification, broadcast::error::RecvError> {
        self.rx.recv().await
    }
}

pub trait GlobalConfigPersistence: Send + Sync {
    fn persist_global_config(&self, update: GlobalConfigUpdate) -> AdapterResult<()>;
}

#[derive(Clone)]
pub struct AdapterContext {
    pub core: Arc<LucarneCore>,
    pub config: Arc<AdapterConfig>,
    pub shutdown: watch::Receiver<bool>,
    pub http_client: reqwest::Client,
    pub system_notifications: SystemNotificationBus,
    pub global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
}

pub fn default_http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(60))
        .tcp_nodelay(true)
        .build()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterErrorKind {
    Transient,
    Permanent,
    Fatal,
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct AdapterError {
    kind: AdapterErrorKind,
    message: String,
}

impl AdapterError {
    pub fn message(message: impl Into<String>) -> Self {
        Self::transient(message)
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            kind: AdapterErrorKind::Transient,
            message: message.into(),
        }
    }

    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            kind: AdapterErrorKind::Permanent,
            message: message.into(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            kind: AdapterErrorKind::Fatal,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> AdapterErrorKind {
        self.kind
    }

    pub fn message_text(&self) -> &str {
        &self.message
    }
}

pub type AdapterResult<T> = Result<T, AdapterError>;

pub struct AdapterTask {
    pub id: &'static str,
    pub handle: JoinHandle<AdapterResult<()>>,
}

impl AdapterTask {
    pub fn spawn<F>(id: &'static str, future: F) -> Self
    where
        F: std::future::Future<Output = AdapterResult<()>> + Send + 'static,
    {
        Self {
            id,
            handle: tokio::spawn(future),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdapterState {
    Starting,
    Running,
    Backoff,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterStatus {
    pub id: &'static str,
    pub state: AdapterState,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub last_started_at_unix_ms: Option<u64>,
    pub last_stopped_at_unix_ms: Option<u64>,
    pub next_retry_at_unix_ms: Option<u64>,
}

impl AdapterStatus {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            state: AdapterState::Starting,
            restart_count: 0,
            last_error: None,
            last_started_at_unix_ms: None,
            last_stopped_at_unix_ms: None,
            next_retry_at_unix_ms: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdapterSupervisorOptions {
    backoff_steps: Vec<Duration>,
    crash_loop_window: Duration,
    crash_loop_limit: usize,
    degraded_probe_interval: Duration,
    history_watch_enabled: bool,
    history_watch_poll_interval: Duration,
}

impl Default for AdapterSupervisorOptions {
    fn default() -> Self {
        Self {
            backoff_steps: vec![
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(5),
                Duration::from_secs(10),
                Duration::from_secs(30),
                Duration::from_secs(60),
            ],
            crash_loop_window: Duration::from_secs(300),
            crash_loop_limit: 5,
            degraded_probe_interval: Duration::from_secs(300),
            history_watch_enabled: true,
            history_watch_poll_interval: Duration::from_millis(50),
        }
    }
}

impl AdapterSupervisorOptions {
    #[cfg(test)]
    fn for_tests() -> Self {
        Self {
            backoff_steps: vec![Duration::from_millis(5)],
            crash_loop_window: Duration::from_secs(1),
            crash_loop_limit: 5,
            degraded_probe_interval: Duration::from_millis(20),
            history_watch_enabled: false,
            history_watch_poll_interval: Duration::from_millis(5),
        }
    }
}

#[derive(Clone)]
pub struct AdapterStatusReader {
    statuses: Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
}

impl AdapterStatusReader {
    pub fn empty() -> Self {
        Self {
            statuses: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub fn status(&self, id: &'static str) -> Option<AdapterStatus> {
        self.statuses
            .read()
            .expect("adapter status lock")
            .get(id)
            .cloned()
    }

    pub fn snapshot(&self) -> Vec<AdapterStatus> {
        self.statuses
            .read()
            .expect("adapter status lock")
            .values()
            .cloned()
            .collect()
    }
}

pub struct AdapterSupervisorHandle {
    status_reader: AdapterStatusReader,
    handles: Vec<JoinHandle<()>>,
    adapter_aborts: Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
    fatal_rx: mpsc::UnboundedReceiver<AdapterFatal>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdapterFatal {
    pub id: &'static str,
    pub error: String,
}

impl AdapterSupervisorHandle {
    pub fn status_reader(&self) -> AdapterStatusReader {
        self.status_reader.clone()
    }

    pub fn status(&self, id: &'static str) -> Option<AdapterStatus> {
        self.status_reader.status(id)
    }

    pub fn snapshot(&self) -> Vec<AdapterStatus> {
        self.status_reader.snapshot()
    }

    pub async fn next_fatal(&mut self) -> Option<AdapterFatal> {
        self.fatal_rx.recv().await
    }
}

impl Drop for AdapterSupervisorHandle {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
        let aborts = self
            .adapter_aborts
            .read()
            .expect("adapter abort lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for abort in aborts {
            abort.abort();
        }
    }
}

#[async_trait]
pub trait AdapterPlugin: Send + Sync {
    fn id(&self) -> &'static str;
    fn name(&self) -> &'static str;
    fn startup_priority(&self) -> i32 {
        0
    }
    fn enabled(&self, config: &AdapterConfig) -> bool;
    async fn spawn(&self, ctx: AdapterContext) -> AdapterResult<AdapterTask>;
}

#[derive(Default)]
pub struct AdapterRegistry {
    plugins: Vec<Arc<dyn AdapterPlugin>>,
}

impl AdapterRegistry {
    pub fn register<P>(&mut self, plugin: P)
    where
        P: AdapterPlugin + 'static,
    {
        info!(
            target: "lucarne_adapter",
            adapter_id = plugin.id(),
            adapter_name = plugin.name(),
            "adapter plugin registered"
        );
        self.plugins.push(Arc::new(plugin));
    }

    pub async fn spawn_enabled(&self, ctx: AdapterContext) -> AdapterResult<Vec<AdapterTask>> {
        let mut enabled = self
            .plugins
            .iter()
            .filter(|plugin| plugin.enabled(&*ctx.config))
            .enumerate()
            .collect::<Vec<_>>();
        enabled.sort_by_key(|(index, plugin)| (plugin.startup_priority(), *index));

        let mut tasks = Vec::with_capacity(enabled.len());
        for (_, plugin) in enabled {
            debug!(
                target: "lucarne_adapter",
                adapter_id = plugin.id(),
                adapter_name = plugin.name(),
                startup_priority = plugin.startup_priority(),
                "adapter plugin enabled"
            );
            tasks.push(plugin.spawn(ctx.clone()).await?);
        }
        info!(
            target: "lucarne_adapter",
            adapter_count = tasks.len(),
            "enabled adapter plugins spawned"
        );
        Ok(tasks)
    }

    pub async fn supervise_enabled(
        &self,
        ctx: AdapterContext,
        options: AdapterSupervisorOptions,
    ) -> AdapterResult<AdapterSupervisorHandle> {
        let mut enabled = self
            .plugins
            .iter()
            .filter(|plugin| plugin.enabled(&*ctx.config))
            .enumerate()
            .collect::<Vec<_>>();
        enabled.sort_by_key(|(index, plugin)| (plugin.startup_priority(), *index));

        let statuses = Arc::new(RwLock::new(BTreeMap::new()));
        let adapter_aborts = Arc::new(RwLock::new(BTreeMap::new()));
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();
        let mut handles =
            Vec::with_capacity(enabled.len() + usize::from(options.history_watch_enabled));
        if options.history_watch_enabled && !enabled.is_empty() {
            handles.push(tokio::spawn(start_history_watch_after_core_subscriber(
                Arc::clone(&ctx.core),
                ctx.shutdown.clone(),
                options.history_watch_poll_interval,
            )));
        }
        for (_, plugin) in enabled {
            let id = plugin.id();
            set_status(&statuses, id, |status| {
                status.state = AdapterState::Starting;
            });
            debug!(
                target: "lucarne_adapter",
                adapter_id = id,
                adapter_name = plugin.name(),
                startup_priority = plugin.startup_priority(),
                "supervised adapter plugin starting"
            );
            match plugin.spawn(ctx.clone()).await {
                Ok(task) => {
                    remember_adapter_task_abort(&adapter_aborts, id, &task);
                    mark_adapter_running(&statuses, id);
                    handles.push(tokio::spawn(supervise_adapter_task(
                        Arc::clone(plugin),
                        ctx.clone(),
                        options.clone(),
                        Arc::clone(&statuses),
                        Arc::clone(&adapter_aborts),
                        fatal_tx.clone(),
                        Some(task),
                        None,
                    )));
                }
                Err(err) if err.kind() == AdapterErrorKind::Fatal => return Err(err),
                Err(err) if err.kind() == AdapterErrorKind::Permanent => {
                    mark_adapter_degraded(&statuses, id, err.message_text().to_string(), None);
                    warn!(
                        target: "lucarne_adapter",
                        adapter_id = id,
                        error = %err,
                        "adapter startup failed permanently"
                    );
                }
                Err(err) => {
                    handles.push(tokio::spawn(supervise_adapter_task(
                        Arc::clone(plugin),
                        ctx.clone(),
                        options.clone(),
                        Arc::clone(&statuses),
                        Arc::clone(&adapter_aborts),
                        fatal_tx.clone(),
                        None,
                        Some(err),
                    )));
                }
            }
        }

        info!(
            target: "lucarne_adapter",
            adapter_count = statuses.read().expect("adapter status lock").len(),
            "enabled adapter plugins supervised"
        );
        Ok(AdapterSupervisorHandle {
            status_reader: AdapterStatusReader { statuses },
            handles,
            adapter_aborts,
            fatal_rx,
        })
    }
}

async fn start_history_watch_after_core_subscriber(
    core: Arc<LucarneCore>,
    mut shutdown: watch::Receiver<bool>,
    poll_interval: Duration,
) {
    lucarne::memory_profile_snapshot!("lucarne_adapter.history_watch.wait_start");
    loop {
        if shutdown_requested(&shutdown) {
            return;
        }
        if core.has_event_subscribers() {
            lucarne::memory_profile_snapshot!("lucarne_adapter.history_watch.after_subscriber");
            match core.start_history_session_watch() {
                Ok(()) => return,
                Err(err) if !core.has_event_subscribers() => {
                    trace_history_watch_subscriber_race(&err);
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_adapter",
                        error = %err,
                        "history watch start failed after adapter subscription"
                    );
                    return;
                }
            }
        }
        if sleep_or_shutdown(poll_interval, &mut shutdown).await {
            return;
        }
    }
}

fn trace_history_watch_subscriber_race(err: &lucarne::core_service::CoreError) {
    tracing::debug!(
        target: "lucarne_adapter",
        error = %err,
        "history watch subscriber disappeared before startup"
    );
}

async fn supervise_adapter_task(
    plugin: Arc<dyn AdapterPlugin>,
    ctx: AdapterContext,
    options: AdapterSupervisorOptions,
    statuses: Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    adapter_aborts: Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
    fatal_tx: mpsc::UnboundedSender<AdapterFatal>,
    mut current_task: Option<AdapterTask>,
    mut startup_error: Option<AdapterError>,
) {
    let id = plugin.id();
    let mut shutdown = ctx.shutdown.clone();
    let mut backoff_index = 0;
    let mut failures = VecDeque::<tokio::time::Instant>::new();

    loop {
        if shutdown_requested(&shutdown) {
            if let Some(task) = current_task.take() {
                abort_adapter_task(id, task, &adapter_aborts);
            }
            mark_adapter_stopped(&statuses, id);
            return;
        }

        if let Some(err) = startup_error.take() {
            if !handle_adapter_failure(
                id,
                err,
                &statuses,
                &options,
                &mut failures,
                &mut backoff_index,
                &fatal_tx,
            )
            .await
            {
                return;
            }
        } else if let Some(task) = current_task.take() {
            match wait_adapter_task(id, task, &mut shutdown, &adapter_aborts).await {
                TaskOutcome::Shutdown => {
                    mark_adapter_stopped(&statuses, id);
                    return;
                }
                TaskOutcome::Result(Ok(())) => {
                    if shutdown_requested(&shutdown) {
                        mark_adapter_stopped(&statuses, id);
                        return;
                    }
                    let err = AdapterError::transient("adapter task stopped unexpectedly");
                    if !handle_adapter_failure(
                        id,
                        err,
                        &statuses,
                        &options,
                        &mut failures,
                        &mut backoff_index,
                        &fatal_tx,
                    )
                    .await
                    {
                        return;
                    }
                }
                TaskOutcome::Result(Err(err)) => {
                    if !handle_adapter_failure(
                        id,
                        err,
                        &statuses,
                        &options,
                        &mut failures,
                        &mut backoff_index,
                        &fatal_tx,
                    )
                    .await
                    {
                        return;
                    }
                }
            }
        }

        let retry_delay = next_retry_delay(&statuses, id)
            .unwrap_or_else(|| current_adapter_backoff_delay(&options, backoff_index));
        if sleep_or_shutdown(retry_delay, &mut shutdown).await {
            mark_adapter_stopped(&statuses, id);
            return;
        }
        set_status(&statuses, id, |status| {
            status.state = AdapterState::Starting;
            status.next_retry_at_unix_ms = None;
        });
        match plugin.spawn(ctx.clone()).await {
            Ok(task) => {
                remember_adapter_task_abort(&adapter_aborts, id, &task);
                mark_adapter_running(&statuses, id);
                current_task = Some(task);
                backoff_index = 0;
            }
            Err(err) if err.kind() == AdapterErrorKind::Fatal => {
                let error = err.message_text().to_string();
                mark_adapter_degraded(&statuses, id, error.clone(), None);
                let _ = fatal_tx.send(AdapterFatal { id, error });
                return;
            }
            Err(err) if err.kind() == AdapterErrorKind::Permanent => {
                mark_adapter_degraded(&statuses, id, err.message_text().to_string(), None);
                return;
            }
            Err(err) => {
                startup_error = Some(err);
            }
        }
    }
}

enum TaskOutcome {
    Shutdown,
    Result(AdapterResult<()>),
}

async fn wait_adapter_task(
    id: &'static str,
    task: AdapterTask,
    shutdown: &mut watch::Receiver<bool>,
    adapter_aborts: &Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
) -> TaskOutcome {
    let mut handle = task.handle;
    remember_adapter_abort(adapter_aborts, id, handle.abort_handle());
    let outcome = tokio::select! {
        changed = shutdown.changed() => {
            if changed.is_err() || *shutdown.borrow() {
                handle.abort();
                TaskOutcome::Shutdown
            } else {
                match handle.await {
                    Ok(result) => TaskOutcome::Result(result),
                    Err(err) => TaskOutcome::Result(Err(AdapterError::transient(format!("adapter task join error: {err}")))),
                }
            }
        }
        outcome = &mut handle => {
            match outcome {
                Ok(result) => TaskOutcome::Result(result),
                Err(err) => TaskOutcome::Result(Err(AdapterError::transient(format!("adapter task join error: {err}")))),
            }
        }
    };
    forget_adapter_abort(adapter_aborts, id);
    outcome
}

async fn handle_adapter_failure(
    id: &'static str,
    err: AdapterError,
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    options: &AdapterSupervisorOptions,
    failures: &mut VecDeque<tokio::time::Instant>,
    backoff_index: &mut usize,
    fatal_tx: &mpsc::UnboundedSender<AdapterFatal>,
) -> bool {
    match err.kind() {
        AdapterErrorKind::Permanent => {
            mark_adapter_degraded(statuses, id, err.message_text().to_string(), None);
            false
        }
        AdapterErrorKind::Fatal => {
            let error = err.message_text().to_string();
            mark_adapter_degraded(statuses, id, error.clone(), None);
            let _ = fatal_tx.send(AdapterFatal { id, error });
            false
        }
        AdapterErrorKind::Transient => {
            let now = tokio::time::Instant::now();
            failures.push_back(now);
            while failures
                .front()
                .is_some_and(|first| now.duration_since(*first) > options.crash_loop_window)
            {
                failures.pop_front();
            }
            let degraded_probe = failures.len() >= options.crash_loop_limit;
            let delay = if degraded_probe {
                failures.clear();
                options.degraded_probe_interval
            } else {
                adapter_backoff_delay(options, backoff_index)
            };
            set_status(statuses, id, |status| {
                status.state = if degraded_probe {
                    AdapterState::Degraded
                } else {
                    AdapterState::Backoff
                };
                status.restart_count = status.restart_count.saturating_add(1);
                status.last_error = Some(err.message_text().to_string());
                status.last_stopped_at_unix_ms = Some(unix_ms_now());
                status.next_retry_at_unix_ms = Some(unix_ms_after(delay));
            });
            warn!(
                target: "lucarne_adapter",
                adapter_id = id,
                error = %err,
                retry_ms = delay.as_millis(),
                "adapter task failed; scheduling restart"
            );
            true
        }
    }
}

fn adapter_backoff_delay(options: &AdapterSupervisorOptions, index: &mut usize) -> Duration {
    let delay = current_adapter_backoff_delay(options, *index);
    if *index + 1 < options.backoff_steps.len() {
        *index += 1;
    }
    delay
}

fn current_adapter_backoff_delay(options: &AdapterSupervisorOptions, index: usize) -> Duration {
    options
        .backoff_steps
        .get(index)
        .copied()
        .or_else(|| options.backoff_steps.last().copied())
        .unwrap_or_else(|| Duration::from_secs(60))
}

fn next_retry_delay(
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    id: &'static str,
) -> Option<Duration> {
    let next = statuses
        .read()
        .expect("adapter status lock")
        .get(id)?
        .next_retry_at_unix_ms?;
    let now = unix_ms_now();
    Some(Duration::from_millis(next.saturating_sub(now)))
}

async fn sleep_or_shutdown(delay: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(delay) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

fn mark_adapter_running(
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    id: &'static str,
) {
    set_status(statuses, id, |status| {
        status.state = AdapterState::Running;
        status.last_started_at_unix_ms = Some(unix_ms_now());
        status.next_retry_at_unix_ms = None;
    });
}

fn mark_adapter_stopped(
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    id: &'static str,
) {
    set_status(statuses, id, |status| {
        status.state = AdapterState::Stopped;
        status.last_stopped_at_unix_ms = Some(unix_ms_now());
        status.next_retry_at_unix_ms = None;
    });
}

fn mark_adapter_degraded(
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    id: &'static str,
    error: String,
    next_retry: Option<Duration>,
) {
    set_status(statuses, id, |status| {
        status.state = AdapterState::Degraded;
        status.last_error = Some(error);
        status.last_stopped_at_unix_ms = Some(unix_ms_now());
        status.next_retry_at_unix_ms = next_retry.map(unix_ms_after);
    });
}

fn remember_adapter_task_abort(
    adapter_aborts: &Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
    id: &'static str,
    task: &AdapterTask,
) {
    remember_adapter_abort(adapter_aborts, id, task.handle.abort_handle());
}

fn remember_adapter_abort(
    adapter_aborts: &Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
    id: &'static str,
    abort: AbortHandle,
) {
    adapter_aborts
        .write()
        .expect("adapter abort lock")
        .insert(id, abort);
}

fn forget_adapter_abort(
    adapter_aborts: &Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
    id: &'static str,
) {
    adapter_aborts
        .write()
        .expect("adapter abort lock")
        .remove(id);
}

fn abort_adapter_task(
    id: &'static str,
    task: AdapterTask,
    adapter_aborts: &Arc<RwLock<BTreeMap<&'static str, AbortHandle>>>,
) {
    task.handle.abort();
    forget_adapter_abort(adapter_aborts, id);
}

fn set_status(
    statuses: &Arc<RwLock<BTreeMap<&'static str, AdapterStatus>>>,
    id: &'static str,
    update: impl FnOnce(&mut AdapterStatus),
) {
    let mut statuses = statuses.write().expect("adapter status lock");
    let status = statuses.entry(id).or_insert_with(|| AdapterStatus::new(id));
    update(status);
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn unix_ms_after(delay: Duration) -> u64 {
    unix_ms_now().saturating_add(delay.as_millis().min(u128::from(u64::MAX)) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lucarne::{
        agent_runtime::AgentRuntime, control_plane::ControlPlaneSqliteStore,
        core_service::HistoryWatchState,
    };
    #[cfg(windows)]
    use std::ffi::OsString;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tokio::sync::Notify;

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
            home_dir_from_env(MapEnv(&env)),
            Some(std::path::PathBuf::from(r"C:\Users\alice"))
        );
    }

    #[cfg(windows)]
    #[test]
    fn expand_home_path_ignores_empty_home_drive_and_home_path_on_windows() {
        let env = BTreeMap::from([("HOMEDRIVE", ""), ("HOMEPATH", r"\Users\alice")]);
        assert_eq!(home_dir_from_env(MapEnv(&env)), None);
    }

    #[test]
    fn adapter_status_reader_empty_has_no_statuses() {
        assert!(AdapterStatusReader::empty().snapshot().is_empty());
    }

    #[test]
    fn adapter_context_exposes_shared_reqwest_client() {
        let source = include_str!("lib.rs")
            .split("\n#[cfg(test)]")
            .next()
            .expect("production source");
        assert!(
            source.contains("pub http_client: reqwest::Client"),
            "AdapterContext must carry one cloned reqwest::Client for all adapters"
        );
        assert!(
            source.contains("pub system_notifications: SystemNotificationBus"),
            "AdapterContext must carry generic system notification bus"
        );
    }

    #[tokio::test]
    async fn system_notification_bus_fans_out_to_subscribers() {
        let bus = SystemNotificationBus::new(16);
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();
        let notification = test_update_notification("0.2.0");

        assert_eq!(bus.send(notification.clone()), 2);
        assert_eq!(first.recv().await.unwrap(), notification);
        assert_eq!(second.recv().await.unwrap(), notification);
    }

    #[test]
    fn system_notification_bus_send_without_receivers_returns_zero() {
        let bus = SystemNotificationBus::new(16);

        assert_eq!(bus.send(test_update_notification("0.2.0")), 0);
    }

    #[tokio::test]
    async fn system_notification_bus_clamps_zero_capacity() {
        let bus = SystemNotificationBus::new(0);
        let mut receiver = bus.subscribe();
        let notification = test_update_notification("0.2.0");

        assert_eq!(bus.send(notification.clone()), 1);
        assert_eq!(receiver.recv().await.unwrap(), notification);
    }

    fn test_update_notification(latest_version: &str) -> SystemNotification {
        SystemNotification::UpdateAvailable(SystemUpdateNotification {
            current_version: "0.1.0".to_string(),
            latest_version: latest_version.to_string(),
            release_name: "Lucarne 0.2.0".to_string(),
            release_url: "https://example.invalid/release".to_string(),
            published_at: Some("2026-05-21T00:00:00Z".to_string()),
            body_markdown: "Update available".to_string(),
            install_hint: "Run installer".to_string(),
        })
    }

    #[test]
    fn channel_enabled_reads_config_file() {
        let path = std::env::temp_dir().join(format!(
            "lucarne-adapter-config-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "channels:\n  telegram:\n    enabled: false\n  wechat:\n    enabled: true\n",
        )
        .expect("write config");

        let config = AdapterConfig::from_env_and_file(Vec::<(String, String)>::new(), Some(&path))
            .expect("load config");

        assert_eq!(config.channel_enabled("telegram"), Some(false));
        assert_eq!(config.channel_enabled("wechat"), Some(true));
        assert_eq!(config.channel_enabled("unknown"), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn config_file_exposes_core_channel_runtime_values() {
        let path = std::env::temp_dir().join(format!(
            "lucarne-adapter-runtime-config-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            r#"
channels:
  telegram:
    enabled: true
    token: telegram-token
    entry_chat_id: 12345
  wechat:
    enabled: true
    credential_path: ~/.lucarned/wechat-credentials.json
    force_login: true
    context:
      ttl_secs: 3600
      expiry_remind_before_secs: 120
      expiry_reminder_template: "还有 {remaining_secs} 秒"
    rate_limit:
      interaction_prompt: "请回复任意消息"
"#,
        )
        .expect("write config");

        let config = AdapterConfig::from_env_and_file(Vec::<(String, String)>::new(), Some(&path))
            .expect("load config");

        assert_eq!(config.channel_enabled("telegram"), Some(true));
        assert_eq!(
            config.channel_value("TELEGRAM_BOT_TOKEN", "telegram", "token"),
            Some("telegram-token")
        );
        assert_eq!(
            config.channel_value("TELEGRAM_CHAT_ID", "telegram", "entry_chat_id"),
            Some("12345")
        );
        assert_eq!(config.channel_enabled("wechat"), Some(true));
        let credential_path = config
            .channel_value("LUCARNE_WECHAT_CRED_PATH", "wechat", "credential_path")
            .expect("wechat credential path");
        let expected_suffix = std::path::PathBuf::from(".lucarned").join("wechat-credentials.json");
        assert!(std::path::Path::new(&credential_path).ends_with(expected_suffix));
        assert_eq!(
            config.channel_value("LUCARNE_WECHAT_FORCE_LOGIN", "wechat", "force_login"),
            Some("true")
        );
        assert_eq!(
            config.channel_value(
                "LUCARNE_WECHAT_CONTEXT_TTL_SECS",
                "wechat",
                "context.ttl_secs"
            ),
            Some("3600")
        );
        assert_eq!(
            config.channel_value(
                "LUCARNE_WECHAT_CONTEXT_EXPIRY_REMIND_BEFORE_SECS",
                "wechat",
                "context.expiry_remind_before_secs"
            ),
            Some("120")
        );
        assert_eq!(
            config.channel_value(
                "LUCARNE_WECHAT_CONTEXT_EXPIRY_REMINDER_TEMPLATE",
                "wechat",
                "context.expiry_reminder_template"
            ),
            Some("还有 {remaining_secs} 秒")
        );
        assert_eq!(
            config.channel_value(
                "LUCARNE_WECHAT_RATE_LIMIT_INTERACTION_PROMPT",
                "wechat",
                "rate_limit.interaction_prompt"
            ),
            Some("请回复任意消息")
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn env_values_override_config_file_values() {
        let path = std::env::temp_dir().join(format!(
            "lucarne-adapter-runtime-env-config-{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "channels:\n  telegram:\n    token: yaml-token\n    entry_chat_id: 1\n",
        )
        .expect("write config");

        let config = AdapterConfig::from_env_and_file(
            vec![
                ("TELEGRAM_BOT_TOKEN", "env-token"),
                ("TELEGRAM_CHAT_ID", "2"),
            ],
            Some(&path),
        )
        .expect("load config");

        assert_eq!(
            config.channel_value("TELEGRAM_BOT_TOKEN", "telegram", "token"),
            Some("env-token")
        );
        assert_eq!(
            config.channel_value("TELEGRAM_CHAT_ID", "telegram", "entry_chat_id"),
            Some("2")
        );

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn channel_enabled_env_overrides_config_file() {
        let path = std::env::temp_dir().join(format!(
            "lucarne-adapter-config-env-{}.yaml",
            std::process::id()
        ));
        std::fs::write(&path, "channels:\n  wechat:\n    enabled: false\n").expect("write config");

        let config =
            AdapterConfig::from_env_and_file(vec![("LUCARNE_WECHAT_ENABLED", "on")], Some(&path))
                .expect("load config");

        assert_eq!(config.channel_enabled("wechat"), Some(true));

        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn spawn_enabled_waits_for_priority_adapter_before_starting_next() {
        let core = test_core();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let ready = Arc::new(Notify::new());
        let mut registry = AdapterRegistry::default();
        registry.register(GatedPlugin {
            order: Arc::clone(&order),
            ready: Arc::clone(&ready),
        });
        registry.register(RecordingPlugin {
            id: "second",
            order: Arc::clone(&order),
        });

        let registry = Arc::new(registry);
        let spawn = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move {
                registry
                    .spawn_enabled(AdapterContext {
                        core,
                        config: Arc::new(AdapterConfig::default()),
                        shutdown: shutdown_rx,
                        http_client: reqwest::Client::new(),
                        system_notifications: SystemNotificationBus::new(16),
                        global_config_persistence: None,
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;

        assert_eq!(
            order.lock().expect("order lock").as_slice(),
            &["first:start"],
            "later adapters must not start while an earlier startup gate is pending"
        );

        ready.notify_one();
        let tasks = spawn.await.expect("spawn task").expect("spawn adapters");
        assert_eq!(
            order.lock().expect("order lock").as_slice(),
            &["first:start", "first:ready", "second:start"]
        );
        for task in tasks {
            task.handle.abort();
        }
    }

    #[tokio::test]
    async fn supervise_enabled_starts_history_watch_after_adapter_subscribes() {
        let core = test_core();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut registry = AdapterRegistry::default();
        registry.register(CoreEventSubscriberPlugin);
        let mut options = AdapterSupervisorOptions::for_tests();
        options.history_watch_enabled = true;

        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core: Arc::clone(&core),
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                options,
            )
            .await
            .expect("start supervised adapter");

        wait_until(|| core.history_watch_status().state != HistoryWatchState::Stopped).await;
        shutdown_tx.send(true).expect("stop supervisor");
        drop(supervisor);
    }

    #[tokio::test]
    async fn supervise_enabled_restarts_transient_task_failure_without_returning_error() {
        let core = test_core();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut registry = AdapterRegistry::default();
        registry.register(FailOnceThenPendingPlugin {
            attempts: Arc::clone(&attempts),
        });

        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core,
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                AdapterSupervisorOptions::for_tests(),
            )
            .await
            .expect("start supervised adapter");

        wait_until(|| attempts.load(Ordering::SeqCst) >= 2).await;
        let status = supervisor.status("transient").expect("adapter status");
        assert_eq!(status.state, AdapterState::Running);
        assert_eq!(status.restart_count, 1);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("temporary network failure")),
            "last error should preserve the transient task error: {status:?}"
        );

        shutdown_tx.send(true).expect("stop supervisor");
    }

    #[tokio::test]
    async fn supervise_enabled_degrades_permanent_startup_error_and_starts_other_adapters() {
        let core = test_core();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let mut registry = AdapterRegistry::default();
        registry.register(PermanentStartupPlugin {
            order: Arc::clone(&order),
        });
        registry.register(RecordingPlugin {
            id: "healthy",
            order: Arc::clone(&order),
        });

        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core,
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                AdapterSupervisorOptions::for_tests(),
            )
            .await
            .expect("start supervised adapters");

        assert_eq!(
            order.lock().expect("order lock").as_slice(),
            &["permanent:start", "second:start"],
            "permanent startup failure must not prevent later adapters from starting"
        );
        let failed = supervisor.status("permanent").expect("failed status");
        assert_eq!(failed.state, AdapterState::Degraded);
        assert_eq!(failed.restart_count, 0);
        assert!(
            failed
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("missing token")),
            "permanent startup error should be visible: {failed:?}"
        );
        let healthy = supervisor.status("healthy").expect("healthy status");
        assert_eq!(healthy.state, AdapterState::Running);
    }

    #[tokio::test]
    async fn supervise_enabled_reports_runtime_fatal_error() {
        let core = test_core();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut registry = AdapterRegistry::default();
        registry.register(FatalTaskPlugin);

        let mut supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core,
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                AdapterSupervisorOptions::for_tests(),
            )
            .await
            .expect("start supervised adapter");

        let fatal = tokio::time::timeout(Duration::from_secs(1), supervisor.next_fatal())
            .await
            .expect("fatal error should be reported")
            .expect("fatal error");
        assert_eq!(fatal.id, "fatal");
        assert!(fatal.error.contains("core invariant broken"));
        let status = supervisor.status("fatal").expect("fatal adapter status");
        assert_eq!(status.state, AdapterState::Degraded);
    }

    #[tokio::test]
    async fn dropping_supervisor_aborts_active_adapter_task() {
        let core = test_core();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let mut registry = AdapterRegistry::default();
        registry.register(PendingDropPlugin {
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
        });

        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core,
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                AdapterSupervisorOptions::for_tests(),
            )
            .await
            .expect("start supervised adapter");
        assert_eq!(
            supervisor
                .status("pending-drop")
                .expect("adapter status")
                .state,
            AdapterState::Running
        );
        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("adapter task should start before supervisor drop is tested");

        drop(supervisor);

        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .expect("dropping the supervisor must abort the active adapter task");
    }

    #[tokio::test]
    async fn supervise_enabled_restarts_adapter_panic_without_stopping_supervisor() {
        let core = test_core();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut registry = AdapterRegistry::default();
        registry.register(PanicOnceThenPendingPlugin {
            attempts: Arc::clone(&attempts),
        });

        let supervisor = registry
            .supervise_enabled(
                AdapterContext {
                    core,
                    config: Arc::new(AdapterConfig::default()),
                    shutdown: shutdown_rx,
                    http_client: reqwest::Client::new(),
                    system_notifications: SystemNotificationBus::new(16),
                    global_config_persistence: None,
                },
                AdapterSupervisorOptions::for_tests(),
            )
            .await
            .expect("start supervised adapter");

        wait_until(|| attempts.load(Ordering::SeqCst) >= 2).await;
        let status = supervisor.status("panic-once").expect("adapter status");
        assert_eq!(status.state, AdapterState::Running);
        assert_eq!(status.restart_count, 1);
        assert!(
            status
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("adapter task join error")),
            "panic should be recorded as a join error: {status:?}"
        );

        shutdown_tx.send(true).expect("stop supervisor");
    }

    #[test]
    fn adapter_backoff_delay_is_bounded_by_configured_cap() {
        let options = AdapterSupervisorOptions {
            backoff_steps: vec![Duration::from_millis(10), Duration::from_millis(20)],
            ..AdapterSupervisorOptions::default()
        };
        let mut index = 0;
        let delays = (0..4)
            .map(|_| {
                let delay = adapter_backoff_delay(&options, &mut index);
                delay.as_millis()
            })
            .collect::<Vec<_>>();

        assert_eq!(delays, vec![10, 20, 20, 20]);
    }

    fn test_core() -> Arc<LucarneCore> {
        let runtime = Arc::new(AgentRuntime::new());
        LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core")
    }

    struct GatedPlugin {
        order: Arc<Mutex<Vec<&'static str>>>,
        ready: Arc<Notify>,
    }

    #[async_trait]
    impl AdapterPlugin for GatedPlugin {
        fn id(&self) -> &'static str {
            "first"
        }

        fn name(&self) -> &'static str {
            "First"
        }

        fn startup_priority(&self) -> i32 {
            -100
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            self.order.lock().expect("order lock").push("first:start");
            self.ready.notified().await;
            self.order.lock().expect("order lock").push("first:ready");
            Ok(AdapterTask::spawn("first", async {
                std::future::pending::<AdapterResult<()>>().await
            }))
        }
    }

    struct RecordingPlugin {
        id: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl AdapterPlugin for RecordingPlugin {
        fn id(&self) -> &'static str {
            self.id
        }

        fn name(&self) -> &'static str {
            self.id
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            self.order.lock().expect("order lock").push("second:start");
            Ok(AdapterTask::spawn(self.id, async {
                std::future::pending::<AdapterResult<()>>().await
            }))
        }
    }

    struct CoreEventSubscriberPlugin;

    #[async_trait]
    impl AdapterPlugin for CoreEventSubscriberPlugin {
        fn id(&self) -> &'static str {
            "core-subscriber"
        }

        fn name(&self) -> &'static str {
            "Core Subscriber"
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            Ok(AdapterTask::spawn("core-subscriber", async move {
                let _core_events = ctx.core.watch_events();
                std::future::pending::<AdapterResult<()>>().await
            }))
        }
    }

    struct FailOnceThenPendingPlugin {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AdapterPlugin for FailOnceThenPendingPlugin {
        fn id(&self) -> &'static str {
            "transient"
        }

        fn name(&self) -> &'static str {
            "Transient"
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(AdapterTask::spawn("transient", async move {
                if attempt == 1 {
                    Err(AdapterError::transient("temporary network failure"))
                } else {
                    std::future::pending::<AdapterResult<()>>().await
                }
            }))
        }
    }

    struct PermanentStartupPlugin {
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl AdapterPlugin for PermanentStartupPlugin {
        fn id(&self) -> &'static str {
            "permanent"
        }

        fn name(&self) -> &'static str {
            "Permanent"
        }

        fn startup_priority(&self) -> i32 {
            -100
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            self.order
                .lock()
                .expect("order lock")
                .push("permanent:start");
            Err(AdapterError::permanent("missing token"))
        }
    }

    struct FatalTaskPlugin;

    #[async_trait]
    impl AdapterPlugin for FatalTaskPlugin {
        fn id(&self) -> &'static str {
            "fatal"
        }

        fn name(&self) -> &'static str {
            "Fatal"
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            Ok(AdapterTask::spawn("fatal", async {
                Err(AdapterError::fatal("core invariant broken"))
            }))
        }
    }

    struct PendingDropPlugin {
        started: Arc<Notify>,
        dropped: Arc<Notify>,
    }

    #[async_trait]
    impl AdapterPlugin for PendingDropPlugin {
        fn id(&self) -> &'static str {
            "pending-drop"
        }

        fn name(&self) -> &'static str {
            "Pending Drop"
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            let started = Arc::clone(&self.started);
            let dropped = Arc::clone(&self.dropped);
            Ok(AdapterTask::spawn("pending-drop", async move {
                let _guard = DropNotify(dropped);
                started.notify_one();
                std::future::pending::<AdapterResult<()>>().await
            }))
        }
    }

    struct DropNotify(Arc<Notify>);

    impl Drop for DropNotify {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }

    struct PanicOnceThenPendingPlugin {
        attempts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AdapterPlugin for PanicOnceThenPendingPlugin {
        fn id(&self) -> &'static str {
            "panic-once"
        }

        fn name(&self) -> &'static str {
            "Panic Once"
        }

        fn enabled(&self, _config: &AdapterConfig) -> bool {
            true
        }

        async fn spawn(&self, _ctx: AdapterContext) -> AdapterResult<AdapterTask> {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(AdapterTask::spawn("panic-once", async move {
                if attempt == 1 {
                    panic!("adapter panic for restart coverage");
                }
                std::future::pending::<AdapterResult<()>>().await
            }))
        }
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        while tokio::time::Instant::now() < deadline {
            if ready() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(ready(), "condition was not reached before timeout");
    }
}
