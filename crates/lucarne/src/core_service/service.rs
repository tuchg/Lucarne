use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use agent_sessions::{
    agent_session::OperationPhase as WatchedOperationPhase, ParseSelection, SessionWatcher,
    WatchChange, WatchConfig, WatchError, WatchEvent, WatchTurnCompleted, WatchUpdate,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use rusqlite::Connection;
use smol_str::SmolStr;
use tokio::sync::broadcast;
use tracing::{debug, info, instrument, trace, warn};

use crate::{
    agent_runtime::{
        AgentActivityStream, AgentCommandResult, AgentError, AgentEventStream, AgentRuntime,
        AgentSession, AgentStatus, Event as AgentEvent, InterventionResponse, KnownAgentProvider,
        OpenSession, ResumeSession, SessionRef,
    },
    control_plane::{
        build_status_snapshot, ActivationPlan, ActivationRequest, ChannelBinding, ChannelBindingId,
        CommandCallbackRecord, CommandCallbackToken, CommandCompletionPolicy, CommandId,
        CommandWorkflow, ControlPlaneError, ControlPlanePersistenceEntity, ControlPlaneSqliteStore,
        ControlPlaneState, ControlPlaneStoreError, EffectiveSettings, ForkWorkspaceSession,
        HistoryOlderCallbackRecord, HistoryOlderCallbackToken, HistoryReplayRecord,
        InterventionCallbackRecord, InterventionCallbackToken, LiveInstanceId, LiveInstanceRecord,
        LiveInstanceState, MessageSessionBinding, PanelRenderId, PanelRenderRecord,
        ProviderSessionId, ProviderSessionRecord, ReconcileOutcome, Revision, ScheduledTaskId,
        ScheduledTaskRecord, StatusSnapshot, SubAgentActionRecord, SubAgentCallbackRecord,
        SubAgentCallbackToken, SubAgentLinkId, SubAgentLinkRecord, SystemSettings, TimelineItem,
        TimelineItemKind, TurnId, TurnPermit, TurnRecord, TurnScheduler, TurnSource, TurnState,
        WorkspaceBinding, WorkspaceId,
    },
    dialect::PermissionMode,
    host::process_table::{self, ProcessAggregate, ProcessSample},
};

use super::{
    AgentResourceEntry, AgentResourceScope, AgentResourceSnapshot, AgentSessionHandle,
    CloseWorkspaceRequest, CoreEvent, CoreEventReceiver, CoreOptions, CoreWorkspaceEventStream,
    DaemonApi, HistoryPage, HistoryProviderCatalogEntry, HistoryWorkspacePage,
    InterruptTurnRequest, InvokeCommandRequest, KillAgentReport, KillAgentRequest, KillAgentTarget,
    KilledAgent, LiveWorkspace, ObservedAgentSession, OpenWorkspaceRequest, OpenedCoreSession,
    ProviderCatalogEntry, ResolvePermissionRequest, ResumeWorkspaceRequest,
    RunDueScheduledTasksRequest, RunScheduledTasksReport, ScheduledTaskRun, ScheduledTaskRunError,
    SubmitTurnRequest, SubmittedTurn, UpsertScheduledTaskRequest, WorkspaceSummary,
};

#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    #[error(transparent)]
    Store(#[from] ControlPlaneStoreError),
    #[error(transparent)]
    Runtime(#[from] AgentError),
    #[error("control-plane: {0:?}")]
    ControlPlane(ControlPlaneError),
    #[error("invalid core state: {0}")]
    InvalidState(String),
    #[error("unknown provider id: {0}")]
    UnknownProvider(String),
    #[error("history watch: {0}")]
    HistoryWatch(String),
    #[error("process snapshot: {0}")]
    ProcessSnapshot(String),
}

impl CoreError {
    pub fn invalid_state(message: impl Into<String>) -> Self {
        Self::InvalidState(message.into())
    }
}

impl From<ControlPlaneError> for CoreError {
    fn from(value: ControlPlaneError) -> Self {
        Self::ControlPlane(value)
    }
}

pub struct LucarneCore {
    runtime: Arc<AgentRuntime>,
    state: Arc<RwLock<ControlPlaneState>>,
    store: ControlPlaneSqliteStore,
    provider_ids: Vec<&'static str>,
    events: broadcast::Sender<CoreEvent>,
    workspace_events: RwLock<HashMap<WorkspaceId, broadcast::Sender<AgentEvent>>>,
    history: crate::history::HistoryIndex,
    live_sessions: Arc<RwLock<HashMap<WorkspaceId, Arc<dyn AgentSession>>>>,
    live_session_generations: Arc<RwLock<HashMap<WorkspaceId, u64>>>,
    live_runtime_terminal_text_claims:
        Arc<RwLock<HashMap<LiveRuntimeTerminalTextClaimKey, Instant>>>,
    next_live_generation: AtomicU64,
    submitted_turn_scheduler: TurnScheduler,
    submitted_turns: Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    submitted_turn_activity: Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    submitted_turn_permits: Arc<RwLock<HashMap<TurnId, TurnPermit>>>,
    live_runtime_turn_claims: Arc<RwLock<HashMap<LiveRuntimeTurnClaimKey, Instant>>>,
    notification_suppression: RwLock<HashMap<WorkspaceId, usize>>,
    notification_suppression_grace: RwLock<HashMap<WorkspaceId, Instant>>,
    history_watch_started: AtomicBool,
    history_watch_start_count: AtomicU64,
    history_watch_status: RwLock<HistoryWatchStatus>,
    observed_sessions: RwLock<HashMap<ProviderSessionId, ObservedAgentSession>>,
    options: CoreOptions,
}

#[derive(Debug, Clone)]
struct SubmittedTurnActivity {
    last_event_at: Instant,
    waiting_intervention: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryWatchState {
    Starting,
    Running,
    Backoff,
    Degraded,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryWatchStatus {
    pub state: HistoryWatchState,
    pub running: bool,
    pub restart_count: u64,
    pub last_error: Option<String>,
    pub next_retry_at_unix_ms: Option<u64>,
}

impl Default for HistoryWatchStatus {
    fn default() -> Self {
        Self {
            state: HistoryWatchState::Stopped,
            running: false,
            restart_count: 0,
            last_error: None,
            next_retry_at_unix_ms: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryWatchLoopExit {
    Disconnected,
}

const LIVE_RUNTIME_TURN_CLAIM_TTL: Duration = Duration::from_secs(30 * 60);
const DIRECT_NOTIFICATION_SUPPRESSION_GRACE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiveRuntimeTurnClaimKey {
    provider_session_id: ProviderSessionId,
    provider_turn_id: SmolStr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LiveRuntimeTerminalTextClaimKey {
    provider_session_id: ProviderSessionId,
    text: SmolStr,
}

fn warn_unknown_provider_filter_ids(enabled_ids: &[String]) {
    use std::collections::HashSet;

    let adapter_ids = crate::adapters::default_adapter_provider_ids();
    let adapter_ids = adapter_ids.into_iter().collect::<HashSet<_>>();
    let requested = enabled_ids
        .iter()
        .map(|id| id.as_str())
        .collect::<HashSet<_>>();
    for id in requested
        .into_iter()
        .filter(|id| !adapter_ids.contains(id) && crate::history::history_provider(id).is_none())
    {
        warn!(target: "lucarne::core_service", provider_id = id, "unknown configured agent provider; skipping");
    }
}

fn history_watch_unix_ms_after(delay: Duration) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .saturating_add(delay.as_millis())
        .min(u128::from(u64::MAX)) as u64
}

fn history_watch_selection() -> ParseSelection {
    ParseSelection::empty().with_meta().with_messages()
}

impl LucarneCore {
    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Arc<Self>, CoreError> {
        Self::open_sqlite_with_options(path, CoreOptions::default())
    }

    pub fn open_sqlite_with_options(
        path: impl AsRef<Path>,
        options: CoreOptions,
    ) -> Result<Arc<Self>, CoreError> {
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.start");
        let runtime = Arc::new(AgentRuntime::new());
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_runtime_new");
        runtime.register_defaults();
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_register_defaults");
        let store = ControlPlaneSqliteStore::open(path.as_ref())?;
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_store_open");
        Self::from_runtime_and_store_with_options(runtime, store, options)
    }

    pub fn open_sqlite_with_provider_filter(
        path: impl AsRef<Path>,
        enabled_ids: &[String],
    ) -> Result<Arc<Self>, CoreError> {
        Self::open_sqlite_with_provider_filter_and_options(
            path,
            enabled_ids,
            CoreOptions::default(),
        )
    }

    pub fn open_sqlite_with_provider_filter_and_options(
        path: impl AsRef<Path>,
        enabled_ids: &[String],
        options: CoreOptions,
    ) -> Result<Arc<Self>, CoreError> {
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.start");
        warn_unknown_provider_filter_ids(enabled_ids);
        let runtime = Arc::new(AgentRuntime::new());
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_runtime_new");
        runtime.register_defaults_filtered(enabled_ids);
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_register_defaults");
        let store = ControlPlaneSqliteStore::open(path.as_ref())?;
        crate::memory_profile_snapshot!("lucarne.core.open_sqlite.after_store_open");
        Self::from_runtime_store_and_history_providers(
            runtime,
            store,
            crate::history::history_providers_for_ids(enabled_ids),
            options,
        )
    }

    pub fn sqlite_connection(&self) -> Arc<Mutex<Connection>> {
        self.store.clone_connection()
    }

    pub fn from_runtime_and_store(
        runtime: Arc<AgentRuntime>,
        store: ControlPlaneSqliteStore,
    ) -> Result<Arc<Self>, CoreError> {
        Self::from_runtime_and_store_with_options(runtime, store, CoreOptions::default())
    }

    pub fn from_runtime_and_store_with_options(
        runtime: Arc<AgentRuntime>,
        store: ControlPlaneSqliteStore,
        options: CoreOptions,
    ) -> Result<Arc<Self>, CoreError> {
        Self::from_runtime_store_and_history_providers(
            runtime,
            store,
            crate::history::history_providers(),
            options,
        )
    }

    fn from_runtime_store_and_history_providers(
        runtime: Arc<AgentRuntime>,
        store: ControlPlaneSqliteStore,
        history_providers: Vec<crate::history::HistoryProviderDescriptor>,
        options: CoreOptions,
    ) -> Result<Arc<Self>, CoreError> {
        crate::memory_profile_snapshot!("lucarne.core.from_runtime_and_store.start");
        let mut state = store.load_control_plane()?.unwrap_or_default();
        state.set_timeline_store(store.clone_connection());
        crate::memory_profile_snapshot!(
            "lucarne.core.from_runtime_and_store.after_load_control_plane"
        );
        let provider_ids = runtime
            .providers()
            .into_iter()
            .map(|provider| provider.id.as_str())
            .collect::<Vec<_>>();
        crate::memory_profile_snapshot!("lucarne.core.from_runtime_and_store.after_provider_ids");
        let (events, _) = broadcast::channel(256);
        info!(
            target: "lucarne::core_service",
            provider_count = provider_ids.len(),
            history_ttl_secs = 30,
            turn_inactivity_secs = options.turn_inactivity.as_secs(),
            turn_deadline_secs = options.turn_deadline.as_secs(),
            session_idle_timeout_secs = options.session_idle_timeout.as_secs(),
            "core service opened"
        );
        Ok(Arc::new(Self {
            runtime,
            state: Arc::new(RwLock::new(state)),
            store,
            history: crate::history::HistoryIndex::new(history_providers, Duration::from_secs(30)),
            provider_ids,
            events,
            workspace_events: RwLock::new(HashMap::new()),
            live_sessions: Arc::new(RwLock::new(HashMap::new())),
            live_session_generations: Arc::new(RwLock::new(HashMap::new())),
            live_runtime_terminal_text_claims: Arc::new(RwLock::new(HashMap::new())),
            next_live_generation: AtomicU64::new(0),
            submitted_turn_scheduler: TurnScheduler::new(),
            submitted_turns: Arc::new(RwLock::new(HashMap::new())),
            submitted_turn_activity: Arc::new(RwLock::new(HashMap::new())),
            submitted_turn_permits: Arc::new(RwLock::new(HashMap::new())),
            live_runtime_turn_claims: Arc::new(RwLock::new(HashMap::new())),
            notification_suppression: RwLock::new(HashMap::new()),
            notification_suppression_grace: RwLock::new(HashMap::new()),
            history_watch_started: AtomicBool::new(false),
            history_watch_start_count: AtomicU64::new(0),
            history_watch_status: RwLock::new(HistoryWatchStatus::default()),
            observed_sessions: RwLock::new(HashMap::new()),
            options,
        }))
    }

    pub fn providers(&self) -> Vec<KnownAgentProvider> {
        self.runtime
            .providers()
            .into_iter()
            .map(|provider| KnownAgentProvider {
                id: provider.id,
                display_name: provider.label.clone(),
                runtime_label: provider.label,
                binary: provider.binary,
            })
            .collect()
    }

    pub fn provider_catalog(&self) -> Vec<ProviderCatalogEntry> {
        self.runtime
            .providers()
            .into_iter()
            .map(|provider| ProviderCatalogEntry {
                provider_id: provider.id.as_str(),
                display_name: provider.label.clone(),
                runtime_label: provider.label,
                binary: provider.binary,
                available: true,
            })
            .collect()
    }

    pub fn provider_ids(&self) -> &[&'static str] {
        &self.provider_ids
    }

    pub fn observed_recent_sessions(&self) -> Vec<ObservedAgentSession> {
        let sessions = self
            .observed_sessions
            .read()
            .expect("observed session registry lock")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut stale = Vec::new();
        let mut sessions = sessions
            .into_iter()
            .filter(|session| {
                if observed_session_process_alive(session) {
                    return true;
                }
                stale.push(session.provider_session_id.clone());
                false
            })
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            let mut observed = self
                .observed_sessions
                .write()
                .expect("observed session registry lock");
            for provider_session_id in stale {
                observed.remove(&provider_session_id);
            }
        }
        sessions.sort_by(|left, right| {
            right
                .last_active_unix
                .cmp(&left.last_active_unix)
                .then_with(|| left.provider_id.cmp(right.provider_id))
                .then_with(|| left.native_resume_ref.cmp(&right.native_resume_ref))
        });
        sessions
    }

    fn observed_recent_sessions_for_scope(
        &self,
        scope: &AgentResourceScope,
    ) -> Vec<ObservedAgentSession> {
        let mut sessions = self.observed_recent_sessions();
        if let AgentResourceScope::Workspace(workspace_id) = scope {
            sessions.retain(|session| &session.workspace_id == workspace_id);
        }
        sessions
    }

    pub fn system_settings(&self) -> SystemSettings {
        self.state
            .read()
            .expect("control plane lock")
            .system_settings()
    }

    pub fn set_system_settings(
        &self,
        settings: SystemSettings,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current == settings {
                return Ok((current, false));
            }
            Ok((state.set_system_settings(settings), true))
        })
    }

    pub fn set_force_bypass_permissions(&self, enabled: bool) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current.session.force_bypass_permissions == enabled {
                return Ok((current, false));
            }
            Ok((state.set_force_bypass_permissions(enabled), true))
        })
    }

    pub fn set_global_notifications_enabled(
        &self,
        enabled: bool,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current.notifications.enabled == enabled {
                return Ok((current, false));
            }
            Ok((state.set_global_notifications_enabled(enabled), true))
        })
    }

    pub fn set_workspace_notifications_enabled(
        &self,
        project_path: &Path,
        enabled: bool,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current
                .workspace
                .get(project_path)
                .and_then(|settings| settings.notifications_enabled)
                == Some(enabled)
            {
                return Ok((current, false));
            }
            Ok((
                state.set_workspace_notifications_enabled(project_path, enabled),
                true,
            ))
        })
    }

    pub fn set_session_notifications_enabled(
        &self,
        provider_session_id: &ProviderSessionId,
        enabled: bool,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current
                .provider_session
                .get(provider_session_id)
                .and_then(|settings| settings.notifications_enabled)
                == Some(enabled)
            {
                return Ok((current, false));
            }
            Ok((
                state.set_session_notifications_enabled(provider_session_id, enabled),
                true,
            ))
        })
    }

    pub fn set_workspace_force_bypass_permissions(
        &self,
        project_path: &Path,
        enabled: bool,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current
                .workspace
                .get(project_path)
                .and_then(|settings| settings.force_bypass_permissions)
                == Some(enabled)
            {
                return Ok((current, false));
            }
            Ok((
                state.set_workspace_force_bypass_permissions(project_path, enabled),
                true,
            ))
        })
    }

    pub fn set_session_force_bypass_permissions(
        &self,
        provider_session_id: &ProviderSessionId,
        enabled: bool,
    ) -> Result<SystemSettings, CoreError> {
        self.mutate_system_settings_and_persist(|state| {
            let current = state.system_settings();
            if current
                .provider_session
                .get(provider_session_id)
                .and_then(|settings| settings.force_bypass_permissions)
                == Some(enabled)
            {
                return Ok((current, false));
            }
            Ok((
                state.set_session_force_bypass_permissions(provider_session_id, enabled),
                true,
            ))
        })
    }

    pub fn effective_settings(
        &self,
        project_path: Option<&Path>,
        provider_session_id: Option<&ProviderSessionId>,
    ) -> EffectiveSettings {
        self.state
            .read()
            .expect("control plane lock")
            .effective_settings(project_path, provider_session_id)
    }

    pub fn history_provider_ids(&self) -> &[&'static str] {
        self.history.provider_ids()
    }

    pub fn history_provider_catalog(&self) -> Vec<HistoryProviderCatalogEntry> {
        self.history
            .provider_catalog()
            .iter()
            .copied()
            .map(|provider| HistoryProviderCatalogEntry {
                provider_id: provider.id(),
                display_name: provider.display_name(),
            })
            .collect()
    }

    pub fn discovered_history_provider_ids(&self) -> Vec<&'static str> {
        self.history.available_provider_ids()
    }

    pub fn watch_events(&self) -> CoreEventReceiver {
        self.events.subscribe()
    }

    pub fn has_event_subscribers(&self) -> bool {
        self.events.receiver_count() > 0
    }

    pub fn start_history_session_watch(self: &Arc<Self>) -> Result<(), CoreError> {
        self.start_history_session_watch_with_config(
            WatchConfig::new()
                .providers(
                    self.history
                        .provider_catalog()
                        .iter()
                        .copied()
                        .map(|provider| provider.watch_provider()),
                )
                .selection(history_watch_selection()),
        )
    }

    pub fn start_history_session_watch_with_config(
        self: &Arc<Self>,
        config: WatchConfig,
    ) -> Result<(), CoreError> {
        crate::memory_profile_snapshot!("lucarne.core.start_history_session_watch.start");
        if self.events.receiver_count() == 0 {
            self.set_history_watch_status(
                HistoryWatchState::Stopped,
                false,
                Some("history watch requires a core event subscriber".into()),
                None,
            );
            return Err(CoreError::HistoryWatch(
                "history watch requires a core event subscriber".into(),
            ));
        }
        if self.history_watch_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        crate::memory_profile_snapshot!(
            "lucarne.core.start_history_session_watch.after_started_flag"
        );
        let handle = tokio::runtime::Handle::try_current().map_err(|err| {
            self.history_watch_started.store(false, Ordering::SeqCst);
            self.set_history_watch_status(
                HistoryWatchState::Stopped,
                false,
                Some(err.to_string()),
                None,
            );
            CoreError::HistoryWatch(err.to_string())
        })?;
        let initial_watcher = self.start_history_watcher_once(&config, Duration::from_secs(1));
        crate::memory_profile_snapshot!(
            "lucarne.core.start_history_session_watch.after_initial_watcher"
        );
        let core = Arc::clone(self);
        handle.spawn(async move {
            let mut next_watcher = initial_watcher;
            let mut restart_backoff = Duration::from_secs(1);
            loop {
                let mut watcher = match next_watcher.take() {
                    Some(watcher) => watcher,
                    None => {
                        tokio::time::sleep(restart_backoff).await;
                        restart_backoff = (restart_backoff * 2).min(Duration::from_secs(60));
                        match core.start_history_watcher_once(&config, restart_backoff) {
                            Some(watcher) => watcher,
                            None => continue,
                        }
                    }
                };
                restart_backoff = Duration::from_secs(1);
                info!(target: "lucarne::core_service", "history session watch started");
                match core.run_history_watch_stream_loop(&mut watcher).await {
                    HistoryWatchLoopExit::Disconnected => {
                        core.mark_history_watch_retry(
                            HistoryWatchState::Backoff,
                            "disconnected",
                            restart_backoff,
                        );
                        warn!(
                            target: "lucarne::core_service",
                            retry_secs = restart_backoff.as_secs(),
                            "history session watch disconnected; restarting"
                        );
                        tokio::time::sleep(restart_backoff).await;
                        restart_backoff = (restart_backoff * 2).min(Duration::from_secs(60));
                    }
                }
            }
        });
        crate::memory_profile_snapshot!(
            "lucarne.core.start_history_session_watch.after_spawn_loop"
        );
        Ok(())
    }

    fn start_history_watcher_once(
        &self,
        config: &WatchConfig,
        retry_after: Duration,
    ) -> Option<SessionWatcher> {
        crate::memory_profile_snapshot!("lucarne.core.start_history_watcher_once.start");
        self.history_watch_start_count
            .fetch_add(1, Ordering::SeqCst);
        self.set_history_watch_status(HistoryWatchState::Starting, false, None, None);
        match SessionWatcher::start(config.clone()) {
            Ok(watcher) => {
                crate::memory_profile_snapshot!(
                    "lucarne.core.start_history_watcher_once.after_watcher_start"
                );
                self.set_history_watch_status(HistoryWatchState::Running, true, None, None);
                Some(watcher)
            }
            Err(WatchError::NoRoots | WatchError::NoProviders) => {
                self.mark_history_watch_retry(
                    HistoryWatchState::Degraded,
                    "provider roots unavailable",
                    retry_after,
                );
                info!(
                    target: "lucarne::core_service",
                    retry_secs = retry_after.as_secs(),
                    "history session watch waiting for provider roots"
                );
                None
            }
            Err(err) => {
                self.mark_history_watch_retry(
                    HistoryWatchState::Backoff,
                    err.to_string(),
                    retry_after,
                );
                warn!(
                    target: "lucarne::core_service",
                    error = %err,
                    retry_secs = retry_after.as_secs(),
                    "history session watch start failed"
                );
                None
            }
        }
    }

    pub fn history_watch_status(&self) -> HistoryWatchStatus {
        self.history_watch_status
            .read()
            .expect("history watch status lock")
            .clone()
    }

    #[cfg(test)]
    fn history_watch_start_count(&self) -> u64 {
        self.history_watch_start_count.load(Ordering::SeqCst)
    }

    fn set_history_watch_status(
        &self,
        state: HistoryWatchState,
        running: bool,
        last_error: Option<String>,
        next_retry: Option<Duration>,
    ) {
        let mut status = self
            .history_watch_status
            .write()
            .expect("history watch status lock");
        status.state = state;
        status.running = running;
        status.last_error = last_error;
        status.next_retry_at_unix_ms = next_retry.map(history_watch_unix_ms_after);
    }

    fn mark_history_watch_retry(
        &self,
        state: HistoryWatchState,
        error: impl Into<String>,
        retry_after: Duration,
    ) {
        let mut status = self
            .history_watch_status
            .write()
            .expect("history watch status lock");
        status.state = state;
        status.running = false;
        status.restart_count = status.restart_count.saturating_add(1);
        status.last_error = Some(error.into());
        status.next_retry_at_unix_ms = Some(history_watch_unix_ms_after(retry_after));
    }

    async fn run_history_watch_stream_loop<S>(&self, stream: &mut S) -> HistoryWatchLoopExit
    where
        S: Stream<Item = Result<Box<[WatchUpdate]>, WatchError>> + Unpin,
    {
        while let Some(result) = stream.next().await {
            match result {
                Ok(updates) => self.handle_history_watch_updates(updates),
                Err(WatchError::Disconnected) => return HistoryWatchLoopExit::Disconnected,
                Err(err) => {
                    self.set_history_watch_status(
                        HistoryWatchState::Running,
                        true,
                        Some(err.to_string()),
                        None,
                    );
                    warn!(
                        target: "lucarne::core_service",
                        error = %err,
                        "history session watch error"
                    );
                }
            }
        }
        HistoryWatchLoopExit::Disconnected
    }

    #[cfg(test)]
    async fn run_history_watch_loop_for_tests(
        &self,
        results: impl IntoIterator<Item = Result<Box<[WatchUpdate]>, WatchError>>,
    ) -> HistoryWatchLoopExit {
        let mut stream = futures::stream::iter(results);
        let exit = self.run_history_watch_stream_loop(&mut stream).await;
        self.mark_history_watch_retry(
            HistoryWatchState::Backoff,
            "disconnected",
            Duration::from_secs(1),
        );
        exit
    }

    pub fn begin_direct_notification_suppression(&self, workspace_id: &WorkspaceId) {
        {
            let mut suppression = self
                .notification_suppression
                .write()
                .expect("notification suppression lock");
            *suppression.entry(workspace_id.clone()).or_default() += 1;
        }
        self.notification_suppression_grace
            .write()
            .expect("notification suppression grace lock")
            .remove(workspace_id);
    }

    pub fn end_direct_notification_suppression(&self, workspace_id: &WorkspaceId) {
        let mut suppression = self
            .notification_suppression
            .write()
            .expect("notification suppression lock");
        if let Some(count) = suppression.get_mut(workspace_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                suppression.remove(workspace_id);
                drop(suppression);
                let now = Instant::now();
                let mut grace = self
                    .notification_suppression_grace
                    .write()
                    .expect("notification suppression grace lock");
                grace.retain(|_, until| *until > now);
                grace.insert(
                    workspace_id.clone(),
                    now + DIRECT_NOTIFICATION_SUPPRESSION_GRACE,
                );
            }
        }
    }

    pub fn direct_notification_suppressed(&self, workspace_id: &WorkspaceId) -> bool {
        self.notification_suppression
            .read()
            .expect("notification suppression lock")
            .get(workspace_id)
            .is_some_and(|count| *count > 0)
    }

    pub fn direct_notification_delivery_suppressed(&self, workspace_id: &WorkspaceId) -> bool {
        if self.direct_notification_suppressed(workspace_id) {
            return true;
        }
        let now = Instant::now();
        let mut grace = self
            .notification_suppression_grace
            .write()
            .expect("notification suppression grace lock");
        match grace.get(workspace_id).copied() {
            Some(until) if until > now => true,
            Some(_) => {
                grace.remove(workspace_id);
                false
            }
            None => false,
        }
    }

    fn watch_workspace_events(&self, workspace_id: WorkspaceId) -> CoreWorkspaceEventStream {
        let events = self.workspace_event_sender(&workspace_id).subscribe();
        CoreWorkspaceEventStream::from_workspace_events(workspace_id, events)
    }

    fn workspace_event_sender(&self, workspace_id: &WorkspaceId) -> broadcast::Sender<AgentEvent> {
        if let Some(sender) = self
            .workspace_events
            .read()
            .expect("workspace event registry lock")
            .get(workspace_id)
            .cloned()
        {
            return sender;
        }
        self.workspace_events
            .write()
            .expect("workspace event registry lock")
            .entry(workspace_id.clone())
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    }

    pub fn list_sessions(&self) -> Vec<WorkspaceSummary> {
        self.list_workspaces()
    }

    pub fn list_workspaces(&self) -> Vec<WorkspaceSummary> {
        self.workspace_bindings()
            .into_iter()
            .map(|binding| self.summary_from_binding(binding))
            .collect()
    }

    pub fn list_history(&self, offset: usize, limit: usize) -> HistoryPage {
        let page = self.history.list_page(offset, limit);
        HistoryPage {
            entries: page.entries,
            total: page.total,
        }
    }

    pub fn list_history_page(&self, offset: usize, limit: usize) -> HistoryPage {
        self.list_history(offset, limit)
    }

    pub fn list_history_filtered(
        &self,
        provider_ids: &[&str],
        project_path: Option<&Path>,
        offset: usize,
        limit: usize,
    ) -> HistoryPage {
        if project_path.is_none() && provider_ids == self.history.provider_ids() {
            return self.list_history(offset, limit);
        }
        let page = self
            .history
            .list_page_filtered(provider_ids, project_path, offset, limit);
        HistoryPage {
            entries: page.entries,
            total: page.total,
        }
    }

    pub fn history_entry_at(&self, index: usize) -> Option<crate::history::HistoryEntry> {
        self.history.entry_at(index)
    }

    pub fn history_entry_at_filtered(
        &self,
        provider_ids: &[&str],
        project_path: Option<&Path>,
        index: usize,
    ) -> Option<crate::history::HistoryEntry> {
        if project_path.is_none() && provider_ids == self.history.provider_ids() {
            return self.history_entry_at(index);
        }
        self.history
            .entry_at_filtered(provider_ids, project_path, index)
    }

    pub fn history_entry_for_provider_session(
        &self,
        provider_id: &str,
        session_id: &str,
        project_path: Option<&Path>,
    ) -> Option<crate::history::HistoryEntry> {
        self.history
            .entry_for_provider_session(provider_id, session_id, project_path)
    }

    pub fn list_history_workspaces(
        &self,
        provider_ids: &[&str],
    ) -> Vec<crate::history::HistoryWorkspace> {
        self.history.list_workspaces(provider_ids)
    }

    pub fn list_history_workspaces_page(
        &self,
        provider_ids: &[&str],
        offset: usize,
        limit: usize,
    ) -> HistoryWorkspacePage {
        let page = self
            .history
            .list_workspaces_page(provider_ids, offset, limit);
        HistoryWorkspacePage {
            entries: page.entries,
            total: page.total,
        }
    }

    pub fn history_transcript_for_entry(
        &self,
        entry: &crate::history::HistoryEntry,
        limit: usize,
        cursor: Option<&crate::history::HistoryCursor>,
    ) -> Result<crate::history::HistoryTranscript, crate::history::HistoryTranscriptError> {
        crate::history::history_transcript_for_entry(entry, limit, cursor)
    }

    pub fn history_replay_record(&self, workspace_id: &WorkspaceId) -> Option<HistoryReplayRecord> {
        self.store
            .history_replay(workspace_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = workspace_id.as_str(),
                    error = %err,
                    "history replay store lookup failed"
                );
                None
            })
    }

    pub fn remember_history_replay_record(
        &self,
        record: HistoryReplayRecord,
    ) -> HistoryReplayRecord {
        self.state
            .write()
            .expect("control plane lock")
            .upsert_history_replay(record)
    }

    pub fn upsert_history_replay_record(
        &self,
        record: HistoryReplayRecord,
    ) -> Result<HistoryReplayRecord, CoreError> {
        let existing = self.store.history_replay(&record.workspace_id)?;
        let record = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.upsert_history_replay(existing);
            }
            state.upsert_history_replay(record)
        };
        self.store.upsert_entity_state(
            "history_replay",
            record.workspace_id.as_str(),
            Some(record.workspace_id.as_str()),
            &record,
        )?;
        Ok(record)
    }

    pub fn remove_history_replay_record(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, CoreError> {
        self.state
            .write()
            .expect("control plane lock")
            .remove_history_replay(workspace_id);
        Ok(self
            .store
            .delete_entity("history_replay", workspace_id.as_str())?)
    }

    pub fn register_history_older_callback(
        &self,
        workspace_id: WorkspaceId,
        provider_id: impl Into<SmolStr>,
        session_id: impl Into<SmolStr>,
        session_path: PathBuf,
        cursor: impl Into<SmolStr>,
    ) -> Result<HistoryOlderCallbackRecord, CoreError> {
        let (record, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            let record = state.register_history_older_callback(
                workspace_id,
                provider_id,
                session_id,
                session_path,
                cursor,
            );
            (
                record.clone(),
                state.persistence_entities_without_timeline(),
            )
        };
        self.persist_non_timeline_entities(entities)?;
        self.store.upsert_entity_state(
            "history_older_callback",
            record.token.as_str(),
            Some(record.workspace_id.as_str()),
            &record,
        )?;
        Ok(record)
    }

    pub fn resolve_history_older_callback_record(
        &self,
        token: &HistoryOlderCallbackToken,
    ) -> Option<HistoryOlderCallbackRecord> {
        self.store
            .history_older_callback(token)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    token = token.as_str(),
                    error = %err,
                    "history older callback store lookup failed"
                );
                None
            })
    }

    pub fn record_workspace(
        &self,
        req: OpenWorkspaceRequest,
    ) -> Result<WorkspaceSummary, CoreError> {
        self.record_workspace_with_id(None, req)
    }

    pub async fn open_workspace(
        &self,
        req: OpenWorkspaceRequest,
    ) -> Result<LiveWorkspace, CoreError> {
        self.open_workspace_with_events(req)
            .await
            .map(|opened| opened.workspace)
    }

    pub async fn open_workspace_with_events(
        &self,
        req: OpenWorkspaceRequest,
    ) -> Result<OpenedCoreSession, CoreError> {
        self.open_workspace_with_id_and_events(None, req).await
    }

    pub async fn open_workspace_binding_with_events(
        &self,
        workspace_id: WorkspaceId,
        req: OpenWorkspaceRequest,
    ) -> Result<OpenedCoreSession, CoreError> {
        self.open_workspace_with_id_and_events(Some(workspace_id), req)
            .await
    }

    pub async fn open_new_workspace_session_with_events(
        &self,
        workspace_id: WorkspaceId,
    ) -> Result<OpenedCoreSession, CoreError> {
        let workspace = self
            .workspace_binding(&workspace_id)
            .ok_or_else(|| CoreError::invalid_state("workspace not found"))?;
        let provider_id = self.provider_id_static(workspace.provider_id.as_str())?;
        let project_path = workspace.project_path.clone();
        let title = workspace.title.to_string();
        let live_instance_id = self
            .live_sessions
            .read()
            .expect("live session registry lock")
            .get(&workspace_id)
            .map(|session| live_instance_id(session.instance_id().0.as_str()));
        if let Some(live_instance_id) = live_instance_id {
            self.detach_live_session(&workspace_id, &live_instance_id, "new session requested")
                .await?;
        }
        self.open_workspace_binding_with_events(
            workspace_id,
            OpenWorkspaceRequest {
                provider_id,
                project_path: Some(project_path),
                title,
            },
        )
        .await
    }

    pub fn upsert_workspace_binding(
        &self,
        workspace_id: WorkspaceId,
        req: OpenWorkspaceRequest,
        native_resume_ref: Option<&str>,
    ) -> Result<WorkspaceSummary, CoreError> {
        let OpenWorkspaceRequest {
            provider_id,
            project_path,
            title,
        } = req;
        let project_path = project_path.unwrap_or_else(default_project_path);
        let existing_workspace = self.store.workspace(&workspace_id)?;
        let (summary, binding, provider_session) = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing_workspace) = existing_workspace {
                state.record_workspace_projection(existing_workspace);
            }
            let mut binding = state.upsert_workspace(WorkspaceBinding::new(
                workspace_id.clone(),
                title,
                provider_id,
                project_path,
            ));
            let mut provider_session = None;
            if let Some(native_resume_ref) = native_resume_ref.filter(|value| !value.is_empty()) {
                let provider_session_id = provider_session_id(provider_id, native_resume_ref);
                let existing_provider_session =
                    self.store.provider_session(&provider_session_id)?;
                provider_session = Some(state.upsert_provider_session(
                    existing_provider_session.unwrap_or_else(|| {
                        ProviderSessionRecord::new(
                            provider_session_id.clone(),
                            provider_id,
                            native_resume_ref,
                        )
                    }),
                ));
                binding =
                    state.activate_provider_session(workspace_id.clone(), provider_session_id)?;
            }
            let summary = self.summary_from_binding(binding.clone());
            (summary, binding, provider_session)
        };
        let mut entities = ControlPlaneState::persistence_entities_for_workspace(&binding);
        if let Some(provider_session) = provider_session {
            entities.extend(
                ControlPlaneState::persistence_entities_for_provider_session(&provider_session),
            );
        }
        self.upsert_persistence_entities(entities)?;
        let _ = self
            .events
            .send(CoreEvent::WorkspaceChanged { workspace_id });
        Ok(summary)
    }

    #[instrument(
        target = "lucarne::core_service",
        skip(self, req),
        fields(provider = req.provider_id, workspace_id = ?workspace_id)
    )]
    async fn open_workspace_with_id_and_events(
        &self,
        workspace_id: Option<WorkspaceId>,
        req: OpenWorkspaceRequest,
    ) -> Result<OpenedCoreSession, CoreError> {
        debug!(
            target: "lucarne::core_service",
            provider = req.provider_id,
            workspace_id = ?workspace_id,
            project_path = ?req.project_path,
            "opening provider workspace"
        );
        self.ensure_provider(req.provider_id)?;
        let settings = self.effective_settings(req.project_path.as_deref(), None);
        let open = OpenSession {
            cwd: req
                .project_path
                .as_ref()
                .and_then(|path| path.to_str())
                .map(|text| text.to_string().into()),
            args: session_args(&settings, false),
            idle_timeout_ms: Some(self.options.session_idle_timeout.as_millis() as u64),
            ..Default::default()
        };
        let session = self.runtime.open(req.provider_id, open).await?;
        let events_source = session.take_events().await?;
        let activity_source = session.take_activity_events().await?;
        let session = Arc::<dyn AgentSession>::from(session);
        let live_instance_id = live_instance_id(session.instance_id().0.as_str());
        let workspace = self.record_live_workspace(
            workspace_id,
            req,
            session.id().0.as_str(),
            &live_instance_id,
        )?;
        let live_generation =
            self.bind_live_session_runtime(workspace.workspace_id.clone(), Arc::clone(&session));
        let events = self.watch_workspace_events(workspace.workspace_id.clone());
        self.spawn_event_pump(
            workspace.workspace_id.clone(),
            live_generation,
            Arc::clone(&session),
            events_source,
            activity_source,
        );
        info!(
            target: "lucarne::core_service",
            provider = workspace.provider_id,
            workspace_id = %workspace.workspace_id.as_str(),
            session_id = %workspace.session_id.0.as_str(),
            "workspace opened"
        );
        Ok(OpenedCoreSession {
            workspace,
            session: Arc::new(AgentSessionHandle::new(session)),
            events,
        })
    }

    pub async fn resume_workspace(
        &self,
        req: ResumeWorkspaceRequest,
    ) -> Result<LiveWorkspace, CoreError> {
        self.resume_workspace_with_events(req)
            .await
            .map(|opened| opened.workspace)
    }

    #[instrument(
        target = "lucarne::core_service",
        skip(self, req),
        fields(workspace_id = %req.workspace_id.as_str())
    )]
    pub async fn resume_workspace_with_events(
        &self,
        req: ResumeWorkspaceRequest,
    ) -> Result<OpenedCoreSession, CoreError> {
        debug!(
            target: "lucarne::core_service",
            workspace_id = %req.workspace_id.as_str(),
            "resuming workspace"
        );
        if req.force_bypass_permissions {
            self.detach_current_live_session(&req.workspace_id, "force-bypass resume requested")
                .await?;
        } else if let Some(opened) = self.existing_live_workspace_session(&req.workspace_id)? {
            info!(
                target: "lucarne::core_service",
                provider = opened.workspace.provider_id,
                workspace_id = %opened.workspace.workspace_id.as_str(),
                session_id = %opened.workspace.session_id.0.as_str(),
                "workspace resume reused live session"
            );
            return Ok(opened);
        }
        let workspace = self
            .workspace_binding(&req.workspace_id)
            .ok_or_else(|| CoreError::invalid_state("workspace not found"))?;
        let provider_id = self.provider_id_static(workspace.provider_id.as_str())?;
        let provider_session_id = workspace
            .active_provider_session_id
            .clone()
            .ok_or_else(|| CoreError::invalid_state("workspace has no provider session"))?;
        let (resume_ref, settings) = {
            let provider_session = self
                .store
                .provider_session(&provider_session_id)?
                .ok_or_else(|| CoreError::invalid_state("provider session not found"))?;
            let state = self.state.read().expect("control plane lock");
            (
                provider_session.native_resume_ref.clone(),
                state.effective_settings(
                    Some(workspace.project_path.as_path()),
                    Some(&provider_session_id),
                ),
            )
        };
        let project_path = workspace.project_path.clone();
        let session = self
            .runtime
            .resume(
                provider_id,
                ResumeSession {
                    session_ref: SessionRef(resume_ref),
                    args: resume_session_args(
                        &settings,
                        req.force_bypass_permissions,
                        &project_path,
                    )?,
                    idle_timeout_ms: Some(self.options.session_idle_timeout.as_millis() as u64),
                    ..Default::default()
                },
            )
            .await?;
        let events_source = session.take_events().await?;
        let activity_source = session.take_activity_events().await?;
        let session = Arc::<dyn AgentSession>::from(session);
        let live_instance_id = live_instance_id(session.instance_id().0.as_str());
        let workspace = self.attach_live_session_record(
            req.workspace_id.clone(),
            provider_id,
            session.id().0.as_str(),
            &live_instance_id,
        )?;
        let live_generation =
            self.bind_live_session_runtime(req.workspace_id, Arc::clone(&session));
        let events = self.watch_workspace_events(workspace.workspace_id.clone());
        self.spawn_event_pump(
            workspace.workspace_id.clone(),
            live_generation,
            Arc::clone(&session),
            events_source,
            activity_source,
        );
        info!(
            target: "lucarne::core_service",
            provider = workspace.provider_id,
            workspace_id = %workspace.workspace_id.as_str(),
            session_id = %workspace.session_id.0.as_str(),
            "workspace resumed"
        );
        Ok(OpenedCoreSession {
            workspace,
            session: Arc::new(AgentSessionHandle::new(session)),
            events,
        })
    }

    fn existing_live_workspace_session(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<OpenedCoreSession>, CoreError> {
        let Some(session) = self
            .live_sessions
            .read()
            .expect("live session registry lock")
            .get(workspace_id)
            .cloned()
        else {
            return Ok(None);
        };

        let workspace = {
            let binding = self
                .workspace_binding(workspace_id)
                .ok_or_else(|| CoreError::invalid_state("workspace not found"))?;
            let provider_id = self.provider_id_static(binding.provider_id.as_str())?;
            let provider_session_id = binding
                .active_provider_session_id
                .as_ref()
                .ok_or_else(|| CoreError::invalid_state("workspace has no provider session"))?;
            let provider_session = self
                .provider_session_record(provider_session_id)
                .ok_or_else(|| CoreError::invalid_state("provider session not found"))?;
            LiveWorkspace {
                workspace_id: binding.workspace_id.clone(),
                provider_id,
                session_id: crate::agent_runtime::SessionId(
                    provider_session.native_resume_ref.clone(),
                ),
            }
        };
        let events = self.watch_workspace_events(workspace.workspace_id.clone());
        Ok(Some(OpenedCoreSession {
            workspace,
            session: Arc::new(AgentSessionHandle::new(session)),
            events,
        }))
    }

    async fn detach_current_live_session(
        &self,
        workspace_id: &WorkspaceId,
        close_reason: &'static str,
    ) -> Result<(), CoreError> {
        let live_instance_id = self
            .live_sessions
            .read()
            .expect("live session registry lock")
            .get(workspace_id)
            .map(|session| LiveInstanceId::new(session.instance_id().0.as_str()));
        let Some(live_instance_id) = live_instance_id else {
            return Ok(());
        };
        self.detach_live_session(workspace_id, &live_instance_id, close_reason)
            .await
    }

    #[instrument(
        target = "lucarne::core_service",
        skip(self, req),
        fields(workspace_id = %req.workspace_id.as_str(), input_bytes = req.input.text.len(), image_count = req.input.images.len())
    )]
    pub async fn submit_turn(&self, req: SubmitTurnRequest) -> Result<SubmittedTurn, CoreError> {
        let SubmitTurnRequest {
            workspace_id,
            source,
            input,
            reply_to_channel_message_id,
        } = req;
        let turn_permit = self.submitted_turn_scheduler.acquire(&workspace_id).await;
        let live = self.live_session(&workspace_id).await?;
        let provider_session_id = self.active_provider_session_id(&workspace_id)?;
        let live_instance_id = LiveInstanceId::new(live.instance_id().0.as_str());
        let input_text = input.text.clone();
        let turn = self.start_turn(
            workspace_id.clone(),
            provider_session_id,
            live_instance_id,
            source,
            input_text.clone(),
            reply_to_channel_message_id,
        )?;
        if source == TurnSource::UserMessage {
            self.append_timeline(TimelineItem::new(
                workspace_id.clone(),
                turn.turn_id.clone(),
                TimelineItemKind::User,
                serde_json::json!({ "text": input_text.as_str() }),
            ))?;
        }
        let turn_id = turn.turn_id;
        self.remember_submitted_turn(workspace_id.clone(), turn_id.clone(), turn_permit);
        debug!(
            target: "lucarne::core_service",
            workspace_id = %workspace_id.as_str(),
            turn_id = %turn_id.as_str(),
            input_bytes = input.text.len(),
            image_count = input.images.len(),
            "submitting turn"
        );
        self.spawn_submitted_turn_watchdog(workspace_id.clone(), turn_id.clone());
        if let Err(err) = live.submit(input).await {
            let error = err.to_string();
            if let Err(fail_err) = self.fail_turn(turn_id.clone(), &error) {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = %workspace_id.as_str(),
                    turn_id = %turn_id.as_str(),
                    submit_error = %error,
                    fail_error = %fail_err,
                    "failed to mark turn failed after live submit error"
                );
                self.remove_submitted_turn(&workspace_id, &turn_id);
            }
            return Err(err.into());
        }
        let _ = self.events.send(CoreEvent::TurnStarted { workspace_id });
        Ok(SubmittedTurn { turn_id })
    }

    pub fn upsert_scheduled_task(
        &self,
        req: UpsertScheduledTaskRequest,
    ) -> Result<ScheduledTaskRecord, CoreError> {
        self.ensure_provider(req.provider_id)?;
        let mut record = ScheduledTaskRecord::new(
            req.task_id,
            req.workspace_id,
            req.provider_id,
            req.project_path,
            req.title,
            req.prompt,
            req.next_run_unix_ms,
        );
        record.enabled = req.enabled;
        let existing = self.store.scheduled_task(&record.task_id)?;
        let record = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.upsert_scheduled_task(existing);
            }
            state.upsert_scheduled_task(record)
        };
        self.store.upsert_entity_state(
            "scheduled_task",
            record.task_id.as_str(),
            Some(record.workspace_id.as_str()),
            &record,
        )?;
        Ok(record)
    }

    pub fn scheduled_task(&self, task_id: &ScheduledTaskId) -> Option<ScheduledTaskRecord> {
        self.store.scheduled_task(task_id).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                task_id = task_id.as_str(),
                error = %err,
                "scheduled task store lookup failed"
            );
            None
        })
    }

    pub async fn run_due_scheduled_tasks(
        &self,
        req: RunDueScheduledTasksRequest,
    ) -> Result<RunScheduledTasksReport, CoreError> {
        let due_tasks = self.store.due_scheduled_tasks(req.now_unix_ms)?;
        let mut report = RunScheduledTasksReport::default();
        for task in due_tasks {
            let result = self.run_scheduled_task(task.clone(), req.now_unix_ms).await;
            match result {
                Ok(run) => report.triggered.push(run),
                Err(error) => report.failed.push(ScheduledTaskRunError {
                    task_id: task.task_id,
                    workspace_id: task.workspace_id,
                    error: error.to_string(),
                }),
            }
        }
        Ok(report)
    }

    async fn run_scheduled_task(
        &self,
        task: ScheduledTaskRecord,
        now_unix_ms: u64,
    ) -> Result<ScheduledTaskRun, CoreError> {
        let provider_id = self.provider_id_static(task.provider_id.as_str())?;
        let has_resumable_session = self
            .workspace_binding(&task.workspace_id)
            .and_then(|workspace| workspace.active_provider_session_id)
            .is_some();
        if has_resumable_session {
            self.resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: task.workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await?;
        } else {
            self.open_workspace_binding_with_events(
                task.workspace_id.clone(),
                OpenWorkspaceRequest {
                    provider_id,
                    project_path: Some(task.project_path.clone()),
                    title: task.title.to_string(),
                },
            )
            .await?;
        }
        let submitted = self
            .submit_turn(SubmitTurnRequest {
                workspace_id: task.workspace_id.clone(),
                source: TurnSource::System,
                input: crate::agent_runtime::AgentInput {
                    text: task.prompt.to_string().into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await?;
        let updated_task = {
            let mut state = self.state.write().expect("control plane lock");
            state.upsert_scheduled_task(task.clone());
            state
                .mark_scheduled_task_triggered(&task.task_id, now_unix_ms)
                .ok_or_else(|| CoreError::invalid_state("scheduled task not found"))?
        };
        self.store.upsert_entity_state(
            "scheduled_task",
            updated_task.task_id.as_str(),
            Some(updated_task.workspace_id.as_str()),
            &updated_task,
        )?;
        Ok(ScheduledTaskRun {
            task_id: task.task_id,
            workspace_id: task.workspace_id,
            turn_id: submitted.turn_id,
        })
    }

    pub async fn interrupt_turn(&self, req: InterruptTurnRequest) -> Result<(), CoreError> {
        self.live_session(&req.workspace_id)
            .await?
            .interrupt()
            .await?;
        Ok(())
    }

    pub fn live_session_is_bound(
        &self,
        workspace_id: &WorkspaceId,
        live_instance_id: &LiveInstanceId,
    ) -> bool {
        self.live_sessions
            .read()
            .expect("live session registry lock")
            .get(workspace_id)
            .is_some_and(|session| session.instance_id().0.as_str() == live_instance_id.as_str())
    }

    pub async fn resolve_permission(&self, req: ResolvePermissionRequest) -> Result<(), CoreError> {
        let ResolvePermissionRequest {
            workspace_id,
            token,
            response,
        } = req;
        let callback = self
            .store
            .intervention_callback(&token)?
            .ok_or_else(|| CoreError::invalid_state("intervention callback not found"))?;
        if callback.workspace_id != workspace_id {
            return Err(CoreError::invalid_state(
                "intervention callback workspace mismatch",
            ));
        }
        self.live_session(&workspace_id)
            .await?
            .resolve(&callback.req_id, response)
            .await?;
        mark_current_submitted_turn_intervention_resolved(
            &self.submitted_turns,
            &self.submitted_turn_activity,
            &workspace_id,
        );
        Ok(())
    }

    pub fn record_fork_workspace_projection(
        &self,
        source_workspace_id: WorkspaceId,
        fork_workspace_id: WorkspaceId,
        title: String,
        provider_id: &'static str,
        native_resume_ref: Option<&str>,
        live_instance_id: Option<LiveInstanceId>,
    ) -> Result<(), CoreError> {
        let provider_session_id =
            native_resume_ref.map(|resume_ref| provider_session_id(provider_id, resume_ref));
        let existing_provider_session = provider_session_id
            .as_ref()
            .map(|provider_session_id| self.store.provider_session(provider_session_id))
            .transpose()?
            .flatten();
        let Some(source_workspace) = self.store.workspace(&source_workspace_id)? else {
            return Err(ControlPlaneError::MissingWorkspace(source_workspace_id).into());
        };
        let result = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(source_workspace);
            if let Some(existing_provider_session) = existing_provider_session {
                state.upsert_provider_session(existing_provider_session);
            }
            state.fork_workspace_session(ForkWorkspaceSession {
                source_workspace_id: source_workspace_id.clone(),
                fork_workspace_id: fork_workspace_id.clone(),
                title: title.into(),
                provider_session_id,
                native_resume_ref: native_resume_ref.map(Into::into),
                live_instance_id,
                pid_or_handle: None,
            })?
        };
        let mut entities =
            ControlPlaneState::persistence_entities_for_workspace(&result.fork_workspace);
        if let Some(provider_session) = result.provider_session.as_ref() {
            entities.extend(
                ControlPlaneState::persistence_entities_for_provider_session(provider_session),
            );
        }
        if let Some(live_instance) = result.live_instance.as_ref() {
            entities.extend(ControlPlaneState::persistence_entities_for_live_instance(
                live_instance,
                Some(&result.fork_workspace.workspace_id),
            ));
        }
        self.upsert_persistence_entities(entities)?;
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: source_workspace_id,
        });
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: fork_workspace_id,
        });
        Ok(())
    }

    fn handle_history_watch_updates(&self, updates: Box<[WatchUpdate]>) {
        for update in updates {
            if let Err(err) = self.handle_history_watch_update(update) {
                warn!(
                    target: "lucarne::core_service",
                    error = %err,
                    "history watch update ignored"
                );
            }
        }
    }

    fn handle_history_watch_update(&self, update: WatchUpdate) -> Result<(), CoreError> {
        trace!(
            target: "lucarne::core_service",
            provider = update.provider.as_str(),
            path = %update.path.display(),
            session_id = update.session_id.as_deref(),
            cwd = update.cwd.as_deref(),
            change = ?update.change,
            events = update.events.len(),
            "received history watch update"
        );
        if !matches!(update.change, WatchChange::Created | WatchChange::Updated) {
            trace!(
                target: "lucarne::core_service",
                provider = update.provider.as_str(),
                path = %update.path.display(),
                change = ?update.change,
                "history watch update ignored because change is not projectable"
            );
            return Ok(());
        }
        if update.events.is_empty() {
            trace!(
                target: "lucarne::core_service",
                provider = update.provider.as_str(),
                path = %update.path.display(),
                session_id = update.session_id.as_deref(),
                change = ?update.change,
                "history watch update ignored because it has no events"
            );
            return Ok(());
        }

        let entry = history_entry_from_watch_update(&update)?;
        let provider_session_id = provider_session_id(entry.provider_id, entry.session_id.as_str());
        let workspace_id = self.ensure_history_watch_workspace(&entry)?;

        self.record_observed_watch_update(&entry, &workspace_id, &update)?;
        trace!(
            target: "lucarne::core_service",
            provider = entry.provider_id,
            session_id = %entry.session_id,
            provider_session_id = %provider_session_id.as_str(),
            workspace_id = %workspace_id.as_str(),
            events = update.events.len(),
            "recorded observed history watch update"
        );

        if current_submitted_turn(&self.submitted_turns, &workspace_id).is_some() {
            debug!(
                target: "lucarne::core_service",
                provider = entry.provider_id,
                session_id = %entry.session_id,
                provider_session_id = %provider_session_id.as_str(),
                workspace_id = %workspace_id.as_str(),
                "history watch update skipped while live submitted turn is current"
            );
            return Ok(());
        }

        let workspace_events = self.workspace_event_sender(&workspace_id);
        for watched_event in &update.events {
            if self.history_watch_event_claimed_by_live_runtime(&provider_session_id, watched_event)
            {
                debug!(
                    target: "lucarne::core_service",
                    provider = entry.provider_id,
                    session_id = %entry.session_id,
                    provider_session_id = %provider_session_id.as_str(),
                    workspace_id = %workspace_id.as_str(),
                    "history watch terminal event skipped because live runtime already emitted it"
                );
                continue;
            }
            let event = match watched_event {
                WatchEvent::UserMessage(message) => {
                    let Some(text) = message.text.as_deref() else {
                        trace!(
                            target: "lucarne::core_service",
                            provider = entry.provider_id,
                            session_id = %entry.session_id,
                            workspace_id = %workspace_id.as_str(),
                            "history user message watch event skipped because text is empty"
                        );
                        continue;
                    };
                    AgentEvent::Message(crate::agent_runtime::MessageEvent {
                        role: crate::agent_runtime::MessageRole::User,
                        text: text.into(),
                        streaming: false,
                    })
                }
                WatchEvent::AssistantMessage(message) => {
                    if message.phase.is_some() {
                        trace!(
                            target: "lucarne::core_service",
                            provider = entry.provider_id,
                            session_id = %entry.session_id,
                            workspace_id = %workspace_id.as_str(),
                            phase = message.phase.as_deref(),
                            "history assistant message watch event skipped because phase belongs to transcript"
                        );
                        continue;
                    }
                    let Some(text) = message.text.as_deref() else {
                        trace!(
                            target: "lucarne::core_service",
                            provider = entry.provider_id,
                            session_id = %entry.session_id,
                            workspace_id = %workspace_id.as_str(),
                            "history assistant message watch event skipped because text is empty"
                        );
                        continue;
                    };
                    AgentEvent::Message(crate::agent_runtime::MessageEvent {
                        role: crate::agent_runtime::MessageRole::Assistant,
                        text: text.into(),
                        streaming: false,
                    })
                }
                WatchEvent::Attachment(attachment) => {
                    let id = attachment
                        .id
                        .clone()
                        .or_else(|| attachment.meta.id.clone())
                        .unwrap_or_else(|| "attachment".into());
                    AgentEvent::Attachment(crate::agent_runtime::Attachment {
                        id,
                        filename: attachment.filename.clone(),
                        media_type: attachment.media_type.clone(),
                        data_base64: attachment.data_base64.to_string(),
                        caption: attachment.caption.clone(),
                    })
                }
                WatchEvent::ToolCall(tool)
                    if matches!(
                        tool.phase,
                        WatchedOperationPhase::Requested | WatchedOperationPhase::Started
                    ) =>
                {
                    let call_id = tool
                        .call_id
                        .as_deref()
                        .map(|id| crate::agent_runtime::CallId(id.into()))
                        .unwrap_or_else(|| crate::agent_runtime::CallId("unknown".into()));
                    let input: serde_json::Value = tool
                        .input_json
                        .as_deref()
                        .and_then(|j| serde_json::from_str(j).ok())
                        .unwrap_or(serde_json::Value::Null);
                    AgentEvent::ToolCall(crate::agent_runtime::ToolCallEvent {
                        call_id,
                        name: tool.name.as_str().into(),
                        input,
                    })
                }
                WatchEvent::ToolResult(tool)
                    if matches!(
                        tool.phase,
                        WatchedOperationPhase::Completed | WatchedOperationPhase::Failed
                    ) =>
                {
                    let call_id = tool
                        .call_id
                        .as_deref()
                        .map(|id| crate::agent_runtime::CallId(id.into()))
                        .unwrap_or_else(|| crate::agent_runtime::CallId("unknown".into()));
                    let output: serde_json::Value = tool
                        .output_json
                        .as_deref()
                        .and_then(|j| serde_json::from_str(j).ok())
                        .unwrap_or(serde_json::Value::Null);
                    AgentEvent::ToolResult(crate::agent_runtime::ToolResultEvent {
                        call_id,
                        output,
                        is_error: Some(tool.is_error),
                    })
                }
                WatchEvent::Usage(usage) => AgentEvent::Usage(crate::agent_runtime::UsageEvent {
                    input_tokens: if usage.input_tokens > 0 {
                        Some(usage.input_tokens)
                    } else {
                        None
                    },
                    output_tokens: if usage.output_tokens > 0 {
                        Some(usage.output_tokens)
                    } else {
                        None
                    },
                    total_tokens: if usage.total_tokens > 0 {
                        Some(usage.total_tokens)
                    } else {
                        None
                    },
                    raw: serde_json::Value::Null,
                }),
                WatchEvent::TurnCompleted(completion) => {
                    let Some(text) = watch_turn_completed_text(completion, true) else {
                        trace!(
                            target: "lucarne::core_service",
                            provider = entry.provider_id,
                            session_id = %entry.session_id,
                            workspace_id = %workspace_id.as_str(),
                            "history turn completion watch event skipped because it has no notifyable text"
                        );
                        continue;
                    };
                    AgentEvent::Message(crate::agent_runtime::MessageEvent {
                        role: crate::agent_runtime::MessageRole::Assistant,
                        text: text.into(),
                        streaming: false,
                    })
                }
                _ => continue,
            };
            let _ = self.events.send(CoreEvent::TimelineEvent {
                workspace_id: workspace_id.clone(),
                turn_id: None,
                event: event.clone(),
            });
            let _ = workspace_events.send(event);
            trace!(
                target: "lucarne::core_service",
                provider = entry.provider_id,
                session_id = %entry.session_id,
                workspace_id = %workspace_id.as_str(),
                "history watch event projected to core timeline"
            );
        }
        Ok(())
    }

    fn history_watch_event_claimed_by_live_runtime(
        &self,
        provider_session_id: &ProviderSessionId,
        watched_event: &WatchEvent,
    ) -> bool {
        if !matches!(
            watched_event,
            WatchEvent::TurnCompleted(_) | WatchEvent::TurnFailed(_)
        ) {
            return false;
        }
        if let Some(provider_turn_id) = watched_event
            .meta()
            .turn_id
            .as_deref()
            .map(str::trim)
            .filter(|turn_id| !turn_id.is_empty())
        {
            let now = Instant::now();
            let mut claims = self
                .live_runtime_turn_claims
                .write()
                .expect("live runtime turn claim lock");
            prune_live_runtime_turn_claims(&mut claims, now);
            return claims.contains_key(&LiveRuntimeTurnClaimKey {
                provider_session_id: provider_session_id.clone(),
                provider_turn_id: provider_turn_id.into(),
            });
        }
        let Some(text) = watched_event_terminal_text(watched_event) else {
            return false;
        };
        let now = Instant::now();
        let mut claims = self
            .live_runtime_terminal_text_claims
            .write()
            .expect("live runtime terminal text claim lock");
        prune_live_runtime_terminal_text_claims(&mut claims, now);
        claims
            .remove(&LiveRuntimeTerminalTextClaimKey {
                provider_session_id: provider_session_id.clone(),
                text,
            })
            .is_some()
    }

    fn record_observed_watch_update(
        &self,
        entry: &crate::history::HistoryEntry,
        workspace_id: &WorkspaceId,
        update: &WatchUpdate,
    ) -> Result<(), CoreError> {
        let resume_ref = entry.session_id.trim();
        if resume_ref.is_empty() {
            return Err(CoreError::HistoryWatch(format!(
                "watched {} session has no provider session id: {}",
                entry.provider_id,
                entry.session_path.display()
            )));
        }

        let provider_session_id = provider_session_id(entry.provider_id, resume_ref);
        let prompt_title = observed_prompt_title(&update.events);
        let metadata_title = update.title.as_deref().and_then(compact_observed_title);
        let (last_active_unix, last_active_display) = observed_activity_time(&update.events);
        let fallback_title = workspace_title_from_history_entry(entry);
        let cwd = entry.cwd.as_deref().map(PathBuf::from);

        let mut observed = self
            .observed_sessions
            .write()
            .expect("observed session registry lock");
        let title = prompt_title
            .or(metadata_title)
            .or_else(|| {
                observed
                    .get(&provider_session_id)
                    .map(|existing| existing.title.to_string())
            })
            .unwrap_or(fallback_title);
        let observed_pid = observed
            .get(&provider_session_id)
            .and_then(|existing| {
                existing
                    .observed_pid
                    .filter(|pid| process_id_is_alive(*pid))
            })
            .or_else(|| observed_session_writer_pid(&entry.session_path));

        observed.insert(
            provider_session_id.clone(),
            ObservedAgentSession {
                workspace_id: workspace_id.clone(),
                provider_id: entry.provider_id,
                provider_session_id,
                native_resume_ref: resume_ref.into(),
                title: title.into(),
                cwd,
                session_path: entry.session_path.clone(),
                last_active_unix,
                last_active_display,
                observed_pid,
            },
        );
        Ok(())
    }

    fn ensure_history_watch_workspace(
        &self,
        entry: &crate::history::HistoryEntry,
    ) -> Result<WorkspaceId, CoreError> {
        let provider_id = self.provider_id_static(entry.provider_id)?;
        let resume_ref = entry.session_id.trim();
        if resume_ref.is_empty() {
            return Err(CoreError::HistoryWatch(format!(
                "watched {provider_id} session has no provider session id: {}",
                entry.session_path.display()
            )));
        }
        let provider_session_id = provider_session_id(provider_id, resume_ref);
        if let Some(workspace) = self.workspace_for_provider_session(&provider_session_id) {
            return Ok(workspace.workspace_id);
        }
        let cwd = entry
            .cwd
            .as_deref()
            .map(str::trim)
            .filter(|cwd| !cwd.is_empty())
            .ok_or_else(|| {
                CoreError::HistoryWatch(format!(
                    "watched {provider_id} session has no cwd: {}",
                    entry.session_path.display()
                ))
            })?;
        let workspace_id = workspace_id_for_resume(provider_id, resume_ref);
        self.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id,
                project_path: Some(PathBuf::from(cwd)),
                title: workspace_title_from_history_entry(entry),
            },
            Some(resume_ref),
        )?;
        Ok(workspace_id)
    }

    fn bind_live_session_runtime(
        &self,
        workspace_id: WorkspaceId,
        session: Arc<dyn AgentSession>,
    ) -> u64 {
        let generation = self.next_live_generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.live_sessions
            .write()
            .expect("live session registry lock")
            .insert(workspace_id.clone(), session);
        self.live_session_generations
            .write()
            .expect("live session generation lock")
            .insert(workspace_id, generation);
        generation
    }

    fn spawn_event_pump(
        &self,
        workspace_id: WorkspaceId,
        live_generation: u64,
        session: Arc<dyn AgentSession>,
        mut source_events: AgentEventStream,
        mut activity_source: Option<AgentActivityStream>,
    ) {
        let events = self.events.clone();
        let workspace_events = self.workspace_event_sender(&workspace_id);
        let submitted_turns = Arc::clone(&self.submitted_turns);
        let submitted_turn_activity = Arc::clone(&self.submitted_turn_activity);
        let submitted_turn_permits = Arc::clone(&self.submitted_turn_permits);
        let live_sessions = Arc::clone(&self.live_sessions);
        let live_session_generations = Arc::clone(&self.live_session_generations);
        let live_runtime_turn_claims = Arc::clone(&self.live_runtime_turn_claims);
        let live_runtime_terminal_text_claims = Arc::clone(&self.live_runtime_terminal_text_claims);
        let state = Arc::clone(&self.state);
        let store = self.store.clone();
        let provider_id = session.provider_id();
        tokio::spawn(async move {
            let mut last_assistant_message: Option<SmolStr> = None;
            debug!(
                target: "lucarne::core_service",
                workspace_id = %workspace_id.as_str(),
                live_generation,
                "spawn_event_pump started"
            );
            loop {
                tokio::select! {
                    biased;
                    activity = recv_agent_activity(&mut activity_source) => {
                        if activity.is_none() {
                            activity_source = None;
                            continue;
                        }
                        if !live_generation_matches(
                            &live_session_generations,
                            &workspace_id,
                            live_generation,
                        ) {
                            debug!(
                                target: "lucarne::core_service",
                                workspace_id = %workspace_id.as_str(),
                                live_generation,
                                "provider activity pump stopped after live session was replaced"
                            );
                            return;
                        }
                        if let Some(turn_id) = current_submitted_turn(&submitted_turns, &workspace_id) {
                            touch_submitted_turn_agent_activity(&submitted_turn_activity, &turn_id);
                        }
                        trace!(
                            target: "lucarne::core_service",
                            workspace_id = %workspace_id.as_str(),
                            "provider activity pumped"
                        );
                    }
                    maybe_event = source_events.recv() => {
                        let Some(event) = maybe_event else {
                            break;
                        };
                        if !live_generation_matches(
                            &live_session_generations,
                            &workspace_id,
                            live_generation,
                        ) {
                            debug!(
                                target: "lucarne::core_service",
                                workspace_id = %workspace_id.as_str(),
                                live_generation,
                                "provider event pump stopped after live session was replaced"
                            );
                            return;
                        }
                        let turn_id = current_submitted_turn(&submitted_turns, &workspace_id);
                        if let Some(turn_id) = turn_id.as_ref() {
                            touch_submitted_turn(&submitted_turn_activity, turn_id, &event);
                        }
                        if let AgentEvent::Message(message) = &event {
                            if message.role == crate::agent_runtime::MessageRole::Assistant
                                && !message.streaming
                            {
                                if let Some(text) = terminal_text_claim_text(message.text.as_ref()) {
                                    last_assistant_message = Some(text);
                                }
                            }
                        }
                        let lifecycle = match &event {
                            AgentEvent::TurnCompleted(completed) => {
                                if let Some(turn_id) = turn_id.as_ref() {
                                    let native_session = session
                                        .provider_session_id()
                                        .await
                                        .unwrap_or_else(|| session.id().clone());
                                    let native_provider_session_id = provider_session_id(
                                        provider_id.as_str(),
                                        native_session.0.as_str(),
                                    );
                                    remember_live_runtime_turn_claim(
                                        &live_runtime_turn_claims,
                                        native_provider_session_id.clone(),
                                        completed.turn_id.clone(),
                                    );
                                    if completed.turn_id.trim().is_empty() {
                                        if let Some(text) = last_assistant_message.take() {
                                            remember_live_runtime_terminal_text_claim(
                                                &live_runtime_terminal_text_claims,
                                                native_provider_session_id,
                                                text,
                                            );
                                        }
                                    }
                                    let usage = completed
                                        .usage
                                        .clone()
                                        .map(serde_json::to_value)
                                        .transpose()
                                        .map_err(|err| {
                                            CoreError::invalid_state(format!(
                                                "failed to serialize turn completion usage: {err}"
                                            ))
                                        });
                                    match usage.and_then(|usage| {
                                        complete_turn_record_with_usage_value(
                                            &store,
                                            &state,
                                            turn_id.clone(),
                                            usage,
                                        )
                                    }) {
                                        Ok(_) => {}
                                        Err(err) => warn!(
                                            target: "lucarne::core_service",
                                            workspace_id = %workspace_id.as_str(),
                                            turn_id = %turn_id.as_str(),
                                            error = %err,
                                            "failed to complete submitted turn record"
                                        ),
                                    }
                                }
                                last_assistant_message = None;
                                Some(CoreEvent::TurnCompleted {
                                    workspace_id: workspace_id.clone(),
                                })
                            }
                            AgentEvent::TurnFailed(failed) => {
                                if let Some(turn_id) = turn_id.as_ref() {
                                    let native_session = session
                                        .provider_session_id()
                                        .await
                                        .unwrap_or_else(|| session.id().clone());
                                    remember_live_runtime_turn_claim(
                                        &live_runtime_turn_claims,
                                        provider_session_id(
                                            provider_id.as_str(),
                                            native_session.0.as_str(),
                                        ),
                                        failed.turn_id.clone(),
                                    );
                                    if let Err(err) = fail_turn_record(
                                        &store,
                                        &state,
                                        turn_id.clone(),
                                        failed.error.as_str(),
                                    ) {
                                        warn!(
                                            target: "lucarne::core_service",
                                            workspace_id = %workspace_id.as_str(),
                                            turn_id = %turn_id.as_str(),
                                            error = %err,
                                            "failed to fail submitted turn record"
                                        );
                                    }
                                }
                                last_assistant_message = None;
                                Some(CoreEvent::TurnFailed {
                                    workspace_id: workspace_id.clone(),
                                    error: failed.error.to_string(),
                                })
                            }
                            _ => None,
                        };
                        let workspace_event = event.clone();
                        let _ = events.send(CoreEvent::TimelineEvent {
                            workspace_id: workspace_id.clone(),
                            turn_id: turn_id.clone(),
                            event,
                        });
                        let _ = workspace_events.send(workspace_event);
                        trace!(
                            target: "lucarne::core_service",
                            workspace_id = %workspace_id.as_str(),
                            "provider event pumped"
                        );
                        if let Some(lifecycle) = lifecycle {
                            let _ = events.send(lifecycle);
                            if turn_id.is_some() {
                                pop_submitted_turn(
                                    &submitted_turns,
                                    &submitted_turn_activity,
                                    &submitted_turn_permits,
                                    &workspace_id,
                                );
                            }
                        }
                    }
                }
            }
            if let Some(turn_id) = current_submitted_turn(&submitted_turns, &workspace_id) {
                let error = "agent event stream closed before turn completed".to_string();
                if pop_submitted_turn_if_current(
                    &submitted_turns,
                    &submitted_turn_activity,
                    &submitted_turn_permits,
                    &workspace_id,
                    &turn_id,
                ) {
                    if let Err(err) = fail_turn_record(&store, &state, turn_id.clone(), &error) {
                        warn!(
                            target: "lucarne::core_service",
                            workspace_id = %workspace_id.as_str(),
                            turn_id = %turn_id.as_str(),
                            error = %err,
                            "failed to fail submitted turn record after event stream closed"
                        );
                    }
                    emit_submitted_turn_failure(
                        &events,
                        &workspace_events,
                        &workspace_id,
                        &turn_id,
                        error,
                    );
                }
            }
            {
                let mut generations = live_session_generations
                    .write()
                    .expect("live session generation lock");
                if generations
                    .get(&workspace_id)
                    .copied()
                    .is_some_and(|generation| generation == live_generation)
                {
                    generations.remove(&workspace_id);
                    let mut sessions = live_sessions.write().expect("live session registry lock");
                    sessions.remove(&workspace_id);
                }
            }
            debug!(
                target: "lucarne::core_service",
                workspace_id = %workspace_id.as_str(),
                live_generation,
                "spawn_event_pump stopped"
            );
        });
    }

    pub async fn resolve_live_request(
        &self,
        workspace_id: &WorkspaceId,
        req_id: &str,
        response: InterventionResponse,
    ) -> Result<(), CoreError> {
        self.live_session(workspace_id)
            .await?
            .resolve(req_id, response)
            .await?;
        mark_current_submitted_turn_intervention_resolved(
            &self.submitted_turns,
            &self.submitted_turn_activity,
            workspace_id,
        );
        Ok(())
    }
    pub async fn invoke_command(
        &self,
        req: InvokeCommandRequest,
    ) -> Result<AgentCommandResult, CoreError> {
        Ok(self
            .live_session(&req.workspace_id)
            .await?
            .invoke_command(req.command)
            .await?)
    }

    pub async fn close_workspace(&self, req: CloseWorkspaceRequest) -> Result<(), CoreError> {
        let session = self.remove_runtime_workspace(&req.workspace_id);
        if let Some(session) = session {
            session.close().await?;
        }
        self.clear_workspace_activation(&req.workspace_id)?;
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: req.workspace_id,
        });
        Ok(())
    }

    pub async fn detach_live_session(
        &self,
        workspace_id: &WorkspaceId,
        live_instance_id: &LiveInstanceId,
        close_reason: impl Into<SmolStr>,
    ) -> Result<(), CoreError> {
        let close_reason = close_reason.into();
        let session = {
            let mut sessions = self
                .live_sessions
                .write()
                .expect("live session registry lock");
            let remove_current = sessions.get(workspace_id).is_some_and(|session| {
                session.instance_id().0.as_str() == live_instance_id.as_str()
            });
            if remove_current {
                sessions.remove(workspace_id)
            } else {
                None
            }
        };
        if session.is_some() {
            self.live_session_generations
                .write()
                .expect("live session generation lock")
                .remove(workspace_id);
        }
        let mut entities = Vec::new();
        if let Some(mut live) = self.store.live_instance(live_instance_id)? {
            live.active_turn_id = None;
            live.state = LiveInstanceState::Closed;
            live.close_reason = Some(close_reason.clone());
            live.last_seen_at = SystemTime::now();
            entities.extend(ControlPlaneState::persistence_entities_for_live_instance(
                &live,
                self.store
                    .workspace_id_for_live_instance(live_instance_id)?
                    .as_ref(),
            ));
            self.store
                .delete_intervention_callbacks_for_live_instances(std::slice::from_ref(
                    live_instance_id,
                ))?;
        }
        if let Some(mut workspace) = self.store.workspace(workspace_id)? {
            if workspace.active_live_instance_id.as_ref() == Some(live_instance_id) {
                workspace.active_live_instance_id = None;
                workspace.updated_at = SystemTime::now();
                workspace.revision = workspace.revision.next();
                entities.extend(ControlPlaneState::persistence_entities_for_workspace(
                    &workspace,
                ));
            }
        }
        if !entities.is_empty() {
            self.upsert_persistence_entities(entities)?;
        }
        if let Some(session) = session {
            if let Err(err) = session.close().await {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = %workspace_id.as_str(),
                    live_instance_id = %live_instance_id.as_str(),
                    error = %err,
                    "detached live session close failed"
                );
            }
        }
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: workspace_id.clone(),
        });
        Ok(())
    }

    pub fn workspace_binding(&self, workspace_id: &WorkspaceId) -> Option<WorkspaceBinding> {
        self.store.workspace(workspace_id).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                workspace_id = workspace_id.as_str(),
                error = %err,
                "workspace binding store lookup failed"
            );
            None
        })
    }

    pub fn has_live_session(&self, workspace_id: &WorkspaceId) -> bool {
        self.live_sessions
            .read()
            .expect("live session registry lock")
            .contains_key(workspace_id)
    }

    pub fn workspace_bindings(&self) -> Vec<WorkspaceBinding> {
        self.store.workspace_bindings().unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                error = %err,
                "workspace bindings store lookup failed"
            );
            Vec::new()
        })
    }

    pub fn provider_session_record(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Option<ProviderSessionRecord> {
        self.store
            .provider_session(provider_session_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    provider_session_id = provider_session_id.as_str(),
                    error = %err,
                    "provider session store lookup failed"
                );
                None
            })
    }

    pub fn bind_message_to_provider_session(
        &self,
        channel: &str,
        chat_id: &str,
        message_id: &str,
        provider_session_id: ProviderSessionId,
    ) -> Result<MessageSessionBinding, CoreError> {
        let provider_exists = self.store.provider_session(&provider_session_id)?.is_some();
        if !provider_exists {
            return Err(ControlPlaneError::MissingProviderSession(provider_session_id).into());
        }

        let existing = self
            .store
            .message_session_binding(channel, chat_id, message_id)?;
        let mut binding =
            MessageSessionBinding::new(channel, chat_id, message_id, provider_session_id);
        if let Some(existing) = existing {
            binding.created_at = existing.created_at;
        }
        self.upsert_persistence_entities(
            ControlPlaneState::persistence_entities_for_message_session_binding(&binding),
        )?;
        Ok(binding)
    }

    pub fn message_session_binding(
        &self,
        channel: &str,
        chat_id: &str,
        message_id: &str,
    ) -> Option<MessageSessionBinding> {
        self.store
            .message_session_binding(channel, chat_id, message_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    channel,
                    chat_id,
                    message_id,
                    error = %err,
                    "message session binding store lookup failed"
                );
                None
            })
    }

    pub fn workspace_for_provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Option<WorkspaceBinding> {
        self.store
            .workspace_for_provider_session(provider_session_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    provider_session_id = provider_session_id.as_str(),
                    error = %err,
                    "workspace for provider session store lookup failed"
                );
                None
            })
    }

    pub fn live_instance_record(
        &self,
        live_instance_id: &LiveInstanceId,
    ) -> Option<LiveInstanceRecord> {
        self.store
            .live_instance(live_instance_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    live_instance_id = live_instance_id.as_str(),
                    error = %err,
                    "live instance store lookup failed"
                );
                None
            })
    }

    pub fn channel_bindings_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Vec<ChannelBinding> {
        self.store
            .channel_bindings_for_workspace(workspace_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = workspace_id.as_str(),
                    error = %err,
                    "channel bindings store lookup failed"
                );
                Vec::new()
            })
    }

    pub fn channel_binding(&self, binding_id: &ChannelBindingId) -> Option<ChannelBinding> {
        self.store
            .channel_binding(binding_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    binding_id = binding_id.as_str(),
                    error = %err,
                    "channel binding store lookup failed"
                );
                None
            })
    }

    pub fn upsert_channel_binding(&self, binding: ChannelBinding) -> Result<(), CoreError> {
        let existing = self.store.channel_binding(&binding.channel_binding_id)?;
        let binding = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.upsert_channel_binding(existing);
            }
            state.upsert_channel_binding(binding)
        };
        self.upsert_persistence_entities(
            ControlPlaneState::persistence_entities_for_channel_binding(&binding),
        )
    }

    pub fn attach_live_session_projection(
        &self,
        workspace_id: WorkspaceId,
        provider_id: &'static str,
        native_resume_ref: &str,
        live_instance_id: &LiveInstanceId,
    ) -> Result<LiveWorkspace, CoreError> {
        self.attach_live_session_record(
            workspace_id,
            provider_id,
            native_resume_ref,
            live_instance_id,
        )
    }

    pub fn clear_workspace_activation(&self, workspace_id: &WorkspaceId) -> Result<(), CoreError> {
        let Some(mut workspace) = self.store.workspace(workspace_id)? else {
            return Ok(());
        };
        let live_instance_id = workspace.active_live_instance_id.clone();
        workspace.active_provider_session_id = None;
        workspace.active_live_instance_id = None;
        workspace.updated_at = SystemTime::now();
        workspace.revision = workspace.revision.next();
        let mut entities = ControlPlaneState::persistence_entities_for_workspace(&workspace);
        if let Some(live_instance_id) = live_instance_id {
            if let Some(mut live) = self.store.live_instance(&live_instance_id)? {
                live.active_turn_id = None;
                live.state = LiveInstanceState::Closed;
                live.close_reason = Some("resume ref cleared".into());
                live.last_seen_at = SystemTime::now();
                entities.extend(ControlPlaneState::persistence_entities_for_live_instance(
                    &live,
                    Some(&workspace.workspace_id),
                ));
            }
        }
        self.upsert_persistence_entities(entities)?;
        Ok(())
    }

    pub async fn remove_workspace_projection(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<(), CoreError> {
        let workspace = self.store.workspace(workspace_id)?;
        let provider_session_id = workspace
            .as_ref()
            .and_then(|workspace| workspace.active_provider_session_id.clone());
        let session = self.remove_runtime_workspace(workspace_id);
        if let Some(session) = session {
            session.close().await?;
        }
        let hot_entities = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(workspace) = workspace.clone() {
                state.record_workspace_projection(workspace);
            }
            state.remove_workspace(workspace_id);
            state.persistence_entities()
        };
        self.store
            .delete_workspace_entities(workspace_id.as_str())?;
        self.persist_non_timeline_entities(hot_entities)?;
        if let Some(provider_session_id) = provider_session_id {
            if !self.store.provider_session_referenced_by_other_workspace(
                &provider_session_id,
                workspace_id,
            )? {
                self.store
                    .delete_message_session_bindings_for_provider_session(&provider_session_id)?;
                self.store
                    .delete_entity("provider_session", provider_session_id.as_str())?;
            }
        }
        Ok(())
    }

    pub fn clear_workspace_records(&self) -> Result<usize, CoreError> {
        let workspace_ids = self
            .workspace_bindings()
            .into_iter()
            .map(|workspace| workspace.workspace_id)
            .collect::<Vec<_>>();
        let cleared = workspace_ids.len();
        self.live_sessions
            .write()
            .expect("live session registry lock")
            .clear();
        self.live_session_generations
            .write()
            .expect("live session generation lock")
            .clear();
        self.workspace_events
            .write()
            .expect("workspace event registry lock")
            .clear();
        self.notification_suppression
            .write()
            .expect("notification suppression lock")
            .clear();
        self.submitted_turns
            .write()
            .expect("submitted turn lock")
            .clear();
        self.submitted_turn_activity
            .write()
            .expect("submitted turn activity lock")
            .clear();
        let entities = {
            let mut state = self.state.write().expect("control plane lock");
            state.clear_workspace_records();
            state.persistence_entities()
        };
        for workspace_id in &workspace_ids {
            self.store
                .delete_workspace_entities(workspace_id.as_str())?;
        }
        self.store.delete_entities_by_kind("provider_session")?;
        self.store
            .delete_entities_by_kind("message_session_binding")?;
        self.persist_non_timeline_entities(entities)?;
        for workspace_id in workspace_ids {
            let _ = self
                .events
                .send(CoreEvent::WorkspaceChanged { workspace_id });
        }
        Ok(cleared)
    }

    pub fn rename_workspace_projection(
        &self,
        workspace_id: &WorkspaceId,
        title: impl Into<SmolStr>,
    ) -> Result<WorkspaceBinding, CoreError> {
        let Some(workspace) = self.store.workspace(workspace_id)? else {
            return Err(ControlPlaneError::MissingWorkspace(workspace_id.clone()).into());
        };
        let renamed = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.rename_workspace(workspace_id, title)?
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_workspace(
            &renamed,
        ))?;
        Ok(renamed)
    }

    pub fn remove_channel_binding(&self, binding_id: &ChannelBindingId) -> Result<(), CoreError> {
        self.state
            .write()
            .expect("control plane lock")
            .remove_channel_binding(binding_id);
        self.store
            .delete_entity("channel_binding", binding_id.as_str())?;
        Ok(())
    }

    pub fn record_panel_stale_revision(
        &self,
        panel_id: PanelRenderId,
        channel: &'static str,
        chat_id: &str,
        observed: Revision,
        current: Revision,
    ) -> Result<(), CoreError> {
        let existing = self.store.panel_render(&panel_id)?;
        let panel = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.upsert_panel_render(existing);
            }
            state.record_panel_stale_revision(panel_id, channel, chat_id, observed, current)
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_panel_render(
            &panel,
        ))
    }

    pub fn upsert_panel_render(&self, panel: PanelRenderRecord) -> Result<(), CoreError> {
        let existing = self.store.panel_render(&panel.panel_id)?;
        let panel = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.upsert_panel_render(existing);
            }
            state.upsert_panel_render(panel)
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_panel_render(
            &panel,
        ))
    }

    pub fn panel_render(&self, panel_id: &PanelRenderId) -> Option<PanelRenderRecord> {
        self.store.panel_render(panel_id).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                panel_id = panel_id.as_str(),
                error = %err,
                "panel render store lookup failed"
            );
            None
        })
    }

    pub fn max_panel_render_revision(&self) -> Option<Revision> {
        self.store
            .max_panel_render_revision()
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    error = %err,
                    "max panel render revision store lookup failed"
                );
                None
            })
    }

    pub fn mark_panel_renders_stale_after_restart(&self) -> Result<(), CoreError> {
        let now = SystemTime::now();
        let mut entities = Vec::new();
        for mut panel in self.store.panel_renders()? {
            if panel.last_observed_stale_revision.is_none() {
                panel.last_observed_stale_revision = Some(panel.last_rendered_revision);
            }
            panel.last_reconcile_outcome = Some(ReconcileOutcome::StaleRevision);
            panel.updated_at = now;
            entities.extend(ControlPlaneState::persistence_entities_for_panel_render(
                &panel,
            ));
        }
        self.upsert_persistence_entities(entities)
    }

    pub fn mark_live_instances_stale_after_restart(
        &self,
        reason: impl Into<SmolStr>,
    ) -> Result<(), CoreError> {
        let reason = reason.into();
        let stale_lives = self.store.live_instances_for_restart_cleanup()?;

        let now = SystemTime::now();
        let mut entities = Vec::new();
        let mut affected_workspaces = Vec::new();
        let mut callback_cleanup_live_instance_ids = HashSet::<LiveInstanceId>::new();
        let mut cleanup_outcomes = HashMap::<LiveInstanceId, ReconcileOutcome>::new();
        let mut known_live_instance_ids = HashSet::<LiveInstanceId>::new();

        for (workspace_id, mut live) in stale_lives {
            let live_instance_id = live.live_instance_id.clone();
            known_live_instance_ids.insert(live_instance_id.clone());
            callback_cleanup_live_instance_ids.insert(live_instance_id.clone());
            let outcome = if live.state == LiveInstanceState::WaitingPermission {
                ReconcileOutcome::PermissionOrphaned
            } else {
                ReconcileOutcome::LiveInstanceStale
            };
            cleanup_outcomes.insert(live_instance_id.clone(), outcome);
            if matches!(
                live.state,
                LiveInstanceState::Closed | LiveInstanceState::Failed | LiveInstanceState::Stale
            ) {
                continue;
            }

            if let Some(active_turn_id) = live.active_turn_id.take() {
                if let Some(mut turn) = self.store.turn(&active_turn_id)? {
                    turn.state = TurnState::Orphaned;
                    turn.completed_at = Some(now);
                    entities.extend(ControlPlaneState::persistence_entities_for_turn_record(
                        &turn,
                    ));
                }
            }

            live.state = LiveInstanceState::Stale;
            live.close_reason = Some(reason.clone());
            live.last_seen_at = now;
            entities.extend(ControlPlaneState::persistence_entities_for_live_instance(
                &live,
                workspace_id.as_ref(),
            ));
        }

        for mut workspace in self.store.workspace_bindings()? {
            let Some(active_live_instance_id) = workspace.active_live_instance_id.clone() else {
                continue;
            };
            let outcome = cleanup_outcomes
                .get(&active_live_instance_id)
                .copied()
                .or_else(|| {
                    (!known_live_instance_ids.contains(&active_live_instance_id))
                        .then_some(ReconcileOutcome::LiveInstanceStale)
                });
            let Some(outcome) = outcome else {
                continue;
            };
            callback_cleanup_live_instance_ids.insert(active_live_instance_id);
            workspace.active_live_instance_id = None;
            workspace.updated_at = now;
            workspace.revision = workspace.revision.next();
            entities.extend(ControlPlaneState::persistence_entities_for_workspace(
                &workspace,
            ));
            entities.extend(
                ControlPlaneState::persistence_entities_for_reconcile_outcome(
                    &workspace.workspace_id,
                    outcome,
                ),
            );
            affected_workspaces.push(workspace.workspace_id);
        }

        let callback_cleanup_live_instance_ids = callback_cleanup_live_instance_ids
            .into_iter()
            .collect::<Vec<_>>();
        self.store
            .delete_intervention_callbacks_for_live_instances(
                &callback_cleanup_live_instance_ids,
            )?;
        self.upsert_persistence_entities(entities)?;
        for workspace_id in affected_workspaces {
            let _ = self
                .events
                .send(CoreEvent::WorkspaceChanged { workspace_id });
        }
        Ok(())
    }

    pub fn workspace_revision(&self, workspace_id: &WorkspaceId) -> Option<Revision> {
        self.workspace_binding(workspace_id)
            .map(|workspace| workspace.revision)
    }

    pub fn status_snapshot(&self, workspace_id: &WorkspaceId) -> Option<StatusSnapshot> {
        let workspace = self.workspace_binding(workspace_id)?;
        let provider_session = workspace
            .active_provider_session_id
            .as_ref()
            .and_then(|provider_session_id| self.provider_session_record(provider_session_id));
        let live_instance = workspace
            .active_live_instance_id
            .as_ref()
            .and_then(|live_instance_id| self.live_instance_record(live_instance_id));
        let channel_binding = self
            .channel_bindings_for_workspace(workspace_id)
            .into_iter()
            .next();
        let last_reconcile_outcome =
            self.store
                .reconcile_outcome(workspace_id)
                .unwrap_or_else(|err| {
                    warn!(
                        target: "lucarne::core_service",
                        workspace_id = workspace_id.as_str(),
                        error = %err,
                        "reconcile outcome store lookup failed"
                    );
                    None
                });
        Some(build_status_snapshot(
            Some(&workspace),
            provider_session.as_ref(),
            live_instance.as_ref(),
            channel_binding.as_ref(),
            last_reconcile_outcome,
        ))
    }

    pub async fn refresh_workspace_status_snapshot(
        &self,
        workspace_id: &WorkspaceId,
        force_bypass_permissions: bool,
    ) -> Result<Option<StatusSnapshot>, CoreError> {
        if !self.has_live_session(workspace_id) {
            self.resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: workspace_id.clone(),
                force_bypass_permissions,
            })
            .await?;
        }
        let session = self
            .live_sessions
            .read()
            .expect("live session registry lock")
            .get(workspace_id)
            .cloned();
        let Some(session) = session else {
            return Ok(self.status_snapshot(workspace_id));
        };
        let status = session.status().await?;
        self.record_provider_status(workspace_id, &status)
    }

    pub async fn agent_resource_snapshot(
        &self,
        scope: AgentResourceScope,
    ) -> Result<AgentResourceSnapshot, CoreError> {
        let mut observed_sessions = self.observed_recent_sessions_for_scope(&scope);
        let mut targets = self.agent_resource_targets(scope)?;
        for target in &mut targets {
            if let Some(observed) = observed_sessions
                .iter()
                .find(|session| session.provider_session_id == target.provider_session_id)
            {
                target.last_active_unix = observed.last_active_unix;
                target.last_active_display = observed.last_active_display.clone();
            }
        }
        let managed_session_ids = targets
            .iter()
            .map(|target| target.provider_session_id.clone())
            .collect::<HashSet<_>>();
        observed_sessions
            .retain(|session| !managed_session_ids.contains(&session.provider_session_id));
        if targets.iter().all(|target| target.pid.is_none()) {
            let mut snapshot = build_agent_resource_snapshot(targets, &[]);
            snapshot.observed_sessions = observed_sessions;
            return Ok(snapshot);
        }
        let samples = process_table::snapshot()
            .await
            .map_err(CoreError::ProcessSnapshot)?;
        let mut snapshot = build_agent_resource_snapshot(targets, &samples);
        snapshot.observed_sessions = observed_sessions;
        Ok(snapshot)
    }

    pub async fn kill_agent_processes(
        &self,
        req: KillAgentRequest,
    ) -> Result<KillAgentReport, CoreError> {
        let targets = self.agent_resource_targets(req.scope)?;
        let (to_kill, not_found) = match req.target {
            KillAgentTarget::All => (targets, None),
            KillAgentTarget::Identity(identity) => {
                let matched = targets
                    .into_iter()
                    .filter(|target| target.matches_identity(&identity))
                    .collect::<Vec<_>>();
                let not_found = matched.is_empty().then_some(identity);
                (matched, not_found)
            }
        };

        let mut killed = Vec::with_capacity(to_kill.len());
        for target in to_kill {
            self.detach_live_session(
                &target.workspace_id,
                &target.live_instance_id,
                "killed by operator command",
            )
            .await?;
            killed.push(target.killed_agent());
        }

        Ok(KillAgentReport { killed, not_found })
    }

    pub fn record_provider_status(
        &self,
        workspace_id: &WorkspaceId,
        status: &AgentStatus,
    ) -> Result<Option<StatusSnapshot>, CoreError> {
        let Some(provider_session_id) = self
            .workspace_binding(workspace_id)
            .and_then(|workspace| workspace.active_provider_session_id)
        else {
            return Ok(None);
        };
        let provider_session = self
            .store
            .provider_session(&provider_session_id)?
            .ok_or_else(|| {
                ControlPlaneError::MissingProviderSession(provider_session_id.clone())
            })?;
        let updated = {
            let mut state = self.state.write().expect("control plane lock");
            state.upsert_provider_session(provider_session);
            state.update_provider_status(&provider_session_id, status)?
        };
        self.upsert_persistence_entities(
            ControlPlaneState::persistence_entities_for_provider_session(&updated),
        )?;
        Ok(self.status_snapshot(workspace_id))
    }

    pub fn plan_activation(&self, request: ActivationRequest) -> Result<ActivationPlan, CoreError> {
        let workspace = self
            .workspace_binding(&request.workspace_id)
            .ok_or_else(|| ControlPlaneError::MissingWorkspace(request.workspace_id.clone()))?;
        let existing_binding = self
            .channel_bindings_for_workspace(&request.workspace_id)
            .into_iter()
            .find(|binding| {
                binding.channel == request.channel && binding.chat_id == request.chat_id
            });
        let provider_session = workspace
            .active_provider_session_id
            .as_ref()
            .and_then(|provider_session_id| self.provider_session_record(provider_session_id));
        let live_instance = workspace
            .active_live_instance_id
            .as_ref()
            .and_then(|live_instance_id| self.live_instance_record(live_instance_id));
        let turns = self.store.turns_for_workspace(&request.workspace_id)?;
        Ok(ControlPlaneState::plan_activation_from_records(
            request,
            workspace,
            existing_binding,
            provider_session,
            live_instance,
            turns,
        ))
    }

    pub fn record_reconcile_outcome(
        &self,
        workspace_id: WorkspaceId,
        outcome: ReconcileOutcome,
    ) -> Result<(), CoreError> {
        if self.store.workspace(&workspace_id)?.is_none() {
            return Err(ControlPlaneError::MissingWorkspace(workspace_id).into());
        }
        self.upsert_persistence_entities(
            ControlPlaneState::persistence_entities_for_reconcile_outcome(&workspace_id, outcome),
        )
    }

    /// Record reconcile outcomes for multiple workspaces in a single
    /// state mutation + DB write, avoiding per-workspace full-table
    /// persistence.
    pub fn record_reconcile_outcomes_batch(
        &self,
        outcomes: Vec<(WorkspaceId, ReconcileOutcome)>,
    ) -> Result<(), CoreError> {
        if outcomes.is_empty() {
            return Ok(());
        }
        let mut deduped = HashMap::<WorkspaceId, ReconcileOutcome>::new();
        for (workspace_id, outcome) in outcomes {
            deduped.insert(workspace_id, outcome);
        }
        for workspace_id in deduped.keys() {
            if self.store.workspace(workspace_id)?.is_none() {
                return Err(ControlPlaneError::MissingWorkspace(workspace_id.clone()).into());
            }
        }
        let mut entities = Vec::new();
        for (workspace_id, outcome) in deduped {
            entities.extend(
                ControlPlaneState::persistence_entities_for_reconcile_outcome(
                    &workspace_id,
                    outcome,
                ),
            );
        }
        self.upsert_persistence_entities(entities)
    }

    pub fn register_command_callback(
        &self,
        workspace_id: WorkspaceId,
        catalog_revision: Revision,
        command_name: impl Into<SmolStr>,
        args: Option<SmolStr>,
        values: serde_json::Value,
    ) -> Result<Option<CommandCallbackRecord>, CoreError> {
        let Some(workspace) = self.store.workspace(&workspace_id)? else {
            return Ok(None);
        };
        let (record, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            let record = state.register_command_callback(
                workspace_id,
                catalog_revision,
                command_name,
                args,
                values,
            );
            (record, state.persistence_entities_without_timeline())
        };
        self.persist_non_timeline_entities(entities)?;
        if let Some(record) = &record {
            self.store.upsert_entity_state(
                "command_callback",
                record.token.as_str(),
                Some(record.workspace_id.as_str()),
                record,
            )?;
        }
        Ok(record)
    }

    pub fn resolve_command_callback(
        &self,
        token: &CommandCallbackToken,
    ) -> Option<CommandCallbackRecord> {
        self.store.command_callback(token).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                token = token.as_str(),
                error = %err,
                "command callback store lookup failed"
            );
            None
        })
    }

    pub fn register_intervention_callback(
        &self,
        workspace_id: WorkspaceId,
        live_instance_id: LiveInstanceId,
        req_id: impl Into<SmolStr>,
        action: serde_json::Value,
    ) -> Result<Option<InterventionCallbackRecord>, CoreError> {
        let Some(workspace) = self.store.workspace(&workspace_id)? else {
            return Ok(None);
        };
        let live_workspace_id = self
            .store
            .workspace_id_for_live_instance(&live_instance_id)?;
        if live_workspace_id.as_ref() != Some(&workspace_id) {
            return Ok(None);
        }
        if workspace.active_live_instance_id.as_ref() != Some(&live_instance_id) {
            return Ok(None);
        }
        let Some(live) = self.store.live_instance(&live_instance_id)? else {
            return Ok(None);
        };
        let Some(provider_session) = self.store.provider_session(&live.provider_session_id)? else {
            return Ok(None);
        };
        let (record, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.upsert_provider_session(provider_session);
            state.record_live_instance_projection(live, Some(workspace_id.clone()));
            let record = state.register_intervention_callback(
                workspace_id,
                live_instance_id,
                req_id,
                action,
            );
            (record, state.persistence_entities_without_timeline())
        };
        self.persist_non_timeline_entities(entities)?;
        if let Some(record) = &record {
            self.store.upsert_entity_state(
                "intervention_callback",
                record.token.as_str(),
                Some(record.workspace_id.as_str()),
                record,
            )?;
        }
        Ok(record)
    }

    pub fn resolve_intervention_callback_record(
        &self,
        token: &InterventionCallbackToken,
    ) -> Option<InterventionCallbackRecord> {
        self.store
            .intervention_callback(token)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    token = token.as_str(),
                    error = %err,
                    "intervention callback store lookup failed"
                );
                None
            })
    }

    pub fn remove_intervention_callbacks_for_request(
        &self,
        live_instance_id: &LiveInstanceId,
        req_id: &str,
    ) -> Result<(), CoreError> {
        self.state
            .write()
            .expect("control plane lock")
            .remove_intervention_callbacks_for_request(live_instance_id, req_id);
        self.store
            .delete_intervention_callbacks_for_request(live_instance_id, req_id)?;
        Ok(())
    }

    pub fn command_workflows_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Vec<CommandWorkflow> {
        self.store
            .command_workflows_for_workspace(workspace_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = workspace_id.as_str(),
                    error = %err,
                    "command workflow store lookup failed"
                );
                Vec::new()
            })
    }

    pub fn command_workflow(&self, command_id: &CommandId) -> Option<CommandWorkflow> {
        self.store.command(command_id).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                command_id = command_id.as_str(),
                error = %err,
                "command workflow store lookup failed"
            );
            None
        })
    }

    pub fn timeline_kinds(&self, workspace_id: &WorkspaceId) -> Vec<TimelineItemKind> {
        let mut state = self.state.write().expect("control plane lock");
        let _ = state.ensure_timeline_loaded(workspace_id);
        state
            .timeline_for_workspace(workspace_id)
            .into_iter()
            .map(|item| item.kind)
            .collect()
    }

    pub fn subagent_links_for_turn(&self, turn_id: &TurnId) -> Vec<SubAgentLinkRecord> {
        self.store
            .subagent_links_for_turn(turn_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    turn_id = turn_id.as_str(),
                    error = %err,
                    "subagent links for turn store lookup failed"
                );
                Vec::new()
            })
    }

    pub fn subagent_links_for_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Vec<SubAgentLinkRecord> {
        self.store
            .subagent_links_for_workspace(workspace_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = workspace_id.as_str(),
                    error = %err,
                    "subagent links for workspace store lookup failed"
                );
                Vec::new()
            })
    }

    pub fn register_subagent_callback(
        &self,
        workspace_id: WorkspaceId,
        link_id: SubAgentLinkId,
    ) -> Result<Option<SubAgentCallbackRecord>, CoreError> {
        let Some(workspace) = self.store.workspace(&workspace_id)? else {
            return Ok(None);
        };
        let Some(link) = self.store.subagent_link(&link_id)? else {
            return Ok(None);
        };
        if link.workspace_id != workspace_id {
            return Ok(None);
        }
        let (record, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.record_subagent_link_projection(link);
            let record = state.register_subagent_callback(workspace_id, link_id);
            (record, state.persistence_entities_without_timeline())
        };
        self.persist_non_timeline_entities(entities)?;
        if let Some(record) = &record {
            self.store.upsert_entity_state(
                "subagent_callback",
                record.token.as_str(),
                Some(record.workspace_id.as_str()),
                record,
            )?;
        }
        Ok(record)
    }

    pub fn resolve_subagent_callback_record(
        &self,
        token: &SubAgentCallbackToken,
    ) -> Option<SubAgentCallbackRecord> {
        self.store.subagent_callback(token).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                token = token.as_str(),
                error = %err,
                "subagent callback store lookup failed"
            );
            None
        })
    }

    pub fn openable_subagent_link(&self, link_id: &SubAgentLinkId) -> Option<SubAgentLinkRecord> {
        self.store
            .subagent_link(link_id)
            .unwrap_or_else(|err| {
                warn!(
                    target: "lucarne::core_service",
                    link_id = link_id.as_str(),
                    error = %err,
                    "subagent link store lookup failed"
                );
                None
            })
            .filter(|link| link.openable)
    }

    pub fn attach_subagent_child_workspace(
        &self,
        link_id: &SubAgentLinkId,
        child_workspace_id: WorkspaceId,
    ) -> Result<Option<SubAgentLinkRecord>, CoreError> {
        let Some(link) = self.store.subagent_link(link_id)? else {
            return Ok(None);
        };
        let updated = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_subagent_link_projection(link);
            state.attach_subagent_child_workspace(link_id, child_workspace_id)
        };
        if let Some(updated) = &updated {
            self.store.upsert_entity_state(
                "subagent_link",
                updated.link_id.as_str(),
                Some(updated.workspace_id.as_str()),
                updated,
            )?;
        }
        Ok(updated)
    }

    pub fn start_turn(
        &self,
        workspace_id: WorkspaceId,
        provider_session_id: ProviderSessionId,
        live_instance_id: LiveInstanceId,
        source: TurnSource,
        input: impl Into<SmolStr>,
        reply_to_channel_message_id: Option<i64>,
    ) -> Result<TurnRecord, CoreError> {
        let Some(workspace) = self.store.workspace(&workspace_id)? else {
            return Err(ControlPlaneError::MissingWorkspace(workspace_id).into());
        };
        let Some(provider_session) = self.store.provider_session(&provider_session_id)? else {
            return Err(ControlPlaneError::MissingProviderSession(provider_session_id).into());
        };
        let live_workspace_id = self
            .store
            .workspace_id_for_live_instance(&live_instance_id)?;
        if live_workspace_id.as_ref() != Some(&workspace_id) {
            return Err(ControlPlaneError::WorkspaceActiveBindingMismatch {
                workspace_id,
                live_instance_id,
            }
            .into());
        }
        let Some(live) = self.store.live_instance(&live_instance_id)? else {
            return Err(ControlPlaneError::MissingLiveInstance(live_instance_id).into());
        };
        let (turn, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.upsert_provider_session(provider_session);
            state.record_live_instance_projection(live, live_workspace_id);
            let turn = state.start_turn(
                workspace_id,
                provider_session_id,
                live_instance_id,
                source,
                input,
                reply_to_channel_message_id,
            )?;
            let entities = state.persistence_entities_for_turn_lifecycle(&turn.turn_id);
            (turn, entities)
        };
        self.upsert_persistence_entities(entities)?;
        Ok(turn)
    }

    pub fn start_command(&self, workflow: CommandWorkflow) -> Result<CommandWorkflow, CoreError> {
        let Some(workspace) = self.store.workspace(&workflow.workspace_id)? else {
            return Err(ControlPlaneError::MissingWorkspace(workflow.workspace_id).into());
        };
        let Some(turn) = self.store.turn(&workflow.turn_id)? else {
            return Err(ControlPlaneError::MissingTurn(workflow.turn_id).into());
        };
        let (command, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.record_turn_projection(turn);
            let command = state.start_command(workflow)?;
            (command, state.persistence_entities_without_timeline())
        };
        self.persist_non_timeline_entities(entities)?;
        self.store.upsert_entity_state(
            "command",
            command.command_id.as_str(),
            Some(command.workspace_id.as_str()),
            &command,
        )?;
        Ok(command)
    }

    pub fn active_provider_session_ref(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<String, CoreError> {
        let workspace = self
            .workspace_binding(workspace_id)
            .ok_or_else(|| CoreError::invalid_state("workspace not found"))?;
        let provider_session_id = workspace
            .active_provider_session_id
            .as_ref()
            .ok_or_else(|| CoreError::invalid_state("workspace has no active provider session"))?;
        let provider_session = self
            .provider_session_record(provider_session_id)
            .ok_or_else(|| CoreError::invalid_state("provider session not found"))?;
        Ok(provider_session.native_resume_ref.to_string())
    }

    pub fn active_provider_session_id(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<ProviderSessionId, CoreError> {
        let workspace = self
            .workspace_binding(workspace_id)
            .ok_or_else(|| CoreError::invalid_state("workspace not found"))?;
        let provider_session_id = workspace
            .active_provider_session_id
            .as_ref()
            .ok_or_else(|| CoreError::invalid_state("workspace has no active provider session"))?;
        if self.provider_session_record(provider_session_id).is_none() {
            return Err(CoreError::invalid_state("provider session not found"));
        }
        Ok(provider_session_id.clone())
    }

    pub fn complete_turn_with_usage_value(
        &self,
        turn_id: TurnId,
        usage: Option<serde_json::Value>,
    ) -> Result<(), CoreError> {
        let workspace_id = complete_turn_record_with_usage_value(
            &self.store,
            &self.state,
            turn_id.clone(),
            usage,
        )?;
        self.remove_submitted_turn(&workspace_id, &turn_id);
        Ok(())
    }

    pub fn update_turn_usage(
        &self,
        turn_id: &TurnId,
        usage: serde_json::Value,
    ) -> Result<(), CoreError> {
        let Some(turn_record) = self.store.turn(turn_id)? else {
            return Err(ControlPlaneError::MissingTurn(turn_id.clone()).into());
        };
        let updated = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_turn_projection(turn_record);
            state.update_turn_usage(turn_id, usage)?
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_turn_record(
            &updated,
        ))
    }

    pub fn mark_turn_waiting_permission(&self, turn_id: &TurnId) -> Result<(), CoreError> {
        let Some(turn_record) = self.store.turn(turn_id)? else {
            return Err(ControlPlaneError::MissingTurn(turn_id.clone()).into());
        };
        let live_owner = self
            .store
            .workspace_id_for_live_instance(&turn_record.live_instance_id)?;
        let Some(live_record) = self.store.live_instance(&turn_record.live_instance_id)? else {
            return Err(
                ControlPlaneError::MissingLiveInstance(turn_record.live_instance_id).into(),
            );
        };
        let live = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_turn_projection(turn_record);
            state.record_live_instance_projection(live_record, live_owner.clone());
            state.mark_turn_waiting_permission(turn_id)?
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_live_instance(
            &live,
            live_owner.as_ref(),
        ))
    }

    pub fn mark_live_instance_running(
        &self,
        live_instance_id: &LiveInstanceId,
    ) -> Result<(), CoreError> {
        let live_owner = self
            .store
            .workspace_id_for_live_instance(live_instance_id)?;
        let Some(live_record) = self.store.live_instance(live_instance_id)? else {
            return Err(ControlPlaneError::MissingLiveInstance(live_instance_id.clone()).into());
        };
        let live = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_live_instance_projection(live_record, live_owner.clone());
            state.mark_live_instance_running(live_instance_id)?
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_live_instance(
            &live,
            live_owner.as_ref(),
        ))
    }

    pub fn fail_turn(&self, turn_id: TurnId, error: &str) -> Result<(), CoreError> {
        let workspace_id = fail_turn_record(&self.store, &self.state, turn_id.clone(), error)?;
        self.remove_submitted_turn(&workspace_id, &turn_id);
        Ok(())
    }

    pub fn append_timeline(&self, item: TimelineItem) -> Result<TimelineItem, CoreError> {
        let Some(workspace) = self.store.workspace(&item.workspace_id)? else {
            return Err(ControlPlaneError::MissingWorkspace(item.workspace_id).into());
        };
        let Some(turn) = self.store.turn(&item.turn_id)? else {
            return Err(ControlPlaneError::MissingTurn(item.turn_id).into());
        };
        let (item, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_workspace_projection(workspace);
            state.record_turn_projection(turn);
            let item = state.append_timeline(item)?;
            let entities = state.persistence_entities_for_timeline_item(&item);
            (item, entities)
        };
        self.upsert_persistence_entities(entities)?;
        Ok(item)
    }

    pub fn timeline_item(
        &self,
        workspace_id: &WorkspaceId,
        seq: crate::control_plane::TimelineSeq,
    ) -> Result<Option<TimelineItem>, CoreError> {
        Ok(self.store.timeline_item(workspace_id, seq)?)
    }

    pub fn turn_record(&self, turn_id: &TurnId) -> Option<TurnRecord> {
        self.store.turn(turn_id).unwrap_or_else(|err| {
            warn!(
                target: "lucarne::core_service",
                turn_id = turn_id.as_str(),
                error = %err,
                "turn store lookup failed"
            );
            None
        })
    }

    pub fn record_subagent_action(
        &self,
        action: SubAgentActionRecord,
    ) -> Result<SubAgentActionRecord, CoreError> {
        let (action, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            let action = state.record_subagent_action(action);
            (action, state.persistence_entities_without_timeline())
        };
        self.persist_non_timeline_entities(entities)?;
        self.store.upsert_entity_state(
            "subagent_action",
            action.action_id.as_str(),
            Some(action.workspace_id.as_str()),
            &action,
        )?;
        Ok(action)
    }

    pub fn upsert_subagent_link(
        &self,
        link: SubAgentLinkRecord,
    ) -> Result<SubAgentLinkRecord, CoreError> {
        let existing = self.store.subagent_link(&link.link_id)?;
        let link = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing) = existing {
                state.record_subagent_link_projection(existing);
            }
            state.upsert_subagent_link(link)
        };
        self.store.upsert_entity_state(
            "subagent_link",
            link.link_id.as_str(),
            Some(link.workspace_id.as_str()),
            &link,
        )?;
        Ok(link)
    }

    pub fn complete_command_for_policy(
        &self,
        command_id: CommandId,
        policy: CommandCompletionPolicy,
        result: serde_json::Value,
    ) -> Result<(), CoreError> {
        let Some(command) = self.store.command(&command_id)? else {
            return Err(ControlPlaneError::MissingCommand(command_id).into());
        };
        let updated = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_command_projection(command);
            state.complete_command_for_policy(command_id, policy, result)?
        };
        self.store.upsert_entity_state(
            "command",
            updated.command_id.as_str(),
            Some(updated.workspace_id.as_str()),
            &updated,
        )?;
        Ok(())
    }

    pub fn fail_command(&self, command_id: CommandId, error: &str) -> Result<(), CoreError> {
        let Some(command) = self.store.command(&command_id)? else {
            return Err(ControlPlaneError::MissingCommand(command_id).into());
        };
        let updated = {
            let mut state = self.state.write().expect("control plane lock");
            state.record_command_projection(command);
            state.fail_command(command_id, error)?
        };
        self.store.upsert_entity_state(
            "command",
            updated.command_id.as_str(),
            Some(updated.workspace_id.as_str()),
            &updated,
        )?;
        Ok(())
    }

    pub fn runtime(&self) -> Arc<AgentRuntime> {
        Arc::clone(&self.runtime)
    }

    pub fn control_plane_store(&self) -> ControlPlaneSqliteStore {
        self.store.clone()
    }

    fn ensure_provider(&self, provider_id: &str) -> Result<(), CoreError> {
        if self.provider_ids.contains(&provider_id) {
            return Ok(());
        }
        Err(CoreError::UnknownProvider(provider_id.to_string()))
    }

    fn provider_id_static(&self, provider_id: &str) -> Result<&'static str, CoreError> {
        let provider_id = provider_id.trim();
        self.provider_ids
            .iter()
            .copied()
            .find(|known| *known == provider_id)
            .ok_or_else(|| CoreError::UnknownProvider(provider_id.to_string()))
    }

    fn summary_from_binding(&self, binding: WorkspaceBinding) -> WorkspaceSummary {
        WorkspaceSummary {
            provider_id: self
                .provider_id_static(binding.provider_id.as_str())
                .unwrap_or("unknown"),
            workspace_id: binding.workspace_id,
            title: binding.title.to_string(),
            project_path: Some(binding.project_path),
            revision: binding.revision,
        }
    }

    fn mutate_system_settings_and_persist<T>(
        &self,
        mutate: impl FnOnce(&mut ControlPlaneState) -> Result<(T, bool), CoreError>,
    ) -> Result<T, CoreError> {
        let (result, entities) = {
            let mut state = self.state.write().expect("control plane lock");
            let (result, changed) = mutate(&mut state)?;
            let entities = changed.then(|| state.persistence_entities_for_system_settings());
            (result, entities)
        };
        if let Some(entities) = entities {
            self.upsert_persistence_entities(entities)?;
        }
        Ok(result)
    }

    fn persist_non_timeline_entities(
        &self,
        entities: Vec<ControlPlanePersistenceEntity>,
    ) -> Result<(), CoreError> {
        self.store.replace_non_timeline_entities(entities)?;
        Ok(())
    }

    fn upsert_persistence_entities(
        &self,
        entities: Vec<ControlPlanePersistenceEntity>,
    ) -> Result<(), CoreError> {
        self.store.upsert_entities(entities)?;
        Ok(())
    }

    fn record_workspace_with_id(
        &self,
        workspace_id: Option<WorkspaceId>,
        req: OpenWorkspaceRequest,
    ) -> Result<WorkspaceSummary, CoreError> {
        let OpenWorkspaceRequest {
            provider_id,
            project_path,
            title,
        } = req;
        self.ensure_provider(provider_id)?;
        let workspace_id = workspace_id.unwrap_or_else(|| {
            WorkspaceId::new(format!("{}:{}", provider_id, uuid::Uuid::new_v4()))
        });
        let project_path = project_path.unwrap_or_else(default_project_path);
        let existing_workspace = self.store.workspace(&workspace_id)?;
        let (summary, binding) = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing_workspace) = existing_workspace {
                state.record_workspace_projection(existing_workspace);
            }
            let binding = state.upsert_workspace(WorkspaceBinding::new(
                workspace_id.clone(),
                title,
                provider_id,
                project_path,
            ));
            (self.summary_from_binding(binding.clone()), binding)
        };
        self.upsert_persistence_entities(ControlPlaneState::persistence_entities_for_workspace(
            &binding,
        ))?;
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: workspace_id.clone(),
        });
        Ok(summary)
    }

    fn record_live_workspace(
        &self,
        workspace_id: Option<WorkspaceId>,
        req: OpenWorkspaceRequest,
        native_resume_ref: &str,
        live_instance_id: &LiveInstanceId,
    ) -> Result<LiveWorkspace, CoreError> {
        let provider_id = req.provider_id;
        let workspace = self.record_workspace_with_id(workspace_id, req)?;
        self.attach_live_session_record(
            workspace.workspace_id,
            provider_id,
            native_resume_ref,
            live_instance_id,
        )
    }

    fn attach_live_session_record(
        &self,
        workspace_id: WorkspaceId,
        provider_id: &'static str,
        native_resume_ref: &str,
        live_instance_id: &LiveInstanceId,
    ) -> Result<LiveWorkspace, CoreError> {
        let provider_session_id = provider_session_id(provider_id, native_resume_ref);
        let existing_workspace = self.store.workspace(&workspace_id)?;
        let existing_provider_session = self.store.provider_session(&provider_session_id)?;
        let (workspace, provider_session, live) = {
            let mut state = self.state.write().expect("control plane lock");
            if let Some(existing_workspace) = existing_workspace {
                state.record_workspace_projection(existing_workspace);
            }
            let provider_session =
                state.upsert_provider_session(existing_provider_session.unwrap_or_else(|| {
                    ProviderSessionRecord::new(
                        provider_session_id.clone(),
                        provider_id,
                        native_resume_ref,
                    )
                }));
            let live = state.attach_live_instance(
                workspace_id.clone(),
                LiveInstanceRecord::new(
                    live_instance_id.clone(),
                    provider_id,
                    provider_session_id,
                    Option::<String>::None,
                ),
            )?;
            let workspace = state
                .get_workspace(&workspace_id)
                .ok_or_else(|| ControlPlaneError::MissingWorkspace(workspace_id.clone()))?
                .clone();
            (workspace, provider_session, live)
        };
        self.upsert_persistence_entities(
            [
                ControlPlaneState::persistence_entities_for_workspace(&workspace),
                ControlPlaneState::persistence_entities_for_provider_session(&provider_session),
                ControlPlaneState::persistence_entities_for_live_instance(
                    &live,
                    Some(&workspace.workspace_id),
                ),
            ]
            .concat(),
        )?;
        let _ = self.events.send(CoreEvent::WorkspaceChanged {
            workspace_id: workspace_id.clone(),
        });
        Ok(LiveWorkspace {
            workspace_id,
            provider_id,
            session_id: crate::agent_runtime::SessionId(SmolStr::new(native_resume_ref)),
        })
    }

    fn remove_runtime_workspace(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Option<Arc<dyn AgentSession>> {
        let session = self
            .live_sessions
            .write()
            .expect("live session registry lock")
            .remove(workspace_id);
        self.live_session_generations
            .write()
            .expect("live session generation lock")
            .remove(workspace_id);
        self.workspace_events
            .write()
            .expect("workspace event registry lock")
            .remove(workspace_id);
        self.notification_suppression
            .write()
            .expect("notification suppression lock")
            .remove(workspace_id);
        let removed_turns = self
            .submitted_turns
            .write()
            .expect("submitted turn lock")
            .remove(workspace_id)
            .unwrap_or_default();
        if !removed_turns.is_empty() {
            let removed_turns = removed_turns.into_iter().collect::<HashSet<_>>();
            self.submitted_turn_activity
                .write()
                .expect("submitted turn activity lock")
                .retain(|turn_id, _| !removed_turns.contains(turn_id));
            self.submitted_turn_permits
                .write()
                .expect("submitted turn permit lock")
                .retain(|turn_id, _| !removed_turns.contains(turn_id));
        }
        session
    }

    fn agent_resource_targets(
        &self,
        scope: AgentResourceScope,
    ) -> Result<Vec<AgentProcessTarget>, CoreError> {
        let live_sessions = self
            .live_sessions
            .read()
            .expect("live session registry lock")
            .iter()
            .map(|(workspace_id, session)| (workspace_id.clone(), Arc::clone(session)))
            .collect::<Vec<_>>();
        let state = self.state.read().expect("control plane lock");
        let mut targets = Vec::new();
        for (workspace_id, session) in live_sessions {
            if let AgentResourceScope::Workspace(scope_workspace_id) = &scope {
                if &workspace_id != scope_workspace_id {
                    continue;
                }
            }
            let Some(workspace) = state.get_workspace(&workspace_id) else {
                continue;
            };
            let native_resume_ref = session.id().0.clone();
            let provider_id = session.provider_id();
            let provider_session_id =
                provider_session_id(provider_id.as_str(), native_resume_ref.as_str());
            let live_instance_id = live_instance_id(session.instance_id().0.as_str());
            let (last_active_unix, last_active_display) = state
                .get_live_instance(&live_instance_id)
                .map(live_instance_last_active)
                .unwrap_or_else(|| (0, String::new()));
            targets.push(AgentProcessTarget {
                workspace_id,
                title: workspace.title.clone(),
                provider_id: provider_id.as_str(),
                provider_session_id,
                native_resume_ref,
                live_instance_id,
                last_active_unix,
                last_active_display,
                pid: session.process_id(),
            });
        }
        Ok(targets)
    }

    async fn live_session(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Arc<dyn AgentSession>, CoreError> {
        self.live_sessions
            .read()
            .expect("live session registry lock")
            .get(workspace_id)
            .cloned()
            .ok_or_else(|| CoreError::invalid_state("workspace has no live session"))
    }

    fn remember_submitted_turn(
        &self,
        workspace_id: WorkspaceId,
        turn_id: TurnId,
        turn_permit: TurnPermit,
    ) {
        let now = Instant::now();
        self.submitted_turns
            .write()
            .expect("submitted turn lock")
            .entry(workspace_id)
            .or_default()
            .push_back(turn_id.clone());
        self.submitted_turn_activity
            .write()
            .expect("submitted turn activity lock")
            .insert(
                turn_id.clone(),
                SubmittedTurnActivity {
                    last_event_at: now,
                    waiting_intervention: false,
                },
            );
        self.submitted_turn_permits
            .write()
            .expect("submitted turn permit lock")
            .insert(turn_id, turn_permit);
    }

    fn remove_submitted_turn(&self, workspace_id: &WorkspaceId, turn_id: &TurnId) {
        let mut submitted_turns = self.submitted_turns.write().expect("submitted turn lock");
        if let Some(turns) = submitted_turns.get_mut(workspace_id) {
            turns.retain(|queued| queued != turn_id);
            if turns.is_empty() {
                submitted_turns.remove(workspace_id);
            }
        }
        self.submitted_turn_activity
            .write()
            .expect("submitted turn activity lock")
            .remove(turn_id);
        release_submitted_turn_permit(&self.submitted_turn_permits, turn_id);
    }

    fn spawn_submitted_turn_watchdog(&self, workspace_id: WorkspaceId, turn_id: TurnId) {
        let events = self.events.clone();
        let workspace_events = self.workspace_event_sender(&workspace_id);
        let submitted_turns = Arc::clone(&self.submitted_turns);
        let submitted_turn_activity = Arc::clone(&self.submitted_turn_activity);
        let submitted_turn_permits = Arc::clone(&self.submitted_turn_permits);
        let state = Arc::clone(&self.state);
        let store = self.store.clone();
        let options = self.options.clone();
        tokio::spawn(async move {
            loop {
                if !submitted_turn_is_current(&submitted_turns, &workspace_id, &turn_id) {
                    return;
                }
                let Some(delay) = submitted_turn_timeout_delay(
                    &submitted_turn_activity,
                    &turn_id,
                    Instant::now(),
                    &options,
                ) else {
                    return;
                };
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                    continue;
                }
                let Some(error) = submitted_turn_timeout_error(
                    &submitted_turn_activity,
                    &turn_id,
                    Instant::now(),
                    &options,
                ) else {
                    continue;
                };
                if !pop_submitted_turn_if_current(
                    &submitted_turns,
                    &submitted_turn_activity,
                    &submitted_turn_permits,
                    &workspace_id,
                    &turn_id,
                ) {
                    return;
                }
                warn!(
                    target: "lucarne::core_service",
                    workspace_id = %workspace_id.as_str(),
                    turn_id = %turn_id.as_str(),
                    error = %error,
                    "submitted turn watchdog failed stuck live turn without closing live session"
                );
                if let Err(err) = fail_turn_record(&store, &state, turn_id.clone(), &error) {
                    warn!(
                        target: "lucarne::core_service",
                        workspace_id = %workspace_id.as_str(),
                        turn_id = %turn_id.as_str(),
                        error = %err,
                        "failed to fail submitted turn record after watchdog timeout"
                    );
                }
                emit_submitted_turn_failure(
                    &events,
                    &workspace_events,
                    &workspace_id,
                    &turn_id,
                    error,
                );
                return;
            }
        });
    }
}

fn complete_turn_record_with_usage_value(
    store: &ControlPlaneSqliteStore,
    state: &Arc<RwLock<ControlPlaneState>>,
    turn_id: TurnId,
    usage: Option<serde_json::Value>,
) -> Result<WorkspaceId, CoreError> {
    let Some(turn_record) = store.turn(&turn_id)? else {
        return Err(ControlPlaneError::MissingTurn(turn_id).into());
    };
    let live_owner = store.workspace_id_for_live_instance(&turn_record.live_instance_id)?;
    let Some(live_record) = store.live_instance(&turn_record.live_instance_id)? else {
        return Err(ControlPlaneError::MissingLiveInstance(turn_record.live_instance_id).into());
    };
    let (workspace_id, live_instance_id, entities) = {
        let mut state = state.write().expect("control plane lock");
        state.record_turn_projection(turn_record);
        state.record_live_instance_projection(live_record, live_owner);
        let turn = state.complete_turn_with_usage(turn_id.clone(), usage)?;
        let workspace_id = turn.workspace_id;
        let live_instance_id = turn.live_instance_id.clone();
        let entities = state.persistence_entities_for_turn_lifecycle(&turn_id);
        (workspace_id, live_instance_id, entities)
    };
    store.delete_intervention_callbacks_for_live_instances(std::slice::from_ref(
        &live_instance_id,
    ))?;
    store.upsert_entities(entities)?;
    Ok(workspace_id)
}

fn fail_turn_record(
    store: &ControlPlaneSqliteStore,
    state: &Arc<RwLock<ControlPlaneState>>,
    turn_id: TurnId,
    error: &str,
) -> Result<WorkspaceId, CoreError> {
    let Some(turn_record) = store.turn(&turn_id)? else {
        return Err(ControlPlaneError::MissingTurn(turn_id).into());
    };
    let live_owner = store.workspace_id_for_live_instance(&turn_record.live_instance_id)?;
    let Some(live_record) = store.live_instance(&turn_record.live_instance_id)? else {
        return Err(ControlPlaneError::MissingLiveInstance(turn_record.live_instance_id).into());
    };
    let (workspace_id, live_instance_id, entities) = {
        let mut state = state.write().expect("control plane lock");
        state.record_turn_projection(turn_record);
        state.record_live_instance_projection(live_record, live_owner);
        let turn = state.fail_turn(turn_id.clone(), error)?;
        let workspace_id = turn.workspace_id;
        let live_instance_id = turn.live_instance_id.clone();
        let entities = state.persistence_entities_for_turn_lifecycle(&turn_id);
        (workspace_id, live_instance_id, entities)
    };
    store.delete_intervention_callbacks_for_live_instances(std::slice::from_ref(
        &live_instance_id,
    ))?;
    store.upsert_entities(entities)?;
    Ok(workspace_id)
}

async fn recv_agent_activity(activity_source: &mut Option<AgentActivityStream>) -> Option<()> {
    match activity_source.as_mut() {
        Some(activity_source) => activity_source.recv().await,
        None => std::future::pending::<Option<()>>().await,
    }
}

#[derive(Debug, Clone)]
struct AgentProcessTarget {
    workspace_id: WorkspaceId,
    title: SmolStr,
    provider_id: &'static str,
    provider_session_id: ProviderSessionId,
    native_resume_ref: SmolStr,
    live_instance_id: LiveInstanceId,
    last_active_unix: i64,
    last_active_display: String,
    pid: Option<i32>,
}

impl AgentProcessTarget {
    fn identity(&self) -> Option<String> {
        self.pid
            .map(|pid| format!("{}:{pid}", self.native_resume_ref))
    }

    fn provider_identity(&self) -> Option<String> {
        self.pid
            .map(|pid| format!("{}:{pid}", self.provider_session_id.as_str()))
    }

    fn matches_identity(&self, identity: &str) -> bool {
        self.identity().as_deref() == Some(identity)
            || self.provider_identity().as_deref() == Some(identity)
    }

    fn killed_agent(&self) -> KilledAgent {
        KilledAgent {
            workspace_id: self.workspace_id.clone(),
            provider_session_id: self.provider_session_id.clone(),
            native_resume_ref: self.native_resume_ref.as_str().into(),
            live_instance_id: self.live_instance_id.clone(),
            pid: self.pid,
            identity: self.identity(),
        }
    }
}

fn build_agent_resource_snapshot(
    targets: Vec<AgentProcessTarget>,
    samples: &[ProcessSample],
) -> AgentResourceSnapshot {
    let mut agents = Vec::with_capacity(targets.len());
    for target in targets {
        let aggregate = target
            .pid
            .map(|pid| process_table::aggregate_for_root(pid, samples))
            .unwrap_or(ProcessAggregate {
                process_count: 0,
                memory_bytes: 0,
                cpu_percent: 0.0,
            });
        let identity = target.identity();
        agents.push(AgentResourceEntry {
            workspace_id: target.workspace_id,
            title: target.title,
            provider_id: target.provider_id,
            provider_session_id: target.provider_session_id,
            native_resume_ref: target.native_resume_ref.as_str().into(),
            live_instance_id: target.live_instance_id,
            pid: target.pid,
            identity,
            process_count: aggregate.process_count,
            cpu_percent: aggregate.cpu_percent,
            memory_bytes: aggregate.memory_bytes,
            last_active_unix: target.last_active_unix,
            last_active_display: target.last_active_display,
        });
    }
    let process_count = agents.iter().map(|agent| agent.process_count).sum();
    let total_cpu_percent = agents.iter().map(|agent| agent.cpu_percent).sum();
    let total_memory_bytes = agents.iter().map(|agent| agent.memory_bytes).sum();
    AgentResourceSnapshot {
        managed_agent_count: agents.len(),
        process_count,
        total_cpu_percent,
        total_memory_bytes,
        agents,
        observed_sessions: Vec::new(),
    }
}

fn remember_live_runtime_turn_claim(
    claims: &Arc<RwLock<HashMap<LiveRuntimeTurnClaimKey, Instant>>>,
    provider_session_id: ProviderSessionId,
    provider_turn_id: SmolStr,
) {
    let provider_turn_id = provider_turn_id.trim();
    if provider_turn_id.is_empty() {
        return;
    }
    let now = Instant::now();
    let mut claims = claims.write().expect("live runtime turn claim lock");
    prune_live_runtime_turn_claims(&mut claims, now);
    claims.insert(
        LiveRuntimeTurnClaimKey {
            provider_session_id,
            provider_turn_id: provider_turn_id.into(),
        },
        now,
    );
}

fn remember_live_runtime_terminal_text_claim(
    claims: &Arc<RwLock<HashMap<LiveRuntimeTerminalTextClaimKey, Instant>>>,
    provider_session_id: ProviderSessionId,
    text: SmolStr,
) {
    let Some(text) = terminal_text_claim_text(text.as_str()) else {
        return;
    };
    let now = Instant::now();
    let mut claims = claims
        .write()
        .expect("live runtime terminal text claim lock");
    prune_live_runtime_terminal_text_claims(&mut claims, now);
    claims.insert(
        LiveRuntimeTerminalTextClaimKey {
            provider_session_id,
            text,
        },
        now,
    );
}

fn watched_event_terminal_text(event: &WatchEvent) -> Option<SmolStr> {
    match event {
        WatchEvent::TurnCompleted(completion) => {
            terminal_text_claim_text(completion.last_agent_message.as_deref()?)
        }
        _ => None,
    }
}

fn terminal_text_claim_text(text: &str) -> Option<SmolStr> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.into())
}

fn prune_live_runtime_turn_claims(
    claims: &mut HashMap<LiveRuntimeTurnClaimKey, Instant>,
    now: Instant,
) {
    claims.retain(|_, claimed_at| now.duration_since(*claimed_at) <= LIVE_RUNTIME_TURN_CLAIM_TTL);
}

fn prune_live_runtime_terminal_text_claims(
    claims: &mut HashMap<LiveRuntimeTerminalTextClaimKey, Instant>,
    now: Instant,
) {
    claims.retain(|_, claimed_at| now.duration_since(*claimed_at) <= LIVE_RUNTIME_TURN_CLAIM_TTL);
}

fn current_submitted_turn(
    submitted_turns: &Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    workspace_id: &WorkspaceId,
) -> Option<TurnId> {
    submitted_turns
        .read()
        .expect("submitted turn lock")
        .get(workspace_id)
        .and_then(|turns| turns.front().cloned())
}

fn submitted_turn_is_current(
    submitted_turns: &Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
) -> bool {
    current_submitted_turn(submitted_turns, workspace_id).as_ref() == Some(turn_id)
}

fn pop_submitted_turn(
    submitted_turns: &Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    permits: &Arc<RwLock<HashMap<TurnId, TurnPermit>>>,
    workspace_id: &WorkspaceId,
) {
    let mut submitted_turns = submitted_turns.write().expect("submitted turn lock");
    let Some(turns) = submitted_turns.get_mut(workspace_id) else {
        return;
    };
    let popped = turns.pop_front();
    if turns.is_empty() {
        submitted_turns.remove(workspace_id);
    }
    drop(submitted_turns);
    if let Some(turn_id) = popped {
        activity
            .write()
            .expect("submitted turn activity lock")
            .remove(&turn_id);
        release_submitted_turn_permit(permits, &turn_id);
    }
}

fn pop_submitted_turn_if_current(
    submitted_turns: &Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    permits: &Arc<RwLock<HashMap<TurnId, TurnPermit>>>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
) -> bool {
    let mut submitted_turns = submitted_turns.write().expect("submitted turn lock");
    let Some(turns) = submitted_turns.get_mut(workspace_id) else {
        return false;
    };
    if turns.front() != Some(turn_id) {
        return false;
    }
    turns.pop_front();
    if turns.is_empty() {
        submitted_turns.remove(workspace_id);
    }
    drop(submitted_turns);
    activity
        .write()
        .expect("submitted turn activity lock")
        .remove(turn_id);
    release_submitted_turn_permit(permits, turn_id);
    true
}

fn release_submitted_turn_permit(
    permits: &Arc<RwLock<HashMap<TurnId, TurnPermit>>>,
    turn_id: &TurnId,
) {
    permits
        .write()
        .expect("submitted turn permit lock")
        .remove(turn_id);
}

fn mark_current_submitted_turn_intervention_resolved(
    submitted_turns: &Arc<RwLock<HashMap<WorkspaceId, VecDeque<TurnId>>>>,
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    workspace_id: &WorkspaceId,
) {
    let Some(turn_id) = current_submitted_turn(submitted_turns, workspace_id) else {
        return;
    };
    let mut activity = activity.write().expect("submitted turn activity lock");
    let Some(turn) = activity.get_mut(&turn_id) else {
        return;
    };
    turn.last_event_at = Instant::now();
    turn.waiting_intervention = false;
}

fn touch_submitted_turn_agent_activity(
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    turn_id: &TurnId,
) {
    let mut activity = activity.write().expect("submitted turn activity lock");
    let Some(turn) = activity.get_mut(turn_id) else {
        return;
    };
    turn.last_event_at = Instant::now();
    turn.waiting_intervention = false;
}

fn touch_submitted_turn(
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    turn_id: &TurnId,
    event: &AgentEvent,
) {
    let mut activity = activity.write().expect("submitted turn activity lock");
    let Some(turn) = activity.get_mut(turn_id) else {
        return;
    };
    turn.last_event_at = Instant::now();
    turn.waiting_intervention = matches!(event, AgentEvent::InterventionRequest(_));
}

fn submitted_turn_timeout_error(
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    turn_id: &TurnId,
    now: Instant,
    options: &CoreOptions,
) -> Option<String> {
    let turn = activity
        .read()
        .expect("submitted turn activity lock")
        .get(turn_id)
        .cloned()?;
    let idle = now.saturating_duration_since(turn.last_event_at);
    if idle >= options.turn_deadline {
        return Some(format!(
            "agent turn deadline reached after {} since last activity without turn_complete signal",
            format_duration_ms(options.turn_deadline.as_millis() as u64)
        ));
    }
    if turn.waiting_intervention {
        return None;
    }
    if idle >= options.turn_inactivity {
        return Some(format!(
            "agent went silent for {} after last event (no turn_complete signal)",
            format_duration_ms(options.turn_inactivity.as_millis() as u64)
        ));
    }
    None
}

fn submitted_turn_timeout_delay(
    activity: &Arc<RwLock<HashMap<TurnId, SubmittedTurnActivity>>>,
    turn_id: &TurnId,
    now: Instant,
    options: &CoreOptions,
) -> Option<Duration> {
    let turn = activity
        .read()
        .expect("submitted turn activity lock")
        .get(turn_id)
        .cloned()?;
    let idle = now.saturating_duration_since(turn.last_event_at);
    let deadline_remaining = options.turn_deadline.saturating_sub(idle);
    if deadline_remaining.is_zero() || turn.waiting_intervention {
        return Some(deadline_remaining);
    }
    let inactivity_remaining = options.turn_inactivity.saturating_sub(idle);
    Some(deadline_remaining.min(inactivity_remaining))
}

fn emit_submitted_turn_failure(
    events: &broadcast::Sender<CoreEvent>,
    workspace_events: &broadcast::Sender<AgentEvent>,
    workspace_id: &WorkspaceId,
    turn_id: &TurnId,
    error: String,
) {
    let event = AgentEvent::TurnFailed(crate::agent_runtime::events::TurnFailedEvent {
        turn_id: turn_id.as_str().into(),
        error: error.clone().into(),
        code: "timeout".into(),
    });
    let _ = events.send(CoreEvent::TimelineEvent {
        workspace_id: workspace_id.clone(),
        turn_id: Some(turn_id.clone()),
        event: event.clone(),
    });
    let _ = workspace_events.send(event);
    let _ = events.send(CoreEvent::TurnFailed {
        workspace_id: workspace_id.clone(),
        error,
    });
}

#[async_trait]
impl DaemonApi for LucarneCore {
    fn providers(&self) -> Vec<KnownAgentProvider> {
        self.providers()
    }

    fn provider_catalog(&self) -> Vec<ProviderCatalogEntry> {
        self.provider_catalog()
    }

    fn list_sessions(&self) -> Vec<WorkspaceSummary> {
        self.list_sessions()
    }

    fn list_history(&self, offset: usize, limit: usize) -> HistoryPage {
        self.list_history(offset, limit)
    }

    fn watch_events(&self) -> CoreEventReceiver {
        self.watch_events()
    }

    async fn open(&self, req: OpenWorkspaceRequest) -> Result<LiveWorkspace, CoreError> {
        self.open_workspace(req).await
    }

    async fn resume(&self, req: ResumeWorkspaceRequest) -> Result<LiveWorkspace, CoreError> {
        self.resume_workspace(req).await
    }

    async fn submit(&self, req: SubmitTurnRequest) -> Result<SubmittedTurn, CoreError> {
        self.submit_turn(req).await
    }

    async fn upsert_scheduled_task(
        &self,
        req: UpsertScheduledTaskRequest,
    ) -> Result<ScheduledTaskRecord, CoreError> {
        LucarneCore::upsert_scheduled_task(self, req)
    }

    async fn run_due_scheduled_tasks(
        &self,
        req: RunDueScheduledTasksRequest,
    ) -> Result<RunScheduledTasksReport, CoreError> {
        LucarneCore::run_due_scheduled_tasks(self, req).await
    }

    async fn interrupt(&self, req: InterruptTurnRequest) -> Result<(), CoreError> {
        self.interrupt_turn(req).await
    }

    async fn resolve(&self, req: ResolvePermissionRequest) -> Result<(), CoreError> {
        self.resolve_permission(req).await
    }

    async fn invoke_command(
        &self,
        req: InvokeCommandRequest,
    ) -> Result<AgentCommandResult, CoreError> {
        self.invoke_command(req).await
    }

    async fn close(&self, req: CloseWorkspaceRequest) -> Result<(), CoreError> {
        self.close_workspace(req).await
    }
}

fn provider_session_id(provider_id: &str, native_ref: &str) -> ProviderSessionId {
    ProviderSessionId::new(format!("{provider_id}:{native_ref}"))
}

fn workspace_id_for_resume(provider_id: &str, resume_ref: &str) -> WorkspaceId {
    WorkspaceId::new(format!(
        "{provider_id}:resume:{}",
        stable_resume_token(provider_id, resume_ref)
    ))
}

fn stable_resume_token(provider_id: &str, resume_ref: &str) -> String {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in provider_id
        .bytes()
        .chain(std::iter::once(0))
        .chain(resume_ref.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    format!("{hash:016x}")
}

fn workspace_title_from_history_entry(entry: &crate::history::HistoryEntry) -> String {
    let base = entry.cwd.as_deref().unwrap_or(&entry.summary);
    let short_base = history_title_path_tail(base);
    format!("{} - {}", entry.provider_id, short_title(short_base, 32))
}

#[cfg(not(windows))]
fn history_title_path_tail(base: &str) -> &str {
    base.rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(base)
}

#[cfg(windows)]
fn history_title_path_tail(base: &str) -> &str {
    base.rsplit(['/', '\\'])
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(base)
}

fn history_entry_from_watch_update(
    update: &WatchUpdate,
) -> Result<crate::history::HistoryEntry, CoreError> {
    let provider_id = update.provider.id();
    let session_id = update
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|session_id| !session_id.is_empty())
        .ok_or_else(|| {
            CoreError::HistoryWatch(format!(
                "watched {provider_id} session has no provider session id: {}",
                update.path.display()
            ))
        })?;
    let cwd = update
        .cwd
        .as_deref()
        .map(str::trim)
        .filter(|cwd| !cwd.is_empty())
        .ok_or_else(|| {
            CoreError::HistoryWatch(format!(
                "watched {provider_id} session has no cwd: {}",
                update.path.display()
            ))
        })?;
    Ok(crate::history::HistoryEntry {
        provider_id,
        session_id: session_id.to_string(),
        session_path: update.path.clone(),
        cwd: Some(cwd.to_string()),
        summary: update.title.as_deref().unwrap_or_default().to_string(),
        last_active_unix: 0,
        last_active_display: String::new(),
    })
}

fn short_title(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out = text
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect::<String>();
    out.push_str("...");
    out
}

fn observed_prompt_title(events: &[WatchEvent]) -> Option<String> {
    events.iter().rev().find_map(|event| {
        event
            .user_text()
            .and_then(|text| compact_observed_title(text))
    })
}

fn compact_observed_title(text: &str) -> Option<String> {
    let title = text.split_whitespace().collect::<Vec<_>>().join(" ");
    (!title.is_empty()).then(|| short_title(&title, 160))
}

fn observed_activity_time(events: &[WatchEvent]) -> (i64, String) {
    let timestamp = events.iter().rev().find_map(WatchEvent::timestamp);
    let last_active_unix = timestamp
        .and_then(parse_rfc3339_unix)
        .unwrap_or_else(current_unix_seconds);
    let last_active_display = crate::time_display::format_last_active_display(last_active_unix);
    (last_active_unix, last_active_display)
}

fn parse_rfc3339_unix(timestamp: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|datetime| datetime.timestamp())
}

fn live_instance_last_active(live: &LiveInstanceRecord) -> (i64, String) {
    let last_active_unix = system_time_unix(live.last_seen_at);
    let last_active_display = crate::time_display::format_last_active_display(last_active_unix);
    (last_active_unix, last_active_display)
}

fn system_time_unix(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn current_unix_seconds() -> i64 {
    system_time_unix(SystemTime::now())
}

fn observed_session_process_alive(session: &ObservedAgentSession) -> bool {
    session.observed_pid.is_none_or(process_id_is_alive)
}

fn process_id_is_alive(pid: i32) -> bool {
    crate::host::process::pid_is_alive(pid)
}

fn observed_session_writer_pid(path: &Path) -> Option<i32> {
    crate::host::file_users::observed_session_writer_pid(path)
}

fn watch_turn_completed_text(
    completion: &WatchTurnCompleted,
    include_duration: bool,
) -> Option<String> {
    let mut text = completion
        .last_agent_message
        .as_deref()
        .map(str::trim)
        .filter(|message| !message.is_empty())?
        .to_string();
    if include_duration {
        if let Some(duration_ms) = completion.duration_ms {
            text.push_str("\n\ncost: ");
            text.push_str(&format_duration_ms(duration_ms));
        }
    }
    Some(text)
}

fn format_duration_ms(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    match (hours, minutes, seconds) {
        (0, 0, seconds) => format!("{seconds}s"),
        (0, minutes, seconds) => format!("{minutes}m {seconds}s"),
        (hours, minutes, seconds) => format!("{hours}h {minutes}m {seconds}s"),
    }
}

fn live_instance_id(instance_id: &str) -> LiveInstanceId {
    LiveInstanceId::new(instance_id)
}

fn live_generation_matches(
    live_session_generations: &Arc<RwLock<HashMap<WorkspaceId, u64>>>,
    workspace_id: &WorkspaceId,
    live_generation: u64,
) -> bool {
    live_session_generations
        .read()
        .expect("live session generation lock")
        .get(workspace_id)
        .copied()
        .is_some_and(|generation| generation == live_generation)
}

fn default_project_path() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}

fn session_args(settings: &EffectiveSettings, force_bypass_permissions: bool) -> serde_json::Value {
    if settings.session.force_bypass_permissions || force_bypass_permissions {
        serde_json::json!({ "permission_mode": PermissionMode::Bypass.as_str() })
    } else {
        serde_json::Value::Null
    }
}

fn resume_session_args(
    settings: &EffectiveSettings,
    force_bypass_permissions: bool,
    project_path: &Path,
) -> Result<serde_json::Value, CoreError> {
    let cwd = project_path
        .to_str()
        .ok_or_else(|| CoreError::invalid_state("workspace project path is not utf-8"))?;
    let mut args = serde_json::Map::new();
    args.insert(
        "cwd".to_string(),
        serde_json::Value::String(cwd.to_string()),
    );
    if settings.session.force_bypass_permissions || force_bypass_permissions {
        args.insert(
            "permission_mode".to_string(),
            serde_json::Value::String(PermissionMode::Bypass.as_str().to_string()),
        );
    }
    Ok(serde_json::Value::Object(args))
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        fs,
        io::Write,
        path::{Path, PathBuf},
        sync::{Arc, Mutex as StdMutex},
    };

    use async_trait::async_trait;
    use base64::Engine as _;

    use super::*;
    use crate::agent_runtime::events::{TurnCompletedEvent, UsageEvent};
    use crate::agent_runtime::{
        AgentErrorKind, AgentEventStream, AgentProvider, AgentSession, ApprovalRequest, InstanceId,
        InterventionRequest, OpenSession, ProbeResult, ReasoningEvent, ResumeSession, SessionId,
    };
    use crate::control_plane::LiveInstanceState;
    use crate::ProviderId;
    use agent_sessions::{
        WatchAssistantMessage, WatchAttachment, WatchEventMeta, WatchMessage, WatchTurnFailed,
    };
    use tokio::sync::Notify;

    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    fn watch_provider(id: &str) -> agent_sessions::WatchProvider {
        agent_sessions::agent_provider(id).expect("watch provider")
    }

    fn expected_history_provider_ids() -> Vec<&'static str> {
        agent_sessions::agent_providers()
            .into_iter()
            .map(|provider| provider.id())
            .collect()
    }

    #[tokio::test]
    async fn direct_notification_suppression_is_counted_runtime_state() {
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-a");

        assert!(!core.direct_notification_suppressed(&workspace_id));
        assert!(!core.direct_notification_delivery_suppressed(&workspace_id));
        core.begin_direct_notification_suppression(&workspace_id);
        core.begin_direct_notification_suppression(&workspace_id);
        assert!(core.direct_notification_suppressed(&workspace_id));
        assert!(core.direct_notification_delivery_suppressed(&workspace_id));
        core.end_direct_notification_suppression(&workspace_id);
        assert!(core.direct_notification_suppressed(&workspace_id));
        assert!(core.direct_notification_delivery_suppressed(&workspace_id));
        core.end_direct_notification_suppression(&workspace_id);
        assert!(!core.direct_notification_suppressed(&workspace_id));
        assert!(
            core.direct_notification_delivery_suppressed(&workspace_id),
            "delivery suppression should keep a short grace window for late core events"
        );
    }

    #[test]
    fn observed_activity_time_formats_display_for_local_timezone_without_year() {
        let timestamp = "2026-04-25T00:01:00.000Z";
        let events = vec![WatchEvent::UserMessage(WatchMessage {
            meta: WatchEventMeta {
                timestamp: Some(timestamp.into()),
                ..WatchEventMeta::default()
            },
            text: Some("prompt".into()),
        })];

        let (unix, display) = observed_activity_time(&events);
        let expected = chrono::DateTime::parse_from_rfc3339(timestamp)
            .expect("timestamp")
            .with_timezone(&chrono::Local)
            .format("%m-%d %H:%M:%S")
            .to_string();

        assert_eq!(
            unix,
            chrono::DateTime::parse_from_rfc3339(timestamp)
                .expect("timestamp")
                .timestamp()
        );
        assert_eq!(display, expected);
        assert!(!display.contains("2026"));
        assert!(!display.contains('T'));
        assert!(!display.ends_with('Z'));
    }

    #[tokio::test]
    async fn opened_workspace_uses_runtime_process_id_for_resource_status() {
        let process_id = std::process::id() as i32;
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(PidProvider { process_id }));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/resource-status")),
                title: "resource status".into(),
            })
            .await
            .expect("open workspace");
        let workspace = core
            .workspace_binding(&opened.workspace.workspace_id)
            .expect("workspace binding");
        let live = core
            .live_instance_record(
                workspace
                    .active_live_instance_id
                    .as_ref()
                    .expect("live instance id"),
            )
            .expect("live instance");

        assert_eq!(live.pid_or_handle, None);

        let snapshot = core
            .agent_resource_snapshot(AgentResourceScope::Workspace(opened.workspace.workspace_id))
            .await
            .expect("resource snapshot");
        assert_eq!(snapshot.managed_agent_count, 1);
        let expected_identity = format!("session-pid:{process_id}");
        assert_eq!(
            snapshot.agents[0].identity.as_deref(),
            Some(expected_identity.as_str())
        );
        assert!(snapshot.process_count >= 1);
        assert!(snapshot.observed_sessions.is_empty());
        let rendered = crate::core_service::render_agent_resource_snapshot(&snapshot);
        assert!(
            !rendered.contains("last active: `unknown`"),
            "managed live agents should render last active from live session state"
        );
    }

    #[tokio::test]
    async fn agent_resource_snapshot_hides_managed_sessions_from_observed_and_uses_observed_activity(
    ) {
        let process_id = std::process::id() as i32;
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(PidProvider { process_id }));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/resource-status-managed-observed")),
                title: "managed observed".into(),
            })
            .await
            .expect("open workspace");
        let provider_session_id = core
            .workspace_binding(&opened.workspace.workspace_id)
            .and_then(|workspace| workspace.active_provider_session_id)
            .expect("active provider session");
        {
            let mut observed = observed_session_for_test("session-pid", Some(process_id));
            observed.workspace_id = opened.workspace.workspace_id.clone();
            observed.provider_session_id = provider_session_id.clone();
            observed.native_resume_ref = "session-pid".into();
            observed.last_active_unix = 1_776_960_000;
            observed.last_active_display = "05-01 00:00:00".into();
            core.observed_sessions
                .write()
                .expect("observed session registry lock")
                .insert(provider_session_id, observed);
        }

        let snapshot = core
            .agent_resource_snapshot(AgentResourceScope::All)
            .await
            .expect("resource snapshot");

        assert_eq!(snapshot.managed_agent_count, 1);
        assert!(
            snapshot.observed_sessions.is_empty(),
            "managed live session must not also render as observed"
        );
        assert_eq!(snapshot.agents[0].last_active_display, "05-01 00:00:00");
        let rendered = crate::core_service::render_agent_resource_snapshot(&snapshot);
        assert!(rendered.contains("observed recent: `0`"));
        assert!(!rendered.contains("\n\nobserved recent\n\n"));
        assert!(rendered.contains("last active: `05-01 00:00:00`"));
    }

    #[tokio::test]
    async fn agent_resource_snapshot_ignores_persisted_live_records_without_runtime_session() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let process_id = std::process::id() as i32;
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(PidProvider { process_id }));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/resource-status-reload")),
            title: "resource status reload".into(),
        })
        .await
        .expect("open workspace");

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        let snapshot = reloaded
            .agent_resource_snapshot(AgentResourceScope::All)
            .await
            .expect("resource snapshot");

        assert_eq!(snapshot.managed_agent_count, 0);
        assert!(snapshot.agents.is_empty());
    }

    #[tokio::test]
    async fn kill_agent_processes_detaches_live_session_in_scope() {
        let process_id = std::process::id() as i32;
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(PidProvider { process_id }));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/kill-agent")),
                title: "kill agent".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let live_instance_id = core
            .workspace_binding(&workspace_id)
            .and_then(|workspace| workspace.active_live_instance_id)
            .expect("live instance id before kill");

        let report = core
            .kill_agent_processes(KillAgentRequest {
                scope: AgentResourceScope::Workspace(workspace_id.clone()),
                target: KillAgentTarget::All,
            })
            .await
            .expect("kill scoped agent");

        assert_eq!(report.killed.len(), 1);
        assert_eq!(report.killed[0].pid, Some(process_id));
        assert!(!core.has_live_session(&workspace_id));
        let live = core
            .live_instance_record(&live_instance_id)
            .expect("live instance record");
        assert_eq!(live.state, LiveInstanceState::Closed);
    }

    #[test]
    fn submitted_turn_runtime_activity_clears_intervention_wait() {
        let activity = Arc::new(RwLock::new(HashMap::new()));
        let turn_id = TurnId::new("turn-waiting-intervention");
        let now = Instant::now();
        activity.write().expect("activity lock").insert(
            turn_id.clone(),
            SubmittedTurnActivity {
                last_event_at: now,
                waiting_intervention: false,
            },
        );

        touch_submitted_turn(
            &activity,
            &turn_id,
            &AgentEvent::InterventionRequest(InterventionRequest::Approval(ApprovalRequest {
                req_id: "approval-1".into(),
                tool_name: "shell".into(),
                message: Some("approval needed".into()),
                input: None,
            })),
        );
        let after_intervention_activity = activity
            .read()
            .expect("activity lock")
            .get(&turn_id)
            .expect("submitted turn activity")
            .last_event_at;
        std::thread::sleep(Duration::from_millis(5));
        touch_submitted_turn(
            &activity,
            &turn_id,
            &AgentEvent::Usage(UsageEvent {
                input_tokens: Some(1),
                output_tokens: Some(1),
                total_tokens: Some(2),
                raw: serde_json::json!({ "source": "bookkeeping" }),
            }),
        );
        let after_usage_activity = activity
            .read()
            .expect("activity lock")
            .get(&turn_id)
            .expect("submitted turn activity")
            .last_event_at;
        assert!(
            after_usage_activity > after_intervention_activity,
            "any runtime stdio event should refresh submitted-turn activity"
        );

        let test_opts = CoreOptions {
            turn_inactivity: Duration::from_millis(50),
            turn_deadline: Duration::from_millis(250),
            session_idle_timeout: Duration::from_millis(500),
        };
        let check_at = after_usage_activity + test_opts.turn_inactivity + test_opts.turn_inactivity;
        let error = submitted_turn_timeout_error(&activity, &turn_id, check_at, &test_opts)
            .expect("runtime activity should clear an outstanding intervention wait");
        assert!(error.contains("agent went silent"));
    }

    #[test]
    fn submitted_turn_intervention_resolution_restarts_inactivity_timeout() {
        let submitted_turns = Arc::new(RwLock::new(HashMap::new()));
        let activity = Arc::new(RwLock::new(HashMap::new()));
        let workspace_id = WorkspaceId::new("workspace-waiting-intervention");
        let turn_id = TurnId::new("turn-waiting-intervention");
        submitted_turns
            .write()
            .expect("submitted turn lock")
            .insert(workspace_id.clone(), VecDeque::from([turn_id.clone()]));
        let now = Instant::now();
        activity.write().expect("activity lock").insert(
            turn_id.clone(),
            SubmittedTurnActivity {
                last_event_at: now,
                waiting_intervention: true,
            },
        );

        mark_current_submitted_turn_intervention_resolved(
            &submitted_turns,
            &activity,
            &workspace_id,
        );

        let test_opts = CoreOptions {
            turn_inactivity: Duration::from_millis(50),
            turn_deadline: Duration::from_millis(250),
            session_idle_timeout: Duration::from_millis(500),
        };
        let check_at = Instant::now() + test_opts.turn_inactivity + test_opts.turn_inactivity;
        let error = submitted_turn_timeout_error(&activity, &turn_id, check_at, &test_opts)
            .expect("resolved intervention should restart inactivity timeout");
        assert!(error.contains("agent went silent"));
    }

    #[test]
    fn submitted_turn_provider_progress_clears_intervention_wait() {
        let activity = Arc::new(RwLock::new(HashMap::new()));
        let turn_id = TurnId::new("turn-progress-after-intervention");
        let now = Instant::now();
        activity.write().expect("activity lock").insert(
            turn_id.clone(),
            SubmittedTurnActivity {
                last_event_at: now,
                waiting_intervention: false,
            },
        );

        touch_submitted_turn(
            &activity,
            &turn_id,
            &AgentEvent::InterventionRequest(InterventionRequest::Approval(ApprovalRequest {
                req_id: "approval-1".into(),
                tool_name: "shell".into(),
                message: Some("approval needed".into()),
                input: None,
            })),
        );
        touch_submitted_turn(
            &activity,
            &turn_id,
            &AgentEvent::Reasoning(ReasoningEvent {
                text: "provider resumed".into(),
            }),
        );

        let test_opts = CoreOptions {
            turn_inactivity: Duration::from_millis(50),
            turn_deadline: Duration::from_millis(250),
            session_idle_timeout: Duration::from_millis(500),
        };
        let check_at = Instant::now() + test_opts.turn_inactivity + test_opts.turn_inactivity;
        let error = submitted_turn_timeout_error(&activity, &turn_id, check_at, &test_opts)
            .expect("provider progress should re-enable inactivity timeout");
        assert!(error.contains("agent went silent"));
    }

    #[test]
    fn submitted_turn_watchdog_delay_uses_remaining_inactivity_window() {
        let activity = Arc::new(RwLock::new(HashMap::new()));
        let turn_id = TurnId::new("turn-progress-before-first-watchdog-wake");
        let now = Instant::now();
        activity.write().expect("activity lock").insert(
            turn_id.clone(),
            SubmittedTurnActivity {
                last_event_at: now + Duration::from_millis(40),
                waiting_intervention: false,
            },
        );
        let test_opts = CoreOptions {
            turn_inactivity: Duration::from_millis(50),
            turn_deadline: Duration::from_millis(250),
            session_idle_timeout: Duration::from_millis(500),
        };

        let check_at = now + Duration::from_millis(50);
        assert!(submitted_turn_timeout_error(&activity, &turn_id, check_at, &test_opts).is_none());
        assert_eq!(
            submitted_turn_timeout_delay(&activity, &turn_id, check_at, &test_opts),
            Some(Duration::from_millis(40)),
            "watchdog must reschedule to the remaining idle window, not sleep another full interval"
        );

        let timeout_at = now + Duration::from_millis(90);
        assert_eq!(
            submitted_turn_timeout_delay(&activity, &turn_id, timeout_at, &test_opts),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn submitted_turn_deadline_uses_last_activity_window() {
        let activity = Arc::new(RwLock::new(HashMap::new()));
        let turn_id = TurnId::new("turn-active-past-total-deadline");
        let now = Instant::now();
        activity.write().expect("activity lock").insert(
            turn_id.clone(),
            SubmittedTurnActivity {
                last_event_at: now + Duration::from_millis(240),
                waiting_intervention: false,
            },
        );
        let test_opts = CoreOptions {
            turn_inactivity: Duration::from_millis(500),
            turn_deadline: Duration::from_millis(250),
            session_idle_timeout: Duration::from_millis(500),
        };

        let check_at = now + Duration::from_millis(300);
        assert!(
            submitted_turn_timeout_error(&activity, &turn_id, check_at, &test_opts).is_none(),
            "recent activity should prevent deadline timeout even when total turn duration exceeds deadline"
        );
        assert_eq!(
            submitted_turn_timeout_delay(&activity, &turn_id, check_at, &test_opts),
            Some(Duration::from_millis(190)),
            "deadline should be measured from last activity, not turn start"
        );
    }

    #[tokio::test]
    async fn history_watch_update_does_not_notify_codex_phase_response() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "phase-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        core.handle_history_watch_update(WatchUpdate {
            provider: watch_provider("codex"),
            path: session_path,
            session_id: Some("phase-thread".into()),
            cwd: Some(project_path.to_str().expect("utf8 project").into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::AssistantMessage(WatchAssistantMessage {
                meta: WatchEventMeta {
                    timestamp: Some("2026-05-07T10:00:00.000Z".into()),
                    ..WatchEventMeta::default()
                },
                model: None,
                phase: Some("final_answer".into()),
                text: Some("final answer".into()),
            })]
            .into_boxed_slice(),
            error: None,
        })
        .expect("ingest phase response watch update");

        let notification = tokio::time::timeout(Duration::from_millis(50), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await;
        assert!(
            notification.is_err(),
            "Codex response_item records belong to transcript projection; task_complete is the external notification boundary"
        );
    }

    #[test]
    fn history_watch_formats_task_complete_with_duration() {
        let completion = WatchTurnCompleted {
            meta: WatchEventMeta {
                timestamp: Some("2026-05-07T10:02:05.000Z".into()),
                ..WatchEventMeta::default()
            },
            last_agent_message: Some("final answer".into()),
            duration_ms: Some(125_000),
            value_json: None,
        };

        assert_eq!(
            watch_turn_completed_text(&completion, true).as_deref(),
            Some("final answer\n\ncost: 2m 5s")
        );
    }

    #[tokio::test]
    async fn history_watch_update_emits_core_event_for_external_provider_session() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "external-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        core.handle_history_watch_update(codex_watch_update(
            &session_path,
            "external-thread",
            project_path.to_str().expect("utf8 project"),
            "assistant received",
        ))
        .expect("ingest watch update");

        let (workspace_id, turn_id, text) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    workspace_id,
                    turn_id,
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                } = event
                {
                    break (workspace_id, turn_id, text);
                }
            }
        })
        .await
        .expect("assistant timeline event");
        assert_eq!(turn_id, None);
        assert_eq!(text.as_str(), "assistant received\n\ncost: 2m 5s");
        let workspace = core
            .workspace_binding(&workspace_id)
            .expect("workspace binding");
        assert_eq!(workspace.project_path, project_path);
        assert_eq!(
            core.active_provider_session_ref(&workspace_id).unwrap(),
            "external-thread"
        );
    }

    #[tokio::test]
    async fn history_watch_attachment_projects_to_core_event() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "attachment-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();
        let png = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);

        core.handle_history_watch_update(WatchUpdate {
            provider: watch_provider("codex"),
            path: session_path,
            session_id: Some("attachment-thread".into()),
            cwd: Some(project_path.to_str().expect("utf8 project").into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::Attachment(WatchAttachment {
                meta: WatchEventMeta {
                    id: Some("ig_watch".into()),
                    timestamp: Some("2026-05-07T10:00:00.000Z".into()),
                    ..WatchEventMeta::default()
                },
                id: Some("ig_watch".into()),
                filename: "codex-image-ig_watch.png".into(),
                media_type: "image/png".into(),
                data_base64: png.into(),
                caption: Some("watch caption".into()),
            })]
            .into_boxed_slice(),
            error: None,
        })
        .expect("ingest attachment watch update");

        let attachment = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event: AgentEvent::Attachment(attachment),
                    ..
                } = event
                {
                    break attachment;
                }
            }
        })
        .await
        .expect("attachment event");

        assert_eq!(attachment.id.as_str(), "ig_watch");
        assert_eq!(attachment.filename.as_str(), "codex-image-ig_watch.png");
        assert_eq!(attachment.media_type.as_str(), "image/png");
        assert_eq!(attachment.caption.as_deref(), Some("watch caption"));
    }

    #[tokio::test]
    async fn history_watch_records_recent_observed_session_title_and_activity() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "observed-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "initial prompt",
        );
        append_codex_user_prompt(
            &session_path,
            "2026-04-25T00:01:00.000Z",
            "fix checkout bug\non status page",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        core.handle_history_watch_update(codex_user_watch_update(
            &session_path,
            "observed-thread",
            project_path.to_str().expect("utf8 project"),
            "fix checkout bug\non status page",
            "2026-04-25T00:01:00.000Z",
        ))
        .expect("ingest prompt watch update");

        let observed = core.observed_recent_sessions();
        assert_eq!(observed.len(), 1);
        let entry = &observed[0];
        assert_eq!(entry.title.as_str(), "fix checkout bug on status page");
        assert_eq!(entry.provider_id, "codex");
        assert_eq!(entry.native_resume_ref.as_str(), "observed-thread");
        assert_eq!(entry.provider_session_id.as_str(), "codex:observed-thread");
        assert_eq!(entry.cwd.as_deref(), Some(project_path.as_path()));
        assert_eq!(entry.session_path, session_path);
        let initial_unix = chrono::DateTime::parse_from_rfc3339("2026-04-25T00:01:00.000Z")
            .expect("timestamp")
            .timestamp();
        assert_eq!(entry.last_active_unix, initial_unix);
        assert_eq!(
            entry.last_active_display,
            crate::time_display::format_last_active_display(initial_unix)
        );

        let snapshot = core
            .agent_resource_snapshot(AgentResourceScope::All)
            .await
            .expect("resource snapshot");
        assert_eq!(snapshot.managed_agent_count, 0);
        assert_eq!(snapshot.observed_sessions.len(), 1);
        assert_eq!(
            snapshot.observed_sessions[0].title.as_str(),
            "fix checkout bug on status page"
        );

        core.handle_history_watch_update(codex_watch_update(
            &session_path,
            "observed-thread",
            project_path.to_str().expect("utf8 project"),
            "assistant received",
        ))
        .expect("ingest completion watch update");

        let observed = core.observed_recent_sessions();
        assert_eq!(observed.len(), 1);
        assert_eq!(
            observed[0].title.as_str(),
            "fix checkout bug on status page"
        );
        let completion_unix = chrono::DateTime::parse_from_rfc3339("2026-04-25T00:01:01.000Z")
            .expect("timestamp")
            .timestamp();
        assert_eq!(
            observed[0].last_active_display,
            crate::time_display::format_last_active_display(completion_unix)
        );
    }

    #[tokio::test]
    async fn history_watch_without_prompt_event_uses_workspace_title() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "observed-scan-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "first request",
        );
        append_codex_user_prompt(
            &session_path,
            "2026-04-25T00:01:30.000Z",
            "nearest user request\nwins",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        core.handle_history_watch_update(codex_watch_update(
            &session_path,
            "observed-scan-thread",
            project_path.to_str().expect("utf8 project"),
            "assistant received",
        ))
        .expect("ingest completion watch update");

        let observed = core.observed_recent_sessions();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].title.as_str(), "codex - project");
    }

    #[tokio::test]
    async fn history_watch_without_prompt_event_uses_watch_metadata_title() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "observed-title-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "first request",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        let mut update = codex_watch_update(
            &session_path,
            "observed-title-thread",
            project_path.to_str().expect("utf8 project"),
            "assistant received",
        );
        update.title = Some("first request".into());
        core.handle_history_watch_update(update)
            .expect("ingest completion watch update");

        let observed = core.observed_recent_sessions();
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].title.as_str(), "first request");
    }

    #[tokio::test]
    async fn observed_recent_sessions_drops_entries_with_dead_observed_pid() {
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let live = observed_session_for_test("live observed", Some(std::process::id() as i32));
        let dead = observed_session_for_test("dead observed", Some(i32::MAX));
        let dead_provider_session_id = dead.provider_session_id.clone();
        {
            let mut observed = core
                .observed_sessions
                .write()
                .expect("observed session registry lock");
            observed.insert(live.provider_session_id.clone(), live);
            observed.insert(dead_provider_session_id.clone(), dead);
        }

        let observed = core.observed_recent_sessions();

        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].title.as_str(), "live observed");
        assert!(!core
            .observed_sessions
            .read()
            .expect("observed session registry lock")
            .contains_key(&dead_provider_session_id));
    }

    #[tokio::test]
    async fn submitted_turn_id_is_attached_to_runtime_timeline_events() {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CompletingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/turn-correlation")),
                title: "turn correlation".into(),
            })
            .await
            .expect("open workspace");
        let mut events = core.watch_events();

        let first = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: opened.workspace.workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: crate::agent_runtime::AgentInput {
                    text: "first".into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .expect("submit first");
        let second = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: opened.workspace.workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: crate::agent_runtime::AgentInput {
                    text: "second".into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .expect("submit second");

        let mut seen = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), async {
            while seen.len() < 2 {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    turn_id,
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    seen.push((turn_id.expect("runtime event turn id"), text.to_string()));
                }
            }
        })
        .await
        .expect("assistant timeline events");

        assert_eq!(
            seen,
            vec![
                (first.turn_id, "reply: first".into()),
                (second.turn_id, "reply: second".into())
            ]
        );
    }

    #[tokio::test]
    async fn submit_turns_wait_for_prior_turn_completion_per_workspace() {
        let state = Arc::new(BlockingSubmitState::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(BlockingSubmitProvider {
            state: Arc::clone(&state),
        }));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/submitted-turn-fifo")),
                title: "submitted turn fifo".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();

        let first_core = Arc::clone(&core);
        let first_workspace = workspace_id.clone();
        let first = tokio::spawn(async move {
            first_core
                .submit_turn(SubmitTurnRequest {
                    workspace_id: first_workspace,
                    source: TurnSource::UserMessage,
                    input: crate::agent_runtime::AgentInput {
                        text: "first".into(),
                        images: Vec::new(),
                    },
                    reply_to_channel_message_id: None,
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), state.first_submitted.notified())
            .await
            .expect("first submit should reach provider");

        let second_core = Arc::clone(&core);
        let second_workspace = workspace_id.clone();
        let second = tokio::spawn(async move {
            second_core
                .submit_turn(SubmitTurnRequest {
                    workspace_id: second_workspace,
                    source: TurnSource::UserMessage,
                    input: crate::agent_runtime::AgentInput {
                        text: "second".into(),
                        images: Vec::new(),
                    },
                    reply_to_channel_message_id: None,
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            state.submitted_inputs(),
            vec!["first".to_string()],
            "a newer inbound submit must not reach the provider before the older turn completes"
        );

        state.release_first.notify_one();
        let first = tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("first submit task should finish")
            .expect("first submit task")
            .expect("first submit");
        let second = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("second submit task should finish after first completes")
            .expect("second submit task")
            .expect("second submit");

        assert_ne!(first.turn_id, second.turn_id);
        assert_eq!(
            state.submitted_inputs(),
            vec!["first".to_string(), "second".to_string()]
        );
    }

    #[tokio::test]
    async fn register_subagent_callback_rejects_cross_workspace_link() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let workspace_a = WorkspaceId::new("workspace-subagent-a");
        let workspace_b = WorkspaceId::new("workspace-subagent-b");
        for workspace_id in [workspace_a.clone(), workspace_b.clone()] {
            core.upsert_workspace_binding(
                workspace_id.clone(),
                OpenWorkspaceRequest {
                    provider_id: "codex",
                    project_path: Some(temp.path().join(workspace_id.as_str())),
                    title: workspace_id.as_str().into(),
                },
                Some(workspace_id.as_str()),
            )
            .expect("workspace binding");
        }
        let link_id = SubAgentLinkId::new("link-cross-workspace");
        core.upsert_subagent_link(SubAgentLinkRecord::new_non_openable(
            link_id.clone(),
            workspace_a,
            crate::control_plane::SubAgentActionId::new("action-cross-workspace"),
            TurnId::new("turn-cross-workspace"),
            provider_session_id("codex", "workspace-subagent-a"),
            "child",
        ))
        .expect("subagent link");

        assert!(core
            .register_subagent_callback(workspace_b, link_id)
            .expect("register callback")
            .is_none());
    }

    #[tokio::test]
    async fn fork_projection_preserves_cold_provider_session_status_after_reload() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            Arc::clone(&runtime),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let source_workspace_id = WorkspaceId::new("workspace-fork-source");
        core.upsert_workspace_binding(
            source_workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "fork source".into(),
            },
            Some("thread-fork-source"),
        )
        .expect("source workspace");
        core.record_provider_status(
            &source_workspace_id,
            &AgentStatus {
                model: Some("gpt-5".into()),
                ..Default::default()
            },
        )
        .expect("provider status");
        drop(core);

        let reloaded = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reloaded core");
        reloaded
            .record_fork_workspace_projection(
                source_workspace_id,
                WorkspaceId::new("workspace-fork-child"),
                "fork child".into(),
                "codex",
                Some("thread-fork-source"),
                None,
            )
            .expect("fork projection");

        assert_eq!(
            reloaded
                .provider_session_record(&provider_session_id("codex", "thread-fork-source"))
                .expect("provider session")
                .model
                .as_deref(),
            Some("gpt-5")
        );
    }

    #[tokio::test]
    async fn cold_upserts_preserve_existing_metadata_after_lazy_reload() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let store = ControlPlaneSqliteStore::open(&db).expect("store");
        let workspace_id = WorkspaceId::new("workspace-cold-preserve");
        let mut task = ScheduledTaskRecord::new(
            ScheduledTaskId::new("task-cold-preserve"),
            workspace_id.clone(),
            "codex",
            temp.path().join("project"),
            "task before reload",
            "prompt before reload",
            10,
        );
        task.last_run_unix_ms = Some(77);
        let task_created_at = task.created_at;
        store
            .upsert_entity_state(
                "scheduled_task",
                task.task_id.as_str(),
                Some(task.workspace_id.as_str()),
                &task,
            )
            .expect("seed task");
        let binding = ChannelBinding::new(
            ChannelBindingId::new("binding-cold-preserve"),
            workspace_id.clone(),
            "telegram",
            "100",
            Some("9"),
        );
        let binding_created_at = binding.created_at;
        store
            .upsert_entity_state(
                "channel_binding",
                binding.channel_binding_id.as_str(),
                Some(binding.workspace_id.as_str()),
                &binding,
            )
            .expect("seed binding");
        let mut panel = PanelRenderRecord::new(
            PanelRenderId::new("panel-cold-preserve"),
            "telegram",
            "100",
            None::<&str>,
            Revision::new(1),
        );
        panel.last_observed_stale_revision = Some(Revision::new(9));
        let panel_created_at = panel.created_at;
        store
            .upsert_entity_state("panel_render", panel.panel_id.as_str(), None, &panel)
            .expect("seed panel");
        let mut link = SubAgentLinkRecord::new_non_openable(
            SubAgentLinkId::new("link-cold-preserve"),
            workspace_id.clone(),
            crate::control_plane::SubAgentActionId::new("action-cold-preserve"),
            TurnId::new("turn-cold-preserve"),
            provider_session_id("codex", "thread-cold-preserve"),
            "child",
        );
        link.revision = Revision::new(7);
        let link_created_at = link.created_at;
        store
            .upsert_entity_state(
                "subagent_link",
                link.link_id.as_str(),
                Some(link.workspace_id.as_str()),
                &link,
            )
            .expect("seed link");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(runtime, store).expect("core");

        let updated_task = core
            .upsert_scheduled_task(UpsertScheduledTaskRequest {
                task_id: ScheduledTaskId::new("task-cold-preserve"),
                workspace_id: workspace_id.clone(),
                provider_id: "codex",
                project_path: temp.path().join("project"),
                title: "task after reload".into(),
                prompt: "prompt after reload".into(),
                next_run_unix_ms: 20,
                enabled: true,
            })
            .expect("upsert task");
        assert_eq!(updated_task.last_run_unix_ms, Some(77));
        assert_eq!(updated_task.created_at, task_created_at);

        core.upsert_channel_binding(ChannelBinding::new(
            ChannelBindingId::new("binding-cold-preserve"),
            workspace_id.clone(),
            "telegram",
            "100",
            Some("10"),
        ))
        .expect("upsert binding");
        assert_eq!(
            core.channel_binding(&ChannelBindingId::new("binding-cold-preserve"))
                .expect("binding")
                .created_at,
            binding_created_at
        );

        core.upsert_panel_render(PanelRenderRecord::new(
            PanelRenderId::new("panel-cold-preserve"),
            "telegram",
            "100",
            None::<&str>,
            Revision::new(2),
        ))
        .expect("upsert panel");
        let updated_panel = core
            .panel_render(&PanelRenderId::new("panel-cold-preserve"))
            .expect("panel");
        assert_eq!(
            updated_panel.last_observed_stale_revision,
            Some(Revision::new(9))
        );
        assert_eq!(updated_panel.created_at, panel_created_at);

        let updated_link = core
            .upsert_subagent_link(SubAgentLinkRecord::new_non_openable(
                SubAgentLinkId::new("link-cold-preserve"),
                workspace_id,
                crate::control_plane::SubAgentActionId::new("action-cold-preserve"),
                TurnId::new("turn-cold-preserve"),
                provider_session_id("codex", "thread-cold-preserve"),
                "child updated",
            ))
            .expect("upsert subagent link");
        assert_eq!(updated_link.revision, Revision::new(8));
        assert_eq!(updated_link.created_at, link_created_at);
    }

    #[tokio::test]
    async fn remove_workspace_projection_clears_runtime_live_session() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RequestRecordingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-remove-runtime");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "remove runtime".into(),
            },
            Some("thread-remove-runtime"),
        )
        .expect("workspace binding");
        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("resume workspace");
        assert!(core.has_live_session(&workspace_id));

        core.remove_workspace_projection(&workspace_id)
            .await
            .expect("remove workspace");

        assert!(!core.has_live_session(&workspace_id));
    }

    #[tokio::test]
    async fn remove_workspace_projection_deletes_orphan_provider_session_and_message_bindings() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-remove-orphan");
        let provider_session_id = provider_session_id("codex", "thread-remove-orphan");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "remove orphan".into(),
            },
            Some("thread-remove-orphan"),
        )
        .expect("workspace binding");
        core.bind_message_to_provider_session(
            "telegram",
            "100",
            "200",
            provider_session_id.clone(),
        )
        .expect("message session binding");

        core.remove_workspace_projection(&workspace_id)
            .await
            .expect("remove workspace");

        assert!(core.workspace_binding(&workspace_id).is_none());
        assert!(core.provider_session_record(&provider_session_id).is_none());
        assert!(core
            .message_session_binding("telegram", "100", "200")
            .is_none());
    }

    #[tokio::test]
    async fn remove_workspace_projection_keeps_shared_provider_session() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let provider_session_id = provider_session_id("codex", "thread-shared");
        for workspace in ["workspace-shared-a", "workspace-shared-b"] {
            core.upsert_workspace_binding(
                WorkspaceId::new(workspace),
                OpenWorkspaceRequest {
                    provider_id: "codex",
                    project_path: Some(temp.path().join(workspace)),
                    title: workspace.into(),
                },
                Some("thread-shared"),
            )
            .expect("workspace binding");
        }

        core.remove_workspace_projection(&WorkspaceId::new("workspace-shared-a"))
            .await
            .expect("remove workspace");

        assert!(core
            .workspace_binding(&WorkspaceId::new("workspace-shared-a"))
            .is_none());
        assert!(core
            .workspace_binding(&WorkspaceId::new("workspace-shared-b"))
            .is_some());
        assert!(core.provider_session_record(&provider_session_id).is_some());
    }

    #[tokio::test]
    async fn record_reconcile_outcomes_batch_updates_cold_workspaces() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        for id in ["workspace-batch-a", "workspace-batch-b"] {
            core.upsert_workspace_binding(
                WorkspaceId::new(id),
                OpenWorkspaceRequest {
                    provider_id: "codex",
                    project_path: Some(temp.path().join(id)),
                    title: id.into(),
                },
                Some(id),
            )
            .expect("workspace binding");
        }
        drop(core);
        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        assert!(reloaded
            .state
            .read()
            .expect("control plane lock")
            .workspace_bindings()
            .is_empty());

        reloaded
            .record_reconcile_outcomes_batch(vec![
                (WorkspaceId::new("workspace-batch-a"), ReconcileOutcome::Ok),
                (
                    WorkspaceId::new("workspace-batch-b"),
                    ReconcileOutcome::StaleRevision,
                ),
            ])
            .expect("record batch");

        assert_eq!(
            reloaded
                .status_snapshot(&WorkspaceId::new("workspace-batch-b"))
                .expect("status")
                .last_reconcile_outcome,
            Some(ReconcileOutcome::StaleRevision)
        );
    }

    #[tokio::test]
    async fn record_reconcile_outcomes_batch_rejects_missing_without_partial_write() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-batch-present");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "present".into(),
            },
            Some("thread-present"),
        )
        .expect("workspace binding");

        let err = core
            .record_reconcile_outcomes_batch(vec![
                (workspace_id.clone(), ReconcileOutcome::StaleRevision),
                (
                    WorkspaceId::new("workspace-batch-missing"),
                    ReconcileOutcome::Ok,
                ),
            ])
            .expect_err("missing workspace rejects batch");
        assert!(err.to_string().contains("MissingWorkspace"));
        assert_eq!(
            core.status_snapshot(&workspace_id)
                .expect("status")
                .last_reconcile_outcome,
            None
        );
    }

    #[tokio::test]
    async fn workspace_rebind_preserves_provider_session_status() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(temp.path().join("state.sqlite3")).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-provider-status");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "provider status".into(),
            },
            Some("thread-provider-status"),
        )
        .expect("workspace binding");
        core.record_provider_status(
            &workspace_id,
            &AgentStatus {
                model: Some("gpt-5".into()),
                ..Default::default()
            },
        )
        .expect("record status");

        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "provider status renamed".into(),
            },
            Some("thread-provider-status"),
        )
        .expect("workspace rebind");

        assert_eq!(
            core.provider_session_record(&provider_session_id("codex", "thread-provider-status"))
                .expect("provider session")
                .model
                .as_deref(),
            Some("gpt-5")
        );
    }

    #[tokio::test]
    async fn restart_cleanup_clears_cold_workspace_active_live_instance() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-restart-cleanup");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "restart cleanup".into(),
            },
            Some("thread-restart-cleanup"),
        )
        .expect("workspace binding");
        let live_instance_id = live_instance_id("live-restart-cleanup");
        core.attach_live_session_projection(
            workspace_id.clone(),
            "codex",
            "thread-restart-cleanup",
            &live_instance_id,
        )
        .expect("live projection");
        core.start_turn(
            workspace_id.clone(),
            provider_session_id("codex", "thread-restart-cleanup"),
            live_instance_id.clone(),
            TurnSource::UserMessage,
            "hello",
            None,
        )
        .expect("running turn");
        drop(core);

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        assert!(
            reloaded
                .state
                .read()
                .expect("control plane lock")
                .get_workspace(&workspace_id)
                .is_none(),
            "workspace rows must stay cold after reload"
        );
        assert_eq!(
            reloaded
                .workspace_binding(&workspace_id)
                .expect("workspace before cleanup")
                .active_live_instance_id,
            Some(live_instance_id.clone())
        );

        reloaded
            .mark_live_instances_stale_after_restart("test restart")
            .expect("restart cleanup");

        let workspace = reloaded
            .workspace_binding(&workspace_id)
            .expect("workspace after cleanup");
        assert_eq!(workspace.active_live_instance_id, None);
        let live = reloaded
            .live_instance_record(&live_instance_id)
            .expect("live instance");
        assert_eq!(live.state, LiveInstanceState::Stale);
        assert_eq!(
            reloaded
                .status_snapshot(&workspace_id)
                .expect("status")
                .last_reconcile_outcome,
            Some(ReconcileOutcome::LiveInstanceStale)
        );
    }

    #[tokio::test]
    async fn restart_cleanup_clears_missing_active_live_without_live_row() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-missing-live");
        let provider_session_id = provider_session_id("codex", "thread-missing-live");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "missing live".into(),
            },
            Some("thread-missing-live"),
        )
        .expect("workspace binding");
        let mut workspace = core
            .workspace_binding(&workspace_id)
            .expect("workspace before patch");
        workspace.active_provider_session_id = Some(provider_session_id);
        workspace.active_live_instance_id = Some(live_instance_id("missing-live-row"));
        core.control_plane_store()
            .upsert_entities(ControlPlaneState::persistence_entities_for_workspace(
                &workspace,
            ))
            .expect("persist patched workspace");
        drop(core);

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        reloaded
            .mark_live_instances_stale_after_restart("test restart")
            .expect("restart cleanup");

        assert_eq!(
            reloaded
                .workspace_binding(&workspace_id)
                .expect("workspace after cleanup")
                .active_live_instance_id,
            None
        );
        assert_eq!(
            reloaded
                .status_snapshot(&workspace_id)
                .expect("status")
                .last_reconcile_outcome,
            Some(ReconcileOutcome::LiveInstanceStale)
        );
    }

    #[tokio::test]
    async fn restart_cleanup_clears_terminal_active_live() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-terminal-live");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "terminal live".into(),
            },
            Some("thread-terminal-live"),
        )
        .expect("workspace binding");
        let live_instance_id = live_instance_id("live-terminal");
        core.attach_live_session_projection(
            workspace_id.clone(),
            "codex",
            "thread-terminal-live",
            &live_instance_id,
        )
        .expect("live projection");
        let mut live = core
            .live_instance_record(&live_instance_id)
            .expect("live before terminal patch");
        live.state = LiveInstanceState::Closed;
        core.control_plane_store()
            .upsert_entities(ControlPlaneState::persistence_entities_for_live_instance(
                &live,
                Some(&workspace_id),
            ))
            .expect("persist terminal live");
        drop(core);

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        reloaded
            .mark_live_instances_stale_after_restart("test restart")
            .expect("restart cleanup");

        assert_eq!(
            reloaded
                .workspace_binding(&workspace_id)
                .expect("workspace after cleanup")
                .active_live_instance_id,
            None
        );
        assert_eq!(
            reloaded
                .live_instance_record(&live_instance_id)
                .expect("live after cleanup")
                .state,
            LiveInstanceState::Closed
        );
    }

    #[tokio::test]
    async fn complete_turn_preserves_live_instance_store_owner_workspace() {
        let store = ControlPlaneSqliteStore::open_in_memory().expect("store");
        let core =
            LucarneCore::from_runtime_and_store(Arc::new(AgentRuntime::new()), store.clone())
                .expect("core");
        let source_workspace_id = WorkspaceId::new("workspace-source");
        let live_workspace_id = WorkspaceId::new("workspace-live-owner");
        let provider_session_id = provider_session_id("codex", "thread-owner");
        let live_instance_id = live_instance_id("live-owner");
        let turn_id = TurnId::new("turn-owner");
        let mut source_workspace = WorkspaceBinding::new(
            source_workspace_id.clone(),
            "source",
            "codex",
            "/tmp/source",
        );
        source_workspace.active_provider_session_id = Some(provider_session_id.clone());
        let mut live_workspace = WorkspaceBinding::new(
            live_workspace_id.clone(),
            "live owner",
            "codex",
            "/tmp/live-owner",
        );
        live_workspace.active_provider_session_id = Some(provider_session_id.clone());
        live_workspace.active_live_instance_id = Some(live_instance_id.clone());
        let provider_session =
            ProviderSessionRecord::new(provider_session_id.clone(), "codex", "thread-owner");
        let mut live = LiveInstanceRecord::new(
            live_instance_id.clone(),
            "codex",
            provider_session_id.clone(),
            Option::<String>::None,
        );
        live.state = LiveInstanceState::Running;
        live.active_turn_id = Some(turn_id.clone());
        let turn = TurnRecord {
            turn_id: turn_id.clone(),
            workspace_id: source_workspace_id.clone(),
            provider_session_id,
            live_instance_id: live_instance_id.clone(),
            source: TurnSource::UserMessage,
            input: "hello".into(),
            reply_to_channel_message_id: None,
            state: TurnState::Running,
            timeline_seq_start: None,
            timeline_seq_end: None,
            usage: serde_json::Value::Null,
            created_at: SystemTime::now(),
            completed_at: None,
        };
        store
            .upsert_entities(
                [
                    ControlPlaneState::persistence_entities_for_workspace(&source_workspace),
                    ControlPlaneState::persistence_entities_for_workspace(&live_workspace),
                    ControlPlaneState::persistence_entities_for_provider_session(&provider_session),
                    ControlPlaneState::persistence_entities_for_live_instance(
                        &live,
                        Some(&live_workspace_id),
                    ),
                    ControlPlaneState::persistence_entities_for_turn_record(&turn),
                ]
                .concat(),
            )
            .expect("seed cold rows");

        core.complete_turn_with_usage_value(turn_id, None)
            .expect("complete turn");

        assert_eq!(
            store
                .workspace_id_for_live_instance(&live_instance_id)
                .expect("live owner lookup"),
            Some(live_workspace_id)
        );
    }

    #[tokio::test]
    async fn clear_workspace_records_removes_persisted_workspace_state() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-clear");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(temp.path().join("project")),
                title: "clear me".into(),
            },
            Some("thread-clear"),
        )
        .expect("workspace binding");
        let provider_session_id = provider_session_id("codex", "thread-clear");
        let live_instance_id = live_instance_id("live-clear");
        core.attach_live_session_projection(
            workspace_id.clone(),
            "codex",
            "thread-clear",
            &live_instance_id,
        )
        .expect("live projection");
        let turn = core
            .start_turn(
                workspace_id.clone(),
                provider_session_id,
                live_instance_id,
                TurnSource::UserMessage,
                "hello",
                None,
            )
            .expect("turn");
        core.append_timeline(TimelineItem::new(
            workspace_id.clone(),
            turn.turn_id,
            TimelineItemKind::Assistant,
            "reply",
        ))
        .expect("timeline");
        let workspace_channel_binding_id = ChannelBindingId::new("telegram:100:9");
        core.upsert_channel_binding(ChannelBinding::new(
            workspace_channel_binding_id.clone(),
            workspace_id.clone(),
            "telegram",
            "100",
            Some("9"),
        ))
        .expect("workspace channel binding");
        let service_channel_binding_id = ChannelBindingId::new("telegram:100:agent-notifications");
        core.upsert_channel_binding(ChannelBinding::new(
            service_channel_binding_id.clone(),
            WorkspaceId::new("telegram:agent-notifications"),
            "telegram",
            "100",
            Some("77"),
        ))
        .expect("service channel binding");

        assert_eq!(core.clear_workspace_records().expect("clear"), 1);
        assert!(core.workspace_bindings().is_empty());
        assert!(core.timeline_kinds(&workspace_id).is_empty());
        assert!(core
            .channel_binding(&workspace_channel_binding_id)
            .is_none());
        assert_eq!(
            core.channel_binding(&service_channel_binding_id)
                .expect("service binding survives workspace cleanup")
                .topic_id
                .as_deref(),
            Some("77")
        );

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        assert!(reloaded.workspace_bindings().is_empty());
        assert!(reloaded.timeline_kinds(&workspace_id).is_empty());
        assert!(reloaded
            .channel_binding(&workspace_channel_binding_id)
            .is_none());
        assert_eq!(
            reloaded
                .channel_binding(&service_channel_binding_id)
                .expect("persisted service binding")
                .topic_id
                .as_deref(),
            Some("77")
        );
    }

    #[tokio::test]
    async fn default_history_session_watch_emits_core_event_for_codex_append() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let _env = EnvGuard::set(&[
            ("HOME", temp.path().as_os_str().to_os_string()),
            ("PATH", temp.path().as_os_str().to_os_string()),
            ("CODEX_HOME", codex_home.as_os_str().to_os_string()),
            (
                "CLAUDE_CONFIG_DIR",
                temp.path().join("claude").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_HOME",
                temp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_CONFIG_DIR",
                temp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "COPILOT_HOME",
                temp.path().join("copilot").as_os_str().to_os_string(),
            ),
        ]);
        let session_path = write_codex_history_session(
            &codex_home,
            "default-watch-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        core.start_history_session_watch()
            .expect("start default history watch");
        append_codex_assistant_response(
            &session_path,
            "2026-04-25T00:01:01.000Z",
            "default watch assistant received",
        );

        let text = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await
        .expect("assistant timeline event");
        assert_eq!(
            text.as_str(),
            "default watch assistant received\n\ncost: 2m 5s"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn history_session_watch_emits_core_event_for_existing_recent_non_hot_codex_append() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("recent-non-hot-project");
        fs::create_dir_all(&project_path).expect("project dir");
        let session_path = write_codex_history_session(
            &codex_home,
            "recent-non-hot-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );

        tokio::time::sleep(Duration::from_millis(25)).await;

        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        core.start_history_session_watch_with_config(
            WatchConfig::new()
                .providers([watch_provider("codex")])
                .provider_roots(watch_provider("codex"), [codex_home.clone()])
                .selection(history_watch_selection())
                .debounce(Duration::from_millis(10))
                .recent_window(Duration::from_secs(60))
                .hot_file_window(Duration::from_nanos(1)),
        )
        .expect("start codex history watch");
        append_codex_assistant_response(
            &session_path,
            "2026-04-25T00:01:01.000Z",
            "recent non-hot assistant received",
        );

        let text = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await
        .expect("assistant timeline event");
        assert_eq!(
            text.as_str(),
            "recent non-hot assistant received\n\ncost: 2m 5s"
        );
        let observed_recent = core.observed_recent_sessions();
        assert!(
            observed_recent.iter().any(|session| {
                session.provider_id == "codex"
                    && session.native_resume_ref.as_str() == "recent-non-hot-thread"
            }),
            "history watch append should also populate observed_recent_sessions"
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn history_session_watch_emits_core_event_for_existing_stale_codex_append_from_recursive_root(
    ) {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("stale-project");
        fs::create_dir_all(&project_path).expect("project dir");
        let session_path = write_codex_history_session(
            &codex_home,
            "stale-reactivated-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );

        tokio::time::sleep(Duration::from_millis(25)).await;

        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        core.start_history_session_watch_with_config(
            WatchConfig::new()
                .providers([watch_provider("codex")])
                .provider_roots(watch_provider("codex"), [codex_home.clone()])
                .selection(history_watch_selection())
                .debounce(Duration::from_millis(10))
                .recent_window(Duration::from_nanos(1))
                .hot_file_window(Duration::from_nanos(1)),
        )
        .expect("start codex history watch");
        append_codex_assistant_response(
            &session_path,
            "2026-04-25T00:01:01.000Z",
            "stale reactivated assistant received",
        );

        let text = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await
        .expect("assistant timeline event");
        assert_eq!(
            text.as_str(),
            "stale reactivated assistant received\n\ncost: 2m 5s"
        );
        let observed_recent = core.observed_recent_sessions();
        assert!(
            observed_recent.iter().any(|session| {
                session.provider_id == "codex"
                    && session.native_resume_ref.as_str() == "stale-reactivated-thread"
            }),
            "recursive root watch should promote a stale session after it becomes active"
        );
    }

    #[tokio::test]
    async fn history_session_watch_emits_core_event_for_new_codex_session_append_after_created_baseline(
    ) {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("new-project");
        fs::create_dir_all(&project_path).expect("project dir");
        let day_dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&day_dir).expect("codex day dir");
        let _env = EnvGuard::set(&[
            ("HOME", temp.path().as_os_str().to_os_string()),
            ("PATH", temp.path().as_os_str().to_os_string()),
            ("CODEX_HOME", codex_home.as_os_str().to_os_string()),
            (
                "CLAUDE_CONFIG_DIR",
                temp.path().join("claude").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_HOME",
                temp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "GEMINI_CONFIG_DIR",
                temp.path().join("gemini").as_os_str().to_os_string(),
            ),
            (
                "COPILOT_HOME",
                temp.path().join("copilot").as_os_str().to_os_string(),
            ),
        ]);
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();

        let mut watcher = SessionWatcher::start(
            WatchConfig::new()
                .providers([watch_provider("codex")])
                .provider_roots(watch_provider("codex"), [codex_home.clone()])
                .debounce(Duration::from_millis(10))
                .scan_interval(Duration::from_millis(25)),
        )
        .expect("start codex watcher");
        let session_path = write_codex_history_session(
            &codex_home,
            "new-watch-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let created =
            next_watch_updates_matching(&mut watcher, "new-watch-thread", WatchChange::Created)
                .await;
        assert!(
            created.iter().all(|update| update.events.is_empty()),
            "newly discovered files only establish a baseline"
        );
        core.handle_history_watch_updates(created);

        append_codex_assistant_response(
            &session_path,
            "2026-04-25T00:01:01.000Z",
            "new watch assistant received",
        );
        let updated =
            next_watch_updates_matching(&mut watcher, "new-watch-thread", WatchChange::Updated)
                .await;
        core.handle_history_watch_updates(updated);

        let text = tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await
        .expect("assistant timeline event");
        assert_eq!(text.as_str(), "new watch assistant received\n\ncost: 2m 5s");
    }

    async fn next_watch_updates_matching(
        watcher: &mut SessionWatcher,
        session_id: &str,
        change: WatchChange,
    ) -> Box<[WatchUpdate]> {
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let updates = watcher
                    .next()
                    .await
                    .expect("watch stream update")
                    .expect("watch update");
                if updates.iter().any(|update| {
                    update.session_id.as_deref() == Some(session_id) && update.change == change
                }) {
                    break updates;
                }
            }
        })
        .await
        .expect("matching watch update")
    }

    #[tokio::test]
    async fn history_watch_update_with_missing_metadata_is_ignored() {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let mut events = core.watch_events();
        let session_path = PathBuf::from("/tmp/missing-metadata-session.jsonl");
        let mut update = codex_watch_update(
            &session_path,
            "missing-thread",
            "/tmp/missing-project",
            "ignored assistant output",
        );
        update.session_id = None;
        update.cwd = None;

        core.handle_history_watch_updates(vec![update].into_boxed_slice());

        assert!(
            tokio::time::timeout(Duration::from_millis(50), events.recv())
                .await
                .is_err(),
            "malformed watch updates without provider metadata must be ignored"
        );
    }

    #[tokio::test]
    async fn history_watch_update_skips_core_owned_live_session() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let _env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            "live-thread",
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RequestRecordingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = workspace_id_for_resume("codex", "live-thread");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(project_path.clone()),
                title: "codex - project".into(),
            },
            Some("live-thread"),
        )
        .expect("workspace binding");
        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("resume workspace");
        submit_noop_turn(&core, &workspace_id).await;
        let mut events = core.watch_events();

        core.handle_history_watch_update(WatchUpdate {
            provider: watch_provider("codex"),
            path: session_path,
            session_id: Some("live-thread".into()),
            cwd: Some(project_path.to_str().expect("utf8 project").into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::AssistantMessage(WatchAssistantMessage {
                meta: WatchEventMeta {
                    timestamp: Some("2026-04-25T00:01:01.000Z".into()),
                    ..WatchEventMeta::default()
                },
                model: None,
                phase: None,
                text: Some("duplicate assistant output".into()),
            })]
            .into_boxed_slice(),
            error: None,
        })
        .expect("ingest watch update");

        assert!(
            tokio::time::timeout(Duration::from_millis(50), events.recv())
                .await
                .is_err(),
            "core-owned live submitted turn history writes must not emit duplicate watch events"
        );
    }

    #[tokio::test]
    async fn history_watch_update_skips_current_submitted_turn_for_any_provider() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        for provider_id in ["codex", "pi"] {
            let session_id = format!("{provider_id}-live-thread");
            let fixture = live_history_fixture_for_provider(provider_id, &session_id).await;
            let submitted = submit_noop_turn(&fixture.core, &fixture.workspace_id).await;
            let before_watch_activity = fixture
                .core
                .submitted_turn_activity
                .read()
                .expect("activity lock")
                .get(&submitted.turn_id)
                .expect("submitted turn activity")
                .last_event_at;
            let mut events = fixture.core.watch_events();
            tokio::time::sleep(Duration::from_millis(5)).await;

            fixture
                .core
                .handle_history_watch_update(provider_watch_update(
                    provider_id,
                    &fixture.session_path,
                    &session_id,
                    fixture.cwd(),
                    "duplicate assistant output",
                    None,
                ))
                .expect("ingest live echo watch update");

            let after_watch_activity = fixture
                .core
                .submitted_turn_activity
                .read()
                .expect("activity lock")
                .get(&submitted.turn_id)
                .expect("submitted turn activity")
                .last_event_at;
            assert_eq!(
                after_watch_activity, before_watch_activity,
                "{provider_id} submitted-turn history echo must not count as watchdog activity"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), async {
                    loop {
                        let event = events.recv().await.expect("core event");
                        if let CoreEvent::TimelineEvent {
                            turn_id: None,
                            event:
                                AgentEvent::Message(crate::agent_runtime::MessageEvent {
                                    role: crate::agent_runtime::MessageRole::Assistant,
                                    streaming: false,
                                    ..
                                }),
                            ..
                        } = event
                        {
                            return;
                        }
                    }
                })
                .await
                .is_err(),
                "{provider_id} submitted-turn history echo must not emit duplicate watch events"
            );
        }
    }

    #[tokio::test]
    async fn runtime_activity_without_public_event_refreshes_submitted_turn() {
        let activity_state = Arc::new(RawActivitySessionState::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RawActivityProvider {
            state: Arc::clone(&activity_state),
        }));
        let core = LucarneCore::from_runtime_and_store_with_options(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
            CoreOptions {
                turn_inactivity: Duration::from_millis(500),
                turn_deadline: Duration::from_millis(1000),
                session_idle_timeout: Duration::from_millis(1000),
            },
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/raw-activity")),
                title: "raw activity".into(),
            })
            .await
            .expect("open workspace");
        let submitted = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: opened.workspace.workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: crate::agent_runtime::AgentInput {
                    text: "please continue".into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .expect("submit turn");
        let before_activity = {
            let mut activity = core.submitted_turn_activity.write().expect("activity lock");
            let turn = activity
                .get_mut(&submitted.turn_id)
                .expect("submitted turn activity");
            turn.waiting_intervention = true;
            turn.last_event_at
        };

        activity_state.emit().await;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let current = core
                    .submitted_turn_activity
                    .read()
                    .expect("activity lock")
                    .get(&submitted.turn_id)
                    .cloned()
                    .expect("submitted turn activity");
                if current.last_event_at > before_activity && !current.waiting_intervention {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("raw runtime activity should refresh submitted-turn activity");
    }

    #[tokio::test]
    async fn history_watch_update_notifies_idle_live_session() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        for provider_id in ["codex", "pi"] {
            let session_id = format!("{provider_id}-external-thread");
            let fixture = live_history_fixture_for_provider(provider_id, &session_id).await;
            let mut events = fixture.core.watch_events();

            fixture
                .core
                .handle_history_watch_update(provider_watch_update(
                    provider_id,
                    &fixture.session_path,
                    &session_id,
                    fixture.cwd(),
                    "external assistant output",
                    None,
                ))
                .expect("ingest external watch update");

            let text = tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let event = events.recv().await.expect("core event");
                    if let CoreEvent::TimelineEvent {
                        turn_id: None,
                        event:
                            AgentEvent::Message(crate::agent_runtime::MessageEvent {
                                role: crate::agent_runtime::MessageRole::Assistant,
                                text,
                                streaming: false,
                            }),
                        ..
                    } = event
                    {
                        break text;
                    }
                }
            })
            .await
            .expect("idle live session external watch event");
            assert_eq!(text.as_str(), "external assistant output\n\ncost: 2m 5s");
        }
    }

    #[tokio::test]
    async fn completed_live_runtime_turn_claim_suppresses_echo_after_live_detach() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CompletingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(project_path.clone()),
                title: "live echo".into(),
            })
            .await
            .expect("open workspace");
        let session_id = opened.workspace.session_id.0.to_string();
        let session_path = write_codex_history_session(
            &codex_home,
            &session_id,
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "please continue",
        );
        let mut events = core.watch_events();
        let submitted = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: opened.workspace.workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: crate::agent_runtime::AgentInput {
                    text: "please continue".into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .expect("submit turn");

        tokio::time::timeout(Duration::from_secs(1), async {
            let mut message = false;
            let mut completed = false;
            while !message || !completed {
                let event = events.recv().await.expect("core event");
                match event {
                    CoreEvent::TimelineEvent {
                        turn_id,
                        event:
                            AgentEvent::Message(crate::agent_runtime::MessageEvent {
                                role: crate::agent_runtime::MessageRole::Assistant,
                                text,
                                streaming: false,
                            }),
                        ..
                    } if turn_id.as_ref() == Some(&submitted.turn_id) => {
                        assert_eq!(text.as_str(), "reply: please continue");
                        message = true;
                    }
                    CoreEvent::TimelineEvent {
                        turn_id,
                        event: AgentEvent::TurnCompleted(_),
                        ..
                    } if turn_id.as_ref() == Some(&submitted.turn_id) => {
                        completed = true;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("runtime turn completed");

        core.live_session_generations
            .write()
            .expect("live session generation lock")
            .remove(&opened.workspace.workspace_id);
        core.live_sessions
            .write()
            .expect("live session registry lock")
            .remove(&opened.workspace.workspace_id);

        let mut background_events = core.watch_events();
        core.handle_history_watch_update(codex_watch_update_with_turn_id(
            &session_path,
            &session_id,
            project_path.to_str().expect("utf8 project"),
            "persisted reply: please continue",
            "provider-turn",
        ))
        .expect("ingest lagging history echo");

        assert!(
            tokio::time::timeout(Duration::from_millis(50), async {
                loop {
                    let event = background_events.recv().await.expect("core event");
                    if let CoreEvent::TimelineEvent {
                        turn_id: None,
                        event:
                            AgentEvent::Message(crate::agent_runtime::MessageEvent {
                                role: crate::agent_runtime::MessageRole::Assistant,
                                streaming: false,
                                ..
                            }),
                        ..
                    } = event
                    {
                        return;
                    }
                }
            })
            .await
            .is_err(),
            "lagging history echo for a completed live runtime turn must not become a background event after the live process is gone"
        );

        let mut external_events = core.watch_events();
        core.handle_history_watch_update(codex_watch_update_with_turn_id(
            &session_path,
            &session_id,
            project_path.to_str().expect("utf8 project"),
            "external assistant output",
            "external-provider-turn",
        ))
        .expect("ingest different provider turn update");

        let text = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = external_events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    turn_id: None,
                    event:
                        AgentEvent::Message(crate::agent_runtime::MessageEvent {
                            role: crate::agent_runtime::MessageRole::Assistant,
                            text,
                            streaming: false,
                        }),
                    ..
                } = event
                {
                    break text;
                }
            }
        })
        .await
        .expect("different provider turn remains visible");
        assert_eq!(text.as_str(), "external assistant output\n\ncost: 2m 5s");
    }

    #[tokio::test]
    async fn history_watch_disconnect_marks_watcher_not_running_for_restart() {
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        core.history_watch_started.store(true, Ordering::SeqCst);
        core.set_history_watch_status(HistoryWatchState::Running, true, None, None);

        let exit = core
            .run_history_watch_loop_for_tests([Err(WatchError::Disconnected)])
            .await;

        assert_eq!(exit, HistoryWatchLoopExit::Disconnected);
        let status = core.history_watch_status();
        assert_eq!(status.state, HistoryWatchState::Backoff);
        assert!(!status.running);
        assert_eq!(status.restart_count, 1);
        assert_eq!(status.last_error.as_deref(), Some("disconnected"));
        assert!(status.next_retry_at_unix_ms.is_some());
        assert!(
            core.history_watch_started.load(Ordering::SeqCst),
            "the history watch supervisor stays started so it can recreate the watcher"
        );
    }

    #[tokio::test]
    async fn history_watch_supervisor_recreates_watcher_after_backoff() {
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let _events = core.watch_events();

        core.start_history_session_watch_with_config(WatchConfig::new())
            .expect("start history watch supervisor");

        tokio::time::timeout(Duration::from_millis(1500), async {
            loop {
                if core.history_watch_start_count() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("history watch supervisor should retry after backoff");
        let status = core.history_watch_status();
        assert!(status.restart_count >= 1);
        assert!(!status.running);
        assert!(matches!(
            status.state,
            HistoryWatchState::Backoff | HistoryWatchState::Degraded
        ));
    }

    #[tokio::test]
    async fn history_watch_requires_core_event_subscriber() {
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        let err = core
            .start_history_session_watch_with_config(WatchConfig::new())
            .expect_err("history watch without subscribers must fail closed");

        assert!(err.to_string().contains("requires a core event subscriber"));
    }

    #[tokio::test]
    async fn history_terminal_update_does_not_complete_live_submitted_turn() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let fixture = live_history_fixture("live-thread").await;
        let submitted = submit_noop_turn(&fixture.core, &fixture.workspace_id).await;
        let mut events = fixture.core.watch_events();
        fixture
            .core
            .handle_history_watch_update(codex_watch_update(
                &fixture.session_path,
                "live-thread",
                fixture.cwd(),
                "assistant received",
            ))
            .expect("ingest watch update");

        assert_eq!(
            current_submitted_turn(&fixture.core.submitted_turns, &fixture.workspace_id),
            Some(submitted.turn_id)
        );
        assert!(
            events.try_recv().is_err(),
            "history terminal events from a live rollout must not echo as submitted-turn output"
        );
    }

    #[tokio::test]
    async fn history_failed_update_does_not_fail_live_submitted_turn() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let fixture = live_history_fixture("live-thread").await;
        let submitted = submit_noop_turn(&fixture.core, &fixture.workspace_id).await;
        let mut events = fixture.core.watch_events();
        fixture
            .core
            .handle_history_watch_update(codex_abort_watch_update(
                &fixture.session_path,
                "live-thread",
                fixture.cwd(),
                "interrupted",
            ))
            .expect("ingest watch update");

        assert_eq!(
            current_submitted_turn(&fixture.core.submitted_turns, &fixture.workspace_id),
            Some(submitted.turn_id)
        );
        assert!(
            events.try_recv().is_err(),
            "history failure events from a live rollout must not fail the submitted turn"
        );
    }

    #[tokio::test]
    async fn live_submitted_turn_timeout_fails_wait_without_closing_live_session() {
        let session_state = Arc::new(RecordingSessionState::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider {
            state: Arc::clone(&session_state),
        }));
        let core = LucarneCore::from_runtime_and_store_with_options(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
            CoreOptions {
                turn_inactivity: Duration::from_millis(50),
                turn_deadline: Duration::from_millis(250),
                session_idle_timeout: Duration::from_millis(500),
            },
        )
        .expect("core");
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/timeout-no-close")),
                title: "timeout no close".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let submitted = core
            .submit_turn(SubmitTurnRequest {
                workspace_id: workspace_id.clone(),
                source: TurnSource::UserMessage,
                input: crate::agent_runtime::AgentInput {
                    text: "please continue".into(),
                    images: Vec::new(),
                },
                reply_to_channel_message_id: None,
            })
            .await
            .expect("submit turn");

        let (failed_turn_id, error) = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let event = events.recv().await.expect("core event");
                if let CoreEvent::TimelineEvent {
                    turn_id,
                    event: AgentEvent::TurnFailed(failed),
                    ..
                } = event
                {
                    return (turn_id.expect("failed turn id"), failed.error.to_string());
                }
            }
        })
        .await
        .expect("submitted turn timeout failure");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(failed_turn_id, submitted.turn_id);
        assert!(error.contains("agent went silent"));
        assert!(
            core.has_live_session(&workspace_id),
            "timed-out submitted turn should leave live session/process running"
        );
        assert_eq!(session_state.interrupt_count(), 0);
        assert_eq!(session_state.close_count(), 0);
    }

    #[tokio::test]
    async fn history_index_uses_history_provider_catalog() {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        assert_eq!(core.provider_ids(), &["codex"]);
        let expected_history_provider_ids = expected_history_provider_ids();
        assert_eq!(
            core.history_provider_ids(),
            expected_history_provider_ids.as_slice()
        );
    }

    #[tokio::test]
    async fn filtered_history_entry_uses_history_provider_catalog_not_runtime_catalog() {
        let _env_lock = ENV_LOCK.lock().expect("env lock");
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let claude_config = temp.path().join("claude");
        let _env = EnvGuard::set(&[
            ("HOME", temp.path().as_os_str().to_os_string()),
            ("CODEX_HOME", codex_home.as_os_str().to_os_string()),
            (
                "CLAUDE_CONFIG_DIR",
                claude_config.as_os_str().to_os_string(),
            ),
            (
                "GEMINI_CONFIG_DIR",
                temp.path().join("gemini").as_os_str().to_os_string(),
            ),
        ]);

        write_codex_history_session(
            &codex_home,
            "codex-older",
            "/tmp/project",
            "2026-04-25T00:01:00.000Z",
            "codex prompt",
        );
        write_claude_history_session(
            &claude_config,
            "claude-newer",
            "/tmp/project",
            "2026-04-25T00:02:00.000Z",
            "claude prompt",
        );

        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");

        assert_eq!(core.provider_ids(), &["codex"]);
        let expected_history_provider_ids = expected_history_provider_ids();
        assert_eq!(
            core.history_provider_ids(),
            expected_history_provider_ids.as_slice()
        );

        let entry = core
            .history_entry_at_filtered(&["codex"], None, 0)
            .expect("filtered codex entry");
        assert_eq!(entry.provider_id, "codex");
        assert_eq!(entry.session_id, "codex-older");

        let page = core.list_history_filtered(&["codex"], None, 0, 10);
        let providers = page
            .entries
            .iter()
            .map(|entry| entry.provider_id)
            .collect::<Vec<_>>();
        assert_eq!(providers, vec!["codex"]);
    }

    #[tokio::test]
    async fn history_replay_record_persists_across_core_reload() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let store = ControlPlaneSqliteStore::open(&db).expect("store");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(runtime, store).expect("core");
        let workspace = core
            .record_workspace(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "history replay".into(),
            })
            .expect("workspace");
        let mut record = HistoryReplayRecord::new(
            workspace.workspace_id.clone(),
            "codex",
            "sess-1",
            PathBuf::from("/tmp/sess-1.jsonl"),
        );
        record.older_cursor = Some("history-before-byte:2".into());
        record.mark_user_sent("turn-1", "101");
        record.mark_assistant_sent("turn-1", "102");

        core.upsert_history_replay_record(record)
            .expect("upsert replay");

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        let loaded = reloaded
            .history_replay_record(&workspace.workspace_id)
            .expect("history replay record");

        assert_eq!(loaded.provider_id.as_str(), "codex");
        assert_eq!(loaded.session_id.as_str(), "sess-1");
        assert_eq!(loaded.session_path, PathBuf::from("/tmp/sess-1.jsonl"));
        assert_eq!(
            loaded.older_cursor.as_deref(),
            Some("history-before-byte:2")
        );
        assert_eq!(loaded.replayed_turns.len(), 1);
        assert_eq!(loaded.replayed_turns[0].turn_id.as_str(), "turn-1");
        assert_eq!(
            loaded.replayed_turns[0].user_channel_message_id.as_deref(),
            Some("101")
        );
        assert!(loaded.replayed_turns[0].assistant_sent);
    }

    #[tokio::test]
    async fn system_settings_force_bypass_permissions_persists_and_applies_to_session_starts() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");

        assert!(!core.system_settings().session.force_bypass_permissions);
        core.set_force_bypass_permissions(true)
            .expect("set system settings");

        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "project".into(),
            })
            .await
            .expect("open workspace");
        assert_eq!(
            provider.open_args(),
            vec![serde_json::json!({ "permission_mode": "bypass" })]
        );

        let resume_runtime = Arc::new(AgentRuntime::new());
        resume_runtime.register(provider.clone());
        let resume_core = LucarneCore::from_runtime_and_store(
            resume_runtime,
            ControlPlaneSqliteStore::open(&db).expect("reopen store for resume"),
        )
        .expect("resume core");
        resume_core
            .resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: opened.workspace.workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await
            .expect("resume workspace");
        assert_eq!(
            provider.resume_args(),
            vec![serde_json::json!({
                "cwd": "/tmp/project",
                "permission_mode": "bypass"
            })]
        );

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        assert!(reloaded.system_settings().session.force_bypass_permissions);
    }

    #[tokio::test]
    async fn resume_request_force_bypass_permissions_applies_without_global_setting() {
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-force-bypass-resume");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-force-bypass-resume")),
                title: "project".into(),
            },
            Some("thread-force-bypass-resume"),
        )
        .expect("workspace binding");

        assert!(!core.system_settings().session.force_bypass_permissions);
        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id,
            force_bypass_permissions: true,
        })
        .await
        .expect("resume workspace");

        assert_eq!(
            provider.resume_args(),
            vec![serde_json::json!({
                "cwd": "/tmp/project-force-bypass-resume",
                "permission_mode": "bypass"
            })]
        );
    }

    #[tokio::test]
    async fn resume_workspace_passes_workspace_project_path_to_provider() {
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-resume-cwd");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-resume-cwd")),
                title: "project".into(),
            },
            Some("thread-resume-cwd"),
        )
        .expect("workspace binding");

        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id,
            force_bypass_permissions: false,
        })
        .await
        .expect("resume workspace");

        assert_eq!(
            provider.resume_args(),
            vec![serde_json::json!({ "cwd": "/tmp/project-resume-cwd" })]
        );
    }

    #[tokio::test]
    async fn resume_workspace_with_events_reuses_existing_live_session() {
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-live-reuse");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-live-reuse")),
                title: "project".into(),
            },
            Some("thread-live-reuse"),
        )
        .expect("workspace binding");

        let first = core
            .resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await
            .expect("first resume");
        assert_eq!(provider.resume_args().len(), 1);
        let first_instance_id = first.session.instance_id().0.to_string();

        let second = core
            .resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id,
                force_bypass_permissions: false,
            })
            .await
            .expect("second resume");

        assert_eq!(
            provider.resume_args().len(),
            1,
            "resuming an already-live workspace must reuse the core live session"
        );
        assert_eq!(second.session.instance_id().0.as_str(), first_instance_id);
    }

    #[tokio::test]
    async fn force_bypass_resume_replaces_existing_default_live_session() {
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-force-bypass-live-replace");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-force-bypass-live-replace")),
                title: "project".into(),
            },
            Some("thread-force-bypass-live-replace"),
        )
        .expect("workspace binding");

        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("first resume");
        assert_eq!(provider.resume_args().len(), 1);

        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id,
            force_bypass_permissions: true,
        })
        .await
        .expect("force bypass resume");

        assert_eq!(
            provider.resume_args(),
            vec![
                serde_json::json!({ "cwd": "/tmp/project-force-bypass-live-replace" }),
                serde_json::json!({
                    "cwd": "/tmp/project-force-bypass-live-replace",
                    "permission_mode": "bypass"
                })
            ],
            "force-bypass resume must reach the provider even when a default live session exists"
        );
    }

    #[tokio::test]
    async fn detached_live_session_resumes_provider_again_without_losing_resume_ref() {
        let provider = Arc::new(RequestRecordingProvider::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(provider.clone());
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-live-detach");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-live-detach")),
                title: "project".into(),
            },
            Some("thread-live-detach"),
        )
        .expect("workspace binding");

        let first = core
            .resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await
            .expect("first resume");
        assert_eq!(provider.resume_args().len(), 1);

        let live_instance_id = LiveInstanceId::new(first.session.instance_id().0.as_str());
        core.detach_live_session(&workspace_id, &live_instance_id, "broken pipe")
            .await
            .expect("detach live session");

        assert!(!core.has_live_session(&workspace_id));
        assert_eq!(
            core.active_provider_session_ref(&workspace_id).unwrap(),
            "thread-live-detach"
        );

        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id,
            force_bypass_permissions: true,
        })
        .await
        .expect("second resume");
        assert_eq!(
            provider.resume_args().len(),
            2,
            "detached live session must not be reused on resume"
        );
    }

    #[tokio::test]
    async fn stale_event_pump_does_not_detach_replaced_live_session() {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RequestRecordingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-replaced-live");
        core.open_workspace_binding_with_events(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-replaced-live")),
                title: "project".into(),
            },
        )
        .await
        .expect("first open");
        core.open_workspace_binding_with_events(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project-replaced-live")),
                title: "project".into(),
            },
        )
        .await
        .expect("second open");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(
            core.has_live_session(&workspace_id),
            "a stale event pump must not detach the replacement live session"
        );
    }

    #[tokio::test]
    async fn resolve_permission_rejects_callback_from_another_workspace() {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RequestRecordingProvider::default()));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
        .expect("core");
        let workspace_a = WorkspaceId::new("workspace-a");
        let workspace_b = WorkspaceId::new("workspace-b");
        for (workspace_id, session_ref) in [(&workspace_a, "thread-a"), (&workspace_b, "thread-b")]
        {
            core.upsert_workspace_binding(
                workspace_id.clone(),
                OpenWorkspaceRequest {
                    provider_id: "codex",
                    project_path: Some(PathBuf::from(format!("/tmp/{session_ref}"))),
                    title: session_ref.to_string(),
                },
                Some(session_ref),
            )
            .expect("workspace binding");
            core.resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await
            .expect("resume workspace");
        }
        let live_instance_a = core
            .workspace_binding(&workspace_a)
            .and_then(|workspace| workspace.active_live_instance_id)
            .expect("workspace a live instance");
        let callback = core
            .register_intervention_callback(
                workspace_a.clone(),
                live_instance_a,
                "approval-1",
                serde_json::json!({ "allow": true }),
            )
            .expect("register callback")
            .expect("callback record");

        let err = core
            .resolve_permission(ResolvePermissionRequest {
                workspace_id: workspace_b,
                token: callback.token,
                response: crate::agent_runtime::InterventionResponse::Approval(
                    crate::agent_runtime::ApprovalDecision::Allow,
                ),
            })
            .await
            .expect_err("callback token must not resolve through another workspace");

        assert!(err.to_string().contains("workspace mismatch"));
    }

    #[tokio::test]
    async fn scoped_config_resolves_global_workspace_and_session_overrides() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let core = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let project_path = PathBuf::from("/tmp/scoped-config-project");
        let provider_session_id = provider_session_id("codex", "thread-1");

        core.set_global_notifications_enabled(false)
            .expect("disable global notifications");
        core.set_workspace_notifications_enabled(&project_path, true)
            .expect("enable workspace notifications");
        core.set_session_notifications_enabled(&provider_session_id, false)
            .expect("disable session notifications");
        core.set_force_bypass_permissions(true)
            .expect("enable global bypass");
        core.set_workspace_force_bypass_permissions(&project_path, false)
            .expect("disable workspace bypass");
        core.set_session_force_bypass_permissions(&provider_session_id, true)
            .expect("enable session bypass");

        let effective = core.effective_settings(Some(&project_path), Some(&provider_session_id));
        assert!(!effective.notifications.enabled);
        assert!(effective.session.force_bypass_permissions);

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        let effective =
            reloaded.effective_settings(Some(&project_path), Some(&provider_session_id));
        assert!(!effective.notifications.enabled);
        assert!(effective.session.force_bypass_permissions);
    }

    #[tokio::test]
    async fn message_session_binding_persists_across_core_reload() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(CatalogProvider));
        let core = LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open(&db).expect("store"),
        )
        .expect("core");
        let workspace_id = WorkspaceId::new("workspace-a");
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "message binding".into(),
            },
            Some("thread-1"),
        )
        .expect("workspace binding");
        let provider_session_id = provider_session_id("codex", "thread-1");

        core.bind_message_to_provider_session(
            "telegram",
            "100",
            "200",
            provider_session_id.clone(),
        )
        .expect("bind message to provider session");

        let reloaded = LucarneCore::from_runtime_and_store(
            Arc::new(AgentRuntime::new()),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        )
        .expect("reload core");
        let binding = reloaded
            .message_session_binding("telegram", "100", "200")
            .expect("message session binding");

        assert_eq!(binding.provider_session_id, provider_session_id);
        assert_eq!(
            reloaded
                .workspace_for_provider_session(&binding.provider_session_id)
                .map(|workspace| workspace.workspace_id),
            Some(workspace_id)
        );
    }

    struct CatalogProvider;

    #[async_trait]
    impl AgentProvider for CatalogProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Err(unsupported("open not used"))
        }

        async fn resume(&self, _req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Err(unsupported("resume not used"))
        }
    }

    struct RequestRecordingProvider {
        provider_id: ProviderId,
        open_args: StdMutex<Vec<serde_json::Value>>,
        resume_args: StdMutex<Vec<serde_json::Value>>,
    }

    impl Default for RequestRecordingProvider {
        fn default() -> Self {
            Self::new("codex")
        }
    }

    impl RequestRecordingProvider {
        fn new(provider_id: &'static str) -> Self {
            Self {
                provider_id: ProviderId::from_static(provider_id),
                open_args: StdMutex::new(Vec::new()),
                resume_args: StdMutex::new(Vec::new()),
            }
        }

        fn open_args(&self) -> Vec<serde_json::Value> {
            self.open_args.lock().unwrap().clone()
        }

        fn resume_args(&self) -> Vec<serde_json::Value> {
            self.resume_args.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AgentProvider for RequestRecordingProvider {
        fn id(&self) -> ProviderId {
            self.provider_id.clone()
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: self.provider_id.clone(),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            self.open_args.lock().unwrap().push(req.args);
            Ok(Box::new(NoopSession::new("session-open")))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            self.resume_args.lock().unwrap().push(req.args);
            Ok(Box::new(NoopSession::new(req.session_ref.0.as_str())))
        }
    }

    struct PidProvider {
        process_id: i32,
    }

    #[async_trait]
    impl AgentProvider for PidProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(NoopSession::new_with_pid(
                "session-pid",
                self.process_id,
            )))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(NoopSession::new_with_pid(
                req.session_ref.0.as_str(),
                self.process_id,
            )))
        }
    }

    #[derive(Default)]
    struct RawActivitySessionState {
        tx: StdMutex<Option<tokio::sync::mpsc::Sender<()>>>,
    }

    impl RawActivitySessionState {
        async fn emit(&self) {
            let tx = self
                .tx
                .lock()
                .expect("raw activity tx lock")
                .clone()
                .expect("raw activity tx");
            tx.send(()).await.expect("send raw activity");
        }
    }

    struct RawActivityProvider {
        state: Arc<RawActivitySessionState>,
    }

    #[async_trait]
    impl AgentProvider for RawActivityProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(RawActivitySession::new(
                "raw-activity-session-open",
                Arc::clone(&self.state),
            )))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(RawActivitySession::new(
                req.session_ref.0.as_str(),
                Arc::clone(&self.state),
            )))
        }
    }

    #[derive(Default)]
    struct RecordingSessionState {
        interrupt_count: StdMutex<usize>,
        close_count: StdMutex<usize>,
    }

    impl RecordingSessionState {
        fn interrupt_count(&self) -> usize {
            *self.interrupt_count.lock().expect("interrupt count lock")
        }

        fn close_count(&self) -> usize {
            *self.close_count.lock().expect("close count lock")
        }
    }

    struct RecordingProvider {
        state: Arc<RecordingSessionState>,
    }

    #[async_trait]
    impl AgentProvider for RecordingProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(RecordingSession::new(
                "recording-session-open",
                Arc::clone(&self.state),
            )))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(RecordingSession::new(
                req.session_ref.0.as_str(),
                Arc::clone(&self.state),
            )))
        }
    }

    struct BlockingSubmitState {
        inputs: StdMutex<Vec<String>>,
        first_submitted: Notify,
        release_first: Notify,
    }

    impl Default for BlockingSubmitState {
        fn default() -> Self {
            Self {
                inputs: StdMutex::new(Vec::new()),
                first_submitted: Notify::new(),
                release_first: Notify::new(),
            }
        }
    }

    impl BlockingSubmitState {
        fn submitted_inputs(&self) -> Vec<String> {
            self.inputs.lock().expect("submitted inputs lock").clone()
        }
    }

    struct BlockingSubmitProvider {
        state: Arc<BlockingSubmitState>,
    }

    #[async_trait]
    impl AgentProvider for BlockingSubmitProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(BlockingSubmitSession::new(
                "blocking-submit-open",
                Arc::clone(&self.state),
            )))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(BlockingSubmitSession::new(
                req.session_ref.0.as_str(),
                Arc::clone(&self.state),
            )))
        }
    }

    #[derive(Default)]
    struct CompletingProvider {
        next: StdMutex<u64>,
    }

    #[async_trait]
    impl AgentProvider for CompletingProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            let mut next = self.next.lock().unwrap();
            *next += 1;
            Ok(Box::new(CompletingSession::new(&format!(
                "complete-session-{next}"
            ))))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            Ok(Box::new(CompletingSession::new(req.session_ref.0.as_str())))
        }
    }

    struct CompletingSession {
        id: SessionId,
        instance_id: InstanceId,
        tx: tokio::sync::mpsc::Sender<AgentEvent>,
        events: StdMutex<Option<AgentEventStream>>,
    }

    impl CompletingSession {
        fn new(id: &str) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            Self {
                id: SessionId(id.into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                tx,
                events: StdMutex::new(Some(rx)),
            }
        }
    }

    struct BlockingSubmitSession {
        id: SessionId,
        instance_id: InstanceId,
        state: Arc<BlockingSubmitState>,
        tx: tokio::sync::mpsc::Sender<AgentEvent>,
        events: StdMutex<Option<AgentEventStream>>,
    }

    impl BlockingSubmitSession {
        fn new(id: &str, state: Arc<BlockingSubmitState>) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            Self {
                id: SessionId(id.into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                state,
                tx,
                events: StdMutex::new(Some(rx)),
            }
        }
    }

    #[async_trait]
    impl AgentSession for BlockingSubmitSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, input: crate::agent_runtime::AgentInput) -> Result<(), AgentError> {
            let text = input.text.to_string();
            self.state
                .inputs
                .lock()
                .expect("submitted inputs lock")
                .push(text.clone());
            if text == "first" {
                self.state.first_submitted.notify_one();
                self.state.release_first.notified().await;
            }
            self.tx
                .send(AgentEvent::Message(crate::agent_runtime::MessageEvent {
                    role: crate::agent_runtime::MessageRole::Assistant,
                    text: format!("reply: {text}").into(),
                    streaming: false,
                }))
                .await
                .map_err(|err| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: err.to_string().into(),
                })?;
            self.tx
                .send(AgentEvent::TurnCompleted(TurnCompletedEvent {
                    turn_id: format!("provider-turn-{text}").into(),
                    usage: None,
                }))
                .await
                .map_err(|err| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: err.to_string().into(),
                })?;
            Ok(())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: crate::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("events already taken"))
        }

        async fn close(&self) -> Result<(), AgentError> {
            Ok(())
        }
    }

    #[async_trait]
    impl AgentSession for CompletingSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, input: crate::agent_runtime::AgentInput) -> Result<(), AgentError> {
            self.tx
                .send(AgentEvent::Message(crate::agent_runtime::MessageEvent {
                    role: crate::agent_runtime::MessageRole::Assistant,
                    text: format!("reply: {}", input.text).into(),
                    streaming: false,
                }))
                .await
                .map_err(|err| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: err.to_string().into(),
                })?;
            self.tx
                .send(AgentEvent::TurnCompleted(TurnCompletedEvent {
                    turn_id: "provider-turn".into(),
                    usage: None,
                }))
                .await
                .map_err(|err| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: err.to_string().into(),
                })?;
            Ok(())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: crate::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("events already taken"))
        }

        async fn close(&self) -> Result<(), AgentError> {
            Ok(())
        }
    }

    struct RawActivitySession {
        id: SessionId,
        instance_id: InstanceId,
        _events_tx: tokio::sync::mpsc::Sender<AgentEvent>,
        events: StdMutex<Option<AgentEventStream>>,
        activity: StdMutex<Option<AgentActivityStream>>,
    }

    impl RawActivitySession {
        fn new(id: &str, state: Arc<RawActivitySessionState>) -> Self {
            let (events_tx, events_rx) = tokio::sync::mpsc::channel(1);
            let (activity_tx, activity_rx) = tokio::sync::mpsc::channel(16);
            *state.tx.lock().expect("raw activity tx lock") = Some(activity_tx);
            Self {
                id: SessionId(id.into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                _events_tx: events_tx,
                events: StdMutex::new(Some(events_rx)),
                activity: StdMutex::new(Some(activity_rx)),
            }
        }
    }

    #[async_trait]
    impl AgentSession for RawActivitySession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, _input: crate::agent_runtime::AgentInput) -> Result<(), AgentError> {
            Ok(())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: crate::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("events already taken"))
        }

        async fn take_activity_events(&self) -> Result<Option<AgentActivityStream>, AgentError> {
            Ok(Some(
                self.activity
                    .lock()
                    .unwrap()
                    .take()
                    .expect("activity already taken"),
            ))
        }

        async fn close(&self) -> Result<(), AgentError> {
            Ok(())
        }
    }

    struct RecordingSession {
        id: SessionId,
        instance_id: InstanceId,
        state: Arc<RecordingSessionState>,
        _tx: tokio::sync::mpsc::Sender<AgentEvent>,
        events: StdMutex<Option<AgentEventStream>>,
    }

    impl RecordingSession {
        fn new(id: &str, state: Arc<RecordingSessionState>) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            Self {
                id: SessionId(id.into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                state,
                _tx: tx,
                events: StdMutex::new(Some(rx)),
            }
        }
    }

    #[async_trait]
    impl AgentSession for RecordingSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, _input: crate::agent_runtime::AgentInput) -> Result<(), AgentError> {
            Ok(())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            *self
                .state
                .interrupt_count
                .lock()
                .expect("interrupt count lock") += 1;
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: crate::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("events already taken"))
        }

        async fn close(&self) -> Result<(), AgentError> {
            *self.state.close_count.lock().expect("close count lock") += 1;
            Ok(())
        }
    }

    struct NoopSession {
        id: SessionId,
        instance_id: InstanceId,
        process_id: Option<i32>,
        _tx: tokio::sync::mpsc::Sender<AgentEvent>,
        events: StdMutex<Option<AgentEventStream>>,
    }

    impl NoopSession {
        fn new(id: &str) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            Self {
                id: SessionId(id.into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                process_id: None,
                _tx: tx,
                events: StdMutex::new(Some(rx)),
            }
        }

        fn new_with_pid(id: &str, process_id: i32) -> Self {
            let mut session = Self::new(id);
            session.process_id = Some(process_id);
            session
        }
    }

    #[async_trait]
    impl AgentSession for NoopSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        fn process_id(&self) -> Option<i32> {
            self.process_id
        }

        async fn submit(&self, _input: crate::agent_runtime::AgentInput) -> Result<(), AgentError> {
            Ok(())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: crate::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            Ok(self
                .events
                .lock()
                .unwrap()
                .take()
                .expect("events already taken"))
        }

        async fn close(&self) -> Result<(), AgentError> {
            Ok(())
        }
    }

    fn unsupported(message: &str) -> AgentError {
        AgentError {
            kind: AgentErrorKind::Unsupported,
            message: message.into(),
        }
    }

    struct EnvGuard(Vec<(&'static str, Option<OsString>)>);

    impl EnvGuard {
        fn set(vars: &[(&'static str, OsString)]) -> Self {
            let old = vars
                .iter()
                .map(|(key, _)| (*key, std::env::var_os(key)))
                .collect();
            for (key, value) in vars {
                std::env::set_var(key, value);
            }
            Self(old)
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.0.drain(..) {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    struct LiveHistoryFixture {
        _temp: tempfile::TempDir,
        _env: EnvGuard,
        core: Arc<LucarneCore>,
        workspace_id: WorkspaceId,
        session_path: PathBuf,
        project_path: PathBuf,
    }

    impl LiveHistoryFixture {
        fn cwd(&self) -> &str {
            self.project_path.to_str().expect("utf8 project")
        }
    }

    async fn live_history_fixture(session_id: &str) -> LiveHistoryFixture {
        live_history_fixture_for_provider("codex", session_id).await
    }

    async fn live_history_fixture_for_provider(
        provider_id: &'static str,
        session_id: &str,
    ) -> LiveHistoryFixture {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let codex_home = temp.path().join("codex");
        let project_path = temp.path().join("project");
        fs::create_dir_all(&project_path).expect("project dir");
        let unrelated_codex_home = temp.path().join("unrelated-codex");
        let env = EnvGuard::set(&[(
            "CODEX_HOME",
            unrelated_codex_home.as_os_str().to_os_string(),
        )]);
        let session_path = write_codex_history_session(
            &codex_home,
            session_id,
            project_path.to_str().expect("utf8 project"),
            "2026-04-25T00:01:00.000Z",
            "ping",
        );
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RequestRecordingProvider::new(provider_id)));
        let core = LucarneCore::from_runtime_and_store_with_options(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
            CoreOptions {
                turn_inactivity: Duration::from_millis(50),
                turn_deadline: Duration::from_millis(250),
                session_idle_timeout: Duration::from_millis(500),
            },
        )
        .expect("core");
        let workspace_id = workspace_id_for_resume(provider_id, session_id);
        core.upsert_workspace_binding(
            workspace_id.clone(),
            OpenWorkspaceRequest {
                provider_id,
                project_path: Some(project_path.clone()),
                title: format!("{provider_id} - project").into(),
            },
            Some(session_id),
        )
        .expect("workspace binding");
        core.resume_workspace_with_events(ResumeWorkspaceRequest {
            workspace_id: workspace_id.clone(),
            force_bypass_permissions: false,
        })
        .await
        .expect("resume workspace");
        LiveHistoryFixture {
            _temp: temp,
            _env: env,
            core,
            workspace_id,
            session_path,
            project_path,
        }
    }

    async fn submit_noop_turn(core: &LucarneCore, workspace_id: &WorkspaceId) -> SubmittedTurn {
        core.submit_turn(SubmitTurnRequest {
            workspace_id: workspace_id.clone(),
            source: TurnSource::UserMessage,
            input: crate::agent_runtime::AgentInput {
                text: "please continue".into(),
                images: Vec::new(),
            },
            reply_to_channel_message_id: None,
        })
        .await
        .expect("submit turn")
    }

    fn write_codex_history_session(
        codex_home: &Path,
        session_id: &str,
        cwd: &str,
        timestamp: &str,
        prompt: &str,
    ) -> PathBuf {
        let dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&dir).expect("codex sessions dir");
        let path = dir.join(format!("rollout-2026-04-25T00-00-00-{session_id}.jsonl"));
        let lines = [
            serde_json::json!({
                "timestamp": timestamp,
                "type": "session_meta",
                "payload": {
                    "session_id": session_id,
                    "cwd": cwd,
                    "originator": "codex-cli",
                    "model": "gpt-5.4",
                },
            })
            .to_string(),
            serde_json::json!({
                "timestamp": timestamp,
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            })
            .to_string(),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).expect("write codex session");
        path
    }

    fn append_codex_assistant_response(path: &Path, timestamp: &str, text: &str) {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open codex session");
        writeln!(
            file,
            r#"{{"timestamp":"{timestamp}","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{text}"}}],"phase":"final_answer"}}}}"#
        )
        .expect("append codex assistant response");
        writeln!(
            file,
            r#"{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"task_complete","last_agent_message":"{text}","duration_ms":125000}}}}"#
        )
        .expect("append codex assistant response");
        file.sync_all().expect("sync codex assistant response");
    }

    fn append_codex_user_prompt(path: &Path, timestamp: &str, prompt: &str) {
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open codex session");
        let line = serde_json::json!({
            "timestamp": timestamp,
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt,
                    }
                ],
            },
        });
        writeln!(file, "\n{}", line).expect("append codex user prompt");
        file.sync_all().expect("sync codex user prompt");
    }

    fn codex_watch_update(path: &Path, session_id: &str, cwd: &str, text: &str) -> WatchUpdate {
        provider_watch_update("codex", path, session_id, cwd, text, None)
    }

    fn codex_watch_update_with_turn_id(
        path: &Path,
        session_id: &str,
        cwd: &str,
        text: &str,
        turn_id: &str,
    ) -> WatchUpdate {
        provider_watch_update("codex", path, session_id, cwd, text, Some(turn_id))
    }

    fn provider_watch_update(
        provider_id: &str,
        path: &Path,
        session_id: &str,
        cwd: &str,
        text: &str,
        turn_id: Option<&str>,
    ) -> WatchUpdate {
        WatchUpdate {
            provider: watch_provider(provider_id),
            path: path.to_path_buf(),
            session_id: Some(session_id.into()),
            cwd: Some(cwd.into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::TurnCompleted(WatchTurnCompleted {
                meta: WatchEventMeta {
                    timestamp: Some("2026-04-25T00:01:01.000Z".into()),
                    turn_id: turn_id.map(Into::into),
                    ..WatchEventMeta::default()
                },
                last_agent_message: Some(text.into()),
                duration_ms: Some(125_000),
                value_json: None,
            })]
            .into_boxed_slice(),
            error: None,
        }
    }

    fn codex_user_watch_update(
        path: &Path,
        session_id: &str,
        cwd: &str,
        text: &str,
        timestamp: &str,
    ) -> WatchUpdate {
        WatchUpdate {
            provider: watch_provider("codex"),
            path: path.to_path_buf(),
            session_id: Some(session_id.into()),
            cwd: Some(cwd.into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::UserMessage(WatchMessage {
                meta: WatchEventMeta {
                    timestamp: Some(timestamp.into()),
                    ..WatchEventMeta::default()
                },
                text: Some(text.into()),
            })]
            .into_boxed_slice(),
            error: None,
        }
    }

    fn codex_abort_watch_update(
        path: &Path,
        session_id: &str,
        cwd: &str,
        reason: &str,
    ) -> WatchUpdate {
        WatchUpdate {
            provider: watch_provider("codex"),
            path: path.to_path_buf(),
            session_id: Some(session_id.into()),
            cwd: Some(cwd.into()),
            title: None,
            change: WatchChange::Updated,
            events: vec![WatchEvent::TurnFailed(WatchTurnFailed {
                meta: WatchEventMeta {
                    timestamp: Some("2026-04-25T00:01:01.000Z".into()),
                    ..WatchEventMeta::default()
                },
                reason: Some(reason.into()),
                duration_ms: Some(125_000),
                value_json: None,
            })]
            .into_boxed_slice(),
            error: None,
        }
    }

    fn observed_session_for_test(title: &str, observed_pid: Option<i32>) -> ObservedAgentSession {
        ObservedAgentSession {
            workspace_id: WorkspaceId::new(format!("workspace-{title}")),
            provider_id: "codex",
            provider_session_id: ProviderSessionId::new(format!("codex:{title}")),
            native_resume_ref: title.into(),
            title: title.into(),
            cwd: Some(PathBuf::from("/tmp/lucarnex")),
            session_path: PathBuf::from(format!("/tmp/{title}.jsonl")),
            last_active_unix: 1_776_960_000,
            last_active_display: "2026-05-01T00:00:00Z".into(),
            observed_pid,
        }
    }

    fn write_claude_history_session(
        claude_config: &Path,
        session_id: &str,
        cwd: &str,
        timestamp: &str,
        prompt: &str,
    ) {
        let dir = claude_config.join("projects/project");
        fs::create_dir_all(&dir).expect("claude sessions dir");
        let path = dir.join(format!("{session_id}.jsonl"));
        let line = format!(
            r#"{{"parentUuid":null,"isSidechain":false,"promptId":"p1","type":"user","message":{{"role":"user","content":"{prompt}"}},"uuid":"u1","timestamp":"{timestamp}","permissionMode":"default","userType":"external","entrypoint":"cli","cwd":"{cwd}","sessionId":"{session_id}","version":"2.1.91","gitBranch":"HEAD"}}"#
        );
        fs::write(path, line).expect("write claude session");
    }
}
