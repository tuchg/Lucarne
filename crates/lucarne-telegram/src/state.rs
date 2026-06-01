//! Bot state.
#[cfg(test)]
use async_trait::async_trait;
#[cfg(test)]
use lucarne::agent_runtime::AgentEventStream;
#[cfg(test)]
use lucarne::agent_runtime::{
    AgentCapabilities, AgentError, AgentErrorKind, AgentProvider, AgentRuntime, OpenSession,
    ProbeResult, ProviderId, ResumeSession,
};
use lucarne::agent_runtime::{
    AgentForkTargetCatalog, AgentImageInput, AgentInput, AgentStatus, InstanceId,
};
#[cfg(test)]
use lucarne::control_plane::ControlPlaneSqliteStore;
use lucarne::control_plane::{
    ActivationPlan, ActivationRequest, ChannelBinding, ChannelBindingId, CommandCallbackToken,
    CommandCompletionPolicy, CommandId, CommandInvocationPlan, CommandWorkflow,
    ControlPlaneStoreError, HistoryOlderCallbackToken, HistoryReplayRecord,
    InterventionCallbackToken, LiveInstanceId, PanelRenderId, PanelRenderRecord, ProviderSessionId,
    ReconcileOutcome, Revision, StatusSnapshot, SubAgentActionRecord, SubAgentCallbackToken,
    SubAgentLinkId, SubAgentLinkRecord, SubAgentState, TurnId, TurnSource, WorkspaceBinding,
    WorkspaceId as ControlWorkspaceId,
};
use lucarne::core_service::{
    CoreWorkspaceEventStream, DaemonSession, LucarneCore, OpenWorkspaceRequest, SubmitTurnRequest,
    TimelineKindProjection, TimelineProjectionItem,
};
use lucarne::event::SubAgentCall;
use lucarne_channel::{ChatId, MessageId, WorkspaceHandle, WorkspaceId};
#[cfg(test)]
use std::sync::OnceLock;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info};

const NOTIFICATION_WORKSPACE_ID: &str = "telegram:agent-notifications";

/// The live lucarne session bound to a workspace.
pub struct LiveSession {
    pub session: Arc<dyn DaemonSession>,
    /// Workspace-scoped event subscription returned by the core daemon API.
    /// Drained per user turn by the bot flow.
    pub events: AsyncMutex<CoreWorkspaceEventStream>,
    /// Channel-side message ids for outstanding intervention prompts,
    /// keyed by `req_id`. When the user clicks a button we delete the
    /// prompt so the conversation stays clean.
    pub pending_intv: Mutex<HashMap<String, MessageId>>,
}

/// A running or formerly-running working session mapped to a channel
/// workspace (e.g. Telegram forum topic).
#[derive(Clone)]
pub struct WorkSession {
    pub workspace: WorkspaceId,
    pub chat: ChatId,
    pub provider_id: &'static str,
    pub project_path: Option<PathBuf>,
    /// Title currently displayed on the channel.
    pub title: String,
    /// `None` when no live session is bound (freshly created topic, or
    /// the bot is waiting for the user's first message to lazy-open).
    pub live: Option<Arc<LiveSession>>,
    /// Session identifier suitable for a future daemon resume call.
    pub resume_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RunningCommandWorkflow {
    pub turn_id: TurnId,
    pub command_id: CommandId,
    pub completion_policy: CommandCompletionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningTurn {
    pub turn_id: TurnId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentCommandCallback {
    pub topic: WorkspaceId,
    pub workspace: WorkspaceId,
    pub catalog_revision: u64,
    pub name: String,
    pub args: String,
    pub values: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentInterventionCallback {
    pub topic: Option<WorkspaceId>,
    pub workspace: WorkspaceId,
    pub live_instance: InstanceId,
    pub req_id: String,
    pub action: crate::turn::IntvAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAgentCallback {
    pub topic: WorkspaceId,
    pub workspace: WorkspaceId,
    pub workspace_revision: Revision,
    pub link_id: SubAgentLinkId,
    pub link_revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryOlderCallback {
    pub topic: WorkspaceId,
    pub workspace: WorkspaceId,
    pub provider_id: &'static str,
    pub session_id: String,
    pub session_path: PathBuf,
    pub cursor: String,
}

impl std::fmt::Debug for WorkSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkSession")
            .field("workspace", &self.workspace)
            .field("chat", &self.chat)
            .field("provider_id", &self.provider_id)
            .field("project_path", &self.project_path)
            .field("title", &self.title)
            .field("live", &self.live.is_some())
            .field("resume_ref", &self.resume_ref)
            .finish()
    }
}

pub struct BotState {
    core: Arc<LucarneCore>,
    inner: RwLock<Inner>,
    provider_ids: Vec<&'static str>,
}

#[derive(Default)]
struct Inner {
    /// Durable workspace id -> session.
    sessions: HashMap<WorkspaceId, WorkSession>,
    /// Durable workspace id -> channel topic identity.
    topic_by_workspace: HashMap<WorkspaceId, ChannelTopicKey>,
    /// Channel chat/topic identity -> durable workspace id.
    workspace_by_topic: HashMap<ChannelTopicKey, WorkspaceId>,
    /// Instance id -> workspace id (reverse lookup for event routing).
    by_instance: HashMap<InstanceId, WorkspaceId>,
    /// Current entry panel snapshot (what `/a1`, `/h1`, `/w1` etc. resolve to).
    panel: PanelSnapshot,
    panel_revision: Revision,
    /// Images sent without text, held until the next textual turn in
    /// the same workspace.
    pending_images: HashMap<WorkspaceId, Vec<AgentImageInput>>,
    /// Best-effort hint for Telegram inline command suggestions. Inline
    /// queries do not include the final topic id, so execution still relies
    /// on the selected result being sent as a normal topic message.
    last_workspace_by_user: HashMap<String, WorkspaceId>,
    /// Current `/fN` fork target selectors, scoped to the resolved workspace.
    fork_targets_by_workspace: HashMap<WorkspaceId, ForkTargetSelection>,
    /// Live-only fork workspaces should ignore the source session id until
    /// the provider surfaces a different, durable fork resume ref.
    live_only_fork_sources: HashMap<WorkspaceId, String>,
    notifications: NotificationState,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChannelTopicKey {
    chat: ChatId,
    topic: WorkspaceId,
}

impl ChannelTopicKey {
    fn new(chat: &ChatId, topic: &WorkspaceId) -> Self {
        Self {
            chat: chat.clone(),
            topic: topic.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct NotificationState {
    topic: Option<ChannelTopicKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderSessionSync {
    SeedIfMissing,
    Replace,
    Clear,
}

#[derive(Clone)]
struct ForkTargetSelection {
    provider_session_id: ProviderSessionId,
    targets: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum StateStoreError {
    #[error(transparent)]
    Store(#[from] ControlPlaneStoreError),
    #[error("core state: {0}")]
    Core(String),
    #[error("unknown provider id in state db: {0}")]
    UnknownProvider(String),
    #[error("missing topic binding for workspace: {0}")]
    MissingTopicBinding(String),
}

/// What the user currently sees in the entry panel. `/a<N>`, `/h<N>`,
/// `/w<N>` are resolved through this mapping so the bot can stay
/// stateless per click — the numbering always matches the most recently
/// rendered panel.
#[derive(Default, Clone)]
pub struct PanelSnapshot {
    pub view: PanelView,
    pub provider_filter: Option<String>,
    pub workspace_filter: Option<PathBuf>,
    /// Provider ids in `/aN` order (1-based index into this list).
    pub agents: Vec<String>,
    /// Global daemon history indices in `/hN` order — already offset-adjusted.
    pub history: Vec<usize>,
    /// Workspace ids in `/wN` order.
    pub workspaces: Vec<WorkspaceId>,
    /// Project/workspace cwd values in `/wN` order for session filtering.
    pub history_workspaces: Vec<PathBuf>,
    /// Current pagination offset for the history section.
    pub history_offset: usize,
    /// Total history entries available (for next/prev bounds).
    pub history_total: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PanelView {
    #[default]
    Overview,
    Workspaces,
    Sessions,
}

impl BotState {
    #[cfg(test)]
    pub fn new() -> Arc<Self> {
        Self::open_sqlite_store_with_provider_ids(
            ControlPlaneSqliteStore::open_in_memory().expect("open in-memory state store"),
            default_test_provider_ids(),
        )
        .expect("open in-memory bot state")
    }

    pub fn new_with_core(core: Arc<LucarneCore>) -> Arc<Self> {
        let provider_ids = core.provider_ids().to_vec();
        Self::new_with_core_and_provider_ids(core, provider_ids)
    }

    pub fn new_with_core_and_provider_ids(
        core: Arc<LucarneCore>,
        provider_ids: Vec<&'static str>,
    ) -> Arc<Self> {
        info!(
            target: "lucarne_telegram::state",
            provider_count = provider_ids.len(),
            "telegram state initialized"
        );
        Arc::new(Self {
            core,
            inner: RwLock::new(Inner::default()),
            provider_ids,
        })
    }

    pub fn hydrate_notification_handle(&self, chat: &ChatId) {
        if self.notification_handle().is_some() {
            return;
        }
        let binding_id = notification_channel_binding_id(chat);
        let Some(binding) = self.core.channel_binding(&binding_id) else {
            return;
        };
        if binding.channel.as_str() != "telegram" || binding.chat_id.as_str() != chat.as_str() {
            return;
        }
        let Some(topic_id) = binding.topic_id.as_ref() else {
            return;
        };
        self.inner.write().unwrap().notifications.topic = Some(ChannelTopicKey::new(
            chat,
            &WorkspaceId::new(topic_id.as_str()),
        ));
    }

    pub fn core_handle(&self) -> Arc<LucarneCore> {
        Arc::clone(&self.core)
    }

    pub async fn submit_user_turn(
        &self,
        ws: &WorkspaceId,
        input: AgentInput,
        reply_to: Option<&MessageId>,
    ) -> Result<RunningTurn, String> {
        let submitted = self
            .core
            .submit_turn(SubmitTurnRequest {
                workspace_id: control_workspace_id(ws),
                source: TurnSource::UserMessage,
                input,
                reply_to_channel_message_id: reply_to
                    .and_then(|id| id.as_str().parse::<i64>().ok()),
            })
            .await
            .map_err(|err| err.to_string())?;
        Ok(RunningTurn {
            turn_id: submitted.turn_id,
        })
    }

    fn sync_core_session(
        &self,
        session: &WorkSession,
        topic: &WorkspaceId,
        provider_sync: ProviderSessionSync,
    ) -> Result<(), String> {
        let workspace_id = self.sync_core_session_projection(session, provider_sync)?;
        self.sync_core_channel_binding(session, topic, &workspace_id)
    }

    fn sync_core_session_projection(
        &self,
        session: &WorkSession,
        provider_sync: ProviderSessionSync,
    ) -> Result<ControlWorkspaceId, String> {
        let workspace_id = control_workspace_id(&session.workspace);
        let project_path = session.project_path.clone();
        let request = OpenWorkspaceRequest {
            provider_id: session.provider_id,
            project_path,
            title: session.title.clone(),
        };

        if provider_sync == ProviderSessionSync::Clear {
            self.core
                .upsert_workspace_binding(workspace_id.clone(), request, None)
                .map_err(|err| err.to_string())?;
            self.core
                .clear_workspace_activation(&workspace_id)
                .map_err(|err| err.to_string())?;
            return Ok(workspace_id);
        }

        let existing_ref = self.core.active_provider_session_ref(&workspace_id).ok();
        let native_ref = match provider_sync {
            ProviderSessionSync::Replace => session.resume_ref.clone().or_else(|| {
                session
                    .live
                    .as_ref()
                    .map(|live| provisional_live_resume_ref(live))
            }),
            ProviderSessionSync::SeedIfMissing => existing_ref
                .or_else(|| session.resume_ref.clone())
                .or_else(|| {
                    session
                        .live
                        .as_ref()
                        .map(|live| provisional_live_resume_ref(live))
                }),
            ProviderSessionSync::Clear => None,
        };

        self.core
            .upsert_workspace_binding(workspace_id.clone(), request, native_ref.as_deref())
            .map_err(|err| err.to_string())?;
        if let (Some(live), Some(native_ref)) = (session.live.as_ref(), native_ref.as_ref()) {
            self.core
                .attach_live_session_projection(
                    workspace_id.clone(),
                    session.provider_id,
                    native_ref,
                    &LiveInstanceId::new(live.session.instance_id().0.as_str()),
                )
                .map_err(|err| err.to_string())?;
        }
        Ok(workspace_id)
    }

    fn sync_core_channel_binding(
        &self,
        session: &WorkSession,
        topic: &WorkspaceId,
        workspace_id: &ControlWorkspaceId,
    ) -> Result<(), String> {
        let next_binding_id = telegram_topic_channel_binding_id(&session.chat, topic);
        for binding in self.core.channel_bindings_for_workspace(workspace_id) {
            if binding.channel.as_str() == "telegram"
                && binding.chat_id.as_str() == session.chat.as_str()
                && binding.channel_binding_id != next_binding_id
            {
                self.core
                    .remove_channel_binding(&binding.channel_binding_id)
                    .map_err(|err| err.to_string())?;
            }
        }
        self.core
            .upsert_channel_binding(ChannelBinding::new(
                next_binding_id,
                workspace_id.clone(),
                "telegram",
                session.chat.as_str(),
                Some(topic.as_str()),
            ))
            .map_err(|err| err.to_string())
    }

    #[cfg(test)]
    pub fn open_sqlite(path: impl AsRef<std::path::Path>) -> Result<Arc<Self>, StateStoreError> {
        Self::open_sqlite_with_provider_ids(path, default_test_provider_ids())
    }

    #[cfg(test)]
    pub fn open_sqlite_with_provider_ids(
        path: impl AsRef<std::path::Path>,
        provider_ids: Vec<&'static str>,
    ) -> Result<Arc<Self>, StateStoreError> {
        let store = ControlPlaneSqliteStore::open(path.as_ref())?;
        Self::open_sqlite_store_with_provider_ids(store, provider_ids)
    }

    #[cfg(test)]
    pub fn open_sqlite_store_with_provider_ids(
        store: ControlPlaneSqliteStore,
        provider_ids: Vec<&'static str>,
    ) -> Result<Arc<Self>, StateStoreError> {
        let has_control_snapshot = store.has_any_record()?;
        let core = test_core_with_provider_ids(store, &provider_ids)?;
        let mut inner = Inner::default();
        let max_panel_revision = core.max_panel_render_revision();
        inner.panel_revision = max_panel_revision
            .map(|revision| Revision::new(revision.get().saturating_add(1)))
            .unwrap_or_default();
        if has_control_snapshot {
            core.mark_live_instances_stale_after_restart("telegram bot restarted")
                .map_err(|err| StateStoreError::Core(err.to_string()))?;
            core.mark_panel_renders_stale_after_restart()
                .map_err(|err| StateStoreError::Core(err.to_string()))?;
        }
        rebuild_sessions_from_core(&mut inner, &core, &provider_ids)?;
        Ok(Arc::new(Self {
            core,
            inner: RwLock::new(inner),
            provider_ids,
        }))
    }

    #[cfg(test)]
    pub fn upsert(&self, session: WorkSession) -> Result<(), StateStoreError> {
        self.upsert_with_topic(session.clone(), session.workspace.clone())
    }

    pub fn upsert_with_topic(
        &self,
        session: WorkSession,
        topic: WorkspaceId,
    ) -> Result<(), StateStoreError> {
        self.upsert_with_topic_sync(session, topic, ProviderSessionSync::SeedIfMissing)
    }

    pub fn upsert_with_topic_replacing_resume_ref(
        &self,
        session: WorkSession,
        topic: WorkspaceId,
    ) -> Result<(), StateStoreError> {
        self.upsert_with_topic_sync(session, topic, ProviderSessionSync::Replace)
    }

    fn upsert_with_topic_sync(
        &self,
        session: WorkSession,
        topic: WorkspaceId,
        provider_sync: ProviderSessionSync,
    ) -> Result<(), StateStoreError> {
        let _ = provider_id_from(&self.provider_ids, session.provider_id)
            .ok_or_else(|| StateStoreError::UnknownProvider(session.provider_id.to_string()))?;
        let mut g = self.inner.write().unwrap();
        insert_session(&mut g, session.clone(), topic.clone());
        drop(g);
        self.sync_core_session(&session, &topic, provider_sync)
            .map_err(StateStoreError::Core)?;
        debug!(
            target: "lucarne_telegram::state",
            workspace = %session.workspace.as_str(),
            topic = %topic.as_str(),
            provider_id = session.provider_id,
            "telegram session upserted"
        );
        Ok(())
    }

    pub fn get(&self, ws: &WorkspaceId) -> Option<WorkSession> {
        let g = self.inner.read().unwrap();
        g.sessions.get(ws).cloned()
    }

    pub fn workspace_for_handle(&self, handle: &WorkspaceHandle) -> Option<WorkspaceId> {
        self.inner
            .read()
            .unwrap()
            .workspace_by_topic
            .get(&ChannelTopicKey::new(&handle.chat, &handle.workspace))
            .cloned()
    }

    pub fn topic_for_workspace(&self, ws: &WorkspaceId) -> Option<WorkspaceId> {
        self.inner
            .read()
            .unwrap()
            .topic_by_workspace
            .get(ws)
            .map(|key| key.topic.clone())
    }

    pub fn workspace_for_topic(&self, topic: &WorkspaceId) -> Option<WorkspaceId> {
        let g = self.inner.read().unwrap();
        let mut matches = g
            .workspace_by_topic
            .iter()
            .filter(|(key, _)| &key.topic == topic)
            .map(|(_, workspace)| workspace.clone());
        let first = matches.next()?;
        if matches.next().is_some() {
            return None;
        }
        Some(first)
    }

    pub fn handle_for_session(
        &self,
        session: &WorkSession,
    ) -> Result<WorkspaceHandle, StateStoreError> {
        let topic = self
            .topic_for_workspace(&session.workspace)
            .ok_or_else(|| {
                StateStoreError::MissingTopicBinding(session.workspace.as_str().to_string())
            })?;
        Ok(WorkspaceHandle::new(session.chat.clone(), topic))
    }

    pub async fn remove(&self, ws: &WorkspaceId) -> Result<Option<WorkSession>, StateStoreError> {
        let (workspace, removed) = {
            let mut g = self.inner.write().unwrap();
            let removed = g.sessions.remove(ws);
            if removed.is_some() {
                if let Some(key) = g.topic_by_workspace.remove(ws) {
                    g.workspace_by_topic.remove(&key);
                }
                g.by_instance.retain(|_, mapped| mapped != ws);
                g.pending_images.remove(ws);
                g.last_workspace_by_user.retain(|_, mapped| mapped != ws);
                g.live_only_fork_sources.remove(ws);
            }
            drop(g);
            (ws.clone(), removed)
        };
        if removed.is_some() {
            self.core
                .remove_workspace_projection(&control_workspace_id(&workspace))
                .await
                .map_err(|err| StateStoreError::Core(err.to_string()))?;
        }
        Ok(removed)
    }

    pub fn workspace_for_instance(&self, inst: &InstanceId) -> Option<WorkspaceId> {
        self.inner.read().unwrap().by_instance.get(inst).cloned()
    }

    pub fn remember_user_workspace(&self, user: &str, ws: &WorkspaceId) {
        self.inner
            .write()
            .unwrap()
            .last_workspace_by_user
            .insert(user.to_string(), ws.clone());
    }

    pub fn last_workspace_for_user(&self, user: &str) -> Option<WorkspaceId> {
        self.inner
            .read()
            .unwrap()
            .last_workspace_by_user
            .get(user)
            .cloned()
    }

    pub fn notifications_enabled_for_session(&self, session: &WorkSession) -> bool {
        let Ok(provider_session_id) = self.active_provider_session_id(&session.workspace) else {
            return false;
        };
        self.core
            .effective_settings(session.project_path.as_deref(), Some(&provider_session_id))
            .notifications
            .enabled
    }

    pub fn notification_handle(&self) -> Option<WorkspaceHandle> {
        self.inner
            .read()
            .unwrap()
            .notifications
            .topic
            .as_ref()
            .map(|topic| WorkspaceHandle::new(topic.chat.clone(), topic.topic.clone()))
    }

    pub fn set_notification_handle(&self, handle: &WorkspaceHandle) -> Result<(), String> {
        self.core
            .upsert_channel_binding(ChannelBinding::new(
                notification_channel_binding_id(&handle.chat),
                ControlWorkspaceId::new(NOTIFICATION_WORKSPACE_ID),
                "telegram",
                handle.chat.as_str(),
                Some(handle.workspace.as_str()),
            ))
            .map_err(|err| err.to_string())?;
        self.inner.write().unwrap().notifications.topic =
            Some(ChannelTopicKey::new(&handle.chat, &handle.workspace));
        Ok(())
    }

    pub fn clear_notification_handle(&self, chat: &ChatId) -> Result<(), String> {
        self.core
            .remove_channel_binding(&notification_channel_binding_id(chat))
            .map_err(|err| err.to_string())?;
        let mut g = self.inner.write().unwrap();
        if g.notifications
            .topic
            .as_ref()
            .is_some_and(|topic| topic.chat == *chat)
        {
            g.notifications.topic = None;
        }
        Ok(())
    }

    pub fn is_notification_handle(&self, handle: &WorkspaceHandle) -> bool {
        self.inner
            .read()
            .unwrap()
            .notifications
            .topic
            .as_ref()
            .is_some_and(|topic| topic == &ChannelTopicKey::new(&handle.chat, &handle.workspace))
    }

    pub fn register_message_session_binding(
        &self,
        channel: &str,
        chat: &ChatId,
        message: &MessageId,
        provider_session_id: ProviderSessionId,
    ) -> Result<(), StateStoreError> {
        self.core
            .bind_message_to_provider_session(
                channel,
                chat.as_str(),
                message.as_str(),
                provider_session_id,
            )
            .map(|_| ())
            .map_err(|err| StateStoreError::Core(err.to_string()))
    }

    pub fn resolve_message_session_binding(
        &self,
        channel: &str,
        chat: &ChatId,
        reply_to: &MessageId,
    ) -> Option<ProviderSessionId> {
        self.core
            .message_session_binding(channel, chat.as_str(), reply_to.as_str())
            .map(|binding| binding.provider_session_id)
    }

    pub fn bind_live(
        &self,
        ws: &WorkspaceId,
        live: Arc<LiveSession>,
        resume_ref: Option<String>,
    ) -> Result<(), StateStoreError> {
        let instance_id = live.session.instance_id().clone();
        let instance_id_text = instance_id.0.to_string();
        let provider_sync = if resume_ref.is_some() {
            ProviderSessionSync::Replace
        } else {
            ProviderSessionSync::SeedIfMissing
        };
        let sync = {
            let mut g = self.inner.write().unwrap();
            let mut updated = None;
            if resume_ref.is_some() {
                g.live_only_fork_sources.remove(ws);
            }
            if let Some(s) = g.sessions.get_mut(ws) {
                let current_live_instance = s
                    .live
                    .as_ref()
                    .map(|live| live.session.instance_id().clone());
                let resume_changed =
                    resume_ref.is_some() && s.resume_ref.as_deref() != resume_ref.as_deref();
                let live_changed = current_live_instance.as_ref() != Some(&instance_id);
                if resume_ref.is_some() {
                    s.resume_ref = resume_ref;
                }
                s.live = Some(live);
                if resume_changed || live_changed {
                    updated = Some(s.clone());
                }
            }
            g.by_instance.insert(instance_id, ws.clone());
            if let Some(session) = updated {
                let topic = topic_for_workspace_optional_locked(&g, &session.workspace);
                let sync = Some((session, topic));
                drop(g);
                sync
            } else {
                drop(g);
                None
            }
        };
        if let Some((session, topic)) = sync {
            if let Some(topic) = topic {
                self.sync_core_session(&session, &topic, provider_sync)
                    .map_err(StateStoreError::Core)?;
            } else {
                self.sync_core_session_projection(&session, provider_sync)
                    .map_err(StateStoreError::Core)?;
            }
            debug!(
                target: "lucarne_telegram::state",
                workspace = %session.workspace.as_str(),
                instance_id = %instance_id_text,
                "telegram live session bound"
            );
        }
        Ok(())
    }

    pub fn bind_live_replacing_resume_ref(
        &self,
        ws: &WorkspaceId,
        live: Arc<LiveSession>,
        resume_ref: Option<String>,
    ) -> Result<(), StateStoreError> {
        let instance_id = live.session.instance_id().clone();
        let provider_sync = if resume_ref.is_some() {
            ProviderSessionSync::Replace
        } else {
            ProviderSessionSync::Clear
        };
        let sync = {
            let mut g = self.inner.write().unwrap();
            let mut updated = None;
            if resume_ref.is_some() {
                g.live_only_fork_sources.remove(ws);
            }
            if let Some(s) = g.sessions.get_mut(ws) {
                s.resume_ref = resume_ref;
                s.live = Some(live);
                updated = Some(s.clone());
            }
            g.by_instance.insert(instance_id, ws.clone());
            if let Some(session) = updated {
                let topic = topic_for_workspace_optional_locked(&g, &session.workspace);
                let sync = Some((session, topic));
                drop(g);
                sync
            } else {
                drop(g);
                None
            }
        };
        if let Some((session, topic)) = sync {
            if let Some(topic) = topic {
                self.sync_core_session(&session, &topic, provider_sync)
                    .map_err(StateStoreError::Core)?;
            } else {
                self.sync_core_session_projection(&session, provider_sync)
                    .map_err(StateStoreError::Core)?;
            }
        }
        Ok(())
    }

    pub fn mark_live_dead(
        &self,
        ws: &WorkspaceId,
        resume_ref: Option<String>,
    ) -> Result<(), StateStoreError> {
        let provider_sync = if resume_ref.is_some() {
            ProviderSessionSync::Replace
        } else {
            ProviderSessionSync::SeedIfMissing
        };
        let sync = {
            let mut g = self.inner.write().unwrap();
            let mut updated = None;
            if resume_ref.is_some() {
                g.live_only_fork_sources.remove(ws);
            }
            if let Some(s) = g.sessions.get_mut(ws) {
                if resume_ref.is_some() {
                    s.resume_ref = resume_ref;
                }
                s.live = None;
                updated = Some(s.clone());
            }
            g.by_instance.retain(|_, mapped| mapped != ws);
            if let Some(session) = updated {
                let topic = topic_for_workspace_optional_locked(&g, &session.workspace);
                let sync = Some((session, topic));
                drop(g);
                sync
            } else {
                drop(g);
                None
            }
        };
        if let Some((session, topic)) = sync {
            if let Some(topic) = topic {
                self.sync_core_session(&session, &topic, provider_sync)
                    .map_err(StateStoreError::Core)?;
            } else {
                self.sync_core_session_projection(&session, provider_sync)
                    .map_err(StateStoreError::Core)?;
            }
            debug!(
                target: "lucarne_telegram::state",
                workspace = %session.workspace.as_str(),
                "telegram live session marked dead"
            );
        }
        Ok(())
    }

    pub fn live_only_fork_source_ref(&self, ws: &WorkspaceId) -> Option<String> {
        self.inner
            .read()
            .unwrap()
            .live_only_fork_sources
            .get(ws)
            .cloned()
    }

    pub fn clear_live_and_resume_ref(&self, ws: &WorkspaceId) -> Result<(), StateStoreError> {
        {
            let mut g = self.inner.write().unwrap();
            if let Some(s) = g.sessions.get_mut(ws) {
                s.live = None;
                s.resume_ref = None;
            }
            g.by_instance.retain(|_, mapped| mapped != ws);
            drop(g);
        }
        self.core
            .clear_workspace_activation(&control_workspace_id(ws))
            .map_err(|err| StateStoreError::Core(err.to_string()))?;
        Ok(())
    }

    pub fn rename(&self, ws: &WorkspaceId, title: String) -> Result<(), StateStoreError> {
        let sync = {
            let mut g = self.inner.write().unwrap();
            let mut updated = None;
            if let Some(s) = g.sessions.get_mut(ws) {
                s.title = title;
                updated = Some(s.clone());
            }
            if let Some(session) = updated {
                let topic = topic_for_workspace_locked(&g, &session.workspace)?;
                let sync = Some((session, topic));
                drop(g);
                sync
            } else {
                drop(g);
                None
            }
        };
        if let Some((session, topic)) = sync {
            self.sync_core_session(&session, &topic, ProviderSessionSync::SeedIfMissing)
                .map_err(StateStoreError::Core)?;
        }
        Ok(())
    }

    pub fn all(&self) -> Vec<WorkSession> {
        self.inner
            .read()
            .unwrap()
            .sessions
            .values()
            .cloned()
            .collect()
    }

    pub fn clear_workspace_records(&self) -> Result<usize, StateStoreError> {
        let cleared = {
            let mut g = self.inner.write().unwrap();
            let cleared = g.sessions.len();
            g.sessions.clear();
            g.topic_by_workspace.clear();
            g.workspace_by_topic.clear();
            g.by_instance.clear();
            g.pending_images.clear();
            g.last_workspace_by_user.clear();
            g.fork_targets_by_workspace.clear();
            g.panel.workspaces.clear();
            cleared
        };
        self.core
            .clear_workspace_records()
            .map_err(|err| StateStoreError::Core(err.to_string()))?;
        Ok(cleared)
    }

    pub fn hydrate_unbound_control_workspaces(
        &self,
        default_chat: &ChatId,
    ) -> Result<Vec<WorkSession>, StateStoreError> {
        let mut g = self.inner.write().unwrap();
        let mut hydrated = Vec::new();
        for workspace in self.core.workspace_bindings() {
            let workspace_id = WorkspaceId::new(workspace.workspace_id.as_str());
            if g.sessions.contains_key(&workspace_id) {
                continue;
            }
            let channel_bindings = self
                .core
                .channel_bindings_for_workspace(&workspace.workspace_id);
            let telegram_binding = channel_bindings
                .iter()
                .find(|binding| {
                    binding.channel.as_str() == "telegram" && binding.topic_id.is_some()
                })
                .or_else(|| {
                    channel_bindings
                        .iter()
                        .find(|binding| binding.channel.as_str() == "telegram")
                });
            let (chat, topic) = telegram_binding
                .map(|binding| {
                    (
                        ChatId::new(binding.chat_id.as_str()),
                        binding
                            .topic_id
                            .as_ref()
                            .map(|topic| WorkspaceId::new(topic.as_str())),
                    )
                })
                .unwrap_or_else(|| (default_chat.clone(), None));
            let session = self.session_from_workspace_binding(workspace, chat)?;
            if let Some(topic) = topic {
                insert_session(&mut g, session.clone(), topic);
            } else {
                g.sessions.insert(workspace_id, session.clone());
            }
            hydrated.push(session);
        }
        Ok(hydrated)
    }

    pub fn hydrate_workspace(
        &self,
        ws: &WorkspaceId,
        default_chat: &ChatId,
    ) -> Result<Option<WorkSession>, StateStoreError> {
        if let Some(session) = self.get(ws) {
            return Ok(Some(session));
        }
        let Some(workspace) = self.core.workspace_binding(&control_workspace_id(ws)) else {
            return Ok(None);
        };
        let channel_bindings = self
            .core
            .channel_bindings_for_workspace(&workspace.workspace_id);
        let telegram_binding = channel_bindings
            .iter()
            .find(|binding| binding.channel.as_str() == "telegram" && binding.topic_id.is_some())
            .or_else(|| {
                channel_bindings
                    .iter()
                    .find(|binding| binding.channel.as_str() == "telegram")
            });
        let (chat, topic) = telegram_binding
            .map(|binding| {
                (
                    ChatId::new(binding.chat_id.as_str()),
                    binding
                        .topic_id
                        .as_ref()
                        .map(|topic| WorkspaceId::new(topic.as_str())),
                )
            })
            .unwrap_or_else(|| (default_chat.clone(), None));
        let session = self.session_from_workspace_binding(workspace, chat)?;
        let mut g = self.inner.write().unwrap();
        if let Some(topic) = topic {
            insert_session(&mut g, session.clone(), topic);
        } else {
            g.sessions.insert(ws.clone(), session.clone());
        }
        Ok(Some(session))
    }

    pub fn hydrate_workspace_for_handle(
        &self,
        handle: &WorkspaceHandle,
    ) -> Result<Option<WorkSession>, StateStoreError> {
        if let Some(workspace_id) = self.workspace_for_handle(handle) {
            return Ok(self.get(&workspace_id));
        }
        let binding_id = telegram_topic_channel_binding_id(&handle.chat, &handle.workspace);
        let Some(binding) = self.core.channel_binding(&binding_id) else {
            return Ok(None);
        };
        if binding.channel.as_str() != "telegram"
            || binding.chat_id.as_str() != handle.chat.as_str()
            || binding
                .topic_id
                .as_ref()
                .is_none_or(|topic| topic.as_str() != handle.workspace.as_str())
        {
            return Ok(None);
        }
        let Some(workspace) = self.core.workspace_binding(&binding.workspace_id) else {
            return Ok(None);
        };
        let session =
            self.session_from_workspace_binding(workspace, ChatId::new(binding.chat_id.as_str()))?;
        let mut g = self.inner.write().unwrap();
        insert_session(&mut g, session.clone(), handle.workspace.clone());
        Ok(Some(session))
    }

    fn session_from_workspace_binding(
        &self,
        workspace: WorkspaceBinding,
        chat: ChatId,
    ) -> Result<WorkSession, StateStoreError> {
        let workspace_id = WorkspaceId::new(workspace.workspace_id.as_str());
        let Some(provider_id) =
            provider_id_from(&self.provider_ids, workspace.provider_id.as_str())
        else {
            return Err(StateStoreError::UnknownProvider(
                workspace.provider_id.to_string(),
            ));
        };
        let resume_ref = workspace
            .active_provider_session_id
            .as_ref()
            .and_then(|provider_session_id| self.core.provider_session_record(provider_session_id))
            .map(|session| session.native_resume_ref.to_string())
            .filter(|resume_ref| !is_provisional_live_resume_ref(resume_ref));
        let project_path = (!workspace.project_path.as_os_str().is_empty())
            .then(|| workspace.project_path.clone());
        Ok(WorkSession {
            workspace: workspace_id,
            chat,
            provider_id,
            project_path,
            title: workspace.title.to_string(),
            live: None,
            resume_ref,
        })
    }

    pub fn set_panel(&self, snap: PanelSnapshot) -> Revision {
        let mut g = self.inner.write().unwrap();
        g.panel_revision = Revision::new(g.panel_revision.get() + 1);
        g.panel = snap;
        g.panel_revision
    }

    pub fn panel(&self) -> PanelSnapshot {
        self.inner.read().unwrap().panel.clone()
    }

    pub fn panel_revision(&self) -> Revision {
        self.inner.read().unwrap().panel_revision
    }

    pub fn panel_revision_matches(&self, observed: Revision) -> bool {
        self.inner.read().unwrap().panel_revision == observed
    }

    pub fn record_entry_panel_stale_revision(
        &self,
        chat: &ChatId,
        observed: Revision,
    ) -> Result<(), String> {
        let current = self.inner.read().unwrap().panel_revision;
        self.core
            .record_panel_stale_revision(
                entry_panel_id(chat),
                "telegram",
                chat.as_str(),
                observed,
                current,
            )
            .map_err(|err| err.to_string())
    }

    pub fn record_entry_panel_render(
        &self,
        chat: &ChatId,
        message_id: &MessageId,
        revision: Revision,
    ) -> Result<(), String> {
        self.core
            .upsert_panel_render(PanelRenderRecord::new(
                entry_panel_id(chat),
                "telegram",
                chat.as_str(),
                Some(message_id.as_str()),
                revision,
            ))
            .map_err(|err| err.to_string())
    }

    pub fn entry_panel_render(&self, chat: &ChatId) -> Option<PanelRenderRecord> {
        self.core.panel_render(&entry_panel_id(chat))
    }

    pub fn workspace_revision(&self, ws: &WorkspaceId) -> Option<Revision> {
        self.core.workspace_revision(&control_workspace_id(ws))
    }

    pub fn status_snapshot(&self, ws: &WorkspaceId) -> Option<StatusSnapshot> {
        self.core.status_snapshot(&control_workspace_id(ws))
    }

    pub fn record_provider_status(
        &self,
        ws: &WorkspaceId,
        status: &AgentStatus,
    ) -> Option<StatusSnapshot> {
        let workspace_id = control_workspace_id(ws);
        self.core
            .record_provider_status(&workspace_id, status)
            .ok()
            .flatten()
    }

    pub fn plan_activation_for_session(
        &self,
        session: &WorkSession,
    ) -> Result<ActivationPlan, String> {
        let planned_session = session.clone();
        let control_workspace = control_workspace_id(&planned_session.workspace);
        if self.core.workspace_binding(&control_workspace).is_none() {
            let native_ref = planned_session.resume_ref.clone().or_else(|| {
                planned_session
                    .live
                    .as_ref()
                    .map(|live| provisional_live_resume_ref(live))
            });
            self.core
                .upsert_workspace_binding(
                    control_workspace.clone(),
                    OpenWorkspaceRequest {
                        provider_id: planned_session.provider_id,
                        project_path: planned_session.project_path.clone(),
                        title: planned_session.title.clone(),
                    },
                    native_ref.as_deref(),
                )
                .map_err(|err| err.to_string())?;
        }
        let plan = self
            .core
            .plan_activation(ActivationRequest {
                workspace_id: control_workspace,
                channel: "telegram".into(),
                chat_id: planned_session.chat.as_str().into(),
            })
            .map_err(|err| format!("{err:?}"))?;
        Ok(plan)
    }

    pub fn record_reconcile_outcome(
        &self,
        ws: &WorkspaceId,
        outcome: ReconcileOutcome,
    ) -> Result<(), String> {
        self.core
            .record_reconcile_outcome(control_workspace_id(ws), outcome)
            .map_err(|err| err.to_string())
    }

    pub fn record_reconcile_outcomes_batch(
        &self,
        outcomes: Vec<(WorkspaceId, ReconcileOutcome)>,
    ) -> Result<(), String> {
        let mapped: Vec<_> = outcomes
            .into_iter()
            .map(|(ws, outcome)| (control_workspace_id(&ws), outcome))
            .collect();
        self.core
            .record_reconcile_outcomes_batch(mapped)
            .map_err(|err| err.to_string())
    }

    pub fn new_history_replay_record(
        &self,
        ws: &WorkspaceId,
        chat: &ChatId,
        topic: &WorkspaceId,
        provider_id: &'static str,
        session_id: impl Into<smol_str::SmolStr>,
        session_path: PathBuf,
    ) -> HistoryReplayRecord {
        let mut record = HistoryReplayRecord::new(
            control_workspace_id(ws),
            provider_id,
            session_id,
            session_path,
        );
        record.set_projection_target("telegram", chat.as_str(), topic.as_str());
        record
    }

    pub fn history_replay_record(
        &self,
        ws: &WorkspaceId,
        chat: &ChatId,
        topic: &WorkspaceId,
    ) -> Option<HistoryReplayRecord> {
        self.core
            .history_replay_record(&control_workspace_id(ws))
            .filter(|record| {
                record.matches_projection_target("telegram", chat.as_str(), topic.as_str())
            })
    }

    pub fn upsert_history_replay_record(&self, record: HistoryReplayRecord) -> Result<(), String> {
        self.core
            .upsert_history_replay_record(record)
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    pub fn remember_history_replay_record(&self, record: HistoryReplayRecord) {
        self.core.remember_history_replay_record(record);
    }

    pub fn recycle_topic_binding(&self, ws: &WorkspaceId) -> Result<(), String> {
        let (chat, removed_topic) = {
            let mut g = self.inner.write().unwrap();
            let removed = g.topic_by_workspace.remove(ws);
            if let Some(key) = removed.as_ref() {
                g.workspace_by_topic.remove(key);
            }
            let chat = g
                .sessions
                .get(ws)
                .map(|session| session.chat.clone())
                .or_else(|| removed.as_ref().map(|key| key.chat.clone()));
            (chat, removed)
        };
        let control_workspace = control_workspace_id(ws);
        if let Some(chat) = chat.as_ref() {
            for binding in self.core.channel_bindings_for_workspace(&control_workspace) {
                if binding.channel.as_str() == "telegram"
                    && binding.chat_id.as_str() == chat.as_str()
                {
                    self.core
                        .remove_channel_binding(&binding.channel_binding_id)
                        .map_err(|err| err.to_string())?;
                }
            }
        }
        self.core
            .remove_history_replay_record(&control_workspace)
            .map_err(|err| err.to_string())?;
        debug!(
            target: "lucarne_telegram::state",
            workspace = %ws.as_str(),
            removed_topic = ?removed_topic.map(|key| key.topic.as_str().to_string()),
            "telegram topic binding recycled"
        );
        Ok(())
    }

    pub fn register_history_older_callback(
        &self,
        ws: &WorkspaceId,
        provider_id: &'static str,
        session_id: &str,
        session_path: PathBuf,
        cursor: &str,
    ) -> Result<String, String> {
        let record = self
            .core
            .register_history_older_callback(
                control_workspace_id(ws),
                provider_id,
                session_id,
                session_path,
                cursor,
            )
            .map_err(|err| err.to_string())?;
        let payload = record.callback_payload();
        assert!(
            payload.len() <= 64,
            "telegram history callback_data exceeds 64 bytes: {payload}"
        );
        Ok(payload)
    }

    pub fn resolve_history_older_callback(&self, token: &str) -> Option<HistoryOlderCallback> {
        let record = self
            .core
            .resolve_history_older_callback_record(&HistoryOlderCallbackToken::new(token))?;
        let workspace_binding = self.core.workspace_binding(&record.workspace_id)?;
        if workspace_binding.provider_id.as_str() != record.provider_id.as_str() {
            return None;
        }
        if record.provider_session_id.is_none()
            || workspace_binding.active_provider_session_id != record.provider_session_id
        {
            return None;
        }
        let provider_id = provider_id_from(&self.provider_ids, record.provider_id.as_str())?;
        let workspace = WorkspaceId::new(record.workspace_id.as_str());
        let g = self.inner.read().unwrap();
        let topic = g.topic_by_workspace.get(&workspace)?.topic.clone();
        Some(HistoryOlderCallback {
            topic,
            workspace,
            provider_id,
            session_id: record.session_id.to_string(),
            session_path: record.session_path,
            cursor: record.cursor.to_string(),
        })
    }

    pub fn register_command_callback(
        &self,
        ws: &WorkspaceId,
        catalog_revision: u64,
        name: &str,
        args: &str,
    ) -> String {
        self.register_command_callback_values(
            ws,
            catalog_revision,
            name,
            args,
            serde_json::Value::Null,
        )
    }

    pub fn register_command_callback_values(
        &self,
        ws: &WorkspaceId,
        catalog_revision: u64,
        name: &str,
        args: &str,
        values: serde_json::Value,
    ) -> String {
        let control_workspace = control_workspace_id(ws);
        let record = match self.core.register_command_callback(
            control_workspace,
            Revision::new(catalog_revision),
            name.trim().trim_start_matches('/').to_ascii_lowercase(),
            (!args.trim().is_empty()).then(|| args.trim().to_string().into()),
            values,
        ) {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(
                    target: "lucarne_telegram::state",
                    error = %err,
                    "failed to register command callback"
                );
                return "agentcmd:c:stale".into();
            }
        };
        let Some(record) = record else {
            return "agentcmd:c:stale".into();
        };
        let payload = record.callback_payload();
        assert!(
            payload.len() <= 64,
            "telegram callback_data exceeds 64 bytes: {payload}"
        );
        payload
    }

    pub fn resolve_command_callback(&self, token: &str) -> Option<AgentCommandCallback> {
        let record = self
            .core
            .resolve_command_callback(&CommandCallbackToken::new(token))?;
        let workspace = self
            .core
            .workspace_binding(&record.workspace_id)
            .filter(|workspace| {
                workspace.revision == record.workspace_revision
                    && workspace.active_provider_session_id == record.provider_session_id
            })?;
        let workspace = WorkspaceId::new(workspace.workspace_id.as_str());
        let g = self.inner.read().unwrap();
        let topic = g.topic_by_workspace.get(&workspace)?.topic.clone();
        Some(AgentCommandCallback {
            topic,
            workspace,
            catalog_revision: record.catalog_revision.get(),
            name: record.command_name.to_string(),
            args: record
                .args
                .as_deref()
                .map(str::to_string)
                .unwrap_or_default(),
            values: record.values,
        })
    }

    pub fn register_intervention_callback(
        &self,
        ws: &WorkspaceId,
        live_instance: &InstanceId,
        req_id: &str,
        action: crate::turn::IntvAction,
    ) -> String {
        let control_workspace = control_workspace_id(ws);
        let action = serde_json::to_value(action).expect("intervention action must serialize");
        let record = match self.core.register_intervention_callback(
            control_workspace,
            LiveInstanceId::new(live_instance.0.as_str()),
            req_id,
            action,
        ) {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(
                    target: "lucarne_telegram::state",
                    error = %err,
                    "failed to register intervention callback"
                );
                return "intv:c:stale".into();
            }
        };
        let Some(record) = record else {
            return "intv:c:stale".into();
        };
        let payload = record.callback_payload();
        assert!(
            payload.len() <= 64,
            "telegram intervention callback_data exceeds 64 bytes: {payload}"
        );
        payload
    }

    pub fn resolve_intervention_callback(&self, token: &str) -> Option<AgentInterventionCallback> {
        let record = self
            .core
            .resolve_intervention_callback_record(&InterventionCallbackToken::new(token))?;
        let workspace = self
            .core
            .workspace_binding(&record.workspace_id)
            .filter(|workspace| {
                workspace.revision == record.workspace_revision
                    && workspace.active_provider_session_id == record.provider_session_id
                    && workspace.active_live_instance_id.as_ref() == Some(&record.live_instance_id)
            })?;
        let workspace_id = WorkspaceId::new(workspace.workspace_id.as_str());
        let g = self.inner.read().unwrap();
        let topic = g
            .topic_by_workspace
            .get(&workspace_id)
            .map(|topic| topic.topic.clone());
        let action = serde_json::from_value(record.action).ok()?;
        Some(AgentInterventionCallback {
            topic,
            workspace: workspace_id,
            live_instance: InstanceId(record.live_instance_id.as_str().into()),
            req_id: record.req_id.to_string(),
            action,
        })
    }

    pub fn remove_intervention_callbacks_for_request(
        &self,
        live_instance: &InstanceId,
        req_id: &str,
    ) -> Result<(), String> {
        self.core
            .remove_intervention_callbacks_for_request(
                &LiveInstanceId::new(live_instance.0.as_str()),
                req_id,
            )
            .map_err(|err| err.to_string())
    }

    #[cfg(test)]
    pub fn command_workflows(&self, ws: &WorkspaceId) -> Vec<CommandWorkflow> {
        self.core
            .command_workflows_for_workspace(&control_workspace_id(ws))
    }

    pub fn resolve_fork_target_selection(&self, ws: &WorkspaceId, index: usize) -> Option<String> {
        if index == 0 {
            return None;
        }
        let (workspace, selection) = {
            let g = self.inner.read().unwrap();
            let selection = g.fork_targets_by_workspace.get(ws)?.clone();
            (ws.clone(), selection)
        };
        let control_workspace = control_workspace_id(&workspace);
        let active_provider_session_id = self
            .core
            .active_provider_session_id(&control_workspace)
            .ok()?;
        if selection.provider_session_id != active_provider_session_id {
            return None;
        }
        selection.targets.get(index - 1).cloned()
    }

    pub fn record_fork_target_selection(
        &self,
        ws: &WorkspaceId,
        catalog: &AgentForkTargetCatalog,
    ) -> Result<(), String> {
        let provider_session_id = self
            .core
            .active_provider_session_id(&control_workspace_id(ws))
            .map_err(|err| err.to_string())?;
        let targets = catalog
            .targets
            .iter()
            .map(|target| target.id.to_string())
            .collect();
        self.inner
            .write()
            .unwrap()
            .fork_targets_by_workspace
            .insert(
                ws.clone(),
                ForkTargetSelection {
                    provider_session_id,
                    targets,
                },
            );
        Ok(())
    }

    #[cfg(test)]
    pub fn timeline_kinds(&self, ws: &WorkspaceId) -> Vec<TimelineKindProjection> {
        self.core.timeline_kinds(&control_workspace_id(ws))
    }

    pub fn subagent_links_for_turn(&self, turn_id: &TurnId) -> Vec<SubAgentLinkRecord> {
        self.core.subagent_links_for_turn(turn_id)
    }

    pub fn subagent_links_for_workspace(&self, ws: &WorkspaceId) -> Vec<SubAgentLinkRecord> {
        self.core
            .subagent_links_for_workspace(&control_workspace_id(ws))
    }

    pub fn register_subagent_callback(
        &self,
        ws: &WorkspaceId,
        link: &SubAgentLinkRecord,
    ) -> String {
        let control_workspace = control_workspace_id(ws);
        let record = match self
            .core
            .register_subagent_callback(control_workspace, link.link_id.clone())
        {
            Ok(record) => record,
            Err(err) => {
                tracing::warn!(
                    target: "lucarne_telegram::state",
                    error = %err,
                    "failed to register subagent callback"
                );
                return "subagent:c:stale".into();
            }
        };
        let Some(record) = record else {
            return "subagent:c:stale".into();
        };
        let payload = record.callback_payload();
        assert!(
            payload.len() <= 64,
            "telegram subagent callback_data exceeds 64 bytes: {payload}"
        );
        payload
    }

    pub fn resolve_subagent_callback(
        &self,
        token: &str,
    ) -> Option<(SubAgentCallback, SubAgentLinkRecord)> {
        let record = self
            .core
            .resolve_subagent_callback_record(&SubAgentCallbackToken::new(token))?;
        let workspace = self
            .core
            .workspace_binding(&record.workspace_id)
            .filter(|workspace| workspace.revision == record.workspace_revision)?;
        let g = self.inner.read().unwrap();
        let topic = g
            .topic_by_workspace
            .get(&WorkspaceId::new(record.workspace_id.as_str()))?
            .topic
            .clone();
        let link = self
            .core
            .openable_subagent_link(&record.link_id)
            .filter(|link| {
                link.workspace_id == record.workspace_id && link.revision == record.link_revision
            })?;
        Some((
            SubAgentCallback {
                topic,
                workspace: WorkspaceId::new(workspace.workspace_id.as_str()),
                workspace_revision: record.workspace_revision,
                link_id: record.link_id,
                link_revision: record.link_revision,
            },
            link,
        ))
    }

    pub fn attach_subagent_child_workspace(
        &self,
        link_id: &SubAgentLinkId,
        child_workspace: &WorkspaceId,
    ) -> Result<(), String> {
        self.core
            .attach_subagent_child_workspace(link_id, control_workspace_id(child_workspace))
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("missing openable subagent link {}", link_id.as_str()))?;
        Ok(())
    }

    pub fn start_command_workflow(
        &self,
        ws: &WorkspaceId,
        live: &LiveSession,
        command_name: &str,
        args: Option<String>,
        values: serde_json::Value,
        catalog_revision: Revision,
        completion_policy: CommandCompletionPolicy,
        reply_to: Option<&MessageId>,
    ) -> Result<RunningCommandWorkflow, String> {
        let workspace_id = control_workspace_id(ws);
        let provider_session_id = self
            .core
            .active_provider_session_id(&workspace_id)
            .map_err(|err| err.to_string())?;
        let live_instance_id = LiveInstanceId::new(live.session.instance_id().0.as_str());
        let reply_to = reply_to.and_then(|id| id.as_str().parse::<i64>().ok());
        let turn = self
            .core
            .start_turn(
                workspace_id.clone(),
                provider_session_id,
                live_instance_id,
                TurnSource::Command,
                format!("/{command_name} {}", args.as_deref().unwrap_or(""))
                    .trim()
                    .to_string(),
                reply_to,
            )
            .map_err(|err| format!("{err:?}"))?;
        let workflow = self
            .core
            .start_command(CommandWorkflow::new(
                workspace_id,
                turn.turn_id.clone(),
                command_name,
                args.map(Into::into),
                values,
                catalog_revision,
                completion_policy,
            ))
            .map_err(|err| format!("{err:?}"))?;
        Ok(RunningCommandWorkflow {
            turn_id: turn.turn_id,
            command_id: workflow.command_id,
            completion_policy,
        })
    }

    pub fn start_planned_command_workflow(
        &self,
        ws: &WorkspaceId,
        live: &LiveSession,
        plan: &CommandInvocationPlan,
        reply_to: Option<&MessageId>,
    ) -> Result<RunningCommandWorkflow, String> {
        self.start_command_workflow(
            ws,
            live,
            plan.name.as_str(),
            plan.args.as_ref().map(|args| args.to_string()),
            plan.values.clone(),
            plan.catalog_revision,
            plan.completion_policy,
            reply_to,
        )
    }

    #[cfg(test)]
    pub fn start_user_turn(
        &self,
        ws: &WorkspaceId,
        live: &LiveSession,
        input: &str,
        reply_to: Option<&MessageId>,
    ) -> Result<RunningTurn, String> {
        let workspace_id = control_workspace_id(ws);
        let provider_session_id = self
            .core
            .active_provider_session_id(&workspace_id)
            .map_err(|err| err.to_string())?;
        let reply_to = reply_to.and_then(|id| id.as_str().parse::<i64>().ok());
        let turn = self
            .core
            .start_turn(
                workspace_id.clone(),
                provider_session_id,
                LiveInstanceId::new(live.session.instance_id().0.as_str()),
                TurnSource::UserMessage,
                input.to_string(),
                reply_to,
            )
            .map_err(|err| format!("{err:?}"))?;
        self.core
            .append_timeline(TimelineProjectionItem::new(
                workspace_id,
                turn.turn_id.clone(),
                TimelineKindProjection::User,
                serde_json::json!({ "text": input }),
            ))
            .map_err(|err| format!("{err:?}"))?;
        Ok(RunningTurn {
            turn_id: turn.turn_id,
        })
    }

    pub fn active_provider_session_ref(&self, ws: &WorkspaceId) -> Result<String, String> {
        self.core
            .active_provider_session_ref(&control_workspace_id(ws))
            .map_err(|err| err.to_string())
    }

    pub fn active_provider_session_id(
        &self,
        ws: &WorkspaceId,
    ) -> Result<ProviderSessionId, String> {
        self.core
            .active_provider_session_id(&control_workspace_id(ws))
            .map_err(|err| err.to_string())
    }

    pub fn workspace_for_provider_session(
        &self,
        provider_session_id: &ProviderSessionId,
    ) -> Option<WorkspaceId> {
        self.core
            .workspace_for_provider_session(provider_session_id)
            .map(|workspace| WorkspaceId::new(workspace.workspace_id.as_str()))
    }

    pub fn record_fork_transition(
        &self,
        source_ws: &WorkspaceId,
        source_ref: String,
        fork_session: &WorkSession,
    ) -> Result<(), String> {
        let mut g = self.inner.write().unwrap();
        let source_workspace = source_ws.clone();
        let mut updated_source = None;
        if let Some(source) = g.sessions.get_mut(&source_workspace) {
            source.resume_ref = Some(source_ref.clone());
            source.live = None;
            updated_source = Some(source.clone());
        }
        g.by_instance
            .retain(|_, mapped| mapped != &source_workspace);
        if fork_session.resume_ref.is_some() {
            g.live_only_fork_sources.remove(&fork_session.workspace);
        } else {
            g.live_only_fork_sources
                .insert(fork_session.workspace.clone(), source_ref.clone());
        }
        let source_topic = if let Some(source) = updated_source.as_ref() {
            Some(
                topic_for_workspace_locked(&g, &source.workspace)
                    .map_err(|err| format!("{err:?}"))?,
            )
        } else {
            None
        };
        drop(g);
        if let Some(source) = updated_source.as_ref() {
            let source_workspace_id = control_workspace_id(&source.workspace);
            self.core
                .clear_workspace_activation(&source_workspace_id)
                .map_err(|err| err.to_string())?;
            if let Some(topic) = source_topic.as_ref() {
                self.sync_core_session(source, topic, ProviderSessionSync::Replace)?;
            }
        }
        let fork_workspace = control_workspace_id(&fork_session.workspace);
        let fork_live_instance = fork_session
            .live
            .as_ref()
            .map(|live| LiveInstanceId::new(live.session.instance_id().0.as_str()));
        let fork_native_ref = fork_session.resume_ref.clone().or_else(|| {
            fork_session
                .live
                .as_ref()
                .map(|live| provisional_live_resume_ref(live))
        });
        self.core
            .record_fork_workspace_projection(
                control_workspace_id(&source_workspace),
                fork_workspace,
                fork_session.title.clone(),
                fork_session.provider_id,
                fork_native_ref.as_deref(),
                fork_live_instance,
            )
            .map_err(|err| err.to_string())
    }

    pub fn complete_turn(&self, turn: &RunningTurn) -> Result<(), String> {
        self.complete_turn_with_usage(turn, None)
    }

    pub fn complete_turn_with_usage(
        &self,
        turn: &RunningTurn,
        usage: Option<lucarne::agent_runtime::UsageEvent>,
    ) -> Result<(), String> {
        let usage = usage
            .map(|usage| {
                serde_json::to_value(usage)
                    .map_err(|err| format!("failed to serialize turn usage: {err}"))
            })
            .transpose()?;
        self.core
            .complete_turn_with_usage_value(turn.turn_id.clone(), usage)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn record_turn_usage(
        &self,
        turn_id: &TurnId,
        usage: lucarne::agent_runtime::UsageEvent,
    ) -> Result<(), String> {
        let usage = serde_json::to_value(usage)
            .map_err(|err| format!("failed to serialize turn usage: {err}"))?;
        self.core
            .update_turn_usage(turn_id, usage)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn mark_turn_waiting_permission(&self, turn_id: &TurnId) -> Result<(), String> {
        self.core
            .mark_turn_waiting_permission(turn_id)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn mark_live_instance_running(&self, live_instance: &InstanceId) -> Result<(), String> {
        self.core
            .mark_live_instance_running(&LiveInstanceId::new(live_instance.0.as_str()))
            .map_err(|err| format!("{err:?}"))
    }

    pub fn fail_turn(&self, turn: &RunningTurn, error: &str) -> Result<(), String> {
        self.core
            .fail_turn(turn.turn_id.clone(), error)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn append_turn_timeline(
        &self,
        ws: &WorkspaceId,
        turn_id: &TurnId,
        kind: TimelineKindProjection,
        payload: serde_json::Value,
    ) -> Result<TimelineProjectionItem, String> {
        let workspace_id = control_workspace_id(ws);
        let item = self
            .core
            .append_timeline(TimelineProjectionItem::new(
                workspace_id,
                turn_id.clone(),
                kind,
                payload,
            ))
            .map_err(|err| format!("{err:?}"))?;
        Ok(item)
    }

    pub fn recorded_timeline_item(
        &self,
        ws: &WorkspaceId,
        item: &TimelineProjectionItem,
    ) -> Result<TimelineProjectionItem, String> {
        let workspace_id = control_workspace_id(ws);
        self.core
            .timeline_item(&workspace_id, item.seq)
            .map_err(|err| format!("{err:?}"))?
            .ok_or_else(|| {
                format!(
                    "recorded timeline item {} is missing for workspace {}",
                    item.seq.get(),
                    workspace_id.as_str()
                )
            })
    }

    pub fn record_subagent_tool_call(
        &self,
        ws: &WorkspaceId,
        turn_id: &TurnId,
        provider_item_id: Option<&str>,
        input: &serde_json::Value,
    ) -> Result<(), String> {
        let call: SubAgentCall =
            serde_json::from_value(input.clone()).map_err(|err| err.to_string())?;
        let control_workspace = control_workspace_id(ws);
        let turn = self
            .core
            .turn_record(turn_id)
            .ok_or_else(|| format!("missing turn {}", turn_id.as_str()))?;
        let mut action = SubAgentActionRecord::new(
            control_workspace.clone(),
            turn_id.clone(),
            turn.provider_session_id.clone(),
            call.tool_name.clone(),
        );
        action.provider_item_id = provider_item_id.map(Into::into);
        action.prompt = call.prompt.clone();
        action.requested_model = call.requested_model.clone();
        action.child_provider_session_id = call
            .child_session_ref
            .as_ref()
            .map(|id| ProviderSessionId::new(id.as_str()));
        action.child_native_ref = call
            .child_session_ref
            .clone()
            .or(call.child_thread_id.clone());
        action.state =
            subagent_state_from_status(call.status.as_deref(), action.child_native_ref.is_some());
        action.summary = call.message.clone();
        action.raw = input.clone();
        let action = self
            .core
            .record_subagent_action(action)
            .map_err(|err| err.to_string())?;
        let label = call
            .nickname
            .clone()
            .or(call.role.clone())
            .or(call.prompt.clone())
            .unwrap_or_else(|| call.tool_name.as_str().into());
        let link_id = subagent_link_id(
            &control_workspace,
            action.child_native_ref.as_deref(),
            action
                .child_provider_session_id
                .as_ref()
                .map(|provider_session_id| provider_session_id.as_str()),
            action.action_id.as_str(),
        );
        let openable =
            action.child_provider_session_id.is_some() || action.child_native_ref.is_some();
        let mut link = if openable {
            SubAgentLinkRecord::new_openable_ref(
                link_id,
                control_workspace,
                action.action_id.clone(),
                turn_id.clone(),
                turn.provider_session_id,
                action.child_provider_session_id.clone(),
                action.child_native_ref.clone(),
            )
        } else {
            SubAgentLinkRecord::new_non_openable(
                link_id,
                control_workspace,
                action.action_id.clone(),
                turn_id.clone(),
                turn.provider_session_id,
                label.clone(),
            )
        };
        link.label = Some(label);
        link.agent_id = call.agent_id.clone();
        link.nickname = call.nickname.clone();
        link.role = call.role.clone();
        link.model = call.requested_model.clone();
        link.prompt = call.prompt.clone();
        link.last_message = call.message.clone();
        link.state = subagent_state_from_status(call.status.as_deref(), link.openable);
        self.core
            .upsert_subagent_link(link)
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    pub fn complete_command_workflow(
        &self,
        workflow: &RunningCommandWorkflow,
        observed_policy: CommandCompletionPolicy,
        result: serde_json::Value,
    ) -> Result<(), String> {
        self.core
            .complete_command_for_policy(workflow.command_id.clone(), observed_policy, result)
            .map_err(|err| format!("{err:?}"))?;
        self.core
            .complete_turn_with_usage_value(workflow.turn_id.clone(), None)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn fail_command_workflow(
        &self,
        workflow: &RunningCommandWorkflow,
        error: &str,
    ) -> Result<(), String> {
        self.core
            .fail_command(workflow.command_id.clone(), error)
            .map_err(|err| format!("{err:?}"))?;
        self.core
            .fail_turn(workflow.turn_id.clone(), error)
            .map_err(|err| format!("{err:?}"))
    }

    pub fn telegram_channel_binding(&self, ws: &WorkspaceId) -> Option<ChannelBinding> {
        let g = self.inner.read().unwrap();
        let topic = g.topic_by_workspace.get(ws)?;
        self.core
            .channel_binding(&telegram_topic_channel_binding_id(
                &topic.chat,
                &topic.topic,
            ))
    }

    pub fn push_pending_images(&self, ws: &WorkspaceId, mut images: Vec<AgentImageInput>) {
        if images.is_empty() {
            return;
        }
        let mut g = self.inner.write().unwrap();
        g.pending_images
            .entry(ws.clone())
            .or_default()
            .append(&mut images);
    }

    pub fn take_pending_images(&self, ws: &WorkspaceId) -> Vec<AgentImageInput> {
        let mut g = self.inner.write().unwrap();
        g.pending_images.remove(ws).unwrap_or_default()
    }
}

fn notification_channel_binding_id(chat: &ChatId) -> ChannelBindingId {
    ChannelBindingId::new(format!("telegram:{}:agent-notifications", chat.as_str()))
}

fn telegram_topic_channel_binding_id(chat: &ChatId, topic: &WorkspaceId) -> ChannelBindingId {
    ChannelBindingId::new(format!("telegram:{}:{}", chat.as_str(), topic.as_str()))
}

fn insert_session(inner: &mut Inner, session: WorkSession, topic: WorkspaceId) {
    let workspace = session.workspace.clone();
    let key = ChannelTopicKey::new(&session.chat, &topic);
    if let Some(old_key) = inner
        .topic_by_workspace
        .insert(workspace.clone(), key.clone())
    {
        inner.workspace_by_topic.remove(&old_key);
    }
    if let Some(old_workspace) = inner.workspace_by_topic.insert(key, workspace.clone()) {
        if old_workspace != session.workspace {
            inner.topic_by_workspace.remove(&old_workspace);
        }
    }
    if let Some(live) = session.live.as_ref() {
        inner
            .by_instance
            .insert(live.session.instance_id().clone(), workspace.clone());
    }
    inner.sessions.insert(workspace, session);
}

#[cfg(test)]
fn default_test_provider_ids() -> Vec<&'static str> {
    static TEST_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime_handle = TEST_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build test runtime")
    });
    let _enter = runtime_handle.enter();
    let runtime = AgentRuntime::new();
    runtime.register_defaults();
    runtime
        .providers()
        .into_iter()
        .map(|provider| provider.id.as_str())
        .collect()
}

#[cfg(test)]
fn test_core_with_provider_ids(
    store: ControlPlaneSqliteStore,
    provider_ids: &[&'static str],
) -> Result<Arc<LucarneCore>, StateStoreError> {
    static TEST_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime_handle = TEST_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build test runtime")
    });
    let _enter = runtime_handle.enter();
    let runtime = Arc::new(AgentRuntime::new());
    for provider_id in provider_ids {
        runtime.register(Arc::new(StateTestProvider { id: *provider_id }));
    }
    LucarneCore::from_runtime_and_store(runtime, store)
        .map_err(|err| StateStoreError::Core(err.to_string()))
}

#[cfg(test)]
struct StateTestProvider {
    id: &'static str,
}

#[cfg(test)]
#[async_trait]
impl AgentProvider for StateTestProvider {
    fn id(&self) -> ProviderId {
        ProviderId::from_static(self.id)
    }

    fn capabilities(&self) -> AgentCapabilities {
        AgentCapabilities::default()
    }

    async fn probe(&self) -> Result<ProbeResult, AgentError> {
        Ok(ProbeResult {
            provider_id: ProviderId::from_static(self.id),
            provider_version: None,
            capabilities: self.capabilities(),
        })
    }

    async fn open(
        &self,
        _req: OpenSession,
    ) -> Result<Box<dyn lucarne::agent_runtime::AgentSession>, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "state test provider cannot open sessions".into(),
        })
    }

    async fn resume(
        &self,
        _req: ResumeSession,
    ) -> Result<Box<dyn lucarne::agent_runtime::AgentSession>, AgentError> {
        Err(AgentError {
            kind: AgentErrorKind::Unsupported,
            message: "state test provider cannot resume sessions".into(),
        })
    }
}

#[cfg(test)]
fn rebuild_sessions_from_core(
    inner: &mut Inner,
    core: &LucarneCore,
    provider_ids: &[&'static str],
) -> Result<(), StateStoreError> {
    inner.sessions.clear();
    inner.topic_by_workspace.clear();
    inner.workspace_by_topic.clear();
    inner.by_instance.clear();

    for workspace in core.workspace_bindings() {
        let Some(provider_id) = provider_id_from(provider_ids, workspace.provider_id.as_str())
        else {
            return Err(StateStoreError::UnknownProvider(
                workspace.provider_id.to_string(),
            ));
        };
        let Some(binding) = core
            .channel_bindings_for_workspace(&workspace.workspace_id)
            .into_iter()
            .find(|binding| binding.channel.as_str() == "telegram" && binding.topic_id.is_some())
        else {
            continue;
        };
        let Some(topic_id) = binding.topic_id.as_ref() else {
            continue;
        };
        let workspace_id = WorkspaceId::new(workspace.workspace_id.as_str());
        let topic = WorkspaceId::new(topic_id.as_str());
        let resume_ref = workspace
            .active_provider_session_id
            .as_ref()
            .and_then(|provider_session_id| core.provider_session_record(provider_session_id))
            .map(|session| session.native_resume_ref.to_string())
            .filter(|resume_ref| !is_provisional_live_resume_ref(resume_ref));
        let project_path = (!workspace.project_path.as_os_str().is_empty())
            .then(|| workspace.project_path.clone());
        let session = WorkSession {
            workspace: workspace_id.clone(),
            chat: ChatId::new(binding.chat_id.as_str()),
            provider_id,
            project_path,
            title: workspace.title.to_string(),
            live: None,
            resume_ref,
        };
        let key = ChannelTopicKey::new(&session.chat, &topic);
        if let Some(old_key) = inner
            .topic_by_workspace
            .insert(workspace_id.clone(), key.clone())
        {
            inner.workspace_by_topic.remove(&old_key);
        }
        inner.workspace_by_topic.insert(key, workspace_id.clone());
        inner.sessions.insert(workspace_id, session);
    }
    Ok(())
}

fn provider_id_from(provider_ids: &[&'static str], id: &str) -> Option<&'static str> {
    provider_ids
        .iter()
        .copied()
        .find(|provider_id| *provider_id == id)
}

fn topic_for_workspace_locked(
    inner: &Inner,
    workspace: &WorkspaceId,
) -> Result<WorkspaceId, StateStoreError> {
    inner
        .topic_by_workspace
        .get(workspace)
        .map(|key| key.topic.clone())
        .ok_or_else(|| StateStoreError::MissingTopicBinding(workspace.as_str().to_string()))
}

fn topic_for_workspace_optional_locked(
    inner: &Inner,
    workspace: &WorkspaceId,
) -> Option<WorkspaceId> {
    inner
        .topic_by_workspace
        .get(workspace)
        .map(|key| key.topic.clone())
}

#[cfg(test)]
fn provider_session_id_for_resume(provider_id: &str, resume_ref: &str) -> ProviderSessionId {
    ProviderSessionId::new(format!("{provider_id}:{resume_ref}"))
}

fn provisional_live_resume_ref(live: &LiveSession) -> String {
    format!("live:{}", live.session.instance_id().0.as_str())
}

fn is_provisional_live_resume_ref(resume_ref: &str) -> bool {
    resume_ref.starts_with("live:")
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

fn control_workspace_id(workspace: &WorkspaceId) -> ControlWorkspaceId {
    ControlWorkspaceId::new(workspace.as_str())
}

fn entry_panel_id(chat: &ChatId) -> PanelRenderId {
    PanelRenderId::new(format!("telegram:{}:entry-panel", chat.as_str()))
}

fn subagent_link_id(
    workspace: &ControlWorkspaceId,
    child_native_ref: Option<&str>,
    child_provider_session_id: Option<&str>,
    fallback_action_id: &str,
) -> SubAgentLinkId {
    let child_key = child_native_ref
        .filter(|value| !value.trim().is_empty())
        .or_else(|| child_provider_session_id.filter(|value| !value.trim().is_empty()));
    match child_key {
        Some(child_key) => SubAgentLinkId::new(format!(
            "{}:subagent:{}",
            workspace.as_str(),
            stable_resume_token("subagent", child_key)
        )),
        None => SubAgentLinkId::new(fallback_action_id),
    }
}

fn subagent_state_from_status(status: Option<&str>, openable: bool) -> SubAgentState {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "starting" | "pending" | "queued" => SubAgentState::Starting,
        "running" | "in_progress" | "started" => SubAgentState::Running,
        "waiting" | "blocked" | "waiting_permission" => SubAgentState::Waiting,
        "completed" | "complete" | "done" | "success" | "succeeded" => SubAgentState::Completed,
        "failed" | "error" => SubAgentState::Failed,
        "stopped" | "cancelled" | "canceled" => SubAgentState::Stopped,
        "unsupported" => SubAgentState::Unsupported,
        _ if openable => SubAgentState::Running,
        _ => SubAgentState::Unsupported,
    }
}

impl crate::turn::SubAgentCallbackRegistry for BotState {
    fn subagent_button_data(&self, target: &WorkspaceHandle, link: &SubAgentLinkRecord) -> String {
        let Some(workspace) = self.workspace_for_handle(target) else {
            return "subagent:c:stale".into();
        };
        self.register_subagent_callback(&workspace, link)
    }
}

impl crate::turn::AgentStatusRecorder for BotState {
    fn record_agent_status(
        &self,
        target: &WorkspaceHandle,
        status: &AgentStatus,
    ) -> Option<StatusSnapshot> {
        let workspace = self.workspace_for_handle(target)?;
        self.record_provider_status(&workspace, status)
    }
}

impl crate::turn::AgentInterventionCallbackRegistry for BotState {
    fn intervention_button_data(
        &self,
        target: &WorkspaceHandle,
        live_instance: &InstanceId,
        req_id: &str,
        action: crate::turn::IntvAction,
    ) -> String {
        let Some(workspace) = self.workspace_for_handle(target) else {
            return "intv:c:stale".into();
        };
        self.register_intervention_callback(&workspace, live_instance, req_id, action)
    }
}

impl crate::turn::TurnEventRecorder for BotState {
    fn append_turn_timeline(
        &self,
        target: &WorkspaceHandle,
        turn_id: &TurnId,
        kind: TimelineKindProjection,
        payload: serde_json::Value,
    ) -> Result<TimelineProjectionItem, String> {
        let workspace = self
            .workspace_for_handle(target)
            .ok_or_else(|| format!("missing topic binding for {}", target.workspace.as_str()))?;
        self.append_turn_timeline(&workspace, turn_id, kind, payload)
    }

    fn record_subagent_tool_call(
        &self,
        target: &WorkspaceHandle,
        turn_id: &TurnId,
        provider_item_id: Option<&str>,
        input: &serde_json::Value,
    ) {
        let result = self
            .workspace_for_handle(target)
            .ok_or_else(|| format!("missing topic binding for {}", target.workspace.as_str()))
            .and_then(|workspace| {
                self.record_subagent_tool_call(&workspace, turn_id, provider_item_id, input)
            });
        if let Err(err) = result {
            tracing::warn!(
                target: "lucarne_telegram::state",
                error = %err,
                "failed to record control-plane subagent action"
            );
        }
    }

    fn record_turn_usage(&self, turn_id: &TurnId, usage: lucarne::agent_runtime::UsageEvent) {
        if let Err(err) = self.record_turn_usage(turn_id, usage) {
            tracing::warn!(
                target: "lucarne_telegram::state",
                error = %err,
                "failed to record control-plane turn usage"
            );
        }
    }

    fn mark_turn_waiting_permission(&self, turn_id: &TurnId) {
        if let Err(err) = self.mark_turn_waiting_permission(turn_id) {
            tracing::warn!(
                target: "lucarne_telegram::state",
                error = %err,
                "failed to mark control-plane turn waiting for permission"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use lucarne::agent_runtime::{
        AgentError, AgentInput, AgentSession, InterventionResponse, SessionId,
    };
    use rusqlite::Connection;
    use std::path::Path;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    #[test]
    fn sqlite_state_reloads_workspace_bindings_without_live_session() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");

        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex · new".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist session");
        state
            .rename(&workspace, "renamed".into())
            .expect("persist rename");
        state
            .mark_live_dead(&workspace, Some("thread-2".into()))
            .expect("persist resume ref");

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        let session = reloaded.get(&workspace).expect("restored workspace");
        assert_eq!(session.chat.as_str(), "100");
        assert_eq!(session.workspace.as_str(), "2");
        assert_eq!(session.provider_id, "codex");
        assert_eq!(
            session.project_path.as_deref(),
            Some(Path::new("/tmp/project"))
        );
        assert_eq!(session.title, "renamed");
        assert_eq!(session.resume_ref.as_deref(), Some("thread-2"));
        assert!(session.live.is_none());

        let revision = reloaded
            .workspace_revision(&workspace)
            .expect("control-plane workspace revision");
        assert!(revision.get() > 0);

        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("control-plane status snapshot");
        assert_eq!(
            snapshot.workspace_id,
            Some(lucarne::control_plane::WorkspaceId::new("2"))
        );
        assert_eq!(snapshot.provider_id.as_deref(), Some("codex"));
        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-2"))
        );
        assert_eq!(snapshot.native_resume_ref.as_deref(), Some("thread-2"));
        assert_eq!(snapshot.live_instance_state, None);
        assert_eq!(
            snapshot.project_path.as_deref(),
            Some(Path::new("/tmp/project"))
        );

        let binding = reloaded
            .telegram_channel_binding(&workspace)
            .expect("control-plane channel binding");
        assert_eq!(
            binding.workspace_id,
            lucarne::control_plane::WorkspaceId::new("2")
        );
        assert_eq!(binding.channel.as_str(), "telegram");
        assert_eq!(binding.chat_id.as_str(), "100");
        assert_eq!(binding.topic_id.as_deref(), Some("2"));
    }

    #[test]
    fn sqlite_state_can_open_from_shared_control_plane_store() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let store = ControlPlaneSqliteStore::open(&db).expect("open shared store");
        let workspace = WorkspaceId::new("shared-store");

        let state = BotState::open_sqlite_store_with_provider_ids(store.clone(), vec!["codex"])
            .expect("open state from shared store");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex · shared".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist session");

        let reloaded = BotState::open_sqlite_store_with_provider_ids(store, vec!["codex"])
            .expect("reload state from shared store");
        let session = reloaded.get(&workspace).expect("restored workspace");
        assert_eq!(session.title, "codex · shared");
        assert_eq!(session.provider_id, "codex");
    }

    #[test]
    fn entry_panel_render_revision_persists_in_control_plane() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let state = BotState::open_sqlite(&db).expect("open state db");
        let revision = state.set_panel(PanelSnapshot {
            view: PanelView::Overview,
            provider_filter: None,
            workspace_filter: None,
            agents: vec!["codex".into()],
            history: vec![3],
            workspaces: vec![WorkspaceId::new("workspace-a")],
            history_workspaces: Vec::new(),
            history_offset: 3,
            history_total: 10,
        });
        state
            .record_entry_panel_render(&ChatId::new("100"), &MessageId::new("42"), revision)
            .expect("persist panel render");
        state
            .record_entry_panel_stale_revision(&ChatId::new("100"), Revision::new(0))
            .expect("persist stale panel revision");

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        let panel = reloaded
            .entry_panel_render(&ChatId::new("100"))
            .expect("panel render record");
        assert_eq!(panel.message_id.as_deref(), Some("42"));
        assert_eq!(panel.last_rendered_revision, revision);
        assert_eq!(panel.last_observed_stale_revision, Some(Revision::new(0)));
        assert_eq!(
            panel.last_reconcile_outcome,
            Some(ReconcileOutcome::StaleRevision)
        );
        assert_eq!(
            reloaded.panel_revision(),
            Revision::new(revision.get() + 1),
            "reloaded state must not trust a pre-restart entry panel snapshot"
        );
    }

    #[test]
    fn sqlite_state_uses_runtime_provider_catalog() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");

        let state = BotState::open_sqlite_with_provider_ids(&db, vec!["copilot"])
            .expect("open state with runtime provider catalog");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "copilot",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "copilot".into(),
                live: None,
                resume_ref: Some("session-1".into()),
            })
            .expect("persist runtime provider session");

        let reloaded = BotState::open_sqlite_with_provider_ids(&db, vec!["copilot"])
            .expect("reload state with runtime provider catalog");
        let session = reloaded.get(&workspace).expect("restored workspace");
        assert_eq!(session.provider_id, "copilot");
        assert_eq!(session.resume_ref.as_deref(), Some("session-1"));

        let err = match BotState::open_sqlite(&db) {
            Ok(_) => panic!("default catalog must not invent copilot"),
            Err(err) => err,
        };
        assert!(matches!(err, StateStoreError::UnknownProvider(provider) if provider == "copilot"));
    }

    #[tokio::test]
    async fn sqlite_state_removes_workspace_binding() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");
        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex old".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");

        let removed = state.remove(&workspace).await.expect("remove workspace");

        assert!(removed.is_some());
        assert!(state.get(&workspace).is_none());
        assert!(BotState::open_sqlite(&db)
            .expect("reload state db")
            .get(&workspace)
            .is_none());
    }

    #[test]
    fn sqlite_state_rejects_unsupported_provider_ids() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let state = BotState::open_sqlite(&db).expect("open state db");

        let err = state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("2"),
                chat: ChatId::new("100"),
                provider_id: "copilot",
                project_path: None,
                title: "copilot · old".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect_err("unsupported provider should fail");

        assert!(matches!(
            err,
            StateStoreError::UnknownProvider(provider) if provider == "copilot"
        ));
    }

    #[test]
    fn sqlite_state_ignores_legacy_work_sessions_without_control_plane_truth() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        {
            let conn = Connection::open(&db).expect("open legacy db");
            conn.execute_batch(
                "CREATE TABLE work_sessions (
                    workspace_id TEXT PRIMARY KEY NOT NULL,
                    chat_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    project_path TEXT,
                    title TEXT NOT NULL,
                    resume_ref TEXT
                );",
            )
            .expect("create legacy schema");
            conn.execute(
                "INSERT INTO work_sessions
                    (workspace_id, chat_id, provider_id, project_path, title, resume_ref)
                 VALUES ('old', '100', 'codex', '/tmp/project', 'legacy', 'thread-1')",
                [],
            )
            .expect("insert legacy row");
        }

        let state = BotState::open_sqlite(&db).expect("legacy table is not truth");
        assert!(state.all().is_empty());
        let table_count: i64 = Connection::open(&db)
            .expect("open persisted db")
            .query_row(
                "SELECT COUNT(*)
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'work_sessions'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite schema");
        assert_eq!(table_count, 0);
    }

    #[test]
    fn handle_for_session_requires_topic_binding() {
        let state = BotState::new();
        let session = WorkSession {
            workspace: WorkspaceId::new("2"),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex".into(),
            live: None,
            resume_ref: Some("thread-1".into()),
        };
        state
            .inner
            .write()
            .unwrap()
            .sessions
            .insert(session.workspace.clone(), session.clone());

        let err = state
            .handle_for_session(&session)
            .expect_err("missing topic binding must fail closed");

        assert!(matches!(
            err,
            StateStoreError::MissingTopicBinding(workspace) if workspace == "2"
        ));
    }

    #[test]
    fn hydrate_unbound_control_workspace_restores_topic_session_mapping() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .core
            .upsert_workspace_binding(
                control_workspace_id(&workspace),
                OpenWorkspaceRequest {
                    provider_id: "codex",
                    project_path: Some(PathBuf::from("/tmp/project")),
                    title: "codex".into(),
                },
                Some("thread-1"),
            )
            .expect("persist workspace binding");
        state
            .core
            .upsert_channel_binding(ChannelBinding::new(
                ChannelBindingId::new("telegram:100:9"),
                control_workspace_id(&workspace),
                "telegram",
                "100",
                Some("9"),
            ))
            .expect("persist topic binding");

        state
            .hydrate_unbound_control_workspaces(&ChatId::new("100"))
            .expect("hydrate control workspaces");

        let from_topic = state
            .workspace_for_topic(&WorkspaceId::new("9"))
            .and_then(|workspace| state.get(&workspace))
            .expect("topic resolves through explicit binding");
        assert_eq!(from_topic.workspace, workspace);
        assert_eq!(
            state.workspace_for_topic(&WorkspaceId::new("9")),
            Some(workspace)
        );
        let handle = state
            .handle_for_session(&from_topic)
            .expect("hydrated session keeps topic binding");
        assert_eq!(handle.workspace.as_str(), "9");
    }

    #[test]
    fn sqlite_persistence_ignores_runtime_session_cache_without_control_plane_record() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let state = BotState::open_sqlite(&db).expect("open state db");
        let session = WorkSession {
            workspace: WorkspaceId::new("2"),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex".into(),
            live: None,
            resume_ref: Some("thread-1".into()),
        };
        {
            let mut inner = state.inner.write().unwrap();
            inner.sessions.insert(session.workspace.clone(), session);
        }
        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        assert!(reloaded.get(&WorkspaceId::new("2")).is_none());
    }

    #[test]
    fn sqlite_state_writes_indexed_control_plane_entities() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert_with_topic(
                WorkSession {
                    workspace: WorkspaceId::new("2"),
                    chat: ChatId::new("100"),
                    provider_id: "codex",
                    project_path: Some(PathBuf::from("/tmp/project")),
                    title: "codex".into(),
                    live: None,
                    resume_ref: Some("thread-1".into()),
                },
                WorkspaceId::new("9"),
            )
            .expect("persist workspace");

        let conn = Connection::open(&db).expect("open persisted db");
        let workspace_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM control_plane_entities
                 WHERE kind = 'workspace' AND workspace_id = '2'",
                [],
                |row| row.get(0),
            )
            .expect("workspace entity count");
        let channel_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM control_plane_entities
                 WHERE kind = 'channel_binding' AND workspace_id = '2'",
                [],
                |row| row.get(0),
            )
            .expect("channel entity count");

        assert_eq!(workspace_rows, 1);
        assert_eq!(channel_rows, 1);
    }

    #[test]
    fn topic_rebind_replaces_stale_channel_binding_for_workspace() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        let session = WorkSession {
            workspace: workspace.clone(),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex".into(),
            live: None,
            resume_ref: Some("thread-1".into()),
        };

        state
            .upsert_with_topic(session.clone(), WorkspaceId::new("9"))
            .expect("first topic binding");
        state
            .upsert_with_topic(session, WorkspaceId::new("10"))
            .expect("replacement topic binding");

        let bindings = state
            .core
            .channel_bindings_for_workspace(&control_workspace_id(&workspace));

        assert_eq!(bindings.len(), 1, "{bindings:?}");
        assert_eq!(bindings[0].topic_id.as_deref(), Some("10"));
    }

    #[test]
    fn append_turn_timeline_persists_incrementally_without_full_rewrite() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let state = BotState::open_sqlite(&db).expect("open state db");
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let turn = state
            .start_user_turn(&workspace, &live, "hello", None)
            .expect("start turn");

        {
            let conn = Connection::open(&db).expect("open persisted db");
            conn.execute_batch(
                "CREATE TRIGGER forbid_timeline_delete
                 BEFORE DELETE ON control_plane_entities
                 WHEN OLD.kind = 'timeline'
                 BEGIN
                   SELECT RAISE(FAIL, 'full rewrite is not allowed for timeline append');
                 END;
                 CREATE TRIGGER forbid_timeline_rewrite
                 BEFORE UPDATE OF state_json ON control_plane_entities
                 WHEN OLD.kind = 'timeline'
                 BEGIN
                   SELECT RAISE(FAIL, 'existing timeline rows must not be rewritten');
                 END;",
            )
            .expect("create delete guard");
        }

        let _reloaded =
            BotState::open_sqlite(&db).expect("startup must not rewrite persisted timeline rows");
        state
            .append_turn_timeline(
                &workspace,
                &turn.turn_id,
                TimelineKindProjection::Assistant,
                serde_json::json!({ "text": "world" }),
            )
            .expect("append timeline incrementally");
        state.register_command_callback(&workspace, 1, "status", "");
        state
            .complete_turn(&turn)
            .expect("complete turn without rewriting timeline rows");
    }

    #[test]
    fn command_callback_is_stale_after_workspace_session_rebind() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");
        let callback_payload = state.register_command_callback(&workspace, 1, "status", "");
        let token = callback_payload
            .strip_prefix("agentcmd:c:")
            .expect("state-backed callback")
            .to_string();

        let live = test_live_session("codex", "thread-2", "instance-2");
        state
            .bind_live(&workspace, live, Some("thread-2".into()))
            .expect("rebind provider session");

        assert!(
            state.resolve_command_callback(&token).is_none(),
            "old command button must not invoke against a rebound provider session"
        );
    }

    #[test]
    fn history_older_callback_is_stale_after_workspace_session_rebind() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert_with_topic(
                WorkSession {
                    workspace: workspace.clone(),
                    chat: ChatId::new("100"),
                    provider_id: "codex",
                    project_path: Some(PathBuf::from("/tmp/project")),
                    title: "codex".into(),
                    live: None,
                    resume_ref: Some("thread-1".into()),
                },
                WorkspaceId::new("topic-2"),
            )
            .expect("insert workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live, Some("thread-1".into()))
            .expect("bind provider session");
        let callback_payload = state
            .register_history_older_callback(
                &workspace,
                "codex",
                "thread-1",
                PathBuf::from("/tmp/thread-1.jsonl"),
                "history-before-byte:2",
            )
            .expect("register history older callback");
        let token = callback_payload
            .strip_prefix("historyolder:c:")
            .expect("state-backed history callback")
            .to_string();

        assert!(
            state.resolve_history_older_callback(&token).is_some(),
            "history callback should resolve before the provider session changes"
        );

        let rebound = test_live_session("codex", "thread-2", "instance-2");
        state
            .bind_live(&workspace, rebound, Some("thread-2".into()))
            .expect("rebind provider session");

        assert!(
            state.resolve_history_older_callback(&token).is_none(),
            "old history older button must not replay an old session after rebind"
        );
    }

    #[test]
    fn command_callback_is_stale_after_workspace_revision_change() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");
        let callback_payload = state.register_command_callback(&workspace, 1, "status", "");
        let token = callback_payload
            .strip_prefix("agentcmd:c:")
            .expect("state-backed callback")
            .to_string();

        state
            .rename(&workspace, "renamed".into())
            .expect("revision change");

        assert!(
            state.resolve_command_callback(&token).is_none(),
            "old command button must not survive a workspace revision change"
        );
    }

    #[test]
    fn waiting_permission_restarts_as_permission_orphaned() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");
        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let turn = state
            .start_user_turn(&workspace, &live, "needs approval", None)
            .expect("start turn");
        state
            .mark_turn_waiting_permission(&turn.turn_id)
            .expect("mark waiting permission");
        let callback_payload = state.register_intervention_callback(
            &workspace,
            live.session.instance_id(),
            "req-1",
            crate::turn::IntvAction::Approve { allow: true },
        );
        let token = callback_payload
            .strip_prefix("intv:c:")
            .expect("state-backed intervention callback");
        assert!(state.resolve_intervention_callback(token).is_some());

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("status snapshot after restart");
        assert_eq!(
            snapshot.last_reconcile_outcome,
            Some(ReconcileOutcome::PermissionOrphaned)
        );
        assert!(
            reloaded.resolve_intervention_callback(token).is_none(),
            "permission callback must not resolve after its live instance was orphaned"
        );
    }

    #[test]
    fn intervention_callback_is_removed_when_turn_fails() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let turn = state
            .start_user_turn(&workspace, &live, "needs approval", None)
            .expect("start turn");
        state
            .mark_turn_waiting_permission(&turn.turn_id)
            .expect("mark waiting permission");
        let callback_payload = state.register_intervention_callback(
            &workspace,
            live.session.instance_id(),
            "req-1",
            crate::turn::IntvAction::Approve { allow: true },
        );
        let token = callback_payload
            .strip_prefix("intv:c:")
            .expect("state-backed intervention callback")
            .to_string();
        assert!(state.resolve_intervention_callback(&token).is_some());

        state
            .fail_turn(&turn, "permission timed out")
            .expect("fail turn");

        assert!(
            state.resolve_intervention_callback(&token).is_none(),
            "stale approval button must not survive turn failure"
        );
    }

    #[test]
    fn planning_activation_does_not_record_reconcile_outcome() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let session = state.get(&workspace).expect("workspace session");

        state
            .plan_activation_for_session(&session)
            .expect("plan activation");

        let snapshot = state.status_snapshot(&workspace).expect("status snapshot");
        assert_eq!(snapshot.last_reconcile_outcome, None);
    }

    #[test]
    fn bind_live_and_clear_update_control_plane_status() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex · new".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("insert workspace");

        let live = test_live_session("codex", "session-ignored", "instance-1");
        state
            .bind_live(&workspace, live, Some("thread-2".into()))
            .expect("bind live");

        let snapshot = state
            .status_snapshot(&workspace)
            .expect("control-plane live snapshot");
        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-2"))
        );
        assert_eq!(snapshot.native_resume_ref.as_deref(), Some("thread-2"));
        assert_eq!(
            snapshot.live_instance_state,
            Some(lucarne::control_plane::LiveInstanceState::Idle)
        );

        state
            .mark_live_dead(&workspace, Some("thread-3".into()))
            .expect("mark live dead");
        let snapshot = state
            .status_snapshot(&workspace)
            .expect("control-plane dead snapshot");
        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-3"))
        );
        assert_eq!(snapshot.native_resume_ref.as_deref(), Some("thread-3"));
        assert_eq!(snapshot.live_instance_state, None);

        state
            .clear_live_and_resume_ref(&workspace)
            .expect("clear live and resume ref");
        let snapshot = state
            .status_snapshot(&workspace)
            .expect("control-plane cleared snapshot");
        assert_eq!(snapshot.provider_session_id, None);
        assert_eq!(snapshot.live_instance_state, None);
    }

    #[test]
    fn bind_live_without_resume_ref_seeds_provisional_provider_session() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "claude",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "claude · new".into(),
                live: None,
                resume_ref: None,
            })
            .expect("insert workspace");

        let live = test_live_session("claude", "runtime-session", "instance-1");
        state
            .bind_live(&workspace, live.clone(), None)
            .expect("bind live without provider-native resume ref");

        let snapshot = state
            .status_snapshot(&workspace)
            .expect("control-plane provisional live snapshot");
        assert_eq!(
            snapshot.native_resume_ref.as_deref(),
            Some("live:instance-1")
        );
        assert_eq!(
            snapshot.live_instance_state,
            Some(lucarne::control_plane::LiveInstanceState::Idle)
        );
        state
            .start_user_turn(&workspace, &live, "hello", None)
            .expect("first user turn can start before provider-native id surfaces");
    }

    #[test]
    fn sqlite_state_reloads_control_plane_runtime_records() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");

        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let turn = state
            .start_user_turn(&workspace, &live, "hello", Some(&MessageId::new("123")))
            .expect("start turn");
        state
            .append_turn_timeline(
                &workspace,
                &turn.turn_id,
                TimelineKindProjection::Assistant,
                serde_json::json!({ "text": "hi" }),
            )
            .expect("append assistant timeline");
        state.complete_turn(&turn).expect("complete turn");
        state
            .record_provider_status(
                &workspace,
                &AgentStatus {
                    version: Some("0.125.0".into()),
                    model: Some("gpt-5.5".into()),
                    reasoning: Some("xhigh".into()),
                    permissions: Some("Default".into()),
                    ..Default::default()
                },
            )
            .expect("record provider status");
        let callback_payload = state.register_command_callback_values(
            &workspace,
            7,
            "fork",
            "msg_01abcdefghijklmnopqrstuvwxyz",
            serde_json::json!({ "target_id": "msg_01abcdefghijklmnopqrstuvwxyz" }),
        );
        let callback_token = callback_payload
            .strip_prefix("agentcmd:c:")
            .expect("short callback payload")
            .to_string();

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");

        assert_eq!(
            reloaded.timeline_kinds(&workspace),
            vec![
                TimelineKindProjection::User,
                TimelineKindProjection::Assistant
            ]
        );
        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("reloaded status snapshot");
        assert_eq!(snapshot.provider_version.as_deref(), Some("0.125.0"));
        assert_eq!(snapshot.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(snapshot.reasoning.as_deref(), Some("xhigh"));
        assert_eq!(snapshot.permission_mode.as_deref(), Some("Default"));
        assert!(
            reloaded.resolve_command_callback(&callback_token).is_none(),
            "restart marks the live instance stale, so old command buttons must refresh"
        );
    }

    #[test]
    fn sqlite_reload_rebuilds_sessions_from_control_plane_projection() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");
        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        state
            .rename(&workspace, "renamed".into())
            .expect("rename workspace");
        state
            .mark_live_dead(&workspace, Some("thread-2".into()))
            .expect("persist provider session ref");
        drop(state);

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        let session = reloaded.get(&workspace).expect("restored workspace");

        assert_eq!(session.title, "renamed");
        assert_eq!(
            session.project_path.as_deref(),
            Some(Path::new("/tmp/project"))
        );
        assert_eq!(session.resume_ref.as_deref(), Some("thread-2"));
        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("status snapshot");
        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-2"))
        );
        assert_eq!(snapshot.native_resume_ref.as_deref(), Some("thread-2"));
        let table_count: i64 = Connection::open(&db)
            .expect("open persisted db")
            .query_row(
                "SELECT COUNT(*)
                 FROM sqlite_master
                 WHERE type = 'table' AND name = 'work_sessions'",
                [],
                |row| row.get(0),
            )
            .expect("query sqlite schema");
        assert_eq!(table_count, 0);
    }

    #[test]
    fn no_output_ack_completes_command_turn_without_orphaning_live() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let workflow = state
            .start_command_workflow(
                &workspace,
                &live,
                "quiet",
                None,
                serde_json::Value::Null,
                Revision::new(1),
                CommandCompletionPolicy::NoOutputAck,
                None,
            )
            .expect("start no-output command");

        state
            .complete_command_workflow(
                &workflow,
                CommandCompletionPolicy::NoOutputAck,
                serde_json::json!({ "name": "quiet" }),
            )
            .expect("ack no-output command");

        let turn = state
            .core
            .turn_record(&workflow.turn_id)
            .expect("stored command turn");
        assert_eq!(turn.state, lucarne::control_plane::TurnState::Completed);
        let command = state
            .core
            .command_workflow(&workflow.command_id)
            .expect("stored command workflow");
        assert_eq!(
            command.state,
            lucarne::control_plane::CommandState::Completed
        );
        let live_record = state
            .core
            .live_instance_record(&LiveInstanceId::new("instance-1"))
            .expect("stored live instance");
        assert_eq!(
            live_record.state,
            lucarne::control_plane::LiveInstanceState::Idle
        );
        assert_eq!(live_record.active_turn_id, None);
    }

    #[test]
    fn turn_and_command_start_from_control_plane_provider_session() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        {
            let mut inner = state.inner.write().unwrap();
            inner
                .sessions
                .get_mut(&workspace)
                .expect("runtime session cache")
                .resume_ref = Some("stale-thread".into());
        }

        let turn = state
            .start_user_turn(&workspace, &live, "hello", None)
            .expect("start user turn from control-plane provider session");
        let stored = state
            .core
            .turn_record(&turn.turn_id)
            .expect("stored user turn");
        assert_eq!(
            stored.provider_session_id,
            provider_session_id_for_resume("codex", "thread-1")
        );
        state.complete_turn(&turn).expect("complete user turn");

        let workflow = state
            .start_command_workflow(
                &workspace,
                &live,
                "status",
                None,
                serde_json::Value::Null,
                Revision::new(7),
                CommandCompletionPolicy::CommandResult,
                None,
            )
            .expect("start command from control-plane provider session");
        let stored = state
            .core
            .turn_record(&workflow.turn_id)
            .expect("stored command turn");
        assert_eq!(
            stored.provider_session_id,
            provider_session_id_for_resume("codex", "thread-1")
        );
        let command = state
            .core
            .command_workflow(&workflow.command_id)
            .expect("stored command workflow");
        assert_eq!(command.catalog_revision, Revision::new(7));
    }

    #[test]
    fn upsert_with_stale_resume_ref_does_not_replace_active_provider_session() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live, Some("thread-1".into()))
            .expect("bind live");

        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("stale-thread".into()),
            })
            .expect("stale projection upsert");

        let snapshot = state
            .status_snapshot(&workspace)
            .expect("control-plane snapshot");
        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-1"))
        );
    }

    #[test]
    fn complete_turn_records_completion_usage() {
        let state = BotState::new();
        let workspace = WorkspaceId::new("2");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live.clone(), Some("thread-1".into()))
            .expect("bind live");
        let turn = state
            .start_user_turn(&workspace, &live, "hello", Some(&MessageId::new("123")))
            .expect("start turn");
        let usage = lucarne::agent_runtime::UsageEvent {
            input_tokens: Some(10),
            output_tokens: Some(2),
            total_tokens: Some(12),
            raw: serde_json::json!({"input_tokens": 10, "output_tokens": 2}),
        };

        state
            .complete_turn_with_usage(&turn, Some(usage.clone()))
            .expect("complete turn with usage");

        let stored_usage = state
            .core
            .turn_record(&turn.turn_id)
            .expect("stored turn")
            .usage
            .clone();
        assert_eq!(stored_usage, serde_json::to_value(&usage).unwrap());
    }

    #[test]
    fn sqlite_reload_detaches_non_resumable_live_instances() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");

        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let live = test_live_session("codex", "thread-1", "instance-1");
        state
            .bind_live(&workspace, live, Some("thread-1".into()))
            .expect("bind live");
        assert_eq!(
            state
                .status_snapshot(&workspace)
                .expect("live snapshot")
                .live_instance_state,
            Some(lucarne::control_plane::LiveInstanceState::Idle)
        );

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("reloaded snapshot");

        assert_eq!(
            snapshot.provider_session_id,
            Some(provider_session_id_for_resume("codex", "thread-1"))
        );
        assert_eq!(snapshot.native_resume_ref.as_deref(), Some("thread-1"));
        assert_eq!(snapshot.live_instance_state, None);
        assert_eq!(
            snapshot.last_reconcile_outcome,
            Some(ReconcileOutcome::LiveInstanceStale)
        );
    }

    #[test]
    fn sqlite_reload_does_not_repair_control_plane_from_runtime_session_cache() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");

        let state = BotState::open_sqlite(&db).expect("open state db");
        state
            .upsert_with_topic(
                WorkSession {
                    workspace: workspace.clone(),
                    chat: ChatId::new("100"),
                    provider_id: "codex",
                    project_path: Some(PathBuf::from("/tmp/project")),
                    title: "codex".into(),
                    live: None,
                    resume_ref: Some("thread-1".into()),
                },
                WorkspaceId::new("9"),
            )
            .expect("persist workspace");
        {
            state
                .core
                .rename_workspace_projection(&control_workspace_id(&workspace), "stale title")
                .expect("damage workspace title");
            state
                .core
                .remove_channel_binding(&ChannelBindingId::new("telegram:100:9"))
                .expect("damage channel binding");
        }

        let reloaded = BotState::open_sqlite(&db).expect("reload state db");
        assert!(reloaded.get(&workspace).is_none());
        let snapshot = reloaded
            .status_snapshot(&workspace)
            .expect("status snapshot after reload");

        assert_eq!(snapshot.channel_binding_id, None);
        assert_eq!(snapshot.channel.as_deref(), None);
        assert_eq!(snapshot.chat_id.as_deref(), None);
        assert_eq!(snapshot.topic_id.as_deref(), None);
        let workspace_record = reloaded
            .core
            .workspace_binding(&control_workspace_id(&workspace))
            .expect("workspace record");
        assert_eq!(workspace_record.title.as_str(), "stale title");
        assert_eq!(workspace_record.provider_id.as_str(), "codex");
        assert_eq!(workspace_record.project_path, PathBuf::from("/tmp/project"));
    }

    fn test_live_session(
        provider_id: &'static str,
        session_id: &str,
        instance_id: &str,
    ) -> Arc<LiveSession> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        Arc::new(LiveSession {
            session: Arc::new(TestSession {
                provider_id,
                id: SessionId(session_id.into()),
                instance_id: InstanceId(instance_id.into()),
            }),
            events: AsyncMutex::new(CoreWorkspaceEventStream::new(
                control_workspace_id(&WorkspaceId::new(session_id)),
                rx,
            )),
            pending_intv: Mutex::new(HashMap::new()),
        })
    }

    struct TestSession {
        provider_id: &'static str,
        id: SessionId,
        instance_id: InstanceId,
    }

    #[async_trait]
    impl AgentSession for TestSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static(self.provider_id)
        }

        async fn submit(&self, _input: AgentInput) -> std::result::Result<(), AgentError> {
            Ok(())
        }

        async fn interrupt(&self) -> std::result::Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            _req_id: &str,
            _response: InterventionResponse,
        ) -> std::result::Result<(), AgentError> {
            Ok(())
        }

        async fn take_events(&self) -> std::result::Result<AgentEventStream, AgentError> {
            let (_tx, rx) = mpsc::channel(1);
            Ok(rx)
        }

        async fn close(&self) -> std::result::Result<(), AgentError> {
            Ok(())
        }
    }
}
