//! Bot flow: fixed entry panel + working sessions.
//!
//! The bot owns three concerns and nothing more:
//!
//! 1. Rendering and refreshing the **entry panel** in the fixed entry
//!    chat (agents, paginated history, management buttons).
//! 2. Handling **button clicks** from the entry panel to create (or
//!    jump to) a working-session workspace.
//! 3. Forwarding text / attachments posted inside a working workspace
//!    to the daemon core and projecting normalized turn output back to
//!    the channel.

use futures::StreamExt;
#[cfg(test)]
use lucarne::agent_runtime::{ProviderId, SessionRef};
use lucarne::core_service::{
    render_agent_resource_snapshot, render_kill_agent_report, AgentResourceScope, CoreEvent,
    InterruptTurnRequest, KillAgentRequest, KillAgentTarget, LucarneCore, ObservedAgentSession,
    OpenWorkspaceRequest, ResumeWorkspaceRequest, SystemSettings,
};
use lucarne::core_service::{HistoryProviderCatalogEntry, ProviderCatalogEntry};
use lucarne::event::{
    CommandResultData as EventCommandResultData, CommandResultPayload as EventCommandResultPayload,
};
use lucarne::{
    agent_runtime::{
        AgentCommand, AgentCommandCatalog, AgentCommandCompletion, AgentCommandInput,
        AgentCommandResult, AgentCommandResultData, AgentCommandSource, AgentError, AgentErrorKind,
        AgentForkResult, AgentForkSelection, AgentImageInput, AgentInput, AgentModelSelection,
        AgentPermissionSelection, ApprovalDecision, Event as AgentEvent, InstanceId,
        InterventionResponse, MessageRole, QuestionAnswer, QuestionResponse,
    },
    control_plane::{
        command_from_catalog, command_help_requested, command_usage, plan_command_invocation,
        resolve_fork_session_refs, ActivationPlan, CommandCompletionPolicy, CommandInvocationPlan,
        CommandPlanError, ForkSessionResolution, ForkSessionResolutionError, HistoryReplayRecord,
        ReconcileOutcome, Revision, SubAgentLinkRecord, TimelineItem, TimelineItemKind, TurnId,
        TurnPermit, TurnScheduler,
    },
};
use lucarne_adapter::{
    GlobalConfigPersistence, GlobalConfigUpdate, SystemNotification, SystemNotificationBus,
    SystemNotificationReceiver,
};
use lucarne_channel::{
    agent_message::{render_agent_message_markdown, AgentMessageFooter},
    ingest,
    robust::{send_with_fallback, send_with_fallback_all},
    types::{
        ChannelError, ChannelEvent, ChatId, CommandQuery, CommandQueryResult, FileUpload,
        IncomingAttachment, IncomingMessage, MessageId, OutgoingButton, OutgoingMessage,
        WorkspaceHandle, WorkspaceId,
    },
    Channel,
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex as AsyncMutex;
use tracing::{debug, info, instrument, warn};

use crate::{
    agents,
    channel::TelegramBotCommand,
    state::{
        BotState, LiveSession, PanelSnapshot, PanelView, RunningCommandWorkflow, RunningTurn,
        WorkSession,
    },
    turn,
};
use base64::Engine;
use lucarne::history::{HistoryCursor, HistoryEntry, HistoryMessage, HistoryWorkspace};

/// Maximum number of rows shown by any list in the entry panel.
const PANEL_LIST_LIMIT: usize = 5;
const HISTORY_REPLAY_LIMIT: usize = 10;
const HISTORY_USER_MARKER: &str = "👤 ";
const MAX_IMAGE_BYTES: u64 = 20 * 1024 * 1024;
const NOTIFICATION_TOPIC_TITLE: &str = "agent notifications";
const RECENT_UNBOUND_TOPIC_CREATION_LIMIT: usize = 32;

const TELEGRAM_MENU_COMMANDS: &[TelegramBotCommand] = &[
    TelegramBotCommand {
        command: "start",
        description: "Open the management panel",
    },
    TelegramBotCommand {
        command: "panel",
        description: "Refresh the management panel",
    },
    TelegramBotCommand {
        command: "help",
        description: "Show command help",
    },
    TelegramBotCommand {
        command: "config",
        description: "Show or set scoped config",
    },
    TelegramBotCommand {
        command: "clear_workspaces",
        description: "Clear saved workspace records",
    },
    TelegramBotCommand {
        command: "reset_notifications",
        description: "Recreate the agent notifications topic",
    },
    TelegramBotCommand {
        command: "rename",
        description: "Rename the current workspace",
    },
    TelegramBotCommand {
        command: "commands",
        description: "List or invoke agent commands",
    },
    TelegramBotCommand {
        command: "model",
        description: "Show or set the agent model",
    },
    TelegramBotCommand {
        command: "permissions",
        description: "Show or set agent permissions",
    },
    TelegramBotCommand {
        command: "skills",
        description: "List available agent skills",
    },
    TelegramBotCommand {
        command: "status",
        description: "Show agent status and process resources",
    },
    TelegramBotCommand {
        command: "interrupt",
        description: "Interrupt the current turn",
    },
    TelegramBotCommand {
        command: "kill",
        description: "Kill an agent process",
    },
    TelegramBotCommand {
        command: "new",
        description: "Start a new agent conversation",
    },
    TelegramBotCommand {
        command: "quit",
        description: "Close the current live session",
    },
    TelegramBotCommand {
        command: "fork",
        description: "Fork the current conversation",
    },
];

pub fn telegram_menu_commands() -> &'static [TelegramBotCommand] {
    TELEGRAM_MENU_COMMANDS
}

enum PanelDelivery {
    Send,
    Edit(MessageId),
}

/// The running bot. Parameterised only on the [`Channel`] and shared daemon core.
pub struct Bot {
    channel: Arc<dyn Channel>,
    state: Arc<BotState>,
    core: Arc<LucarneCore>,
    entry: WorkspaceHandle,
    turn_scheduler: TurnScheduler,
    notification_handle_lock: AsyncMutex<()>,
    workspace_topic_repair_lock: AsyncMutex<()>,
    recent_unbound_topic_creations: AsyncMutex<Vec<(String, String)>>,
    global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
}

#[derive(Clone)]
enum TurnRecorderScope {
    TopicHandle,
    Workspace(WorkspaceId),
}

impl TurnRecorderScope {
    fn recorder(&self, state: &Arc<BotState>) -> Arc<dyn turn::TurnEventRecorder> {
        match self {
            Self::TopicHandle => Arc::clone(state) as Arc<dyn turn::TurnEventRecorder>,
            Self::Workspace(workspace) => Arc::new(WorkspaceTurnRecorder {
                state: Arc::clone(state),
                workspace: workspace.clone(),
            }) as Arc<dyn turn::TurnEventRecorder>,
        }
    }

    fn intervention_callback_registry(
        &self,
        state: &Arc<BotState>,
    ) -> Option<Arc<dyn turn::AgentInterventionCallbackRegistry>> {
        match self {
            Self::TopicHandle => {
                Some(Arc::clone(state) as Arc<dyn turn::AgentInterventionCallbackRegistry>)
            }
            Self::Workspace(workspace) => Some(Arc::new(WorkspaceInterventionCallbackRegistry {
                state: Arc::clone(state),
                workspace: workspace.clone(),
            })
                as Arc<dyn turn::AgentInterventionCallbackRegistry>),
        }
    }
}

struct WorkspaceTurnRecorder {
    state: Arc<BotState>,
    workspace: WorkspaceId,
}

impl turn::TurnEventRecorder for WorkspaceTurnRecorder {
    fn append_turn_timeline(
        &self,
        _target: &WorkspaceHandle,
        turn_id: &TurnId,
        kind: TimelineItemKind,
        payload: serde_json::Value,
    ) -> Result<TimelineItem, String> {
        self.state
            .append_turn_timeline(&self.workspace, turn_id, kind, payload)
    }

    fn record_subagent_tool_call(
        &self,
        _target: &WorkspaceHandle,
        turn_id: &TurnId,
        provider_item_id: Option<&str>,
        input: &serde_json::Value,
    ) {
        if let Err(err) =
            self.state
                .record_subagent_tool_call(&self.workspace, turn_id, provider_item_id, input)
        {
            warn!(
                target: "lucarne_telegram::bot",
                workspace = %self.workspace.as_str(),
                error = %err,
                "failed to record notification-routed subagent action"
            );
        }
    }

    fn record_turn_usage(&self, turn_id: &TurnId, usage: lucarne::agent_runtime::UsageEvent) {
        if let Err(err) = self.state.record_turn_usage(turn_id, usage) {
            warn!(
                target: "lucarne_telegram::bot",
                workspace = %self.workspace.as_str(),
                error = %err,
                "failed to record notification-routed turn usage"
            );
        }
    }

    fn mark_turn_waiting_permission(&self, turn_id: &TurnId) {
        if let Err(err) = self.state.mark_turn_waiting_permission(turn_id) {
            warn!(
                target: "lucarne_telegram::bot",
                workspace = %self.workspace.as_str(),
                error = %err,
                "failed to mark notification-routed turn waiting for permission"
            );
        }
    }
}

struct WorkspaceStatusRecorder {
    state: Arc<BotState>,
    workspace: WorkspaceId,
}

impl turn::AgentStatusRecorder for WorkspaceStatusRecorder {
    fn record_agent_status(
        &self,
        _target: &WorkspaceHandle,
        status: &lucarne::agent_runtime::AgentStatus,
    ) -> Option<lucarne::control_plane::StatusSnapshot> {
        self.state.record_provider_status(&self.workspace, status)
    }
}

struct WorkspaceInterventionCallbackRegistry {
    state: Arc<BotState>,
    workspace: WorkspaceId,
}

impl turn::AgentInterventionCallbackRegistry for WorkspaceInterventionCallbackRegistry {
    fn intervention_button_data(
        &self,
        _target: &WorkspaceHandle,
        live_instance: &InstanceId,
        req_id: &str,
        action: turn::IntvAction,
    ) -> String {
        self.state
            .register_intervention_callback(&self.workspace, live_instance, req_id, action)
    }
}

struct DirectNotificationGuard {
    core: Arc<LucarneCore>,
    workspace: WorkspaceId,
}

impl DirectNotificationGuard {
    fn new(core: Arc<LucarneCore>, workspace: WorkspaceId) -> Self {
        core.begin_direct_notification_suppression(&lucarne::control_plane::WorkspaceId::new(
            workspace.as_str(),
        ));
        Self { core, workspace }
    }
}

impl Drop for DirectNotificationGuard {
    fn drop(&mut self) {
        self.core
            .end_direct_notification_suppression(&lucarne::control_plane::WorkspaceId::new(
                self.workspace.as_str(),
            ));
    }
}

impl Bot {
    pub fn new(channel: Arc<dyn Channel>, core: Arc<LucarneCore>, entry: WorkspaceHandle) -> Self {
        let provider_ids = core.provider_ids().to_vec();
        Self::new_with_state(
            channel,
            Arc::clone(&core),
            entry,
            BotState::new_with_core_and_provider_ids(core, provider_ids),
        )
    }

    pub fn new_with_state(
        channel: Arc<dyn Channel>,
        core: Arc<LucarneCore>,
        entry: WorkspaceHandle,
        state: Arc<BotState>,
    ) -> Self {
        Self::new_with_state_and_history_watch(channel, core, entry, state, true)
    }

    pub fn new_with_state_and_global_config_persistence(
        channel: Arc<dyn Channel>,
        core: Arc<LucarneCore>,
        entry: WorkspaceHandle,
        state: Arc<BotState>,
        global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
    ) -> Self {
        Self::new_with_state_and_history_watch_and_global_config_persistence(
            channel,
            core,
            entry,
            state,
            true,
            global_config_persistence,
        )
    }

    pub fn new_with_state_and_history_watch(
        channel: Arc<dyn Channel>,
        core: Arc<LucarneCore>,
        entry: WorkspaceHandle,
        state: Arc<BotState>,
        start_history_watch: bool,
    ) -> Self {
        Self::new_with_state_and_history_watch_and_global_config_persistence(
            channel,
            core,
            entry,
            state,
            start_history_watch,
            None,
        )
    }

    pub fn new_with_state_and_history_watch_and_global_config_persistence(
        channel: Arc<dyn Channel>,
        core: Arc<LucarneCore>,
        entry: WorkspaceHandle,
        state: Arc<BotState>,
        _start_history_watch: bool,
        global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
    ) -> Self {
        state.hydrate_notification_handle(&entry.chat);
        Self {
            channel,
            state,
            core,
            entry,
            turn_scheduler: TurnScheduler::new(),
            notification_handle_lock: AsyncMutex::new(()),
            workspace_topic_repair_lock: AsyncMutex::new(()),
            recent_unbound_topic_creations: AsyncMutex::new(Vec::new()),
            global_config_persistence,
        }
    }

    async fn acquire_workspace_turn_slot(
        &self,
        ws: &WorkspaceId,
        handle: &WorkspaceHandle,
        reply_to: &MessageId,
    ) -> TurnPermit {
        let control_workspace = lucarne::control_plane::WorkspaceId::new(ws.as_str());
        let admission = self.turn_scheduler.admit(&control_workspace);
        if let Some(position) = admission.queued_position() {
            let queued = OutgoingMessage::plain(format!(
                "⏳ queued · position {position} · waiting for the current turn to finish"
            ))
            .reply_to(reply_to.clone())
            .silent();
            if let Err(err) = self.channel.send(handle, queued).await {
                warn!(
                    target: "lucarne_telegram::bot",
                    workspace = %ws.as_str(),
                    error = %err,
                    "failed to send queued turn notice"
                );
            }
        }
        admission.wait().await
    }

    async fn refreshed_workspace_session(
        &self,
        ws: &WorkspaceId,
        handle: &WorkspaceHandle,
        reply_to: &MessageId,
    ) -> Result<Option<WorkSession>, String> {
        let session = match self.state.get(ws) {
            Some(session) => Some(session),
            None => self
                .state
                .hydrate_workspace(ws, &handle.chat)
                .map_err(|err| err.to_string())?,
        };
        match session {
            Some(mut latest) => {
                self.repair_history_replay_resume_ref(&mut latest, handle)?;
                Ok(Some(latest))
            }
            None => {
                warn!(
                    target: "lucarne_telegram::bot",
                    workspace = %ws.as_str(),
                    "workspace disappeared while a queued turn was waiting"
                );
                let msg = OutgoingMessage::markdown(
                    "This workspace is no longer bound to an agent session.",
                )
                .reply_to(reply_to.clone());
                self.channel
                    .send(handle, msg)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(None)
            }
        }
    }

    fn repair_history_replay_resume_ref(
        &self,
        session: &mut WorkSession,
        handle: &WorkspaceHandle,
    ) -> Result<(), String> {
        if session.live.is_some() {
            return Ok(());
        }
        let Some(record) =
            self.state
                .history_replay_record(&session.workspace, &handle.chat, &handle.workspace)
        else {
            return Ok(());
        };
        if record.provider_id.as_str() != session.provider_id {
            return Ok(());
        }
        let replay_ref = record.session_id.as_str().trim();
        if replay_ref.is_empty() {
            return Ok(());
        }
        let active_ref = self
            .state
            .active_provider_session_ref(&session.workspace)
            .ok();
        if session.resume_ref.as_deref() == Some(replay_ref)
            && active_ref.as_deref() == Some(replay_ref)
        {
            return Ok(());
        }
        session.chat = handle.chat.clone();
        session.resume_ref = Some(replay_ref.to_string());
        self.state
            .upsert_with_topic_replacing_resume_ref(session.clone(), handle.workspace.clone())
            .map_err(|e| e.to_string())
    }

    fn core_provider_id(&self, id: &str) -> Option<&'static str> {
        self.core
            .provider_ids()
            .iter()
            .copied()
            .find(|provider| *provider == id)
    }

    /// Consume channel events forever.
    pub async fn run(self: Arc<Self>) {
        let noop_system_notifications = SystemNotificationBus::new(1);
        let system_notifications = noop_system_notifications.subscribe();
        Arc::clone(&self)
            .run_with_system_notifications(system_notifications)
            .await;
        drop(noop_system_notifications);
    }

    /// Consume channel events and daemon system notifications forever.
    pub async fn run_with_system_notifications(
        self: Arc<Self>,
        mut system_notifications: SystemNotificationReceiver,
    ) {
        lucarne::memory_profile_snapshot!("lucarne_telegram.bot.run.start");
        info!(target: "lucarne_telegram::bot", channel = self.channel.name(), "bot run loop starting");
        let core_watcher = {
            let bot = self.clone();
            let mut core_events = self.core.watch_events();
            lucarne::memory_profile_snapshot!(
                "lucarne_telegram.bot.run.after_watch_events_subscribe"
            );
            let core_watcher = tokio::spawn(async move {
                loop {
                    match core_events.recv().await {
                        Ok(event) => {
                            let bot = bot.clone();
                            tokio::spawn(async move {
                                if let Err(e) = bot.handle_core_event(event).await {
                                    warn!(target: "lucarne_telegram::bot", error = %e, "core event handler error");
                                }
                            });
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(target: "lucarne_telegram::bot", skipped, "core event watch lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            warn!(target: "lucarne_telegram::bot", "core event watch closed");
                            break;
                        }
                    }
                }
            });
            lucarne::memory_profile_snapshot!("lucarne_telegram.bot.run.after_core_watcher_spawn");
            core_watcher
        };

        debug!(
            target: "lucarne_telegram::bot",
            "initial panel render skipped; waiting for explicit input"
        );
        if let Err(err) = self.send_startup_help().await {
            warn!(
                target: "lucarne_telegram::bot",
                error = %err,
                "telegram startup help send failed"
            );
        }

        let mut stream = self.channel.subscribe();
        let mut system_notifications_open = true;
        lucarne::memory_profile_snapshot!("lucarne_telegram.bot.run.after_channel_subscribe");
        loop {
            tokio::select! {
                event = stream.next() => {
                    let Some(ev) = event else {
                        break;
                    };
                    let bot = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = bot.handle(ev).await {
                            warn!(
                                target: "lucarne_telegram::bot",
                                error = %e,
                                "handler error"
                            );
                        }
                    });
                }
                notification = system_notifications.recv(), if system_notifications_open => {
                    match notification {
                        Ok(notification) => {
                            if let Err(e) = self.handle_system_notification(notification).await {
                                warn!(
                                    target: "lucarne_telegram::bot",
                                    error = %e,
                                    "system notification handler error"
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(target: "lucarne_telegram::bot", skipped, "system notification watch lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            system_notifications_open = false;
                            warn!(target: "lucarne_telegram::bot", "system notification watch closed");
                        }
                    }
                }
            }
        }
        core_watcher.abort();
        warn!(target: "lucarne_telegram::bot", "channel event stream ended — bot stopping");
    }

    async fn handle_system_notification(
        &self,
        notification: SystemNotification,
    ) -> Result<(), String> {
        match notification {
            SystemNotification::UpdateAvailable(update) => {
                let msg = OutgoingMessage::markdown(update.body_markdown);
                send_with_fallback(&*self.channel, &self.entry, msg, "lucarned")
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
        }
    }

    async fn handle(self: Arc<Self>, ev: ChannelEvent) -> Result<(), String> {
        match ev {
            ChannelEvent::Message(m) => self.handle_message(m).await,
            ChannelEvent::CommandQuery(q) => self.handle_command_query(q).await,
            ChannelEvent::Button {
                chat,
                workspace,
                data,
                source_message,
                ..
            } => {
                self.handle_button(chat, workspace, data, source_message)
                    .await
            }
            ChannelEvent::Warning(w) => {
                warn!(target: "lucarne_telegram", "channel warning: {w}");
                Ok(())
            }
        }
    }

    async fn handle_core_event(self: Arc<Self>, event: CoreEvent) -> Result<(), String> {
        let CoreEvent::TimelineEvent {
            workspace_id,
            event:
                AgentEvent::Message(lucarne::agent_runtime::MessageEvent {
                    role: MessageRole::Assistant,
                    text,
                    streaming: false,
                }),
            ..
        } = event
        else {
            return Ok(());
        };
        let workspace = WorkspaceId::new(workspace_id.as_str());
        let mut session = self.state.get(&workspace);
        if session.is_none() {
            self.state
                .hydrate_unbound_control_workspaces(&self.entry.chat)
                .map_err(|err| err.to_string())?;
            session = self.state.get(&workspace);
        }
        let Some(session) = session else {
            warn!(
                target: "lucarne_telegram::bot",
                workspace = %workspace.as_str(),
                "watched agent message has no Telegram session binding"
            );
            return Ok(());
        };
        if self.core.direct_notification_suppressed(&workspace_id) {
            return Ok(());
        }
        if session.live.is_some() && self.state.topic_for_workspace(&session.workspace).is_some() {
            return self
                .send_watched_agent_message_to_session_topic(&session, text.as_ref())
                .await;
        }
        if self
            .core
            .direct_notification_delivery_suppressed(&workspace_id)
        {
            return Ok(());
        }
        if !self.state.notifications_enabled_for_session(&session) {
            return Ok(());
        }
        self.send_agent_notification(&session, text.as_ref()).await
    }

    async fn send_watched_agent_message_to_session_topic(
        &self,
        session: &WorkSession,
        text: &str,
    ) -> Result<(), String> {
        let Some((mut session, mut handle)) = self.ensure_session_topic_handle(session).await?
        else {
            return self
                .send_watched_agent_message_as_notification_if_enabled(session, text)
                .await;
        };
        let msg = OutgoingMessage::markdown(text.trim());
        match send_with_fallback(&*self.channel, &handle, msg.clone(), session.provider_id).await {
            Ok(_) => Ok(()),
            Err(ChannelError::WorkspaceNotFound(reason)) => {
                warn!(
                    target: "lucarne_telegram::bot",
                    workspace = %session.workspace.as_str(),
                    old_topic = %handle.workspace.as_str(),
                    reason = %reason,
                    "telegram workspace topic missing during watched send; recreating"
                );
                let Some((repaired_session, repaired_handle)) = self
                    .replace_missing_session_topic(&session, &handle, false)
                    .await?
                else {
                    return self
                        .send_watched_agent_message_as_notification_if_enabled(&session, text)
                        .await;
                };
                session = repaired_session;
                handle = repaired_handle;
                send_with_fallback(&*self.channel, &handle, msg, session.provider_id)
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            Err(err) => Err(err.to_string()),
        }
    }

    async fn send_watched_agent_message_as_notification_if_enabled(
        &self,
        session: &WorkSession,
        text: &str,
    ) -> Result<(), String> {
        let session = self
            .state
            .get(&session.workspace)
            .unwrap_or_else(|| session.clone());
        if !self.state.notifications_enabled_for_session(&session) {
            return Ok(());
        }
        self.send_agent_notification(&session, text).await
    }

    async fn ensure_session_topic_handle(
        &self,
        session: &WorkSession,
    ) -> Result<Option<(WorkSession, WorkspaceHandle)>, String> {
        let session = self
            .state
            .get(&session.workspace)
            .unwrap_or_else(|| session.clone());
        if session.live.is_none() {
            return Ok(None);
        }
        let handle = self
            .state
            .handle_for_session(&session)
            .map_err(|err| err.to_string())?;
        match self.channel.probe_workspace(&handle).await {
            Ok(()) | Err(ChannelError::Unsupported(_)) => Ok(Some((session, handle))),
            Err(ChannelError::WorkspaceNotFound(reason)) => {
                warn!(
                    target: "lucarne_telegram::bot",
                    workspace = %session.workspace.as_str(),
                    old_topic = %handle.workspace.as_str(),
                    reason = %reason,
                    "telegram workspace topic missing before watched send; recreating"
                );
                self.replace_missing_session_topic(&session, &handle, true)
                    .await
            }
            Err(err) => Err(err.to_string()),
        }
    }

    async fn replace_missing_session_topic(
        &self,
        session: &WorkSession,
        missing: &WorkspaceHandle,
        verify_current: bool,
    ) -> Result<Option<(WorkSession, WorkspaceHandle)>, String> {
        let _guard = self.workspace_topic_repair_lock.lock().await;
        let mut session = self
            .state
            .get(&session.workspace)
            .unwrap_or_else(|| session.clone());
        if session.live.is_none() {
            return Ok(None);
        }
        let current = self
            .state
            .handle_for_session(&session)
            .map_err(|err| err.to_string())?;
        if current != *missing || verify_current {
            match self.channel.probe_workspace(&current).await {
                Ok(()) | Err(ChannelError::Unsupported(_)) => return Ok(Some((session, current))),
                Err(ChannelError::WorkspaceNotFound(_)) => {}
                Err(err) => return Err(err.to_string()),
            }
        }
        let handle = self
            .channel
            .create_workspace(&session.chat, &session.title)
            .await
            .map_err(|err| err.to_string())?;
        session.chat = handle.chat.clone();
        self.state
            .upsert_with_topic(session.clone(), handle.workspace.clone())
            .map_err(|err| err.to_string())?;
        self.state
            .record_reconcile_outcome(&session.workspace, ReconcileOutcome::TopicMissingRecreated)
            .map_err(|err| err.to_string())?;
        Ok(Some((session, handle)))
    }

    async fn ensure_notification_handle(&self) -> Result<WorkspaceHandle, String> {
        if let Some(handle) = self.state.notification_handle() {
            return match self.channel.probe_workspace(&handle).await {
                Ok(()) | Err(ChannelError::Unsupported(_)) => Ok(handle),
                Err(ChannelError::WorkspaceNotFound(_)) => {
                    self.replace_missing_notification_handle(&handle).await
                }
                Err(err) => Err(err.to_string()),
            };
        }
        let _guard = self.notification_handle_lock.lock().await;
        self.state.hydrate_notification_handle(&self.entry.chat);
        if let Some(handle) = self.state.notification_handle() {
            match self.channel.probe_workspace(&handle).await {
                Ok(()) | Err(ChannelError::Unsupported(_)) => return Ok(handle),
                Err(ChannelError::WorkspaceNotFound(_)) => {
                    match self.channel.delete_workspace(&handle).await {
                        Ok(()) | Err(ChannelError::WorkspaceNotFound(_)) => {
                            self.state.clear_notification_handle(&handle.chat)?;
                        }
                        Err(err) => return Err(err.to_string()),
                    }
                }
                Err(err) => return Err(err.to_string()),
            }
        }
        let handle = self
            .channel
            .create_workspace(&self.entry.chat, NOTIFICATION_TOPIC_TITLE)
            .await
            .map_err(|err| err.to_string())?;
        self.state.set_notification_handle(&handle)?;
        Ok(handle)
    }

    async fn replace_missing_notification_handle(
        &self,
        missing: &WorkspaceHandle,
    ) -> Result<WorkspaceHandle, String> {
        let _guard = self.notification_handle_lock.lock().await;
        if let Some(current) = self.state.notification_handle() {
            if current != *missing {
                match self.channel.probe_workspace(&current).await {
                    Ok(()) | Err(ChannelError::Unsupported(_)) => return Ok(current),
                    Err(ChannelError::WorkspaceNotFound(_)) => {
                        match self.channel.delete_workspace(&current).await {
                            Ok(()) | Err(ChannelError::WorkspaceNotFound(_)) => {
                                self.state.clear_notification_handle(&current.chat)?;
                            }
                            Err(err) => return Err(err.to_string()),
                        }
                    }
                    Err(err) => return Err(err.to_string()),
                }
            } else {
                match self.channel.delete_workspace(&current).await {
                    Ok(()) | Err(ChannelError::WorkspaceNotFound(_)) => {
                        self.state.clear_notification_handle(&current.chat)?;
                    }
                    Err(err) => return Err(err.to_string()),
                }
            }
        }
        let handle = self
            .channel
            .create_workspace(&self.entry.chat, NOTIFICATION_TOPIC_TITLE)
            .await
            .map_err(|err| err.to_string())?;
        self.state.set_notification_handle(&handle)?;
        Ok(handle)
    }

    async fn reset_notification_topic(
        self: Arc<Self>,
        response_handle: &WorkspaceHandle,
    ) -> Result<(), String> {
        let (old_handle, new_handle) = {
            let _guard = self.notification_handle_lock.lock().await;
            let old_handle = self.state.notification_handle();
            if let Some(old) = old_handle.as_ref() {
                match self.channel.delete_workspace(old).await {
                    Ok(()) | Err(ChannelError::WorkspaceNotFound(_)) => {
                        self.state.clear_notification_handle(&old.chat)?;
                    }
                    Err(err) => return Err(err.to_string()),
                }
            }
            let new_handle = self
                .channel
                .create_workspace(&self.entry.chat, NOTIFICATION_TOPIC_TITLE)
                .await
                .map_err(|err| err.to_string())?;
            self.state.set_notification_handle(&new_handle)?;
            (old_handle, new_handle)
        };

        let response_target = if old_handle
            .as_ref()
            .is_some_and(|old| old == response_handle)
        {
            new_handle.clone()
        } else {
            response_handle.clone()
        };
        let old_topic = old_handle
            .as_ref()
            .map(|handle| handle.workspace.as_str())
            .unwrap_or("none");
        let body = format!(
            "reset agent notifications topic\nold: {old_topic}\nnew: {}\n\nIf the old tab still appears on mobile, reopen Telegram to refresh the topic list.",
            new_handle.workspace.as_str()
        );
        self.channel
            .send(&response_target, OutgoingMessage::plain(body).silent())
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn send_agent_notification(
        &self,
        session: &WorkSession,
        text: &str,
    ) -> Result<(), String> {
        let mut handle = self.ensure_notification_handle().await?;
        let provider_session_id = self
            .state
            .active_provider_session_id(&session.workspace)
            .map_err(|err| err.to_string())?;
        let session_ref = self
            .core
            .provider_session_record(&provider_session_id)
            .map(|record| record.native_resume_ref.to_string());
        let msg = render_agent_notification(session, session_ref.as_deref(), text);
        let message_ids =
            match send_with_fallback_all(&*self.channel, &handle, msg.clone(), session.provider_id)
                .await
            {
                Ok(ids) => ids,
                Err(ChannelError::WorkspaceNotFound(reason)) => {
                    warn!(
                        target: "lucarne_telegram::bot",
                        old_topic = %handle.workspace.as_str(),
                        reason = %reason,
                        "telegram notification topic missing; recreating"
                    );
                    handle = self.replace_missing_notification_handle(&handle).await?;
                    send_with_fallback_all(&*self.channel, &handle, msg, session.provider_id)
                        .await
                        .map_err(|err| err.to_string())?
                }
                Err(err) => return Err(err.to_string()),
            };
        self.register_message_session_bindings(&handle.chat, &message_ids, provider_session_id)?;
        Ok(())
    }

    async fn handle_entry_config_command(
        self: Arc<Self>,
        handle: &WorkspaceHandle,
        args: &str,
    ) -> Result<(), String> {
        let changed = self.handle_config_command(handle, None, args).await?;
        if changed && handle.chat == self.entry.chat && handle.workspace == self.entry.workspace {
            Arc::clone(&self).render_entry_panel().await?;
        }
        Ok(())
    }

    async fn handle_workspace_config_command(
        &self,
        handle: &WorkspaceHandle,
        workspace: &WorkspaceId,
        args: &str,
    ) -> Result<bool, String> {
        let session = self
            .state
            .get(workspace)
            .ok_or_else(|| "workspace config requires a bound session".to_string())?;
        self.handle_config_command(handle, Some(&session), args)
            .await
    }

    async fn handle_config_command(
        &self,
        handle: &WorkspaceHandle,
        session: Option<&WorkSession>,
        args: &str,
    ) -> Result<bool, String> {
        let action = parse_config_action(args, session.is_some())?;
        let changed = if let Some(update) = action.update {
            self.apply_config_update(session, update)?;
            true
        } else {
            false
        };
        let scope = action.scope;
        let settings = self.effective_config_for_scope(session, scope)?;
        let msg = OutgoingMessage::markdown(render_config_message(scope, &settings)).silent();
        self.channel
            .send(handle, msg)
            .await
            .map_err(|err| err.to_string())
            .map(|_| changed)
    }

    fn apply_config_update(
        &self,
        session: Option<&WorkSession>,
        update: ConfigUpdate,
    ) -> Result<(), String> {
        match (update.scope, update.setting) {
            (ConfigScope::Global, ConfigSetting::Notifications) => {
                self.persist_global_config_update(update.setting, update.enabled)?;
                self.core
                    .set_global_notifications_enabled(update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            (ConfigScope::Global, ConfigSetting::Bypass) => {
                self.persist_global_config_update(update.setting, update.enabled)?;
                self.core
                    .set_force_bypass_permissions(update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            (ConfigScope::Workspace, ConfigSetting::Notifications) => {
                let project_path = config_project_path(session)?;
                self.core
                    .set_workspace_notifications_enabled(project_path, update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            (ConfigScope::Workspace, ConfigSetting::Bypass) => {
                let project_path = config_project_path(session)?;
                self.core
                    .set_workspace_force_bypass_permissions(project_path, update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            (ConfigScope::Session, ConfigSetting::Notifications) => {
                let provider_session_id = config_provider_session_id(&self.state, session)?;
                self.core
                    .set_session_notifications_enabled(&provider_session_id, update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
            (ConfigScope::Session, ConfigSetting::Bypass) => {
                let provider_session_id = config_provider_session_id(&self.state, session)?;
                self.core
                    .set_session_force_bypass_permissions(&provider_session_id, update.enabled)
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
        }
    }

    fn persist_global_config_update(
        &self,
        setting: ConfigSetting,
        enabled: bool,
    ) -> Result<(), String> {
        let Some(persistence) = self.global_config_persistence.as_ref() else {
            return Ok(());
        };
        let settings = self.core.effective_settings(None, None);
        let mut update = GlobalConfigUpdate {
            bypass: settings.session.force_bypass_permissions,
            notifications: settings.notifications.enabled,
        };
        match setting {
            ConfigSetting::Bypass => update.bypass = enabled,
            ConfigSetting::Notifications => update.notifications = enabled,
        }
        persistence
            .persist_global_config(update)
            .map_err(|err| err.to_string())
    }

    fn effective_config_for_scope(
        &self,
        session: Option<&WorkSession>,
        scope: ConfigScope,
    ) -> Result<lucarne::control_plane::EffectiveSettings, String> {
        match scope {
            ConfigScope::Global => Ok(self.core.effective_settings(None, None)),
            ConfigScope::Workspace => {
                let project_path = config_project_path(session)?;
                Ok(self.core.effective_settings(Some(project_path), None))
            }
            ConfigScope::Session => {
                let project_path = config_project_path(session)?;
                let provider_session_id = config_provider_session_id(&self.state, session)?;
                Ok(self
                    .core
                    .effective_settings(Some(project_path), Some(&provider_session_id)))
            }
        }
    }

    async fn handle_notification_topic_message(
        self: Arc<Self>,
        m: IncomingMessage,
        handle: WorkspaceHandle,
    ) -> Result<(), String> {
        if let Some(text) = m.text.as_deref() {
            let trimmed = text.trim();
            if let Some(command) = parse_entry_command_help(trimmed) {
                info!(target: "lucarne_telegram::bot", command = %command.name, "notification command help");
                return self.send_command_help_message(&handle, &command).await;
            }
            if let Some(args) = parse_entry_config_command(trimmed) {
                info!(target: "lucarne_telegram::bot", "notification cmd: /config");
                return self
                    .handle_entry_config_command(&handle, args.as_ref())
                    .await;
            }
            if parse_entry_command(trimmed) == Some(EntryAction::ResetNotifications) {
                info!(target: "lucarne_telegram::bot", "notification cmd: /reset_notifications");
                return self.reset_notification_topic(&handle).await;
            }
        }

        if let Err(err) = self.channel.acknowledge(&handle).await {
            debug!(target: "lucarne_telegram::bot", error = %err, "notification message acknowledgement failed");
        }
        let Some(reply_to) = m.reply_to.clone() else {
            let msg =
                OutgoingMessage::plain("Reply to an agent notification to continue that session.")
                    .reply_to(m.message_id);
            self.channel
                .send(&handle, msg)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())?;
            return Ok(());
        };
        let Some(provider_session_id) =
            self.state
                .resolve_message_session_binding(self.channel.name(), &m.chat, &reply_to)
        else {
            let msg = OutgoingMessage::plain("That notification is no longer routable.")
                .reply_to(m.message_id);
            self.channel
                .send(&handle, msg)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())?;
            return Ok(());
        };
        let Some(workspace) = self
            .state
            .workspace_for_provider_session(&provider_session_id)
        else {
            let msg = OutgoingMessage::plain("That notification is no longer routable.")
                .reply_to(m.message_id);
            self.channel
                .send(&handle, msg)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())?;
            return Ok(());
        };
        self.state.remember_user_workspace(&m.user, &workspace);
        let prompt = m
            .text
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string);
        let Some(prompt) = prompt else {
            let msg = OutgoingMessage::plain("Send text with the reply to continue that session.")
                .reply_to(m.message_id);
            self.channel
                .send(&handle, msg)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())?;
            return Ok(());
        };
        if prompt.starts_with('/') {
            let parsed_topic_command = parse_topic_slash_command(&prompt);
            let should_bypass_turn_slot = parsed_topic_command
                .as_ref()
                .is_some_and(|cmd| cmd.name == "interrupt");
            let _workspace_turn_guard = if should_bypass_turn_slot {
                None
            } else {
                Some(
                    self.acquire_workspace_turn_slot(&workspace, &handle, &m.message_id)
                        .await,
                )
            };
            let Some(session) = self
                .refreshed_workspace_session(&workspace, &handle, &m.message_id)
                .await?
            else {
                return Ok(());
            };
            return self
                .handle_topic_command(&handle, &session, &prompt, Some(m.message_id))
                .await;
        }
        let report = self
            .run_workspace_turn(
                &workspace,
                &handle,
                prompt,
                Vec::new(),
                &m.message_id,
                Some(m.message_id.clone()),
                TurnRecorderScope::Workspace(workspace.clone()),
                true,
                true,
            )
            .await?;
        let provider_session_id = self
            .state
            .active_provider_session_id(&workspace)
            .map_err(|err| err.to_string())?;
        self.register_message_session_bindings(
            &handle.chat,
            &report.message_ids,
            provider_session_id,
        )?;
        Ok(())
    }

    fn register_message_session_bindings(
        &self,
        chat: &ChatId,
        message_ids: &[MessageId],
        provider_session_id: lucarne::control_plane::ProviderSessionId,
    ) -> Result<(), String> {
        for message_id in message_ids {
            self.state
                .register_message_session_binding(
                    self.channel.name(),
                    chat,
                    message_id,
                    provider_session_id.clone(),
                )
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    async fn remember_unbound_topic_creation(&self, handle: &WorkspaceHandle) {
        let mut recent = self.recent_unbound_topic_creations.lock().await;
        let key = (
            handle.chat.as_str().to_string(),
            handle.workspace.as_str().to_string(),
        );
        if recent.iter().any(|existing| existing == &key) {
            return;
        }
        if recent.len() >= RECENT_UNBOUND_TOPIC_CREATION_LIMIT {
            recent.remove(0);
        }
        recent.push(key);
    }

    async fn take_recent_unbound_topic_creation(&self, handle: &WorkspaceHandle) -> bool {
        let mut recent = self.recent_unbound_topic_creations.lock().await;
        let key = (handle.chat.as_str(), handle.workspace.as_str());
        let Some(pos) = recent
            .iter()
            .position(|(chat, topic)| chat == key.0 && topic == key.1)
        else {
            return false;
        };
        recent.remove(pos);
        true
    }

    async fn try_handle_entry_text_command(self: &Arc<Self>, text: &str) -> Result<bool, String> {
        let trimmed = text.trim();
        if let Some(command) = parse_entry_command_help(trimmed) {
            info!(target: "lucarne_telegram::bot", command = %command.name, "entry command help");
            self.send_entry_command_help(&command).await?;
            return Ok(true);
        }
        if let Some(cmd) = parse_topic_slash_command(trimmed) {
            if cmd.name == "config" {
                let entry = self.entry.clone();
                Arc::clone(self)
                    .handle_entry_config_command(&entry, &cmd.args)
                    .await?;
                return Ok(true);
            }
            if cmd.name == "status" {
                self.send_agent_resource_status(&self.entry, AgentResourceScope::All)
                    .await?;
                return Ok(true);
            }
            if cmd.name == "kill" {
                self.handle_kill_command(&self.entry, AgentResourceScope::All, &cmd.args)
                    .await?;
                return Ok(true);
            }
        }
        if let Some(action) = parse_entry_command(trimmed) {
            info!(target: "lucarne_telegram::bot", ?action, "entry command");
            Arc::clone(self).dispatch_entry_command(action).await?;
            return Ok(true);
        }
        Ok(false)
    }

    #[instrument(
        skip_all,
        fields(
            workspace = m.workspace.as_ref().map(|w| w.as_str().to_string()).unwrap_or_else(|| "entry".into()),
            user = %m.user,
            bytes = m.text.as_ref().map(|t| t.len()).unwrap_or(0),
            attachments = m.attachments.len(),
        )
    )]
    async fn handle_message(self: Arc<Self>, m: IncomingMessage) -> Result<(), String> {
        let is_entry_chat = m.chat == self.entry.chat;
        if is_entry_chat && m.workspace.is_none() {
            if let Some(text) = &m.text {
                if self.try_handle_entry_text_command(text).await? {
                    return Ok(());
                }
            }
            debug!(target: "lucarne_telegram::bot", "entry chat free-form text → sending hint");
            let hint = OutgoingMessage::markdown(
                "This is the management panel. Send /panel to refresh, or tap an item like /a1, /h1, /w1.",
            )
            .silent();
            self.channel
                .send(&self.entry, hint)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }

        let topic = match m.workspace.clone() {
            Some(ws) => ws,
            None => {
                debug!(target: "lucarne_telegram::bot", "message with no workspace; ignoring");
                return Ok(());
            }
        };
        let handle = WorkspaceHandle::new(m.chat.clone(), topic.clone());
        if self.state.is_notification_handle(&handle) {
            return self.handle_notification_topic_message(m, handle).await;
        }
        let ws = match self.state.workspace_for_handle(&handle) {
            Some(ws) => ws,
            None => match self.state.hydrate_workspace_for_handle(&handle) {
                Ok(Some(session)) => {
                    debug!(
                        target: "lucarne_telegram::bot",
                        workspace = %session.workspace.as_str(),
                        chat = %m.chat.as_str(),
                        topic = %topic.as_str(),
                        "lazily hydrated topic workspace binding"
                    );
                    session.workspace
                }
                Ok(None) => {
                    return self
                        .handle_unbound_topic_message(m, handle, is_entry_chat, topic)
                        .await;
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_telegram::bot",
                        chat = %m.chat.as_str(),
                        topic = %topic.as_str(),
                        error = %err,
                        "lazy topic workspace hydration failed"
                    );
                    return self
                        .handle_unbound_topic_message(m, handle, is_entry_chat, topic)
                        .await;
                }
            },
        };
        self.state.remember_user_workspace(&m.user, &ws);

        if self.state.get(&ws).is_none() {
            warn!(
                target: "lucarne_telegram::bot",
                workspace = %ws.as_str(),
                "unbound workspace received a message"
            );
            let msg = OutgoingMessage::markdown(
                "This workspace isn't bound to an agent session yet. Tap a history entry in the entry panel to rebind.",
            );
            self.channel
                .send(&handle, msg)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        }
        if let Err(e) = self.channel.acknowledge(&handle).await {
            debug!(target: "lucarne_telegram::bot", error = %e, "message acknowledgement failed");
        }

        let mut parts: Vec<String> = Vec::new();
        if let Some(text) = m.text.as_deref() {
            if !text.is_empty() {
                if let Some(cmd) = text.strip_prefix('/') {
                    info!(target: "lucarne_telegram::bot", cmd, "topic command");
                    let parsed_topic_command = parse_topic_slash_command(cmd.trim());
                    if let Some(parsed) = parsed_topic_command.as_ref() {
                        if parsed.name == "config" {
                            if command_help_requested(&parsed.args) {
                                if let Some(command) = topic_command_descriptor("config") {
                                    self.send_command_help_message(&handle, &command).await?;
                                }
                            } else {
                                let changed = self
                                    .handle_workspace_config_command(
                                        &handle,
                                        &ws,
                                        parsed.args.as_ref(),
                                    )
                                    .await?;
                                if changed {
                                    Arc::clone(&self).render_entry_panel().await?;
                                }
                            }
                            return Ok(());
                        }
                        if parsed.name == "kill" {
                            return self
                                .handle_kill_command(
                                    &handle,
                                    AgentResourceScope::Workspace(
                                        lucarne::control_plane::WorkspaceId::new(ws.as_str()),
                                    ),
                                    &parsed.args,
                                )
                                .await;
                        }
                    }
                    let should_bypass_turn_slot = parsed_topic_command
                        .as_ref()
                        .is_some_and(|cmd| cmd.name == "interrupt");
                    let _workspace_turn_guard = if should_bypass_turn_slot {
                        None
                    } else {
                        Some(
                            self.acquire_workspace_turn_slot(&ws, &handle, &m.message_id)
                                .await,
                        )
                    };
                    let Some(session) = self
                        .refreshed_workspace_session(&ws, &handle, &m.message_id)
                        .await?
                    else {
                        return Ok(());
                    };
                    return self
                        .handle_topic_command(&handle, &session, cmd.trim(), Some(m.message_id))
                        .await;
                }
                parts.push(text.to_string());
            }
        }
        let mut images = self.state.take_pending_images(&ws);
        for att in &m.attachments {
            match self.fetch_image_attachment(att).await {
                Ok(Some(image)) => {
                    debug!(
                        target: "lucarne_telegram::bot",
                        filename = att.filename.as_deref().unwrap_or("?"),
                        mime = image.media_type.as_str(),
                        "ingested image attachment"
                    );
                    images.push(image);
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        target: "lucarne_telegram::bot",
                        filename = att.filename.as_deref().unwrap_or("?"),
                        error = %e,
                        "image attachment fetch failed"
                    );
                    let note = OutgoingMessage::plain(format!(
                        "(skipped image {}: {})",
                        att.filename.as_deref().unwrap_or("?"),
                        e
                    ))
                    .silent();
                    let _ = self.channel.send(&handle, note).await;
                    continue;
                }
            }
            match self.fetch_text_attachment(att).await {
                Ok(Some(snippet)) => {
                    debug!(
                        target: "lucarne_telegram::bot",
                        filename = att.filename.as_deref().unwrap_or("?"),
                        bytes = snippet.len(),
                        "ingested text attachment"
                    );
                    parts.push(snippet);
                }
                Ok(None) => {
                    debug!(
                        target: "lucarne_telegram::bot",
                        filename = att.filename.as_deref().unwrap_or("?"),
                        mime = att.mime_type.as_deref().unwrap_or("?"),
                        "skipping non-textual attachment"
                    );
                }
                Err(e) => {
                    warn!(
                        target: "lucarne_telegram::bot",
                        filename = att.filename.as_deref().unwrap_or("?"),
                        error = %e,
                        "attachment fetch failed"
                    );
                    let note = OutgoingMessage::plain(format!(
                        "(skipped attachment {}: {})",
                        att.filename.as_deref().unwrap_or("?"),
                        e
                    ))
                    .silent();
                    let _ = self.channel.send(&handle, note).await;
                }
            }
        }
        if parts.is_empty() && !images.is_empty() {
            self.state.push_pending_images(&ws, images);
            debug!(
                target: "lucarne_telegram::bot",
                "image-only message stored for the next textual turn"
            );
            return Ok(());
        }
        if parts.is_empty() {
            debug!(target: "lucarne_telegram::bot", "no effective content after filtering");
            return Ok(());
        }
        let prompt = parts.join("\n\n");

        self.run_workspace_turn(
            &ws,
            &handle,
            prompt,
            images,
            &m.message_id,
            Some(m.message_id.clone()),
            TurnRecorderScope::TopicHandle,
            false,
            false,
        )
        .await
        .map(|_| ())
    }

    async fn handle_unbound_topic_message(
        self: &Arc<Self>,
        m: IncomingMessage,
        handle: WorkspaceHandle,
        is_entry_chat: bool,
        topic: WorkspaceId,
    ) -> Result<(), String> {
        if is_entry_chat && m.text.is_none() && m.attachments.is_empty() {
            self.remember_unbound_topic_creation(&handle).await;
            debug!(
                target: "lucarne_telegram::bot",
                chat = %m.chat.as_str(),
                topic = %topic.as_str(),
                "ignoring unbound topic service message"
            );
            return Ok(());
        }
        if is_entry_chat {
            if let Some(text) = &m.text {
                if self.try_handle_entry_text_command(text).await? {
                    if self.take_recent_unbound_topic_creation(&handle).await {
                        if let Err(err) = self.channel.delete_workspace(&handle).await {
                            warn!(
                                target: "lucarne_telegram::bot",
                                chat = %m.chat.as_str(),
                                topic = %topic.as_str(),
                                error = %err,
                                "failed to delete accidental unbound topic"
                            );
                        }
                    }
                    return Ok(());
                }
            }
        }
        warn!(
            target: "lucarne_telegram::bot",
            chat = %m.chat.as_str(),
            topic = %topic.as_str(),
            "unbound topic received a message"
        );
        let msg = OutgoingMessage::markdown(
            "This workspace isn't bound to an agent session yet. Tap a history entry in the entry panel to rebind.",
        );
        self.channel
            .send(&handle, msg)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn submit_and_drain_user_turn(
        &self,
        ws: &WorkspaceId,
        handle: &WorkspaceHandle,
        live: &LiveSession,
        input: AgentInput,
        provider_id: &str,
        recorder: Arc<dyn turn::TurnEventRecorder>,
        intervention_callback_registry: Option<Arc<dyn turn::AgentInterventionCallbackRegistry>>,
        final_footer: Option<AgentMessageFooter>,
        source_message: &MessageId,
        reply_to: Option<MessageId>,
    ) -> Result<(RunningTurn, turn::TurnRunReport), String> {
        turn::prepare_turn_drain(live).await;
        let _direct_notification_guard =
            DirectNotificationGuard::new(self.state.core_handle(), ws.clone());
        let running_turn = self
            .state
            .submit_user_turn(ws, input, Some(source_message))
            .await
            .map_err(|err| err.to_string())?;
        debug!("prompt submitted");
        let report = turn::drain_turn_with_options(
            &self.channel,
            handle,
            live,
            provider_id,
            turn::TurnRunOptions {
                recording: Some(turn::TurnRecording {
                    turn_id: running_turn.turn_id.clone(),
                    recorder,
                }),
                intervention_callback_registry,
                final_footer,
            },
            reply_to,
        )
        .await?;
        self.state.complete_turn(&running_turn)?;
        Ok((running_turn, report))
    }

    async fn run_workspace_turn(
        &self,
        ws: &WorkspaceId,
        handle: &WorkspaceHandle,
        prompt: String,
        images: Vec<AgentImageInput>,
        source_message: &MessageId,
        reply_to: Option<MessageId>,
        recorder_scope: TurnRecorderScope,
        include_agent_footer: bool,
        force_bypass_permissions: bool,
    ) -> Result<turn::TurnRunReport, String> {
        let _workspace_turn_guard = self
            .acquire_workspace_turn_slot(ws, handle, source_message)
            .await;
        let Some(latest) = self
            .refreshed_workspace_session(ws, handle, source_message)
            .await?
        else {
            return Ok(turn::TurnRunReport::default());
        };
        let mut session = latest;

        if session.live.is_none() {
            info!(
                target: "lucarne_telegram::bot",
                provider = session.provider_id,
                "opening/resuming agent session on first turn"
            );
            match self
                .ensure_live_bound(&mut session, force_bypass_permissions)
                .await
            {
                Ok(_) => {
                    info!(target: "lucarne_telegram::bot", "live session bound");
                }
                Err(err) => {
                    self.state.push_pending_images(ws, images);
                    warn!(
                        target: "lucarne_telegram::bot",
                        error = %err,
                        provider = session.provider_id,
                        "agent session open/resume failed"
                    );
                    let msg = OutgoingMessage::markdown(format!("⚠ agent start failed: {err}"));
                    send_with_fallback(&*self.channel, handle, msg, session.provider_id)
                        .await
                        .map_err(|err| err.to_string())?;
                    return Ok(turn::TurnRunReport::default());
                }
            }
        }
        let mut live = session.live.clone().expect("bound above");
        let final_footer = include_agent_footer.then(|| {
            let session_ref = self
                .state
                .active_provider_session_ref(&session.workspace)
                .ok()
                .or_else(|| session.resume_ref.clone())
                .unwrap_or_else(|| "-".to_string());
            let cwd = session
                .project_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "-".to_string());
            AgentMessageFooter {
                cost: None,
                session: Some(session_ref),
                cwd: Some(cwd),
            }
        });
        let input = AgentInput {
            text: prompt.into(),
            images,
        };

        let t0 = std::time::Instant::now();
        let recorder = recorder_scope.recorder(&self.state);
        let intervention_callback_registry =
            recorder_scope.intervention_callback_registry(&self.state);
        let (running_turn, report) = match self
            .submit_and_drain_user_turn(
                ws,
                handle,
                &live,
                input.clone(),
                session.provider_id,
                Arc::clone(&recorder),
                intervention_callback_registry.clone(),
                final_footer.clone(),
                source_message,
                reply_to.clone(),
            )
            .await
        {
            Ok((running_turn, report)) => (running_turn, report),
            Err(err) if is_recoverable_live_session_error(&err) => {
                warn!(
                    target: "lucarne_telegram::bot",
                    error = %err,
                    provider = session.provider_id,
                    "live agent session failed; reopening and retrying turn once"
                );
                let observed_close = live.session.observed_close_reason().await;
                if is_idle_recycled_session(observed_close.as_deref(), &err) {
                    let notice =
                        OutgoingMessage::plain("agent process was idle-recycled; resuming session")
                            .silent();
                    let _ = self.channel.send(handle, notice).await;
                }
                let resume_ref = match session.resume_ref.clone() {
                    Some(resume_ref) => Some(resume_ref),
                    None => live_provider_resume_ref(&live).await,
                };
                self.state
                    .mark_live_dead(ws, resume_ref.clone())
                    .map_err(|err| err.to_string())?;
                self.detach_core_live_session(ws, &live, &err).await?;
                let mut retry_session = WorkSession {
                    live: None,
                    resume_ref: resume_ref.clone(),
                    ..session.clone()
                };
                let retry_live = self
                    .ensure_live_bound(&mut retry_session, force_bypass_permissions)
                    .await?;
                let retry_result = self
                    .submit_and_drain_user_turn(
                        ws,
                        handle,
                        &retry_live,
                        input,
                        session.provider_id,
                        recorder,
                        intervention_callback_registry,
                        final_footer,
                        source_message,
                        reply_to,
                    )
                    .await?;
                live = retry_live;
                retry_result
            }
            Err(err) => {
                let resume_ref = match session.resume_ref.clone() {
                    Some(resume_ref) => Some(resume_ref),
                    None => live_provider_resume_ref(&live).await,
                };
                self.state
                    .mark_live_dead(ws, resume_ref)
                    .map_err(|err| err.to_string())?;
                self.detach_core_live_session(ws, &live, &err).await?;
                return Err(err);
            }
        };
        self.refresh_live_resume_ref(ws, &mut session, &live)
            .await?;
        self.send_turn_subagent_links(handle, &running_turn.turn_id, session.provider_id)
            .await?;
        info!(
            target: "lucarne_telegram::bot",
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "agent turn completed"
        );
        Ok(report)
    }

    #[instrument(
        skip_all,
        fields(
            provider = session.provider_id,
            resume = session.resume_ref.is_some(),
            cwd = session.project_path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "-".into()),
        )
    )]
    async fn open_or_resume_live(
        &self,
        session: &WorkSession,
        force_bypass_permissions: bool,
    ) -> Result<Arc<LiveSession>, String> {
        let canonical_resume_ref = match self.state.active_provider_session_ref(&session.workspace)
        {
            Ok(reference) if !is_provisional_live_resume_ref(&reference) => Some(reference),
            Ok(_) => None,
            Err(err) if session.resume_ref.is_some() => {
                return Err(format!(
                    "control-plane provider session unavailable for {}: {err}",
                    session.workspace.as_str()
                ));
            }
            Err(_) => None,
        };
        let resume_ref = canonical_resume_ref
            .as_deref()
            .or(session.resume_ref.as_deref());
        let core_workspace_id =
            lucarne::control_plane::WorkspaceId::new(session.workspace.as_str());
        let open_request = OpenWorkspaceRequest {
            provider_id: session.provider_id,
            project_path: session.project_path.clone(),
            title: session.title.clone(),
        };
        let opened = if let Some(reference) = resume_ref {
            debug!(target: "lucarne_telegram::bot", session_ref = reference, "resuming session");
            self.core
                .upsert_workspace_binding(core_workspace_id.clone(), open_request, Some(reference))
                .map_err(|e| e.to_string())?;
            self.core
                .resume_workspace_with_events(ResumeWorkspaceRequest {
                    workspace_id: core_workspace_id,
                    force_bypass_permissions,
                })
                .await
                .map_err(|e| e.to_string())?
        } else {
            debug!(target: "lucarne_telegram::bot", "opening fresh session");
            self.core
                .open_workspace_binding_with_events(core_workspace_id, open_request)
                .await
                .map_err(|e| e.to_string())?
        };

        Ok(Arc::new(LiveSession {
            session: opened.session,
            events: AsyncMutex::new(opened.events),
            pending_intv: Default::default(),
        }))
    }

    async fn send_turn_subagent_links(
        &self,
        handle: &WorkspaceHandle,
        turn_id: &lucarne::control_plane::TurnId,
        provider_id: &'static str,
    ) -> Result<(), String> {
        let links = self.state.subagent_links_for_turn(turn_id);
        let Some(msg) = turn::render_subagent_links(handle, &links, Some(self.state.as_ref()))
        else {
            return Ok(());
        };
        send_with_fallback(&*self.channel, handle, msg, provider_id)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn refresh_live_resume_ref(
        &self,
        ws: &WorkspaceId,
        session: &mut WorkSession,
        live: &Arc<LiveSession>,
    ) -> Result<(), String> {
        let Some(resume_ref) = live_provider_resume_ref(live).await else {
            return Ok(());
        };
        if session.resume_ref.is_none()
            && self.should_keep_live_only_resume_ref(ws, None, Some(resume_ref.as_str()))
        {
            return Ok(());
        }
        if session.resume_ref.as_deref() == Some(resume_ref.as_str()) {
            return Ok(());
        }
        self.state
            .bind_live(ws, live.clone(), Some(resume_ref.clone()))
            .map_err(|e| e.to_string())?;
        session.resume_ref = Some(resume_ref);
        Ok(())
    }

    fn command_run_options_for_session(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
        completion_policy: CommandCompletionPolicy,
    ) -> turn::CommandRunOptions {
        let mut options = command_run_options(
            session.provider_id,
            completion_policy,
            Arc::clone(&self.state),
        );
        if self.state.is_notification_handle(handle) {
            options.status_recorder = Some(Arc::new(WorkspaceStatusRecorder {
                state: Arc::clone(&self.state),
                workspace: session.workspace.clone(),
            }));
            options.intervention_callback_registry =
                Some(Arc::new(WorkspaceInterventionCallbackRegistry {
                    state: Arc::clone(&self.state),
                    workspace: session.workspace.clone(),
                }));
        }
        options
    }

    fn command_turn_recorder_for_session(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
    ) -> Arc<dyn turn::TurnEventRecorder> {
        if self.state.is_notification_handle(handle) {
            Arc::new(WorkspaceTurnRecorder {
                state: Arc::clone(&self.state),
                workspace: session.workspace.clone(),
            }) as Arc<dyn turn::TurnEventRecorder>
        } else {
            Arc::clone(&self.state) as Arc<dyn turn::TurnEventRecorder>
        }
    }

    async fn handle_topic_command(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
        cmd: &str,
        reply_to: Option<MessageId>,
    ) -> Result<(), String> {
        let Some(mut cmd) = parse_topic_slash_command(cmd) else {
            return Ok(());
        };
        if cmd.name == "help" {
            self.send_workspace_help_message(handle).await?;
            return Ok(());
        }
        if cmd.name == "config" {
            if command_help_requested(&cmd.args) {
                if let Some(command) = topic_command_descriptor("config") {
                    self.send_command_help_message(handle, &command).await?;
                }
                return Ok(());
            }
            let changed = self
                .handle_workspace_config_command(handle, &session.workspace, &cmd.args)
                .await?;
            let _ = changed;
            return Ok(());
        }
        if cmd.name == "kill" {
            return self
                .handle_kill_command(
                    handle,
                    AgentResourceScope::Workspace(lucarne::control_plane::WorkspaceId::new(
                        session.workspace.as_str(),
                    )),
                    &cmd.args,
                )
                .await;
        }
        if cmd.name == "rename" {
            if command_help_requested(&cmd.args) {
                if let Some(command) = topic_command_descriptor("rename") {
                    self.send_command_help_message(handle, &command).await?;
                }
                return Ok(());
            }
            let name = cmd.args.trim();
            if name.is_empty() {
                return Ok(());
            }
            self.channel
                .rename_workspace(handle, name)
                .await
                .map_err(|e| e.to_string())?;
            self.state
                .rename(&session.workspace, name.to_string())
                .map_err(|e| e.to_string())?;
            let msg = OutgoingMessage::plain(format!("Renamed workspace to {name}.")).silent();
            let _ = self.channel.send(handle, msg).await;
            return Ok(());
        }
        if let Some(index) = parse_fork_target_shortcut(&cmd.name, &cmd.args) {
            let Some(target) = self
                .state
                .resolve_fork_target_selection(&session.workspace, index)
            else {
                let msg =
                    OutgoingMessage::plain("Stale fork target. Run /fork to refresh.").silent();
                let _ = self.channel.send(handle, msg).await;
                return Ok(());
            };
            cmd = TopicSlashCommand::new("fork", target.clone()).with_values(serde_json::json!({
                "target_id": target.as_str(),
                "name": target.as_str(),
            }));
        }
        let mut session = session.clone();
        let live = self.ensure_live_bound(&mut session, false).await?;
        if cmd.name == "interrupt" {
            return self
                .handle_interrupt_command(handle, &session.workspace)
                .await;
        }
        let catalog = live
            .session
            .list_commands()
            .await
            .map_err(|e| e.to_string())?;
        if cmd.name == "commands" {
            if cmd.args.trim().is_empty() {
                let workflow = self.state.start_command_workflow(
                    &session.workspace,
                    &live,
                    "commands",
                    None,
                    serde_json::Value::Null,
                    Revision::new(catalog.revision),
                    CommandCompletionPolicy::CommandResult,
                    reply_to.as_ref(),
                )?;
                let result = AgentCommandResult {
                    name: "commands".into(),
                    source: AgentCommandSource::AdapterMapped,
                    data: AgentCommandResultData::Commands(catalog.clone()),
                };
                let payload = match serde_json::to_value(&result) {
                    Ok(payload) => payload,
                    Err(err) => {
                        let err = format!("failed to serialize command catalog: {err}");
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                };
                let item = match self.state.append_turn_timeline(
                    &session.workspace,
                    &workflow.turn_id,
                    TimelineItemKind::CommandResult,
                    payload.clone(),
                ) {
                    Ok(item) => item,
                    Err(err) => {
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                };
                let options = self.command_run_options_for_session(
                    handle,
                    &session,
                    CommandCompletionPolicy::CommandResult,
                );
                let message = match turn::render_immediate_command_timeline_item(
                    &handle,
                    &item,
                    Some(&options),
                ) {
                    Ok(message) => message,
                    Err(err) => {
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                };
                if let Err(err) = self.state.complete_command_workflow(
                    &workflow,
                    CommandCompletionPolicy::CommandResult,
                    payload,
                ) {
                    let _ = self.state.fail_command_workflow(&workflow, &err);
                    return Err(err);
                }
                if let Some(message) = message {
                    let message_ids = self
                        .deliver_workspace_message(handle, message, PanelDelivery::Send)
                        .await?;
                    self.bind_notification_command_messages(handle, &session, &message_ids)?;
                }
                return Ok(());
            }
            if !command_help_requested(&cmd.args) {
                let Some(nested) = parse_topic_slash_command(&cmd.args) else {
                    return Ok(());
                };
                cmd = nested;
            }
        }

        let _direct_notification_guard =
            DirectNotificationGuard::new(Arc::clone(&self.core), session.workspace.clone());
        self.invoke_agent_command(handle, &live, &session, &catalog, cmd, reply_to)
            .await?;
        Ok(())
    }

    async fn invoke_agent_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        catalog: &AgentCommandCatalog,
        cmd: TopicSlashCommand,
        reply_to: Option<MessageId>,
    ) -> Result<(), String> {
        if command_help_requested(&cmd.args) {
            let Some(command) = command_descriptor_for_help(catalog, &cmd.name) else {
                self.send_unsupported_agent_command(handle, session, cmd.name.as_str())
                    .await?;
                return Ok(());
            };
            let plan = CommandInvocationPlan {
                name: command.name.clone(),
                args: None,
                values: serde_json::Value::Null,
                source: command.source,
                catalog_revision: Revision::new(catalog.revision),
                completion_policy: CommandCompletionPolicy::CommandResult,
            };
            let workflow = self.state.start_planned_command_workflow(
                &session.workspace,
                live,
                &plan,
                reply_to.as_ref(),
            )?;
            let result = AgentCommandResult {
                name: command.name.clone(),
                source: command.source,
                data: AgentCommandResultData::Text {
                    text: render_command_help(&command).into(),
                },
            };
            return self
                .finish_immediate_agent_command(
                    handle,
                    live,
                    session,
                    &workflow,
                    &plan,
                    result,
                    None,
                    reply_to.as_ref(),
                )
                .await;
        }
        let plan = match plan_topic_command_invocation(catalog, &cmd) {
            Ok(plan) => plan,
            Err(err) => {
                self.send_command_plan_error(handle, session, err).await?;
                return Ok(());
            }
        };
        let command_name = plan.name.to_string();
        let provider_ref_before = if matches!(command_name.as_str(), "new" | "quit" | "fork") {
            live_provider_resume_ref(live).await
        } else {
            None
        };
        let command_args = plan.args.as_ref().map(|args| args.to_string());
        let workflow = self.state.start_planned_command_workflow(
            &session.workspace,
            live,
            &plan,
            reply_to.as_ref(),
        )?;
        let mut options =
            self.command_run_options_for_session(handle, session, plan.completion_policy);
        if command_name == "status" {
            options.status_snapshot = self.state.status_snapshot(&session.workspace);
            options.status_resource = self.agent_status_resource(&session.workspace).await?;
        }
        options.recording = Some(turn::TurnRecording {
            turn_id: workflow.turn_id.clone(),
            recorder: self.command_turn_recorder_for_session(handle, session),
        });
        if let Some(result) = self
            .invoke_immediate_list_command(live, &plan)
            .await
            .map_err(|err| {
                let _ = self.state.fail_command_workflow(&workflow, &err);
                err
            })?
        {
            self.finish_immediate_agent_command(
                handle,
                live,
                session,
                &workflow,
                &plan,
                result,
                provider_ref_before,
                reply_to.as_ref(),
            )
            .await?;
            return Ok(());
        }
        if let Some(selection) = model_selection_from_plan(&plan)? {
            return self
                .invoke_session_set_model_command(
                    handle,
                    live,
                    session,
                    &workflow,
                    &plan,
                    selection,
                    provider_ref_before,
                    reply_to.as_ref(),
                )
                .await;
        }
        if let Some(selection) = permission_selection_from_plan(&plan) {
            return self
                .invoke_session_set_permissions_command(
                    handle,
                    live,
                    session,
                    &workflow,
                    &plan,
                    selection,
                    provider_ref_before,
                    reply_to.as_ref(),
                )
                .await;
        }
        if command_name == "new" {
            return self
                .invoke_session_new_command(
                    handle,
                    live,
                    session,
                    &workflow,
                    &plan,
                    provider_ref_before,
                    reply_to.as_ref(),
                )
                .await;
        }
        if command_name == "quit" {
            return self
                .invoke_session_quit_command(
                    handle,
                    live,
                    session,
                    &workflow,
                    &plan,
                    provider_ref_before,
                    reply_to.as_ref(),
                )
                .await;
        }
        if command_name == "fork" {
            if let Some(target) = command_args.as_deref() {
                return self
                    .invoke_session_fork_command(
                        handle,
                        live,
                        session,
                        &workflow,
                        target,
                        provider_ref_before,
                    )
                    .await;
            }
        }
        let invocation = plan.invocation();
        let run_result = turn::run_command(
            &self.channel,
            handle,
            live,
            invocation,
            session.provider_id,
            options,
            reply_to,
        )
        .await;
        match run_result {
            Ok(report)
                if matches!(
                    report.outcome,
                    turn::CommandDrainOutcome::CommandResult
                        | turn::CommandDrainOutcome::TurnCompleted
                        | turn::CommandDrainOutcome::ProviderIdle
                        | turn::CommandDrainOutcome::NoOutputAck
                ) =>
            {
                let observed_policy = report.outcome.completion_policy();
                let fork_result = (command_name == "fork")
                    .then(|| {
                        report
                            .command_result
                            .as_ref()
                            .and_then(command_report_fork_result)
                    })
                    .flatten();
                let result = match (observed_policy, report.command_result) {
                    (CommandCompletionPolicy::CommandResult, Some(result)) => result,
                    (CommandCompletionPolicy::CommandResult, None) => {
                        let err = "command_result completion had no structured result payload"
                            .to_string();
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                    (_, Some(result)) => result,
                    (_, None) => serde_json::json!({
                        "name": command_name,
                        "args": command_args,
                        "completion": observed_policy,
                    }),
                };
                if let Some(fork_result) = fork_result {
                    if workflow.completion_policy != observed_policy {
                        let err = format!(
                            "CommandCompletionPolicyMismatch {{ policy: {:?} }}",
                            workflow.completion_policy
                        );
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                    if let Err(err) = self
                        .finish_fork_command_result(
                            handle,
                            live,
                            session,
                            fork_result,
                            provider_ref_before,
                            true,
                        )
                        .await
                    {
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                    if let Err(err) =
                        self.state
                            .complete_command_workflow(&workflow, observed_policy, result)
                    {
                        let _ = self.state.fail_command_workflow(&workflow, &err);
                        return Err(err);
                    }
                    return Ok(());
                }
                if let Err(err) =
                    self.state
                        .complete_command_workflow(&workflow, observed_policy, result)
                {
                    let _ = self.state.fail_command_workflow(&workflow, &err);
                    return Err(err);
                }
            }
            Ok(_) => unreachable!("all command drain outcomes are handled"),
            Err(err) => {
                let _ = self.state.fail_command_workflow(&workflow, &err);
                return Err(err);
            }
        }
        self.finish_agent_command(
            &session.workspace,
            live,
            command_name.as_str(),
            provider_ref_before,
        )
        .await
    }

    async fn invoke_session_set_model_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        plan: &CommandInvocationPlan,
        selection: AgentModelSelection,
        provider_ref_before: Option<String>,
        reply_to: Option<&MessageId>,
    ) -> Result<(), String> {
        let status = live.session.set_model(selection).await.map_err(|err| {
            let err = err.to_string();
            let _ = self.state.fail_command_workflow(workflow, &err);
            err
        })?;
        let result = AgentCommandResult {
            name: "model".into(),
            source: AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Status(status),
        };
        self.finish_immediate_agent_command(
            handle,
            live,
            session,
            workflow,
            plan,
            result,
            provider_ref_before,
            reply_to,
        )
        .await
    }

    async fn invoke_session_set_permissions_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        plan: &CommandInvocationPlan,
        selection: AgentPermissionSelection,
        provider_ref_before: Option<String>,
        reply_to: Option<&MessageId>,
    ) -> Result<(), String> {
        let status = live
            .session
            .set_permissions(selection)
            .await
            .map_err(|err| {
                let err = err.to_string();
                let _ = self.state.fail_command_workflow(workflow, &err);
                err
            })?;
        let result = AgentCommandResult {
            name: "permissions".into(),
            source: AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Status(status),
        };
        self.finish_immediate_agent_command(
            handle,
            live,
            session,
            workflow,
            plan,
            result,
            provider_ref_before,
            reply_to,
        )
        .await
    }

    async fn invoke_session_new_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        plan: &CommandInvocationPlan,
        provider_ref_before: Option<String>,
        reply_to: Option<&MessageId>,
    ) -> Result<(), String> {
        live.session.new().await.map_err(|err| {
            let err = err.to_string();
            let _ = self.state.fail_command_workflow(workflow, &err);
            err
        })?;
        let result = AgentCommandResult {
            name: "new".into(),
            source: AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Empty,
        };
        self.finish_immediate_agent_command(
            handle,
            live,
            session,
            workflow,
            plan,
            result,
            provider_ref_before,
            reply_to,
        )
        .await
    }

    async fn invoke_session_quit_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        plan: &CommandInvocationPlan,
        provider_ref_before: Option<String>,
        reply_to: Option<&MessageId>,
    ) -> Result<(), String> {
        live.session.quit().await.map_err(|err| {
            let err = err.to_string();
            let _ = self.state.fail_command_workflow(workflow, &err);
            err
        })?;
        let result = AgentCommandResult {
            name: "quit".into(),
            source: AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Empty,
        };
        self.finish_immediate_agent_command(
            handle,
            live,
            session,
            workflow,
            plan,
            result,
            provider_ref_before,
            reply_to,
        )
        .await
    }

    async fn invoke_immediate_list_command(
        &self,
        live: &Arc<LiveSession>,
        plan: &CommandInvocationPlan,
    ) -> Result<Option<AgentCommandResult>, String> {
        if plan.args.is_some()
            || !matches!(
                plan.name.as_str(),
                "model" | "permissions" | "skills" | "status" | "fork"
            )
        {
            return Ok(None);
        }
        let data = match plan.name.as_str() {
            "model" => match live.session.list_models().await {
                Ok(catalog) => AgentCommandResultData::Models(catalog),
                Err(err) if is_unsupported_typed_command(&err) => return Ok(None),
                Err(err) => return Err(err.to_string()),
            },
            "permissions" => match live.session.list_permissions().await {
                Ok(catalog) => AgentCommandResultData::Permissions(catalog),
                Err(err) if is_unsupported_typed_command(&err) => return Ok(None),
                Err(err) => return Err(err.to_string()),
            },
            "skills" => match live.session.list_skills().await {
                Ok(catalog) => AgentCommandResultData::Skills(catalog),
                Err(err) if is_unsupported_typed_command(&err) => return Ok(None),
                Err(err) => return Err(err.to_string()),
            },
            "status" => match live.session.status().await {
                Ok(status) => AgentCommandResultData::Status(status),
                Err(err) if is_unsupported_typed_command(&err) => return Ok(None),
                Err(err) => return Err(err.to_string()),
            },
            "fork" => match live.session.list_fork_targets().await {
                Ok(catalog) => AgentCommandResultData::ForkTargets(catalog),
                Err(err) if is_unsupported_typed_command(&err) => return Ok(None),
                Err(err) => return Err(err.to_string()),
            },
            _ => return Ok(None),
        };
        Ok(Some(AgentCommandResult {
            name: plan.name.clone(),
            source: AgentCommandSource::AdapterMapped,
            data,
        }))
    }

    async fn send_agent_resource_status(
        &self,
        handle: &WorkspaceHandle,
        scope: AgentResourceScope,
    ) -> Result<(), String> {
        let snapshot = self
            .core
            .agent_resource_snapshot(scope)
            .await
            .map_err(|err| err.to_string())?;
        self.channel
            .send(
                handle,
                OutgoingMessage::markdown(render_agent_resource_snapshot(&snapshot)).silent(),
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn agent_status_resource(
        &self,
        workspace: &WorkspaceId,
    ) -> Result<Option<lucarne::core_service::AgentResourceEntry>, String> {
        let snapshot = self
            .core
            .agent_resource_snapshot(AgentResourceScope::Workspace(
                lucarne::control_plane::WorkspaceId::new(workspace.as_str()),
            ))
            .await
            .map_err(|err| err.to_string())?;
        Ok(snapshot.agents.into_iter().next())
    }

    async fn handle_interrupt_command(
        &self,
        handle: &WorkspaceHandle,
        workspace: &WorkspaceId,
    ) -> Result<(), String> {
        self.core
            .interrupt_turn(InterruptTurnRequest {
                workspace_id: lucarne::control_plane::WorkspaceId::new(workspace.as_str()),
            })
            .await
            .map_err(|err| err.to_string())?;
        self.channel
            .send(
                handle,
                OutgoingMessage::plain("interrupted current turn").silent(),
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn handle_kill_command(
        &self,
        handle: &WorkspaceHandle,
        scope: AgentResourceScope,
        args: &str,
    ) -> Result<(), String> {
        let Some(target) = parse_kill_target(args) else {
            self.channel
                .send(
                    handle,
                    OutgoingMessage::markdown("usage: `/kill all` or `/kill <session_id:pid>`")
                        .silent(),
                )
                .await
                .map_err(|err| err.to_string())?;
            return Ok(());
        };
        let report = self
            .core
            .kill_agent_processes(KillAgentRequest { scope, target })
            .await
            .map_err(|err| err.to_string())?;
        for killed in &report.killed {
            self.state
                .mark_live_dead(
                    &WorkspaceId::new(killed.workspace_id.as_str()),
                    Some(killed.native_resume_ref.to_string()),
                )
                .map_err(|err| err.to_string())?;
        }
        self.channel
            .send(
                handle,
                OutgoingMessage::markdown(render_kill_agent_report(&report)).silent(),
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn finish_immediate_agent_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        plan: &CommandInvocationPlan,
        result: AgentCommandResult,
        provider_ref_before: Option<String>,
        reply_to: Option<&MessageId>,
    ) -> Result<(), String> {
        if let AgentCommandResultData::ForkTargets(catalog) = &result.data {
            if let Err(err) = self
                .state
                .record_fork_target_selection(&session.workspace, catalog)
            {
                let _ = self.state.fail_command_workflow(workflow, &err);
                return Err(err);
            }
        }
        let payload = serde_json::to_value(&result)
            .map_err(|err| format!("failed to serialize immediate command result: {err}"))?;
        let item = match self.state.append_turn_timeline(
            &session.workspace,
            &workflow.turn_id,
            TimelineItemKind::CommandResult,
            payload.clone(),
        ) {
            Ok(item) => item,
            Err(err) => {
                let _ = self.state.fail_command_workflow(workflow, &err);
                return Err(err);
            }
        };
        let mut options =
            self.command_run_options_for_session(handle, session, plan.completion_policy);
        if plan.name.as_str() == "status" {
            options.status_snapshot = self.state.status_snapshot(&session.workspace);
            options.status_resource = self.agent_status_resource(&session.workspace).await?;
        }
        let message =
            match turn::render_immediate_command_timeline_item(handle, &item, Some(&options)) {
                Ok(message) => message,
                Err(err) => {
                    let _ = self.state.fail_command_workflow(workflow, &err);
                    return Err(err);
                }
            };
        if let Err(err) =
            self.state
                .complete_command_workflow(workflow, plan.completion_policy, payload)
        {
            let _ = self.state.fail_command_workflow(workflow, &err);
            return Err(err);
        }
        if let Some(mut message) = message {
            if let Some(reply_to) = reply_to {
                if message.reply_to.is_none() {
                    message = message.reply_to(reply_to.clone());
                }
            }
            let message_ids = self
                .deliver_workspace_message(handle, message, PanelDelivery::Send)
                .await?;
            self.bind_notification_command_messages(handle, session, &message_ids)?;
        }
        self.finish_agent_command(
            &session.workspace,
            live,
            plan.name.as_str(),
            provider_ref_before,
        )
        .await
    }

    async fn invoke_session_fork_command(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        workflow: &RunningCommandWorkflow,
        target: &str,
        provider_ref_before: Option<String>,
    ) -> Result<(), String> {
        let fork_result = live
            .session
            .fork(AgentForkSelection {
                target_id: target.into(),
            })
            .await
            .map_err(|err| {
                let err = err.to_string();
                let _ = self.state.fail_command_workflow(workflow, &err);
                err
            })?;
        let result = AgentCommandResult {
            name: "fork".into(),
            source: AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Fork(fork_result.clone()),
        };
        let payload = serde_json::to_value(&result)
            .map_err(|err| format!("failed to serialize fork command result: {err}"))?;
        if let Err(err) = self.state.append_turn_timeline(
            &session.workspace,
            &workflow.turn_id,
            TimelineItemKind::CommandResult,
            payload.clone(),
        ) {
            let _ = self.state.fail_command_workflow(workflow, &err);
            return Err(err);
        }
        if let Err(err) = self
            .finish_fork_command_result(
                handle,
                live,
                session,
                fork_result,
                provider_ref_before,
                true,
            )
            .await
        {
            let _ = self.state.fail_command_workflow(workflow, &err);
            return Err(err);
        }
        self.state
            .complete_command_workflow(workflow, CommandCompletionPolicy::CommandResult, payload)
            .map_err(|e| e.to_string())
    }

    async fn finish_fork_command_result(
        &self,
        handle: &WorkspaceHandle,
        live: &Arc<LiveSession>,
        session: &WorkSession,
        result: AgentForkResult,
        provider_ref_before: Option<String>,
        allow_live_only_fork: bool,
    ) -> Result<(), String> {
        let result_source_ref = result
            .source_session_ref
            .as_ref()
            .map(|session_ref| session_ref.0.to_string());
        let result_fork_ref = match result.session_ref.as_ref() {
            Some(session_ref) => Some(session_ref.0.to_string()),
            None if allow_live_only_fork => None,
            None => live_provider_resume_ref(live).await,
        };
        let resolution = resolve_fork_session_refs(
            result_source_ref,
            result_fork_ref,
            provider_ref_before,
            session.resume_ref.clone(),
            allow_live_only_fork,
        )
        .map_err(fork_resolution_error_message)?;
        let (source_ref, fork_workspace_ref, fork_resume_ref, message) = match resolution {
            ForkSessionResolution::LiveOnly { source_ref } => {
                let provisional_ref = provisional_live_resume_ref(live);
                (
                    source_ref,
                    provisional_ref,
                    None,
                    OutgoingMessage::markdown("✓ forked\n\nSend a message to start this fork.")
                        .silent(),
                )
            }
            ForkSessionResolution::Resumable {
                source_ref,
                fork_ref,
            } => {
                let message = OutgoingMessage::markdown(format!(
                    "✓ forked `{fork_ref}`\n\nContinue in this fork."
                ))
                .silent();
                (source_ref, fork_ref.clone(), Some(fork_ref), message)
            }
        };
        let fork_live = if fork_resume_ref.is_none() {
            Some(live.clone())
        } else {
            self.detach_core_live_session(
                &session.workspace,
                live,
                "fork command split live sessions",
            )
            .await?;
            self.state
                .mark_live_dead(&session.workspace, Some(source_ref.clone()))
                .map_err(|e| e.to_string())?;
            None
        };
        let fork_prewarm = fork_resume_ref.as_ref().map(|resume_ref| ResumePrewarm {
            resume_ref: resume_ref.clone(),
            retry_history_idx: None,
        });
        let fork_session = WorkSession {
            workspace: workspace_id_for_resume(session.provider_id, &fork_workspace_ref),
            chat: handle.chat.clone(),
            provider_id: session.provider_id,
            project_path: session.project_path.clone(),
            title: format!("{} · fork", session.title),
            live: fork_live,
            resume_ref: fork_resume_ref.clone(),
        };
        self.state
            .record_fork_transition(&session.workspace, source_ref, &fork_session)
            .map_err(|e| e.to_string())?;
        let fork_workspace = fork_session.workspace.clone();
        self.state
            .upsert_with_topic_replacing_resume_ref(fork_session, handle.workspace.clone())
            .map_err(|e| e.to_string())?;
        if let Some(prewarm) = fork_prewarm {
            let mut stored = self
                .state
                .get(&fork_workspace)
                .expect("fork workspace just inserted");
            if stored.resume_ref.is_none() {
                stored.resume_ref = Some(prewarm.resume_ref);
            }
            if let Err(err) = self.ensure_live_bound(&mut stored, false).await {
                self.send_resume_error(handle, session.provider_id, None, &err)
                    .await?;
                return Ok(());
            }
        }
        send_with_fallback(&*self.channel, handle, message, session.provider_id)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn send_command_plan_error(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
        err: CommandPlanError,
    ) -> Result<(), String> {
        match err {
            CommandPlanError::Unsupported { name } => {
                self.send_unsupported_agent_command(handle, session, name.as_str())
                    .await
            }
            CommandPlanError::MissingRequiredArgs { name, label } => {
                let msg = OutgoingMessage::plain(format!(
                    "/{} requires arguments: {}\nSend /commands {} {}",
                    name, label, name, label
                ));
                self.channel
                    .send(handle, msg)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
        }
    }

    async fn send_command_help_message(
        &self,
        handle: &WorkspaceHandle,
        command: &AgentCommand,
    ) -> Result<(), String> {
        self.channel
            .send(
                handle,
                OutgoingMessage::markdown(render_command_help(command)).silent(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn send_entry_command_help(&self, command: &AgentCommand) -> Result<(), String> {
        self.channel
            .send(
                &self.entry,
                OutgoingMessage::markdown(render_command_help(command)).silent(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn send_workspace_help_message(&self, handle: &WorkspaceHandle) -> Result<(), String> {
        let body = "workspace commands\n\
`/help` - show this help\n\
`/config` - show effective config\n\
`/config workspace|session bypass|notifications on|off` - set scoped config\n\
`/commands` - list bound agent commands\n\
`/commands <command> help` - show help for one command\n\
`/model [model] [reasoning]` - show or set the model\n\
`/permissions [mode]` - show or set permissions\n\
`/skills` - list available skills\n\
`/status` - show status plus process resources\n\
`/interrupt` - interrupt the current turn\n\
`/kill all|<session_id:pid>` - kill managed agent processes\n\
`/fork [target]` - list fork targets or fork one target\n\
`/new` - start a new conversation\n\
`/quit` - close the live session\n\
`/rename <name>` - rename this workspace";
        self.channel
            .send(handle, OutgoingMessage::markdown(body).silent())
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn finish_agent_command(
        &self,
        workspace: &WorkspaceId,
        live: &Arc<LiveSession>,
        command_name: &str,
        provider_ref_before: Option<String>,
    ) -> Result<(), String> {
        match command_name {
            "new" => {
                let provider_ref_after = live_provider_resume_ref(live).await;
                if provider_ref_after.is_some() && provider_ref_after != provider_ref_before {
                    self.state
                        .bind_live_replacing_resume_ref(workspace, live.clone(), provider_ref_after)
                        .map_err(|e| e.to_string())?;
                } else {
                    self.detach_core_live_session(
                        workspace,
                        live,
                        "new command cleared live session",
                    )
                    .await?;
                    self.state
                        .clear_live_and_resume_ref(workspace)
                        .map_err(|e| e.to_string())?;
                }
            }
            "quit" => {
                let resume_ref = live_provider_resume_ref(live).await.or(provider_ref_before);
                self.detach_core_live_session(workspace, live, "quit command")
                    .await?;
                self.state
                    .mark_live_dead(workspace, resume_ref)
                    .map_err(|e| e.to_string())?;
            }
            _ => {
                let provider_ref_after = live_provider_resume_ref(live).await;
                let keep_live_only = self.should_keep_live_only_resume_ref(
                    workspace,
                    provider_ref_before.as_deref(),
                    provider_ref_after.as_deref(),
                );
                if keep_live_only {
                    self.state
                        .bind_live(workspace, live.clone(), None)
                        .map_err(|e| e.to_string())?;
                } else if let Some(resume_ref) = provider_ref_after {
                    self.state
                        .bind_live(workspace, live.clone(), Some(resume_ref))
                        .map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }

    fn should_keep_live_only_resume_ref(
        &self,
        workspace: &WorkspaceId,
        provider_ref_before: Option<&str>,
        provider_ref_after: Option<&str>,
    ) -> bool {
        if let Some(source_ref) = self.state.live_only_fork_source_ref(workspace) {
            return provider_ref_after.is_none() || provider_ref_after == Some(source_ref.as_str());
        }
        let Ok(active_ref) = self.state.active_provider_session_ref(workspace) else {
            return false;
        };
        is_provisional_live_resume_ref(&active_ref)
            && (provider_ref_after.is_none() || provider_ref_after == provider_ref_before)
    }

    async fn send_unsupported_agent_command(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
        command: &str,
    ) -> Result<(), String> {
        let msg = OutgoingMessage::plain(format!(
            "Unsupported command /{} for {}.",
            command, session.provider_id
        ))
        .silent();
        let _ = self.channel.send(handle, msg).await;
        Ok(())
    }

    fn bind_notification_command_messages(
        &self,
        handle: &WorkspaceHandle,
        session: &WorkSession,
        message_ids: &[MessageId],
    ) -> Result<(), String> {
        if !self.state.is_notification_handle(handle) || message_ids.is_empty() {
            return Ok(());
        }
        let provider_session_id = self.state.active_provider_session_id(&session.workspace)?;
        self.register_message_session_bindings(&handle.chat, message_ids, provider_session_id)
    }

    async fn deliver_workspace_message(
        &self,
        handle: &WorkspaceHandle,
        msg: OutgoingMessage,
        delivery: PanelDelivery,
    ) -> Result<Vec<MessageId>, String> {
        match delivery {
            PanelDelivery::Send => self
                .channel
                .send_all(handle, msg)
                .await
                .map_err(|e| e.to_string()),
            PanelDelivery::Edit(message_id) => {
                self.channel
                    .edit(handle, &message_id, msg)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(Vec::new())
            }
        }
    }

    async fn ensure_live_bound(
        &self,
        session: &mut WorkSession,
        force_bypass_permissions: bool,
    ) -> Result<Arc<LiveSession>, String> {
        if let Some(live) = session.live.as_ref().cloned() {
            let observed_close = live.session.observed_close_reason().await;
            if observed_close.is_none() {
                let provider_ref_after = live_provider_resume_ref(&live).await;
                let core_workspace_id =
                    lucarne::control_plane::WorkspaceId::new(session.workspace.as_str());
                let core_live_instance_id = lucarne::control_plane::LiveInstanceId::new(
                    live.session.instance_id().0.as_str(),
                );
                if !self
                    .core
                    .live_session_is_bound(&core_workspace_id, &core_live_instance_id)
                {
                    let resume_ref = match session.resume_ref.clone() {
                        Some(resume_ref) => Some(resume_ref),
                        None if self.should_keep_live_only_resume_ref(
                            &session.workspace,
                            None,
                            provider_ref_after.as_deref(),
                        ) =>
                        {
                            None
                        }
                        None => provider_ref_after.clone(),
                    };
                    warn!(
                        target: "lucarne_telegram::bot",
                        provider = session.provider_id,
                        workspace = %session.workspace.as_str(),
                        live_instance = %core_live_instance_id.as_str(),
                        "discarding live session missing from core runtime registry before turn"
                    );
                    self.state
                        .mark_live_dead(&session.workspace, resume_ref.clone())
                        .map_err(|e| e.to_string())?;
                    session.resume_ref = resume_ref;
                    session.live = None;
                } else if let (Some(expected_ref), Some(live_ref)) =
                    (session.resume_ref.clone(), provider_ref_after.clone())
                {
                    if expected_ref != live_ref {
                        warn!(
                            target: "lucarne_telegram::bot",
                            provider = session.provider_id,
                            workspace = %session.workspace.as_str(),
                            expected_ref = %expected_ref,
                            live_ref = %live_ref,
                            "discarding mismatched live session before command"
                        );
                        self.detach_core_live_session(
                            &session.workspace,
                            &live,
                            "live session provider ref mismatched workspace",
                        )
                        .await?;
                        self.state
                            .mark_live_dead(&session.workspace, Some(expected_ref.clone()))
                            .map_err(|e| e.to_string())?;
                        session.resume_ref = Some(expected_ref);
                        session.live = None;
                    } else {
                        let workspace = session.workspace.clone();
                        self.refresh_live_resume_ref(&workspace, session, &live)
                            .await?;
                        return Ok(live);
                    }
                } else {
                    let workspace = session.workspace.clone();
                    self.refresh_live_resume_ref(&workspace, session, &live)
                        .await?;
                    return Ok(live);
                }
            } else {
                let provider_ref_after = live_provider_resume_ref(&live).await;
                let resume_ref = match session.resume_ref.clone() {
                    Some(resume_ref) => Some(resume_ref),
                    None if self.should_keep_live_only_resume_ref(
                        &session.workspace,
                        None,
                        provider_ref_after.as_deref(),
                    ) =>
                    {
                        None
                    }
                    None => provider_ref_after,
                };
                warn!(
                    target: "lucarne_telegram::bot",
                    close_reason = observed_close.as_deref().unwrap_or(""),
                    provider = session.provider_id,
                    "discarding closed live session before command"
                );
                self.state
                    .mark_live_dead(&session.workspace, resume_ref.clone())
                    .map_err(|e| e.to_string())?;
                self.detach_core_live_session(
                    &session.workspace,
                    &live,
                    observed_close.as_deref().unwrap_or("closed live session"),
                )
                .await?;
                session.resume_ref = resume_ref;
                session.live = None;
            }
        }
        let live = match self
            .open_or_resume_live(session, force_bypass_permissions)
            .await
        {
            Ok(live) => live,
            Err(err) => {
                let _ = self.state.record_reconcile_outcome(
                    &session.workspace,
                    ReconcileOutcome::ProviderSessionStale,
                );
                return Err(err);
            }
        };
        let provider_ref_after = live_provider_resume_ref(&live).await;
        let keep_live_only = self.should_keep_live_only_resume_ref(
            &session.workspace,
            session.resume_ref.as_deref(),
            provider_ref_after.as_deref(),
        );
        let resume_ref = if keep_live_only {
            None
        } else {
            provider_ref_after.or_else(|| session.resume_ref.clone())
        };
        self.state
            .bind_live(&session.workspace, live.clone(), resume_ref.clone())
            .map_err(|e| e.to_string())?;
        session.resume_ref = resume_ref;
        session.live = Some(live.clone());
        Ok(live)
    }

    async fn detach_core_live_session(
        &self,
        workspace: &WorkspaceId,
        live: &Arc<LiveSession>,
        reason: &str,
    ) -> Result<(), String> {
        let workspace_id = lucarne::control_plane::WorkspaceId::new(workspace.as_str());
        let live_instance_id =
            lucarne::control_plane::LiveInstanceId::new(live.session.instance_id().0.as_str());
        self.core
            .detach_live_session(&workspace_id, &live_instance_id, reason)
            .await
            .map_err(|err| err.to_string())
    }

    async fn command_context(
        &self,
        workspace: WorkspaceId,
    ) -> Result<
        (
            WorkSession,
            WorkspaceHandle,
            Arc<LiveSession>,
            AgentCommandCatalog,
        ),
        String,
    > {
        let mut session = self
            .state
            .get(&workspace)
            .ok_or_else(|| format!("no session for workspace {}", workspace.as_str()))?;
        let handle = self
            .state
            .handle_for_session(&session)
            .map_err(|e| e.to_string())?;
        self.repair_history_replay_resume_ref(&mut session, &handle)?;
        let live = self.ensure_live_bound(&mut session, false).await?;
        let catalog = live
            .session
            .list_commands()
            .await
            .map_err(|e| e.to_string())?;
        Ok((session, handle, live, catalog))
    }

    async fn handle_command_query(&self, query: CommandQuery) -> Result<(), String> {
        let results = self.command_query_results(&query).await;
        self.channel
            .answer_command_query(&query, results)
            .await
            .map_err(|e| e.to_string())
    }

    async fn command_query_results(&self, query: &CommandQuery) -> Vec<CommandQueryResult> {
        if let Some(ws) = self.state.last_workspace_for_user(&query.user) {
            if let Some(session) = self.state.get(&ws) {
                if let Some(live) = session.live.as_ref() {
                    if let Ok(catalog) = live.session.list_commands().await {
                        return command_query_results_from_catalog(&query.query, &catalog);
                    }
                }
            }
        }
        Vec::new()
    }

    async fn fetch_text_attachment(
        &self,
        att: &IncomingAttachment,
    ) -> Result<Option<String>, String> {
        if !ingest::looks_textual(att) {
            return Ok(None);
        }
        let bytes = self
            .channel
            .download_attachment(att)
            .await
            .map_err(|e| e.to_string())?;
        let text = ingest::read_text(att, bytes, ingest::DEFAULT_MAX_TEXT_BYTES)
            .map_err(|e| e.to_string())?;
        Ok(Some(ingest::format_for_agent(att, &text)))
    }

    async fn fetch_image_attachment(
        &self,
        att: &IncomingAttachment,
    ) -> Result<Option<AgentImageInput>, String> {
        let Some(media_type) = image_media_type(att) else {
            return Ok(None);
        };
        if let Some(size) = att.size {
            if size > MAX_IMAGE_BYTES {
                return Err(format!(
                    "image is {} bytes, over Telegram bot download limit {}",
                    size, MAX_IMAGE_BYTES
                ));
            }
        }
        let bytes = self
            .channel
            .download_attachment(att)
            .await
            .map_err(|e| e.to_string())?;
        if bytes.is_empty() {
            return Err("image attachment was empty".into());
        }
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            return Err(format!(
                "image is {} bytes, over Telegram bot download limit {}",
                bytes.len(),
                MAX_IMAGE_BYTES
            ));
        }
        let data_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        Ok(Some(AgentImageInput {
            media_type: media_type.into(),
            data_base64: data_base64.into(),
        }))
    }

    #[instrument(skip(self), fields(data = %data))]
    async fn handle_button(
        self: Arc<Self>,
        chat: ChatId,
        workspace: Option<WorkspaceId>,
        data: String,
        source_message: MessageId,
    ) -> Result<(), String> {
        debug!(target: "lucarne_telegram::bot", "button click");
        if let Some(turn::IntvCallback::Token { token }) = turn::parse_intv_callback(&data) {
            return self
                .handle_intervention_button(chat, workspace, token, source_message)
                .await;
        }
        if let Some(token) = parse_subagent_button(&data) {
            return self.handle_subagent_button(chat, workspace, token).await;
        }
        if let Some(button) = parse_agent_command_button(&data) {
            return self
                .handle_agent_command_button(chat, workspace, button, source_message)
                .await;
        }
        if let Some(token) = data.strip_prefix("historyolder:c:") {
            return self.handle_history_older_button(token).await;
        }
        if let Some(idx) = data.strip_prefix("history:") {
            let idx: usize = idx.parse().map_err(|_| "bad history index".to_string())?;
            let snap = self.state.panel();
            return self
                .open_history_entry_with_filter(idx, snap.provider_filter, snap.workspace_filter)
                .await;
        }
        if let Some(provider) = data.strip_prefix("newagent:") {
            return self.open_new_project(provider).await;
        }
        if let Some((revision, view)) = parse_panel_view_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            let snap = self.state.panel();
            return self
                .edit_entry_panel_state(
                    view,
                    snap.provider_filter,
                    snap.workspace_filter,
                    0,
                    source_message,
                )
                .await;
        }
        if let Some((revision, provider_filter)) = parse_panel_provider_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            let snap = self.state.panel();
            return self
                .edit_entry_panel_state(snap.view, provider_filter, None, 0, source_message)
                .await;
        }
        if let Some((revision, workspace_index)) = parse_panel_workspace_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            let snap = self.state.panel();
            let workspace_filter = snap
                .history_workspaces
                .get(workspace_index - 1)
                .cloned()
                .ok_or_else(|| format!("workspace filter {workspace_index} out of range"))?;
            return self
                .edit_entry_panel_state(
                    PanelView::Sessions,
                    snap.provider_filter,
                    Some(workspace_filter),
                    0,
                    source_message,
                )
                .await;
        }
        if let Some((revision, provider_id)) = parse_panel_new_workspace_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            let snap = self.state.panel();
            let project_path = snap
                .workspace_filter
                .clone()
                .ok_or_else(|| "workspace new requires a selected workspace".to_string())?;
            return self
                .open_new_project_with_path(provider_id, Some(project_path))
                .await;
        }
        if let Some(revision) = parse_panel_config_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            let snap = self.state.panel();
            let workspace_filter = snap.workspace_filter.clone().ok_or_else(|| {
                "workspace notification config is only valid in details".to_string()
            })?;
            let enabled = self
                .core
                .effective_settings(Some(workspace_filter.as_path()), None)
                .notifications
                .enabled;
            self.core
                .set_workspace_notifications_enabled(&workspace_filter, !enabled)
                .map_err(|err| err.to_string())?;
            return self
                .edit_entry_panel_state(
                    PanelView::Sessions,
                    snap.provider_filter,
                    Some(workspace_filter),
                    snap.history_offset,
                    source_message,
                )
                .await;
        }
        if data == "noop" {
            return Ok(());
        }
        if let Some((revision, off)) = parse_panel_page_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            return self.edit_entry_panel_page(off, source_message).await;
        }
        if let Some(revision) = parse_panel_refresh_button(&data) {
            if !self.state.panel_revision_matches(revision) {
                self.state
                    .record_entry_panel_stale_revision(&chat, revision)?;
                return self.edit_entry_panel_page(0, source_message).await;
            }
            return self.edit_entry_panel_page(0, source_message).await;
        }
        if data == "help:rename" {
            return self.help_rename().await;
        }
        if data == "help:commands" {
            return self.help_commands().await;
        }
        warn!(target: "lucarne_telegram::bot", "unhandled callback data");
        Ok(())
    }

    async fn handle_subagent_button(
        self: Arc<Self>,
        chat: ChatId,
        workspace: Option<WorkspaceId>,
        token: String,
    ) -> Result<(), String> {
        let topic = workspace.ok_or_else(|| "subagent click without workspace".to_string())?;
        let Some((callback, link)) = self.state.resolve_subagent_callback(&token) else {
            return self.send_stale_subagent_button(chat, &topic).await;
        };
        let handle = WorkspaceHandle::new(chat.clone(), topic.clone());
        let Some(resolved_ws) = self.state.workspace_for_handle(&handle) else {
            return self.send_stale_subagent_button(chat, &topic).await;
        };
        if callback.topic != topic || callback.workspace != resolved_ws {
            return self.send_stale_subagent_button(chat, &topic).await;
        }
        let parent = self
            .state
            .get(&callback.workspace)
            .ok_or_else(|| format!("no session for workspace {}", callback.workspace.as_str()))?;
        let resume_ref = link
            .child_native_ref
            .as_ref()
            .map(|value| value.to_string())
            .or_else(|| {
                link.child_provider_session_id
                    .as_ref()
                    .map(|value| value.as_str().to_string())
            })
            .ok_or_else(|| {
                format!(
                    "subagent link {} is not resumable",
                    callback.link_id.as_str()
                )
            })?;
        let durable_ref = link
            .child_provider_session_id
            .as_ref()
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| resume_ref.clone());
        let title = subagent_workspace_title(&parent, &link);
        let existing = link
            .child_workspace_id
            .as_ref()
            .and_then(|workspace| self.state.get(&WorkspaceId::new(workspace.as_str())))
            .or_else(|| {
                self.state.all().into_iter().find(|session| {
                    session.provider_id == parent.provider_id
                        && session.resume_ref.as_deref() == Some(resume_ref.as_str())
                })
            });
        let session = WorkSession {
            workspace: existing
                .as_ref()
                .map(|session| session.workspace.clone())
                .unwrap_or_else(|| workspace_id_for_resume(parent.provider_id, &durable_ref)),
            chat: existing
                .as_ref()
                .map(|session| session.chat.clone())
                .unwrap_or_else(|| chat.clone()),
            provider_id: parent.provider_id,
            project_path: parent.project_path.clone(),
            title,
            live: existing.as_ref().and_then(|session| session.live.clone()),
            resume_ref: Some(resume_ref.clone()),
        };
        let project_display = parent
            .project_path
            .as_ref()
            .map(|path| path.display().to_string());
        let (session, _) = self
            .activate_workspace_request(WorkspaceActivationRequest {
                session,
                message: Self::resume_notice_message(&resume_ref, project_display.as_deref()),
                provider: parent.provider_id,
                prewarm: Some(ResumePrewarm {
                    resume_ref,
                    retry_history_idx: None,
                }),
            })
            .await?;
        self.state
            .attach_subagent_child_workspace(&callback.link_id, &session.workspace)
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    async fn handle_agent_command_button(
        &self,
        chat: ChatId,
        workspace: Option<WorkspaceId>,
        button: AgentCommandButton,
        source_message: MessageId,
    ) -> Result<(), String> {
        let ws = workspace.ok_or_else(|| "agent command click without workspace".to_string())?;
        let topic_handle = WorkspaceHandle {
            chat: chat.clone(),
            workspace: ws.clone(),
        };
        match button {
            AgentCommandButton::Token { token } => {
                let Some(callback) = self.state.resolve_command_callback(&token) else {
                    return self.send_stale_agent_command_button(chat, &ws).await;
                };
                let Some(resolved_ws) = self.state.workspace_for_handle(&topic_handle) else {
                    return self.send_stale_agent_command_button(chat, &ws).await;
                };
                if callback.topic != ws || callback.workspace != resolved_ws {
                    return self.send_stale_agent_command_button(chat, &ws).await;
                }
                let _workspace_turn_guard = self
                    .acquire_workspace_turn_slot(
                        &callback.workspace,
                        &topic_handle,
                        &source_message,
                    )
                    .await;
                let (session, handle, live, catalog) =
                    self.command_context(callback.workspace.clone()).await?;
                if catalog.revision != callback.catalog_revision {
                    return self.send_stale_agent_command_button(chat, &ws).await;
                }
                let cmd = TopicSlashCommand::new(callback.name, callback.args)
                    .with_values(callback.values);
                return self
                    .invoke_checked_agent_command(session, handle, live, catalog, cmd)
                    .await;
            }
        }
    }

    async fn invoke_checked_agent_command(
        &self,
        session: WorkSession,
        handle: WorkspaceHandle,
        live: Arc<LiveSession>,
        catalog: AgentCommandCatalog,
        cmd: TopicSlashCommand,
    ) -> Result<(), String> {
        self.invoke_agent_command(&handle, &live, &session, &catalog, cmd, None)
            .await
    }

    async fn send_stale_agent_command_button(
        &self,
        chat: ChatId,
        workspace: &WorkspaceId,
    ) -> Result<(), String> {
        let handle = WorkspaceHandle {
            chat,
            workspace: workspace.clone(),
        };
        self.channel
            .send(
                &handle,
                OutgoingMessage::plain("Stale command button. Send /commands to refresh.").silent(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn send_stale_intervention_button(
        &self,
        chat: ChatId,
        workspace: &WorkspaceId,
    ) -> Result<(), String> {
        let handle = WorkspaceHandle {
            chat,
            workspace: workspace.clone(),
        };
        self.channel
            .send(
                &handle,
                OutgoingMessage::plain("Stale approval button. Re-run the request.").silent(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn send_stale_subagent_button(
        &self,
        chat: ChatId,
        workspace: &WorkspaceId,
    ) -> Result<(), String> {
        let handle = WorkspaceHandle {
            chat,
            workspace: workspace.clone(),
        };
        self.channel
            .send(
                &handle,
                OutgoingMessage::plain("Stale subagent button. Run the turn again to refresh.")
                    .silent(),
            )
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Resolve a daemon intervention request from the topic where the
    /// user clicked the button.
    async fn handle_intervention_button(
        &self,
        chat: ChatId,
        workspace: Option<WorkspaceId>,
        token: String,
        source_message: MessageId,
    ) -> Result<(), String> {
        let ws = workspace.ok_or_else(|| "intervention click without workspace".to_string())?;
        let Some(callback) = self.state.resolve_intervention_callback(&token) else {
            return self.send_stale_intervention_button(chat, &ws).await;
        };
        let source_handle = WorkspaceHandle::new(chat.clone(), ws.clone());
        let source_workspace = self.state.workspace_for_handle(&source_handle);
        let source_matches_workspace_topic = source_workspace.as_ref().is_some_and(|resolved_ws| {
            callback.topic.as_ref() == Some(&ws) && callback.workspace == *resolved_ws
        });
        let source_matches_notification_hub = self.state.is_notification_handle(&source_handle);
        if !source_matches_workspace_topic && !source_matches_notification_hub {
            return self.send_stale_intervention_button(chat, &ws).await;
        }
        let session = self
            .state
            .get(&callback.workspace)
            .ok_or_else(|| format!("no session for workspace {}", callback.workspace.as_str()))?;
        let live = session
            .live
            .as_ref()
            .ok_or_else(|| "workspace has no live agent session".to_string())?;
        if live.session.instance_id() != &callback.live_instance {
            return self.send_stale_intervention_button(chat, &ws).await;
        }

        let req_id = callback.req_id;
        let (response, ack) = match callback.action {
            turn::IntvAction::Approve { allow } => {
                let decision = if allow {
                    ApprovalDecision::Allow
                } else {
                    ApprovalDecision::Deny
                };
                let ack = if allow { "✅ Allowed" } else { "❌ Denied" };
                (InterventionResponse::Approval(decision), ack)
            }
            turn::IntvAction::Answer { q_idx, values } => {
                // Single-select first-question flow: the answer vec
                // must have one QuestionAnswer per question. We pad
                // empty answers for any leading questions we skipped.
                let mut answers: Vec<QuestionAnswer> = (0..q_idx)
                    .map(|_| QuestionAnswer { values: Vec::new() })
                    .collect();
                answers.push(QuestionAnswer {
                    values: values.into_iter().map(Into::into).collect(),
                });
                let resp = InterventionResponse::Answers(QuestionResponse { answers });
                (resp, "✅ Answered")
            }
        };

        info!(
            target: "lucarne_telegram::bot",
            req_id = %req_id,
            workspace = %ws.as_str(),
            "resolving intervention"
        );
        self.core
            .resolve_live_request(
                &lucarne::control_plane::WorkspaceId::new(callback.workspace.as_str()),
                &req_id,
                response,
            )
            .await
            .map_err(|e| e.to_string())?;
        self.state
            .mark_live_instance_running(&callback.live_instance)
            .map_err(|e| e.to_string())?;
        self.state
            .remove_intervention_callbacks_for_request(&callback.live_instance, &req_id)
            .map_err(|e| e.to_string())?;

        // Delete the original prompt message so the conversation
        // stays clean. The ack is shown as a short-lived transient
        // message and then also deleted.
        let prompt_id = live
            .pending_intv
            .lock()
            .unwrap()
            .remove(&req_id)
            .unwrap_or(source_message);
        if let Err(e) = self.channel.delete(&source_handle, &prompt_id).await {
            debug!(
                target: "lucarne_telegram::bot",
                error = %e,
                "deleting intervention prompt failed (non-fatal)"
            );
        }

        let ack_msg = OutgoingMessage::plain(ack).silent();
        if let Ok(ack_id) = self.channel.send(&source_handle, ack_msg).await {
            // Fire-and-forget: let the user see the ack briefly, then
            // remove it so the thread doesn't accumulate UI chatter.
            let channel = self.channel.clone();
            let target = source_handle.clone();
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
                let _ = channel.delete(&target, &ack_id).await;
            });
        }
        Ok(())
    }

    async fn help_rename(&self) -> Result<(), String> {
        let body = "Inside a workspace, send `/rename <new name>` to rename it. `/status` shows the bound agent status and process resources.";
        let msg = OutgoingMessage::markdown(body).silent();
        send_with_fallback(&*self.channel, &self.entry, msg, "help")
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    #[cfg(test)]
    async fn open_history_entry(self: Arc<Self>, idx: usize) -> Result<(), String> {
        self.open_history_entry_with_filter(idx, None, None).await
    }

    async fn open_history_entry_with_filter(
        self: Arc<Self>,
        idx: usize,
        provider_filter: Option<String>,
        workspace_filter: Option<PathBuf>,
    ) -> Result<(), String> {
        let provider_ids =
            history_provider_ids(provider_filter.as_deref(), self.core.history_provider_ids());
        let item = self
            .core
            .history_entry_at_filtered(&provider_ids, workspace_filter.as_deref(), idx)
            .ok_or_else(|| format!("history index {idx} out of range"))?;
        let provider = self
            .core_provider_id(item.provider_id)
            .ok_or_else(|| format!("provider {} is not supported for resume", item.provider_id))?;
        let resume_ref = runtime_resume_ref_from_history(&item)?;
        let title = workspace_title_from_history(&item);
        let project_path = item.cwd.as_ref().map(PathBuf::from);
        self.open_resume_workspace(
            provider,
            resume_ref,
            title,
            project_path,
            item.cwd.clone(),
            Some(idx),
            Some(item),
        )
        .await
    }

    async fn open_resume_workspace(
        self: Arc<Self>,
        provider_id: &'static str,
        resume_ref: String,
        title: String,
        project_path: Option<PathBuf>,
        project_display: Option<String>,
        retry_history_idx: Option<usize>,
        history_entry: Option<HistoryEntry>,
    ) -> Result<(), String> {
        // Resolve any prior session for this resume target. Two ways the same
        // logical session may already exist: (a) the resume_ref still matches
        // (no provider rebind has happened yet), or (b) the provider replaced
        // the resume_ref with a fresh native session id but the deterministic
        // workspace_id is still the canonical one we'd compute from the panel
        // input. Either is sufficient to treat the session as "previously
        // seen" and therefore worth recycling its topic on a panel re-entry.
        let canonical_workspace_id = workspace_id_for_resume(provider_id, &resume_ref);
        let existing = self.state.all().into_iter().find(|s| {
            s.provider_id == provider_id
                && (s.resume_ref.as_deref() == Some(resume_ref.as_str())
                    || s.workspace == canonical_workspace_id)
        });
        let workspace_id = existing
            .as_ref()
            .map(|session| session.workspace.clone())
            .unwrap_or_else(|| canonical_workspace_id.clone());
        let provider = self
            .core_provider_id(provider_id)
            .ok_or_else(|| format!("unsupported provider {provider_id}"))?;

        let session = WorkSession {
            workspace: workspace_id.clone(),
            chat: existing
                .as_ref()
                .map(|session| session.chat.clone())
                .unwrap_or_else(|| self.entry.chat.clone()),
            provider_id: provider,
            project_path,
            title: title.clone(),
            live: existing.as_ref().and_then(|session| session.live.clone()),
            resume_ref: Some(resume_ref.clone()),
        };
        let (session, handle) = self
            .activate_workspace_request(WorkspaceActivationRequest {
                session,
                message: Self::resume_notice_message(&resume_ref, project_display.as_deref()),
                provider,
                prewarm: Some(ResumePrewarm {
                    resume_ref,
                    retry_history_idx,
                }),
            })
            .await?;
        if let Some(item) = history_entry.as_ref() {
            self.replay_history_batch(&session, &handle, item, None)
                .await?;
        }
        Ok(())
    }

    async fn handle_history_older_button(self: Arc<Self>, token: &str) -> Result<(), String> {
        let callback = self
            .state
            .resolve_history_older_callback(token)
            .ok_or_else(|| "history older callback is stale".to_string())?;
        let session = self
            .state
            .get(&callback.workspace)
            .ok_or_else(|| "history workspace is no longer available".to_string())?;
        let handle = WorkspaceHandle::new(session.chat.clone(), callback.topic.clone());
        let entry = HistoryEntry {
            provider_id: callback.provider_id,
            session_id: callback.session_id,
            session_path: callback.session_path,
            cwd: None,
            summary: String::new(),
            last_active_unix: 0,
            last_active_display: String::new(),
        };
        let cursor = HistoryCursor::new(callback.cursor);
        self.replay_history_batch(&session, &handle, &entry, Some(&cursor))
            .await
    }

    async fn replay_history_batch(
        &self,
        session: &WorkSession,
        handle: &WorkspaceHandle,
        item: &HistoryEntry,
        cursor: Option<&HistoryCursor>,
    ) -> Result<(), String> {
        self.replay_history_batch_with_mode(session, handle, item, cursor, false)
            .await
    }

    async fn replay_history_batch_with_mode(
        &self,
        session: &WorkSession,
        handle: &WorkspaceHandle,
        item: &HistoryEntry,
        cursor: Option<&HistoryCursor>,
        force_reload: bool,
    ) -> Result<(), String> {
        let mut replay = self
            .state
            .history_replay_record(&session.workspace, &session.chat, &handle.workspace)
            .filter(|record| {
                record.provider_id.as_str() == item.provider_id
                    && record.session_id.as_str() == item.session_id
                    && record.session_path == item.session_path
            })
            .unwrap_or_else(|| {
                self.state.new_history_replay_record(
                    &session.workspace,
                    &session.chat,
                    &handle.workspace,
                    item.provider_id,
                    item.session_id.clone(),
                    item.session_path.clone(),
                )
            });
        let reload_full_window = cursor.is_some() || force_reload;
        let replay_limit = if cursor.is_some() {
            // Load-older: extend the window by HISTORY_REPLAY_LIMIT past what
            // we have already replayed, so the user sees genuinely new turns
            // appended to the existing window.
            replay
                .replayed_turns
                .len()
                .saturating_add(HISTORY_REPLAY_LIMIT)
                .max(HISTORY_REPLAY_LIMIT)
        } else {
            // Fresh entry (force_reload) or first open: latest N turns only.
            HISTORY_REPLAY_LIMIT
        };
        let transcript_cursor = if reload_full_window { None } else { cursor };
        let transcript = match self.core.history_transcript_for_entry(
            item,
            replay_limit,
            transcript_cursor,
        ) {
            Ok(transcript) => transcript,
            Err(err) => {
                warn!(
                    target: "lucarne_telegram::bot",
                    provider_id = item.provider_id,
                    session_id = %item.session_id,
                    session_path = %item.session_path.display(),
                    error = %err,
                    "history transcript load failed"
                );
                let err_text = err.to_string();
                let msg =
                    OutgoingMessage::plain(format!("history load failed: {err_text}")).silent();
                send_with_fallback(&*self.channel, handle, msg, item.provider_id)
                        .await
                        .map(|_| ())
                        .map_err(|send_err| {
                            format!(
                                "history load failed: {err_text}; failed to notify Telegram: {send_err}"
                            )
                        })?;
                return Err(err_text);
            }
        };
        if transcript.turns.is_empty() {
            warn!(
                target: "lucarne_telegram::bot",
                provider_id = item.provider_id,
                session_id = %item.session_id,
                session_path = %item.session_path.display(),
                "history transcript had no visible user/assistant turns after filtering"
            );
            let msg = OutgoingMessage::plain("no user/assistant history found").silent();
            send_with_fallback(&*self.channel, handle, msg, item.provider_id)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())?;
            return Ok(());
        }

        if reload_full_window {
            for id in replay.projected_channel_message_ids() {
                if let Err(err) = self
                    .channel
                    .delete(handle, &MessageId::new(id.as_str()))
                    .await
                {
                    warn!(
                        target: "lucarne_telegram::bot",
                        provider_id = item.provider_id,
                        session_id = %item.session_id,
                        message_id = %id,
                        error = %err,
                        "history replay cleanup delete failed"
                    );
                }
            }
            replay.clear_replayed_turns();
            self.state.upsert_history_replay_record(replay.clone())?;
        }

        let older_cursor = transcript
            .older_cursor
            .as_ref()
            .map(|cursor| cursor.as_str().to_string());
        if let Some(cursor) = older_cursor.as_deref() {
            let data = match self.state.register_history_older_callback(
                &session.workspace,
                transcript.provider_id,
                &transcript.session_id,
                transcript.session_path.clone(),
                cursor,
            ) {
                Ok(data) => data,
                Err(err) => {
                    warn!(
                        target: "lucarne_telegram::bot",
                        provider_id = item.provider_id,
                        session_id = %item.session_id,
                        session_path = %item.session_path.display(),
                        error = %err,
                        "history older callback registration failed"
                    );
                    let msg =
                        OutgoingMessage::plain(format!("history load older unavailable: {err}"))
                            .silent();
                    send_with_fallback(&*self.channel, handle, msg, item.provider_id)
                        .await
                        .map(|_| ())
                        .map_err(|send_err| {
                            format!(
                                "history older callback registration failed: {err}; failed to notify Telegram: {send_err}"
                            )
                        })?;
                    return Err(err);
                }
            };
            let msg = history_older_control_message(data);
            if let Some(id) = replay.older_channel_message_id.as_ref() {
                if let Err(err) = self
                    .channel
                    .edit(handle, &MessageId::new(id.as_str()), msg)
                    .await
                {
                    warn!(
                        target: "lucarne_telegram::bot",
                        provider_id = item.provider_id,
                        session_id = %item.session_id,
                        message_id = %id,
                        error = %err,
                        "history older control edit failed"
                    );
                    let replacement =
                        history_older_control_message(self.state.register_history_older_callback(
                            &session.workspace,
                            transcript.provider_id,
                            &transcript.session_id,
                            transcript.session_path.clone(),
                            cursor,
                        )?);
                    let message_id =
                        send_with_fallback(&*self.channel, handle, replacement, item.provider_id)
                            .await
                            .map_err(|err| err.to_string())?;
                    replay.older_channel_message_id = Some(message_id.as_str().into());
                }
            } else {
                let message_id = send_with_fallback(&*self.channel, handle, msg, item.provider_id)
                    .await
                    .map_err(|err| err.to_string())?;
                replay.older_channel_message_id = Some(message_id.as_str().into());
            }
            replay.older_cursor = Some(cursor.into());
            self.remember_history_replay_record(&replay);
        } else {
            if let Some(id) = replay.older_channel_message_id.take() {
                if let Err(err) = self
                    .channel
                    .delete(handle, &MessageId::new(id.as_str()))
                    .await
                {
                    warn!(
                        target: "lucarne_telegram::bot",
                        provider_id = item.provider_id,
                        session_id = %item.session_id,
                        message_id = %id,
                        error = %err,
                        "history older control delete failed"
                    );
                }
            }
            replay.older_cursor = None;
            self.remember_history_replay_record(&replay);
        }

        for turn in &transcript.turns {
            let existing = replay.turn(&turn.id).cloned();
            let assistant_already_sent = existing
                .as_ref()
                .is_some_and(|record| record.assistant_sent);
            let existing_user_id = existing
                .as_ref()
                .and_then(|record| record.user_channel_message_id.as_ref());
            let images_already_sent = existing
                .as_ref()
                .map(|record| history_user_image_projection_count(record, turn.user.images.len()))
                .unwrap_or(0);
            let user_projection_complete = images_already_sent >= turn.user.images.len();
            if existing_user_id.is_some()
                && user_projection_complete
                && (turn.assistant.is_none() || assistant_already_sent)
            {
                continue;
            }

            let user_message_id = if let Some(id) = existing_user_id {
                MessageId::new(id.as_str())
            } else {
                let msg = OutgoingMessage::markdown(history_user_message_body(&turn.user)).silent();
                let message_id =
                    match send_with_fallback(&*self.channel, handle, msg, item.provider_id).await {
                        Ok(message_id) => message_id,
                        Err(err) => {
                            warn!(
                                target: "lucarne_telegram::bot",
                                provider_id = item.provider_id,
                                session_id = %item.session_id,
                                turn_id = %turn.id,
                                error = %err,
                                "history user marker projection failed"
                            );
                            self.persist_history_replay_record_after_projection_error(&replay);
                            return Err(err.to_string());
                        }
                    };
                replay.mark_user_sent(&turn.id, message_id.as_str());
                self.remember_history_replay_record(&replay);
                message_id
            };

            for image in turn.user.images.iter().skip(images_already_sent) {
                let upload = match history_image_upload(
                    &image.image_url,
                    history_message_timestamp(&turn.user),
                    user_message_id.clone(),
                ) {
                    Ok(Some(upload)) => upload,
                    Ok(None) => {
                        warn!(
                            target: "lucarne_telegram::bot",
                            provider_id = item.provider_id,
                            session_id = %item.session_id,
                            turn_id = %turn.id,
                            "history image source is not uploadable"
                        );
                        continue;
                    }
                    Err(err) => {
                        self.persist_history_replay_record_after_projection_error(&replay);
                        return Err(err);
                    }
                };
                let message_id = match self.channel.send_file(handle, upload).await {
                    Ok(message_id) => message_id,
                    Err(err) => {
                        warn!(
                            target: "lucarne_telegram::bot",
                            provider_id = item.provider_id,
                            session_id = %item.session_id,
                            turn_id = %turn.id,
                            error = %err,
                            "history image projection failed"
                        );
                        self.persist_history_replay_record_after_projection_error(&replay);
                        return Err(err.to_string());
                    }
                };
                replay.mark_user_image_sent(&turn.id, message_id.as_str());
                self.remember_history_replay_record(&replay);
            }

            if let Some(assistant) = turn.assistant.as_ref() {
                if !assistant_already_sent {
                    let msg = OutgoingMessage::markdown(history_reply_message_body(assistant))
                        .reply_to(user_message_id)
                        .silent();
                    let message_id =
                        match send_with_fallback(&*self.channel, handle, msg, item.provider_id)
                            .await
                        {
                            Ok(message_id) => message_id,
                            Err(err) => {
                                warn!(
                                    target: "lucarne_telegram::bot",
                                    provider_id = item.provider_id,
                                    session_id = %item.session_id,
                                    turn_id = %turn.id,
                                    error = %err,
                                    "history assistant reply projection failed"
                                );
                                self.persist_history_replay_record_after_projection_error(&replay);
                                return Err(err.to_string());
                            }
                        };
                    replay.mark_assistant_sent(&turn.id, message_id.as_str());
                    self.remember_history_replay_record(&replay);
                }
            }
        }
        self.persist_history_replay_record(&replay)?;

        Ok(())
    }

    fn remember_history_replay_record(&self, replay: &HistoryReplayRecord) {
        self.state.remember_history_replay_record(replay.clone());
    }

    fn persist_history_replay_record(&self, replay: &HistoryReplayRecord) -> Result<(), String> {
        self.state.upsert_history_replay_record(replay.clone())
    }

    fn persist_history_replay_record_after_projection_error(&self, replay: &HistoryReplayRecord) {
        if let Err(err) = self.persist_history_replay_record(replay) {
            warn!(
                target: "lucarne_telegram::bot",
                error = %err,
                "history replay partial persistence failed"
            );
        }
    }

    async fn activate_workspace_request(
        &self,
        request: WorkspaceActivationRequest,
    ) -> Result<(WorkSession, WorkspaceHandle), String> {
        let workspace = request.session.workspace.clone();
        let provider = request.provider;
        let replace_resume_ref = request.prewarm.is_some();
        let (mut session, mut handle, plan) = self
            .activate_workspace_binding(request.session, replace_resume_ref)
            .await?;
        if let Some(prewarm) = request.prewarm {
            let mut stored = self.state.get(&workspace).expect("just inserted");
            if stored.resume_ref.is_none() {
                stored.resume_ref = Some(prewarm.resume_ref.clone());
            }
            match self.ensure_live_bound(&mut stored, false).await {
                Ok(_) => {
                    session = stored;
                    if plan.reconcile_outcome == ReconcileOutcome::ProviderSessionProbeRequired {
                        self.state
                            .record_reconcile_outcome(&workspace, ReconcileOutcome::Ok)
                            .map_err(|e| e.to_string())?;
                    }
                }
                Err(e) => {
                    session = stored;
                    self.send_resume_error(&handle, provider, prewarm.retry_history_idx, &e)
                        .await?;
                    return Ok((session, handle));
                }
            }
            self.send_workspace_message(&mut session, &mut handle, request.message, provider)
                .await?;
            return Ok((session, handle));
        }
        if session.live.is_some()
            || (plan.requires_provider_probe() && session.resume_ref.is_some())
        {
            let mut stored = self.state.get(&workspace).expect("just inserted");
            match self.ensure_live_bound(&mut stored, false).await {
                Ok(_) => {
                    session = stored;
                    if plan.reconcile_outcome == ReconcileOutcome::ProviderSessionProbeRequired {
                        self.state
                            .record_reconcile_outcome(&workspace, ReconcileOutcome::Ok)
                            .map_err(|e| e.to_string())?;
                    }
                }
                Err(e) => {
                    session = stored;
                    self.send_resume_error(&handle, provider, None, &e).await?;
                    return Ok((session, handle));
                }
            }
        }
        self.send_workspace_message(&mut session, &mut handle, request.message, provider)
            .await?;
        Ok((session, handle))
    }

    async fn activate_workspace_binding(
        &self,
        mut session: WorkSession,
        replace_resume_ref: bool,
    ) -> Result<(WorkSession, WorkspaceHandle, ActivationPlan), String> {
        let mut plan = self.state.plan_activation_for_session(&session)?;
        let mut reconcile_outcome = plan.reconcile_outcome;
        let preferred_topic = self.state.topic_for_workspace(&session.workspace);
        let mut handle = if let Some(topic) = preferred_topic.as_ref() {
            WorkspaceHandle::new(session.chat.clone(), topic.clone())
        } else if let Some(topic) = plan
            .channel_binding
            .as_ref()
            .and_then(|binding| binding.topic_id.as_ref())
        {
            WorkspaceHandle::new(session.chat.clone(), WorkspaceId::new(topic.as_str()))
        } else {
            let handle = self
                .channel
                .create_workspace(&session.chat, &session.title)
                .await
                .map_err(|e| e.to_string())?;
            reconcile_outcome = ReconcileOutcome::TopicMissingRecreated;
            handle
        };
        if preferred_topic.is_some() || plan.requires_topic_probe() {
            match self.channel.probe_workspace(&handle).await {
                Ok(()) => {}
                Err(ChannelError::WorkspaceNotFound(_)) => {
                    handle = self
                        .channel
                        .create_workspace(&session.chat, &session.title)
                        .await
                        .map_err(|e| e.to_string())?;
                    reconcile_outcome = ReconcileOutcome::TopicMissingRecreated;
                }
                Err(ChannelError::Unsupported(_)) => {
                    reconcile_outcome = ReconcileOutcome::ManualAttentionRequired;
                }
                Err(e) => {
                    self.state
                        .record_reconcile_outcome(
                            &session.workspace,
                            ReconcileOutcome::ManualAttentionRequired,
                        )
                        .map_err(|e| e.to_string())?;
                    return Err(e.to_string());
                }
            }
        }
        session.chat = handle.chat.clone();
        if replace_resume_ref {
            self.state
                .upsert_with_topic_replacing_resume_ref(session.clone(), handle.workspace.clone())
                .map_err(|e| e.to_string())?;
        } else {
            self.state
                .upsert_with_topic(session.clone(), handle.workspace.clone())
                .map_err(|e| e.to_string())?;
        }
        self.state
            .record_reconcile_outcome(&session.workspace, reconcile_outcome)
            .map_err(|e| e.to_string())?;
        plan.reconcile_outcome = reconcile_outcome;
        Ok((session, handle, plan))
    }

    async fn send_workspace_message(
        &self,
        session: &mut WorkSession,
        handle: &mut WorkspaceHandle,
        msg: OutgoingMessage,
        provider: &'static str,
    ) -> Result<(), String> {
        match send_with_fallback(&*self.channel, handle, msg.clone(), provider).await {
            Ok(_) => Ok(()),
            Err(ChannelError::WorkspaceNotFound(_)) => {
                *handle = self
                    .channel
                    .create_workspace(&session.chat, &session.title)
                    .await
                    .map_err(|e| e.to_string())?;
                session.chat = handle.chat.clone();
                self.state
                    .upsert_with_topic(session.clone(), handle.workspace.clone())
                    .map_err(|e| e.to_string())?;
                self.state
                    .record_reconcile_outcome(
                        &session.workspace,
                        ReconcileOutcome::TopicMissingRecreated,
                    )
                    .map_err(|e| e.to_string())?;
                send_with_fallback(&*self.channel, handle, msg, provider)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn resume_notice_message(session_id: &str, project_display: Option<&str>) -> OutgoingMessage {
        OutgoingMessage::markdown(format!(
            "✓ resumed `{}`\n\nProject: `{}`\n\nContinue the conversation below.",
            session_id,
            project_display.unwrap_or("(unknown)")
        ))
        .silent()
    }

    async fn send_resume_error(
        &self,
        handle: &WorkspaceHandle,
        provider: &'static str,
        _idx: Option<usize>,
        error: &str,
    ) -> Result<(), String> {
        let msg = OutgoingMessage::markdown(format!("⚠ agent start failed: {error}"));
        send_with_fallback(&*self.channel, handle, msg, provider)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn open_workspace_entry(self: Arc<Self>, idx: usize) -> Result<(), String> {
        let snap = self.state.panel();
        if matches!(snap.view, PanelView::Workspaces) {
            if let Some(workspace_filter) = snap.history_workspaces.get(idx - 1).cloned() {
                return self
                    .deliver_entry_panel_state(
                        PanelView::Sessions,
                        snap.provider_filter,
                        Some(workspace_filter),
                        0,
                        PanelDelivery::Send,
                    )
                    .await;
            }
        }
        let msg = OutgoingMessage::plain(format!(
            "/w{idx} is only available in the Workspaces view; send /panel and tap Workspaces"
        ))
        .silent();
        self.channel
            .send(&self.entry, msg)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }

    async fn open_new_project(self: Arc<Self>, provider_id: &str) -> Result<(), String> {
        self.open_new_project_with_path(provider_id, None).await
    }

    async fn open_new_project_with_path(
        self: Arc<Self>,
        provider_id: &str,
        project_path: Option<PathBuf>,
    ) -> Result<(), String> {
        let title = new_project_title(provider_id, project_path.as_deref());
        let Some(provider) = self.core_provider_id(provider_id) else {
            let msg = OutgoingMessage::plain(format!(
                "{provider_id} is a history-only provider in this panel. Starting a new live session is not supported here."
            ))
            .silent();
            self.channel
                .send(&self.entry, msg)
                .await
                .map_err(|e| e.to_string())?;
            return Ok(());
        };
        let project_line = project_path
            .as_ref()
            .map(|path| format!("\n\nProject: `{}`", path.display()))
            .unwrap_or_default();
        let msg = OutgoingMessage::markdown(format!(
            "New {provider_id} session{project_line}\n\nSend a message to start. Use /rename `<name>` to rename this workspace."
        ));
        let workspace_id = workspace_id_for_new_project(provider_id, self.state.all().len() + 1);
        self.activate_workspace_request(WorkspaceActivationRequest {
            session: WorkSession {
                workspace: workspace_id,
                chat: self.entry.chat.clone(),
                provider_id: provider,
                project_path,
                title,
                live: None,
                resume_ref: None,
            },
            message: msg,
            provider,
            prewarm: None,
        })
        .await?;
        Ok(())
    }

    async fn render_entry_panel(self: Arc<Self>) -> Result<(), String> {
        self.deliver_entry_panel_state(PanelView::Overview, None, None, 0, PanelDelivery::Send)
            .await
    }

    async fn render_entry_panel_page(self: Arc<Self>, offset: usize) -> Result<(), String> {
        let snap = self.state.panel();
        self.deliver_entry_panel_state(
            snap.view,
            snap.provider_filter,
            snap.workspace_filter,
            offset,
            PanelDelivery::Send,
        )
        .await
    }

    async fn edit_entry_panel_page(
        self: Arc<Self>,
        offset: usize,
        source_message: MessageId,
    ) -> Result<(), String> {
        let snap = self.state.panel();
        self.deliver_entry_panel_state(
            snap.view,
            snap.provider_filter,
            snap.workspace_filter,
            offset,
            PanelDelivery::Edit(source_message),
        )
        .await
    }

    async fn edit_entry_panel_state(
        self: Arc<Self>,
        view: PanelView,
        provider_filter: Option<String>,
        workspace_filter: Option<PathBuf>,
        offset: usize,
        source_message: MessageId,
    ) -> Result<(), String> {
        self.deliver_entry_panel_state(
            view,
            provider_filter,
            workspace_filter,
            offset,
            PanelDelivery::Edit(source_message),
        )
        .await
    }

    async fn deliver_entry_panel_state(
        self: Arc<Self>,
        view: PanelView,
        provider_filter: Option<String>,
        workspace_filter: Option<PathBuf>,
        offset: usize,
        delivery: PanelDelivery,
    ) -> Result<(), String> {
        let provider_ids =
            history_provider_ids(provider_filter.as_deref(), self.core.history_provider_ids());
        let history_provider_catalog = self.core.history_provider_catalog();
        let (history, history_workspaces, total) = if matches!(view, PanelView::Workspaces) {
            let workspace_page =
                self.core
                    .list_history_workspaces_page(&provider_ids, offset, PANEL_LIST_LIMIT);
            (Vec::new(), workspace_page.entries, workspace_page.total)
        } else {
            let history_page = self.core.list_history_filtered(
                &provider_ids,
                workspace_filter.as_deref(),
                offset,
                PANEL_LIST_LIMIT,
            );
            (history_page.entries, Vec::new(), history_page.total)
        };
        let runtime_provider_ids = self.core.provider_ids();
        let runtime_catalog = self.core.provider_catalog();
        let history_command_indices = history
            .iter()
            .enumerate()
            .filter_map(|(idx, entry)| {
                runtime_provider_ids
                    .contains(&entry.provider_id)
                    .then_some(offset + idx)
            })
            .collect::<Vec<_>>();
        let scanned_provider_ids = self.core.discovered_history_provider_ids();
        let agents = panel_agent_entries(
            &scanned_provider_ids,
            &history_provider_catalog,
            &runtime_catalog,
        );
        let attached_sessions = attached_session_entries(self.state.all());
        let observed_sessions = self.core.observed_recent_sessions();
        let visible_agents = agents
            .iter()
            .take(PANEL_LIST_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        let workspace_new_provider =
            if matches!(view, PanelView::Sessions) && workspace_filter.is_some() {
                workspace_new_provider(provider_filter.as_deref(), &history, runtime_provider_ids)
            } else {
                None
            };

        // Build the /tX index map first so rendering and dispatch stay
        // in sync.
        let snapshot = PanelSnapshot {
            view,
            provider_filter: provider_filter.clone(),
            workspace_filter: workspace_filter.clone(),
            agents: visible_agents
                .iter()
                .map(|agent| agent.provider_id.clone())
                .collect(),
            history: history_command_indices,
            workspaces: Vec::new(),
            history_workspaces: history_workspaces
                .iter()
                .map(|workspace| workspace.cwd.clone())
                .collect(),
            history_offset: offset,
            history_total: total,
        };
        let panel_revision = self.state.set_panel(snapshot);

        let page_end = (offset + PANEL_LIST_LIMIT).min(total);
        let settings = self.core.system_settings();

        let mut body = String::new();
        body.push_str("🛠 lucarne\n");
        body.push_str(&format!(
            "view: {} · provider: {}{}\n\n",
            panel_view_label(view),
            provider_filter
                .as_deref()
                .map(|provider_id| history_provider_label(provider_id, &history_provider_catalog))
                .unwrap_or_else(|| "All".into()),
            workspace_filter
                .as_ref()
                .map(|path| {
                    format!(" · cwd: {}", compact_path(&path.display().to_string(), 42))
                })
                .unwrap_or_default(),
        ));
        body.push_str(&render_config_panel_line(&settings));

        match view {
            PanelView::Overview => {
                render_overview_body(
                    &mut body,
                    &agents,
                    &history,
                    &attached_sessions,
                    &observed_sessions,
                    total,
                    page_end,
                    offset,
                    runtime_provider_ids,
                );
            }
            PanelView::Workspaces => {
                render_workspaces_body(&mut body, &history_workspaces, total, page_end, offset);
            }
            PanelView::Sessions => {
                render_sessions_body(
                    &mut body,
                    &history,
                    total,
                    page_end,
                    offset,
                    runtime_provider_ids,
                );
            }
        }

        // Minimal button row: pagination + global actions only. Items
        // themselves are addressed via /aN /hN /wN.
        let mut rows = panel_control_rows(
            view,
            provider_filter.as_deref(),
            panel_revision,
            &scanned_provider_ids,
            &history_provider_catalog,
        );
        if matches!(view, PanelView::Sessions) {
            if let Some(project_path) = workspace_filter.as_deref() {
                let mut workspace_actions = Vec::new();
                if let Some(provider_id) = workspace_new_provider {
                    workspace_actions.push(panel_new_workspace_button(panel_revision, provider_id));
                }
                let enabled = self
                    .core
                    .effective_settings(Some(project_path), None)
                    .notifications
                    .enabled;
                workspace_actions.push(panel_config_button(panel_revision, enabled));
                rows.push(workspace_actions);
            }
        }
        let mut nav: Vec<OutgoingButton> = Vec::new();
        if offset > 0 {
            let prev = offset.saturating_sub(PANEL_LIST_LIMIT);
            nav.push(OutgoingButton {
                label: "<".into(),
                data: format!("hist_page:{}:{prev}", panel_revision.get()),
            });
        }
        if page_end < total {
            nav.push(OutgoingButton {
                label: ">".into(),
                data: format!(
                    "hist_page:{}:{}",
                    panel_revision.get(),
                    offset + PANEL_LIST_LIMIT
                ),
            });
        }
        nav.push(OutgoingButton {
            label: format!(
                "{}-{} / {total}",
                if total == 0 { 0 } else { offset + 1 },
                page_end
            ),
            data: "noop".into(),
        });
        if !nav.is_empty() {
            rows.push(nav);
        }
        rows.push(action_button_row(panel_revision));

        let msg = OutgoingMessage::markdown(body).with_buttons(rows).silent();
        let message_id = match delivery {
            PanelDelivery::Send => send_with_fallback(&*self.channel, &self.entry, msg, "panel")
                .await
                .map_err(|e| e.to_string())?,
            PanelDelivery::Edit(source_message) => {
                self.channel
                    .edit(&self.entry, &source_message, msg)
                    .await
                    .map_err(|e| e.to_string())?;
                source_message
            }
        };
        self.state
            .record_entry_panel_render(&self.entry.chat, &message_id, panel_revision)?;
        Ok(())
    }

    async fn dispatch_entry_command(self: Arc<Self>, action: EntryAction) -> Result<(), String> {
        match action {
            EntryAction::Panel => self.render_entry_panel().await,
            EntryAction::Next => {
                let snap = self.state.panel();
                let next = snap.history_offset + PANEL_LIST_LIMIT;
                let target = if next >= snap.history_total {
                    snap.history_offset
                } else {
                    next
                };
                self.render_entry_panel_page(target).await
            }
            EntryAction::Prev => {
                let snap = self.state.panel();
                let target = snap.history_offset.saturating_sub(PANEL_LIST_LIMIT);
                self.render_entry_panel_page(target).await
            }
            EntryAction::Agent(idx) => {
                let snap = self.state.panel();
                let provider = snap
                    .agents
                    .get(idx - 1)
                    .cloned()
                    .ok_or_else(|| format!("/a{idx} out of range"))?;
                Arc::clone(&self).open_new_project(&provider).await
            }
            EntryAction::History(idx) => {
                let snap = self.state.panel();
                let global = *snap
                    .history
                    .get(idx - 1)
                    .ok_or_else(|| format!("/h{idx} out of range"))?;
                self.open_history_entry_with_filter(
                    global,
                    snap.provider_filter,
                    snap.workspace_filter,
                )
                .await
            }
            EntryAction::Workspace(idx) => Arc::clone(&self).open_workspace_entry(idx).await,
            EntryAction::ClearWorkspaces => Arc::clone(&self).clear_workspace_records().await,
            EntryAction::ResetNotifications => {
                let entry = self.entry.clone();
                self.reset_notification_topic(&entry).await
            }
            EntryAction::Help => self.help_commands().await,
        }
    }

    async fn clear_workspace_records(self: Arc<Self>) -> Result<(), String> {
        for session in self.state.all() {
            if let Some(live) = session.live {
                let _ = live.session.close().await;
            }
        }
        let cleared = self
            .state
            .clear_workspace_records()
            .map_err(|err| err.to_string())?;
        let msg = OutgoingMessage::plain(format!("cleared {cleared} workspace records")).silent();
        self.channel
            .send(&self.entry, msg)
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())?;
        self.render_entry_panel().await
    }

    async fn send_startup_help(&self) -> Result<(), String> {
        let body = format!("👋 lucarned started.\n\n{}", telegram_help_body());
        let msg = OutgoingMessage::plain(body).silent();
        self.channel
            .send(&self.entry, msg)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    async fn help_commands(&self) -> Result<(), String> {
        let msg = OutgoingMessage::plain(telegram_help_body()).silent();
        self.channel
            .send(&self.entry, msg)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}

fn telegram_help_body() -> &'static str {
    "commands\n\
            entry / notifications:\n\
            /panel /start    — refresh panel\n\
            /help            — show this help\n\
            /config          — show global config\n\
            /config global bypass|notifications on|off\n\
            /status          — show all agent resources\n\
            /kill all|<session_id:pid> — kill agent processes globally\n\
            /clear_workspaces — clear saved workspace records\n\
            /reset_notifications — recreate agent notifications topic\n\
            /aN              — start a new session with agent N\n\
            /hN              — resume history entry N (current page)\n\
            /wN              — open workspace filter N in Workspaces view\n\
            \n\
            inside a workspace:\n\
            /help            — show workspace help\n\
            /rename <name>   — rename the workspace\n\
            /config workspace|session bypass|notifications on|off\n\
            /commands [command] — list or invoke bound agent commands\n\
            /commands <command> help — show help for one command\n\
            /model [model] [reasoning] (/models) — show or set model\n\
            /permissions [mode] — show or set permissions\n\
            /skills          — list available skills\n\
            /status          — show status plus process resources\n\
            /interrupt       — interrupt the current turn\n\
            /kill all|<session_id:pid> — kill managed agent processes\n\
            /fork [target]   — list fork targets or fork one target\n\
            /fN              — fork listed target N\n\
            /new             — start a new conversation\n\
            /quit            — close the live session"
}

fn workspace_title_from_history(h: &HistoryEntry) -> String {
    let base = h.cwd.as_deref().unwrap_or(&h.summary);
    let short_base = base
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or(base);
    format!("{} · {}", h.provider_id, short(short_base, 32))
}

fn runtime_resume_ref_from_history(h: &HistoryEntry) -> Result<String, String> {
    let session_id = h.session_id.trim();
    if session_id.is_empty() {
        return Err(format!(
            "history entry has no provider session id: {}",
            h.session_path.display()
        ));
    }
    Ok(session_id.to_string())
}

fn workspace_id_for_resume(provider_id: &str, resume_ref: &str) -> WorkspaceId {
    WorkspaceId::new(format!(
        "{provider_id}:resume:{}",
        stable_resume_token(provider_id, resume_ref)
    ))
}

fn provisional_live_resume_ref(live: &LiveSession) -> String {
    format!("live:{}", live.session.instance_id().0.as_str())
}

fn is_provisional_live_resume_ref(resume_ref: &str) -> bool {
    resume_ref.starts_with("live:")
}

fn fork_resolution_error_message(error: ForkSessionResolutionError) -> String {
    match error {
        ForkSessionResolutionError::MissingSourceRef => {
            "fork requires a resumable provider session".into()
        }
        ForkSessionResolutionError::MissingForkRef => {
            "fork result did not include a resumable provider session".into()
        }
        ForkSessionResolutionError::ReusedSourceSession => {
            "fork result reused the source provider session".into()
        }
    }
}

#[cfg(test)]
fn provider_session_id_for_resume(
    provider_id: &str,
    resume_ref: &str,
) -> lucarne::control_plane::ProviderSessionId {
    lucarne::control_plane::ProviderSessionId::new(format!("{provider_id}:{resume_ref}"))
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

fn workspace_id_for_new_project(provider_id: &str, index: usize) -> WorkspaceId {
    WorkspaceId::new(format!("{provider_id}:new:{index}"))
}

fn new_project_title(provider_id: &str, project_path: Option<&Path>) -> String {
    let Some(project_path) = project_path else {
        return format!("{provider_id} · new");
    };
    let name = project_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("new");
    format!("{provider_id} · {}", short(name, 32))
}

#[cfg(test)]
fn resume_request_for_session(
    session: &WorkSession,
    reference: &str,
) -> lucarne::agent_runtime::ResumeSession {
    let mut req = lucarne::agent_runtime::ResumeSession {
        session_ref: SessionRef(reference.into()),
        ..Default::default()
    };
    if let Some(cwd) = session
        .project_path
        .as_ref()
        .and_then(|path| path.to_str())
        .filter(|cwd| !cwd.is_empty())
    {
        req.args = serde_json::json!({ "cwd": cwd });
    }
    req
}

fn short(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let prefix: String = s.chars().take(max.saturating_sub(1)).collect();
    format!("{prefix}…")
}

fn render_agent_notification(
    session: &WorkSession,
    session_ref: Option<&str>,
    text: &str,
) -> OutgoingMessage {
    let session_ref = session_ref.unwrap_or("-");
    let cwd = session
        .project_path
        .as_ref()
        .map(|path| compact_path(&path.display().to_string(), 58))
        .unwrap_or_else(|| "-".to_string());
    let body = render_agent_message_markdown(
        text,
        &AgentMessageFooter {
            cost: None,
            session: Some(session_ref.to_string()),
            cwd: Some(cwd),
        },
    );
    OutgoingMessage::markdown(body)
}

fn short_line(s: &str, max: usize) -> String {
    short(&s.split_whitespace().collect::<Vec<_>>().join(" "), max)
}

fn compact_path(path: &str, max: usize) -> String {
    lucarne_channel::agent_message::compact_path(path, max)
}

fn current_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn relative_time(last_active_unix: i64, fallback: &str) -> String {
    relative_time_from(current_unix(), last_active_unix, fallback)
}

fn relative_time_from(now_unix: i64, last_active_unix: i64, fallback: &str) -> String {
    if last_active_unix <= 0 {
        return if fallback.is_empty() {
            "unknown".into()
        } else {
            fallback.to_string()
        };
    }
    let seconds = if now_unix > last_active_unix {
        now_unix - last_active_unix
    } else {
        0
    };
    if seconds < 60 {
        return format!("{seconds}s ago");
    }

    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{minutes}m ago");
    }

    let hours = seconds / 3_600;
    if hours < 24 {
        return format!("{hours}h ago");
    }

    let days = seconds / 86_400;
    if days < 30 {
        let extra_hours = (seconds % 86_400) / 3_600;
        return if extra_hours > 0 {
            format!("{days}d {extra_hours}h ago")
        } else {
            format!("{days}d ago")
        };
    }

    if days < 365 {
        return format!("{}mo ago", days / 30);
    }

    let years = days / 365;
    let extra_months = (days % 365) / 30;
    if extra_months > 0 {
        format!("{years}y {extra_months}mo ago")
    } else {
        format!("{years}y ago")
    }
}

fn history_older_control_message(data: String) -> OutgoingMessage {
    OutgoingMessage::plain("Earlier history is available")
        .with_buttons(vec![vec![OutgoingButton {
            label: "Load older history".into(),
            data,
        }]])
        .silent()
}

fn history_user_image_projection_count(
    record: &lucarne::control_plane::HistoryReplayTurnRecord,
    expected_images: usize,
) -> usize {
    if !record.user_image_channel_message_ids.is_empty() {
        return record
            .user_image_channel_message_ids
            .len()
            .min(expected_images);
    }
    let mut count = record.projected_channel_message_ids.len();
    if record.user_channel_message_id.is_some() {
        count = count.saturating_sub(1);
    }
    if record.assistant_sent {
        count = count.saturating_sub(1);
    }
    count.min(expected_images)
}

fn history_user_message_body(message: &HistoryMessage) -> String {
    history_message_body(Some(HISTORY_USER_MARKER), message)
}

fn history_reply_message_body(message: &HistoryMessage) -> String {
    history_message_body(None, message)
}

fn history_message_body(prefix: Option<&str>, message: &HistoryMessage) -> String {
    let mut parts = Vec::new();
    if let Some(prefix) = prefix {
        parts.push(prefix.to_string());
    }
    let text = message.text.trim();
    if !text.is_empty() {
        parts.push(text.to_string());
    }
    if let Some(timestamp) = history_message_timestamp(message) {
        parts.push(timestamp);
    }
    parts.join("\n\n")
}

fn history_message_timestamp(message: &HistoryMessage) -> Option<String> {
    history_timestamp_label(message.timestamp.as_deref())
}

fn history_timestamp_label(timestamp: Option<&str>) -> Option<String> {
    let timestamp = timestamp?.trim();
    let (date, rest) = timestamp.split_once('T')?;
    let mut date_parts = date.split('-');
    let year = date_parts.next()?.parse::<u32>().ok()?;
    let month = date_parts.next()?.parse::<u32>().ok()?;
    let day = date_parts.next()?.parse::<u32>().ok()?;
    let time = rest.split(['Z', '+', '-']).next().unwrap_or(rest);
    let mut time_parts = time.split(':');
    let hour = time_parts.next()?;
    let minute = time_parts.next()?;
    Some(format!("🕒 {year}/{month}/{day} {hour}:{minute}"))
}

fn history_image_upload(
    image_url: &str,
    caption: Option<String>,
    reply_to: MessageId,
) -> Result<Option<FileUpload>, String> {
    let image_url = image_url.trim();
    if image_url.is_empty() {
        return Ok(None);
    }
    if let Some(upload) = history_data_url_upload(image_url, caption.clone(), reply_to.clone())? {
        return Ok(Some(upload));
    }
    if let Some(upload) = history_file_path_upload(image_url, caption, reply_to)? {
        return Ok(Some(upload));
    }
    Ok(None)
}

fn history_data_url_upload(
    image_url: &str,
    caption: Option<String>,
    reply_to: MessageId,
) -> Result<Option<FileUpload>, String> {
    let Some(rest) = image_url.strip_prefix("data:image/") else {
        return Ok(None);
    };
    let Some((media, data)) = rest.split_once(";base64,") else {
        return Ok(None);
    };
    let ext = history_image_extension(media);
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|err| format!("invalid history image data url: {err}"))?;
    Ok(Some(history_upload_from_bytes(
        ext, bytes, caption, reply_to,
    )))
}

fn history_file_path_upload(
    image_url: &str,
    caption: Option<String>,
    reply_to: MessageId,
) -> Result<Option<FileUpload>, String> {
    let path_text = image_url.strip_prefix("file://").unwrap_or(image_url);
    let path = std::path::Path::new(path_text);
    if !path.exists() {
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(image_url) {
            return Ok(Some(history_upload_from_bytes(
                "png", bytes, caption, reply_to,
            )));
        }
        return Ok(None);
    }
    let bytes = std::fs::read(path)
        .map_err(|err| format!("failed to read history image {}: {err}", path.display()))?;
    let ext = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(history_image_extension)
        .unwrap_or("png");
    Ok(Some(history_upload_from_bytes(
        ext, bytes, caption, reply_to,
    )))
}

fn history_upload_from_bytes(
    ext: &'static str,
    bytes: Vec<u8>,
    caption: Option<String>,
    reply_to: MessageId,
) -> FileUpload {
    let mut upload = FileUpload::new(format!("history-image.{ext}"), bytes)
        .reply_to(reply_to)
        .silent();
    if let Some(caption) = caption.filter(|caption| !caption.trim().is_empty()) {
        upload = upload.with_caption(caption);
    }
    upload
}

fn history_image_extension(media: &str) -> &'static str {
    match media
        .split(';')
        .next()
        .unwrap_or(media)
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "jpg" | "jpeg" | "image/jpeg" => "jpg",
        "webp" | "image/webp" => "webp",
        "gif" | "image/gif" => "gif",
        "png" | "image/png" => "png",
        _ => "png",
    }
}

#[derive(Debug, Clone, PartialEq)]
struct TopicSlashCommand {
    name: String,
    args: String,
    values: serde_json::Value,
}

impl TopicSlashCommand {
    fn new(name: impl Into<String>, args: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            args: args.into(),
            values: serde_json::Value::Null,
        }
    }

    fn with_values(mut self, values: serde_json::Value) -> Self {
        self.values = values;
        self
    }
}

enum AgentCommandButton {
    Token { token: String },
}

struct WorkspaceActivationRequest {
    session: WorkSession,
    message: OutgoingMessage,
    provider: &'static str,
    prewarm: Option<ResumePrewarm>,
}

struct ResumePrewarm {
    resume_ref: String,
    retry_history_idx: Option<usize>,
}

fn parse_agent_command_button(data: &str) -> Option<AgentCommandButton> {
    let rest = data.strip_prefix("agentcmd:")?;
    let token = rest.strip_prefix("c:")?.trim();
    if token.is_empty() || token.contains(':') {
        return None;
    }
    Some(AgentCommandButton::Token {
        token: token.to_string(),
    })
}

fn parse_subagent_button(data: &str) -> Option<String> {
    let token = data.strip_prefix("subagent:c:")?.trim();
    if token.is_empty() || token.contains(':') {
        return None;
    }
    Some(token.to_string())
}

fn parse_panel_page_button(data: &str) -> Option<(Revision, usize)> {
    let rest = data.strip_prefix("hist_page:")?;
    let (revision, offset) = rest.split_once(':')?;
    Some((Revision::new(revision.parse().ok()?), offset.parse().ok()?))
}

fn parse_panel_refresh_button(data: &str) -> Option<Revision> {
    let revision = data.strip_prefix("refresh:")?;
    Some(Revision::new(revision.parse().ok()?))
}

fn parse_panel_view_button(data: &str) -> Option<(Revision, PanelView)> {
    let rest = data.strip_prefix("panel_view:")?;
    let (revision, view) = rest.split_once(':')?;
    Some((
        Revision::new(revision.parse().ok()?),
        parse_panel_view(view)?,
    ))
}

fn parse_panel_provider_button(data: &str) -> Option<(Revision, Option<String>)> {
    let rest = data.strip_prefix("panel_provider:")?;
    let (revision, provider) = rest.split_once(':')?;
    let filter = match provider {
        "all" => None,
        id if is_provider_filter_token(id) => Some(id.to_string()),
        _ => return None,
    };
    Some((Revision::new(revision.parse().ok()?), filter))
}

fn is_provider_filter_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

fn parse_panel_workspace_button(data: &str) -> Option<(Revision, usize)> {
    let rest = data.strip_prefix("panel_workspace:")?;
    let (revision, index) = rest.split_once(':')?;
    let index = index.parse().ok()?;
    (index > 0).then_some((Revision::new(revision.parse().ok()?), index))
}

fn parse_panel_new_workspace_button(data: &str) -> Option<(Revision, &str)> {
    let rest = data.strip_prefix("panel_new_workspace:")?;
    let (revision, provider_id) = rest.split_once(':')?;
    is_provider_filter_token(provider_id)
        .then_some((Revision::new(revision.parse().ok()?), provider_id))
}

fn parse_panel_config_button(data: &str) -> Option<Revision> {
    let rest = data.strip_prefix("panel_config:")?;
    let (revision, action) = rest.split_once(':')?;
    (action == "notifications").then_some(Revision::new(revision.parse().ok()?))
}

fn parse_panel_view(view: &str) -> Option<PanelView> {
    match view {
        "overview" => Some(PanelView::Overview),
        "workspaces" => Some(PanelView::Workspaces),
        "sessions" => Some(PanelView::Sessions),
        _ => None,
    }
}

fn panel_view_id(view: PanelView) -> &'static str {
    match view {
        PanelView::Overview => "overview",
        PanelView::Workspaces => "workspaces",
        PanelView::Sessions => "sessions",
    }
}

fn panel_view_label(view: PanelView) -> &'static str {
    match view {
        PanelView::Overview => "Overview",
        PanelView::Workspaces => "Workspaces",
        PanelView::Sessions => "Sessions",
    }
}

fn history_provider_ids(
    filter: Option<&str>,
    available_provider_ids: &[&'static str],
) -> Vec<&'static str> {
    if let Some(filter) = filter {
        available_provider_ids
            .iter()
            .copied()
            .filter(|provider_id| *provider_id == filter)
            .collect()
    } else {
        available_provider_ids.to_vec()
    }
}

fn history_provider_label(provider_id: &str, catalog: &[HistoryProviderCatalogEntry]) -> String {
    catalog
        .iter()
        .find_map(|entry| (entry.provider_id == provider_id).then_some(entry.display_name))
        .unwrap_or(provider_id)
        .to_string()
}

fn panel_control_rows(
    view: PanelView,
    provider_filter: Option<&str>,
    revision: Revision,
    scanned_provider_ids: &[&'static str],
    history_provider_catalog: &[HistoryProviderCatalogEntry],
) -> Vec<Vec<OutgoingButton>> {
    let view_row = [
        PanelView::Overview,
        PanelView::Workspaces,
        PanelView::Sessions,
    ]
    .into_iter()
    .map(|candidate| OutgoingButton {
        label: if candidate == view {
            format!("✓ {}", panel_view_label(candidate))
        } else {
            panel_view_label(candidate).to_string()
        },
        data: format!("panel_view:{}:{}", revision.get(), panel_view_id(candidate)),
    })
    .collect::<Vec<_>>();

    let mut provider_row = Vec::new();
    provider_row.push(OutgoingButton {
        label: if provider_filter.is_none() {
            "✓ All".into()
        } else {
            "All".into()
        },
        data: format!("panel_provider:{}:all", revision.get()),
    });
    provider_row.extend(
        scanned_provider_ids
            .iter()
            .filter_map(|provider_id| {
                history_provider_catalog
                    .iter()
                    .find(|entry| entry.provider_id == *provider_id)
            })
            .map(|entry| OutgoingButton {
                label: if provider_filter == Some(entry.provider_id) {
                    format!("✓ {}", entry.display_name)
                } else {
                    entry.display_name.to_string()
                },
                data: format!("panel_provider:{}:{}", revision.get(), entry.provider_id),
            }),
    );

    vec![view_row, provider_row]
}

fn panel_config_button(revision: Revision, enabled: bool) -> OutgoingButton {
    OutgoingButton {
        label: if enabled {
            "🔔 notifications on".into()
        } else {
            "🔕 notifications off".into()
        },
        data: format!("panel_config:{}:notifications", revision.get()),
    }
}

fn panel_new_workspace_button(revision: Revision, provider_id: &str) -> OutgoingButton {
    OutgoingButton {
        label: "＋ new".into(),
        data: format!("panel_new_workspace:{}:{provider_id}", revision.get()),
    }
}

fn action_button_row(revision: Revision) -> Vec<OutgoingButton> {
    vec![OutgoingButton {
        label: "↻ refresh".into(),
        data: format!("refresh:{}", revision.get()),
    }]
}

fn history_agent_entries(
    provider_ids: &[&'static str],
    history_provider_catalog: &[HistoryProviderCatalogEntry],
    runtime_provider_ids: &[&'static str],
) -> Vec<agents::AgentEntry> {
    provider_ids
        .iter()
        .map(|provider_id| agents::AgentEntry {
            display_name: history_provider_label(provider_id, history_provider_catalog),
            provider_id: (*provider_id).into(),
            command: (*provider_id).into(),
            available: runtime_provider_ids.contains(provider_id),
        })
        .collect()
}

fn panel_agent_entries(
    scanned_provider_ids: &[&'static str],
    history_provider_catalog: &[HistoryProviderCatalogEntry],
    runtime_catalog: &[ProviderCatalogEntry],
) -> Vec<agents::AgentEntry> {
    let runtime_provider_ids = runtime_catalog
        .iter()
        .map(|provider| provider.provider_id)
        .collect::<Vec<_>>();
    history_agent_entries(
        scanned_provider_ids,
        history_provider_catalog,
        &runtime_provider_ids,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttachedSessionEntry {
    title: String,
    provider_id: &'static str,
    project_path: Option<PathBuf>,
}

fn attached_session_entries(sessions: Vec<WorkSession>) -> Vec<AttachedSessionEntry> {
    let mut entries = sessions
        .into_iter()
        .filter(|session| session.live.is_some())
        .map(|session| AttachedSessionEntry {
            title: session.title,
            provider_id: session.provider_id,
            project_path: session.project_path,
        })
        .collect::<Vec<_>>();
    entries.sort_by(|a, b| {
        a.title
            .cmp(&b.title)
            .then_with(|| a.provider_id.cmp(b.provider_id))
            .then_with(|| a.project_path.cmp(&b.project_path))
    });
    entries
}

fn workspace_new_provider(
    provider_filter: Option<&str>,
    history: &[HistoryEntry],
    runtime_provider_ids: &[&'static str],
) -> Option<&'static str> {
    if let Some(provider_filter) = provider_filter {
        return runtime_provider_ids
            .iter()
            .copied()
            .find(|provider_id| *provider_id == provider_filter);
    }
    history.iter().find_map(|entry| {
        runtime_provider_ids
            .iter()
            .copied()
            .find(|provider_id| *provider_id == entry.provider_id)
    })
}

fn render_overview_body(
    body: &mut String,
    agents: &[agents::AgentEntry],
    history: &[HistoryEntry],
    attached_sessions: &[AttachedSessionEntry],
    observed_sessions: &[ObservedAgentSession],
    total: usize,
    page_end: usize,
    offset: usize,
    runtime_provider_ids: &[&'static str],
) {
    body.push_str("overview\n");
    body.push_str(&format!(
        "agents: {} · attached: {} · observed: {} · sessions: {}\n\n",
        agents.len(),
        attached_sessions.len(),
        observed_sessions.len(),
        total
    ));
    render_agents_body(body, visible_panel_rows(agents));
    body.push('\n');
    render_attached_sessions_body(body, visible_panel_rows(attached_sessions));
    render_observed_sessions_body(body, visible_panel_rows(observed_sessions));
    render_sessions_body(body, history, total, page_end, offset, runtime_provider_ids);
}

fn visible_panel_rows<T>(rows: &[T]) -> &[T] {
    let end = rows.len().min(PANEL_LIST_LIMIT);
    &rows[..end]
}

fn render_attached_sessions_body(body: &mut String, sessions: &[AttachedSessionEntry]) {
    if sessions.is_empty() {
        return;
    }

    body.push_str("attached sessions\n");
    for session in sessions {
        body.push_str(&format!("• {} — {}", session.title, session.provider_id));
        if let Some(path) = session.project_path.as_ref() {
            body.push_str(&format!(
                " · {}",
                compact_path(&path.display().to_string(), 42)
            ));
        }
        body.push('\n');
    }
    body.push('\n');
}

fn render_observed_sessions_body(body: &mut String, sessions: &[ObservedAgentSession]) {
    if sessions.is_empty() {
        return;
    }

    body.push_str("recent observed\n");
    for session in sessions {
        body.push_str(&format!(
            "• {} — {} · {}\n",
            short_line(session.title.as_ref(), 72),
            session.provider_id,
            relative_time(session.last_active_unix, &session.last_active_display),
        ));
        if let Some(path) = session.cwd.as_ref() {
            body.push_str(&format!(
                "     📁 {}\n",
                compact_path(&path.display().to_string(), 48)
            ));
        }
        body.push_str(&format!("      `{}`\n", session.native_resume_ref));
        body.push('\n');
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigScope {
    Global,
    Workspace,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigSetting {
    Bypass,
    Notifications,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigUpdate {
    scope: ConfigScope,
    setting: ConfigSetting,
    enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigAction {
    scope: ConfigScope,
    update: Option<ConfigUpdate>,
}

fn parse_config_action(args: &str, has_workspace_context: bool) -> Result<ConfigAction, String> {
    let args = args.trim();
    if args.is_empty() {
        return Ok(ConfigAction {
            scope: if has_workspace_context {
                ConfigScope::Session
            } else {
                ConfigScope::Global
            },
            update: None,
        });
    }

    let parts = args.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [scope, setting, value] => {
            let scope = parse_config_scope(scope, has_workspace_context)?;
            let setting = parse_config_setting(setting)?;
            let enabled = parse_on_off(value)?;
            Ok(ConfigAction {
                scope,
                update: Some(ConfigUpdate {
                    scope,
                    setting,
                    enabled,
                }),
            })
        }
        _ => Err("usage: /config [global|workspace|session] <setting> <on|off>".into()),
    }
}

fn parse_config_scope(value: &str, has_workspace_context: bool) -> Result<ConfigScope, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "global" => Ok(ConfigScope::Global),
        "workspace" if has_workspace_context => Ok(ConfigScope::Workspace),
        "session" if has_workspace_context => Ok(ConfigScope::Session),
        "workspace" | "session" => {
            Err("workspace and session config require a workspace topic".into())
        }
        _ => Err("usage: /config [global|workspace|session] <setting> <on|off>".into()),
    }
}

fn parse_config_setting(value: &str) -> Result<ConfigSetting, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "bypass" => Ok(ConfigSetting::Bypass),
        "notifications" => Ok(ConfigSetting::Notifications),
        _ => Err("usage: /config [global|workspace|session] <setting> <on|off>".into()),
    }
}

fn parse_on_off(value: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" => Ok(true),
        "off" => Ok(false),
        _ => Err("usage: /config [global|workspace|session] <setting> <on|off>".into()),
    }
}

fn render_config_panel_line(settings: &SystemSettings) -> String {
    let bypass = on_off(settings.session.force_bypass_permissions);
    let notifications = on_off(settings.notifications.enabled);
    let config_toggle = if settings.session.force_bypass_permissions {
        "/config global bypass off"
    } else {
        "/config global bypass on"
    };
    format!(
        "⚙ config: global bypass {bypass} · notifications {notifications} · `{config_toggle}`\n\n"
    )
}

fn render_config_message(
    scope: ConfigScope,
    settings: &lucarne::control_plane::EffectiveSettings,
) -> String {
    format!(
        "⚙ config\n\
scope: `{}`\n\
bypass: `{}`\n\
notifications: `{}`\n\n\
usage: `/config [global|workspace|session] <setting> <on|off>`\n\
settings: `bypass`, `notifications`",
        config_scope_label(scope),
        on_off(settings.session.force_bypass_permissions),
        on_off(settings.notifications.enabled),
    )
}

fn config_scope_label(scope: ConfigScope) -> &'static str {
    match scope {
        ConfigScope::Global => "global",
        ConfigScope::Workspace => "workspace",
        ConfigScope::Session => "session",
    }
}

fn on_off(enabled: bool) -> &'static str {
    if enabled {
        "on"
    } else {
        "off"
    }
}

fn config_project_path(session: Option<&WorkSession>) -> Result<&Path, String> {
    session
        .and_then(|session| session.project_path.as_deref())
        .ok_or_else(|| "workspace config requires a project path".to_string())
}

fn config_provider_session_id(
    state: &BotState,
    session: Option<&WorkSession>,
) -> Result<lucarne::control_plane::ProviderSessionId, String> {
    let session = session.ok_or_else(|| "session config requires a workspace topic".to_string())?;
    state.active_provider_session_id(&session.workspace)
}

fn render_agents_body(body: &mut String, agents: &[agents::AgentEntry]) {
    body.push_str("agents\n");
    if agents.is_empty() {
        body.push_str("  (none)\n");
        return;
    }
    for (idx, agent) in agents.iter().enumerate() {
        let tag = if agent.available {
            "available"
        } else {
            "history-only"
        };
        body.push_str(&format!(
            "/a{}  {} — {} · {}\n",
            idx + 1,
            agent.display_name,
            agent.provider_id,
            tag
        ));
    }
}

fn render_workspaces_body(
    body: &mut String,
    workspaces: &[HistoryWorkspace],
    total: usize,
    page_end: usize,
    offset: usize,
) {
    body.push_str(&format!(
        "workspaces ({lo}-{hi} of {total})\n",
        lo = if total == 0 { 0 } else { offset + 1 },
        hi = page_end,
    ));
    if workspaces.is_empty() {
        body.push_str("  (no local workspaces found)\n");
        return;
    }
    for (idx, workspace) in workspaces.iter().enumerate() {
        body.push_str(&format!(
            "/w{}  {} · {}\n     🧾 {} · 🤖 {}\n     📁 {}\n",
            idx + 1,
            workspace.display_name,
            relative_time(workspace.last_active_unix, &workspace.last_active_display),
            workspace_session_count_label(workspace.session_count),
            workspace.provider_ids.join(", "),
            compact_path(&workspace.cwd.display().to_string(), 58),
        ));
        if idx + 1 < workspaces.len() {
            body.push('\n');
        }
    }
}

fn workspace_session_count_label(session_count: usize) -> String {
    if session_count == 1 {
        "1 session".into()
    } else {
        format!("{session_count} sessions")
    }
}

fn render_sessions_body(
    body: &mut String,
    history: &[HistoryEntry],
    total: usize,
    page_end: usize,
    offset: usize,
    runtime_provider_ids: &[&'static str],
) {
    body.push_str(&format!(
        "sessions ({lo}-{hi} of {total})\n",
        lo = if total == 0 { 0 } else { offset + 1 },
        hi = page_end,
    ));
    if history.is_empty() {
        body.push_str("  (no local agent sessions found)\n");
        return;
    }
    let mut command_idx = 1usize;
    for (idx, entry) in history.iter().enumerate() {
        let command = if runtime_provider_ids.contains(&entry.provider_id) {
            let command = format!("/h{command_idx}");
            command_idx += 1;
            command
        } else {
            "•".into()
        };
        body.push_str(&format!(
            "{}  {} · {}\n     💬 {}\n     📁 {}\n      `{}`\n",
            command,
            entry.provider_id,
            relative_time(entry.last_active_unix, &entry.last_active_display),
            short_line(&entry.summary, 60),
            compact_path(entry.cwd.as_deref().unwrap_or("(no cwd)"), 48),
            entry.session_id,
        ));
        if idx + 1 < history.len() {
            body.push('\n');
        }
    }
}

fn subagent_workspace_title(parent: &WorkSession, link: &SubAgentLinkRecord) -> String {
    let label = link
        .label
        .as_deref()
        .or(link.nickname.as_deref())
        .or(link.role.as_deref())
        .or(link.prompt.as_deref())
        .unwrap_or("subagent");
    format!("{} · {}", parent.title, short(label, 32))
}

fn command_run_options(
    provider_id: &'static str,
    completion_policy: CommandCompletionPolicy,
    state: Arc<BotState>,
) -> turn::CommandRunOptions {
    turn::CommandRunOptions {
        provider_id: Some(provider_id),
        status_snapshot: None,
        status_resource: None,
        completion_policy,
        intervention_callback_registry: Some(state.clone()),
        status_recorder: Some(state),
        recording: None,
    }
}

fn plan_topic_command_invocation(
    catalog: &AgentCommandCatalog,
    cmd: &TopicSlashCommand,
) -> Result<CommandInvocationPlan, CommandPlanError> {
    match plan_command_invocation(catalog, &cmd.name, &cmd.args, cmd.values.clone()) {
        Ok(plan) => Ok(plan),
        Err(CommandPlanError::Unsupported { name }) => {
            session_trait_command_plan(&name, &cmd.args, cmd.values.clone(), catalog.revision)
                .ok_or(CommandPlanError::Unsupported { name })
        }
        Err(err) => Err(err),
    }
}

fn command_descriptor_for_help(catalog: &AgentCommandCatalog, name: &str) -> Option<AgentCommand> {
    if name == "commands" {
        return topic_command_descriptor(name);
    }
    command_from_catalog(catalog, name)
        .cloned()
        .or_else(|| topic_command_descriptor(name))
}

fn render_command_help(command: &AgentCommand) -> String {
    let mut lines = vec![format!("`/{}`", command.name)];
    if let Some(description) = command.description.as_deref() {
        lines.push(description.to_string());
    }
    lines.push(format!("usage: `{}`", command_usage(command)));
    if !command.aliases.is_empty() {
        let aliases = command
            .aliases
            .iter()
            .map(|alias| format!("`/{}`", alias.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("aliases: {aliases}"));
    }
    lines.join("\n")
}

fn topic_command_descriptor(name: &str) -> Option<AgentCommand> {
    match name {
        "commands" => Some(adapter_command(
            "commands",
            "List or invoke bound agent commands.",
            AgentCommandInput::Text {
                label: "command [arguments]".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "help" => Some(adapter_command(
            "help",
            "Show workspace command help.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        "config" => Some(adapter_command(
            "config",
            "Show or set scoped config.",
            AgentCommandInput::Text {
                label: "[global|workspace|session] <setting> <on|off>".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "interrupt" => Some(adapter_command(
            "interrupt",
            "Interrupt the current turn.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        "rename" => Some(adapter_command(
            "rename",
            "Rename the current workspace.",
            AgentCommandInput::Text {
                label: "name".into(),
                required: true,
            },
            AgentCommandCompletion::CommandResult,
        )),
        _ => session_trait_command_descriptor(name),
    }
}

fn entry_command_descriptor(name: &str) -> Option<AgentCommand> {
    match name {
        "panel" | "start" => {
            let aliases = ["start"].into_iter().map(Into::into).collect();
            Some(adapter_command_with_aliases(
                "panel",
                "Refresh the management panel.",
                aliases,
                AgentCommandInput::None,
                AgentCommandCompletion::CommandResult,
            ))
        }
        "help" => Some(adapter_command(
            "help",
            "Show entry-panel help.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        "config" => Some(adapter_command(
            "config",
            "Show or set scoped config.",
            AgentCommandInput::Text {
                label: "[global|workspace|session] <setting> <on|off>".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "reset_notifications" => Some(adapter_command(
            "reset_notifications",
            "Delete and recreate the shared agent notifications topic.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        name if entry_index_command_kind(name, "a").is_some() => Some(adapter_command(
            "aN",
            "Start a new session with agent N.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        name if entry_index_command_kind(name, "h").is_some() => Some(adapter_command(
            "hN",
            "Resume history entry N from the current page.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        name if entry_index_command_kind(name, "w").is_some() => Some(adapter_command(
            "wN",
            "Jump to open workspace N.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        _ => None,
    }
}

fn entry_index_command_kind<'a>(name: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = name.strip_prefix(prefix)?;
    (!rest.is_empty() && rest.chars().all(|ch| ch.is_ascii_digit())).then_some(rest)
}

fn session_trait_command_descriptor(name: &str) -> Option<AgentCommand> {
    match name {
        "model" => Some(adapter_command(
            "model",
            "Show or set the agent model.",
            AgentCommandInput::Text {
                label: "model [reasoning]".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "permissions" => Some(adapter_command(
            "permissions",
            "Show or set agent permissions.",
            AgentCommandInput::Text {
                label: "mode".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "skills" => Some(adapter_command(
            "skills",
            "List available agent skills.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        "status" => Some(adapter_command(
            "status",
            "Show current agent status.",
            AgentCommandInput::None,
            AgentCommandCompletion::CommandResult,
        )),
        "fork" => Some(adapter_command(
            "fork",
            "List fork targets or fork the current conversation.",
            AgentCommandInput::Text {
                label: "target".into(),
                required: false,
            },
            AgentCommandCompletion::CommandResult,
        )),
        "new" => Some(adapter_command(
            "new",
            "Start a new agent conversation.",
            AgentCommandInput::None,
            AgentCommandCompletion::TurnCompleted,
        )),
        "quit" => Some(adapter_command(
            "quit",
            "Close the current live session.",
            AgentCommandInput::None,
            AgentCommandCompletion::TurnCompleted,
        )),
        _ => None,
    }
}

fn adapter_command(
    name: &'static str,
    description: &'static str,
    input: AgentCommandInput,
    completion: AgentCommandCompletion,
) -> AgentCommand {
    adapter_command_with_aliases(name, description, Vec::new(), input, completion)
}

fn adapter_command_with_aliases(
    name: &'static str,
    description: &'static str,
    aliases: Vec<smol_str::SmolStr>,
    input: AgentCommandInput,
    completion: AgentCommandCompletion,
) -> AgentCommand {
    AgentCommand {
        name: name.into(),
        description: Some(description.into()),
        aliases,
        source: AgentCommandSource::AdapterMapped,
        input,
        completion,
    }
}

fn session_trait_command_plan(
    name: &str,
    args: &str,
    values: serde_json::Value,
    catalog_revision: u64,
) -> Option<CommandInvocationPlan> {
    let completion_policy = match name {
        "model" | "permissions" | "skills" | "status" | "fork" => {
            CommandCompletionPolicy::CommandResult
        }
        "new" | "quit" => CommandCompletionPolicy::TurnCompleted,
        _ => return None,
    };
    let args = args.trim();
    Some(CommandInvocationPlan {
        name: name.into(),
        args: (!args.is_empty()).then(|| args.to_string().into()),
        values,
        source: AgentCommandSource::AdapterMapped,
        catalog_revision: Revision::new(catalog_revision),
        completion_policy,
    })
}

fn is_unsupported_typed_command(err: &AgentError) -> bool {
    err.kind == AgentErrorKind::Unsupported
}

fn model_selection_from_plan(
    plan: &CommandInvocationPlan,
) -> Result<Option<AgentModelSelection>, String> {
    if plan.name.as_str() != "model" {
        return Ok(None);
    }
    let value_model = string_value(&plan.values, "model");
    let value_reasoning =
        string_value(&plan.values, "reasoning").or_else(|| string_value(&plan.values, "effort"));
    if let Some(model) = value_model {
        return Ok(Some(AgentModelSelection {
            model: model.into(),
            reasoning: value_reasoning.map(Into::into),
        }));
    }

    let raw = plan.args.as_deref().unwrap_or("").trim();
    if raw.is_empty() {
        return Ok(None);
    }
    let mut parts = raw.split_whitespace();
    let model = parts.next().unwrap_or_default();
    let reasoning = parts.next();
    if parts.next().is_some() {
        return Err("model expects <model> [reasoning]".into());
    }
    Ok(Some(AgentModelSelection {
        model: model.into(),
        reasoning: reasoning.map(Into::into),
    }))
}

fn permission_selection_from_plan(
    plan: &CommandInvocationPlan,
) -> Option<AgentPermissionSelection> {
    if plan.name.as_str() != "permissions" {
        return None;
    }
    let mode = string_value(&plan.values, "mode")
        .or_else(|| string_value(&plan.values, "permissionMode"))
        .or_else(|| string_value(&plan.values, "approval_policy"))
        .or_else(|| {
            plan.args
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })?;
    Some(AgentPermissionSelection { mode: mode.into() })
}

fn string_value(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn command_report_fork_result(value: &serde_json::Value) -> Option<AgentForkResult> {
    serde_json::from_value::<EventCommandResultPayload>(value.clone())
        .ok()
        .and_then(|payload| match payload.result {
            EventCommandResultData::Forked(result) => Some(result),
            _ => None,
        })
        .or_else(|| {
            serde_json::from_value::<EventCommandResultData>(value.clone())
                .ok()
                .and_then(|data| match data {
                    EventCommandResultData::Forked(result) => Some(result),
                    _ => None,
                })
        })
        .or_else(|| {
            serde_json::from_value::<AgentCommandResult>(value.clone())
                .ok()
                .and_then(|result| match result.data {
                    AgentCommandResultData::Fork(result) => Some(result),
                    _ => None,
                })
        })
}

#[cfg(test)]
fn command_supports_name(command: &AgentCommand, name: &str) -> bool {
    command.name.as_str() == name || command.aliases.iter().any(|alias| alias.as_str() == name)
}

fn command_list_description(command: &AgentCommand) -> Option<String> {
    let mut description = command.description.as_deref().map(str::to_string);
    if command.aliases.is_empty() {
        return description;
    }
    let aliases = command
        .aliases
        .iter()
        .map(|alias| format!("/{}", alias.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    match &mut description {
        Some(description) => {
            description.push_str(" · aliases: ");
            description.push_str(&aliases);
        }
        None => description = Some(format!("aliases: {aliases}")),
    }
    description
}

#[cfg(test)]
fn agent_command_button_data_for_workspace(
    state: &BotState,
    workspace: &WorkspaceId,
    catalog_revision: u64,
    name: &str,
    args: &str,
) -> String {
    state.register_command_callback(workspace, catalog_revision, name, args)
}

fn parse_topic_slash_command(raw: &str) -> Option<TopicSlashCommand> {
    let trimmed = raw.trim().trim_start_matches('/').trim();
    if trimmed.is_empty() {
        return None;
    }
    let (name, args) = match trimmed.split_once(char::is_whitespace) {
        Some((name, args)) => (name, args.trim()),
        None => (trimmed, ""),
    };
    let name = name
        .split('@')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    if name.is_empty() || !is_valid_agent_command_name(&name) {
        return None;
    }
    let name = match name.as_str() {
        "models" => "model".to_string(),
        _ => name,
    };
    Some(TopicSlashCommand {
        name,
        args: args.to_string(),
        values: serde_json::Value::Null,
    })
}

fn parse_kill_target(args: &str) -> Option<KillAgentTarget> {
    let target = args.trim();
    if target.eq_ignore_ascii_case("all") {
        return Some(KillAgentTarget::All);
    }
    if target.is_empty() {
        return None;
    }
    target
        .rsplit_once(':')
        .and_then(|(_, pid)| pid.parse::<i32>().ok())
        .filter(|pid| *pid > 0)
        .map(|_| KillAgentTarget::Identity(target.to_string()))
}

fn parse_fork_target_shortcut(name: &str, args: &str) -> Option<usize> {
    if !args.trim().is_empty() {
        return None;
    }
    name.strip_prefix('f').and_then(parse_idx)
}

fn parse_entry_config_command(text: &str) -> Option<String> {
    let command = parse_topic_slash_command(text)?;
    (command.name == "config").then_some(command.args)
}

fn is_valid_agent_command_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn command_query_results_from_catalog(
    query: &str,
    catalog: &AgentCommandCatalog,
) -> Vec<CommandQueryResult> {
    let normalized = query.trim().trim_start_matches('/').to_ascii_lowercase();
    let (needle, arg_prefix) = match normalized.split_once(char::is_whitespace) {
        Some((name, args)) => (name, Some(args.trim())),
        None => (normalized.as_str(), None),
    };
    let mut results = Vec::new();
    for command in &catalog.commands {
        for name in std::iter::once(command.name.as_str())
            .chain(command.aliases.iter().map(|alias| alias.as_str()))
        {
            if !needle.is_empty() && !name.starts_with(needle) && name != needle {
                continue;
            }
            if name == needle {
                let option_results =
                    command_option_results(command, name, arg_prefix.unwrap_or(""));
                if !option_results.is_empty() {
                    results.extend(option_results);
                    continue;
                }
            }
            let message_text = if name == needle {
                match arg_prefix.filter(|arg| !arg.is_empty()) {
                    Some(arg) => format!("/{name} {arg}"),
                    None => format!("/{name}"),
                }
            } else {
                format!("/{name}")
            };
            results.push(CommandQueryResult {
                id: format!("cmd:{name}"),
                title: format!("/{name}"),
                description: command_list_description(command),
                message_text,
            });
            if results.len() >= 50 {
                break;
            }
        }
        if results.len() >= 50 {
            break;
        }
    }
    results
}

fn command_option_results(
    command: &AgentCommand,
    name: &str,
    arg_prefix: &str,
) -> Vec<CommandQueryResult> {
    if name == "model" {
        return model_option_results(command, arg_prefix);
    }
    schema_enum_values(command, None)
        .into_iter()
        .filter(|value| arg_prefix.is_empty() || value.starts_with(arg_prefix))
        .map(|value| CommandQueryResult {
            id: format!("cmd:{name}:{value}"),
            title: format!("/{name} {value}"),
            description: command.description.as_deref().map(str::to_string),
            message_text: format!("/{name} {value}"),
        })
        .take(50)
        .collect()
}

fn model_option_results(command: &AgentCommand, arg_prefix: &str) -> Vec<CommandQueryResult> {
    let models = schema_enum_values(command, Some("model"));
    let mut efforts = schema_enum_values(command, Some("reasoning"));
    if efforts.is_empty() {
        efforts = schema_enum_values(command, Some("effort"));
    }
    if models.is_empty() {
        return Vec::new();
    }

    let raw = arg_prefix;
    let trimmed = raw.trim();
    let mut results = Vec::new();
    if trimmed.is_empty() {
        results.push(CommandQueryResult {
            id: "cmd:model:list".into(),
            title: "/model".into(),
            description: Some("List supported models".into()),
            message_text: "/model".into(),
        });
        for model in models {
            results.push(model_result(&model, None));
            if results.len() >= 50 {
                break;
            }
        }
        return results;
    }

    let parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.len() == 1 && !raw.ends_with(char::is_whitespace) {
        return models
            .into_iter()
            .filter(|model| model_matches(model, parts[0]))
            .map(|model| model_result(&model, None))
            .take(50)
            .collect();
    }

    let Some(model) = models
        .iter()
        .find(|model| model_matches(model, parts.first().copied().unwrap_or_default()))
        .cloned()
    else {
        return Vec::new();
    };
    let effort_prefix = parts.get(1).copied().unwrap_or("");
    efforts
        .into_iter()
        .filter(|effort| effort_prefix.is_empty() || effort.starts_with(effort_prefix))
        .map(|effort| model_result(&model, Some(&effort)))
        .take(50)
        .collect()
}

fn model_result(model: &str, effort: Option<&str>) -> CommandQueryResult {
    let message_text = match effort {
        Some(effort) => format!("/model {model} {effort}"),
        None => format!("/model {model}"),
    };
    CommandQueryResult {
        id: match effort {
            Some(effort) => format!("cmd:model:{model}:{effort}"),
            None => format!("cmd:model:{model}"),
        },
        title: message_text.clone(),
        description: Some(match effort {
            Some(_) => "Switch model and reasoning effort".into(),
            None => "Switch model".into(),
        }),
        message_text,
    }
}

fn model_matches(model: &str, prefix: &str) -> bool {
    let prefix = prefix.to_ascii_lowercase();
    let model = model.to_ascii_lowercase();
    model.starts_with(&prefix) || model.replace('-', "").starts_with(&prefix)
}

fn schema_enum_values(command: &AgentCommand, property: Option<&str>) -> Vec<String> {
    let AgentCommandInput::JsonSchema { schema } = &command.input else {
        return Vec::new();
    };
    schema
        .get("properties")
        .and_then(|value| value.as_object())
        .into_iter()
        .flat_map(|properties| properties.iter())
        .filter_map(|(name, property_schema)| {
            if property.map_or(true, |expected| expected == name) {
                property_schema
                    .get("enum")
                    .and_then(|value| value.as_array())
            } else {
                None
            }
        })
        .flat_map(|values| values.iter())
        .filter_map(|value| value.as_str())
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
fn enum_command_input(property: &str, values: &[&str]) -> AgentCommandInput {
    AgentCommandInput::JsonSchema {
        schema: serde_json::json!({
            "type": "object",
            "properties": {
                property: {
                    "type": "string",
                    "enum": values,
                }
            }
        }),
    }
}

/// Parsed result of a `/…` command typed in the entry chat.
///
/// Keeping this as a data type (rather than dispatching from the
/// parser directly) makes unit-testing the parser trivial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryAction {
    Panel,
    Next,
    Prev,
    Agent(usize),
    History(usize),
    Workspace(usize),
    ClearWorkspaces,
    ResetNotifications,
    Help,
}

pub fn parse_entry_command(text: &str) -> Option<EntryAction> {
    let t = text.trim();
    if t.split_whitespace().nth(1).is_some() {
        return None;
    }
    let command = t.split('@').next().unwrap_or(t);
    match command {
        "/panel" | "/start" | "/refresh" => Some(EntryAction::Panel),
        "/next" => Some(EntryAction::Next),
        "/prev" => Some(EntryAction::Prev),
        "/clear_workspaces" => Some(EntryAction::ClearWorkspaces),
        "/reset_notifications" => Some(EntryAction::ResetNotifications),
        "/help" => Some(EntryAction::Help),
        _ => {
            if let Some(n) = command.strip_prefix("/a").and_then(parse_idx) {
                return Some(EntryAction::Agent(n));
            }
            if let Some(n) = command.strip_prefix("/h").and_then(parse_idx) {
                return Some(EntryAction::History(n));
            }
            if let Some(n) = command.strip_prefix("/w").and_then(parse_idx) {
                return Some(EntryAction::Workspace(n));
            }
            None
        }
    }
}

fn parse_entry_command_help(text: &str) -> Option<AgentCommand> {
    let trimmed = text.trim().trim_start_matches('/').trim();
    let (name, args) = trimmed.split_once(char::is_whitespace)?;
    if !args.trim().eq_ignore_ascii_case("help") {
        return None;
    }
    let name = name
        .split('@')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    entry_command_descriptor(&name)
}

fn parse_idx(rest: &str) -> Option<usize> {
    // Accept an optional `@botname` suffix ("/h1@lucarnebot"); strip it.
    let rest = rest.split('@').next()?;
    // Reject empty and non-numeric tails.
    if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let n: usize = rest.parse().ok()?;
    if n == 0 {
        None
    } else {
        Some(n)
    }
}

fn image_media_type(att: &IncomingAttachment) -> Option<String> {
    if let Some(mime) = att.mime_type.as_deref() {
        let lower = mime.to_ascii_lowercase();
        if lower.starts_with("image/") {
            return Some(lower);
        }
    }

    let filename = att.filename.as_deref()?.to_ascii_lowercase();
    let media_type = if filename.ends_with(".jpg") || filename.ends_with(".jpeg") {
        "image/jpeg"
    } else if filename.ends_with(".png") {
        "image/png"
    } else if filename.ends_with(".webp") {
        "image/webp"
    } else if filename.ends_with(".gif") {
        "image/gif"
    } else {
        return None;
    };
    Some(media_type.to_string())
}

async fn live_provider_resume_ref(live: &LiveSession) -> Option<String> {
    live.session
        .provider_session_id()
        .await
        .map(|session_id| session_id.0.to_string())
}

fn is_recoverable_live_session_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("broken pipe")
        || lower.contains("os error 32")
        || lower.contains("stdin closed")
        || lower.contains("event stream closed")
        || lower.contains("session is closing")
        || lower.contains("idle timeout")
}

fn is_idle_recycled_session(close_reason: Option<&str>, error: &str) -> bool {
    close_reason
        .map(|reason| reason.to_ascii_lowercase().contains("idle timeout"))
        .unwrap_or(false)
        || error.to_ascii_lowercase().contains("idle timeout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::{
        stream::{self, BoxStream},
        StreamExt,
    };
    use lucarne::agent_runtime::{
        events::{CommandResultEvent, ToolCallEvent, TurnCompletedEvent},
        AgentCapabilities, AgentCommand, AgentCommandCatalog, AgentCommandCompletion,
        AgentCommandInput, AgentCommandInvocation, AgentCommandResult,
        AgentCommandResultData as AgentRuntimeCommandResultData, AgentCommandSource,
        AgentContextUsage, AgentError, AgentErrorKind, AgentEventStream, AgentForkResult,
        AgentForkTarget, AgentForkTargetCatalog, AgentImageInput, AgentInput, AgentModelCatalog,
        AgentModelOption, AgentModelSelection, AgentPermissionCatalog, AgentPermissionOption,
        AgentPermissionSelection, AgentProvider, AgentReasoningOption, AgentRuntime, AgentSession,
        AgentSkillCatalog, AgentSkillSummary, AgentStatus, AgentTokenUsage, ApprovalRequest,
        CallId, Event, InstanceId, InterventionRequest, MessageEvent, MessageRole, OpenSession,
        ProbeResult, ProtocolProvider, ResumeSession, SessionId,
    };
    use lucarne::control_plane::{
        ChannelBinding, ChannelBindingId, CommandState, ControlPlaneSqliteStore, SubAgentState,
        WorkspaceId as ControlWorkspaceId,
    };
    use lucarne::core_service::CoreWorkspaceEventStream;
    use lucarne::event::{CommandResultData, CommandResultPayload};
    use lucarne::testing::live::{
        ensure_live_git_repo, prepare_recorded_provider,
        recorded_provider_or_return as replay_provider_or_return, LiveProvider,
        PreparedRecordingRun, RecordedLiveCase,
    };
    use lucarne_channel::{
        ChannelEvent, CommandQuery, CommandQueryResult, FileUpload, IncomingAttachment, MessageId,
        Result,
    };
    use serde_json::json;
    use smol_str::SmolStr;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::{
        collections::{HashMap, VecDeque},
        ffi::OsString,
        fs,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering},
            Arc, Mutex as StdMutex,
        },
        time::Duration,
    };
    use tempfile::TempDir;
    use tokio::sync::{broadcast, mpsc, Mutex as TokioMutex, Notify};
    use tokio::time::{sleep, timeout};

    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    #[test]
    fn memory_profile_snapshots_mark_bot_run_startup_phases() {
        let source = include_str!("bot.rs");

        for label in [
            "lucarne_telegram.bot.run.start",
            "lucarne_telegram.bot.run.after_watch_events_subscribe",
            "lucarne_telegram.bot.run.after_core_watcher_spawn",
            "lucarne_telegram.bot.run.after_channel_subscribe",
        ] {
            assert!(source.contains(label), "missing snapshot {label}");
        }
    }

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn core_with_runtime(runtime: Arc<AgentRuntime>) -> Arc<LucarneCore> {
        LucarneCore::from_runtime_and_store(
            runtime,
            ControlPlaneSqliteStore::open_in_memory().expect("open in-memory control-plane store"),
        )
        .expect("build test core")
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

    impl RecordingChannel {
        fn split_next_send_as(&self, ids: &[&str]) {
            self.split_send_ids
                .lock()
                .unwrap()
                .push_back(ids.iter().map(|id| id.to_string()).collect());
        }
    }

    #[derive(Default)]
    struct RecordingChannel {
        sent: StdMutex<Vec<OutgoingMessage>>,
        sent_records: StdMutex<Vec<(String, OutgoingMessage)>>,
        sent_targets: StdMutex<Vec<String>>,
        edits: StdMutex<Vec<(String, OutgoingMessage)>>,
        deleted_messages: StdMutex<Vec<String>>,
        sent_files: StdMutex<Vec<(String, FileUpload)>>,
        split_send_ids: StdMutex<VecDeque<Vec<String>>>,
        created_workspaces: StdMutex<Vec<(String, String, String)>>,
        deleted_workspaces: StdMutex<Vec<String>>,
        next_workspace_id: AtomicUsize,
        next_message_id: AtomicUsize,
        send_results: StdMutex<VecDeque<std::result::Result<(), ChannelError>>>,
        missing_workspaces: StdMutex<Vec<String>>,
        probed_workspaces: StdMutex<Vec<String>>,
        renames: StdMutex<Vec<(String, String)>>,
        downloads: StdMutex<HashMap<String, Vec<u8>>>,
        acks: StdMutex<Vec<WorkspaceHandle>>,
        command_query_answers: StdMutex<Vec<(String, Vec<CommandQueryResult>)>>,
    }

    #[async_trait]
    impl Channel for RecordingChannel {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn message_char_limit(&self) -> usize {
            4096
        }

        async fn send(&self, target: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId> {
            self.send_all(target, msg)
                .await?
                .into_iter()
                .last()
                .ok_or_else(|| ChannelError::Transport("send returned no message ids".into()))
        }

        async fn send_all(
            &self,
            target: &WorkspaceHandle,
            msg: OutgoingMessage,
        ) -> Result<Vec<MessageId>> {
            if self
                .missing_workspaces
                .lock()
                .unwrap()
                .iter()
                .any(|ws| ws == target.workspace.as_str())
            {
                return Err(ChannelError::WorkspaceNotFound("TOPIC_ID_INVALID".into()));
            }
            if let Some(result) = self.send_results.lock().unwrap().pop_front() {
                result?;
            }
            self.sent_targets
                .lock()
                .unwrap()
                .push(target.workspace.as_str().to_string());
            let ids = self
                .split_send_ids
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| {
                    vec![format!(
                        "sent-{}",
                        self.next_message_id.fetch_add(1, AtomicOrdering::SeqCst) + 1
                    )]
                });
            for id in &ids {
                self.sent.lock().unwrap().push(msg.clone());
                self.sent_records
                    .lock()
                    .unwrap()
                    .push((id.clone(), msg.clone()));
            }
            Ok(ids.into_iter().map(MessageId::new).collect())
        }

        async fn edit(
            &self,
            _target: &WorkspaceHandle,
            id: &MessageId,
            msg: OutgoingMessage,
        ) -> Result<()> {
            self.edits
                .lock()
                .unwrap()
                .push((id.as_str().to_string(), msg));
            Ok(())
        }

        async fn delete(&self, _target: &WorkspaceHandle, id: &MessageId) -> Result<()> {
            self.deleted_messages
                .lock()
                .unwrap()
                .push(id.as_str().to_string());
            Ok(())
        }

        async fn create_workspace(&self, parent: &ChatId, title: &str) -> Result<WorkspaceHandle> {
            let id = self.next_workspace_id.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            let workspace = WorkspaceId::new(id.to_string());
            self.created_workspaces.lock().unwrap().push((
                parent.as_str().to_string(),
                title.to_string(),
                workspace.as_str().to_string(),
            ));
            Ok(WorkspaceHandle::new(parent.clone(), workspace))
        }

        async fn probe_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
            self.probed_workspaces
                .lock()
                .unwrap()
                .push(handle.workspace.as_str().to_string());
            if self
                .missing_workspaces
                .lock()
                .unwrap()
                .iter()
                .any(|ws| ws == handle.workspace.as_str())
            {
                return Err(ChannelError::WorkspaceNotFound(
                    "message thread not found".into(),
                ));
            }
            Ok(())
        }

        async fn rename_workspace(&self, handle: &WorkspaceHandle, title: &str) -> Result<()> {
            self.renames
                .lock()
                .unwrap()
                .push((handle.workspace.as_str().to_string(), title.to_string()));
            if self
                .missing_workspaces
                .lock()
                .unwrap()
                .iter()
                .any(|ws| ws == handle.workspace.as_str())
            {
                return Err(ChannelError::WorkspaceNotFound("TOPIC_ID_INVALID".into()));
            }
            Ok(())
        }

        async fn delete_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
            self.deleted_workspaces
                .lock()
                .unwrap()
                .push(handle.workspace.as_str().to_string());
            if self
                .missing_workspaces
                .lock()
                .unwrap()
                .iter()
                .any(|ws| ws == handle.workspace.as_str())
            {
                return Err(ChannelError::WorkspaceNotFound("TOPIC_ID_INVALID".into()));
            }
            Ok(())
        }

        fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
            stream::empty().boxed()
        }

        async fn download_attachment(&self, att: &IncomingAttachment) -> Result<Vec<u8>> {
            Ok(self
                .downloads
                .lock()
                .unwrap()
                .get(&att.file_ref)
                .cloned()
                .unwrap_or_default())
        }

        async fn send_file(&self, target: &WorkspaceHandle, file: FileUpload) -> Result<MessageId> {
            let id = format!(
                "sent-{}",
                self.next_message_id.fetch_add(1, AtomicOrdering::SeqCst) + 1
            );
            self.sent_files
                .lock()
                .unwrap()
                .push((target.workspace.as_str().to_string(), file));
            Ok(MessageId::new(id))
        }

        async fn acknowledge(&self, target: &WorkspaceHandle) -> Result<()> {
            self.acks.lock().unwrap().push(target.clone());
            if self
                .missing_workspaces
                .lock()
                .unwrap()
                .iter()
                .any(|ws| ws == target.workspace.as_str())
            {
                return Err(ChannelError::WorkspaceNotFound(
                    "message thread not found".into(),
                ));
            }
            Ok(())
        }

        async fn answer_command_query(
            &self,
            query: &CommandQuery,
            results: Vec<CommandQueryResult>,
        ) -> Result<()> {
            self.command_query_answers
                .lock()
                .unwrap()
                .push((query.id.clone(), results));
            Ok(())
        }
    }

    fn test_bot(channel: Arc<RecordingChannel>) -> Arc<Bot> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(Arc::new(StdMutex::new(
            Vec::new(),
        )))));
        let core = core_with_runtime(runtime);
        Arc::new(Bot::new(
            channel,
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ))
    }

    fn test_bot_with_runtime(
        channel: Arc<RecordingChannel>,
        runtime: Arc<AgentRuntime>,
    ) -> Arc<Bot> {
        let core = core_with_runtime(runtime);
        Arc::new(Bot::new(
            channel,
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ))
    }

    fn test_bot_with_state(channel: Arc<RecordingChannel>, state: Arc<BotState>) -> Arc<Bot> {
        let core = state.core_handle();
        Arc::new(Bot::new_with_state(
            channel,
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state,
        ))
    }

    #[tokio::test]
    async fn public_bot_constructor_uses_runtime_provider_catalog_for_state() {
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_provider_id(
            "copilot",
            Arc::new(StdMutex::new(Vec::new())),
            Arc::new(StdMutex::new(Vec::new())),
            test_command_catalog(),
        )));
        let core = core_with_runtime(runtime);
        let bot = Bot::new(
            channel,
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        );

        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("copilot:session-1"),
                chat: ChatId::new("100"),
                provider_id: "copilot",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "copilot".into(),
                live: None,
                resume_ref: Some("session-1".into()),
            })
            .expect("state should accept providers registered in runtime");
    }

    #[tokio::test]
    async fn bot_run_sends_startup_help_and_waits_for_explicit_panel_request() {
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(Arc::new(StdMutex::new(
            Vec::new(),
        )))));
        let core = core_with_runtime(runtime);
        let state = BotState::new_with_core(Arc::clone(&core));
        let bot = Arc::new(Bot::new_with_state_and_history_watch(
            channel.clone(),
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state,
            false,
        ));

        Arc::clone(&bot).run().await;

        let sent = channel.sent.lock().unwrap().clone();
        assert_eq!(
            sent.len(),
            1,
            "Telegram startup should send one help message"
        );
        assert!(sent[0].body.contains("commands"));
        assert!(sent[0].body.contains("/help            — show this help"));
        assert!(
            channel.edits.lock().unwrap().is_empty(),
            "Telegram startup must not edit an entry panel"
        );

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("panel-request"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "u".into(),
            text: Some("/panel".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        assert!(
            channel
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|message| message.body.contains("🛠 lucarne")),
            "explicit /panel should still render the entry panel"
        );
    }

    #[tokio::test]
    async fn bot_run_ignores_startup_help_send_failure() {
        let channel = Arc::new(RecordingChannel::default());
        channel
            .send_results
            .lock()
            .unwrap()
            .push_back(Err(ChannelError::Transport("startup send failed".into())));
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).run().await;

        assert!(channel.send_results.lock().unwrap().is_empty());
        assert!(channel.sent.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn system_update_notification_sends_markdown_to_entry_chat() {
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        bot.handle_system_notification(lucarne_adapter::SystemNotification::UpdateAvailable(
            lucarne_adapter::SystemUpdateNotification {
                current_version: "0.1.0".into(),
                latest_version: "0.2.0".into(),
                release_name: "Lucarne 0.2.0".into(),
                release_url: "https://github.com/tuchg/Lucarne/releases/tag/v0.2.0".into(),
                published_at: Some("2026-05-21T00:00:00Z".into()),
                body_markdown: "## Lucarne update available\n\nInstall with `lucarned update`."
                    .into(),
                install_hint: "installer command".into(),
            },
        ))
        .await
        .expect("send system update notification");

        let sent = channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].format, lucarne_channel::TextFormat::Markdown);
        assert!(sent[0].body.contains("Lucarne update available"));
        assert!(sent[0].reply_to.is_none());
        assert_eq!(
            sent[0].notification,
            lucarne_channel::NotificationPolicy::Notify
        );
        drop(sent);

        assert_eq!(
            channel.sent_targets.lock().unwrap().clone(),
            vec![String::new()]
        );
        assert!(channel.created_workspaces.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn topic_message_without_binding_does_not_fallback_to_matching_workspace_id() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(Arc::clone(&inputs))));
        let core = core_with_runtime(runtime);
        let bot = Arc::new(Bot::new(
            Arc::clone(&channel) as Arc<dyn Channel>,
            Arc::clone(&core),
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ));
        core.upsert_workspace_binding(
            ControlWorkspaceId::new("2"),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex unbound".into(),
            },
            Some("thread-1"),
        )
        .expect("persist unbound workspace");
        bot.state
            .hydrate_unbound_control_workspaces(&ChatId::new("100"))
            .expect("hydrate unbound workspace");

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("m1"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "alice".into(),
            text: Some("should not submit".into()),
            attachments: Vec::new(),
        }))
        .await
        .expect("handle unbound topic message");

        assert!(
            inputs.lock().unwrap().is_empty(),
            "an unbound topic must not fall back to the same textual workspace id"
        );
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("isn't bound to an agent session")),
            "unbound topic should get an explicit binding error: {sent:?}"
        );
    }

    struct LiveBotE2eGuard;

    fn live_bot_e2e_provider(provider: &str) -> Option<LiveBotE2eGuard> {
        let _ = dotenvy::dotenv();
        if std::env::var("LUCARNE_TELEGRAM_LIVE_E2E").ok().as_deref() != Some("1") {
            return None;
        }
        if let Ok(allowed) = std::env::var("LUCARNE_LIVE_PROVIDERS") {
            let allowed = allowed
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .collect::<Vec<_>>();
            if !allowed.is_empty() && !allowed.iter().any(|name| *name == provider) {
                return None;
            }
        }
        Some(LiveBotE2eGuard)
    }

    fn live_bot_e2e_timeout() -> Duration {
        std::env::var("LUCARNE_TELEGRAM_LIVE_TIMEOUT")
            .or_else(|_| std::env::var("LUCARNE_LIVE_TIMEOUT"))
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(180))
    }

    async fn live_send_text(bot: &Arc<Bot>, ws: &WorkspaceId, text: &str, message_id: &str) {
        timeout(
            live_bot_e2e_timeout(),
            bot.clone().handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new(message_id),
                chat: ChatId::new("100"),
                workspace: Some(ws.clone()),
                reply_to: None,
                user: "live-e2e".into(),
                text: Some(text.into()),
                attachments: Vec::new(),
            })),
        )
        .await
        .unwrap_or_else(|_| panic!("live bot e2e message timed out: {text}"))
        .unwrap_or_else(|err| panic!("live bot e2e message failed for {text}: {err}"));
    }

    async fn live_send_entry_text(bot: &Arc<Bot>, text: &str, message_id: &str) {
        timeout(
            live_bot_e2e_timeout(),
            bot.clone().handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new(message_id),
                chat: ChatId::new("100"),
                workspace: None,
                reply_to: None,
                user: "live-e2e".into(),
                text: Some(text.into()),
                attachments: Vec::new(),
            })),
        )
        .await
        .unwrap_or_else(|_| panic!("live bot e2e entry message timed out: {text}"))
        .unwrap_or_else(|err| panic!("live bot e2e entry message failed for {text}: {err}"));
    }

    async fn live_click_button(bot: &Arc<Bot>, ws: &WorkspaceId, data: &str, source_message: &str) {
        timeout(
            live_bot_e2e_timeout(),
            bot.clone().handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(ws.clone()),
                user: "live-e2e".into(),
                data: data.into(),
                source_message: MessageId::new(source_message),
            }),
        )
        .await
        .unwrap_or_else(|_| panic!("live bot e2e button timed out for {source_message}: {data}"))
        .unwrap_or_else(|err| {
            panic!("live bot e2e button failed for {source_message} ({data}): {err}")
        });
    }

    fn live_find_button(channel: &RecordingChannel, prefix: &str) -> Option<String> {
        channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .rev()
            .flat_map(|message| message.buttons.iter().flatten())
            .find(|button| button.data.starts_with(prefix))
            .map(|button| button.data.clone())
    }

    fn live_command_callback_for_button(
        bot: &Bot,
        data: &str,
    ) -> crate::state::AgentCommandCallback {
        let token = data
            .strip_prefix("agentcmd:c:")
            .unwrap_or_else(|| panic!("expected state-backed agent command callback: {data}"));
        bot.state
            .resolve_command_callback(token)
            .unwrap_or_else(|| panic!("missing command callback for token {token}"))
    }

    fn live_command_button_by_value(
        bot: &Bot,
        msg: &OutgoingMessage,
        command: &str,
        key: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> (String, crate::state::AgentCommandCallback) {
        msg.buttons
            .iter()
            .flatten()
            .filter_map(|button| {
                let callback = live_command_callback_for_button(bot, &button.data);
                (callback.name == command
                    && callback
                        .values
                        .get(key)
                        .and_then(|value| value.as_str())
                        .is_some_and(&predicate))
                .then(|| (button.data.clone(), callback))
            })
            .next()
            .unwrap_or_else(|| {
                panic!(
                    "missing {command} button with {key} matching predicate; rows: {:?}",
                    msg.buttons
                )
            })
    }

    fn live_command_button_with_different_value(
        bot: &Bot,
        msg: &OutgoingMessage,
        command: &str,
        key: &str,
        current: Option<&str>,
        preferred: &[&str],
    ) -> (String, crate::state::AgentCommandCallback) {
        for wanted in preferred {
            if current == Some(*wanted) {
                continue;
            }
            if let Some(found) = msg.buttons.iter().flatten().find_map(|button| {
                let callback = live_command_callback_for_button(bot, &button.data);
                (callback.name == command
                    && callback.values.get(key).and_then(|value| value.as_str()) == Some(*wanted))
                .then(|| (button.data.clone(), callback))
            }) {
                return found;
            }
        }
        msg.buttons
            .iter()
            .flatten()
            .filter_map(|button| {
                let callback = live_command_callback_for_button(bot, &button.data);
                let value = callback.values.get(key).and_then(|value| value.as_str())?;
                (callback.name == command && Some(value) != current)
                    .then(|| (button.data.clone(), callback))
            })
            .next()
            .unwrap_or_else(|| {
                panic!(
                    "missing {command} button with {key} different from {current:?}; rows: {:?}",
                    msg.buttons
                )
            })
    }

    fn live_current_value_from_list(body: &str) -> Option<String> {
        body.lines()
            .find_map(|line| line.strip_prefix("current: `"))
            .and_then(|tail| tail.split('`').next())
            .map(str::to_string)
    }

    fn live_markdown_list_values(body: &str) -> Vec<String> {
        body.lines()
            .filter_map(|line| {
                let (_, tail) = line.split_once(". `")?;
                let value = tail.split('`').next()?.trim();
                (!value.is_empty()).then(|| value.to_string())
            })
            .collect()
    }

    fn live_latest_status_body(channel: &RecordingChannel) -> String {
        live_message_bodies(channel)
            .into_iter()
            .rev()
            .find(|body| body.starts_with("status\n"))
            .unwrap_or_else(|| "missing status message".into())
    }

    fn live_assert_status_contains(channel: &RecordingChannel, label: &str, fragments: &[&str]) {
        let status = live_latest_status_body(channel);
        for fragment in fragments {
            assert!(
                status
                    .to_ascii_lowercase()
                    .contains(&fragment.to_ascii_lowercase()),
                "{label} status missing {fragment:?}: {status:?}"
            );
        }
    }

    fn live_assert_no_failures(channel: &RecordingChannel) {
        let failures = channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|message| {
                message.body.starts_with('⚠')
                    || message.body.contains("open failed")
                    || message.body.contains("resume failed")
                    || message.body.contains("agent start failed")
            })
            .map(|message| message.body.clone())
            .collect::<Vec<_>>();
        assert!(failures.is_empty(), "live bot e2e failures: {failures:?}");
    }

    struct LiveBotFixture {
        _env_lock: Option<std::sync::MutexGuard<'static, ()>>,
        _env: Option<EnvGuard>,
        _temp: TempDir,
        _recording_temp: Option<TempDir>,
        project: PathBuf,
        channel: Arc<RecordingChannel>,
        state: Arc<BotState>,
        bot: Arc<Bot>,
        ws: WorkspaceId,
        recording: Option<PreparedRecordingRun>,
    }

    fn live_bot_fixture(provider_id: &'static str, title: &str) -> LiveBotFixture {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).expect("project dir");
        std::fs::write(project.join("README.md"), "lucarne telegram live bot e2e\n")
            .expect("readme");
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = core_with_runtime(runtime);
        let state = BotState::new_with_core(Arc::clone(&core));
        let bot = Arc::new(Bot::new_with_state(
            channel.clone(),
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state.clone(),
        ));
        let ws = WorkspaceId::new(format!("live-{provider_id}-core"));
        state
            .upsert(WorkSession {
                workspace: ws.clone(),
                chat: ChatId::new("100"),
                provider_id,
                project_path: Some(project.clone()),
                title: title.into(),
                live: None,
                resume_ref: None,
            })
            .expect("seed workspace");
        LiveBotFixture {
            _env_lock: None,
            _env: None,
            _temp: temp,
            _recording_temp: None,
            project,
            channel,
            state,
            bot,
            ws,
            recording: None,
        }
    }

    fn recorded_live_bot_fixture(
        provider: LiveProvider,
        title: &str,
        case: RecordedLiveCase,
        env_lock: std::sync::MutexGuard<'static, ()>,
    ) -> LiveBotFixture {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).expect("project dir");
        std::fs::write(
            project.join("README.md"),
            "lucarne telegram recorded bot e2e\n",
        )
        .expect("readme");
        ensure_live_git_repo(&project).expect("recorded live git repo");

        let env_vars = provider
            .recording_env(&project)
            .expect("recorded provider environment")
            .into_iter()
            .map(|(key, value)| (key, value.into_os_string()))
            .collect::<Vec<_>>();
        let env = (!env_vars.is_empty()).then(|| EnvGuard::set(&env_vars));

        let recording_temp = tempfile::TempDir::new().expect("recording temp dir");
        let recording = prepare_recorded_provider(recording_temp.path(), &provider, case, &project)
            .expect("prepare recorded provider")
            .expect("recorded provider");
        let active_provider = recording.provider.clone();
        let runtime = AgentRuntime::new();
        runtime.register(Arc::new(
            ProtocolProvider::new(active_provider.adapter()).expect("recorded provider"),
        ));
        let core = core_with_runtime(Arc::new(runtime));
        let channel = Arc::new(RecordingChannel::default());
        let state = BotState::new_with_core(Arc::clone(&core));
        let bot = Arc::new(Bot::new_with_state(
            channel.clone(),
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state.clone(),
        ));
        let ws = WorkspaceId::new(format!("recorded-{}-core", provider.name()));
        state
            .upsert(WorkSession {
                workspace: ws.clone(),
                chat: ChatId::new("100"),
                provider_id: provider.name(),
                project_path: Some(project.clone()),
                title: title.into(),
                live: None,
                resume_ref: None,
            })
            .expect("seed workspace");
        LiveBotFixture {
            _env_lock: Some(env_lock),
            _env: env,
            _temp: temp,
            _recording_temp: Some(recording_temp),
            project,
            channel,
            state,
            bot,
            ws,
            recording: Some(recording),
        }
    }

    impl LiveBotFixture {
        async fn prepare_recorded_history_effects(&mut self) {
            let Some(recording) = self.recording.as_ref() else {
                return;
            };
            if !recording.is_replay() {
                self.close_live().await;
            }
            let recording = self.recording.as_mut().expect("recording remains present");
            if recording.is_replay() {
                recording
                    .apply_recorded_effects(&self.project)
                    .expect("apply recorded provider effects");
            } else {
                recording
                    .finish(&self.project)
                    .expect("finish recorded provider effects");
            }
        }

        async fn close_live(&self) {
            let Some(live) = self.state.get(&self.ws).and_then(|session| session.live) else {
                return;
            };
            live.session
                .close()
                .await
                .expect("close recorded live session");
        }
    }

    #[cfg(unix)]
    struct LivePiRpcFixture {
        _temp: TempDir,
        _env: EnvGuard,
        channel: Arc<RecordingChannel>,
        state: Arc<BotState>,
        bot: Arc<Bot>,
        ws: WorkspaceId,
    }

    #[cfg(unix)]
    fn live_pi_rpc_fixture(title: &str) -> LivePiRpcFixture {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).expect("project dir");
        std::fs::write(project.join("README.md"), "lucarne telegram pi rpc e2e\n").expect("readme");

        let pi_bin = temp.path().join("pi");
        write_fake_pi_rpc_binary(&pi_bin);
        let parent_session = temp.path().join("pi-parent.jsonl");
        let new_session = temp.path().join("pi-new.jsonl");
        let fork_session = temp.path().join("pi-fork.jsonl");
        let env = EnvGuard::set(&[
            ("LUCARNE_PI_BIN", pi_bin.as_os_str().to_os_string()),
            (
                "PI_FAKE_PARENT_SESSION",
                parent_session.as_os_str().to_os_string(),
            ),
            (
                "PI_FAKE_NEW_SESSION",
                new_session.as_os_str().to_os_string(),
            ),
            (
                "PI_FAKE_FORK_SESSION",
                fork_session.as_os_str().to_os_string(),
            ),
            ("LUCARNE_TELEGRAM_LIVE_TIMEOUT", OsString::from("5")),
        ]);

        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = core_with_runtime(runtime);
        let state = BotState::new_with_core(Arc::clone(&core));
        let bot = Arc::new(Bot::new_with_state(
            channel.clone(),
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state.clone(),
        ));
        let ws = WorkspaceId::new("live-pi-rpc");
        state
            .upsert(WorkSession {
                workspace: ws.clone(),
                chat: ChatId::new("100"),
                provider_id: "pi",
                project_path: Some(project),
                title: title.into(),
                live: None,
                resume_ref: None,
            })
            .expect("seed workspace");

        LivePiRpcFixture {
            _temp: temp,
            _env: env,
            channel,
            state,
            bot,
            ws,
        }
    }

    #[cfg(unix)]
    fn write_fake_pi_rpc_binary(path: &Path) {
        // Keep Pi command coverage at the process boundary: Telegram -> runtime -> Pi RPC.
        let script = r#"#!/bin/sh
session_current="${PI_FAKE_PARENT_SESSION}"
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  session_id="pi-fake"
  case "$session_current" in
    "$PI_FAKE_NEW_SESSION") session_id="pi-new" ;;
    "$PI_FAKE_FORK_SESSION") session_id="pi-fork" ;;
  esac
  case "$line" in
    *'"type":"get_state"'*)
      printf '{"id":"%s","type":"response","command":"get_state","success":true,"data":{"sessionId":"%s","sessionFile":"%s","version":"1.2.3","provider":"deepseek","modelId":"deepseek-v4-pro","model":"deepseek/deepseek-v4-pro","thinkingLevel":"high","permissionMode":"default","account":"PI_AUTH_TOKEN (firstParty)","baseUrl":"https://api.pi.example","proxy":"http://127.0.0.1:6152","settingSources":"User settings, Local settings"}}\n' "$id" "$session_id" "$session_current"
      printf '{"type":"agent_start"}\n'
      ;;
    *'"type":"get_commands"'*)
      printf '{"id":"%s","type":"response","command":"get_commands","success":true,"data":{"commands":[{"name":"status","description":"Show Pi status"},{"name":"cost","description":"Show cost timing","source":"skill","displayName":"Cost timing"},{"name":"hello","description":"Run provider-native hello","sourceInfo":"text"},{"name":"set_auto_retry","description":"Enable auto retry"},{"name":"abort_retry","description":"Abort auto retry"}]}}\n' "$id"
      ;;
    *'"type":"get_available_models"'*)
      printf '{"id":"%s","type":"response","command":"get_available_models","success":true,"data":{"models":[{"provider":"fake","id":"fast","description":"Fast fake model","supportsReasoning":true}]}}\n' "$id"
      ;;
    *'"type":"get_session_stats"'*)
      printf '{"id":"%s","type":"response","command":"get_session_stats","success":true,"data":{"inputTokens":1200,"outputTokens":3400,"totalTokens":4600,"context":{"usedTokens":313700,"maxTokens":1000000,"percentUsed":31},"compactions":2,"cost":{"total":0.01}}}\n' "$id"
      ;;
    *'"type":"new_session"'*)
      session_current="${PI_FAKE_NEW_SESSION}"
      printf '{"id":"%s","type":"response","command":"new_session","success":true,"data":{"sessionId":"pi-new","sessionFile":"%s"}}\n' "$id" "$session_current"
      printf '{"type":"agent_start"}\n'
      ;;
    *'"type":"get_fork_messages"'*)
      printf '{"id":"%s","type":"response","command":"get_fork_messages","success":true,"data":{"messages":[{"entryId":"entry-fork-1","text":"Fork from fake user prompt"}]}}\n' "$id"
      ;;
    *'"type":"fork"'*)
      session_current="${PI_FAKE_FORK_SESSION}"
      printf '{"id":"%s","type":"response","command":"fork","success":true,"data":{"text":"Fork from fake user prompt","cancelled":false}}\n' "$id"
      printf '{"type":"agent_start"}\n'
      ;;
    *'"type":"set_model"'*)
      printf '{"id":"%s","type":"response","command":"set_model","success":true,"data":{}}\n' "$id"
      ;;
    *'"type":"set_thinking_level"'*)
      printf '{"id":"%s","type":"response","command":"set_thinking_level","success":true,"data":{}}\n' "$id"
      ;;
    *'"type":"set_auto_retry"'*)
      printf '{"id":"%s","type":"response","command":"set_auto_retry","success":true,"data":{}}\n' "$id"
      ;;
    *'"type":"abort_retry"'*)
      printf '{"id":"%s","type":"response","command":"abort_retry","success":true,"data":{}}\n' "$id"
      ;;
    *'/hello'*)
      printf '{"id":"%s","type":"response","command":"prompt","success":true,"data":{}}\n' "$id"
      printf '{"type":"turn_start"}\n'
      printf '{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"hello command ran"}}\n'
      printf '{"type":"turn_end","message":{"role":"assistant","content":[{"type":"text","text":"hello command ran"}]},"usage":{"inputTokens":1,"outputTokens":2}}\n'
      ;;
    *'"type":"prompt"'*)
      printf '{"id":"%s","type":"response","command":"prompt","success":true,"data":{}}\n' "$id"
      printf '{"type":"turn_start"}\n'
      printf '{"type":"message_update","assistantMessageEvent":{"type":"text_delta","delta":"PI_FAKE_REPLY"}}\n'
      printf '{"type":"turn_end","message":{"role":"assistant","content":[{"type":"text","text":"PI_FAKE_REPLY"}]},"usage":{"inputTokens":1,"outputTokens":2}}\n'
      ;;
    *)
      printf '{"id":"%s","type":"response","command":"unknown","success":false,"error":"unhandled fake pi rpc request"}\n' "$id"
      ;;
  esac
done
"#;
        fs::write(path, script).expect("write fake pi rpc binary");
        let mut perms = fs::metadata(path).expect("fake pi metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("make fake pi executable");
    }

    fn live_clear_channel(channel: &RecordingChannel) {
        channel.sent.lock().unwrap().clear();
        channel.sent_records.lock().unwrap().clear();
        channel.edits.lock().unwrap().clear();
    }

    fn live_message_bodies(channel: &RecordingChannel) -> Vec<String> {
        let mut bodies = channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .map(|message| message.body.clone())
            .collect::<Vec<_>>();
        bodies.extend(
            channel
                .edits
                .lock()
                .unwrap()
                .iter()
                .map(|(_, message)| message.body.clone()),
        );
        bodies
    }

    fn live_assert_message_contains(channel: &RecordingChannel, label: &str, fragments: &[&str]) {
        let bodies = live_message_bodies(channel);
        for fragment in fragments {
            assert!(
                bodies.iter().any(|body| body.contains(fragment)),
                "{label} missing fragment {fragment:?}; bodies: {bodies:?}"
            );
        }
        live_assert_no_failures(channel);
    }

    async fn live_wait_message_contains(
        channel: &RecordingChannel,
        label: &str,
        fragments: &[&str],
    ) {
        timeout(live_bot_e2e_timeout(), async {
            loop {
                let bodies = live_message_bodies(channel);
                if fragments
                    .iter()
                    .all(|fragment| bodies.iter().any(|body| body.contains(fragment)))
                {
                    live_assert_no_failures(channel);
                    return;
                }
                sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{label} timed out waiting for fragments {fragments:?}; bodies: {:?}",
                live_message_bodies(channel)
            )
        });
    }

    fn live_assert_reply_to(channel: &RecordingChannel, message_id: &str) {
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter().any(|message| {
                message
                    .reply_to
                    .as_ref()
                    .is_some_and(|reply| reply.as_str() == message_id)
            }),
            "expected at least one bot message to reply to {message_id}; sent: {sent:?}"
        );
    }

    fn live_find_sent_message(channel: &RecordingChannel, fragment: &str) -> OutgoingMessage {
        channel
            .sent
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|message| message.body.contains(fragment))
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "missing sent message containing {fragment:?}; bodies: {:?}",
                    live_message_bodies(channel)
                )
            })
    }

    async fn live_send_and_expect(
        fixture: &LiveBotFixture,
        text: &str,
        message_id: &str,
        fragments: &[&str],
    ) -> (Option<String>, Option<String>, bool) {
        let before_ref = fixture
            .state
            .get(&fixture.ws)
            .and_then(|session| session.resume_ref);
        live_clear_channel(&fixture.channel);
        live_send_text(&fixture.bot, &fixture.ws, text, message_id).await;
        live_assert_message_contains(&fixture.channel, text, fragments);
        live_assert_state_matches_provider_ref(fixture).await;
        let after = fixture
            .state
            .get(&fixture.ws)
            .expect("live fixture workspace remains bound");
        (before_ref, after.resume_ref, after.live.is_some())
    }

    async fn live_assert_state_matches_provider_ref(fixture: &LiveBotFixture) {
        let Some(session) = fixture.state.get(&fixture.ws) else {
            panic!("live fixture workspace disappeared");
        };
        let Some(live) = session.live else {
            return;
        };
        let provider_ref = live_provider_resume_ref(&live).await;
        if provider_ref.is_some() {
            assert_eq!(
                session.resume_ref, provider_ref,
                "state resume_ref must track the provider-native session id"
            );
        }
    }

    async fn live_history_index_for_resume_ref(
        bot: &Bot,
        provider_id: &'static str,
        resume_ref: &str,
    ) -> usize {
        timeout(live_bot_e2e_timeout(), async {
            loop {
                let page = bot.core.list_history(0, 100);
                if let Some(index) = page.entries.iter().position(|entry| {
                    entry.provider_id == provider_id && entry.session_id == resume_ref
                }) {
                    return index;
                }
                sleep(Duration::from_millis(250)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!("live history entry not available for {provider_id} session {resume_ref}")
        })
    }

    fn live_provider_command_cases(provider_id: &str) -> Vec<(&'static str, Vec<&'static str>)> {
        match provider_id {
            "claude" => vec![
                ("/commands", vec!["agent commands", "provider: `claude`"]),
                (
                    "/status",
                    vec!["status", "Model:", "Directory:", "Session:", "PID:"],
                ),
                ("/model", vec!["models"]),
                ("/permissions", vec!["permission modes"]),
                ("/skills", vec!["skills"]),
                ("/commands status", vec!["status", "Directory:"]),
                ("/commands cost", vec!["Total cost"]),
                ("/model haiku", vec!["Updated model"]),
                ("/permissions default", vec!["Updated permissions"]),
                ("/new", vec!["✓ 完成"]),
                ("/quit", vec!["✓ 完成"]),
            ],
            "codex" => vec![
                ("/commands", vec!["agent commands", "provider: `codex`"]),
                (
                    "/status",
                    vec!["status", "Model:", "Directory:", "Session:", "PID:"],
                ),
                ("/model", vec!["models"]),
                ("/permissions", vec!["permission modes"]),
                ("/skills", vec!["skills"]),
                ("/commands status", vec!["status", "Directory:"]),
                ("/permissions default", vec!["Updated permissions"]),
                ("/new", vec!["✓ 完成"]),
                ("/quit", vec!["✓ 完成"]),
            ],
            _ => vec![
                ("/commands", vec!["agent commands"]),
                ("/status", vec!["status"]),
                ("/model", vec!["models"]),
                ("/permissions", vec!["permission modes"]),
                ("/skills", vec!["skills"]),
                ("/commands status", vec!["status"]),
                ("/new", vec!["✓ 完成"]),
                ("/quit", vec!["✓ 完成"]),
            ],
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_commands_catalog_lists_provider_commands_from_rpc() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc commands");

        live_send_text(&fixture.bot, &fixture.ws, "/commands", "pi-rpc-commands").await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /commands",
            &[
                "agent commands",
                "provider: `pi`",
                "`/status`",
                "`/hello`",
                "`/set_auto_retry`",
                "`/abort_retry`",
            ],
        );
        let commands = live_find_sent_message(&fixture.channel, "agent commands");
        assert!(
            !commands.body.contains("`/cost`"),
            "Pi skill commands must not appear in /commands: {}",
            commands.body
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_status_command_renders_status_from_rpc_state() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc status");
        let parent_ref = "pi-fake";

        live_send_text(&fixture.bot, &fixture.ws, "/status", "pi-rpc-status").await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /status",
            &[
                "status",
                "Version: `1.2.3`",
                "Model: `deepseek/deepseek-v4-pro`",
                "`deepseek-v4-pro`",
                "reasoning high",
                "Permissions: `default`",
                "Account: PI_AUTH_TOKEN (firstParty)",
                "Base URL: `https://api.pi.example`",
                "Proxy: `http://127.0.0.1:6152`",
                "Setting sources: User settings, Local settings",
                "Token usage: 1.2k in / 3.4k out",
                "Context: 313.7k/1m (31%)",
                "Compactions: 2",
                "Provider: `pi`",
                "Session:",
                parent_ref,
            ],
        );
        live_clear_channel(&fixture.channel);

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "/commands status",
            "pi-rpc-nested-status",
        )
        .await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /commands status",
            &[
                "status",
                "Version: `1.2.3`",
                "Model: `deepseek/deepseek-v4-pro`",
                "Token usage: 1.2k in / 3.4k out",
                "Context: 313.7k/1m (31%)",
                "Provider: `pi`",
                "Session:",
                parent_ref,
            ],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_model_command_lists_and_sets_rpc_model() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc model");

        live_send_text(&fixture.bot, &fixture.ws, "/model", "pi-rpc-model-list").await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /model",
            &["models", "`fake/fast`", "reasoning levels: `high`"],
        );
        live_clear_channel(&fixture.channel);

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "/model fake/fast high",
            "pi-rpc-model-set",
        )
        .await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /model set",
            &["Updated model", "fake/fast", "high"],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_permissions_command_renders_pi_permission_catalog() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc permissions");

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "/permissions",
            "pi-rpc-permissions",
        )
        .await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /permissions",
            &["permission modes", "set: `/permissions <mode>`", "(none)"],
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_no_output_catalog_command_completes_without_hanging() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc no output command");

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "/commands set_auto_retry",
            "pi-rpc-set-auto-retry",
        )
        .await;

        live_assert_message_contains(&fixture.channel, "pi /commands set_auto_retry", &["✓ 完成"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_quit_command_closes_live_session_without_hanging() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc quit");

        live_send_text(&fixture.bot, &fixture.ws, "/quit", "pi-rpc-quit").await;

        live_assert_no_failures(&fixture.channel);
        let session = fixture
            .state
            .get(&fixture.ws)
            .expect("workspace should remain after /quit");
        assert!(
            session.live.is_none(),
            "Pi /quit should detach the live session"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_skills_command_renders_skill_catalog_from_rpc_commands() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc skills");

        live_send_text(&fixture.bot, &fixture.ws, "/skills", "pi-rpc-skills").await;

        live_assert_message_contains(&fixture.channel, "pi /skills", &["skills", "`cost`"]);
        assert_eq!(
            fixture
                .state
                .get(&fixture.ws)
                .and_then(|session| session.resume_ref)
                .as_deref(),
            Some("pi-fake"),
            "Pi live commands should persist the provider-native session discovered by RPC"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_new_command_rebinds_to_new_rpc_session() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc new");
        let new_ref = "pi-new";

        live_send_text(&fixture.bot, &fixture.ws, "/new", "pi-rpc-new").await;

        live_assert_no_failures(&fixture.channel);
        let session = fixture
            .state
            .get(&fixture.ws)
            .expect("source workspace remains bound after /new");
        assert_eq!(
            session.resume_ref.as_deref(),
            Some(new_ref),
            "Pi /new must keep the live workspace bound to the new RPC session"
        );
        assert!(
            session.live.is_some(),
            "Pi /new should keep the new live session"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_fork_selection_keeps_unpersisted_fork_live_only() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc fork");

        live_send_text(&fixture.bot, &fixture.ws, "/fork", "pi-rpc-fork-list").await;
        live_assert_message_contains(
            &fixture.channel,
            "pi /fork",
            &["fork targets", "/f1  `entry-fork-1`"],
        );
        let live = fixture
            .state
            .get(&fixture.ws)
            .and_then(|session| session.live)
            .expect("Pi source live session");
        let provisional_ref = provisional_live_resume_ref(&live);
        let fork_workspace = workspace_id_for_resume("pi", &provisional_ref);
        live_clear_channel(&fixture.channel);

        live_send_text(&fixture.bot, &fixture.ws, "/f1", "pi-rpc-fork-select").await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /f1",
            &["✓ forked", "Send a message to start this fork."],
        );
        let fork_session = fixture
            .state
            .get(&fork_workspace)
            .expect("Pi fork should create a live-only child workspace");
        assert!(
            fork_session.resume_ref.is_none(),
            "unpersisted Pi fork must not be stored as a resumable session"
        );
        assert!(
            fork_session.live.is_some(),
            "Pi fork should reuse the live RPC process"
        );
        let fork_topic = fixture.ws.clone();
        assert!(
            fixture
                .channel
                .created_workspaces
                .lock()
                .unwrap()
                .is_empty(),
            "Pi fork selection should rebind the current topic instead of creating a fork topic"
        );
        assert_eq!(
            fixture
                .state
                .workspace_for_handle(&WorkspaceHandle::new(
                    ChatId::new("100"),
                    fork_topic.clone()
                ))
                .as_ref(),
            Some(&fork_workspace),
            "current topic should now route to the Pi fork workspace"
        );
        assert_eq!(
            fixture.state.topic_for_workspace(&fork_workspace).as_ref(),
            Some(&fork_topic),
            "fork workspace should reuse the source Telegram topic"
        );
        live_clear_channel(&fixture.channel);

        live_send_text(&fixture.bot, &fork_topic, "/status", "pi-rpc-fork-status").await;

        live_assert_no_failures(&fixture.channel);
        let fork_session = fixture
            .state
            .get(&fork_workspace)
            .expect("Pi fork workspace remains live-only");
        assert!(
            fork_session.resume_ref.is_none(),
            "/status must not promote Pi's unpersisted fork id into a resume ref: {:?}",
            fork_session.resume_ref
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn pi_live_rpc_catalog_provider_command_is_invokable_from_telegram_commands() {
        let _env_lock = env_lock();
        let fixture = live_pi_rpc_fixture("pi rpc provider command");

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "/commands hello Codex",
            "pi-rpc-hello",
        )
        .await;

        live_assert_message_contains(
            &fixture.channel,
            "pi /commands hello",
            &["hello command ran"],
        );
    }

    async fn live_bot_e2e_core_command_surface(provider_id: &'static str) {
        let fixture = live_bot_fixture(provider_id, &format!("{provider_id} live core"));

        live_send_text(
            &fixture.bot,
            &fixture.ws,
            "Reply with exactly: LIVE_LUCARNE_CORE_READY",
            "core-ready",
        )
        .await;
        assert!(
            fixture
                .state
                .get(&fixture.ws)
                .and_then(|session| session.resume_ref)
                .is_some(),
            "{provider_id} must persist a provider-native resume ref after the first live turn"
        );

        live_send_and_expect(
            &fixture,
            "/commands",
            "core-command-panel",
            &["agent commands", &format!("provider: `{provider_id}`")],
        )
        .await;
        {
            let sent = fixture.channel.sent.lock().unwrap();
            let commands = sent
                .iter()
                .find(|msg| msg.body.contains("agent commands"))
                .expect("commands panel");
            assert!(
                commands.buttons.is_empty(),
                "/commands must render as a text catalog"
            );
        }

        for (index, (command, fragments)) in live_provider_command_cases(provider_id)
            .into_iter()
            .enumerate()
        {
            let (before_ref, after_ref, live_bound) = live_send_and_expect(
                &fixture,
                command,
                &format!("core-command-{provider_id}-{index}"),
                &fragments,
            )
            .await;
            if command == "/new" {
                if live_bound {
                    assert_ne!(
                        before_ref, after_ref,
                        "{provider_id} /new kept live state without a new provider-native session id"
                    );
                    assert!(
                        after_ref.is_some(),
                        "{provider_id} /new kept live state without a resume ref"
                    );
                } else {
                    assert!(
                        after_ref.is_none(),
                        "{provider_id} /new should clear live state when the provider does not expose a new session id"
                    );
                }
                live_send_text(
                    &fixture.bot,
                    &fixture.ws,
                    "Reply with exactly: LIVE_LUCARNE_AFTER_NEW",
                    &format!("core-after-new-{provider_id}"),
                )
                .await;
                live_assert_no_failures(&fixture.channel);
                live_assert_state_matches_provider_ref(&fixture).await;
                assert!(
                    fixture
                        .state
                        .get(&fixture.ws)
                        .and_then(|session| session.resume_ref)
                        .is_some(),
                    "{provider_id} should have a provider-native resume ref after the post-/new turn"
                );
            } else if command == "/quit" {
                assert!(
                    !live_bound,
                    "{provider_id} /quit should close the live session"
                );
                assert!(
                    after_ref.is_some(),
                    "{provider_id} /quit should keep the provider-native resume ref"
                );
                live_send_text(
                    &fixture.bot,
                    &fixture.ws,
                    "Reply with exactly: LIVE_LUCARNE_AFTER_QUIT",
                    &format!("core-after-quit-{provider_id}"),
                )
                .await;
                live_assert_no_failures(&fixture.channel);
                live_assert_state_matches_provider_ref(&fixture).await;
                assert!(
                    fixture
                        .state
                        .get(&fixture.ws)
                        .and_then(|session| session.resume_ref)
                        .is_some(),
                    "{provider_id} should have a provider-native resume ref after the post-/quit turn"
                );
            }
        }
    }

    async fn live_bot_e2e_human_like_workspace_journey(provider_id: &'static str) {
        let fixture = live_bot_fixture(provider_id, &format!("{provider_id} live human"));

        live_send_entry_text(&fixture.bot, "/panel", "human-entry-panel").await;
        live_assert_message_contains(&fixture.channel, "entry panel", &["🛠 lucarne", provider_id]);

        let topic = fixture
            .state
            .topic_for_workspace(&fixture.ws)
            .expect("workspace should have a topic binding");

        live_clear_channel(&fixture.channel);
        live_send_text(
            &fixture.bot,
            &topic,
            "Reply with exactly: LIVE_LUCARNE_HUMAN_ONE",
            "human-user-one",
        )
        .await;
        live_assert_message_contains(
            &fixture.channel,
            "first human turn",
            &["LIVE_LUCARNE_HUMAN_ONE"],
        );
        live_assert_reply_to(&fixture.channel, "human-user-one");
        live_assert_state_matches_provider_ref(&fixture).await;
        let first_resume_ref = fixture
            .state
            .get(&fixture.ws)
            .and_then(|session| session.resume_ref)
            .expect("human journey should persist provider resume ref after first turn");

        live_clear_channel(&fixture.channel);
        live_send_text(&fixture.bot, &topic, "/commands", "human-commands").await;
        let commands = live_find_sent_message(&fixture.channel, "agent commands");
        assert!(
            commands.buttons.is_empty(),
            "/commands must render as a text catalog"
        );
        let command_messages = [
            ("/status", vec!["status"]),
            ("/model", vec!["models"]),
            ("/permissions", vec!["permission modes"]),
            ("/skills", vec!["skills"]),
        ];
        for (command, fragments) in command_messages {
            live_clear_channel(&fixture.channel);
            live_send_text(
                &fixture.bot,
                &topic,
                command,
                &format!("human-command-message-{provider_id}-{command}"),
            )
            .await;
            live_assert_message_contains(&fixture.channel, command, &fragments);
            live_assert_state_matches_provider_ref(&fixture).await;
        }

        live_clear_channel(&fixture.channel);
        live_send_text(&fixture.bot, &topic, "/model", "human-model-list").await;
        let models = live_find_sent_message(&fixture.channel, "models");
        let current_model = live_current_value_from_list(&models.body);
        let model_preferences = match provider_id {
            "claude" => &["haiku", "sonnet", "opus"][..],
            "codex" => &["gpt-5.4", "gpt-5.5"][..],
            _ => &[][..],
        };
        let (set_model_data, model_callback) = if model_preferences.is_empty() {
            live_command_button_with_different_value(
                &fixture.bot,
                &models,
                "model",
                "model",
                current_model.as_deref(),
                &[],
            )
        } else {
            live_command_button_by_value(&fixture.bot, &models, "model", "model", |model| {
                current_model.as_deref() != Some(model)
                    && model_preferences
                        .iter()
                        .any(|wanted| model.to_ascii_lowercase().contains(wanted))
            })
        };
        assert!(
            set_model_data.len() <= 64,
            "Telegram callback data must fit Bot API limit: {set_model_data}"
        );
        let selected_model = model_callback
            .values
            .get("model")
            .and_then(|value| value.as_str())
            .expect("model callback should carry typed model value")
            .to_string();
        let selected_reasoning = model_callback
            .values
            .get("reasoning")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        live_clear_channel(&fixture.channel);
        live_click_button(
            &fixture.bot,
            &topic,
            &set_model_data,
            "human-set-model-button",
        )
        .await;
        live_assert_message_contains(&fixture.channel, "set model", &["Updated model"]);
        live_clear_channel(&fixture.channel);
        live_send_text(&fixture.bot, &topic, "/status", "human-status-after-model").await;
        let mut model_status_fragments = vec![selected_model.as_str()];
        if let Some(reasoning) = selected_reasoning.as_deref() {
            model_status_fragments.push(reasoning);
        }
        live_assert_status_contains(&fixture.channel, "model change", &model_status_fragments);

        live_clear_channel(&fixture.channel);
        live_send_text(
            &fixture.bot,
            &topic,
            "/permissions",
            "human-permissions-list",
        )
        .await;
        let permissions = live_find_sent_message(&fixture.channel, "permission modes");
        let current_permission = live_current_value_from_list(&permissions.body);
        assert!(
            permissions.buttons.is_empty(),
            "/permissions must render as a text catalog"
        );
        let permission_preferences = match provider_id {
            "claude" => &["acceptEdits", "default"][..],
            "codex" => &["auto-review", "default"][..],
            _ => &[][..],
        };
        let available_permissions = live_markdown_list_values(&permissions.body);
        let selected_permission = permission_preferences
            .iter()
            .find(|preferred| {
                current_permission.as_deref() != Some(**preferred)
                    && available_permissions
                        .iter()
                        .any(|mode| mode.as_str() == **preferred)
            })
            .map(|preferred| (*preferred).to_string())
            .or_else(|| {
                available_permissions
                    .iter()
                    .find(|mode| current_permission.as_deref() != Some(mode.as_str()))
                    .cloned()
            })
            .unwrap_or_else(|| {
                panic!(
                    "missing selectable permission mode different from current {current_permission:?}; body: {}",
                    permissions.body
                )
            });
        live_clear_channel(&fixture.channel);
        live_send_text(
            &fixture.bot,
            &topic,
            &format!("/permissions {selected_permission}"),
            "human-set-permissions-text",
        )
        .await;
        live_assert_message_contains(
            &fixture.channel,
            "set permissions",
            &["Updated permissions"],
        );
        live_clear_channel(&fixture.channel);
        live_send_text(
            &fixture.bot,
            &topic,
            "/status",
            "human-status-after-permissions",
        )
        .await;
        live_assert_status_contains(
            &fixture.channel,
            "permissions change",
            &[selected_permission.as_str()],
        );

        live_clear_channel(&fixture.channel);
        live_send_text(
            &fixture.bot,
            &topic,
            "Reply with exactly: LIVE_LUCARNE_HUMAN_TWO",
            "human-user-two",
        )
        .await;
        live_assert_message_contains(
            &fixture.channel,
            "second human turn",
            &["LIVE_LUCARNE_HUMAN_TWO"],
        );
        live_assert_reply_to(&fixture.channel, "human-user-two");
        live_assert_state_matches_provider_ref(&fixture).await;
        assert_eq!(
            fixture
                .state
                .get(&fixture.ws)
                .and_then(|session| session.resume_ref),
            Some(first_resume_ref),
            "{provider_id} should keep the same provider session across human-like turns"
        );

        let created_before_reopen = fixture.channel.created_workspaces.lock().unwrap().len();
        live_clear_channel(&fixture.channel);
        live_send_entry_text(&fixture.bot, "/panel", "human-entry-panel-reopen").await;
        live_send_entry_text(&fixture.bot, "/w1", "human-entry-reopen-workspace").await;
        let created_after_reopen = fixture.channel.created_workspaces.lock().unwrap().len();
        assert_eq!(
            created_after_reopen, created_before_reopen,
            "/w1 should reuse the existing topic during the human-like journey"
        );
        live_assert_no_failures(&fixture.channel);
    }

    async fn live_bot_e2e_history_entry_replays_recent_10_turns(
        mut fixture: LiveBotFixture,
        provider_id: &'static str,
    ) {
        let history_prefix = if provider_id == "codex"
            && fixture
                .recording
                .as_ref()
                .is_some_and(PreparedRecordingRun::is_replay)
        {
            "LIVE_AMUX_HISTORY"
        } else {
            "LIVE_LUCARNE_HISTORY"
        };
        for idx in 1..=10 {
            let expected = format!("{history_prefix}_{idx:02}");
            let prompt = format!("Reply with exactly: {expected}");
            live_clear_channel(&fixture.channel);
            live_send_text(
                &fixture.bot,
                &fixture.ws,
                &prompt,
                &format!("history-live-user-{idx}"),
            )
            .await;
            live_wait_message_contains(
                &fixture.channel,
                &format!("history setup turn {idx}"),
                &[expected.as_str()],
            )
            .await;
            live_assert_state_matches_provider_ref(&fixture).await;
        }
        let resume_ref = fixture
            .state
            .get(&fixture.ws)
            .and_then(|session| session.resume_ref)
            .expect("history live session should have a provider-native resume ref");
        fixture.prepare_recorded_history_effects().await;
        let history_index =
            live_history_index_for_resume_ref(&fixture.bot, provider_id, &resume_ref).await;

        live_clear_channel(&fixture.channel);
        timeout(
            live_bot_e2e_timeout(),
            fixture.bot.clone().handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "live-e2e".into(),
                data: format!("history:{history_index}"),
                source_message: MessageId::new("history-entry-button"),
            }),
        )
        .await
        .expect("live history open timed out")
        .expect("live history open failed");
        live_assert_no_failures(&fixture.channel);

        let sent = fixture.channel.sent.lock().unwrap();
        let markers = sent
            .iter()
            .filter(|message| message.body.starts_with(HISTORY_USER_MARKER))
            .collect::<Vec<_>>();
        assert_eq!(
            markers.len(),
            10,
            "{provider_id} history replay should project the latest 10 user turns; sent: {sent:?}"
        );
        for idx in 1..=10 {
            assert!(
                markers
                    .iter()
                    .any(|message| message.body.contains(&format!("{history_prefix}_{idx:02}"))),
                "{provider_id} history marker missing turn {idx}; sent: {sent:?}"
            );
        }
        drop(sent);

        let records = fixture.channel.sent_records.lock().unwrap();
        for idx in 1..=10 {
            let expected = format!("{history_prefix}_{idx:02}");
            let marker_id = records
                .iter()
                .find(|(_, message)| {
                    message.body.starts_with(HISTORY_USER_MARKER)
                        && message.body.contains(&expected)
                })
                .map(|(id, _)| id.clone())
                .unwrap_or_else(|| panic!("missing marker for {expected}; records: {records:?}"));
            let assistant = records
                .iter()
                .find(|(_, message)| {
                    !message.body.starts_with(HISTORY_USER_MARKER)
                        && message
                            .reply_to
                            .as_ref()
                            .is_some_and(|reply| reply.as_str() == marker_id)
                })
                .map(|(_, message)| message)
                .unwrap_or_else(|| {
                    panic!("missing assistant history reply for {expected}; records: {records:?}")
                });
            assert!(
                !assistant.body.trim().is_empty(),
                "{provider_id} assistant history reply should not be empty for {expected}"
            );
        }
        drop(records);

        timeout(
            live_bot_e2e_timeout(),
            fixture.bot.clone().handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "live-e2e".into(),
                data: format!("history:{history_index}"),
                source_message: MessageId::new("history-entry-button-repeat"),
            }),
        )
        .await
        .expect("live history reopen timed out")
        .expect("live history reopen failed");

        let sent = fixture.channel.sent.lock().unwrap();
        let marker_count = sent
            .iter()
            .filter(|message| message.body.starts_with(HISTORY_USER_MARKER))
            .count();
        assert_eq!(
            marker_count, 10,
            "{provider_id} history reopen should reuse the bound topic without reprojecting already replayed turns; sent: {sent:?}"
        );
    }

    struct RecordingProvider {
        provider_id: &'static str,
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
        opens: Arc<AtomicUsize>,
        resumes: Arc<StdMutex<Vec<String>>>,
        catalog: AgentCommandCatalog,
        fork_targets: Vec<AgentForkTarget>,
        submit_events: Vec<Event>,
    }

    impl RecordingProvider {
        fn new(inputs: Arc<StdMutex<Vec<AgentInput>>>) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations: Arc::new(StdMutex::new(Vec::new())),
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog: test_command_catalog(),
                fork_targets: test_fork_targets(),
                submit_events: Vec::new(),
            }
        }

        fn new_with_invocations(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
        ) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations,
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog: test_command_catalog(),
                fork_targets: test_fork_targets(),
                submit_events: Vec::new(),
            }
        }

        fn new_with_catalog(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
            catalog: AgentCommandCatalog,
        ) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations,
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog,
                fork_targets: test_fork_targets(),
                submit_events: Vec::new(),
            }
        }

        fn new_with_fork_targets(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
            fork_targets: Vec<AgentForkTarget>,
        ) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations,
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog: test_command_catalog(),
                fork_targets,
                submit_events: Vec::new(),
            }
        }

        fn new_with_submit_events(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            submit_events: Vec<Event>,
        ) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations: Arc::new(StdMutex::new(Vec::new())),
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog: test_command_catalog(),
                fork_targets: test_fork_targets(),
                submit_events,
            }
        }

        fn new_with_provider_id(
            provider_id: &'static str,
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
            catalog: AgentCommandCatalog,
        ) -> Self {
            Self {
                provider_id,
                inputs,
                invocations,
                opens: Arc::new(AtomicUsize::new(0)),
                resumes: Arc::new(StdMutex::new(Vec::new())),
                catalog,
                fork_targets: test_fork_targets(),
                submit_events: Vec::new(),
            }
        }

        fn new_with_lifecycle_recording(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
            opens: Arc<AtomicUsize>,
            resumes: Arc<StdMutex<Vec<String>>>,
        ) -> Self {
            Self {
                provider_id: "codex",
                inputs,
                invocations,
                opens,
                resumes,
                catalog: test_command_catalog(),
                fork_targets: test_fork_targets(),
                submit_events: Vec::new(),
            }
        }
    }

    struct BlockingOpenProvider {
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        opens: Arc<AtomicUsize>,
        release_open: Arc<AtomicBool>,
        notify_open: Arc<Notify>,
    }

    #[async_trait]
    impl AgentProvider for BlockingOpenProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        fn capabilities(&self) -> AgentCapabilities {
            AgentCapabilities {
                command_catalog: true,
                ..Default::default()
            }
        }

        async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(
            &self,
            _req: OpenSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            self.opens.fetch_add(1, AtomicOrdering::SeqCst);
            while !self.release_open.load(AtomicOrdering::SeqCst) {
                self.notify_open.notified().await;
            }
            Ok(Box::new(RecordingSession::new(
                "codex",
                Arc::clone(&self.inputs),
                Arc::new(StdMutex::new(Vec::new())),
                test_command_catalog(),
                test_fork_targets(),
                Vec::new(),
            )))
        }

        async fn resume(
            &self,
            _req: ResumeSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            Err(AgentError {
                kind: AgentErrorKind::Unsupported,
                message: "blocking provider resume unsupported".into(),
            })
        }
    }

    #[async_trait]
    impl AgentProvider for RecordingProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static(self.provider_id)
        }

        fn capabilities(&self) -> AgentCapabilities {
            AgentCapabilities {
                command_catalog: true,
                ..Default::default()
            }
        }

        async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static(self.provider_id),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(
            &self,
            _req: OpenSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            self.opens.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Box::new(RecordingSession::new(
                self.provider_id,
                Arc::clone(&self.inputs),
                Arc::clone(&self.invocations),
                self.catalog.clone(),
                self.fork_targets.clone(),
                self.submit_events.clone(),
            )))
        }

        async fn resume(
            &self,
            req: ResumeSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            self.resumes
                .lock()
                .unwrap()
                .push(req.session_ref.0.to_string());
            let session = RecordingSession::new(
                self.provider_id,
                Arc::clone(&self.inputs),
                Arc::clone(&self.invocations),
                self.catalog.clone(),
                self.fork_targets.clone(),
                self.submit_events.clone(),
            );
            *session.provider_session_id.lock().unwrap() =
                Some(SessionId(req.session_ref.0.to_string().into()));
            Ok(Box::new(session))
        }
    }

    struct RecordingSession {
        provider_id: &'static str,
        id: SessionId,
        instance_id: InstanceId,
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
        catalog: AgentCommandCatalog,
        fork_targets: Vec<AgentForkTarget>,
        submit_events: Vec<Event>,
        provider_session_id: Arc<StdMutex<Option<SessionId>>>,
        process_id: Option<i32>,
        model_state: Arc<StdMutex<(String, Option<String>)>>,
        permission_state: Arc<StdMutex<String>>,
        tx: mpsc::Sender<Event>,
        rx: TokioMutex<Option<AgentEventStream>>,
    }

    impl RecordingSession {
        fn new(
            provider_id: &'static str,
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
            catalog: AgentCommandCatalog,
            fork_targets: Vec<AgentForkTarget>,
            submit_events: Vec<Event>,
        ) -> Self {
            let (tx, rx) = mpsc::channel(8);
            let id = SessionId("session-test".into());
            Self {
                provider_id,
                provider_session_id: Arc::new(StdMutex::new(Some(id.clone()))),
                id,
                instance_id: InstanceId("instance-test".into()),
                inputs,
                invocations,
                catalog,
                fork_targets,
                submit_events,
                tx,
                process_id: Some(std::process::id() as i32),
                model_state: Arc::new(StdMutex::new(("gpt-5.5".into(), Some("medium".into())))),
                permission_state: Arc::new(StdMutex::new("Default".into())),
                rx: TokioMutex::new(Some(rx)),
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
            ProviderId::from_static(self.provider_id)
        }

        fn process_id(&self) -> Option<i32> {
            self.process_id
        }

        async fn provider_session_id(&self) -> Option<SessionId> {
            self.provider_session_id.lock().unwrap().clone()
        }

        async fn submit(&self, input: AgentInput) -> std::result::Result<(), AgentError> {
            self.inputs.lock().unwrap().push(input);
            for event in self.submit_events.clone() {
                self.tx.send(event).await.unwrap();
            }
            self.tx
                .send(Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text: "ok".into(),
                    streaming: false,
                }))
                .await
                .unwrap();
            self.tx
                .send(Event::TurnCompleted(TurnCompletedEvent {
                    turn_id: "turn-test".into(),
                    usage: None,
                }))
                .await
                .unwrap();
            Ok(())
        }

        async fn list_commands(&self) -> std::result::Result<AgentCommandCatalog, AgentError> {
            Ok(self.catalog.clone())
        }

        async fn set_model(
            &self,
            selection: AgentModelSelection,
        ) -> std::result::Result<AgentStatus, AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "model".into(),
                    args: Some(match selection.reasoning.as_deref() {
                        Some(reasoning) => format!("{} {reasoning}", selection.model).into(),
                        None => selection.model.to_string().into(),
                    }),
                    values: serde_json::json!({
                        "model": selection.model.as_str(),
                        "reasoning": selection.reasoning.as_deref(),
                    }),
                    source: AgentCommandSource::AdapterMapped,
                });
            *self.model_state.lock().unwrap() = (
                selection.model.to_string(),
                selection.reasoning.as_deref().map(str::to_string),
            );
            Ok(AgentStatus {
                model: Some(selection.model),
                reasoning: selection.reasoning,
                ..Default::default()
            })
        }

        async fn set_permissions(
            &self,
            selection: AgentPermissionSelection,
        ) -> std::result::Result<AgentStatus, AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "permissions".into(),
                    args: Some(selection.mode.to_string().into()),
                    values: serde_json::json!({
                        "mode": selection.mode.as_str(),
                    }),
                    source: AgentCommandSource::AdapterMapped,
                });
            *self.permission_state.lock().unwrap() = selection.mode.to_string();
            Ok(AgentStatus {
                permissions: Some(selection.mode.to_string().into()),
                ..Default::default()
            })
        }

        async fn list_skills(&self) -> std::result::Result<AgentSkillCatalog, AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "skills".into(),
                    args: None,
                    values: serde_json::json!({}),
                    source: AgentCommandSource::AdapterMapped,
                });
            Ok(AgentSkillCatalog {
                skills: vec![AgentSkillSummary {
                    name: "frontend-design".into(),
                    display_name: Some("Frontend Design".into()),
                    description: Some("Build production-grade interfaces.".into()),
                    path: None,
                    scope: None,
                    source: Some("user".into()),
                    tokens: Some(47),
                    enabled: None,
                }],
            })
        }

        async fn list_fork_targets(
            &self,
        ) -> std::result::Result<AgentForkTargetCatalog, AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "fork".into(),
                    args: None,
                    values: serde_json::json!({}),
                    source: AgentCommandSource::AdapterMapped,
                });
            Ok(AgentForkTargetCatalog {
                targets: self.fork_targets.clone(),
            })
        }

        async fn fork(
            &self,
            selection: AgentForkSelection,
        ) -> std::result::Result<AgentForkResult, AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "fork".into(),
                    args: Some(selection.target_id.to_string().into()),
                    values: serde_json::json!({
                        "target_id": selection.target_id.as_str(),
                    }),
                    source: AgentCommandSource::AdapterMapped,
                });
            let source_session_ref = self
                .provider_session_id
                .lock()
                .unwrap()
                .clone()
                .map(|session_id| SessionRef(session_id.0));
            if selection.target_id.as_str() == "no-ref" {
                *self.provider_session_id.lock().unwrap() = None;
                return Ok(AgentForkResult {
                    session_ref: None,
                    source_session_ref,
                });
            }
            *self.provider_session_id.lock().unwrap() = Some(SessionId("session-fork".into()));
            Ok(AgentForkResult {
                session_ref: Some(SessionRef("session-fork".into())),
                source_session_ref,
            })
        }

        async fn new(&self) -> std::result::Result<(), AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "new".into(),
                    args: None,
                    values: serde_json::json!({}),
                    source: AgentCommandSource::AdapterMapped,
                });
            *self.provider_session_id.lock().unwrap() = Some(SessionId("session-new".into()));
            Ok(())
        }

        async fn quit(&self) -> std::result::Result<(), AgentError> {
            self.invocations
                .lock()
                .unwrap()
                .push(AgentCommandInvocation {
                    name: "quit".into(),
                    args: None,
                    values: serde_json::json!({}),
                    source: AgentCommandSource::AdapterMapped,
                });
            Ok(())
        }

        async fn invoke_command(
            &self,
            command: AgentCommandInvocation,
        ) -> std::result::Result<AgentCommandResult, AgentError> {
            let recorded = command.clone();
            self.invocations.lock().unwrap().push(command);
            let result = AgentCommandResult {
                name: recorded.name.clone(),
                source: AgentCommandSource::AdapterMapped,
                data: AgentRuntimeCommandResultData::Empty,
            };
            if recorded.name.as_str() == "new" && recorded.args.is_none() {
                *self.provider_session_id.lock().unwrap() = Some(SessionId("session-new".into()));
                self.tx
                    .send(Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text: "Started new thread session-new.".into(),
                        streaming: false,
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "model" && recorded.args.is_some() {
                let args = recorded.args.as_deref().unwrap_or_default();
                let mut parts = args.split_whitespace();
                let model = parts.next().unwrap_or(args).to_string();
                let reasoning = parts.next().map(str::to_string);
                *self.model_state.lock().unwrap() = (model.clone(), reasoning.clone());
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "model".into(),
                        result: CommandResultPayload {
                            command: "model".into(),
                            result: CommandResultData::ModelChanged(AgentModelSelection {
                                model: model.into(),
                                reasoning: reasoning.map(Into::into),
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "model" && recorded.args.is_none() {
                let (current_model, current_reasoning) = self.model_state.lock().unwrap().clone();
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "model".into(),
                        result: CommandResultPayload {
                            command: "model".into(),
                            result: CommandResultData::Models(AgentModelCatalog {
                                current_model: Some(current_model.into()),
                                current_reasoning: current_reasoning.map(Into::into),
                                models: vec![
                                    AgentModelOption {
                                        id: "gpt-5.5".into(),
                                        display_name: Some("GPT-5.5".into()),
                                        description: Some("Frontier model".into()),
                                        supported_reasoning: vec![
                                            AgentReasoningOption {
                                                value: "medium".into(),
                                                description: None,
                                                is_default: Some(true),
                                            },
                                            AgentReasoningOption {
                                                value: "high".into(),
                                                description: None,
                                                is_default: None,
                                            },
                                        ],
                                    },
                                    AgentModelOption {
                                        id: "gpt-5.4".into(),
                                        display_name: None,
                                        description: None,
                                        supported_reasoning: Vec::new(),
                                    },
                                ],
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "permissions" && recorded.args.is_some() {
                let mode = recorded.args.as_deref().unwrap_or_default();
                *self.permission_state.lock().unwrap() = mode.to_string();
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "permissions".into(),
                        result: CommandResultPayload {
                            command: "permissions".into(),
                            result: CommandResultData::PermissionsChanged(
                                AgentPermissionSelection { mode: mode.into() },
                            ),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "permissions" && recorded.args.is_none() {
                let current_mode = self.permission_state.lock().unwrap().clone();
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "permissions".into(),
                        result: CommandResultPayload {
                            command: "permissions".into(),
                            result: CommandResultData::Permissions(AgentPermissionCatalog {
                                current_mode: Some(current_mode.into()),
                                modes: vec![
                                    AgentPermissionOption {
                                        id: "on-request".into(),
                                        display_name: None,
                                        description: None,
                                    },
                                    AgentPermissionOption {
                                        id: "never".into(),
                                        display_name: None,
                                        description: None,
                                    },
                                ],
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "skills" && recorded.args.is_none() {
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "skills".into(),
                        result: CommandResultPayload {
                            command: "skills".into(),
                            result: CommandResultData::Skills(AgentSkillCatalog {
                                skills: vec![AgentSkillSummary {
                                    name: "frontend-design".into(),
                                    display_name: Some("Frontend Design".into()),
                                    description: Some("Build production-grade interfaces.".into()),
                                    path: None,
                                    scope: None,
                                    source: Some("user".into()),
                                    tokens: Some(47),
                                    enabled: None,
                                }],
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "status" && recorded.args.is_none() {
                let (model, reasoning) = self.model_state.lock().unwrap().clone();
                let permissions = self.permission_state.lock().unwrap().clone();
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "status".into(),
                        result: CommandResultPayload {
                            command: "status".into(),
                            result: CommandResultData::Status(AgentStatus {
                                session_id: Some("session-test".into()),
                                directory: Some("/tmp/project".into()),
                                model: Some(model.clone().into()),
                                model_detail: Some(format!("{model}-2026-04-01").into()),
                                reasoning: reasoning.map(Into::into),
                                permissions: Some(permissions.into()),
                                tokens: Some(AgentTokenUsage {
                                    input_tokens: Some(1900),
                                    output_tokens: Some(387),
                                    total_tokens: Some(2287),
                                }),
                                context: Some(AgentContextUsage {
                                    used_tokens: Some(14000),
                                    max_tokens: Some(205000),
                                    percent_used: Some(7),
                                }),
                                compactions: Some(0),
                                ..Default::default()
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "structured" {
                return Ok(AgentCommandResult {
                    name: recorded.name.clone(),
                    source: AgentCommandSource::AdapterMapped,
                    data: AgentRuntimeCommandResultData::Text {
                        text: "structured ok".into(),
                    },
                });
            } else if self.catalog.commands.iter().any(|cmd| {
                command_supports_name(cmd, recorded.name.as_str())
                    && cmd.source == AgentCommandSource::ProviderNative
            }) {
                let should_stay_silent = self.catalog.commands.iter().any(|cmd| {
                    command_supports_name(cmd, recorded.name.as_str())
                        && matches!(
                            &cmd.input,
                            AgentCommandInput::Text { label, .. } if label.as_str() == "no_output"
                        )
                });
                if should_stay_silent {
                    return Ok(result);
                }
                self.tx
                    .send(Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text: format!("agent {} output", recorded.name).into(),
                        streaming: false,
                    }))
                    .await
                    .unwrap();
                self.tx
                    .send(Event::TurnCompleted(TurnCompletedEvent {
                        turn_id: "command-test".into(),
                        usage: None,
                    }))
                    .await
                    .unwrap();
                return Ok(result);
            } else if recorded.name.as_str() == "usage" && recorded.args.is_none() {
                self.tx
                    .send(Event::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text: format!("agent {} output", recorded.name).into(),
                        streaming: false,
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "fork" && recorded.args.as_deref() == Some("no-ref")
            {
                let source_session_ref = self
                    .provider_session_id
                    .lock()
                    .unwrap()
                    .clone()
                    .map(|session_id| SessionRef(session_id.0));
                *self.provider_session_id.lock().unwrap() = None;
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "fork".into(),
                        result: CommandResultPayload {
                            command: "fork".into(),
                            result: CommandResultData::Forked(AgentForkResult {
                                session_ref: None,
                                source_session_ref,
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "fork" && recorded.args.is_some() {
                let source_session_ref = self
                    .provider_session_id
                    .lock()
                    .unwrap()
                    .clone()
                    .map(|session_id| SessionRef(session_id.0));
                *self.provider_session_id.lock().unwrap() = Some(SessionId("session-fork".into()));
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "fork".into(),
                        result: CommandResultPayload {
                            command: "fork".into(),
                            result: CommandResultData::Forked(AgentForkResult {
                                session_ref: Some(SessionRef("session-fork".into())),
                                source_session_ref,
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            } else if recorded.name.as_str() == "fork" && recorded.args.is_none() {
                self.tx
                    .send(Event::CommandResult(CommandResultEvent {
                        command: "fork".into(),
                        result: CommandResultPayload {
                            command: "fork".into(),
                            result: CommandResultData::ForkTargets(AgentForkTargetCatalog {
                                targets: self.fork_targets.clone(),
                            }),
                        },
                    }))
                    .await
                    .unwrap();
            }
            self.tx
                .send(Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text: "command ok".into(),
                    streaming: false,
                }))
                .await
                .unwrap();
            self.tx
                .send(Event::TurnCompleted(TurnCompletedEvent {
                    turn_id: "command-test".into(),
                    usage: None,
                }))
                .await
                .unwrap();
            Ok(result)
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
            self.rx.lock().await.take().ok_or_else(|| AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "events already taken".into(),
            })
        }

        async fn close(&self) -> std::result::Result<(), AgentError> {
            Ok(())
        }
    }

    struct ClosedSession {
        id: SessionId,
        instance_id: InstanceId,
    }

    impl ClosedSession {
        fn new() -> Self {
            Self::with_ids("closed-session", "closed-instance")
        }

        fn with_ids(session_id: &str, instance_id: &str) -> Self {
            Self {
                id: SessionId(session_id.into()),
                instance_id: InstanceId(instance_id.into()),
            }
        }
    }

    #[async_trait]
    impl AgentSession for ClosedSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, _input: AgentInput) -> std::result::Result<(), AgentError> {
            panic!("closed session should not receive submit")
        }

        async fn list_commands(&self) -> std::result::Result<AgentCommandCatalog, AgentError> {
            panic!("closed session should not be reused for commands")
        }

        async fn invoke_command(
            &self,
            _command: AgentCommandInvocation,
        ) -> std::result::Result<AgentCommandResult, AgentError> {
            panic!("closed session should not receive command invocations")
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
            Err(AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "closed session has no event stream".into(),
            })
        }

        async fn close(&self) -> std::result::Result<(), AgentError> {
            Ok(())
        }

        async fn observed_close_reason(&self) -> Option<SmolStr> {
            Some("ok".into())
        }
    }

    fn runtime_with_recorder(inputs: Arc<StdMutex<Vec<AgentInput>>>) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(inputs)));
        runtime
    }

    fn runtime_with_submit_events(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        submit_events: Vec<Event>,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_submit_events(
            inputs,
            submit_events,
        )));
        runtime
    }

    fn runtime_with_command_recorder(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_invocations(
            inputs,
            invocations,
        )));
        runtime
    }

    fn runtime_with_command_catalog_recorder(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
        catalog: AgentCommandCatalog,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_catalog(
            inputs,
            invocations,
            catalog,
        )));
        runtime
    }

    fn runtime_with_lifecycle_command_recorder(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        invocations: Arc<StdMutex<Vec<AgentCommandInvocation>>>,
        opens: Arc<AtomicUsize>,
        resumes: Arc<StdMutex<Vec<String>>>,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_lifecycle_recording(
            inputs,
            invocations,
            opens,
            resumes,
        )));
        runtime
    }

    fn test_command_catalog() -> AgentCommandCatalog {
        AgentCommandCatalog {
            commands: vec![
                AgentCommand {
                    name: "model".into(),
                    description: Some("List or switch the model".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::JsonSchema {
                        schema: json!({
                            "type": "object",
                            "properties": {
                                "model": {
                                    "type": "string",
                                    "enum": ["gpt-5.5", "gpt-5.4"]
                                },
                                "reasoning": {
                                    "type": "string",
                                    "enum": ["low", "medium", "high", "xhigh"]
                                }
                            }
                        }),
                    },
                    completion: AgentCommandCompletion::CommandResult,
                },
                AgentCommand {
                    name: "permissions".into(),
                    description: Some("View or adjust permissions".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: enum_command_input(
                        "approval_policy",
                        &["untrusted", "on-request", "on-failure", "never"],
                    ),
                    completion: AgentCommandCompletion::CommandResult,
                },
                AgentCommand {
                    name: "skills".into(),
                    description: Some("List skills".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::CommandResult,
                },
                AgentCommand {
                    name: "status".into(),
                    description: Some("Read status".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::CommandResult,
                },
                AgentCommand {
                    name: "new".into(),
                    description: Some("Run new".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::TurnCompleted,
                },
                AgentCommand {
                    name: "quit".into(),
                    description: Some("Run quit".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::TurnCompleted,
                },
                AgentCommand {
                    name: "fork".into(),
                    description: Some("List fork targets or fork a selected target".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::Text {
                        label: "target".into(),
                        required: false,
                    },
                    completion: AgentCommandCompletion::CommandResult,
                },
            ],
            complete: true,
            revision: 1,
        }
    }

    fn test_fork_targets() -> Vec<AgentForkTarget> {
        vec![AgentForkTarget {
            id: "msg-2".into(),
            label: Some("assistant 2".into()),
            description: Some("Explain the plan".into()),
        }]
    }

    fn catalog_with_plain_adapter_command(name: &str) -> AgentCommandCatalog {
        AgentCommandCatalog {
            commands: vec![AgentCommand {
                name: name.into(),
                description: Some(format!("Run {name}").into()),
                aliases: Vec::new(),
                source: AgentCommandSource::AdapterMapped,
                input: AgentCommandInput::None,
                completion: AgentCommandCompletion::TurnCompleted,
            }],
            complete: true,
            revision: 1,
        }
    }

    struct RecoveringProvider {
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        opens: AtomicUsize,
        resumes: Arc<StdMutex<Vec<String>>>,
        first_close_reason: Option<SmolStr>,
    }

    impl RecoveringProvider {
        fn new(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            resumes: Arc<StdMutex<Vec<String>>>,
        ) -> Self {
            Self::new_with_first_close_reason(inputs, resumes, None)
        }

        fn new_with_first_close_reason(
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            resumes: Arc<StdMutex<Vec<String>>>,
            first_close_reason: Option<SmolStr>,
        ) -> Self {
            Self {
                inputs,
                opens: AtomicUsize::new(0),
                resumes,
                first_close_reason,
            }
        }
    }

    #[async_trait]
    impl AgentProvider for RecoveringProvider {
        fn id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn probe(&self) -> std::result::Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: ProviderId::from_static("codex"),
                provider_version: Some("test".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(
            &self,
            _req: OpenSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            let idx = self.opens.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Box::new(RecoveringSession::new(
                format!("session-{}", idx + 1),
                Arc::clone(&self.inputs),
                idx == 0,
                if idx == 0 {
                    self.first_close_reason.clone()
                } else {
                    None
                },
            )))
        }

        async fn resume(
            &self,
            req: ResumeSession,
        ) -> std::result::Result<Box<dyn AgentSession>, AgentError> {
            self.resumes
                .lock()
                .unwrap()
                .push(req.session_ref.0.to_string());
            let idx = self.opens.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(Box::new(RecoveringSession::new(
                format!("session-{}", idx + 1),
                Arc::clone(&self.inputs),
                false,
                None,
            )))
        }
    }

    struct RecoveringSession {
        id: SessionId,
        instance_id: InstanceId,
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        fail_submit: bool,
        close_reason: Option<SmolStr>,
        tx: mpsc::Sender<Event>,
        rx: TokioMutex<Option<AgentEventStream>>,
    }

    impl RecoveringSession {
        fn new(
            id: String,
            inputs: Arc<StdMutex<Vec<AgentInput>>>,
            fail_submit: bool,
            close_reason: Option<SmolStr>,
        ) -> Self {
            let (tx, rx) = mpsc::channel(8);
            Self {
                id: SessionId(id.clone().into()),
                instance_id: InstanceId(format!("instance-{id}").into()),
                inputs,
                fail_submit,
                close_reason,
                tx,
                rx: TokioMutex::new(Some(rx)),
            }
        }
    }

    #[async_trait]
    impl AgentSession for RecoveringSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("codex")
        }

        async fn submit(&self, input: AgentInput) -> std::result::Result<(), AgentError> {
            if self.fail_submit {
                return Err(AgentError {
                    kind: AgentErrorKind::Internal,
                    message: "io: Broken pipe (os error 32)".into(),
                });
            }
            self.inputs.lock().unwrap().push(input);
            self.tx
                .send(Event::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text: "ok".into(),
                    streaming: false,
                }))
                .await
                .unwrap();
            self.tx
                .send(Event::TurnCompleted(TurnCompletedEvent {
                    turn_id: "turn-test".into(),
                    usage: None,
                }))
                .await
                .unwrap();
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
            self.rx.lock().await.take().ok_or_else(|| AgentError {
                kind: AgentErrorKind::InvalidState,
                message: "events already taken".into(),
            })
        }

        async fn close(&self) -> std::result::Result<(), AgentError> {
            Ok(())
        }

        async fn observed_close_reason(&self) -> Option<SmolStr> {
            self.close_reason.clone()
        }
    }

    fn runtime_with_recovering_provider(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        resumes: Arc<StdMutex<Vec<String>>>,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecoveringProvider::new(inputs, resumes)));
        runtime
    }

    fn runtime_with_idle_recycled_recovering_provider(
        inputs: Arc<StdMutex<Vec<AgentInput>>>,
        resumes: Arc<StdMutex<Vec<String>>>,
    ) -> Arc<AgentRuntime> {
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecoveringProvider::new_with_first_close_reason(
            inputs,
            resumes,
            Some("idle timeout".into()),
        )));
        runtime
    }

    fn bind_test_workspace(bot: &Bot) {
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("2"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: None,
                title: "codex".into(),
                live: None,
                resume_ref: None,
            })
            .expect("bind test workspace");
    }

    fn image_attachment(file_ref: &str) -> IncomingAttachment {
        IncomingAttachment {
            file_ref: file_ref.into(),
            filename: Some("photo.jpg".into()),
            mime_type: Some("image/jpeg".into()),
            size: Some(3),
        }
    }

    fn image_input(media_type: &str, data_base64: &str) -> AgentImageInput {
        AgentImageInput {
            media_type: media_type.into(),
            data_base64: data_base64.into(),
        }
    }

    fn write_codex_history_session(codex_home: &Path, session_id: &str, cwd: &str, prompt: &str) {
        let dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&dir).expect("create codex session dir");
        let path = dir.join(format!("rollout-2026-04-25T00-00-00-{session_id}.jsonl"));
        let content = [
            format!(
                r#"{{"timestamp":"2026-04-25T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.4"}}}}"#
            ),
            format!(
                r#"{{"timestamp":"2026-04-25T00:00:01.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{prompt}"}}]}}}}"#
            ),
        ]
        .join("\n");
        fs::write(path, content).expect("write codex history session");
    }

    fn write_codex_history_session_with_turns(
        codex_home: &Path,
        session_id: &str,
        cwd: &str,
        turns: usize,
    ) {
        let dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&dir).expect("create codex session dir");
        let path = dir.join(format!("rollout-2026-04-25T00-00-00-{session_id}.jsonl"));
        let mut lines = vec![format!(
            r#"{{"timestamp":"2026-04-25T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.4"}}}}"#
        )];
        for idx in 1..=turns {
            lines.push(format!(
                r#"{{"timestamp":"2026-04-25T00:{idx:02}:00.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"user {idx}"}}]}}}}"#
            ));
            lines.push(format!(
                r#"{{"timestamp":"2026-04-25T00:{idx:02}:01.000Z","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"assistant {idx}"}}]}}}}"#
            ));
        }
        fs::write(path, lines.join("\n")).expect("write codex history session");
    }

    fn write_codex_history_session_with_image(codex_home: &Path, session_id: &str, cwd: &str) {
        let dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&dir).expect("create codex session dir");
        let path = dir.join(format!("rollout-2026-04-25T00-00-00-{session_id}.jsonl"));
        let content = [
            format!(
                r#"{{"timestamp":"2026-04-25T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.4"}}}}"#
            ),
            r#"{"timestamp":"2026-04-25T00:01:00.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"look at this image"},{"type":"input_image","image_url":"data:image/png;base64,AQID"}]}}"#.to_string(),
            r#"{"timestamp":"2026-04-25T00:01:01.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"image seen"}]}}"#.to_string(),
        ]
        .join("\n");
        fs::write(path, content).expect("write codex image history session");
    }

    fn write_codex_history_session_with_local_image(
        codex_home: &Path,
        session_id: &str,
        cwd: &str,
        image_path: &Path,
    ) {
        let dir = codex_home.join("sessions/2026/04/25");
        fs::create_dir_all(&dir).expect("create codex session dir");
        let path = dir.join(format!("rollout-2026-04-25T00-00-00-{session_id}.jsonl"));
        let image_path_json =
            serde_json::to_string(image_path.to_str().expect("utf-8 image path")).unwrap();
        let content = [
            format!(
                r#"{{"timestamp":"2026-04-25T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{cwd}","originator":"codex-cli","model":"gpt-5.4"}}}}"#
            ),
            format!(
                r#"{{"timestamp":"2026-04-25T00:01:00.000Z","type":"event_msg","payload":{{"type":"user_message","message":"look at local image","images":[],"local_images":[{image_path_json}],"text_elements":[]}}}}"#
            ),
            r#"{"timestamp":"2026-04-25T00:01:01.000Z","type":"event_msg","payload":{"type":"agent_message","message":"local image seen","phase":"final_answer","memory_citation":null}}"#.to_string(),
        ]
        .join("\n");
        fs::write(path, content).expect("write codex local image history session");
    }

    fn write_copilot_history_session(home: &Path, session_id: &str, prompt: &str) {
        let dir = home.join(".copilot/session-state/session-1");
        fs::create_dir_all(&dir).expect("create copilot session dir");
        let content = [
            format!(
                r#"{{"type":"session.start","timestamp":"2026-04-25T00:00:00.000Z","data":{{"sessionId":"{session_id}","selectedModel":"gpt-5.4"}}}}"#
            ),
            format!(
                r#"{{"type":"user.message","timestamp":"2026-04-25T00:00:01.000Z","data":{{"messageId":"u1","content":"{prompt}"}}}}"#
            ),
        ]
        .join("\n");
        fs::write(dir.join("events.jsonl"), content).expect("write copilot history session");
        fs::write(
            dir.join("workspace.yaml"),
            format!("cwd: /tmp/copilot-project\nsummary: {prompt}\n"),
        )
        .expect("write copilot workspace metadata");
    }

    fn write_pi_history_session(home: &Path, session_id: &str, prompt: &str) {
        let dir = home.join(".pi/agent/sessions/--tmp-pi-project--");
        fs::create_dir_all(&dir).expect("create pi session dir");
        let content = [
            format!(
                r#"{{"type":"session","version":3,"id":"{session_id}","timestamp":"2026-05-11T09:47:45.778Z","cwd":"/tmp/pi-project"}}"#
            ),
            format!(
                r#"{{"type":"message","id":"u1","timestamp":"2026-05-11T09:47:46.778Z","message":{{"role":"user","content":[{{"type":"text","text":"{prompt}"}}]}}}}"#
            ),
        ]
        .join("\n");
        fs::write(
            dir.join(format!("2026-05-11T09-47-45-778Z_{session_id}.jsonl")),
            content,
        )
        .expect("write pi history session");
    }

    #[tokio::test]
    async fn history_entry_with_existing_resume_ref_reuses_workspace_topic() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session(&codex_home, "thread-existing", "/tmp/project", "resume me");

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("4"),
                chat: ChatId::new("200"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex old".into(),
                live: None,
                resume_ref: Some("thread-existing".into()),
            })
            .expect("insert existing workspace");

        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        assert!(
            channel.deleted_workspaces.lock().unwrap().is_empty(),
            "panel re-entry must reuse the prior bound Telegram topic when it still exists"
        );
        assert!(
            channel.created_workspaces.lock().unwrap().is_empty(),
            "panel re-entry must not recreate the Telegram topic for the resumed session"
        );
        assert!(
            channel.renames.lock().unwrap().is_empty(),
            "history selection must not rename a Telegram topic to probe existence"
        );
        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["thread-existing"],
            "existing persisted workspace should resume through the provider session id"
        );
        assert!(bot.state.get(&WorkspaceId::new("4")).is_some());
        let sent = channel.sent.lock().unwrap();
        assert!(sent.iter().any(|msg| msg.body.contains("✓ resumed")
            && msg.body.contains("thread-existing")
            && msg.body.contains("/tmp/project")));
        assert!(sent
            .iter()
            .any(|msg| { msg.body.starts_with("👤 ") && msg.body.contains("resume me") }));
        assert!(
            !sent.iter().any(|msg| msg.body.trim() == "↪ jump here"),
            "history selection should send the resume notice in the topic, not a jump ping"
        );
        assert!(
            !sent.iter().any(|msg| msg.body.trim() == "↩ resumed"),
            "history selection must not early-return with an invisible short ack"
        );
    }

    #[tokio::test]
    async fn history_entry_replaces_stale_active_provider_ref_on_canonical_workspace() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session(&codex_home, "thread-existing", "/tmp/project", "resume me");

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );
        let canonical_workspace = workspace_id_for_resume("codex", "thread-existing");
        bot.state
            .upsert(WorkSession {
                workspace: canonical_workspace,
                chat: ChatId::new("200"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex old".into(),
                live: None,
                resume_ref: Some("stale-thread".into()),
            })
            .expect("insert polluted canonical workspace");

        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["thread-existing"],
            "history panel selection must replace stale active provider refs on the canonical workspace"
        );
    }

    #[tokio::test]
    async fn history_replay_topic_replaces_stale_active_provider_ref_before_status() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", tmp.path().join("codex").into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_lifecycle_command_recorder(
                inputs,
                invocations,
                opens,
                Arc::clone(&resumes),
            ),
        );
        let workspace = workspace_id_for_resume("codex", "thread-existing");
        let topic = WorkspaceId::new("topic-existing");
        let session = WorkSession {
            workspace: workspace.clone(),
            chat: ChatId::new("200"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex old".into(),
            live: None,
            resume_ref: Some("stale-thread".into()),
        };
        bot.state
            .upsert_with_topic(session, topic.clone())
            .expect("insert polluted history topic");
        let replay = bot.state.new_history_replay_record(
            &workspace,
            &ChatId::new("200"),
            &topic,
            "codex",
            "thread-existing",
            tmp.path().join("thread-existing.jsonl"),
        );
        bot.state
            .upsert_history_replay_record(replay)
            .expect("insert history replay record");

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("status"),
            chat: ChatId::new("200"),
            workspace: Some(topic),
            reply_to: None,
            user: "u".into(),
            text: Some("/status".into()),
            attachments: Vec::new(),
        }))
        .await
        .expect("handle status");

        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["thread-existing"],
            "bound history topics must repair stale active refs before command resume"
        );
    }

    #[tokio::test]
    async fn history_entry_replays_recent_10_turns_with_assistant_replies() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_turns(&codex_home, "thread-history", "/tmp/project", 12);

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );

        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let user_markers = sent
            .iter()
            .filter(|msg| msg.body.starts_with("👤 "))
            .collect::<Vec<_>>();
        assert_eq!(
            user_markers.len(),
            10,
            "opening history should replay the latest 10 user turns; sent: {sent:?}"
        );
        assert!(user_markers[0].body.contains("user 3"));
        assert!(user_markers[9].body.contains("user 12"));
        for idx in 3..=12 {
            assert!(
                sent.iter().any(|msg| {
                    msg.body.starts_with(&format!("assistant {idx}")) && msg.reply_to.is_some()
                }),
                "assistant {idx} should be sent as a reply; sent: {sent:?}"
            );
        }
        assert_ne!(
            sent.iter()
                .find(|msg| {
                    msg.buttons
                        .iter()
                        .flatten()
                        .any(|button| button.data.starts_with("historyolder:c:"))
                })
                .map(|msg| msg.body.as_str()),
            Some("Load older history"),
            "load-older message body must not duplicate the button label"
        );
        assert!(
            sent.iter().any(|msg| {
                msg.buttons
                    .iter()
                    .flatten()
                    .any(|button| button.data.starts_with("historyolder:c:"))
            }),
            "recent replay should expose a load-older button; sent: {sent:?}"
        );
        drop(sent);

        let records = channel.sent_records.lock().unwrap();
        let resume_idx = records
            .iter()
            .position(|(_, msg)| msg.body.contains("✓ resumed"))
            .expect("resume notice");
        let older_idx = records
            .iter()
            .position(|(_, msg)| {
                msg.buttons
                    .iter()
                    .flatten()
                    .any(|button| button.data.starts_with("historyolder:c:"))
            })
            .expect("load older control");
        let first_history_idx = records
            .iter()
            .position(|(_, msg)| msg.body.starts_with("👤 "))
            .expect("first history message");
        assert!(
            resume_idx < older_idx && older_idx < first_history_idx,
            "load-older control should sit between resume notice and first replayed history message: {records:?}"
        );
    }

    #[tokio::test]
    async fn history_entry_reuses_topic_and_suppresses_replay_when_reopened() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_turns(&codex_home, "thread-history", "/tmp/project", 12);

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );

        Arc::clone(&bot).open_history_entry(0).await.unwrap();
        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        let deleted_topics = channel.deleted_workspaces.lock().unwrap();
        assert!(
            deleted_topics.is_empty(),
            "reopen must reuse the prior bound topic; deleted_workspaces: {deleted_topics:?}"
        );
        drop(deleted_topics);

        let created_topics = channel.created_workspaces.lock().unwrap();
        assert_eq!(
            created_topics.len(),
            1,
            "reopen must not create a fresh topic when the binding still exists; created: {created_topics:?}"
        );
        drop(created_topics);

        let sent = channel.sent.lock().unwrap();
        let total_user_markers = sent
            .iter()
            .filter(|msg| msg.body.starts_with("👤 "))
            .count();
        assert_eq!(
            total_user_markers, 10,
            "reopen must not reproject an already replayed transcript; sent: {sent:?}"
        );
        let older_buttons: Vec<_> = sent
            .iter()
            .flat_map(|msg| msg.buttons.iter().flatten())
            .filter(|button| button.data.starts_with("historyolder:c:"))
            .collect();
        assert_eq!(
            older_buttons.len(),
            1,
            "reopen must keep one load-older button for the reused topic; sent: {sent:?}"
        );
    }

    #[tokio::test]
    async fn history_entry_replays_user_images_as_files_with_timestamps() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_image(&codex_home, "thread-image", "/tmp/project");

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );

        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let user = sent
            .iter()
            .find(|msg| msg.body.starts_with("👤 "))
            .expect(" marker");
        assert!(user.body.contains("look at this image"));
        assert!(user.body.contains("🕒 2026/4/25 00:01"));
        let user_id = channel
            .sent_records
            .lock()
            .unwrap()
            .iter()
            .find(|(_, msg)| msg.body.starts_with("👤 "))
            .map(|(id, _)| id.clone())
            .expect("user message id");
        drop(sent);

        let files = channel.sent_files.lock().unwrap();
        assert_eq!(files.len(), 1, "history image should be sent as a file");
        let (_, file) = &files[0];
        assert_eq!(file.filename, "history-image.png");
        assert_eq!(file.bytes, vec![1, 2, 3]);
        assert_eq!(
            file.reply_to.as_ref().map(|id| id.as_str()),
            Some(user_id.as_str())
        );
        assert!(file
            .caption
            .as_deref()
            .is_some_and(|caption| caption.contains("🕒 2026/4/25 00:01")));
    }

    #[tokio::test]
    async fn history_replay_retries_missing_local_image_without_duplicate_text_messages() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let image_path = tmp.path().join("history-local.png");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_local_image(
            &codex_home,
            "thread-local-image",
            "/tmp/project",
            &image_path,
        );

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );
        let session = WorkSession {
            workspace: WorkspaceId::new("history-image-retry"),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex image retry".into(),
            live: None,
            resume_ref: Some("thread-local-image".into()),
        };
        let handle = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("topic-image"));
        let entry = bot.core.history_entry_at(0).expect("history entry");

        bot.replay_history_batch(&session, &handle, &entry, None)
            .await
            .expect("first replay should skip missing local image");
        assert!(
            channel.sent_files.lock().unwrap().is_empty(),
            "missing local image should not produce a file upload"
        );

        fs::write(&image_path, [4, 5, 6]).expect("materialize local image");
        bot.replay_history_batch(&session, &handle, &entry, None)
            .await
            .expect("retry should send the now-available local image");

        let sent = channel.sent.lock().unwrap();
        assert_eq!(
            sent.iter()
                .filter(|msg| msg.body.starts_with("👤 "))
                .count(),
            1,
            "retry must not duplicate  text"
        );
        assert_eq!(
            sent.iter()
                .filter(|msg| msg.body.starts_with("local image seen"))
                .count(),
            1,
            "retry must not duplicate assistant history text"
        );
        drop(sent);

        let files = channel.sent_files.lock().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].1.filename, "history-image.png");
        assert_eq!(files[0].1.bytes, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn history_transcript_load_failure_is_visible_in_topic() {
        let tmp = TempDir::new().unwrap();
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));
        let session = WorkSession {
            workspace: WorkspaceId::new("history-missing"),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex missing".into(),
            live: None,
            resume_ref: Some("missing-history".into()),
        };
        let handle = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("topic-missing"));
        let entry = HistoryEntry {
            provider_id: "codex",
            session_id: "missing-history".into(),
            session_path: tmp.path().join("missing.jsonl"),
            cwd: Some("/tmp/project".into()),
            summary: "missing".into(),
            last_active_unix: 0,
            last_active_display: String::new(),
        };

        let err = bot
            .replay_history_batch(&session, &handle, &entry, None)
            .await
            .expect_err("missing transcript should fail");

        assert!(err.contains("failed to read history transcript"));
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.starts_with("history load failed:")),
            "history load failure should be visible in the topic: {sent:?}"
        );
    }

    #[tokio::test]
    async fn history_projection_retry_finishes_assistant_without_duplicate_user_marker() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_turns(&codex_home, "thread-history", "/tmp/project", 1);

        let channel = Arc::new(RecordingChannel::default());
        channel.send_results.lock().unwrap().extend([
            Ok(()),
            Err(ChannelError::Transport("assistant failed".into())),
        ]);
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, resumes),
        );
        let session = WorkSession {
            workspace: WorkspaceId::new("history-partial"),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex partial".into(),
            live: None,
            resume_ref: Some("thread-history".into()),
        };
        let handle = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("topic-partial"));
        let entry = bot.core.history_entry_at(0).expect("history entry");

        let err = bot
            .replay_history_batch(&session, &handle, &entry, None)
            .await
            .expect_err("assistant projection should fail once");
        assert!(err.contains("assistant failed"));
        {
            let sent = channel.sent.lock().unwrap();
            assert_eq!(
                sent.iter()
                    .filter(|msg| msg.body.starts_with("👤 "))
                    .count(),
                1
            );
            assert!(!sent.iter().any(|msg| msg.body.starts_with("assistant 1")));
        }

        bot.replay_history_batch(&session, &handle, &entry, None)
            .await
            .expect("retry should finish assistant side");

        let sent = channel.sent.lock().unwrap();
        assert_eq!(
            sent.iter()
                .filter(|msg| msg.body.starts_with("👤 "))
                .count(),
            1,
            "retry must not duplicate completed user marker: {sent:?}"
        );
        let assistant = sent
            .iter()
            .find(|msg| msg.body.starts_with("assistant 1"))
            .expect("assistant retry message");
        assert_eq!(
            assistant.reply_to.as_ref().map(|id| id.as_str()),
            Some("sent-1")
        );
    }

    #[tokio::test]
    async fn load_older_history_reloads_full_window_without_stale_history_messages() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_turns(&codex_home, "thread-history", "/tmp/project", 12);

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );

        Arc::clone(&bot).open_history_entry(0).await.unwrap();
        let topic = bot
            .state
            .topic_for_workspace(&workspace_id_for_resume("codex", "thread-history"))
            .expect("topic binding");
        let button = {
            let sent = channel.sent.lock().unwrap();
            sent.iter()
                .flat_map(|msg| msg.buttons.iter().flatten())
                .find(|button| button.data.starts_with("historyolder:c:"))
                .expect("load older button")
                .data
                .clone()
        };
        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: Some(topic),
            user: "u".into(),
            data: button,
            source_message: MessageId::new("older"),
        })
        .await
        .unwrap();

        let deleted = channel.deleted_messages.lock().unwrap();
        assert!(
            !deleted.is_empty(),
            "load older should clear the previously replayed history messages"
        );
        let records = channel.sent_records.lock().unwrap();
        let active_user_markers = records
            .iter()
            .filter(|(id, msg)| {
                msg.body.starts_with("👤 ") && !deleted.iter().any(|deleted| deleted == id)
            })
            .map(|(_, msg)| msg.body.clone())
            .collect::<Vec<_>>();
        assert_eq!(active_user_markers.len(), 12);
        assert!(active_user_markers[0].contains("user 1"));
        assert!(active_user_markers[11].contains("user 12"));
        assert!(records.iter().any(|(id, msg)| {
            !deleted.iter().any(|deleted| deleted == id)
                && msg.body.starts_with("assistant 1")
                && msg.reply_to.is_some()
        }));
        assert!(records.iter().any(|(id, msg)| {
            !deleted.iter().any(|deleted| deleted == id)
                && msg.body.starts_with("assistant 2")
                && msg.reply_to.is_some()
        }));
        let active_older_controls = records
            .iter()
            .filter(|(id, msg)| {
                !deleted.iter().any(|deleted| deleted == id)
                    && msg
                        .buttons
                        .iter()
                        .flatten()
                        .any(|button| button.data.starts_with("historyolder:c:"))
            })
            .count();
        assert_eq!(
            active_older_controls, 0,
            "all history is loaded, so the older control should be removed"
        );
    }

    #[tokio::test]
    async fn live_e2e_history_button_replays_and_loads_older() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session_with_turns(&codex_home, "thread-history", "/tmp/project", 12);

        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );

        Arc::clone(&bot)
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "u".into(),
                data: "history:0".into(),
                source_message: MessageId::new("entry-panel"),
            })
            .await
            .unwrap();

        let topic = bot
            .state
            .topic_for_workspace(&workspace_id_for_resume("codex", "thread-history"))
            .expect("topic binding");
        let button = {
            let sent = channel.sent.lock().unwrap();
            sent.iter()
                .flat_map(|msg| msg.buttons.iter().flatten())
                .find(|button| button.data.starts_with("historyolder:c:"))
                .expect("load older button")
                .data
                .clone()
        };
        Arc::clone(&bot)
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(topic),
                user: "u".into(),
                data: button,
                source_message: MessageId::new("older"),
            })
            .await
            .unwrap();

        assert_eq!(resumes.lock().unwrap().as_slice(), ["thread-history"]);
        let deleted = channel.deleted_messages.lock().unwrap();
        let records = channel.sent_records.lock().unwrap();
        let user_marker_count = records
            .iter()
            .filter(|(id, msg)| {
                msg.body.starts_with("👤 ") && !deleted.iter().any(|deleted| deleted == id)
            })
            .count();
        assert_eq!(
            user_marker_count, 12,
            "history replay should cover all 12 turns after older"
        );

        let user_three_id = records
            .iter()
            .find(|(id, msg)| {
                msg.body.starts_with("👤 ")
                    && msg.body.contains("user 3")
                    && !deleted.iter().any(|deleted| deleted == id)
            })
            .map(|(id, _)| id.clone())
            .expect("user 3 marker id");
        let assistant_three = records
            .iter()
            .find(|(id, msg)| {
                msg.body.starts_with("assistant 3") && !deleted.iter().any(|deleted| deleted == id)
            })
            .map(|(_, msg)| msg)
            .expect("assistant 3 reply");
        assert_eq!(
            assistant_three.reply_to.as_ref().map(|id| id.as_str()),
            Some(user_three_id.as_str())
        );
        let user_one_id = records
            .iter()
            .find(|(id, msg)| {
                msg.body.starts_with("👤 ")
                    && msg.body.contains("user 1")
                    && !deleted.iter().any(|deleted| deleted == id)
            })
            .map(|(id, _)| id.clone())
            .expect("user 1 marker id");
        let assistant_one = records
            .iter()
            .find(|(id, msg)| {
                msg.body.starts_with("assistant 1") && !deleted.iter().any(|deleted| deleted == id)
            })
            .map(|(_, msg)| msg)
            .expect("assistant 1 reply");
        assert_eq!(
            assistant_one.reply_to.as_ref().map(|id| id.as_str()),
            Some(user_one_id.as_str())
        );
    }

    #[tokio::test]
    async fn history_entry_with_stale_workspace_creates_replacement_topic() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session(&codex_home, "thread-existing", "/tmp/project", "resume me");

        let channel = Arc::new(RecordingChannel::default());
        channel.missing_workspaces.lock().unwrap().push("4".into());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs, Arc::clone(&resumes)),
        );
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("4"),
                chat: ChatId::new("200"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex old".into(),
                live: None,
                resume_ref: Some("thread-existing".into()),
            })
            .expect("insert stale workspace");

        Arc::clone(&bot).open_history_entry(0).await.unwrap();

        assert!(bot.state.get(&WorkspaceId::new("4")).is_some());
        assert!(
            channel.deleted_workspaces.lock().unwrap().is_empty(),
            "stale topic recovery should rely on the activation probe instead of deleting up front"
        );
        assert_eq!(
            channel.probed_workspaces.lock().unwrap().as_slice(),
            ["4"],
            "stale topic recovery should probe the prior topic before creating a replacement"
        );
        assert!(
            channel.renames.lock().unwrap().is_empty(),
            "stale history selection must not rename a Telegram topic to probe existence"
        );
        assert_eq!(
            channel.created_workspaces.lock().unwrap().as_slice(),
            [(
                "200".to_string(),
                "codex · project".to_string(),
                "1".to_string()
            )]
        );
        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["thread-existing"],
            "stale Telegram topic should keep the provider session binding"
        );
        assert_eq!(channel.sent_targets.lock().unwrap().as_slice(), ["1", "1"]);
        let sent = channel.sent.lock().unwrap();
        assert!(sent[0].body.contains("✓ resumed"));
        assert!(sent[0].body.contains("thread-existing"));
        assert!(sent
            .iter()
            .any(|msg| { msg.body.starts_with("👤 ") && msg.body.contains("resume me") }));
    }

    #[tokio::test]
    async fn agent_notification_recreates_stale_notification_topic() {
        let channel = Arc::new(RecordingChannel::default());
        channel
            .missing_workspaces
            .lock()
            .unwrap()
            .push("old-notifications".into());
        let bot = test_bot(Arc::clone(&channel));
        let old_handle =
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("old-notifications"));
        bot.state
            .set_notification_handle(&old_handle)
            .expect("seed stale notification handle");
        let session = WorkSession {
            workspace: WorkspaceId::new("codex:thread-existing"),
            chat: ChatId::new("200"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex old".into(),
            live: None,
            resume_ref: Some("thread-existing".into()),
        };
        bot.state
            .upsert_with_topic(session.clone(), WorkspaceId::new("workspace-topic"))
            .expect("insert session");

        bot.send_agent_notification(&session, "assistant reply")
            .await
            .expect("stale notification topic should be recreated");

        assert_eq!(
            channel.deleted_workspaces.lock().unwrap().as_slice(),
            ["old-notifications"]
        );
        assert_eq!(
            channel.created_workspaces.lock().unwrap().as_slice(),
            [(
                "100".to_string(),
                "agent notifications".to_string(),
                "1".to_string()
            )]
        );
        assert_eq!(channel.sent_targets.lock().unwrap().as_slice(), ["1"]);
        let current = bot
            .state
            .notification_handle()
            .expect("notification handle should be refreshed");
        assert_eq!(current.chat.as_str(), "100");
        assert_eq!(current.workspace.as_str(), "1");
    }

    #[tokio::test]
    async fn history_watch_message_for_live_unbound_workspace_uses_agent_notification() {
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_provider_id(
            "pi",
            Arc::new(StdMutex::new(Vec::new())),
            Arc::new(StdMutex::new(Vec::new())),
            test_command_catalog(),
        )));
        let core = core_with_runtime(runtime);
        let bot = Arc::new(Bot::new(
            Arc::clone(&channel) as Arc<dyn Channel>,
            Arc::clone(&core),
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ));
        let workspace = WorkspaceId::new("pi:resume:thread-live");
        core.upsert_workspace_binding(
            ControlWorkspaceId::new(workspace.as_str()),
            OpenWorkspaceRequest {
                provider_id: "pi",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "pi live".into(),
            },
            Some("thread-live"),
        )
        .expect("persist unbound workspace");
        bot.state
            .hydrate_unbound_control_workspaces(&ChatId::new("100"))
            .expect("hydrate unbound workspace");
        assert!(
            bot.state.topic_for_workspace(&workspace).is_none(),
            "regression setup requires no Telegram topic binding"
        );
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        let live = Arc::new(LiveSession {
            session: Arc::new(ClosedSession::with_ids("thread-live", "instance-live")),
            events: AsyncMutex::new(CoreWorkspaceEventStream::new(
                ControlWorkspaceId::new(workspace.as_str()),
                rx,
            )),
            pending_intv: StdMutex::new(HashMap::new()),
        });
        bot.state
            .bind_live(&workspace, live, Some("thread-live".into()))
            .expect("bind live without topic");

        Arc::clone(&bot)
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: ControlWorkspaceId::new(workspace.as_str()),
                turn_id: None,
                event: AgentEvent::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text: "assistant reply".into(),
                    streaming: false,
                }),
            })
            .await
            .expect("history-watch message should use notification topic");

        assert_eq!(
            channel.created_workspaces.lock().unwrap().as_slice(),
            [(
                "100".to_string(),
                "agent notifications".to_string(),
                "1".to_string()
            )]
        );
        assert_eq!(channel.sent_targets.lock().unwrap().as_slice(), ["1"]);
        let sent = channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body.contains("assistant reply"));
        assert!(sent[0].body.contains("session: `thread-live`"));
    }

    #[tokio::test]
    async fn late_workspace_turn_echo_after_live_clear_does_not_enter_notifications() {
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(Arc::new(StdMutex::new(
            Vec::new(),
        )))));
        let core = core_with_runtime(runtime);
        let bot = Arc::new(Bot::new(
            Arc::clone(&channel) as Arc<dyn Channel>,
            Arc::clone(&core),
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ));
        let workspace = WorkspaceId::new("workspace-a");
        let session = WorkSession {
            workspace: workspace.clone(),
            chat: ChatId::new("100"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/workspace-a")),
            title: "codex workspace-a".into(),
            live: None,
            resume_ref: Some("thread-1".into()),
        };
        bot.state
            .upsert_with_topic(session, WorkspaceId::new("9"))
            .expect("bind workspace topic");
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        let live = Arc::new(LiveSession {
            session: Arc::new(ClosedSession::with_ids("thread-1", "instance-1")),
            events: AsyncMutex::new(CoreWorkspaceEventStream::new(
                ControlWorkspaceId::new(workspace.as_str()),
                rx,
            )),
            pending_intv: StdMutex::new(HashMap::new()),
        });
        bot.state
            .bind_live(&workspace, live, Some("thread-1".into()))
            .expect("bind active live session");

        let guard = DirectNotificationGuard::new(Arc::clone(&core), workspace.clone());
        drop(guard);
        bot.state
            .mark_live_dead(&workspace, Some("thread-1".into()))
            .expect("live cleared before delayed core event is handled");

        Arc::clone(&bot)
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: ControlWorkspaceId::new(workspace.as_str()),
                turn_id: None,
                event: AgentEvent::Message(MessageEvent {
                    role: MessageRole::Assistant,
                    text: "late workspace answer".into(),
                    streaming: false,
                }),
            })
            .await
            .expect("late direct echo should be suppressed");

        assert!(
            channel.created_workspaces.lock().unwrap().is_empty(),
            "late workspace turn echo must not create the notification topic"
        );
        assert!(
            channel.sent.lock().unwrap().is_empty(),
            "late workspace turn echo must not be delivered as a notification"
        );
    }

    #[tokio::test]
    async fn startup_cleanup_keeps_notification_binding_hydratable_after_restart() {
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new(Arc::new(StdMutex::new(
            Vec::new(),
        )))));
        let core = core_with_runtime(runtime);
        let entry = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new(""));
        let state = BotState::new_with_core(Arc::clone(&core));
        let bot = Arc::new(Bot::new_with_state_and_history_watch(
            channel.clone(),
            Arc::clone(&core),
            entry.clone(),
            state,
            false,
        ));
        let notification = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("77"));
        bot.state
            .set_notification_handle(&notification)
            .expect("seed notification topic");

        let restarted_state = BotState::new_with_core(Arc::clone(&core));
        let restarted_bot = Arc::new(Bot::new_with_state_and_history_watch(
            channel.clone(),
            Arc::clone(&core),
            entry,
            restarted_state,
            false,
        ));
        let hydrated = restarted_bot
            .state
            .notification_handle()
            .expect("notification topic should hydrate after restart");
        assert_eq!(hydrated.chat.as_str(), "100");
        assert_eq!(hydrated.workspace.as_str(), "77");

        let ensured = restarted_bot
            .ensure_notification_handle()
            .await
            .expect("existing notification topic should be usable");
        assert_eq!(ensured.workspace.as_str(), "77");
        assert_eq!(channel.probed_workspaces.lock().unwrap().as_slice(), ["77"]);
        assert!(
            channel.created_workspaces.lock().unwrap().is_empty(),
            "startup restart must not create a duplicate notification topic"
        );
    }

    #[tokio::test]
    async fn replacing_missing_notification_topic_probes_newer_record_before_reusing() {
        let channel = Arc::new(RecordingChannel::default());
        channel
            .missing_workspaces
            .lock()
            .unwrap()
            .extend(["old-notifications".into(), "newer-stale".into()]);
        let bot = test_bot(Arc::clone(&channel));
        let missing =
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("old-notifications"));
        let newer_stale = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("newer-stale"));
        bot.state
            .set_notification_handle(&newer_stale)
            .expect("seed newer stale notification topic");

        let ensured = bot
            .replace_missing_notification_handle(&missing)
            .await
            .expect("newer stale notification topic should be recreated");

        assert_eq!(
            channel.probed_workspaces.lock().unwrap().as_slice(),
            ["newer-stale"]
        );
        assert_eq!(
            channel.deleted_workspaces.lock().unwrap().as_slice(),
            ["newer-stale"]
        );
        assert_eq!(ensured.workspace.as_str(), "1");
        assert_eq!(
            channel.created_workspaces.lock().unwrap().as_slice(),
            [(
                "100".to_string(),
                "agent notifications".to_string(),
                "1".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn agent_notification_binds_every_split_message_id() {
        let channel = Arc::new(RecordingChannel::default());
        channel.split_next_send_as(&["split-1", "split-2", "split-3"]);
        let bot = test_bot(Arc::clone(&channel));
        let session = WorkSession {
            workspace: WorkspaceId::new("codex:thread-split"),
            chat: ChatId::new("200"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex split".into(),
            live: None,
            resume_ref: Some("thread-split".into()),
        };
        bot.state
            .upsert_with_topic(session.clone(), WorkspaceId::new("workspace-topic"))
            .expect("insert session");

        bot.send_agent_notification(&session, "assistant reply")
            .await
            .expect("send split notification");

        let sent = channel.sent.lock().unwrap();
        assert!(sent.iter().all(|msg| msg.body.contains("\n\n---\n\n")
            && msg.body.contains("session: `thread-split`")
            && msg.body.contains("cwd: `/tmp/project`")));
        drop(sent);

        let provider_session_id = bot
            .state
            .active_provider_session_id(&session.workspace)
            .expect("provider session id");
        let notification_chat = ChatId::new("100");
        for id in ["split-1", "split-2", "split-3"] {
            assert_eq!(
                bot.state.resolve_message_session_binding(
                    channel.name(),
                    &notification_chat,
                    &MessageId::new(id),
                ),
                Some(provider_session_id.clone()),
                "split notification chunk {id} must route replies"
            );
        }
    }

    #[tokio::test]
    async fn notification_follow_up_reply_can_target_turn_output() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let runtime = runtime_with_recorder(Arc::clone(&inputs));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        let session = WorkSession {
            workspace: WorkspaceId::new("codex:thread-follow-up"),
            chat: ChatId::new("200"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex follow-up".into(),
            live: None,
            resume_ref: Some("thread-follow-up".into()),
        };
        bot.state
            .upsert_with_topic(session.clone(), WorkspaceId::new("workspace-topic"))
            .expect("insert session");

        bot.send_agent_notification(&session, "assistant reply")
            .await
            .expect("send notification");

        let notification_topic = bot.state.notification_handle().expect("notification topic");
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("user-1"),
                chat: notification_topic.chat.clone(),
                workspace: Some(notification_topic.workspace.clone()),
                reply_to: Some(MessageId::new("sent-1")),
                user: "alice".into(),
                text: Some("first follow-up".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect("first notification reply");

        let provider_session_id = bot
            .state
            .active_provider_session_id(&session.workspace)
            .expect("provider session id");
        assert_eq!(
            bot.state.resolve_message_session_binding(
                channel.name(),
                &notification_topic.chat,
                &MessageId::new("sent-2"),
            ),
            Some(provider_session_id.clone()),
            "completed status message must route replies"
        );
        assert_eq!(
            bot.state.resolve_message_session_binding(
                channel.name(),
                &notification_topic.chat,
                &MessageId::new("sent-3"),
            ),
            None,
            "deleted silent preview should not route replies"
        );
        assert_eq!(
            bot.state.resolve_message_session_binding(
                channel.name(),
                &notification_topic.chat,
                &MessageId::new("sent-4"),
            ),
            Some(provider_session_id),
            "final assistant message must route replies"
        );

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("user-2"),
                chat: notification_topic.chat.clone(),
                workspace: Some(notification_topic.workspace.clone()),
                reply_to: Some(MessageId::new("sent-4")),
                user: "alice".into(),
                text: Some("second follow-up".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect("second notification reply");

        let inputs = inputs.lock().unwrap();
        let submitted = inputs
            .iter()
            .map(|input| input.text.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(submitted, ["first follow-up", "second follow-up"]);
        assert!(
            channel
                .sent
                .lock()
                .unwrap()
                .iter()
                .all(|msg| !msg.body.contains("no longer routable")),
            "notification follow-up should not lose routing"
        );
    }

    #[tokio::test]
    async fn notification_reply_after_restart_hydrates_workspace_session() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let runtime = runtime_with_recorder(Arc::clone(&inputs));
        let core = core_with_runtime(runtime);
        let entry = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new(""));
        let state = BotState::new_with_core(Arc::clone(&core));
        let notification = WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("77"));
        state
            .set_notification_handle(&notification)
            .expect("persist notification topic");
        let session = WorkSession {
            workspace: WorkspaceId::new("codex:thread-restart"),
            chat: ChatId::new("200"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex restart".into(),
            live: None,
            resume_ref: Some("thread-restart".into()),
        };
        state
            .upsert_with_topic(session.clone(), WorkspaceId::new("workspace-topic"))
            .expect("persist workspace session");
        let provider_session_id = state
            .active_provider_session_id(&session.workspace)
            .expect("provider session id");
        state
            .register_message_session_binding(
                channel.name(),
                &notification.chat,
                &MessageId::new("sent-before-restart"),
                provider_session_id,
            )
            .expect("persist notification routing");

        let restarted_state = BotState::new_with_core(Arc::clone(&core));
        let restarted_bot = Arc::new(Bot::new_with_state_and_history_watch(
            Arc::clone(&channel) as Arc<dyn Channel>,
            Arc::clone(&core),
            entry,
            Arc::clone(&restarted_state),
            false,
        ));
        assert!(
            restarted_bot.state.get(&session.workspace).is_none(),
            "restart begins without workspace sessions in memory"
        );

        restarted_bot
            .clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("user-after-restart"),
                chat: notification.chat.clone(),
                workspace: Some(notification.workspace.clone()),
                reply_to: Some(MessageId::new("sent-before-restart")),
                user: "alice".into(),
                text: Some("continue after restart".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect("notification reply after restart");

        assert!(
            restarted_bot.state.get(&session.workspace).is_some(),
            "notification reply should hydrate the workspace session"
        );
        let submitted = inputs
            .lock()
            .unwrap()
            .iter()
            .map(|input| input.text.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(submitted, ["continue after restart"]);
        assert!(
            channel
                .sent
                .lock()
                .unwrap()
                .iter()
                .all(|msg| !msg.body.contains("no longer bound")),
            "hydrated notification reply must not report a missing binding"
        );
    }

    #[tokio::test]
    async fn workspace_command_outside_workspaces_view_is_rejected() {
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));
        let workspace = WorkspaceId::new("9");
        bot.state.set_panel(PanelSnapshot {
            view: PanelView::Overview,
            provider_filter: None,
            workspace_filter: None,
            agents: Vec::new(),
            history: Vec::new(),
            workspaces: vec![workspace],
            history_workspaces: Vec::new(),
            history_offset: 0,
            history_total: 0,
        });

        Arc::clone(&bot)
            .dispatch_entry_command(EntryAction::Workspace(1))
            .await
            .expect("overview /wN should render a visible hint");

        let sent = channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body.contains("Workspaces view"));
    }

    #[tokio::test]
    async fn overview_panel_shows_attached_session_without_workspace_snapshot_entry() {
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));
        let workspace = WorkspaceId::new("codex:live");
        bot.state
            .upsert_with_topic(
                WorkSession {
                    workspace: workspace.clone(),
                    chat: ChatId::new("200"),
                    provider_id: "codex",
                    project_path: Some(PathBuf::from(
                        "/Volumes/Data/opensource/conductor/lucarnex",
                    )),
                    title: "lucarnex".into(),
                    live: None,
                    resume_ref: None,
                },
                workspace.clone(),
            )
            .expect("insert session");
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        let live = Arc::new(LiveSession {
            session: Arc::new(ClosedSession::with_ids("thread-live", "instance-live")),
            events: AsyncMutex::new(CoreWorkspaceEventStream::new(
                ControlWorkspaceId::new(workspace.as_str()),
                rx,
            )),
            pending_intv: StdMutex::new(HashMap::new()),
        });
        bot.state
            .bind_live(&workspace, live, Some("thread-live".into()))
            .expect("bind live");

        Arc::clone(&bot).render_entry_panel().await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let panel = sent.last().expect("panel message");
        assert!(panel.body.contains("attached sessions"));
        assert!(panel.body.contains("lucarnex — codex"));
        assert!(!panel.body.contains("open workspaces"));
        assert!(bot.state.panel().workspaces.is_empty());
    }

    #[tokio::test]
    async fn topic_message_lazily_hydrates_persisted_binding_without_startup_probe() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let runtime = runtime_with_recorder(Arc::clone(&inputs));
        let core = core_with_runtime(runtime);
        let bot = Arc::new(Bot::new(
            Arc::clone(&channel) as Arc<dyn Channel>,
            Arc::clone(&core),
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
        ));
        let workspace = WorkspaceId::new("codex:thread-existing");
        core.upsert_workspace_binding(
            ControlWorkspaceId::new(workspace.as_str()),
            OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex old".into(),
            },
            Some("thread-existing"),
        )
        .expect("persist workspace");
        core.upsert_channel_binding(ChannelBinding::new(
            ChannelBindingId::new("telegram:200:9"),
            ControlWorkspaceId::new(workspace.as_str()),
            "telegram",
            "200",
            Some("9"),
        ))
        .expect("persist topic binding");
        assert!(
            bot.state.get(&workspace).is_none(),
            "fresh Telegram state starts without hydrating old workspaces"
        );

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("lazy-topic-message"),
                chat: ChatId::new("200"),
                workspace: Some(WorkspaceId::new("9")),
                reply_to: None,
                user: "u".into(),
                text: Some("hello after restart".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(
            bot.state.get(&workspace).is_some(),
            "first topic message should hydrate the persisted workspace binding"
        );
        assert!(
            inputs
                .lock()
                .unwrap()
                .iter()
                .any(|input| input.text.as_str() == "hello after restart"),
            "lazy-hydrated workspace should receive the message"
        );
        assert!(
            channel.probed_workspaces.lock().unwrap().is_empty(),
            "lazy topic hydration should not probe all startup workspaces"
        );
        assert!(channel.created_workspaces.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn image_caption_message_submits_image_to_agent() {
        let channel = Arc::new(RecordingChannel::default());
        channel
            .downloads
            .lock()
            .unwrap()
            .insert("photo-ref".into(), vec![1, 2, 3]);
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let bot =
            test_bot_with_runtime(Arc::clone(&channel), runtime_with_recorder(inputs.clone()));
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("这是个啥".into()),
                attachments: vec![image_attachment("photo-ref")],
            }))
            .await
            .unwrap();

        let inputs = inputs.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].text.as_str(), "这是个啥");
        assert_eq!(inputs[0].images, vec![image_input("image/jpeg", "AQID")]);
        assert_eq!(channel.acks.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn image_only_message_is_attached_to_next_text_message() {
        let channel = Arc::new(RecordingChannel::default());
        channel
            .downloads
            .lock()
            .unwrap()
            .insert("photo-ref".into(), vec![1, 2, 3]);
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let bot =
            test_bot_with_runtime(Arc::clone(&channel), runtime_with_recorder(inputs.clone()));
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: None,
                attachments: vec![image_attachment("photo-ref")],
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("这是个啥".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let inputs = inputs.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].text.as_str(), "这是个啥");
        assert_eq!(inputs[0].images, vec![image_input("image/jpeg", "AQID")]);
    }

    #[tokio::test]
    async fn broken_pipe_reopens_session_and_retries_turn_once() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_recovering_provider(inputs.clone(), resumes.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("继续".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let inputs = inputs.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].text.as_str(), "继续");
        assert_eq!(resumes.lock().unwrap().as_slice(), ["session-1"]);
    }

    #[tokio::test]
    async fn rename_after_restart_uses_persisted_workspace_binding() {
        let tmp = TempDir::new().unwrap();
        let db = tmp.path().join("state.sqlite3");
        let workspace = WorkspaceId::new("2");
        let before_restart = BotState::open_sqlite(&db).expect("open state db");
        before_restart
            .upsert(WorkSession {
                workspace: workspace.clone(),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: None,
                title: "codex · new".into(),
                live: None,
                resume_ref: Some("thread-1".into()),
            })
            .expect("persist workspace");
        let after_restart = BotState::open_sqlite(&db).expect("reload state db");
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot_with_state(Arc::clone(&channel), after_restart.clone());

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(workspace.clone()),
                reply_to: None,
                user: "u".into(),
                text: Some("/rename xx".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert_eq!(
            channel.renames.lock().unwrap().as_slice(),
            [("2".to_string(), "xx".to_string())]
        );
        assert_eq!(
            BotState::open_sqlite(&db)
                .expect("reload state db")
                .get(&workspace)
                .expect("workspace")
                .title,
            "xx"
        );
        assert!(
            !channel
                .sent
                .lock()
                .unwrap()
                .iter()
                .any(|msg| msg.body.contains("isn't bound")),
            "rename should not emit stale unbound-workspace message"
        );
    }

    #[tokio::test]
    async fn idle_recycled_session_sends_notice_and_retries_turn_once() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_idle_recycled_recovering_provider(inputs.clone(), resumes.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("继续".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let inputs = inputs.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].text.as_str(), "继续");
        assert_eq!(resumes.lock().unwrap().as_slice(), ["session-1"]);
        assert!(
            channel.sent.lock().unwrap().iter().any(|msg| {
                msg.body.contains("idle-recycled") && msg.body.contains("resuming session")
            }),
            "expected idle recycle recovery notice"
        );
    }

    #[tokio::test]
    async fn topic_slash_command_invokes_bound_agent_command() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/skills".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "skills");
        assert_eq!(invocations[0].args.as_deref(), None);
        let sent = channel.sent.lock().unwrap();
        let skills = sent
            .iter()
            .find(|msg| msg.body == "skills\n\n- `frontend-design`\n")
            .expect("skills list message");
        assert!(!skills.body.contains("Frontend Design"));
        assert!(!skills.body.contains("Build production-grade interfaces."));
        assert_eq!(skills.reply_to.as_ref().map(|id| id.as_str()), Some("10"));
    }

    #[tokio::test]
    async fn skills_topic_command_uses_typed_list_even_when_catalog_marks_native_idle() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        let skills = catalog
            .commands
            .iter_mut()
            .find(|command| command.name.as_str() == "skills")
            .expect("skills command");
        skills.source = AgentCommandSource::ProviderNative;
        skills.completion = AgentCommandCompletion::ProviderIdle;
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/skills".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "skills");
        assert_eq!(
            invocations[0].source,
            AgentCommandSource::AdapterMapped,
            "/skills must use typed skills/list data instead of provider slash text"
        );
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        let skills = sent
            .iter()
            .find(|msg| msg.body == "skills\n\n- `frontend-design`\n")
            .expect("skills list message");
        assert!(!skills.body.contains("Frontend Design"));
        assert!(!skills.body.contains("Build production-grade interfaces."));
    }

    #[tokio::test]
    async fn status_topic_command_invokes_bound_agent_status_and_merges_process_resources() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let process_id = std::process::id();
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/status".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "status");
        assert_eq!(invocations[0].args.as_deref(), None);
        let sent = channel.sent.lock().unwrap();
        let status = sent
            .iter()
            .find(|msg| msg.body.contains("status") && msg.body.contains("gpt-5.5"))
            .expect("status message");
        assert!(status
            .body
            .contains("Model: `gpt-5.5` (`gpt-5.5-2026-04-01`, reasoning medium)"));
        assert!(status.body.contains("Directory: `/tmp/project`"));
        assert!(status.body.contains("Permissions: `Default`"));
        assert!(status.body.contains("Session: `session-test`"));
        assert!(status.body.contains("🧮 Token usage: 1.9k in / 387 out"));
        assert!(status.body.contains("📚 Context: 14k/205k (7%)"));
        assert!(
            status
                .body
                .contains(&format!("Process identity: `session-test:{process_id}`")),
            "{}",
            status.body
        );
        assert!(status.body.contains(&format!("PID: `{process_id}`")));
        assert!(status.body.contains("Processes: `"));
        assert!(status.body.contains("CPU: `"));
        assert!(status.body.contains("Memory: `"));
        assert!(status.body.contains("Workspace: `2`"));
        assert!(status.body.contains("Live: `running`"));
        assert!(status.body.contains("Channel: `telegram:100:2`"));
        assert!(!sent.iter().any(|msg| msg.body.contains("provider: codex")));
        drop(sent);

        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 1);
        assert_eq!(
            workflows[0].completion_policy,
            CommandCompletionPolicy::CommandResult
        );
        assert_eq!(workflows[0].state, CommandState::Completed);
        let result = workflows[0]
            .result
            .as_ref()
            .expect("command result payload");
        assert_eq!(result["command"], "status");
        assert_eq!(result["result"]["type"], "status");
        assert_eq!(
            result["result"]["payload"]["model"],
            serde_json::Value::String("gpt-5.5".into())
        );

        let snapshot = bot
            .state
            .status_snapshot(&WorkspaceId::new("2"))
            .expect("status snapshot");
        assert_eq!(
            snapshot
                .provider_status
                .as_ref()
                .and_then(|status| status.model.as_deref()),
            Some("gpt-5.5")
        );
        assert_eq!(snapshot.model.as_deref(), Some("gpt-5.5"));
        assert_eq!(snapshot.reasoning.as_deref(), Some("medium"));
        assert_eq!(snapshot.permission_mode.as_deref(), Some("Default"));
    }

    #[tokio::test]
    async fn status_topic_command_does_not_use_provider_native_usage_alias() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_catalog(
            inputs.clone(),
            invocations.clone(),
            AgentCommandCatalog {
                commands: vec![
                    AgentCommand {
                        name: "usage".into(),
                        description: Some("Show session stats".into()),
                        aliases: Vec::new(),
                        source: AgentCommandSource::ProviderNative,
                        input: AgentCommandInput::None,
                        completion: AgentCommandCompletion::ProviderIdle,
                    },
                    AgentCommand {
                        name: "status".into(),
                        description: Some("Read status".into()),
                        aliases: Vec::new(),
                        source: AgentCommandSource::AdapterMapped,
                        input: AgentCommandInput::None,
                        completion: AgentCommandCompletion::CommandResult,
                    },
                ],
                complete: true,
                revision: 1,
            },
        )));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/status".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "status");
        let sent = channel.sent.lock().unwrap();
        assert!(sent
            .iter()
            .any(|msg| msg.body.contains("🧮 Token usage: 1.9k in / 387 out")));
        assert!(sent
            .iter()
            .any(|msg| msg.body.contains("Process identity: `session-test:")));
        assert!(!sent.iter().any(|msg| msg.body.contains("provider: codex")));
        assert!(!sent
            .iter()
            .any(|msg| msg.body.contains("agent usage output")));
    }

    #[tokio::test]
    async fn top_level_model_command_invokes_bound_agent_command() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model gpt-5.5 medium".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
        assert_eq!(invocations[0].args.as_deref(), Some("gpt-5.5 medium"));
        assert_eq!(
            invocations[0].source,
            AgentCommandSource::AdapterMapped,
            "/model <model> must use the typed session API"
        );
    }

    #[tokio::test]
    async fn command_result_policy_rejects_turn_completed_without_command_result() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_catalog(
            inputs,
            invocations,
            AgentCommandCatalog {
                commands: vec![AgentCommand {
                    name: "strict".into(),
                    description: Some("Must return a structured command result".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::CommandResult,
                }],
                complete: true,
                revision: 1,
            },
        )));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        let err = bot
            .clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/strict".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect_err("turn_completed must not complete a CommandResult command");

        assert!(err.contains("CommandCompletionPolicyMismatch"), "{err}");
        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 1);
        assert_eq!(workflows[0].state, CommandState::Failed);
        assert_eq!(
            workflows[0].completion_policy,
            CommandCompletionPolicy::CommandResult
        );
    }

    #[tokio::test]
    async fn immediate_structured_command_result_is_rendered_without_assistant_message() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_catalog(
            inputs,
            invocations,
            AgentCommandCatalog {
                commands: vec![AgentCommand {
                    name: "structured".into(),
                    description: Some("Returns a typed immediate command result".into()),
                    aliases: Vec::new(),
                    source: AgentCommandSource::AdapterMapped,
                    input: AgentCommandInput::None,
                    completion: AgentCommandCompletion::CommandResult,
                }],
                complete: true,
                revision: 1,
            },
        )));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/structured".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect("structured command should complete");

        let bodies = live_message_bodies(&channel);
        assert!(
            bodies.iter().any(|body| body.contains("structured ok")),
            "immediate command result was not rendered: {bodies:?}"
        );
    }

    #[tokio::test]
    async fn new_command_keeps_provider_native_new_session_ref() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/new".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let session = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains bound");
        assert!(session.live.is_some());
        assert_eq!(session.resume_ref.as_deref(), Some("session-new"));
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "new");
        assert_eq!(
            invocations[0].source,
            AgentCommandSource::AdapterMapped,
            "/new must use the typed session API"
        );
    }

    #[tokio::test]
    async fn quit_command_marks_live_dead_and_preserves_resume_ref() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/quit".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let session = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains bound");
        assert!(session.live.is_none());
        assert_eq!(session.resume_ref.as_deref(), Some("session-test"));
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "quit");
        assert_eq!(
            invocations[0].source,
            AgentCommandSource::AdapterMapped,
            "/quit must use the typed session API"
        );
    }

    #[tokio::test]
    async fn new_then_quit_then_next_turn_resumes_the_new_provider_ref() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_lifecycle_command_recorder(
                inputs.clone(),
                invocations.clone(),
                opens.clone(),
                resumes.clone(),
            ),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/new".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        let after_new = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains after /new");
        assert!(after_new.live.is_some());
        assert_eq!(after_new.resume_ref.as_deref(), Some("session-new"));

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/quit".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        let after_quit = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains after /quit");
        assert!(after_quit.live.is_none());
        assert_eq!(after_quit.resume_ref.as_deref(), Some("session-new"));

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("12"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("after lifecycle commands".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert_eq!(opens.load(AtomicOrdering::SeqCst), 1);
        assert_eq!(resumes.lock().unwrap().as_slice(), ["session-new"]);
        let inputs = inputs.lock().unwrap();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].text.as_str(), "after lifecycle commands");
        let final_state = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains after resume turn");
        assert!(final_state.live.is_some());
        assert_eq!(final_state.resume_ref.as_deref(), Some("session-new"));
    }

    #[tokio::test]
    async fn commands_topic_command_lists_bound_agent_catalog() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 1);
        assert_eq!(workflows[0].name.as_str(), "commands");
        assert_eq!(workflows[0].state, CommandState::Completed);
        let sent = channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body.contains("agent commands"));
        assert!(sent[0].body.contains("1. `/model`"));
        assert!(sent[0].body.contains("`/permissions`"));
        assert!(sent[0].body.contains("`/skills`"));
        assert!(
            sent[0].buttons.is_empty(),
            "/commands must render as a text catalog without per-command buttons"
        );
        assert!(
            !sent[0].body.contains("`/commands`"),
            "/commands is a Telegram wrapper, not an injected agent command:\n{}",
            sent[0].body
        );
    }

    #[tokio::test]
    async fn commands_buttons_use_short_state_backed_tokens_for_long_args() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        let long_target = format!("msg_{}", "a".repeat(160));
        let data = agent_command_button_data_for_workspace(
            bot.state.as_ref(),
            &WorkspaceId::new("2"),
            7,
            "fork",
            &long_target,
        );

        assert!(
            data.len() <= 64,
            "Telegram callback_data must stay <= 64 bytes: {data}"
        );
        assert!(
            !data.contains(&long_target),
            "callback_data must not embed raw command args"
        );
        let AgentCommandButton::Token { token } =
            parse_agent_command_button(&data).expect("short command token");
        let callback = bot
            .state
            .resolve_command_callback(&token)
            .expect("state-backed callback payload");
        assert_eq!(callback.workspace, WorkspaceId::new("2"));
        assert_eq!(callback.catalog_revision, 7);
        assert_eq!(callback.name, "fork");
        assert_eq!(callback.args, long_target);
    }

    #[tokio::test]
    async fn intervention_buttons_use_short_live_bound_tokens_for_long_request_ids() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let long_req_id = format!("req_{}", "x".repeat(160));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new({
            let mut provider = RecordingProvider::new_with_submit_events(
                inputs.clone(),
                vec![Event::InterventionRequest(InterventionRequest::Approval(
                    ApprovalRequest {
                        req_id: long_req_id.clone().into(),
                        tool_name: "edit".into(),
                        message: Some("needs approval".into()),
                        input: None,
                    },
                ))],
            );
            provider.catalog = test_command_catalog();
            provider
        }));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("please edit".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let data = {
            let sent = channel.sent.lock().unwrap();
            let approval = sent
                .iter()
                .find(|msg| msg.body.contains("needs approval"))
                .expect("approval prompt");
            let allow = approval
                .buttons
                .iter()
                .flatten()
                .find(|button| button.label.contains("Allow"))
                .expect("allow button");
            assert!(
                allow.data.len() <= 64,
                "intervention callback_data must fit Telegram limit: {}",
                allow.data
            );
            assert!(
                !allow.data.contains(&long_req_id),
                "callback_data must not embed provider request ids"
            );
            allow.data.clone()
        };

        let turn::IntvCallback::Token { token } =
            turn::parse_intv_callback(&data).expect("intervention token");
        assert!(
            bot.state.resolve_intervention_callback(&token).is_none(),
            "completed turns must expire permission buttons instead of leaving stale callbacks"
        );
    }

    #[tokio::test]
    async fn stale_agent_command_button_is_rejected_after_topic_rebind() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);
        let skills_button =
            bot.state
                .register_command_callback(&WorkspaceId::new("2"), 1, "skills", "");
        bot.state
            .upsert_with_topic(
                WorkSession {
                    workspace: WorkspaceId::new("other"),
                    chat: ChatId::new("100"),
                    provider_id: "codex",
                    project_path: Some(PathBuf::from("/tmp/other")),
                    title: "other".into(),
                    live: None,
                    resume_ref: Some("other-session".into()),
                },
                WorkspaceId::new("2"),
            )
            .expect("rebind topic");

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: skills_button,
                source_message: MessageId::new("42"),
            })
            .await
            .unwrap();

        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("Stale command button")),
            "{sent:?}"
        );
    }

    #[tokio::test]
    async fn stale_agent_command_button_is_rejected_after_catalog_revision_changes() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);
        let stale_button =
            bot.state
                .register_command_callback(&WorkspaceId::new("2"), 0, "skills", "");

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: stale_button,
                source_message: MessageId::new("42"),
            })
            .await
            .unwrap();

        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("Stale command button")),
            "{sent:?}"
        );
    }

    #[tokio::test]
    async fn commands_topic_command_does_not_inject_missing_core_commands() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs.clone(),
                invocations.clone(),
                AgentCommandCatalog {
                    commands: vec![AgentCommand {
                        name: "usage".into(),
                        description: Some("Show usage".into()),
                        aliases: vec!["cost".into()],
                        source: AgentCommandSource::ProviderNative,
                        input: AgentCommandInput::None,
                        completion: AgentCommandCompletion::ProviderIdle,
                    }],
                    complete: true,
                    revision: 1,
                },
            ),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].body.contains("`/usage`"));
        assert!(
            sent[0].buttons.is_empty(),
            "/commands must render provider catalogs as text without buttons"
        );
        for command in [
            "/model",
            "/permissions",
            "/skills",
            "/status",
            "/new",
            "/quit",
            "/fork",
        ] {
            assert!(
                !sent[0].body.contains(&format!("`{command}`")),
                "Telegram must not inject {command} into provider command catalog:\n{}",
                sent[0].body
            );
        }
    }

    #[tokio::test]
    async fn commands_topic_command_invokes_nested_provider_native_command_without_completion() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "native-report".into(),
            description: Some("Show a provider-native report".into()),
            aliases: vec!["report".into()],
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::None,
            completion: AgentCommandCompletion::ProviderIdle,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands report".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "report");
        assert_eq!(invocations[0].args.as_deref(), None);
        drop(invocations);

        let sent = channel.sent.lock().unwrap();
        assert!(sent.iter().any(|msg| msg.body == "agent report output"));
        drop(sent);

        let edits = channel.edits.lock().unwrap();
        assert!(edits.iter().any(|(_, msg)| msg.body.starts_with("✓ 完成")));
        assert!(!edits.iter().any(|(_, msg)| msg.body.starts_with("⚠ 失败")));
        drop(edits);

        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 1);
        assert_eq!(
            workflows[0].completion_policy,
            CommandCompletionPolicy::ProviderIdle
        );
        assert_eq!(workflows[0].state, CommandState::Completed);
    }

    #[tokio::test]
    async fn commands_topic_command_help_renders_provider_command_help_without_invoking_provider() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "goal".into(),
            description: Some("set, view, pause, resume, or clear the goal".into()),
            aliases: Vec::new(),
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::Text {
                label: "objective|pause|resume|clear".into(),
                required: false,
            },
            completion: AgentCommandCompletion::ProviderIdle,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands goal help".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let help = sent
            .iter()
            .find(|msg| msg.body.contains("`/goal`"))
            .expect("goal help message");
        assert!(help
            .body
            .contains("usage: `/goal [objective|pause|resume|clear]`"));
        assert!(!help.body.contains("source:"));
        assert!(!help.body.contains("completion:"));
    }

    #[tokio::test]
    async fn direct_topic_command_help_renders_provider_command_help_without_invoking_provider() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "goal".into(),
            description: Some("set, view, pause, resume, or clear the goal".into()),
            aliases: Vec::new(),
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::Text {
                label: "objective|pause|resume|clear".into(),
                required: false,
            },
            completion: AgentCommandCompletion::ProviderIdle,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/goal help".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let help = sent
            .iter()
            .find(|msg| msg.body.contains("`/goal`"))
            .expect("goal help message");
        assert!(help
            .body
            .contains("usage: `/goal [objective|pause|resume|clear]`"));
        assert!(!help.body.contains("source:"));
    }

    #[tokio::test]
    async fn command_help_uses_canonical_provider_command_for_aliases() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "native-report".into(),
            description: Some("Show provider report".into()),
            aliases: vec!["report".into()],
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::None,
            completion: AgentCommandCompletion::ProviderIdle,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands report help".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let help = sent
            .iter()
            .find(|msg| msg.body.contains("`/native-report`"))
            .expect("native-report help message");
        assert!(help.body.contains("usage: `/native-report`"));
        assert!(help.body.contains("aliases: `/report`"));
        assert!(!help.body.contains("source:"));
    }

    #[tokio::test]
    async fn command_help_covers_provider_command_named_help() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "help".into(),
            description: Some("Show provider help".into()),
            aliases: Vec::new(),
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::None,
            completion: AgentCommandCompletion::ProviderIdle,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands help help".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let help = sent
            .iter()
            .find(|msg| msg.body.contains("`/help`"))
            .expect("provider help command help message");
        assert!(help.body.contains("Show provider help"));
        assert!(help.body.contains("usage: `/help`"));
    }

    #[tokio::test]
    async fn command_help_covers_session_trait_commands_outside_provider_catalog() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let catalog = AgentCommandCatalog {
            commands: Vec::new(),
            complete: true,
            revision: 1,
        };
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/commands status help".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let help = sent
            .iter()
            .find(|msg| msg.body.contains("`/status`"))
            .expect("status help message");
        assert!(help.body.contains("usage: `/status`"));
        assert!(!help.body.contains("source:"));
    }

    #[tokio::test]
    async fn command_help_covers_commands_wrapper_and_workspace_rename_command() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs.clone(),
                invocations.clone(),
                test_command_catalog(),
            ),
        );
        bind_test_workspace(&bot);

        for (message_id, text) in [("10", "/commands help"), ("11", "/rename help")] {
            bot.clone()
                .handle(ChannelEvent::Message(IncomingMessage {
                    message_id: MessageId::new(message_id),
                    chat: ChatId::new("100"),
                    workspace: Some(WorkspaceId::new("2")),
                    reply_to: None,
                    user: "u".into(),
                    text: Some(text.into()),
                    attachments: Vec::new(),
                }))
                .await
                .unwrap();
        }

        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
        assert!(channel.renames.lock().unwrap().is_empty());
        let sent = channel.sent.lock().unwrap();
        let commands = sent
            .iter()
            .find(|msg| msg.body.contains("`/commands`"))
            .expect("commands help message");
        assert!(commands
            .body
            .contains("usage: `/commands [command [arguments]]`"));
        let rename = sent
            .iter()
            .find(|msg| msg.body.contains("`/rename`"))
            .expect("rename help message");
        assert!(rename.body.contains("usage: `/rename <name>`"));
    }

    #[tokio::test]
    async fn provider_native_command_button_with_no_output_completes_from_ack() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "native-report".into(),
            description: Some("Show a provider-native report".into()),
            aliases: Vec::new(),
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::Text {
                label: "no_output".into(),
                required: false,
            },
            completion: AgentCommandCompletion::NoOutputAck,
        });
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);
        let button =
            bot.state
                .register_command_callback(&WorkspaceId::new("2"), 1, "native-report", "");

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            bot.clone().handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: button,
                source_message: MessageId::new("42"),
            }),
        )
        .await
        .expect("no-output provider-native command should not wait for turn inactivity")
        .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "native-report");
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter().any(|msg| msg.body.starts_with("✓ 完成")),
            "NoOutputAck should complete from command acceptance, not event-drain timeout: {:?}",
            sent.iter().map(|msg| msg.body.as_str()).collect::<Vec<_>>()
        );
        assert!(!sent.iter().any(|msg| msg.body.starts_with("⚠ 失败")));
        drop(sent);
        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 1);
        assert_eq!(
            workflows[0].completion_policy,
            CommandCompletionPolicy::NoOutputAck
        );
        assert_eq!(workflows[0].state, CommandState::Completed);
        let session = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains bound");
        assert!(session.live.is_some());
        assert_eq!(session.resume_ref.as_deref(), Some("session-test"));
    }

    #[tokio::test]
    async fn workspace_bound_agent_command_button_invokes_matching_workspace() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);
        let button = bot
            .state
            .register_command_callback(&WorkspaceId::new("2"), 1, "status", "");

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: button,
                source_message: MessageId::new("42"),
            })
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "status");
    }

    #[tokio::test]
    async fn workspace_bound_agent_command_button_rejects_foreign_workspace() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);
        let button =
            bot.state
                .register_command_callback(&WorkspaceId::new("foreign"), 1, "status", "");

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: button,
                source_message: MessageId::new("42"),
            })
            .await
            .unwrap();

        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("Stale command button")),
            "{sent:?}"
        );
        assert!(inputs.lock().unwrap().is_empty());
        assert!(invocations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn legacy_agent_command_button_is_ignored() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("3"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: None,
                title: "codex other".into(),
                live: None,
                resume_ref: None,
            })
            .expect("bind other test workspace");

        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("3")),
            user: "u".into(),
            data: "agentcmd:status".into(),
            source_message: MessageId::new("42"),
        })
        .await
        .unwrap();

        let sent_targets = channel.sent_targets.lock().unwrap();
        assert!(
            sent_targets.is_empty(),
            "legacy callback must not execute commands; sent targets: {sent_targets:?}"
        );
        assert!(invocations.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn topic_command_reopens_observed_closed_live_session_before_invoking_command() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        let (_tx, rx) = broadcast::channel(1);
        let closed_live = Arc::new(LiveSession {
            session: Arc::new(ClosedSession::new()),
            events: TokioMutex::new(lucarne::core_service::CoreWorkspaceEventStream::new(
                ControlWorkspaceId::new("2"),
                rx,
            )),
            pending_intv: StdMutex::new(HashMap::new()),
        });
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("2"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: Some(closed_live),
                resume_ref: Some("closed-session".into()),
            })
            .unwrap();

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
    }

    #[tokio::test]
    async fn topic_command_reopens_live_session_when_provider_ref_belongs_to_another_session() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_lifecycle_command_recorder(
                Arc::clone(&inputs),
                Arc::clone(&invocations),
                Arc::clone(&opens),
                Arc::clone(&resumes),
            ),
        );
        let mismatched = RecordingSession::new(
            "codex",
            Arc::clone(&inputs),
            Arc::clone(&invocations),
            test_command_catalog(),
            test_fork_targets(),
            Vec::new(),
        );
        *mismatched.provider_session_id.lock().unwrap() = Some(SessionId("session-fork".into()));
        let (_tx, rx) = broadcast::channel(1);
        let live = Arc::new(LiveSession {
            session: Arc::new(mismatched),
            events: TokioMutex::new(lucarne::core_service::CoreWorkspaceEventStream::new(
                ControlWorkspaceId::new("2"),
                rx,
            )),
            pending_intv: StdMutex::new(HashMap::new()),
        });
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("2"),
                chat: ChatId::new("100"),
                provider_id: "codex",
                project_path: Some(PathBuf::from("/tmp/project")),
                title: "codex".into(),
                live: Some(live),
                resume_ref: Some("session-test".into()),
            })
            .unwrap();

        Arc::clone(&bot)
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/status".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["session-test"],
            "workspace must reopen its own provider session instead of using a fork live"
        );
        let session = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("workspace remains bound");
        assert_eq!(session.resume_ref.as_deref(), Some("session-test"));
        let live_ref = if let Some(live) = session.live.as_ref() {
            live_provider_resume_ref(live).await
        } else {
            None
        };
        assert_eq!(live_ref.as_deref(), Some("session-test"));
    }

    #[tokio::test]
    async fn model_topic_command_renders_text_usage_without_buttons() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        Arc::clone(&bot)
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
        assert_eq!(invocations[0].args.as_deref(), None);
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        let models = sent
            .iter()
            .find(|msg| msg.body.contains("models") && msg.body.contains("`gpt-5.5`"))
            .expect("model list message");
        assert!(models.buttons.is_empty(), "{models:?}");
        assert!(models.body.contains("set: `/model <model> [reasoning]`"));
        assert!(models.body.contains("reasoning levels: `medium`, `high`"));
        assert!(models.body.contains("example: `/model gpt-5.5 high`"));
    }

    #[tokio::test]
    async fn models_alias_renders_model_usage_text() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        Arc::clone(&bot)
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/models".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
        assert_eq!(invocations[0].args.as_deref(), None);
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("models") && msg.body.contains("`gpt-5.5`")),
            "/models should render the model catalog, not an unsupported-command message: {sent:?}"
        );
        assert!(
            sent.iter()
                .all(|msg| !msg.body.contains("Unsupported command /models")),
            "{sent:?}"
        );
        let models = sent
            .iter()
            .find(|msg| msg.body.contains("models") && msg.body.contains("`gpt-5.5`"))
            .expect("model list message");
        assert!(models.buttons.is_empty(), "{models:?}");
        assert!(models.body.contains("reasoning levels: `medium`, `high`"));
    }

    #[tokio::test]
    async fn model_list_uses_agent_command_when_catalog_has_no_structured_options() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs.clone(),
                invocations.clone(),
                catalog_with_plain_adapter_command("model"),
            ),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
        assert_eq!(invocations[0].args.as_deref(), None);
    }

    #[tokio::test]
    async fn model_text_command_sets_model_and_reasoning() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model gpt-5.5 medium".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "model");
        assert_eq!(invocations[0].args.as_deref(), Some("gpt-5.5 medium"));
    }

    #[tokio::test]
    async fn permissions_topic_command_renders_text_modes_and_invokes_text_set() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/permissions".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let list_invocations = invocations.lock().unwrap();
        assert_eq!(list_invocations.len(), 1);
        assert_eq!(list_invocations[0].name.as_str(), "permissions");
        assert_eq!(list_invocations[0].args.as_deref(), None);
        drop(list_invocations);
        let sent = channel.sent.lock().unwrap();
        let permissions = sent
            .iter()
            .find(|msg| msg.body.contains("permission modes"))
            .expect("permission list message");
        assert!(permissions.body.contains("set: `/permissions <mode>`"));
        assert!(permissions.buttons.is_empty());
        drop(sent);

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("11"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("/permissions on-request".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let set_invocations = invocations.lock().unwrap();
        assert_eq!(set_invocations.len(), 2);
        assert_eq!(set_invocations[1].name.as_str(), "permissions");
        assert_eq!(set_invocations[1].args.as_deref(), Some("on-request"));
        assert_eq!(
            set_invocations[1].source,
            AgentCommandSource::AdapterMapped,
            "/permissions <mode> must use the typed session API"
        );
    }

    #[tokio::test]
    async fn model_text_command_and_permissions_text_command_update_agent_status() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("model-list"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/model gpt-5.4".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        live_clear_channel(&channel);
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("status-after-model"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/status".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        live_assert_status_contains(&channel, "model status", &["gpt-5.4"]);

        live_clear_channel(&channel);
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("permissions-list"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/permissions".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        let permissions = live_find_sent_message(&channel, "permission modes");
        assert!(permissions.buttons.is_empty());
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("permissions-set"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/permissions on-request".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        live_clear_channel(&channel);
        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("status-after-permissions"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("/status".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();
        live_assert_status_contains(&channel, "permissions status", &["on-request"]);

        let invocations = invocations.lock().unwrap();
        assert!(invocations
            .iter()
            .any(|invocation| invocation.name.as_str() == "model"
                && invocation.args.as_deref() == Some("gpt-5.4")));
        assert!(invocations
            .iter()
            .any(|invocation| invocation.name.as_str() == "permissions"
                && invocation.args.as_deref() == Some("on-request")));
        assert!(inputs.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn fork_topic_command_renders_target_list_and_text_selection_invokes_target() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let resumes = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_lifecycle_command_recorder(
                inputs.clone(),
                invocations.clone(),
                Arc::clone(&opens),
                Arc::clone(&resumes),
            ),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations_after_list = invocations.lock().unwrap();
        assert_eq!(invocations_after_list.len(), 1);
        assert_eq!(invocations_after_list[0].name.as_str(), "fork");
        assert_eq!(invocations_after_list[0].args.as_deref(), None);
        drop(invocations_after_list);

        let sent = channel.sent.lock().unwrap();
        let forks = sent
            .iter()
            .find(|msg| msg.body.contains("fork targets") && msg.body.contains("msg-2"))
            .expect("fork target list message");
        assert!(forks.body.contains("/f1  `msg-2`"));
        assert!(
            forks.buttons.is_empty(),
            "fork targets should render as /fN text selectors"
        );
        drop(sent);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/f1".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[1].name.as_str(), "fork");
        assert_eq!(invocations[1].args.as_deref(), Some("msg-2"));
        drop(invocations);
        assert_eq!(
            resumes.lock().unwrap().as_slice(),
            ["session-fork"],
            "resumable fork should resume a separate live session instead of reusing the source live"
        );

        let fork_workspace = workspace_id_for_resume("codex", "session-fork");
        let fork_session = bot
            .state
            .get(&fork_workspace)
            .expect("fork should create a canonical fork workspace");
        assert_eq!(fork_session.resume_ref.as_deref(), Some("session-fork"));
        assert!(
            fork_session.live.is_some(),
            "fork workspace owns live session"
        );
        assert!(
            channel.created_workspaces.lock().unwrap().is_empty(),
            "fork target selection should not create a Telegram topic"
        );
        assert_eq!(
            bot.state
                .workspace_for_handle(&WorkspaceHandle::new(
                    ChatId::new("100"),
                    WorkspaceId::new("2")
                ))
                .as_ref(),
            Some(&fork_workspace),
            "current topic should route to the fork workspace"
        );
        assert_eq!(
            bot.state
                .topic_for_workspace(&fork_workspace)
                .map(|workspace| workspace.as_str().to_string())
                .as_deref(),
            Some("2"),
            "fork workspace should reuse the current topic"
        );
        let source = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("source workspace must remain visible");
        assert_eq!(source.resume_ref.as_deref(), Some("session-test"));
        assert!(
            source.live.is_none(),
            "source workspace should not keep fork live"
        );
        let source_snapshot = bot
            .state
            .status_snapshot(&WorkspaceId::new("2"))
            .expect("source workspace control-plane snapshot");
        assert_eq!(
            source_snapshot.native_resume_ref.as_deref(),
            Some("session-test")
        );
        assert_eq!(
            source_snapshot
                .live_instance_id
                .as_ref()
                .map(|id| id.as_str()),
            None,
            "source workspace must not keep the fork live instance"
        );
    }

    #[tokio::test]
    async fn fork_topic_command_uses_typed_list_when_catalog_marks_provider_native() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog
            .commands
            .iter_mut()
            .find(|command| command.name.as_str() == "fork")
            .expect("fork command")
            .source = AgentCommandSource::ProviderNative;
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "fork");
        assert_eq!(invocations[0].args.as_deref(), None);
        assert_eq!(
            invocations[0].source,
            AgentCommandSource::AdapterMapped,
            "/fork must list targets through the typed session API"
        );
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("fork targets") && msg.body.contains("msg-2")),
            "/fork should render fork targets instead of provider-authored text: {sent:?}"
        );
        assert!(
            sent.iter()
                .all(|msg| !msg.body.contains("agent fork output")),
            "{sent:?}"
        );
    }

    #[tokio::test]
    async fn fork_text_selection_uses_typed_session_fork_when_catalog_marks_provider_native() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let mut catalog = test_command_catalog();
        catalog
            .commands
            .iter_mut()
            .find(|command| command.name.as_str() == "fork")
            .expect("fork command")
            .source = AgentCommandSource::ProviderNative;
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(inputs.clone(), invocations.clone(), catalog),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        {
            let sent = channel.sent.lock().unwrap();
            let forks = sent
                .iter()
                .find(|msg| msg.body.contains("fork targets"))
                .expect("fork target list message");
            assert!(forks.body.contains("/f1  `msg-2`"));
            assert!(forks.buttons.is_empty());
        }

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/f1".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        assert!(inputs.lock().unwrap().is_empty());
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[1].name.as_str(), "fork");
        assert_eq!(invocations[1].args.as_deref(), Some("msg-2"));
        assert_eq!(
            invocations[1].source,
            AgentCommandSource::AdapterMapped,
            "fork target selection must use the typed session API"
        );
        drop(invocations);
        let fork_workspace = workspace_id_for_resume("codex", "session-fork");
        let fork_session = bot
            .state
            .get(&fork_workspace)
            .expect("typed fork should create a canonical fork workspace");
        assert_eq!(fork_session.resume_ref.as_deref(), Some("session-fork"));
        assert!(
            channel.created_workspaces.lock().unwrap().is_empty(),
            "typed fork target selection should not create a Telegram topic"
        );
        assert_eq!(
            bot.state
                .workspace_for_handle(&WorkspaceHandle::new(
                    ChatId::new("100"),
                    WorkspaceId::new("2")
                ))
                .as_ref(),
            Some(&fork_workspace),
            "current topic should route to the typed fork workspace"
        );
    }

    #[tokio::test]
    async fn fork_text_selection_resolves_long_target_id_from_latest_list() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let long_target = "msg_01JAqMAL2CFkaXcXj2g6xc1r_really_long_claude_message_id";
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_fork_targets(
            inputs.clone(),
            invocations.clone(),
            vec![AgentForkTarget {
                id: long_target.into(),
                label: Some("long target".into()),
                description: Some("Long provider-native target id".into()),
            }],
        )));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        {
            let sent = channel.sent.lock().unwrap();
            let forks = sent
                .iter()
                .find(|msg| msg.body.contains("fork targets") && msg.body.contains(long_target))
                .expect("fork target list message");
            assert!(forks.body.contains("/f1"));
            assert!(forks.body.contains(long_target));
            assert!(
                forks.buttons.is_empty(),
                "long fork targets should not need callback buttons"
            );
        }

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("11"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("/f1".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[1].name.as_str(), "fork");
        assert_eq!(invocations[1].args.as_deref(), Some(long_target));
    }

    #[tokio::test]
    async fn fork_text_selection_rejects_target_list_from_previous_provider_session() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_recorder(inputs.clone(), invocations.clone()),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/new".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("12"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/f1".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 2, "{invocations:?}");
        assert_eq!(invocations[0].name.as_str(), "fork");
        assert_eq!(invocations[0].args.as_deref(), None);
        assert_eq!(invocations[1].name.as_str(), "new");
        drop(invocations);
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("Stale fork target")),
            "{sent:?}"
        );
    }

    #[tokio::test]
    async fn fork_result_without_resumable_ref_creates_live_only_workspace() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(RecordingProvider::new_with_invocations(
            inputs.clone(),
            invocations.clone(),
        )));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork no-ref".into()),
                attachments: Vec::new(),
            }))
            .await
            .expect("live-only fork should complete");
        let fork_session = bot
            .state
            .all()
            .into_iter()
            .find(|session| session.title.ends_with(" · fork"))
            .expect("live-only fork workspace");
        assert!(
            fork_session.resume_ref.is_none(),
            "live-only fork must not be resumable"
        );
        assert!(
            fork_session.live.is_some(),
            "live-only fork should keep live"
        );
        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert!(
            workflows
                .iter()
                .any(|workflow| workflow.name.as_str() == "fork"
                    && workflow.args.as_deref() == Some("no-ref")
                    && workflow.state == CommandState::Completed),
            "{workflows:?}"
        );
    }

    #[tokio::test]
    async fn fork_text_selection_uses_session_trait_without_provider_fork_api() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let runtime = Arc::new(AgentRuntime::new());
        let provider = RecordingProvider::new_with_provider_id(
            "claude",
            inputs.clone(),
            invocations.clone(),
            test_command_catalog(),
        );
        runtime.register(Arc::new(provider));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bot.state
            .upsert(WorkSession {
                workspace: WorkspaceId::new("2"),
                chat: ChatId::new("100"),
                provider_id: "claude",
                project_path: Some(PathBuf::from("/tmp/claude-project")),
                title: "claude".into(),
                live: None,
                resume_ref: Some("sess-source".into()),
            })
            .expect("bind claude workspace");

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/fork".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        {
            let sent = channel.sent.lock().unwrap();
            let forks = sent
                .iter()
                .find(|msg| msg.body.contains("fork targets"))
                .expect("fork target list message");
            assert!(forks.body.contains("/f1  `msg-2`"));
            assert!(forks.buttons.is_empty());
        }
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("11"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("/f1".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 2);
        assert_eq!(invocations[0].name.as_str(), "fork");
        assert_eq!(invocations[0].args.as_deref(), None);
        assert_eq!(invocations[1].name.as_str(), "fork");
        assert_eq!(invocations[1].args.as_deref(), Some("msg-2"));
        drop(invocations);

        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter()
                .any(|msg| msg.body.contains("✓ forked `session-fork`")),
            "{sent:?}"
        );
        drop(sent);

        let workflows = bot.state.command_workflows(&WorkspaceId::new("2"));
        assert_eq!(workflows.len(), 2);
        assert!(
            workflows
                .iter()
                .any(|workflow| workflow.name.as_str() == "fork"
                    && workflow.args.as_deref() == Some("msg-2")
                    && workflow.state == CommandState::Completed
                    && workflow.completion_policy == CommandCompletionPolicy::CommandResult),
            "{workflows:?}"
        );

        let source = bot
            .state
            .get(&WorkspaceId::new("2"))
            .expect("source workspace remains bound");
        assert_eq!(source.resume_ref.as_deref(), Some("sess-source"));
        let forked = bot
            .state
            .get(&workspace_id_for_resume("claude", "session-fork"))
            .expect("fork creates a separate workspace");
        assert_eq!(forked.resume_ref.as_deref(), Some("session-fork"));
        assert!(forked.live.is_some());
        let fork_snapshot = bot
            .state
            .status_snapshot(&forked.workspace)
            .expect("fork workspace control-plane snapshot");
        assert_eq!(
            fork_snapshot.provider_session_id,
            Some(provider_session_id_for_resume("claude", "session-fork"))
        );
        assert_eq!(
            fork_snapshot.native_resume_ref.as_deref(),
            Some("session-fork")
        );
        assert_eq!(
            fork_snapshot
                .live_instance_id
                .as_ref()
                .map(|id| id.as_str()),
            Some("instance-test")
        );
    }

    #[tokio::test]
    async fn live_bot_e2e_claude_core_command_surface_round_trip() {
        let Some(_guard) = live_bot_e2e_provider("claude") else {
            return;
        };
        live_bot_e2e_core_command_surface("claude").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_codex_core_command_surface_round_trip() {
        let Some(_guard) = live_bot_e2e_provider("codex") else {
            return;
        };
        live_bot_e2e_core_command_surface("codex").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_gemini_core_command_surface_round_trip() {
        let Some(_guard) = live_bot_e2e_provider("gemini") else {
            return;
        };
        live_bot_e2e_core_command_surface("gemini").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_claude_human_like_workspace_journey() {
        let Some(_guard) = live_bot_e2e_provider("claude") else {
            return;
        };
        live_bot_e2e_human_like_workspace_journey("claude").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_claude_concurrent_workspace_messages_queue() {
        let Some(_guard) = live_bot_e2e_provider("claude") else {
            return;
        };
        let fixture = live_bot_fixture("claude", "claude live queued messages");
        let first_bot = fixture.bot.clone();
        let first_ws = fixture.ws.clone();
        let first = tokio::spawn(async move {
            timeout(
                live_bot_e2e_timeout(),
                first_bot.handle(ChannelEvent::Message(IncomingMessage {
                    message_id: MessageId::new("queue-first"),
                    chat: ChatId::new("100"),
                    workspace: Some(first_ws),
                    reply_to: None,
                    user: "live-e2e".into(),
                    text: Some(
                        "Use the Read tool to read README.md, then reply exactly: LIVE_LUCARNE_QUEUE_ONE"
                            .into(),
                    ),
                    attachments: Vec::new(),
                })),
            )
            .await
            .expect("first queued live message timed out")
            .expect("first queued live message failed");
        });

        timeout(Duration::from_secs(30), async {
            loop {
                if fixture
                    .channel
                    .sent
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|message| message.body.starts_with("⏳"))
                {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("first Claude turn should enter live processing before second message");

        let second_bot = fixture.bot.clone();
        let second_ws = fixture.ws.clone();
        let second = tokio::spawn(async move {
            timeout(
                live_bot_e2e_timeout(),
                second_bot.handle(ChannelEvent::Message(IncomingMessage {
                    message_id: MessageId::new("queue-second"),
                    chat: ChatId::new("100"),
                    workspace: Some(second_ws),
                    reply_to: None,
                    user: "live-e2e".into(),
                    text: Some("Reply with exactly: LIVE_LUCARNE_QUEUE_TWO".into()),
                    attachments: Vec::new(),
                })),
            )
            .await
            .expect("second queued live message timed out")
            .expect("second queued live message failed");
        });

        timeout(Duration::from_secs(10), async {
            loop {
                if fixture.channel.sent.lock().unwrap().iter().any(|message| {
                    message.body.contains("queued")
                        && message
                            .reply_to
                            .as_ref()
                            .is_some_and(|id| id.as_str() == "queue-second")
                }) {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("second Claude message should queue with visible feedback");

        first.await.expect("first queued live task join");
        second.await.expect("second queued live task join");
        live_assert_message_contains(
            &fixture.channel,
            "Claude queued first turn",
            &["LIVE_LUCARNE_QUEUE_ONE"],
        );
        live_assert_message_contains(
            &fixture.channel,
            "Claude queued second turn",
            &["LIVE_LUCARNE_QUEUE_TWO"],
        );
        live_assert_reply_to(&fixture.channel, "queue-first");
        live_assert_reply_to(&fixture.channel, "queue-second");
        assert_eq!(
            fixture.state.all().len(),
            1,
            "queued Claude messages should not create an extra workspace/session"
        );
        live_assert_state_matches_provider_ref(&fixture).await;
        live_assert_no_failures(&fixture.channel);
    }

    #[tokio::test]
    async fn live_bot_e2e_codex_human_like_workspace_journey() {
        let Some(_guard) = live_bot_e2e_provider("codex") else {
            return;
        };
        live_bot_e2e_human_like_workspace_journey("codex").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_codex_history_entry_replays_recent_10_turns() {
        let case = RecordedLiveCase {
            suite: "telegram_live_e2e",
            case_id: "history_recent_10_codex",
        };
        let env_lock = env_lock();
        let Some(provider) = replay_provider_or_return("codex", case) else {
            return;
        };
        let fixture = recorded_live_bot_fixture(provider, "codex recorded history", case, env_lock);
        live_bot_e2e_history_entry_replays_recent_10_turns(fixture, "codex").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_gemini_human_like_workspace_journey() {
        let Some(_guard) = live_bot_e2e_provider("gemini") else {
            return;
        };
        live_bot_e2e_human_like_workspace_journey("gemini").await;
    }

    #[tokio::test]
    async fn live_bot_e2e_claude_fork_rebind_resume_round_trip() {
        let Some(_guard) = live_bot_e2e_provider("claude") else {
            return;
        };
        let temp = tempfile::TempDir::new().expect("temp dir");
        let project = temp.path().join("project");
        std::fs::create_dir_all(&project).expect("project dir");
        std::fs::write(project.join("README.md"), "lucarne telegram live bot e2e\n")
            .expect("readme");
        let db = temp.path().join("state.sqlite3");
        let channel = Arc::new(RecordingChannel::default());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register_defaults();
        let core = core_with_runtime(runtime);
        let state = BotState::open_sqlite(&db).expect("open temp state");
        let bot = Arc::new(Bot::new_with_state(
            channel.clone(),
            core,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state.clone(),
        ));
        let ws = WorkspaceId::new("live-claude");
        state
            .upsert(WorkSession {
                workspace: ws.clone(),
                chat: ChatId::new("100"),
                provider_id: "claude",
                project_path: Some(project.clone()),
                title: "claude live".into(),
                live: None,
                resume_ref: None,
            })
            .expect("seed workspace");

        live_send_text(&bot, &ws, "Reply with exactly: LIVE_LUCARNE_ONE", "u1").await;
        live_send_text(&bot, &ws, "Reply with exactly: LIVE_LUCARNE_TWO", "u2").await;
        let source_resume_ref = state
            .get(&ws)
            .and_then(|session| session.resume_ref)
            .expect("source provider resume ref after first turns");

        live_send_text(&bot, &ws, "/fork", "fork-list").await;
        live_send_text(&bot, &ws, "/f1", "fork-select").await;

        let source_after_fork = state.get(&ws).expect("source workspace remains after fork");
        assert_eq!(
            source_after_fork.resume_ref.as_deref(),
            Some(source_resume_ref.as_str()),
            "Claude fork must preserve the source provider session"
        );
        assert!(
            source_after_fork.live.is_none(),
            "Claude fork must move the live session out of the source workspace"
        );

        let fork_session = state
            .all()
            .into_iter()
            .find(|session| {
                session.provider_id == "claude" && session.workspace != ws && session.live.is_some()
            })
            .expect("Claude fork should create a live child workspace");
        let fork_ws = fork_session.workspace.clone();
        let fork_topic = state
            .topic_for_workspace(&fork_ws)
            .unwrap_or_else(|| fork_ws.clone());
        if let Some(fork_resume_ref) = fork_session.resume_ref.as_deref() {
            assert_ne!(fork_resume_ref, source_resume_ref);
        }

        live_send_text(
            &bot,
            &fork_topic,
            "Reply with exactly: LIVE_LUCARNE_FORKED",
            "forked-turn",
        )
        .await;
        let fork_resume_ref = state
            .get(&fork_ws)
            .and_then(|session| session.resume_ref)
            .expect("fork provider resume ref after first forked turn");
        assert_ne!(fork_resume_ref, source_resume_ref);

        if let Some(live) = state.get(&fork_ws).and_then(|session| session.live) {
            live.session.close().await.expect("close pre-restart live");
        }

        let channel_after_restart = Arc::new(RecordingChannel::default());
        let runtime_after_restart = Arc::new(AgentRuntime::new());
        runtime_after_restart.register_defaults();
        let core_after_restart = core_with_runtime(runtime_after_restart);
        let state_after_restart = BotState::open_sqlite(&db).expect("reload temp state");
        let bot_after_restart = Arc::new(Bot::new_with_state(
            channel_after_restart.clone(),
            core_after_restart,
            WorkspaceHandle::new(ChatId::new("100"), WorkspaceId::new("")),
            state_after_restart,
        ));
        let fork_topic_after_restart = bot_after_restart
            .state
            .topic_for_workspace(&fork_ws)
            .unwrap_or_else(|| fork_ws.clone());

        live_send_text(
            &bot_after_restart,
            &fork_topic_after_restart,
            "Reply with exactly: LIVE_LUCARNE_RESUMED",
            "resumed-turn",
        )
        .await;
        live_assert_no_failures(&channel);
        live_assert_no_failures(&channel_after_restart);
    }

    #[tokio::test]
    async fn permissions_list_uses_agent_command_when_catalog_has_no_structured_options() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs.clone(),
                invocations.clone(),
                catalog_with_plain_adapter_command("permissions"),
            ),
        );
        bind_test_workspace(&bot);

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("10"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("/permissions".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "permissions");
        assert_eq!(invocations[0].args.as_deref(), None);
    }

    #[tokio::test]
    async fn agent_command_button_uses_agent_command_when_catalog_has_no_structured_options() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs.clone(),
                invocations.clone(),
                catalog_with_plain_adapter_command("status"),
            ),
        );
        bind_test_workspace(&bot);

        let data = bot
            .state
            .register_command_callback(&WorkspaceId::new("2"), 1, "status", "");
        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            user: "u".into(),
            data,
            source_message: MessageId::new("42"),
        })
        .await
        .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "status");
        assert_eq!(invocations[0].args.as_deref(), None);
    }

    #[tokio::test]
    async fn inline_query_does_not_invent_unbound_permission_modes() {
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        bot.handle(ChannelEvent::CommandQuery(CommandQuery {
            id: "inline-1".into(),
            user: "u".into(),
            query: "permissions a".into(),
            chat_type: Some("Private".into()),
        }))
        .await
        .unwrap();

        let answers = channel.command_query_answers.lock().unwrap();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].0, "inline-1");
        assert!(
            answers[0].1.is_empty(),
            "unbound inline query must not use a fallback command catalog"
        );
    }

    #[test]
    fn command_query_permissions_suggestions_use_bound_agent_catalog() {
        let results = command_query_results_from_catalog("permissions on", &test_command_catalog());
        assert!(results
            .iter()
            .any(|result| result.message_text == "/permissions on-request"));
        assert!(!results
            .iter()
            .any(|result| result.message_text == "/permissions auto"));
    }

    #[test]
    fn command_query_alias_suggestions_use_bound_agent_catalog() {
        let mut catalog = test_command_catalog();
        catalog.commands.push(AgentCommand {
            name: "usage".into(),
            description: Some("Show session stats".into()),
            aliases: vec!["cost".into()],
            source: AgentCommandSource::ProviderNative,
            input: AgentCommandInput::None,
            completion: AgentCommandCompletion::ProviderIdle,
        });

        let results = command_query_results_from_catalog("cost", &catalog);
        assert!(results
            .iter()
            .any(|result| { result.title == "/cost" && result.message_text == "/cost" }));
    }

    #[test]
    fn command_plan_uses_catalog_completion_semantics_not_input_label() {
        let catalog = AgentCommandCatalog {
            commands: vec![AgentCommand {
                name: "native-report".into(),
                description: Some("Show native report".into()),
                aliases: Vec::new(),
                source: AgentCommandSource::ProviderNative,
                input: AgentCommandInput::Text {
                    label: "no_output".into(),
                    required: false,
                },
                completion: AgentCommandCompletion::ProviderIdle,
            }],
            complete: true,
            revision: 1,
        };

        let plan = plan_command_invocation(&catalog, "native-report", "", serde_json::Value::Null)
            .expect("command plan");
        assert_eq!(
            plan.completion_policy,
            CommandCompletionPolicy::ProviderIdle
        );
    }

    #[tokio::test]
    async fn callback_values_round_trip_into_agent_invocation() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let invocations = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_command_catalog_recorder(
                inputs,
                invocations.clone(),
                catalog_with_plain_adapter_command("review"),
            ),
        );
        bind_test_workspace(&bot);
        let values = json!({"target": {"type": "uncommitted_changes"}, "delivery": "inline"});
        let button = bot.state.register_command_callback_values(
            &WorkspaceId::new("2"),
            1,
            "review",
            "",
            values.clone(),
        );

        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            user: "u".into(),
            data: button,
            source_message: MessageId::new("42"),
        })
        .await
        .unwrap();

        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].name.as_str(), "review");
        assert_eq!(invocations[0].values, values);
    }

    #[tokio::test]
    async fn topic_turn_final_reply_references_user_message() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime_with_recorder(inputs));
        bind_test_workspace(&bot);

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("10"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("hello".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        let sent = channel.sent.lock().unwrap();
        assert!(sent.iter().any(|msg| {
            msg.body == "ok" && msg.reply_to.as_ref().map(|id| id.as_str()) == Some("10")
        }));
        assert!(sent
            .iter()
            .any(|msg| msg.body.starts_with("⏳") && msg.reply_to.is_none()));
        let edits = channel.edits.lock().unwrap();
        let turn_bodies = sent
            .iter()
            .map(|msg| msg.body.as_str())
            .chain(edits.iter().map(|(_, msg)| msg.body.as_str()))
            .filter(|body| body.contains("ok"))
            .collect::<Vec<_>>();
        assert!(!turn_bodies.is_empty());
        assert!(
            turn_bodies.iter().all(|body| !body.contains("\n\n---\n\n")
                && !body.contains("session:")
                && !body.contains("cwd:")),
            "non-notification Telegram turn must not include agent footer: {turn_bodies:?}"
        );
    }

    #[tokio::test]
    async fn concurrent_workspace_messages_queue_during_first_open_without_duplicate_live() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let release_open = Arc::new(AtomicBool::new(false));
        let notify_open = Arc::new(Notify::new());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(BlockingOpenProvider {
            inputs: Arc::clone(&inputs),
            opens: Arc::clone(&opens),
            release_open: Arc::clone(&release_open),
            notify_open: Arc::clone(&notify_open),
        }));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);

        let first = tokio::spawn(bot.clone().handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("first"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("first prompt".into()),
            attachments: Vec::new(),
        })));

        timeout(Duration::from_secs(1), async {
            while opens.load(AtomicOrdering::SeqCst) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first message should start opening the provider");

        let second = tokio::spawn(bot.clone().handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("second"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("second prompt".into()),
            attachments: Vec::new(),
        })));

        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            opens.load(AtomicOrdering::SeqCst),
            1,
            "second same-workspace message must queue behind the first open, not start another live"
        );
        assert!(
            inputs.lock().unwrap().is_empty(),
            "no prompt should submit until the blocked open is released"
        );
        {
            let sent = channel.sent.lock().unwrap();
            assert!(
                sent.iter().any(|msg| {
                    msg.body.contains("queued")
                        && msg.reply_to.as_ref().map(|id| id.as_str()) == Some("second")
                }),
                "queued same-workspace message should get visible feedback replying to the user; sent: {sent:?}"
            );
        }

        release_open.store(true, AtomicOrdering::SeqCst);
        notify_open.notify_waiters();
        first
            .await
            .expect("first handler join")
            .expect("first handler");
        second
            .await
            .expect("second handler join")
            .expect("second handler");

        assert_eq!(
            opens.load(AtomicOrdering::SeqCst),
            1,
            "both messages should reuse one live session"
        );
        let submitted = inputs
            .lock()
            .unwrap()
            .iter()
            .map(|input| input.text.to_string())
            .collect::<Vec<_>>();
        assert_eq!(submitted, vec!["first prompt", "second prompt"]);
    }

    #[tokio::test]
    async fn agent_command_button_queues_behind_active_workspace_turn() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let opens = Arc::new(AtomicUsize::new(0));
        let release_open = Arc::new(AtomicBool::new(false));
        let notify_open = Arc::new(Notify::new());
        let runtime = Arc::new(AgentRuntime::new());
        runtime.register(Arc::new(BlockingOpenProvider {
            inputs: Arc::clone(&inputs),
            opens: Arc::clone(&opens),
            release_open: Arc::clone(&release_open),
            notify_open: Arc::clone(&notify_open),
        }));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime);
        bind_test_workspace(&bot);
        let button = bot
            .state
            .register_command_callback(&WorkspaceId::new("2"), 1, "skills", "");

        let first = tokio::spawn(bot.clone().handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("first"),
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            reply_to: None,
            user: "u".into(),
            text: Some("first prompt".into()),
            attachments: Vec::new(),
        })));

        timeout(Duration::from_secs(1), async {
            while opens.load(AtomicOrdering::SeqCst) == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("first message should start opening the provider");

        let clicked = tokio::spawn(bot.clone().handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: Some(WorkspaceId::new("2")),
            user: "u".into(),
            data: button,
            source_message: MessageId::new("button"),
        }));

        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            opens.load(AtomicOrdering::SeqCst),
            1,
            "command button must queue behind active open instead of starting another live"
        );
        {
            let sent = channel.sent.lock().unwrap();
            assert!(
                sent.iter().any(|msg| {
                    msg.body.contains("queued")
                        && msg.reply_to.as_ref().map(|id| id.as_str()) == Some("button")
                }),
                "queued command button should get visible feedback replying to the source message; sent: {sent:?}"
            );
        }

        release_open.store(true, AtomicOrdering::SeqCst);
        notify_open.notify_waiters();
        first
            .await
            .expect("first handler join")
            .expect("first handler");
        clicked
            .await
            .expect("button handler join")
            .expect("button handler");

        assert_eq!(
            opens.load(AtomicOrdering::SeqCst),
            1,
            "button should reuse the live session opened by the active turn"
        );
        let submitted = inputs
            .lock()
            .unwrap()
            .iter()
            .map(|input| input.text.to_string())
            .collect::<Vec<_>>();
        assert_eq!(submitted, vec!["first prompt"]);
        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.iter().any(|msg| msg.body.contains("skills")),
            "button command should still run after the queued turn completes; sent: {sent:?}"
        );
    }

    #[tokio::test]
    async fn topic_user_turn_records_control_plane_timeline() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(Arc::clone(&channel), runtime_with_recorder(inputs));
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("hello timeline".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let kinds = bot.state.timeline_kinds(&WorkspaceId::new("2"));
        assert!(
            kinds.contains(&lucarne::control_plane::TimelineItemKind::User),
            "user input should be recorded in the control-plane timeline: {kinds:?}"
        );
        assert!(
            kinds.contains(&lucarne::control_plane::TimelineItemKind::Assistant),
            "assistant output should be recorded in the control-plane timeline: {kinds:?}"
        );
    }

    #[tokio::test]
    async fn subagent_tool_call_records_control_plane_link() {
        let channel = Arc::new(RecordingChannel::default());
        let inputs = Arc::new(StdMutex::new(Vec::new()));
        let bot = test_bot_with_runtime(
            Arc::clone(&channel),
            runtime_with_submit_events(
                inputs,
                vec![Event::ToolCall(ToolCallEvent {
                    call_id: CallId("call-subagent".into()),
                    name: "sub_agent".into(),
                    input: json!({
                        "tool_name": "Task",
                        "prompt": "Inspect parser",
                        "child_session_ref": "child-session-1",
                        "child_thread_id": "child-thread-1",
                        "agent_id": "agent-1",
                        "nickname": "Parser",
                        "role": "explorer",
                        "status": "running"
                    }),
                })],
            ),
        );
        bind_test_workspace(&bot);

        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("10"),
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                reply_to: None,
                user: "u".into(),
                text: Some("spawn child".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let links = bot
            .state
            .subagent_links_for_workspace(&WorkspaceId::new("2"));
        assert_eq!(links.len(), 1);
        assert!(links[0].openable);
        assert_eq!(links[0].agent_id.as_deref(), Some("agent-1"));
        assert_eq!(links[0].nickname.as_deref(), Some("Parser"));
        assert_eq!(links[0].role.as_deref(), Some("explorer"));
        assert_eq!(links[0].model.as_deref(), None);
        assert_eq!(links[0].prompt.as_deref(), Some("Inspect parser"));
        assert_eq!(links[0].last_message.as_deref(), None);
        assert_eq!(links[0].state, SubAgentState::Running);
        assert_eq!(
            links[0]
                .child_provider_session_id
                .as_ref()
                .map(|id| id.as_str()),
            Some("child-session-1")
        );
        assert_eq!(
            links[0].child_native_ref.as_deref(),
            Some("child-session-1")
        );
        assert_eq!(links[0].label.as_deref(), Some("Parser"));

        let subagent_button =
            live_find_button(&channel, "subagent:c:").expect("subagent open button");
        assert!(
            subagent_button.len() <= 64,
            "callback_data must fit Telegram's 64-byte limit: {subagent_button}"
        );

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: Some(WorkspaceId::new("2")),
                user: "u".into(),
                data: subagent_button,
                source_message: MessageId::new("subagents"),
            })
            .await
            .unwrap();

        let child = bot
            .state
            .all()
            .into_iter()
            .find(|session| session.resume_ref.as_deref() == Some("child-session-1"))
            .expect("subagent child workspace");
        assert_ne!(child.workspace, WorkspaceId::new("2"));
        assert_eq!(child.title, "codex · Parser");
        assert!(
            child.live.is_some(),
            "subagent activation should prewarm resume"
        );
        let created = channel.created_workspaces.lock().unwrap();
        assert!(
            created
                .iter()
                .any(|(_, title, _)| title == "codex · Parser"),
            "subagent activation should create/open a dedicated child workspace: {created:?}"
        );
    }

    #[tokio::test]
    async fn panel_page_button_edits_source_message() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", tmp.path().join("codex").into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: None,
            user: "u".into(),
            data: "hist_page:0:5".into(),
            source_message: MessageId::new("42"),
        })
        .await
        .unwrap();

        assert_eq!(channel.sent.lock().unwrap().len(), 0);
        let edits = channel.edits.lock().unwrap();
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].0, "42");
        assert!(edits[0].1.body.contains("sessions"));
        assert!(!edits[0].1.buttons.is_empty());
    }

    #[tokio::test]
    async fn panel_tabs_provider_and_workspace_filters_edit_in_place() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session(
            &codex_home,
            "thread-project-a",
            "/tmp/project-a",
            "first project",
        );
        write_codex_history_session(
            &codex_home,
            "thread-project-b",
            "/tmp/project-b",
            "second project",
        );
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();
        let sent = channel.sent.lock().unwrap();
        let panel = sent.last().expect("entry panel");
        assert_eq!(
            panel.buttons[0]
                .iter()
                .map(|button| button.label.as_str())
                .collect::<Vec<_>>(),
            vec!["✓ Overview", "Workspaces", "Sessions"]
        );
        assert_eq!(
            panel.buttons[1]
                .iter()
                .map(|button| button.label.as_str())
                .collect::<Vec<_>>(),
            vec!["✓ All", "Codex"]
        );
        assert_eq!(panel.format, lucarne_channel::TextFormat::Markdown);
        let workspaces_button = panel.buttons[0][1].data.clone();
        drop(sent);

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "u".into(),
                data: workspaces_button,
                source_message: MessageId::new("panel-1"),
            })
            .await
            .unwrap();
        {
            let edits = channel.edits.lock().unwrap();
            let workspace_panel = &edits.last().expect("workspaces edit").1;
            assert!(workspace_panel.body.contains("workspaces"));
            assert!(workspace_panel.body.contains("/w1"));
            assert_eq!(workspace_button_count(workspace_panel), 0);
        }

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("workspace-filter"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "u".into(),
            text: Some("/w1".into()),
            attachments: Vec::new(),
        }))
        .await
        .unwrap();

        let sent = channel.sent.lock().unwrap();
        let sessions_panel = sent.last().expect("sessions panel");
        assert!(sessions_panel.body.contains("sessions"));
        assert!(sessions_panel.body.contains("project-a"));
        assert!(sessions_panel.body.contains("first project"));
        assert!(!sessions_panel.body.contains("second project"));
    }

    #[tokio::test]
    async fn panel_provider_filters_follow_scanned_history_not_runtime_catalog() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_copilot_history_session(tmp.path(), "copilot-history-only", "copilot prompt");
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let panel = sent.last().expect("entry panel");
        assert_eq!(
            panel.buttons[1]
                .iter()
                .map(|button| button.label.as_str())
                .collect::<Vec<_>>(),
            vec!["✓ All", "Copilot"]
        );
        assert!(panel.body.contains("copilot prompt"));
        assert!(panel.body.contains("copilot ·"));
        assert!(panel.body.contains("/a1  Copilot — copilot · history-only"));
        assert!(!panel.body.contains("OpenAI Codex"));
        assert!(
            !panel.body.contains("/h1"),
            "history rows for providers without runtime resume support must not expose /hN: {}",
            panel.body
        );
        drop(sent);

        bot.handle(ChannelEvent::Message(IncomingMessage {
            message_id: MessageId::new("history-only-agent"),
            chat: ChatId::new("100"),
            workspace: None,
            reply_to: None,
            user: "u".into(),
            text: Some("/a1".into()),
            attachments: Vec::new(),
        }))
        .await
        .expect("history-only /aN should render a visible unsupported notice");

        let sent = channel.sent.lock().unwrap();
        assert!(
            sent.last()
                .expect("history-only notice")
                .body
                .contains("history-only provider"),
            "history-only /aN notice should be visible: {:?}",
            sent.last()
        );
    }

    #[test]
    fn panel_provider_buttons_follow_scanned_provider_order() {
        let rows = panel_control_rows(
            PanelView::Overview,
            None,
            Revision::new(7),
            &["pi", "copilot"],
            &[
                HistoryProviderCatalogEntry {
                    provider_id: "copilot",
                    display_name: "Copilot",
                },
                HistoryProviderCatalogEntry {
                    provider_id: "pi",
                    display_name: "Pi",
                },
            ],
        );

        assert_eq!(
            rows[1]
                .iter()
                .map(|button| button.label.as_str())
                .collect::<Vec<_>>(),
            vec!["✓ All", "Pi", "Copilot"]
        );
    }

    #[tokio::test]
    async fn entry_panel_shows_pi_when_pi_history_is_scanned() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.into()),
            ("COPILOT_HOME", tmp.path().join("copilot").into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_pi_history_session(tmp.path(), "pi-panel-session", "pi prompt");
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let panel = sent.last().expect("entry panel");
        assert_eq!(
            panel.buttons[1]
                .iter()
                .map(|button| button.label.as_str())
                .collect::<Vec<_>>(),
            vec!["✓ All", "Pi"]
        );
        assert!(panel.body.contains("/a1  Pi — pi ·"));
        assert!(!panel.body.contains("OpenAI Codex"));
    }

    #[tokio::test]
    async fn panel_workspaces_page_paginates_workspace_buttons() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        for idx in 0..12 {
            write_codex_history_session(
                &codex_home,
                &format!("thread-project-{idx:02}"),
                &format!("/tmp/project-{idx:02}"),
                &format!("project {idx:02} prompt"),
            );
        }
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();
        let workspaces_button = {
            let sent = channel.sent.lock().unwrap();
            sent.last().expect("entry panel").buttons[0][1].data.clone()
        };

        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "u".into(),
                data: workspaces_button,
                source_message: MessageId::new("panel-1"),
            })
            .await
            .unwrap();

        let next_button = {
            let edits = channel.edits.lock().unwrap();
            let workspace_panel = &edits.last().expect("workspaces edit").1;
            assert!(workspace_panel.body.contains("workspaces (1-5 of 12)"));
            assert_eq!(workspace_button_count(workspace_panel), 0);
            workspace_panel
                .buttons
                .iter()
                .flatten()
                .find(|button| button.label == ">")
                .expect("workspace next button")
                .data
                .clone()
        };

        bot.handle(ChannelEvent::Button {
            chat: ChatId::new("100"),
            workspace: None,
            user: "u".into(),
            data: next_button,
            source_message: MessageId::new("panel-1"),
        })
        .await
        .unwrap();

        let edits = channel.edits.lock().unwrap();
        let workspace_panel = &edits.last().expect("next workspaces edit").1;
        assert!(workspace_panel.body.contains("workspaces (6-10 of 12)"));
        assert_eq!(workspace_button_count(workspace_panel), 0);
    }

    #[tokio::test]
    async fn panel_overview_hides_add_agent_and_help_actions() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", tmp.path().join("codex").into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();

        let sent = channel.sent.lock().unwrap();
        let panel = sent.last().expect("entry panel");
        let labels = panel
            .buttons
            .iter()
            .flatten()
            .map(|button| button.label.as_str())
            .collect::<Vec<_>>();
        assert!(
            !labels.iter().any(|label| label.contains("add agent")),
            "overview buttons should not expose add agent: {labels:?}"
        );
        assert!(
            !labels.iter().any(|label| label.contains("help")),
            "overview buttons should not expose help: {labels:?}"
        );
        assert!(
            !panel.body.contains("/add_agent"),
            "overview body should not promote add-agent setup: {}",
            panel.body
        );
    }

    #[tokio::test]
    async fn panel_workspace_sessions_new_button_creates_project_scoped_session() {
        let _env_lock = ENV_LOCK.lock().unwrap();
        let tmp = TempDir::new().unwrap();
        let codex_home = tmp.path().join("codex");
        let project = "/tmp/project-a";
        let _env = EnvGuard::set(&[
            ("HOME", tmp.path().into()),
            ("PATH", tmp.path().into()),
            ("CODEX_HOME", codex_home.clone().into()),
            ("CLAUDE_CONFIG_DIR", tmp.path().join("claude").into()),
            ("GEMINI_CONFIG_DIR", tmp.path().join("gemini").into()),
        ]);
        write_codex_history_session(&codex_home, "thread-project-a", project, "first project");
        let channel = Arc::new(RecordingChannel::default());
        let bot = test_bot(Arc::clone(&channel));

        Arc::clone(&bot).render_entry_panel().await.unwrap();
        let workspaces_button = {
            let sent = channel.sent.lock().unwrap();
            sent.last().expect("entry panel").buttons[0][1].data.clone()
        };
        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "u".into(),
                data: workspaces_button,
                source_message: MessageId::new("panel-1"),
            })
            .await
            .unwrap();
        bot.clone()
            .handle(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new("workspace-filter"),
                chat: ChatId::new("100"),
                workspace: None,
                reply_to: None,
                user: "u".into(),
                text: Some("/w1".into()),
                attachments: Vec::new(),
            }))
            .await
            .unwrap();

        let new_button = {
            let sent = channel.sent.lock().unwrap();
            let sessions_panel = sent.last().expect("workspace sessions panel");
            assert!(sessions_panel.body.contains("sessions"));
            sessions_panel
                .buttons
                .iter()
                .flatten()
                .find(|button| button.label == "＋ new")
                .expect("workspace sessions panel should expose new")
                .data
                .clone()
        };
        bot.clone()
            .handle(ChannelEvent::Button {
                chat: ChatId::new("100"),
                workspace: None,
                user: "u".into(),
                data: new_button,
                source_message: MessageId::new("panel-sessions"),
            })
            .await
            .unwrap();

        let created = bot
            .state
            .all()
            .into_iter()
            .find(|session| {
                session.provider_id == "codex"
                    && session.resume_ref.is_none()
                    && session.project_path.as_deref() == Some(Path::new(project))
            })
            .expect("new workspace session should keep selected cwd");
        assert_eq!(created.title, "codex · project-a");
        assert_eq!(
            channel.created_workspaces.lock().unwrap().as_slice(),
            [(
                "100".to_string(),
                "codex · project-a".to_string(),
                "1".to_string()
            )]
        );
    }

    fn workspace_button_count(panel: &OutgoingMessage) -> usize {
        panel
            .buttons
            .iter()
            .flatten()
            .filter(|button| button.data.starts_with("panel_workspace:"))
            .count()
    }

    #[test]
    fn compact_path_preserves_important_tail_segments() {
        let path = "/Volumes/Data/opensource/conductor/lucarnex";

        let rendered = compact_path(path, 28);

        assert_eq!(rendered, "…/conductor/lucarnex");
        assert!(!rendered.contains("/Volumes/Data"));
    }

    #[test]
    fn compact_path_removes_noisy_absolute_prefix_even_when_under_limit() {
        let path = "/Volumes/Data/opensource/conductor/lucarnex";

        let rendered = compact_path(path, 58);

        assert_eq!(rendered, "…/opensource/conductor/lucarnex");
        assert!(!rendered.starts_with("/Volumes"));
    }

    #[test]
    fn agent_notification_uses_panel_compact_cwd() {
        let session = WorkSession {
            workspace: WorkspaceId::new("topic-1"),
            chat: ChatId::new("chat-1"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/Volumes/Data/opensource/conductor/lucarnex")),
            title: "lucarnex".into(),
            live: None,
            resume_ref: None,
        };

        let msg = render_agent_notification(&session, Some("thread-1"), "done");

        assert!(msg.body.contains("cwd: `…/opensource/conductor/lucarnex`"));
        assert!(!msg.body.contains("/Volumes/Data"));
    }

    #[test]
    fn relative_time_uses_compact_panel_units() {
        assert_eq!(relative_time_from(1_000, 959, ""), "41s ago");
        assert_eq!(relative_time_from(3_600, 3_000, ""), "10m ago");
        assert_eq!(relative_time_from(10_800, 3_600, ""), "2h ago");
        assert_eq!(
            relative_time_from(4 * 86_400 + 3 * 3_600, 0, "missing"),
            "missing"
        );
        assert_eq!(relative_time_from(4 * 86_400 + 3 * 3_600, 0, ""), "unknown");
        assert_eq!(
            relative_time_from(4 * 86_400 + 3 * 3_600, 1, ""),
            "4d 2h ago"
        );
        assert_eq!(relative_time_from(75 * 86_400, 0, "fallback"), "fallback");
        assert_eq!(relative_time_from(75 * 86_400, 86_400, ""), "2mo ago");
        assert_eq!(relative_time_from(430 * 86_400, 0, ""), "unknown");
        assert_eq!(relative_time_from(430 * 86_400, 1, ""), "1y 2mo ago");
    }

    #[test]
    fn sessions_body_uses_readable_blocks_and_copyable_session_code() {
        let now = current_unix();
        let entry = HistoryEntry {
            provider_id: "codex",
            session_id: "thread-123".into(),
            session_path: PathBuf::from("/tmp/rollout-thread-123.jsonl"),
            cwd: Some(
                "/Volumes/Data/noisy/prefix/that/should/drop/opensource/conductor/lucarnex".into(),
            ),
            summary: "summary".into(),
            last_active_unix: now - 65,
            last_active_display: "2026-05-04T00:00:00Z".into(),
        };
        let second_entry = HistoryEntry {
            provider_id: "codex",
            session_id: "thread-456".into(),
            session_path: PathBuf::from("/tmp/rollout-thread-456.jsonl"),
            cwd: Some("/Volumes/Data/opensource/conductor/lucarnex".into()),
            summary: "second\nline".into(),
            last_active_unix: now - 3_600,
            last_active_display: "2026-05-04T00:00:00Z".into(),
        };
        let mut body = String::new();

        render_sessions_body(&mut body, &[entry, second_entry], 2, 2, 0, &["codex"]);

        assert!(body.contains("/h1  codex · 1m ago\n"));
        assert!(body.contains("     💬 summary\n"));
        assert!(body.contains("     📁 "));
        assert!(body.contains("…/"));
        assert!(body.contains("/opensource/conductor/lucarnex"));
        assert!(!body.contains("/Volumes/Data/noisy/prefix"));
        assert!(body.contains("      `thread-123`\n"));
        assert!(body.contains("`thread-123`\n\n/h2  codex · 1h ago\n"));
        assert!(!body.contains("```"));
        assert!(body.contains("     💬 second line\n"));
        assert!(!body.contains("second\nline"));
        assert!(!body.contains("     msg  "));
        assert!(!body.contains("     cwd  "));
        assert!(!body.contains("     sid  "));
        assert!(!body.contains("session:"));
    }

    #[test]
    fn workspaces_body_uses_activity_and_session_density_blocks() {
        let now = current_unix();
        let workspaces = vec![
            HistoryWorkspace {
                cwd: PathBuf::from("/Volumes/Data/opensource/conductor/lucarnex"),
                display_name: "lucarnex".into(),
                provider_ids: vec!["claude", "codex"],
                session_count: 858,
                last_active_unix: now - 2 * 3_600,
                last_active_display: "2026-05-04T00:00:00Z".into(),
            },
            HistoryWorkspace {
                cwd: PathBuf::from("/Volumes/Data/crypto/feeds"),
                display_name: "feeds".into(),
                provider_ids: vec!["codex"],
                session_count: 1,
                last_active_unix: now - 86_400,
                last_active_display: "2026-05-04T00:00:00Z".into(),
            },
        ];
        let mut body = String::new();

        render_workspaces_body(&mut body, &workspaces, 2, 2, 0);

        assert!(body.contains("/w1  lucarnex · 2h ago\n"));
        assert!(!body.contains("     ⏱ "));
        assert!(body.contains("     🧾 858 sessions · 🤖 claude, codex\n"));
        assert!(body.contains("     📁 …/opensource/conductor/lucarnex\n"));
        assert!(body.contains("…/opensource/conductor/lucarnex\n\n/w2  feeds · 1d ago\n"));
        assert!(body.contains("     🧾 1 session · 🤖 codex\n"));
        assert!(!body.contains(" — "));
    }

    #[test]
    fn telegram_bot_does_not_expose_manual_agent_json_command() {
        assert!(!telegram_menu_commands()
            .iter()
            .any(|command| command.command == "add_agent"));
    }

    #[test]
    fn agents_body_keeps_rows_compact() {
        let agents = vec![
            agents::AgentEntry {
                display_name: "Codex".into(),
                provider_id: "codex".into(),
                command: "codex".into(),
                available: true,
            },
            agents::AgentEntry {
                display_name: "Gemini".into(),
                provider_id: "gemini".into(),
                command: "gemini".into(),
                available: false,
            },
        ];
        let mut body = String::new();

        render_agents_body(&mut body, &agents);

        assert!(body.contains("/a1  Codex — codex · available\n/a2  Gemini"));
        assert!(!body.contains("available\n\n/a2"));
    }

    #[test]
    fn overview_body_limits_visible_agents_and_attached_sessions() {
        let agents = (0..6)
            .map(|idx| agents::AgentEntry {
                display_name: format!("Agent {idx}"),
                provider_id: format!("provider-{idx}"),
                command: format!("agent-{idx}"),
                available: true,
            })
            .collect::<Vec<_>>();
        let attached = (0..6)
            .map(|idx| AttachedSessionEntry {
                title: format!("attached-{idx}"),
                provider_id: "codex",
                project_path: Some(PathBuf::from(format!("/tmp/attached-{idx}"))),
            })
            .collect::<Vec<_>>();
        let mut body = String::new();

        render_overview_body(&mut body, &agents, &[], &attached, &[], 0, 0, 0, &[]);

        assert!(body.contains("agents: 6 · attached: 6"));
        assert!(body.contains("/a5  Agent 4"));
        assert!(!body.contains("/a6  Agent 5"));
        assert!(body.contains("attached-4"));
        assert!(!body.contains("attached-5"));
    }

    #[test]
    fn overview_renders_recent_observed_sessions_without_management_commands() {
        let now = current_unix();
        let observed = vec![lucarne::core_service::ObservedAgentSession {
            workspace_id: lucarne::control_plane::WorkspaceId::new("codex:resume:observed"),
            provider_id: "codex",
            provider_session_id: lucarne::control_plane::ProviderSessionId::new(
                "codex:observed-thread",
            ),
            native_resume_ref: "observed-thread".into(),
            title: "fix checkout bug".into(),
            cwd: Some(PathBuf::from("/Volumes/Data/opensource/conductor/lucarnex")),
            session_path: PathBuf::from("/tmp/rollout-observed-thread.jsonl"),
            last_active_unix: now - 65,
            last_active_display: "2026-05-12T00:00:00Z".into(),
            observed_pid: None,
        }];
        let mut body = String::new();

        render_overview_body(&mut body, &[], &[], &[], &observed, 0, 0, 0, &[]);

        assert!(body.contains("agents: 0 · attached: 0 · observed: 1 · sessions: 0"));
        assert!(body.contains("recent observed"));
        assert!(body.contains("• fix checkout bug — codex · 1m ago\n"));
        assert!(body.contains("     📁 …/opensource/conductor/lucarnex\n"));
        assert!(body.contains("      `observed-thread`\n"));
        assert!(!body.contains("/o1"));
    }

    #[test]
    fn overview_does_not_render_saved_workspace_records() {
        let mut body = String::new();

        render_overview_body(&mut body, &[], &[], &[], &[], 0, 0, 0, &[]);

        assert!(!body.contains("open workspaces"));
        assert!(!body.contains("/w1"));
    }

    #[test]
    fn overview_renders_runtime_attached_sessions_without_workspace_commands() {
        let mut body = String::new();
        let attached = vec![AttachedSessionEntry {
            title: "lucarnex".into(),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/Volumes/Data/opensource/conductor/lucarnex")),
        }];

        render_overview_body(&mut body, &[], &[], &attached, &[], 0, 0, 0, &[]);

        assert!(body.contains("attached sessions"));
        assert!(body.contains("lucarnex — codex"));
        assert!(!body.contains("open workspaces"));
        assert!(!body.contains("/w1"));
    }

    #[test]
    fn parse_panel_aliases() {
        assert_eq!(parse_entry_command("/panel"), Some(EntryAction::Panel));
        assert_eq!(parse_entry_command("/start"), Some(EntryAction::Panel));
        assert_eq!(parse_entry_command("/refresh"), Some(EntryAction::Panel));
        assert_eq!(
            parse_entry_command("/clear_workspaces"),
            Some(EntryAction::ClearWorkspaces)
        );
        assert_eq!(parse_entry_command("/clear_records"), None);
        assert_eq!(
            parse_entry_command("/reset_notifications"),
            Some(EntryAction::ResetNotifications)
        );
        assert_eq!(parse_entry_command("/reset_notification_topic"), None);
        assert_eq!(
            parse_entry_command("/reset_notifications@lucarnebot"),
            Some(EntryAction::ResetNotifications)
        );
    }

    #[test]
    fn telegram_menu_hides_pagination_and_refresh_shortcuts() {
        let names = telegram_menu_commands()
            .iter()
            .map(|command| command.command)
            .collect::<Vec<_>>();

        assert!(!names.contains(&"next"));
        assert!(!names.contains(&"prev"));
        assert!(!names.contains(&"refresh"));
        assert!(names.contains(&"reset_notifications"));
        assert!(!names.contains(&"reset_notification_topic"));
        assert!(names.contains(&"clear_workspaces"));
        assert!(!names.contains(&"clear_records"));
    }

    #[test]
    fn parse_indexed() {
        assert_eq!(parse_entry_command("/a1"), Some(EntryAction::Agent(1)));
        assert_eq!(parse_entry_command("/h12"), Some(EntryAction::History(12)));
        assert_eq!(parse_entry_command("/w3"), Some(EntryAction::Workspace(3)));
    }

    #[test]
    fn parse_rejects_bad_index() {
        assert_eq!(parse_entry_command("/a0"), None);
        assert_eq!(parse_entry_command("/aX"), None);
        assert_eq!(parse_entry_command("/a"), None);
    }

    #[test]
    fn parse_strips_bot_mention() {
        assert_eq!(
            parse_entry_command("/h2@lucarnebot"),
            Some(EntryAction::History(2))
        );
    }

    #[test]
    fn parse_topic_slash_strips_bot_mention_and_args() {
        assert_eq!(
            parse_topic_slash_command("/model@lucarnebot reason high"),
            Some(TopicSlashCommand {
                name: "model".into(),
                args: "reason high".into(),
                values: serde_json::Value::Null,
            })
        );
        assert_eq!(
            parse_topic_slash_command("/models@lucarnebot reason high"),
            Some(TopicSlashCommand {
                name: "model".into(),
                args: "reason high".into(),
                values: serde_json::Value::Null,
            })
        );
        assert_eq!(
            parse_topic_slash_command("security-review urgent"),
            Some(TopicSlashCommand {
                name: "security-review".into(),
                args: "urgent".into(),
                values: serde_json::Value::Null,
            })
        );
    }

    #[test]
    fn command_query_model_suggestions_include_reasoning_effort() {
        let results = command_query_results_from_catalog("model gpt5.5 m", &test_command_catalog());
        assert!(results
            .iter()
            .any(|result| result.message_text == "/model gpt-5.5 medium"));
        assert!(!results
            .iter()
            .any(|result| result.message_text.starts_with("/model reason ")));
    }

    #[test]
    fn telegram_menu_commands_cover_telegram_and_session_trait_commands() {
        let commands: std::collections::HashSet<_> = telegram_menu_commands()
            .iter()
            .map(|cmd| cmd.command)
            .collect();

        for command in [
            "start",
            "panel",
            "help",
            "config",
            "commands",
            "rename",
            "model",
            "permissions",
            "skills",
            "status",
            "kill",
            "new",
            "quit",
            "fork",
        ] {
            assert!(
                commands.contains(command),
                "missing Telegram menu command: {command}"
            );
        }
    }

    #[test]
    fn telegram_menu_commands_are_valid_for_bot_api() {
        let commands = telegram_menu_commands();
        assert!(!commands.is_empty());
        assert!(commands.len() <= 100);

        for cmd in commands {
            assert!((1..=32).contains(&cmd.command.len()), "{cmd:?}");
            assert!((1..=256).contains(&cmd.description.len()), "{cmd:?}");
            assert!(
                cmd.command
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "{cmd:?}"
            );
        }
    }

    #[test]
    fn history_resume_ref_uses_provider_session_id_not_file_path() {
        let entry = HistoryEntry {
            provider_id: "codex",
            session_id: "thread-123".into(),
            session_path: PathBuf::from("/tmp/rollout-thread-123.jsonl"),
            cwd: Some("/tmp/project".into()),
            summary: "summary".into(),
            last_active_unix: 1,
            last_active_display: "1970-01-01T00:00:01Z".into(),
        };

        assert_eq!(
            runtime_resume_ref_from_history(&entry).unwrap(),
            "thread-123"
        );
    }

    #[test]
    fn history_resume_ref_rejects_missing_provider_session_id() {
        let entry = HistoryEntry {
            provider_id: "codex",
            session_id: String::new(),
            session_path: PathBuf::from("/tmp/rollout-missing-id.jsonl"),
            cwd: Some("/tmp/project".into()),
            summary: "summary".into(),
            last_active_unix: 1,
            last_active_display: "1970-01-01T00:00:01Z".into(),
        };

        assert!(runtime_resume_ref_from_history(&entry).is_err());
    }

    #[test]
    fn resume_workspace_id_does_not_reuse_provider_session_ref() {
        let workspace = workspace_id_for_resume("codex", "thread-123");

        assert!(workspace.as_str().starts_with("codex:resume:"));
        assert!(!workspace.as_str().contains("thread-123"));
        assert_eq!(
            workspace,
            workspace_id_for_resume("codex", "thread-123"),
            "resume workspace id must be stable across restarts"
        );
        assert_ne!(
            workspace,
            WorkspaceId::new("codex:thread-123"),
            "workspace id must not be the provider session ref"
        );
    }

    #[test]
    fn resume_request_preserves_history_cwd() {
        let session = WorkSession {
            workspace: WorkspaceId::new("ws"),
            chat: ChatId::new("chat"),
            provider_id: "codex",
            project_path: Some(PathBuf::from("/tmp/project")),
            title: "codex".into(),
            live: None,
            resume_ref: Some("thread-123".into()),
        };

        let req = resume_request_for_session(&session, "thread-123");

        assert_eq!(req.session_ref.0.as_str(), "thread-123");
        assert_eq!(req.args["cwd"], "/tmp/project");
    }

    #[test]
    fn parse_unrelated_is_none() {
        assert_eq!(parse_entry_command("hello"), None);
    }
}
