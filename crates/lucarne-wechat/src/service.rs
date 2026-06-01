use smol_str::SmolStr;

use crate::intervention::{
    intervention_request_id, parse_intervention_text_response_zh, render_intervention_markdown_zh,
};

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::{stream::BoxStream, StreamExt};
use lucarne::{
    agent_runtime::{
        AgentContextUsage, AgentInput, AgentTokenUsage, Attachment as AgentAttachment,
        Event as AgentEvent, InterventionRequest, MessageRole,
    },
    control_plane::{
        MessageSessionBinding, ProviderSessionId, StatusSnapshot, TurnId, TurnSource,
        WorkspaceBinding, WorkspaceId,
    },
    core_service::{
        AgentResourceEntry, AgentResourceScope, AgentResourceSnapshot, CoreEvent, KillAgentReport,
        KillAgentRequest, KillAgentTarget, LucarneCore, ResumeWorkspaceRequest, SubmitTurnRequest,
    },
};
use lucarne_adapter::{
    GlobalConfigPersistence, GlobalConfigUpdate, SystemNotification, SystemNotificationBus,
    SystemNotificationReceiver,
};
use lucarne_channel::{
    agent_message::{compact_path, format_cost_duration},
    robust::retry_attachment_delivery,
};
use tokio::{sync::oneshot, time::MissedTickBehavior};
use tracing::{debug, warn};
use wechat_ilink::{UserInteractionReason, WechatContext};

const DEFAULT_TYPING_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(4);
const DEFAULT_PENDING_REPLY_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const STARTUP_HELP_MAX_RETRIES: u8 = 3;
const MAX_PENDING_REPLIES: usize = 10;
const MAX_PENDING_NOTIFICATIONS: usize = 10;
const MAX_PENDING_INTERVENTIONS: usize = 10;
const WECHAT_STALE_NOTIFICATION: &str = "⚠️ 这条通知已无法路由，请重新引用最新的 agent 通知。";
const WECHAT_REPLY_REQUIRED: &str = "💬 请引用一条 agent 通知后再回复，以继续对应会话。";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WechatSendReceipt {
    pub message_ids: Vec<String>,
    pub visible_texts: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WechatIncoming {
    pub message_id: String,
    pub user_id: String,
    pub text: String,
    pub quoted_message_id: Option<String>,
    pub quoted_text: Option<String>,
    pub(crate) sdk_message: Option<wechat_ilink::IncomingMessage>,
}

impl WechatIncoming {
    pub fn new(
        message_id: impl Into<String>,
        user_id: impl Into<String>,
        text: impl Into<String>,
        quoted_message_id: Option<impl Into<String>>,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            user_id: user_id.into(),
            text: text.into(),
            quoted_message_id: quoted_message_id.map(Into::into),
            quoted_text: None,
            sdk_message: None,
        }
    }

    pub(crate) fn from_sdk(message: wechat_ilink::IncomingMessage) -> Self {
        let message_id = message
            .message_id
            .clone()
            .unwrap_or_else(|| message.client_id.clone());
        let quoted_message_id = message
            .quoted
            .as_ref()
            .and_then(|quoted| quoted.message_id.clone());
        let quoted_text = message
            .quoted
            .as_ref()
            .and_then(|quoted| quoted.text.clone());
        debug!(
            target: "lucarne_wechat::service",
            message_id = %message_id,
            client_id = %message.client_id,
            user_id = %message.user_id,
            quoted_message_id = quoted_message_id.as_deref(),
            quoted = ?message.quoted,
            raw = ?message.raw,
            "wechat incoming message parsed"
        );
        Self {
            message_id,
            user_id: message.user_id.clone(),
            text: message.text.clone(),
            quoted_message_id,
            quoted_text,
            sdk_message: Some(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WechatUserInteractionRequest {
    pub account_key: String,
    pub user_id: Option<String>,
    pub reason: UserInteractionReason,
}

#[derive(Debug, thiserror::Error)]
pub enum WechatError {
    #[error("wechat transport: {0}")]
    Transport(String),
    #[error("wechat rate limited: {message}; retry after {}s", retry_after.as_secs())]
    RateLimited {
        retry_after: Duration,
        message: String,
    },
    #[error("wechat message context missing for {0}")]
    MissingMessageContext(String),
    #[error("core: {0}")]
    Core(String),
}

#[async_trait]
pub trait WechatTransport: Send + Sync {
    fn subscribe(&self) -> BoxStream<'static, WechatIncoming>;
    fn subscribe_user_interactions(&self) -> BoxStream<'static, WechatUserInteractionRequest>;
    async fn context_for_user(&self, user_id: &str) -> Result<Option<WechatContext>, WechatError>;
    async fn send(
        &self,
        context: &WechatContext,
        text: &str,
    ) -> Result<WechatSendReceipt, WechatError>;
    async fn reply(
        &self,
        message: &WechatIncoming,
        text: &str,
    ) -> Result<WechatSendReceipt, WechatError>;
    async fn send_typing(&self, context: &WechatContext) -> Result<(), WechatError>;
    async fn send_attachment(
        &self,
        _context: &WechatContext,
        _attachment: &AgentAttachment,
    ) -> Result<WechatSendReceipt, WechatError> {
        Err(WechatError::Transport(
            "wechat attachment send unsupported".into(),
        ))
    }
    async fn reply_attachment(
        &self,
        _message: &WechatIncoming,
        _attachment: &AgentAttachment,
    ) -> Result<WechatSendReceipt, WechatError> {
        Err(WechatError::Transport(
            "wechat attachment reply unsupported".into(),
        ))
    }
    async fn stop(&self) -> Result<(), WechatError>;
}

#[derive(Clone)]
pub struct WechatServiceOptions {
    pub initial_user_ids: Vec<String>,
    pub typing_keepalive_interval: Duration,
    pub pending_reply_retry_interval: Duration,
    pub rate_limit_interaction_prompt: Option<String>,
    pub global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
}

impl Default for WechatServiceOptions {
    fn default() -> Self {
        Self {
            initial_user_ids: Vec::new(),
            typing_keepalive_interval: DEFAULT_TYPING_KEEPALIVE_INTERVAL,
            pending_reply_retry_interval: DEFAULT_PENDING_REPLY_RETRY_INTERVAL,
            rate_limit_interaction_prompt: None,
            global_config_persistence: None,
        }
    }
}

pub struct WechatNotificationService<T> {
    core: Arc<LucarneCore>,
    transport: Arc<T>,
    state: Mutex<WechatState>,
    rate_limiter: Arc<WechatRateLimiter>,
    typing_keepalive_interval: Duration,
    pending_reply_retry_interval: Duration,
    context_expiry_reminder: Option<crate::adapter::WechatContextExpiryReminderConfig>,
    rate_limit_interaction_prompt: Option<SmolStr>,
    global_config_persistence: Option<Arc<dyn GlobalConfigPersistence>>,
}

#[derive(Default)]
struct WechatState {
    users: BTreeSet<String>,
    startup_help_retries: HashMap<String, u8>,
    pending_replies: HashMap<TurnId, WechatPendingReply>,
    pending_reply_order: VecDeque<TurnId>,
    pending_notifications: VecDeque<WechatPendingNotification>,
    pending_interventions: HashMap<SmolStr, WechatPendingIntervention>,
    pending_intervention_order: VecDeque<SmolStr>,
    typing_cancellations: HashMap<WorkspaceId, oneshot::Sender<()>>,
}

#[derive(Default)]
struct WechatRateLimiter {
    until: Mutex<Option<Instant>>,
}

impl WechatRateLimiter {
    fn remaining(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut until = self.until.lock().expect("wechat rate limiter lock");
        match *until {
            Some(deadline) => match deadline.checked_duration_since(now) {
                Some(remaining) if !remaining.is_zero() => Some(remaining),
                _ => {
                    *until = None;
                    None
                }
            },
            None => None,
        }
    }

    fn defer(&self, retry_after: Duration) -> Duration {
        let now = Instant::now();
        let new_deadline = now + retry_after;
        let mut until = self.until.lock().expect("wechat rate limiter lock");
        if until.map_or(true, |deadline| deadline < new_deadline) {
            *until = Some(new_deadline);
        }
        (*until)
            .and_then(|deadline| deadline.checked_duration_since(now))
            .unwrap_or_default()
    }

    fn clear(&self) {
        *self.until.lock().expect("wechat rate limiter lock") = None;
    }
}

#[derive(Clone)]
struct WechatPendingNotification {
    workspace_id: WorkspaceId,
    user_id: String,
    content: WechatPendingNotificationContent,
    provider_session_id: ProviderSessionId,
}

#[derive(Clone)]
enum WechatPendingNotificationContent {
    Text(SmolStr),
    Attachment(AgentAttachment),
}

#[derive(Clone)]
struct WechatPendingReply {
    workspace_id: WorkspaceId,
    message: WechatIncoming,
    started_at: Instant,
    latest_assistant_text: Option<SmolStr>,
    pending_attachments: Vec<AgentAttachment>,
    terminal: Option<PendingReplyTerminal>,
}

#[derive(Clone)]
struct WechatPendingIntervention {
    workspace_id: WorkspaceId,
    turn_id: Option<TurnId>,
    request: InterventionRequest,
    user_ids: BTreeSet<String>,
    prompt_message_ids: BTreeSet<String>,
    prompt_quote_ids: BTreeSet<String>,
}

#[derive(Clone)]
enum PendingInterventionLookup {
    None,
    One(WechatPendingIntervention),
    Multiple,
}

#[derive(Clone)]
enum PendingReplyTerminal {
    Completed,
    Failed(SmolStr),
}

impl<T> WechatNotificationService<T>
where
    T: WechatTransport + 'static,
{
    pub fn new(core: Arc<LucarneCore>, transport: Arc<T>, options: WechatServiceOptions) -> Self {
        Self {
            core,
            transport,
            state: Mutex::new(WechatState {
                users: options.initial_user_ids.into_iter().collect(),
                startup_help_retries: HashMap::new(),
                pending_replies: HashMap::new(),
                pending_reply_order: VecDeque::new(),
                pending_notifications: VecDeque::new(),
                pending_interventions: HashMap::new(),
                pending_intervention_order: VecDeque::new(),
                typing_cancellations: HashMap::new(),
            }),
            rate_limiter: Arc::new(WechatRateLimiter::default()),
            typing_keepalive_interval: options.typing_keepalive_interval,
            pending_reply_retry_interval: options.pending_reply_retry_interval,
            context_expiry_reminder: None,
            rate_limit_interaction_prompt: options.rate_limit_interaction_prompt.map(SmolStr::from),
            global_config_persistence: options.global_config_persistence,
        }
    }

    /// Attach a context expiry reminder config. When set, the service
    /// sends reminder prompts on its retry tick.
    pub fn with_context_expiry_reminder(
        mut self,
        config: crate::adapter::WechatContextExpiryReminderConfig,
    ) -> Self {
        self.context_expiry_reminder = Some(config);
        self
    }

    pub fn transport(&self) -> Arc<T> {
        Arc::clone(&self.transport)
    }

    pub async fn run_until_shutdown(
        &self,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), WechatError> {
        let noop_system_notifications = SystemNotificationBus::new(1);
        let system_notifications = noop_system_notifications.subscribe();
        let result = self
            .run_until_shutdown_with_system_notifications(shutdown, system_notifications)
            .await;
        drop(noop_system_notifications);
        result
    }

    pub async fn run_until_shutdown_with_system_notifications(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        mut system_notifications: SystemNotificationReceiver,
    ) -> Result<(), WechatError> {
        let mut core_events = self.core.watch_events();
        let mut incoming = self.transport.subscribe();
        let mut user_interactions = self.transport.subscribe_user_interactions();
        let mut pending_retry = tokio::time::interval(self.pending_reply_retry_interval);
        let mut system_notifications_open = true;
        pending_retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
        self.send_startup_help().await;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                event = core_events.recv() => {
                    match event {
                        Ok(event) => self.handle_core_event(event).await?,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(target: "lucarne_wechat::service", skipped, "core event watch lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
                notification = system_notifications.recv(), if system_notifications_open => {
                    match notification {
                        Ok(notification) => {
                            if let Err(err) = self.deliver_system_notification(notification).await {
                                warn!(
                                    target: "lucarne_wechat::service",
                                    error = %err,
                                    "wechat system notification ignored after delivery error"
                                );
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(target: "lucarne_wechat::service", skipped, "system notification watch lagged");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            system_notifications_open = false;
                            warn!(target: "lucarne_wechat::service", "system notification watch closed");
                        }
                    }
                }
                _ = pending_retry.tick() => {
                    self.retry_pending_startup_help(true).await;
                    self.retry_pending_notifications().await?;
                    self.retry_completed_pending_replies().await?;
                }
                message = incoming.next() => {
                    let Some(message) = message else {
                        return Ok(());
                    };
                    if let Err(err) = self.handle_incoming(message).await {
                        warn!(
                            target: "lucarne_wechat::service",
                            error = %err,
                            "wechat incoming message ignored after handler error"
                        );
                    }
                }
                request = user_interactions.next() => {
                    let Some(request) = request else {
                        return Ok(());
                    };
                    if let Err(err) = self.handle_user_interaction_request(request).await {
                        warn!(
                            target: "lucarne_wechat::service",
                            error = %err,
                            "wechat user interaction request ignored after handler error"
                        );
                    }
                }
            }
        }
    }

    async fn deliver_system_notification(
        &self,
        notification: SystemNotification,
    ) -> Result<(), WechatError> {
        match notification {
            SystemNotification::UpdateAvailable(update) => {
                self.deliver_update_notification(&update.body_markdown)
                    .await
            }
        }
    }

    async fn deliver_update_notification(&self, body: &str) -> Result<(), WechatError> {
        let users = self.notification_users();
        if users.is_empty() {
            warn!(
                target: "lucarne_wechat::service",
                "wechat update notification skipped because no notification users are configured or remembered"
            );
            return Ok(());
        }

        if let Some(remaining) = self.rate_limit_remaining() {
            warn!(
                target: "lucarne_wechat::service",
                remaining_ms = remaining.as_millis() as u64,
                "wechat update notification skipped by active rate limit"
            );
            return Ok(());
        }

        for user_id in users {
            let context = match self.transport.context_for_user(&user_id).await {
                Ok(Some(context)) => context,
                Ok(None) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        "wechat update notification skipped: no stored context for user"
                    );
                    continue;
                }
                Err(WechatError::RateLimited {
                    retry_after,
                    message,
                }) => {
                    self.record_rate_limit(retry_after, &message);
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        retry_after_secs = retry_after.as_secs(),
                        error = %message,
                        "wechat update notification context lookup rate limited"
                    );
                    return Ok(());
                }
                Err(WechatError::Transport(err)) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat update notification context lookup failed"
                    );
                    continue;
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat update notification context lookup failed"
                    );
                    continue;
                }
            };

            match self.transport.send(&context, body).await {
                Ok(receipt) => {
                    debug!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        message_ids = ?receipt.message_ids,
                        bytes = body.len(),
                        "wechat update notification sent"
                    );
                }
                Err(WechatError::RateLimited {
                    retry_after,
                    message,
                }) => {
                    self.record_rate_limit(retry_after, &message);
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        retry_after_secs = retry_after.as_secs(),
                        error = %message,
                        "wechat update notification rate limited"
                    );
                    return Ok(());
                }
                Err(WechatError::Transport(err)) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat update notification send failed"
                    );
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat update notification send failed"
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn handle_core_event(&self, event: CoreEvent) -> Result<(), WechatError> {
        match event {
            CoreEvent::TimelineEvent {
                workspace_id,
                turn_id,
                event: AgentEvent::InterventionRequest(request),
            } => {
                self.deliver_intervention_request(&workspace_id, turn_id.as_ref(), request)
                    .await
            }
            CoreEvent::TimelineEvent {
                turn_id: Some(turn_id),
                event:
                    AgentEvent::Message(lucarne::agent_runtime::MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        streaming: false,
                    }),
                ..
            } if self.record_pending_assistant_message(&turn_id, text.as_ref()) => Ok(()),
            CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: Some(turn_id),
                event: AgentEvent::Attachment(attachment),
            } if self.pending_reply(&turn_id).is_some() => {
                self.deliver_pending_reply_attachment(&workspace_id, &turn_id, attachment)
                    .await
            }
            CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: Some(turn_id),
                event: AgentEvent::TurnCompleted(_),
            } => {
                let Some(reply_target) = self.mark_pending_reply_completed(&turn_id) else {
                    return Ok(());
                };
                self.deliver_pending_reply(&workspace_id, &turn_id, reply_target)
                    .await
            }
            CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: Some(turn_id),
                event: AgentEvent::TurnFailed(failed),
            } => {
                let text = if failed.error.is_empty() {
                    "agent 执行失败".to_string()
                } else {
                    format!("agent 执行失败：{}", failed.error)
                };
                let Some(reply_target) = self.mark_pending_reply_failed(&turn_id, text) else {
                    return Ok(());
                };
                self.deliver_pending_reply(&workspace_id, &turn_id, reply_target)
                    .await
            }
            CoreEvent::TimelineEvent {
                workspace_id,
                event:
                    AgentEvent::Message(lucarne::agent_runtime::MessageEvent {
                        role: MessageRole::Assistant,
                        text,
                        streaming: false,
                    }),
                ..
            } => {
                self.deliver_assistant_message(&workspace_id, text.as_ref())
                    .await
            }
            CoreEvent::TimelineEvent {
                workspace_id,
                event: AgentEvent::Attachment(attachment),
                ..
            } => {
                self.deliver_attachment_notification(&workspace_id, attachment)
                    .await
            }
            _ => Ok(()),
        }
    }

    pub async fn handle_incoming(&self, message: WechatIncoming) -> Result<(), WechatError> {
        self.rate_limiter.clear();
        self.remember_user(&message.user_id);
        let text = message.text.trim();
        if let Some(config) = parse_config_command(text) {
            self.handle_config_command(&message, config).await?;
            return Ok(());
        }
        if let Some(command) = parse_slash_command(text) {
            self.handle_slash_command(&message, command).await?;
            return Ok(());
        }
        if text.is_empty() {
            return Ok(());
        }
        if self
            .handle_pending_intervention_response(&message, text)
            .await?
        {
            return Ok(());
        }
        let has_quote = message.quoted_message_id.is_some() || message.quoted_text.is_some();
        let Some(binding) = self.resolve_quoted_binding(&message) else {
            let body = if has_quote {
                WECHAT_STALE_NOTIFICATION.to_string()
            } else {
                format!("{WECHAT_REPLY_REQUIRED}\n\n{}", render_wechat_help())
            };
            self.transport.reply(&message, &body).await?;
            return Ok(());
        };
        let Some(workspace) = self
            .core
            .workspace_for_provider_session(&binding.provider_session_id)
        else {
            self.transport
                .reply(&message, WECHAT_STALE_NOTIFICATION)
                .await?;
            return Ok(());
        };
        self.continue_session(workspace.workspace_id, message).await
    }

    async fn send_startup_help(&self) {
        let users = self.notification_users();
        if users.is_empty() {
            return;
        }
        self.remember_pending_startup_help_users(users);
        self.retry_pending_startup_help(false).await;
    }

    async fn retry_pending_startup_help(&self, count_failures_as_retries: bool) {
        let body = render_wechat_startup_help();
        for user_id in self.pending_startup_help_users() {
            if let Some(remaining) = self.rate_limit_remaining() {
                debug!(
                    target: "lucarne_wechat::service",
                    user_id = %user_id,
                    remaining_ms = remaining.as_millis() as u64,
                    "wechat startup help skipped by active rate limit"
                );
                continue;
            }
            let context = match self.transport.context_for_user(&user_id).await {
                Ok(Some(context)) => context,
                Ok(None) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        "wechat startup help skipped: no stored context for user"
                    );
                    if count_failures_as_retries {
                        self.record_startup_help_retry_failure(&user_id);
                    }
                    continue;
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat startup help context lookup failed"
                    );
                    if count_failures_as_retries {
                        self.record_startup_help_retry_failure(&user_id);
                    }
                    continue;
                }
            };
            match self.transport.send(&context, &body).await {
                Ok(receipt) => {
                    self.forget_pending_startup_help(&user_id);
                    debug!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        message_ids = ?receipt.message_ids,
                        "wechat startup help sent"
                    );
                }
                Err(WechatError::RateLimited {
                    retry_after,
                    message,
                }) => {
                    self.record_rate_limit(retry_after, &message);
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        retry_after_secs = retry_after.as_secs(),
                        error = %message,
                        "wechat startup help rate limited"
                    );
                    if count_failures_as_retries {
                        self.record_startup_help_retry_failure(&user_id);
                    }
                }
                Err(err) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        user_id = %user_id,
                        error = %err,
                        "wechat startup help send failed"
                    );
                    if count_failures_as_retries {
                        self.record_startup_help_retry_failure(&user_id);
                    }
                }
            }
        }
    }

    async fn deliver_assistant_message(
        &self,
        workspace_id: &WorkspaceId,
        text: &str,
    ) -> Result<(), WechatError> {
        let Some(workspace) = self.core.workspace_binding(workspace_id) else {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "watched agent message has no workspace binding"
            );
            return Ok(());
        };
        let provider_session_id = self
            .core
            .active_provider_session_id(workspace_id)
            .map_err(|err| WechatError::Core(err.to_string()))?;
        let session_ref = self
            .core
            .provider_session_record(&provider_session_id)
            .map(|record| record.native_resume_ref.to_string())
            .unwrap_or_else(|| provider_session_id.as_str().to_string());

        let body = render_agent_message(text, &workspace, &session_ref, None);

        if self.core.direct_notification_suppressed(workspace_id) {
            debug!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "wechat notification suppressed by direct conversation"
            );
            return Ok(());
        }
        if !self.notifications_enabled(&workspace, &provider_session_id) {
            return Ok(());
        }

        let users = self.notification_users();
        if users.is_empty() {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "wechat notification skipped because no notification users are configured or remembered"
            );
            return Ok(());
        }

        for user_id in users {
            if !self
                .try_send_notification(workspace_id, &user_id, &body, provider_session_id.clone())
                .await?
            {
                self.remember_pending_notification(
                    workspace_id.clone(),
                    user_id,
                    body.clone().into(),
                    provider_session_id.clone(),
                );
            }
        }
        Ok(())
    }

    async fn deliver_attachment_notification(
        &self,
        workspace_id: &WorkspaceId,
        attachment: AgentAttachment,
    ) -> Result<(), WechatError> {
        let Some(workspace) = self.core.workspace_binding(workspace_id) else {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "watched attachment has no workspace binding"
            );
            return Ok(());
        };
        let provider_session_id = self
            .core
            .active_provider_session_id(workspace_id)
            .map_err(|err| WechatError::Core(err.to_string()))?;
        if self.core.direct_notification_suppressed(workspace_id) {
            debug!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "wechat attachment notification suppressed by direct conversation"
            );
            return Ok(());
        }
        if !self.notifications_enabled(&workspace, &provider_session_id) {
            return Ok(());
        }
        let users = self.notification_users();
        if users.is_empty() {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                "wechat attachment notification skipped because no notification users are configured or remembered"
            );
            return Ok(());
        }
        for user_id in users {
            if !self
                .try_send_attachment_notification(
                    workspace_id,
                    &user_id,
                    &attachment,
                    provider_session_id.clone(),
                )
                .await?
            {
                self.remember_pending_attachment_notification(
                    workspace_id.clone(),
                    user_id,
                    attachment.clone(),
                    provider_session_id.clone(),
                );
            }
        }
        Ok(())
    }

    async fn deliver_intervention_request(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: Option<&TurnId>,
        request: InterventionRequest,
    ) -> Result<(), WechatError> {
        let body = render_intervention_markdown_zh(&request);
        let provider_session_id = self.core.active_provider_session_id(workspace_id).ok();
        if let Some(reply_target) = turn_id.and_then(|turn_id| self.pending_reply(turn_id)) {
            let user_id = reply_target.message.user_id.clone();
            let receipt = self.transport.reply(&reply_target.message, &body).await?;
            if let Some(provider_session_id) = provider_session_id.clone() {
                self.bind_receipt(&user_id, receipt.clone(), &body, provider_session_id)?;
            }
            self.remember_pending_intervention(
                workspace_id.clone(),
                turn_id.cloned(),
                user_id,
                request,
                receipt.message_ids,
                receipt.visible_texts,
            );
            self.stop_typing_keepalive(workspace_id);
            return Ok(());
        }

        let users = self.notification_users();
        if users.is_empty() {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                req_id = %intervention_request_id(&request),
                "wechat intervention request skipped because no known users"
            );
            return Ok(());
        }

        for user_id in users {
            let Some(context) = self.transport.context_for_user(&user_id).await? else {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    "wechat intervention prompt skipped: no stored context for user"
                );
                continue;
            };
            let receipt = match self.transport.send(&context, &body).await {
                Ok(receipt) => receipt,
                Err(WechatError::RateLimited {
                    retry_after,
                    message,
                }) => {
                    self.record_rate_limit(retry_after, &message);
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        user_id = %user_id,
                        retry_after_secs = retry_after.as_secs(),
                        error = %message,
                        "wechat intervention prompt rate limited"
                    );
                    self.remember_pending_intervention(
                        workspace_id.clone(),
                        None,
                        user_id.clone(),
                        request.clone(),
                        Vec::new(),
                        Vec::new(),
                    );
                    if let Some(provider_session_id) = provider_session_id.clone() {
                        self.remember_pending_notification(
                            workspace_id.clone(),
                            user_id,
                            body.clone().into(),
                            provider_session_id,
                        );
                    }
                    continue;
                }
                Err(WechatError::Transport(err)) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        user_id = %user_id,
                        error = %err,
                        "wechat intervention prompt send failed"
                    );
                    self.remember_pending_intervention(
                        workspace_id.clone(),
                        None,
                        user_id.clone(),
                        request.clone(),
                        Vec::new(),
                        Vec::new(),
                    );
                    if let Some(provider_session_id) = provider_session_id.clone() {
                        self.remember_pending_notification(
                            workspace_id.clone(),
                            user_id,
                            body.clone().into(),
                            provider_session_id,
                        );
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };
            if let Some(provider_session_id) = provider_session_id.clone() {
                self.bind_receipt(&user_id, receipt.clone(), &body, provider_session_id)?;
            }
            self.remember_pending_intervention(
                workspace_id.clone(),
                None,
                user_id,
                request.clone(),
                receipt.message_ids,
                receipt.visible_texts,
            );
        }
        Ok(())
    }

    async fn handle_pending_intervention_response(
        &self,
        message: &WechatIncoming,
        text: &str,
    ) -> Result<bool, WechatError> {
        let pending = match self.lookup_pending_intervention(message) {
            PendingInterventionLookup::None => return Ok(false),
            PendingInterventionLookup::Multiple => {
                self.transport
                    .reply(message, "有多个待确认请求。请引用对应的授权/问题消息回复。")
                    .await?;
                return Ok(true);
            }
            PendingInterventionLookup::One(pending) => pending,
        };
        let parsed = match parse_intervention_text_response_zh(&pending.request, text) {
            Ok(parsed) => parsed,
            Err(help) => {
                self.transport.reply(message, &help).await?;
                return Ok(true);
            }
        };
        let req_id = intervention_request_id(&pending.request).to_string();
        match self
            .core
            .resolve_live_request(&pending.workspace_id, &req_id, parsed.response)
            .await
        {
            Ok(()) => {
                self.forget_pending_intervention(&req_id);
                if pending
                    .turn_id
                    .as_ref()
                    .is_some_and(|turn_id| self.pending_reply(turn_id).is_some())
                {
                    self.start_typing_keepalive(
                        pending.workspace_id.clone(),
                        message.user_id.clone(),
                    );
                }
                self.transport.reply(message, &parsed.ack_markdown).await?;
            }
            Err(err) => {
                self.transport
                    .reply(message, &format!("提交失败：`{err}`。请稍后重试。"))
                    .await?;
            }
        }
        Ok(true)
    }

    async fn try_send_notification(
        &self,
        workspace_id: &WorkspaceId,
        user_id: &str,
        body: &str,
        provider_session_id: ProviderSessionId,
    ) -> Result<bool, WechatError> {
        if let Some(remaining) = self.rate_limit_remaining() {
            debug!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                user_id = %user_id,
                remaining_ms = remaining.as_millis() as u64,
                "wechat notification send deferred by active rate limit"
            );
            return Ok(false);
        }
        let Some(context) = self.transport.context_for_user(user_id).await? else {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                user_id = %user_id,
                "wechat notification send skipped: no stored context for user"
            );
            return Ok(false);
        };
        let receipt = match self.transport.send(&context, body).await {
            Ok(receipt) => receipt,
            Err(WechatError::Transport(err)) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    error = %err,
                    "wechat notification send failed; retaining for retry"
                );
                return Ok(false);
            }
            Err(WechatError::RateLimited {
                retry_after,
                message,
            }) => {
                self.record_rate_limit(retry_after, &message);
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    retry_after_secs = retry_after.as_secs(),
                    error = %message,
                    "wechat notification send rate limited; retaining for delayed retry"
                );
                return Ok(false);
            }
            Err(err) => return Err(err),
        };
        let message_ids = receipt.message_ids.clone();
        self.bind_receipt(user_id, receipt, body, provider_session_id.clone())?;
        debug!(
            target: "lucarne_wechat::service",
            workspace_id = %workspace_id.as_str(),
            user_id = %user_id,
            provider_session_id = %provider_session_id.as_str(),
            message_ids = ?message_ids,
            bytes = body.len(),
            "wechat notification sent"
        );
        Ok(true)
    }

    async fn try_send_attachment_notification(
        &self,
        workspace_id: &WorkspaceId,
        user_id: &str,
        attachment: &AgentAttachment,
        provider_session_id: ProviderSessionId,
    ) -> Result<bool, WechatError> {
        let Some(context) = self.transport.context_for_user(user_id).await? else {
            warn!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                user_id = %user_id,
                attachment_id = %attachment.id,
                "wechat attachment notification skipped: no stored context for user"
            );
            return Ok(true);
        };
        let receipt = match self
            .send_notification_attachment_with_retries(&context, attachment)
            .await
        {
            Ok(receipt) => receipt,
            Err(WechatError::Transport(err)) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    attachment_id = %attachment.id,
                    error = %err,
                    "wechat attachment notification failed; sending failure details"
                );
                self.send_attachment_notification_failure(
                    workspace_id,
                    user_id,
                    &context,
                    attachment,
                    provider_session_id.clone(),
                    &err,
                )
                .await?;
                return Ok(true);
            }
            Err(WechatError::RateLimited {
                retry_after,
                message,
            }) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    attachment_id = %attachment.id,
                    retry_after_secs = retry_after.as_secs(),
                    error = %message,
                    "wechat attachment notification rate limited; sending failure details without retaining media retry"
                );
                self.send_attachment_notification_failure(
                    workspace_id,
                    user_id,
                    &context,
                    attachment,
                    provider_session_id.clone(),
                    &message,
                )
                .await?;
                return Ok(true);
            }
            Err(err) => return Err(err),
        };
        let message_ids = receipt.message_ids.clone();
        let binding_text = attachment_binding_text(attachment);
        self.bind_receipt(user_id, receipt, &binding_text, provider_session_id.clone())?;
        debug!(
            target: "lucarne_wechat::service",
            workspace_id = %workspace_id.as_str(),
            user_id = %user_id,
            provider_session_id = %provider_session_id.as_str(),
            attachment_id = %attachment.id,
            message_ids = ?message_ids,
            "wechat attachment notification sent"
        );
        Ok(true)
    }

    async fn send_notification_attachment_with_retries(
        &self,
        context: &WechatContext,
        attachment: &AgentAttachment,
    ) -> Result<WechatSendReceipt, WechatError> {
        retry_attachment_delivery(
            || self.transport.send_attachment(context, attachment),
            is_retryable_wechat_attachment_error,
        )
        .await
    }

    async fn reply_attachment_with_retries(
        &self,
        message: &WechatIncoming,
        attachment: &AgentAttachment,
    ) -> Result<WechatSendReceipt, WechatError> {
        retry_attachment_delivery(
            || self.transport.reply_attachment(message, attachment),
            is_retryable_wechat_attachment_error,
        )
        .await
    }

    async fn send_attachment_notification_failure(
        &self,
        workspace_id: &WorkspaceId,
        user_id: &str,
        context: &WechatContext,
        attachment: &AgentAttachment,
        provider_session_id: ProviderSessionId,
        error: &str,
    ) -> Result<(), WechatError> {
        let body = render_attachment_delivery_failure(attachment, error);
        match self.transport.send(context, &body).await {
            Ok(receipt) => {
                self.bind_receipt(user_id, receipt, &body, provider_session_id)?;
            }
            Err(err) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    attachment_id = %attachment.id,
                    error = %err,
                    "wechat attachment failure details send failed"
                );
            }
        }
        Ok(())
    }

    async fn deliver_pending_reply_attachment(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        attachment: AgentAttachment,
    ) -> Result<(), WechatError> {
        if self.pending_reply(turn_id).is_none() {
            return Ok(());
        }
        debug!(
            target: "lucarne_wechat::service",
            workspace_id = %workspace_id.as_str(),
            turn_id = %turn_id.as_str(),
            attachment_id = %attachment.id,
            "wechat pending reply attachment queued until terminal reply"
        );
        self.remember_pending_reply_attachment(turn_id, attachment);
        Ok(())
    }

    async fn send_pending_reply_attachment_failure(
        &self,
        reply_target: &WechatPendingReply,
        attachment: &AgentAttachment,
        error: &str,
    ) -> Result<(), WechatError> {
        let body = render_attachment_delivery_failure(attachment, error);
        match self.transport.reply(&reply_target.message, &body).await {
            Ok(_) => {}
            Err(WechatError::RateLimited {
                retry_after,
                message,
            }) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %reply_target.workspace_id.as_str(),
                    user_id = %reply_target.message.user_id,
                    attachment_id = %attachment.id,
                    retry_after_secs = retry_after.as_secs(),
                    error = %message,
                    "wechat attachment failure details rate limited"
                );
            }
            Err(err) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %reply_target.workspace_id.as_str(),
                    user_id = %reply_target.message.user_id,
                    attachment_id = %attachment.id,
                    error = %err,
                    "wechat attachment failure details send failed"
                );
            }
        }
        Ok(())
    }

    async fn deliver_pending_reply(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        reply_target: WechatPendingReply,
    ) -> Result<(), WechatError> {
        if let Some(remaining) = self.rate_limit_remaining() {
            debug!(
                target: "lucarne_wechat::service",
                workspace_id = %workspace_id.as_str(),
                turn_id = %turn_id.as_str(),
                user_id = %reply_target.message.user_id,
                remaining_ms = remaining.as_millis() as u64,
                "wechat pending reply delivery deferred by active rate limit"
            );
            return Ok(());
        }
        let Some(terminal) = reply_target.terminal.clone() else {
            return Ok(());
        };
        let (body, provider_session_id) = match terminal {
            PendingReplyTerminal::Failed(text) => (text.to_string(), None),
            PendingReplyTerminal::Completed => {
                let Some(text) = reply_target.latest_assistant_text.as_deref() else {
                    if !reply_target.pending_attachments.is_empty() {
                        self.deliver_pending_reply_attachments_best_effort(
                            workspace_id,
                            turn_id,
                            &reply_target,
                        )
                        .await?;
                        self.take_pending_reply(turn_id);
                        self.core.end_direct_notification_suppression(workspace_id);
                        return Ok(());
                    }
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        turn_id = %turn_id.as_str(),
                        "completed wechat reply turn had no assistant message"
                    );
                    self.take_pending_reply(turn_id);
                    self.core.end_direct_notification_suppression(workspace_id);
                    return Ok(());
                };
                let Some(workspace) = self.core.workspace_binding(workspace_id) else {
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        "completed wechat reply has no workspace binding"
                    );
                    self.take_pending_reply(turn_id);
                    self.core.end_direct_notification_suppression(workspace_id);
                    return Ok(());
                };
                let provider_session_id = self
                    .core
                    .active_provider_session_id(workspace_id)
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                let session_ref = self
                    .core
                    .provider_session_record(&provider_session_id)
                    .map(|record| record.native_resume_ref.to_string())
                    .unwrap_or_else(|| provider_session_id.as_str().to_string());
                (
                    render_agent_message(
                        text,
                        &workspace,
                        &session_ref,
                        Some(format_cost_duration(reply_target.started_at.elapsed())),
                    ),
                    Some(provider_session_id),
                )
            }
        };
        let receipt = match self.transport.reply(&reply_target.message, &body).await {
            Ok(receipt) => receipt,
            Err(WechatError::Transport(err)) => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    turn_id = %turn_id.as_str(),
                    user_id = %reply_target.message.user_id,
                    error = %err,
                    "wechat pending reply delivery failed; retaining for retry"
                );
                return Ok(());
            }
            Err(WechatError::RateLimited {
                retry_after,
                message,
            }) => {
                self.record_rate_limit(retry_after, &message);
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    turn_id = %turn_id.as_str(),
                    user_id = %reply_target.message.user_id,
                    retry_after_secs = retry_after.as_secs(),
                    error = %message,
                    "wechat pending reply delivery rate limited; retaining for delayed retry"
                );
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        let message_ids = receipt.message_ids.clone();
        if let Some(provider_session_id) = provider_session_id.clone() {
            self.bind_receipt(
                &reply_target.message.user_id,
                receipt,
                &body,
                provider_session_id.clone(),
            )?;
        }
        debug!(
            target: "lucarne_wechat::service",
            workspace_id = %workspace_id.as_str(),
            user_id = %reply_target.message.user_id,
            provider_session_id = provider_session_id.as_ref().map(|id| id.as_str()),
            reply_to_message_id = %reply_target.message.message_id,
            message_ids = ?message_ids,
            bytes = body.len(),
            "wechat reply sent"
        );
        self.deliver_pending_reply_attachments_best_effort(workspace_id, turn_id, &reply_target)
            .await?;
        self.take_pending_reply(turn_id);
        self.core.end_direct_notification_suppression(workspace_id);
        Ok(())
    }

    async fn deliver_pending_reply_attachments_best_effort(
        &self,
        workspace_id: &WorkspaceId,
        turn_id: &TurnId,
        reply_target: &WechatPendingReply,
    ) -> Result<(), WechatError> {
        for attachment in reply_target.pending_attachments.clone() {
            match self
                .reply_attachment_with_retries(&reply_target.message, &attachment)
                .await
            {
                Ok(receipt) => {
                    if let Ok(provider_session_id) =
                        self.core.active_provider_session_id(workspace_id)
                    {
                        let binding_text = attachment_binding_text(&attachment);
                        self.bind_receipt(
                            &reply_target.message.user_id,
                            receipt,
                            &binding_text,
                            provider_session_id,
                        )?;
                    }
                    debug!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        turn_id = %turn_id.as_str(),
                        user_id = %reply_target.message.user_id,
                        attachment_id = %attachment.id,
                        "wechat pending reply attachment sent after terminal reply"
                    );
                }
                Err(WechatError::Transport(err)) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        turn_id = %turn_id.as_str(),
                        user_id = %reply_target.message.user_id,
                        attachment_id = %attachment.id,
                        error = %err,
                        "wechat pending reply attachment failed after bounded retries; sending failure details"
                    );
                    self.send_pending_reply_attachment_failure(reply_target, &attachment, &err)
                        .await?;
                }
                Err(WechatError::RateLimited {
                    retry_after,
                    message,
                }) => {
                    warn!(
                        target: "lucarne_wechat::service",
                        workspace_id = %workspace_id.as_str(),
                        turn_id = %turn_id.as_str(),
                        user_id = %reply_target.message.user_id,
                        attachment_id = %attachment.id,
                        retry_after_secs = retry_after.as_secs(),
                        error = %message,
                        "wechat pending reply attachment rate limited after bounded retries; sending failure details"
                    );
                    self.send_pending_reply_attachment_failure(reply_target, &attachment, &message)
                        .await?;
                }
                Err(err) => return Err(err),
            }
            self.remove_pending_reply_attachment(turn_id, attachment.id.as_str());
        }
        Ok(())
    }

    async fn retry_completed_pending_replies(&self) -> Result<(), WechatError> {
        if self.rate_limit_remaining().is_some() {
            return Ok(());
        }
        for (turn_id, reply_target) in self.completed_pending_replies() {
            let workspace_id = reply_target.workspace_id.clone();
            self.deliver_pending_reply(&workspace_id, &turn_id, reply_target)
                .await?;
        }
        Ok(())
    }

    async fn retry_pending_notifications(&self) -> Result<(), WechatError> {
        if self.rate_limit_remaining().is_some() {
            return Ok(());
        }
        for notification in self.take_pending_notifications() {
            match notification.content {
                WechatPendingNotificationContent::Text(body) => {
                    if !self
                        .try_send_notification(
                            &notification.workspace_id,
                            &notification.user_id,
                            &body,
                            notification.provider_session_id.clone(),
                        )
                        .await?
                    {
                        self.remember_pending_notification(
                            notification.workspace_id,
                            notification.user_id,
                            body,
                            notification.provider_session_id,
                        );
                    }
                }
                WechatPendingNotificationContent::Attachment(attachment) => {
                    if !self
                        .try_send_attachment_notification(
                            &notification.workspace_id,
                            &notification.user_id,
                            &attachment,
                            notification.provider_session_id.clone(),
                        )
                        .await?
                    {
                        self.remember_pending_attachment_notification(
                            notification.workspace_id,
                            notification.user_id,
                            attachment,
                            notification.provider_session_id,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    async fn continue_session(
        &self,
        workspace_id: WorkspaceId,
        message: WechatIncoming,
    ) -> Result<(), WechatError> {
        let user_id = message.user_id.clone();
        let context = self.transport.context_for_user(&user_id).await?;
        match context.as_ref() {
            Some(context) => {
                if self.rate_limit_remaining().is_none() {
                    match self.transport.send_typing(context).await {
                        Ok(()) => {}
                        Err(WechatError::RateLimited {
                            retry_after,
                            message,
                        }) => {
                            self.record_rate_limit(retry_after, &message);
                            warn!(
                                target: "lucarne_wechat::service",
                                workspace_id = %workspace_id.as_str(),
                                user_id = %user_id,
                                retry_after_secs = retry_after.as_secs(),
                                error = %message,
                                "wechat initial typing rate limited; continuing incoming reply"
                            );
                        }
                        Err(err) => {
                            warn!(
                                target: "lucarne_wechat::service",
                                workspace_id = %workspace_id.as_str(),
                                user_id = %user_id,
                                error = %err,
                                "wechat initial typing send failed; continuing incoming reply"
                            );
                        }
                    }
                }
            }
            None => {
                warn!(
                    target: "lucarne_wechat::service",
                    workspace_id = %workspace_id.as_str(),
                    user_id = %user_id,
                    "wechat initial typing skipped: no stored context for user"
                );
            }
        }
        self.start_typing_keepalive(workspace_id.clone(), user_id);
        if let Err(err) = self.ensure_live(&workspace_id).await {
            self.stop_typing_keepalive(&workspace_id);
            let body = format!("⚠️ agent 启动失败：{err}");
            self.transport.reply(&message, &body).await?;
            return Ok(());
        }
        self.core
            .begin_direct_notification_suppression(&workspace_id);
        let input = AgentInput {
            text: message.text.trim().to_string().into(),
            images: Vec::new(),
        };
        let submit = self
            .core
            .submit_turn(SubmitTurnRequest {
                workspace_id: workspace_id.clone(),
                source: TurnSource::UserMessage,
                input,
                reply_to_channel_message_id: None,
            })
            .await;
        let submitted = match submit {
            Ok(submitted) => submitted,
            Err(err) => {
                self.core.end_direct_notification_suppression(&workspace_id);
                self.stop_typing_keepalive(&workspace_id);
                let body = format!("⚠️ 消息提交失败：{err}");
                self.transport.reply(&message, &body).await?;
                return Ok(());
            }
        };
        self.remember_pending_reply(submitted.turn_id, workspace_id, message);
        Ok(())
    }

    async fn ensure_live(&self, workspace_id: &WorkspaceId) -> Result<(), WechatError> {
        if self.workspace_has_live_session(workspace_id) {
            return Ok(());
        }
        self.core
            .resume_workspace_with_events(ResumeWorkspaceRequest {
                workspace_id: workspace_id.clone(),
                force_bypass_permissions: false,
            })
            .await
            .map(|_| ())
            .map_err(|err| WechatError::Core(err.to_string()))
    }

    fn workspace_has_live_session(&self, workspace_id: &WorkspaceId) -> bool {
        self.core.has_live_session(workspace_id)
    }

    fn notifications_enabled(
        &self,
        workspace: &WorkspaceBinding,
        provider_session_id: &ProviderSessionId,
    ) -> bool {
        self.core
            .effective_settings(
                Some(workspace.project_path.as_path()),
                Some(provider_session_id),
            )
            .notifications
            .enabled
    }

    async fn handle_config_command(
        &self,
        message: &WechatIncoming,
        config: ConfigCommand,
    ) -> Result<(), WechatError> {
        match config {
            ConfigCommand::Show => {
                let settings = self.core.effective_settings(None, None);
                self.transport
                    .reply(message, &render_global_config(&settings))
                    .await?;
            }
            ConfigCommand::SetGlobalBypass(enabled) => {
                self.persist_global_config_update(Some(enabled), None)?;
                self.core
                    .set_force_bypass_permissions(enabled)
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                let settings = self.core.effective_settings(None, None);
                self.transport
                    .reply(message, &render_global_config(&settings))
                    .await?;
            }
            ConfigCommand::SetGlobalNotifications(enabled) => {
                self.persist_global_config_update(None, Some(enabled))?;
                self.core
                    .set_global_notifications_enabled(enabled)
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                let settings = self.core.effective_settings(None, None);
                self.transport
                    .reply(message, &render_global_config(&settings))
                    .await?;
            }
        }
        Ok(())
    }

    fn persist_global_config_update(
        &self,
        bypass: Option<bool>,
        notifications: Option<bool>,
    ) -> Result<(), WechatError> {
        let Some(persistence) = self.global_config_persistence.as_ref() else {
            return Ok(());
        };
        let settings = self.core.effective_settings(None, None);
        let update = GlobalConfigUpdate {
            bypass: bypass.unwrap_or(settings.session.force_bypass_permissions),
            notifications: notifications.unwrap_or(settings.notifications.enabled),
        };
        persistence
            .persist_global_config(update)
            .map_err(|err| WechatError::Core(err.to_string()))
    }

    async fn handle_slash_command(
        &self,
        message: &WechatIncoming,
        command: SlashCommand,
    ) -> Result<(), WechatError> {
        match command {
            SlashCommand::Help => {
                self.transport.reply(message, render_wechat_help()).await?;
            }
            SlashCommand::New => {
                let Some(scope) = self.slash_scope(message)? else {
                    self.transport
                        .reply(message, WECHAT_STALE_NOTIFICATION)
                        .await?;
                    return Ok(());
                };
                let SlashScope::Workspace(workspace) = scope else {
                    self.transport
                        .reply(message, render_wechat_new_usage())
                        .await?;
                    return Ok(());
                };
                let opened = self
                    .core
                    .open_new_workspace_session_with_events(workspace.workspace_id.clone())
                    .await
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                let provider_session_id = self
                    .core
                    .active_provider_session_id(&opened.workspace.workspace_id)
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                let body = render_wechat_new_session(
                    opened.workspace.provider_id,
                    opened.workspace.session_id.0.as_str(),
                    &workspace,
                );
                let receipt = self.transport.reply(message, &body).await?;
                self.bind_receipt(&message.user_id, receipt, &body, provider_session_id)?;
            }
            SlashCommand::Status => {
                let Some(scope) = self.slash_scope(message)? else {
                    self.transport
                        .reply(message, WECHAT_STALE_NOTIFICATION)
                        .await?;
                    return Ok(());
                };
                let body = match scope {
                    SlashScope::Global => {
                        let snapshot = self
                            .core
                            .agent_resource_snapshot(AgentResourceScope::All)
                            .await
                            .map_err(|err| WechatError::Core(err.to_string()))?;
                        render_wechat_agent_resource_snapshot(&snapshot)
                    }
                    SlashScope::Workspace(workspace) => {
                        self.core
                            .begin_direct_notification_suppression(&workspace.workspace_id);
                        let status_snapshot = self
                            .core
                            .refresh_workspace_status_snapshot(&workspace.workspace_id, false)
                            .await;
                        self.core
                            .end_direct_notification_suppression(&workspace.workspace_id);
                        let status_snapshot =
                            status_snapshot.map_err(|err| WechatError::Core(err.to_string()))?;
                        let resource_snapshot = self
                            .core
                            .agent_resource_snapshot(AgentResourceScope::Workspace(
                                workspace.workspace_id.clone(),
                            ))
                            .await
                            .map_err(|err| WechatError::Core(err.to_string()))?;
                        render_wechat_workspace_status(
                            &workspace,
                            status_snapshot.as_ref(),
                            &resource_snapshot,
                        )
                    }
                };
                self.transport.reply(message, &body).await?;
            }
            SlashCommand::Kill(target) => {
                let Some(scope) = self.slash_scope(message)? else {
                    self.transport
                        .reply(message, WECHAT_STALE_NOTIFICATION)
                        .await?;
                    return Ok(());
                };
                let scope = match scope {
                    SlashScope::Global => AgentResourceScope::All,
                    SlashScope::Workspace(workspace) => {
                        AgentResourceScope::Workspace(workspace.workspace_id)
                    }
                };
                let report = self
                    .core
                    .kill_agent_processes(KillAgentRequest { scope, target })
                    .await
                    .map_err(|err| WechatError::Core(err.to_string()))?;
                self.transport
                    .reply(message, &render_wechat_kill_agent_report(&report))
                    .await?;
            }
            SlashCommand::InvalidKill => {
                self.transport
                    .reply(message, "用法：`/kill all` 或 `/kill <session_id:pid>`")
                    .await?;
            }
        }
        Ok(())
    }

    fn slash_scope(&self, message: &WechatIncoming) -> Result<Option<SlashScope>, WechatError> {
        let has_quote = message.quoted_message_id.is_some() || message.quoted_text.is_some();
        let Some(binding) = self.resolve_quoted_binding(message) else {
            return if has_quote {
                Ok(None)
            } else {
                Ok(Some(SlashScope::Global))
            };
        };
        let Some(workspace) = self
            .core
            .workspace_for_provider_session(&binding.provider_session_id)
        else {
            return Ok(None);
        };
        Ok(Some(SlashScope::Workspace(workspace)))
    }

    fn bind_receipt(
        &self,
        user_id: &str,
        receipt: WechatSendReceipt,
        body: &str,
        provider_session_id: ProviderSessionId,
    ) -> Result<(), WechatError> {
        for message_id in receipt.message_ids {
            self.core
                .bind_message_to_provider_session(
                    "wechat",
                    user_id,
                    &message_id,
                    provider_session_id.clone(),
                )
                .map_err(|err| WechatError::Core(err.to_string()))?;
        }
        for visible_text in receipt.visible_texts {
            self.core
                .bind_message_to_provider_session(
                    "wechat",
                    user_id,
                    &sent_quote_binding_id(&visible_text),
                    provider_session_id.clone(),
                )
                .map_err(|err| WechatError::Core(err.to_string()))?;
        }
        self.core
            .bind_message_to_provider_session(
                "wechat",
                user_id,
                &sent_quote_binding_id(body),
                provider_session_id,
            )
            .map_err(|err| WechatError::Core(err.to_string()))?;
        Ok(())
    }

    fn resolve_quoted_binding(&self, message: &WechatIncoming) -> Option<MessageSessionBinding> {
        message
            .quoted_message_id
            .as_deref()
            .and_then(|message_id| {
                self.core
                    .message_session_binding("wechat", &message.user_id, message_id)
            })
            .or_else(|| {
                message.quoted_text.as_deref().and_then(|text| {
                    self.core.message_session_binding(
                        "wechat",
                        &message.user_id,
                        &visible_quote_binding_id(text),
                    )
                })
            })
    }

    fn remember_user(&self, user_id: &str) {
        self.state
            .lock()
            .expect("wechat service state lock")
            .users
            .insert(user_id.to_string());
    }

    fn notification_users(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("wechat service state lock")
            .users
            .iter()
            .cloned()
            .collect()
    }

    fn remember_pending_startup_help_users(&self, users: Vec<String>) {
        let mut state = self.state.lock().expect("wechat service state lock");
        for user_id in users {
            state.startup_help_retries.entry(user_id).or_insert(0);
        }
    }

    fn pending_startup_help_users(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("wechat service state lock")
            .startup_help_retries
            .keys()
            .cloned()
            .collect()
    }

    fn forget_pending_startup_help(&self, user_id: &str) {
        self.state
            .lock()
            .expect("wechat service state lock")
            .startup_help_retries
            .remove(user_id);
    }

    fn record_startup_help_retry_failure(&self, user_id: &str) {
        let abandoned_retries = {
            let mut state = self.state.lock().expect("wechat service state lock");
            let retries = {
                let Some(retries) = state.startup_help_retries.get_mut(user_id) else {
                    return;
                };
                *retries = retries.saturating_add(1);
                *retries
            };
            if retries >= STARTUP_HELP_MAX_RETRIES {
                state.startup_help_retries.remove(user_id);
                Some(retries)
            } else {
                None
            }
        };
        if let Some(retries) = abandoned_retries {
            warn!(
                target: "lucarne_wechat::service",
                user_id = %user_id,
                retries,
                max_retries = STARTUP_HELP_MAX_RETRIES,
                "wechat startup help abandoned after bounded retries"
            );
        }
    }

    fn rate_limit_remaining(&self) -> Option<Duration> {
        self.rate_limiter.remaining()
    }

    fn record_rate_limit(&self, retry_after: Duration, message: &str) {
        let backoff = self.rate_limiter.defer(retry_after);
        warn!(
            target: "lucarne_wechat::service",
            retry_after_secs = retry_after.as_secs(),
            backoff_secs = backoff.as_secs(),
            error = %message,
            "wechat outbound rate limited; deferring retries"
        );
    }

    async fn handle_user_interaction_request(
        &self,
        request: WechatUserInteractionRequest,
    ) -> Result<(), WechatError> {
        match request.reason {
            UserInteractionReason::ContextExpiring {
                expires_at,
                remind_before: _,
                observed_at: _,
            } => {
                if self.context_expiry_reminder.is_none() {
                    debug!(
                        target: "lucarne_wechat::service",
                        account_key = %request.account_key,
                        "wechat context expiry interaction request skipped: no reminder policy configured"
                    );
                    return Ok(());
                }
                let Some(user_id) = request.user_id.as_deref() else {
                    warn!(
                        target: "lucarne_wechat::service",
                        account_key = %request.account_key,
                        "wechat context expiry interaction request skipped: missing user_id"
                    );
                    return Ok(());
                };
                let prompt = self.context_expiry_prompt(user_id, expires_at);
                self.try_send_interaction_prompt(user_id, &prompt).await?;
            }
            UserInteractionReason::OutboundRateLimitApproaching { .. } => {
                let Some(prompt) = self.rate_limit_interaction_prompt.as_deref() else {
                    debug!(
                        target: "lucarne_wechat::service",
                        account_key = %request.account_key,
                        "wechat rate-limit interaction request skipped: no prompt policy configured"
                    );
                    return Ok(());
                };
                let users = request
                    .user_id
                    .map(|user_id| vec![user_id])
                    .unwrap_or_else(|| self.notification_users());
                if users.is_empty() {
                    warn!(
                        target: "lucarne_wechat::service",
                        account_key = %request.account_key,
                        "wechat rate-limit interaction request skipped: no known users"
                    );
                    return Ok(());
                }
                for user_id in users {
                    self.try_send_interaction_prompt(&user_id, prompt).await?;
                }
            }
        }
        Ok(())
    }

    fn context_expiry_prompt(&self, user_id: &str, expires_at: SystemTime) -> String {
        let now = SystemTime::now();
        let remaining = expires_at.duration_since(now).unwrap_or_default();
        let remaining_ms = remaining.as_millis() as i64;
        let remaining_secs = remaining.as_secs() as i64;
        let remaining_minutes = (remaining_secs + 59) / 60;
        let expires_at_unix_ms = expires_at
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default();
        let template = self
            .context_expiry_reminder
            .as_ref()
            .map(|config| config.prompt_template.as_str())
            .expect("context expiry reminder policy checked by caller");
        template
            .replace("{user_id}", user_id)
            .replace("{expires_at_unix_ms}", &expires_at_unix_ms.to_string())
            .replace("{remaining_ms}", &remaining_ms.to_string())
            .replace("{remaining_secs}", &remaining_secs.to_string())
            .replace("{remaining_minutes}", &remaining_minutes.to_string())
    }

    async fn try_send_interaction_prompt(
        &self,
        user_id: &str,
        prompt: &str,
    ) -> Result<(), WechatError> {
        if let Some(remaining) = self.rate_limit_remaining() {
            debug!(
                target: "lucarne_wechat::service",
                user_id = %user_id,
                remaining_ms = remaining.as_millis() as u64,
                "wechat interaction prompt deferred by active rate limit"
            );
            return Ok(());
        }
        let Some(context) = self.transport.context_for_user(user_id).await? else {
            warn!(
                target: "lucarne_wechat::service",
                user_id = %user_id,
                "wechat interaction prompt skipped: no stored context for user"
            );
            return Ok(());
        };
        match self.transport.send(&context, prompt).await {
            Ok(_) => Ok(()),
            Err(WechatError::RateLimited {
                retry_after,
                message,
            }) => {
                self.record_rate_limit(retry_after, &message);
                Ok(())
            }
            Err(WechatError::Transport(err)) => {
                warn!(
                    target: "lucarne_wechat::service",
                    user_id = %user_id,
                    error = %err,
                    "wechat interaction prompt send failed"
                );
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn remember_pending_reply(
        &self,
        turn_id: TurnId,
        workspace_id: WorkspaceId,
        message: WechatIncoming,
    ) {
        let mut dropped_workspaces = Vec::new();
        {
            let mut state = self.state.lock().expect("wechat service state lock");
            if !state.pending_replies.contains_key(&turn_id) {
                while state.pending_replies.len() >= MAX_PENDING_REPLIES {
                    let Some(oldest_turn_id) = state.pending_reply_order.pop_front() else {
                        break;
                    };
                    let Some(dropped) = state.pending_replies.remove(&oldest_turn_id) else {
                        continue;
                    };
                    let dropped_workspace_id = dropped.workspace_id.clone();
                    let workspace_pending = state
                        .pending_replies
                        .values()
                        .any(|reply| reply.workspace_id == dropped_workspace_id);
                    if !workspace_pending {
                        if let Some(cancel) =
                            state.typing_cancellations.remove(&dropped_workspace_id)
                        {
                            let _ = cancel.send(());
                        }
                    }
                    dropped_workspaces.push(dropped_workspace_id);
                }
                state.pending_reply_order.push_back(turn_id.clone());
            }
            state.pending_replies.insert(
                turn_id.clone(),
                WechatPendingReply {
                    workspace_id,
                    message,
                    started_at: Instant::now(),
                    latest_assistant_text: None,
                    pending_attachments: Vec::new(),
                    terminal: None,
                },
            );
        }
        for workspace_id in &dropped_workspaces {
            self.core.end_direct_notification_suppression(workspace_id);
        }
        if !dropped_workspaces.is_empty() {
            warn!(
                target: "lucarne_wechat::service",
                dropped = dropped_workspaces.len(),
                max_pending = MAX_PENDING_REPLIES,
                "wechat pending reply queue full; dropped oldest pending replies"
            );
        }
    }

    fn record_pending_assistant_message(&self, turn_id: &TurnId, text: &str) -> bool {
        let mut state = self.state.lock().expect("wechat service state lock");
        let Some(pending) = state.pending_replies.get_mut(turn_id) else {
            return false;
        };
        pending.latest_assistant_text = Some(text.to_string().into());
        true
    }

    fn pending_reply(&self, turn_id: &TurnId) -> Option<WechatPendingReply> {
        self.state
            .lock()
            .expect("wechat service state lock")
            .pending_replies
            .get(turn_id)
            .cloned()
    }

    fn remember_pending_intervention(
        &self,
        workspace_id: WorkspaceId,
        turn_id: Option<TurnId>,
        user_id: String,
        request: InterventionRequest,
        message_ids: Vec<String>,
        visible_texts: Vec<String>,
    ) {
        let req_id = intervention_request_id(&request).clone();
        let quote_ids = visible_texts
            .iter()
            .map(|text| visible_quote_binding_id(text))
            .collect::<Vec<_>>();
        let mut state = self.state.lock().expect("wechat service state lock");
        if !state.pending_interventions.contains_key(&req_id) {
            while state.pending_interventions.len() >= MAX_PENDING_INTERVENTIONS {
                let Some(oldest_req_id) = state.pending_intervention_order.pop_front() else {
                    break;
                };
                state.pending_interventions.remove(&oldest_req_id);
            }
            state.pending_intervention_order.push_back(req_id.clone());
            state.pending_interventions.insert(
                req_id.clone(),
                WechatPendingIntervention {
                    workspace_id,
                    turn_id,
                    request,
                    user_ids: BTreeSet::new(),
                    prompt_message_ids: BTreeSet::new(),
                    prompt_quote_ids: BTreeSet::new(),
                },
            );
        }
        if let Some(pending) = state.pending_interventions.get_mut(&req_id) {
            pending.user_ids.insert(user_id);
            pending.prompt_message_ids.extend(message_ids);
            pending.prompt_quote_ids.extend(quote_ids);
        }
    }

    fn lookup_pending_intervention(&self, message: &WechatIncoming) -> PendingInterventionLookup {
        let state = self.state.lock().expect("wechat service state lock");
        if let Some(message_id) = message.quoted_message_id.as_deref() {
            if let Some(pending) = state
                .pending_interventions
                .values()
                .find(|pending| pending.prompt_message_ids.contains(message_id))
            {
                return PendingInterventionLookup::One(pending.clone());
            }
        }
        if let Some(quoted_text) = message.quoted_text.as_deref() {
            let quote_id = visible_quote_binding_id(quoted_text);
            if let Some(pending) = state
                .pending_interventions
                .values()
                .find(|pending| pending.prompt_quote_ids.contains(&quote_id))
            {
                return PendingInterventionLookup::One(pending.clone());
            }
        }
        let matches = state
            .pending_interventions
            .values()
            .filter(|pending| pending.user_ids.contains(&message.user_id))
            .cloned()
            .collect::<Vec<_>>();
        match matches.len() {
            0 => PendingInterventionLookup::None,
            1 => PendingInterventionLookup::One(matches.into_iter().next().unwrap()),
            _ => PendingInterventionLookup::Multiple,
        }
    }

    fn forget_pending_intervention(&self, req_id: &str) -> Option<WechatPendingIntervention> {
        let mut state = self.state.lock().expect("wechat service state lock");
        let pending = state.pending_interventions.remove(req_id);
        state
            .pending_intervention_order
            .retain(|pending_req_id| pending_req_id.as_str() != req_id);
        pending
    }

    fn remember_pending_notification(
        &self,
        workspace_id: WorkspaceId,
        user_id: String,
        body: SmolStr,
        provider_session_id: ProviderSessionId,
    ) {
        let dropped_oldest = {
            let mut state = self.state.lock().expect("wechat service state lock");
            let dropped_oldest = state.pending_notifications.len() >= MAX_PENDING_NOTIFICATIONS;
            if dropped_oldest {
                state.pending_notifications.pop_front();
            }
            state
                .pending_notifications
                .push_back(WechatPendingNotification {
                    workspace_id,
                    user_id,
                    content: WechatPendingNotificationContent::Text(body),
                    provider_session_id,
                });
            dropped_oldest
        };
        if dropped_oldest {
            warn!(
                target: "lucarne_wechat::service",
                max_pending = MAX_PENDING_NOTIFICATIONS,
                "wechat pending notification queue full; dropped oldest notification"
            );
        }
    }

    fn remember_pending_attachment_notification(
        &self,
        workspace_id: WorkspaceId,
        user_id: String,
        attachment: AgentAttachment,
        provider_session_id: ProviderSessionId,
    ) {
        let dropped_oldest = {
            let mut state = self.state.lock().expect("wechat service state lock");
            let dropped_oldest = state.pending_notifications.len() >= MAX_PENDING_NOTIFICATIONS;
            if dropped_oldest {
                state.pending_notifications.pop_front();
            }
            state
                .pending_notifications
                .push_back(WechatPendingNotification {
                    workspace_id,
                    user_id,
                    content: WechatPendingNotificationContent::Attachment(attachment),
                    provider_session_id,
                });
            dropped_oldest
        };
        if dropped_oldest {
            warn!(
                target: "lucarne_wechat::service",
                max_pending = MAX_PENDING_NOTIFICATIONS,
                "wechat pending notification queue full; dropped oldest notification"
            );
        }
    }

    fn remember_pending_reply_attachment(&self, turn_id: &TurnId, attachment: AgentAttachment) {
        let mut state = self.state.lock().expect("wechat service state lock");
        let Some(pending) = state.pending_replies.get_mut(turn_id) else {
            return;
        };
        if pending
            .pending_attachments
            .iter()
            .any(|existing| existing.id == attachment.id)
        {
            return;
        }
        pending.pending_attachments.push(attachment);
    }

    fn remove_pending_reply_attachment(&self, turn_id: &TurnId, attachment_id: &str) {
        if let Some(pending) = self
            .state
            .lock()
            .expect("wechat service state lock")
            .pending_replies
            .get_mut(turn_id)
        {
            pending
                .pending_attachments
                .retain(|attachment| attachment.id.as_str() != attachment_id);
        }
    }

    fn take_pending_notifications(&self) -> Vec<WechatPendingNotification> {
        self.state
            .lock()
            .expect("wechat service state lock")
            .pending_notifications
            .drain(..)
            .collect()
    }

    fn mark_pending_reply_completed(&self, turn_id: &TurnId) -> Option<WechatPendingReply> {
        let mut state = self.state.lock().expect("wechat service state lock");
        let pending = state.pending_replies.get_mut(turn_id)?;
        pending.terminal = Some(PendingReplyTerminal::Completed);
        Some(pending.clone())
    }

    fn mark_pending_reply_failed(
        &self,
        turn_id: &TurnId,
        text: String,
    ) -> Option<WechatPendingReply> {
        let mut state = self.state.lock().expect("wechat service state lock");
        let pending = state.pending_replies.get_mut(turn_id)?;
        pending.terminal = Some(PendingReplyTerminal::Failed(text.into()));
        Some(pending.clone())
    }

    fn completed_pending_replies(&self) -> Vec<(TurnId, WechatPendingReply)> {
        self.state
            .lock()
            .expect("wechat service state lock")
            .pending_replies
            .iter()
            .filter(|(_, pending)| pending.terminal.is_some())
            .map(|(turn_id, pending)| (turn_id.clone(), pending.clone()))
            .collect()
    }

    #[cfg(test)]
    fn pending_reply_count(&self) -> usize {
        self.state
            .lock()
            .expect("wechat service state lock")
            .pending_replies
            .len()
    }

    #[cfg(test)]
    fn pending_notification_count(&self) -> usize {
        self.state
            .lock()
            .expect("wechat service state lock")
            .pending_notifications
            .len()
    }

    fn take_pending_reply(&self, turn_id: &TurnId) -> Option<WechatPendingReply> {
        let mut state = self.state.lock().expect("wechat service state lock");
        let pending = state.pending_replies.remove(turn_id)?;
        state
            .pending_reply_order
            .retain(|pending_turn_id| pending_turn_id != turn_id);
        let workspace_id = pending.workspace_id.clone();
        let workspace_pending = state
            .pending_replies
            .values()
            .any(|reply| reply.workspace_id == workspace_id);
        if !workspace_pending {
            if let Some(cancel) = state.typing_cancellations.remove(&workspace_id) {
                let _ = cancel.send(());
            }
        }
        Some(pending)
    }

    fn start_typing_keepalive(&self, workspace_id: WorkspaceId, user_id: String) {
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        if let Some(cancel) = self
            .state
            .lock()
            .expect("wechat service state lock")
            .typing_cancellations
            .insert(workspace_id.clone(), cancel_tx)
        {
            let _ = cancel.send(());
        }
        let transport = Arc::clone(&self.transport);
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let interval = self.typing_keepalive_interval;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {
                        if rate_limiter.remaining().is_some() {
                            continue;
                        }
                        let context = match transport.context_for_user(&user_id).await {
                            Ok(Some(ctx)) => ctx,
                            Ok(None) => {
                                warn!(
                                    target: "lucarne_wechat::service",
                                    workspace_id = %workspace_id.as_str(),
                                    user_id = %user_id,
                                    "wechat typing keepalive stopped: no stored context for user"
                                );
                                return;
                            }
                            Err(err) => {
                                warn!(
                                    target: "lucarne_wechat::service",
                                    workspace_id = %workspace_id.as_str(),
                                    user_id = %user_id,
                                    error = %err,
                                    "wechat typing keepalive stopped after context_for_user failed"
                                );
                                return;
                            }
                        };
                        match transport.send_typing(&context).await {
                            Ok(()) => {}
                            Err(WechatError::RateLimited {
                                retry_after,
                                message,
                            }) => {
                                rate_limiter.defer(retry_after);
                                warn!(
                                    target: "lucarne_wechat::service",
                                    workspace_id = %workspace_id.as_str(),
                                    user_id = %user_id,
                                    retry_after_secs = retry_after.as_secs(),
                                    error = %message,
                                    "wechat typing keepalive rate limited; pausing outbound sends"
                                );
                            }
                            Err(err) => {
                                warn!(
                                    target: "lucarne_wechat::service",
                                    workspace_id = %workspace_id.as_str(),
                                    user_id = %user_id,
                                    error = %err,
                                    "wechat typing keepalive stopped after send_typing failed"
                                );
                                return;
                            }
                        }
                    }
                    _ = &mut cancel_rx => return,
                }
            }
        });
    }

    fn stop_typing_keepalive(&self, workspace_id: &WorkspaceId) {
        if let Some(cancel) = self
            .state
            .lock()
            .expect("wechat service state lock")
            .typing_cancellations
            .remove(workspace_id)
        {
            let _ = cancel.send(());
        }
    }
}

fn is_retryable_wechat_attachment_error(err: &WechatError) -> bool {
    !matches!(
        err,
        WechatError::RateLimited { .. } | WechatError::MissingMessageContext(_)
    )
}

fn attachment_binding_text(attachment: &AgentAttachment) -> String {
    attachment
        .caption
        .as_ref()
        .map(|caption| caption.to_string())
        .unwrap_or_else(|| format!("[附件：{}]", attachment.filename))
}

fn render_attachment_delivery_failure(attachment: &AgentAttachment, error: &str) -> String {
    let error = error.trim();
    let error = if error.is_empty() {
        "未知附件发送错误"
    } else {
        error
    };
    format!(
        "附件发送失败。\n\n文件：{}\n媒体类型：{}\n附件 ID：{}\n错误：{}",
        attachment.filename, attachment.media_type, attachment.id, error
    )
}

fn render_agent_message(
    text: &str,
    workspace: &WorkspaceBinding,
    session_ref: &str,
    cost: Option<String>,
) -> String {
    let (text, extracted_cost) = split_wechat_trailing_cost(text);
    let mut footer_lines = Vec::new();
    if let Some(cost) = cost.as_deref().or(extracted_cost) {
        footer_lines.push(format!("耗时：{cost}"));
    }
    footer_lines.push(format!("会话：{session_ref}"));
    footer_lines.push(format!(
        "目录：{}",
        compact_path(&workspace.project_path.display().to_string(), 58)
    ));
    let text = text.trim();
    if footer_lines.is_empty() {
        text.to_string()
    } else if text.is_empty() {
        footer_lines.join("\n")
    } else {
        format!("{}\n\n ---\n\n{}", text, footer_lines.join("\n"))
    }
}

fn split_wechat_trailing_cost(text: &str) -> (&str, Option<&str>) {
    let trimmed = text.trim_end();
    let Some((before, line)) = trimmed.rsplit_once('\n') else {
        return (text.trim(), None);
    };
    if !before.ends_with('\n') {
        return (text.trim(), None);
    }
    let line = line.trim();
    let cost = line
        .strip_prefix("cost:")
        .or_else(|| line.strip_prefix("耗时:"))
        .or_else(|| line.strip_prefix("耗时："))
        .map(str::trim)
        .filter(|cost| !cost.is_empty());
    match cost {
        Some(cost) => (before.trim_end(), Some(cost)),
        None => (text.trim(), None),
    }
}

fn render_wechat_workspace_status(
    workspace: &WorkspaceBinding,
    status: Option<&StatusSnapshot>,
    resource_snapshot: &AgentResourceSnapshot,
) -> String {
    let mut body = format!(
        "📊 状态\n工作区：`{}`\n目录：`{}`",
        workspace.workspace_id.as_str(),
        workspace.project_path.display()
    );
    if let Some(status) = status {
        if let Some(version) = status.provider_version.as_deref() {
            body.push_str(&format!("\n版本：`{version}`"));
        }
        if let Some(provider) = status.provider_id.as_ref() {
            body.push_str(&format!("\n提供方：`{provider}`"));
        }
        if let Some(model) = status.model.as_deref() {
            body.push_str(&format!("\n模型：`{model}`"));
            match (status.model_detail.as_deref(), status.reasoning.as_deref()) {
                (Some(detail), Some(reasoning)) => {
                    body.push_str(&format!("（`{detail}`，推理 {reasoning}）"));
                }
                (Some(detail), None) => body.push_str(&format!("（`{detail}`）")),
                (None, Some(reasoning)) => {
                    body.push_str(&format!("（推理 {reasoning}）"));
                }
                (None, None) => {}
            }
        }
        if let Some(directory) = status.directory.as_deref() {
            body.push_str(&format!("\n目录：`{directory}`"));
        }
        if let Some(permissions) = status.permission_mode.as_deref() {
            body.push_str(&format!("\n权限：`{permissions}`"));
        }
        if let Some(path) = status.agents_md.as_deref() {
            body.push_str(&format!("\nAGENTS.md：`{path}`"));
        }
        if let Some(account) = status.account.as_deref() {
            body.push_str(&format!("\n账号：{account}"));
        }
        if let Some(base_url) = status.base_url.as_deref() {
            body.push_str(&format!("\n基础 URL：`{base_url}`"));
        }
        if let Some(proxy) = status.proxy.as_deref() {
            body.push_str(&format!("\n代理：`{proxy}`"));
        }
        if let Some(sources) = status.setting_sources.as_deref() {
            body.push_str(&format!("\n配置来源：{sources}"));
        }
        if let Some(session) = status
            .native_resume_ref
            .as_deref()
            .or_else(|| status.provider_session_id.as_ref().map(|id| id.as_str()))
        {
            body.push_str(&format!("\n会话：`{session}`"));
        }
        let token_usage = status.token_usage.clone().or_else(|| {
            status
                .usage_snapshot
                .as_ref()
                .and_then(|usage| serde_json::from_value::<AgentTokenUsage>(usage.clone()).ok())
        });
        if let Some(tokens) = &token_usage {
            if tokens.input_tokens.is_some() || tokens.output_tokens.is_some() {
                body.push_str(&format!(
                    "\nToken 用量：{} 输入 / {} 输出",
                    compact_number(tokens.input_tokens.unwrap_or(0)),
                    compact_number(tokens.output_tokens.unwrap_or(0))
                ));
            }
        }
        let context_usage = status.context_usage.clone().or_else(|| {
            status.context_snapshot.as_ref().and_then(|context| {
                serde_json::from_value::<AgentContextUsage>(context.clone()).ok()
            })
        });
        if let Some(context) = &context_usage {
            if let (Some(used), Some(max)) = (context.used_tokens, context.max_tokens) {
                let percent = context.percent_used.unwrap_or_else(|| {
                    if max == 0 {
                        0
                    } else {
                        ((used as f64 / max as f64) * 100.0).round() as u8
                    }
                });
                body.push_str(&format!(
                    "\n上下文：{}/{}（{}%）",
                    compact_number(used),
                    compact_number(max),
                    percent
                ));
                if let Some(compactions) = status.compactions {
                    body.push_str(&format!(" · 压缩：{compactions}"));
                }
            }
        }
        if let Some(state) = status.live_instance_state {
            body.push_str(&format!("\n运行状态：`{}`", format_live_state(state)));
        }
        if let Some(binding) = status.channel_binding_state.as_deref() {
            body.push_str(&format!("\n通道：`{binding}`"));
        }
    }
    if let Some(resource) = resource_snapshot.agents.first() {
        render_wechat_resource(resource, &mut body);
    }
    body
}

fn render_wechat_agent_resource_snapshot(snapshot: &AgentResourceSnapshot) -> String {
    let mut body = "### 📊 Agent 资源\n\n".to_string();
    begin_wechat_markdown_table(&mut body);
    push_wechat_markdown_table_line(
        &mut body,
        "| 托管 agent | 最近历史会话 | 实际进程 | CPU | 内存 |",
    );
    push_wechat_markdown_table_line(&mut body, "|---:|---:|---:|---:|---:|");
    push_wechat_markdown_table_line(
        &mut body,
        &format!(
            "| {} | {} | {} | {:.1}% | {} |",
            snapshot.managed_agent_count,
            snapshot.observed_sessions.len(),
            snapshot.process_count,
            snapshot.total_cpu_percent,
            wechat_table_cell(format_resource_bytes(snapshot.total_memory_bytes))
        ),
    );

    end_wechat_markdown_table(&mut body);

    if !snapshot.agents.is_empty() {
        body.push_str("\n### 托管 agent\n\n");
        begin_wechat_markdown_table(&mut body);
        push_wechat_markdown_table_line(
            &mut body,
            "| 标题 | 工作区 | 提供方 | 会话 | PID | 进程 | CPU | 内存 | 最近活跃 |",
        );
        push_wechat_markdown_table_line(&mut body, "|---|---|---|---|---:|---:|---:|---:|---|");
        for agent in &snapshot.agents {
            push_wechat_markdown_table_line(
                &mut body,
                &format!(
                    "| {} | {} | {} | {} | {} | {} | {:.1}% | {} | {} |",
                    wechat_table_cell(agent.title.as_str()),
                    wechat_table_cell(agent.workspace_id.as_str()),
                    wechat_table_cell(agent.provider_id),
                    wechat_table_cell(agent.native_resume_ref.as_str()),
                    wechat_table_cell(
                        agent
                            .pid
                            .map(|pid| pid.to_string())
                            .unwrap_or_else(|| "未知".to_string()),
                    ),
                    agent.process_count,
                    agent.cpu_percent,
                    wechat_table_cell(format_resource_bytes(agent.memory_bytes)),
                    wechat_table_cell(non_empty_or_unknown(&agent.last_active_display)),
                ),
            );
        }
        end_wechat_markdown_table(&mut body);
    }

    if !snapshot.observed_sessions.is_empty() {
        body.push_str("\n### 最近历史会话\n\n");
        begin_wechat_markdown_table(&mut body);
        push_wechat_markdown_table_line(&mut body, "| 标题 | 提供方 | 会话 | 最近活跃 | 目录 |");
        push_wechat_markdown_table_line(&mut body, "|---|---|---|---|---|");
        for session in &snapshot.observed_sessions {
            let cwd = session
                .cwd
                .as_ref()
                .map(|cwd| compact_path(&cwd.display().to_string(), 48))
                .unwrap_or_else(|| "未知".to_string());
            push_wechat_markdown_table_line(
                &mut body,
                &format!(
                    "| {} | {} | {} | {} | {} |",
                    wechat_table_cell(session.title.as_str()),
                    wechat_table_cell(session.provider_id),
                    wechat_table_cell(session.native_resume_ref.as_str()),
                    wechat_table_cell(non_empty_or_unknown(&session.last_active_display)),
                    wechat_table_cell(cwd),
                ),
            );
        }
        end_wechat_markdown_table(&mut body);
    }
    body
}

fn begin_wechat_markdown_table(body: &mut String) {
    body.push_str("");
}

fn end_wechat_markdown_table(body: &mut String) {
    body.push_str("");
}

fn push_wechat_markdown_table_line(body: &mut String, line: &str) {
    body.push_str(line);
    body.push('\n');
}

fn wechat_table_cell(value: impl AsRef<str>) -> String {
    let value = value
        .as_ref()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let value = value.trim();
    if value.is_empty() {
        return "未知".to_string();
    }
    value.replace('`', "\\`").replace('|', "\\|")
}

fn render_wechat_kill_agent_report(report: &KillAgentReport) -> String {
    let mut body = format!("🛑 终止 agent\n已终止：`{}`", report.killed.len());
    for killed in &report.killed {
        body.push_str("\n\n");
        body.push_str(killed.identity.as_deref().unwrap_or("未识别"));
        body.push('\n');
        body.push_str(&format!(
            "工作区：`{}`\n会话：`{}`\nPID：`{}`",
            killed.workspace_id.as_str(),
            killed.native_resume_ref,
            killed
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "未知".to_string()),
        ));
    }
    if let Some(identity) = report.not_found.as_deref() {
        body.push_str(&format!("\n\n未找到：`{identity}`"));
    }
    body
}

fn non_empty_or_unknown(value: &str) -> &str {
    if value.trim().is_empty() {
        "未知"
    } else {
        value
    }
}

fn format_live_state(state: lucarne::control_plane::LiveInstanceState) -> &'static str {
    match state {
        lucarne::control_plane::LiveInstanceState::Starting => "启动中",
        lucarne::control_plane::LiveInstanceState::Idle => "空闲",
        lucarne::control_plane::LiveInstanceState::Running => "运行中",
        lucarne::control_plane::LiveInstanceState::WaitingPermission => "等待授权",
        lucarne::control_plane::LiveInstanceState::Closing => "关闭中",
        lucarne::control_plane::LiveInstanceState::Closed => "已关闭",
        lucarne::control_plane::LiveInstanceState::Failed => "失败",
        lucarne::control_plane::LiveInstanceState::Stale => "已过期",
    }
}

fn render_wechat_resource(resource: &AgentResourceEntry, body: &mut String) {
    body.push_str(&format!(
        "\n进程标识：`{}`\nPID：`{}`\n进程数：`{}`\nCPU：`{:.1}%`\n内存：`{}`",
        resource.identity.as_deref().unwrap_or("未识别"),
        resource
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "未知".to_string()),
        resource.process_count,
        resource.cpu_percent,
        format_resource_bytes(resource.memory_bytes),
    ));
}

fn format_resource_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

fn compact_number(value: u64) -> String {
    if value >= 1_000_000 {
        trim_number(value as f64 / 1_000_000.0, "m")
    } else if value >= 1_000 {
        trim_number(value as f64 / 1_000.0, "k")
    } else {
        value.to_string()
    }
}

fn trim_number(value: f64, suffix: &str) -> String {
    let rendered = format!("{value:.1}");
    format!(
        "{}{}",
        rendered.trim_end_matches('0').trim_end_matches('.'),
        suffix
    )
}

fn sent_quote_binding_id(text: &str) -> String {
    quote_binding_id(text)
}

fn visible_quote_binding_id(text: &str) -> String {
    quote_binding_id(text)
}

fn quote_binding_id(text: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in text.trim().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("quote:{hash:016x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigCommand {
    Show,
    SetGlobalBypass(bool),
    SetGlobalNotifications(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SlashCommand {
    Help,
    New,
    Status,
    Kill(KillAgentTarget),
    InvalidKill,
}

#[derive(Debug, Clone)]
enum SlashScope {
    Global,
    Workspace(WorkspaceBinding),
}

fn parse_config_command(text: &str) -> Option<ConfigCommand> {
    let mut parts = text.split_whitespace();
    if parts.next()? != "/config" {
        return None;
    }
    let rest = parts.collect::<Vec<_>>();
    match rest.as_slice() {
        [] => Some(ConfigCommand::Show),
        ["global", "bypass", value] => parse_on_off(value).map(ConfigCommand::SetGlobalBypass),
        ["global", "notifications", value] => {
            parse_on_off(value).map(ConfigCommand::SetGlobalNotifications)
        }
        _ => Some(ConfigCommand::Show),
    }
}

fn parse_slash_command(text: &str) -> Option<SlashCommand> {
    let trimmed = text.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let mut parts = trimmed.split_whitespace();
    let name = parts
        .next()?
        .trim_start_matches('/')
        .split('@')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    match name.as_str() {
        "help" => Some(SlashCommand::Help),
        "new" => Some(SlashCommand::New),
        "status" => Some(SlashCommand::Status),
        "kill" => {
            let args = parts.collect::<Vec<_>>().join(" ");
            parse_kill_target(&args)
                .map(SlashCommand::Kill)
                .or(Some(SlashCommand::InvalidKill))
        }
        _ => None,
    }
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

fn parse_on_off(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "on" => Some(true),
        "off" => Some(false),
        _ => None,
    }
}

fn on_off(enabled: bool) -> &'static str {
    if enabled {
        "开启"
    } else {
        "关闭"
    }
}

fn render_global_config(settings: &lucarne::control_plane::EffectiveSettings) -> String {
    format!(
        "⚙️ 配置\n范围：全局\n权限绕过：{}\n通知：{}",
        on_off(settings.session.force_bypass_permissions),
        on_off(settings.notifications.enabled)
    )
}

fn render_wechat_new_usage() -> &'static str {
    "请引用一条 agent 通知后发送 `/new`，为该工作区开启新会话。"
}

fn render_wechat_new_session(
    provider_id: &str,
    session_ref: &str,
    workspace: &WorkspaceBinding,
) -> String {
    format!(
        "🆕 新的 {provider_id} 会话\n目录：`{}`\n会话：`{session_ref}`\n\n回复此消息即可开始。",
        workspace.project_path.display()
    )
}

fn render_wechat_startup_help() -> String {
    format!("👋 lucarned 已启动。\n\n{}", render_wechat_help())
}

fn render_wechat_help() -> &'static str {
    "📖 命令\n\
\n\
| 命令 | 说明 |\n\
|---|---|\n\
| `/help` | 显示帮助 |\n\
| `/config` | 查看全局配置 |\n\
| `/config global bypass on/off` | 开关全局权限绕过 |\n\
| `/config global notifications on/off` | 开关全局通知 |\n\
| `/status` | 查看全局状态；引用通知时查看该会话状态 |\n\
| `/new` | 引用通知后开启该工作区的新会话 |\n\
| `/kill all / <session_id:pid>` | 终止 agent 进程 |\n\
\n⚠️\n\
微信主动通知有频率和会话时效限制（可在配置文件配置）。如果长时间没收到 agent 消息，可以随便发一条消息刷新维持会话。\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use agent_sessions::{agent_provider, ParseSelection, WatchConfig};
    use futures::stream;
    use lucarne::{
        agent_runtime::events::{TurnCompletedEvent, TurnFailedEvent},
        agent_runtime::{
            AgentError, AgentErrorKind, AgentEventStream, AgentProvider, AgentSession, AgentStatus,
            ApprovalDecision, ApprovalRequest, InstanceId, InterventionRequest,
            InterventionResponse, MessageEvent, OpenSession, ProbeResult, ProviderId, Question,
            QuestionOption, QuestionRequest, ResumeSession, SessionId,
        },
        control_plane::ControlPlaneSqliteStore,
        core_service::OpenWorkspaceRequest,
    };
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::mpsc;

    #[test]
    fn agent_notification_message_uses_panel_compact_cwd() {
        let workspace = WorkspaceBinding::new(
            WorkspaceId::new("workspace-1"),
            "lucarnex",
            "codex",
            "/Volumes/Data/opensource/conductor/lucarnex",
        );

        let body = render_agent_message("done", &workspace, "thread-1", None);

        assert!(body.contains("目录：…/opensource/conductor/lucarnex"));
        assert!(!body.contains("/Volumes/Data"));
    }

    #[test]
    fn wechat_agent_resource_snapshot_uses_markdown_tables_for_observed_sessions() {
        let snapshot = AgentResourceSnapshot {
            managed_agent_count: 0,
            process_count: 0,
            total_cpu_percent: 0.0,
            total_memory_bytes: 0,
            agents: Vec::new(),
            observed_sessions: vec![lucarne::core_service::ObservedAgentSession {
                workspace_id: WorkspaceId::new("codex:resume:observed"),
                provider_id: "codex",
                provider_session_id: ProviderSessionId::new("codex:thread-1"),
                native_resume_ref: "thread-1".into(),
                title: "real user title".into(),
                cwd: Some(PathBuf::from("/Volumes/Data/crypto/meme/meme-strategy-me")),
                session_path: PathBuf::from("/tmp/rollout-thread-1.jsonl"),
                last_active_unix: 1_776_960_000,
                last_active_display: "05-23 15:50:34".into(),
                observed_pid: None,
            }],
        };

        let body = render_wechat_agent_resource_snapshot(&snapshot);

        assert!(body.contains("### 📊 Agent 资源"));
        assert!(body.contains("| 托管 agent | 最近历史会话 | 实际进程 | CPU | 内存 |"));
        assert!(body.contains("| 0 | 1 | 0 | 0.0% | 0 B |"));
        assert!(body.contains("### 最近历史会话"));
        assert!(body.contains("| 标题 | 提供方 | 会话 | 最近活跃 | 目录 |"));
        assert!(body.contains("real user title"));
        assert!(body.contains("thread-1"));
        assert!(body.contains("meme-strategy-me"));

        assert!(!body.contains("```"));
        assert!(!body.contains("| 指标 | 值 |"));
        assert!(body.contains("| 标题 | 提供方 | 会话 | 最近活跃 | 目录 |"));
        assert!(body.contains("| real user title | codex | thread-1 |"));
    }

    fn compact_cwd_footer(path: &Path) -> String {
        format!("目录：{}", compact_path(&path.display().to_string(), 58))
    }

    #[tokio::test]
    async fn notification_hub_binds_replies_and_returns_agent_output_to_reply_message() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-a".into()),
                title: "workspace-a".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].user_id, "user-1");
        assert!(sends[0].text.contains("background complete"));
        assert!(sends[0].text.contains("会话：thread-1"));
        assert!(sends[0].text.contains("目录：/tmp/workspace-a"));
        assert!(
            core.message_session_binding("wechat", "user-1", &sends[0].message_id)
                .is_some(),
            "notification message should be bound to the provider session"
        );
        assert!(
            core.message_session_binding(
                "wechat",
                "user-1",
                &sent_quote_binding_id(&sends[0].text)
            )
            .is_some(),
            "notification text should be bound for WeChat quote payloads that omit ids"
        );

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(sends[0].message_id.clone()),
            ))
            .await
            .expect("submit reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].user_id, "user-1");
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
        assert!(
            replies[0]
                .text
                .contains(" ---\n\n耗时：0s\n会话：thread-1\n目录：/tmp/workspace-a"),
            "assistant reply footer must keep cost/session/cwd in one shared block: {}",
            replies[0].text
        );
        assert!(
            core.message_session_binding("wechat", "user-1", &replies[0].message_id)
                .is_some(),
            "assistant reply should also be bound so the user can keep replying"
        );
        assert!(
            core.message_session_binding(
                "wechat",
                "user-1",
                &sent_quote_binding_id(&replies[0].text)
            )
            .is_some(),
            "assistant reply text should be bound for continued quoted replies"
        );
        assert!(!core.direct_notification_suppressed(&workspace_id));
    }

    #[tokio::test]
    async fn quoted_reply_reports_agent_start_error_to_wechat_user() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-a".into()),
                title: "workspace-a".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let live_instance_id =
            lucarne::control_plane::LiveInstanceId::new(opened.session.instance_id().0.as_str());
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        core.detach_live_session(&workspace_id, &live_instance_id, "test detached")
            .await
            .expect("detach live session");
        provider.set_resume_failure("adapter: resolve command \"pi\" in PATH");

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(transport.sends()[0].message_id.clone()),
            ))
            .await
            .expect("report start error");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(
            replies[0].text.contains("⚠️ agent 启动失败"),
            "{}",
            replies[0].text
        );
        assert!(
            replies[0].text.contains("resolve command \"pi\" in PATH"),
            "{}",
            replies[0].text
        );
    }

    #[tokio::test]
    async fn system_update_notifications_send_to_configured_and_known_users_without_binding() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let transport = Arc::new(FakeTransport::default());
        let service = Arc::new(WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        ));
        service.remember_user("user-2");

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let system_notifications = lucarne_adapter::SystemNotificationBus::new(8);
        let receiver = system_notifications.subscribe();
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move {
                service
                    .run_until_shutdown_with_system_notifications(shutdown_rx, receiver)
                    .await
            }
        });

        assert_eq!(
            system_notifications.send(lucarne_adapter::SystemNotification::UpdateAvailable(
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
            )),
            1
        );

        for _ in 0..20 {
            let update_send_count = transport
                .sends()
                .iter()
                .filter(|send| send.text.contains("Lucarne update available"))
                .count();
            if update_send_count >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let sends = transport
            .sends()
            .into_iter()
            .filter(|send| send.text.contains("Lucarne update available"))
            .collect::<Vec<_>>();
        assert_eq!(sends.len(), 2);
        let sent_users = sends
            .iter()
            .map(|send| send.user_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(sent_users, BTreeSet::from(["user-1", "user-2"]));
        for send in &sends {
            assert!(send.text.contains("Lucarne update available"));
            assert!(
                core.message_session_binding("wechat", &send.user_id, &send.message_id)
                    .is_none(),
                "system updates must not bind provider sessions"
            );
        }

        shutdown_tx.send(true).expect("request shutdown");
        tokio::time::timeout(Duration::from_secs(1), run)
            .await
            .expect("service exits after shutdown")
            .expect("service task")
            .expect("service result");
    }

    #[tokio::test]
    async fn intervention_approval_prompts_wechat_and_allow_reply_resolves_provider() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-approval".into()),
            title: "workspace-approval".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_intervention(
            "thread-1",
            InterventionRequest::Approval(ApprovalRequest {
                req_id: "approval-1".into(),
                tool_name: "apply_patch".into(),
                message: Some("需要修改文件".into()),
                input: Some(serde_json::json!({ "cmd": "apply_patch", "path": "src/main.rs" })),
            }),
        );
        service
            .handle_core_event(next_intervention_event(&mut events).await)
            .await
            .expect("send approval prompt");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].user_id, "user-1");
        assert!(sends[0].text.contains("## 需要授权"), "{}", sends[0].text);
        assert!(
            sends[0].text.contains("**工具**：`apply_patch`"),
            "{}",
            sends[0].text
        );
        assert!(sends[0].text.contains("```json"), "{}", sends[0].text);
        assert!(sends[0].text.contains("回复 `允许`"), "{}", sends[0].text);

        service
            .handle_incoming(WechatIncoming::new(
                "decision-1",
                "user-1",
                "允许",
                Option::<String>::None,
            ))
            .await
            .expect("resolve approval");

        assert_eq!(
            provider.resolved_interventions(),
            vec![(
                "approval-1".to_string(),
                InterventionResponse::Approval(ApprovalDecision::Allow),
            )]
        );
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].text.contains("已允许"), "{}", replies[0].text);
    }

    #[tokio::test]
    async fn intervention_approval_response_resumes_typing_for_pending_reply_turn() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-approval-typing".into()),
                title: "workspace-approval-typing".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        );
        let turn_id = TurnId::new("turn-approval-typing");
        let source = WechatIncoming::new("incoming-1", "user-1", "改一下", Option::<String>::None);
        service.remember_pending_reply(turn_id.clone(), workspace_id.clone(), source.clone());

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: workspace_id.clone(),
                turn_id: Some(turn_id),
                event: AgentEvent::InterventionRequest(InterventionRequest::Approval(
                    ApprovalRequest {
                        req_id: "approval-typing".into(),
                        tool_name: "apply_patch".into(),
                        message: Some("需要修改文件".into()),
                        input: None,
                    },
                )),
            })
            .await
            .expect("send approval prompt");
        assert_eq!(
            transport.typing_count("user-1"),
            0,
            "approval prompt should not look like agent output is still being generated"
        );

        service
            .handle_incoming(WechatIncoming::new(
                "decision-1",
                "user-1",
                "允许",
                Option::<String>::None,
            ))
            .await
            .expect("resolve approval");
        wait_until(|| transport.typing_count("user-1") > 0).await;
    }

    #[tokio::test]
    async fn intervention_question_prompts_wechat_and_answer_reply_resolves_provider() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-question".into()),
            title: "workspace-question".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_intervention(
            "thread-1",
            InterventionRequest::Question(QuestionRequest {
                req_id: "question-1".into(),
                questions: vec![Question {
                    header: Some("选择分支".into()),
                    text: "要切到哪个分支？".into(),
                    options: vec![
                        QuestionOption {
                            label: "main".into(),
                            description: Some("稳定分支".into()),
                        },
                        QuestionOption {
                            label: "feature".into(),
                            description: Some("功能分支".into()),
                        },
                    ],
                    multi_select: false,
                }],
            }),
        );
        service
            .handle_core_event(next_intervention_event(&mut events).await)
            .await
            .expect("send question prompt");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].text.contains("## 需要你回答"), "{}", sends[0].text);
        assert!(
            sends[0].text.contains("### 1. 选择分支"),
            "{}",
            sends[0].text
        );
        assert!(
            sends[0].text.contains("- `A` **main** — 稳定分支"),
            "{}",
            sends[0].text
        );

        service
            .handle_incoming(WechatIncoming::new(
                "answer-1",
                "user-1",
                "B",
                Option::<String>::None,
            ))
            .await
            .expect("resolve question");

        assert_eq!(
            provider.resolved_interventions(),
            vec![(
                "question-1".to_string(),
                InterventionResponse::Answers(lucarne::agent_runtime::QuestionResponse {
                    answers: vec![lucarne::agent_runtime::QuestionAnswer {
                        values: vec!["feature".into()],
                    }],
                }),
            )]
        );
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(
            replies[0].text.contains("已提交回答"),
            "{}",
            replies[0].text
        );
    }

    #[tokio::test]
    async fn slash_status_without_quote_returns_global_agent_resources() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-a".into()),
            title: "workspace-a".into(),
        })
        .await
        .expect("open workspace");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "status-1",
                "user-1",
                "/status",
                Option::<String>::None,
            ))
            .await
            .expect("handle status");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].text.contains("📊 Agent 资源"));
        assert!(replies[0]
            .text
            .contains("| 托管 agent | 最近历史会话 | 实际进程 | CPU | 内存 |"));
        assert!(replies[0].text.contains("| 1 | 0 |"));
        assert!(replies[0]
            .text
            .contains("| 标题 | 工作区 | 提供方 | 会话 | PID |"));
        assert!(replies[0].text.contains("workspace-a"));
        assert!(replies[0].text.contains("thread-1"));
        assert!(replies[0].text.contains(&std::process::id().to_string()));
    }

    #[tokio::test]
    async fn slash_status_with_quote_returns_that_agent_resources() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-a".into()),
                title: "workspace-a".into(),
            })
            .await
            .expect("open workspace");
        let provider_session_id = core
            .active_provider_session_id(&opened.workspace.workspace_id)
            .expect("provider session");
        core.bind_message_to_provider_session("wechat", "user-1", "notify-1", provider_session_id)
            .expect("bind quote");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "status-1",
                "user-1",
                "/status",
                Some("notify-1"),
            ))
            .await
            .expect("handle status");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].text.contains("📊 状态"));
        assert!(replies[0].text.contains("工作区：`"));
        assert!(replies[0].text.contains("目录：`/tmp/workspace-a`"));
        assert!(replies[0].text.contains("会话：`thread-1`"));
        assert!(replies[0].text.contains("运行状态：`空闲`"));
        assert!(replies[0]
            .text
            .contains(&format!("进程标识：`thread-1:{}", std::process::id())));
    }

    #[tokio::test]
    async fn notification_reply_status_invokes_provider_status_like_telegram_topic() {
        let provider = Arc::new(FakeProvider::with_status(AgentStatus {
            version: Some("codex-test".into()),
            model: Some("gpt-5".into()),
            model_detail: Some("high".into()),
            reasoning: Some("max".into()),
            permissions: Some("danger-full-access".into()),
            directory: Some("/tmp/workspace-a".into()),
            tokens: Some(AgentTokenUsage {
                input_tokens: Some(1234),
                output_tokens: Some(567),
                total_tokens: Some(1801),
            }),
            context: Some(AgentContextUsage {
                used_tokens: Some(42_000),
                max_tokens: Some(128_000),
                percent_used: Some(33),
            }),
            compactions: Some(2),
            ..Default::default()
        }));
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-a".into()),
                title: "workspace-a".into(),
            })
            .await
            .expect("open workspace");
        let provider_session_id = core
            .active_provider_session_id(&opened.workspace.workspace_id)
            .expect("provider session");
        core.bind_message_to_provider_session("wechat", "user-1", "notify-1", provider_session_id)
            .expect("bind quote");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "status-1",
                "user-1",
                "/status",
                Some("notify-1"),
            ))
            .await
            .expect("handle status");

        assert_eq!(provider.status_calls(), 1);
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].text.contains("版本：`codex-test`"));
        assert!(replies[0]
            .text
            .contains("模型：`gpt-5`（`high`，推理 max）"));
        assert!(replies[0].text.contains("权限：`danger-full-access`"));
        assert!(replies[0].text.contains("Token 用量：1.2k 输入 / 567 输出"));
        assert!(replies[0]
            .text
            .contains("上下文：42k/128k（33%） · 压缩：2"));
        assert!(replies[0]
            .text
            .contains(&format!("进程标识：`thread-1:{}", std::process::id())));
    }

    #[tokio::test]
    async fn slash_status_with_stale_quote_reports_no_longer_routable() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "status-1",
                "user-1",
                "/status",
                Some("missing-notify"),
            ))
            .await
            .expect("handle status");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].text, WECHAT_STALE_NOTIFICATION);
    }

    #[tokio::test]
    async fn slash_new_with_quote_starts_new_session_and_binds_reply() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-new".into()),
                title: "workspace-new".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "new-1",
                "user-1",
                "/new",
                Some(notification),
            ))
            .await
            .expect("start new session");

        assert_eq!(
            core.active_provider_session_ref(&workspace_id).unwrap(),
            "thread-2"
        );
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "new-1");
        assert!(replies[0].text.contains("🆕 新的 codex 会话"));
        assert!(replies[0].text.contains("会话：`thread-2`"));
        let ack_message_id = replies[0].message_id.clone();
        let binding = core
            .message_session_binding("wechat", "user-1", &ack_message_id)
            .expect("new-session acknowledgement should be bound");
        assert_eq!(binding.provider_session_id.as_str(), "codex:thread-2");

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-new",
                "user-1",
                "hello new session",
                Some(ack_message_id),
            ))
            .await
            .expect("submit reply to new session");
        deliver_until_reply_count(&service, &mut events, &transport, 2).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[1].reply_to_message_id, "user-reply-new");
        assert!(replies[1].text.contains("reply: hello new session"));
        assert!(replies[1].text.contains("会话：thread-2"));
    }

    #[tokio::test]
    async fn slash_new_without_quote_explains_reply_requirement() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "new-1",
                "user-1",
                "/new",
                Option::<String>::None,
            ))
            .await
            .expect("handle new");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].text, render_wechat_new_usage());
    }

    #[tokio::test]
    async fn config_global_bypass_toggle_updates_shared_system_setting() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "config-1",
                "user-1",
                "/config global bypass on",
                Option::<String>::None,
            ))
            .await
            .expect("handle config bypass");
        service
            .handle_incoming(WechatIncoming::new(
                "config-2",
                "user-1",
                "/config",
                Option::<String>::None,
            ))
            .await
            .expect("show config");

        assert!(core.system_settings().session.force_bypass_permissions);
        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        for reply in &replies {
            assert!(reply.text.contains("⚙️ 配置"));
            assert!(reply.text.contains("范围：全局"));
            assert!(reply.text.contains("权限绕过：开启"));
            assert!(reply.text.contains("通知：开启"));
        }
    }

    #[tokio::test]
    async fn config_global_notifications_toggle_persists_yaml_update_when_hook_present() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        core.set_force_bypass_permissions(true)
            .expect("set initial bypass");
        let transport = Arc::new(FakeTransport::default());
        let persistence = Arc::new(RecordingGlobalConfigPersistence::default());
        let persistence_hook: Arc<dyn GlobalConfigPersistence> = persistence.clone();
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                global_config_persistence: Some(persistence_hook),
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_incoming(WechatIncoming::new(
                "config-1",
                "user-1",
                "/config global notifications off",
                Option::<String>::None,
            ))
            .await
            .expect("handle config notifications");

        assert!(!core.system_settings().notifications.enabled);
        assert_eq!(
            persistence.updates(),
            vec![GlobalConfigUpdate {
                bypass: true,
                notifications: false,
            }]
        );
    }

    #[tokio::test]
    async fn slash_help_replies_with_wechat_commands() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "help-1",
                "user-1",
                "/help",
                Option::<String>::None,
            ))
            .await
            .expect("handle help");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0].text.contains("📖 命令"));
        assert!(replies[0].text.contains("| 命令 | 说明 |"));
        assert!(replies[0].text.contains("`/config global bypass on/off`"));
        assert!(replies[0].text.contains("`/status`"));
        assert!(replies[0].text.contains("`/new`"));
        assert!(replies[0].text.contains("微信主动通知有频率和会话时效限制"));
        assert!(!replies[0].text.contains(WECHAT_REPLY_REQUIRED));
    }

    #[tokio::test]
    async fn pi_created_history_watch_reaches_wechat_as_single_costed_notification() {
        let temp = tempfile::TempDir::new().expect("pi chain temp dir");
        let root = temp.path().join("pi-sessions");
        let project = temp.path().join("project");
        fs::create_dir_all(&root).expect("pi sessions dir");
        fs::create_dir_all(&project).expect("project dir");
        let provider = Arc::new(FakeProvider::new("pi", ""));
        let core = test_core(Arc::clone(&provider));
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );
        start_pi_history_watch(&core, &root).await;
        wait_for_history_watch_baseline().await;

        write_completed_pi_history_session(
            &root,
            "pi-created-complete",
            &project,
            "现在几点了",
            "现在 22:15:05 CST（UTC+8）。",
        );
        assert!(
            poll_core_event_for_wechat(&service, &mut events, Duration::from_secs(2)).await,
            "created Pi history completion should reach WeChat"
        );

        let sends = transport.sends();
        assert_eq!(sends.len(), 1, "created Pi completion should send once");
        assert!(
            sends[0].text.contains("现在 22:15:05 CST（UTC+8）。"),
            "created Pi notification should contain assistant text: {}",
            sends[0].text
        );
        assert!(
            sends[0].text.contains("\n\n ---\n\n耗时：14s"),
            "created Pi notification should use synthesized completion cost: {}",
            sends[0].text
        );
    }

    #[tokio::test]
    async fn pi_wechat_reply_does_not_duplicate_after_history_watch_echo() {
        let temp = tempfile::TempDir::new().expect("pi duplicate temp dir");
        let root = temp.path().join("pi-sessions");
        let project = temp.path().join("project");
        fs::create_dir_all(&project).expect("project dir");
        let provider = Arc::new(FakeProvider::new("pi", ""));
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "pi",
                project_path: Some(project.clone()),
                title: "pi workspace".into(),
            })
            .await
            .expect("open pi workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let session_id = opened.workspace.session_id.0.to_string();
        let session_path = write_pi_history_session(&root, &session_id, &project, "现在呢");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );
        start_pi_history_watch(&core, &root).await;
        wait_for_history_watch_baseline().await;

        provider.emit_assistant(&session_id, "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver initial notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(notification),
            ))
            .await
            .expect("submit pi reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;
        assert_eq!(transport.replies().len(), 1);
        assert_eq!(transport.sends().len(), 1);
        assert!(
            !core.direct_notification_suppressed(&workspace_id),
            "pending reply delivery should release direct suppression"
        );

        append_pi_assistant_response(
            &session_path,
            "2026-05-18T14:15:35.000Z",
            "reply: please continue",
        );
        if poll_core_event_for_wechat(&service, &mut events, Duration::from_millis(500)).await {
            panic!("lagging Pi history echo reached WeChat and would duplicate the reply");
        }
        assert_eq!(transport.replies().len(), 1);
        assert_eq!(transport.sends().len(), 1);
    }

    #[tokio::test]
    async fn notification_reply_waits_for_turn_completed_before_delivering_latest_agent_message() {
        let provider = Arc::new(FakeProvider::default());
        provider.set_submit_prefix_messages(vec!["working first"]);
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-turn-complete".into()),
            title: "workspace-turn-complete".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(notification),
            ))
            .await
            .expect("submit reply");

        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("buffer intermediate assistant message");
        assert!(
            transport.replies().is_empty(),
            "intermediate assistant message must not consume the pending reply"
        );

        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("buffer latest assistant message");
        assert!(
            transport.replies().is_empty(),
            "assistant message must wait for the matching turn completion"
        );

        service
            .handle_core_event(next_turn_completed_event(&mut events).await)
            .await
            .expect("deliver completed reply");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
        assert!(!replies[0].text.contains("working first"));
        assert!(
            replies[0]
                .text
                .contains(" ---\n\n耗时：0s\n会话：thread-1\n目录：/tmp/workspace-turn-complete"),
            "completed reply should keep cost/session/cwd in one shared block: {}",
            replies[0].text
        );
        assert_eq!(
            transport.sends().len(),
            1,
            "pending reply output must not also be sent as a background notification"
        );
    }

    #[tokio::test]
    async fn completed_pending_reply_transport_failure_is_retained_and_retried() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-retry".into()),
                title: "workspace-retry".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(notification),
            ))
            .await
            .expect("submit reply");

        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("record assistant reply");
        transport.fail_replies("transport down");
        service
            .handle_core_event(next_turn_completed_event(&mut events).await)
            .await
            .expect("transport failure should not escape core event handler");

        assert!(transport.replies().is_empty());
        assert_eq!(service.pending_reply_count(), 1);
        assert!(
            core.direct_notification_suppressed(&workspace_id),
            "suppression should remain while the durable reply is retrying"
        );

        transport.clear_reply_failures();
        service
            .retry_completed_pending_replies()
            .await
            .expect("retry pending reply");

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
        assert_eq!(service.pending_reply_count(), 0);
        assert!(!core.direct_notification_suppressed(&workspace_id));
    }

    #[tokio::test]
    async fn notification_reply_reports_turn_failure_to_user() {
        let provider = Arc::new(FakeProvider::default());
        provider.set_submit_failure("agent went silent");
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-turn-failed".into()),
                title: "workspace-turn-failed".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(notification),
            ))
            .await
            .expect("submit reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert_eq!(replies[0].text, "agent 执行失败：agent went silent");
        assert!(
            !core.direct_notification_suppressed(&workspace_id),
            "turn failure should end direct notification suppression"
        );
    }

    #[tokio::test]
    async fn notification_reply_routes_by_quoted_text_when_wechat_omits_quote_id() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-quoted-text".into()),
            title: "workspace-quoted-text".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        service
            .handle_incoming(WechatIncoming {
                message_id: "user-reply-1".into(),
                user_id: "user-1".into(),
                text: "please continue".into(),
                quoted_message_id: None,
                quoted_text: Some(sends[0].text.clone()),
                sdk_message: None,
            })
            .await
            .expect("submit quoted-text reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].user_id, "user-1");
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
    }

    #[tokio::test]
    async fn notification_reply_routes_by_quoted_text_from_any_split_chunk() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-split-quote".into()),
            title: "workspace-split-quote".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        transport.split_next_send_as(&[
            ("wechat-split-1", "first visible notification chunk"),
            ("wechat-split-2", "second visible notification chunk"),
            ("wechat-split-3", "third visible notification chunk"),
        ]);
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");

        for id in ["wechat-split-1", "wechat-split-2", "wechat-split-3"] {
            assert!(
                core.message_session_binding("wechat", "user-1", id)
                    .is_some(),
                "split WeChat notification id {id} should be bound"
            );
        }

        service
            .handle_incoming(WechatIncoming {
                message_id: "user-reply-1".into(),
                user_id: "user-1".into(),
                text: "please continue from split quote".into(),
                quoted_message_id: None,
                quoted_text: Some("second visible notification chunk".into()),
                sdk_message: None,
            })
            .await
            .expect("submit split quoted-text reply");
        let early_replies = transport.replies();
        assert!(
            early_replies.is_empty(),
            "split quoted text should route to the provider, not return immediately: {early_replies:?}"
        );
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0]
            .text
            .contains("reply: please continue from split quote"));
    }

    #[tokio::test]
    async fn notification_reply_routes_by_raw_quoted_text_with_inline_code_markers() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-quoted-code".into()),
            title: "workspace-quoted-code".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "now is `2026-05-07 22:54:05 CST`.");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        service
            .handle_incoming(WechatIncoming {
                message_id: "user-reply-1".into(),
                user_id: "user-1".into(),
                text: "please continue".into(),
                quoted_message_id: None,
                quoted_text: Some(sends[0].text.clone()),
                sdk_message: None,
            })
            .await
            .expect("submit quoted-text reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
    }

    #[test]
    fn quote_binding_matches_raw_wechat_quote_with_markdown_filter_disabled() {
        let sent_body = r#"有 FSEvents，但 periodic scan 还不能删。

原因是现在是“双层保障”：

- FSEvents：负责 macOS 递归目录事件，能发现新目录/新文件，解决之前 kqueue 递归 fd 爆炸的问题。
- periodic scan：兜底发现 FSEvents/notify 漏掉或启动竞态期间没覆盖到的 session 文件，也处理非 macOS、测试、根目录刚创建、provider 特殊布局这类情况。

所以你看到的 `discovering sessions in explicit roots` 不是因为没有 FSEvents，而是 `SessionWatcher::recv()` 每 `scan_interval=10s` 超时后仍然跑一次 `reconcile_startup_changes()`。这在逻辑上是“watch 事件之外的兜底扫描”。

现在真正不合理的是日志级别：每 10 秒、每 provider 打 `DEBUG` 太吵。应该把 `Session::discover_in()` 这条通用日志降到 `trace`，保留 scan summary 或出错日志在 debug/warn。这样不影响 FSEvents，也不丢兜底。

cost: 13s
session: `019e027d-a5c5-78d0-baab-e6c70dc5d693`
cwd: `/Volumes/Data/opensource/conductor/lucarnex`"#;
        let quoted_text = sent_body;

        let sent_id = sent_quote_binding_id(sent_body);
        assert_eq!(visible_quote_binding_id(quoted_text), sent_id);
    }

    #[tokio::test]
    async fn notification_reply_keeps_wechat_typing_active_until_agent_output() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-typing".into()),
            title: "workspace-typing".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let sends = transport.sends();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(sends[0].message_id.clone()),
            ))
            .await
            .expect("submit reply");

        wait_until(|| transport.typing_count("user-1") >= 2).await;
        let typing_before_output = transport.typing_count("user-1");

        deliver_until_reply_count(&service, &mut events, &transport, 1).await;
        tokio::time::sleep(Duration::from_millis(35)).await;
        let typing_after_output = transport.typing_count("user-1");
        tokio::time::sleep(Duration::from_millis(35)).await;

        assert_eq!(
            transport.typing_count("user-1"),
            typing_after_output,
            "typing keepalive should stop after the agent output is delivered"
        );
        assert!(
            typing_after_output <= typing_before_output + 1,
            "at most one in-flight typing refresh should finish after cancellation"
        );
    }

    #[tokio::test]
    async fn notification_reply_still_submits_when_initial_typing_fails() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-typing-failure".into()),
            title: "workspace-typing-failure".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let sends = transport.sends();
        transport.fail_typing("getconfig failed");

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some(sends[0].message_id.clone()),
            ))
            .await
            .expect("typing failure must not drop the incoming reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
    }

    #[tokio::test]
    async fn notification_reply_resume_uses_global_bypass_config_default() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let provider = Arc::new(FakeProvider::default());
        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        );
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-reloaded".into()),
                title: "workspace-reloaded".into(),
            })
            .await
            .expect("open workspace");
        let provider_session_id = core
            .active_provider_session_id(&opened.workspace.workspace_id)
            .expect("provider session");
        core.bind_message_to_provider_session("wechat", "user-1", "notify-1", provider_session_id)
            .expect("bind notification");
        drop(core);

        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        );
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some("notify-1"),
            ))
            .await
            .expect("submit reply after core reload");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: please continue"));
        assert_eq!(
            provider.resume_args(),
            vec![r#"{"cwd":"/tmp/workspace-reloaded"}"#.to_string()],
            "notification replies must resume remote agent sessions with the workspace cwd and no bypass permissions unless global config enables it"
        );
    }

    #[tokio::test]
    async fn notification_reply_resume_uses_global_bypass_config_when_enabled() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let provider = Arc::new(FakeProvider::default());
        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        );
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-config-bypass".into()),
                title: "workspace-config-bypass".into(),
            })
            .await
            .expect("open workspace");
        let provider_session_id = core
            .active_provider_session_id(&opened.workspace.workspace_id)
            .expect("provider session");
        core.bind_message_to_provider_session("wechat", "user-1", "notify-1", provider_session_id)
            .expect("bind notification");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        service
            .handle_incoming(WechatIncoming::new(
                "config-1",
                "user-1",
                "/config global bypass on",
                Option::<String>::None,
            ))
            .await
            .expect("enable bypass");
        drop(core);

        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        );
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "please continue",
                Some("notify-1"),
            ))
            .await
            .expect("submit reply after core reload");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;

        assert_eq!(
            provider.resume_args(),
            vec![
                r#"{"cwd":"/tmp/workspace-config-bypass","permission_mode":"bypass"}"#.to_string()
            ],
            "global bypass config must apply to WeChat notification resumes"
        );
    }

    #[tokio::test]
    async fn notification_reply_keeps_typing_active_while_resuming_workspace() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let db = temp.path().join("state.sqlite3");
        let provider = Arc::new(FakeProvider::default());
        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("store"),
        );
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-typing-reload".into()),
                title: "workspace-typing-reload".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let provider_session_id = core
            .active_provider_session_id(&workspace_id)
            .expect("provider session");
        core.bind_message_to_provider_session("wechat", "user-1", "notify-1", provider_session_id)
            .expect("bind notification");
        drop(core);

        provider.set_resume_delay(Duration::from_millis(250));
        let core = test_core_with_store(
            Arc::clone(&provider),
            ControlPlaneSqliteStore::open(&db).expect("reopen store"),
        );
        core.mark_live_instances_stale_after_restart("test restart")
            .expect("mark stale");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = Arc::new(WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        ));
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move {
                service
                    .handle_incoming(WechatIncoming::new(
                        "user-reply-1",
                        "user-1",
                        "please continue",
                        Some("notify-1"),
                    ))
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            transport.typing_count("user-1") >= 2,
            "typing keepalive should refresh while workspace resume is still pending"
        );

        run.await
            .expect("join incoming handler")
            .expect("submit reply");
        deliver_until_reply_count(&service, &mut events, &transport, 1).await;
    }

    #[tokio::test]
    async fn run_loop_sends_startup_help_to_known_users() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = Arc::new(WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.run_until_shutdown(shutdown_rx).await }
        });

        wait_until(|| {
            transport.sends().iter().any(|send| {
                send.user_id == "user-1"
                    && send.text.contains("👋 lucarned 已启动。")
                    && send.text.contains("| `/help` | 显示帮助 |")
            })
        })
        .await;

        shutdown_tx.send(true).expect("stop service");
        run.await
            .expect("wechat service task should join")
            .expect("wechat service should stop cleanly");
    }

    #[tokio::test]
    async fn run_loop_retries_startup_help_after_send_failure() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        transport.fail_sends("network down");
        let service = Arc::new(WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                pending_reply_retry_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.run_until_shutdown(shutdown_rx).await }
        });

        wait_until(|| transport.send_attempt_count() >= 1).await;
        assert!(transport.sends().is_empty());

        transport.clear_send_failures();
        wait_until(|| {
            transport.sends().iter().any(|send| {
                send.user_id == "user-1"
                    && send.text.contains("👋 lucarned 已启动。")
                    && send.text.contains("| `/help` | 显示帮助 |")
            })
        })
        .await;

        shutdown_tx.send(true).expect("stop service");
        run.await
            .expect("wechat service task should join")
            .expect("wechat service should stop cleanly");
    }

    #[tokio::test]
    async fn run_loop_abandons_startup_help_after_three_failed_retries() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        transport.fail_sends("network down");
        let service = Arc::new(WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                pending_reply_retry_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.run_until_shutdown(shutdown_rx).await }
        });

        wait_until(|| transport.send_attempt_count() >= 4).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(transport.send_attempt_count(), 4);
        assert!(transport.sends().is_empty());

        shutdown_tx.send(true).expect("stop service");
        run.await
            .expect("wechat service task should join")
            .expect("wechat service should stop cleanly");
    }

    #[tokio::test]
    async fn run_loop_continues_after_incoming_handler_error() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::with_incoming(vec![WechatIncoming::new(
            "user-message-1",
            "user-1",
            "hello",
            Option::<String>::None,
        )]));
        transport.fail_replies("reply send failed");
        let service = WechatNotificationService::new(
            core,
            transport,
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

        service
            .run_until_shutdown(shutdown_rx)
            .await
            .expect("incoming handler errors should not stop the adapter loop");
    }

    #[tokio::test]
    async fn queued_notification_replies_each_receive_agent_output() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-queued-replies".into()),
            title: "workspace-queued-replies".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");
        let notification = transport.sends()[0].message_id.clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-1",
                "user-1",
                "first",
                Some(notification.clone()),
            ))
            .await
            .expect("submit first reply");
        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-2",
                "user-1",
                "second",
                Some(notification),
            ))
            .await
            .expect("submit second reply");

        deliver_until_reply_count(&service, &mut events, &transport, 1).await;
        deliver_until_reply_count(&service, &mut events, &transport, 2).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-1");
        assert!(replies[0].text.contains("reply: first"));
        assert_eq!(replies[1].reply_to_message_id, "user-reply-2");
        assert!(replies[1].text.contains("reply: second"));
    }

    #[tokio::test]
    async fn concurrent_session_notifications_route_replies_by_message_binding() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-one".into()),
            title: "workspace-one".into(),
        })
        .await
        .expect("open workspace one");
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-two".into()),
            title: "workspace-two".into(),
        })
        .await
        .expect("open workspace two");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                typing_keepalive_interval: Duration::from_millis(10),
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "first session complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver first notification");
        provider.emit_assistant("thread-2", "second session complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver second notification");

        let sends = transport.sends();
        assert_eq!(sends.len(), 2);
        let first_notification = sends
            .iter()
            .find(|send| send.text.contains("first session complete"))
            .expect("first notification")
            .message_id
            .clone();
        let second_notification = sends
            .iter()
            .find(|send| send.text.contains("second session complete"))
            .expect("second notification")
            .message_id
            .clone();

        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-second",
                "user-1",
                "reply to second",
                Some(second_notification),
            ))
            .await
            .expect("submit second reply first");
        service
            .handle_incoming(WechatIncoming::new(
                "user-reply-first",
                "user-1",
                "reply to first",
                Some(first_notification),
            ))
            .await
            .expect("submit first reply second");

        deliver_until_reply_count(&service, &mut events, &transport, 1).await;
        deliver_until_reply_count(&service, &mut events, &transport, 2).await;

        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        assert_eq!(replies[0].reply_to_message_id, "user-reply-second");
        assert!(replies[0].text.contains("reply: reply to second"));
        assert!(replies[0].text.contains("会话：thread-2"));
        assert_eq!(replies[1].reply_to_message_id, "user-reply-first");
        assert!(replies[1].text.contains("reply: reply to first"));
        assert!(replies[1].text.contains("会话：thread-1"));
    }

    #[tokio::test]
    async fn notification_delivery_respects_global_and_workspace_policy() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let project_path = std::path::PathBuf::from("/tmp/workspace-policy");
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some(project_path.clone()),
            title: "workspace-policy".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            transport,
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        core.set_global_notifications_enabled(false)
            .expect("disable global notifications");
        provider.emit_assistant("thread-1", "muted globally");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("handle muted event");
        assert!(service.transport.sends().is_empty());

        core.set_workspace_notifications_enabled(&project_path, true)
            .expect("enable workspace notifications");
        provider.emit_assistant("thread-1", "enabled for workspace");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("handle workspace event");
        assert_eq!(service.transport.sends().len(), 1);
    }

    #[tokio::test]
    async fn direct_conversation_suppression_prevents_duplicate_notifications() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-active".into()),
                title: "workspace-active".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            transport,
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        core.begin_direct_notification_suppression(&workspace_id);
        provider.emit_assistant("thread-1", "normal topic output");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("handle suppressed event");
        core.end_direct_notification_suppression(&workspace_id);

        assert!(service.transport.sends().is_empty());
    }

    #[tokio::test]
    async fn watched_notification_transport_failure_does_not_escape_core_event_handler() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-send-failure".into()),
            title: "workspace-send-failure".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        transport.fail_sends("send transport down");
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "background complete");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("notification transport failure should not escape core event handler");

        assert!(transport.sends().is_empty());
        assert_eq!(service.pending_notification_count(), 1);

        transport.clear_send_failures();
        service
            .retry_pending_notifications()
            .await
            .expect("retry pending notification");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].text.contains("background complete"));
        assert_eq!(service.pending_notification_count(), 0);
    }

    #[tokio::test]
    async fn attachment_notification_uses_transport_attachment_send() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-notify".into()),
                title: "workspace-attachment-notify".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: None,
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-wechat".into(),
                    filename: "codex-image-ig-wechat.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: Some("caption".into()),
                }),
            })
            .await
            .expect("attachment notification");

        let attachments = transport.attachments();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].user_id, "user-1");
        assert_eq!(attachments[0].message_id, "wechat-out-1");
        assert_eq!(attachments[0].id.as_str(), "ig-wechat");
        assert_eq!(
            attachments[0].filename.as_str(),
            "codex-image-ig-wechat.png"
        );
        assert_eq!(attachments[0].media_type.as_str(), "image/png");
        assert_eq!(attachments[0].caption.as_deref(), Some("caption"));
        assert_eq!(attachments[0].reply_to_message_id, None);
    }

    #[tokio::test]
    async fn failed_attachment_notification_sends_chinese_failure_message_after_bounded_retries() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-notify-failure".into()),
                title: "workspace-attachment-notify-failure".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        transport.fail_attachment_sends("media upload failed: 413 payload too large");
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: None,
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-failed".into(),
                    filename: "too-large.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: Some("caption".into()),
                }),
            })
            .await
            .expect("attachment failure should not escape core event handler");

        assert!(transport.attachments().is_empty());
        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].text.contains("附件发送失败。"));
        assert!(sends[0].text.contains("文件：too-large.png"));
        assert!(sends[0]
            .text
            .contains("错误：media upload failed: 413 payload too large"));
        assert_eq!(
            transport.attachment_send_attempt_count(),
            lucarne_channel::robust::ATTACHMENT_DELIVERY_MAX_RETRIES + 1
        );
        assert_eq!(service.pending_notification_count(), 0);
    }

    #[tokio::test]
    async fn rate_limited_attachment_notification_sends_chinese_failure_message_without_infinite_retry(
    ) {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-notify-rate-limit".into()),
                title: "workspace-attachment-notify-rate-limit".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        transport.fail_attachment_sends_rate_limited(Duration::from_secs(30));
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id,
                turn_id: None,
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-rate-limited".into(),
                    filename: "rate-limited.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: Some("caption".into()),
                }),
            })
            .await
            .expect("attachment rate limit should not escape core event handler");

        assert!(transport.attachments().is_empty());
        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert!(sends[0].text.contains("附件发送失败。"));
        assert!(sends[0].text.contains("文件：rate-limited.png"));
        assert!(sends[0].text.contains("错误：ret=-2"));
        assert_eq!(service.pending_notification_count(), 0);
    }

    #[tokio::test]
    async fn failed_pending_reply_attachment_reports_failure_without_blocking_text_reply() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-reply-failure".into()),
                title: "workspace-attachment-reply-failure".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        transport.fail_attachment_replies("media upload failed: invalid base64");
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        let turn_id = TurnId::new("turn-attachment-reply-failure");
        let source = WechatIncoming::new(
            "incoming-attachment-failure",
            "user-1",
            "画图",
            Option::<String>::None,
        );
        service.remember_pending_reply(turn_id.clone(), workspace_id.clone(), source);

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: workspace_id.clone(),
                turn_id: Some(turn_id.clone()),
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-reply-failed".into(),
                    filename: "bad-image.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: None,
                }),
            })
            .await
            .expect("attachment should be queued");
        assert_eq!(transport.attachment_reply_attempt_count(), 0);
        assert_eq!(
            service
                .pending_reply(&turn_id)
                .expect("pending reply")
                .pending_attachments
                .len(),
            1
        );
        service.record_pending_assistant_message(&turn_id, "text still arrives");
        let reply_target = service
            .mark_pending_reply_completed(&turn_id)
            .expect("pending reply");
        service
            .deliver_pending_reply(&workspace_id, &turn_id, reply_target)
            .await
            .expect("text reply should still be delivered");

        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        assert!(replies[0].text.contains("text still arrives"));
        assert!(replies[1].text.contains("附件发送失败。"));
        assert!(replies[1].text.contains("文件：bad-image.png"));
        assert!(replies[1]
            .text
            .contains("错误：media upload failed: invalid base64"));
        assert_eq!(
            transport.attachment_reply_attempt_count(),
            lucarne_channel::robust::ATTACHMENT_DELIVERY_MAX_RETRIES + 1
        );
    }

    #[tokio::test]
    async fn rate_limited_pending_reply_attachment_reports_failure_without_blocking_text_reply_or_infinite_retry(
    ) {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-reply-rate-limit".into()),
                title: "workspace-attachment-reply-rate-limit".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        transport.fail_attachment_replies_rate_limited(Duration::from_secs(30));
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        let turn_id = TurnId::new("turn-attachment-reply-rate-limit");
        let source = WechatIncoming::new(
            "incoming-attachment-rate-limit",
            "user-1",
            "画图",
            Option::<String>::None,
        );
        service.remember_pending_reply(turn_id.clone(), workspace_id.clone(), source);

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: workspace_id.clone(),
                turn_id: Some(turn_id.clone()),
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-reply-rate-limited".into(),
                    filename: "rate-limited.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: None,
                }),
            })
            .await
            .expect("attachment should be queued");
        assert_eq!(transport.attachment_reply_attempt_count(), 0);
        assert_eq!(
            service
                .pending_reply(&turn_id)
                .expect("pending reply")
                .pending_attachments
                .len(),
            1,
            "attachment should only wait for the terminal reply, not retry immediately"
        );

        service.record_pending_assistant_message(&turn_id, "text still arrives");
        let reply_target = service
            .mark_pending_reply_completed(&turn_id)
            .expect("pending reply");
        service
            .deliver_pending_reply(&workspace_id, &turn_id, reply_target)
            .await
            .expect("text reply should still be delivered");

        assert!(transport.attachments().is_empty());
        let replies = transport.replies();
        assert_eq!(replies.len(), 2);
        assert!(replies[0].text.contains("text still arrives"));
        assert!(replies[1].text.contains("附件发送失败。"));
        assert!(replies[1].text.contains("文件：rate-limited.png"));
        assert!(replies[1].text.contains("错误：ret=-2"));
    }

    #[test]
    fn wechat_ilink_rate_limit_retry_count_stays_three() {
        assert_eq!(crate::adapter::WECHAT_RATE_LIMIT_MAX_RETRIES, 3);
        assert_eq!(
            crate::adapter::WECHAT_RATE_LIMIT_MAX_RETRIES,
            lucarne_channel::robust::ATTACHMENT_DELIVERY_MAX_RETRIES
        );
    }

    #[tokio::test]
    async fn pending_reply_attachment_replies_to_source_message() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-attachment-reply".into()),
                title: "workspace-attachment-reply".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        let turn_id = TurnId::new("turn-attachment-reply");
        let source = WechatIncoming::new(
            "incoming-attachment",
            "user-1",
            "画图",
            Option::<String>::None,
        );
        service.remember_pending_reply(turn_id.clone(), workspace_id.clone(), source);

        service
            .handle_core_event(CoreEvent::TimelineEvent {
                workspace_id: workspace_id.clone(),
                turn_id: Some(turn_id.clone()),
                event: AgentEvent::Attachment(AgentAttachment {
                    id: "ig-reply".into(),
                    filename: "codex-image-ig-reply.png".into(),
                    media_type: "image/png".into(),
                    data_base64: "AQID".into(),
                    caption: None,
                }),
            })
            .await
            .expect("attachment queued");
        assert!(transport.attachments().is_empty());
        let reply_target = service
            .mark_pending_reply_completed(&turn_id)
            .expect("pending reply");
        service
            .deliver_pending_reply(&workspace_id, &turn_id, reply_target)
            .await
            .expect("attachment reply");

        let attachments = transport.attachments();
        assert_eq!(attachments.len(), 1);
        assert_eq!(
            attachments[0].reply_to_message_id.as_deref(),
            Some("incoming-attachment")
        );
        assert_eq!(attachments[0].message_id, "wechat-out-1");
        assert_eq!(attachments[0].id.as_str(), "ig-reply");
        assert_eq!(attachments[0].filename.as_str(), "codex-image-ig-reply.png");
    }

    #[tokio::test]
    async fn pending_notification_queue_is_bounded() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service =
            WechatNotificationService::new(core, transport, WechatServiceOptions::default());

        for index in 0..(MAX_PENDING_NOTIFICATIONS + 1) {
            service.remember_pending_notification(
                WorkspaceId::new("workspace-pending-notifications"),
                "user-1".into(),
                format!("body-{index}").into(),
                ProviderSessionId::new("codex:session-1"),
            );
        }

        assert_eq!(
            service.pending_notification_count(),
            MAX_PENDING_NOTIFICATIONS
        );
        let notifications = service.take_pending_notifications();
        assert_eq!(MAX_PENDING_NOTIFICATIONS, 10);
        assert_eq!(notifications.len(), MAX_PENDING_NOTIFICATIONS);
        match &notifications[0].content {
            WechatPendingNotificationContent::Text(body) => {
                assert_eq!(body.as_str(), "body-1");
            }
            WechatPendingNotificationContent::Attachment(_) => panic!("expected text notification"),
        }
    }

    #[tokio::test]
    async fn rate_limited_notification_waits_before_retrying() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-rate-limit".into()),
                title: "workspace-rate-limit".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let provider_session_id = core
            .active_provider_session_id(&workspace_id)
            .expect("active provider session");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        transport.fail_sends_rate_limited(Duration::from_millis(50));
        let delivered = service
            .try_send_notification(
                &workspace_id,
                "user-1",
                "delayed body",
                provider_session_id.clone(),
            )
            .await
            .unwrap();
        assert!(!delivered);
        assert!(transport.sends().is_empty());

        service.remember_pending_notification(
            workspace_id.clone(),
            "user-1".into(),
            "delayed body".into(),
            provider_session_id,
        );
        transport.clear_send_failures();

        service.retry_pending_notifications().await.unwrap();
        assert!(transport.sends().is_empty());
        assert_eq!(service.pending_notification_count(), 1);

        tokio::time::sleep(Duration::from_millis(60)).await;
        service.retry_pending_notifications().await.unwrap();
        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].text, "delayed body");
        assert_eq!(service.pending_notification_count(), 0);
    }

    #[tokio::test]
    async fn incoming_message_clears_local_rate_limit_backoff() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let opened = core
            .open_workspace_with_events(OpenWorkspaceRequest {
                provider_id: "codex",
                project_path: Some("/tmp/workspace-rate-limit-reset".into()),
                title: "workspace-rate-limit-reset".into(),
            })
            .await
            .expect("open workspace");
        let workspace_id = opened.workspace.workspace_id.clone();
        let provider_session_id = core
            .active_provider_session_id(&workspace_id)
            .expect("active provider session");
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        transport.fail_sends_rate_limited(Duration::from_secs(60));
        let delivered = service
            .try_send_notification(
                &workspace_id,
                "user-1",
                "delayed body",
                provider_session_id.clone(),
            )
            .await
            .unwrap();
        assert!(!delivered);
        service.remember_pending_notification(
            workspace_id,
            "user-1".into(),
            "delayed body".into(),
            provider_session_id,
        );
        transport.clear_send_failures();

        service
            .handle_incoming(WechatIncoming::new(
                "message-reset",
                "user-1",
                "   ",
                None::<String>,
            ))
            .await
            .unwrap();
        service.retry_pending_notifications().await.unwrap();

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].text, "delayed body");
    }

    #[tokio::test]
    async fn user_interaction_request_sends_configured_context_reminder() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        transport.store_context(wechat_ilink::WechatContext {
            account_key: "account-1".into(),
            user_id: "user-1".into(),
            context_token: "ctx-1".into(),
            observed_at_unix_ms: 1_000,
            source_message_id: Some("msg-1".into()),
        });
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        )
        .with_context_expiry_reminder(crate::adapter::WechatContextExpiryReminderConfig {
            expires_after: Duration::from_secs(24 * 60 * 60),
            remind_before: Duration::from_secs(30 * 60),
            prompt_template: "ctx for {user_id}".to_string(),
        });

        service
            .handle_user_interaction_request(WechatUserInteractionRequest {
                account_key: "account-1".into(),
                user_id: Some("user-1".into()),
                reason: UserInteractionReason::ContextExpiring {
                    observed_at: SystemTime::now(),
                    expires_at: SystemTime::now() + Duration::from_secs(60),
                    remind_before: Duration::from_secs(30),
                },
            })
            .await
            .unwrap();

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].user_id, "user-1");
        assert_eq!(sends[0].text, "ctx for user-1");
        assert_eq!(sends[0].context_token, "ctx-1");
    }

    #[tokio::test]
    async fn context_expiry_interaction_without_policy_is_not_sent() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        transport.store_context(wechat_ilink::WechatContext {
            account_key: "account-1".into(),
            user_id: "user-1".into(),
            context_token: "ctx-1".into(),
            observed_at_unix_ms: 1_000,
            source_message_id: Some("msg-1".into()),
        });
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_user_interaction_request(WechatUserInteractionRequest {
                account_key: "account-1".into(),
                user_id: Some("user-1".into()),
                reason: UserInteractionReason::ContextExpiring {
                    observed_at: SystemTime::now(),
                    expires_at: SystemTime::now() + Duration::from_secs(60),
                    remind_before: Duration::from_secs(30),
                },
            })
            .await
            .unwrap();

        assert!(transport.sends().is_empty());
    }

    #[tokio::test]
    async fn rate_limit_interaction_without_prompt_policy_is_not_sent() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        service.remember_user("user-1");

        service
            .handle_user_interaction_request(WechatUserInteractionRequest {
                account_key: "account-1".into(),
                user_id: None,
                reason: UserInteractionReason::OutboundRateLimitApproaching {
                    sent_count: 6,
                    window: Duration::from_secs(300),
                    threshold: 7,
                },
            })
            .await
            .unwrap();

        assert!(transport.sends().is_empty());
    }

    #[tokio::test]
    async fn rate_limit_interaction_sends_configured_prompt_to_known_users() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions {
                rate_limit_interaction_prompt: Some("请回复任意消息刷新微信窗口。".into()),
                ..WechatServiceOptions::default()
            },
        );
        service.remember_user("user-1");

        service
            .handle_user_interaction_request(WechatUserInteractionRequest {
                account_key: "account-1".into(),
                user_id: None,
                reason: UserInteractionReason::OutboundRateLimitApproaching {
                    sent_count: 6,
                    window: Duration::from_secs(300),
                    threshold: 7,
                },
            })
            .await
            .unwrap();

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].user_id, "user-1");
        assert_eq!(sends[0].text, "请回复任意消息刷新微信窗口。");
    }

    #[tokio::test]
    async fn pending_reply_queue_is_bounded() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service =
            WechatNotificationService::new(core, transport, WechatServiceOptions::default());
        let workspace_id = WorkspaceId::new("workspace-pending-replies");

        for index in 0..(MAX_PENDING_REPLIES + 1) {
            service.remember_pending_reply(
                TurnId::new(format!("turn-{index}")),
                workspace_id.clone(),
                WechatIncoming::new(
                    format!("message-{index}"),
                    "user-1",
                    format!("body-{index}"),
                    None::<String>,
                ),
            );
        }

        let state = service.state.lock().expect("wechat service state lock");
        assert_eq!(MAX_PENDING_REPLIES, 10);
        assert_eq!(state.pending_replies.len(), MAX_PENDING_REPLIES);
        assert_eq!(state.pending_reply_order.len(), MAX_PENDING_REPLIES);
        assert!(!state.pending_replies.contains_key(&TurnId::new("turn-0")));
        assert!(state.pending_replies.contains_key(&TurnId::new("turn-1")));
        assert_eq!(
            state.pending_reply_order.front(),
            Some(&TurnId::new("turn-1"))
        );
    }

    #[tokio::test]
    async fn rate_limited_pending_reply_waits_before_retrying() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(provider);
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            core,
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );
        let workspace_id = WorkspaceId::new("workspace-rate-limited-reply");
        let turn_id = TurnId::new("turn-rate-limited");
        service.remember_pending_reply(
            turn_id.clone(),
            workspace_id,
            WechatIncoming::new("message-1", "user-1", "hello", None::<String>),
        );
        service
            .mark_pending_reply_failed(&turn_id, "reply failed".to_string())
            .unwrap();

        transport.fail_replies_rate_limited(Duration::from_millis(50));
        service.retry_completed_pending_replies().await.unwrap();
        assert!(transport.replies().is_empty());
        assert_eq!(service.pending_reply_count(), 1);

        transport.clear_reply_failures();
        service.retry_completed_pending_replies().await.unwrap();
        assert!(transport.replies().is_empty());
        assert_eq!(service.pending_reply_count(), 1);

        tokio::time::sleep(Duration::from_millis(60)).await;
        service.retry_completed_pending_replies().await.unwrap();
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].text, "reply failed");
        assert_eq!(service.pending_reply_count(), 0);
    }

    #[tokio::test]
    async fn live_history_watch_file_append_sends_wechat_notification() {
        let temp = tempfile::tempdir().expect("live watch temp dir");
        let root = temp.path().join("codex-home").join("sessions");
        let project = temp.path().join("project");
        fs::create_dir_all(&project).expect("create project dir");
        let session_path = write_live_watch_codex_session(
            &root,
            "watch-thread-wechat",
            &project,
            "initial wechat prompt",
        );
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        let transport = Arc::new(FakeTransport::default());
        let service = Arc::new(WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn({
            let service = Arc::clone(&service);
            async move { service.run_until_shutdown(shutdown_rx).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let codex = agent_provider("codex").expect("codex provider descriptor");
        core.start_history_session_watch_with_config(
            WatchConfig::new()
                .providers([codex])
                .provider_roots(codex, [root.clone()])
                .selection(ParseSelection::empty().with_meta().with_messages())
                .debounce(std::time::Duration::from_millis(25)),
        )
        .expect("start live history watch");
        append_codex_assistant_response(
            &session_path,
            "2026-05-06T00:00:02.000Z",
            "wechat live watch complete",
        );

        wait_until(|| {
            transport.sends().iter().any(|send| {
                send.user_id == "user-1"
                    && send.text.contains("wechat live watch complete")
                    && send.text.contains("会话：watch-thread-wechat")
                    && send.text.contains(compact_cwd_footer(&project).as_str())
            })
        })
        .await;

        shutdown_tx.send(true).expect("stop service");
        run.await
            .expect("wechat service task should join")
            .expect("wechat service should stop cleanly");
    }

    #[tokio::test]
    async fn first_incoming_message_registers_user_for_later_notifications() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-first-incoming".into()),
            title: "workspace-first-incoming".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions::default(),
        );

        service
            .handle_incoming(WechatIncoming::new(
                "incoming-1",
                "user-1",
                "hello",
                Option::<String>::None,
            ))
            .await
            .expect("handle first incoming message");

        assert_eq!(service.notification_users(), vec!["user-1".to_string()]);
        let replies = transport.replies();
        assert_eq!(replies.len(), 1);
        assert!(replies[0]
            .text
            .starts_with("💬 请引用一条 agent 通知后再回复，以继续对应会话。\n\n📖 命令"));
        assert!(replies[0].text.contains("| `/help` | 显示帮助 |"));
        assert!(replies[0].text.contains("| `/config` | 查看全局配置 |"));

        provider.emit_assistant("thread-1", "later notification");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification to remembered user");

        let sends = transport.sends();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].user_id, "user-1");
        assert!(sends[0].text.contains("later notification"));
    }

    #[tokio::test]
    async fn notification_uses_stored_wechat_context_for_send() {
        let provider = Arc::new(FakeProvider::default());
        let core = test_core(Arc::clone(&provider));
        core.open_workspace_with_events(OpenWorkspaceRequest {
            provider_id: "codex",
            project_path: Some("/tmp/workspace-stored-ctx".into()),
            title: "workspace-stored-ctx".into(),
        })
        .await
        .expect("open workspace");
        let mut events = core.watch_events();
        let transport = Arc::new(FakeTransport::default());
        let stored_token = "stored-ctx-token-user-1";
        transport.store_context(WechatContext {
            account_key: "account-1".into(),
            user_id: "user-1".into(),
            context_token: stored_token.into(),
            observed_at_unix_ms: 100,
            source_message_id: Some("msg-ctx".into()),
        });
        let service = WechatNotificationService::new(
            Arc::clone(&core),
            Arc::clone(&transport),
            WechatServiceOptions {
                initial_user_ids: vec!["user-1".into()],
                ..WechatServiceOptions::default()
            },
        );

        provider.emit_assistant("thread-1", "stored context test");
        service
            .handle_core_event(next_timeline_event(&mut events).await)
            .await
            .expect("deliver notification");

        let sends = transport.sends();
        assert_eq!(
            sends.len(),
            1,
            "notification should be sent with stored context"
        );
        assert_eq!(
            sends[0].context_token, stored_token,
            "SentRecord must contain the context_token from the stored context, not a synthetic one"
        );
    }

    async fn next_intervention_event(
        events: &mut lucarne::core_service::CoreEventReceiver,
    ) -> CoreEvent {
        loop {
            let event = events.recv().await.expect("core event");
            if matches!(
                event,
                CoreEvent::TimelineEvent {
                    event: AgentEvent::InterventionRequest(_),
                    ..
                }
            ) {
                return event;
            }
        }
    }

    async fn next_timeline_event(
        events: &mut lucarne::core_service::CoreEventReceiver,
    ) -> CoreEvent {
        loop {
            let event = events.recv().await.expect("core event");
            if matches!(
                event,
                CoreEvent::TimelineEvent {
                    event: AgentEvent::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        streaming: false,
                        ..
                    }),
                    ..
                }
            ) {
                return event;
            }
        }
    }

    async fn deliver_until_reply_count(
        service: &WechatNotificationService<FakeTransport>,
        events: &mut lucarne::core_service::CoreEventReceiver,
        transport: &FakeTransport,
        reply_count: usize,
    ) {
        for _ in 0..16 {
            service
                .handle_core_event(next_reply_turn_event(events).await)
                .await
                .expect("handle reply turn event");
            if transport.replies().len() >= reply_count {
                return;
            }
        }
        panic!("reply count {reply_count} was not reached");
    }

    async fn next_reply_turn_event(
        events: &mut lucarne::core_service::CoreEventReceiver,
    ) -> CoreEvent {
        loop {
            let event = events.recv().await.expect("core event");
            if matches!(
                event,
                CoreEvent::TimelineEvent {
                    event: AgentEvent::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        streaming: false,
                        ..
                    }) | AgentEvent::TurnCompleted(_)
                        | AgentEvent::TurnFailed(_),
                    ..
                }
            ) {
                return event;
            }
        }
    }

    async fn next_turn_completed_event(
        events: &mut lucarne::core_service::CoreEventReceiver,
    ) -> CoreEvent {
        loop {
            let event = events.recv().await.expect("core event");
            if matches!(
                event,
                CoreEvent::TimelineEvent {
                    event: AgentEvent::TurnCompleted(_),
                    ..
                }
            ) {
                return event;
            }
        }
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline {
            if ready() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(ready(), "condition was not reached before timeout");
    }

    fn write_live_watch_codex_session(
        root: &Path,
        session_id: &str,
        cwd: &Path,
        prompt: &str,
    ) -> std::path::PathBuf {
        let dir = root.join("2026/05/06");
        fs::create_dir_all(&dir).expect("create codex session dir");
        let path = dir.join(format!("rollout-2026-05-06T00-00-00-{session_id}.jsonl"));
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                format_args!(
                    r#"{{"timestamp":"2026-05-06T00:00:00.000Z","type":"session_meta","payload":{{"session_id":"{session_id}","cwd":"{}","originator":"codex-cli","model":"gpt-5.5"}}}}"#,
                    cwd.display()
                ),
                format_args!(
                    r#"{{"timestamp":"2026-05-06T00:00:01.000Z","type":"response_item","payload":{{"type":"message","role":"user","content":[{{"type":"input_text","text":"{prompt}"}}]}}}}"#
                )
            ),
        )
        .expect("write codex session");
        path
    }

    fn append_codex_assistant_response(path: &Path, timestamp: &str, text: &str) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open session");
        writeln!(
            file,
            r#"{{"timestamp":"{timestamp}","type":"response_item","payload":{{"type":"message","role":"assistant","content":[{{"type":"output_text","text":"{text}"}}]}}}}"#
        )
        .expect("append assistant response");
        file.sync_all().expect("sync assistant response");
    }

    fn write_pi_history_session(
        root: &Path,
        session_id: &str,
        cwd: &Path,
        prompt: &str,
    ) -> PathBuf {
        fs::create_dir_all(root).expect("create pi sessions dir");
        let path = root.join(format!("{session_id}.jsonl"));
        fs::write(
            &path,
            format!(
                "{}\n{}\n",
                format_args!(
                    r#"{{"type":"session","version":3,"id":"{session_id}","timestamp":"2026-05-18T14:15:00.000Z","cwd":"{}"}}"#,
                    cwd.display()
                ),
                format_args!(
                    r#"{{"type":"message","id":"u-{session_id}","timestamp":"2026-05-18T14:15:01.000Z","message":{{"role":"user","content":[{{"type":"text","text":"{prompt}"}}]}}}}"#
                ),
            ),
        )
        .expect("write pi session");
        path
    }

    fn write_completed_pi_history_session(
        root: &Path,
        session_id: &str,
        cwd: &Path,
        prompt: &str,
        answer: &str,
    ) -> PathBuf {
        let path = write_pi_history_session(root, session_id, cwd, prompt);
        append_pi_assistant_response(&path, "2026-05-18T14:15:15.000Z", answer);
        path
    }

    fn append_pi_assistant_response(path: &Path, timestamp: &str, text: &str) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open pi session");
        writeln!(
            file,
            r#"{{"type":"message","id":"a-{timestamp}","parentId":"u1","timestamp":"{timestamp}","message":{{"role":"assistant","model":"gpt-5.5","stopReason":"stop","content":[{{"type":"text","text":"{text}"}}]}}}}"#
        )
        .expect("append pi assistant response");
        file.sync_all().expect("sync pi assistant response");
    }

    async fn start_pi_history_watch(core: &Arc<LucarneCore>, root: &Path) {
        let pi = agent_provider("pi").expect("pi provider descriptor");
        core.start_history_session_watch_with_config(
            WatchConfig::new()
                .providers([pi])
                .provider_roots(pi, [root.to_path_buf()])
                .selection(ParseSelection::empty().with_meta().with_messages())
                .debounce(Duration::from_millis(25)),
        )
        .expect("start pi history watch");
    }

    async fn wait_for_history_watch_baseline() {
        tokio::time::sleep(Duration::from_millis(75)).await;
    }

    async fn poll_core_event_for_wechat(
        service: &WechatNotificationService<FakeTransport>,
        events: &mut lucarne::core_service::CoreEventReceiver,
        timeout_duration: Duration,
    ) -> bool {
        match tokio::time::timeout(timeout_duration, next_timeline_event(events)).await {
            Ok(event) => {
                service
                    .handle_core_event(event)
                    .await
                    .expect("handle core event");
                true
            }
            Err(_) => false,
        }
    }

    fn test_core(provider: Arc<FakeProvider>) -> Arc<LucarneCore> {
        test_core_with_store(
            provider,
            ControlPlaneSqliteStore::open_in_memory().expect("store"),
        )
    }

    fn test_core_with_store(
        provider: Arc<FakeProvider>,
        store: ControlPlaneSqliteStore,
    ) -> Arc<LucarneCore> {
        let runtime = Arc::new(lucarne::agent_runtime::AgentRuntime::new());
        runtime.register(provider);
        LucarneCore::from_runtime_and_store(runtime, store).expect("core")
    }

    struct FakeProvider {
        provider_id: ProviderId,
        completion_turn_id: SmolStr,
        sessions: StdMutex<HashMap<String, mpsc::Sender<AgentEvent>>>,
        next: StdMutex<u64>,
        resume_delay: StdMutex<Option<Duration>>,
        resume_failure: StdMutex<Option<String>>,
        resume_args: StdMutex<Vec<String>>,
        submit_prefix_messages: StdMutex<Vec<String>>,
        submit_failure: StdMutex<Option<String>>,
        status: AgentStatus,
        status_calls: Arc<StdMutex<usize>>,
        resolved_interventions: Arc<StdMutex<Vec<(String, InterventionResponse)>>>,
    }

    impl Default for FakeProvider {
        fn default() -> Self {
            Self::new("codex", "provider-turn")
        }
    }

    impl FakeProvider {
        fn new(provider_id: &'static str, completion_turn_id: &'static str) -> Self {
            Self {
                provider_id: ProviderId::from_static(provider_id),
                completion_turn_id: completion_turn_id.into(),
                sessions: StdMutex::new(HashMap::new()),
                next: StdMutex::new(0),
                resume_delay: StdMutex::new(None),
                resume_failure: StdMutex::new(None),
                resume_args: StdMutex::new(Vec::new()),
                submit_prefix_messages: StdMutex::new(Vec::new()),
                submit_failure: StdMutex::new(None),
                status: AgentStatus::default(),
                status_calls: Arc::new(StdMutex::new(0)),
                resolved_interventions: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn with_status(status: AgentStatus) -> Self {
            Self {
                status,
                ..Self::default()
            }
        }

        fn status_calls(&self) -> usize {
            *self.status_calls.lock().expect("status calls lock")
        }

        fn set_resume_delay(&self, delay: Duration) {
            *self.resume_delay.lock().expect("resume delay lock") = Some(delay);
        }

        fn set_resume_failure(&self, error: &str) {
            *self.resume_failure.lock().expect("resume failure lock") = Some(error.to_string());
        }

        fn set_submit_prefix_messages(&self, messages: Vec<&str>) {
            *self
                .submit_prefix_messages
                .lock()
                .expect("submit prefix messages lock") =
                messages.into_iter().map(str::to_string).collect();
        }

        fn set_submit_failure(&self, error: &str) {
            *self.submit_failure.lock().expect("submit failure lock") = Some(error.to_string());
        }

        fn resume_args(&self) -> Vec<String> {
            self.resume_args.lock().expect("resume args lock").clone()
        }

        fn resolved_interventions(&self) -> Vec<(String, InterventionResponse)> {
            self.resolved_interventions
                .lock()
                .expect("resolved interventions lock")
                .clone()
        }

        fn emit_assistant(&self, session_id: &str, text: &str) {
            let tx = self
                .sessions
                .lock()
                .expect("sessions lock")
                .get(session_id)
                .cloned()
                .expect("session sender");
            tx.try_send(AgentEvent::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: text.into(),
                streaming: false,
            }))
            .expect("send fake event");
        }

        fn emit_intervention(&self, session_id: &str, request: InterventionRequest) {
            let tx = self
                .sessions
                .lock()
                .expect("sessions lock")
                .get(session_id)
                .cloned()
                .expect("session sender");
            tx.try_send(AgentEvent::InterventionRequest(request))
                .expect("send fake intervention");
        }
    }

    #[async_trait]
    impl AgentProvider for FakeProvider {
        fn id(&self) -> ProviderId {
            self.provider_id.clone()
        }

        async fn probe(&self) -> Result<ProbeResult, AgentError> {
            Ok(ProbeResult {
                provider_id: self.provider_id.clone(),
                provider_version: Some("fake".into()),
                capabilities: Default::default(),
            })
        }

        async fn open(&self, _req: OpenSession) -> Result<Box<dyn AgentSession>, AgentError> {
            let mut next = self.next.lock().expect("next lock");
            *next += 1;
            let session_id = format!("thread-{next}");
            drop(next);
            Ok(Box::new(self.session(session_id)))
        }

        async fn resume(&self, req: ResumeSession) -> Result<Box<dyn AgentSession>, AgentError> {
            self.resume_args
                .lock()
                .expect("resume args lock")
                .push(req.args.to_string());
            let delay = *self.resume_delay.lock().expect("resume delay lock");
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            if let Some(error) = self
                .resume_failure
                .lock()
                .expect("resume failure lock")
                .clone()
            {
                return Err(AgentError {
                    kind: AgentErrorKind::Internal,
                    message: error.into(),
                });
            }
            Ok(Box::new(self.session(req.session_ref.0.to_string())))
        }
    }

    impl FakeProvider {
        fn session(&self, session_id: String) -> FakeSession {
            let (tx, rx) = mpsc::channel(16);
            let submit_prefix_messages = self
                .submit_prefix_messages
                .lock()
                .expect("submit prefix messages lock")
                .clone();
            let submit_failure = self
                .submit_failure
                .lock()
                .expect("submit failure lock")
                .clone();
            self.sessions
                .lock()
                .expect("sessions lock")
                .insert(session_id.clone(), tx.clone());
            FakeSession {
                session_id: SessionId(session_id.into()),
                instance_id: InstanceId(format!("instance-{}", uuid_suffix()).into()),
                provider_id: self.provider_id.clone(),
                completion_turn_id: self.completion_turn_id.clone(),
                process_id: Some(std::process::id() as i32),
                tx,
                rx: StdMutex::new(Some(rx)),
                submit_prefix_messages,
                submit_failure,
                status: self.status.clone(),
                status_calls: Arc::clone(&self.status_calls),
                resolved_interventions: Arc::clone(&self.resolved_interventions),
            }
        }
    }

    struct FakeSession {
        session_id: SessionId,
        instance_id: InstanceId,
        provider_id: ProviderId,
        completion_turn_id: SmolStr,
        process_id: Option<i32>,
        tx: mpsc::Sender<AgentEvent>,
        rx: StdMutex<Option<AgentEventStream>>,
        submit_prefix_messages: Vec<String>,
        submit_failure: Option<String>,
        status: AgentStatus,
        status_calls: Arc<StdMutex<usize>>,
        resolved_interventions: Arc<StdMutex<Vec<(String, InterventionResponse)>>>,
    }

    #[async_trait]
    impl AgentSession for FakeSession {
        fn id(&self) -> &SessionId {
            &self.session_id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            self.provider_id.clone()
        }

        fn process_id(&self) -> Option<i32> {
            self.process_id
        }

        async fn submit(&self, input: AgentInput) -> Result<(), AgentError> {
            for message in &self.submit_prefix_messages {
                self.tx
                    .send(AgentEvent::Message(MessageEvent {
                        role: MessageRole::Assistant,
                        text: message.clone().into(),
                        streaming: false,
                    }))
                    .await
                    .map_err(|err| AgentError {
                        kind: AgentErrorKind::Internal,
                        message: err.to_string().into(),
                    })?;
            }
            if let Some(error) = self.submit_failure.as_ref() {
                return self
                    .tx
                    .send(AgentEvent::TurnFailed(TurnFailedEvent {
                        turn_id: self.completion_turn_id.clone(),
                        error: error.clone().into(),
                        code: "test".into(),
                    }))
                    .await
                    .map_err(|err| AgentError {
                        kind: AgentErrorKind::Internal,
                        message: err.to_string().into(),
                    });
            }
            self.tx
                .send(AgentEvent::Message(MessageEvent {
                    role: MessageRole::Assistant,
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
                    turn_id: self.completion_turn_id.clone(),
                    usage: None,
                }))
                .await
                .map_err(|err| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: err.to_string().into(),
                })
        }

        async fn status(&self) -> Result<AgentStatus, AgentError> {
            *self.status_calls.lock().expect("status calls lock") += 1;
            Ok(self.status.clone())
        }

        async fn interrupt(&self) -> Result<(), AgentError> {
            Ok(())
        }

        async fn resolve(
            &self,
            req_id: &str,
            response: lucarne::agent_runtime::InterventionResponse,
        ) -> Result<(), AgentError> {
            self.resolved_interventions
                .lock()
                .expect("resolved interventions lock")
                .push((req_id.to_string(), response));
            Ok(())
        }

        async fn take_events(&self) -> Result<AgentEventStream, AgentError> {
            self.rx
                .lock()
                .expect("event stream lock")
                .take()
                .ok_or_else(|| AgentError {
                    kind: AgentErrorKind::Internal,
                    message: "event stream already taken".into(),
                })
        }

        async fn close(&self) -> Result<(), AgentError> {
            Ok(())
        }
    }

    fn uuid_suffix() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        NEXT.fetch_add(1, Ordering::SeqCst).to_string()
    }

    #[derive(Debug, Clone)]
    struct SentRecord {
        user_id: String,
        message_id: String,
        text: String,
        context_token: String,
    }

    #[derive(Debug, Clone)]
    struct ReplyRecord {
        user_id: String,
        message_id: String,
        reply_to_message_id: String,
        text: String,
    }

    #[derive(Debug, Clone)]
    struct AttachmentRecord {
        user_id: String,
        message_id: String,
        reply_to_message_id: Option<String>,
        id: SmolStr,
        filename: SmolStr,
        media_type: SmolStr,
        caption: Option<SmolStr>,
    }

    #[derive(Default)]
    struct RecordingGlobalConfigPersistence {
        updates: StdMutex<Vec<GlobalConfigUpdate>>,
    }

    impl RecordingGlobalConfigPersistence {
        fn updates(&self) -> Vec<GlobalConfigUpdate> {
            self.updates.lock().expect("global config lock").clone()
        }
    }

    impl GlobalConfigPersistence for RecordingGlobalConfigPersistence {
        fn persist_global_config(
            &self,
            update: GlobalConfigUpdate,
        ) -> lucarne_adapter::AdapterResult<()> {
            self.updates
                .lock()
                .expect("global config lock")
                .push(update);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeTransport {
        next: StdMutex<u64>,
        sends: StdMutex<Vec<SentRecord>>,
        send_attempts: StdMutex<usize>,
        replies: StdMutex<Vec<ReplyRecord>>,
        attachments: StdMutex<Vec<AttachmentRecord>>,
        typings: StdMutex<Vec<String>>,
        contexts: StdMutex<HashMap<String, WechatContext>>,
        incoming: StdMutex<Option<Vec<WechatIncoming>>>,
        interactions: StdMutex<Option<Vec<WechatUserInteractionRequest>>>,
        split_sends: StdMutex<VecDeque<Vec<(String, String)>>>,
        send_error: StdMutex<Option<String>>,
        send_rate_limit: StdMutex<Option<Duration>>,
        attachment_send_error: StdMutex<Option<String>>,
        attachment_reply_error: StdMutex<Option<String>>,
        attachment_send_rate_limit: StdMutex<Option<Duration>>,
        attachment_reply_rate_limit: StdMutex<Option<Duration>>,
        attachment_send_attempts: StdMutex<usize>,
        attachment_reply_attempts: StdMutex<usize>,
        reply_error: StdMutex<Option<String>>,
        reply_rate_limit: StdMutex<Option<Duration>>,
        typing_error: StdMutex<Option<String>>,
        typing_rate_limit: StdMutex<Option<Duration>>,
    }

    impl FakeTransport {
        fn with_incoming(messages: Vec<WechatIncoming>) -> Self {
            Self {
                incoming: StdMutex::new(Some(messages)),
                ..Self::default()
            }
        }

        fn fail_replies(&self, error: &str) {
            *self.reply_error.lock().expect("reply error lock") = Some(error.to_string());
        }

        fn fail_sends(&self, error: &str) {
            *self.send_error.lock().expect("send error lock") = Some(error.to_string());
        }

        fn fail_attachment_sends(&self, error: &str) {
            *self
                .attachment_send_error
                .lock()
                .expect("attachment send error lock") = Some(error.to_string());
        }

        fn fail_attachment_replies(&self, error: &str) {
            *self
                .attachment_reply_error
                .lock()
                .expect("attachment reply error lock") = Some(error.to_string());
        }

        fn fail_attachment_sends_rate_limited(&self, retry_after: Duration) {
            *self
                .attachment_send_rate_limit
                .lock()
                .expect("attachment send rate limit lock") = Some(retry_after);
        }

        fn fail_attachment_replies_rate_limited(&self, retry_after: Duration) {
            *self
                .attachment_reply_rate_limit
                .lock()
                .expect("attachment reply rate limit lock") = Some(retry_after);
        }

        fn fail_sends_rate_limited(&self, retry_after: Duration) {
            *self.send_rate_limit.lock().expect("send rate limit lock") = Some(retry_after);
        }

        fn fail_replies_rate_limited(&self, retry_after: Duration) {
            *self.reply_rate_limit.lock().expect("reply rate limit lock") = Some(retry_after);
        }

        fn split_next_send_as(&self, chunks: &[(&str, &str)]) {
            self.split_sends
                .lock()
                .expect("split sends lock")
                .push_back(
                    chunks
                        .iter()
                        .map(|(id, text)| (id.to_string(), text.to_string()))
                        .collect(),
                );
        }

        fn clear_send_failures(&self) {
            *self.send_error.lock().expect("send error lock") = None;
            *self.send_rate_limit.lock().expect("send rate limit lock") = None;
        }

        fn clear_reply_failures(&self) {
            *self.reply_error.lock().expect("reply error lock") = None;
            *self.reply_rate_limit.lock().expect("reply rate limit lock") = None;
        }

        fn store_context(&self, ctx: WechatContext) {
            self.contexts
                .lock()
                .expect("contexts lock")
                .insert(ctx.user_id.clone(), ctx);
        }

        fn fail_typing(&self, error: &str) {
            *self.typing_error.lock().expect("typing error lock") = Some(error.to_string());
        }

        fn next_message_id(&self) -> String {
            let mut next = self.next.lock().expect("next lock");
            *next += 1;
            format!("wechat-out-{next}")
        }

        fn sends(&self) -> Vec<SentRecord> {
            self.sends.lock().expect("sends lock").clone()
        }

        fn send_attempt_count(&self) -> usize {
            *self.send_attempts.lock().expect("send attempts lock")
        }

        fn replies(&self) -> Vec<ReplyRecord> {
            self.replies.lock().expect("replies lock").clone()
        }

        fn attachments(&self) -> Vec<AttachmentRecord> {
            self.attachments.lock().expect("attachments lock").clone()
        }

        fn attachment_send_attempt_count(&self) -> usize {
            *self
                .attachment_send_attempts
                .lock()
                .expect("attachment send attempts lock")
        }

        fn attachment_reply_attempt_count(&self) -> usize {
            *self
                .attachment_reply_attempts
                .lock()
                .expect("attachment reply attempts lock")
        }

        fn typing_count(&self, user_id: &str) -> usize {
            self.typings
                .lock()
                .expect("typings lock")
                .iter()
                .filter(|seen| seen.as_str() == user_id)
                .count()
        }
    }

    #[async_trait]
    impl WechatTransport for FakeTransport {
        fn subscribe(&self) -> BoxStream<'static, WechatIncoming> {
            let messages = self
                .incoming
                .lock()
                .expect("incoming lock")
                .take()
                .unwrap_or_default();
            if messages.is_empty() {
                Box::pin(stream::pending())
            } else {
                Box::pin(stream::iter(messages))
            }
        }

        fn subscribe_user_interactions(&self) -> BoxStream<'static, WechatUserInteractionRequest> {
            let requests = self
                .interactions
                .lock()
                .expect("interactions lock")
                .take()
                .unwrap_or_default();
            if requests.is_empty() {
                Box::pin(stream::pending())
            } else {
                Box::pin(stream::iter(requests))
            }
        }

        async fn context_for_user(
            &self,
            user_id: &str,
        ) -> Result<Option<WechatContext>, WechatError> {
            let mut contexts = self.contexts.lock().expect("contexts lock");
            if let Some(ctx) = contexts.get(user_id) {
                return Ok(Some(ctx.clone()));
            }
            // Auto-seed a synthetic context for test convenience.
            let ctx = WechatContext {
                account_key: "test".into(),
                user_id: user_id.to_string(),
                context_token: format!("synthetic-{user_id}"),
                observed_at_unix_ms: 0,
                source_message_id: None,
            };
            contexts.insert(user_id.to_string(), ctx.clone());
            Ok(Some(ctx))
        }

        async fn send(
            &self,
            context: &WechatContext,
            text: &str,
        ) -> Result<WechatSendReceipt, WechatError> {
            *self.send_attempts.lock().expect("send attempts lock") += 1;
            if let Some(retry_after) = *self.send_rate_limit.lock().expect("send rate limit lock") {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(error) = self.send_error.lock().expect("send error lock").clone() {
                return Err(WechatError::Transport(error));
            }
            let user_id = &context.user_id;
            let chunks = self
                .split_sends
                .lock()
                .expect("split sends lock")
                .pop_front()
                .unwrap_or_else(|| vec![(self.next_message_id(), text.to_string())]);
            let mut message_ids = Vec::with_capacity(chunks.len());
            let mut visible_texts = Vec::with_capacity(chunks.len());
            for (message_id, chunk_text) in chunks {
                self.sends.lock().expect("sends lock").push(SentRecord {
                    user_id: user_id.to_string(),
                    message_id: message_id.clone(),
                    text: chunk_text.clone(),
                    context_token: context.context_token.clone(),
                });
                message_ids.push(message_id);
                visible_texts.push(chunk_text);
            }
            Ok(WechatSendReceipt {
                message_ids,
                visible_texts,
            })
        }

        async fn reply(
            &self,
            message: &WechatIncoming,
            text: &str,
        ) -> Result<WechatSendReceipt, WechatError> {
            if let Some(retry_after) = *self.reply_rate_limit.lock().expect("reply rate limit lock")
            {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(error) = self.reply_error.lock().expect("reply error lock").clone() {
                return Err(WechatError::Transport(error));
            }
            let message_id = self.next_message_id();
            self.replies
                .lock()
                .expect("replies lock")
                .push(ReplyRecord {
                    user_id: message.user_id.clone(),
                    message_id: message_id.clone(),
                    reply_to_message_id: message.message_id.clone(),
                    text: text.to_string(),
                });
            Ok(WechatSendReceipt {
                message_ids: vec![message_id],
                visible_texts: vec![text.to_string()],
            })
        }

        async fn send_attachment(
            &self,
            context: &WechatContext,
            attachment: &AgentAttachment,
        ) -> Result<WechatSendReceipt, WechatError> {
            *self
                .attachment_send_attempts
                .lock()
                .expect("attachment send attempts lock") += 1;
            if let Some(error) = self
                .attachment_send_error
                .lock()
                .expect("attachment send error lock")
                .clone()
            {
                return Err(WechatError::Transport(error));
            }
            if let Some(retry_after) = *self
                .attachment_send_rate_limit
                .lock()
                .expect("attachment send rate limit lock")
            {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(retry_after) = *self.send_rate_limit.lock().expect("send rate limit lock") {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(error) = self.send_error.lock().expect("send error lock").clone() {
                return Err(WechatError::Transport(error));
            }
            let message_id = self.next_message_id();
            self.attachments
                .lock()
                .expect("attachments lock")
                .push(AttachmentRecord {
                    user_id: context.user_id.clone(),
                    message_id: message_id.clone(),
                    reply_to_message_id: None,
                    id: attachment.id.clone(),
                    filename: attachment.filename.clone(),
                    media_type: attachment.media_type.clone(),
                    caption: attachment.caption.clone(),
                });
            Ok(WechatSendReceipt {
                message_ids: vec![message_id],
                visible_texts: attachment
                    .caption
                    .as_ref()
                    .map(|caption| vec![caption.to_string()])
                    .unwrap_or_default(),
            })
        }

        async fn reply_attachment(
            &self,
            message: &WechatIncoming,
            attachment: &AgentAttachment,
        ) -> Result<WechatSendReceipt, WechatError> {
            *self
                .attachment_reply_attempts
                .lock()
                .expect("attachment reply attempts lock") += 1;
            if let Some(error) = self
                .attachment_reply_error
                .lock()
                .expect("attachment reply error lock")
                .clone()
            {
                return Err(WechatError::Transport(error));
            }
            if let Some(retry_after) = *self
                .attachment_reply_rate_limit
                .lock()
                .expect("attachment reply rate limit lock")
            {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(retry_after) = *self.reply_rate_limit.lock().expect("reply rate limit lock")
            {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(error) = self.reply_error.lock().expect("reply error lock").clone() {
                return Err(WechatError::Transport(error));
            }
            let message_id = self.next_message_id();
            self.attachments
                .lock()
                .expect("attachments lock")
                .push(AttachmentRecord {
                    user_id: message.user_id.clone(),
                    message_id: message_id.clone(),
                    reply_to_message_id: Some(message.message_id.clone()),
                    id: attachment.id.clone(),
                    filename: attachment.filename.clone(),
                    media_type: attachment.media_type.clone(),
                    caption: attachment.caption.clone(),
                });
            Ok(WechatSendReceipt {
                message_ids: vec![message_id],
                visible_texts: attachment
                    .caption
                    .as_ref()
                    .map(|caption| vec![caption.to_string()])
                    .unwrap_or_default(),
            })
        }

        async fn send_typing(&self, context: &WechatContext) -> Result<(), WechatError> {
            if let Some(retry_after) = *self
                .typing_rate_limit
                .lock()
                .expect("typing rate limit lock")
            {
                return Err(WechatError::RateLimited {
                    retry_after,
                    message: "ret=-2".into(),
                });
            }
            if let Some(error) = self.typing_error.lock().expect("typing error lock").clone() {
                return Err(WechatError::Transport(error));
            }
            self.typings
                .lock()
                .expect("typings lock")
                .push(context.user_id.clone());
            Ok(())
        }

        async fn stop(&self) -> Result<(), WechatError> {
            Ok(())
        }
    }
}
