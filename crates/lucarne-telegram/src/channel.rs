//! Telegram channel: [`lucarne_channel::Channel`] implementation backed by
//! teloxide.
//!
//! This module owns the translation between generic channel events and
//! Telegram-specific types (Updates, forum topics, inline keyboards).
//! All markdown is rendered through [`lucarne_channel::markdown`] so the
//! bot authors in CommonMark-ish and we emit valid MarkdownV2.

use async_trait::async_trait;
use futures::stream::{BoxStream, StreamExt};
use lucarne_channel::{
    markdown::render_telegram_markdown_v2,
    splitter::split_for_channel,
    types::{
        Attachment, ChannelError, ChannelEvent, ChatId, CommandQuery, CommandQueryResult,
        FileUpload, IncomingAttachment, IncomingMessage, MessageId, OutgoingButton,
        OutgoingMessage, Result, WorkspaceHandle, WorkspaceId,
    },
    Channel, TextFormat,
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use teloxide::{
    net::Download,
    payloads::{
        AnswerInlineQuerySetters, EditForumTopicSetters, EditMessageTextSetters, GetUpdatesSetters,
        SendChatActionSetters, SendDocumentSetters, SendMessageSetters, SendPhotoSetters,
        SendVideoSetters,
    },
    prelude::Requester,
    types::{
        AllowedUpdate, BotCommand, ChatAction, ChatId as TgChatId, FileId, InlineKeyboardButton,
        InlineKeyboardMarkup, InlineQueryResult, InlineQueryResultArticle, InputFile,
        InputMessageContent, InputMessageContentText, MaybeInaccessibleMessage,
        MessageId as TgMessageId, ParseMode, ReplyParameters, ThreadId, UpdateKind,
    },
    ApiError, Bot, RequestError,
};
use tokio::sync::mpsc;
use tracing::{debug, info, instrument, trace, warn};

const EVENT_QUEUE: usize = 128;
const TELEGRAM_MESSAGE_LIMIT: usize = 4000;
const GET_UPDATES_TIMEOUT_SECS: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TelegramAttachmentKind {
    Photo,
    Video,
    Document,
}

fn telegram_attachment_kind(media_type: &str) -> TelegramAttachmentKind {
    let media_type = media_type.trim().to_ascii_lowercase();
    if media_type.starts_with("image/") {
        TelegramAttachmentKind::Photo
    } else if media_type.starts_with("video/") {
        TelegramAttachmentKind::Video
    } else {
        TelegramAttachmentKind::Document
    }
}

/// Configuration for [`TelegramChannel`].
#[derive(Debug, Clone)]
pub struct TelegramConfig {
    /// Bot token (from `@BotFather`).
    pub token: String,
    /// Numeric chat id of the super-group with forum topics enabled
    /// that hosts the fixed entry + working workspaces.
    pub entry_chat_id: i64,
    /// Optional allow-list of user ids. Empty = everyone who can reach
    /// the bot is allowed.
    pub authorized_user_ids: Vec<i64>,
}

pub struct TelegramChannel {
    bot: Bot,
    events_rx: Mutex<Option<mpsc::Receiver<ChannelEvent>>>,
    _poll_task: tokio::task::JoinHandle<()>,
    cfg: Arc<TelegramConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelegramBotCommand {
    pub command: &'static str,
    pub description: &'static str,
}

impl Drop for TelegramChannel {
    fn drop(&mut self) {
        self._poll_task.abort();
    }
}

impl TelegramChannel {
    pub fn start(cfg: TelegramConfig) -> Arc<Self> {
        let bot = Bot::new(cfg.token.clone());
        Self::start_with_bot(cfg, bot)
    }

    pub fn start_with_client(cfg: TelegramConfig, http_client: reqwest::Client) -> Arc<Self> {
        let bot = Bot::with_client(cfg.token.clone(), http_client);
        Self::start_with_bot(cfg, bot)
    }

    fn start_with_bot(cfg: TelegramConfig, bot: Bot) -> Arc<Self> {
        lucarne::memory_profile_snapshot!("lucarne_telegram.channel.start.start");
        let cfg = Arc::new(cfg);
        info!(
            target: "lucarne_telegram",
            entry_chat_id = cfg.entry_chat_id,
            allow_list = cfg.authorized_user_ids.len(),
            "starting TelegramChannel"
        );
        lucarne::memory_profile_snapshot!("lucarne_telegram.channel.start.after_bot_new");
        let (tx, rx) = mpsc::channel(EVENT_QUEUE);

        let poll_bot = bot.clone();
        let poll_cfg = Arc::clone(&cfg);
        let handle = tokio::spawn(async move {
            if let Err(e) = poll_updates(poll_bot, tx, poll_cfg).await {
                warn!(target: "lucarne_telegram", "update loop ended: {e}");
            }
        });
        lucarne::memory_profile_snapshot!("lucarne_telegram.channel.start.after_poll_spawn");

        Arc::new(Self {
            bot,
            events_rx: Mutex::new(Some(rx)),
            _poll_task: handle,
            cfg,
        })
    }

    /// Test connection by sending a silent message. Returns Ok on success,
    /// or an error if the bot token or chat id is invalid.
    #[instrument(skip(self), fields(chat = self.cfg.entry_chat_id))]
    pub async fn test_connection(&self) -> Result<()> {
        use teloxide::prelude::Requester;
        debug!("sending connection-test message");
        self.bot
            .send_message(TgChatId(self.cfg.entry_chat_id), "✓ lucarne online")
            .parse_mode(ParseMode::MarkdownV2)
            .disable_notification(true)
            .await
            .map_err(|e| {
                warn!(error = %e, "connection test failed");
                ChannelError::Transport(format!("connection test failed: {e}"))
            })?;
        info!("connection test ok");
        Ok(())
    }

    pub fn entry_chat(&self) -> ChatId {
        ChatId::new(self.cfg.entry_chat_id.to_string())
    }

    /// The chat-root workspace handle used for the entry panel.
    pub fn entry_handle(&self) -> WorkspaceHandle {
        WorkspaceHandle::new(self.entry_chat(), WorkspaceId::new(""))
    }

    pub async fn sync_commands(&self, commands: &[TelegramBotCommand]) -> Result<()> {
        let commands: Vec<_> = commands
            .iter()
            .map(|cmd| BotCommand::new(cmd.command, cmd.description))
            .collect();
        let count = commands.len();
        self.bot.set_my_commands(commands).await.map_err(map_err)?;
        info!(
            target: "lucarne_telegram",
            count,
            "telegram bot commands synced"
        );
        Ok(())
    }
}

fn parse_tg_chat_id(id: &ChatId) -> std::result::Result<TgChatId, ChannelError> {
    i64::from_str(id.as_str())
        .map(TgChatId)
        .map_err(|_| ChannelError::Transport(format!("invalid chat id: {}", id.as_str())))
}

fn parse_tg_thread(ws: &WorkspaceId) -> Option<ThreadId> {
    if ws.as_str().is_empty() {
        return None;
    }
    i32::from_str(ws.as_str())
        .ok()
        .map(|n| ThreadId(TgMessageId(n)))
}

fn parse_tg_message_id(id: &MessageId) -> std::result::Result<TgMessageId, ChannelError> {
    i32::from_str(id.as_str())
        .map(TgMessageId)
        .map_err(|_| ChannelError::Transport(format!("invalid message id {}", id.as_str())))
}

fn render_body(msg: &OutgoingMessage) -> (String, Option<ParseMode>) {
    match msg.format {
        TextFormat::Markdown => (
            render_telegram_markdown_v2(&msg.body),
            Some(ParseMode::MarkdownV2),
        ),
        TextFormat::Plain => (msg.body.clone(), None),
    }
}

fn build_keyboard(rows: &[Vec<OutgoingButton>]) -> Option<InlineKeyboardMarkup> {
    if rows.is_empty() {
        return None;
    }
    let kb = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|b| InlineKeyboardButton::callback(b.label.clone(), b.data.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Some(InlineKeyboardMarkup::new(kb))
}

fn map_err(e: RequestError) -> ChannelError {
    if let RequestError::Api(api) = &e {
        // teloxide exposes this as ApiError::Unknown("Bad Request:
        // can't parse entities: ...") for MarkdownV2 issues.
        let text = api.to_string();
        let lower = text.to_ascii_lowercase();
        if lower.contains("message thread not found")
            || lower.contains("topic not found")
            || lower.contains("topic_id_invalid")
        {
            return ChannelError::WorkspaceNotFound(text);
        }
        if lower.contains("parse entities")
            || lower.contains("parse markdown")
            || lower.contains("can't parse")
            || matches!(api, ApiError::MessageIsTooLong)
        {
            return if matches!(api, ApiError::MessageIsTooLong) {
                ChannelError::PayloadTooLarge
            } else {
                ChannelError::FormatRejected(text)
            };
        }
    }
    ChannelError::Transport(e.to_string())
}

#[async_trait]
impl Channel for TelegramChannel {
    fn name(&self) -> &'static str {
        "telegram"
    }

    fn message_char_limit(&self) -> usize {
        TELEGRAM_MESSAGE_LIMIT
    }

    async fn send(&self, target: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId> {
        self.send_all(target, msg)
            .await?
            .into_iter()
            .last()
            .ok_or_else(|| ChannelError::Transport("no chunks sent".into()))
    }

    async fn send_all(
        &self,
        target: &WorkspaceHandle,
        msg: OutgoingMessage,
    ) -> Result<Vec<MessageId>> {
        let chat = parse_tg_chat_id(&target.chat)?;
        let thread = parse_tg_thread(&target.workspace);
        let (body, parse_mode) = render_body(&msg);
        let chunks = split_for_channel(&body, TELEGRAM_MESSAGE_LIMIT);
        let kb = build_keyboard(&msg.buttons);
        debug!(
            target: "lucarne_telegram",
            chat = chat.0,
            thread = ?thread.map(|t| t.0.0),
            chunks = chunks.len(),
            bytes = body.len(),
            mode = ?parse_mode,
            buttons = msg.buttons.len(),
            "sending message"
        );

        let mut message_ids = Vec::with_capacity(chunks.len());
        for (idx, chunk) in chunks.iter().enumerate() {
            let mut req = self.bot.send_message(chat, chunk.clone());
            if let Some(pm) = parse_mode {
                req = req.parse_mode(pm);
            }
            if let Some(t) = thread {
                req = req.message_thread_id(t);
            }
            if msg.notification.is_silent() {
                req = req.disable_notification(true);
            }
            if idx == 0 {
                if let Some(reply_to) = msg.reply_to.as_ref() {
                    req = req.reply_parameters(
                        ReplyParameters::new(parse_tg_message_id(reply_to)?)
                            .allow_sending_without_reply(),
                    );
                }
            }
            // Attach keyboard only on final chunk.
            if idx + 1 == chunks.len() {
                if let Some(kb) = &kb {
                    req = req.reply_markup(kb.clone());
                }
            }
            let m = req.await.map_err(|e| {
                let mapped = map_err(e);
                warn!(
                    target: "lucarne_telegram",
                    chunk_idx = idx, error = %mapped,
                    "send chunk failed"
                );
                mapped
            })?;
            trace!(target: "lucarne_telegram", chunk_idx = idx, tg_message_id = m.id.0, "chunk sent");
            message_ids.push(MessageId::new(m.id.0.to_string()));
        }
        if message_ids.is_empty() {
            return Err(ChannelError::Transport("no chunks sent".into()));
        }
        Ok(message_ids)
    }

    async fn edit(
        &self,
        target: &WorkspaceHandle,
        id: &MessageId,
        msg: OutgoingMessage,
    ) -> Result<()> {
        let chat = parse_tg_chat_id(&target.chat)?;
        let msg_id = parse_tg_message_id(id)?;
        let (body, parse_mode) = render_body(&msg);
        let kb = build_keyboard(&msg.buttons);

        // Editing only makes sense for the first chunk; if the payload
        // would need splitting we truncate with a sentinel so the bot
        // keeps a well-defined contract.
        let truncated = if body.chars().count() > TELEGRAM_MESSAGE_LIMIT {
            let mut s: String = body.chars().take(TELEGRAM_MESSAGE_LIMIT - 16).collect();
            s.push_str(" …(truncated)");
            s
        } else {
            body
        };

        let mut req = self.bot.edit_message_text(chat, msg_id, truncated);
        if let Some(pm) = parse_mode {
            req = req.parse_mode(pm);
        }
        if let Some(kb) = kb {
            req = req.reply_markup(kb);
        }
        match req.await {
            Ok(_) => {}
            Err(RequestError::Api(ApiError::MessageNotModified)) => {}
            Err(e) => return Err(map_err(e)),
        }
        Ok(())
    }

    async fn delete(&self, target: &WorkspaceHandle, id: &MessageId) -> Result<()> {
        let chat = parse_tg_chat_id(&target.chat)?;
        let msg_id = parse_tg_message_id(id)?;
        match self.bot.delete_message(chat, msg_id).await {
            Ok(_) => Ok(()),
            Err(RequestError::Api(ApiError::MessageToDeleteNotFound)) => Ok(()),
            Err(e) => Err(map_err(e)),
        }
    }

    async fn create_workspace(&self, parent: &ChatId, title: &str) -> Result<WorkspaceHandle> {
        let chat = parse_tg_chat_id(parent)?;
        info!(target: "lucarne_telegram", chat = chat.0, title, "creating forum topic");
        let topic = self
            .bot
            .create_forum_topic(chat, title.to_string())
            .await
            .map_err(|e| {
                let m = map_err(e);
                warn!(target: "lucarne_telegram", error = %m, "create_forum_topic failed");
                m
            })?;
        info!(
            target: "lucarne_telegram",
            thread_id = topic.thread_id.0.0,
            "forum topic created"
        );
        Ok(WorkspaceHandle::new(
            parent.clone(),
            WorkspaceId::new(topic.thread_id.0 .0.to_string()),
        ))
    }

    async fn probe_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
        let chat = parse_tg_chat_id(&handle.chat)?;
        let mut req = self.bot.send_chat_action(chat, ChatAction::Typing);
        if let Some(t) = parse_tg_thread(&handle.workspace) {
            req = req.message_thread_id(t);
        }
        req.await.map_err(map_err)?;
        Ok(())
    }

    async fn rename_workspace(&self, handle: &WorkspaceHandle, title: &str) -> Result<()> {
        let chat = parse_tg_chat_id(&handle.chat)?;
        let thread = parse_tg_thread(&handle.workspace)
            .ok_or_else(|| ChannelError::Unsupported("rename requires a topic".into()))?;
        self.bot
            .edit_forum_topic(chat, thread)
            .name(title.to_string())
            .await
            .map_err(map_err)?;
        Ok(())
    }

    async fn delete_workspace(&self, handle: &WorkspaceHandle) -> Result<()> {
        let chat = parse_tg_chat_id(&handle.chat)?;
        let thread = parse_tg_thread(&handle.workspace)
            .ok_or_else(|| ChannelError::Unsupported("delete requires a topic".into()))?;
        self.bot
            .delete_forum_topic(chat, thread)
            .await
            .map_err(map_err)?;
        Ok(())
    }

    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let mut guard = self
            .events_rx
            .lock()
            .expect("Channel::subscribe called more than once");
        let rx = guard
            .take()
            .expect("Channel::subscribe can only be called once per Channel");
        futures::stream::unfold(
            rx,
            |mut rx| async move { rx.recv().await.map(|ev| (ev, rx)) },
        )
        .boxed()
    }

    async fn download_attachment(&self, att: &IncomingAttachment) -> Result<Vec<u8>> {
        let file = self
            .bot
            .get_file(FileId(att.file_ref.clone()))
            .await
            .map_err(map_err)?;
        let mut buf = Vec::with_capacity(file.size as usize);
        self.bot
            .download_file(&file.path, &mut buf)
            .await
            .map_err(|e| ChannelError::Transport(format!("download: {e}")))?;
        Ok(buf)
    }

    async fn acknowledge(&self, target: &WorkspaceHandle) -> Result<()> {
        self.probe_workspace(target).await
    }

    async fn send_file(&self, target: &WorkspaceHandle, file: FileUpload) -> Result<MessageId> {
        let chat = parse_tg_chat_id(&target.chat)?;
        let thread = parse_tg_thread(&target.workspace);
        info!(
            target: "lucarne_telegram",
            chat = chat.0,
            thread = ?thread.map(|t| t.0.0),
            filename = %file.filename,
            bytes = file.bytes.len(),
            "uploading file"
        );
        let input = InputFile::memory(file.bytes).file_name(file.filename);
        let mut req = self.bot.send_document(chat, input);
        if let Some(t) = thread {
            req = req.message_thread_id(t);
        }
        if let Some(cap) = file.caption {
            req = req.caption(cap);
        }
        if file.notification.is_silent() {
            req = req.disable_notification(true);
        }
        if let Some(reply_to) = file.reply_to.as_ref() {
            req = req.reply_parameters(
                ReplyParameters::new(parse_tg_message_id(reply_to)?).allow_sending_without_reply(),
            );
        }
        let m = req.await.map_err(|e| {
            let m = map_err(e);
            warn!(target: "lucarne_telegram", error = %m, "send_document failed");
            m
        })?;
        Ok(MessageId::new(m.id.0.to_string()))
    }

    async fn send_attachment(
        &self,
        target: &WorkspaceHandle,
        attachment: Attachment,
    ) -> Result<MessageId> {
        let chat = parse_tg_chat_id(&target.chat)?;
        let thread = parse_tg_thread(&target.workspace);
        let kind = telegram_attachment_kind(&attachment.media_type);
        info!(
            target: "lucarne_telegram",
            chat = chat.0,
            thread = ?thread.map(|t| t.0.0),
            filename = %attachment.filename,
            media_type = %attachment.media_type,
            bytes = attachment.bytes.len(),
            kind = ?kind,
            "uploading attachment"
        );
        let Attachment {
            filename,
            media_type: _,
            bytes,
            caption,
            reply_to,
            notification,
        } = attachment;
        let input = InputFile::memory(bytes).file_name(filename);
        let reply_parameters = reply_to
            .as_ref()
            .map(parse_tg_message_id)
            .transpose()?
            .map(|id| ReplyParameters::new(id).allow_sending_without_reply());
        let message = match kind {
            TelegramAttachmentKind::Photo => {
                let mut req = self.bot.send_photo(chat, input);
                if let Some(t) = thread {
                    req = req.message_thread_id(t);
                }
                if let Some(cap) = caption {
                    req = req.caption(cap);
                }
                if notification.is_silent() {
                    req = req.disable_notification(true);
                }
                if let Some(reply) = reply_parameters {
                    req = req.reply_parameters(reply);
                }
                req.await.map_err(|e| {
                    let m = map_err(e);
                    warn!(target: "lucarne_telegram", error = %m, "send_photo failed");
                    m
                })?
            }
            TelegramAttachmentKind::Video => {
                let mut req = self.bot.send_video(chat, input);
                if let Some(t) = thread {
                    req = req.message_thread_id(t);
                }
                if let Some(cap) = caption {
                    req = req.caption(cap);
                }
                if notification.is_silent() {
                    req = req.disable_notification(true);
                }
                if let Some(reply) = reply_parameters {
                    req = req.reply_parameters(reply);
                }
                req.await.map_err(|e| {
                    let m = map_err(e);
                    warn!(target: "lucarne_telegram", error = %m, "send_video failed");
                    m
                })?
            }
            TelegramAttachmentKind::Document => {
                let mut req = self.bot.send_document(chat, input);
                if let Some(t) = thread {
                    req = req.message_thread_id(t);
                }
                if let Some(cap) = caption {
                    req = req.caption(cap);
                }
                if notification.is_silent() {
                    req = req.disable_notification(true);
                }
                if let Some(reply) = reply_parameters {
                    req = req.reply_parameters(reply);
                }
                req.await.map_err(|e| {
                    let m = map_err(e);
                    warn!(target: "lucarne_telegram", error = %m, "send_document failed");
                    m
                })?
            }
        };
        Ok(MessageId::new(message.id.0.to_string()))
    }

    async fn answer_command_query(
        &self,
        query: &CommandQuery,
        results: Vec<CommandQueryResult>,
    ) -> Result<()> {
        let tg_results = results
            .into_iter()
            .take(50)
            .map(|result| {
                InlineQueryResult::Article(
                    InlineQueryResultArticle::new(
                        result.id,
                        result.title,
                        InputMessageContent::Text(InputMessageContentText::new(
                            result.message_text,
                        )),
                    )
                    .description(result.description.unwrap_or_default()),
                )
            })
            .collect::<Vec<_>>();

        self.bot
            .answer_inline_query(query.id.clone().into(), tg_results)
            .is_personal(true)
            .cache_time(0)
            .await
            .map_err(|e| {
                let m = map_err(e);
                warn!(target: "lucarne_telegram", error = %m, "answer_inline_query failed");
                m
            })?;
        Ok(())
    }
}

#[instrument(skip(bot, tx, cfg), fields(chat = cfg.entry_chat_id))]
async fn poll_updates(
    bot: Bot,
    tx: mpsc::Sender<ChannelEvent>,
    cfg: Arc<TelegramConfig>,
) -> Result<()> {
    lucarne::memory_profile_snapshot!("lucarne_telegram.channel.poll_updates.start");
    info!(target: "lucarne_telegram", "long-poll loop starting");
    let allowed = [
        AllowedUpdate::Message,
        AllowedUpdate::CallbackQuery,
        AllowedUpdate::ChannelPost,
        AllowedUpdate::InlineQuery,
    ];
    let mut offset: i32 = 0;
    loop {
        let res = bot
            .get_updates()
            .offset(offset)
            .timeout(GET_UPDATES_TIMEOUT_SECS)
            .allowed_updates(allowed.to_vec())
            .await;
        let updates = match res {
            Ok(u) => u,
            Err(e) => {
                warn!(target: "lucarne_telegram", error = %e, "getUpdates error, sleeping 3s");
                tokio::time::sleep(Duration::from_secs(3)).await;
                continue;
            }
        };
        if !updates.is_empty() {
            debug!(target: "lucarne_telegram", count = updates.len(), "received updates");
        }
        for u in updates {
            offset = u.id.0 as i32 + 1;
            if let Some(ev) = translate_update(u, &cfg) {
                if tx.send(ev).await.is_err() {
                    warn!(target: "lucarne_telegram", "event channel closed, exiting poll loop");
                    return Ok(());
                }
            }
        }
    }
}

fn translate_update(u: teloxide::types::Update, cfg: &TelegramConfig) -> Option<ChannelEvent> {
    match u.kind {
        UpdateKind::Message(m) => {
            if !authorized(cfg, m.from.as_ref().map(|u| u.id.0 as i64)) {
                debug!(target: "lucarne_telegram", "ignoring unauthorized message");
                return None;
            }
            let chat = ChatId::new(m.chat.id.0.to_string());
            let workspace = m.thread_id.map(|t| WorkspaceId::new(t.0 .0.to_string()));
            let user = m
                .from
                .as_ref()
                .map(|u| {
                    u.username
                        .clone()
                        .unwrap_or_else(|| format!("id:{}", u.id.0))
                })
                .unwrap_or_else(|| "unknown".into());
            let text = m.text().or_else(|| m.caption()).map(|s| s.to_string());
            let reply_to = m
                .reply_to_message()
                .map(|reply| MessageId::new(reply.id.0.to_string()));
            let mut attachments = Vec::new();
            if let Some(photo) = m.photo().and_then(largest_photo) {
                attachments.push(IncomingAttachment {
                    file_ref: photo.file.id.clone().to_string(),
                    filename: Some("photo.jpg".into()),
                    mime_type: Some("image/jpeg".into()),
                    size: Some(photo.file.size as u64),
                });
            }
            if let Some(doc) = m.document() {
                attachments.push(IncomingAttachment {
                    file_ref: doc.file.id.clone().to_string(),
                    filename: doc.file_name.clone(),
                    mime_type: doc.mime_type.as_ref().map(|m| m.to_string()),
                    size: Some(doc.file.size as u64),
                });
            }
            Some(ChannelEvent::Message(IncomingMessage {
                message_id: MessageId::new(m.id.0.to_string()),
                chat,
                workspace,
                reply_to,
                user,
                text,
                attachments,
            }))
        }
        UpdateKind::InlineQuery(q) => {
            if !authorized(cfg, Some(q.from.id.0 as i64)) {
                debug!(target: "lucarne_telegram", "ignoring unauthorized inline query");
                return None;
            }
            let user = q
                .from
                .username
                .clone()
                .unwrap_or_else(|| format!("id:{}", q.from.id.0));
            Some(ChannelEvent::CommandQuery(CommandQuery {
                id: q.id.0,
                user,
                query: q.query,
                chat_type: q.chat_type.map(|kind| format!("{kind:?}")),
            }))
        }
        UpdateKind::CallbackQuery(q) => {
            if !authorized(cfg, Some(q.from.id.0 as i64)) {
                return None;
            }
            let data = q.data?;
            let msg = q.message?;
            let (chat_id, workspace, message_id) = match &msg {
                MaybeInaccessibleMessage::Regular(m) => (
                    m.chat.id,
                    m.thread_id.map(|t| WorkspaceId::new(t.0 .0.to_string())),
                    m.id,
                ),
                MaybeInaccessibleMessage::Inaccessible(m) => (m.chat.id, None, m.message_id),
            };
            let chat = ChatId::new(chat_id.0.to_string());
            let source_message = MessageId::new(message_id.0.to_string());
            let user = q
                .from
                .username
                .clone()
                .unwrap_or_else(|| format!("id:{}", q.from.id.0));
            Some(ChannelEvent::Button {
                chat,
                workspace,
                user,
                data,
                source_message,
            })
        }
        _ => None,
    }
}

fn largest_photo(photos: &[teloxide::types::PhotoSize]) -> Option<&teloxide::types::PhotoSize> {
    photos
        .iter()
        .max_by_key(|p| u64::from(p.width) * u64::from(p.height))
}

fn authorized(cfg: &TelegramConfig, user_id: Option<i64>) -> bool {
    if cfg.authorized_user_ids.is_empty() {
        return true;
    }
    user_id
        .map(|id| cfg.authorized_user_ids.contains(&id))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_config() -> TelegramConfig {
        TelegramConfig {
            token: "test".into(),
            entry_chat_id: 100,
            authorized_user_ids: Vec::new(),
        }
    }

    #[test]
    fn telegram_attachment_kind_uses_native_media_for_images_and_videos() {
        assert_eq!(
            telegram_attachment_kind("image/png"),
            TelegramAttachmentKind::Photo
        );
        assert_eq!(
            telegram_attachment_kind(" IMAGE/JPEG "),
            TelegramAttachmentKind::Photo
        );
        assert_eq!(
            telegram_attachment_kind("video/mp4"),
            TelegramAttachmentKind::Video
        );
        assert_eq!(
            telegram_attachment_kind("application/pdf"),
            TelegramAttachmentKind::Document
        );
    }

    #[tokio::test]
    async fn dropping_channel_aborts_poll_task() {
        struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(done) = self.0.take() {
                    let _ = done.send(());
                }
            }
        }

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let poll_task = tokio::spawn(async move {
            let _notify = NotifyOnDrop(Some(done_tx));
            std::future::pending::<()>().await;
        });
        let (_tx, rx) = tokio::sync::mpsc::channel(EVENT_QUEUE);
        let channel = TelegramChannel {
            bot: Bot::new("test"),
            events_rx: Mutex::new(Some(rx)),
            _poll_task: poll_task,
            cfg: Arc::new(test_config()),
        };
        tokio::task::yield_now().await;

        drop(channel);

        tokio::time::timeout(Duration::from_secs(1), done_rx)
            .await
            .expect("poll task should be aborted when channel is dropped")
            .expect("poll task drop notifier should fire");
    }

    #[test]
    fn telegram_channel_exposes_shared_reqwest_client_constructor() {
        let source = include_str!("channel.rs")
            .split("\n#[cfg(test)]")
            .next()
            .expect("production source");
        assert!(source.contains("pub fn start_with_client"));
        assert!(source.contains("http_client: reqwest::Client"));
        assert!(
            source.contains("Bot::with_client(cfg.token.clone(), http_client)"),
            "Telegram bot must use the shared reqwest client"
        );
    }

    #[test]
    fn memory_profile_snapshots_mark_telegram_channel_startup() {
        let source = include_str!("channel.rs");

        for label in [
            "lucarne_telegram.channel.start.start",
            "lucarne_telegram.channel.start.after_bot_new",
            "lucarne_telegram.channel.start.after_poll_spawn",
            "lucarne_telegram.channel.poll_updates.start",
        ] {
            let needle = format!("lucarne::memory_profile_snapshot!(\"{label}\")");
            assert!(source.contains(&needle), "missing snapshot {label}");
        }
    }

    #[test]
    fn photo_message_becomes_image_attachment_with_caption_text() {
        let update: teloxide::types::Update = serde_json::from_str(
            r#"{
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "from": {
                        "id": 123,
                        "is_bot": false,
                        "first_name": "era",
                        "username": "era"
                    },
                    "chat": {
                        "id": 100,
                        "type": "supergroup",
                        "title": "lucarne"
                    },
                    "message_thread_id": 2,
                    "date": 1,
                    "caption": "这是个啥",
                    "photo": [
                        {
                            "file_id": "small-photo",
                            "file_unique_id": "small-unique",
                            "file_size": 100,
                            "width": 90,
                            "height": 90
                        },
                        {
                            "file_id": "large-photo",
                            "file_unique_id": "large-unique",
                            "file_size": 12345,
                            "width": 800,
                            "height": 600
                        }
                    ]
                }
            }"#,
        )
        .unwrap();

        let ev = translate_update(update, &test_config()).expect("channel event");
        let ChannelEvent::Message(msg) = ev else {
            panic!("expected message");
        };

        assert_eq!(msg.text.as_deref(), Some("这是个啥"));
        assert_eq!(msg.workspace.as_ref().map(|w| w.as_str()), Some("2"));
        assert_eq!(msg.attachments.len(), 1);
        assert_eq!(msg.attachments[0].file_ref, "large-photo");
        assert_eq!(msg.attachments[0].filename.as_deref(), Some("photo.jpg"));
        assert_eq!(msg.attachments[0].mime_type.as_deref(), Some("image/jpeg"));
        assert_eq!(msg.attachments[0].size, Some(12345));
    }

    fn real_bot_api_channel_from_env() -> Option<Arc<TelegramChannel>> {
        let _ = dotenvy::dotenv();
        if std::env::var("LUCARNE_TELEGRAM_REAL_E2E").ok().as_deref() != Some("1") {
            return None;
        }
        let token = std::env::var("TELEGRAM_BOT_TOKEN").ok()?;
        let entry_chat_id = std::env::var("TELEGRAM_CHAT_ID")
            .or_else(|_| std::env::var("LUCARNE_ENTRY_CHAT_ID"))
            .ok()?
            .parse()
            .ok()?;
        let cfg = Arc::new(TelegramConfig {
            token: token.clone(),
            entry_chat_id,
            authorized_user_ids: Vec::new(),
        });
        let (_tx, rx) = tokio::sync::mpsc::channel(EVENT_QUEUE);
        Some(Arc::new(TelegramChannel {
            bot: Bot::new(token),
            events_rx: Mutex::new(Some(rx)),
            _poll_task: tokio::spawn(async { std::future::pending::<()>().await }),
            cfg,
        }))
    }

    #[tokio::test]
    async fn real_bot_api_forum_topic_keyboard_reply_round_trip() {
        let Some(channel) = real_bot_api_channel_from_env() else {
            return;
        };
        channel
            .test_connection()
            .await
            .expect("telegram connection");
        let parent = channel.entry_chat();
        let topic_title = format!(
            "lucarne-real-e2e-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        let topic = channel
            .create_workspace(&parent, &topic_title)
            .await
            .expect("create forum topic");

        let first = channel
            .send(
                &topic,
                OutgoingMessage::plain("lucarne real Telegram Bot API e2e")
                    .with_buttons(vec![vec![OutgoingButton {
                        label: "status".into(),
                        data: "agentcmd:c:t1".into(),
                    }]])
                    .silent(),
            )
            .await
            .expect("send keyboard message");
        let reply = channel
            .send(
                &topic,
                OutgoingMessage::plain("reply check")
                    .reply_to(first.clone())
                    .silent(),
            )
            .await
            .expect("send reply");
        channel
            .edit(
                &topic,
                &first,
                OutgoingMessage::plain("lucarne real Telegram Bot API e2e edited").with_buttons(
                    vec![vec![OutgoingButton {
                        label: "refresh".into(),
                        data: "refresh:1".into(),
                    }]],
                ),
            )
            .await
            .expect("edit message");
        channel.probe_workspace(&topic).await.expect("probe topic");
        channel.delete(&topic, &reply).await.expect("delete reply");
        channel
            .delete_workspace(&topic)
            .await
            .expect("delete forum topic");
    }
}
