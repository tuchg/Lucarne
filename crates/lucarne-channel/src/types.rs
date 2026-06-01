//! Platform-independent identifier and event types used by the
//! [`super::Channel`] trait.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// A raw chat/group/DM identifier on some channel (opaque string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChatId(pub String);

impl ChatId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A sub-conversation inside a chat (Telegram forum topic, Slack
/// thread, Matrix room under a space, …).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceId(pub String);

impl WorkspaceId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A workspace handle pairs a chat and a workspace id. Channels use
/// this to route sends.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceHandle {
    pub chat: ChatId,
    pub workspace: WorkspaceId,
}

impl WorkspaceHandle {
    pub fn new(chat: ChatId, workspace: WorkspaceId) -> Self {
        Self { chat, workspace }
    }
}

/// A message identifier returned by the platform after a send.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(pub String);

impl MessageId {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An inline button associated with an outgoing message. `data` is an
/// opaque payload that will be delivered back in a
/// [`ChannelEvent::Button`] when the user clicks it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutgoingButton {
    pub label: String,
    pub data: String,
}

/// Per-message delivery notification intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NotificationPolicy {
    /// Let the platform alert the user normally.
    #[default]
    Notify,
    /// Deliver without a push/alert where the platform supports it.
    Silent,
}

impl NotificationPolicy {
    pub fn is_silent(self) -> bool {
        matches!(self, Self::Silent)
    }
}

/// A message the bot wants to send. `format` tells the channel how to
/// render `body` (plain vs markdown). `buttons` is optional inline
/// keyboard laid out row by row.
#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub body: String,
    pub format: super::TextFormat,
    pub buttons: Vec<Vec<OutgoingButton>>,
    /// If set, the platform should send this message as a reply to
    /// the given source message.
    pub reply_to: Option<MessageId>,
    /// Whether this delivery may alert the user.
    pub notification: NotificationPolicy,
}

impl OutgoingMessage {
    pub fn plain(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            format: super::TextFormat::Plain,
            buttons: Vec::new(),
            reply_to: None,
            notification: NotificationPolicy::Notify,
        }
    }
    pub fn markdown(body: impl Into<String>) -> Self {
        Self {
            body: body.into(),
            format: super::TextFormat::Markdown,
            buttons: Vec::new(),
            reply_to: None,
            notification: NotificationPolicy::Notify,
        }
    }
    pub fn with_buttons(mut self, rows: Vec<Vec<OutgoingButton>>) -> Self {
        self.buttons = rows;
        self
    }
    pub fn reply_to(mut self, id: MessageId) -> Self {
        self.reply_to = Some(id);
        self
    }
    pub fn with_notification(mut self, notification: NotificationPolicy) -> Self {
        self.notification = notification;
        self
    }
    pub fn silent(mut self) -> Self {
        self.notification = NotificationPolicy::Silent;
        self
    }
}

/// A file the bot wants to upload (usually as a fallback when a
/// message body is too large or its markup was rejected by the
/// platform).
#[derive(Debug, Clone)]
pub struct FileUpload {
    pub filename: String,
    pub bytes: Vec<u8>,
    pub caption: Option<String>,
    pub reply_to: Option<MessageId>,
    pub notification: NotificationPolicy,
}

impl FileUpload {
    pub fn new(filename: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            filename: filename.into(),
            bytes,
            caption: None,
            reply_to: None,
            notification: NotificationPolicy::Notify,
        }
    }
    pub fn with_caption(mut self, caption: impl Into<String>) -> Self {
        self.caption = Some(caption.into());
        self
    }
    pub fn reply_to(mut self, id: MessageId) -> Self {
        self.reply_to = Some(id);
        self
    }
    pub fn with_notification(mut self, notification: NotificationPolicy) -> Self {
        self.notification = notification;
        self
    }
    pub fn silent(mut self) -> Self {
        self.notification = NotificationPolicy::Silent;
        self
    }
}

/// An agent-originated attachment ready for channel delivery.
#[derive(Debug, Clone)]
pub struct Attachment {
    pub filename: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
    pub caption: Option<String>,
    pub reply_to: Option<MessageId>,
    pub notification: NotificationPolicy,
}

impl Attachment {
    pub fn new(filename: impl Into<String>, media_type: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            filename: filename.into(),
            media_type: media_type.into(),
            bytes,
            caption: None,
            reply_to: None,
            notification: NotificationPolicy::Notify,
        }
    }
    pub fn with_caption(mut self, caption: impl Into<String>) -> Self {
        self.caption = Some(caption.into());
        self
    }
    pub fn reply_to(mut self, id: MessageId) -> Self {
        self.reply_to = Some(id);
        self
    }
    pub fn with_notification(mut self, notification: NotificationPolicy) -> Self {
        self.notification = notification;
        self
    }
    pub fn silent(mut self) -> Self {
        self.notification = NotificationPolicy::Silent;
        self
    }
}

/// A user-originated attachment (file, image, voice note, …). Payload
/// is downloaded lazily via [`super::Channel::download_attachment`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncomingAttachment {
    /// Opaque platform-specific file reference.
    pub file_ref: String,
    /// Original filename if provided by the user.
    pub filename: Option<String>,
    /// Reported MIME type.
    pub mime_type: Option<String>,
    /// Reported size in bytes, if known.
    pub size: Option<u64>,
}

/// A message the bot receives from a user.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub message_id: MessageId,
    pub chat: ChatId,
    /// Set when the message was posted inside a sub-workspace
    /// (forum topic / thread). `None` means it landed in the chat root.
    pub workspace: Option<WorkspaceId>,
    /// Source platform message this message replies to, when available.
    pub reply_to: Option<MessageId>,
    pub user: String,
    pub text: Option<String>,
    pub attachments: Vec<IncomingAttachment>,
}

/// A platform autocomplete query for slash commands.
///
/// Telegram delivers this as an inline query. It intentionally does not carry
/// a concrete workspace because Telegram inline query updates do not include
/// the final destination topic; command execution still happens when the
/// selected result is sent as a normal message.
#[derive(Debug, Clone)]
pub struct CommandQuery {
    pub id: String,
    pub user: String,
    pub query: String,
    pub chat_type: Option<String>,
}

/// One autocomplete result returned to a [`CommandQuery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandQueryResult {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    /// Plain text inserted into the target conversation when selected.
    pub message_text: String,
}

/// Events delivered by [`super::Channel::subscribe`].
#[derive(Debug, Clone)]
pub enum ChannelEvent {
    Message(IncomingMessage),
    CommandQuery(CommandQuery),
    /// An inline button click. `data` is the opaque payload sent on the
    /// originating [`OutgoingButton`].
    Button {
        chat: ChatId,
        workspace: Option<WorkspaceId>,
        user: String,
        data: String,
        /// The message the button was attached to, useful for editing
        /// the same bubble in response.
        source_message: MessageId,
    },
    /// Soft error/log that the channel layer observed; surfaced so the
    /// bot can log it uniformly.
    Warning(String),
}

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("channel transport: {0}")]
    Transport(String),
    /// Platform rejected the rendered markup (e.g. Telegram
    /// "can't parse entities"). Callers typically respond by
    /// falling back to an uploaded text file.
    #[error("format rejected: {0}")]
    FormatRejected(String),
    /// The payload was above the platform's size ceiling even after
    /// splitting; caller should fall back to a file upload.
    #[error("payload too large for inline send")]
    PayloadTooLarge,
    /// The requested sub-conversation no longer exists on the backing
    /// platform, e.g. a Telegram forum topic was deleted.
    #[error("workspace not found: {0}")]
    WorkspaceNotFound(String),
    #[error("channel capability not supported: {0}")]
    Unsupported(String),
    #[error("attachment too large: {size} > {limit} bytes")]
    AttachmentTooLarge { size: u64, limit: u64 },
    #[error("attachment not textual: {reason}")]
    NotTextual { reason: String },
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, ChannelError>;
