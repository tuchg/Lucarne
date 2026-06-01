//! Channel abstraction for lucarne bots.
//!
//! A *channel* is a messaging platform (Telegram today, other IM
//! platforms tomorrow) through which users talk to their local agents.
//! The [`Channel`] trait captures the minimum surface the bot needs so
//! the high-level flow is platform-agnostic.
//!
//! Sub-modules:
//! * [`markdown`] — convert generic markdown to a target channel format.
//! * [`splitter`] — split long messages at safe boundaries.
//! * [`ingest`] — read incoming text files for agent consumption.
//! * [`types`] — platform-independent IDs and event types.

pub mod agent_message;
pub mod ingest;
pub mod markdown;
pub mod robust;
pub mod splitter;
pub mod types;

use async_trait::async_trait;
use futures::stream::BoxStream;

pub use types::{
    Attachment, ChannelError, ChannelEvent, ChatId, CommandQuery, CommandQueryResult, FileUpload,
    IncomingAttachment, IncomingMessage, MessageId, NotificationPolicy, OutgoingButton,
    OutgoingMessage, Result, WorkspaceHandle, WorkspaceId,
};

/// Describes how rendered text was produced so a channel impl knows
/// whether it still needs to escape markup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextFormat {
    /// Plain text, no formatting requested.
    Plain,
    /// Generic markdown; the channel is responsible for translating to
    /// its own dialect (e.g. Telegram MarkdownV2).
    Markdown,
}

/// The core abstraction. One `Channel` instance represents the bot's
/// connection to a single platform/account.
#[async_trait]
pub trait Channel: Send + Sync {
    /// Human-readable identifier used in logs, e.g. "telegram".
    fn name(&self) -> &'static str;

    /// Maximum characters a single outbound message may contain before
    /// the caller should split using [`splitter::split_for_channel`].
    fn message_char_limit(&self) -> usize;

    /// Send a message. For long content callers should pre-split.
    async fn send(&self, target: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId>;

    /// Send a message and return every platform message id produced by the send.
    ///
    /// Channels that split long content should override this so callers that
    /// need per-message routing can bind every visible chunk. The default keeps
    /// the existing single-message contract for simple channels.
    async fn send_all(
        &self,
        target: &WorkspaceHandle,
        msg: OutgoingMessage,
    ) -> Result<Vec<MessageId>> {
        self.send(target, msg).await.map(|id| vec![id])
    }

    /// Edit an existing message (used for "typing…" → final answer).
    async fn edit(
        &self,
        target: &WorkspaceHandle,
        id: &MessageId,
        msg: OutgoingMessage,
    ) -> Result<()>;

    /// Delete a previously sent message. Best-effort: callers treat
    /// failures as non-fatal (e.g. message already gone). Default impl
    /// returns [`ChannelError::Unsupported`] so channels without a
    /// native delete primitive fail gracefully.
    async fn delete(&self, _target: &WorkspaceHandle, _id: &MessageId) -> Result<()> {
        Err(ChannelError::Unsupported("delete".into()))
    }

    /// Create a new workspace (Telegram forum topic / Slack thread / …)
    /// under `parent_chat` with the given human title.
    async fn create_workspace(&self, parent: &ChatId, title: &str) -> Result<WorkspaceHandle>;

    /// Probe whether a workspace still exists without sending a
    /// user-visible message. Channel implementations should return
    /// [`ChannelError::WorkspaceNotFound`] when the platform says the
    /// workspace/thread/topic is gone.
    async fn probe_workspace(&self, _handle: &WorkspaceHandle) -> Result<()> {
        Err(ChannelError::Unsupported("probe_workspace".into()))
    }

    /// Rename a workspace in-place.
    async fn rename_workspace(&self, handle: &WorkspaceHandle, title: &str) -> Result<()>;

    /// Delete a workspace and its channel-side contents. The default
    /// unsupported implementation lets channels without a destructive
    /// workspace primitive opt out explicitly.
    async fn delete_workspace(&self, _handle: &WorkspaceHandle) -> Result<()> {
        Err(ChannelError::Unsupported("delete_workspace".into()))
    }

    /// Subscribe to inbound user events (messages, button clicks, file
    /// uploads). The stream is expected to live as long as the
    /// [`Channel`] object does.
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent>;

    /// Download an attachment's bytes (the channel layer hides API
    /// specifics like file_id resolution).
    async fn download_attachment(&self, att: &IncomingAttachment) -> Result<Vec<u8>>;

    /// Acknowledge that an inbound message is being handled. Channels
    /// may implement this as a typing/chat action; the default is a
    /// no-op because not every platform exposes read/typing state.
    async fn acknowledge(&self, _target: &WorkspaceHandle) -> Result<()> {
        Ok(())
    }

    /// Upload a file (fallback path for oversized / format-rejected
    /// payloads). Default impl returns [`ChannelError::Unsupported`];
    /// channels that want fallback support should override.
    async fn send_file(&self, _target: &WorkspaceHandle, _file: FileUpload) -> Result<MessageId> {
        Err(ChannelError::Unsupported("send_file".into()))
    }

    /// Send an agent-originated attachment. Channels may override this to use
    /// native media APIs; default delivery falls back to [`Channel::send_file`].
    async fn send_attachment(
        &self,
        target: &WorkspaceHandle,
        attachment: Attachment,
    ) -> Result<MessageId> {
        let mut upload = FileUpload::new(attachment.filename, attachment.bytes);
        upload = upload.with_notification(attachment.notification);
        if let Some(caption) = attachment.caption {
            upload = upload.with_caption(caption);
        }
        if let Some(reply_to) = attachment.reply_to {
            upload = upload.reply_to(reply_to);
        }
        self.send_file(target, upload).await
    }

    /// Answer a platform command autocomplete query. Telegram implements this
    /// with inline query results; channels without such a primitive can ignore
    /// it by using the default unsupported response.
    async fn answer_command_query(
        &self,
        _query: &CommandQuery,
        _results: Vec<CommandQueryResult>,
    ) -> Result<()> {
        Err(ChannelError::Unsupported("answer_command_query".into()))
    }
}

#[cfg(test)]
mod attachment_tests {
    use super::*;
    use async_trait::async_trait;
    use futures::{
        stream::{self, BoxStream},
        StreamExt,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingChannel {
        files: Mutex<Vec<FileUpload>>,
    }

    #[async_trait]
    impl Channel for RecordingChannel {
        fn name(&self) -> &'static str {
            "recording"
        }

        fn message_char_limit(&self) -> usize {
            4096
        }

        async fn send(
            &self,
            _target: &WorkspaceHandle,
            _msg: OutgoingMessage,
        ) -> Result<MessageId> {
            Ok(MessageId::new("text-1"))
        }

        async fn edit(
            &self,
            _target: &WorkspaceHandle,
            _id: &MessageId,
            _msg: OutgoingMessage,
        ) -> Result<()> {
            Ok(())
        }

        async fn create_workspace(
            &self,
            _parent: &ChatId,
            _title: &str,
        ) -> Result<WorkspaceHandle> {
            Err(ChannelError::Unsupported("create_workspace".into()))
        }

        async fn rename_workspace(&self, _handle: &WorkspaceHandle, _title: &str) -> Result<()> {
            Ok(())
        }

        fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
            stream::empty().boxed()
        }

        async fn download_attachment(&self, _att: &IncomingAttachment) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }

        async fn send_file(
            &self,
            _target: &WorkspaceHandle,
            file: FileUpload,
        ) -> Result<MessageId> {
            self.files.lock().expect("files").push(file);
            Ok(MessageId::new("file-1"))
        }
    }

    #[tokio::test]
    async fn send_attachment_default_falls_back_to_file_upload() {
        let channel = RecordingChannel::default();
        let target = WorkspaceHandle::new(ChatId::new("1"), WorkspaceId::new("2"));
        let attachment = Attachment {
            filename: "logo.png".into(),
            media_type: "image/png".into(),
            bytes: vec![1, 2, 3],
            caption: Some("Logo".into()),
            reply_to: Some(MessageId::new("source")),
            notification: NotificationPolicy::Silent,
        };

        let id = channel.send_attachment(&target, attachment).await.unwrap();

        assert_eq!(id.as_str(), "file-1");
        let files = channel.files.lock().expect("files");
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "logo.png");
        assert_eq!(files[0].bytes, vec![1, 2, 3]);
        assert_eq!(files[0].caption.as_deref(), Some("Logo"));
        assert_eq!(
            files[0].reply_to.as_ref().map(|id| id.as_str()),
            Some("source")
        );
        assert_eq!(files[0].notification, NotificationPolicy::Silent);
    }
}
