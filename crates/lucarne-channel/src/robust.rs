//! Robust send helpers.
//!
//! When the underlying [`crate::Channel`] rejects a message because
//! its rendered markup cannot be parsed (MarkdownV2 is notoriously
//! strict) or because the payload would be too large even after
//! splitting, we transparently fall back to uploading the message
//! body as a plain-text attachment. This keeps the bot useful even
//! when an agent emits exotic output.
//!
//! The helper is intentionally channel-agnostic: it only relies on
//! the [`crate::Channel`] trait surface.

use crate::{
    types::{ChannelError, FileUpload, MessageId, OutgoingMessage, Result, WorkspaceHandle},
    Channel, TextFormat,
};
use tracing::{debug, info, instrument, warn};

/// Cutoff above which we bypass the inline send path entirely and go
/// straight to a file upload. Measured in UTF-8 bytes for simplicity;
/// well below any realistic chat platform limit but above a normal
/// agent turn.
pub const INLINE_BYTE_CEILING: usize = 60_000;

/// Number of bounded retries for attachment delivery after the initial attempt.
pub const ATTACHMENT_DELIVERY_MAX_RETRIES: usize = 3;

/// Retry an attachment delivery operation with the shared bounded retry policy.
pub async fn retry_attachment_delivery<F, Fut, T, E, R>(
    mut operation: F,
    mut retryable: R,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    R: FnMut(&E) -> bool,
{
    let mut retries = 0usize;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) if retries < ATTACHMENT_DELIVERY_MAX_RETRIES && retryable(&err) => {
                retries += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Send `msg` through `channel`, falling back to a `.txt` upload if
/// the platform rejects the markup or the payload is too large.
///
/// The file is named from `fallback_stem` (e.g. `"reply"`, an agent
/// identifier) and includes an optional caption so the user sees
/// context.
#[instrument(
    name = "send_with_fallback",
    skip(channel, target, msg),
    fields(
        channel = channel.name(),
        workspace = %target.workspace.as_str(),
        chat = %target.chat.as_str(),
        stem = fallback_stem,
        bytes = msg.body.len(),
        format = ?msg.format,
    )
)]
pub async fn send_with_fallback(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    msg: OutgoingMessage,
    fallback_stem: &str,
) -> Result<MessageId> {
    let ids = send_with_fallback_inner(channel, target, msg, fallback_stem).await?;
    last_message_id(ids)
}

/// Like [`send_with_fallback`], but preserves every platform message id
/// produced when the channel splits long content.
#[instrument(
    name = "send_with_fallback_all",
    skip(channel, target, msg),
    fields(
        channel = channel.name(),
        workspace = %target.workspace.as_str(),
        chat = %target.chat.as_str(),
        stem = fallback_stem,
        bytes = msg.body.len(),
        format = ?msg.format,
    )
)]
pub async fn send_with_fallback_all(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    msg: OutgoingMessage,
    fallback_stem: &str,
) -> Result<Vec<MessageId>> {
    send_with_fallback_inner(channel, target, msg, fallback_stem).await
}

async fn send_with_fallback_inner(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    msg: OutgoingMessage,
    fallback_stem: &str,
) -> Result<Vec<MessageId>> {
    if msg.body.len() > INLINE_BYTE_CEILING {
        info!(
            bytes = msg.body.len(),
            ceiling = INLINE_BYTE_CEILING,
            "payload exceeds inline ceiling, uploading as file",
        );
        return file_fallback(channel, target, &msg, fallback_stem, "payload too large")
            .await
            .map(|id| vec![id]);
    }
    debug!("attempting inline send");
    match channel.send_all(target, msg.clone()).await {
        Ok(ids) => {
            debug!(
                messages = ids.len(),
                last_message_id = ids.last().map(|id| id.as_str()).unwrap_or(""),
                "inline send ok"
            );
            ensure_message_ids(ids)
        }
        Err(ChannelError::FormatRejected(reason)) => {
            warn!(reason = %reason, "format rejected, falling back to file");
            file_fallback(channel, target, &msg, fallback_stem, &reason)
                .await
                .map(|id| vec![id])
        }
        Err(ChannelError::PayloadTooLarge) => {
            warn!("payload too large after split, falling back to file");
            file_fallback(channel, target, &msg, fallback_stem, "payload too large")
                .await
                .map(|id| vec![id])
        }
        Err(other) => {
            if matches!(msg.format, TextFormat::Markdown)
                && looks_like_parse_error(&other.to_string())
            {
                warn!(
                    error = %other,
                    "transport error looks like a parse error, retrying as plain",
                );
                let plain = OutgoingMessage {
                    body: msg.body.clone(),
                    format: TextFormat::Plain,
                    buttons: msg.buttons.clone(),
                    reply_to: msg.reply_to.clone(),
                    notification: msg.notification,
                };
                match channel.send_all(target, plain).await {
                    Ok(ids) => {
                        debug!(
                            messages = ids.len(),
                            last_message_id = ids.last().map(|id| id.as_str()).unwrap_or(""),
                            "plain retry ok"
                        );
                        return ensure_message_ids(ids);
                    }
                    Err(retry_err) => {
                        warn!(error = %retry_err, "plain retry failed, falling back to file");
                        return file_fallback(
                            channel,
                            target,
                            &msg,
                            fallback_stem,
                            &other.to_string(),
                        )
                        .await
                        .map(|id| vec![id]);
                    }
                }
            }
            warn!(error = %other, "inline send failed");
            Err(other)
        }
    }
}

fn ensure_message_ids(ids: Vec<MessageId>) -> Result<Vec<MessageId>> {
    if ids.is_empty() {
        return Err(ChannelError::Transport(
            "send returned no message ids".into(),
        ));
    }
    Ok(ids)
}

fn last_message_id(ids: Vec<MessageId>) -> Result<MessageId> {
    ids.into_iter()
        .last()
        .ok_or_else(|| ChannelError::Transport("send returned no message ids".into()))
}

#[instrument(
    skip(channel, target, msg),
    fields(
        bytes = msg.body.len(),
        workspace = %target.workspace.as_str(),
    )
)]
async fn file_fallback(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    msg: &OutgoingMessage,
    stem: &str,
    reason: &str,
) -> Result<MessageId> {
    let filename = fallback_filename(stem);
    info!(filename = %filename, reason = %reason, "uploading fallback file");
    let caption = format!("↳ inline send failed ({}); see attached.", reason);
    let mut file = FileUpload::new(filename, msg.body.as_bytes().to_vec())
        .with_caption(caption)
        .with_notification(msg.notification);
    if let Some(reply_to) = msg.reply_to.clone() {
        file = file.reply_to(reply_to);
    }
    match channel.send_file(target, file).await {
        Ok(id) => {
            debug!(message_id = %id.as_str(), "fallback file uploaded");
            Ok(id)
        }
        Err(e) => {
            warn!(error = %e, "fallback file upload failed");
            Err(e)
        }
    }
}

fn looks_like_parse_error(s: &str) -> bool {
    let s = s.to_ascii_lowercase();
    s.contains("parse")
        || s.contains("entities")
        || s.contains("markdown")
        || s.contains("can't parse")
}

fn fallback_filename(stem: &str) -> String {
    let clean: String = stem
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let stem = if clean.is_empty() {
        "reply"
    } else {
        clean.as_str()
    };
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("{stem}-{ts}.txt")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChatId, NotificationPolicy, WorkspaceId};
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeChannel {
        send_results: Mutex<Vec<std::result::Result<(), ChannelError>>>,
        last_sent: Mutex<Option<OutgoingMessage>>,
        last_file: Mutex<Option<FileUpload>>,
    }

    #[async_trait]
    impl Channel for FakeChannel {
        fn name(&self) -> &'static str {
            "fake"
        }
        fn message_char_limit(&self) -> usize {
            4000
        }
        async fn send(&self, _t: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId> {
            *self.last_sent.lock().unwrap() = Some(msg);
            let mut q = self.send_results.lock().unwrap();
            if q.is_empty() {
                return Ok(MessageId::new("ok"));
            }
            match q.remove(0) {
                Ok(()) => Ok(MessageId::new("ok")),
                Err(e) => Err(e),
            }
        }
        async fn edit(
            &self,
            _t: &WorkspaceHandle,
            _id: &MessageId,
            _msg: OutgoingMessage,
        ) -> Result<()> {
            Ok(())
        }
        async fn create_workspace(&self, _p: &ChatId, _title: &str) -> Result<WorkspaceHandle> {
            unimplemented!()
        }
        async fn rename_workspace(&self, _h: &WorkspaceHandle, _title: &str) -> Result<()> {
            Ok(())
        }
        fn subscribe(&self) -> BoxStream<'static, crate::types::ChannelEvent> {
            unimplemented!()
        }
        async fn download_attachment(
            &self,
            _att: &crate::types::IncomingAttachment,
        ) -> Result<Vec<u8>> {
            Ok(Vec::new())
        }
        async fn send_file(&self, _t: &WorkspaceHandle, f: FileUpload) -> Result<MessageId> {
            *self.last_file.lock().unwrap() = Some(f);
            Ok(MessageId::new("file"))
        }
    }

    fn handle() -> WorkspaceHandle {
        WorkspaceHandle::new(ChatId::new("c"), WorkspaceId::new(""))
    }

    #[tokio::test]
    async fn format_rejection_falls_back_to_file() {
        let ch = FakeChannel::default();
        ch.send_results
            .lock()
            .unwrap()
            .push(Err(ChannelError::FormatRejected("bad md".into())));
        let msg = OutgoingMessage::markdown("# hi").silent();
        let id = send_with_fallback(&ch, &handle(), msg, "reply")
            .await
            .unwrap();
        assert_eq!(id.as_str(), "file");
        let file = ch.last_file.lock().unwrap();
        let file = file.as_ref().expect("fallback file");
        assert_eq!(file.notification, NotificationPolicy::Silent);
    }

    #[tokio::test]
    async fn oversized_goes_straight_to_file() {
        let ch = FakeChannel::default();
        let body = "x".repeat(INLINE_BYTE_CEILING + 1);
        let msg = OutgoingMessage::plain(body);
        let id = send_with_fallback(&ch, &handle(), msg, "reply")
            .await
            .unwrap();
        assert_eq!(id.as_str(), "file");
        // No inline send should have been attempted.
        assert!(ch.last_sent.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn happy_path_still_works() {
        let ch = FakeChannel::default();
        let msg = OutgoingMessage::markdown("hello");
        let id = send_with_fallback(&ch, &handle(), msg, "reply")
            .await
            .unwrap();
        assert_eq!(id.as_str(), "ok");
    }

    #[tokio::test]
    async fn attachment_delivery_retry_is_bounded_to_three_retries() {
        let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let attempts_for_operation = std::sync::Arc::clone(&attempts);
        let result = retry_attachment_delivery(
            move || {
                let attempts = std::sync::Arc::clone(&attempts_for_operation);
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err::<(), _>(ChannelError::Transport("still down".into()))
                }
            },
            |_| true,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            ATTACHMENT_DELIVERY_MAX_RETRIES + 1
        );
    }
}
