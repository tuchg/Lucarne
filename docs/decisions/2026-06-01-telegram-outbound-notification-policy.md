# Telegram Outbound Notification Policy

## Context

Telegram delivery has two separate concerns: sending a bot message into the
topic and deciding whether that send should trigger a user push notification.
Lucarne already used silent text messages for status and control replies, but
file uploads and agent attachments had no channel-level silent/notify intent.

## Decision

Add a channel-level `NotificationPolicy` shared by text messages, fallback file
uploads, and agent attachments. Channel callers set semantic intent; Telegram
maps `Silent` to `disable_notification(true)` at the adapter boundary.

Turn progress, reasoning previews, assistant live previews, command/control
acknowledgements, history replay, and other routine status output are silent.
Final assistant output is sent as a new notify-eligible message; once that send
succeeds, the silent live preview is deleted instead of being edited into the
final answer. Final agent attachments keep the default notify policy. Approval
and clarification prompts also notify because they require user action to
unblock a turn.

## Consequences

- Telegram text, document, photo, and video sends can all preserve silent
  delivery intent.
- Fallback files inherit the notification policy of the message they replace.
- Long-running turns can show silent progress without suppressing the final
  answer notification.
- The common channel layer still exposes only delivery intent; Telegram API
  details stay inside `lucarne-telegram`.
