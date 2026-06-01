//! Telegram projection for recorded turn timeline items.
//!
//! This module owns channel-specific message shape: draft previews,
//! final assistant replies, structured command result rendering, and
//! intervention buttons. It does not submit provider work or record
//! provider lifecycle events; callers pass already-recorded read-model
//! snapshots and bounded callback registries for button tokens.

use std::{sync::Arc, time::Instant};

use lucarne::agent_runtime::{
    AgentCommandCatalog, AgentCommandResult, AgentCommandResultData, AgentContextUsage,
    AgentForkTargetCatalog, AgentModelCatalog, AgentPermissionCatalog, AgentSkillCatalog,
    AgentTokenUsage, CommandResultEvent, InstanceId, InterventionRequest,
};
use lucarne::control_plane::command_usage;
use lucarne::control_plane::{
    LiveInstanceState, ReconcileOutcome, StatusSnapshot, SubAgentLinkRecord, SubAgentState,
    TimelineItem, TimelineItemKind,
};
use lucarne::event::{CommandResultData, CommandResultPayload};
#[cfg(test)]
use lucarne_channel::types::{ChatId, WorkspaceId};
use lucarne_channel::{
    agent_message::{render_agent_message_markdown, AgentMessageFooter},
    robust::send_with_fallback_all,
    types::{MessageId, OutgoingButton, OutgoingMessage, WorkspaceHandle},
    Channel,
};
use tracing::{debug, info, warn};

use super::{
    log_event_text, AgentInterventionCallbackRegistry, CommandRunOptions, DrainMode, IntvAction,
    SubAgentCallbackRegistry, EVENT_LOG_TEXT_MAX, STATUS_EDIT_INTERVAL,
};

/// Marker inserted when the in-progress preview grows beyond a single
/// channel message and we need to keep only the newest tail.
pub(super) const PREVIEW_TRUNCATED_NOTICE: &str = "… earlier updates omitted …";
const PROCESS_PREVIEW_CHAR_LIMIT: usize = 1200;

/// One-of the two content kinds we stream to the user.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum DraftKind {
    Message,
    Thought,
}

pub(super) struct ActiveDraft {
    pub(super) kind: DraftKind,
    pub(super) text: String,
    pub(super) message_streaming: bool,
    pub(super) last_push_at: Option<Instant>,
    /// Last text we actually sent to the channel via `send` / `edit`.
    /// Used to skip no-op updates.
    pub(super) last_render: String,
}

/// Streaming output for a single turn. Assistant message chunks update a silent
/// live preview bubble and track a final candidate. Once the provider emits
/// `TurnCompleted`, the final candidate is sent as the formal answer.
pub(super) struct DraftStream {
    pub(super) current: Option<ActiveDraft>,
    /// Latest assistant text candidate to commit as the formal final
    /// answer once the provider emits `TurnCompleted`.
    pub(super) final_message: Option<String>,
    pub(super) final_rich_message: Option<OutgoingMessage>,
    /// Message id of the live edit-streamed reply bubble.
    pub(super) fallback_msg_id: Option<MessageId>,
    pub(super) reply_to: Option<MessageId>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DraftFinalizeResult {
    pub(super) bytes: usize,
    pub(super) message_ids: Vec<MessageId>,
}

impl DraftStream {
    pub(super) fn new() -> Self {
        Self {
            current: None,
            final_message: None,
            final_rich_message: None,
            fallback_msg_id: None,
            reply_to: None,
        }
    }

    pub(super) fn with_reply_to(reply_to: Option<MessageId>) -> Self {
        Self {
            reply_to,
            ..Self::new()
        }
    }

    pub(super) fn push_rich_final(&mut self, msg: OutgoingMessage) {
        self.final_rich_message = Some(msg);
    }

    /// Update the single live reply bubble for this turn. Each new
    /// chunk edits the same channel message; the same message is edited
    /// one final time on `TurnCompleted`.
    pub(super) async fn push(
        &mut self,
        channel: &dyn Channel,
        target: &WorkspaceHandle,
        kind: DraftKind,
        chunk: &str,
        streaming: bool,
    ) {
        if chunk.is_empty() {
            return;
        }

        if kind == DraftKind::Message {
            if streaming {
                self.final_message
                    .get_or_insert_with(String::new)
                    .push_str(chunk);
            } else {
                self.final_message = Some(chunk.to_string());
            }
        }

        let (preview, now) = {
            let cur = self.current.get_or_insert_with(|| ActiveDraft {
                kind,
                text: String::new(),
                message_streaming: false,
                last_push_at: None,
                last_render: String::new(),
            });
            let replace_stream_with_final =
                kind == DraftKind::Message && !streaming && cur.message_streaming;
            if replace_stream_with_final {
                cur.text.clear();
            }
            cur.kind = kind;
            append_preview_chunk(&mut cur.text, kind, chunk, streaming);
            if kind == DraftKind::Message {
                cur.message_streaming = streaming;
            }
            if cur.text.is_empty() {
                return;
            }

            let now = Instant::now();
            let preview_limit = preview_limit_for_kind(kind, channel.message_char_limit());
            let preview = render_draft_preview(&cur.text, preview_limit);
            let should_push = cur
                .last_push_at
                .map(|t| now.duration_since(t) >= STATUS_EDIT_INTERVAL)
                .unwrap_or(true);
            if !should_push || preview == cur.last_render {
                return;
            }

            (preview, now)
        };

        self.edit_fallback(channel, target, &preview).await;
        if let Some(c) = self.current.as_mut() {
            c.last_render = preview;
            c.last_push_at = Some(now);
        }
    }

    async fn edit_fallback(&mut self, channel: &dyn Channel, target: &WorkspaceHandle, text: &str) {
        let msg = self.with_reply(OutgoingMessage::markdown(text.to_string()).silent());
        match self.fallback_msg_id.as_ref() {
            Some(id) => {
                if let Err(e) = channel.edit(target, id, msg).await {
                    debug!(error = %e, "fallback edit failed");
                }
            }
            None => match channel.send(target, msg).await {
                Ok(id) => {
                    self.fallback_msg_id = Some(id);
                }
                Err(e) => {
                    warn!(error = %e, "fallback send failed");
                }
            },
        }
    }

    /// Finalize the turn by sending a formal final reply if one exists.
    /// Live preview bubbles are silent drafts; after the final send
    /// succeeds, the preview is deleted so the final reply is the
    /// message that may notify the user.
    pub(super) async fn finalize(
        &mut self,
        channel: &dyn Channel,
        target: &WorkspaceHandle,
        provider_id: &str,
        footer: Option<&AgentMessageFooter>,
    ) -> DraftFinalizeResult {
        let preview_id = self.fallback_msg_id.take();
        self.current.take();

        if let Some(msg) = self.final_rich_message.take() {
            let msg = self.with_reply(msg);
            let bytes = msg.body.len();
            let mut result = DraftFinalizeResult {
                bytes,
                message_ids: Vec::new(),
            };
            match channel.send(target, msg).await {
                Ok(id) => {
                    result.message_ids.push(id);
                    if let Some(preview_id) = preview_id.as_ref() {
                        delete_preview(channel, target, preview_id).await;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "rich final send failed");
                }
            }
            return result;
        }

        let Some(final_text) = self.final_message.take() else {
            if let Some(preview_id) = preview_id.as_ref() {
                delete_preview(channel, target, preview_id).await;
            }
            return DraftFinalizeResult::default();
        };
        let final_text = match footer {
            Some(footer) => render_agent_message_markdown(&final_text, footer),
            None => final_text,
        };
        let bytes = final_text.len();
        let mut result = DraftFinalizeResult {
            bytes,
            message_ids: Vec::new(),
        };
        info!(
            target: "lucarne_telegram::turn",
            provider = provider_id,
            bytes,
            chars = final_text.chars().count(),
            agent_return = %log_event_text(&final_text, EVENT_LOG_TEXT_MAX),
            "sending final assistant reply"
        );
        let msg = self.with_reply(final_reply_message(final_text));
        match send_with_fallback_all(channel, target, msg, provider_id).await {
            Ok(ids) => {
                result.message_ids = ids;
                if let Some(preview_id) = preview_id.as_ref() {
                    delete_preview(channel, target, preview_id).await;
                }
            }
            Err(e) => {
                warn!(error = %e, "final send failed");
            }
        }
        result
    }

    fn with_reply(&self, mut msg: OutgoingMessage) -> OutgoingMessage {
        if msg.reply_to.is_none() {
            msg.reply_to = self.reply_to.clone();
        }
        msg
    }
}

async fn delete_preview(channel: &dyn Channel, target: &WorkspaceHandle, id: &MessageId) {
    if let Err(e) = channel.delete(target, id).await {
        warn!(error = %e, "preview delete failed");
    }
}

fn final_reply_message(text: String) -> OutgoingMessage {
    OutgoingMessage::markdown(text)
}

pub(super) fn maybe_reply_to(
    mut msg: OutgoingMessage,
    reply_to: Option<&MessageId>,
) -> OutgoingMessage {
    if msg.reply_to.is_none() {
        msg.reply_to = reply_to.cloned();
    }
    msg
}

fn append_preview_chunk(buf: &mut String, kind: DraftKind, chunk: &str, streaming: bool) {
    if kind == DraftKind::Message && streaming {
        if chunk.is_empty() {
            return;
        }
        buf.push_str(chunk);
        return;
    }
    let chunk = chunk.trim();
    if chunk.is_empty() {
        return;
    }
    let rendered = match kind {
        DraftKind::Message => chunk.to_string(),
        DraftKind::Thought => chunk.to_string(),
    };
    if buf.is_empty() {
        buf.push_str(&rendered);
        return;
    }
    if buf.ends_with(&rendered) {
        return;
    }
    buf.push_str("\n\n");
    buf.push_str(&rendered);
}

fn preview_limit_for_kind(kind: DraftKind, channel_limit: usize) -> usize {
    match kind {
        DraftKind::Message => channel_limit,
        DraftKind::Thought => channel_limit.min(PROCESS_PREVIEW_CHAR_LIMIT),
    }
}

pub(super) fn render_draft_preview(text: &str, limit: usize) -> String {
    let full = text.to_string();
    if full.chars().count() <= limit {
        return full;
    }

    let reserved = PREVIEW_TRUNCATED_NOTICE.chars().count() + 2;
    if limit <= reserved {
        return full.chars().take(limit).collect();
    }
    let tail_limit = limit - reserved;
    let tail: String = text
        .chars()
        .rev()
        .take(tail_limit)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{PREVIEW_TRUNCATED_NOTICE}\n\n{tail}")
}

pub(super) fn timeline_text(item: &TimelineItem) -> Option<&str> {
    item.payload.get("text").and_then(serde_json::Value::as_str)
}

fn timeline_bool(item: &TimelineItem, key: &str) -> Option<bool> {
    item.payload.get(key).and_then(serde_json::Value::as_bool)
}

fn command_result_from_timeline(item: &TimelineItem) -> Result<CommandResultEvent, String> {
    if item.kind != TimelineItemKind::CommandResult {
        return Err(format!(
            "expected command_result timeline item, got {:?}",
            item.kind
        ));
    }
    let payload = serde_json::from_value::<CommandResultPayload>(item.payload.clone())
        .map_err(|err| format!("command result timeline projection is invalid: {err}"))?;
    Ok(CommandResultEvent {
        command: payload.command.clone().into(),
        result: payload,
    })
}

#[cfg(test)]
pub(super) fn render_command_result(
    workspace: &WorkspaceId,
    event: &CommandResultEvent,
    options: Option<&CommandRunOptions>,
) -> Option<OutgoingMessage> {
    let target = WorkspaceHandle::new(ChatId::new("test"), workspace.clone());
    render_command_result_with_snapshot(
        &target,
        event,
        options,
        options.and_then(|options| options.status_snapshot.as_ref()),
    )
}

pub(super) fn render_command_result_with_snapshot(
    target: &WorkspaceHandle,
    event: &CommandResultEvent,
    options: Option<&CommandRunOptions>,
    status_snapshot: Option<&StatusSnapshot>,
) -> Option<OutgoingMessage> {
    match &event.result.result {
        CommandResultData::Models(catalog) => Some(render_models(catalog)),
        CommandResultData::ModelChanged(selection) => Some(render_command_text(format!(
            "Updated model to {}{}.",
            selection.model,
            selection
                .reasoning
                .as_ref()
                .map(|reasoning| format!(" with reasoning effort {reasoning}"))
                .unwrap_or_default()
        ))),
        CommandResultData::Permissions(catalog) => {
            Some(render_permissions(target, catalog, options))
        }
        CommandResultData::PermissionsChanged(selection) => Some(render_command_text(format!(
            "Updated permissions to {}.",
            selection.mode
        ))),
        CommandResultData::Status(_) => Some(match status_snapshot {
            Some(snapshot) => render_agent_status(
                snapshot,
                options.and_then(|options| options.status_resource.as_ref()),
            ),
            None => render_missing_status_snapshot(),
        }),
        CommandResultData::Skills(catalog) => Some(render_skills(catalog)),
        CommandResultData::Forked(result) => {
            let text = match result.session_ref.as_ref() {
                Some(session_ref) => format!("✓ forked `{}`", session_ref.0),
                None => "✓ forked".into(),
            };
            Some(render_command_text(text))
        }
        CommandResultData::ForkTargets(catalog) => {
            Some(render_fork_targets(target, catalog, options))
        }
        CommandResultData::Commands(catalog) => Some(render_commands(target, catalog, options)),
        CommandResultData::Text { text } => Some(render_command_text(text.to_string())),
    }
}

pub(super) fn render_immediate_command_result(
    target: &WorkspaceHandle,
    result: &AgentCommandResult,
    options: Option<&CommandRunOptions>,
    status_snapshot: Option<&StatusSnapshot>,
) -> Option<OutgoingMessage> {
    match &result.data {
        AgentCommandResultData::Models(catalog) => Some(render_models(catalog)),
        AgentCommandResultData::Permissions(catalog) => {
            Some(render_permissions(target, catalog, options))
        }
        AgentCommandResultData::Skills(catalog) => Some(render_skills(catalog)),
        AgentCommandResultData::Commands(catalog) => {
            Some(render_commands(target, catalog, options))
        }
        AgentCommandResultData::ForkTargets(catalog) => {
            Some(render_fork_targets(target, catalog, options))
        }
        AgentCommandResultData::Fork(result) => {
            let text = match result.session_ref.as_ref() {
                Some(session_ref) => format!("✓ forked `{}`", session_ref.0),
                None => "✓ forked".into(),
            };
            Some(render_command_text(text))
        }
        AgentCommandResultData::Status(status) if result.name.as_str() == "model" => {
            status.model.as_ref().map(|model| {
                render_command_text(format!(
                    "Updated model to {}{}.",
                    model,
                    status
                        .reasoning
                        .as_ref()
                        .map(|reasoning| format!(" with reasoning effort {reasoning}"))
                        .unwrap_or_default()
                ))
            })
        }
        AgentCommandResultData::Status(status) if result.name.as_str() == "permissions" => status
            .permissions
            .as_ref()
            .map(|mode| render_command_text(format!("Updated permissions to {}.", mode))),
        AgentCommandResultData::Status(_) => Some(match status_snapshot {
            Some(snapshot) => render_agent_status(
                snapshot,
                options.and_then(|options| options.status_resource.as_ref()),
            ),
            None => render_missing_status_snapshot(),
        }),
        AgentCommandResultData::Text { text } => Some(render_command_text(text.to_string())),
        AgentCommandResultData::Json(value) => Some(render_command_text(
            serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
        )),
        AgentCommandResultData::Empty => None,
    }
}

fn render_command_text(text: String) -> OutgoingMessage {
    OutgoingMessage::markdown(text).silent()
}

fn render_agent_status(
    snapshot: &StatusSnapshot,
    resource: Option<&lucarne::core_service::AgentResourceEntry>,
) -> OutgoingMessage {
    let mut body = String::from("status\n");
    if let Some(version) = snapshot.provider_version.as_deref() {
        body.push_str(&format!("Version: `{version}`\n"));
    }
    if let Some(model) = snapshot.model.as_deref() {
        body.push_str(&format!("Model: `{model}`"));
        match (
            snapshot.model_detail.as_deref(),
            snapshot.reasoning.as_deref(),
        ) {
            (Some(detail), Some(reasoning)) => {
                body.push_str(&format!(" (`{detail}`, reasoning {reasoning})"));
            }
            (Some(detail), None) => {
                body.push_str(&format!(" (`{detail}`)"));
            }
            (None, Some(reasoning)) => {
                body.push_str(&format!(" (reasoning {reasoning})"));
            }
            (None, None) => {}
        }
        body.push('\n');
    }
    if let Some(directory) = snapshot
        .directory
        .as_deref()
        .map(str::to_string)
        .or_else(|| {
            snapshot
                .project_path
                .as_ref()
                .map(|path| path.to_string_lossy().into_owned())
        })
    {
        body.push_str(&format!("Directory: `{directory}`\n"));
    }
    if let Some(permissions) = snapshot.permission_mode.as_deref() {
        body.push_str(&format!("Permissions: `{permissions}`\n"));
    }
    if let Some(path) = snapshot.agents_md.as_deref() {
        body.push_str(&format!("Agents.md: `{path}`\n"));
    }
    if let Some(account) = snapshot.account.as_deref() {
        body.push_str(&format!("Account: {account}\n"));
    }
    if let Some(base_url) = snapshot.base_url.as_deref() {
        body.push_str(&format!("Base URL: `{base_url}`\n"));
    }
    if let Some(proxy) = snapshot.proxy.as_deref() {
        body.push_str(&format!("Proxy: `{proxy}`\n"));
    }
    if let Some(sources) = snapshot.setting_sources.as_deref() {
        body.push_str(&format!("Setting sources: {sources}\n"));
    }
    if let Some(session_id) = snapshot
        .native_resume_ref
        .as_deref()
        .or_else(|| snapshot.provider_session_id.as_ref().map(|id| id.as_str()))
    {
        body.push_str(&format!("Session: `{session_id}`\n"));
    }
    let token_usage = snapshot.token_usage.clone().or_else(|| {
        snapshot
            .usage_snapshot
            .as_ref()
            .and_then(|usage| serde_json::from_value::<AgentTokenUsage>(usage.clone()).ok())
    });
    if let Some(tokens) = &token_usage {
        if tokens.input_tokens.is_some() || tokens.output_tokens.is_some() {
            body.push_str(&format!(
                "🧮 Token usage: {} in / {} out\n",
                compact_number(tokens.input_tokens.unwrap_or(0)),
                compact_number(tokens.output_tokens.unwrap_or(0))
            ));
        }
    }
    let context_usage = snapshot.context_usage.clone().or_else(|| {
        snapshot
            .context_snapshot
            .as_ref()
            .and_then(|context| serde_json::from_value::<AgentContextUsage>(context.clone()).ok())
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
                "📚 Context: {}/{} ({}%)",
                compact_number(used),
                compact_number(max),
                percent
            ));
            if let Some(compactions) = snapshot.compactions {
                body.push_str(&format!(" · 🧹 Compactions: {compactions}"));
            }
            body.push('\n');
        }
    }
    if let Some(resource) = resource {
        render_process_resource(resource, &mut body);
    }
    render_status_snapshot(snapshot, &mut body);
    if !body.ends_with('\n') {
        body.push('\n');
    }
    OutgoingMessage::markdown(body).silent()
}

fn render_process_resource(
    resource: &lucarne::core_service::AgentResourceEntry,
    body: &mut String,
) {
    body.push_str(&format!(
        "Process identity: `{}`\n",
        resource.identity.as_deref().unwrap_or("unidentified")
    ));
    body.push_str(&format!(
        "PID: `{}`\n",
        resource
            .pid
            .map(|pid| pid.to_string())
            .unwrap_or_else(|| "unknown".to_string())
    ));
    body.push_str(&format!("Processes: `{}`\n", resource.process_count));
    body.push_str(&format!("CPU: `{:.1}%`\n", resource.cpu_percent));
    body.push_str(&format!(
        "Memory: `{}`\n",
        format_resource_bytes(resource.memory_bytes)
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

fn render_missing_status_snapshot() -> OutgoingMessage {
    OutgoingMessage::markdown("status\nStatus snapshot unavailable\n").silent()
}

fn render_status_snapshot(snapshot: &StatusSnapshot, body: &mut String) {
    if let Some(workspace) = &snapshot.workspace_id {
        body.push_str(&format!("Workspace: `{}`\n", workspace.as_str()));
    }
    if let Some(provider) = snapshot.provider_id.as_ref() {
        body.push_str(&format!("Provider: `{provider}`\n"));
    }
    if let Some(state) = snapshot.live_instance_state {
        body.push_str(&format!("Live: `{}`\n", live_state_label(state)));
    }
    if let Some(binding) = snapshot.channel_binding_state.as_deref() {
        body.push_str(&format!("Channel: `{binding}`\n"));
    }
    if let Some(outcome) = &snapshot.last_reconcile_outcome {
        body.push_str(&format!("Reconcile: `{}`\n", reconcile_label(outcome)));
    }
}

fn live_state_label(state: LiveInstanceState) -> &'static str {
    match state {
        LiveInstanceState::Starting => "starting",
        LiveInstanceState::Idle => "idle",
        LiveInstanceState::Running => "running",
        LiveInstanceState::WaitingPermission => "waiting_permission",
        LiveInstanceState::Closing => "closing",
        LiveInstanceState::Closed => "closed",
        LiveInstanceState::Failed => "failed",
        LiveInstanceState::Stale => "stale",
    }
}

fn reconcile_label(outcome: &ReconcileOutcome) -> &'static str {
    match outcome {
        ReconcileOutcome::Ok => "ok",
        ReconcileOutcome::StaleRevision => "stale_revision",
        ReconcileOutcome::TopicMissing => "topic_missing",
        ReconcileOutcome::TopicMissingRecreated => "topic_missing_recreated",
        ReconcileOutcome::ProviderSessionProbeRequired => "provider_session_probe_required",
        ReconcileOutcome::ProviderSessionStale => "provider_session_stale",
        ReconcileOutcome::LiveInstanceStale => "live_instance_stale",
        ReconcileOutcome::TurnOrphaned => "turn_orphaned",
        ReconcileOutcome::PermissionOrphaned => "permission_orphaned",
        ReconcileOutcome::ManualAttentionRequired => "manual_attention_required",
    }
}

fn render_models(catalog: &AgentModelCatalog) -> OutgoingMessage {
    let mut body = String::from("models\n");
    if let Some(current) = catalog.current_model.as_deref() {
        body.push_str(&format!("current: `{current}`"));
        if let Some(reasoning) = catalog.current_reasoning.as_deref() {
            body.push_str(&format!(" (reasoning `{reasoning}`)"));
        }
        body.push('\n');
    }
    body.push_str("\nset: `/model <model> [reasoning]`\n");
    if let Some(example) = model_usage_example(catalog) {
        body.push_str(&format!("example: `{example}`\n"));
    }
    body.push_str("reasoning levels: ");
    append_reasoning_levels(&mut body, catalog);
    body.push('\n');
    if catalog.models.is_empty() {
        body.push_str("\n(none)\n");
    } else {
        body.push_str("\navailable:\n");
        for (index, model) in catalog.models.iter().enumerate() {
            append_list_item_gap(&mut body, index);
            let title = model.display_name.as_deref().unwrap_or(model.id.as_str());
            body.push_str(&format!("{}. `{}`", index + 1, model.id));
            if title != model.id.as_str() {
                body.push_str(&format!(" — {title}"));
            }
            if let Some(description) = &model.description {
                body.push_str(&format!(" — {description}"));
            }
            body.push('\n');
        }
    }
    OutgoingMessage::markdown(body).silent()
}

fn append_reasoning_levels(body: &mut String, catalog: &AgentModelCatalog) {
    let mut levels = Vec::<&str>::new();
    for model in &catalog.models {
        for effort in &model.supported_reasoning {
            if !levels.contains(&effort.value.as_str()) {
                levels.push(effort.value.as_str());
            }
        }
    }
    if levels.is_empty() {
        body.push_str("(not listed)");
        return;
    }
    for (index, level) in levels.iter().enumerate() {
        if index > 0 {
            body.push_str(", ");
        }
        body.push_str(&format!("`{level}`"));
    }
}

fn model_usage_example(catalog: &AgentModelCatalog) -> Option<String> {
    let model = catalog.models.first()?;
    let reasoning = model.supported_reasoning.last()?;
    Some(format!("/model {} {}", model.id, reasoning.value))
}

fn render_permissions(
    _target: &WorkspaceHandle,
    catalog: &AgentPermissionCatalog,
    _options: Option<&CommandRunOptions>,
) -> OutgoingMessage {
    let mut body = String::from("permission modes\n");
    if let Some(current) = catalog.current_mode.as_deref() {
        body.push_str(&format!("current: `{current}`\n"));
    }
    body.push_str("\nset: `/permissions <mode>`\n");
    if catalog.modes.is_empty() {
        body.push_str("\n(none)\n");
    } else {
        body.push_str("\navailable:\n");
        for (index, mode) in catalog.modes.iter().enumerate() {
            append_list_item_gap(&mut body, index);
            let title = mode.display_name.as_deref().unwrap_or(mode.id.as_str());
            body.push_str(&format!("{}. `{}`", index + 1, mode.id));
            if title != mode.id.as_str() {
                body.push_str(&format!(" — {title}"));
            }
            if let Some(description) = &mode.description {
                body.push_str(&format!(" — {description}"));
            }
            body.push('\n');
        }
    }
    OutgoingMessage::markdown(body).silent()
}

fn render_skills(catalog: &AgentSkillCatalog) -> OutgoingMessage {
    const SUPERPOWERS_PREFIX: &str = "superpowers:";

    let mut body = String::from("skills\n");
    if catalog.skills.is_empty() {
        body.push_str("\n(none)\n");
    } else {
        body.push('\n');
        let superpowers = catalog
            .skills
            .iter()
            .filter_map(|skill| skill.name.strip_prefix(SUPERPOWERS_PREFIX))
            .filter(|name| !name.is_empty() && *name != "*")
            .collect::<Vec<_>>();
        let mut wrote_superpowers = false;
        for skill in &catalog.skills {
            let name = skill.name.as_str();
            if name == "superpowers:*" || name.starts_with(SUPERPOWERS_PREFIX) {
                if !wrote_superpowers {
                    body.push_str("- `superpowers:*`\n");
                    for child in &superpowers {
                        body.push_str(&format!("  |-- `{child}`\n"));
                    }
                    wrote_superpowers = true;
                }
                continue;
            }
            body.push_str(&format!("- `{name}`\n"));
        }
    }
    OutgoingMessage::markdown(body).silent()
}

fn render_fork_targets(
    _target: &WorkspaceHandle,
    catalog: &AgentForkTargetCatalog,
    _options: Option<&CommandRunOptions>,
) -> OutgoingMessage {
    let mut body = String::from("fork targets\n");
    if catalog.targets.is_empty() {
        body.push_str("\n(none)\n");
    } else {
        for (index, target) in catalog.targets.iter().enumerate() {
            append_list_item_gap(&mut body, index);
            let label = target.label.as_deref().unwrap_or(target.id.as_str());
            body.push_str(&format!("/f{}  `{}`", index + 1, target.id));
            if label != target.id.as_str() {
                body.push_str(&format!(" — {label}"));
            }
            if let Some(description) = &target.description {
                body.push_str(&format!(" — {description}"));
            }
            body.push('\n');
        }
    }
    OutgoingMessage::markdown(body).silent()
}

fn render_commands(
    _target: &WorkspaceHandle,
    catalog: &AgentCommandCatalog,
    options: Option<&CommandRunOptions>,
) -> OutgoingMessage {
    let mut body = String::from("agent commands\n");
    if let Some(provider_id) = options.and_then(|options| options.provider_id) {
        body.push_str(&format!("provider: `{provider_id}`\n"));
    }
    for (index, command) in catalog.commands.iter().enumerate() {
        append_list_item_gap(&mut body, index);
        body.push_str(&format!("{}. `/{}`\n", index + 1, command.name));
        if let Some(description) = &command.description {
            body.push_str(&format!("   {description}\n"));
        }
        body.push_str(&format!("   usage: `{}`\n", command_usage(command)));
        if !command.aliases.is_empty() {
            let aliases = command
                .aliases
                .iter()
                .map(|alias| format!("`/{}`", alias.as_str()))
                .collect::<Vec<_>>()
                .join(", ");
            body.push_str(&format!("   aliases: {aliases}\n"));
        }
    }
    OutgoingMessage::markdown(body).silent()
}

pub(crate) fn render_subagent_links(
    target: &WorkspaceHandle,
    links: &[SubAgentLinkRecord],
    callback_registry: Option<&dyn SubAgentCallbackRegistry>,
) -> Option<OutgoingMessage> {
    if links.is_empty() {
        return None;
    }

    let mut body = String::from("subagents\n");
    let mut buttons = Vec::new();
    for (idx, link) in links.iter().enumerate() {
        append_list_item_gap(&mut body, idx);
        let label = subagent_label(link);
        let state = subagent_state_label(link.state);
        body.push_str(&format!("{}. {} — {state}", idx + 1, label));
        if let Some(model) = link.model.as_ref() {
            body.push_str(&format!(" · {model}"));
        }
        if let Some(message) = link.last_message.as_deref() {
            body.push_str(&format!(" — {}", short(message, 80)));
        }
        body.push('\n');

        if link.openable {
            if let Some(registry) = callback_registry {
                let data = registry.subagent_button_data(target, link);
                assert!(
                    data.len() <= 64,
                    "telegram callback_data exceeds 64 bytes: {data}"
                );
                buttons.push(OutgoingButton {
                    label: short(&format!("open {label}"), 28),
                    data,
                });
            }
        }
    }

    Some(
        OutgoingMessage::plain(body)
            .with_buttons(button_rows(buttons, 2))
            .silent(),
    )
}

fn subagent_label(link: &SubAgentLinkRecord) -> String {
    link.label
        .as_deref()
        .or(link.nickname.as_deref())
        .or(link.role.as_deref())
        .or(link.prompt.as_deref())
        .unwrap_or("subagent")
        .to_string()
}

fn subagent_state_label(state: SubAgentState) -> &'static str {
    match state {
        SubAgentState::Starting => "starting",
        SubAgentState::Running => "running",
        SubAgentState::Waiting => "waiting",
        SubAgentState::Completed => "completed",
        SubAgentState::Failed => "failed",
        SubAgentState::Stopped => "stopped",
        SubAgentState::Unsupported => "not openable",
    }
}

fn append_list_item_gap(body: &mut String, index: usize) {
    if index > 0 {
        body.push('\n');
    }
}

fn approval_description(a: &lucarne::ApprovalRequest) -> Option<String> {
    a.message
        .as_deref()
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| a.input.as_ref().and_then(approval_input_summary))
}

fn approval_input_summary(input: &serde_json::Value) -> Option<String> {
    let Some(obj) = input.as_object() else {
        return serde_json::to_string(input)
            .ok()
            .filter(|rendered| !rendered.is_empty())
            .map(|rendered| format!("Details: {rendered}"));
    };

    let mut lines = Vec::new();
    if let Some(title) = string_field(obj, &["title"]) {
        lines.push(format!("Action: {title}"));
    }
    if let Some(command) = string_field(obj, &["command"]) {
        lines.push(format!("Command: {command}"));
    }
    if let Some(file) = string_field(obj, &["file", "path"]) {
        lines.push(format!("File: {file}"));
    }
    if let Some(cwd) = string_field(obj, &["cwd"]) {
        lines.push(format!("CWD: {cwd}"));
    }

    if lines.is_empty() {
        serde_json::to_string(input)
            .ok()
            .filter(|rendered| !rendered.is_empty())
            .map(|rendered| format!("Details: {rendered}"))
    } else {
        Some(lines.join("\n"))
    }
}

fn string_field<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| obj.get(*key).and_then(serde_json::Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn button_rows(buttons: Vec<OutgoingButton>, width: usize) -> Vec<Vec<OutgoingButton>> {
    let width = width.max(1);
    buttons
        .chunks(width)
        .map(|chunk| chunk.to_vec())
        .collect::<Vec<_>>()
}

fn short(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!(
            "{}…",
            s.chars().take(max.saturating_sub(1)).collect::<String>()
        )
    }
}

fn compact_number(value: u64) -> String {
    if value >= 1_000_000 {
        let n = value as f64 / 1_000_000.0;
        trim_number(n, "m")
    } else if value >= 1_000 {
        let n = value as f64 / 1_000.0;
        trim_number(n, "k")
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

/// Render an intervention request as a Telegram message with inline
/// buttons. Callback data is encoded so a later click can be parsed
/// back with `parse_intv_callback`.
pub(super) fn build_intervention_message(
    req: &InterventionRequest,
    registry: Option<&Arc<dyn AgentInterventionCallbackRegistry>>,
    target: &WorkspaceHandle,
    live_instance: &InstanceId,
) -> OutgoingMessage {
    match req {
        InterventionRequest::Approval(a) => {
            let desc = approval_description(a);
            let body = if let Some(desc) = desc {
                format!(
                    "🙋 Tool `{}` requests approval\n\n{}",
                    a.tool_name,
                    short(&desc, 400)
                )
            } else {
                format!("🙋 Tool `{}` requests approval", a.tool_name)
            };
            let mut message = OutgoingMessage::markdown(body);
            if let Some(registry) = registry {
                let buttons = vec![vec![
                    OutgoingButton {
                        label: "✅ Allow".into(),
                        data: registry.intervention_button_data(
                            target,
                            live_instance,
                            a.req_id.as_str(),
                            IntvAction::Approve { allow: true },
                        ),
                    },
                    OutgoingButton {
                        label: "❌ Deny".into(),
                        data: registry.intervention_button_data(
                            target,
                            live_instance,
                            a.req_id.as_str(),
                            IntvAction::Approve { allow: false },
                        ),
                    },
                ]];
                message = message.with_buttons(buttons);
            }
            message
        }
        InterventionRequest::Question(q) => {
            let first = q.questions.first();
            let header = first
                .and_then(|qq| qq.header.as_deref())
                .unwrap_or("agent 需要澄清");
            let text = first.map(|qq| qq.text.as_ref()).unwrap_or("");
            let body = if text.is_empty() {
                format!("🙋 {header}")
            } else {
                format!("🙋 {header}\n\n{text}")
            };
            let mut rows: Vec<Vec<OutgoingButton>> = Vec::new();
            if let Some(question) = first {
                if !question.multi_select {
                    for opt in question.options.iter().take(8) {
                        if let Some(registry) = registry {
                            rows.push(vec![OutgoingButton {
                                label: opt.label.to_string(),
                                data: registry.intervention_button_data(
                                    target,
                                    live_instance,
                                    q.req_id.as_str(),
                                    IntvAction::Answer {
                                        q_idx: 0,
                                        values: vec![opt.label.to_string()],
                                    },
                                ),
                            }]);
                        }
                    }
                }
            }
            OutgoingMessage::markdown(body).with_buttons(rows)
        }
    }
}

pub(super) enum TimelineRenderOutcome {
    None,
    CommandResult {
        payload: serde_json::Value,
    },
    Intervention {
        req_id: String,
        message_id: MessageId,
    },
}

pub(super) async fn render_timeline_item_to_draft(
    channel: &Arc<dyn Channel>,
    target: &WorkspaceHandle,
    live_instance: &InstanceId,
    drafts: &mut DraftStream,
    mode: &DrainMode,
    item: &TimelineItem,
    status_snapshot: Option<&StatusSnapshot>,
) -> Result<TimelineRenderOutcome, String> {
    match item.kind {
        TimelineItemKind::Assistant => {
            let projected_text = timeline_text(item)
                .ok_or_else(|| "assistant timeline projection missing text".to_string())?
                .to_string();
            let projected_streaming = timeline_bool(item, "streaming").unwrap_or(false);
            drafts
                .push(
                    &**channel,
                    target,
                    DraftKind::Message,
                    &projected_text,
                    projected_streaming,
                )
                .await;
            Ok(TimelineRenderOutcome::None)
        }
        TimelineItemKind::Reasoning => {
            let projected_text = timeline_text(item)
                .ok_or_else(|| "reasoning timeline projection missing text".to_string())?
                .to_string();
            drafts
                .push(
                    &**channel,
                    target,
                    DraftKind::Thought,
                    &projected_text,
                    false,
                )
                .await;
            Ok(TimelineRenderOutcome::None)
        }
        TimelineItemKind::CommandResult => {
            let projected_result = command_result_from_timeline(item)?;
            if let Some(msg) = render_command_result_with_snapshot(
                target,
                &projected_result,
                mode.command_options(),
                status_snapshot,
            ) {
                drafts.push_rich_final(msg);
            }
            Ok(TimelineRenderOutcome::CommandResult {
                payload: item.payload.clone(),
            })
        }
        TimelineItemKind::Permission => {
            let projected_req = serde_json::from_value::<InterventionRequest>(item.payload.clone())
                .map_err(|err| format!("permission timeline projection is invalid: {err}"))?;
            let req_id = match &projected_req {
                InterventionRequest::Approval(a) => a.req_id.to_string(),
                InterventionRequest::Question(q) => q.req_id.to_string(),
            };
            let msg = build_intervention_message(
                &projected_req,
                mode.intervention_callback_registry(),
                target,
                live_instance,
            );
            let message_id = channel
                .send(target, msg)
                .await
                .map_err(|err| err.to_string())?;
            Ok(TimelineRenderOutcome::Intervention { req_id, message_id })
        }
        _ => Ok(TimelineRenderOutcome::None),
    }
}
