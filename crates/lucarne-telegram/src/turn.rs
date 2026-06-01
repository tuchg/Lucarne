//! Single-turn runner for submitting work and draining provider events.
//!
//! The runner owns provider communication and control-plane recording:
//!
//! 1. Submitting the user's prompt to the agent session.
//! 2. Sending a lightweight **status message** ("⏳ Processing · 3s") that is
//!    edited in place as intermediate agent events arrive (tool calls,
//!    reasoning snippets, elapsed-time ticks).
//! 3. Appending provider events to the control-plane timeline.
//! 4. Draining agent events until the provider emits an explicit turn
//!    completion signal, then finalising the status line.
//!
//! Telegram-specific timeline projection lives in [`projection`]. Keeping
//! that separate prevents provider communication, control-plane writes, and
//! channel rendering from bleeding into each other.

use base64::Engine as _;
#[cfg(test)]
use lucarne::agent_runtime::InterventionRequest;
use lucarne::agent_runtime::{
    AgentCommandInvocation, AgentCommandResult, AgentCommandResultData, AgentInput, AgentStatus,
    Attachment as AgentAttachment, CommandResultEvent, Event, InstanceId, MessageEvent,
    MessageRole,
};
#[cfg(test)]
use lucarne::agent_runtime::{AgentSkillCatalog, AgentSkillSummary, ProviderId};
use lucarne::control_plane::{
    CommandCompletionPolicy, StatusSnapshot, SubAgentLinkRecord, TimelineItem, TimelineItemKind,
    TurnId as ControlTurnId,
};
use lucarne::core_service::{
    AgentResourceEntry, CoreWorkspaceEventRecvError, CoreWorkspaceEventTryRecvError,
};
use lucarne::event::CommandResultData;
use lucarne_channel::{
    agent_message::{format_cost_duration, AgentMessageFooter},
    robust::retry_attachment_delivery,
    robust::{send_with_fallback, send_with_fallback_all},
    types::{
        Attachment as ChannelAttachment, ChannelError, MessageId, NotificationPolicy,
        OutgoingMessage, WorkspaceHandle,
    },
    Channel,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tracing::{debug, info, instrument, warn};

mod projection;

pub(crate) use projection::render_subagent_links;
#[cfg(test)]
use projection::{
    build_intervention_message, render_command_result, render_draft_preview, DraftKind,
    PREVIEW_TRUNCATED_NOTICE,
};
use projection::{
    maybe_reply_to, render_immediate_command_result, render_timeline_item_to_draft, DraftStream,
    TimelineRenderOutcome,
};

use crate::state::LiveSession;

/// Provider-native slash commands such as Claude `/cost` can emit their
/// complete output without a turn-complete event.
const PROVIDER_IDLE_QUIET: Duration = Duration::from_secs(3);
/// Minimum interval between status-message edits (Telegram ~1/s).
const STATUS_EDIT_INTERVAL: Duration = Duration::from_millis(1200);
/// After this much silence from the agent, start appending a
/// "still waiting" hint to the status line so the user can tell the
/// difference between "actively working" and "hung".
const QUIET_HEARTBEAT: Duration = Duration::from_secs(30);
/// Maximum characters to keep from an activity detail line.
const ACTIVITY_DETAIL_MAX: usize = 80;
/// Maximum characters from agent-emitted text/JSON copied into logs.
const EVENT_LOG_TEXT_MAX: usize = 2000;
/// Maximum decoded bytes accepted for one agent attachment.
const MAX_ATTACHMENT_BYTES: usize = 8 * 1024 * 1024;
/// Telegram attachment captions are capped by platform policy; leave headroom
/// and send overflow as a separate message from Telegram delivery code.
const TELEGRAM_ATTACHMENT_CAPTION_CHARS: usize = 900;
/// Format a [`Duration`] as a compact human string: `5s`, `2m 13s`, `1h 04m`.
pub fn format_elapsed(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        return format!("{secs}s");
    }
    if secs < 3600 {
        let m = secs / 60;
        let s = secs % 60;
        return format!("{m}m {s:02}s");
    }
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    format!("{h}h {m:02}m")
}

/// Shared progress snapshot updated by the event loop and consumed by
/// the ticker task.
struct Progress {
    /// Latest timer/status activity line (e.g. `🔧 Bash`), sans timer.
    activity: String,
    /// Tool call count (for "n steps" counter).
    tool_calls: u32,
    /// Wall-clock time of the most recent agent event (any kind). Used
    /// by the status ticker to surface a heartbeat warning when the
    /// agent has gone quiet mid-turn.
    last_event_at: Instant,
}

impl Progress {
    fn new(now: Instant) -> Self {
        Self {
            activity: String::new(),
            tool_calls: 0,
            last_event_at: now,
        }
    }
}

struct Shared {
    progress: Mutex<Progress>,
    start: Instant,
    /// Set once the turn is complete; stops the ticker.
    done: AtomicBool,
    /// Notified when progress changes, so the ticker can edit
    /// immediately rather than wait for its interval tick.
    bump: Notify,
}

impl Shared {
    fn new() -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            progress: Mutex::new(Progress::new(now)),
            start: now,
            done: AtomicBool::new(false),
            bump: Notify::new(),
        })
    }

    fn mark_event(&self) {
        self.progress.lock().unwrap().last_event_at = Instant::now();
    }

    fn set_activity(&self, line: impl Into<String>) {
        {
            let mut g = self.progress.lock().unwrap();
            g.activity = line.into();
            g.last_event_at = Instant::now();
        }
        self.bump.notify_one();
    }

    fn bump_tool(&self, name: &str, detail: &str) {
        {
            let mut g = self.progress.lock().unwrap();
            g.tool_calls = g.tool_calls.saturating_add(1);
            g.activity = format_tool_line(name, detail);
            g.last_event_at = Instant::now();
        }
        self.bump.notify_one();
    }

    fn snapshot(&self) -> (String, u32, Duration, Duration) {
        let g = self.progress.lock().unwrap();
        (
            g.activity.clone(),
            g.tool_calls,
            self.start.elapsed(),
            g.last_event_at.elapsed(),
        )
    }
}

pub(crate) trait AgentStatusRecorder: Send + Sync {
    fn record_agent_status(
        &self,
        target: &WorkspaceHandle,
        status: &AgentStatus,
    ) -> Option<StatusSnapshot>;
}

pub(crate) trait SubAgentCallbackRegistry: Send + Sync {
    fn subagent_button_data(&self, target: &WorkspaceHandle, link: &SubAgentLinkRecord) -> String;
}

pub(crate) trait AgentInterventionCallbackRegistry: Send + Sync {
    fn intervention_button_data(
        &self,
        target: &WorkspaceHandle,
        live_instance: &InstanceId,
        req_id: &str,
        action: IntvAction,
    ) -> String;
}

pub(crate) trait TurnEventRecorder: Send + Sync {
    fn append_turn_timeline(
        &self,
        target: &WorkspaceHandle,
        turn_id: &ControlTurnId,
        kind: TimelineItemKind,
        payload: serde_json::Value,
    ) -> Result<TimelineItem, String>;

    fn record_subagent_tool_call(
        &self,
        target: &WorkspaceHandle,
        turn_id: &ControlTurnId,
        provider_item_id: Option<&str>,
        input: &serde_json::Value,
    );

    fn record_turn_usage(&self, turn_id: &ControlTurnId, usage: lucarne::agent_runtime::UsageEvent);

    fn mark_turn_waiting_permission(&self, turn_id: &ControlTurnId);
}

#[derive(Clone)]
pub(crate) struct TurnRecording {
    pub(crate) turn_id: ControlTurnId,
    pub(crate) recorder: Arc<dyn TurnEventRecorder>,
}

impl std::fmt::Debug for TurnRecording {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnRecording")
            .field("turn_id", &self.turn_id)
            .field("recorder", &true)
            .finish()
    }
}

#[derive(Clone, Default)]
pub(crate) struct TurnRunOptions {
    pub(crate) recording: Option<TurnRecording>,
    pub(crate) intervention_callback_registry: Option<Arc<dyn AgentInterventionCallbackRegistry>>,
    pub(crate) final_footer: Option<AgentMessageFooter>,
}

impl std::fmt::Debug for TurnRunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnRunOptions")
            .field("recording", &self.recording.is_some())
            .field(
                "intervention_callback_registry",
                &self.intervention_callback_registry.is_some(),
            )
            .field("final_footer", &self.final_footer.is_some())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct CommandRunOptions {
    pub(crate) provider_id: Option<&'static str>,
    pub(crate) status_snapshot: Option<StatusSnapshot>,
    pub(crate) status_resource: Option<AgentResourceEntry>,
    pub(crate) completion_policy: CommandCompletionPolicy,
    pub(crate) intervention_callback_registry: Option<Arc<dyn AgentInterventionCallbackRegistry>>,
    pub(crate) status_recorder: Option<Arc<dyn AgentStatusRecorder>>,
    pub(crate) recording: Option<TurnRecording>,
}

impl Default for CommandRunOptions {
    fn default() -> Self {
        Self {
            provider_id: None,
            status_snapshot: None,
            status_resource: None,
            completion_policy: CommandCompletionPolicy::TurnCompleted,
            intervention_callback_registry: None,
            status_recorder: None,
            recording: None,
        }
    }
}

impl std::fmt::Debug for CommandRunOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandRunOptions")
            .field("provider_id", &self.provider_id)
            .field("status_snapshot", &self.status_snapshot)
            .field("status_resource", &self.status_resource)
            .field("completion_policy", &self.completion_policy)
            .field(
                "intervention_callback_registry",
                &self.intervention_callback_registry.is_some(),
            )
            .field("status_recorder", &self.status_recorder.is_some())
            .field("recording", &self.recording.is_some())
            .finish()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum CommandDrainOutcome {
    CommandResult,
    TurnCompleted,
    ProviderIdle,
    NoOutputAck,
}

impl CommandDrainOutcome {
    pub(crate) fn completion_policy(self) -> CommandCompletionPolicy {
        match self {
            CommandDrainOutcome::CommandResult => CommandCompletionPolicy::CommandResult,
            CommandDrainOutcome::TurnCompleted => CommandCompletionPolicy::TurnCompleted,
            CommandDrainOutcome::ProviderIdle => CommandCompletionPolicy::ProviderIdle,
            CommandDrainOutcome::NoOutputAck => CommandCompletionPolicy::NoOutputAck,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CommandDrainReport {
    pub(crate) outcome: CommandDrainOutcome,
    pub(crate) command_result: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TurnRunReport {
    pub(crate) message_ids: Vec<MessageId>,
}

#[derive(Clone, Debug)]
enum DrainMode {
    Turn(TurnRunOptions),
    Command(CommandRunOptions),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DrainOutcome {
    CommandResult,
    TurnCompleted,
    CommandProviderIdle,
}

#[derive(Debug)]
struct PendingAttachmentDelivery {
    attachment: AgentAttachment,
    reply_to: Option<MessageId>,
    notification: NotificationPolicy,
}

#[derive(Debug)]
struct DrainReport {
    outcome: DrainOutcome,
    command_result: Option<serde_json::Value>,
    message_ids: Vec<MessageId>,
    attachments: Vec<PendingAttachmentDelivery>,
}

impl DrainReport {
    fn new(outcome: DrainOutcome, command_result: Option<serde_json::Value>) -> Self {
        Self {
            outcome,
            command_result,
            message_ids: Vec::new(),
            attachments: Vec::new(),
        }
    }

    fn with_attachments(mut self, attachments: Vec<PendingAttachmentDelivery>) -> Self {
        self.attachments = attachments;
        self
    }
}

#[derive(Debug)]
struct SubmittedRunReport {
    drain: DrainReport,
    message_ids: Vec<MessageId>,
}

impl DrainMode {
    fn is_command(&self) -> bool {
        matches!(self, Self::Command(_))
    }

    fn command_options(&self) -> Option<&CommandRunOptions> {
        match self {
            Self::Turn(_) => None,
            Self::Command(options) => Some(options),
        }
    }

    fn recording(&self) -> Option<&TurnRecording> {
        match self {
            Self::Turn(options) => options.recording.as_ref(),
            Self::Command(options) => options.recording.as_ref(),
        }
    }

    fn intervention_callback_registry(
        &self,
    ) -> Option<&Arc<dyn AgentInterventionCallbackRegistry>> {
        match self {
            Self::Turn(options) => options.intervention_callback_registry.as_ref(),
            Self::Command(options) => options.intervention_callback_registry.as_ref(),
        }
    }

    fn completion_policy(&self) -> Option<CommandCompletionPolicy> {
        self.command_options()
            .map(|options| options.completion_policy)
    }

    fn final_footer(&self) -> Option<&AgentMessageFooter> {
        match self {
            Self::Turn(options) => options.final_footer.as_ref(),
            Self::Command(_) => None,
        }
    }
}

fn log_event_text(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "(empty)".into();
    }

    let mut out = String::new();
    for (idx, ch) in trimmed.chars().enumerate() {
        if idx >= max_chars {
            out.push('…');
            break;
        }
        match ch {
            '\n' | '\r' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

fn log_event_json(value: &serde_json::Value) -> String {
    log_event_text(&value.to_string(), EVENT_LOG_TEXT_MAX)
}

pub(crate) async fn run_turn_with_options(
    channel: &Arc<dyn Channel>,
    target: &WorkspaceHandle,
    live: &LiveSession,
    input: AgentInput,
    provider_id: &str,
    options: TurnRunOptions,
    reply_to: Option<MessageId>,
) -> Result<TurnRunReport, String> {
    // 0. Drop any leftover events from a prior turn. With
    //    `Event::TurnCompleted` now being the authoritative end-of-turn
    //    signal, this should almost always be empty in practice — if
    //    it isn't, we log loudly so the bug is visible instead of
    //    silently swallowed. We do NOT call `interrupt()` here any
    //    more: that would SIGTERM a perfectly healthy provider process
    //    if we raced a slow background completion.
    let stale = drain_stale_events(live).await;
    if stale > 0 {
        warn!(
            target: "lucarne_telegram::turn",
            count = stale,
            "discarded stale events from previous turn (inspect logs above for event kinds)"
        );
    }

    // 1. Submit the prompt.
    live.session
        .submit_turn(input)
        .await
        .map_err(|e| e.to_string())?;
    debug!("prompt submitted");

    drain_submitted_events(
        channel,
        target,
        live,
        provider_id,
        DrainMode::Turn(options),
        reply_to,
    )
    .await
    .map(|report| TurnRunReport {
        message_ids: report.message_ids,
    })
}

/// Invoke an agent slash command and drain the resulting event stream through
/// the same Telegram UX used by normal user turns.
#[instrument(
    name = "run_command",
    skip(channel, live, command),
    fields(
        provider = provider_id,
        command = %command.name,
        args = command.args.as_deref().unwrap_or(""),
        workspace = %target.workspace.as_str(),
    ),
)]
pub(crate) async fn run_command(
    channel: &Arc<dyn Channel>,
    target: &WorkspaceHandle,
    live: &LiveSession,
    command: AgentCommandInvocation,
    provider_id: &str,
    options: CommandRunOptions,
    reply_to: Option<MessageId>,
) -> Result<CommandDrainReport, String> {
    let stale = drain_stale_events(live).await;
    if stale > 0 {
        warn!(
            target: "lucarne_telegram::turn",
            count = stale,
            "discarded stale events from previous turn before command invocation"
        );
    }

    let immediate_result = live
        .session
        .run_command(command)
        .await
        .map_err(|e| e.to_string())?;
    debug!("command submitted");

    if options.completion_policy == CommandCompletionPolicy::NoOutputAck {
        let payload = serde_json::to_value(&immediate_result)
            .map_err(|err| format!("failed to serialize command acknowledgement: {err}"))?;
        let mode = DrainMode::Command(options);
        let item = record_timeline(
            &mode,
            target,
            TimelineItemKind::CommandResult,
            payload.clone(),
        )?;
        let projected_result = immediate_command_result_from_timeline(&item)?;
        let message = maybe_reply_to(
            OutgoingMessage::plain("✓ 完成 · 0s · 0 steps").silent(),
            reply_to.as_ref(),
        );
        channel
            .send(target, message)
            .await
            .map_err(|err| err.to_string())?;
        return Ok(CommandDrainReport {
            outcome: CommandDrainOutcome::NoOutputAck,
            command_result: Some(
                serde_json::to_value(&projected_result).map_err(|err| {
                    format!("failed to serialize projected command result: {err}")
                })?,
            ),
        });
    }

    if options.completion_policy == CommandCompletionPolicy::CommandResult
        && !matches!(immediate_result.data, AgentCommandResultData::Empty)
    {
        let payload = serde_json::to_value(&immediate_result)
            .map_err(|err| format!("failed to serialize command result: {err}"))?;
        let mode = DrainMode::Command(options);
        let item = record_timeline(
            &mode,
            target,
            TimelineItemKind::CommandResult,
            payload.clone(),
        )?;
        let projected_result = immediate_command_result_from_timeline(&item)?;
        let status_snapshot =
            record_immediate_status_snapshot(target, &projected_result, mode.command_options());
        if let Some(message) = render_immediate_command_result(
            target,
            &projected_result,
            mode.command_options(),
            status_snapshot.as_ref(),
        ) {
            let message = maybe_reply_to(message, reply_to.as_ref());
            channel
                .send(target, message)
                .await
                .map_err(|err| err.to_string())?;
        }
        return Ok(CommandDrainReport {
            outcome: CommandDrainOutcome::CommandResult,
            command_result: Some(
                serde_json::to_value(&projected_result).map_err(|err| {
                    format!("failed to serialize projected command result: {err}")
                })?,
            ),
        });
    }

    let report = drain_submitted_events(
        channel,
        target,
        live,
        provider_id,
        DrainMode::Command(options),
        reply_to,
    )
    .await?;
    Ok(CommandDrainReport {
        outcome: match report.drain.outcome {
            DrainOutcome::CommandResult => CommandDrainOutcome::CommandResult,
            DrainOutcome::TurnCompleted => CommandDrainOutcome::TurnCompleted,
            DrainOutcome::CommandProviderIdle => CommandDrainOutcome::ProviderIdle,
        },
        command_result: report.drain.command_result,
    })
}

async fn drain_submitted_events(
    channel: &Arc<dyn Channel>,
    target: &WorkspaceHandle,
    live: &LiveSession,
    provider_id: &str,
    mode: DrainMode,
    reply_to: Option<MessageId>,
) -> Result<SubmittedRunReport, String> {
    // 2. Send the initial status message (plain text: cheapest + no
    //    markdown parse risk on every edit).
    let shared = Shared::new();
    let status_id = match channel
        .send(
            target,
            OutgoingMessage::plain(render_status(&shared, false)).silent(),
        )
        .await
    {
        Ok(id) => Some(id),
        Err(e) => {
            // Non-fatal; we can still run the turn without a status
            // bubble. Just log and move on.
            warn!(error = %e, "failed to send status message; continuing without live progress");
            None
        }
    };

    // 3. Kick off ticker task that edits the status message.
    let ticker_handle = status_id.as_ref().map(|id| {
        let channel = channel.clone();
        let target = target.clone();
        let id = id.clone();
        let shared = shared.clone();
        tokio::spawn(status_ticker(channel, target, id, shared))
    });

    // 4. Drain events through the streaming draft pipeline.
    let mut drafts = DraftStream::with_reply_to(reply_to);
    let final_footer_template = mode.final_footer().cloned();
    let drain_result = drain_events(
        channel,
        target,
        live,
        provider_id,
        &shared,
        &mut drafts,
        mode,
    )
    .await;
    shared.done.store(true, Ordering::SeqCst);
    shared.bump.notify_one();
    if let Some(h) = ticker_handle {
        let _ = h.await;
    }

    let elapsed = shared.start.elapsed();

    match drain_result {
        Ok(mut outcome) => {
            let final_footer = final_footer_template.map(|mut footer| {
                if footer.cost.is_none() {
                    footer.cost = Some(format_cost_duration(elapsed));
                }
                footer
            });
            let finalized = drafts
                .finalize(&**channel, target, provider_id, final_footer.as_ref())
                .await;
            let mut message_ids = std::mem::take(&mut outcome.message_ids);
            message_ids.extend(finalized.message_ids);
            if let Some(id) = status_id.as_ref() {
                let _ = channel
                    .edit(
                        target,
                        id,
                        OutgoingMessage::plain(format!(
                            "✓ 完成 · {} · {} steps",
                            format_elapsed(elapsed),
                            shared.snapshot().1,
                        )),
                    )
                    .await;
                message_ids.push(id.clone());
            }
            if finalized.bytes == 0 {
                // Edge case: drain ended OK but there was no committed
                // message content (e.g. intervention branch). Nothing
                // more to send — status already shows "✓ 完成".
            }
            let mut attachment_ids = deliver_pending_attachments(
                &**channel,
                target,
                provider_id,
                std::mem::take(&mut outcome.attachments),
            )
            .await;
            message_ids.append(&mut attachment_ids);
            Ok(SubmittedRunReport {
                drain: outcome,
                message_ids,
            })
        }
        Err(e) => {
            // Clean up any in-flight drafts first, then interrupt so
            // the next prompt doesn't race with a still-running turn.
            let _ = drafts.finalize(&**channel, target, provider_id, None).await;
            if let Err(ie) = live.session.interrupt_turn().await {
                warn!(error = %ie, "interrupt after failed turn errored");
            }
            if let Some(id) = status_id.as_ref() {
                let _ = channel
                    .edit(
                        target,
                        id,
                        OutgoingMessage::plain(format!(
                            "⚠ 失败 · {} · {e}",
                            format_elapsed(elapsed)
                        )),
                    )
                    .await;
            } else {
                let msg = OutgoingMessage::markdown(format!("⚠ {e}"));
                let _ = send_with_fallback(&**channel, target, msg, provider_id).await;
            }
            Err(e)
        }
    }
}

/// Pull any events left over from a previous turn and drop them. Uses
/// `try_recv` so it never blocks. Returns how many events were
/// discarded (purely for logging).
async fn drain_stale_events(live: &LiveSession) -> usize {
    let mut events = live.events.lock().await;
    let mut n = 0usize;
    loop {
        match events.try_recv() {
            Ok(ev) => {
                warn!(
                    target: "lucarne_telegram::turn",
                    stale_kind = event_kind_name(&ev),
                    "leaking stale event from previous turn"
                );
                n += 1;
            }
            Err(CoreWorkspaceEventTryRecvError::Empty | CoreWorkspaceEventTryRecvError::Closed) => {
                break;
            }
            Err(CoreWorkspaceEventTryRecvError::Lagged(skipped)) => {
                warn!(
                    target: "lucarne_telegram::turn",
                    skipped,
                    "workspace event stream lagged while draining stale events"
                );
                break;
            }
        }
    }
    n
}

fn event_kind_name(e: &Event) -> &'static str {
    match e {
        Event::Message(MessageEvent {
            role: MessageRole::User,
            ..
        }) => "user_message",
        Event::Message(_) => "assistant_message",
        Event::Attachment(_) => "attachment",
        Event::Reasoning(_) => "reasoning",
        Event::ToolCall(_) => "tool_call",
        Event::ToolResult(_) => "tool_result",
        Event::Usage(_) => "usage",
        Event::CommandResult(_) => "command_result",
        Event::InterventionRequest(_) => "intervention_request",
        Event::TurnCompleted(_) => "turn_completed",
        Event::TurnFailed(_) => "turn_failed",
    }
}

async fn status_ticker(
    channel: Arc<dyn Channel>,
    target: WorkspaceHandle,
    id: MessageId,
    shared: Arc<Shared>,
) {
    let mut last_render: Option<String> = None;
    loop {
        if shared.done.load(Ordering::SeqCst) {
            return;
        }
        // Wait for either a progress bump or the edit interval.
        tokio::select! {
            _ = shared.bump.notified() => {}
            _ = tokio::time::sleep(STATUS_EDIT_INTERVAL) => {}
        }
        if shared.done.load(Ordering::SeqCst) {
            return;
        }
        let body = render_status(&shared, true);
        if last_render.as_deref() == Some(body.as_str()) {
            continue;
        }
        match channel
            .edit(&target, &id, OutgoingMessage::plain(body.clone()))
            .await
        {
            Ok(()) => {
                last_render = Some(body);
            }
            Err(e) => {
                // Telegram returns an error when content is identical —
                // shouldn't happen here since we diffed, but ignore any
                // transient failure; we'll retry next tick.
                debug!(error = %e, "status edit failed");
            }
        }
    }
}

fn render_status(shared: &Shared, include_timer: bool) -> String {
    let (activity, tools, elapsed, quiet) = shared.snapshot();
    let mut header = if include_timer {
        format!("⏳ Processing · {}", format_elapsed(elapsed))
    } else {
        "⏳ Processing · 0s".to_string()
    };
    if tools > 0 {
        header.push_str(&format!(" · {tools} steps"));
    }
    if include_timer && quiet >= QUIET_HEARTBEAT {
        header.push_str(&format!(
            " · waiting for agent ({} idle)",
            format_elapsed(quiet)
        ));
    }
    if activity.is_empty() {
        header
    } else {
        format!("{header}\n{activity}")
    }
}

pub(crate) fn render_immediate_command_timeline_item(
    target: &WorkspaceHandle,
    item: &TimelineItem,
    options: Option<&CommandRunOptions>,
) -> Result<Option<OutgoingMessage>, String> {
    let projected_result = immediate_command_result_from_timeline(item)?;
    let status_snapshot = record_immediate_status_snapshot(target, &projected_result, options);
    Ok(render_immediate_command_result(
        target,
        &projected_result,
        options,
        status_snapshot.as_ref(),
    ))
}

fn record_event_status_snapshot(
    target: &WorkspaceHandle,
    event: &CommandResultEvent,
    options: Option<&CommandRunOptions>,
) -> Option<StatusSnapshot> {
    match &event.result.result {
        CommandResultData::Status(status) => {
            record_command_status_snapshot(target, status, options)
        }
        _ => None,
    }
}

fn record_immediate_status_snapshot(
    target: &WorkspaceHandle,
    result: &AgentCommandResult,
    options: Option<&CommandRunOptions>,
) -> Option<StatusSnapshot> {
    match &result.data {
        AgentCommandResultData::Status(status) => {
            record_command_status_snapshot(target, status, options)
        }
        _ => None,
    }
}

fn record_command_status_snapshot(
    target: &WorkspaceHandle,
    status: &AgentStatus,
    options: Option<&CommandRunOptions>,
) -> Option<StatusSnapshot> {
    options
        .and_then(|options| options.status_recorder.as_ref())
        .and_then(|recorder| recorder.record_agent_status(target, status))
        .or_else(|| options.and_then(|options| options.status_snapshot.clone()))
}

fn record_timeline(
    mode: &DrainMode,
    target: &WorkspaceHandle,
    kind: TimelineItemKind,
    payload: serde_json::Value,
) -> Result<TimelineItem, String> {
    if let Some(recording) = mode.recording() {
        return recording
            .recorder
            .append_turn_timeline(target, &recording.turn_id, kind, payload);
    }
    let _ = (target, kind, payload);
    Err("timeline recording is required before Telegram projection rendering".into())
}

fn render_attachment_delivery_failure(attachment: &AgentAttachment, error: &str) -> String {
    let error = error.trim();
    let error = if error.is_empty() {
        "Unknown attachment delivery error"
    } else {
        error
    };
    format!(
        "Attachment delivery failed.\n\nFile: {}\nMedia type: {}\nAttachment id: {}\nError: {}",
        attachment.filename, attachment.media_type, attachment.id, error
    )
}

async fn send_attachment_delivery_failure(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    provider_id: &str,
    attachment: &AgentAttachment,
    reply_to: Option<MessageId>,
    notification: NotificationPolicy,
    error: &str,
) -> Option<MessageId> {
    let mut msg = OutgoingMessage::plain(render_attachment_delivery_failure(attachment, error))
        .with_notification(notification);
    if let Some(reply_to) = reply_to {
        msg = msg.reply_to(reply_to);
    }
    match send_with_fallback(channel, target, msg, provider_id).await {
        Ok(id) => Some(id),
        Err(err) => {
            warn!(
                target: "lucarne_telegram::turn",
                attachment_id = %attachment.id,
                error = %err,
                "attachment failure details send failed"
            );
            None
        }
    }
}

async fn send_channel_attachment_with_retries(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    attachment: ChannelAttachment,
) -> Result<MessageId, ChannelError> {
    retry_attachment_delivery(
        || channel.send_attachment(target, attachment.clone()),
        |_| true,
    )
    .await
}

fn split_telegram_attachment_caption(caption: &str) -> (String, Option<String>) {
    match caption
        .char_indices()
        .nth(TELEGRAM_ATTACHMENT_CAPTION_CHARS)
        .map(|(idx, _)| idx)
    {
        Some(split_at) => (
            caption[..split_at].to_string(),
            Some(caption[split_at..].to_string()),
        ),
        None => (caption.to_string(), None),
    }
}

async fn send_attachment_caption_overflow(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    provider_id: &str,
    body: String,
    reply_to: MessageId,
    notification: NotificationPolicy,
) -> Vec<MessageId> {
    let msg = OutgoingMessage::plain(body)
        .reply_to(reply_to)
        .with_notification(notification);
    match send_with_fallback_all(channel, target, msg, provider_id).await {
        Ok(ids) => ids,
        Err(err) => {
            warn!(
                target: "lucarne_telegram::turn",
                error = %err,
                "attachment caption overflow send failed"
            );
            Vec::new()
        }
    }
}

async fn deliver_pending_attachments(
    channel: &dyn Channel,
    target: &WorkspaceHandle,
    provider_id: &str,
    attachments: Vec<PendingAttachmentDelivery>,
) -> Vec<MessageId> {
    let mut message_ids = Vec::new();
    for pending in attachments {
        let attachment = pending.attachment;
        let reply_to = pending.reply_to;
        let notification = pending.notification;
        let (channel_attachment, caption_overflow) =
            match channel_attachment_from_event(&attachment, reply_to.clone(), notification) {
                Ok(channel_attachment) => channel_attachment,
                Err(err) => {
                    warn!(
                        target: "lucarne_telegram::turn",
                        attachment_id = %attachment.id,
                        error = %err,
                        "attachment conversion failed; sending failure details"
                    );
                    if let Some(id) = send_attachment_delivery_failure(
                        channel,
                        target,
                        provider_id,
                        &attachment,
                        reply_to,
                        notification,
                        &err,
                    )
                    .await
                    {
                        message_ids.push(id);
                    }
                    continue;
                }
            };
        match send_channel_attachment_with_retries(channel, target, channel_attachment).await {
            Ok(id) => {
                message_ids.push(id.clone());
                if let Some(overflow) = caption_overflow {
                    message_ids.extend(
                        send_attachment_caption_overflow(
                            channel,
                            target,
                            provider_id,
                            overflow,
                            id,
                            notification,
                        )
                        .await,
                    );
                }
            }
            Err(err) => {
                warn!(
                    target: "lucarne_telegram::turn",
                    attachment_id = %attachment.id,
                    error = %err,
                    "attachment send failed after bounded retries; sending failure details"
                );
                let failure_error = match &err {
                    ChannelError::Transport(message) => message.to_string(),
                    _ => err.to_string(),
                };
                if let Some(id) = send_attachment_delivery_failure(
                    channel,
                    target,
                    provider_id,
                    &attachment,
                    reply_to,
                    notification,
                    &failure_error,
                )
                .await
                {
                    message_ids.push(id);
                }
            }
        }
    }
    message_ids
}

fn channel_attachment_from_event(
    attachment: &AgentAttachment,
    reply_to: Option<MessageId>,
    notification: NotificationPolicy,
) -> Result<(ChannelAttachment, Option<String>), String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(attachment.data_base64.as_bytes())
        .map_err(|err| format!("attachment {} has invalid base64: {err}", attachment.id))?;
    if bytes.len() > MAX_ATTACHMENT_BYTES {
        return Err(format!(
            "attachment {} is {} bytes, above {} byte limit",
            attachment.id,
            bytes.len(),
            MAX_ATTACHMENT_BYTES
        ));
    }
    let mut channel_attachment = ChannelAttachment::new(
        attachment.filename.to_string(),
        attachment.media_type.to_string(),
        bytes,
    )
    .with_notification(notification);
    let mut caption_overflow = None;
    if let Some(caption) = attachment.caption.as_ref() {
        let (caption, overflow) = split_telegram_attachment_caption(caption.as_str());
        channel_attachment = channel_attachment.with_caption(caption);
        caption_overflow = overflow;
    }
    if let Some(reply_to) = reply_to {
        channel_attachment = channel_attachment.reply_to(reply_to);
    }
    Ok((channel_attachment, caption_overflow))
}

fn immediate_command_result_from_timeline(
    item: &TimelineItem,
) -> Result<AgentCommandResult, String> {
    if item.kind != TimelineItemKind::CommandResult {
        return Err(format!(
            "expected command_result timeline item, got {:?}",
            item.kind
        ));
    }
    serde_json::from_value::<AgentCommandResult>(item.payload.clone())
        .map_err(|err| format!("immediate command timeline projection is invalid: {err}"))
}

fn record_subagent_tool_call(
    mode: &DrainMode,
    target: &WorkspaceHandle,
    provider_item_id: &str,
    input: &serde_json::Value,
) {
    if let Some(recording) = mode.recording() {
        recording.recorder.record_subagent_tool_call(
            target,
            &recording.turn_id,
            Some(provider_item_id),
            input,
        );
    }
}

fn mark_turn_waiting_permission(mode: &DrainMode) {
    if let Some(recording) = mode.recording() {
        recording
            .recorder
            .mark_turn_waiting_permission(&recording.turn_id);
    }
}

async fn drain_events(
    channel: &Arc<dyn Channel>,
    target: &WorkspaceHandle,
    live: &LiveSession,
    _provider_id: &str,
    shared: &Shared,
    drafts: &mut DraftStream,
    mode: DrainMode,
) -> Result<DrainReport, String> {
    let mut events = live.events.lock().await;
    // `has_message` is a soft latch used purely for UX heuristics
    // (status strings). For normal turns, completion is not inferred
    // from idle — the provider emits an explicit `Event::TurnCompleted`
    // (projected from `Payload::TurnCompleted`) as the authoritative
    // end-of-turn signal. This fixes a class of bugs
    // where providers pause >10s between assistant messages
    // during long reasoning runs, previously causing the turn to
    // finalize prematurely and the agent's continuation events to be
    // dropped on the next prompt.
    let mut has_message = false;
    let mut command_result_payload = None;
    let mut pending_attachments = Vec::new();
    let mut awaiting_intervention = false;
    let mut suppress_next_command_message = false;
    loop {
        // Turn-level timeouts (inactivity / deadline) are enforced by
        // the core service watchdog, which emits TurnFailed on the
        // same event stream. The drain loop itself does not impose an
        // independent timeout.
        let wait = if awaiting_intervention {
            None
        } else if mode.completion_policy() == Some(CommandCompletionPolicy::ProviderIdle)
            && has_message
        {
            Some(PROVIDER_IDLE_QUIET)
        } else {
            None
        };
        let recv_result: Result<Result<Event, CoreWorkspaceEventRecvError>, String> = async {
            match wait {
                Some(timeout) => match tokio::time::timeout(timeout, events.recv()).await {
                    Ok(result) => Ok(result),
                    Err(_elapsed) => Err("provider idle timeout".to_string()),
                },
                None => Ok(events.recv().await),
            }
        }
        .await;
        match recv_result {
            Ok(Ok(Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text,
                streaming,
            }))) => {
                if suppress_next_command_message && !streaming {
                    suppress_next_command_message = false;
                    shared.mark_event();
                    continue;
                }
                info!(
                    target: "lucarne_telegram::turn",
                    event = "message",
                    role = "assistant",
                    bytes = text.len(),
                    chars = text.chars().count(),
                    agent_return = %log_event_text(&text, EVENT_LOG_TEXT_MAX),
                    "assistant message received"
                );
                let item = record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::Assistant,
                    serde_json::json!({ "text": text.as_str(), "streaming": streaming }),
                )?;
                awaiting_intervention = false;
                shared.mark_event();
                has_message = true;
                render_timeline_item_to_draft(
                    channel,
                    target,
                    live.session.instance_id(),
                    drafts,
                    &mode,
                    &item,
                    None,
                )
                .await?;
            }
            Ok(Ok(Event::Attachment(attachment))) => {
                info!(
                    target: "lucarne_telegram::turn",
                    event = "attachment",
                    id = %attachment.id,
                    filename = %attachment.filename,
                    media_type = %attachment.media_type,
                    bytes_base64 = attachment.data_base64.len(),
                    "attachment received"
                );
                let item = record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::Attachment,
                    serde_json::to_value(&attachment).expect("attachment event must serialize"),
                )?;
                awaiting_intervention = false;
                shared.set_activity(format!(
                    "📎 {} ({})",
                    attachment.filename, attachment.media_type
                ));
                has_message = true;
                pending_attachments.push(PendingAttachmentDelivery {
                    attachment,
                    reply_to: drafts.reply_to.clone(),
                    notification: NotificationPolicy::Notify,
                });
                debug!(
                    target: "lucarne_telegram::turn",
                    timeline_seq = item.seq.get(),
                    "attachment recorded and queued for post-final delivery"
                );
            }
            Ok(Ok(Event::CommandResult(result))) => {
                info!(
                    target: "lucarne_telegram::turn",
                    event = "command_result",
                    command = %result.command,
                    "command result received"
                );
                let raw_payload = serde_json::to_value(&result.result)
                    .map_err(|err| format!("failed to serialize command result: {err}"))?;
                let item =
                    record_timeline(&mode, target, TimelineItemKind::CommandResult, raw_payload)?;
                let status_snapshot =
                    record_event_status_snapshot(target, &result, mode.command_options());
                awaiting_intervention = false;
                shared.set_activity("📋 正在渲染列表");
                has_message = true;
                if let TimelineRenderOutcome::CommandResult { payload } =
                    render_timeline_item_to_draft(
                        channel,
                        target,
                        live.session.instance_id(),
                        drafts,
                        &mode,
                        &item,
                        status_snapshot.as_ref(),
                    )
                    .await?
                {
                    if mode.is_command() {
                        command_result_payload = Some(payload);
                    }
                    suppress_next_command_message = true;
                }
                if mode.completion_policy() == Some(CommandCompletionPolicy::CommandResult) {
                    return Ok(DrainReport::new(
                        DrainOutcome::CommandResult,
                        command_result_payload,
                    )
                    .with_attachments(pending_attachments));
                }
            }
            Ok(Ok(Event::Message(MessageEvent {
                role: MessageRole::User,
                text,
                ..
            }))) => {
                debug!(
                    target: "lucarne_telegram::turn",
                    event = "message", role = "user", bytes = text.len(),
                    chars = text.chars().count(),
                    user_message = %log_event_text(&text, EVENT_LOG_TEXT_MAX),
                    "user message echoed"
                );
                record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::User,
                    serde_json::json!({ "text": text.as_str() }),
                )?;
                shared.mark_event();
            }
            Ok(Ok(Event::ToolCall(tc))) => {
                info!(
                    target: "lucarne_telegram::turn",
                    event = "tool_call",
                    tool = %tc.name,
                    input = %log_event_json(&tc.input),
                    "tool call received"
                );
                let item = record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::ToolCall,
                    serde_json::json!({
                        "call_id": tc.call_id.0.as_str(),
                        "name": tc.name.as_str(),
                        "input": tc.input.clone(),
                    }),
                )?;
                awaiting_intervention = false;
                let tool_name = item
                    .payload
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| "tool_call timeline projection missing name".to_string())?;
                let tool_input = item
                    .payload
                    .get("input")
                    .ok_or_else(|| "tool_call timeline projection missing input".to_string())?;
                let detail = tool_call_detail(tool_input);
                shared.bump_tool(tool_name, &detail);
                if tool_name == "sub_agent" {
                    let call_id = item
                        .payload
                        .get("call_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(|| {
                            "sub_agent timeline projection missing call_id".to_string()
                        })?;
                    record_subagent_tool_call(&mode, target, call_id, tool_input);
                }
            }
            Ok(Ok(Event::Reasoning(r))) => {
                debug!(
                    target: "lucarne_telegram::turn",
                    event = "reasoning", bytes = r.text.len(),
                    chars = r.text.chars().count(),
                    reasoning = %log_event_text(&r.text, EVENT_LOG_TEXT_MAX),
                    "reasoning received"
                );
                let item = record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::Reasoning,
                    serde_json::json!({ "text": r.text.as_str() }),
                )?;
                awaiting_intervention = false;
                shared.mark_event();
                render_timeline_item_to_draft(
                    channel,
                    target,
                    live.session.instance_id(),
                    drafts,
                    &mode,
                    &item,
                    None,
                )
                .await?;
            }
            Ok(Ok(Event::ToolResult(tr))) => {
                debug!(
                    target: "lucarne_telegram::turn",
                    event = "tool_result",
                    is_error = ?tr.is_error,
                    output = %log_event_json(&tr.output),
                    "tool result received"
                );
                record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::ToolResult,
                    serde_json::json!({
                        "call_id": tr.call_id.0.as_str(),
                        "output": tr.output.clone(),
                        "is_error": tr.is_error,
                    }),
                )?;
                awaiting_intervention = false;
                shared.mark_event();
            }
            Ok(Ok(Event::Usage(u))) => {
                debug!(
                    target: "lucarne_telegram::turn",
                    event = "usage",
                    input = ?u.input_tokens, output = ?u.output_tokens,
                    "public event"
                );
                record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::Usage,
                    serde_json::to_value(&u).expect("usage event must serialize"),
                )?;
                shared.mark_event();
            }
            Ok(Ok(Event::TurnCompleted(tc))) => {
                info!(
                    target: "lucarne_telegram::turn",
                    event = "turn_completed",
                    turn_id = %tc.turn_id,
                    has_message,
                    "provider signalled turn complete; ending drain"
                );
                if let Some(usage) = tc.usage.clone() {
                    record_timeline(
                        &mode,
                        target,
                        TimelineItemKind::Usage,
                        serde_json::to_value(&usage).expect("turn completion usage must serialize"),
                    )?;
                    if let Some(recording) = mode.recording() {
                        recording
                            .recorder
                            .record_turn_usage(&recording.turn_id, usage);
                    }
                }
                let outcome =
                    if mode.completion_policy() == Some(CommandCompletionPolicy::ProviderIdle) {
                        DrainOutcome::CommandProviderIdle
                    } else {
                        DrainOutcome::TurnCompleted
                    };
                return Ok(DrainReport::new(outcome, command_result_payload)
                    .with_attachments(pending_attachments));
            }
            Ok(Ok(Event::TurnFailed(tf))) => {
                warn!(
                    target: "lucarne_telegram::turn",
                    event = "turn_failed",
                    turn_id = %tf.turn_id,
                    code = %tf.code,
                    error = %tf.error,
                    "provider signalled turn failure"
                );
                return Err(if tf.error.is_empty() {
                    format!("agent turn failed ({})", tf.code)
                } else {
                    tf.error.to_string()
                });
            }
            Ok(Ok(Event::InterventionRequest(req))) => {
                info!(
                    target: "lucarne_telegram::turn",
                    event = "intervention_request",
                    "asking user"
                );
                let item = record_timeline(
                    &mode,
                    target,
                    TimelineItemKind::Permission,
                    serde_json::to_value(&req).expect("intervention request must serialize"),
                )?;
                mark_turn_waiting_permission(&mode);
                shared.set_activity("🙋 等待用户决定…");
                awaiting_intervention = true;
                if let TimelineRenderOutcome::Intervention { req_id, message_id } =
                    render_timeline_item_to_draft(
                        channel,
                        target,
                        live.session.instance_id(),
                        drafts,
                        &mode,
                        &item,
                        None,
                    )
                    .await?
                {
                    live.pending_intv.lock().unwrap().insert(req_id, message_id);
                }
            }
            Ok(Err(CoreWorkspaceEventRecvError::Closed)) => {
                warn!("event stream closed mid-turn");
                let detail = live
                    .session
                    .observed_close_reason()
                    .await
                    .map(|reason| format!(": {reason}"))
                    .unwrap_or_default();
                return Err(format!("agent event stream closed mid-turn{detail}"));
            }
            Ok(Err(CoreWorkspaceEventRecvError::Lagged(skipped))) => {
                warn!(skipped, "workspace event stream lagged mid-turn");
                return Err(format!(
                    "daemon event stream lagged by {skipped} events during turn"
                ));
            }
            Err(_) => {
                // Only reached on PROVIDER_IDLE_QUIET timeout—the
                // provider-native command finished without a TurnCompleted.
                if mode.completion_policy() == Some(CommandCompletionPolicy::ProviderIdle)
                    && has_message
                {
                    return Ok(DrainReport::new(
                        DrainOutcome::CommandProviderIdle,
                        command_result_payload,
                    )
                    .with_attachments(pending_attachments));
                }
                warn!("turn timed out waiting for intervention");
                return Err("intervention timed out without user response".into());
            }
        }
    }
}

fn format_tool_line(name: &str, detail: &str) -> String {
    if detail.is_empty() {
        format!("🔧 {name}")
    } else {
        format!("🔧 {name}({detail})")
    }
}

/// Callback-data prefix used to route intervention button clicks back
/// into [`crate::bot`]'s dispatcher. Kept public so `bot.rs` can match
/// against the same constant.
pub const INTV_CALLBACK_PREFIX: &str = "intv:";

/// Parsed intervention button click.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntvCallback {
    Token { token: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntvAction {
    Approve { allow: bool },
    Answer { q_idx: usize, values: Vec<String> },
}

/// Parse a callback data string into [`IntvCallback`]. Returns `None`
/// when the payload isn't an intervention callback.
pub fn parse_intv_callback(data: &str) -> Option<IntvCallback> {
    let rest = data.strip_prefix(INTV_CALLBACK_PREFIX)?;
    rest.strip_prefix("c:")
        .filter(|token| !token.is_empty())
        .map(|token| IntvCallback::Token {
            token: token.to_string(),
        })
}

fn tool_call_detail(input: &serde_json::Value) -> String {
    // Pull out the first plausible argument: command / path / query / pattern / url.
    let keys = ["command", "cmd", "path", "file", "query", "pattern", "url"];
    for k in keys {
        if let Some(v) = input.get(k) {
            if let Some(s) = v.as_str() {
                return short_line(s, ACTIVITY_DETAIL_MAX);
            }
        }
    }
    // Fall back to the first string field we see.
    if let Some(obj) = input.as_object() {
        for (_, v) in obj.iter() {
            if let Some(s) = v.as_str() {
                return short_line(s, ACTIVITY_DETAIL_MAX);
            }
        }
    }
    String::new()
}

fn short_line(s: &str, max: usize) -> String {
    let first = s.lines().next().unwrap_or(s).trim();
    if first.chars().count() <= max {
        return first.to_string();
    }
    let prefix: String = first.chars().take(max.saturating_sub(1)).collect();
    format!("{prefix}…")
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
        AgentError, AgentEventStream, AgentSession, CallId, InterventionResponse, ReasoningEvent,
        SessionId, ToolCallEvent, ToolResultEvent,
    };
    use lucarne::core_service::{CoreEvent, CoreWorkspaceEventStream};
    use lucarne::{Question, QuestionOption, QuestionRequest};
    use lucarne_channel::{
        ChannelError, ChannelEvent, ChatId, FileUpload, IncomingAttachment, MessageId,
        OutgoingMessage, Result, WorkspaceHandle, WorkspaceId,
    };
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::{broadcast, mpsc};

    #[test]
    fn turn_progress_keeps_activity_and_heartbeat_under_one_lock() {
        let source = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/turn.rs"),
        )
        .expect("turn source must be readable");
        let production = source
            .split("#[cfg(test)]\nmod tests")
            .next()
            .unwrap_or(&source);

        assert!(
            !production.contains("last_event_at: Mutex<Instant>"),
            "turn progress heartbeat should share the progress lock instead of using a second mutex"
        );
    }

    #[derive(Default)]
    struct TestChannel {
        sends: StdMutex<Vec<OutgoingMessage>>,
        attachments: StdMutex<Vec<ChannelAttachment>>,
        attachment_errors: StdMutex<Vec<String>>,
        attachment_attempts: StdMutex<usize>,
        edits: StdMutex<Vec<(String, OutgoingMessage)>>,
        deletes: StdMutex<Vec<String>>,
        edit_errors: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl Channel for TestChannel {
        fn name(&self) -> &'static str {
            "test"
        }

        fn message_char_limit(&self) -> usize {
            4096
        }

        async fn send(&self, _target: &WorkspaceHandle, msg: OutgoingMessage) -> Result<MessageId> {
            let mut sends = self.sends.lock().unwrap();
            sends.push(msg);
            Ok(MessageId::new(sends.len().to_string()))
        }

        async fn edit(
            &self,
            _target: &WorkspaceHandle,
            id: &MessageId,
            msg: OutgoingMessage,
        ) -> Result<()> {
            if let Some(error) = self.edit_errors.lock().unwrap().pop() {
                return Err(ChannelError::Transport(error));
            }
            self.edits
                .lock()
                .unwrap()
                .push((id.as_str().to_string(), msg));
            Ok(())
        }

        async fn delete(&self, _target: &WorkspaceHandle, id: &MessageId) -> Result<()> {
            self.deletes.lock().unwrap().push(id.as_str().to_string());
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
            _file: FileUpload,
        ) -> Result<MessageId> {
            Err(ChannelError::Unsupported("send_file".into()))
        }

        async fn send_attachment(
            &self,
            _target: &WorkspaceHandle,
            attachment: ChannelAttachment,
        ) -> Result<MessageId> {
            *self.attachment_attempts.lock().unwrap() += 1;
            if let Some(error) = self.attachment_errors.lock().unwrap().pop() {
                return Err(ChannelError::Transport(error));
            }
            let mut attachments = self.attachments.lock().unwrap();
            attachments.push(attachment);
            Ok(MessageId::new(format!("attachment-{}", attachments.len())))
        }
    }

    fn test_target() -> WorkspaceHandle {
        WorkspaceHandle::new(ChatId::new("1"), WorkspaceId::new("2"))
    }

    fn test_event_stream(
        workspace: &WorkspaceId,
        capacity: usize,
    ) -> (broadcast::Sender<CoreEvent>, CoreWorkspaceEventStream) {
        let (tx, rx) = broadcast::channel(capacity);
        (
            tx,
            CoreWorkspaceEventStream::new(
                lucarne::control_plane::WorkspaceId::new(workspace.as_str()),
                rx,
            ),
        )
    }

    fn send_test_event(tx: &broadcast::Sender<CoreEvent>, target: &WorkspaceHandle, event: Event) {
        tx.send(CoreEvent::TimelineEvent {
            workspace_id: lucarne::control_plane::WorkspaceId::new(target.workspace.as_str()),
            turn_id: None,
            event,
        })
        .expect("send core event");
    }

    struct SilentSession {
        id: SessionId,
        instance_id: InstanceId,
    }

    #[async_trait]
    impl AgentSession for SilentSession {
        fn id(&self) -> &SessionId {
            &self.id
        }

        fn instance_id(&self) -> &InstanceId {
            &self.instance_id
        }

        fn provider_id(&self) -> ProviderId {
            ProviderId::from_static("test")
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

    #[derive(Default)]
    struct TestSubAgentCallbackRegistry {
        calls: StdMutex<Vec<String>>,
    }

    impl SubAgentCallbackRegistry for TestSubAgentCallbackRegistry {
        fn subagent_button_data(
            &self,
            _target: &WorkspaceHandle,
            link: &SubAgentLinkRecord,
        ) -> String {
            self.calls
                .lock()
                .unwrap()
                .push(link.link_id.as_str().to_string());
            "subagent:c:s1".into()
        }
    }

    #[derive(Default)]
    struct TestInterventionCallbackRegistry {
        actions: StdMutex<Vec<IntvAction>>,
    }

    impl AgentInterventionCallbackRegistry for TestInterventionCallbackRegistry {
        fn intervention_button_data(
            &self,
            _target: &WorkspaceHandle,
            _live_instance: &InstanceId,
            _req_id: &str,
            action: IntvAction,
        ) -> String {
            self.actions.lock().unwrap().push(action);
            "intv:c:i1".into()
        }
    }

    #[derive(Default)]
    struct NormalizingTimelineRecorder {
        items: StdMutex<Vec<(TimelineItemKind, serde_json::Value)>>,
    }

    impl TurnEventRecorder for NormalizingTimelineRecorder {
        fn append_turn_timeline(
            &self,
            target: &WorkspaceHandle,
            turn_id: &ControlTurnId,
            kind: TimelineItemKind,
            mut payload: serde_json::Value,
        ) -> std::result::Result<TimelineItem, String> {
            if kind == TimelineItemKind::Assistant {
                payload["text"] = serde_json::Value::String("timeline-normalized".to_string());
            }
            self.items.lock().unwrap().push((kind, payload.clone()));
            Ok(TimelineItem::new(
                lucarne::control_plane::WorkspaceId::new(target.workspace.as_str()),
                turn_id.clone(),
                kind,
                payload,
            ))
        }

        fn record_subagent_tool_call(
            &self,
            _target: &WorkspaceHandle,
            _turn_id: &ControlTurnId,
            _provider_item_id: Option<&str>,
            _input: &serde_json::Value,
        ) {
        }

        fn record_turn_usage(
            &self,
            _turn_id: &ControlTurnId,
            _usage: lucarne::agent_runtime::UsageEvent,
        ) {
        }

        fn mark_turn_waiting_permission(&self, _turn_id: &ControlTurnId) {}
    }

    struct FailingTimelineRecorder;

    impl TurnEventRecorder for FailingTimelineRecorder {
        fn append_turn_timeline(
            &self,
            _target: &WorkspaceHandle,
            _turn_id: &ControlTurnId,
            _kind: TimelineItemKind,
            _payload: serde_json::Value,
        ) -> std::result::Result<TimelineItem, String> {
            Err("timeline store unavailable".into())
        }

        fn record_subagent_tool_call(
            &self,
            _target: &WorkspaceHandle,
            _turn_id: &ControlTurnId,
            _provider_item_id: Option<&str>,
            _input: &serde_json::Value,
        ) {
        }

        fn record_turn_usage(
            &self,
            _turn_id: &ControlTurnId,
            _usage: lucarne::agent_runtime::UsageEvent,
        ) {
        }

        fn mark_turn_waiting_permission(&self, _turn_id: &ControlTurnId) {}
    }

    #[tokio::test]
    async fn drain_reports_failure_when_turn_failed_event_arrives() {
        let channel: Arc<dyn Channel> = Arc::new(TestChannel::default());
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 16);
        // Simulate core watchdog emitting TurnFailed.
        tx.send(CoreEvent::TimelineEvent {
            workspace_id: lucarne::control_plane::WorkspaceId::new(target.workspace.as_str()),
            turn_id: Some(ControlTurnId::new("turn-deadline")),
            event: Event::TurnFailed(lucarne::agent_runtime::events::TurnFailedEvent {
                turn_id: "turn-deadline".into(),
                error: "agent turn deadline reached after 3600s".into(),
                code: "timeout".into(),
            }),
        })
        .expect("send TurnFailed");
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-deadline".into()),
                instance_id: InstanceId("instance-deadline".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();
        let result = drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-deadline"),
                    recorder: Arc::new(NormalizingTimelineRecorder::default()),
                }),
                ..Default::default()
            }),
        )
        .await;
        let err = result.expect_err("drain must fail on TurnFailed event");
        assert!(
            err.contains("timeout") || err.contains("deadline"),
            "must surface failure, got: {err}"
        );
    }

    #[tokio::test]
    async fn telegram_rendering_uses_recorded_timeline_projection() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 4);
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "provider-raw".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-timeline".into()),
                instance_id: InstanceId("instance-timeline".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();
        let recorder = Arc::new(NormalizingTimelineRecorder::default());

        drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-timeline"),
                    recorder,
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("turn should drain");
        let _ = drafts.finalize(&*channel, &target, "test", None).await;

        let sent = test_channel.sends.lock().unwrap();
        let rendered = sent
            .last()
            .map(|msg| msg.body.clone())
            .or_else(|| {
                test_channel
                    .edits
                    .lock()
                    .unwrap()
                    .last()
                    .map(|(_, msg)| msg.body.clone())
            })
            .expect("rendered telegram message");
        assert!(
            rendered.contains("timeline-normalized"),
            "telegram rendering must consume the timeline projection, got: {rendered}"
        );
        assert!(
            !rendered.contains("provider-raw"),
            "provider raw event text must not bypass the timeline projection: {rendered}"
        );
    }

    #[tokio::test]
    async fn drain_sends_attachment_and_records_timeline() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 4);
        let png = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        send_test_event(
            &tx,
            &target,
            Event::Attachment(AgentAttachment {
                id: "ig_turn".into(),
                filename: "codex-image-ig_turn.png".into(),
                media_type: "image/png".into(),
                data_base64: png,
                caption: Some("caption".into()),
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-attachment".into()),
                instance_id: InstanceId("instance-attachment".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();
        let recorder = Arc::new(NormalizingTimelineRecorder::default());

        let mut report = drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-attachment"),
                    recorder: recorder.clone(),
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("turn should drain");

        assert!(report.message_ids.is_empty());
        assert_eq!(report.attachments.len(), 1);
        let ids = deliver_pending_attachments(
            &*channel,
            &target,
            "test",
            std::mem::take(&mut report.attachments),
        )
        .await;
        assert_eq!(ids, vec![MessageId::new("attachment-1")]);
        let attachments = test_channel.attachments.lock().unwrap();
        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].filename, "codex-image-ig_turn.png");
        assert_eq!(attachments[0].media_type, "image/png");
        assert_eq!(attachments[0].bytes, vec![1, 2, 3]);
        assert_eq!(attachments[0].caption.as_deref(), Some("caption"));
        assert_eq!(attachments[0].notification, NotificationPolicy::Notify);
        let recorded = recorder.items.lock().unwrap();
        assert!(recorded
            .iter()
            .any(|(kind, _payload)| *kind == TimelineItemKind::Attachment));
    }

    #[tokio::test]
    async fn deliver_pending_attachments_splits_long_caption_to_followup_message() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let png = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        let caption = format!("{}TAIL", "a".repeat(TELEGRAM_ATTACHMENT_CAPTION_CHARS));

        let ids = deliver_pending_attachments(
            &*channel,
            &target,
            "test",
            vec![PendingAttachmentDelivery {
                attachment: AgentAttachment {
                    id: "ig_long".into(),
                    filename: "codex-image-ig_long.png".into(),
                    media_type: "image/png".into(),
                    data_base64: png,
                    caption: Some(caption.into()),
                },
                reply_to: None,
                notification: NotificationPolicy::Notify,
            }],
        )
        .await;

        assert_eq!(
            ids,
            vec![MessageId::new("attachment-1"), MessageId::new("1")]
        );
        let attachments = test_channel.attachments.lock().unwrap();
        assert_eq!(attachments.len(), 1);
        let attached_caption = attachments[0].caption.as_deref().expect("caption");
        assert_eq!(
            attached_caption.chars().count(),
            TELEGRAM_ATTACHMENT_CAPTION_CHARS
        );
        assert!(attached_caption.chars().all(|ch| ch == 'a'));
        let sends = test_channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].body, "TAIL");
        assert_eq!(sends[0].reply_to, Some(MessageId::new("attachment-1")));
        assert_eq!(sends[0].notification, NotificationPolicy::Notify);
    }

    #[tokio::test]
    async fn failed_attachment_reports_english_failure_without_blocking_final_text() {
        let test_channel = Arc::new(TestChannel::default());
        test_channel.attachment_errors.lock().unwrap().extend(
            (0..=lucarne_channel::robust::ATTACHMENT_DELIVERY_MAX_RETRIES)
                .map(|_| "upload exploded".to_string()),
        );
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 8);
        let png = base64::engine::general_purpose::STANDARD.encode([1u8, 2, 3]);
        send_test_event(
            &tx,
            &target,
            Event::Attachment(AgentAttachment {
                id: "ig_failed".into(),
                filename: "failed.png".into(),
                media_type: "image/png".into(),
                data_base64: png,
                caption: None,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "final text still arrives".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-attachment-failure".into()),
                instance_id: InstanceId("instance-attachment-failure".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();
        let recorder = Arc::new(NormalizingTimelineRecorder::default());

        let mut report = drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-attachment-failure"),
                    recorder,
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("attachment send failure must not fail turn drain");
        assert!(report.message_ids.is_empty());
        assert_eq!(*test_channel.attachment_attempts.lock().unwrap(), 0);

        let _ = drafts.finalize(&*channel, &target, "test", None).await;
        let edited = test_channel.edits.lock().unwrap();
        let bodies = test_channel
            .sends
            .lock()
            .unwrap()
            .iter()
            .map(|msg| msg.body.clone())
            .chain(edited.iter().map(|(_, msg)| msg.body.clone()))
            .collect::<Vec<_>>();
        assert!(
            bodies
                .iter()
                .any(|body| body.contains("timeline-normalized")),
            "final assistant text missing from Telegram sends/edits: {bodies:?}"
        );
        drop(edited);

        let ids = deliver_pending_attachments(
            &*channel,
            &target,
            "test",
            std::mem::take(&mut report.attachments),
        )
        .await;
        assert_eq!(ids, vec![MessageId::new("3")]);
        assert_eq!(
            *test_channel.attachment_attempts.lock().unwrap(),
            lucarne_channel::robust::ATTACHMENT_DELIVERY_MAX_RETRIES + 1
        );
        let sent = test_channel.sends.lock().unwrap();
        assert!(sent
            .iter()
            .any(|msg| msg.body.contains("Attachment delivery failed.")
                && msg.body.contains("File: failed.png")
                && msg.body.contains("Media type: image/png")
                && msg.body.contains("Attachment id: ig_failed")
                && msg.body.contains("Error: upload exploded")));
        assert!(test_channel.attachments.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pi_like_reasoning_tool_flow_keeps_process_output_outside_status() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 8);
        send_test_event(
            &tx,
            &target,
            Event::Reasoning(ReasoningEvent { text: "The".into() }),
        );
        send_test_event(
            &tx,
            &target,
            Event::ToolCall(ToolCallEvent {
                call_id: CallId("call-1".into()),
                name: "bash".into(),
                input: serde_json::json!({"command": "date"}),
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::ToolResult(ToolResultEvent {
                call_id: CallId("call-1".into()),
                output: serde_json::json!("Tue May 12 13:17:01 CST 2026\n"),
                is_error: Some(false),
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "下午 1:17，2026年5月12日周二。".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-pi-like".into()),
                instance_id: InstanceId("instance-pi-like".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();

        drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-pi-like"),
                    recorder: Arc::new(NormalizingTimelineRecorder::default()),
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("turn should drain after final message");
        let _ = drafts.finalize(&*channel, &target, "pi", None).await;

        let bodies = test_channel
            .sends
            .lock()
            .unwrap()
            .iter()
            .map(|message| message.body.clone())
            .chain(
                test_channel
                    .edits
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(_, message)| message.body.clone()),
            )
            .collect::<Vec<_>>();
        assert!(
            bodies.iter().any(|body| body == "The"),
            "Pi reasoning should render as process output outside the timer status: {bodies:?}"
        );
        assert!(
            bodies
                .iter()
                .any(|body| body.contains("timeline-normalized")),
            "final assistant message must be the visible reply: {bodies:?}"
        );
    }

    #[tokio::test]
    async fn process_reasoning_stays_out_of_timer_status_while_tools_stay_in_timer() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 8);
        send_test_event(
            &tx,
            &target,
            Event::Reasoning(ReasoningEvent {
                text: "I am checking the workspace.".into(),
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::ToolCall(ToolCallEvent {
                call_id: CallId("call-1".into()),
                name: "bash".into(),
                input: serde_json::json!({"command": "date"}),
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "Done.".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-process-boundary".into()),
                instance_id: InstanceId("instance-process-boundary".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();

        drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-process-boundary"),
                    recorder: Arc::new(NormalizingTimelineRecorder::default()),
                }),
                ..Default::default()
            }),
        )
        .await
        .expect("turn should drain after final message");
        let _ = drafts.finalize(&*channel, &target, "pi", None).await;

        let status = render_status(&shared, true);
        assert!(
            status.contains("1 steps") && status.contains("🔧 bash(date)"),
            "tool activity should stay in the timer status: {status}"
        );
        assert!(
            !status.contains("workspace") && !status.contains("正在回复"),
            "non-tool process/assistant text must not be timer activity: {status}"
        );

        let bodies = test_channel
            .sends
            .lock()
            .unwrap()
            .iter()
            .map(|message| message.body.clone())
            .chain(
                test_channel
                    .edits
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(_, message)| message.body.clone()),
            )
            .collect::<Vec<_>>();
        assert!(
            bodies
                .iter()
                .any(|body| body.contains("I am checking the workspace.")),
            "reasoning should render as process output outside the timer status: {bodies:?}"
        );
    }

    #[tokio::test]
    async fn timeline_append_failure_fails_turn_without_rendering_raw_event() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 4);
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "provider-raw".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-timeline-failure".into()),
                instance_id: InstanceId("instance-timeline-failure".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();

        let err = drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions {
                recording: Some(TurnRecording {
                    turn_id: ControlTurnId::new("turn-timeline-failure"),
                    recorder: Arc::new(FailingTimelineRecorder),
                }),
                ..Default::default()
            }),
        )
        .await
        .expect_err("timeline append failure must fail the turn");

        assert!(err.contains("timeline store unavailable"));
        assert!(
            test_channel.sends.lock().unwrap().is_empty(),
            "raw provider text must not render when timeline projection cannot be recorded"
        );
    }

    #[tokio::test]
    async fn telegram_projection_requires_timeline_recording() {
        let test_channel = Arc::new(TestChannel::default());
        let channel: Arc<dyn Channel> = test_channel.clone();
        let target = test_target();
        let (tx, rx) = test_event_stream(&target.workspace, 4);
        send_test_event(
            &tx,
            &target,
            Event::Message(MessageEvent {
                role: MessageRole::Assistant,
                text: "provider-raw".into(),
                streaming: false,
            }),
        );
        send_test_event(
            &tx,
            &target,
            Event::TurnCompleted(lucarne::agent_runtime::events::TurnCompletedEvent {
                turn_id: "provider-turn".into(),
                usage: None,
            }),
        );
        drop(tx);
        let live = LiveSession {
            session: Arc::new(SilentSession {
                id: SessionId("session-no-recording".into()),
                instance_id: InstanceId("instance-no-recording".into()),
            }),
            events: tokio::sync::Mutex::new(rx),
            pending_intv: StdMutex::new(std::collections::HashMap::new()),
        };
        let shared = Shared::new();
        let mut drafts = DraftStream::new();

        let err = drain_events(
            &channel,
            &target,
            &live,
            "test",
            &shared,
            &mut drafts,
            DrainMode::Turn(TurnRunOptions::default()),
        )
        .await
        .expect_err("Telegram projection must require a control-plane timeline");

        assert!(err.contains("timeline recording is required"));
        assert!(
            test_channel.sends.lock().unwrap().is_empty(),
            "raw provider text must not render without timeline recording"
        );
    }

    #[test]
    fn elapsed_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "0s");
        assert_eq!(format_elapsed(Duration::from_secs(5)), "5s");
        assert_eq!(format_elapsed(Duration::from_secs(59)), "59s");
    }

    #[test]
    fn elapsed_minutes() {
        assert_eq!(format_elapsed(Duration::from_secs(60)), "1m 00s");
        assert_eq!(format_elapsed(Duration::from_secs(75)), "1m 15s");
        assert_eq!(format_elapsed(Duration::from_secs(3599)), "59m 59s");
    }

    #[test]
    fn elapsed_hours() {
        assert_eq!(format_elapsed(Duration::from_secs(3600)), "1h 00m");
        assert_eq!(format_elapsed(Duration::from_secs(3600 + 125)), "1h 02m");
        assert_eq!(
            format_elapsed(Duration::from_secs(7 * 3600 + 45 * 60)),
            "7h 45m"
        );
    }

    #[test]
    fn status_renders_only_status_snapshot_read_model() {
        let event = CommandResultEvent {
            command: "status".into(),
            result: lucarne::event::CommandResultPayload {
                command: "status".into(),
                result: CommandResultData::Status(AgentStatus {
                    model: Some("event-model".into()),
                    directory: Some("/event/path".into()),
                    ..Default::default()
                }),
            },
        };
        let options = CommandRunOptions {
            status_snapshot: Some(StatusSnapshot {
                workspace_id: Some(lucarne::control_plane::WorkspaceId::new("workspace-1")),
                provider_id: Some("codex".into()),
                model: Some("snapshot-model".into()),
                project_path: Some(std::path::PathBuf::from("/snapshot/path")),
                channel_binding_state: Some("telegram:100:2".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let msg = render_command_result(&WorkspaceId::new("workspace-1"), &event, Some(&options))
            .expect("status message");

        assert!(msg.body.contains("snapshot-model"));
        assert!(msg.body.contains("/snapshot/path"));
        assert!(!msg.body.contains("event-model"));
        assert!(!msg.body.contains("/event/path"));
    }

    #[test]
    fn subagent_child_rows_render_from_control_plane_links() {
        let registry = Arc::new(TestSubAgentCallbackRegistry::default());
        let mut openable = lucarne::control_plane::SubAgentLinkRecord::new_openable(
            lucarne::control_plane::SubAgentLinkId::new(format!(
                "subagent-action-{}",
                "x".repeat(96)
            )),
            lucarne::control_plane::WorkspaceId::new("workspace-1"),
            lucarne::control_plane::SubAgentActionId::new("subagent-action-1"),
            lucarne::control_plane::TurnId::new("turn-1"),
            lucarne::control_plane::ProviderSessionId::new("parent-session"),
            lucarne::control_plane::ProviderSessionId::new("child-session"),
            "child-session",
        );
        openable.label = Some("Parser".into());
        openable.role = Some("explorer".into());
        openable.model = Some("opus".into());
        let non_openable = lucarne::control_plane::SubAgentLinkRecord::new_non_openable(
            lucarne::control_plane::SubAgentLinkId::new("subagent-action-2"),
            lucarne::control_plane::WorkspaceId::new("workspace-1"),
            lucarne::control_plane::SubAgentActionId::new("subagent-action-2"),
            lucarne::control_plane::TurnId::new("turn-1"),
            lucarne::control_plane::ProviderSessionId::new("parent-session"),
            "Review",
        );

        let target = test_target();
        let msg =
            render_subagent_links(&target, &[openable, non_openable], Some(registry.as_ref()))
                .expect("subagent rows");

        assert!(msg.body.contains("Parser"));
        assert!(msg.body.contains("Review"));
        assert!(msg.body.contains("Parser — running · opus\n\n2. Review"));
        let buttons = msg.buttons.iter().flatten().collect::<Vec<_>>();
        assert_eq!(buttons.len(), 1);
        assert_eq!(buttons[0].label, "open Parser");
        assert!(buttons[0].data.starts_with("subagent:c:s"));
        assert!(!buttons[0].data.contains(&"x".repeat(32)));
        assert!(buttons[0].data.len() <= 64);
        let calls = registry.calls.lock().unwrap();
        assert_eq!(
            calls[0].as_str(),
            "subagent-action-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        );
    }

    #[test]
    fn tool_detail_from_command() {
        let v = serde_json::json!({"command": "cargo test --workspace"});
        assert_eq!(tool_call_detail(&v), "cargo test --workspace");
    }

    #[test]
    fn tool_detail_truncates_long() {
        let long = "a".repeat(200);
        let v = serde_json::json!({"path": long});
        let out = tool_call_detail(&v);
        assert!(out.chars().count() <= ACTIVITY_DETAIL_MAX);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn parse_intv_unrelated_returns_none() {
        assert!(parse_intv_callback("history:2").is_none());
        assert!(parse_intv_callback("refresh").is_none());
        assert!(parse_intv_callback("intv:").is_none());
        assert!(parse_intv_callback("intv:req-42:app:allow").is_none());
    }

    #[test]
    fn parse_intv_approval() {
        let cb = parse_intv_callback("intv:c:i42").unwrap();
        assert_eq!(
            cb,
            IntvCallback::Token {
                token: "i42".into()
            }
        );
    }

    #[test]
    fn parse_intv_answer() {
        let cb = parse_intv_callback("intv:c:i100").unwrap();
        assert_eq!(
            cb,
            IntvCallback::Token {
                token: "i100".into()
            }
        );
    }

    #[test]
    fn question_buttons_store_answer_label_values_not_option_indexes() {
        let registry = Arc::new(TestInterventionCallbackRegistry::default());
        let req = InterventionRequest::Question(QuestionRequest {
            req_id: "req-1".into(),
            questions: vec![Question {
                header: Some("mode".into()),
                text: "Pick mode".into(),
                options: vec![
                    QuestionOption {
                        label: "Default".into(),
                        description: None,
                    },
                    QuestionOption {
                        label: "Full Access".into(),
                        description: Some("Dangerous".into()),
                    },
                ],
                multi_select: false,
            }],
        });

        let msg = build_intervention_message(
            &req,
            Some(&(registry.clone() as Arc<dyn AgentInterventionCallbackRegistry>)),
            &test_target(),
            &InstanceId("instance-test".into()),
        );

        assert_eq!(msg.buttons.len(), 2);
        assert_eq!(msg.notification, NotificationPolicy::Notify);
        assert_eq!(
            registry.actions.lock().unwrap().as_slice(),
            &[
                IntvAction::Answer {
                    q_idx: 0,
                    values: vec!["Default".into()],
                },
                IntvAction::Answer {
                    q_idx: 0,
                    values: vec!["Full Access".into()],
                },
            ]
        );
    }

    #[test]
    fn approval_message_summarizes_command_input_when_message_is_missing() {
        let req = InterventionRequest::Approval(lucarne::ApprovalRequest {
            req_id: "req-1".into(),
            tool_name: "item/commandExecution/requestApproval".into(),
            message: None,
            input: Some(serde_json::json!({
                "command": "rm delete-target.txt",
                "cwd": "/tmp/repo"
            })),
        });

        let msg = build_intervention_message(
            &req,
            None,
            &test_target(),
            &InstanceId("instance-test".into()),
        );

        assert!(msg.body.contains("rm delete-target.txt"));
        assert_eq!(msg.notification, NotificationPolicy::Notify);
        assert!(msg.body.contains("/tmp/repo"));
        assert!(msg.body.contains("Command:"));
        assert!(msg.body.contains("CWD:"));
        assert!(!msg.body.contains("命令"));
        assert!(!msg.body.contains("目录"));
    }

    #[test]
    fn skills_catalog_renders_compact_copyable_names() {
        let result = AgentCommandResult {
            name: "skills".into(),
            source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
            data: AgentCommandResultData::Skills(AgentSkillCatalog {
                skills: vec![
                    AgentSkillSummary {
                        name: "imagegen".into(),
                        display_name: Some("Image Generation".into()),
                        description: Some("Generate images.".into()),
                        path: None,
                        scope: None,
                        source: None,
                        tokens: None,
                        enabled: None,
                    },
                    AgentSkillSummary {
                        name: "superpowers:brainstorming".into(),
                        display_name: Some("Brainstorming".into()),
                        description: Some("Explore intent before implementation.".into()),
                        path: None,
                        scope: None,
                        source: None,
                        tokens: None,
                        enabled: None,
                    },
                    AgentSkillSummary {
                        name: "superpowers:test-driven-development".into(),
                        display_name: Some("Test Driven Development".into()),
                        description: Some("Write the test first.".into()),
                        path: None,
                        scope: None,
                        source: None,
                        tokens: None,
                        enabled: None,
                    },
                    AgentSkillSummary {
                        name: "ui-ux-pro-max".into(),
                        display_name: Some("UI/UX Pro Max".into()),
                        description: Some("Design intelligence.".into()),
                        path: None,
                        scope: None,
                        source: None,
                        tokens: None,
                        enabled: None,
                    },
                ],
            }),
        };

        let target = test_target();
        let msg =
            render_immediate_command_result(&target, &result, None, None).expect("skills message");

        assert_eq!(
            msg.body,
            "skills\n\n- `imagegen`\n- `superpowers:*`\n  |-- `brainstorming`\n  |-- `test-driven-development`\n- `ui-ux-pro-max`\n"
        );
        assert!(!msg.body.contains("Image Generation"));
        assert!(!msg.body.contains("Explore intent before implementation."));
        assert!(!msg.body.contains("superpowers:brainstorming"));
        assert!(!msg.body.contains("\n  - `brainstorming`"));
    }

    #[test]
    fn command_catalog_lists_insert_blank_lines_between_items() {
        let target = test_target();

        let models = render_immediate_command_result(
            &target,
            &AgentCommandResult {
                name: "model".into(),
                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                data: AgentCommandResultData::Models(lucarne::agent_runtime::AgentModelCatalog {
                    current_model: None,
                    current_reasoning: None,
                    models: vec![
                        lucarne::agent_runtime::AgentModelOption {
                            id: "gpt-5.5".into(),
                            display_name: Some("GPT-5.5".into()),
                            description: Some("Frontier model".into()),
                            supported_reasoning: Vec::new(),
                        },
                        lucarne::agent_runtime::AgentModelOption {
                            id: "gpt-5.4".into(),
                            display_name: None,
                            description: None,
                            supported_reasoning: Vec::new(),
                        },
                    ],
                }),
            },
            None,
            None,
        )
        .expect("model list");
        assert!(models
            .body
            .contains("1. `gpt-5.5` — GPT-5.5 — Frontier model\n\n2. `gpt-5.4`"));

        let permissions = render_immediate_command_result(
            &target,
            &AgentCommandResult {
                name: "permissions".into(),
                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                data: AgentCommandResultData::Permissions(
                    lucarne::agent_runtime::AgentPermissionCatalog {
                        current_mode: None,
                        modes: vec![
                            lucarne::agent_runtime::AgentPermissionOption {
                                id: "on-request".into(),
                                display_name: None,
                                description: None,
                            },
                            lucarne::agent_runtime::AgentPermissionOption {
                                id: "never".into(),
                                display_name: None,
                                description: None,
                            },
                        ],
                    },
                ),
            },
            None,
            None,
        )
        .expect("permission list");
        assert!(permissions.body.contains("1. `on-request`\n\n2. `never`"));
        assert!(permissions.body.contains("set: `/permissions <mode>`"));
        assert!(
            permissions.buttons.is_empty(),
            "permission modes should be selected with /permissions <mode> text commands"
        );

        let forks = render_immediate_command_result(
            &target,
            &AgentCommandResult {
                name: "fork".into(),
                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                data: AgentCommandResultData::ForkTargets(
                    lucarne::agent_runtime::AgentForkTargetCatalog {
                        targets: vec![
                            lucarne::agent_runtime::AgentForkTarget {
                                id: "msg-1".into(),
                                label: Some("first assistant".into()),
                                description: None,
                            },
                            lucarne::agent_runtime::AgentForkTarget {
                                id: "msg-2".into(),
                                label: Some("second assistant".into()),
                                description: None,
                            },
                        ],
                    },
                ),
            },
            None,
            None,
        )
        .expect("fork list");
        assert!(forks
            .body
            .contains("/f1  `msg-1` — first assistant\n\n/f2  `msg-2` — second assistant"));
        assert!(
            forks.buttons.is_empty(),
            "fork targets should be selected with /fN text commands"
        );

        let commands = render_immediate_command_result(
            &target,
            &AgentCommandResult {
                name: "commands".into(),
                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                data: AgentCommandResultData::Commands(
                    lucarne::agent_runtime::AgentCommandCatalog {
                        commands: vec![
                            lucarne::agent_runtime::AgentCommand {
                                name: "review".into(),
                                description: Some("Review current changes.".into()),
                                aliases: vec!["r".into()],
                                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                                input: lucarne::agent_runtime::AgentCommandInput::Text {
                                    label: "target".into(),
                                    required: false,
                                },
                                completion: Default::default(),
                            },
                            lucarne::agent_runtime::AgentCommand {
                                name: "status".into(),
                                description: Some("Show status.".into()),
                                aliases: Vec::new(),
                                source: lucarne::agent_runtime::AgentCommandSource::AdapterMapped,
                                input: lucarne::agent_runtime::AgentCommandInput::None,
                                completion: Default::default(),
                            },
                        ],
                        complete: true,
                        revision: 1,
                    },
                ),
            },
            None,
            None,
        )
        .expect("command list");
        assert!(commands.body.contains(
            "1. `/review`\n   Review current changes.\n   usage: `/review [target]`\n   aliases: `/r`\n\n2. `/status`\n   Show status."
        ));
        assert!(commands
            .body
            .contains("2. `/status`\n   Show status.\n   usage: `/status`"));
    }

    #[tokio::test]
    async fn message_preview_appends_updates_but_tracks_latest_final_message() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "先看一下目录", false)
            .await;
        drafts
            .push(&channel, &target, DraftKind::Message, "这是最终结论", false)
            .await;

        let cur = drafts.current.as_ref().expect("draft exists");
        assert_eq!(cur.kind, DraftKind::Message);
        assert_eq!(cur.text, "先看一下目录\n\n这是最终结论");
        assert_eq!(drafts.final_message.as_deref(), Some("这是最终结论"));
    }

    #[tokio::test]
    async fn preview_reuses_single_draft_across_kind_switches() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "我先看看目录", false)
            .await;
        let first_id = drafts
            .fallback_msg_id
            .as_ref()
            .map(|id| id.as_str().to_string());

        drafts
            .push(&channel, &target, DraftKind::Thought, "继续分析实现", false)
            .await;

        let cur = drafts.current.as_ref().expect("draft exists");
        assert_eq!(
            drafts
                .fallback_msg_id
                .as_ref()
                .map(|id| id.as_str().to_string()),
            first_id
        );
        assert_eq!(cur.kind, DraftKind::Thought);
        assert_eq!(cur.text, "我先看看目录\n\n继续分析实现");
        assert_eq!(drafts.final_message.as_deref(), Some("我先看看目录"));
    }

    #[tokio::test]
    async fn reasoning_before_first_message_sends_process_draft() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Thought, "The", false)
            .await;

        assert!(
            channel
                .sends
                .lock()
                .unwrap()
                .iter()
                .any(|message| message.body == "The"
                    && message.notification == NotificationPolicy::Silent),
            "reasoning should create process output outside the timer status"
        );
        assert!(drafts.fallback_msg_id.is_some());
        assert!(drafts.final_message.is_none());

        drafts
            .push(&channel, &target, DraftKind::Message, "下午 1:17", false)
            .await;
        let finalized = drafts.finalize(&channel, &target, "pi", None).await;

        assert_eq!(finalized.bytes, "下午 1:17".len());
        assert_eq!(finalized.message_ids, vec![MessageId::new("2")]);
        for (_, msg) in channel.edits.lock().unwrap().iter() {
            assert_eq!(msg.notification, NotificationPolicy::Silent);
        }
        let sends = channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[1].body, "下午 1:17");
        assert_eq!(sends[1].notification, NotificationPolicy::Notify);
        assert_eq!(channel.deletes.lock().unwrap().as_slice(), &["1"]);
    }

    #[tokio::test]
    async fn consecutive_reasoning_chunks_render_plain_process_text() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Thought, "第一段过程", false)
            .await;
        drafts
            .push(&channel, &target, DraftKind::Thought, "第二段过程", false)
            .await;

        let cur = drafts.current.as_ref().expect("draft exists");
        assert_eq!(cur.text, "第一段过程\n\n第二段过程");
    }

    #[tokio::test]
    async fn reasoning_preview_omits_early_content_before_channel_limit() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();
        let text = format!("{}最终过程", "早期过程".repeat(400));
        assert!(
            text.chars().count() < channel.message_char_limit(),
            "test fixture must stay below the transport hard limit"
        );

        drafts
            .push(&channel, &target, DraftKind::Thought, &text, false)
            .await;

        let sends = channel.sends.lock().unwrap();
        let body = sends.last().expect("process preview").body.as_str();
        assert!(
            body.contains(PREVIEW_TRUNCATED_NOTICE),
            "long process previews should omit early content before the hard channel limit: {body}"
        );
        assert!(
            body.contains("最终过程"),
            "latest process tail must stay visible: {body}"
        );
        assert!(
            !body.starts_with("早期过程早期过程"),
            "early process content should be omitted from the live preview: {body}"
        );
    }

    #[tokio::test]
    async fn final_reply_deletes_silent_preview_and_sends_notify_message() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "这是最终结论", false)
            .await;

        let finalized = drafts.finalize(&channel, &target, "test", None).await;

        assert_eq!(finalized.bytes, "这是最终结论".len());
        assert_eq!(finalized.message_ids, vec![MessageId::new("2")]);
        assert_eq!(channel.deletes.lock().unwrap().as_slice(), &["1"]);
        assert!(channel.edits.lock().unwrap().is_empty());
        let sends = channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0].notification, NotificationPolicy::Silent);
        assert_eq!(sends[1].body, "这是最终结论");
        assert_eq!(sends[1].format, lucarne_channel::TextFormat::Markdown);
        assert_eq!(sends[1].notification, NotificationPolicy::Notify);
    }

    #[tokio::test]
    async fn final_reply_without_preview_is_sent_as_markdown() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();
        drafts.final_message = Some("Use `skills` and **markdown**".into());

        let finalized = drafts.finalize(&channel, &target, "test", None).await;

        assert_eq!(finalized.bytes, "Use `skills` and **markdown**".len());
        let sends = channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].body, "Use `skills` and **markdown**");
        assert_eq!(sends[0].format, lucarne_channel::TextFormat::Markdown);
        assert_eq!(sends[0].notification, NotificationPolicy::Notify);
    }

    #[tokio::test]
    async fn live_preview_uses_markdown() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(
                &channel,
                &target,
                DraftKind::Message,
                "Working on `skills`",
                false,
            )
            .await;

        let sends = channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0].format, lucarne_channel::TextFormat::Markdown);
        assert_eq!(sends[0].notification, NotificationPolicy::Silent);
    }

    #[tokio::test]
    async fn final_reply_does_not_depend_on_preview_edit_success() {
        let channel = TestChannel::default();
        channel.edit_errors.lock().unwrap().push(
            "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message"
                .into(),
        );
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "Hello", false)
            .await;

        let finalized = drafts.finalize(&channel, &target, "test", None).await;

        assert_eq!(finalized.bytes, "Hello".len());
        assert_eq!(finalized.message_ids, vec![MessageId::new("2")]);
        let sends = channel.sends.lock().unwrap();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0].notification, NotificationPolicy::Silent);
        assert_eq!(sends[1].body, "Hello");
        assert_eq!(sends[1].notification, NotificationPolicy::Notify);
        assert_eq!(channel.deletes.lock().unwrap().as_slice(), &["1"]);
    }

    #[tokio::test]
    async fn streaming_chunks_accumulate_without_replaying_prior_text() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "Hello ", true)
            .await;
        drafts
            .push(&channel, &target, DraftKind::Message, "world", true)
            .await;

        let cur = drafts.current.as_ref().expect("draft exists");
        assert_eq!(cur.text, "Hello world");
        assert_eq!(drafts.final_message.as_deref(), Some("Hello world"));
    }

    #[tokio::test]
    async fn final_full_message_replaces_streaming_preview() {
        let channel = TestChannel::default();
        let target = test_target();
        let mut drafts = DraftStream::new();

        drafts
            .push(&channel, &target, DraftKind::Message, "Hel", true)
            .await;
        drafts
            .push(&channel, &target, DraftKind::Message, "Hello", false)
            .await;

        let cur = drafts.current.as_ref().expect("draft exists");
        assert_eq!(cur.text, "Hello");
        assert_eq!(drafts.final_message.as_deref(), Some("Hello"));
    }

    #[test]
    fn draft_preview_truncates_oldest_content_when_over_limit() {
        let text = "第一段过程\n\n第二段过程\n\n第三段过程\n\n最终一段".repeat(20);
        let rendered = render_draft_preview(&text, 80);
        assert!(rendered.chars().count() <= 80);
        assert!(rendered.contains(PREVIEW_TRUNCATED_NOTICE));
        assert!(rendered.contains("最终一段"));
    }

    #[test]
    fn event_log_text_keeps_agent_return_visible() {
        let text = "第一行 agent 返回\n第二行包含 DEBUG_TOKEN";
        let rendered = log_event_text(text, 200);

        assert!(rendered.contains("第一行 agent 返回"));
        assert!(rendered.contains("DEBUG_TOKEN"));
        assert!(!rendered.contains('\n'));
    }

    #[test]
    fn event_log_text_truncates_long_agent_return() {
        let rendered = log_event_text(&"x".repeat(128), 16);

        assert_eq!(rendered.chars().count(), 17);
        assert!(rendered.ends_with('…'));
    }
}
