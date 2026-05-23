use smol_str::SmolStr;
use std::borrow::Cow;
use std::io::BufRead;

use serde::{Deserialize, Deserializer as SerdeDeserializer, Serialize};
use serde_json::value::RawValue;
use tracing::trace;

use crate::util::{box_str, cow_to_box, opt_cow_to_box};
use crate::{Error, ParseSelection, Result};

mod types;
pub(crate) use types::*;

#[cfg(feature = "discovery")]
mod discovery;
#[cfg(feature = "agent_session")]
mod event;

pub struct Claude;

#[cfg(feature = "agent_session")]
impl Claude {
    pub fn probe_session_meta<R>(reader: R) -> Result<Option<crate::agent_session::SessionMeta>>
    where
        R: BufRead,
    {
        Self::probe_session_meta_inner(reader, false).map(|opt| opt.map(|(meta, _)| meta))
    }

    /// Like `probe_session_meta` but also extracts a usable title from the
    /// first user-role message that carries `cwd` (which is also the line that
    /// terminates the cwd search). The title is taken from the message's first
    /// text content block, single-line trimmed to 80 chars.
    ///
    /// Stops as soon as cwd + title are both known, so the cost is the same as
    /// `probe_session_meta` for typical Claude rollouts.
    pub fn probe_session_meta_with_title<R>(
        reader: R,
    ) -> Result<Option<(crate::agent_session::SessionMeta, Option<SmolStr>)>>
    where
        R: BufRead,
    {
        Self::probe_session_meta_inner(reader, true)
    }

    fn probe_session_meta_inner<R>(
        mut reader: R,
        want_title: bool,
    ) -> Result<Option<(crate::agent_session::SessionMeta, Option<SmolStr>)>>
    where
        R: BufRead,
    {
        let mut line = Vec::new();
        let mut session_id: Option<SmolStr> = None;
        let mut created_at: Option<SmolStr> = None;
        let mut source_kind: Option<SmolStr> = None;
        let mut cwd: Option<SmolStr> = None;
        let mut title: Option<UserTitle> = None;

        loop {
            if !read_next_nonempty_line(&mut reader, &mut line)? {
                break;
            }

            let raw: RawEntry<'_> = serde_json::from_slice(&line)?;

            if session_id.is_none() && raw.session_id.is_some() {
                session_id = opt_box_str(raw.session_id);
                created_at = opt_timestamp_box_str(raw.timestamp.as_ref());
                source_kind = raw.kind.map(box_str);
            }

            if cwd.is_none() {
                if let Some(value) = raw.cwd.as_deref() {
                    cwd = Some(box_str(value));
                    if want_title {
                        if let Some(found) = extract_user_message_title(&raw) {
                            let should_return = found.is_delegated_fork_marker
                                || !raw_is_before_session_start(&raw, created_at.as_deref());
                            title = Some(found);
                            if should_return {
                                break;
                            }
                        }
                    } else {
                        break;
                    }
                    continue;
                }
                // Pre-cwd lines (e.g. `queue-operation`) carry session_id but
                // no cwd; keep scanning.
                continue;
            }

            // Past cwd, only useful work left is hunting for the title.
            if !want_title || title.as_ref().is_some_and(UserTitle::is_final) {
                break;
            }

            if let Some(found) = extract_user_message_title(&raw) {
                let should_return = found.is_delegated_fork_marker
                    || !raw_is_before_session_start(&raw, created_at.as_deref());
                if title.is_none() || found.is_delegated_fork_marker {
                    title = Some(found);
                }
                if should_return {
                    break;
                }
            }
        }

        let title = title.map(|title| title.title);

        if session_id.is_none() && cwd.is_none() {
            return Ok(None);
        }
        Ok(Some((
            crate::agent_session::SessionMeta {
                session_id: crate::agent_session::smol_opt(session_id),
                cwd,
                created_at: crate::agent_session::smol_opt(created_at),
                source_kind: crate::agent_session::smol_opt(source_kind),
                ..crate::agent_session::SessionMeta::default()
            },
            title,
        )))
    }
}

/// Pull a single-line title from the `message.content` of a user RawEntry.
/// Accepts both shapes Claude writes:
///   - content as a JSON string (`"hello"`)
///   - content as an array of blocks (`[{"type":"text","text":"hello"}, ...]`)
///
/// Skips Claude Code's wrapper messages so the title reflects what the user
/// actually said:
///   - `isMeta: true` rows (Claude's own marker for "not a real prompt"; e.g.
///     `<local-command-caveat>` continuation banners)
///   - rows whose content text starts with one of the known XML-style wrappers
///     Claude Code injects for slash commands and tool/IDE plumbing
///     (`<command-name>`, `<local-command-stdout>`, `<system-reminder>`,
///     `<bash-input>`, `<bash-stdout>`, `<bash-stderr>`).
struct UserTitle {
    title: SmolStr,
    is_delegated_fork_marker: bool,
}

impl UserTitle {
    fn is_final(&self) -> bool {
        self.is_delegated_fork_marker
    }
}

fn extract_user_message_title(raw: &RawEntry<'_>) -> Option<UserTitle> {
    if !matches!(raw.kind, Some("user")) {
        return None;
    }
    if raw.is_meta.unwrap_or(false) {
        return None;
    }
    let message = raw.message.as_ref()?;
    let content = message.content?;
    let text = parse_user_content_text(content)?;
    if is_claude_wrapper_message(&text) {
        return None;
    }
    let snippet = first_line_snippet(&text, 80);
    if snippet.is_empty() {
        None
    } else {
        Some(UserTitle {
            title: snippet.into(),
            is_delegated_fork_marker: is_claude_delegated_fork_user_text(&text),
        })
    }
}

fn raw_is_before_session_start(raw: &RawEntry<'_>, session_started_at: Option<&str>) -> bool {
    match (raw.timestamp.as_ref(), session_started_at) {
        (Some(Timestamp::Text(timestamp)), Some(session_started_at)) => {
            timestamp.as_ref() < session_started_at
        }
        (Some(Timestamp::Millis(timestamp)), Some(session_started_at)) => session_started_at
            .parse::<i64>()
            .is_ok_and(|session_started_at| *timestamp < session_started_at),
        _ => false,
    }
}

fn is_claude_wrapper_message(text: &str) -> bool {
    const WRAPPERS: &[&str] = &[
        "<command-name>",
        "<command-message>",
        "<command-args>",
        "<command-stdout>",
        "<local-command-caveat>",
        "<local-command-stdout>",
        "<local-command-stderr>",
        "<system-reminder>",
        "<bash-input>",
        "<bash-stdout>",
        "<bash-stderr>",
        "<user-prompt-submit-hook>",
    ];
    let trimmed = text.trim_start();
    WRAPPERS.iter().any(|prefix| trimmed.starts_with(prefix))
}

fn is_claude_delegated_fork_user_text(text: &str) -> bool {
    text.trim_start().starts_with(
        "Task: You are a delegated subagent running from a fork of the parent session.",
    )
}

fn parse_user_content_text(raw: &RawValue) -> Option<String> {
    let bytes = raw.get().as_bytes();
    let first = bytes.iter().copied().find(|b| !b.is_ascii_whitespace())?;
    if first == b'"' {
        return serde_json::from_str::<String>(raw.get()).ok();
    }
    if first != b'[' {
        return None;
    }
    let blocks: Vec<TitleProbeBlock<'_>> = serde_json::from_str(raw.get()).ok()?;
    blocks.into_iter().find_map(|block| match block.kind {
        Some("text") => block.text.map(|cow| cow.into_owned()),
        _ => None,
    })
}

#[derive(Deserialize)]
struct TitleProbeBlock<'a> {
    #[serde(rename = "type", default)]
    kind: Option<&'a str>,
    #[serde(default, borrow)]
    text: Option<Cow<'a, str>>,
}

fn first_line_snippet(text: &str, max: usize) -> String {
    let first = text.lines().next().unwrap_or(text).trim();
    if first.chars().count() > max {
        let mut iter = first.chars();
        let truncated: String = iter.by_ref().take(max).collect();
        format!("{truncated}…")
    } else {
        first.to_string()
    }
}

impl Claude {
    pub(crate) fn name() -> &'static str {
        "claude"
    }
}

#[cfg(any(test, feature = "watch"))]
fn parse_claude_reader<R>(mut reader: R, selection: ParseSelection) -> Result<(Version, Body)>
where
    R: BufRead,
{
    parse_claude_body_reader(&mut reader, selection)
}

pub(super) fn parse_claude_body_reader<R>(
    mut reader: R,
    selection: ParseSelection,
) -> Result<(Version, Body)>
where
    R: BufRead,
{
    if selection.is_meta_only() {
        return parse_claude_meta_only(reader);
    }

    let mut entries = Vec::new();
    let mut version = Version::V1;
    let mut line = Vec::new();

    while read_next_nonempty_line(&mut reader, &mut line)? {
        let raw: RawEntry<'_> = serde_json::from_slice(&line)?;

        match raw.kind {
            Some("user") | Some("assistant") => {
                if !(selection.includes_messages() || selection.includes_usage()) {
                    continue;
                }
                let blocks = if selection.includes_messages() {
                    raw.message
                        .as_ref()
                        .map(parse_claude_message)
                        .transpose()?
                        .unwrap_or_default()
                } else {
                    Vec::new()
                };
                let usage = if selection.includes_usage() {
                    raw.message
                        .as_ref()
                        .and_then(|message| message.usage)
                        .map(parse_claude_usage)
                        .transpose()?
                } else {
                    None
                };

                entries.push(Entry::Message(MessageEntry {
                    message_id: raw
                        .message
                        .as_ref()
                        .and_then(|message| opt_box_str(message.id)),
                    role: map_role(raw.kind.unwrap_or("unknown")),
                    session_id: opt_box_str(raw.session_id),
                    cwd: opt_cow_to_box(raw.cwd.clone()),
                    model: raw
                        .message
                        .as_ref()
                        .and_then(|message| message.model.clone().map(cow_to_box)),
                    stop_reason: raw
                        .message
                        .as_ref()
                        .and_then(|message| opt_box_str(message.stop_reason)),
                    usage,
                    timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    blocks: blocks.into_boxed_slice(),
                }));
            }
            Some("tool_use") => {
                version = Version::V2;
                if selection.includes_operations() {
                    entries.push(Entry::ToolUse(ToolUseEntry {
                        tool_name: opt_box_str(raw.tool_name),
                        tool_input_json: opt_raw_json_box(raw.tool_input),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
            Some("tool_result") => {
                version = Version::V2;
                if selection.includes_operations() {
                    entries.push(Entry::ToolResult(ToolResultEntry {
                        tool_name: opt_box_str(raw.tool_name),
                        tool_input_json: opt_raw_json_box(raw.tool_input),
                        tool_output_json: opt_raw_json_box(raw.tool_output),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
            Some("attachment") => {
                version = Version::V2;
                if selection.includes_snapshots() {
                    let attachment = raw.attachment;
                    entries.push(Entry::Attachment(AttachmentEntry {
                        attachment_type: attachment
                            .as_ref()
                            .and_then(|value| opt_box_str(value.attachment_type)),
                        name: attachment
                            .as_ref()
                            .and_then(|value| opt_cow_to_box(value.name.clone())),
                        species: attachment
                            .as_ref()
                            .and_then(|value| opt_box_str(value.species)),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
            Some("permission-mode") => {
                version = Version::V2;
                if selection.includes_state() {
                    entries.push(Entry::PermissionMode(PermissionModeEntry {
                        permission_mode: opt_box_str(raw.permission_mode),
                        session_id: opt_box_str(raw.session_id),
                    }));
                }
            }
            Some("file-history-snapshot") => {
                version = Version::V2;
                if selection.includes_snapshots() {
                    entries.push(Entry::FileHistorySnapshot(FileHistorySnapshotEntry {
                        message_id: opt_box_str(raw.message_id),
                        timestamp: raw
                            .snapshot
                            .as_ref()
                            .and_then(|snapshot| opt_box_str(snapshot.timestamp)),
                    }));
                }
            }
            Some("last-prompt") => {
                version = Version::V2;
                if selection.includes_state() {
                    entries.push(Entry::LastPrompt(LastPromptEntry {
                        session_id: opt_box_str(raw.session_id),
                        last_prompt: raw.last_prompt.map(cow_to_box),
                    }));
                }
            }
            Some("progress") => {
                version = Version::V2;
                if !selection.includes_state() {
                    continue;
                }
                let progress = raw.progress.as_ref();
                let progress_kind = progress.and_then(|value| value.kind);
                match progress_kind {
                    Some("hook_progress") => {
                        entries.push(Entry::Progress(ProgressEntry::HookProgress {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            hook_event: progress.and_then(|value| opt_box_str(value.hook_event)),
                            hook_name: progress.and_then(|value| opt_box_str(value.hook_name)),
                            command: progress
                                .and_then(|value| opt_cow_to_box(value.command.clone())),
                        }))
                    }
                    Some("bash_progress") => {
                        entries.push(Entry::Progress(ProgressEntry::BashProgress {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            output: progress.and_then(|value| opt_cow_to_box(value.output.clone())),
                            full_output: progress
                                .and_then(|value| opt_cow_to_box(value.full_output.clone())),
                            elapsed_time_seconds: progress
                                .and_then(|value| value.elapsed_time_seconds),
                            total_lines: progress.and_then(|value| value.total_lines),
                        }))
                    }
                    Some("agent_progress") => {
                        entries.push(Entry::Progress(ProgressEntry::AgentProgress {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            prompt: progress.and_then(|value| opt_cow_to_box(value.prompt.clone())),
                            agent_id: progress.and_then(|value| opt_box_str(value.agent_id)),
                            message_json: progress.and_then(|value| value.message.map(json_to_box)),
                        }))
                    }
                    Some("query_update") => {
                        entries.push(Entry::Progress(ProgressEntry::QueryUpdate {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            query: progress.and_then(|value| opt_cow_to_box(value.query.clone())),
                        }))
                    }
                    Some("search_results_received") => {
                        entries.push(Entry::Progress(ProgressEntry::SearchResultsReceived {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            query: progress.and_then(|value| opt_cow_to_box(value.query.clone())),
                            result_count: progress.and_then(|value| value.result_count),
                        }))
                    }
                    Some("mcp_progress") => {
                        entries.push(Entry::Progress(ProgressEntry::McpProgress {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            status: progress.and_then(|value| opt_box_str(value.status)),
                            server_name: progress.and_then(|value| opt_box_str(value.server_name)),
                            tool_name: progress.and_then(|value| opt_box_str(value.tool_name)),
                        }))
                    }
                    Some("waiting_for_task") => {
                        entries.push(Entry::Progress(ProgressEntry::WaitingForTask {
                            session_id: opt_box_str(raw.session_id),
                            cwd: opt_cow_to_box(raw.cwd.clone()),
                            timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                            parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                            tool_use_id: opt_box_str(raw.tool_use_id),
                            task_description: progress
                                .and_then(|value| opt_cow_to_box(value.task_description.clone())),
                            task_type: progress.and_then(|value| opt_box_str(value.task_type)),
                        }))
                    }
                    _ => entries.push(Entry::Progress(ProgressEntry::Other {
                        kind: progress_kind.map(box_str),
                        session_id: opt_box_str(raw.session_id),
                        cwd: opt_cow_to_box(raw.cwd.clone()),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                        parent_tool_use_id: opt_box_str(raw.parent_tool_use_id),
                        tool_use_id: opt_box_str(raw.tool_use_id),
                    })),
                }
            }
            Some("queue-operation") => {
                version = Version::V2;
                if selection.includes_state() {
                    entries.push(Entry::QueueOperation(QueueOperationEntry {
                        session_id: opt_box_str(raw.session_id),
                        operation: opt_box_str(raw.operation),
                        content: raw.content.map(cow_to_box),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
            Some("system") => {
                version = Version::V2;
                if selection.includes_state() {
                    entries.push(Entry::System(SystemEntry {
                        subtype: opt_box_str(raw.subtype),
                        level: opt_box_str(raw.level),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
            None if raw.display.is_some()
                || raw.project.is_some()
                || raw.pasted_contents.is_some() =>
            {
                version = Version::V2;
                if selection.includes_snapshots() {
                    entries.push(Entry::InputSnapshot(InputSnapshotEntry {
                        display: raw.display.map(cow_to_box),
                        pasted_contents_json: opt_raw_json_box(raw.pasted_contents),
                        project: raw.project.map(cow_to_box),
                        session_id: opt_box_str(raw.session_id),
                        timestamp_millis: opt_timestamp_millis(raw.timestamp.as_ref()),
                    }));
                }
            }
            other => {
                if selection.includes_raw_unknown() {
                    entries.push(Entry::Unknown(UnknownEntry {
                        kind: box_str(other.unwrap_or("unknown")),
                        raw_json: serde_json::to_string(&raw)?.into(),
                        timestamp: opt_timestamp_box_str(raw.timestamp.as_ref()),
                    }));
                }
            }
        }
    }

    if entries.is_empty() && selection.is_full() {
        return Err(Error::Detection {
            agent: Claude::name(),
        });
    }

    trace!(
        target: "agent_sessions::parse",
        agent = Claude::name(),
        version = ?version,
        entries = entries.len(),
        "parsed Claude bundle"
    );
    Ok((
        version,
        Body {
            entries: entries.into_boxed_slice(),
        },
    ))
}

fn parse_claude_meta_only<R>(mut reader: R) -> Result<(Version, Body)>
where
    R: BufRead,
{
    let mut line = Vec::new();
    let mut session_id: Option<SmolStr> = None;
    let mut cwd: Option<SmolStr> = None;
    let mut version = Version::V2;
    let mut timestamp: Option<SmolStr> = None;

    while read_next_nonempty_line(&mut reader, &mut line)? {
        let raw: RawEntry<'_> = match serde_json::from_slice(&line) {
            Ok(raw) => raw,
            Err(_err) if session_id.is_some() || cwd.is_some() => {
                return Ok(parsed_claude_meta(version, session_id, cwd, timestamp));
            }
            Err(err) => return Err(err.into()),
        };
        if session_id.is_none() && raw.session_id.is_some() {
            session_id = opt_box_str(raw.session_id);
            timestamp = opt_timestamp_box_str(raw.timestamp.as_ref());
            version = match raw.kind {
                Some("user" | "assistant") => Version::V1,
                _ => Version::V2,
            };
        }
        if cwd.is_none() && raw.cwd.is_some() {
            cwd = opt_cow_to_box(raw.cwd.clone());
            if timestamp.is_none() {
                timestamp = opt_timestamp_box_str(raw.timestamp.as_ref());
            }
            version = match raw.kind {
                Some("user" | "assistant") => Version::V1,
                _ => Version::V2,
            };
        }
        if session_id.is_none() && cwd.is_none() {
            continue;
        }
        if cwd.is_some() {
            return Ok(parsed_claude_meta(version, session_id, cwd, timestamp));
        }
    }

    if session_id.is_some() || cwd.is_some() {
        return Ok(parsed_claude_meta(version, session_id, cwd, timestamp));
    }

    Err(Error::Detection {
        agent: Claude::name(),
    })
}

fn parsed_claude_meta(
    version: Version,
    session_id: Option<SmolStr>,
    cwd: Option<SmolStr>,
    timestamp: Option<SmolStr>,
) -> (Version, Body) {
    (
        version,
        Body {
            entries: vec![Entry::Progress(ProgressEntry::Other {
                kind: Some("meta".into()),
                session_id,
                cwd,
                timestamp,
                parent_tool_use_id: None,
                tool_use_id: None,
            })]
            .into_boxed_slice(),
        },
    )
}

fn read_next_nonempty_line<R>(reader: &mut R, line: &mut Vec<u8>) -> Result<bool>
where
    R: BufRead,
{
    loop {
        line.clear();
        let bytes_read = reader
            .read_until(b'\n', line)
            .map_err(|err| Error::Message(err.to_string().into()))?;
        if bytes_read == 0 {
            return Ok(false);
        }
        if !line.iter().all(|byte| byte.is_ascii_whitespace()) {
            return Ok(true);
        }
    }
}

fn map_role(raw: &str) -> Role {
    match raw {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "system" => Role::System,
        other => Role::Other(box_str(other)),
    }
}

fn map_claude_blocks(blocks: Vec<ClaudeBlock<'_>>) -> Vec<ContentBlock> {
    blocks
        .into_iter()
        .map(|block| match block.kind {
            "text" => ContentBlock::Text(TextBlock {
                text: cow_to_box(block.text.unwrap_or(Cow::Borrowed(""))),
            }),
            "thinking" => ContentBlock::Thinking(ThinkingBlock {
                text: cow_to_box(block.thinking.unwrap_or(Cow::Borrowed(""))),
            }),
            "image" => ContentBlock::Image(ImageBlock {
                source_type: block
                    .source
                    .as_ref()
                    .and_then(|source| opt_box_str(source.kind)),
                media_type: block
                    .source
                    .as_ref()
                    .and_then(|source| opt_box_str(source.media_type)),
                data: block
                    .source
                    .as_ref()
                    .and_then(|source| opt_cow_to_box(source.data.clone())),
            }),
            "tool_use" => ContentBlock::ToolUse(ToolUseBlock {
                id: opt_box_str(block.id),
                name: box_str(block.name.unwrap_or("unknown")),
                input_json: block.input,
            }),
            "tool_result" => ContentBlock::ToolResult(ToolResultBlock {
                tool_use_id: opt_box_str(block.tool_use_id),
                content: block.content.unwrap_or_else(|| box_str("")),
                is_error: block.is_error.unwrap_or(false),
            }),
            other => ContentBlock::Raw(RawBlock {
                kind: box_str(other),
                raw_json: block.raw.unwrap_or_else(|| box_str("{}")),
            }),
        })
        .collect()
}

#[derive(Deserialize, Serialize)]
struct RawEntry<'a> {
    #[serde(rename = "type")]
    kind: Option<&'a str>,
    #[serde(default, alias = "sessionId")]
    session_id: Option<&'a str>,
    #[serde(default, borrow)]
    cwd: Option<Cow<'a, str>>,
    #[serde(default, alias = "isMeta")]
    is_meta: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_opt_timestamp")]
    timestamp: Option<Timestamp<'a>>,
    #[serde(default, borrow)]
    message: Option<ClaudeMessage<'a>>,
    #[serde(default, alias = "tool_name")]
    tool_name: Option<&'a str>,
    #[serde(default, borrow)]
    tool_input: Option<&'a RawValue>,
    #[serde(default, borrow)]
    tool_output: Option<&'a RawValue>,
    #[serde(default)]
    attachment: Option<AttachmentPayload<'a>>,
    #[serde(default, alias = "permissionMode")]
    permission_mode: Option<&'a str>,
    #[serde(default, alias = "messageId")]
    message_id: Option<&'a str>,
    #[serde(default)]
    snapshot: Option<SnapshotPayload<'a>>,
    #[serde(default, alias = "lastPrompt")]
    last_prompt: Option<Cow<'a, str>>,
    #[serde(default)]
    operation: Option<&'a str>,
    #[serde(default, borrow)]
    content: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    display: Option<Cow<'a, str>>,
    #[serde(default, alias = "pastedContents", borrow)]
    pasted_contents: Option<&'a RawValue>,
    #[serde(default)]
    project: Option<Cow<'a, str>>,
    #[serde(default)]
    subtype: Option<&'a str>,
    #[serde(default)]
    level: Option<&'a str>,
    #[serde(default, alias = "parentToolUseID")]
    parent_tool_use_id: Option<&'a str>,
    #[serde(default, alias = "toolUseID")]
    tool_use_id: Option<&'a str>,
    #[serde(default, rename = "data")]
    progress: Option<ProgressPayload<'a>>,
}

#[derive(Deserialize, Serialize)]
struct ClaudeMessage<'a> {
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    model: Option<Cow<'a, str>>,
    #[serde(default)]
    usage: Option<&'a RawValue>,
    #[serde(default)]
    stop_reason: Option<&'a str>,
    #[serde(default, borrow)]
    content: Option<&'a RawValue>,
}

#[derive(Deserialize, Serialize)]
struct ClaudeUsage<'a> {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cache_creation_input_tokens: Option<u64>,
    #[serde(default)]
    cache_read_input_tokens: Option<u64>,
    #[serde(default)]
    server_tool_use: Option<ClaudeServerToolUse>,
    #[serde(default, borrow)]
    speed: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Serialize)]
struct ClaudeServerToolUse {
    #[serde(default)]
    web_search_requests: Option<u64>,
}

#[derive(Deserialize, Serialize)]
struct ClaudeBlock<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default, borrow)]
    text: Option<Cow<'a, str>>,
    #[serde(default, borrow)]
    thinking: Option<Cow<'a, str>>,
    #[serde(default)]
    source: Option<ImageSource<'a>>,
    #[serde(default)]
    id: Option<&'a str>,
    #[serde(default)]
    name: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_json_box")]
    input: Option<SmolStr>,
    #[serde(default)]
    tool_use_id: Option<&'a str>,
    #[serde(default, deserialize_with = "deserialize_opt_string_or_json_box")]
    content: Option<SmolStr>,
    #[serde(default)]
    is_error: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_opt_json_box")]
    raw: Option<SmolStr>,
}

#[derive(Deserialize, Serialize)]
struct ImageSource<'a> {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: Option<&'a str>,
    #[serde(default)]
    media_type: Option<&'a str>,
    #[serde(default, borrow)]
    data: Option<Cow<'a, str>>,
}

fn parse_claude_message(message: &ClaudeMessage<'_>) -> Result<Vec<ContentBlock>> {
    let Some(raw_content) = message.content else {
        return Ok(Vec::new());
    };

    match raw_content
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => {
            let text = serde_json::from_str::<Cow<'_, str>>(raw_content.get())?;
            Ok(vec![ContentBlock::Text(TextBlock {
                text: cow_to_box(text),
            })])
        }
        Some(b'[') => {
            let blocks: Vec<ClaudeBlock<'_>> = serde_json::from_str(raw_content.get())?;
            Ok(map_claude_blocks(blocks))
        }
        _ => Ok(Vec::new()),
    }
}

fn parse_claude_usage(raw: &RawValue) -> Result<Usage> {
    let usage: ClaudeUsage<'_> = serde_json::from_str(raw.get())?;
    Ok(Usage {
        input_tokens: usage.input_tokens.unwrap_or(0),
        output_tokens: usage.output_tokens.unwrap_or(0),
        cache_creation_input_tokens: usage.cache_creation_input_tokens.unwrap_or(0),
        cache_read_input_tokens: usage.cache_read_input_tokens.unwrap_or(0),
        web_search_requests: usage
            .server_tool_use
            .as_ref()
            .and_then(|tool_use| tool_use.web_search_requests)
            .unwrap_or(0),
        speed: usage.speed.as_deref().map(box_str),
    })
}

fn deserialize_opt_json_box<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SmolStr>, D::Error>
where
    D: SerdeDeserializer<'de>,
{
    let raw = Option::<&RawValue>::deserialize(deserializer)?;
    Ok(raw.map(json_to_box))
}

fn deserialize_opt_string_or_json_box<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SmolStr>, D::Error>
where
    D: SerdeDeserializer<'de>,
{
    let raw = Option::<&RawValue>::deserialize(deserializer)?;
    Ok(raw.map(string_or_json_to_box))
}

fn deserialize_opt_timestamp<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Timestamp<'de>>, D::Error>
where
    D: SerdeDeserializer<'de>,
{
    let raw = Option::<&RawValue>::deserialize(deserializer)?;
    raw.map(parse_timestamp)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn json_to_box(raw: &RawValue) -> SmolStr {
    box_str(raw.get())
}

fn opt_raw_json_box(raw: Option<&RawValue>) -> Option<SmolStr> {
    raw.map(json_to_box)
}

fn opt_box_str(value: Option<&str>) -> Option<SmolStr> {
    value.map(box_str)
}

fn opt_timestamp_box_str(value: Option<&Timestamp<'_>>) -> Option<SmolStr> {
    value.map(timestamp_to_box_str)
}

fn opt_timestamp_millis(value: Option<&Timestamp<'_>>) -> Option<i64> {
    value.and_then(|value| match value {
        Timestamp::Millis(value) => Some(*value),
        Timestamp::Text(_) => None,
    })
}

fn string_or_json_to_box(raw: &RawValue) -> SmolStr {
    match raw
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => {
            let text = serde_json::from_str::<Cow<'_, str>>(raw.get())
                .expect("quoted JSON string should deserialize");
            cow_to_box(text)
        }
        _ => json_to_box(raw),
    }
}

fn parse_timestamp<'a>(raw: &'a RawValue) -> serde_json::Result<Timestamp<'a>> {
    match raw
        .get()
        .as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
    {
        Some(b'"') => Ok(Timestamp::Text(serde_json::from_str(raw.get())?)),
        _ => Ok(Timestamp::Millis(serde_json::from_str(raw.get())?)),
    }
}

fn timestamp_to_box_str(timestamp: &Timestamp<'_>) -> SmolStr {
    match timestamp {
        Timestamp::Text(value) => cow_to_box(value.clone()),
        Timestamp::Millis(value) => value.to_string().into(),
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
enum Timestamp<'a> {
    Text(#[serde(borrow)] Cow<'a, str>),
    Millis(i64),
}

#[derive(Deserialize, Serialize)]
struct AttachmentPayload<'a> {
    #[serde(rename = "type")]
    #[serde(default)]
    attachment_type: Option<&'a str>,
    #[serde(default)]
    name: Option<Cow<'a, str>>,
    #[serde(default)]
    species: Option<&'a str>,
}

#[derive(Deserialize, Serialize)]
struct SnapshotPayload<'a> {
    #[serde(default)]
    timestamp: Option<&'a str>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressPayload<'a> {
    #[serde(rename = "type")]
    #[serde(default)]
    kind: Option<&'a str>,
    #[serde(default)]
    hook_event: Option<&'a str>,
    #[serde(default)]
    hook_name: Option<&'a str>,
    #[serde(default)]
    command: Option<Cow<'a, str>>,
    #[serde(default)]
    output: Option<Cow<'a, str>>,
    #[serde(default, alias = "fullOutput")]
    full_output: Option<Cow<'a, str>>,
    #[serde(default, alias = "elapsedTimeSeconds")]
    elapsed_time_seconds: Option<u64>,
    #[serde(default, alias = "totalLines")]
    total_lines: Option<u64>,
    #[serde(default)]
    prompt: Option<Cow<'a, str>>,
    #[serde(default, alias = "agentId")]
    agent_id: Option<&'a str>,
    #[serde(default, borrow)]
    message: Option<&'a RawValue>,
    #[serde(default)]
    query: Option<Cow<'a, str>>,
    #[serde(default, alias = "resultCount")]
    result_count: Option<u64>,
    #[serde(default)]
    status: Option<&'a str>,
    #[serde(default, alias = "serverName")]
    server_name: Option<&'a str>,
    #[serde(default, alias = "toolName")]
    tool_name: Option<&'a str>,
    #[serde(default, alias = "taskDescription")]
    task_description: Option<Cow<'a, str>>,
    #[serde(default, alias = "taskType")]
    task_type: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde::Deserialize;
    use smol_str::SmolStr;

    #[test]
    fn deserializes_json_string_without_reparsing_objects() {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(deserialize_with = "super::deserialize_opt_string_or_json_box")]
            value: Option<SmolStr>,
        }

        let string_case: Wrapper = serde_json::from_str(r#"{"value":"line\n\"quoted\""}"#).unwrap();
        assert_eq!(string_case.value.as_deref(), Some("line\n\"quoted\""));

        let object_case: Wrapper = serde_json::from_str(r#"{"value":{"k":"v"}}"#).unwrap();
        assert_eq!(object_case.value.as_deref(), Some(r#"{"k":"v"}"#));
    }

    #[test]
    fn direct_agent_session_reader_parses_current_fixture() {
        let bytes =
            include_bytes!("../../../tests/fixtures/claude/claude_current_sample.jsonl").as_slice();
        let metadata = crate::InputMetadata::new().name("claude-session.jsonl");
        let selection = crate::ParseSelection::full();
        let direct = super::event::parse_direct_agent_session_reader_selected(
            Cursor::new(bytes),
            metadata,
            selection,
        )
        .unwrap();

        assert_eq!(direct.agent.as_str(), "claude");
        assert!(!direct.events.is_empty());
    }

    #[test]
    fn reader_meta_only_stops_before_later_malformed_line() {
        let bytes = concat!(
            r#"{"type":"queue-operation","operation":"enqueue","timestamp":"2026-05-01T11:09:46.280Z","sessionId":"sess-1"}"#,
            "\n",
            "not-json",
        );

        let (version, body) =
            super::parse_claude_reader(Cursor::new(bytes), crate::ParseSelection::meta_only())
                .unwrap();

        assert_eq!(version, super::Version::V2);
        let [super::Entry::Progress(super::ProgressEntry::Other { session_id, .. })] =
            body.entries.as_ref()
        else {
            panic!("meta-only reader parse should return one progress meta entry");
        };
        assert_eq!(session_id.as_deref(), Some("sess-1"));
    }

    #[test]
    fn reader_accepts_escaped_windows_cwd() {
        let bytes = concat!(
            r#"{"type":"user","sessionId":"sess-win","cwd":"D:\\Input\\Api\\src\\InputService","timestamp":"2026-05-01T11:09:46.305Z","message":{"id":"msg-win","model":"claude-sonnet-4","content":"hello from windows"}}"#,
            "\n",
        );

        let (_version, body) = super::parse_claude_reader(
            Cursor::new(bytes.as_bytes()),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let [super::Entry::Message(message)] = body.entries.as_ref() else {
            panic!("expected one message entry");
        };
        assert_eq!(message.session_id.as_deref(), Some("sess-win"));
        assert_eq!(
            message.cwd.as_deref(),
            Some(r#"D:\Input\Api\src\InputService"#)
        );
    }

    #[test]
    fn reader_accepts_escaped_windows_progress_command() {
        let bytes = concat!(
            r#"{"type":"progress","sessionId":"sess-win","cwd":"D:\\Input","timestamp":"2026-05-01T11:09:46.305Z","data":{"type":"hook_progress","command":"cd D:\\Input\\Api\nrun tests"}}"#,
            "\n",
        );

        let (_version, body) = super::parse_claude_reader(
            Cursor::new(bytes.as_bytes()),
            crate::ParseSelection::full(),
        )
        .unwrap();

        let [super::Entry::Progress(super::ProgressEntry::HookProgress { command, .. })] =
            body.entries.as_ref()
        else {
            panic!("expected hook progress entry");
        };
        assert_eq!(command.as_deref(), Some("cd D:\\Input\\Api\nrun tests"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_accepts_escaped_windows_cwd() {
        let bytes = concat!(
            r#"{"type":"user","sessionId":"sess-win","cwd":"D:\\Input\\Api\\src\\InputService","timestamp":"2026-05-01T11:09:46.305Z"}"#,
            "\n",
        );

        let meta = super::Claude::probe_session_meta(Cursor::new(bytes.as_bytes()))
            .unwrap()
            .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("sess-win"));
        assert_eq!(
            meta.cwd.as_deref(),
            Some(r#"D:\Input\Api\src\InputService"#)
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_keeps_reading_until_cwd_seen() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-05-01T11:09:46.280Z\",\"sessionId\":\"sess-1\"}\n",
            "{\"type\":\"queue-operation\",\"operation\":\"dequeue\",\"timestamp\":\"2026-05-01T11:09:46.283Z\",\"sessionId\":\"sess-1\"}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-1\",\"cwd\":\"/work/project\",\"timestamp\":\"2026-05-01T11:09:46.305Z\"}\n",
        );

        let meta = super::Claude::probe_session_meta(Cursor::new(bytes.as_bytes()))
            .unwrap()
            .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("sess-1"));
        assert_eq!(meta.cwd.as_deref(), Some("/work/project"));
        assert_eq!(meta.source_kind.as_deref(), Some("queue-operation"));
        assert_eq!(meta.created_at.as_deref(), Some("2026-05-01T11:09:46.280Z"),);
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_returns_partial_when_no_cwd_present() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-05-01T11:09:46.280Z\",\"sessionId\":\"sess-only\"}\n",
            "{\"type\":\"queue-operation\",\"operation\":\"dequeue\",\"timestamp\":\"2026-05-01T11:09:46.283Z\",\"sessionId\":\"sess-only\"}\n",
        );

        let meta = super::Claude::probe_session_meta(Cursor::new(bytes.as_bytes()))
            .unwrap()
            .expect("expected session meta even without cwd");

        assert_eq!(meta.session_id.as_deref(), Some("sess-only"));
        assert!(meta.cwd.is_none());
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_extracts_first_user_text_block() {
        use std::io::Cursor;

        // Real-shape Claude rollout: queue-operation prelude, then a `user`
        // line whose `message.content` is an array with the first text block
        // carrying the user prompt.
        let bytes = concat!(
            "{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-05-01T11:09:46.280Z\",\"sessionId\":\"sess-1\"}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-1\",\"cwd\":\"/work/project\",\"timestamp\":\"2026-05-01T11:09:46.305Z\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"investigate the production bug\\nmore details\"}]}}\n",
        );

        let (meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta + title");

        assert_eq!(meta.cwd.as_deref(), Some("/work/project"));
        assert_eq!(
            title.as_deref(),
            Some("investigate the production bug"),
            "title should be the first line of the first user text block",
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_handles_string_content() {
        use std::io::Cursor;

        // Some Claude tools/tool_result entries serialize `content` as a JSON
        // string rather than a block array. Make sure that shape is handled.
        let bytes = "{\"type\":\"user\",\"sessionId\":\"sess-2\",\"cwd\":\"/tmp/proj\",\"timestamp\":\"2026-05-01T11:09:46.305Z\",\"message\":{\"role\":\"user\",\"content\":\"hello, can you fix it?\"}}\n";

        let (_meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(title.as_deref(), Some("hello, can you fix it?"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_skips_is_meta_continuation_banner() {
        use std::io::Cursor;

        // Continuation rollouts open with a `<local-command-caveat>` user
        // line that Claude marks `isMeta:true`. The title must reach past it
        // to the real follow-up prompt.
        let bytes = concat!(
            "{\"type\":\"user\",\"sessionId\":\"sess-c\",\"cwd\":\"/work/c\",\"isMeta\":true,",
            "\"timestamp\":\"2026-05-02T06:55:51.617Z\",",
            "\"message\":{\"role\":\"user\",\"content\":\"<local-command-caveat>resumed</local-command-caveat>\"}}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-c\",\"cwd\":\"/work/c\",\"timestamp\":\"2026-05-02T06:55:52.000Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"please continue the bug fix\"}]}}\n",
        );

        let (meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(meta.cwd.as_deref(), Some("/work/c"));
        assert_eq!(title.as_deref(), Some("please continue the bug fix"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_skips_slash_command_wrapper() {
        use std::io::Cursor;

        // Slash-command invocations (`/cost`, `/model`, ...) are written as
        // user messages whose content is a `<command-name>` XML wrapper —
        // not something a human typed. Skip until a real user prompt.
        let bytes = concat!(
            "{\"type\":\"user\",\"sessionId\":\"sess-s\",\"cwd\":\"/work/s\",\"timestamp\":\"2026-05-02T07:00:00.000Z\",",
            "\"message\":{\"role\":\"user\",\"content\":\"<command-name>/cost</command-name>\\n<command-args></command-args>\"}}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-s\",\"cwd\":\"/work/s\",\"timestamp\":\"2026-05-02T07:00:01.000Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"investigate the leak\"}]}}\n",
        );

        let (_meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(title.as_deref(), Some("investigate the leak"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_keeps_first_user_without_fork_marker() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"sessionId\":\"sess-plain\"}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-plain\",\"cwd\":\"/work/plain\",\"timestamp\":\"2026-05-10T11:59:25.404Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"plain first user title\"}]}}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-plain\",\"cwd\":\"/work/plain\",\"timestamp\":\"2026-05-11T00:49:51.117Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"plain Claude session title\"}]}}\n",
        );

        let (_meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(title.as_deref(), Some("plain first user title"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_keeps_delegated_user_text() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"type\":\"user\",\"sessionId\":\"sess-fork\",\"cwd\":\"/work/fork\",\"timestamp\":\"2026-05-11T00:49:51.117Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"Task: You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue.\\n\\nTask:\\nImplement the Claude streaming parser.\"}]}}\n",
        );

        let (_meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(
            title.as_deref(),
            Some(
                "Task: You are a delegated subagent running from a fork of the parent session. Tr…"
            )
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_uses_first_post_fork_user_text_after_inherited_user() {
        use std::io::Cursor;

        let bytes = concat!(
            "{\"type\":\"queue-operation\",\"operation\":\"enqueue\",\"timestamp\":\"2026-05-11T00:49:47.936Z\",\"sessionId\":\"sess-fork\"}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-fork\",\"cwd\":\"/work/fork\",\"timestamp\":\"2026-05-10T11:59:25.404Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"parent title should not leak\"}]}}\n",
            "{\"type\":\"user\",\"sessionId\":\"sess-fork\",\"cwd\":\"/work/fork\",\"timestamp\":\"2026-05-11T00:49:51.117Z\",",
            "\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"Task: You are a delegated subagent running from a fork of the parent session. Treat the inherited conversation as reference-only context, not a live thread to continue.\\n\\nTask:\\nImplement the Claude fork title.\"}]}}\n",
        );

        let (_meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(
            title.as_deref(),
            Some(
                "Task: You are a delegated subagent running from a fork of the parent session. Tr…"
            )
        );
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_scans_past_wrapper_lines_for_late_title() {
        use std::io::Cursor;

        let mut bytes = String::new();
        bytes.push_str(
            "{\"type\":\"user\",\"sessionId\":\"sess-late-wrapper\",\"cwd\":\"/work/cap\",\"timestamp\":\"2026-05-02T07:00:00.000Z\",\
             \"message\":{\"role\":\"user\",\"content\":\"<command-name>/cost</command-name>\"}}\n",
        );
        for _ in 0..200 {
            bytes.push_str(
                "{\"type\":\"user\",\"sessionId\":\"sess-late-wrapper\",\"cwd\":\"/work/cap\",\"timestamp\":\"2026-05-02T07:00:01.000Z\",\
                 \"message\":{\"role\":\"user\",\"content\":\"<command-name>/cost</command-name>\"}}\n",
            );
        }
        bytes.push_str(
            "{\"type\":\"user\",\"sessionId\":\"sess-late-wrapper\",\"cwd\":\"/work/cap\",\"timestamp\":\"2026-05-02T07:10:00.000Z\",\
             \"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"late Claude title\"}]}}\n",
        );

        let (meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(meta.cwd.as_deref(), Some("/work/cap"));
        assert_eq!(title.as_deref(), Some("late Claude title"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_scans_to_late_cwd_and_title() {
        use std::io::Cursor;

        let mut bytes = String::new();
        for idx in 0..300 {
            bytes.push_str(&format!(
                "{{\"type\":\"queue-operation\",\"sessionId\":\"sess-pre-cwd\",\"timestamp\":\"2026-05-02T07:{idx:02}:00.000Z\"}}\n"
            ));
        }
        bytes.push_str(
            "{\"type\":\"user\",\"sessionId\":\"sess-pre-cwd\",\"cwd\":\"/work/late-cwd\",\"timestamp\":\"2026-05-02T08:00:00.000Z\",\
             \"message\":{\"role\":\"user\",\"content\":\"late cwd title\"}}\n",
        );
        bytes.push_str("not-json-after-title\n");

        let (meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .expect("poison after title must not be read")
                .expect("expected session meta");

        assert_eq!(meta.session_id.as_deref(), Some("sess-pre-cwd"));
        assert_eq!(meta.cwd.as_deref(), Some("/work/late-cwd"));
        assert_eq!(title.as_deref(), Some("late cwd title"));
    }

    #[cfg(feature = "agent_session")]
    #[test]
    fn probe_session_meta_with_title_returns_none_title_when_first_cwd_line_is_not_user() {
        use std::io::Cursor;

        // First line carrying cwd is a `system` log, not a user message.
        // Probe must still return cwd; title falls through to None so the
        // caller can use a downstream fallback (path-based).
        let bytes = "{\"type\":\"system\",\"sessionId\":\"sess-3\",\"cwd\":\"/tmp/proj\",\"timestamp\":\"2026-05-01T11:09:46.305Z\"}\n";

        let (meta, title) =
            super::Claude::probe_session_meta_with_title(Cursor::new(bytes.as_bytes()))
                .unwrap()
                .expect("expected session meta");

        assert_eq!(meta.cwd.as_deref(), Some("/tmp/proj"));
        assert!(title.is_none());
    }
}
